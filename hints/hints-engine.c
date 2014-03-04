/*
    This file is part of VK/KittenPHP-DB-Engine.

    VK/KittenPHP-DB-Engine is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 2 of the License, or
    (at your option) any later version.

    VK/KittenPHP-DB-Engine is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with VK/KittenPHP-DB-Engine.  If not, see <http://www.gnu.org/licenses/>.

    This program is released under the GPL with the additional exemption
    that compiling, linking, and/or using OpenSSL is allowed.
    You are free to remove this exemption from derived works.

    Copyright 2010-2013 Vkontakte Ltd
              2010-2013 Arseny Smirnov
              2010-2013 Aliaksei Levin
*/

#define _FILE_OFFSET_BITS 64

#include <assert.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/mman.h>
#include <signal.h>

#include "crc32.h"
#include "kdb-data-common.h"
#include "kdb-hints-binlog.h"
#include "net-events.h"
#include "net-buffers.h"
#include "server-functions.h"
#include "net-connections.h"
#include "net-memcache-server.h"
#include "net-crypto-aes.h"

#include "net-rpc-server.h"
#include "net-rpc-common.h"

#include "hints-data.h"
#include "dl-set-get.h"

#include "common-data.h"
#define MAX_VALUE_LEN (1 << 20)

#define VERSION "0.9999"

#ifdef NOHINTS
#define VERSION_STR "rating "VERSION
#else
#define VERSION_STR "hints "VERSION
#endif

#define TCP_PORT 11211
#define UDP_PORT 11211

/*
 *
 *    MEMCACHED PORT
 *
 */

int port = TCP_PORT, udp_port = UDP_PORT;

struct in_addr settings_addr;
int interactive = 0;

volatile int sigpoll_cnt;

long long binlog_loaded_size;
double binlog_load_time, index_load_time;

long long cmd_get, cmd_set, cmd_global, cmd_delete, get_hits, get_missed, cmd_incr, cmd_get_user_info, cmd_stats, cmd_version;
double cmd_get_time = 0.0, cmd_set_time = 0.0, cmd_delete_time = 0.0, cmd_incr_time = 0.0;
double max_cmd_get_time = 0.0, max_cmd_set_time = 0.0, max_cmd_delete_time = 0.0, max_cmd_incr_time = 0.0;

#define STATS_BUFF_SIZE (1 << 16)
int stats_buff_len;
char stats_buff[STATS_BUFF_SIZE];
int hints_prepare_stats (void);


int hints_engine_wakeup (struct connection *c);
int hints_engine_alarm (struct connection *c);

conn_type_t ct_hints_engine_server = {
  .magic = CONN_FUNC_MAGIC,
  .title = "hints_engine_server",
  .accept = accept_new_connections,
  .init_accepted = mcs_init_accepted,
  .run = server_read_write,
  .reader = server_reader,
  .writer = server_writer,
  .parse_execute = mcs_parse_execute,
  .close = server_close_connection,
  .free_buffers = free_connection_buffers,
  .init_outbound = server_failed,
  .connected = server_failed,
  .wakeup = hints_engine_wakeup,
  .alarm = hints_engine_alarm,
#ifndef NOAES
  .crypto_init = aes_crypto_init,
  .crypto_free = aes_crypto_free,
  .crypto_encrypt_output = aes_crypto_encrypt_output,
  .crypto_decrypt_input = aes_crypto_decrypt_input,
  .crypto_needed_output_bytes = aes_crypto_needed_output_bytes,
#endif
};



int memcache_store (struct connection *c, int op, const char *key, int key_len, int flags, int delay, int size);
int memcache_get (struct connection *c, const char *key, int key_len);
int memcache_delete (struct connection *c, const char *key, int key_len);
int memcache_incr (struct connection *c, int op, const char *key, int key_len, long long arg);
int memcache_stats (struct connection *c);
int memcache_version (struct connection *c);
int memcache_get_start (struct connection *c);
int memcache_get_end (struct connection *c, int key_count);

struct memcache_server_functions memcache_methods = {
  .execute = mcs_execute,
  .mc_store = memcache_store,
  .mc_get_start = memcache_get_start,
  .mc_get = memcache_get,
  .mc_get_end = memcache_get_end,
  .mc_incr = memcache_incr,
  .mc_delete = memcache_delete,
  .mc_version = memcache_version,
  .mc_stats = memcache_stats,
  .mc_check_perm = mcs_default_check_perm,
  .mc_init_crypto = mcs_init_crypto
};

struct rpc_server_functions rpc_methods = {
  .execute = default_tl_rpcs_execute,
  .check_ready = server_check_ready,
  .flush_packet = rpcs_flush_packet,
  .rpc_check_perm = rpcs_default_check_perm,
  .rpc_init_crypto = rpcs_init_crypto,
  .rpc_close = default_tl_close_conn,
  .memcache_fallback_type = &ct_hints_engine_server,
  .memcache_fallback_extra = &memcache_methods
};


char buf[MAX_VALUE_LEN];

char *history_q[MAX_HISTORY + 1];
int history_t[MAX_HISTORY + 1];
int history_l, history_r;
const char *op_names[4] = {"set", "get", "increment", "delete"};

void history_q_add (char *s, int t) {
  if (s == NULL) {
    return;
  }
  history_t[history_r] = t;
  history_q[history_r++] = dl_strdup (s);
  if (history_r > MAX_HISTORY) {
    history_r = 0;
  }
  if (history_l >= history_r) {
    dl_strfree (history_q[history_l]);
    history_q[history_l++] = 0;
    if (history_l > MAX_HISTORY) {
      history_l = 0;
    }
  }
}


static inline void safe_read_in (netbuffer_t *H, char *data, int len) {
  assert (read_in (H, data, len) == len);
  int i;
  for (i = 0; i < len; i++) {
    if (data[i] == 0) {
      data[i] = ' ';
    }
  }
}

static inline void eat_at (const char *key, int key_len, char **new_key, int *new_len) {
  if (*key == '^') {
    key++;
    key_len--;
  }

  *new_key = (char *)key;
  *new_len = key_len;

  if ((*key >= '0' && *key <= '9') || (*key == '-' && key[1] >= '0' && key[1] <= '9')) {
    key++;
    while (*key >= '0' && *key <= '9') {
      key++;
    }

    if (*key++ == '@') {
      if (*key == '^') {
        key++;
      }

      *new_len -= (key - *new_key);
      *new_key = (char *)key;
    }
  }
}

int memcache_store (struct connection *c, int op, const char *old_key, int old_key_len, int flags, int delay, int size) {
  INIT;

  if (verbosity > 1) {
    fprintf (stderr, "memcache_store: key='%s', key_len=%d, value_len=%d, ", old_key, old_key_len, size);
  }

  char *key;
  int key_len;

  eat_at (old_key, old_key_len, &key, &key_len);

  history_q_add (key, 0);

  if (size + 1 < MAX_VALUE_LEN) {
    if (!write_only && key_len >= 12 && (!strncmp (key, "sort_by_rate", 12) || !strncmp (key, "sort_by_rand", 12))) {
      int random_tag;

      if (sscanf (key + 12, "%*d_%d", &random_tag) != 1) {
        RETURN(set, -2);
      }

      if (msg_reinit (MESSAGE (c), size, random_tag) < 0) {
        RETURN(set, -2);
      }
      safe_read_in (&c->In, msg_get_buf (MESSAGE (c)), size);

      RETURN(set, 1);
    }

    safe_read_in (&c->In, buf, size);
    buf[size] = 0;

    //Обнуление рейтинга пользователя
    //set ("user_rating_nullify{$user_id}", "")
    if (key_len >= 19 && !strncmp (key, "user_rating_nullify", 19)) {
      int user_id;
      int result = sscanf (key + 19, "%d", &user_id) == 1;

      if (verbosity > 1) {
        fprintf (stderr, "user rating nullify %d\n", user_id);
      }

      result &= do_nullify_user_rating (user_id);

      RETURN(set, result);
    }

    //Изменение состояния пользователя
    //set ("user_rating_state{$user_id}", "{$flag}")
    if (key_len >= 17 && !strncmp (key, "user_rating_state", 17)) {
      int user_id, type;

      if (verbosity > 1) {
        fprintf (stderr, "user rating state\n");
      }

      int result = (sscanf (key + 17, "%d", &user_id) == 1 && sscanf (buf, "%d", &type) == 1 && do_set_user_rating_state (user_id, type));

      RETURN(set, result);
    }

    //Получение статуса пользователя
    //set ("user_info{$user_id}", "{$text}")
    if (key_len >= 9 && !strncmp (key, "user_info", 9)) {
      int user_id, info;

      if (verbosity > 1) {
        fprintf (stderr, "set user info\n");
      }

      int result = sscanf (key + 9, "%d", &user_id) == 1 && sscanf (buf, "%d", &info) == 1 && do_set_user_info (user_id, info);

      RETURN(set, result);
    }

    //Увеличение рейтинга одного из объектов пользователя за счёт уменьшения рейтинга других
    //set ("user_object_winner{$user_id},{$type}:{$winner_id}*{$rating_num}", "{$loser_n},{$loser_id},...,{$loser_id}")
    if (key_len >= 18 && !strncmp (key, "user_object_winner", 18)) {
      int user_id, type;
      long long object_id;
      int object_id2;
      int add = -1, add2 = -1;

      int t = sscanf (key + 18, "%d,%d:%lld%n_%d%n", &user_id, &type, &object_id, &add, &object_id2, &add2);
      if (t == 3 || t == 4) {
        if (t == 4) {
          if (object_id != user_id && object_id2 != user_id) {
            RETURN(set, 0);
          }
          if (object_id == user_id) {
            object_id = object_id2;
          }
          add = add2;
        }

        assert (add != -1);
        int num;
        if (sscanf (key + 18 + add, "*%d", &num) != 1) {
          num = 0;
        }

        int losers_cnt;
        if (sscanf (buf, "%d%n", &losers_cnt, &add) == 1 && 0 < losers_cnt && losers_cnt <= 16000) {
          static int losers[16000];
          int cur_add = 0, i;
          for (i = 0; i < losers_cnt; i++) {
            if (buf[add] != ',') {
              RETURN(set, 0);
            }
            add++;

            if (sscanf (buf + add, "%d%n", &losers[i], &cur_add) != 1 || !check_object_id (losers[i]) || losers[i] == object_id) {
              RETURN(set, 0);
            }
            add += cur_add;
          }
          if (buf[add]) {
            RETURN(set, 0);
          }

          int result = do_user_object_winner (user_id, type, num, object_id, losers_cnt, losers);

          RETURN(set, result);
        }
      }

      RETURN(set, 0);
    }

    //Изменение типа объекта для одного пользователя
    //set ("user_object_type{$user_id},{$type}:{$object_id}", "{$new_type}")
    if (key_len >= 16 && !strncmp (key, "user_object_type", 16)) {
      int user_id, type;
      long long object_id;
      int object_id2;

      int t = sscanf (key + 16, "%d,%d:%lld_%d", &user_id, &type, &object_id, &object_id2);
      if (t == 3 || t == 4) {
        if (t == 4) {
          if (object_id != user_id && object_id2 != user_id) {
            RETURN(set, 0);
          }
          if (object_id == user_id) {
            object_id = object_id2;
          }
        }

        int new_type;
        if (sscanf (buf, "%d", &new_type) == 1) {
          RETURN(set, do_set_user_object_type (user_id, type, object_id, new_type));
        }
      }

      RETURN(set, 0);
    }

    //Изменение рейтинга объекта для одного пользователя
    //set ("user_object_rating{$user_id},{$type}:{$object_id}", "{$new_rating}")
    if (key_len >= 18 && !strncmp (key, "user_object_rating", 18)) {
      int user_id, type;
      long long object_id;
      int object_id2;
      int add = -1;

      int t = sscanf (key + 18, "%d,%d:%lld%n_%d%n", &user_id, &type, &object_id, &add, &object_id2, &add);
      if (t == 3 || t == 4) {
        if (t == 4) {
          if (object_id != user_id && object_id2 != user_id) {
            RETURN(set, 0);
          }
          if (object_id == user_id) {
            object_id = object_id2;
          }
        }
        int num;
        if (sscanf (key + 18 + add, "*%d", &num) != 1) {
          num = 0;
        }

        float new_rating;
        int pos;
        if (sscanf (buf, "%f%n", &new_rating, &pos) != 1 || buf[pos]) {
          RETURN(set, 0);
        }

        int result = do_set_user_object_rating (user_id, type, object_id, new_rating, num);
        RETURN(set, result);
      }

      RETURN(set, 0);
    }

    //Добавление нового объекта для одного пользователя
    //set ("user_object{$user_id},{$type}:{$object_id}", "{$text}")
    if (key_len >= 11 && !strncmp (key, "user_object", 11)) {
      int user_id, type;
      long long object_id;
      int object_id2;

      int t = sscanf (key + 11, "%d,%d:%lld_%d", &user_id, &type, &object_id, &object_id2);
      if (t == 3 || t == 4) {
        if (t == 4) {
          if (object_id != user_id && object_id2 != user_id) {
            RETURN(set, 0);
          }
          if (object_id == user_id) {
            object_id = object_id2;
          }
        }
#ifdef HINTS
        RETURN(set, do_add_user_object (user_id, type, object_id, size, buf));
#else
        RETURN(set, do_add_user_object (user_id, type, object_id));
#endif
      }

      RETURN(set, 0);
    }

#ifdef HINTS
    //Изменение ключевых слов для данного глобального объекта
    //set ("object_text{$type}:{$object_id}", "{$text}")
    if (key_len >= 11 && !strncmp (key, "object_text", 11)) {
      int type;
      long long object_id;

      if (sscanf (key + 11, "%d:%lld", &type, &object_id) == 2) {
        cmd_global++;
        RETURN(set, do_add_object_text (type, object_id, size, buf));
      }

      RETURN(set, 0);
    }
#endif

    //Изменение типа глобального объекта
    //set ("object_type{$type}:{$object_id}", "{$new_type}")
    if (key_len >= 11 && !strncmp (key, "object_type", 11)) {
      int type;
      long long object_id;
      int new_type;

      if (sscanf (key + 11, "%d:%lld", &type, &object_id) == 2 && 
          sscanf (buf, "%d", &new_type) == 1) {
        cmd_global++;
        RETURN(set, do_set_object_type (type, object_id, new_type));
      }

      RETURN(set, 0);
    }

    RETURN(set, 0);
  }

  RETURN(set, -2);
}

long long get_long (const char **s) {
  unsigned long long res = 0;
  while (**s <= '9' && **s >= '0') {
    res = res * 10 + **s - '0';
    (*s)++;
  }
  return (long long)res;
}

int memcache_try_get (struct connection *c, const char *old_key, int old_key_len) {
  if (verbosity > 1) {
    fprintf (stderr, "memcache_get: key='%s', key_len=%d\n", old_key, old_key_len);
  }

  char *key;
  int key_len;

  eat_at (old_key, old_key_len, &key, &key_len);

  int need_raw_format = 0;
  if (key_len >= 1 && key[0] == '!') {
    need_raw_format = 1;
    key_len--;
    key++;
  }

  history_q_add (key, 1);

  if (!write_only && key_len >= 11 && !strncmp (key, "unload_user", 11)) {
    int user_id;

    sscanf (key + 11, "%d", &user_id);
    test_user_unload (user_id);

    sprintf (buf, "1,0,%d", user_id);

    return_one_key (c, old_key, buf, strlen (buf));

    return 0;
  }

  //Получение статуса пользователя
  //get ("user_info{$user_id}")
  if (!write_only && key_len >= 9 && !strncmp (key, "user_info", 9)) {
    cmd_get_user_info++;
    int user_id;

    if (sscanf (key + 9, "%d", &user_id) != 1) {
      return 0;
    }

    int result = get_user_info (user_id);
    if (result < -2) {
      return 0;
    }

    sprintf (buf, "%d", result);

    return_one_key (c, old_key, buf, strlen (buf));

    return 0;
  }

  if (key_len >= 5 && !strncmp (key, "stats", 5)) {
    int len = hints_prepare_stats ();
    int len2 = prepare_stats (c, stats_buff + len, STATS_BUFF_SIZE - len);
    return_one_key (c, old_key, stats_buff, len + len2 - 1);

    return 0;
  }
  if (key_len == 6 && !strncmp (key, "gstats", 6)) {
    int len = get_global_stats (stats_buff);
    return_one_key (c, old_key, stats_buff, len);

    return 0;
  }

  INIT;

#define TRY(x) if (x) {          \
                 RETURN(get, 0); \
               }                 \

  if (!write_only && key_len >= 12 && (!strncmp (key, "sort_by_rate", 12) || !strncmp (key, "sort_by_rand", 12))) {
    int random_tag;
    message *msg = MESSAGE (c);

    TRY (sscanf (key + 12, "%*d_%d", &random_tag) != 1 || msg_verify (msg, random_tag) < 0);

    int user_id, add, res_cnt = MAX_CNT, num = 0;
    char *s = msg->text;

    assert (s != NULL);

    TRY (sscanf (s, "%d%n", &user_id, &add) != 1);

    s += add;
    if (s[0] == '*') {
      s++;
      TRY (sscanf (s, "%d%n", &num, &add) != 1);
      s += add;
    }

    if (s[0] == '#') {
      s++;
      TRY (sscanf (s, "%d%n", &res_cnt, &add) != 1);
      s += add;
    }

    int objects_cnt = 0;
    while (s[0] == ',' && objects_cnt < MAX_CNT) {
      int type, id;
      TRY (sscanf (s, ",%d,%d%n", &type, &id, &add) != 2);
      s += add;
      objects_typeids_to_sort[objects_cnt++] = TYPE_ID(type, id);
    }

    res_cnt = sort_user_objects (user_id, objects_cnt, objects_typeids_to_sort, res_cnt, num, key[11] == 'd');
    if (res_cnt < 0) {
      RETURN(get, res_cnt);
    }

    //<$found_cnt>,<$type1>,<$object_id1>,<$type2>,<$object_id2>,...\n

    int result_len = sprintf (buf, "%d", res_cnt);
    int i;
    for (i = 0; i < res_cnt; i++) {
      long long h = objects_typeids_to_sort[i];
      result_len += sprintf (buf + result_len, ",%d,%d", TYPE(h), ID(h));
      if (result_len > sizeof (buf) - 1024) {
        fprintf (stderr, "Output limit exceeded.\n");
        break;
      }
    }

    return_one_key (c, old_key, buf, result_len);
    RETURN(get, 0);
  }

  //Получение подсказок
  //get ("user_hints{$user_id}#{$res_cnt}({$word1}+{$word2}+...)")
  int gather_flag = 0;
  if (!write_only && ((key_len >= 10 && !strncmp (key, "user_hints", 10)) || (key_len >= 12 && !strncmp (key, "gather_hints", 12) && (gather_flag = 1)))) {
    int need_rating = 0;
#ifdef HINTS
    int need_text = 0;
    int need_full = 0;
    int need_latin = 0;
#else
    int need_rand = 0;
#endif
    int add = 10 + gather_flag * 2;

    if (key_len >= add + 7 && !strncmp (key + add, "_rating", 7)) {
      need_rating = 1;
      add += 7;
    }
#ifdef HINTS
    if (key_len >= add + 5 && !strncmp (key + add, "_text", 5)) {
      need_text = 1;
      add += 5;
    }
    if (key_len >= add + 5 && !strncmp (key + add, "_full", 5)) {
      need_full = 1;
      add += 5;
    }

    if (key_len >= add + 6 && !strncmp (key + add, "_latin", 6)) {
      need_latin = 6;
      add += 6;
    }
#else
    if (key_len >= add + 5 && !strncmp (key + add, "_rand", 5)) {
      need_rand = 1;
      add += 5;
    }
#endif

    if (gather_flag) {
      need_raw_format = 1;
    }

    int user_id;
    int res_cnt = MAX_CNT;
    int res_type = -1;
    int res_num = 0;
    const char *s = key + add;
    user_id = (int)get_long (&s);
    int was_type = 0, was_cnt = 0, was_num = 0;

    while (s[0] == '#' || s[0] == ',' || s[0] == '*') {
      if (s[0] == ',') {
        TRY (was_type);
        was_type = 1;
        s++;
        res_type = (int)get_long (&s);
      }
      if (s[0] == '#') {
        TRY (was_cnt);
        was_cnt = 1;
        s++;
        res_cnt = (int)get_long (&s);
      }
      if (s[0] == '*') {
        TRY (was_num);
        was_num = 1;
        s++;
        res_num = (int)get_long (&s);
      }
    }
    TRY (s[0] != '(');

    buf[0] = 0;
    sscanf (s + 1, "%s", buf);
    int len = strlen (buf);
    if (len > 0 && buf[len - 1] == ')') {
      buf[len - 1] = 0;
    }

    if (verbosity > 1) {
      fprintf (stderr, "run get_hints (user_id = %d, text = %s)\n", user_id, buf);
    }

#ifdef HINTS
    int buf_len = get_user_hints (user_id, MAX_VALUE_LEN, buf, res_type, res_cnt, res_num, need_rating | need_full, need_text | need_full, need_latin, need_raw_format);
#else
    int buf_len = get_user_hints (user_id, MAX_VALUE_LEN, buf, res_type, res_cnt, res_num, need_rating, need_rand, need_raw_format);
#endif
    if (buf_len < 0) {
      RETURN(get, buf_len);
    }

    return_one_key (c, old_key, buf, buf_len);

    if (verbosity > 0) {
      if (mytime() + cmd_time > 0.005) {
        fprintf (stderr, "Warning!!! Search query for user %d was %lf seconds.\n", user_id, mytime() + cmd_time);
      }
    }

    RETURN(get, 0);
  }

#ifdef HINTS
  //Получение текста объекта
  if (!write_only && key_len >= 16 && !strncmp (key, "user_object_text", 16)) {
    int user_id, type;
    long long object_id;
    int object_id2;

    int t = sscanf (key + 16, "%d,%d:%lld_%d", &user_id, &type, &object_id, &object_id2);
    if (t == 3 || t == 4) {
      if (t == 4) {
        TRY (object_id == user_id || object_id2 == user_id);
        if (object_id == user_id) {
          object_id = object_id2;
        }
      }

      char *text = NULL;
      int res = get_user_object_text (user_id, type, object_id, &text);

      if (res <= 0) {
        RETURN(get, res);
      }
      assert (text != NULL);

      return_one_key (c, old_key, text, strlen (text));
    }

    RETURN(get, 0);
  }
#endif

  if (key_len >= 7 && !strncmp (key, "history", 7)) {
    int cnt;
    if (sscanf (key + 7, "%d", &cnt) != 1) {
      cnt = MAX_HISTORY;
    }

    char *res = buf;
    int cur = history_r;

    while (cnt-- && cur != history_l) {
      cur--;
      if (cur == -1) {
        cur += MAX_HISTORY + 1;
      }
      int l = strlen (history_q[cur]);
      if (res - buf + l + 2 + 40 >= MAX_VALUE_LEN) {
        break;
      }
      int nl = strlen (op_names[history_t[cur]]);
      memcpy (res, op_names[history_t[cur]], nl);
      res += nl;
      *res++ = ' ';

      memcpy (res, history_q[cur], l);
      res += l;
      *res++ = '\n';
    }
    *res++ = 0;

    return_one_key (c, old_key, buf, strlen (buf));

    RETURN(get, 0);
  }

  RETURN(get, 0);
#undef TRY
}


conn_query_type_t aio_metafile_query_type = {
  .magic = CQUERY_FUNC_MAGIC,
  .title = "hints-data-aio-metafile-query",
  .wakeup = aio_query_timeout,
  .close = delete_aio_query,
  .complete = delete_aio_query
};


int memcache_get (struct connection *c, const char *key, int key_len) {
  if (verbosity > 1) {
    fprintf (stderr, "memcache_get: key='%s'\n", key);
  }

  WaitAioArrClear();

  int res = memcache_try_get (c, key, key_len);
  if (res == -2) {
    if (c->flags & C_INTIMEOUT) {
      if (verbosity > 1) {
        fprintf (stderr, "memcache_get: IN TIMEOUT (%p)\n", c);
      }
      return 0;
    }
    if (c->Out.total_bytes > 8192) {
      c->flags |= C_WANTWR;
      c->type->writer (c);
    }
//    fprintf (stderr, "memcache_get_nonblock returns -2, WaitAio=%p\n", WaitAio);

    assert (WaitAioArrPos);

    int i;
    for (i = 0; i < WaitAioArrPos; i++) {
      assert (WaitAioArr[i]);
      conn_schedule_aio (WaitAioArr[i], c, 0.7, &aio_metafile_query_type);
    }
    set_connection_timeout (c, 0.5);
  }
  return 0;
}

int memcache_get_start (struct connection *c) {
  c->flags &= ~C_INTIMEOUT;
  c->query_start_time = mytime();
  return 0;
}

int memcache_get_end (struct connection *c, int key_count) {
  c->flags &= ~C_INTIMEOUT;
  c->last_query_time = mytime() - c->query_start_time;
  write_out (&c->Out, "END\r\n", 5);
  if (verbosity > 1) {
    fprintf (stderr, "memcache_get end: query time %.3fms\n", c->last_query_time * 1000);
  }
  return 0;
}

int memcache_delete (struct connection *c, const char *old_key, int old_key_len) {
  INIT;

  if (verbosity > 1) {
    fprintf (stderr, "memcache_delete: key='%s'\n", old_key);
  }

  char *key;
  int key_len;

  eat_at (old_key, old_key_len, &key, &key_len);

  history_q_add (key, 3);

  //Удаление объекта для одного пользователя
  //delete ("user_object{$user_id},{$type}:{$object_id}")
  if (key_len >= 11 && !strncmp (key, "user_object", 11)) {
    int user_id, type;
    long long object_id;
    int object_id2;

    int t = sscanf (key + 11, "%d,%d:%lld_%d", &user_id, &type, &object_id, &object_id2);
    if (t == 3 || t == 4) {
      if (t == 4) {
        if (object_id == user_id || object_id2 == user_id) {
          write_out (&c->Out, "NOT_FOUND\r\n", 11);
          RETURN(delete, 0);
        }
        if (object_id == user_id) {
          object_id = object_id2;
        }
      }
      if (do_del_user_object (user_id, type, object_id)) {
        write_out (&c->Out, "DELETED\r\n", 9);
      } else {
        write_out (&c->Out, "NOT_FOUND\r\n", 11);
      }
      RETURN(delete, 0);
    }

    write_out (&c->Out, "NOT_FOUND\r\n", 11);
    RETURN(delete, 0);
  }

  //Удаление объекта
  //delete ("object_text{$type}:{$object_id}")
  if (key_len >= 11 && !strncmp (key, "object_text", 11)) {
    int type;
    long long object_id;
    int object_id2;

    int t = sscanf (key + 11, "%d:%lld_%d", &type, &object_id, &object_id2);
    if (t == 2 || t == 3) {
      if (t == 3) {
        write_out (&c->Out, "NOT_FOUND\r\n", 11);
        RETURN(delete, 0);
      }

      if (do_del_object_text (type, object_id)) {
        write_out (&c->Out, "DELETED\r\n", 9);
      } else {
        write_out (&c->Out, "NOT_FOUND\r\n", 11);
      }
      cmd_global++;
      RETURN(delete, 0);
    }

    write_out (&c->Out, "NOT_FOUND\r\n", 11);
    RETURN(delete, 0);
  }

  write_out (&c->Out, "NOT_FOUND\r\n", 11);
  RETURN(delete, 0);
}

int memcache_incr (struct connection *c, int op, const char *old_key, int old_key_len, long long arg) {
  INIT;

  if (verbosity > 1) {
    fprintf (stderr, "memcache_incr: op=%d, key='%s', val=%lld\n", op, old_key, arg);
  }

  char *key;
  int key_len;

  eat_at (old_key, old_key_len, &key, &key_len);

  history_q_add (key, 2);

  if (op == 1) {
    if (fading) {
      write_out (&c->Out, "NOT_FOUND\r\n", 11);
      RETURN(incr, 0);
    }
    arg *= -1;
  }

  if (arg > (1ll << 30)) {
    arg = (1ll << 30);
  } else if (arg < -(1ll << 30)) {
    arg = -(1ll << 30);
  }

  //Изменение рейтинга объекта
  //increment ("user_object{$user_id},{$type}:{$object_id}", {$cnt})</code>
  if (key_len >= 11 && !strncmp (key, "user_object", 11)) {
    int user_id, type;
    long long object_id;
    int object_id2;
    int add = -1, add2 = -1;

    int t = sscanf (key + 11, "%d,%d:%lld%n_%d%n", &user_id, &type, &object_id, &add, &object_id2, &add2);
    if (t == 3 || t == 4) {
      if (t == 4) {
        if ((object_id != user_id && object_id2 != user_id) || add2 == -1) {
          RETURN(incr, 0);
        }
        if (object_id == user_id) {
          object_id = object_id2;
        }
        add = add2;
      }
      int num;
      if (sscanf (key + 11 + add, "*%d", &num) != 1) {
        num = 0;
      }

      int x = do_increment_user_object_rating (user_id, type, object_id, arg, num);
      if (x) {
        write_out (&c->Out, "1\r\n", 3);
        RETURN(incr, 0);
      }
    }
  }

  write_out (&c->Out, "NOT_FOUND\r\n", 11);
  RETURN(incr, 0);
}

int memcache_version (struct connection *c) {
  cmd_version++;
  write_out (&c->Out, "VERSION "VERSION"\r\n", 9 + sizeof (VERSION));
  return 0;
}

int hints_prepare_stats (void) {
  cmd_stats++;
  int log_uncommitted = compute_uncommitted_log_bytes();

  return snprintf (stats_buff, STATS_BUFF_SIZE,
        "heap_used\t%ld\n"
        "heap_max\t%ld\n"
        "binlog_original_size\t%lld\n"
        "binlog_loaded_bytes\t%lld\n"
        "binlog_load_time\t%.6lfs\n"
        "current_binlog_size\t%lld\n"
        "binlog_uncommitted_bytes\t%d\n"
        "binlog_path\t%s\n"
        "binlog_first_timestamp\t%d\n"
        "binlog_read_timestamp\t%d\n"
        "binlog_last_timestamp\t%d\n"
        "max_binlog_size\t%lld\n"
        "index_users\t%d\n"
        "index_loaded_bytes\t%d\n"
        "index_size\t%lld\n"
        "index_path\t%s\n"
        "index_load_time\t%.6lfs\n"
        "indexed_users\t%d\n"
        "new_users\t%d\n"
        "memory_users\t%d\n"
        "unloaded_users\t%lld\n"
        "min_cache_time\t%d\n"
        "mean_cache_time\t%.7lf\n"
        "max_cache_time\t%d\n"
        "friend_changes\t%lld\n"
        "MAX_CNT\t%d\n"
        "pid\t%d\n"
        "version\t%s\n"
        "pointer_size\t%d\n"
        "static_memory_used\t%ld\n"
        "current_memory_used\t%ld\n"
        "current_changes_memory_used\t%ld\n"
        "current_treap_memory_used\t%ld\n"
        "cmd_get\t%lld\n"
        "cmd_set\t%lld\n"
        "cmd_delete\t%lld\n"
        "cmd_incr\t%lld\n"
        "cmd_global\t%lld\n"
        "cmd_get_user_info\t%lld\n"
        "cmd_stats\t%lld\n"
        "cmd_version\t%lld\n"
        "cmd_get_time\t%.7lf\n"
        "cmd_set_time\t%.7lf\n"
        "cmd_delete_time\t%.7lf\n"
        "cmd_incr_time\t%.7lf\n"
        "max_cmd_get_time\t%.7lf\n"
        "max_cmd_set_time\t%.7lf\n"
        "max_cmd_delete_time\t%.7lf\n"
        "max_cmd_incr_time\t%.7lf\n"
        "bad_requests\t%lld\n"
#ifdef HINTS
        "words_per_request\t%lld %lld %lld %lld %lld %lld\n"
#endif
        "tot_aio_queries\t%lld\n"
        "active_aio_queries\t%lld\n"
        "expired_aio_queries\t%lld\n"
        "avg_aio_query_time\t%.6f\n"
        "limit_max_dynamic_memory\t%ld\n"
        "version\t" VERSION_STR " compiled at " __DATE__ " " __TIME__ " by gcc " __VERSION__ "\n",
        (long) (dyn_cur - dyn_first),
        (long) (dyn_last - dyn_first),
        log_readto_pos,
        log_readto_pos - jump_log_pos,
        binlog_load_time,
        log_pos,
        log_uncommitted,
        binlogname ? (strlen (binlogname) < 250 ? binlogname : "(too long)") : "(none)",
        log_first_ts,
        log_read_until,
        log_last_ts,
        max_binlog_size,
        index_users,
        header_size,
        engine_snapshot_size,
        engine_snapshot_name,
        index_load_time,
        indexed_users,
        get_new_users(),
        cur_users,
        get_del_by_LRU(),
        min_cache_time,
        get_del_by_LRU() ? total_cache_time * 1.0 / get_del_by_LRU() : 0,
        max_cache_time,
        friend_changes,
        MAX_CNT,
        getpid(),
        VERSION,
        (int)(sizeof (void *) * 8),
        static_memory,
        get_memory_used() + static_memory,
        (long)get_changes_memory(),
        (long)trp_get_memory(),
        cmd_get,
        cmd_set,
        cmd_delete,
        cmd_incr,
        cmd_global,
        cmd_get_user_info,
        cmd_stats,
        cmd_version,
        cmd_get_time,
        cmd_set_time,
        cmd_delete_time,
        cmd_incr_time,
        max_cmd_get_time,
        max_cmd_set_time,
        max_cmd_delete_time,
        max_cmd_incr_time,
        bad_requests,
#ifdef HINTS
        words_per_request[0],
        words_per_request[1],
        words_per_request[2],
        words_per_request[3],
        words_per_request[4],
        words_per_request[5],
#endif
        tot_aio_queries,
        active_aio_queries,
        expired_aio_queries,
        tot_aio_queries > 0 ? total_aio_time / tot_aio_queries : 0,
        max_memory + static_memory);
}

int memcache_stats (struct connection *c) {
  int len = hints_prepare_stats ();
  int len2 = prepare_stats (c, stats_buff + len, STATS_BUFF_SIZE - len);
  write_out (&c->Out, stats_buff, len + len2);
  write_out (&c->Out, "END\r\n", 5);
  return 0;
}


int hints_engine_wakeup (struct connection *c) {
  struct mcs_data *D = MCS_DATA(c);

  if (verbosity > 1) {
    fprintf (stderr, "hints_engine_wakeup (%p)\n", c);
  }

  c->generation = ++conn_generation;
  c->pending_queries = 0;
  c->status = conn_reading_query;
  assert (D->query_type == mct_get_resume);
  clear_connection_timeout (c);

  if (c->Out.total_bytes > 0) {
    c->flags |= C_WANTWR;
  }

  return 0;
}


int hints_engine_alarm (struct connection *c) {
  if (verbosity > 1) {
    fprintf (stderr, "hints_engine connection %d timeout alarm, %d queries pending, status=%d\n", c->fd, c->pending_queries, c->status);
  }
  assert (c->status == conn_wait_aio);
  c->flags |= C_INTIMEOUT;
  return hints_engine_wakeup (c);
}


/*
 *
 *    RPC interface
 *
 */

void tl_check_get (void) {
  assert (!index_mode);
  if (write_only) {
    tl_fetch_set_error_format (-4001, "Get operations are unsupported in write_only mode");
  }
}

int tl_fetch_bool (void) {
  if (tl_fetch_error()) {
    return 0;
  }
  int bool = tl_fetch_int();
  if (bool == TL_BOOL_TRUE) {
    return 1;
  }
  if (bool != TL_BOOL_FALSE) {
    tl_fetch_set_error_format (TL_ERROR_BAD_VALUE, "Bool excpected, %x found", bool);
  }
  return 0;
}

int tl_fetch_user_id (void) {
  if (tl_fetch_error()) {
    return 0;
  }
  int user_id = tl_fetch_int();
  if (!check_user_id (user_id)) {
    tl_fetch_set_error_format (TL_ERROR_BAD_VALUE, "Wrong server: user_id = %d, log_split_mod = %d, log_split_min = %d, log_split_max = %d", user_id, log_split_mod, log_split_min, log_split_max);
  }

  return user_id;
}

int tl_fetch_type (void) {
  if (tl_fetch_error()) {
    return 0;
  }
  int type = tl_fetch_int();
  if (!check_type (type)) {
    tl_fetch_set_error_format (TL_ERROR_BAD_VALUE, "Wrong object type %d", type);
  }
  return type;
}

int tl_fetch_object_id (void) {
  if (tl_fetch_error()) {
    return 0;
  }
  int object_id = tl_fetch_int();
  if (!check_object_id (object_id)) {
    tl_fetch_set_error_format (TL_ERROR_BAD_VALUE, "Wrong object id %d", object_id);
  }
  return object_id;
}

double tl_fetch_rating (void) {
  if (tl_fetch_error()) {
    return 0;
  }
  double val = tl_fetch_double();
  if (!check_rating (val)) {
    tl_fetch_set_error_format (TL_ERROR_BAD_VALUE, "Wrong rating %.6lf", val);
  }
  return val;
}

int tl_fetch_rating_num (void) {
  if (tl_fetch_error()) {
    return 0;
  }
  int num = tl_fetch_int();
  if (!check_rating_num (num)) {
    tl_fetch_set_error_format (TL_ERROR_BAD_VALUE, "Wrong rating num %d", num);
  }
  return num;
}


static inline void tl_store_bool (int res) {
  if (res == 1) {
    tl_store_int (TL_BOOL_TRUE);
  } else {
    tl_store_int (TL_BOOL_FALSE);
  }
}

static inline void tl_store_bool_stat (int res) {
  tl_store_int (TL_BOOL_STAT);
  tl_store_int (res > 0);
  tl_store_int (res <= 0);
  tl_store_int (0);
}


TL_DO_FUN(hints_nullify_rating)
  int res = do_nullify_user_rating (e->user_id);
  if (res < 0) {
    return res;
  }

  tl_store_bool (res);
TL_DO_FUN_END

TL_DO_FUN(hints_set_rating_state)
  int res = do_set_user_rating_state (e->user_id, e->rating_enabled);
  if (res < 0) {
    return res;
  }

  tl_store_bool (res);
TL_DO_FUN_END

TL_DO_FUN(hints_set_info)
  int res = do_set_user_info (e->user_id, e->info);
  if (res < 0) {
    return res;
  }

  tl_store_bool (res);
TL_DO_FUN_END

TL_DO_FUN(hints_set_winner)
  int res = do_user_object_winner (e->user_id, e->type, e->rating_num, e->winner, e->losers_cnt, e->losers);
  if (res < 0) {
    return res;
  }

  tl_store_bool (res);
TL_DO_FUN_END

TL_DO_FUN(hints_set_rating)
  int res = do_set_user_object_rating (e->user_id, e->type, e->object_id, e->rating, e->rating_num);
  if (res < 0) {
    return res;
  }

  tl_store_bool (res);
TL_DO_FUN_END

#ifdef HINTS
TL_DO_FUN(hints_set_text)
  int res = do_add_user_object (e->user_id, e->type, e->object_id, e->text_len, e->text);
  if (res < 0) {
    return res;
  }

  tl_store_bool (res);
TL_DO_FUN_END
#else
TL_DO_FUN(rating_add_object)
  int res = do_add_user_object (e->user_id, e->type, e->object_id);
  if (res < 0) {
    return res;
  }

  tl_store_bool (res);
TL_DO_FUN_END
#endif

TL_DO_FUN(hints_set_type)
  int res = do_set_user_object_type (e->user_id, e->type, e->object_id, e->new_type);
  if (res < 0) {
    return res;
  }

  tl_store_bool (res);
TL_DO_FUN_END

#ifdef HINTS
TL_DO_FUN(hints_set_text_global)
  int res = do_add_object_text (e->type, e->object_id, e->text_len, e->text);
  if (res < 0) {
    return res;
  }

  tl_store_bool_stat (res);
TL_DO_FUN_END
#endif

TL_DO_FUN(hints_set_type_global)
  int res = do_set_object_type (e->type, e->object_id, e->new_type);
  if (res < 0) {
    return res;
  }

  tl_store_bool_stat (res);
TL_DO_FUN_END

TL_DO_FUN(hints_get_info)
  int res = get_user_info (e->user_id);

  if (res >= -2) {
    tl_store_int (TL_MAYBE_TRUE);
    tl_store_int (res);
  } else {
    tl_store_int (TL_MAYBE_FALSE);
  }
TL_DO_FUN_END

TL_DO_FUN(hints_sort)
  int res = sort_user_objects (e->user_id, e->objects_cnt, e->objects, e->limit, e->rating_num, e->need_rand);
  if (res < 0) {
    return res;
  }

  tl_store_int (TL_VECTOR);
  tl_store_int (res);
  int i;
  for (i = 0; i < res; i++) {
    long long h = e->objects[i];
    tl_store_int (TYPE(h));
    tl_store_int (ID(h));
  }
TL_DO_FUN_END

#ifdef HINTS
TL_DO_FUN(hints_get_hints)
  int res = rpc_get_user_hints (e->user_id, e->query_len, e->query, e->type, e->limit, e->rating_num, e->need_rating, e->need_text, e->need_latin);
  if (res < 0) {
    return res;
  }
TL_DO_FUN_END
#else
TL_DO_FUN(rating_get_hints)
  int res = rpc_get_user_hints (e->user_id, e->exceptions_cnt, e->exceptions, e->type, e->limit, e->rating_num, e->need_rating, e->need_rand);
  if (res < 0) {
    return res;
  }
TL_DO_FUN_END
#endif

#ifdef HINTS
TL_DO_FUN(hints_get_object_text)
  char *text = NULL;
  int res = get_user_object_text (e->user_id, e->type, e->object_id, &text);
  if (res < 0) {
    return res;
  }

  if (res > 0) {
    tl_store_int (TL_MAYBE_TRUE);
    assert (text != NULL);
    tl_store_string (text, strlen (text));
  } else {
    tl_store_int (TL_MAYBE_FALSE);
  }
TL_DO_FUN_END
#endif

TL_DO_FUN(hints_delete_object)
  int res = do_del_user_object (e->user_id, e->type, e->object_id);
  if (res < 0) {
    return res;
  }

  tl_store_bool (res);
TL_DO_FUN_END

TL_DO_FUN(hints_delete_object_global)
  int res = do_del_object_text (e->type, e->object_id);
  if (res < 0) {
    return res;
  }

  tl_store_bool_stat (res);
TL_DO_FUN_END

TL_DO_FUN(hints_increment_rating)
  int res = do_increment_user_object_rating (e->user_id, e->type, e->object_id, e->cnt, e->rating_num);
  if (res < 0) {
    return res;
  }

  tl_store_bool (res);
TL_DO_FUN_END


TL_PARSE_FUN(hints_nullify_rating, void)
  e->user_id = tl_fetch_user_id();
TL_PARSE_FUN_END

TL_PARSE_FUN(hints_set_rating_state, void)
  e->user_id = tl_fetch_user_id();
  e->rating_enabled = tl_fetch_bool();
TL_PARSE_FUN_END

TL_PARSE_FUN(hints_set_info, void)
  e->user_id = tl_fetch_user_id();
  e->info = tl_fetch_int();
TL_PARSE_FUN_END

TL_PARSE_FUN(hints_set_winner, void)
  e->user_id = tl_fetch_user_id();
  e->type = tl_fetch_type();
  e->winner = tl_fetch_object_id();

  e->losers_cnt = tl_fetch_int();
  if (e->losers_cnt <= 0 || e->losers_cnt > 16000) {
    tl_fetch_set_error_format (TL_ERROR_BAD_VALUE, "Wrong number of losers %d", e->losers_cnt);
    return NULL;
  }

  extra->size += e->losers_cnt * sizeof (int);
  if (extra->size >= STATS_BUFF_SIZE) {
    tl_fetch_set_error_format (TL_ERROR_BAD_VALUE, "Number of losers is too large");
    return NULL;
  }

  int i;
  for (i = 0; i < e->losers_cnt; i++) {
    e->losers[i] = tl_fetch_object_id();
  }

  e->rating_num = tl_fetch_rating_num();
TL_PARSE_FUN_END

TL_PARSE_FUN(hints_set_rating, void)
  e->user_id = tl_fetch_user_id();
  e->type = tl_fetch_type();
  e->object_id = tl_fetch_object_id();
  e->rating_num = tl_fetch_rating_num();
  e->rating = tl_fetch_rating();
TL_PARSE_FUN_END

#ifdef HINTS
TL_PARSE_FUN(hints_set_text, void)
  e->user_id = tl_fetch_user_id();
  e->type = tl_fetch_type();
  e->object_id = tl_fetch_object_id();
  e->text_len = tl_fetch_string0 (e->text, STATS_BUFF_SIZE - extra->size - 1);
  extra->size += e->text_len + 1;

  if (!check_text_len (e->text_len)) {
    tl_fetch_set_error_format (TL_ERROR_BAD_VALUE, "Wrong text length %d", e->text_len);
    return NULL;
  }
  if (extra->size >= STATS_BUFF_SIZE) {
    tl_fetch_set_error_format (TL_ERROR_BAD_VALUE, "Text is too long");
    return NULL;
  }
TL_PARSE_FUN_END
#else
TL_PARSE_FUN(rating_add_object, void)
  e->user_id = tl_fetch_user_id();
  e->type = tl_fetch_type();
  e->object_id = tl_fetch_object_id();
TL_PARSE_FUN_END
#endif

TL_PARSE_FUN(hints_set_type, void)
  e->user_id = tl_fetch_user_id();
  e->type = tl_fetch_type();
  e->object_id = tl_fetch_object_id();
  e->new_type = tl_fetch_type();
TL_PARSE_FUN_END

#ifdef HINTS
TL_PARSE_FUN(hints_set_text_global, void)
  e->type = tl_fetch_type();
  e->object_id = tl_fetch_object_id();
  e->text_len = tl_fetch_string0 (e->text, STATS_BUFF_SIZE - extra->size - 1);
  extra->size += e->text_len + 1;

  if (!check_text_len (e->text_len)) {
    tl_fetch_set_error_format (TL_ERROR_BAD_VALUE, "Wrong text length %d", e->text_len);
    return NULL;
  }
  if (extra->size >= STATS_BUFF_SIZE) {
    tl_fetch_set_error_format (TL_ERROR_BAD_VALUE, "Text is too long");
    return NULL;
  }
TL_PARSE_FUN_END
#endif

TL_PARSE_FUN(hints_set_type_global, void)
  e->type = tl_fetch_type();
  e->object_id = tl_fetch_object_id();
  e->new_type = tl_fetch_type();
TL_PARSE_FUN_END

TL_PARSE_FUN(hints_get_info, void)
  tl_check_get();
  e->user_id = tl_fetch_user_id();
TL_PARSE_FUN_END

TL_PARSE_FUN(hints_sort, int need_rand)
  tl_check_get();
  e->user_id = tl_fetch_user_id();
  e->limit = tl_fetch_nonnegative_int();
  e->rating_num = tl_fetch_rating_num();
  e->need_rand = need_rand;
  e->objects_cnt = tl_fetch_nonnegative_int();
  extra->size += e->objects_cnt * sizeof (long long);

  if (extra->size >= STATS_BUFF_SIZE) {
    tl_fetch_set_error_format (TL_ERROR_BAD_VALUE, "Number of objects is too large");
    return NULL;
  }
  
  int i;
  for (i = 0; i < e->objects_cnt; i++) {
    int type = tl_fetch_type();
    int object_id = tl_fetch_object_id();
    e->objects[i] = TYPE_ID(type, object_id);
  }
TL_PARSE_FUN_END

#ifdef HINTS
TL_PARSE_FUN(hints_get_hints, int need_rating, int need_text, int need_latin)
  tl_check_get();
  e->user_id = tl_fetch_user_id();
  e->type = tl_fetch_int();
  if (e->type != -1 && !check_type (e->type)) {
    tl_fetch_set_error_format (TL_ERROR_BAD_VALUE, "Wrong parameter type = %d", e->type);
  }
  e->limit = tl_fetch_nonnegative_int();
  e->rating_num = tl_fetch_rating_num();
  e->need_rating = need_rating;
  e->need_text = need_text;
  e->need_latin = need_latin;
  e->query_len = tl_fetch_string0 (e->query, STATS_BUFF_SIZE - extra->size - 1);
  extra->size += e->query_len + 1;

  if (!check_text_len (e->query_len)) {
    tl_fetch_set_error_format (TL_ERROR_BAD_VALUE, "Wrong query length %d", e->query_len);
    return NULL;
  }
  if (extra->size >= STATS_BUFF_SIZE) {
    tl_fetch_set_error_format (TL_ERROR_BAD_VALUE, "Text is too long");
    return NULL;
  }
TL_PARSE_FUN_END
#else
TL_PARSE_FUN(rating_get_hints, int need_rating, int need_rand)
  tl_check_get();
  e->user_id = tl_fetch_user_id();
  e->type = tl_fetch_int();
  if (e->type != -1 && !check_type (e->type)) {
    tl_fetch_set_error_format (TL_ERROR_BAD_VALUE, "Wrong parameter type = %d", e->type);
  }
  e->limit = tl_fetch_nonnegative_int();
  e->rating_num = tl_fetch_rating_num();
  e->need_rating = need_rating;
  e->need_rand = need_rand;
  
  e->exceptions_cnt = tl_fetch_int();
  if (e->exceptions_cnt < 0 || e->exceptions_cnt > 16000) {
    tl_fetch_set_error_format (TL_ERROR_BAD_VALUE, "Wrong number of exceptions");
    return NULL;
  }

  extra->size += e->exceptions_cnt * sizeof (long long);
  if (extra->size >= STATS_BUFF_SIZE) {
    tl_fetch_set_error_format (TL_ERROR_BAD_VALUE, "Number of exceptions is too large");
    return NULL;
  }

  int i;
  for (i = 0; i < e->exceptions_cnt; i++) {
    int type = tl_fetch_type();
    int object_id = tl_fetch_object_id();
    e->exceptions[i] = TYPE_ID(type, object_id);
  }
TL_PARSE_FUN_END
#endif

#ifdef HINTS
TL_PARSE_FUN(hints_get_object_text, void)
  tl_check_get();
  e->user_id = tl_fetch_user_id();
  e->type = tl_fetch_type();
  e->object_id = tl_fetch_object_id();
TL_PARSE_FUN_END
#endif

TL_PARSE_FUN(hints_delete_object, void)
  e->user_id = tl_fetch_user_id();
  e->type = tl_fetch_type();
  e->object_id = tl_fetch_object_id();
TL_PARSE_FUN_END

TL_PARSE_FUN(hints_delete_object_global, void)
  e->type = tl_fetch_type();
  e->object_id = tl_fetch_object_id();
TL_PARSE_FUN_END

TL_PARSE_FUN(hints_increment_rating, void)
  e->user_id = tl_fetch_user_id();
  e->type = tl_fetch_type();
  e->object_id = tl_fetch_object_id();
  e->cnt = tl_fetch_int();
  e->rating_num = tl_fetch_rating_num();
TL_PARSE_FUN_END


struct tl_act_extra *hints_parse_function (long long actor_id) {
  if (actor_id != 0) {
    tl_fetch_set_error ("Hints only supports actor_id = 0", TL_ERROR_WRONG_ACTOR_ID);
    return NULL;
  }

  int op = tl_fetch_int();
  if (tl_fetch_error()) {
    tl_fetch_set_error_format (TL_ERROR_BAD_VALUE, "Request is empty");
    return NULL;
  }

  switch (op) {
#ifdef HINTS
    case TL_HINTS_NULLIFY_RATING:
      return tl_hints_nullify_rating();
    case TL_HINTS_SET_RATING_STATE:
      return tl_hints_set_rating_state();
    case TL_HINTS_SET_INFO:
      return tl_hints_set_info();
    case TL_HINTS_SET_WINNER:
      return tl_hints_set_winner();
    case TL_HINTS_SET_RATING:
      return tl_hints_set_rating();
    case TL_HINTS_SET_TEXT:
      return tl_hints_set_text();
    case TL_HINTS_SET_TYPE:
      return tl_hints_set_type();
    case TL_HINTS_SET_TEXT_GLOBAL:
      return tl_hints_set_text_global();
    case TL_HINTS_SET_TYPE_GLOBAL:
      return tl_hints_set_type_global();
    case TL_HINTS_GET_INFO:
      return tl_hints_get_info();
    case TL_HINTS_SORT:
      return tl_hints_sort (0);
    case TL_HINTS_GET_RANDOM:
      return tl_hints_sort (1);
    case TL_HINTS_GET_HINTS:
      return tl_hints_get_hints (0, 0, 0);
    case TL_HINTS_GET_HINTS_RATING:
      return tl_hints_get_hints (1, 0, 0);
    case TL_HINTS_GET_HINTS_TEXT:
      return tl_hints_get_hints (0, 1, 0);
    case TL_HINTS_GET_HINTS_FULL:
      return tl_hints_get_hints (1, 1, 0);
    case TL_HINTS_GET_HINTS_LATIN:
      return tl_hints_get_hints (0, 0, 1);
    case TL_HINTS_GET_HINTS_LATIN_RATING:
      return tl_hints_get_hints (1, 0, 1);
    case TL_HINTS_GET_HINTS_LATIN_TEXT:
      return tl_hints_get_hints (0, 1, 1);
    case TL_HINTS_GET_HINTS_LATIN_FULL:
      return tl_hints_get_hints (1, 1, 1);
    case TL_HINTS_GET_OBJECT_TEXT:
      return tl_hints_get_object_text();
    case TL_HINTS_DELETE_OBJECT:
      return tl_hints_delete_object();
    case TL_HINTS_DELETE_OBJECT_GLOBAL:
      return tl_hints_delete_object_global();
    case TL_HINTS_INCREMENT_RATING:
      return tl_hints_increment_rating();
#else
    case TL_RATING_NULLIFY_RATING:
      return tl_hints_nullify_rating();
    case TL_RATING_SET_RATING_STATE:
      return tl_hints_set_rating_state();
    case TL_RATING_SET_INFO:
      return tl_hints_set_info();
    case TL_RATING_SET_WINNER:
      return tl_hints_set_winner();
    case TL_RATING_SET_RATING:
      return tl_hints_set_rating();
    case TL_RATING_ADD_OBJECT:
      return tl_rating_add_object();
    case TL_RATING_SET_TYPE:
      return tl_hints_set_type();
    case TL_RATING_SET_TYPE_GLOBAL:
      return tl_hints_set_type_global();
    case TL_RATING_GET_INFO:
      return tl_hints_get_info();
    case TL_RATING_SORT:
      return tl_hints_sort (0);
    case TL_RATING_GET_RANDOM:
      return tl_hints_sort (1);
    case TL_RATING_GET_HINTS:
      return tl_rating_get_hints (0, 0);
    case TL_RATING_GET_HINTS_RATING:
      return tl_rating_get_hints (1, 0);
    case TL_RATING_GET_RANDOM_HINTS:
      return tl_rating_get_hints (0, 1);
    case TL_RATING_GET_RANDOM_HINTS_RATING:
      return tl_rating_get_hints (1, 1);
    case TL_RATING_DELETE_OBJECT:
      return tl_hints_delete_object();
    case TL_RATING_DELETE_OBJECT_GLOBAL:
      return tl_hints_delete_object_global();
    case TL_RATING_INCREMENT_RATING:
      return tl_hints_increment_rating();
#endif
  }

  tl_fetch_set_error_format (TL_ERROR_UNKNOWN_FUNCTION_ID, "Unknown function %08x", op);
  return NULL;
}


/*
 *
 *      SERVER
 *
 */


void reopen_logs (void) {
  int fd;
  fflush (stdout);
  fflush (stderr);
  if ((fd = open ("/dev/null", O_RDWR, 0)) != -1) {
    dup2 (fd, 0);
    dup2 (fd, 1);
    dup2 (fd, 2);
    if (fd > 2) {
      assert (close (fd) >= 0);
    }
  }
  if (logname && (fd = open (logname, O_WRONLY|O_APPEND|O_CREAT, 0640)) != -1) {
    dup2 (fd, 1);
    dup2 (fd, 2);
    if (fd > 2) {
      assert (close (fd) >= 0);
    }
  }
  if (verbosity > 0) {
    fprintf (stderr, "logs reopened.\n");
  }
}

static void sigint_immediate_handler (const int sig) {
  const char message[] = "SIGINT handled immediately.\n";
  kwrite (2, message, sizeof (message) - (size_t)1);

  exit (1);
}

static void sigterm_immediate_handler (const int sig) {
  const char message[] = "SIGTERM handled immediately.\n";
  kwrite (2, message, sizeof (message) - (size_t)1);

  exit (1);
}

static void sigint_handler (const int sig) {
  const char message[] = "SIGINT handled.\n";
  kwrite (2, message, sizeof (message) - (size_t)1);

  pending_signals |= (1ll << sig);
  signal (sig, sigint_immediate_handler);
}

static void sigterm_handler (const int sig) {
  const char message[] = "SIGTERM handled.\n";
  kwrite (2, message, sizeof (message) - (size_t)1);

  pending_signals |= (1 << sig);
  signal (sig, sigterm_immediate_handler);
}

static void sighup_handler (const int sig) {
  const char message[] = "got SIGHUP.\n";
  kwrite (2, message, sizeof (message) - (size_t)1);

  pending_signals |= (1ll << sig);
  signal (sig, sighup_handler);
}

static void sigusr1_handler (const int sig) {
  const char message[] = "got SIGUSR1, rotate logs.\n";
  kwrite (2, message, sizeof (message) - (size_t)1);

  pending_signals |= (1ll << sig);
  signal (sig, sigusr1_handler);
}

static void sigpoll_handler (const int sig) {
  sigpoll_cnt++;
  signal (SIGPOLL, sigpoll_handler);
}

void cron (void) {
  flush_binlog();
}

int sfd;

void start_server (void) {
  int i;
  int prev_time;

  init_epoll();
  init_netbuffers();

  prev_time = 0;

  if (!sfd) {
    sfd = server_socket (port, settings_addr, backlog, 0);
  }

  if (sfd < 0) {
    fprintf (stderr, "cannot open server socket at port %d: %m\n", port);
    exit (3);
  }

  if (verbosity) {
    fprintf (stderr, "created listening socket at %s:%d, fd=%d\n", conv_addr (settings_addr.s_addr, buf), port, sfd);
  }

  if (daemonize) {
    setsid();
  }

  if (change_user (username) < 0) {
    fprintf (stderr, "fatal: cannot change user to %s\n", username ? username : "(none)");
    exit (1);
  }

  if (binlogname && !binlog_disabled) {
    assert (append_to_binlog (Binlog) == log_readto_pos);
  }

  tl_parse_function = hints_parse_function;
  tl_aio_timeout = 0.7;
  init_listening_connection (sfd, &ct_rpc_server, &rpc_methods);

  if (binlog_disabled && binlog_fd >= 0) {
    epoll_pre_event = read_new_events;
  }

  signal (SIGINT, sigint_handler);
  signal (SIGTERM, sigterm_handler);
  signal (SIGUSR1, sigusr1_handler);
  signal (SIGPIPE, SIG_IGN);
  signal (SIGPOLL, sigpoll_handler);
  if (daemonize) {
    signal (SIGHUP, sighup_handler);
    reopen_logs();
  }

  if (verbosity) {
    fprintf (stderr, "Server started\n");
  }

  for (i = 0; !(pending_signals & ~((1ll << SIGUSR1) | (1ll << SIGHUP))); i++) {
    if (verbosity > 0 && !(i & 255)) {
      fprintf (stderr, "epoll_work(): %d out of %d connections, network buffers: %d used, %d out of %d allocated\n",
         active_connections, maxconn, NB_used, NB_alloc, NB_max);
    }
    fetch_binlog_data (binlog_disabled ? log_cur_pos() : log_write_pos ());
    epoll_work (17);

    if (sigpoll_cnt > 0) {
      if (verbosity > 1) {
        fprintf (stderr, "after epoll_work(), sigpoll_cnt=%d\n", sigpoll_cnt);
      }
      sigpoll_cnt = 0;
    }

    if (pending_signals & (1ll << SIGHUP)) {
      pending_signals &= ~(1ll << SIGHUP);

      sync_binlog (2);
    }

    if (pending_signals & (1ll << SIGUSR1)) {
      pending_signals &= ~(1ll << SIGUSR1);

      reopen_logs();
      sync_binlog (2);
    }

    if (!NOAIO) {
      check_all_aio_completions();
    }
    tl_restart_all_ready();

    if (now != prev_time) {
      prev_time = now;
      cron();
    }

    if (epoll_pre_event) {
      epoll_pre_event();
    }

    flush_binlog ();
    cstatus_binlog_pos (binlog_disabled ? log_cur_pos() : log_write_pos(), binlog_disabled);

    if (quit_steps && !--quit_steps) break;
  }

  if (verbosity > 0 && pending_signals) {
    fprintf (stderr, "Quitting because of pending signals = %llx\n", pending_signals);
  }

  epoll_close (sfd);
  assert (close (sfd) >= 0);

  flush_binlog_last();
  sync_binlog (2);
}


/*
 *
 *    MAIN
 *
 */

void usage (void) {
  printf ("usage: %s [options] <index-file>\n"
    "\t" VERSION_STR " compiled at " __DATE__ " " __TIME__ " by gcc " __VERSION__ " "
#ifdef __LP64__
    "64-bit"
#else
    "32-bit"
#endif
    " after commit " COMMIT "\n"
    "\t%s\n",
    progname,
    index_mode ?
    "Generates new index of hints using given old index": 
    "Performs hints retrieval queries using given index");

  parse_usage();
  exit (2);
}

static int default_max_cnt_type = -1;

int hints_parse_option (int val) {
  switch (val) {
    case 'D': {
      dump_mode = 1;
      int d_type = atoi (optarg);
      if (!check_type (d_type)) {
        usage ();
        return 2;
      }
#ifdef NOHINTS
      assert (dump_type[d_type] == 0);
#endif
      dump_type[d_type]++;
      break;
    }
    case 'e':
      estimate_users = atoi (optarg);
      break;
    case 'F':
      default_max_cnt_type = atoi (optarg);
      break;
    case 'L': {
      int type, max_cnt_t;
      assert (sscanf (optarg, "%d,%d", &type, &max_cnt_t) >= 2);
      assert (check_type (type));
      max_cnt_type[type] = max_cnt_t;
      break;
    }
    case 'm':
      max_memory = atoi (optarg);
      if (max_memory < 1) {
        max_memory = 1;
      }
      max_memory <<= 20;
      break;
    case 'M':
      MAX_CNT = atoi (optarg);
      assert (1 <= MAX_CNT && (MAX_CNT + 1) * (long long)sizeof (rating) * (long long)MAX_RATING_NUM <= 2000000000);
      break;
    case 'N':
      RATING_NORM = atoi (optarg);
      assert (1 <= RATING_NORM && RATING_NORM <= MAX_RATING_NORM);
      RATING_NORM *= 60 * 60;
      break;
    case 'o':
      index_mode = 1;
      new_binlog_name = optarg;
      break;
    case 'q':
      MAX_RATING = atoi (optarg);
      break;
    case 'R':
      rating_num = atoi (optarg);
      assert (1 <= rating_num && rating_num <= MAX_RATING_NUM);
      break;
    case 'A':
      keep_not_alive = 1;
      break;
    case 'f':
      no_changes = 1;
      break;
#ifdef NOHINTS
    case 'g':
      add_on_increment = 0;
      break;
#endif
    case 'I':
      immediate_mode = 1;
      break;
    case 'i':
      index_mode = 1;
      break;
    case 'k':
      if (mlockall (MCL_CURRENT | MCL_FUTURE) != 0) {
        fprintf (stderr, "error: fail to lock paged memory\n");
      }
      break;
    case 'w':
      write_only = 1;
      break;
    case 'z':
      fading = 0;
      break;
    default:
      return -1;
  }
  return 0;
}

int main (int argc, char *argv[]) {
  int i;

  dl_set_debug_handlers();
  progname = argv[0];
  now = time (NULL);

  index_mode = 0;
  if (strstr (progname, "hints-index") != NULL) {
    index_mode = 1;
  }

  for (i = 1; i <= 255; i++) {
    max_cnt_type[i] = -1;
  }

  remove_parse_option (204);
  parse_option ("dump-type", required_argument, NULL, 'D', "<dump-type> dump events of specified type into file dump<engine-number>.<dump-type>, forces index mode");
  parse_option ("estimate-users", required_argument, NULL, 'e', "<user-count> sets estimated number of users (default is %d)", estimate_users);
  parse_option ("max-objects-per-type", required_argument, NULL, 'F', "<max-objects-per-type> sets maximal default number of objects for any type (default is max-objects)");
  parse_option ("object-type-limit", required_argument, NULL, 'L', "<object-type,limit> sets maximal number of objects of specified type");
  parse_option ("memory-limit", required_argument, NULL, 'm', "<memory-limit> sets maximal size of used memory not including zmemory in mebibytes");
  parse_option ("object-limit", required_argument, NULL, 'M', "<max-objects> sets maximal number of objects for one user (default is %d)", MAX_CNT);
  parse_option ("rating-norm", required_argument, NULL, 'N', "<rating-norm> sets half-life of fading rating in hours (default is %d, maximal is %d)", RATING_NORM / 3600, MAX_RATING_NORM);
  parse_option ("new-binlog-name", required_argument, NULL, 'o', "<new-binlog-name> generate new binlog with given name, forces index mode");
  parse_option ("max-rating", required_argument, NULL, 'q', "<max-rating> sets maximal absolute value of object rating (default is %d)", MAX_RATING);
  parse_option ("ratings-count", required_argument, NULL, 'R', "<ratings-count> sets number of ratings to store for each object (default is %d, maximal is %d)", rating_num, MAX_RATING_NUM);
  parse_option ("keep-not-alive", no_argument, NULL, 'A', "don't delete inactive users, index mode only");
  if (!index_mode) {
    parse_option ("no-changes", no_argument, NULL, 'f', "don't save new events in memory, only write them to binlog");
  }
#ifdef NOHINTS
  parse_option ("do-not-add-on-increment", no_argument, NULL, 'g', "don't add new objects when incrementing rating");
#endif
  parse_option ("immediate-mode", no_argument, NULL, 'I', "immediately apply all changes (requires user metafile loading)");
  if (!index_mode) {
    parse_option ("index-mode", no_argument, NULL, 'i', "run in index mode");
  }
  parse_option ("lock-memory", no_argument, NULL, 'k', "lock paged memory");
  if (!index_mode) {
    parse_option ("write-only", no_argument, NULL, 'w', "don't save changes in memory and don't answer queries");
  }
  parse_option ("discrete-rating", no_argument, NULL, 'z', "use discrete not fading rating");

  parse_engine_options_long (argc, argv, hints_parse_option);
  if (argc != optind + 1) {
    usage();
    return 2;
  }

  if (default_max_cnt_type == -1) {
    default_max_cnt_type = MAX_CNT;
  }
  assert (0 <= default_max_cnt_type && default_max_cnt_type <= MAX_CNT);

  for (i = 1; i <= 255; i++) {
    if (max_cnt_type[i] == -1) {
      max_cnt_type[i] = default_max_cnt_type;
    }

    assert (0 <= max_cnt_type[i] && max_cnt_type[i] <= MAX_CNT);
  }

  assert (rating_num + 1 <= 1000000000 / MAX_CNT);

  if (dump_mode) {
    index_mode = 1;
  }

  if (index_mode) {
    write_only = 0;
    no_changes = 0;
    binlog_disabled = 1;
  }

  if (verbosity > 0) {
    fprintf (stderr, "index_mode = %d\n", index_mode);
  }

  dynamic_data_buffer_size = (1 << 18); //18 for AIO

  init_dyn_data();

  if (!username && maxconn == MAX_CONNECTIONS && geteuid()) {
    maxconn = 1000; //not for root
  }

  if (raise_file_rlimit (maxconn + 16) < 0) {
    fprintf (stderr, "fatal: cannot raise open file limit to %d\n", maxconn + 16);
    exit (1);
  }

  if (!index_mode) {
    if (port < PRIVILEGED_TCP_PORTS) {
      sfd = server_socket (port, settings_addr, backlog, 0);
      if (sfd < 0) {
        fprintf (stderr, "cannot open server socket at port %d: %m\n", port);
        exit (1);
      }
    }
  }

  aes_load_pwd_file (NULL);

  if (change_user (username) < 0 && !interactive) {
    fprintf (stderr, "fatal: cannot change user to %s\n", username ? username : "(none)");
    exit (1);
  }

  index_name = strdup (argv[optind]);

  if (engine_preload_filelist (argv[optind], binlogname) < 0) {
    fprintf (stderr, "cannot open binlog files for %s\n", binlogname ? binlogname : argv[optind]);
    exit (1);
  }

  init_common_data (0, index_mode ? CD_INDEXER : CD_ENGINE);
  cstatus_binlog_name (engine_replica->replica_prefix);

  //Snapshot reading
  Snapshot = open_recent_snapshot (engine_snapshot_replica);

  if (Snapshot) {
    engine_snapshot_name = Snapshot->info->filename;
    engine_snapshot_size = Snapshot->info->file_size;

    if (verbosity) {
      fprintf (stderr, "load index file %s (size %lld)\n", engine_snapshot_name, engine_snapshot_size);
    }
  } else {
    engine_snapshot_name = NULL;
    engine_snapshot_size = 0;
  }

  index_load_time = -mytime();

  i = init_all (Snapshot);

  index_load_time += mytime();

  if (i < 0) {
    fprintf (stderr, "fatal: error %d while loading index file %s\n", i, engine_snapshot_name);
    exit (1);
  }

  if (verbosity) {
    fprintf (stderr, "load index: done, jump_log_pos=%lld, alloc_mem=%ld, time %.06lfs\n",
       jump_log_pos, (long) get_memory_used(), index_load_time);
  }

  static_memory = get_memory_used() - htbl_get_memory() - htbl_vct_get_memory() - trp_get_memory() - chg_list_get_memory();
  max_memory -= static_memory;
  assert ("Not enough memory " && max_memory > 10485760);

  log_ts_interval = 3;

  //Binlog reading
  Binlog = open_binlog (engine_replica, jump_log_pos);
  if (!Binlog) {
    fprintf (stderr, "fatal: cannot find binlog for %s, log position %lld\n", engine_replica->replica_prefix, jump_log_pos);
    exit (1);
  }

  binlogname = Binlog->info->filename;

  if (verbosity) {
    fprintf (stderr, "replaying binlog file %s (size %lld)\n", binlogname, Binlog->info->file_size);
  }
  binlog_load_time = -mytime();

  clear_log();
  init_log_data (jump_log_pos, jump_log_ts, jump_log_crc32);

  if (verbosity) {
    fprintf (stderr, "replay log events started\n");
  }

  i = replay_log (0, 1);

  if (verbosity) {
    fprintf (stderr, "replay log events finished\n");
  }

  binlog_load_time += mytime();
  binlog_loaded_size = log_readto_pos - jump_log_pos;

  if (!binlog_disabled) {
    clear_read_log();
  }

  if (i == -2) {
    long long true_readto_pos = log_readto_pos - Binlog->info->log_pos + Binlog->offset;
    fprintf (stderr, "REPAIR: truncating %s at log position %lld (file position %lld)\n", Binlog->info->filename, log_readto_pos, true_readto_pos);
    if (truncate (Binlog->info->filename, true_readto_pos) < 0) {
      perror ("truncate()");
      exit (2);
    }
  } else if (i < 0) {
    fprintf (stderr, "fatal: error reading binlog\n");
    exit (1);
  }

  if (verbosity) {
    fprintf (stderr, "replay binlog file: done, log_pos=%lld, alloc_mem=%ld, time %.06lfs\n",
             (long long) log_pos, (long) get_memory_used(), binlog_load_time);
  }

  clear_write_log();

  start_time = time (NULL);

  if (index_mode) {
    save_index();

    if (verbosity) {
      int len = hints_prepare_stats();
      stats_buff[len] = 0;
      fprintf (stderr, "%s\n", stats_buff);
    }

    free_all();
    return 0;
  }

  update_user_info();

  start_server();

  free_all();
  return 0;
}

