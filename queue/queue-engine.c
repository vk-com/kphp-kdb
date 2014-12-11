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
#include <netdb.h>

#include "crc32.h"
#include "kdb-data-common.h"
#include "net-events.h"
#include "net-buffers.h"
#include "server-functions.h"
#include "net-connections.h"
#include "net-memcache-server.h"
#include "net-memcache-client.h"
#include "net-crypto-aes.h"
#include "net-http-server.h"
#include "word-split.h"
#include "stemmer-new.h"
#include "resolver.h"

#include "net-rpc-server.h"
#include "net-rpc-client.h"
#include "net-rpc-common.h"


#include "md5.h"
#include "net-http-server.h"
#include "net-crypto-aes.h"

#include "queue-data.h"

#define MAX_VALUE_LEN (1 << 20)

#define VERSION "0.5"
#define VERSION_STR "queue "VERSION

#define TCP_PORT 11211
#define UDP_PORT 11211

/*
 *
 *    MEMCACHED PORT
 *
 */


int port = TCP_PORT, udp_port = UDP_PORT;

int watchcat_port = 6670, engine_id = 0, engine_n = 1000,
    news_port = 6671;

long max_memory = MAX_MEMORY;

struct in_addr settings_addr;
int interactive = 0;

volatile int sigpoll_cnt;

char *hostname = "localhost";


long long cmd_get, cmd_set, cmd_stats, cmd_version;
long long rpc_failed, rpc_sent, rpc_received, rpc_received_news_subscr, rpc_received_news_redirect;
double cmd_get_time = 0.0, cmd_set_time = 0.0, load_time;
double max_cmd_get_time = 0.0, max_cmd_set_time = 0.0;
char max_cmd_get_query[3000], max_cmd_set_query[3000];
int header_size;

long long rpc_received_size, rpc_sent_size, http_sent_size, http_sent;
long long rpc_received_news_redirect_size, rpc_received_news_subscr_size;
double cmd_http_time = 0.0;
double max_cmd_http_time = 0.0;
char max_cmd_http_query[3000];

#define STATS_BUFF_SIZE (1 << 14)
int stats_buff_len;
char stats_buff[STATS_BUFF_SIZE];
int queue_prepare_stats (void);

long long http_queries_ok, http_queries_delayed, http_failed[4];
long long buff_overflow_cnt;
int pending_http_queries;
long long sent_queries;

conn_type_t ct_queue_engine_server = {
  .magic = CONN_FUNC_MAGIC,
  .title = "queue_engine_server",
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
  .wakeup = server_failed,
  .alarm = server_failed,
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

struct memcache_server_functions memcache_methods = {
  .execute = mcs_execute,
  .mc_store = memcache_store,
  .mc_get_start = mcs_get_start,
  .mc_get = memcache_get,
  .mc_get_end = mcs_get_end,
  .mc_incr = mcs_incr,
  .mc_delete = mcs_delete,
  .mc_version = memcache_version,
  .mc_stats = memcache_stats,
  .mc_check_perm = mcs_default_check_perm,
  .mc_init_crypto = mcs_init_crypto
};

int rpcs_execute (struct connection *c, int op, int len);

struct rpc_server_functions queue_rpc_server = {
  .execute = rpcs_execute,
  .check_ready = server_check_ready,
  .flush_packet = rpcs_flush_packet,
  .rpc_check_perm = rpcs_default_check_perm,
  .rpc_init_crypto = rpcs_init_crypto,
  .memcache_fallback_type = &ct_queue_engine_server,
  .memcache_fallback_extra = &memcache_methods
};


char buf[MAX_VALUE_LEN + 50];

#define MAX_WID_LEN 10000
long long wids[MAX_WID_LEN];
int wn;

#define MAX_SUBS_LEN 20000
int subs[MAX_SUBS_LEN];
int sn;

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

static inline void safe_read_in (netbuffer_t *H, char *data, int len) {
  assert (read_in (H, data, len) == len);
  int i;
  for (i = 0; i < len; i++) {
    if (data[i] == 0) {
      data[i] = ' ';
    }
  }
}

char query[3000];

#define QRETURN(x, y)                        \
  cmd_time += mytime() - 1e-6;               \
  cmd_ ## x ## _time += cmd_time;            \
  if (cmd_time > max_cmd_ ## x ## _time &&   \
      now > start_time + 600) {              \
    strcpy (max_cmd_ ## x ## _query, query); \
    max_cmd_ ## x ## _time = cmd_time;       \
  }                                          \
  return y;


int memcache_store (struct connection *c, int op, const char *old_key, int old_key_len, int flags, int delay, int size) {
  cmd_set++;
  double cmd_time = -mytime();

  if (verbosity > 1) {
    fprintf (stderr, "memcache_store: key='%s', key_len=%d, value_len=%d\n", old_key, old_key_len, size);
  }
  dl_log_add (LOG_HISTORY, 1, "memcache_store: key='%s', key_len=%d, value_len=%d\n", old_key, old_key_len, size);
  sprintf (query, "key='%s', value_len=%d", old_key, size);

  if (size + 1 < MAX_VALUE_LEN) {
    char *key;
    int key_len;

    eat_at (old_key, old_key_len, &key, &key_len);

    if (key_len >= 5 && !strncmp (key, "queue", 5)) {
      int add = 0, x, y;
      if (sscanf(key + 5, "%d,%d%n", &x, &y, &add) != 2) {
        x = y = add = 0;
      }
      if (key[key_len - 1] != ')' || key[5 + add] != '(' || (key_len - 6 >= STATS_BUFF_SIZE)) {
        QRETURN(set, -2);
      }

      safe_read_in (&c->In, buf, size);
      buf[size] = 0;

      int len = key_len - 7 - add;
      memcpy (stats_buff, key + 6 + add, len);
      stats_buff[len] = 0;

      if (!do_add_event (stats_buff, len, buf, size, x, y, TTL_EVENT)) {
        QRETURN(set, 0)
      }

      QRETURN(set, 1);
    }

    if (!strcmp (key, "entry")) {
      safe_read_in (&c->In, buf, size);
      buf[size] = 0;
      char *s = buf;
      wn = 0;
      while (*s) {
        long long x = 0;
        int f = 0;
        if (*s == '-') {
          f = 1;
          s++;
        }
        while (*s >= '0' && *s <= '9') {
          x = x * 10 + *s++ - '0';
        }
        if (f) {
          x = -x;
        }

        if (wn < MAX_WID_LEN) {
          wids[wn++] = x;
        }

        if (*s != ',') {
          break;
        }
        s++;
      }
      if (*s != ';') {
        QRETURN(set, 0);
      }
      s++;

      add_event_to_watchcats (wids, wn, s);

      QRETURN(set, 1);
    }

    if (key_len >= 10 && !strncmp (key, "news_local", 10)) {
      ll id;
      int n, x, y, ttl;
      if (sscanf (key + 10, "%lld,%d,%d,%d,%d", &id, &x, &y, &ttl, &n) == 5) {
        read_in (&c->In, buf, size); // no safe_read_in
        buf[size] = 0;
        if (size >= (int)sizeof (pli) * n) {
          add_event_to_news (id, x, y, ttl, (pli *)buf, n, buf + sizeof (pli) * n, 0);
          QRETURN(set, 1);
        }

        QRETURN(set, 0);
      }

      QRETURN(set, -2);
    }

    if (key_len >= 4 && !strncmp (key, "news", 4)) {
      ll id;
      int x, y;
      if (sscanf (key + 4, "%lld,%d,%d", &id, &x, &y) == 3) {
        safe_read_in (&c->In, buf, size);

        buf[size] = 0;
        dbg ("NEWS %lld %d %d: [%s]\n", id, x, y, buf);

        redirect_news (id, x, y, TTL_NEWS_EVENT, buf, size);
        QRETURN(set, 1);
      }

      QRETURN(set, -2);
    }

    int tp = -1;
    if (key_len >= 15 && !strncmp (key, "add_news_subscr", 15)) {
      tp = 0;
    } else if (key_len >= 15 && !strncmp (key, "del_news_subscr", 15)) {
      tp = 1;
    } else if (key_len >= 10 && !strncmp (key, "subscr_add", 10)) {
      tp = 3;
    } else if (key_len >= 10 && !strncmp (key, "subscr_del", 10)) {
      tp = 4;
    } else if (key_len >= 6 && !strncmp (key, "subscr", 6)) {
      tp = 2;
    }

    static int _len[] = {15, 15, 6, 10, 10};

    if (tp != -1) {
      read_in (&c->In, buf, size); // no safe_read_in
      buf[size] = 0;
      char *s = key + _len[tp];
      ll id;
      int is_rev = 0;
      if (*s == '!') {
        is_rev = 1;
        s++;
      }

      if (sscanf (s, "%lld", &id) == 1) {
        int res = 0;

        if (tp >= 2) {
          dbg ("%lld[%lld] %% %d == %d?\n", id, dl_abs(id), engine_n, engine_id);
          if (dl_abs (id) % engine_n == engine_id) {
            dbg ("OK [%s]\n", buf);
            sn = 0;
            int i, mul = 1;
            ll x = 0;
            for (i = 0; i <= size - !size && sn + 2 < MAX_SUBS_LEN; i++) {
              if (buf[i] == ',' || buf[i] == 0) {
                if (sn % 3 == 0 || tp == 4) {
                  *(ll *)&subs[sn] = x * mul;
                  sn += 2;
                } else {
                  subs[sn++] = x * mul;
                }
                x = 0;
                mul = 1;
              } else if (buf[i] == '-') {
                mul = -1;
              } else {
                x = x * 10 + buf[i] - '0';
                if (buf[i] < '0' || buf[i] > '9') {
                  QRETURN(set, 0);
                }
              }
            }

            if (tp != 4) {
              if (sn % 3 != 0) {
                dbg ("FAIL sn = %d\n", sn);
                QRETURN(set, 0);
              }
              sn /= 3;
            } else {
              if (sn % 2 != 0) {
                dbg ("FAIL sn = %d\n", sn);
                QRETURN(set, 0);
              }
              sn /= 2;
            }

            if (tp == 2) {
              res = set_news_subscr (id, (pli *)subs, sn);
            } else if (tp == 3) {
              res = set_news_subscr_add (id, (pli *)subs, sn);
            } else if (tp == 4) {
              res = set_news_subscr_del (id, (ll *)subs, sn);
            } else {
              assert (0);
            }

            QRETURN(set, res);
          }

          QRETURN(set, 0);
        }

        if (size % sizeof (pli) != 0) {
          QRETURN(set, 0);
        }

        int wn = size / sizeof (pli);
        if (wn >= 1) {
          if (tp == 0) {
            if (is_rev) {
              res = subscribers_add_new_rev (id, (pli *)buf, wn);
            } else {
              res = subscribers_add_new (id, (pli *)buf, wn);
            }
          } else if (tp == 1) {
            if (is_rev) {
              res = subscribers_del_rev (id, (pli *)buf, wn);
            } else {
              res = subscribers_del_old (id, (pli *)buf, wn);
            }
          }

          QRETURN(set, res);
        }
      }

      QRETURN(set, 0);
    }
  }

  QRETURN(set, -2);
}

int memcache_get (struct connection *c, const char *old_key, int old_key_len) {
  if (verbosity > 1) {
    fprintf (stderr, "memcache_get: key='%s', key_len=%d\n", old_key, old_key_len);
  }
  dl_log_add (LOG_HISTORY, 1, "memcache_get: key='%s', key_len=%d\n", old_key, old_key_len);
  sprintf (query, "key='%s', key_len=%d", old_key, old_key_len);

  char *key;
  int key_len;

  eat_at (old_key, old_key_len, &key, &key_len);

  if (key_len >= 5 && !strncmp (key, "stats", 5)) {
    int len = queue_prepare_stats();
    int len2 = prepare_stats (c, stats_buff + len, STATS_BUFF_SIZE - len);

    return_one_key (c, old_key, stats_buff, len + len2 - 1);
    return 0;
  }

  GET_LOG;

  SET_LOG_VERBOSITY;

  double cmd_time = -mytime();
  cmd_get++;

  if (key_len >= 13 && !strncmp (key, "timestamp_key", 13)) {
    int id, timeout, st;
    long long ip;
    if (sscanf (key, "timestamp_key%d,%lld,%d%n", &id, &ip, &timeout, &st) == 3 && key[st] == '(' && key[key_len - 1] == ')' && key_len - st - 2 < STATS_BUFF_SIZE) {
      char *s = stats_buff;
      int len = key_len - st - 2;
      memcpy (s, key + st + 1, len);
      s[len] = 0;
      dl_log_add (LOG_HISTORY, 1, "GET KEY (%s) id = %d, ip = %lld\n", s, id, ip);
      s = get_timestamp_key (s, id, ip, timeout, NULL, Q_DEF);
      return_one_key (c, old_key, s, strlen (s));
    }

    QRETURN(get, 0);
  }

  if (key_len >= 5 && !strncmp (key, "alias", 5)) {
    int st = 5, is_news = 0;
    if (key_len >= 10 && !strncmp (key + 5, "_news", 5)) {
      st += 5;
      is_news = 1;
    }
    if (key[st] == '(' && key[key_len - 1] == ')' && key_len - st - 2 < STATS_BUFF_SIZE) {
      char *s = stats_buff;
      int len = key_len - st - 2;
      memcpy (s, key + st + 1, len);
      s[len] = 0;
      dl_log_add (LOG_HISTORY, 1, "GET ALIAS (%s)\n", s);

      ll res;
      if (is_news) {
        ll id;
        if (sscanf (s, "%lld", &id) != 1 || !get_queue_news_alias (id, &res)) {//TODO get alias_news(1qwerty)
          QRETURN(get, 0);
        }
      } else {
        if (!is_news && !get_queue_alias (s, &res)) {
          QRETURN(get, 0);
        }
      }

      sprintf (s, "%lld", res);

      return_one_key (c, old_key, s, strlen (s));
    }

    QRETURN(get, 0);
  }

  if (key_len >= 14 && !strncmp (key, "qname_by_alias", 14)) {
    ll id;
    int st;
    if (sscanf (key, "qname_by_alias%lld%n", &id, &st) == 1 && key[st] == 0) {
      queue *q = get_queue_by_alias (id);
      if (q != NULL) {
        sprintf (buf, "%s : ref_cnt = %d, ev_first = %p, keys_cnt = %d, subscr_cnt = %d", q->name, q->ref_cnt, q->ev_first, q->keys_cnt, q->subscr_cnt);
        return_one_key (c, old_key, buf, strlen (buf));
      }
    }

    QRETURN(get, 0);
  }

  if (key_len >= 10 && !strncmp (key, "queue_info", 10)) {
    if (key[10] == '(' && key[key_len - 1] == ')') {
      key[key_len - 1] = 0;
      char *qname = key + 11;
      queue *q = get_queue (qname, 0);
      if (q != NULL) {
        sprintf (buf,
          "name\t%s\n"
          "ref_cnt\t%d\n"
          "key_cnt\t%d\n"
          "subscr_cnt\t%d\n"
          "ev_frist\t%p\n"
          , q->name, q->ref_cnt, q->keys_cnt, q->subscr_cnt, q->ev_first);
        return_one_key (c, old_key, buf, strlen (buf));
      }
    }
    QRETURN (get, 0);
  }

  if (key_len >= 12 && !strncmp (key, "watchcat_key", 12)) {
    int id, timeout, st;
    long long ip;
    if (sscanf (key, "watchcat_key%d,%lld,%d%n", &id, &ip, &timeout, &st) == 3 && key[st] == '(' && key[key_len - 1] == ')' && key_len - st - 2 < STATS_BUFF_SIZE) {
      char *s = stats_buff;
      int len = key_len - st - 2;
      memcpy (s, key + st + 1, len);
      s[len] = 0;

      char *tmp = s;
      while (*tmp) {
        if (*tmp == '?') {
          *tmp = 0x1f;
        }
        tmp++;
      }

      s = get_watchcat_key (s, id, ip, timeout);
      if (s != NULL) {
        return_one_key (c, old_key, s, strlen (s));
      }
    }

    QRETURN(get, 0);
  }

  if (key_len >= 8 && !strncmp (key, "news_key", 8)) {
    int id, timeout;
    long long ip;
    ll uid;
    if (sscanf (key, "news_key%d,%lld,%d(%lld)", &id, &ip, &timeout, &uid) == 4) {
      char *s;
      dl_log_add (LOG_HISTORY, 1, "GET NEWS KEY (%lld) id = %d, ip = %lld\n", uid, id, ip);
      s = get_news_key (id, ip, timeout, uid);
      if (s != NULL) {
        return_one_key (c, old_key, s, strlen (s));
      }
    }

    QRETURN(get, 0);
  }

  if (key_len >= 5 && !strncmp (key, "queue", 5)) {
    int id, ts, st;
    long long ip;
    if (sscanf (key, "queue%d,%lld,%d%n", &id, &ip, &ts, &st) == 3 && key[st] == '(' && key[key_len - 1] == ')' && key_len - st - 2 < STATS_BUFF_SIZE) {
      char *s = stats_buff, *ans = "{\"failed\":2}";
      int len = key_len - st - 2;

      if (len == KEY_LEN) {
        memcpy (s, key + st + 1, len);
        s[len] = 0;

        qkey *k;

        //fprintf (stderr, "mc validate : %s %d %lld %d\n", s, id, ip, ts);

        char err;
        if ((k = validate_key (s, id, ip, ts, 0, &err)) != NULL) {
          ans = get_events_http (k);
        }
      }

      return_one_key (c, old_key, ans, strlen (ans));
    }

    QRETURN(get, 0);
  }

  if (key_len >= 10 && !strncmp (key, "upd_secret", 10)) {
    int id;
    if (sscanf (key, "upd_secret%d", &id) == 1) {
      upd_secret (id);
    }
    return_one_key (c, old_key, "OK", 2);

    QRETURN(get, 0);
  }


  QRETURN(get, 0);
}

int memcache_version (struct connection *c) {
  cmd_version++;
  write_out (&c->Out, "VERSION "VERSION"\r\n", 9 + sizeof (VERSION));
  return 0;
}

int queue_prepare_stats (void) {
  cmd_stats++;

  char *s = stats_buff;
#define W(args...) WRITE_ALL (s, ## args)
  W ("heap_used\t%ld\n",                  (long) (dyn_cur - dyn_first));
  W ("heap_max\t%ld\n",                   (long) (dyn_last - dyn_first));
  W ("load_time\t%lf\n",                  load_time);
  W ("pid\t%d\n",                         getpid());
  W ("version\t%s\n",                     VERSION);
  W ("pointer_size\t%d\n",                (int)(sizeof (void *) * 8));
  W ("current_memory_used\t%lld\n",       dl_get_memory_used());
  W ("cmd_get\t%lld\n",                   cmd_get);
  W ("cmd_get_time\t%.7lf\n",             cmd_get_time);
  W ("max_cmd_get_time\t%.7lf\n",         max_cmd_get_time);
  W ("max_cmd_get_query\t%s\n",           max_cmd_get_query);
  W ("cmd_set\t%lld\n",                   cmd_set);
  W ("cmd_set_time\t%.7lf\n",             cmd_set_time);
  W ("max_cmd_set_time\t%.7lf\n",         max_cmd_set_time);
  W ("max_cmd_set_query\t%s\n",           max_cmd_set_query);
  W ("cmd_http_time\t%.7lf\n",            cmd_http_time);
  W ("max_cmd_http_time\t%.7lf\n",        max_cmd_http_time);
  W ("max_cmd_http_query\t%s\n",          max_cmd_http_query);
  W ("cmd_stats\t%lld\n",                 cmd_stats);
  W ("cmd_version\t%lld\n",               cmd_version);

  W ("limit_max_dynamic_memory\t%ld\n",   max_memory);
  W ("events_memory\t%ld\n",              events_memory);
  W ("events_total\t%d\n",                events_cnt);
  W ("events_created\t%lld\n",            events_created);
  W ("events_sent\t%lld\n",               events_sent);
  W ("keys_memory\t%ld\n",                keys_memory);
  W ("keys_count\t%d\n",                  keys_cnt);
  W ("queues_memory\t%ld\n",              queues_memory);
  W ("queues_count\t%d\n",                queues_cnt);
  W ("subscribers_memory\t%ld\n",         subscribers_memory);
  W ("subscribers_count\t%ld\n",          subscribers_cnt);
  W ("treap_memory\t%ld\n",               (long)treap_get_memory());
  W ("treap_cnt\t%d\n",                   treap_cnt);
  W ("treap_cnt_rev\t%d\n",               treap_cnt_rev);
  W ("str_memory\t%ld\n",                 str_memory);
  W ("htbl_memory\t%ld\n",                get_htbls_memory());
  W ("extra_memory\t%lld\n",              dl_get_memory_used() - events_memory - keys_memory - subscribers_memory
                                                               - treap_get_memory() - str_memory - get_htbls_memory()
                                                               - crypto_memory - queues_memory - time_keys_memory);
  W ("sent_queries\t%lld\n",              sent_queries);

  STAT_OUT (send_create_watchcat);
  STAT_OUT (send_changes_cnt);
  STAT_OUT (process_changes_cnt);
  STAT_OUT (changes_len_max);
  STAT_OUT (process_changes_total_len);
  STAT_OUT (changes_add_rev_cnt);
  STAT_OUT (changes_add_rev_len);
  STAT_OUT (changes_add_cnt);
  STAT_OUT (changes_add_len);
  STAT_OUT (changes_del_rev_cnt);
  STAT_OUT (changes_del_rev_len);
  STAT_OUT (changes_del_cnt);
  STAT_OUT (changes_del_len);
  STAT_OUT (send_news_cnt);
  STAT_OUT (redirect_news_twice_cnt);
  STAT_OUT (redirect_news_cnt);
  STAT_OUT (redirect_news_len);

  STAT_OUT (to_add_overflow);
  STAT_OUT (to_del_overflow);
  STAT_OUT (buff_overflow_cnt);

  W ("engine_id\t%d\n", engine_id);
  W ("engine_n\t%d\n", engine_n);

  STAT_OUT (news_subscr_cnt);
  STAT_OUT (news_subscr_missed);
  W ("news_subscr_cache_missed\t%.6lf\n", (double)news_subscr_missed / news_subscr_cnt);

  STAT_OUT (news_subscr_len);
  STAT_OUT (news_subscr_actual_len);
  W ("news_subscr_cache_missed\t%.6lf\n", (double)news_subscr_actual_len / news_subscr_len);

  STAT_OUT (rpc_received_news_subscr);
  STAT_OUT (rpc_received_news_subscr_size);
  W ("rpc_received_news_subcr_avg\t%.2lf\n", (double)rpc_received_news_subscr_size / rpc_received_news_subscr);

  STAT_OUT (rpc_received_news_redirect);
  STAT_OUT (rpc_received_news_redirect_size);
  W ("rpc_received_news_redirect_avg\t%.2lf\n", (double)rpc_received_news_redirect_size / rpc_received_news_redirect);

  STAT_OUT (rpc_failed);
  STAT_OUT (rpc_sent);
  STAT_OUT (rpc_sent_size);
  W ("rpc_sent_avg\t%.2lf\n", (double)rpc_sent_size / rpc_sent);
  STAT_OUT (rpc_received);
  STAT_OUT (rpc_received_size);
  W ("rpc_received_avg\t%.2lf\n", (double)rpc_received_size / rpc_received);


  STAT_OUT (http_queries);
  STAT_OUT (http_queries_size);
  W ("http_queries_avg\t%lf\n", (double)http_queries_size / http_queries);
  STAT_OUT (http_sent);
  STAT_OUT (http_sent_size);
  W ("http_sent_avg\t%lf\n", (double)http_sent_size / http_sent);



  W ("http_connections\t%d\n",            http_connections);
  W ("pending_http_queries\t%d\n",        pending_http_queries);
  W ("http_queries_ok\t%lld\n",           http_queries_ok);
  W ("http_queries_delayed\t%lld\n",      http_queries_delayed);
  W ("http_bad_headers\t%lld\n",          http_bad_headers);
  W ("http_success\t%lld\n",              http_failed[0]);
  W ("http_failed1\t%lld\n",              http_failed[1]);
  W ("http_failed2\t%lld\n",              http_failed[2]);
  W ("http_failed3\t%lld\n",              http_failed[3]);
  W ("http_qps\t%.6f\n",                  now > start_time ? http_queries * 1.0 / (now - start_time) : 0);
  W ("max_delay\t%.6f\n",                 max_delay);
  W ("mean_delay\t%.6f\n",                cnt_delay ? sum_delay / cnt_delay : 0);

  W ("version\t" VERSION_STR " compiled at " __DATE__ " " __TIME__ " by gcc " __VERSION__ "\n");
#undef W
  int len = s - stats_buff;
  assert (len < STATS_BUFF_SIZE);
  return len;
}

int memcache_stats (struct connection *c) {
  int len = queue_prepare_stats();
  int len2 = prepare_stats (c, stats_buff + len, STATS_BUFF_SIZE - len);
  write_out (&c->Out, stats_buff, len + len2);
  write_out (&c->Out, "END\r\n", 5);
  return 0;
}

/*
 *
 *      END (PROXY MEMCACHE SERVER)
 *
 */


/*
 *
 *      "HISTORY EVENTS"
 *
 */

int hts_wakeup (struct connection *c);
int hts_close (struct connection *c, int who);
int delete_history_query (struct conn_query *q);

struct conn_query_functions history_cq_func = {
.magic = CQUERY_FUNC_MAGIC,
.title = "queue-events-query",
.wakeup = delete_history_query, // history_query_timeout
.close = delete_history_query,
.complete = delete_history_query
};


int create_history_query (queue *U, struct connection *c, double timeout,
                          struct conn_query **rq, char *kname) {
  struct conn_query *q = zmalloc (sizeof (struct conn_query));

  if (verbosity > 1) {
    fprintf (stderr, "OPPA\n");
    fprintf (stderr, "create_history_query (%p[%s], key = %p[%s], %p[%d]): q=%p\n", U, U->name,
    ((qkey *)HTS_DATA (c)->extra)->name, ((qkey *)HTS_DATA (c)->extra)->name,
    c, c->fd, q);
  }

  q->custom_type = 0;
  q->outbound = (struct connection *) U;
  q->requester = c;
  q->start_time = mytime();
  str_memory += strlen (kname) + 1;
  q->extra = dl_strdup (kname);

  q->cq_type = &history_cq_func;
  q->timer.wakeup_time = (timeout > 0 ? q->start_time + timeout : 0);

  pending_http_queries++;
  insert_conn_query (q);

  if (verbosity > 1) {
    fprintf (stderr, "after insert_conn_query()\n");
  }
  *rq = q;

  return 1;
}

int create_history_query_group (qkey_group *g, struct connection *c, double timeout) {
  int i;

  if (verbosity > 2) {
    fprintf (stderr, "create_history_query_group (size = %d) (timeout = %lf)\n", g->n, timeout);
  }
  for (i = 0; i < g->n; i++) {
    queue *q = g->k[i]->q;
    assert (q != NULL);
    HTS_DATA (c)->extra = g->k[i];
    create_history_query (q, c, timeout, &g->k[i]->conn, g->k[i]->name);
    g->k[i]->lock++;
    g->k[i]->subscribed = get_utime (CLOCK_MONOTONIC);
  }
  c->pending_queries = 1;

  return 1;
}



int delete_history_query (struct conn_query *q) {
  if (verbosity > 1) {
    fprintf (stderr, "delete_history_query (%p,%p)\n", q, q->requester);
  }

  char *kname = (char *)q->extra;
  qkey_clear_conn (kname);
  str_memory -= strlen (kname) + 1;
  dl_strfree (kname);

  pending_http_queries--;
  struct connection *c = q->requester;

  int req_generation = q->req_generation;
  delete_conn_query (q);

  if (c->generation == req_generation) {
    c->generation = ++conn_generation;
    c->pending_queries = 0;
  }
  zfree (q, sizeof (*q));
  return 0;
}



/*
 *
 *      HTTP INTERFACE
 *
 */

struct http_server_functions http_methods = {
  .execute = hts_execute,
  .ht_wakeup = hts_wakeup,
  .ht_alarm = hts_wakeup,
  .ht_close = hts_close
};

static char *qPost, *qGet, *qUri, *qHeaders;
static int qPostLen, qGetLen, qUriLen, qHeadersLen;

// writes at most b_len-1 bytes of value of argument arg_name into buffer
// + 0
// returns # of written bytes
// scans GET, then POST

int getArgFrom (char *buffer, int b_len, const char *arg_name, char *where, int where_len) {
  char *where_end = where + where_len;
  int arg_len = strlen (arg_name);
  while (where < where_end) {
    char *start = where;
    while (where < where_end && (*where != '=' && *where != '&')) {
      ++where;
    }
    if (where == where_end) {
      buffer[0] = 0;
      return -1;
    }
    if (*where == '=') {
      if (arg_len == where - start && !memcmp (arg_name, start, arg_len)) {
        start = ++where;
        while (where < where_end && *where != '&') {
          ++where;
        }
        b_len--;
        if (where - start < b_len) {
          b_len = where - start;
        }
        memcpy (buffer, start, b_len);
        buffer[b_len] = 0;
        return b_len;
      }
      ++where;
    }
    while (where < where_end && *where != '&') {
      ++where;
    }
    if (where < where_end) {
      ++where;
    }
  }
  buffer[0] = 0;
  return -1;
}

int getArg (char *buffer, int b_len, const char *arg_name) {
  int res = getArgFrom (buffer, b_len, arg_name, qGet, qGetLen);
  if (res < 0) {
    res = getArgFrom (buffer, b_len, arg_name, qPost, qPostLen);
  }
  return res;
}

#define KEY_MAX_CNT 100
static char key_buff[KEY_LEN * KEY_MAX_CNT + 1], ip_buff[64 + 1];

static char no_cache_headers[] =
  "Pragma: no-cache\r\n"
  "Cache-Control: no-store\r\n";

void http_return (struct connection *c, const char *str) {
  assert (str != NULL);
  int len = strlen (str);
  http_sent++;
  http_sent_size += len;
  write_basic_http_header (c, 200, 0, len, no_cache_headers, "text/javascript; charset=UTF-8");
  write_out (&c->Out, str, len);
}

int http_return_history (struct connection *c, const char *ans) {
  if (ans == NULL) {
    fprintf (stderr, "buffer overflow\n");
    return -500;
  }

  http_return (c, ans);

  return 0;
}

#define MAX_POST_SIZE 4096

inline int conv_hex_digit (int c) {
  if ('0' <= c && c <= '9') {
    return c - '0';
  }
  return (c | 0x20) - 'a' + 10;
}

int md5_last_bits (char *x) {
  char md5_buf[32];
  int l = strlen (x);
  md5_hex (x, l, md5_buf);

  return conv_hex_digit (md5_buf[29]) * 256 + conv_hex_digit (md5_buf[30]) * 16 + conv_hex_digit (md5_buf[31]);
}


int ipv6_to_ipv4 (void) {
  int i;
  for (i = 0; ip_buff[i]; i++) {
    if ('A' <= ip_buff[i] && ip_buff[i] < 'Z') {
      ip_buff[i] = ip_buff[i] - 'A' + 'a';
    }
  }
  int l = 0, r = i;
  if (ip_buff[0] == ':' && ip_buff[1] == ':') {
    l++;
  } else {
    if (r > 2 && ip_buff[r - 1] == ':' && ip_buff[r - 2] == ':') {
      r--;
    }
  }
  ip_buff[r] = ':';

  char *x[9];
  int cnt = 0;
  for (i = l; i <= r && cnt <= 8; i++) {
    x[cnt++] = ip_buff + i;
    while (ip_buff[i] != ':') {
      i++;
    }
    ip_buff[i] = 0;
  }

  if (cnt < 2 || cnt > 8) {
    return 0;
  }
  if (cnt < 8) {
    int k = -1;
    for (i = 0; i < cnt; i++) {
      if (x[i][0] == 0) {
        if (k >= 0) {
          return 0;
        }
        k = i;
      }
    }
    if (k < 0) {
      return 0;
    }
    for (i = cnt - 1; i > k; i--) {
      x[i - cnt + 8] = x[i];
    }
    for (i = k - cnt + 8; i >= k; i--) {
      x[i] = "0";
    }
  }
  int y[8];
  for (i = 0; i < 8; i++) {
    int pos = -1;
    if (sscanf (x[i], "%x%n", &y[i], &pos) < 1 || y[i] >= 65536 || y[i] < 0 || x[i][pos]) {
      return 0;
    }
  }

  char buf[17];
  sprintf (buf, "%04x%04x", y[0], y[1]);
  int f1 = md5_last_bits (buf);
  sprintf (buf, "%04x%04x", y[2], y[3]);
  int f2 = md5_last_bits (buf);
  sprintf (buf, "%04x%04x%04x%04x", y[4], y[5], y[6], y[7]);
  int f3 = md5_last_bits (buf);
  int n1 = ((f1 >> 4) & 0x1f) | 0xe0;
  int n2 = ((f1 & 0x0f) << 4) | (f2 >> 8);
  int n3 = f2 & 0xff;
  int n4 = f3 & 0xff;
  sprintf (ip_buff, "%d.%d.%d.%d", n1, n2, n3, n4);
  return 1;
}

int conv_ip (void) {
  int dots = 0, i;
  for (i = 0; ip_buff[i] && i < 20; i++) {
    dots += (ip_buff[i] == '.');
  }
  if (dots == 0) {
    if (!ipv6_to_ipv4()) {
      return 0;
    }
  }

  int nums[4], nn = 0, v = 0;
  for (i = 0; ip_buff[i] && i < 20; i++) {
    char c = ip_buff[i];
    if (c == '.') {
      if (nn == 3) {
        return 0;
      }
      nums[nn++] = v;
      v = 0;
    } else if ('0' <= c && c <= '9') {
      v = v * 10 + c - '0';
      if (v > 255) {
        return 0;
      }
    } else {
      return 0;
    }
  }
  nums[nn++] = v;
  return (nums[0] << 24) | (nums[1] << 16) | (nums[2] << 8) | nums[3];
}

int hts_execute (struct connection *c, int op) {
  struct hts_data *D = HTS_DATA(c);
  static char ReqHdr[MAX_HTTP_HEADER_SIZE];
  static char Post[MAX_POST_SIZE];
  static char tmp_buff[MAX_POST_SIZE];
  int Post_len = 0;

  static int req_ts[KEY_MAX_CNT];

  if (verbosity > 1) {
    fprintf (stderr, "in hts_execute: connection #%d, op=%d, header_size=%d, data_size=%d, http_version=%d\n",
             c->fd, op, D->header_size, D->data_size, D->http_ver);
  }

  if (D->data_size >= MAX_POST_SIZE) {
    return -413;
  }

  if (D->query_type != htqt_get && D->query_type != htqt_post) {
    D->query_flags &= ~QF_KEEPALIVE;
    return -501;
  }

  if (D->data_size > 0) {
    Post_len = D->data_size;
    int have_bytes = get_total_ready_bytes (&c->In);
    if (have_bytes < Post_len + D->header_size) {
      if (verbosity > 1) {
        fprintf (stderr, "-- need %d more bytes, waiting\n", Post_len + D->header_size - have_bytes);
      }
      return Post_len + D->header_size - have_bytes;
    }
  }

  double cmd_time = -mytime();

  assert (D->header_size <= MAX_HTTP_HEADER_SIZE);
  safe_read_in (&c->In, ReqHdr, D->header_size);

  qHeaders = ReqHdr + D->first_line_size;
  qHeadersLen = D->header_size - D->first_line_size;
  assert (D->first_line_size > 0 && D->first_line_size <= D->header_size);

  if (verbosity > 1) {
    fprintf (stderr, "===============\n%.*s\n==============\n", D->header_size, ReqHdr);
    fprintf (stderr, "%d,%d,%d,%d\n", D->host_offset, D->host_size, D->uri_offset, D->uri_size);

    fprintf (stderr, "hostname: '%.*s'\n", D->host_size, ReqHdr + D->host_offset);
    fprintf (stderr, "URI: '%.*s'\n", D->uri_size, ReqHdr + D->uri_offset);
  }

//  D->query_flags &= ~QF_KEEPALIVE;

  if (Post_len > 0) {
    if (read_in (&c->In, Post, Post_len) < Post_len) {
      D->query_flags &= ~QF_KEEPALIVE;
      QRETURN(http, -500);
    }
    Post[Post_len] = 0;
    if (verbosity > 1) {
      fprintf (stderr, "have %d POST bytes: `%.80s`\n", Post_len, Post);
    }
    qPost = Post;
    qPostLen = Post_len;
  } else {
    qPost = 0;
    qPostLen = 0;
  }

  qUri = ReqHdr + D->uri_offset;
  qUriLen = D->uri_size;

  char *get_qm_ptr = memchr (qUri, '?', qUriLen);
  if (get_qm_ptr) {
    qGet = get_qm_ptr + 1;
    qGetLen = qUri + qUriLen - qGet;
    qUriLen = get_qm_ptr - qUri;
  } else {
    qGet = 0;
    qGetLen = 0;
  }

  int query_len = qPostLen < 27 ? qPostLen : 27;
  memcpy (query, qPost, query_len);
  query[query_len] = 0;

  if (qUriLen >= 20) {
    QRETURN(http, -418);
  }

  if (qUriLen >= 4 && !memcmp (qUri, "/im", 3)) {
    int wait_sec = 0, req_ts_n = 0;

    getArg (tmp_buff, sizeof (tmp_buff), "act");

    int tp = 0;
    if (!((!strcmp (tmp_buff, "a_check") && (tp = 1)) || (!strcmp (tmp_buff, "a_release") && (tp = 2)))
        || getArg (key_buff, sizeof (key_buff), "key") % KEY_LEN || get_http_header (qHeaders, qHeadersLen, ip_buff, sizeof (ip_buff), "X-REAL-IP", 9) < 7) {
      QRETURN(http, -204);
    }
    if (getArg (tmp_buff, sizeof (tmp_buff), "ts") > 0) {
      char *s = tmp_buff, pc = 1, *t = s;

      while (pc) {
        pc = *t;
        if (pc == '_') {
          *t = 0;
        }

        if (*t == 0) {
          if (req_ts_n >= KEY_MAX_CNT) {
            QRETURN(http, -204);
          }
          req_ts[req_ts_n++] = atoi (s);
          s = t + 1;
        }
        t++;
      }
    }

    sprintf (query + query_len, "... #keys = %d.", req_ts_n);

    if (getArg (tmp_buff, sizeof (tmp_buff), "wait") > 0) {
      wait_sec = atoi (tmp_buff);
      if (wait_sec < 0) {
        wait_sec = 0;
      }
      if (wait_sec > 120) {
        wait_sec = 120;
      }
    }
    int id, ip;
    if (getArg (tmp_buff, sizeof (tmp_buff), "id") > 0) {
      id = atoi (tmp_buff);
    } else {
      id = -1;
    }
    ip = conv_ip();

    qkey_group *k;
    static char tmp_key[sizeof (key_buff)];
    strcpy (tmp_key, key_buff);

    if (verbosity > 0) {
      fprintf (stderr, "validate : %s\n", tmp_key);
    }

    if (id == -1 || ip == 0 || !(k = validate_key_group (key_buff, id, ip, req_ts, req_ts_n, tp == 2))) {
      if (verbosity > 1) {
        fprintf (stderr, "key %s validation failed, code = 2\n", tmp_key);
      }
      http_failed[1]++;
      http_return (c, "{\"failed\":2}\r\n");
      QRETURN(http, 0);
    }

    if (tp == 2) {
      release_key_group (k);
      http_queries_ok++;
      http_return (c, "{\"OK\":1}");
      qkey_group_free (k);
      QRETURN(http, 0);
    }

    char *ans = get_events_http_group (k);

    if (verbosity > 1) {
      fprintf (stderr, "ans = %s\n", ans);
    }

    if (wait_sec == 0 || !may_wait (ans)) {
      http_queries_ok++;
      qkey_group_free (k);
      int res = http_return_history (c, ans);
      QRETURN(http, res);
    }

    c->generation = ++conn_generation;
    c->pending_queries = 0;

    create_history_query_group (k, c, wait_sec);
    D->extra = k;

    c->status = conn_wait_net;
    set_connection_timeout (c, wait_sec + 1.0);

    http_queries_delayed++;

    QRETURN(http, 0);
  }

  QRETURN(http, -404);
}

int hts_wakeup (struct connection *c) {
//  fprintf (stderr, "HTS_WAKEUP: IN\n");
  struct hts_data *D = HTS_DATA(c);

  qkey_group *k = D->extra;

  if (verbosity > 2) {
    fprintf (stderr, "hts_wakeup : keys [");
    int i;
    for (i = 0; i < k->n; i++) {
      fprintf (stderr, "%s%c", k->k[i]->name, ",]"[i + 1 == k->n]);
    }
    fprintf (stderr, "\n");
  }

  assert (c->status == conn_expect_query || c->status == conn_wait_net);
  c->status = conn_expect_query;
  clear_connection_timeout (c);

  if (!(D->query_flags & QF_KEEPALIVE)) {
    c->status = conn_write_close;
    c->parse_state = -1;
  }

  char *ans = get_events_http_group (k);
  c->generation = ++conn_generation;
  c->pending_queries = 0;

  int i;
  for (i = 0; i < k->n; i++) {
    k->k[i]->lock--;
    k->k[i]->subscribed = 0;
    k->k[i]->conn = NULL;
    assert (k->k[i]->lock >= 0);
  }
  qkey_group_free (k);
  D->extra = NULL;

//  fprintf (stderr, "HTS_WAKEUP: OUT\n");
  return http_return_history (c, ans);
}

int hts_close (struct connection *c, int who) {
  struct hts_data *D = HTS_DATA(c);

  qkey_group *k = D->extra;

  if (k != NULL) {
    int i;
    for (i = 0; i < k->n; i++) {
      k->k[i]->lock--;
      k->k[i]->subscribed = 0;
      k->k[i]->conn = NULL;
      assert (k->k[i]->lock >= 0);
    }
    qkey_group_free (k);
    D->extra = NULL;
  }


  return 0;
}

/***
  RPC
 ***/

int rpcc_execute (struct connection *c, int op, int len);

struct rpc_client_functions queue_rpc_client = {
  .execute = rpcc_execute,
  .check_ready = server_check_ready,
  .flush_packet = rpcc_flush_packet_later,
  .rpc_check_perm = rpcc_default_check_perm,
  .rpc_init_crypto = rpcc_init_crypto,
  .rpc_start_crypto = rpcc_start_crypto
//  .rpc_ready = rpcc_send_query
};

struct conn_target rpc_default_ct = {
  .min_connections = 1,
  .max_connections = 1,
  .type = &ct_rpc_client,
  .extra = (void *)&queue_rpc_client,
  .port = 11210,
  .reconnect_timeout = 1
};


struct conn_target *queue_connections;

int use_rpc;

#define	MAX_CLUSTER_SERVERS	1024
struct conn_target *CS[MAX_CLUSTER_SERVERS];
int CSN;

struct connection *get_target_connection (struct conn_target *S) {
  struct connection *c, *d = 0;
  int r, u = 10000;
  if (!S) {
    return 0;
  }
  for (c = S->first_conn; c != (struct connection *)S; c = c->next) {
    r = server_check_ready (c);
    if (r == cr_ok) {
      return c;
    } else if (r == cr_stopped && c->unreliability < u) {
      u = c->unreliability;
      d = c;
    }
  }
  /* all connections failed? */
  return d;
}

void flush_conn (struct conn_target *S) {
  struct connection *c;
  if (!S) {
    return;
  }
  for (c = S->first_conn; c != (struct connection *)S; c = c->next) {
    MCC_FUNC (c)->flush_query (c);
  }
}

#define	MAX_CONFIG_SIZE	(1 << 16)

char config_buff[MAX_CONFIG_SIZE + 4], *config_filename, *cfg_start, *cfg_end, *cfg_cur;
int config_bytes, cfg_lno;

static int cfg_skipspc(void) {
  while (*cfg_cur == ' ' || *cfg_cur == 9 || *cfg_cur == 13 || *cfg_cur == 10 || *cfg_cur == '#') {
    if (*cfg_cur == '#') {
      do cfg_cur++; while (*cfg_cur && *cfg_cur != 10);
      continue;
    }
    if (*cfg_cur == 10) {
      cfg_lno++;
    }
    cfg_cur++;
  }
  return (unsigned char) *cfg_cur;
}

static int cfg_skspc(void) {
  while (*cfg_cur == ' ' || *cfg_cur == 9) {
    cfg_cur++;
  }
  return (unsigned char) *cfg_cur;
}

static void syntax (const char *msg) __attribute__ ((noreturn));

static void syntax (const char *msg) {
  char *ptr = cfg_cur, *end = ptr + 20;
  if (!msg) {
    msg = "syntax error";
  }
  if (cfg_lno) {
    fprintf (stderr, "%s:%d: ", config_filename, cfg_lno);
  }
  fprintf (stderr, "fatal: %s near ", msg);
  while (*ptr && *ptr != 13 && *ptr != 10) {
    putc (*ptr++, stderr);
    if (ptr > end) {
      fprintf (stderr, " ...");
      break;
    }
  }
  putc ('\n', stderr);

  exit(2);
}

void parse_config (void) {
  int r, c;
  char *ptr, *s;
  struct conn_target *D;
  struct hostent *h;
  config_bytes = r = read (fd[0], config_buff, MAX_CONFIG_SIZE + 1);
  if (r < 0) {
    fprintf (stderr, "error reading configuration file %s: %m\n", config_filename);
    exit (2);
  }
  if (r > MAX_CONFIG_SIZE) {
    fprintf (stderr, "configuration file %s too long (max %d bytes)\n", config_filename, MAX_CONFIG_SIZE);
    exit (2);
  }

  cfg_cur = cfg_start = config_buff;
  cfg_end = cfg_start + r;
  *cfg_end = 0;
  cfg_lno = 0;

  CSN = 0;
  while (cfg_skipspc()) {
    ptr = s = cfg_cur;
    while ((*s >= 'a' && *s <= 'z') || (*s >= 'A' && *s <= 'Z') || (*s >= '0' && *s <= '9') || *s == '.' || *s == '-' || *s == '_') {
      s++;
    }
    if (s == ptr) {
      syntax("hostname expected");
      return;
    }
    if (s > ptr + 63) {
      syntax("hostname too long");
      return;
    }
    c = *s;
    *s = 0;
    if (CSN >= MAX_CLUSTER_SERVERS) {
      syntax("too many servers in cluster");
      return;
    }
    D = &rpc_default_ct;
    D->port = -1;
    if (!(h = kdb_gethostbyname (ptr)) || h->h_addrtype != AF_INET || h->h_length != 4 || !h->h_addr_list || !h->h_addr) {
      syntax("cannot resolve hostname");
      return;
    }
    D->target = *((struct in_addr *) h->h_addr);

    cfg_cur = ptr = s;
    *s = c;
    if (cfg_skspc() != ':') {
      syntax("':' expected");
    }
    cfg_cur++;
    cfg_skspc();
    if (*cfg_cur < '0' || *cfg_cur > '9') {
      syntax("port number expected");
    }
    D->port = strtol (cfg_cur, &cfg_cur, 10);
    if (D->port <= 0 || D->port >= 0x10000) {
      syntax("port number out of range");
    }
    if (cfg_skspc() != ';') {
      syntax("';' expected");
    }
    cfg_cur++;

    if (verbosity > 0) {
      fprintf (stderr, "server #%d: ip %s, port %d\n", CSN, inet_ntoa (D->target), D->port);
    }

    int was_created = -1;
    D = create_target (D, &was_created);
    if (was_created <= 0) {
      syntax ("duplicate hostname:port");
    }
    CS[CSN] = D;
    CSN++;
  }
  if (!CSN) {
    fprintf (stderr, "fatal: no cluster servers defined\n");
    exit(1);
  }
}



int rpcc_execute (struct connection *c, int op, int len) {
  if (verbosity > 0) {
    fprintf (stderr, "rpcc_execute: fd=%d, op=%d, len=%d\n", c->fd, op, len);
  }

  return SKIP_ALL_BYTES;
}

int send_to_news_rpc (int addr, int *q) {
  //fprintf (stderr, "send_to_news_rpc to @%d, [len = %d]\n", addr, q[0]);

  addr = dl_abs (addr) % CSN;
  struct connection *c = get_target_connection (CS[addr]);

  //fprintf (stderr, "connection = %p\n", c);

  if (c == NULL) {
    STAT (rpc_failed);
    return 0;
  }
  STAT (rpc_sent);

  int n = q[0];
  assert (n >= 4);
  q[0] *= sizeof (int);
  q[1] = RPCC_DATA(c)->out_packet_num++;
  q[n - 1] = compute_crc32 (q, q[0] - sizeof (int));

  assert (write_out (&c->Out, q, q[0]) == q[0]);

  rpc_sent_size += q[0];

  //TODO delete it
  RPCC_FUNC(c)->flush_packet(c);
  return 0;
}



/***
  RPC SERVER
 ***/

int rpcs_execute (struct connection *c, int op, int len) {
  if (verbosity > 0) {
    fprintf (stderr, "rpcs_execute: fd=%d, op=%d, len=%d\n", c->fd, op, len);
  }
  int *v = (int *)buf;

  ll id;
  int x, y, ttl, text_n, text_len, n, is_rev, is_add, res;
  char *text;

  STAT (rpc_received);
  rpc_received_size += len;

  switch (op & 0xffff0000) {
  case RPC_NEWS_SUBSCR:
    STAT (rpc_received_news_subscr);
    rpc_received_news_subscr_size += len;
    assert (len < MAX_VALUE_LEN);
    assert (read_in (&c->In, v, len) == len);

    is_rev = (op >> 1) & 1,
    is_add = op & 1;
    id = *(ll *)&v[3];
    n = len / sizeof (int) - 4 - 2;
    assert (n % 3 == 0);
    n /= 3;
    pli *p = (pli *)&v[5];

    if (is_add) {
      //TODO fix this!!!
      if (is_rev) {
        //assert (!is_rev);
        res = subscribers_add_new_rev (id, p, n);
      } else {
        res = subscribers_add_new (id, p, n);
      }
    } else {
      if (is_rev) {
        //assert (!is_rev);
        res = subscribers_del_rev (id, p, n);
      } else {
        res = subscribers_del_old (id, p, n);
      }
    }

    return 0;
    break;

  case RPC_NEWS_REDIRECT:
    STAT (rpc_received_news_redirect);
    rpc_received_news_redirect_size += len;
    assert (len < MAX_VALUE_LEN);
    assert (read_in (&c->In, v, len) == len);

    v += 3;

    READ_LONG (v, id);
    READ_INT (v, x);
    READ_INT (v, y);
    READ_INT (v, ttl);
    READ_INT (v, n);
    READ_INT (v, text_n);
    text_len = (text_n + 1 + 3) / 4;
    pli *to = (pli *)v;
    v += n * 3;
    text = (char *)v;

    int need_debug = op & 1;

    // FAIL: TOO MUCH DEBUG DATA dbg ("RECEIVE REDIRECTED NEW: debug = %d, [id = %lld(%d;%d)], [ttl = %d] {%s}", need_debug, id, x, y, ttl, text);
    add_event_to_news (id, x, y, ttl, to, n, text, need_debug);
    return 0;
    break;
  }

  return SKIP_ALL_BYTES;
}

/*
 *
 *  MEMCACHED CLIENT
 *
 *
 */


int proxy_client_execute (struct connection *c, int op);

struct memcache_client_functions mc_proxy_outbound = {
  .execute = proxy_client_execute,
  .check_ready = server_check_ready,

  .flush_query = mcc_flush_query,
  .mc_check_perm = mcc_default_check_perm,
  .mc_init_crypto = mcc_init_crypto,
  .mc_start_crypto = mcc_start_crypto
};


struct conn_target default_ct = {
.min_connections = 1,
.max_connections = 1,
.type = &ct_memcache_client,
.extra = &mc_proxy_outbound,
.reconnect_timeout = 1
}, *watchcat_conn, *news_conn;

struct in_addr settings_addr;

int send_to (struct conn_target *S, char *query, int query_len, int force) {
  struct connection *d = get_target_connection (S);
  if (d == NULL) {
    if (verbosity > 0) {
      fprintf (stderr, "cannot find connection to target %s:%d for query %s, dropping query\n", S ? conv_addr (S->target.s_addr, 0) : "?", S ? S->port : 0, query);
    }
    return -1;
  }
  if (verbosity > 1) {
    fprintf (stderr, "send query '%s'\n", query);
  }
  assert (write_out (&d->Out, query, query_len) == query_len);

  sent_queries++;

  if (force) {
    MCC_FUNC (d)->flush_query (d);
  }
  d->last_query_sent_time = precise_now;
  return 0;
}

int send_to_watchcat (char *query, int query_len) {
  return send_to (watchcat_conn, query, query_len, 1);
}

void flush_watchcat (void) {
  flush_conn (watchcat_conn);
}

int send_to_news (char *query, int query_len) {
//  fprintf (stderr, "OPPA %p", query);
//  fprintf (stderr, "SEND TO NEWS [%s], %d\n", query, query_len);
  return send_to (news_conn, query, query_len, 0);
}

void flush_news (void) {
  flush_conn (news_conn);
}


int proxy_client_execute (struct connection *c, int op) {
  struct mcc_data *D = MCC_DATA(c);

  if (verbosity > 0) {
    fprintf (stderr, "proxy_mc_client: op=%d, key_len=%d, arg#=%d, response_len=%d\n", op, D->key_len, D->arg_num, D->response_len);
  }

  c->last_response_time = precise_now;
  return SKIP_ALL_BYTES;
}


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

static void sigint_handler (const int sig) {
  const char message[] = "SIGINT handled.\n";
  kwrite (2, message, sizeof (message) - (size_t)1);

  free_all();
  exit (0);
}

static void sigterm_handler (const int sig) {
  const char message[] = "SIGTERM handled.\n";
  kwrite (2, message, sizeof (message) - (size_t)1);

  free_all();
  exit (0);
}

static void sighup_handler (const int sig) {
  const char message[] = "got SIGHUP.\n";
  kwrite (2, message, sizeof (message) - (size_t)1);

  signal (SIGHUP, sighup_handler);
}

static void sigusr1_handler (const int sig) {
  const char message[] = "got SIGUSR1, rotate logs.\n";
  kwrite (2, message, sizeof (message) - (size_t)1);

  reopen_logs();
  signal (SIGUSR1, sigusr1_handler);
}

static void sigpoll_handler (const int sig) {
  sigpoll_cnt++;
  signal (SIGPOLL, sigpoll_handler);
}

void cron (void) {
  free_by_time (537);
  create_all_outbound_connections();
}

int sfd, http_sfd = -1, http_port;

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

  if (http_port > 0 && http_sfd < 0) {
    http_sfd = server_socket (http_port, settings_addr, backlog, 0);
    if (http_sfd < 0) {
      fprintf (stderr, "cannot open http server socket at port %d: %m\n", http_port);
      exit (1);
    }
  }

  if (verbosity > 0) {
    fprintf (stderr, "created listening socket at %s:%d, fd=%d\n", conv_addr (settings_addr.s_addr, buf), port, sfd);
  }

  if (daemonize) {
    setsid();
  }

  if (change_user (username) < 0) {
    fprintf (stderr, "fatal: cannot change user to %s\n", username ? username : "(none)");
    exit (1);
  }

  if (use_rpc) {
    init_listening_connection (sfd, &ct_rpc_server, &queue_rpc_server);
  } else {
    init_listening_connection (sfd, &ct_queue_engine_server, &memcache_methods);
  }

  if (http_sfd >= 0) {
    init_listening_connection (http_sfd, &ct_http_server, &http_methods);
  }

  struct hostent *h;
  if (!(h = gethostbyname (hostname)) || h->h_addrtype != AF_INET || h->h_length != 4 || !h->h_addr_list || !h->h_addr) {
    fprintf (stderr, "fatal: cannot resolve hostname %s: %m\n", hostname);
    exit (1);
  }

  if(watchcat_port!=0){
    default_ct.target = *(struct in_addr *) h->h_addr;
    default_ct.port = watchcat_port;
    watchcat_conn = create_target (&default_ct, 0);
  }

  if (!use_rpc && news_port!=0) {
    default_ct.target = *(struct in_addr *) h->h_addr;
    default_ct.port = news_port;
    news_conn = create_target (&default_ct, 0);
  }

  get_utime_monotonic();
  create_all_outbound_connections();

  signal (SIGINT, sigint_handler);
  signal (SIGTERM, sigterm_handler);
  signal (SIGUSR1, sigusr1_handler);
  signal (SIGPIPE, SIG_IGN);
  signal (SIGPOLL, sigpoll_handler);
  if (daemonize) {
    signal (SIGHUP, sighup_handler);
    reopen_logs();
  }

  if (verbosity > 0) {
    fprintf (stderr, "Server started\n");
  }

  for (i = 0; ; i++) {
    if (verbosity > 0 && !(i & 1023)) {
      fprintf (stderr, "epoll_work(): %d out of %d connections, network bufers: %d used, %d out of %d allocated\n",
         active_connections, maxconn, NB_used, NB_alloc, NB_max);
    }
    epoll_work (57);

    if (sigpoll_cnt > 0) {
      if (verbosity > 1) {
        fprintf (stderr, "after epoll_work(), sigpoll_cnt=%d\n", sigpoll_cnt);
      }
      sigpoll_cnt = 0;
    }

    if (now != prev_time) {
      prev_time = now;
      cron();
    }

    if (epoll_pre_event) {
      epoll_pre_event();
    }

    if (quit_steps && !--quit_steps) break;
  }

  fprintf (stderr, "Quitting.\n");

  epoll_close (sfd);
  assert (close (sfd) >= 0);
}


/*
 *
 *    MAIN
 *
 */


void usage (void) {
  printf ("usage: %s [options]\n"
    "\t" VERSION_STR " compiled at " __DATE__ " " __TIME__ " by gcc " __VERSION__ " "
#ifdef __LP64__
    "64-bit"
#else
    "32-bit"
#endif
    " after commit " COMMIT "\n"
    "\tEmulates events queue\n",
    progname);

  parse_usage();
  exit (2);
}

int queue_parse_option (int val) {
  switch (val) {
    case 'C':
      config_filename = optarg;
      break;
    case 'e':
      engine_n = atoi (optarg);
      break;
    case 'H':
      http_port = atoi (optarg);
      break;
    case 'm':
      max_memory = atoi (optarg);
      if (max_memory < 1) {
        max_memory = 1;
      }
      max_memory *= 1048576;
      break;
    case 'N':
      news_port = atoi (optarg);
      break;
    case 'P':
      watchcat_port = atoi (optarg);
      break;
    case 'q':
      engine_id = atoi (optarg);
      break;
    case 'k':
      if (mlockall (MCL_CURRENT | MCL_FUTURE) != 0) {
        fprintf (stderr, "error: fail to lock paged memory\n");
      }
      break;
    case 'S':
      use_stemmer = 1;
      break;
    default:
      return -1;
  }
  return 0;
}

int main (int argc, char *argv[]) {
  dl_set_debug_handlers();
  progname = argv[0];
  now = time (NULL);

  remove_parse_option ('a');
  remove_parse_option ('B');
  remove_parse_option ('r');
  remove_parse_option (204);
  parse_option ("rpc-config", required_argument, NULL, 'C', "<cluster-description-file> description of queue-engine cluster used by rpc");
  parse_option ("total-engines", required_argument, NULL, 'e', "<total_engines> total number of engines");
  parse_option ("http-port", required_argument, 0, 'H', "<port> http port number (default %d)", http_port);
  parse_option ("memory-limit", required_argument, NULL, 'm', "<memory-limit> sets maximal size of used memory not including zmemory for struct conn_query in mebibytes");
  parse_option ("news-port", required_argument, 0, 'N', "<port> port number for communication with other queue-engines in cluster when not using rpc (default %d). 0 - disabled.", news_port);
  parse_option ("watchcat-port", required_argument, 0, 'P', "<port> port number for communication with watchcats (default %d). 0 - disabled.", watchcat_port);
  parse_option ("engine-number", required_argument, NULL, 'q', "<engine_number> number of this engine");
  parse_option ("lock-memory", no_argument, NULL, 'k', "lock paged memory");
  parse_option ("stemmer", no_argument, 0, 'S', "enable stemmer");

  parse_engine_options_long (argc, argv, queue_parse_option);
  if (argc != optind) {
    usage();
    return 2;
  }

  PID.port = port;

  init_is_letter();
  enable_is_letter_sigils();
  if (use_stemmer) {
    stem_init();
  }
/*
  fprintf (stderr, "TESTING\n");
  prepare_watchcat_str ("a b c d", 1);
  prepare_watchcat_str ("a a a a", 1);
  prepare_watchcat_str ("    a   a   a   a   ", 1);
  prepare_watchcat_str ("    aaaa   aaa   aaa   aaaaa   ", 1);
  prepare_watchcat_str ("###abc###", 1);
  prepare_watchcat_str ("#a a #b b ##c#d", 1);
  prepare_watchcat_str ("��������� �������� �� �������", 1);
  prepare_watchcat_str ("��������� ���������� #���������", 1);
  prepare_watchcat_str ("#test #Test Test #���������� ����������", 1);
  return 0;
*/

//  fprintf (stderr, "config name = %s\n", config_filename);
  if (config_filename != NULL) {
    use_rpc = 1;
  }

  if (use_rpc) {
    dl_open_file (0, config_filename, 0);
    parse_config();
    dl_close_file (0);
    assert (engine_n == CSN);
  }
//  fprintf (stderr, "config parced\n");

  dynamic_data_buffer_size = (1 << 26);//26 for struct conn_query
  init_dyn_data();

  if (!username && maxconn == MAX_CONNECTIONS && geteuid()) {
    maxconn = 1000; //not for root
  }

  if (raise_file_rlimit (maxconn + 16) < 0) {
    fprintf (stderr, "fatal: cannot raise open file limit to %d\n", maxconn + 16);
    exit (1);
  }

  if (port < PRIVILEGED_TCP_PORTS) {
    sfd = server_socket (port, settings_addr, backlog, 0);
    if (sfd < 0) {
      fprintf (stderr, "cannot open server socket at port %d: %m\n", port);
      exit (1);
    }
  }
/*
  if (http_port > 0 && http_port < PRIVILEGED_TCP_PORTS) {
    http_sfd = server_socket (http_port, settings_addr, backlog, 0);
    if (http_sfd < 0) {
      fprintf (stderr, "cannot open http server socket at port %d: %m\n", http_port);
      exit (1);
    }
  }*/

  aes_load_pwd_file (NULL);

  if (change_user (username) < 0 && !interactive) {
    fprintf (stderr, "fatal: cannot change user to %s\n", username ? username : "(none)");
    exit (1);
  }


  load_time = -mytime();

  init_all();

  load_time += mytime();

  start_time = time (NULL);

  start_server();

  free_all();
  return 0;
}
