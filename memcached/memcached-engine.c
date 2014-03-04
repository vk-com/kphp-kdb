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
              2010-2012 Arseny Smirnov
              2010-2012 Aliaksei Levin
              2012-2013 Vitaliy Valtman
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
#include <sys/wait.h>
#include <signal.h>

#include "net-events.h"
#include "net-buffers.h"
#include "server-functions.h"
#include "net-connections.h"
#include "net-memcache-server.h"
#include "net-crypto-aes.h"
#include "tl-memcached-const.h"

#include "memcached-data.h"

#include "net-rpc-server.h"
#define HISTORY

#define MAX_VALUE_LEN (1 << 20)
#define MAX_KEY_LEN (1 << 10)

#define VERSION "1.01"
#define VERSION_STR "memcached "VERSION

#define TCP_PORT 11211
#define UDP_PORT 11211

#define SEC_IN_MONTH (60 * 60 * 24 * 30)

#include "vv-tl-parse.h"

//extern int rpc_disable_crc32;
extern int rpc_disable_crc32_check;

/*
 *
 *    MEMCACHED PORT
 *
 */

int port = TCP_PORT, udp_port = UDP_PORT;
long max_memory = MAX_MEMORY;

struct in_addr settings_addr;
int interactive = 0;
int return_false_if_not_found = 0;
int oom_score_adj;

long long cmd_get, cmd_set, get_hits, get_missed, cmd_incr, cmd_decr, cmd_delete, cmd_version, cmd_stats, curr_items, total_items;

#define STATS_BUFF_SIZE (16 << 10)
int stats_buff_len;
char stats_buff[STATS_BUFF_SIZE];

int memcache_store (struct connection *c, int op, const char *key, int key_len, int flags, int delay, int size);
int memcache_get (struct connection *c, const char *key, int key_len);
int memcache_incr (struct connection *c, int op, const char *key, int key_len, long long arg);
int memcache_delete (struct connection *c, const char *key, int key_len);
int memcache_stats (struct connection *c);
int memcache_version (struct connection *c);

struct memcache_server_functions memcache_methods = {
  .execute = mcs_execute,
  .mc_store = memcache_store,
  .mc_get_start = mcs_get_start,
  .mc_get = memcache_get,
  .mc_get_end = mcs_get_end,
  .mc_incr = memcache_incr,
  .mc_delete = memcache_delete,
  .mc_version = memcache_version,
  .mc_stats = memcache_stats,
  .mc_check_perm = mcs_default_check_perm,
  .mc_init_crypto = mcs_init_crypto
};

int memcache_rpcs_execute (struct connection *c, int len, int op);
struct rpc_server_functions memcache_rpc_server = {
  .execute = memcache_rpcs_execute,
  .check_ready = server_check_ready,
  .flush_packet = flush_later,
  .rpc_check_perm = rpcs_default_check_perm,
  .rpc_init_crypto = rpcs_init_crypto,
  .memcache_fallback_type = &ct_memcache_server,
  .memcache_fallback_extra = &memcache_methods
};

#ifdef HISTORY

#define LAST_OPER_BUF_SIZE (1 << 10)
long long last_oper_hash[LAST_OPER_BUF_SIZE];
char last_oper_type[LAST_OPER_BUF_SIZE];
int cur_pos;

int last_oper_key_pos[LAST_OPER_BUF_SIZE];
char last_oper_key[LAST_OPER_BUF_SIZE * 1000];
int cur_key_pos;

#define STAT_PERIOD 61
#define STAT_CNT 9

long long stats[STAT_PERIOD][STAT_CNT];
long long *stats_now;

#define ADD_OPER(op_type)                                         \
{                                                                 \
  stats_now[op_type]++;                                           \
                                                                  \
  last_oper_type[cur_pos] = op_type;                              \
  last_oper_hash[cur_pos] = key_hash;                             \
  if (key_len + cur_key_pos + 10 >= LAST_OPER_BUF_SIZE * 1000) {  \
    cur_key_pos = 0;                                              \
  }                                                               \
                                                                  \
  memcpy (last_oper_key + cur_key_pos, key, key_len);             \
  last_oper_key_pos[cur_pos] = cur_key_pos;                       \
  cur_key_pos += key_len;                                         \
  last_oper_key[cur_key_pos++] = 0;                               \
                                                                  \
  if (++cur_pos == LAST_OPER_BUF_SIZE) {                          \
    cur_pos = 0;                                                  \
  }                                                               \
}

#else

#define ADD_OPER(op_type)

#endif

int memcache_store (struct connection *c, int op, const char *key, int key_len, int flags, int delay, int size) {
  cmd_set++;

  if (delay == 0) {
    delay = SEC_IN_MONTH;
  } else if (delay > SEC_IN_MONTH) {
    delay -= now;
    if (delay > SEC_IN_MONTH) {
      delay = SEC_IN_MONTH;
    }
  }

  if (delay < 0) {
    return -2;
  }

  delay += get_utime (CLOCK_MONOTONIC);

  if (verbosity > 0) {
    fprintf (stderr, "memcache_store: key='%s', key_len=%d, value_len=%d, flags = %d, exp_time = %d\n", key, key_len, size, flags, delay);
  }

  if (size < MAX_VALUE_LEN) {
    long long key_hash = get_hash (key, key_len);
    int x = get_entry (key, key_len, key_hash);

    ADD_OPER (0);
#ifdef HISTORY
    stats_now[4] += size;
#endif

    hash_entry_t *entry;

    if (x != -1) {
      if (op == mct_add) {
        return -2;
      }

      if (verbosity > 0) {
        fprintf (stderr, "found old entry x = %d\n", x);
      }
      entry = get_entry_ptr (x);

      zzfree (entry->data, entry->data_len + 1);

      del_entry_used (x);
      del_entry_time (x);
    } else {
      if (op == mct_replace) {
        return -2;
      }

      total_items++;

      x = get_new_entry ();

      if (verbosity > 0) {
        fprintf (stderr, "created new entry x = %d\n", x);
      }

      entry = get_entry_ptr (x);

      char *k;
      k = zzmalloc (key_len + 1);
      memcpy (k, key, key_len);
      k[key_len] = 0;

      entry->key = k;
      entry->key_len = key_len;
      entry->key_hash = key_hash;

      add_entry (x);
    }

    entry->data = zzmalloc (size + 1);
    assert (read_in (&c->In, entry->data, size) == size);
    entry->data[size] = 0;

    entry->data_len = size;
    entry->flags = flags;
    entry->exp_time = delay;

    add_entry_used (x);
    add_entry_time (x);

    return 1;
  }

  return -2;
}

char *operations[4] = {"set", "get", "increment", "delete"};

int memcache_get (struct connection *c, const char *key, int key_len) {
  if (verbosity > 0) {
    fprintf (stderr, "memcache_get: key='%s'\n", key);
  }
  cmd_get++;

#ifdef HISTORY
  if (key_len == 5 && !strncmp (key, "=^_^=", 5)) {
    int i;

    for (i = 0; i < LAST_OPER_BUF_SIZE && last_oper_type[i] >= 0; i++) {
      int x = get_entry_no_check (last_oper_hash[i]);

      return_one_key_flags (c, "operation_type", operations[(int)last_oper_type[i]], strlen (operations[(int)last_oper_type[i]]), 0);
      if (x != -1) {
        hash_entry_t *entry = get_entry_ptr (x);
        return_one_key_flags (c, entry->key, entry->data, entry->data_len, entry->flags);
      } else {
        return_one_key_flags (c, last_oper_key + last_oper_key_pos[i], "???", 3, 0);
      }
    }

    return 0;
  }

  if (key_len >= 10 && !strncmp (key, "=^_^=stats", 10)) {
    long long res[STAT_CNT] = {0};
    int i, j;
    for (i = 0; i < STAT_PERIOD; i++) {
      if (stats_now != stats[i]) {
        for (j = 0; j < STAT_CNT; j++) {
          res[j] += stats[i][j];
        }
      }
    }
    res[6] = stats_now[6] - stats[(now + 1) % STAT_PERIOD][6];
    res[7] = max_memory - get_memory_used();
    res[8] = get_del_by_LRU();

    if (key_len == 11 && key[10] == '%') {
      return_one_key_flags (c, key, (char *)res, STAT_CNT * sizeof (long long), 0);
      return 0;
    } else if (key_len == 10) {
      char *s = stats_buff;
      for (i = 0; i < STAT_CNT; i++) {
        if (i) {
          *s++ =',';
        }
        s += sprintf (s, "%lld", res[i]);
      }
      return_one_key_flags (c, key, stats_buff, s - stats_buff, 0);
      return 0;
    }
  }
#endif

  if (key_len == 15 && !strncmp (key, "=^_^=verbosity", 14) && '0' <= key[14] && key[14] <= '9') {
    verbosity = key[14] - '0';

    snprintf (stats_buff, STATS_BUFF_SIZE, "Verbosity is set to %d.", verbosity);

    return_one_key (c, key, stats_buff, strlen (stats_buff));

    return 0;
  }

  if (key_len == 9 && !strncmp (key, "=^_^=help", 9)) {
    snprintf (stats_buff, STATS_BUFF_SIZE,
                VERSION_STR " compiled at " __DATE__ " " __TIME__ " by gcc " __VERSION__ " (%d-bit) after commit " COMMIT
                "\nSupported commands:"
                "\n\tget =^_^=help\t\t\tthis help"
                "\n\tget =^_^=verbosity<new_verb>\tsets engine verbosity to <new_verb>, 0 <= <new_verb> <= 9"
#ifdef HISTORY
                "\n\tget =^_^=\t\t\tshow up to %d last operation performed with keys and values (when possible)"
                "\n\tget =^_^=stats\t\t\tshow stats for last %d seconds, stats are"
                "\n\t\t\t\t\tnumber of sets, gets, incr/decrements, deletions, total size of values in sets, gets,"
                "\n\t\t\t\t\tused cpu time in nanoseconds, free memory in bytes, number of deleted by LRU entries"
                "\n\tget =^_^=stats%%\t\t\tthe same, but in binary format, all stats are %d-bit"
#endif
                ,(int) sizeof(void *) * 8
#ifdef HISTORY
                ,LAST_OPER_BUF_SIZE
                ,STAT_PERIOD - 1
                ,(int) sizeof(long long) * 8
#endif
             );

    return_one_key (c, key, stats_buff, strlen (stats_buff));

    return 0;
  }

  long long key_hash = get_hash (key, key_len);
  int x = get_entry (key, key_len, key_hash);

  ADD_OPER (1);

  if (x != -1) {
    get_hits++;

    del_entry_used (x);
    add_entry_used (x);

    hash_entry_t *entry = get_entry_ptr (x);
#ifdef HISTORY
    stats_now[5] += entry->data_len;
#endif
    return_one_key_flags (c, key, entry->data, entry->data_len, entry->flags);
  } else {
    if (return_false_if_not_found) {
      return_one_key_flags (c, key, "b:0;", 4, 1);
    }
    get_missed++;
  }

  return 0;
}

int memcache_incr (struct connection *c, int op, const char *key, int key_len, long long arg) {
  if (verbosity > 0) {
    fprintf (stderr, "memcache_incr: op=%d, key='%s', val=%lld\n", op, key, arg);
  }
  if (arg < 0) {
    arg *= -1;
    op ^= 1;
  }

  if (op == 1) {
    cmd_decr++;
  } else {
    cmd_incr++;
  }

  long long key_hash = get_hash (key, key_len);
  int x = get_entry (key, key_len, key_hash);

  ADD_OPER (2);

  if (x == -1) {
    write_out (&c->Out, "NOT_FOUND\r\n", 11);
    return 0;
  }

  hash_entry_t *entry = get_entry_ptr (x);

  unsigned long long val = 0;
  int i, f = 1;

  for (i = 0; i < entry->data_len && f; i++) {
    if ('0' <= entry->data[i] && entry->data[i] <= '9') {
      val = val * 10 + entry->data[i] - '0';
    } else {
      f = 0;
    }
  }

  if (f == 0) {
    val = 0;
  }

  if (op == 1) {
    if ((unsigned long long)arg > val) {
      val = 0;
    } else {
      val -= arg;
    }
  } else {
    val += (unsigned long long)arg;
  }

  zzfree (entry->data, entry->data_len + 1);
  del_entry_used (x);
  add_entry_used (x);

  char buff[30];
  sprintf (buff, "%llu", val);

  int len = strlen (buff);

  char *d = zzmalloc (len + 1);
  memcpy (d, buff, len + 1);

  entry->data = d;
  entry->data_len = len;

  write_out (&c->Out, d, len);
  write_out (&c->Out, "\r\n", 2);

  return 0;
}

int memcache_delete (struct connection *c, const char *key, int key_len) {
  if (verbosity > 0) {
    fprintf (stderr, "memcache_delete: key='%s'\n", key);
  }
  cmd_delete++;

  long long key_hash = get_hash (key, key_len);
  int x = get_entry (key, key_len, key_hash);

  ADD_OPER (3);

  if (x != -1) {
    del_entry (x);
    write_out (&c->Out, "DELETED\r\n", 9);
    return 0;
  }

  write_out (&c->Out, "NOT_FOUND\r\n", 11);
  return 0;
}

int memcache_version (struct connection *c) {
  cmd_version++;
  write_out (&c->Out, "VERSION "VERSION"\r\n", 9 + sizeof (VERSION));
  return 0;
}

int memcache_stats (struct connection *c) {
  cmd_stats++;
  static char stats_buffer[65536];
  int stats_len = prepare_stats (c, stats_buffer, 65530);
  stats_len += snprintf (stats_buffer + stats_len, 65530 - stats_len,
        "del_by_LRU\t%lld\n"
        "current_time_gap\t%lld\n"
        "pid\t%d\n"
        "version\t" VERSION_STR " compiled at " __DATE__ " " __TIME__ " by gcc " __VERSION__ "\n"
        "pointer_size\t%d\n"
        "curr_items\t%d\n"
        "total_items\t%lld\n"
        "static_memory_used\t%ld\n"
        "current_memory_used\t%ld\n"
        "cmd_get\t%lld\n"
        "cmd_set\t%lld\n"
        "get_hits\t%lld\n"
        "get_misses\t%lld\n"
        "cmd_incr\t%lld\n"
        "cmd_decr\t%lld\n"
        "cmd_delete\t%lld\n"
        "cmd_version\t%lld\n"
        "cmd_stats\t%lld\n"
        "limit_max_dynamic_memory\t%ld\n",
        get_del_by_LRU(),
        get_time_gap(),
        getpid(),
        (int)(sizeof (int *) * 8),
        get_entry_cnt(),
        total_items,
        get_min_memory_bytes(),
        get_memory_used(),
        cmd_get,
        cmd_set,
        get_hits,
        get_missed,
        cmd_incr,
        cmd_decr,
        cmd_delete,
        cmd_version,
        cmd_stats,
        max_memory);
  write_out (&c->Out, stats_buffer, stats_len);
  write_out (&c->Out, "END\r\n", 5);
  return 0;
}

int tl_memcache_store (int op) {
  static char key[MAX_KEY_LEN + 1];
  int key_len = tl_fetch_string (key, MAX_KEY_LEN);
  int flags = tl_fetch_int ();
  int delay = tl_fetch_int ();
  int size = tl_fetch_string_len (MAX_VALUE_LEN - 1);
  tl_fetch_check_str_end (size);
  if (tl_fetch_error ()) {
    return -1;
  }
  cmd_set++;

  if (delay == 0) {
    delay = SEC_IN_MONTH;
  } else if (delay > SEC_IN_MONTH) {
    delay -= now;
    if (delay > SEC_IN_MONTH) {
      delay = SEC_IN_MONTH;
    }
  }

  if (delay < 0) {
    return -2;
  }

  delay += get_utime (CLOCK_MONOTONIC);

  if (verbosity > 0) {
    key[key_len] = 0;
    fprintf (stderr, "memcache_store: key='%s', key_len=%d, value_len=%d, flags = %d, exp_time = %d\n", key, key_len, size, flags, delay);
  }

  assert (size < MAX_VALUE_LEN);
  long long key_hash = get_hash (key, key_len);
  int x = get_entry (key, key_len, key_hash);

  ADD_OPER (0);
#ifdef HISTORY
  stats_now[4] += size;
#endif

  hash_entry_t *entry;

  if (x != -1) {
    if (op == mct_add) {
      tl_fetch_skip_string_data (size);
      assert (tl_fetch_check_eof ());
      tl_store_int (TL_BOOL_FALSE);
      return 0;
    }

    if (verbosity > 0) {
      fprintf (stderr, "found old entry x = %d\n", x);
    }
    entry = get_entry_ptr (x);

    zzfree (entry->data, entry->data_len + 1);

    del_entry_used (x);
    del_entry_time (x);
  } else {
    if (op == mct_replace) {
      tl_fetch_skip_string_data (size);
      assert (tl_fetch_check_eof ());
      tl_store_int (TL_BOOL_FALSE);
      return 0;
    }

    total_items++;

    x = get_new_entry ();

    if (verbosity > 0) {
      fprintf (stderr, "created new entry x = %d\n", x);
    }

    entry = get_entry_ptr (x);

    char *k;
    k = zzmalloc (key_len + 1);
    memcpy (k, key, key_len);
    k[key_len] = 0;

    entry->key = k;
    entry->key_len = key_len;
    entry->key_hash = key_hash;

    add_entry (x);
  }

  entry->data = zzmalloc (size + 1);
  //assert (read_in (&c->In, entry->data, size) == size);
  tl_fetch_string_data (entry->data, size);
  assert (tl_fetch_check_eof ());
  entry->data[size] = 0;

  entry->data_len = size;
  entry->flags = flags;
  entry->exp_time = delay;

  add_entry_used (x);
  add_entry_time (x);

  tl_store_int (TL_BOOL_TRUE);

  return 1;
}

int tl_memcache_incr (int op) {
  static char key[MAX_KEY_LEN + 1];
  int key_len = tl_fetch_string (key, MAX_KEY_LEN);
  long long arg = tl_fetch_long ();
  tl_fetch_end ();
  if (tl_fetch_error ()) {
    return -1;
  }
  if (verbosity > 0) {
    key[key_len] = 0;
    fprintf (stderr, "memcache_incr: op=%d, key='%s', val=%lld\n", op, key, arg);
  }
  if (arg < 0) {
    arg *= -1;
    op ^= 1;
  }

  if (op == 1) {
    cmd_decr++;
  } else {
    cmd_incr++;
  }

  long long key_hash = get_hash (key, key_len);
  int x = get_entry (key, key_len, key_hash);

  ADD_OPER (2);

  if (x == -1) {
    tl_store_int (TL_MEMCACHE_NOT_FOUND);
    return 0;
  }

  hash_entry_t *entry = get_entry_ptr (x);

  unsigned long long val = 0;
  int i, f = 1;

  for (i = 0; i < entry->data_len && f; i++) {
    if ('0' <= entry->data[i] && entry->data[i] <= '9') {
      val = val * 10 + entry->data[i] - '0';
    } else {
      f = 0;
    }
  }

  if (f == 0) {
    val = 0;
  }

  if (op == 1) {
    if ((unsigned long long)arg > val) {
      val = 0;
    } else {
      val -= arg;
    }
  } else {
    val += (unsigned long long)arg;
  }

  zzfree (entry->data, entry->data_len + 1);
  del_entry_used (x);
  add_entry_used (x);

  char buff[30];
  sprintf (buff, "%llu", val);

  int len = strlen (buff);

  char *d = zzmalloc (len + 1);
  memcpy (d, buff, len + 1);

  entry->data = d;
  entry->data_len = len;

  tl_store_int (TL_MEMCACHE_LONGVALUE);
  tl_store_long (val);
  tl_store_int (entry->flags);

  return 0;
}

int tl_memcache_delete (void) {
  static char key[MAX_KEY_LEN + 1];
  int key_len = tl_fetch_string (key, MAX_KEY_LEN);
  tl_fetch_end ();
  if (tl_fetch_error ()) {
    return -1;
  }
  if (verbosity > 0) {
    key[key_len] = 0;
    fprintf (stderr, "memcache_delete: key='%s'\n", key);
  }
  cmd_delete++;

  long long key_hash = get_hash (key, key_len);
  int x = get_entry (key, key_len, key_hash);

  ADD_OPER (3);

  if (x != -1) {
    del_entry (x);
    tl_store_int (TL_BOOL_TRUE);
    return 1;
  }
  tl_store_int (TL_BOOL_FALSE);
  return 0;
}

int tl_memcache_get (void) {
  static char key[MAX_KEY_LEN + 1];
  int key_len = tl_fetch_string (key, MAX_KEY_LEN);
  tl_fetch_end ();
  if (tl_fetch_error ()) {
    return -1;
  }
  if (verbosity > 0) {
    key[key_len] = 0;
    fprintf (stderr, "memcache_get: key='%s'\n", key);
  }
  cmd_get++;


  long long key_hash = get_hash (key, key_len);
  int x = get_entry (key, key_len, key_hash);

  ADD_OPER (1);

  if (x != -1) {
    get_hits++;

    del_entry_used (x);
    add_entry_used (x);

    hash_entry_t *entry = get_entry_ptr (x);
#ifdef HISTORY
    stats_now[5] += entry->data_len;
#endif
    tl_store_int (TL_MEMCACHE_STRVALUE);
    tl_store_string (entry->data, entry->data_len);
    tl_store_int (entry->flags);
  } else {
    get_missed++;
    tl_store_int (TL_MEMCACHE_NOT_FOUND);
  }

  return 0;
}

int memcache_rpcs_execute (struct connection *c, int op, int len) {
  if (op != RPC_INVOKE_REQ) {
    return SKIP_ALL_BYTES;
  }
  tl_fetch_init (c, len - 4);
  struct tl_query_header h;
  tl_fetch_query_header (&h);
  tl_store_init_keep_error (c, h.qid);
  assert (h.op == op || tl_fetch_error ());

  if (h.actor_id) {
    tl_fetch_set_error ("Memcached only support actor_id = 0", 0);
  }

  int f = tl_fetch_int ();
  int result = -1;

  switch (f) {
  case TL_MEMCACHE_SET:
    result = tl_memcache_store (mct_set);
    break;
  case TL_MEMCACHE_ADD:
    result = tl_memcache_store (mct_add);
    break;
  case TL_MEMCACHE_REPLACE:
    result = tl_memcache_store (mct_replace);
    break;
  case TL_MEMCACHE_INCR:
    result = tl_memcache_incr (0);
    break;
  case TL_MEMCACHE_DECR:
    result = tl_memcache_incr (1);
    break;
  case TL_MEMCACHE_DELETE:
    result = tl_memcache_delete ();
    break;
  case TL_MEMCACHE_GET:
    result = tl_memcache_get ();
    break;
  default:
    tl_fetch_set_error_format (TL_ERROR_UNKNOWN_FUNCTION_ID, "Unknown function id 0x%08x", f);
  }

  if (result < 0) {
    tl_fetch_set_error ("Unknown error occured", TL_ERROR_UNKNOWN);
  }
  tl_store_end ();
  assert (advance_skip_read_ptr (&c->In, 4 + tl_fetch_unread ()) == 4 + tl_fetch_unread ());
  return 0;

}

/*
 *
 *      END (PROXY MEMCACHE SERVER)
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


static void sigint_handler (const int sig) {
  const char message[] = "SIGINT handled.\n";
  kwrite (2, message, sizeof (message) - (size_t)1);

  exit (EXIT_SUCCESS);
}

static void sigterm_handler (const int sig) {
  const char message[] = "SIGTERM handled.\n";
  kwrite (2, message, sizeof (message) - (size_t)1);

  exit (EXIT_SUCCESS);
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

int sfd;

int child_pid;
int force_write_stats;

static void sigrtmax_handler (const int sig) {
  const char message[] = "got SIGUSR3, write stats.\n";
  kwrite (2, message, sizeof (message) - (size_t)1);

  force_write_stats = 1;
}

void check_child_status (void) {
  if (!child_pid) {
    return;
  }
  int status = 0;
  int res = waitpid (child_pid, &status, WNOHANG);
  if (res == child_pid) {
    if (WIFEXITED (status) || WIFSIGNALED (status)) {
      if (verbosity > 0) {
        fprintf (stderr, "child process %d terminated: exited = %d, signaled = %d, exit code = %d\n",
          child_pid, WIFEXITED (status) ? 1 : 0, WIFSIGNALED (status) ? 1 : 0, WEXITSTATUS (status));
      }
      child_pid = 0;
    }
  } else if (res == -1) {
    if (errno != EINTR) {
      fprintf (stderr, "waitpid (%d): %m\n", child_pid);
      child_pid = 0;
    }
  } else if (res) {
    fprintf (stderr, "waitpid (%d) returned %d???\n", child_pid, res);
  }
}

void fork_write_stats (void) {
  if (child_pid) {
    return;
  }

  int res = fork();

  if (res < 0) {
    fprintf (stderr, "fork: %m\n");
  } else if (!res) {
    assert (close (sfd) >= 0);

    write_stats();
    exit (0);
  } else {
    if (verbosity > 0) {
      fprintf (stderr, "created child process pid = %d\n", res);
    }
    child_pid = res;
  }
}



void cron (void) {
  create_all_outbound_connections();

  free_by_time (137);

#ifdef HISTORY
  stats_now = stats[now % STAT_PERIOD];
  memset (stats_now, 0, STAT_CNT * sizeof (long long));
  stats_now[6] = get_utime (CLOCK_PROCESS_CPUTIME_ID) * 1e9;
#endif

  if (force_write_stats) {
    fork_write_stats();
    force_write_stats = 0;
  }
  check_child_status();
}

void start_server (void) {
  int i;
  int prev_time;

  init_epoll();
  init_netbuffers();

  prev_time = 0;

  if (daemonize) {
    setsid();
  }

  if (change_user (username) < 0 && !interactive) {
    fprintf (stderr, "fatal: cannot change user to %s\n", username ? username : "(none)");
    exit (1);
  }

  init_listening_connection (sfd, &ct_rpc_server, &memcache_rpc_server);

  signal (SIGINT, sigint_handler);
  signal (SIGTERM, sigterm_handler);
  signal (SIGUSR1, sigusr1_handler);
  signal (SIGRTMAX, sigrtmax_handler);
  signal (SIGPIPE, SIG_IGN);
  if (daemonize) {
    signal (SIGHUP, sighup_handler);
  }

  for (i = 0; ; i++) {
    if (verbosity > 0 && !(i & 255)) {
      fprintf (stderr, "epoll_work(): %d out of %d connections, network buffers: %d used, %d out of %d allocated\n",
         active_connections, maxconn, NB_used, NB_alloc, NB_max);
    }
    epoll_work (57);
    if (now != prev_time) {
      prev_time = now;
      cron();
    }
    if (quit_steps && !--quit_steps) break;
  }

  epoll_close (sfd);
  assert (close (sfd) >= 0);
}

/*
 *
 *    MAIN
 *
 */

void help (void) {
  printf (VERSION_STR " compiled at " __DATE__ " " __TIME__ " by gcc " __VERSION__ " (%d-bit) after commit " COMMIT "\n"
          "[-p <port>]\tTCP port number to listen on (default: %d)\n"
          "[-U <port>]\tUDP port number to listen on (default: %d, 0 is off)\n"
          "[-d]\t\trun as a daemon\n"
          "[-k]\t\tlock down all paged memory [ignored]\n"
          "[-r]\t\tmaximize core file limit [ignored]\n"
          "[-u <username>]\tassume identity of <username> (only when run as root)\n"
          "[-m <size>]\tmax memory to use for items in mebibytes, minimum is %ld MiB, "
               "default is %ld MiB\n"
          "[-c <max_conn>]\tmax simultaneous connections, default is %d\n"
          "[-v]\t\tverbose\n"
          "[-vv]\t\tvery verbose\n"
          "[-h]\t\tprint this help and exit\n"
          "[-b <backlog>]\n"
          "[-n <nice>]\n"
          "[-O <oom-score-adj>]\n"
          "[-f]\t\treturn false to php if key not found\n"
          "[-l <log_name>]\tlog... about something\n",
          (int) sizeof(void *) * 8,
          TCP_PORT,
          UDP_PORT,
          get_min_memory(),
          get_min_memory() + (long)(MAX_MEMORY / 1048576),
          MAX_CONNECTIONS
         );
  exit (2);
}

int main (int argc, char *argv[]) {
  int i;
  rpc_disable_crc32_check = 1;

  while ((i = getopt (argc, argv, "b:c:l:p:U:m:n:dfhu:vrkO:C:")) != -1) {
    switch (i) {
    case 'v':
      verbosity++;
      break;
    case 'h':
      help();
      return 2;
    case 'b':
      backlog = atoi (optarg);
      if (backlog <= 0) {
        backlog = BACKLOG;
      }
      break;
    case 'c':
      maxconn = atoi (optarg);
      if (maxconn <= 0 || maxconn > MAX_CONNECTIONS) {
        maxconn = MAX_CONNECTIONS;
      }
      break;
    case 'm':
      max_memory = atoi (optarg);
      max_memory -= get_min_memory();
      if (max_memory < 1) {
        max_memory = 1;
      }
      max_memory *= 1048576;
      break;
    case 'n':
      errno = 0;
      nice (atoi (optarg));
      if (errno) {
        perror ("nice");
      }
      break;
    case 'p':
      port = atoi (optarg);
      break;
    case 'O':
      oom_score_adj = atoi (optarg);
      break;
    case 'U':
      udp_port = atoi (optarg);
      break;
    case 'u':
      username = optarg;
      break;
    case 'l':
      logname = optarg;
      break;
    case 'd':
      daemonize ^= 1;
      break;
    case 'f':
      return_false_if_not_found = 1;
      break;
    case 'r':
      // nothing to do
      break;
    case 'C':
      rpc_crc32_mode = atoi (optarg);
      rpc_disable_crc32_check = (rpc_crc32_mode & 1);
    case 'k':
      break;
      if (mlockall (/* MCL_CURRENT | */ MCL_FUTURE) != 0) {
        fprintf (stderr, "error: fail to lock paged memory\n");
      }
      break;
    }
  }

  if (argc != optind) {
    help();
    return 2;
  }

  if (!username && maxconn == MAX_CONNECTIONS && geteuid()) {
    maxconn = 1000; //not for root
  }

  if (dynamic_data_buffer_size > max_memory) {
    dynamic_data_buffer_size = max_memory;
  }

  if (MAX_ZMALLOC_MEM > 0) {
    init_dyn_data();
  }

  init_hash_table();
#ifdef HISTORY
  memset (last_oper_type, -1, LAST_OPER_BUF_SIZE * sizeof (char));
#endif

  if (raise_file_rlimit (maxconn + 16) < 0) {
    fprintf (stderr, "fatal: cannot raise open file limit to %d\n", maxconn + 16);
    exit (1);
  }

  if (oom_score_adj) {
    adjust_oom_score (oom_score_adj);
  }

  aes_load_pwd_file (0);

  sfd = server_socket (port, settings_addr, backlog, 0);
  if (sfd < 0) {
    fprintf (stderr, "cannot open server socket at port %d: %m\n", port);
    exit (1);
  }

  start_time = time (NULL);

  start_server();

  return 0;
}

