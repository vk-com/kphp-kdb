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
              2010-2011 Nikolai Durov
              2010-2011 Andrei Lopatin
              2011-2013 Vitaliy Valtman
              2011-2013 Anton Maydell
*/

#define	_FILE_OFFSET_BITS	64

#include <assert.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <ctype.h>
#include <unistd.h>
#include <aio.h>
#include <sys/wait.h>

#include "kfs.h"
#include "server-functions.h"
#include "net-connections.h"
#include "net-memcache-server.h"
#include "net-crypto-aes.h"
#include "net-parse.h"
#include "kdb-news-binlog.h"
#include "news-data.h"
#include "net-aio.h"
#include "am-stats.h"
#include "am-server-functions.h"

#include "vv-tl-parse.h"
#include "vv-tl-aio.h"
#include "news-tl.h"
#include "TL/constants.h"
#include "news-interface-structures.h"
#include "common-data.h"

#define VERSION_STR	"news-engine-0.2-r10"
const char FullVersionStr[] = VERSION_STR " compiled at " __DATE__ " " __TIME__ " by gcc " __VERSION__ " "
#ifdef __LP64__
  "64-bit"
#else
  "32-bit"
#endif
" after commit " COMMIT;

#define mytime() (get_utime(CLOCK_MONOTONIC))

/*
 *
 *		NEWS ENGINE
 *
 */

int index_mode;

/* stats counters */
static long long cmd_stats;
static int last_collect_garbage_time;
static long long binlog_loaded_size;
static long long delete_queries, undelete_queries, update_queries, minor_update_queries;
static long long get_raw_recommend_updates_queries, get_max_raw_recommend_updates_queries, skipped_set_recommend_updates_queries;
static long long tot_raw_recommend_updates_records;

static double binlog_load_time, loadavg_last_minute = 0.0;
#define STATS_BUFF_SIZE	0x100000
char stats_buff[STATS_BUFF_SIZE];
extern int binlog_read;

long long jump_log_pos;
int jump_log_ts;
unsigned jump_log_crc32;
double index_load_time;
int udp_enabled;

/*
 *
 *		SERVER
 *
 */

extern struct aio_connection *WaitAio;
extern conn_query_type_t aio_metafile_query_type;

int news_wakeup (struct connection *c);
int news_alarm (struct connection *c);

conn_type_t ct_news_engine_server = {
  .magic = CONN_FUNC_MAGIC,
  .title = "news_engine_server",
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
  .wakeup = news_wakeup,
  .alarm = news_alarm,
#ifndef NOAES
  .crypto_init = aes_crypto_init,
  .crypto_free = aes_crypto_free,
  .crypto_encrypt_output = aes_crypto_encrypt_output,
  .crypto_decrypt_input = aes_crypto_decrypt_input,
  .crypto_needed_output_bytes = aes_crypto_needed_output_bytes,
#endif
};

int memcache_delete (struct connection *c, const char *key, int key_len);
int memcache_store (struct connection *c, int op, const char *key, int key_len, int flags, int delay, int size);
int memcache_get (struct connection *c, const char *key, int key_len);
int memcache_get_start (struct connection *c);
int memcache_get_end (struct connection *c, int key_count);
int memcache_stats (struct connection *c);

struct memcache_server_functions memcache_methods = {
  .execute = mcs_execute,
  .mc_store = memcache_store,
  .mc_get_start = memcache_get_start,
  .mc_get = memcache_get,
  .mc_get_end = memcache_get_end,
  .mc_incr = mcs_incr,
  .mc_delete = memcache_delete,
  .mc_version = mcs_version,
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
  .memcache_fallback_type = &ct_news_engine_server,
  .memcache_fallback_extra = &memcache_methods
};

extern int metafiles_load_errors;
extern int metafiles_loaded;
extern int metafiles_load_success;
extern int metafiles_unloaded;
extern long long tot_aio_loaded_bytes;
extern long long allocated_metafiles_size;
extern int small_users_number;
extern int large_users_number;
extern int new_users_number;
extern long long metafiles_cache_miss;
extern long long metafiles_cache_ok;
extern long long metafiles_cache_loading;
extern int bookmarks_ptr;
extern long long max_allocated_metafiles_size;

int news_prepare_stats (struct connection *c) {
  const char *const sobjs = COMM_MODE ? "comments" : "items";
  stats_buffer_t sb;
  sb_prepare (&sb, c, stats_buff, STATS_BUFF_SIZE);
  sb_memory (&sb, AM_GET_MEMORY_USAGE_SELF);
  sb_printf (&sb, "user_group_mode\t%d\n", ug_mode);

  SB_BINLOG;
  SB_INDEX;

  SB_PRINT_QUERIES (delete_queries);
  SB_PRINT_QUERIES (undelete_queries);
  SB_PRINT_QUERIES (update_queries);
  SB_PRINT_QUERIES (minor_update_queries);

  if (RECOMMEND_MODE) {
    SB_PRINT_QUERIES (get_raw_recommend_updates_queries);
    SB_PRINT_QUERIES (get_max_raw_recommend_updates_queries);
    SB_PRINT_QUERIES (skipped_set_recommend_updates_queries);
    sb_printf (&sb, "raw_recommend_updates_avg_returned_records\t%.3lf\n", safe_div (tot_raw_recommend_updates_records, get_raw_recommend_updates_queries));
  }

  SB_PRINT_I32 (max_news_days);

  sb_printf (&sb,
    "memory_users\t%d\n"
    "max_uid\t%d\n"
    "total_items\t%d\n"
    "total_places\t%d\n"
    "total_comments\t%d\n"
    "loadavg_last_minute\t%.6f\n"
    "garbage_collection_uptime\t%d\n"
    "%s_removed_in_process_new\t%lld\n"
    "%s_removed_in_prepare_updates\t%lld\n"
    "%s_removed_by_garbage_collector\t%lld\n",
    tot_users,
    max_uid,
    items_kept,
    tot_places,
    comments_kept,
    loadavg_last_minute,
    now - last_collect_garbage_time,
    sobjs, items_removed_in_process_new,
    sobjs, items_removed_in_prepare_updates,
    sobjs, garbage_objects_collected);

  if (RECOMMEND_MODE) {
    SB_PRINT_I64 (dups_removed_in_process_raw_updates);
    SB_PRINT_I64 (dups_users_removed_from_urlist);
  }

  SB_PRINT_I64 (garbage_users_collected);

  if (COMM_MODE) {
    sb_printf (&sb,
      "small_users_in_index\t%d\n"
      "large_users_in_index\t%d\n"
      "users_with_new_bookmarks\t%d\n"
      "new_bookmarks\t%d\n"
      "tot_aio_queries\t%lld\n"
      "active_aio_queries\t%lld\n"
      "expired_aio_queries\t%lld\n"
      "avg_aio_query_time\t%.6f\n"
      "metafiles_load_errors\t%d\n"
      "metafiles_load_success\t%d\n"
      "metafiles_in_memory\t%d\n"
      "metafiles_unloaded\t%d\n"
      "total_aio_loaded_bytes\t%lld\n"
      "allocated_metafiles_size\t%lld\n"
      "max_allocated_metafiles_size\t%lld\n"
      "metafiles_cache_miss\t%lld\n"
      "metafiles_cache_ok\t%lld\n"
      "metafiles_cache_loading\t%lld\n",
    small_users_number,
    large_users_number,
    new_users_number,
    (bookmarks_ptr > 0 ? bookmarks_ptr : -bookmarks_ptr) - new_users_number,
    tot_aio_queries,
    active_aio_queries,
    expired_aio_queries,
    tot_aio_queries > 0 ? total_aio_time / tot_aio_queries : 0,
    metafiles_load_errors,
    metafiles_load_success,
    metafiles_loaded,
    metafiles_unloaded,
    tot_aio_loaded_bytes,
    allocated_metafiles_size,
    max_allocated_metafiles_size,
    metafiles_cache_miss,
    metafiles_cache_ok,
    metafiles_cache_loading);
  }

  sb_printf (&sb, "version\t%s\n", FullVersionStr);

  return sb.pos;
}



extern struct aio_connection *WaitAio;
extern conn_query_type_t aio_metafile_query_type;

int news_wait (struct connection *c) {
  if (verbosity > 2) {
    fprintf (stderr, "wait for aio..\n");
  }
  if (c->flags & C_INTIMEOUT) {
    if (verbosity > 1) {
      fprintf (stderr, "news-engine: IN TIMEOUT (%p)\n", c);
    }
    return 0;
  }
  if (c->Out.total_bytes > 8192) {
    c->flags |= C_WANTWR;
    c->type->writer (c);
  }
//    fprintf (stderr, "memcache_get_nonblock returns -2, WaitAio=%p\n", WaitAio);
  if (!WaitAioArrPos) {
    return 0;
  }
  /*if (!WaitAio) {
    fprintf (stderr, "WaitAio=0 - no memory to load user metafile, query dropped.\n");
    return 0;
  }*/
  int i;
  for (i = 0; i < WaitAioArrPos; i++) {
    assert (WaitAioArr[i]);
    create_aio_query (WaitAioArr[i], c, 0.7, &aio_metafile_query_type);
  }
  //conn_schedule_aio (WaitAio, c, 0.7, &aio_metafile_query_type);
  set_connection_timeout (c, 0.5);
  return 1;
}

int news_wakeup (struct connection *c) {

  if (verbosity > 1) {
    fprintf (stderr, "news_wakeup (%p)\n", c);
  }

  c->generation = ++conn_generation;
  c->pending_queries = 0;
  c->status = conn_reading_query;

  clear_connection_timeout (c);

  if (c->Out.total_bytes > 0) {
    c->flags |= C_WANTWR;
  }

  return 0;
}

int news_alarm (struct connection *c) {
  if (verbosity > 1) {
    fprintf (stderr, "news connection %d timeout alarm, %d queries pending, status=%d\n", c->fd, c->pending_queries, c->status);
  }
  assert (c->status == conn_wait_aio);
  c->flags |= C_INTIMEOUT;
  return news_wakeup (c);
}


int memcache_stats (struct connection *c) {
  cmd_stats++;
  int stats_len = news_prepare_stats (c);
  write_out (&c->Out, stats_buff, stats_len);
  write_out (&c->Out, "END\r\n", 5);
  return 0;
}

#define	MAX_QUERY	262144
int QL, Q[MAX_QUERY];

static inline void init_tmp_buffers (struct connection *c) {
  free_tmp_buffers (c);
  c->Tmp = alloc_head_buffer ();
  assert (c->Tmp);
}

struct keep_mc_store {
  int user_id;
  //int no_reply;
  int len;
  //int op;
  //int type;
  //int cat;
  char text[0];
};

#define MAX_STORE_BUFFER_SIZE 10240

inline int storing_hugelist (int act) {
  return (1 << act) & ((1 << 3) + (1 << 4) + (1 << 8));
}

int memcache_store (struct connection *c, int op, const char *key, int key_len, int flags, int delay, int size) {
  c->flags &= ~C_INTIMEOUT;
  int user_id, type = 0, res = 0, s;
  int user, group, owner, place, item, mask, action, item_creation_time;
  double rate;
  static char value_buff[MAX_STORE_BUFFER_SIZE + 1];
  struct keep_mc_store Data;

  int dog_len = get_at_prefix_length (key, key_len);
  key += dog_len;
  key_len -= dog_len;

  if (COMM_MODE && key_len >= 9 && sscanf (key, "bookmark_%d_%d_%d:%d", &type, &owner, &place, &user_id) == 4) {
    advance_skip_read_ptr (&c->In, size);
    return do_add_del_bookmark (user_id, type, owner, place, 1);
  }

  if (size >= 131072) {
    return -2;
  }

  if (op != mct_set) {
    return -2;
  }

  int act = 0;
  if (UG_MODE && sscanf (key, "update%d", &user_id) == 1 && user_id && (user_id ^ ug_mode) >= 0) {
    act = 1;
  } else if (UG_MODE && sscanf (key, "mask%d", &user_id) == 1 && user_id && (user_id ^ ug_mode) >= 0) {
    act = 2;
  } else if (UG_MODE && sscanf (key, "userlist%d", &user_id) == 1 && user_id < 0) {
    act = 3;
  } else if (COMM_MODE && sscanf (key, "objectlist%d", &user_id) == 1 && user_id < 0) {
    act = 4;
  } else if (COMM_MODE && !strncmp (key, "comm_update", 11)) {
    act = 5;
    user_id = 0;
  } else if (NOTIFY_MODE && sscanf (key, "notification_update%d", &user_id) == 1 && user_id) {
    act = 6;
  } else if (RECOMMEND_MODE && sscanf (key, "recommend_update%d", &user_id) == 1 && user_id) {
    act = 7;
  } else if (RECOMMEND_MODE && sscanf (key, "urlist%d", &user_id) == 1 && user_id < 0) {
    act = 8;
  } else if (RECOMMEND_MODE && sscanf (key, "recommend_rate%d,%d", &type, &action) == 2) {
    act = 9;
  }

  vkprintf (2, "act = %d, user_id = %d\n", act, user_id);

  if (!act) {
    return -2;
  }

  //if (act != 3 && act != 4 && act != 8) {
  if (!storing_hugelist (act)) {
    if (size >= MAX_STORE_BUFFER_SIZE) {
      return -2;
    }
    assert (read_in (&c->In, value_buff, size) == size);
    value_buff[size] = 0;
  }

  minor_update_queries++;

  switch (act) {
  case 1:
    res = 0;
    if (sscanf (value_buff, "%d,%d,%d,%d,%d,%d", &type, &user, &group, &owner, &place, &item) >= 6) {
      res = do_process_news_item (user_id, type, user, group, owner, place, item);
      update_queries++;
    }
    break;
  case 2:
    res = 0;
    if (sscanf (value_buff, "%d", &mask) == 1) {
      res = do_set_privacy (user_id, mask);
      update_queries++;
    }
    break;
  case 3:
    s = np_news_parse_list (Q, MAX_QUERY, 1, &c->In, size);
    res = 0;
    if (s >= 0) {
      Data.user_id = user_id;
      Data.len = s;
      init_tmp_buffers (c);
      write_out (c->Tmp, &Data, sizeof (Data));
      write_out (c->Tmp, Q, s * 4);
      res = 1;
    }
    vkprintf (2, "stored large user list: size = %d, first entry = %d, res = %d\n", s, Q[0], res);
    break;
  case 4:
    s = np_news_parse_list (Q, MAX_QUERY, 3, &c->In, size);
    res = 0;
    if (s >= 0) {
      Data.user_id = user_id;
      Data.len = s;
      init_tmp_buffers (c);
      write_out (c->Tmp, &Data, sizeof (Data));
      write_out (c->Tmp, Q, s * 12);
      res = 1;
    }
    vkprintf (2, "stored large object list: size = %d, first entry = %d_%d_%d, res = %d\n", s, Q[0], Q[1], Q[2], res);
    break;
  case 5:
    res = 0;
    if (sscanf (value_buff, "%d,%d,%d,%d,%d,%d", &type, &user, &group, &owner, &place, &item) >= 6) {
      res = do_process_news_comment (type, user, group, owner, place, item);
      update_queries++;
    }
    break;
  case 6:
    res = 0;
    if (sscanf (value_buff, "%d,%d,%d,%d,%d", &type, &user, &owner, &place, &item) >= 5) {
      res = do_process_news_notify (user_id, type, user, owner, place, item);
    }
    break;
  case 7:
    res = 0;
    if (sscanf (value_buff, "%d,%d,%d,%d,%d,%d", &type, &owner, &place, &action, &item, &item_creation_time) == 6) {
      res = do_process_news_recommend (user_id, type, owner, place, action, item, item_creation_time);
      if (res <= 0) {
        skipped_set_recommend_updates_queries++;
      }
    }
    break;
  case 8:
    s = np_news_parse_list (Q, MAX_QUERY, 2, &c->In, size);
    res = 0;
    if (s >= 0) {
      Data.user_id = user_id;
      Data.len = s;
      init_tmp_buffers (c);
      write_out (c->Tmp, &Data, sizeof (Data));
      write_out (c->Tmp, Q, s * 8);
      res = 1;
    }
    vkprintf (2, "stored large (user-rating) list: size = %d, first entry = %d_%d, res = %d\n", s, Q[0], Q[1], res);
    break;
  case 9:
    if (sscanf (value_buff, "%lf", &rate) == 1) {
      res = do_set_recommend_rate (type, action, rate);
    }
    break;
  default:
    assert (0);
    break;
  }

  if (!storing_hugelist (act)) {
    vkprintf (4, "act: %d, free_tmp_buffers\n", act);
    free_tmp_buffers (c);
  }

  return res;
}

int memcache_delete (struct connection *c, const char *key, int key_len) {
  c->flags &= ~C_INTIMEOUT;
  int user_id, type, owner, place, item, res = 0;
  switch (*key) {
  case 'u':
    if (UG_MODE && sscanf (key, "updates%d", &user_id) == 1 && user_id && (user_id ^ ug_mode) >= 0) {
      delete_queries++;
      res = do_delete_user (user_id);
    } else if (COMM_MODE && sscanf (key, "undelete_comm_update%d_%d_%d_%d", &type, &owner, &place, &item) == 4 && place > 0 && item > 0) {
      undelete_queries++;
      res = do_undelete_comment (type, owner, place, item);
    } else if (NOTIFY_MODE && sscanf (key, "undelete_notification_update%d_%d_%d_%d", &type, &owner, &place, &item) == 4 && place > 0 && item >= 0) {
      undelete_queries++;
      res = do_undelete_comment (type, owner, place, item);
    } else if (NOTIFY_MODE && sscanf (key, "undelete_notification_user_update%d_%d_%d_%d_%d", &user_id, &type, &owner, &place, &item) == 5 && place > 0 && item >= 0) {
      undelete_queries++;
      res = do_undelete_user_comment (user_id, type, owner, place, item);
    }
    break;
  case 'c':
    if (COMM_MODE && sscanf (key, "comm_updates%d_%d_%d", &type, &owner, &place) == 3 && place > 0) {
      delete_queries++;
      res = do_delete_place (type, owner, place);
    } else if (COMM_MODE && sscanf (key, "comm_update%d_%d_%d_%d", &type, &owner, &place, &item) == 4 && place > 0 && item > 0) {
      delete_queries++;
      res = do_delete_comment (type, owner, place, item);
    }
    break;
  case 'b':
    if (COMM_MODE && sscanf (key, "bookmark_%d_%d_%d:%d", &type, &owner, &place, &user_id) == 4) {
      res = do_add_del_bookmark (user_id, type, owner, place, 0);
    }
    break;
  case 'n':
    if (NOTIFY_MODE && sscanf (key, "notification_updates%d_%d_%d", &type, &owner, &place) == 3 && place > 0) {
      delete_queries ++;
      res = do_delete_place (type, owner, place);
    } else if (NOTIFY_MODE && sscanf (key, "notification_update%d_%d_%d_%d", &type, &owner, &place, &item) == 4 && place > 0) {
      delete_queries++;
      res = do_delete_comment (type, owner, place, item);
    } else if (NOTIFY_MODE && sscanf (key, "notification_user_update%d_%d_%d_%d_%d", &user_id, &type, &owner, &place, &item) == 5 && place > 0) {
      delete_queries++;
      res = do_delete_user_comment (user_id, type, owner, place, item);
    }

    break;
  }

  if (res > 0) {
    write_out (&c->Out, "DELETED\r\n", 9);
  } else {
    write_out (&c->Out, "NOT_FOUND\r\n", 11);
  }

  return 0;
}

static int exec_get_raw_updates (struct connection *c, const char *key, int key_len, int dog_len) {
  int user_id;
  int mask = -1, st_time = 0, end_time = 0, x = 0, raw = 0;
  struct keep_mc_store *Data = 0;

  if (verbosity >= 2) {
    fprintf (stderr, "exec_get_raw_updates (%p, %s, %d, %d)\n", c, key, key_len, dog_len);
  }

  if (c->Tmp) {
    Data = (struct keep_mc_store *) c->Tmp->start;
  }

  if (*key == '%') {
    raw = 1;
  }

  if ((sscanf (key+raw, "raw_updates%d[%d,%d]:%n", &mask, &st_time, &end_time, &x) >= 3 ||
       sscanf (key+raw, "raw_updates%n%d", &x, &user_id) >= 1) && x > 0) {
    if (verbosity >= 3) {
      fprintf (stderr, "mask = %d, st_time = %d, end_time = %d, x = %d, user_id = %d\n", mask, st_time, end_time, x, user_id);
    }
    if (x == 11) { mask = -1; }
    x += raw;
    QL = np_parse_list_str (Q, MAX_QUERY, 1, key + x, key_len - x);
    if (verbosity >= 3) { fprintf (stderr, "QL = %d\n", QL); }
    if (QL == 1 && Q[0] < 0 && Data && Data->user_id == Q[0] && Data->len) {
      if (verbosity > 1) {
        fprintf (stderr, "found userlist %d, %d entries\n", Data->user_id, Data->len);
      }
      advance_read_ptr (c->Tmp, sizeof (struct keep_mc_store));
      QL = Data->len;
      if (QL > MAX_QUERY) { QL = MAX_QUERY; }
      x = read_in (c->Tmp, Q, QL*4);
      assert (x == QL*4);
      if (verbosity > 1 && QL > 0) {
        fprintf (stderr, "first entry: %d\n", Q[0]);
      }
    }

    clear_result_buffer ();

    int i, best = -1;
    for (i = 0; i < QL; i++) {
      user_id = Q[i];
      int res = prepare_raw_updates (user_id, mask, st_time, end_time);
      if (verbosity > 1) {
        fprintf (stderr, "prepare_raw_updates(%d) = %d\n", user_id, res);
      }
      if (res > best) {
        best = res;
      }
    }
    if (best >= 0) {
      return return_one_key_list (c, key-dog_len, dog_len + key_len, /*(R_end - R) / 9*/ 0x7fffffff, -raw, R, R_end - R);
    }
  }
  return 0;
}

static int exec_get_raw_notification_updates (struct connection *c, const char *key, int key_len, int dog_len) {
  int user_id;
  int mask = -1, st_time = 0, end_time = 0, x = 0, raw = 0;

  if (verbosity >= 2) {
    fprintf (stderr, "exec_get_raw_notifications (%p, %s, %d, %d)\n", c, key, key_len, dog_len);
  }

  if (*key == '%') {
    raw = 1;
  }

  if ((sscanf (key+raw, "raw_notification_updates%d[%d,%d]:%n%d", &mask, &st_time, &end_time, &x, &user_id) >= 4 ||
       sscanf (key+raw, "raw_notification_updates%n%d", &x, &user_id) >= 1) && x > 0) {
    if (verbosity >= 3) {
      fprintf (stderr, "mask = %d, st_time = %d, end_time = %d, x = %d, user_id = %d\n", mask, st_time, end_time, x, user_id);
    }
    x += raw;

    clear_result_buffer ();
    int res = prepare_raw_notify_updates (user_id, mask, st_time, end_time, 0);
    if (verbosity > 1) {
      fprintf (stderr, "prepare_raw_updates(%d) = %d\n", user_id, res);
    }
    if (res >= 0) {
      return return_one_key_list (c, key-dog_len, dog_len + key_len, /*(R_end - R) / 9*/ 0x7fffffff, -raw, R, R_end - R);
    }
  }
  return 0;
}

struct notify_item {
  int user_id;
  int date;
  int random_tag;
  int type;
  int user;
  int owner;
  int place;
  int item;
};

int notify_compare (const void *a, const void *b) {
  #define aa ((struct notify_item *)a)
  #define bb ((struct notify_item *)b)
  if (aa->type != bb->type) {
    return aa->type - bb->type;
  }
  if (aa->owner != bb->owner) {
    return aa->owner - bb->owner;
  }
  if (aa->place != bb->place) {
    return aa->place - bb->place;
  }
  if (aa->date != bb->date) {
    return bb->date - aa->date;
  }
  if (aa->random_tag > bb->random_tag) {
    return 1;
  }
  if (aa->random_tag < bb->random_tag) {
    return -1;
  }
  return 0;
  #undef aa
  #undef bb
}

int return_one_key_flags (struct connection *c, const char *key, char *val, int vlen) {
  static char buff[65536];
  int l = sprintf (buff, "VALUE %s 1 %d\r\n", key, vlen);
  assert (l <= 65536);
  write_out (&c->Out, buff, l);
  write_out (&c->Out, val, vlen);
  write_out (&c->Out, "\r\n", 2);
  return 0;
}

struct notify_group {
  int pos;
  int date;
  int num;
  int random;
};

int notify_group_compare (const void *a, const void *b) {
  #define aa ((struct notify_group *)a)
  #define bb ((struct notify_group *)b)
  if (aa->date != bb->date) {
    return bb->date - aa->date;
  }
  if (aa->pos != bb->pos) {
    return aa->pos - bb->pos;
  }
  if (aa->num != bb->num) {
    return aa->num - bb->num;
  }
  return 0;
  #undef aa
  #undef bb
}

struct notify_group notify_groups [MAX_GROUPS];

#define RR ((struct notify_item *)R)

static int prepare_notify_groups (int user_id, int mask, int date, int end_date, int grouping, int timestamp, int limit, int *found, int *total_groups) {
  if (timestamp <= 0) {
    timestamp = date;
  }
  if (limit <= 0 || limit > MAX_GROUPS) {
    limit = MAX_GROUPS;
  }
  clear_result_buffer ();
  int res = prepare_raw_notify_updates (user_id, mask, timestamp, end_date, 1);
  if (res < 0) {
    *found = *total_groups = 0;
    return -1;
  }
  int f = (R_end - R) / 8;
  if (f > MAX_GROUPS) {
    f = MAX_GROUPS;
  }
  if (grouping) {
    qsort (R, f, 8 * sizeof (int), notify_compare);
  }
  int offset = 0, t = 0;
  int i;
  if (!grouping) {
    int cc = 0;
    for (i = 0; i < f; i++) if (RR[i].random_tag >= 0) {
      notify_groups[cc].pos = i;
      notify_groups[cc].date = RR[i].date;
      notify_groups[cc].num = 1;
      cc ++;
    }
    t = cc;
  } else {
    int cur_pos = 0;
    while (cur_pos < f) {
      int tpos = cur_pos + 1;
      while (tpos < f && (RR[tpos].date - offset) / 86400 == (RR[cur_pos].date - offset) / 86400 &&
           RR[tpos].type == RR[cur_pos].type && RR[tpos].owner == RR[cur_pos].owner && RR[tpos].place == RR[cur_pos].place) {
        tpos ++;
      }
      notify_groups[t].pos = cur_pos;
      notify_groups[t].num = tpos - cur_pos;
      if (RR[cur_pos].random_tag > 0) {
        notify_groups[t].date = RR[cur_pos].date;
      } else {
        if (tpos - cur_pos > 1) {
          notify_groups[t].date = RR[cur_pos + 1].date;
        } else {
          notify_groups[t].date = RR[cur_pos].date - ((RR[cur_pos].date - offset) % 86400);
        }
      }
      t++;
      cur_pos = tpos;
    }
  }
  qsort (notify_groups, t, sizeof (struct notify_group), notify_group_compare);
  if (t > limit) {
    t = limit;
  }
  int x = t;
  if (!grouping) {
    for (i = 0; i < t; i++) {
      if (RR[notify_groups[i].pos].random_tag < 0) {
        x--;
      }
    }
  }
  *found = f;
  *total_groups = t;
  return x;
}

static int exec_get_notification_updates (struct connection *c, const char *key, int key_len, int dog_len) {
  int user_id;
  int mask = -1, timestamp = 0, end_date = 0, raw = 0, date = 0, grouping = 0, limit = 0;

  static char value_buff[10000000];
  char *ptr = value_buff;
  char *end_ptr = value_buff + sizeof (value_buff);

  if (verbosity >= 2) {
    fprintf (stderr, "exec_get_notifications (%p, %s, %d, %d)\n", c, key, key_len, dog_len);
  }

  if (*key == '%') {
    raw = 1;
  }

  if (sscanf (key+raw, "notification_updates%d_%d,%d_%d>%d#%d:%d", &mask, &date, &end_date, &grouping, &timestamp, &limit, &user_id) == 7 ||
      sscanf (key+raw, "notification_updates%d_%d,%d_%d>%d:%d", &mask, &date, &end_date, &grouping, &timestamp, &user_id) == 6 ||
      sscanf (key+raw, "notification_updates%d_%d,%d_%d#%d:%d", &mask, &date, &end_date, &grouping, &limit, &user_id) == 6 ||
      sscanf (key+raw, "notification_updates%d_%d,%d_%d:%d", &mask, &date, &end_date, &grouping, &user_id) == 5) {
  } else {
    return 0;
  }

  int found, total_groups;
  int x = prepare_notify_groups (user_id, mask, date, end_date, grouping, timestamp, limit, &found, &total_groups);
  if (x < 0) {
    return 0;
  }

  int i;
  ptr += snprintf (ptr, end_ptr - ptr, "a:%d:{", x);

  for (i = 0; i < total_groups; i++) {
    char s[100];
    int cur_pos = notify_groups[i].pos;
    int tpos = (notify_groups[i].num > grouping) ? cur_pos + grouping : cur_pos + notify_groups[i].num;
    assert (tpos <= found);
    if (RR[cur_pos].random_tag < 0 && !grouping) {
      continue;
    }

    ptr += snprintf (ptr, end_ptr - ptr, "i:%d;a:%d:{", i, 5);
    ptr += snprintf (ptr, end_ptr - ptr, "i:0;i:%d;i:1;i:%d;i:2;i:%d;", RR[cur_pos].type, RR[cur_pos].owner, RR[cur_pos].place);
    if (!grouping) {
      int l = snprintf (s, 100, "%d_%d", RR[cur_pos].user, RR[cur_pos].item);
      ptr += snprintf (ptr, end_ptr - ptr, "i:3;s:%d:\"%s\";i:4;i:%d;", l, s, RR[cur_pos].date);
    } else {
      int j;
      int total = notify_groups[i].num;
      if (RR[cur_pos].random_tag <= 0) {
        total = -RR[cur_pos].random_tag;
        if (tpos < cur_pos + notify_groups[i].num) {
          tpos ++;
        }
        cur_pos ++;
      }
      for (j = 3; j < 5; j++) {
        ptr += snprintf (ptr, end_ptr - ptr, "i:%d;a:%d:{i:0;i:%d;", j, tpos - cur_pos + 1, total);
        int k;
        for (k = cur_pos; k < tpos; k++) {
          if (j == 3) {
            int l = snprintf (s, 100, "%d_%d", RR[k].user, RR[k].item);
            ptr += snprintf (ptr, ptr - end_ptr, "i:%d;s:%d:\"%s\";", k - cur_pos + 1, l, s);
          } else {
            ptr += snprintf (ptr, ptr - end_ptr, "i:%d;i:%d;", k - cur_pos + 1, RR[k].date);
          }
        }
        ptr += snprintf (ptr, end_ptr - ptr, "}");
      }
    }
    ptr += snprintf (ptr, end_ptr - ptr, "}");
  }

  ptr += snprintf (ptr, end_ptr - ptr, "}");
  if (end_ptr > ptr) {
    return return_one_key_flags (c, key-dog_len, value_buff, ptr - value_buff);
  }
  return 0;
}
#undef RR

extern int bookmarks_size;

static int exec_get_raw_user_comm_updates (struct connection *c, const char *key, int key_len, int dog_len) {
  int user_id;
  int st_time = 0, end_time = 0, x = 0, raw = 0, mask = 0xffffffff;
  char *ptr;


  if (*key == '%') {
    raw = 1;
  }

  if ((sscanf (key+raw, "raw_user_comm_updates[%d,%d]:%n", &st_time, &end_time, &x) >= 2 ||
       sscanf (key+raw, "raw_user_comm_updates%n%d", &x, &user_id) >= 1) && x > 0) {
    x += raw;
    ptr = 0;
    user_id = strtol (key+x, &ptr, 10);
    if (ptr < key + key_len && *ptr == ',') {
      ptr ++;
      mask = strtol (ptr, &ptr, 10);
    }

    QL = get_bookmarks (user_id, mask, Q, MAX_QUERY / 3);
    if (verbosity >= 2) {
      fprintf (stderr, "QL = %d, bookmarks_size = %d\n", QL, bookmarks_size);
    }
    if (QL < 0) {
      return -2;
    }

    clear_result_buffer ();
    int i, best = 0;
    QL *= 3;
    assert (QL <= MAX_QUERY);
    for (i = 0; i < QL; i += 3) {
      int res = prepare_raw_comm_updates (Q[i], Q[i+1], Q[i+2], st_time, end_time);
      if (verbosity > 2) {
        fprintf (stderr, "prepare_raw_comm_updates(%d_%d_%d) = %d\n", Q[i], Q[i+1], Q[i+2], res);
      }
      if (res > best) {
        best = res;
      }
    }
    if (best >= 0) {
      return return_one_key_list (c, key - dog_len, key_len + dog_len, /*(R_end - R) / 9*/ 0x7fffffff, -raw, R, R_end - R);
    }
  }
  return 0;
}


static int exec_get_raw_recommend_updates (struct connection *c, const char *key, int key_len, int dog_len) {
  vkprintf (4, "exec_get_raw_recommend_updates (key:%s)\n", key);
  int user_id;
  int st_time = 0, end_time = 0, x = 0, raw = 0, mask = 0xffffffff, excluded_user_id, t, timestamp;
  char *ptr;

  struct keep_mc_store *Data = 0;
  vkprintf (4, "exec_get_raw_recommend_updates: c->Tmp = %p\n", c->Tmp);
  if (c->Tmp) {
    Data = (struct keep_mc_store *) c->Tmp->start;
  }

  if (*key == '%') {
    raw = 1;
  }

  if (sscanf (key+raw, "raw_recommend_updates%d_%d,%d_%d_%d_%d:%n",
    &mask, &st_time, &end_time, &excluded_user_id, &t, &timestamp, &x) >= 6 && x > 0) {
    x += raw;
    ptr = 0;
    user_id = strtol (key+x, &ptr, 10);
    vkprintf (4, "exec_get_raw_recommend_updates: user_id = %d, Data = %p\n", user_id, Data);

    if (user_id < 0 && ptr == key + key_len && Data && user_id == Data->user_id && Data->len) {
      vkprintf (2, "found userlist %d, %d entries\n", user_id, Data->len);
      advance_read_ptr (c->Tmp, sizeof (struct keep_mc_store));
      QL = Data->len;
      if (QL > MAX_QUERY) { QL = MAX_QUERY; }
      x = read_in (c->Tmp, Q, QL * 8);
      assert (x == QL * 8);
      if (QL > 0) {
        vkprintf (2, "first entry: %d_%d\n", Q[0], Q[1]);
      }
    } else {
      vkprintf (4, "np_parse_list_str (%s)\n", key + x);
      QL = np_parse_list_str (Q, MAX_QUERY, 2, key + x, key_len - x);
    }
    get_raw_recommend_updates_queries++;
    if (QL >= 0) {
      int res = recommend_prepare_raw_updates (Q, QL, mask, st_time, end_time, excluded_user_id, timestamp, t);
      vkprintf (4, "recommend_prepare_raw_updates returns %d.\n", res);
      if (res >= 0) {
        if (res == MAX_GROUPS) {
          get_max_raw_recommend_updates_queries++;
        }
        tot_raw_recommend_updates_records += res;
        return return_one_key_list (c, key - dog_len, key_len + dog_len, /*(R_end - R) / 9*/ 0x7fffffff, -raw, R, R_end - R);
      }
    }
  }
  return 0;
}

static int exec_get_raw_user_comm_bookmarks (struct connection *c, const char *key, int key_len, int dog_len) {
  int user_id;
  int x = 0, raw = 0, mask = 0xffffffff;
  char *ptr;


  if (*key == '%') {
    raw = 1;
  }

  if ((sscanf (key+raw, "raw_user_comm_bookmarks%n", &x) >= 0) && x > 0) {
    x += raw;
    ptr = 0;
    user_id = strtol (key+x, &ptr, 10);
    if (ptr < key + key_len && *ptr == ',') {
      ptr ++;
      mask = strtol (ptr, &ptr, 10);
    }

    QL = get_bookmarks (user_id, mask, Q, MAX_QUERY / 3);
    if (verbosity >= 2) {
      fprintf (stderr, "QL = %d, bookmarks_size = %d\n", QL, bookmarks_size);
    }
    if (QL < 0) {
      return -2;
    }
    assert (QL * 3 <= MAX_QUERY);
    return_one_key_list (c, key - dog_len, key_len + dog_len, 0x7fffffff, -raw, Q, QL * 3);
  }
  return 0;
}


static int exec_get_raw_comm_updates (struct connection *c, const char *key, int key_len, int dog_len) {
  int user_id;
  int st_time = 0, end_time = 0, x = 0, raw = 0;
  char *ptr;

  struct keep_mc_store *Data = 0;

  if (c->Tmp) {
    Data = (struct keep_mc_store *) c->Tmp->start;
  }

  if (*key == '%') {
    raw = 1;
  }

  if ((sscanf (key+raw, "raw_comm_updates[%d,%d]:%n", &st_time, &end_time, &x) >= 2 ||
       sscanf (key+raw, "raw_comm_updates%n%d", &x, &user_id) >= 1) && x > 0) {
    x += raw;
    ptr = 0;
    user_id = strtol (key+x, &ptr, 10);


    if (user_id < 0 && ptr == key + key_len && Data && user_id == Data->user_id && Data->len) {
      if (verbosity > 1) {
        fprintf (stderr, "found userlist %d, %d entries\n", user_id, Data->len);
      }

      advance_read_ptr (c->Tmp, sizeof (struct keep_mc_store));
      QL = Data->len;
      if (QL > MAX_QUERY) { QL = MAX_QUERY; }
      x = read_in (c->Tmp, Q, QL*12);
      assert (x == QL*12);
      if (verbosity > 1 && QL > 0) {
        fprintf (stderr, "first entry: %d_%d_%d\n", Q[0], Q[1], Q[2]);
      }
    } else {
      QL = np_parse_list_str (Q, MAX_QUERY, 3, key + x, key_len - x);
    }

    clear_result_buffer ();
    int i, best = -1;
    QL *= 3;
    for (i = 0; i < QL; i += 3) {
      int res = prepare_raw_comm_updates (Q[i], Q[i+1], Q[i+2], st_time, end_time);
      if (verbosity > 1) {
        fprintf (stderr, "prepare_raw_comm_updates(%d_%d_%d) = %d\n", Q[i], Q[i+1], Q[i+2], res);
      }
      if (res > best) {
        best = res;
      }
    }
    if (best >= 0) {
      return return_one_key_list (c, key - dog_len, key_len + dog_len, /*(R_end - R) / 9*/ 0x7fffffff, -raw, R, R_end - R);
    }
  }
  return 0;
}

int memcache_get (struct connection *c, const char *key, int key_len) {
  int dog_len = get_at_prefix_length (key, key_len);
  key += dog_len;
  key_len -= dog_len;
  int user_id, res = 0;

  if (key_len >= 11 && UG_MODE && (!strncmp (key, "raw_updates", 11) || !strncmp (key, "%raw_updates", 12))) {
    res = exec_get_raw_updates (c, key, key_len, dog_len);
    free_tmp_buffers (c);
    return res;
  }

  if (key_len >= 16 && COMM_MODE && (!strncmp (key, "raw_comm_updates", 16) || !strncmp (key, "%raw_comm_updates", 17))) {
    res = exec_get_raw_comm_updates (c, key, key_len, dog_len);
    free_tmp_buffers (c);
    return res;
  }

  if (key_len >= 21 && RECOMMEND_MODE && (!strncmp (key, "raw_recommend_updates", 21) || !strncmp (key, "%raw_recommend_updates", 22))) {
    res = exec_get_raw_recommend_updates (c, key, key_len, dog_len);
    free_tmp_buffers (c);
    return res;
  }
  free_tmp_buffers (c);

  if (key_len >= 21 && COMM_MODE && (!strncmp (key, "raw_user_comm_updates", 21) || !strncmp (key, "%raw_user_comm_updates", 22))) {
    res = exec_get_raw_user_comm_updates (c, key, key_len, dog_len);
    if (res < 0) {
      res = news_wait (c);
      return 0;
    }
    return res;
  }

  if (key_len >= 23 && COMM_MODE && (!strncmp (key, "raw_user_comm_bookmarks", 23) || !strncmp (key, "%raw_user_comm_bookmarks", 24))) {
    res = exec_get_raw_user_comm_bookmarks (c, key, key_len, dog_len);
    if (res < 0) {
      res = news_wait (c);
      return 0;
    }
    return res;
  }

  if (key_len >= 25 && NOTIFY_MODE && (!strncmp (key, "raw_notification_updates", 24) || !strncmp (key, "%raw_notification_updates", 25))) {
    res = exec_get_raw_notification_updates (c, key, key_len, dog_len);
    return res;
  }

  if (key_len >= 21 && NOTIFY_MODE && !strncmp (key, "notification_updates", 20)) {
    res = exec_get_notification_updates (c, key, key_len, dog_len);
    return res;
  }


  if (UG_MODE && sscanf (key, "mask%d", &user_id) == 1) {
    res = get_privacy_mask (user_id);
    if (verbosity > 1) {
      fprintf (stderr, "get_privacy(%d) = %d\n", user_id, res);
    }
    if (res >= 0) {
      return return_one_key (c, key - dog_len, stats_buff, sprintf (stats_buff, "%d", res));
    }
  }

  if (COMM_MODE && !strncmp (key, "add_bookmark", 12)) {
    int type, owner, place;
    if (sscanf (key, "add_bookmark_%d_%d_%d:%d", &type, &owner, &place, &user_id) == 4) {
      int res = 0;
      if (check_split (owner) || check_split (place)) {
        res = do_add_del_bookmark (user_id, type, owner, place, 1);
      }
      return return_one_key (c, key - dog_len, res ? "1" : "0", 1);
    }
  }

  if (COMM_MODE && !strncmp (key, "del_bookmark", 12)) {
    int type, owner, place;
    if (sscanf (key, "del_bookmark_%d_%d_%d:%d", &type, &owner, &place, &user_id) == 4) {
      int res = 0;
      if (check_split (owner) || check_split (place)) {
        res = do_add_del_bookmark (user_id, type, owner, place, 0);
      }
      return return_one_key (c, key - dog_len, res ? "1" : "0", 1);
    }
  }

  if (RECOMMEND_MODE && !strncmp (key, "recommend_rate", 14))  {
    int type, action;
    if (sscanf (key + 14, "%d,%d", &type, &action) == 2) {
      double rate;
      int res = get_recommend_rate (type, action, &rate);
      if (!res) {
        return return_one_key (c, key - dog_len, stats_buff, sprintf (stats_buff, "%.10lf", rate));
      }
    }
  }

  if (key_len >= 5 && !strncmp (key, "stats", 5)) {
    int stats_len = news_prepare_stats (c);
    return return_one_key (c, key - dog_len, stats_buff, stats_len);
  }
  return 0;
}


int memcache_get_start (struct connection *c) {
  c->flags &= ~C_INTIMEOUT;
  return 0;
}

int memcache_get_end (struct connection *c, int key_count) {
  c->flags &= ~C_INTIMEOUT;
  write_out (&c->Out, "END\r\n", 5);
  return 0;
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
      close (fd);
    }
  }
  if (logname && (fd = open (logname, O_WRONLY|O_APPEND|O_CREAT, 0640)) != -1) {
    dup2 (fd, 1);
    dup2 (fd, 2);
    if (fd > 2) {
      close (fd);
    }
  }

  vkprintf (1, "logs reopened.\n");
}

void tl_news_store_result_vector (int arity) {
  int num = R_end - R;
  assert (!(num % arity));
  tl_store_int (TL_VECTOR);
  tl_store_int (num / arity);
  tl_store_raw_data ( (void *) R, 4 * num);
}

/******************** News ********************/
extern unsigned allowed_types_mask;

TL_DO_FUN(news_update)
  if (binlog_disabled == 1) {
    tl_fetch_set_error_format (TL_ERROR_BINLOG_DISABLED, "Binlog disabled.");
    return -1;
  }
  if (conv_uid (e->id) < 0) {
    tl_fetch_set_error_format (TL_ERROR_BAD_VALUE, "wrong split: uid = %d, log_split_min = %d, log_split_max = %d, log_split_mod = %d, ug_mode = %d", e->id, log_split_min, log_split_max, log_split_mod, ug_mode);
    return -1;
  }
  update_queries++;
  const int res = do_process_news_item (e->id, e->type, e->user, e->group, e->owner, e->place, e->item);
  tl_store_int (res <= 0 ? TL_BOOL_FALSE : TL_BOOL_TRUE);
TL_DO_FUN_END

TL_DEFAULT_PARSE_FUN(news_update)

/*
TL_PARSE_FUN(news_update)
  e->id = tl_fetch_int ();
  e->type = tl_fetch_int ();
  e->user = tl_fetch_int ();
  e->group = tl_fetch_int ();
  e->owner = tl_fetch_int ();
  e->place = tl_fetch_int ();
  e->item = tl_fetch_int ();
TL_PARSE_FUN_END
*/

TL_DO_FUN(news_delete)
  if (binlog_disabled == 1) {
    tl_fetch_set_error_format (TL_ERROR_BINLOG_DISABLED, "Binlog disabled.");
    return -1;
  }
  if (conv_uid (e->id) < 0) {
    tl_fetch_set_error_format (TL_ERROR_BAD_VALUE, "wrong split: uid = %d, log_split_min = %d, log_split_max = %d, log_split_mod = %d, ug_mode = %d", e->id, log_split_min, log_split_max, log_split_mod, ug_mode);
    return -1;
  }
  delete_queries++;
  tl_store_int (do_delete_user (e->id) <= 0 ? TL_BOOL_FALSE : TL_BOOL_TRUE);
TL_DO_FUN_END

TL_PARSE_FUN(news_delete)
  e->id = tl_fetch_int ();
TL_PARSE_FUN_END

TL_DO_FUN(news_set_privacy_mask)
  if (binlog_disabled == 1) {
    tl_fetch_set_error_format (TL_ERROR_BINLOG_DISABLED, "Binlog disabled.");
    return -1;
  }
  if (conv_uid (e->id) < 0) {
    tl_fetch_set_error_format (TL_ERROR_BAD_VALUE, "wrong split: uid = %d, log_split_min = %d, log_split_max = %d, log_split_mod = %d, ug_mode = %d", e->id, log_split_min, log_split_max, log_split_mod, ug_mode);
    return -1;
  }
  update_queries++;
  tl_store_int (do_set_privacy (e->id, e->mask) <= 0 ? TL_BOOL_FALSE : TL_BOOL_TRUE);
TL_DO_FUN_END

TL_PARSE_FUN(news_set_privacy_mask)
  e->id = tl_fetch_int ();
  e->mask = tl_fetch_int ();
TL_PARSE_FUN_END

TL_DO_FUN(news_get_privacy_mask)
  if (conv_uid (e->id) < 0) {
    tl_fetch_set_error_format (TL_ERROR_BAD_VALUE, "wrong split: uid = %d, log_split_min = %d, log_split_max = %d, log_split_mod = %d, ug_mode = %d", e->id, log_split_min, log_split_max, log_split_mod, ug_mode);
    return -1;
  }
  const int res = get_privacy_mask (e->id);
  if (res < 0) {
    tl_store_int (TL_MAYBE_FALSE);
  } else {
    tl_store_int (TL_MAYBE_TRUE);
    tl_store_int (res);
  }
TL_DO_FUN_END

TL_PARSE_FUN(news_get_privacy_mask)
  e->id = tl_fetch_int ();
TL_PARSE_FUN_END

TL_DO_FUN(news_get_raw_updates)
  int i;
  clear_result_buffer ();
  for (i = 0; i < e->num; i++) {
    int res = prepare_raw_updates (e->user_list[i], e->type_mask, e->start_date, e->end_date);
    vkprintf (2, "prepare_raw_updates(%d) = %d\n", e->user_list[i], res);
  }
  tl_news_store_result_vector (9);
TL_DO_FUN_END

TL_PARSE_FUN(news_get_raw_updates)
  e->type_mask = tl_fetch_int ();
  e->start_date = tl_fetch_int ();
  e->end_date = tl_fetch_int ();
  e->num = tl_fetch_int ();
  extra->size += 4 * e->num;
  if (e->num < 0 || e->num > MAX_QUERY || extra->size > STATS_BUFF_SIZE) {
    tl_fetch_set_error_format (TL_ERROR_BAD_VALUE, "Invalid user_list size: num = %d", e->num);
    return 0;
  }
  tl_fetch_raw_data (e->user_list, 4 * e->num);
TL_PARSE_FUN_END

/******************** Comment news ********************/

TL_DO_FUN(cnews_update)
  if (binlog_disabled == 1) {
    tl_fetch_set_error_format (TL_ERROR_BINLOG_DISABLED, "Binlog disabled.");
    return -1;
  }
  if (!check_split (e->owner) && !check_split (e->place)) {
    tl_fetch_set_error_format (TL_ERROR_BAD_VALUE, "Bad split: log_split_min = %d, log_split_max = %d, log_split_mod = %d, owner = %d, place = %d\n", log_split_min, log_split_max, log_split_mod, e->owner, e->place);
    return -1;
  }
  if (e->type < 0 || e->type >= 32 || !(allowed_types_mask & (1 << e->type))) {
    tl_fetch_set_error_format (TL_ERROR_BAD_VALUE, "Bad type: type = %d, allowed_types_mask = 0x%08x", e->type, allowed_types_mask);
    return -1;
  }
  update_queries++;
  tl_store_int (do_process_news_comment (e->type, e->user, e->group, e->owner, e->place, e->item)  <= 0 ? TL_BOOL_FALSE : TL_BOOL_TRUE);
TL_DO_FUN_END
TL_DEFAULT_PARSE_FUN(cnews_update)

TL_DO_FUN(cnews_delete_updates)
  if (binlog_disabled == 1) {
    tl_fetch_set_error_format (TL_ERROR_BINLOG_DISABLED, "Binlog disabled.");
    return -1;
  }
  if (!check_split (e->owner) && !check_split (e->place)) {
    tl_fetch_set_error_format (TL_ERROR_BAD_VALUE, "Bad split: log_split_min = %d, log_split_max = %d, log_split_mod = %d, owner = %d, place = %d\n", log_split_min, log_split_max, log_split_mod, e->owner, e->place);
    return -1;
  }
  if (e->type < 0 || e->type >= 32 || !(allowed_types_mask & (1 << e->type))) {
    tl_fetch_set_error_format (TL_ERROR_BAD_VALUE, "Bad type: type = %d, allowed_types_mask = 0x%08x", e->type, allowed_types_mask);
    return -1;
  }
  delete_queries++;
  tl_store_int (do_delete_place (e->type, e->owner, e->place) <= 0 ? TL_BOOL_FALSE : TL_BOOL_TRUE);
TL_DO_FUN_END
TL_DEFAULT_PARSE_FUN(cnews_delete_updates)

TL_DO_FUN(cnews_delete_update)
  if (binlog_disabled == 1) {
    tl_fetch_set_error_format (TL_ERROR_BINLOG_DISABLED, "Binlog disabled.");
    return -1;
  }
  if (!check_split (e->owner) && !check_split (e->place)) {
    tl_fetch_set_error_format (TL_ERROR_BAD_VALUE, "Bad split: log_split_min = %d, log_split_max = %d, log_split_mod = %d, owner = %d, place = %d\n", log_split_min, log_split_max, log_split_mod, e->owner, e->place);
    return -1;
  }
  if (e->type < 0 || e->type >= 32 || !(allowed_types_mask & (1 << e->type))) {
    tl_fetch_set_error_format (TL_ERROR_BAD_VALUE, "Bad type: type = %d, allowed_types_mask = 0x%08x", e->type, allowed_types_mask);
    return -1;
  }
  delete_queries++;
  tl_store_int (do_delete_comment (e->type, e->owner, e->place, e->item) <= 0 ? TL_BOOL_FALSE : TL_BOOL_TRUE);
TL_DO_FUN_END
TL_DEFAULT_PARSE_FUN(cnews_delete_update)

TL_DO_FUN(cnews_undelete_update)
  if (binlog_disabled == 1) {
    tl_fetch_set_error_format (TL_ERROR_BINLOG_DISABLED, "Binlog disabled.");
    return -1;
  }
  if (!check_split (e->owner) && !check_split (e->place)) {
    tl_fetch_set_error_format (TL_ERROR_BAD_VALUE, "Bad split: log_split_min = %d, log_split_max = %d, log_split_mod = %d, owner = %d, place = %d\n", log_split_min, log_split_max, log_split_mod, e->owner, e->place);
    return -1;
  }
  if (e->type < 0 || e->type >= 32 || !(allowed_types_mask & (1 << e->type))) {
    tl_fetch_set_error_format (TL_ERROR_BAD_VALUE, "Bad type: type = %d, allowed_types_mask = 0x%08x", e->type, allowed_types_mask);
    return -1;
  }
  undelete_queries++;
  tl_store_int (do_undelete_comment (e->type, e->owner, e->place, e->item) <= 0 ? TL_BOOL_FALSE : TL_BOOL_TRUE);
TL_DO_FUN_END
TL_DEFAULT_PARSE_FUN(cnews_undelete_update)

TL_DO_FUN(cnews_get_raw_updates)
  const int n = 3 * e->num;
  int i;
  clear_result_buffer ();
  for (i = 0; i < n; i += 3) {
    int res = prepare_raw_comm_updates (e->object_list[i], e->object_list[i+1], e->object_list[i+2], e->start_date, e->end_date);
    vkprintf (2, "prepare_raw_comm_updates(%d, %d, %d, %d, %d) = %d\n", e->object_list[i], e->object_list[i+1], e->object_list[i+2], e->start_date, e->end_date, res);
  }
  tl_news_store_result_vector (8);
TL_DO_FUN_END

TL_PARSE_FUN(cnews_get_raw_updates)
  e->start_date = tl_fetch_int ();
  e->end_date = tl_fetch_int ();
  e->num = tl_fetch_int ();
  extra->size += 12 * e->num;
  if (e->num < 0 || e->num > (MAX_QUERY / 3) || extra->size > STATS_BUFF_SIZE) {
    tl_fetch_set_error_format (TL_ERROR_BAD_VALUE, "Invalid object_list size: num = %d", e->num);
    return 0;
  }
  tl_fetch_raw_data (e->object_list, 12 * e->num);
TL_PARSE_FUN_END

TL_DO_FUN(cnews_get_raw_user_updates)
  QL = get_bookmarks (e->user_id, e->type_mask, Q, MAX_QUERY / 3);
  vkprintf (2, "QL = %d, bookmarks_size = %d\n", QL, bookmarks_size);
  if (QL < 0) { return -2; }

  const int n = 3 * QL;
  int i;
  clear_result_buffer ();
  for (i = 0; i < n; i += 3) {
    int res = prepare_raw_comm_updates (Q[i], Q[i + 1], Q[i + 2], e->start_date, e->end_date);
    vkprintf (2, "prepare_raw_comm_updates(%d, %d, %d, %d, %d) = %d\n", Q[i], Q[i + 1], Q[i + 2], e->start_date, e->end_date, res);
  }
  tl_news_store_result_vector (8);
TL_DO_FUN_END

TL_PARSE_FUN(cnews_get_raw_user_updates)
  e->type_mask = tl_fetch_int ();
  e->start_date = tl_fetch_int ();
  e->end_date = tl_fetch_int ();
  e->user_id = tl_fetch_int ();
TL_PARSE_FUN_END

TL_DO_FUN(cnews_add_bookmark)
  int res = 0;
  if (check_split (e->owner) || check_split (e->place)) {
    res = do_add_del_bookmark (e->user_id, e->type, e->owner, e->place, 1);
    if (res == -2) { return -2; }
    tl_store_int (res <= 0 ? TL_BOOL_FALSE : TL_BOOL_TRUE);
  } else {
    tl_fetch_set_error_format (TL_ERROR_BAD_VALUE, "Bad split: log_split_min = %d, log_split_max = %d, log_split_mod = %d, owner = %d, place = %d\n", log_split_min, log_split_max, log_split_mod, e->owner, e->place);
    return -1;
  }
TL_DO_FUN_END
TL_DEFAULT_PARSE_FUN(cnews_add_bookmark)

TL_DO_FUN(cnews_del_bookmark)
  int res = 0;
  if (check_split (e->owner) || check_split (e->place)) {
    res = do_add_del_bookmark (e->user_id, e->type, e->owner, e->place, 0);
    if (res == -2) { return -2; }
    tl_store_int (res <= 0 ? TL_BOOL_FALSE : TL_BOOL_TRUE);
  } else {
    tl_fetch_set_error_format (TL_ERROR_BAD_VALUE, "Bad split: log_split_min = %d, log_split_max = %d, log_split_mod = %d, owner = %d, place = %d\n", log_split_min, log_split_max, log_split_mod, e->owner, e->place);
    return -1;
  }
TL_DO_FUN_END
TL_DEFAULT_PARSE_FUN(cnews_del_bookmark)

/******************** Notification news ********************/

TL_DO_FUN(nnews_update)
  if (binlog_disabled == 1) {
    tl_fetch_set_error_format (TL_ERROR_BINLOG_DISABLED, "Binlog disabled.");
    return -1;
  }
  update_queries++;
  tl_store_int (do_process_news_notify (e->id, e->type, e->user, e->owner, e->place, e->item) <= 0 ? TL_BOOL_FALSE : TL_BOOL_TRUE);
TL_DO_FUN_END
TL_DEFAULT_PARSE_FUN(nnews_update)

TL_DO_FUN(nnews_delete_updates)
  if (binlog_disabled == 1) {
    tl_fetch_set_error_format (TL_ERROR_BINLOG_DISABLED, "Binlog disabled.");
    return -1;
  }
  delete_queries++;
  int res = do_delete_place (e->type, e->owner, e->place);
  tl_store_int (TL_BOOL_STAT);
  tl_store_int (res > 0);
  tl_store_int (res <= 0);
  tl_store_int (0);
TL_DO_FUN_END
TL_DEFAULT_PARSE_FUN(nnews_delete_updates)

TL_DO_FUN(nnews_delete_update)
  if (binlog_disabled == 1) {
    tl_fetch_set_error_format (TL_ERROR_BINLOG_DISABLED, "Binlog disabled.");
    return -1;
  }
  delete_queries++;
  int res = do_delete_comment (e->type, e->owner, e->place, e->item);
  tl_store_int (TL_BOOL_STAT);
  tl_store_int (res > 0);
  tl_store_int (res <= 0);
  tl_store_int (0);
TL_DO_FUN_END
TL_DEFAULT_PARSE_FUN(nnews_delete_update)

TL_DO_FUN(nnews_undelete_update)
  if (binlog_disabled == 1) {
    tl_fetch_set_error_format (TL_ERROR_BINLOG_DISABLED, "Binlog disabled.");
    return -1;
  }
  undelete_queries++;
  int res = do_undelete_comment (e->type, e->owner, e->place, e->item);
  tl_store_int (TL_BOOL_STAT);
  tl_store_int (res > 0);
  tl_store_int (res <= 0);
  tl_store_int (0);
TL_DO_FUN_END
TL_DEFAULT_PARSE_FUN(nnews_undelete_update)

TL_DO_FUN(nnews_delete_user_update)
  if (binlog_disabled == 1) {
    tl_fetch_set_error_format (TL_ERROR_BINLOG_DISABLED, "Binlog disabled.");
    return -1;
  }
  delete_queries++;
  tl_store_int (do_delete_user_comment (e->user_id, e->type, e->owner, e->place, e->item) <= 0 ? TL_BOOL_FALSE : TL_BOOL_TRUE);
TL_DO_FUN_END
TL_DEFAULT_PARSE_FUN(nnews_delete_user_update)

TL_DO_FUN(nnews_undelete_user_update)
  if (binlog_disabled == 1) {
    tl_fetch_set_error_format (TL_ERROR_BINLOG_DISABLED, "Binlog disabled.");
    return -1;
  }
  undelete_queries++;
  tl_store_int (do_undelete_user_comment (e->user_id, e->type, e->owner, e->place, e->item) <= 0 ? TL_BOOL_FALSE : TL_BOOL_TRUE);
TL_DO_FUN_END
TL_DEFAULT_PARSE_FUN(nnews_undelete_user_update)

#define RR ((struct notify_item *)R)

TL_DO_FUN(nnews_get_updates)
  int found, total_groups;
  int x = prepare_notify_groups (e->user_id, e->mask, e->date, e->end_date, 0, e->timestamp, e->limit, &found, &total_groups);
  if (x < 0) {
    x = 0;
  }
  tl_store_int (TL_VECTOR);
  tl_store_int (x);

  int i;
  for (i = 0; i < total_groups; i++) {
    int cur_pos = notify_groups[i].pos;
    if (RR[cur_pos].random_tag < 0) {
      continue;
    }
    tl_store_int (RR[cur_pos].type);
    tl_store_int (RR[cur_pos].owner);
    tl_store_int (RR[cur_pos].place);
    tl_store_int (RR[cur_pos].user);
    tl_store_int (RR[cur_pos].item);
    tl_store_int (RR[cur_pos].date);
    x--;
  }
  assert (x == 0);
TL_DO_FUN_END
TL_DEFAULT_PARSE_FUN(nnews_get_updates)

TL_DO_FUN(nnews_get_grouped_updates)
  int i, found, total_groups;
  int x = prepare_notify_groups (e->user_id, e->mask, e->date, e->end_date, e->grouping, e->timestamp, e->limit, &found, &total_groups);
  vkprintf (3, "%s: prepare_notify_groups returns %d (found: %d, total_groups: %d).\n", __func__, x, found, total_groups);
  if (x < 0) {
    x = 0;
  }
  tl_store_int (TL_VECTOR);
  tl_store_int (x);
  assert (x == total_groups);
  for (i = 0; i < total_groups; i++) {
    int cur_pos = notify_groups[i].pos,
        tpos = (notify_groups[i].num > e->grouping) ? cur_pos + e->grouping : cur_pos + notify_groups[i].num;
    assert (tpos <= found);
    int k, total = notify_groups[i].num;
    if (RR[cur_pos].random_tag <= 0) {
      total += -RR[cur_pos].random_tag - 1;
      if (tpos < cur_pos + notify_groups[i].num) {
        tpos++;
      }
      cur_pos++;
    }

    if (tpos - cur_pos > 1) {
      tl_store_int (TL_NEWS_FLAG_TYPE | TL_NEWS_FLAG_OWNER | TL_NEWS_FLAG_PLACE | (2 * TL_NEWS_FLAG_USER) | (2 * TL_NEWS_FLAG_ITEM) | (2 * TL_NEWS_FLAG_DATE));
    } else {
      tl_store_int (TL_NEWS_FLAG_TYPE | TL_NEWS_FLAG_OWNER | TL_NEWS_FLAG_PLACE | (TL_NEWS_FLAG_USER) | (TL_NEWS_FLAG_ITEM) | (TL_NEWS_FLAG_DATE));
    }
    tl_store_int (total);
    tl_store_int (tpos - cur_pos);

    tl_store_int (RR[cur_pos].type);
    //user_id
    for (k = cur_pos; k < tpos; k++) {
      tl_store_int (RR[k].date);
    }
    //tag
    for (k = cur_pos; k < tpos; k++) {
      tl_store_int (RR[k].user);
    }
    //group
    tl_store_int (RR[cur_pos].owner);
    tl_store_int (RR[cur_pos].place);
    for (k = cur_pos; k < tpos; k++) {
      tl_store_int (RR[k].item);
    }
  }
TL_DO_FUN_END
TL_DEFAULT_PARSE_FUN(nnews_get_grouped_updates)

#undef RR
/******************** Recommendation news ********************/

TL_DO_FUN(rnews_update)
  if (binlog_disabled == 1) {
    tl_fetch_set_error_format (TL_ERROR_BINLOG_DISABLED, "Binlog disabled.");
    return -1;
  }
  update_queries++;
  const int res = do_process_news_recommend (e->id, e->type, e->owner, e->place, e->action, e->item, e->item_creation_time);
  if (res <= 0) {
    skipped_set_recommend_updates_queries++;
    tl_store_int (TL_BOOL_FALSE);
  } else {
    tl_store_int (TL_BOOL_TRUE);
  }
TL_DO_FUN_END
TL_DEFAULT_PARSE_FUN(rnews_update)

TL_DO_FUN(rnews_set_rate)
  if (binlog_disabled == 1) {
    tl_fetch_set_error_format (TL_ERROR_BINLOG_DISABLED, "Binlog disabled.");
    return -1;
  }
  update_queries++;
  const int res = do_set_recommend_rate (e->type, e->action, e->rate);
  vkprintf (3, "%s: do_set_recommend_rate (%d, %d, %.6lf) returns %d.\n", __func__, e->type, e->action, e->rate, res);
  tl_store_int (TL_BOOL_STAT);
  if (res > 0) {
    tl_store_int (1);
    tl_store_int (0);
  } else {
    tl_store_int (0);
    tl_store_int (1);
  }
  tl_store_int (0);
TL_DO_FUN_END
TL_DEFAULT_PARSE_FUN(rnews_set_rate)

TL_DO_FUN(rnews_get_rate)
  double rate;
  if (get_recommend_rate (e->type, e->action, &rate) < 0) {
    tl_store_int (TL_MAYBE_FALSE);
  } else {
    tl_store_int (TL_MAYBE_TRUE);
    tl_store_double (rate);
  }
TL_DO_FUN_END
TL_DEFAULT_PARSE_FUN(rnews_get_rate)

TL_DO_FUN(rnews_get_raw_updates)
  get_raw_recommend_updates_queries++;
  int res = recommend_prepare_raw_updates (e->user_list, 2 * e->num, e->type_mask, e->start_date, e->end_date, e->id, e->timestamp, e->t);
  vkprintf (3, "%s: recommend_prepare_raw_updates returns %d.\n", __func__, res);
  if (res < 0) {
    R_end = R;
    res = 0;
  }
  if (res == MAX_GROUPS) {
    get_max_raw_recommend_updates_queries++;
  }
  tot_raw_recommend_updates_records += res;
  tl_news_store_result_vector (6);
TL_DO_FUN_END

TL_PARSE_FUN(rnews_get_raw_updates)
  e->type_mask = tl_fetch_int ();
  e->start_date = tl_fetch_int ();
  e->end_date = tl_fetch_int ();
  e->id = tl_fetch_int ();
  e->t = tl_fetch_int ();
  e->timestamp = tl_fetch_int ();
  e->num = tl_fetch_int ();
  extra->size += 8 * e->num;
  if (e->num < 0 || e->num > MAX_QUERY / 2 || extra->size > STATS_BUFF_SIZE) {
    tl_fetch_set_error_format (TL_ERROR_BAD_VALUE, "Invalid user_list size: num = %d", e->num);
    return 0;
  }
  tl_fetch_raw_data (e->user_list, 8 * e->num);
TL_PARSE_FUN_END

void *tl_news_error_bad_mode (int op) {
  const char *s =  (UG_MODE) ? "ug" : (COMM_MODE) ? "comm" : (NOTIFY_MODE) ? "notify" : (RECOMMEND_MODE) ? "recommend" : "unknown";
  tl_fetch_set_error_format (TL_ERROR_UNKNOWN_FUNCTION_ID, "Operation 0x%08x is forbidden in %s_mode", op, s);
  return 0;
}

struct tl_act_extra *news_parse_function (long long actor_id) {
  if (actor_id != 0) {
    tl_fetch_set_error ("News only support actor_id = 0", TL_ERROR_WRONG_ACTOR_ID);
    return 0;
  }
  int op = tl_fetch_int ();
  if (tl_fetch_error ()) {
    return 0;
  }
  switch (op) {
  case TL_NEWS_UPDATE:
    if (!UG_MODE) {
      return tl_news_error_bad_mode (op);
    }
    return tl_news_update ();
  case TL_NEWS_DELETE_UPDATES:
    if (!UG_MODE) {
      return tl_news_error_bad_mode (op);
    }
    return tl_news_delete ();
  case TL_NEWS_SET_PRIVACY_MASK:
    if (!UG_MODE) {
      return tl_news_error_bad_mode (op);
    }
    return tl_news_set_privacy_mask ();
  case TL_NEWS_GET_PRIVACY_MASK:
    if (!UG_MODE) {
      return tl_news_error_bad_mode (op);
    }
    return tl_news_get_privacy_mask ();
  case TL_NEWS_GET_RAW_UPDATES:
    if (!UG_MODE) {
      return tl_news_error_bad_mode (op);
    }
    return tl_news_get_raw_updates ();
  case TL_NNEWS_UPDATE:
    if (!NOTIFY_MODE) {
      return tl_news_error_bad_mode (op);
    }
    return tl_nnews_update ();
  case TL_NNEWS_DELETE_UPDATES:
    if (!NOTIFY_MODE) {
      return tl_news_error_bad_mode (op);
    }
    return tl_nnews_delete_updates ();
  case TL_NNEWS_DELETE_UPDATE:
    if (!NOTIFY_MODE) {
      return tl_news_error_bad_mode (op);
    }
    return tl_nnews_delete_update ();
  case TL_NNEWS_UNDELETE_UPDATE:
    if (!NOTIFY_MODE) {
      return tl_news_error_bad_mode (op);
    }
    return tl_nnews_undelete_update ();
  case TL_NNEWS_DELETE_USER_UPDATE:
    if (!NOTIFY_MODE) {
      return tl_news_error_bad_mode (op);
    }
    return tl_nnews_delete_user_update ();
  case TL_NNEWS_UNDELETE_USER_UPDATE:
    if (!NOTIFY_MODE) {
      return tl_news_error_bad_mode (op);
    }
    return tl_nnews_undelete_user_update ();
  case TL_NNEWS_GET_UPDATES:
    if (!NOTIFY_MODE) {
      return tl_news_error_bad_mode (op);
    }
    return tl_nnews_get_updates ();
  case TL_NNEWS_GET_GROUPED_UPDATES:
    if (!NOTIFY_MODE) {
      return tl_news_error_bad_mode (op);
    }
    return tl_nnews_get_grouped_updates ();
  case TL_CNEWS_UPDATE:
    if (!COMM_MODE) {
      return tl_news_error_bad_mode (op);
    }
    return tl_cnews_update ();
  case TL_CNEWS_DELETE_UPDATES:
    if (!COMM_MODE) {
      return tl_news_error_bad_mode (op);
    }
    return tl_cnews_delete_updates ();
  case TL_CNEWS_DELETE_UPDATE:
    if (!COMM_MODE) {
      return tl_news_error_bad_mode (op);
    }
    return tl_cnews_delete_update ();
  case TL_CNEWS_UNDELETE_UPDATE:
    if (!COMM_MODE) {
      return tl_news_error_bad_mode (op);
    }
    return tl_cnews_undelete_update ();
  case TL_CNEWS_GET_RAW_UPDATES:
    if (!COMM_MODE) {
      return tl_news_error_bad_mode (op);
    }
    return tl_cnews_get_raw_updates ();
  case TL_CNEWS_GET_RAW_USER_UPDATES:
    if (!COMM_MODE) {
      return tl_news_error_bad_mode (op);
    }
    return tl_cnews_get_raw_user_updates ();
  case TL_CNEWS_ADD_BOOKMARK:
    if (!COMM_MODE) {
      return tl_news_error_bad_mode (op);
    }
    return tl_cnews_add_bookmark ();
  case TL_CNEWS_DEL_BOOKMARK:
    if (!COMM_MODE) {
      return tl_news_error_bad_mode (op);
    }
    return tl_cnews_del_bookmark ();
  case TL_RNEWS_UPDATE:
    if (!RECOMMEND_MODE) {
      return tl_news_error_bad_mode (op);
    }
    return tl_rnews_update ();
  case TL_RNEWS_SET_RATE:
    if (!RECOMMEND_MODE) {
      return tl_news_error_bad_mode (op);
    }
    return tl_rnews_set_rate ();
  case TL_RNEWS_GET_RATE:
    if (!RECOMMEND_MODE) {
      return tl_news_error_bad_mode (op);
    }
    return tl_rnews_get_rate ();
  case TL_RNEWS_GET_RAW_UPDATES:
    if (!RECOMMEND_MODE) {
      return tl_news_error_bad_mode (op);
    }
    return tl_rnews_get_raw_updates ();
  default:
    tl_fetch_set_error_format (TL_ERROR_UNKNOWN_FUNCTION_ID, "Unknown op 0x%08x", op);
    return 0;
  }
}

void cron (void) {
  create_all_outbound_connections ();
  flush_binlog ();
  if (!binlog_disabled) {
    update_offsets (1);
  }
  dyn_garbage_collector ();
  last_collect_garbage_time = now;
  news_collect_garbage (100);
}

engine_t news_engine;
server_functions_t news_functions = {
  .cron = cron,
  .save_index = save_index
};

void start_server (void) {
  tl_parse_function = news_parse_function;
  tl_aio_timeout = 2.0;
  server_init (&news_engine, &news_functions, &ct_rpc_server, &rpc_methods);
  int quit_steps = 0, epoll_work_timeout = 17, i;
  for (i = 0; ; i++) {
    if (verbosity > 0 && !(i & 1023)) {
      fprintf (stderr, "epoll_work(): %d out of %d connections, network buffers: %d used, %d out of %d allocated\n",
	       active_connections, maxconn, NB_used, NB_alloc, NB_max);
    }
    fetch_binlog_data (binlog_disabled ? log_cur_pos() : log_write_pos ());
    epoll_work (epoll_work_timeout);
    
    if (!process_signals ()) {
      break;
    }

    check_all_aio_completions ();
    tl_restart_all_ready ();
    
    flush_binlog ();
    cstatus_binlog_pos (binlog_disabled ? log_cur_pos() : log_write_pos (), binlog_disabled);

    /*if (getloadavg (&loadavg_last_minute, 1) == 1 && loadavg_last_minute < 1.0) {
      vkprintf (4, "Collect garbage\n");
      last_collect_garbage_time = now;
      news_collect_garbage (100);
      epoll_work_timeout = 10;
    } else {
      epoll_work_timeout = 97;
      }*/

    if (quit_steps && !--quit_steps) break;
  }

  server_exit (&news_engine);
}

/*
 *
 *		MAIN
 *
 */

extern int check_index_mode;
extern int regenerate_index_mode;

void usage (void) {
  printf ("usage: %s [-v] [-r] [-p<port>] [-u<username>] [-b<backlog>] [-c<max-conn>] [-a<binlog-name>] [-l<log-name>] [-t<days>] [-C<cache-size>] [-L] [-R] <huge-index-file> [<metaindex-file>]\n"
    "%s\n"
    "\tPerforms news/updates/bookmarks queries using given indexes\n",
    progname, FullVersionStr);
  parse_usage ();
  exit (2);
}

int custom_max_days;
int f_parse_option (int val) {
  switch (val) {
  case 't':
    max_news_days = atoi(optarg);
    break;
  case 'i':
    index_mode++;
    break;
  case 'C':
    max_allocated_metafiles_size = atoi (optarg) * (long long) 1024 * 1024;
    if (max_allocated_metafiles_size < MIN_MAX_ALLOCATED_METAFILES_SIZE) {
	    max_allocated_metafiles_size = DEFAULT_MAX_ALLOCATED_METAFILES_SIZE;
    }
    break;
  case 'L':
    check_index_mode ++;
    break;
  case 'R':
    regenerate_index_mode ++;
    break;
  default:
    return -1;
  }
  return 0;
}

char *aes_pwd_file;
int main (int argc, char *argv[]) {
  int i;
  set_debug_handlers ();
  progname = argv[0];

  parse_option ("days", required_argument, 0, 't', "Number of days to keep news (default %d)", MAX_NEWS_DAYS);
  parse_option ("index", no_argument, 0, 'i', "reindex");
  parse_option ("metafiles-memory", required_argument, 0, 'C', "memory for metafiles cache (default %lld)", (long long)DEFAULT_MAX_ALLOCATED_METAFILES_SIZE);
  parse_option ("check-index", no_argument, 0, 'L', "check index mode");
  parse_option ("regenerate-index", no_argument, 0, 'R', "regenerate index mode");

  parse_engine_options_long (argc, argv, f_parse_option);
  if (argc != optind + 1 && argc != optind + 2) {
    usage();
    return 2;
  }

  if (regenerate_index_mode) {
    check_index_mode = 0;
  }

  if (strlen (argv[0]) >= 5 && memcmp ( argv[0] + strlen (argv[0]) - 5, "index" , 5) == 0) {
    index_mode++;
  }

  engine_init (&news_engine, aes_pwd_file, index_mode);

  if (engine_preload_filelist (argv[optind], binlogname) < 0) {
    fprintf (stderr, "cannot open binlog files for %s\n", binlogname ? binlogname : argv[optind]);
    exit (1);
  }

  log_ts_interval = 0;

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

  i = load_index (Snapshot);

  index_load_time += mytime();

  if (i < 0) {
    fprintf (stderr, "fatal: error %d while loading index file %s\n", i, engine_snapshot_name);
    exit (1);
  }

  //close_snapshot (Snapshot, 1);

  if (verbosity) {
    fprintf (stderr, "load index: done, jump_log_pos=%lld, time %.06lfs\n",
       jump_log_pos, index_load_time);
  }

  //Binlog reading
  Binlog = open_binlog (engine_replica, jump_log_pos);
  if (!Binlog) {
    fprintf (stderr, "fatal: cannot find binlog for %s, log position %lld\n", engine_replica->replica_prefix, jump_log_pos);
    exit (1);
  }
  binlogname = Binlog->info->filename;
  
  init_common_data (0, index_mode ? CD_INDEXER : CD_ENGINE);
  cstatus_binlog_name (engine_replica->replica_prefix);

  if (verbosity) {
    fprintf (stderr, "replaying binlog file %s (size %lld) from position %lld (crc32=%u, ts=%d)\n", binlogname, Binlog->info->file_size, jump_log_pos, jump_log_crc32, jump_log_ts);
  }

  binlog_load_time = -mytime();

  clear_log();

  init_log_data (jump_log_pos, jump_log_ts, jump_log_crc32);

  if (verbosity) { fprintf (stderr, "replay log events started\n");}

  min_logevent_time = time(0) - (max_news_days + 1) * 86400;
  i = replay_log (0, 1);
  if (verbosity) { fprintf (stderr, "replay log events finished\n");}

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
  binlog_read = 1;
  binlog_crc32_verbosity_level = 5;

  //if (verbosity) {
  //  fprintf (stderr, "replay binlog file: done, log_pos=%lld, alloc_mem=%ld, time %.06lfs\n",
  //           (long long) log_pos, (long) get_memory_used (), binlog_load_time);
  //}


  clear_write_log();
  last_collect_garbage_time = start_time = time(0);
  check_index_mode = 0;

  if (index_mode) {
    save_index (0);
  } else {
    start_server ();
  }

  return 0;
}
