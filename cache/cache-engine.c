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

    Copyright 2012-2013 Vkontakte Ltd
              2012-2013 Anton Maydell
*/

#define	_FILE_OFFSET_BITS	64

#include <assert.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <ctype.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/types.h>
#include <aio.h>
#include <sys/wait.h>
#include <sys/stat.h>

#include "kfs.h"
#include "kdb-data-common.h"
#include "server-functions.h"
#include "cache-data.h"
#include "net-connections.h"
#include "net-memcache-server.h"
#include "net-crypto-aes.h"
#include "am-stats.h"

#include "am-server-functions.h"
#include "vv-tl-parse.h"
#include "vv-tl-aio.h"
#include "TL/constants.h"
#include "cache-interface-structures.h"

#ifndef COMMIT
#define COMMIT "unknown"
#endif

#define	VERSION_STR	"cache-engine-1.01-r8"
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
 *		CACHE ENGINE
 *
 */

static int allow_save_index_without_counters = 0;
static int hash_size = 2000000;
int start_time;
long long binlog_loaded_size;
static double binlog_load_time, index_load_time;

#ifdef CACHE_FEATURE_MONTHLY_COUNTER_PERF_STATS
static char *monthly_stat_filename = NULL;
#endif

#ifdef CACHE_FEATURE_CORRELATION_STATS
static char *correlation_stat_dirname = NULL;
#endif

#define VALUE_BUFF_SIZE	(1000000)
#define STATS_BUFF_SIZE	(16 << 10)
static char value_buff[VALUE_BUFF_SIZE];
static char stats_buff[STATS_BUFF_SIZE];

conn_type_t ct_cache_engine_server = {
  .magic = CONN_FUNC_MAGIC,
  .title = "cache_engine_server",
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
  .wakeup = 0,
  .alarm = 0,
#ifndef NOAES
  .crypto_init = aes_crypto_init,
  .crypto_free = aes_crypto_free,
  .crypto_encrypt_output = aes_crypto_encrypt_output,
  .crypto_decrypt_input = aes_crypto_decrypt_input,
  .crypto_needed_output_bytes = aes_crypto_needed_output_bytes,
#endif
};

int memcache_delete (struct connection *c, const char *key, int key_len);
int memcache_get (struct connection *c, const char *key, int key_len);
int memcache_stats (struct connection *c);
int memcache_store (struct connection *c, int op, const char *key, int key_len, int flags, int delay, int size);

struct memcache_server_functions memcache_methods = {
  .execute = mcs_execute,
  .mc_store = memcache_store,
  .mc_get_start = mcs_get_start,
  .mc_get = memcache_get,
  .mc_get_end = mcs_get_end,
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
  .memcache_fallback_type = &ct_cache_engine_server,
  .memcache_fallback_extra = &memcache_methods
};

static long long set_queries, set_file_size_queries, get_queries, get_access_queries,
  get_convert_queries,
  get_local_copies_queries, get_acounter_queries,
  get_yellow_time_remaining_queries,
  top_stats_queries, bottom_stats_queries,
  set_new_local_copy_queries, set_delete_local_copy_queries, set_yellow_time_remaining_queries,
  delete_queries, delete_remote_server_queries, delete_remote_disk_queries;
static long long access_misses, cron_acounter_update_calls;

struct query_stat {
  long long t;
  double max_time;
  double sum_time;
};

struct query_stat get_bottom_disk_stat, get_top_access_stat, get_bottom_access_stat, get_top_disk_stat;

inline void update_query_stat (struct query_stat *S, double query_time) {
  S->t++;
  if (S->max_time < query_time) {
    S->max_time = query_time;
  }
  S->sum_time += query_time;
}

int cache_prepare_stats (struct connection *c);

inline int not_found (struct connection *c) {
  write_out (&c->Out, "NOT_FOUND\r\n", 11);
  return 0;
}

int memcache_delete (struct connection *c, const char *key, int key_len) {
  int id, node_id, server_id;
  delete_queries++;
  if (binlog_disabled) {
    return not_found (c);
  }
  if ((cache_features_mask & CACHE_FEATURE_LONG_QUERIES) && key_len >= 13 && !memcmp (key, "remote_server", 13) && sscanf (key + 13, "%d,%d,%d", &id, &node_id, &server_id) == 3 && cache_id == id) {
    delete_remote_server_queries++;
    int r = cache_do_delete_remote_disk (node_id, server_id, 0);
    if (r > 0) {
      write_out (&c->Out, "DELETED\r\n", 9);
      return 0;
    }
    vkprintf (2, "cache_do_delete_remote_disk (node_id:%d, server_id:%d) retuned %d.\n", node_id, server_id, r);
  }
  int disk_id;
  if ((cache_features_mask & CACHE_FEATURE_LONG_QUERIES) && key_len >= 11 && !memcmp (key, "remote_disk", 11) && sscanf (key + 11, "%d,%d,%d,%d", &id, &node_id, &server_id, &disk_id) == 4 && cache_id == id) {
    delete_remote_disk_queries++;
    int r = cache_do_delete_remote_disk (node_id, server_id, disk_id);
    if (r > 0) {
      write_out (&c->Out, "DELETED\r\n", 9);
      return 0;
    }
  }

  return not_found (c);
}

int memcache_store (struct connection *c, int op, const char *key, int key_len, int flags, int delay, int size) {
  int id;
  char uri[256], local_uri[256];

  if (op != 1 || key_len < 1 || binlog_disabled) {
    return -2;
  }
  set_queries++;

  switch (*key) {
    case 'd':
      if (key_len >= 17 && size < 256 && !memcmp (key, "delete_local_copy", 17) && sscanf (key + 17, "%d:%255s", &id, uri) == 2 && id == cache_id) {
        assert (read_in (&c->In, value_buff, size) == size);
        value_buff[size] = 0;
        set_delete_local_copy_queries++;
        return (cache_do_delete_local_copy (uri, value_buff) < 0) ? 0 : 1;
      }
      break;
    case 'f':
      if (key_len >= 9 && size <= 64 && !memcmp (key, "file_size", 9) && sscanf (key + 9, "%d:%255s", &id, uri) == 2 && id == cache_id) {
        long long s;
        assert (read_in (&c->In, value_buff, size) == size);
        value_buff[size] = 0;
        if (sscanf (value_buff, "%lld", &s) == 1) {
          set_file_size_queries++;
          if (!cache_do_set_size (uri, s)) {
            return 1;
          }
        }
        return 0;
      }
      break;
    case 'n':
      if (key_len >= 14 && size < 256 && !memcmp (key, "new_local_copy", 14) && sscanf (key + 14, "%d:%255s", &id, uri) == 2 && id == cache_id) {
        assert (read_in (&c->In, value_buff, size) == size);
        value_buff[size] = 0;
        set_new_local_copy_queries++;
        return (cache_do_set_new_local_copy (uri, value_buff) < 0) ? 0 : 1;
      }
      break;
    case 'y':
      if (key_len >= 21 && size < 256 && !memcmp (key, "yellow_time_remaining", 21) && sscanf (key + 21, "%d:%255[^~]~%255s", &id, uri, local_uri) == 3 && id == cache_id) {
        assert (read_in (&c->In, value_buff, size) == size);
        value_buff[size] = 0;
        int duration;
        if (sscanf (value_buff, "%d", &duration) == 1) {
          set_yellow_time_remaining_queries++;
          if (!cache_do_set_yellow_light_remaining (uri, local_uri, duration)) {
            return 1;
          }
        }
        return 0;
      }
      break;
  }
  return -2;
}

int return_one_key_flags (struct connection *c, const char *key, char *val, int vlen, int flags) {
  static char buff[4096];
  int l = snprintf (buff, sizeof (buff), "VALUE %s %d %d\r\n", key, flags, vlen);
  assert (l < sizeof (buff));
  write_out (&c->Out, buff, l);
  write_out (&c->Out, val, vlen);
  write_out (&c->Out, "\r\n", 2);
  return 0;
}

static int serialize_local_copies_as_text (struct cache_local_copy *LC, int n) {
  int i, o = 0;
  for (i = 0; i < n; i++) {
    union cache_packed_local_copy_location u;
    int f = cache_local_copy_get_flags (LC + i, &u);
    o += snprintf (value_buff + o, VALUE_BUFF_SIZE - o, "%s\t%d\t%d\t%d\t%d\t%d\n",
        LC[i].location,
        (int) u.p.node_id, (int) u.p.server_id, (int) u.p.disk_id,
        f, LC[i].cached_at);
    if (o >= VALUE_BUFF_SIZE) {
      return -1;
    }
  }
  return o;
}

static int serialize_top_result_as_text (cache_top_access_result_t *R, char *output, int olen) {
  int k, o = 0;
  for (k = 1; k <= R->cnt; k++) {
    char record[2048];
    cache_buffer_t b;
    struct cache_uri *U = R->heap->H[k];
    cache_bclear (&b, record, sizeof (record));
    struct cache_local_copy *L = NULL;
    if (R->flags & 0x80000000) {
      L = cache_uri_local_copy_find (U, R->disk_filter);
      assert (L);
      cache_bprintf (&b, "%s\t", L->location);
    }
    cache_bprintf (&b, "%s", cache_get_uri_name (U));
    if (R->flags & 4) {
      int j;
      for (j = 0; j < amortization_counter_types; j++) {
        cache_bprintf (&b, "\t%.7lg", (double) cache_uri_get_acounter_value (U, j));
      }
    } else if (R->flags & 1) {
      struct amortization_counter *C = ((struct amortization_counter *) &U->data[R->heap_acounter_off]);
      cache_bprintf (&b, "\t%.7lg", (double) C->value);
    }

    if (R->flags & 2) {
      cache_bprintf (&b, "\t%lld", cache_uri_get_size (U));
    }

    if (R->flags & (32+64)) {
      int remaining_time = -1, elapsed_time = -1;
      assert (L);
      cache_local_copy_get_yellow_light_time (L, &remaining_time, &elapsed_time);
      if (R->flags & 32) {
        cache_bprintf (&b, "\t%d", remaining_time);
      }
      if (R->flags & 64) {
        cache_bprintf (&b, "\t%d", elapsed_time);
      }
    }

    cache_bprintf (&b, "\n");
    if (b.pos >= b.size) {
      break;
    }
    if (o + b.pos > olen) {
      break;
    }
    memcpy (output + o, b.buff, b.pos);
    o += b.pos;
  }
  return o;
}

int serialize_stat_server_as_text (cache_stat_server_t **A, int n, char *output, int olen) {
  int k, o = 0;
  for (k = 0; k < n; k++) {
    char record[2048];
    cache_buffer_t b;
    cache_stat_server_t *S = A[k];
    union cache_packed_local_copy_location u;
    u.i = S->id;
    cache_bclear (&b, record, sizeof (record));
    cache_bprintf (&b, "%d\t%d\t%.6lf\t%lld\t%lld\n",
      (int) u.p.node_id, (int) u.p.server_id,
      safe_div (100.0 * S->access_queries, access_success_counters[stats_counters].value),
      S->files_bytes,
      S->files);
    if (b.pos >= b.size) {
      break;
    }
    if (o + b.pos > olen) {
      break;
    }
    memcpy (output + o, b.buff, b.pos);
    o += b.pos;
  }
  return o;
}

int memcache_get (struct connection *c, const char *key, int key_len) {
  double query_time;
  get_queries++;
  int dog_len = get_at_prefix_length (key, key_len);
  const char *orig_key = key;
  key += dog_len;
  key_len -= dog_len;

  int id, t, limit, r, flags = 0, min_rate = 0, node_id, server_id, disk_id;
  char uri[256], local_uri[256];

  if (key_len < 1) {
    return 0;
  }

  cache_top_access_result_t R;

  switch (*key) {
    case 'a':
      if (!binlog_disabled && (cache_features_mask & CACHE_FEATURE_ACCESS_QUERIES) && key_len >= 6 && !memcmp (key, "access", 6) && sscanf (key + 6, "%d:%255s", &id, uri) == 2) {
        get_access_queries++;
        if (id != cache_id) {
          access_misses++;
        } else {
          cache_do_access (uri);
          return return_one_key (c, orig_key, "1", 1);
        }
      }
      if (key_len >= 8 && !memcmp (key, "acounter", 8) && sscanf (key + 8, "%d:%d:%255s", &id, &t, uri) == 3) {
        get_acounter_queries++;
        if (id == cache_id) {
          double v;
          if (cache_acounter (uri, t, &v) >= 0) {
            return return_one_key (c, orig_key, stats_buff, sprintf (stats_buff, "%.7lg", v));
          }
        }
      }
      break;
    case 'b':
      if ((cache_features_mask & CACHE_FEATURE_LONG_QUERIES) && key_len >= 11 && !memcmp (key, "bottom_disk", 11) && sscanf (key + 11, "%d,%d,%d,%d,%d,%d,%d", &id, &t, &limit, &flags, &node_id, &server_id, &disk_id) == 7 && cache_id == id) {
        query_time = -get_resource_usage_time ();
        r = cache_get_bottom_disk (&R, t, cgsl_order_bottom, limit, node_id, server_id, disk_id, flags);
        if (r >= 0) {
          r = serialize_top_result_as_text (&R, value_buff, VALUE_BUFF_SIZE);
        }
        query_time += get_resource_usage_time ();
        update_query_stat (&get_bottom_disk_stat, query_time);
        if (r >= 0) {
          return return_one_key (c, orig_key, value_buff, r);
        }
      }
      if ((cache_features_mask & CACHE_FEATURE_LONG_QUERIES) && key_len >= 13 && !memcmp (key, "bottom_access", 10) && sscanf (key + 13, "%d,%d,%d,%d,%d", &id, &t, &limit, &flags, &min_rate) >= 3 && id == cache_id) {
        query_time = -get_resource_usage_time ();
        r = cache_get_sorted_list (&R, t, cgsl_order_bottom, limit, flags, min_rate);
        if (r >= 0) {
          r = serialize_top_result_as_text (&R, value_buff, VALUE_BUFF_SIZE);
        }
        query_time += get_resource_usage_time ();
        update_query_stat (&get_bottom_access_stat, query_time);
        if (r >= 0) {
          return return_one_key (c, orig_key, value_buff, r);
        }
      }

      if ((cache_features_mask & CACHE_FEATURE_LONG_QUERIES) && key_len >= 12 && !memcmp (key, "bottom_stats", 12) && sscanf (key + 12, "%d,%d,%d", &id, &t, &limit) == 3 && id == cache_id) {
        bottom_stats_queries++;
        r = cache_get_top_stats (t, cgsl_order_bottom, limit, value_buff, VALUE_BUFF_SIZE);
        if (r >= 0) {
          return return_one_key (c, orig_key, value_buff, r);
        }
      }
      break;
    case 'c':
      if (key_len >= 7 && key_len < 1024 && !memcmp (key, "convert", 7) && sscanf (key + 7, "%d:%255s", &id, uri) == 2) {
        get_convert_queries++;
        if (id == cache_id) {
          int r = cache_do_convert (uri, value_buff, VALUE_BUFF_SIZE);
          if (r == 1) {
            cache_stats_counter_incr (convert_success_counters);
            return return_one_key (c, orig_key, value_buff, strlen (value_buff));
          }
          cache_stats_counter_incr (convert_miss_counters);
          if (r == 0) {
            return return_one_key_flags (c, orig_key, "b:0;", 4, 1);
          }
        }
      }
      break;
    case 'd':
      if (!binlog_disabled && key_len >= 14 && !memcmp (key, "disable_server", 14) && sscanf (key + 14, "%d,%d,%d", &id, &node_id, &server_id) == 3 && cache_id == id) {
        int r = cache_do_change_disk_status (node_id, server_id, 0, 0);
        if (r > 0) {
          return_one_key_flags (c, orig_key, "b:1;", 4, 1);
        } else {
          return_one_key_flags (c, orig_key, "b:0;", 4, 1);
        }
        return 0;
      }

      if (!binlog_disabled && key_len >= 12 && !memcmp (key, "disable_disk", 12) && sscanf (key + 12, "%d,%d,%d,%d", &id, &node_id, &server_id, &disk_id)== 4 && cache_id == id) {
        int r = cache_do_change_disk_status (node_id, server_id, disk_id, 0);
        if (r > 0) {
          return_one_key_flags (c, orig_key, "b:1;", 4, 1);
        } else {
          return_one_key_flags (c, orig_key, "b:0;", 4, 1);
        }
        return 0;
      }

      if ((cache_features_mask & CACHE_FEATURE_DETAILED_SERVER_STATS) && key_len >= 21 && !memcmp (key, "detailed_server_stats", 21) && sscanf (key + 21, "%d,%d", &id, &flags) >= 1 && cache_id == id) {
        dyn_mark_t mrk;
        cache_stat_server_t **A;
        dyn_mark (mrk);
        int r = cache_do_detailed_server_stats (&A, flags);
        if (r > 0) {
          r = serialize_stat_server_as_text (A, r, value_buff, VALUE_BUFF_SIZE);
        }
        dyn_release (mrk);
        if (r >= 0) {
          return return_one_key (c, orig_key, value_buff, r);
        }
        return 0;
      }
      break;
    case 'e':
      if (!binlog_disabled && key_len >= 13 && !memcmp (key, "enable_server", 13) && sscanf (key + 13, "%d,%d,%d", &id, &node_id, &server_id) == 3 && cache_id == id) {
        int r = cache_do_change_disk_status (node_id, server_id, 0, 1);
        if (r > 0) {
          return_one_key_flags (c, orig_key, "b:1;", 4, 1);
        } else {
          return_one_key_flags (c, orig_key, "b:0;", 4, 1);
        }
        return 0;
      }

      if (!binlog_disabled && key_len >= 11 && !memcmp (key, "enable_disk", 11) && sscanf (key + 11, "%d,%d,%d,%d", &id, &node_id, &server_id, &disk_id)== 4 && cache_id == id) {
        int r = cache_do_change_disk_status (node_id, server_id, disk_id, 1);
        if (r > 0) {
          return_one_key_flags (c, orig_key, "b:1;", 4, 1);
        } else {
          return_one_key_flags (c, orig_key, "b:0;", 4, 1);
        }
        return 0;
      }
      break;
    case 'f':
      if (key_len >= 9 && !memcmp (key, "file_size", 9) && sscanf (key + 9, "%d:%255s", &id, uri) == 2 && id == cache_id) {
        long long s;
        if (cache_get_file_size (uri, &s) < 0) {
          return 0;
        }
        return return_one_key (c, orig_key, value_buff, sprintf (value_buff, "%lld", s));
      }
      break;
    case 'l':
      if (key_len >= 12 && key_len < 1024 && !memcmp (key, "local_copies", 12) && sscanf (key + 12, "%d:%255s", &id, uri) == 2) {
        get_local_copies_queries++;
        if (id == cache_id) {
          struct cache_local_copy *LC;
          int r = cache_do_local_copies (uri, &LC);
          if (r >= 0) {
            r = serialize_local_copies_as_text (LC, r);
            if (r >= 0) {
              return return_one_key (c, orig_key, value_buff, r);
            }
          }
        }
      }
      break;
    case 'm':
      if (key_len >= 12 && !memcmp (key, "memory_stats", 12)) {
        int r = cache_do_memory_stats (value_buff, VALUE_BUFF_SIZE);
        if (r >= 0) {
          return return_one_key (c, orig_key, value_buff, r);
        }
      }
      break;
    case 's':
      if (key_len >= 5 && !strncmp (key, "stats", 5)) {
        int len = cache_prepare_stats (c);
        return return_one_key (c, orig_key, stats_buff, len);
      }
      break;
    case 't':
      if ((cache_features_mask & CACHE_FEATURE_LONG_QUERIES) && key_len >= 10 && !memcmp (key, "top_access", 10) && sscanf (key + 10, "%d,%d,%d,%d,%d", &id, &t, &limit, &flags, &min_rate) >= 3 && id == cache_id) {
        query_time = -get_resource_usage_time ();
        r = cache_get_sorted_list (&R, t, cgsl_order_top, limit, flags, min_rate);
        if (r >= 0) {
          r = serialize_top_result_as_text (&R, value_buff, VALUE_BUFF_SIZE);
        }
        query_time += get_resource_usage_time ();
        update_query_stat (&get_top_access_stat, query_time);
        vkprintf (1, "top_access execution time: %.6lfs, returned value: %d.\n", query_time, r);
        if (r >= 0) {
          return return_one_key (c, orig_key, value_buff, r);
        }
      }

      if ((cache_features_mask & CACHE_FEATURE_LONG_QUERIES) && key_len >= 8 && !memcmp (key, "top_disk", 8) && sscanf (key + 8, "%d,%d,%d,%d,%d,%d,%d", &id, &t, &limit, &flags, &node_id, &server_id, &disk_id) == 7 && cache_id == id) {
        query_time = -get_resource_usage_time ();
        r = cache_get_bottom_disk (&R, t, cgsl_order_top, limit, node_id, server_id, disk_id, flags);
        if (r >= 0) {
          r = serialize_top_result_as_text (&R, value_buff, VALUE_BUFF_SIZE);
        }
        query_time += get_resource_usage_time ();
        update_query_stat (&get_top_disk_stat, query_time);
        if (r >= 0) {
          return return_one_key (c, orig_key, value_buff, r);
        }
      }

      if ((cache_features_mask & CACHE_FEATURE_LONG_QUERIES) && key_len >= 9 && !memcmp (key, "top_stats", 9) && sscanf (key + 9, "%d,%d,%d", &id, &t, &limit) == 3 && id == cache_id) {
        top_stats_queries++;
        r = cache_get_top_stats (t, cgsl_order_top, limit, value_buff, VALUE_BUFF_SIZE);
        if (r >= 0) {
          return return_one_key (c, orig_key, value_buff, r);
        }
      }
      break;
    case 'y':
      if (key_len >= 21 && !memcmp (key, "yellow_time_remaining", 21) && sscanf (key + 21, "%d:%255[^~]~%255s", &id, uri, local_uri) == 3 && id == cache_id) {
        int remaining_time, elapsed_time;
        get_yellow_time_remaining_queries++;
        r = cache_get_yellow_light_remaining (uri, local_uri, &remaining_time, &elapsed_time);
        if (r >= 0) {
          return return_one_key (c, orig_key, value_buff, sprintf (value_buff, "%d,%d", remaining_time, elapsed_time));
        } else {
          return return_one_key (c, orig_key, "NOT_FOUND", 9);
        }
      }
      break;
  }

  return 0;
}

int memcache_stats (struct connection *c) {
  int len = cache_prepare_stats (c);
  write_out (&c->Out, stats_buff, len);
  write_out (&c->Out, "END\r\n", 5);
  return 0;
}

void sb_print_stat (stats_buffer_t *sb, struct query_stat *S, const char *const desc) {
  sb_printf (sb, "%s_queries\t%lld\nqps_%s\t%.3lf\n", desc, S->t, desc, safe_div (S->t, now - start_time));
  sb_printf (sb, "%s_max_query_time\t%.3lfs\n", desc, S->max_time);
  sb_printf (sb, "%s_avg_query_time\t%.3lfs\n", desc, safe_div (S->sum_time, S->t));
}

int cache_prepare_stats (struct connection *c) {
  int i;

  stats_buffer_t sb;
  sb_prepare (&sb, c, stats_buff, STATS_BUFF_SIZE);
  sb_memory (&sb, AM_GET_MEMORY_USAGE_SELF + AM_GET_MEMORY_USAGE_OVERALL);
  SB_BINLOG;
  SB_INDEX;

  SB_PRINT_QUERIES(set_queries);
  SB_PRINT_QUERIES(set_file_size_queries);
  SB_PRINT_QUERIES(set_new_local_copy_queries);
  SB_PRINT_QUERIES(set_delete_local_copy_queries);
  SB_PRINT_QUERIES(set_yellow_time_remaining_queries);
  SB_PRINT_QUERIES(get_queries);
  SB_PRINT_QUERIES(get_access_queries);
  //SB_PRINT_QUERIES(get_access_cached_queries);
  //sb_printf (&sb, "get_access_cached_queries_all_disks_are_available_percent\t%.6lf\n", safe_div (100.0 * access_cached_queries, get_access_queries - access_misses));
  SB_PRINT_I64(access_misses);
  SB_PRINT_QUERIES(get_convert_queries);
  //SB_PRINT_QUERIES(get_convert_successfull_queries);
  //sb_printf (&sb, "get_convert_successfull_queries_percent\t%.6lf\n", safe_div (100.0 * get_convert_successfull_queries, get_convert_queries));
  SB_PRINT_QUERIES(get_local_copies_queries);
  SB_PRINT_QUERIES(get_acounter_queries);
  SB_PRINT_QUERIES(get_yellow_time_remaining_queries);

  SB_PRINT_QUERIES(top_stats_queries);
  SB_PRINT_QUERIES(bottom_stats_queries);

  sb_print_stat (&sb, &get_top_access_stat, "get_top_access");
  sb_print_stat (&sb, &get_bottom_access_stat, "get_bottom_access");
  sb_print_stat (&sb, &get_bottom_disk_stat, "get_bottom_disk");
  sb_print_stat (&sb, &get_top_disk_stat, "get_top_disk");

  SB_PRINT_QUERIES(delete_queries);
  SB_PRINT_QUERIES(delete_remote_server_queries);
  SB_PRINT_QUERIES(delete_remote_disk_queries);

  SB_PRINT_I32(cache_id);
  SB_PRINT_I32(uri_hash_prime);
  sb_printf (&sb, "acounters_init_string\t%s\n", acounters_init_string);
  SB_PRINT_I32(amortization_counter_types);
  for (i = 0; i < amortization_counter_types; i++) {
    sb_printf (&sb, "ac_T_%d\t%.3lfs\n", i, TAT[i].T);
  }
  sb_printf (&sb, "optimized_top_access_uncached_acounter_id\t%d\n", acounter_uncached_bucket_id);

  SB_PRINT_I64(uries);
  SB_PRINT_I64(cached_uries);
  sb_printf (&sb, "cached_uries_percent\t%.6lf\n", safe_div (cached_uries * 100.0, uries));
  SB_PRINT_I64(deleted_uries);
  SB_PRINT_I64(uri_bytes);
  SB_PRINT_I64(local_copies_bytes);
  sb_printf (&sb, "avg_uri_bytes\t%.3lf\n", safe_div (uri_bytes, uries));
  SB_PRINT_I64(uri_reallocs);

  SB_PRINT_I64(sum_all_cached_files_sizes);
  sb_printf (&sb, "cached_uries_known_size_percent\t%.6lf\n", safe_div (cached_uries_knowns_size * 100.0, cached_uries));

  SB_PRINT_I64(access_short_logevents);
  SB_PRINT_I64(access_long_logevents);
  SB_PRINT_I64(skipped_access_logevents);
  SB_PRINT_I64(uri2md5_extra_calls);
  SB_PRINT_I64(get_uri_f_calls);
  SB_PRINT_I64(uri_cache_hits);
  sb_printf (&sb, "uri_cache_hits_percent\t%.6lf\n", safe_div (uri_cache_hits * 100.0, get_uri_f_calls));
  SB_PRINT_I64(cron_acounter_update_calls);

  char *tmp_buff = alloca (1024);
  cache_stats_perf (tmp_buff, 1024, access_success_counters, access_miss_counters);
  sb_printf (&sb, "access_performance%s\n", tmp_buff);
  cache_stats_perf (tmp_buff, 1024, convert_success_counters, convert_miss_counters);
  sb_printf (&sb, "convert_performance%s\n", tmp_buff);

  sb_printf (&sb, "feature_long_queries\t%d\n", (cache_features_mask & CACHE_FEATURE_LONG_QUERIES) ? 1 : 0);
  sb_printf (&sb, "feature_replay_delete\t%d\n", (cache_features_mask & CACHE_FEATURE_REPLAY_DELETE) ? 1 : 0);
  sb_printf (&sb, "feature_detailed_server_stats\t%d\n", (cache_features_mask & CACHE_FEATURE_DETAILED_SERVER_STATS) ? 1 : 0);
  sb_printf (&sb, "feature_fast_bottom_access\t%d\n", (cache_features_mask & CACHE_FEATURE_FAST_BOTTOM_ACCESS) ? 1 : 0);
  sb_printf (&sb, "feature_access_queries\t%d\n", (cache_features_mask & CACHE_FEATURE_ACCESS_QUERIES) ? 1 : 0);

  sb_printf (&sb, "version\t%s\n", FullVersionStr);

  return sb.pos;
}

/******************** RPC functions ********************/
TL_DO_FUN(cache_access)
  if (!(cache_features_mask & CACHE_FEATURE_ACCESS_QUERIES)) {
    tl_fetch_set_error_format (TL_ERROR_FEATURE_DISABLED, "Access queries are disabled.");
    return -1;
  }

  if (binlog_disabled == 1) {
    tl_fetch_set_error_format (TL_ERROR_BINLOG_DISABLED, "Binlog disabled.");
    return -1;
  }

  get_access_queries++;
  if (e->cache_id != cache_id) {
    access_misses++;
    tl_store_int (TL_BOOL_FALSE);
  } else {
    cache_do_access (e->url);
    tl_store_int (TL_BOOL_TRUE);
  }
TL_DO_FUN_END

TL_PARSE_FUN(cache_access, void)
  e->cache_id = tl_fetch_int ();
  extra->size += tl_fetch_string0 (e->url, MAX_URL_LENGTH) + 1;
TL_PARSE_FUN_END

TL_DO_FUN(cache_set_file_size)
  if (binlog_disabled == 1) {
    tl_fetch_set_error_format (TL_ERROR_BINLOG_DISABLED, "Binlog disabled.");
    return -1;
  }
  if (e->cache_id != cache_id) {
    tl_store_int (TL_BOOL_FALSE);
  } else {
    set_file_size_queries++;
    tl_store_int (!cache_do_set_size (e->url, e->size) ? TL_BOOL_TRUE : TL_BOOL_FALSE);
  }
TL_DO_FUN_END

TL_PARSE_FUN(cache_set_file_size, void)
  e->cache_id = tl_fetch_int ();
  extra->size += tl_fetch_string0 (e->url, MAX_URL_LENGTH) + 1;
  e->size = tl_fetch_long ();
TL_PARSE_FUN_END

TL_DO_FUN(cache_set_yellow_time_remaining)
  if (binlog_disabled == 1) {
    tl_fetch_set_error_format (TL_ERROR_BINLOG_DISABLED, "Binlog disabled.");
    return -1;
  }
  if (e->cache_id != cache_id) {
    tl_store_int (TL_BOOL_FALSE);
  } else {
    set_yellow_time_remaining_queries++;
    tl_store_int (!cache_do_set_yellow_light_remaining (e->data, e->data + e->local_url_off, e->time) ? TL_BOOL_TRUE : TL_BOOL_FALSE);
  }
TL_DO_FUN_END

TL_PARSE_FUN(cache_set_yellow_time_remaining, void)
  e->cache_id = tl_fetch_int ();
  extra->size += e->local_url_off = tl_fetch_string0 (e->data, MAX_URL_LENGTH) + 1;
  extra->size += tl_fetch_string0 (e->data + e->local_url_off, MAX_URL_LENGTH) + 1;
  e->time = tl_fetch_int ();
TL_PARSE_FUN_END

TL_DO_FUN(cache_set_new_local_copy)
  if (binlog_disabled == 1) {
    tl_fetch_set_error_format (TL_ERROR_BINLOG_DISABLED, "Binlog disabled.");
    return -1;
  }
  if (e->cache_id != cache_id) {
    tl_store_int (TL_BOOL_FALSE);
  } else {
    if (e->act < 0) {
      set_delete_local_copy_queries++;
      tl_store_int (cache_do_delete_local_copy (e->data, e->data + e->local_url_off) < 0 ? TL_BOOL_FALSE : TL_BOOL_TRUE);
    } else {
      set_new_local_copy_queries++;
      tl_store_int (cache_do_set_new_local_copy (e->data, e->data + e->local_url_off) < 0 ? TL_BOOL_FALSE : TL_BOOL_TRUE);
    }
  }
TL_DO_FUN_END

TL_PARSE_FUN(cache_set_new_local_copy, int act)
  e->act = act;
  e->cache_id = tl_fetch_int ();
  extra->size += e->local_url_off = tl_fetch_string0 (e->data, MAX_URL_LENGTH) + 1;
  extra->size += tl_fetch_string0 (e->data + e->local_url_off, MAX_URL_LENGTH) + 1;
TL_PARSE_FUN_END

TL_DO_FUN(cache_enable_disk)
  if (binlog_disabled == 1) {
    tl_fetch_set_error_format (TL_ERROR_BINLOG_DISABLED, "Binlog disabled.");
    return -1;
  }
  if (e->cache_id != cache_id) {
    tl_store_int (TL_BOOL_FALSE);
  } else {
    tl_store_int (cache_do_change_disk_status (e->node_id, e->server_id, e->disk_id, e->act) > 0 ? TL_BOOL_TRUE : TL_BOOL_FALSE);
  }
TL_DO_FUN_END

TL_PARSE_FUN(cache_enable_disk, int act, int disk)
  e->act = act;
  e->cache_id = tl_fetch_int ();
  e->node_id = tl_fetch_int ();
  e->server_id = tl_fetch_int ();
  e->disk_id = disk ? tl_fetch_int () : 0;
TL_PARSE_FUN_END

TL_DO_FUN(cache_delete_disk)
  if (!(cache_features_mask & CACHE_FEATURE_LONG_QUERIES)) {
    tl_fetch_set_error_format (TL_ERROR_FEATURE_DISABLED, "Long queries are disabled.");
    return -1;
  }
  if (binlog_disabled == 1) {
    tl_fetch_set_error_format (TL_ERROR_BINLOG_DISABLED, "Binlog disabled.");
    return -1;
  }
  if (e->cache_id != cache_id) {
    tl_store_int (TL_BOOL_FALSE);
  } else {
    if (e->disk_id) {
      delete_remote_disk_queries++;
    } else {
      delete_remote_server_queries++;
    }
    tl_store_int (cache_do_delete_remote_disk (e->node_id, e->server_id, e->disk_id) > 0 ? TL_BOOL_TRUE : TL_BOOL_FALSE);
  }
TL_DO_FUN_END

TL_PARSE_FUN(cache_delete_disk, int disk)
  e->cache_id = tl_fetch_int ();
  e->node_id = tl_fetch_int ();
  e->server_id = tl_fetch_int ();
  e->disk_id = disk ? tl_fetch_int () : 0;
TL_PARSE_FUN_END

TL_DO_FUN(cache_convert)
  get_convert_queries++;
  if (e->cache_id != cache_id || cache_do_convert (e->url, value_buff, VALUE_BUFF_SIZE) != 1) {
    cache_stats_counter_incr (convert_miss_counters);
    tl_store_int (TL_MAYBE_FALSE);
  } else {
    cache_stats_counter_incr (convert_success_counters);
    tl_store_int (TL_MAYBE_TRUE);
    tl_store_string0 (value_buff);
  }
TL_DO_FUN_END

TL_PARSE_FUN(cache_convert, void)
  e->cache_id = tl_fetch_int ();
  extra->size += tl_fetch_string0 (e->url, MAX_URL_LENGTH) + 1;
TL_PARSE_FUN_END

TL_DO_FUN(cache_get_file_size)
  long long s;
  if (e->cache_id != cache_id || cache_get_file_size (e->url, &s) < 0) {
    tl_store_int (TL_MAYBE_FALSE);
  } else {
    tl_store_int (TL_MAYBE_TRUE);
    tl_store_long (s);
  }
TL_DO_FUN_END

TL_PARSE_FUN(cache_get_file_size, void)
  e->cache_id = tl_fetch_int ();
  extra->size += tl_fetch_string0 (e->url, MAX_URL_LENGTH) + 1;
TL_PARSE_FUN_END

TL_DO_FUN(cache_get_local_copies)
  get_local_copies_queries++;
  struct cache_local_copy *LC;
  int n;
  if (e->cache_id != cache_id || (n = cache_do_local_copies (e->url, &LC)) < 0) {
    tl_store_int (TL_MAYBE_FALSE);
  } else {
    int i;
    tl_store_int (TL_MAYBE_TRUE);
    tl_store_int (n);
    for (i = 0; i < n; i++) {
      union cache_packed_local_copy_location u;
      int f = cache_local_copy_get_flags (LC + i, &u);
      tl_store_string0 (LC[i].location);
      tl_store_int (u.p.node_id);
      tl_store_int (u.p.server_id);
      tl_store_int (u.p.disk_id);
      tl_store_int (f);
      tl_store_int (LC[i].cached_at);
    }
  }
TL_DO_FUN_END

TL_PARSE_FUN(cache_get_local_copies, void)
  e->cache_id = tl_fetch_int ();
  extra->size += tl_fetch_string0 (e->url, MAX_URL_LENGTH) + 1;
TL_PARSE_FUN_END

TL_DO_FUN(cache_get_yellow_time)
  get_yellow_time_remaining_queries++;
  int remaining_time, elapsed_time;
  if (e->cache_id != cache_id || cache_get_yellow_light_remaining (e->data, e->data + e->local_url_off, &remaining_time, &elapsed_time) < 0) {
    tl_store_int (TL_MAYBE_FALSE);
  } else {
    tl_store_int (TL_MAYBE_TRUE);
    tl_store_int (remaining_time);
    tl_store_int (elapsed_time);
  }
TL_DO_FUN_END

TL_PARSE_FUN(cache_get_yellow_time, void)
  e->cache_id = tl_fetch_int ();
  extra->size += e->local_url_off = tl_fetch_string0 (e->data, MAX_URL_LENGTH) + 1;
  extra->size += tl_fetch_string0 (e->data + e->local_url_off, MAX_URL_LENGTH) + 1;
TL_PARSE_FUN_END

void tl_store_top_access_result (cache_top_access_result_t *R) {
  vkprintf (3, "%s: R->cnt (%d), R->flags (%d)\n", __func__, R->cnt, R->flags);
  int k;
  assert (R->cnt >= 0);
  tl_store_int (R->cnt);
  for (k = 1; k <= R->cnt; k++) {
    struct cache_uri *U = R->heap->H[k];
    struct cache_local_copy *L = NULL;
    if (R->flags & 0x80000000) {
      L = cache_uri_local_copy_find (U, R->disk_filter);
      assert (L);
      tl_store_string0 (L->location);
    }
    tl_store_string0 (cache_get_uri_name (U));

    if (R->flags & 1) {
      struct amortization_counter *C = ((struct amortization_counter *) &U->data[R->heap_acounter_off]);
      tl_store_double (C->value);
    }

    if (R->flags & 2) {
      tl_store_long (cache_uri_get_size (U));
    }

    if (R->flags & 4) {
      int j;
      tl_store_int (amortization_counter_types);
      for (j = 0; j < amortization_counter_types; j++) {
        tl_store_double ((double) cache_uri_get_acounter_value (U, j));
      }
    }

    if (R->flags & (32+64)) {
      int remaining_time = -1, elapsed_time = -1;
      assert (L);
      cache_local_copy_get_yellow_light_time (L, &remaining_time, &elapsed_time);
      if (R->flags & 32) {
        tl_store_int (remaining_time);
      }
      if (R->flags & 64) {
        tl_store_int (elapsed_time);
      }
    }
  }
}

TL_DO_FUN(cache_get_top_access)
  if (!(cache_features_mask & CACHE_FEATURE_LONG_QUERIES)) {
    tl_fetch_set_error_format (TL_ERROR_FEATURE_DISABLED, "Long queries are disabled.");
    return -1;
  }
  if (cache_id != e->cache_id) {
    tl_fetch_set_error_format (TL_ERROR_BAD_VALUE, "Expected %d cache_id, but received %d.", cache_id, e->cache_id);
    return -1;
  }

  cache_top_access_result_t R;
  tl_store_int (TL_VECTOR);
  double query_time = -get_resource_usage_time ();
  int r = cache_get_sorted_list (&R, e->t, e->order, e->limit, e->flags, e->min_rate);
  if (r < 0) {
    R.cnt = 0;
  }
  tl_store_top_access_result (&R);
  query_time += get_resource_usage_time ();
  update_query_stat (e->order == cgsl_order_top ? &get_top_access_stat : &get_bottom_access_stat, query_time);
  vkprintf (2, "top_access execution time: %.6lfs, returned value: %d.\n", query_time, r);
TL_DO_FUN_END

TL_PARSE_FUN(cache_get_top_access, int order)
  e->order = order;
  e->cache_id = tl_fetch_int ();
  e->t = tl_fetch_int ();
  e->limit = tl_fetch_int ();
  e->flags = tl_fetch_int ();
  e->min_rate = tl_fetch_int ();
TL_PARSE_FUN_END

TL_DO_FUN(cache_get_bottom_disk)
  if (!(cache_features_mask & CACHE_FEATURE_LONG_QUERIES)) {
    tl_fetch_set_error_format (TL_ERROR_FEATURE_DISABLED, "Long queries are disabled.");
    return -1;
  }

  if (cache_id != e->cache_id) {
    tl_fetch_set_error_format (TL_ERROR_BAD_VALUE, "Expected %d cache_id, but received %d.", cache_id, e->cache_id);
    return -1;
  }

  if (!(e->node_id >= 1 && e->node_id <= MAX_NODE_ID)) {
    tl_fetch_set_error_format (TL_ERROR_BAD_VALUE, "Invalid node_id.");
    return -1;
  }

  if (!(e->server_id >= 1 && e->server_id <= MAX_SERVER_ID)) {
    tl_fetch_set_error_format (TL_ERROR_BAD_VALUE, "Invalid server_id.");
    return -1;
  }

  if (!(e->disk_id >= 1 && e->disk_id <= MAX_DISK_ID)) {
    tl_fetch_set_error_format (TL_ERROR_BAD_VALUE, "Invalid disk_id.");
    return -1;
  }

  cache_top_access_result_t R;
  tl_store_int (TL_VECTOR);
  double query_time = -get_resource_usage_time ();
  int r = cache_get_bottom_disk (&R, e->t, e->order, e->limit, e->node_id, e->server_id, e->disk_id, e->flags);
  if (r < 0) {
    R.cnt = 0;
  }
  tl_store_top_access_result (&R);
  query_time += get_resource_usage_time ();
  update_query_stat (e->order == cgsl_order_top ? &get_top_disk_stat : &get_bottom_disk_stat, query_time);
  vkprintf (2, "top_access execution time: %.6lfs, returned value: %d.\n", query_time, r);
TL_DO_FUN_END

TL_PARSE_FUN(cache_get_bottom_disk, int order)
  e->order = order;
  e->cache_id = tl_fetch_int ();
  e->t = tl_fetch_int ();
  e->limit = tl_fetch_int ();
  e->flags = tl_fetch_int ();
  e->node_id = tl_fetch_int ();
  e->server_id = tl_fetch_int ();
  e->disk_id = tl_fetch_int ();
TL_PARSE_FUN_END

TL_DO_FUN(cache_get_acounter)
  get_acounter_queries++;
  double v;
  if (e->cache_id != cache_id || cache_acounter (e->url, e->t, &v) < 0) {
    tl_store_int (TL_MAYBE_FALSE);
  } else {
    tl_store_int (TL_MAYBE_TRUE);
    tl_store_double (v);
  }
TL_DO_FUN_END

TL_PARSE_FUN(cache_get_acounter, void)
  e->cache_id = tl_fetch_int ();
  extra->size += tl_fetch_string0 (e->url, MAX_URL_LENGTH) + 1;
  e->t = tl_fetch_int ();
TL_PARSE_FUN_END

TL_DO_FUN(cache_get_server_stats)
  if (!(cache_features_mask & CACHE_FEATURE_DETAILED_SERVER_STATS)) {
    tl_fetch_set_error_format (TL_ERROR_FEATURE_DISABLED, "Detailed server stat queries are disabled.");
    return -1;
  }
  if (cache_id != e->cache_id) {
    tl_fetch_set_error_format (TL_ERROR_BAD_VALUE, "Expected %d cache_id, but received %d.", cache_id, e->cache_id);
    return -1;
  }

  dyn_mark_t mrk;
  cache_stat_server_t **A;
  dyn_mark (mrk);
  int r = cache_do_detailed_server_stats (&A, e->sorting_flags);
  if (r < 0) {
    r = 0;
  }
  tl_store_int (TL_VECTOR);
  tl_store_int (r);
  int k;
  for (k = 0; k < r; k++) {
    cache_stat_server_t *S = A[k];
    union cache_packed_local_copy_location u;
    u.i = S->id;
    tl_store_int ((int) u.p.node_id);
    tl_store_int ((int) u.p.server_id);
    tl_store_double (safe_div (100.0 * S->access_queries, access_success_counters[stats_counters].value));
    tl_store_long (S->files_bytes);
    tl_store_long (S->files);
  }
  dyn_release (mrk);
TL_DO_FUN_END

TL_PARSE_FUN(cache_get_server_stats)
  e->cache_id = tl_fetch_int ();
  e->sorting_flags = tl_fetch_int ();
TL_PARSE_FUN_END

struct tl_act_extra *cache_parse_function (long long actor_id) {
  if (actor_id != 0) {
    tl_fetch_set_error ("Cache only support actor_id = 0", TL_ERROR_WRONG_ACTOR_ID);
    return 0;
  }
  int op = tl_fetch_int ();
  if (tl_fetch_error ()) {
    return 0;
  }
  switch (op) {
  case TL_CACHE_ACCESS: return tl_cache_access ();
  case TL_CACHE_SET_FILE_SIZE: return tl_cache_set_file_size ();
  case TL_CACHE_SET_YELLOW_TIME_REMAINING: return tl_cache_set_yellow_time_remaining ();
  case TL_CACHE_SET_NEW_LOCAL_COPY: return tl_cache_set_new_local_copy (1);
  case TL_CACHE_DELETE_LOCAL_COPY: return tl_cache_set_new_local_copy (-1);
  case TL_CACHE_DISABLE_SERVER: return tl_cache_enable_disk (0, 0);
  case TL_CACHE_ENABLE_SERVER: return tl_cache_enable_disk (1, 0);
  case TL_CACHE_DISABLE_DISK: return tl_cache_enable_disk (0, 1);
  case TL_CACHE_ENABLE_DISK: return tl_cache_enable_disk (1, 1);
  case TL_CACHE_DELETE_SERVER: return tl_cache_delete_disk (0);
  case TL_CACHE_DELETE_DISK: return tl_cache_delete_disk (1);
  case TL_CACHE_CONVERT: return tl_cache_convert ();
  case TL_CACHE_GET_FILE_SIZE: return tl_cache_get_file_size ();
  case TL_CACHE_GET_LOCAL_COPIES: return tl_cache_get_local_copies ();
  case TL_CACHE_GET_YELLOW_TIME: return tl_cache_get_yellow_time ();
  case TL_CACHE_GET_TOP_ACCESS: return tl_cache_get_top_access (cgsl_order_top);
  case TL_CACHE_GET_BOTTOM_ACCESS: return tl_cache_get_top_access (cgsl_order_bottom);
  case TL_CACHE_GET_TOP_DISK: return tl_cache_get_bottom_disk (cgsl_order_top);
  case TL_CACHE_GET_BOTTOM_DISK: return tl_cache_get_bottom_disk (cgsl_order_bottom);
  case TL_CACHE_GET_ACOUNTER: return tl_cache_get_acounter ();
  case TL_CACHE_GET_SERVER_STATS: return tl_cache_get_server_stats ();
  default:
    tl_fetch_set_error_format (TL_ERROR_UNKNOWN_FUNCTION_ID, "Unknown op 0x%08x", op);
    return 0;
  }
}

static void cron (void) {
  create_all_outbound_connections ();
  if (get_top_access_stat.t) {
    cron_acounter_update_calls += cache_acounters_update_step (500);
  }
  if (!binlog_disabled) {
    cache_garbage_collector_step (500);
  }
  flush_binlog ();
  dyn_garbage_collector ();
  cache_stats_relax ();
}

engine_t cache_engine;
server_functions_t cache_functions = {
  .cron = cron,
  .save_index = save_index
};

int disallow_save_index (int writing_binlog) {
  kprintf ("Writing index without counters ([-D 16] command line mode) is forbidden. Run cache-engine with [-A] option for allowing this action.\n");
  return -1;
}

static void start_server (void) {
  int i;

  if (!(cache_features_mask & CACHE_FEATURE_ACCESS_QUERIES) && !allow_save_index_without_counters) {
    cache_functions.save_index = disallow_save_index;
  }

  tl_parse_function = cache_parse_function;
  tl_aio_timeout = 2.0;
  server_init (&cache_engine, &cache_functions, &ct_rpc_server, &rpc_methods);

  int quit_steps = 0;

  for (i = 0; ; i++) {
    if (!(i & 255)) {
      vkprintf (1, "epoll_work(): %d out of %d connections, network buffers: %d used, %d out of %d allocated\n",
	       active_connections, maxconn, NB_used, NB_alloc, NB_max);
    }
    epoll_work (57);
    if (!process_signals ()) {
      break;
    }
    if (quit_steps && !--quit_steps) break;
  }
  server_exit (&cache_engine);
}

/*
 *
 *		MAIN
 *
 */


void usage (void) {
  printf ("%s\n", FullVersionStr);
  printf ("usage: cache-engine [-v] [-r] [-i] [-D<disable-feature-mask>] [-T<acounters_init_string>] [-p<port>] [-u<username>] [-b<backlog>] [-c<max-conn>] [-a<binlog-name>] [-l<log-name>] <binlog> \n"
      "\tPerforms cache queries.\n"
      "\t[-v]\toutput statistical and debug information into stderr\n"
      "\t[-r]\tread-only binlog (don't log new events)\n"
      "\t[-T<acounters_init_string>]\tcomma separated list of half-life periods in seconds.\n"
      "\t\tHalf-life period could be terminated by characters ('s','h','d','w','m').\n"
      "\t\tAlso it is possible to use reserved words: hour, day, week and month.\n"
      "\t\tacounter_init_string example: \"3600,1d,week,1m\"\n"
      "\t[-E<cache_id,split_min,split_mod>]\tcreate empty binlog\n"
      "\t[-H<heap-size>]\tdefines maximum heap size\n"
      "\t[-S<hash-slots>]\tset global uries hashtable size, <hash-slots> is a natural number (engine himself finds prime)\n"
      "\t\t\t<hash-slots> should be around half of uries in the engine stats (default value is %d)\n"
#ifdef CACHE_FEATURE_MONTHLY_COUNTER_PERF_STATS
      "\t[-M<filename>]\toutput unreasonable downloads percent for monthly counters to the given file and exit\n"
#endif
#ifdef CACHE_FEATURE_CORRELATION_STATS
      "\t[-C<dir>]\toutput correlaction tables and exit\n"
#endif
      "\t[-D<disable-feature-mask>]\tdisable some features\n"
      "\t[-D 1]\tdisable long queries : get (top|bottom)_(access|disk|stats), delete remote|disk (memory optimization)\n"
      "\t[-D 2]\tdisable uri delete during binlog replaying (fix wrong Garbage Collector logevents)\n"
      "\t[-D 4]\tdisable get detailed_server_stat queries\n"
      "\t[-D 8]\tdisable fast get bottom_disk queries, but increase performance of get top_access for cached files.\n"
      "\t[-D 16]\tdisable access queries (memory optimization).\n"
      "\t[-A]\tallows handling kill -64 in the case of [-D 16] option.\n"
      "\t[-I<timestamp[,path]>]\tspecial indexing mode - dump to stdout \"local_url\\tglobal_url\\n\"\n"
      "\t\t\tit is possible to set filename containing list of local servers (\"cs{$node_id}_{$server_id}\")\n"
      "\t[-J<timestamp,new_cache_id,map_file>]\tspecial indexing mode\n"
      "\t\tin pseudo indexing mode original binlog read till given timestamp\n"
      "\t[-K]\tspecial indexing mode (dump uncached uries)\n",
      hash_size
      );
  exit (2);
}

static const char *options = "AD:E:H:I:J:KS:T:a:b:c:dhil:p:ru:v"
#ifdef CACHE_FEATURE_CORRELATION_STATS
"C:"
#endif
#ifdef CACHE_FEATURE_MONTHLY_COUNTER_PERF_STATS
"M:"
#endif
;

long long parse_memory_limit (int option, const char *limit, long long min_res, long long max_res) {
  long long x;
  char c = 0;
  if (sscanf (optarg, "%lld%c", &x, &c) < 1) {
    kprintf ("Parsing limit for option '%c' fail: %s\n", option, limit);
    exit (1);
  }
  switch (c | 0x20) {
    case ' ': break;
    case 'k':  x <<= 10; break;
    case 'm':  x <<= 20; break;
    case 'g':  x <<= 30; break;
    case 't':  x <<= 40; break;
    default: kprintf ("Parsing limit fail. Unknown suffix '%c'.\n", c); exit (1);
  }

  if (x < min_res) {
    kprintf ("Parsing limit for option '%c' fail (limit is too big), limit: %s, min_limit: %lld.\n", option, limit, min_res);
    exit (1);
  }

  if (x > max_res) {
    kprintf ("Parsing limit for option '%c' fail (limit is too small), limit: %s, max_limit: %lld.\n", option, limit, max_res);
    exit (1);
  }

  return x;
}

int main (int argc, char *argv[]) {
  static int index_mode = 0;
  static int index_timestamp = 0;
  static int disable_feature_mask = 0;
  static char *init_str = NULL;
  static char *index_filter_server_list = NULL;
  static int new_cache_id = 0;
  char c;
  int i, x;
  daemonize = 1;
  set_debug_handlers ();
  while ((i = getopt (argc, argv, options)) != -1) {
    switch (i) {
    case 'A':
      allow_save_index_without_counters = 1;
    break;
#ifdef CACHE_FEATURE_CORRELATION_STATS
    case 'C':
      correlation_stat_dirname = optarg;
      break;
#endif
    case 'D':
      disable_feature_mask |= atoi (optarg);
      break;
    case 'E':
      if (sscanf (optarg, "%d,%d,%d", &cache_id, &log_split_min, &log_split_mod) != 3) {
        usage ();
      }
      index_mode = -1;
      break;
    case 'H':
      dynamic_data_buffer_size = parse_memory_limit (i, optarg, 128 << 20, DYNAMIC_DATA_BIG_BUFFER_SIZE);
      break;
    case 'S':
      hash_size = atoi (optarg);
      break;
    case 'I':
      index_mode = 2;
      x = -1;
      if (sscanf (optarg, "%d%c%n", &index_timestamp, &c, &x) >= 1) {
        if (index_timestamp < 0) {
          kprintf ("invalid -I option: timestamp should be not negatives\n");
          exit (1);
        }
        if (!index_timestamp) {
          index_timestamp = INT_MAX;
        }
        if (c == ',' && x >= 0) {
          index_filter_server_list = optarg + x;
        }
      } else {
        kprintf ("invalid -%c%s option\n", i, optarg);
        exit (1);
      }
      break;
    case 'J':
      index_mode = 3;
      x = -1;
      if (sscanf (optarg, "%d,%d%c%n", &index_timestamp, &new_cache_id, &c, &x) >= 2) {
        if (index_timestamp < 0) {
          kprintf ("invalid -J option: timestamp should be not negative\n");
          exit (1);
        }
        if (!index_timestamp) {
          index_timestamp = INT_MAX;
        }
        if (c == ',' && x >= 0) {
          index_filter_server_list = optarg + x;
        }
      } else {
        kprintf ("invalid -%c%s option\n", i, optarg);
        exit (1);
      }
    break;
    case 'K':
      index_mode = 4;
      index_timestamp = INT_MAX;
    break;
#ifdef CACHE_FEATURE_MONTHLY_COUNTER_PERF_STATS
    case 'M':
      monthly_stat_filename = optarg;
      break;
#endif
   case 'T':
      init_str = optarg;
      break;
    case 'a':
      binlogname = optarg;
      break;
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
    case 'd':
      daemonize ^= 1;
      break;
    case 'h':
      usage();
      return 2;
    case 'i':
      index_mode = 1;
      break;
    case 'l':
      logname = optarg;
      break;
    case 'p':
      port = atoi (optarg);
      break;
    case 'r':
      binlog_disabled = 1;
      break;
    case 'u':
      username = optarg;
      break;
    case 'v':
      verbosity++;
      break;
    default:
      fprintf (stderr, "Unimplemented option %c\n", i);
      exit (1);
    }
  }

  cache_features_mask &= ~disable_feature_mask;

  if (!index_mode && !binlog_disabled & !(cache_features_mask & CACHE_FEATURE_ACCESS_QUERIES)) {
    kprintf ("It is impossible to disable access queries without [-r] option.\n");
    exit (1);
  }

  if ((cache_features_mask & (CACHE_FEATURE_LONG_QUERIES|CACHE_FEATURE_ACCESS_QUERIES)) == CACHE_FEATURE_LONG_QUERIES) {
    kprintf ("NOTICE: Access queries was disabled ([-D 16]), but long queries wasn't. cache-engine clears long queries flag and continues.\n");
    cache_features_mask &= ~CACHE_FEATURE_LONG_QUERIES;
  }

#ifdef CACHE_FEATURE_MONTHLY_COUNTER_PERF_STATS
  cache_features_mask &= ~CACHE_FEATURE_MONTHLY_COUNTER_PERF_STATS;
  assert (monthly_stat_filename);
  assert (cache_features_mask & CACHE_FEATURE_LONG_QUERIES);
  cache_features_mask |= CACHE_FEATURE_MONTHLY_COUNTER_PERF_STATS;
#endif

#ifdef CACHE_FEATURE_CORRELATION_STATS
  assert (correlation_stat_dirname);
#endif

#if defined(CACHE_FEATURE_MONTHLY_COUNTER_PERF_STATS) || defined(CACHE_FEATURE_CORRELATION_STATS)
  cache_features_mask &= ~CACHE_FEATURE_DETAILED_SERVER_STATS;
  if (daemonize) {
    setsid ();
    reopen_logs ();
  }
#endif

  if (index_mode) {
    cache_features_mask &= ~CACHE_FEATURE_DETAILED_SERVER_STATS;
    if (index_mode == 1 && !(cache_features_mask & CACHE_FEATURE_ACCESS_QUERIES) && !allow_save_index_without_counters) {
      disallow_save_index (0);
      return 1;
    }
  }

  #ifdef TEST
  cache_heap_test ();
  #endif

  if (argc != optind + 1) {
    usage();
    return 2;
  }

  vkprintf (2, "cache_features_mask: 0x%x\n", cache_features_mask);

  engine_init (&cache_engine, NULL, index_mode);

  if (index_mode == -1) {
    make_empty_binlog (argv[optind]);
    exit (0);
  }

#ifdef CACHE_FEATURE_CORRELATION_STATS
  if (mkdir (correlation_stat_dirname, 0750) < 0) {
    kprintf ("mkdir (\"%s\", 0750) fail. %m\n", correlation_stat_dirname);
    exit (1);
  }
#endif

  if (init_str == NULL) {
    init_str = acounters_init_string;
  }

  if (cache_hashtable_init (hash_size) < 0) {
    kprintf ("cache_hashtable_init (%d) fail.\n", hash_size);
    exit (1);
  }
  cache_garbage_collector_init ();

  if (cache_set_amortization_tables_initialization_string (init_str) < 0) {
    kprintf ("Illegal acounters_init_string - \"%s\".\n", optarg);
    exit (1);
  }

  log_ts_interval = 3;

  if (engine_preload_filelist (argv[optind], binlogname) < 0) {
    kprintf ("cannot open binlog files for %s\n", binlogname ? binlogname : argv[optind]);
    exit (1);
  }

  vkprintf (3, "engine_preload_filelist done\n");

  //Snapshot reading

  if (cache_pseudo_index_check_mode (index_mode)) {
    if (cache_pseudo_index_init (index_mode, index_timestamp, index_filter_server_list, new_cache_id) < 0) {
      kprintf ("cache_pseudo_index_init failed.\n");
      exit (1);
    }
    if (engine_snapshot_replica) {
      int p = engine_snapshot_replica->snapshot_num - 1;
      while (p >= 0) {
        Snapshot = open_snapshot (engine_snapshot_replica, p);
        if (Snapshot) {
          engine_snapshot_name = Snapshot->info->filename;
          engine_snapshot_size = Snapshot->info->file_size;
          vkprintf (1, "load index file %s (size %lld)\n", engine_snapshot_name, engine_snapshot_size);
          index_load_time = -mytime ();
          i = load_index (Snapshot);
          index_load_time += mytime ();
          if (!i) {
            break;
          }
          if (i < 0 && i != CACHE_ERR_NEW_INDEX) {
            kprintf ("load_index returned fail code %d.\n", i);
            exit (1);
          }
        }
        --p;
      }
    }
  } else {
    Snapshot = open_recent_snapshot (engine_snapshot_replica);
    if (Snapshot) {
      engine_snapshot_name = Snapshot->info->filename;
      engine_snapshot_size = Snapshot->info->file_size;
      vkprintf (1, "load index file %s (size %lld)\n", engine_snapshot_name, engine_snapshot_size);
      index_load_time = -mytime ();
      i = load_index (Snapshot);
      if (i < 0) {
        kprintf ("load_index returned fail code %d.\n", i);
        exit (1);
      }
      index_load_time += mytime();
    } else {
      engine_snapshot_name = NULL;
      engine_snapshot_size = 0;
      index_load_time = 0;
    }
  }

  //Binlog reading
  Binlog = open_binlog (engine_replica, jump_log_pos);
  if (!Binlog) {
    kprintf ("fatal: cannot find binlog for %s, log position %lld\n", engine_replica->replica_prefix, jump_log_pos);
    exit (1);
  }

  binlogname = Binlog->info->filename;

  vkprintf (1, "replaying binlog file %s (size %lld) from position %lld\n", binlogname, Binlog->info->file_size, jump_log_pos);
  binlog_load_time = -mytime();

  clear_log ();

  vkprintf (3, "init_log_date (jump_log_pos: %lld, jump_log_ts: %d, jump_log_crc32: 0x%x)\n", jump_log_pos, jump_log_ts, jump_log_crc32);

  init_log_data (jump_log_pos, jump_log_ts, jump_log_crc32);

  vkprintf (1, "replay log events started\n");
  i = replay_log (0, 1);
  vkprintf (1, "replay log events finished\n");

  binlog_load_time += mytime();
  binlog_loaded_size = log_readto_pos - jump_log_pos;

  vkprintf (2, "binlog_loaded_size: %lld\n", binlog_loaded_size);

  if (!binlog_disabled) {
    clear_read_log ();
  }

  if (i == -2) {
    long long true_readto_pos = log_readto_pos - Binlog->info->log_pos + Binlog->offset;
    kprintf ("REPAIR: truncating %s at log position %lld (file position %lld)\n", Binlog->info->filename, log_readto_pos, true_readto_pos);
    if (truncate (Binlog->info->filename, true_readto_pos) < 0) {
      perror ("truncate()");
      exit (2);
    }
  } else if (i < 0) {
    kprintf ("fatal: error reading binlog\n");
    exit (1);
  }

  clear_write_log ();
  start_time = time (0);

#ifdef CACHE_FEATURE_MONTHLY_COUNTER_PERF_STATS
  if (monthly_stat_filename) {
    if (!now) {
      vkprintf (1, "assign log_read_until to now (now: %d, log_read_until: %d)\n", now, log_read_until);
      now = log_read_until;
    }
    if (cache_monthly_stat_report (monthly_stat_filename) < 0) {
      exit (1);
    }
    exit (0);
  }
#endif

#ifdef CACHE_FEATURE_CORRELATION_STATS
  if (correlation_stat_dirname) {
    if (cache_correlation_stat_report (correlation_stat_dirname) < 0) {
      exit (1);
    }
    exit (0);
  }
#endif

  if (cache_pseudo_index_check_mode (index_mode)) {
    cache_save_pseudo_index ();
    return 0;
  }

  if (index_mode) {
    save_index (0);
  } else {
    start_server ();
  }

  return 0;
}

