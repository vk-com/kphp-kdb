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

    Copyright 2009-2013 Vkontakte Ltd
              2009-2013 Nikolai Durov
              2009-2013 Andrei Lopatin
		   2013 Vitaliy Valtman
*/

#define	_FILE_OFFSET_BITS	64

#include <assert.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/types.h>
#include <aio.h>
#include <ctype.h>
#include <signal.h>

#include "md5.h"
#include "net-events.h"
#include "net-buffers.h"
#include "server-functions.h"
#include "kdb-text-binlog.h"
#include "text-data.h"
#include "word-split.h"
#include "net-connections.h"
#include "net-memcache-server.h"
#include "net-http-server.h"
#include "net-crypto-aes.h"
#include "stemmer.h"
#include "net-parse.h"

#include "net-rpc-server.h"
#include "net-rpc-common.h"
#include "vv-tl-parse.h"
#include "vv-tl-aio.h"

#include "text-tl.h"
#include "text-interface-structures.h"

#define	VERSION_STR	"text-engine-0.40"

#define TCP_PORT 11211

#define MAX_NET_RES	(1L << 16)

#undef TL_NAMESPACE
#define TL_NAMESPACE TL_TEXT_


/*
 *
 *		TEXT ENGINE
 *
 */

long long http_queries_ok, http_queries_bad, http_queries_delayed, http_failed[4];
int pending_http_queries;

int text_engine_wakeup (struct connection *c);
int text_engine_alarm (struct connection *c);


conn_type_t ct_text_engine_server = {
  .magic = CONN_FUNC_MAGIC,
  .title = "text_engine_server",
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
  .wakeup = text_engine_wakeup,
  .alarm = text_engine_alarm,
#ifndef NOAES
  .crypto_init = aes_crypto_init,
  .crypto_free = aes_crypto_free,
  .crypto_encrypt_output = aes_crypto_encrypt_output,
  .crypto_decrypt_input = aes_crypto_decrypt_input,
  .crypto_needed_output_bytes = aes_crypto_needed_output_bytes,
#endif
};



int verbosity = 0, test_mode = 0, utf8_mode = 0;

// unsigned char is_letter[256];
char *progname = "text-engine", *username, *binlogname, *logname;

/* stats counters */
int start_time;
long long binlog_loaded_size;
long long netw_queries, search_queries, delete_queries, get_queries, update_queries,
  minor_update_queries, increment_queries;
long long tot_response_words, tot_response_bytes;
double binlog_load_time, index_load_time, total_get_time;

volatile int sigpoll_cnt;

#define STATS_BUFF_SIZE	(1 << 20)
int stats_buff_len;
char stats_buff[STATS_BUFF_SIZE + (1 << 16)];

int udp_enabled;
char *aes_pwd_file;
/*
 *
 *		SERVER
 *
 */


int backlog = BACKLOG, port = TCP_PORT, maxconn = MAX_CONNECTIONS, daemonize = 1, is_server = 0;
struct in_addr settings_addr;
int active_connections;

int memcache_store (struct connection *c, int op, const char *key, int key_len, int flags, int delay, int size);
int memcache_get (struct connection *c, const char *key, int key_len);
int memcache_incr (struct connection *c, int op, const char *key, int key_len, long long arg);
int memcache_delete (struct connection *c, const char *key, int key_len);
int memcache_stats (struct connection *c);
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
  .memcache_fallback_type = &ct_text_engine_server,
  .memcache_fallback_extra = &memcache_methods
};

int quit_steps;

static double safe_div (double x, double y) { return y > 0 ? x/y : 0; }

int text_prepare_stats (void) {
  int uptime = now - start_time;
  int log_uncommitted = compute_uncommitted_log_bytes();

  return stats_buff_len = snprintf (
		  stats_buff, STATS_BUFF_SIZE,
		  "heap_used\t%ld\n"
		  "heap_max\t%ld\n"
		  "binlog_original_size\t%lld\n"
		  "binlog_loaded_bytes\t%lld\n"
		  "binlog_load_time\t%.6fs\n"
		  "current_binlog_size\t%lld\n"
		  "binlog_uncommitted_bytes\t%d\n"
		  "binlog_path\t%s\n"
		  "binlog_first_timestamp\t%d\n"
		  "binlog_read_timestamp\t%d\n"
		  "binlog_last_timestamp\t%d\n"
		  "index_users\t%d\n"
		  "index_loaded_bytes\t%d\n"
		  "index_size\t%lld\n"
		  "index_path\t%s\n"
		  "index_load_time\t%.6fs\n"
		  "index_last_global_id\t%d\n"
		  "index_crc_enabled\t%d\n"
		  "index_extra_mask\t%d\n"
		  "read_extra_mask\t%d\n"
		  "write_extra_mask\t%d\n"
		  "memory_users\t%d\n"
		  "tree_nodes\t%d\n"
		  "online_tree_nodes\t%d\n"
		  "online_hold_time\t%d\n"
		  "incore_messages\t%d\n"
		  "msg_search_nodes\t%d\n"
		  "msg_search_nodes_bytes\t%ld\n"
		  "edit_text_nodes\t%d\n"
		  "edit_text_bytes\t%ld\n"
		  "search_enabled\t%d\n"
		  "stemmer_enabled\t%d\n"
		  "hashtags_enabled\t%d\n"
		  "searchtags_enabled\t%d\n"
		  "incore_persistent_history_lists\t%d\n"
		  "incore_persistent_history_events\t%d\n"
		  "incore_persistent_history_bytes\t%lld\n"
		  "last_global_id\t%d\n"
		  "current_user_metafiles\t%d\n"
		  "current_user_bytes\t%lld\n"
		  "loaded_user_metafiles\t%d\n"
		  "loaded_user_bytes\t%lld\n"
		  "allocated_metafile_bytes\t%lld\n"
		  "max_metafile_bytes\t%lld\n"
		  "current_history_metafiles\t%d\n"
		  "current_history_bytes\t%lld\n"
		  "loaded_history_metafiles\t%d\n"
		  "loaded_history_bytes\t%lld\n"
		  "allocated_history_metafile_bytes\t%lld\n"
		  "current_search_metafiles\t%d\n"
		  "current_search_bytes\t%lld\n"
		  "loaded_search_metafiles\t%d\n"
		  "loaded_search_bytes\t%lld\n"
		  "allocated_search_metafile_bytes\t%lld\n"
		  "queries_search\t%lld\n"
		  "qps_search\t%.3f\n"
		  "queries_get\t%lld\n"
		  "qps_get\t%.3f\n"
		  "avg_get_time\t%.6f\n"
		  "queries_delete\t%lld\n"
		  "qps_delete\t%.3f\n"
		  "queries_minor_update\t%lld\n"
		  "qps_minor_update\t%.3f\n"
		  "queries_increment\t%lld\n"
		  "qps_increment\t%.3f\n"
		  "tot_aio_queries\t%lld\n"
		  "active_aio_queries\t%lld\n"
		  "expired_aio_queries\t%lld\n"
		  "aio_read_errors\t%lld\n"
		  "aio_crc_errors\t%lld\n"
		  "avg_aio_query_time\t%.6f\n"
		  "alloc_history_strings\t%d\n"
		  "alloc_history_strings_size\t%lld\n"
		  "http_connections\t%d\n"
		  "pending_http_queries\t%d\n"
		  "http_queries\t%lld\n"
		  "http_queries_ok\t%lld\n"
		  "http_queries_delayed\t%lld\n"
		  "http_queries_bad\t%lld\n"
		  "http_bad_headers\t%lld\n"
		  "http_success\t%lld\n"
		  "http_failed1\t%lld\n"
		  "http_failed2\t%lld\n"
		  "http_failed3\t%lld\n"
		  "http_qps\t%.6f\n"
		  "version\t" VERSION_STR " compiled at " __DATE__ " " __TIME__ " by gcc " __VERSION__ " "
#ifdef __LP64__
		  "64-bit"
#else
     		  "32-bit"
#endif
		  " after commit " COMMIT "\n",
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
		  idx_users, idx_loaded_bytes,
		  engine_snapshot_size, engine_snapshot_name,
		  index_load_time,
		  idx_last_global_id,
		  idx_crc_enabled,
		  index_extra_mask,
		  read_extra_mask,
		  write_extra_mask,
		  tot_users,
		  tree_nodes,
		  online_tree_nodes,
		  hold_online_time,
		  incore_messages,
		  msg_search_nodes,
		  msg_search_nodes_bytes,
		  edit_text_nodes,
		  edit_text_bytes,
		  search_enabled,
		  use_stemmer,
		  hashtags_enabled,
		  searchtags_enabled,
		  incore_persistent_history_lists,
		  incore_persistent_history_events,
		  incore_persistent_history_bytes,
		  last_global_id,
		  cur_user_metafiles,
		  cur_user_metafile_bytes,
		  tot_user_metafiles,
		  tot_user_metafile_bytes,
		  allocated_metafile_bytes,
		  metafile_alloc_threshold,
		  cur_history_metafiles,
		  cur_history_metafile_bytes,
		  tot_history_metafiles,
		  tot_history_metafile_bytes,
		  allocated_history_metafile_bytes,
		  cur_search_metafiles,
		  cur_search_metafile_bytes,
		  tot_search_metafiles,
		  tot_search_metafile_bytes,
		  allocated_search_metafile_bytes,
		  search_queries,
		  safe_div (search_queries, uptime),
		  get_queries,
		  safe_div (get_queries, uptime),
		  get_queries > 0 ? total_get_time / get_queries : 0,
		  delete_queries,
		  safe_div (delete_queries, uptime),
		  minor_update_queries,
		  safe_div (minor_update_queries, uptime),
		  increment_queries,
		  safe_div (increment_queries, uptime),
		  tot_aio_queries,
		  active_aio_queries,
		  expired_aio_queries,
		  aio_read_errors,
		  aio_crc_errors,
		  tot_aio_queries > 0 ? total_aio_time / tot_aio_queries : 0,
		  alloc_history_strings,
		  alloc_history_strings_size,
		  http_connections,
		  pending_http_queries,
		  http_queries,
		  http_queries_ok,
		  http_queries_delayed,
		  http_queries_bad,
		  http_bad_headers,
		  http_failed[0],
		  http_failed[1],
		  http_failed[2],
		  http_failed[3],
		  safe_div(http_queries, uptime)
		  );
}

int memcache_wait (struct connection *c, const char *key, int key_len) {
  if (c->flags & C_INTIMEOUT) {
    if (verbosity >= 0) {
      fprintf (stderr, "memcache_wait: IN TIMEOUT (%p), key='%.*s'\n", c, key_len, key);
    }
    return 0;
  }
  if (c->Out.total_bytes > 8192) {
    c->flags |= C_WANTWR;
    c->type->writer (c);
  }
  //fprintf (stderr, "memcache_get_nonblock returns -2, WaitAio=%p\n", WaitAio);
  if (!WaitAioArrPos) {
    return 0;
  }
  c->pending_queries = 0;
  c->generation = ++conn_generation;
  int i;
  for (i = 0; i < WaitAioArrPos; i++) {
    assert (WaitAioArr[i]);
    create_aio_query (WaitAioArr[i], c, 2.7, &aio_metafile_query_type);
  }
  
  set_connection_timeout (c, 2.5);
    
  
  c->status = conn_wait_aio;

  return 0; 
}

static inline void eat_at (const char *key, int key_len, char **new_key, int *new_len) {
  *new_key = (char *)key;
  *new_len = key_len;

  if ((*key >= '0' && *key <= '9') || (*key == '-' && key[1] >= '0' && key[1] <= '9')) {
    key++;
    while (*key >= '0' && *key <= '9') {
      key++;
    }
    if (*key++ == '@') {
      *new_len = key + key_len - *new_key;
      *new_key = (char *)key;
    }
  }
}

static inline void init_tmp_buffers (struct connection *c) {
  free_tmp_buffers (c);
  c->Tmp = alloc_head_buffer ();
  assert (c->Tmp);
}

int memcache_store (struct connection *c, int op, const char *key, int key_len, int flags, int delay, int size) {
  int user_id, random_tag = 0, extra_mask = 0, new_key_len;
  char *new_key;
  if (verbosity > 1) {
    fprintf (stderr, "memcache_store: key='%s', key_len=%d, value_len=%d\n", key, key_len, size);
  }
  free_tmp_buffers (c);
  if (key_len >= 6 && !strncmp (key, "newmsg", 6)) {
    if (!((sscanf (key, "newmsg%d,%d#%d", &user_id, &extra_mask, &random_tag) >= 2 || 
           sscanf (key, "newmsg%d#%d", &user_id, &random_tag) >= 1) 
        && user_id && size < STATS_BUFF_SIZE)) {
      return -2;
    }
    assert (read_in (&c->In, stats_buff, size) == size);
    char *text = memchr (stats_buff, '\n', size);
    if (!text || (extra_mask & ~MAX_EXTRA_MASK)) {
      return 0;
    }
    *text++ = 0;
    stats_buff[size] = 0;
    static struct {
      struct lev_add_message M;
      int extra[16];
    } Z;
    long long legacy_id = 0;
    memset (&Z, 0, sizeof (Z));
    if (extra_mask) {
      int x = -1, i;
      long long y;
      if (sscanf (stats_buff, "%d,%d,%d,%lld,%n", &Z.M.type, &Z.M.peer_id, &Z.M.peer_msg_id, &legacy_id, &x) < 4 || x < 0) {
        return 0;
      }
      char *ptr = stats_buff + x - 1;
      int *W_extra = Z.M.extra;
      for (i = 1; i < MAX_EXTRA_MASK; i <<= 1) {
        if (extra_mask & i) {
          if (*ptr++ != ',') {
            return 0;
          }
          char *old_ptr = ptr;
          y = strtoll (ptr, &ptr, 10);
          if (old_ptr == ptr) {
            return 0;
          }
          if (i < 256) {
            if (y != (int) y) {
              return 0;
            }
            *W_extra++ = y;
          } else {
            *(long long *) W_extra = y;
            W_extra += 2;
          }
        }
      }
      sscanf (ptr, ":%u,%u,%u,%llu", &Z.M.ip, &Z.M.port, &Z.M.front, &Z.M.ua_hash);
    } else if (sscanf (stats_buff, "%d,%d,%d,%lld:%u,%u,%u,%llu", &Z.M.type, &Z.M.peer_id, &Z.M.peer_msg_id, &legacy_id, &Z.M.ip, &Z.M.port, &Z.M.front, &Z.M.ua_hash) < 2) {
      return 0;
    }
    Z.M.date = 0;
    if (Z.M.type & -0x10000) {
      return 0;
    }
    Z.M.type |= (extra_mask << 16);
    Z.M.text_len = stats_buff + size - text;
    Z.M.user_id = user_id;
    vkprintf (2, "before do_store_new_message(): flags=%04x, uid=%d, peer=%d, legacy_id=%lld, text_len=%d\n", Z.M.type, Z.M.user_id, Z.M.peer_id, legacy_id, Z.M.text_len);
    int res = do_store_new_message (&Z.M, random_tag, text, legacy_id);
    vkprintf (2, "do_store_new_message() = %d\n", res);
    return res > 0;
  }

  if (key_len >= 12 && !strncmp (key, "message_text", 12)) {
    int local_id = 0;
    if (op != mct_replace || sscanf (key, "message_text%d_%d", &user_id, &local_id) != 2 ||
        !user_id || local_id <= 0 || size >= STATS_BUFF_SIZE || size >= MAX_TEXT_LEN) {
      return -2;
    }

    if (verbosity > 1) {
      fprintf (stderr, "before do_replace_message_text(): uid=%d, local_id=%d, text_len=%d\n", user_id, local_id, size);
    }

    WaitAioArrClear ();
    //WaitAio = WaitAio2 = WaitAio3 = 0;

    int flags = get_message_flags (user_id, local_id, 1);

    if (flags == -2) {
      if (verbosity > 1) {
	fprintf (stderr, "postponing do_replace_message_text(): scheduling aio to check message existence\n");
      }
      return memcache_wait (c, key, key_len);
    }

    if (flags == -1) {
      if (verbosity > 1) {
	fprintf (stderr, "ignoring do_replace_message_text(): message%d_%d does not exist\n", user_id, local_id);
      }
      return -2;
    }

    assert (read_in (&c->In, stats_buff, size) == size);
    stats_buff[size] = 0;

    int res = do_replace_message_text (user_id, local_id, stats_buff, size);

    if (verbosity > 1) {
      fprintf (stderr, "do_replace_message_text () = %d\n", res);
    }

    return res > 0;
  }

  if (key_len >= 5 && !strncmp (key, "flags", 5) && size < 20) {
    int local_id;
    if (sscanf (key, "flags%d_%d", &user_id, &local_id) == 2 && user_id && local_id > 0) { 
      assert (read_in (&c->In, stats_buff, size) == size);
      stats_buff[size] = 0;
      int flags = atoi (stats_buff);
      if (size > 0 && stats_buff[0] >= '0' && stats_buff[0] <= '9') {
        int res = do_set_flags (user_id, local_id, flags);
        return res > 0;
      }
      return 0;
    }
    return -2;
  }

  eat_at (key, key_len, &new_key, &new_key_len);

  if (new_key_len >= 11 && !strncmp (new_key, "ExtraFields", 11) && size < 24) {
    int mask = -1;
    if (sscanf (new_key, "ExtraFields%d\n", &mask) >= 1 && !(mask & ~MAX_EXTRA_MASK)) {
      assert (read_in (&c->In, stats_buff, size) == size);
      stats_buff[size] = 0;
      if (atoi (stats_buff) != (mask ^ 0xbeda)) {
        return 0;
      }
      int res = do_change_mask (mask);
      return res > 0;
    }
    return -2;
  }

  if (key_len >= 5 && !strncmp (key, "extra", 5) && size < 24) {
    int local_id, value_id;
    if (sscanf (key, "extra%d_%d:%d", &user_id, &local_id, &value_id) == 3 && user_id && local_id > 0 && (unsigned) value_id < 8) {
      assert (read_in (&c->In, stats_buff, size) == size);
      stats_buff[size] = 0;
      struct value_data V;
      char *ptr;
      V.data[0] = strtol (stats_buff, &ptr, 10);
      if (ptr != stats_buff + size || !size) {
        return 0;
      }
      V.fields_mask = V.zero_mask = (1 << value_id);
      int res = do_set_values (user_id, local_id, &V);
      return res > 0;
    }
    return -2;
  }
      
  if (key_len >= 5 && !strncmp (key, "Extra", 5) && size < 24) {
    int local_id, value_id;
    if (sscanf (key, "Extra%d_%d:%d", &user_id, &local_id, &value_id) == 3 && user_id && local_id > 0 && value_id >= 8 && value_id < 12) {
      assert (read_in (&c->In, stats_buff, size) == size);
      stats_buff[size] = 0;
      struct value_data V;
      char *ptr;
      V.ldata[0] = strtoll (stats_buff, &ptr, 10);
      if (ptr != stats_buff + size || !size) {
        return 0;
      }
      V.fields_mask = V.zero_mask = (1 << value_id);
      int res = do_set_values (user_id, local_id, &V);
      return res > 0;
    }
    return -2;
  }

  if (key_len >= 14 && !strncmp (key, "history_action", 14) && size < 65536) {
    int action_id, place, flags = 0;
    if (sscanf (key, "history_action%d#%d", &user_id, &action_id) == 2 && action_id >= 50 && action_id < 200 && (size < 100 || action_id >= 100)) {
      assert (read_in (&c->In, stats_buff, size) == size);
      stats_buff[size] = 0;
      if (action_id < 100) {
	if (sscanf (stats_buff, "%d,%d", &place, &flags) < 1) {
	  return 0;
	}
	return do_update_history (user_id, place, flags, action_id) > 0;
      } else {
	return do_update_history_extended (user_id, stats_buff, size, action_id) > 0;
      }
    }
    return -2;
  }

  if (key_len >= 8 && !strncmp (key, "userdata", 8) && size < 20) {
    if (sscanf (key, "userdata%d", &user_id) == 1) {
      assert (read_in (&c->In, stats_buff, size) == size);
      stats_buff[size] = 0;
      int mode = atoi (stats_buff);
      if (size > 0 && stats_buff[0] >= '0' && stats_buff[0] <= '9') {
        switch (mode) {
        case 0:
          unload_user_metafile (user_id);
          return 1;
        case 1:
        case 2:
          load_user_metafile (user_id);
          return 1;
        }
      }
      return 0;
    }
    return -2;
  }

  if (key_len >= 7 && !strncmp (key, "secret", 6) && size == 8) {
    if (sscanf (key, "secret%d", &user_id) == 1 && user_id) { 
      assert (read_in (&c->In, stats_buff, size) == size);
      stats_buff[size] = 0;
      int res = set_user_secret (user_id, stats_buff);
      return res > 0;
    }
    return -2;
  }

  if (
      (
       (key_len >= 6 && !strncmp (key, "online", 6)) ||
       (key_len >= 7 && !strncmp (key, "offline", 7)) ||
       (key_len >= 4 && !strncmp (key, "temp", 4))
      ) && (size < STATS_BUFF_SIZE)
     ) {
    int online = (key[0] == 't' ? 3 : (key[1] == 'n')), user_id = 0;
    if (sscanf (key + 7 - online, "%d", &user_id) != 1 || !user_id) {
      return -2;
    }
    assert (read_in (&c->In, stats_buff, size) == size);
    int *A, N;
    if (size > 3 && stats_buff[3] == 0) {
      N = *(int *) stats_buff;
      if (N <= 0 || N > MAX_USER_FRIENDS || size != 4 * N + 4) {
        return 0;
      }
      A = (int *)(stats_buff + 4);
    } else {
      N = 0;
      stats_buff[size] = 0;
      char *ptr = stats_buff;
      while (N < MAX_USER_FRIENDS && *ptr) {
        char *oldptr;
        R[N++] = strtoul (oldptr = ptr, &ptr, 10);
        if (ptr == oldptr || (*ptr && *ptr++ != ',')) {
          return 0;
        }
      }
      if (*ptr) {
        return 0;
      }
      A = R;
    }
    if (online == 3) {
      init_tmp_buffers (c);
      write_out (c->Tmp, &user_id, 4);
      write_out (c->Tmp, A, N * 4);
      return 1;
    }
    int res = online ? user_friends_online (user_id, N, A) : user_friends_offline (user_id, N, A);
    return res > 0;
  }

  return -2;
}

int memcache_stats (struct connection *c) {
  int len = text_prepare_stats ();
  int len2 = prepare_stats (c, stats_buff + len, STATS_BUFF_SIZE - len);
  write_out (&c->Out, stats_buff, len + len2);
  write_out (&c->Out, "END\r\n", 5);
  return 0;
}


int mc_get_message (char *buff, int user_id, int local_id, int mode, int max_len) {
  struct imessage_long *M = (struct imessage_long *) (stats_buff + 1024);
  int res = load_message_long (M, user_id, local_id, max_len, (mode >> 3) ^ 2);
  struct message *msg;
  struct message_extras *msg_e;
  char *ptr = buff, *start;
  int msg_len, kludges_len;

  if (verbosity > 1) {
    fprintf (stderr, "in mc_get_message(%d,%d,%d): ptr=%p, res=%d, msg=%p, msg->len=%d, msg->kludges=%d\n", user_id, local_id, mode, ptr, res, M->msg, res > 0 && M->msg ? M->msg->len : -1, res > 0 && M->msg ? M->msg->kludges_size : -1);
  }

  if (res != 1) {
    return res;
  }
  msg = M->msg;
  assert (msg);
  msg_e = M->m_extra;

  ptr += sprintf (ptr, "%d,%d,%d", msg->flags, msg->date, msg->peer_id);
  if (mode & 1) {
    ptr += sprintf (ptr, ",%d", msg->global_id);
  }
  if (mode & 2) {
    ptr += sprintf (ptr, ",%lld", msg->legacy_id);
  }
  if (mode & 4) {
    ptr += sprintf (ptr, ",%d", msg->peer_msg_id);
  }
  if (mode & 8) {
    ptr += sprintf (ptr, ",%d", msg->kludges_size);
  }
  if (mode & 256) {
    ptr += sprintf (ptr, ",%u", msg_e ? msg_e->ip : 0);
  }
  if (mode & 512) {
    ptr += sprintf (ptr, ",%d", msg_e ? msg_e->port : 0);
  }
  if (mode & 1024) {
    ptr += sprintf (ptr, ",%u", msg_e ? msg_e->front : 0);
  }
  if (mode & 2048) {
    ptr += sprintf (ptr, ",%llu", msg_e ? msg_e->ua_hash : 0);
  }

  int *M_extra = msg->extra, i, mask = (mode >> 16);

  for (i = 1; i < MAX_EXTRA_MASK; i <<= 1) {
    if (read_extra_mask & mask & i) {
      if (i < 256) {
        ptr += sprintf (ptr, ",%d", *M_extra);
      } else {
        ptr += sprintf (ptr, ",%lld", *(long long *) M_extra);
      }
    } else if (mask & i) {
      *ptr++ = ',';
      *ptr++ = '0';
    }
    if (index_extra_mask & i) {
      M_extra += (i < 256 ? 1 : 2);
    }
  }

  assert (msg->text + text_shift == (char *) M_extra);

  *ptr++ = '\t';

  if (!M->edit_text) {
    msg_len = msg->len;
    kludges_len = msg->kludges_size;
    if (kludges_len < 0) {
      kludges_len = 0;
    }
    start = msg->text + text_shift;
  } else {
    edit_text_t *X = M->edit_text;
    msg_len = X->len;
    kludges_len = X->kludges_size;
    start = X->text;
  }

  switch (mode & 24) {
  case 0:
    start += kludges_len;
    msg_len -= kludges_len;
    break;
  case 8:
    break;
  case 16:
    msg_len = 0;
    break;
  case 24:
    msg_len = kludges_len;
    break;
  }

  if (msg_len > max_len) {
    msg_len = max_len;
  }

  memmove (ptr, start, msg_len);
  ptr[msg_len] = 0;

//  fprintf (stderr, "prepared string (buff=%p ptr=%p len=%d): '%.*s'\n", buff, ptr, ptr - buff + msg_len, ptr - buff + msg_len, buff);

  return ptr - buff + msg_len;
}

static int memcache_get_nonblock (struct connection *c, const char *key, int key_len) {
  int user_id, peer_id, local_id;

  if (key_len >= 11 && !strncmp (key, "joinpeermsg", 11)) {
    int tag, limit = -1, keep_tag, flags = -1;
    if (c->Tmp && sscanf (key, "joinpeermsg%d_%d,%d#%d", &user_id, &tag, &flags, &limit) >= 4 && user_id && tag > 0 && !flags && limit > 0) {
      nb_iterator_t it;
      nbit_set (&it, c->Tmp);
      if (nbit_read_in (&it, &keep_tag, 4) != 4 || keep_tag != tag) {
	free_tmp_buffers (c);
	return 0;
      }
      assert (MAX_RES * 4 <= STATS_BUFF_SIZE);

      int len = nbit_read_in (&it, stats_buff, MAX_RES * 4);
      assert (len >= 0 && len <= MAX_RES * 4 && !(len & 3));
      
      int res = get_join_peer_msglist (user_id, (int *) stats_buff, len >> 2, limit);
      if (res < 0) {
	if (res != -2) {
	  free_tmp_buffers (c);
	}
	return res;
      }
      return_one_key_list (c, key, key_len, res, 0, R, R_cnt);
    }
    free_tmp_buffers (c);
    return 0;
  }

  free_tmp_buffers (c);

  if (key_len >= 7 && !strncmp (key, "message", 7)) {
    int mode = 0, max_len = 0x7fffffff, res;
    if (sscanf (key, "message%d_%d#%d,%d", &user_id, &local_id, &mode, &max_len) >= 2) {
      if (max_len < 0) {
        max_len = 0;
      }
      if (max_len >= STATS_BUFF_SIZE - 2048) {
        max_len = STATS_BUFF_SIZE - 2048;
      }
      res = mc_get_message (stats_buff, user_id, local_id, mode, max_len);
      if (res <= 0) {
        return res;
      }
      return_one_key (c, key, stats_buff, res);
    }
    return 0;
  }

  if (key_len >= 5 && !strncmp (key, "flags", 5)) {
    if (sscanf (key, "flags%d_%d", &user_id, &local_id) >= 2) {
      int res = get_message_flags (user_id, local_id, 1);
      if (res < 0) {
        return res;
      }
      return_one_key (c, key, stats_buff, sprintf (stats_buff, "%d", res));
    }
    return 0;
  }

  if (key_len == 11 && !strncmp (key, "ExtraFields", 11)) {
    return_one_key (c, key, stats_buff, sprintf (stats_buff, "%d:%d:%d", write_extra_mask, read_extra_mask, index_extra_mask));
    return 0;
  }

  if (key_len >= 5 && !strncmp (key, "extra", 5)) {
    int local_id, value_id;
    if (sscanf (key, "extra%d_%d:%d", &user_id, &local_id, &value_id) == 3 && user_id && local_id > 0 && (unsigned) value_id < 8) {
      long long nres;
      int res = get_message_value (user_id, local_id, value_id, 1, &nres);
      if (res < 0) {
        return res;
      }
      return_one_key (c, key, stats_buff, sprintf (stats_buff, "%d", (int) nres));
    }
    return 0;
  }

  if (key_len >= 5 && !strncmp (key, "Extra", 5)) {
    int local_id, value_id;
    if (sscanf (key, "Extra%d_%d:%d", &user_id, &local_id, &value_id) == 3 && user_id && local_id > 0 && value_id >= 8 && value_id < 12) {
      long long nres;
      int res = get_message_value (user_id, local_id, value_id, 1, &nres);
      if (res < 0) {
        return res;
      }
      return_one_key (c, key, stats_buff, sprintf (stats_buff, "%lld", nres));
    }
    return 0;
  }

  if (key_len >= 12 && !strncmp (key, "peermsglist", 11)) {
    if (key[11] == 'p') {
      if (sscanf (key, "peermsglistpos%d_%d:%d", &user_id, &peer_id, &local_id) >= 3) {
	int res = get_peer_msglist_pos (user_id, peer_id, local_id);
	if (res < 0) {
	  return res;
	}
	return_one_key (c, key, stats_buff, sprintf (stats_buff, "%d", res));
      }
      return 0;
    } else {
      int from = 0, to = 0;
      if (sscanf (key, "peermsglist%d_%d#%d,%d", &user_id, &peer_id, &from, &to) >= 2) {
	int res = get_peer_msglist (user_id, peer_id, from, to);
	if (res < 0) {
	  return res;
	}
	return_one_key_list (c, key, key_len, res, 0, R, R_cnt);
      }
      return 0;
    }
  }

  if (key_len >= 7 && !strncmp (key, "search(", 7)) {
    int max_res = -1, x = -1;
    if (sscanf (key, "search(%d,%u,%n", &user_id, &max_res, &x) >= 2 && x > 0) {
      search_queries++;
      int res = get_search_results (user_id, 0, -1, -1, 0, 0, max_res, key + x);
      if (res < 0) {
        return res;
      }
      return_one_key_list (c, key, key_len, res, 0, R, R_cnt);
    }
    return 0;
  }

  if (key_len >= 8 && !strncmp (key, "xsearch(", 8)) {
    int max_res = -1, x = -1, and_mask = -1, xor_mask = -1, peer_id = 0, min_time = 0, max_time = 0;
    if (sscanf (key, "xsearch(%d,%d:%d,%d,%d,%d,%u,%n", &user_id, &and_mask, &xor_mask, &peer_id, &min_time, &max_time, &max_res, &x) >= 7 && x > 0) {
      search_queries++;
      int res = get_search_results (user_id, peer_id, and_mask, xor_mask, min_time, max_time, max_res, key + x);
      if (res < 0) {
        return res;
      }
      return_one_key_list (c, key, key_len, res, 0, R, R_cnt);
    }
    return 0;
  }

  if (key_len >= 10 && !strncmp (key, "topmsglist", 10)) {
    int from = 0, to = 0;
    if (sscanf (key, "topmsglist%d#%d,%d", &user_id, &from, &to) >= 1) {
      int res = get_top_msglist (user_id, from, to);
      if (res < 0) {
        return res;
      }
      return_one_key_list (c, key, key_len, res, 0, R, R_cnt);
    }
    return 0;
  }

  if (key_len == 14 && !strcmp (key, "sublists_types")) {
    struct sublist_descr SD[16];
    int i, res = get_sublist_types (SD);
    char *ptr = stats_buff;
    assert (res >= 0 && res <= 16);
    for (i = 0; i < res; i++) {
      if (i) {
        *ptr++ = ',';
      }
      ptr += sprintf (ptr, "%d:%d", SD[i].and_mask, SD[i].xor_mask);
    }
    return_one_key (c, key, stats_buff, ptr - stats_buff);
    return 0;
  }

  if (key_len == 12 && !strcmp (key, "peermsg_type")) {
    struct sublist_descr *SD = get_peer_sublist_type ();
    return_one_key (c, key, stats_buff, sprintf (stats_buff, "%d:%d", SD->and_mask, SD->xor_mask));
    return 0;
  }

  if (key_len >= 8 && !strncmp (key, "sublist", 7)) {
    if (key[7] == 'x') {  // sublistx
      int and_mask, xor_mask, mode, from = 0, to = 0;
      if (sscanf (key, "sublistx%d_%d,%d:%d#%d,%d", &user_id, &mode, &and_mask, &xor_mask, &from, &to) >= 4) {
	int res = get_msg_sublist_ext (user_id, and_mask, xor_mask, mode, from, to);
	if (res == -3) {
	  return_one_key (c, key, "NOT_SUPPORTED", 13);
	  return 0;
	}
	if (res < 0) {
	  return res;
	}
	return_one_key_list (c, key, key_len, res, 0, R, R_cnt);
      }
      return 0;
    }

    if (key[7] == 'p') { // sublistpos
      int and_mask, xor_mask, local_id = -1;
      if (sscanf (key, "sublistpos%d_%d:%d,%d", &user_id, &and_mask, &xor_mask, &local_id) >= 4 && local_id > 0) {
	int res = get_msg_sublist_pos (user_id, and_mask, xor_mask, local_id);
	if (res == -3) {
	  return_one_key (c, key, "NOT_SUPPORTED", 13);
	  return 0;
	}
	if (res < 0) {
	  return res;
	}
	return_one_key (c, key, stats_buff, sprintf (stats_buff, "%d", res));
      }
      return 0;
    }

    // sublist
    int and_mask, xor_mask, from = 0, to = 0;
    if (sscanf (key, "sublist%d_%d:%d#%d,%d", &user_id, &and_mask, &xor_mask, &from, &to) >= 3) {
      int res = get_msg_sublist (user_id, and_mask, xor_mask, from, to);
      if (res == -3) {
        return_one_key (c, key, "NOT_SUPPORTED", 13);
        return 0;
      }
      if (res < 0) {
        return res;
      }
      return_one_key_list (c, key, key_len, res, 0, R, R_cnt);
    }
    return 0;
  }

  if (key_len >= 8 && !strncmp (key, "newmsgid", 8)) {
    int random_tag;
    if (sscanf (key, "newmsgid%d#%d", &user_id, &random_tag) >= 2 && user_id && random_tag > 0) {
      int res = get_local_id_by_random_tag (user_id, random_tag);
      if (res > 0) {
        return_one_key (c, key, stats_buff, sprintf (stats_buff, "%d", res));
      }
    }
    return 0;
  }

  if (key_len >= 17 && !strncmp (key, "convert_legacy_id", 17)) {
    long long legacy_id;
    if (sscanf (key, "convert_legacy_id%d_%lld", &user_id, &legacy_id) >= 2 && user_id && legacy_id) {
      int res = get_local_id_by_legacy_id (user_id, legacy_id);
      if (res < 0) {
        return res;
      }
      if (res > 0) {
        return_one_key (c, key, stats_buff, sprintf (stats_buff, "%d", res));
      }
    }
    return 0;
  }

  if (key_len >= 8 && !strncmp (key, "userdata", 8)) {
    if (sscanf (key, "userdata%d", &user_id) == 1) {
      int res = check_user_metafile (user_id, R);
      if (res < 0) {
        return res;
      }
      if (!res) {
        return_one_key (c, key, "0", 1);
        return 0;
      }
      return_one_key_list (c, key, key_len, res, 0, R, res);
    }
    return 0;
  }

  if (key_len >= 9 && !strncmp (key, "timestamp", 9)) {
    if (sscanf (key, "timestamp%d", &user_id) == 1 && user_id) {
      int res = get_timestamp (user_id, 0);
      if (res > 0) {
        return_one_key (c, key, stats_buff, sprintf (stats_buff, "%d", res));
      }
    }
    return 0;
  }
      
  if (key_len >= 15 && !strncmp (key, "force_timestamp", 15)) {
    if (sscanf (key, "force_timestamp%d", &user_id) == 1 && user_id) {
      int res = get_timestamp (user_id, 1);
      if (res > 0) {
        return_one_key (c, key, stats_buff, sprintf (stats_buff, "%d", res));
      }
    }
    return 0;
  }
      
  if (key_len >= 7 && !strncmp (key, "history", 7)) {
    int timestamp, limit = 0;
    if (sscanf (key, "history%d#%d,%d", &user_id, &timestamp, &limit) >= 2 && user_id && timestamp > 0) {
      int res = get_history (user_id, timestamp, limit, R);
      if (res >= 0) {
	int i;
	for (i = 0; i < res; i++) {
	  if (R[3*i] >= 100) {
	    R[3*i+1] = R[3*i+2] = -1;
	  }
	}
        return_one_key_list (c, key, key_len, res > 0 ? timestamp + res : get_timestamp (user_id, 1), 0, R, 3*res);
      }
    }
    return 0;
  }

  if (key_len >= 8 && !strncmp (key, "xhistory", 8)) {
    int timestamp, limit = 0;
    if (sscanf (key, "xhistory%d#%d,%d", &user_id, &timestamp, &limit) >= 2 && user_id && timestamp > 0) {
      int res = get_history (user_id, timestamp, limit, R);
      if (res >= 0) {
	int istp = get_timestamp (user_id, 1);
	int ilen = sprintf (stats_buff, "%d", istp);
	char *ptr = stats_buff + ilen;
	int i;
	*ptr++ = '\n';
	for (i = 0; i < res && ptr < stats_buff + STATS_BUFF_SIZE - 65536 - 16; i++) {
	  if (R[3*i] < 100) {
	    ptr += sprintf (ptr, "%d\t%d,%d\n", R[3*i], R[3*i+1], R[3*i+2]);
	  } else {
	    ptr += sprintf (ptr, "%d\t%.65535s\n", R[3*i], *(char **)(R + 3*i + 1));
	  }
	}
	int nlen = sprintf (stats_buff, "%d", istp - (res - i));
	assert (nlen <= ilen);
	char *ans = stats_buff + (ilen - nlen);
	if (nlen != ilen) {
	  memmove (ans, stats_buff, nlen);
	}
	ans[nlen] = '\n';
        return_one_key (c, key, ans, ptr - ans);
      }
    }
    return 0;
  }

  if (key_len >= 11 && !strncmp (key, "p_timestamp", 11)) {
    if (sscanf (key, "p_timestamp%d", &user_id) == 1 && user_id) {
      int res = get_persistent_timestamp (user_id);
      if (res > 0) {
        return_one_key (c, key, stats_buff, sprintf (stats_buff, "%d", res));
      }
    }
    return 0;
  }
      
  if (key_len >= 9 && !strncmp (key, "p_history", 9)) {
    int timestamp, limit = 0;
    if (sscanf (key, "p_history%d#%d,%d", &user_id, &timestamp, &limit) >= 2 && user_id && timestamp > 0) {
      int res = get_persistent_history (user_id, timestamp, limit, R);
      if (res == -2) {
	return res;
      }
      if (res >= 0) {
        return_one_key_list (c, key, key_len, timestamp + res, 0, R, 3*res);
      }
    }
    return 0;
  }

  if (key_len >= 7 && !strncmp (key, "secret", 6)) {
    if (sscanf (key, "secret%d", &user_id) == 1 && user_id) {
      char *secret = get_user_secret (user_id);
      if (secret) {
        return_one_key (c, key, secret, 8);
      }
    }
    return 0;
  }

  if (key_len >= 13 && !strncmp (key, "onlinefriends", 13)) {
    int user_id = 0, mode = 0;
    if (sscanf (key, "onlinefriends%d#%d", &user_id, &mode) >= 1 && user_id && (unsigned) mode <= 1) {
      int res = get_online_friends (user_id, mode);
      if (res < 0) {
        return res;
      }
      return_one_key_list (c, key, key_len, res, 0, R, R_cnt);
    }
    return 0;
  }


  if (key_len >= 16 && !strncmp (key, "free_block_stats", 16)) {
    return_one_key_list (c, key, key_len, MAX_RECORD_WORDS, 0, FreeCnt, MAX_RECORD_WORDS);
    return 0;
  }

  if (key_len >= 16 && !strncmp (key, "used_block_stats", 16)) {
    return_one_key_list (c, key, key_len, MAX_RECORD_WORDS, 0, UsedCnt, MAX_RECORD_WORDS);
    return 0;
  }

  if (key_len >= 16 && !strncmp (key, "allocation_stats", 16)) {
    return_one_key_list (c, key, key_len, MAX_RECORD_WORDS, 0, NewAllocations[0], MAX_RECORD_WORDS * 4);
    return 0;
  }

  if (key_len >= 17 && !strncmp (key, "split_block_stats", 17)) {
    return_one_key_list (c, key, key_len, MAX_RECORD_WORDS, 0, SplitBlocks, MAX_RECORD_WORDS);
    return 0;
  }

  if (key_len >= 5 && !strncmp (key, "stats", 5)) {
    int len = text_prepare_stats ();
    int len2 = prepare_stats (c, stats_buff + len, STATS_BUFF_SIZE - len);
    return_one_key (c, key, stats_buff, len + len2);
    return 0;
  }

  return 0;
}

int memcache_get (struct connection *c, const char *key, int key_len) {
  if (verbosity > 1) {
    fprintf (stderr, "memcache_get: key='%s'\n", key);
  }
  WaitAioArrClear ();

  //WaitAio = WaitAio2 = WaitAio3 = 0;

  int res = memcache_get_nonblock (c, key, key_len);

  if (res == -2) {
    return memcache_wait (c, key, key_len);
  }

  return 0;
}


int memcache_get_start (struct connection *c) {
  c->flags &= ~C_INTIMEOUT;
  c->query_start_time = get_utime (CLOCK_MONOTONIC);
  return 0;
}

int memcache_get_end (struct connection *c, int key_count) {
  c->flags &= ~C_INTIMEOUT;
  c->last_query_time = get_utime (CLOCK_MONOTONIC) - c->query_start_time;
  total_get_time += c->last_query_time;
  get_queries++;
  write_out (&c->Out, "END\r\n", 5);
  if (verbosity > 1) {
    fprintf (stderr, "memcache_get end: query time %.3fms\n", c->last_query_time * 1000);
  }
  return 0;
}



int memcache_incr (struct connection *c, int op, const char *key, int key_len, long long arg) {
  int user_id, local_id, res;
  if (verbosity > 1) {
    fprintf (stderr, "memcache_incr: op=%d, key='%s', val=%lld\n", op, key, arg);
  }

  free_tmp_buffers (c);

  if (sscanf (key, "flags%d_%d", &user_id, &local_id) >= 2 && user_id && local_id > 0) {
    if (op) {
      res = do_decr_flags (user_id, local_id, arg);
    } else {
      res = do_incr_flags (user_id, local_id, arg);
    }
    if (res == -2) {
      write_out (&c->Out, "4294967295\r\n", 12);
      return 0;
    } else if (res >= 0) {
      write_out (&c->Out, stats_buff, sprintf (stats_buff, "%d\r\n", res));
      return 0;
    }
  }

  if (key_len >= 5 && !strncmp (key, "extra", 5)) {
    int local_id, value_id;
    if (sscanf (key, "extra%d_%d:%d", &user_id, &local_id, &value_id) == 3 && user_id && local_id > 0 && (unsigned) value_id < 8) {
      res = do_incr_value (user_id, local_id, value_id, op ? -arg : arg);
      if (res <= 0) {
        write_out (&c->Out, "NOT_FOUND\r\n", 11);
        return 0;
      } else {
        long long nres;
        res = get_message_value (user_id, local_id, value_id, -1, &nres);
        if (res == -2) {
          write_out (&c->Out, "-2147483648\r\n", 13);
          return 0;
        } else if (res <= 0) {
          write_out (&c->Out, "NOT_FOUND\r\n", 11);
          return 0;
        }
        write_out (&c->Out, stats_buff, sprintf (stats_buff, "%d\r\n", (int) nres));
        return 0;
      }
    }
  }

  if (key_len >= 5 && !strncmp (key, "Extra", 5)) {
    int local_id, value_id;
    if (sscanf (key, "Extra%d_%d:%d", &user_id, &local_id, &value_id) == 3 && user_id && local_id > 0 && value_id >= 8 && value_id < 12) {
      res = do_incr_value_long (user_id, local_id, value_id, op ? -arg : arg);
      if (res <= 0) {
        write_out (&c->Out, "NOT_FOUND\r\n", 11);
        return 0;
      } else {
        long long nres;
        res = get_message_value (user_id, local_id, value_id, -1, &nres);
        if (res == -2) {
          write_out (&c->Out, "-9223372036854775808\r\n", 22);
          return 0;
        } else if (res <= 0) {
          write_out (&c->Out, "NOT_FOUND\r\n", 11);
          return 0;
        }
        write_out (&c->Out, stats_buff, sprintf (stats_buff, "%lld\r\n", nres));
        return 0;
      }
    }
  }

  write_out (&c->Out, "NOT_FOUND\r\n", 11);
  return 0;
}

int memcache_delete (struct connection *c, const char *key, int key_len) {
  int user_id, local_id;
  if (verbosity > 1) {
    fprintf (stderr, "memcache_delete: key='%s'\n", key);
  }

  free_tmp_buffers (c);

  if (key_len >= 8 && !strncmp (key, "userdata", 8)) {
    if (sscanf (key, "userdata%d", &user_id) == 1) {
      if (unload_user_metafile (user_id) > 0) {
 	write_out (&c->Out, "DELETED\r\n", 9);
	return 0;
      } else {
 	write_out (&c->Out, "NOT_FOUND\r\n", 11);
	return 0;
      }
    }
  }

  if (sscanf (key, "message%d_%d", &user_id, &local_id) == 2 && user_id && local_id > 0) {
    if (do_delete_message (user_id, local_id) == 1) {
      write_out (&c->Out, "DELETED\r\n", 9);
      return 0;
    } else {
      write_out (&c->Out, "NOT_FOUND\r\n", 11);
      return 0;
    }
  }

  if (sscanf (key, "first_messages%d_%d", &user_id, &local_id) == 2 && user_id && local_id > 0) {
    if (do_delete_first_messages (user_id, local_id) == 1) {
      write_out (&c->Out, "DELETED\r\n", 9);
      return 0;
    } else {
      write_out (&c->Out, "NOT_FOUND\r\n", 11);
      return 0;
    }
  }

  if (sscanf (key, "secret%d", &user_id) == 1 && user_id) { 
    if (set_user_secret (user_id, 0) == 1) {
      write_out (&c->Out, "DELETED\r\n", 9);
      return 0;
    }
  }

  write_out (&c->Out, "NOT_FOUND\r\n", 11);
  return 0;
}


int text_engine_wakeup (struct connection *c) {
  struct mcs_data *D = MCS_DATA(c);

  if (verbosity > 1) {
    fprintf (stderr, "text_engine_wakeup (%p)\n", c);
  }

  c->generation = ++conn_generation;
  c->pending_queries = 0;
  c->status = conn_reading_query;
  assert (D->query_type == mct_get_resume || D->query_type == mct_replace_resume);
  clear_connection_timeout (c);

  if (c->Out.total_bytes > 0) {
    c->flags |= C_WANTWR;
  }

  return 0;
}


int text_engine_alarm (struct connection *c) {
  if (verbosity > 1) {
    fprintf (stderr, "text_engine connection %d timeout alarm, %d queries pending, status=%d\n", c->fd, c->pending_queries, c->status);
  }
  assert (c->status == conn_wait_aio);
  c->flags |= C_INTIMEOUT;
  return text_engine_wakeup (c);
}

/*
 *
 *	"HISTORY EVENTS"
 *
 */

int hts_wakeup (struct connection *c);
int delete_history_query (struct conn_query *q);

struct conn_query_functions history_cq_func = {
.magic = CQUERY_FUNC_MAGIC,
.title = "text-engine-http-history-query",
.wakeup = delete_history_query, // history_query_timeout
.close = delete_history_query,
.complete = delete_history_query
};

int create_history_query (user_t *U, struct connection *c, double timeout) {
  struct conn_query *Q = zmalloc (sizeof (struct conn_query));

  if (verbosity > 1) {
    fprintf (stderr, "create_history_query(%p[%d], %p[%d]): Q=%p\n", U, U->user_id, c, c->fd, Q);
  }

  Q->custom_type = 0;
  Q->outbound = (struct connection *) U;
  Q->requester = c;
  Q->start_time = /*c->query_start_time*/get_utime (CLOCK_MONOTONIC);
  Q->extra = 0;
  Q->cq_type = &history_cq_func;
  Q->timer.wakeup_time = (timeout > 0 ? Q->start_time + timeout : 0);

  pending_http_queries++;
  insert_conn_query (Q);

  if (verbosity > 1) {
    fprintf (stderr, "after insert_conn_query()\n");
  }

  return 1;
}

int delete_history_query (struct conn_query *q) {
  if (verbosity > 1) {
    fprintf (stderr, "delete_history_query(%p,%p)\n", q, q->requester);
  }

  pending_http_queries--;
  delete_conn_query (q);
  zfree (q, sizeof (*q));
  return 0;
}

int create_persistent_history_query (user_t *U, struct connection *c, double timeout) {
  struct conn_query *Q = zmalloc (sizeof (struct conn_query));

  if (verbosity > 1) {
    fprintf (stderr, "create_persistent_history_query(%p[%d], %p[%d]): Q=%p\n", U, U->user_id, c, c->fd, Q);
  }

  Q->custom_type = 0;
  Q->outbound = (struct connection *) USER_PCONN (U);
  Q->requester = c;
  Q->start_time = /*c->query_start_time*/get_utime (CLOCK_MONOTONIC);
  Q->extra = 0;
  Q->cq_type = &history_cq_func;
  Q->timer.wakeup_time = (timeout > 0 ? Q->start_time + timeout : 0);

  pending_http_queries++;
  insert_conn_query (Q);

  if (verbosity > 1) {
    fprintf (stderr, "after insert_conn_query()\n");
  }

  return 1;
}





/*
 *
 *	HTTP INTERFACE
 *
 */

struct http_server_functions http_methods = {
  .execute = hts_execute,
  .ht_wakeup = hts_wakeup,
  .ht_alarm = hts_wakeup
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

static char key_buff[64], ip_buff[64];
int req_ts, req_mode;
int key_user_id, key_time;


char im_secret1 [] = "0123456789ABCDEF";  // replace with appropriate secret values
char im_secret2 [] = "0123456789ABCDEF";
char im_secret3 [] = "0123456789ABCDEF";
char im_secret4 [] = "0123456789ABCDEF";

int normalize_ip_buff (void) {
  static char aux_buff1[64], aux_buff2[64];
  char *ip_src = ip_buff;
  int i, j, l = strlen (ip_src);
  int sc = 0;
  for (i = 0; i < l; i++) {
    if (ip_src[i] == ':') ++sc;
  }
  if (sc < 2 || sc > 7) {
    if (!sc) {
      char *ptr = ip_buff;
      int dots = 0;

      while (*ptr) {
	if (*ptr++ == '.') {
	  if (++dots == 3) {
	    break;
	  }
	}
      }
      *ptr = 0;
      if (dots < 3 || ptr - ip_buff > 12) {
	return -1;
      }
      return 0;
    }
    return -1;
  }
  if (l >= 2 && *ip_src == ':' && ip_src[1] == ':') {
    ++ip_src;
    --l;
    --sc;
  } else if (l >= 2 && ip_src[l - 2] == ':' && ip_src[l - 1] == ':') {
    --l;
    --sc;
    ip_src[l] = 0;
  }
  char *dest = aux_buff1;
  memcpy (aux_buff2, ip_src, l + 1);
  
  char *src = 0, *nx = aux_buff2;
  int cc = 0, fl = 0;
  while (nx) {
    src = nx;
    while (*nx && *nx != ':') {
      ++nx;
    }
    int ll = nx - src;
    if (*nx) {
      *nx = 0;
      ++nx;
    } else {
      nx = 0;
    }
    if (ll > 4) {
      return -1;
    }
    if (!ll) {
      if (fl) {
	return -1;
      }
      ++fl;
      for (i = 0; i < 8 - sc; i++) {
	for (j = 0; j < 4; j++) {
	  *dest++ = '0';
	}
	++cc;
	if (cc < 8) {
	  *dest++ = ':';
	}
      }
    } else {
      for (j = 0; j < 4 - ll; j++) {
	*dest++ = '0';
      }
      for (j = 0; j < ll; j++) {
	char c = tolower (src[j]);
	if (!((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f'))) {
	  return -1;
	}
	*dest++ = c;
      }
      ++cc;
      if (cc < 8) {
	*dest++ = ':';
      }
    }
  }
  *dest = 0;
  assert (cc == 8 && dest == aux_buff1 + 39);
  memcpy (ip_buff, aux_buff1, 40);
  return 0;
}

/*
void md5_hex (char *what, int len, char *where) {
  static unsigned char mdbuff[16];

  md5 ((unsigned char *)what, len, mdbuff);

  int i;

  for (i = 0; i < 16; i++) {
    where += sprintf (where, "%02x", (unsigned)mdbuff[i]);
  }
}
*/

inline int char_to_hex (int c) {
  if (c <= '9') {
    return c - '0';
  } else {
    return c - 'a' + 10;
  }
}

inline int hex_to_char (int c) {
  if (c < 10) {
    return c + '0';
  } else {
    return c - 10 + 'a';
  }
}

void xor_str (char *str1, const char *str2, int digits) {
  int i;
  for (i = 0; i < digits; i++) {
    str1[i] = hex_to_char (char_to_hex (str1[i]) ^ char_to_hex (*str2++));
  }
}


int parse16 (const char *from, int len) {
  const char *from_end = from + len;
  int res = 0;
  while (from < from_end) {
    res = res * 16 + char_to_hex (*from++);
  }
  return res;
}


int validate_key (void) {
/*
  $subnet = $inc->extractSubnetwork($_SERVER['HTTP_X_REAL_IP']);
  $nonce = sprintf ("%08x", mt_rand (0, 0x7fffffff));
  $hlam1 = substr (md5($nonce.$im_secret1.$subnet), 4, 8);
  $utime = sprintf ("%08x", time());
  $utime_xor = $inc->xor_str ($utime, $hlam1);
  $uid = sprintf ("%08x", $id);
  $uid_xor = $inc->xor_str ($uid, substr (md5($nonce.$subnet.$utime_xor.$im_secret2), 6, 8));
  $check = substr (md5($utime.$uid.$nonce.$im_secret4.$subnet.$user_secret), 12, 16);

  $im_session = $nonce.$uid_xor.$check.$utime_xor;
*/
  int i;
  static char buf1[256], buf2[256];
  
  if (normalize_ip_buff () < 0) {
    return -1;
  }

  if (verbosity > 1) {
    fprintf (stderr, "validate_key (%s, %s)\n", key_buff, ip_buff);
  }

  for (i = 0; i < 40; i++) {
    if (!((key_buff[i] >= '0' && key_buff[i] <= '9') ||
          (key_buff[i] >= 'a' && key_buff[i] <= 'f'))) {
      return -1;
    }
  }

  int subnet_len = strlen (ip_buff);

  memcpy (buf1, key_buff, 8);
  memcpy (buf1 + 8, im_secret1, 16);
  memcpy (buf1 + 24, ip_buff, subnet_len);

  md5_hex (buf1, 24 + subnet_len, buf2);

  xor_str (buf2 + 4, key_buff + 32, 8);

  key_time = parse16 (buf2 + 4, 8);

  memcpy (buf1 + 8, ip_buff, subnet_len);
  memcpy (buf1 + 8 + subnet_len, key_buff + 32, 8);
  memcpy (buf1 + 16 + subnet_len, im_secret2, 16);

  md5_hex (buf1, 32 + subnet_len, buf2);

  xor_str (buf2 + 6, key_buff + 8, 8);
  key_user_id = parse16 (buf2 + 6, 8);

  char *secret = get_user_secret (key_user_id);
  if (!secret) {
    return -1;
  }

  int ll = sprintf (buf1, "%08x%08x%.8s%s%.*s", key_time, key_user_id, key_buff, im_secret4, subnet_len, ip_buff);
  memcpy (buf1 + ll, secret, 8);
  md5_hex (buf1, ll + 8, buf2);


  if (memcmp (key_buff + 16, buf2 + 12, 16)) {
    if (verbosity > 1) {
      fprintf (stderr, "FAILED: uid = %d, utime = %d, subnet = %.*s\n", key_user_id, key_time, subnet_len, ip_buff);
    }
    return -1;
  } else {
    //fprintf (stderr, "uid = %d, utime = %d, subnet = %.*s\n", key_user_id, key_time, subnet_len, ip_buff);
    if (key_user_id > 0 && key_time > now - 10800 && key_time < now + 120) {
      return key_user_id;
    } else {
      return -1;
    }
  }
}

static char no_cache_headers[] =
"Pragma: no-cache\r\n"
"Cache-Control: no-store\r\n";

#define	HTTP_RESULT_SIZE	(4 << 20)

static char http_body_buffer[HTTP_RESULT_SIZE], *http_w;
#define	http_w_end	(http_body_buffer + HTTP_RESULT_SIZE - 16384)

void http_return (struct connection *c, const char *str, int len) {
  if (len < 0) {
    len = strlen (str);
  }
  write_basic_http_header (c, 200, 0, len, no_cache_headers, "text/javascript; charset=UTF-8");
  write_out (&c->Out, str, len);
}

int cp1251_unicode_codes[128] = {
  0x402  ,
  0x403  ,
  0x201a ,
  0x453  ,
  0x201e ,
  0x2026 ,
  0x2020 ,
  0x2021 ,
  0x20ac ,
  0x2030 ,
  0x409  ,
  0x2039 ,
  0x40a  ,
  0x40c  ,
  0x40b  ,
  0x40f  ,
  0x452  ,
  0x2018 ,
  0x2019 ,
  0x201c ,
  0x201d ,
  0x2022 ,
  0x2013 ,
  0x2014 ,
  0xfffd ,
  0x2122 ,
  0x459  ,
  0x203a ,
  0x45a  ,
  0x45c  ,
  0x45b  ,
  0x45f  ,
  0xa0   ,
  0x40e  ,
  0x45e  ,
  0x408  ,
  0xa4   ,
  0x490  ,
  0xa6   ,
  0xa7   ,
  0x401  ,
  0xa9   ,
  0x404  ,
  0xab   ,
  0xac   ,
  0xad   ,
  0xae   ,
  0x407  ,
  0xb0   ,
  0xb1   ,
  0x406  ,
  0x456  ,
  0x491  ,
  0xb5   ,
  0xb6   ,
  0xb7   ,
  0x451  ,
  0x2116 ,
  0x454  ,
  0xbb   ,
  0x458  ,
  0x405  ,
  0x455  ,
  0x457  ,
  0x410  ,
  0x411  ,
  0x412  ,
  0x413  ,
  0x414  ,
  0x415  ,
  0x416  ,
  0x417  ,
  0x418  ,
  0x419  ,
  0x41a  ,
  0x41b  ,
  0x41c  ,
  0x41d  ,
  0x41e  ,
  0x41f  ,
  0x420  ,
  0x421  ,
  0x422  ,
  0x423  ,
  0x424  ,
  0x425  ,
  0x426  ,
  0x427  ,
  0x428  ,
  0x429  ,
  0x42a  ,
  0x42b  ,
  0x42c  ,
  0x42d  ,
  0x42e  ,
  0x42f  ,
  0x430  ,
  0x431  ,
  0x432  ,
  0x433  ,
  0x434  ,
  0x435  ,
  0x436  ,
  0x437  ,
  0x438  ,
  0x439  ,
  0x43a  ,
  0x43b  ,
  0x43c  ,
  0x43d  ,
  0x43e  ,
  0x43f  ,
  0x440  ,
  0x441  ,
  0x442  ,
  0x443  ,
  0x444  ,
  0x445  ,
  0x446  ,
  0x447  ,
  0x448  ,
  0x449  ,
  0x44a  ,
  0x44b  ,
  0x44c  ,
  0x44d  ,
  0x44e  ,
  0x44f  
};

static inline char *utf8_char (char *ptr, unsigned c) {
  if (c < 0x80) {
    *ptr = c;
    return ptr + 1;
  }
  if (c < 0x800) {
    ptr[0] = 0xc0 + (c >> 6);
    ptr[1] = 0x80 + (c & 0x3f);
    return ptr + 2;
  }
  if (c < 0x10000) {
    ptr[0] = 0xe0 + (c >> 12);
    ptr[1] = 0x80 + ((c >> 6) & 0x3f);
    ptr[2] = 0x80 + (c & 0x3f);
    return ptr + 3;
  }
  if (c < 0x200000) {
    ptr[0] = 0xf0 + (c >> 18);
    ptr[1] = 0x80 + ((c >> 12) & 0x3f);
    ptr[2] = 0x80 + ((c >> 6) & 0x3f);
    ptr[3] = 0x80 + (c & 0x3f);
    return ptr + 4;
  }
  return ptr;
}

int utf8_json_encode (char *to, int to_size, char *from, int from_size) {
  char *to_end = to + to_size - 8;
  char *from_end = from + from_size;
  char *wptr = to;
  int c, uc;

  if ((unsigned) from_size > 0xffffff || to_size <= 8) {
    fprintf (stderr, "utf8_json_encode: invalid initial data\n");
    return -1;
  }

  if (utf8_mode) {
    while (from < from_end) {
      if (wptr > to_end) {
	fprintf (stderr, "utf8_json_encode: buffer overflow\n");
	return -1;
      }
      c = *from++;
      switch (c) {
      case '"': case '\\': case '/':
	*wptr++ = '\\';
	break;
      case '\n':
	*wptr++ = '\\';
	c = 'n';
	break;
      case '\r':
	*wptr++ = '\\';
	c = 'r';
	break;
      case '\t':
	*wptr++ = '\\';
	c = 't';
	break;
      case '\b':
	*wptr++ = '\\';
	c = 'b';
	break;
      case '\f':
	*wptr++ = '\\';
	c = 'f';
	break;
      }
      if (!(c & -32)) {
	wptr += sprintf (wptr, "\\u%04x", c);
      } else {
	*wptr++ = c;
      }
    }
    return wptr - to;
  }

  while (from < from_end) {
    if (wptr > to_end) {
      fprintf (stderr, "utf8_json_encode: buffer overflow\n");
      return -1;
    }
    c = *from++;
    if (c < 0) {
      uc = cp1251_unicode_codes[c+128];
      wptr = utf8_char (wptr, uc);
      continue;
    }
    if (c == '&') {
      char *ptr = from;
      if (from == from_end) {
        return wptr - to;
      }
      if (ptr[0] == '#') {
        ptr++;
        uc = 0;
        while (ptr < from_end && ptr < from + 8 && *ptr >= '0' && *ptr <= '9') {
          uc = uc * 10 + (*ptr++ - '0');
        }
        if (ptr == from_end) {
          return wptr - to;
        }
        if (*ptr == ';' && uc > 0 && uc != '<' && uc != '>') {
          from = ptr + 1;
          c = uc;
        }
      } else if (ptr[0] >= 'a' && ptr[0] <= 'z') {
        while (ptr < from_end && ptr < from + 16 && *ptr >= 'a' && *ptr <= 'z') {
          ptr++;
        }
        if (ptr == from_end) {
          return wptr - to;
        }
      }
    }
    switch (c) {
    case '"': case '\\': case '/':
      *wptr++ = '\\';
      break;
    case '\n':
      *wptr++ = '\\';
      c = 'n';
      break;
    case '\r':
      *wptr++ = '\\';
      c = 'r';
      break;
    case '\t':
      *wptr++ = '\\';
      c = 't';
      break;
    case '\b':
      *wptr++ = '\\';
      c = 'b';
      break;
    case '\f':
      *wptr++ = '\\';
      c = 'f';
      break;
    }
    if (!(c & -32)) {
      wptr += sprintf (wptr, "\\u%04x", c);
    } else {
      wptr = utf8_char (wptr, c);
    }
  }

  return wptr - to;
}

int kludges_json_array_encode (char *to, int to_size, char *from, int from_size, int flags) {
  char *from_end = from + from_size, *to_end = to + to_size - 8, *to_start = to;
  int out_n = 0, len;

  if (to_size < 8 || from_size < 0) {
    return -1;
  }

  *to++ = '{';

  while (from < from_end && to < to_end) {
    if (*from != 1 && *from != 2) {
      break;
    }
    char *p = from, *q = 0;
    while (p < from_end && *p != '\t') {
      if (*p == ' ' && !q) {
	q = p;
      }
      p++;
    }
    if (!(flags & *from) || !q) {
      from = p + 1;
      continue;
    }
    *to++ = '"';
    len = utf8_json_encode (to, to_end - to, from + 1, q - from - 1);
    if (len < 0) {
      return len;
    }
    to += len;
    if (to > to_end) {
      return -1;
    }
    to[0] = '"';
    to[1] = ':';
    to[2] = '"';
    to += 3;
    len = utf8_json_encode (to, to_end - to, q + 1, p - q - 1);
    if (len < 0) {
      return len;
    }
    to += len;
    if (to > to_end) {
      return -1;
    }
    *to++ = '"';
    *to++ = ',';
    out_n++;
    from = p + 1;
  }

  if (out_n) {
    --to;
  }
  *to++ = '}';
  assert (to <= to_end + 8);
  return to - to_start;
}
      

int http_return_history (struct connection *c, int *R, int entries, int new_ts) {
  int i;

  assert (entries >= 0 && entries <= 256 && new_ts > 0);
  http_w = http_body_buffer;

  http_w += sprintf (http_w, "{\"ts\":%d,", new_ts);
  if ((req_mode & 48) == 32) {
    int pts = get_persistent_timestamp (key_user_id);
    if (pts > 0) {
      http_w += sprintf (http_w, "\"pts\":%d,", pts);
    }
  }
  http_w += sprintf (http_w, "\"updates\":[");
  for (i = 0; i < entries; i++) {
    if (http_w > http_w_end) {
      fprintf (stderr, "buffer overflow\n");
      return -500;
    }
    if (R[3*i] >= 100) {
      if (!(req_mode & 8)) {
	http_w += sprintf (http_w, "[%d,-1,-1],", R[3*i]);
	continue;
      }
      char *str = *(char **)(R + 3*i + 1);
      int len = *(unsigned short *)(str - 2);
      if (http_w + len > http_w_end) {
	fprintf (stderr, "buffer overflow\n");
	return -500;
      }
      http_w += sprintf (http_w, "[%d,", R[3*i]);
      memcpy (http_w, str, len);
      http_w += len;
      *http_w++ = ']';
      *http_w++ = ',';
      continue;
    }
    http_w += sprintf (http_w, "[%d,%d,%d],", R[3*i], R[3*i+1], R[3*i+2]);

    if (R[3*i] == 8 || R[3*i] == 9 || R[3*i] >= 50) {
      continue;
    }

    struct imessage_long *M = (struct imessage_long *) stats_buff;
    int res = load_message ((struct imessage *) M, key_user_id, R[3*i+1], 0);
    struct message *msg;
    int len;

    if (res != 1) {
      continue;
    }

    M->builtin_msg.user_id = key_user_id;

    if (R[3*i] == 4 || R[3*i] == 5 || (R[3*i] == 3 && (R[3*i+2] & 192)) || (R[3*i] == 1 && !(R[3*i+2] & 192))) {
      len = STATS_BUFF_SIZE - 1024;
    } else {
      len = 0;
    }
    unpack_message_long (M, len, 2);

    msg = M->msg;
    assert (msg);

    http_w -= 2;
    http_w += sprintf (http_w, ",%d", msg->peer_id);

    if (len > 0) {

      int kludges_len = msg->kludges_size > 0 ? msg->kludges_size : 0;
      int msg_len = msg->len - kludges_len;
      char *msg_start = msg->text + text_shift;

      if (M->edit_text) {
	edit_text_t *X = M->edit_text;
	kludges_len = X->kludges_size;
	msg_len = X->len - kludges_len;
	msg_start = X->text;
      }

      char *msg_text = msg_start + kludges_len;
      char *msg_tab = memchr (msg_text, '\t', msg_len);
      if (!msg_tab) {
        fprintf (stderr, "message %d:%d in history doesn't contain a tab separator\n", key_user_id, R[3*i+1]);
        memcpy (http_w, "],", 2);
        http_w += 2;
        continue;
      }

      http_w += sprintf (http_w, ",%d,\"", msg->date);

      len = utf8_json_encode (http_w, http_w_end - http_w, msg_text, msg_tab - msg_text);
      if (len < 0) {
        fprintf (stderr, "utf8_json_encode returned -500\n");
        return -500;
      }

      memcpy (http_w += len, "\",\"", 3);
      http_w += 3;

      len = utf8_json_encode (http_w, http_w_end - http_w, msg_tab + 1, (msg_text + msg_len) - msg_tab - 1);
      if (len < 0) {
        fprintf (stderr, "utf8_json_encode returned -500\n");
        return -500;
      }

      if ((req_mode & 7) == 1) {
	memcpy (http_w += len, "\",\"", 3);
	http_w += 3;

	len = utf8_json_encode (http_w, http_w_end - http_w, msg_start, msg_text - msg_start);
	if (len < 0) {
	  fprintf (stderr, "utf8_json_encode returned -500\n");
	  return -500;
	}
      }

      if ((req_mode & 6) != 0 && !(req_mode & 1)) {
	memcpy (http_w += len, "\",", 2);
	http_w += 2;

	len = kludges_json_array_encode (http_w, http_w_end - http_w, msg_start, msg_text - msg_start, (req_mode & 6) >> 1);
	if (len < 0) {
	  fprintf (stderr, "kludges_json_array_encode returned -500\n");
	  return -500;
	}
	http_w += len;

      } else {
	http_w += len;
	*http_w++ = '"';
      }
    }
    *http_w++ = ']';
    *http_w++ = ',';
  }

  if (entries) {
    http_w--;
  }

  memcpy (http_w, "]}\r\n", 4);
  http_w += 4;

  assert (http_w >= http_body_buffer && http_w <= http_body_buffer + HTTP_RESULT_SIZE);

  http_failed[0]++;

  http_return (c, http_body_buffer, http_w - http_body_buffer);

  return 0;
}

#define	MAX_POST_SIZE	4096

int hts_execute (struct connection *c, int op) {
  struct hts_data *D = HTS_DATA(c);
  static char ReqHdr[MAX_HTTP_HEADER_SIZE];
  static char Post[MAX_POST_SIZE];
  static char tmp_buff[4096];
  int Post_len = 0;

  int wait_sec, len;

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

  assert (D->header_size <= MAX_HTTP_HEADER_SIZE);
  assert (read_in (&c->In, &ReqHdr, D->header_size) == D->header_size);

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
      return -500;
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


  if (qUriLen >= 20) {
    return -418;
  }

  if (qUriLen >= 3 && !memcmp (qUri, "/im", 3)) {
    key_buff[0] = '?';
    
    getArg (tmp_buff, sizeof (tmp_buff), "act");
    if (strcmp (tmp_buff, "a_check") || getArg (key_buff, 64, "key") != 40 || get_http_header (qHeaders, qHeadersLen, ip_buff, 64, "X-REAL-IP", 9) < 7) {
      return -204;
    }
    if (getArg (tmp_buff, sizeof (tmp_buff), "ts") > 0) {
      req_ts = atoi (tmp_buff);
    } else {
      req_ts = 0;
    }
    if (getArg (tmp_buff, sizeof (tmp_buff), "mode") > 0) {
      req_mode = atoi (tmp_buff);
    } else {
      req_mode = 0;
    }
    if (getArg (tmp_buff, sizeof (tmp_buff), "wait") > 0) {
      wait_sec = atoi (tmp_buff);
      if (wait_sec < 0) {
	wait_sec = 0;
      }
      if (wait_sec > 120) {
	wait_sec = 120;
      }
    } else {
      wait_sec = 0;
    }
    if (validate_key() < 0) {
      if (verbosity > 1) {
	fprintf (stderr, "key %s validation failed, code=2\n", key_buff);
      }
      http_failed[2]++;
      http_return (c, "{\"failed\":2}\r\n", -1);
      return 0;
    } else {
      int res, new_ts;
      if (req_mode & 16) {
	res = get_persistent_history (key_user_id, req_ts, 0, R);
	new_ts = get_persistent_timestamp (key_user_id);
      } else {
	res = get_history (key_user_id, req_ts, 0, R);
	new_ts = get_timestamp (key_user_id, 1);
      }
      if (new_ts < 0) {
	if (verbosity > 1) {
	  fprintf (stderr, "failed with code 3 for user %d, ts %d: new_ts=%d < 0\n", key_user_id, req_ts, new_ts);
	}
	http_failed[3]++;
        http_return (c, "{\"failed\":3}\r\n", -1);
        return 0;
      }
      if (res < 0) {
	if (verbosity > 1) {
	  fprintf (stderr, "failed with code 1 for user %d, ts %d, new_ts=%d, get_history=%d < 0\n", key_user_id, req_ts, new_ts, res);
	}
	http_failed[1]++;
        len = sprintf (tmp_buff, "{\"failed\":1,\"ts\":%d}\r\n", new_ts);
        http_return (c, tmp_buff, len);
        return 0;
      }
      if (verbosity > 1) {
	fprintf (stderr, "connection %d: user %d, ts %d, new_ts %d, get_history=%d\n", c->fd, key_user_id, req_ts, new_ts, res);
      }
      if (res > 0 || wait_sec <= 0) {
        http_queries_ok++;
	return http_return_history (c, R, res, new_ts);
      }

      c->generation = ++conn_generation;
      c->pending_queries = 0;
      D->extra_int = key_user_id;
      D->extra_int2 = req_ts;
      D->extra_int3 = req_mode;
      if (req_mode & 16) {
	create_persistent_history_query (get_user (key_user_id), c, wait_sec);
      } else {
	create_history_query (get_user (key_user_id), c, wait_sec);
      }
      c->status = conn_wait_net;
      set_connection_timeout (c, wait_sec + 1.0);

      http_queries_delayed++;

      key_user_id = req_ts = 0;

      return 0;
    }
  }

  return -404;
}

int hts_wakeup (struct connection *c) {
  struct hts_data *D = HTS_DATA(c);
  static char tmp_buff[4096];
  int res, len, new_ts;

  key_user_id = D->extra_int;
  req_ts = D->extra_int2;
  req_mode = D->extra_int3;

  assert (c->status == conn_expect_query || c->status == conn_wait_net);
  c->status = conn_expect_query;
  clear_connection_timeout (c);

  if (!(D->query_flags & QF_KEEPALIVE)) {
    c->status = conn_write_close;
    c->parse_state = -1;
  }

  if (req_mode & 16) {
    res = get_persistent_history (key_user_id, req_ts, 0, R);
    new_ts = get_persistent_timestamp (key_user_id);
  } else {
    res = get_history (key_user_id, req_ts, 0, R);
    new_ts = get_timestamp (key_user_id, 1);
  }

  if (new_ts < 0) {
    http_failed[3]++;
    http_return (c, "{\"failed\":3}\r\n", -1);
    return 0;
  }

  if (res < 0) {
    http_failed[1]++;
    len = sprintf (tmp_buff, "{\"failed\":1,\"ts\":%d}\r\n", new_ts);
    http_return (c, tmp_buff, len);
    return 0;
  }
  res = http_return_history (c, R, res, new_ts);

  if (res < 0) {
    write_http_error (c, -res);
  }

  return 0;
}
/*
conn_query_type_t tl_aio_metafile_query_type = {
  .magic = CQUERY_FUNC_MAGIC,
  .title = "text-aio-metafile-query",
  .wakeup = aio_query_timeout,
  .close = tl_aio_on_end_default,
  .complete = tl_aio_on_end_default
};

int tl_memcache_wait (struct tl_saved_query *q) {
  assert (q->restart);
  q->attempt ++;

  tl_aio_start (WaitAioArr, WaitAioArrPos, 2.7, q);
  return 0;
  if (!WaitAio && !WaitAio2 && !WaitAio3) {
    fprintf (stderr, "WaitAio=0 - no memory to load user metafile, query dropped.\n");
    return -1;
  }
  vkprintf (1, "Creating aio for rpc\n");

  if (WaitAio) {
    tl_create_aio_query (WaitAio, &Connections[0], 0.5, &tl_aio_metafile_query_type, q);
    (*(int *)(q->extra)) ++;
  }

  if (WaitAio2) {
    tl_create_aio_query (WaitAio2, &Connections[0], 0.5, &tl_aio_metafile_query_type, q);
    (*(int *)(q->extra)) ++;
  }

  if (WaitAio3) {
    tl_create_aio_query (WaitAio3, &Connections[0], 0.5, &tl_aio_metafile_query_type, q);
    (*(int *)(q->extra)) ++;
  }
  vkprintf (2, "WaitAio: ref_cnt = %d\n", *(int *)(q->extra));
  //conn_schedule_aio (WaitAio, Connections[0], 0.5, &rpc_metafile_query_type);
  return 0;
}*/

int tl_parse_uid (void) {
  int uid = tl_fetch_int ();
  if (!uid || conv_uid (uid) < 0) {
    tl_fetch_set_error_format (TL_ERROR_BAD_VALUE, "Wrong server: user_id = %d, log_split_mod = %d, log_split_min = %d, log_split_max = %d", uid, log_split_mod, log_split_min, log_split_max);
    return -1;
  }
  return uid;
}

int tl_parse_local_id (void) {
  int local_id = tl_fetch_int ();
  if (local_id <= 0) {
    tl_fetch_set_error_format (TL_ERROR_BAD_VALUE, "Local id must be positive integer");
    return -1;
  }
  return local_id;
}

int tl_parse_peer_id (void) {
  int peer_id = tl_fetch_int ();
  if (!peer_id) {
    tl_fetch_set_error_format (TL_ERROR_BAD_VALUE, "Peer id must be non-zero integer");
    return -1;
  }
  return peer_id;
}

TL_DO_FUN(get_message)
  int uid = e->uid;
  int local_id = e->local_id;
  int mode = e->mode;
  int max_len = extra->extra[3];
  if (max_len >= STATS_BUFF_SIZE - 2048) {
    max_len = STATS_BUFF_SIZE - 2048;
  }
  struct imessage_long *M = (struct imessage_long *) (stats_buff + 1024);
  int res = load_message_long (M, uid, local_id, max_len, (mode >> 3));
  if (res < 0) {
    return res;    
  }
  if (!res) {
    tl_store_int (TL_MAYBE_FALSE);
    return 0;
  }
  tl_store_int (TL_MAYBE_TRUE);

  struct message *msg;
  struct message_extras *msg_e;
  
  msg = M->msg;
  assert (msg);
  msg_e = M->m_extra;

  if (mode & (1 << 14)) { tl_store_int (local_id);  }
  if (mode & (1 << 0))  { tl_store_int (msg->global_id); }
  if (mode & (1 << 1))  { tl_store_int (msg->legacy_id); } 
  if (mode & (1 << 2))  { tl_store_int (msg->peer_msg_id); }
  if (mode & (1 << 5))  { tl_store_int (msg->flags); }
  if (mode & (1 << 6))  { tl_store_int (msg->date); }
  if (mode & (1 << 7))  { tl_store_int (msg->peer_id); }
  if (mode & (1 << 8))  { tl_store_int (msg_e ? msg_e->ip : 0); }
  if (mode & (1 << 9))  { tl_store_int (msg_e ? msg_e->port : 0); }
  if (mode & (1 << 10))  { tl_store_int (msg_e ? msg_e->front : 0); }
  if (mode & (1 << 11))  { tl_store_long (msg_e ? msg_e->ua_hash : 0); }
  if (mode & (1 << 15))  { tl_store_long (msg->legacy_id); } 

  int *M_extra = msg->extra, i, mask = (mode >> 16);

  for (i = 1; i < MAX_EXTRA_MASK; i <<= 1) {
    if (read_extra_mask & mask & i) {
      if (i < 256) {
        tl_store_int (*M_extra);
      } else {
        tl_store_long (*(long long*)M_extra);
      }
    } else if (mask & i) {
      if (i < 256) {
        tl_store_int (0);
      } else {
        tl_store_long (0);
      }
    }
    if (index_extra_mask & i) {
      M_extra += (i < 256 ? 1 : 2);
    }
  }

  assert (msg->text + text_shift == (char *) M_extra);
  int msg_len, kludges_len;
  char *start;

  if (!M->edit_text) {
    msg_len = msg->len;
    kludges_len = msg->kludges_size;
    if (kludges_len < 0) {
      kludges_len = 0;
    }
    start = msg->text + text_shift;
  } else {
    edit_text_t *X = M->edit_text;
    msg_len = X->len;
    kludges_len = X->kludges_size;
    start = X->text;
  }

  if (msg_len > max_len) {
    msg_len = max_len;
  }

  if (kludges_len > msg_len) {
    kludges_len = msg_len;
  }
  if (mode & (1 << 3))  { tl_store_string (start, kludges_len); }
  if (mode & (1 << 4)) { tl_store_string (start + kludges_len, msg_len - kludges_len); }
TL_DO_FUN_END

TL_DO_FUN(convert_legacy_id)
  int res = get_local_id_by_legacy_id (e->uid, e->legacy_id);
  if (res < 0) {
    return res;
  }
  if (!res) {
    tl_store_int (TL_MAYBE_FALSE);
    return 0;
  } else {
    tl_store_int (TL_MAYBE_TRUE);
    tl_store_int (res);
    return 0;
  }
TL_DO_FUN_END

TL_DO_FUN(convert_random_id)
  int res = get_local_id_by_legacy_id (e->uid, e->random_id);
  if (res < 0) {
    return res;
  }
  if (!res) {
    tl_store_int (TL_MAYBE_FALSE);
    return 0;
  } else {
    tl_store_int (TL_MAYBE_TRUE);
    tl_store_int (res);
    return 0;
  }
TL_DO_FUN_END

TL_DO_FUN(peer_msg_list)
	int res = get_peer_msglist (e->uid, e->peer_id, e->from, e->to);
  if (res < 0) {
    return res;
  }
  tl_store_int (TL_MAYBE_TRUE);
  tl_store_int (res);
  tl_store_int (R_cnt);
  int i;
  for (i = 0; i < R_cnt; i++) {
    tl_store_int (R[i]);
  }
TL_DO_FUN_END

TL_DO_FUN(peer_msg_list_pos)
	int res = get_peer_msglist_pos (e->uid, e->peer_id, e->local_id);
  if (res < 0) {
    return res;
  }
  if (!res) {
    tl_store_int (TL_MAYBE_FALSE);
  } else {
    tl_store_int (TL_MAYBE_TRUE);
    tl_store_int (res);
  }
TL_DO_FUN_END

TL_DO_FUN(top_msg_list)
  int res = get_top_msglist (e->uid, e->from, e->to);
  if (res < 0) {
    return res;
  }
  tl_store_int (TL_MAYBE_TRUE);
  tl_store_int (res);
  tl_store_int (R_cnt / 2);
  int i;
  for (i = 0; i < R_cnt; i+=2) {
    tl_store_int (R[i + 1]);
    tl_store_int (R[i]);
  }
TL_DO_FUN_END

TL_DO_FUN(sublist)
  int res = get_msg_sublist_ext (e->uid, e->and_mask, e->or_mask, e->mode & ~(1 << 14), e->from, e->to);
  if (res == -3) {
    tl_fetch_set_error_format (TL_ERROR_BAD_VALUE, "Sublist type and_mask=%08x or_mask=%08x unsupported", e->and_mask, e->or_mask);
  }
  if (res < 0) {
    return res;
  }
  tl_store_int (TL_MAYBE_TRUE);
  tl_store_int (res);
  int size = __builtin_popcount (e->mode | (1 << 14));
  tl_store_int (R_cnt / size);
  int i = 0;
  while (i < R_cnt) {
    if (e->mode & (1 << 14)) {
      tl_store_int (R[i ++]);
    } else {
      i ++;
    }
    if (e->mode & 32) {
      tl_store_int (R[i ++]);
    }
    if (e->mode & 64) {
      tl_store_int (R[i ++]);
    }
    if (e->mode & 128) {
      tl_store_int (R[i ++]);
    }
    assert (i % size == 0);
  }
TL_DO_FUN_END

TL_DO_FUN(sublist_pos)
  int res = get_msg_sublist_pos (e->uid, e->and_mask, e->or_mask, e->local_id);
  if (res == -3) {
    tl_fetch_set_error_format (TL_ERROR_BAD_VALUE, "Sublist type and_mask=%08x or_mask=%08x unsupported", e->and_mask, e->or_mask);
  }
  if (res < 0) {
    return res;
  }
  tl_store_int (TL_MAYBE_TRUE);
  tl_store_int (res);
TL_DO_FUN_END

TL_DO_FUN(sublist_types)
  struct sublist_descr SD[16];
  int res = get_sublist_types (SD);
  int i;
  assert (res >= 0);
  tl_store_int (TL_VECTOR);
  tl_store_int (res);
  for (i = 0; i < res; i++) {
    tl_store_int (SD[i].and_mask);
    tl_store_int (SD[i].xor_mask);
  }
TL_DO_FUN_END

TL_DO_FUN(peermsg_type)
  struct sublist_descr *SD = get_peer_sublist_type ();
  tl_store_int (TL (SUBLIST_TYPE));
  tl_store_int (SD->and_mask);
  tl_store_int (SD->xor_mask);
TL_DO_FUN_END

TL_DO_FUN(send_message)
  vkprintf (2, "before do_store_new_message(): flags=%04x, uid=%d, peer=%d, legacy_id=%lld, text_len=%d\n", e->Z.M.type, e->Z.M.user_id, e->Z.M.peer_id, e->legacy_id, e->Z.M.text_len);
  int res = do_store_new_message (&e->Z.M, 0, e->text, e->legacy_id);
  vkprintf (2, "do_store_new_message() = %d\n", res);
  if (res >= 0) {
    tl_store_int (TL_MAYBE_TRUE);
    tl_store_int (res);
  } else {
    tl_store_int (TL_MAYBE_FALSE);
  }
TL_DO_FUN_END

TL_DO_FUN(delete_message)
  int res = do_delete_message (e->uid, e->local_id);
  if (res < 0) {
    return res;
  }

  tl_store_int (res <= 0 ? TL_BOOL_FALSE : TL_BOOL_TRUE);
TL_DO_FUN_END

TL_DO_FUN(delete_first_messages)
  int res = do_delete_first_messages (e->uid, e->min_local_id);
  if (res < 0) {
    return res;
  }

  tl_store_int (res <= 0 ? TL_BOOL_FALSE : TL_BOOL_TRUE);
TL_DO_FUN_END

TL_DO_FUN(get_flags)
  int res = get_message_flags (e->uid, e->local_id, 1);
  if (res < 0) {
    return res;
  }

  tl_store_int (TL_MAYBE_TRUE);
  tl_store_int (res);
TL_DO_FUN_END

TL_DO_FUN(set_flags)
  int res = do_set_flags (e->uid, e->local_id, e->flags);
  if (res < 0) {
    return res;
  }

  tl_store_int (res <= 0 ? TL_BOOL_FALSE : TL_BOOL_TRUE);
TL_DO_FUN_END

TL_DO_FUN(incr_flags)
  int res = (e->decr ? do_decr_flags : do_incr_flags) (e->uid, e->local_id, e->flags);
  
  if (res < 0 && res != -2) {
    return res;
  }

  if (res == -2) {
    tl_store_int (TL_MAYBE_TRUE);
    tl_store_int (0x7fffffff);
  } else if (res <= 0) {  
    tl_store_int (TL_MAYBE_FALSE);
  } else {
    tl_store_int (TL_MAYBE_TRUE);
    tl_store_int (res);
  }
TL_DO_FUN_END

TL_DO_FUN(get_extra)
  long long nres;
  int res = get_message_value (e->uid, e->local_id, e->k, 1, &nres);
  if (res < 0) {
    return res;
  }

  vkprintf (2, "tl_do_get_extra: uid:%d, loacl_id:%d, k:%d, result %lld\n", e->uid, e->local_id, e->k, nres);
  tl_store_int (TL_MAYBE_TRUE);
  tl_store_int (nres);
TL_DO_FUN_END

TL_DO_FUN(set_extra)
  struct value_data V;
  V.data[0] = e->value;
  V.fields_mask = V.zero_mask = (1 << e->k);
  int res = do_set_values (e->uid, e->local_id, &V);
  if (res < 0) {
    return res;
  }
  tl_store_int (res <= 0 ? TL_BOOL_FALSE : TL_BOOL_TRUE);
TL_DO_FUN_END

TL_DO_FUN(incr_extra)
  int res = do_incr_value (e->uid, e->local_id, e->k, e->value);
  if (res < 0) {
    return res;
  }
  if (!res) {
    tl_store_int (TL_MAYBE_FALSE);
    return 0;
  }
  long long nres;
  res = get_message_value (e->uid, e->local_id, e->k, -1, &nres);
  if (res == -2) {
    tl_store_int (TL_MAYBE_TRUE);
    tl_store_int (1 << 31);    
  } else if (res <= 0) {
    tl_store_int (TL_MAYBE_FALSE);
  } else {
    tl_store_int (TL_MAYBE_TRUE);
    tl_store_int (nres);
  }
TL_DO_FUN_END

TL_DO_FUN(get_Extra)
  long long nres;
  int res = get_message_value (e->uid, e->local_id, e->k, 1, &nres);
  if (res < 0) {
    return res;
  }

  vkprintf (2, "tl_do_get_Extra: uid:%d, loacl_id:%d, k:%d, result %lld\n", e->uid, e->local_id, e->k, nres);
  tl_store_int (TL_MAYBE_TRUE);
  tl_store_long (nres);
TL_DO_FUN_END

TL_DO_FUN(set_Extra)
  struct value_data V;
  V.data[0] = e->value;
  V.fields_mask = V.zero_mask = (1 << e->k);
  int res = do_set_values (e->uid, e->local_id, &V);
  if (res < 0) {
    return res;
  }
  tl_store_int (res <= 0 ? TL_BOOL_FALSE : TL_BOOL_TRUE);
TL_DO_FUN_END

TL_DO_FUN(incr_Extra)
  int res = do_incr_value_long (e->uid, e->local_id, e->k, e->value);
  if (res < 0) { return res; }
  if (!res) {
    tl_store_int (TL_MAYBE_FALSE);
  } else {
    long long nres;
    res = get_message_value (e->uid, e->local_id, e->k, -1, &nres);
    if (res == -2) {
      tl_store_int (TL_MAYBE_TRUE);
      tl_store_long (1 << 31);    
    } else if (res <= 0) {
      tl_store_int (TL_MAYBE_FALSE);
    } else {
      tl_store_int (TL_MAYBE_TRUE);
      tl_store_long (nres);
    }
  }
TL_DO_FUN_END

TL_DO_FUN(get_extra_mask)
  tl_store_int (TL_TUPLE);
  tl_store_int (write_extra_mask);
  tl_store_int (read_extra_mask);
  tl_store_int (index_extra_mask);
TL_DO_FUN_END

TL_DO_FUN(set_extra_mask)
  int res = do_change_mask (e->new_mask);
  tl_store_int (TL_BOOL_STAT);
  tl_store_int (res > 0 ? 1 : 0);
  tl_store_int (res <= 0 ? 1 : 0);
  tl_store_int (0);
TL_DO_FUN_END

TL_DO_FUN(replace_message_text)
  int flags = get_message_flags (e->uid, e->local_id, 1);
  if (flags == -2) { return flags; }
  if (flags == -1) {
    tl_store_int (TL_BOOL_FALSE);
    return 0;
  }

  int res = do_replace_message_text (e->uid, e->local_id, e->text, e->text_len);
  tl_store_int (res > 0 ? TL_BOOL_TRUE : TL_BOOL_FALSE);
TL_DO_FUN_END

TL_DO_FUN(get_userdata)
  int res = check_user_metafile (e->uid, R);
  if (res <= 0) {
    tl_store_int (TL_MAYBE_FALSE);
  } else {
    tl_store_int (TL_MAYBE_TRUE);
    tl_store_int (R[0]);
  }
TL_DO_FUN_END

TL_DO_FUN(delete_userdata)
  int res = unload_user_metafile (e->uid);
  tl_store_int (res <= 0 ? TL_BOOL_FALSE : TL_BOOL_TRUE);
TL_DO_FUN_END

TL_DO_FUN(load_userdata)
  switch (e->force) {
  case 0:
    unload_user_metafile (e->uid);
    tl_store_int (TL_BOOL_TRUE);
    break;
  case 1:
    unload_user_metafile (e->uid);
    tl_store_int (TL_BOOL_TRUE);
    break;
  case 2:
    load_user_metafile (e->uid);
    tl_store_int (TL_BOOL_TRUE);
    break;
  default:
    tl_store_int (TL_BOOL_FALSE);
  }
TL_DO_FUN_END

TL_DO_FUN(search)
  int res = get_search_results (e->uid, e->peer_id, e->and_mask, e->or_mask, e->min_time, e->max_time, e->num, e->text);
  if (res < 0 && res != -3) { return res; }
  if (res == -3) {
    tl_store_int (TL_MAYBE_FALSE);
    return 0;
  } else {
    tl_store_int (TL_MAYBE_TRUE);
    tl_store_int (res);
    tl_store_int (R_cnt);
    tl_store_raw_data ((char *)(R), sizeof (int) * R_cnt);
  }
TL_DO_FUN_END

TL_DO_FUN(get_timestamp)
  int res = get_timestamp (e->uid, e->force);
  if (res < 0) { return res; }

  if (res > 0) {
    tl_store_int (TL_MAYBE_TRUE);
    tl_store_int (res);
  } else {
    tl_store_int (TL_MAYBE_FALSE);
  }
TL_DO_FUN_END

TL_DO_FUN(history)
  vkprintf (2, "get_history: uid = %d, timestamp = %d, limit = %d\n", e->uid, e->timestamp, e->limit);
  int res = get_history (e->uid, e->timestamp, e->limit, R);
  if (res < 0) {
    return res;
  }

  tl_store_int (TL_MAYBE_TRUE);
  tl_store_int (get_timestamp (e->uid, 1));
  tl_store_int (res);
  int i;
  for (i = 0; i < res; i++) {
    if (R[3 * i] < 100) {
      tl_store_int (TL(EVENT_BASE));
      tl_store_int (R[3 * i]);
      tl_store_int (R[3 * i + 1]);
      tl_store_int (R[3 * i + 2]);
    } else {
      tl_store_int (TL(EVENT_EX));
      int len = strlen (*(char **)(R + 3 * i + 1));
      tl_store_int (R[3 * i]);
      tl_store_string (*(char **)(R + 3 * i + 1), len < 65536 - 16 ? len : 65536 - 16);
    }
  }
TL_DO_FUN_END

TL_DO_FUN(history_action_base)
	int res =  do_update_history (e->uid, e->who, e->data, e->event_id);
  if (res < 0) {
    return res;
  }
  tl_store_int (res <= 0 ? TL_BOOL_FALSE : TL_BOOL_TRUE);
TL_DO_FUN_END

TL_DO_FUN(history_action_ex)
	int res =  do_update_history_extended (e->uid, e->text, e->text_len, e->event_id);
  if (res < 0) { return res; }
  tl_store_int (res <= 0 ? TL_BOOL_FALSE : TL_BOOL_TRUE);
TL_DO_FUN_END

TL_DO_FUN(get_ptimestamp)
  int res = get_persistent_timestamp (e->uid);
  if (res < 0) {
    return res;
  }
  if (res > 0) {
    tl_store_int (TL_MAYBE_TRUE);
    tl_store_int (res);
  } else {
    tl_store_int (TL_MAYBE_FALSE);
  }
TL_DO_FUN_END

TL_DO_FUN(phistory)
  vkprintf (2, "get_persistent_history: uid = %d, timestamp = %d, limit = %d\n", e->uid, e->timestamp, e->limit);
  int res = get_persistent_history (e->uid, e->timestamp, e->limit, R);
  if (res < 0) {
    return res;
  }

  tl_store_int (TL_MAYBE_TRUE);
  tl_store_int (e->timestamp + res);
  tl_store_int (res);
  int i;
  for (i = 0; i < res; i++) {
    if (R[3 * i] < 100) {
      tl_store_int (TL(EVENT_BASE));
      tl_store_int (R[3 * i]);
      tl_store_int (R[3 * i + 1]);
      tl_store_int (R[3 * i + 2]);
    } else {
      tl_store_int (TL(EVENT_EX));
      int len = strlen (*(char **)(R + 3 * i + 1));
      tl_store_int (R[3 * i]);
      tl_store_string (*(char **)(R + 3 * i + 1), len < 65536 - 16 ? len : 65536 - 16);
    }
  }
TL_DO_FUN_END

TL_DO_FUN(online)
  int res = (e->offline ? user_friends_offline : user_friends_online) (e->uid, e->size, e->data);
  if (res < 0) {
    return res;
  }
  tl_store_int (TL_BOOL_STAT);
  tl_store_int (res > 0);
  tl_store_int (res <= 0);
  tl_store_int (0);
TL_DO_FUN_END

TL_DO_FUN(online_friends)
  int res = get_online_friends (e->uid, (e->mode & 1));
  if (res < 0) {
    return res;
  }
  tl_store_int (TL_MAYBE_TRUE);
  tl_store_int (res);
  tl_store_string_data ((char *)R, R_cnt * 4);
TL_DO_FUN_END

TL_DO_FUN(set_secret)
  int res = set_user_secret (e->uid, e->secret);
  if (res < 0) {
    return res;
  }
  tl_store_int (res <= 0 ? TL_BOOL_FALSE : TL_BOOL_TRUE);
TL_DO_FUN_END

TL_DO_FUN(delete_secret)
  int res = set_user_secret (e->uid, 0);
  if (res < 0) {
    return res;
  }
  tl_store_int (res <= 0 ? TL_BOOL_FALSE : TL_BOOL_TRUE);
TL_DO_FUN_END

TL_DO_FUN(get_secret)
  char *s = get_user_secret (e->uid);
  if (!s) {
    tl_store_int (TL_MAYBE_FALSE);
  } else {
    tl_store_int (TL_MAYBE_TRUE);
    tl_store_string (s, 8);
  }
TL_DO_FUN_END

TL_PARSE_FUN(get_message,int full)
  e->uid = tl_parse_uid ();
  e->local_id = tl_parse_local_id (); //local_id
  e->mode = full ? tl_fetch_int () : 6369; // mode
  e->max_len = full ? tl_fetch_int () : 0x7fffffff; //max_len;
TL_PARSE_FUN_END

TL_PARSE_FUN(convert_legacy_id,void)
  e->uid = tl_parse_uid ();
  e->legacy_id = tl_fetch_int ();
TL_PARSE_FUN_END

TL_PARSE_FUN(convert_random_id,void)
  e->uid = tl_parse_uid ();
  e->random_id = tl_fetch_long ();
TL_PARSE_FUN_END

TL_PARSE_FUN(peer_msg_list,void)
  e->uid = tl_parse_uid ();
  e->peer_id = tl_parse_peer_id ();
  e->from = tl_fetch_int ();
  e->to = tl_fetch_int ();
TL_PARSE_FUN_END

TL_PARSE_FUN(peer_msg_list_pos,void)
  e->uid = tl_parse_uid ();
  e->peer_id = tl_parse_peer_id ();
  e->local_id = tl_parse_local_id ();
TL_PARSE_FUN_END

TL_PARSE_FUN(top_msg_list,void)
  e->uid = tl_parse_uid ();
  e->from = tl_fetch_int ();
  e->to = tl_fetch_int ();
TL_PARSE_FUN_END

TL_PARSE_FUN(sublist,int full)
  e->uid = tl_parse_uid ();
  e->mode = full ? tl_fetch_int () : (1 << 14);
  e->and_mask = tl_fetch_int ();
  e->or_mask = tl_fetch_int ();
  e->from = tl_fetch_int ();
  e->to = tl_fetch_int ();
  if (e->mode & ~(32 | 64 | 128 | (1 << 14))) {
    tl_fetch_set_error ("Sublist only supports flags 5, 6, 7 and 14", TL_ERROR_VALUE_NOT_IN_RANGE);
    return 0;
  }
TL_PARSE_FUN_END

TL_PARSE_FUN(sublist_pos,void)
  e->uid = tl_parse_uid ();
  e->and_mask = tl_fetch_int ();
  e->or_mask = tl_fetch_int ();
  e->local_id = tl_parse_local_id ();
TL_PARSE_FUN_END

TL_PARSE_FUN(sublist_types,void)
TL_PARSE_FUN_END

TL_PARSE_FUN(peermsg_type,void)
TL_PARSE_FUN_END

TL_PARSE_FUN(send_message,void)
  e->uid = tl_parse_uid ();
  int mode;
  mode = e->mode = tl_fetch_int ();

  if (mode & ((1 << 14) | (1 << 0) | (1 << 6))) {
    tl_fetch_set_error_format (TL_ERROR_VALUE_NOT_IN_RANGE, "Mode can not have bits 14(local_id), 0(global_id), 6(date)  mode = 0x%08x", mode);
    return 0;
  }

  if ((~mode) & ((1 << 5) | (1 << 7) | (1 << 4))) {
    tl_fetch_set_error_format (TL_ERROR_VALUE_NOT_IN_RANGE, "Mode must have bit 5(flags), 7(peer_id), 4(text) mode = 0x%08x", mode);
    return 0;
  }
  
  memset (&e->Z, 0, sizeof (e->Z));
  e->legacy_id = 0;
  e->Z.M.user_id = e->uid;
  e->Z.M.date = 0;
  e->legacy_id = (mode & (1 << 1)) ? tl_fetch_int () : 0; //legacy_id
  e->Z.M.peer_msg_id = (mode & (1 << 2)) ? tl_fetch_int () : 0; //peer_msg_id
  e->Z.M.type = (mode & (1 << 5)) ? tl_fetch_int () : 0; //flags
  e->Z.M.peer_id = (mode & (1 << 7)) ? tl_fetch_int () : 0; //peer_id
  e->Z.M.ip = (mode & (1 << 8)) ? tl_fetch_int () : 0; //ip
  e->Z.M.port = (mode & (1 << 9)) ? tl_fetch_int () : 0; //front
  e->Z.M.front = (mode & (1 << 10)) ? tl_fetch_int () : 0; //port
  e->Z.M.ua_hash = (mode & (1 << 11)) ? tl_fetch_long () : 0; //ua_hash
  if (mode & (1 << 15)) {
    e->legacy_id = tl_fetch_long ();
  }

  if (e->Z.M.type & -0x10000) {
    tl_fetch_set_error_format (TL_ERROR_BAD_VALUE, "Flags can not have bits 16--31 set (flags = %08x)", e->Z.M.type);
    return 0;
  }

  int extra_mask = (mode & 0xfff0000) >> 16;
  //int extra_num = __builtin_popcount (extra_mask);
  e->Z.M.type |= mode & 0xfff0000;

  int i;
  int cc = 0;
  for (i = 0; i < 12; i++) if (extra_mask & (1 << i)) {
    if (i < 8) {
      e->Z.extra[cc ++] = tl_fetch_int ();
    } else {
      *(long long *)(e->Z.extra + cc) = tl_fetch_long ();
      cc += 2;
    }
  }

  int len = 0;
  if (mode & (1 << 3)) {
    len = tl_fetch_string0 (e->text, MAX_TEXT_LEN - 2);
    if (len < 0) { return 0; }
    if (check_kludges (e->text, len) != e->text + len) {
      tl_fetch_set_error_format (TL_ERROR_BAD_VALUE, "Bad cludges");
      return 0;
    }
    //e->text[len++] = '\t';    
  }
  len += tl_fetch_string0 (e->text + len, MAX_TEXT_LEN - len - 2);
  extra->size += len + 1;
  e->Z.M.text_len = len;
TL_PARSE_FUN_END

TL_PARSE_FUN(delete_message,void)
  e->uid = tl_parse_uid ();
  e->local_id = tl_parse_local_id ();
TL_PARSE_FUN_END

TL_PARSE_FUN(delete_first_messages,void)
  e->uid = tl_parse_uid ();
  e->min_local_id = tl_parse_local_id ();
TL_PARSE_FUN_END

TL_PARSE_FUN(get_flags,void)
  e->uid = tl_parse_uid ();
  e->local_id = tl_parse_local_id ();
TL_PARSE_FUN_END

TL_PARSE_FUN(set_flags,void)
  e->uid = tl_parse_uid ();
  e->local_id = tl_parse_local_id ();
  e->flags = tl_fetch_int ();
TL_PARSE_FUN_END

TL_PARSE_FUN(incr_flags,int decr)
  e->uid = tl_parse_uid ();
  e->local_id = tl_parse_local_id ();
  e->flags = tl_fetch_int ();
  e->decr = decr;
TL_PARSE_FUN_END

TL_PARSE_FUN(get_extra,void)
  e->uid = tl_parse_uid ();
  e->local_id = tl_parse_local_id ();
  e->k = tl_fetch_int ();
  if (e->k < 0 || e->k >= 8) {
    tl_fetch_set_error ("extra num should be in range 0--7", TL_ERROR_VALUE_NOT_IN_RANGE);
    return 0;
  }
TL_PARSE_FUN_END

TL_PARSE_FUN(set_extra,void)
  e->uid = tl_parse_uid ();
  e->local_id = tl_parse_local_id ();
  e->k = tl_fetch_int ();
  e->value = tl_fetch_int ();
  if (e->k < 0 || e->k >= 8) {
    tl_fetch_set_error ("extra num should be in range 0--7", TL_ERROR_VALUE_NOT_IN_RANGE);
    return 0;
  }
TL_PARSE_FUN_END

TL_PARSE_FUN(incr_extra,int decr)
  e->uid = tl_parse_uid ();
  e->local_id = tl_parse_local_id (); 
  e->k = tl_fetch_int ();
  e->value = decr ? -tl_fetch_int () : tl_fetch_int ();
  if (e->k < 0 || e->k >= 8) {
    tl_fetch_set_error ("extra num should be in range 0--7", TL_ERROR_VALUE_NOT_IN_RANGE);
    return 0;
  }
TL_PARSE_FUN_END

TL_PARSE_FUN(get_Extra,void)
  e->uid = tl_parse_uid ();
  e->local_id = tl_parse_local_id ();
  e->k = tl_fetch_int ();
  if (extra->extra[2] < 8 || extra->extra[2] >= 12) {
    tl_fetch_set_error ("Extra num should be in range 8--11", TL_ERROR_VALUE_NOT_IN_RANGE);
    return 0;
  }
TL_PARSE_FUN_END

TL_PARSE_FUN(set_Extra,void)
  e->uid = tl_parse_uid ();
  e->local_id = tl_parse_local_id ();
  e->k = tl_fetch_int ();
  if (e->k < 8 || e->k >= 12) {
    tl_fetch_set_error ("extra num should be in range 8--11", TL_ERROR_VALUE_NOT_IN_RANGE);
    return 0;
  }
  e->value = tl_fetch_long ();
TL_PARSE_FUN_END

TL_PARSE_FUN(incr_Extra,int decr)
  e->uid = tl_parse_uid ();
  e->local_id = tl_parse_local_id ();
  e->k = tl_fetch_int ();
  if (e->k < 8 || e->k >= 12) {
    tl_fetch_set_error ("extra num should be in range 8--11", TL_ERROR_VALUE_NOT_IN_RANGE);
    return 0;
  }
  e->value = decr ? -tl_fetch_long () : tl_fetch_long ();
TL_PARSE_FUN_END

TL_PARSE_FUN(get_extra_mask,void)
TL_PARSE_FUN_END

TL_PARSE_FUN(set_extra_mask,void)
  e->new_mask = tl_fetch_int ();
  if (e->new_mask & ~MAX_EXTRA_MASK) {
    tl_fetch_set_error_format (TL_ERROR_BAD_VALUE, "extra_mask (0x%08x) should be submask of 0x%08x", e->new_mask, MAX_EXTRA_MASK);
    return 0;
  }
TL_PARSE_FUN_END


TL_PARSE_FUN(replace_message_text,void)
  e->uid = tl_parse_uid ();
  e->local_id = tl_parse_local_id ();
  e->text_len = tl_fetch_string0 (e->text, MAX_TEXT_LEN);
  extra->size += e->text_len + 1;
TL_PARSE_FUN_END

TL_PARSE_FUN(get_userdata)
  e->uid = tl_parse_uid ();
TL_PARSE_FUN_END

TL_PARSE_FUN(delete_userdata)
  e->uid = tl_parse_uid ();
TL_PARSE_FUN_END

TL_PARSE_FUN(load_userdata)
  e->uid = tl_parse_uid ();
  e->force = tl_fetch_int ();
TL_PARSE_FUN_END

TL_PARSE_FUN(search,int full)
  e->uid = tl_parse_uid ();
  e->num = tl_fetch_int ();
  e->text_len = tl_fetch_string0 (e->text, MAX_TEXT_LEN - 1);
  extra->size += e->text_len + 1;
  e->flags = full ? tl_fetch_int () : 0;
  if (e->flags & 1) {
    e->and_mask = tl_fetch_int ();
    e->or_mask = tl_fetch_int ();
  } else {
    e->and_mask = -1;
    e->or_mask = -1;
  }
  if (e->flags & 2) {
    e->min_time = tl_fetch_int ();
    e->max_time = tl_fetch_int ();
  } else {
    e->min_time = 0;
    e->max_time = 0x7fffffff;
  }
  if (e->flags & 4) {
    e->peer_id = tl_fetch_int ();
  } else {
    e->peer_id = 0;
  }
TL_PARSE_FUN_END

TL_PARSE_FUN(get_timestamp,int force)
  e->uid = tl_parse_uid ();
  e->force = force;
TL_PARSE_FUN_END

TL_PARSE_FUN(history,void)
  e->uid = tl_parse_uid ();
  e->timestamp = tl_fetch_int ();
  e->limit = tl_fetch_int ();
TL_PARSE_FUN_END

TL_PARSE_FUN(history_action_base,int uid)
  e->uid  = uid;
  e->event_id = tl_fetch_int ();
  e->who = tl_fetch_int ();
  e->data = tl_fetch_int ();
TL_PARSE_FUN_END

TL_PARSE_FUN(history_action_ex,int uid)
  e->uid  = uid;
  e->event_id = tl_fetch_int ();
  e->text_len = tl_fetch_string0 (e->text, 1023);
  extra->size += e->text_len + 1;
TL_PARSE_FUN_END


struct tl_act_extra *tl_history_action (void) {
  int uid = tl_parse_uid ();
  int x = tl_fetch_int ();
  if (x == TL(EVENT_BASE)) {
    return tl_history_action_base (uid);
  } else if (x == TL(EVENT_EX)) {
    return tl_history_action_ex (uid);
  } else {
    tl_fetch_set_error_format (TL_ERROR_SYNTAX, "TL_TEXT_EVENT_BASE(0x%08x) or TL_TEXT_EVENT_EX(0x%08x) expected. %08x presented", TL(EVENT_BASE), TL(EVENT_EX), x);
    return 0;
  }
}

TL_PARSE_FUN(get_ptimestamp,void)
  e->uid = tl_parse_uid ();
TL_PARSE_FUN_END

TL_PARSE_FUN(phistory,void)
  e->uid = tl_parse_uid ();
  e->timestamp = tl_fetch_int ();
  e->limit = tl_fetch_int ();
TL_PARSE_FUN_END

TL_PARSE_FUN(online,int offline)
  e->uid = tl_fetch_int ();
  e->offline = offline;
  e->size = tl_fetch_int ();
  if (e->size < 0 || e->size > MAX_USER_FRIENDS) {
    tl_fetch_set_error_format (TL_ERROR_VALUE_NOT_IN_RANGE, "Number of friends should be in range [0...%d]", MAX_USER_FRIENDS);
    return 0;
  }
  tl_fetch_string_data ((char *)(e->data), e->size * 4);
  extra->size += e->size * 4;
  int i;
  for (i = 0; i < extra->extra[2]; i++) {
    if (conv_uid (extra->extra[3 + i]) < 0) {
      tl_fetch_set_error_format (TL_ERROR_BAD_VALUE, "Wrong server: user_id = %d, log_split_mod = %d, log_split_min = %d, log_split_max = %d", extra->extra[3 + i], log_split_mod, log_split_min, log_split_max);
      return 0;
    }
  }
TL_PARSE_FUN_END

TL_PARSE_FUN(online_friends,int full)
  e->uid = tl_parse_uid ();
  e->mode = full ? tl_fetch_int () : 0;
TL_PARSE_FUN_END

TL_PARSE_FUN(set_secret,void)
  e->uid = tl_parse_uid ();
  if (tl_fetch_string0 (e->secret, 8) != 8) {
    tl_fetch_set_error_format (TL_ERROR_BAD_VALUE, "secret must be exactly 8 characters long");
    return 0;
  }
TL_PARSE_FUN_END

TL_PARSE_FUN(get_secret,void)
  e->uid = tl_parse_uid ();
TL_PARSE_FUN_END

TL_PARSE_FUN(delete_secret,void)
  e->uid = tl_parse_uid ();
TL_PARSE_FUN_END

struct tl_act_extra *text_parse_function (long long actor_id) {
  if (actor_id != 0) {
    tl_fetch_set_error ("Text only support actor_id = 0", TL_ERROR_WRONG_ACTOR_ID);
    return 0;
  }
  int op = tl_fetch_int ();
  if (tl_fetch_error ()) {
    return 0;
  }
  switch (op) {
  case TL(GET_MESSAGE):
    return tl_get_message (1);
  case TL(GET_MESSAGE_SHORT):
    return tl_get_message (0);
  case TL(CONVERT_LEGACY_ID):
    return tl_convert_legacy_id ();
  case TL(CONVERT_RANDOM_ID):
    return tl_convert_random_id ();
  case TL(PEER_MSG_LIST):
    return tl_peer_msg_list ();
  case TL(PEER_MSG_LIST_POS):
    return tl_peer_msg_list_pos ();
  case TL(TOP_MSG_LIST):
    return tl_top_msg_list ();
  case TL(SUBLIST):
    return tl_sublist (1);
  case TL(SUBLIST_SHORT):
    return tl_sublist (0);
  case TL(SUBLIST_POS):
    return tl_sublist_pos ();
  case TL(SUBLIST_TYPES):
    return tl_sublist_types ();
  case TL(PEERMSG_TYPE):
    return tl_peermsg_type ();
  case TL(SEND_MESSAGE):
    return tl_send_message ();
  case TL(DELETE_MESSAGE):
    return tl_delete_message ();
  case TL(DELETE_FIRST_MESSAGES):
    return tl_delete_first_messages ();
  case TL(GET_MESSAGE_FLAGS):
    return tl_get_flags ();
  case TL(SET_MESSAGE_FLAGS):
    return tl_set_flags ();
  case TL(INCR_MESSAGE_FLAGS):
    return tl_incr_flags (0);
  case TL(DECR_MESSAGE_FLAGS):
    return tl_incr_flags (1);
  case TL(GET_MESSAGE_e_EXTRA):
    return tl_get_extra ();
  case TL(SET_MESSAGE_e_EXTRA):
    return tl_set_extra ();
  case TL(INCR_MESSAGE_e_EXTRA):
    return tl_incr_extra (0);
  case TL(DECR_MESSAGE_e_EXTRA):
    return tl_incr_extra (1);
  case TL(GET_MESSAGE_E_EXTRA):
    return tl_get_Extra ();
  case TL(SET_MESSAGE_E_EXTRA):
    return tl_set_Extra ();
  case TL(INCR_MESSAGE_E_EXTRA):
    return tl_incr_Extra (0);
  case TL(DECR_MESSAGE_E_EXTRA):
    return tl_incr_Extra (1);
  case TL(GET_USERDATA):
    return tl_get_userdata ();
  case TL(LOAD_USERDATA):
    return tl_load_userdata ();
  case TL(DELETE_USERDATA):
    return tl_delete_userdata ();
  case TL(REPLACE_MESSAGE_TEXT):
    return tl_replace_message_text ();
  case TL(GET_EXTRA_MASK):
    return tl_get_extra_mask ();
  case TL(SET_EXTRA_MASK):
    return tl_set_extra_mask ();
  case TL(SEARCH):
    return tl_search (0);
  case TL(SEARCH_EX):
    return tl_search (1);
  case TL(GET_TIMESTAMP):
    return tl_get_timestamp (0);
  case TL(GET_FORCE_TIMESTAMP):
    return tl_get_timestamp (1);
  case TL(HISTORY):
    return tl_history ();
  case TL(HISTORY_ACTION):
    return tl_history_action ();
  case TL(GET_P_TIMESTAMP):
    return tl_get_ptimestamp ();
  case TL(P_HISTORY):
    return tl_phistory ();
  case TL(ONLINE):
    return tl_online (0);
  case TL(OFFLINE):
    return tl_online (1);
  case TL(ONLINE_FRIENDS_ID):
    return tl_online_friends (0);
  case TL(ONLINE_FRIENDS):
    return tl_online_friends (1);
  case TL(SET_SECRET):
    return tl_set_secret ();
  case TL(GET_SECRET):
    return tl_get_secret ();
  case TL(DELETE_SECRET):
    return tl_delete_secret ();
  default:
    tl_fetch_set_error_format (TL_ERROR_UNKNOWN_FUNCTION_ID, "Unknown op %08x", op);
    return 0;
  }
}

/*
 *
 *	PARSE ARGS & INITIALIZATION
 *
 */


void reopen_logs(void) {
  int fd;
  fflush(stdout);
  fflush(stderr);
  if ((fd = open("/dev/null", O_RDWR, 0)) != -1) {
    dup2(fd, 0);
    dup2(fd, 1);
    dup2(fd, 2);
    if (fd > 2)
      close(fd);
  }
  if (logname && (fd = open(logname, O_WRONLY|O_APPEND|O_CREAT, 0640)) != -1) {
    dup2(fd, 1);
    dup2(fd, 2);
    if (fd > 2)
      close(fd);
  }
  if (verbosity > 0) {
    fprintf (stderr, "logs reopened.\n");
  }
}

static void sigint_immediate_handler (const int sig) {
  fprintf (stderr, "SIGINT handled immediately.\n");
  //  flush_binlog_last();
  //  sync_binlog (2);
  exit (1);
}

static void sigterm_immediate_handler (const int sig) {
  fprintf (stderr, "SIGTERM handled immediately.\n");
  //  flush_binlog_last();
  //  sync_binlog (2);
  exit (1);
}

static void sigint_handler (const int sig) {
  fprintf (stderr, "SIGINT handled.\n");
  pending_signals |= (1 << SIGINT);
  signal(SIGINT, sigint_immediate_handler);
}

static void sigterm_handler (const int sig) {
  fprintf (stderr, "SIGTERM handled.\n");
  pending_signals |= (1 << SIGTERM);
  signal(SIGTERM, sigterm_immediate_handler);
}

static void sighup_handler (const int sig) {
  fprintf (stderr, "got SIGHUP.\n");
  // sync_binlog (2);
  signal(SIGHUP, sighup_handler);
}

static void sigusr1_handler (const int sig) {
  fprintf (stderr, "got SIGUSR1, rotate logs.\n");
  reopen_logs();
  // sync_binlog (2);
  signal(SIGUSR1, sigusr1_handler);
}

static void sigpoll_handler (const int sig) {
  sigpoll_cnt++;
  signal(SIGPOLL, sigpoll_handler);
}

void cron (void) {
  flush_binlog();
  adjust_some_users ();
}

int sfd, http_sfd = -1, http_port;

void start_server (void) { 
  char buf[64];
  int i;
  int prev_time;

  init_epoll();
  init_netbuffers();
  if (udp_enabled) {
    init_msg_buffers (0);
  }

  prev_time = 0;

  if (!sfd) {
    sfd = server_socket (port, settings_addr, backlog, 0);
  }

  if (sfd < 0) {
    fprintf(stderr, "cannot open server socket at port %d: %m\n", port);
    exit(3);
  }

  if (http_port > 0 && http_sfd < 0) {
    http_sfd = server_socket (http_port, settings_addr, backlog, 0);
    if (http_sfd < 0) {
      fprintf(stderr, "cannot open http server socket at port %d: %m\n", http_port);
      exit(1);
    }
  }


  if (verbosity) {
    fprintf (stderr, "created listening socket at %s:%d, fd=%d\n", conv_addr(settings_addr.s_addr, buf), port, sfd);
  }

  if (daemonize) {
    setsid ();
  }

  if (change_user (username) < 0) {
    fprintf (stderr, "fatal: cannot change user to %s\n", username ? username : "(none)");
    exit (1);
  }

  if (binlogname && !binlog_disabled) {
    assert (append_to_binlog (Binlog) == log_readto_pos);
  }
 
  //init_listening_connection (sfd, &ct_text_engine_server, &memcache_methods);
  //init_listening_connection (sfd, &ct_text_engine_server, &rpc_methods);
  tl_parse_function = text_parse_function;
  tl_aio_timeout = 2.5;
  init_listening_connection (sfd, &ct_rpc_server, &rpc_methods);
  if (udp_enabled) {
    add_udp_socket (port, 0);
  }

  if (http_sfd >= 0) {
    init_listening_connection (http_sfd, &ct_http_server, &http_methods);
  }
 
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

  for (i = 0; ; i++) {
    if (verbosity > 0 && !(i & 255)) {
      fprintf (stderr, "epoll_work(): %d out of %d connections, network buffers: %d used, %d out of %d allocated\n",
	       active_connections, maxconn, NB_used, NB_alloc, NB_max);
    }

    epoll_work (53);

    
    if (sigpoll_cnt > 0) {
      if (verbosity > 1) {
        fprintf (stderr, "after epoll_work(), sigpoll_cnt=%d\n", sigpoll_cnt);
      }
      sigpoll_cnt = 0;
    }

    /* !!! */
    check_all_aio_completions ();
    //tl_aio_query_restart_all_finished ();
    tl_restart_all_ready ();


    if (now != prev_time) {
      prev_time = now;
      cron();
    }

    if (pending_signals) {
      break;
    }

    if (epoll_pre_event) {
      epoll_pre_event();
    }

    if (quit_steps && !--quit_steps) break;
  }

  if (verbosity > 0) {
    fprintf (stderr, "quitting because of pending signals = %llx\n", pending_signals);
  }

  epoll_close (sfd);
  close(sfd);

  flush_binlog_last();
  sync_binlog (2);
}

/*
 *
 *		MAIN
 *
 */

void test_normalize_ip (void) {
  /*strcpy (ip_buff, "195.19.228.2");
  int r = normalize_ip_buff ();
  printf ("%d %s\n", r, ip_buff);

  strcpy (ip_buff, "195.19.228");
  r = normalize_ip_buff ();
  printf ("%d %s\n", r, ip_buff);

  strcpy (ip_buff, "a::b");
  r = normalize_ip_buff ();
  printf ("%d %s\n", r, ip_buff);

  strcpy (ip_buff, "2001:470:1f0a:5b1::2");
  r = normalize_ip_buff ();
  printf ("%d %s\n", r, ip_buff);

  strcpy (ip_buff, "::2");
  r = normalize_ip_buff ();
  printf ("%d %s\n", r, ip_buff);

  strcpy (ip_buff, "2001::");
  r = normalize_ip_buff ();
  printf ("%d %s\n", r, ip_buff);

  strcpy (ip_buff, "2001:::");
  r = normalize_ip_buff ();
  printf ("%d %s\n", r, ip_buff);*/
}

void usage (void) {
  printf ("usage: %s [-v] [-r] [-i] [-S] [-p<port>] [-H<http-port>] [-u<username>] [-b<backlog>] [-c<max-conn>] [-a<binlog-name>] [-l<log-name>] [-o<online-seconds>] <index-file>\n"
  	  "\t" VERSION_STR " compiled at " __DATE__ " " __TIME__ " by gcc " __VERSION__ " "
#ifdef __LP64__
	  "64-bit"
#else
	  "32-bit"
#endif
	  " after commit " COMMIT "\n",
	  progname);
  parse_usage ();
  exit(2);
}

static int half_mem;
static int list_large_metafiles;
int f_parse_option (int val) {
  long long x;
  char c;
  int i;
  switch (val) {
  case 'Z': case 'm':
    c = 0;
    assert (sscanf (optarg, "%lld%c", &x, &c) >= 1);
    switch (c | 0x20) {
      case 'k':  x <<= 10; break;
      case 'm':  x <<= 20; break;
      case 'g':  x <<= 30; break;
      case 't':  x <<= 40; break;
      default: assert (c == 0x20);
    }
    if (val == 'Z' && x >= (1LL << 20) && x <= (sizeof(long) == 4 ? (3LL << 30) : (20LL << 30))) {
      dynamic_data_buffer_size = x;
    } else if (val == 'm' && x >= MAX_METAFILE_SIZE && x < (1LL << 34)) {
      metafile_alloc_threshold = x;
    }
    break;
  case 'H':
    http_port = atoi (optarg);
    break;
  case 'T':
    test_mode = 1;
    break;
  case 'U':
    word_split_utf8 = utf8_mode = 1;
    break;
  case 'M':
    half_mem = 1;
    break;
  case 'L':
    list_large_metafiles = 1;
    break;
  case 'i':
    search_enabled = 1;
    break;
  case 'y':
    persistent_history_enabled = 1;
    break;
  case 'S':
    use_stemmer = 1;
    break;
  case 't':
    hashtags_enabled = 1;
    break;
  case 'q':
    searchtags_enabled = 1;
    break;
  case 'o':
    i = atoi (optarg);
    if (i >= MIN_HOLD_ONLINE_TIME && i <= MAX_HOLD_ONLINE_TIME) {
      hold_online_time = i;
    }
    break;
  default:
    return -1;
  }
  return 0;
}

int main (int argc, char *argv[]) {
  int i;

  set_debug_handlers();

  parse_option (0, required_argument, 0, 'Z', "memory for zmalloc");
  parse_option ("metafile-memory", required_argument, 0, 'm', "maximal size for metafile cache");
  parse_option ("http-port", required_argument, 0, 'H', "http port (default %d)", (int)http_port);
  parse_option ("test-mode", no_argument, 0, 'T', "test mode");
  parse_option ("utf8", no_argument, 0, 'U', "word split utf8");
  parse_option ("half-mem", no_argument, 0, 'M', 0);
  parse_option ("list-large-metafiles", no_argument, 0, 'L', "list all large metafiles from index and exit");
  parse_option ("search", no_argument, 0, 'i', "enable search");
  parse_option ("persistent-history", no_argument, 0, 'y', "enables persistent history");
  parse_option ("stemmer", no_argument, 0, 'S', "enables stemmer");
  parse_option ("hashtags", no_argument, 0, 't', "enables hashtags");
  parse_option ("tags", no_argument, 0, 'q', "enables search tags");
  parse_option ("online-seconds", required_argument, 0, 'o', "online seconds (default %d)", hold_online_time);

  progname = argv[0];
  parse_engine_options_long (argc, argv, f_parse_option);

  if (argc != optind + 1 && argc != optind + 2) {
    usage();
    return 2;
  }

  signal (SIGUSR1, sigusr1_handler);

  binlog_crc32_verbosity_level = 5;

  if (!search_enabled) {
    hashtags_enabled = searchtags_enabled = 0;
  }

  init_is_letter();
  if (hashtags_enabled) {
    enable_is_letter_sigils ();
  }
  if (searchtags_enabled) {
    enable_search_tag_sigil ();
  }
  if (use_stemmer) {
    stem_init ();
  }

  if (raise_file_rlimit (maxconn + 16) < 0 && !test_mode && !list_large_metafiles) {
    fprintf (stderr, "fatal: cannot raise open file limit to %d\n", maxconn+16);
    exit (1);
  }


  if (port < PRIVILEGED_TCP_PORTS && !list_large_metafiles) {
    sfd = server_socket (port, settings_addr, backlog, 0);
    if (sfd < 0) {
      fprintf(stderr, "cannot open server socket at port %d: %m\n", port);
      exit(1);
    }
  }

  if (http_port > 0 && http_port < PRIVILEGED_TCP_PORTS && !list_large_metafiles) {
    http_sfd = server_socket (http_port, settings_addr, backlog, 0);
    if (http_sfd < 0) {
      fprintf(stderr, "cannot open http server socket at port %d: %m\n", http_port);
      exit(1);
    }
  }

  aes_load_pwd_file (aes_pwd_file);

  if (change_user (username) < 0) {
    fprintf (stderr, "fatal: cannot change user to %s\n", username ? username : "(none)");
    exit(1);
  }

  if (half_mem) {
    dynamic_data_buffer_size /= 2;
  }

  init_dyn_data();
  if (udp_enabled) {
    init_server_PID (get_my_ipv4 (), port);
  }
  log_ts_interval = 3;

  if (engine_preload_filelist (argv[optind], binlogname) < 0) {
    fprintf (stderr, "cannot open binlog files for %s\n", binlogname ? binlogname : argv[optind]);
    exit (1);
  }

  Snapshot = open_recent_snapshot (engine_snapshot_replica);

  if (Snapshot) {
    engine_snapshot_name = Snapshot->info->filename;
    engine_snapshot_size = Snapshot->info->file_size;

    if (verbosity) {
      fprintf (stderr, "load index file %s (size %lld)\n", engine_snapshot_name, engine_snapshot_size);
    }
    index_load_time = get_utime(CLOCK_MONOTONIC);

    i = load_index (Snapshot);

    index_load_time = get_utime(CLOCK_MONOTONIC) - index_load_time;

    if (i < 0) {
      fprintf (stderr, "fatal: error %d while loading index file %s\n", i, engine_snapshot_name);
      exit (1);
    }

    if (verbosity) {
      fprintf (stderr, "load index: done, jump_log_pos=%lld, alloc_mem=%ld out of %ld, time %.06lfs\n", 
	       jump_log_pos, (long) (dyn_cur - dyn_first), (long) (dyn_last - dyn_first), index_load_time);
    }

  }

  if (list_large_metafiles) {
    output_large_metafiles ();
    exit (0);
  }

  Binlog = open_binlog (engine_replica, jump_log_pos);
  if (!Binlog) {
    fprintf (stderr, "fatal: cannot find binlog for %s, log position %lld\n", engine_replica->replica_prefix, jump_log_pos);
    exit (1);
  }

  binlogname = Binlog->info->filename;

  history_enabled = 0;

  if (verbosity) {
    fprintf (stderr, "replaying binlog file %s (size %lld)\n", binlogname, Binlog->info->file_size);
  }

  binlog_load_time = get_utime (CLOCK_MONOTONIC);

  clear_log ();
  init_log_data (jump_log_pos, jump_log_ts, jump_log_crc32);

  i = replay_log (0, 1);

  binlog_load_time = get_utime (CLOCK_MONOTONIC) - binlog_load_time;
  binlog_loaded_size = log_readto_pos - jump_log_pos;

  if (i < 0) {
    fprintf (stderr, "fatal: error reading binlog\n");
    exit (1);
  }

  if (verbosity) {
    fprintf (stderr, "replay binlog file: done, pos=%lld, alloc_mem=%ld out of %ld, time %.06lfs\n", 
             log_pos, (long) (dyn_cur - dyn_first), (long) (dyn_last - dyn_first), binlog_load_time);
  }

  if (!binlog_disabled) {
    clear_read_log();
  }

  history_enabled = 1;

  clear_write_log ();
  start_time = time(0);

  start_server ();

  return 0;
}
