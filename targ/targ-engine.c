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
              2011,2013 Vitaliy Valtman
                   2013 Anton Maydell
*/

#define        _FILE_OFFSET_BITS        64

#include <assert.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <aio.h>
#include <math.h>

#include "md5.h"
#include "net-events.h"
#include "net-buffers.h"
#include "server-functions.h"
#include "kdb-targ-binlog.h"
#include "targ-data.h"
#include "targ-index.h"
#include "targ-search.h"
#include "targ-weights.h"
#include "word-split.h"
#include "stemmer.h"

#include "kfs.h"
#include "net-connections.h"
#include "net-buffers.h"
#include "net-events.h"
#include "net-crypto-aes.h"
#include "net-aio.h"
#include "net-memcache-server.h"

#include "net-rpc-server.h"
#include "vv-tl-parse.h"
#include "vv-tl-aio.h"

#include "targ-interface-structures.h"
#include "TL/constants.h"

#define VERSION_STR        "targ-engine-0.22"


#define mytime() get_utime (CLOCK_MONOTONIC)

#define        BACKLOG        8192
#define TCP_PORT 11211
#define UDP_PORT 11212

#define MAX_NET_RES        (1L << 16)

/*
 *
 *                TARGETING ENGINE
 *
 */

int targ_engine_wakeup (struct connection *c);
int targ_engine_alarm (struct connection *c);

conn_type_t ct_targ_engine_server = {
  .magic = CONN_FUNC_MAGIC,
  .title = "targ_engine_server",
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
  .wakeup = targ_engine_wakeup,
  .alarm = targ_engine_alarm,
#ifndef NOAES
  .crypto_init = aes_crypto_init,
  .crypto_free = aes_crypto_free,
  .crypto_encrypt_output = aes_crypto_encrypt_output,
  .crypto_decrypt_input = aes_crypto_decrypt_input,
  .crypto_needed_output_bytes = aes_crypto_needed_output_bytes,
#endif
};

int mcs_targ_store (struct connection *c, int op, const char *key, int key_len, int flags, int delay, int size);
int memcache_get (struct connection *c, const char *key, int key_len);
int memcache_get_start (struct connection *c);
int memcache_get_end (struct connection *c, int key_count);
int mcs_targ_delete (struct connection *c, const char *key, int key_len);
int mcs_targ_stats (struct connection *c);

struct memcache_server_functions memcache_methods = {
  .execute = mcs_execute,
  .mc_store = mcs_targ_store,
  .mc_get_start = memcache_get_start,
  .mc_get = memcache_get,
  .mc_get_end = memcache_get_end,
  .mc_incr = mcs_incr,
  .mc_delete = mcs_targ_delete,
  .mc_version = mcs_version,
  .mc_stats = mcs_targ_stats,
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
  .memcache_fallback_type = &ct_targ_engine_server,
  .memcache_fallback_extra = &memcache_methods
};


int verbosity = 0, test_mode = 0;
double index_load_time;
long long jump_log_pos;
int jump_log_crc32;
int jump_log_ts;

char *progname = "targ-engine", *username, *binlogname, *logname;
char *read_stats_filename, *write_stats_filename, *weights_engine_address;
#define SAVED_LONG_QUERIES                64
#define SAVED_LONG_QUERY_BUF_LEN        128
char last_long_query_buff[SAVED_LONG_QUERIES][SAVED_LONG_QUERY_BUF_LEN];
double last_long_query_time[SAVED_LONG_QUERIES];
double last_long_query_utime[SAVED_LONG_QUERIES];
int last_long_query_found[SAVED_LONG_QUERIES];
struct in_addr settings_addr;

unsigned treespace_ints = DEFAULT_TREESPACE_INTS, wordspace_ints = DEFAULT_WORDSPACE_INTS;

int long_query_buff_ptr, log_long_queries;

/* stats counters */
int start_time;
long long binlog_loaded_size;
long long search_queries, targeting_queries, pricing_queries, audience_queries, targ_audience_queries;
long long delete_queries, minor_update_queries;
long long tot_response_words, tot_response_bytes;
long long tot_long_queries;
double binlog_load_time, index_load_time, tot_long_queries_time;
int udp_enabled;

int get_queries;
double total_get_time;

volatile int sigpoll_cnt;

int force_write_index;
int child_pid;

#define STATS_BUFF_SIZE        (1 << 20)
char stats_buff[STATS_BUFF_SIZE];
/*
 *
 *                SERVER
 *
 */

int backlog = BACKLOG, port = TCP_PORT, maxconn = MAX_CONNECTIONS, daemonize = 1, is_server = 0;
int quit_steps;

static double safe_div (double x, double y) { return y > 0 ? x/y : 0; }

long long total_memory_used (void) {
  long long res = (long) (dyn_cur - dyn_first + dyn_last - dyn_top);
  res += (AdSpace ? ((struct treespace_header *)AdSpace)->used_ints : 0) * 4LL; 
  res += ((struct treespace_header *)WordSpace)->used_ints * 4LL;
  res += tot_userlists_size;
  res += allocated_metafile_bytes;
  return res;
}


int targ_prepare_stats (struct connection *c) {
  int uptime = now - start_time;
  int log_uncommitted = compute_uncommitted_log_bytes();
  dyn_update_stats();

  int stats_buff_len = prepare_stats (c, stats_buff, STATS_BUFF_SIZE);

  return stats_buff_len += 
        snprintf (stats_buff + stats_buff_len, STATS_BUFF_SIZE - stats_buff_len,
                  "heap_used_low\t%ld\n"
                  "heap_used\t%ld\n"
                  "heap_max\t%ld\n"
                  "current_memory_used\t%lld\n"
                  "wasted_heap_blocks\t%d\n"
                  "wasted_heap_bytes\t%ld\n"
                  "free_heap_blocks\t%d\n"
                  "free_heap_bytes\t%ld\n"
                  "binlog_original_size\t%lld\n"
                  "binlog_loaded_bytes\t%lld\n"
                  "binlog_load_time\t%.6fs\n"
                  "current_binlog_size\t%lld\n"
                  "binlog_uncommitted_bytes\t%d\n"
                  "binlog_path\t%.250s\n"
                  "binlog_first_timestamp\t%d\n"
                  "binlog_read_timestamp\t%d\n"
                  "binlog_last_timestamp\t%d\n"
                  "index_data_bytes\t%lld\n"
                  "index_loaded_bytes\t%lld\n"
                  "index_size\t%lld\n"
                  "index_path\t%s\n"
                  "index_load_time\t%.6fs\n"
                  "idx_fresh_ads\t%d\n"
                  "idx_stale_ads\t%d\n"
                  "idx_last_timestamp\t%d\n"
                  "idx_recent_views\t%d\n"
                  "ancient_ads_pending\t%d\n"
                  "ancient_ads_loading\t%d\n"
                  "ancient_ads_loaded\t%d\n"
                  "ancient_ads_aio_loaded\t%d\n"
                  "ancient_ads_loaded_bytes\t%lld\n"
                  "ancient_ads_aio_loaded_bytes\t%lld\n"
                  "allocated_metafiles\t%d\n"
                  "allocated_metafile_bytes\t%lld\n"
                  "tot_userlists\t%d\n"
                  "tot_userlists_size\t%lld\n"
                  "treespaces_used_ints\t%d %d\n"
                  "treespaces_free_ints\t%d %d\n"
                  "active_ad_nodes\t%d\n"
                  "inactive_ad_nodes\t%d\n"
                  "clicked_ad_nodes\t%d\n"
                  "index_words\t%d\n"
                  "memory_words\t%d\n"
                  "queries_search\t%lld\n"
                  "qps_search\t%.3f\n"
                  "queries_target\t%lld\n"
                  "qps_target\t%.3f\n"
                  "queries_pricing\t%lld\n"
                  "qps_pricing\t%.3f\n"
                  "queries_delete\t%lld\n"
                  "qps_delete\t%.3f\n"
                  "queries_minor_update\t%lld\n"
                  "qps_minor_update\t%.3f\n"
                  "queries_long\t%lld\n"
                  "qps_long\t%.3f\n"
                  "avg_long_query_time\t%.6f\n"
                  "last_long_query\t%.120s\n"
                  "memory_users\t%d\n"
                  "memory_groups\t%d\n"
                  "memory_user_group_pairs\t%lld\n"
                  "memory_langs\t%d\n"
                  "memory_user_lang_pairs\t%lld\n"
                  "memory_min_group_id\t%d\n"
                  "memory_max_group_id\t%d\n"
                  "ads_total\t%d\n"
                  "ads_structures\t%d\n"
                  "ads_active\t%d\n"
                  "ad_views_total\t%lld\n"
                  "ad_clicks_total\t%lld\n"
                  "ad_money_total\t%lld\n"
                  "recent_views\t%d\n"
                  "init_l_clicks\t%.2lf\n"
                  "init_l_views\t%d\n"
                  "init_lambda\t%.4f\n"
                  "init_delta\t%.4f\n"
                  "weights_address\t%s\n"
                  "weights_coords\t%d\n"
                  "targ_weights_last_update_time\t%d\n"
                  "targ_weights_delay\t%ds\n"
                  "tot_weights_vectors\t%d\n"
                  "tot_weights_vector_bytes\t%lld\n"
                  "weights_small_updates\t%lld\n"
                  "weights_updates\t%lld\n"
                  "version\t" VERSION_STR " compiled at " __DATE__ " " __TIME__ " by gcc " __VERSION__ " "
#ifdef __LP64__
                  "64-bit"
#else
                       "32-bit"
#endif
                  " after commit " COMMIT "\n",
                  (long) (dyn_cur - dyn_first),
                  (long) (dyn_cur - dyn_first + dyn_last - dyn_top),
                  (long) (dyn_last - dyn_first),
                  total_memory_used (),
                  wasted_blocks,
                  wasted_bytes,
                  freed_blocks,
                  freed_bytes,
                  log_readto_pos,
                  binlog_loaded_size,
                  binlog_load_time,
                  log_pos,
                  log_uncommitted,
                  binlogname ? binlogname : "(none)",
                  log_first_ts,
                  log_read_until,
                  log_last_ts,
                  idx_bytes, idx_loaded_bytes,
                  engine_snapshot_size, engine_snapshot_name,
                  index_load_time,
                  idx_fresh_ads, idx_stale_ads,
                  jump_log_ts,
                  idx_recent_views,
                  ancient_ads_pending, ancient_ads_loading, ancient_ads_loaded, ancient_ads_aio_loaded,
                  ancient_ads_loaded_bytes, ancient_ads_aio_loaded_bytes,
                  allocated_metafiles, allocated_metafile_bytes,
                  tot_userlists,
                  tot_userlists_size,
                  AdSpace ? ((struct treespace_header *)AdSpace)->used_ints : 0, 
                  ((struct treespace_header *)WordSpace)->used_ints, 
                  AdSpace ? get_treespace_free_stats (AdSpace) * 4 : 0,
                  get_treespace_free_stats (WordSpace) * 4,
                  active_ad_nodes,
                  inactive_ad_nodes,
                  clicked_ad_nodes,
                  idx_words,
                  hash_word_nodes,
                  search_queries,
                  safe_div (search_queries, uptime),
                  targeting_queries,
                  safe_div (targeting_queries, uptime),
                  pricing_queries,
                  safe_div (pricing_queries, uptime),
                  delete_queries,
                  safe_div (delete_queries, uptime),
                  minor_update_queries,
                  safe_div (minor_update_queries, uptime),
                  tot_long_queries,
                  safe_div (tot_long_queries, uptime),
                  safe_div (tot_long_queries_time, tot_long_queries),
                  last_long_query_buff[long_query_buff_ptr & (SAVED_LONG_QUERIES - 1)],
                  tot_users,
                  tot_groups,
                  user_group_pairs,
                  tot_langs,
                  user_lang_pairs,
                  min_group_id,
                  max_group_id,
                  tot_ads,
                  tot_ad_versions,
                  active_ads,
                  tot_views,
                  tot_clicks,            
                  tot_click_money,
                  get_recent_views_num (),
                  (double)INIT_L_CLICKS,
                  (int)INIT_L_VIEWS,
                  (double)INITIAL_LAMBDA,
                  1.0 / sqrt((double)INITIAL_INV_D),
                  weights_engine_address,
                  weights_coords,
                  targ_weights_last_update_time,
                  now - targ_weights_last_update_time,
                  tot_weights_vectors,
                  tot_weights_vector_bytes,
                  weights_small_updates, weights_updates
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
    fprintf (stderr, "WaitAio=0 - no memory to load user metafile, query %.*s dropped.\n", key_len, key);
    return 0;
  }
    
  set_connection_timeout (c, 0.5);
    
  c->generation = ++conn_generation;
  c->pending_queries = 0;

  int i;
  for (i = 0; i < WaitAioArrPos; i++) {
//  if (WaitAio) {
    create_aio_query (WaitAioArr[i], c, 0.7, &aio_metafile_query_type);
  }
  
  c->status = conn_wait_aio;

  return 0; 
}



static inline void init_search_extras (void) {
  Q_order = Q_limit = Q_raw = 0;
}
                  
static const char *parse_search_extras (const char *where) {
  int l = 4;
  if (*where != '#') {
    return where;
  }
  where++;
  while (--l) {
    switch (*where) {
    case 'i':
      Q_order = 1;
      where++;
      break;
    case 'I':
      Q_order = -1;
      where++;
      break;
    case 'r':
      Q_order = 2;
      where++;
      break;
    case 'R':
      Q_order = -2;
      where++;
      break;
    case '0' ... '9':
      Q_limit = strtol ((char *)where, (char **)&where, 10);
      break;
    case '%':
      Q_raw = -1;
      where++;
      break;
    case '@':
      if (where[1] == '#' && where[2] == '$') {
        return where + 3;
      }
    default:
      return where;
    }
  }
  return where;
}

static inline int parse_search_query_termination (const char *where) {
  where = parse_search_extras (where);
  return *where == ')' && !where[1];
}

static int prepare_user_ads (int uid, int limit, int flags, int and_mask, int xor_mask, char *buffer, long long cat_mask) {
  if (limit < 0) {
    return -1;
  }
  if (limit > 16384) {
    limit = 16384;
  }

  int res = compute_user_ads (uid, limit, flags, and_mask, xor_mask, cat_mask);
  if (res < 0) { 
    return -1; 
  }
  if (res > limit) { 
    res = limit; 
  }
  if (flags & 7) {
    res *= (flags & 1) + ((flags >> 1) & 1) + ((flags >> 2) & 1) + 1;
  }

  return res;
}


static void register_long_query (const char *key, int len) {
  long_query_buff_ptr = (long_query_buff_ptr - 1) & (SAVED_LONG_QUERIES - 1);
  last_long_query_utime[long_query_buff_ptr] = get_utime (CLOCK_MONOTONIC); 
  len = ((len) < (SAVED_LONG_QUERY_BUF_LEN - 8)) ? (len) : (SAVED_LONG_QUERY_BUF_LEN - 8);
  memcpy (last_long_query_buff[long_query_buff_ptr], key, len);
  last_long_query_buff[long_query_buff_ptr][len] = 0;
  ++tot_long_queries;
}


static void complete_long_query (struct connection *c, int res) {
  last_long_query_time[long_query_buff_ptr] = get_utime (CLOCK_MONOTONIC) - last_long_query_utime[long_query_buff_ptr];
  tot_long_queries_time += last_long_query_time[long_query_buff_ptr];
  last_long_query_found[long_query_buff_ptr] = res;
  if (verbosity > 0 || log_long_queries) {
    kprintf ("%.6lf %d (%d) %s\n", last_long_query_time[long_query_buff_ptr], c ? c->fd : -1, res, last_long_query_buff[long_query_buff_ptr]);
  }
}


static int prepare_long_query_stats (void) {
  char *tmp = stats_buff;
  int i;
  for (i = long_query_buff_ptr - 1; i >= 0; i--) {
    tmp += sprintf (tmp, "%.3lf %.6lf %d %.120s\n", last_long_query_utime[i], last_long_query_time[i], last_long_query_found[i], last_long_query_buff[i]);
  }
  for (i = SAVED_LONG_QUERIES - 1; i >= long_query_buff_ptr; i--) {
    tmp += sprintf (tmp, "%.3lf %.6lf %d %.120s\n", last_long_query_utime[i], last_long_query_time[i], last_long_query_found[i], last_long_query_buff[i]);
  }
  return tmp - stats_buff;
}

int memcache_get_start (struct connection *c) {
  c->flags &= ~C_INTIMEOUT;
  //  c->query_start_time = get_utime (CLOCK_MONOTONIC);
  return 0;
}

int memcache_get_end (struct connection *c, int key_count) {
  c->flags &= ~C_INTIMEOUT;
  //  c->last_query_time = get_utime (CLOCK_MONOTONIC) - c->query_start_time;
  //  total_get_time += c->last_query_time;
  get_queries++;
  write_out (&c->Out, "END\r\n", 5);

  free_tmp_buffers (c);
  if (verbosity > 1) {
    // fprintf (stderr, "memcache_get end: query time %.3fms\n", c->last_query_time * 1000);
  }
  return 0;
}

#define VALUE_BUFF_SIZE        262144
char value_buff[VALUE_BUFF_SIZE];
int tag_id, tag_len;

int load_temp_data (struct connection *c) {
  nb_iterator_t it;
  tag_len = 0;
  aux_userlist_size = aux_userlist_tag = 0;
  if (!c->Tmp || nbit_set (&it, c->Tmp) < 0 || nbit_read_in (&it, &tag_id, 4) < 4 || !tag_id) {
    return tag_id = 0;
  }
  if (tag_id < 0) {
    int len = nbit_read_in (&it, aux_userlist, MAX_AUX_USERS * 4);
    if (len < 0 || (len & 3) || len == MAX_AUX_USERS * 4) {
      return tag_id = 0;
    }
    aux_userlist_size = (len >> 2);
    aux_userlist_tag = tag_id;
    vkprintf (2, "restoring %d words of temp userlist data with tag %d\n", aux_userlist_size, tag_id);
    return tag_id = 0;
  }
  int len = nbit_read_in (&it, value_buff, VALUE_BUFF_SIZE - 1);
  if (len <= 0 || len >= VALUE_BUFF_SIZE - 1) {
    return tag_id = 0;
  }
  value_buff[len] = 0;
  tag_len = len;
  vkprintf (2, "restoring %d bytes of temp data with tag %d\n", len, tag_id);
  return tag_id;
}

int prepare_multiple_query_query (struct connection *c, const char *start, const char *key, int key_len) {
  char *cur = (char *) parse_search_extras (start), *q_ptr, *q_start = cur + 1;
  if (*cur != '(') {
    return -1;
  }
  register_long_query (key, key_len);
  clear_query_list ();
  char *q_err = 0;
  if (load_temp_data (c) > 0 && cur[1] >= '0' && cur[1] <= '9' && strtol (cur + 1, &q_start, 10) == tag_id) {
    q_ptr = value_buff;
    if (compile_add_query (&q_ptr)) {
      while (*q_ptr == ';' || *q_ptr == '\n') {
        q_ptr++;
        if (!compile_add_query (&q_ptr)) {
          q_err = q_ptr;
          break;
        }
      }
    }
    if (!q_err) {
      if (*q_ptr) {
        q_err = q_ptr;
      } else if (*q_start++ != ';') {
        q_err = q_start - 1;
      }
    }
  }
  q_ptr = q_start;
  if (!q_err && compile_add_query (&q_ptr)) {
    while (*q_ptr == ';') {
      q_ptr++;
      if (!compile_add_query (&q_ptr)) {
        q_err = q_ptr;
        break;
      }
    }
  }
  parse_search_extras (start);
  if (!q_err && !parse_search_query_termination (q_ptr)) {
    q_err = q_ptr;
  }
  if (q_err) {
    complete_long_query (c, -1);
    return_one_key (c, key, stats_buff, sprintf (stats_buff, "ERROR near '%.256s'\n", q_err));
    return 0;
  } else {
    return 1;
  }
}

int memcache_get_nonblock (struct connection *c, const char *key, int len) {
  int user_id = -1, ad_id = -1, price = 0, group_id = 0, x = -1;
  static long long RR[64];

  vkprintf (2, "memcache_get: key='%s'\n", key);
  init_search_extras ();

  switch (*key) {
  case 'a':
    if (len >= 8 && !memcmp (key, "audience", 8)) {
      int res = prepare_multiple_query_query (c, key + 8, key, len);
      if (res <= 0) {
        return res;
      }
      audience_queries++;
      R_cnt = 0;
      res = perform_audience_query ();
      complete_long_query (c, res);
      return return_one_key_list (c, key, len, res, Q_raw, R, R_cnt);
    }
    if (key[1] == 'd' && key[2] == '_') {
      switch (key[3]) {
      case 'c':
        if (len >= 10 && !memcmp (key + 3, "ctrsump", 7) && sscanf (key + 10, "%u%n", &ad_id, &x) >= 1 && ad_id > 0 && x > 0 && !*parse_search_extras (key + 10 + x)) {
          int res = compute_ad_ctr (ad_id, RR);
          if (res > 0) {
            R[0] = RR[0] & ((1 << 20) - 1);
            R[1] = RR[0] >> 20;
            R[2] = RR[1] & ((1 << 20) - 1);
            R[3] = RR[1] >> 20;
            long i, j;
            for (i = 2, j = 4; i < 5; i++) {
              R[j++] = RR[i] & ((1 << 20) - 1);
              RR[i] >>= 20;
              R[j++] = RR[i] & ((1 << 20) - 1);
              R[j++] = (RR[i] >> 20);
            }
            return return_one_key_list (c, key, len, 1, Q_raw, R, 13);
          }
        } else if (len >= 6 && !memcmp (key + 3, "ctr", 3) && sscanf (key + 6, "%u%n", &ad_id, &x) >= 1 && ad_id > 0 && x > 0 && !*parse_search_extras (key + 6 + x)) {
          int res = compute_ad_ctr (ad_id, RR);
          if (res > 0) {
            R[0] = RR[0] & ((1 << 20) - 1);
            R[1] = RR[0] >> 20;
            R[2] = RR[1] & ((1 << 20) - 1);
            R[3] = RR[1] >> 20;
            return return_one_key_list (c, key, len, 1, Q_raw, R, 4);
          }
        } else if (len >= 6 && !memcmp (key + 3, "clicks", 6) && sscanf (key + 9, "%u%n", &ad_id, &x) >= 1 && ad_id > 0 && x > 0 && !*parse_search_extras (key + 9 + x)) {
          return return_one_key_list (c, key, len, compute_ad_clicks (ad_id), Q_raw, 0, 0);
        }
        break;
      case 'd':
        if (len >= 7 && !memcmp (key + 3, "disable", 7) && sscanf (key + 10, "%u%n", &ad_id, &x) >= 1 && ad_id > 0 && x > 0 && !*parse_search_extras (key + 10 + x)) {
          register_long_query (key, len);
          int res = do_ad_disable (ad_id);
          complete_long_query (c, res);
          return return_one_key_list (c, key, len, res, Q_raw, 0, 0);
        } 
        break;
      case 'e':
        if (len >= 9 && !memcmp (key + 3, "enable", 6)) {
          if (sscanf (key + 9, "%u%n", &ad_id, &x) >= 1 && ad_id > 0 && x > 0) {
            int y = -1;
            if (key[x + 9] == '_' && sscanf (key + 9 + x + 1, "%u%n", &price, &y) >= 1 && price && y > 0) {
              x += y + 1;
            }
            if (!*parse_search_extras (key + 9 + x)) {
              register_long_query (key, len);
              int res = do_ad_price_enable (ad_id, price);
              complete_long_query (c, res);
              if (res < 0) {
                return res;
              }
              return return_one_key_list (c, key, len, res, Q_raw, 0, 0);
            }
          }
        }
        break;
      case 'i':
        if (len >= 7 && !memcmp (key + 3, "info", 4) && sscanf (key + 7, "%u%n", &ad_id, &x) >= 1 && ad_id > 0 && x > 0 && !*parse_search_extras (key + 7 + x)) {
          int res = compute_ad_info (ad_id, RR);
          if (res > 0) {
            return return_one_key_list_long (c, key, len, 0x7fffffff, Q_raw, RR, res);
          }
        }
        break;
      case 'l':
        if (len >= 16 && !memcmp (key + 3, "limited_views", 13) && sscanf (key + 16, "%u,%u%n", &ad_id, &price, &x) >= 2 && ad_id > 0 && x > 0 && (price == 100 || price == 0) && !*parse_search_extras (key + 16 + x)) {
          int res = do_ad_limit_user_views (ad_id, price);
          if (res < 0) {
            return res;
          }
          return return_one_key_list (c, key, len, res, Q_raw, 0, 0);
        }
        break;
      case 'm':
        if (len >= 8 && !memcmp (key + 3, "money", 5) && sscanf (key + 8, "%u%n", &ad_id, &x) >= 1 && ad_id > 0 && x > 0 && !*parse_search_extras (key + 8 + x)) {
          return return_one_key_list (c, key, len, compute_ad_money (ad_id), Q_raw, 0, 0);
        }
        break;
      case 'p':
        if (len >= 10 && !memcmp (key + 3, "pricing", 7) && sscanf (key + 10, "%u_%u%n", &price, &ad_id, &x) >= 2 && price > 0 && price < 2048 && ad_id > 0 && x > 0) {
          int max_users, y = -1, and_mask, xor_mask;
          if (key[x + 10] == '_' && sscanf (key + 10 + x, "_%d:%d%n", &and_mask, &xor_mask, &y) >= 2 && y >= 0) {
            x += y;
          } else {
            and_mask = 254;
            xor_mask = 0;
          }
          y = -1;
          if (key[x + 10] == ',' && sscanf (key + 10 + x, ",%u%n", &max_users, &y) >= 1 && y > 0 && max_users > 0 && max_users <= 1000) {
            x += y;
          } else {
            max_users = 50;
          }
          if (*parse_search_extras (key + 10 + x)) {
            break;
          }
          register_long_query (key, len);
          pricing_queries++;
          R_cnt = 0;
          int res = perform_ad_pricing (ad_id, price & 1023, (price & 1024) ? 8 : 0, and_mask, xor_mask, max_users);
          complete_long_query (c, res);
          return return_one_key_list (c, key, len, res, Q_raw, R, R_cnt);
        }
        break;
      case 'q':
        if (len >= 8 && !memcmp (key + 3, "query", 5) && sscanf (key + 8, "%u%n", &ad_id, &x) >= 1 && ad_id > 0 && x > 0) {
          char *ptr = get_ad_query (ad_id);
          if (ptr && (x = strlen (ptr)) <= 1000) {
            return return_one_key (c, key, ptr, x);
          }
        }
        break;
      case 'r':
        if (len >= 15 && !memcmp (key + 3, "recent_views", 12) && sscanf (key + 15, "%u%n", &ad_id, &x) >= 1 && ad_id > 0 && x > 0 && !*parse_search_extras (key + 15 + x)) {
          return return_one_key_list (c, key, len, compute_ad_recent_views (ad_id), Q_raw, 0, 0);
        }
        break;
      case 's':
        if (len >= 7 && !memcmp (key + 3, "sump", 4) && sscanf (key + 7, "%u%n", &ad_id, &x) >= 1 && ad_id > 0 && x > 0 && !*parse_search_extras (key + 7 + x)) {
          int res = compute_ad_ctr (ad_id, RR);
          if (res > 0) {
            long i, j;
            for (i = 2, j = 0; i < 5; i++) {
              R[j++] = RR[i] & ((1 << 20) - 1);
              RR[i] >>= 20;
              R[j++] = RR[i] & ((1 << 20) - 1);
              R[j++] = (RR[i] >> 20);
            }
            return return_one_key_list (c, key, len, 1, Q_raw, R, 9);
          }
        } else if (len >= 13 && !memcmp (key + 3, "setctrsump", 10)) {
          long long clicks, views, sump0, sump1, sump2 = -1;
          if (sscanf (key + 13, "%u:%lld,%lld,%lld,%lld,%lld%n", &ad_id, &clicks, &views, &sump0, &sump1, &sump2, &x) >= 3 && ad_id > 0 && clicks >= 0 && views > 0 && sump0 >= 0 && sump1 >= 0 && sump2 >= 0 && x > 0 && !*parse_search_extras (key + 13 + x)) {
            return return_one_key_list (c, key, len, do_set_ad_ctr_sump (ad_id, clicks, views, sump0, sump1, sump2), Q_raw, 0, 0);
          }
        } else if (len >= 9 && !memcmp (key + 3, "setctr", 6)) {
          long long clicks = -1, views = -1;
          if (sscanf (key + 9, "%u:%lld,%lld%n", &ad_id, &clicks, &views, &x) >= 3 && ad_id > 0 && clicks >= 0 && views > 0 && x > 0 && !*parse_search_extras (key + 9 + x)) {
            return return_one_key_list (c, key, len, do_set_ad_ctr (ad_id, clicks, views), Q_raw, 0, 0);
          }
        } else if (len >= 10 && !memcmp (key + 3, "setsump", 7)) {
          long long sump0, sump1, sump2 = -1;
          if (sscanf (key + 10, "%u:%lld,%lld,%lld%n", &ad_id, &sump0, &sump1, &sump2, &x) >= 3 && ad_id > 0 && sump0 >= 0 && sump1 >= 0 && sump2 >= 0 && x > 0 && !*parse_search_extras (key + 10 + x)) {
            return return_one_key_list (c, key, len, do_set_ad_sump (ad_id, sump0, sump1, sump2), Q_raw, 0, 0);
          }
        } else if (len >= 9 && !memcmp (key + 3, "setaud", 6)) {
          int aud = -1;
          if (sscanf (key + 9, "%u:%d%n", &ad_id, &aud, &x) >= 2 && ad_id > 0 && aud >= 0 && aud <= MAX_AD_AUD && x > 0 && !*parse_search_extras (key + 9 + x)) {
            return return_one_key_list (c, key, len, do_set_ad_aud (ad_id, aud), Q_raw, 0, 0);
          }
        } else if (len >= 8 && !memcmp (key + 3, "sites", 5)) {
          if (sscanf (key + 8, "%u,%u%n", &ad_id, &price, &x) >= 2 && ad_id > 0 && x > 0 && !(price & -0x100) && !*parse_search_extras (key + 8 + x)) {
            int res = do_ad_change_sites (ad_id, price);
            if (res < 0) {
              return res;
            }
            return return_one_key_list (c, key, len, res, Q_raw, 0, 0);
          }
        } else if (len >= 12 && !memcmp (key + 3, "setdomain", 9)) {
          int domain = -1;
          if (sscanf (key + 12, "%u:%d%n", &ad_id, &domain, &x) >= 2 && ad_id > 0 && domain >= 0 && domain <= MAX_AD_DOMAIN && x > 0 && !*parse_search_extras (key + 12 + x)) {
            int res = do_ad_set_domain (ad_id, domain);
            return res < 0 ? res : return_one_key_list (c, key, len, res, Q_raw, 0, 0);
          }
        } else if (len >= 11 && !memcmp (key + 3, "setgroup", 8)) {
          int group = -1 << 31;
          if (sscanf (key + 11, "%u:%d%n", &ad_id, &group, &x) >= 2 && ad_id > 0 && group != (-1 << 31) && x > 0 && !*parse_search_extras (key + 11 + x)) {
            int res = do_ad_set_group (ad_id, group);
            return res < 0 ? res : return_one_key_list (c, key, len, res, Q_raw, 0, 0);
          }
        } else if (len >= 16 && !memcmp (key + 3, "setcategories", 13)) {
	  int category = -1, subcategory = -1;
	  if (sscanf (key + 16, "%u:%d,%d%n", &ad_id, &category, &subcategory, &x) >= 3 && ad_id > 0 && category >= 0 && category <= MAX_AD_CATEGORY && subcategory >= 0 && subcategory <= MAX_AD_CATEGORY && x > 0 && !*parse_search_extras (key + 16 + x)) {
            int res = do_ad_set_categories (ad_id, category, subcategory);
            return res < 0 ? res : return_one_key_list (c, key, len, res, Q_raw, 0, 0);
          }
        } else if (len >= 12 && !memcmp (key + 3, "setfactor", 9)) {
          int factor = -1;
          if (sscanf (key + 12, "%u:%d%n", &ad_id, &factor, &x) >= 2 && ad_id > 0 && factor > 1e5 && factor <= 1e6 && x > 0 && !*parse_search_extras (key + 12 + x)) {
            int res = do_ad_set_factor (ad_id, factor);
            return res < 0 ? res : return_one_key_list (c, key, len, res, Q_raw, 0, 0);
          }
        }
        break;
      case 'v':
        if (len >= 19 && !memcmp (key + 3, "views_rate_limit", 16) && sscanf (key + 19, "%u,%u%n", &ad_id, &price, &x) >= 2 && ad_id > 0 && x > 0 && price >= 0 && !*parse_search_extras (key + 19 + x)) {
          int res = do_ad_limit_recent_views (ad_id, price);
          return res < 0 ? res : return_one_key_list (c, key, len, res, Q_raw, 0, 0);
        } else if (len >= 8 && !memcmp (key + 3, "views", 5) && sscanf (key + 8, "%u%n", &ad_id, &x) >= 1 && ad_id > 0 && x > 0 && !*parse_search_extras (key + 8 + x)) {
          return return_one_key_list (c, key, len, compute_ad_views (ad_id), Q_raw, 0, 0);
        } 
        break;
      }
    } else if (len >= 16 && !memcmp (key, "allocation_stats", 16)) {
      return return_one_key_list (c, key, len, MAX_RECORD_WORDS, 0, NewAllocations[0], MAX_RECORD_WORDS*4);
    }
    break;
  case 'd':
    if (len >= 11 && !memcmp (key, "deletegroup", 11) && sscanf (key + 11, "%d%n", &group_id, &x) >= 1 && group_id && x > 0 && !*parse_search_extras (key + 11 + x)) {
      return return_one_key_list (c, key, len, do_delete_group (group_id), Q_raw, 0, 0);
    }
    break;
  case 'f':
    if (len >= 5 && !memcmp (key, "flags", 5) && sscanf (key + 5, "%d", &user_id) == 1 && user_id > 0) {
      int res = get_has_photo (user_id);
      if (res >= 0) {
        return return_one_key (c, key, stats_buff, sprintf (stats_buff, "%d", res));
      }
    } else if (len >= 20 && !memcmp (key, "free_treespace_stats", 20)) {
      int x = AdSpace ? get_treespace_free_detailed_stats (AdSpace, (int *)stats_buff) : 0;
      int y = get_treespace_free_detailed_stats (WordSpace, (int *)(stats_buff + x * 4));
      assert ((x + y) < STATS_BUFF_SIZE / 4);  
      return return_one_key_list (c, key, len, x + y, 0, (int *)stats_buff, x + y);
    } else if (len >= 16 && !memcmp (key, "free_block_stats", 16)) {
      return return_one_key_list (c, key, len, MAX_RECORD_WORDS, 0, FreeCnt, MAX_RECORD_WORDS);
    }
    break;
  case 'g':
    if (len >= 18 && !memcmp (key, "global_click_stats", 18)) {
      return return_one_key_list_long (c, key, len, MAX_AD_VIEWS, 0, (long long *) AdStats.g, MAX_AD_VIEWS * 2);
    }
    break;
  case 'l':
    if (len >= 16 && !memcmp (key, "long_query_stats", 16)) {
      return return_one_key (c, key, stats_buff, prepare_long_query_stats ());
    } else if (len >= 17 && !memcmp (key, "local_click_stats", 17)) {
      return return_one_key_list_long (c, key, len, MAX_AD_VIEWS, 0, (long long *) AdStats.l, MAX_AD_VIEWS * 2);
    }
    break;
  case 'p':
    if (len >= 6 && len <= 1020 && !memcmp (key, "prices", 6) && sscanf (key + 6, "%u%n", &price, &x) >= 1 && price > 0 && x > 0) {
      int y = -1, and_mask, xor_mask;
      if (key[6 + x] == '_' && sscanf (key + 6 + x, "_%d:%d%n", &and_mask, &xor_mask, &y) >= 2 && y >= 0) {
        x += y;
      } else {
        and_mask = 254;
        xor_mask = 0;
      }
      const char *cur = parse_search_extras (key + 6 + x);
      if (*cur == '(') {
        register_long_query (key, len);
        char *q_end, *q_tmp;
        if (load_temp_data (c) > 0 && cur[1] >= '0' && cur[1] <= '9' && strtol (cur + 1, &q_tmp, 10) == tag_id) {
          q_end = compile_query (value_buff);
          if (!q_end) {
            Qs = q_tmp;
          }
        } else {
          q_end = compile_query ((char *)(cur + 1));
        }
        assert (cur == parse_search_extras (key + 6 + x));
        if (!q_end && !parse_search_query_termination (Qs)) {
          q_end = Qs;
        }
        if (q_end) {
          complete_long_query (c, -1);
          return return_one_key (c, key, stats_buff, sprintf (stats_buff, "ERROR near '%.256s'\n", q_end));
        } else {
          pricing_queries++;
          R_cnt = 0;
          int res = perform_pricing (price & 1023, (price & (1024)) >> (10 - 3), and_mask, xor_mask); // 1024 -> 8
          complete_long_query (c, res);
          return return_one_key_list (c, key, len, res, Q_raw, R, R_cnt);
        }
      }
    }
    break;
  case 'r':
    if (len >= 4 && !memcmp (key, "rate", 4) && sscanf (key + 4, "%d", &user_id) == 1 && user_id > 0) {
      int rate = get_user_rate (user_id);
      if (rate != (-1 << 31)) {
        return return_one_key (c, key, stats_buff, sprintf (stats_buff, "%d", rate));
      }
    } else if (len >= 18 && !memcmp (key, "recent_views_stats", 18) && !*parse_search_extras (key + 18)) {
      int res = compute_recent_views_stats ();
      return return_one_key_list (c, key, len, res, Q_raw, R, R_cnt);
    } else if (len >= 17 && !memcmp (key, "recent_ad_viewers", 17) && sscanf (key + 17, "%d%n", &ad_id, &x) >= 1 && ad_id > 0 && x > 0 && !*parse_search_extras (key + 17 + x)) {
      int res = compute_recent_ad_viewers (ad_id);
      return return_one_key_list (c, key, len, res, Q_raw, R, R_cnt);
    } else if (len >= 15 && !memcmp (key, "recent_user_ads", 15) && sscanf (key + 15, "%d%n", &user_id, &x) >= 1 && user_id > 0 && x > 0 && !*parse_search_extras (key + 15 + x)) {
      int res = compute_recent_user_ads (user_id);
      return return_one_key_list (c, key, len, res, Q_raw, R, R_cnt);
    }
    break;
  case 's':
    if (len >= 7 && len <= 1020 && !memcmp (key, "search", 6)) {
      const char *cur = parse_search_extras (key + 6);
      if (*cur == '(') {
        if (!strncmp (cur + 1, "one", 3) && parse_search_query_termination (cur + 4)) {
          return return_one_key_list (c, key, len, 1, Q_raw, 0, 0);
        }
        register_long_query (key, len);
        char *q_end, *q_tmp;
        if (load_temp_data (c) > 0 && cur[1] >= '0' && cur[1] <= '9' && strtol (cur + 1, &q_tmp, 10) == tag_id) {
          q_end = compile_query (value_buff);
          if (!q_end) {
            Qs = q_tmp;
          }
        } else {
          q_end = compile_query ((char *)(cur + 1));
        }
        assert (cur == parse_search_extras (key + 6));
        if (!q_end && !parse_search_query_termination (Qs)) {
          q_end = Qs;
        }
        if (q_end) {
          complete_long_query (c, -1);
          return return_one_key (c, key, stats_buff, sprintf (stats_buff, "ERROR near '%.256s'\n", q_end));
        } else {
          search_queries++;
          R_cnt = 0;
          int res = perform_query (0);
          complete_long_query (c, res);
          return return_one_key_list (c, key, len, res, Q_raw, R, R_cnt);
        }
      }
    } else if (len >= 5 && !memcmp (key, "stats", 5)) {
      return return_one_key (c, key, stats_buff, targ_prepare_stats(c));
    } else if (len >= 18 && !memcmp (key, "search_query_stats", 18)) {
      return return_one_key (c, key, stats_buff, prepare_long_query_stats ());
    } else if (len >= 17 && !memcmp (key, "split_block_stats", 17)) {
      return return_one_key_list (c, key, len, MAX_RECORD_WORDS, 0, SplitBlocks, MAX_RECORD_WORDS);
    }
    break;
  case 't':
    if (len >= 6 && len <= 1020 && !memcmp (key, "target", 6) && sscanf (key + 6, "%u_%d%n", &ad_id, &price, &x) >= 2 && ad_id > 0 && x > 0) {
      int factor = 0;
      if (key[6+x] == '-') {
        char *nptr;
        factor = strtol (key + 7 + x, &nptr, 10);
        if (factor < 0 || factor >= 90 || nptr == key + 7 + x || *nptr != '%') {
          break;
        }
        x = nptr - key - 6 + 1;
        factor = (100 - factor) * 10000;
      }
      const char *cur = parse_search_extras (key + 6 + x), *q_start = cur + 1;
      if (*cur == '(') {
        register_long_query (key, len);
        char *q_end, *q_tmp;
        if (load_temp_data (c) > 0 && cur[1] >= '0' && cur[1] <= '9' && strtol (cur + 1, &q_tmp, 10) == tag_id) {
          aux_userlist_tag = 0;
          q_end = compile_query (value_buff);
          if (!q_end) {
            Qs = q_tmp;
          }
          q_start = value_buff;
        } else {
          aux_userlist_tag = 0;
          q_end = compile_query ((char *)(cur + 1));
        }
        assert (cur == parse_search_extras (key + 6 + x));
        if (!q_end && !parse_search_query_termination (Qs)) {
          q_end = Qs;
        }
        if (q_end) {
          complete_long_query (c, -1);
          return return_one_key (c, key, stats_buff, sprintf (stats_buff, "ERROR near '%.256s'\n", q_end));
        } else {
          char sav = *Qs;
          char *sptr = Qs;
          *Qs = 0;
          targeting_queries++;
          int res = do_perform_targeting (ad_id, price, factor, (char *) q_start);
          *sptr = sav;
          complete_long_query (c, res);
          if (res < 0) {
            return res;
          }
          return return_one_key_list (c, key, len, res, Q_raw, 0, 0);
        }
      }
      break;
    }
    if (len >= 13 && !memcmp (key, "targ_audience", 13) && sscanf (key + 13, "%u_%d%n", &ad_id, &price, &x) >= 2 && ad_id > 0 && price > 0 && x > 0) {
      int y = -1, and_mask, xor_mask;
      if (key[x + 13] == '_' && sscanf (key + 13 + x, "_%d:%d%n", &and_mask, &xor_mask, &y) >= 2 && y >= 0) {
        x += y;
      } else {
        and_mask = 254;
        xor_mask = 0;
      }

      int res = prepare_multiple_query_query (c, key + 13 + x, key, len);
      if (res <= 0) {
        return res;
      }
      targ_audience_queries++;
      R_cnt = 0;
      res = perform_targ_audience_query (ad_id, price, and_mask, xor_mask);
      complete_long_query (c, res);
      return return_one_key_list (c, key, len, res, Q_raw, R, R_cnt);
    }

    break;
  case 'u':
    if (len >= 8 && !memcmp (key, "user_ads", 8)) {
      int limit = 32, flags = 0, and_mask = 254, xor_mask = 0;
      long long cat_mask = -1LL;
      if (sscanf (key, "user_ads%u#%d,%d_%d:%d_%lld", &user_id, &limit, &flags, &and_mask, &xor_mask, &cat_mask) >= 1 && user_id > 0) {
        int res = prepare_user_ads (user_id, limit, flags, and_mask, xor_mask, stats_buff, cat_mask);
        if (res >= 0) {
          return return_one_key_list (c, key, len, 0x7fffffff, 0, R, res);
        }
      }
    } else if (len >= 9 && !memcmp (key, "user_view", 9) && sscanf (key + 9, "%u,%u%n", &user_id, &ad_id, &x) >= 2 && user_id > 0 && ad_id > 0 && x > 0 && !*parse_search_extras (key + 9 + x)) {
      int res = do_register_user_view (user_id, ad_id);
      if (res < 0) {
        return res;
      }
      return return_one_key_list (c, key, len, res, Q_raw, 0, 0);
    } else if (len >= 15 && !memcmp (key, "user_clicked_ad", 15) && sscanf (key + 15, "%u,%u%n", &user_id, &ad_id, &x) >= 2 && user_id > 0 && ad_id > 0 && x > 0 && !*parse_search_extras (key + 15 + x)) {
      int res = compute_ad_user_clicked (user_id, ad_id);
      return return_one_key_list (c, key, len, res, Q_raw, 0, 0);
    } else if (len >= 10 && !memcmp (key, "user_click", 10) && sscanf (key + 10, "%u,%u,%d%n", &user_id, &ad_id, &price, &x) >= 3 && user_id > 0 && ad_id > 0 && x > 0 && !*parse_search_extras (key + 10 + x)) {
      int res = do_register_user_click (user_id, ad_id, price);
      if (res < 0) {
        return res;
      }
      return return_one_key_list (c, key, len, res, Q_raw, 0, 0);
    } else if (len >= 11 && !memcmp (key, "user_groups", 11) && sscanf (key + 11, "%u%n", &user_id, &x) >= 1 && user_id > 0 && x > 0 && !*parse_search_extras (key + 11 + x)) {
      int res = get_user_groups (user_id);
      if (res < 0) {
        return res;
      }
      return return_one_key_list (c, key, len, res, Q_raw, R, R_cnt);
    } else if (len >= 16 && !memcmp (key, "used_block_stats", 16)) {
      return return_one_key_list (c, key, len, MAX_RECORD_WORDS, 0, UsedCnt, MAX_RECORD_WORDS);
    }
    break;
  } 

  return 0;
}

int memcache_get (struct connection *c, const char *key, int key_len) {
  if (verbosity > 1) {
    fprintf (stderr, "memcache_get: key='%s'\n", key);
  }

  WaitAioArrClear ();

  int res = memcache_get_nonblock (c, key, key_len);

  if (res == -2) {
    return memcache_wait (c, key, key_len);
  }

  return res;
}

int parse_int_list (char *text, int text_len) {
  int i = 0;
  long x;
  char *ptr = text, *ptr_e = text + text_len, *ptr_n;
  while (ptr < ptr_e) {
    if (i && *ptr++ != ',') {
      return -1;
    }
    R[i++] = x = strtol (ptr, &ptr_n, 10);
    if (ptr == ptr_n || i == MAX_USERS || x <= 0 || x >= 0x7fffffff) {
      return -1;
    }
    ptr = ptr_n;
  }
  return i;
}

int parse_signed_int_list (char *text, int text_len) {
  int i = 0;
  long x;
  char *ptr = text, *ptr_e = text + text_len, *ptr_n;
  while (ptr < ptr_e) {
    if (i && *ptr++ != ',') {
      return -1;
    }
    R[i++] = x = strtol (ptr, &ptr_n, 10);
    if (ptr == ptr_n || i == MAX_USERS || x <= -0x7fffffff || x >= 0x7fffffff) {
      return -1;
    }
    ptr = ptr_n;
  }
  return i;
}

typedef int (*do_set_custom_func)(int user_id, int val);

const static do_set_custom_func do_set_custom[15] = {
  do_set_custom1,
  do_set_custom2,
  do_set_custom3,
  do_set_custom4,
  do_set_custom5,
  do_set_custom6,
  do_set_custom7,
  do_set_custom8,
  do_set_custom9,
  do_set_custom10,
  do_set_custom11,
  do_set_custom12,
  do_set_custom13,
  do_set_custom14,
  do_set_custom15,
};

int mcs_targ_store (struct connection *c, int op, const char *key, int len, int flags, int expire, int bytes)
{
  int user_id = -1, type = 0, group_id = 0, temp_tag = -1;
  int rate, cute, x, y, z;
  static struct company                C ;
  static struct address                A ;
  static struct school                S ;
  static struct education        E ;

  free_tmp_buffers (c);

  vkprintf (2, "memcache_store: key='%s', key_len=%d, value_len=%d\n", key, len, bytes);
  
  if ((unsigned) bytes >= sizeof (value_buff) - 1024 || op != mct_set) {
    return -2;
  }

  assert (read_in (&c->In, value_buff, bytes) == bytes);
  value_buff[bytes] = 0;

  minor_update_queries++;

  switch (key[0]) {
  case 'a':
    if (sscanf (key, "address%d ", &user_id) == 1 && user_id > 0 && bytes < 256) {
      x = 0;
      if (sscanf (value_buff, "%hhu:%hhu,%d,%d,%d,%d%n", 
                  &A.atype, &A.country, &A.city, &A.district, &A.station, &A.street, &x) >= 6 && x > 0 && value_buff[x] == 9) {
        y = ++x;
        while (value_buff[y] && value_buff[y] != 9) { y++; }
        A.name = 0;
        if (value_buff[y]) {
          A.name = value_buff + y + 1;
          value_buff[y] = 0;
        }
        A.house = value_buff + x;
        return do_set_address (user_id, &A);
      }
    } else if (sscanf (key, "alcohol%d ", &user_id) == 1 && user_id > 0) {
      return do_set_alcohol (user_id, atol (value_buff));
    }
    break;
  case 'b':
    if (sscanf (key, "browser%d ", &user_id) == 1 && user_id > 0) {
      return do_set_browser (user_id, atol (value_buff));
    } else if (sscanf (key, "birthday%d ", &user_id) == 1 && user_id > 0) {
      if (sscanf (value_buff, "%d/%d/%d ", &x, &y, &z) == 3) {
        return do_set_birthday (user_id, x, y, z);
      }
    }
    break;
  case 'c':
    if (sscanf (key, "company%d ", &user_id) == 1 && user_id > 0 && bytes < 256) {
      x = 0;
      if (sscanf (value_buff, "%hhu,%d,%d,%hd,%hd%n", 
                  &C.country, &C.city, &C.company, &C.start, &C.finish, &x) >= 5 && x > 0) {
        if (value_buff[x] != 9) {
          break;
        }
        y = ++x;
        while (value_buff[y] && value_buff[y] != 9) { 
          y++; 
        }
        C.company_name = 0;
        if (value_buff[y]) {
          C.company_name = value_buff + y + 1;
          value_buff[y] = 0;
        }
        C.job = value_buff + x;
        return do_set_work (user_id, &C);
      }
    } else if (sscanf (key, "countries_visited%d ", &user_id) == 1 && user_id > 0) {
      return do_set_cvisited (user_id, atol (value_buff));
    } else if (sscanf (key, "city%d ", &user_id) == 1 && user_id > 0) {
      if (sscanf (value_buff, "%d,%d ", &x, &y) == 2) {
        return do_set_country_city (user_id, x, y);
      }
    } else if (sscanf (key, "cute%d ", &user_id) == 1 && user_id > 0) {
      return do_set_cute (user_id, atol (value_buff));
    } else if (sscanf (key, "custom%d_%d ", &type, &user_id) == 2 && user_id > 0 && type > 0 && type < 16) {
      return do_set_custom[type - 1] (user_id, atol (value_buff));
    }
    break;
  case 'e':
    if (sscanf (key, "education%d ", &user_id) == 1 && user_id > 0) {
      E.primary = 0;
      if (sscanf (value_buff, "%hhu,%d,%d,%d,%d,%d,%hhd,%hhd,%hhd ", 
                  &E.country, &E.city, &E.university, &E.faculty, &E.chair, &E.grad_year, &E.edu_form, &E.edu_status, &E.primary) >= 8) {
        return do_set_education (user_id, &E);
      }
    }
    break;
  case 'f':
    if (sscanf (key, "flags%d ", &user_id) == 1 && user_id > 0) {
      return do_set_has_photo (user_id, atol (value_buff));
    }
    break;
  case 'g':
    if (sscanf (key, "gcountry%d ", &user_id) == 1 && user_id > 0) {
      return do_set_gcountry (user_id, atol (value_buff));
    } else if (len >= 18 && !memcmp (key, "global_click_stats", 18)) {
      static long long A[MAX_AD_VIEWS*2], *q = A;
      char *p = value_buff, *p1, *pe = value_buff + bytes;
      while (p < pe && q < A + MAX_AD_VIEWS*2) {
        *q = strtoll (p, &p1, 10);
        if (p1 == p) {
          break;
        }
        q++;
        p = p1;
        if (*p != ',') {
          break;
        }
        p++;
      }
      if (p != pe || q != A + MAX_AD_VIEWS * 2) {
        break;
      }
      return do_set_global_click_stats (MAX_AD_VIEWS, (struct views_clicks_ll *) A);
    }
  case 'h':
    if (sscanf (key, "hometown%d", &user_id) >= 1 && user_id > 0 && bytes < 256) {
      return do_set_hometown (user_id, value_buff, bytes);
    } else if (sscanf (key, "height%d ", &user_id) == 1 && user_id > 0) {
      return do_set_height (user_id, atol (value_buff));
    } else if (sscanf (key, "hidden%d", &user_id) == 1 && user_id > 0) {
      return do_set_hidden (user_id, atol (value_buff));
    }
    break;
  case 'i':
    if (sscanf (key, "interests%d#%d ", &user_id, &type) >= 2 && user_id > 0 && type > 0 && type <= MAX_INTERESTS) {
      return do_set_interest (user_id, value_buff, bytes, type);
    } else if (sscanf (key, "important_in_others%d ", &user_id) == 1 && user_id > 0) {
      return do_set_iiothers (user_id, atol (value_buff));
    }
    break;
  case 'm':
    if (sscanf (key, "mstatus%d ", &user_id) == 1 && user_id > 0) {
      return do_set_mstatus (user_id, atol (value_buff));
    } else if (sscanf (key, "military%d ", &user_id) == 1 && user_id > 0) {
      if (sscanf (value_buff, "%d,%d,%d ", &x, &y, &z) == 3) {
        return do_set_military (user_id, x, y, z);
      }
    }
    break;
  case 'o':
    if (sscanf (key, "operator%d ", &user_id) == 1 && user_id > 0) {
      return do_set_operator (user_id, atol (value_buff));
    }
    break;
  case 'p':
    if (sscanf (key, "personal_priority%d ", &user_id) == 1 && user_id > 0) {
      return do_set_ppriority (user_id, atol (value_buff));
    } else if (sscanf (key, "political%d ", &user_id) == 1 && user_id > 0) {
      return do_set_political (user_id, atol (value_buff));
    } else if (sscanf (key, "privacy%d ", &user_id) == 1 && user_id > 0) {
      return do_set_privacy (user_id, atol (value_buff));
    } else if (sscanf (key, "proposal%d", &user_id) >= 1 && user_id > 0 && bytes < 1024) {
      return do_set_proposal (user_id, value_buff, bytes);
    }
    break;
  case 'r':
    if (sscanf (key, "rates%d ", &user_id) == 1 && user_id > 0) {
      if (sscanf (value_buff, "%d,%d ", &rate, &cute) == 2) {
        return do_set_rate_cute (user_id, rate, cute);
      }
    } else if (sscanf (key, "rate%d ", &user_id) == 1 && user_id > 0) {
      return do_set_rate (user_id, atol (value_buff));
    } else if (sscanf (key, "region%d ", &user_id) == 1 && user_id > 0) {
      return do_set_region (user_id, atol (value_buff));
    } else if (sscanf (key, "religion%d ", &user_id) == 1 && user_id > 0) {
      return do_set_religion (user_id, value_buff, bytes);
    }
    break;
  case 's':
    if (sscanf (key, "school%d ", &user_id) == 1 && user_id > 0 && bytes < 256) {
      x = -1;
      if (sscanf (value_buff, "%hhu,%d,%d,%hd,%hd,%hd,%hhd,%hhd%n", 
                  &S.country, &S.city, &S.school, &S.start, &S.finish, &S.grad, &S.sch_class, &S.sch_type, &x) >= 8) {
        S.spec = 0;
        if (x > 0) {
          while (value_buff[x] == '\t' || value_buff[x] == ' ') {
            x++;
          }
          if (x < bytes) {
            S.spec = value_buff + x;
          }
        }
        return do_set_school (user_id, &S);
      }
    } else if (sscanf (key, "smoking%d ", &user_id) == 1 && user_id > 0) {
      return do_set_smoking (user_id, atol (value_buff));
    } else if (sscanf (key, "sex%d ", &user_id) == 1 && user_id > 0) {
      return do_set_sex (user_id, atol (value_buff));
    }
    break;
  case 't':
    if (sscanf (key, "temp%d", &temp_tag) == 1 && temp_tag > 0) {
      if (!c->Tmp) {
        c->Tmp = alloc_head_buffer ();
        assert (c->Tmp);
      }
      write_out (c->Tmp, &temp_tag, 4);
      write_out (c->Tmp, value_buff, bytes);
      vkprintf (2, "stored temp query with tag %d, length %d: %.100s\n", temp_tag, bytes, value_buff); 
      return 1;
    } else if (sscanf (key, "timezone%d ", &user_id) == 1 && user_id > 0) {
      return do_set_timezone (user_id, atol (value_buff));
    }
    break;
  case 'u':
    if (sscanf (key, "user_group_types%d", &user_id) >= 1 && user_id > 0) {
      static unsigned user_group_types[4];
      memset (user_group_types, 0, sizeof (user_group_types));
      char *ptr = value_buff;
      while (*ptr) {
        char *eptr;
        unsigned x = strtoul (ptr, &eptr, 10);
        if (eptr == ptr || x > 127) {
          break;
        }
        user_group_types[x >> 5] |= (1 << (x & 31));
        ptr = eptr;
        if (*ptr != ',') {
          break;
        }
        ++ptr;
      }
      if (!*ptr) {
        return do_set_user_group_types (user_id, user_group_types);
      }
    } else if (sscanf (key, "user_group%d_%d ", &user_id, &group_id) >= 2 && user_id > 0 && group_id != 0) {
      return do_set_user_group (user_id, group_id);
    } else if (sscanf (key, "user_single_group_type%d", &user_id) >= 1 && user_id > 0) {
      return do_set_user_single_group_type (user_id, atol (value_buff));
    } else if (sscanf (key, "user_groups_add%d", &user_id) >= 1 && user_id > 0) {
      x = parse_signed_int_list (value_buff, bytes);
      if (x > 0) {
        return do_add_user_groups (user_id, R, x);
      }
    } else if (sscanf (key, "user_groups_set%d", &user_id) >= 1 && user_id > 0) {
      x = parse_signed_int_list (value_buff, bytes);
      if (x > 0) {
        return do_set_user_groups (user_id, R, x);
      }
    } else if (sscanf (key, "user_groups_del%d", &user_id) >= 1 && user_id > 0) {
      x = parse_signed_int_list (value_buff, bytes);
      if (x > 0) {
        return do_del_user_groups (user_id, R, x);
      }
    } else if (sscanf (key, "user_lang%d_%d ", &user_id, &group_id) >= 2 && user_id > 0) {
      return do_set_user_lang (user_id, group_id);
    } else if (sscanf (key, "username%d ", &user_id) == 1 && user_id > 0 && bytes < 256) {
      return do_set_username (user_id, value_buff, bytes);
    }
    break;
  case 'v':
    if (len >= 9 && !memcmp (key, "verbosity", 9) && bytes == 1 && value_buff[0] >= '0' && value_buff[0] <= '9') {
      verbosity = value_buff[0] - '0';
      return 1;
    } else if (sscanf (key, "visit%d ", &user_id) == 1 && user_id > 0 && bytes < 256) {
      return do_user_visit (user_id, value_buff, bytes);
    }
    break;
  case 'x':
    if (sscanf (key, "xtemp%d", &temp_tag) == 1 && temp_tag < 0) {
      if (!c->Tmp) {
        c->Tmp = alloc_head_buffer ();
        assert (c->Tmp);
      }
      if ((bytes & 3) || !bytes || *(int *) value_buff != 0x30303030) {
        x = parse_int_list (value_buff, bytes);
        if (x < 0) {
          return 0;
        }
        write_out (c->Tmp, &temp_tag, 4);
        write_out (c->Tmp, R, x*4);
        vkprintf (2, "stored xtemp userlist with tag %d, %d users\n", temp_tag, x); 
      } else {
        write_out (c->Tmp, &temp_tag, 4);
        write_out (c->Tmp, value_buff + 4, bytes - 4);
        vkprintf (2, "stored binary xtemp userlist with tag %d, %d users\n", temp_tag, (bytes >> 2) - 1); 
      }
      return 1;
    }
    break;
  }

  minor_update_queries--; // unrecognized

  return 0;
}

int mcs_targ_delete (struct connection *c, const char *str, int len) {
  int user_id = -1, group_id = 0, lang_id = -1, type = 0, res = 0;

  vkprintf (2, "delete \"%s\"\n", str);
  free_tmp_buffers (c);
  
  switch (*str) {
  case 'a':
    if (sscanf (str, "address%d ", &user_id) == 1) {
      res = do_delete_addresses (user_id);
    }
    break;
  case 'c':
    if (sscanf (str, "company%d ", &user_id) == 1) {
      res = do_delete_work (user_id);
    }
    break;
  case 'e':
    if (sscanf (str, "education%d ", &user_id) == 1) {
      res = do_delete_education (user_id);
    }
    break;
  case 'i':
    if (sscanf (str, "interests%d#%d ", &user_id, &type) >= 1) {
      res = do_delete_interests (user_id, type);
    }
    break;
  case 'm':
    if (sscanf (str, "military%d ", &user_id) == 1) {
      res = do_delete_military (user_id);
    }
    break;
  case 'p':
    if (sscanf (str, "proposal%d ", &user_id) == 1) {
      res = do_delete_proposal (user_id);
    }
    break;
  case 's':
    if (sscanf (str, "school%d ", &user_id) == 1) {
      res = do_delete_schools (user_id);
    }
    break;
  case 'u':
    if (sscanf (str, "user_group%d_%d ", &user_id, &group_id) == 2) {
      res = do_delete_user_group (user_id, group_id);
    } else if (sscanf (str, "user_langs%d ", &user_id) == 1) {
      res = do_delete_langs (user_id);
    } else if (sscanf (str, "user_lang%d_%d ", &user_id, &lang_id) == 2) {
      res = do_delete_user_lang (user_id, lang_id);
    } else if (sscanf (str, "user_groups%d ", &user_id) == 1) {
      res = do_delete_groups (user_id);
    } else if (sscanf (str, "user_groups_positive%d ", &user_id) == 1) {
      res = do_delete_positive_groups (user_id);
    } else if (sscanf (str, "user_groups_negative%d ", &user_id) == 1) {
      res = do_delete_negative_groups (user_id);
    } else if (sscanf (str, "user%d ", &user_id) == 1) {
      res = do_delete_user (user_id);
    }
    break;
  }

  if (!res) {
    write_out (&c->Out, "NOT_FOUND\r\n", 11);
  } else {
    write_out (&c->Out, "DELETED\r\n", 9);
  }
  return 0;
}

int mcs_targ_stats (struct connection *c) {
  write_out (&c->Out, stats_buff, targ_prepare_stats (c));
  return 0;
}

int targ_engine_wakeup (struct connection *c) {
  struct mcs_data *D = MCS_DATA(c);

  if (verbosity > 1) {
    fprintf (stderr, "targ_engine_wakeup (%p)\n", c);
  }

  c->generation = ++conn_generation;
  c->pending_queries = 0;
  c->status = conn_reading_query;
  assert (D->query_type == mct_get_resume /* || D->query_type == mct_replace_resume */);
  clear_connection_timeout (c);

  if (c->Out.total_bytes > 0) {
    c->flags |= C_WANTWR;
  }

  return 0;
}


int targ_engine_alarm (struct connection *c) {
  if (verbosity > 1) {
    fprintf (stderr, "targ_engine connection %d timeout alarm, %d queries pending, status=%d\n", c->fd, c->pending_queries, c->status);
  }
  assert (c->status == conn_wait_aio);
  c->flags |= C_INTIMEOUT;
  return targ_engine_wakeup (c);
}



void targ_stats (void) {
  targ_prepare_stats (0);
  tl_store_stats (stats_buff, 0);
}

int tl_parse_ad_id (void) {
  return tl_fetch_positive_int ();
}

long long tl_parse_sump0 (void) {
  return tl_fetch_nonnegative_long ();
}

long long tl_parse_sump1 (void) {
  return tl_fetch_positive_long ();
}

long long tl_parse_sump2 (void) {
  return tl_fetch_positive_long ();
}

long long tl_parse_clicks (void) {
  return tl_fetch_nonnegative_long ();
}

long long tl_parse_views (void) {
  return tl_fetch_positive_long ();
}

int tl_parse_aud (void) {
  return tl_fetch_int_range (0, MAX_AD_AUD);
}

int tl_parse_max_views (void) {
  int x = tl_fetch_int ();
  if (x != 0 && x != 100) {
    tl_fetch_set_error_format (TL_ERROR_VALUE_NOT_IN_RANGE, "max_views should be 0 or 100 (%d presented)", x);
  }
  return x;
}

int tl_parse_uid (void) {
  int x = tl_fetch_positive_int ();
  if (x > 0) {
    if (conv_user_id (x) < 0) {
      tl_fetch_set_error_format (TL_ERROR_VALUE_NOT_IN_RANGE, "user_id is not from this engine: log_split_min = %d, log_split_max = %d, log_split_mod = %d, uid = %d", log_split_min, log_split_max, log_split_mod, x);
      x = -1;
    }
  }
  return x;
}

int convert_mode (int mode) {
  Q_order = 0;
  if (mode & 1) {
    Q_order = 2;
  } else if (mode & 2) {
    Q_order = 1;
  }
  if (!(mode & 4)) {
    Q_order = -Q_order;
  }
  return Q_order;
}

TL_DO_FUN(ad_enable)
  int res = do_ad_price_enable (e->ad, e->price);
  if (res == -2) { return -2; }
  tl_store_int (TL_BOOL_STAT);
  tl_store_int (res > 0);
  tl_store_int (res <= 0);
  tl_store_int (0);
TL_DO_FUN_END

TL_PARSE_FUN(ad_enable, int price)
  e->ad = tl_parse_ad_id ();
  if (price) {
    e->price = tl_fetch_int ();
  } else {
    e->price = 0;
  }
TL_PARSE_FUN_END

TL_DO_FUN(ad_disable)
  int res = do_ad_disable (e->ad);
  if (res == -2) { return -2; }
  tl_store_int (TL_BOOL_STAT);
  tl_store_int (res > 0);
  tl_store_int (res <= 0);
  tl_store_int (0);
TL_DO_FUN_END

TL_PARSE_FUN(ad_disable)
  e->ad = tl_parse_ad_id ();
TL_PARSE_FUN_END

TL_DO_FUN(ad_set_ctr)
  int res = do_set_ad_ctr (e->ad, e->clicks, e->views);
  if (res == -2) { return -2; }
  tl_store_int (TL_BOOL_STAT);
  tl_store_int (res > 0);
  tl_store_int (res <= 0);
  tl_store_int (0);
TL_DO_FUN_END

TL_PARSE_FUN(ad_set_ctr)
  e->ad = tl_parse_ad_id ();
  e->clicks = tl_parse_clicks ();
  e->views = tl_parse_views ();
TL_PARSE_FUN_END

TL_DO_FUN(ad_set_sump)
  int res = do_set_ad_sump (e->ad, e->sump0, e->sump1, e->sump2);
  if (res == -2) { return -2; }
  tl_store_int (TL_BOOL_STAT);
  tl_store_int (res > 0);
  tl_store_int (res <= 0);
  tl_store_int (0);
TL_DO_FUN_END

TL_PARSE_FUN(ad_set_sump)
  e->ad = tl_parse_ad_id ();
  e->sump0 = tl_parse_sump0 ();
  e->sump1 = tl_parse_sump1 ();
  e->sump2 = tl_parse_sump2 ();
TL_PARSE_FUN_END

TL_DO_FUN(ad_set_ctr_sump)
  int res = do_set_ad_ctr_sump (e->ad, e->clicks, e->views, e->sump0, e->sump1, e->sump2);
  if (res == -2) { return -2; }
  tl_store_int (TL_BOOL_STAT);
  tl_store_int (res > 0);
  tl_store_int (res <= 0);
  tl_store_int (0);
TL_DO_FUN_END

TL_PARSE_FUN(ad_set_ctr_sump)
  e->ad = tl_parse_ad_id ();
  e->clicks = tl_parse_clicks ();
  e->views = tl_parse_views ();
  e->sump0 = tl_parse_sump0 ();
  e->sump1 = tl_parse_sump1 ();
  e->sump2 = tl_parse_sump2 ();
TL_PARSE_FUN_END

TL_DO_FUN(ad_set_aud)
  int res = do_set_ad_aud (e->ad, e->aud);
  if (res == -2) { return -2; }
  tl_store_int (TL_BOOL_STAT);
  tl_store_int (res > 0);
  tl_store_int (res <= 0);
  tl_store_int (0);
TL_DO_FUN_END

TL_PARSE_FUN(ad_set_aud)
  e->ad = tl_parse_ad_id ();
  e->aud = tl_parse_aud ();
TL_PARSE_FUN_END

TL_DO_FUN(ad_limited_views)
  int res = do_ad_limit_user_views (e->ad, e->max_views);
  if (res == -2) { return -2; }
  tl_store_int (TL_BOOL_STAT);
  tl_store_int (res > 0);
  tl_store_int (res <= 0);
  tl_store_int (0);
TL_DO_FUN_END

TL_PARSE_FUN(ad_limited_views)
  e->ad = tl_parse_ad_id ();
  e->max_views = tl_parse_max_views ();
TL_PARSE_FUN_END

TL_DO_FUN(ad_views_rate_limit)
  int res = do_ad_limit_recent_views (e->ad, e->rate_limit);
  if (res == -2) { return -2; }
  tl_store_int (TL_BOOL_STAT);
  tl_store_int (res > 0);
  tl_store_int (res <= 0);
  tl_store_int (0);
TL_DO_FUN_END

TL_PARSE_FUN(ad_views_rate_limit)
  e->ad = tl_parse_ad_id ();
  e->rate_limit = tl_fetch_nonnegative_int ();
TL_PARSE_FUN_END

TL_DO_FUN(ad_sites)
        int res = do_ad_change_sites (e->ad, e->mask);
  if (res == -2) { return -2; }
  tl_store_int (TL_BOOL_STAT);
  tl_store_int (res > 0);
  tl_store_int (res <= 0);
  tl_store_int (0);
TL_DO_FUN_END

TL_PARSE_FUN(ad_sites)
  e->ad = tl_parse_ad_id ();
  e->mask = tl_fetch_int_range (0, 255);
TL_PARSE_FUN_END

TL_DO_FUN(ad_set_factor)
  int res = do_ad_set_factor (e->ad, e->factor);
  if (res == -2) { return -2; }
  tl_store_int (TL_BOOL_STAT);
  tl_store_int (res > 0);
  tl_store_int (res <= 0);
  tl_store_int (0);
TL_DO_FUN_END

TL_PARSE_FUN(ad_set_factor)
  e->ad = tl_parse_ad_id ();
  e->factor = tl_fetch_int_range (1e5 + 1, 1e6);
TL_PARSE_FUN_END

TL_DO_FUN(ad_set_domain)
  int res = do_ad_set_domain (e->ad, e->domain);
  if (res == -2) { return -2; }
  tl_store_int (TL_BOOL_STAT);
  tl_store_int (res > 0);
  tl_store_int (res <= 0);
  tl_store_int (0);
TL_DO_FUN_END

TL_PARSE_FUN(ad_set_domain)
  e->ad = tl_parse_ad_id ();
  e->domain = tl_fetch_int_range (0, MAX_AD_DOMAIN);
TL_PARSE_FUN_END

TL_DO_FUN(ad_set_group)
  int res = do_ad_set_group (e->ad, e->group);
  if (res == -2) { return -2; }
  tl_store_int (TL_BOOL_STAT);
  tl_store_int (res > 0);
  tl_store_int (res <= 0);
  tl_store_int (0);
TL_DO_FUN_END

TL_PARSE_FUN(ad_set_group)
  e->ad = tl_parse_ad_id ();
  e->group = tl_fetch_int_range (-0x7fffffff, 0x7fffffff);
TL_PARSE_FUN_END

TL_DO_FUN(ad_set_categories)
  int res = do_ad_set_categories (e->ad, e->category, e->subcategory);
  if (res == -2) { return -2; }
  tl_store_int (TL_BOOL_STAT);
  tl_store_int (res > 0);
  tl_store_int (res <= 0);
  tl_store_int (0);
TL_DO_FUN_END

TL_PARSE_FUN(ad_set_categories)
  e->ad = tl_parse_ad_id ();
  e->category = tl_fetch_int_range (0, MAX_AD_CATEGORY);
  e->subcategory = tl_fetch_int_range (0, MAX_AD_CATEGORY);
TL_PARSE_FUN_END

TL_DO_FUN(ad_clicks)
  int res = compute_ad_clicks (e->ad);
  if (res == -2) { return -2; }
  if (res < 0) {
    tl_store_int (TL_MAYBE_FALSE);
  } else {
    tl_store_int (TL_MAYBE_TRUE);
    tl_store_int (res);
  }
TL_DO_FUN_END

TL_PARSE_FUN(ad_clicks)
  e->ad = tl_parse_ad_id ();
TL_PARSE_FUN_END

TL_DO_FUN(ad_ctr)
  long long R[64];
  int res = compute_ad_ctr (e->ad, R);
  if (res <= 0) {
    tl_store_int (TL_MAYBE_FALSE);
  } else {
    tl_store_int (TL_MAYBE_TRUE);
    if (e->mask & 1) {
      tl_store_int (R[0] & ((1 << 20) - 1));
      tl_store_int (R[0] >> 20);
      tl_store_int (R[1] & ((1 << 20) - 1));
      tl_store_int (R[1] >> 20);
    } 
    if (e->mask & 2) {
      int i;
      for (i = 2; i < 5; i++) {
        tl_store_int (R[i] & ((1 << 20) - 1));
        R[i] >>= 20;
        tl_store_int (R[i] & ((1 << 20) - 1));
        R[i] >>= 20;
        tl_store_int (R[i]);
      }
    }
  }
TL_DO_FUN_END

TL_PARSE_FUN(ad_ctr,int mask)
  e->ad = tl_parse_ad_id ();
  e->mask = mask;
TL_PARSE_FUN_END

TL_DO_FUN(ad_money)
  int res = compute_ad_money (e->ad);
  if (res == -2) { return -2; }
  if (res < 0) {
    tl_store_int (TL_MAYBE_FALSE);
  } else {
    tl_store_int (TL_MAYBE_TRUE);
    tl_store_int (res);
  }
TL_DO_FUN_END

TL_PARSE_FUN(ad_money)
  e->ad = tl_parse_ad_id ();
TL_PARSE_FUN_END

TL_DO_FUN(ad_views)
  int res = compute_ad_views (e->ad);
  if (res == -2) { return -2; }
  if (res < 0) {
    tl_store_int (TL_MAYBE_FALSE);
  } else {
    tl_store_int (TL_MAYBE_TRUE);
    tl_store_int (res);
  }
TL_DO_FUN_END

TL_PARSE_FUN(ad_views)
  e->ad = tl_parse_ad_id ();
TL_PARSE_FUN_END

TL_DO_FUN(ad_recent_views)
  int res = compute_ad_recent_views (e->ad);
  if (res == -2) { return -2; }
  if (res < 0) {
    tl_store_int (TL_MAYBE_FALSE);
  } else {
    tl_store_int (TL_MAYBE_TRUE);
    tl_store_int (res);
  }
TL_DO_FUN_END

TL_PARSE_FUN(ad_recent_views)
  e->ad = tl_parse_ad_id ();
TL_PARSE_FUN_END

TL_DO_FUN(recent_views_stats)
  Q_limit = e->limit;
  Q_order = convert_mode (e->mode);

  int res = compute_recent_views_stats ();
  if (res == -2) { return -2; }
  if (res < 0) { 
    tl_store_int (TL_VECTOR_TOTAL);
    tl_store_int (0);
    tl_store_int (0);
  } else {
    tl_store_int (TL_VECTOR_TOTAL);
    tl_store_int (res);
    assert (!(R_cnt & 1));
    tl_store_int (R_cnt / 2);
    tl_store_raw_data (R, 4 * R_cnt);
  }
TL_DO_FUN_END

TL_PARSE_FUN(recent_views_stats)
  e->mode = tl_fetch_int ();
  e->limit = tl_fetch_int ();
TL_PARSE_FUN_END

TL_DO_FUN(recent_ad_viewers)
  Q_limit = e->limit;
  Q_order = convert_mode (e->mode);

  int res = compute_recent_ad_viewers (e->ad);
  if (res == -2) { return -2; }
  if (res < 0) { 
    tl_store_int (TL_VECTOR_TOTAL);
    tl_store_int (0);
    tl_store_int (0);
  } else {
    tl_store_int (TL_VECTOR_TOTAL);
    tl_store_int (res);
    assert (!(R_cnt & 1));
    tl_store_int (R_cnt / 2);
    tl_store_raw_data (R, 4 * R_cnt);
  }
TL_DO_FUN_END

TL_PARSE_FUN(recent_ad_viewers)
  e->ad = tl_parse_ad_id ();
  e->mode = tl_fetch_int ();
  e->limit = tl_fetch_int ();
TL_PARSE_FUN_END

TL_DO_FUN(ad_info)
  static long long RR[64];
  int res = compute_ad_info (e->ad, RR);
  if (res == -2) { return -2; }
  if (res <= 0) {
    tl_store_int (TL_MAYBE_FALSE);
  } else {
    tl_store_int (TL_MAYBE_TRUE);
    assert (res == 29);
    tl_store_raw_data (RR, res * 8);
  }
TL_DO_FUN_END

TL_PARSE_FUN(ad_info)
  e->ad = tl_parse_ad_id ();
TL_PARSE_FUN_END

TL_DO_FUN(ad_query)
  char *ptr = get_ad_query (e->ad);
  if (ptr && (strlen (ptr) <= 1000)) {
    tl_store_int (TL_MAYBE_TRUE);
    tl_store_string0 (ptr);
  } else {
    tl_store_int (TL_MAYBE_FALSE);
  }
TL_DO_FUN_END

TL_PARSE_FUN(ad_query)
  e->ad = tl_parse_ad_id ();
TL_PARSE_FUN_END

TL_DO_FUN(user_view)
  int res = do_register_user_view (e->uid, e->ad);
  if (res == -2) { return -2; }
  if (res <= 0) {
    tl_store_int (TL_BOOL_FALSE);
  } else {
    tl_store_int (TL_BOOL_TRUE);
  }
TL_DO_FUN_END

TL_PARSE_FUN(user_view)
  e->uid = tl_parse_uid ();
  e->ad = tl_parse_ad_id ();
TL_PARSE_FUN_END

TL_DO_FUN(user_groups)
  int res = get_user_groups (e->uid);
  if (res == -2) { return -2; }
  if (res < 0) {
    tl_store_int (TL_MAYBE_FALSE);
  } else {
    tl_store_int (TL_MAYBE_TRUE);
    tl_store_int (R_cnt);
    tl_store_raw_data (R, R_cnt * 4);
  }
TL_DO_FUN_END

TL_PARSE_FUN(user_groups)
  e->uid = tl_parse_uid ();
TL_PARSE_FUN_END

TL_DO_FUN(user_click)
  int res = do_register_user_click (e->uid, e->ad, e->price);
  if (res == -2) { return -2; }
  if (res <= 0) {
    tl_store_int (TL_BOOL_FALSE);
  } else {
    tl_store_int (TL_BOOL_TRUE);
  }
TL_DO_FUN_END

TL_PARSE_FUN(user_click)
  e->uid = tl_parse_uid ();
  e->ad = tl_parse_ad_id ();
  e->price = tl_fetch_int ();
TL_PARSE_FUN_END

TL_DO_FUN(user_flags)
  int res = get_has_photo (e->uid);
  if (res == -2) { return -2; }
  if (res < 0) {
    tl_store_int (TL_MAYBE_FALSE);
  } else {
    tl_store_int (TL_MAYBE_TRUE);
    tl_store_int (res);
  }
TL_DO_FUN_END

TL_PARSE_FUN(user_flags)
  e->uid = tl_parse_uid ();
TL_PARSE_FUN_END

TL_DO_FUN(user_clicked_ad)
  int res = compute_ad_user_clicked (e->uid, e->ad);
  if (res == -2) { return -2; }
  tl_store_int (TL_MAYBE_TRUE);
  tl_store_int (res);
TL_DO_FUN_END

TL_PARSE_FUN(user_clicked_ad)
  e->uid = tl_parse_uid ();
  e->ad = tl_parse_ad_id ();
TL_PARSE_FUN_END

TL_DO_FUN(delete_group)
  int res = do_delete_group (e->gid);
  if (res == -2) { return -2; }
  tl_store_int (TL_BOOL_STAT);
  tl_store_int (res > 0);
  tl_store_int (res <= 0);
  tl_store_int (0);
TL_DO_FUN_END

TL_PARSE_FUN(delete_group)
  e->gid = tl_fetch_int ();
TL_PARSE_FUN_END

TL_DO_FUN(target)
  char *query = (char *)(e->user_list + e->user_list_size);
  register_long_query (query, e->query_len);

  memcpy (aux_userlist, e->user_list, 4 * e->user_list_size);
  aux_userlist_tag = -1;
  aux_userlist_size = e->user_list_size;

  char *q_end = compile_query (query);
  if (q_end) {
    complete_long_query (0, -1);
    tl_fetch_set_error_format (TL_ERROR_BAD_VALUE, "Error near %.256s", q_end);
    return -1;
  }
  
  targeting_queries++;
  int res = do_perform_targeting (e->ad, e->price, e->factor, query);
  if (res == -2) { return res; }

  if (res >= 0) {
    tl_store_int (TL_MAYBE_TRUE);
    tl_store_int (res);
  } else {
    tl_store_int (TL_MAYBE_FALSE);
  }
  complete_long_query (0, res);
TL_DO_FUN_END

TL_PARSE_FUN(target)
  e->mode = tl_fetch_int ();
  e->ad = tl_parse_ad_id ();
  e->price = tl_fetch_int ();
  if (e->mode & (1 << 16)) { 
    e->factor = tl_fetch_int_range (0, 89);
    e->factor = (100 - e->factor) * 10000;
  } else {
    e->factor = 0;
  }
  //e->user_list_size = tl_fetch_int_range (0, 10000);
  //if (e->user_list_size < 0) { e->user_list_size = 0; }
  //tl_fetch_raw_data (e->user_list, e->user_list_size * 4);
  e->user_list_size = 0;
  extra->size += e->user_list_size * 4;
  char *q = (char *)(e->user_list + e->user_list_size);
  e->query_len = tl_fetch_string0 (q, (1 << 19));
  extra->size += e->query_len + 1;
TL_PARSE_FUN_END

TL_DO_FUN(prices)
  char *query = (char *)(e->user_list + e->user_list_size);
  register_long_query (query, e->query_len);

  memcpy (aux_userlist, e->user_list, 4 * e->user_list_size);
  aux_userlist_tag = -1;
  aux_userlist_size = e->user_list_size;


  char *q_end = compile_query (query);
  if (q_end) {
    complete_long_query (0, -1);
    tl_fetch_set_error_format (TL_ERROR_BAD_VALUE, "Error near %.256s", q_end);
    return -1;
  }
  
  Q_limit = e->limit;

  pricing_queries++;
  R_cnt = 0;  
  int res = perform_pricing (e->place & 1023, (e->place & (1024)) >> (10 - 3), e->and_mask, e->xor_mask); // 1024 -> 8
  if (res < 0) { return res; }
  tl_store_int (TL_TARG_PRICES_RESULT);
  tl_store_int (res);
  assert (R_cnt <= e->limit);
  tl_store_raw_data (R, 4 * e->limit);
  complete_long_query (0, res);
TL_DO_FUN_END

TL_PARSE_FUN(prices)
  e->mode = tl_fetch_int ();
  e->place = tl_fetch_int ();
  if (e->mode & (1 << 17)) { 
    e->and_mask = tl_fetch_int ();
    e->xor_mask = tl_fetch_int ();
  } else {
    e->and_mask = 254;
    e->xor_mask = 0;
  }
  e->limit = tl_fetch_int_range (1, 10000);
  e->user_list_size = (e->mode & (1 << 19)) ? tl_fetch_int_range (0, 10000) : 0;
  if (e->user_list_size < 0) { e->user_list_size = 0; }
  tl_fetch_raw_data (e->user_list, e->user_list_size * 4);
  extra->size += e->user_list_size * 4;
  char *q = (char *)(e->user_list + e->user_list_size);
  e->query_len = tl_fetch_string0 (q, (1 << 19));
  extra->size += e->query_len + 1;
TL_PARSE_FUN_END

TL_DO_FUN(ad_pricing)
  register_long_query ("ad_pricing", strlen ("ad_pricing"));

  memcpy (aux_userlist, e->user_list, 4 * e->user_list_size);
  aux_userlist_tag = -1;
  aux_userlist_size = e->user_list_size;
  
  Q_limit = e->limit;
  pricing_queries++;

  R_cnt = 0;
  int res = perform_ad_pricing (e->ad, e->place & 1023, (e->place & 1024) ? 8 : 0, e->and_mask, e->xor_mask, e->max_users);
  if (res < 0) { return res; }
  tl_store_int (TL_TARG_PRICES_RESULT);
  tl_store_int (res);
  assert (R_cnt <= e->limit);
  tl_store_raw_data (R, 4 * e->limit);
  complete_long_query (0, res);
TL_DO_FUN_END

TL_PARSE_FUN(ad_pricing)
  e->mode = tl_fetch_int ();
  e->ad = tl_parse_ad_id ();
  e->place = tl_fetch_int ();
  if (e->mode & (1 << 17)) { 
    e->and_mask = tl_fetch_int ();
    e->xor_mask = tl_fetch_int ();
  } else {
    e->and_mask = 254;
    e->xor_mask = 0;
  }
  if (e->mode & (1 << 18)) {
    e->max_users = tl_fetch_int ();
  } else {
    e->max_users = 50;
  }
  e->limit = tl_fetch_int_range (1, 10000);
  e->user_list_size = 0;
  e->query_len = 0;
//  e->user_list_size = (e->mode & (1 << 19)) ? tl_fetch_int_range (0, 10000) : 0;
//  if (e->user_list_size < 0) { e->user_list_size = 0; }
//  tl_fetch_raw_data (e->user_list, e->user_list_size * 4);
//  extra->size += e->user_list_size * 4;
//  char *q = (char *)(e->user_list + e->user_list_size);
//  e->query_len = tl_fetch_string0 (q, (1 << 19));
//  extra->size += e->query_len + 1;
TL_PARSE_FUN_END

TL_DO_FUN(targ_audience)
  char *query = (char *)(e->user_list + e->user_list_size);
  register_long_query (query, e->query_len);

  memcpy (aux_userlist, e->user_list, 4 * e->user_list_size);
  aux_userlist_tag = -1;
  aux_userlist_size = e->user_list_size;
  
  targ_audience_queries++;
  
  clear_query_list ();
  char *q_ptr = query;
  if (compile_add_query (&q_ptr)) {
    while (*q_ptr == ';') {
      q_ptr++;
      if (!compile_add_query (&q_ptr)) {
        complete_long_query (0, -1);
        tl_fetch_set_error_format (TL_ERROR_BAD_VALUE, "Error near %.256s", q_ptr);
        return -1;
      }
    }
    if (*q_ptr) {
      complete_long_query (0, -1);
      tl_fetch_set_error_format (TL_ERROR_BAD_VALUE, "Error near %.256s", q_ptr);
      return -1;
    }
  } else {
    complete_long_query (0, -1);
    tl_fetch_set_error_format (TL_ERROR_BAD_VALUE, "Error near %.256s", q_ptr);
    return -1;
  }
  Q_limit = e->max_users;
  R_cnt = 0;
  int res = perform_targ_audience_query (e->place, e->cpv, e->and_mask, e->xor_mask);
  if (res < 0) { return res; }
  tl_store_int (TL_VECTOR);
  assert (R_cnt % 3 == 2);
//  assert (R_cnt == (e->n + 1) * 3);
  tl_store_int ((R_cnt + 1) / 3);
  tl_store_int (res);
  tl_store_raw_data (R, R_cnt * 4);
  complete_long_query (0, res);
TL_DO_FUN_END

TL_PARSE_FUN(targ_audience)
  e->mode = tl_fetch_int ();
  e->place = tl_fetch_int ();
  e->cpv = tl_fetch_int ();
  if (e->mode & (1 << 17)) { 
    e->and_mask = tl_fetch_int ();
    e->xor_mask = tl_fetch_int ();
  } else {
    e->and_mask = 254;
    e->xor_mask = 0;
  }
  if (e->mode & (1 << 18)) {
    e->max_users = tl_fetch_int ();
  } else {
    e->max_users = 50;
  }
  e->user_list_size = (e->mode & (1 << 19)) ? tl_fetch_int_range (0, 10000) : 0;
  if (e->user_list_size < 0) { e->user_list_size = 0; }
  tl_fetch_raw_data (e->user_list, e->user_list_size * 4);
  extra->size += e->user_list_size * 4;
  char *q = (char *)(e->user_list + e->user_list_size);
  e->query_len = 0;
  int n = tl_fetch_int ();
  int i;
  for (i = 0; i < n + 1; i++) {
    int l = tl_fetch_string0 (q + e->query_len, (1 << 19) - e->query_len);
    if (l < 0) { return 0; }
    e->query_len += l;
    if (i != n) {
      q[e->query_len ++] = ';';
    }
  }
  extra->size += e->query_len + 1;
TL_PARSE_FUN_END

TL_DO_FUN(audience)
  char *query = (char *)(e->user_list + e->user_list_size);
  register_long_query (query, e->query_len);

  memcpy (aux_userlist, e->user_list, 4 * e->user_list_size);
  aux_userlist_tag = -1;
  aux_userlist_size = e->user_list_size;
  
  targ_audience_queries++;
  
  clear_query_list ();
  char *q_ptr = query;
  if (compile_add_query (&q_ptr)) {
    while (*q_ptr == ';') {
      q_ptr++;
      if (!compile_add_query (&q_ptr)) {
        complete_long_query (0, -1);
        tl_fetch_set_error_format (TL_ERROR_BAD_VALUE, "Error near %.256s", q_ptr);
        return -1;
      }
    }
    if (*q_ptr) {
      complete_long_query (0, -1);
      tl_fetch_set_error_format (TL_ERROR_BAD_VALUE, "Error near %.256s", q_ptr);
      return -1;
    }
  } else {
    complete_long_query (0, -1);
    tl_fetch_set_error_format (TL_ERROR_BAD_VALUE, "Error near %.256s", q_ptr);
    return -1;
  }
  audience_queries++;
  R_cnt = 0;
  int res = perform_audience_query ();
  if (res < 0) { return res; }
  tl_store_int (TL_VECTOR);
  tl_store_int (R_cnt + 1);
  tl_store_int (res);
  tl_store_raw_data (R, R_cnt * 4);
  complete_long_query (0, res);
TL_DO_FUN_END

TL_PARSE_FUN(audience)
  e->mode = tl_fetch_int ();
  e->user_list_size = (e->mode & (1 << 19)) ? tl_fetch_int_range (0, 10000) : 0;
  if (e->user_list_size < 0) { e->user_list_size = 0; }
  tl_fetch_raw_data (e->user_list, e->user_list_size * 4);
  extra->size += e->user_list_size * 4;
  char *q = (char *)(e->user_list + e->user_list_size);
  e->query_len = 0;
  int n = tl_fetch_int ();
  int i;
  for (i = 0; i < n + 1; i++) {
    int l = tl_fetch_string0 (q + e->query_len, (1 << 19) - e->query_len);
    if (l < 0) { return 0; }
    e->query_len += l;
    if (i != n) {
      q[e->query_len ++] = ';';
    }
  }
  extra->size += e->query_len + 1;
TL_PARSE_FUN_END

TL_DO_FUN(search)
  char *query = (char *)(e->user_list + e->user_list_size);
  register_long_query (query, e->query_len);

  memcpy (aux_userlist, e->user_list, 4 * e->user_list_size);
  aux_userlist_tag = -1;
  aux_userlist_size = e->user_list_size;
        
  Q_order = convert_mode (e->mode);
  Q_limit = e->limit;

  char *q_end = compile_query (query);
  if (q_end) {
    complete_long_query (0, -1);
    tl_fetch_set_error_format (TL_ERROR_BAD_VALUE, "Error near %.256s", q_end);
    return -1;
  }
  
  Q_order = convert_mode (e->mode);
  Q_limit = e->limit;
  
  search_queries++;
  R_cnt = 0;
  int res = perform_query (0);
  if (res < 0) { return res; }
  tl_store_int (TL_VECTOR_TOTAL);
  tl_store_int (res);
  if (e->mode & 1) {
    assert (R_cnt % 2 == 0);
    tl_store_int (R_cnt / 2);
  } else {
    tl_store_int (R_cnt);
  }
  tl_store_raw_data (R, 4 * R_cnt);
  complete_long_query (0, res);
TL_DO_FUN_END

TL_PARSE_FUN(search)
  e->mode = tl_fetch_int ();
  e->limit = tl_fetch_int ();
  e->user_list_size = (e->mode & (1 << 19)) ? tl_fetch_int_range (0, 10000) : 0;
  if (e->user_list_size < 0) { e->user_list_size = 0; }
  tl_fetch_raw_data (e->user_list, e->user_list_size * 4);
  extra->size += e->user_list_size * 4;
  char *q = (char *)(e->user_list + e->user_list_size);
  e->query_len = tl_fetch_string0 (q, (1 << 19));
  extra->size += e->query_len + 1;
TL_PARSE_FUN_END

#define fun_set_something(name,NAME) \
  struct tl_set_ ## name { \
    int uid; \
    int value; \
  }; \
  TL_DO_FUN(set_ ## name) \
    int res = do_set_ ## name (e->uid, e->value); \
    if (res == -2) { return -2; } \
    tl_store_int (res <= 0 ? TL_BOOL_FALSE: TL_BOOL_TRUE); \
  TL_DO_FUN_END \
  TL_PARSE_FUN(set_ ## name,void)\
    e->uid = tl_parse_uid (); \
    e->value = tl_fetch_int_range (0, MAX_ ## NAME); \
  TL_PARSE_FUN_END 

#define fun_set_something_negative(name,NAME) \
  struct tl_set_ ## name { \
    int uid; \
    int value; \
  }; \
  TL_DO_FUN(set_ ## name) \
    int res = do_set_ ## name (e->uid, e->value); \
    if (res == -2) { return -2; } \
    tl_store_int (res <= 0 ? TL_BOOL_FALSE: TL_BOOL_TRUE); \
  TL_DO_FUN_END \
  TL_PARSE_FUN(set_ ## name,void)\
    e->uid = tl_parse_uid (); \
    e->value = tl_fetch_int_range (-MAX_ ## NAME, MAX_ ## NAME); \
  TL_PARSE_FUN_END 

#define fun_set_something_any(name,NAME) \
  struct tl_set_ ## name { \
    int uid; \
    int value; \
  }; \
  TL_DO_FUN(set_ ## name) \
    int res = do_set_ ## name (e->uid, e->value); \
    if (res == -2) { return -2; } \
    tl_store_int (res <= 0 ? TL_BOOL_FALSE: TL_BOOL_TRUE); \
  TL_DO_FUN_END \
  TL_PARSE_FUN(set_ ## name,void)\
    e->uid = tl_parse_uid (); \
    e->value = tl_fetch_int (); \
  TL_PARSE_FUN_END 

#define fun_delete_something(name) \
  struct tl_delete_ ## name { \
    int uid; \
  }; \
  TL_DO_FUN(delete_ ## name) \
    int res = do_delete_ ## name (e->uid); \
    if (res == -2) { return -2; } \
    tl_store_int (res <= 0 ? TL_BOOL_FALSE: TL_BOOL_TRUE); \
  TL_DO_FUN_END \
  TL_PARSE_FUN(delete_ ## name,void)\
    e->uid = tl_parse_uid (); \
  TL_PARSE_FUN_END 

#define CASE(name,NAME) \
  case TL_TARG_SET_ ## NAME: \
    return tl_set_ ## name (); 

#define CASE_DEL(name,NAME) \
  case TL_TARG_DELETE_ ## NAME: \
    return tl_delete_ ## name (); 


#define MAX_USER_SINGLE_GROUP_TYPE 127
fun_set_something (sex,SEX)
fun_set_something (operator,OPERATOR)
fun_set_something (browser,BROWSER)
fun_set_something (region,REGION)
fun_set_something (height,HEIGHT)
fun_set_something (smoking,SMOKING)
fun_set_something (alcohol,ALCOHOL)
fun_set_something (ppriority,PPRIORITY)
fun_set_something (iiothers,IIOTHERS)
fun_set_something (hidden,HIDDEN)
fun_set_something (cvisited,CVISITED)
fun_set_something (gcountry,GCOUNTRY)
fun_set_something (privacy,PRIVACY)
fun_set_something (political,POLITICAL)
fun_set_something (mstatus,MSTATUS)
fun_set_something_negative (timezone,TIMEZONE)
fun_set_something_any (rate,RATE)
fun_set_something_any (cute,CUTE)
fun_set_something (has_photo,HAS_PHOTO)
fun_set_something (user_single_group_type,USER_SINGLE_GROUP_TYPE)
fun_set_something (user_lang,LANGS)

TL_DO_FUN(set_custom)
  int res = do_set_custom[e->type - 1] (e->uid, e->value);
  if (res == -2) { return -2; }
  tl_store_int (res <= 0 ? TL_BOOL_FALSE: TL_BOOL_TRUE);
TL_DO_FUN_END

TL_PARSE_FUN(set_custom,void)
  e->uid = tl_parse_uid ();
  e->type = tl_fetch_int_range (1, 15);
  e->value = tl_fetch_int_range (0, 255);
TL_PARSE_FUN_END
  
TL_DO_FUN(set_rates)
  int res = do_set_rate_cute (e->uid, e->rate, e->cute);
  if (res == -2) { return -2; }
  tl_store_int (res <= 0 ? TL_BOOL_FALSE: TL_BOOL_TRUE);
TL_DO_FUN_END

TL_PARSE_FUN(set_rates,void)
  e->uid = tl_parse_uid ();
  e->rate = tl_fetch_int ();
  e->cute = tl_fetch_int ();
TL_PARSE_FUN_END
  
TL_DO_FUN(set_username)
  int res = do_set_username (e->uid, e->name, e->len);
  if (res == -2) { return -2; }
  tl_store_int (res <= 0 ? TL_BOOL_FALSE: TL_BOOL_TRUE);
TL_DO_FUN_END

TL_PARSE_FUN(set_username,void)
  e->uid = tl_parse_uid ();
  e->len = tl_fetch_string0 (e->name,255);
  extra->size += e->len + 1;
TL_PARSE_FUN_END
  
TL_DO_FUN(set_user_group_types)
  int i;
  static unsigned t[4];
  memset (t, 0, sizeof (t));
  for (i = 0; i < e->n; i++) {
    t[e->data[i] >> 5] |= e->data[i] & 31;
  }
  int res = do_set_user_group_types (e->uid, t);
  if (res == -2) { return -2; }
  tl_store_int (res <= 0 ? TL_BOOL_FALSE: TL_BOOL_TRUE);
TL_DO_FUN_END

TL_PARSE_FUN(set_user_group_types,void)
  e->uid = tl_parse_uid ();
  e->n = tl_fetch_int_range (0, (1 << 17));
  int i;
  for (i = 0; i < e->n; i++) {
    e->data[i] = tl_fetch_int_range (0, 127);
    if (tl_fetch_error ()) { break; }
  }
  extra->size += e->n * 4;
TL_PARSE_FUN_END
  
TL_DO_FUN(set_country_city)
  int res = do_set_country_city (e->uid, e->country, e->city);
  if (res == -2) { return -2; }
  tl_store_int (res <= 0 ? TL_BOOL_FALSE: TL_BOOL_TRUE);
TL_DO_FUN_END

TL_PARSE_FUN(set_country_city,void)
  e->uid = tl_parse_uid ();
  e->country = tl_fetch_int_range (0, 255);
  e->city = tl_fetch_nonnegative_int ();
TL_PARSE_FUN_END
  
TL_DO_FUN(set_birthday)
  int res = do_set_birthday (e->uid, e->day, e->month, e->year);
  if (res == -2) { return -2; }
  tl_store_int (res <= 0 ? TL_BOOL_FALSE: TL_BOOL_TRUE);
TL_DO_FUN_END

TL_PARSE_FUN(set_birthday,void)
  e->uid = tl_parse_uid ();
  e->day = tl_fetch_int_range (0, 31);
  e->month = tl_fetch_int_range (0, 12);
  e->year = tl_fetch_int ();
  if (e->year && (e->year < 1900 || e->year > 2008)) {
    tl_fetch_set_error_format (TL_ERROR_VALUE_NOT_IN_RANGE, "year should be in range [1900,2008] or 0");
    return 0;
  }
TL_PARSE_FUN_END
  
TL_DO_FUN(set_religion)
  int res = do_set_religion (e->uid, e->name, e->len);
  if (res == -2) { return -2; }
  tl_store_int (res <= 0 ? TL_BOOL_FALSE: TL_BOOL_TRUE);
TL_DO_FUN_END

TL_PARSE_FUN(set_religion,void)
  e->uid = tl_parse_uid ();
  e->len = tl_fetch_string0 (e->name,255);
  extra->size += e->len + 1;
TL_PARSE_FUN_END
  
TL_DO_FUN(set_hometown)
  int res = do_set_hometown (e->uid, e->name, e->len);
  if (res == -2) { return -2; }
  tl_store_int (res <= 0 ? TL_BOOL_FALSE: TL_BOOL_TRUE);
TL_DO_FUN_END

TL_PARSE_FUN(set_hometown,void)
  e->uid = tl_parse_uid ();
  e->len = tl_fetch_string0 (e->name,255);
  extra->size += e->len + 1;
TL_PARSE_FUN_END
  
TL_DO_FUN(set_proposal)
  int res = do_set_proposal (e->uid, e->name, e->len);
  if (res == -2) { return -2; }
  tl_store_int (res <= 0 ? TL_BOOL_FALSE: TL_BOOL_TRUE);
TL_DO_FUN_END

TL_PARSE_FUN(set_proposal,void)
  e->uid = tl_parse_uid ();
  e->len = tl_fetch_string0 (e->name,255);
  extra->size += e->len + 1;
TL_PARSE_FUN_END
  
struct tl_set_school {
  int uid;
  int len;
  struct school S;
  char name[0];
};

TL_DO_FUN(set_school)
  e->S.spec = e->name;
  int res = do_set_school (e->uid, &e->S);
  if (res == -2) { return -2; }
  tl_store_int (res <= 0 ? TL_BOOL_FALSE: TL_BOOL_TRUE);
TL_DO_FUN_END

TL_PARSE_FUN(set_school,void)
  e->uid = tl_parse_uid ();
  e->S.country = tl_fetch_int ();
  e->S.city = tl_fetch_int ();
  e->S.school = tl_fetch_int ();
  e->S.start = tl_fetch_int ();
  e->S.finish = tl_fetch_int ();
  e->S.grad = tl_fetch_int ();
  e->S.sch_class = tl_fetch_int ();
  e->S.sch_type = tl_fetch_int ();
  e->len = tl_fetch_string0 (e->name,255);
  extra->size += e->len + 1;
TL_PARSE_FUN_END

struct tl_set_education {
  int uid;
  struct education E;
};

TL_DO_FUN(set_education)
  int res = do_set_education (e->uid, &e->E);
  if (res == -2) { return -2; }
  tl_store_int (res <= 0 ? TL_BOOL_FALSE: TL_BOOL_TRUE);
TL_DO_FUN_END

TL_PARSE_FUN(set_education,void)
  e->uid = tl_parse_uid ();
  e->E.country = tl_fetch_int ();
  e->E.city = tl_fetch_int ();
  e->E.university = tl_fetch_int ();
  e->E.faculty = tl_fetch_int ();
  e->E.chair = tl_fetch_int ();
  e->E.grad_year = tl_fetch_int ();
  e->E.edu_form = tl_fetch_int ();
  e->E.edu_status = tl_fetch_int ();
  e->E.primary = tl_fetch_int ();
TL_PARSE_FUN_END

struct tl_set_company {
  int uid;
  int len1;
  int len2;
  struct company C;
  char name[0];
};

TL_DO_FUN(set_company)
  e->C.company_name = e->name;
  e->C.job = e->name + e->len1 + 1;
  int res = do_set_work (e->uid, &e->C);
  if (res == -2) { return -2; }
  tl_store_int (res <= 0 ? TL_BOOL_FALSE: TL_BOOL_TRUE);
TL_DO_FUN_END

TL_PARSE_FUN(set_company,void)
  e->uid = tl_parse_uid ();
  e->C.country = tl_fetch_int ();
  e->C.city = tl_fetch_int ();
  e->C.company = tl_fetch_int ();
  e->C.start = tl_fetch_int ();
  e->C.finish = tl_fetch_int ();
  e->len1 = tl_fetch_string0 (e->name, 255);
  extra->size += e->len1 + 1;
  e->len2 = tl_fetch_string0 (e->name + e->len1 + 1, 255);
  extra->size += e->len2 + 1;
TL_PARSE_FUN_END

struct tl_set_military {
  int uid;
  int unit_id;
  int start;
  int finish;
};

TL_DO_FUN(set_military)
  int res = do_set_military (e->uid, e->unit_id, e->start, e->finish);
  if (res == -2) { return -2; }
  tl_store_int (res <= 0 ? TL_BOOL_FALSE: TL_BOOL_TRUE);
TL_DO_FUN_END

TL_PARSE_FUN(set_military,void)
  e->uid = tl_parse_uid ();
  e->unit_id = tl_fetch_int ();
  e->start = tl_fetch_int ();
  e->finish = tl_fetch_int ();
TL_PARSE_FUN_END

struct tl_set_address {
  int uid;
  int len1;
  int len2;
  struct address A;
  char name[0];
};

TL_DO_FUN(set_address)
  e->A.name = e->name;
  e->A.house = e->name + e->len1 + 1;
  int res = do_set_address (e->uid, &e->A);
  if (res == -2) { return -2; }
  tl_store_int (res <= 0 ? TL_BOOL_FALSE: TL_BOOL_TRUE);
TL_DO_FUN_END

TL_PARSE_FUN(set_address,void)
  e->uid = tl_parse_uid ();
  e->A.atype = tl_fetch_int ();
  e->A.country = tl_fetch_int ();
  e->A.city = tl_fetch_int ();
  e->A.district = tl_fetch_int ();
  e->A.station = tl_fetch_int ();
  e->A.street = tl_fetch_int ();
  e->len1 = tl_fetch_string0 (e->name, 255);
  extra->size += e->len1 + 1;
  e->len2 = tl_fetch_string0 (e->name + e->len1 + 1, 255);
  extra->size += e->len2 + 1;
TL_PARSE_FUN_END

struct tl_set_interest {
  int uid;
  int len;
  int type;
  char name[0];
};

TL_DO_FUN(set_interest)
  int res = do_set_interest (e->uid, e->name, e->len, e->type);
  if (res == -2) { return -2; }
  tl_store_int (res <= 0 ? TL_BOOL_FALSE: TL_BOOL_TRUE);
TL_DO_FUN_END

TL_PARSE_FUN(set_interest,void)
  e->uid = tl_parse_uid ();
  e->type = tl_fetch_int_range (1, MAX_INTERESTS);
  e->len = tl_fetch_string0 (e->name, 255);
  extra->size += e->len + 1;
TL_PARSE_FUN_END

struct tl_user_visit {
  int uid;
  int len;
  char name[0];
};

TL_DO_FUN(user_visit)
  int res = do_user_visit (e->uid, e->name, e->len);
  if (res == -2) { return -2; }
  tl_store_int (res <= 0 ? TL_BOOL_FALSE: TL_BOOL_TRUE);
TL_DO_FUN_END

TL_PARSE_FUN(user_visit,void)
  e->uid = tl_parse_uid ();
  e->len = tl_fetch_string0 (e->name, 255);
  extra->size += e->len + 1;
TL_PARSE_FUN_END

struct tl_set_user_group {
  int uid;
  int gid;
};

TL_DO_FUN(set_user_group)
  int res = do_set_user_group (e->uid, e->gid);
  if (res == -2) { return -2; }
  tl_store_int (res <= 0 ? TL_BOOL_FALSE: TL_BOOL_TRUE);
TL_DO_FUN_END

TL_PARSE_FUN(set_user_group,void)
  e->uid = tl_parse_uid ();
  e->gid = tl_fetch_int ();
  if (!e->gid) {
    tl_fetch_set_error_format (TL_ERROR_VALUE_NOT_IN_RANGE, "group_id should be non-zero");
  }
TL_PARSE_FUN_END

struct tl_delete_user {
  int uid;
};

TL_DO_FUN(delete_user)
  int res = do_delete_user (e->uid);
  if (res == -2) { return -2; }
  tl_store_int (res <= 0 ? TL_BOOL_FALSE: TL_BOOL_TRUE);
TL_DO_FUN_END

TL_PARSE_FUN(delete_user,void)
  e->uid = tl_parse_uid ();
TL_PARSE_FUN_END

fun_delete_something (education);
fun_delete_something (schools);
fun_delete_something (work);
fun_delete_something (addresses);
fun_delete_something (military);
fun_delete_something (groups);
fun_delete_something (positive_groups);
fun_delete_something (negative_groups);
fun_delete_something (langs);
fun_delete_something (proposal);

struct tl_delete_interests {
  int uid;
  int type;
};

TL_DO_FUN(delete_interests)
  int res = do_delete_interests (e->uid, e->type);
  if (res == -2) { return -2; }
  tl_store_int (res <= 0 ? TL_BOOL_FALSE: TL_BOOL_TRUE);
TL_DO_FUN_END

TL_PARSE_FUN(delete_interests,void)
  e->uid = tl_parse_uid ();
  e->type = tl_fetch_int_range (0, MAX_INTERESTS);
TL_PARSE_FUN_END

struct tl_delete_user_group {
  int uid;
  int gid;
};

TL_DO_FUN(delete_user_group)
  int res = do_delete_user_group (e->uid, e->gid);
  if (res == -2) { return -2; }
  tl_store_int (res <= 0 ? TL_BOOL_FALSE: TL_BOOL_TRUE);
TL_DO_FUN_END

TL_PARSE_FUN(delete_user_group,void)
  e->uid = tl_parse_uid ();
  e->gid = tl_fetch_int ();
TL_PARSE_FUN_END

struct tl_delete_user_lang {
  int uid;
  int lang;
};

TL_DO_FUN(delete_user_lang)
  int res = do_delete_user_lang (e->uid, e->lang);
  if (res == -2) { return -2; }
  tl_store_int (res <= 0 ? TL_BOOL_FALSE: TL_BOOL_TRUE);
TL_DO_FUN_END

TL_PARSE_FUN(delete_user_lang,void)
  e->uid = tl_parse_uid ();
  e->lang = tl_fetch_int_range (1, MAX_LANGS);
TL_PARSE_FUN_END

struct tl_set_user_groups {
  int uid;
  int len;
  int op;
  int groups[0];
};

TL_DO_FUN(set_user_groups)
  int res;
  switch (e->op) {
  case 0:
    res = do_set_user_groups (e->uid, e->groups, e->len);
    break;
  case 1:
    res = do_add_user_groups (e->uid, e->groups, e->len);
    break;
  case -1:
    res = do_del_user_groups (e->uid, e->groups, e->len);
    break;
  default:
    res = 0;
    assert (0);
  }
  if (res == -2) { return -2; }
  tl_store_int (res <= 0 ? TL_BOOL_FALSE: TL_BOOL_TRUE);
TL_DO_FUN_END

TL_PARSE_FUN(set_user_groups,int op)
  e->op = op;
  e->uid = tl_parse_uid ();
  e->len = tl_fetch_int_range (1, MAX_USER_LEV_GROUPS);
  if (e->len >= 0) {
    tl_fetch_raw_data (e->groups, 4 * e->len);
  }
  extra->size += 4 * e->len;
TL_PARSE_FUN_END

TL_DO_FUN(weights_send_small_updates)
  int i;
  vkprintf (2, "Received %d small updates\n", e->num);
  for (i = 0; i < e->num; i++) {
    targ_weights_small_update (e->data[4*i], e->data[4*i+1], e->data[4*i+2], e->data[4*i+3]);
  }
  tl_store_int (TL_BOOL_TRUE);
TL_DO_FUN_END

TL_PARSE_FUN(weights_send_small_updates, void)
  e->num = tl_fetch_nonnegative_int ();
  if (e->num > (sizeof (stats_buff) - 4) / 16) {
    tl_fetch_set_error_format (TL_ERROR_BAD_VALUE, "Invalid updates size: num = %d", e->num);
    return 0;
  }
  extra->size += 16 * e->num;
  tl_fetch_raw_data (e->data, 16 * e->num);
TL_PARSE_FUN_END

TL_DO_FUN(weights_send_updates)
  vkprintf (1, "Received %d updates\n", e->num);
  int i;
  const int t = 2 + e->n;
  int *d = e->data;
  for (i = 0; i < e->num; i++) {
    targ_weights_update (d[0], d[1], e->n, d + 2);
    d += t;
  }
  tl_store_int (TL_BOOL_TRUE);
TL_DO_FUN_END

TL_PARSE_FUN(weights_send_updates, void)
  e->n = tl_fetch_positive_int ();
  e->num = tl_fetch_nonnegative_int ();
  long long sz = (long long) e->num * (4LL * e->n + 8LL);
  if (sz + 8 > sizeof (stats_buff)) {
    tl_fetch_set_error_format (TL_ERROR_BAD_VALUE, "e->n (%d) and updates size (%d) is too big", e->n, e->num);
    return 0;
  }
  extra->size += sz;
  tl_fetch_raw_data (e->data, (int) sz);
TL_PARSE_FUN_END

TL_DO_FUN(user_ads)
  int res = compute_user_ads (e->uid, e->limit, e->flags & 31, e->and_mask, e->xor_mask, e->cat_mask);
  if (res < 0) { 
    return res;
  }
  if (res > e->limit) {
    res = e->limit;
  }
  tl_store_int (TL_VECTOR);
  tl_store_int (res);
  res *= (1 + ((e->flags & 1) ? 1 : 0) + ((e->flags & 2) ? 1 : 0) + ((e->flags & 4) ? 1 : 0));
  tl_store_raw_data (R, 4 * res); 
TL_DO_FUN_END

TL_PARSE_FUN(user_ads, void)
  e->uid = tl_parse_uid ();
  e->limit = tl_fetch_nonnegative_int ();
  if (e->limit > 16384) {
    e->limit = 16384;
  }
  e->flags = tl_fetch_int ();
  if (!(e->flags & (1 << 17))) {
    if (e->flags & 16) {
      e->and_mask = 255;
      e->xor_mask = 1;
    } else {
      e->and_mask = 254;
      e->xor_mask = 0;
    }
  } else {
    e->and_mask = tl_fetch_int ();
    e->xor_mask = tl_fetch_int ();
  }
  e->cat_mask = tl_fetch_long ();
TL_PARSE_FUN_END

struct tl_act_extra *targ_parse_function (long long actor_id) {
  if (actor_id != 0) {
    tl_fetch_set_error ("Targ only support actor_id = 0", TL_ERROR_WRONG_ACTOR_ID);
    return 0;
  }
  int op = tl_fetch_int ();
  if (tl_fetch_error ()) {
    return 0;
  }
  switch (op) {
  case TL_TARG_AD_ENABLE:
    return tl_ad_enable (0);
  case TL_TARG_AD_ENABLE_PRICE:
    return tl_ad_enable (1);
  case TL_TARG_AD_DISABLE:
    return tl_ad_disable ();
  case TL_TARG_AD_SET_CTR:
    return tl_ad_set_ctr ();
  case TL_TARG_AD_SET_SUMP:
    return tl_ad_set_sump ();
  case TL_TARG_AD_SET_CTR_SUMP:
    return tl_ad_set_ctr_sump ();
  case TL_TARG_AD_SET_AUD:
    return tl_ad_set_aud ();
  case TL_TARG_AD_LIMITED_VIEWS:
    return tl_ad_limited_views ();
  case TL_TARG_AD_VIEWS_RATE_LIMIT:
    return tl_ad_views_rate_limit ();
  case TL_TARG_AD_SITES:
    return tl_ad_sites ();
  case TL_TARG_AD_SET_FACTOR:
    return tl_ad_set_factor ();
  case TL_TARG_AD_SET_DOMAIN:
    return tl_ad_set_domain ();
  case TL_TARG_AD_SET_GROUP:
    return tl_ad_set_group ();
  case TL_TARG_AD_SET_CATEGORIES:
    return tl_ad_set_categories ();
  case TL_TARG_AD_CLICKS:
    return tl_ad_clicks ();
  case TL_TARG_AD_CTR:
    return tl_ad_ctr (1);
  case TL_TARG_AD_SUMP:
    return tl_ad_ctr (2);
  case TL_TARG_AD_CTR_SUMP:
    return tl_ad_ctr (3);
  case TL_TARG_AD_MONEY:
    return tl_ad_money ();
  case TL_TARG_AD_VIEWS:
    return tl_ad_views ();
  case TL_TARG_AD_RECENT_VIEWS:
    return tl_ad_recent_views ();
  case TL_TARG_RECENT_VIEWS_STATS:
    return tl_recent_views_stats ();
  case TL_TARG_RECENT_AD_VIEWERS:
    return tl_recent_ad_viewers ();
  case TL_TARG_AD_INFO:
    return tl_ad_info ();
  case TL_TARG_AD_QUERY:
    return tl_ad_query ();
  case TL_TARG_USER_VIEW:
    return tl_user_view ();
  case TL_TARG_USER_GROUPS:
    return tl_user_groups ();
  case TL_TARG_USER_CLICK:
    return tl_user_click ();
  case TL_TARG_USER_FLAGS:
    return tl_user_flags ();
  case TL_TARG_USER_CLICKED_AD:
    return tl_user_clicked_ad ();
  case TL_TARG_DELETE_GROUP:
    return tl_delete_group ();
  case TL_TARG_TARGET:
    return tl_target ();
  case TL_TARG_PRICES:
    return tl_prices ();
  case TL_TARG_AD_PRICING:
    return tl_ad_pricing ();
  case TL_TARG_TARG_AUDIENCE:
    return tl_targ_audience ();
  case TL_TARG_AUDIENCE:
    return tl_audience ();
  case TL_TARG_SEARCH:
    return tl_search ();
  CASE(sex,SEX)
  CASE(operator,OPERATOR)
  CASE(browser,BROWSER)
  CASE(region,REGION)
  CASE(height,HEIGHT)
  CASE(smoking,SMOKING)
  CASE(alcohol,ALCOHOL)
  CASE(ppriority,PPRIORITY)
  CASE(iiothers,IIOTHERS)
  CASE(hidden,HIDDEN)
  CASE(cvisited,CVISITED)
  CASE(gcountry,GCOUNTRY)
  CASE(privacy,PRIVACY)
  CASE(political,POLITICAL)
  CASE(mstatus,MSTATUS)
  CASE(timezone,TIMEZONE)
  CASE(rate,RATE)
  CASE(cute,CUTE)
  CASE(has_photo,FLAGS)
  CASE(user_single_group_type,USER_SINGLE_GROUP_TYPE)
  CASE(user_lang,USER_LANG)
  case TL_TARG_SET_CUSTOM:
    return tl_set_custom ();
  case TL_TARG_SET_RATES:
    return tl_set_rates ();
  case TL_TARG_SET_USERNAME:
    return tl_set_username ();
  case TL_TARG_SET_USER_GROUP_TYPES:
    return tl_set_user_group_types ();
  case TL_TARG_SET_COUNTRY_CITY:
    return tl_set_country_city ();
  case TL_TARG_SET_BIRTHDAY:
    return tl_set_birthday ();
  case TL_TARG_SET_RELIGION:
    return tl_set_religion ();
  case TL_TARG_SET_HOMETOWN:
    return tl_set_hometown ();
  case TL_TARG_SET_PROPOSAL:
    return tl_set_proposal ();
  case TL_TARG_SET_SCHOOL:
    return tl_set_school ();
  case TL_TARG_SET_EDUCATION:
    return tl_set_education ();
  case TL_TARG_SET_COMPANY:
    return tl_set_company ();
  case TL_TARG_SET_MILITARY:
    return tl_set_military ();
  case TL_TARG_SET_ADDRESS:
    return tl_set_address ();
  case TL_TARG_SET_INTEREST:
    return tl_set_interest ();
  case TL_TARG_SET_USER_GROUP:
    return tl_set_user_group ();
  case TL_TARG_DELETE_USER:
    return tl_delete_user ();
  CASE_DEL(education,EDUCATION)
  CASE_DEL(schools,SCHOOLS)
  CASE_DEL(work,WORK)
  CASE_DEL(addresses,ADDRESSES)
  CASE_DEL(military,MILITARY)
  CASE_DEL(groups,GROUPS)
  CASE_DEL(positive_groups,POSITIVE_GROUPS)
  CASE_DEL(negative_groups,NEGATIVE_GROUPS)
  CASE_DEL(langs,LANGS)
  CASE_DEL(proposal,PROPOSAL)
  case TL_TARG_DELETE_INTERESTS:
    return tl_delete_interests ();
  case TL_TARG_DELETE_USER_GROUP:
    return tl_delete_user_group ();
  case TL_TARG_DELETE_USER_GROUPS:
    return tl_set_user_groups (-1);
  case TL_TARG_SET_USER_GROUPS:
    return tl_set_user_groups (0);
  case TL_TARG_ADD_USER_GROUPS:
    return tl_set_user_groups (1);
  case TL_TARG_USER_VISIT:
    return tl_user_visit ();
  case TL_TARG_DELETE_USER_LANG:
    return tl_delete_user_lang ();
  case TL_WEIGHTS_SEND_SMALL_UPDATES:
    return tl_weights_send_small_updates ();
  case TL_WEIGHTS_SEND_UPDATES:
    return tl_weights_send_updates ();
  case TL_TARG_USER_ADS:
    return tl_user_ads ();
  default:
    tl_fetch_set_error_format (TL_ERROR_UNKNOWN_FUNCTION_ID, "Unknown op %08x", op);
    return 0;
  }
}


void reopen_logs (void) {
  int fd;
  fflush(stdout);
  fflush(stderr);
  if ((fd = open ("/dev/null", O_RDWR, 0)) != -1) {
    dup2(fd, 0);
    dup2(fd, 1);
    dup2(fd, 2);
    if (fd > 2) {
      close(fd);
    }
  }
  if (logname && (fd = open (logname, O_WRONLY|O_APPEND|O_CREAT, 0640)) != -1) {
    dup2(fd, 1);
    dup2(fd, 2);
    if (fd > 2) {
      close(fd);
    }
  }
  if (verbosity > 0) {
    fprintf (stderr, "logs reopened.\n");
  }
}

static void sigint_immediate_handler (const int sig) {
  fprintf (stderr, "SIGINT handled immediately.\n");
  exit (1);
}

static void sigterm_immediate_handler (const int sig) {
  fprintf (stderr, "SIGTERM handled immediately.\n");
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
  signal(SIGHUP, sighup_handler);
}

static void sigusr1_handler (const int sig) {
  fprintf (stderr, "got SIGUSR1, rotate logs.\n");
  reopen_logs ();
  signal (SIGUSR1, sigusr1_handler);
}

static void sigrtmax_handler (const int sig) {
  fprintf (stderr, "got SIGUSR3, write index.\n");
  force_write_index = 1;
}

static void sigpoll_handler (const int sig) {
  sigpoll_cnt++;
  signal(SIGPOLL, sigpoll_handler);
}

void check_child_status (void) {
  if (!child_pid) {
    return;
  }
  int status = 0;
  int res = waitpid (child_pid, &status, WNOHANG);
  if (res == child_pid) {
    if (WIFEXITED (status) || WIFSIGNALED (status)) {
      vkprintf (1, "child process %d terminated: exited = %d, signaled = %d, exit code = %d\n", 
                child_pid, WIFEXITED (status) ? 1 : 0, WIFSIGNALED (status) ? 1 : 0, WEXITSTATUS (status));
      child_pid = 0;
    }
  } else if (res == -1) {
    if (errno != EINTR) {
      kprintf ("waitpid (%d): %m\n", child_pid);
      child_pid = 0;
    }
  } else if (res) {
    kprintf ("waitpid (%d) returned %d???\n", child_pid, res);
  }
}

int sfd;

void fork_write_index (void) {
  if (child_pid) {
    vkprintf (1, "process with pid %d already generates index, skipping\n", child_pid);
    return;
  }

  flush_binlog_ts ();
  
  int res = fork ();

  if (res < 0) {
    kprintf ("fork: %m\n");
  } else if (!res) {
    binlogname = 0;
    close (sfd);
    res = write_index (!binlog_disabled);
    exit (res);
  } else {
    vkprintf (1, "created child process pid = %d\n", res);
    child_pid = res;
  }

  force_write_index = 0;
}

void cron (void) {
  flush_binlog();
  if (force_write_index) {
    fork_write_index ();
  }
  check_child_status();
  dyn_garbage_collector();

  retarget_dynamic_ads ();
  process_lru_ads ();
  forget_old_views ();
  create_all_outbound_connections ();
}

void targ_read_new_events (void) {
  use_aio = -use_aio;
  read_new_events ();
  use_aio = -use_aio;
}


void start_server (void) { 
  int i, prev_time = 0;

  init_epoll();
  init_netbuffers();
  if (udp_enabled) {
    init_msg_buffers (0);
  }

  signal (SIGINT, sigint_handler);
  signal (SIGTERM, sigterm_handler);
  signal (SIGUSR1, sigusr1_handler);
  signal (SIGPIPE, SIG_IGN);
  signal (SIGPOLL, sigpoll_handler);
  signal (SIGRTMAX, sigrtmax_handler);

  if (daemonize) {
    signal (SIGHUP, sighup_handler);
  }

  if (daemonize) {
    setsid();
  }

  if (!sfd) {
    sfd = server_socket (port, settings_addr, backlog, 0);
  }

  if (sfd < 0) {
    kprintf ("cannot open server socket at port %d: %m\n", port);
    exit (1);
  }

  if (change_user (username) < 0) {
    kprintf ("fatal: cannot change user to %s\n", username ? username : "(none)");
    exit (1);
  }

  memcache_auto_answer_mode = 1;
  //init_listening_connection (sfd, &ct_targ_engine_server, &memcache_methods);
  init_listening_connection (sfd, &ct_rpc_server, &rpc_methods);
  if (udp_enabled) {
    add_udp_socket (port, 0);
  }
  
  if (binlog_disabled && binlog_fd >= 0) {
    epoll_pre_event = targ_read_new_events;
  }

  if (binlogname && !binlog_disabled) {
    assert (append_to_binlog (Binlog) == log_readto_pos);
    int res = retarget_all_dynamic_ads ();
    vkprintf (1, "%d periodic ads retargeted; log_last_ts = %d\n", res, log_last_ts);
  }

  if (weights_engine_address) {
    if (targ_weights_create_target (weights_engine_address) < 0) {
      exit (1);
    }
  }

  for (i = 0; !pending_signals; i++) {
    if (verbosity > 0 && !(i & 4095)) {
      kprintf ("epoll_work(): %d out of %d connections, network buffers: %d used, %d out of %d allocated\n",
               active_connections, maxconn, NB_used, NB_alloc, NB_max);
    }
    epoll_work (10);
  
    if (sigpoll_cnt > 0) {
      vkprintf (2, "after epoll_work(), sigpoll_cnt=%d\n", sigpoll_cnt);
      sigpoll_cnt = 0;
    }

    check_all_aio_completions ();
    tl_restart_all_ready ();

    if (now != prev_time) {
      prev_time = now;
      cron();
    }
    
    if (pending_signals) {
      break;
    }

    if (epoll_pre_event) {
      epoll_pre_event ();
    }
    
    if (quit_steps && !--quit_steps) break;
  }

  epoll_close (sfd);
  close (sfd);
  
  flush_binlog_last ();
  sync_binlog (2);
}

/*
 *
 *                MAIN
 *
 */

void usage (void) {
  printf ("usage: %s [-v] [-w] [-r] [-D] [-i] [-n<nice>] [-p<port>] [-u<username>] [-b<backlog>] [-c<max-conn>] [-a<binlog-replica-name>] [-l<log-name>] <replica-name>\n"
            "\t" VERSION_STR " compiled at " __DATE__ " " __TIME__ " by gcc " __VERSION__ " "
#ifdef __LP64__
          "64-bit"
#else
          "32-bit"
#endif
          " after commit " COMMIT "\n"
          "\tPerforms targeting and people search queries using given indexes\n",
          progname);
  parse_usage ();
  exit (2);
}

int f_parse_option (int val) {
  long long x;
  char c;
  switch (val) {
  case 'i':
    targeting_disabled = 1;
    break;
  case 'I':
    index_mode = 1;
    break;
  case 'C':
    binlog_check_mode++;
    break;
  case 'S':
    use_stemmer ^= 1;
    break;
  case 'D':
    delay_targeting = 0;
    break;
  case 'R':
    read_stats_filename = optarg;
    break;
  case 'W':
    write_stats_filename = optarg;
    break;
  case 'H':
    c = 0;
    assert (sscanf (optarg, "%lld%c", &x, &c) >= 1);
    switch (c | 0x20) {
      case 'k':  x <<= 10; break;
      case 'm':  x <<= 20; break;
      case 'g':  x <<= 30; break;
      case 't':  x <<= 40; break;
      default: assert (c == 0x20);
    }
    if (val == 'H' && x >= (1LL << 20) && x <= (sizeof(long) == 4 ? (3LL << 30) : (100LL << 30))) {
      dynamic_data_buffer_size = x;
    }
    break;
  default:
    return -1;
  }
  return 0;
}

int main (int argc, char *argv[]) {
  int i; 

  //signal (SIGRTMAX, sigrtmax_handler);
  set_debug_handlers ();

  progname = argv[0];
  i = strlen (progname);

  if (i >= 5 && !strcmp (progname + i - 5, "index")) {
    index_mode = 1;
  }

  parse_option ("no-targeting", no_argument, 0, 'i', "Disable targeting, search only replica");
  parse_option ("index", no_argument, 0, 'I', "Reindex");
  parse_option ("check-binlog", no_argument, 0, 'C', "Binlog check mode");
  parse_option ("no-stemmer", no_argument, 0, 'S', "Disable stemmer");
  parse_option ("no-delay", no_argument, 0, 'D', "process all targeting requests immediatly while reading binlog");
  parse_option ("log-long-queries", no_argument, 0, 'w', 0);
  parse_option ("read-stats-file", required_argument, 0, 'R', 0);
  parse_option ("write-stats-file", required_argument, 0, 'W', 0);
  parse_option ("weights-engine", required_argument, 0, 'U', "<host:port> of weights-engine");

  use_stemmer = 1;
  use_aio = 1;
  parse_engine_options_long (argc, argv, f_parse_option);

  while ((i = getopt (argc, argv, "a:b:c:l:p:n:dhu:vrkiICDwSR:W:B:H:U:T")) != -1) {
    switch (i) {
    }
  }

  if (argc != optind + 1) {
    usage ();
    return 2;
  }

  if (!username && maxconn == MAX_CONNECTIONS && geteuid()) {
    maxconn = 1000; //not for root
  }

  if (maxconn < 16) {
    maxconn = 16;
  }

  if (raise_file_rlimit (maxconn) < 0) { // was: +16 
    kprintf ("fatal: cannot raise open file limit to %d\n", maxconn);
    exit (1);
  }

  if (port < PRIVILEGED_TCP_PORTS && binlog_check_mode <= 1 && !index_mode) {
    sfd = server_socket (port, settings_addr, backlog, 0);
    if (sfd < 0) {
      fprintf (stderr, "cannot open server socket at port %d: %m\n", port);
      exit (1);
    }
  }
  
  aes_load_pwd_file (0);
  
  if (change_user (username) < 0) {
    kprintf ("fatal: cannot change user to %s\n", username ? username : "(none)");
    exit(1);
  }
  
  init_is_letter ();
  
  if (use_stemmer) {
    stem_init ();
  }

#if !defined(_LP64)
  //  dynamic_data_buffer_size = dynamic_data_buffer_size * 0.8;
#else
  if (index_mode) {
    treespace_ints *= 3;
    wordspace_ints *= 4;
  }
#endif

  init_dyn_data ();
  if (udp_enabled) {
    init_server_PID (get_my_ipv4 (), port);
  }
  log_ts_interval = 0;

  if (!targeting_disabled) {
    AdSpace = create_treespace (treespace_ints, sizeof (struct intree_node) / 4);
  }
  WordSpace = create_treespace (wordspace_ints, sizeof (struct intree_node) / 4);

  if (engine_preload_filelist (argv[optind], binlogname) < 0) {
    kprintf ("cannot open binlog files for %s\n", binlogname ? binlogname : argv[optind]);
    exit (1);
  }

  //Snapshot reading
  Snapshot = open_recent_snapshot (engine_snapshot_replica);

  if (Snapshot) {
    engine_snapshot_name = Snapshot->info->filename;
    engine_snapshot_size = Snapshot->info->file_size;

    vkprintf (1, "load index file %s (size %lld)\n", engine_snapshot_name, engine_snapshot_size);
    index_load_time = -get_utime (CLOCK_MONOTONIC);

    i = load_index (Snapshot);

    index_load_time += get_utime (CLOCK_MONOTONIC);

    if (i < 0) {
      fprintf (stderr, "fatal: error %d while loading index file %s\n", i, engine_snapshot_name);
      exit(1);
    }

    vkprintf (1, "load index: done, jump_log_pos=%lld, alloc_mem=%ld out of %ld, time %.06lfs\n", 
              jump_log_pos, (long) (dyn_cur - dyn_first), (long) (dyn_last - dyn_first), index_load_time);

    //close_snapshot (Snapshot, 1);
  } else {
    engine_snapshot_name = NULL;
    engine_snapshot_size = 0;
  }

  // Reading Binlog
  vkprintf (2, "starting reading binlog\n");

  Binlog = open_binlog (engine_replica, jump_log_pos);
  if (!Binlog) {
    kprintf ("fatal: cannot find binlog for %s, log position %lld\n", engine_replica->replica_prefix, jump_log_pos);
    exit (1);
  }

  if (!jump_log_pos && read_stats_filename) {
    load_stats_file (read_stats_filename);
  }

  assert (AdStats.g[0].views); // global stats data MUST be present, otherwise anything fails

  binlogname = Binlog->info->filename;

  vkprintf (1, "replaying binlog file %s (size %lld)\n", binlogname, Binlog->info->file_size);
  binlog_load_time = -mytime();

  clear_log();

  init_log_data (jump_log_pos, jump_log_ts, jump_log_crc32);
  use_aio = -use_aio;

  vkprintf (1, "replay log events started\n");
  i = replay_log (0, 1);
 
  use_aio = -use_aio;
  if (verbosity > 0) {
    vkprintf (1, "replay log events finished\n");
    dump_profiler_data ();
  }

  if (delay_targeting) {
    vkprintf (1, "performing delayed ad activation started\n");

    int res = perform_delayed_ad_activation ();

    vkprintf (1, "performing delayed ad activation finished: %d ads activated\n", res);
  }

  assert (!binlog_check_errors || binlog_check_mode <= 1);

  if (binlog_check_mode >= 2) {
    return 0;
  }

  binlog_load_time += mytime();
  binlog_loaded_size = log_readto_pos - jump_log_pos;

  if (!binlog_disabled) {
    clear_read_log();
  }

  clear_write_log ();
  start_time = time (NULL);

  if (write_stats_filename) {
    save_stats_file (write_stats_filename);
  }

  //  vkprintf (1, "global sum_p stats: %lld %.3f %.6f\n", total_sump0, total_sump1, total_sump2);

  if (index_mode) {
    /* create_new_snapshot (Binlog, log_readto_pos); */
    return write_index (0);
  }

  binlog_crc32_verbosity_level = 6;
  
  tl_parse_function = targ_parse_function;
  tl_aio_timeout = 2.0;
  tl_stat_function = targ_stats;
  start_server ();

  return 0;
}

