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

    Copyright 2013 Vkontakte Ltd
              2013 Anton Maydell
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

#include "md5.h"
#include "server-functions.h"
#include "search-y-data.h"
#include "word-split.h"
#include "stemmer-new.h"
#include "net-connections.h"
#include "net-memcache-server.h"
#include "net-crypto-aes.h"
#include "search-profile.h"
#include "utils.h"
#include "am-stats.h"
#include "vv-tl-parse.h"
#include "search-tl.h"
#include "vv-tl-aio.h"

#include "search-interface-structures.h"

#include "am-server-functions.h"

#ifndef COMMIT
#define COMMIT "unknown"
#endif

#define	VERSION_STR	"search-y-engine-0.01-r20"
const char FullVersionStr[] = VERSION_STR " compiled at " __DATE__ " " __TIME__ " by gcc " __VERSION__ " "
#ifdef __LP64__
  "64-bit"
#else
  "32-bit"
#endif
" after commit " COMMIT;

#define mytime() (get_utime(CLOCK_MONOTONIC))

#define MAX_QUERY_LEN 10000
#define MAX_TAG_LEN 10000
#define MAX_TEXT_LEN 10000

/*
 *
 *		SEARCH ENGINE
 *
 */

/* stats counters */
static long long search_queries, increment_queries, set_rate_queries, delete_item_queries, set_item_queries;
static double binlog_load_time, index_load_time;

#define STATS_BUFF_SIZE	(1 << 20)
static char stats_buff[STATS_BUFF_SIZE];

conn_type_t ct_searchy_engine_server = {
  .magic = CONN_FUNC_MAGIC,
  .title = "searchy_engine_server",
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

int memcache_stats (struct connection *c);
int memcache_get (struct connection *c, const char *key, int key_len);

struct memcache_server_functions memcache_methods = {
  .execute = mcs_execute,
  .mc_store = mcs_store,
  .mc_get_start = mcs_get_start,
  .mc_get = memcache_get,
  .mc_get_end = mcs_get_end,
  .mc_incr = mcs_incr,
  .mc_delete = mcs_delete,
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
  .memcache_fallback_type = &ct_searchy_engine_server,
  .memcache_fallback_extra = &memcache_methods
};

int searchy_prepare_stats (struct connection *c);

int memcache_stats (struct connection *c) {
  int len = searchy_prepare_stats (c);
  write_out (&c->Out, stats_buff, len);
  write_out (&c->Out, "END\r\n", 5);
  return 0;
}

int memcache_get (struct connection *c, const char *key, int key_len) {
  int dog_len = get_at_prefix_length (key, key_len);
  key += dog_len;
  key_len -= dog_len;
  if (key_len >= 5 && !memcmp (key, "stats", 5)) {
    int len = searchy_prepare_stats (c);
    return return_one_key (c, key - dog_len, stats_buff, len);
  }
  if (key_len == 20 && !memcmp (key, "worst_search_queries", 20)) {
    int len = search_query_worst (stats_buff, sizeof (stats_buff));
    return return_one_key (c, key - dog_len, stats_buff, len);
  }
  return 0;
}

int searchy_prepare_stats (struct connection *c) {
  stats_buffer_t sb;
  sb_prepare (&sb, c, stats_buff, STATS_BUFF_SIZE);
  sb_memory (&sb, AM_GET_MEMORY_USAGE_SELF);
  sb_printf (&sb, "malloc_memory_used\t%lld\n", get_malloc_memory_used ());
  SB_BINLOG;
  SB_INDEX;
  sb_printf (&sb,
    "index_items\t%d\n"
    "index_words\t%d\n"
    "index_data_bytes\t%lld\n",
    idx_items, idx_words, idx_bytes);

  sb_printf (&sb, "left_subtree_size_threshold\t%d\n", Header.left_subtree_size_threshold);
  SB_PRINT_QUERIES(search_queries);
  SB_PRINT_QUERIES(increment_queries);
  SB_PRINT_QUERIES(set_rate_queries);
  SB_PRINT_QUERIES(delete_item_queries);
  SB_PRINT_QUERIES(set_item_queries);

  sb_printf (&sb,
      "items\t%d\n"
      "items_marked_as_deleted\t%d\n"
      "items_freed\t%d\n"
      "index_items_deleted\t%d\n"
      "tree_nodes_allocated\t%d\n"
      "tree_nodes_unused\t%d\n",
    tot_items,
    del_items,
    tot_freed_deleted_items,
    mod_items,
    alloc_tree_nodes,
    free_tree_nodes);

  SB_PRINT_I64(tree_positions_bytes);
  SB_PRINT_I32(decoder_positions_max_capacity);
  SB_PRINT_I32(max_search_query_memory);
  SB_PRINT_I32(use_stemmer);
  SB_PRINT_I32(universal);
  SB_PRINT_I32(hashtags_enabled);
  SB_PRINT_I32(wordfreqs_enabled);
  SB_PRINT_I32(stemmer_version);
  SB_PRINT_I32(word_split_version);
  SB_PRINT_I32(word_split_utf8);
  SB_PRINT_I32(tag_owner);
  SB_PRINT_I32(listcomp_version);
  SB_PRINT_I32(creation_date);

  sb_printf (&sb, "version\t%s\n", FullVersionStr);

  return sb.pos;
}

static void searchy_stats (void) {
  searchy_prepare_stats (0);
  tl_store_stats (stats_buff, 0);
}

static long long tl_get_item_id (void) {
  int n = tl_fetch_int ();
  if (n != 1 && n != 2) {
    tl_fetch_set_error ("number of ints in id should be 1 or 2", TL_ERROR_VALUE_NOT_IN_RANGE);
    return -1;
  }
  int owner_id = 0, item_id;
  if (n == 2) {
    owner_id = tl_fetch_int ();
  }
  item_id = tl_fetch_int ();
  if (tl_fetch_error ()) {
    return -1;
  }
  if (item_id <= 0) {
    tl_fetch_set_error ("item_id should be positive", TL_ERROR_VALUE_NOT_IN_RANGE);
    return -1;
  }
  return (n == 2) ? ((long long) item_id << 32) + (unsigned) owner_id : item_id;
}

TL_DO_FUN(set_rate)
  if (binlog_disabled == 1) {
    tl_fetch_set_error_format (TL_ERROR_BINLOG_DISABLED, "Binlog disabled.");
    return -1;
  }
  set_rate_queries++;
  int res = do_set_rate_new (e->item_id, e->rate_type, e->rate_value);
  tl_store_int (res <= 0 ? TL_BOOL_FALSE : TL_BOOL_TRUE);
TL_DO_FUN_END

TL_PARSE_FUN(set_rate)
  e->item_id = tl_get_item_id ();
  if (e->item_id == -1) { return 0; }
  e->rate_type = tl_fetch_int ();
  if (e->rate_type < 0 || e->rate_type >= MAX_RATES) {
    tl_fetch_set_error_format (TL_ERROR_VALUE_NOT_IN_RANGE, "Rate_type should be in range (0...%d)", MAX_RATES - 1);
    return 0;
  }
  e->rate_value = tl_fetch_int ();
TL_PARSE_FUN_END

TL_DO_FUN(incr_rate)
  if (binlog_disabled == 1) {
    tl_fetch_set_error_format (TL_ERROR_BINLOG_DISABLED, "Binlog disabled.");
    return -1;
  }
  increment_queries++;
  int res = do_incr_rate_new (e->item_id, e->rate_type, e->rate_value);
  tl_store_int (res <= 0 ? TL_BOOL_FALSE : TL_BOOL_TRUE);
TL_DO_FUN_END

TL_PARSE_FUN(incr_rate, int decrement)
  e->item_id = tl_get_item_id ();
  if (e->item_id == -1) { return 0; }
  e->rate_type = tl_fetch_int ();
  if (e->rate_type < 0 || e->rate_type >= MAX_RATES) {
    tl_fetch_set_error_format (TL_ERROR_VALUE_NOT_IN_RANGE, "Rate_type should be in range (0...%d)", MAX_RATES - 1);
    return 0;
  }
  e->rate_value = tl_fetch_int ();
  if (decrement) {
    e->rate_value *= -1;
  }
TL_PARSE_FUN_END

TL_DO_FUN(get_rate)
  int r = 0;
  int res = get_single_rate (&r, e->item_id, e->rate_type);
  if (res <= 0) {
    tl_store_int (TL_MAYBE_FALSE);
  } else {
    tl_store_int (TL_MAYBE_TRUE);
    tl_store_int (r);
  }
TL_DO_FUN_END

TL_PARSE_FUN(get_rate, void)
  e->item_id = tl_get_item_id ();
  if (e->item_id == -1) { return 0; }
  e->rate_type = tl_fetch_int ();
  if (e->rate_type < 0 || e->rate_type >= MAX_RATES) {
    tl_fetch_set_error_format (TL_ERROR_VALUE_NOT_IN_RANGE, "Rate_type should be in range (0...%d)", MAX_RATES - 1);
    return 0;
  }
TL_PARSE_FUN_END

static void copy_query (search_query_heap_en_t *A, void *arg) {
  struct tl_search *e = (struct tl_search *) arg;
  char *p = A->query, *end = A->query + sizeof (A->query);
  p += sprintf (p, "flags=>%d limit=>%d", e->flags, e->limit);
  if (e->flags & FLAG_SORT) {
    p += sprintf (p, " rate_type=>%d", e->rate_type);
  }
  int *data = e->data;
  int i;
  for (i = 0; i < e->restr_num; i++) {
    p += snprintf (p, end - p, " [%d,%d,%d]", data[0], data[1], data[2]);
    if (p >= end) {
      return;
    }
    data += 3;
  }
  snprintf (p, end - p, " query=>%s", (char *) data);
}

TL_DO_FUN(search)
  Q_limit = e->limit;
  if (Q_limit < 0) { Q_limit = 0; }
  Q_order = 0;

  if (e->flags & FLAG_SORT) {
    Q_order |= e->rate_type;
    if (e->flags & FLAG_SORT_DESC) {
      Q_order |= FLAG_REVERSE_SEARCH;
    }
  } else {
    if (e->flags & FLAG_RAND) {
      Q_order |= 17;
    } else {
      Q_order |= MAX_RATES;
      if (e->flags & FLAG_SORT_DESC) {
        Q_order |= FLAG_REVERSE_SEARCH;
      }
    }
  }
  int *data = e->data;
  init_ranges ();
  int i;
  for (i = 0; i < e->restr_num; i++) {
    add_range (data[0], data[1], data[2]);
    data += 3;
  }
  sort_ranges ();

  assert (data == e->data + 3 * e->restr_num);
  search_query_heap_en_t E;
  search_query_start (&E);
  int res = searchy_perform_query ((char *) data);
  search_query_end (&E, res, (void *) e, copy_query);
  if (res < 0) {
    return -1;
  }
  tl_store_int (TL_SEARCH_RESULT);
  int n = 1;
  for (i = 0; i < R_cnt; i++) {
    if (R[i]->item_id >> 32) {
      n = 2;
      break;
    }
  }
  tl_store_int (n);
  tl_store_int (res);
  vkprintf (2, "res = %d, R_cnt = %d, R_tot = %d\n", res, R_cnt, R_tot);
  tl_store_int (R_cnt);
  for (i = 0; i < R_cnt; i++) {
    if (n == 1) {
      tl_store_int (R[i]->item_id);
    } else {
      tl_store_int (R[i]->item_id);
      tl_store_int (R[i]->item_id >> 32);
    }
    if (e->flags & FLAG_SORT) {
      tl_store_int (RV[i]);
    }
  }
TL_DO_FUN_END

TL_PARSE_FUN(search,void)
  search_queries++;
  e->flags = tl_fetch_int ();
  e->limit = tl_fetch_int ();

  if (e->flags & (FLAG_HASH_CHANGE | FLAG_RELEVANCE | FLAG_OPTTAG | FLAG_CUSTOM_RATE_WEIGHT | FLAG_DECAY | FLAG_CUSTOM_PRIORITY_WEIGHT | FLAG_EXTENDED_MODE | FLAG_OCCURANCE_COUNT | FLAG_EXACT_HASH)) {
    tl_fetch_set_error_format (TL_ERROR_QUERY_INCORRECT, "Trying to use disabled features in searchy (flags %08x)", e->flags);
    return 0;
  }

  if (e->flags & FLAG_SORT) {
    e->rate_type = tl_fetch_int ();
    if (e->rate_type < 0 || e->rate_type >= MAX_RATES) {
      tl_fetch_set_error_format (TL_ERROR_VALUE_NOT_IN_RANGE, "rate_type should be in range (0...%d)", MAX_RATES);
      return 0;
    }
  } else {
    e->rate_type = -1;
  }

  if (e->flags & FLAG_RESTR) {
    e->restr_num = tl_fetch_int ();
    if (e->restr_num < 0 || e->restr_num > MAX_RATES + 2) {
      tl_fetch_set_error_format (TL_ERROR_VALUE_NOT_IN_RANGE, "maximal number of restrictions is %d", MAX_RATES + 2);
      return 0;
    }
    tl_fetch_raw_data ((char *)(e->data), 12 * e->restr_num);
  } else {
    e->restr_num = 0;
  }

  extra->size += 12 * e->restr_num;
  e->rate_weights_num = 0;
  char *text = (char *)(e->data + e->restr_num * 3);
  e->text_len = tl_fetch_string0 (text, MAX_QUERY_LEN);
  extra->size += e->text_len + 1;
TL_PARSE_FUN_END

TL_DO_FUN(delete_item)
  if (binlog_disabled == 1) {
    tl_fetch_set_error_format (TL_ERROR_BINLOG_DISABLED, "Binlog disabled.");
    return -1;
  }
  delete_item_queries++;
  int res = do_delete_item (e->item_id);
  tl_store_int (res <= 0 ? TL_BOOL_FALSE : TL_BOOL_TRUE);
TL_DO_FUN_END

TL_PARSE_FUN(delete_item, void)
  e->item_id = tl_get_item_id ();
  if (e->item_id == -1) { return 0; }
TL_PARSE_FUN_END

TL_DO_FUN(set_item)
  if (binlog_disabled == 1) {
    tl_fetch_set_error_format (TL_ERROR_BINLOG_DISABLED, "Binlog disabled.");
    return -1;
  }
  set_item_queries++;
  int res = do_change_item (e->text, e->size, e->item_id, e->rate, e->sate);
  tl_store_int (res <= 0 ? TL_BOOL_FALSE : TL_BOOL_TRUE);
TL_DO_FUN_END

TL_PARSE_FUN(set_item, void)
  e->item_id = tl_get_item_id ();
  if (e->item_id == -1) { return 0; }
  e->rate = tl_fetch_int ();
  e->sate = tl_fetch_int ();
  e->size = tl_fetch_string0 (e->text, MAX_TEXT_LEN);
  extra->size += e->size + 1;
TL_PARSE_FUN_END

struct tl_act_extra *searchy_parse_function (long long actor_id) {
  if (actor_id != 0) {
    tl_fetch_set_error ("Search only support actor_id = 0", TL_ERROR_WRONG_ACTOR_ID);
    return 0;
  }
  int op = tl_fetch_int ();
  if (tl_fetch_error ()) {
    return 0;
  }
  switch (op) {
  case TL_SEARCH_SET_RATE:
    return tl_set_rate ();
  case TL_SEARCH_INCR_RATE:
    return tl_incr_rate (0);
  case TL_SEARCH_DECR_RATE:
    return tl_incr_rate (1);
  case TL_SEARCH_GET_RATE:
    return tl_get_rate ();
  case TL_SEARCH_DELETE_ITEM:
    return tl_delete_item ();
  case TL_SEARCH_SET_ITEM:
    return tl_set_item ();
  case TL_SEARCH_SEARCH:
    return tl_search ();
  default:
    tl_fetch_set_error_format (TL_ERROR_UNKNOWN_FUNCTION_ID, "Unknown op %08x", op);
    return 0;
  }
}

void cron (void) {
  create_all_outbound_connections ();
  flush_binlog ();
  search_query_remove_expired ();
}

engine_t searchy_engine;
server_functions_t searchy_functions = {
  .cron = cron,
};

void start_server (void) {
  int i;

  tl_parse_function = searchy_parse_function;
  tl_stat_function = searchy_stats;
  tl_aio_timeout = 2.0;

  server_init (&searchy_engine, &searchy_functions, &ct_rpc_server, &rpc_methods);

  int quit_steps = 0;

  for (i = 0; ; i++) {
    if (verbosity > 1 && !(i & 255)) {
      kprintf ("epoll_work(): %d out of %d connections, network buffers: %d used, %d out of %d allocated\n",
	       active_connections, maxconn, NB_used, NB_alloc, NB_max);
    }

    epoll_work (57);
    if (!process_signals ()) {
      break;
    }
    tl_restart_all_ready ();
    if (quit_steps && !--quit_steps) break;
  }

  server_exit (&searchy_engine);
}

/*
 *
 *		MAIN
 *
 */

void usage (void) {
  printf ("usage: %s [-v] [-r] [-i] [-t] [-A] [-S] [-p<port>] [-u<username>] [-b<backlog>] [-c<max-conn>] [-a<binlog-name>] [-l<log-name>] <huge-index-file> [<metaindex-file>]\n"
	  "\t%s\n"
	  "\tPerforms search queries using given indexes\n"
	  "\tIf <metaindex-file> is not specified, <huge-index-file>.idx is used\n"
	  "\t-v\toutput statistical and debug information into stderr\n"
	  "\t-r\tread-only binlog (don't log new events)\n"
	  "\t-t\tenable tags (*word @word #word are considered words)\n"
	  "\t-A\tenable universal tag\n"
	  "\t-S\tuse stemmer\n"
	  "\t-I\timport only mode (in this mode engine only writes log indexing log events and don't update index tree)\n"
	  "\t-U\tenable UTF-8 mode\n"
	  "\t-D\tstore in item's date modification time (default: first creation time)\n"
	  "\t-B<max-binlog-size>\tdefines maximum size of each binlog file\n"
	  "\t-O\tenable tag owner mode\n",
	  progname,
	  FullVersionStr);
  exit(2);
}

long long parse_memory_limit (int option, const char *limit) {
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
  return x;
}

int main (int argc, char *argv[]) {
  int i;
  long long y;

  set_debug_handlers ();
  char *custom_encr = 0;

  progname = argv[0];
  while ((i = getopt (argc, argv, "AB:CDE:IOSUa:b:c:dhl:p:rtu:v")) != -1) {
    switch (i) {
    case 'A':
      universal = 1;
      break;
    case 'B':
      y = parse_memory_limit ('B', optarg);
      if (y >= 1024 && y < (1LL << 60)) {
        max_binlog_size = y;
      }
      break;
    case 'C':
      binlog_check_mode = 1;
      break;
    case 'D':
      creation_date = 0;
      break;
    case 'E':
      custom_encr = optarg;
      break;
    case 'I':
      import_only_mode = 1;
      break;
    case 'O':
      tag_owner = 1;
      break;
    case 'S':
      use_stemmer = 1;
      break;
    case 'U':
      word_split_utf8 = 1;
      break;
    case 'a':
      binlogname = optarg;
      break;
    case 'b':
      backlog = atoi(optarg);
      if (backlog <= 0) backlog = BACKLOG;
      break;
    case 'c':
      maxconn = atoi(optarg);
      if (maxconn <= 0 || maxconn > MAX_CONNECTIONS) {
      	maxconn = MAX_CONNECTIONS;
      }
      break;
    case 'd':
      daemonize ^= 1;
      break;
    case 'h':
      usage ();
      return 2;
    case 'l':
      logname = optarg;
      break;
    case 'p':
      port = atoi (optarg);
      break;
    case 'r':
      binlog_disabled = 1;
      break;
    case 't':
      hashtags_enabled = 1;
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
  if (argc != optind + 1) {
    usage();
    return 2;
  }

  init_is_letter();
  if (hashtags_enabled) {
    enable_is_letter_sigils ();
  }
  if (use_stemmer) {
    stem_init ();
  }

  engine_init (&searchy_engine, custom_encr, 0);

  if (engine_preload_filelist ( argv[optind], binlogname) < 0) {
    kprintf ("cannot open binlog files for %s\n", binlogname ? binlogname : argv[optind]);
    exit (1);
  }

  vkprintf (3, "engine_preload_filelist done\n");

  //Snapshot reading
  Snapshot = open_recent_snapshot (engine_snapshot_replica);

  if (Snapshot) {
    engine_snapshot_name = strdup (Snapshot->info->filename);
    engine_snapshot_size = Snapshot->info->file_size;
    vkprintf (1, "load index file %s (size %lld)\n", engine_snapshot_name, engine_snapshot_size);

    index_load_time = -mytime();

    i = load_index (Snapshot);

    if (i < 0) {
      kprintf ("load_index returned fail code %d. Skipping index.\n", i);
      memset (&Header, 0, sizeof (Header));
    }

    index_load_time += mytime();
  } else {
    engine_snapshot_name = NULL;
    engine_snapshot_size = 0;
    index_load_time = 0;
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

  init_log_data (jump_log_pos, jump_log_ts, jump_log_crc32);

  vkprintf (1, "replay log events started\n");
  i = replay_log (0, 1);
  vkprintf (1, "replay log events finished\n");

  binlog_load_time += mytime();

  if (!binlog_disabled) {
    clear_read_log();
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

  start_server ();

  return 0;
}
