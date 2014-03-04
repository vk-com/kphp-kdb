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
#include "weights-data.h"
#include "word-split.h"
#include "stemmer-new.h"
#include "net-connections.h"
#include "net-memcache-server.h"
#include "net-crypto-aes.h"
#include "am-stats.h"
#include "vv-tl-parse.h"
#include "TL/constants.h"
#include "vv-tl-aio.h"

#include "weights-interface-structures.h"
#include "am-server-functions.h"

#ifndef COMMIT
#define COMMIT "unknown"
#endif

#define	VERSION_STR	"weights-engine-0.01-r19"
const char FullVersionStr[] = VERSION_STR " compiled at " __DATE__ " " __TIME__ " by gcc " __VERSION__ " "
#ifdef __LP64__
  "64-bit"
#else
  "32-bit"
#endif
" after commit " COMMIT;

#define mytime() (get_utime(CLOCK_MONOTONIC))

int udp_enabled;

/*
 *
 *		WEIGHTS ENGINE
 *
 */

/* stats counters */
static long long incr_queries, set_half_life_queries, at_queries, get_vector_queries;
static double binlog_load_time, index_load_time;
static int index_mode;

#define STATS_BUFF_SIZE	(1 << 20)
static char stats_buff[STATS_BUFF_SIZE];

conn_type_t ct_weights_engine_server = {
  .magic = CONN_FUNC_MAGIC,
  .title = "weigths_engine_server",
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
  .rpc_close = default_tl_close_conn,
  .memcache_fallback_type = &ct_weights_engine_server,
  .memcache_fallback_extra = &memcache_methods
};

int weights_prepare_stats (struct connection *c);

int memcache_stats (struct connection *c) {
  int len = weights_prepare_stats (c);
  write_out (&c->Out, stats_buff, len);
  write_out (&c->Out, "END\r\n", 5);
  return 0;
}

int memcache_get (struct connection *c, const char *key, int key_len) {
  int dog_len = get_at_prefix_length (key, key_len);
  key += dog_len;
  key_len -= dog_len;
  if (key_len >= 5 && !memcmp (key, "stats", 5)) {
    int len = weights_prepare_stats (c);
    return return_one_key (c, key - dog_len, stats_buff, len);
  }
  return 0;
}

int weights_prepare_stats (struct connection *c) {
  stats_buffer_t sb;
  sb_prepare (&sb, c, stats_buff, STATS_BUFF_SIZE);
  sb_memory (&sb, AM_GET_MEMORY_USAGE_SELF);
  SB_BINLOG;
  SB_INDEX;

  SB_PRINT_QUERIES(incr_queries);
  SB_PRINT_QUERIES(set_half_life_queries);
  SB_PRINT_QUERIES(at_queries);
  SB_PRINT_QUERIES(get_vector_queries);

  SB_PRINT_I32(tot_vectors);
  SB_PRINT_I32(tot_amortization_tables);
  SB_PRINT_I32(tot_counters_arrays);
  SB_PRINT_I32(tot_subscriptions);
  SB_PRINT_I32(vector_hash_prime);

  weights_half_life_stat_t half_life;
  weights_half_life_stats (&half_life);
  SB_PRINT_I32(half_life.min);
  SB_PRINT_I32(half_life.max);
  sb_printf (&sb, "half_life.avg\t%.3lf\n", half_life.avg);

  sb_printf (&sb, "version\t%s\n", FullVersionStr);

  return sb.pos;
}

static void weights_stats (void) {
  weights_prepare_stats (0);
  tl_store_stats (stats_buff, 0);
}

static int check_vector_id (int vector_id) {
  if (vector_id <= 0) {
    tl_fetch_set_error_format (TL_ERROR_BAD_VALUE, "vector_id isn't positive");
    return -1;
  }
  if (vector_id % log_split_mod != log_split_min) {
    tl_fetch_set_error_format (TL_ERROR_BAD_VALUE, "wrong split: vector_id = %d, log_split_min = %d, log_split_mod = %d", vector_id, log_split_min, log_split_mod);
    return -1;
  }
  return 0;
}

TL_DO_FUN(weights_incr)
  if (binlog_disabled == 1) {
    tl_fetch_set_error_format (TL_ERROR_BINLOG_DISABLED, "Binlog disabled.");
    return -1;
  }
  if (e->coord_id < 0 || e->coord_id >= WEIGHTS_MAX_COORDS) {
    tl_fetch_set_error_format (TL_ERROR_BAD_VALUE, "coord_id is out of range");
    return -1;
  }
  if (check_vector_id (e->vector_id) < 0) {
    return -1;
  }
  if (!e->value) {
    tl_fetch_set_error_format (TL_ERROR_BAD_VALUE, "zero increment value");
    return -1;
  }
  incr_queries++;
  int res = do_weights_incr (e->vector_id, e->coord_id, e->value);
  tl_store_int (TL_MAYBE_TRUE);
  tl_store_int (res);
TL_DO_FUN_END
TL_DEFAULT_PARSE_FUN(weights_incr)

TL_DO_FUN(weights_set_half_life)
  if (binlog_disabled == 1) {
    tl_fetch_set_error_format (TL_ERROR_BINLOG_DISABLED, "Binlog disabled.");
    return -1;
  }
  if (e->coord_id < 0 || e->coord_id >= WEIGHTS_MAX_COORDS) {
    tl_fetch_set_error_format (TL_ERROR_BAD_VALUE, "coord_id is out of range");
    return -1;
  }
  if (e->half_life <= 0) {
    tl_fetch_set_error_format (TL_ERROR_BAD_VALUE, "half_life isn't positive");
    return -1;
  }
  set_half_life_queries++;
  int res = do_weights_set_half_life (e->coord_id, e->half_life);
  tl_store_int (TL_BOOL_STAT);
  if (res < 0) {
    tl_store_int (0);
    tl_store_int (1);
  } else {
    tl_store_int (1);
    tl_store_int (0);
  }
  tl_store_int (0);
TL_DO_FUN_END
TL_DEFAULT_PARSE_FUN(weights_set_half_life)

TL_DO_FUN(weights_at)
  if (e->coord_id < 0 || e->coord_id >= WEIGHTS_MAX_COORDS) {
    tl_fetch_set_error_format (TL_ERROR_BAD_VALUE, "coord_id is out of range");
    return -1;
  }
  if (check_vector_id (e->vector_id) < 0) {
    return -1;
  }
  at_queries++;
  int value;
  int res = weights_at (e->vector_id, e->coord_id, &value);
  if (res < 0) {
    tl_store_int (TL_MAYBE_FALSE);
  } else {
    tl_store_int (TL_MAYBE_TRUE);
    tl_store_int (value);
  }
TL_DO_FUN_END
TL_DEFAULT_PARSE_FUN(weights_at)

TL_DO_FUN(weights_get_vector)
  if (check_vector_id (e->vector_id) < 0) {
    return -1;
  }
  get_vector_queries++;
  int r[WEIGHTS_MAX_COORDS];
  int res = weights_get_vector (e->vector_id, r);
  if (res < 0) {
    tl_store_int (TL_MAYBE_FALSE);
  } else {
    int i;
    tl_store_int (TL_MAYBE_TRUE);
    tl_store_int (res);
    for (i = 0; i < res; i++) {
      tl_store_int (r[i]);
    }
  }
TL_DO_FUN_END
TL_DEFAULT_PARSE_FUN(weights_get_vector)

TL_DO_FUN(weights_subscribe)
  int half_life[WEIGHTS_MAX_COORDS];
  int r = weights_subscribe (TL_OUT, e->coord_ids_num, e->coord_ids, e->vector_rem, e->vector_mod, e->updates_start_time, e->updates_seek_limit, e->updates_limit, e->small_updates_seek_limit, e->small_updates_limit, half_life);
  if (r < 0) {
    tl_store_int (TL_MAYBE_FALSE);
  } else {
    tl_store_int (TL_MAYBE_TRUE);
    tl_store_int (e->coord_ids_num);
    tl_store_raw_data (half_life, 4 * e->coord_ids_num);
  }
TL_DO_FUN_END

TL_PARSE_FUN(weights_subscribe, void)
  e->vector_rem = tl_fetch_int ();
  e->vector_mod = tl_fetch_positive_int ();
  if (!(e->vector_rem >= 0 && e->vector_rem < e->vector_mod)) {
    tl_fetch_set_error_format (TL_ERROR_BAD_VALUE, "vector_rem isn't in semi-interval [0, vector_mod): vector_rem = %d, vector_mode = %d", e->vector_rem, e->vector_mod);
    return 0;
  }
  e->coord_ids_num = tl_fetch_int ();
  if (e->coord_ids_num < 0 || e->coord_ids_num > WEIGHTS_MAX_COORDS) {
    tl_fetch_set_error_format (TL_ERROR_BAD_VALUE, "Invalid coord_ids size: num = %d", e->coord_ids_num);
    return 0;
  }
  extra->size += 4 * e->coord_ids_num;
  tl_fetch_raw_data (e->coord_ids, 4 * e->coord_ids_num);
  e->updates_start_time = tl_fetch_int ();
  e->updates_seek_limit = tl_fetch_int ();
  e->updates_limit = tl_fetch_int ();
  e->small_updates_seek_limit = tl_fetch_int ();
  e->small_updates_limit = tl_fetch_int ();
TL_PARSE_FUN_END

/*
TL_DO_FUN(subscription_stop)
  weights_subscription_stop (TL_OUT);
TL_DO_FUN_END

TL_PARSE_FUN(subscription_stop, void)
TL_PARSE_FUN_END
*/

struct tl_act_extra *weights_parse_function (long long actor_id) {
  vkprintf (3, "%s: actor_id = %lld\n", __func__, actor_id);
  if (actor_id != 0) {
    tl_fetch_set_error ("Weights only support actor_id = 0", TL_ERROR_WRONG_ACTOR_ID);
    return 0;
  }
  int op = tl_fetch_int ();
  if (tl_fetch_error ()) {
    return 0;
  }
  switch (op) {
  case TL_WEIGHTS_INCR:
    return tl_weights_incr ();
  case TL_WEIGHTS_SET_HALF_LIFE:
    return tl_weights_set_half_life ();
  case TL_WEIGHTS_AT:
    return tl_weights_at ();
  case TL_WEIGHTS_GET_VECTOR:
    return tl_weights_get_vector ();
  case TL_WEIGHTS_SUBSCRIBE:
    return tl_weights_subscribe ();
  default:
    tl_fetch_set_error_format (TL_ERROR_UNKNOWN_FUNCTION_ID, "Unknown op %08x", op);
    return 0;
  }
}


void cron (void) {
  create_all_outbound_connections ();
  flush_binlog ();
}

engine_t weights_engine;
server_functions_t weights_functions = {
  .cron = cron,
  .save_index = save_index
};

void start_server (void) {
  int i;

  tl_parse_function = weights_parse_function;
  tl_stat_function = weights_stats;
  tl_aio_timeout = 2.0;

  server_init (&weights_engine, &weights_functions, &ct_rpc_server, &rpc_methods);

  int quit_steps = 0;

  for (i = 0; ; i++) {
    if (verbosity > 1 && !(i & 255)) {
      kprintf ("epoll_work(): %d out of %d connections, network buffers: %d used, %d out of %d allocated\n",
	       active_connections, maxconn, NB_used, NB_alloc, NB_max);
    }

    epoll_work (37);
    if (!process_signals ()) {
      break;
    }
    tl_restart_all_ready ();

    weights_subscriptions_work ();

    if (quit_steps && !--quit_steps) break;
  }

  server_exit (&weights_engine);
}

/*
 *
 *		MAIN
 *
 */

void usage (void) {
  printf ("usage: %s [-v] [-r] [-i] [-p<port>] [-u<username>] [-b<backlog>] [-c<max-conn>] [-a<binlog-name>] [-l<log-name>] <binlog>\n"
	  "\t%s\n"
	  "\tPerforms weights queries.\n"
	  , progname,
	  FullVersionStr);
  parse_usage ();
  exit(2);
}

int f_parse_option (int val) {
  switch (val) {
  case 'U':
    udp_enabled++;
    break;
  case 'i':
    index_mode = 1;
    break;
  default:
    fprintf (stderr, "Unimplemented option '%c' (%d)\n", (char) val, val);
    return -1;
  }
  return 0;
}

int main (int argc, char *argv[]) {
  int i;
  set_debug_handlers ();
  parse_option ("index", no_argument, 0, 'i', "reindex");
  remove_parse_option (201);
  parse_option ("udp", no_argument, 0, 'U', "enables udp message support");
  parse_engine_options_long (argc, argv, f_parse_option);

  progname = argv[0];

  if (argc != optind + 1) {
    usage();
    return 2;
  }

  engine_init (&weights_engine, aes_pwd_file, index_mode);

  if (engine_preload_filelist (argv[optind], binlogname) < 0) {
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

    i = load_index ();

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

  if (i < 0) {
    kprintf ("fatal: error reading binlog\n");
    exit (1);
  }

  clear_write_log ();
  start_time = time (0);

  if (index_mode) {
    save_index (0);
  } else {
    start_server ();
  }

  return 0;
}
