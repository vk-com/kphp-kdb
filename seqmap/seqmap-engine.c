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
              2013 Vitaliy Valtman
*/

#define	_FILE_OFFSET_BITS	64

#include <assert.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <ctype.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/mman.h>
#include <sys/wait.h>
#include <signal.h>

#include "crc32.h"
#include "kdb-data-common.h"
#include "net-events.h"
#include "net-buffers.h"
#include "server-functions.h"
#include "net-connections.h"
#include "net-memcache-server.h"
#include "net-crypto-aes.h"
#include "kdb-binlog-common.h"
#include "kfs.h"

#include "vv-tl-parse.h"
#include "vv-tl-aio.h"
#include "am-stats.h"

#include "seqmap-data.h"
#include "seqmap-tl.h"
#include "seqmap-interface-structures.h"

#define	MAX_KEY_LEN	255
#define MAX_KEY_RETRIES	3
#define	MAX_VALUE_LEN	(1 << 20)

#define	VERSION "1.03"
#define	VERSION_STR "seqmap-engine "VERSION
#ifndef COMMIT
#define COMMIT "unknown"
#endif
const char FullVersionStr[] = "version\t" VERSION_STR " compiled at " __DATE__ " " __TIME__ " by gcc " __VERSION__ " "
#ifdef __LP64__
     "64-bit"
#else
     "32-bit"
#endif
      " after commit " COMMIT;
;

#define TCP_PORT 11211

#define MAX_NET_RES	(1L << 16)

#define	PING_INTERVAL	10
#define	RESPONSE_FAIL_TIMEOUT	5

#define SEC_IN_MONTH (60 * 60 * 24 * 30)

#define mytime() get_utime (CLOCK_MONOTONIC)

/*
 *
 *		MEMCACHED PORT
 *
 */


conn_type_t ct_seqmap_engine_server = {
  .magic = CONN_FUNC_MAGIC,
  .title = "seqmap_engine_server",
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
#ifndef NOAES
  .crypto_init = aes_crypto_init,
  .crypto_free = aes_crypto_free,
  .crypto_encrypt_output = aes_crypto_encrypt_output,
  .crypto_decrypt_input = aes_crypto_decrypt_input,
  .crypto_needed_output_bytes = aes_crypto_needed_output_bytes,
#endif
};




int backlog = BACKLOG, port = TCP_PORT, maxconn = MAX_CONNECTIONS, daemonize = 0;
long long max_memory = MAX_MEMORY;

struct in_addr settings_addr;
int verbosity = 0, interactive = 0;
int return_false_if_not_found = 0;
int quit_steps;
double index_load_time, binlog_load_time;
int jump_log_ts;
unsigned jump_log_crc32;
long long jump_log_pos;
long long binlog_loaded_size;
long long binlog_readed;
int index_mode = 0;
long long snapshot_size;
long long index_size;
int index_type;
volatile int force_write_index;
volatile int force_check_aio;
int metafile_size = METAFILE_SIZE;
int flush_often;
int reindex_on_low_memory;
int last_reindex_on_low_memory_time;
int disable_wildcard;
int protected_mode;
int udp_enabled;
char *aes_pwd_file;

int metafile_mode = 1;
char *progname = "seqmap-1.02", *username, *logname, *binlogname;

int pack_mode;
int pack_fd;

int start_time;

long long cmd_get, cmd_set, get_hits, get_missed, cmd_incr, cmd_decr, cmd_delete, cmd_version, cmd_stats, curr_items, total_items;
long long tot_response_words, tot_response_bytes;

#define STATS_BUFF_SIZE	(1 << 23)
char stats_buff[STATS_BUFF_SIZE];

char key_buff[MAX_KEY_LEN+1];
char value_buff[MAX_VALUE_LEN+1];

int memcache_stats (struct connection *c);

struct memcache_server_functions memcache_methods = {
  .execute = mcs_execute,
  .mc_store = mcs_store,
  .mc_get_start = mcs_get_start,
  .mc_get = mcs_get,
  .mc_get_end = mcs_get_end,
  .mc_incr = mcs_incr,
  .mc_delete = mcs_delete,
  .mc_version = mcs_version,
  .mc_stats = memcache_stats,
  .mc_check_perm = mcs_default_check_perm,
  .mc_init_crypto = mcs_init_crypto
};

struct rpc_server_functions memcache_rpc_server = {
  .execute = default_tl_rpcs_execute,
  .check_ready = server_check_ready,
  .flush_packet = rpcs_flush_packet,
  .rpc_check_perm = rpcs_default_check_perm,
  .rpc_init_crypto = rpcs_init_crypto,
  .rpc_close = default_tl_close_conn,
  .memcache_fallback_type = &ct_seqmap_engine_server,
  .memcache_fallback_extra = &memcache_methods
};

extern long long malloc_mem;
extern long long zalloc_mem;
int seqmap_prepare_stats (struct connection *c) {
  stats_buffer_t sb;
  sb_prepare (&sb, c, stats_buff, STATS_BUFF_SIZE);
  sb_memory (&sb, AM_GET_MEMORY_USAGE_SELF);
  sb_printf (&sb,
    "malloc_mem\t%ld\n"
    "zalloc_mem\t%ld\n",
    (long) malloc_mem,
    (long) zalloc_mem);

  SB_BINLOG;
  //data_prepare_stats (&sb);
  sb_printf (&sb, "%s\n", FullVersionStr);
  return sb.pos;
}

int memcache_stats (struct connection *c) {
  cmd_stats++;
  int stats_len = seqmap_prepare_stats (c);
  write_out (&c->Out, stats_buff, stats_len);
  write_out (&c->Out, "END\r\n", 5);
  return 0;
}

TL_DO_FUN(store)
  int res = do_store (e->mode, e->key_len, e->data, e->value_len, e->data + e->key_len, e->time, e->force);
  if (res < 0) { return res; }
  tl_store_int (res <= 0 ? TL_BOOL_FALSE : TL_BOOL_TRUE);
TL_DO_FUN_END

TL_DO_FUN(delete)
  int res = do_delete (e->key_len, e->data, e->force);
  if (res < 0) { return res; }
  tl_store_int (res <= 0 ? TL_BOOL_FALSE : TL_BOOL_TRUE);
TL_DO_FUN_END

TL_DO_FUN(get)
  struct item *I = get (e->key_len, e->data);
  if (!I) {
    return -2;
  }
  if (I->key_len < 0 || I->value_len < 0) {
    tl_store_int (TL_MAYBE_FALSE);
  } else {
    tl_store_int (TL_MAYBE_TRUE);
    tl_store_int (I->value_len);
    tl_store_raw_data ((char *)I->value, I->value_len * 4);
  }
TL_DO_FUN_END

TL_DO_FUN(get_range_count)
  int r = get_range_count (e->left_len, e->data, e->right_len, e->data + e->left_len);
  if (r < 0) { return r; }
  tl_store_int (TL_INT);
  tl_store_int (r);
TL_DO_FUN_END


#define R_SIZE (1 << 20)
static int R[R_SIZE];

TL_DO_FUN(get_range)
  int cnt = 0;
  int total = 0;
  int r = get_range (e->left_len, e->data, e->right_len, e->data + e->left_len, e->limit, R, R_SIZE, &cnt, &total);
  if (r < 0) { return r; }
  assert (cnt >= 0);
  tl_store_int (TL_VECTOR_TOTAL);
  tl_store_int (total);
  tl_store_int (cnt);
  tl_store_raw_data (R, 4 * r);
TL_DO_FUN_END

TL_PARSE_FUN(store,int mode, int force)
  e->mode = mode;
  e->force = force;
  e->key_len = tl_fetch_int ();
  if (e->key_len <= 0 || e->key_len > MAX_KEY_LEN) {
    tl_fetch_set_error_format (TL_ERROR_VALUE_NOT_IN_RANGE, "key should contain between 1 and %d ints", MAX_KEY_LEN);
    return 0;
  }
  tl_fetch_raw_data (e->data, 4 * e->key_len); 

  e->value_len = tl_fetch_int ();
  if (e->value_len <= 0 || e->value_len > MAX_VALUE_LEN) {
    tl_fetch_set_error_format (TL_ERROR_VALUE_NOT_IN_RANGE, "value should contain between 1 and %d ints", MAX_VALUE_LEN);
    return 0;
  }
  tl_fetch_raw_data (e->data + e->key_len, 4 * e->value_len); 
  extra->size += e->key_len * 4 + e->value_len * 4;
  e->time = tl_fetch_int ();
  if (e->time > 0 && e->time < 100000000) {
    e->time += now;
  }
TL_PARSE_FUN_END

TL_PARSE_FUN(delete,int force)
  e->force = force;
  e->key_len = tl_fetch_int ();
  if (e->key_len <= 0 || e->key_len > MAX_KEY_LEN) {
    tl_fetch_set_error_format (TL_ERROR_VALUE_NOT_IN_RANGE, "key should contain between 1 and %d ints", MAX_KEY_LEN);
    return 0;
  }
  tl_fetch_raw_data (e->data, 4 * e->key_len); 
TL_PARSE_FUN_END

TL_PARSE_FUN(get,void)
  e->key_len = tl_fetch_int ();
  if (e->key_len <= 0 || e->key_len > MAX_KEY_LEN) {
    tl_fetch_set_error_format (TL_ERROR_VALUE_NOT_IN_RANGE, "key should contain between 1 and %d ints", MAX_KEY_LEN);
    return 0;
  }
  tl_fetch_raw_data (e->data, 4 * e->key_len); 
TL_PARSE_FUN_END

TL_PARSE_FUN(get_range_count,int wildcard)
  e->left_len = tl_fetch_int ();
  if (e->left_len <= 0 || e->left_len > MAX_KEY_LEN) {
    tl_fetch_set_error_format (TL_ERROR_VALUE_NOT_IN_RANGE, "key should contain between 1 and %d ints", MAX_KEY_LEN);
    return 0;
  }
  tl_fetch_raw_data (e->data, 4 * e->left_len);
  if (!wildcard) {
    e->right_len = tl_fetch_int ();
    if (e->right_len <= 0 || e->right_len > MAX_KEY_LEN) {
      tl_fetch_set_error_format (TL_ERROR_VALUE_NOT_IN_RANGE, "key should contain between 1 and %d ints", MAX_KEY_LEN);
      return 0;
    }
    tl_fetch_raw_data (e->data + e->left_len, 4 * e->right_len);
  } else {
    int i = e->left_len - 1;
    while (i >= 0 && e->data[i] == 0x7fffffff) {
      i --;
    }
    e->right_len = i + 1;
    memcpy (e->data + e->right_len, e->data, e->right_len * 4);
    if (i >= 0) {
       e->data[e->left_len + e->right_len - 1] ++;
    }
  }
  extra->size += e->right_len * 4 + e->left_len * 4;
TL_PARSE_FUN_END

TL_PARSE_FUN(get_range,int wildcard)
  e->left_len = tl_fetch_int ();
  if (e->left_len <= 0 || e->left_len > MAX_KEY_LEN) {
    tl_fetch_set_error_format (TL_ERROR_VALUE_NOT_IN_RANGE, "key should contain between 1 and %d ints", MAX_KEY_LEN);
    return 0;
  }
  tl_fetch_raw_data (e->data, 4 * e->left_len);
  if (!wildcard) {
    e->right_len = tl_fetch_int ();
    if (e->right_len <= 0 || e->right_len > MAX_KEY_LEN) {
      tl_fetch_set_error_format (TL_ERROR_VALUE_NOT_IN_RANGE, "key should contain between 1 and %d ints", MAX_KEY_LEN);
      return 0;
    }
    tl_fetch_raw_data (e->data + e->left_len, 4 * e->right_len);
  } else {
    int i = e->left_len - 1;
    while (i >= 0 && e->data[i] == 0x7fffffff) {
      i --;
    }
    e->right_len = i + 1;
    memcpy (e->data + e->right_len, e->data, e->right_len * 4);
    if (i >= 0) {
       e->data[e->left_len + e->right_len - 1] ++;
    }
  }
  e->limit = tl_fetch_int ();
  extra->size += e->right_len * 4 + e->left_len * 4;
TL_PARSE_FUN_END

struct tl_act_extra *seqmap_parse_function (long long actor_id) {
  if (actor_id != 0) {
    tl_fetch_set_error ("Seqmap only support actor_id = 0", TL_ERROR_WRONG_ACTOR_ID);
    return 0;
  }
  int op = tl_fetch_int ();
  if (tl_fetch_error ()) {
    return 0;
  }
  switch (op) {
  case TL_SEQMAP_SET:
    return tl_store (3, 1);
  case TL_SEQMAP_ADD:
    return tl_store (1, 1);
  case TL_SEQMAP_REPLACE:
    return tl_store (2, 1);
  case TL_SEQMAP_DELETE:
    return tl_delete (1);
  case TL_SEQMAP_GET:
    return tl_get ();
  case TL_SEQMAP_GET_WILDCARD_COUNT:
    return tl_get_range_count (1);
  case TL_SEQMAP_GET_RANGE_COUNT:
    return tl_get_range_count (0);
  case TL_SEQMAP_GET_WILDCARD:
    return tl_get_range (1);
  case TL_SEQMAP_GET_RANGE:
    return tl_get_range (0);
  default:
    tl_fetch_set_error_format (TL_ERROR_UNKNOWN_FUNCTION_ID, "Unknown op %08x", op);
    return 0;
  }
}

void seqmap_stats (void) {
  seqmap_prepare_stats (0);
  tl_store_stats (stats_buff, 0);
}

/*
 *
 *      END (PROXY MEMCACHE SERVER)
 *
 */

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


static void sigint_handler (const int sig) {
  fprintf (stderr, "SIGINT handled.\n");
  flush_binlog_last();
  sync_binlog (2);
  exit (EXIT_SUCCESS);
}

static void sigterm_handler (const int sig) {
  fprintf (stderr, "SIGTERM handled.\n");
  flush_binlog_last();
  sync_binlog (2);
  exit (EXIT_SUCCESS);
}

static void sighup_handler (const int sig) {
  fprintf (stderr, "got SIGHUP.\n");
  sync_binlog (2);
  signal (SIGHUP, sighup_handler);
}

static void sigusr1_handler (const int sig) {
  fprintf (stderr, "got SIGUSR1, rotate logs.\n");
  sync_binlog (2);
  reopen_logs ();
  signal (SIGUSR1, sigusr1_handler);
}

static void sigrtmax_handler (const int sig) {
  fprintf(stderr, "got SIGUSR3, write index.\n");
  force_write_index = 1;
}


int child_pid;

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

void fork_write_index (void) {
  if (child_pid) {
    if (verbosity > 0) {
      fprintf (stderr, "process with pid %d already generates index, skipping\n", child_pid);
    }
    return;
  }

  flush_binlog_ts ();

  int res = fork ();

  if (res < 0) {
    fprintf (stderr, "fork: %m\n");
  } else if (!res) {
    binlogname = 0;
    res = save_index (!binlog_disabled);
    exit (res);
  } else {
    if (verbosity > 0) {
      fprintf (stderr, "created child process pid = %d\n", res);
    }
    child_pid = res;
  }

  force_write_index = 0;
}

void cron (void) {
  create_all_outbound_connections ();
  flush_binlog ();
  free_by_time ();
  redistribute_memory ();
  if (!reindex_on_low_memory && memory_full_warning ()) {
    force_write_index ++;
    reindex_on_low_memory ++;
    last_reindex_on_low_memory_time = now;
  }
  if (force_write_index) {
    fork_write_index ();
  }
  check_child_status();
}



int sfd;

void start_server (void) {
  int i;
  int prev_time;

  init_epoll();
  init_netbuffers();
  if (udp_enabled) {
    init_msg_buffers (0);
  }

  prev_time = 0;

  if (daemonize) {
    setsid ();
    reopen_logs ();
  }

  if (change_user (username) < 0 && !interactive) {
    fprintf (stderr, "fatal: cannot change user to %s\n", username ? username : "(none)");
    exit (1);
  }

  if (binlogname && !binlog_disabled) {
    assert (append_to_binlog (Binlog) == log_readto_pos);
  }
  
  tl_parse_function = seqmap_parse_function;
  tl_stat_function = seqmap_stats;
  tl_aio_timeout = 0.5;

  init_listening_connection (sfd, &ct_rpc_server, &memcache_rpc_server);
  if (udp_enabled) {
    add_udp_socket (port, 0);
  }

  if (binlog_disabled && binlog_fd >= 0) {
    epoll_pre_event = read_new_events;
  }

  signal (SIGINT, sigint_handler);
  signal (SIGTERM, sigterm_handler);
  signal (SIGUSR1, sigusr1_handler);
  signal (SIGPIPE, SIG_IGN);
  signal (SIGIO, SIG_IGN);
  signal (SIGRTMAX, sigrtmax_handler);

  if (daemonize) {
    signal (SIGHUP, sighup_handler);
  }

  for (i = 0; ; i++) {
    if (verbosity > 0 && !(i & 255)) {
      fprintf (stderr, "epoll_work(): %d out of %d connections, network buffers: %d used, %d out of %d allocated\n",
        active_connections, maxconn, NB_used, NB_alloc, NB_max);
    }
    epoll_work (57);

    check_all_aio_completions ();
    tl_restart_all_ready ();

    if (now != prev_time) {
      prev_time = now;
      cron();
    }

    flush_binlog ();

    if (epoll_pre_event) {
      epoll_pre_event();
    }

    if (quit_steps && !--quit_steps) break;
  }

  epoll_close (sfd);
  close (sfd);
}

/*
 *
 *		MAIN
 *
 */

void usage (void) {
  printf ("%s\n",          
          FullVersionStr
         );
  parse_usage ();
  exit (2);
}

int f_parse_function (int val) {
  switch (val) {
  case 'Q':
    tcp_maximize_buffers = 1;
    break;
  case 'U':
    udp_enabled ++;
    break;
  default:
    return -1;
  }
  return 0;
}

int disable_cache;
int main (int argc, char *argv[]) {
  int i;
  binlog_readed = 0;

  signal (SIGRTMAX, sigrtmax_handler);
  set_debug_handlers ();

  progname = argv[0];

  parse_option ("maximize-tcp-buffers", no_argument, 0, 'Q', "Tries to maximize tcp buffers");
  parse_option (0, no_argument, 0, 'U', "Enabled udp");

  parse_engine_options_long (argc, argv, f_parse_function);

  if (argc != optind + 1) {
    usage ();
    return 2;
  }

  if (strlen (argv[0]) >= 5 && memcmp ( argv[0] + strlen (argv[0]) - 5, "index" , 5) == 0) {
    index_mode = 1;
  }

  if (!username && maxconn == MAX_CONNECTIONS && geteuid()) {
    maxconn = 1000; //not for root
  }

  if (dynamic_data_buffer_size > max_memory) {
    dynamic_data_buffer_size = max_memory;
  }

  if (MAX_ZMALLOC_MEM == 0) {
    dynamic_data_buffer_size = 1 << 22;
  }

  if (raise_file_rlimit(maxconn + 16) < 0) {
    fprintf (stderr, "fatal: cannot raise open file limit to %d\n", maxconn + 16);
    exit (1);
  }

  aes_load_pwd_file (aes_pwd_file);

  if (change_user(username) < 0) {
    fprintf (stderr, "fatal: cannot change user to %s\n", username ? username : "(none)");
    exit(1);
  }

  init_dyn_data();
  if (udp_enabled) {
    init_server_PID (get_my_ipv4 (), port);
  }
  init_tree ();

  if (!index_mode) {
    sfd = server_socket (port, settings_addr, backlog, 0);
    if (sfd < 0) {
      fprintf (stderr, "cannot open server socket at port %d: %m\n", port);
      exit (1);
    }
  }

  if (engine_preload_filelist ( argv[optind], binlogname) < 0) {
    fprintf (stderr, "cannot open binlog files for %s\n", binlogname ? binlogname : argv[optind]);
    exit (1);
  }
  
  //Snapshot reading
  Snapshot = open_recent_snapshot (engine_snapshot_replica);

  if (Snapshot) {
    engine_snapshot_name = strdup (Snapshot->info->filename);
    engine_snapshot_size = Snapshot->info->file_size;
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
  
  if (i < 0) {
    fprintf (stderr, "fatal: error reading binlog\n");
    exit (1);
  }

  if (verbosity) {
    fprintf (stderr, "replay binlog file: done, log_pos=%lld, alloc_mem=%lld, time %.06lfs\n",
             (long long) log_pos, get_memory_used (), binlog_load_time);
  }

  binlog_readed = 1;

  clear_write_log ();
  start_time = time (NULL);

  if (!index_mode) {
    start_server ();
  } else {
    save_index (!binlog_disabled);
  }

  return 0;
}

