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

    Copyright 2011-2013 Vkontakte Ltd
              2011-2013 Anton Maydell
*/

#define	_FILE_OFFSET_BITS	64
#define _GNU_SOURCE

#include <assert.h>
#include <stdio.h>
#include <stddef.h>
#include <stdlib.h>
#include <string.h>
#include <fcntl.h>
#include <signal.h>
#include <zlib.h>
#include <ctype.h>
#include <sys/types.h>
#include <sys/stat.h>

#include "server-functions.h"
#include "net-connections.h"
#include "net-memcache-server.h"
#include "net-crypto-aes.h"
#include "net-crypto-rsa.h"
#include "kfs.h"
#include "kdb-data-common.h"
#include "kdb-copyexec-binlog.h"
#include "copyexec-data.h"
#include "copyexec-results-client.h"
#include "am-stats.h"

#define	VERSION_STR	"copyexec-engine-0.01-r17"
#define	BACKLOG	8192
#define TCP_PORT 11211
#define mytime() (get_utime(CLOCK_MONOTONIC))

#ifndef COMMIT
#define COMMIT "unknown"
#endif

const char FullVersionStr[] = VERSION_STR " compiled at " __DATE__ " " __TIME__ " by gcc " __VERSION__ " "
#ifdef __LP64__
  "64-bit"
#else
  "32-bit"
#endif
" after commit " COMMIT;

int verbosity = 0;
double binlog_load_time, index_load_time;
#define STATS_BUFF_SIZE	(16 << 10)
static char stats_buffer[STATS_BUFF_SIZE];

static char *aux_binlogname = NULL;

static long long jump_log_pos;
static int jump_log_ts;
static unsigned jump_log_crc32;


conn_type_t ct_copyexec_engine_server = {
  .magic = CONN_FUNC_MAGIC,
  .title = "copyexec_engine_server",
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

int memcache_get (struct connection *c, const char *key, int key_len);
int memcache_stats (struct connection *c);

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

int copyexec_prepare_stats (struct connection *c) {
  int log_uncommitted = compute_uncommitted_log_bytes ();
  int child_running_list_size, auto_running_list_size;

  get_running_lists_size (&child_running_list_size, &auto_running_list_size);

  stats_buffer_t sb;
  sb_prepare (&sb, c, stats_buffer, STATS_BUFF_SIZE);
  sb_memory (&sb, AM_GET_MEMORY_USAGE_SELF);
  sb_printf (&sb,
      "binlog_original_size\t%lld\n"
      "binlog_loaded_bytes\t%lld\n"
      "binlog_load_time\t%.6fs\n"
      "current_binlog_size\t%lld\n"
      "binlog_uncommitted_bytes\t%d\n"
      "binlog_path\t%s\n"
      "binlog_first_timestamp\t%d\n"
      "binlog_read_timestamp\t%d\n"
      "binlog_last_timestamp\t%d\n"
      "aux_binlog_path\t%s\n"
      "aux_log_read_start\t%lld\n"
      "aux_log_readto_pos\t%lld\n"
      "index_size\t%lld\n"
      "index_path\t%s\n"
      "index_load_time\t%.6lfs\n"
      "main_volume_id\t%llu\n"
      "aux_volume_id\t%llu\n"
      "instance_mask\t%d\n"
      "first_transaction_id\t%d\n"
      "transactions\t%d\n"
      "tot_memory_transactions\t%d\n"
      "child_running_list_size\t%d\n"
      "auto_running_list_size\t%d\n"
      "tot_ignored\t%d\n"
      "tot_interrupted\t%d\n"
      "tot_cancelled\t%d\n"
      "tot_terminated\t%d\n"
      "tot_failed\t%d\n"
      "tot_decryption_failed\t%d\n"
      "tot_io_failed\t%d\n"
      "version\t%s\n",
    log_readto_pos,
    log_readto_pos - jump_log_pos,
    binlog_load_time,
    log_pos,
    log_uncommitted,
    binlogname ? (strlen(binlogname) < 250 ? binlogname : "(too long)") : "(none)",
    log_first_ts,
    log_read_until,
    log_last_ts,
    strlen (aux_binlogname) < 250 ? aux_binlogname : "(too long)",
    aux_log_read_start,
    aux_log_readto_pos,
    engine_snapshot_size,
    engine_snapshot_name,
    index_load_time,
    main_volume_id,
    aux_volume_id,
    instance_mask,
    first_transaction_id,
    transactions,
    tot_memory_transactions,
    child_running_list_size, auto_running_list_size,
    tot_ignored, tot_interrupted, tot_cancelled, tot_terminated, tot_failed, tot_decryption_failed, tot_io_failed,
    FullVersionStr
  );
  return sb.pos;
}

int memcache_get (struct connection *c, const char *key, int key_len) {
  if (key_len >= 5 && !strncmp (key, "stats", 5)) {
    int stats_len = copyexec_prepare_stats (c);
    return_one_key (c, key, stats_buffer, stats_len);
    return 0;
  }
  return 0;
}

int memcache_stats (struct connection *c) {
  int stats_len = copyexec_prepare_stats (c);
  write_out (&c->Out, stats_buffer, stats_len);
  write_out (&c->Out, "END\r\n", 5);
  return 0;
}

int copyexec_replay_logevent (struct lev_generic *E, int size);

int init_copyexec_main_data (int schema) {
  replay_logevent = copyexec_replay_logevent;
  return 0;
}

int copyexec_replay_logevent (struct lev_generic *E, int size) {
  vkprintf (4, "LE (type=%x, offset=%lld)\n", E->type, log_cur_pos ());
  int s;
  switch (E->type) {
    case LEV_START:
      if (size < 24 || E->b < 0 || E->b > 4096) { return -2; }
      s = 24 + ((E->b + 3) & -4);
      if (size < s) { return -2; }
      return copyexec_main_le_start ((struct lev_start *) E) >= 0 ? s : -1;
    case LEV_NOOP:
    case LEV_TIMESTAMP:
    case LEV_CRC32:
    case LEV_ROTATE_FROM:
    case LEV_ROTATE_TO:
      return default_replay_logevent (E, size);
    case LEV_COPYEXEC_MAIN_TRANSACTION_STATUS ... LEV_COPYEXEC_MAIN_TRANSACTION_STATUS + ts_io_failed:
      s = sizeof (struct lev_copyexec_main_transaction_status);
      if (size < s) {
        return -2;
      }
      do_set_status ((struct lev_copyexec_main_transaction_status *) E);
      return s;
    case LEV_COPYEXEC_MAIN_COMMAND_BEGIN:
      s = sizeof (struct lev_copyexec_main_command_begin);
      if (size < s) {
        return -2;
      }
      s += ((struct lev_copyexec_main_command_begin *) E)->command_size;
      if (size < s) {
        return -2;
      }

      return s;
    case LEV_COPYEXEC_MAIN_COMMAND_END:
      s = sizeof (struct lev_copyexec_main_command_end);
      if (size < s) {
        return -2;
      }
      s += ((struct lev_copyexec_main_command_end *) E)->saved_stdout_size;
      s += ((struct lev_copyexec_main_command_end *) E)->saved_stderr_size;
      if (size < s) {
        return -2;
      }

      return s;
    case LEV_COPYEXEC_MAIN_TRANSACTION_ERROR:
      s = sizeof (struct lev_copyexec_main_transaction_err);
      if (size < s) {
        return -2;
      }
      s += ((struct lev_copyexec_main_transaction_err *) E)->error_msg_size;
      if (size < s) {
        return -2;
      }

      return s;
    case LEV_COPYEXEC_MAIN_COMMAND_WAIT:
      s = sizeof (struct lev_copyexec_main_command_wait);
      if (size < s) {
        return -2;
      }

      return s;
    case LEV_COPYEXEC_MAIN_COMMAND_KILL:
      s = sizeof (struct lev_copyexec_main_command_kill);
      if (size < s) {
        return -2;
      }

      return s;
    case LEV_COPYEXEC_MAIN_TRANSACTION_SKIP:
      s = sizeof (struct lev_copyexec_main_transaction_skip);
      if (size < s) {
        return -2;
      }
      if (first_transaction_id < ((struct lev_copyexec_main_transaction_skip *) E)->first_transaction_id) {
        first_transaction_id = ((struct lev_copyexec_main_transaction_skip *) E)->first_transaction_id;
      }
      return s;

  }

  kprintf ("unknown log event type %08x at position %lld\n", E->type, log_cur_pos ());

  return -3;
}

/*
 *
 *		MAIN
 *
 */

void usage (void) {
  fprintf (stderr, "%s\n", FullVersionStr);
  fprintf (stderr, "Performs transactions from auxiliary binlog and writes results to the binlog.\n");
  fprintf (stderr,
           "./copyexec-engine -I<instance-mask> -T<tmp-dir> -P<public-key-prefix> [-R<host:port>] [-v] [-p<port>] [-b<backlog>] [-c<max-conn>] [-l<log-name>] -a<aux-binlog-name> <binlog>\n"
                   "\t-I<instance-mask>\tmandatory option, instance mask could be hex (prefix:'0x'), oct (prefix:'0') or dec\n"
                   "\t-P<public-key-prefix>\t(mandatory option), full public key name is concatenation of <public-key-prefix> and key_id found in aux binlog.\n"
                   "\t-T<tmp-dir>\ttemporary transaction files will be created in subdirs of given directory\n"
                   "\t-a<aux-binlog-name>\tshould be full binlog filename (mandatory option)\n"
                   "\t-E<volume-id>\tcreates new empty binlog, volume-id is a string.\n"
                   "\t\t\tFor creating new binlog also need specify public-key-prefix and aux-binlog-name (for finding and writing sync point to the main binlog).\n"
                   "\t-p<port>\tif port given when copyexec-engine will reply to the stats memcache command\n"
                   "\t-R<host:port>\tif given copyexec-engine will send results to the copyexec-results-engine\n"
                   "\t-v\tincrease verbosity level\n"
          );
  exit (2);
}

struct in_addr settings_addr;
int port = -1;


const long long interrupting_signal_mask = (1LL << SIGTERM) | (1LL << SIGINT);


static void aux_binlog_check_updates (void) {
  struct stat st;
  if (fstat (fd_aux_binlog, &st) >= 0 && st.st_size > aux_log_readto_pos) {
    copyexec_aux_replay_binlog (aux_log_readto_pos, exec_transaction);
  }
}

void cron (void) {
  flush_binlog ();
  //dyn_garbage_collector ();
  if (!interrupted_by_signal ()) {
    aux_binlog_check_updates ();
  }
  transaction_check_child_status ();
  transaction_check_auto_status ();
}

static void pending_signals_clear_bit (const sigset_t *ss, const int sig) {
  sigset_t old;
  int r = sigprocmask (SIG_BLOCK, ss, &old);
  assert (!r);
  pending_signals &= ~(1LL << sig);
  r = sigprocmask (SIG_SETMASK, &old, NULL);
  assert (!r);
}



void start_server (void) {
  int i, prev_time = 0;

  init_epoll ();

  if (!sfd && port >= 0) {
    sfd = server_socket (port, settings_addr, backlog, 0);
    if (sfd < 0) {
      kprintf ("cannot open server socket at port %d: %m\n", port);
      exit (3);
    }
    init_netbuffers ();
  }

  if (daemonize) {
    setsid ();
    reopen_logs ();
  }

  vkprintf (1, "%s\n", FullVersionStr);

  if (binlogname && !binlog_disabled) {
    vkprintf (3, "log_readto_pos: %lld\n", log_readto_pos);
    assert (append_to_binlog (Binlog) == log_readto_pos);
  }

  i = find_running_transactions ();
  vkprintf (1, "found %d running transactions\n", i);

  copyexec_aux_binlog_seek ();

  if (sfd) {
    init_listening_connection (sfd, &ct_copyexec_engine_server, &memcache_methods);
  }

  signal (SIGPIPE, SIG_IGN);
  signal (SIGIO, SIG_IGN);

  sigset_t signal_set;
  sigemptyset (&signal_set);
  sigaddset (&signal_set, SIGINT);
  sigaddset (&signal_set, SIGTERM);
  sigaddset (&signal_set, SIGUSR1);
  sigaddset (&signal_set, SIGCHLD);
  if (daemonize) {
    sigaddset (&signal_set, SIGHUP);
  }
  struct sigaction act;
  act.sa_handler = copyexec_main_sig_handler;
  act.sa_mask = signal_set;
  act.sa_flags = SA_NOCLDSTOP;
  for (i = 1; i <= SIGRTMAX; i++) {
    if (sigismember (&signal_set, i)) {
      if (sigaction (i, &act, NULL) < 0) {
        kprintf ("sigaction (%d) failed. %m\n", i);
        exit (1);
      }
    }
  }

  int quit_steps = 0;

  for (i = 0; ; i++) {
    if (!(i & 1023)) {
      vkprintf (2, "epoll_work(): %d out of %d connections, network buffers: %d used, %d out of %d allocated\n",
	       active_connections, maxconn, NB_used, NB_alloc, NB_max);
    }
    epoll_work (77);

    if (interrupted_by_signal ()) {
      break;
    }

    if (pending_signals & (1LL << SIGCHLD)) {
      pending_signals_clear_bit (&signal_set, SIGCHLD);
      kprintf ("got SIGCHLD.\n");
      transaction_check_child_status ();
    }

    if (pending_signals & (1LL << SIGHUP)) {
      pending_signals_clear_bit (&signal_set, SIGHUP);
      kprintf ("got SIGHUP.\n");
      sync_binlog (2);
    }

    if (pending_signals & (1LL << SIGUSR1)) {
      pending_signals_clear_bit (&signal_set, SIGUSR1);
      kprintf ("got SIGUSR1, rotate logs.\n");
      reopen_logs ();
      transaction_child_kill (SIGUSR1);
      transaction_auto_kill (SIGUSR1);
      if (results_client_pid && results_client_creation_time && results_client_creation_time == get_process_creation_time (results_client_pid)) {
        kill (results_client_pid, SIGUSR1);
      }

      sync_binlog (2);
    }

    if (now != prev_time) {
      prev_time = now;
      cron ();
    }

    if (quit_steps && !--quit_steps) break;
  }

  if (sfd) {
    epoll_close (sfd);
    close (sfd);
  }

  flush_binlog_last ();
  sync_binlog (2);
  vkprintf (0, "Main process terminated (pending_signals: 0x%llx).\n", pending_signals);
}

static char *empty_binlog_volume_name = NULL;
static int instance_mask_initialized = 0;

static void create_main_empty_binlog (char *binlog_name) {
  void *extra = NULL;
  int extra_len = 0;
  struct lev_copyexec_main_transaction_skip E;

  int lev_start_extra[3];
  lev_start_extra[2] = instance_mask;

  int r = get_random_bytes (lev_start_extra, 8);
  if (r != 8) {
    kprintf ("random_tag creation failed, get_random_bytes returns %d instead of 8.\n", r);
    exit (1);
  }

  const int sp = find_last_synchronization_point ();
  if (sp) {
    kprintf ("Last synchronization point transaction id is %d.\n", sp);
    E.type = LEV_COPYEXEC_MAIN_TRANSACTION_SKIP;
    E.first_transaction_id = sp + 1;
    extra = &E;
    extra_len = sizeof (E);
  }

  make_empty_binlog (binlog_name, empty_binlog_volume_name, COPYEXEC_MAIN_SCHEMA_V1, lev_start_extra, 12, extra, extra_len);

}

int main (int argc, char *argv[]) {
  int i;
  check_superuser ();
  copy_argv (argc, argv);
  set_debug_handlers ();
  kprintf_multiprocessing_mode_enable ();
  progname = argv[0];
  while ((i = getopt (argc, argv, "a:b:c:dhl:p:u:vE:I:P:R:T:")) != -1) {
    switch (i) {
    case 'E':
      empty_binlog_volume_name = strdup (optarg);
      break;
    case 'I':
      if (sscanf (optarg, "%i", &i) == 1) {
        instance_mask = i;
        instance_mask_initialized++;
      }
      break;
    case 'P':
      public_key_prefix = strdup (optarg); /* changing process name function clears argv strings in child process */
      break;
    case 'R':
      if (parse_copyexec_results_addr (optarg) < 0) {
        usage ();
      }
      break;
    case 'T':
      tmp_dir = realpath (optarg, NULL);
      if (tmp_dir == NULL) {
        kprintf ("realpath (%s) failed. %m\n", optarg);
        exit (1);
      }
      break;
    case 'a':
      aux_binlogname = strdup (optarg);
      break;
    case 'b':
      backlog = atoi (optarg);
      if (backlog <= 0) backlog = BACKLOG;
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
      usage ();
      return 2;
    case 'l':
      logname = strdup (optarg);
      break;
    case 'p':
      port = atoi (optarg);
      break;
    case 'u':
      // ignoring -u switch
      break;
    case 'v':
      verbosity++;
      break;
    }
  }

  if (optind + 1 != argc) {
    kprintf ("main binlog wasn't given\n");
    usage ();
  }

  if (public_key_prefix == NULL) {
    kprintf ("public-key-prefix wasn't given\n");
    usage ();
  }

  if (copyexec_aux_binlog_readonly_open (aux_binlogname)) {
    exit (1);
  }

  if (!instance_mask_initialized) {
    kprintf ("instance-mask wasn't given\n");
    usage ();
  }

  if (!check_mask (instance_mask)) {
    kprintf ("illegal instance-mask: reserved high bits were set\n");
    usage ();
  }

  if (!instance_mask) {
    kprintf ("illegal instance-mask: instance-mask couldn't equal to zero\n");
    usage ();
  }

  if (empty_binlog_volume_name) {
    create_main_empty_binlog (argv[optind]);
    exit (0);
  }

  if (tmp_dir == NULL) {
    kprintf ("tmp-dir wasn't given\n");
    usage ();
  }

  if (raise_file_rlimit (maxconn + 16) < 0) {
    kprintf ("fatal: cannot raise open file limit to %d\n", maxconn+16);
    exit (1);
  }

  set_sigusr1_handler ();
  set_sigusr2_handler ();

  dynamic_data_buffer_size = (1 << 23);
  init_dyn_data ();
  aes_load_pwd_file (NULL);

  copyexec_main_process_init ();

  if (engine_preload_filelist (argv[optind], binlogname) < 0) {
    kprintf ("cannot open binlog files for %s\n", binlogname ? binlogname : argv[optind]);
    exit (1);
  }

  results_client_pid = create_results_sending_process ();
  if (results_client_pid > 0) {
    results_client_creation_time = get_process_creation_time (results_client_pid);
    vkprintf (1, "results_client_pid: %d, results_client_creation_time: %d\n", results_client_pid, results_client_creation_time);
  }

  //Binlog reading
  Binlog = open_binlog (engine_replica, jump_log_pos);
  if (!Binlog) {
    kprintf ("fatal: cannot find binlog for %s, log position %lld\n", engine_replica->replica_prefix, jump_log_pos);
    exit (1);
  }

  check_superuser_main_binlog ();
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
    clear_read_log ();
  }

  clear_write_log ();

  start_time = time (NULL);

  start_server ();

  return 0;
}


