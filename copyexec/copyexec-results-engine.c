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

#include <assert.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <ctype.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/types.h>
#include <aio.h>
#include <signal.h>

#include "kfs.h"
#include "server-functions.h"
#include "net-connections.h"
#include "net-memcache-server.h"
#include "net-rpc-server.h"
#include "net-crypto-aes.h"
#include "kdb-data-common.h"
#include "kdb-copyexec-binlog.h"
#include "copyexec-results-data.h"
#include "copyexec-rpc-layout.h"
#include "copyexec-rpc.h"
#include "copyexec-err.h"
#include "am-stats.h"

#define VERSION_STR	"copyexec-results-engine-0.1-r10"
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

#define mytime() (get_utime(CLOCK_MONOTONIC))

static long long jump_log_pos;
static int jump_log_ts;
static unsigned jump_log_crc32;

static long long binlog_loaded_size;
static double binlog_load_time;

struct in_addr settings_addr;

char *binlogname;

#define STATS_BUFF_SIZE	(64 << 10)
int stats_buff_len;
char stats_buff[STATS_BUFF_SIZE];
long long cmd_get = 0, cmd_stats = 0;

int rpcs_execute (struct connection *c, int op, int len);

int memcache_get (struct connection *c, const char *key, int key_len);
int memcache_stats (struct connection *c);

struct memcache_server_functions copyexec_results_memcache_inbound = {
  .execute = mcs_execute,
  .mc_store = mcs_store,
  .mc_get_start = mcs_get_start,
  .mc_get = memcache_get,
  .mc_get_end = mcs_get_end,
  .mc_incr = mcs_incr,
  .mc_delete = mcs_delete,
  .mc_version = mcs_version,
  .mc_stats = memcache_stats,
  .mc_wakeup = mcs_do_wakeup,
  .mc_alarm = mcs_do_wakeup,
  .mc_check_perm = mcs_default_check_perm,
  .mc_init_crypto = mcs_init_crypto
};

struct rpc_server_functions copyexec_result_rpc_server = {
  .execute = rpcs_execute,
  .check_ready = server_check_ready,
  .flush_packet = rpcs_flush_packet,
  .rpc_check_perm = rpcs_default_check_perm,
  .rpc_init_crypto = rpcs_init_crypto,
  .memcache_fallback_type = &ct_memcache_server,
  .memcache_fallback_extra = &copyexec_results_memcache_inbound
};

int get_memory_usage (long long *a, int m) {
  memset (a, 0, sizeof (a[0]) * m);
  char buf[1024], *p;
  int fd = open ("/proc/self/statm", O_RDONLY), n = -1, i;
  if (fd < 0) {
    return -1;
  }
  do {
    n = read (fd, buf, sizeof (buf));
    if (n < 0) {
      if (errno == EINTR) {
        continue;
      }
    }
    break;
  } while (1);

  while (close (fd) < 0 && errno == EINTR) {}

  if (n < 0 || n >= sizeof (buf)) {
    return -1;
  }
  buf[n] = 0;
  vkprintf (3, "/proc/self/statm: %s\n", buf);
  long long page_size = sysconf (_SC_PAGESIZE);
  p = buf;
  for (i = 0; i < m; i++) {
    if (sscanf (p, "%lld", &a[i]) != 1) {
      return -1;
    }
    a[i] *= page_size;
    while (*p && !isspace (*p)) {
      p++;
    }
  }
  return 0;
}
int copyexec_results_prepare_stats (struct connection *c) {
  int log_uncommitted = compute_uncommitted_log_bytes();
  dyn_update_stats ();
  long long a[7];
  get_memory_usage (a, 7);

  stats_buffer_t sb;
  sb_prepare (&sb, c, stats_buff, STATS_BUFF_SIZE);
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
      "cmd_get\t%lld\n"
      "cmd_stats\t%lld\n"
      "hosts\t%d\n"
      "tot_memory_transactions\t%d\n"
      "max_memory_transactions\t%d\n"
      "alloc_tree_nodes\t%d\n"
      "free_tree_nodes\t%d\n"
      "version\t%s\n",
    log_readto_pos,
    binlog_loaded_size,
    binlog_load_time,
    log_pos,
    log_uncommitted,
    binlogname ? (strlen(binlogname) < 250 ? binlogname : "(too long)") : "(none)",
    log_first_ts,
    log_read_until,
    log_last_ts,
    cmd_get,
    cmd_stats,
    hosts,
    tot_memory_transactions,
    max_lru_size,
    alloc_tree_nodes,
    free_tree_nodes,
    FullVersionStr);
  return sb.pos;
}

int memcache_get (struct connection *c, const char *key, int key_len) {
  cmd_get++;
  unsigned long long volume_id = 0, random_tag = 0;
  int transaction_id = 0;
  char status[32];
  const int dog_len = get_at_prefix_length (key, key_len);
  key += dog_len;
  key_len -= dog_len;

  if (key_len >= 5 && !strncmp (key, "stats", 5)) {
    int len = copyexec_results_prepare_stats (c);
    return return_one_key (c, key - dog_len, stats_buff, len);
  }

  if (key_len >= 6 && !memcmp (key, "rfreqs", 6)) {
    if (sscanf (key + 6, "%llu,%d", &volume_id, &transaction_id) == 2) {
      char *r = get_results_freqs (volume_id, transaction_id);
      if (r) {
        return_one_key (c, key - dog_len, r, strlen (r));
        free (r);
        return 0;
      }
    }
  }

  if (key_len >= 6 && !memcmp (key, "sfreqs", 6)) {
    if (sscanf (key + 6, "%llu,%d", &volume_id, &transaction_id) == 2) {
      char *r = get_status_freqs (volume_id, transaction_id);
      if (r) {
        return_one_key (c, key - dog_len, r, strlen (r));
        free (r);
        return 0;
      }
    }
  }

  if (key_len >= 7 && !memcmp (key, "srfreqs", 7)) {
    if (sscanf (key + 7, "%llu,%d", &volume_id, &transaction_id) == 2) {
      char *r = get_status_results_freqs (volume_id, transaction_id);
      if (r) {
        return_one_key (c, key - dog_len, r, strlen (r));
        free (r);
        return 0;
      }
    }
  }

  if (key_len >= 6 && !memcmp (key, "rhosts", 6)) {
    unsigned result_or, result_and;
    if (sscanf (key + 5, "%llu,%d,%u,%u", &volume_id, &transaction_id, &result_or, &result_and) == 4) {
      char *r = get_hosts_list (volume_id, transaction_id, result_or, result_and);
      if (r) {
        return_one_key (c, key - dog_len, r, strlen (r));
        free (r);
        return 0;
      }
    }
  }

  if (key_len >= 6 && !memcmp (key, "shosts", 6)) {
    if (sscanf (key + 6, "%llu,%d,%31[a-z_]", &volume_id, &transaction_id, status) == 3) {
      char *r = get_hosts_list_by_status (volume_id, transaction_id, status);
      if (r) {
        return_one_key (c, key - dog_len, r, strlen (r));
        free (r);
        return 0;
      }
    }
  }

  if (key_len >= 7 && !memcmp (key, "srhosts", 7)) {
    unsigned result;
    if (sscanf (key + 7, "%llu,%d,%31[a-z_]:0x%x", &volume_id, &transaction_id, status, &result)== 4) {
      char *r = get_hosts_list_by_status_and_result (volume_id, transaction_id, status, result);
      if (r) {
        return_one_key (c, key - dog_len, r, strlen (r));
        free (r);
        return 0;
      }
    }
  }

  if (key_len == 7 && !memcmp (key, "volumes", 7)) {
    char *r = get_volumes ();
    if (r) {
      return_one_key (c, key - dog_len, r, strlen (r));
      free (r);
      return 0;
    }
  }

  if (key_len >= 9 && !memcmp (key, "deadhosts", 9)) {
    int delay;
    if (sscanf (key + 9, "%llu,%d", &volume_id, &delay) == 2) {
      char *r = get_dead_hosts_list (volume_id, delay);
      if (r) {
        return_one_key (c, key - dog_len, r, strlen (r));
        free (r);
        return 0;
      }
    }
  }

  if (key_len >= 14 && !memcmp (key, "deadhosts_full", 14)) {
    int delay;
    if (sscanf (key + 14, "%llu,%d", &volume_id, &delay) == 2) {
      char *r = get_dead_hosts_list_full (volume_id, delay);
      if (r) {
        return_one_key (c, key - dog_len, r, strlen (r));
        free (r);
        return 0;
      }
    }
  }

  if (key_len == 10 && !memcmp (key, "collisions", 10)){
    char *r = get_collisions_list ();
    if (r) {
      return_one_key (c, key - dog_len, r, strlen (r));
      free (r);
      return 0;
    }
  }

  if (key_len >= 6 && !memcmp (key, "enable", 6) && sscanf (key + 6, "0x%llx", &random_tag) == 1) {
    int r = do_set_enable (random_tag, 1);
    return return_one_key (c, key - dog_len, (r >= 0) ? "1" : "0", 1);
  }

  if (key_len >= 7 && !memcmp (key, "disable", 7) && sscanf (key + 7, "0x%llx", &random_tag) == 1) {
    int r = do_set_enable (random_tag, 0);
    return return_one_key (c, key - dog_len, (r >= 0) ? "1" : "0", 1);
  }

  if (key_len >= 13 && !memcmp (key, "list_disabled", 13) && sscanf (key + 13, "%llu", &volume_id) == 1) {
    char *r = get_disabled (volume_id);
    if (r != NULL) {
      return_one_key (c, key - dog_len, r, strlen (r));
      free (r);
      return 0;
    }
  }

  return 0;
}

int memcache_stats (struct connection *c) {
  cmd_stats++;
  int len = copyexec_results_prepare_stats (c);
  write_out (&c->Out, stats_buff, len);
  write_out (&c->Out, "END\r\n", 5);
  return 0;
}

static int P[MAX_PACKET_LEN/4], Q[MAX_PACKET_LEN/4];

int rpc_execute_handshake (struct connection *c, struct copyexec_rpc_handshake *P, int len) {
  int sz = sizeof (struct copyexec_rpc_handshake);
  if (len < sz) {
    return -__LINE__;
  }
  int l = P->hostname_length;
  sz += l;
  sz = (sz + 3) & -4;
  if (len != sz) {
    return -__LINE__;
  }

  host_t *H;
  char ch = P->hostname[l];
  P->hostname[l] = 0; /* possibly broke packet crc32 */
  int r = do_connect (c, P->volume_id, P->random_tag, P->hostname, P->pid, &H);
  P->hostname[l] = ch; /* restore packet crc32 */

  if (r < 0) {
    kprintf ("rpc_execute_handshake: do_connect (c->remote_ip: %s, volume_id: 0x%llx, random_tag: 0x%llx, hostname: %.*s) returns %s.\n",
      show_ip (c->remote_ip), P->volume_id, P->random_tag, P->hostname_length, P->hostname, copyexec_strerror (r));
    struct copyexec_rpc_handshake_error *E = rpc_create_query (Q, sizeof (*E), c, COPYEXEC_RPC_TYPE_ERR_HANDSHAKE);
    if (E == NULL) {
      return -__LINE__;
    }
    E->error_code = r;
    return rpc_send_query (Q, c);
  }

  struct copyexec_rpc_pos *E = rpc_create_query (Q, sizeof (*E), c, COPYEXEC_RPC_TYPE_VALUE_POS);
  if (E == NULL) {
    return -__LINE__;
  }
  E->binlog_pos = H->binlog_pos;

  return rpc_send_query (Q, c);
}

int rpc_execute_send_data (struct connection *c, struct copyexec_rpc_send_data *P, int len) {
  if (len != sizeof (struct copyexec_rpc_send_data)) {
    return -__LINE__;
  }

  host_t *H = get_host_by_connection (c);
  if (H == NULL) {
    vkprintf (1, "rpc_execute_send_data: get_host_by_connection returns NULL.\n");
    return -__LINE__;
  }

  int r = do_set_result (c, P->transaction_id, P->result, P->binlog_pos);
  if (r < 0) {
    vkprintf (1, "rpc_execute_send_data: do_set_result (c:%p, transaction_id: %d, result: %u, binlog_pos: 0x%llx) returns %s.\n",
              c, P->transaction_id, P->result, P->binlog_pos, copyexec_strerror (r));
    return -__LINE__;
  }

  struct copyexec_rpc_pos *E = rpc_create_query (Q, sizeof (*E), c, COPYEXEC_RPC_TYPE_VALUE_POS);
  if (E == NULL) {
    return -__LINE__;
  }
  E->binlog_pos = P->binlog_pos;
  return rpc_send_query (Q, c);
}

int rpc_execute_ping (struct connection *c) {
  host_t *H = get_host_by_connection (c);
  if (H == NULL) {
    vkprintf (1, "rpc_execute_ping: get_host_by_connection returns NULL.\n");
    return -__LINE__;
  }
  H->last_action_time = now;
  return 0;
}

int rpcs_execute (struct connection *c, int op, int len) {
  vkprintf (3, "rpcs_execute: fd=%d, op=%x, len=%d\n", c->fd, op, len);

  if (len > MAX_PACKET_LEN) {
    return SKIP_ALL_BYTES;
  }

  assert (read_in (&c->In, &P, len) == len);
  len -= 16;
  switch (op) {
    case COPYEXEC_RPC_TYPE_HANDSHAKE:
      return rpc_execute_handshake (c, (struct copyexec_rpc_handshake *) (P + 3), len);
    case COPYEXEC_RPC_TYPE_SEND_DATA:
      return rpc_execute_send_data (c, (struct copyexec_rpc_send_data *) (P + 3), len);
    case COPYEXEC_RPC_TYPE_PING:
      return rpc_execute_ping (c);
  }

  return -__LINE__;
}

void cron (void) {
  create_all_outbound_connections ();
  flush_binlog ();
  dyn_garbage_collector ();
}

static int sfd;

/********************** signals **********************/
void copyexec_results_sig_handler (const int sig) {
  /* since next operator isn't atomic for 32-bit version, */
  /* sigaction function was used for blocking other signals changing pending_signal variable (sa_mask field) */
  pending_signals |= 1LL << sig;
}

static int interrupted_by_term_signal (void) {
  static const int interrupting_signal_mask = (1 << SIGTERM) | (1 << SIGINT);
  return ((int) pending_signals) & interrupting_signal_mask;
}

static void pending_signals_clear_bit (const sigset_t *ss, const int sig) {
  sigset_t old;
  int r = sigprocmask (SIG_BLOCK, ss, &old);
  assert (!r);
  pending_signals &= ~(1LL << sig);
  r = sigprocmask (SIG_SETMASK, &old, NULL);
  assert (!r);
}

static void reopen_logs (void) {
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
}

void start_server (void) {
  char buf[64];
  int i, prev_time = 0;

  init_epoll ();
  init_netbuffers ();

  if (!sfd) {
    sfd = server_socket (port, settings_addr, backlog, 0);
  }

  if (sfd < 0) {
    kprintf ("cannot open server socket at port %d: %m\n", port);
    exit (3);
  }

  vkprintf (1, "created listening socket at %s:%d, fd=%d\n", conv_addr (settings_addr.s_addr, buf), port, sfd);

  if (daemonize) {
    setsid ();
  }

  if (change_user (username) < 0) {
    kprintf ("fatal: cannot change user to %s\n", username ? username : "(none)");
    exit (1);
  }

  if (binlogname && !binlog_disabled) {
    assert (append_to_binlog (Binlog) == log_readto_pos);
  }

  init_listening_connection (sfd, &ct_rpc_server, &copyexec_result_rpc_server);

  sigset_t signal_set;
  sigemptyset (&signal_set);
  sigaddset (&signal_set, SIGINT);
  sigaddset (&signal_set, SIGTERM);
  sigaddset (&signal_set, SIGUSR1);
  if (daemonize) {
    sigaddset (&signal_set, SIGHUP);
  }
  struct sigaction act;
  act.sa_handler = copyexec_results_sig_handler;
  act.sa_mask = signal_set;
  act.sa_flags = 0;
  for (i = 1; i <= SIGRTMAX; i++) {
    if (sigismember (&signal_set, i)) {
      if (sigaction (i, &act, NULL) < 0) {
        kprintf ("sigaction (%d) failed. %m\n", i);
        exit (1);
      }
    }
  }

  for (i = 0; ; i++) {
    if (!(i & 255)) {
      vkprintf (1, "epoll_work(): %d out of %d connections, network buffers: %d used, %d out of %d allocated\n",
	       active_connections, maxconn, NB_used, NB_alloc, NB_max);
    }
    epoll_work (71);

    if (interrupted_by_term_signal ()) {
      break;
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
      sync_binlog (2);
    }

    if (now != prev_time) {
      prev_time = now;
      cron ();
    }
    if (quit_steps && !--quit_steps) break;
  }

  epoll_close (sfd);
  close (sfd);

  flush_binlog_last ();
  sync_binlog (2);
}

/**************************************** empty binlog *******************************************************/
static void copyexec_results_make_empty_binlog (const char *binlog_name) {
  char a[PATH_MAX];
  assert (snprintf (a, PATH_MAX, "%s.bin", binlog_name) < PATH_MAX);
  int sz = 24;
  struct lev_start *E = malloc (sz);
  assert (E);
  memset (E, 0, sz);
  E->type = LEV_START;
  E->schema_id = COPYEXEC_RESULT_SCHEMA_V1;
  E->extra_bytes = 0;
  E->split_mod = 1;
  E->split_min = 0;
  E->split_max = 1;
  int fd = open (a, O_CREAT | O_WRONLY | O_EXCL, 0640);
  if (fd < 0) {
    kprintf ("open (%s, O_CREAT | O_WRONLY | O_EXCL, 0640) failed. %m\n", a);
    assert (fd >= 0);
  }
  assert (write (fd, E, sz) == sz);
  assert (fsync (fd) >= 0);
  assert (close (fd) >= 0);
  free (E);
}

/*
 *
 *		MAIN
 *
 */

void usage (void) {
  printf ("%s\n", FullVersionStr);
  printf ("usage: [-v] [-p<port>] [-u<username>] [-M<max-memory-transactions>] [-b<backlog>] [-c<max-conn>] [-a<binlog-name>] [-l<log-name>] [-E] <binlog>\n"
	  "\tCollects transactions results from copyexec-engine.\n"
    "\t-E\tcreate copyexec-results empty binlog\n"
    "\t-M<max-memory-transactions>\tlimit memory transaction number (default value is %d).\n"
    "\t-v\toutput statistical and debug information into stderr\n", max_lru_size);
  exit (2);
}

int main (int argc, char *argv[]) {
  int i;
  char new_binlog = 0;
  set_debug_handlers ();

  progname = argv[0];
  while ((i = getopt (argc, argv, "a:b:c:l:p:dhu:vEM:")) != -1) {
    switch (i) {
    case 'E':
      new_binlog = 1;
      break;
    case 'M':
      if (sscanf (optarg, "%d", &i) != 1 || i < 1) {
        kprintf ("invalid -M arg: %s", optarg);
        usage ();
      }
      max_lru_size = i;
      break;
    case 'a':
      binlogname = optarg;
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
      break;
    case 'l':
      logname = optarg;
      break;
    case 'p':
      port = atoi(optarg);
      break;
    case 'u':
      username = optarg;
      break;
    case 'v':
      verbosity++;
      break;
    }
  }

  if (argc != optind + 1) {
    usage ();
  }

  if (raise_file_rlimit (maxconn + 16) < 0) {
    kprintf ("fatal: cannot raise open file limit to %d\n", maxconn+16);
    exit (1);
  }

  aes_load_pwd_file (0);

  if (change_user (username) < 0) {
    kprintf ("fatal: cannot change user to %s\n", username ? username : "(none)");
    exit (1);
  }

  if (new_binlog) {
    copyexec_results_make_empty_binlog (argv[optind]);
    exit (0);
  }

  init_dyn_data ();

  if (engine_preload_filelist (argv[optind], binlogname) < 0) {
    kprintf ("cannot open binlog files for %s\n", binlogname ? binlogname : argv[optind]);
    exit (1);
  }

  vkprintf (3, "engine_preload_filelist done\n");

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
  if (i < 0) {
    kprintf ("fatal: error reading binlog\n");
    exit (1);
  }
  binlog_load_time += mytime();
  binlog_loaded_size = log_readto_pos - jump_log_pos;
  if (!binlog_disabled) {
    clear_read_log();
  }
  clear_write_log ();
  start_time = time (0);

  start_server ();

  return 0;
}
