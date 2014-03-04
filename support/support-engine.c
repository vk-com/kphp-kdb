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

    Copyright 2011-2012 Vkontakte Ltd
              2011-2012 Arseny Smirnov
              2011-2012 Aliaksei Levin
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
#include <signal.h>

#include "net-events.h"
#include "net-buffers.h"
#include "net-connections.h"
#include "net-memcache-server.h"
#include "net-crypto-aes.h"

#include "support-data.h"
#include "dl-utils.h"
#include "dl-set-get.h"


#define VERSION "0.01"
#define VERSION_STR "support "VERSION

#define TCP_PORT 11211
#define UDP_PORT 11211

/** stats vars **/
#define STATS_BUFF_SIZE (1 << 16)
int stats_buff_len;
char stats_buff[STATS_BUFF_SIZE];
int support_prepare_stats (void);

/** server vars **/
int port = TCP_PORT, udp_port = UDP_PORT;
struct in_addr settings_addr;
int interactive = 0;

volatile int sigpoll_cnt;

/** index vars **/
double index_load_time;

/** binlog vars **/
long long binlog_loaded_size;
double binlog_load_time;


/*
 *
 *     MEMCACHED interface
 *
 */


#define MAX_VALUE_LEN (1 << 20)
char buf[MAX_VALUE_LEN];

/** stats **/
long long cmd_get, cmd_set, cmd_delete, total_requests, cmd_stats, cmd_version;
double cmd_get_time = 0.0, cmd_set_time = 0.0, cmd_delete_time = 0.0;
double max_cmd_get_time = 0.0, max_cmd_set_time = 0.0, max_cmd_delete_time = 0.0;

conn_type_t ct_support_engine_server = {
  .magic = CONN_FUNC_MAGIC,
  .title = "support_engine_server",
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
  .wakeup = server_failed,
  .alarm = server_failed,
#ifndef NOAES
  .crypto_init = aes_crypto_init,
  .crypto_free = aes_crypto_free,
  .crypto_encrypt_output = aes_crypto_encrypt_output,
  .crypto_decrypt_input = aes_crypto_decrypt_input,
  .crypto_needed_output_bytes = aes_crypto_needed_output_bytes,
#endif
};

int memcache_store (struct connection *c, int op, const char *key, int key_len, int flags, int delay, int size);
int memcache_get (struct connection *c, const char *key, int key_len);
int memcache_stats (struct connection *c);
int memcache_version (struct connection *c);

struct memcache_server_functions memcache_methods = {
  .execute = mcs_execute,
  .mc_store = memcache_store,
  .mc_get_start = mcs_get_start,
  .mc_get = memcache_get,
  .mc_get_end = mcs_get_end,
  .mc_incr = mcs_incr,
  .mc_delete = mcs_delete,
  .mc_version = memcache_version,
  .mc_stats = memcache_stats,
  .mc_check_perm = mcs_default_check_perm,
  .mc_init_crypto = mcs_init_crypto
};


static inline void eat_at (const char *key, int key_len, char **new_key, int *new_len) {
  if (*key == '^') {
    key++;
    key_len--;
  }

  *new_key = (char *)key;
  *new_len = key_len;

  if ((*key >= '0' && *key <= '9') || (*key == '-' && key[1] >= '0' && key[1] <= '9')) {
    key++;
    while (*key >= '0' && *key <= '9') {
      key++;
    }

    if (*key++ == '@') {
      if (*key == '^') {
        key++;
      }

      *new_len -= (key - *new_key);
      *new_key = (char *)key;
    }
  }
}

// result of read_in will always be a string with length of len
// data must have size at least (len + 1)
static inline void safe_read_in (netbuffer_t *H, char *data, int len) {
  assert (read_in (H, data, len) == len);
  int i;
  for (i = 0; i < len; i++) {
    if (data[i] == 0) {
      data[i] = ' ';
    }
  }
  data[len] = 0;
}


// -2 -- NOT_STORED and not readed
//  0 -- NOT_STORED
//  1 -- STORED
int memcache_store (struct connection *c, int op, const char *old_key, int old_key_len, int flags, int delay, int size) {
  INIT;

  hst ("memcache_store: key='%s', key_len=%d, value_len=%d\n", old_key, old_key_len, size);

  if (unlikely (size >= MAX_VALUE_LEN)) {
    RETURN (set, -2);
  }

  char *key;
  int key_len;
  eat_at (old_key, old_key_len, &key, &key_len);

  //set("question{$random_tag}", "$text")
  if (key_len >= 8 && !strncmp (key, "question", 8)) {
    int random_tag;
    if (sscanf (key + 8, "%d", &random_tag) != 1) {
      RETURN (set, -2);
    }

    if (msg_reinit (MESSAGE (c), size, random_tag) < 0) {
      RETURN (set, -2);
    }
    safe_read_in (&c->In, msg_get_buf (MESSAGE (c)), size);

    RETURN (set, 1);
  }

  if (key_len >= 6 && !strncmp (key, "answer", 6)) {
    int user_id, agent_id, mark, cur;
    safe_read_in (&c->In, buf, size);

    if (sscanf (key + 6, "%d,%d,%d%n", &user_id, &agent_id, &mark, &cur) < 3 || key[6 + cur]) {
      RETURN (set, 0);
    }

    int res = do_add_answer (user_id, agent_id, mark, size, buf);
    RETURN (set, res);
  }

  //get("mark{$question_id}", {$new_mark});
  if (key_len >= 4 && !strncmp (key, "mark", 4)) {
    int user_id, mark, cur;
    safe_read_in (&c->In, buf, size);

    if (sscanf (key + 4, "%d%n", &user_id, &cur) < 1 || key[4 + cur] || sscanf (buf, "%d%n", &mark, &cur) < 1 || buf[cur]) {
      RETURN (set, 0);
    }

    int res = do_set_mark (user_id, mark);
    RETURN (set, res);
  }

  RETURN (set, -2);
}

#define OK "OK"

int return_one_key_flags (struct connection *c, const char *key, char *val, int vlen, int flags) {
  int l = snprintf (stats_buff, STATS_BUFF_SIZE, "VALUE %s %d %d\r\n", key, flags, vlen);
  assert (l <= STATS_BUFF_SIZE);
  write_out (&c->Out, stats_buff, l);
  write_out (&c->Out, val, vlen);
  write_out (&c->Out, "\r\n", 2);
  return 0;
}

// -2 -- wait for AIO
//  0 -- all other cases
int memcache_get (struct connection *c, const char *old_key, int old_key_len) {
  hst ("memcache_get: key='%s', key_len=%d\n", old_key, old_key_len);

  char *key;
  int key_len;
  eat_at (old_key, old_key_len, &key, &key_len);

  if (key_len >= 5 && !strncmp (key, "stats", 5)) {
    int len = support_prepare_stats ();
    int len2 = prepare_stats (c, stats_buff + len, STATS_BUFF_SIZE - len);
    return_one_key (c, old_key, stats_buff, len + len2 - 1);

    return 0;
  }

  GET_LOG;

  SET_LOG_VERBOSITY;

  INIT;

  if (key_len >= 6 && !strncmp (key, "answer", 6)) {
    int add = 6, debug = 0;
    if (!strncmp (key + 6, "_debug", 6)) {
      debug = 1;
      add += 6;
    }
    int user_id, agent_id, random_tag, cur, cnt = MAX_RES;
    if (sscanf (key + add, "%d,%d,%d%n#%d%n", &user_id, &agent_id, &random_tag, &cur, &cnt, &cur) < 3 || key[add + cur]) {
      RETURN (get, 0);
    }
    message *msg = MESSAGE (c);

    if (msg_verify (msg, random_tag) < 0 || cnt <= 0) {
      msg_free (msg);
      RETURN (get, 0);
    }

    char *res = get_answer (user_id, agent_id, msg->len, msg->text, cnt, debug);

    if (res != NULL) {
      return_one_key_flags (c, old_key, res, strlen (res), 1);
    }

    msg_free (msg);
    RETURN (get, 0);
  }

  if (key_len >= 13 && !strncmp (key, "delete_answer", 13)) {
    int add = 13;
    int user_id, cur;
    if (sscanf (key + add, "%d%n", &user_id, &cur) < 1 || key[add + cur]) {
      RETURN (delete, 0);
    }

    int res = do_delete_answer (user_id);

    if (res) {
      return_one_key_flags (c, old_key, "DELETED", strlen ("DELETED"), 1);
    }

    RETURN (delete, 0);
  }

  RETURN (delete, 0);
}

int memcache_version (struct connection *c) {
  cmd_version++;
  write_out (&c->Out, "VERSION "VERSION"\r\n", 9 + sizeof (VERSION));
  return 0;
}


int memcache_stats (struct connection *c) {
  int len = support_prepare_stats ();
  int len2 = prepare_stats (c, stats_buff + len, STATS_BUFF_SIZE - len);
  write_out (&c->Out, stats_buff, len + len2);
  write_out (&c->Out, "END\r\n", 5);
  return 0;
}

/*
 *
 *       STATS
 *
 */


int support_prepare_stats (void) {
  cmd_stats++;
  int log_uncommitted = compute_uncommitted_log_bytes();

  char *s = stats_buff;
#define W(args...) WRITE_ALL (s, ## args)
  W ("version\t" VERSION_STR " compiled at " __DATE__ " " __TIME__ " by gcc " __VERSION__ "\n");

  /* binlog stats */
  W ("binlog_original_size\t%lld\n",     log_readto_pos);
  W ("binlog_loaded_bytes\t%lld\n",      log_readto_pos - jump_log_pos);
  W ("binlog_load_time\t%.6lfs\n",       binlog_load_time);
  W ("current_binlog_size\t%lld\n",      log_pos);
  W ("binlog_uncommitted_bytes\t%d\n",   log_uncommitted);
  W ("binlog_path\t%s\n",                binlogname ? (strlen (binlogname) < 250 ? binlogname : "(too long)") : "(none)");
  W ("binlog_first_timestamp\t%d\n",     log_first_ts);
  W ("binlog_read_timestamp\t%d\n",      log_read_until);
  W ("binlog_last_timestamp\t%d\n",      log_last_ts);
  W ("max_binlog_size\t%lld\n",          max_binlog_size);

  /* index stats */
  W ("index_loaded_bytes\t%d\n",         header_size);
  W ("index_size\t%lld\n",               engine_snapshot_size);
  W ("index_path\t%s\n",                 engine_snapshot_name);
  W ("index_load_time\t%.6lfs\n",        index_load_time);

  W ("answers_cnt\t%d\n",                answers_cnt);

  /* misc stats */
  W ("pid\t%d\n",                        getpid());
  W ("version\t%s\n",                    VERSION);
  W ("pointer_size\t%d\n",               (int)sizeof (void *) * 8);
  W ("cmd_get\t%lld\n",                  cmd_get);
  W ("cmd_get_time\t%.7lf\n",            cmd_get_time);
  W ("max_cmd_get_time\t%.7lf\n",        max_cmd_get_time);
  W ("cmd_set\t%lld\n",                  cmd_set);
  W ("cmd_set_time\t%.7lf\n",            cmd_set_time);
  W ("max_cmd_set_time\t%.7lf\n",        max_cmd_set_time);
  W ("cmd_stats\t%lld\n",                cmd_stats);
  W ("cmd_version\t%lld\n",              cmd_version);

  /* memory stats */
  W ("current_memory_used\t%ld\n",       (long)dl_get_memory_used());
  W ("limit_max_dynamic_memory\t%ld\n",  max_memory);
#undef W

  int len = s - stats_buff;
  assert (len < STATS_BUFF_SIZE);
  return len;
}

/***
  SERVER
 ***/

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
  if (logname && (fd = open (logname, O_WRONLY | O_APPEND | O_CREAT, 0640)) != -1) {
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

static void sigint_immediate_handler (const int sig) {
  const char message[] = "SIGINT handled immediately.\n";
  kwrite (2, message, sizeof (message) - (size_t)1);

  exit (1);
}

static void sigterm_immediate_handler (const int sig) {
  const char message[] = "SIGTERM handled immediately.\n";
  kwrite (2, message, sizeof (message) - (size_t)1);

  exit (1);
}

static void sigint_handler (const int sig) {
  const char message[] = "SIGINT handled.\n";
  kwrite (2, message, sizeof (message) - (size_t)1);

  pending_signals |= (1 << sig);
  signal (sig, sigint_immediate_handler);
}

static void sigterm_handler (const int sig) {
  const char message[] = "SIGTERM handled.\n";
  kwrite (2, message, sizeof (message) - (size_t)1);

  pending_signals |= (1 << sig);
  signal (sig, sigterm_immediate_handler);
}

static void sighup_handler (const int sig) {
  const char message[] = "got SIGHUP.\n";
  kwrite (2, message, sizeof (message) - (size_t)1);

  pending_signals |= (1ll << sig);
  signal (sig, sighup_handler);
}

static void sigusr1_handler (const int sig) {
  const char message[] = "got SIGUSR1, rotate logs.\n";
  kwrite (2, message, sizeof (message) - (size_t)1);

  pending_signals |= (1ll << sig);
  signal (sig, sigusr1_handler);
}

static void sigpoll_handler (const int sig) {
  sigpoll_cnt++;
  signal (SIGPOLL, sigpoll_handler);
}

void cron (void) {
  flush_binlog();
}

int sfd;

char *teach_file_name = NULL;

void start_server (void) {
  int i;
  int prev_time;

  init_epoll();
  init_netbuffers();

  prev_time = 0;

  if (!sfd) {
    sfd = server_socket (port, settings_addr, backlog, 0);
  }

  if (sfd < 0) {
    fprintf (stderr, "cannot open server socket at port %d: %m\n", port);
    exit (3);
  }

  if (verbosity) {
    fprintf (stderr, "created listening socket at %s:%d, fd=%d\n", conv_addr (settings_addr.s_addr, stats_buff), port, sfd);
  }

  if (daemonize) {
    setsid();
  }

  if (change_user (username) < 0) {
    fprintf (stderr, "fatal: cannot change user to %s\n", username ? username : "(none)");
    exit (1);
  }

  if (binlogname && !binlog_disabled) {
    assert (append_to_binlog (Binlog) == log_readto_pos);
  }

  init_listening_connection (sfd, &ct_support_engine_server, &memcache_methods);

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

  if (verbosity > 0) {
    fprintf (stderr, "Server started\n");
  }

  if (teach_file_name != NULL) {
    FILE *f = fopen (teach_file_name, "r");

    assert (f != NULL);

    int answers = 0, bad_answers = 0;
    while (fgets (buf, MAX_VALUE_LEN, f)) {
      //{ticket_id}\t{ticket_date}\t{moder_id}\t{rate}\t{title+text}\t{answer}\n
      char *q = buf;
      int j;
      for (j = 0; j < 4; j++) {
        while (*q++ != '\t') {
        }
      }
      int user_id, agent_id, mark;
      if (sscanf (buf, "%d\t%*d\t%d\t%d", &user_id, &agent_id, &mark) != 3) {
        fputs (buf, stderr);
        bad_answers++;
        continue;
      }

      int l = strlen (q) - 1;
      q[l] = 0;
      do_add_answer (user_id, agent_id, mark, l, q);

      if (answers++ % 1000 == 0) {
//        fprintf (stderr, "Answers = %d\n", answers);
        flush_binlog_forced (0);
      }
    }
    fprintf (stderr, "Teaching has finished. Answers = %d, bad_answers = %d.\n", answers, bad_answers);

    fclose (f);
  }

  for (i = 0; !(pending_signals & ~((1ll << SIGUSR1) | (1ll << SIGHUP))); i++) {
    if (verbosity > 0 && !(i & 255)) {
      fprintf (stderr, "epoll_work(): %d out of %d connections, network buffers: %d used, %d out of %d allocated\n",
         active_connections, maxconn, NB_used, NB_alloc, NB_max);
    }
    epoll_work (57);

    if (sigpoll_cnt > 0) {
      if (verbosity > 1) {
        fprintf (stderr, "after epoll_work(), sigpoll_cnt=%d\n", sigpoll_cnt);
      }
      sigpoll_cnt = 0;
    }

    if (pending_signals & (1ll << SIGHUP)) {
      pending_signals &= ~(1ll << SIGHUP);

      sync_binlog (2);
    }

    if (pending_signals & (1ll << SIGUSR1)) {
      pending_signals &= ~(1ll << SIGUSR1);

      reopen_logs();
      sync_binlog (2);
    }

    if (now != prev_time) {
      prev_time = now;
      cron();
    }

    if (epoll_pre_event) {
      epoll_pre_event();
    }

    if (quit_steps && !--quit_steps) break;
  }

  if (verbosity > 0 && pending_signals) {
    fprintf (stderr, "Quitting because of pending signals = %llx\n", pending_signals);
  }

  epoll_close (sfd);
  assert (close (sfd) >= 0);

  flush_binlog_last();
  sync_binlog (2);
}


/*
 *
 *     MAIN
 *
 */


void usage (void) {
  printf ("usage: %s [options] <index-file>\n"
    "\t" VERSION_STR " compiled at " __DATE__ " " __TIME__ " by gcc " __VERSION__ " "
#ifdef __LP64__
    "64-bit"
#else
    "32-bit"
#endif
    " after commit " COMMIT "\n"
    "\t%s\n",
    progname,
    index_mode ?
    "Generates new index of support": 
    "Tries its best to get good answer on support questions");

  parse_usage();
  exit (2);
}

int support_parse_option (int val) {
  switch (val) {
    case 'm':
      max_memory = atoi (optarg);
      if (max_memory < 1) {
        max_memory = 1;
      }
      max_memory *= 1048576;
      break;
    case 't':
      teach_file_name = optarg;
      break;
    case 'i':
      index_mode = 1;
      break;
    case 'k':
      if (mlockall (MCL_CURRENT | MCL_FUTURE) != 0) {
        fprintf (stderr, "error: fail to lock paged memory\n");
      }
      break;
    default:
      return -1;
  }
  return 0;
}

int main (int argc, char *argv[]) {
  int i;

  dl_set_debug_handlers();
  progname = argv[0];
  now = time (NULL);

  index_mode = 0;
  if (strstr (progname, "support-index") != NULL) {
    index_mode = 1;
  }

  remove_parse_option (204);
  parse_option ("memory-limit", required_argument, NULL, 'm', "<memory-limit> sets maximal size of used memory not including zmemory in mebibytes");
  parse_option ("teach-file", required_argument, NULL, 't', "<teach-file-name> name of file with dumped answers from database");
  if (!index_mode) {
    parse_option ("index-mode", no_argument, NULL, 'i', "run in index mode");
  }
  parse_option ("lock-memory", no_argument, NULL, 'k', "lock paged memory");

  parse_engine_options_long (argc, argv, support_parse_option);
  if (argc != optind + 1) {
    usage();
    return 2;
  }

  if (verbosity) {
    fprintf (stderr, "index_mode = %d\n", index_mode);
  }

  if (index_mode) {
    binlog_disabled = 1;
  }

  if (!username && maxconn == MAX_CONNECTIONS && geteuid()) {
    maxconn = 1000; //not for root
  }

  if (raise_file_rlimit (maxconn + 16) < 0) {
    fprintf (stderr, "fatal: cannot raise open file limit to %d\n", maxconn + 16);
    exit (1);
  }

  if (!index_mode) {
    if (port < PRIVILEGED_TCP_PORTS) {
      sfd = server_socket (port, settings_addr, backlog, 0);
      if (sfd < 0) {
        fprintf (stderr, "cannot open server socket at port %d: %m\n", port);
        exit (1);
      }
    }
  }

  aes_load_pwd_file (NULL);

  if (change_user (username) < 0 && !interactive) {
    fprintf (stderr, "fatal: cannot change user to %s\n", username ? username : "(none)");
    exit (1);
  }

  if (engine_preload_filelist (argv[optind], binlogname) < 0) {
    fprintf (stderr, "cannot open binlog files for %s\n", binlogname ? binlogname : argv[optind]);
    exit (1);
  }

  //Snapshot reading
  Snapshot = open_recent_snapshot (engine_snapshot_replica);

  if (Snapshot) {
    engine_snapshot_name = strdup (Snapshot->info->filename);
    engine_snapshot_size = Snapshot->info->file_size;

    if (verbosity) {
      fprintf (stderr, "loading index file %s (size %lld)\n", engine_snapshot_name, engine_snapshot_size);
    }
  } else {
    engine_snapshot_name = NULL;
    engine_snapshot_size = 0;
  }

  index_load_time = -mytime();

  i = init_all (Snapshot);

  index_load_time += mytime();

  if (i < 0) {
    fprintf (stderr, "fatal: error %d while loading index file %s\n", i, engine_snapshot_name);
    exit (1);
  }

  if (verbosity > 0) {
    fprintf (stderr, "load index: done, jump_log_pos=%lld, alloc_mem=%ld, time %.06lfs\n",
       jump_log_pos, (long)dl_get_memory_used(), index_load_time);
  }
  dbg ("load index: done, jump_log_pos=%lld, alloc_mem=%ld, time %.06lfs\n", jump_log_pos, (long)dl_get_memory_used(), index_load_time);

  close_snapshot (Snapshot, 1);

  //Binlog reading
  Binlog = open_binlog (engine_replica, jump_log_pos);
  if (!Binlog) {
    fprintf (stderr, "fatal: cannot find binlog for %s, log position %lld\n", engine_replica->replica_prefix, jump_log_pos);
    exit (1);
  }

  binlogname = Binlog->info->filename;

  if (verbosity > 0) {
    fprintf (stderr, "replaying binlog file %s (size %lld)\n", binlogname, Binlog->info->file_size);
  }
  binlog_load_time = -mytime();

  clear_log();
  init_log_data (jump_log_pos, jump_log_ts, jump_log_crc32);

  dbg ("jump_log_pos = %lld\n", jump_log_pos);

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

  if (verbosity) {
    fprintf (stderr, "replay binlog file: done, log_pos=%lld, alloc_mem=%ld, time %.06lfs\n",
             (long long) log_pos, (long)dl_get_memory_used(), binlog_load_time);
  }
  dbg ("replay binlog file: done, log_pos=%lld, alloc_mem=%lld, time %.06lfs\n", (long long) log_pos, dl_get_memory_used(), binlog_load_time);

  clear_write_log();

  binlog_readed = 1;

  start_time = time (NULL);

  if (index_mode) {
    int result = save_index();

    if (verbosity) {
      int len = support_prepare_stats();
      stats_buff[len] = 0;
      fprintf (stderr, "%s\n", stats_buff);
    }

    free_all();
    return result;
  }

  start_server();

  free_all();
  return 0;
}
