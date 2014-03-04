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
              2011-2013 Arseny Smirnov
              2011-2013 Aliaksei Levin
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
#include "server-functions.h"
#include "net-connections.h"
#include "net-memcache-server.h"
#include "net-crypto-aes.h"

#include "net-rpc-server.h"
#include "net-rpc-common.h"

#include "letters-data.h"

#define MAX_VALUE_LEN (1 << 20)

#define VERSION "0.01"
#define VERSION_STR "letters "VERSION

#define TCP_PORT 11211
#define UDP_PORT 11211

/*
 *
 *    MEMCACHED PORT
 *
 */

int port = TCP_PORT, udp_port = UDP_PORT;

struct in_addr settings_addr;
int interactive = 0;

volatile int sigpoll_cnt;

char *index_name;

double index_load_time;

long long cmd_get, cmd_set, cmd_delete, cmd_stats, cmd_version;
double cmd_get_time = 0.0, cmd_set_time = 0.0, cmd_delete_time = 0.0;
double max_cmd_get_time = 0.0, max_cmd_set_time = 0.0, max_cmd_delete_time = 0.0;

#define STATS_BUFF_SIZE (1 << 16)
int stats_buff_len;
char stats_buff[STATS_BUFF_SIZE];
int letters_prepare_stats (void);

conn_type_t ct_letters_engine_server = {
  .magic = CONN_FUNC_MAGIC,
  .title = "letters_engine_server",
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
int memcache_delete (struct connection *c, const char *key, int key_len);
int memcache_stats (struct connection *c);
int memcache_version (struct connection *c);

struct memcache_server_functions memcache_methods = {
  .execute = mcs_execute,
  .mc_store = memcache_store,
  .mc_get_start = mcs_get_start,
  .mc_get = memcache_get,
  .mc_get_end = mcs_get_end,
  .mc_incr = mcs_incr,
  .mc_delete = memcache_delete,
  .mc_version = memcache_version,
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
  .memcache_fallback_type = &ct_letters_engine_server,
  .memcache_fallback_extra = &memcache_methods
};


char buf[MAX_VALUE_LEN];

static inline void safe_read_in (netbuffer_t *H, char *data, int len) {
  assert (read_in (H, data, len) == len);
  int i;
  for (i = 0; i < len; i++) {
    if (data[i] == 0) {
      data[i] = ' ';
    }
  }
}

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

int memcache_store (struct connection *c, int op, const char *old_key, int old_key_len, int flags, int delay, int size) {
  INIT;

  if (verbosity > 1) {
    fprintf (stderr, "memcache_store: key='%s', key_len=%d, value_len=%d, \n", old_key, old_key_len, size);
  }

  if (size + 1 < MAX_VALUE_LEN) {
    char *key;
    int key_len;

    eat_at (old_key, old_key_len, &key, &key_len);

    if (key_len >= 15 && !strncmp (key, "letter_priority", 15)) {
      int priority;
      long long id;
      int delay = 0;

      if (sscanf (key + 15, "%lld,%d,%d", &id, &priority, &delay) < 2 || priority <= 0 || priority >= MAX_PRIORITY || delay < 0 || delay > MAX_DELAY) {
        RETURN(set, -2);
      }

      safe_read_in (&c->In, buf, size);
      buf[size] = 0;

      int result = add_letter_priority (id, priority, delay, buf);
      RETURN(set, result);
    }

    if (key_len >= 6 && !strncmp (key, "letter", 6)) {
      int engine_num;
      int delay = 0;
      long long task_id = 0;

      if (sscanf (key + 6, "%d,%d,%lld", &engine_num, &delay, &task_id) < 1 || delay < 0 || delay > MAX_DELAY) {
        RETURN(set, -2);
      }

      safe_read_in (&c->In, buf, size);
      buf[size] = 0;

      int result = add_letter (delay, task_id, buf);
      RETURN(set, result);
    }
  }

  RETURN(set, -2);
}

int return_one_key_flags (struct connection *c, const char *key, char *val, int vlen, int flags) {
  static char buff[65536];
  int l = sprintf (buff, "VALUE %s %d %d\r\n", key, flags, vlen);
  assert (l <= 65536);
  write_out (&c->Out, buff, l);
  write_out (&c->Out, val, vlen);
  write_out (&c->Out, "\r\n", 2);
  return 0;
}

int memcache_get (struct connection *c, const char *old_key, int old_key_len) {
  INIT;

  if (verbosity > 1) {
    fprintf (stderr, "memcache_get: key='%s', key_len=%d\n", old_key, old_key_len);
  }

  char *key;
  int key_len;

  eat_at (old_key, old_key_len, &key, &key_len);

  if (key_len >= 5 && !strncmp (key, "stats", 5)) {
    int len = letters_prepare_stats();
    int len2 = prepare_stats (c, stats_buff + len, STATS_BUFF_SIZE - len);
    return_one_key (c, old_key, stats_buff, len + len2 - 1);

    return 0;
  }

  if (key_len >= 7 && !strncmp (key, "letters", 7)) {
    int min_priority, max_priority, cnt, immediate_delete = 0, add = 7;
    if (!strncmp (key + add, "_immediate", 10)) {
      add += 10;
      immediate_delete = 1;
    }

    if (sscanf (key + add, "%*d,%d,%d#%d", &min_priority, &max_priority, &cnt) != 3 || min_priority <= 0 || max_priority >= MAX_PRIORITY || min_priority > max_priority) {
      RETURN(get, 0);
    }

    char *res = get_letters (min_priority, max_priority, cnt, immediate_delete);

    return_one_key_flags (c, old_key, res, strlen (res), 1);

    RETURN(get, 0);
  }

  if (key_len >= 11 && !strncmp (key, "clear_queue", 11)) {
    int priority;
    if (sscanf (key + 11, "%d", &priority) < 1 || priority < 0 || priority >= MAX_PRIORITY) {
      RETURN(get, 0);
    }

    char *ret = dl_pstr ("%lld", letters_clear (priority));
    return_one_key (c, old_key, ret, strlen (ret));

    RETURN(get, 0);
  }

  RETURN(get, 0);
}

int memcache_delete (struct connection *c, const char *old_key, int old_key_len) {
  INIT;

  if (verbosity > 1) {
    fprintf (stderr, "memcache_delete: key='%s'\n", old_key);
  }

  char *key;
  int key_len;

  eat_at (old_key, old_key_len, &key, &key_len);

  if (key_len >= 18 && !strncmp (key, "letters_by_task_id", 18)) {
    long long task_id;

    if (sscanf (key + 18, "%lld", &task_id) == 1 && delete_letters_by_task_id (task_id)) {
      write_out (&c->Out, "DELETED\r\n", 9);
    } else {
      write_out (&c->Out, "NOT_FOUND\r\n", 11);
    }

    RETURN(delete, 0);
  }

  if (key_len >= 6 && !strncmp (key, "letter", 6)) {
    long long id;

    if (sscanf (key + 6, "%lld", &id) == 1 && delete_letter (id)) {
      write_out (&c->Out, "DELETED\r\n", 9);
    } else {
      write_out (&c->Out, "NOT_FOUND\r\n", 11);
    }

    RETURN(delete, 0);
  }

  write_out (&c->Out, "NOT_FOUND\r\n", 11);
  RETURN(delete, 0);
}

int memcache_version (struct connection *c) {
  cmd_version++;
  write_out (&c->Out, "VERSION "VERSION"\r\n", 9 + sizeof (VERSION));
  return 0;
}

int letters_prepare_stats (void) {
  cmd_stats++;

  char *s = stats_buff + snprintf (stats_buff, STATS_BUFF_SIZE,
        "index_path\t%s\n"
        "index_load_time\t%.6lfs\n"
        "pid\t%d\n"
        "version\t%s\n"
        "pointer_size\t%d\n"
        "current_memory_used\t%lld\n"
        "cmd_get\t%lld\n"
        "cmd_set\t%lld\n"
        "cmd_delete\t%lld\n"
        "cmd_stats\t%lld\n"
        "cmd_version\t%lld\n"
        "cmd_get_time\t%.7lf\n"
        "cmd_set_time\t%.7lf\n"
        "cmd_delete_time\t%.7lf\n"
        "max_cmd_get_time\t%.7lf\n"
        "max_cmd_set_time\t%.7lf\n"
        "max_cmd_delete_time\t%.7lf\n"
        "max_current_sync_delay\t%d\n"
        "expired_letters\t%lld\n"
        "task_deletes_begin\t%d\n"
        "task_deletes_size\t%d\n"
        "limit_max_dynamic_memory\t%ld\n",
        index_name ? index_name : "<none>",
        index_load_time,
        getpid(),
        VERSION,
        (int)(sizeof (void *) * 8),
        dl_get_memory_used(),
        cmd_get,
        cmd_set,
        cmd_delete,
        cmd_stats,
        cmd_version,
        cmd_get_time,
        cmd_set_time,
        cmd_delete_time,
        max_cmd_get_time,
        max_cmd_set_time,
        max_cmd_delete_time,
        get_sync_delay(),
        expired_letters,
        task_deletes_begin,
        task_deletes_size,
        max_memory
        );

  int i, j;
  for (i = 0; i < MAX_PRIORITY; i++) {
    long long used = get_drive_buffer_size (i);
    long long size = get_drive_buffer_mx (i);
    s += sprintf (s, "\t\nbuffer_usage_%d\t%lld/%lld = %lld%%\n", i, used, size, used * 100 / size);
    for (j = 0; j < 6; j++) {
      s += sprintf (s, "%s_%d\t%lld\n", letter_stat_name[j], i, letter_stat[i][j]);
    }
  }

  s += sprintf (s, "\t\nversion\t" VERSION_STR " compiled at " __DATE__ " " __TIME__ " by gcc " __VERSION__ "\n");
  return s - stats_buff;
}

int memcache_stats (struct connection *c) {
  int len = letters_prepare_stats();
  int len2 = prepare_stats (c, stats_buff + len, STATS_BUFF_SIZE - len);
  write_out (&c->Out, stats_buff, len + len2);
  write_out (&c->Out, "END\r\n", 5);
  return 0;
}

/*
 *
 *      END (PROXY MEMCACHE SERVER)
 *
 */

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
  if (logname && (fd = open (logname, O_WRONLY|O_APPEND|O_CREAT, 0640)) != -1) {
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
  letter_delete_time (60, "Time expired");

  process_delayed_letters (0, 0);

  flush_all (0);
}

int sfd;

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

  if (verbosity > 0) {
    fprintf (stderr, "created listening socket at %s:%d, fd=%d\n", conv_addr (settings_addr.s_addr, buf), port, sfd);
  }

  if (daemonize) {
    setsid();
  }

  if (change_user (username) < 0) {
    fprintf (stderr, "fatal: cannot change user to %s\n", username ? username : "(none)");
    exit (1);
  }

  init_listening_connection (sfd, &ct_rpc_server, &rpc_methods);

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

  for (i = 0; !(pending_signals & ~((1ll << SIGUSR1) | (1ll << SIGHUP))); i++) {
    if (verbosity > 0 && !(i & 255)) {
      fprintf (stderr, "epoll_work(): %d out of %d connections, network bufers: %d used, %d out of %d allocated\n",
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

      flush_all (1);
    }

    if (pending_signals & (1ll << SIGUSR1)) {
      pending_signals &= ~(1ll << SIGUSR1);

      reopen_logs();
      flush_all (1);
    }

    tl_restart_all_ready();

    if (now != prev_time) {
      prev_time = now;
      cron();
    }

    if (quit_steps && !--quit_steps) break;
  }

  if (verbosity > 0 && pending_signals) {
    fprintf (stderr, "Quitting because of pending signals = %llx\n", pending_signals);
  }

  epoll_close (sfd);
  assert (close (sfd) >= 0);
}


/*
 *
 *    MAIN
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
    "\tManages postponed tasks\n",
    progname);

  parse_usage();
  exit (2);
}

static long long size[MAX_PRIORITY];

int letters_parse_option (int val) {
  switch (val) {
    case 'M': {
      const char *s = optarg;
      long long x = 0;
      int j, k;
      for (j = 0, k = 1; (!j || s[j - 1]) && k < MAX_PRIORITY; j++) {
        switch (s[j]) {
          case 'k':
          case 'K':
            x <<= 10;
            break;
          case 'm':
          case 'M':
            x <<= 20;
            break;
          case 'g':
          case 'G':
            x <<= 30;
            break;
          case '0' ... '9':
            x = x * 10 + s[j] - '0';
            break;
          case 0:
          case ',':
            size[k++] = x;
            x = 0;
            break;
          default:
            assert (0);
        }
      }
      assert (s[j - 1] == 0);
      break;
    }
    case 'm':
      max_memory = atoi (optarg);
      max_memory *= 1048576;
      break;
    case 'N':
      total_engines = atoi (optarg);
      break;
    case 'n':
      engine_num = atoi (optarg);
      break;
    case 'D':
      log_drive = 1;
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

  for (i = 0; i < MAX_PRIORITY; i++) {
    size[i] = DEFAULT_BUFFER_SIZE;
  }

  dl_set_debug_handlers();
  progname = argv[0];
  now = time (NULL);

  remove_parse_option ('a');
  remove_parse_option ('B');
  remove_parse_option ('r');
  remove_parse_option (204);
  parse_option ("memory-limit", required_argument, NULL, 'm', "<memory-limit> sets maximal size of used memory in mebibytes");
  parse_option ("buffer-sizes", required_argument, NULL, 'M', "<m1,m2, ... ,m9> sets size of buffer for letters with corresponding priority, buffer is stored on hard drive, modifiers GMKgmk allowed, names of files with buffers are <index-file><priority>");
  parse_option ("engine-number", required_argument, NULL, 'n', "<engine_number> number of this engine, required");
  parse_option ("total-engines", required_argument, NULL, 'N', "<total_engines> total number of engines, required");
  parse_option ("lock-memory", no_argument, NULL, 'k', "lock paged memory");
  parse_option ("dump-drive-operations", no_argument, NULL, 'D', "write drive operations to stderr");

  parse_engine_options_long (argc, argv, letters_parse_option);
  if (argc != optind + 1 || engine_num < 0 || engine_num >= total_engines) {
    usage();
    return 2;
  }

  if (!username && maxconn == MAX_CONNECTIONS && geteuid()) {
    maxconn = 1000; //not for root
  }

  if (raise_file_rlimit (maxconn + 16) < 0) {
    fprintf (stderr, "fatal: cannot raise open file limit to %d\n", maxconn + 16);
    exit (1);
  }

  if (port < PRIVILEGED_TCP_PORTS) {
    sfd = server_socket (port, settings_addr, backlog, 0);
    if (sfd < 0) {
      fprintf (stderr, "cannot open server socket at port %d: %m\n", port);
      exit (1);
    }
  }

  aes_load_pwd_file (NULL);

  if (change_user (username) < 0 && !interactive) {
    fprintf (stderr, "fatal: cannot change user to %s\n", username ? username : "(none)");
    exit (1);
  }

  index_name = argv[optind];

  index_load_time = -mytime();

  i = init_all (index_name, size);

  index_load_time += mytime();

  if (i < 0) {
    fprintf (stderr, "fatal: error %d while loading index file %s\n", i, index_name);
    exit (1);
  }

  if (verbosity > 0) {
    fprintf (stderr, "load index: done, alloc_mem=%ld, time %.06lfs\n",
      (long) dl_get_memory_used(), index_load_time);
  }

  start_time = time (NULL);

  start_server();

  free_all();

//  letters_prepare_stats();
//  fprintf (stderr, "%s\n", stats_buff);
  return 0;
}
