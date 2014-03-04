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

    Copyright 2012-2013	Vkontakte Ltd
              2012      Sergey Kopeliovich <Burunduk30@gmail.com>
              2012      Anton Timofeev <atimofeev@vkontakte.ru>
*/
/**
 * Author:  Anton  Timofeev    (atimofeev@vkontakte.ru)
 *          Server implementation and memcached wrappers.
 * Created: 22.03.2012
 */

#define _FILE_OFFSET_BITS 64

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <assert.h>
#include <fcntl.h>
#include <signal.h>
#include <unistd.h>

#include "net-connections.h"
#include "net-crypto-aes.h"
#include "net-memcache-server.h"

#include "dl-utils.h"

#include "antispam-common.h"
#include "kdb-antispam-binlog.h"
#include "antispam-index-layout.h"
#include "antispam-engine.h"
#include "antispam-data.h"
#include "antispam-db.h"

// Names and versions
#define VERSION "0.02"
#define NAME "antispam-engine"
#define NAME_VERSION NAME "-" VERSION
#ifndef COMMIT
#  define COMMIT "unknown"
#endif
static const char full_version_str[] = NAME_VERSION " compiled at " __DATE__ " " __TIME__ " by gcc " __VERSION__ " "
#ifdef __LP64__
  "64-bit"
#else
  "32-bit"
#endif
" after commit " COMMIT;

/**
 * Required variables by engine kernel
 */

#define TCP_PORT 30303
int index_mode = 0;
int backlog = BACKLOG, port = TCP_PORT, maxconn = MAX_CONNECTIONS, daemonize = 0;
struct in_addr settings_addr;
int sfd;
volatile int sigpoll_cnt;
static int flush_often = 0;

/**
 * Stats vars
 */

#define STATS_BUFF_SIZE (1 << 16)
static char stats_buff[STATS_BUFF_SIZE];
static int antispam_prepare_stats (struct connection *c);

#define CMD_CNT_VARS(name)                   \
  static long long cmd_ ## name = 0;         \
  static double cmd_ ## name ## _time = 0.0, max_cmd_ ## name ## _time = 0.0;
CMD_CNT_VARS (get_matches);
CMD_CNT_VARS (set_matches);
CMD_CNT_VARS (add_pattern);
CMD_CNT_VARS (del_pattern);
CMD_CNT_VARS (other);
static long long cmd_stats, cmd_version;

/**
 * Server connection options
 */

conn_type_t ct_antispam_engine_server = {
  .magic = CONN_FUNC_MAGIC,
  .title = "antispam_engine_server",
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

/**
 * Memcache server options
 */

typedef struct
{
  unsigned int random_tag;
  int size;
  ip_t ip;
  uahash_t uahash;
} request_t;

static char value_buf[MAX_PATTERN_LEN];

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

// Prepare tmp buffer for paired set/get request
static inline void init_tmp_buffers (struct connection *c) {
  free_tmp_buffers (c);
  c->Tmp = alloc_head_buffer ();
  assert (c->Tmp);
}

// -2 -- NOT_STORED and not readed
//  0 -- NOT_STORED
//  1 -- STORED
int memcache_store (struct connection *c, int op, const char *key, int key_len, int flags, int delay, int size) {
  INIT;

  free_tmp_buffers (c);
  if (verbosity >= 2) {
    fprintf (stderr, "memcache_store: key='%s', key_len=%d, value_len=%d\n", key, key_len, size);
  }

  if (unlikely (size >= MAX_PATTERN_LEN)) {
    RETURN (other, -2);
  }

  const int shift = 7;      // shift  == |pattern| == |matches|

  // set ("pattern{$id},{$ip},{$uahash}[,{$flags}]", $str);
  if (key_len >= shift && !strncmp (key, "pattern", shift)) {
    antispam_pattern_t p;
    int already_read = 0;
    st_safe_read_in (&c->In, value_buf, size);

    if (sscanf (key + shift, "%d,%u,%u%n,%hu%n", &p.id, &p.ip, &p.uahash, &already_read, &p.flags, &already_read) < 3
     || key[shift + already_read]) {
      RETURN (add_pattern, 0);
    }
    int res = do_add_pattern (p, strlen (value_buf), value_buf, (op != mct_add));
    RETURN (add_pattern, res);
  }
  // set ("matches{$ip},{$uahash},{$random_tag}", ${text});
  else if (key_len >= shift && !strncmp (key, "matches", shift)) {
    int already_read = 0;
    unsigned int ip, uahash, random_tag;

    st_safe_read_in (&c->In, value_buf, size);
    if (sscanf (key + shift, "%u,%u,%u%n", &ip, &uahash, &random_tag, &already_read) != 3 || key[shift + already_read]) {
      RETURN (set_matches, 0);
    }

    bool res = TRUE;
    request_t request = {random_tag, size, ip, uahash};
    init_tmp_buffers (c);
    if (verbosity >= 3) {
      st_printf ("STORED: random_tag = %u, size = %d, ip = %u, uahash = %u\n"
                   "expect '$2get matches$6%u$^' command\n", random_tag, size, ip, uahash, random_tag);
    }
    res &= (write_out (c->Tmp, &request, sizeof (request)) == sizeof (request));
    res &= (write_out (c->Tmp, value_buf, size) == size);
    RETURN (set_matches, res);
  }

  RETURN (other, -2);
}

DECLARE_VEC (char, vec_char)
static vec_char_t result_str;

// -2 -- wait for AIO
//  0 -- all other cases
static int memcache_get_without_free (struct connection *c, char const *key, int key_len) {
  if (verbosity >= 2) {
    fprintf (stderr, "memcache_get: key='%s', key_len=%d\n", key, key_len);
  }

  if (key_len >= 5 && !strncmp (key, "stats", 5)) {
    return_one_key (c, key, stats_buff, antispam_prepare_stats (c));
    return 0;
  }

  // Max cmd time isn't computed for cmd_stats
  INIT;

  const int shift      = 7;  // shift == |matches| == |pattern|

  if (key_len >= shift && !strncmp (key, "matches", shift)) {
    if (!c->Tmp) {
      RETURN (get_matches, 0);
    }

    byte fields = 0;
    char const *first = key + shift;
    char const *last  = key + key_len;
    while (first && *first == ',') {
      if (last - first >= 3 && !strncmp (first, ",id", 3)) {
        fields |= ANTISPAM_DB_FIELDS_IDS;
        first += 3;
      } else if (last - first >= 6 && !strncmp (first, ",flags", 6)) {
        fields |= ANTISPAM_DB_FIELDS_FLAGS;
        first += 6;
      } else {
        break;
      }
    }
    if (fields == 0) {
      fields = ANTISPAM_DB_FIELDS_IDS;
    }

    if (verbosity >= 3) {
      st_printf ("$3fields: %u$^\n", (unsigned)fields);
    }

    unsigned int random_tag = 0;
    int limit = 0x7FFFFFFF;
    int already_read = 0;
    if (sscanf (first, "%u%n#%d%n", &random_tag, &already_read, &limit, &already_read) < 1 || first[already_read]) {
      RETURN (get_matches, 0);
    }

    request_t request;
    assert (read_in (c->Tmp, (char*)&request, sizeof (request)) == sizeof (request));
    if (random_tag != request.random_tag) {
      if (verbosity >= 1) {
        fprintf (stderr, "Wrong 'get matches' request: different random_tags, in set = %u, in get = %u\n", request.random_tag, random_tag);
      }
      RETURN (get_matches, 0);
    }

    int size = request.size;
    assert (0 <= size && size < MAX_PATTERN_LEN);
    st_safe_read_in (c->Tmp, value_buf, size);
    assert (value_buf[size] == 0);

    // Retrieve list of matches from trie DB
    int i, matches_number = antispam_get_matches (request.ip, request.uahash, value_buf, fields, limit);
    int answer_size = st_vec_size (antispam_db_request);

    assert (sizeof (int) == 4);
    st_vec_grow (result_str, (1 + answer_size) * 11);
    char *result_str_first = st_vec_to_array (result_str);
    char *vptr = result_str_first;
    vptr += sprintf (vptr, "%d", matches_number);
    for (i = 0; i < answer_size; ++i) {
      vptr += sprintf (vptr, ",%d", st_vec_at (antispam_db_request, i));
    }
    // return string without '\0'
    return_one_key (c, key, result_str_first, vptr - result_str_first);
    RETURN (get_matches, 0);
  }
  // get ("pattern{$id}");
  else if (key_len >= shift && !strncmp (key, "pattern", shift)) {
    int id = 0, already_read = 0;
    if (sscanf (key + shift, "%d%n", &id, &already_read) != 1 || key[shift + already_read]) {
      RETURN (other, 0);
    }
    if (antispam_serialize_pattern (id, value_buf)) {
      return_one_key (c, key, value_buf, strlen (value_buf));
    }
    RETURN (other, 0);
  }
  RETURN (other, 0);
}

int memcache_get (struct connection *c, const char *key, int key_len) {
  int ret = memcache_get_without_free (c, key, key_len);
  free_tmp_buffers (c);
  return ret;
}

int memcache_delete (struct connection *c, const char *key, int key_len) {
  INIT;

  // delete ("pattern{$id}");
  const int shift = 7; // shift == |pattern|
  if (key_len >= shift && !strncmp (key, "pattern", shift)) {
    int id = 0, already_read = 0;
    if (sscanf (key + shift, "%d%n", &id, &already_read) >= 1 && !key[shift + already_read]) {
      if (do_del_pattern (id)) {
        write_out (&c->Out, "DELETED\r\n", 9);
        RETURN (del_pattern, 0);
      }
    }
  }
  write_out (&c->Out, "NOT_FOUND\r\n", 11);
  RETURN (del_pattern, 0);
}

int memcache_stats (struct connection *c) {
  int len = antispam_prepare_stats (c);
  write_out (&c->Out, stats_buff, len);
  write_out (&c->Out, "END\r\n", 5);
  return 0;
}

int memcache_version (struct connection *c) {
  cmd_version++;
  write_out (&c->Out, "VERSION "VERSION"\r\n", 9 + sizeof (VERSION));
  return 0;
}

static int antispam_prepare_stats (struct connection *c) {
  cmd_stats++;
  int log_uncommitted = compute_uncommitted_log_bytes();
  dyn_update_stats();

  char* s = stats_buff;
  char* stats_buff_end = stats_buff + STATS_BUFF_SIZE;

#define WSL(name, format, value) {              \
  assert (stats_buff_end - s >= 1000);          \
  s += sprintf (s, "%s\t" format, name, value); \
}
  WSL ("version", "%s\n",                   full_version_str);

  // binlog stats
  WSL ("binlog_original_size", "%lld\n",    log_readto_pos);
  WSL ("binlog_loaded_bytes", "%lld\n",     log_readto_pos - jump_log_pos);
  WSL ("binlog_load_time", "%.6lfs\n",      binlog_load_time);
  WSL ("current_binlog_size", "%lld\n",     log_pos);
  WSL ("binlog_uncommitted_bytes", "%d\n",  log_uncommitted);
  WSL ("binlog_path", "%s\n",               binlogname ? (sizeof (binlogname) < 250 ? binlogname : "(too long)") : "(none)");
  WSL ("binlog_first_timestamp", "%d\n",    log_first_ts);
  WSL ("binlog_read_timestamp", "%d\n",     log_read_until);
  WSL ("binlog_last_timestamp", "%d\n",     log_last_ts);
  WSL ("max_binlog_size", "%lld\n",         max_binlog_size);

  // index common stats
  WSL ("index_loaded_bytes", "%d\n",        (int)sizeof (index_header));
  WSL ("index_size", "%lld\n",              engine_snapshot_size);
  WSL ("index_path", "%s\n",                engine_snapshot_name);
  WSL ("index_load_time", "%.6lfs\n",       index_load_time);

  // misc stats
  WSL ("pid", "%d\n",                       getpid());
  WSL ("version", "%s\n",                   VERSION);
  WSL ("pointer_size", "%d\n",              (int)sizeof (void *) * 8);

  // antispam-db
  antispam_write_engine_stats (&s, stats_buff_end);

  // command counters stats
#define CMD_CNT_VARS_W(name) \
  WSL ("cmd_" #name "", "%lld\n",           cmd_ ## name); \
  WSL ("cmd_" #name "_time", "%.7lf\n",     cmd_ ## name ## _time); \
  WSL ("max_cmd_" #name "_time", "%.7lf\n", max_cmd_ ## name ## _time);

  CMD_CNT_VARS_W (get_matches);
  CMD_CNT_VARS_W (set_matches);
  CMD_CNT_VARS_W (add_pattern);
  CMD_CNT_VARS_W (del_pattern);
  CMD_CNT_VARS_W (other);
  WSL ("cmd_stats", "%lld\n",               cmd_stats);
  WSL ("cmd_version", "%lld\n",             cmd_version);

  // memory stats
  WSL ("dl_malloc", "%lld\n",               (long long)(dl_get_memory_used ()));
  WSL ("z_heap_allocated", "%ld\n",         (long)(dyn_cur - dyn_first));
  WSL ("z_heap_max", "%ld\n",               (long)(dyn_last - dyn_first));
  WSL ("wasted_heap_blocks", "%d\n",        (int)wasted_blocks);
  WSL ("wasted_heap_bytes", "%ld\n",        (long)wasted_bytes);
  WSL ("free_heap_blocks", "%d\n",          (int)freed_blocks);
  WSL ("free_heap_bytes", "%ld\n",          (long)freed_bytes);
  WSL ("current_z_memory_used", "%ld\n",    (long)((dyn_cur - dyn_first) - freed_bytes));

#undef CMD_CNT_VARS_W
#undef WSL

  int len = s - stats_buff;
  assert (len < STATS_BUFF_SIZE);
  len += prepare_stats(c, s, STATS_BUFF_SIZE - len);
  assert (len < STATS_BUFF_SIZE);
  return len;
}

/**
 * Server implementation
 */

static void reopen_logs (void) {
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

  pending_signals |= (1 << SIGINT);
  signal (SIGINT, sigint_immediate_handler);
}

static void sigterm_handler (const int sig) {
  const char message[] = "SIGTERM handled.\n";
  kwrite (2, message, sizeof (message) - (size_t)1);

  pending_signals |= (1 << SIGTERM);
  signal (SIGTERM, sigterm_immediate_handler);
}

// Handled only if 'daemonize'
static void sighup_handler (const int sig) {
  const char message[] = "got SIGHUP.\n";
  kwrite (2, message, sizeof (message) - (size_t)1);

  sync_binlog (2);
  signal (SIGHUP, sighup_handler);
}

static void sigusr1_handler (const int sig) {
  const char message[] = "got SIGUSR1, rotate logs.\n";
  kwrite (2, message, sizeof (message) - (size_t)1);

  reopen_logs();
  sync_binlog (2);
  signal (SIGUSR1, sigusr1_handler);
}

static void sigpoll_handler (const int sig) {
  sigpoll_cnt++;
  signal (SIGPOLL, sigpoll_handler);
}

static void cron (void) {
  flush_binlog();
}

static void start_server (void) {
  int i;
  int prev_time = 0;

  if (verbosity >= 2) {
    fprintf (stderr, "Run server...\n");
  }

  init_epoll();
  init_netbuffers();

  if (!sfd) {
    sfd = server_socket (port, settings_addr, backlog, 0);
  }
  if (sfd < 0) {
    fprintf (stderr, "cannot open server socket at port %d: %m\n", port);
    exit (3);
  }

  if (verbosity > 0) {
    char buf[64];
    fprintf (stderr, "created listening socket at %s:%d, sfd=%d\n", conv_addr (settings_addr.s_addr, buf), port, sfd);
  }

  if (daemonize) {
    setsid();
  }

  if (binlogname && binlog_disabled != 1) {
    assert (append_to_binlog (Binlog) == log_readto_pos);
  }

  init_listening_connection (sfd, &ct_antispam_engine_server, &memcache_methods);

  if (binlog_disabled && binlog_fd >= 0) {
    epoll_pre_event = read_new_events;
  }

  signal (SIGINT,  sigint_handler);
  signal (SIGTERM, sigterm_handler);
  signal (SIGUSR1, sigusr1_handler);
  signal (SIGPIPE, SIG_IGN);
  signal (SIGPOLL, sigpoll_handler);
  if (daemonize) {
    signal (SIGHUP, sighup_handler);
    reopen_logs();
  }

  if (verbosity >= 2) {
    fprintf (stderr, "Server started!\n");
  }

  for (i = 0; !pending_signals; i++) {
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

    if (now != prev_time) {
      prev_time = now;
      cron();
    } else if (flush_often) {
      flush_binlog ();
    }

    if (epoll_pre_event) {
      epoll_pre_event();
    }

    if (quit_steps && !--quit_steps) {
      break;
    }
  }

  if (verbosity > 0 && pending_signals) {
    fprintf (stderr, "Quitting because of pending signals = %llx\n", pending_signals);
  }

  epoll_close (sfd);
  assert (close (sfd) >= 0);

  flush_binlog_last();
  sync_binlog (2);
}

/**
 * Main: reading engine options and initialization
 */

void usage (void) {
  printf ("%s\n", full_version_str);
  printf ("usage: %s [-v] [-r] [-i] [-p<port>] [-u<username>] [-b<backlog>] [-c<max-conn>] [-a<binlog-name>] [-B<max-binlog-size>] [-l<log-name>] [-d] [-W] <index-prefix>\n"
    "\tPerforms antispam patterns queries\n"
    "\t<index-prefix>\tprefix to generate name of index files\n"
    "\t              \tand binlog files (if there is no -a option)\n"
    "\t-h\tdisplay this message\n"
    "\t-v\toutput statistical and debug information into stderr\n"
    "\t  \t(type several times to increase verbosity level)\n"
    "\t-r\tread-only binlog (don't log new events)\n"
    "\t-i\tenable index mode (create index snapshot instead of running)\n"
    "\t-p<port>\tspecify where to open server socket (default 30303)\n"
    "\t-u<username>\tspecify process owner\n"
    "\t-c<max-conn>\tmaximum number of connection to this engine instance\n"
    "\t  \t(default: 1000/65536 depends on your permissions level)\n"
    "\t-a<binlog-name>\tspecify binlog to load from\n"
    "\t-B<max-binlog-size>\tdefines maximum size of each binlog file\n"
    "\t-l<log-name>\tspecify where to write stderr log\n"
    "\t-d\tenable daemonize mode\n"
    "\t-W\tflush binlog in main cycle\n",
    progname);
  exit (2);
}

int main (int argc, char **argv) {
  progname = argv[0];
  int i = 0;
  long long x = 0;
  char c = 0;
  while ((i = getopt (argc, argv, "hvrip:u:b:c:a:l:dW")) != -1) {
    switch (i) {
    case 'h':
      usage();
      return 2;
    case 'v':
      verbosity++;
      break;
    case 'r':
      binlog_disabled = 1;
      break;
    case 'i':
      index_mode = 1;
      break;
    case 'p':
      port = atoi (optarg);
      break;
    case 'u':
      username = optarg;
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
    case 'a':
      binlogname = optarg;
      break;
    case 'B':
      assert (sscanf (optarg, "%lld%c", &x, &c) >= 1);
      switch (c | 0x20) {
      case 'k':  x <<= 10; break;
      case 'm':  x <<= 20; break;
      case 'g':  x <<= 30; break;
      case 't':  x <<= 40; break;
      default: assert (c == 0x20);
      }
      if (x >= 1024 && x < (1LL << 60)) {
        max_binlog_size = x;
      }
      break;
    case 'l':
      logname = optarg;
      break;
    case 'd':
      daemonize = 1;
      break;
    case 'W':
      flush_often = 1;
      break;
    default:
      usage ();
      return 2;
    }
  }

  if (argc != optind + 1) {
    fprintf (stderr, "wrong number of parameters specified\n");
    usage();
    return 2;
  }

  // Decrease MAX_CONNECTIONS number to 1000
  // if username not specified, user != root and maxconn set to max value
  if (!username && maxconn == MAX_CONNECTIONS && geteuid()) {
    maxconn = 1000; //not for root
  }

  // manually switch engine to index_mode by binary name suffix
  int len = strlen (progname);
  if (len >= 5 && memcmp (progname + len - 5, "index" , 5) == 0) {
    index_mode = 1;
  }
  if (verbosity > 0) {
    fprintf (stderr, "index_mode: %s\n", index_mode ? "Enabled" : "Disabled");
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

  aes_load_pwd_file (0);

  antispam_change_user ();

  // argv[optind] == <index-file>
  antispam_engine_common_init_part (argv[optind]);

  st_vec_create (result_str, 1024);

  if (index_mode) {
    // TODO: implementation
    st_printf ("$1index_mode $2is not supported$^\n");
    assert (0);
  } else {
    start_server();
    finish_all();
  }

  st_vec_destroy (result_str);

  if (verbosity > 2) {
    st_printf ("Memory lost: z_malloc = $3%ld$^, dl_malloc = $3%lld$^\n", dyn_used_memory (), dl_get_memory_used ());
  }

  mt_test();
  return 0;
}
