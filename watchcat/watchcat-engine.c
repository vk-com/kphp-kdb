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
#include <netdb.h>

#include "crc32.h"
#include "kdb-data-common.h"
#include "net-events.h"
#include "net-buffers.h"
#include "server-functions.h"
#include "net-connections.h"
#include "net-memcache-server.h"
#include "net-memcache-client.h"
#include "net-crypto-aes.h"
#include "word-split.h"
#include "stemmer-new.h"


#include "watchcat-data.h"

#define MAX_VALUE_LEN (1 << 20)

#define VERSION "0.01"
#define VERSION_STR "watchcat "VERSION

#define TCP_PORT 11211
#define UDP_PORT 11211

int my_verbosity_tmp = 0;

/*
 *
 *    WATCHCAT PORT
 *
 */

volatile int sigpoll_cnt;

int queue_port = 6650;

int port = TCP_PORT, udp_port = UDP_PORT;
long max_memory = MAX_MEMORY;
struct in_addr settings_addr;
int interactive = 0;

char *hostname = "localhost";

long long cmd_get, cmd_set, cmd_version, cmd_stats;
double cmd_get_time = 0.0, cmd_set_time = 0.0;
double max_cmd_get_time = 0.0, max_cmd_set_time = 0.0;
long long sent_queries;

int stats_buff_len;
char stats_buff[MAX_VALUE_LEN];
char buf[MAX_VALUE_LEN + 1000];

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

int watchcat_prepare_stats (void);
int send_to_queue (char *query, int query_len, int q_id);

conn_type_t ct_watchcat_engine_server = {
  .magic = CONN_FUNC_MAGIC,
  .title = "watchcat_engine_server",
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

static inline void safe_read_in (netbuffer_t *H, char *data, int len) {
  assert (read_in (H, data, len) == len);
  int i;
  for (i = 0; i < len; i++) {
    if (data[i] == 0) {
      data[i] = ' ';
    }
  }
}

int memcache_store (struct connection *c, int op, const char *old_key, int old_key_len, int flags, int delay, int size) {
  INIT;

  if (my_verbosity_tmp > 1) {
    fprintf (stderr, "memcache_store: key='%s', key_len=%d, value_len=%d, flags = %d, exp_time = %d\n", old_key, old_key_len, size, flags, delay);
  }

  if (size < MAX_VALUE_LEN) {
    char *key;
    int key_len;

    eat_at (old_key, old_key_len, &key, &key_len);

    if (!strcmp (key, "entry")) {
      safe_read_in (&c->In, stats_buff, size);
      stats_buff[size] = 0;

      if (my_verbosity_tmp > 1) {
        fprintf (stderr, "entry: %s\n", stats_buff);
      }
      char *res = gen_addrs (stats_buff);
      if (ans_n) {
        //fprintf (stderr, "entry:%s\n", stats_buff);
        int i, f = 0;
        char *s = buf;

        for (i = 0; i < ans_n; i++) {
          if (f) {
            *s++ = ',';
          } else {
            f = 1;
          }

          s += sprintf (s, "%lld", ans[i].w_id);

          if (my_verbosity_tmp > 1) {
            fprintf (stderr, "%d:%lld\n", ans[i].q_id, ans[i].w_id);
          }
          if (i + 1 == ans_n || ans[i + 1].q_id != ans[i].q_id) {
            *s++ = ';';
            *s = 0;

            //TODO buffer overflow is here
            WRITE_STRING (s, res);
            send_to_queue (buf, s - buf, ans[i].q_id);
            s = buf;
            f = 0;
          }
        }
      }

      RETURN(set, 1);
    }
  }

  RETURN(set, -2);
}

int memcache_get (struct connection *c, const char *old_key, int old_key_len) {
  INIT;

  if (my_verbosity_tmp > 1) {
    fprintf (stderr, "memcache_get: key='%s'\n", old_key);
  }

  char *key;
  int key_len;

  eat_at (old_key, old_key_len, &key, &key_len);

  if (key_len >= 5 && !strncmp (key, "stats", 5)) {
    int len = watchcat_prepare_stats ();
    int len2 = prepare_stats (c, stats_buff + len, MAX_VALUE_LEN - len);
    return_one_key (c, key, stats_buff, len + len2 - 1);

    return 0;
  }

  if (key_len >= 15 && !strncmp (key, "create_watchcat", 15) ) {
    int len;
    long long id;
    int timeout, q_id;

    if (sscanf (key + 15, "%lld,%d,%d%n", &id, &timeout, &q_id, &len) == 3 && key[len += 15] == '(' && key[key_len - 1] == ')') {
      char *s = stats_buff;
      int n = key_len - len - 2;
      memcpy (s, key + len + 1, n);
      s[n] = 0;

      subscribe_watchcat (id, s, q_id, timeout);
    }
  }

  RETURN(get, 0);
}


int memcache_version (struct connection *c) {
  cmd_version++;
  write_out (&c->Out, "VERSION "VERSION"\r\n", 9 + sizeof (VERSION));
  return 0;
}

int watchcat_prepare_stats (void) {
  cmd_stats++;

  return snprintf (stats_buff, MAX_VALUE_LEN,
        "pid\t%d\n"
        "version\t%s\n"
        "pointer_size\t%d\n"
        "current_memory_used\t%lld\n"
        "cmd_get\t%lld\n"
        "cmd_set\t%lld\n"
        "cmd_stats\t%lld\n"
        "cmd_version\t%lld\n"
        "cmd_get_time\t%.7lf\n"
        "cmd_set_time\t%.7lf\n"
        "max_cmd_get_time\t%.7lf\n"
        "max_cmd_set_time\t%.7lf\n"
        "watchcats_memory\t%ld\n"
        "watchcats_count\t%d\n"
        "keys_memory\t%ld\n"
        "keys_count\t%d\n"
        "sent_queries\t%lld\n"
        "limit_max_dynamic_memory\t%ld\n"
        "version\t" VERSION_STR " compiled at " __DATE__ " " __TIME__ " by gcc " __VERSION__ "\n",
        getpid(),
        VERSION,
        (int)(sizeof (void *) * 8),
        dl_get_memory_used(),
        cmd_get,
        cmd_set,
        cmd_stats,
        cmd_version,
        cmd_get_time,
        cmd_set_time,
        max_cmd_get_time,
        max_cmd_set_time,
        watchcats_memory,
        watchcats_cnt,
        keys_memory,
        keys_cnt,
        sent_queries,
        max_memory);
}

int memcache_stats (struct connection *c) {
  int len = watchcat_prepare_stats();
  int len2 = prepare_stats (c, stats_buff + len, MAX_VALUE_LEN - len);
  write_out (&c->Out, stats_buff, len + len2);
  write_out (&c->Out, "END\r\n", 5);
  return 0;
}

/*
 *
 *      END (PROXY MEMCACHE SERVER)
 *
 */

/*
 *
 *  MEMCACHED CLIENT
 *
 *
 */

int proxy_client_execute (struct connection *c, int op);

struct memcache_client_functions mc_proxy_outbound = {
  .execute = proxy_client_execute,
  .check_ready = server_check_ready,

  .flush_query = mcc_flush_query,
  .mc_check_perm = mcc_default_check_perm,
  .mc_init_crypto = mcc_init_crypto,
  .mc_start_crypto = mcc_start_crypto
};

struct conn_target default_ct = {
.min_connections = 1,
.max_connections = 1,
.type = &ct_memcache_client,
.extra = &mc_proxy_outbound,
.reconnect_timeout = 1
}, *queue_conn;


struct in_addr settings_addr;

struct connection *get_target_connection (struct conn_target *S) {
  struct connection *c, *d = 0;
  int r, u = 10000;
  if (!S) {
    return 0;
  }
  for (c = S->first_conn; c != (struct connection *) S; c = c->next) {
    r = server_check_ready (c);
    if (r == cr_ok) {
      return c;
    } else if (r == cr_stopped && c->unreliability < u) {
      u = c->unreliability;
      d = c;
    }
  }
  /* all connections failed? */
  return d;
}


int send_to_queue (char *query, int query_len, int q_id) {
  struct conn_target *S = queue_conn;
  struct connection *d = get_target_connection (S);
  if (!d) {
    if (verbosity > 0) {
      fprintf (stderr, "cannot find connection to target %s:%d for query %s, dropping query\n", S ? conv_addr (S->target.s_addr, 0) : "?", S ? S->port : 0, query);
    }
    return -1;
  }
  if (my_verbosity_tmp > 1) {
    fprintf (stderr, "send query %d|%s\n", q_id, query);
  }

  static char header[50];
  int hn = sprintf (header, "set %d@entry 0 0 %d\r\n", q_id, query_len);

  assert (write_out (&d->Out, header, hn) == hn);
  assert (write_out (&d->Out, query, query_len) == query_len);
  assert (write_out (&d->Out, "\r\n", 2) == 2);

  sent_queries++;

  MCC_FUNC (d)->flush_query (d);
  d->last_query_sent_time = precise_now;
  return 0;
}


int proxy_client_execute (struct connection *c, int op) {
  struct mcc_data *D = MCC_DATA(c);

  if (verbosity > 0) {
    fprintf (stderr, "proxy_mc_client: op=%d, key_len=%d, arg#=%d, response_len=%d\n", op, D->key_len, D->arg_num, D->response_len);
  }

  c->last_response_time = precise_now;
  return SKIP_ALL_BYTES;
}


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

static void sigint_handler (const int sig) {
  const char message[] = "SIGINT handled.\n";
  kwrite (2, message, sizeof (message) - (size_t)1);

  free_all();
  exit (EXIT_SUCCESS);
}

static void sigterm_handler (const int sig) {
  const char message[] = "SIGTERM handled.\n";
  kwrite (2, message, sizeof (message) - (size_t)1);

  free_all();
  exit (EXIT_SUCCESS);
}

static void sighup_handler (const int sig) {
  const char message[] = "got SIGHUP.\n";
  kwrite (2, message, sizeof (message) - (size_t)1);

  signal (SIGHUP, sighup_handler);
}

static void sigusr1_handler (const int sig) {
  const char message[] = "got SIGUSR1, rotate logs.\n";
  kwrite (2, message, sizeof (message) - (size_t)1);

  reopen_logs();
  signal (SIGUSR1, sigusr1_handler);
}

static void sigpoll_handler (const int sig) {
  sigpoll_cnt++;
  signal (SIGPOLL, sigpoll_handler);
}

void cron (void) {
  create_all_outbound_connections();

  free_by_time (137);
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

  if (verbosity) {
    fprintf (stderr, "created listening socket at %s:%d, fd=%d\n", conv_addr (settings_addr.s_addr, buf), port, sfd);
  }

  if (daemonize) {
    setsid();
  }

  if (change_user (username) < 0) {
    fprintf (stderr, "fatal: cannot change user to %s\n", username ? username : "(none)");
    exit (1);
  }

  init_listening_connection (sfd, &ct_watchcat_engine_server, &memcache_methods);

  struct hostent *h;
  if (!(h = gethostbyname (hostname)) || h->h_addrtype != AF_INET || h->h_length != 4 || !h->h_addr_list || !h->h_addr) {
    fprintf (stderr, "fatal: cannot resolve hostname %s: %m\n", hostname);
    exit (1);
  }

  default_ct.target = *(struct in_addr *) h->h_addr;
  default_ct.port = queue_port;
  queue_conn = create_target (&default_ct, 0);

  create_all_outbound_connections();

  signal (SIGINT, sigint_handler);
  signal (SIGTERM, sigterm_handler);
  signal (SIGUSR1, sigusr1_handler);
  signal (SIGPIPE, SIG_IGN);
  signal (SIGPOLL, sigpoll_handler);
  if (daemonize) {
    signal (SIGHUP, sighup_handler);
    reopen_logs();
  }

  if (verbosity) {
    fprintf (stderr, "Server started\n");
  }

  for (i = 0; ; i++) {
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

    if (now != prev_time) {
      prev_time = now;
      cron();
    }

    if (epoll_pre_event) {
      epoll_pre_event();
    }

    if (quit_steps && !--quit_steps) break;
  }

  fprintf (stderr, "Quitting.\n");

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
    "\tForwards news search updates to queue\n",
    progname);

  parse_usage();
  exit (2);
}

int watchcat_parse_option (int val) {
  switch (val) {
    case 'm':
      max_memory = atoi (optarg);
      if (max_memory < 1) {
        max_memory = 1;
      }
      max_memory *= 1048576;
      break;
    case 'P':
      queue_port = atoi (optarg);
      break;
    case 'k':
      if (mlockall (MCL_CURRENT | MCL_FUTURE) != 0) {
        fprintf (stderr, "error: fail to lock paged memory\n");
      }
      break;
    case 'S':
      use_stemmer = 1;
      break;
    default:
      return -1;
  }
  return 0;
}

int main (int argc, char *argv[]) {
  dl_set_debug_handlers ();
  progname = argv[0];
  now = time (NULL);

  remove_parse_option ('a');
  remove_parse_option ('B');
  remove_parse_option ('r');
  remove_parse_option (204);
  parse_option ("memory-limit", required_argument, NULL, 'm', "<memory-limit> sets maximal size of used memory not including zmemory in mebibytes");
  parse_option ("queue-port", required_argument, 0, 'P', "<port> port number for communication with queue (default %d)", queue_port);
  parse_option ("lock-memory", no_argument, NULL, 'k', "lock paged memory");
  parse_option ("stemmer", no_argument, 0, 'S', "enable stemmer");

  parse_engine_options_long (argc, argv, watchcat_parse_option);
  if (argc != optind) {
    usage();
    return 2;
  }

  init_is_letter();
  enable_is_letter_sigils();
  if (use_stemmer) {
    stem_init();
  }

  dynamic_data_buffer_size = (1 << 20);
  init_dyn_data();

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

  init_all();
  start_time = time (NULL);

  start_server();

  free_all();
  return 0;
}

