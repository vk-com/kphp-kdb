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

#define _FILE_OFFSET_BITS 64

#include <assert.h>
#include <stdio.h>
#include <fcntl.h>
#include <sys/types.h>
#include <stdlib.h>
#include <string.h>
#include <signal.h>
#include <time.h>
#include <sys/mman.h>
#include <sys/wait.h>
#include <semaphore.h>

#include "kdb-data-common.h"
#include "net-events.h"
#include "net-msg.h"
#include "server-functions.h"
#include "net-udp.h"
#include "net-connections.h"
#include "net-memcache-server.h"
#include "net-crypto-aes.h"
#include "kfs.h"
#include "am-stats.h"

#include "dns-data.h"

#ifndef COMMIT
#define COMMIT "unknown"
#endif

#define	VERSION_STR	"dns-engine-1.00-r16"
const char FullVersionStr[] = VERSION_STR " compiled at " __DATE__ " " __TIME__ " by gcc " __VERSION__ " "
#ifdef __LP64__
  "64-bit"
#else
  "32-bit"
#endif
" after commit " COMMIT;

#define mytime() (get_utime(CLOCK_MONOTONIC))

static int workers, slave_mode, worker_id = -1;
static pid_t parent_pid, *pids;
static int keep_going = 0;
static char *config_name;
static struct in_addr settings_addr;
static char *output_binlog_name;
static double binlog_load_time;
static const long long jump_log_pos = 0;

/********************* MEMCACHED *********************/
conn_type_t ct_dns_engine_server = {
  .magic = CONN_FUNC_MAGIC,
  .title = "dns_engine_server",
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

static int stats_port;
static char stats_buff[16384];

/******************** stats ********************/

static worker_stats_t *WStats;
static sem_t semaphore;

#define	MAX_MSG_BYTES	16384
static char msg_in[MAX_MSG_BYTES+4], msg_out[MAX_MSG_BYTES+4];

/* dump packet for further usage in python script */
static void python_dump (const char *s, const char *e) {
  fprintf (stderr, "packet = \"");
  while (s != e) {
    if (*s >= 32 && *s < 128 && *s != '"' && *s != '\'' && *s != '\\') {
      fputc (*s, stderr);
    } else {
      fprintf (stderr, "\\x%02x", (int) ((unsigned char) *s));
    }
    s++;
  }
  fprintf (stderr, "\"\n");
}

/******************** TCP ********************/
int dns_init_accepted (struct connection *c);
int dns_close_connection (struct connection *c, int who);
int dns_parse_execute (struct connection *c);

conn_type_t ct_dns_server = {
  .magic = CONN_FUNC_MAGIC,
  .title = "dns_server",
  .accept = accept_new_connections,
  .init_accepted = dns_init_accepted,
  .run = server_read_write,
  .reader = server_reader,
  .writer = server_writer,
  .parse_execute = dns_parse_execute,
  .close = dns_close_connection,
  .free_buffers = free_connection_buffers,
  .init_outbound = server_failed,
  .connected = server_failed,
  //.wakeup = 0,
  //.alarm = 0
};

int dns_init_accepted (struct connection *c) {
  wstat.dns_tcp_connections++;
  c->last_query_time = now;
  return 0;
}

int dns_close_connection (struct connection *c, int who) {
  wstat.dns_tcp_connections--;
  return server_close_connection (c, who);
}

int dns_parse_execute (struct connection *c) {
  int len = nbit_total_ready_bytes (&c->Q);
  if (len < 2) {
    c->status = conn_expect_query;
    return 2 - len;
  }
  unsigned short query_size;
/* The message is prefixed with a two byte length field which gives the message
   length, excluding the two byte length field. This length field allows
   the low-level processing to assemble a complete message before beginning
   to parse it.
*/
  assert (nbit_read_in (&c->Q, &msg_in, 2) == 2);
  query_size = htons (*((unsigned short *) msg_in));
  vkprintf (4, "%s: TCP message size is %d bytes.\n", __func__, (int) query_size);
  if (len < 2 + (int) query_size) {
    c->status = conn_expect_query;
    return 2 + (int) query_size - len;
  }

  c->last_query_time = now;
  wstat.dns_tcp_queries++;

  if (query_size > MAX_MSG_BYTES) {
    wstat.dns_tcp_skipped_long_queries++;
    advance_skip_read_ptr (&c->In, (2 + (int) query_size));
    c->status = conn_write_close;
    put_event_into_heap (c->ev);
    return 0;
  }

  assert (nbit_read_in (&c->Q, msg_in + 2, query_size) == query_size);

  if (verbosity >= 3) {
    fprintf (stderr, "%d bytes read (TCP)\n", (int) query_size);
    hexdump (msg_in, msg_in + 2 + query_size);
    python_dump (msg_in, msg_in + 2 + query_size);
  }

  wstat.dns_tcp_query_bytes += query_size;
  dns_query_t q;
  int res = dns_query_parse (&q, (unsigned char *) msg_in + 2, query_size, 0);
  if (res < 0) {
    vkprintf (2, "dns_query_parse returns %d error code.\n", res);
    wstat.dns_tcp_bad_parse_queries++;
  } else {
    dns_response_t r;
    if (c->flags & C_IPV6) {
      dns_query_set_ip (&q, AF_INET6, c->remote_ipv6);
    } else {
      dns_query_set_ip (&q, AF_INET, &c->remote_ip);
    }
    res = dns_query_act (&q, &r, (unsigned char *) msg_out + 2, MAX_MSG_BYTES);
    if (res >= 0) {
      vkprintf (4, "dns_query_act returns %d.\n", res);
      //dns_truncated_responses += r.truncated;
      wstat.dns_tcp_response_bytes += res + 2;
      if (wstat.dns_tcp_max_response_bytes < res + 2) {
        wstat.dns_tcp_max_response_bytes = res + 2;
      }
      *((unsigned short *) msg_out) = htons ((unsigned short) res);
      assert (write_out (&c->Out, msg_out, res + 2) == res + 2);
      if (verbosity >= 3) {
        kprintf ("%d bytes sent (TCP)\n", res + 2);
        hexdump (msg_out, msg_out + res + 2);
      }
    } else {
      vkprintf (2, "dns_query_act returns %d error code.\n", res);
      wstat.dns_tcp_bad_act_queries++;
    }
  }

  advance_skip_read_ptr (&c->In, (2 + (int) query_size));
  return 0;
}

int dns_prepare_stats (struct connection *c);

int memcache_stats (struct connection *c) {
  int len = dns_prepare_stats (c);
  write_out (&c->Out, stats_buff, len);
  write_out (&c->Out, "END\r\n", 5);
  return 0;
}

int memcache_get (struct connection *c, const char *key, int key_len) {
  int dog_len = get_at_prefix_length (key, key_len);
  key += dog_len;
  key_len -= dog_len;
  if (key_len >= 5 && !memcmp (key, "stats", 5)) {
    int len = dns_prepare_stats (c);
    return return_one_key (c, key - dog_len, stats_buff, len);
  }
  return 0;
}


void update_stats (worker_stats_t *Sum, worker_stats_t *W) {
#define ADD(x) Sum->x += W->x
#define MAX(x) if (Sum->x < W->x) Sum->x = W->x
  ADD(dns_udp_queries);
  ADD(dns_udp_bad_act_queries);
  ADD(dns_udp_bad_parse_queries);
  ADD(dns_udp_skipped_long_queries);
  ADD(dns_udp_query_bytes);
  ADD(dns_udp_response_bytes);
  ADD(dns_tcp_queries);
  ADD(dns_tcp_bad_act_queries);
  ADD(dns_tcp_bad_parse_queries);
  ADD(dns_tcp_skipped_long_queries);
  ADD(dns_tcp_query_bytes);
  ADD(dns_tcp_response_bytes);
  ADD(dns_truncated_responses);
  MAX(dns_udp_max_response_bytes);
  MAX(dns_tcp_max_response_bytes);
  ADD(dns_tcp_connections);

  ADD(rcode_no_error_queries);
  ADD(rcode_format_queries);
  ADD(rcode_server_failure_queries);
  ADD(rcode_name_error_queries);
  ADD(rcode_not_implemented_queries);
  ADD(rcode_refused_queries);
  ADD(refused_by_remote_ip_queries);

  ADD(workers_average_idle_percent);
  ADD(workers_recent_idle_percent);
  MAX(workers_max_idle_percent);
  MAX(workers_max_recent_idle_percent);

#undef ADD
#undef MAX
}

#define W_PRINT_I64(x) sb_printf (&sb, "%s\t%lld\n", #x, W->x)
#define W_PRINT_I32(x) sb_printf (&sb, "%s\t%d\n", #x, W->x)
#define W_PRINT_D(x) sb_printf (&sb, "%s\t%.3lf\n", #x, W->x)
#define W_PRINT_QUERIES(x) sb_print_queries (&sb, #x, W->x)

static void update_idle_stats (void) {
  const int uptime = now - start_time;
  wstat.workers_max_idle_percent = wstat.workers_average_idle_percent = uptime > 0 ? tot_idle_time / uptime * 100 : 0,
  wstat.workers_max_recent_idle_percent = wstat.workers_recent_idle_percent = a_idle_quotient > 0 ? a_idle_time / a_idle_quotient * 100 : a_idle_time;
}

int dns_prepare_stats (struct connection *c) {
  stats_buffer_t sb;
  sb_prepare (&sb, c, stats_buff, sizeof (stats_buff));
  sb_memory (&sb, AM_GET_MEMORY_USAGE_SELF);
  worker_stats_t *W, T;

  if (workers) {
    int i;
    W = &T;
    update_idle_stats ();
    memcpy (W, &wstat, sizeof (worker_stats_t));
    sem_wait (&semaphore);
    for (i = 0; i < workers; i++) {
      update_stats (W, WStats + i);
    }
    sem_post (&semaphore);
    W->workers_average_idle_percent /= workers + 1;
    W->workers_recent_idle_percent /= workers + 1;
    W_PRINT_D(workers_average_idle_percent);
    W_PRINT_D(workers_max_idle_percent);
    W_PRINT_D(workers_recent_idle_percent);
    W_PRINT_D(workers_max_recent_idle_percent);
  } else {
    W = &wstat;
  }

  sb_printf (&sb, "config\t%.255s\n", config_name);

  if (include_binlog_name) {
    char buf[512];
    SB_BINLOG;
    if (!dns_binlog_allow_query_networks_dump (buf, sizeof (buf))) {
      sb_printf (&sb, "binlog_allow_query_networks\t%s\n", buf);
    }
  }

  W_PRINT_I32(dns_tcp_connections);
  W_PRINT_QUERIES(dns_udp_queries);
  long long dns_udp_bad_queries = W->dns_udp_bad_act_queries + W->dns_udp_bad_parse_queries;
  SB_PRINT_QUERIES(dns_udp_bad_queries);
  W_PRINT_QUERIES(dns_udp_skipped_long_queries);
  W_PRINT_QUERIES(dns_udp_bad_parse_queries);
  W_PRINT_QUERIES(dns_udp_bad_act_queries);
  W_PRINT_QUERIES(dns_tcp_queries);
  long long dns_tcp_bad_queries = W->dns_tcp_bad_act_queries + W->dns_tcp_bad_parse_queries;
  SB_PRINT_QUERIES(dns_tcp_bad_queries);
  W_PRINT_QUERIES(dns_tcp_skipped_long_queries);
  W_PRINT_QUERIES(dns_tcp_bad_parse_queries);
  W_PRINT_QUERIES(dns_tcp_bad_act_queries);

  W_PRINT_QUERIES(rcode_no_error_queries);
  W_PRINT_QUERIES(rcode_format_queries);
  W_PRINT_QUERIES(rcode_server_failure_queries);
  W_PRINT_QUERIES(rcode_name_error_queries);
  W_PRINT_QUERIES(rcode_not_implemented_queries);
  W_PRINT_QUERIES(rcode_refused_queries);
  W_PRINT_QUERIES(refused_by_remote_ip_queries);

  W_PRINT_I64(dns_udp_query_bytes);
  W_PRINT_I32(dns_udp_max_response_bytes);
  W_PRINT_I64(dns_udp_response_bytes);
  W_PRINT_I64(dns_truncated_responses);
  W_PRINT_I64(dns_tcp_query_bytes);
  W_PRINT_I32(dns_tcp_max_response_bytes);
  W_PRINT_I64(dns_tcp_response_bytes);

  const int labels_bytes = labels_wptr;
  const int records_bytes = records_wptr;
  SB_PRINT_I32(labels_bytes);
  SB_PRINT_I32(records_bytes);
  SB_PRINT_I32(labels_saved_bytes);
  SB_PRINT_I32(records_saved_bytes);
  SB_PRINT_I32(tot_records);
  SB_PRINT_I32(trie_nodes);
  SB_PRINT_I32(trie_edges);
  dns_stat_t static_limits;
  dns_stats (&static_limits);
  SB_PRINT_PERCENT(static_limits.percent_label_buff);
  SB_PRINT_PERCENT(static_limits.percent_record_buff);
  SB_PRINT_PERCENT(static_limits.percent_nodes);
  SB_PRINT_PERCENT(static_limits.percent_edges);

  const int reload_uptime = now - reload_time;
  SB_PRINT_I32(reload_uptime);
  SB_PRINT_I32(config_zones);
  SB_PRINT_I32(zones);

  /* command line settings */
  SB_PRINT_I32(dns_max_response_records);
  SB_PRINT_I32(edns_response_bufsize);
  SB_PRINT_I32(workers);
  sb_printf (&sb, "version\t%s\n", FullVersionStr);
  return sb.pos;
}

#undef W_PRINT_I64
#undef W_PRINT_I32
#undef W_PRINT_QUERIES

/******************** UDP ********************/

static int dns_process_msg (struct udp_socket *u, struct udp_message *msg);

struct udp_functions ut_dns_server = {
  .magic = UDP_FUNC_MAGIC,
  .title = "dns udp server",
  .process_msg = dns_process_msg
};

static int dns_process_msg (struct udp_socket *u, struct udp_message *msg) {
  vkprintf (2, "%s: processing udp message from [%s]:%d (%d bytes)\n", __func__, show_ipv6 (msg->ipv6), msg->port, msg->raw.total_bytes);
  int r = rwm_fetch_data (&msg->raw, msg_in, MAX_MSG_BYTES+4);

  if (verbosity >= 3) {
    fprintf (stderr, "%d bytes read (UDP)\n", r);
    hexdump (msg_in, msg_in + r);
    python_dump (msg_in, msg_in + r);
  }

  if (r > MAX_MSG_BYTES) {
    wstat.dns_udp_skipped_long_queries++;
    vkprintf (1, "message too long, skipping\n");
    return 0;
  }

  wstat.dns_udp_queries++;
  wstat.dns_udp_query_bytes += r;
  dns_query_t q;
  int res = dns_query_parse (&q, (unsigned char *) msg_in, r, 1);
  if (res < 0) {
    vkprintf (2, "dns_query_parse returns %d error code.\n", res);
    wstat.dns_udp_bad_parse_queries++;
  } else {
    dns_response_t r;
    if (is_4in6 (msg->ipv6)) {
      int ip = ntohl (extract_4in6 (msg->ipv6));
      dns_query_set_ip (&q, AF_INET, &ip);
    } else {
      dns_query_set_ip (&q, AF_INET6, msg->ipv6);
    }
    res = dns_query_act (&q, &r, (unsigned char *) msg_out, MAX_MSG_BYTES);
    if (res >= 0) {
      vkprintf (4, "dns_query_act returns %d.\n", res);
      wstat.dns_truncated_responses += r.truncated;
      wstat.dns_udp_response_bytes += res;
      if (wstat.dns_udp_max_response_bytes < res) {
        wstat.dns_udp_max_response_bytes = res;
      }
      struct udp_message *a = malloc (sizeof (struct udp_message));
      assert (rwm_create (&a->raw, msg_out, res) == res);
      a->next = 0;
      memcpy (a->ipv6, msg->ipv6, 16);
      a->port = msg->port;
      a->our_ip_idx = msg->our_ip_idx;
      udp_queue_message (u, a);
      if (verbosity >= 3) {
        kprintf ("%d bytes sent (UDP)\n", res);
        hexdump (msg_out, msg_out + res);
      }
    } else {
      vkprintf (2, "dns_query_act returns %d error code.\n", res);
      wstat.dns_udp_bad_act_queries++;
    }
  }
  return 0;
}

/******************** SIGNALS ********************/
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
  vkprintf (1, "logs reopened.\n");
}

static void sigint_immediate_handler (const int sig) {
  static const char message[] = "SIGINT handled immediately.\n";
  kwrite (2, message, sizeof (message) - (size_t)1);
  exit (1);
}

static void sigterm_immediate_handler (const int sig) {
  static const char message[] = "SIGTERM handled immediately.\n";
  kwrite (2, message, sizeof (message) - (size_t) 1);
  exit (1);
}

static void sigint_handler (const int sig) {
  static const char message[] = "SIGINT handled.\n";
  kwrite (2, message, sizeof (message) - (size_t) 1);
  pending_signals |= 1 << SIGINT;
  signal (sig, sigint_immediate_handler);
}

static void sigterm_handler (const int sig) {
  static const char message[] = "SIGTERM handled.\n";
  kwrite (2, message, sizeof (message) - (size_t) 1);
  pending_signals |= 1 << SIGTERM;
  signal (sig, sigterm_immediate_handler);
}

volatile int sigusr1_cnt = 0;
static void sigusr1_handler (const int sig) {
  static const char message[] = "got SIGUSR1.\n";
  kwrite (2, message, sizeof (message) - (size_t) 1);
  sigusr1_cnt++;
}

volatile int sigusr2_cnt = 0;
static void sigusr2_handler (const int sig) {
  static const char message[] = "got SIGUSR2.\n";
  kwrite (2, message, sizeof (message) - (size_t) 1);
  sigusr2_cnt++;
}

static int tcp_sfd = -1, udp_sfd = -1;

static void open_server_sockets (void) {
  char buf2[256];
  if (udp_sfd < 0) {
    udp_sfd = server_socket (port, settings_addr, backlog, SM_UDP + enable_ipv6);
    if (udp_sfd < 0) {
      kprintf ("cannot open UDP server socket at port %d: %m\n", port);
      exit (1);
    }
    if (verbosity) {
      printf ("created listening udp socket at %s:%d, fd=%d\n", conv_addr (settings_addr.s_addr, buf2), port, udp_sfd);
    }
  }
  if (tcp_sfd < 0) {
    tcp_sfd = server_socket (port, settings_addr, backlog, enable_ipv6);
    if (tcp_sfd < 0) {
      kprintf ("cannot open TCP server socket at port %d: %m\n", port);
      exit (1);
    }
  }
}

static void check_children_dead (void) {
  int i, j;
  for (j = 0; j < 11; j++) {
    for (i = 0; i < workers; i++) {
      if (pids[i]) {
        int status = 0;
        int res = waitpid (pids[i], &status, WNOHANG);
        if (res == pids[i]) {
          if (WIFEXITED (status) || WIFSIGNALED (status)) {
            pids[i] = 0;
          } else {
            break;
          }
        } else if (res == 0) {
          break;
        } else if (res != -1 || errno != EINTR) {
          pids[i] = 0;
        } else {
          break;
        }
      }
    }
    if (i == workers) {
      break;
    }
    if (j < 10) {
      usleep (100000);
    }
  }
  if (j == 11) {
    int cnt = 0;
    for (i = 0; i < workers; i++) {
      if (pids[i]) {
        ++cnt;
        kill (pids[i], SIGKILL);
      }
    }
    kprintf ("WARNING: %d children unfinished --> they are now killed\n", cnt);
  }
}

static void kill_children (int signal) {
  int i;
  assert (workers);
  for (i = 0; i < workers; i++) {
    if (pids[i]) {
      kill (pids[i], signal);
    }
  }
}

static void check_children_status (void) {
  if (workers) {
    int i;
    for (i = 0; i < workers; i++) {
      int status = 0;
      int res = waitpid (pids[i], &status, WNOHANG);
      if (res == pids[i]) {
        if (WIFEXITED (status) || WIFSIGNALED (status)) {
          kprintf ("Child %d terminated, aborting\n", pids[i]);
          pids[i] = 0;
          kill_children (SIGTERM);
          check_children_dead ();
          exit (EXIT_FAILURE);
        }
      } else if (res == 0) {
      } else if (res != -1 || errno != EINTR) {
        kprintf ("Child %d: unknown result during wait (%d, %m), aborting\n", pids[i], res);
        pids[i] = 0;
        kill_children (SIGTERM);
        check_children_dead ();
        exit (EXIT_FAILURE);
      }
    }
  } else if (slave_mode) {
    if (getppid () != parent_pid) {
      kprintf ("Parent %d is changed to %d, aborting\n", parent_pid, getppid ());
      exit (EXIT_FAILURE);
    }
  }
}

static void fd_close (int fd) {
  if (fd >= 0) {
    epoll_close (fd);
    close (fd);
  }
}

static void cron (void) {
  static int cur_conn_idx = 0;
  int seek_steps = 100, first_conn_idx = cur_conn_idx;
  int t = now - 120;
  while (seek_steps > 0) {
    struct connection *c = Connections + cur_conn_idx;
    if (c->type == &ct_dns_server && c->last_query_time < t && c->status == conn_expect_query) {
      vkprintf (3, "Closing idle TCP connection %d.\n", c->fd);
      c->status = conn_write_close;
      put_event_into_heap (c->ev);
    }
    if (++cur_conn_idx > max_connection) {
      cur_conn_idx = 0;
    }
    if (cur_conn_idx == first_conn_idx) {
      break;
    }
    seek_steps--;
  }

  if (worker_id >= 0) {
    update_idle_stats ();
    if (!sem_wait (&semaphore)) {
      memcpy (WStats + worker_id, &wstat, sizeof (worker_stats_t));
      sem_post (&semaphore);
    }
  }
  check_children_status ();
}

static void signals_init (void) {
  set_debug_handlers ();
  struct sigaction sa;
  memset (&sa, 0, sizeof (sa));
  sa.sa_handler = sigint_handler;
  sigemptyset (&sa.sa_mask);
  sigaddset (&sa.sa_mask, SIGTERM);
  sigaction (SIGINT, &sa, NULL);

  sa.sa_handler = sigterm_handler;
  sigemptyset (&sa.sa_mask);
  sigaddset (&sa.sa_mask, SIGINT);
  sigaction (SIGTERM, &sa, NULL);

  sa.sa_handler = SIG_IGN;
  sigaction (SIGPIPE, &sa, NULL);
  sigaction (SIGPOLL, &sa, NULL);
  sigaction (SIGUSR1, &sa, NULL);

  sa.sa_handler = sigusr1_handler;
  sigemptyset (&sa.sa_mask);
  sigaction (SIGUSR1, &sa, NULL);

  sa.sa_handler = sigusr2_handler;
  sigemptyset (&sa.sa_mask);
  sigaction (SIGUSR2, &sa, NULL);

}

void start_server (void) {
  int last_cron_time = 0;
  open_server_sockets ();
  if (workers > 0) {
    int i;
    if (sem_init (&semaphore, 1, 1) < 0) {
      kprintf ("Fail to init semaphore. %m\n");
      exit (1);
    }
    pids = malloc (sizeof (pids[0]) * workers);
    WStats = mmap (0, workers * sizeof (worker_stats_t), PROT_READ | PROT_WRITE, MAP_SHARED | MAP_ANONYMOUS, -1, 0);
    assert (WStats != MAP_FAILED);
    kprintf_multiprocessing_mode_enable ();
    vkprintf (0, "creating %d workers\n", workers);
    for (i = 0; i < workers; i++) {
      int pid = fork ();
      assert (pid >= 0);
      if (!pid) {
        worker_id = i;
        workers = 0;
        slave_mode = 1;
        parent_pid = getppid ();
        assert (parent_pid > 1);
        pids = NULL;
        break;
      } else {
        pids[i] = pid;
      }
    }
  }

  init_epoll ();
  init_netbuffers ();


  init_udp_port (udp_sfd, port, &ut_dns_server, 0, enable_ipv6);
  init_listening_tcpv6_connection (tcp_sfd, &ct_dns_server, NULL, enable_ipv6);

  if (binlog_disabled && binlog_fd >= 0) {
    epoll_pre_event = read_new_events;
  }


  int stats_sfd = -1;
  if (stats_port && !slave_mode) {
    static struct in_addr settings_addr;
    stats_sfd = server_socket (stats_port, settings_addr, backlog, 0);
    if (stats_sfd < 0) {
      kprintf ("cannot open server socket at port %d: %m\n", stats_port);
      exit (1);
    }
    init_listening_connection (stats_sfd, &ct_dns_engine_server, &memcache_methods);
  }

  int i;
  for (i = 0; ; i++) {
    if (pending_signals & ((1 << SIGINT) | (1 << SIGTERM))) {
      break;
    }
    if (!(i & 1023)) {
      vkprintf (1, "epoll_work()\n");
    }

    if (__sync_fetch_and_and (&sigusr1_cnt, 0)) {
      reopen_logs ();
      if (workers) {
        kill_children (SIGUSR1);
      }
    }

    if (__sync_fetch_and_and (&sigusr2_cnt, 0)) {
      if (include_binlog_name == NULL) {
        dns_reset ();
        if (dns_config_load (config_name, !keep_going, NULL) < 0) {
          exit (1);
        }
      } else {
        kprintf ("Skip reloading since config contains $BINLOG macro.\n");
      }
      if (workers) {
        kill_children (SIGUSR2);
      }
    }

    epoll_work (37);
    if (last_cron_time != now) {
      last_cron_time = now;
      cron ();
    }

    if (epoll_pre_event) {
      epoll_pre_event ();
    }

    if (quit_steps && !--quit_steps) break;
  }

  if (workers) {
    if (pending_signals & (1 << SIGTERM)) {
      kill_children (SIGTERM);
    } else if (pending_signals & (1 << SIGINT)) {
      kill_children (SIGINT);
    }
    check_children_dead ();
  }

  fd_close (udp_sfd);
  fd_close (tcp_sfd);
  if (stats_port) {
    fd_close (stats_sfd);
  }
  if (pending_signals & (1 << SIGTERM)) {
    kprintf ("Terminated by SIGTERM.\n");
  } else if (pending_signals & (1 << SIGINT)) {
    kprintf ("Terminated by SIGINT.\n");
  }
}

/*
 *
 *		MAIN
 *
 */

void usage (void) {
  fprintf (stderr, "usage: %s [-u<username>] [-v] [-p<listen-port>] [-P<stats-port>] [-d] [-6] [-l<log-name>] <config>\n%s\n"
    "DNS server\n"
    , progname, FullVersionStr);
  parse_usage ();
  exit (2);
}

int f_parse_option (int val) {
  int c;
  switch (val) {
  case 'E':
    output_binlog_name = optarg;
    break;
  case 'M':
    workers = atoi (optarg);
    if (workers < 0) {
      workers = 0;
    }
    if (workers > 16) {
      kprintf ("Too many additional workers\n");
      exit (1);
    }
    break;
  case 'P':
    stats_port = atoi (optarg);
    break;
  case 'R':
    c = atoi (optarg);
    if (c >= 1 && c <= DNS_MAX_RESPONSE_RECORDS) {
      dns_max_response_records = c;
    }
    break;
  case 'k':
    keep_going = 1;
    break;
  default:
    fprintf (stderr, "Unimplemented option '%c' (%d)\n", (char) val, val);
    return -1;
  }
  return 0;
}

int main (int argc, char *argv[]) {
  signals_init ();
  progname = argv[0];

  remove_parse_option ('r');
  remove_parse_option ('B');
  remove_parse_option (203);
  parse_option ("workers", required_argument, 0, 'M', "sets number of additional worker processes");
  parse_option ("max-response-records", required_argument, 0, 'R', "limits max records number in the response");
  parse_option ("stats-port", required_argument, 0, 'P', "sets port for getting stats using memcache queries");
  parse_option ("output", required_argument, 0, 'E', "sets exported binlog name for converting config to binlog");
  parse_option ("keep-going", no_argument, 0, 'k', "reports as much config errors as possible");
  parse_engine_options_long (argc, argv, f_parse_option);

  if (output_binlog_name) {
    port = stats_port = 0;
  }
  if (!port && output_binlog_name == NULL) {
    usage ();
  }
  config_name = argv[optind];

  if (port > 0 && port < PRIVILEGED_TCP_PORTS) {
    open_server_sockets ();
  }

  if (raise_file_rlimit (maxconn + 16) < 0) {
    kprintf ("fatal: cannot raise open file limit to %d\n", maxconn + 16);
    exit (1);
  }

  if (change_user (username) < 0) {
    kprintf ("fatal: cannot change user to %s\n", username ? username : "(none)");
    exit (1);
  }

  srand48 (time (0));
  dynamic_data_buffer_size = 64 << 20;
  init_dyn_data ();
  dns_reset ();
  //TODO: replace config by binlog
  if (dns_config_load (config_name, !keep_going, output_binlog_name) < 0) {
    exit (1);
  }
  if (output_binlog_name) {
    exit (0);
  }
  if (include_binlog_name) {
    binlog_load_time = -mytime ();
    binlog_disabled = 1;
    if (engine_preload_filelist (include_binlog_name, NULL) < 0) {
      kprintf ("cannot open binlog files for %s\n", include_binlog_name);
      exit (1);
    }
    Binlog = open_binlog (engine_replica, jump_log_pos);
    if (!Binlog) {
      kprintf ("fatal: cannot find binlog for %s, log position %lld\n", engine_replica->replica_prefix, jump_log_pos);
      exit (1);
    }
    binlogname = Binlog->info->filename;
    clear_log ();
    init_log_data (jump_log_pos, 0, 0);
    vkprintf (1, "replay log events started\n");
    if (replay_log (0, 1) < 0) {
      exit (1);
    }
    vkprintf (1, "replay log events finished\n");
    binlog_load_time += mytime ();
    if (!binlog_disabled) {
      clear_read_log ();
    }
    clear_write_log ();
  }
  init_msg_buffers (0);
  start_time = time (NULL);
  start_server ();
  return 0;
}
