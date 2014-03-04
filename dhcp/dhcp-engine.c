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
#define _XOPEN_SOURCE 500

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <ctype.h>
#include <string.h>
#include <math.h>
#include <alloca.h>
#include <fcntl.h>
#include <sys/types.h>
#include <stddef.h>
#include <signal.h>
#include <time.h>
#include <sys/wait.h>
#include <netdb.h>

#include "kdb-data-common.h"
#include "net-events.h"
#include "net-msg.h"
#include "server-functions.h"
#include "net-udp.h"
#include "net-connections.h"
#include "net-memcache-server.h"
#include "net-crypto-aes.h"
#include "kfs.h"
#include "dhcp-data.h"

#ifndef COMMIT
#define COMMIT "unknown"
#endif

#define DHCP_PORT 67

#define	VERSION_STR	"dhcp-egnine-1.00"
const char FullVersionStr[] = VERSION_STR " compiled at " __DATE__ " " __TIME__ " by gcc " __VERSION__ " "
#ifdef __LP64__
  "64-bit"
#else
  "32-bit"
#endif
" after commit " COMMIT;

#define mytime() (get_utime(CLOCK_MONOTONIC))

#define	MAX_MSG_BYTES 16384

#define DEBUG_UDPDUMP 1
#define DEBUG_FILEDUMP 2

static int debug_mode;

static char msg_in[MAX_MSG_BYTES+4], msg_out[MAX_MSG_BYTES+4];
static int dhcp_debug_process_msg (struct udp_socket *u, struct udp_message *msg);
static int dhcp_process_msg (struct udp_socket *u, struct udp_message *msg);
struct udp_functions ut_dhcp_server = {
  .magic = UDP_FUNC_MAGIC,
  .title = "DHCP server",
  .process_msg = dhcp_process_msg
};

struct udp_functions ut_debug_dhcp_server = {
  .magic = UDP_FUNC_MAGIC,
  .title = "Debug DHCP server",
  .process_msg = dhcp_debug_process_msg
};

static int file_dump (const char *filename) {
  FILE *f = fopen (filename, "r");
  if (f == NULL) {
    kprintf ("%s: fopen (\"%s\", \"r\") failed. %m\n", __func__, filename);
    return -1;
  }
  int r = fread (msg_in, 1, sizeof (msg_in), f);
  if (r < 0) {
    fclose (f);
    return -1;
  }
  fclose (f);
  dhcp_message_t m;
  if (dhcp_message_parse (&m, (unsigned char *) msg_in, r) < 0) {
    kprintf ("fail to parse %d bytes packet\n", r);
    return -1;
  }
  dhcp_message_print (&m, stderr);
  return 0;
}

/******************** UDP ********************/
int dhcp_debug_process_msg (struct udp_socket *u, struct udp_message *msg) {
  kprintf ("%s: processing udp message from [%s]:%d (%d bytes)\n", __func__, show_ipv6 (msg->ipv6), msg->port, msg->raw.total_bytes);
  int r = rwm_fetch_data (&msg->raw, msg_in, MAX_MSG_BYTES+4);
  kprintf ("%d bytes read (UDP)\n", r);
  hexdump (msg_in, msg_in + r);
  if (r > MAX_MSG_BYTES) {
    vkprintf (1, "message too long, skipping\n");
    return 0;
  }
  dhcp_message_t m;
  if (dhcp_message_parse (&m, (unsigned char *) msg_in, r) < 0) {
    kprintf ("fail to parse %d bytes packet\n", r);
    return 0;
  }
  dhcp_message_print (&m, stderr);
  return 0;
}

int dhcp_process_msg (struct udp_socket *u, struct udp_message *msg) {
  vkprintf (2, "%s: processing udp message from [%s]:%d (%d bytes)\n", __func__, show_ipv6 (msg->ipv6), msg->port, msg->raw.total_bytes);
  int r = rwm_fetch_data (&msg->raw, msg_in, MAX_MSG_BYTES+4);
  if (verbosity >= 3) {
    kprintf ("%d bytes read (UDP)\n", r);
    hexdump (msg_in, msg_in + r);
  }
  if (r > MAX_MSG_BYTES) {
    vkprintf (1, "message too long, skipping\n");
    return 0;
  }

  dhcp_message_t m;
  if (dhcp_message_parse (&m, (unsigned char *) msg_in, r) < 0) {
    vkprintf (1, "fail to parse %d bytes packet\n", r);
    if (verbosity >= 1 && verbosity < 3) {
      hexdump (msg_in, msg_in + r);
    }
    return 0;
  }
  if (verbosity >= 2) {
    dhcp_message_print (&m, stderr);
  }
  //TODO: form answer
  msg_out[0] = 0;

  return 0;
}

static void fd_close (int *fd) {
  if (*fd >= 0) {
    epoll_close (*fd);
    assert (!close (*fd));
    *fd = -1;
  }
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
  sigaction (SIGUSR2, &sa, NULL);

  sa.sa_handler = sigusr1_handler;
  sigemptyset (&sa.sa_mask);
  sigaction (SIGUSR1, &sa, NULL);

}

static void cron (void) {
}

void usage (void) {
  fprintf (stderr, "usage: %s [-u<username>] [-v] [-d] [-6] [-l<log-name>] <config>\n%s\n"
    "DHCP server.\n"
    , progname, FullVersionStr);
  parse_usage ();
  exit (2);
}

static int udp_sfd = -1, udp_sfd2 = -1;

int open_udp_socket (int port) {
  static struct in_addr settings_addr;
  char buf2[256];
  int sfd = server_socket (port, settings_addr, backlog, SM_UDP + enable_ipv6);
  if (sfd < 0) {
    kprintf ("cannot open UDP server socket at port %d: %m\n", port);
    exit (1);
  }
  if (verbosity) {
    printf ("created listening udp socket at %s:%d, fd=%d\n", conv_addr (settings_addr.s_addr, buf2), port, sfd);
  }
  return sfd;
}

void open_server_sockets (void) {
  if (udp_sfd < 0) {
    udp_sfd = open_udp_socket (port);
  }
  if (debug_mode == DEBUG_UDPDUMP) {
    if (udp_sfd2 < 0) {
      udp_sfd2 = open_udp_socket (68);
    }
  }
}

void start_server (void) {
  int last_cron_time = 0;

  init_epoll ();
  init_netbuffers ();

  open_server_sockets ();
  if (debug_mode == DEBUG_UDPDUMP) {
    init_udp_port (udp_sfd, port, &ut_debug_dhcp_server, 0, enable_ipv6);
    init_udp_port (udp_sfd2, 68, &ut_debug_dhcp_server, 0, enable_ipv6);
  } else {
    init_udp_port (udp_sfd, port, &ut_dhcp_server, 0, enable_ipv6);
  }

  if (binlog_disabled && binlog_fd >= 0) {
    epoll_pre_event = read_new_events;
  }

  if (daemonize) {
    reopen_logs ();
    setsid ();
  }

/*
  int stats_sfd = -1;
  if (stats_port) {
    static struct in_addr settings_addr;
    stats_sfd = server_socket (stats_port, settings_addr, backlog, 0);
    if (stats_sfd < 0) {
      kprintf ("cannot open server socket at port %d: %m\n", stats_port);
      exit (1);
    }
    init_listening_connection (stats_sfd, &ct_dns_engine_server, &memcache_methods);
  }
*/

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

  fd_close (&udp_sfd);
/*
  if (stats_port) {
    fd_close (&stats_sfd);
  }
*/
  if (pending_signals & (1 << SIGTERM)) {
    kprintf ("Terminated by SIGTERM.\n");
  } else if (pending_signals & (1 << SIGINT)) {
    kprintf ("Terminated by SIGINT.\n");
  }
}

int f_parse_option (int val) {
  switch (val) {
  case 'F':
    assert (!debug_mode);
    debug_mode = DEBUG_FILEDUMP;
    break;
  case 'U':
    assert (!debug_mode);
    debug_mode = DEBUG_UDPDUMP;
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

  parse_option ("debug-udpdump", no_argument, 0, 'U', "listen 67/68 ports and dump received packets to the log");
  parse_option ("debug-filedump", no_argument, 0, 'F', "parse packet from file (filename is given in <config> parameter) and exit");

  parse_engine_options_long (argc, argv, f_parse_option);

  if (argc != optind + 1) {
    usage ();
  }

  if (debug_mode == DEBUG_FILEDUMP) {
    exit ((file_dump (argv[optind]) < 0) ? 1 : 0);
  }

  if (!port || debug_mode == DEBUG_UDPDUMP) {
    port = DHCP_PORT;
  }

  if (port < PRIVILEGED_TCP_PORTS) {
    open_server_sockets ();
  }

  if (raise_file_rlimit (maxconn + 16) < 0) {
    kprintf ("fatal: cannot raise open file limit to %d\n", maxconn + 16);
    exit (1);
  }

  aes_load_pwd_file (0); //srand48

  if (change_user (username) < 0) {
    kprintf ("fatal: cannot change user to %s\n", username ? username : "(none)");
    exit (1);
  }

  init_dyn_data ();
  init_msg_buffers (0);

  if (dhcp_config_load (argv[optind]) < 0) {
    kprintf ("fatal: fail to load config file '%s'.\n", argv[optind]);
    exit (1);
  }

  start_time = time (NULL);
  start_server ();
  return 0;
}
