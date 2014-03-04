/*
    This file is part of VK/KittenPHP-DB-Engine Library.

    VK/KittenPHP-DB-Engine Library is free software: you can redistribute it and/or modify
    it under the terms of the GNU Lesser General Public License as published by
    the Free Software Foundation, either version 2 of the License, or
    (at your option) any later version.

    VK/KittenPHP-DB-Engine Library is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Lesser General Public License for more details.

    You should have received a copy of the GNU Lesser General Public License
    along with VK/KittenPHP-DB-Engine Library.  If not, see <http://www.gnu.org/licenses/>.

    Copyright 2013 Vkontakte Ltd
              2013 Vitaliy Valtman              
              2013 Anton Maydell
*/

#ifndef __AM_SERVER_FUNCTIONS_H__
#define __AM_SERVER_FUNCTIONS_H__

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <signal.h>
#include <assert.h>
#include <sys/wait.h>
#include <sys/resource.h>
#include "kfs.h"
#include "kdb-data-common.h"
#include "kdb-binlog-common.h"
#include "net-crypto-aes.h"
#include "am-server-functions.h"
#include "server-functions.h"
#include "vv-tl-parse.h"

static volatile int sighup_cnt = 0, sigusr1_cnt = 0, sigrtmax_cnt = 0;
static int child_pid = 0, last_cron_time = 0;
int udp_enabled;


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

static void default_sighup (void) {
  sync_binlog (2);
}

static void default_sigusr1 (void) {
  sync_binlog (2);
  reopen_logs ();
}

void default_cron (void) {
  create_all_outbound_connections ();
  flush_binlog ();
  dyn_garbage_collector ();
}

static server_functions_t sf = {
  .sighup = default_sighup,
  .sigusr1 = default_sigusr1,
  .cron = default_cron,
  .save_index = NULL
};


static void check_child_status (void) {
  if (!child_pid) {
    return;
  }
  int status = 0;
  int res = waitpid (child_pid, &status, WNOHANG);
  if (res == child_pid) {
    if (WIFEXITED (status) || WIFSIGNALED (status)) {
      vkprintf (1, "child process %d terminated: exited = %d, signaled = %d, exit code = %d\n",
        child_pid, WIFEXITED (status) ? 1 : 0, WIFSIGNALED (status) ? 1 : 0, WEXITSTATUS (status));
      child_pid = 0;
    }
  } else if (res == -1) {
    if (errno != EINTR) {
      kprintf ("waitpid (%d): %m\n", child_pid);
      child_pid = 0;
    }
  } else if (res) {
    kprintf ("waitpid (%d) returned %d???\n", child_pid, res);
  }
}

static void fork_write_index (void) {
  if (sf.save_index == NULL) {
    vkprintf (1, "%s: save_index isn't defined.\n", __func__);
    return;  
  }
  if (child_pid) {
    vkprintf (1, "%s: process with pid %d already generates index, skipping\n", __func__, child_pid);
    return;
  }
  flush_binlog_ts ();
  int res = fork ();
  if (res < 0) {
    kprintf ("fork: %m\n");
  } else if (!res) {
    binlogname = 0;
    res = sf.save_index (!binlog_disabled);
    exit (res);
  } else {
    vkprintf (1, "created child process pid = %d\n", res);
    child_pid = res;
  }
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


static void sighup_handler (const int sig) {
  static const char message[] = "got SIGHUP.\n";
  kwrite (2, message, sizeof (message) - (size_t) 1);
  sighup_cnt++;
}

static void sigusr1_handler (const int sig) {
  static const char message[] = "got SIGUSR1.\n";
  kwrite (2, message, sizeof (message) - (size_t) 1);
  sigusr1_cnt++;
}

static void sigrtmax_handler (const int sig) {
  static const char message[] = "got SIGRTMAX.\n";
  kwrite (2, message, sizeof (message) - (size_t) 1);
  sigrtmax_cnt++;
}

int process_signals (void) {
  if (pending_signals & ((1 << SIGINT) | (1 << SIGTERM))) {
    return 0;
  }
  if (last_cron_time != now) {
    last_cron_time = now;
    check_child_status ();
    if (__sync_fetch_and_and (&sighup_cnt, 0)) {
      sf.sighup ();
    }
    if (__sync_fetch_and_and (&sigusr1_cnt, 0)) {
      sf.sigusr1 ();
    }
    if (__sync_fetch_and_and (&sigrtmax_cnt, 0)) {
      fork_write_index ();
    }
    sf.cron ();
  }
  if (epoll_pre_event) {
    epoll_pre_event ();
  }
  return 1;
}

void engine_init (engine_t *E, const char *const pwd_filename, int index_mode) {
  E->sfd = 0;
  struct sigaction sa;
  memset (&sa, 0, sizeof (sa));
  sa.sa_handler = sigusr1_handler;
  sigemptyset (&sa.sa_mask);
  sigaction (SIGUSR1, &sa, NULL);
  sa.sa_handler = sigrtmax_handler;
  sigaction (SIGRTMAX, &sa, NULL);

  if (!index_mode && port < PRIVILEGED_TCP_PORTS) {
    E->sfd = server_socket (port, E->settings_addr, backlog, 0);
    if (E->sfd < 0) {
      kprintf ("cannot open server socket at port %d: %m\n", port);
      exit (1);
    }
  }

  const int gap = 16;
  if (getuid ()) {
    struct rlimit rlim;
    if (getrlimit (RLIMIT_NOFILE, &rlim) < 0) {
      kprintf ("%s: getrlimit (RLIMIT_NOFILE) fail. %m\n", __func__);
      exit (1);
    }
    if (maxconn > rlim.rlim_cur - gap) {
      maxconn = rlim.rlim_cur - gap;
    }
  } else {
    if (raise_file_rlimit (maxconn + gap) < 0) {
      kprintf ("fatal: cannot raise open file limit to %d\n", maxconn + gap);
      exit (1);
    }
  }

  aes_load_pwd_file (pwd_filename);
  
  if (change_user (username) < 0) {
    kprintf ("fatal: cannot change user to %s\n", username ? username : "(none)");
    exit (1);
  }

  init_dyn_data ();
  if (udp_enabled) {
    init_server_PID (get_my_ipv4 (), port);
  }
}

void server_init (engine_t *E, server_functions_t *F, conn_type_t *listen_connection_type, void *listen_connection_extra) {
  if (F != NULL) {
    if (F->sighup) {
      sf.sighup = F->sighup;
    }
    if (F->sigusr1) {
      sf.sigusr1 = F->sigusr1;
    }
    if (F->save_index) {
      sf.save_index = F->save_index;
    }
    if (F->cron) {
      sf.cron = F->cron;
    }
  }
  
  init_epoll ();
  init_netbuffers ();
  if (udp_enabled) {
    init_msg_buffers (0);
  }
  
  if (daemonize) {
    setsid ();
    reopen_logs ();
  }

  if (!E->sfd) {
    E->sfd = server_socket (port, E->settings_addr, backlog, 0);
  }

  if (E->sfd < 0) {
    kprintf ("cannot open server socket at port %d: %m\n", port);
    exit (1);
  }

  if (change_user (username) < 0) {
    kprintf ("fatal: cannot change user to %s\n", username ? username : "(none)");
    exit (1);
  }

  if (binlogname && !binlog_disabled) {
    assert (append_to_binlog (Binlog) == log_readto_pos);
  }

  init_listening_connection (E->sfd, listen_connection_type, listen_connection_extra);
  if (udp_enabled) {
    add_udp_socket (port, 0);
  }
  
  if (binlog_disabled && binlog_fd >= 0) {
    epoll_pre_event = read_new_events;
  }

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
  
  if (daemonize) {
    sa.sa_handler = sighup_handler;
    sigemptyset (&sa.sa_mask);
    sigaction (SIGHUP, &sa, NULL);
  }
}

void server_exit (engine_t *E) {
  epoll_close (E->sfd);
  close (E->sfd);
  flush_binlog_last ();
  sync_binlog (2);
  if (pending_signals & (1 << SIGTERM)) {
    kprintf ("Terminated by SIGTERM.\n");
  } else if (pending_signals & (1 << SIGINT)) {
    kprintf ("Terminated by SIGINT.\n");
  }
}

#endif
