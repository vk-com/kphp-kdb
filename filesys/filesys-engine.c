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

#define _FILE_OFFSET_BITS 64
#define FUSE_USE_VERSION 26

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <signal.h>
#include <unistd.h>
#include <pthread.h>

#include "filesys-mount.h"
#include "net-events.h"
#include "net-connections.h"
#include "net-memcache-server.h"
#include "kdb-data-common.h"
#include "server-functions.h"
#include "kfs.h"
#include "filesys-data.h"

#define TCP_PORT 11211
#define UDP_PORT 11211
int backlog = BACKLOG, port = TCP_PORT, udp_port = UDP_PORT, maxconn = MAX_CONNECTIONS, daemonize = 0;
struct in_addr settings_addr;


int jump_log_ts;
unsigned jump_log_crc32;
long long jump_log_pos;
long long binlog_loaded_size;
double binlog_load_time;
double index_load_time;

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

  vkprintf (1, "logs reopened.\n");
}

static void done (void) {
  flush_binlog_last();
  sync_binlog (2);
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

#define MAX_FUSE_PARAMS 32
char *fuse_argv[MAX_FUSE_PARAMS];
int fuse_argc;

static void fuse_argv_add (char *s) {
  char *p;
  for (p = strtok (s, "\t "); p != NULL; p = strtok (NULL, "\t ")) {
    assert (fuse_argc < MAX_FUSE_PARAMS);
    fuse_argv[fuse_argc++] = p;
  }
}

void help (void) {
  printf (VERSION_STR"\n"
          "[-i]\t\t run in index mode\n"
          "[-f \"<fuse options>\"]\tmount filesystem in userspace\n"
          "[-R]\tread only filesystem\n"
          "[-V <volume_id>]\tvolume id (int64)\n"
          "[-E]\tcreate empty binlog and exit\n"
          "[-p <port>]\tTCP port number to listen on (default: %d)\n"
          "[-U <port>]\tUDP port number to listen on (default: %d, 0 is off)\n"
          "[-d]\t\trun as a daemon\n"
          "[-u <username>]\tassume identity of <username> (only when run as root)\n"
          "[-c <max_conn>]\tmax simultaneous connections, default is %d\n"
          "[-v]\t\tverbose\n"
          "[-vv]\t\tvery verbose\n"
          "[-h]\t\tprint this help and exit\n"
          "[-b <backlog>]\n"
          "[-n <nice>]\n"
          "[-l <log_name>]\tlog... about something\n"
          "[-a <binlog_name>]\tbinlog\n"
          "[-m <max_memory>]\tmaximal memory in MiB\n",
          TCP_PORT,
          UDP_PORT,
          MAX_CONNECTIONS
         );
  exit (2);
}
int main (int argc, char *argv[]) {
  int binlog_readed = 0;
  int max_memory = 1 << 30;
  int i;
  int index_mode = 0;
  long long l;
  int save_empty_binlog = 0;
  set_debug_handlers ();
  fuse_argv[0] = progname = argv[0];
  fuse_argc = 1;
  while ((i = getopt (argc, argv, "a:b:c:f:l:p:U:n:dhu:vrim:M:RV:E")) != -1) {
    switch (i) {
    case 'E':
      save_empty_binlog = 1;
      break;
    case 'V':
      if (sscanf (optarg, "%lld", &l) >= 1 && l >= 0) {
        volume_id = l;
      }
      break;
    case 'R':
      ff_readonly = 1;
      break;
    case 'f':
      fuse_argv_add (optarg);
      break;
    case 'a':
      binlogname = optarg;
      break;
    case 'v':
      verbosity++;
      break;
    case 'h':
      help ();
      return 2;
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
    case 'n':
      errno = 0;
      nice (atoi (optarg));
      if (errno) {
        perror ("nice");
      }
      break;
    case 'p':
      port = atoi (optarg);
      break;
    case 'U':
      udp_port = atoi (optarg);
      break;
    case 'u':
      username = optarg;
      break;
    case 'l':
      logname = optarg;
      break;
    case 'd':
      daemonize ^= 1;
      break;
    case 'r':
      // nothing to do
      binlog_disabled = 1;
      break;
    case 'i':
      index_mode = 1;
      break;
    case 'm':
      max_memory = 1024 * 1024 * (long long) atoi (optarg);
      break;
    }
  }
  assert (fuse_argc < MAX_FUSE_PARAMS);
  fuse_argv[fuse_argc] = "";

  if (argc != optind + 1) {
    help();
    return 2;
  }

  if (save_empty_binlog) {
    printf ("Volume_id = %lld\n", volume_id);
    printf ("Save empty binlog to the file: %s\n", argv[optind]);

    if (volume_id < 0) {
      fprintf (stderr, "Volume id isn't set!\n");
      exit (1);
    }
    int fd;
    fd = open (argv[optind], O_CREAT | O_TRUNC | O_WRONLY | O_EXCL, 0660);
    if (fd != -1) {
      struct lev_start *a = malloc (32);
      a->type = 0x044c644b;
      a->schema_id = 0x723d0101;
      a->extra_bytes = 8;
      a->split_mod = 1;
      a->split_min = 0;
      a->split_max = 1;
      memcpy (&a->str, &volume_id, 8);
      assert (write (fd, a, 32) == 32);
      free (a);
      close (fd);
      printf ("Binlog successfully saved.\n");
      return 0;
    }
    return 1;
  }

  if (strlen (argv[0]) >= 5 && memcmp ( argv[0] + strlen (argv[0]) - 5, "index" , 5) == 0) {
    index_mode = 1;
  }

  vkprintf (3, "Command line parsed\n");

  if (!username && maxconn == MAX_CONNECTIONS && geteuid()) {
    maxconn = 1000; //not for root
  }

  if (dynamic_data_buffer_size > 0.2 * max_memory) {
    dynamic_data_buffer_size = 0.2 * max_memory;
  }
  set_memory_limit (0.8 * max_memory);

  if (change_user (username) < 0) {
    fprintf (stderr, "fatal: cannot change user to %s\n", username ? username : "(none)");
    exit(1);
  }

  init_dyn_data();

  ff_sfd = server_socket (port, settings_addr, backlog, 0);
  if (ff_sfd < 0) {
    fprintf (stderr, "cannot open server socket at port %d: %m\n", port);
    exit (1);
  }

  vkprintf (2, "starting reading binlog\n");

  int optind = argc;
  if (engine_preload_filelist (argv[optind-1], binlogname) < 0) {
    fprintf (stderr, "cannot open binlog files for %s\n", binlogname ? binlogname : argv[optind]);
    exit (1);
  }

  vkprintf (3, "engine_preload_filelist done\n");

  //Snapshot reading
  Snapshot = open_recent_snapshot (engine_snapshot_replica);

  if (Snapshot) {
    engine_snapshot_name = Snapshot->info->filename;
    engine_snapshot_size = Snapshot->info->file_size;
    vkprintf (1, "load index file %s (size %lld)\n", engine_snapshot_name, engine_snapshot_size);
  } else {
    engine_snapshot_name = NULL;
    engine_snapshot_size = 0;
  }

  index_load_time = -get_utime (CLOCK_MONOTONIC);

  i = load_index (Snapshot);

  index_load_time += get_utime (CLOCK_MONOTONIC);

  if (i < 0) {
    fprintf (stderr, "fatal: error %d while loading index file %s\n", i, engine_snapshot_name);
    exit (1);
  }

  vkprintf (1, "load index: done, jump_log_pos=%lld, time %.06lfs\n", jump_log_pos, index_load_time);

  //Binlog reading
  Binlog = open_binlog (engine_replica, jump_log_pos);
  if (!Binlog) {
    fprintf (stderr, "fatal: cannot find binlog for %s, log position %lld\n", engine_replica->replica_prefix, jump_log_pos);
    exit (1);
  }

  binlogname = Binlog->info->filename;

  vkprintf (1, "replaying binlog file %s (size %lld)\n", binlogname, Binlog->info->file_size);

  binlog_load_time = -get_utime (CLOCK_MONOTONIC);

  clear_log();

  init_log_data (jump_log_pos, jump_log_ts, jump_log_crc32);

  vkprintf (1, "replay log events started\n");

  i = replay_log (0, 1);

  vkprintf (1, "replay log events finished\n");

  binlog_load_time += get_utime (CLOCK_MONOTONIC);
  binlog_loaded_size = log_readto_pos - jump_log_pos;

  if (!binlog_disabled) {
    clear_read_log();
  }

  vkprintf (1, "replay binlog file: done, log_pos=%lld, time %.06lfs\n",
           (long long) log_pos, binlog_load_time);

  vkprintf (1, "binlog_disabled = %d\n", binlog_disabled);

  binlog_readed = 1;

  if (index_mode) {
    save_index (!binlog_disabled);
    return 0;
  }

  clear_write_log ();
  start_time = time (NULL);

  if (verbosity >= 3) {
    dump_all_files ();
  }

  if (daemonize) {
    setsid();
  }

  if (binlogname && !binlog_disabled) {
    assert (append_to_binlog (Binlog) == log_readto_pos);
  }

  signal (SIGINT, sigint_handler);
  signal (SIGTERM, sigterm_handler);
  signal (SIGUSR1, sigusr1_handler);
  signal (SIGPIPE, SIG_IGN);
  signal (SIGIO, SIG_IGN);
  //signal (SIGRTMAX, sigrtmax_handler);

  if (daemonize) {
    signal (SIGHUP, sighup_handler);
  }

  if (binlog_disabled && binlog_fd >= 0) {
    epoll_pre_event = read_new_events;
  }

  atexit (done);
  return ff_main (fuse_argc, fuse_argv);
}
