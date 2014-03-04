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

    Copyright 2012-2013 Vkontakte Ltd
              2012-2013 Vitaliy Valtman
*/

#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <assert.h>

#include "kdb-binlog-common.h"
#include "kdb-data-common.h"
#include "kfs.h"
#include "server-functions.h"

#define RPC_PROXY_INDEX_MAGIC 0x162cb9e2

#define VERSION_STR "rpc-proxy-delete-old-0.1"
int read_only;
int verbosity;

void usage (void) {
  printf ("usage: %s [-v] [-r] [-h] [-u<username>] [-b<backlog>] [-a<binlog-name>] [-l<log-name>] name\n"
      "\t" VERSION_STR " compiled at " __DATE__ " " __TIME__ " by gcc " __VERSION__ " "
#ifdef __LP64__
    "64-bit"
#else
    "32-bit"
#endif
    " after commit " COMMIT "\n"
    "\t-r\tonly prints files, that should be deleted\n"
    ,
    progname);
  exit(2);
}

int main (int argc, char *argv[]) {
  int i;
  set_debug_handlers ();

  while ((i = getopt (argc, argv, "vrb:u:a:l:")) != -1) {
    switch (i) {
    case 'v':
      verbosity++;
      break;
    case 'r':
      read_only = 1;
      break;
    case 'h':
      usage();
      return 2;
    case 'b':
      backlog = atoi(optarg);
      if (backlog <= 0) backlog = BACKLOG;
      break;
    case 'u':
      username = optarg;
      break;
    case 'a':
      binlogname = optarg;
      break;
    case 'l':
      logname = optarg;
      break;
    }
  }
  if (argc != optind + 1 && argc != optind + 2) {
    usage();
    return 2;
  }

  if (change_user (username) < 0) {
    fprintf (stderr, "fatal: cannot change user to %s\n", username ? username : "(none)");
    exit(1);
  }

  init_dyn_data ();

  if (engine_preload_filelist (argv[optind], binlogname) < 0) {
    fprintf (stderr, "cannot open binlog files for %s\n", binlogname ? binlogname : argv[optind]);
    exit (1);
  }

  Snapshot = open_recent_snapshot (engine_snapshot_replica);

  if (!Snapshot) {
    fprintf (stderr, "No snapshots found. Nothing to do\n");
    return 0;
  } else {
    int t[5];
    assert (read (Snapshot->fd, t, 20) == 20);
    if (t[0] != RPC_PROXY_INDEX_MAGIC) {
      fprintf (stderr, "Snapshot is not from rpc-proxy\n");
      return 1;
    }
    long long binlog_pos = *(long long *)(t + 1);
    if (verbosity > 0) {
      fprintf (stderr, "log_pos = %lld\n", binlog_pos);
    }
    for (i = 0; i < engine_replica->binlog_num - 1; i++) {
      struct kfs_file_info *FI = engine_replica->binlogs[i];
      int fd = preload_file_info (FI);
      assert (fd >= 0);
      if (FI->log_pos + FI->file_size <= binlog_pos) {
        if (read_only) {
          fprintf (stderr, "Would delete file %s\n", engine_replica->binlogs[i]->filename);
        } else {
          if (verbosity > 0) {
            fprintf (stderr, "Deleting file %s\n", engine_replica->binlogs[i]->filename);
          }
          int r = unlink (engine_replica->binlogs[i]->filename);
          if (r < 0) {
            fprintf (stderr, "Error deleting file %s: %m\n", engine_replica->binlogs[i]->filename);
          }
        }
      }
      if (fd >= 0) {
        close (fd);
      }
    }
    return 0;
  }
}

