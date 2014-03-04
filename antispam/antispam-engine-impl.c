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
 *          Engine common part implementation part
 *          that must be separated from 'main' function.
 * Created: 01.04.2012
 */

#define _FILE_OFFSET_BITS 64

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <assert.h>
#include <fcntl.h>
#include <signal.h>

#include "net-connections.h"

#include "antispam-common.h"
#include "kdb-antispam-binlog.h"
#include "antispam-data.h"

/**
 * Required variables by engine kernel
 */

int verbosity = 0;
int start_time = 0;
long long binlog_loaded_size;
double binlog_load_time;
int jump_log_ts;
unsigned jump_log_crc32;
long long jump_log_pos;
double index_load_time;
char *progname = 0;
char *username = 0, *binlogname = 0, *logname = 0;

/**
 * Engine initialization common patterns
 */

void antispam_change_user (void)
{
  if (change_user (username) < 0) {
    fprintf (stderr, "fatal: cannot change user to %s\n", username ? username : "(none)");
    exit (1);
  }
  if (verbosity >= 2) {
    fprintf (stderr, "User changed to %s\n", username ? username : "(none)");
  }
}

// Declared: antispam-engine.h
void antispam_engine_common_init_part (char const* index_fname) {
  if (engine_preload_filelist (index_fname, binlogname) < 0) {
    fprintf (stderr, "fatal: cannot preload files for binlog: '%s' and index: '%s'\n", binlogname, index_fname);
    exit (1);
  }

  Snapshot = open_recent_snapshot (engine_snapshot_replica);

  if (Snapshot) {
    engine_snapshot_name = strdup (Snapshot->info->filename);
    engine_snapshot_size = Snapshot->info->file_size;


    if (verbosity > 0) {
      fprintf (stderr, "load index file %s (size %lld)\n", engine_snapshot_name, engine_snapshot_size);
    }
  } else {
    engine_snapshot_name = NULL;
    engine_snapshot_size = 0;
  }

  init_all (Snapshot);
  close_snapshot (Snapshot, 1);

  // Load binlog
  Binlog = open_binlog (engine_replica, jump_log_pos);
  if (!Binlog) {
    fprintf (stderr, "fatal: cannot find binlog for %s, log position %lld\n", engine_replica->replica_prefix, 0LL);
    exit (1);
  }

  binlogname = Binlog->info->filename;

  if (verbosity > 0) {
    fprintf (stderr, "replaying binlog file %s (size %lld)\n", binlogname, Binlog->info->file_size);
  }
  binlog_load_time = get_utime (CLOCK_MONOTONIC);

  clear_log();

  init_log_data (jump_log_pos, jump_log_ts, jump_log_crc32);

  if (verbosity > 0) {
    fprintf (stderr, "jump_log_pos = %lld\nreplay log events started\n", jump_log_pos);
  }

  int replay_log_result = replay_log (0, 1);

  if (binlog_disabled != 1) {
    clear_read_log();
  }

  if (replay_log_result < 0) {
    fprintf (stderr, "fatal: error reading binlog\n");
    exit (1);
  }
  if (verbosity > 0) {
    fprintf (stderr, "replay log events finished\n");
  }

  binlog_load_time = get_utime (CLOCK_MONOTONIC) - binlog_load_time;
  binlog_loaded_size = log_readto_pos - jump_log_pos;

  if (verbosity > 0) {
    fprintf (stderr, "replay binlog file: done, log_pos=%lld, used_z_memory=%ld, time %.06lfs\n",
            (long long)log_pos, dyn_used_memory (), binlog_load_time);
  }

  clear_write_log();

  start_time = time (0);
}
