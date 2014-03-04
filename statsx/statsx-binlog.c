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

    Copyright 2011 Vkontakte Ltd
              2011 Anton Maydell
*/

#define _FILE_OFFSET_BITS 64

#include <assert.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <fcntl.h>
#include <unistd.h>
#include <time.h>

#include "server-functions.h"
#include "kdb-statsx-binlog.h"
#include "statsx-data.h"

int verbosity = 0, skip_timestamps = 0;
double binlog_load_time, snapshot_load_time;
int jump_log_ts;
unsigned jump_log_crc32;
long long jump_log_pos = 0;
long long binlog_loaded_size;
FILE *out;


#define mytime() get_utime (CLOCK_MONOTONIC)

/* replay log */
#define ANY_COUNTER INT_MIN
static int cnt_id = ANY_COUNTER;
int start_time = 0, end_time = INT_MAX;

static int stats_le_start (struct lev_start *E) {
  if (E->schema_id != STATS_SCHEMA_V1) {
    return -1;
  }
  log_split_min = E->split_min;
  log_split_max = E->split_max;
  log_split_mod = E->split_mod;
  assert (log_split_mod > 0 && log_split_min >= 0 && log_split_min + 1 ==  log_split_max && log_split_max <= log_split_mod);

  return 0;
}

#undef COUNTERS_PRIME
#define COUNTERS_PRIME 10000019
int Counters[COUNTERS_PRIME];
int T[COUNTERS_PRIME];


static void qsort_i (int a, int b) {
  int i, j;
  int h;
  if (a >= b) { return; }
  h = T[(a+b)>>1];
  i = a;
  j = b;
  do {
    while (T[i] < h) { i++; }
    while (T[j] > h) { j--; }
    if (i <= j) {
      int t = T[i];  T[i] = T[j];  T[j] = t;
      t = Counters[i]; Counters[i] = Counters[j]; Counters[j] = t;
      i++; j--;
    }
  } while (i <= j);
  qsort_i (a, j);
  qsort_i (i, b);
}

void count (void) {
  int i, n = 0;
  for (i = 0; i < COUNTERS_PRIME; i++) {
    if (Counters[i]) {
      Counters[n] = Counters[i];
      T[n] = T[i];
      n++;
    }
  }
  if (n > 0) {
    qsort_i (0, n - 1);
    for (i = n - 1; i >= 0 && i >= n - 100; i--) {
      printf ("%d\t%d\n", Counters[i], T[i]);
    }
  }
}

void counter_add (int counter_id) {
  int h1, h2;
  int D;
  if (!counter_id) { return; }
  //if (counter_id < 0) { return; } //DEBUG
  h1 = h2 = counter_id;
  if (h1 < 0) { h1 = 17-h1; }
  h1 = h1 % COUNTERS_PRIME;
  if (h2 < 0) { h2 = 17239-h2; }
  if (h1 < 0 || h2 < 0) { return; }
  h2 = 1 + (h2 % (COUNTERS_PRIME - 1));
  while ((D = Counters[h1]) != 0) {
    if (D == counter_id) { 
      T[h1]++;
      return; 
    }
    h1 += h2;
    if (h1 >= COUNTERS_PRIME) { h1 -= COUNTERS_PRIME; }
  }
  Counters[h1] = counter_id;
  T[h1] = 1;
  fprintf (stdout, "%d\n", counter_id);
  return;
}

inline int check (struct lev_generic *E) {
  return ((E->a == cnt_id || cnt_id == ANY_COUNTER) && now >= start_time);
}

int active_users_stats_replay_logevent (struct lev_generic *E, int size) {
  int s;
  switch (E->type) {
  case LEV_START:
    if (size < 24 || E->b < 0 || E->b > 4096) { return -2; }
    s = 24 + ((E->b + 3) & -4);
    if (size < s) { return -1; }
    memset (Counters, 0, sizeof(Counters));
    return stats_le_start ((struct lev_start *) E) >= 0 ? s : -1;
  case LEV_NOOP:
  case LEV_TIMESTAMP:
  case LEV_CRC32:
  case LEV_ROTATE_TO:
  case LEV_ROTATE_FROM:
    return default_replay_logevent (E, size);
  case LEV_STATS_VIEWS_EXT:
    if (size < 12) { return -2; }
    return 12;
  case LEV_STATS_VIEWS ... LEV_STATS_VIEWS + 0xff:
    if (size < 8) { return -2; }
    return 8;
  case LEV_STATS_VISITOR_OLD:
    if (size < 12) { return -2; }
    return 12;
  case LEV_STATS_VISITOR ... LEV_STATS_VISITOR + 0xff:
    if (size < 12) { return -2; }
    return 12;
  case LEV_STATS_VISITOR_OLD_EXT:
    if (size < 20) { return -2; }
    if (check (E)) {
      counter_add (E->b);
    }
    return 20;
  case LEV_STATS_VISITOR_EXT ... LEV_STATS_VISITOR_EXT + 0xff:
    if (size < sizeof (struct lev_stats_visitor_ext)) { return -2; }
    if (check (E)) {
      counter_add (E->b);
    }
    return sizeof (struct lev_stats_visitor_ext);
  case LEV_STATS_COUNTER_ON:
    if (size < 8) { return -2; }
    return 8;
  case LEV_STATS_COUNTER_OFF:
    if (size < 8) { return -2; }
    return 8;
  case LEV_STATS_TIMEZONE ... LEV_STATS_TIMEZONE + 0xff:
    if (size < 8) { return -2; }
    return 8;
  }
   
  fprintf (stderr, "unknown log event type %08x at position %lld\n", E->type, log_cur_pos());

  return -1;
}


int dump_unique_cid_stats_replay_logevent (struct lev_generic *E, int size) {
  int s;
  switch (E->type) {
  case LEV_START:
    if (size < 24 || E->b < 0 || E->b > 4096) { return -2; }
    s = 24 + ((E->b + 3) & -4);
    if (size < s) { return -1; }
    memset (Counters, 0, sizeof(Counters));
    return stats_le_start ((struct lev_start *) E) >= 0 ? s : -1;
  case LEV_NOOP:
  case LEV_TIMESTAMP:
  case LEV_CRC32:
  case LEV_ROTATE_TO:
  case LEV_ROTATE_FROM:
    return default_replay_logevent (E, size);
  case LEV_STATS_VIEWS_EXT:
    if (size < 12) { return -2; }
    counter_add (E->a);
    return 12;
  case LEV_STATS_VIEWS ... LEV_STATS_VIEWS + 0xff:
    if (size < 8) { return -2; }
    counter_add (E->a);
    return 8;
  case LEV_STATS_VISITOR_OLD:
    if (size < 12) { return -2; }
    counter_add (E->a);
    return 12;
  case LEV_STATS_VISITOR ... LEV_STATS_VISITOR + 0xff:
    if (size < 12) { return -2; }
    counter_add (E->a);
    return 12;
  case LEV_STATS_VISITOR_OLD_EXT:
    if (size < 20) { return -2; }
    counter_add (E->a);
    return 20;
  case LEV_STATS_VISITOR_EXT ... LEV_STATS_VISITOR_EXT + 0xff:
    if (size < sizeof (struct lev_stats_visitor_ext)) { return -2; }
    counter_add (E->a);
    return sizeof (struct lev_stats_visitor_ext);
  case LEV_STATS_COUNTER_ON:
    if (size < 8) { return -2; }
    counter_add (E->a);
    return 8;
  case LEV_STATS_COUNTER_OFF:
    if (size < 8) { return -2; }
    counter_add (E->a);
    return 8;
  case LEV_STATS_TIMEZONE ... LEV_STATS_TIMEZONE + 0xff:
    if (size < 8) { return -2; }
    counter_add (E->a);
    //set_timezone (E->a, E->type - LEV_STATS_TIMEZONE, 1);
    return 8;
  case LEV_STATS_VISITOR_VERSION:
    if (size < 12) { return -2; }
    counter_add (E->a);
    return 12;
  }
   
  fprintf (stderr, "unknown log event type %08x at position %lld\n", E->type, log_cur_pos());

  return -1;
}


int dump_stats_replay_logevent (struct lev_generic *E, int size) {
  int s;
  if (now > end_time) {
    exit (0);
  }
  switch (E->type) {
  case LEV_START:
    if (size < 24 || E->b < 0 || E->b > 4096) { return -2; }
    s = 24 + ((E->b + 3) & -4);
    if (size < s) { return -1; }
    return stats_le_start ((struct lev_start *) E) >= 0 ? s : -1;
  case LEV_NOOP:
  case LEV_TIMESTAMP:
  case LEV_CRC32:
  case LEV_ROTATE_TO:
  case LEV_ROTATE_FROM:
    return default_replay_logevent (E, size);
  case LEV_STATS_VIEWS_EXT:
    if (size < 12) { return -2; }
    if (check (E)) {
      printf("%d LEV_STATS_VIEWS_EXT %d %d\n", now, E->a, E->b);
    }
    return 12;
  case LEV_STATS_VIEWS ... LEV_STATS_VIEWS + 0xff:
    if (size < 8) { return -2; }
    if (check (E)) {
      printf("%d LEV_STATS_VIEWS+%d %d\n", now, (E->type & 0xff), E->a);
    }
    return 8;
  case LEV_STATS_VISITOR_OLD:
    if (size < 12) { return -2; }
    if (check (E)) {
      printf("%d LEV_STATS_VISITOR_OLD %d %d\n", now, E->a, E->b);
    }
    return 12;
  case LEV_STATS_VISITOR ... LEV_STATS_VISITOR + 0xff:
    if (size < 12) { return -2; }
    if (check (E)) {
      printf("%d LEV_STATS_VISITOR+%d %d %d\n", now, E->type & 0xff, E->a, E->b);
    }
    return 12;
  case LEV_STATS_VISITOR_OLD_EXT:
    if (size < 20) { return -2; }
    if (check (E)) {
      struct lev_stats_visitor_old_ext *EE = (struct lev_stats_visitor_old_ext *) E;
      printf("%d LEV_STATS_VISITOR_OLD_EXT %d %d %d %d %d %d %d\n", now, EE->cnt_id, EE->user_id, EE->sex_age,
             EE->m_status, EE->polit_views, EE->section, EE->city);
    }
    return 20;
  case LEV_STATS_VISITOR_EXT ... LEV_STATS_VISITOR_EXT + 0xff:
    if (size < sizeof (struct lev_stats_visitor_ext)) { return -2; }
    if (check (E)) {
      struct lev_stats_visitor_ext *EE = (struct lev_stats_visitor_ext *) E;
      printf("%d LEV_STATS_VISITOR_EXT+%d %d %d %d %d %d %d %d %d %d %d\n", now, E->type & 0xff, 
             EE->cnt_id, EE->user_id, EE->sex_age,
             EE->m_status, EE->polit_views, EE->section, EE->city, EE->geoip_country, EE->country, EE->source);
    }
    return sizeof (struct lev_stats_visitor_ext);
  case LEV_STATS_COUNTER_ON:
    if (size < 8) { return -2; }
    if (check (E)) {
      printf("%d LEV_STATS_COUNTER_ON %d\n", now, E->a);
    }
    return 8;
  case LEV_STATS_COUNTER_OFF:
    if (size < 8) { return -2; }
    if (check (E)) {
      printf("%d LEV_STATS_COUNTER_OFF %d\n", now, E->a);
    }
    return 8;
  case LEV_STATS_TIMEZONE ... LEV_STATS_TIMEZONE + 0xff:
    if (size < 8) { return -2; }
    //set_timezone (E->a, E->type - LEV_STATS_TIMEZONE, 1);
    return 8;
  case LEV_STATS_VISITOR_VERSION:
    if (size < 12) { return -2; }
    if (check (E)) {
      printf ("%d LEV_STATS_VISITOR_VERSION %d\t%d\n", now, E->a, E->b);
    }
    return 12;
  }
   
  fprintf (stderr, "unknown log event type %08x at position %lld\n", E->type, log_cur_pos());

  return -1;

}

void usage() {
  fprintf (stderr, "Debug utils: read stats binlog and dumps events for given counter.\n");
  fprintf (stderr, "\t-c [counter_id]\tDumps only logevent for given counter.\n");
  fprintf (stderr, "\t-l\tGenerate list of all used counters\n");
  fprintf (stderr, "\t-u\tGenerate list of active users (finding bots)\n");
  fprintf (stderr, "\t-S<start time>\n");
  fprintf (stderr, "\t-T<end time>\n");
  fprintf (stderr, "\t-i\tuse recent snapshot (for the case when first binlog was deleted)\n");
  exit (2);
}

int load_index (kfs_file_handle_t Index) {
  if (Index == NULL) {
    jump_log_ts = 0;
    jump_log_pos = 0;
    jump_log_crc32 = 0;
    return 0;
  }
  int idx_fd = Index->fd;
  struct index_header_v2 header;
  read (idx_fd, &header, sizeof (struct index_header_v2));
  if (header.magic !=  STATSX_INDEX_MAGIC_V1 && header.magic != STATSX_INDEX_MAGIC_OLD && header.magic !=  STATSX_INDEX_MAGIC_V1 + 1 && header.magic != STATSX_INDEX_MAGIC_V2 && header.magic != STATSX_INDEX_MAGIC_V2) {
    fprintf (stderr, "index file is not for statsx\n");
    return -1;
  }

  jump_log_ts = header.log_timestamp;
  jump_log_pos = header.log_pos1;
  jump_log_crc32 = header.log_pos1_crc32;
  return 1;
}

int main (int argc, char *argv[]) {
  int use_index = 0;
  int i;
  out = stdout;
  replay_logevent = dump_stats_replay_logevent;
  while ((i = getopt (argc, argv, "c:hvlS:T:ui")) != -1) {
    switch (i) {
    case 'i':
      use_index = 1;
      break;
    case 'S':
      if (1 == sscanf(optarg, "%d", &i)) {
        start_time = i;
      }
      break;
    case 'T':
      if (1 == sscanf(optarg, "%d", &i)) {
        end_time = i;
      }
      break;
    case 'c':
      if (1 == sscanf(optarg, "%d", &i)) {
        cnt_id = i;
      }
      break;
    case 'v':
      verbosity++;
      break;
    case 'h':
      usage();
      return 2;
    case 'l':
      replay_logevent = dump_unique_cid_stats_replay_logevent;
      break;
    case 'u':
      replay_logevent = active_users_stats_replay_logevent;
      break;
    }
  }

  if (optind >= argc) {
    usage();
    return 2;
  }

  if (engine_preload_filelist ( argv[optind], binlogname) < 0) {
    fprintf (stderr, "cannot open binlog files for %s\n", binlogname ? binlogname : argv[optind]);
    exit (1);
  }
  
  if (use_index) {
    vkprintf (1, "Use index\n");
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

    snapshot_load_time = -get_utime(CLOCK_MONOTONIC);
    i = load_index (Snapshot);  
    snapshot_load_time += get_utime(CLOCK_MONOTONIC);
    if (i < 0) {
      fprintf (stderr, "fatal: error %d while loading index file %s\n", i, engine_snapshot_name);
      exit (1);
    }
  }


  if (verbosity>=3){
    fprintf (stderr, "engine_preload_filelist done\n");
  }
  vkprintf (3, "jump_log_pos = %lld\n", jump_log_pos);
  Binlog = open_binlog (engine_replica, jump_log_pos);
  if (!Binlog) {
    fprintf (stderr, "fatal: cannot find binlog for %s, log position %lld\n", engine_replica->replica_prefix, jump_log_pos);
    exit (1);
  }

  binlogname = Binlog->info->filename;

  if (verbosity) {
    fprintf (stderr, "replaying binlog file %s (size %lld)\n", binlogname, Binlog->info->file_size);
  }
  binlog_load_time = -mytime();
  clear_log();
  init_log_data (jump_log_pos, jump_log_ts, jump_log_crc32);
  if (verbosity) {
    fprintf (stderr, "replay log events started\n");
  }

  i = replay_log (0, 1);
 
  if (verbosity) {
    fprintf (stderr, "replay log events finished\n");
  }

  binlog_load_time += mytime();
  binlog_loaded_size = log_readto_pos - jump_log_pos;

  if (replay_logevent == active_users_stats_replay_logevent || replay_logevent == dump_unique_cid_stats_replay_logevent) {
    count ();
  }

  return 0;
}

