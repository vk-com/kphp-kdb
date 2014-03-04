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
#include "kdb-pmemcached-binlog.h"
#include "pmemcached-data.h"

int verbosity = 0, skip_timestamps = 0;
double binlog_load_time;
int jump_log_ts;
unsigned jump_log_crc32;
long long jump_log_pos = 0;
long long binlog_loaded_size;
FILE *out;
int dump_log_pos = 0;
int dump_timestamp = 0;
#define mytime() get_utime (CLOCK_MONOTONIC)

/* replay log */

static int pmemcached_le_start (struct lev_start *E) {
  if (E->schema_id != PMEMCACHED_SCHEMA_V1) {
    return -1;
  }
  log_split_min = E->split_min;
  log_split_max = E->split_max;
  log_split_mod = E->split_mod;
  assert (log_split_mod > 0 && log_split_min >= 0 && log_split_min + 1 ==  log_split_max && log_split_max <= log_split_mod);

  return 0;
}

void dump_str (const char *s, int len) {
  int i;
  for (i = 0; i < len; i++) { fputc (s[i], stdout); }
}

inline void dump_line_header (void) {
  if (dump_log_pos) { fprintf (stdout, "%lld\t",  log_cur_pos()); }
  if (dump_timestamp) { fprintf (stdout, "%d\t", now); }
}

int dump_pmemcached_replay_logevent (struct lev_generic *E, int size) {
  int s, key_len;

  switch (E->type) {
  case LEV_START:
    if (size < 24 || E->b < 0 || E->b > 4096) { return -2; }
    s = 24 + ((E->b + 3) & -4);
    if (size < s) { return -1; }
    return pmemcached_le_start ((struct lev_start *) E) >= 0 ? s : -1;
  case LEV_NOOP:
  case LEV_TIMESTAMP:
  case LEV_CRC32:
  case LEV_ROTATE_FROM:
  case LEV_ROTATE_TO:
    return default_replay_logevent (E, size);
  case LEV_PMEMCACHED_DELETE:
    if (size < sizeof (struct lev_pmemcached_delete)) {
      return -2;
    }
    s = ((struct lev_pmemcached_delete *) E)->key_len;
    if (s < 0) {
      return -4;
    }
    s += 1 + offsetof (struct lev_pmemcached_delete, key);
    if (size < s) {
      return -2;
    }
    dump_line_header ();
    fprintf (stdout, "LEV_PMEMCACHED_DELETE\t");
    dump_str (((struct lev_pmemcached_delete *)E)->key, ((struct lev_pmemcached_delete *)E)->key_len);
    fputc ('\n', stdout);
    return s;
  case LEV_PMEMCACHED_GET:
    if (size < sizeof (struct lev_pmemcached_get)) {
      return -2;
    }
    s = ((struct lev_pmemcached_get *) E)->key_len;
    if (s < 0) {
      return -4;
    }

    s += 1 + offsetof (struct lev_pmemcached_get, key);
    if (size < s) {
      return -2;
    }
    dump_line_header ();
    fprintf (stdout, "LEV_PMEMCACHED_GET\t%016llx\t", ((struct lev_pmemcached_get *)E)->hash);
    dump_str (((struct lev_pmemcached_get *)E)->key, ((struct lev_pmemcached_get *)E)->key_len);
    fputc ('\n', stdout);
    return s;
  case LEV_PMEMCACHED_STORE...LEV_PMEMCACHED_STORE+2:
    if (size < sizeof (struct lev_pmemcached_store)) {
      return -2;
    }
    s = ((struct lev_pmemcached_store *) E)->key_len + ((struct lev_pmemcached_store *) E)->data_len;
    if (s < 0) {
      return -4;
    }
    s += 1 + offsetof (struct lev_pmemcached_store, data);
    if (size < s) {
      return -2;
    }
    dump_line_header ();
    key_len = ((struct lev_pmemcached_store *)E)->key_len;
    fprintf (stdout, "LEV_PMEMCACHED_STORE+%d\t%d\t%d\t", E->type & 0xff,
            (int) ((struct lev_pmemcached_store *)E)->flags,
            (int) ((struct lev_pmemcached_store *)E)->delay);
    dump_str (((struct lev_pmemcached_store *)E)->data, key_len);
    fputc('\t', stdout);
    dump_str (((struct lev_pmemcached_store *)E)->data + key_len,
              ((struct lev_pmemcached_store *)E)->data_len);
    fputc ('\n', stdout);
    return s;
  case LEV_PMEMCACHED_STORE_FOREVER...LEV_PMEMCACHED_STORE_FOREVER+2:
    if (size < sizeof (struct lev_pmemcached_store_forever)) {
      return -2;
    }
    s = ((struct lev_pmemcached_store_forever *) E)->key_len + ((struct lev_pmemcached_store_forever *) E)->data_len;
    if (s < 0) {
      return -4;
    }
    s += 1 + offsetof (struct lev_pmemcached_store_forever, data);
    if (size < s) {
      return -2;
    }
    dump_line_header ();
    key_len = ((struct lev_pmemcached_store_forever *)E)->key_len;
    fprintf (stdout, "LEV_PMEMCACHED_STORE_FOREVER+%d\t", E->type & 0xff);
    dump_str (((struct lev_pmemcached_store_forever *)E)->data, key_len);
    fputc('\t', stdout);
    dump_str (((struct lev_pmemcached_store_forever *)E)->data + key_len,
              ((struct lev_pmemcached_store_forever *)E)->data_len);
    fputc ('\n', stdout);
    return s;
  case LEV_PMEMCACHED_INCR:
    if (size < sizeof (struct lev_pmemcached_incr)) {
      return -2;
    }
    s = ((struct lev_pmemcached_incr *) E)->key_len;
    if (s < 0) {
      return -4;
    }

    s += 1 + offsetof (struct lev_pmemcached_incr, key);
    if (size < s) {
      return -2;
    }
    dump_line_header ();
    fprintf (stdout, "LEV_PMEMCACHED_INCR\t%lld\t", ((struct lev_pmemcached_incr *)E)->arg);
    dump_str (((struct lev_pmemcached_incr *)E)->key,
              ((struct lev_pmemcached_incr *)E)->key_len);
    fputc ('\n', stdout);
    return s;
  case LEV_PMEMCACHED_INCR_TINY...LEV_PMEMCACHED_INCR_TINY+255:
    if (size < sizeof (struct lev_pmemcached_incr_tiny)) {
      return -2;
    }
    s = ((struct lev_pmemcached_incr_tiny *) E)->key_len;
    if (s < 0) {
      return -4;
    }

    s += 1 + offsetof (struct lev_pmemcached_incr_tiny, key);
    if (size < s) {
      return -2;
    }
    dump_line_header ();
    fprintf (stdout, "LEV_PMEMCACHED_INCR_TINY+%d\t", E->type & 0xff);
    dump_str (((struct lev_pmemcached_incr_tiny *)E)->key,
              ((struct lev_pmemcached_incr_tiny *)E)->key_len);
    fputc ('\n', stdout);
    return s;
  }

  fprintf (stderr, "unknown log event type %08x at position %lld\n", E->type, log_cur_pos());

  return -1;
}

void usage() {
  fprintf (stderr, "Debug utils: read pmemcached binlog and dumps events.\n");
}

int main (int argc, char *argv[]) {
  int i;
  long long jump_log_pos = 0;
  disable_crc32 = 3;
  out = stdout;
  replay_logevent = dump_pmemcached_replay_logevent;
  while ((i = getopt (argc, argv, "pthvS:")) != -1) {
    switch (i) {
    case 'S':
      if (sscanf (optarg, "%lli", &jump_log_pos) != 1) {
        jump_log_pos = 0;
      }
    case 'p':
      dump_log_pos = 1;
      break;
    case 't':
      dump_timestamp = 1;
      break;
    case 'v':
      verbosity++;
      break;
    case 'h':
      usage();
      return 2;
    }
  }

  if (optind >= argc) {
    usage();
    return 2;
  }

  vkprintf (3, "jump_log_pos: %lld\n", jump_log_pos);

  if (engine_preload_filelist ( argv[optind], binlogname) < 0) {
    fprintf (stderr, "cannot open binlog files for %s\n", binlogname ? binlogname : argv[optind]);
    exit (1);
  }

  if (verbosity>=3){
    fprintf (stderr, "engine_preload_filelist done\n");
  }

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

  return 0;

}
