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
              2012-2013 Anton Maydell
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
#include "kdb-cache-binlog.h"
#include "kfs.h"
#include "md5.h"

//int verbosity = 0, skip_timestamps = 0;
double binlog_load_time;
int jump_log_ts;
unsigned jump_log_crc32;
long long jump_log_pos = 0;
long long binlog_loaded_size;
FILE *out;

int now;
int dump_log_pos = 0;
int dump_timestamp = 0;
#define mytime() get_utime (CLOCK_MONOTONIC)

const char *filtered_uri_short_md5 = "24525683f0f42452cb4216ee2cc44fe3";

/* replay log */

static int cache_le_start (struct lev_start *E) {
  if (E->schema_id != CACHE_SCHEMA_V1) {
    return -1;
  }
  log_split_min = E->split_min;
  log_split_max = E->split_max;
  log_split_mod = E->split_mod;
  assert (log_split_mod > 0 && log_split_min >= 0 && log_split_min + 1 ==  log_split_max && log_split_max <= log_split_mod);

  return 0;
}

static int dump_line_header (const char *const msg) {
  if (dump_log_pos) { fprintf (out, "%lld\t",  log_cur_pos()); }
  if (dump_timestamp) { fprintf (out, "%d\t", now); }
  fprintf (out, "%s", msg);
  return 1;
}

static void a2hex (unsigned char *a, int len, char *output) {
  static char hcyf[16] = "0123456789abcdef";
  int i;
  for (i = 0; i < len; i++) {
    output[2*i] = hcyf[(a[i] >> 4)];
    output[2*i+1] = hcyf[a[i] & 15];
  }
  output[2*len] = 0;
}

static void cache_access_short (struct lev_cache_access_short *E, int t) {
  char output[33];
  a2hex (E->data, 8, output);
  if (!strncmp (output, filtered_uri_short_md5, 16)) {
    if (dump_line_header ("LEV_CACHE_ACCESS_SHORT")) {
      fprintf (out, "+%d\t%s\n", t, output);
    }
  }
}

static void cache_access_long (struct lev_cache_access_long *E, int t) {
}

static void cache_uri_add (struct lev_cache_uri *E, int l) {
  char output[33];
  md5_hex (E->data, l, output);
  if (!strncmp (output, filtered_uri_short_md5, 16)) {
    if (dump_line_header ("LEV_CACHE_URI_ADD")) {
      fprintf (out, "\t%.*s\n", l, E->data);
    }
  }
}

static void cache_uri_delete (struct lev_cache_uri *E, int l) {
  char output[33];
  md5_hex (E->data, l, output);
  if (!strncmp (output, filtered_uri_short_md5, 16)) {
    if (dump_line_header ("LEV_CACHE_URI_DELETE")) {
      fprintf (out, "\t%.*s\n", l, E->data);
    }
  }
}

void cache_set_size_short (struct lev_cache_set_size_short *E) {
  char output[33];
  a2hex ((unsigned char *) E->data, 8, output);
  if (!strncmp (output, filtered_uri_short_md5, 16)) {
    if (dump_line_header ("LEV_CACHE_SET_SIZE_SHORT")) {
      fprintf (out, "\t%s\t%d\n", output, E->size);
    }
  }
}

void cache_set_size_long (struct lev_cache_set_size_long *E) {
}

static int cache_set_new_local_copy_short (struct lev_cache_set_new_local_copy_short *E) {
  return 0;
}

static int cache_set_new_local_copy_long (struct lev_cache_set_new_local_copy_long *E, int local_url_len) {
  return 0;
}

static int cache_delete_local_copy_short (struct lev_cache_set_new_local_copy_short *E) {
  return 0;
}

static int cache_delete_local_copy_long (struct lev_cache_set_new_local_copy_long *E, int local_url_len) {
  return 0;
}

typedef struct {
  short node_id;
  unsigned char server_id;
  unsigned char disk_id;
} cache_local_copy_packed_location_t;

union cache_packed_local_copy_location {
  int i;
  cache_local_copy_packed_location_t p;
};

static int cache_change_disk_status (struct lev_cache_change_disk_status *E, int enable) {
  if (dump_line_header ("LEV_CACHE_CHANGE_DISK_STATUS")) {
    union cache_packed_local_copy_location u;
    u.i = E->packed_location;
    fprintf (out, "+%d\t%d\t%d\t%d\n", enable, (int) u.p.node_id, (int) u.p.server_id, (int) u.p.disk_id);
  }
  return 0;
}

static int cache_delete_remote_disk (struct lev_cache_change_disk_status *E) {
  if (dump_line_header ("LEV_CACHE_DELETE_REMOTE_DISK")) {
    union cache_packed_local_copy_location u;
    u.i = E->packed_location;
    fprintf (out, "\t%d\t%d\t%d\n", (int) u.p.node_id, (int) u.p.server_id, (int) u.p.disk_id);
  }
  return 0;
}

static int cache_set_local_copy_yellow_light_short (struct lev_cache_set_local_copy_yellow_light_short *E) {
  return 0;
}

int dump_cache_replay_logevent (struct lev_generic *E, int size) {
  int s;
  switch (E->type) {
  case LEV_START:
    if (size < 24 || E->b < 0 || E->b > 4096) { return -1; }
    s = 24 + ((E->b + 3) & -4);
    if (size < s) { return -1; }
    return cache_le_start ((struct lev_start *) E) >= 0 ? s : -1;
  case LEV_NOOP:
  case LEV_TIMESTAMP:
  case LEV_CRC32:
  case LEV_ROTATE_TO:
  case LEV_ROTATE_FROM:
    return default_replay_logevent (E, size);
  case LEV_CACHE_ACCESS_SHORT...LEV_CACHE_ACCESS_SHORT+0xff:
    s = sizeof (struct lev_cache_access_short);
    if (size < s) { return -2; }
    cache_access_short ((struct lev_cache_access_short *) E, E->type & 0xff);
    return s;
  case LEV_CACHE_ACCESS_LONG...LEV_CACHE_ACCESS_LONG+0xff:
    s = sizeof (struct lev_cache_access_long);
    if (size < s) { return -2; }
    cache_access_long ((struct lev_cache_access_long *) E, E->type & 0xff);
    return s;
  case LEV_CACHE_URI_ADD...LEV_CACHE_URI_ADD+0xff:
    s = sizeof (struct lev_cache_uri) + (E->type & 0xff);
    if (size < s) { return -2; }
    cache_uri_add ((struct lev_cache_uri *) E, E->type & 0xff);
    return s;
  case LEV_CACHE_URI_DELETE...LEV_CACHE_URI_DELETE+0xff:
    s = sizeof (struct lev_cache_uri) + (E->type & 0xff);
    if (size < s) { return -2; }
    cache_uri_delete ((struct lev_cache_uri *) E, E->type & 0xff);
    return s;
  case LEV_CACHE_SET_SIZE_SHORT:
    s = sizeof (struct lev_cache_set_size_short);
    if (size < s) { return -2; }
    cache_set_size_short ((struct lev_cache_set_size_short *) E);
    return s;
  case LEV_CACHE_SET_SIZE_LONG:
    s = sizeof (struct lev_cache_set_size_long);
    if (size < s) { return -2; }
    cache_set_size_long ((struct lev_cache_set_size_long *) E);
    return s;
  case LEV_CACHE_SET_NEW_LOCAL_COPY_SHORT:
    s = sizeof (struct lev_cache_set_new_local_copy_short);
    if (size < s) { return -2; }
    cache_set_new_local_copy_short ((struct lev_cache_set_new_local_copy_short *) E);
    return s;
  case LEV_CACHE_SET_NEW_LOCAL_COPY_LONG...LEV_CACHE_SET_NEW_LOCAL_COPY_LONG+0xff:
    s = sizeof (struct lev_cache_set_new_local_copy_long) + (E->type & 0xff);
    if (size < s) { return -2; }
    cache_set_new_local_copy_long ((struct lev_cache_set_new_local_copy_long *) E, E->type & 0xff);
    return s;
  case LEV_CACHE_DELETE_LOCAL_COPY_SHORT:
    s = sizeof (struct lev_cache_set_new_local_copy_short);
    if (size < s) { return -2; }
    cache_delete_local_copy_short ((struct lev_cache_set_new_local_copy_short *) E);
    return s;
  case LEV_CACHE_DELETE_LOCAL_COPY_LONG...LEV_CACHE_DELETE_LOCAL_COPY_LONG+0xff:
    s = sizeof (struct lev_cache_set_new_local_copy_long) + (E->type & 0xff);
    if (size < s) { return -2; }
    cache_delete_local_copy_long ((struct lev_cache_set_new_local_copy_long *) E, E->type & 0xff);
    return s;
  case LEV_CACHE_CHANGE_DISK_STATUS...LEV_CACHE_CHANGE_DISK_STATUS+1:
    s = sizeof (struct lev_cache_change_disk_status);
    if (size < s) { return -2; }
    cache_change_disk_status ((struct lev_cache_change_disk_status *) E, E->type & 1);
    return s;
  case LEV_CACHE_DELETE_REMOTE_DISK:
    s = sizeof (struct lev_cache_change_disk_status);
    if (size < s) { return -2; }
    cache_delete_remote_disk ((struct lev_cache_change_disk_status *) E);
    return s;
  case LEV_CACHE_SET_LOCAL_COPY_YELLOW_LIGHT_SHORT:
    s = sizeof (struct lev_cache_set_local_copy_yellow_light_short);
    if (size < s) { return -2; }
    cache_set_local_copy_yellow_light_short ((struct lev_cache_set_local_copy_yellow_light_short *) E);
    return s;
/*
  case LEV_CACHE_SET_LOCAL_COPY_YELLOW_LIGHT_LONG...LEV_CACHE_SET_LOCAL_COPY_YELLOW_LIGHT_LONG+0xff:
    s = sizeof (struct lev_cache_set_local_copy_yellow_light_long) + (E->type & 0xff);
    if (size < s) { return -2; }
    cache_set_local_copy_yellow_light_long ((struct lev_cache_set_local_copy_yellow_light_long *) E, E->type & 0xff);
    return s;
*/
  }

  kprintf ("unknown log event type %08x at position %lld\n", E->type, log_cur_pos());

  return -3;

}

void usage() {
  fprintf (stderr,
    "cache-binlog [-p] [-t] [-v] <cache-binlog>\n"
    "\tRead cache binlog and dumps logevents.\n"
    "\t-F<filtered-uri-hex-md5>\tsets global URI's md5 which will be dumped\n"
    "\t-p\tdump log pos\n"
    "\t-t\tdump timestamp\n"
  );
  exit (2);
}

int main (int argc, char *argv[]) {
  int i;
  long long jump_log_pos = 0;
  out = stdout;
  replay_logevent = dump_cache_replay_logevent;
  while ((i = getopt (argc, argv, "tphv")) != -1) {
    switch (i) {
    case 'F':
      filtered_uri_short_md5 = optarg;
      break;
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
    }
  }

  if (optind >= argc) {
    usage();
  }

  if (engine_preload_filelist ( argv[optind], binlogname) < 0) {
    fprintf (stderr, "cannot open binlog files for %s\n", binlogname ? binlogname : argv[optind]);
    exit (1);
  }

  Binlog = open_binlog (engine_replica, jump_log_pos);
  if (!Binlog) {
    kprintf ("fatal: cannot find binlog for %s, log position %lld\n", engine_replica->replica_prefix, jump_log_pos);
    exit (1);
  }

  binlogname = Binlog->info->filename;

  vkprintf (1, "replaying binlog file %s (size %lld)\n", binlogname, Binlog->info->file_size);
  binlog_load_time = -mytime();
  clear_log();
  init_log_data (jump_log_pos, jump_log_ts, jump_log_crc32);
  vkprintf (1, "replay log events started\n");

  i = replay_log (0, 1);

  vkprintf (1, "replay log events finished\n");

  binlog_load_time += mytime();
  binlog_loaded_size = log_readto_pos - jump_log_pos;

  return 0;

}
