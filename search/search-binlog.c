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

    Copyright 2009-2013 Vkontakte Ltd
              2009-2010 Nikolai Durov
              2009-2010 Andrei Lopatin
              2010-2013 Vitaliy Valtman
              2010-2013 Anton Maydell
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
#include "kdb-search-binlog.h"
#include "search-data.h"

int verbosity = 0, skip_timestamps = 0;
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

/* replay log */


static int search_le_start (struct lev_start *E) {
  if (E->schema_id != SEARCH_SCHEMA_V1) {
    return -1;
  }
  log_split_min = E->split_min;
  log_split_max = E->split_max;
  log_split_mod = E->split_mod;
  assert (log_split_mod > 0 && log_split_min >= 0 && log_split_min + 1 ==  log_split_max && log_split_max <= log_split_mod);

  return 0;
}

void dump_item_id (long long item_id) {
  int t = SHORT_ID (item_id);
  if (t) {
    fprintf (stdout, "%d_%d", (int) item_id, (int) t);
  } else {
    fprintf (stdout, "%d", (int) item_id);
  }
}
inline void dump_line_header (void) {
  if (dump_log_pos) { fprintf (stdout, "%lld\t",  log_cur_pos()); }
  if (dump_timestamp) { fprintf (stdout, "%d\t", now); }
}

int dump_search_replay_logevent (struct lev_generic *E, int size) {
  int s;
  switch (E->type) {
  case LEV_START:
    if (size < 24 || E->b < 0 || E->b > 4096) { return -1; }
    s = 24 + ((E->b + 3) & -4);
    if (size < s) { return -1; }
    return search_le_start ((struct lev_start *) E) >= 0 ? s : -1;
  case LEV_NOOP:
  case LEV_TIMESTAMP:
  case LEV_CRC32:
  case LEV_ROTATE_TO:
  case LEV_ROTATE_FROM:
    return default_replay_logevent (E, size);
  case LEV_SEARCH_TEXT_LONG:
    if (size < 24) { return -2; }
    struct lev_search_text_long_entry *EL = (void *) E;
    s = (unsigned) EL->text_len;
    if (size < 23 + s) { return -2; }
    if (EL->text[s]) { return -4; }
    //change_item (EL->text, s, EL->obj_id, EL->rate, EL->rate2);
    dump_line_header ();
    fprintf (stdout, "LEV_SEARCH_TEXT_LONG\t");
    dump_item_id (((struct lev_search_text_short_entry *) E)->obj_id);
    fprintf (stdout, "\t%d\t%d\t%s\n",
                     ((struct lev_search_text_short_entry *) E)->rate,
                     ((struct lev_search_text_short_entry *) E)->rate2,
                     ((struct lev_search_text_short_entry *) E)->text
            );
    return 23+s;
  case LEV_SEARCH_TEXT_SHORT ... LEV_SEARCH_TEXT_SHORT+0xff:
    if (size < 20) { return -2; }
    struct lev_search_text_short_entry *ES = (void *) E;
    s = (E->type & 0xff);
    if (size < 21 + s) { return -2; }
    if (ES->text[s]) { return -4; }
    //change_item (ES->text, s, ES->obj_id, ES->rate, ES->rate2);
    dump_line_header ();
    fprintf (stdout, "LEV_SEARCH_TEXT_SHORT\t");
    dump_item_id (((struct lev_search_text_short_entry *) E)->obj_id);
    fprintf (stdout, "\t%d\t%d\t%s\n",
                     ((struct lev_search_text_short_entry *) E)->rate,
                     ((struct lev_search_text_short_entry *) E)->rate2,
                     ((struct lev_search_text_short_entry *) E)->text
            );
    return 21+s;
  case LEV_SEARCH_DELETE_ITEM:
    if (size < 12) { return -2; }
    dump_line_header ();
    fprintf (stdout, "LEV_SEARCH_DELETE_ITEM\t");
    dump_item_id (((struct lev_search_delete_item *) E)->obj_id);
    fprintf (stdout, "\n");
    return 12;
  case LEV_SEARCH_SET_RATE:
    if (size < 16) { return -2; }
    dump_line_header ();
    fprintf (stdout, "LEV_SET_RATE\t");
    dump_item_id (((struct lev_search_set_rate *) E)->obj_id);
    fprintf (stdout, "\t%d\n", E->a);
    //set_rate (((struct lev_search_set_rate *) E)->obj_id, E->a);
    return 16;
  case LEV_SEARCH_SET_RATE2:
    if (size < 16) { return -2; }
    dump_line_header ();
    fprintf (stdout, "LEV_SET_RATE2\t");
    dump_item_id (((struct lev_search_set_rate *) E)->obj_id);
    fprintf (stdout, "\t%d\n", E->a);
    //set_rate2 (((struct lev_search_set_rate *) E)->obj_id, E->a);
    return 16;
  case LEV_SEARCH_SET_RATES:
    if (size < 20) { return -2; }
    //set_rates (((struct lev_search_set_rates *) E)->obj_id, E->a, E->d);
    dump_line_header ();
    fprintf (stdout, "LEV_SET_RATES\t");
    dump_item_id (((struct lev_search_set_rates *) E)->obj_id);
    fprintf (stdout, "\t%d\t%d\n", E->a, E->d);
    return 20;
  case LEV_SEARCH_INCR_RATE_SHORT ... LEV_SEARCH_INCR_RATE_SHORT + 0xff:
    if (size < 12) { return -2; }
    //incr_rate (((struct lev_search_incr_rate_short *) E)->obj_id, (signed char) E->type);
    dump_line_header ();
    fprintf (stdout, "LEV_INCR_RATE_SHORT+%d\t", E->type & 0xff);
    dump_item_id (((struct lev_search_incr_rate_short *) E)->obj_id);
    fprintf (stdout, "\n");
    return 12;
  case LEV_SEARCH_INCR_RATE:
    if (size < 16) { return -2; }
    dump_line_header ();
    fprintf (stdout, "LEV_INCR_RATE\t");
    dump_item_id (((struct lev_search_incr_rate *) E)->obj_id);
    fprintf (stdout, "\t%d\n", E->a);
    //incr_rate (((struct lev_search_incr_rate *) E)->obj_id, E->a);
    return 16;
  case LEV_SEARCH_INCR_RATE2_SHORT ... LEV_SEARCH_INCR_RATE2_SHORT + 0xff:
    if (size < 12) { return -2; }
    dump_line_header ();
    fprintf (stdout, "LEV_INCR_RATE2_SHORT+%d\t", E->type & 0xff);
    dump_item_id (((struct lev_search_incr_rate_short *) E)->obj_id);
    fprintf (stdout, "\n");
    //incr_rate2 (((struct lev_search_incr_rate_short *) E)->obj_id, (signed char) E->type);
    return 12;
  case LEV_SEARCH_INCR_RATE2:
    if (size < 16) { return -2; }
    //incr_rate2 (((struct lev_search_incr_rate *) E)->obj_id, E->a);
    dump_line_header ();
    fprintf (stdout, "LEV_INCR_RATE2\t");
    dump_item_id (((struct lev_search_incr_rate *) E)->obj_id);
    fprintf (stdout, "\t%d\n", E->a);
    return 16;
  case LEV_SEARCH_INCR_RATE_NEW ... LEV_SEARCH_INCR_RATE_NEW + 0xff:
    if (size < 16) { return -2; }
    dump_line_header ();
    fprintf (stdout, "LEV_INCR_RATE_NEW+%d\t", E->type & 0xff);
    dump_item_id (((struct lev_search_incr_rate_new *) E)->obj_id);
    fprintf (stdout, "\t%d\n", ((struct lev_search_incr_rate_new *) E)->rate_incr);
    return 16;
  case LEV_SEARCH_SET_RATE_NEW ... LEV_SEARCH_SET_RATE_NEW + 0xff:
    if (size < 16) { return -2; }
    dump_line_header ();
    fprintf (stdout, "LEV_SET_RATE_NEW+%d\t", E->type & 0xff);
    dump_item_id (((struct lev_search_set_rate_new *) E)->obj_id);
    fprintf (stdout, "\t%d\n", ((struct lev_search_set_rate_new *) E)->rate);
    return 16;
  case LEV_SEARCH_SET_HASH:
    if (size < 20) { return -2; }
    dump_line_header ();
    fprintf (stdout, "LEV_SET_HASH\t");
    dump_item_id (((struct lev_search_set_hash *) E)->obj_id);
    fprintf (stdout, "\t%llx\n", ((struct lev_search_set_hash *) E)->hash);
    return 20;
  case LEV_SEARCH_RESET_ALL_RATES:
    if (size < 8) { return -2; }
    dump_line_header ();
    fprintf (stdout, "LEV_RESET_ALL_RATES\t%d\n", ((struct lev_search_reset_all_rates *) E)->rate_id);
    return 8;
  }

  fprintf (stderr, "unknown log event type %08x at position %lld\n", E->type, log_cur_pos());

  return -3;

}

void usage() {
  fprintf (stderr, "Debug utils: read search binlog and dumps logevents.\n");
  fprintf (stderr, "Dumps: only delete item event.\n");
}

int main (int argc, char *argv[]) {
  int i;
  long long jump_log_pos = 0;
  out = stdout;
  replay_logevent = dump_search_replay_logevent;
  while ((i = getopt (argc, argv, "tphv")) != -1) {
    switch (i) {
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
