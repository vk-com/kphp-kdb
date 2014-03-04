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

    Copyright 2010-2013 Vkontakte Ltd
              2011-2013 Anton Maydell
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
#include "kfs.h"
#include "kdb-news-binlog.h"
#include "news-data.h"

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

int ug_mode, allowed_types_mask;
int min_utime = 0, max_utime = 0x7fffffff;
/* replay log */

static int news_le_start (struct lev_start *E) {
  switch (E->schema_id) {
  case NEWS_SCHEMA_USER_V1:
    ug_mode = 0;
    allowed_types_mask = USER_TYPES_MASK;
    break;
  case NEWS_SCHEMA_GROUP_V1:
    ug_mode = -1;
    allowed_types_mask = GROUP_TYPES_MASK;
    break;
  case NEWS_SCHEMA_COMMENT_V1:
    ug_mode = 1;
    allowed_types_mask = COMMENT_TYPES_MASK;
    break;
  case NEWS_SCHEMA_NOTIFY_V1:
    ug_mode = 2;
    allowed_types_mask = NOTIFY_TYPES_MASK;
    break;
  case NEWS_SCHEMA_RECOMMEND_V1:
    ug_mode = 3;
    allowed_types_mask = RECOMMEND_TYPES_MASK;
    break;
  default:
    return -1;
  }
  log_split_min = E->split_min;
  log_split_max = E->split_max;
  log_split_mod = E->split_mod;
  assert (log_split_mod > 0 && log_split_min >= 0 && log_split_min + 1 == log_split_max && log_split_max <= log_split_mod);

  return 0;
}

inline void dump_line_header (const char* const msg, int t) {
  if (dump_log_pos) { fprintf (out, "%lld\t",  log_cur_pos()); }
  if (dump_timestamp) { fprintf (out, "%d\t", now); }
  fprintf (out, "LEV_NEWS_%s", msg);
  if (t >= 0) {
    fprintf (out, "+%d", t);
  }
  fprintf (out, "\t");
}

int dump_news_replay_logevent (struct lev_generic *E, int size) {
  int s;
  if (now - 600 > max_utime) {
    exit (0);
  }

  switch (E->type) {
  case LEV_START:
    if (size < 24 || E->b < 0 || E->b > 4096) { return -2; }
    s = 24 + ((E->b + 3) & -4);
    if (size < s) { return -2; }
    return news_le_start ((struct lev_start *) E) >= 0 ? s : -1;
  case LEV_NOOP:
  case LEV_TIMESTAMP:
  case LEV_CRC32:
    return default_replay_logevent (E, size);
  case LEV_NEWS_USERDEL:
    if (size < 8) { return -2; }
    if (ug_mode > 0) { return -1; }
    if (now >= min_utime) {
      dump_line_header ("USERDEL", -1);
      fprintf (out, "%d\n", E->a);
    }
    return 8;
  case LEV_NEWS_PRIVACY:
    if (size < 12) { return -2; }
    if (ug_mode > 0) { return -1; }
    if (now >= min_utime) {
      dump_line_header ("PRIVACY", -1);
      fprintf (out, "%d\t%d\n", E->a, E->b);
    }
    return 12;
  case LEV_NEWS_ITEM+1 ... LEV_NEWS_ITEM+20:
    if (size < 28) { return -2; }
    if (ug_mode > 0) { return -1; }
    if (ug_mode == 0 && E->type ==  LEV_NEWS_ITEM+20) { return -1; }
    if (now >= min_utime) {
      dump_line_header ("ITEM", E->type & 0xff);
      fprintf (out, "%d\t%d\t%d\t%d\t%d\t%d\n",
                        ((struct lev_news_item *) E)->user_id,
                        ((struct lev_news_item *) E)->user,
                        ((struct lev_news_item *) E)->group,
                        ((struct lev_news_item *) E)->owner,
                        ((struct lev_news_item *) E)->place,
                        ((struct lev_news_item *) E)->item);
    }
    return 28;
  case LEV_NEWS_COMMENT+20 ... LEV_NEWS_COMMENT+24:
    if (size < 24) { return -2; }
    if (ug_mode <= 0) { return -1; }
    if (now >= min_utime) {
      dump_line_header ("COMMENT", E->type & 0xff);
      fprintf (out, "%d\t%d\t%d\t%d\t%d\n",
                ((struct lev_news_comment *) E)->user,
                ((struct lev_news_comment *) E)->group,
                ((struct lev_news_comment *) E)->owner,
                ((struct lev_news_comment *) E)->place,
                ((struct lev_news_comment *) E)->item);
    }
    return 24;
  case LEV_NEWS_PLACEDEL+20 ... LEV_NEWS_PLACEDEL+24:
    if (size < 12) { return -2; }
    if (ug_mode <= 0) { return -1; }
    if (now >= min_utime) {
      dump_line_header ("PLACEDEL", E->type & 0xff);
      fprintf (out, "%d\t%d\n", E->a, E->b);
    }
    return 12;
  case LEV_NEWS_HIDEITEM+20 ... LEV_NEWS_HIDEITEM+24:
    if (size < 16) { return -2; }
    if (ug_mode <= 0) { return -1; }
    if (now >= min_utime) {
      dump_line_header ("HIDEITEM", E->type & 0xff);
      fprintf (out, "%d\t%d\t%d\n", E->a, E->b, E->c);
    }
    return 16;
  case LEV_NEWS_SHOWITEM+20 ... LEV_NEWS_SHOWITEM+24:
    if (size < 16) { return -2; }
    if (ug_mode <= 0) { return -1; }
    if (now >= min_utime) {
      dump_line_header ("SHOWITEM", E->type & 0xff);
      fprintf (out, "%d\t%d\t%d\n", E->a, E->b, E->c);
    }
    return 16;
  case LEV_NEWS_RECOMMEND+0 ... LEV_NEWS_RECOMMEND+31:
    if (size < sizeof (struct lev_news_recommend)) { return -2; }
    if (!RECOMMEND_MODE) { return -1; }
    if (now >= min_utime) {
      dump_line_header ("RECOMMEND", E->type & 0xff);
      fprintf (out, "%d\t%d\t%d\t%d\t%d\t%d\n", E->a, E->b, E->c, E->d, E->e, ((struct lev_news_recommend *) E)->item_creation_time);
    }
    return sizeof (struct lev_news_recommend);
  case LEV_NEWS_SET_RECOMMEND_RATE+0 ... LEV_NEWS_SET_RECOMMEND_RATE+31:
    if (size < sizeof (struct lev_news_set_recommend_rate)) { return -2; }
    if (!RECOMMEND_MODE) { return -1; }
    if (now >= min_utime) {
      dump_line_header ("SET_RECOMMEND_RATE", E->type & 0xff);
      fprintf (out, "%d\t%.10lf\n", ((struct lev_news_set_recommend_rate *) E)->action, ((struct lev_news_set_recommend_rate *) E)->rate);
    }
    return sizeof (struct lev_news_set_recommend_rate);
  }

  fprintf (stderr, "unknown log event type %08x at position %lld\n", E->type, log_cur_pos());

  return -1;

}

void usage() {
  fprintf (stderr, "Debug utils: read news binlog and dumps logevents.\n");
  fprintf (stderr, "\t-p\tDumps log positions\n");
  fprintf (stderr, "\t-t\tDumps timestamps\n");
  fprintf (stderr, "\t-T[min_utime,max_utime]\tDump only part of binlog in given time interval.\n");
}

int main (int argc, char *argv[]) {
  int i, x, y;
  long long jump_log_pos = 0;
  out = stdout;
  replay_logevent = dump_news_replay_logevent;
  while ((i = getopt (argc, argv, "tphvT:")) != -1) {
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
    case 'T':
      if (2 == sscanf(optarg, "%d,%d", &x, &y)) {
        min_utime = x;
        max_utime = y;
      }
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
