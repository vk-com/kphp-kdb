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

    Copyright 2012 Vkontakte Ltd
              2012 Anton Maydell
*/

#define _FILE_OFFSET_BITS 64

#include <assert.h>
#include <stdio.h>
#include <string.h>
#include <stddef.h>
#include <stdlib.h>
#include <fcntl.h>
#include <unistd.h>
#include <time.h>
#include <sys/wait.h>

#include "net-connections.h"
#include "kfs.h"
#include "server-functions.h"
#include "kdb-text-binlog.h"

int skip_timestamps = 0;
double binlog_load_time;
int jump_log_ts;
unsigned jump_log_crc32;
long long jump_log_pos = 0;
long long binlog_loaded_size;
FILE *out;

int start_time = 0, end_time = 0x7fffffff;
static int dump_log_pos = 0, dump_timestamp = 0;
#define mytime() get_utime (CLOCK_MONOTONIC)

#define	BB0(x)	x,
#define BB1(x)	BB0(x) BB0(x+1) BB0(x+1) BB0(x+2)
#define BB2(x)	BB1(x) BB1(x+1) BB1(x+1) BB1(x+2)
#define BB3(x)	BB2(x) BB2(x+1) BB2(x+1) BB2(x+2)
#define BB4(x)	BB3(x) BB3(x+1) BB3(x+1) BB3(x+2)
#define BB5(x)	BB4(x) BB4(x+2) BB4(x+2) BB4(x+4)
#define BB6(x)	BB5(x) BB5(x+2) BB5(x+2) BB5(x+4)

char prec_mask_intcount[MAX_EXTRA_MASK+2] = { BB6(0) 0 };

static inline int extra_mask_intcount (int mask) {
  return prec_mask_intcount[mask & MAX_EXTRA_MASK];
}

static int text_le_start (struct lev_start *E) {
  if (E->schema_id != TEXT_SCHEMA_V1) {
    return -1;
  }
  log_split_min = E->split_min;
  log_split_max = E->split_max;
  log_split_mod = E->split_mod;
  assert (log_split_mod > 0 && log_split_min >= 0 && log_split_min + 1 == log_split_max && log_split_max <= log_split_mod);

/*
  int q = 0;
  if (E->extra_bytes >= 3 && E->str[0] == 1) {
    init_extra_mask (*(unsigned short *) (E->str + 1));
    q = 3;
  }
  if (E->extra_bytes >= q + 6 && !memcmp (E->str + q, "status", 6)) {
    memcpy (&PeerFlagFilter, &Statuses_PeerFlagFilter, sizeof (PeerFlagFilter));
    Sublists = Statuses_Sublists;
    count_sublists ();
  }
  if (E->extra_bytes >= q + 5 && !memcmp (E->str + q, "forum", 5)) {
    memcpy (&PeerFlagFilter, &Forum_PeerFlagFilter, sizeof (PeerFlagFilter));
    Sublists = Forum_Sublists;
    count_sublists ();
  }
*/  

  return 0;
}

inline int dump_line_header (const char *const tp, char ch) {
  if (start_time > now) { return -1; }
  if (dump_log_pos) { fprintf (out, "%lld\t",  log_cur_pos()); }
  if (dump_timestamp) { fprintf (out, "%d\t", now); }
  fprintf (out, "%s%c", tp, ch);
  return 0;
}

static int dumping_crc32;

void dump_crc32 (struct lev_crc32 *E) {
  if (!dumping_crc32) { return; }
  if (dump_line_header ("LEV_CRC32", '\t')) {
    return;
  }
  fprintf (out, "%lld\t0x%x\n", E->pos, E->crc32);
}

static void dump_incr_message_flags (struct lev_generic *E) {
  if (dump_line_header ("LEV_TX_INCR_MESSAGE_FLAGS", '+') < 0) {
    return; 
  }
  fprintf (out, "%d\t%d\t%d\n", E->type & 0xff, E->a, E->b);
}

int dump_text_replay_logevent (struct lev_generic *E, int size);

int init_text_data (int schema) {
  replay_logevent = dump_text_replay_logevent;
  return 0;
}

int dump_text_replay_logevent (struct lev_generic *E, int size) {
  int s, t;
  if (now > end_time) {
    fflush (out);
    fclose (out);
    exit (0);
  }
  struct lev_add_message *EM;
  switch (E->type) {
    case LEV_START:
      if (size < 24 || E->b < 0 || E->b > 4096) { return -2; }
      s = 24 + ((E->b + 3) & -4);
      if (size < s) { return -2; }
      return text_le_start ((struct lev_start *) E) >= 0 ? s : -1;
    case LEV_NOOP:
    case LEV_TIMESTAMP:
    case LEV_ROTATE_FROM:
    case LEV_ROTATE_TO:
      return default_replay_logevent (E, size);
    case LEV_CRC32:
      if (size < sizeof (struct lev_crc32)) { return -2; }
      dump_crc32 ((struct lev_crc32 *) E);
      return default_replay_logevent (E, size);
    case LEV_TX_ADD_MESSAGE ... LEV_TX_ADD_MESSAGE + 0xff:
    case LEV_TX_ADD_MESSAGE_MF ... LEV_TX_ADD_MESSAGE_MF + 0xfff:
    case LEV_TX_ADD_MESSAGE_LF:
      if (size < sizeof (struct lev_add_message)) { return -2; }
      EM = (void *) E;
      s = sizeof (struct lev_add_message) + EM->text_len + 1;
      if (size < s) { return -2; }
      if ((unsigned) EM->text_len >= MAX_TEXT_LEN || EM->text[EM->text_len]) { 
        return -4; 
      }
      //store_new_message (EM, 0);
      return s;
    case LEV_TX_ADD_MESSAGE_EXT ... LEV_TX_ADD_MESSAGE_EXT + MAX_EXTRA_MASK:
    case LEV_TX_ADD_MESSAGE_EXT_ZF ... LEV_TX_ADD_MESSAGE_EXT_ZF + MAX_EXTRA_MASK:
      if (size < sizeof (struct lev_add_message)) { return -2; }
      EM = (void *) E;
      t = extra_mask_intcount (E->type & MAX_EXTRA_MASK) * 4;
      s = sizeof (struct lev_add_message) + t + EM->text_len + 1;
      if (size < s) { return -2; }
      if ((unsigned) EM->text_len >= MAX_TEXT_LEN || EM->text[t + EM->text_len]) { 
        return -4; 
      }
      //store_new_message (EM, 0);
      return s;
    case LEV_TX_REPLACE_TEXT ... LEV_TX_REPLACE_TEXT + 0xfff:
      if (size < sizeof (struct lev_replace_text)) { return -2; }
      t = E->type & 0xfff;
      s = offsetof (struct lev_replace_text, text) + t + 1; 
      if (size < s) { return -2; }
      if ((unsigned) t >= MAX_TEXT_LEN || ((struct lev_replace_text *) E)->text[t]) {
        return -4;
      }
      //replace_message_text ((struct lev_replace_text_long *) E);
      return s;
    case LEV_TX_REPLACE_TEXT_LONG:
      if (size < sizeof (struct lev_replace_text_long)) { return -2; }
      t = ((struct lev_replace_text_long *) E)->text_len;
      s = offsetof (struct lev_replace_text_long, text) + t + 1; 
      if (size < s) { return -2; }
      if ((unsigned) t >= MAX_TEXT_LEN || ((struct lev_replace_text_long *) E)->text[t]) {
        return -4;
      }
      //replace_message_text ((struct lev_replace_text_long *) E);
      return s;
    case LEV_TX_DEL_MESSAGE:
      if (size < 12) { return -2; }
      //adjust_message (E->a, E->b, -1, -1);
      return 12;
    case LEV_TX_DEL_FIRST_MESSAGES:
      if (size < 12) { return -2; }
      //delete_first_messages (E->a, E->b);
      return 12;
    case LEV_TX_SET_MESSAGE_FLAGS ... LEV_TX_SET_MESSAGE_FLAGS+0xff:
      if (size < 12) { return -2; }
      //adjust_message (E->a, E->b, ~(E->type & 0xff), E->type & 0xff);
      return 12;
    case LEV_TX_SET_MESSAGE_FLAGS_LONG:
      if (size < 16) { return -2; }
      //adjust_message (E->a, E->b, ~(E->c & 0xffff), (E->c & 0xffff));
      return 16;
    case LEV_TX_INCR_MESSAGE_FLAGS ... LEV_TX_INCR_MESSAGE_FLAGS+0xff:
      if (size < 12) { return -2; }
      dump_incr_message_flags (E);
      //adjust_message (E->a, E->b, 0, E->type & 0xff);
      return 12;
    case LEV_TX_INCR_MESSAGE_FLAGS_LONG:
      if (size < 16) { return -2; }
      //adjust_message (E->a, E->b, 0, (E->c & 0xffff));
      return 16;
    case LEV_TX_DECR_MESSAGE_FLAGS ... LEV_TX_DECR_MESSAGE_FLAGS+0xff:
      if (size < 12) { return -2; }
      //adjust_message (E->a, E->b, E->type & 0xff, 0);
      return 12;
    case LEV_TX_DECR_MESSAGE_FLAGS_LONG:
      if (size < 16) { return -2; }
      //adjust_message (E->a, E->b, E->c & 0xffff, 0);
      return 16;
    case LEV_TX_SET_EXTRA_FIELDS ... LEV_TX_SET_EXTRA_FIELDS + MAX_EXTRA_MASK:
      s = 12 + 4 * extra_mask_intcount (E->type & MAX_EXTRA_MASK);
      if (size < s) { return -2; }
      /*
      V = alloc_value_data (E->type & MAX_EXTRA_MASK);
      V->zero_mask = V->fields_mask;
      memcpy (V->data, ((struct lev_set_extra_fields *) E)->extra, s - 12);
      adjust_message_values (E->a, E->b, V);
      */
      return s;
    case LEV_TX_INCR_FIELD ... LEV_TX_INCR_FIELD + 7:
      if (size < 16) { return -2; }
      /*
      V = alloc_value_data (1 << (E->type & 7));
      V->zero_mask = 0;
      V->data[0] = E->c;
      adjust_message_values (E->a, E->b, V);
      */
      return 16;
    case LEV_TX_INCR_FIELD_LONG + 8 ... LEV_TX_INCR_FIELD_LONG + 11:
      if (size < 20) { return -2; }
      /*
      V = alloc_value_data (0x100 << (E->type & 3));
      V->zero_mask = 0;
      V->data[0] = E->c;
      V->data[1] = E->d;
      adjust_message_values (E->a, E->b, V);
      */
      return 20;
    case LEV_CHANGE_FIELDMASK_DELAYED:
      if (size < 8) { return -2; }
      //change_extra_mask (E->a);
      return 8;
  }

  kprintf ("unknown log event type %08x at position %lld\n", E->type, log_cur_pos ());

  return -3;

}


void usage() {
  fprintf (stderr, "text-binlog [-p] [-t] [-v] [-h] [-S<start-time>] [-T<end-time] <binlog>\n"
                   "\tConverts text into text format.\n"
                   "\tUnfinished version (only LEV_TX_INCR_MESSAGE_FLAGS logevents).\n"
                   "\t-p\tdump log pos\n"
                   "\t-t\tdump timestamp\n"
                   "\t-C\tdump CRC32\n"
                   "\t-S<start-time>\tsets start-time\n"
                   "\t-T<end-time>\tsets end-time\n");
  exit (2);
}

int main (int argc, char *argv[]) {
  int i;
  long long jump_log_pos = 0;
  out = stdout;
  while ((i = getopt (argc, argv, "tphvCS:T:")) != -1) {
    switch (i) {
    case 'C':
      dumping_crc32 = 1;
      break;
    case 'S':
      start_time = atoi (optarg);
      break;
    case 'T':
      end_time = atoi (optarg);
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
      usage ();
    }
  }

  if (optind >= argc) {
    usage ();
  }

  if (engine_preload_filelist (argv[optind], binlogname) < 0) {
    kprintf ("cannot open binlog files for %s\n", binlogname ? binlogname : argv[optind]);
    exit (1);
  }

  vkprintf (3, "engine_preload_filelist done\n");

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

  fflush (out);
  fclose (out);

  vkprintf (1, "replay log events finished\n");

  binlog_load_time += mytime();
  binlog_loaded_size = log_readto_pos - jump_log_pos;

  return 0;
}
