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

    Copyright 2011-2012 Vkontakte Ltd
              2011-2012 Nikolai Durov
              2011-2012 Andrei Lopatin
*/

#define	_FILE_OFFSET_BITS	64

#include <assert.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <fcntl.h>
#include <unistd.h>
#include <time.h>

#include "server-functions.h"
#include "kdb-targ-binlog.h"
#include "kfs.h"

#define	READ_BUFFER_SIZE	(1L << 24)
#define	WRITE_BUFFER_SIZE	(1L << 24)
#define	READ_THRESHOLD		(1L << 17)
#define	WRITE_THRESHOLD		(1L << 18)

#define MAX_AD_ID	(1 << 24)
#define MAX_PEND_VIEWS	1000000

char *progname = "targ-log-split", *username, *targ_fname;
int verbosity = 0;
int now;
int targ_fd;
int targ_existed;
long long rd_bytes, wr_bytes, rd_rec, wr_rec, true_rd_bytes, true_wr_bytes, rd_views_rec, wr_views_rec;

int split_min, split_max, split_mod, copy_rem, copy_mod = 1, split_quotient, split_shift;
long long position, targ_orig_size, jump_log_pos, keep_log_limit_pos = -1;
int skip_rotate = 0;

int Views[MAX_AD_ID];

struct lev_timestamp LogTs = { .type = LEV_TIMESTAMP };

char *wptr, *wst;
char WB[WRITE_BUFFER_SIZE+4];

void flush_out (void) {
  int a, b = wptr - wst;
  assert (b >= 0);
  if (!b) {
    wptr = wst = WB;
    return;
  }
  a = write (targ_fd, wst, b);
  if (a > 0) {
    true_wr_bytes += a;
  }
  if (a < b) {
    fprintf (stderr, "error writing to %s: %d bytes written out of %d: %m\n", targ_fname, a, b);
    exit(3);
  }
  if (verbosity > 0) {
    fprintf (stderr, "%d bytes written to %s\n", a, targ_fname);
  }
  wptr = wst = WB;
}

void prepare_write (int x) {
  if (x < 0) {
    x = WRITE_THRESHOLD;
  }
  assert (x > 0 && x <= WRITE_THRESHOLD);
  if (!wptr) {
    wptr = wst = WB;
  }
  if (WB + WRITE_BUFFER_SIZE - wptr < x) {
    flush_out();
  }
}

void *write_alloc (int s) {
  char *p;
  int t = (s + 3) & -4;
  assert (s > 0 && s <= WRITE_THRESHOLD);
  prepare_write (t);
  p = wptr;
  wptr += t;
  while (s < t) {
    p[s++] = LEV_ALIGN_FILL;
  }
  return p;
}

/*
 *
 *
 */

static int targ_le_start (struct lev_start *E) {
  if (E->schema_id != TARG_SCHEMA_V1) {
    return -1;
  }
  if (split_mod) {
    return 0;
//    assert (E->split_mod >= split_mod && !(E->split_mod % split_mod) && split_min == E->split_min % split_mod && E->split_max == E->split_min + 1);
  }
  split_min = E->split_min;
  split_max = E->split_max;
  split_mod = E->split_mod;
  assert (split_mod > 0 && split_min >= 0 && split_min + 1 == split_max && split_max <= split_mod);
  if (split_quotient) {
    assert (copy_mod / split_quotient % split_mod == 0);
  } else {
    assert (copy_mod % split_mod == 0);
    split_quotient = copy_mod / split_mod;
  }
  split_shift = copy_rem / (copy_mod / split_quotient);

  return 0;
}

int immediate_exit;

static int get_logrec_size (int type, void *ptr, int size) {
  struct lev_generic *E = ptr;
  int s, t;
  switch (type) {
  case LEV_START:
    if (size < 24 || E->b < 0 || E->b > 4096) { 
      return -2; 
    }
    s = 24 + ((E->b + 3) & -4);
    if (size < s) { 
      return -2; 
    }
    targ_le_start ((struct lev_start *) E);
    if (verbosity > 0) {
      fprintf (stderr, "splitting %d %% %d -> %d %% %d ; split_shift/shift_quotient = %d/%d\n", split_min, split_mod, copy_rem, copy_mod, split_shift, split_quotient);
    }
    if (immediate_exit) {
      return -13;
    }
    return s;
  case LEV_NOOP:
    return 4;
  case LEV_TIMESTAMP:
  case LEV_CRC32:
    if (size >= 8) {
      LogTs.timestamp = E->a;
    }
    return default_replay_logevent (E, size);
  case LEV_ROTATE_FROM:
  case LEV_ROTATE_TO:
    vkprintf (2, "found LEV_ROTATE_TO, size = %d, skip=%d\n", size, skip_rotate);
    if (size >= 8) {
      LogTs.timestamp = E->a;
    }
    if (skip_rotate) {
      return 36;
    }
    return default_replay_logevent (E, size);
  case LEV_TARG_USERNAME ... LEV_TARG_USERNAME+0xff:
    return (type & 0xff) + 9;
  case LEV_TARG_USERFLAGS ... LEV_TARG_USERFLAGS+0xff:
    return 8;
  case LEV_TARG_POLITICAL:
  case LEV_TARG_MSTATUS:
  case LEV_TARG_SEX:
  case LEV_TARG_OPERATOR:
  case LEV_TARG_BROWSER:
  case LEV_TARG_REGION:
  case LEV_TARG_HEIGHT:
  case LEV_TARG_SMOKING:
  case LEV_TARG_ALCOHOL:
  case LEV_TARG_PPRIORITY:
  case LEV_TARG_IIOTHERS:
  case LEV_TARG_CVISITED:
  case LEV_TARG_GCOUNTRY:
  case LEV_TARG_TIMEZONE:
  case LEV_TARG_PRIVACY:
    return 12;
  case LEV_TARG_UNIVCITY:
    return 16;
  case LEV_TARG_BIRTHDAY:
    return 12;
  case LEV_TARG_RATECUTE:
    return 16;
  case LEV_TARG_RATE:
    return 12;
  case LEV_TARG_CUTE:
    return 12;
  case LEV_TARG_ONLINE:
    return 16;
  case LEV_TARG_DELUSER:
    return 8;
  case LEV_TARG_PROPOSAL_DEL:
    return 8;
  case LEV_TARG_EDUCLEAR:
    return 8;
  case LEV_TARG_EDUADD ... LEV_TARG_EDUADD_PRIM:
    return 32;
  case LEV_TARG_RELIGION ... LEV_TARG_RELIGION+0xff:
    return (type & 0xff) + 9;
  case LEV_TARG_HOMETOWN ... LEV_TARG_HOMETOWN+0xff:
    return (type & 0xff) + 9;
  case LEV_TARG_SCH_CLEAR:
    return 8;
  case LEV_TARG_SCH_ADD ... LEV_TARG_SCH_ADD+0xff:
    return (type & 0xff) + 26;
  case LEV_TARG_COMP_CLEAR:
    return 8;
  case LEV_TARG_COMP_ADD ... LEV_TARG_COMP_ADD+0x1ff:
    return (type & 0x1ff) + 22;
  case LEV_TARG_ADDR_CLEAR:
    return 8;
  case LEV_TARG_ADDR_ADD:
    return 28;
  case LEV_TARG_ADDR_EXT_ADD ... LEV_TARG_ADDR_EXT_ADD + 0xff:
    return (type & 0xff) + 27;
  case LEV_TARG_GRTYPE_CLEAR:
    return 8;
  case LEV_TARG_GRTYPE_ADD ... LEV_TARG_GRTYPE_ADD+0x7f:
    return 8;
  case LEV_TARG_INTERESTS_CLEAR ... LEV_TARG_INTERESTS_CLEAR + MAX_INTERESTS:
    return 8;
  case LEV_TARG_INTERESTS + 1 ... LEV_TARG_INTERESTS + MAX_INTERESTS:
    if (size < sizeof (struct lev_interests)) {
      return -2;
    }
    return ((struct lev_interests *) E)->len + 11;
  case LEV_TARG_PROPOSAL:
    if (size < sizeof (struct lev_proposal)) {
      return -2;
    }
    return ((struct lev_proposal *) E)->len + 11;
  case LEV_TARG_MIL_CLEAR:
    return 8;
  case LEV_TARG_MIL_ADD:
    return 16;
  case LEV_TARG_GROUP_CLEAR:
    return 8;
  case LEV_TARG_GROUP_ADD ... LEV_TARG_GROUP_ADD + 0xff:
  case LEV_TARG_GROUP_DEL ... LEV_TARG_GROUP_DEL + 0xff:
    t = type & 0xff;
    return 8 + t * 4;
  case LEV_TARG_GROUP_EXT_ADD:
  case LEV_TARG_GROUP_EXT_DEL:
    if (size < sizeof (struct lev_groups_ext)) {
      return -2;
    }
    t = ((struct lev_groups_ext *) E)->gr_num;
    if (t < 0 || t > 0x10000) { 
      return -1; 
    }
    return 12 + t * 4;
  case LEV_TARG_LANG_CLEAR:
    return 8;
  case LEV_TARG_LANG_ADD + 1 ... LEV_TARG_LANG_ADD + 0xff:
    t = E->type & 0xff;
    return 8 + t * 4;
  case LEV_TARG_LANG_DEL + 1 ... LEV_TARG_LANG_DEL + 0xff:
    t = E->type & 0xff;
    return 8 + t * 4;
  case LEV_TARG_TARGET:
    if (size < sizeof (struct lev_targ_target)) {
      return -2;
    }
    return ((struct lev_targ_target *) E)->ad_query_len + 15;
  case LEV_TARG_AD_ON:
    return 8;
  case LEV_TARG_AD_OFF:
    return 8;
  case LEV_TARG_AD_PRICE:
    return 12;
  case LEV_TARG_CLICK:
    return 12;
  case LEV_TARG_CLICK_EXT:
    return 16;
  case LEV_TARG_VIEWS:
    return 12;
  case LEV_TARG_STAT_LOAD:
    return sizeof (struct lev_targ_stat_load);
  case LEV_TARG_USER_VIEW:
    return 12;
  case LEV_TARG_ONLINE_LITE:
    return 8;
  case LEV_TARG_AD_RETARGET:
    return 8;
  case LEV_TARG_AD_SETAUD:
    return 12;
  case LEV_TARG_AD_SETCTR:
    return 20;
  case LEV_TARG_AD_SETCTR_PACK ... LEV_TARG_AD_SETCTR_PACK + 0xff:
    return 12;
  case LEV_TARG_AD_SETSUMP:
    return 32;
  case LEV_TARG_AD_LIMIT_USER_VIEWS ... LEV_TARG_AD_LIMIT_USER_VIEWS + 0xffff:
    return 8;
  case LEV_TARG_AD_DO_NOT_ALLOW_SITES ... LEV_TARG_AD_DO_ALLOW_SITES:
    return 8;
  case LEV_TARG_AD_SETFACTOR:
    return 12;
  case LEV_TARG_AD_LIMIT_RECENT_VIEWS ... LEV_TARG_AD_LIMIT_RECENT_VIEWS + 0xffff:
    return 8;
  case LEV_TARG_GLOBAL_CLICK_STATS:
    if (size < 8) { return -2; }
    if (E->a > 65536 || E->a < 64) { return -1; }
    if (size < 8 + 16 * E->a) { return -2; }
    return 8 + 16 * E->a;
  default:
    fprintf (stderr, "unknown record type %08x\n", type);
    break;
  }
   
  return -1;
}

int split_par;

void adjust_split_params (void) {
  split_par = (split_shift + (log_cur_pos() >> 2)) % split_quotient;
  if (split_par < 0) {
    split_par = -split_par;
  }
}

int split_int (int x) {
  return x >= 0 ? (x + split_par) / split_quotient : (x - split_par) / split_quotient;
}

static void flush_views (unsigned ad_id) {
  if (ad_id >= MAX_AD_ID || !Views[ad_id]) {
    return;
  }
  struct lev_targ_views *E = write_alloc (12);
  E->type = LEV_TARG_VIEWS;
  E->ad_id = ad_id;
  E->views = Views[ad_id];
  Views[ad_id] = 0;
  wr_views_rec++;
  wr_rec++;
  wr_bytes += 12;
}

static void incr_views (int ad_id, int views) {
  if ((Views[ad_id] += views) >= MAX_PEND_VIEWS) {
    flush_views (ad_id);
  }
}

static int want_write (int type, void *rec) {
  int user_id = 0, ad_id, views;
  switch (type) {
  case LEV_START:
    return !targ_existed++ && !jump_log_pos;
  case LEV_NOOP:
    return 0;
  case LEV_TIMESTAMP:
  case LEV_CRC32:
  case LEV_ROTATE_TO:
  case LEV_ROTATE_FROM:
    return 0;
  case LEV_TARG_TARGET:
  case LEV_TARG_AD_ON:
  case LEV_TARG_AD_OFF:
  case LEV_TARG_AD_PRICE:
    flush_views (((struct lev_targ_ad_on *) rec)->ad_id);
    return 1;
  case LEV_TARG_CLICK:
  case LEV_TARG_CLICK_EXT:
  case LEV_TARG_USER_VIEW:
    user_id = ((struct lev_targ_click *) rec)->user_id;
    break;
  case LEV_TARG_VIEWS:
    rd_views_rec++;
    adjust_split_params ();
    ad_id = ((struct lev_targ_views *) rec)->ad_id;
    views = split_int (((struct lev_targ_views *) rec)->views);
    if ((unsigned) ad_id < MAX_AD_ID && views > 0) {
      incr_views (ad_id, views);
      return 0;
    }
    return views > 0;
  case LEV_TARG_STAT_LOAD:
    flush_views (((struct lev_targ_stat_load *) rec)->ad_id);
    adjust_split_params ();
    return 1;
  case LEV_TARG_AD_RETARGET:
  case LEV_TARG_AD_SETAUD:
  case LEV_TARG_AD_SETCTR:
  case LEV_TARG_AD_SETCTR_PACK ... LEV_TARG_AD_SETCTR_PACK + 0xff:
  case LEV_TARG_GLOBAL_CLICK_STATS:
    return 1;
  default:
    user_id = ((struct lev_user_generic *) rec)->user_id;
  }
  if (user_id > 0 && user_id % copy_mod == copy_rem) {
    return 1;
  }
  return 0;
}

int targ_replay_logevent (struct lev_generic *E, int size) {
  int type, s;
  static int type_ok = -1;

  if (size < 4) {
    return -2;
  }

  type = *((int *)E);
  s = get_logrec_size (type, E, size);

  if (s > size || s == -2) {
    return -2;
  }

  if (s < 0) {
    if (s != -13) {
      fprintf (stderr, "error %d reading binlog at position %lld, write position %lld, type %08x (prev type %08x)\n", s, log_cur_pos(), wr_bytes + targ_orig_size, type, type_ok);
    }
    return s;
  }

  if (!targ_existed && type != LEV_START && jump_log_pos == 0) {
    fprintf (stderr, "error: first record must be a LEV_START\n");
    return -1;
  }

  if (immediate_exit) {
    fprintf (stderr, "error: first record in a binlog must be LEV_START\n");
    exit (1);
  }

  s = ((s + 3) & -4);

  rd_bytes += s;
  rd_rec++;

  type_ok = type;

  if (want_write (type, E)) {
    if (LogTs.timestamp) {
      memcpy (write_alloc (8), &LogTs, 8);
      wr_bytes += 8;
      wr_rec++;
      LogTs.timestamp = 0;
    }
    struct lev_generic *N = write_alloc (s);
    memcpy (N, E, s);
    if (type == LEV_START) {
      N->c = copy_mod;
      N->d = copy_rem;
      N->e = copy_rem+1;
    } else if (type == LEV_TARG_VIEWS) {
      wr_views_rec++;
      N->b = split_int (N->b);
    } else if (type == LEV_TARG_STAT_LOAD) {
      int i, *z = (int *) N;
      for (i = 2; i < 7; i++) {
        z[i] = split_int (z[i]);
      }
    }
    wr_bytes += s;
    wr_rec++;
  }

  return s;
}


int init_targ_data (int schema) {

  replay_logevent = targ_replay_logevent;

  return 0;
}


/*
 * END MAIN
 */

void output_stats (void) {
  fprintf (stderr,
	   "read: %lld bytes (reported binlog position %lld), %lld records, out of them %lld ad_view\n"
	   "written: %lld bytes, %lld records, out of them %lld ad_view\n",
	   rd_bytes, log_cur_pos(), rd_rec, rd_views_rec, wr_bytes, wr_rec, wr_views_rec);
}


void usage (void) {
  fprintf (stderr, "usage:\t%s [-v] [-u<username>] [-m<rem>,<mod>] <old-binlog-file> [<output-file>]\n"
	   "\tCopies (some of) targeting engine binlog records to another binlog. "
	   "If <output-file> is specified, resulting binlog is appended to it.\n"
	   "\t-h\tthis help screen\n"
	   "\t-v\tverbose mode on\n"
           "\t-f\tskip LEV_ROTATE_TO and LEV_ROTATE_FROM entries (useful when binlog is merged in one file)\n"
	   "\t-m<rem>,<mod>\tcopy only record with id %% <mod> == <rem>\n"
	   "\t-q<split-quotient>\tnumber of new binlogs made from one old\n"
	   "\t-s<start-binlog-pos>\tstart reading binlog from specified position\n"
	   "\t-t<stop-binlog-pos>\tstop reading binlog at specified position\n"
	   "\t-u<username>\tassume identity of given user\n",
	   progname);
}

int main (int argc, char *argv[]) {
  int i;
  progname = argv[0];
  while ((i = getopt (argc, argv, "fhvu:m:s:t:q:")) != -1) {
    switch (i) {
    case 'v':
      verbosity += 1;
      break;
    case 'f':
      // vkprintf(2, "setting skip_rotate\n");
      skip_rotate = 1;
      break;
    case 'h':
      usage();
      return 2;
    case 'u':
      username = optarg;
      break;
    case 'q':
      split_quotient = atoi (optarg);
      break;
    case 'm':
      if (sscanf (optarg, "%d,%d", &copy_rem, &copy_mod) != 2 || copy_rem < 0 || copy_rem >= copy_mod) {
	usage();
	return 2;
      }
      break;
    case 's':
      jump_log_pos = atoll (optarg);
      break;
    case 't':
      keep_log_limit_pos = log_limit_pos = atoll (optarg);
      break;
    }
  }

  if (optind >= argc || optind + 2 < argc || !copy_mod) {
    usage ();
    return 2;
  }

  if (split_quotient > 0 && copy_mod % split_quotient != 0) {
    usage ();
    return 2;
  }

  if (log_limit_pos >= 0) {
    if (jump_log_pos > log_limit_pos) {
      fprintf (stderr, "fatal: log start position %lld after stop position %lld\n", jump_log_pos, log_limit_pos);
      return 2;
    }
  }

  if (username && change_user (username) < 0) {
    fprintf (stderr, "fatal: cannot change user to %s\n", username ? username : "(none)");
    return 1;
  }

  if (engine_preload_filelist (argv[optind], binlogname) < 0) {
    fprintf (stderr, "cannot open binlog files for %s\n", binlogname ? binlogname : argv[optind]);
    exit (1);
  }

  Binlog = open_binlog (engine_replica, 0);
  if (!Binlog) {
    fprintf (stderr, "fatal: cannot find binlog for %s, log position %lld\n", engine_replica->replica_prefix, 0LL);
    exit (1);
  }

  binlogname = Binlog->info->filename;

  if (verbosity) {
    fprintf (stderr, "replaying binlog file %s (size %lld)\n", binlogname, Binlog->info->file_size);
  }

  clear_log ();

  init_log_data (0, 0, 0);

  if (optind + 1 < argc) {
    targ_fname = argv[optind+1];
    targ_fd = open (targ_fname, O_WRONLY | O_APPEND | O_CREAT, 0644);
    if (targ_fd < 0) {
      fprintf (stderr, "cannot create %s: %m\n", targ_fname);
      return 1;
    }
    targ_orig_size = lseek (targ_fd, 0, SEEK_END);
    targ_existed = (targ_orig_size > 0);
  } else {
    targ_fname = "stdout";
    targ_fd = 1;
  }

  if (jump_log_pos > 0) {

    log_limit_pos = 256;
    immediate_exit = 1;
    disable_crc32 = 3;

    i = replay_log (0, 1);

    if (!split_mod) {
      fprintf (stderr, "fatal: cannot parse first LEV_START entry");
      exit (1);
    }

    log_limit_pos = keep_log_limit_pos;
    immediate_exit = 0;

    clear_log ();

    close_binlog (Binlog, 1);
    Binlog = 0;

    Binlog = open_binlog (engine_replica, jump_log_pos);
    if (!Binlog) {
      fprintf (stderr, "fatal: cannot find binlog for %s, log position %lld\n", engine_replica->replica_prefix, jump_log_pos);
      exit (1);
    }

    binlogname = Binlog->info->filename;

    if (verbosity) {
      fprintf (stderr, "replaying binlog file %s (size %lld) from log position %lld\n", binlogname, Binlog->info->file_size, jump_log_pos);
    }
    
    init_log_data (jump_log_pos, 0, 0);
  }

  disable_crc32 = 3;

  i = replay_log (0, 1);

  if (i < 0) {
    fprintf (stderr, "fatal: error reading binlog\n");
    exit (1);
  }

  if (log_limit_pos >= 0 && log_readto_pos != log_limit_pos) {
    fprintf (stderr, "fatal: binlog read up to position %lld instead of %lld\n", log_readto_pos, log_limit_pos);
    exit (1);
  }

  for (i = 0; i < MAX_AD_ID; i++) {
    if (Views[i]) {
      flush_views (i);
    }
  }

  flush_out ();

  if (targ_fd != 1) {
    if (fdatasync (targ_fd) < 0) {
      fprintf (stderr, "error syncing %s: %m", targ_fname);
      exit (1);
    }
    close (targ_fd);
  }

  if (verbosity > 0) {
    output_stats ();
  }

  return 0;
}
