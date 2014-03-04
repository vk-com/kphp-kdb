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
                   2012 Yury Petrov
                   2013 Aliaksei Levin
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
#include "kdb-hints-binlog.h"
#include "kfs.h"
#include "hints-data.h"

#define	READ_BUFFER_SIZE	(1L << 24)
#define	WRITE_BUFFER_SIZE	(1L << 24)
#define	READ_THRESHOLD		(1L << 17)
#define	WRITE_THRESHOLD		(1L << 18)

char *progname = "hints-log-split", *username, *targ_fname;
int verbosity = 0;
int now;
int targ_fd;
int targ_existed;
long long rd_bytes, wr_bytes, rd_rec, wr_rec, true_rd_bytes, true_wr_bytes;

int split_min, split_max, split_mod, copy_rem, copy_mod = 1;
long long position, targ_orig_size, jump_log_pos;

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

static int get_logrec_size (int type, void *ptr, int size) {
  struct lev_generic *E = ptr;
  int s;
  switch (type) {
  case LEV_START:
    if (size < 24 || E->b < 0 || E->b > 4096) { return -2; }
    s = 24 + ((E->b + 3) & -4);
    if (size < s) { return -2; }
    if (E->a != HINTS_SCHEMA_V1) {
      fprintf (stderr, "fatal: HINTS_SCHEMA_V1 expected\n");
    }
    if (split_mod) {
      assert (E->c >= split_mod && !(E->c % split_mod) && split_min == E->d % split_mod && E->e == E->d + 1);
    }
    split_mod = E->c;
    split_min = E->d;
    split_max = E->e;
    assert (split_min >= 0 && split_max == split_min + 1 && split_max <= split_mod);
    if (verbosity > 0) {
      fprintf (stderr, "splitting %d..%d %% %d -> %d %% %d\n", split_min, split_max-1, split_mod, copy_rem, copy_mod);
    }
    return s;
  case LEV_NOOP:
    return 4;
  case LEV_TIMESTAMP:
  case LEV_CRC32:
  case LEV_ROTATE_FROM:
  case LEV_ROTATE_TO:
    if (size >= 8) {
      LogTs.timestamp = E->a;
    }
    return default_replay_logevent (E, size);
  case LEV_HINTS_ADD_USER_OBJECT:
    if (size < (int)sizeof (struct lev_hints_add_user_object)) {
      return -2;
    }
    s = ((struct lev_hints_add_user_object *) E)->text_len;
    if (s < -1 || s > 4095) {
      struct lev_hints_add_user_object *EE = (struct lev_hints_add_user_object *)E;
      fprintf (stderr, "Bad log event, user_id = %d, object_id = %d, object_type = %d, text_len = %d\n", EE->user_id, EE->object_id, EE->object_type, EE->text_len);
    }
    if (s < -1) {
      return -4;
    }
    s += 1 + offsetof (struct lev_hints_add_user_object, text);
    if (size < s) {
      return -2;
    }
    return s;
  case LEV_HINTS_SET_USER_OBJECT_TYPE ... LEV_HINTS_SET_USER_OBJECT_TYPE + 0xffff:
    return 12;
  case LEV_HINTS_ADD_OBJECT_TEXT:
    if (size < (int)sizeof (struct lev_hints_add_object_text)) {
      return -2;
    }
    s = ((struct lev_hints_add_object_text *) E)->text_len;
    if (s < 0 || s > 4095) {
      struct lev_hints_add_object_text *EE = (struct lev_hints_add_object_text *)E;
      fprintf (stderr, "Bad log event, object_id = %d, object_type = %d, text_len = %d\n", EE->object_id, EE->object_type, EE->text_len);
    }
    if (s < 0) {
      return -4;
    }
    s += 1 + offsetof (struct lev_hints_add_object_text, text);
    if (size < s) {
      return -2;
    }
    return s;
  case LEV_HINTS_SET_OBJECT_TYPE ... LEV_HINTS_SET_OBJECT_TYPE + 0xffff:
    return 8;
  case LEV_HINTS_DEL_USER_OBJECT ... LEV_HINTS_DEL_USER_OBJECT + 0xff:
    return 12;
  case LEV_HINTS_DEL_OBJECT_TEXT ... LEV_HINTS_DEL_OBJECT_TEXT + 0xff:
    return 8;
  case LEV_HINTS_SET_USER_INFO ... LEV_HINTS_SET_USER_INFO + 0xff:
    return 8;
  case LEV_HINTS_SET_USER_OBJECT_RATING + 1 ... LEV_HINTS_SET_USER_OBJECT_RATING + 0xff + ((MAX_RATING_NUM - 1) << 8):
    return 16;
  case LEV_HINTS_INCREMENT_USER_OBJECT_RATING ... LEV_HINTS_INCREMENT_USER_OBJECT_RATING + 0xff + ((MAX_RATING_NUM - 1) << 8):
    return 16;
  case LEV_HINTS_INCREMENT_USER_OBJECT_RATING_CHAR ... LEV_HINTS_INCREMENT_USER_OBJECT_RATING_CHAR + 0xffff + ((MAX_RATING_NUM - 1) << 16):
    return 12;
  case LEV_HINTS_NULLIFY_USER_RATING:
    return 8;
  case LEV_HINTS_SET_USER_RATING_STATE ... LEV_HINTS_SET_USER_RATING_STATE + 0xff:
    if (size < (int)sizeof (struct lev_hints_set_user_rating_state)) {
      return -2;
    }
    return sizeof (struct lev_hints_set_user_rating_state);
  case LEV_HINTS_USER_OBJECT_WINNER ... LEV_HINTS_USER_OBJECT_WINNER + MAX_RATING_NUM - 1:
    if (size < (int)sizeof (struct lev_hints_user_object_winner)) {
      return -2;
    }
    s = ((struct lev_hints_user_object_winner *) E)->losers_cnt * sizeof (int);
    if (s < 0 || s > 65536) {
      return -4;
    }
    s += offsetof (struct lev_hints_user_object_winner, losers);
    if (size < s) {
      return -2;
    }
    return s;
  case LEV_HINTS_ADD_USER_OBJECT_RATING + 1 ... LEV_HINTS_ADD_USER_OBJECT_RATING + MAX_RATING_NUM:
    if (size < (int)sizeof (struct lev_hints_add_user_object_rating)) {
      return -2;
    }
    s = ((struct lev_hints_add_user_object_rating *) E)->text_len;
    if (s < -1 || s > 4095 + MAX_RATING_NUM * (int)sizeof (float)) {
      struct lev_hints_add_user_object_rating *EE = (struct lev_hints_add_user_object_rating *)E;
      fprintf (stderr, "Bad log event, user_id = %d, object_id = %d, object_type = %d, text_len = %d\n", EE->user_id, EE->object_id, EE->object_type, EE->text_len);
    }
    if (s < -1) {
      return -4;
    }
    s += 1 + offsetof (struct lev_hints_add_user_object_rating, text);
    if (size < s) {
      return -2;
    }
    return s;
  }

  return -1;
}

static int want_write (int type, void *rec) {
  int user_id = 0;
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
  case LEV_HINTS_ADD_USER_OBJECT:
  case LEV_HINTS_DEL_USER_OBJECT ... LEV_HINTS_DEL_USER_OBJECT + 0xff:
  case LEV_HINTS_SET_USER_INFO ... LEV_HINTS_SET_USER_INFO + 0xff:
  case LEV_HINTS_SET_USER_OBJECT_RATING + 1 ... LEV_HINTS_SET_USER_OBJECT_RATING + 0xff + ((MAX_RATING_NUM - 1) << 8):
  case LEV_HINTS_INCREMENT_USER_OBJECT_RATING ... LEV_HINTS_INCREMENT_USER_OBJECT_RATING + 0xff + ((MAX_RATING_NUM - 1) << 8):
  case LEV_HINTS_INCREMENT_USER_OBJECT_RATING_CHAR ... LEV_HINTS_INCREMENT_USER_OBJECT_RATING_CHAR + 0xffff + ((MAX_RATING_NUM - 1) << 16):
  case LEV_HINTS_NULLIFY_USER_RATING:
  case LEV_HINTS_SET_USER_RATING_STATE ... LEV_HINTS_SET_USER_RATING_STATE + 0xff:
  case LEV_HINTS_USER_OBJECT_WINNER ... LEV_HINTS_USER_OBJECT_WINNER + MAX_RATING_NUM - 1:
  case LEV_HINTS_ADD_USER_OBJECT_RATING + 1 ... LEV_HINTS_ADD_USER_OBJECT_RATING + MAX_RATING_NUM:
    user_id = ((struct lev_generic *) rec)->a;
    break;
  case LEV_HINTS_ADD_OBJECT_TEXT:
  case LEV_HINTS_DEL_OBJECT_TEXT ... LEV_HINTS_DEL_OBJECT_TEXT + 0xff:
    return 1;
  default:
    fprintf (stderr, "unknown record type %08x\n", type);
    break;
  }
  if (user_id > 0) {
    int t = user_id;
    if (t < 0) {
      t = -t;
    }
    if (t && t % copy_mod == copy_rem) {
      return 1;
    }
  }
  return 0;
}

int hints_replay_logevent (struct lev_generic *E, int size) {
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
    fprintf (stderr, "error %d reading binlog at position %lld, write position %lld, type %08x (prev type %08x)\n", s, log_cur_pos(), wr_bytes + targ_orig_size, type, type_ok);
    return s;
  }

  if (!targ_existed && type != LEV_START && jump_log_pos == 0) {
    fprintf (stderr, "error: first record must be a LEV_START\n");
    return -1;
  }

  rd_bytes += s;
  rd_rec++;

  type_ok = type;

  s = ((s + 3) & -4);

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
    }
    wr_bytes += s;
    wr_rec++;
  }

  return s;
}


int init_hints_data (int schema) {

  replay_logevent = hints_replay_logevent;

  return 0;
}


/*
 * END MAIN
 */

void output_stats (void) {
  fprintf (stderr,
	   "read: %lld bytes (reported binlog position %lld), %lld records\n"
	   "written: %lld bytes, %lld records\n",
	   rd_bytes, log_cur_pos(), rd_rec, wr_bytes, wr_rec);
}


void usage (void) {
  fprintf (stderr, "usage:\t%s [-v] [-u<username>] [-m<rem>,<mod>] <old-binlog-file> [<output-file>]\n"
	   "\tCopies (some of) search records to another binlog. "
	   "If <output-file> is specified, resulting binlog is appended to it.\n"
	   "\t-h\tthis help screen\n"
	   "\t-v\tverbose mode on\n"
	   "\t-m<rem>,<mod>\tcopy only record with id %% <mod> == <rem>\n"
	   "\t-s<start-binlog-pos>\tstart reading binlog from specified position\n"
	   "\t-t<stop-binlog-pos>\tstop reading binlog at specified position\n"
	   "\t-u<username>\tassume identity of given user\n",
	   progname);
}

int main (int argc, char *argv[]) {
  int i;
  progname = argv[0];
  while ((i = getopt (argc, argv, "hvu:m:s:t:")) != -1) {
    switch (i) {
    case 'v':
      verbosity = 1;
      break;
    case 'h':
      usage();
      return 2;
    case 'u':
      username = optarg;
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
      log_limit_pos = atoll (optarg);
      break;
    }
  }

  if (optind >= argc || optind + 2 < argc) {
    usage();
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

  Binlog = open_binlog (engine_replica, jump_log_pos);
  if (!Binlog) {
    fprintf (stderr, "fatal: cannot find binlog for %s, log position %lld\n", engine_replica->replica_prefix, 0LL);
    exit (1);
  }

  binlogname = Binlog->info->filename;

  if (verbosity) {
    fprintf (stderr, "replaying binlog file %s (size %lld)\n", binlogname, Binlog->info->file_size);
  }

  clear_log();

  init_log_data (jump_log_pos, 0, 0);

  if (jump_log_pos > 0) {
    init_hints_data (0);
  }

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

  i = replay_log (0, 1);

  if (i < 0) {
    fprintf (stderr, "fatal: error reading binlog\n");
    exit (1);
  }

  if (log_limit_pos >= 0 && log_readto_pos != log_limit_pos) {
    fprintf (stderr, "fatal: binlog read up to position %lld instead of %lld\n", log_readto_pos, log_limit_pos);
    exit (1);
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

  fprintf (stderr, "%lld\n", log_readto_pos);

  return 0;
}
