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

    Copyright 2011-2013 Vkontakte Ltd
              2011-2013 Nikolai Durov
              2011-2013 Andrei Lopatin
*/

#define	_FILE_OFFSET_BITS	64

#include <assert.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <stddef.h>
#include <fcntl.h>
#include <unistd.h>
#include <time.h>

#include "server-functions.h"
#include "kdb-text-binlog.h"
#include "kfs.h"
#include "translit.h"

#define	READ_BUFFER_SIZE	(1L << 24)
#define	WRITE_BUFFER_SIZE	(1L << 24)
#define	READ_THRESHOLD		(1L << 17)
#define	WRITE_THRESHOLD		(1L << 18)

char *progname = "text-log-split", *username, *targ_fname;
int verbosity = 0;
int now;
int targ_fd;
int targ_existed;
long long rd_bytes, wr_bytes, rd_rec, wr_rec, conv_rec, true_wr_bytes;

int split_min, split_max, split_mod, copy_rem, copy_mod = 1;
long long position, targ_orig_size, jump_log_pos, keep_log_limit_pos = -1;

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

#define	MAX_MAP_USERS	(1 << 27)
#define MAX_PNAME_LEN	128
#define	MAX_PNAME_TR_LEN	256

char *map_filename;
long long map_fsize;
char *Map;
unsigned *UMap;

int load_map (void) {
  int map_fd = open (map_filename, O_RDONLY);
  if (map_fd < 0) {
    fprintf (stderr, "cannot open user names file %s: %m\n", map_filename);
    exit (1);
  }
  map_fsize = lseek (map_fd, 0, SEEK_END);
  assert (map_fsize >= 0 && map_fsize == (long) map_fsize);
  assert (map_fsize >= MAX_MAP_USERS * 4);
  Map = malloc (map_fsize);
  assert (Map);
  UMap = (unsigned *) Map;

  assert (lseek (map_fd, 0, SEEK_SET) == 0);
  long long rd = 0;
  while (rd < map_fsize) {
    int s = map_fsize - rd < (1 << 30) ? map_fsize - rd : (1 << 30);
    assert (read (map_fd, Map + rd, s) == s);
    rd += s;
  }
  close (map_fd);

  int i;
  assert (UMap[0] == MAX_MAP_USERS * 4);
  for (i = 1; i < MAX_MAP_USERS; i++) {
    assert (UMap[i-1] <= UMap[i]);
  }
  assert (UMap[MAX_MAP_USERS-1] <= map_fsize);
  if (verbosity > 0) {
    fprintf (stderr, "successfully loaded user names file %s, size %lld\n", map_filename, map_fsize);
  }

  return 0;
}


int write_convert_record (struct lev_add_message *E, int size, int text_len) {
  int text_offset = size - text_len - 1;
  char *text = (char *) E + text_offset;
  int peer_id = E->peer_id;
 
  assert (text_offset > 8);
  if ((unsigned) peer_id >= MAX_MAP_USERS-1 || !peer_id || UMap[peer_id] == UMap[peer_id+1]) {
    return 0;
  }
  if (text_len >= 7 && !memcmp (text, "\x2pname ", 7)) {
    return 0;
  }
  char *pname = Map + UMap[peer_id];
  int plen = UMap[peer_id+1] - UMap[peer_id];
  if (plen > MAX_PNAME_LEN) {
    plen = MAX_PNAME_LEN;
  }
  static char pname2_buff[MAX_PNAME_TR_LEN+1];
  translit_str (pname2_buff, MAX_PNAME_TR_LEN, pname, plen);
  int plen2 = strlen (pname2_buff);
  if (plen2 == plen && !memcmp (pname, pname2_buff, plen)) {
    plen2 = -1;
  }
  int add_size = 7+plen+1+plen2+1;
  if (text_len + add_size >= MAX_TEXT_LEN) {
    return 0;
  }
  int new_size = size + add_size;

  struct lev_add_message *N = write_alloc (new_size);

  memcpy (N, E, text_offset);
  char *to = (char *) N + text_offset;
  memcpy (to, "\x2pname ", 7);
  to += 7;
  memcpy (to, pname, plen);
  to += plen;
  if (plen2 > 0) {
    *to++ = ' ';
    memcpy (to, pname2_buff, plen2);
    to += plen2;
  }
  *to++ = 9;
  memcpy (to, text, text_len);
  to[text_len] = 0;
  N->text_len += add_size;

  return new_size;
}


int extra_mask = 0, text_len = -1;

static int text_le_start (struct lev_start *E) {
  assert (E->schema_id == TEXT_SCHEMA_V1);
  if (split_mod) {
    assert (E->split_mod >= split_mod && !(E->split_mod % split_mod) && split_min == E->split_min % split_mod && E->split_max == E->split_min + 1);
  }
  split_min = E->split_min;
  split_max = E->split_max;
  split_mod = E->split_mod;
  assert (split_mod > 0 && split_min >= 0 && split_min + 1 == split_max && split_max <= split_mod);

  extra_mask = 0;
  if (E->extra_bytes >= 3 && E->str[0] == 1) {
    extra_mask = *(unsigned short *) (E->str + 1);
  }

  return 0;
}

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

static int get_logrec_size (int type, void *ptr, int size) {
  struct lev_generic *E = ptr;
  struct lev_add_message *EM;
  int s, t;

  text_len = -1;

  switch (type) {
  case LEV_START:
    if (size < 24 || E->b < 0 || E->b > 4096) { return -2; }
    s = 24 + ((E->b + 3) & -4);
    if (size < s) { return -2; }
    text_le_start ((struct lev_start *) E);
    if (verbosity > 0) {
      fprintf (stderr, "splitting %d %% %d -> %d %% %d\n", split_min, split_mod, copy_rem, copy_mod);
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
    text_len = EM->text_len;
    return s;
  case LEV_TX_ADD_MESSAGE_EXT ... LEV_TX_ADD_MESSAGE_EXT + MAX_EXTRA_MASK:
  case LEV_TX_ADD_MESSAGE_EXT_ZF ... LEV_TX_ADD_MESSAGE_EXT_ZF + MAX_EXTRA_MASK:
  case LEV_TX_ADD_MESSAGE_EXT_LL ... LEV_TX_ADD_MESSAGE_EXT_LL + MAX_EXTRA_MASK:
    if (size < sizeof (struct lev_add_message)) { return -2; }
    EM = (void *) E;
    t = extra_mask_intcount (E->type & MAX_EXTRA_MASK) * 4;
    s = sizeof (struct lev_add_message) + t + EM->text_len + 1;
    if (size < s) { return -2; }
    if ((unsigned) EM->text_len >= MAX_TEXT_LEN || EM->text[t + EM->text_len]) { 
      return -4; 
    }
    text_len = EM->text_len;
    return s;
  case LEV_TX_DEL_MESSAGE:
    return 12;
  case LEV_TX_DEL_FIRST_MESSAGES:
    return 12;
  case LEV_TX_SET_MESSAGE_FLAGS ... LEV_TX_SET_MESSAGE_FLAGS+0xff:
    return 12;
  case LEV_TX_SET_MESSAGE_FLAGS_LONG:
    return 16;
  case LEV_TX_INCR_MESSAGE_FLAGS ... LEV_TX_INCR_MESSAGE_FLAGS+0xff:
    return 12;
  case LEV_TX_INCR_MESSAGE_FLAGS_LONG:
    return 16;
  case LEV_TX_DECR_MESSAGE_FLAGS ... LEV_TX_DECR_MESSAGE_FLAGS+0xff:
    return 12;
  case LEV_TX_DECR_MESSAGE_FLAGS_LONG:
    return 16;
  case LEV_TX_SET_EXTRA_FIELDS ... LEV_TX_SET_EXTRA_FIELDS + MAX_EXTRA_MASK:
    return 12 + 4 * extra_mask_intcount (E->type & MAX_EXTRA_MASK);
  case LEV_TX_INCR_FIELD ... LEV_TX_INCR_FIELD + 7:
    return 16;
  case LEV_TX_INCR_FIELD_LONG + 8 ... LEV_TX_INCR_FIELD_LONG + 11:
    return 20;
  case LEV_CHANGE_FIELDMASK_DELAYED:
    return 8;
  case LEV_TX_REPLACE_TEXT ... LEV_TX_REPLACE_TEXT + 0xfff:
    if (size < sizeof (struct lev_replace_text)) { return -2; }
    text_len = E->type & 0xfff;
    s = offsetof (struct lev_replace_text, text) + text_len + 1;
    if (size < s) { return -2; }
    if ((unsigned) text_len >= MAX_TEXT_LEN || ((struct lev_replace_text *) E)->text[text_len]) {
      return -4;
    }
    return s;
  case LEV_TX_REPLACE_TEXT_LONG:
    if (size < sizeof (struct lev_replace_text_long)) { return -2; }
    text_len = ((struct lev_replace_text_long *) E)->text_len;
    s = offsetof (struct lev_replace_text_long, text) + text_len + 1;
    if (size < s) { return -2; }
    if ((unsigned) text_len >= MAX_TEXT_LEN || ((struct lev_replace_text_long *) E)->text[text_len]) {
      return -4;
    }
    return s;

  default:
    fprintf (stderr, "unknown record type %08x\n", type);
    break;
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
  case LEV_CHANGE_FIELDMASK_DELAYED:
    return 1;
  default:
    user_id = ((struct lev_set_flags *) rec)->user_id;
  }
  user_id %= copy_mod;
  if (user_id == copy_rem || user_id == -copy_rem) {
    return 1;
  }
  return 0;
}

int text_replay_logevent (struct lev_generic *E, int size) {
  int type, s, t, xs;
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

  xs = ((s + 3) & -4);

  rd_bytes += xs;
  rd_rec++;

  type_ok = type;

  if (want_write (type, E)) {
    if (LogTs.timestamp) {
      memcpy (write_alloc (8), &LogTs, 8);
      wr_bytes += 8;
      wr_rec++;
      LogTs.timestamp = 0;
    }
    if (text_len >= 0 && Map && (t = write_convert_record ((struct lev_add_message *) E, s, text_len))) {
      wr_bytes += (t + 3) & -4;
      wr_rec++;
      conv_rec++;
      return s;
    }
    struct lev_generic *N = write_alloc (s);
    memcpy (N, E, s);
    if (type == LEV_START) {
      N->c = copy_mod;
      N->d = copy_rem;
      N->e = copy_rem+1;
    }
    wr_bytes += xs;
    wr_rec++;
  }

  return s;
}


int init_text_data (int schema) {

  replay_logevent = text_replay_logevent;

  return 0;
}


/*
 * END MAIN
 */

void output_stats (void) {
  fprintf (stderr,
	   "read: %lld bytes (reported binlog position %lld), %lld records\n"
	   "written: %lld bytes, %lld records (%lld out of them converted)\n",
	   rd_bytes, log_cur_pos(), rd_rec, wr_bytes, wr_rec, conv_rec);
}


void usage (void) {
  fprintf (stderr, "usage:\t%s [-v] [-u<username>] [-m<rem>,<mod>] [-g<map-file>] <old-binlog-file> [<output-binlog-file>]\n"
	   "\tCopies (some of) text engine binlog records to another binlog. "
	   "If <output-file> is specified, resulting binlog is appended to it.\n"
	   "\t-h\tthis help screen\n"
	   "\t-v\tverbose mode on\n"
	   "\t-g<map-file>\tload file with user names\n"
	   "\t-m<rem>,<mod>\tcopy only record with id %% <mod> == <rem>\n"
	   "\t-s<start-binlog-pos>\tstart reading binlog from specified position\n"
	   "\t-t<stop-binlog-pos>\tstop reading binlog at specified position\n"
	   "\t-u<username>\tassume identity of given user\n",
	   progname);
}

int main (int argc, char *argv[]) {
  int i;
  progname = argv[0];
  while ((i = getopt (argc, argv, "hvu:g:m:s:t:")) != -1) {
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
    case 'g':
      map_filename = optarg;
      break;
    case 's':
      jump_log_pos = atoll (optarg);
      break;
    case 't':
      keep_log_limit_pos = log_limit_pos = atoll (optarg);
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

  if (change_user (username) < 0) {
    fprintf (stderr, "fatal: cannot change user to %s\n", username ? username : "(none)");
    return 1;
  }

  if (engine_preload_filelist (argv[optind], binlogname) < 0) {
    fprintf (stderr, "cannot open binlog files for %s\n", binlogname ? binlogname : argv[optind]);
    exit (1);
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

  clear_log();

  init_log_data (jump_log_pos, 0, 0);

  if (jump_log_pos > 0) {
    init_text_data (0);
  }

  if (map_filename) {
    load_map ();
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

  return 0;
}
