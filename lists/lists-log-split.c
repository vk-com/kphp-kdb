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
              2010-2013 Nikolai Durov
              2010-2013 Andrei Lopatin
              2011-2013 Anton Maydell
*/

#define	_FILE_OFFSET_BITS	64

#include <assert.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <fcntl.h>
#include <unistd.h>
#include <time.h>

#define	LISTS_Z
#include "server-functions.h"
#include "kdb-lists-binlog.h"
#include "kfs.h"
#include "crc32.h"

#define	READ_BUFFER_SIZE	(1L << 24)
#define	WRITE_BUFFER_SIZE	(1L << 24)
#define	READ_THRESHOLD		(1L << 17)
#define	WRITE_THRESHOLD		(1L << 18)

char *progname = "lists-log-split", *username, *targ_fname;
int verbosity = 0;
int now;
int targ_fd;
int targ_existed;
long long rd_bytes, wr_bytes, rd_rec, wr_rec, true_rd_bytes, true_wr_bytes;
unsigned wr_crc32_complement = -1;
int last_timestamp = 0;

enum {
    SPLIT_FIRSTINT,
    SPLIT_LIKED,
} split_mode = SPLIT_FIRSTINT;
int split_min, split_max, split_mod, copy_rem, copy_mod = 1;
long long position, targ_orig_size, jump_log_pos, keep_log_limit_pos = -1;
int skip_rotate = 0;
int filter_member_fan = 0;

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

static int list_id_ints, object_id_ints, value_ints, list_id_bytes, object_id_bytes, list_object_bytes;
static int new_entry_value_offset;

static int lists_le_start (struct lev_start *E) {
  int t_list_id_ints, t_object_id_ints, t_value_ints;
  if (E->schema_id != LISTS_SCHEMA_CUR && E->schema_id != LISTS_SCHEMA_V1) {
    fprintf (stderr, "incorrect binlog schema %08x for lists-engine\n", E->schema_id);
    exit (1);
  }
  if (split_mod) {
    assert (E->split_mod >= split_mod && !(E->split_mod % split_mod) && split_min == E->split_min % split_mod && E->split_max == E->split_min + 1);
  }
  split_min = E->split_min;
  split_max = E->split_max;
  split_mod = E->split_mod;
  assert (split_mod > 0 && split_min >= 0 && split_min + 1 == split_max && split_max <= split_mod);

  if (E->extra_bytes >= 6 && E->str[0] == 1) {
    struct lev_lists_start_ext *EX = (struct lev_lists_start_ext *) E;
    assert (EX->kludge_magic == 1 && EX->schema_id == LISTS_SCHEMA_V3);
    t_list_id_ints = EX->list_id_ints;
    t_object_id_ints = EX->object_id_ints;
    t_value_ints = EX->value_ints;
    assert (!EX->extra_mask);
  } else {
    if (E->schema_id != LISTS_SCHEMA_V1) {
      fprintf (stderr, "incorrect binlog for lists-engine\n");
      exit (1);
    } else {
      t_list_id_ints = t_object_id_ints = t_value_ints = 1;
    }
  }

  if (list_id_ints) {
    assert (t_list_id_ints == list_id_ints && t_object_id_ints == object_id_ints && t_value_ints == value_ints);
  }

  list_id_ints = t_list_id_ints;
  object_id_ints = t_object_id_ints;
  value_ints = t_value_ints;
  new_entry_value_offset = list_id_ints + object_id_ints + 1;

  assert (list_id_ints > 0 && list_id_ints <= MAX_LIST_ID_INTS);
  assert (object_id_ints > 0 && object_id_ints <= MAX_OBJECT_ID_INTS);
  assert (value_ints == 1 || value_ints == 2);
  assert (sizeof (value_t) == 4);

  object_id_bytes = object_id_ints * 4;
  list_id_bytes = list_id_ints * 4;
  list_object_bytes = list_id_bytes + object_id_bytes;

  return 0;
}

int immediate_exit;

static int get_logrec_size (int type, void *ptr, int size) {
  struct lev_generic *E = ptr;
  int s;
  switch (type) {
  case LEV_START:
    if (size < 24 || E->b < 0 || E->b > 4096) { return -2; }
    s = 24 + ((E->b + 3) & -4);
    if (size < s) { return -2; }
    lists_le_start ((struct lev_start *) E);
    if (verbosity > 0) {
      fprintf (stderr, "splitting %d %% %d -> %d %% %d ; list_id/object_id/value ints = %d %d %d\n", split_min, split_mod, copy_rem, copy_mod, list_id_ints, object_id_ints, value_ints);
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
      last_timestamp = LogTs.timestamp = E->a;
    }
    return default_replay_logevent (E, size);
  case LEV_ROTATE_FROM:
  case LEV_ROTATE_TO:
    vkprintf(2, "found LEV_ROTATE_TO, size = %d, skip=%d\n", size, skip_rotate);
    if (size >= 8) {
      LogTs.timestamp = E->a;
    }
    if (skip_rotate) {
      return 36;
    }
    return default_replay_logevent (E, size);
  case LEV_LI_SET_ENTRY ... LEV_LI_SET_ENTRY+0xff:
  case LEV_LI_ADD_ENTRY ... LEV_LI_ADD_ENTRY+0xff:
  case LEV_LI_REPLACE_ENTRY ... LEV_LI_REPLACE_ENTRY+0xff:
    return sizeof (struct lev_new_entry) + list_object_bytes;
  case LEV_LI_SET_ENTRY_EXT ... LEV_LI_SET_ENTRY_EXT+0xff:
  case LEV_LI_ADD_ENTRY_EXT ... LEV_LI_ADD_ENTRY_EXT+0xff:
    return sizeof (struct lev_new_entry_ext) + list_object_bytes;
  case LEV_LI_SET_ENTRY_TEXT ... LEV_LI_SET_ENTRY_TEXT+0xff:
    s = E->type & 0xff;
    if (size < sizeof (struct lev_set_entry_text) + list_object_bytes + s) { return -2; }
    return sizeof (struct lev_set_entry_text) + list_object_bytes + s;
  case LEV_LI_SET_FLAGS ... LEV_LI_SET_FLAGS+0xff:
    return sizeof (struct lev_set_flags) + list_object_bytes;
  case LEV_LI_INCR_FLAGS ... LEV_LI_INCR_FLAGS+0xff:
  case LEV_LI_DECR_FLAGS ... LEV_LI_DECR_FLAGS+0xff:
    return sizeof (struct lev_set_flags) + list_object_bytes;
  case LEV_LI_SET_FLAGS_LONG:
    return sizeof (struct lev_set_flags_long) + list_object_bytes;
  case LEV_LI_CHANGE_FLAGS_LONG:
    return sizeof (struct lev_change_flags_long) + list_object_bytes;
  case LEV_LI_SET_VALUE:
    return sizeof (struct lev_set_value) + list_object_bytes;
  case LEV_LI_INCR_VALUE:
    return sizeof (struct lev_set_value) + list_object_bytes;
  case LEV_LI_INCR_VALUE_TINY ... LEV_LI_INCR_VALUE_TINY + 0xff:
    return sizeof (struct lev_del_entry) + list_object_bytes;
  case LEV_LI_DEL_LIST:
    return sizeof (struct lev_del_list) + list_id_bytes;
  case LEV_LI_DEL_OBJ:
    return sizeof (struct lev_del_obj) + object_id_bytes;
  case LEV_LI_DEL_ENTRY:
    return sizeof (struct lev_del_entry) + list_object_bytes;
  case LEV_LI_SUBLIST_FLAGS:
    return sizeof (struct lev_sublist_flags) + list_id_bytes;
  case LEV_LI_DEL_SUBLIST:
    return sizeof (struct lev_del_sublist) + list_id_bytes;
  default:
    fprintf (stderr, "unknown record type %08x\n", type);
    break;
  }
  return -1;
}

#define MODIFIED_BUFFSIZE 256
static int modified = 0;
static unsigned char modified_record[MODIFIED_BUFFSIZE];

static int default_want_write (int type, void *rec, int size) {
  int list_id = 0;
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
  case LEV_LI_DEL_OBJ:
    return 1;
  default: {
    switch (split_mode) {
    case SPLIT_FIRSTINT:
      list_id = ((struct lev_del_list *) rec)->list_id[0];
      break;
    case SPLIT_LIKED:
      list_id = abs(((struct lev_del_list *) rec)->list_id[0]) + abs(((struct lev_del_list *) rec)->list_id[1]);
      break;
    }
  }
  }
  list_id %= copy_mod;
  if (list_id == copy_rem || list_id == -copy_rem) {
    return 1;
  }
  return 0;
}

void set_value (void *rec, int new_value) {
  int *a = rec;
  a[new_entry_value_offset] = new_value;
}

static long long skipped_change_value = 0, skipped_change_flags = 0;

static int member_fans_want_write (int type, void *rec, int size) {
  int list_id = 0, r = 0;
  switch (type) {
  case LEV_START:
    return !targ_existed++ && !jump_log_pos;
  case LEV_NOOP:
  case LEV_TIMESTAMP:
  case LEV_CRC32:
  case LEV_ROTATE_TO:
  case LEV_ROTATE_FROM:
    return 0;
  case LEV_LI_SET_FLAGS ... LEV_LI_SET_FLAGS+0xff:
  case LEV_LI_INCR_FLAGS ... LEV_LI_INCR_FLAGS+0xff:
  case LEV_LI_DECR_FLAGS ... LEV_LI_DECR_FLAGS+0xff:
  case LEV_LI_SET_FLAGS_LONG:
  case LEV_LI_CHANGE_FLAGS_LONG:
    skipped_change_flags += size;
    return 0;
  case LEV_LI_SET_VALUE:
  case LEV_LI_INCR_VALUE:
  case LEV_LI_INCR_VALUE_TINY ... LEV_LI_INCR_VALUE_TINY + 0xff:
    skipped_change_value += size;
    return 0;
  case LEV_LI_DEL_OBJ:
    return 1;
  default:
    list_id = ((struct lev_del_list *) rec)->list_id[0];
  }
  list_id %= copy_mod;
  if (list_id == copy_rem || list_id == -copy_rem) {
    r = 1;
  }
  struct lev_new_entry_ext *E;
  switch (type) {
    case LEV_LI_SET_ENTRY ... LEV_LI_SET_ENTRY+0xff:
    case LEV_LI_ADD_ENTRY ... LEV_LI_ADD_ENTRY+0xff:
    case LEV_LI_REPLACE_ENTRY ... LEV_LI_REPLACE_ENTRY+0xff:
    case LEV_LI_SET_ENTRY_EXT ... LEV_LI_SET_ENTRY_EXT+0xff:
    case LEV_LI_ADD_ENTRY_EXT ... LEV_LI_ADD_ENTRY_EXT+0xff:
      modified = 1;
      assert (size <= MODIFIED_BUFFSIZE);
      memcpy (modified_record, rec, size);
      E = (struct lev_new_entry_ext *) modified_record;
      assert (type == E->type);
      E->type &= -0x100; /* flags := 0 */
      set_value (modified_record, 0);
      break;
  }
  return r;
}

static int fan_members_want_write (int type, void *rec, int size) {
  int list_id = 0, r = 0;
  switch (type) {
  case LEV_START:
    return !targ_existed++ && !jump_log_pos;
  case LEV_NOOP:
  case LEV_TIMESTAMP:
  case LEV_CRC32:
  case LEV_ROTATE_TO:
  case LEV_ROTATE_FROM:
    return 0;
  case LEV_LI_SET_VALUE:
  case LEV_LI_INCR_VALUE:
  case LEV_LI_INCR_VALUE_TINY ... LEV_LI_INCR_VALUE_TINY + 0xff:
    skipped_change_value += size;
    return 0;
  case LEV_LI_DEL_OBJ:
    return 1;
  case LEV_LI_CHANGE_FLAGS_LONG:
    kprintf ("not implemented: type = 0x%x", type);
    assert (0);
    break;
  case LEV_LI_INCR_FLAGS ... LEV_LI_INCR_FLAGS+0xff:
  case LEV_LI_DECR_FLAGS ... LEV_LI_DECR_FLAGS+0xff:
    if (!(type & 4)) {
      skipped_change_flags += size;
      return 0;
    }
  default:
    list_id = ((struct lev_del_list *) rec)->list_id[0];
  }
  list_id %= copy_mod;
  if (list_id == copy_rem || list_id == -copy_rem) {
    r = 1;
  }
  struct lev_new_entry_ext *E;
  struct lev_set_flags *F;
  struct lev_set_flags_long *G;
  switch (type) {
    case LEV_LI_SET_ENTRY ... LEV_LI_SET_ENTRY+0xff:
    case LEV_LI_ADD_ENTRY ... LEV_LI_ADD_ENTRY+0xff:
    case LEV_LI_REPLACE_ENTRY ... LEV_LI_REPLACE_ENTRY+0xff:
    case LEV_LI_SET_ENTRY_EXT ... LEV_LI_SET_ENTRY_EXT+0xff:
    case LEV_LI_ADD_ENTRY_EXT ... LEV_LI_ADD_ENTRY_EXT+0xff:
      modified = 1;
      assert (size <= MODIFIED_BUFFSIZE);
      memcpy (modified_record, rec, size);
      E = (struct lev_new_entry_ext *) modified_record;
      E->type &= ~0xfb; /* flags := flags & 4 */
      set_value (modified_record, 1);
      break;
    case LEV_LI_SET_FLAGS ... LEV_LI_SET_FLAGS+0xff:
      modified = 1;
      assert (size <= MODIFIED_BUFFSIZE);
      memcpy (modified_record, rec, size);
      F = (struct lev_set_flags *) modified_record;
      F->type &= ~0xfb;
      break;
    case LEV_LI_SET_FLAGS_LONG:
      modified = 1;
      assert (size <= MODIFIED_BUFFSIZE);
      memcpy (modified_record, rec, size);
      G = (struct lev_set_flags_long *) modified_record;
      G->flags &= ~4;
      break;
    case LEV_LI_INCR_FLAGS ... LEV_LI_INCR_FLAGS+0xff:
    case LEV_LI_DECR_FLAGS ... LEV_LI_DECR_FLAGS+0xff:
      modified = 1;
      assert (size <= MODIFIED_BUFFSIZE);
      memcpy (modified_record, rec, size);
      F = (struct lev_set_flags *) modified_record;
      assert (type & 4);
      F->type = (type & -0x100) + 4;
      break;
  }
  return r;
}

typedef int (*want_write_vector_t) (int type, void *rec, int size);
want_write_vector_t want_write = default_want_write;

int lists_replay_logevent (struct lev_generic *E, int size) {
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

  rd_bytes += s;
  rd_rec++;

  type_ok = type;

  s = ((s + 3) & -4);

  //relax_log_crc32 (s);

  if (want_write (type, E, s)) {
    if (LogTs.timestamp) {
      void *N = write_alloc (8);
      memcpy (N, &LogTs, 8);
      wr_crc32_complement = crc32_partial (N, 8, wr_crc32_complement);
      wr_bytes += 8;
      wr_rec++;
      LogTs.timestamp = 0;
    }
    struct lev_generic *N = write_alloc (s);
    if (modified) {
      memcpy (N, modified_record, s);
      modified = 0;
    } else {
      memcpy (N, E, s);
    }
    if (type == LEV_START) {
      N->c = copy_mod;
      N->d = copy_rem;
      N->e = copy_rem+1;
    }
    wr_crc32_complement = crc32_partial (N, s, wr_crc32_complement);
    wr_bytes += s;
    wr_rec++;
  }

  return s;
}


int init_lists_data (int schema) {

  replay_logevent = lists_replay_logevent;

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
  fprintf (stderr, "skipped_change_value: %lld bytes.\n", skipped_change_value);
  fprintf (stderr, "skipped_change_flags: %lld bytes.\n", skipped_change_flags);
}


void usage (void) {
  fprintf (stderr, "usage:\t%s [-v] [-u<username>] [-m<rem>,<mod>] [...options...] <old-binlog-file> [<output-file>]\n"
      "\tCopies (some of) lists engine binlog records to another binlog. "
      "If <output-file> is specified, resulting binlog is appended to it.\n"
      "\t-h\tthis help screen\n"
      "\t-v\tverbose mode on\n"
      "\t-f\tskip LEV_ROTATE_TO and LEV_ROTATE_FROM entries (useful when binlog is merged in one file)\n"
      "\t-m<rem>,<mod>\tcopy only record with id %% <mod> == <rem>\n"
      "\t-s<start-binlog-pos>\tstart reading binlog from specified position\n"
      "\t-t<stop-binlog-pos>\tstop reading binlog at specified position\n"
      "\t-u<username>\tassume identity of given user\n"
      "\t-M<mode>\t split mode: firstint (default) or liked"
      "\t-f\tskip rotate\n"
      "\t-F\tmember_fans: value := 0; flags := 0\n"
      "\t\tfans_members: value := 1; flags := flags & 4\n",
      progname);
}

int main (int argc, char *argv[]) {
  int i;
  progname = argv[0];
  while ((i = getopt (argc, argv, "fhvu:m:s:t:M:F")) != -1) {
    switch (i) {
    case 'F':
      filter_member_fan = 1;
      break;
    case 'v':
      verbosity += 1;
      break;
    case 'f':
      // vkprintf(2, "setting skip_rotate\n");
      skip_rotate = 1;
      break;
    case 'h':
      usage ();
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
      keep_log_limit_pos = log_limit_pos = atoll (optarg);
      break;
    case 'M':
      if (!strncmp(optarg, "firstint", 9)) {
        split_mode = SPLIT_FIRSTINT;
      } else if (!strncmp(optarg, "liked", 6)) {
        split_mode = SPLIT_LIKED;
      } else {
        usage();
        return 2;
      }
      break;
    default:
      assert (0);
      return 2;
    }
  }

  if (optind >= argc || optind + 2 < argc) {
    usage();
    return 2;
  }

  if (filter_member_fan) {
    vkprintf (1, "fix member_fans, fan_members mode\n");
    char *p = strrchr (argv[optind], '/');
    p = (p == NULL) ? argv[optind] : (p + 1);
    if (!strncmp (p, "member_fans", 11)) {
      want_write = member_fans_want_write;
    } else if (!strncmp (p, "fan_members", 11)) {
      want_write = fan_members_want_write;
    } else {
      kprintf ("binlogname should starts from member_fans of fan_members when command line switch -F used.\n");
      exit (1);
    }
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

  clear_log();

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

    i = replay_log (0, 1);

    if (!list_id_ints) {
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

  i = replay_log (0, 1);

  if (i < 0) {
    fprintf (stderr, "fatal: error reading binlog\n");
    exit (1);
  }

  if (log_limit_pos >= 0 && log_readto_pos != log_limit_pos) {
    fprintf (stderr, "fatal: binlog read up to position %lld instead of %lld\n", log_readto_pos, log_limit_pos);
    exit (1);
  }

  if (!targ_orig_size && !jump_log_pos) {
    vkprintf (1, "Writing CRC32 to the end of target binlog.\n");
    struct lev_crc32 *C = write_alloc (20);
    C->type = LEV_CRC32;
    C->timestamp = last_timestamp;
    C->pos = wr_bytes;
    C->crc32 = ~wr_crc32_complement;
    wr_bytes += 20;
    wr_rec++;
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

