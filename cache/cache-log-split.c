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

#define	_FILE_OFFSET_BITS	64

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

#define	READ_BUFFER_SIZE	(1L << 24)
#define	WRITE_BUFFER_SIZE	(1L << 24)
#define	READ_THRESHOLD		(1L << 17)
#define	WRITE_THRESHOLD		(1L << 18)

static char *targ_fname;
static int targ_fd, targ_existed, output_cache_id = -1;
static long long rd_bytes, wr_bytes, rd_rec, wr_rec, true_wr_bytes;

static int split_min, split_max, split_mod, copy_rem, copy_mod = 1;
static int write_local_copies_logevents = 1;
static int start_timestamp = 0;
static long long targ_orig_size, jump_log_pos;

static struct lev_timestamp LogTs = { .type = LEV_TIMESTAMP };

static char *wptr, *wst;
static char WB[WRITE_BUFFER_SIZE+4];

static void flush_out (void) {
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
    kprintf ("error writing to %s: %d bytes written out of %d: %m\n", targ_fname, a, b);
    exit (3);
  }

  vkprintf  (1, "%d bytes written to %s\n", a, targ_fname);

  wptr = wst = WB;
}

static void prepare_write (int x) {
  if (x < 0) {
    x = WRITE_THRESHOLD;
  }
  assert (x > 0 && x <= WRITE_THRESHOLD);
  if (!wptr) {
    wptr = wst = WB;
  }
  if (WB + WRITE_BUFFER_SIZE - wptr < x) {
    flush_out ();
  }
}

static void *write_alloc (int s) {
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
    if (E->a != CACHE_SCHEMA_V1) {
      kprintf ("fatal: CACHE_SCHEMA expected\n");
      return -2;
    }
    if (split_mod) {
      assert (E->c >= split_mod && !(E->c % split_mod) && split_min == E->d % split_mod && E->e == E->d + 1);
    }
    split_mod = E->c;
    split_min = E->d;
    split_max = E->e;
    assert (split_min >= 0 && split_max == split_min + 1 && split_max <= split_mod);

    vkprintf (1, "splitting %d..%d %% %d -> %d %% %d\n", split_min, split_max-1, split_mod, copy_rem, copy_mod);
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
  case LEV_CACHE_ACCESS_SHORT...LEV_CACHE_ACCESS_SHORT+0xff:
    return sizeof (struct lev_cache_access_short);
  case LEV_CACHE_ACCESS_LONG...LEV_CACHE_ACCESS_LONG+0xff:
    return sizeof (struct lev_cache_access_long);
  case LEV_CACHE_URI_ADD...LEV_CACHE_URI_ADD+0xff:
    return sizeof (struct lev_cache_uri) + (E->type & 0xff);
  case LEV_CACHE_URI_DELETE...LEV_CACHE_URI_DELETE+0xff:
    return sizeof (struct lev_cache_uri) + (E->type & 0xff);
  case LEV_CACHE_SET_SIZE_SHORT:
    return sizeof (struct lev_cache_set_size_short);
  case LEV_CACHE_SET_SIZE_LONG:
    return sizeof (struct lev_cache_set_size_long);
  case LEV_CACHE_SET_NEW_LOCAL_COPY_SHORT:
    return sizeof (struct lev_cache_set_new_local_copy_short);
  case LEV_CACHE_SET_NEW_LOCAL_COPY_LONG...LEV_CACHE_SET_NEW_LOCAL_COPY_LONG+0xff:
    return sizeof (struct lev_cache_set_new_local_copy_long) + (E->type & 0xff);
  case LEV_CACHE_DELETE_LOCAL_COPY_SHORT:
    return sizeof (struct lev_cache_set_new_local_copy_short);
  case LEV_CACHE_DELETE_LOCAL_COPY_LONG...LEV_CACHE_DELETE_LOCAL_COPY_LONG+0xff:
    return sizeof (struct lev_cache_set_new_local_copy_long) + (E->type & 0xff);
  case LEV_CACHE_CHANGE_DISK_STATUS...LEV_CACHE_CHANGE_DISK_STATUS+1:
    return sizeof (struct lev_cache_change_disk_status);
  case LEV_CACHE_DELETE_REMOTE_DISK:
    return sizeof (struct lev_cache_change_disk_status);
  case LEV_CACHE_SET_LOCAL_COPY_YELLOW_LIGHT_SHORT:
    return sizeof (struct lev_cache_set_local_copy_yellow_light_short);
  case LEV_CACHE_SET_LOCAL_COPY_YELLOW_LIGHT_LONG...LEV_CACHE_SET_LOCAL_COPY_YELLOW_LIGHT_LONG+0xff:
    return sizeof (struct lev_cache_set_local_copy_yellow_light_long) + (E->type & 0xff);
  }

  return -1;
}

static int want_write (int type, void *rec) {
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

  case LEV_CACHE_ACCESS_SHORT...LEV_CACHE_ACCESS_SHORT+0xff:
  case LEV_CACHE_ACCESS_LONG...LEV_CACHE_ACCESS_LONG+0xff:
  case LEV_CACHE_URI_ADD...LEV_CACHE_URI_ADD+0xff:
  case LEV_CACHE_URI_DELETE...LEV_CACHE_URI_DELETE+0xff:
  case LEV_CACHE_SET_SIZE_SHORT:
  case LEV_CACHE_SET_SIZE_LONG:
    return log_last_ts >= start_timestamp;
  case LEV_CACHE_SET_NEW_LOCAL_COPY_SHORT:
  case LEV_CACHE_SET_NEW_LOCAL_COPY_LONG...LEV_CACHE_SET_NEW_LOCAL_COPY_LONG+0xff:
  case LEV_CACHE_DELETE_LOCAL_COPY_SHORT:
  case LEV_CACHE_DELETE_LOCAL_COPY_LONG...LEV_CACHE_DELETE_LOCAL_COPY_LONG+0xff:
  case LEV_CACHE_CHANGE_DISK_STATUS...LEV_CACHE_CHANGE_DISK_STATUS+1:
  case LEV_CACHE_DELETE_REMOTE_DISK:
  case LEV_CACHE_SET_LOCAL_COPY_YELLOW_LIGHT_SHORT:
  case LEV_CACHE_SET_LOCAL_COPY_YELLOW_LIGHT_LONG...LEV_CACHE_SET_LOCAL_COPY_YELLOW_LIGHT_LONG+0xff:
    return write_local_copies_logevents && log_last_ts >= start_timestamp;
  default:
    kprintf ("%s: unknown logevent type (0x%08x)", __func__, type);
    exit (1);
  }
}

int cache_replay_logevent (struct lev_generic *E, int size) {
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
    kprintf ("error %d reading binlog at position %lld, write position %lld, type %08x (prev type %08x)\n", s, log_cur_pos(), wr_bytes + targ_orig_size, type, type_ok);
    return s;
  }

  if (!targ_existed && type != LEV_START && jump_log_pos == 0) {
    kprintf ("error: first record must be a LEV_START\n");
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
      assert (s == 28);
      if (output_cache_id > 0) {
        memcpy (((char *) N) + 24, &output_cache_id, 4);
      }
    }
    wr_bytes += s;
    wr_rec++;
  }

  return s;
}

int init_cache_data (int schema) {
  replay_logevent = cache_replay_logevent;
  return 0;
}

/*
 * END MAIN
 */

void output_stats (void) {
  kprintf (
	   "read: %lld bytes (reported binlog position %lld), %lld records\n"
	   "written: %lld bytes, %lld records\n",
	   rd_bytes, log_cur_pos(), rd_rec, wr_bytes, wr_rec);
}

void usage (void) {
  fprintf (stderr, "usage:\t%s [-v] [-u<username>] [-m<rem>,<mod>] <old-binlog-file> [<output-file>]\n"
      "\tCopies (some of) cache records to another binlog. "
      "If <output-file> is specified, resulting binlog is appended to it.\n"
      "\t[-h]\t\tthis help screen\n"
      "\t[-v]\t\tverbose mode on\n"
      "\t[-m<rem>,<mod>]\tcopy only record with id %% <mod> == <rem>\n"
      "\t[-s<start-binlog-pos>]\tstart reading binlog from specified position\n"
      "\t[-t<stop-binlog-pos>]\tstop reading binlog at specified position\n"
      "\t[-S<timestamp>]\tcopy logevents only after given timestamp\n"
      "\t[-L]\t\tdon't copy local copy logevents\n"
      "\t[-C<cache_id>]\twrite given cache_id to LEV_START\n"
      "\t[-u<username>]\tassume identity of given user\n",
      progname);
  exit (2);
}

int main (int argc, char *argv[]) {
  int i;
  progname = argv[0];
  while ((i = getopt (argc, argv, "C:LS:hm:s:t:u:v")) != -1) {
    switch (i) {
    case 'C':
      output_cache_id = atoi (optarg);
      break;
    case 'L':
      write_local_copies_logevents = 0;
      break;
    case 'S':
      start_timestamp = atoi (optarg);
      if (start_timestamp > time (NULL)) {
        kprintf ("start_timestamps could be after current time\n");
        exit (1);
      }
      break;
    case 'h':
      usage ();
      return 2;
    case 'm':
      if (sscanf (optarg, "%d,%d", &copy_rem, &copy_mod) != 2 || copy_rem < 0 || copy_rem >= copy_mod) {
	      usage();
      }
      break;
    case 's':
      jump_log_pos = atoll (optarg);
      break;
    case 't':
      log_limit_pos = atoll (optarg);
      break;
    case 'u':
      username = optarg;
      break;
    case 'v':
      verbosity = 1;
      break;
    default:
      fprintf (stderr, "Unimplemented option -%c\n", (char) i);
      exit (1);
    }
  }

  if (optind >= argc || optind + 2 < argc) {
    usage ();
  }

  if (log_limit_pos >= 0) {
    if (jump_log_pos > log_limit_pos) {
      kprintf ("fatal: log start position %lld after stop position %lld\n", jump_log_pos, log_limit_pos);
      return 2;
    }
  }

  if (change_user (username) < 0) {
    kprintf ("fatal: cannot change user to %s\n", username ? username : "(none)");
    return 1;
  }

  if (engine_preload_filelist (argv[optind], binlogname) < 0) {
    kprintf ("cannot open binlog files for %s\n", binlogname ? binlogname : argv[optind]);
    exit (1);
  }

  Binlog = open_binlog (engine_replica, jump_log_pos);
  if (!Binlog) {
    kprintf ("fatal: cannot find binlog for %s, log position %lld\n", engine_replica->replica_prefix, 0LL);
    exit (1);
  }

  binlogname = Binlog->info->filename;

  vkprintf (1, "replaying binlog file %s (size %lld)\n", binlogname, Binlog->info->file_size);

  clear_log ();

  init_log_data (jump_log_pos, 0, 0);

  if (jump_log_pos > 0) {
    init_cache_data (0);
  }

  if (optind + 1 < argc) {
    targ_fname = argv[optind+1];
    targ_fd = open (targ_fname, O_WRONLY | O_APPEND | O_CREAT, 0644);
    if (targ_fd < 0) {
      kprintf ("cannot create %s: %m\n", targ_fname);
      return 1;
    }
    targ_orig_size = lseek (targ_fd, 0, SEEK_END);
    targ_existed = (targ_orig_size > 0);
  } else {
    targ_fname = "stdout";
    targ_fd = 1;
  }

  if (output_cache_id > 0 && targ_existed) {
    kprintf ("You couldn't specify '-C %d' and existed target together.\n", output_cache_id);
    exit (1);
  }

  i = replay_log (0, 1);

  if (i < 0) {
    kprintf ("fatal: error reading binlog\n");
    exit (1);
  }

  if (log_limit_pos >= 0 && log_readto_pos != log_limit_pos) {
    kprintf ("fatal: binlog read up to position %lld instead of %lld\n", log_readto_pos, log_limit_pos);
    exit (1);
  }

  flush_out ();

  if (targ_fd != 1) {
    if (fdatasync (targ_fd) < 0) {
      kprintf ("error syncing %s: %m", targ_fname);
      exit (1);
    }
    close (targ_fd);
  }

  if (verbosity > 0) {
    output_stats ();
  }

  return 0;
}
