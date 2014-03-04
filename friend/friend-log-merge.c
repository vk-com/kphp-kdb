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

    Copyright 2009-2010 Vkontakte Ltd
              2009-2010 Nikolai Durov
              2009-2010 Andrei Lopatin
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
#include "kdb-friends-binlog.h"


#define	READ_BUFFER_SIZE	(1L << 24)
#define	WRITE_BUFFER_SIZE	(1L << 24)
#define	READ_THRESHOLD		(1L << 18)
#define	WRITE_THRESHOLD		(1L << 18)

char *progname = "friend-log-merge", *username, *src_fname, *targ_fname, *curr_fname;
int verbosity = 0;
int src_fd, targ_fd, curr_fd;
long long rd_bytes, wr_bytes, rd_rec, wr_rec, true_rd_bytes, true_wr_bytes;

int mode, t_now, t_cutoff, cutoff_ago = 18000, skip_timestamps;
long long position, targ_orig_size;

char *rptr, *rend, *wptr, *wst;
char RB[READ_BUFFER_SIZE+4], WB[WRITE_BUFFER_SIZE+4];

int prepare_read (void) {
  int a, b;
  if (rptr == rend && rptr) {
    return 0;
  }
  if (!rptr) {
    rptr = rend = RB + READ_BUFFER_SIZE;
  }
  a = rend - rptr;
  assert (a >= 0);
  if (a >= READ_THRESHOLD || rend < RB + READ_BUFFER_SIZE) {
    return a;
  }
  memcpy (RB, rptr, a);
  rend -= rptr - RB;
  rptr = RB;
  b = RB + READ_BUFFER_SIZE - rend;
  a = read (src_fd, rend, b);
  if (a < 0) {
    fprintf (stderr, "error reading %s: %m\n", src_fname);
    *rend = 0;
    return rend - rptr;
  } else {
    true_rd_bytes += a;
  }
  if (verbosity > 0) {
    fprintf (stderr, "read %d bytes from %s\n", a, src_fname);
  }
  rend += a;
  *rend = 0;
  return rend - rptr;
}

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
    return s;
  case LEV_NOOP:
    return 4;
  case LEV_TIMESTAMP:
    return 8;
  case LEV_FR_SETLIST_CAT_LONG:
    if (size < 12) { return -2; }
    s = ((struct lev_setlist_cat_long *) E)->num;
    if (s & -0x10000) { return -4; }
    return 12 + 8*s;
  case LEV_FR_SETLIST_CAT ... LEV_FR_SETLIST_CAT+0xff:
    s = E->type & 0xff;
    return 8 + 8*s;
  case LEV_FR_SETLIST_LONG:
    if (size < 12) { return -2; }
    s = ((struct lev_setlist_long *) E)->num;
    if (s & -0x10000) { return -4; }
    return 12 + 4*s;
  case LEV_FR_SETLIST ... LEV_FR_SETLIST+0xff:
    s = E->type & 0xff;
    return 8 + 4*s;
  case LEV_FR_USERDEL:
    return 8;
  case LEV_FR_ADD_FRIENDREQ:
    return 16;
  case LEV_FR_REPLACE_FRIENDREQ:
    return 16;
  case LEV_FR_NEW_FRIENDREQ:
    return 16;
  case LEV_FR_DEL_FRIENDREQ:
    return 12;
  case LEV_FR_DELALL_REQ:
    return 8;
  case LEV_FR_ADD_FRIEND:
    return 16;
  case LEV_FR_EDIT_FRIEND:
    return 16;
  case LEV_FR_EDIT_FRIEND_OR:
    return 16;
  case LEV_FR_EDIT_FRIEND_AND:
    return 16;
  case LEV_FR_DEL_FRIEND:
    return 12;
  case LEV_FR_ADDTO_CAT+1 ... LEV_FR_ADDTO_CAT+0x1f:
    return 12;
  case LEV_FR_REMFROM_CAT+1 ... LEV_FR_REMFROM_CAT+0x1f:
    return 12;
  case LEV_FR_DEL_CAT+1 ... LEV_FR_DEL_CAT+0x1f:
    return 8;
  case LEV_FR_CLEAR_CAT+1 ... LEV_FR_CLEAR_CAT+0x1f:
    return 8;
  case LEV_FR_CAT_SETLIST+1 ... LEV_FR_CAT_SETLIST+0x1f:
    if (size < 12) { return -2; }
    s = ((struct lev_setlist_long *) E)->num;
    if (s & -0x10000) { return -4; }
    return 12 + 4*s;
  case LEV_FR_SET_PRIVACY ... LEV_FR_SET_PRIVACY+0xff:
  case LEV_FR_SET_PRIVACY_FORCE ... LEV_FR_SET_PRIVACY_FORCE+0xff:
    s = (E->type & 0xff);
    return 16 + 4*s;
  case LEV_FR_SET_PRIVACY_LONG ... LEV_FR_SET_PRIVACY_LONG+1:
    if (size < 20) { return -2; }
    s = ((struct lev_set_privacy_long *) E)->len;
    return s <= 65536 ? 20 + 4*s : -2;
  case LEV_FR_DEL_PRIVACY:
    return 16;
  }
   
  return -1;
}

int t_now = 0;

static int want_write (int type, void *ptr) {
  struct lev_generic *E = ptr;

  if (type == LEV_TIMESTAMP && E->a > t_now) {
    t_now = E->a;
    if (t_now > t_cutoff && !mode) {
      mode = 1;
      fprintf (stderr, "reached binlog time %d above cutoff time %d at read position %lld\n", t_now, t_cutoff, rd_bytes);
    }
  }

  if (type == LEV_TIMESTAMP && skip_timestamps) {
    return 0;
  }

  return mode;
}

int process_record (void) {
  int size, type, s;
  static int type_ok = -1;

  prepare_read();
  if (rptr == rend) {
    return -1;
  }
  size = rend - rptr;
  if (size < 4) {
    return -2;
  }

  type = *((int *) rptr);
  s = get_logrec_size (type, rptr, size);

  if (s > size) {
    s = -2;
  }

  if (s < 0) {
    fprintf (stderr, "error %d reading binlog at position %lld, write position %lld, type %08x (prev type %08x)\n", s, rd_bytes, wr_bytes + targ_orig_size, type, type_ok);
    return s;
  }

  type_ok = type;

  s = ((s + 3) & -4);

  if (want_write (type, rptr)) {
    memcpy (write_alloc (s), rptr, s);
    wr_bytes += s;
    wr_rec++;
  }

  rd_bytes += s;
  rd_rec++;
  rptr += s;

  return 0;
}



/*
 * END MAIN
 */

void output_stats (void) {
  fprintf (stderr,
	   "read: %lld bytes (%ld remaining), %lld records\n"
	   "written: %lld bytes, %lld records\n",
	   rd_bytes, (long)(rend - rptr), rd_rec, wr_bytes, wr_rec);
}


void usage (void) {
  fprintf (stderr, "usage:\t%s [-v] [-u<username>] [-t<seconds-ago>] <old-binlog-file> [<output-binlog-file>]\n"
	   "\tAppends most recent privacy records from first binlog to another binlog. "
	   "If <output-binlog-file> is specified, resulting binlog is appended to it.\n"
	   "\t-h\tthis help screen\n"
	   "\t-v\tverbose mode on\n"
	   "\t-t\tcutoff time relative to present moment\n"
	   "\t-i\tdo not import timestamps\n"
	   "\t-u<username>\tassume identity of given user\n",
	   progname);
}

int main (int argc, char *argv[]) {
  int i;
  progname = argv[0];
  while ((i = getopt (argc, argv, "hivt:u:")) != -1) {
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
    case 'i':
      skip_timestamps = 1;
      break;
    case 't':
      cutoff_ago = atoi (optarg);
      break;
    }
  }

  if (optind >= argc || optind + 2 < argc || cutoff_ago <= 0) {
    usage();
    return 2;
  }

  t_cutoff = time(0) - cutoff_ago;

  src_fname = argv[optind];

  if (username && change_user (username) < 0) {
    fprintf (stderr, "fatal: cannot change user to %s\n", username ? username : "(none)");
    return 1;
  }

  src_fd = open (src_fname, O_RDONLY);
  if (src_fd < 0) {
    fprintf (stderr, "cannot open %s: %m\n", src_fname);
    return 1;
  }

  if (optind + 1 < argc) {
    targ_fname = argv[optind+1];
    targ_fd = open (targ_fname, O_WRONLY | O_APPEND | O_CREAT, 0644);
    if (targ_fd < 0) {
      fprintf (stderr, "cannot create %s: %m\n", targ_fname);
      return 1;
    }
    targ_orig_size = lseek (targ_fd, 0, SEEK_END);
    assert (targ_orig_size > 0);
  } else {
    targ_fname = "stdout";
    targ_fd = 1;
  }

  while (process_record() >= 0) { }

  flush_out();

  if (targ_fd != 1) {
    if (fdatasync(targ_fd) < 0) {
      fprintf (stderr, "error syncing %s: %m", targ_fname);
      exit (1);
    }
    close (targ_fd);
  }

  if (verbosity > 0) {
    output_stats();
  }

  return rend > rptr ? 1 : 0;
}
