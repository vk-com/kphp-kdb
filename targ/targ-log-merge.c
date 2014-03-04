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
#include "kdb-targ-binlog.h"


#define	READ_BUFFER_SIZE	(1L << 24)
#define	WRITE_BUFFER_SIZE	(1L << 24)
#define	READ_THRESHOLD		(1L << 17)
#define	WRITE_THRESHOLD		(1L << 16)

char *progname = "targ-log-merge", *username, *src_fname, *targ_fname, *curr_fname;
int verbosity = 0;
int src_fd, targ_fd, curr_fd;
long long rd_bytes, wr_bytes, rd_rec, wr_rec, true_rd_bytes, true_wr_bytes;

int mode, addr_ext_size = 27;
long long position, targ_orig_size;

int time_threshold, time_offset = 86400;

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
  int s, t;
  switch (type) {
  case LEV_START:
    if (size < 24 || E->b < 0 || E->b > 4096) { return -1; }
    s = 24 + ((E->b + 3) & -4);
    return s;
  case LEV_NOOP:
    return 4;
  case LEV_TIMESTAMP:
    if (!mode && *((int *) (ptr + 4)) >= time_threshold) {
      mode = -1;
      if (verbosity > 0) {
        fprintf (stderr, "reached copy-all threshold (timestamp %d) at read position %lld, write position %lld\n", *((int *) (ptr + 4)), rd_bytes, wr_bytes + targ_orig_size);
      }
    }
    return 8;
  case LEV_TARG_USERNAME ... LEV_TARG_USERNAME+0xff:
    return (type & 0xff) + 9;
  case LEV_TARG_USERFLAGS ... LEV_TARG_USERFLAGS+0xff:
    return 8;
  case LEV_TARG_POLITICAL:
    return 12;
  case LEV_TARG_MSTATUS:
    return 12;
  case LEV_TARG_SEX:
    return 12;
  case LEV_TARG_OPERATOR:
    return 12;
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
    return (type & 0xff) + addr_ext_size;
  case LEV_TARG_GRTYPE_CLEAR:
    return 8;
  case LEV_TARG_GRTYPE_ADD ... LEV_TARG_GRTYPE_ADD+0x7f:
    return 8;
  case LEV_TARG_INTERESTS_CLEAR ... LEV_TARG_INTERESTS_CLEAR + 7:
    if (addr_ext_size == 29) {
      return -17;
    }
    return 8;
  case LEV_TARG_INTERESTS+1 ... LEV_TARG_INTERESTS+7:
    return ((struct lev_interests *) E)->len + 11;
  case LEV_TARG_PROPOSAL:
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
    t = ((struct lev_groups_ext *) E)->gr_num;
    if (t < 0 || t > 0x10000 || size < 12 + t * 4) { return -1; }
    return 12 + t * 4;
  case LEV_TARG_TARGET:
    return ((struct lev_targ_target *) E)->ad_query_len + 15;
  case LEV_TARG_AD_ON:
    return 8;
  case LEV_TARG_AD_OFF:
    return 8;
  case LEV_TARG_AD_PRICE:
    return 12;
  case LEV_TARG_CLICK:
    return 12;
  case LEV_TARG_VIEWS:
    return 12;
  }

  return -1;
}

static int want_write (int type) {
  if (mode < 0) {
    return 1;
  }
  switch (type) {
  case LEV_TARG_INTERESTS_CLEAR ... LEV_TARG_INTERESTS_CLEAR + 7:
    if (mode == 1) {
      mode = 2;
      position = wr_bytes;
      fprintf (stderr, "switched to mode 2 at read position %lld, write position %lld\n", rd_bytes, wr_bytes + targ_orig_size);
    }
    return 0;
  case LEV_TARG_ADDR_EXT_ADD ... LEV_TARG_ADDR_EXT_ADD + 0xff:
    if (mode == 1 && !(type & 2)) {
      mode = 2;
      position = wr_bytes;
      fprintf (stderr, "switched to mode 2 at read position %lld, write position %lld\n", rd_bytes, wr_bytes + targ_orig_size);
    }
    return 0;
  case LEV_TARG_TARGET:
  case LEV_TARG_AD_ON:
  case LEV_TARG_AD_OFF:
  case LEV_TARG_AD_PRICE:
  case LEV_TARG_CLICK:
  case LEV_TARG_VIEWS:
    return 1;
  }
  return 0;
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

  if (want_write (type)) {
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
  fprintf (stderr, "usage:\t%s [-v] [-u<username>] [-t<seconds-ago>] <old-binlog-file> [<new-binlog-file>] [<current-binlog-file>]\n"
	   "\tAppends targeting records to another binlog. "
	   "If <output-file> is specified, resulting binlog is appended to it.\n"
	   "\t-h\tthis help screen\n"
	   "\t-v\tverbose mode on\n"
	   "\t-t<seconds-ago>\tcopy all records created after given threshold\n"
	   "\t-u<username>\tassume identity of given user\n",
	   progname);
}

int main (int argc, char *argv[]) {
  int i;
  progname = argv[0];
  while ((i = getopt (argc, argv, "hviu:t:")) != -1) {
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
    case 't':
      time_offset = atoi (optarg);
      break;
    case 'i':
      addr_ext_size = 29;
      break;
    }
  }

  if (optind >= argc || optind + 3 < argc) {
    usage();
    return 2;
  }

  if (time_offset <= 0) {
    time_offset = 86400;
  }
  time_threshold = time(0) - time_offset;

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

  if (optind + 2 < argc) {
    curr_fname = argv[optind+2];
    curr_fd = open (curr_fname, O_RDONLY);
    mode = 1;
    if (curr_fd < 0) {
      fprintf (stderr, "cannot open %s: %m\n", curr_fname);
      return 1;
    }
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

  if (mode == 2 && rend == rptr) {
    assert (position > 0);
    position += targ_orig_size;
    if (verbosity > 0) {
      fprintf (stderr, "copying from position %lld of file %s\n", position, curr_fname);
    }
    assert (lseek (curr_fd, position, SEEK_SET) == position);
    while (1) {
      int r = read (curr_fd, WB, WRITE_BUFFER_SIZE);
      if (r <= 0) { assert (!r); break; }
      int w = write (targ_fd, WB, r);
      assert (w == r);
    }
    if (verbosity > 0) {
      fprintf (stderr, "transferred %lld bytes from %s\n", lseek(curr_fd, 0, SEEK_CUR) - position, curr_fname);
    }
  }

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
