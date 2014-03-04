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
#include "kdb-text-binlog.h"
#include "text-index-layout.h"
#include "text-data.h"

#define	MAX_FILE_DICTIONARY_BYTES	(1 << 25)
#define	MAX_USERLIST_BYTES		(1 << 27)


#define	READ_BUFFER_SIZE	(1L << 24)
#define	WRITE_BUFFER_SIZE	(1L << 24)
#define	READ_THRESHOLD		(1L << 18)
#define	WRITE_THRESHOLD		(1L << 18)

char *progname = "text-log-merge", *username, *src_fname, *targ_fname, *curr_fname, *idx_fname;
int verbosity = 0;
int src_fd, targ_fd, curr_fd, idx_fd;
long long rd_bytes, wr_bytes, rd_rec, wr_rec, true_rd_bytes, true_wr_bytes;

int mode, t_now;
long long position, targ_orig_size, idx_size;

char *rptr, *rend, *wptr, *wst;
char RB[READ_BUFFER_SIZE+4], WB[WRITE_BUFFER_SIZE+4];

struct text_index_header Header;

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


int load_index_header (int fd, long long fsize) {
  int r;

  r = read (fd, &Header, sizeof (struct text_index_header));
  assert (r == sizeof (struct text_index_header));
  assert (Header.magic == TEXT_INDEX_MAGIC);

  assert ((unsigned) Header.sublists_num <= MAX_SUBLISTS);
  assert ((unsigned) Header.tot_users <= MAX_USERS);
  assert (Header.last_global_id >= 0);


  int userlist_entry_size = sizeof (struct file_user_list_entry) + 4 * Header.sublists_num;

  assert (Header.sublists_descr_offset >= sizeof (struct text_index_header));
  assert (Header.sublists_descr_offset + Header.sublists_num * 4 <= Header.word_char_dictionary_offset);
  assert (Header.word_char_dictionary_offset + sizeof (struct file_char_dictionary) <= Header.notword_char_dictionary_offset);
  assert (Header.notword_char_dictionary_offset + sizeof (struct file_char_dictionary) <= Header.word_dictionary_offset);
  assert (Header.notword_dictionary_offset >= Header.word_dictionary_offset + 4);
  assert (Header.notword_dictionary_offset <= Header.word_dictionary_offset + MAX_FILE_DICTIONARY_BYTES);
  assert (Header.user_list_offset >= Header.notword_dictionary_offset + 4);
  assert (Header.user_list_offset <= Header.notword_dictionary_offset + MAX_FILE_DICTIONARY_BYTES);
//  fprintf (stderr, "user_list_offset=%lld, tot_users=%d, userlist_entry_size=%d, user_data_offset=%lld\n");
  assert (Header.user_list_offset + Header.tot_users * userlist_entry_size + 16 <= Header.user_data_offset);
  assert (Header.user_data_offset <= Header.extra_data_offset);
  assert (Header.extra_data_offset <= Header.data_end_offset);
  assert (Header.data_end_offset == fsize);

  return 0;
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
  case LEV_TX_ADD_MESSAGE ... LEV_TX_ADD_MESSAGE + 0xff:
  case LEV_TX_ADD_MESSAGE_LF:
    if (size < sizeof (struct lev_add_message)) { return -2; }
    struct lev_add_message *EM = (void *) E;
    s = sizeof (struct lev_add_message) + EM->text_len + 1;
    if (size < s) { return -2; }
    if ((unsigned) EM->text_len >= MAX_TEXT_LEN || EM->text[EM->text_len]) { 
      return -4; 
    }
    return s;
  case LEV_TX_DEL_MESSAGE:
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
  }
   
  return -1;
}

int t_now = 0;

static int want_write (int type, void *ptr) {
  struct lev_generic *E = ptr;

  if (type == LEV_TIMESTAMP && E->a > t_now) {
    t_now = E->a;
    /*if (t_now > t_cutoff && !mode) {
      mode = 1;
      fprintf (stderr, "reached binlog time %d above cutoff time %d at read position %lld\n", t_now, t_cutoff, rd_bytes);
    }*/
  }

  if (
      (type >= LEV_TX_ADD_MESSAGE && type <= LEV_TX_ADD_MESSAGE + 0xff) ||
      (type == LEV_TX_ADD_MESSAGE_LF)
     ) {
    struct lev_add_message *EM = (struct lev_add_message *)E;

    if (!EM->legacy_id) {
      fprintf (stderr, "warning: binlog message (type=%02x, date=%d/%d, user_id=%d, peer_id=%d) has zero legacy id, ignored\n", EM->type & 0xff, EM->date, t_now, EM->user_id, EM->peer_id);
    }

    if (EM->legacy_id > Header.max_legacy_id) {
      mode |= 1;
      return 1;
    }
    if (EM->legacy_id < Header.min_legacy_id) {
      mode |= 2;
      return 2;
    }
  }

  if (type == LEV_TIMESTAMP) {
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
  fprintf (stderr, "usage:\t%s [-v] [-u<username>] <index-file> <old-binlog-file> [<output-binlog-file>]\n"
	   "\tAppends most recent messages from first binlog to another binlog. "
	   "If <output-binlog-file> is specified, resulting binlog is appended to it.\n"
	   "\t-h\tthis help screen\n"
	   "\t-v\tverbose mode on\n"
	   "\t-u<username>\tassume identity of given user\n",
	   progname);
}

int main (int argc, char *argv[]) {
  int i;
  progname = argv[0];
  while ((i = getopt (argc, argv, "hvu:")) != -1) {
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
    }
  }

  if (optind + 1 >= argc || optind + 3 < argc) {
    usage();
    return 2;
  }

  idx_fname = argv[optind];
  src_fname = argv[optind + 1];

  if (username && change_user (username) < 0) {
    fprintf (stderr, "fatal: cannot change user to %s\n", username ? username : "(none)");
    return 1;
  }

  idx_fd = open (idx_fname, O_RDONLY);
  if (idx_fd < 0) {
    fprintf (stderr, "cannot open index %s: %m\n", idx_fname);
    return 1;
  }

  idx_size = lseek (idx_fd, 0, SEEK_END);
  lseek (idx_fd, 0, SEEK_SET);

  load_index_header (idx_fd, idx_size);

  src_fd = open (src_fname, O_RDONLY);
  if (src_fd < 0) {
    fprintf (stderr, "cannot open %s: %m\n", src_fname);
    return 1;
  }

  if (optind + 2 < argc) {
    targ_fname = argv[optind+2];
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

  prepare_read ();
  for (i = 0; i <= 2; i++) {
    if (rend - rptr >= 4096 && *((int *) rptr) == 0x0473664b) {
      rptr += 4096;
    }
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
