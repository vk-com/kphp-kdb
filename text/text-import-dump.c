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

    Copyright 2009-2013 Vkontakte Ltd
              2009-2013 Nikolai Durov
              2009-2013 Andrei Lopatin
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
#include "kfs-layout.h"
#include "crc32.h"
#include "md5.h"

#define	TF_NONE		0
#define	TF_INBOX	1
#define	TF_OUTBOX	2
#define	TF_STATUSES	3
#define	TF_WALL		4
#define TF_VOTINGS	5
#define	TF_PHOTO_COMMENTS	6
#define	TF_VIDEO_COMMENTS	7

#define	READ_BUFFER_SIZE	(1 << 24)
#define	WRITE_BUFFER_SIZE	(1 << 24)
#define	READ_THRESHOLD		(1 << 17)
#define	WRITE_THRESHOLD		(1 << 18)

#define	MAX_USERS	(1 << 23)


char default_progname [] = "text-import-dump";
char *progname = default_progname, *username, *src_fname, *targ_fname;
int verbosity = 0, output_format = 0, table_format = 0, split_mod = 0, split_rem = 0, allow_negative = 0;
int split_min = 0, split_max = 0;
int Args_per_line;
int src_fd, targ_fd;
long long rd_bytes, wr_bytes;
unsigned tot_crc32 = -1;

int stdout_mode = 0;

int force_read_threshold;

char allowed[256];

int line_no, adj_rec;
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
    rd_bytes += a;
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
  tot_crc32 = crc32_partial (wst, b, tot_crc32);
  a = write (targ_fd, wst, b);
  if (a > 0) {
    wr_bytes += a;
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

#define MAXV 64
#define MAX_STRLEN 32768
char S[MAXV+1][MAX_STRLEN];
int L[MAXV+1];
long long I[MAXV+1];

int read_record (void) {
  int i, c = '\t', state;
  char *ptr, *end;
  prepare_read();
  assert (Args_per_line > 0 && Args_per_line < MAXV);
  if (rptr == rend) {
    return 0;
  }
  for (i = 0; i < Args_per_line && c != '\n'; i++) {
    assert (c == '\t');
    ptr = &S[i][0];
    end = ptr + MAX_STRLEN - 2;
    state = 0;
    do {
      assert (rptr < rend && ptr < end);
      c = *rptr++;
      if (c == '\n' || c == '\t') {
	if (state == '\\') {
	  ptr--;
	} else {
	  break;
	}
      }
      if (c == '\\' && state == '\\') {
	state = 0;
	ptr--;
      } else {
	state = c;
      }
      *ptr++ = c;
    } while (1);
    *ptr = 0;
    L[i] = ptr - S[i];
    I[i] = atoll(S[i]);
  }
  assert (i == Args_per_line);
  line_no++;
  return 1;
}

int get_dump_format (char *str) {
  if (!strncmp (str, "inbox", 5)) {
    return TF_INBOX;
  }
  if (!strncmp (str, "outbox", 6)) {
    return TF_OUTBOX;
  }
  if (!strncmp (str, "status", 6)) {
    return TF_STATUSES;
  }
  if (!strncmp (str, "minifeed", 8)) {
    return TF_STATUSES;
  }
  if (!strncmp (str, "wall", 4)) {
    return TF_WALL;
  }
  if (!strncmp (str, "voting", 6)) {
    return TF_VOTINGS;
  }
  if (!strncmp (str, "photos_comments", 15)) {
    return TF_PHOTO_COMMENTS;
  }
  if (!strncmp (str, "videos_comments", 15)) {
    return TF_VIDEO_COMMENTS;
  }

  return TF_NONE;
}

char *fname_last (char *ptr) {
  char *s = ptr;
  while (*ptr) {
    if (*ptr++ == '/') {
      s = ptr;
    }
  }
  return s;
}

/********** some binlog functions ***********/

int binlog_existed = 0;
int binlog_headers;
struct kfs_file_header kfs_Hdr[3], *KHDR;

struct lev_start *ST;
struct lev_rotate_from *CONT;

void randomize (void) {
  static char RB[16];
  int rfd = open ("/dev/urandom", O_RDONLY);
  assert (read (rfd, RB, sizeof (long)) == sizeof (long));
  srand48 (*((long *) RB));
}

kfs_hash_t kfs_random_hash (void) {
  return (((kfs_hash_t) lrand48()) << 32) | ((unsigned) lrand48());
}

kfs_hash_t kfs_string_hash (const char *str) {
  static union {
    unsigned char output[16];
    kfs_hash_t hash;
  } tmp;
  md5 ((unsigned char *) str, strlen (str), tmp.output);
  return tmp.hash;
}

int check_kfs_header_basic (struct kfs_file_header *H) {
  assert (H->magic == KFS_MAGIC);
  if (compute_crc32 (H, 4092) != H->header_crc32) {
    return -1;
  }
  if (H->kfs_version != KFS_V01) {
    return -1;
  }
  return 0;
}

void create_kfs_header_basic (struct kfs_file_header *H) {
  memset (H, 0, 4096);
  H->magic = KFS_MAGIC;
  H->kfs_version = KFS_V01;
  H->finished = -1;
}

void fix_kfs_header_crc32 (struct kfs_file_header *H) {
  H->header_crc32 = compute_crc32 (H, 4092);
}


int lock_whole_file (int fd, int mode) {
  static struct flock L;
  L.l_type = mode;
  L.l_whence = SEEK_SET;
  L.l_start = 0;
  L.l_len = 0;
  if (fcntl (fd, F_SETLK, &L) < 0) {
    fprintf (stderr, "cannot lock file %d: %m\n", fd);
    return -1;
  }
  return 1;
}

int load_binlog_headers (int fd) {
  int r = read (fd, kfs_Hdr, 4096 * 3);
  struct lev_start *E;
  assert (sizeof (struct kfs_file_header) == 4096);
  binlog_headers = 0;
  binlog_existed = 0;
  ST = 0;
  CONT = 0;
  KHDR = 0;
  if (!r) {
    return 0;
  }
  if (r >= 4096 && kfs_Hdr[0].magic == KFS_MAGIC) {
    if (check_kfs_header_basic (kfs_Hdr) < 0 || kfs_Hdr[0].kfs_file_type != kfs_binlog) {
      fprintf (stderr, "bad kfs header #0\n");
      return -1;
    }
    binlog_headers++;
    if (r >= 8192 && kfs_Hdr[1].magic == KFS_MAGIC) {
      if (check_kfs_header_basic (kfs_Hdr + 1) < 0 || kfs_Hdr[1].kfs_file_type != kfs_binlog) {
        fprintf (stderr, "bad kfs header #1\n");
        return -1;
      }
      binlog_headers++;
      if (kfs_Hdr[1].header_seq_num == kfs_Hdr[0].header_seq_num) {
        assert (!memcmp (kfs_Hdr + 1, kfs_Hdr, 4096));
      }
    }
  }
  r -= binlog_headers * 4096;
  if (r < 4) {
    fprintf (stderr, "no first entry in binlog\n");
    return -1;
  }
  E = (struct lev_start *) (kfs_Hdr + binlog_headers);

  switch (E->type) {
  case LEV_START:
    assert (r >= sizeof (struct lev_start));
    ST = E;
    break;
  case LEV_ROTATE_FROM:
    assert (r >= sizeof (struct lev_rotate_from));
    CONT = (struct lev_rotate_from *) E;
    break;
  default:
    fprintf (stderr, "fatal: binlog file begins with wrong entry type %08x\n", E->type);
    return -1;
  }

  binlog_existed = 1;

  if (!binlog_headers) {
    return 0;
  }

  KHDR = kfs_Hdr;
  if (binlog_headers > 1 && kfs_Hdr[1].header_seq_num > kfs_Hdr[0].header_seq_num) {
    KHDR++;
  }

  assert (KHDR->data_size + binlog_headers * 4096 == KHDR->raw_size);
  assert (lseek (fd, 0, SEEK_END) == KHDR->raw_size);

  tot_crc32 = ~KHDR->data_crc32;

  if (KHDR->finished == -1) {
    fprintf (stderr, "fatal: incomplete kfs file\n");
    return -1;
  }

  if (ST) {
    if (ST->schema_id != KHDR->schema_id) {
      fprintf (stderr, "fatal: binlog schema id mismatch.\n");
      return -1;
    }
    if (ST->split_min != KHDR->split_min || ST->split_max != KHDR->split_max || ST->split_mod != KHDR->split_mod) {
      fprintf (stderr, "fatal: binlog slice parameters mismatch.\n");
      return -1;
    }
    if (KHDR->log_pos) {
      fprintf (stderr, "fatal: first binlog file has non-zero log_pos %lld\n", KHDR->log_pos);
      return -1;
    }
  }

  if (CONT) {
    if (KHDR->log_pos != CONT->cur_log_pos) {
      fprintf (stderr, "fatal: continuation binlog file log_pos mismatch: %lld != %lld\n", KHDR->log_pos, CONT->cur_log_pos);
      return -1;
    }
    if (KHDR->prev_log_hash != CONT->prev_log_hash) {
      fprintf (stderr, "fatal: binlog file prev_log_hash mismatch: %016llx != %016llx\n", KHDR->prev_log_hash, CONT->prev_log_hash);
      return -1;
    }
  }

  return binlog_headers;
}

void create_binlog_headers0 (int schema_id, char *schema_str, int schema_strlen) {
  assert (!binlog_headers && !KHDR);

  if (1 || stdout_mode) {
    return;
  }

  binlog_headers = 2;
  create_kfs_header_basic (kfs_Hdr);

  KHDR = kfs_Hdr;

  KHDR->kfs_file_type = kfs_binlog;
  KHDR->modified = time (0);
  KHDR->header_seq_num++;

  KHDR->file_id_hash = kfs_random_hash ();
  KHDR->replica_id_hash = KHDR->slice_id_hash = kfs_random_hash ();

  switch (table_format) {
  case TF_INBOX:
  case TF_OUTBOX:
    KHDR->table_id_hash = kfs_string_hash ("messages");
    strcpy (KHDR->table_name, "messages");
    sprintf (KHDR->slice_name, "messages%03d", split_min);
    break;
  case TF_WALL:
  case TF_STATUSES:
    KHDR->table_id_hash = kfs_string_hash ("statuses");
    strcpy (KHDR->table_name, "statuses");
    sprintf (KHDR->slice_name, "statuses%03d", split_min);
    break;
  default:
    assert (0);
  }

  KHDR->raw_size = 8192;
  KHDR->replica_created = KHDR->slice_created = KHDR->created = KHDR->modified;

  KHDR->total_copies = 1;

  strcpy (KHDR->creator, default_progname);
  strcpy (KHDR->slice_creator, default_progname);

  KHDR->schema_id = schema_id;
  if (schema_str) {
    KHDR->extra_bytes = schema_strlen;
    if (KHDR->extra_bytes > 512) {
      KHDR->extra_bytes = 512;
    }
    memcpy (KHDR->schema_extra, schema_str, KHDR->extra_bytes);
  }

  KHDR->split_mod = split_mod;
  KHDR->split_min = split_min;
  KHDR->split_max = split_max;

  fix_kfs_header_crc32 (KHDR);

  assert (lseek (targ_fd, 0, SEEK_SET) == 0);
  assert (write (targ_fd, kfs_Hdr, 4096) == 4096);
  assert (write (targ_fd, kfs_Hdr, 4096) == 4096);
}

void write_binlog_headers (void) {
  int i;

  if (!KHDR) {
    return;
  }
 
  KHDR->modified = time (0);
  ++KHDR->header_seq_num;
  KHDR->finished = 0;
  KHDR->data_crc32 = ~tot_crc32;
  KHDR->data_size += wr_bytes;
  KHDR->raw_size += wr_bytes;

  fix_kfs_header_crc32 (KHDR);

  assert (lseek (targ_fd, 0, SEEK_SET) == 0);
  for (i = 0; i < binlog_headers; i++) {
    assert (write (targ_fd, kfs_Hdr, 4096) == 4096);
  }
}

void start_binlog (int schema_id, char *str, int strlen) {
  int len = str ? strlen+1 : 0;
  int extra = (len + 3) & -4;
  if (len == 1) { extra = len = 0; }

  assert (!len || !str[strlen]);

  if (binlog_existed) {
    if (ST) {
      assert (ST->schema_id == schema_id && ST->split_mod == split_mod && ST->split_min == split_min && ST->split_max == split_max);
    }
    if (KHDR) {
      assert (KHDR->schema_id == schema_id && KHDR->split_mod == split_mod && KHDR->split_min == split_min && KHDR->split_max == split_max);
    }
    return;
  }

  create_binlog_headers0 (schema_id, str, strlen);

  struct lev_start *E = write_alloc (sizeof (struct lev_start) - 4 + extra);
  E->type = LEV_START;
  E->schema_id = schema_id;
  E->extra_bytes = extra;
  E->split_mod = split_mod;
  E->split_min = split_rem;
  E->split_max = split_rem + 1;
  if (len) {
    memcpy (E->str, str, len);
  }
}

/*
 *
 *  TABLE/HEAP FUNCTIONS
 *
 */

#define	MAX_GROUPS	(1 << 30)
#define	MAX_GID		(1 << 24)
#define MAX_VOTINGS	(1 << 23)

char *groups_fname, *groups_fname2;
char GT[MAX_GROUPS], *GA, *GB, *GC = GT, *GS = GT;
int Gc, Gd;

typedef struct voting_data {
  int owner_id;
  int topic_id;
  int voting_id;
} voting_t;

voting_t VD[MAX_VOTINGS];
int VN;

void *gmalloc (unsigned size) {
  void *res;
  assert (size <= MAX_GROUPS);
  assert (GS >= GT && GS <= GT + MAX_GROUPS - 8);
  res = GS += (- (long) GS) & 3;
  assert (GT + MAX_GROUPS - GS >= size);
  GS += size;
  return res;
}

/*
 * BEGIN MAIN
 */

int list_id;

int conv_uid (int id) {
  if (id < 0 && allow_negative) {
    id = -id;
  }
  if (id <= 0) {
    return -1;
  }
  return id % split_mod == split_rem ? id / split_mod : -1;
}

void log_0ints (long type) {
  struct lev_generic *L = write_alloc (8);
  L->type = type;
  L->a = list_id;
}
  
void log_1int (long type, long arg) {
  struct lev_generic *L = write_alloc (12);
  L->type = type;
  L->a = list_id;
  L->b = arg;
}
  
void log_2ints (long type, long arg1, long arg2) {
  struct lev_generic *L = write_alloc (16);
  L->type = type;
  L->a = list_id;
  L->b = arg1;
  L->c = arg2;
}

int is_friend (int x, int y) {
  int a = -1, b = ((GB - GA) >> 3), c;
  int *Q = (int *) GA;
  while (b - a > 1) {
    c = ((a + b) >> 1);
    if (Q[2*c] < x || (Q[2*c] == x && Q[2*c+1] <= y)) {
      a = c;
    } else {
      b = c;
    }
  }
  return (a >= 0 && Q[2*a] == x && Q[2*a+1] == y);
}

/* inbox */

enum {
  ib_id, ib_from_id, ib_to_id, ib_to_reply, ib_date, ib_read_date,
  ib_ip, ib_port, ib_front, ib_ua_hash,
  ib_read_state, ib_to_shown, ib_replied, ib_title, ib_message,
  ib_END
};

void process_inbox_row (void) {
  int user_id = I[ib_from_id];
  int i, len;
  char *ptr, *str;
  list_id = I[ib_to_id]; 
  if (conv_uid (list_id) < 0 || list_id <= 0 || user_id <= 0) {
    return;
  }
  if (I[ib_date] && I[ib_date] < force_read_threshold) {
    I[ib_read_state] = 1;
  }
  if (L[ib_title] == 2 && !strcmp (S[ib_title], "\\N")) {
    L[ib_title] = 3;
    strcpy (S[ib_title], "...");
  }
  if (L[ib_message] == 2 && !strcmp (S[ib_message], "\\N")) {
    L[ib_message] = 0;
    S[ib_message][0] = 0;
  }
  struct lev_add_message *E = write_alloc (sizeof (struct lev_add_message) + L[ib_title] + L[ib_message] + 2);
  E->type = LEV_TX_ADD_MESSAGE + (I[ib_read_state] ? 0 : TXF_UNREAD) + (I[ib_to_shown] ? TXF_SPAM : 0) + (is_friend (list_id, user_id) ? TXF_FRIENDS : 0);
  E->user_id = list_id;
  E->legacy_id = I[ib_id];
  E->peer_id = user_id;
  E->peer_msg_id = (I[ib_read_state] ? 0 : -I[ib_read_date]);
  E->date = I[ib_date];
  E->ip = I[ib_ip];
  E->port = I[ib_port];
  E->front = I[ib_front];
  E->ua_hash = strtoull (S[ib_ua_hash], 0, 10);
  E->text_len = L[ib_title] + 1 + L[ib_message];
  ptr = E->text;
  len = L[ib_title];
  str = S[ib_title];
  for (i = 0; i < len; i++) {
    if ((unsigned char) str[i] < ' ') {
      *ptr++ = ' ';
    } else {
      *ptr++ = str[i];
    }
  }
  *ptr++ = 9;
  len = L[ib_message];
  str = S[ib_message];
  for (i = 0; i < len; i++) {
    if ((unsigned char) str[i] < ' ' && str[i] != '\t') {
      *ptr++ = ' ';
    } else {
      *ptr++ = str[i];
    }
  }
  *ptr++ = 0;

  adj_rec++;
}

/* outbox */

enum {
  ob_id, ob_from_id, ob_to_id, ob_to_reply, ob_date, ob_read_date,
  ob_ip, ob_port, ob_front, ob_ua_hash,
  ob_read_state, ob_to_shown, ob_replied, ob_title, ob_message,
  ob_END
};

void process_outbox_row (void) {
  int user_id = I[ob_to_id];
  int i, len;
  char *ptr, *str;
  list_id = I[ob_from_id]; 
  if (conv_uid (list_id) < 0 || list_id <= 0 || user_id <= 0) {
    return;
  }
  if (L[ob_title] == 2 && !strcmp (S[ob_title], "\\N")) {
    L[ob_title] = 3;
    strcpy (S[ob_title], "...");
  }
  if (L[ob_message] == 2 && !strcmp (S[ob_message], "\\N")) {
    L[ob_message] = 0;
    S[ob_message][0] = 0;
  }
  struct lev_add_message *E = write_alloc (sizeof (struct lev_add_message) + L[ob_title] + L[ob_message] + 2);
  E->type = LEV_TX_ADD_MESSAGE + (I[ob_read_state] ? 0 : TXF_UNREAD) + TXF_OUTBOX + (is_friend (list_id, user_id) ? TXF_FRIENDS : 0);
  E->user_id = list_id;
  E->legacy_id = -I[ob_id];
  E->peer_id = user_id;
  E->peer_msg_id = 0;
  E->date = I[ob_date];
  E->ip = I[ob_ip];
  E->port = I[ob_port];
  E->front = I[ob_front];
  E->ua_hash = strtoull (S[ob_ua_hash], 0, 10);
  E->text_len = L[ob_title] + 1 + L[ob_message];
  ptr = E->text;
  len = L[ob_title];
  str = S[ob_title];
  for (i = 0; i < len; i++) {
    if ((unsigned char) str[i] < ' ') {
      *ptr++ = ' ';
    } else {
      *ptr++ = str[i];
    }
  }
  *ptr++ = 9;
  len = L[ob_message];
  str = S[ob_message];
  for (i = 0; i < len; i++) {
    if ((unsigned char) str[i] < ' ' && str[i] != '\t') {
      *ptr++ = ' ';
    } else {
      *ptr++ = str[i];
    }
  }
  *ptr++ = 0;

  adj_rec++;
}


/* wall */

enum {
  wa_id, wa_from_id, wa_to_id, wa_date, wa_to_shown,
  wa_ip, wa_port, wa_front, wa_hash,
  wa_message,
  wa_END
};

int check_wall_embedded_media (char *str, int len) {
  int x = -1;

  switch (str[2]) {
  case 'a':
    if (len >= 10 && !memcmp (str + 2, "audio", 5)) {
      if (sscanf (str, "[[audio%*u]]%n", &x) >= 0 && x >= 0) {
        return 1;
      }
      if (sscanf (str, "[[audio%*d_%*u]]%n", &x) >= 0 && x >= 0) {
        return 1;
      }
      break;
    }
    if (len >= 13 && !memcmp (str + 2, "app_post", 8)) {
      int y = -1;
      if (sscanf (str, "[[app_post%*u|%n", &x) < 0 || x < 0) {
        return 0;
      }
      switch (str[x]) {
      case 'a':
        sscanf (str + x, "a_%*u_%*u_%*u|%n", &y);
        break;
      case 'p':
        sscanf (str + x, "p_%*u_%*u_%*u_%*[0-9a-z]|%n", &y);
        break;
      }
      if (y < 0) {
        return 0;
      }
      x += y;
      if (str[x] == ']' && str[x+1] == ']') {
        return 5;
      }
      y = -1;
      if (sscanf (str + x, "%*[0-9a-z]]]%n", &y) >= 0 && y >= 0) {
        return 5;
      }
    }
    break;
  case 'g':
    if (len >= 13 && !memcmp (str + 2, "graffiti", 8)) {
      if (sscanf (str, "[[graffiti%*u]]%n", &x) >= 0 && x >= 0) {
        return 1;
      }
    }
    break;
  case 'p':
    if (len >= 10 && !memcmp (str + 2, "photo", 5)) {
      if (sscanf (str, "[[photo%*d_%*u]]%n", &x) >= 0 && x >= 0) {
        return 1;
      }
      break;
    }
    if (len >= 23 && !memcmp (str + 2, "posted_photo", 12)) {
      if (sscanf (str, "[[posted_photo%*u_%*u_%*[0-9a-z]_%*u]]%n", &x) >= 0 && x >= 0) {
        return 1;
      }
    }
    break;
  case 'v':
    if (len >= 10 && !memcmp (str + 2, "video", 5)) {
      if (sscanf (str, "[[video%*u]]%n", &x) >= 0 && x >= 0) {
        return 1;
      }
      if (sscanf (str, "[[video%*d_%*u]]%n", &x) >= 0 && x >= 0) {
        return 1;
      }
    }
    break;
  }
  return 0;
}

void process_wall_row (void) {
  int user_id = I[wa_from_id];
  int i, len, has_media = 0;
  char *ptr, *str;
  list_id = I[wa_to_id]; 
  if (conv_uid (list_id) < 0 || !list_id || user_id <= 0) {
    return;
  }
  if (L[wa_message] == 2 && !strcmp (S[wa_message], "\\N")) {
    L[wa_message] = 0;
    S[wa_message][0] = 0;
  }
  if (L[wa_message] >= 5 && S[wa_message][0] == '[' && S[wa_message][1] == '[') {
    has_media = check_wall_embedded_media (S[wa_message], L[wa_message]);
    if (verbosity > 2 && !has_media) {
      fprintf (stderr, "has_media=%d for '%s'\n", has_media, S[wa_message]);
    }
  }
  struct lev_add_message *E = write_alloc (sizeof (struct lev_add_message) + L[wa_message] + 1);
  E->type = (has_media ? LEV_TX_ADD_MESSAGE_MF + has_media * TXFS_MEDIA : LEV_TX_ADD_MESSAGE) + 
            TXFS_WALL + (I[wa_to_shown] ? TXFS_SPAM : 0);
  E->user_id = list_id;
  E->legacy_id = I[wa_id];
  E->peer_id = (user_id == list_id ? (int) 2e9 : user_id);
  E->peer_msg_id = 0;
  E->date = I[wa_date];
  E->ip = I[wa_ip];
  E->port = I[wa_port];
  E->front = I[wa_front];
  E->ua_hash = strtoull (S[wa_hash], 0, 10);
  E->text_len = len = L[wa_message];
  ptr = E->text;
  str = S[wa_message];
  for (i = 0; i < len; i++) {
    if ((unsigned char) str[i] < ' ' && str[i] != '\t') {
      *ptr++ = ' ';
    } else {
      *ptr++ = str[i];
    }
  }
  *ptr++ = 0;

  adj_rec++;
}

/* statuses */

enum {
  st_id, st_user_id, st_created, st_item_text,
  st_END
};

void process_minifeed_row (void) {
  int i, len, skip = 0;
  char *ptr, *str;
  list_id = I[st_user_id]; 
  if (conv_uid (list_id) < 0 || list_id <= 0) {
    return;
  }
  if (L[st_item_text] == 2 && !strcmp (S[st_item_text], "\\N")) {
    L[st_item_text] = 0;
    S[st_item_text][0] = 0;
  }
  if (L[st_item_text] <= 2 && !I[st_created]) {
    return;
  }
  skip = (S[st_item_text][0] == ' ');
  struct lev_add_message *E = write_alloc (sizeof (struct lev_add_message) + L[st_item_text] + 1 - skip);
  E->type = (skip ? LEV_TX_ADD_MESSAGE_MF + TXFS_SMS : LEV_TX_ADD_MESSAGE);
  E->user_id = list_id;
  E->legacy_id = I[st_id];
  E->peer_id = (int) 2e9;
  E->peer_msg_id = 0;
  E->date = I[st_created];
  E->ip = 0;
  E->port = 0;
  E->front = 0;
  E->ua_hash = 0;
  E->text_len = len = L[st_item_text] - skip;
  ptr = E->text;
  str = S[st_item_text] + skip;
  for (i = 0; i < len; i++) {
    if ((unsigned char) str[i] < ' ' && str[i] != '\t') {
      *ptr++ = ' ';
    } else {
      *ptr++ = str[i];
    }
  }
  *ptr++ = 0;

  adj_rec++;
}

/* votings */

enum {
  vt_id, vt_question, vt_owner_id, vt_user_id, vt_is_multiple, 
  vt_place_type, vt_place_id, vt_created, vt_anonymous, vt_who_can_vote, 
  vt_closed, vt_max_user_id,
  vt_END
};

void process_votings_row (void) {
  int owner_id = I[vt_owner_id], topic_id = I[vt_place_id], voting_id = I[vt_id];
  if (I[vt_place_type] != 2 || owner_id >= 0 || topic_id <= 0 || voting_id <= 0) {
    return;
  }
  assert (VN < MAX_VOTINGS);
  VD[VN].owner_id = owner_id;
  VD[VN].topic_id = topic_id;
  VD[VN].voting_id = voting_id;
  VN++;

  adj_rec++;
}

static inline int cmp_voting (voting_t *x, voting_t *y) {
  if (x->owner_id < y->owner_id) {
    return -1;
  } else if (x->owner_id > y->owner_id) {
    return 1;
  } else if (x->topic_id < y->topic_id) {
    return -1;
  } else if (x->topic_id > y->topic_id) {
    return 1;
  } else if (x->voting_id < y->voting_id) {
    return -1;
  } else if (x->voting_id > y->voting_id) {
    return 1;
  } else {
    return 0;
  }
}

static void sort_votings (voting_t *A, int b) {
  if (b <= 0) {
    return;
  }
  int i = 0, j = b;
  voting_t h = A[b >> 1], t;
  do {
    while (cmp_voting (&A[i], &h) < 0) { i++; }
    while (cmp_voting (&h, &A[j]) < 0) { j--; }
    if (i <= j) {
      t = A[i];  A[i++] = A[j];  A[j--] = t;
    }
  } while (i <= j);
  sort_votings (A + i, b - i);
  sort_votings (A, j);
}

static void kill_dup_votings (void) {
  int i, j = 1;
  if (!VN) {
    return;
  }
  for (i = 1; i < VN; i++) {
    if (cmp_voting (&VD[i-1], &VD[i])) {
      VD[j++] = VD[i];
    }
  }
  if (verbosity > 0) {
    fprintf (stderr, "deleted %d duplicate records\n", VN - j);
  }
  VN = j;
}

/* photo_comments */

enum {
  pc_id, pc_from_id, pc_to_id, pc_photo_id, pc_owner_id, pc_parent_id, pc_parent_local_id, pc_local_id,
  pc_date, pc_ip, pc_port, pc_front, pc_ua_hash, pc_message,
  pc_END
};

void process_photo_comments_row (void) {
  int i, len, from_id = I[pc_from_id];
  int extra_id = I[pc_local_id], extra_bytes = (extra_id ? 4 : 0);
  char *ptr, *str;
  list_id = I[pc_owner_id];
  if (conv_uid (list_id) < 0 || !list_id || !from_id) {
    return;
  }
  if (L[pc_message] == 2 && !strcmp (S[pc_message], "\\N")) {
    L[pc_message] = 0;
    S[pc_message][0] = 0;
  }
  static char kludge[32];
  int kludge_bytes = from_id < 0 ? sprintf (kludge, "\x1ras %d\t", list_id) : 0;
  struct lev_add_message *E = write_alloc (sizeof (struct lev_add_message) + extra_bytes + kludge_bytes + L[pc_message] + 1);
  E->type = extra_bytes ? LEV_TX_ADD_MESSAGE_EXT_ZF + 1 : LEV_TX_ADD_MESSAGE;
  E->user_id = list_id;
  E->legacy_id = I[pc_id];
  E->peer_id = I[pc_photo_id];
  E->peer_msg_id = (from_id >= 0 ? from_id : -from_id);
  E->date = I[pc_date];
  E->ip = I[pc_ip];
  E->port = I[pc_port];
  E->front = I[pc_front];
  E->ua_hash = strtoull (S[pc_ua_hash], 0, 10);
  len = L[pc_message];
  E->text_len = kludge_bytes + len;
  ptr = E->text;
  if (extra_bytes) {
    *((int *) ptr) = extra_id;
    ptr += 4;
  }
  if (kludge_bytes) {
    memcpy (ptr, kludge, kludge_bytes);
    ptr += kludge_bytes;
  }
  str = S[pc_message];
  for (i = 0; i < len; i++) {
    if ((unsigned char) str[i] < ' ' && str[i] != '\t') {
      *ptr++ = ' ';
    } else {
      *ptr++ = str[i];
    }
  }
  *ptr++ = 0;

  adj_rec++;
}

/* video_comments */

enum {
  vc_id, vc_from_id, vc_to_id, vc_video_id, vc_owner_id, vc_folder_id, vc_parent_local_id, vc_local_id, 
  vc_from_shown, vc_date, vc_rate, vc_reply_to, 
  vc_ip, vc_port, vc_front, vc_ua_hash, vc_message,
  vc_END
};

void process_video_comments_row (void) {
  int i, len, from_id = I[vc_from_id];
  int extra_id = I[vc_local_id], extra_bytes = (extra_id ? 4 : 0);
  char *ptr, *str;
  list_id = I[vc_owner_id];
  if (conv_uid (list_id) < 0 || !list_id || !from_id) {
    return;
  }
  if (L[vc_message] == 2 && !strcmp (S[vc_message], "\\N")) {
    L[vc_message] = 0;
    S[vc_message][0] = 0;
  }
  static char kludge[32];
  int kludge_bytes = from_id < 0 ? sprintf (kludge, "\x1ras %d\t", list_id) : 0;
  struct lev_add_message *E = write_alloc (sizeof (struct lev_add_message) + extra_bytes + kludge_bytes + L[vc_message] + 1);
  E->type = extra_bytes ? LEV_TX_ADD_MESSAGE_EXT_ZF + 1 : LEV_TX_ADD_MESSAGE;
  E->user_id = list_id;
  E->legacy_id = I[vc_id];
  E->peer_id = I[vc_video_id];
  E->peer_msg_id = (from_id >= 0 ? from_id : -from_id);
  E->date = I[vc_date];
  E->ip = I[vc_ip];
  E->port = I[vc_port];
  E->front = I[vc_front];
  E->ua_hash = strtoull (S[vc_ua_hash], 0, 10);
  len = L[vc_message];
  E->text_len = kludge_bytes + len;
  ptr = E->text;
  if (extra_bytes) {
    *((int *) ptr) = extra_id;
    ptr += 4;
  }
  if (kludge_bytes) {
    memcpy (ptr, kludge, kludge_bytes);
    ptr += kludge_bytes;
  }
  str = S[vc_message];
  for (i = 0; i < len; i++) {
    if ((unsigned char) str[i] < ' ' && str[i] != '\t') {
      *ptr++ = ' ';
    } else {
      *ptr++ = str[i];
    }
  }
  *ptr++ = 0;

  adj_rec++;
}

/*
 * END MAIN
 */

void output_stats (void) {
  fprintf (stderr,
	   "read: %lld bytes, %d records read, %d processed\n"
	   "written: %lld bytes\n"
	   "temp data: %ld bytes allocated, %d+%d in read/write maps\n",
	   rd_bytes, line_no, adj_rec, wr_bytes, (long)(GS - GC), Gc, Gd
	  );
}


void usage (void) {
  fprintf (stderr, "usage:\t%s [-v] [-n] [-t<days>] [-u<username>] [-m<rem>,<mod>] [-g<filename>] [-f<format>] [-o<output-class>] <input-file> [<output-file>]\n"
	   "\tConverts tab-separated table dump into KDB binlog. "
	   "If <output-file> is specified, resulting binlog is appended to it.\n"
	   "\t-h\tthis help screen\n"
	   "\t-v\tverbose mode on\n"
	   "\t-u<username>\tassume identity of given user\n"
	   "\t-g<filename>\tloads auxiliary data from given file\n"
	   "\t-m<rem>,<mod>\tslice parameters: consider only objects with id %% mod == rem\n"
	   "\t-n\tindex objects with negative ids\n"
	   "\t-t<days>\tmark old messages read\n"
	   "\t-o<int>\tdetermines output format\n"
	   "\t-f<format>\tdetermines dump format, one of inbox, outbox, minifeed, wall, ...\n",
	   progname);
}

int main (int argc, char *argv[]) {
  int i;
  progname = argv[0];
  while ((i = getopt (argc, argv, "hvnu:m:f:g:o:t:")) != -1) {
    switch (i) {
    case 'v':
      verbosity++;
      break;
    case 'h':
      usage();
      return 2;
    case 'n':
      allow_negative = 1;
      break;
    case 'm':
      assert (sscanf(optarg, "%d,%d", &split_rem, &split_mod) == 2);
      assert (split_mod > 0 && split_mod <= 1000 && split_rem >= 0 && split_rem < split_mod);
      break;
    case 'f':
      table_format = get_dump_format(optarg);
      if (!table_format) {
	fprintf (stderr, "fatal: unsupported table dump format: %s\n", optarg);
	return 2;
      }
      break;
    case 'o':
      output_format = atol (optarg);
      break;
    case 't':
      force_read_threshold = atol (optarg);
      break;
    case 'g':
      if (groups_fname) {
        groups_fname2 = optarg;
      } else {
        groups_fname = optarg;
      }
      break;
    case 'u':
      username = optarg;
      break;
    }
  }

  split_min = split_rem;
  split_max = split_rem + 1;

  if (optind >= argc || optind + 2 < argc) {
    usage();
    return 2;
  }

  if (force_read_threshold > 0) {
    force_read_threshold = time(0) - 86400 * force_read_threshold;
  }

  src_fname = argv[optind];

  if (username && change_user (username) < 0) {
    fprintf (stderr, "fatal: cannot change user to %s\n", username ? username : "(none)");
    return 1;
  }

  if (groups_fname) {
    src_fd = open (groups_fname, O_RDONLY);
    if (src_fd < 0) {
      fprintf (stderr, "cannot open %s: %m\n", groups_fname);
      return 1;
    }
    Gc = read (src_fd, GT, MAX_GROUPS);
    if (verbosity > 0) {
      fprintf (stderr, "read %d bytes from %s\n", Gc, groups_fname);
    }
    assert (Gc >= 0 && Gc < MAX_GROUPS);
    close (src_fd);
    src_fd = 0;
    GA = GT;
    GS = GC = GB = GA + ((Gc + 3) & -4);
  }

  if (groups_fname2) {
    src_fd = open (groups_fname2, O_RDONLY);
    if (src_fd < 0) {
      fprintf (stderr, "cannot open %s: %m\n", groups_fname2);
      return 1;
    }
    Gd = read (src_fd, GB, GA + MAX_GROUPS - GB);
    if (verbosity > 0) {
      fprintf (stderr, "read %d bytes from %s\n", Gd, groups_fname2);
    }
    assert (Gd >= 0 && Gd < MAX_GROUPS);
    close (src_fd);
    src_fd = 0;
    GS = GC = GB + ((Gd + 3) & -4);
  }

  src_fd = open (src_fname, O_RDONLY);
  if (src_fd < 0) {
    fprintf (stderr, "cannot open %s: %m\n", src_fname);
    return 1;
  }

  if (!table_format) {
    table_format = get_dump_format (fname_last (src_fname));
    if (!table_format) {
      fprintf (stderr, "fatal: cannot determine table type from filename %s\n", src_fname);
    }
  }

  if (optind + 1 < argc) {
    targ_fname = argv[optind+1];
    targ_fd = open (targ_fname, O_RDWR);
    if (targ_fd >= 0 && table_format != TF_VOTINGS) {
      assert (lock_whole_file (targ_fd, F_WRLCK) > 0);
      if (load_binlog_headers (targ_fd) < 0) {
        fprintf (stderr, "fatal: bad binlog headers of %s\n", targ_fname);
      }
      lseek (targ_fd, 0, SEEK_END);
    } else {
      targ_fd = open (targ_fname, O_WRONLY | O_CREAT | O_EXCL, 0644);
      if (targ_fd < 0) {
        fprintf (stderr, "cannot create %s: %m\n", targ_fname);
        return 1;
      }
      assert (lock_whole_file (targ_fd, F_WRLCK) > 0);
      ftruncate (targ_fd, 0);
      lseek (targ_fd, 0, SEEK_SET);
    }
  } else {
    targ_fname = "stdout";
    targ_fd = 1;
    stdout_mode = 1;
  }

  randomize ();

  switch (table_format) {
  case TF_INBOX:
    assert (split_mod);
    Args_per_line = ib_END;
    start_binlog(TEXT_SCHEMA_V1, "", 0);
    while (read_record() > 0) {
      process_inbox_row();
    }
    break;
  case TF_OUTBOX:
    assert (split_mod);
    Args_per_line = ob_END;
    start_binlog(TEXT_SCHEMA_V1, "", 0);
    while (read_record() > 0) {
      process_outbox_row();
    }
    break;
  case TF_WALL:
    assert (split_mod);
    Args_per_line = wa_END;
    start_binlog(TEXT_SCHEMA_V1, "\x1\x3\x0status", 9);
    while (read_record() > 0) {
      process_wall_row();
    }
    break;
  case TF_STATUSES:
    assert (split_mod);
    Args_per_line = st_END;
    start_binlog(TEXT_SCHEMA_V1,  "\x1\x3\x0status", 9);
    while (read_record() > 0) {
      process_minifeed_row();
    }
    break;
  case TF_VOTINGS:
    Args_per_line = vt_END;
    while (read_record() > 0) {
      process_votings_row();
    }
    sort_votings (VD, VN - 1);
    kill_dup_votings ();
    assert (write (targ_fd, VD, VN * 12) == VN * 12);
    break;
  case TF_PHOTO_COMMENTS:
    assert (split_mod);
    Args_per_line = pc_END;
    start_binlog (TEXT_SCHEMA_V1, "\x1\x1\0comments_photo", 17);
    while (read_record() > 0) {
      process_photo_comments_row ();
    }
    break;
  case TF_VIDEO_COMMENTS:
    assert (split_mod);
    Args_per_line = vc_END;
    start_binlog (TEXT_SCHEMA_V1, "\x1\x1\0comments_video", 17);
    while (read_record() > 0) {
      process_video_comments_row ();
    }
    break;
  default:
    fprintf (stderr, "unknown table type\n");
    exit(1);
  }

  flush_out();
  if (targ_fd != 1) {
    write_binlog_headers ();
    if (fsync(targ_fd) < 0) {
      fprintf (stderr, "error syncing %s: %m", targ_fname);
      exit (1);
    }
    close (targ_fd);
  }

  if (verbosity > 0) {
    output_stats();
  }

  return 0;
}
