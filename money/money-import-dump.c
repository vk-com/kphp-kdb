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

    Copyright 2010 Vkontakte Ltd
              2010 Nikolai Durov
              2010 Andrei Lopatin
*/
#define	_FILE_OFFSET_BITS	64

#include <assert.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <stddef.h>
#include <string.h>
#include <unistd.h>
#include <time.h>

#include "server-functions.h"
#include "kdb-money-binlog.h"
#include "kfs-layout.h"
#include "crc32.h"
#include "md5.h"

#define	TF_NONE		0
#define	TF_ACCTYPES	1
#define	TF_ACCOUNTS	2

#define	READ_BUFFER_SIZE	(1 << 24)
#define	WRITE_BUFFER_SIZE	(1 << 24)
#define	READ_THRESHOLD		(1 << 17)
#define	WRITE_THRESHOLD		(1 << 18)

#define	MAX_USERS	(1 << 23)


char default_progname [] = "money-import-dump";
char *progname = default_progname, *username, *src_fname, *targ_fname;
int verbosity = 0, output_format = 0, table_format = 0, split_mod = 0, split_rem = 0, allow_negative = 0;
int split_min = 0, split_max = 0;
int Args_per_line;
int src_fd, targ_fd;
long long rd_bytes, wr_bytes;
unsigned tot_crc32 = -1;

int stdout_mode = 0;

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
  if (!strncmp (str, "acctypes", 8)) {
    return TF_ACCTYPES;
  }
  if (!strncmp (str, "account_types", 13)) {
    return TF_ACCTYPES;
  }
  if (!strncmp (str, "accounts", 8)) {
    return TF_ACCOUNTS;
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
  assert (read (rfd, RB, 4) == 4);
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

void create_binlog_headers0 (int schema_id, char *schema_str) {
  assert (!binlog_headers && !KHDR);

  if (stdout_mode) {
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

  KHDR->table_id_hash = kfs_string_hash ("money");
  strcpy (KHDR->table_name, "money");
  if (split_mod > 1) {
    sprintf (KHDR->slice_name, "money%03d", split_min);
  } else {
    strcpy (KHDR->slice_name, "money");
  }

  KHDR->raw_size = 8192;
  KHDR->replica_created = KHDR->slice_created = KHDR->created = KHDR->modified;

  KHDR->total_copies = 1;

  strcpy (KHDR->creator, default_progname);
  strcpy (KHDR->slice_creator, default_progname);

  KHDR->schema_id = schema_id;
  if (schema_str) {
    KHDR->extra_bytes = strlen (schema_str);
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

void start_binlog (int schema_id, char *str) {
  int len = str ? strlen(str)+1 : 0;
  int extra = (len + 3) & -4;
  if (len == 1) { extra = len = 0; }

  if (binlog_existed) {
    if (ST) {
      assert (ST->schema_id == schema_id && ST->split_mod == split_mod && ST->split_min == split_min && ST->split_max == split_max);
    }
    if (KHDR) {
      assert (KHDR->schema_id == schema_id && KHDR->split_mod == split_mod && KHDR->split_min == split_min && KHDR->split_max == split_max);
    }
    return;
  }

  create_binlog_headers0 (schema_id, str);

  struct lev_start *E = write_alloc(sizeof(struct lev_start) - 4 + extra);
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
char *groups_fname, *groups_fname2;
char GT[MAX_GROUPS], *GA, *GB, *GC = GT, *GS = GT;
int Gc, Gd;

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

void log_0ints (int type, int arg) {
  struct lev_generic *L = write_alloc (8);
  L->type = type;
  L->a = arg;
}
  

int parse_account_type (char *ptr, char **endptr) {
  int i, res = 0;

  for (i = 0; i < 3; i++) {
    if (*ptr < 'A' || *ptr > 'Z') {
      break;
    }
    res = res * 27 + (*ptr - 'A') + 1;
    ++ptr;
  }

  *endptr = ptr;
  return res;
}

money_auth_code_t get_code (int x) {
  char *ptr;
  money_auth_code_t res = strtoull (S[x], &ptr, 16);
  assert (!L[x] || (L[x] == 16 && ptr == S[x] + 16));
  return res;
}

/* account_types */

enum {
  at_id,
  at_class,
  at_currency,
  at_creator_id,
  at_ip,
  at_comment,
  at_auth_code,
  at_admin_code,
  at_access_code,
  at_withdraw_code,
  at_block_code,
  at_create_code,
  at_END
};

void process_account_types_row (void) {
  char *ptr;
  int acc_type_id = parse_account_type (S[at_id], &ptr);

  assert (ptr == S[at_id] + L[at_id]);
  assert (acc_type_id > 0 && acc_type_id <= MAX_ACCOUNT_TYPE && (unsigned) I[at_class] <= 7);
  assert (L[at_comment] <= 4095);
  assert (I[at_currency] && I[at_currency] >= MIN_CURRENCY_ID && I[at_currency] <= MAX_CURRENCY_ID);

  struct lev_money_new_atype *E = write_alloc (offsetof (struct lev_money_new_atype, comment) + L[at_comment] + 1);
  E->type = LEV_MONEY_NEW_ATYPE;
  E->currency = I[at_currency];
  E->acc_type_id = acc_type_id;
  E->acc_class = I[at_class];
  E->creator_id = I[at_creator_id];
  E->auth_code = get_code (at_auth_code);
  E->admin_code = get_code (at_admin_code);
  E->access_code = get_code (at_access_code);
  E->withdraw_code = get_code (at_withdraw_code);
  E->block_code = get_code (at_block_code);
  E->create_code = get_code (at_create_code);
  E->ip = I[at_ip];
  E->comm_len = L[at_comment];
  memcpy (E->comment, S[at_comment], L[at_comment] + 1);

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
  fprintf (stderr, "usage:\t%s [-v] [-u<username>] [-m<rem>,<mod>] [-g<filename>] [-f<format>] [-o<output-class>] <input-file> [<output-file>]\n"
	   "\tConverts tab-separated table dump into KDB binlog. "
	   "If <output-file> is specified, resulting binlog is appended to it.\n"
	   "\t-h\tthis help screen\n"
	   "\t-v\tverbose mode on\n"
	   "\t-u<username>\tassume identity of given user\n"
	   "\t-g<filename>\tloads auxiliary data from given file\n"
	   "\t-m<rem>,<mod>\tslice parameters: consider only objects with id %% mod == rem\n"
	   "\t-o<int>\tdetermines output format\n"
	   "\t-f<format>\tdetermines dump format, one of account_types, accounts, ...\n",
	   progname);
}

int main (int argc, char *argv[]) {
  int i;
  progname = argv[0];
  while ((i = getopt (argc, argv, "hvu:m:f:g:o:")) != -1) {
    switch (i) {
    case 'v':
      verbosity++;
      break;
    case 'h':
      usage();
      return 2;
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
    if (targ_fd >= 0) {
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
  case TF_ACCTYPES:
    start_binlog(MONEY_SCHEMA_V1, "money");
    Args_per_line = at_END;
    while (read_record() > 0) {
      process_account_types_row();
    }
    break;
  case TF_ACCOUNTS:
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
