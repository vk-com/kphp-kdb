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
                   2011 Anton Maydell
              2012-2013 Oleg Davydov
*/

#define	_FILE_OFFSET_BITS	64

#include <assert.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>

//#include "net-memcache-server.h"
#include "server-functions.h"
#include "kdb-pmemcached-binlog.h"
#include "crc32.h"
#include "md5.h"
#include "base64.h"

// TF: table formats
#define	TF_NONE		0
#define	TF_SHORT_URLS	1
#define	TF_API_UVARS	2
#define	TF_STORAGE_IMG	3
#define TF_KAD          4

// SM: split modes
#define SM_NONE		0
#define SM_HASH		1
#define SM_FIRSTINT	2
#define SM_HASHDOT      3

#define	READ_BUFFER_SIZE	(1 << 24)
#define	WRITE_BUFFER_SIZE	(1 << 24)
#define	READ_THRESHOLD		(1 << 17)
#define	WRITE_THRESHOLD		(1 << 18)

char *progname = "pmemcache-import-dump", *username, *src_fname, *targ_fname;
int verbosity = 0, output_format = 0, table_format = 0, split_mode = 0, split_modulo = 0, split_rem = 0, allow_negative = 0;
int Args_per_line;
int src_fd, targ_fd, need_levstart;
int ignore_backslashes = 0, ignore_bad_lines = 0;
long long rd_bytes = 0, wr_bytes = 0;
long long total_urls_len = 0;
long long total_urls_hash_len = 0;
long long total_base62_number_len = 0;
long long nrecords = 0;
long long total_keys = 0;
char allowed[256];

int line_no;
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
      } else if (c == '\\' && !ignore_backslashes) {
        state = c;
      }
      *ptr++ = c;
    } while (1);
    *ptr = 0;
    L[i] = ptr - S[i];
    I[i] = atoll(S[i]);
  }
  if (i != Args_per_line) {
    fprintf (stderr, "%s: wrong number of items on line %d: %d instead of %d\n", src_fname, line_no, i, Args_per_line);
    if (ignore_bad_lines)
      return 2;
    exit(1);
  }
  line_no++;
  // skip all extra field
  while (c != '\n') {
    assert (rptr < rend);
    c = *rptr++;
  }
  return 1;
}

int get_split_mode (char *str) {
  if (!strncmp (str, "hash", 5)) {
    return SM_HASH;
  } else if (!strncmp (str, "firstint", 9)) {
    return SM_FIRSTINT;
  } else if (!strncmp (str, "hashdot", 8)) {
    return SM_HASHDOT;
  }
  return SM_NONE;
}

int get_dump_format (char *str) {
  if (!strncmp (str, "short_urls", 9)) {
    return TF_SHORT_URLS;
  } else if (!strncmp (str, "api_uvars", 9)) {
    return TF_API_UVARS;
  } else if (!strncmp (str, "storage_img", 11) || !strncmp (str, "keyvalue", 9)) {
    return TF_STORAGE_IMG;
  } else if (!strncmp (str, "kad", 4)) {
    return TF_KAD;
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

void start_binlog (int schema_id, char *str) {
  int len = str ? strlen(str)+1 : 0;
  int extra = (len + 3) & -4;
  if (len == 1) { extra = len = 0; }
  struct lev_start *E = write_alloc (sizeof(struct lev_start) - 4 + extra);
  E->type = LEV_START;
  E->schema_id = schema_id;
  E->extra_bytes = extra;
  E->split_mod = split_modulo;
  E->split_min = split_rem;
  E->split_max = split_rem + 1;
  if (len) {
    memcpy (E->str, str, len);
  }
}

/*
 * BEGIN MAIN
 */

/* copy-pasted from mc-proxy.c */
unsigned hash_key (const char *key, int key_len) {
  unsigned hash = (compute_crc32 (key, key_len) >> 8) & 0x7fffff;
  return hash ? hash : 1;
}

unsigned long long extract_num (const char *key, int key_len, char **eptr) {
  const char *ptr = key, *ptr_e = key + key_len, *num_st;
  unsigned long long x;
  while (ptr < ptr_e && (*ptr < '0' || *ptr > '9')) {
    ptr++;
  }
  if (ptr == ptr_e) {
    if (eptr) {
      *eptr = (char *)ptr;
    }
    return (unsigned long long) -1;
  }
  do {
    num_st = ptr;
    x = 0;
    while (ptr < ptr_e && *ptr >= '0' && *ptr <= '9') {
      if (x >= 0x7fffffffffffffffLL / 10) {
        if (eptr) {
          *eptr = (char *)ptr;
        }
        return (unsigned long long) -1;
      }
      x = x*10 + (*ptr - '0');
      ptr++;
    }
    if (ptr == num_st) {
      if (eptr) {
        *eptr = (char *)ptr;
      }
      return (unsigned long long) -1;
    }
  } while (num_st == key && ptr < ptr_e && *ptr++ == '~');
  if (eptr) {
    *eptr = (char *)ptr;
  }
  return x;
}
/* *************************** */

int conv_key (const char* const key) {
  unsigned long long id, i = -1;
  switch (split_mode) {
    case SM_HASH:
      id = hash_key(key, strlen(key));
      break;
    case SM_FIRSTINT:
      id = extract_num(key, strlen(key), NULL);
      break;
    case SM_HASHDOT:
      for (i = 0; key[i] && key[i] != '.'; i++)
        ;
      id = hash_key(key, i);
      break;
    default:
      assert("unknown split mode!" && 0);
  }
  return id % split_modulo == split_rem ? 0 : -1;
}

/* short_urls */
// WTF?
enum {
  su_id, su_url,
  su_END
};

/* storage_img */
enum {
  si_old_path, si_new_path,
  si_END
};

char base62number_buf[32];
unsigned char url_md5[16];
char url_base64_buf[32];
const char* const id_prefix  = "short_url";
const char* const id_prefix2 = "url_short";
int id_prefix_length, id_prefix2_length;
char number_with_prefix[64];
char *write_buffer = number_with_prefix; // shitness +3
char url_base64_with_prefix2[64];

void do_store (const char* const key, const char* const data) {
  if (conv_key (key) == -1) {
    return;    
  }
  if (verbosity >= 4) {
    fprintf (stderr, "do_store: (%s, %s)\n", key, data);
  }
  int key_len = strlen (key);
  int data_len = strlen (data);
  //int type = pmct_set;
  int type = 1;
  int s = offsetof (struct lev_pmemcached_store_forever, data) + 1 + key_len + data_len;
  struct lev_pmemcached_store_forever *E = (struct lev_pmemcached_store_forever*) write_alloc (s);
  E->type = LEV_PMEMCACHED_STORE_FOREVER + type;
  E->key_len = key_len;
  E->data_len = data_len;
  //E->flags = 0;
  //E->delay = -1;
  memcpy (E->data, key, key_len * sizeof (char));
  if (data_len >= 0) {
    memcpy (E->data+key_len, data, data_len * sizeof (char));
    E->data[key_len+data_len] = 0;
  }
  total_keys++;
  if (verbosity >= 4) {
    fprintf (stderr, "E->data = %s\n", E->data);
  }
}

void process_short_urls_row (void) {
  long long id = I[su_id];
  assert (id >= 0);
  unsigned char *url = (unsigned char*) S[su_url];
  const int url_len = L[su_url];

  if (number_to_base62 (id, base62number_buf, sizeof(base62number_buf))) {
    fprintf (stderr, "base62number_buf size too small");
    exit (-1);
  }
 
  assert (id_prefix_length + strlen(base62number_buf) + 1 <= sizeof(number_with_prefix));
  strcpy (number_with_prefix + id_prefix_length, base62number_buf);

  if (verbosity >= 4) {
    fprintf(stderr, "number_with_prefix: %s\n", number_with_prefix);
  }
  
  total_urls_len += url_len;
  total_base62_number_len += strlen(base62number_buf);

  md5 (url, url_len, url_md5);
  if (base64_encode (url_md5, 16, url_base64_buf, sizeof (url_base64_buf) )) {
    fprintf (stderr, "url_base64_buf size too small");
    exit (-1);
  }

  total_urls_hash_len += strlen(url_base64_buf);

  assert (id_prefix2_length + strlen(url_base64_buf) + 1 <= sizeof(url_base64_with_prefix2) );
  strcpy (url_base64_with_prefix2 + id_prefix2_length, url_base64_buf);

  do_store (number_with_prefix, S[su_url]);
  do_store (url_base64_with_prefix2, base62number_buf);
  nrecords++;
  /*
  if (conv_uid (list_id) < 0 || list_id <= 0 || app_id <= 0) {
    return;
  }
  */
}

void process_api_uvars_row (void) {
  long long id = I[0];
  char *data = (char*)S[1];
  long long api_id = id / 1000000000000ll; // 10^12
  long long user_id = id / 1000 % 1000000000;
  long long var_id = id % 1000;
  sprintf (write_buffer, "lu%lld.%lld.%lld", user_id, api_id, var_id);
  if (verbosity > 1) {
    fprintf (stderr, "store: %s\n", write_buffer);
  }
  do_store (write_buffer, data);
}

void process_storage_img_row (void) {
  do_store (S[si_old_path], S[si_new_path]);
  nrecords++;
}

/* kad */
enum {
  kad_uid, kad_file, kad_print, kad_genre, kad_bitrate, kad_album,
  kad_END
};
int source_server = -1;

void process_kad_row (void) {
  char key_prefix[100], key[200], value[50];
  snprintf (key_prefix, 99, "%d_%lld_%s", source_server, I[kad_uid], S[kad_file]);
  // store kad
  snprintf (key, 199, "%s.kad", key_prefix);
  do_store (key, S[kad_print]);
  // store genre
  snprintf (key, 199, "%s.genre", key_prefix);
  snprintf (value, 49, "%lld", I[kad_genre]);
  do_store (key, value);
  // store bitrate
  snprintf (key, 199, "%s.rate", key_prefix);
  snprintf (value, 49, "%lld", I[kad_bitrate]);
  do_store (key, value);
  // store album
  snprintf (key, 199, "%s.album", key_prefix);
  do_store (key, S[kad_album]);
}

/*
 * END MAIN
 */

void output_stats (void) {
  fprintf (stderr,
      "read: %lld bytes, %d records read\n"
      "written: %lld bytes\n"
      "stored keys: %lld\n",
      rd_bytes, line_no, wr_bytes,
      total_keys
      );
  if (total_urls_len && line_no) {
    fprintf (stderr, "average url length: %.3lf\n"
         "average url hash length: %.3lf\n"
         "average id in base62 length: %.3lf\n",
         ((double) (total_urls_len)) / ((double) (line_no)),
         ((double) (total_urls_hash_len)) / ((double) (line_no)),
         ((double) (total_base62_number_len)) / ((double) (line_no))
         );    
  }
}


void usage (void) {
  fprintf (stderr, "usage:\t%s [-v] [-n] [-u<username>] [-m<rem>,<mod>] [-s<mode>] [-f<format>] [-o<output-class>] <input-file> [<output-file>]\n"
      "\tConverts tab-separated table dump into KDB binlog. "
      "If <output-file> is specified, resulting binlog is appended to it.\n"
      "\t-h\tthis help screen\n"
      "\t-v\tverbose mode on\n"
      "\t-u<username>\tassume identity of given user\n"
      "\t-s<mode>\tsplit mode: hash, firstint, ...\n"
      "\t-m<rem>,<mod>\tslice parameters: consider only objects with id %% mod == rem\n"
      "\t-n\tindex objects with negative ids\n"
      "\t-o<int>\tdetermines output format\n"
      "\t-f<format>\tdetermines dump format, one of short_urls, api_uvars, storage_img, ...\n"
      "\t-k<server>\tfile source server (actual for kad)\n"
      "\t-l\t skip bad lines instead of assert\n",
      progname);
}

int main (int argc, char *argv[]) {
  int i, r;
  set_debug_handlers();
  progname = argv[0];
  while ((i = getopt (argc, argv, "hvnlu:s:m:f:o:k:")) != -1) {
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
      case 'l':
        ignore_bad_lines = 1;
        break;
      case 's':
        split_mode = get_split_mode(optarg);
	if (!split_mode) {
	  fprintf (stderr, "fatal: unsupported split mode: %s\n", optarg);
	  return 2;
	}
        break;
      case 'm':
        assert (sscanf(optarg, "%d,%d", &split_rem, &split_modulo) == 2);
        assert (split_modulo > 0 && split_modulo <= 1000 && split_rem >= 0 && split_rem < split_modulo);
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
      case 'u':
        username = optarg;
        break;
      case 'k':
        source_server = atol (optarg);
        break;
    }
  }

  if (optind >= argc || optind + 2 < argc) {
    usage();
    return 2;
  }

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

  if (!table_format) {
    table_format = get_dump_format (fname_last (src_fname));
    if (!table_format) {
      fprintf (stderr, "fatal: cannot determine table type from filename %s\n", src_fname);
    }
  }

  if (optind + 1 < argc) {
    int r;
    struct stat s;
    targ_fname = argv[optind+1];
    targ_fd = open (targ_fname, O_WRONLY | O_APPEND | O_CREAT, 0644);
    if (targ_fd < 0) {
      fprintf (stderr, "cannot create %s: %m\n", targ_fname);
      return 1;
    }
    r = fstat (targ_fd, &s);
    if (r != 0) {
      fprintf (stderr, "stat: %s: %m\n", targ_fname);
      return 1;
    }
    need_levstart = s.st_size == 0;
  } else {
    targ_fname = "stdout";
    targ_fd = 1;
    need_levstart = 1;
  }
  
  assert (split_modulo);
  switch (table_format) {
    case TF_SHORT_URLS:
      Args_per_line = su_END;
      start_binlog (PMEMCACHED_SCHEMA_V1, "");
      
      id_prefix_length = strlen (id_prefix);
      id_prefix2_length = strlen (id_prefix2);

      assert (id_prefix_length + 1 <= sizeof(number_with_prefix));
      assert (id_prefix2_length + 1 <= sizeof(url_base64_with_prefix2));

      strcpy (number_with_prefix, id_prefix);
      strcpy (url_base64_with_prefix2, id_prefix2);

      while (read_record() > 0) {
        process_short_urls_row ();
      }
      break;
    case TF_API_UVARS:
      Args_per_line = 2;
      start_binlog (PMEMCACHED_SCHEMA_V1, "");      
      while (read_record() > 0) {
        process_api_uvars_row ();
      }
      break;
    case TF_STORAGE_IMG:
      Args_per_line = si_END;
      start_binlog (PMEMCACHED_SCHEMA_V1, "");      
      while (read_record() > 0) {
        process_storage_img_row ();
      }
      break;
    case TF_KAD:
      Args_per_line = kad_END;
      ignore_backslashes = 1;
      if (need_levstart) {
        start_binlog (PMEMCACHED_SCHEMA_V1, "");
      }
      while ((r = read_record()) > 0) {
        if (r == 1) {
          process_kad_row ();
        }
      }
      break;
    default:
      fprintf (stderr, "unknown table type\n");
      exit(1);
  }

  flush_out ();
  if (targ_fd != 1) {
    if (fdatasync(targ_fd) < 0) {
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
