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
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <stddef.h>
#include <unistd.h>
#include <time.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <byteswap.h>

#include "crc32.h"
#include "server-functions.h"
#include "kdb-text-binlog.h"
#include "kdb-data-common.h"
#include "word-split.h"
#include "stemmer.h"
#include "listcomp.h"
#include "kfs.h"
#include "text-index-layout.h"

#define	VERSION_STR	"text-index-0.31"

#define	MAX_NONDICT_FREQ	8
#define	MAX_MESSAGE_SIZE	(1 << 19)

#ifdef _LP64
//#define	MAX_USER_MESSAGES	((3 << 24) + (1 << 20))
# define MAX_USER_MESSAGES	(1 << 27)
#else
# define MAX_USER_MESSAGES	(1 << 24)
#endif

extern int binlog_zipped;

int verbosity, search_enabled, history_enabled, use_stemmer, hashtags_enabled, searchtags_enabled;
int force_pm, ignore_delete_first_messages, extra_mask_changes;
int now;

char *fnames[3];
int fd[3];
long long fsize[3];

char *progname = "text-index", *username, *binlogname, *newidxname, *temp_binlog_directory;
char metaindex_fname_buff[256], binlog_fname_buff[256], temp_binlog_fname_buff[256];
int temp_binlog_dir_len;

#define	MAX_USERS	(1 << 21)
#define	NEGATIVE_USER_OFFSET	(1 << 20)

/* stats counters */
int start_time;
long long binlog_loaded_size;
double binlog_load_time, last_process_time;

unsigned log_limit_crc32;

int dict_size;
long long word_instances, nonword_instances;
int max_nd_freq = MAX_NONDICT_FREQ;
int last_global_id, last_global_id0;


/* file utils */

int open_file (int x, char *fname, int creat) {
  fnames[x] = fname;
  fd[x] = open (fname, creat > 0 ? O_RDWR | O_CREAT | O_EXCL : O_RDONLY, 0600);
  if (creat < 0 && fd[x] < 0) {
    if (fd[x] < 0) {
      fprintf (stderr, "%s: cannot open %s: %m\n", progname, fname);
    }
    return -1;
  }
  if (fd[x] < 0) {
    fprintf (stderr, "%s: cannot open %s: %m\n", progname, fname);
    exit(1);
  }
  fsize[x] = lseek (fd[x], 0, SEEK_END);
  if (fsize[x] < 0) {
    fprintf (stderr, "%s: cannot seek %s: %m\n", progname, fname);
    exit(2);
  }
  lseek (fd[x], 0, SEEK_SET);
  if (verbosity) {
    fprintf (stderr, "opened file %s, fd=%d, size=%lld\n", fname, fd[x], fsize[x]);
  }
  return fd[x];
}

/*
 *
 *	DATA STRUCTURES
 *
 */

typedef struct message message_t;
typedef struct user user_t;

struct message {
  message_t *prev;
  int flags;
  int user_id;
  int local_id;
  long long legacy_id;
  int global_id;
  int peer_id;
  int peer_msg_id;
  int date;
  unsigned ip;
  int port;
  unsigned front;
  unsigned long long ua_hash;
  int len;
  int extra[0];
  char text[1]; 
};

struct message_short {
  message_t *prev;
  int flags;
  int user_id;
  int local_id;
};

struct message_medium {
  message_t *prev;
  int flags;
  int user_id;
  int local_id;
  int extra[0];
};

struct user {
  message_t *last;
  int user_id;
  int first_local_id;
  int last_local_id;
};

struct sublist_descr {
  int xor_mask, and_mask;
};

struct sublist_descr Messages_Sublists[] = {
{ 0, TXF_OUTBOX | TXF_SPAM | TXF_DELETED },
{ TXF_OUTBOX, TXF_OUTBOX | TXF_SPAM | TXF_DELETED },
{ TXF_UNREAD, TXF_OUTBOX | TXF_UNREAD | TXF_SPAM | TXF_DELETED },
{ TXF_IMPORTANT, TXF_IMPORTANT | TXF_SPAM | TXF_DELETED },
{ TXF_FRIENDS, TXF_OUTBOX | TXF_FRIENDS | TXF_SPAM | TXF_DELETED },
{ TXF_OUTBOX | TXF_FRIENDS, TXF_OUTBOX | TXF_FRIENDS | TXF_SPAM | TXF_DELETED },
{ TXF_UNREAD | TXF_CHAT, TXF_OUTBOX | TXF_UNREAD | TXF_CHAT | TXF_SPAM | TXF_DELETED },
{ 0, TXF_OUTBOX | TXF_CHAT | TXF_SPAM | TXF_DELETED },
{ TXF_OUTBOX, TXF_OUTBOX | TXF_CHAT | TXF_SPAM | TXF_DELETED },
{ TXF_UNREAD, TXF_OUTBOX | TXF_UNREAD | TXF_CHAT | TXF_SPAM | TXF_DELETED },
{ TXF_SPAM, TXF_SPAM | TXF_DELETED },
{ TXF_UNREAD | TXF_SPAM, TXF_UNREAD | TXF_SPAM | TXF_DELETED },
{ 0, 0 }
};

struct sublist_descr Statuses_Sublists[] = {
{ 0, TXFS_WALL | TXFS_REPLY | TXFS_COPY | TXFS_SPAM | TXFS_DELETED },
{ 0, TXFS_WALL | TXFS_REPLY | TXFS_SPAM | TXFS_DELETED },
{ TXFS_COPY, TXFS_WALL | TXFS_REPLY | TXFS_COPY | TXFS_SPAM | TXFS_DELETED },
{ TXFS_COPIED, TXFS_WALL | TXFS_REPLY | TXFS_COPY | TXFS_COPIED | TXFS_SPAM | TXFS_DELETED },
{ 0, TXFS_REPLY | TXFS_SPAM | TXFS_DELETED },
{ TXFS_WALL, TXFS_WALL | TXFS_REPLY | TXFS_SPAM | TXFS_DELETED },
{ TXFS_WALL, TXFS_WALL | TXFS_SPAM | TXFS_DELETED },
{ 0, TXFS_SPAM | TXFS_DELETED },
{ TXFS_LOCATION, TXFS_WALL | TXFS_REPLY | TXFS_COPY | TXFS_LOCATION | TXFS_SPAM | TXFS_DELETED },
{ 0, TXFS_FRIENDSONLY | TXFS_WALL | TXFS_REPLY | TXFS_SPAM | TXFS_DELETED },
{ 0, TXFS_FRIENDSONLY | TXFS_REPLY | TXFS_SPAM | TXFS_DELETED },
{ 0, 0 }
};

struct sublist_descr Forum_Sublists[] = {
{ TXFP_TOPIC, TXFP_REPLY | TXFP_TOPIC | TXFP_FIXED | TXFP_CLOSED | TXFP_SPAM | TXFP_DELETED },
{ TXFP_TOPIC, TXFP_REPLY | TXFP_TOPIC | TXFP_FIXED | TXFP_SPAM | TXFP_DELETED },
{ TXFP_TOPIC | TXFP_FIXED, TXFP_REPLY | TXFP_TOPIC | TXFP_FIXED | TXFP_SPAM | TXFP_DELETED },
{ TXFP_TOPIC | TXFP_CLOSED, TXFP_REPLY | TXFP_TOPIC | TXFP_CLOSED | TXFP_SPAM | TXFP_DELETED },
{ 0, 0 }
};

struct sublist_descr Comments_Sublists[] = {
{ 0, TXF_SPAM | TXF_DELETED },
{ TXF_SPAM, TXF_SPAM | TXF_DELETED },
{ 0, 0 }
};


struct sublist_descr *Sublists = Messages_Sublists;
struct sublist_descr PeerFlagFilter = { 0, TXF_DELETED | TXF_SPAM },
	    Statuses_PeerFlagFilter = { 0, TXFS_DELETED | TXFS_SPAM | TXFS_COPY },
	       Forum_PeerFlagFilter = { 0, TXFP_TOPIC | TXFP_DELETED | TXFP_SPAM },
            Comments_PeerFlagFilter = { 0, TXF_DELETED | TXF_SPAM };

union packed_sublist_descr Sublists_packed[MAX_SUBLISTS+1];
int sublists_num = 0;

int preserve_legacy_ids = 0;
int msg_date_sort = 0;

long long min_legacy_id, max_legacy_id;

#define FITS_AN_INT(a) ( (a) >= -0x80000000LL && (a) <= 0x7fffffffLL )
#define LONG_LEGACY_IDS (min_legacy_id < -0x80000000LL || max_legacy_id > 0x7fffffffLL)

message_t *threshold_msg;


/*
 *
 *	WORD HASH SUPPORT, HUFFMAN CODER
 *
 */

#ifdef _LP64
# define PRIME	30000001
#else
# define PRIME	1000003
#endif
//#define TEXT_SIZE (1L << 14)

typedef struct word word_t;

struct word {
  word_t *next;
  long long freq;
  unsigned code;
  unsigned char code_len;
  unsigned char len;
  char str[1];
};

typedef struct word_hash {
  int cnt;
  int tot_len;
  // word_t *A[PRIME];
  word_t **A;
} word_hash_t;

struct char_dictionary {
  /* first three fields are needed to pack */
  char code_len[256];
  long long freq[256]; /* NB: in index they are int's */
  unsigned code[256];
  /* next three fields are needed to unpack */
  unsigned first_codes[32];
  int *code_ptr[32];
  int chars[256];
  int max_bits;
} WordCharDict, NotWordCharDict;

word_hash_t Words, NotWords;
word_t **WordList, **NotWordList;
word_t *WordFreqWords[256], *NotWordFreqWords[256];

static word_t *hash_get (word_hash_t *Set, char *str, int len, int force) {
  int h = 0, i;
  word_t *w;
  assert (len >= 0 && len < 128);
  for (i = 0; i < len; i++) {
    if (h > (1 << (8*sizeof(int)-2))/37) {
      h %= PRIME;
    }
    h = h * 37 + (unsigned char) str[i];
  }
  if (h >= PRIME) {
    h %= PRIME;
  }
  assert (h >= 0 && h < PRIME);
  w = Set->A[h];
  while (w) {
    if (w->len == len && !memcmp (w->str, str, len)) {
      return w;
    }
    w = w->next;
  }
  if (!force) {
    return w;
  }
  w = zmalloc (sizeof (word_t) + len);
  assert (w);
  w->next = Set->A[h];
  Set->A[h] = w;
  Set->cnt++;
  Set->tot_len += len;
  w->freq = 0;
  w->code = 0;
  w->code_len = 0;
  w->len = len;
  memcpy (w->str, str, len);
  w->str[len] = 0;
  return w;
}

int count_rare_words (const word_hash_t *Set, int max_freq) {
  int cnt = 0, i;
  const word_t *ptr;
  for (i = 0; i < PRIME; i++) {
    for (ptr = Set->A[i]; ptr; ptr = ptr->next) {
      if (ptr->freq <= max_freq) {
	cnt++;
      }
    }
  }
  return cnt;
}

long import_freq (long long *table, const word_hash_t *Set) {
  long cnt = 0;
  int i;
  const word_t *ptr;
  for (i = 0; i < PRIME; i++) {
    for (ptr = Set->A[i]; ptr; ptr = ptr->next) {
      if (ptr->freq > 0) {
	*table++ = ptr->freq;
	cnt++;
      }
    }
  }
  return cnt;
}


void preprocess_text (char *text, int text_len) {
  char *ptr;
  int len, w_c = 0;
  word_t *w;
  //fprintf (stderr, "TEXT: %s\n", text);

  ptr = text;
  while (*ptr) {
    len = get_word(ptr);
    if (len < 0 || len >= 128) {
      fprintf (stderr, "WORD: '%.*s'\nTEXT: '%.*s'\n", len, ptr, text_len, text);
    }
    assert (len >= 0);
    w = hash_get (&Words, ptr, len, 1);
    w->freq++;
    //    fprintf (stderr, "%p: word='%s', freq=%d\n", w, w->str, w->freq);
    ptr += len;
    w_c++;

    if (!*ptr) {
      break;
    }

    len = get_notword(ptr);
    if (len < 0) {
      break;
    }
    /*if (len < 0 || len >= 4096) 
      fprintf (stderr, "NOT WORD: %d '%.*s'\n", len, len, ptr);*/
    //    fprintf (stderr, "NOT-WORD: '%.*s'\n", len, ptr);
    w = hash_get (&NotWords, ptr, len, 1);
    w->freq++;
    //    fprintf (stderr, "%p: word='%s', freq=%d\n", w, w->str, w->freq);*/
    ptr += len;
    w_c++;
  }

  word_instances += (w_c >> 1);
  nonword_instances += ((w_c + 1) >> 1);
}

int pack_text (char *dest, int max_packed_len, char *text, int text_len) {
  char *ptr, *wptr = dest, *wptr_e = dest + max_packed_len;
  int len, x, y = 1;
  word_t *w;
  //fprintf (stderr, "TEXT: %s\n", text);
#define EncodeBit(__b)  { if (y >= 0x100) { *wptr++ = (char) y; y = 1; } y <<= 1; y += __b; }
#define Encode(__c,__l) { int t=__l; int c=__c; while (t>0) { EncodeBit(c < 0); c<<=1; t--; } }

  ptr = text;
  while (*ptr) {
    assert (wptr <= wptr_e);
    len = get_word (ptr);
    assert (len >= 0);
    //    fprintf (stderr, "WORD: '%.*s'\n", len, ptr);
    w = hash_get (&Words, ptr, len, 0);

    if (w && w->freq > 0) {
      Encode (w->code, w->code_len);
      ptr += len;
    } else {
      w = WordFreqWords[len];
      Encode (w->code, w->code_len);
      while (len > 0) {
        x = (unsigned char) *ptr++;
        Encode (WordCharDict.code[x], WordCharDict.code_len[x]);
        len--;
      }
    }

    if (!*ptr) {
      break;
    }

    len = get_notword(ptr);
    if (len < 0) {
      break;
    }
    //    fprintf (stderr, "NOT-WORD: '%.*s'\n", len, ptr);
    w = hash_get (&NotWords, ptr, len, 0);

    if (w && w->freq > 0) {
      Encode (w->code, w->code_len);
      ptr += len;
    } else {
      w = NotWordFreqWords[len];
      Encode (w->code, w->code_len);
      while (len > 0) {
        x = (unsigned char) *ptr++;
        Encode (NotWordCharDict.code[x], NotWordCharDict.code_len[x]);
        len--;
      }
    }
  }

  EncodeBit (1);
  while (y < 0x100) {
    EncodeBit (0);
  }
  *wptr++ = (char) y;
  assert (wptr <= wptr_e);
  return wptr - dest;

#undef EncodeBit
#undef Encode
}

/*static void isort (int *A, int b) {
  int i = 0, j = b;
  int h, t;
  if (b <= 0) { return; }
  h = A[b >> 1];
  do {
    while (A[i] < h) { i++; }
    while (A[j] > h) { j--; }
    if (i <= j) {
      t = A[i];  A[i++] = A[j];  A[j--] = t;
    }
  } while (i <= j);
  isort (A+i, b-i);
  isort (A, j);
}*/

static void ull_sort (unsigned long long *A, int b) {
  int i = 0, j = b;
  unsigned long long h, t;
  if (b <= 0) { return; }
  h = A[b >> 1];
  do {
    while (A[i] < h) { i++; }
    while (A[j] > h) { j--; }
    if (i <= j) {
      t = A[i];  A[i++] = A[j];  A[j--] = t;
    }
  } while (i <= j);
  ull_sort (A+i, b-i);
  ull_sort (A, j);
}


static unsigned long long WordCRC[MAX_TEXT_LEN / 2];

int compute_message_distinct_words (char *text, int text_len) {
  char *ptr = text;
  int count = 0, len;
  static char buff[1024];

  assert ((unsigned) text_len < MAX_TEXT_LEN);

  while (*ptr == 1 || *ptr == 2) {
    char *tptr = ptr;
    while (*tptr && *tptr != 9) {
      ++tptr;
    }

    if (*ptr == 2) {
      char tptr_peek = *tptr;
      
      if (*tptr == 9) {
        *tptr = 0;
      }

      while (*ptr && *ptr != 32) {
        ++ptr;
      }

      while (*ptr) {
        len = get_notword (ptr);
        if (len < 0) {
          break;
        }
        if (ptr + len > tptr && ptr <= tptr) {
          fprintf (stderr, "get_notword went out of bounds: ptr=%p len=%d\n", ptr, len);
        }
        ptr += len;

        len = get_word (ptr);
        assert (len >= 0);
        if (ptr + len > tptr && ptr <= tptr) {
          fprintf (stderr, "get_word went out of bounds: ptr=%p len=%d\n", ptr, len);
        }

        if (len > 0) {
          assert (count < MAX_TEXT_LEN / 2);
          WordCRC[count++] = crc64 (buff, my_lc_str (buff, ptr, len)) & -64;
        }
        ptr += len;
      }

      *tptr = tptr_peek;

      if (ptr != tptr && (ptr > tptr || verbosity > 0)) {
        fprintf (stderr, "warning: error while parsing kludge in '%.*s': text=%p ptr=%p tptr=%p\n", text_len, text, text, ptr, tptr);
      }
      assert (ptr <= tptr);

    }

    ptr = tptr;

    if (*ptr) {
      ++ptr;
    }
  }

  while (*ptr) {
    len = get_word(ptr);
    assert (len >= 0);
 
    if (len > 0) {
      assert (count < MAX_TEXT_LEN / 2);
      WordCRC[count++] = crc64 (buff, my_lc_str (buff, ptr, len)) & -64;
    }
    ptr += len;

    len = get_notword (ptr);
    if (len < 0) {
      break;
    }
    ptr += len;
  }

  ull_sort (WordCRC, count - 1);

  if (count > 0) {
    int newcount = 1, i;
    for (i = 1; i < count; i++) {
      if (WordCRC[i] != WordCRC[i - 1]) {
        WordCRC[newcount++] = WordCRC[i];
      }
    }
    return newcount;
  }

  return 0;
}

/*
 *	CREATE/WRITE TEMPORARY FILES
 */

char prec_mask_intcount[MAX_EXTRA_MASK+2];

static inline int extra_mask_intcount (int mask) {
  return prec_mask_intcount[mask & MAX_EXTRA_MASK];
}

static int conv_uid (int user_id);

#define	MAX_PASSES	129
#define	TEMP_WRITE_BUFFER_SIZE	(1L << 23)

struct buff_file {
  char *wst, *wptr, *wend;
  char *filename;
  int fd;
  unsigned crc32;
  long long wpos;
  int timestamp;
  int after_crc32;
  int global_id;
};

int passes;
int pass_min_uid[MAX_PASSES+1];
int is_message_event;
struct buff_file temp_file[MAX_PASSES];

long long tmp_bytes_written;

void tmp_write_lev (struct buff_file *T, const void *L, int size);

int close_temp_files (int op) {
  int i, c = 0;
  if (!temp_binlog_directory) {
    return -1;
  }
  for (i = 0; i < passes; i++) {
    if ((op & 1) && temp_file[i].fd > 0) {
      assert (close (temp_file[i].fd) >= 0);
      temp_file[i].fd = 0;
      c++;
    }
    if ((op & 2) && temp_file[i].filename) {
      assert (unlink (temp_file[i].filename) >= 0);
      free (temp_file[i].filename);
      temp_file[i].filename = 0;
      c++;
    }
  }
  return c;
}

int open_temp_files (void) {
  static struct lev_start lstart = {.type = LEV_START, .schema_id = 0x504d4554};
  int l, i;
  assert (temp_binlog_directory && passes);
  l = strlen (temp_binlog_directory);
  assert (l <= 120);
  memcpy (temp_binlog_fname_buff, temp_binlog_directory, l);
  if (!l) {
    temp_binlog_fname_buff[0] = '.';
    l = 1;
  }
  if (temp_binlog_fname_buff[l-1] != '/') {
    temp_binlog_fname_buff[l++] = '/';
  }
  temp_binlog_fname_buff[l] = 0;
  temp_binlog_dir_len = l;
  if (access (temp_binlog_fname_buff, W_OK | X_OK) < 0) {
    mkdir (temp_binlog_fname_buff, 0750);
    if (access (temp_binlog_fname_buff, W_OK | X_OK) < 0) {
      fprintf (stderr, "cannot create files in directory %s: %m\n", temp_binlog_fname_buff);
      exit (3);
    }
  }
  for (i = 0; i < passes; i++) {
    struct buff_file *T = temp_file + i;
    memset (T, 0, sizeof (*T));
    sprintf (temp_binlog_fname_buff + l, "temp%d_%d_%d.bin", log_split_min, log_split_mod, i);
    T->filename = strdup (temp_binlog_fname_buff);
    int fd = open (T->filename, O_RDWR);
    if (fd < 0) {
      fd = open (T->filename, O_RDWR | O_CREAT | O_EXCL, 0600);
      if (fd < 0) {
	fprintf (stderr, "cannot create temporary file %s: %m\n", T->filename);
	exit (3);
      }
    }
    assert (lock_whole_file (fd, F_WRLCK) > 0);
    if (ftruncate (fd, 0) < 0) {
      fprintf (stderr, "cannot truncate temporary file %s: %m\n", T->filename);
      exit (3);
    }
    assert (fd > 2);
    T->fd = fd;
    T->crc32 = -1;
    T->wptr = T->wst = zmalloc (TEMP_WRITE_BUFFER_SIZE);
    T->wend = T->wst + TEMP_WRITE_BUFFER_SIZE;
    tmp_write_lev (T, &lstart, 24);
  }
  return passes;
}

void tmp_flush_out (struct buff_file *T) {
  int b = T->wptr - T->wst;
  if (!b) {
    T->wptr = T->wst;
    return;
  }
  assert (b > 0 && b <= TEMP_WRITE_BUFFER_SIZE);
  int a = write (T->fd, T->wst, b);
  if (a < b) {
    if (a >= 0) {
      int c = write (T->fd, T->wst + a, b - a);
      a = (c >= 0 ? a + c : c);
    }
  }
  if (a != b) {
    if (a < 0) {
      fprintf (stderr, "cannot write %d bytes to temporary file %s: %m\n", b, T->filename);
    } else {
      fprintf (stderr, "cannot write %d bytes to temporary file %s: only %d bytes written\n", b, T->filename, a);
    }
    close_temp_files (3);
    exit (3);
  }
  T->wpos += a;
  tmp_bytes_written += a;
  T->wptr = T->wst;
  if (verbosity > 0) {
    fprintf (stderr, "%d bytes written to temporary file %s\n", a, T->filename);
  }
}

void tmp_write_lev (struct buff_file *T, const void *L, int size) {
  size = (size + 3) & -4;
  assert (!(size & (-0x100000)));
  if (T->wptr + size > T->wend) {
    tmp_flush_out (T);
    assert (T->wptr + size <= T->wend);
  }
  memcpy (T->wptr, L, size);
  T->wptr += size;
  T->after_crc32 += size;
  T->crc32 = crc32_partial (L, size, T->crc32);
}

void tmp_write_ts (struct buff_file *T, int timestamp) {
  static struct lev_timestamp LogTs = {.type = LEV_TIMESTAMP};
  LogTs.timestamp = T->timestamp = timestamp;
  tmp_write_lev (T, &LogTs, sizeof (LogTs));
}

void tmp_write_crc32 (struct buff_file *T, int timestamp) {
  static struct lev_crc32 LogCrc = {.type = LEV_CRC32};
  LogCrc.timestamp = T->timestamp = timestamp;
  LogCrc.pos = T->wpos + (T->wptr - T->wst);
  LogCrc.crc32 = ~T->crc32;
  tmp_write_lev (T, &LogCrc, sizeof (LogCrc));
  T->after_crc32 = 0;
}

void tmp_adjust_global_id (struct buff_file *T, int is_message_event) {
  int v = last_global_id - T->global_id - is_message_event;
  assert (v >= 0);
  static struct lev_generic lgid;
  if (v > 0 && v <= 0xffff) {
    lgid.type = v + LEV_TX_INCREASE_GLOBAL_ID_SMALL;
    tmp_write_lev (T, &lgid, 4);
  } else if (v) {
    lgid.type = LEV_TX_INCREASE_GLOBAL_ID_LARGE;
    lgid.a = v;
    tmp_write_lev (T, &lgid, 8);
  }
  T->global_id = last_global_id;
}

void tmp_write_logevent (struct buff_file *T, const void *L, int size) {
  if (T->after_crc32 > 16384) {
    tmp_write_crc32 (T, now);
  } else if (now != T->timestamp && now) {
    tmp_write_ts (T, now);
  }
  if (is_message_event) {
    tmp_adjust_global_id (T, 1);
  }
  tmp_write_lev (T, L, size);
}

int tmp_dispatch_logevent (const void *L, int size) {
  assert (size >= 8);
  int user_id = ((int *) L)[1];
  int uid = conv_uid (user_id);
  if (uid < 0) {
    return 0;
  }
  int a = -1, b = passes, c;
  while (b - a > 1) {
    c = ((a + b) >> 1);
    if (pass_min_uid[c] <= uid) {
      a = c;
    } else {
      b = c;
    }
  }
  assert (a >= 0 && uid < pass_min_uid[b]);
  tmp_write_logevent (&temp_file[a], L, size);
  return 1;
}

int tmp_dispatch_logevent_all (const void *L, int size) {
  int i;
  for (i = 0; i < passes; i++) {
    tmp_write_logevent (&temp_file[i], L, size);
  }
  return passes;
}

int tmp_flush_all (void) {
  int i;
  for (i = 0; i < passes; i++) {
    struct buff_file *T = &temp_file[i];
    tmp_adjust_global_id (T, 0);
    if (T->after_crc32) {
      tmp_write_crc32 (T, T->timestamp);
    }
    tmp_flush_out (T);
    assert (lseek (T->fd, 0, SEEK_SET) == 0);
  }
  return passes;
}

static int get_logrec_size (int type, void *ptr, int size) {
  struct lev_generic *E = ptr;
  struct lev_add_message *EM;
  int s, t;

  int text_len = -1;

  switch (type) {
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


int text_split_replay_logevent (struct lev_generic *E, int size) {
  int s;

  is_message_event = 0;
  switch (E->type) {
  case LEV_START:
    s = 24 + ((E->b + 3) & -4);
    if (size < s) { return -1; }
    return s;
  case LEV_NOOP:
  case LEV_TIMESTAMP:
  case LEV_CRC32:
  case LEV_ROTATE_FROM:
  case LEV_ROTATE_TO:
  case LEV_TAG:
    return default_replay_logevent (E, size);
  case LEV_CHANGE_FIELDMASK_DELAYED:
    if (size < 8) { return -2; }
    tmp_dispatch_logevent_all (E, 8);
    return 8;
  case LEV_TX_ADD_MESSAGE ... LEV_TX_ADD_MESSAGE + 0xff:
  case LEV_TX_ADD_MESSAGE_MF ... LEV_TX_ADD_MESSAGE_MF + 0xfff:
  case LEV_TX_ADD_MESSAGE_EXT ... LEV_TX_ADD_MESSAGE_EXT + MAX_EXTRA_MASK:
  case LEV_TX_ADD_MESSAGE_EXT_ZF ... LEV_TX_ADD_MESSAGE_EXT_ZF + MAX_EXTRA_MASK:
  case LEV_TX_ADD_MESSAGE_EXT_LL ... LEV_TX_ADD_MESSAGE_EXT_LL + MAX_EXTRA_MASK:
  case LEV_TX_ADD_MESSAGE_LF:
    s = get_logrec_size (E->type, E, size);
    if (s > size || s == -2) {
      return -2;
    }
    if (s < 0) {
      fprintf (stderr, "error %d reading binlog at position %lld, type %08x\n", s, log_cur_pos(), E->type);
      return s;
    }
#define	EM	((struct lev_add_message *) E)
    if (conv_uid (EM->user_id) < 0) {
      return s;
    }
#undef EM
    is_message_event = 1;
    ++last_global_id;

    tmp_dispatch_logevent (E, s);
    return s;
  default:
    s = get_logrec_size (E->type, E, size);

    if (s > size || s == -2) {
      return -2;
    }

    if (s < 0) {
      fprintf (stderr, "error %d reading binlog at position %lld, type %08x\n", s, log_cur_pos(), E->type);
      return s;
    }
    tmp_dispatch_logevent (E, s);
    return s;
  }
}

/*
 * read binlog
 */

void (*process_message)(struct lev_add_message *E, int extra_bytes);
void (*adjust_message)(int user_id, int local_id, int flags, int op, int *extra);
void (*delete_first_messages)(int user_id, int first_local_id);
void (*replace_message_text)(struct lev_replace_text_long *E);
void (*change_extra_mask)(int new_mask);

int log_split_min, log_split_max, log_split_mod;
int adj_rec, discarded_rec;
long long msgs_read, msgs_bytes;
long long factual_unpacked_bytes, factual_packed_bytes, factual_messages, replaced_messages;
long long tot_search_words, max_user_search_words, max_user_search_id;

int max_uid, cur_min_uid, cur_max_uid, tot_users;
int UserMsgCnt[MAX_USERS], UserMsgBytes[MAX_USERS], UserMsgDel[MAX_USERS], 
    UserMsgExtras[MAX_USERS], UserSearchWords[MAX_USERS];
user_t *User[MAX_USERS];

struct file_user_list_entry **UserDirectory;
char *UserDirectoryData;
int user_dir_size;



off_t extra_field_start_offset[MAX_EXTRA_FIELDS];
int init_extra_mask, current_extra_mask, final_extra_mask, extra_ints_num, text_shift;
int SE[MAX_EXTRA_INTS], ES[MAX_EXTRA_INTS];

static int text_le_start (struct lev_start *E) {
  int q = 0;
  if (E->schema_id != TEXT_SCHEMA_V1) {
    return -1;
  }
  log_split_min = E->split_min;
  log_split_max = E->split_max;
  log_split_mod = E->split_mod;
  assert (log_split_mod > 0 && log_split_min >= 0 && log_split_min + 1 ==  log_split_max && log_split_max <= log_split_mod);

  current_extra_mask = 0;
  extra_mask_changes = 0;
  if (E->extra_bytes >= 3 && E->str[0] == 1) {
    change_extra_mask (init_extra_mask = *(unsigned short *) (E->str + 1));
    q = 3;
  }
  if (E->extra_bytes >= q + 6 && !memcmp (E->str + q, "status", 6)) {
    memcpy (&PeerFlagFilter, &Statuses_PeerFlagFilter, sizeof (PeerFlagFilter));
    Sublists = Statuses_Sublists;
  }
  if (E->extra_bytes >= q + 5 && !memcmp (E->str + q, "forum", 5)) {
    memcpy (&PeerFlagFilter, &Forum_PeerFlagFilter, sizeof (PeerFlagFilter));
    Sublists = Forum_Sublists;
  }
  if (E->extra_bytes >= q + 8 && !memcmp (E->str + q, "comments", 8)) {
    memcpy (&PeerFlagFilter, &Comments_PeerFlagFilter, sizeof (PeerFlagFilter));
    Sublists = Comments_Sublists;
  }
  return 0;
}

static int unpack_extra_mask (int extra_mask) {
  int res = extra_mask & 0xff;
  res += 3*(extra_mask & 0x100);
  res += 6*(extra_mask & 0x200);
  res += 12*(extra_mask & 0x400);
  res += 24*(extra_mask & 0x800);
  return res;
}

/*static int pack_extra_mask (int extra_mask) {
  int res = extra_mask & 0x1ff;
  extra_mask >>= 1;
  res += extra_mask & 0x200;
  extra_mask >>= 1;
  res += extra_mask & 0x400;
  extra_mask >>= 1;
  res += extra_mask & 0x800;
  return res;
}*/


static inline int convert_unpacked_extra_field_num (int fnum) {
  fnum -= 8;
  if (fnum >= 0) {
    fnum >>= 1;
  }
  return fnum + 8;
}

#define	BB0(x)	x,
#define BB1(x)	BB0(x) BB0(x+1) BB0(x+1) BB0(x+2)
#define BB2(x)	BB1(x) BB1(x+1) BB1(x+1) BB1(x+2)
#define BB3(x)	BB2(x) BB2(x+1) BB2(x+1) BB2(x+2)
#define BB4(x)	BB3(x) BB3(x+1) BB3(x+1) BB3(x+2)
#define BB5(x)	BB4(x) BB4(x+2) BB4(x+2) BB4(x+4)
#define BB6(x)	BB5(x) BB5(x+2) BB5(x+2) BB5(x+4)

char prec_mask_intcount[MAX_EXTRA_MASK+2] = { BB6(0) 0 };

static inline int byte_bitcount (int x) {
  return prec_mask_intcount [x];
}

static int conv_uid (int user_id) {
  int t = user_id % log_split_mod;
  if (!user_id || (t != log_split_min && t != -log_split_min)) {
    return -1;
  }
  user_id /= log_split_mod;
  if (user_id < 0 || t < 0) {
    user_id--;
  }
  user_id += NEGATIVE_USER_OFFSET;
  return (unsigned) user_id < MAX_USERS ? user_id : -1;
}

static int unconv_uid (int uid) {
  uid -= NEGATIVE_USER_OFFSET;
  uid *= log_split_mod;
  if (uid < 0) {
    uid += log_split_mod - log_split_min;
  } else {
    uid += log_split_min;
  }
  return uid;
}

void adjust_message0 (int user_id, int local_id, int flags, int op, int *extra) {
  int uid = conv_uid (user_id);
  if (uid < 0 || local_id <= 0) {
    discarded_rec++;
    return;
  }
  UserMsgDel[uid]++;
  if (extra) {
    assert (!(flags & ~current_extra_mask) && flags);
    UserMsgExtras[uid] += extra_mask_intcount (flags & current_extra_mask);
  }
}

void process_message0 (struct lev_add_message *E, int extra_bytes) {
  int uid = conv_uid (E->user_id);
  int bytes = E->text_len;
  if (uid < 0) {
    discarded_rec++;
    return;
  }
  last_global_id++;
  if (!User[uid]) {
    User[uid] = zmalloc0 (sizeof (user_t));
    User[uid]->user_id = E->user_id;
    tot_users++;
  }
  long long legacy_id = E->legacy_id;
  if ((E->type & ~MAX_EXTRA_MASK) == LEV_TX_ADD_MESSAGE_EXT_LL) {
    legacy_id = (legacy_id & 0xffffffffLL) | (E->ua_hash & 0xffffffff00000000LL);
  }
  if (legacy_id < min_legacy_id) {
    min_legacy_id = legacy_id;
  }
  if (legacy_id > max_legacy_id) {
    max_legacy_id = legacy_id;
  }
  if (extra_bytes) {
    assert (!(E->type & MAX_EXTRA_MASK & ~current_extra_mask));
  } else {
    assert ((E->type & ~MAX_EXTRA_MASK) != LEV_TX_ADD_MESSAGE_EXT && (E->type & ~MAX_EXTRA_MASK) != LEV_TX_ADD_MESSAGE_EXT_ZF); // LL is allowed
  }
  preprocess_text (E->text + extra_bytes, bytes);

  if (search_enabled) {
    int d_words = compute_message_distinct_words (E->text + extra_bytes, bytes);
    tot_search_words += d_words;
    UserSearchWords[uid] += d_words;
    if (UserSearchWords[uid] > max_user_search_words) {
      max_user_search_words = UserSearchWords[uid];
      max_user_search_id = E->user_id;
    }
  }

  msgs_read++;
  msgs_bytes += bytes;

  if (uid > max_uid) {
    max_uid = uid;
  }
  UserMsgCnt[uid]++;
  UserMsgBytes[uid] += bytes;
}

void replace_message_text0 (struct lev_replace_text_long *E) {
  int text_len;
  char *text;

  if (E->type == LEV_TX_REPLACE_TEXT_LONG) {
    text = E->text;
    text_len = E->text_len;
  } else {
    assert ((E->type & -0x1000) == LEV_TX_REPLACE_TEXT);
    text = ((struct lev_replace_text *) E)->text;
    text_len = E->type & 0xfff;
  }

  int uid = conv_uid (E->user_id);

  if (uid < 0) {
    discarded_rec++;
    return;
  }

  assert (User[uid]);

  preprocess_text (text, text_len);

  if (search_enabled) {
    int d_words = compute_message_distinct_words (text, text_len);
    tot_search_words += d_words;
    UserSearchWords[uid] += d_words;
    if (UserSearchWords[uid] > max_user_search_words) {
      max_user_search_words = UserSearchWords[uid];
      max_user_search_id = E->user_id;
    }
  }

  msgs_read++;
  msgs_bytes += text_len;

  assert (uid <= max_uid);

  UserMsgCnt[uid]++;
  UserMsgBytes[uid] += text_len;
}

void delete_first_messages0 (int user_id, int first_local_id) {
  int uid = conv_uid (user_id);
  if (uid < 0 || ignore_delete_first_messages) {
    discarded_rec++;
    return;
  }
  assert (first_local_id > 0 && first_local_id <= UserMsgCnt[uid] + 1);
  if (!User[uid]) {
    User[uid] = zmalloc0 (sizeof (user_t));
    User[uid]->user_id = user_id;
    tot_users++;
  }
  if (User[uid]->first_local_id < first_local_id) {
    User[uid]->first_local_id = first_local_id;
  }
}

void change_extra_mask0 (int new_mask) {
  int i;
  ++extra_mask_changes;
  for (i = 0; i < MAX_EXTRA_FIELDS; i++) {
    if ((new_mask & (1 << i)) & !(current_extra_mask & (1 << i))) {
      extra_field_start_offset[i] = extra_mask_changes;
    }
  }
  current_extra_mask = new_mask;
}

void adjust_message1 (int user_id, int local_id, int flags, int op, int *extra) {
  int uid = conv_uid (user_id), i;
  user_t *U;

  if (uid < 0 || local_id <= 0) {
    return;
  }
  if (extra && !(flags & current_extra_mask & MAX_EXTRA_MASK)) {
    return;
  }

  U = User[uid];
  if (!U) {
    return;
  }

  if (uid < cur_min_uid || uid >= cur_max_uid) {
    return;
  }

  if (local_id < U->first_local_id) {
    return;
  }

  assert (dyn_top >= dyn_cur + sizeof (struct message_short));
  message_t *M = (message_t *) dyn_cur;
  memset (M, 0, sizeof (struct message_short));

  if (threshold_msg == (void *) -1L && now) {
    threshold_msg = M;
  }

  assert (!msg_date_sort || now);

  M->prev = U->last;
  U->last = M;

  M->user_id = user_id;
  M->local_id = local_id;

  assert (op >= 0 && op <= 3);
  assert (!extra || !(flags & ~MAX_EXTRA_MASK));

  M->flags = 0x80000000 | (op << 29);

  dyn_cur += offsetof (struct message_medium, extra);

  if (!extra) {
    M->flags |= flags & 0xffff;
    return;  
  }

  for (i = 0; i < MAX_EXTRA_FIELDS; i++) {
    if (flags & (1 << i)) {
      if (current_extra_mask & (1 << i)) {
        M->flags |= (1 << (16 + i));
        if (i < 8) {
          *((int *) dyn_cur) = *extra++;
          dyn_cur += 4;
        } else {
          *((long long *) dyn_cur) = *(long long *) extra;
          extra += 2;
          dyn_cur += 8;
        }
      } else {
        extra += (i < 8) ? 1 : 2;
      }
    }
  }

  assert (M->flags & TXF_HAS_EXTRAS);
}

void process_message1 (struct lev_add_message *E, int extra_bytes) {
  int uid = conv_uid (E->user_id);
  int bytes = E->text_len;
  int i, packed_bytes, *extra;
  user_t *U;
  message_t *M;

  if (uid < 0) {
    return;
  }
  U = User[uid];
  assert (U);

  last_global_id++;

  if (uid < cur_min_uid || uid >= cur_max_uid) {
    return;
  }

  if (++U->last_local_id < U->first_local_id) {
    return;
  }

  assert (dyn_top >= dyn_cur + sizeof (message_t) + MAX_MESSAGE_SIZE + 16 + 4 * MAX_EXTRA_INTS);
  M = (message_t *) dyn_cur;
  memset (M, 0, sizeof (message_t) + text_shift);

  if (threshold_msg == (void *) -1L && now) {
    threshold_msg = M;
  }

  M->prev = U->last;
  U->last = M;

  M->user_id = E->user_id;
  M->peer_id = E->peer_id;
  M->legacy_id = E->legacy_id;
  M->local_id = U->last_local_id;
  M->global_id = last_global_id;
  M->peer_msg_id = E->peer_msg_id;
  M->date = E->date ? E->date : now;
  M->ip = E->ip;
  M->port = E->port;
  M->front = E->front;
  M->ua_hash = E->ua_hash;

  if ((E->type & -0x1000) == LEV_TX_ADD_MESSAGE_MF) {
    M->flags = E->type & 0xfff;
  } else if ((E->type & -0x100) == LEV_TX_ADD_MESSAGE) {
    M->flags = E->type & 0xff;
  } else if (E->type == LEV_TX_ADD_MESSAGE_LF || (E->type & ~MAX_EXTRA_MASK) == LEV_TX_ADD_MESSAGE_EXT) {
    M->flags = E->date & 0xffff;
    M->date = now;
  } else if ((E->type & ~MAX_EXTRA_MASK) == LEV_TX_ADD_MESSAGE_EXT_LL) {
    M->flags = E->date & 0xffff;
    M->date = now;
    M->legacy_id = (M->legacy_id & 0xffffffffLL) | (M->ua_hash & 0xffffffff00000000LL);
    M->ua_hash &= 0xffffffffLL;
  } else if ((E->type & ~MAX_EXTRA_MASK) == LEV_TX_ADD_MESSAGE_EXT_ZF) {
    M->flags = 0;
  } else {
    assert (0 && "Invalid LEV_TX_ADD_MESSAGE event");
  }

  if (verbosity > 1 && U->last_local_id > 1800 && E->user_id == 6492 && ((M->flags & 199) == 0 || (U->last_local_id > 1864 && U->last_local_id < 1880))) {
    fprintf (stderr, "created new message with local id %d; origin: binlog record %08x, date %d, global id %d, peer %d, rec ptr %p, legacy id %016llx, thr %p, log cur pos %016llx\n", U->last_local_id, E->type, M->date, last_global_id, E->peer_msg_id, E, M->legacy_id, threshold_msg, log_cur_pos ());
  }

  packed_bytes = M->len = pack_text (M->text + text_shift, MAX_MESSAGE_SIZE, E->text + extra_bytes, bytes);
  //memcpy (M->extra, E->extra, extra_bytes); what the hell was here?

  extra = E->extra;
  
  if (extra_bytes) {
    int src_mask = unpack_extra_mask (E->type & MAX_EXTRA_MASK);

    for (i = 0; i < MAX_EXTRA_INTS; i++, src_mask >>= 1) {
      if (src_mask & 1) {
        if (SE[i] >= 0) {
          M->extra[SE[i]] = *extra;
        }
        ++extra;
      }
    }
  }

  factual_packed_bytes += packed_bytes;
  factual_unpacked_bytes += bytes;
  factual_messages++;

  if (verbosity > 200) {
    int i;
    verbosity--;
    fprintf (stderr, "%d bytes '%s' packed to %d bytes:\n", bytes, E->text, packed_bytes);
    for (i = 0; i < packed_bytes; i++) {
      fprintf (stderr, "%02x", (unsigned char) M->text[i]);
    }
    fprintf (stderr, "\n");
  }

  assert (dyn_alloc (sizeof (message_t) + text_shift + packed_bytes, 4) == (void *) M);

}


void replace_message_text1 (struct lev_replace_text_long *E) {
  int text_len;
  char *text;
  int local_id = E->local_id;

  if (E->type == LEV_TX_REPLACE_TEXT_LONG) {
    text = E->text;
    text_len = E->text_len;
  } else {
    assert ((E->type & -0x1000) == LEV_TX_REPLACE_TEXT);
    text = ((struct lev_replace_text *) E)->text;
    text_len = E->type & 0xfff;
  }

  int uid = conv_uid (E->user_id);
  int packed_bytes;
  user_t *U;
  message_t *M;

  if (uid < 0) {
    return;
  }
  U = User[uid];
  assert (U);

  if (uid < cur_min_uid || uid >= cur_max_uid) {
    return;
  }

  assert (local_id > 0 && local_id <= U->last_local_id);

  if (local_id < U->first_local_id) {
    return;
  }

  assert (dyn_top >= dyn_cur + sizeof (message_t) + MAX_MESSAGE_SIZE + 16 + 4 * MAX_EXTRA_INTS);
  M = (message_t *) dyn_cur;
  memset (M, 0, sizeof (message_t) + text_shift);

  if (threshold_msg == (void *) -1L && now) {
    threshold_msg = M;
  }

  M->prev = U->last;
  U->last = M;

  M->user_id = E->user_id;
  M->local_id = local_id;
  M->date = now;

  M->flags = TEXT_REPLACE_FLAGS;

  packed_bytes = M->len = pack_text (M->text + text_shift, MAX_MESSAGE_SIZE, text, text_len);

  factual_packed_bytes += packed_bytes;
  factual_unpacked_bytes += text_len;
  factual_messages++;
  replaced_messages++;

  if (verbosity > 200) {
    int i;
    verbosity--;
    fprintf (stderr, "%d bytes '%s' packed to %d bytes:\n", text_len, text, packed_bytes);
    for (i = 0; i < packed_bytes; i++) {
      fprintf (stderr, "%02x", (unsigned char) M->text[i]);
    }
    fprintf (stderr, "\n");
  }

  assert (dyn_alloc (sizeof (message_t) + text_shift + packed_bytes, 4) == (void *) M);

}


void delete_first_messages1 (int user_id, int first_local_id) {
}


void change_extra_mask1 (int new_mask) {
  int i;
  ++extra_mask_changes;
  for (i = 0; i < MAX_EXTRA_FIELDS; i++) {
    if ((new_mask & (1 << i)) && extra_mask_changes >= extra_field_start_offset[i]) {
      current_extra_mask |= 1 << i;
    }
  }
  current_extra_mask &= new_mask & final_extra_mask;
}

void prepare_extra_mask (void) {
  int i, unpacked_mask;
  final_extra_mask = current_extra_mask;
  extra_ints_num = 0;

  unpacked_mask = unpack_extra_mask (final_extra_mask);
  for (i = 0; i < MAX_EXTRA_INTS; i++) {
    SE[i] = ES[i] = -1;
    if (unpacked_mask & (1 << i)) {
      SE[i] = extra_ints_num;
      ES[extra_ints_num++] = i;
    }
  }
  text_shift = extra_ints_num * 4;
  if (verbosity > 0) {
    fprintf (stderr, "extra_mask=%08x, extra_ints=%d\n", final_extra_mask, extra_ints_num);
  }
}

int text_replay_logevent (struct lev_generic *E, int size) {
  int s, t, ts;
  switch (E->type) {
  case LEV_START:
    if (size < 24 || E->b < 0 || E->b > 4096) { return -1; }
    s = 24 + ((E->b + 3) & -4);
    if (size < s) { return -1; }
    if (process_message == process_message1 && temp_binlog_directory) {
      assert (E->a == 0x504d4554 && s == 24);
      current_extra_mask = 0;
      extra_mask_changes = 0;
      change_extra_mask (init_extra_mask);
      return s;
    }
    return text_le_start ((struct lev_start *) E) >= 0 ? s : -1;
  case LEV_NOOP:
  case LEV_TIMESTAMP:
  case LEV_CRC32:
  case LEV_ROTATE_FROM:
  case LEV_ROTATE_TO:
  case LEV_TAG:
    return default_replay_logevent (E, size);
  case LEV_TX_INCREASE_GLOBAL_ID_SMALL ... LEV_TX_INCREASE_GLOBAL_ID_SMALL + 0xffff:
    if (size < 4) { return -2; }
    assert (process_message == process_message1 && temp_binlog_directory);
    last_global_id += E->type & 0xffff;
    return 4;
  case LEV_TX_INCREASE_GLOBAL_ID_LARGE:
    if (size < 8) { return -2; }
    assert (process_message == process_message1 && temp_binlog_directory);
    last_global_id += E->a;
    return 8;
  case LEV_TX_ADD_MESSAGE ... LEV_TX_ADD_MESSAGE + 0xff:
  case LEV_TX_ADD_MESSAGE_MF ... LEV_TX_ADD_MESSAGE_MF + 0xfff:
  case LEV_TX_ADD_MESSAGE_EXT ... LEV_TX_ADD_MESSAGE_EXT + MAX_EXTRA_MASK:
  case LEV_TX_ADD_MESSAGE_EXT_ZF ... LEV_TX_ADD_MESSAGE_EXT_ZF + MAX_EXTRA_MASK:
  case LEV_TX_ADD_MESSAGE_EXT_LL ... LEV_TX_ADD_MESSAGE_EXT_LL + MAX_EXTRA_MASK:
  case LEV_TX_ADD_MESSAGE_LF:
    if (size < sizeof (struct lev_add_message)) { return -2; }
    struct lev_add_message *EM = (void *) E;
    s = sizeof (struct lev_add_message) + EM->text_len + 1;
    ts = 0;
    if ((E->type & ~MAX_EXTRA_MASK) == LEV_TX_ADD_MESSAGE_EXT || (E->type & ~MAX_EXTRA_MASK) == LEV_TX_ADD_MESSAGE_EXT_ZF || 
	(E->type & ~MAX_EXTRA_MASK) == LEV_TX_ADD_MESSAGE_EXT_LL) {
      s += ts = extra_mask_intcount (E->type & MAX_EXTRA_MASK) * 4;
    }
    if (size < s) { return -2; }
    if ((unsigned) EM->text_len >= MAX_TEXT_LEN || EM->text[ts + EM->text_len]) { 
      return -4; 
    }
    if (conv_uid (EM->user_id) >= 0) {
      process_message (EM, ts);
      adj_rec++;
    } else {
      discarded_rec++;
    }

//    if (verbosity > 0 && ((adj_rec + discarded_rec) & 65535) == 0) {
//      fprintf (stderr, "processed %d, discarded %d, word instances %d, non-word instances %d, dictionary size %d+%d\n",
//                 adj_rec, discarded_rec, word_instances, nonword_instances, Words.cnt, NotWords.cnt);
//    }
    return s;
  case LEV_TX_REPLACE_TEXT ... LEV_TX_REPLACE_TEXT + 0xfff:
    if (size < sizeof (struct lev_replace_text)) { return -2; }
    t = E->type & 0xfff;
    s = offsetof (struct lev_replace_text, text) + t + 1; 
    if (size < s) { return -2; }
    if ((unsigned) t >= MAX_TEXT_LEN || ((struct lev_replace_text *) E)->text[t]) {
      return -4;
    }
    replace_message_text ((struct lev_replace_text_long *) E);
    return s;
  case LEV_TX_REPLACE_TEXT_LONG:
    if (size < sizeof (struct lev_replace_text_long)) { return -2; }
    t = ((struct lev_replace_text_long *) E)->text_len;
    s = offsetof (struct lev_replace_text_long, text) + t + 1; 
    if (size < s) { return -2; }
    if ((unsigned) t >= MAX_TEXT_LEN || ((struct lev_replace_text_long *) E)->text[t]) {
      return -4;
    }
    replace_message_text ((struct lev_replace_text_long *) E);
    return s;
  case LEV_TX_DEL_MESSAGE:
    if (size < 12) { return -2; }
    adjust_message (E->a, E->b, 0, MSG_ADJ_DEL, 0);
    return 12;
  case LEV_TX_DEL_FIRST_MESSAGES:
    if (size < 12) { return -2; }
    delete_first_messages (E->a, E->b);
    return 12;
  case LEV_TX_SET_MESSAGE_FLAGS ... LEV_TX_SET_MESSAGE_FLAGS+0xff:
    if (size < 12) { return -2; }
    adjust_message (E->a, E->b, E->type & 0xff, MSG_ADJ_SET, 0);
    return 12;
  case LEV_TX_SET_MESSAGE_FLAGS_LONG:
    if (size < 16) { return -2; }
    adjust_message (E->a, E->b, E->c & 0xffff, MSG_ADJ_SET, 0);
    return 16;
  case LEV_TX_INCR_MESSAGE_FLAGS ... LEV_TX_INCR_MESSAGE_FLAGS+0xff:
    if (size < 12) { return -2; }
    adjust_message (E->a, E->b, E->type & 0xff, MSG_ADJ_INC, 0);
    return 12;
  case LEV_TX_INCR_MESSAGE_FLAGS_LONG:
    if (size < 16) { return -2; }
    adjust_message (E->a, E->b, E->c & 0xffff, MSG_ADJ_INC, 0);
    return 16;
  case LEV_TX_DECR_MESSAGE_FLAGS ... LEV_TX_DECR_MESSAGE_FLAGS+0xff:
    if (size < 12) { return -2; }
    adjust_message (E->a, E->b, E->type & 0xff, MSG_ADJ_DEC, 0);
    return 12;
  case LEV_TX_DECR_MESSAGE_FLAGS_LONG:
    if (size < 16) { return -2; }
    adjust_message (E->a, E->b, E->c & 0xffff, MSG_ADJ_DEC, 0);
    return 16;
  case LEV_TX_SET_EXTRA_FIELDS ... LEV_TX_SET_EXTRA_FIELDS + MAX_EXTRA_MASK:
    s = 12 + 4 * extra_mask_intcount (E->type & MAX_EXTRA_MASK);
    if (size < s) { return -2; }
    adjust_message (E->a, E->b, E->type & MAX_EXTRA_MASK, MSG_ADJ_SET, ((struct lev_set_extra_fields *) E)->extra);
    return s;
  case LEV_TX_INCR_FIELD ... LEV_TX_INCR_FIELD + 7:
    if (size < 16) { return -2; }
    adjust_message (E->a, E->b, 1 << (E->type & 0xf), MSG_ADJ_INC, &E->c);
    return 16;
  case LEV_TX_INCR_FIELD_LONG + 8 ... LEV_TX_INCR_FIELD_LONG + 11:
    if (size < 20) { return -2; }
    adjust_message (E->a, E->b, 1 << (E->type & 0xf), MSG_ADJ_INC, &E->c);
    return 20;
  case LEV_CHANGE_FIELDMASK_DELAYED:
    if (size < 8) { return -2; }
    change_extra_mask (E->a);
    return 8;
  }
   
  fprintf (stderr, "unknown log event type %08x at position %lld\n", E->type, (long long) log_cur_pos());

  return -3;

}

int init_text_data (int schema) {
  replay_logevent = text_replay_logevent;
  return 0;
}


/*
 *
 * GENERIC BUFFERED WRITE
 *
 */

#define	BUFFSIZE	16777216

char Buff[BUFFSIZE], *rptr = Buff, *wptr = Buff;
long long write_pos;
int metafile_pos;
unsigned crc32_acc;

struct text_index_header Header;

void flushout (void) {
  int w, s;
  if (rptr < wptr) {
    s = wptr - rptr;
    w = write (fd[0], rptr, s);
    assert (w == s);
  }
  rptr = wptr = Buff;
}

void clearin (void) {
  rptr = wptr = Buff + BUFFSIZE;
}

void writeout (const void *D, size_t len) {
  const char *d = D;
  crc32_acc = crc32_partial (D, len, crc32_acc); 
  while (len > 0) {
    int r = Buff + BUFFSIZE - wptr;
    if (r > len) { 
      r = len; 
    }
    memcpy (wptr, d, r);
    d += r;
    wptr += r;
    write_pos += r;
    metafile_pos += r;
    len -= r;
    if (len > 0) {
      flushout();
    }
  }
}

static inline void initcrc (void) {
  crc32_acc = (unsigned) -1;
}

static inline void writecrc (void) {
  unsigned crc32 = ~crc32_acc;
  writeout (&crc32, 4);
  crc32_acc = (unsigned) -1;
}

void write_seek (long long new_pos) {
  flushout();
  assert (lseek (fd[0], new_pos, SEEK_SET) == new_pos);
  write_pos = new_pos;
}

void *readin (size_t len) {
  assert (len >= 0);
  if (rptr + len <= wptr) {
    return rptr;
  }
  if (wptr < Buff + BUFFSIZE) {
    return 0;
  }
  memcpy (Buff, rptr, wptr - rptr);
  wptr -= rptr - Buff;
  rptr = Buff;
  int r = read (fd[0], wptr, Buff + BUFFSIZE - wptr);
  if (r < 0) {
    fprintf (stderr, "error reading file: %m\n");
  } else {
    wptr += r;
  }
  if (rptr + len <= wptr) {
    return rptr;
  } else {
    return 0;
  }
}

void readadv (size_t len) {
  assert (len >= 0 && len <= wptr - rptr);
  rptr += len;
}

/*
 *
 *  BUILD MODEL (FOR HUFFMAN)
 *
 */

long long hapax_words, hapax_notwords, rare_words, rare_notwords, word_codes_cnt, notword_codes_cnt;
long long total_word_bits, total_word_char_bits, total_notword_bits, total_notword_char_bits;
long long total_packed_bytes;
int hash_conflicts;
int dict_size;

long long word_char_freq[256], notword_char_freq[256], word_len_freq[256], notword_len_freq[256];


/* these data structures are needed for unpacking if we want search indexes */

struct word_dictionary {
/*  char *raw_data;
  int raw_data_len;
  int word_num;
  int reserved; */
  word_t **words;
  int max_bits;
  unsigned first_codes[32];
  word_t **code_ptr[32];
};

struct word_dictionary WordDict, NotWordDict;


int increase_rare_word_char_freq (long long Freq[], long long LenFreq[], word_hash_t *Set, int max_freq) {
  int i, j, cnt = 0;
  long long f;
  word_t *ptr, **pptr;
  for (i = 0; i < PRIME; i++) {
    for (pptr = &Set->A[i], ptr = *pptr; ptr; ptr = *pptr) {
      f = ptr->freq;
      if (f <= max_freq && f > 0) {
	*pptr = ptr->next;
	ptr->freq = -f;
	cnt++;
	LenFreq[ptr->len] += f;
	for (j = 0; j < ptr->len; j++) {
	  Freq[(unsigned char) ptr->str[j]] += f;
	}
	zfree (ptr, sizeof (word_t) + ptr->len);
      } else {
	pptr = &ptr->next;
      }
    }
  }
  return cnt;
}

/* ======================= Huffman model (code lengths vector) builder ==================== */

long long *huffman_alloc (long long N) {
  assert ((long long) (dyn_top - dyn_cur) >= N * 8);
  return (long long *) dyn_cur;
}

void huffman_dealloc (long long *table, long N) {
  dyn_cur += N * 8;
}

/* ------------ classical Huffman implementation -------------- */

static void heap_sift (long long *A, long N, long long x) {
  int i = 1, j;
  long long h = A[x], u;
  while (1) {
    j = (i << 1);
    if (j > N) { 
      break; 
    }
    u = A[(long) A[j]];
    if (j < N && A[(long) A[j+1]] < u) { 
      u = A[(long) A[++j]]; 
    }
    if (u >= h) { 
      break; 
    }
    A[i] = A[j];
    i = j;
  }
  A[i] = x;
}


int huffman_build (long long *A, int N) {
  int i, j, k, M = 0;

  A += N-1;

  /* build heap from pointers to non-zero frequences */
  for (i = 1 - N; i <= 0; i++) {
    if (A[i] > 0) {
      j = ++M;
      while (j > 1) {
	k = (j >> 1);
	if (A[(long) A[k]] <= A[i]) { break; }
	A[j] = A[k];
	j = k;
      }
      A[j] = i;
    }
  }

  if (M < 2) {
    if (M) {
      A[(long) A[1]] = 1;
    }
    return M;
  }

  k = M;

  /* while there are at least two elements in heap, combine two smallest of them into one */
  while (M >= 2) {
    long long x = A[1], y = A[M--];
    heap_sift (A, M, y);
    y = A[1];
    A[M+1] = A[x] + A[y];
    A[x] = A[y] = M+1;
    heap_sift (A, M, M+1);
  }

  /* compute code lengths */
  M = k;
  A[2] = 0;
  for (i = 3; i <= M; i++) {
    A[i] = A[(long) A[i]] + 1;
  }

  for (i = 1 - N; i <= 0; i++) {
    if (A[i] > 0) {
      A[i] = A[(long) A[i]] + 1;
    }
  }

  return M;
}

long long huffman_build_char_dictionary (struct char_dictionary *Dict, long long *code_len, long long *freq) {
  long long total_bits = 0;
  static int code_cnt[32];
  int i, j;
  for (i = 0; i < 32; i++) {
    code_cnt[i] = 0;
  }
  for (i = 0; i < 256; i++) {
    if ((unsigned long long) code_len[i] > 32) {
      fprintf (stderr, "fatal: character %02x encoded with %lld > 32 bits\n", i, code_len[i]);
      exit (3);
    }
    assert (freq[i] >= 0);
    total_bits += (long long) code_len[i] * freq[i];
    code_cnt[code_len[i]]++;
    Dict->code_len[i] = code_len[i];
    Dict->freq[i] = freq[i];
  }
  unsigned x = 0, y = (1 << 31);
  int *cptr = Dict->chars;
  for (i = 1; i <= 32; i++) {
    Dict->first_codes[i-1] = x;
    Dict->code_ptr[i-1] = cptr - (x >> (32 - i));
    for (j = 0; j < 256; j++) {
      if (code_len[j] == i) {
        *cptr++ = j;
        Dict->code[j] = x;
        x += y;
        assert (x >= y || !x);
      }

    }
    y >>= 1;
  }
  assert (!x);

  i = 32;
  while (i && !Dict->first_codes[i-1]) {
    i--;
  }
  Dict->max_bits = i;

  if (verbosity > 1) {
    for (i = 0; i < 256; i++) {
      fprintf (stderr, "character %02x ('%c'): %08x:%d\n", i, (i < 32 ? '.' : i), Dict->code[i], Dict->code_len[i]);
    }
  }
    
  return total_bits;
}

long long export_codelen (long long *table, word_hash_t *Set) {
  int i;
  word_t *ptr;
  long long sum = 0;
  for (i = 0; i < PRIME; i++) {
    for (ptr = Set->A[i]; ptr; ptr = ptr->next) {
      if (ptr->freq > 0) {
	ptr->code_len = *table++;
	sum += (long long) ptr->code_len * ptr->freq;
	dict_size += ptr->len;
	if (ptr->code_len <= 10 && verbosity > 1) {
	  fprintf (stderr, "'%.*s'\t%lld\t%d\n", ptr->len, ptr->str, ptr->freq, ptr->code_len);
	}
      }
    }
  }
  return sum;
}

static inline int word_cmp (word_t *a, word_t *b) {
  int al = a->len, bl = b->len;
  int x = memcmp (a->str, b->str, al < bl ? al : bl);
  return x ? x : al - bl;
}

void word_sort (word_t **A, int b) {
  int i = 0, j = b;
  word_t *h, *t;
  if (b <= 0) { return; }
  h = A[b >> 1];
  do {
    while (word_cmp (A[i], h) < 0) { i++; }
    while (word_cmp (A[j], h) > 0) { j--; }
    if (i <= j) {
      t = A[i];  A[i++] = A[j];  A[j--] = t;
    }
  } while (i <= j);
  word_sort (A+i, b-i);
  word_sort (A, j);
}

void build_sorted_word_list (word_t *List[], word_hash_t *Set, long N) {
  int i;
  word_t **cur = List, **last = cur + N, *ptr;
  for (i = 0; i < PRIME; i++) {
    for (ptr = Set->A[i]; ptr; ptr = ptr->next) {
      if (ptr->freq > 0) {
        *cur++ = ptr;
        assert (cur <= last);
      }
    }
  }
  if (cur != last) {
    fprintf (stderr, "expected %lld words, found %lld\n", (long long) N, (long long) (last - List));
  }
  assert (cur == last);
  word_sort (List, N-1);
}

long long huffman_assign_word_codes (word_t *List[], long N, struct word_dictionary *Dict) {
  long long total_bits = 0;
  static int code_cnt[32];
  int i, j;
  word_t *W, **W_ptr;

  W_ptr = Dict->words = malloc (sizeof (word_t *) * N);
  assert (W_ptr);

  for (i = 0; i < 32; i++) {
    code_cnt[i] = 0;
  }
  for (i = 0; i < N; i++) {
    W = List[i];
    if (W->code_len > 32) {
      fprintf (stderr, "huffman build error: string '%.*s' (freq=%lld) encoded with %d > 32 bits (will try package-merge instead)\n", W->len, W->str, W->freq, W->code_len);
      free (W_ptr);
      Dict->words = 0;
      return -1;
    }
    assert (W->freq >= 0);
    total_bits += (long long) W->code_len * W->freq;
    code_cnt[W->code_len]++;
  }

  unsigned x = 0, y = (1 << 31);

  for (i = 1; i <= 32; i++) {
    Dict->code_ptr[i-1] = W_ptr - (x >> (32 - i));
    Dict->first_codes[i-1] = x;

    for (j = 0; j < N; j++) {
      W = List[j];
      if (W->code_len == i) {
        *W_ptr++ = W;
        W->code = x;
        x += y;
        assert (x >= y || !x);
      }

    }
    y >>= 1;
  }
  assert (!x);

  i = 32;
  while (i && !Dict->first_codes[i-1]) {
    i--;
  }
  Dict->max_bits = i;


  x = 0;
  if (verbosity > 1) {
    for (i = 1; i <= 32; i++) {
      for (j = 0; j < N && x < 256; j++) {
        W = List[j];
        if (W->code_len == i) {
          fprintf (stderr, "string '%.*s' (freq=%lld): %08x:%d\n", W->len, W->str, W->freq, W->code, W->code_len);
          x++;
        }
      }
    }
  }
    
  return total_bits;
}

/* ------------- package-merge algorithm ---------------- */

typedef union pm_cell {
  word_t *w;
  struct {
    int left, right;
  };
} pm_cell_t;

void pm_sort (pm_cell_t *A, int b) {
  int i = 0, j = b;
  word_t *t;
  long long h;
  if (b <= 0) { return; }
  h = A[b >> 1].w->freq;
  do {
    while (A[i].w->freq < h) { i++; }
    while (A[j].w->freq > h) { j--; }
    if (i <= j) {
      t = A[i].w;  A[i++].w = A[j].w;  A[j--].w = t;
    }
  } while (i <= j);
  pm_sort (A+i, b-i);
  pm_sort (A, j);
}

long pm_preinit_freq (pm_cell_t *A, word_hash_t *Set) {
  long cnt = 0;
  int i;
  word_t *ptr;
  for (i = 0; i < PRIME; i++) {
    for (ptr = Set->A[i]; ptr; ptr = ptr->next) {
      if (ptr->freq > 0) {
	(A++)->w = ptr;
	ptr->code_len = 0;
	cnt++;
      }
    }
  }
  return cnt;
}

#define	FREQ_INFTY	(1LL << 60)

struct pm_tree_builder {
  int choice;   /* 0 or 1 with minimal freq[choice] */
  int next[2];  /* (negative) index of next simple word, or 0 if end of list */
		/* (positive) index of next combined node, or 0 */
  long long freq[2]; /* A[next[0]].w->freq ; weight of node at next[1] */
};

static struct pm_tree_builder PB[33];

static inline void print_tree_builder (pm_cell_t *A, struct pm_tree_builder *B) {
  fprintf (stderr, "TB[%d]: %d {%d, %lld} {%d: (%d, %d), %lld}\n", (int) (B - PB), B->choice, B->next[0], B->freq[0], B->next[1], A[B->next[1]].left, A[B->next[1]].right, B->freq[1]);
}

static void pm_tree_advance (pm_cell_t *A, struct pm_tree_builder *B);

static void pm_tree_adva (pm_cell_t *A, struct pm_tree_builder *B) {
  if (!B->next[0]) {
    return;
  }
  int n = ++B->next[0];
  if (!n) {
    B->freq[0] = FREQ_INFTY;
    B->choice = (B->next[1] != 0);
    return;
  }
  long long f = A[n].w->freq;
  B->freq[0] = f;
  B->choice = (B->freq[1] < f);
}

static void pm_tree_advb (pm_cell_t *A, struct pm_tree_builder *B) {
  ++B;
  int n1 = B->next[B->choice];
  if (!n1) {
    --B;
    B->next[1] = 0;
    B->freq[1] = FREQ_INFTY;
    B->choice = 0;
    return;
  }
  long long f = B->freq[B->choice];
  pm_tree_advance (A, B);
  int n2 = B->next[B->choice];
  if (!n2) {
    --B;
    B->next[1] = 0;
    B->freq[1] = FREQ_INFTY;
    B->choice = 0;
    return;
  }
  f += B->freq[B->choice];
  pm_tree_advance (A, B);
  int n = A[0].left;
  if (n) {
    A[0].left = A[n].left;
  } else { /* no cells in free list */
    n = --A[0].right;
    assert (n && "Out of package-merge cell memory" != 0);
  }
  A[n].left = n1;
  A[n].right = n2;
  --B;
  B->next[1] = n;
  B->freq[1] = f;
  B->choice = (f < B->freq[0]);
}

static void pm_tree_advance (pm_cell_t *A, struct pm_tree_builder *B) {
  if (B->choice) {
    pm_tree_advb (A, B);
  } else {
    pm_tree_adva (A, B);
  }
}

static void pm_tree_free (pm_cell_t *A, int n) {
  assert (n);
  if (n < 0) {
    A[n].w->code_len++;
    // fprintf (stderr, "%d[%d]", n, A[n].w->code_len);
  } else {
    // fprintf (stderr, "%d:(", n);
    pm_tree_free (A, A[n].left);
    // fprintf (stderr, " ");
    pm_tree_free (A, A[n].right);
    // fprintf (stderr, ")");
    A[n].left = A[0].left;
    A[0].left = n;
  }
}

void pm_huffman_build (pm_cell_t *A, int N) {
  int i;
  assert ((unsigned) N < (1 << 27) && N > 1); // we need -N .. 7*N to fit in int
  A += N;
  A[0].left = 0;	   // head of free cell list
  A[0].right = 15 * N;      // last available cell + 1
  PB[32].freq[0] = PB[32].freq[1] = FREQ_INFTY;
  // print_tree_builder (A, PB + 32);
  for (i = 31; i >= 0; i--) {
    PB[i].next[0] = -N;
    PB[i].freq[0] = A[-N].w->freq;
    pm_tree_advb (A, PB + i);
    // print_tree_builder (A, PB + i);
  }
  for (i = 2*N - 2; i > 0; i--) { /* do N-1 times */
    // print_tree_builder (A, PB);
    int n = PB->next[PB->choice];
    assert (n);
    // fprintf (stderr, "#%d:\t%lld\t", i, PB->freq[PB->choice]);
    pm_tree_free (A, n);
    // fprintf (stderr, "\n");
    pm_tree_advance (A, PB);
  }
  if (verbosity > 1) {
    fprintf (stderr, "package-merge: maximal memory usage is %d+%d 8-byte cells out of %d+%d\n", N, 15 * N - A[0].right, N, 15 * N);
  }
}

/* --------------- build Huffman codes ------------------ */

void build_model (void) {
  int i;
  static char tmpw[4];
  long N;
  word_t *w;
  long long *table;
  pm_cell_t *pm_table;

  hapax_words = count_rare_words (&Words, 1);
  hapax_notwords = count_rare_words (&NotWords, 1);

  rare_words = increase_rare_word_char_freq (word_char_freq, word_len_freq, &Words, max_nd_freq);
  rare_notwords = increase_rare_word_char_freq (notword_char_freq, notword_len_freq, &NotWords, max_nd_freq);

  tmpw[0] = 0;
  for (i = 0; i < 256; i++) {
    tmpw[1] = i;
    if (word_len_freq[i]) {
      w = hash_get (&Words, tmpw, 2, 1);
      w->freq = word_len_freq[i];
      WordFreqWords[i] = w;
    }
    if (notword_len_freq[i]) {
      w = hash_get (&NotWords, tmpw, 2, 1);
      w->freq = notword_len_freq[i];
      NotWordFreqWords[i] = w;
    }
  }

  table = huffman_alloc (256 * 2);
  assert (table);

  if (verbosity > 0) {
    fprintf (stderr, "building Huffman code for word characters...\n");
  }

  for (i = 0; i < 256; i++) {
    table[i] = word_char_freq[i];
  }

  huffman_build (table, 256);
  total_word_char_bits = huffman_build_char_dictionary (&WordCharDict, table, word_char_freq);
  if (verbosity > 1) {
    for (i = 0; i < 256; i++) {
      if (word_char_freq[i] || table[i]) {
        fprintf (stderr, "%c %d:\t%lld\t%lld\n", i >= 0x20 && i != 0x7f ? i : '.', i, word_char_freq[i], table[i]);
      }
    }
  }  

  if (verbosity > 0) {
    fprintf (stderr, "building Huffman code for non-word characters...\n");
  }

  for (i = 0; i < 256; i++) {
    table[i] = notword_char_freq[i];
  }
  huffman_build (table, 256);
  total_notword_char_bits = huffman_build_char_dictionary (&NotWordCharDict, table, notword_char_freq);
  if (verbosity > 1) {
    for (i = 0; i < 256; i++) {
      if (notword_char_freq[i] || table[i]) {
        fprintf (stderr, "%c %d:\t%lld\t%lld\n", i >= 0x20 && i != 0x7f ? i : '.', i, notword_char_freq[i], table[i]);
      }
    }
  }

  huffman_dealloc (table, 0);

  if (verbosity > 0) {
    fprintf (stderr, "sorting words...\n");
  }

  word_codes_cnt = N = Words.cnt - rare_words;
  WordList = zmalloc (N * sizeof (void *));
  build_sorted_word_list (WordList, &Words, N);

  if (verbosity > 0) {
    fprintf (stderr, "building Huffman code for words...\n");
  }

  table = huffman_alloc (N * 2);
  assert (table);
  assert (import_freq (table, &Words) == N);
  huffman_build (table, N);
  total_word_bits = export_codelen (table, &Words);
  huffman_dealloc (table, 0);
  long long hr = huffman_assign_word_codes (WordList, N, &WordDict);

  if (hr < 0 || force_pm) {
    if (verbosity > 0) {
      fprintf (stderr, "invoking package-merge algorithm to build Huffman code for words...\n");
    }
    assert (N < (1L << 27));
    pm_table = (pm_cell_t *) huffman_alloc (N * 16);
    assert (pm_table);
    assert (pm_preinit_freq (pm_table, &Words) == N);
    pm_sort (pm_table, N - 1);
    pm_huffman_build (pm_table, N);
    huffman_dealloc ((long long *) pm_table, 0);
    hr = huffman_assign_word_codes (WordList, N, &WordDict);
    if (verbosity > 0) {
      fprintf (stderr, "package-merge wants to use %lld word bits, Huffman wanted %lld\n", hr, total_word_bits);
    }
    if (hr < 0) {
      fprintf (stderr, "**FATAL**: package-merge assigned word code longer than 32 bits\n");
      exit (3);
    }
    total_word_bits = hr;
  }

  assert (hr == total_word_bits);

  if (verbosity > 0) {
    fprintf (stderr, "sorting not-words...\n");
  }

  notword_codes_cnt = N = NotWords.cnt - rare_notwords;
  NotWordList = zmalloc (N * sizeof (void *));
  build_sorted_word_list (NotWordList, &NotWords, N);

  if (verbosity > 0) {
    fprintf (stderr, "building Huffman code for not-words...\n");
  }

  table = huffman_alloc (N * 2);
  assert (table);
  assert (import_freq (table, &NotWords) == N);
  huffman_build (table, N);
  total_notword_bits = export_codelen (table, &NotWords);
  huffman_dealloc (table, 0);
  hr = huffman_assign_word_codes (NotWordList, N, &NotWordDict);

  if (hr < 0 || force_pm) {
    if (verbosity > 0) {
      fprintf (stderr, "invoking package-merge algorithm to build Huffman code for notwords...\n");
    }
    assert (N < (1L << 27));
    pm_table = (pm_cell_t *) huffman_alloc (N * 16);
    assert (pm_table);
    assert (pm_preinit_freq (pm_table, &NotWords) == N);
    pm_sort (pm_table, N - 1);
    pm_huffman_build (pm_table, N);
    huffman_dealloc ((long long *) pm_table, 0);
    hr = huffman_assign_word_codes (NotWordList, N, &NotWordDict);
    if (verbosity > 0) {
      fprintf (stderr, "package-merge wants to use %lld not-word bits, Huffman wanted %lld\n", hr, total_notword_bits);
    }
    if (hr < 0) {
      fprintf (stderr, "**FATAL**: package-merge assigned not-word code longer than 32 bits\n");
      exit (3);
    }
    total_notword_bits = hr;
  }

  assert (hr == total_notword_bits);

  total_packed_bytes = (total_word_bits + total_word_char_bits + total_notword_bits + total_notword_char_bits + (9 * (long long) msgs_read >> 1) + 7) >> 3;
}

static long long estimate_opt_pfx_size (unsigned long long *A, int N) {
  if (N <= 0) {
    return 0;
  }
  int k = (N - 1) >> 1;
  long long u = A[k] ^ A[k-1], v = A[k] ^ A[k+1];
  return __builtin_clzll (u < v ? u : v) + 1 - __builtin_clzll (A[-1] ^ A[N]) + 
         estimate_opt_pfx_size (A, k) + estimate_opt_pfx_size (A + k + 1, N - k - 1);
}

void test_word_hashes (void) {
  int i, j, N;
  word_t *ptr;
  unsigned long long *A, *Ae;
  long long tot_pfx_size = 0, opt_pfx_size;

  Ae = A = (unsigned long long *) (dyn_cur + 8);

  for (i = 0; i < PRIME; i++) {
    for (ptr = Words.A[i]; ptr; ptr = ptr->next) {
      assert ((char *) Ae + 16 <= dyn_top);
      *Ae++ = crc64 (ptr->str, ptr->len);
    }
  }

  N = Ae - A;

  if (!N) {
    return;
  }

  ull_sort (A, N-1);

  for (i = 1, j = 1; i < N; i++) {
    if (A[i] != A[i-1]) {
      A[j++] = A[i];
    }
  }

  hash_conflicts = N - j;

  fprintf (stderr, "%d distinct words, %d distinct word hashes (%d conflicts)\n", N, j, hash_conflicts);

  N = j;

  A[-1] = -1LL;
  A[N] = 0;

  for (i = 0; i < N; i++) {
    unsigned long long u = A[i] ^ A[i-1], v = A[i] ^ A[i+1];
    int pfx_size = __builtin_clzll (u < v ? u : v) + 1;
    tot_pfx_size += pfx_size;
  }

  opt_pfx_size = estimate_opt_pfx_size (A, N);

  fprintf (stderr, "total word hash prefix size %lld, %.3f average; optimal total size %lld, %.3f average\n", tot_pfx_size, (double) tot_pfx_size / N, opt_pfx_size, (double) opt_pfx_size / N);
}

int pass, passes, extra_passes;
double pass_threshold;

int compute_max_uid (int min_uid, double threshold) {
  double sum = 0, max_sw = 0, ratio = msgs_bytes ?  (double) total_packed_bytes / msgs_bytes : 1;
  int i;
  for (i = min_uid; i <= max_uid && sum + max_sw <= threshold; i++) {
    if (max_sw < UserSearchWords[i] * 16.0) {
      max_sw = UserSearchWords[i] * 16.0;
    }
    sum += UserMsgCnt[i] * (sizeof (message_t) + 4 * extra_ints_num + 2.2) + 
      UserMsgBytes[i] * ratio + UserMsgDel[i] * sizeof (struct message_short) +
      UserMsgExtras[i] * 4;
  }
  return i;
}

int check_needed_passes (double threshold) {
  int k = 0, uid = 0;
  while (uid <= max_uid) {
    int t = compute_max_uid (uid, threshold);
    if (t <= uid) {
      return 1000000000;
    }
    k++;
    uid = t;
  }
  return k;
}

int estimate_needed_passes (void) {
  double total = 0, max_sw, sum, ratio = msgs_bytes ? (double) total_packed_bytes / msgs_bytes : 1;
  double threshold = (dyn_top - dyn_cur) * (temp_binlog_directory ? 0.5 : 0.8);
  int i = 0, k = 0;
  while (i <= max_uid) {
    k++;
    max_sw = 0;
    for (sum = 0; i <= max_uid && sum + max_sw < threshold; i++) {
      if (max_sw < UserSearchWords[i] * 16.0) {
        max_sw = UserSearchWords[i] * 16.0;
      }
      sum += UserMsgCnt[i] * (sizeof (message_t) + 4 * extra_ints_num + 2.2) + 
      	UserMsgBytes[i] * ratio + UserMsgDel[i] * sizeof (struct message_short) +
      	UserMsgExtras[i] * 4;
    }
    total += sum + max_sw;
  }
  pass_threshold = (k ? total / k : threshold);

  assert (check_needed_passes (threshold) <= k);

  while (check_needed_passes (pass_threshold) > k) {
    pass_threshold *= 1.01;
    assert (pass_threshold <= threshold * 1.1);
  }

  return check_needed_passes (pass_threshold);
}

int prepare_pass_splitting (void) {
  int i, uid = 0;
  assert (passes > 0 && passes <= MAX_PASSES);
  for (i = 0; i < passes; i++) {
    pass_min_uid[i] = uid;
    uid = compute_max_uid (uid, pass_threshold);
  }
  pass_min_uid[i] = uid;
  assert (uid == max_uid + 1);
  if (temp_binlog_directory) {
    if (i <= 2) {
      vkprintf (1, "only %d passes needed, ignoring temporary directory\n", i);
      temp_binlog_directory = 0;
    }
  }
  return i;
}


/*
 *
 *  POST-PROCESS USER MESSAGES
 *
 */

void prepare_user_directory (void) {
  int i, j;
  int entry_size = sizeof (struct file_user_list_entry) + 4 * (sublists_num + (history_enabled ? 3 : search_enabled));
  user_dir_size = tot_users * entry_size + 16;
  Header.user_list_offset = write_pos;
  Header.tot_users = tot_users;
  Header.user_data_offset = write_pos + user_dir_size + 4;

  UserDirectoryData = zmalloc0 (user_dir_size);
  UserDirectory = zmalloc ((tot_users + 1) * sizeof (void *));

  for (j = 0; j <= tot_users; j++) {
    UserDirectory[j] = (struct file_user_list_entry *) (UserDirectoryData + entry_size * j);
  }

  for (i = j = 0; i < MAX_USERS; i++) {
    if (User[i]) {
      UserDirectory[j++]->user_id = unconv_uid (i);
    }
  }
  assert (j == tot_users);
  UserDirectory[j]->user_id = ~(-1LL << 63);
      
  writeout (UserDirectoryData, user_dir_size);
  initcrc ();
  writecrc ();
}

void write_user_directory (void) {
  Header.extra_data_offset = write_pos;
  Header.data_end_offset = write_pos;
  Header.tot_users = tot_users;

  UserDirectory[tot_users]->user_data_offset = write_pos;

  write_seek (Header.user_list_offset);
  initcrc ();
  writeout (UserDirectoryData, user_dir_size);
  writecrc ();
}

long long sum_peers_offset, sum_sublists_offset, sum_legacy_list_offset,
  sum_directory_offset, sum_data_offset, sum_extra_offset, sum_search_index_size, 
  sum_history_size, sum_total_bytes;


int processed_users;

int cur_msg_cnt, cur_unread_cnt, cur_messages_bytes;
int max_user_msg_cnt, max_user_msg_cnt_id, max_metafile_size, max_metafile_id;
int max_search_metafile_size, max_search_metafile_id;
int max_history_metafile_size, max_history_metafile_id;
struct message **Messages;
//[MAX_USER_MESSAGES];
//int legacy_id_index[MAX_USER_MESSAGES*2];
//int peer_id_index[MAX_USER_MESSAGES*2];
int *legacy_id_index;
int *peer_id_index;
int message_start_offset[MAX_USER_MESSAGES+1];
int peer_directory[MAX_USER_MESSAGES*2+1];
struct file_user_header UserHdr;

int count_sublists (void) {
  int i = 0;
  while (Sublists[i].xor_mask || Sublists[i].and_mask) {
    Sublists_packed[i].and_mask = Sublists[i].and_mask;
    Sublists_packed[i].xor_mask = Sublists[i].xor_mask;
    i++;
  }
  sublists_num = i;
  assert (sublists_num <= MAX_SUBLISTS);
  return i;
}

static inline int message_cmp (message_t *a, message_t *b) {
  int x = a->local_id - b->local_id;
  if (x) {
    return x;
  }
  return (a < b ? -1 : (a > b ? 1 : 0));
}

void message_sort (message_t **A, int b) {
  int i = 0, j = b;
  message_t *h, *t;
  if (b <= 0) { return; }
  h = A[b >> 1];
  do {
    while (message_cmp (A[i], h) < 0) { i++; }
    while (message_cmp (A[j], h) > 0) { j--; }
    if (i <= j) {
      t = A[i];  A[i++] = A[j];  A[j--] = t;
    }
  } while (i <= j);
  message_sort (A+i, b-i);
  message_sort (A, j);
}

static inline int message_cmp2 (message_t *a, message_t *b) {
  int x = a->date - b->date;
  if (x) {
    return x;
  }
  return (a < b ? -1 : (a > b ? 1 : 0));
}

void message_sort2 (message_t **A, int b) {
  int i = 0, j = b;
  message_t *h, *t;
  if (b <= 0) { return; }
  h = A[b >> 1];
  do {
    while (message_cmp2 (A[i], h) < 0) { i++; }
    while (message_cmp2 (A[j], h) > 0) { j--; }
    if (i <= j) {
      t = A[i];  A[i++] = A[j];  A[j--] = t;
    }
  } while (i <= j);
  message_sort2 (A+i, b-i);
  message_sort2 (A, j);
}



typedef struct pair pair_t;

struct pair {
  int key;
  int local_id;
};


#pragma pack(push, 4)
typedef struct llpair llpair_t;

struct llpair {
  long long key;
  int local_id;
};
#pragma pack(pop)


static inline int pair_cmp (struct pair *a, struct pair *b) {
  if (a->key < b->key) {
    return -1;
  } else if (a->key > b->key) {
    return 1;
  }
  return a->local_id - b->local_id;
}

void pair_sort (pair_t *A, int b) {
  int i = 0, j = b;
  pair_t h, t;
  if (b <= 0) { return; }
  h = A[b >> 1];
  do {
    while (pair_cmp (&A[i], &h) < 0) { i++; }
    while (pair_cmp (&A[j], &h) > 0) { j--; }
    if (i <= j) {
      t = A[i];  A[i++] = A[j];  A[j--] = t;
    }
  } while (i <= j);
  pair_sort (A+i, b-i);
  pair_sort (A, j);
}

static inline int llpair_cmp (struct llpair *a, struct llpair *b) {
  if (a->key < b->key) {
    return -1;
  } else if (a->key > b->key) {
    return 1;
  }
  return a->local_id - b->local_id;
}

void llpair_sort (llpair_t *A, int b) {
  int i = 0, j = b;
  llpair_t h, t;
  if (b <= 0) { return; }
  h = A[b >> 1];
  do {
    while (llpair_cmp (&A[i], &h) < 0) { i++; }
    while (llpair_cmp (&A[j], &h) > 0) { j--; }
    if (i <= j) {
      t = A[i];  A[i++] = A[j];  A[j--] = t;
    }
  } while (i <= j);
  llpair_sort (A+i, b-i);
  llpair_sort (A, j);
}

int peer_lists_size;
int cur_sublist_size[MAX_SUBLISTS], tot_sublists_size;
int sublist_directory[MAX_SUBLISTS*2+2];
int *sublists;

long user_search_index_size;
struct file_search_header *user_search_index;
struct file_history_header user_history_header;


static long build_user_search_index (int tot_msgs, int first_local_id, int last_local_id);

void process_user_messages (struct file_user_list_entry *UL, user_t *U) {
  struct message *tmp;
  int i, j, k, s, t, op, fl, fl_unpacked, history_size = 0;
  int ul_sublists_offset = history_enabled ? 3 : search_enabled;
  int *history = 0;

  i = 0;
  for (tmp = U->last; tmp; tmp = tmp->prev) {
    Messages[i++] = tmp;
    if (tmp->flags >= 0 || !(tmp->flags & TXF_HAS_EXTRAS)) {
      history_size++;
    }
    if (i >= MAX_USER_MESSAGES) {
      fprintf (stderr, "fatal: more than %d actions for user %d\n", i, U->user_id);
    }
    assert (i < MAX_USER_MESSAGES);
  }

  j = i - 1;
  if (msg_date_sort) {
    for (j = i - 1; j >= 0; j--) {
      if (Messages[j] >= threshold_msg) {
        break;
      }
      assert (Messages[j]->flags >= 0);
    }
    /* Messages[j+1 .. i-1] */
    message_sort2 (Messages + j+1, i-j-2);
    for (t = j+1; t < i; t++) {
      Messages[t]->local_id = t - j;
    }
    assert (!U->first_local_id || j == i - 1);
  }

  if (history_enabled && history_size) {
    history = malloc (history_size * 8);
    assert (history);
    k = 0;
    for (t = j+1; t < i; t++) {
      history[k++] = t - j;
      history[k++] = (Messages[t]->flags & 0xffff) | (4 << 24);
    }
    for (t = j; t >= 0; t--) {
      tmp = Messages[t];
      fl = tmp->flags;
      if (fl >= 0) {
	history[k++] = tmp->local_id;
	if (fl == TEXT_REPLACE_FLAGS) {
	  history[k++] = (5 << 24);
	} else {
	  history[k++] = (fl & 0xffff) | (4 << 24);
	}
      } else if (!(fl & TXF_HAS_EXTRAS)) {	
        op = (fl >> 29) & 3;
	history[k++] = tmp->local_id;
	history[k++] = (fl & 0xffff) | (op << 24);
      }
    }
    assert (k == history_size * 2);
  }

  message_sort (Messages, i-1);

  k = -1;
  for (j = 0; j < i; j++) {
    if (k >= 0 && Messages[j]->local_id == Messages[k]->local_id) {
      if (Messages[j]->flags >= 0) {
	if (Messages[j]->flags == TEXT_REPLACE_FLAGS) {
	  int len = Messages[j]->len;
	  memcpy (Messages[j], Messages[k], offsetof (struct message, text) + text_shift);
	  Messages[j]->len = len;
	}
        Messages[k] = Messages[j];
      } else {
        fl = Messages[j]->flags;
        op = (fl >> 29) & 3;
        if (fl & TXF_HAS_EXTRAS) {
          int *extra = ((struct message_medium *) Messages[j])->extra;
          fl >>= 16;
          fl &= MAX_EXTRA_MASK;

          fl_unpacked = unpack_extra_mask (fl);
          switch (op) {
          case MSG_ADJ_DEL:
            assert (op != MSG_ADJ_DEL);
            break;
          case MSG_ADJ_SET:
            for (t = 0; t < MAX_EXTRA_INTS; t++, fl_unpacked >>= 1) {
              if (fl_unpacked & 1) {
                assert (SE[t] >= 0);
                Messages[k]->extra[SE[t]] = *extra++;
              }
            }
            break;
          case MSG_ADJ_INC:
          case MSG_ADJ_DEC:
            assert (!(fl & (fl - 1)) && fl);
            fl = __builtin_ctz (fl_unpacked);
            assert (SE[fl] >= 0);
            if (fl < 8) {
              Messages[k]->extra[SE[fl]] += (op == MSG_ADJ_INC ? *extra : -*extra);
            } else {
              *(long long *)(Messages[k]->extra + SE[fl]) += (op == MSG_ADJ_INC ? *(long long *)extra : -*(long long *)extra);
            }  
          }
        } else {
          fl &= 0xffff;
          switch (op) {
          case MSG_ADJ_DEL:
            k--;
            break;
          case MSG_ADJ_SET:
            Messages[k]->flags = fl;
            break;
          case MSG_ADJ_INC:
            Messages[k]->flags |= fl;
            break;
          case MSG_ADJ_DEC:
            Messages[k]->flags &= ~fl;
            break;
          }
        }
      } 
    } else if (Messages[j]->flags >= 0) {
      assert (Messages[j]->flags != TEXT_REPLACE_FLAGS);
      Messages[++k] = Messages[j];
    }
  }

  /*
  if (msg_date_sort) {
    message_sort2 (Messages, k);
    for (t = 0; t <= k; t++) {
      Messages[t]->local_id = t + 1;
    }
  }
  */

  /*if (verbosity > 1) {
    fprintf (stderr, "user = %d, i = %d, k = %d\n", U->user_id, i, k);
  }*/

  cur_msg_cnt = k+1;
  if (cur_msg_cnt > max_user_msg_cnt) {
    max_user_msg_cnt = cur_msg_cnt;
    max_user_msg_cnt_id = U->user_id;
  }

  s = 0;
  for (i = 0; i <= k; i++) {
    tmp = Messages[i];
    if (LONG_LEGACY_IDS) {
      *(long long *)(legacy_id_index + i*3) = tmp->legacy_id;
      legacy_id_index[i*3 + 2] = tmp->local_id;
    } else {
      legacy_id_index[i*2] = tmp->legacy_id;
      legacy_id_index[i*2 + 1] = tmp->local_id;
    }
    if (!((tmp->flags ^ PeerFlagFilter.xor_mask) & PeerFlagFilter.and_mask)) {
      peer_id_index[s++] = tmp->peer_id;
      peer_id_index[s++] = tmp->local_id;
    }
  }

  peer_lists_size = (s >> 1);

  if (LONG_LEGACY_IDS) {
    llpair_sort ((llpair_t *) legacy_id_index, k);
  } else {
    pair_sort ((pair_t *) legacy_id_index, k);
  }
  pair_sort ((pair_t *) peer_id_index, peer_lists_size - 1);

  user_search_index = 0;
  if (search_enabled && cur_msg_cnt) {
    user_search_index_size = build_user_search_index (k+1, Messages[0]->local_id, U->last_local_id);
  } else {
    user_search_index_size = 0;
  }

  UserHdr.magic = FILE_USER_MAGIC;
  UserHdr.user_last_local_id = UL->user_last_local_id = U->last_local_id;
  UserHdr.user_first_local_id = cur_msg_cnt ? Messages[0]->local_id : U->last_local_id + 1;
  UserHdr.user_id = UL->user_id;
  UL->user_data_offset = write_pos;

  if (search_enabled || history_enabled) {
    ((struct file_user_list_entry_search *)UL)->user_search_size = user_search_index_size;
  }
  if (history_enabled) {
    ((struct file_user_list_entry_search_history *)UL)->user_history_min_ts = 1;
    ((struct file_user_list_entry_search_history *)UL)->user_history_max_ts = history_size;
  }

  UserHdr.sublists_offset = UserHdr.peers_offset = sizeof (struct file_user_header);

  int peer_c = (peer_lists_size <= 0) ? 0 : 1;

  peer_directory[0] = 0;
  t = peer_id_index[0];
  peer_directory[1] = t;
  peer_id_index[0] = peer_id_index[1];
  for (i = 1; i < peer_lists_size; i++) {
    if (peer_id_index[i*2] != t) {
      peer_directory[peer_c*2+1] = t = peer_id_index[i*2];
      peer_directory[peer_c*2] = i;
      ++peer_c;
    }
    peer_id_index[i] = peer_id_index[2*i+1];
  }
  peer_directory[peer_c*2] = i;

  t = UserHdr.peers_offset + peer_c * 8 + 4;
  for (i = 0; i <= peer_c; i++) {
    peer_directory[i*2] = peer_directory[i*2] * 4 + t;
  }

  UserHdr.peers_num = peer_c;
  if (peer_c > 0) {
    UserHdr.sublists_offset += peer_c * 8 + 4 + peer_lists_size * 4;
  }

  memset (cur_sublist_size, 0, 4 * sublists_num);
  cur_messages_bytes = 0;
  message_start_offset[0] = 0;

  s = 0;
  for (i = 0; i <= k; i++) {
    t = Messages[i]->len + 1;
    if (t >= 255) {
      t += (t >= 65537 ? 4 : 2);
    }
    if (Messages[i]->peer_msg_id && (Sublists != Messages_Sublists || (Messages[i]->flags & TXF_UNREAD))) {
      t += 4;
      Messages[i]->flags |= TXF_HAS_PEER_MSGID;
    }
    if (Messages[i]->legacy_id && preserve_legacy_ids) {
      if (!FITS_AN_INT (Messages[i]->legacy_id)) {
	assert (LONG_LEGACY_IDS);
	t += 8;
	Messages[i]->flags |= TXF_HAS_LONG_LEGACY_ID;
      } else {
	t += 4;
	Messages[i]->flags |= TXF_HAS_LEGACY_ID;
      }
    }
    for (j = 0; j < extra_ints_num; j++) {
      if (Messages[i]->extra[j]) {
        Messages[i]->flags |= (1 << (16 + convert_unpacked_extra_field_num (ES[j])));
      }
    }

    t += 4 * extra_mask_intcount ((Messages[i]->flags >> 16) & MAX_EXTRA_MASK);

    t += sizeof (struct file_message) + 3;
    t &= -4;

    j = Messages[i]->local_id - UserHdr.user_first_local_id;
    assert (j >= s && j < MAX_USER_MESSAGES);

    while (s < j) {
      message_start_offset[++s] = cur_messages_bytes;
    }
    cur_messages_bytes += t;
    message_start_offset[++s] = cur_messages_bytes;

    t = Messages[i]->flags;
    for (j = 0; j < sublists_num; j++) {
      if (!((t ^ Sublists[j].xor_mask) & Sublists[j].and_mask)) {
        cur_sublist_size[j]++;
      }
    }
  }

  tot_sublists_size = 0;
  for (j = 0; j < sublists_num; j++) {
    sublist_directory[j*2+1] = sublist_directory[j*2] = tot_sublists_size;
    tot_sublists_size += UL->user_sublists_size[j + ul_sublists_offset] = cur_sublist_size[j];
  }
  sublist_directory[j*2] = tot_sublists_size;

  sublists = zmalloc (tot_sublists_size * 4);

  for (i = 0; i <= k; i++) {
    t = Messages[i]->flags;
    for (j = 0; j < sublists_num; j++) {
      if (!((t ^ Sublists[j].xor_mask) & Sublists[j].and_mask)) {
        sublists[sublist_directory[2*j+1]++] = Messages[i]->local_id;
      }
    }
  }

  for (j = 0; j < sublists_num; j++) {
    assert (sublist_directory[2*j+1] == sublist_directory[2*j+2]);
    sublist_directory[2*j+1] = Sublists_packed[j].combined_xor_and;
  }

  UserHdr.legacy_list_offset = UserHdr.sublists_offset;
  UserHdr.sublists_num = sublists_num;
  if (sublists_num > 0) {
    UserHdr.legacy_list_offset += sublists_num * 8 + 4 + tot_sublists_size * 4;
  }

  UserHdr.directory_offset = UserHdr.legacy_list_offset;
  if (preserve_legacy_ids) {
    UserHdr.directory_offset += (LONG_LEGACY_IDS ? 12 : 8) * cur_msg_cnt;
  }

  UserHdr.data_offset = UserHdr.directory_offset + 4 * (UserHdr.user_last_local_id - UserHdr.user_first_local_id + 2);
  assert (UserHdr.data_offset >= 0);
  UL->user_data_size = UserHdr.data_offset + cur_messages_bytes;
  UserHdr.extra_offset = UL->user_data_size + 4;
  assert (UserHdr.extra_offset >= 0);
  UserHdr.total_bytes = UserHdr.extra_offset + (UserHdr.user_last_local_id - UserHdr.user_first_local_id + 1) * sizeof (struct file_message_extras) + 4 + 
    (user_search_index_size ? user_search_index_size + 4 : 0) +
    (history ? history_size * 8 + 16 : 0);
  assert (UserHdr.total_bytes >= 0);

  if (UserHdr.total_bytes > max_metafile_size) {
    max_metafile_size = UserHdr.total_bytes;
    max_metafile_id = U->user_id;
  }

  if (user_search_index_size > max_search_metafile_size) {
    max_search_metafile_size = user_search_index_size;
    max_search_metafile_id = U->user_id;
  }

  if (history && history_size * 8 + 16 > max_history_metafile_size) {
    max_history_metafile_size = history_size * 8 + 16;
    max_history_metafile_id = U->user_id;
  }


  if (verbosity > 2 && UL->user_id < 100000) {
    fprintf (stderr, "user id = %lld, last local id %d, messages %d, unread messages %d, distinct peers %d, sublists %d, search index size %ld\n", 
	     UL->user_id, UserHdr.user_last_local_id, cur_msg_cnt, cur_unread_cnt, UserHdr.peers_num, UserHdr.sublists_num, user_search_index_size);
    fprintf (stderr, "offsets: peers=%d, sublists=%d, legacy_list=%d, directory=%d, data=%d, extra=%d, total=%d\n",
	    UserHdr.peers_offset, UserHdr.sublists_offset, UserHdr.legacy_list_offset,
	    UserHdr.directory_offset, UserHdr.data_offset, UserHdr.extra_offset, UserHdr.total_bytes);
    verbosity--;
  }

  metafile_pos = 0;
  initcrc ();

  writeout (&UserHdr, sizeof (struct file_user_header));
  assert (metafile_pos == UserHdr.peers_offset);

  if (peer_c > 0) {
    writeout (peer_directory, peer_c * 8 + 4);
    writeout (peer_id_index, peer_lists_size * 4);
  }
  assert (metafile_pos == UserHdr.sublists_offset);

  if (sublists_num > 0) {
    writeout (sublist_directory, sublists_num * 8 + 4);
    writeout (sublists, tot_sublists_size * 4);
  }
  assert (metafile_pos == UserHdr.legacy_list_offset);

  if (preserve_legacy_ids) {
    writeout (legacy_id_index, cur_msg_cnt * (LONG_LEGACY_IDS ? 12 : 8));
  }
  assert (metafile_pos == UserHdr.directory_offset);

  t = UserHdr.data_offset;
  for (i = 0; i <= s; i++) {
    message_start_offset[i] += t;
  }
  writeout (message_start_offset, (s + 1) * 4);
  assert (metafile_pos == UserHdr.data_offset);

  for (i = 0; i <= k; i++) {
    static int msghdr[8];
    static char msglen[12];

    tmp = Messages[i];
    j = tmp->local_id - UserHdr.user_first_local_id;
    assert (metafile_pos == message_start_offset[j]);

    msghdr[0] = tmp->flags;
    msghdr[1] = tmp->peer_id;
    msghdr[2] = tmp->date;
    t = 3;

    if (tmp->flags & TXF_HAS_LEGACY_ID) {
      msghdr[t++] = tmp->legacy_id;
    }
    if (tmp->flags & TXF_HAS_LONG_LEGACY_ID) {
      *(long long *)(msghdr + t) = tmp->legacy_id;
      t += 2;
      assert (!(tmp->flags & TXF_HAS_LEGACY_ID));
    }
    if (tmp->flags & TXF_HAS_PEER_MSGID) {
      msghdr[t++] = tmp->peer_msg_id;
    }
    fl_unpacked = unpack_extra_mask ((tmp->flags >> 16) & MAX_EXTRA_MASK);
    for (j = 0; j < MAX_EXTRA_INTS; j++, fl_unpacked >>= 1) {
      if (fl_unpacked & 1) {
        assert (SE[j] >= 0);
        msghdr[t++] = tmp->extra[SE[j]];
      }
    }

    writeout (msghdr, t * 4);

    t = tmp->len;
    if (t < 254) {
      msglen[0] = t;
      t = 1;
    } else if (t < 65536) {
      msglen[0] = 254;
      *((short *) (msglen + 1)) = t;
      t = 3;
    } else {
      msglen[0] = 255;
      *((int *) (msglen + 1)) = t;
      t = 5;
    }

    writeout (msglen, t);
    writeout (tmp->text + text_shift, tmp->len);

    t += tmp->len;
    writeout (msglen + 8, -t & 3);
  }

  assert (k < 0 || metafile_pos == message_start_offset[UserHdr.user_last_local_id - UserHdr.user_first_local_id + 1]);

  writecrc ();
  assert (metafile_pos == UserHdr.extra_offset);

  if (user_search_index_size) {
    initcrc ();
    writeout (user_search_index, user_search_index_size);
    writecrc ();
  }

  if (history) {
    initcrc ();
    user_history_header.magic = FILE_USER_HISTORY_MAGIC;
    user_history_header.history_min_ts = 1;
    user_history_header.history_max_ts = history_size;
    writeout (&user_history_header, 12);
    writeout (history, history_size * 8);
    writecrc ();
    free (history);
    sum_history_size += history_size * 8 + 16;
  }

  initcrc ();
  s = -1;
  for (i = 0; i <= k; i++) {
    static struct file_message_extras zero_extra_data, msg_extra_data;
    tmp = Messages[i];
    j = tmp->local_id - UserHdr.user_first_local_id;
    assert (j > s);
    while (++s < j) {
      writeout (&zero_extra_data, sizeof (struct file_message_extras));
    }
    msg_extra_data.global_id = tmp->global_id;
    msg_extra_data.ip = tmp->ip;
    msg_extra_data.port = tmp->port;
    msg_extra_data.front = tmp->front;
    msg_extra_data.ua_hash = tmp->ua_hash;
    writeout (&msg_extra_data, sizeof (struct file_message_extras));
  }

  writecrc();
  assert (metafile_pos == UserHdr.total_bytes);

  sum_peers_offset += UserHdr.peers_offset;
  sum_sublists_offset += UserHdr.sublists_offset;
  sum_legacy_list_offset += UserHdr.legacy_list_offset;
  sum_directory_offset += UserHdr.directory_offset;
  sum_data_offset += UserHdr.data_offset;
  sum_extra_offset += UserHdr.extra_offset;
  sum_search_index_size += user_search_index_size;
  sum_total_bytes += UserHdr.total_bytes;
}

void process_loaded_messages (void) {
  int i;
  char *keep_dyn_cur;

  double tm = get_utime (CLOCK_MONOTONIC);

  for (i = cur_min_uid; i < cur_max_uid; i++) {
    if (User[i]) {
      assert (processed_users < tot_users);
      assert (UserDirectory[processed_users]->user_id == unconv_uid (i));
      keep_dyn_cur = dyn_cur;
      process_user_messages (UserDirectory[processed_users], User[i]);
      dyn_cur = keep_dyn_cur;
      processed_users++;
    }
  }

  last_process_time = get_utime (CLOCK_MONOTONIC) - tm;
}

/*
 *
 *  BUILD USER SEARCH INDEX
 *
 */


// taken from text-data.c
static inline word_t *word_code_lookup (struct word_dictionary *D, unsigned code, int *l) {
  int a = -1, b = D->max_bits, c;
  while (b - a > 1) {
    c = ((a + b) >> 1);
    if (D->first_codes[c] <= code) { a = c; } else { b = c; }
  }
  assert (a >= 0);
  *l = a + 1;
  return D->code_ptr[a][code >> (31 - a)];
}

static inline int char_code_lookup (struct char_dictionary *D, unsigned code, int *l) {
  int a = -1, b = D->max_bits, c;
  while (b - a > 1) {
    c = ((a + b) >> 1);
    if (D->first_codes[c] <= code) { a = c; } else { b = c; }
  }
  assert (a >= 0);
  *l = a + 1;
  return D->code_ptr[a][code >> (31 - a)];
}


// adapted from text-data.c
int unpack_message (char *buff, char *from, int packed_len) {
  char *wptr = buff, *rptr = from;
  int bits = packed_len * 8 - 1;
  int k = from[packed_len-1];

  assert (k);
  while (!(k & 1)) { k >>= 1; bits--; }

  unsigned t = bswap_32 (*((unsigned *) rptr));
  unsigned z = (((unsigned char) rptr[4]) << 1) + 1;
  rptr += 5;

#define	LOAD_BITS(__n)	{ int n=(__n); bits-=n; do {if (!(z&0xff)) {z=((*rptr++)<<1)+1;} t<<=1; if (z&0x100) t++; z<<=1;} while (--n);}

  while (bits > 0) {
    word_t *W;
    int c, N, l;

    /* decode one word */
    W = word_code_lookup (&WordDict, t, &l);
    LOAD_BITS (l);
    if (W->len != 2 || W->str[0]) {
      char *p = W->str, *q = p + W->len;
      while (p < q) {
        *wptr++ = *p++;
      }
    } else {
      /* special case: word not in dictionary, decode N word characters */
      N = (unsigned char) W->str[1];
      assert (N > 0);
      do {
        c = char_code_lookup (&WordCharDict, t, &l);
        LOAD_BITS (l);
        *wptr++ = c;
      } while (--N);
    }

    if (bits <= 0) {
      break;
    }

    /* decode one not-word */
    W = word_code_lookup (&NotWordDict, t, &l);
    LOAD_BITS (l);
    if (W->len != 2 || W->str[0]) {
      char *p = W->str, *q = p + W->len;
      while (p < q) {
        *wptr++ = *p++;
      }
    } else {
      /* special case: not-word not in dictionary, decode N not-word characters */
      N = (unsigned char) W->str[1];
      assert (N > 0);
      do {
        c = char_code_lookup (&NotWordCharDict, t, &l);
        LOAD_BITS (l);
        *wptr++ = c;
      } while (--N);
    }
  }
  assert (!bits && wptr < buff + MAX_TEXT_LEN);

#undef LOAD_BITS

  *wptr = 0;

  return wptr - buff;
}


#pragma pack(push,4)
struct search_index_pair {
  int idx;
  unsigned long long crc;
};
#pragma pack(pop)

static void build_message_search_pairs (message_t *msg) {
  assert (dyn_top >= dyn_cur + MAX_TEXT_LEN);

  int msg_len = unpack_message (dyn_cur, msg->text + text_shift, msg->len);
  assert (msg_len >= 0 && msg_len < MAX_TEXT_LEN);
  dyn_cur[msg_len] = 0;

  //fprintf (stderr, "message, local_id=%d: len=%d '%s'\n", msg->local_id, msg_len, dyn_cur);

  int cnt = compute_message_distinct_words (dyn_cur, msg_len);

  dyn_top -= cnt * sizeof (struct search_index_pair);
  assert (dyn_cur <= dyn_top);

  struct search_index_pair *ptr = (struct search_index_pair *)dyn_top;

  int i;
  for (i = 0; i < cnt; i++, ptr++) {
    ptr->crc = WordCRC[i];
    ptr->idx = msg->local_id;
  }
}

static inline int sip_cmp (struct search_index_pair *a, struct search_index_pair *b) {
  if (a->crc < b->crc) {
    return -1;
  } else if (a->crc > b->crc) {
    return 1;
  } else if (a->idx < b->idx) {
    return -1;
  } else if (a->idx > b->idx) {
    return 1;
  }
  return 0;
}

void sip_sort (struct search_index_pair *A, int b) {
  int i = 0, j = b;
  struct search_index_pair h, t;
  if (b <= 0) { return; }
  h = A[b >> 1];
  do {
    while (sip_cmp (&A[i], &h) < 0) { i++; }
    while (sip_cmp (&A[j], &h) > 0) { j--; }
    if (i <= j) {
      t = A[i];  A[i++] = A[j];  A[j--] = t;
    }
  } while (i <= j);
  sip_sort (A+i, b-i);
  sip_sort (A, j);
}

static void convert_words_list (unsigned long long *A, int N, int q) {
  if (N <= 0) {
    return;
  }
  int k = (N - 1) >> 1;
  unsigned long long u = ((q & 1) && !k ? -1LL : A[k] ^ A[k-1]);
  unsigned long long v = ((q & 2) && N == 1 ? -1LL : A[k] ^ A[k+1]);
  int need_bits = __builtin_clzll (u < v ? u : v) + 1;
  int skip_bits = __builtin_clzll (A[-1] ^ A[N]);

//  if (verbosity > 3) {
//    fprintf (stderr, "k=%d N=%d A[-1]=%016llx A[k-1]=%016llx A[k]=%016llx A[k+1]=%016llx A[N]=%016llx need=%d skip=%d\n",
//      k, N, A[-1], A[k-1], A[k], A[k+1], A[N], need_bits, skip_bits);
//  }

  convert_words_list (A, k, q & 1);
  convert_words_list (A + k + 1, N - k - 1, q & 2);

  assert (need_bits >= skip_bits && need_bits <= skip_bits + 58);

  A[k] &= (-1LL << (64 - need_bits));
  A[k] <<= skip_bits;
  A[k] |= need_bits - skip_bits;
}

static long build_user_search_index (int tot_msgs, int first_local_id, int last_local_id) {
  char *keep_dyn_top = dyn_top;
  int i;

  for (i = 0; i < tot_msgs; i++) {
    build_message_search_pairs (Messages[i]);
  }

  assert (dyn_top >= dyn_cur);

  int pc = (keep_dyn_top - dyn_top) / sizeof (struct search_index_pair);

  if (!pc) {
    return 0;
  }

  sip_sort ((struct search_index_pair *)dyn_top, pc - 1);

  int wcnt = 1;
  unsigned long long *words_list = WordCRC + 1;
  struct search_index_pair *ptr = (struct search_index_pair *) dyn_top;

  words_list[-1] = 0;
  words_list[0] = ptr[0].crc;
//  if (verbosity > 3) {
//    fprintf (stderr, "word[0] = %016llx\n", words_list[0]);
//  }

  for (i = 1; i < pc; i++, ptr++) {
    if (ptr[1].crc != ptr[0].crc) {
      if (wcnt < sizeof (WordCRC) / 8 - 2) {
        words_list[wcnt] = ptr[1].crc;
//        if (verbosity > 3) {
//          fprintf (stderr, "word[%d] = %016llx\n", wcnt, words_list[wcnt]);
//        }
      }
      wcnt++;
    }
  }

  if (wcnt > sizeof (WordCRC) / 8 - 2) {
    words_list = malloc (8 * wcnt + 16);
    assert (words_list);

    ptr = (struct search_index_pair *) dyn_top;

    *words_list++ = 0;
    words_list[0] = ptr[0].crc;
    wcnt = 1;

    for (i = 1; i < pc; i++, ptr++) {
      if (ptr[1].crc != ptr[0].crc) {
        words_list[wcnt++] = ptr[1].crc;
      }
    }
  }

  words_list[wcnt] = -1LL;

  user_search_index = (struct file_search_header *)dyn_cur;
  dyn_cur += sizeof (struct file_search_header) + wcnt * 4;

  assert (dyn_cur <= dyn_top);

  ptr = (struct search_index_pair *)dyn_top;

  convert_words_list (words_list, wcnt, 3);

  int j = 0, w = 0;

  for (j = 0, w = 0; j < pc; w++) {
//    if (verbosity > 3) {
//      fprintf (stderr, "adj_words[%d] = %016llx\n", w, words_list[w]);
//    }
    user_search_index->word_start[w] = dyn_cur - (char *)user_search_index;
    i = j++;
    unsigned long long word = ptr[i].crc;
    while (j < pc && ptr[j].crc == word) {
      ++j;
    }
    int s = words_list[w] & 63;
    assert (s <= 58);

    int k;
    int *pi = (int *)(ptr + j);

    *--pi = last_local_id + 1;


    for (k = j - 1; k >= i; k--) {
      *--pi = ptr[k].idx;
    }

    *--pi = first_local_id - 1;

    assert (pi[0] < pi[1]);
    assert (pi[j-i] < pi[j-i+1]);

    static struct bitwriter bw;

    bw.m = 2;
    bw.ptr = (unsigned char *) dyn_cur;
    bw.end_ptr = (unsigned char *) pi - 4;
    assert (bw.ptr < bw.end_ptr);
    *bw.ptr = s << 2;

    bwrite_nbitsull (&bw, words_list[w] >> (64 - s), s);

    int t = 31 - __builtin_clz (j - i);

    bwrite_nbits (&bw, -2, t + 1);
    bwrite_nbits (&bw, j - i, t);

    bwrite_interpolative_sublist (&bw, pi, 0, j - i + 1);

    if (bw.m != 0x80) { 
      bw.ptr++; 
    }

    assert (bw.ptr <= bw.end_ptr);

/*
    if (verbosity > 3) {
      unsigned char *dc = (unsigned char *) dyn_cur;
      int skip = 6 + s + 1 + 2*t, q;
      fprintf (stderr, "packed list #%d, len=%d from %d: (%d) %d %d %d... %d (%d)\nresult: (%ld bytes, skip %d bits) %02x %02x %02x %02x %02x %02x %02x %02x\n", 
               w, j - i, last_local_id - first_local_id + 1, pi[0], pi[1], pi[2], pi[3], pi[j-i], pi[j-i+1], enc.ptr-dc, skip,
               dc[0], dc[1], dc[2], dc[3], dc[4], dc[5], dc[6], dc[7]);
      dyn_cur = enc.ptr + 4;
      dyn_cur += (-(long) dyn_cur) & 7;

      struct list_decoder *dec = zmalloc_list_decoder (last_local_id - first_local_id + 1, j - i, dc + (skip >> 3), le_interpolative, skip & 7);
      fprintf (stderr, "decoding (N=%d, K=%d): ", dec->N, dec->K);
      for (q = 0; q < j - i; q++) {
        int u = dec->decode_int (dec) + first_local_id;
        if (q < 20) {
          fprintf (stderr, "%d ", u);
        }
        assert (u == pi[q+1]);
      }
      fprintf (stderr, "... (%ld bytes decoded)\n", dec->ptr - dc);
      dyn_cur = dc;
    }
*/

    if ((char *) bw.ptr <= dyn_cur + 4) {
      *((int *) bw.ptr) = 0;
      user_search_index->word_start[w] = bswap_32 (*((int *) dyn_cur)) | (-1 << 31);
    } else {
      dyn_cur = (char *) bw.ptr;
    }
  }

  assert (w == wcnt);

  dyn_top = keep_dyn_top;

  if (wcnt > sizeof (WordCRC) / 8 - 2) {
    free (words_list - 1);
  }

  user_search_index->magic = FILE_USER_SEARCH_MAGIC;
  user_search_index->words_num = w;

  int z = (-(long)dyn_cur) & 3;

  for (i = 0; i < z; i++) {
    *dyn_cur++ = 0;
  }

  int ret = dyn_cur - (char *)user_search_index;

//  dyn_cur += (-(long)dyn_cur) & 7;
  assert (dyn_cur <= dyn_top);
  
  return ret;
}



/*
 *
 *  WRITE INDEX
 *
 */

int hapax_legomena;
int tot_items;

void write_header0 (void) {
  Header.magic = -1;
  Header.log_pos1 = log_pos;
  Header.dictionary_log_pos = log_pos;
  Header.log_timestamp = log_read_until;
  write_pos = 0;
  writeout (&Header, sizeof(Header));
}

void write_sublists_descr (void) {
  Header.sublists_descr_offset = write_pos;
  Header.sublists_num = sublists_num;
  initcrc ();
  writeout (Sublists_packed, 4 * sublists_num);
  writecrc ();
}

static inline void write_freq_int_array (long long freq[256]) {
  static int i_freq[256];
  int i;
  for (i = 0; i < 256; i++) {
    i_freq[i] = (freq[i] > 0x7fffffff ? 0x7fffffff : freq[i]);
  }
  writeout (i_freq, 1024);
}

void write_char_dictionary (struct char_dictionary *Dict) {
  static int dict_size = 256;
  initcrc ();
  writeout (&dict_size, 4);
  writeout (Dict->code_len, 256);
  writecrc ();
  write_freq_int_array (Dict->freq);
  writecrc ();
}

int write_word_dictionary_word (word_t *W) {
  static char buff[8];
  int sz, i = 1;
  assert (W->code_len > 0 && W->code_len <= 32 && W->len < 256);
  buff[0] = W->code_len;
  buff[1] = W->len;
  writeout (buff, 2);
  writeout (W->str, W->len);
  sz = W->len + 2;
  i = -sz & 3;
  writeout (buff+2, i);
  return sz + i;
}

void write_word_dictionary (word_t *List[], int N) {
  int *dict_header, cur_offset = 4*N+8, i;
  long long keep_pos = write_pos;
  unsigned keep_crc;
  dyn_mark (0);
  dict_header = zmalloc (cur_offset);
  *dict_header++ = N;
  write_seek (keep_pos + cur_offset);
  initcrc ();
  for (i = 0; i < N; i++) {
    dict_header[i] = cur_offset;
    cur_offset += write_word_dictionary_word (List[i]);
  }
  dict_header[N] = cur_offset;
  keep_crc = crc32_acc;
  initcrc ();
  write_seek (keep_pos);
  writeout (dict_header - 1, 4*N+8);
  crc32_acc = ~compute_crc32_combine (~crc32_acc, ~keep_crc, cur_offset - 4*N - 8);
  write_seek (keep_pos + cur_offset);
  for (i = 0; i < N; i++) {
    writeout (&List[i]->freq, 4);
  }
  writecrc ();
  flushout ();
  dyn_release (0);
}

void write_dictionaries (void) {
  Header.word_char_dictionary_offset = write_pos;
  write_char_dictionary (&WordCharDict);
  Header.notword_char_dictionary_offset = write_pos;
  write_char_dictionary (&NotWordCharDict);
  Header.word_dictionary_offset = write_pos;
  write_word_dictionary (WordList, word_codes_cnt);
  Header.notword_dictionary_offset = write_pos;
  write_word_dictionary (NotWordList, notword_codes_cnt);
  Header.user_list_offset = write_pos;
}

void write_header1 (void) {
  write_seek (0);
  Header.magic = history_enabled ? TEXT_INDEX_CRC_SEARCH_HISTORY_MAGIC :
    (search_enabled ? TEXT_INDEX_CRC_SEARCH_MAGIC : TEXT_INDEX_CRC_MAGIC);
  Header.created_at = time(0);
  Header.log_split_min = log_split_min;
  Header.log_split_max = log_split_max;
  Header.log_split_mod = log_split_mod;
  assert (max_legacy_id >= 0 && min_legacy_id <= 0);
  Header.max_legacy_id = max_legacy_id <=  0x7fffffffLL ? max_legacy_id : -0x80000000;
  Header.min_legacy_id = min_legacy_id >= -0x80000000LL ? min_legacy_id :  0x7fffffff;
  Header.peer_list_mask.xor_mask = PeerFlagFilter.xor_mask;
  Header.peer_list_mask.and_mask = PeerFlagFilter.and_mask;
  Header.last_global_id = last_global_id;
  Header.log_pos1_crc32 = log_limit_crc32;
  Header.extra_fields_mask = final_extra_mask;
  Header.search_coding_used = search_enabled ? searchtags_enabled * 16 + word_split_utf8 * 8 + hashtags_enabled * 4 + use_stemmer + 2 : 0;
  Header.header_crc32 = compute_crc32 (&Header, offsetof (struct text_index_header, header_crc32));
  writeout (&Header, sizeof(Header));
  flushout ();
}

/*
 *
 *		STATISTICS & DEBUG OUTPUT
 *
 */

int percent (long long a, long long b) {
  if (b <= 0 || a <= 0) return 0;
  if (a >= b) return 100;
  return (a*100 / b);
}

void output_stats (void) {
  int t;
  fprintf (stderr, "%lld messages read, %lld bytes, average length %.2f\n", 
	   msgs_read, msgs_bytes, (double) msgs_bytes / msgs_read);
  fprintf (stderr, "%d distinct users, maximal user id = %d\n", 
	   tot_users, max_uid);
  fprintf (stderr, "%lld word instances, %lld non-word instances\n",
	   word_instances, nonword_instances);
  fprintf (stderr, "%d distinct words, total length %d, average length %.2f\n",
	   Words.cnt, Words.tot_len, (double) Words.tot_len / Words.cnt);
  fprintf (stderr, "%d distinct not-words, total length %d, average length %.2f\n",
	   NotWords.cnt, NotWords.tot_len, (double) NotWords.tot_len / NotWords.cnt);
  fprintf (stderr, "hapax legomena: %lld words (%d%%), %lld not-words (%d%%)\n",
	   hapax_words, percent(hapax_words, Words.cnt), hapax_notwords, percent(hapax_notwords, NotWords.cnt));
  fprintf (stderr, "rare (not)words (freq <= %d): %lld words (%d%%), %lld not-words (%d%%)\n",
	   max_nd_freq, rare_words, percent(rare_words, Words.cnt), rare_notwords, percent(rare_notwords, NotWords.cnt));
  fprintf (stderr, "%lld word codes, %lld not-word codes\n", word_codes_cnt, notword_codes_cnt);
  fprintf (stderr, "bits: %lld for words, %lld for word chars, %lld for not-words, %lld for not-word chars\n",
	   total_word_bits, total_word_char_bits, total_notword_bits, total_notword_char_bits);
  fprintf (stderr, "estimated packed size: %lld, %.3f per message; unpacked size: %lld, %.3f per message\n",
	   total_packed_bytes, (double) total_packed_bytes / msgs_read, msgs_bytes, (double) msgs_bytes / msgs_read);
  if (factual_messages) {
    fprintf (stderr, "factual packed text size: %lld, %.3f per message; unpacked text size: %lld, %.3f per message, %lld messages, %lld out of them replaced\n",
	     factual_packed_bytes, (double) factual_packed_bytes / factual_messages, factual_unpacked_bytes, (double) factual_unpacked_bytes / factual_messages, factual_messages, replaced_messages);
  }
  t = word_codes_cnt + notword_codes_cnt;
  fprintf (stderr, "estimated dictionary size: %d entries * 5 + %d characters = %d\n",
	   t, dict_size, t*5+dict_size);
  if (Header.user_list_offset) {
    fprintf (stderr, "factual dictionary size: %lld bytes\n", Header.user_list_offset - sizeof (Header));
  }
  if (max_user_msg_cnt > 0) {
    fprintf (stderr, "maximal user messages: %d for user %d\n", max_user_msg_cnt, max_user_msg_cnt_id);
  }
  if (max_metafile_size > 0) {
    fprintf (stderr, "maximal metafile size: %d for user %d\n", max_metafile_size, max_metafile_id);
  }
  if (max_search_metafile_size > 0) {
    fprintf (stderr, "maximal search sub-metafile size: %d for user %d\n", max_search_metafile_size, max_search_metafile_id);
  }
  if (max_history_metafile_size > 0) {
    fprintf (stderr, "maximal history sub-metafile size: %d for user %d\n", max_history_metafile_size, max_history_metafile_id);
  }
  if (tot_search_words > 0) {
    fprintf (stderr, "total amount of word-message pairs: %lld, %.3f per user; maximal %lld for user %lld\n",
	   tot_search_words, (double) tot_search_words / tot_users, max_user_search_words, max_user_search_id);
  }
  if (Header.data_end_offset > 0) {
    fprintf (stderr, "index offsets: user_list=%lld user_data=%lld extra_data=%lld end=%lld\n",
 	Header.user_list_offset, Header.user_data_offset, Header.extra_data_offset, Header.data_end_offset);
  }
  if (sum_total_bytes > 0) {
    fprintf (stderr, "total user data metafile space distribution: "
	"total %lld, headers %lld (%d%%), peers %lld (%d%%), "
	"sublists %lld (%d%%), legacy list %lld (%d%%), msg directory %lld (%d%%), "
	"messages %lld (%d%%), search %lld (%d%%), history %lld (%d%%), extra %lld (%d%%)\n",
	sum_total_bytes, sum_peers_offset, percent (sum_peers_offset, sum_total_bytes),
	sum_sublists_offset - sum_peers_offset, percent (sum_sublists_offset - sum_peers_offset, sum_total_bytes),
	sum_legacy_list_offset - sum_sublists_offset, percent (sum_legacy_list_offset - sum_sublists_offset, sum_total_bytes),
	sum_directory_offset - sum_legacy_list_offset, percent (sum_directory_offset - sum_legacy_list_offset, sum_total_bytes),
	sum_data_offset - sum_directory_offset, percent (sum_data_offset - sum_directory_offset, sum_total_bytes),
	sum_extra_offset - sum_data_offset, percent (sum_extra_offset - sum_data_offset, sum_total_bytes),
	sum_search_index_size, percent (sum_search_index_size, sum_total_bytes),
	sum_history_size, percent (sum_history_size, sum_total_bytes),
	sum_total_bytes - sum_extra_offset - sum_search_index_size, percent (sum_total_bytes - sum_extra_offset - sum_search_index_size, sum_total_bytes)
    );	
  }
  fprintf (stderr, "last pass process user messages time: %.3fs\n", last_process_time);
  fprintf (stderr, "compress ratio: estimated %d%%, factual %d%%\n", percent (total_packed_bytes, msgs_bytes), percent (factual_packed_bytes, factual_unpacked_bytes));
  fprintf (stderr, "used memory: %ld bytes out of %ld\n", (long) (dyn_cur - dyn_first), (long) (dyn_last - dyn_first));
}


/*
 *
 *		MAIN
 *
 */

void usage (void) {
  printf ("usage: %s [-s] [-v] [-p] [-u<username>] [-i] [-S] [-y] [-a<binlog-name>] [-f<rare-word-freq>] <huge-index-file>\n"
  	  "\t" VERSION_STR " compiled at " __DATE__ " " __TIME__ " by gcc " __VERSION__ " "
#ifdef __LP64__
	  "64-bit"
#else
	  "32-bit"
#endif
	  " after commit " COMMIT "\n"
	  "\tBuilds a text fast access data file from given binlog file <huge-index-file>.bin or <binlog-name>.\n"
	  "\t-v\toutput statistical and debug information into stderr\n"
	  "\t-e\testimate compress ratio only\n"
	  "\t-s\tsort messages with zero timestamp by date\n"
	  "\t-p\tpreserve legacy message ids\n"
	  "\t-i\tbuild per-metafile search index\n"
	  "\t-y\tbuild per-metafile persistent history index\n"
	  "\t-U\tenable native UTF-8 mode\n"
          "\t-Z<heap-size>\tdefines maximum heap size\n"
	  "\t-S\tuse stemmer for search\n"
	  "\t-t\tenable hashtags\n"
	  "\t-q\tenable search tags (0x1f-prefixed)\n"
	  "\t-f<rare-word-freq>\tset rare word frequency (rare words removed from the dictionary)\n"
	  "\t-G\tignore delete_first_messages records\n"
	  "\t-T<tmp-dir>\tdecrease number of passes by creating temporary files in selected directory\n"
	  "\t-L<binlog-pos>\tgenerate snapshot for given binlog position\n"
	  "\t-m\tforce package-merge algorithm invocation\n",
	  progname);
  exit(2);
}

int main (int argc, char *argv[]) {
  int i;
  char c;
  long long x;

  set_debug_handlers();

  progname = argv[0];
  while ((i = getopt (argc, argv, "a:f:iStqspehu:mvyUGL:T:Z:")) != -1) {
    switch (i) {
    case 'e':
      passes = -1;
    case 'v':
      verbosity++;
      break;
    case 'h':
      usage ();
      return 2;
    case 'u':
      username = optarg;
      break;
    case 'a':
      binlogname = optarg;
      break;
    case 'i':
      search_enabled = 1;
      break;
    case 'y':
      history_enabled = 1;
      break;
    case 's':
      msg_date_sort = 1;
      break;
    case 'S':
      use_stemmer = 1;
      break;
    case 'U':
      word_split_utf8 = 1;
      break;
    case 't':
      hashtags_enabled = 1;
      break;
    case 'q':
      searchtags_enabled = 1;
      break;
    case 'G':
      ignore_delete_first_messages = 1;
      break;
    case 'p':
      preserve_legacy_ids = 1;
      break;
    case 'f':
      max_nd_freq = atoi (optarg);
      if (max_nd_freq < 0 || max_nd_freq > 100) {
        max_nd_freq = MAX_NONDICT_FREQ;
      }
      break;
    case 'm':
      force_pm = 1;
      break;
    case 'Z':
      c = 0;
      assert (sscanf (optarg, "%lld%c", &x, &c) >= 1);
      switch (c | 0x20) {
      case 'k':  x <<= 10; break;
      case 'm':  x <<= 20; break;
      case 'g':  x <<= 30; break;
      case 't':  x <<= 40; break;
      default: assert (c == 0x20);
      }
      if (x >= (1LL << 20) && x <= (sizeof(long) == 4 ? (3LL << 30) : (20LL << 30))) {
        dynamic_data_buffer_size = x;
      }
      break;
    case 'T':
      if (strlen (optarg) < 120) {
	temp_binlog_directory = optarg;
      }
      break;
    case 'L':
      log_limit_pos = atoll (optarg);
      break;
    }
  }
  if (argc != optind + 1 || (use_stemmer && !search_enabled)) {
    usage();
    return 2;
  }

  if (change_user (username) < 0) {
    fprintf (stderr, "fatal: cannot change user to %s\n", username ? username : "(none)");
    exit (1);
  }

  if (!search_enabled) {
    hashtags_enabled = searchtags_enabled = 0;
  }

  init_is_letter ();
  if (hashtags_enabled) {
    enable_is_letter_sigils ();
  }
  if (searchtags_enabled) {
    enable_search_tag_sigil ();
  }
  init_letter_freq ();
  if (use_stemmer) {
    stem_init ();
  }

  init_dyn_data();

  Messages = malloc (sizeof (struct message *) * MAX_USER_MESSAGES);
  peer_id_index = malloc (sizeof (int) * MAX_USER_MESSAGES * 2);
  legacy_id_index = malloc (sizeof (int) * MAX_USER_MESSAGES * 3);
  assert (Messages && peer_id_index && legacy_id_index);

  Words.A = malloc (PRIME * sizeof (void *));
  NotWords.A = malloc (PRIME * sizeof (void *));
  assert (Words.A && NotWords.A);

  if (engine_preload_filelist (argv[optind], binlogname) < 0) {
    fprintf (stderr, "cannot open binlog files for %s\n", binlogname ? binlogname : argv[optind]);
    exit (1);
  }

  Binlog = open_binlog (engine_replica, 0);
  if (!Binlog) {
    fprintf (stderr, "fatal: cannot find binlog for %s\n", engine_replica->replica_prefix);
    exit (1);
  }

  binlogname = Binlog->info->filename;

  if (verbosity) {
    fprintf (stderr, "replaying binlog file %s (size %lld)\n", binlogname, Binlog->info->file_size);
  }

  binlog_load_time = get_utime (CLOCK_MONOTONIC);

  clear_log();
  init_log_data (0, 0, 0);

  process_message = process_message0;
  adjust_message = adjust_message0;
  change_extra_mask = change_extra_mask0;
  delete_first_messages = delete_first_messages0;
  replace_message_text = replace_message_text0;

  i = replay_log (0, 1);

  binlog_load_time = get_utime (CLOCK_MONOTONIC) - binlog_load_time;
  binlog_loaded_size = log_readto_pos;

  close_binlog (Binlog, 1);

  assert (log_limit_pos < 0 || log_readto_pos == log_limit_pos);
  log_limit_pos = log_readto_pos;
  log_limit_crc32 = ~log_crc32_complement;
  last_global_id0 = last_global_id;

  if (i < 0) {
    fprintf (stderr, "fatal: error reading binlog\n");
    exit (1);
  }

  if (verbosity) {
      fprintf (stderr, "replay binlog file: done, pos=%lld, last_global_id=%d, alloc_mem=%ld out of %ld, time %.06lfs\n", 
	       (long long) log_pos, last_global_id, (long) (dyn_cur - dyn_first), (long) (dyn_last - dyn_first), binlog_load_time);
  }

  if (search_enabled && verbosity > 0) {
    test_word_hashes ();
  }

  prepare_extra_mask ();
  count_sublists ();
  build_model ();

  if (passes < 0) {
    output_stats ();
    return 0;
  }

  passes = estimate_needed_passes ();
  if (verbosity > 0) {
    fprintf (stderr, "%ld bytes available for indexing, will need %d passes\n", (long) (dyn_last - dyn_cur), passes);
  }

  if (engine_snapshot_replica) {
    assert (update_replica (engine_snapshot_replica, 1) > 0);
    newidxname = get_new_snapshot_name (engine_snapshot_replica, log_cur_pos(), engine_snapshot_replica->replica_prefix);
  }

  if (!newidxname || newidxname[0] == '-') {
    fprintf (stderr, "cannot write index: cannot compute its name\n");
    exit (1);
  }

  open_file (0, newidxname, 1);
  assert (lock_whole_file (fd[0], F_WRLCK) > 0);

  write_header0 ();
  write_sublists_descr ();
  write_dictionaries ();
  prepare_user_directory ();

  prepare_pass_splitting ();

  if (temp_binlog_directory) {
    if (verbosity > 0) {
      fprintf (stderr, "running splitting pass to create temporary files\n");
    }

    Binlog = open_binlog (engine_replica, 0);
    if (!Binlog) {
      fprintf (stderr, "fatal: cannot find binlog for %s\n", engine_replica->replica_prefix);
      exit (1);
    }

    binlogname = Binlog->info->filename;

    if (verbosity) {
      fprintf (stderr, "replaying binlog file %s (size %lld)\n", binlogname, Binlog->info->file_size);
    }

    dyn_mark (0);
    open_temp_files ();

    clear_log();
    init_log_data (0, 0, 0);
    replay_logevent = text_split_replay_logevent;
    last_global_id = 0;

    binlog_load_time = get_utime(CLOCK_MONOTONIC);

    i = replay_log (0, 1);

    binlog_load_time = get_utime(CLOCK_MONOTONIC) - binlog_load_time;
    replay_logevent = text_replay_logevent;

    if (i < 0) {
      fprintf (stderr, "fatal: error reading binlog\n");
      exit (1);
    }
    if (verbosity) {
        fprintf (stderr, "replay binlog file (pass %d): done, alloc_mem=%ld out of %ld, time %.06lfs\n", 
	         pass, (long) (dyn_cur - dyn_first), (long) (dyn_last - dyn_first), binlog_load_time);
    }

    close_binlog (Binlog, 1);
    Binlog = 0;

    if (last_global_id != last_global_id0) {
      fprintf (stderr, "last global id mismatch: original pass %d, current pass %d\n", last_global_id0, last_global_id);
    }
    assert (log_limit_crc32 == ~log_crc32_complement && last_global_id == last_global_id0);

    tmp_flush_all ();

    dyn_release (0);
  }

  for (pass = 1; pass <= passes; pass++) {
    struct buff_file *T = 0;
    cur_min_uid = pass_min_uid[pass - 1];
    cur_max_uid = pass_min_uid[pass];
    if (verbosity > 0) {
      output_stats ();
      fprintf (stderr, "starting pass #%d, uid %d..%d\n", pass, cur_min_uid, cur_max_uid);
    }

    if (!temp_binlog_directory) {
      Binlog = open_binlog (engine_replica, 0);
      if (!Binlog) {
	fprintf (stderr, "fatal: cannot find binlog for %s\n", engine_replica->replica_prefix);
	exit (1);
      }

      binlogname = Binlog->info->filename;

      if (verbosity) {
	fprintf (stderr, "replaying binlog file %s (size %lld)\n", binlogname, Binlog->info->file_size);
      }
      clear_log ();
      init_log_data (0, 0, 0);
    } else {
      T = &temp_file[pass - 1];
      clear_log ();
      binlog_zipped = 0;
      binlogname = T->filename;
      set_log_data (T->fd, T->wpos);
      log_seek (0, 0, 0);
      log_limit_pos = -1;
    }

    dyn_mark (0);

    last_global_id = 0;
    threshold_msg = (void *) -1;

    process_message = process_message1;
    adjust_message = adjust_message1;
    change_extra_mask = change_extra_mask1;
    delete_first_messages = delete_first_messages1;
    replace_message_text = replace_message_text1;

    binlog_load_time = get_utime(CLOCK_MONOTONIC);

    i = replay_log (0, 1);

    binlog_load_time = get_utime(CLOCK_MONOTONIC) - binlog_load_time;

    if (i < 0) {
      fprintf (stderr, "fatal: error reading binlog\n");
      close_temp_files (3);
      exit (1);
    }
    if (verbosity) {
        fprintf (stderr, "replay binlog file %s (pass %d): done, alloc_mem=%ld out of %ld, time %.06lfs\n", 
	         binlogname, pass, (long) (dyn_cur - dyn_first), (long) (dyn_last - dyn_first), binlog_load_time);
    }

    if (!temp_binlog_directory) {
      close_binlog (Binlog, 1);
      assert (log_limit_crc32 == ~log_crc32_complement);
    } else {
      assert (T->crc32 = ~log_crc32_complement);
    }

    process_loaded_messages ();

    dyn_release (0);

    if (last_global_id != last_global_id0) {
      fprintf (stderr, "last global id mismatch: original pass %d, current pass %d\n", last_global_id0, last_global_id);
    }
    assert (last_global_id == last_global_id0);

    /*
    if (cur_max_uid <= max_uid && pass == passes && extra_passes < 3) {
      fprintf (stderr, "WARNING: last pass #%d processed %d..%d, but max_uid=%d; scheduling extra pass\n", passes, cur_min_uid, cur_max_uid - 1, max_uid);
      passes++;
      extra_passes++;
    }
    */
  }

  if (temp_binlog_directory) {
    close_temp_files (3);
  }

  assert (cur_max_uid == max_uid + 1);

  write_user_directory ();
  write_header1 ();

  flushout ();

  assert (fsync(fd[0]) >= 0);
  close (fd[0]);

  if (rename_temporary_snapshot (newidxname)) {
    fprintf (stderr, "cannot rename new index file from %s: %m\n", newidxname);
    unlink (newidxname);
    exit (1);
  }

  if (verbosity > 0) {
    output_stats();
  }

  print_snapshot_name (newidxname);

  return 0;
}
