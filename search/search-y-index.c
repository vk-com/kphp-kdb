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

    Copyright 2013 Vkontakte Ltd
              2013 Anton Maydell
*/

#define        _FILE_OFFSET_BITS       64

#include <assert.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <limits.h>
#include <fcntl.h>
#include <sys/types.h>
#include <aio.h>
#include <sys/timerfd.h>
#include "kfs.h"
#include "md5.h"
#include "server-functions.h"
#include "kdb-search-binlog.h"
#include "kdb-data-common.h"
#include "word-split.h"
#include "stemmer-new.h"
#include "crc32.h"
#include "search-index-layout.h"
#include "search-y-parse.h"
#include "utils.h"
#include "listcomp.h"

#ifndef COMMIT
#define COMMIT "unknown"
#endif

#define	VERSION_STR	"search-y-index"
const char FullVersionStr[] = VERSION_STR " compiled at " __DATE__ " " __TIME__ " by gcc " __VERSION__ " "
#ifdef __LP64__
  "64-bit"
#else
  "32-bit"
#endif
" after commit " COMMIT;

#define MAX_PAIRS 100000000

int now;
char *fnames[3];
int fd[3];
long long fsize[3];

long long deleted_text_bytes = 0;
int max_pairs = 100000000;
int lss_threshold = 64;

/* file utils */
int open_file (int x, char *fname, int creat) {
  fnames[x] = fname;
  fd[x] = open (fname, creat > 0 ? O_RDWR | O_CREAT | O_TRUNC : O_RDONLY, 0600);
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
 * read binlog
 */

#ifdef	_LP64
#define	MAX_ITEMS	50000000
#define	ITEMS_HASH_PRIME	60888739
#else
#define	MAX_ITEMS	(1 << 24)
#define	ITEMS_HASH_PRIME	24000001
#endif

#define FLAG_DELETED 1

int log_split_min, log_split_max, log_split_mod;

struct item {
  long long item_id;
  int len;
  unsigned short mask;
  char rates_len;
  char extra;
  int *rates;
  char *str;
};

static int tot_terms, tot_tags;
static int tot_items;
static int del_items; /* number deleted items in hash array Items */
static struct item *Items[ITEMS_HASH_PRIME];

int search_replay_logevent (struct lev_generic *E, int size);

int init_search_data (int schema) {
  replay_logevent = search_replay_logevent;
  return 0;
}

static int search_le_start (struct lev_start *E) {
  if (E->schema_id != SEARCH_SCHEMA_V1) {
    return -1;
  }
  log_split_min = E->split_min;
  log_split_max = E->split_max;
  log_split_mod = E->split_mod;
  assert (log_split_mod > 0 && log_split_min >= 0 && log_split_min + 1 ==  log_split_max && log_split_max <= log_split_mod);
  return 0;
}

static int fits (int x) {
  if (x < 0) {
    x = -x;
  }
  return x % log_split_mod == log_split_min;
}

static int create_item (struct item *C, long long item_id) {
  C->item_id = item_id;
  C->mask = 0;
  C->rates_len = 0;
  C->rates = 0;
  C->extra = 0;
  C->len = 0;
  C->str = 0;
  return 1;
}

static struct item *get_item_f (long long item_id, int force) {
  int h1, h2;
  struct item *C;
  if (item_id <= 0 || !fits (item_id)) { return 0; }
  h1 = item_id % ITEMS_HASH_PRIME;
  h2 = 1 + (item_id % (ITEMS_HASH_PRIME - 1));
  int first_deleted = -1;
  while ((C = Items[h1]) != 0) {
    if (C->item_id == item_id) {
      return C;
    }
    if (first_deleted < 0 && (C->extra & FLAG_DELETED)) { first_deleted = h1; }
    h1 += h2;
    if (h1 >= ITEMS_HASH_PRIME) { h1 -= ITEMS_HASH_PRIME; }
  }
  if (!force || tot_items >= MAX_ITEMS) { return 0; }
  if (first_deleted >= 0) {
    C = Items[first_deleted];
    del_items--;
  } else {
    C = ztmalloc (sizeof (struct item));
    if (!C) { return C; }
    Items[h1] = C;
    tot_items++;
  }
  assert (create_item (C, item_id));
  return C;
}

static int delete_item (long long item_id) {
  struct item *I = get_item_f (item_id, 0);
  if (!I || (I->extra & FLAG_DELETED)) { return 0; }
  I->extra |= FLAG_DELETED;
  zzfree (I->rates, I->rates_len * 4);
  I->rates = 0;
  I->mask = 0;
  I->rates_len = 0;
  zzfree (I->str, I->len+1);
  deleted_text_bytes += I->len;
  I->len = 0;
  I->str = 0;
  del_items++;
  return 1;
}

static int set_multiple_rates_item (struct item *I, int mask, int *rates) {
  if (!I || (I->extra & FLAG_DELETED)) { return 0; }
  int new_mask = I->mask | mask;
  if (new_mask != I->mask) {
    int u = new_mask ^ I->mask;
    while (u) {
      I->rates_len++;
      u &= u - 1;
    }
    I->rates = zzrealloc_ushort_mask (I->rates, I->mask, new_mask, sizeof (int));
    I->mask = new_mask;
  }
  int i = 0, u = mask;
  while (u) {
    int x;
    u ^= x = u & -u;
    I->rates[popcount_short (new_mask & (x-1))] = rates[i++];
  }
  return 1;
}

static int set_rate_item (struct item *I, int n, int rate) {
  if (!I || (I->extra & FLAG_DELETED)) {
    return 0;
  }
  if ((I->mask & (1 << n)) == 0) {
    I->rates = zzrealloc_ushort_mask (I->rates, I->mask, I->mask | (1 << n), 4);
    I->mask |= (1 << n);
    I->rates_len++;
  }
  I->rates [get_bitno (I->mask, n)] = rate;
  return 1;
}

static int incr_rate_item (struct item *I, int n, int rate) {
  if (!I || (I->extra & FLAG_DELETED)) {
    return 0;
  }
  if ((I->mask & (1 << n)) == 0) {
    I->rates = zzrealloc_ushort_mask (I->rates, I->mask, I->mask | (1 << n), 4);
    I->mask |= (1 << n);
    I->rates_len++;
  }
  I->rates [get_bitno (I->mask, n)] += rate;
  return 1;
}

static int set_rate_new (long long item_id, int n, int rate) {
  return set_rate_item (get_item_f (item_id, 0), n, rate);
}

static int incr_rate_new (long long item_id, int n, int rate) {
  return incr_rate_item (get_item_f (item_id, 0), n, rate);
}

static int set_rate (long long item_id, int rate) {
  return set_rate_new (item_id, 0, rate);
}

static int set_rate2 (long long item_id, int rate2) {
  return set_rate_new (item_id, 1, rate2);
}

static int set_rates (long long item_id, int rate, int rate2) {
  if (!set_rate_new (item_id, 0, rate)) { return 0;}
  return set_rate_new (item_id, 1, rate2);
}

static int incr_rate (long long item_id, int rate_incr) {
  return incr_rate_new (item_id, 0, rate_incr);
}

static int incr_rate2 (long long item_id, int rate2_incr) {
  return incr_rate_new (item_id, 1, rate2_incr);
}

static int reset_all_rates (struct lev_search_reset_all_rates *E) {
  vkprintf (2, "%s: rate_id = %d\n", __func__, E->rate_id);
  if (E->rate_id < 0 || E->rate_id >= 14) {
    return -1;
  }
  const short mask = 1 << E->rate_id;
  const int n = E->rate_id;
  int i, r = 0;
  for (i = 0; i < ITEMS_HASH_PRIME; i++) {
    struct item *I = Items[i];
    if (I && !(I->extra & FLAG_DELETED) && (I->mask & mask)) {
      int p = get_bitno (I->mask, n);
      if (I->rates[p]) {
        I->rates[p] = 0;
        r++;
      }
    }
  }
  vkprintf (2, "%s: reset %d ratings (rate_id = %d).\n", __func__, r, n);
  return r;
}

void save_index (void);

static int add_item_tags (const char *text, int len, long long item_id) {
  struct item *I = get_item_f (item_id, 1);
  if (I == NULL) {
    return 0;
  }
  assert (I->str);
  int new_len = I->len + len + 1;
  char *s = zzmalloc (new_len + 1);
  assert (s);
  strcpy (s, text);
  s[len] = ' ';
  strcpy (s + len + 1, I->str);
  zzfree (I->str, I->len+1);
  deleted_text_bytes += I->len;
  I->len = new_len;
  I->str = s;
  return 1;
}

static int change_item (const char *text, int len, long long item_id, int rate, int rate2) {
  if (!fits (item_id)) {
    return 0;
  }

  struct item *I = get_item_f (item_id, 1);

  if (!I && tot_items == MAX_ITEMS && del_items > 1000) {
    save_index ();
    fprintf (stderr, "exit (13)\n");
    exit (13);
  }

  if (!I) {
    fprintf (stderr, "tot_items = %d\n", tot_items);
    assert (I);
  }
  assert (text && len >= 0 && len < 65536 && !text[len]);

  if (I->extra & FLAG_DELETED) {
    del_items--;
    I->extra ^= FLAG_DELETED;
  }

  int rates[4], mask = 3, l = 2;
  rates[0] = rate;
  rates[1] = rate2;
  if (!creation_date || !(I->mask & 4)) {
    rates[l++] = log_last_ts;
    mask += 4;
  }
  set_multiple_rates_item (I, mask, rates);

  if (I->str) {
    zzfree (I->str, I->len+1);
    deleted_text_bytes += I->len;
    I->len = 0;
  }

  I->str = zzmalloc (len+1);
  assert (I->str);
  I->len = len;
  assert (strlen (text) <= len);
  strcpy (I->str, text);

  return 1;
}


int search_replay_logevent (struct lev_generic *E, int size) {
  int s;
  switch (E->type) {
  case LEV_START:
    if (size < 24 || E->b < 0 || E->b > 4096) { return -1; }
    s = 24 + ((E->b + 3) & -4);
    if (size < s) { return -1; }
    return search_le_start ((struct lev_start *) E) >= 0 ? s : -1;
  case LEV_NOOP:
  case LEV_TIMESTAMP:
  case LEV_CRC32:
  case LEV_ROTATE_FROM:
  case LEV_ROTATE_TO:
  case LEV_TAG:
    return default_replay_logevent (E, size);
  case LEV_SEARCH_TEXT_LONG:
    if (size < 24) { return -2; }
    struct lev_search_text_long_entry *EL = (void *) E;
    s = (unsigned) EL->text_len;
    if (size < 23 + s) { return -2; }
    if (EL->text[s]) { return -4; }
    change_item (EL->text, s, EL->obj_id, EL->rate, EL->rate2);
    return 23+s;
  case LEV_SEARCH_TEXT_SHORT ... LEV_SEARCH_TEXT_SHORT+0xff:
    if (size < 20) { return -2; }
    struct lev_search_text_short_entry *ES = (void *) E;
    s = (E->type & 0xff);
    if (size < 21 + s) { return -2; }
    if (ES->text[s]) { return -4; }
    change_item (ES->text, s, ES->obj_id, ES->rate, ES->rate2);
    return 21+s;
  case LEV_SEARCH_DELETE_ITEM:
    if (size < 12) { return -2; }
    delete_item (((struct lev_search_delete_item *) E)->obj_id);
    return 12;
  case LEV_SEARCH_SET_RATE:
    if (size < 16) { return -2; }
    set_rate (((struct lev_search_set_rate *) E)->obj_id, E->a);
    return 16;
  case LEV_SEARCH_SET_RATE2:
    if (size < 16) { return -2; }
    set_rate2 (((struct lev_search_set_rate *) E)->obj_id, E->a);
    return 16;
  case LEV_SEARCH_SET_RATES:
    if (size < 20) { return -2; }
    set_rates (((struct lev_search_set_rates *) E)->obj_id, E->a, E->d);
    return 20;
  case LEV_SEARCH_INCR_RATE_SHORT ... LEV_SEARCH_INCR_RATE_SHORT + 0xff:
    if (size < 12) { return -2; }
    incr_rate (((struct lev_search_incr_rate_short *) E)->obj_id, (signed char) E->type);
    return 12;
  case LEV_SEARCH_INCR_RATE:
    if (size < 16) { return -2; }
    incr_rate (((struct lev_search_incr_rate *) E)->obj_id, E->a);
    return 16;
  case LEV_SEARCH_INCR_RATE2_SHORT ... LEV_SEARCH_INCR_RATE2_SHORT + 0xff:
    if (size < 12) { return -2; }
    incr_rate2 (((struct lev_search_incr_rate_short *) E)->obj_id, (signed char) E->type);
    return 12;
  case LEV_SEARCH_INCR_RATE2:
    if (size < 16) { return -2; }
    incr_rate2 (((struct lev_search_incr_rate *) E)->obj_id, E->a);
    return 16;
  case LEV_SEARCH_INCR_RATE_NEW ... LEV_SEARCH_INCR_RATE_NEW + 0xff:
    if (size < 16) { return -2; }
    incr_rate_new (((struct lev_search_incr_rate_new *) E)->obj_id, (unsigned char) E->type, E->a);
    return 16;
  case LEV_SEARCH_SET_RATE_NEW ... LEV_SEARCH_SET_RATE_NEW + 0xff:
    if (size < 16) { return -2; }
    set_rate_new (((struct lev_search_set_rate_new *) E)->obj_id, (unsigned char) E->type, E->a);
    return 16;
  case LEV_SEARCH_SET_HASH:
    if (size < 20) { return -2; }
    return 20;
  case LEV_SEARCH_ITEM_ADD_TAGS ... LEV_SEARCH_ITEM_ADD_TAGS+0xff:
    if (size < 12) { return -2; }
    struct lev_search_item_add_tags *ET = (struct lev_search_item_add_tags *) E;
    s = (E->type & 0xff);
    if (size < 13+s) { return -2; }
    if (ET->text[s]) { return -4; }
    add_item_tags (ET->text, s, ET->obj_id);
    return 13+s;
  case LEV_SEARCH_RESET_ALL_RATES:
    if (size < 8) { return -2; }
    reset_all_rates ((struct lev_search_reset_all_rates *) E);
    return 8;
  }

  kprintf ("unknown log event type %08x at position %lld\n", E->type, log_cur_pos());

  return -3;

}

/*
 * SORT
 */

static void qsort_i (int a, int b) {
  int i, j;
  long long h;
  struct item *t;
  if (a >= b) { return; }
  h = Items[(a+b)>>1]->item_id;
  i = a;
  j = b;
  do {
    while (Items[i]->item_id < h) { i++; }
    while (Items[j]->item_id > h) { j--; }
    if (i <= j) {
      t = Items[i];  Items[i++] = Items[j];  Items[j--] = t;
    }
  } while (i <= j);
  qsort_i (a, j);
  qsort_i (i, b);
}

void sort_items (void) {
  int i, j = 0;
  struct item *I;
  for (i = 0; i < ITEMS_HASH_PRIME; i++) {
    I = Items[i];
    if (I) {
      if (!(I->extra & FLAG_DELETED)) {
        Items[j++] = I;
      } else {
        assert (!I->str && !I->rates);
      }
    }
  }
  tot_items = j;
  if (verbosity >= 1) {
    fprintf (stderr, "found %d items. Deleted %d items.\n", tot_items, del_items);
  }
  del_items = 0;
  qsort_i (0, j-1);

}

/*
 *  BUILD WORD-DOCUMENT-POSITION PAIRS
 */

typedef struct pair {
  hash_t word;
  int doc_id;
  int position;
} pair_t;

pair_t *P;
int PC;

static inline int cmp_pair (pair_t *X, pair_t *Y) {
  if (X->word < Y->word) {
    return -1;
  }
  if (X->word > Y->word) {
    return 1;
  }
  if (X->doc_id < Y->doc_id) {
    return -1;
  }
  if (X->doc_id > Y->doc_id) {
    return 1;
  }
  return X->position - Y->position;
}

static void qsort_p (int a, int b) {
  int i, j;
  pair_t h, t;
  if (a >= b) { return; }
  h = P[(a+b)>>1];
  i = a;
  j = b;
  do {
    while (cmp_pair (P+i, &h) < 0) { i++; }
    while (cmp_pair (P+j, &h) > 0) { j--; }
    if (i <= j) {
      t = P[i];  P[i++] = P[j];  P[j--] = t;
    }
  } while (i <= j);
  qsort_p (a, j);
  qsort_p (i, b);
}

#define MAX_WORDS	65536

searchy_pair_word_position_t Q[MAX_WORDS];

static long long *item_ids;
static int *item_positions;

void keep_pair (int doc_id, hash_t word, int position) {
  if (PC >= max_pairs) {
    fprintf (stderr, "\nThere are too many pairs (max_pairs = %d).\n"
                     "Try increase max_pairs using -P command line switch.\n", max_pairs);
    exit (1);
  }
  pair_t *P1 = &P[PC++];
  P1->word = word;
  P1->doc_id = doc_id;
  P1->position = position;
}

static void searchy_make_pairs (const char *text, int len, int doc_id, long long item_id, int *positions) {
  int i, Qw = searchy_extract_words (text, len, Q, MAX_WORDS, universal, tag_owner, item_id, positions);
  for (i = 0; i < Qw; i++) {
    keep_pair (doc_id, Q[i].word, Q[i].position);
  }
}

/*
 *
 *  WRITE INDEX
 *
 */

long long item_texts_offset, words_offset, hapax_legomena_offset, freq_words_offset, word_index_offset;

struct search_index_header Header;
struct search_index_crc32_header CRC32_Header;

//int hapax_legomena;

int load_item (struct item *C) {
  vkprintf (4, "loading item...");

  bread (&C->item_id, 8);
  bread (&C->mask, 2);
  bread (&C->rates_len, 1);
  bread (&C->extra, 1);

  int sz = 4 * C->rates_len;
  C->rates = zzmalloc (sz);
  bread (C->rates, sz);

  assert (popcount_short (C->mask) == C->rates_len);
  return sz + 12;
}

long long jump_log_pos = 0;
int jump_log_ts = 0;
unsigned jump_log_crc32 = 0;

int load_index (kfs_file_handle_t Index) {
  int fd = Index->fd;
  //long long fsize = Index->info->file_size;
  int index_with_crc32 = -1;

  long r;
  int i;

  int sz = sizeof (struct search_index_header);

  r = read (fd, &Header, sz);
  if (r < 0) {
    kprintf ("error reading index file header: %m\n");
    return -3;
  }

  if (r < sz) {
    kprintf ("index file too short: only %ld bytes read\n", r);
    return -2;
  }

  if (Header.magic == SEARCH_INDEX_WITH_CRC32_MAGIC || Header.magic == SEARCHY_INDEX_MAGIC) {
    /* search-x index always defended by crc32 */
    index_with_crc32 = 1;
  } else if (Header.magic == SEARCH_INDEX_MAGIC) {
    kprintf ("index file doesn't defended by CRC32.\n");
    return -8;
  } else {
    kprintf ("bad index file header\n");
    return -4;
  }

  //int sz_headers = sizeof (Header) + index_with_crc32 * sizeof (CRC32_Header);
  sz = sizeof (CRC32_Header);
  if (sz != read (fd, &CRC32_Header, sz)) {
    kprintf ("error reading index (crc32_header). %m\n");
    return -5;
  }

  log_split_min = Header.log_split_min;
  log_split_max = Header.log_split_max;
  log_split_mod = Header.log_split_mod;
  assert (log_split_mod > 0 && log_split_min >= 0 && log_split_min + 1 ==  log_split_max && log_split_max <= log_split_mod);

  assert ((unsigned) Header.items <= MAX_ITEMS);
  //assert ((unsigned) Header.words <= MAX_INDEX_WORDS);
  //assert ((unsigned) Header.frequent_words <= (unsigned) Header.words);
  //assert ((unsigned long long) Header.item_texts_size < MAX_INDEX_TEXTS);

  long long idx_loaded_bytes = sizeof (Header);
  int idx_items = Header.items;

  struct item **IndexItems = zzmalloc (sizeof (IndexItems[0]) * idx_items);
  set_read_file (fd);
  int rbytes = 0;
  struct item tmp_item;
  for (i = 0; i < idx_items; i++) {
    int t = load_item (&tmp_item);
    assert (t >= 0);
    //if (t < 0) { return -2;}
    rbytes += t;
    assert (!i || IndexItems[i-1]->item_id < tmp_item.item_id);
    IndexItems[i] = get_item_f (tmp_item.item_id, 1);
    assert (IndexItems[i]);
    IndexItems[i]->mask = tmp_item.mask;
    IndexItems[i]->rates_len = tmp_item.rates_len;
    IndexItems[i]->extra = tmp_item.extra;
    IndexItems[i]->rates = tmp_item.rates;
  }
  assert (rbytes == Header.index_items_size);
  idx_loaded_bytes += rbytes;
  if (index_with_crc32) {
    assert (~idx_crc32_complement == CRC32_Header.crc32_items);
  }

  idx_crc32_complement = -1;

  vkprintf (1, "reading item text from index\n");

  for (i = 0; i < idx_items; i++) {
    struct search_item_text *IT = readin (12);
    assert (IT);
    assert (IT->item_id == IndexItems[i]->item_id);
    assert ((unsigned) IT->len < 65536);
    int len = IT->len;
    IT = readin (13+len);
    assert (IT);
    IndexItems[i]->len = len;
    IndexItems[i]->str = zzmalloc (len+1);
    memcpy (IndexItems[i]->str, IT->text, len);
    IndexItems[i]->str[len] = 0;
    int sz = (16 + len) & -4;
    readadv (sz);
    idx_loaded_bytes += sz;
  }

  if (index_with_crc32) {
    assert (~idx_crc32_complement == CRC32_Header.crc32_text);
  }

  zzfree (IndexItems, sizeof (IndexItems[0]) * idx_items);
  vkprintf (1, "finished loading index: %d items\n", idx_items);

  log_schema = SEARCH_SCHEMA_V1;
  init_search_data (log_schema);

  replay_logevent = search_replay_logevent;

  jump_log_ts = Header.log_timestamp;
  jump_log_pos = Header.log_pos1;
  jump_log_crc32 = Header.log_pos1_crc32;

  clearin ();
  close_snapshot (Index, fd);
  return 0;
}

void write_header0 (void) {
  memset (&Header, 0, sizeof (Header));
  Header.magic = -1;
  Header.log_pos1 = log_cur_pos ();
  relax_log_crc32 (0);
  Header.log_pos1_crc32 = ~log_crc32_complement;
  Header.items = tot_items;
  Header.log_timestamp = log_read_until;
  Header.command_line_switches_flags = get_cls_flags ();
  Header.stemmer_version = stemmer_version;
  Header.listcomp_version = listcomp_version;
  Header.word_split_version = word_split_version;
  Header.left_subtree_size_threshold = lss_threshold;
  writeout (&Header, sizeof (Header));
  writeout (&CRC32_Header, sizeof (CRC32_Header));
}

void write_header1 (void) {
  lseek (fd[0], 0, SEEK_SET);
  clearin ();
  Header.magic = SEARCHY_INDEX_MAGIC;
  Header.created_at = time (0);
  Header.log_split_min = log_split_min;
  Header.log_split_max = log_split_max;
  Header.log_split_mod = log_split_mod;
  CRC32_Header.crc32_header = compute_crc32 (&Header, sizeof (Header));
  writeout (&Header, sizeof (Header));
  writeout (&CRC32_Header, sizeof (CRC32_Header));
  flushout ();
}

int kill_zero_rates (struct item *I) {
  int u = I->mask, m = 0, i = 0, mask = 0;
  while (u) {
    int x;
    u ^= x = u & -u;
    if (I->rates[i] || (x & 0xc000)) {
      mask |= x;
      I->rates[m++] = I->rates[i];
    }
    i++;
  }
  assert (i == I->rates_len);
  I->rates_len = m;
  I->mask = mask;
  return i;
}

void write_text (void) {
  struct item *I;
  int len, i;
  Header.index_items_size = 0;
  idx_crc32_complement = -1;
  for (i = 0; i < tot_items; i++) {
    I = Items[i];
    int old_rates_len = kill_zero_rates (I);
    writeout (&I->item_id, 8);
    writeout (&I->mask, 2);
    writeout (&I->rates_len, 1);
    writeout (&I->extra, 1);
    writeout (I->rates, 4 * I->rates_len);
    zzfree (I->rates, 4 * old_rates_len);
    I->rates = 0;
    vkprintf (4, "I->mask = %x, I->rates_len = %d\n", (int) I->mask, I->rates_len);
    assert (popcount_short (I->mask) == I->rates_len);
    Header.index_items_size += 8 + 2 + 1 + 1 + 4 * I->rates_len;
  }
  CRC32_Header.crc32_items = ~idx_crc32_complement;
  idx_crc32_complement = -1;
  const long long padded = 0;
  for (i = 0; i < tot_items; i++) {
    I = Items[i];
    len = (I->len + 4) & -4;
    writeout (I, 12);
    writeout (I->str, I->len);
    writeout (&padded, len - I->len);
    zzfree (I->str, I->len + 1);
    Header.item_texts_size += len+12;
    I->str = 0;
    I->len = 0;
  }
  CRC32_Header.crc32_text = ~idx_crc32_complement;
  vkprintf (1, "%d item descriptions, %lld bytes of item text written\n", tot_items, Header.item_texts_size);
}

void build_pairs_from_text (void) {
  int i;
  struct search_item_text *IT;

  zzcheck_memory_leaks ();
  if (verbosity >= 3) {
    fprintf (stderr, "tot_items = %d\n", tot_items);
    for (i = 0; i < 4096; i++) {
      if (UsedCnt[i]) {
        fprintf (stderr, "UsedCnt[%d] = %d\n", i, UsedCnt[i]);
      }
    }
  }
  dyn_clear_low ();
  dyn_clear_free_blocks ();
  unsigned long long bytes = 8LL * tot_items;
  item_ids = zmalloc (bytes);
  for (i = 0; i < tot_items; i++) {
    item_ids[i] = Items[i]->item_id;
    Items[i] = 0;
  }

  dyn_clear_high ();

  item_positions = zmalloc (4LL * tot_items + 4);
  bytes += 4LL * tot_items + 4;

  flushout ();
  clearin ();
  lseek (fd[0], sizeof (Header) + sizeof (CRC32_Header) + Header.index_items_size, SEEK_SET);

  double t =  -get_utime(CLOCK_MONOTONIC);
  unsigned long long old_bytes = bytes;
  bytes = (bytes + 15) & -16LL;
  zmalloc (bytes - old_bytes);
  long sz = max_pairs;
  sz *= sizeof (pair_t);
  P = zmalloc (sz);

  for (i = 0; i < tot_items; i++) {
    IT = readin (12);
    assert (IT);
    assert (IT->item_id == item_ids[i]);
    assert ((unsigned) IT->len < 65536);
    IT = readin (13+IT->len);
    assert (IT);
    readadv ((16+IT->len) & -4);
    searchy_make_pairs (IT->text, IT->len, i, IT->item_id, item_positions + i);
  }

  item_positions[tot_items] = compute_crc32 (item_positions, 4LL * tot_items);

  vkprintf (1, "re-reading item text for word hashing (%.6lf seconds).\n", t + get_utime (CLOCK_MONOTONIC));
  assert (~idx_crc32_complement == CRC32_Header.crc32_text);

  clearin ();

  qsort_p (0, PC-1);

  vkprintf (1, "sorting %d word instances (%.6lf seconds).\n", PC, t + get_utime (CLOCK_MONOTONIC));
  vkprintf (1, "%d items, %d word instances\n", tot_items, PC);
}

/*
 * WORDS
 */

struct searchy_index_word *W;

static int count_words (void) {
  int c, m, e, i;
  hash_t word;

  if (!PC) {
    return 0;
  }

  word = P[0].word;
  c = 1;
  m = 0;
  e = 0;
  for (i = 1; i < PC; i++) {
    if (word != P[i].word) {
      word = P[i].word;
      if (!m) { e++; }
      m = 0;
      c++;
    } else {
      m++;
    }
  }

  if (!m) { e++; }

  //hapax_legomena = e;
  if (verbosity > 0) {
    fprintf (stderr, "%d distinct words, %d hapax legomena\n", c, e);
  }
  return c;
  //return c - hapax_legomena;
}

/*
 * REVERSE LISTS
 */

static unsigned char *Li, *Le;

static int max_encoded_list_size[1];
static long long encoded_bytes[1];
static long long cur_offs;
static int lists_encoded;
//long long redundant_bits, interpolative_ext_bits;
static long long coordinate_bits = 0, tag_bits = 0, terms_bits = 0;

static void encode_lists_reset (void) {
  memset (encoded_bytes, 0, sizeof (encoded_bytes));
  memset (max_encoded_list_size, 0, sizeof (max_encoded_list_size));
  cur_offs = word_index_offset;
  lists_encoded = 0;
}

static void bwrite_coordinates_list (struct bitwriter *bw, pair_t *P, int len) {
  int i;
  assert (P->doc_id >= 0 && P->doc_id < tot_items);
  coordinate_bits -= bwrite_get_bits_written (bw);
  bwrite_gamma_code (bw, len);
  struct list_encoder enc;
  const int N = item_positions[P->doc_id];
  assert (len > 0 && len <= N);
  list_encoder_init (&enc, item_positions[P->doc_id], len, bw->start_ptr, bw->end_ptr, le_golomb, bwrite_get_bits_written (bw));
  //fprintf (stderr, "[ ");
  for (i = 0; i < len; i++) {
    //fprintf (stderr, "%d ", P[i].position);
    assert (P[i].position >= 1 && P[i].position <= N);
    assert (!i || P[i-1].position < P[i].position);
    enc.encode_int (&enc, P[i].position - 1);
  }
  //fprintf (stderr, "] \n");
  //memcpy (bw, &enc.bw, sizeof (struct bitwriter));
  bw->ptr = enc.bw.ptr;
  bw->m = enc.bw.m;
  coordinate_bits += bwrite_get_bits_written (bw);
}

static int curword_docs;
static inline int get_doc_id (pair_t *P, int *O, int m) {
  if (m <= 0) {
    return -1;
  }
  if (m > curword_docs) {
    return tot_items;
  }
  return P[O[m]].doc_id;
}

static void bwrite_ylist_sublist_first_pass (struct bitwriter *bw, pair_t *P, int *O, int u, int v, int left_subtree_size_threshold, struct left_subtree_bits_array *p) {
  const int sz = v - u;
  if (sz <= 1) { return; }
  const int  m = (u + v) >> 1,
            hi = get_doc_id (P, O, v) - (v - m),
            lo = get_doc_id (P, O, u) + (m - u),
             a = get_doc_id (P, O, m) - lo,
             r = hi - lo + 1;
  bwrite_interpolative_encode_value (bw, a, r);
  bwrite_coordinates_list (bw, P + O[m], O[m+1] - O[m]);
  if (sz >= left_subtree_size_threshold) {
    assert (p->idx < p->n);
    int *q = &p->a[p->idx];
    p->idx++;
    int tree_bits = -bwrite_get_bits_written (bw);
    bwrite_ylist_sublist_first_pass (bw, P, O, u, m, left_subtree_size_threshold, p);
    tree_bits += bwrite_get_bits_written (bw);
    *q = tree_bits;
    bwrite_gamma_code (bw, tree_bits + 1);
  } else {
    bwrite_ylist_sublist_first_pass (bw, P, O, u, m, left_subtree_size_threshold, p);
  }
  bwrite_ylist_sublist_first_pass (bw, P, O, m, v, left_subtree_size_threshold, p);
}

static void bwrite_ylist_sublist_second_pass (struct bitwriter *bw, pair_t *P, int *O, int u, int v, int left_subtree_size_threshold, struct left_subtree_bits_array *p) {
  //fprintf (stderr, "%s[1]: [%d:%d] written_bits: %d\n", __func__, u, v, bwrite_get_bits_written (bw));
  const int sz = v - u;
  if (sz <= 1) { return; }
  const int  m = (u + v) >> 1,
            hi = get_doc_id (P, O, v) - (v - m),
            lo = get_doc_id (P, O, u) + (m - u),
             a = get_doc_id (P, O, m) - lo,
             r = hi - lo + 1;
  bwrite_interpolative_encode_value (bw, a, r);
  //fprintf (stderr, "%s[2]: [%d:%d] written_bits: %d\n", __func__, u, v, bwrite_get_bits_written (bw));
  bwrite_coordinates_list (bw, P + O[m], O[m+1] - O[m]);
  //fprintf (stderr, "%s[3]: [%d:%d] written_bits: %d\n", __func__, u, v, bwrite_get_bits_written (bw));
  if (sz >= left_subtree_size_threshold) {
    assert (p->idx < p->n);
    const int lsb = p->a[p->idx];
    p->idx++;
    bwrite_gamma_code (bw, lsb + 1);
    int tree_bits = -bwrite_get_bits_written (bw);
    //fprintf (stderr, "before call [%d:%d] bwrite_get_bits_written (bw): %d\n", u, v, bwrite_get_bits_written (bw));
    bwrite_ylist_sublist_second_pass (bw, P, O, u, m, left_subtree_size_threshold, p);
    tree_bits += bwrite_get_bits_written (bw);
    //fprintf (stderr, "after call [%d:%d] bwrite_get_bits_written (bw): %d\n", u, v, bwrite_get_bits_written (bw));
    //fprintf (stderr, "lsb: %d, tree_bits: %d\n", lsb, tree_bits);
    assert (lsb == tree_bits);
  } else {
    bwrite_ylist_sublist_second_pass (bw, P, O, u, m, left_subtree_size_threshold, p);
  }
  bwrite_ylist_sublist_second_pass (bw, P, O, m, v, left_subtree_size_threshold, p);
}

static int searchy_encode_list (pair_t *P, int len, struct searchy_index_word *W) {
  assert (len > 0);
  long long *file_offset = &W->file_offset;
  *file_offset = 0;
  int i, j, K;
  struct bitwriter bw;

  dyn_mark_t mrk;
  dyn_mark (mrk);

  if (searchy_is_term (W->word)) {
    /* term */
    tot_terms++;
    for (i = 0; i < len; i++) {
      assert (P[i].position > 0);
    }
    K = 0;
    for (i = 0; i < len; i++) {
      if (!i || P[i-1].doc_id != P[i].doc_id) {
        K++;
      }
    }
    int *off = zmalloc (4 * (K + 2));
    j = 1;
    for (i = 0; i < len; i++) {
      if (!i || P[i-1].doc_id != P[i].doc_id) {
        off[j++] = i;
      }
    }
    off[j] = len;
    curword_docs = K;
    assert (j == K + 1);
    bwrite_init (&bw, Li, Le, 0);
    struct left_subtree_bits_array p;
    p.n = get_subtree_array_size (0, K+1, lss_threshold);
    p.a = zmalloc (p.n * sizeof (int));
    p.idx = 0;
    bwrite_ylist_sublist_first_pass (&bw, P, off, 0, K+1, lss_threshold, &p);
    //fprintf (stderr, "%s: bwrite_get_bits_written (bw): %d\n", __func__, bwrite_get_bits_written (&bw));
    bwrite_init (&bw, Li, Le, 0);
    assert (p.idx == p.n);
    p.idx = 0;
    bwrite_ylist_sublist_second_pass (&bw, P, off, 0, K+1, lss_threshold, &p);
    terms_bits += bwrite_get_bits_written (&bw);
  } else {
    /* tag */
    tot_tags++;
    for (i = 0; i < len; i++) {
      assert (!P[i].position);
    }
    K = 0;
    for (i = 0; i < len; i++) {
      if (!i || P[i-1].doc_id != P[i].doc_id) {
        K++;
      }
    }
    int *L = zmalloc (4 * (K + 2));
    L[0] = -1;
    j = 1;

    bwrite_init (&bw, Li, Le, 0);
    for (i = 0; i < len; i++) {
      if (!i || P[i-1].doc_id != P[i].doc_id) {
        L[j++] = P[i].doc_id;
      }
    }
    L[j] = tot_items;
    assert (j == K + 1);
    int redundant_bits = 0;
    bwrite_interpolative_ext_sublist (&bw, L, 0, K + 1, lss_threshold, &redundant_bits);
    tag_bits += bwrite_get_bits_written (&bw);
  }

  W->len = K;
  int bits_written = bwrite_get_bits_written (&bw);
  const int a = (bits_written >> 3) + ((bits_written & 7) ? 1 : 0);

  if (max_encoded_list_size[0] < a) {
    max_encoded_list_size[0] = a;
  }

  if (a <= 8) {
    memcpy (file_offset, Li, a);
    W->len *= -1;
  } else {
    *file_offset = cur_offs;
    writeout (Li, a);
    lists_encoded++;
    cur_offs += a;
    encoded_bytes[0] += a;
  }

  dyn_release (mrk);

  /*
  vkprintf (3, "Checking encoded data.\n");
  struct list_decoder* dec = zmalloc_list_decoder (N, K, Li, le_golomb, 0);
  len = W->len;
  P -= len;
  while (len > 0) {
    len--;
    int d = dec->decode_int (dec);
    assert (d == P->doc_id);
    unsigned freq1 = bread_gamma_code (&dec->br) - 1;
    unsigned freq2 = bread_gamma_code (&dec->br) - 1;
    if (!freq1) { freq2++; }
    assert (P->freqs == ((freq1 << 16) + freq2));
    P++;
  }
  int bits_read = bread_get_bits_read (&dec->br);
  if (bits_written != bits_read) {
    kprintf ("bits_written = %d, bits_read = %d, a = %d\n", bits_written, bits_read, a);
    assert (bits_written == bits_read);
  }
  zfree_list_decoder (dec);
  */
  return a;
}

static int searchy_encode_lists (void) {
  encode_lists_reset ();
  lseek (fd[0], word_index_offset, SEEK_SET);
  int i = 0, j = 0, k;
  for (k = 0; k < PC; k = j) {
    hash_t word = P[k].word;
    for (j = k + 1; j < PC && P[j].word == word; j++) {}
    W[i].word = P[k].word;
    searchy_encode_list (P + k, j - k, W + i);
    i++;
  }
  assert (i == Header.words);

  CRC32_Header.crc32_data = ~idx_crc32_complement;
  memset (W+i, 0, sizeof (struct searchy_index_word));
  W[i].file_offset = cur_offs;

  writeout (item_positions, 4LL * tot_items + 4);
  vkprintf (1, "wrote %d item positions bytes\n", i);

  flushout ();
  long long data_bits = terms_bits + tag_bits;
  vkprintf (1, "total data bits %lld, coordinates (%.6lf%%)\n",
    data_bits, (100.0 * coordinate_bits) / data_bits);
  vkprintf (1, "wrote %lld codes bytes in %d lists\n", cur_offs - word_index_offset, lists_encoded);
  vkprintf (1, "%d terms (%lld bits) and %d tags (%lld bits)\n",
    tot_terms, terms_bits, tot_tags, tag_bits);
  return i;
}


static void build_word_lists (void) {
  int i;
  Header.words = count_words ();
  Header.hapax_legomena = 0;
  /*
  Header.frequent_words = Header.words < MAX_FREQUENT_WORDS ? 0 : MAX_FREQUENT_WORDS;
  */
  Header.frequent_words = 0;
  item_texts_offset = sizeof (Header) + sizeof (CRC32_Header) + Header.index_items_size;
  words_offset = item_texts_offset + Header.item_texts_size;
  hapax_legomena_offset = words_offset + (Header.words + 1) * sizeof (struct searchy_index_word);
  freq_words_offset = hapax_legomena_offset + 0;
  word_index_offset = freq_words_offset + 0;

  clearin ();

  W = ztmalloc ((Header.words + 1) * sizeof (struct searchy_index_word));

  long MaxL = dyn_free_bytes () >> 1;
  if (MaxL < 1000000) {
    MaxL = 1000000;
  }
  Li = zmalloc (MaxL);
  Le = Li + MaxL;

  searchy_encode_lists ();

  lseek (fd[0], words_offset, SEEK_SET);
  i = (Header.words + 1) * sizeof (struct searchy_index_word);
  assert (write (fd[0], W, i) == i);
  CRC32_Header.crc32_words = compute_crc32 (W, i);
  vkprintf (1, "wrote %d word description bytes for %d words\n", i, Header.words);



/*
  i = (Header.hapax_legomena + 1) * sizeof (struct search_index_hapax_legomena);
  assert (write (fd[0], HL, i) == i);
  CRC32_Header.crc32_hapax_legomena = compute_crc32 (HL, i);
  vkprintf (1, "wrote %d word description bytes for %d hapax legomena\n", i, Header.hapax_legomena);
*/

}

/*
 *
 *		MAIN
 *
 */

void usage (void) {
  printf ("%s\n", FullVersionStr);
  printf ("usage: %s [-v] [-t] [-A] [-u<username>] [-a<binlog-name>] [-l<log-name>] [-f] <huge-index-file>\n"
      "\tBuilds a search index from given binlog file.\n"
      "\t-t\tenable tags (*word @word #word are considered words)\n"
      "\t-A\tenable universal tag\n"
      "\t-S\tuse stemmer\n"
      "\t-U\tenable UTF-8 mode\n"
      "\t-D\tstore in item's date modification time (default: first creation time)\n"
      "\t-H<heap-size>\tsets maximal heap size, e.g. 4g\n"
      "\t-B<heap-size>\tsets maximal binlog size, e.g. 4g\n"
      "\t-P<max_pairs>\tsets maximal number of pairs\n"
      "\t-v\toutput statistical and debug information into stderr\n"
      "\t-O\tenable tag owner mode\n"
      "\t-x<left subtree size threshold>\tdefault (-x '%d')\n"
      , progname, lss_threshold
    );
  exit (2);
}

void save_index (void) {
  char *newidxname = NULL;
  if (engine_snapshot_replica) {
    newidxname = get_new_snapshot_name (engine_snapshot_replica, log_cur_pos(), engine_snapshot_replica->replica_prefix);
  }

  if (!newidxname || newidxname[0] == '-') {
    fprintf (stderr, "cannot write index: cannot compute its name\n");
    exit (1);
  }

  open_file (0, newidxname, 1);

  set_read_file (fd[0]);
  set_write_file (fd[0]);

  sort_items ();

  write_header0 ();
  write_text ();
  build_pairs_from_text ();
  build_word_lists ();
  write_header1 ();

  assert (fsync(fd[0]) >= 0);
  assert (close (fd[0]) >= 0);

  if (rename_temporary_snapshot (newidxname)) {
    kprintf ("cannot rename new index file from %s: %m\n", newidxname);
    unlink (newidxname);
    exit (1);
  }

  print_snapshot_name (newidxname);
}

int main (int argc, char *argv[]) {
  int i;
  char c;
  long long x;

  set_debug_handlers();
  dynamic_data_buffer_size = DYNAMIC_DATA_BIG_BUFFER_SIZE;

  progname = argv[0];
  while ((i = getopt (argc, argv, "AB:H:OP:SUa:hl:tu:vx:")) != -1) {
    switch (i) {
    case 'A':
      universal = 1;
      break;
    case 'B':
    case 'H':
      c = 0;
      assert (sscanf (optarg, "%lld%c", &x, &c) >= 1);
      switch (c | 0x20) {
        case 'k':  x <<= 10; break;
        case 'm':  x <<= 20; break;
        case 'g':  x <<= 30; break;
        case 't':  x <<= 40; break;
        default: assert (c == 0x20);
      }
      if (i == 'B' && x >= 1024 && x < (1LL << 60)) {
        max_binlog_size = x;
      } else if (i == 'H' && x >= (1LL << 20) && x <= (sizeof(long) == 4 ? (3LL << 30) : (20LL << 30))) {
        dynamic_data_buffer_size = x;
      }
      break;
    case 'D':
      creation_date = 0;
      break;
    case 'O':
      tag_owner = 1;
      break;
    case 'P':
      if (sscanf (optarg, "%lld", &x) == 1) {
        if (x > 0 && x <= INT_MAX) { max_pairs = x; }
      }
      break;
    case 'S':
      use_stemmer = 1;
      break;
    case 'U':
      word_split_utf8 = 1;
      break;
    case 'a':
      binlogname = optarg;
      break;
    case 'h':
      usage();
      return 2;
    case 'l':
      logname = optarg;
      break;
    case 't':
      hashtags_enabled = 1;
      break;
    case 'u':
      username = optarg;
      break;
    case 'v':
      verbosity++;
      break;
    case 'x':
      if (sscanf (optarg, "%lld", &x) == 1) {
        if (x < 16) {
          x = 16;
        }
        if (x <= INT_MAX) {
          lss_threshold = x;
        }
      }
      break;
    default:
      fprintf (stderr, "Unimplemented option -%c\n", i);
      exit (1);
    }
  }

  long long sz = max_pairs;
  sz *= sizeof (pair_t);
  if (sz > dynamic_data_buffer_size) {
    fprintf (stderr, "max_pairs too high for fits into dynamic data buffer.\n");
    usage();
    return 2;
  }

  if (argc != optind + 1) {
    usage();
    return 2;
  }

  if (change_user (username) < 0) {
    kprintf ("fatal: cannot change user to %s\n", username ? username : "(none)");
    exit (1);
  }

  init_is_letter ();
  if (hashtags_enabled) {
    enable_is_letter_sigils ();
  }
  if (use_stemmer) {
    stem_init ();
  }

  init_dyn_data ();

  if (engine_preload_filelist (argv[optind], binlogname) < 0) {
    kprintf ("cannot open binlog files for %s\n", binlogname ? binlogname : argv[optind]);
    exit (1);
  }

  double index_load_time;
  //Snapshot reading
  Snapshot = open_recent_snapshot (engine_snapshot_replica);
  if (Snapshot) {
    engine_snapshot_name = strdup (Snapshot->info->filename);
    engine_snapshot_size = Snapshot->info->file_size;
    vkprintf (1, "load index file %s (size %lld)\n", engine_snapshot_name, engine_snapshot_size);
    index_load_time = -get_utime(CLOCK_MONOTONIC);
    i = load_index (Snapshot);
    if (i < 0) {
      kprintf ("load_index returned fail code %d. Skipping index.\n", i);
    }
    index_load_time += get_utime(CLOCK_MONOTONIC);
    vkprintf (1, "Index load time %.6lf seconds.\n", index_load_time);
  } else {
    engine_snapshot_name = NULL;
    engine_snapshot_size = 0;
    index_load_time = 0;
  }

  Binlog = open_binlog (engine_replica, jump_log_pos);
  if (!Binlog) {
    kprintf ("fatal: cannot find binlog for %s\n", engine_replica->replica_prefix);
    exit (1);
  }

  binlogname = Binlog->info->filename;

  log_ts_interval = 10;

  vkprintf (1, "replaying binlog file %s (size %lld) from position %lld\n", binlogname, Binlog->info->file_size, jump_log_pos);

  double binlog_load_time = get_utime(CLOCK_MONOTONIC);

  clear_log();

  init_log_data (jump_log_pos, jump_log_ts, jump_log_crc32);

  replay_logevent = search_replay_logevent;
  i = replay_log (0, 1);

  binlog_load_time = get_utime(CLOCK_MONOTONIC) - binlog_load_time;

  close_binlog (Binlog, 1);

  if (i < 0) {
    kprintf ("fatal: error reading binlog\n");
    exit (1);
  }

  if (verbosity) {
    fprintf (stderr, "replay binlog file: done, pos=%lld, alloc_mem=%ld out of %ld, time %.06lfs\n",
	     log_pos, (long)(dyn_cur - dyn_first), (long)(dyn_last - dyn_first), binlog_load_time);
    fprintf (stderr, "deleted text bytes: %lld\n", deleted_text_bytes);
  }

  save_index ();

  return 0;
}
