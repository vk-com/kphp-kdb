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
              2009-2010 Nikolai Durov
              2009-2010 Andrei Lopatin
              2010-2013 Vitaliy Valtman
              2010-2013 Anton Maydell
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
#include "search-common.h"
#include "utils.h"
#include "listcomp.h"

#ifndef COMMIT
#define COMMIT "unknown"
#endif

#define	VERSION_STR	"search-index"
const char FullVersionStr[] = VERSION_STR " compiled at " __DATE__ " " __TIME__ " by gcc " __VERSION__ " "
#ifdef __LP64__
  "64-bit"
#else
  "32-bit"
#endif
" after commit " COMMIT;

#define MAX_FREQUENT_WORDS 16384
#define MAX_PAIRS 100000000

double interpolative_percent = 90.0;

int verbosity = 0, interactive = 0, daemonize = 0, hash_stats = 0;
int compression_speed = -1;
int load_snapshot_without_crc32 = 0;

int now;

char *fnames[3];
int fd[3];
long long fsize[3];

char *progname, *username, *binlogname, *logname, *newidxname, *itemids_filename = NULL;

/* stats counters */
int start_time;
long long binlog_loaded_size;
double binlog_load_time;

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

static long long get_hash_item (const struct item *I) {
  if ( (I->mask & 0xc000) == 0xc000) {
    assert (I->rates_len >= 2);
    /* since hash stored in 14 (lowest dword) and in 15 (highest word),
       we doesn't need to call get_bitno function
    */
    return (((unsigned long long) I->rates[I->rates_len-1]) << 32) | ((unsigned int) I->rates[I->rates_len-2]);
  }
  assert ( ! (I->mask & 0xc000) );
  return 0LL;
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

int set_hash_item (struct item *I, long long hc) {
  if (!I || (I->extra & FLAG_DELETED)) {
    return 0;
  }
  if ( (I->mask & 0xc000) != 0xc000) {
    assert( ! (I->mask & 0xc000) );
    void *p = zzrealloc (I->rates, I->rates_len << 2, (I->rates_len + 2) << 2);
    if (p == 0) {
      return 0;
    }
    I->mask |= 0xc000;
    I->rates_len += 2;
    I->rates = p;
  }
  assert (I->rates_len >= 2);
  I->rates[I->rates_len-1] = hc >> 32;
  I->rates[I->rates_len-2] = (unsigned int) hc;
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

static int set_hash (long long item_id, long long hash) {
  return set_hash_item (get_item_f (item_id, 0), hash);
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
    rates[l++] = now;
    mask += 4;
  }
  set_multiple_rates_item (I, mask, rates);

  /*
  set_rate_item (I, 0, rate);
  set_rate_item (I, 1, rate2);
  set_rate_item (I, 2, now);
  */

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
    set_hash (((struct lev_search_set_hash *) E)->obj_id,
               ((struct lev_search_set_hash *) E)->hash);
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

int cmp_item_hash (const void *a, const void *b) {
  long long c = get_hash_item (* ((const struct item **) a));
  long long d = get_hash_item (* ((const struct item **) b));
  if (c < d) { return -1; }
  if (c > d) { return  1; }
  return 0;
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

  if (hash_stats) {
    int n = 0;
    for (i = 0; i < tot_items; i++) {
      long long hc = get_hash_item (Items[i]);
      if (hc) { n++; }
    }
    if (n) {
      int max_tot = INT_MIN;
      long long most_frequent_hc = 0;
      struct item **J = zzmalloc (sizeof (J[0]) * n);
      n = 0;
      for (i = 0; i < tot_items; i++) {
        long long hc = get_hash_item (Items[i]);
        if (hc) { J[n++] = Items[i]; }
      }
      qsort (J, n, sizeof (J[0]), cmp_item_hash);
      int m = 0;
      for (i = 0; i < n; ) {
        const long long hc = get_hash_item (J[i]);
        j = i + 1;
        while (j < n && get_hash_item (J[j]) == hc) { j++; }
        m++;
        int tot = j - i;
        if (max_tot < tot) {
          max_tot = tot;
          most_frequent_hc = hc;
        }
        if ((verbosity >= 3 && tot > 1) || (verbosity >= 2 && tot > 100)) {
          fprintf (stderr, "Group %d (items = %d):\n", m, tot);
          int o;
          for (o = i; o < j; o++) {
            fprintf (stderr, "%d_%d\t%s\n", (int) J[o]->item_id, (int) (J[o]->item_id >> 32), J[o]->str);
          }
        }
        i = j;
      }
      zzfree (J, sizeof (J[0]) * n);
      fprintf (stderr, "There are %d (%.6lf%%) unique hashes and %d items with hashes.\n"
                       "Most frequent hash is %llx occurs %d times.\n",
                       m+1, ((double) m+1) * 100.0 / n, n,
                       most_frequent_hc, max_tot);
    }
  }
}


/*
 *  BUILD WORD-DOCUMENT PAIRS
 */

typedef struct pair {
  hash_t word;
  int doc_id;
  unsigned freqs;
} pair_t;

pair_t *P;
int PC;

static void qsort_p (int a, int b) {
  int i, j;
  pair_t h, t;
  if (a >= b) { return; }
  h = P[(a+b)>>1];
  i = a;
  j = b;
  do {
    while (P[i].word < h.word || (P[i].word == h.word && P[i].doc_id < h.doc_id)) { i++; }
    while (P[j].word > h.word || (P[j].word == h.word && P[j].doc_id > h.doc_id)) { j--; }
    if (i <= j) {
      t = P[i];  P[i++] = P[j];  P[j--] = t;
    }
  } while (i <= j);
  qsort_p (a, j);
  qsort_p (i, b);
}

#define MAX_WORDS	65536

pair_word_freqs_t Q[MAX_WORDS];

static long long *item_ids;
#ifdef SEARCHX
static unsigned *item_freqs;
#endif

void keep_pair (int doc_id, hash_t word, unsigned freqs) {
  if (PC >= max_pairs) {
    fprintf (stderr, "\nThere are too many pairs (max_pairs = %d).\n"
                     "Try increase max_pairs using -P command line switch.\n", max_pairs);
    exit (1);
  }
  pair_t *P1 = &P[PC++];
  P1->word = word;
  P1->doc_id = doc_id;
  P1->freqs = freqs;
}

#ifdef SEARCHX
static void searchx_make_pairs (const char *text, int len, int doc_id, unsigned *freqs, long long item_id) {
  int i, Qw = extract_words (text, len, universal, Q, MAX_WORDS, tag_owner, item_id);
  for (i = 0; i < Qw; i++) {
    keep_pair (doc_id, Q[i].word, Q[i].freqs ? Q[i].freqs : 1);
  }
  evaluate_freq_sqr_sums (Q, Qw, freqs, freqs + 1, freqs + 2);
}
#else
static void search_make_pairs (const char *text, int len, int doc_id, long long item_id) {
  int i, Qw = extract_words (text, len, universal, Q, MAX_WORDS, tag_owner, item_id);
  for (i = 0; i < Qw; i++) {
    keep_pair (doc_id, Q[i].word, Q[i].freqs);
  }
}
#endif

/*
 *
 *  WRITE INDEX
 *
 */

long long item_texts_offset, words_offset, hapax_legomena_offset, freq_words_offset, word_index_offset;

struct search_index_header Header;
struct search_index_crc32_header CRC32_Header;

int hapax_legomena;

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

  if (Header.magic == SEARCH_INDEX_WITH_CRC32_MAGIC || Header.magic == SEARCHX_INDEX_MAGIC) {
    /* search-x index always defended by crc32 */
    index_with_crc32 = 1;
  } else if (Header.magic == SEARCH_INDEX_MAGIC) {
    if (!load_snapshot_without_crc32) {
      kprintf ("index file doesn't defended by CRC32.\n");
      return -8;
    }
    index_with_crc32 = 0;
  } else {
    kprintf ("bad index file header\n");
    return -4;
  }

  //int sz_headers = sizeof (Header) + index_with_crc32 * sizeof (CRC32_Header);
  if (index_with_crc32) {
    sz = sizeof (CRC32_Header);
    if (sz != read (fd, &CRC32_Header, sz)) {
      kprintf ("error reading index (crc32_header). %m\n");
      return -5;
    }
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
#ifdef SEARCHX
  Header.magic = SEARCHX_INDEX_MAGIC;
#else
  Header.magic = SEARCH_INDEX_WITH_CRC32_MAGIC;
#endif
  Header.created_at = time (0);
  Header.log_split_min = log_split_min;
  Header.log_split_max = log_split_max;
  Header.log_split_mod = log_split_mod;
  CRC32_Header.crc32_header = compute_crc32 (&Header, sizeof (Header));
  writeout (&Header, sizeof( Header));
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
    if (wordfreqs_enabled) {
      int Qw = extract_words (I->str, I->len, universal, Q, MAX_WORDS, 0, 0); /* tag_owner doesn't matter */
      set_rate_item (I, 13, evaluate_uniq_words_count (Q, Qw));
    }
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

  #ifdef SEARCHX
  item_freqs = zmalloc (12LL * tot_items + 4);
  bytes += 12LL * tot_items + 4;
  #endif

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
#ifdef SEARCHX
    searchx_make_pairs (IT->text, IT->len, i, item_freqs + (3 * i), IT->item_id);
#else
    search_make_pairs (IT->text, IT->len, i, IT->item_id);
#endif
  }
#ifdef SEARCHX
    item_freqs[3 * tot_items] = compute_crc32 (item_freqs, 12LL * tot_items);
#endif
  vkprintf (1, "re-reading item text for word hashing (%.6lf seconds).\n", t + get_utime (CLOCK_MONOTONIC));
  assert (~idx_crc32_complement == CRC32_Header.crc32_text);

  clearin ();
  //lseek (fd[0], sizeof (Header) + sizeof (CRC32_Header) + Header.index_items_size + Header.item_texts_size, SEEK_SET);

  qsort_p (0, PC-1);

  vkprintf (1, "sorting %d word instances (%.6lf seconds).\n", PC, t + get_utime (CLOCK_MONOTONIC));
  vkprintf (1, "%d items, %d word instances\n", tot_items, PC);
}

/*
 * WORDS
 */

struct search_index_word *W;
struct search_index_hapax_legomena *HL;

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

  hapax_legomena = e;
  if (verbosity > 0) {
    fprintf (stderr, "%d distinct words, %d hapax legomena\n", c, e);
  }

  return c - hapax_legomena;
}

/*
 * REVERSE LISTS
 */

static unsigned char *Li, *Le;

int max_encoded_list_size[2];
long long encoded_bytes[2];
static long long cur_offs;
int lists_encoded;
long long redundant_bits, interpolative_ext_bits;

static inline int in_title (pair_t *P) {
  return (P->freqs >= 0x10000);
}

static void encode_lists_reset (void) {
  memset (encoded_bytes, 0, sizeof (encoded_bytes));
  memset (max_encoded_list_size, 0, sizeof (max_encoded_list_size));
  cur_offs = word_index_offset;
  lists_encoded = 0;
}

#ifdef SEARCHX
static int searchx_encode_list (pair_t *P, int len, struct search_index_word *W) {
  W->len = len;
  long long *file_offset = &W->file_offset;
  unsigned short *bytes = &W->bytes;
  *file_offset = 0;
  if (len == 0) {
    return 0;
  }
  const int N = tot_items, K = len;
  struct list_encoder Encoder, *enc = &Encoder;
  list_encoder_init (enc, N, K, Li, Le, le_golomb, 0);
  while (len > 0) {
    len--;
    enc->encode_int (enc, P->doc_id);
    int freq1 = (P->freqs >> 16) + 1, freq2 = (P->freqs & 0xffff) + 1;
    if (freq1 == 1) { freq2--; }
    assert (freq1 >= 1 && freq2 >= 1);
    bwrite_gamma_code (&enc->bw, freq1);
    bwrite_gamma_code (&enc->bw, freq2);
    P++;
  }
  list_encoder_finish (enc);
  int bits_written = bwrite_get_bits_written (&enc->bw);
  const int a = (bits_written >> 3) + ((bits_written & 7) ? 1 : 0);

  if (max_encoded_list_size[0] < a) {
    max_encoded_list_size[0] = a;
  }

  *bytes = (a <= 0xffff ? a : 0xffff);

  if (a <= 8) {
    memcpy (file_offset, Li, a);
  } else {
    *file_offset = cur_offs;
    writeout (Li, a);
    lists_encoded++;
    cur_offs += a;
    encoded_bytes[0] += a;
  }

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
  return a;
}

static int searchx_encode_lists (void) {
  encode_lists_reset ();
  lseek (fd[0], word_index_offset, SEEK_SET);
  int i = 0, j = 0, k, hl = 0;
  for (k = 0; k < PC; k = j) {
    hash_t word = P[k].word;
    for (j = k + 1; j < PC && P[j].word == word; j++) {}
    if (j - k > 1) {
      assert (i < Header.words);
      W[i].word = word;
      searchx_encode_list (P + k, j-k, W + i);
      i++;
    } else {
      if (hl >= Header.hapax_legomena) { vkprintf (1, "h1 = %d, Header.hapax_legomena = %d\n", hl, Header.hapax_legomena); }
      assert (hl < Header.hapax_legomena);
      HL[hl].word = word;
      HL[hl].doc_id_and_priority = P[k].doc_id;
      if (in_title (P + k)) {
        HL[hl].doc_id_and_priority |= (1U << 31);
      }
      hl++;
    }
  }
  assert (i == Header.words);
  assert (hl == Header.hapax_legomena);
  memset (HL+hl, 0, sizeof (struct search_index_hapax_legomena));

  CRC32_Header.crc32_data = ~idx_crc32_complement;
  memset (W+i, 0, sizeof (struct search_index_word));
  W[i].file_offset = cur_offs;

  writeout (item_freqs, 12LL * tot_items + 4);

  flushout ();

  vkprintf (1, "wrote %lld codes bytes in %d lists\n", cur_offs - word_index_offset, lists_encoded);
  return i;
}

#else
static long long get_compression_bytes (int method) {
  return Header.compression_bytes[2*method] + Header.compression_bytes[2*method+1];
}

static double percent (long long a, long long b) {
  return (double) (100.0 * a / b);
}

static int search_encode_core (pair_t *P, int len, int len_cur, struct search_index_word *W, int mode, int compression_method, int queit) {
  long long *file_offset;
  if (mode == 0) {
    file_offset = &W->file_offset;
  } else {
    file_offset = &W->file_offset_subseq;
  }
  unsigned short *bytes;
  if (mode == 0) {
    bytes = &W->bytes;
  } else {
    bytes = &W->bytes_subseq;
  }

  *file_offset = 0;
  if (len_cur == 0) {
    return 0;
  }
  if (len_cur <= 2) {
    int s = 0;
    int l = 0;
    while (len > 0) {
      len--;
      if (mode == 0 || in_title (P)) {
        if (mode == 0) {
          *file_offset += ((long long)P->doc_id) << s;
        } else {
          *file_offset += ((long long)l) << s;
        }
        s += 32;
      }
      l++;
      P++;
    }
    assert (s == 32 * len_cur);
    return 0;
  }

  int N, K;
  if (mode == 0) {
    N = tot_items;
    K = len;
  } else {
    N = len;
    K = len_cur;
  }

  vkprintf (3, "Starting encoding data. Mode = %d\n", mode);
  int bits_written;
  if (compression_method != le_interpolative_ext) {
    struct list_encoder Encoder, *enc = &Encoder;
    list_encoder_init (enc, N, K, Li, Le, compression_method, 0);
    int l = 0;
    while (len > 0) {
      len--;
      if (mode == 0 || in_title (P)) {
        if (mode == 0) {
          enc->encode_int (enc, P->doc_id);
        } else {
          enc->encode_int (enc, l);
        }
      }
      P++;
      l++;
    }
    list_encoder_finish (enc);
    bits_written = bwrite_get_bits_written (&enc->bw);
  } else {
    int *L = zzmalloc ((K + 2) << 2);
    int l = 0, o = 1;
    while (len > 0) {
      len--;
      if (mode == 0 || in_title (P)) {
        if (mode == 0) {
          L[o++] = P->doc_id;
        } else {
          L[o++] = l;
        }
      }
      P++;
      l++;
    }
    assert (o == K+1);
    L[0] = -1;
    L[K+1] = N;
    struct bitwriter bw;
    bwrite_init (&bw, Li, Le, 0);
    int rb;
    bwrite_interpolative_ext_sublist (&bw, L, 0, K+1, lss_threshold, &rb);
    bits_written = bwrite_get_bits_written (&bw);
    redundant_bits += rb;
    interpolative_ext_bits += bits_written;
    zzfree (L, (K + 2) << 2);
  }
  const int a = (bits_written >> 3) + ((bits_written & 7) ? 1 : 0);

  if (max_encoded_list_size[mode] < a) {
    max_encoded_list_size[mode] = a;
  }

  *bytes = (a <= 0xffff ? a : 0xffff);

  if (!queit) {
    if (a <= 8) {
      memcpy (file_offset, Li, a);
    } else {
      *file_offset = cur_offs;
      writeout (Li, a);
    }
  }

  if (a > 8) {
    lists_encoded++;
    cur_offs += a;
    encoded_bytes[mode] += a;
  }

  if (!queit) {
    vkprintf (3, "Checking encoded data.\n");
    if (compression_method >= 0) {
      struct list_decoder* dec = zmalloc_list_decoder_ext (N, K, Li, compression_method, 0, lss_threshold);
      len = W->len;
      P -= len;
      int l = 0;
      while (len > 0) {
        len--;
        if (mode == 0 || in_title (P)) {
          if (mode == 0) {
            int d = dec->decode_int (dec);
            assert (d == P->doc_id);
          } else {
            int d = dec->decode_int (dec);
            assert (d == l);
          }
        }
        P++;
        l++;
      }
      int bits_read = bread_get_bits_read (&dec->br);
      if (bits_written != bits_read) {
        fprintf (stderr, "bits_written = %d, bits_read = %d, a = %d\n", bits_written, bits_read, a);
        assert (bits_written == bits_read);
      }
      zfree_list_decoder (dec);
    }
  }
  return a;
}

static int search_encode_list (pair_t *P, int len, struct search_index_word *W, const int compression_methods[2], int quiet) {
  W->len = len;
  //W->requests = (len >= 0xffff0 ? 0xffff : (len >> 4) + 1);

  W->bytes = W->bytes_subseq = 0;
  assert (len > 0);

  int len_sub = 0;
  while (len > 0) {
    len--;
    if (in_title (P)) {
      len_sub++;
    }
    P++;
  }

  len = W->len;
  P-=len;
  W->len_subseq = len_sub;
  int res = 0;
  res += search_encode_core (P, len, len, W, 0, compression_methods[0], quiet);
  res += search_encode_core (P, len, len_sub, W, 1, compression_methods[1], quiet);
  return res;
}

static int search_encode_lists (const int methods[2], int quiet) {
  encode_lists_reset ();
  int i = 0, j, k, hl = 0;
  for (k = 0; k < PC; k = j) {
    hash_t word = P[k].word;
    for (j = k + 1; j < PC && P[j].word == word; j++) {}
    if (j - k > 1) {
      assert (i < Header.words);
      W[i].word = word;
      search_encode_list (P+k, j-k, W+i, methods, quiet);
      i++;
    } else {
      if (hl >= Header.hapax_legomena) { vkprintf (1, "h1 = %d, Header.hapax_legomena = %d\n", hl, Header.hapax_legomena); }
      assert (hl < Header.hapax_legomena);
      HL[hl].word = word;
      HL[hl].doc_id_and_priority = P[k].doc_id;
      if (in_title (P + k)) {
        HL[hl].doc_id_and_priority |= (1U << 31);
      }
      hl++;
    }
  }
  assert (i == Header.words);
  assert (hl == Header.hapax_legomena);
  memset (HL+hl, 0, sizeof (struct search_index_hapax_legomena));
  for (k=0;k<2;k++) {
    Header.compression_bytes[2*methods[k]+k] = encoded_bytes[k];
  }
  return i;
}

#define NCOMPRESSION_METHODS 3

void search_estimate_compression_method (int c[2]) {
  memset (c, 0, sizeof(int) * 2);
  if (compression_speed < 0) {
    c[0] = c[1] = le_interpolative_ext;
    return;
  }
  if (!compression_speed) {
    return;
  }

  int method;
  int methods[2];
  for (method = 0; method < NCOMPRESSION_METHODS; method++) {
    methods[0] = methods[1] = method;
    double t = -get_utime(CLOCK_MONOTONIC);
    assert (!get_compression_bytes (method));
    search_encode_lists (methods, 1);
    if ((cur_offs - word_index_offset) != get_compression_bytes (method)) {
      kprintf ("cur_offs - word_index_offset == %lld\n", cur_offs - word_index_offset);
      kprintf ("get_compression_bytes (%d) = %lld\n", method, get_compression_bytes (method));
      assert (0);
    }
    t += get_utime (CLOCK_MONOTONIC);
    if (verbosity > 0) {
      fprintf (stderr, "Compression method: %s, %lld(%.6lf%%) = %lld(%.6lf%%)+%lld(%.6lf%%) bytes in %d lists (%.6lf seconds).\nMax encoded list bytes = %d.\n",
      list_get_compression_method_description (method),
      get_compression_bytes (method), percent (get_compression_bytes (method), get_compression_bytes (0)),
      encoded_bytes[0], percent (encoded_bytes[0], Header.compression_bytes[0]),
      encoded_bytes[1], percent (encoded_bytes[1], Header.compression_bytes[1]),
      lists_encoded, t,
      (max_encoded_list_size[0] > max_encoded_list_size[1]) ? max_encoded_list_size[0] : max_encoded_list_size[1]);
    }

    if (method) {
      int k;
      for (k = 0; k < 2; k++) {
        double p = percent (encoded_bytes[k], Header.compression_bytes[c[k] * 2 + k]);
        switch (method) {
          case 1:
            if (p < 100.0) { c[k] = method; }
            break;
          case 2:
            if (p < interpolative_percent) { c[k] = method; }
            break;
        }
      }
    }
  }

  if (verbosity > 0) {
    fprintf (stderr, "Choose %s+%s coding.\n",
             list_get_compression_method_description (c[0]),
             list_get_compression_method_description (c[1]));
  }
  return;
}

static void search_optimal_encode_lists (void) {
  int compression_methods[2];
  search_estimate_compression_method (compression_methods);
  lists_encoded = 0;
  memcpy (Header.word_list_compression_methods, compression_methods, 8);

  lseek (fd[0], word_index_offset, SEEK_SET);
  int i = search_encode_lists (compression_methods, 0);
  CRC32_Header.crc32_data = ~idx_crc32_complement;
  memset (W+i, 0, sizeof (struct search_index_word));
  W[i].file_offset = cur_offs;
  flushout ();
  if (verbosity > 0) {
    kprintf ("wrote %lld codes bytes in %d lists\n",
      cur_offs - word_index_offset, lists_encoded);
    if (compression_speed < 0) {
      kprintf ("wrote %lld(%.6lf%%) interpolative redundant bits.\n", redundant_bits, percent (redundant_bits, interpolative_ext_bits));
    }
  }
}
#endif

static void build_word_lists (void) {
  int i;
  Header.words = count_words();
  Header.hapax_legomena = hapax_legomena;
  /*
  Header.frequent_words = Header.words < MAX_FREQUENT_WORDS ? 0 : MAX_FREQUENT_WORDS;
  */
  Header.frequent_words = 0;
  item_texts_offset = sizeof (Header) + sizeof (CRC32_Header) + Header.index_items_size;
  words_offset = item_texts_offset + Header.item_texts_size;
  hapax_legomena_offset = words_offset + (Header.words + 1) * sizeof (struct search_index_word);
  freq_words_offset = hapax_legomena_offset + (Header.hapax_legomena + 1) * sizeof (struct search_index_hapax_legomena);
  word_index_offset = freq_words_offset + (Header.frequent_words + 1) * sizeof (struct search_index_word);

  clearin ();

  W = ztmalloc ((Header.words + 1) * sizeof (struct search_index_word));
  HL = ztmalloc ((Header.hapax_legomena + 1) * sizeof (struct search_index_hapax_legomena));

  long MaxL = dyn_free_bytes () >> 1;
  if (MaxL < 1000000) {
    MaxL = 1000000;
  }
  Li = zmalloc (MaxL);
  Le = Li + MaxL;

#ifdef SEARCHX
  searchx_encode_lists ();
#else
  search_optimal_encode_lists ();
#endif

  lseek (fd[0], words_offset, SEEK_SET);
  i = (Header.words + 1) * sizeof (struct search_index_word);
  assert (write (fd[0], W, i) == i);
  CRC32_Header.crc32_words = compute_crc32 (W, i);
  vkprintf (1, "wrote %d word description bytes for %d words\n", i, Header.words);
  i = (Header.hapax_legomena + 1) * sizeof (struct search_index_hapax_legomena);
  assert (write (fd[0], HL, i) == i);
  CRC32_Header.crc32_hapax_legomena = compute_crc32 (HL, i);
  vkprintf (1, "wrote %d word description bytes for %d hapax legomena\n", i, Header.hapax_legomena);

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
	  "\t-W\tenable storing words frequencies in item\n"
    "\t-D\tstore in item's date modification time (default: first creation time)\n"
    "\t-H<heap-size>\tsets maximal heap size, e.g. 4g\n"
    "\t-B<heap-size>\tsets maximal binlog size, e.g. 4g\n"
    "\t-P<max_pairs>\tsets maximal number of pairs\n"
    "\t-v\toutput statistical and debug information into stderr\n"
    "\t-f\tallow to load snapshots without crc32\n"
    "\t-O\tenable tag owner mode\n"
#ifndef SEARCHX
    "\t-Q\toutput hash stats\n"
    "\t-0\tuse only Golomb coding (faster decompression)\n"
    "\t-1\tuse Interpolative coding (slower decompression)\n"
    "\t\t\tif it consumes less than %.6lf%% memory used by Golomb coding\n"
    "\t-2\tuse Interpolative coding if it consumes less memory than Golomb coding\n"
    "\t-x<left subtree size threshold>\tuse Redundant Interpolative coding.\n"
    "\t by default -x%d compression is used.\n"
    "\t-I<filename>\toutput to the given text file all item_ids and exit without saving index\n"
#endif
	  , progname
#ifndef SEARCHX
    , interpolative_percent, lss_threshold
#endif
    );
  exit (2);
}

void dump_itemids (char *filename) {
  int i;
  FILE *f = fopen (filename, "w");
  if (f == NULL) {
    kprintf ("fopen (\"%s\", \"w\") fail. %m\n", filename);
    exit (1);
  }
  sort_items ();
  for (i = 0; i < tot_items; i++) {
    struct item *I = Items[i];
    int t = (int) (I->item_id >> 32);
    if (t) {
      fprintf (f, "%d_%d\n", (int) I->item_id, t);
    } else {
      fprintf (f, "%d\n", (int) I->item_id);
    }
  }
  int fd = fileno (f);
  assert (fd >= 0);
  assert (!fsync (fd));
  assert (!fclose (f));
}

void save_index (void) {
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

  flushout ();

  assert (fsync(fd[0]) >= 0);
  assert (close (fd[0]) >= 0);

  if (rename_temporary_snapshot (newidxname)) {
    kprintf ("cannot rename new index file from %s: %m\n", newidxname);
    unlink (newidxname);
    exit (1);
  }

  print_snapshot_name (newidxname);
}

static const char *options = "AB:H:I:OP:SWa:dfhl:tu:vU"
#ifndef SEARCHX
  "Q0123x:"
#endif
;

int main (int argc, char *argv[]) {
  int i;
  char c;
  long long x;

  set_debug_handlers();
  dynamic_data_buffer_size = DYNAMIC_DATA_BIG_BUFFER_SIZE;

  progname = argv[0];
  while ((i = getopt (argc, argv, options)) != -1) {
    switch (i) {
    case 'O':
      tag_owner = 1;
      break;
    case 'I':
      itemids_filename = optarg;
      break;
    case 'W':
      wordfreqs_enabled = 1;
      break;
    case 'f':
      load_snapshot_without_crc32 = 1;
      break;
    case 'x':
      compression_speed = -1;
      if (sscanf (optarg, "%lld", &x) == 1) {
        if (x < 16) {
          x = 16;
        }
        if (x <= INT_MAX) {
          lss_threshold = x;
        }
      }
      break;
    case '0':
      compression_speed = 0;
      break;
    case '1':
      compression_speed = 1;
      break;
    case '2':
      compression_speed = 1;
      interpolative_percent = 100.0;
      break;
    case '3': /* debug */
      compression_speed = 1;
      interpolative_percent = 1000.0;
      break;
    case 'Q':
      hash_stats++;
      break;
    case 'v':
      verbosity++;
      break;
    case 'h':
      usage();
      return 2;
    case 'u':
      username = optarg;
      break;
    case 'a':
      binlogname = optarg;
      break;
    case 'l':
      logname = optarg;
      break;
    case 'A':
      universal = 1;
      break;
    case 'D':
      creation_date = 0;
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
    case 'd':
      daemonize ^= 1;
       break;
    case 'P':
      if (sscanf (optarg, "%lld", &x) == 1) {
        if (x > 0 && x <= INT_MAX) { max_pairs = x; }
      }
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

  binlog_load_time = get_utime(CLOCK_MONOTONIC);

  clear_log();

  init_log_data (jump_log_pos, jump_log_ts, jump_log_crc32);

  replay_logevent = search_replay_logevent;
  i = replay_log (0, 1);

  binlog_load_time = get_utime(CLOCK_MONOTONIC) - binlog_load_time;

  binlog_loaded_size = log_readto_pos;

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

  if (itemids_filename) {
    dump_itemids (itemids_filename);
    exit (0);
  }

  save_index ();

  return 0;
}
