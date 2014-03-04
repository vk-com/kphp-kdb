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
              2011-2013 Anton Maydell
*/

#define _FILE_OFFSET_BITS 64
#define SEARCHX

#include <assert.h>
#include <stddef.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <ctype.h>
#include <math.h>
#include <unistd.h>
#include <limits.h>
#include <fcntl.h>
#include <sys/types.h>
#include <aio.h>

#include "kdb-binlog-common.h"
#include "kdb-data-common.h"
#include "kdb-search-binlog.h"
#include "search-index-layout.h"
#include "search-common.h"
#include "search-x-data.h"
#include "server-functions.h"
#include "word-split.h"
#include "stemmer-new.h"
#include "utils.h"
#include "crc32.h"

extern int now;
extern int verbosity;

#define MIN_ITEM_ID (-1LL << 63)
#define MAX_ITEM_ID (~MIN_ITEM_ID)

/* --------- search data ------------- */
static int items_cleared;
static const int items_short_ids = 0;

int tot_items, del_items, del_item_instances, mod_items, tot_freed_deleted_items, idx_items_with_hash;
long long rebuild_hashmap_calls = 0;

char last_search_query[LAST_SEARCH_QUERY_BUFF_SIZE];

struct item *Items[ITEMS_HASH_PRIME];
tree_t *Root;

int search_replay_logevent (struct lev_generic *E, int size);
static void free_tree (tree_t *T);

/*                                   0000000000111111 */
/*                                   0123456789012345 */
const char* rate_first_characters = "rsdbcfghklmn"; //Do not change position of RATE, SATE, DATE
static int tbl_sorting_mode[128];
static void init_tbl_sorting_mode () {
  const char *p = rate_first_characters;
  int i;
  for (i = 0; i < 128; i++) {
    tbl_sorting_mode[i] = -1;
  }
/*
  assert (strchr(rate_first_characters, 'i') == 0);
  tbl_sorting_mode['i'] = MAX_RATES;
  tbl_sorting_mode['I'] = MAX_RATES | FLAG_REVERSE_SEARCH;
*/
  i = 0;
  while (*p) {
    assert ('a' <= *p && *p <= 'z');
    tbl_sorting_mode[(int) *p] = i;
    tbl_sorting_mode[(int) (*p - 32)] = i | FLAG_REVERSE_SEARCH;
    i++;
    p++;
  }
}

int get_sorting_mode (int c) {
  if (c < 0 || c >= 128) return -1;
  return tbl_sorting_mode[c];
}

int init_search_data (int schema) {
  last_search_query[0] = 0;
  init_tbl_sorting_mode ();

  replay_logevent = search_replay_logevent;

  if (items_cleared++) {
    memset (Items, 0, sizeof (Items));
  }

  free_tree (Root);
  Root = 0;

  return 0;
}

/* --------- LOAD INDEX -------- */

#define MAX_INDEX_ITEMS (48 << 20)
#define MAX_INDEX_WORDS (1 << 25)
#define MAX_INDEX_TEXTS (1LL << 36)
#define MAX_INDEX_BYTES (1 << 30)

long long jump_log_pos;
int jump_log_ts;
unsigned jump_log_crc32;

struct search_index_header Header;
struct search_index_crc32_header CRC32_Header;

static struct index_item *IndexItems;
static struct search_index_word *IndexWords;
static struct search_index_hapax_legomena *IndexHapaxLegomena;
static char *IndexData;
int idx_items, idx_words, idx_hapax_legomena;
long idx_bytes, idx_loaded_bytes;

static long long item_texts_offset, words_offset, hapax_legomena_offset, freq_words_offset, word_index_offset, index_size;

long long get_hash_item_unsafe (const struct item *I) {
  return (((unsigned long long) I->rates[I->rates_len-1]) << 32) | ((unsigned int) I->rates[I->rates_len-2]);
}

long long get_hash_item (const item_t *I) {
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

int set_hash_item (item_t *I, long long hc) {
  vkprintf (3, "set_hash_item : %016llx\n", hc);

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
  I->rates[I->rates_len-1] = (unsigned int) (hc >> 32);
  I->rates[I->rates_len-2] = (unsigned int) hc;
  return 1;
}

int load_item (struct index_item *C) {
  vkprintf (5, "loading item...");
  bread (&C->item_id, 8);
  /* read mask, rates_len, extra */
  bread (&C->mask, 2);
  bread (&C->rates_len, 1);
  bread (&C->extra, 1);
  int sz = ((int) C->rates_len) * sizeof (int);
  C->rates = zmalloc (sz);
  bread (C->rates, sz);
  assert (popcount_short (C->mask) == C->rates_len);
  if (C->mask & 0xc000) {
    idx_items_with_hash++;
  }
  return sz + 12;
}

int load_index (kfs_file_handle_t Index) {
  int fd = Index->fd;
  long long fsize = Index->info->file_size;

  long r, s;
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

  if (Header.magic != SEARCHX_INDEX_MAGIC) {
    kprintf ("bad index file header\n");
    return -4;
  }

  if (!check_header (&Header)) {
    return -7;
  }

  const int sz_headers = sizeof (Header) + sizeof (CRC32_Header);
  sz = sizeof (CRC32_Header);
  if (sz != read (fd, &CRC32_Header, sz)) {
    kprintf ("error reading index (crc32_header). %m\n");
    return -5;
  }
  assert (CRC32_Header.crc32_header == compute_crc32 (&Header, sizeof (Header)));

  assert ((unsigned) Header.items <= MAX_INDEX_ITEMS);
  assert ((unsigned) Header.words <= MAX_INDEX_WORDS);
  assert ((unsigned) Header.frequent_words <= (unsigned) Header.words);
  assert ((unsigned long long) Header.item_texts_size < MAX_INDEX_TEXTS);

  item_texts_offset = sz_headers + Header.index_items_size;
  words_offset = item_texts_offset + Header.item_texts_size;
  hapax_legomena_offset = words_offset + (Header.words + 1) * sizeof (struct search_index_word);
  freq_words_offset = hapax_legomena_offset + (Header.hapax_legomena + 1) * sizeof (struct search_index_hapax_legomena);
  word_index_offset = freq_words_offset + (Header.frequent_words + 1) * sizeof (struct search_index_word);
  index_size = fsize;

  if (word_index_offset > fsize) {
    kprintf ("fsize = %lld, word_index_offset = %lld\n", fsize, word_index_offset);
  }
  //assert (word_index_offset <= fsize);

  idx_loaded_bytes = sz_headers;
  /* we never freed this array, so we use zmalloc instead of zzmalloc */
  IndexItems = zmalloc0 (sizeof (struct index_item) * (Header.items + 1));
  set_read_file (fd);
  int rbytes = 0;
  for (i = 0; i < Header.items; i++) {
    int t = load_item (IndexItems + i);
    if (t < 0) { return -2;}
    rbytes += t;
  }
  assert (rbytes == Header.index_items_size);
  assert (CRC32_Header.crc32_items == ~idx_crc32_complement);
  /* this barrier is needed for list_interpolative_ext_forward_decode_item */
  IndexItems[Header.items].item_id = MAX_ITEM_ID;
  idx_crc32_complement = -1;

  vkprintf (4, "rbytes = %d, Header.index_items_size = %d\n", rbytes, (int) Header.index_items_size);
  idx_loaded_bytes += rbytes;
  vkprintf (1, "%lld bytes for %d items read from index\n", Header.index_items_size, Header.items);

  assert (lseek (fd, words_offset, SEEK_SET) >= 0);

  s = (Header.words + 1) * 1LL * sizeof (struct search_index_word);
  /* we never freed this array, so we use zmalloc instead of zzmalloc */
  IndexWords = zmalloc (s);

  r = read (fd, IndexWords, s);
  if (r > 0) {
    idx_loaded_bytes += r;
  }
  if (r < s) {
    kprintf ("error reading words from index file: read %ld bytes instead of %ld at position %lld: %m\n", r, s, words_offset);
    return -2;
  } else {
    vkprintf (1, "%ld bytes for %d words read from index\n", r, Header.words);
  }
  assert (CRC32_Header.crc32_words == compute_crc32 (IndexWords, s));

  s = (Header.hapax_legomena + 1) * 1LL * sizeof (struct search_index_hapax_legomena);
  IndexHapaxLegomena = zmalloc (s);
  r = read (fd, IndexHapaxLegomena, s);
  if (r > 0) {
    idx_loaded_bytes += r;
  }
  if (r < s) {
    kprintf ("error reading words from index file: read %ld bytes instead of %ld at position %lld: %m\n", r, s, hapax_legomena_offset);
    return -2;
  } else {
    vkprintf (1, "%ld bytes for %d hapax_legomena read from index\n", r, Header.hapax_legomena);
  }
  assert (CRC32_Header.crc32_hapax_legomena == compute_crc32 (IndexHapaxLegomena, s));

  idx_items = Header.items;
  idx_words = Header.words;
  idx_hapax_legomena = Header.hapax_legomena;
  idx_bytes = 0;

  if (!idx_items || !idx_words) {
    return 0;
  }

  assert (IndexItems[0].item_id > 0);
  for (i = 1; i < idx_items; i++) {
    assert (IndexItems[i].item_id > IndexItems[i-1].item_id);
  }

  /*
     for (i = 0; i <= idx_words; i++) {
     fprintf (stderr, "word #%d:\t%016llx\t%016llx\t%d\t%d\t%d\n", i, IndexWords[i].word, IndexWords[i].file_offset, IndexWords[i].len, IndexWords[i].bytes, IndexWords[i].requests);
     }
   */

  for (i = 1; i < idx_words; i++) {
    if (verbosity >= 2 && IndexWords[i].word <= IndexWords[i-1].word) {
      kprintf ("Error at i=%d: IndexWords[i].word = %lld <= IndexWords[i-1].word %lld, ", i, IndexWords[i].word , IndexWords[i-1].word);
    }
    //if (verbosity >= 3) { fprintf (stderr, "%lld,", IndexWords[i-1].word); }
    assert (IndexWords[i].word > IndexWords[i-1].word);
  }

  for (i = 1; i < idx_hapax_legomena; i++) {
    assert (IndexHapaxLegomena[i].word > IndexHapaxLegomena[i-1].word);
  }

  assert ((IndexWords[idx_words].file_offset >= word_index_offset && IndexWords[idx_words].file_offset <= fsize) || IndexWords[idx_words].bytes <= 8);

  idx_bytes = IndexWords[idx_words].file_offset - word_index_offset;
  //assert (idx_bytes <= MAX_INDEX_BYTES);

  s = idx_bytes;
  /* we never freed this array, so we use zmalloc instead of zzmalloc */
  IndexData = zmalloc (s);
  vkprintf (2, "cut golomb data cmd: head -c %lld %s | tail -c %lld >output\n",
          (long long) word_index_offset + s, Index->info->filename,  (long long) s);
  assert (lseek (fd, word_index_offset, SEEK_SET) >= 0);
  r = read (fd, IndexData, s);
  idx_loaded_bytes += r;
  if (r < s) {
    kprintf ("error reading data from index file: read %ld bytes instead of %ld at position %lld: %m\n", r, s, word_index_offset);
    return -2;
  }
  assert (CRC32_Header.crc32_data == compute_crc32 (IndexData, s));

  //s = 4 * (idx_items + 1);
  clearin ();
  for (i = 0; i < idx_items; i++) {
    bread (&IndexItems[i].sum_sqr_freq_title, 4);
    bread (&IndexItems[i].sum_freq_title_freq_text, 4);
    bread (&IndexItems[i].sum_sqr_freq_text, 4);
  }
  unsigned computed_crc32_freqs = ~idx_crc32_complement;
  unsigned crc32_freqs;
  bread (&crc32_freqs, 4);
  assert (crc32_freqs == computed_crc32_freqs);

  vkprintf (1, "finished loading index: %d items, %d words, %ld index bytes, %ld preloaded bytes\n", idx_items, idx_words, idx_bytes, idx_loaded_bytes);

  log_split_min = Header.log_split_min;
  log_split_max = Header.log_split_max;
  log_split_mod = Header.log_split_mod;
  assert (log_split_mod > 0 && log_split_min >= 0 && log_split_min + 1 ==  log_split_max && log_split_max <= log_split_mod);

  log_schema = SEARCH_SCHEMA_V1;
  init_search_data (log_schema);

  replay_logevent = search_replay_logevent;

  jump_log_ts = Header.log_timestamp;
  jump_log_pos = Header.log_pos1;
  jump_log_crc32 = Header.log_pos1_crc32;

  close_snapshot (Index, fd);

  return 0;
}

static struct index_item *get_idx_item (long long item_id) {
  int a = -1, b = idx_items, c;
  while (b - a > 1) {
    c = ((a + b) >> 1);
    if (IndexItems[c].item_id <= item_id) { a = c; } else { b = c; }
  }
  if (a >= 0 && IndexItems[a].item_id == item_id && !(IndexItems[a].extra & FLAG_DELETED)) {
    return IndexItems + a;
  } else {
    return 0;
  }
};

/* ---------- tree functions ------------ */

static int cur_wordlist_size;
static struct tree *cur_wordlist_head;

int alloc_tree_nodes, free_tree_nodes;
static tree_t free_tree_head = {.left = &free_tree_head, .right = &free_tree_head};

static tree_t *new_tree_node (int y, unsigned freqs) {
  tree_t *P;
  if (free_tree_nodes) {
    assert (--free_tree_nodes >= 0);
    P = free_tree_head.right;
    assert (P != &free_tree_head && P->left == &free_tree_head);
    P->right->left = &free_tree_head;
    free_tree_head.right = P->right;
  } else {
    P = zmalloc (sizeof (tree_t));
    assert (P);
    alloc_tree_nodes++;
  }

  P->left = P->right = 0;
  P->y = y;
  P->freq_title = freqs >> 16;
  P->freq_text = freqs & 0xffff;
  P->next = cur_wordlist_head;
  cur_wordlist_head = P;
  return P;
}

static tree_t *tree_lookup (tree_t *T, hash_t word, item_t *item) {
  while (T) {
    if (word < T->word) {
      T = T->left;
    } else if (word > T->word) {
      T = T->right;
    } else if (item->item_id < T->item->item_id) {
      T = T->left;
    } else if (item->item_id > T->item->item_id) {
      T = T->right;
    } else {
      return T;
    }
  }
  return T;
}

static void tree_split (tree_t **L, tree_t **R, tree_t *T, hash_t word, item_t *item) {
  if (!T) { *L = *R = 0; return; }
  if (word < T->word || (word == T->word && item->item_id < T->item->item_id)) {
    *R = T;
    tree_split (L, &T->left, T->left, word, item);
  } else {
    *L = T;
    tree_split (&T->right, R, T->right, word, item);
  }
}

static tree_t *tree_insert (tree_t *T, hash_t word, item_t *item, int y, unsigned freqs) {
  tree_t *P;
  if (!T) {
    P = new_tree_node (y, freqs);
    P->word = word;
    P->item = item;
    return P;
  }
  if (T->y >= y) {
    if (word < T->word || (word == T->word && item->item_id < T->item->item_id)) {
      T->left = tree_insert (T->left, word, item, y, freqs);
    } else {
      T->right = tree_insert (T->right, word, item, y, freqs);
    }
    return T;
  }
  P = new_tree_node (y, freqs);
  P->word = word;
  P->item = item;
  tree_split (&P->left, &P->right, T, word, item);
  return P;
}

static int tree_depth (tree_t *T, int d) {
  if (!T) { return d; }
  int u = tree_depth (T->left, d+1);
  int v = tree_depth (T->right, d+1);
  return (u > v ? u : v);
}

static void free_tree_node (tree_t *T) {
  (T->right = free_tree_head.right)->left = T;
  free_tree_head.right = T;
  T->left = &free_tree_head;
  free_tree_nodes++;
}

static tree_t *tree_delete (tree_t *T, hash_t word, item_t *item) {
  tree_t *Root = T, **U = &Root, *L, *R;
  const long long item_id = item->item_id;
  while (word != T->word || item_id != T->item->item_id) {
    U = (word < T->word || (word == T->word && item_id < T->item->item_id)) ? &T->left : &T->right;
    T = *U;
    if (!T) { return Root; }
  }

  L = T->left;
  R = T->right;
  free_tree_node (T);
  while (L && R) {
    if (L->y > R->y) {
      *U = L;
      U = &L->right;
      L = *U;
    } else {
      *U = R;
      U = &R->left;
      R = *U;
    }
  }
  *U = L ? L : R;
  return Root;
}

static void free_tree (tree_t *T) {
  if (T) {
    free_tree (T->left);
    free_tree (T->right);
    free_tree_node (T);
  }
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

typedef enum {
  ONLY_FIND, ADD_NOT_FOUND_ITEM
} get_item_f_mode;

static struct item *get_item_f (long long item_id, get_item_f_mode force) {
  int h1, h2;
  struct item *C, *D;
  if (item_id <= 0) { return 0; }
  if (items_short_ids) {
    h2 = SHORT_ID(item_id);
    h1 = h2 % ITEMS_HASH_PRIME;
    h2 = 1 + (h2 % (ITEMS_HASH_PRIME - 1));
  } else {
    h1 = item_id % ITEMS_HASH_PRIME;
    h2 = 1 + (item_id % (ITEMS_HASH_PRIME - 1));
  }

  switch (force) {
    case ONLY_FIND:
      while ((D = Items[h1]) != 0) {
        if (D->item_id == item_id) {
          return D;
        }
        h1 += h2;
        if (h1 >= ITEMS_HASH_PRIME) { h1 -= ITEMS_HASH_PRIME; }
      }
      return 0;
    case ADD_NOT_FOUND_ITEM:
      while ((D = Items[h1]) != 0) {
        if (ITEM_DELETED(D)) {
          break;
        }
        h1 += h2;
        if (h1 >= ITEMS_HASH_PRIME) { h1 -= ITEMS_HASH_PRIME; }
      }
    if (tot_items >= MAX_ITEMS) { return 0; }
    C = zmalloc0 (sizeof (struct item));
    if (!C) { return C; }
    if (D) {
      zfree (D, sizeof (struct item));
      tot_freed_deleted_items++;
    }
    else { tot_items++; }
    Items[h1] = C;
    C->item_id = item_id;
    return C;
  }
  assert (0);
  return 0;
}

/*
 * replay log
 */


static void clear_cur_wordlist (void) {
  cur_wordlist_size = 0;
  cur_wordlist_head = 0;
}

static void item_clear_wordlist (struct item *I) {
  struct tree *p = I->words;
  while (p) {
    tree_t *w = p->next;
    Root = tree_delete (Root, p->word, I);
    p = w;
  }
  I->words = 0;
}

static void item_add_word (struct item *I, hash_t word, unsigned freqs) {
  tree_t *T = tree_lookup (Root, word, I);
  if (!T) {
    int y = lrand48 ();
    Root = tree_insert (Root, word, I, y, freqs);
  } else  {
    assert (T->item == I);
  }
}

static int set_multiple_rates_item (struct item *I, int mask, int *rates) {
  if (!I || (I->extra & FLAG_DELETED)) { return 0; }
  int i = 0, j = 0, u = mask, deleted_mask = 0, x, new_mask;
  while (u) {
    if (rates[i]) {
      rates[j++] = rates[i];
      u &= u - 1;
    } else {
      u ^= x = u & -u;
      deleted_mask |= x;
    }
    i++;
  }
  mask &= ~deleted_mask;
  new_mask = (I->mask & (~deleted_mask)) | mask;
  if (new_mask != I->mask) {
    I->rates = zzrealloc_ushort_mask (I->rates, I->mask, new_mask, sizeof (int));
    I->mask = new_mask;
  }
  i = 0;
  u = mask;
  while (u) {
    u ^= x = u & -u;
    I->rates[popcount_short (new_mask & (x-1))] = rates[i++];
  }

  assert (i == j);
  I->rates_len = popcount_short (I->mask);
  return 1;
}

static int set_rate_item (struct item *I, int p, int rate) {
  if (!I || (I->extra & FLAG_DELETED)) { return 0; }

  if (!rate) {
    /* memory optimization: don't store zero rates into heap memory */
    if (I->mask & (1 << p)) {
      I->rates = zzrealloc_ushort_mask (I->rates, I->mask, I->mask ^ (1 << p), sizeof (int));
      I->mask ^= (1 << p);
      I->rates_len--;
    }
    return 1;
  }

  if ((I->mask & (1 << p)) == 0) {
    I->rates = zzrealloc_ushort_mask (I->rates, I->mask, I->mask | (1 << p), sizeof (int));
    I->mask |= (1 << p);
    I->rates_len++;
  }
  I->rates[get_bitno (I->mask, p)] = rate;
  return 1;
}

static inline int get_rate_item_fast (struct item *I, int p) {
  int i = get_bitno (I->mask, p);
  return i >= 0 ? I->rates[i] : 0;
}

static int get_rate_item (struct item *I, int p) {
  if (!I || (I->extra & FLAG_DELETED)) { return 0;}
  int i = get_bitno (I->mask, p);
  return i >= 0 ? I->rates[i] : 0;
}

static void delete_item_rates (struct item *I) {
  zfree (I->rates, I->rates_len * 4);
  I->rates = 0;
  I->rates_len = 0;
  I->mask = 0;
  I->extra |= FLAG_DELETED;
}

static void move_item_rates (struct item *dst, struct index_item *src) {
  dst->rates = src->rates;
  dst->rates_len = src->rates_len;
  dst->mask = src->mask;

  src->rates = 0;
  src->rates_len = 0;
  src->mask = 0;
}

static pair_word_freqs_t Q[65536];

static int change_item (const char *text, int len, long long item_id, int rate, int rate2) {
  struct item *I;
  struct index_item *II;

  assert (text && len >= 0 && len < 65536 && !text[len]);
  assert (item_id > 0);

  if (!fits (item_id)) {
    return 0;
  }

  vkprintf (4, "change_item: text=%s, len = %d, item_id = %016llx, rate = %d, rate2 = %d\n",
    text, len, item_id, rate, rate2);

  II = get_idx_item (item_id);
  if (II) {
    mod_items++;
    II->extra |= FLAG_DELETED;
  }

  I = get_item_f (item_id, ONLY_FIND);
  if (I) {
    if (I->extra & FLAG_DELETED) {
      del_items--;
      I->extra ^= FLAG_DELETED;
    }
    item_clear_wordlist (I);
  } else {
    I = get_item_f (item_id, ADD_NOT_FOUND_ITEM);
    if (!I) {
      return 0;
    }
  }

  if (II) {
    move_item_rates (I, II);
  }

  int rates[3];
  rates[0] = rate;
  rates[1] = rate2;
  rates[2] = now;
  set_multiple_rates_item (I, 1 + 2 + 4, rates);

  /*
  set_rate_item (I, 0, rate);
  set_rate_item (I, 1, rate2);
  set_rate_item (I, 2, now);
  */

  clear_cur_wordlist ();
  int i, Wc = extract_words (text, len, universal, Q, 65536, tag_owner, item_id);
  for (i = 0; i < Wc; i++) {
    item_add_word (I, Q[i].word, Q[i].freqs);
  }
  evaluate_freq_sqr_sums (Q, Wc, &I->sum_sqr_freq_title, &I->sum_freq_title_freq_text, &I->sum_sqr_freq_text);
  I->words = cur_wordlist_head;
  return 1;
}

static int delete_item (long long item_id) {
  struct item *I = get_item_f (item_id, ONLY_FIND);
  struct index_item *II = get_idx_item (item_id);
  if (II) {
    II->extra |= FLAG_DELETED;
    mod_items++;
    assert (!I);
    return 1;
  }
  if (!I || (I->extra & FLAG_DELETED)) { return 0; }
  delete_item_rates (I);
  item_clear_wordlist (I);
  del_items++;
  del_item_instances++;
  return 1;
}

int get_hash (long long *hash, long long item_id) {
  struct item *I = get_item_f (item_id, ONLY_FIND);
  struct item *II = (struct item*) get_idx_item (item_id);

  vkprintf (2, "get_hash(%016llx): %p %p\n", item_id, I, II);

  if (II) {
    assert (!I);
    *hash = get_hash_item (II);
    return 1;
  }

  if (!I || (I->extra & FLAG_DELETED)) {
    return 0;
  }
  *hash = get_hash_item (I);
  return 1;
}

int get_single_rate (int *rate, long long item_id, int p) {
  struct item *I = get_item_f (item_id, ONLY_FIND);
  struct item *II = (struct item*) get_idx_item (item_id);

  vkprintf (2, "get_single_rate(%016llx): %p %p\n", item_id, I, II);

  if (II) {
    assert (!I);
    *rate = get_rate_item (II, p);
    return 1;
  }

  if (!I || (I->extra & FLAG_DELETED)) {
    return 0;
  }
  *rate = get_rate_item (I, p);
  return 1;
}

static int set_rate_new (long long item_id, int p, int rate) {
  vkprintf (4, "set_rate_new(%016llx), p = %d, rate = %d\n", item_id, p, rate);

  struct item *I = get_item_f (item_id, ONLY_FIND);
  struct item *II = (struct item*) get_idx_item (item_id);

  if (II) {
    set_rate_item (II, p, rate);
    assert (!I);
    return 1;
  }

  if (!I || (I->extra & FLAG_DELETED)) {
    return 0;
  }
  set_rate_item (I, p, rate);
  return 1;
}

static int set_hash (long long item_id, long long hash) {
  struct item *I = get_item_f (item_id, ONLY_FIND);
  struct item *II = (struct item*) get_idx_item (item_id);
  if (II) {
    set_hash_item (II, hash);
    assert (!I);
    return 1;
  }
  if (!I || (I->extra & FLAG_DELETED)) {
    return 0;
  }
  set_hash_item (I, hash);
  return 1;
}

static int set_rate (long long item_id, int rate) {
  return set_rate_new (item_id, 0, rate);
}

static int set_rate2 (long long item_id, int rate2) {
  return set_rate_new (item_id, 1, rate2);
}

static int set_rates (long long item_id, int rate, int rate2) {
  if (!set_rate_new (item_id, 0, rate)) { return 0;}
  if (!set_rate_new (item_id, 1, rate2)) { return 0;}
  return 1;
}

static int incr_rate_item (struct item *I, int p, int rate_incr) {
  if ((I->mask & (1 << p)) == 0) {
    I->rates = zzrealloc_ushort_mask (I->rates, I->mask, I->mask | (1 << p), sizeof (int));
    I->mask |= (1 << p);
    I->rates_len++;
  }
  rate_incr += I->rates[get_bitno (I->mask, p)];
  I->rates[get_bitno (I->mask, p)] = rate_incr;
  return 1;
}

static int incr_rate_new (long long item_id, int p, int rate_incr) {
  struct item *I = get_item_f (item_id, ONLY_FIND);
  struct item *II = (struct item*) get_idx_item (item_id);
  if (II) {
    assert (!I);
    return incr_rate_item (II, p, rate_incr);
  }
  if (!I || (I->extra & FLAG_DELETED)) {
    return 0;
  }
  return incr_rate_item (I, p, rate_incr);
}

static int incr_rate (long long item_id, int rate_incr) {
  return incr_rate_new (item_id, 0, rate_incr);
}

static int incr_rate2 (long long item_id, int rate2_incr) {
  return incr_rate_new (item_id, 1, rate2_incr);
}

int search_replay_logevent (struct lev_generic *E, int size) {
  int s;
  switch (E->type) {
    case LEV_START:
      if (size < 24 || E->b < 0 || E->b > 4096) { return -2; }
      s = 24 + ((E->b + 3) & -4);
      if (size < s) { return -2; }
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
    case LEV_SEARCH_RESET_ALL_RATES:
      return (size < 8) ? -2 : 8;
  }

  fprintf (stderr, "unknown log event type %08x at position %lld\n", E->type, log_cur_pos());

  return -3;
}

/* adding new log entries */

int do_delete_item (long long item_id) {
  if (!fits (item_id)) { return 0; }
  struct lev_search_delete_item *ED = alloc_log_event(LEV_SEARCH_DELETE_ITEM, 12, 0);
  assert (ED);
  ED->obj_id = item_id;
  return delete_item (item_id);
}

int do_change_item (const char *text, int len, long long item_id, int rate, int rate2) {
  char *q;
  int i;
  if (len >= 65536 || len < 0 || !fits (item_id)) {
    return 0;
  }
  if (len < 256) {
    struct lev_search_text_short_entry *LS = alloc_log_event (LEV_SEARCH_TEXT_SHORT+len, 21+len, rate);
    LS->rate2 = rate2;
    LS->obj_id = item_id;
    q = LS->text;
  } else {
    struct lev_search_text_long_entry *LL = alloc_log_event (LEV_SEARCH_TEXT_LONG, 23+len, rate);
    LL->rate2 = rate2;
    LL->obj_id = item_id;
    LL->text_len = len;
    q = LL->text;
  }
  i = 0;
  while (i < len) {
    if (text[i] == 0x1f) {
      do {
        *q++ = text[i++];
      } while (i < len && (unsigned char) text[i] >= 0x40);
    } else if ((unsigned char) text[i] < ' ' && text[i] != 9) {
      *q++ = ' ';
      i++;
    } else {
      *q++ = text[i++];
    }
  }
  *q = 0;
  return change_item (q - len, len, item_id, rate, rate2);
}

int do_change_item_long (netbuffer_t *Source, int len, long long item_id, int rate, int rate2) {
  char *q, *qe;
  int i;

  if (len >= 65536 || len < 0 || !fits (item_id)) {
    advance_read_ptr (Source, len);
    return 0;
  }

  if (len < 256) {
    struct lev_search_text_short_entry *LS = alloc_log_event (LEV_SEARCH_TEXT_SHORT+len, 21+len, rate);
    LS->rate2 = rate2;
    LS->obj_id = item_id;
    q = LS->text;
  } else {
    struct lev_search_text_long_entry *LL = alloc_log_event (LEV_SEARCH_TEXT_LONG, 23+len, rate);
    LL->rate2 = rate2;
    LL->obj_id = item_id;
    LL->text_len = len;
    q = LL->text;
  }

  i = read_in (Source, q, len);
  assert (i == len);

  qe = q + i;

  while (q < qe) {
    if (*q == 0x1f) {
      do {
        q++;
      } while (q < qe && (unsigned char) *q >= 0x40);
    } else if ((unsigned char) *q < ' ' && *q != 9) {
      *q++ = ' ';
    } else {
      q++;
    }
  }
  *q = 0;
  return change_item (q - len, len, item_id, rate, rate2);
}

int do_set_rate_new (long long item_id, int p, int rate) {
  if (!fits (item_id)) { return 0; }
  struct lev_search_set_rate_new *LR = alloc_log_event (LEV_SEARCH_SET_RATE_NEW + p, 16, rate);
  LR->obj_id = item_id;
  return set_rate_new (item_id, p, rate);
}

int do_set_rate (long long item_id, int rate) {
  if (!fits (item_id)) { return 0; }
  struct lev_search_set_rate *LR = alloc_log_event (LEV_SEARCH_SET_RATE, 16, rate);
  LR->obj_id = item_id;
  return set_rate (item_id, rate);
}

int do_set_rate2 (long long item_id, int rate2) {
  if (!fits (item_id)) { return 0; }
  struct lev_search_set_rate *LR = alloc_log_event (LEV_SEARCH_SET_RATE2, 16, rate2);
  LR->obj_id = item_id;
  return set_rate2 (item_id, rate2);
}

int do_set_rates (long long item_id, int rate, int rate2) {
  if (!fits (item_id)) { return 0; }
  struct lev_search_set_rates *LR = alloc_log_event (LEV_SEARCH_SET_RATES, 20, rate);
  LR->obj_id = item_id;
  LR->rate2 = rate2;
  return set_rates (item_id, rate, rate2);
}

int do_set_hash (long long item_id, long long hash) {
  if (!fits (item_id)) { return 0; }
  struct lev_search_set_hash *LR = alloc_log_event (LEV_SEARCH_SET_HASH, 20, 0);
  LR->obj_id = item_id;
  LR->hash = hash;
  return set_hash (item_id, hash);
}

int do_incr_rate (long long item_id, int rate_incr) {
  if (!fits (item_id)) { return 0; }
  if (rate_incr == (signed char) rate_incr) {
    struct lev_search_incr_rate_short *IS = alloc_log_event (LEV_SEARCH_INCR_RATE_SHORT + (rate_incr & 0xff), 12, 0);
    IS->obj_id = item_id;
  } else {
    struct lev_search_incr_rate *IL = alloc_log_event (LEV_SEARCH_INCR_RATE, 16, rate_incr);
    IL->obj_id = item_id;
  }
  return incr_rate (item_id, rate_incr);
}

int do_incr_rate2 (long long item_id, int rate2_incr) {
  if (!fits (item_id)) { return 0; }
  if (rate2_incr == (signed char) rate2_incr) {
    struct lev_search_incr_rate_short *IS = alloc_log_event (LEV_SEARCH_INCR_RATE2_SHORT + (rate2_incr & 0xff), 12, 0);
    IS->obj_id = item_id;
  } else {
    struct lev_search_incr_rate *IL = alloc_log_event (LEV_SEARCH_INCR_RATE2, 16, rate2_incr);
    IL->obj_id = item_id;
  }
  return incr_rate2 (item_id, rate2_incr);
}

int do_incr_rate_new (long long item_id, int p, int rate_incr) {
  if (!fits (item_id)) { return 0; }
  struct lev_search_incr_rate *IL = alloc_log_event (LEV_SEARCH_INCR_RATE_NEW + p, 16, rate_incr);
  IL->obj_id = item_id;
  return incr_rate_new(item_id, p, rate_incr);
}

int get_rates (int *rates, long long item_id) {
  struct item *I = get_item_f (item_id, ONLY_FIND);
  struct item *II = (struct item*) get_idx_item (item_id);

  vkprintf (2, "get_rates(%016llx): %p %p\n", item_id, I, II);

  if (II) {
    assert (!I);
    rates[0] = get_rate_item (II, 0);
    rates[1] = get_rate_item (II, 1);
    return 1;
  }
  if (!I || (I->extra & FLAG_DELETED)) {
    return 0;
  }
  rates[0] = get_rate_item (I, 0);
  rates[1] = get_rate_item (I, 1);
  return 1;
}

/*
 *
 *  INDEX LIST DECODER
 *
 */
struct list_decoder *allocated_list_decoders[2*MAX_WORDS];
int Q_decoders = 0;

int get_word_frequency (hash_t word) {
  int a = -1, b = idx_words, c;

  while (b - a > 1) {
    c = ((a + b) >> 1);
    if (IndexWords[c].word <= word) { a = c; } else { b = c; }
  }

  if (a < 0 || IndexWords[a].word != word) {
    return 0;
  }

  return IndexWords[a].len;
}

static void init_decoder (struct searchx_list_decoder *D, int N, int K, int bytes, const void *file_offset) {
  unsigned char *ptr;
  if (bytes <= 8) {
    ptr = (unsigned char *) file_offset;
  } else {
    long long offs;
    memcpy (&offs, file_offset, 8);
    assert (offs >= word_index_offset && offs < index_size);
    assert (offs + bytes <= index_size);
    offs -= word_index_offset;
    assert (offs >= 0 && offs < idx_bytes && offs + bytes <= idx_bytes);
    ptr = (unsigned char *)(IndexData + offs);
  }
  assert (Q_decoders < 2 * MAX_WORDS);
  D->dec = allocated_list_decoders[Q_decoders++] = zmalloc_list_decoder (N, K, ptr, le_golomb, 0);
  D->remaining = K;
  D->len = K;
}

int init_ilist_decoder (ilist_decoder_t *D, hash_t word) {
  /* *D is already cleared,
     since D is a part of already cleared heap entry
  */
  D->doc_id = -1;

  int a = -1, b = idx_words, c;

  while (b - a > 1) {
    c = ((a + b) >> 1);
    if (IndexWords[c].word <= word) { a = c; } else { b = c; }
  }

  if (a < 0 || IndexWords[a].word != word) {
    a = -1;
    b = idx_hapax_legomena;
    while (b - a > 1) {
      c = ((a + b) >> 1);
      if (IndexHapaxLegomena[c].word <= word) { a = c; } else { b = c; }
    }
    if (a >= 0 && IndexHapaxLegomena[a].word == word) {
      unsigned int u = IndexHapaxLegomena[a].doc_id_and_priority;
      int doc_id = u & 0x7fffffff;
      unsigned freq1, freq2;
      if (u & 0x80000000) {
        //hapax legomena in title
        freq1 = 2;
        freq2 = 1;
      } else {
        //hapax legomena in text
        freq1 = 1;
        freq2 = 1;
      }
      void *Li = &D->extra, *Le = Li + 8;

      struct list_encoder enc;
      list_encoder_init (&enc, idx_items, 1, Li, Le, le_golomb, 0);
      enc.encode_int (&enc, doc_id);
      bwrite_gamma_code (&enc.bw, freq1);
      bwrite_gamma_code (&enc.bw, freq2);
      list_encoder_finish (&enc);
      return 1;
    }
    return 0;
  }
  D->sword = IndexWords + a;
  init_decoder (&D->dec, idx_items, D->sword->len, D->sword->bytes, &D->sword->file_offset);
  return 1;
}

static void free_all_list_decoders (void) {
  int i;
  for (i = 0; i < Q_decoders; i++) { zfree_list_decoder (allocated_list_decoders[i]); }
  Q_decoders = 0;
}

static int adv_ilist (ilist_decoder_t *D) {
  struct index_item *II;
  while (D->dec.remaining > 0) {
    struct list_decoder *dec = D->dec.dec;
    D->dec.remaining--;
    D->doc_id = dec->decode_int (D->dec.dec);
    assert (D->doc_id >= 0 && D->doc_id < idx_items);
    unsigned freq1 = bread_gamma_code (&dec->br) - 1;
    unsigned freq2 = bread_gamma_code (&dec->br) - 1;
    II = IndexItems + D->doc_id;
    if (!(II->extra & FLAG_DELETED)) {
      if (!freq1) { freq2++; }
      D->item_id = II->item_id;
      D->freq_title = freq1;
      D->freq_text = freq2;
      return D->doc_id;
    }
  }
  if (!D->dec.remaining) {
    D->doc_id = -1;
    D->item_id = 0;
    D->freq_title = D->freq_text = 0;
    D->dec.remaining--;
    return -1;
  }
  return -1;
}

/*
 *
 *  SEARCH ENGINE
 *
 */

static int Q_words;
int Q_limit, Q_hash_group_mode;
static hash_t QW[MAX_WORDS];
static char QT[MAX_WORDS];

int R_cnt, R_tot, R_tot_undef_hash, R_tot_groups;
item_t *R[MAX_RES+1];
double RR[MAX_RES+1];
int RS[MAX_RES+1]; /* value or index in hash table in group by hash mode */

iheap_en_t IHE[MAX_WORDS];

struct hashmap_ll_int hm;
struct hashset_ll hs;

static void clear_res (void) {
  R_cnt = R_tot = R_tot_undef_hash = R_tot_groups = 0;
  hs.h = 0;
  hm.h = 0;
  hs.filled = 0;
}

struct rate_weight {
  int p;
  double weight;
  double (*f) (struct item *, struct rate_weight *);
};

static int query_rate_weights;
static struct rate_weight QRW[16];

static double rw_func_log (struct item *I, struct rate_weight *R) {
  unsigned rate = get_rate_item_fast (I, R->p);
  rate -= INT_MIN;
  if (rate == -1U) { return 0.0; }
  return 1.0 - (log (rate + 1) * 0.04508422002825);
}

static double rw_func_log_reverse (struct item *I, struct rate_weight *R) {
  unsigned rate = get_rate_item_fast (I, R->p);
  rate -= INT_MIN;
  if (rate == -1U) { return 1.0; }
  return (log (rate + 1) * 0.04508422002825);
}

static double rw_func_linear (struct item *I, struct rate_weight *R) {
  unsigned rate = get_rate_item_fast (I, R->p);
  rate -= INT_MIN;
  return 1.0 - rate * (1.0 / 4294967295.0);
}

static double rw_func_linear_reverse (struct item *I, struct rate_weight *R) {
  unsigned rate = get_rate_item_fast (I, R->p);
  rate -= INT_MIN;
  return rate * (1.0 / 4294967295.0);
}

static void rate_weight_clear (int text_weight) {
  QRW[0].weight = text_weight;
  query_rate_weights = 1;
}

static int rate_weight_add (int func_tp, int tp, int weight) {
  if (query_rate_weights >= 16) { return -3; }
  int p = get_sorting_mode (tp);
  if (p < 0) { return -1; }
  int reverse_search = p & FLAG_REVERSE_SEARCH;
  QRW[query_rate_weights].p = p & 15;
  QRW[query_rate_weights].weight = weight;
  if (func_tp == 'L') {
    QRW[query_rate_weights].f = reverse_search ? rw_func_log_reverse : rw_func_log;
  } else if (func_tp == 'l') {
    QRW[query_rate_weights].f = reverse_search ? rw_func_linear_reverse : rw_func_linear;
  } else {
    return -2;
  }

  query_rate_weights++;
  return 0;
}

static int normalize_query_rate_weights (void) {
  int i;
  double s = 0.0;
  for (i = 0; i < query_rate_weights; i++) {
    s += QRW[i].weight;
  }
  if (s < 1e-9) { return -1; }
  s = 1.0 / s;
  for (i = 0; i < query_rate_weights; i++) {
    QRW[i].weight *= s;
  }
  return 0;
}

static double word_title_weight, word_text_weight, tag_weight;
static double sqr_word_title_weight, word_title_weight_word_text_weight, sqr_word_text_weight;

/* 0.0 -> cool */
/* 1.0 -> bad  */
static inline double evaluate_rating (struct item *I, double w) {
  int i;
  //vkprintf (4, "evaluate_rating (itemid = %lld, I->freq_title = %d, I->freq_text = %d\n", I->item_id, (int) I->freq_title, (int) I->freq_text);
  double s = sqr_word_title_weight * I->sum_sqr_freq_title +
             word_title_weight_word_text_weight * I->sum_freq_title_freq_text +
             sqr_word_text_weight * I->sum_sqr_freq_text;
  if (s < 1e-9) { return 1.0; }
  w /= sqrt (s);
  double r = 1.0 - w * QRW[0].weight;
  for (i = 1; i < query_rate_weights; i++) {
    r -= QRW[i].f (I, QRW + i) * QRW[i].weight;
  }
  if (r < 0.0) { r = 0.0; }
  if (r > 1.0) { r = 1.0; }
  return r;
}

struct item_with_rating {
  item_t *I;
  double V;
};

static void heapify_front (struct item_with_rating *E, int i, int slot) {
  while (1) {
    int j = i << 1;
    if (j > R_cnt) { break; }
    if (j < R_cnt && RR[j] < RR[j+1]) { j++; }
    if (E->V >= RR[j]) { break; }
    R[i] = R[j];
    RR[i] = RR[j];
    hm.h[RS[i] = RS[j]].value = i;
    i = j;
  }
  R[i] = E->I;
  RR[i] = E->V;
  hm.h[RS[i] = slot].value = i;
}

void rebuild_hashmap (void) {
  rebuild_hashmap_calls++;
  vkprintf (2, "rebuild_hashmap: old hashmap size = %d, Q_limit = %d\n", hm.size, Q_limit);
  int i;
  int n = hm.n << 1;

  if (n > MAX_RES + 10) {
    n = MAX_RES + 10;
  }
  hashmap_ll_int_free (&hm);
  if (!hashmap_ll_int_init (&hm, n)) {
    fprintf (stderr, "Not enough memory for allocate hash table for storing %d entries.\n", n);
    exit (2);
  }
  for (i = 1; i <= R_cnt; i++) {
    long long hc = get_hash_item (R[i]);
    if (hc) {
      int slot;
      int r = hashmap_ll_int_get (&hm, hc, &slot);
      if (r) {
        fprintf (stderr, "log_cur_pos ()\t%lld\n", log_cur_pos ());
        fprintf (stderr, "last_search_query\t%s\n", last_search_query);
        assert (!r);
      }
      hm.h[slot].key = hc;
      hm.h[slot].value = i;
      hm.filled++;
      RS[i] = slot;
    } else {
      RS[i] = hm.size;
    }
  }
}

void store_res_group_mode (struct item *I, double r) {
  if (R_tot == 1) {
    int hs = 2 * Q_limit;
    if (hs < 600) { hs = 600; }
    if (!hashmap_ll_int_init (&hm, hs)) {
      kprintf ("Not enough memory for allocate hash table\n");
      exit (1);
    }
  }

  long long hc = get_hash_item (I);
  if (!hc) {
    R_tot_undef_hash++;
  }

  struct item_with_rating tmp, *E = &tmp;
  E->V = r;

  /* optimization: don't look into hash table if current item is worser */
  if (R_cnt == Q_limit && E->V >= RR[1]) { return; }

  int slot = hm.size; /* items with unset hash map into special last slot of hashtable */
  E->I = I;

  //vkprintf (4, "E->I->item_id = %016llx, E->V = %d\n", E->I->item_id, E->V);

  if (hc && hashmap_ll_int_get (&hm, hc, &slot)) {
    /* change group */
    int pos = hm.h[slot].value;
    if (pos != -1) {
      /* item exists in heap and hash */
      assert (pos >= 1 && pos <= R_cnt);
      if (E->V < RR[pos]) {
        /* current value is better */
        heapify_front (E, pos, slot);
      }
    } else {
      /* item should be inserted into heap */
      hm.h[RS[1]].value = -1; /* delete item from heap */
      heapify_front (E, 1, slot);
    }
  } else {
    /* add new group */
    R_tot_groups++;
    if (hc) {
      if (hm.filled >= hm.n) {
        rebuild_hashmap ();
        hashmap_ll_int_get (&hm, hc, &slot);
      }
      hm.filled++;
      hm.h[slot].key = hc;
    }
    if (R_cnt == Q_limit) {
      hm.h[RS[1]].value = -1; /* deleted from heap */
      heapify_front (E, 1, slot);
    } else {
      int i = ++R_cnt;
      while (i > 1) {
        int j = (i >> 1);
        if (RR[j] >= E->V) { break; }
        R[i] = R[j];
        RR[i] = RR[j];
        hm.h[RS[i] = RS[j]].value = i;
        i = j;
      }
      R[i] = E->I;
      RR[i] = E->V;
      hm.h[RS[i] = slot].value = i;
    }
  }
}

void add_items_into_hashset (void) {
  int i;
  for (i = 0; i < R_cnt && hs.filled + R_tot_undef_hash <= MAX_RES; i++) {
    hashset_ll_insert (&hs, get_hash_item_unsafe (R[i]));
  }
}

void hashset_init (int n) {
  if (!hashset_ll_init (&hs, n)) {
    fprintf (stderr, "Could allocate hashset_ll, n = %d\n", n);
    exit (2);
  }
}

struct query_range  {
  int minr;
  int maxr;
  int idx;
};

/* Q_minr, Q_maxr, Q_minr2, Q_maxr2 - replacement */
struct query_range Q_range[MAX_RATES];
int n_ranges;

static int cmp_query_range (const void *a, const void *b) {
  const struct query_range *x = (const struct query_range *) a;
  const struct query_range *y = (const struct query_range *) b;
  unsigned int c = x->maxr - x->minr;
  unsigned int d = y->maxr - y->minr;
  if (c < d) {
    return -1;
  }
  if (c > d) {
    return 1;
  }
  return 0;
}

/*
  returns 1 in case continue search
  returns 0 in case stop search (for example too many items found case)
*/

int store_res (struct item *I, double w) {
  vkprintf (3, "store_res (item_id = %lld, w = %.10lf\n", I->item_id, w);
  if (w < 1e-9) { return 1; }
  int i, j = 0;
  for (i = 0; i < n_ranges; i++) {
    int r0 = get_rate_item_fast (I, Q_range[i].idx);
    if (r0 < Q_range[i].minr || r0 > Q_range[i].maxr) {
      return 1;
    }
  }

  R_tot++;
  if (Q_limit <= 0) {
    return 1;
  }

  double r = evaluate_rating (I, w);

  if (Q_hash_group_mode) {
    store_res_group_mode (I, r);
    return 1;
  }

  if (R_cnt == Q_limit) {
    if (RR[1] <= r) {
      return 1;
    }
    i = 1;
    while (1) {
      j = i*2;
      if (j > R_cnt) { break; }
      if (j < R_cnt) {
        if (RR[j+1] > RR[j]) {
          j++;
        }
      }
      if (RR[j] <= r) { break; }
      R[i] = R[j];
      RR[i] = RR[j];
      i = j;
    }
    R[i] = I;
    RR[i] = r;
  } else {
    i = ++R_cnt;
    while (i > 1) {
      j = (i >> 1);
      if (RR[j] >= r) { break; }
      R[i] = R[j];
      RR[i] = RR[j];
      i = j;
    }
    R[i] = I;
    RR[i] = r;
  }
  return 1;
}

static void heap_sort_res (void) {
  int i, j, k;
  struct item *t;
  for (k = R_cnt - 1; k > 0; k--) {
    t = R[k+1];
    double r = RR[k+1];
    R[k+1] = R[1];
    RR[k+1] = RR[1];
    i = 1;
    while (1) {
      j = 2*i;
      if (j > k) { break; }
      if (j < k) {
        if (RR[j+1] > RR[j]) { j++; }
      }
      if (r >= RR[j]) { break; }
      R[i] = R[j];
      RR[i] = RR[j];
      i = j;
    }
    R[i] = t;
    RR[i] = r;
  }

  for (i = 0; i < R_cnt; i++) {
    R[i] = R[i+1];
    RR[i] = RR[i+1];
  }
}

void postprocess_res_group_mode (void) {
  if (Q_limit <= 0) {
    if (hs.h == 0 && R_cnt > 0) {
      hashset_init (R_cnt);
      add_items_into_hashset ();
    }
    R_tot_groups = hs.filled + R_tot_undef_hash;
    if (R_tot_groups > MAX_RES) { R_tot_groups = -1; }
    return;
  }
  if (!R_cnt) { return; }
  hashmap_ll_int_free (&hm);
  heap_sort_res ();
}

void postprocess_res (void) {
  if (Q_hash_group_mode) {
    postprocess_res_group_mode ();
    return;
  }
  if (!R_cnt) { return; }
  heap_sort_res ();
}

static int find_range_idx (int sm) {
  int i;
  for (i = 0; i < n_ranges; i++) {
    if (Q_range[i].idx == sm) {
      return i;
    }
  }
  if (n_ranges >= MAX_RATES) {
    return -1;
  }
  Q_range[n_ranges].minr = INT_MIN;
  Q_range[n_ranges].maxr = INT_MAX;
  Q_range[n_ranges].idx = sm;
  return n_ranges++;
}

static int get_range_idx_with_add (int ch) {
  int sm = get_sorting_mode (ch);
  if (sm < 0) {
    return -1;
  }
  if (! (sm & FLAG_REVERSE_SEARCH)) {
    //ch is not uppercase letter
    return -1;
  }
  sm &= ~FLAG_REVERSE_SEARCH;
  return find_range_idx (sm);
}

static int add_query_word (hash_t word, char tag) {
  int i;
  for (i = 0; i < Q_words; i++) {
    if (QW[i] == word) {
      return i;
    }
  }
  if (i == MAX_WORDS) {
    return -1;
  }
  QW[i] = word;
  QT[i] = tag;
  IHE[i].sp = -2;
  return Q_words++;
}

static void word_weight_set (int title_weight, int text_weight) {
  double x = ((double) title_weight) + ((double) text_weight);
  x = 1.0 / x;
  word_title_weight = x * title_weight;
  word_text_weight = x * text_weight;

  sqr_word_title_weight = word_title_weight * word_title_weight;
  word_title_weight_word_text_weight = word_title_weight * word_text_weight;
  sqr_word_text_weight = word_text_weight * word_text_weight;

  vkprintf (3, "word_title_weight = %.10lf, word_text_weight = %.10lf\n", word_title_weight, word_text_weight);
}

char *parse_query (char *text, int *Q_raw, int *error) {
  static char buff[512];
  Q_words = 0;
  *error = -239;
  if (strncmp (text, "search", 6)) {
    *error = -1;
    return text;
  }
  text += 6;

  if (*text == 'x') {
    Q_hash_group_mode = 0;
  } else if (*text == 'u') {
    Q_hash_group_mode = 1;
  } else {
    *error = -2;
    return text;
  }
  text++;

  if (*text == '#') {
    *Q_raw = 0;
  } else if (*text == '%') {
    *Q_raw = 1;
  } else {
    *error = -3;
    return text;
  }
  text++;

  int end = -1;
  unsigned word_title_weight, word_text_weight, text_weight;
  if (sscanf (text, "%u,%u,%u,%lf%n", &word_title_weight, &word_text_weight, &text_weight, &tag_weight, &end) < 4 || end < 0 || text[end] != ',') {
    *error = -4;
    return text;
  }
  if (word_title_weight >= 0x7fffffffU || word_text_weight >= 0x7fffffffU || text_weight >= 0x7fffffffU || tag_weight > 1 || tag_weight < 0) {
    *error = -5;
    return text;
  }
  text += end;
  text++; /* skip ',' */
  word_weight_set (word_title_weight, word_text_weight);
  rate_weight_clear (text_weight);
  Q_limit = 0;
  while (isdigit (*text)) {
    unsigned w;
    end = -1;
    if (sscanf (text, "%u%n", &w, &end) < 1 || end < 0) {
      *error = -6;
      return text;
    }
    text += end;
    if (w >= 0x7fffffffU) {
      *error = -7;
      return text;
    }
    if (*text  == '(' || *text == '<' || *text == '[') {
      Q_limit = w;
      break;
    }
    if (!text[0] || !text[1] || rate_weight_add (text[0], text[1], w) < 0) {
      *error = -8;
      return text;
    }
    text += 2;
  }
  if (*text  != '(' && *text != '<' && *text != '[') {
    *error = -9;
    return text;
  }

  int no_nw = 1;

  n_ranges = 0;

  while (*text == '[' || *text == '<') {
    char *ptr = (char *) text + 1;
    if (*text == '<') {
      long long x;
      ptr = strchr (ptr, '>');
      if (ptr == 0) {
        *error = -10;
        return text;
      }
      if (sscanf (text+1, "%llx>", &x) != 1) { break; }
      text = ptr + 1;
      int i = find_range_idx (14);
      Q_range[i].minr = Q_range[i].maxr = (int) x;
      i = find_range_idx (15);
      Q_range[i].minr = Q_range[i].maxr = (int) (x >> 32);
    }
    int l = INT_MIN, r = INT_MAX, c;
    if (*ptr == '-' || (*ptr >= '0' && *ptr <= '9')) {
      l = strtol (ptr, &ptr, 10);
    }

    c = *ptr++;
    int idx = get_range_idx_with_add (c);
    if (idx < 0) {
      break;
    }

    if (*ptr == '-' || (*ptr >= '0' && *ptr <= '9')) {
      r = strtol (ptr, &ptr, 10);
    }
    if (*ptr != ']') {
      break;
    }
    text = ptr + 1;
    if (l > Q_range[idx].minr) { Q_range[idx].minr = l; }
    if (r < Q_range[idx].maxr) { Q_range[idx].maxr = r; }
  }

  // optimization: checking starts from ranges with lesser length
  qsort (Q_range, n_ranges, sizeof(Q_range[0]), cmp_query_range);

  if (*text == '(') {
    text++;
  }

  while (Q_words < MAX_WORDS && (*text == 0x1f || *text == '?')) {
    int i = 1;
    buff[0] = 0x1f;
    text++;
    while ((unsigned char) *text >= 0x40 && i < 32) {
      buff[i++] = *text++;
    }
    add_query_word (word_hash (buff, i), 1);
    while (*text == '+') {
      text++;
    }
  }

  while (*text && Q_words < MAX_WORDS) {
    int wl = no_nw ? 0 : get_notword (text);
    no_nw = 0;
    if (wl < 0) {
      break;
    }
    while (wl > 0 && *text != 0x1f) {
      text++;
      wl--;
    }
    if (*text == 0x1f) {
      wl = 1;
      while ((unsigned char) text[wl] >= 0x40) {
        wl++;
      }
      no_nw = 1;
    } else {
      wl = get_word (text);
    }

    if (!wl) {
      continue;
    }
    assert (wl < 511);
    add_query_word (word_hash (buff, my_lc_str (buff, text, wl)), 0);
    // fprintf (stderr, "word #%d: %.*s (%.*s), hash=%016llx\n", Q_words, wl, buff, wl, text, QW[Q_words-1]);
    text += wl;
  }

  if (Q_limit <= 0 && Q_hash_group_mode) {
    *error = -11;
    return text;
  }

  if (normalize_query_rate_weights () < 0) {
    *error = -12;
    return text;
  }
  *error = 0;
  return text;
}

static int ihe_load (iheap_en_t *A) {
  item_t *I0 = A->cur0, *I1 = A->cur1;
  if (unlikely(!I0 && !I1)) {
    A->item_id = MAX_ITEM_ID;
    A->cur = 0;
    A->cur_freq_title = A->cur_freq_text = 0;
    return 0;
  }
  if (!I1) {
    A->cur = I0;
    A->cur_freq_title = A->cur_freq_title0;
    A->cur_freq_text = A->cur_freq_text0;
    A->item_id = I0->item_id;
  } else if (!I0) {
    A->cur = I1;
    A->cur_freq_title = A->Decoder.freq_title;
    A->cur_freq_text = A->Decoder.freq_text;
    A->item_id = I1->item_id;
  } else if (I0->item_id < I1->item_id) {
    A->cur = I0;
    A->cur_freq_title = A->cur_freq_title0;
    A->cur_freq_text = A->cur_freq_text0;
    A->item_id = I0->item_id;
  } else {
    A->cur = I1;
    A->cur_freq_title = A->Decoder.freq_title;
    A->cur_freq_text = A->Decoder.freq_text;
    A->item_id = I1->item_id;
  }
  return 1;
}

/*
#ifdef DEBUG
static void ihe_dump (iheap_en_t *A) {
  int i;
  for (i = 0; i <= A->sp; i++) {
    fprintf (stderr, "%p.%d ", A->TS[i], A->Bt[i]);
  }
  fprintf (stderr, "\n");
}

#endif
*/

static inline int ihe_sgn (tree_t *T, iheap_en_t *A) {
  return T->word < A->word ? -1 : (T->word > A->word ? 1 : 0);
}

static int ihe_dive (iheap_en_t *A) {
  int sp = A->sp, bt, sgn;
  tree_t *T;

  //  fprintf (stderr, "ihe_dive(%p): sp=%d\n", A, sp);

  T = A->TS[sp];
  bt = A->Bt[sp];

  while (1) {
    assert (T);
    sgn = ihe_sgn (T, A);
    if (sgn >= 0 && T->left) {
      T = T->left;
      if (!sgn) { bt = sp; }
    } else if (sgn < 0 && T->right) {
      T = T->right;
    } else {
      break;
    }
    assert (sp < MAX_TREE_DEPTH-1);
    A->TS[++sp] = T;
    A->Bt[sp] = bt;
  }

  A->sp = sp;

  // fprintf (stderr, "ihe_dive(): done, sp=%d\n, res=%d", sp, sgn);

  return sgn;
}

static int ihe_advance0 (iheap_en_t *A) {
  int sp = A->sp;
  tree_t *T;

  // fprintf (stderr, "ihe_advance(%p): sp=%d\n", A, sp);

  assert (A->cur0 && sp >= 0);

  T = A->TS[sp];

  do {
    if (!T->right) {
      sp = A->Bt[sp];
      if (sp < 0) {
        break;
      }
      T = A->TS[sp];
    } else {
      assert (sp < MAX_TREE_DEPTH-1);
      A->TS[++sp] = T->right;
      A->Bt[sp] = A->Bt[sp-1];
      A->sp = sp;

      if (ihe_dive(A) != 0) {
        sp = -1;
        break;
      }
      sp = A->sp;
      T = A->TS[sp];
    }
    A->cur0 = T->item;
    A->cur_freq_title0 = T->freq_title;
    A->cur_freq_text0 = T->freq_text;
  } while (0);

  // fprintf (stderr, "ihe_adv() done, sp=%d, cur=%p (item_id=%016llx)\n", sp, sp < 0 ? 0 : A->cur, sp < 0 ? 0 : A->cur->item_id);

  if (sp < 0) {
    A->sp = -1;
    A->cur0 = 0;
    return ihe_load (A);
  }

  A->sp = sp;

  return ihe_load (A);
}

static int ihe_advance1 (iheap_en_t *A) {
  if (adv_ilist (&A->Decoder) >= 0) {
    A->cur1 = (struct item *) (IndexItems + A->Decoder.doc_id);
  } else {
    A->cur1 = 0;
  }
  return ihe_load (A);
}

static int ihe_advance (iheap_en_t *A) {
  if (!A->cur) { return 0; }
  if (A->cur == A->cur0) { return ihe_advance0 (A); }
  else if (A->cur == A->cur1) { return ihe_advance1 (A); }
  else assert (0);
  return 0;
}

static int ihe_init (iheap_en_t *A, hash_t word) {
  int sgn, sp;
  memset (A, 0, sizeof (*A));
  A->word = word;
  A->TS[0] = Root;
  A->Bt[0] = -1;

  if (init_ilist_decoder (&A->Decoder, word)) {
    if (adv_ilist (&A->Decoder) >= 0) {
      A->cur1 = (struct item *) (IndexItems + A->Decoder.doc_id);
    }
  }

  if (!Root) {
    sgn = 1;
  } else {
    sgn = ihe_dive (A);
    sp = A->sp;

    if (sgn < 0 && A->Bt[sp] >= 0) {
      sp = A->Bt[sp];
      sgn = ihe_sgn (A->TS[sp], A);
    }
  }

  if (sgn != 0) {
    A->sp = -1;
    A->cur0 = 0;
    return ihe_load (A);
  }

  A->sp = sp;
  A->cur0 = A->TS[sp]->item;

  A->cur_freq_title0 = A->TS[sp]->freq_title;
  A->cur_freq_text0 = A->TS[sp]->freq_text;

  return ihe_load (A);
}

static int HC;
static iheap_en_t *H[MAX_WORDS+1];
static void iheap_insert (iheap_en_t *A) {
  int i, j;
  long long item_id = A->item_id;
  assert (HC < MAX_WORDS);
  if (!A->cur) { return; }
  i = ++HC;
  while (i > 1) {
    j = (i >> 1);
    if (H[j]->item_id <= item_id) {
      break;
    }
    H[i] = H[j];
    i = j;
  }
  H[i] = A;
}

static void iheap_pop (void) {
  int i, j;
  iheap_en_t *A;

  if (!HC) { return; }
  A = H[1];
  if (!A->cur) {
    A = H[HC--];
    if (!HC) { return; }
  }

  i = 1;
  while (1) {
    j = i*2;
    if (j > HC) { break; }
    if (j < HC && H[j+1]->item_id < H[j]->item_id) { j++; }
    if (A->item_id <= H[j]->item_id) { break; }
    H[i] = H[j];
    i = j;
  }
  H[i] = A;
}

static int has_empty_range () {
  int i = 0;
  for (i = 0; i < n_ranges; i++) {
    if (Q_range[i].minr > Q_range[i].maxr) {
      return 1;
    }
  }
  return 0;
}

static void ranking_query (void) {
  vkprintf (3, "priority_sort_query\n");
  //Q_skip_mismatch_words_if_complete_case_found = (Q_order & FLAG_ENTRY_SORT_SEARCH) ? 1 : 0;
  int i;
  HC = 0;
  for (i = 0; i < Q_words; i++) {
    if (!QT[i]) { break; }
  }
  const int mandatory_tags = i;
  int tags = mandatory_tags;
  if (mandatory_tags == Q_words) { return; }
  for ( ; i < Q_words; i++) {
    if (QT[i]) {
      /* optinal tag */
      QT[i] = 2;
      tags++;
    }
  }
  const int words = Q_words - tags;
  if (words <= 0) { return; }

  for (i = 0; i < Q_words; i++) {
    if (ihe_init (IHE+i, QW[i])) {
      iheap_insert (IHE+i);
      IHE[i].tag = QT[i];
    } else {
      if (QT[i] == 1) {
        /* mandatory tag always should be in document text */
        return;
      }
    }
  }
  const double z = 1.0 / sqrt (words + tag_weight * tag_weight * tags);
  double w = -239; // **MAY BE USED UNINITIALIZED
  vkprintf (3, "HC = %d, mandatory_tags = %d, tags = %d, Q_words = %d\n", HC, mandatory_tags, tags, Q_words);
  item_t *I = NULL;
  int j = -239; // **MAY BE USED UNINITIALIZED
  while (HC) {
    assert (H[1]->cur);
    if (H[1]->cur != I) {
      if (I != NULL && i == mandatory_tags && j) {
        store_res (I, w * z);
      }
      I = H[1]->cur;
      const double v = H[1]->cur_freq_title * word_title_weight + H[1]->cur_freq_text * word_text_weight;
      if (!H[1]->tag) {
        i = 0;
        j = 1;
        w = v;
      } else {
        j = 0;
        i = 2 - H[1]->tag;
        w = v * tag_weight;
      }
    } else {
      const double v = H[1]->cur_freq_title * word_title_weight + H[1]->cur_freq_text * word_text_weight;
      if (!H[1]->tag) {
        j++;
        w += v;
      } else {
        i += 2 - H[1]->tag;
        w += v * tag_weight;
      }
    }
    ihe_advance (H[1]);
    iheap_pop ();
  }
  if (I != NULL && i == mandatory_tags && j) {
    store_res (I, w * z);
  }
}

int perform_query (void) {
  clear_res ();

  if (!Q_words || has_empty_range () )  {
    return 0;
  }

  if (verbosity >= 3) {
    int t_depth = tree_depth (Root, 0);
    kprintf ("tree depth = %d\n", t_depth);
  }

  ranking_query ();

  free_all_list_decoders ();
  postprocess_res ();
  return Q_hash_group_mode ? R_tot_groups : R_tot;
}
