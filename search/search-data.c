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

#define _FILE_OFFSET_BITS 64

#include <assert.h>
#include <stddef.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <math.h>
#include <ctype.h>
#include <unistd.h>
#include <limits.h>
#include <fcntl.h>
#include <sys/types.h>
#include <aio.h>

#include "kdb-binlog-common.h"
#include "kdb-data-common.h"
#include "kdb-search-binlog.h"
#include "search-index-layout.h"
#include "search-data.h"
#include "search-common.h"
#include "server-functions.h"
#include "word-split.h"
#include "stemmer-new.h"
#include "utils.h"
#include "crc32.h"

extern int now;
extern int verbosity;

#define MIN_ITEM_ID (-1LL << 63)
#define MAX_ITEM_ID (~MIN_ITEM_ID)
#define MAX_PRIORITY (10)

/* --------- search data ------------- */
int MAX_MISMATCHED_WORDS = 2;
int items_cleared;
int items_short_ids;

int import_only_mode = 0;
int tot_items, del_items, del_item_instances, mod_items, tot_freed_deleted_items, idx_items_with_hash;
long long rebuild_hashmap_calls, assign_max_set_rate_calls, change_multiple_rates_set_rate_calls;

char last_search_query[LAST_SEARCH_QUERY_BUFF_SIZE];
static hash_t universal_tag_hash;

item_t *Items[ITEMS_HASH_PRIME];
tree_t *Root;


int search_replay_logevent (struct lev_generic *E, int size);
static void free_tree (tree_t *T);

/*                                   0000000000111111 */
/*                                   0123456789012345 */
const char *rate_first_characters = "rsdbcfghklmn"; //Do not change position of RATE, SATE, DATE
int tbl_sorting_mode[128];
void init_tbl_sorting_mode () {
  const char *p = rate_first_characters;
  int i;
  for (i = 0; i < 128; i++) {
    tbl_sorting_mode[i] = -1;
  }

  assert (strchr(rate_first_characters, 'i') == 0);
  tbl_sorting_mode['i'] = MAX_RATES;
  tbl_sorting_mode['I'] = MAX_RATES | FLAG_REVERSE_SEARCH;
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

  items_short_ids = 0;
  universal_tag_hash = word_hash ("\x1f@@", 3);

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

static tree_t **IndexTreeWords = NULL;
static struct index_item *IndexItems;
static struct search_index_word *IndexWords;
static struct search_index_hapax_legomena *IndexHapaxLegomena;
static char *IndexData;
int idx_items, idx_words, idx_hapax_legomena;
long idx_bytes, idx_loaded_bytes;

static long long item_texts_offset, words_offset, hapax_legomena_offset, freq_words_offset, word_index_offset, index_size;

long long get_hash_item_unsafe (const item_t *I) {
  return (((unsigned long long) I->rates[I->rates_len-1]) << 32) | ((unsigned int) I->rates[I->rates_len-2]);
}

static long long get_hash_item (const item_t *I) {
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
  C->rates = zzmalloc (sz);
  bread (C->rates, sz);
  assert (popcount_short (C->mask) == C->rates_len);
  if (C->mask & 0xc000) {
    idx_items_with_hash++;
  }
  return sz + 12;
}

int load_index (kfs_file_handle_t Index) {
  int fd = Index->fd;
  int index_with_crc32 = -1;
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

  if (Header.magic == SEARCH_INDEX_WITH_CRC32_MAGIC) {
    index_with_crc32 = 1;
  } else if (Header.magic == SEARCH_INDEX_MAGIC) {
    index_with_crc32 = 0;
  } else {
    kprintf ("bad index file header\n");
    return -4;
  }

  if (!check_header (&Header)) {
    return -7;
  }

  if (Header.left_subtree_size_threshold < 16) {
    kprintf ("bad left_subtree_size_threshold = %d\n", Header.left_subtree_size_threshold);
    return -6;
  }

  const int sz_headers = sizeof (Header) + index_with_crc32 * sizeof (CRC32_Header);
  if (index_with_crc32) {
    sz = sizeof (CRC32_Header);
    if (sz != read (fd, &CRC32_Header, sz)) {
      kprintf ("error reading index (crc32_header). %m\n");
      return -5;
    }
  }
  assert (!index_with_crc32 || CRC32_Header.crc32_header == compute_crc32 (&Header, sizeof (Header)));

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
    int t = load_item (IndexItems+i);
    if (t < 0) { return -2;}
    rbytes += t;
  }
  assert (rbytes == Header.index_items_size);
  assert (!index_with_crc32 || CRC32_Header.crc32_items == ~idx_crc32_complement);
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
  assert (!index_with_crc32 || CRC32_Header.crc32_words == compute_crc32 (IndexWords, s));

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
  assert (!index_with_crc32 || CRC32_Header.crc32_hapax_legomena == compute_crc32 (IndexHapaxLegomena, s));

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
  assert (!index_with_crc32 || CRC32_Header.crc32_data == compute_crc32 (IndexData, s));

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

struct tree *cur_wordlist_head;

int alloc_tree_nodes, free_tree_nodes;
tree_t free_tree_head = {.left = &free_tree_head, .right = &free_tree_head};

static tree_t *new_tree_node (int y) {
  tree_t *P;
  if (free_tree_nodes) {
    assert (--free_tree_nodes >= 0);
    P = free_tree_head.right;
    assert (P != &free_tree_head && P->left == &free_tree_head);
    P->right->left = &free_tree_head;
    free_tree_head.right = P->right;
  } else {
    P = zzmalloc (sizeof (tree_t));
    assert (P);
    alloc_tree_nodes++;
  }

  P->left = P->right = 0;
  P->y = y;
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

static tree_t *tree_insert (tree_t *T, hash_t word, item_t *item, int y) {
  tree_t *P;
  if (!T) {
    P = new_tree_node (y);
    P->word = word;
    P->item = item;
    return P;
  }
  if (T->y >= y) {
    if (word < T->word || (word == T->word && item->item_id < T->item->item_id)) {
      T->left = tree_insert (T->left, word, item, y);
    } else {
      T->right = tree_insert (T->right, word, item, y);
    }
    return T;
  }
  P = new_tree_node (y);
  P->word = word;
  P->item = item;
  tree_split (&P->left, &P->right, T, word, item);
  return P;
}

int tree_depth (tree_t *T, int d) {
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

static item_t *get_item_f (long long item_id, get_item_f_mode force) {
  int h1, h2;
  item_t *C, *D;
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
    C = zzmalloc0 (sizeof (item_t));
    if (!C) { return C; }
    if (D) {
      zzfree (D, sizeof (item_t));
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
  cur_wordlist_head = 0;
}

static void item_clear_wordlist (item_t *I, tree_t **p_I_words) {
  if (p_I_words == NULL) {
    return;
  }
  struct tree *p = *p_I_words;
  while (p) {
    tree_t *w = p->next;
    Root = tree_delete (Root, p->word, I);
    p = w;
  }
  *p_I_words = NULL;
}

static inline int in_title (unsigned freqs) {
  return (freqs >= 0x10000);
}

static void item_add_word (item_t *I, hash_t word, unsigned freqs) {
  tree_t *T = tree_lookup (Root, word, I);
  if (!T) {
    int y = lrand48() << 1;
    if (in_title (freqs)) { y |= 1; }
    Root = tree_insert (Root, word, I, y);
  } else  {
    assert (T->item == I);
  }
}

static int set_multiple_rates_item (item_t *I, int mask, int *rates) {
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

static int set_rate_item (item_t *I, int p, int rate) {
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

static inline int get_rate_item_fast (const item_t *I, int p) {
  int i = get_bitno (I->mask, p);
  return i >= 0 ? I->rates[i] : 0;
}

static int get_rate_item (item_t *I, int p) {
  if (!I || (I->extra & FLAG_DELETED)) { return 0;}
  if (!(I->mask & (1 << p))) { return 0;}
  return I->rates[get_bitno (I->mask,p)];
}

static void delete_item_rates (item_t *I) {
  zzfree (I->rates, I->rates_len * 4);
  I->rates = 0;
  I->rates_len = 0;
  I->mask = 0;
  I->extra |= FLAG_DELETED;
}

static void move_item_rates (item_t *dst, struct index_item *src) {
  dst->rates = src->rates;
  dst->rates_len = src->rates_len;
  dst->mask = src->mask;

  src->rates = 0;
  src->rates_len = 0;
  src->mask = 0;
}

static pair_word_freqs_t Q[65536];

static tree_t **get_index_item_words_ptr (struct index_item *II, int force) {
  if (IndexTreeWords == NULL) {
    if (!force) {
      return NULL;
    }
    IndexTreeWords = zmalloc0 (sizeof (IndexTreeWords[0]) * (idx_items + 1));
  }
  int i = II - IndexItems;
  return IndexTreeWords + i;
}

static int add_item_tags (const char *const text, int len, long long item_id) {
  item_t *I = NULL;
  struct index_item *II = NULL;
  tree_t **p_words;

  assert (text && len >= 0 && len < 256 && !text[len]);
  assert (item_id > 0);
  if (!fits (item_id)) {
    return 0;
  }
  if (import_only_mode) {
    return 1;
  }

  II = get_idx_item (item_id);
  if (II != NULL) {
    p_words = get_index_item_words_ptr (II, 1);
    I = (item_t *) II;
  } else {
    I = get_item_f (item_id, ONLY_FIND);
    if (I == NULL) {
      return 0;
    }
    p_words = &(I->words);
  }

  cur_wordlist_head = *p_words;
  int i, Wc = extract_words (text, len, 0, Q, 65536, tag_owner, item_id);
  for (i = 0; i < Wc; i++) {
    item_add_word (I, Q[i].word, Q[i].freqs);
  }
  *p_words = cur_wordlist_head;

  return 1;
}

static int change_item (const char *text, int len, long long item_id, int rate, int rate2) {
  item_t *I;
  struct index_item *II;

  assert (text && len >= 0 && len < 65536 && !text[len]);
  assert (item_id > 0);

  if (!fits (item_id)) {
    return 0;
  }

  if (import_only_mode) {
    return 1;
  }

  vkprintf (4, "change_item: text=%s, len = %d, item_id = %016llx, rate = %d, rate2 = %d\n",
    text, len, item_id, rate, rate2);

  II = get_idx_item (item_id);
  if (II) {
    mod_items++;
    II->extra |= FLAG_DELETED;
    item_clear_wordlist ((item_t *) II, get_index_item_words_ptr (II, 0));
  }

  I = get_item_f (item_id, ONLY_FIND);
  if (I) {
    if (I->extra & FLAG_DELETED) {
      del_items--;
      I->extra ^= FLAG_DELETED;
    }
    item_clear_wordlist (I, &I->words);
  } else {
    I = get_item_f (item_id, ADD_NOT_FOUND_ITEM);
    if (!I) {
      return 0;
    }
  }

  if (II) {
    move_item_rates (I, II);
  }

  int rates[4], mask = 1 + 2, l = 2;
  rates[0] = rate;
  rates[1] = rate2;
  if (!creation_date || !(I->mask & 4)) {
    rates[l++] = now;
    mask += 4;
  }

  clear_cur_wordlist ();
  int i, Wc = extract_words (text, len, universal, Q, 65536, tag_owner, item_id);
  for (i = 0; i < Wc; i++) {
    item_add_word (I, Q[i].word, Q[i].freqs);
  }
  I->words = cur_wordlist_head;
  if (wordfreqs_enabled) {
    rates[l++] = evaluate_uniq_words_count (Q, Wc);
    mask |= 1 << 13;
  }
  set_multiple_rates_item (I, mask, rates);
  return 1;
}

static int delete_item (long long item_id) {
  item_t *I = get_item_f (item_id, ONLY_FIND);
  struct index_item *II = get_idx_item (item_id);
  if (II) {
    II->extra |= FLAG_DELETED;
    mod_items++;
    assert (!I);
    item_clear_wordlist ((item_t *) II, get_index_item_words_ptr (II, 0));
    return 1;
  }
  if (!I || (I->extra & FLAG_DELETED)) { return 0; }
  delete_item_rates (I);
  item_clear_wordlist (I, &I->words);
  del_items++;
  del_item_instances++;
  return 1;
}

int get_hash (long long *hash, long long item_id) {
  item_t *I = get_item_f (item_id, ONLY_FIND);
  item_t *II = (item_t *) get_idx_item (item_id);

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
  item_t *I = get_item_f (item_id, ONLY_FIND);
  item_t *II = (item_t *) get_idx_item (item_id);

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

  item_t *I = get_item_f (item_id, ONLY_FIND);
  item_t *II = (item_t *) get_idx_item (item_id);

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
  item_t *I = get_item_f (item_id, ONLY_FIND);
  item_t *II = (item_t *) get_idx_item (item_id);
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

static int incr_rate_item (item_t *I, int p, int rate_incr) {
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
  item_t *I = get_item_f (item_id, ONLY_FIND);
  item_t *II = (item_t *) get_idx_item (item_id);
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
    case LEV_SEARCH_ITEM_ADD_TAGS ... LEV_SEARCH_ITEM_ADD_TAGS+0xff:
      if (size < 12) { return -2; }
      struct lev_search_item_add_tags *ET = (struct lev_search_item_add_tags *) E;
      s = (E->type & 0xff);
      if (size < 13+s) { return -2; }
      if (ET->text[s]) { return -4; }
      add_item_tags (ET->text, s, ET->obj_id);
      return 13+s;
    case LEV_SEARCH_RESET_ALL_RATES:
      return (size < 8) ? -2 : 8;
  }

  kprintf ("unknown log event type %08x at position %lld\n", E->type, log_cur_pos());

  return -3;
}

/* adding new log entries */

int do_delete_item (long long item_id) {
  if (!fits (item_id)) { return 0; }
  struct lev_search_delete_item *ED = alloc_log_event (LEV_SEARCH_DELETE_ITEM, 12, 0);
  assert (ED);
  ED->obj_id = item_id;
  return delete_item (item_id);
}

int do_add_item_tags (const char *const text, int len, long long item_id) {
  char *q;
  int i;
  if (len >= 256 || len < 0 || !fits (item_id)) {
    return 0;
  }
  assert (len < 256);
  struct lev_search_item_add_tags *E = alloc_log_event (LEV_SEARCH_ITEM_ADD_TAGS + len, 13+len, 0);
  E->obj_id = item_id;
  q = E->text;
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
  return add_item_tags (q - len, len, item_id);
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

/*
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
*/

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
  item_t *I = get_item_f (item_id, ONLY_FIND);
  item_t *II = (item_t *) get_idx_item (item_id);

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

static char QT[MAX_WORDS];

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

static void init_decoder (struct search_list_decoder *D, int N, int K, int bytes, const void *file_offset, int compression_method) {
  unsigned char *ptr;
  vkprintf (3, "init_decoder (N = %d, K = %d)\n", N, K);
  if (K <= 2) {
    ptr = (unsigned char *) file_offset;
    compression_method = le_raw_int32;
  } else if (bytes <= 8) {
    ptr = (unsigned char *) file_offset;
  } else {
    long long offs;
    memcpy (&offs, file_offset, 8);
    assert (offs >= word_index_offset && offs < index_size);
    assert (offs + bytes <= index_size);
    /*
    if (bytes == 0xffff) {
      while (b < idx_words && (IndexWords[b].len <= 2 || IndexWords[b].bytes <= 8)) {
        b++;
      }
      assert (IndexWords[b].file_offset >= offs + 0xffff && IndexWords[b].file_offset <= index_size);
      bytes = IndexWords[b].file_offset - offs;
    }
    */
    offs -= word_index_offset;
    assert (offs >= 0 && offs < idx_bytes && offs + bytes <= idx_bytes);
    ptr = (unsigned char *)(IndexData + offs);
  }
  assert (Q_decoders < 2 * MAX_WORDS);
  D->dec = allocated_list_decoders[Q_decoders++] = zmalloc_list_decoder_ext (N, K, ptr, compression_method, 0, Header.left_subtree_size_threshold);
  D->remaining = K;
  D->len = K;
}

/* return priority for given doc_id */
static int adv_ilist_subseq_slow (ilist_decoder_t *D, int idx) {
  struct search_list_decoder *dec_subseq = &D->dec_subseq;
  while (D->last_subseq < idx) {
    if (dec_subseq->remaining <= 0) {
      D->last_subseq = 0x7fffffff;
      return 0;
    }
    dec_subseq->remaining--;
    D->last_subseq = dec_subseq->dec->decode_int (dec_subseq->dec);
  }
  return (D->last_subseq == idx) ? 1 : 0;
}

static int adv_ilist_subseq_fast (ilist_decoder_t *D, int idx) {
  D->last_subseq = list_interpolative_ext_forward_decode_idx (D->dec_subseq.dec, idx);
  return (D->last_subseq == idx) ? 1 : 0;
}
static void init_adv_ilist_subseq (ilist_decoder_t *D) {
  D->adv_ilist_subseq = (D->dec_subseq.dec->tp == le_interpolative_ext) ? adv_ilist_subseq_fast : adv_ilist_subseq_slow;
}

int init_ilist_decoder (ilist_decoder_t *D, hash_t word) {
  /* *D is already cleared,
     since D is a part of already cleared heap entry
  */
  const static int hapax_legomena_buf[1] = {0};
  D->doc_id = -1;
  D->last_subseq = -1;

  /* optimization: since Interplolative encoding time is O(N*ln(N))
                   use le_degenerate decoder for universal tag (all items)
  */
  if (Q_words == 1 && QT[0] && word == universal_tag_hash && idx_items >= 10) {
    init_decoder (&D->dec, idx_items, idx_items, 4, &D->extra, le_degenerate);
    init_decoder (&D->dec_subseq, 1, 0, 4, hapax_legomena_buf, -2);
    init_adv_ilist_subseq (D);
    return 1;
  }

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
      D->extra = u & 0x7fffffff;
      init_decoder (&D->dec, idx_items, 1, 4, &D->extra, -2);
      init_decoder (&D->dec_subseq, 1, (u >> 31) ? 1 : 0, 4, hapax_legomena_buf, -2);
      init_adv_ilist_subseq (D);
      return 1;
    }
    return 0;
  }
  D->sword = IndexWords + a;
  init_decoder (&D->dec, idx_items, D->sword->len, D->sword->bytes, &D->sword->file_offset, Header.word_list_compression_methods[0]);
  if (((Q_order & (FLAG_ENTRY_SORT_SEARCH | FLAG_PRIORITY_SORT_SEARCH)) && Q_limit > 0) || Q_min_priority) {
    init_decoder (&D->dec_subseq, D->sword->len, D->sword->len_subseq, D->sword->bytes_subseq, &D->sword->file_offset_subseq, Header.word_list_compression_methods[1]);
  } else {
    init_decoder (&D->dec_subseq, D->sword->len, 0, 0, hapax_legomena_buf, -2);
  }
  init_adv_ilist_subseq (D);

  return 1;
}

static void free_all_list_decoders (void) {
  int i;
  for (i = 0; i < Q_decoders; i++) { zfree_list_decoder (allocated_list_decoders[i]); }
  Q_decoders = 0;
}

int adv_ilist (ilist_decoder_t *D) {
  struct index_item *II;
  while (D->dec.remaining > 0) {
    D->dec.remaining--;
    D->doc_id = D->dec.dec->decode_int (D->dec.dec);
    assert (D->doc_id >= 0 && D->doc_id < idx_items);
    II = IndexItems + D->doc_id;
    if (!(II->extra & FLAG_DELETED)) {
      D->item_id = II->item_id;
      D->field = D->adv_ilist_subseq (D, D->dec.len - 1 - D->dec.remaining);
      return D->doc_id;
    }
  }
  if (!D->dec.remaining) {
    D->doc_id = -1;
    D->item_id = 0;
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


typedef struct query_node query_node_t;

enum {
  qn_false, qn_true, qn_word, qn_and, qn_or, qn_minus, qn_error, qn_cum_and, qn_cum_or
};

struct query_nodes_list {
  query_node_t *v;
  struct query_nodes_list *next;
};

struct query_node {
  int op;
  int priority;
  hash_t word;
  long long item_id;
  query_node_t *left, *right;
  struct query_nodes_list *head;
  int frequency;
  iheap_en_t *iter;
  item_t *cur;
};

#define MAX_QUERY_NODES MAX_WORDS

struct query_range  {
  int minr;
  int maxr;
  int idx;
};

/* Q_minr, Q_maxr, Q_minr2, Q_maxr2 - replacement */
struct query_range Q_range[MAX_RATES + 2];
int n_ranges;

int cmp_query_range (const void *a, const void *b) {
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

#define MAX_OPTIONAL_TAGS 16

int Q_order, Q_limit, Q_words, Q_extmode, Q_nodes, Q_hash_group_mode, Q_hash_rating, Q_type, order, Q_min_priority, Q_relevance;
double Q_K_Opt_Tag, Q_Relevance_Power;
int Q_optional_tags;
static hash_t QW[MAX_WORDS];
static int QOTW[MAX_WORDS]; /* optional tag weight */
static query_node_t QV[MAX_QUERY_NODES+1], *Q_root;

int R_cnt, R_tot, R_tot_undef_hash, R_tot_groups;
item_t *R[MAX_RES+1];
int RV[MAX_RES+1];
int RS[MAX_RES+1]; /* value or index in hash table in group by hash mode */

iheap_en_t IHE[MAX_WORDS];
iheap_en_t IHT[MAX_OPTIONAL_TAGS];
static double optional_tags_mult_coeff;

struct hashmap_ll_int hm;
struct hashset_ll hs;

long long extract_hash_item (const item_t *I) {
  if (Q_hash_rating < 0) {
    return get_hash_item (I);
  }
  const int rate = get_rate_item_fast (I, Q_hash_rating);
  if (rate > 0) {
    return rate;
  }
  const long long h = get_hash_item (I);
  if (h > 0 && h < INT_MIN) {
    return 0;
  }
  return h;
}

/*************************** mixed rating code **************************************************/

struct rate_weight {
  int p;
  double weight;
  double (*f) (item_t *, struct rate_weight *);
};

static int query_rate_weights;
static struct rate_weight QRW[MAX_RATE_WEIGHTS+1]; /* last reserved for QRP normalization */
static struct rate_weight QRT, QRP;
static int QRT_min_creation_time;

static double rw_func_log (item_t *I, struct rate_weight *R) {
  int rate = get_rate_item_fast (I, R->p);
  if (rate < 0) { rate = 0; }
  return 1.0 - log (rate + 1.0) * 0.046538549706095;
}

static double rw_func_log_reverse (item_t *I, struct rate_weight *R) {
  int rate = get_rate_item_fast (I, R->p);
  if (rate < 0) { rate = 0; }
  return log (rate + 1.0) * 0.046538549706095;
}

static double rw_func_linear (item_t *I, struct rate_weight *R) {
  int rate = get_rate_item_fast (I, R->p);
  if (rate < 0) { rate = 0; }
  return 1.0 - rate * (1.0 / 2147483647.0);
}

static double rw_func_linear_reverse (item_t *I, struct rate_weight *R) {
  int rate = get_rate_item_fast (I, R->p);
  if (rate < 0) { rate = 0; }
  return rate * (1.0 / 2147483647.0);
}

static void rate_weight_clear (void) {
  query_rate_weights = 0;
  QRT.p = -1;
  QRP.p = -1;
}

void init_rate_weights (void) {
  query_rate_weights = 0;
  QRT.p = -1;
  QRP.p = -1;
}

void add_rate_weight (int rate_type, double weight, int flags) {
  if (weight <= 0) {
    return;
  }
  assert (query_rate_weights < MAX_RATE_WEIGHTS);
  if (rate_type == -1) {
    QRP.p = 0;
    QRP.weight = weight;
    QRW[query_rate_weights].f = 0;
    Q_order |= FLAG_ENTRY_SORT_SEARCH;
    return;
  }
  int reverse_search = flags & 1;
  QRW[query_rate_weights].p = rate_type;
  QRW[query_rate_weights].weight = weight;
  if (flags & 2) {
    QRW[query_rate_weights].f = reverse_search ? rw_func_log_reverse : rw_func_log;
  } else {
    QRW[query_rate_weights].f = reverse_search ? rw_func_linear_reverse : rw_func_linear;
  }
  query_rate_weights++;
}

void add_decay (int rate_type, double weight) {
  QRT.p = rate_type;
  QRT.weight = -(M_LN2 / weight);
  QRT_min_creation_time = now - weight * 40;
}


static int rate_weight_add (int func_tp, int tp, int weight) {
  if (weight <= 0) {
    return 0;
  }
  if (query_rate_weights >= MAX_RATE_WEIGHTS) { return -3; }

  if (tp == 'P') {
    if (func_tp == 'l') {
      QRP.p = 0;
      QRP.weight = weight;
      QRW[query_rate_weights].f = NULL;
      Q_order |= FLAG_ENTRY_SORT_SEARCH;
      return 0;
    }
    return -4;
  }

  int p = get_sorting_mode (tp);
  if (p < 0) { return -1; }
  int reverse_search = p & FLAG_REVERSE_SEARCH;
  QRW[query_rate_weights].p = p & 15;
  QRW[query_rate_weights].weight = weight;
  if (func_tp == 'L') {
    QRW[query_rate_weights].f = reverse_search ? rw_func_log_reverse : rw_func_log;
  } else if (func_tp == 'l') {
    QRW[query_rate_weights].f = reverse_search ? rw_func_linear_reverse : rw_func_linear;
  } else if (func_tp == 'T') {
    QRT.p = QRW[query_rate_weights].p;
    QRT.weight = -(M_LN2 / weight);
    QRT_min_creation_time = now - weight * 40;
    return 0;
  } else {
    return -2;
  }
  query_rate_weights++;
  return 0;
}

int normalize_query_rate_weights (void) {
  int i;
  double s = 0.0;

  if (!QRP.p) {
    QRW[query_rate_weights++].weight = QRP.weight;
  }

  if (!query_rate_weights) {
    return 0;
  }

  for (i = 0; i < query_rate_weights; i++) {
    s += QRW[i].weight;
  }

  if (s < 1e-9) {
    query_rate_weights = 0;
    return 0;
  }

  s = 1.0 / s;
  for (i = 0; i < query_rate_weights; i++) {
    QRW[i].weight *= s;
  }

  if (!QRP.p) {
    query_rate_weights--;
    QRP.weight = QRW[query_rate_weights].weight;
  }

  return 0;
}

/************************************************************************************************/
#define RELEVANCE_TABLE_SIZE 4096
static int item_count_optional_tags_sum (item_t *I);

static double old_Q_Relevance_Power = -1.0;
static double tbl_relevance[RELEVANCE_TABLE_SIZE];

static void tbl_relevance_init (void) {
  int i;
  if (!Q_relevance) { return; }
  if (fabs (old_Q_Relevance_Power - Q_Relevance_Power) < 1e-9) {
    Q_Relevance_Power = old_Q_Relevance_Power;
    return;
  }
  old_Q_Relevance_Power = Q_Relevance_Power;
  for (i = 0; i < RELEVANCE_TABLE_SIZE; i++) {
    tbl_relevance[i] = -1.0;
  }
}

static int evaluate_relevance_search_rating (item_t *I, int priority) {
  int i;
  double r;
  if (!query_rate_weights) { r = 0.5; }
  else {
    r = 0.0;
    i = 0;
    do {
      r += QRW[i].weight * QRW[i].f (I, &QRW[i]);
    } while (++i < query_rate_weights);
  }

  if (!QRP.p) {
    r += QRP.weight * priority;
  }

  if (!(QRT.p & -16)) {
    int rate = get_rate_item_fast (I, QRT.p);
    if (rate < QRT_min_creation_time) {
      return 0;
    }
    if (rate < now) {
      r *= exp (QRT.weight * (now - rate));
    }
  }

  if (r < 0) {
    vkprintf (3, "evaluate_searchx_rating: r = %.lg\n", r);
    return 0;
  }

  if (r > 1) {
    vkprintf (3, "evaluate_searchx_rating: r = %.lg\n", r);
    return 0x7fffffff;
  }

  if (Q_optional_tags) {
    r *= (1.0 + Q_K_Opt_Tag * item_count_optional_tags_sum (I)) * optional_tags_mult_coeff;
  }

  if (Q_relevance) {
    unsigned w = get_rate_item_fast (I, 13);
    vkprintf (3, "item_id = %lld, in title = %d, whole = %d\n", I->item_id, w >> 16, w & 0xffff);
    int l = (Q_order & FLAG_ONLY_TITLE_SEARCH) ? (w >> 16) : (w & 0xffff);
    l++;
    if (l >= RELEVANCE_TABLE_SIZE) { l = RELEVANCE_TABLE_SIZE - 1; }
    if (tbl_relevance[l] < -0.5) {
      tbl_relevance[l] = 1.0 / pow (l, Q_Relevance_Power);
    }
    r *= tbl_relevance[l];
  }

  vkprintf (3, "item_id = %lld, r = %.10lf\n", I->item_id, r);
  return (int) (r * 2147483647.0);
}

static inline unsigned int get_object_id (long long item_id) {
  unsigned int r = item_id >> 32;
  return r ? r : (unsigned int) item_id;
}

static int get_rating_as_object_id (item_t *I, int priority) {
  return get_object_id (I->item_id);
}

static int random_rating (item_t *I, int priority) {
  return lrand48 ();
}

static int mix_priority_with_object_id (item_t *I, int priority) {
  long long rr = ((order == 1) ? (5 - priority) : (priority - 5)) * 200000000;
  rr += get_object_id (I->item_id);
  if (rr < INT_MIN) { return INT_MIN; }
  if (rr > INT_MAX) { return INT_MAX; }
  return rr;
}

static int mix_priority_with_rating (item_t *I, int priority) {
  long long rr = ((order == 1) ? (5 - priority) : (priority - 5)) * 100000000;
  int p = get_bitno (I->mask, Q_type);
  if (p != -1) {
    rr += I->rates[p];
  }
  if (rr < INT_MIN) { return INT_MIN; }
  if (rr > INT_MAX) { return INT_MAX; }
  return rr;
}

static int get_rating (item_t *I, int priority) {
  return get_rate_item_fast (I, Q_type);
}

int (*evaluate_rating) (item_t *, int) = NULL;

static void optional_tags_init (void);

static void init_order (void) {
  Q_type = Q_order & 0xff;
  order = (Q_order & FLAG_REVERSE_SEARCH) ? 2 : 1;
  vkprintf (3, "Q_order = %d, Q_type = %d, order = %d\n", Q_order, Q_type, order);
  evaluate_rating = NULL;
  if (Q_type == MAX_RATES + 2) {
    tbl_relevance_init ();
    optional_tags_init ();
    evaluate_rating = evaluate_relevance_search_rating;
  } else if (Q_type == MAX_RATES + 1) {
    evaluate_rating = random_rating;
  } else if (Q_order & (FLAG_ENTRY_SORT_SEARCH | FLAG_PRIORITY_SORT_SEARCH)) {
    if (Q_type == MAX_RATES) {
      evaluate_rating = mix_priority_with_object_id;
    } else {
      assert (Q_type <= 15);
      evaluate_rating = mix_priority_with_rating;
    }
  } else if (Q_type == MAX_RATES) {
    evaluate_rating = get_rating_as_object_id;
  } else {
    assert (Q_type <= 15);
    evaluate_rating = get_rating;
  }
}

void clear_res (void) {
  R_cnt = R_tot = R_tot_undef_hash = R_tot_groups = 0;
  init_order ();
  hs.h = 0;
  hm.h = 0;
  hs.filled = 0;
}

/*
int evaluate_rating (item_t *I, int priority) {
  if (Q_type == MAX_RATES + 1) {
    return lrand48 ();
  }
  if (Q_order & (FLAG_ENTRY_SORT_SEARCH | FLAG_PRIORITY_SORT_SEARCH)) {
    long long rr;
    if (Q_type == MAX_RATES) {
      rr = ((order == 1) ? (5 - priority) : (priority - 5)) * 200000000;
      rr += get_object_id (I->item_id);
    } else {
      assert (Q_type <= 15);
      rr = ((order == 1) ? (5 - priority) : (priority - 5)) * 100000000;
      int p = get_bitno (I->mask, Q_type);
      if (p != -1) {
        rr += I->rates[p];
      }
    }
    if (rr < INT_MIN) { return INT_MIN; }
    if (rr > INT_MAX) { return INT_MAX; }
    return rr;
  }
  assert (Q_type <= 15);
  int p = get_bitno (I->mask, Q_type);
  if (p != -1) {
    return I->rates[p];
  }
  return 0;
}
*/

struct item_with_rating {
  item_t *I;
  int V;
};

static void heapify_front (struct item_with_rating *E, int i, int slot) {
  while (1) {
    int j = i << 1;
    if (j > R_cnt) { break; }
    if (j < R_cnt && RV[j] <  RV[j+1]) { j++; }
    if (E->V >= RV[j]) { break; }
    R[i] = R[j];
    RV[i] = RV[j];
    hm.h[RS[i] = RS[j]].value = i;
    i = j;
  }
  R[i] = E->I;
  RV[i] = E->V;
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
    const long long hc = extract_hash_item (R[i]);
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

void store_res_group_mode (item_t *I, int priority) {
  if (R_tot == 1) {
    int hs = 2 * Q_limit;
    if (hs < 600) { hs = 600; }
    if (!hashmap_ll_int_init (&hm, hs)) {
      fprintf (stderr, "Not enough memory for allocate hash table\n");
      exit (2);
    }
  }

  const long long hc = extract_hash_item (I);

  if (!hc) {
    R_tot_undef_hash++;
  }

  struct item_with_rating tmp, *E = &tmp;
  E->V = evaluate_rating (I, priority);

  if (order == 2) {
    E->V = -(E->V + 1);
  }

  /* optimization: don't look into hash table if current item is worser */
  if (R_cnt == Q_limit && E->V >= RV[1]) { return; }

  int slot = hm.size; /* items with unset hash map into special last slot of hashtable */
  E->I = I;

  //vkprintf (4, "E->I->item_id = %016llx, E->V = %d\n", E->I->item_id, E->V);

  if (hc && hashmap_ll_int_get (&hm, hc, &slot)) {
    /* change group */
    int pos = hm.h[slot].value;
    if (pos != -1) {
      /* item exists in heap and hash */
      assert (pos >= 1 && pos <= R_cnt);
      if (E->V < RV[pos]) {
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
        if (RV[j] >= E->V) { break; }
        R[i] = R[j];
        RV[i] = RV[j];
        hm.h[RS[i] = RS[j]].value = i;
        i = j;
      }
      R[i] = E->I;
      RV[i] = E->V;
      hm.h[RS[i] = slot].value = i;
    }
  }
}

static void add_items_into_hashset (void) {
  int i;
  for (i = 0; i < R_cnt && hs.filled + R_tot_undef_hash <= MAX_RES; i++) {
    hashset_ll_insert (&hs, Q_hash_rating < 0 ? get_hash_item_unsafe (R[i]) : get_rate_item_fast (R[i], Q_hash_rating));
  }
}

static void hashset_init (int n) {
  if (!hashset_ll_init (&hs, n)) {
    fprintf (stderr, "Could allocate hashset_ll, n = %d\n", n);
    exit (2);
  }
}

/*
  returns 1 in case continue search
  returns 0 in case stop search (for example too many items found case)
*/

static int store_res (item_t *I, int priority) {
  vkprintf (3, "store_res!!, n_ranges = %d\n", n_ranges);
  int i, j = 0, r;
  for (i = 0; i < n_ranges; i++) {
    int r0 = get_rate_item (I, Q_range[i].idx);
    vkprintf (3, "ranges: r0 = %d, Q_range[i].minr = %d, Q_range[i].maxr = %d\n", r0, Q_range[i].minr, Q_range[i].maxr);
    if (r0 < Q_range[i].minr || r0 > Q_range[i].maxr) {
      return 1;
    }
  }

  R_tot++;
  if (Q_limit <= 0) {
    if (!Q_hash_group_mode) { return 1; }
    const long long hc = extract_hash_item (R[i]);
    if (hc != 0) {
      if (R_cnt < MAX_RES) {
        R[R_cnt++] = I;
      } else {
        if (hs.h == 0) {
          /* add found items into hashset */
          hashset_init (MAX_RES);
          add_items_into_hashset ();
        }
        if (hashset_ll_insert (&hs, hc)) {
          if (hs.filled + R_tot_undef_hash > MAX_RES) {
            /* stop search : we found too many different groups */
            return 0;
          }
        }
      }
    } else {
      R_tot_undef_hash++;
    }
    return (hs.filled + R_tot_undef_hash > MAX_RES) ? 0 : 1;
  }

  if (Q_hash_group_mode) {
    store_res_group_mode (I, priority);
    return 1;
  }

  if (Q_type == MAX_RATES && !(Q_order & (FLAG_ENTRY_SORT_SEARCH | FLAG_PRIORITY_SORT_SEARCH))) { //sort by id
    if ((Q_order & FLAG_REVERSE_SEARCH) && R_cnt == Q_limit) {
      R_cnt = 0;
    }
    if (R_cnt < Q_limit) {
      R[R_cnt++] = I;
    }
    return 1;
  }

  r = evaluate_rating (I, priority);

  if (order == 2) {
    r = -(r + 1);
  }


  assert (order != 0);

  if (R_cnt == Q_limit) {
    if (RV[1] <= r) {
      return 1;
    }
    i = 1;
    while (1) {
      j = i*2;
      if (j > R_cnt) { break; }
      if (j < R_cnt) {
        if (RV[j+1] > RV[j]) {
          j++;
        }
      }
      if (RV[j] <= r) { break; }
      R[i] = R[j];
      RV[i] = RV[j];
      i = j;
    }
    R[i] = I;
    RV[i] = r;
  } else {
    i = ++R_cnt;
    while (i > 1) {
      j = (i >> 1);
      if (RV[j] >= r) { break; }
      R[i] = R[j];
      RV[i] = RV[j];
      i = j;
    }
    R[i] = I;
    RV[i] = r;
  }
  return 1;
}

static void heap_sort_res (void) {
  int i, j, k, r;
  item_t *t;
  for (k = R_cnt - 1; k > 0; k--) {
    t = R[k+1];
    r = RV[k+1];
    R[k+1] = R[1];
    RV[k+1] = RV[1];
    i = 1;
    while (1) {
      j = 2*i;
      if (j > k) { break; }
      if (j < k) {
        if (RV[j+1] > RV[j]) { j++; }
      }
      if (r >= RV[j]) { break; }
      R[i] = R[j];
      RV[i] = RV[j];
      i = j;
    }
    R[i] = t;
    RV[i] = r;
  }

  if (order == 2) {
    for (i = 0; i < R_cnt; i++) {
      R[i] = R[i+1];
      RV[i] = -(RV[i+1] + 1);
    }
  } else {
    for (i = 0; i < R_cnt; i++) {
      R[i] = R[i+1];
      RV[i] = RV[i+1];
    }
  }
}

static void postprocess_res_group_mode (void) {
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

static void postprocess_res (void) {
  if (Q_hash_group_mode) {
    postprocess_res_group_mode ();
    return;
  }

  int i, k;
  item_t *t;

  if (Q_type == MAX_RATES && !(Q_order & (FLAG_ENTRY_SORT_SEARCH | FLAG_PRIORITY_SORT_SEARCH))) { //sort by id
    if (Q_order & FLAG_REVERSE_SEARCH) {
      k = R_cnt - 1;
      for (i = 0; i < k - i; i++) {
        t = R[k-i];  R[k-i] = R[i];  R[i] = t;
      }
      if (R_tot >= Q_limit) {
        k = R_cnt + Q_limit - 1;
        for (i = R_cnt; i < k - i; i++) {
          t = R[k-i];  R[k-i] = R[i];  R[i] = t;
        }
        R_cnt = Q_limit;
      }
    }
    return;
  }

  if (!R_cnt) { return; }
  heap_sort_res ();
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
  vkprintf (3, "add_query_word (%llx, %d)\n", word, (int) tag);
  QW[i] = word;
  QT[i] = tag;
  IHE[i].sp = -2;
  QOTW[i] = 1;
  return Q_words++;
}

static char *parse_ptr;

static inline void skipspc (void) {
  while (*parse_ptr == ' ' || *parse_ptr == 9 || *parse_ptr == 10 || *parse_ptr == 13) {
    parse_ptr++;
  }
};

static query_node_t *new_query_node (int op) {
  query_node_t *X = QV + Q_nodes;
  if (Q_nodes >= MAX_QUERY_NODES) {
    return 0;
  }
  Q_nodes++;
  memset (X, 0, sizeof (*X));
  X->op = op;
  return X;
}

static query_node_t *query_word_node (hash_t word) {
  /*  --- the following optimization is incorrect in general ---
      int i;
      for (i = 0; i < Q_nodes; i++) {
      if (QV[i].op == qn_word && QV[i].word == word) {
      return &QV[i];
      }
      }
   */
  query_node_t *X = new_query_node (qn_word);
  if (!X) {
    return 0;
  }
  X->word = word;
  return X;
}

static query_node_t *make_universal_node (void) {
  return new_query_node (qn_true);
  /*  --- the following optimization is incorrect in general ---
      if (!Q_universal) {
      Q_universal = new_query_node (qn_true);
      }
      return Q_universal;
   */
}

static query_node_t *parse_query_expr (void);
static char *parse_query_words (char *text);

// term ::= "word" | \x1f<hashtag> | ?<hashtag> | '(' expr ')' | '!' term | 'true' | 'false' | '1' | '0'

query_node_t *parse_query_term (void) {
  char *s_ptr, ch;
  query_node_t *X, *Y;
  static char buff[512];

  if (Q_nodes >= MAX_QUERY_NODES - 2) {
    return 0;
  }

  skipspc();
  switch (*parse_ptr) {
    case '"':
      s_ptr = ++parse_ptr;
      while (*parse_ptr && *parse_ptr != '"') {
        parse_ptr++;
      }
      if (*parse_ptr != '"' || parse_ptr >= s_ptr + 509) {
        parse_ptr = s_ptr - 1;
        return 0;
      }

      ch = *parse_ptr;
      *parse_ptr = 0;
      Q_words = 0;
      parse_query_words (s_ptr);
      *parse_ptr = ch;
      parse_ptr++;
      if (!Q_words) {
        return make_universal_node ();
      }
      X = NULL;
      while (Q_words) {
        if (X == NULL) {
          X = query_word_node (QW[--Q_words]);
          if (X == NULL) {
            return NULL;
          }
        } else {
          Y = new_query_node (qn_and);
          if (Y == NULL) {
            return NULL;
          }
          Y->left = X;
          Y->right = query_word_node (QW[--Q_words]);
          if (!Y->right) {
            return NULL;
          }
          X = Y;
        }
      }
      return X;
    case 0x1f:
    case '?':
      s_ptr = parse_ptr++;
      while ((unsigned char) *parse_ptr >= 0x40) {
        parse_ptr++;
      }
      if (parse_ptr >= s_ptr + 509) {
        parse_ptr = s_ptr;
        return 0;
      }
      memcpy (buff, s_ptr, parse_ptr - s_ptr);
      buff[0] = 0x1f;
      X = query_word_node (word_hash (buff, parse_ptr - s_ptr));
      return X;
    case '(':
      parse_ptr++;
      X = parse_query_expr ();
      if (!X) {
        return 0;
      }
      skipspc();
      if (*parse_ptr != ')') {
        return 0;
      }
      parse_ptr++;
      return X;
    case '!':
      parse_ptr++;
      Y = parse_query_term ();
      if (!Y) {
        return 0;
      }
      X = new_query_node (qn_minus);
      if (!X) {
        return 0;
      }
      X->left = make_universal_node ();
      if (!X->left) {
        return 0;
      }
      X->right = Y;
      return X;
    case 't':
      if (memcmp (parse_ptr, "true", 4)) {
        return 0;
      }
      parse_ptr += 4;
      return make_universal_node ();
    case '1':
      parse_ptr++;
      return make_universal_node ();
    case 'f':
      if (memcmp (parse_ptr, "false", 5)) {
        return 0;
      }
      parse_ptr += 4;
    case '0':
      parse_ptr++;
      return new_query_node (qn_false);
  }
  return 0;
}

// expr ::= term { ( '&' | '+' | '-' | '#' ) term }
static query_node_t *parse_query_expr (void) {
  query_node_t *X, *Y;
  X = parse_query_term ();
  if (!X) {
    return 0;
  }
  while (1) {
    skipspc();
    if (*parse_ptr != '&' && *parse_ptr != '+' && *parse_ptr != '-' && *parse_ptr != '#') {
      return X;
    }
    Y = new_query_node (*parse_ptr == '#' ? qn_or : (*parse_ptr == '-' ? qn_minus : qn_and));
    if (!Y) {
      return 0;
    }
    Y->left = X;
    parse_ptr++;
    Y->right = parse_query_term ();
    if (!Y->right) {
      return 0;
    }
    X = Y;
  }
  return X;
}

query_node_t *optimize_query (query_node_t *X) {
  if (!X) {
    return 0;
  }
  X->left = optimize_query (X->left);
  X->right = optimize_query (X->right);
  switch (X->op) {
    case qn_false:
    case qn_true:
    case qn_word:
      return X;
    case qn_and:
      if (X->left->op == qn_false) {
        return X->left;
      }
      if (X->left->op == qn_true) {
        return X->right;
      }
      if (X->right->op == qn_false) {
        return X->right;
      }
      if (X->right->op == qn_true) {
        return X->left;
      }
      return X;
    case qn_or:
      if (X->left->op == qn_true) {
        return X->left;
      }
      if (X->left->op == qn_false) {
        return X->right;
      }
      if (X->right->op == qn_true) {
        return X->right;
      }
      if (X->right->op == qn_false) {
        return X->left;
      }
      return X;
    case qn_minus:
      if (X->left->op == qn_false) {
        return X->left;
      }
      if (X->right->op == qn_true) {
        X->op = qn_false;
        X->left = X->right = 0;
        X->priority = 0;
        return X;
      }
      if (X->right->op == qn_false) {
        return X->left;
      }
      return X;
  }
  assert (0);
}

static int find_range_idx (int sm) {
  int i;
  for (i = 0; i < n_ranges; i++) {
    if (Q_range[i].idx == sm) {
      return i;
    }
  }
  if (n_ranges >= MAX_RATES + 2) {
    return -1;
  }
  Q_range[n_ranges].minr = INT_MIN;
  Q_range[n_ranges].maxr = INT_MAX;
  Q_range[n_ranges].idx = sm;
  return n_ranges++;
}

void init_ranges (void) {
  n_ranges = 0;
}

void add_range (int type, int min_value, int max_value) {
  int t = find_range_idx (type);
  assert (t >= 0);
  if (Q_range[t].minr < min_value) {
    Q_range[t].minr = min_value;
  }
  if (Q_range[t].maxr > max_value) {
    Q_range[t].maxr = max_value;
  }
}

void sort_ranges (void) {
  qsort (Q_range, n_ranges, sizeof(Q_range[0]), cmp_query_range);
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

static int ihe_init (iheap_en_t *A, hash_t word, int tag, int optional_tag_weight);

static char *parse_ranges (char *text) {
  n_ranges = 0;
  while (*text == '[' || *text == '<') {
    char *ptr = (char *) text + 1;
    if (*text == '<') {
      long long x;
      ptr = strchr (ptr, '>');
      if (ptr == 0) { break; }
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
  return text;
}

static int decode_tag_value (const unsigned char *text, int wl, int *value) {
  vkprintf (3, "%s: text: \"%.*s\"\n", __func__, wl, text);
  if (wl > 5) {
    return -1;
  }
  int i, s = 0;
  *value = 0;
  unsigned long long r = 0;
  for (i = 0; i < wl - 1; i++) {
    if (text[i] < 0x80) {
      return -1;
    }
    r |= ((unsigned long long) (text[i] - 0x80)) << s;
    s += 7;
  }
  if (text[i] < 0x40 || text[i] >= 0x80) {
    return -1;
  }
  r |= ((unsigned long long) (text[i] - 0x40)) << s;
  if (r > INT_MAX) {
    return -1;
  }
  *value = (int) r;
  vkprintf (3, "%s: *value = %d\n", __func__, *value);
  return 0;
}

static char *parse_query_words (char *text) {
  static char buff[512];
  vkprintf (3, "%s: %s\n", __func__, text);
  int last_tag_id = -1;
  int no_nw = 1;
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
    vkprintf (3, "add_query_word (%.*s)\n", i, buff);
    add_query_word (word_hash (buff, i), 1);
    while (*text == '+') {
      text++;
    }
  }

  while (*text && Q_words < MAX_WORDS) {
    int wl = no_nw ? 0 : get_notword (text);
    vkprintf (3, "no_nw: %d, text: %s, wl: %d\n", no_nw, text, wl);
    no_nw = 0;
    if (wl < 0) {
      break;
    }
    while (wl > 0 && (*text != 0x1f && *text != '?')) {
      text++;
      wl--;
    }
    if (*text == 0x1f || *text == '?') {
      wl = 1;
      while ((unsigned char) text[wl] >= 0x40) {
        wl++;
      }
      if (wl <= 32) {
        int tag_value;
        memcpy (buff, text, wl);
        buff[0] = 0x1f;
        if (!Q_extmode && wl >= 3 && buff[1] == '~' && !decode_tag_value ((const unsigned char *) buff + 2, wl - 2, &tag_value)) {
          if (last_tag_id >= 0) {
            QOTW[last_tag_id] = tag_value;
          }
        } else {
          vkprintf (3, "add_query_word (%.*s)\n", wl, buff);
          last_tag_id = add_query_word (word_hash (buff, wl), 1);
        }
      }
      no_nw = 1;
      text += wl;
      continue;
    } else {
      wl = get_word (text);
      vkprintf (3, "get_word('%s') returns %d.\n", text, wl);
    }

    if (!wl) {
      continue;
    }
    assert (wl < 511);
    vkprintf (3, "add_query_word (%.*s)\n", wl, text);
    add_query_word (word_hash (buff, my_lc_str (buff, text, wl)), 0);
    // fprintf (stderr, "word #%d: %.*s (%.*s), hash=%016llx\n", Q_words, wl, buff, wl, text, QW[Q_words-1]);
    text += wl;
  }
  return text;
}

char *parse_query (char *text, int do_parse_ranges) {
  Q_words = Q_nodes = 0;
  Q_root = 0;

  if (do_parse_ranges) {
    text = parse_ranges (text);
  }

  if (Q_extmode) {
    parse_ptr = text;

    Q_root = parse_query_expr ();
    if (!Q_root || *parse_ptr) {
      return parse_ptr;
    }

    Q_root = optimize_query (Q_root);
    if (!Q_root) {
      return parse_ptr;
    }

    vkprintf (2, "successfully compiled extended query, %d nodes used, root=%p\n", Q_nodes, Q_root);

    return 0;
  }
  text = parse_query_words (text);

  return 0;
}

char *parse_relevance_search_query (char *text, int *Q_raw, int *error, int do_parse_ranges) {
  vkprintf (3, "parse_relevance_search_query: %s\n", text);
  Q_limit = 0;
  Q_order = (MAX_RATES + 2) | FLAG_REVERSE_SEARCH;
  Q_words = 0;
  Q_extmode = 0;
  Q_hash_group_mode = 0;
  Q_hash_rating = -1;
  Q_min_priority = 0;
  Q_relevance = 0;
  Q_root = 0;
  *error = -239;
  if (strncmp (text, "search", 6)) {
    *error = -1;
    return text;
  }
  text += 6;

  if (*text == 'x') {
    Q_hash_group_mode = 0;
    text++;
  } else if (*text == 'u') {
    Q_hash_group_mode = 1;
    text++;
    int c = get_sorting_mode (*text);
    if (c >= 0 && c < MAX_RATES) {
      Q_hash_rating = c;
      text++;
    }
  } else {
    *error = -2;
    return text;
  }
  vkprintf (4, "Q_hash_rating: %d\n", Q_hash_rating);

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
  int only_title = -1;
  double rel_coeff = -1.0;
  if (sscanf (text, "%lf,%d,%lf%n", &rel_coeff, &only_title, &Q_K_Opt_Tag, &end) < 3 || end < 0 || text[end] != ',') {
    *error = -4;
    return text;
  }
  if (rel_coeff >= 0.25 && rel_coeff <= 4.0) {
    Q_relevance = 1;
    Q_Relevance_Power = rel_coeff * 0.5;
  }
  if (!wordfreqs_enabled) {
    Q_relevance = 0;
  }
  if (only_title > 0) {
    Q_order |= FLAG_ONLY_TITLE_SEARCH;
    Q_min_priority = 1;
  }
  text += end;
  text++; // skip ','
  rate_weight_clear ();
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

  if (do_parse_ranges) {
    text = parse_ranges (text);
  }
  text = parse_query_words (text);

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

char *parse_relevance_search_query_raw (char *text) {
  vkprintf (3, "parse_relevance_search_query_raw: %s\n", text);
  vkprintf (4, "Q_hash_rating: %d\n", Q_hash_rating);
  text = parse_query_words (text);
  return 0;
}

static void optional_tags_init (void) {
  int i, tot_opt_tags = 0;
  Q_optional_tags = 0;
  for (i = 0; i < Q_words; i++) {
    if (!QT[i]) {
      break;
    }
  }
  const int first_words_idx = i;
  int tot_optinal_tags_weight = 0;
  if (i < Q_words) {
    /* extract optional tags */
    while (Q_words > 0 && Q_optional_tags < MAX_OPTIONAL_TAGS) {
      if (QT[Q_words - 1]) {
        Q_words--;
        if (ihe_init (IHT+Q_optional_tags, QW[Q_words], 1, QOTW[Q_words])) {
          Q_optional_tags++;
          tot_optinal_tags_weight += QOTW[Q_words];
        }
        tot_opt_tags++;
      } else {
        break;
      }
    }
  }
  optional_tags_mult_coeff = 1.0 / (1.0 + Q_K_Opt_Tag * tot_optinal_tags_weight);
  vkprintf (3, "Q_words = %d, optinal_tags = %d\n", Q_words, Q_optional_tags);
  int words = Q_words - first_words_idx;
  if (words > MAX_PRIORITY) {
    words = MAX_PRIORITY;
  }
  if (words > 0) {
    QRP.weight /= (double) words;
  }
}

static int ihe_load (iheap_en_t *A) {
  item_t *I0 = A->cur0, *I1 = A->cur1;
  if (unlikely(!I0 && !I1)) {
    A->item_id = MAX_ITEM_ID;
    A->cur = 0;
    A->cur_y = 0;
    return 0;
  }
  if (unlikely(!I1)) {
    A->cur = I0;
    assert (A->sp >= 0);
    A->cur_y = A->TS[A->sp]->y | A->tag_word;
    A->item_id = I0->item_id;
  } else if (!I0) {
    A->cur = I1;
    A->cur_y = A->cur_y1 | A->tag_word;
    A->item_id = I1->item_id;
  } else if (unlikely(I0->item_id < I1->item_id)) {
    A->cur = I0;
    A->cur_y = A->TS[A->sp]->y | A->tag_word;
    A->item_id = I0->item_id;
  } else {
    A->cur = I1;
    A->cur_y = A->cur_y1 | A->tag_word;
    A->item_id = I1->item_id;
  }
  return 1;
}

#ifdef DEBUG
/*
static void ihe_dump (iheap_en_t *A) {
  int i;
  for (i = 0; i <= A->sp; i++) {
    fprintf (stderr, "%p.%d ", A->TS[i], A->Bt[i]);
  }
  fprintf (stderr, "\n");
}
*/
#endif

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
    A->cur_y = T->y;
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
    A->cur1 = (item_t *) (IndexItems + A->Decoder.doc_id);
    A->cur_y1 = A->Decoder.field;
  } else {
    A->cur1 = 0;
    //A->cur_y1 = 0;
  }
  return ihe_load (A);
}

/* tree forward advance */
static inline int ihe_sgn_ext (tree_t *T, iheap_en_t *A, long long item_id) {
  if (T->word < A->word) {
    return -2;
  }
  if (T->word > A->word) {
    return 2;
  }
  if (T->item->item_id < item_id) {
    return -1;
  }
  if (T->item->item_id > item_id) {
    return 1;
  }
  return 0;
}

static void ihe_skip_advance0 (iheap_en_t *A, long long item_id) {
  assert (A->cur0);
  if (A->cur0->item_id >= item_id) {
    return;
  }
  int sp = A->sp, best = -1;
  assert (sp >= 0);
  tree_t *T = A->TS[sp];
  do {
    assert (T->word == A->word);
    if (T->item->item_id >= item_id) {
      best = sp;
      break;
    }
    if (A->Bt[sp] < 0) {
      break;
    }
    sp = A->Bt[sp];
    T = A->TS[sp];
  } while (1);
  int bt = A->Bt[sp];
  while (1) {
    assert (T);
    int sgn = ihe_sgn_ext (T, A, item_id);
    if (sgn >= 0 && T->left) {
      T = T->left;
      if (sgn < 2) { bt = sp; }
    } else if (sgn < 0 && T->right) {
      T = T->right;
    } else {
      break;
    }
    assert (sp < MAX_TREE_DEPTH-1);
    A->TS[++sp] = T;
    A->Bt[sp] = bt;
    if (T->word == A->word && T->item->item_id >= item_id) {
      best = sp;
    }
  }
  if (best < 0) {
    A->cur0 = NULL;
    A->sp = -1;
    return;
  }
  A->sp = best;
  A->cur0 = A->TS[best]->item;
}

static int ihe_advance1_noload (iheap_en_t *A) {
  if (adv_ilist (&A->Decoder) >= 0) {
    A->cur1 = (item_t *) (IndexItems + A->Decoder.doc_id);
    A->cur_y1 = A->Decoder.field;
    return 1;
  } else {
    A->cur1 = 0;
    //A->cur_y1 = 0;
    return 0;
  }
}

static void ihe_skip_advance1_slow (iheap_en_t *A, long long item_id) {
  while (A->cur1->item_id < item_id) {
    if (!ihe_advance1_noload (A)) {
      break;
    }
  }
}

static int list_interpolative_ext_forward_decode_item (struct list_decoder *dec, long long item_id_lowerbound) {
  struct interpolative_ext_decoder_stack_entry *data = (struct interpolative_ext_decoder_stack_entry *) dec->data;

  int p = dec->p;
  data += dec->p;
  while (IndexItems[data->right_value].item_id <= item_id_lowerbound) {
    data--;
    p--;
  }

  if (p < dec->p) {
    int middle = (data->left_idx + data->right_idx) >> 1;
    if (data->right_subtree_offset < 0) {
      while (dec->k < middle) {
        dec->decode_int (dec);
      }
    } else {
      bread_seek (&dec->br, data->right_subtree_offset);
      dec->k = middle;
    }
    dec->p = p;
  }

  for ( ; ; dec->p++, data++) {
    int middle = (data->left_idx + data->right_idx) >> 1;
    if (IndexItems[data->middle_value].item_id == item_id_lowerbound) {
      if (data->right_subtree_offset < 0) {
        while (dec->k < middle) {
          dec->decode_int (dec);
        }
      } else {
        bread_seek (&dec->br, data->right_subtree_offset);
        dec->k = middle;
      }
      return data->middle_value;
    }
    const int l = data->right_idx - data->left_idx;
    assert (l >= 2);
    if (l == 2) {
      assert (IndexItems[data->right_value].item_id >= item_id_lowerbound);
      if (IndexItems[data->middle_value].item_id < item_id_lowerbound) {
        if (data->right_idx == dec->K + 1) {
          return -1;
        }
        dec->k = data->right_idx;
        return data->right_value;
      }
      if (IndexItems[data->left_value].item_id < item_id_lowerbound) {
        dec->k = middle;
        return data->middle_value;
      }
      //assert (data->left_value >= doc_id_lowerbound);
      dec->k = data->left_idx;
      return data->left_value;
    }
    struct interpolative_ext_decoder_stack_entry *next = data + 1;
    if (IndexItems[data->middle_value].item_id > item_id_lowerbound) {
      // left subtree
      if (data->left_idx == middle - 1) {
        dec->k = middle;
        return data->middle_value;
      }
      next->left_idx = data->left_idx;
      next->left_value = data->left_value;
      next->right_idx = middle;
      next->right_value = data->middle_value;
      interpolative_ext_decode_node (dec, next);
    } else {
      if (data->right_subtree_offset < 0) {
        while (dec->k < middle) {
          dec->decode_int (dec);
        }
      } else {
        bread_seek (&dec->br, data->right_subtree_offset);
        dec->k = middle;
      }
      next->left_idx = middle;
      next->left_value = data->middle_value;
      next->right_idx = data->right_idx;
      next->right_value = data->right_value;
      interpolative_ext_decode_node (dec, next);
    }
  }
  assert (0);
  return -1;
}

static void ihe_skip_advance1_fast (iheap_en_t *A, long long item_id) {
  if (A->cur1->item_id >= item_id) {
    return;
  }
  struct search_list_decoder *dec = &A->Decoder.dec;
  ilist_decoder_t *D = &A->Decoder;
  D->doc_id = list_interpolative_ext_forward_decode_item (dec->dec, item_id);
  if (D->doc_id < 0) {
    dec->remaining = 0;
    A->cur1 = 0;
    //A->cur_y1 = 0;
    return;
  }
  dec->remaining = dec->len - dec->dec->k;
  for (;;) {
    struct index_item *II = IndexItems + D->doc_id;
    if (!(II->extra & FLAG_DELETED)) {
      A->cur1 = (item_t *) II;
      D->item_id = II->item_id;
      A->cur_y1 = D->adv_ilist_subseq (D, D->dec.len - 1 - D->dec.remaining);
      return;
    }
    if (dec->remaining <= 0) {
      dec->remaining = 0;
      A->cur1 = 0;
      //A->cur_y1 = 0;
      return;
    }
    dec->remaining--;
    D->doc_id = dec->dec->decode_int (dec->dec);
  }
}

inline static int ihe_skip_advance (iheap_en_t *A, long long item_id) {
  if (A->cur0) {
    ihe_skip_advance0 (A, item_id);
  }
  if (A->cur1) {
    A->ihe_skip_advance1 (A, item_id);
  }
  return ihe_load (A);
}

static int item_count_optional_tags_sum (item_t *I) {
  int i, t = 0;
  for (i = 0; i < Q_optional_tags; ) {
    //vkprintf (3, "IHT[%d].cur->item_id = %lld\n", i, IHT[i].cur->item_id);
    if (IHT[i].cur->item_id < I->item_id && !ihe_skip_advance (&IHT[i], I->item_id)) {
      if (i != Q_optional_tags - 1) {
        memcpy (&IHT[i], &IHT[Q_optional_tags-1], sizeof (IHT[i]));
      }
      Q_optional_tags--;
      continue;
    }
    assert (IHT[i].cur->item_id >= I->item_id);
    //vkprintf (3, "IHT[%d].cur->item_id = %lld\n", i, IHT[i].cur->item_id);
    if (IHT[i].cur == I) {
      t += IHT[i].optional_tag_weight;
    }
    i++;
  }
  vkprintf (3, "item_count_optional_tags (item_id = %lld) = %d\n", I->item_id, t);
  return t;
}

static int ihe_advance (iheap_en_t *A) {
  if (!A->cur) { return 0; }
  if (A->cur == A->cur0) { return ihe_advance0 (A); }
  else if (A->cur == A->cur1) { return ihe_advance1 (A); }
  else assert (0);
  return 0;
}

static int ihe_init (iheap_en_t *A, hash_t word, int tag, int optional_tag_weight) {
  int sgn, sp;
  memset (A, 0, sizeof (*A));
  A->tag_word = tag;
  A->optional_tag_weight = optional_tag_weight;
  //commented useless code: after memset
  //A->sp = 0;
  //A->cur = A->cur0 = A->cur1 = 0;

  //  fprintf (stderr, "ihe_init(%p, %016llx)\n", A, word);
  A->word = word;
  A->TS[0] = Root;
  A->Bt[0] = -1;

  A->ihe_skip_advance1 = &ihe_skip_advance1_slow;
  if (init_ilist_decoder (&A->Decoder, word)) {
    if (A->Decoder.dec.dec->tp == le_interpolative_ext) {
      A->ihe_skip_advance1 = &ihe_skip_advance1_fast;
    }
    if (adv_ilist (&A->Decoder) >= 0) {
      A->cur1 = (item_t *) (IndexItems + A->Decoder.doc_id);
      A->cur_y1 = A->Decoder.field;
      // fprintf (stderr, "%d items in index, first=%lld\n", A->Decoder.len, A->cur1->item_id);
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
  //  A->cur_y = A->TS[sp]->y;

  if (ITEM_DELETED(A->cur0)) {
    ihe_advance0 (A);
  }

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

static int t_depth = -1;
static int reoptimize_flag;

struct query_nodes_list query_nodes_list_buffer[MAX_QUERY_NODES];
query_node_t query_nodes_kill_dup_buffer[MAX_QUERY_NODES];
int tot_query_nodes_kill_dup;
int tot_query_nodes_list;

static void merge_query_nodes_lists (struct query_nodes_list *p, struct query_nodes_list *q) {
  while (p->next) {
    p = p->next;
  }
  p->next = q;
}

static struct query_nodes_list *new_query_nodes_list (query_node_t *X) {
  assert (tot_query_nodes_list < MAX_QUERY_NODES);
  struct query_nodes_list *p = &query_nodes_list_buffer[tot_query_nodes_list++];
  p->v = X;
  p->next = 0;
  return p;
}

static void query_convert_to_cumulatative_expression (query_node_t *X) {
  if (!X) { return; }
  query_node_t *Y = X->left, *Z = X->right;
  query_convert_to_cumulatative_expression (Y);
  query_convert_to_cumulatative_expression (Z);
  if (X->op == qn_and || X->op == qn_or) {
    X->op = (X->op == qn_and) ? qn_cum_and : qn_cum_or;
    if (Y->op == X->op) {
      if (Z->op == X->op) {
        //merge list(Y) and list(Z)
        merge_query_nodes_lists (Y->head, Z->head);
        X->head = Y->head;
      } else {
        merge_query_nodes_lists (Y->head, new_query_nodes_list (Z));
        X->head = Y->head;
      }
    } else {
      if (Z->op == X->op) {
        merge_query_nodes_lists (Z->head, new_query_nodes_list (Y));
        X->head = Z->head;
      } else {
        X->head = new_query_nodes_list (Y);
        merge_query_nodes_lists (X->head, new_query_nodes_list (Z));
      }
    }
  }
}

static int cmp_query_node (const void *a, const void *b) {
  const query_node_t *x = *((query_node_t **) a);
  const query_node_t *y = *((query_node_t **) b);
  if (x->op == y->op) {
    if (x->op == qn_word) {
      /* shorter list comes first */
      if (x->frequency < y->frequency) { return -1; }
      if (x->frequency > y->frequency) { return  1; }
      /* same words has same lists lengths */
      if (x->word < y->word) { return -1; }
      if (x->word > y->word) { return  1; }
    }
  } else {
    if (x->op == qn_word) { return -1; }
    if (y->op == qn_word) { return  1; }
  }
  if (x < y) { return -1; }
  if (x > y) { return  1; }
  return 0;
}

/* should be call after tree convertion */
static query_node_t *query_kill_duplicate_words (query_node_t *X) {
  if (!X) { return 0; }
  if (X->op == qn_cum_and || X->op == qn_cum_or) {
    int op = (X->op == qn_cum_and) ? qn_and : qn_or;
    struct query_nodes_list *p;
    int i, k = 0;
    for (p = X->head; p != 0; p = p->next) {
      p->v = query_kill_duplicate_words (p->v);
      k++;
    }
    query_node_t **a = zzmalloc (sizeof (query_node_t *) * k);
    k = 0;
    for (p = X->head; p != 0; p = p->next) {
      a[k++] = p->v;
    }
    for (i = 0; i < k; i++) {
      if (a[i]->op == qn_word) {
        a[i]->frequency = get_word_frequency (a[i]->word);
      }
    }
    qsort (a, k, sizeof (a[0]), cmp_query_node);
    int m = 0;
    /* remove duplicate words */
    for (i = 1; i < k; i++) {
      if (a[i]->op != qn_word || a[i]->word != a[m]->word) {
        a[++m] = a[i];
      }
    }
    query_node_t *w = a[0];
    for (i = 1; i <= m; i++) {
      assert (tot_query_nodes_kill_dup < MAX_QUERY_NODES);
      query_node_t *t = &query_nodes_kill_dup_buffer[tot_query_nodes_kill_dup++];
      t->op = op;
      t->left = w;
      t->right = a[i];
      w = t;
    }
    zzfree (a, sizeof (query_node_t *) * k);
    return w;
  } else {
    X->left = query_kill_duplicate_words (X->left);
    X->right = query_kill_duplicate_words (X->right);
    return X;
  }
}

static void query_optimize_kill_dups (void) {
  tot_query_nodes_kill_dup = tot_query_nodes_list = 0;
  query_convert_to_cumulatative_expression (Q_root);
  Q_root = query_kill_duplicate_words (Q_root);
}

int prepare_query_iterators (query_node_t *X) {
  int i;
  if (!X) {
    return 0;
  }
  if (prepare_query_iterators (X->left) < 0 ||
      prepare_query_iterators (X->right) < 0) {
    return -1;
  }
  X->item_id = (X->op == qn_false ? MAX_ITEM_ID : MIN_ITEM_ID);
  X->priority = 0;
  if (X->op == qn_true && !X->word) {
    X->word = universal_tag_hash;
  } else if (X->op != qn_word || X->iter) {
    return 0;
  }

  /*  --- the following optimization is incorrect in general ---
      i = add_query_word (X->word);
      if (i < 0) {
      return -1;
      }
   */
  if (Q_words >= MAX_WORDS) {
    return -1;
  }
  i = Q_words++;
  IHE[i].sp = -2;

  if (IHE[i].sp == -2) {
    ihe_init (IHE+i, X->word, 0, 0);
  }
  X->iter = &IHE[i];
  X->item_id = X->iter->item_id;
  X->cur = X->iter->cur;
  if (X->item_id == MAX_ITEM_ID) {
    reoptimize_flag++;
    X->op = qn_false;
  }
  X->priority = X->iter->cur_y & 1;
  return 1;
}

void dump_op_type (const char *msg, query_node_t *X) {
  static char *ops[] = {"qn_false", "qn_true", "qn_word", "qn_and", "qn_or", "qn_minus", "qn_error", "qn_cum_and", "qn_cum_or"};
  kprintf ("%s: %s %llx\n", msg, ops[X->op], X->word);
}

long long advance_iterators (query_node_t *X, long long min_item_id) {
  iheap_en_t *A;
  query_node_t *Y, *Z;
  if (X->item_id == MAX_ITEM_ID) {
    return X->item_id;
  }
  switch (X->op) {
    case qn_false:
      X->cur = 0;
      return MAX_ITEM_ID;
    case qn_true:
    case qn_word:
      A = X->iter;
      if (!ihe_skip_advance (A, min_item_id)) {
        X->op = qn_false;
        X->priority = 0;
        reoptimize_flag++;
        X->cur = 0;
        return X->item_id = MAX_ITEM_ID;
      }
      X->priority = A->cur_y & 1;
      X->cur = A->cur;
      return X->item_id = A->item_id;
    case qn_and:
      Y = X->left; Z = X->right;
      if (Y->item_id < min_item_id) {
        if (MAX_ITEM_ID == advance_iterators (Y, min_item_id)) { return X->item_id = MAX_ITEM_ID; }
      }

      while (Y->item_id != Z->item_id) {
        if (Y->item_id < Z->item_id) {
          if (MAX_ITEM_ID == advance_iterators (Y, Z->item_id)) { return X->item_id = MAX_ITEM_ID; }
        } else {
          if (MAX_ITEM_ID == advance_iterators (Z, Y->item_id)) { return X->item_id = MAX_ITEM_ID; }
        }
      }
      X->cur = Y->cur;
      X->priority = Y->priority + Z->priority;
      return X->item_id = Y->item_id;
    case qn_or:
      Y = X->left; Z = X->right;
      if (Y->item_id < min_item_id) {
        advance_iterators (Y, min_item_id);
      }
      if (Z->item_id < min_item_id) {
        advance_iterators (Z, min_item_id);
      }
      if (Y->item_id < Z->item_id) {
        X->priority = Y->priority;
        X->cur = Y->cur;
        return X->item_id = Y->item_id;
      } else if (Y->item_id > Z->item_id) {
        X->priority = Z->priority;
        X->cur = Z->cur;
        return X->item_id = Z->item_id;
      }
      X->cur = Y->cur;
      X->priority = Y->priority + Z->priority;
      return X->item_id = Y->item_id;
    case qn_minus:
      Y = X->left; Z = X->right;
      if (Y->item_id < min_item_id) {
        if (MAX_ITEM_ID == advance_iterators (Y, min_item_id)) { return X->item_id = MAX_ITEM_ID; }
      }

      if (Y->item_id > Z->item_id) {
        advance_iterators (Z, Y->item_id);
      }
      while (Y->item_id == Z->item_id && Y->item_id < MAX_ITEM_ID) {
        if (MAX_ITEM_ID == advance_iterators (Y, Y->item_id + 1)) {
          return X->item_id = MAX_ITEM_ID;
        }
        advance_iterators (Z, Y->item_id);
      }
      X->priority = Y->priority;
      X->cur = Y->cur;
      return X->item_id = Y->item_id;
    default:
      assert (0);
  }
}

static void dump_query_tree (query_node_t *X) {
  if (!X) {
    fprintf (stderr, "(NULL)");
    return;
  }
  switch (X->op) {
    case qn_false:
      fprintf (stderr, "(false)");
      return;
    case qn_true:
      fprintf (stderr, "(true)");
      return;
    case qn_word:
      fprintf (stderr, "(word %016llx, freq = %d)", X->word, get_word_frequency (X->word));
      return;
    case qn_and:
      fprintf (stderr, "(and ");
      break;
    case qn_or:
      fprintf (stderr, "(or ");
      break;
    case qn_minus:
      fprintf (stderr, "(minus ");
      break;
    default:
      fprintf (stderr, "(???%d ", X->op);
  }
  dump_query_tree (X->left);
  fprintf (stderr, " ");
  dump_query_tree (X->right);
  fprintf (stderr, ")");
}


int perform_ext_query (void) {
  long long item_id;

  if (verbosity > 1) {
    fprintf (stderr, "performing extended query: root=%p\n", Q_root);
    dump_query_tree (Q_root);
    fprintf (stderr, "\n");
  }

  assert (Q_root && !Q_words);
  query_optimize_kill_dups ();

  if (verbosity > 1) {
    fprintf (stderr, "after killing duplicate words: root=%p\n", Q_root);
    dump_query_tree (Q_root);
    fprintf (stderr, "\n");
  }

  reoptimize_flag = 0;
  if (prepare_query_iterators (Q_root) < 0) {
    return 0;
  }

  item_id = MIN_ITEM_ID;

  do {
    if (reoptimize_flag) {
      Q_root = optimize_query (Q_root);
      reoptimize_flag = 0;
      if (verbosity > 1) {
        fprintf (stderr, "query after optimization: root=%p\n", Q_root);
        dump_query_tree (Q_root);
        fprintf (stderr, "\n");
      }
    }

    if (Q_root->op == qn_false) {
      break;
    }

    item_id = advance_iterators (Q_root, item_id + 1);
    if (item_id == MAX_ITEM_ID) {
      break;
    }
  } while (store_res (Q_root->cur, Q_root->priority > 10 ? 10 : Q_root->priority));

  postprocess_res();

  return Q_hash_group_mode ? R_tot_groups : R_tot;

}

int has_empty_range () {
  int i = 0;
  for (i = 0; i < n_ranges; i++) {
    if (Q_range[i].minr > Q_range[i].maxr) {
      return 1;
    }
  }
  return 0;
}

static int cmp_iheap_entries (const void *a, const void *b) {
  const iheap_en_t *A = (const iheap_en_t *) a;
  const iheap_en_t *B = (const iheap_en_t *) b;
  int na = A->Decoder.dec.len;
  int nb = B->Decoder.dec.len;
  if (na < nb) { return -1; }
  if (na > nb) { return  1; }
  return 0;
}

static void intersect_lists (void) {
  int i, j;
  qsort (IHE, Q_words, sizeof (IHE[0]), cmp_iheap_entries);
  /* firstly goes shorter lists,
     if intersection of first K list is empty,
     we willn't decode (K+1)-th list
  */

  if (Q_min_priority) {
    while (1) {
      item_t *I = IHE[0].cur;
      if (!I) { break; }
      if (!(IHE[0].cur_y & 1)) {
        if (!ihe_advance (IHE)) { return; }
        continue;
      }
      for (i = 1; i < Q_words; i++) {
        if (!ihe_skip_advance (&IHE[i], I->item_id)) { return; }
        if (IHE[i].cur != I || !(IHE[i].cur_y & 1)) { break; }
      }
      if (i == Q_words) {
        if (!store_res (I, Q_min_priority)) { return; }
        if (!ihe_advance (IHE)) { return; }
      } else {
        if (IHE[i].cur == I) {
          if (!ihe_advance (IHE)) { return; }
        } else {
          if (!ihe_skip_advance (&IHE[0], IHE[i].cur->item_id)) { return; }
        }
      }
    }
  } else {
    while (1) {
      item_t *I = IHE[0].cur;
      if (!I) { break; }
      j = IHE[0].cur_y & 1;
      for (i = 1; i < Q_words; i++) {
        if (!ihe_skip_advance (&IHE[i], I->item_id)) { return; }
        if (IHE[i].cur != I) break;
        j += IHE[i].cur_y & 1;
      }
      if (i == Q_words) {
        if (Q_words > MAX_PRIORITY) {
          j -= Q_words - MAX_PRIORITY;
          if (j < 0) { j = 0; }
        }
        if (!store_res (I, j)) { return; }
        if (!ihe_advance (IHE)) { return; }
      } else {
        if (!ihe_skip_advance (&IHE[0], IHE[i].cur->item_id)) { return; }
      }
    }
  }
}

static int Q_skip_mismatch_words_if_complete_case_found;
static void priority_sort_store_res (item_t *I, int p, int *found) {
  if (Q_skip_mismatch_words_if_complete_case_found) {
    if (p >= 6) {
      if (!(*found)) {
        *found = 1;
        clear_res ();
      }
      store_res (I, p);
    } else {
      if (!(*found)) {
        store_res (I, p);
      }
    }
  } else {
    store_res (I, p);
  }
}

static void priority_sort_query (void) {
  vkprintf (3, "priority_sort_query\n");
  Q_skip_mismatch_words_if_complete_case_found = (Q_order & FLAG_ENTRY_SORT_SEARCH) ? 1 : 0;
  int found = 0;
  int i, j, t, p, tags = 0;
  item_t *I;
  HC = 0;
  for (i = 0; i < Q_words; i++) {
    tags += QT[i];
    if (ihe_init (IHE+i, QW[i], QT[i], 0)) {
      iheap_insert (IHE+i);
    } else {
      if (QT[i]) {
        /* tag not found */
        return;
      }
    }
  }
  vkprintf (3, "HC = %d, tags = %d, Q_words = %d\n", HC, tags, Q_words);

  if (HC + MAX_MISMATCHED_WORDS < Q_words) {
    return;
  }

  I = NULL;
  i = j = t = 0;
  int min_priority = Q_words - MAX_MISMATCHED_WORDS;
  if (min_priority < 1) {
    min_priority = 1;
  }

  if (min_priority <= tags && tags < Q_words) {
    min_priority = tags + 1;
  }
  vkprintf (3, "min_priority = %d\n", min_priority);

  while (HC) {
    vkprintf (3, "item_id = %lld\n", H[1]->cur->item_id);
    assert (H[1]->cur);
    if (H[1]->cur != I) {
      if (I != NULL && i >= min_priority && t == tags) {
        if (!Q_min_priority || i == j) {
          p = 6 - (Q_words - i);
          if (Q_words == i) {
            int dp = 3 - (i - j);
            if (dp < 0) { dp = 0; }
            p += dp;
          }
          priority_sort_store_res (I, p, &found);
        }
      }
      I = H[1]->cur;
      i = 1;
      j = H[1]->cur_y & 1;
      t = H[1]->tag_word;
    } else {
      i++;
      j += H[1]->cur_y & 1;
      t += H[1]->tag_word;
    }
    ihe_advance (H[1]);
    iheap_pop ();
  }
  if (I != NULL && i >= min_priority && t == tags) {
    if (!Q_min_priority || i == j) {
      p = 6 - (Q_words - i);
      if (Q_words == i) {
        int dp = 3 - (i - j);
        if (dp < 0) { dp = 0; }
        p += dp;
      }
      priority_sort_store_res (I, p, &found);
    }
  }
}

static void fast_intersection_query (void) {
  int i;
  for (i = 0; i < Q_words; i++) {
    if (!ihe_init (IHE+i, QW[i], QT[i], 0)) {
      return;
    }
  }
  if (Q_min_priority) {
    Q_min_priority = Q_words;
    if (Q_min_priority > MAX_PRIORITY) {
      Q_min_priority = MAX_PRIORITY;
    }
  }

  if (Q_words == 1) {
    iheap_en_t *H = IHE;
    if (Q_min_priority) {
      while (H->cur) {
        if (H->cur_y & 1) {
          if (!store_res (H->cur, 1)) { break; }
        }
        ihe_advance (H);
      }
    } else {
      while (H->cur) {
        if (!store_res (H->cur, H->cur_y & 1)) { break; }
        ihe_advance (H);
      }
    }
  } else {
    intersect_lists ();
  }
}

int perform_query (void) {
  int i;
  clear_res ();

  if ((!Q_words && !Q_root) || has_empty_range () )  {
    return 0;
  }

  if (verbosity > 1 && t_depth < 0) {
    t_depth = tree_depth (Root, 0);
    fprintf (stderr, "tree depth = %d\n", t_depth);
  }

  if (Q_root) {
    Q_min_priority = 0;
    i = perform_ext_query ();
    free_all_list_decoders ();
    return i;
  }

  vkprintf (3, "Q_order = %x\n", Q_order);
  if (Q_order & FLAG_PRIORITY_SORT_SEARCH) {
    priority_sort_query ();
  } else {
    fast_intersection_query ();
  }

  free_all_list_decoders ();
  postprocess_res ();
  return Q_hash_group_mode ? R_tot_groups : R_tot;
}

struct list_itemid_entry {
  long long item_id;
  struct list_itemid_entry *next;
};

static inline void flushing_binlog_check (void) {
  if (compute_uncommitted_log_bytes () > ((ULOG_BUFFER_SIZE) >> 1)) {
    flush_binlog_forced (0);
  }
}

int do_delete_items_list (struct list_itemid_entry *head) {
  struct list_itemid_entry *p;
  int tot_deleted = 0;
  for (p = head; p != NULL; p = head) {
    if (do_delete_item (p->item_id)) {
      tot_deleted++;
      flushing_binlog_check ();
    }
    head = p->next;
    zfree (p, sizeof (struct list_itemid_entry));
  }
  vkprintf (1, "%d items was deleted.\n", tot_deleted);
  free_all_list_decoders ();
  return tot_deleted;
}

int do_delete_items_with_hash_using_hashset (struct hashset_ll *HS) {
  struct list_itemid_entry *head = NULL, *tail = NULL, *p;
  Q_order = 0;
  clear_res ();

  if (!ihe_init (IHE, universal_tag_hash, 1, 0)) {
    return 0;
  }

  iheap_en_t *H = IHE;
  while (H->cur) {
    const long long item_hash = get_hash_item (H->cur);
    if (item_hash && hashset_ll_get (HS, item_hash)) {
      p = zmalloc (sizeof (struct list_itemid_entry));
      p->item_id = H->cur->item_id;
      p->next = NULL;
      if (head) {
        tail->next = p;
        tail = p;
      } else {
        head = tail = p;
      }
    }
    ihe_advance (H);
  }
  return do_delete_items_list (head);
}

int do_delete_items_with_rate_using_hashset (struct hashset_int *HS, int rate_id) {
  struct list_itemid_entry *head = NULL, *tail = NULL, *p;
  Q_order = 0;
  clear_res ();

  if (!ihe_init (IHE, universal_tag_hash, 1, 0)) {
    return 0;
  }

  iheap_en_t *H = IHE;
  while (H->cur) {
    int rate = get_rate_item (H->cur, rate_id);
    if (rate && hashset_int_get (HS, rate)) {
      p = zmalloc (sizeof (struct list_itemid_entry));
      p->item_id = H->cur->item_id;
      p->next = NULL;
      if (head) {
        tail->next = p;
        tail = p;
      } else {
        head = tail = p;
      }
    }
    ihe_advance (H);
  }
  return do_delete_items_list (head);
}

int do_assign_max_rate_using_hashset (struct hashset_ll *HS, int rate_id, int value) {
  Q_order = 0;
  clear_res ();

  if (!ihe_init (IHE, universal_tag_hash, 1, 0)) {
    return 0;
  }

  iheap_en_t *H = IHE;
  while (H->cur) {
    const long long item_hash = get_hash_item (H->cur);
    if (item_hash && hashset_ll_get (HS, item_hash)) {
      int old_rate = get_rate_item (H->cur, rate_id);
      if (old_rate < value) {
        assign_max_set_rate_calls++;
        do_set_rate_new (H->cur->item_id, rate_id, value);
        flushing_binlog_check ();
      }
    }
    ihe_advance (H);
  }
  free_all_list_decoders ();
  return 1;
}


int do_change_multiple_rates_using_hashmap (struct hashmap_int_int *HM, int rate_id) {
  Q_order = 0;
  clear_res ();

  if (!ihe_init (IHE, universal_tag_hash, 1, 0)) {
    return 0;
  }

  iheap_en_t *H = IHE;
  while (H->cur) {
    int slot;
    int old_rate = get_rate_item (H->cur, rate_id);
    if (old_rate && hashmap_int_int_get (HM, old_rate, &slot)) {
      int new_rate = HM->h[slot].value;
      if (old_rate != new_rate) {
        change_multiple_rates_set_rate_calls++;
        do_set_rate_new (H->cur->item_id, rate_id, new_rate);
        flushing_binlog_check ();
      }
    }
    ihe_advance (H);
  }
  free_all_list_decoders ();
  return 1;
}

int do_delete_items_with_hash (long long hash) {
  struct list_itemid_entry *head = NULL, *tail = NULL, *p;
  if (hash == 0) {
    return 0;
  }
  Q_order = 0;
  clear_res ();

  if (!ihe_init (IHE, universal_tag_hash, 1, 0)) {
    return 0;
  }

  iheap_en_t *H = IHE;
  while (H->cur) {
    if (get_hash_item (H->cur) == hash) {
      p = zmalloc (sizeof (struct list_itemid_entry));
      p->item_id = H->cur->item_id;
      p->next = NULL;
      if (head) {
        tail->next = p;
        tail = p;
      } else {
        head = tail = p;
      }
    }
    ihe_advance (H);
  }
  return do_delete_items_list (head);
}

int do_contained_query (long long item_id, char **text) {
  free_all_list_decoders ();
  Q_limit = 0;
  Q_words = Q_nodes = 0;
  parse_ptr = *text;
  Q_root = parse_query_expr ();
  if (!Q_root || *parse_ptr) {
    free_all_list_decoders ();
    *text = parse_ptr;
    return -1;
  }
  if (prepare_query_iterators (Q_root) < 0) {
    free_all_list_decoders ();
    return -1;
  }
  int r = (item_id == advance_iterators (Q_root, item_id)) ? 1 : 0;
  free_all_list_decoders ();
  return r;
}

int do_reset_all_rates (int p) {
  if (p < 0 || p >= 14) {
    return -1;
  }
  alloc_log_event (LEV_SEARCH_RESET_ALL_RATES, 8, p);
  return 1;
}
