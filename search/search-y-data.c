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

#define _FILE_OFFSET_BITS 64

#include <assert.h>
#include <stddef.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <math.h>
#include <limits.h>

#include "kdb-binlog-common.h"
#include "kdb-data-common.h"
#include "kdb-search-binlog.h"
#include "search-index-layout.h"
#include "search-y-data.h"
#include "search-y-parse.h"
#include "server-functions.h"
#include "word-split.h"
#include "utils.h"
#include "crc32.h"
#include "vv-tl-parse.h"

#define MIN_ITEM_ID (-1LL << 63)
#define MAX_ITEM_ID (~MIN_ITEM_ID)

/* --------- search data ------------- */

int import_only_mode = 0;
int tot_items, del_items, del_item_instances, mod_items, tot_freed_deleted_items;
int max_search_query_memory = 0;
long long tree_positions_bytes;

item_t *Items[ITEMS_HASH_PRIME];

int searchy_replay_logevent (struct lev_generic *E, int size);

int init_search_data (int schema) {
  replay_logevent = searchy_replay_logevent;
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
static int *IndexPositions;
static struct searchy_index_word *IndexWords;
static char *IndexData;
int idx_items, idx_words;
long long idx_bytes, idx_loaded_bytes;

static long long item_texts_offset, words_offset, hapax_legomena_offset, freq_words_offset, word_index_offset, index_size;

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

  if (Header.magic != SEARCHY_INDEX_MAGIC) {
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
  hapax_legomena_offset = words_offset + (Header.words + 1) * sizeof (struct searchy_index_word);
  freq_words_offset = hapax_legomena_offset;
  word_index_offset = freq_words_offset;
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
  assert (CRC32_Header.crc32_items == ~idx_crc32_complement);
  /* this barrier is needed for list_interpolative_ext_forward_decode_item */
  IndexItems[Header.items].item_id = MAX_ITEM_ID;
  idx_crc32_complement = -1;

  vkprintf (4, "rbytes = %d, Header.index_items_size = %d\n", rbytes, (int) Header.index_items_size);
  idx_loaded_bytes += rbytes;
  vkprintf (1, "%lld bytes for %d items read from index\n", Header.index_items_size, Header.items);

  assert (lseek (fd, words_offset, SEEK_SET) >= 0);

  s = (Header.words + 1) * 1LL * sizeof (struct searchy_index_word);
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

  idx_items = Header.items;
  idx_words = Header.words;
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

  assert ((IndexWords[idx_words].file_offset >= word_index_offset && IndexWords[idx_words].file_offset <= fsize) || IndexWords[idx_words].len < 0);

  idx_bytes = IndexWords[idx_words].file_offset - word_index_offset;
  assert (idx_bytes >= 0 && idx_bytes <= MAX_INDEX_BYTES);

  s = idx_bytes;
  /* we never freed this array, so we use zmalloc instead of zzmalloc */
  IndexData = zmalloc (s);
  assert (lseek (fd, word_index_offset, SEEK_SET) >= 0);
  r = read (fd, IndexData, s);
  idx_loaded_bytes += r;
  if (r < s) {
    kprintf ("error reading data from index file: read %ld bytes instead of %ld at position %lld: %m\n", r, s, word_index_offset);
    return -2;
  }

  assert (CRC32_Header.crc32_data == compute_crc32 (IndexData, s));

  s = 4 * idx_items + 4;
  IndexPositions = zmalloc (s);
  r = read (fd, IndexPositions, s);
  idx_loaded_bytes += r;
  if (r < s) {
    kprintf ("error reading index positions from index file: read %ld bytes instead of %ld at position %lld: %m\n", r, s, word_index_offset);
    return -2;
  }
  assert (IndexPositions[idx_items] == compute_crc32 (IndexPositions, s - 4));
  vkprintf (1, "finished loading index: %d items, %d words, %lld index bytes, %lld preloaded bytes\n", idx_items, idx_words, idx_bytes, idx_loaded_bytes);

  //check_lists_decoding ();

  log_split_min = Header.log_split_min;
  log_split_max = Header.log_split_max;
  log_split_mod = Header.log_split_mod;
  assert (log_split_mod > 0 && log_split_min >= 0 && log_split_min + 1 ==  log_split_max && log_split_max <= log_split_mod);

  log_schema = SEARCH_SCHEMA_V1;
  init_search_data (log_schema);

  replay_logevent = searchy_replay_logevent;

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

static struct tree *cur_wordlist_head;

int alloc_tree_nodes, free_tree_nodes;
tree_t free_tree_head = {.left = &free_tree_head, .right = &free_tree_head};
static tree_t *Root;

static tree_t *new_tree_node (int y) {
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

static tree_t *tree_insert (tree_t *T, hash_t word, item_t *item, int y, int *positions) {
  tree_t *P;
  if (!T) {
    P = new_tree_node (y);
    P->word = word;
    P->item = item;
    P->positions = positions;
    return P;
  }
  if (T->y >= y) {
    if (word < T->word || (word == T->word && item->item_id < T->item->item_id)) {
      T->left = tree_insert (T->left, word, item, y, positions);
    } else {
      T->right = tree_insert (T->right, word, item, y, positions);
    }
    return T;
  }
  P = new_tree_node (y);
  P->word = word;
  P->item = item;
  P->positions = positions;
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
  if (T->positions) {
    int sz = 4 * (T->positions[0] + 1);
    tree_positions_bytes -= sz;
    zzfree (T->positions, sz);
    T->positions = NULL;
  }
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

static int searchy_le_start (struct lev_start *E) {
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
  h1 = item_id % ITEMS_HASH_PRIME;
  h2 = 1 + (item_id % (ITEMS_HASH_PRIME - 1));

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
    C = zmalloc0 (sizeof (item_t));
    if (!C) { return C; }
    if (D) {
      zfree (D, sizeof (item_t));
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

static searchy_pair_word_position_t Q[65536];

static void item_add_word (item_t *I, hash_t word, int *positions) {
  tree_t *T = tree_lookup (Root, word, I);
  if (!T) {
    int y = lrand48 ();
    Root = tree_insert (Root, word, I, y, positions);
  } else  {
    assert (T->item == I);
  }
}

static int cmp_word_freq (const void *a, const void *b) {
  const searchy_pair_word_position_t *x = (const searchy_pair_word_position_t *) a,
                                     *y = (const searchy_pair_word_position_t *) b;
  if (x->word < y->word) {
    return -1;
  }
  if (x->word > y->word) {
    return 1;
  }
  return x->position < y->position ? -1 : x->position > y->position ? 1 : 0;
}

static void item_add_words (item_t *I, int Wc) {
  int i, n;
  for (i = 0, n = 0; i < Wc; i++) {
    if (Q[i].position) {
      if (i != n) {
        searchy_pair_word_position_t tmp;
        tmp = Q[i]; Q[i] = Q[n]; Q[n] = tmp;
      }
      n++;
    } else {
      item_add_word (I, Q[i].word, NULL);
    }
  }
  if (!n) {
    return;
  }
  qsort (Q, n, sizeof (Q[0]), cmp_word_freq);
  for (i = 0; i < n; ) {
    hash_t word = Q[i].word;
    int j;
    for (j = i + 1; j < n && word == Q[j].word; j++) {}
    int k = j - i;
    int *positions = zzmalloc (4 * (k + 1));
    tree_positions_bytes += 4 * (k + 1);
    positions[0] = k;
    k = 1;
    while (i < j) {
      positions[k++] = Q[i++].position;
    }
    item_add_word (I, word, positions);
    i = j;
  }
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
    //item_clear_wordlist ((item_t *) II, get_index_item_words_ptr (II, 0));
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
    rates[l++] = log_last_ts;
    mask += 4;
  }

  clear_cur_wordlist ();
  int positions;
  int Wc = searchy_extract_words (text, len, Q, 65536, universal, tag_owner, item_id, &positions);
  item_add_words (I, Wc);
  I->words = cur_wordlist_head;
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
    return 1;
  }
  if (!I || (I->extra & FLAG_DELETED)) { return 0; }
  delete_item_rates (I);
  item_clear_wordlist (I, &I->words);
  del_items++;
  del_item_instances++;
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

int searchy_replay_logevent (struct lev_generic *E, int size) {
  int s;
  switch (E->type) {
    case LEV_START:
      if (size < 24 || E->b < 0 || E->b > 4096) { return -2; }
      s = 24 + ((E->b + 3) & -4);
      if (size < s) { return -2; }
      return searchy_le_start ((struct lev_start *) E) >= 0 ? s : -1;
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
      set_rate_new (((struct lev_search_set_rate *) E)->obj_id, 0, E->a);
      return 16;
    case LEV_SEARCH_SET_RATE2:
      if (size < 16) { return -2; }
      set_rate_new (((struct lev_search_set_rate *) E)->obj_id, 1, E->a);
      return 16;
    case LEV_SEARCH_SET_RATES:
      if (size < 20) { return -2; }
      set_rates (((struct lev_search_set_rates *) E)->obj_id, E->a, E->d);
      return 20;
    case LEV_SEARCH_INCR_RATE_SHORT ... LEV_SEARCH_INCR_RATE_SHORT + 0xff:
      if (size < 12) { return -2; }
      incr_rate_new (((struct lev_search_incr_rate_short *) E)->obj_id, 0, (signed char) E->type);
      return 12;
    case LEV_SEARCH_INCR_RATE:
      if (size < 16) { return -2; }
      incr_rate_new (((struct lev_search_incr_rate *) E)->obj_id, 0, E->a);
      return 16;
    case LEV_SEARCH_INCR_RATE2_SHORT ... LEV_SEARCH_INCR_RATE2_SHORT + 0xff:
      if (size < 12) { return -2; }
      incr_rate_new (((struct lev_search_incr_rate_short *) E)->obj_id, 1, (signed char) E->type);
      return 12;
    case LEV_SEARCH_INCR_RATE2:
      if (size < 16) { return -2; }
      incr_rate_new (((struct lev_search_incr_rate *) E)->obj_id, 1, E->a);
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
      return 13+s;
    case LEV_SEARCH_RESET_ALL_RATES:
      return (size < 8) ? -2 : 8;
  }

  kprintf ("unknown log event type %08x at position %lld\n", E->type, log_cur_pos());

  return -3;
}

/******************** DO functions ********************/

int do_incr_rate_new (long long item_id, int p, int rate_incr) {
  if (!fits (item_id)) { return 0; }
  struct lev_search_incr_rate *IL = alloc_log_event (LEV_SEARCH_INCR_RATE_NEW + p, 16, rate_incr);
  IL->obj_id = item_id;
  return incr_rate_new(item_id, p, rate_incr);
}

int do_set_rate_new (long long item_id, int p, int rate) {
  if (!fits (item_id)) { return 0; }
  struct lev_search_set_rate_new *LR = alloc_log_event (LEV_SEARCH_SET_RATE_NEW + p, 16, rate);
  LR->obj_id = item_id;
  return set_rate_new (item_id, p, rate);
}

int do_delete_item (long long item_id) {
  if (!fits (item_id)) { return 0; }
  struct lev_search_delete_item *ED = alloc_log_event (LEV_SEARCH_DELETE_ITEM, 12, 0);
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

/******************** ylist ********************/
static int bsr (int i) {
  int r, t;
  asm("bsr %1,%0\n\t"
    : "=&q" (r), "=&q" (t)
    : "1" (i)
    : "cc");
  return r;
}

typedef struct {
  list_int_entry_t *head, *tail;
  int num;
} list_int_free_blocks_t;

list_int_free_blocks_t list_int_free_blocks;
static void ylist_positions_free (struct ylist_decoder_stack_entry *data) {
  if (unlikely (!list_int_free_blocks.num)) {
    list_int_free_blocks.head = data->positions_head;
  } else {
    list_int_free_blocks.tail->next = data->positions_head;
  }
  list_int_free_blocks.tail = data->positions_tail;
  list_int_free_blocks.num += data->num;
}

static void ylist_positions_alloc (struct ylist_decoder_stack_entry *data, int num) {
  //fprintf (stderr, "%s: (num = %d)\n", __func__, num);
  if (data->num) {
    ylist_positions_free (data);
  }
  data->num = num;
  int i = 0;
  list_int_entry_t *p;
  if (likely (num <= list_int_free_blocks.num)) {
    //fprintf (stderr, "%d <= %d\n", num, list_int_free_blocks.num);
    p = data->positions_head = list_int_free_blocks.head;
    for (i = 1; i < num; i++) {
      p = p->next;
    }
    data->positions_tail = p;
    list_int_free_blocks.num -= num;
    list_int_free_blocks.head = p->next;
    p->next = NULL;
  } else {
    data->positions_head = data->positions_tail = zmalloc (sizeof (*p));
    data->positions_tail->next = NULL;
    for (i = 1; i < num; i++) {
      p = ztmalloc (sizeof (*p));
      p->next = NULL;
      data->positions_tail->next = p;
      data->positions_tail = p;
    }
  }
}

#define	decode_cur_bit (m < 0)
#define	decode_load_bit()	{ m <<= 1; if (unlikely(m == (-1 << 31))) { m = ((int) *br->ptr++ << 24) + (1 << 23); } }
static void ylist_decode_node (struct ylist_decoder *dec, struct ylist_decoder_stack_entry *data) {
  int middle = (data->left_idx + data->right_idx) >> 1;
  const int hi = data->right_value - (data->right_idx - middle);
  int lo = data->left_value + (middle - data->left_idx), r = hi - lo;
  struct bitreader *br = &dec->br;
  if (r) {
    r++;
    int m = br->m;
    int i = 1;
    while (i < r) {
      i <<= 1;
      if (decode_cur_bit) {
        i++;
      }
      decode_load_bit();
    }
    br->m = m;
    i -= r;
    lo += (r >> 1) + (1 - ((i & 1) << 1)) * (i >> 1) - (i & 1);
/*
    if (i & 1) {
      lo += (r >> 1) - (i >> 1) - 1;
    } else {
      lo += (r >> 1) + (i >> 1);
    }
*/
  }
  data->middle_value = lo;
  struct list_int_entry *p;
  assert (lo >= 0 && lo < idx_items);
  const int N = IndexPositions[lo];
  int K = bread_gamma_code (br);
  struct list_decoder golomb_dec;
  golomb_list_decoder_init (&golomb_dec, N, K, br->start_ptr, bread_get_bitoffset (br));
  ylist_positions_alloc (data, K);
  p = data->positions_head;
  do {
    p->value = golomb_dec.decode_int (&golomb_dec);
    assert (p->value < N);
    if (--K <= 0) {
      break;
    }
    p = p->next;
  } while (1);
  bread_seek (br, bread_get_bitoffset (&golomb_dec.br));
  if (data->right_idx - data->left_idx >= dec->left_subtree_size_threshold) {
    data->right_subtree_offset = bread_gamma_code (br) - 1;
    data->right_subtree_offset += bread_get_bitoffset (br);
  } else {
    data->right_subtree_offset = -1;
  }
}

int decoder_positions_max_capacity = 64;

struct ylist_decoder *zmalloc_ylist_decoder (iheap_en_t *H, int N, int K, const unsigned char *start_ptr, int prefix_bit_offset, int left_subtree_size_threshold) {
  assert (K >= 0);
  const int stack_sz = bsr (K + 1);
  int sz = sizeof (struct ylist_decoder) + sizeof (struct ylist_decoder_stack_entry) * (stack_sz + 1);
  struct ylist_decoder *dec = zmalloc (sz);

  dec->H = H;
  dec->size = sz;
  bread_init (&dec->br, start_ptr, prefix_bit_offset);

  dec->capacity = decoder_positions_max_capacity;
  dec->H->positions1 = dec->positions = zmalloc (4 * dec->capacity);

  dec->N = N;
  dec->K = K;
  dec->p = 0;
  dec->left_subtree_size_threshold = left_subtree_size_threshold;

  struct ylist_decoder_stack_entry *data = dec->stack;
  int i;
  for (i = 0; i <= stack_sz; i++) {
    data[i].num = 0;
  }
  data->left_idx = 0;
  data->left_value = -1;
  data->right_idx = dec->K + 1;
  data->right_value = dec->N;
  ylist_decode_node (dec, data);
  dec->k = 0;

  return dec;
}

/*
static void zfree_ylist_decoder (struct ylist_decoder *dec) {
  int i;
  const int stack_sz = bsr (dec->K + 1);
  struct ylist_decoder_stack_entry *data = dec->stack;
  for (i = 0; i <= stack_sz; i++) {
    if (data->num) {
      ylist_positions_free (data);
    }
    data++;
  }
  zfree (dec->positions, 4 * dec->capacity);
  zfree (dec, dec->size);
}
*/

static void ylist_copy_positions (struct ylist_decoder *dec, struct ylist_decoder_stack_entry *data) {
  if (unlikely (dec->capacity < data->num + 1)) {
    int c = dec->capacity;
    while (c < data->num + 1) {
      c *= 2;
    }
    //zfree (dec->positions, 4 * dec->capacity);
    dec->H->positions1 = dec->positions = zmalloc (4 * c);
    dec->capacity = c;
    if (decoder_positions_max_capacity < c) {
      decoder_positions_max_capacity = c;
    }
  }
  dec->positions[0] = data->num;
  int k;
  struct list_int_entry *p;
  for (k = 0, p = data->positions_head; p != NULL; p = p->next) {
    dec->positions[++k] = p->value;
  }
}

int ylist_decode_int (struct ylist_decoder *dec) {
  if (dec->k >= dec->K) {
    dec->positions[0] = 0;
    return 0x7fffffff;
  }
  dec->k++;
  struct ylist_decoder_stack_entry *data = dec->stack + dec->p;
  for (;;) {
    int middle = (data->left_idx + data->right_idx) >> 1;
    for (;;) {
      if (dec->k == middle) {
        ylist_copy_positions (dec, data);
        return data->middle_value;
      }
      if (dec->k < data->right_idx) { break; }
      dec->p--;
      data--;
      middle = (data->left_idx + data->right_idx) >> 1;
    }
    dec->p++;
    struct ylist_decoder_stack_entry *next = data + 1;
    if (dec->k < middle) {
      next->left_idx = data->left_idx;
      next->left_value = data->left_value;
      next->right_idx = middle;
      next->right_value = data->middle_value;
      ylist_decode_node (dec, next);
      data = next;
    } else {
      next->left_idx = middle;
      next->left_value = data->middle_value;
      next->right_idx = data->right_idx;
      next->right_value = data->right_value;
      ylist_decode_node (dec, next);
      data = next;
    }
  }
}

static void ylist_uptree (struct ylist_decoder *dec, struct ylist_decoder_stack_entry *data, int idx) {
  dec->k = idx;
  for (;;) {
    data--;
    (dec->p)--;
    assert (dec->p >= 0);
    int middle = (data->left_idx + data->right_idx) >> 1;
    if (middle == idx) {
      ylist_copy_positions (dec, data);
      return;
    }
  }
}

static inline long long docid_to_itemid (int doc_id) {
  if (unlikely(doc_id < 0)) {
    return -1LL;
  }
  if (unlikely(doc_id >= idx_items)) {
    return MAX_ITEM_ID;
  }
  return IndexItems[doc_id].item_id;
}

int ylist_forward_decode_item (struct ylist_decoder *dec, long long item_id_lowerbound) {
  struct ylist_decoder_stack_entry *data = dec->stack;

  int p = dec->p;
  data += dec->p;
  while (docid_to_itemid (data->right_value) <= item_id_lowerbound) {
    data--;
    p--;
  }

  if (p < dec->p) {
    int middle = (data->left_idx + data->right_idx) >> 1;
    if (data->right_subtree_offset < 0) {
      while (dec->k < middle) {
        ylist_decode_int (dec);
      }
    } else {
      bread_seek (&dec->br, data->right_subtree_offset);
      dec->k = middle;
    }
    dec->p = p;
  }

  for ( ; ; dec->p++, data++) {
    int middle = (data->left_idx + data->right_idx) >> 1;
    if (docid_to_itemid (data->middle_value) == item_id_lowerbound) {
      if (data->right_subtree_offset < 0) {
        while (dec->k < middle) {
          ylist_decode_int (dec);
        }
      } else {
        bread_seek (&dec->br, data->right_subtree_offset);
        dec->k = middle;
      }
      ylist_copy_positions (dec, data);
      return data->middle_value;
    }
    const int l = data->right_idx - data->left_idx;
    assert (l >= 2);
    if (l == 2) {
      assert (docid_to_itemid (data->right_value) >= item_id_lowerbound);
      if (docid_to_itemid (data->middle_value) < item_id_lowerbound) {
        if (data->right_idx == dec->K + 1) {
          return -1;
        }
        ylist_uptree (dec, data, data->right_idx);
        return data->right_value;
      }
      if (docid_to_itemid (data->left_value) < item_id_lowerbound) {
        dec->k = middle;
        ylist_copy_positions (dec, data);
        return data->middle_value;
      }
      //assert (data->left_value >= doc_id_lowerbound);
      ylist_uptree (dec, data, data->left_idx);
      return data->left_value;
    }
    struct ylist_decoder_stack_entry *next = data + 1;
    if (docid_to_itemid (data->middle_value) > item_id_lowerbound) {
      // left subtree
      if (data->left_idx == middle - 1) {
        dec->k = middle;
        ylist_copy_positions (dec, data);
        return data->middle_value;
      }
      next->left_idx = data->left_idx;
      next->left_value = data->left_value;
      next->right_idx = middle;
      next->right_value = data->middle_value;
      ylist_decode_node (dec, next);
    } else {
      if (data->right_subtree_offset < 0) {
        while (dec->k < middle) {
          ylist_decode_int (dec);
        }
      } else {
        bread_seek (&dec->br, data->right_subtree_offset);
        dec->k = middle;
      }
      next->left_idx = middle;
      next->left_value = data->middle_value;
      next->right_idx = data->right_idx;
      next->right_value = data->right_value;
      ylist_decode_node (dec, next);
    }
  }
  assert (0);
  return -1;
}

static void init_decoder (iheap_en_t *H, ilist_decoder_t *D, int N, int K, const void *file_offset, int term) {
  unsigned char *ptr;
  vkprintf (3, "init_decoder (N = %d, K = %d)\n", N, K);
  if (K < 0) {
    K *= -1;
    ptr = (unsigned char *) file_offset;
  } else {
    long long offs;
    memcpy (&offs, file_offset, 8);
    assert (offs >= word_index_offset && offs < index_size);
    offs -= word_index_offset;
    assert (offs >= 0 && offs < idx_bytes);
    ptr = (unsigned char *)(IndexData + offs);
  }
  D->term = term;
  if (term) {
    D->dec.term_dec = zmalloc_ylist_decoder (H, N, K, ptr, 0, Header.left_subtree_size_threshold);
  } else {
    D->dec.tag_dec = zmalloc_list_decoder_ext (N, K, ptr, le_interpolative_ext, 0, Header.left_subtree_size_threshold);
  }
  D->remaining = K;
  D->len = K;
}

/*
static void free_decoder (ilist_decoder_t *D) {
  if (D->term) {
    if (D->dec.term_dec) {
      zfree_ylist_decoder (D->dec.term_dec);
      D->dec.term_dec = NULL;
    }
  } else {
    if (D->dec.tag_dec) {
      zfree_list_decoder (D->dec.tag_dec);
      D->dec.tag_dec = NULL;
    }
  }
}
*/

int init_ilist_decoder (iheap_en_t *H, ilist_decoder_t *D, hash_t word) {
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
    return 0;
  }

  D->sword = IndexWords + a;
  init_decoder (H, D, idx_items, D->sword->len, &D->sword->file_offset, searchy_is_term (word));
  return 1;
}

static int adv_ilist (ilist_decoder_t *D) {
  while (D->remaining > 0) {
    D->remaining--;
    D->doc_id = D->term ? ylist_decode_int (D->dec.term_dec) : D->dec.tag_dec->decode_int (D->dec.tag_dec);
    assert (D->doc_id >= 0 && D->doc_id < idx_items);
    struct index_item *II = IndexItems + D->doc_id;
    if (!(II->extra & FLAG_DELETED)) {
      return D->doc_id;
    }
  }
  return D->doc_id = -1;
}

/*
 *
 *  SEARCH ENGINE
 *
 */

struct query_range  {
  int minr;
  int maxr;
  int idx;
};

struct query_range Q_range[MAX_RATES + 2];
int n_ranges;

int has_empty_range (void) {
  int i = 0;
  for (i = 0; i < n_ranges; i++) {
    if (Q_range[i].minr > Q_range[i].maxr) {
      return 1;
    }
  }
  return 0;
}

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

int Q_order, Q_limit, Q_type;
static searchy_iheap_phrase_t Phrases[SEARCHY_MAX_QUERY_PHRASES];
static iheap_en_t *Terms[SEARCHY_MAX_QUERY_WORDS];
static iheap_en_t IHE[SEARCHY_MAX_QUERY_WORDS];

int R_cnt, R_tot;
item_t *R[MAX_RES+1];
int RV[MAX_RES+1];

static inline unsigned int get_object_id (long long item_id) {
  unsigned int r = item_id >> 32;
  return r ? r : (unsigned int) item_id;
}

static int get_rating_as_object_id (item_t *I) {
  return get_object_id (I->item_id);
}

static int random_rating (item_t *I) {
  return lrand48 ();
}

static int get_rating (item_t *I) {
  return get_rate_item_fast (I, Q_type);
}

int (*evaluate_rating) (item_t *) = NULL;

static void init_order (void) {
  Q_type = Q_order & 0xff;
  vkprintf (3, "Q_order = %d, Q_type = %d\n", Q_order, Q_type);
  evaluate_rating = NULL;
  if (Q_type == MAX_RATES + 1) {
    evaluate_rating = random_rating;
  } else if (Q_type == MAX_RATES) {
    evaluate_rating = get_rating_as_object_id;
  } else {
    assert (Q_type <= 15);
    evaluate_rating = get_rating;
  }
}

void clear_res (void) {
  R_cnt = R_tot = 0;
  init_order ();
}

/*
  returns 1 in case continue search
  returns 0 in case stop search (for example too many items found case)
*/

static int store_res (item_t *I) {
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
    return 1;
  }

  if (Q_type == MAX_RATES) { //sort by id
    if ((Q_order & FLAG_REVERSE_SEARCH) && R_cnt == Q_limit) {
      R_cnt = 0;
    }
    if (R_cnt < Q_limit) {
      R[R_cnt++] = I;
    }
    return 1;
  }

  r = evaluate_rating (I);

  if (Q_order & FLAG_REVERSE_SEARCH) {
    r = -(r + 1);
  }

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

  if (Q_order & FLAG_REVERSE_SEARCH) {
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

static void postprocess_res (void) {
  int i, k;
  item_t *t;
  if (Q_type == MAX_RATES) { //sort by id
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

/*
static int add_query_word (hash_t word) __attribute__ ((warn_unused_result));
static int add_query_word (hash_t word) {
  int i;
  for (i = 0; i < Q_words; i++) {
    if (IHE[i].word == word) {
      return i;
    }
  }
  if (i == MAX_WORDS) {
    tl_fetch_set_error_format (TL_ERROR_QUERY_INCORRECT, "too many query words, MAX_WORDS is %d", MAX_WORDS);
    return -1;
  }
  vkprintf (3, "add_query_word (%llx)\n", word);
  IHE[i].word = word;
  return Q_words++;
}
*/

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
  vkprintf (3, "%s: type:%d, min_value:%d, max_value:%d\n", __func__, type, min_value, max_value);
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

static int ihe_load_I1_set (iheap_en_t *A) {
  item_t *I0 = A->cur0, *I1 = A->cur1;
  if (likely (I0 == NULL || I0->item_id > I1->item_id)) {
    A->item_id = (A->cur = I1)->item_id;
    A->positions = A->positions1;
    return 1;
  }
  A->item_id = (A->cur = I0)->item_id;
  assert (A->sp >= 0);
  A->positions = A->TS[A->sp]->positions;
  return 1;
}

static int ihe_load (iheap_en_t *A) {
  item_t *I0 = A->cur0, *I1 = A->cur1;

  if (likely (I1 != NULL)) {
    if (likely (I0 == NULL || I0->item_id > I1->item_id)) {
      A->item_id = (A->cur = I1)->item_id;
      A->positions = A->positions1;
      return 1;
    }
    A->item_id = (A->cur = I0)->item_id;
    assert (A->sp >= 0);
    A->positions = A->TS[A->sp]->positions;
    return 1;
  }
  if (I0) {
    A->item_id = (A->cur = I0)->item_id;
    assert (A->sp >= 0);
    A->positions = A->TS[A->sp]->positions;
    return 1;
  }
  A->item_id = MAX_ITEM_ID;
  A->cur = NULL;
  A->positions = NULL;
  return 0;
}

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
  } while (0);

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
    return ihe_load_I1_set (A);
  }
  A->cur1 = NULL;
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

static void ihe_skip_advance1 (iheap_en_t *A, long long item_id) {
  if (A->cur1->item_id >= item_id) {
    return;
  }
  ilist_decoder_t *D = &A->Decoder;
  if (D->term) {
    struct ylist_decoder *dec = D->dec.term_dec;
    D->doc_id = ylist_forward_decode_item (dec, item_id);
    if (D->doc_id < 0) {
      D->remaining = 0;
      A->cur1 = 0;
      return;
    }
    D->remaining = D->len - dec->k;
    for (;;) {
      struct index_item *II = IndexItems + D->doc_id;
      if (!(II->extra & FLAG_DELETED)) {
        A->cur1 = (item_t *) II;
        return;
      }
      if (D->remaining <= 0) {
        D->remaining = 0;
        A->cur1 = 0;
        return;
      }
      D->remaining--;
      D->doc_id = ylist_decode_int (dec);
    }
  } else {
    struct list_decoder *dec = D->dec.tag_dec;
    D->doc_id = list_interpolative_ext_forward_decode_item (dec, item_id);
    if (D->doc_id < 0) {
      D->remaining = 0;
      A->cur1 = 0;
      return;
    }
    D->remaining = D->len - dec->k;
    for (;;) {
      struct index_item *II = IndexItems + D->doc_id;
      if (!(II->extra & FLAG_DELETED)) {
        A->cur1 = (item_t *) II;
        return;
      }
      if (D->remaining <= 0) {
        D->remaining = 0;
        A->cur1 = 0;
        return;
      }
      D->remaining--;
      D->doc_id = dec->decode_int (dec);
    }
  }
}

inline static int ihe_skip_advance (iheap_en_t *A, long long item_id) {
  if (A->cur0) {
    ihe_skip_advance0 (A, item_id);
  }
  if (A->cur1) {
    ihe_skip_advance1 (A, item_id);
  }
  return ihe_load (A);
}

static int ihe_advance (iheap_en_t *A) {
  assert (A->cur);
  if (likely (A->cur == A->cur1)) { return ihe_advance1 (A); }
  assert (A->cur == A->cur0);
  return ihe_advance0 (A);
}

static int ihe_init (iheap_en_t *A) {
  const hash_t word = A->word;
  int sgn, sp;
  memset (A, 0, sizeof (*A));

  //commented useless code: after memset
  //A->sp = 0;
  //A->cur = A->cur0 = A->cur1 = 0;

  A->word = word;
  A->TS[0] = Root;
  A->Bt[0] = -1;

  if (init_ilist_decoder (A, &A->Decoder, word)) {
    if (adv_ilist (&A->Decoder) >= 0) {
      A->cur1 = (item_t *) (IndexItems + A->Decoder.doc_id);
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

  return ihe_load (A);
}

//static int HC;
//static iheap_en_t *H[MAX_WORDS+1];

static int cmp_ptr_iheap_entries (const void *a, const void *b) {
  const iheap_en_t *A = *((const iheap_en_t **) a);
  const iheap_en_t *B = *((const iheap_en_t **) b);
  int na = A->Decoder.len;
  int nb = B->Decoder.len;
  if (na < nb) { return -1; }
  if (na > nb) { return  1; }
  return 0;
}

static int phrase_count_extra_words (searchy_iheap_phrase_t *P) {
  const int terms = P->words;
  //fprintf (stderr, "%s: terms = %d\n", __func__, terms);
  int i, *o = alloca (sizeof (o[0]) * terms), **pos = alloca (sizeof (pos[0]) * terms);
  for (i = 0; i < terms; i++) {
    pos[i] = P->E[i]->positions;
    assert (pos[i]);
    (pos[i])++;
  }
  memset (o, 0, sizeof (o[0]) * terms);
/*
  if (verbosity >= 3) {
    for (i = 0; i < Q_terms; i++) {
      int j;
      fprintf (stderr, "[");
      for (j = 0; j < pos[i][-1]; j++) {
        fprintf (stderr, " %d", pos[i][j]);
      }
      fprintf (stderr, " ]\n");
    }
  }
*/
  int r = INT_MAX;
  for (i = 0; i < pos[0][-1]; i++) {
    int cur = pos[0][i], j;
    for (j = 1; j < terms; j++) {
      int num = pos[j][-1];
      while (o[j] < num && pos[j][o[j]] <= cur) {
        o[j]++;
      }
      if (o[j] >= num) {
        return r - terms;
      }
      cur = pos[j][o[j]];
    }
    const int l = cur - pos[0][i] + 1;
    if (r > l) {
      //fprintf (stderr, "%s: l = %d, terms = %d\n", __func__, l, terms);
      r = l;
    }
  }
  return r - terms;
}

static int count_max_extra_words (int Q_phrases, int max_extra_words) {
  int i, r = 0;
  for (i = 0; i < Q_phrases; i++) {
    int w = phrase_count_extra_words (Phrases + i);
    if (w > max_extra_words) {
      return INT_MAX;
    }
    if (r < w) {
      r = w;
    }
  }
  return r;
}

static inline int cut_by_reason_of_minus_phrases (long long item_id, int plus_phrases, int phrases) {
  int i;
  for (i = plus_phrases; i < phrases; i++) {
    int j;
    searchy_iheap_phrase_t *A = &Phrases[i];
    for (j = 0; j < A->words; j++) {
      iheap_en_t *I = A->E[j];
      if (I->item_id < item_id) {
        ihe_skip_advance (I, item_id);
      }
      if (I->item_id > item_id) {
        goto next_phrase;
      }
    }
    if (A->words < 2 || phrase_count_extra_words (A) <= 0) {
      return 1;
    }
    next_phrase:;
  }
  return 0;
}

static void fast_intersection_query (int plus_words, int words, int plus_phrases, int phrases) {
  vkprintf (3, "%s: plus_words:%d, words:%d, plus_phrases:%d, phrases:%d\n",
    __func__, plus_words, words, plus_phrases, phrases);
  int i;
  for (i = 0; i < words; i++) {
    if (!ihe_init (IHE + i)) {
      return;
    }
  }

  iheap_en_t **H = alloca (plus_words * sizeof (H[0]));
  for (i = 0; i < plus_words; i++) {
    H[i] = IHE + i;
  }
  qsort (H, plus_words, sizeof (H[0]), cmp_ptr_iheap_entries);
  /* firstly goes shorter lists,
     if intersection of first K list is empty,
     we willn't decode (K+1)-th list
  */

  if (verbosity >= 3) {
    for (i = 0; i < plus_words; i++) {
      fprintf (stderr, "H[%d]->Decoder.len = %d\n", i, H[i]->Decoder.len);
    }
  }

  while (1) {
    item_t *I = H[0]->cur;
    if (!I) { break; }
    for (i = 1; i < plus_words; i++) {
      if (!ihe_skip_advance (H[i], I->item_id)) { return; }
      if (H[i]->cur != I) break;
    }
    if (i == plus_words) {
      if (count_max_extra_words (plus_phrases, 0) <= 0 && !cut_by_reason_of_minus_phrases (I->item_id, plus_phrases, phrases)) {
        if (!store_res (I)) { return; }
      }
      if (!ihe_advance (H[0])) { return; }
    } else {
      if (!ihe_skip_advance (H[0], H[i]->cur->item_id)) { return; }
    }
  }
}

static int cmp_hash (const void *a, const void *b) {
  const hash_t *x = (const hash_t *) a,
               *y = (const hash_t *) b;
  return *x < *y ? -1 : *x > *y ? 1 : 0;
}

iheap_en_t *iheap_word_bsearch (iheap_en_t *H, int n, hash_t word) {
  int a = -1, b = n;
  while (b - a > 1) {
    int c = ((a + b) >> 1);
    if (H[c].word <= word) { a = c; } else { b = c; }
  }
  if (a >= 0 && H[a].word == word) {
    return H + a;
  } else {
    return NULL;
  }
}

static int perform_query (char *query) {
  int i;
  if (has_empty_range ()) {
    return 0;
  }
  searchy_query_t Q;
  if (searchy_query_parse (&Q, query) < 0) {
    tl_fetch_set_error (Q.error, TL_ERROR_QUERY_INCORRECT);
    return -1;
  }

  if (!Q.words) {
    return 0;
  }

  int m = 0;
  searchy_query_phrase_t *P;
  hash_t *W = alloca (Q.words * sizeof (W[0]));
  m = 0;
  for (P = Q.phrases[0]; P != NULL; P = P->next) {
    memcpy (W + m, P->H, sizeof (P->H[0]) * P->words);
    m += P->words;
  }
  qsort (W, m, sizeof (W[0]), cmp_hash);
  if (!m) {
    return 0;
  }
  int Q_words = 0;
  for (i = 1; i < m; i++) {
    if (W[i] != W[Q_words]) {
      W[++Q_words] = W[i];
    }
  }
  Q_words++;
  for (i = 0; i < Q_words; i++) {
    IHE[i].word = W[i];
  }
  int Q_plus_words = Q_words;
  int Q_phrases = 0;
  for (P = Q.phrases[0]; P != NULL; P = P->next) {
    if (P->words < 2) {
      continue;
    }
    if (Q_phrases >= SEARCHY_MAX_QUERY_PHRASES) {
      tl_fetch_set_error_format (TL_ERROR_QUERY_INCORRECT, "too many query phrases, SEARCHY_MAX_QUERY_PHRASES is %d", SEARCHY_MAX_QUERY_PHRASES);
      return -1;
    }
    searchy_iheap_phrase_t *A = &Phrases[Q_phrases];
    A->E = Q_phrases ? A[-1].E + A[-1].words : Terms;
    A->words = P->words;
    for (i = 0; i < P->words; i++) {
      A->E[i] = iheap_word_bsearch (IHE, Q_words, P->H[i]);
    }
    Q_phrases++;
  }
  int Q_plus_phrases = Q_phrases;

  for (P = Q.phrases[1]; P != NULL; P = P->next) {
    if (Q_phrases >= SEARCHY_MAX_QUERY_PHRASES) {
      tl_fetch_set_error_format (TL_ERROR_QUERY_INCORRECT, "too many query phrases, SEARCHY_MAX_QUERY_PHRASES is %d", SEARCHY_MAX_QUERY_PHRASES);
      return -1;
    }
    searchy_iheap_phrase_t *A = &Phrases[Q_phrases];
    A->E = Q_phrases ? A[-1].E + A[-1].words : Terms;
    A->words = P->words;
    for (i = 0; i < P->words; i++) {
      A->E[i] = IHE + Q_words;
      IHE[Q_words++].word = P->H[i];
    }
    Q_phrases++;
  }
  clear_res ();
  fast_intersection_query (Q_plus_words, Q_words, Q_plus_phrases, Q_phrases);
  postprocess_res ();
  return R_tot;
}

static long heap_used (void) {
  return (long)(dyn_cur - dyn_first) + (long) (dyn_last - dyn_top);
}

int searchy_perform_query (char *query) {
  long m = -heap_used ();
  memset (&list_int_free_blocks, 0, sizeof (list_int_free_blocks));
  dyn_mark_t mrk;
  dyn_mark (mrk);
  const int res = perform_query (query);
  m += heap_used ();
  if (max_search_query_memory < m) {
    max_search_query_memory = m;
  }
  dyn_release (mrk);
  return res;
}

/******************** debug ********************/

void check_lists_decoding (void) {
  int i;
  double t = -get_rusage_time ();
  long l = heap_used (), max_list_memory = 0;
  for (i = 0; i < idx_words; i++) {
    memset (&list_int_free_blocks, 0, sizeof (list_int_free_blocks));
    dyn_mark_t mrk;
    dyn_mark (mrk);
    memset (IHE, 0, sizeof (IHE[0]));
    ilist_decoder_t *D = &IHE[0].Decoder;
    D->sword = IndexWords + i;
    vkprintf (3, "Decode %d-th word (0x%016llx), len %d.\n", i, D->sword->word, D->sword->len);
    init_decoder (IHE, D, idx_items, D->sword->len, &D->sword->file_offset, searchy_is_term (D->sword->word));
    while (D->remaining > 0) {
      assert (adv_ilist (D) >= 0);
    }
    long t = heap_used ();
    dyn_release (mrk);
    t -= heap_used ();
    if (max_list_memory < t) {
      max_list_memory = t;
    }
  }
  assert (heap_used () == l);
  t += get_rusage_time ();
  vkprintf (1, "finish %d lists decoding (decoding time: %.6lf seconds).\n", idx_words, t);
  vkprintf (1, "max memory usage: %ld\n", max_list_memory);
  exit (0);
}
