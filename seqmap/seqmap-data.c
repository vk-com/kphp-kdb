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
              2013 Vitaliy Valtman
*/

#define _FILE_OFFSET_BITS 64
#include <assert.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <ctype.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/mman.h>
#include <sys/wait.h>
#include <signal.h>
#include <stddef.h>

#include "crc32.h"
#include "kdb-data-common.h"
#include "server-functions.h"
#include "kdb-binlog-common.h"
#include "kfs.h"
#include "kdb-seqmap-binlog.h"
#include "net-events.h"

#include "seqmap-data.h"
#include "vv-tl-aio.h"

struct item *item_tree;
struct item *min_item, *max_item;

int index_entries;

struct timeq timeq[MAX_TIME]; 

void init_tree (void) {
  int i;
  for (i = 0; i < MAX_TIME; i++) {
    struct item *I = (struct item *)(((char *)(timeq + i)) - offsetof (struct item, prev_time));
    I->prev_time = I->next_time = I;
  }
  for (i = 0; i < MAX_TIME; i++) {
    struct item *I = (struct item *)(((char *)(timeq + i)) - offsetof (struct item, prev_time));
    assert (I->prev_time == I && I->next_time == I);
  }
}


/* Memory functions {{{ */
long long zalloc_mem;
long long malloc_mem;
extern long long max_memory;
int items_count;
long long items_memory;

long long get_memory_used (void) {
  return zalloc_mem + malloc_mem;
}

void *zzmalloc (int size) {
  if (size < 0) { return 0; }
  void *res;

  if (get_memory_used () > max_memory) {
    fprintf (stderr, "too much memory used: %lld of %lld\n", (long long) get_memory_used (), (long long) max_memory);
//    fprintf (stderr, "memory distributes as follow: %lld to current, %lld to cache, %lld to metafiles, %lld to init data\n", entry_memory, cache_size, allocated_metafile_bytes, init_memory);
  }
  assert (get_memory_used () <= max_memory);

  if (size < MAX_ZMALLOC_MEM) {
    if (!(res = dyn_alloc (size, PTRSIZE))) {
      fprintf (stderr, "Out of memory\n");
    }
    assert (res);      
    zalloc_mem += size;
  } else {
    if (!(res = malloc (size))) {
      fprintf (stderr, "Out of memory\n");
    }
    assert (res);      
    malloc_mem += size;
  }

  assert(res);
  return res;
}

void zzfree (void *ptr, int size) {
  if (size < 0) return;
  if (size < MAX_ZMALLOC_MEM) {
    zalloc_mem -= size;
    zfree (ptr, size);
  } else {
    malloc_mem -= size;
    free (ptr);
  }
}

int memory_full_warning (void) {
  return 0;
}

void redistribute_memory (void) {
}
/* }}} */


#define min(x,y) ((x) < (y) ? (x) : (y))
int key_cmp2 (int l1, const int *k1, int l2, const int *k2, int s) {
  int i;
  int l = min (l1, l2);
  for (i = s; i < l; i++) {
    if (k1[i] < k2[i]) {
      return -(i + 1);
    } else if (k1[i] > k2[i]) {
      return (i + 1);
    }
  }
  if (l1 < l2) {
    return -(i + 1);
  } else if (l1 > l2) {
    return (i + 1);
  } else {
    return 0;
  }
}

int key_cmp (int l1, const int *k1, int l2, const int *k2) {
  return key_cmp2 (l1, k1, l2, k2, 0);
}

int item_cmp (const struct item *L, const struct item *R) {
  if (L == R) { return 0; }
  //if (L == min_item || R == max_item) { return -1; }
  //if (L == max_item || R == min_item) { return 1; }
  return key_cmp (L->key_len, L->key, R->key_len, R->key);
}

/* Tree {{{ */

void update_counters (struct item *T) {
  if (!T) { return; }
  T->size = 1;
  int x = NODE_TYPE_T (T);
  int u = NODE_TYPE_S (T);
  if (x == NODE_TYPE_PLUS) {
    T->delta = 1;
    if (u == NODE_TYPE_UNSURE) {
      T->plus_unsure = 1;
    }
    T->minus_unsure = 0;
  } else if (x == NODE_TYPE_MINUS) {
    T->delta = -1;
    if (u == NODE_TYPE_UNSURE) {
      T->minus_unsure = 1;
    }
    T->plus_unsure = 0;
  } else {
    assert (x == NODE_TYPE_ZERO);
    T->delta = 0;
    assert (u == NODE_TYPE_SURE);
    T->minus_unsure = 0;
    T->plus_unsure = 0;
  }
  if (T->left) {
    T->size += T->left->size;
    T->delta += T->left->delta;
    T->minus_unsure += T->left->minus_unsure;
    T->plus_unsure += T->left->plus_unsure;
  }
  if (T->right) {
    T->size += T->right->size;
    T->delta += T->right->delta;
    T->minus_unsure += T->right->minus_unsure;
    T->plus_unsure += T->right->plus_unsure;
  }
}

static void tree_count_one (struct item *T, int Z[3]) {
  int x = NODE_TYPE_T (T);
  int u = NODE_TYPE_S (T) == NODE_TYPE_UNSURE;
  if (x == NODE_TYPE_PLUS) {
    Z[0] ++;
    if (u) { Z[2] ++; }
  } else if (x == NODE_TYPE_MINUS) {
    Z[0] --;
    if (u) { Z[1] ++; }
  }
}

static void tree_get_counters (struct item *T, struct item *L, int Z[3]) __attribute__ ((unused));
static void tree_get_counters (struct item *T, struct item *L, int Z[3]) {
  if (!T) { return; }
  int c = item_cmp (T, L);
  if (c > 0) {
    tree_get_counters (T->left, L, Z);
  } else {
    if (c) {
      tree_get_counters (T->right, L, Z);
      tree_count_one (T, Z);
    }
    if (T->left) {
      Z[0] += T->left->delta;
      Z[1] += T->left->minus_unsure;
      Z[2] += T->left->plus_unsure;
    }
  }
}

static void tree_count (struct item *T, int left_len, const int *left, int right_len, const int *right, int Z[3]) {
  struct item *L = T;
  struct item *R = T;
  while (L || R) {    
    int c1 = L ? key_cmp (L->key_len, L->key, left_len, left) : 0;
    int c2 = R ? key_cmp (R->key_len, R->key, right_len, right) : 0;
    if (L == R) {
      if (c1 <= 0) {
        if (c1 == 0) {
          tree_count_one (L, Z);
        }
        L = R = L->right;
      } else if (c2 >= 0) {
        if (c2 == 0) {
          tree_count_one (L, Z);
        }
        L = R = L->left;
      } else {
        tree_count_one (L, Z);
        L = L->left;
        R = R->right;
      }
    } else {
      if (L) {
        if (c1 >= 0) {
          if (L->right) {
            Z[0] += L->right->delta;
            Z[1] += L->right->minus_unsure;
            Z[2] += L->right->plus_unsure;
          }
          tree_count_one (L, Z);
          L = L->left;
        } else {
          L = L->right;
        }
      }
      if (R) {
        if (c2 <= 0) {
          if (R->left) {
            Z[0] += R->left->delta;
            Z[1] += R->left->minus_unsure;
            Z[2] += R->left->plus_unsure;
          }
          tree_count_one (R, Z);
          R = R->right;
        } else {
          R = R->left;
        }
      }
    }
  }
}

static struct item *tree_lookup (struct item *T, int key_len, const int *key) {
  int c;
  int lc = 1;
  int rc = 1;
  while (T && (c = key_cmp2 (key_len, key, T->key_len, T->key, min (lc, rc) - 1))) {
//  while (T && (c = key_cmp2 (key_len, key, T->key_len, T->key, 0))) {
    T = (c < 0) ? T->left : T->right;
    if (c < 0) {
      rc = -c;
    } else {
      assert (c > 0);
      lc = c;
    }
  }
  return T;
}

static struct item *tree_lookup_next (struct item *T, int key_len, const int *key, int lc, int rc) __attribute__ ((unused));
static struct item *tree_lookup_next (struct item *T, int key_len, const int *key, int lc, int rc) {
  if (!T) {
    return 0;
  }
  int c = key_cmp2 (key_len, key, T->key_len, T->key, min (lc, rc) - 1);
  if (c < 0) {
    assert (-c >= min (lc, rc));
    struct item *N = tree_lookup_next (T->left, key_len, key, lc, -c);
    return N ? N : T;
  } else {
    if (c == 0) { c = 2000; }
    assert (c >= min (lc, rc));
    return tree_lookup_next (T->right, key_len, key, c, rc); 
  }
}

static struct item *tree_lookup_prev (struct item *T, int key_len, const int *key, int lc, int rc) __attribute__ ((unused));
static struct item *tree_lookup_prev (struct item *T, int key_len, const int *key, int lc, int rc) {
  if (!T) {
    return 0;
  }
  int c = key_cmp2 (key_len, key, T->key_len, T->key, min (lc, rc) - 1);
  if (c <= 0) {
    if (c == 0) { c = -2000; }
    assert (-c >= min (lc, rc));
    return tree_lookup_prev (T->left, key_len, key, lc, -c);
  } else {
    assert (c >= min (lc, rc));
    struct item *N = tree_lookup_prev (T->right, key_len, key, c, rc);
    return N ? N : T;
  }
}


static struct item *tree_lookup_next_or_eq (struct item *T, int key_len, const int *key, int lc, int rc) __attribute__ ((unused));
static struct item *tree_lookup_next_or_eq (struct item *T, int key_len, const int *key, int lc, int rc) {
  if (!T) {
    return 0;
  }
  int c = key_cmp2 (key_len, key, T->key_len, T->key, min (lc, rc) - 1);
  if (c == 0) { return T; }
  if (c < 0) {
    assert (-c >= min (lc, rc));
    struct item *N = tree_lookup_next_or_eq (T->left, key_len, key, lc, -c);
    return N ? N : T;
  } else {
    assert (c >= min (lc, rc));
    return tree_lookup_next_or_eq (T->right, key_len, key, c, rc); 
  }
}

static struct item *tree_lookup_prev_or_eq (struct item *T, int key_len, const int *key, int lc, int rc) __attribute__ ((unused));
static struct item *tree_lookup_prev_or_eq (struct item *T, int key_len, const int *key, int lc, int rc) {
  if (!T) {
    return 0;
  }
  int c = key_cmp2 (key_len, key, T->key_len, T->key, min (lc, rc) - 1);
  if (c == 0) { return T; }
  if (c <= 0) {
    assert (-c >= min (lc, rc));
    return tree_lookup_prev_or_eq (T->left, key_len, key, lc, -c);
  } else {
    assert (c >= min (lc, rc));
    struct item *N = tree_lookup_prev_or_eq (T->right, key_len, key, c, rc);
    return N ? N : T;
  }
}

/*
int iterator_report (struct hash_entry *hash_entry);
int tree_lookup_all_next (struct item *T, const char *key, int key_len, int prefix_len, int strict, int lc, int rc) {
  //vkprintf (1, "%s - %s [%d - %d]\n", key, T ? T->x->key : "NULL", lc, rc);
  if (!T) {
    return 0;
  }
  int c = mystrcmp2 (key, key_len, T->x->key, T->x->key_len, min (lc, rc) - 1);
  if (c < 0 || c > prefix_len) {
    int t = tree_lookup_all_next (T->left, key, key_len, prefix_len, strict, lc, c < 0 ? -c : c);
    if (t < 0) { return t; }
  }
  if ((c == 0 && !strict) || c < -prefix_len || c > prefix_len) {
    int t = iterator_report (T->x);
    if (t < 0) { return t; }
  }
  if (c > 0 || c < -prefix_len) {
    int t = tree_lookup_all_next (T->right, key, key_len, prefix_len, strict, c < 0 ? -c : c, rc);
    if (t < 0) { return t; }
  }
  return 0;
}*/

static void tree_split (struct item **L, struct item **R, struct item *T, int key_len, const int *key) {
  if (!T) { *L = *R = 0; return; }
  if (key_cmp (key_len, key, T->key_len, T->key) < 0) {
    *R = T;
    tree_split (L, &T->left, T->left, key_len, key);
  } else {
    *L = T;
    tree_split (&T->right, R, T->right, key_len, key);
  }
  update_counters (T);
}

static struct item *tree_insert (struct item *T, struct item *N) __attribute__ ((warn_unused_result));
static struct item *tree_insert (struct item *T, struct item *N) {
  if (!T) { 
    update_counters (N);
    return N;
  }
  int c;
  assert (c = key_cmp (N->key_len, N->key, T->key_len, T->key));
  if (T->y >= N->y) {
    if (c < 0) {
      T->left = tree_insert (T->left, N);
    } else {
      T->right = tree_insert (T->right, N);
    }
    update_counters (T);
    return T;
  }
  tree_split (&N->left, &N->right, T, N->key_len, N->key);
  update_counters (N);
  return N;
}

static struct item *tree_merge (struct item *L, struct item *R) __attribute__ ((warn_unused_result));
static struct item *tree_merge (struct item *L, struct item *R) {
  if (!L) { return R; }
  if (!R) { return L; }
  if (L->y > R->y) {
    L->right = tree_merge (L->right, R);
    update_counters (L);
    return L;
  } else {
    R->left = tree_merge (L, R->left);
    update_counters (R);
    return R;
  }
}

static struct item *tree_delete (struct item *T, int key_len, const int *key) __attribute__ ((warn_unused_result));
static struct item *tree_delete (struct item *T, int key_len, const int *key) {
  int c;
  assert (T);
  if (!(c = key_cmp (key_len, key, T->key_len, T->key))) {
    struct item *N = tree_merge (T->left, T->right);
    update_counters (N);
    return N;
  }
  if (c < 0) {
    T->left = tree_delete (T->left, key_len, key);
  } else {
    T->right = tree_delete (T->right, key_len, key);
  }
  update_counters (T);
  return T;
}
/* }}} */

/* Index functions {{{ */

void delay_op (struct item *I, int size, void *E) {
  assert (0);
}

struct item *alloc_item (int key_len, const int *key, int type, int value_len);
int load_index (kfs_file_handle_t Index) {
  if (!Index) {
    index_entries = 0;
    min_item = alloc_item (0, 0, NODE_TYPE_PLUS | NODE_TYPE_SURE, 0);
    static int key[1000];
    int i;
    for (i = 0; i < 1000; i++) { key[i] = 0x7fffffff; }
    max_item = alloc_item (1000, key, NODE_TYPE_PLUS | NODE_TYPE_SURE, 0);
    return 0;
  }
  assert (0);
  return 0;
}

int save_index (int writing_binlog) {
  vkprintf (0, "index not implemented\n");
  assert (0);
  return 0;
}

int index_check_metafiles (int left_len, const int *left, int right_len, const int *right) {
  if (!index_entries) {
    return 0;
  }
  assert (0);
  return 0;
}

int index_count_range (int left_len, const int *left, int right_len, const int *right, int min_pos, int max_pos) {
  if (min_pos == max_pos) {
    return 0;
  }
  assert (0);
  return 0;
}

int index_load_unsure (int left_len, const int *left, int right_len, const int *right) {
  assert (0);
  return 0;
}

int get_index_pos (int key_len, const int *key) {
  return 0;
}

struct index_entry *get_index_entry (int p) {
  assert (p >= 0 && p < index_entries);
  return 0;
}

int index_load_metafile (int key_len, const int *key) {
  assert (0);
  return 0;
}
/* }}} */

/* Time functions {{{ */
void delete_time (struct item *I) {
  if (!I->next_time) { return; }
  assert (I->prev_time);
  I->next_time->prev_time = I->prev_time;
  I->prev_time->next_time = I->next_time;
  I->next_time = I->prev_time = 0;
}

void insert_time (struct item *I) {
  if (I->time <= now || I->time >= now + TIME_MASK) { return; }
  assert (!I->next_time && !I->prev_time);
  int d = I->time & TIME_MASK;
  struct item *II = (struct item *)(((char *)(timeq + d)) - offsetof (struct item, prev_time));
  I->next_time = II->next_time;
  I->prev_time = II;
  I->next_time->prev_time = I;
  I->prev_time->next_time = I;
}

int prev_free_by_time;
int delete (int E_size, void *E, int key_len, int *key);
void free_by_time (void) {
  while (prev_free_by_time < now) {
    int x = (prev_free_by_time ++) & TIME_MASK;
    struct item *II = (struct item *)(((char *)(timeq + x)) - offsetof (struct item, prev_time));
    while (II->next_time != II) {
      struct item *I = II->next_time;
      assert (I->value_len >= 0);
      assert (I->key_len >= 0 && I->key_len <= 255);
      assert (delete (0, 0, I->key_len, I->key));
    }
  }
}

void free_after_binlog_read (void) {
  int t = time (NULL);
  int i;
  for (i = t - 1000000; i <= t; i++) {
    int x = i & TIME_MASK;    
    struct item *II = (struct item *)(((char *)(timeq + x)) - offsetof (struct item, prev_time));
    while (II->next_time != II) {
      struct item *I = II->next_time;
      assert (I->value_len >= 0);
      delete (0, 0, I->key_len, I->key);
    }
  }
  prev_free_by_time = t;
}
/* }}} */

struct item *alloc_item (int key_len, const int *key, int type, int value_len) {
  assert (value_len == 0 || value_len == -1);
  struct item *I = zzmalloc (sizeof (*I));
  memset (I, 0, sizeof (*I));
  items_memory += sizeof (*I);
  I->key_len = key_len;
  I->key = zzmalloc (4 * key_len);
  memcpy (I->key, key, 4 * key_len);
  items_memory += key_len * 4;
  I->value_len = value_len;
  I->value = 0;
  items_count ++;
  I->time = -1;
  I->type = type;
  I->y = lrand48 ();

  item_tree = tree_insert (item_tree, I);
  assert (item_tree->size == items_count);
  return I;
};

void free_item (struct item *I) {
  item_tree = tree_delete (item_tree, I->key_len, I->key);
  items_count --;
  assert (item_tree ? (item_tree->size == items_count) : (!items_count));
  if (I->key_len) {
    zzfree (I->key, 4 * I->key_len);
    items_memory -= 4 * I->key_len;
  }
  if (I->value_len) {
    zzfree (I->value, 4 * I->value_len);
    items_memory -= 4 * I->value_len;
  }
  zzfree (I, sizeof (*I));
  items_memory -= sizeof (*I);
}


struct item *preload_key (int key_len, const int *key, int force) {
  struct item *I = tree_lookup (item_tree, key_len, key);
  if (I) { return I; }
  static struct item T;
  I = &T;
  I->key_len = -1;
  I->next_time = I->prev_time = 0;
  return I;
}

void change_node_type (struct item *I, int new_type) {
  item_tree = tree_delete (item_tree, I->key_len, I->key);
  I->type = (I->type & ~7) | new_type;
  item_tree = tree_insert (item_tree, I);
}

void change_value (struct item *I, int value_len, int *value) {
  if (I->value_len >= 0) {
    zzfree (I->value, 4 * I->value_len);
    items_memory -= 4 * I->value_len;
  } else {
    assert (NODE_TYPE (I) == (NODE_TYPE_MINUS | NODE_TYPE_UNSURE));
    change_node_type (I, NODE_TYPE_PLUS | NODE_TYPE_UNSURE);
  }
  I->value_len = value_len;
  I->value = zzmalloc (4 * value_len);
  memcpy (I->value, value, 4 * value_len);
  items_memory += 4 * value_len;
}

void delete_value (struct item *I) {
  if (I->value_len >= 0) {
    if (NODE_TYPE (I) == (NODE_TYPE_PLUS | NODE_TYPE_UNSURE)) {
      zzfree (I->value, 4 * I->value_len);
      items_memory -= 4 * I->value_len;
      I->value_len = -1;
      change_node_type (I, NODE_TYPE_MINUS | NODE_TYPE_UNSURE);
    } else if (NODE_TYPE (I) == (NODE_TYPE_PLUS | NODE_TYPE_SURE)) {
      free_item (I);
    } else {
      assert (NODE_TYPE (I) == (NODE_TYPE_ZERO | NODE_TYPE_SURE));
      zzfree (I->value, 4 * I->value_len);
      items_memory -= 4 * I->value_len;
      I->value_len = -1;
      change_node_type (I, NODE_TYPE_MINUS | NODE_TYPE_SURE);
    }
  }
}

int store (int E_size, void *E, int key_len, int *key, int value_len, int *value, int delay, int mode) {
  assert (key_len >= 0 && key_len <= 255);
  assert (value_len >= 0 && value_len <= (1 << 20));
  assert (mode >= 1 && mode <= 3);
  
  if (delay <= now && delay > 0) {
    return 0;
  }

  struct item *I = preload_key (key_len, key, 0);
  if (I->key_len == -3 && mode != 3) {
    delay_op (I, E_size, E);
    return 1;
  }

//  fprintf (stderr, "key_len = %d, value_len = %d, mode = %d\n", I->key_len, I->value_len, mode);
  if (I->key_len >= 0 && I->value_len >= 0) {
    if (!(mode & 2)) { return 0; }
    delete_time (I);
    change_value (I, value_len, value);
    I->time = delay;
    insert_time (I);
    return 1;
  } else {
    if (!(mode & 1)) { return 0; }
    delete_time (I);
    if (I->key_len >= 0) {
      change_value (I, value_len, value);
      I->time = delay;
      insert_time (I);
      return 1;
    } else {
      struct item *II = alloc_item (key_len, key, NODE_TYPE_PLUS | (I->key_len == -3 ? NODE_TYPE_UNSURE : NODE_TYPE_SURE), 0);
      change_value (II, value_len, value);
      II->time = delay;
      insert_time (II);
      return 1;
    }
  }
}

int delete (int E_size, void *E, int key_len, int *key) {
  assert (key_len >= 0 && key_len <= 255);
  struct item *I = preload_key (key_len, key, 0);
  delete_time (I);
  int t = I->key_len;
  if (I->key_len >= 0) {
    delete_value (I);
  } else {
    if (I->key_len == -3) {
      alloc_item (key_len, key, NODE_TYPE_MINUS | NODE_TYPE_UNSURE, -1);
    }
  }
  return t != -1;
}

struct item *get (int key_len, const int *key) {
  assert (key_len >= 0 && key_len <= 255);
  struct item *I = preload_key (key_len, key, 1);
  if (I->key_len == -2) {
    return 0;
  }
  return I;  
}
/*
int get_range_count (int left_len, const int *left, int right_len, const int *right) {
  if (left_len && right_len && key_cmp (left_len, left, right_len, right) > 0) {
    return 0;
  }
  struct item *LL = left_len ? tree_lookup_prev_or_eq (item_tree, left_len, left, 1, 1) : min_item;
  struct item *LR = left_len ? tree_lookup_next_or_eq (item_tree, left_len, left, 1, 1) : min_item;
  struct item *RL = right_len ? tree_lookup_prev_or_eq (item_tree, right_len, right, 1, 1) : max_item;
  struct item *RR = right_len ? tree_lookup_next_or_eq (item_tree, right_len, right, 1, 1) : max_item;

  if (index_check_metafiles (left_len, left, right_len, right) == -2) {
    return -2;
  }

  if (item_cmp (LR, RL) > 0) {
    assert (LL == RL);
    assert (LR == RR);
    if (LL->max_index_pos == RR->min_index_pos) {
      return 0;
    } else {
      return index_count_range (left_len, left, right_len, right, LL->min_index_pos, RR->max_index_pos);
    }
  } else {
    int LC[3], RC[3];
    memset (LC, 0, sizeof (LC));
    memset (RC, 0, sizeof (RC));
    tree_get_counters (item_tree, LR, LC);
    tree_get_counters (item_tree, RR, RC);
    int t = RC[1] + RC[2] - LC[1] - LC[2];
    assert (t >= 0);
    if (t) {
      index_load_unsure (LR, RR);
      assert (WaitAioArrPos);
      return -2;
    }
    int res = RC[0] - LC[0];
    if (LL->min_index_pos < LR->max_index_pos) {
      int r = index_count_range (left_len, left, LR->key_len, LR->key, LL->min_index_pos, LR->max_index_pos);
      if (r == -2) { return -2; }
      res += r;
    }
    if (RL->min_index_pos < RR->max_index_pos) {
      int r = index_count_range (right_len, right, RR->key_len, RR->key, RL->min_index_pos, RR->max_index_pos);
      if (r == -2) { return -2; }
      res += r;
    }
    return res;
  }
}

int get_range (int left_len, const int *left, int right_len, int *right, int limit, int *R, int size, int *cnt) {
  return 0;
}
*/

int get_range_count (int left_len, const int *left, int right_len, const int *right) {
  if (left_len && right_len && key_cmp (left_len, left, right_len, right) > 0) {
    return 0;
  }
  int min_index_pos = left_len ? 0 : get_index_pos (left_len, left);
  int max_index_pos = right_len ? index_entries : get_index_pos (right_len, right);
  if (min_index_pos == -2 || max_index_pos == -2) {
    return -2;
  }
  int Z[3];
  memset (Z, 0, sizeof (Z));
  tree_count (item_tree, left_len, left, right_len, right, Z);
  if (Z[1] || Z[2]) {
    index_load_unsure (left_len, left, right_len, right);
    return -2;
  } else {
    assert (Z[0] + max_index_pos - min_index_pos >= 0);
    return Z[0] + max_index_pos - min_index_pos;
  }
}

#define R_MAX (1 << 17)
static int RR[R_MAX]; 
static int Rpos;
static int Rmax;
static int Rt; 
static int *B;
static int Bsize;
static int Bpos;
int __index_pos;
int (*__array_report)(struct index_entry *);
int (*__tree_report)(struct item *);


int report_index (int p1, int p2) {
  int i;
  for (i = p1; i < p2; i++) {
    struct index_entry *E = get_index_entry (i);
    if (!E) { return -2; }
    int r = __array_report (E);
    if (r < 0) { return r; }
  }
  return 0;
}

int do_listree_iterator (struct item *T, int left_len, const int *left, int right_len, int *right) {
  if (!T) { return 0; }
  int c = key_cmp (T->key_len, T->key, left_len, left);
  if (c < 0) {
    return do_listree_iterator (T->right, left_len, left, right_len, right);
  } else {
    int r = do_listree_iterator (T->left, left_len, left, right_len, right);
    if (r < 0) { return r; }
    c = key_cmp (T->key_len, T->key, right_len, right);
    if (c > 0) { return 0; }
    int x = NODE_TYPE_T (T);
    if (NODE_TYPE_S (T) == NODE_TYPE_UNSURE) {
      if (index_load_metafile (T->key_len, T->key) == -2) {
        return -2;
      }
    }
    assert (NODE_TYPE_S (T) == NODE_TYPE_SURE);
    r = report_index (__index_pos, T->min_index_pos);
    if (r < 0) { return r; }
    __index_pos = T->min_index_pos;
    if (x == NODE_TYPE_PLUS) {
      int z = __tree_report (T);
      if (z < 0) { return z; }
    } else if (x == NODE_TYPE_ZERO) {
      int z = __tree_report (T);
      if (z < 0) { return z; }
      __index_pos ++;
    } else {
      assert (x == NODE_TYPE_MINUS);
      __index_pos ++;
    }
    return do_listree_iterator (T->right, left_len, left, right_len, right);
  }
}

int listree_iterator (int left_len, const int *left, int right_len, int *right, int min_index_pos, int max_index_pos) {
  __index_pos = min_index_pos;
  int r = do_listree_iterator (item_tree, left_len, left, right_len, right);
  if (r < 0) { return r; }
  r = report_index (__index_pos, max_index_pos);
  if (r < 0) { return r; }
  return 0;
}

int array_ifwrite (struct index_entry *E) {
  assert (0);
  return 0;
}

int tree_ifwrite (struct item *I) {
  if (Rt < Rmax && RR[Rt] == Rpos) {
    Rt ++;
    Rpos ++;
    if (2 + I->key_len + I->value_len < Bsize) {
      B[0] = I->key_len;
      memcpy (B + 1, I->key, 4 * I->key_len);
      B += (1 + I->key_len);
      B[0] = I->value_len;
      memcpy (B + 1, I->value, 4 * I->value_len);
      B += (1 + I->value_len);
      Bsize -= (2 + I->key_len + I->value_len);
      return 0;
    } else {
      Rmax = Rt;
      return -1;
    }
  } else {
    Rpos ++;
    return 0;
  }
}

int cmp (const void *a, const void *b) {
  return *(int *)a - *(int *)b;
}

int get_range (int left_len, const int *left, int right_len, int *right, int limit, int *R, int size, int *cnt, int *total) {
  if (left_len && right_len && key_cmp (left_len, left, right_len, right) > 0) {
    *cnt = *total = 0;
    return 0;
  }
  if (limit > R_MAX) {
    limit = R_MAX;
  }
  int min_index_pos = left_len ? 0 : get_index_pos (left_len, left);
  int max_index_pos = right_len ? index_entries : get_index_pos (right_len, right);
  if (min_index_pos == -2 || max_index_pos == -2) {
    return -2;
  }
  int Z[3];
  memset (Z, 0, sizeof (Z));
  tree_count (item_tree, left_len, left, right_len, right, Z);
  assert (!Z[1] && !Z[2]);
  int d = Z[0] + (max_index_pos - min_index_pos);
  if (d > limit) {
    Rmax = limit;
  } else {
    Rmax = d;
  }
  *total = d;
  int i;
  for (i = 0; i < d; i++) {
    if (i < Rmax) {
      RR[i] = i;
    } else {      
      int k = lrand48 () % (i + 1);
      if (k < Rmax) {
        RR[k] = i;
      }
    }
  }
  if (d > Rmax) {
    qsort (RR, Rmax, 4, cmp);
  }
  __array_report = array_ifwrite;
  __tree_report = tree_ifwrite;
  B = R;
  Bpos = 0;
  Bsize = size;
  Rpos = 0;
  Rt = 0;
  int r = listree_iterator (left_len, left, right_len, right, min_index_pos, max_index_pos);
  if (r == -2) { return r; }
  *cnt = Rmax;
  return (size - Bsize);
}

int store_inf (struct lev_seq_store_inf *E) {
  int key_len = (E->type - LEV_SEQ_STORE_INF) & 0xff;
  int mode = ((E->type - LEV_SEQ_STORE_INF) & 0x300) >> 8;
  return store (sizeof (*E) + key_len * 4 + E->value_len * 4, E, key_len, E->data, E->value_len, E->data + key_len, 0, mode);
}

int store_time (struct lev_seq_store_time *E) {
  int key_len = (E->type - LEV_SEQ_STORE_TIME) & 0xff;
  int mode = ((E->type - LEV_SEQ_STORE_TIME) & 0x300) >> 8;
  return store (sizeof (*E) + key_len * 4 + E->value_len * 4, E, key_len, E->data, E->value_len, E->data + key_len, E->time, mode);
}

int delete_lev (struct lev_seq_delete *E) {
  int key_len = (E->type - LEV_SEQ_DELETE) & 0xff;
  return delete (sizeof (*E) + key_len * 4, E, key_len, E->data);
}


int do_store (int mode, int key_len, const int *key, int value_len, const int *value, int delay, int force) {
  if (delay <= now && delay > 0) {
    return 0;
  }
  struct item *I = preload_key (key_len, key, force && (mode != 3));
  if (I->key_len == -2) {
    return -2;
  }
  
  assert (key_len <= 255);
  if (delay <= 0) {
    struct lev_seq_store_inf *E = alloc_log_event (LEV_SEQ_STORE_INF + mode * 256 + key_len, sizeof (*E) + key_len * 4 + value_len * 4, 0);
    E->value_len = value_len;
    memcpy (E->data, key, 4 * key_len);
    memcpy (E->data + key_len, value, 4 * value_len);
    return store_inf (E);
  } else {
    struct lev_seq_store_time *E = alloc_log_event (LEV_SEQ_STORE_TIME + mode * 256 + key_len, sizeof (*E) + key_len * 4 + value_len * 4, 0);
    E->time = delay;
    E->value_len = value_len;
    memcpy (E->data, key, 4 * key_len);
    memcpy (E->data + key_len, value, 4 * value_len);
    return store_time (E);
  }
}

int do_delete (int key_len, const int *key, int force) {
  struct item *I = preload_key (key_len, key, force);
  if (I->key_len == -2) {
    return -2;
  }
  assert (key_len >= 0 && key_len <= 255);
  struct lev_seq_delete *E = alloc_log_event (LEV_SEQ_DELETE + key_len, sizeof (*E) + key_len * 4, 0);
  memcpy (E->data, key, 4 * key_len);
  return delete_lev (E);
}

/* Binlog functions {{{ */
int seqmap_replay_logevent (struct lev_generic *E, int size);

int init_seqmap_data (int schema) {
  replay_logevent = seqmap_replay_logevent;
  return 0;
}


int seqmap_replay_logevent (struct lev_generic *E, int size) {
  int s;
  switch (E->type) {
  case LEV_START:
    return size;
  case LEV_NOOP:
  case LEV_TIMESTAMP:
  case LEV_CRC32:
  case LEV_ROTATE_FROM:
  case LEV_ROTATE_TO:
  case LEV_TAG:
    return default_replay_logevent (E, size);
  case LEV_SEQ_STORE_INF + 0x100 ... LEV_SEQ_STORE_INF + 0x3ff:
    s = E->type & 0xff;
    if (size < 8 || size < sizeof (struct lev_seq_store_inf) + 4 * s + 4 * E->a) { return -2; }
    store_inf ((void *)E);
    return sizeof (struct lev_seq_store_inf) + 4 * s + 4 * E->a;
  case LEV_SEQ_STORE_TIME + 0x100 ... LEV_SEQ_STORE_TIME + 0x3ff:
    s = E->type & 0xff;
    if (size < 12 || size < sizeof (struct lev_seq_store_time) + 4 * s + 4 * E->b) { return -2; }
    store_inf ((void *)E);
    return sizeof (struct lev_seq_store_time) + 4 * s + 4 * E->b;
  case LEV_SEQ_DELETE ... LEV_SEQ_DELETE + 0xff:
    s = E->type & 0xff;
    if (size < sizeof (struct lev_seq_delete) + 4 * s) { return -2; }
    delete_lev ((void *)E);
    return sizeof (struct lev_seq_delete) + 4 * s;
  }

  fprintf (stderr, "unknown log event type %08x at position %lld\n", E->type, log_cur_pos());

  return -1;
}

void seqmap_register_replay_logevent () {
  replay_logevent = seqmap_replay_logevent;
}
/* }}} */
