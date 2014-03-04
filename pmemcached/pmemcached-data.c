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
                   2010 Arseny Smirnov (Original memcached code)
                   2010 Aliaksei Levin (Original memcached code)
              2011-2013 Vitaliy Valtman
                   2011 Anton Maydell
*/

#define _FILE_OFFSET_BITS 64

#include "pmemcached-data.h"
#include <assert.h>
#include <unistd.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/time.h>
#include <sys/mman.h>

#define pmct_add 0
#define pmct_set 1
#define pmct_replace 2

#define GET_ENTRY_ID(x) ((unsigned int)(x) & HASH_TABLE_MASK)

#define TIME_TABLE_RATIO_EXP (4)
#define TIME_TABLE_SIZE_EXP (22 - TIME_TABLE_RATIO_EXP)
#define TIME_TABLE_SIZE (1 << TIME_TABLE_SIZE_EXP)
#define TIME_TABLE_MASK (TIME_TABLE_SIZE - 1)
#define GET_TIME_ID(x) (((unsigned int)(x) >> TIME_TABLE_RATIO_EXP) & TIME_TABLE_MASK)
#define MAX_TIME_GAP ((60 * 60) >> TIME_TABLE_RATIO_EXP)

#define local_clock() (local_clock_counter++)

extern int pack_mode;
extern int pack_fd;
extern int disable_cache;

/*hashtable functions*/
long long get_hash (const char * s, int sn);
void init_hash_table (void);

void del_entry_time (hash_entry_t *entry);
void add_entry_time (hash_entry_t *entry);
void free_by_time (int mx);
void free_wildcard_entry (struct wildcard_entry *entry);
void add_entry (hash_entry_t *entry);
void del_entry (hash_entry_t *entry);
hash_entry_t *get_entry (const char *key, int key_len);
//hash_entry_t *get_entry_ptr (int x);
hash_entry_t *get_new_entry (void);
void free_entry (hash_entry_t *entry);
int free_LRU (void);

/*pmemcached functions*/
int pmemcached_incr (struct lev_pmemcached_incr *E);
int pmemcached_incr_tiny (struct lev_pmemcached_incr_tiny *E);
int pmemcached_store (struct lev_pmemcached_store *E);
int pmemcached_store_forever (struct lev_pmemcached_store_forever *E); 
struct data pmemcached_get (struct lev_pmemcached_get *E);
int pmemcached_delete (struct lev_pmemcached_delete *E);

/*cache functions*/
int cache_load (const char *key, int key_len, int forceload);
int cache_reload (struct entry *entry);

/*pmemcached functions with cache*/
int pmemcached_incr_current (long long arg);
int pmemcached_delete_current (void);
inline struct data pmemcached_get_current (void);
int pmemcached_store_current (int op_type, const char *data, int data_len, int flags, int delay);
int pmemcached_check_time_current (void);


int cache_load_local (const char *key, int key_len, struct hash_entry *hash_entry, struct index_entry *index_entry);


hash_entry_t entry_buffer[TIME_TABLE_SIZE];

//int buffer_stack[MAX_HASH_TABLE_SIZE], buffer_stack_size;
tree_t *tree;
tree_t *wildcard_cache_tree;
tree_t *wildcard_rpc_cache_tree;
int time_st[TIME_TABLE_SIZE];

int last_del_time;
long long malloc_mem;
long long zalloc_mem;
extern long long max_memory;
long long del_by_LRU;
extern long long total_items;
extern int verbosity;
long long entry_memory;
long long init_memory;
long long allocated_metafile_bytes;
long long wildcard_cache_memory;
int wildcard_cache_entries;

struct entry cache[HASH_TABLE_SIZE];
struct entry *current_cache;
int cache_next[2 * HASH_TABLE_SIZE];
int cache_prev[2 * HASH_TABLE_SIZE];
int cache_prev_use[HASH_TABLE_SIZE + 1];
int cache_next_use[HASH_TABLE_SIZE + 1];
int cache_stack[HASH_TABLE_SIZE];
int cache_free;
long long cache_size;

long long cache_ok = 0;
long long cache_miss = 0;
long long cache_update = 0;
long long cache_deletes = 0;
int local_clock_counter = 0;
extern int index_type;

struct wildcard_entry wildcard_use;


struct index_entry index_entry_not_found = {
  .key_len = -1, 
  .flags = 0,
  .data_len = -1,
  .delay = 0
};

struct data data_loading = {
  .data_len = -2,
  .delay = -1,
  .flags = 0,
  .data = 0
};

double get_double_time_since_epoch(void) {
  struct timeval tv;
  gettimeofday(&tv, 0);
  return tv.tv_sec + 1e-6 * tv.tv_usec;
}

long long memory_for_metafiles;
long long memory_for_cache;
long long memory_for_entries;
long long memory_for_wildcard_cache;

void init_memory_bounds (void) {
  memory_for_metafiles = (long long) (MEMORY_FOR_METAFILES * max_memory);
  if (memory_for_metafiles < MIN_MEMORY_FOR_METAFILES) {
    memory_for_metafiles = MIN_MEMORY_FOR_METAFILES;
  }
  memory_for_cache = (long long) (MEMORY_FOR_CACHE * max_memory);
  if (memory_for_cache < MIN_MEMORY_FOR_CACHE) {
    memory_for_cache = MIN_MEMORY_FOR_CACHE;
  }
  memory_for_wildcard_cache = (long long) (MEMORY_FOR_WILDCARD_CACHE * max_memory);
  if (memory_for_wildcard_cache < MIN_MEMORY_FOR_WILDCARD_CACHE) {
    memory_for_wildcard_cache = MIN_MEMORY_FOR_WILDCARD_CACHE;
  }
  memory_for_entries = (long long) ((1 - MEMORY_RESERVED) * max_memory) - memory_for_cache - memory_for_metafiles - init_memory;
}

void redistribute_memory (void) {
  if (entry_memory <= 0.8 * memory_for_entries) return;
  memory_for_metafiles /= 2;
  if (memory_for_metafiles < MIN_MEMORY_FOR_METAFILES) {
    memory_for_metafiles = MIN_MEMORY_FOR_METAFILES;
  }
  memory_for_cache /= 2;
  if (memory_for_cache < MIN_MEMORY_FOR_CACHE) {
    memory_for_cache = MIN_MEMORY_FOR_CACHE;
  }
  memory_for_wildcard_cache /= 2;
  if (memory_for_wildcard_cache < MIN_MEMORY_FOR_WILDCARD_CACHE) {
    memory_for_wildcard_cache = MIN_MEMORY_FOR_WILDCARD_CACHE;
  }
  memory_for_entries = (long long) ((1 - MEMORY_RESERVED) * max_memory) - memory_for_cache - memory_for_metafiles - init_memory;
  free_cache ();
  free_metafiles ();
}

int memory_full_warning (void) {
  return entry_memory >= 0.9 * memory_for_entries;
}


int mystrcmp (const char *str1, int l1, const char *str2, int l2) {
  int t = l1;
  if (l1 > l2) {
    t = l2;
  }
  t = memcmp (str1, str2, t);
  if (t != 0) {
    return t;
  }
  if (l1 < l2) {
    return -1;
  }
  if (l2 < l1) {
    return 1;
  }
  return 0;
}

int mystrcmp2 (const char *str1, int l1, const char *str2, int l2, int x) {
  while (x < l1 && x < l2) {
    if (str1[x] < str2[x]) {
      return -x - 1;
    }
    if (str1[x] > str2[x]) {
      return x + 1;
    }
    x++;
  }
  if (l1 < l2) {
    return -x - 1;
  }
  if (l1 > l2) {
    return x + 1;
  }
  return 0;
}

/****
  memory functions
               ****/

void *zzmalloc (int size) {
  if (size<0) return 0;
  void *res;

  if (get_memory_used() > max_memory) {
    fprintf (stderr, "Out of memory\n");
  }

  if (get_memory_used () > max_memory) {
    fprintf (stderr, "too much memory used: %lld of %lld\n", (long long) get_memory_used (), (long long) max_memory);
    fprintf (stderr, "memory distributes as follow: %lld to current, %lld to cache, %lld to metafiles, %lld to init data\n", entry_memory, cache_size, allocated_metafile_bytes, init_memory);
  }
  assert (get_memory_used() <= max_memory);

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


/****
  Tree functions
             ****/

int alloc_tree_nodes;

int node_cmp (hash_entry_t *x, hash_entry_t *y) {
  return mystrcmp (x->key, x->key_len, y->key, y->key_len);
}

static tree_t *new_tree_node (hash_entry_t *x, int y) {
  assert (x);
  tree_t *P;
  P = zzmalloc (sizeof (tree_t));
  assert (P);
  alloc_tree_nodes++;
  P->left = P->right = 0;
  P->x = x;
  P->y = y;
  entry_memory += sizeof (tree_t);
  return P;
}

static void free_tree_node (tree_t *T) {
  zzfree (T, sizeof (tree_t));
  alloc_tree_nodes--;
  entry_memory -= sizeof (tree_t);
}

#define min(x,y) ((x) < (y) ? (x) : (y))
static tree_t *tree_lookup (tree_t *T, const char *key, int key_len) {
  int c;
  int lc = 1;
  int rc = 1;
  while (T && (c = mystrcmp2 (key, key_len, T->x->key, T->x->key_len, min (lc, rc) - 1))) {
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

static tree_t *tree_lookup_next (tree_t *T, const char *key, int key_len, int lc, int rc) {
  //vkprintf (1, "%s - %s [%d - %d]\n", key, T ? T->x->key : "NULL", lc, rc);
  if (!T) {
    return 0;
  }
  int c = mystrcmp2 (key, key_len, T->x->key, T->x->key_len, min (lc, rc) - 1);
  if (c < 0) {
    assert (-c >= min (lc, rc));
    tree_t *N = tree_lookup_next (T->left, key, key_len, lc, -c);
    return N ? N : T;
  } else {
    if (c == 0) { c = 2000; }
    assert (c >= min (lc, rc));
    return tree_lookup_next (T->right, key, key_len, c, rc); 
  }
}

int iterator_report (struct hash_entry *hash_entry);
int tree_lookup_all_next (tree_t *T, const char *key, int key_len, int prefix_len, int strict, int lc, int rc) {
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
}

static void tree_split (tree_t **L, tree_t **R, tree_t *T, hash_entry_t *x) {
  if (!T) { *L = *R = 0; return; }
  if (node_cmp (x, T->x) < 0) {
    *R = T;
    tree_split (L, &T->left, T->left, x);
  } else {
    *L = T;
    tree_split (&T->right, R, T->right, x);
  }
}

static tree_t *tree_insert (tree_t *T, hash_entry_t *x, int y) __attribute__ ((warn_unused_result));
static tree_t *tree_insert (tree_t *T, hash_entry_t *x, int y) {
  tree_t *P;
  if (!T) { 
    P = new_tree_node (x, y);
    return P;
  }
  int c;
  assert (c = node_cmp (x, T->x));
  if (T->y >= y) {
    if (c < 0) {
      T->left = tree_insert (T->left, x, y);
    } else {
      T->right = tree_insert (T->right, x, y);
    }
    return T;
  }
  P = new_tree_node (x, y);
  tree_split (&P->left, &P->right, T, x);
  return P;
}

static tree_t *tree_merge (tree_t *L, tree_t *R) __attribute__ ((warn_unused_result));
static tree_t *tree_merge (tree_t *L, tree_t *R) {
  if (!L) { return R; }
  if (!R) { return L; }
  if (L->y > R->y) {
    L->right = tree_merge (L->right, R);
    return L;
  } else {
    R->left = tree_merge (L, R->left);
    return R;
  }
}

static tree_t *tree_delete (tree_t *T, hash_entry_t *x) __attribute__ ((warn_unused_result));
static tree_t *tree_delete (tree_t *T, hash_entry_t *x) {
  int c;
  assert (T);
  if (!(c = node_cmp (x, T->x))) {
    tree_t *N = tree_merge (T->left, T->right);
    free_tree_node (T);
    return N;
  }
  if (c < 0) {
    T->left = tree_delete (T->left, x);
  } else {
    T->right = tree_delete (T->right, x);
  }
  return T;
}

static tree_t *tree_wildcard_delete (tree_t *T, hash_entry_t *x) __attribute__ ((warn_unused_result));
static tree_t *tree_wildcard_delete (tree_t *T, hash_entry_t *x) {
  int c;
  assert (T);
  if (!(c = node_cmp (x, T->x))) {
    tree_t *N = tree_merge (T->left, T->right);
    free_wildcard_entry ((struct wildcard_entry *)T->x);
    free_tree_node (T);
    return N;
  }
  if (c < 0) {
    T->left = tree_wildcard_delete (T->left, x);
  } else {
    T->right = tree_wildcard_delete (T->right, x);
  }
  return T;
}

tree_t *tree_delete_prefix (tree_t *T, const char *key, int key_len, int lc, int rc) __attribute__ ((warn_unused_result));
tree_t *tree_delete_prefix (tree_t *T, const char *key, int key_len, int lc, int rc) {
  //vkprintf (1, "%s - %s [%d - %d]\n", key, T ? T->x->key : "NULL", lc, rc);
  if (!T) {
    return 0;
  }
  int c = mystrcmp2 (key, key_len, T->x->key, T->x->key_len, min (abs (lc), abs (rc)) - 1);
  assert (lc > 0);
  if (c == 0) {
    c = key_len + 2; //Infinity
  }
  if (c < 0 || c > lc) {
    T->left = tree_delete_prefix (T->left, key, key_len, lc, -c);
  }
  if (c > 0 && (rc > 0 || -rc > c)) {
    T->right = tree_delete_prefix (T->right, key, key_len, c, rc);
  }
  if (c == 0 || c < -T->x->key_len || c > T->x->key_len) {
    tree_t *N = tree_merge (T->left, T->right);
    free_wildcard_entry ((struct wildcard_entry *)T->x);
    free_tree_node (T);
    return N;
  } else {
    return T;
  }
}

static int tree_dump_pointers (tree_t *T, hash_entry_t **x, int p, int maxp) {
  if (!T) {
    return p;
  }
  p = tree_dump_pointers (T->left, x, p, maxp);
  assert (p < maxp);
  x[p ++] = T->x;
  return tree_dump_pointers (T->right, x, p, maxp);
}

int dump_pointers (hash_entry_t **x, int p, int maxp) {
  return tree_dump_pointers (tree, x, p, maxp);
}

void  on_value_change (const char *key, int key_len) {
  wildcard_cache_tree = tree_delete_prefix (wildcard_cache_tree, key, key_len, 1, 1);
  wildcard_rpc_cache_tree = tree_delete_prefix (wildcard_rpc_cache_tree, key, key_len, 1, 1);
}

void wildcard_add_use (struct wildcard_entry *entry) {
  entry->prev_use = &wildcard_use;
  entry->next_use = wildcard_use.next_use;
  entry->prev_use->next_use = entry;
  entry->next_use->prev_use = entry;
}

void wildcard_del_use (struct wildcard_entry *entry) {
  entry->prev_use->next_use = entry->next_use;
  entry->next_use->prev_use = entry->prev_use;
}

void wildcard_update_use (struct wildcard_entry *entry) {
  wildcard_del_use (entry);
  wildcard_add_use (entry);
}

void wildcard_free_lru (void) {
  struct wildcard_entry *entry = wildcard_use.prev_use;
  assert (entry != &wildcard_use);
  if (entry->flags & 1) {
    wildcard_cache_tree = tree_wildcard_delete (wildcard_cache_tree, (hash_entry_t *)entry);
  } else {
    wildcard_rpc_cache_tree = tree_wildcard_delete (wildcard_rpc_cache_tree, (hash_entry_t *)entry);
  }
  vkprintf (2, "wildcard_lru succ: memory = %lld, entry = %p\n", wildcard_cache_memory, entry);
}

void wildcard_add_value (const char *key, int key_len, const char *data, int data_len) {
  while (wildcard_cache_memory + data_len + key_len + sizeof (struct wildcard_entry) > memory_for_wildcard_cache) {
    wildcard_free_lru ();
  }
  struct data x = wildcard_get_value (key, key_len);
  if (x.data_len != -1) {
    return;
  }
  struct wildcard_entry *entry = zzmalloc (sizeof (struct wildcard_entry));
  wildcard_add_use (entry);
  entry->key_len = key_len;
  entry->key = zzmalloc (key_len);
  memcpy (entry->key, key, key_len);
  entry->data_len = data_len;
  entry->data = zzmalloc (data_len);
  entry->flags = 1;
  memcpy (entry->data, data, data_len);
  wildcard_cache_tree = tree_insert (wildcard_cache_tree, (hash_entry_t *)entry, lrand48 ());
  wildcard_cache_memory += data_len + key_len + sizeof (struct wildcard_entry);
  wildcard_cache_entries ++;
}

void wildcard_add_rpc_value (const char *key, int key_len, const char *data, int data_len) {
  while (wildcard_cache_memory + data_len + key_len + sizeof (struct wildcard_entry) > memory_for_wildcard_cache) {
    wildcard_free_lru ();
  }
  struct data x = wildcard_get_value (key, key_len);
  if (x.data_len != -1) {
    return;
  }
  struct wildcard_entry *entry = zzmalloc (sizeof (struct wildcard_entry));
  wildcard_add_use (entry);
  entry->key_len = key_len;
  entry->key = zzmalloc (key_len);
  memcpy (entry->key, key, key_len);
  entry->data_len = data_len;
  entry->data = zzmalloc (data_len);
  memcpy (entry->data, data, data_len);
  entry->flags = 0;
  wildcard_rpc_cache_tree = tree_insert (wildcard_rpc_cache_tree, (hash_entry_t *)entry, lrand48 ());
  wildcard_cache_memory += data_len + key_len + sizeof (struct wildcard_entry);
  wildcard_cache_entries ++;
}

struct data wildcard_get_value (const char *key, int key_len) {
  tree_t *T = tree_lookup (wildcard_cache_tree, key, key_len);
  struct data data;
  if (!T) {
    data.data_len = -1;
    data.data = 0;
  } else {
    struct wildcard_entry *entry = (struct wildcard_entry *)T->x;
    data.data_len = entry->data_len;
    data.flags = 1;
    data.delay = -1;
    data.data = entry->data;
    wildcard_update_use (entry);
  }
  return data;
}

struct data wildcard_get_rpc_value (const char *key, int key_len) {
  tree_t *T = tree_lookup (wildcard_rpc_cache_tree, key, key_len);
  struct data data;
  if (!T) {
    data.data_len = -1;
    data.data = 0;
  } else {
    struct wildcard_entry *entry = (struct wildcard_entry *)T->x;
    data.data_len = entry->data_len;
    data.flags = 1;
    data.delay = -1;
    data.data = entry->data;
    wildcard_update_use (entry);
  }
  return data;
}

/*
 *
 *hashtable functions
 *
 */

long long get_hash (const char *s, int sn) {
  long long h = 239;
  int i;       

  for (i = 0; i < sn; i++) {
    h = h * 999983 + s[i];
  }
  
  return h;
}

void init_hash_table (void) {
  int i;

  tree = 0;
  wildcard_cache_tree = 0;
  wildcard_use.next_use = &wildcard_use;
  wildcard_use.prev_use = &wildcard_use;

  for (i = 0; i < TIME_TABLE_SIZE; i++) {
    time_st[i] = i;

    entry_buffer[time_st[i]].next_time = &entry_buffer[time_st[i]];
    entry_buffer[time_st[i]].prev_time = &entry_buffer[time_st[i]];
  }


  last_del_time = GET_TIME_ID (get_double_time_since_epoch());
  malloc_mem = 0;
  del_by_LRU = 0;

  if (!disable_cache) {
    for (i=0; i < HASH_TABLE_SIZE; i++) {
      cache[i].key_len = -1;
      cache[i].key = 0;
      cache_next [HASH_TABLE_SIZE + i] = HASH_TABLE_SIZE + i;
      cache_prev [HASH_TABLE_SIZE + i] = HASH_TABLE_SIZE + i;
      cache_stack[i] = i;
    }

    cache_prev_use [HASH_TABLE_SIZE] = HASH_TABLE_SIZE;
    cache_next_use [HASH_TABLE_SIZE] = HASH_TABLE_SIZE;
    cache_free = HASH_TABLE_SIZE;
  }
  init_memory_bounds ();
}

void del_entry_time (hash_entry_t *entry) {
  assert (entry);
  //assert (0 <= x && x < MAX_HASH_TABLE_SIZE + TIME_TABLE_SIZE + 1);

  assert (entry->prev_time);
  assert (entry->next_time);
  entry->next_time->prev_time = entry->prev_time;
  entry->prev_time->next_time = entry->next_time;
}

void add_entry_time (hash_entry_t *entry) {
  assert (entry);

  if (entry->exp_time >=0) {
    int f = time_st[GET_TIME_ID (entry->exp_time)];
    hash_entry_t *entry_time  = &entry_buffer[f];
    assert (entry_time->next_time);
    assert (entry_time->prev_time);
    entry->next_time = entry_time;
    entry->prev_time = entry_time->prev_time;
    entry->next_time->prev_time = entry;
    entry->prev_time->next_time = entry;
    assert (entry_time->next_time);
    assert (entry_time->prev_time);
  } else {
    entry->next_time = entry;
    entry->prev_time = entry;
  }
}

void add_entry (hash_entry_t *entry) {
  if (verbosity >= 4) {
    fprintf (stderr, "add_entry (%p)\n", entry);
  }
  assert (entry);
  //assert (0 <= x && x < MAX_HASH_TABLE_SIZE + TIME_TABLE_SIZE + 1);


  entry->timestamp = local_clock ();
  tree = tree_insert (tree, entry, lrand48 ());
  total_items++;
}

void debug_dump_hash_entry (hash_entry_t *E);

void del_entry (hash_entry_t *entry) {
  assert (entry);
  if (verbosity >= 4) {
    fprintf(stderr, "deleted %p\n", entry);
    debug_dump_hash_entry (entry);
  }

  del_entry_time (entry);

  tree = tree_delete (tree, entry);
  
  zzfree (entry->key, entry->key_len + 1); 
  zzfree (entry->data, entry->data_len + 1);
  entry_memory -= entry->key_len + entry->data_len + 2;

  entry->key = 0;
  entry->key_len = -1;
  entry->timestamp = -1;

  free_entry (entry);
}



void free_by_time (int mx) {
  vkprintf (4, "free_by_time: mx = %d\n", mx);
  int en = GET_TIME_ID (get_double_time_since_epoch()),
      st = time_st[last_del_time];

  while (en - last_del_time > MAX_TIME_GAP || last_del_time - en > TIME_TABLE_SIZE - MAX_TIME_GAP || 
         (mx-- && last_del_time != en)) {
    hash_entry_t  *entry_time = &entry_buffer[st];
    assert (entry_time->next_time);
    if (entry_time->next_time != entry_time) {
      if (verbosity >= 4) {
        fprintf(stderr, "del entry %p by time(key = %s) gap = %d\n", entry_time->next_time, entry_time->next_time->key, en - last_del_time);
      }
      do_pmemcached_delete (entry_time->next_time->key, entry_time->next_time->key_len);
    } else {
      if (++last_del_time == TIME_TABLE_SIZE) {
        last_del_time = 0;
      }
      st = time_st[last_del_time];
    }
  }
}


hash_entry_t *get_new_entry (void) {
  return zzmalloc (sizeof (hash_entry_t));
}

void free_entry (hash_entry_t *entry) {
  zzfree (entry, sizeof (hash_entry_t));
}

void free_wildcard_entry (struct wildcard_entry *entry) {
  wildcard_del_use (entry);
  zzfree (entry->key, entry->key_len);
  zzfree (entry->data, entry->data_len);
  wildcard_cache_memory -= entry->key_len + entry->data_len + sizeof (struct wildcard_entry);
  zzfree (entry, sizeof (struct wildcard_entry));
  wildcard_cache_entries --;
}

hash_entry_t *get_entry (const char *key, int key_len) {
  tree_t *T = tree_lookup (tree, key, key_len);
  if (!T) {
    return 0;
  } else {
    return T->x;
  }
}

hash_entry_t *get_next_entry (const char *key, int key_len) {
  tree_t *T = tree_lookup_next (tree, key, key_len, 1, 1);
  if (!T) {
    return 0;
  } else {
    return T->x;
  }
}

int get_all_next_entries (const char *key, int key_len, int prefix_len, int strict) {
  return tree_lookup_all_next (tree, key, key_len, prefix_len, strict, 1, 1);
}


/*
 *
 *statistic functions
 *
 */
inline int get_entry_cnt (void) {
  return alloc_tree_nodes - wildcard_cache_entries;
}

long long get_memory_used (void) {
  return malloc_mem + zalloc_mem;
}

/*
 *
 *pmemcached exterface functions
 *
 */

int do_pmemcached_preload (const char *key, int key_len, int forceload) {
  int x = cache_load (key, key_len, forceload);
/*  if (x != -2) {
    pmemcached_check_time_current ();
  }*/
  return x;
}

int do_pmemcached_get_next_key (const char *key, int key_len, char **result_key, int *result_key_len) {
  char *cur_key = 0;
  int cur_key_len = 0;
  struct hash_entry *hash_entry = get_next_entry (key, key_len);
  int r = -1;
  if (hash_entry) {
    cur_key_len = hash_entry->key_len;
    cur_key = hash_entry->key;
    r = 0;
  }
  struct index_entry *index_entry = index_get_next (key, key_len);
  if (!index_entry) {    
    return -2;
  }
  vkprintf (4, "next in index: %d, next in memory: %d\n", index_entry->data_len != -1, r != -1);
  if (index_entry->data_len == -1 && r == -1) {
    return -1;
  }
  int c;
  if (index_entry->data_len == -1) {
    c = -1;
  } else if (!cur_key) {
    c = 1;
  } else {
    c = mystrcmp (cur_key, cur_key_len, index_entry->data, index_entry->key_len);
  }
  if (c <= 0) {
    *result_key = cur_key;
    *result_key_len = cur_key_len;
  } else {
    *result_key = index_entry->data;
    *result_key_len = index_entry->key_len;
  }
  cache_load_local (*result_key, *result_key_len, (c <= 0) ? hash_entry : 0, (c >= 0) ? index_entry : 0);
  return 0;
}

struct index_entry *iterator_index_entry;
const char *iterator_key;
int iterator_prefix_len;

int wildcard_report (void) {  
  assert (current_cache->data.data_len != -2);
  if (current_cache->data.data_len >= 0) {
    if (wildcard_engine_report (current_cache->key, current_cache->key_len, current_cache->data) == 0) { return -1; }
  }
  return 0;
}

int iterator_report (struct hash_entry *hash_entry) {
  vkprintf (3, "iterator_report: index_entry %p, hash_entry %p\n", iterator_index_entry, hash_entry);
  struct index_entry *index_entry = iterator_index_entry;
  char *cur_key = 0;
  int cur_key_len = 0;
  int r = -1;
  if (hash_entry) {
    cur_key_len = hash_entry->key_len;
    cur_key = hash_entry->key;
    r = 0;
  }
  int c;
  while (1) {
    if (index_entry->data_len == -1) {
      c = -1;
    } else if (!cur_key) {
      c = 1;
    } else {
      c = mystrcmp (cur_key, cur_key_len, index_entry->data, index_entry->key_len);
    }
    if (c <= 0) {
      break;
    }
    if (!hash_entry) {
      int x = mystrcmp2 (iterator_key, iterator_prefix_len, index_entry->data, index_entry->key_len, 0);
      assert (x <= 0);
      if (x && x >= -iterator_prefix_len) {
        return 0;
      }
    }
    //fprintf (stderr, ".");
    cache_load_local (index_entry->data, index_entry->key_len, (c <= 0) ? hash_entry : 0, (c >= 0) ? index_entry : 0);
    if (wildcard_report () < 0) {
      return -1;
    }

    index_entry = index_entry_next (index_entry);
    if (!index_entry) {
      return -2;
    }
  }
  if (!hash_entry) {
    return 0;
  }
  cache_load_local (hash_entry->key, hash_entry->key_len, (c <= 0) ? hash_entry : 0, (c >= 0) ? index_entry : 0);
  //fprintf (stderr, "-");
  if (wildcard_report () < 0) { return -1;};
  if (c == 0) {
    index_entry = index_entry_next (index_entry);
    if (!index_entry) {
      return -2;
    }
  }
  iterator_index_entry = index_entry;
  return 0;
}

int do_pmemcached_get_all_next_keys (const char *key, int key_len, int prefix_len, int strict) {
  struct index_entry *index_entry = 0;
  iterator_key = key;
  iterator_prefix_len = prefix_len;
  if (!strict) {
    index_entry = index_get (key, key_len);
    if (!index_entry) {
      return -2;
    }
    if (index_entry->data_len == -1) {
      index_entry = 0;
    }
  }
  if (!index_entry) {
    index_entry = index_get_next (key, key_len);
    if (!index_entry) {
      return -2;
    }
  }
  iterator_index_entry = index_entry;
  if (get_all_next_entries (key, key_len, prefix_len, strict) != -2) {
    return iterator_report (0);
  }
  return 0;
}

int do_pmemcached_incr (int op, const char *key, int key_len, long long arg) {
  //on_value_change (key, key_len);
  if (op) {
    arg = -arg;
  }
  if (verbosity >= 4) { fprintf (stderr, "cache_load isn't return -2\n"); }
  if (arg < -127 || arg > 127){
    struct lev_pmemcached_incr *E = alloc_log_event (LEV_PMEMCACHED_INCR, offsetof(struct lev_pmemcached_incr, key) + 1 + key_len, 0);
    E->key_len = key_len;
    E->arg = arg;
    memcpy(E->key, key, key_len * sizeof(char));      
    E->key[key_len] = 0;
    return pmemcached_incr(E);
  } else {
    unsigned char c = (unsigned char)arg;
    struct lev_pmemcached_incr_tiny *E = alloc_log_event (LEV_PMEMCACHED_INCR_TINY + c, offsetof (struct lev_pmemcached_incr_tiny, key) + 1 + key_len, 0);
    E->key_len = key_len;
    memcpy(E->key, key, key_len * sizeof(char));      
    E->key[key_len] = 0;
    return pmemcached_incr_tiny(E);
  }
}

/*
void expand_lev_pmemcached_store_forever (struct lev_pmemcached_store *dest, const struct lev_pmemcached_store_forever *src) {
  dest->type = LEV_PMEMCACHED_STORE + (src->type & 3);
  dest->key_len = src->key_len;
  dest->flags = 0;
  dest->data_len = src->data_len;
  dest->delay = DELAY_INFINITY;
  memcpy (dest->data, src->data, src->key_len + src->data_len + 1);
}
*/

int do_pmemcached_store (int op_type, const char *key, int key_len, int flags, int delay, const char* data, int data_len) {
  int type;
  if (op_type == mct_add && current_cache->data.data_len != -1) {
    return 0;
  }
  if (op_type == mct_replace && current_cache->data.data_len == -1) {
    return 0;
  }
  //on_value_change (key, key_len);
  type = pmct_set;
  pmemcached_check_time_current ();
  struct lev_pmemcached_store *E = 0;
  if (!flags && delay == DELAY_INFINITY) {
    int s = offsetof (struct lev_pmemcached_store_forever, data) + 1 + key_len + data_len;
    struct lev_pmemcached_store_forever *EE = alloc_log_event (LEV_PMEMCACHED_STORE_FOREVER + type, s, 0);    
    EE->key_len = key_len;
    EE->data_len = data_len;
    memcpy (EE->data, key, key_len*sizeof(char));
    if (data_len >= 0) {
      memcpy (EE->data+key_len, data, data_len*sizeof(char));
      EE->data[key_len+data_len] = 0;
    }
    return pmemcached_store_forever (EE);
  } else {
    E = alloc_log_event (LEV_PMEMCACHED_STORE + type, offsetof (struct lev_pmemcached_store, data) + 1 + key_len + data_len, 0);
    E->key_len = key_len;
    E->data_len = data_len;
    E->flags = flags;
    E->delay = delay;
    memcpy (E->data, key, key_len*sizeof(char));
    if (data_len >= 0) {
      memcpy (E->data+key_len, data, data_len*sizeof(char));
      E->data[key_len+data_len] = 0;
    }    
    return pmemcached_store (E);
  }
  
}

struct data do_pmemcached_get (const char *key, int key_len) {
#ifdef LOG_PMEMCACHED_GET
  //long long hash = get_hash (key, key_len);
  struct lev_pmemcached_get *E = alloc_log_event(LEV_PMEMCACHED_GET, offsetof(struct lev_pmemcached_get,key)+1+key_len, 0);
  E->key_len = key_len;
  //E->hash = hash;
  memcpy (E->key, key, key_len);
  E->key[key_len] = 0;
  return pmemcached_get (E);
#else
  //return pmemcached_get_common (key, key_len, hash);
  pmemcached_check_time_current ();
  return pmemcached_get_current ();
#endif
}

void do_pmemcached_merge (const char *key, int key_len) {
  assert(cache_load (key, key_len, 1) != -2);
  //return current_cache->hash_entry;
}


int do_pmemcached_delete (const char *key, int key_len) {
  struct lev_pmemcached_delete *E = alloc_log_event (LEV_PMEMCACHED_DELETE, offsetof (struct lev_pmemcached_delete, key) + 1 + key_len, 0);
  E->key_len = key_len;
  memcpy(E->key, key, key_len);
  E->key[key_len] = 0;
  return pmemcached_delete (E);
}

/*
 *
 *pmemcached functions
 *
 */

int pmemcached_incr (struct lev_pmemcached_incr *E) {
  cache_load (E->key, E->key_len, 0);
  return pmemcached_incr_current (E->arg);
}


int pmemcached_incr_tiny (struct lev_pmemcached_incr_tiny *E) {
  cache_load (E->key, E->key_len, 0);
  return pmemcached_incr_current ((signed char)E->type);
}


int pmemcached_store (struct lev_pmemcached_store *E) {
  cache_load (E->data, E->key_len, 0);
  return pmemcached_store_current (E->type & 3, E->data+E->key_len, E->data_len, E->flags, E->delay);
}

int pmemcached_store_forever (struct lev_pmemcached_store_forever *E) {
  cache_load (E->data, E->key_len, 0);

  return pmemcached_store_current (E->type & 3, E->data+E->key_len, E->data_len, 0, DELAY_INFINITY);
}

struct data pmemcached_get (struct lev_pmemcached_get *E) {
  cache_load (E->key, E->key_len, 0);
  return pmemcached_get_current ();
}

int pmemcached_delete (struct lev_pmemcached_delete *E) {
  cache_load (E->key, E->key_len, 0);
  return pmemcached_delete_current ();
}


/*
 *
 *binlog functions
 *
 */

int pmemcached_replay_logevent (struct lev_generic *E, int size);

int init_pmemcached_data (int schema) {
  replay_logevent = pmemcached_replay_logevent;
  return 0;
}


int pmemcached_replay_logevent (struct lev_generic *E, int size) {
  int s;

  if (get_entry_cnt () > ENTRIES_TO_INDEX || memory_full_warning ()) {
    save_index (0);
    exit (13);
  }

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
  case LEV_PMEMCACHED_DELETE:
    if (size < sizeof (struct lev_pmemcached_delete)) {
      return -2;
    }
    s = ((struct lev_pmemcached_delete *) E)->key_len;
    if (s < 0) { 
      return -4;
    }
    
    s += 1 + offsetof (struct lev_pmemcached_delete, key);
    if (size < s) {
      return -2;
    }
    pmemcached_delete ( (struct lev_pmemcached_delete *)E);
    if (pack_mode) {
      int sn = s;
      sn = (sn + 3) & -4;
      write (pack_fd, E, sn);
      //write (pack_fd, E, s);
    }
    return s;
  case LEV_PMEMCACHED_GET:
    if (size < sizeof (struct lev_pmemcached_get)) {
      return -2;
    }
    s = ((struct lev_pmemcached_get *) E)->key_len;
    if (s < 0) { 
      return -4;
    }
    
    s += 1 + offsetof (struct lev_pmemcached_get, key);
    if (size < s) {
      return -2;
    }
    pmemcached_get ( (struct lev_pmemcached_get *)E);
    if (pack_mode) {
      int sn = s;
      sn = (sn + 3) & -4;
      write (pack_fd, E, sn);
    }
    return s;
  case LEV_PMEMCACHED_STORE...LEV_PMEMCACHED_STORE+2:
    if (size < sizeof (struct lev_pmemcached_store)) {
      return -2;
    }
    s = ((struct lev_pmemcached_store *) E)->key_len + ((struct lev_pmemcached_store *) E)->data_len;
    if (s < 0) { 
      return -4;
    }
    
    s += 1 + offsetof (struct lev_pmemcached_store, data);
    if (size < s) {
      return -2;
    }
    pmemcached_store ( (struct lev_pmemcached_store *)E);
    if (pack_mode) {
      int sn = s;
      sn = (sn + 3) & -4;
      write (pack_fd, E, sn);
      //write (pack_fd, E, s);
    }
    return s;
  case LEV_PMEMCACHED_STORE_FOREVER...LEV_PMEMCACHED_STORE_FOREVER+2:
    if (size < sizeof (struct lev_pmemcached_store_forever)) {
      return -2;
    }
    s = ((struct lev_pmemcached_store_forever *) E)->key_len + ((struct lev_pmemcached_store_forever *) E)->data_len;
    if (s < 0) { 
      return -4;
    }    
    s += 1 + offsetof (struct lev_pmemcached_store_forever, data);
    if (size < s) {
      return -2;
    }    
    pmemcached_store_forever ((struct lev_pmemcached_store_forever*) E);
    if (pack_mode) {
      int sn = s;
      sn = (sn + 3) & -4;
      write (pack_fd, E, sn);
      //write (pack_fd, E, s);
    }
    return s;  
  case LEV_PMEMCACHED_INCR:
    if (size < sizeof (struct lev_pmemcached_incr)) {
      return -2;
    }
    s = ((struct lev_pmemcached_incr *) E)->key_len;
    if (s < 0) { 
      return -4;
    }
    
    s += 1 + offsetof (struct lev_pmemcached_incr, key);
    if (size < s) {
      return -2;
    }
    pmemcached_incr ( (struct lev_pmemcached_incr *)E);
    if (pack_mode) {
      int sn = s;
      sn = (sn + 3) & -4;
      write (pack_fd, E, sn);
      //write (pack_fd, E, s);
    }
    return s;
  case LEV_PMEMCACHED_INCR_TINY...LEV_PMEMCACHED_INCR_TINY+255:
    if (size < sizeof (struct lev_pmemcached_incr_tiny)) {
      return -2;
    }
    s = ((struct lev_pmemcached_incr_tiny *) E)->key_len;
    if (s < 0) { 
      return -4;
    }
    
    s += 1 + offsetof (struct lev_pmemcached_incr_tiny, key);
    if (size < s) {
      return -2;
    }
    pmemcached_incr_tiny ( (struct lev_pmemcached_incr_tiny *)E);
    if (pack_mode) {
      int sn = s;
      sn = (sn + 3) & -4;
      write (pack_fd, E, sn);
      //write (pack_fd, E, s);
    }
    return s;
  }

  fprintf (stderr, "unknown log event type %08x at position %lld\n", E->type, log_cur_pos());

  return -1;
}

void pmemcached_register_replay_logevent () {
  replay_logevent = pmemcached_replay_logevent;
}


/*
 *
 * cache
 *
 */

inline void cache_assign_hash_entry ( struct entry *entry) {
  entry->hash_entry = get_entry (entry->key, entry->key_len);
}

inline int cache_assign_index_entry ( struct entry *entry) {
  struct index_entry *index_entry = index_get (entry->key, entry->key_len);
  if (verbosity >= 4) {
    fprintf (stderr, "copying data from index_entry... ");
  }
  if (!index_entry) {    
    return -2;
  }
  if (index_entry->data_len == -1) {
    entry->index_entry = 0;
    entry->data.data_len = -1;
    entry->data.data = 0;
  } else {
    entry->index_entry = 1;
    entry->data.data_len = index_entry->data_len;
    if (index_type != PMEMCACHED_TYPE_INDEX_DISK) {
      entry->data.data = index_entry->data + entry->key_len;
      entry->allocated_here = 0;
    } else {
      entry->data.data = zzmalloc (entry->data.data_len + 1);
      memcpy (entry->data.data, index_entry->data+ entry->key_len, entry->data.data_len + 1);
      entry->allocated_here = 1;
      cache_size += entry->data.data_len + 1;
    }
    entry->data.flags = index_entry->flags;
    entry->data.delay = index_entry->delay;
  }
  entry->timestamp = -2;
  if (verbosity >= 4) {
    fprintf (stderr, "done... \n");
    fprintf (stderr, "data_len = %d\n", entry->data.data_len);
  }
  return 0;
}

inline int cache_assign_index_entry_local (struct entry *entry, struct index_entry *index_entry) {
  if (!index_entry || index_entry->data_len == -1) {
    entry->index_entry = 0;
    entry->data.data_len = -1;
    entry->data.data = 0;
  } else {
    entry->index_entry = 1;
    entry->data.data_len = index_entry->data_len;
    if (index_type != PMEMCACHED_TYPE_INDEX_DISK) {
      entry->data.data = index_entry->data + entry->key_len;
      entry->allocated_here = 0;
    } else {
      entry->data.data = zzmalloc (entry->data.data_len + 1);
      memcpy (entry->data.data, index_entry->data+ entry->key_len, entry->data.data_len + 1);
      entry->allocated_here = 1;
      cache_size += entry->data.data_len + 1;
    }
    entry->data.flags = index_entry->flags;
    entry->data.delay = index_entry->delay;
  }
  entry->timestamp = -2;
  if (verbosity >= 4) {
    fprintf (stderr, "done... \n");
    fprintf (stderr, "data_len = %d\n", entry->data.data_len);
  }
  return 0;
}

inline void cache_assign_data (struct entry *entry) {
  if (entry->hash_entry) {
    if (entry->allocated_here) {
      zzfree (entry->data.data, entry->data.data_len + 1);
      cache_size -= entry->data.data_len + 1;
      entry->allocated_here = 0;
    }
    entry->data.data_len = entry->hash_entry->data_len;
    entry->data.data = entry->hash_entry->data;
    entry->data.flags = entry->hash_entry->flags;
    entry->data.delay = entry->hash_entry->exp_time;
    entry->timestamp = entry->hash_entry->timestamp;
  } else if (entry->index_entry != 1) {
    if (entry->allocated_here) {
      zzfree (entry->data.data, entry->data.data_len + 1);
      cache_size -= entry->data.data_len + 1;
      entry->allocated_here = 0;
    }
    if (entry->index_entry == 0) {
      entry->data.data_len = -1;
    } else {
      entry->data.data_len = -2;
    }
    entry->data.data = 0;
    entry->data.flags = 0;
    entry->data.delay = -1;
    entry->timestamp = -2;
  }
}
void debug_dump_key (const char *key, int key_len) {
  int i;
  if (key_len < 0) { fprintf (stderr, "[len = %d]", key_len); }
  for (i = 0; i < key_len; i++) fputc (key[i], stderr);
  fputc ('\n', stderr);
}

void debug_dump_hash_entry (hash_entry_t *E) {
  fprintf (stderr, "E->key = "); debug_dump_key (E->key, E->key_len);
  fprintf (stderr, "E->data = "); debug_dump_key (E->data, E->data_len);
  fprintf (stderr, "flags = %d, exp_time = %d, accum_value = %lld, timestamp = %d\n", (int) E->flags, E->exp_time, E->accum_value, E->timestamp);
  //char need_merging;
}

void del_cache_use (int n) {
  cache_next_use[cache_prev_use[n]] = cache_next_use[n];
  cache_prev_use[cache_next_use[n]] = cache_prev_use[n];
}

void add_cache_use (int n) {
  int e = HASH_TABLE_SIZE;
  cache_next_use[n] = cache_next_use[e];
  cache_prev_use[cache_next_use[n]] = n;
  cache_prev_use[n] = e;
  cache_next_use[e] = n;
}

void update_cache_use (int n) {
  del_cache_use (n);
  add_cache_use (n);
}

void create_new_cache_item (struct entry* entry, const char *key, int key_len) {
  entry->key_len = key_len;
  entry->key = zzmalloc (key_len * sizeof(char));
  entry->index_entry = -1;
  entry->data.data_len = -2;
  if (key_len >= 0) {
    memcpy (entry->key, key, key_len * sizeof(char));
  }  
  entry->hash_entry = 0;
  cache_size += key_len * sizeof (char);
}

void free_cache_item (struct entry *entry) {
  if (entry->key) {
    zzfree (entry->key, entry->key_len * sizeof (char));
    cache_size -= entry->key_len * sizeof (char);
    if (entry->allocated_here) {
      zzfree (entry->data.data, entry->data.data_len + 1);
      entry->allocated_here = 0;
      cache_size -= entry->data.data_len + 1;
    }
  }
  entry->key = 0;
}

void delete_cache_item (int n) {
  if (verbosity >= 4) { fprintf (stderr, "delete_cache_item\n"); }
  struct entry* entry = cache + n;
  free_cache_item (entry);
  del_cache_use (n);
  cache_next[cache_prev[n]] = cache_next[n];
  cache_prev[cache_next[n]] = cache_prev[n];
  cache_stack[cache_free++] = n;
  cache_deletes ++;
}

int cache_free_LRU (void) {
  int e = HASH_TABLE_SIZE;
  if (cache_prev_use[e] == e) {
    return 0;
  }
  delete_cache_item (cache_prev_use[e]);
  return 1;
}


void free_cache (void) {
  //int deleted = 0;
  while (!cache_free || cache_size > memory_for_cache * 0.9) {
    if (!cache_free_LRU ()) {
      break;
    }
    //deleted ++;
  }
}

static struct entry __tmp;
struct entry *get_cache_item_simple (const char *key, int key_len) {
  if (verbosity >= 4) { fprintf (stderr, "get_cache_item_simple\n"); }
  if (key_len >= 0 && key_len ==  __tmp.key_len && !memcmp (key, __tmp.key, key_len)) {
    return &__tmp;
  } else {
    free_cache_item (&__tmp);
    create_new_cache_item (&__tmp, key, key_len);
    return &__tmp;
  }
}

struct entry *get_cache_item (const char *key, int key_len) {
  if (verbosity >= 4) { fprintf (stderr, "get_cache_item\n"); }
  long long key_hash = get_hash (key, key_len);
  int h = key_hash & HASH_TABLE_MASK;
  int n = h + HASH_TABLE_SIZE;
  while (cache_next[n] < HASH_TABLE_SIZE) {
    struct entry *e = cache + cache_next[n];
    if (key_len == e->key_len && key_len >= 0 && !memcmp (key, e->key, key_len)) {
      update_cache_use (cache_next[n]);
      if (verbosity >= 4) {
        fprintf (stderr, "cache found data_len=%d (key_len = %d key = %s)\n", e->data.data_len, key_len, key);
      }
      return e;
    }
    n = cache_next[n];
  }
  if (verbosity >= 4) {
    fprintf (stderr, "cache not found (key_len = %d key = %s)...\n", key_len, key);
  }
  assert (cache_free);
  int e = cache_stack [--cache_free];
  cache_next[e] = cache_next[n];
  cache_prev[cache_next[e]] = e;
  cache_prev[e] = n;
  cache_next[n] = e;
  create_new_cache_item (cache + e, key, key_len);
  free_cache ();
  add_cache_use (e);
  return cache + e;
}


int cache_reload_local (struct entry *entry, struct hash_entry *hash_entry, struct index_entry *index_entry);
int cache_load_local (const char *key, int key_len, struct hash_entry *hash_entry, struct index_entry *index_entry) {
  current_cache = disable_cache ? get_cache_item_simple (key, key_len) : get_cache_item (key, key_len);
  cache_reload_local (current_cache, hash_entry, index_entry); 
  if (current_cache->data.data_len == -2) {
    cache_assign_index_entry_local (current_cache, index_entry);
    if (current_cache->hash_entry && current_cache->hash_entry->accum_value) {
      assert (current_cache->hash_entry->data_len == -2);
      long long arg = current_cache->hash_entry->accum_value;        
      del_entry (current_cache->hash_entry);
      current_cache->hash_entry = 0;
      if (current_cache->index_entry) {
        pmemcached_incr_current (arg);
      }
    }
  }
  cache_ok++;
  return current_cache->data.data_len == -1 ? 0 : 1;
}

int cache_load (const char *key, int key_len, int forceload) {
  if (verbosity >= 4) {
    fprintf (stderr, "loading cache item... ");
  }
  //current_cache = get_cache_item (key, key_len);
  current_cache = disable_cache ? get_cache_item_simple (key, key_len) : get_cache_item (key, key_len);
  if (verbosity >= 4) {
    fprintf (stderr, "current_cache->data.data_len = %d\n", current_cache->data.data_len);
    fprintf (stderr, "forceload = %d\n", forceload);
    fprintf (stderr, "current_cache->key = "); debug_dump_key (current_cache->key, current_cache->key_len);
    fprintf (stderr, "key = "); debug_dump_key (key, key_len);
    fprintf (stderr, "key_len = %d, current_cache->key_len = %d\n", key_len, current_cache->key_len);
  }
  assert (key_len >= 0);
  if (!forceload || current_cache->data.data_len != -2) {
    cache_ok++;
    if (verbosity >= 4) {
      fprintf (stderr, "already in cache\n");
    }
    cache_reload (current_cache);  
    return current_cache->data.data_len == -1 ? 0 : 1;
  } else { 
    cache_miss++;
    if (verbosity >= 4) {
      fprintf (stderr, "not in cache. Adding...");
    }

    cache_reload (current_cache);

    if (!current_cache->hash_entry || current_cache->data.data_len == -2) {
      if (forceload) {
        if (verbosity >= 4) {
          fprintf (stderr, "Copying data from index...");
        }
        if (cache_assign_index_entry (current_cache) == -2) {
          if (verbosity >= 4) {
            fprintf (stderr, "Data is not loaded. Using aio.\n");
          }
          return -2;
        }
        if (current_cache->hash_entry && current_cache->hash_entry->accum_value) {
          assert (current_cache->hash_entry->data_len == -2);
          if (verbosity >= 4) {
            fprintf (stderr, "Adding accumulated data...");
          }
          long long arg = current_cache->hash_entry->accum_value;
          del_entry (current_cache->hash_entry);
          current_cache->hash_entry = 0;
          if (current_cache->index_entry) {
            pmemcached_incr_current (arg);
          }
        }
      } else {
        if (verbosity >= 4) {
          fprintf (stderr, "Skipping loading data from index, because forceload set to 0...");
        }
      }
      if (verbosity >= 4) {
        fprintf (stderr, "Added successfully.\n");
      }
    }                                                                               
    return current_cache->data.data_len == -1 ? 0 : 1;
  }
}

int cache_reload (struct entry *entry) {
  if (verbosity >= 4 && entry->hash_entry) {
    fprintf (stderr, "entry->hash_entry->timestamp = %d, entry->timestamp = %d\n", entry->hash_entry->timestamp, entry->timestamp);
  }
  struct hash_entry *hash_entry = get_entry (entry->key, entry->key_len);
  if (!entry->hash_entry || entry->hash_entry != hash_entry || entry->hash_entry->timestamp != entry->timestamp) {
  //if ((!entry->hash_entry && hash_entry) || (hash_entry && entry->hash_entry->timestamp != entry->timestamp) || entry->data.data_len == -2) {
    if (verbosity >= 4) {
      fprintf (stderr, "Reloading cache information. Hash_entry = %p\n", hash_entry);
    }
    //cache_assign_hash_entry (entry);
    entry->hash_entry = hash_entry;
    cache_assign_data (entry);
    cache_update++;
  }
  return 0;
}

int cache_reload_local (struct entry *entry, struct hash_entry *hash_entry, struct index_entry *index_entry) {
  if (verbosity >= 4 && entry->hash_entry) {
    fprintf (stderr, "entry->hash_entry->timestamp = %d, entry->timestamp = %d\n", entry->hash_entry->timestamp, entry->timestamp);
  }
  //if ((!entry->hash_entry && hash_entry) || (hash_entry && entry->hash_entry->timestamp != entry->timestamp) || entry->data.data_len == -2) {
  if (!entry->hash_entry || entry->hash_entry != hash_entry || entry->hash_entry->timestamp != entry->timestamp) {
    if (verbosity >= 4) {
      fprintf (stderr, "Reloading cache information.\n");
    }
    entry->hash_entry = hash_entry;
    cache_assign_data (entry);
    cache_update++;
  }
  return 0;
}

inline struct data pmemcached_get_current (void) {
  return current_cache->data;
}


int pmemcached_check_time_current (void) {
  if (current_cache->data.data_len != -1 && current_cache->data.delay >= 0 && current_cache->data.delay < get_double_time_since_epoch()) {
    return do_pmemcached_delete (current_cache->key, current_cache->key_len);
  }
  return 0;
}

int pmemcached_delete_current (void) {
  if (current_cache->data.data_len == -1) {
    return -1;
  }
  on_value_change (current_cache->key, current_cache->key_len);
  int delay = current_cache->data.delay;
  if (current_cache->index_entry != 0) {
    pmemcached_store_current (pmct_set, 0, -1, 0, -1);
  } else {
    del_entry (current_cache->hash_entry);
  }
  cache_reload (current_cache);
  if (delay < get_double_time_since_epoch() && delay != DELAY_INFINITY) {
    return -1;
  }
  return 1;
}

int pmemcached_store_current (int op_type, const char *data, int data_len, int flags, int delay) {  
  if (verbosity >= 4) {
    fprintf (stderr, "data_len=%d\n", current_cache->data.data_len);    
  }  
  if (op_type == pmct_add && current_cache->data.data_len != -1) {
    return 0;
  }
  if (op_type == pmct_replace && current_cache->data.data_len == -1) {
    return 0;
  }
  
  on_value_change (current_cache->key, current_cache->key_len);

  hash_entry_t *entry = current_cache->hash_entry;

  if (entry) {
    if (entry->data_len >= 0) {
      zzfree (entry->data, entry->data_len + 1);
      entry_memory -= entry->data_len + 1;
      del_entry_time(entry);
    }
  } else {

    entry = get_new_entry ();

    char *k;
    k = zzmalloc (current_cache->key_len + 1);
    memcpy (k, current_cache->key, current_cache->key_len);
    k[current_cache->key_len] = 0;
    entry_memory += current_cache->key_len + 1;

    entry->key = k;
    entry->key_len = current_cache->key_len;

    add_entry (entry);
  }

  char *buf = 0; 
  if (data_len >= 0) {
    assert (buf = zzmalloc (data_len + 1));
    memcpy(buf, data, data_len);
    buf[data_len] = 0;
    entry_memory += data_len + 1;
  }

  entry->data = buf;
  entry->data_len = data_len;
  entry->flags = flags;
  entry->exp_time = delay;
  entry->timestamp = local_clock();

  add_entry_time (entry);
  cache_reload (current_cache);

  return 1;
}

int pmemcached_incr_current (long long arg) {
  if (current_cache->data.data_len == -1) {
    return -1;
  }
  on_value_change (current_cache->key, current_cache->key_len);

  if (current_cache->data.data_len == -2) {
    if (current_cache->hash_entry) {
      current_cache->hash_entry->accum_value += arg;
    } else {
      pmemcached_store_current (pmct_set, 0, -2, 0, -1);
      current_cache->hash_entry->accum_value = arg;
    }
    return 0;
  }

  unsigned long long val = 0;
  int i, f = 1;

  for (i = 0; i < current_cache->data.data_len && f; i++) {
    if ('0' <= current_cache->data.data[i] && current_cache->data.data[i] <= '9') {
      val = val * 10 + current_cache->data.data[i] - '0';
    } else {
      f = 0;
    }
  }

  if (f == 0) {
    val = 0;
  }

  val+=arg;

  char buff[30];
  sprintf (buff, "%llu", val);

  int len = strlen (buff);

  if (verbosity >= 4) {
    fprintf (stderr, "new value is %s\n", buff);
  }

  return pmemcached_store_current (pmct_set, buff, len, current_cache->data.flags, current_cache->data.delay);
}
                      
void data_prepare_stats (stats_buffer_t *sb) {
  sb_printf (sb,
    "limit_entries_memory\t%lld\n"
    "limit_cache_memory\t%lld\n"
    "limit_metafiles_memory\t%lld\n"
    "cache_number\t%d\n"
    "cache_free\t%d\n"
    "cache_size\t%lld\n"
    "cache_ok\t%lld\n"
    "cache_updates\t%lld\n"
    "cache_miss\t%lld\n"
    "cache_deletes\t%lld\n"
    "entry_number\t%d\n"
    "entry_size\t%lld\n"
    "init_size\t%lld\n",
    memory_for_entries,
    memory_for_cache,
    memory_for_metafiles,
    HASH_TABLE_SIZE - cache_free,
    cache_free,
    cache_size,
    cache_ok,
    cache_update,
    cache_miss,
    cache_deletes,
    get_entry_cnt (),
    entry_memory,
    init_memory);
}
