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

    Copyright 2010-2013 Vkontakte Ltd
              2010-2013 Nikolai Durov
              2010-2013 Andrei Lopatin
              2011-2013 Vitaliy Valtman
*/

#ifndef __LISTS_DATA_H__
#define __LISTS_DATA_H__

#include "kdb-data-common.h"
#include "kdb-lists-binlog.h"
#include "kfs.h"
//#if ( defined(LISTS64) || defined(LISTS_Z))
# define LISTS_GT
//#endif
#ifdef LISTS_GT
typedef unsigned int global_id_t;
#else
typedef int global_id_t;
#endif
#include "lists-index-layout.h"


#ifdef _LP64
# define LISTS_PRIME	50000017
#else
# define LISTS_PRIME	30000001
#endif
#define	MAX_LISTS	((int) (LISTS_PRIME * 0.7))
#define	MAX_LIST_SIZE	10000000

#define	CYCLIC_BUFFER_SIZE	(1 << 17)

//#define	IDX_MIN_FREE_HEAP	(64L << 20)
#define	IDX_MIN_FREE_HEAP	(1L << 30)
extern long long idx_min_free_heap;

extern int object_id_ints, list_id_ints;

extern int value_offset;

extern long long jump_log_pos;
extern int jump_log_ts;
extern unsigned jump_log_crc32;
extern int idx_lists; 
extern global_id_t idx_last_global_id;
extern long long idx_bytes, idx_loaded_bytes;

extern int index_mode;
extern int remove_dates;

int load_index (void);
int write_index (int writing_binlog);

int init_lists_data (int schema);

extern int alloc_ltree_nodes, alloc_large_nodes, alloc_global_nodes, alloc_small_nodes;

typedef struct ltree ltree_t;


struct ltree {
  ltree_t *left, *right;
  int y;
  ltree_x_t x;
};

extern int max_lists;
extern int tot_lists;
extern long tot_list_entries;
extern int negative_list_id_offset;
extern global_id_t last_global_id;

extern int debug_list_id;

typedef struct list list_t;

#define	SUBCATS	8

/* node types */
#define	TF_PLUS		1	/* node present in memory, absent in metafile */
#define	TF_ZERO		0	/* node replaced, but text remains in metafile unchanged */
#define	TF_MINUS	3	/* node absent from memory, present in metafile */
#define	TF_REPLACED	2	/* node replaced with new text */

//#pragma pack(push,4)

typedef struct tree_ext_small tree_ext_small_t;
typedef struct tree_ext_global tree_ext_global_t;
typedef struct tree_ext_large tree_ext_large_t;

#ifdef LISTS_GT
typedef tree_ext_global_t tree_ext_xglobal_t;
#else
typedef tree_ext_small_t tree_ext_xglobal_t;
#endif




struct tree_ext_small {
  tree_ext_small_t *left, *right;
  int y;
  int rpos;		/* # of (A_i >= x) in related list, i.e. A[N-i-1]<x<=A[N-i] */
  			/* actually, we store rpos' = rpos*4 + node_type */
  int delta;		/* sum of deltas for subtree, this node included */
  alloc_object_id_t x;
};	


struct tree_ext_global {
  tree_ext_global_t *left, *right;
  int y;
  int rpos;		/* # of (A_i >= x) in related list, i.e. A[N-i-1]<x<=A[N-i] */
  			/* actually, we store rpos' = rpos*4 + node_type */
  int delta;		/* sum of deltas for subtree, this node included */
  global_id_t x;
  alloc_object_id_t z;
};


struct tree_payload {
  char *text;
  global_id_t global_id;	
  value_t value;
  int flags;
  int date;
};


struct tree_ext_large {
  tree_ext_large_t *left, *right;
  int y;
  int rpos;		/* # of (A_i >= x) in related list, i.e. A[N-i-1]<x<=A[N-i] */
  			/* actually, we store rpos' = rpos*4 + node_type */
  int delta;		/* sum of deltas for subtree, this node included */
  alloc_object_id_t x;
  struct tree_payload payload;
};

//#pragma pack(pop)

typedef struct list_tree_direct listree_direct_t;
typedef struct list_tree_global listree_global_t;
typedef struct list_tree_indirect listree_t;

#ifdef LISTS_GT
typedef listree_global_t listree_xglobal_t;
#else
typedef listree_t listree_xglobal_t;
#endif

struct list_tree_direct {
  int N;
  tree_ext_large_t **root;
  array_object_id_t *A;	// *root->left->...->right->x is of the same type as A[j]
  			// A[0] < A[1] < ... < A[N-1]
  			// i is the temp_id for A[i], so A corresponds to DA in tree_list_indirect
  			// with IA[j] := j for all j
  			// A = 0  <=> N = 0
};

struct list_tree_global {
  int N;
  tree_ext_global_t **root;
  int *IA;		// IA[0..N-1] contains (distinct >= 0) indices in DA  (DA[IA[i]] is valid)
  			// DA[IA[0]] < DA[IA[1]] < ... < DA[IA[N-1]]
  			// actually, IA[i] is a temp_id of a record
  global_id_t *DA;		// *root->left->...->right->x is of the same type as DA[j]
};

struct list_tree_indirect {
  int N;
  tree_ext_small_t **root;
  int *IA;		// IA[0..N-1] contains (distinct >= 0) indices in DA  (DA[IA[i]] is valid)
  			// DA[IA[0]] < DA[IA[1]] < ... < DA[IA[N-1]]
  			// actually, IA[i] is a temp_id of a record
  array_object_id_t *DA;	// *root->left->...->right->x is of the same type as DA[j]
  			// might be reduced to list_tree_direct with A[i] := DA[IA[i]]
};


struct list {
  int metafile_index;	    /* metafile index reference, -1 if not present in index or complete deletion performed */
  tree_ext_large_t *o_tree;
  tree_ext_xglobal_t *g_tree;
  tree_ext_small_t *o_tree_sub[SUBCATS];
  tree_ext_xglobal_t *g_tree_sub[SUBCATS];
  var_list_id_t list_id;
};
  

struct cyclic_buffer_entry {
  int flags;
  int value;
  global_id_t global_id;
  int date;
  char *text;
  int extra[4];
  alloc_list_id_t list_id;
  alloc_object_id_t object_id;
};

struct saved_data_small {
  int op;
  var_list_id_t list_id;
  var_object_id_t object_id;
  int a, c, d;
  long long b;
};

struct saved_data_long {
  int op;
  var_list_id_t list_id;
  var_object_id_t object_id;
  int a, c, d, e, f;
  long long b;
  char *g;
};

int do_delete_list (list_id_t list_id);
int do_delete_object (object_id_t object_id);
int do_delete_list_entry (list_id_t list_id, object_id_t object_id);

int do_add_list_entry (list_id_t list_id, object_id_t object_id, int mode, int flags, value_t value, const int *extra);
int do_change_entry_text (list_id_t list_id, object_id_t object_id, const char *text, int len);
int do_change_entry_flags (list_id_t list_id, object_id_t object_id, int set_flags, int clear_flags);
long long do_change_entry_value (list_id_t list_id, object_id_t object_id, value_t value, int incr);
long long do_add_incr_value (list_id_t list_id, object_id_t object_id, int flags, value_t value, const int *extra);

int do_delete_sublist (list_id_t list_id, int xor_cond, int and_cond);
int do_change_sublist_flags (list_id_t list_id, int xor_cond, int and_cond, int and_set, int xor_set);

int fetch_list_entry (list_id_t list_id, object_id_t object_id, int result[13]);
int get_list_counter (list_id_t list_id, int counter_id);
int fetch_list_counters (list_id_t list_id, int result[SUBCATS+1]);
int get_entry_position (list_id_t list_id, object_id_t object_id);
int get_entry_sublist_position (list_id_t list_id, object_id_t object_id, int mode);

#define	MAX_RES	131072
extern int R[MAX_RES+64];
extern int *R_end, R_entry_size;

#define	RR	((long long *) R)

#define VSORT_HEAP_SIZE	16384

int prepare_list (list_id_t list_id, int mode, int limit, int offset);
int prepare_list_intersection (list_id_t list_id, int mode, array_object_id_t List[], int LL, int have_weights, int id_ints);
int prepare_list_subtraction (list_id_t list_id, int mode, array_object_id_t List[], int LL, int have_weights, int id_ints);
long long prepare_list_sum (list_id_t list_id, int mode, array_object_id_t List[], int LL, int have_weights, int id_ints);

int prepare_value_sorted_list (list_id_t list_id, int xor_mask, int and_mask, int mode, int limit);

int prepare_list_date_distr (list_id_t list_id, int mode, int min_date, int max_date, int step);

long long dump_all_value_sums (void);

int dump_all_lists (int sublist, int dump_rem, int dump_mod);
void dump_msizes (void);
void init_hash_table (int x);

int conv_list_id (list_id_t list_id);

void ignore_list_object_add (list_id_t list_id, object_id_t object_id);
int ignore_list_check (list_id_t list_id, object_id_t object_id);
#endif
