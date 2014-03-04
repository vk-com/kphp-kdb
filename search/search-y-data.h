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

#ifndef __SEARCH_DATA_H__
#define __SEARCH_DATA_H__

#include "kdb-data-common.h"
#include "kdb-search-binlog.h"
#include "search-index-layout.h"
#include "net-buffers.h"
#include "kfs.h"
#include "utils.h"
#include "listcomp.h"

#define MAX_RATES 16
#define	MAX_ITEMS		(1 << 24)
#define	ITEMS_HASH_PRIME	24000001
#define	MAX_TREE_DEPTH		256
//#define	MAX_WORDS		1024
//#define	MAX_PHRASES		128

#define	MAX_RES		65536
//#define MAX_ITEMS_IN_GROUP 50

#define FLAG_REVERSE_SEARCH       0x40000000
#define FLAG_ENTRY_SORT_SEARCH    0x20000000
#define FLAG_PRIORITY_SORT_SEARCH 0x10000000
#define FLAG_ONLY_TITLE_SEARCH    0x08000000

#define MAX_RATE_WEIGHTS 16

extern struct search_index_header Header;

extern long long jump_log_pos;
extern int jump_log_ts;
extern unsigned jump_log_crc32;
extern int idx_items, idx_words, idx_hapax_legomena;
extern long long idx_bytes, idx_loaded_bytes, tree_positions_bytes;
extern int import_only_mode;

int load_index (kfs_file_handle_t Index);

int init_search_data (int schema);

extern int alloc_tree_nodes, free_tree_nodes, decoder_positions_max_capacity, max_search_query_memory;
extern int tot_items, del_items, mod_items, tot_freed_deleted_items;

typedef struct tree tree_t;

typedef struct {
  long long item_id;
  int *rates;
  unsigned short mask;
  char rates_len;
  char extra;
  tree_t *words;
} item_t;

typedef struct index_item {
  long long item_id;
  int *rates;
  unsigned short mask;
  char rates_len;
  char extra;
} index_item_t;

struct tree {
  hash_t word;
  tree_t *left, *right;
  item_t *item;
  tree_t *next; /* list tree nodes with same item */
  int *positions;
  int y;
};


/* ylist : compression increasing sequence of integers with coordinates (Golomb) using interpolative encoding with jumps */

typedef struct list_int_entry {
  struct list_int_entry *next;
  int value;
} list_int_entry_t;


struct ylist_decoder_stack_entry {
  struct list_int_entry *positions_head, *positions_tail;
  int left_idx;
  int left_value;
  int middle_value;
  int right_idx;
  int right_value;
  int right_subtree_offset;
  int num;
};


/*
struct search_list_decoder {
  union {
    struct list_decoder *tag_dec;
    struct ylist_decoder *term_dec;
  } d;
  int len;
  int remaining;
};
*/

typedef struct {
  struct searchy_index_word *sword;
  union {
    struct list_decoder *tag_dec;
    struct ylist_decoder *term_dec;
  } dec;
  int term;
  int doc_id;
  int len;
  int remaining;
} ilist_decoder_t;

typedef struct intersect_heap_entry {
  tree_t *TS[MAX_TREE_DEPTH];
  int Bt[MAX_TREE_DEPTH];
  long long item_id;
  hash_t word;
  item_t *cur, *cur0, *cur1;
  int *positions, *positions1;
  int sp;
  ilist_decoder_t Decoder;
} iheap_en_t;

struct ylist_decoder {
  iheap_en_t *H;
  int *positions;
  struct bitreader br;
  int size;
  int k, p, last;
  int left_subtree_size_threshold;
  int N, K;
  int capacity;
  struct ylist_decoder_stack_entry stack[0];
};

typedef struct {
  iheap_en_t **E;
  int words;
} searchy_iheap_phrase_t;

//#define	RATE_DELETED	(-1 << 31)
#define FLAG_DELETED 1
#define	ITEM_DELETED(__x)	((__x)->extra & FLAG_DELETED)

extern int Q_order, Q_limit, R_cnt, R_tot;
extern item_t *R[MAX_RES+1];
extern int RV[MAX_RES+1];

void init_ranges (void);
void add_range (int type, int min_value, int max_value);
void sort_ranges (void);
//int searchy_parse_query (char *query);
//int perform_query (void);
int searchy_perform_query (char *query);

int get_single_rate (int *rate, long long item_id, int p);
int do_change_item (const char *text, int len, long long item_id, int rate, int rate2);
int do_delete_item (long long item_id);
int do_incr_rate_new (long long item_id, int p, int rate_incr);
int do_set_rate_new (long long item_id, int p, int rate);

void check_lists_decoding (void);
#endif
