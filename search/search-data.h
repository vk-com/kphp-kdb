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
#define	MAX_WORDS		64

#define	MAX_RES		65536
#define MAX_ITEMS_IN_GROUP 50
#define USE_AIO 0

#define FLAG_REVERSE_SEARCH       0x40000000
#define FLAG_ENTRY_SORT_SEARCH    0x20000000
#define FLAG_PRIORITY_SORT_SEARCH 0x10000000
#define FLAG_ONLY_TITLE_SEARCH    0x08000000

#define MAX_RATE_WEIGHTS 16

#define LAST_SEARCH_QUERY_BUFF_SIZE (4 << 10)
extern struct search_index_header Header;
extern char last_search_query[LAST_SEARCH_QUERY_BUFF_SIZE];

extern long long jump_log_pos;
extern int jump_log_ts;
extern unsigned jump_log_crc32;
extern int idx_items, idx_words, idx_hapax_legomena;
extern long idx_bytes, idx_loaded_bytes;
extern int import_only_mode;

int load_index (kfs_file_handle_t Index);

int init_search_data (int schema);

extern int MAX_MISMATCHED_WORDS;

extern int alloc_tree_nodes, free_tree_nodes;
extern int tot_items, del_items, mod_items, tot_freed_deleted_items, idx_items_with_hash;
extern long long rebuild_hashmap_calls, assign_max_set_rate_calls, change_multiple_rates_set_rate_calls;

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
  tree_t *left, *right;
  hash_t word;
  item_t *item;
  tree_t *next; /* list tree nodes with same item */
  int y;
};

struct search_list_decoder {
  struct list_decoder *dec;
  int len;
  int remaining;
};

typedef struct index_list_decoder {
  long long item_id;
  struct search_index_word *sword;
  int doc_id;
  int field;
  int last_subseq;
  int extra;
  struct search_list_decoder dec;
  struct search_list_decoder dec_subseq;
  int (*adv_ilist_subseq) (struct index_list_decoder *, int);
} ilist_decoder_t;

typedef struct intersect_heap_entry {
  tree_t *TS[MAX_TREE_DEPTH];
  int Bt[MAX_TREE_DEPTH];
  hash_t word;
  long long item_id;
  void (*ihe_skip_advance1) (struct intersect_heap_entry *A, long long);
  item_t *cur, *cur0, *cur1;
  int sp;
  int cur_y, cur_y1;
  int tag_word;
  int optional_tag_weight;
  ilist_decoder_t Decoder;
} iheap_en_t;

#define	SHORT_ID(__x)	((int) ((__x) >> 32))

//#define	RATE_DELETED	(-1 << 31)
#define FLAG_DELETED 1
#define	ITEM_DELETED(__x)	((__x)->extra & FLAG_DELETED)

extern int Q_order, Q_limit, Q_extmode, Q_words, Q_hash_group_mode, Q_min_priority, R_cnt, R_tot, R_tot_undef_hash;
extern item_t *R[MAX_RES+1];
extern int RV[MAX_RES+1];
extern int R_HASH_NEXT[MAX_RES];
char *parse_query (char *text, int do_parse_ranges);
char *parse_relevance_search_query (char *text, int *Q_raw, int *error, int do_parse_ranges);
char *parse_relevance_search_query_raw (char *text);
int perform_query (void);
extern const char* rate_first_characters;

int do_delete_item (long long item_id);
int do_delete_items_with_hash (long long hash);
int do_delete_items_with_hash_using_hashset (struct hashset_ll *H);
int do_delete_items_with_rate_using_hashset (struct hashset_int *HS, int rate_id);
int do_assign_max_rate_using_hashset (struct hashset_ll *HS, int rate_id, int value);
int do_change_multiple_rates_using_hashmap (struct hashmap_int_int *HM, int rate_id);
int do_change_item (const char *text, int len, long long item_id, int rate, int rate2);
int do_add_item_tags (const char *const text, int len, long long item_id);
int do_set_rates (long long item_id, int rate, int rate2);
int do_incr_rate (long long item_id, int rate_incr);
int do_incr_rate2 (long long item_id, int rate2_incr);
int do_incr_rate_new (long long item_id, int p, int rate_incr);
int do_set_rate_new (long long item_id, int p, int rate);
int do_set_hash (long long item_id, long long hash);
int do_reset_all_rates (int p);
int get_single_rate (int *rate, long long item_id, int p);
int get_rates (int *rates, long long item_id);
int get_sorting_mode (int c);
int get_hash (long long *hash, long long item_id);

long long extract_hash_item (const item_t *I);
long long get_hash_item_unsafe (const item_t *I);
extern struct hashset_ll hs;

void init_ranges (void);
void add_range (int type, int min_value, int max_value);
void sort_ranges (void);

void init_rate_weights (void);
void add_rate_weight (int rate_type, double weight, int flags);
void add_decay (int rate_type, double weight);
int normalize_query_rate_weights (void);

int do_contained_query (long long item_id, char **text);
#endif
