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

#define FLAG_REVERSE_SEARCH    0x40000000
#define FLAG_ENTRY_SORT_SEARCH 0x20000000
#define FLAG_PRIORITY_SORT_SEARCH 0x10000000

#define LAST_SEARCH_QUERY_BUFF_SIZE (4 << 10)
extern struct search_index_header Header;
extern char last_search_query[LAST_SEARCH_QUERY_BUFF_SIZE];

extern long long jump_log_pos;
extern int jump_log_ts;
extern unsigned jump_log_crc32;
extern int idx_items, idx_words, idx_hapax_legomena;
extern long idx_bytes, idx_loaded_bytes;

int load_index (kfs_file_handle_t Index);

int init_search_data (int schema);

extern int MAX_MISMATCHED_WORDS;

extern int alloc_tree_nodes, free_tree_nodes;
extern int tot_items, del_items, mod_items, tot_freed_deleted_items, idx_items_with_hash;
extern long long rebuild_hashmap_calls;
//extern int universal, use_stemmer, hashtags_enabled;

typedef struct tree tree_t;

typedef struct item {
  long long item_id;
  int* rates;
  unsigned sum_sqr_freq_title, sum_freq_title_freq_text, sum_sqr_freq_text;
  unsigned short mask;
  char rates_len;
  char extra;
  tree_t *words;
} item_t;

typedef struct index_item {
  long long item_id;
  int* rates;
  unsigned sum_sqr_freq_title, sum_freq_title_freq_text, sum_sqr_freq_text;
  unsigned short mask;
  char rates_len;
  char extra;
} index_item_t;

struct tree {
  tree_t *left, *right;
  hash_t word;
  struct item *item;
  tree_t *next; /* list tree nodes with same item */
  int y;
  unsigned short freq_title, freq_text;
};

struct searchx_list_decoder {
  struct list_decoder *dec;
  int len;
  int remaining;
};

typedef struct index_list_decoder {
  long long item_id;
  long long extra; /* used in hapax legomena case */
  struct search_index_word *sword;
  int doc_id;
  struct searchx_list_decoder dec;
  unsigned short freq_title, freq_text;
} ilist_decoder_t;

typedef struct intersect_heap_entry {
  tree_t *TS[MAX_TREE_DEPTH];
  int Bt[MAX_TREE_DEPTH];
  hash_t word;
  long long item_id;
  item_t *cur, *cur0, *cur1;
  int sp;
  unsigned short cur_freq_title, cur_freq_text;
  unsigned short cur_freq_title0, cur_freq_text0;
  char tag;

  ilist_decoder_t Decoder;
} iheap_en_t;

#define	SHORT_ID(__x)	((int) ((__x) >> 32))

//#define	RATE_DELETED	(-1 << 31)
#define FLAG_DELETED 1
#define	ITEM_DELETED(__x)	((__x)->extra & FLAG_DELETED)

//extern int Q_order, Q_limit, Q_words, Q_hash_group_mode, Q_min_priority, R_cnt, R_tot, R_tot_undef_hash;
extern int R_cnt, R_tot, Q_hash_group_mode, Q_limit;
extern item_t *R[MAX_RES+1];
extern double RR[MAX_RES+1];
char *parse_query (char *text, int *Q_raw, int *error);
int perform_query (void);
extern const char* rate_first_characters;

int do_delete_item (long long item_id);
int do_change_item (const char *text, int len, long long item_id, int rate, int rate2);
//int do_change_item_long (netbuffer_t *Source, int len, long long item_id, int rate, int rate2);

//int do_set_rate (long long item_id, int rate);
//int do_set_rate2 (long long item_id, int rate2);
int do_set_rates (long long item_id, int rate, int rate2);
int do_incr_rate (long long item_id, int rate_incr);
int do_incr_rate2 (long long item_id, int rate2_incr);
int do_incr_rate_new (long long item_id, int p, int rate_incr);
int do_set_rate_new (long long item_id, int p, int rate);
int do_set_hash (long long item_id, long long hash);
int get_single_rate (int *rate, long long item_id, int p);
int get_rates (int *rates, long long item_id);
int get_sorting_mode (int c);
int get_hash (long long *hash, long long item_id);

long long get_hash_item (const struct item *I);
long long get_hash_item_unsafe (const struct item *I);

extern struct hashset_ll hs;

#endif
