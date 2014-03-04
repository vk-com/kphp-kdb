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
              2009-2013 Nikolai Durov
              2009-2013 Andrei Lopatin
*/

#ifndef __TARG_SEARCH_H__
#define __TARG_SEARCH_H__

#include "targ-data.h"

extern int tot_queries;
extern double tot_queries_time;

/* aux userlist */

#define MAX_AUX_USERS	(1 << 16)

extern int aux_userlist[MAX_AUX_USERS];
extern int aux_userlist_size;
extern int aux_userlist_tag;

/* parser */

#define MAX_AUX_QUERIES	256

#define	INFTY	1000000000
#define MAX_QUERY_NODES	1024
#define MAX_HASHED_WORDS	256

#define	MAX_IB_SIZE	1000000

/* enum query_type moved to targ-index-layout.h */

typedef struct query query_t;

struct query {
  int type;
  int flags;
  query_t *left, *right, *last;
  int value, value2;
  hash_t hash, hash2;
  int max_res;
  int complexity;
};

struct query_keyword_descr {
  int q_type;
  int flags;
  int minv;
  int maxv;
  char *str;
};

extern char *Qs;
extern int Q_order, Q_limit, Q_raw;
extern query_t *Qq;

extern int global_birthday_in_query;

char *compile_query (char *str);
query_t *compile_add_query (char **str_ptr);
void clear_query_list (void);

int perform_query (int seed);

int perform_audience_query (void);
int perform_targ_audience_query (int place, int cpv, int and_mask, int xor_mask);

typedef void (*store_res_func_t)(int uid);
typedef void (*postprocess_res_func_t)(void);
extern store_res_func_t store_res;
extern postprocess_res_func_t postprocess_res;

void store_res_std (int uid);

void dump_profiler_data (void);

typedef struct iterator *iterator_t;

typedef int (*iterator_jump_func_t)(iterator_t I, int req_pos);

struct iterator {
  iterator_jump_func_t jump_to;
  int pos;
};

struct mult_iterator {
  iterator_jump_func_t jump_to;
  int pos, mult;
};

iterator_t build_word_iterator (hash_t word);

#endif
