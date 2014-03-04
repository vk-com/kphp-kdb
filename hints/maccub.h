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
              2010-2013 Arseny Smirnov
              2010-2013 Aliaksei Levin
*/

#pragma once

#include <assert.h>
#include <unistd.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <math.h>
#include <time.h>

#include "treap.h"

typedef int *changes;

#define CHG_INIT(x) (x = 0)

void chg_add (changes *x, int val);
void chg_del (changes *x, int val);
int  chg_has (changes x, int val);
int  chg_max_size (changes x);
int  chg_conv_to_array (changes x, int *res);
void chg_debug_print (changes x, FILE *f);
void chg_free (changes *x);

#ifdef HINTS

#define MAX_WORDS 15

//#define GET_CHANGES(x) (*(changes *)((x) + 1 - (x)[0]))


// may be golomb, blist, bit code, interpolation code
#define LIST bcode
#define LIST_(x) bcode_ ## x

/* LIST
 * 1. save/load.
 *  int pref_encode (int *P, int len, int tot_items, unsigned char *res);
 *
 * 2. iterate
 *  iterator must be able to:
 *
 *  pref_iterator
 *
 *  data_iter_val_and_next
 *
 *  int pref_iter_val ();
 *  int pref_iter_next ();
 *  int pref_iter_can_has ();
 *  int pref_iter_has (int val); // may be empty but must be
 *  int pref_iter_max_size ();
 *
 *
 *
 *  #LIST_(x) PREF_##x
 */

//TODO unify iterator field names

typedef int* blist;
typedef const char* golomb;
typedef const unsigned char* bcode;
typedef const unsigned char* iCode;

typedef struct {
  changes x;

  int i;
#define IT_MAXN 200
  trp_node_ptr stack_ptr[IT_MAXN];
  int stack_state[IT_MAXN], stack_top;
} chg_iterator;

void chg_iter_init (chg_iterator *it, changes x);
void chg_iter_next (chg_iterator *it);
int chg_iter_val (chg_iterator *it);

typedef struct {
  blist x;

  int i, val;
} blist_iterator;

typedef struct {
  golomb ptr;

  int a, k, m, M, len, p;
} golomb_iterator;

typedef struct {
  bcode ptr;

  int i, k, len, val;
} bcode_iterator;

typedef struct {
  iCode ptr;

  int n[IT_MAXN], l[IT_MAXN], r[IT_MAXN], st[IT_MAXN], s_val[IT_MAXN];

  int val, pred, top;
} iCode_iterator;

typedef struct {
  int val;
  int l, r, tot_items;

  LIST_(iterator) list_it;

  chg_iterator chg_it;
} data_iterator;


void data_iter_init (data_iterator *it, LIST x, changes y, int tot_items, int len, int l, int r);
int data_iter_val_and_next (data_iterator *it);
int data_iter_can_has (void);
int data_iter_has (data_iterator *it, int val);

int LIST_(encode_list) (int *P, int len, int tot_items, unsigned char *res);

typedef struct {
  data_iterator it[MAX_WORDS];
  int l, r;

  int n, val;
} uni_iterator;

extern int *intersect_buff;
int *uni_iter_intersect (uni_iterator *a, int n, int max_cnt);

void uni_iter_add (uni_iterator *v, LIST a, changes b, int tot_items, int len);
int uni_iter_val_and_next (uni_iterator *it);
int uni_iter_can_has (void);
int uni_iter_has (uni_iterator *it, int val);

#endif