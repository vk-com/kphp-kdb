/*
    This file is part of VK/KittenPHP-DB-Engine Library.

    VK/KittenPHP-DB-Engine Library is free software: you can redistribute it and/or modify
    it under the terms of the GNU Lesser General Public License as published by
    the Free Software Foundation, either version 2 of the License, or
    (at your option) any later version.

    VK/KittenPHP-DB-Engine Library is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Lesser General Public License for more details.

    You should have received a copy of the GNU Lesser General Public License
    along with VK/KittenPHP-DB-Engine Library.  If not, see <http://www.gnu.org/licenses/>.

    Copyright 2010-2013 Vkontakte Ltd
              2010-2013 Arseny Smirnov
              2010-2013 Aliaksei Levin
*/

#pragma once

//#include "vector-def.h"

#include <stdio.h>
#include <assert.h>
#include <stdlib.h>
#include <math.h>

#include "dl-utils.h"

#define PERM(x, args...) DL_ADD_SUFF (perm, x) ( args )

typedef struct dl_prm dl_prm;
struct dl_prm {
  dl_prm *l, *r, *p;
  int a, b, y, len;

  dl_prm *xl, *xr;
};

typedef dl_prm* dl_prm_ptr;

typedef struct {
  int n, len;
  dl_prm_ptr v, rv;
} dl_perm;


int dl_perm_get_i (dl_perm *p, int i);
int dl_perm_get_rev_i (dl_perm *p, int i);
int dl_perm_move (dl_perm *p, int i, int j);
int dl_perm_move_and_create (dl_perm *p, int id, int i);
int dl_perm_del (dl_perm *p, int i);
void dl_perm_inc (dl_perm *p, int n);
void dl_perm_inc_pass (dl_perm *p, int n, int pass_n);
int dl_perm_is_trivial (dl_perm *p);
int dl_perm_is_trivial_pass (dl_perm *p, int pass_n);
int dl_perm_conv_to_array (dl_perm *p, int *a, int n);
int dl_perm_slice (dl_perm *p, int *a, int n, int offset);
void dl_perm_init (dl_perm *p);
void dl_perm_free (dl_perm *p);
int dl_perm_len (dl_perm *p);
void dl_perm_dbg (dl_perm *p);
void _go (dl_prm_ptr v, int d);

typedef struct list_tp perm_list;

struct list_tp{
  perm_list *l, *r;
  int a, b; // [a;b)
};

typedef struct {
  int n, len;
  perm_list *v;
} dl_perm_list;

int dl_perm_list_get_i (dl_perm_list *p, int i);
int dl_perm_list_get_rev_i (dl_perm_list *p, int i);
int dl_perm_list_move_and_create (dl_perm_list *p, int id, int i);
int dl_perm_list_move (dl_perm_list *p, int i, int j);
int dl_perm_list_del (dl_perm_list *p, int i);
void dl_perm_list_inc (dl_perm_list *p, int n);
int dl_perm_list_is_trivial (dl_perm_list *p);
int dl_perm_list_conv_to_array (dl_perm_list *p, int *a, int n);
int dl_perm_list_slice (dl_perm_list *p, int *a, int n, int offset);
void dl_perm_list_init (dl_perm_list *p);
void dl_perm_list_free (dl_perm_list *p);
int dl_perm_list_len (dl_perm_list *p);
void dl_perm_list_dbg (dl_perm_list *p);

/*
typedef struct {
  int x, y;
} pii;

typedef struct {
  vector (pii, v);
  int n;
} dl_perm_vec;

int dl_perm_vec_get_i (dl_perm_vec *p, int i);
void dl_perm_vec_move (dl_perm_vec *p, int i, int j);
void dl_perm_vec_inc (dl_perm_vec *p, int n);
void dl_perm_vec_init (dl_perm_vec *p);
void dl_perm_vec_dbg (dl_perm_vec *p);
  */