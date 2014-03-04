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
#include "dl-utils.h"

typedef struct dl_trp_nodex dl_trp_node;
struct dl_trp_nodex {
  dl_trp_node *l, *r;
  int x, y;
};

typedef dl_trp_node* dl_trp_node_ptr;

typedef struct {
  int size;
  dl_trp_node_ptr root;
} dl_treap;

void dl_trp_init_mem (int n);

int my_rand (void);

size_t dl_trp_get_memory (void);

void dl_trp_add (dl_treap *v, int x, int y);

void dl_trp_add_or_set (dl_treap *v, int x, int y);

int dl_trp_has (dl_treap v, int x);
int dl_trp_fnd (dl_treap *t, int x);

dl_trp_node_ptr dl_trp_conv_from_array (int *a, int n);
dl_trp_node_ptr dl_trp_conv_from_array_rev (int *a, int n);

int dl_trp_conv_to_array (dl_trp_node_ptr v, int *a, int n);


void dl_trp_free (dl_trp_node_ptr v);

void dl_trp_debug_print (dl_trp_node_ptr v, FILE *f);
int dl_trp_del (dl_treap *tr, int x);
void dl_trp_incr (dl_treap *tr, int x);
void dl_trp_init (dl_treap *tr);

