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

#include "utils.h"

typedef struct trp_nodex trp_node;
struct trp_nodex {
  trp_node *l, *r;
  int x, y;
};

typedef trp_node* trp_node_ptr;

typedef struct {
  int size;
  trp_node_ptr root;
} treap;

void trp_init_mem (int n);

int my_rand (void);

size_t trp_get_memory (void);

void trp_add (treap *v, int x, int y);

void trp_add_or_set (treap *v, int x, int y);

int trp_has (treap v, int x);

trp_node_ptr trp_conv_from_array (int *a, int n);
trp_node_ptr trp_conv_from_array_rev (int *a, int n);

int trp_conv_to_array_rev (treap tr, int *res);

void trp_free (trp_node_ptr v);

void trp_debug_print (trp_node_ptr v, FILE *f);
int trp_del (treap *tr, int x);
void trp_incr (treap *tr, int x);
void trp_init (treap *tr);
