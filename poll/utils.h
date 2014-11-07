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

#include <fcntl.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <unistd.h>
#include <assert.h>

#include "dl-utils.h"
#include "../watchcat/utils.h"

#pragma pack(push,4)
typedef struct {
  ll x;
  int y;
} pli;
#pragma pack(pop)

typedef struct treap_nodex treap_node;
struct treap_nodex {
  treap_node *l, *r;
  ll x;
  int y, val;
};

typedef treap_node* treap_node_ptr;

typedef struct {
  int size;
  treap_node_ptr root;
} treap;

void treap_init_mem (int n);
size_t treap_get_memory (void);
void treap_add (treap *v, ll x, int val, int y);

treap_node_ptr treap_fnd (treap *t, ll x);

int treap_conv_to_array (treap_node_ptr v, pli *a, int n);
void treap_free (treap *t);

int treap_del (treap *tr, ll x);
void treap_init (treap *tr);
