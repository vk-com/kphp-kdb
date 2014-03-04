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

#ifndef _AHO_H_
#define _AHO_H_

#include <assert.h>
#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <string.h>

#define CHAR char

typedef struct trie_node_t trie_node;

struct trie_node_t {
  trie_node *h, *v;
  int code;
  int is_end, cnt;
};

trie_node *get_node (void);

void trie_add (trie_node **v, CHAR *s);
void trie_del (trie_node *v, CHAR *s);

typedef struct trie_arr_node_t trie_arr_node;

struct trie_arr_node_t {
  int is_end, en, suff;
  int edges[2];
};

size_t trie_encode (trie_node *v, char *buff, int is_end);
void trie_arr_aho (trie_arr_node *v);
int trie_arr_check (trie_arr_node *v, CHAR *s);
void trie_arr_text_save (trie_arr_node *v, char *buff, int *bn);
void trie_arr_print (trie_arr_node *v);


#endif