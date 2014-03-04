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

#ifndef _HASH_TABLE_H_
#define _HASH_TABLE_H_

#include "utils.h"

typedef struct hash_entry_x hash_entry;
typedef hash_entry* hash_entry_ptr;

struct hash_entry_x
{
  int data;
  int key;
  hash_entry_ptr next_entry;
};

typedef struct {
  int size;
  int hash;

  hash_entry_ptr *h;
} hash_table;

void htbl_init (hash_table *table);
void htbl_set_size (hash_table *table, int size);

size_t htbl_get_memory (void);

void htbl_init_mem (int n);
void htbl_free (hash_table *table);
int* htbl_find (hash_table *table, int key);
int* htbl_find_or_create (hash_table *table, int key);
int htbl_del (hash_table *table, int key);


typedef struct {
  int size, currId;
  int hash;

  hash_table to;
  int *rev;
} lookup_table;

void ltbl_init (lookup_table *table);
void ltbl_set_size (lookup_table *table, int size);
void ltbl_free (lookup_table *table);

int ltbl_add (lookup_table *table, int key);

//0 if nothing
int ltbl_get_to (lookup_table *table, int key);
//0 if nothing
int ltbl_get_rev (lookup_table *table, int val);



#define spam x
#define ham y

#pragma pack(push,4)

typedef struct {
  int x, y;
} pair;

typedef struct {
  ll h;
  pair val;
} entry_t;


typedef struct {
  int n, size;
  entry_t *e;
} qhash_table;

#pragma pack(pop)

void qhtbl_init (qhash_table *ht);
void qhtbl_set_size (qhash_table *ht, int size);
inline pair* qhtbl_add (qhash_table *ht, ll h);
inline pair* qhtbl_get (qhash_table *ht, ll h);
void qhtbl_free (qhash_table *ht);

#endif