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

#include "maccub.h"
typedef float rating;

extern int now;
extern int write_only;

typedef struct hash_entry_x hash_entry;
typedef hash_entry* hash_entry_ptr;

struct hash_entry_x
{
  changes data;

  long long key;
  hash_entry_ptr next_entry;
};

typedef struct {
  int size;

  hash_entry_ptr *h;
} hash_table;

void htbl_init (hash_table *table);
void htbl_set_size (hash_table *table, int size);

size_t htbl_get_memory (void);


void htbl_init_mem (int n);

void htbl_free (hash_table *table);

changes* htbl_find (hash_table *table, long long key);
changes* htbl_find_or_create (hash_table *table, long long key);

void htbl_add (hash_table *table, long long key, int val);
void htbl_del (hash_table *table, long long key, int val);

size_t htbl_vct_get_memory (void);

#ifdef HINTS
typedef struct hash_entry_vct_x hash_entry_vct;
typedef hash_entry_vct* hash_entry_vct_ptr;

struct hash_entry_vct_x
{
  vector data;

  long long key;
  hash_entry_vct_ptr next_entry;
};

typedef struct {
  int size;

  hash_entry_vct_ptr *h;
} hash_table_vct;

void htbl_vct_init (hash_table_vct *table);
void htbl_vct_set_size (hash_table_vct *table, int size);

void htbl_vct_init_mem (int n);

void htbl_vct_free (hash_table_vct *table);

vector* htbl_vct_find (hash_table_vct *table, long long key);
vector* htbl_vct_find_or_create (hash_table_vct *table, long long key);

void htbl_vct_add (hash_table_vct *table, long long key, int val);
void htbl_vct_del (hash_table_vct *table, long long key, int val);
#endif


typedef struct {
  int size, currId;

  hash_table to;
  long long *rev;
} lookup_table;

void ltbl_init (lookup_table *table);
void ltbl_set_size (lookup_table *table, int size);
void ltbl_free (lookup_table *table);

int ltbl_add (lookup_table *table, long long key);

//0 if nothing
int ltbl_get_to (lookup_table *table, long long key);
//0 if nothing
long long ltbl_get_rev (lookup_table *table, int val);

typedef struct change_listx change_list;
typedef change_list* change_list_ptr;

struct change_listx {
  change_list_ptr next;
  int timestamp;
  int number;
  union {
    char *s;
    int cnt;
    rating val;
  };
  int x, type;
};

size_t chg_list_get_memory (void);
void chg_list_init (change_list_ptr *st, change_list_ptr *en);

void chg_list_add_int    (change_list_ptr *st, change_list_ptr *en, int type, int x, int cnt);
void chg_list_add_string (change_list_ptr *st, change_list_ptr *en, int type, int x, char *s);
void chg_list_add_rating (change_list_ptr *st, change_list_ptr *en, int type, int x, rating val);
