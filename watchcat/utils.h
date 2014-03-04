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
              2011-2013 Arseny Smirnov
              2011-2013 Aliaksei Levin
*/

#pragma once
#include "search-y-parse.h"

#define MAX_NAME_SIZE 65536

char *prepare_watchcat_str (char *x, int uni);

/***
  Query
 ***/
typedef struct {
  hash_t *H;
  int words;
  enum searchy_query_phrase_type type;
  char minus_flag;
} watchcat_query_phrase_t;

typedef struct {
  watchcat_query_phrase_t *phrases;
  int phrases_cnt;

  hash_t *words_buf;
  int words;
} watchcat_query_t;

watchcat_query_t *create_watchcat_query (char *s);
void free_watchcat_query (watchcat_query_t *q);
long long watchcat_query_hash (char *s);
long long watchcat_query_hash_impl (watchcat_query_t *query);

/*** Entry ***/
#define MAX_WATCHCAT_ENTRY_SIZE 65536
typedef struct {
  searchy_pair_word_position_t by_pos[MAX_WATCHCAT_ENTRY_SIZE];
  searchy_pair_word_position_t by_hash[MAX_WATCHCAT_ENTRY_SIZE];
  int n;
} watchcat_entry_t;

void watchcat_prepare_entry (watchcat_entry_t *entry, const char *text, int len);
int check_watchcat_query (watchcat_entry_t *entry, watchcat_query_t *query);
