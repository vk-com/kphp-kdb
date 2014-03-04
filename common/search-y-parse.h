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
              2008-2013 Nikolai Durov
              2008-2013 Andrei Lopatin
              2010-2013 Arseny Smirnov
              2010-2013 Aliaksei Levin
              2010-2013 Vitaliy Valtman
              2010-2013 Anton Maydell
*/

#ifndef __COMMON_SEARCH_Y_PARSE_H__
#define __COMMON_SEARCH_Y_PARSE_H__

#include "kdb-data-common.h"

#define SEARCHY_MAX_QUERY_WORDS 1024
#define SEARCHY_MAX_QUERY_PHRASES 128

typedef struct {
  hash_t word;
  unsigned position;
} searchy_pair_word_position_t;

enum searchy_query_phrase_type {
  sqpt_no_quotation = 0,
  sqpt_single_quotation = 1,
  sqpt_double_quotation = 2
};

typedef struct searchy_query_phrase {
  struct searchy_query_phrase *next;
  hash_t *H; /* phrase words */
  int words;
  enum searchy_query_phrase_type type;
} searchy_query_phrase_t;

typedef struct {
  /* public */
  searchy_query_phrase_t *phrases[2]; /* [0] - plus, [1] - minus */
  int words;
  char error[128];
  /* private */
  hash_t words_buf[SEARCHY_MAX_QUERY_WORDS];
  searchy_query_phrase_t *last_phrase;
} searchy_query_t;

extern inline int searchy_is_term (hash_t word) {
  return (word & 0x8000000000000000ULL) ? 1 : 0;
}

unsigned long long searchy_word_hash (const char *str, int len);
int searchy_extract_words (const char *text, int len, searchy_pair_word_position_t *Q, int max_words, int universal, int tag_owner, long long item_id, int *positions);
int searchy_query_parse (searchy_query_t *Q, char *query);
void searchy_query_free (searchy_query_t *Q);

#endif
