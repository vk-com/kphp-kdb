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
              2011-2013 Anton Maydell
*/

#ifndef __SEARCH_COMMON_H__
#define __SEARCH_COMMON_H__

#include "kdb-data-common.h"

typedef struct {
  hash_t word;
  unsigned freqs;
} pair_word_freqs_t;

/* returns in hiword: number unique words in title, in loword: number unique words in title+text (tags not included) */
unsigned evaluate_uniq_words_count (pair_word_freqs_t *Q, int n);
void evaluate_freq_sqr_sums (pair_word_freqs_t *Q, int n, unsigned *sum_sqr_freq_title, unsigned *sum_freq_title_freq_text, unsigned *sum_sqr_freq_text);
int extract_words (const char *text, int len, int universal, pair_word_freqs_t *Q, int max_words, int tag_owner, long long item_id);

unsigned long long searchy_word_hash (const char *str, int len);
int searchy_extract_words (const char *text, int len, int universal, pair_word_freqs_t *Q, int max_words, int tag_owner, long long item_id, int *positions);

hash_t searchy_term_hash (char *text, int wl, int stem);
static inline int searchy_is_term (hash_t word) {
  return (word & 0x8000000000000000ULL) ? 1 : 0;
}

#endif

