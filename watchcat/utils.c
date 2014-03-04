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

#define _FILE_OFFSET_BITS 64


#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <assert.h>


#include "utils.h"
#include "word-split.h"
#include "stemmer-new.h"
#include "dl-hashable.h"
#include "utf8_utils.h"
#include "dl-utils.h"

char prep_buf[MAX_NAME_SIZE];
char prep_buf_res[MAX_NAME_SIZE];
char *words_buf[MAX_NAME_SIZE];

inline int strcmp_void (const void *x, const void *y) {
  const char *s1 = *(const char **)x;
  const char *s2 = *(const char **)y;
  while (*s1 == *s2 && *s1 != ' ')
    s1++, s2++;
  return *s1-*s2;
}

inline int iseq (const char *s1, const char *s2) {
  while (*s1 == *s2 && *s1 != ' ')
    s1++, s2++;
  return *s1 == ' ' && *s2 == ' ';
}

char *prepare_str_cp1251 (char *v) {
  int i;
  for (i = 0; v[i] == ' '; i++) {
  }

  int k = 0;
  while (v[i]) {
    words_buf[k++] = v + i;
    while (v[i] && v[i] != ' ') {
      i++;
    }
    while (v[i] == ' ') {
      i++;
    }
  }
  v[i] = ' ';

  int j = 0;
  qsort (words_buf, k, sizeof (char *), strcmp_void);

  for (i = 0; i < k; i++) {
    if (i == 0 || !iseq (words_buf[i - 1], words_buf[i])) {
      words_buf[j++] = words_buf[i];
    }
  }
  k = j;

  char *res = prep_buf_res;
  for (i = 0; i < k; i++) {
    char *tmp = words_buf[i];
    while (*tmp != ' ') {
      *res++ = *tmp++;
    }
    *res++ = '+';
  }
  *res++ = 0;

  assert (res - prep_buf_res < MAX_NAME_SIZE);
  return prep_buf_res;
}

char *prepare_watchcat_str (char *x, int uni) {
  if (strlen (x) >= MAX_NAME_SIZE) {
    return NULL;
  }

//  fprintf (stderr, "prepare\n%s\n", x);

  char delim = uni ? ' ' : '+';

  char *s = prep_buf;
  /* copypaste from search-data.c */
  int no_nw = 1;
  const char *prev = 0;
  while (*x) {
    if (x == prev) {
      fprintf (stderr, "error at %.30s\n", x);
      exit (2);
    }
    prev = x - no_nw;
    int wl = no_nw ? 0 : get_notword (x);
    no_nw = 0;
    if (wl < 0) {
      break;
    }
    while (wl > 0 && *x != 0x1f) {
      x++;
      wl--;
    }
    if (*x == 0x1f) {
      wl = 1;
      while ((unsigned char) x[wl] >= 0x40) {
        wl++;
      }
      no_nw = 1;
    } else {
      wl = get_word (x);
    }
    if (!wl) {
      continue;
    }
    assert (wl > 0 && wl < 511);
    if (*x != 0x1f) {//TODO why not s += my_lc_str (s, x, wl);
      int len = my_lc_str (prep_buf_res, x, wl);
      memcpy (s, prep_buf_res, len);
      s += len;
    } else {
      memcpy (s, x, wl);
      s += wl;
    }
    *s++ = delim;

    x += wl;
  }
  *s = 0;

//  memcpy (prep_buf, x, strlen (x) + 1);

  char *v = uni ? prepare_str_cp1251 (prep_buf) : prep_buf;

//  fprintf (stderr, "%s\n--------------------------\n", v);
  return v;
}

/***
  Query
 ***/
int searchy_phrases_length (searchy_query_phrase_t *cur) {
  int res = 0;
  while (cur != NULL) {
    res++;
    cur = cur->next;
  }
  return res;
}

watchcat_query_phrase_t *searchy_phrases_dump (watchcat_query_phrase_t *dest, searchy_query_phrase_t *cur,
  int minus_flag, hash_t *dest_words_buf, hash_t *src_words_buf) {
  while (cur != NULL) {
    dest->H = dest_words_buf + (cur->H - src_words_buf);
    dest->words = cur->words;
    dest->type = cur->type;
    dest->minus_flag = minus_flag;
    dest++;
    cur = cur->next;
  }
  return dest;
}

inline int cmp_watchcat_query_phrase (const void *x_, const void *y_) {
  const watchcat_query_phrase_t *x = x_;
  const watchcat_query_phrase_t *y = y_;
  if (x->H[0] != y->H[0]) {
    return x->H[0] < y->H[0] ? -1 : +1;
  }
  if (x->words != y->words) {
    return x->words - y->words;
  }
  if (x->minus_flag != y->minus_flag) {
    return x->minus_flag - y->minus_flag;
  }
  return memcmp (x->H, y->H, sizeof (hash_t) * x->words);
}

watchcat_query_t *create_watchcat_query (char *s) {
  static searchy_query_t Q;
  searchy_query_parse (&Q, s);

  searchy_query_t *from = &Q;
  watchcat_query_t *to = dl_malloc0 (sizeof (watchcat_query_t));
  to->words = from->words;
  size_t words_size = sizeof (hash_t) * to->words;
  to->words_buf = dl_malloc (words_size);
  memcpy (to->words_buf, from->words_buf, words_size);

  int phrases_cnt_0 = searchy_phrases_length (from->phrases[0]);
  int phrases_cnt_1 = searchy_phrases_length (from->phrases[1]);
  to->phrases_cnt = phrases_cnt_0 + phrases_cnt_1;

  to->phrases = dl_malloc (sizeof (watchcat_query_phrase_t) * to->phrases_cnt);
  watchcat_query_phrase_t *phrase_ptr = to->phrases;
  phrase_ptr = searchy_phrases_dump (
      phrase_ptr, from->phrases[0], 0, to->words_buf, from->words_buf
  );
  phrase_ptr = searchy_phrases_dump (
      phrase_ptr, from->phrases[1], 1, to->words_buf, from->words_buf
  );
  assert (phrase_ptr == to->phrases + to->phrases_cnt);

  qsort (to->phrases, to->phrases_cnt, sizeof (watchcat_query_phrase_t), cmp_watchcat_query_phrase);

  searchy_query_free (&Q);
  return to;
}


long long watchcat_query_hash_impl (watchcat_query_t *query) {
  long long query_hash = 0x1234123412341234LL;
  int was_plus = 0;
  int i;
  for (i = 0; i < query->phrases_cnt; i++) {
    watchcat_query_phrase_t *phrase = &query->phrases[i];
    query_hash *= 0x4321432143214321LL;
    long long phrase_hash = 0x1234567812345678LL;
    if (phrase->minus_flag) {
      phrase_hash++;
    } else {
      was_plus = 1;
    }
    phrase_hash += phrase->minus_flag;
    int j;
    for (j = 0; j < phrase->words; j++) {
      phrase_hash *= 0x8765432187654321LL;
      phrase_hash += phrase->H[j];
    }

    query_hash += phrase_hash;
  }

  if (!was_plus) {
    //bad query
    /*printf ("Bad query\n");*/
    return -1;
  }

  return query_hash;
}

long long watchcat_query_hash (char *s) {
  watchcat_query_t *query = create_watchcat_query (s);
  long long query_hash = watchcat_query_hash_impl (query);
  free_watchcat_query (query);
  return query_hash;
}

void free_watchcat_query (watchcat_query_t *to) {
  dl_free (to->phrases, sizeof (watchcat_query_phrase_t) * to->phrases_cnt);
  dl_free (to->words_buf, sizeof (hash_t) * to->words);
  dl_free (to, sizeof (watchcat_query_t));
}

/*** Entry ***/
inline int cmp_spwp_by_hash (const void *x_, const void *y_) {
  const searchy_pair_word_position_t *x = x_;
  const searchy_pair_word_position_t *y = y_;

  if (x->word < y->word) {
    return -1;
  }
  if (x->word > y->word) {
    return +1;
  }
  return 0;
}

void watchcat_prepare_entry (watchcat_entry_t *entry, const char *text, int len) {
  int positions = 0;
  int n = searchy_extract_words (text, len, entry->by_pos, MAX_WATCHCAT_ENTRY_SIZE, 0, 0, 0, &positions);
  entry->n = n;
  memcpy (&entry->by_hash, &entry->by_pos, sizeof (searchy_pair_word_position_t) * n);

  int i;
  for (i = 0; i < n; i++) {
    entry->by_hash[i].position = i;
  }

  qsort (&entry->by_hash, n, sizeof (searchy_pair_word_position_t), cmp_spwp_by_hash);
}

inline int check_ (searchy_pair_word_position_t *begin, searchy_pair_word_position_t *end, hash_t *needle_begin, hash_t *needle_end) {
  while (begin != end && needle_begin != needle_end) {
    int ok = 0;
    searchy_pair_word_position_t *cur = begin;
    do {
      if (cur->word == needle_begin[0]) {
        ok = 1;
      }
      cur++;
    } while (cur != end && cur->position == begin->position);
    if (!ok) {
      return 0;
    }
    begin = cur;
    needle_begin++;
  }
  return needle_end == needle_begin;
}

int check_watchcat_query (watchcat_entry_t *entry, watchcat_query_t *query) {
  int entry_i = 0, query_i = 0;

  for (query_i = 0; query_i < query->phrases_cnt; query_i++) {
    watchcat_query_phrase_t *phrase = &query->phrases[query_i];
    hash_t cur_hash = phrase->H[0];
    while (entry_i < entry->n && entry->by_hash[entry_i].word < cur_hash) {
      entry_i++;
    }

    int i;
    int ok = 0;
    for (i = entry_i; i < entry->n && entry->by_hash[i].word == cur_hash && !ok; i++) {
       int by_pos_i = entry->by_hash[i].position;
       ok = check_ (entry->by_pos + by_pos_i, entry->by_pos + entry->n, phrase->H, phrase->H + phrase->words);
    }

    if (ok ^ (phrase->minus_flag == 0)) {
      return 0;
    }
  }

  return 1;
}

