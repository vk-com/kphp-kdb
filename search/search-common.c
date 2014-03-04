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

#include <string.h>
#include <stdlib.h>
#include <assert.h>
#include "search-common.h"
#include "server-functions.h"
#include "word-split.h"
#include "stemmer-new.h"

unsigned long long searchy_word_hash (const char *str, int len) {
  unsigned long long h = word_hash (str, len);
  h &= 0x7fffffffffffffffULL;
  if (*str != 0x1f) {
    h |= 0x8000000000000000ULL;
  }
  return h;
}

static void qsort_q (pair_word_freqs_t *Q, int a, int b) {
  int i, j;
  hash_t h, t;
  int s;
  if (a >= b) { return; }
  h = Q[(a+b)>>1].word;
  i = a;
  j = b;
  do {
    while (Q[i].word < h) { i++; }
    while (Q[j].word > h) { j--; }
    if (i <= j) {
      t = Q[i].word; Q[i].word = Q[j].word; Q[j].word = t;
      s = Q[i].freqs; Q[i].freqs = Q[j].freqs; Q[j].freqs = s;
      i++; j--;
    }
  } while (i <= j);
  qsort_q (Q, a, j);
  qsort_q (Q, i, b);
}

unsigned evaluate_uniq_words_count (pair_word_freqs_t *Q, int n) {
  int i;
  unsigned r = 0;
  for (i = 0; i < n; i++) {
    if (Q[i].freqs >= 0x10000) {
      r += 0x10000;
    }
    if (Q[i].freqs) {
      r++;
    }
  }
  return r;
}

void evaluate_freq_sqr_sums (pair_word_freqs_t *Q, int n, unsigned *sum_sqr_freq_title, unsigned *sum_freq_title_freq_text, unsigned *sum_sqr_freq_text) {
  int i;
  *sum_sqr_freq_title = *sum_freq_title_freq_text = *sum_sqr_freq_text = 0;
  for (i = 0; i < n; i++) {
    unsigned freq_title = Q[i].freqs >> 16, freq_text = Q[i].freqs & 0xffff;
    *sum_sqr_freq_title += freq_title * freq_title;
    *sum_freq_title_freq_text += freq_title * freq_text;
    *sum_sqr_freq_text += freq_text * freq_text;
  }
}

static unsigned long long make_tag (char *tag_name, int tag_name_len, unsigned int value) {
  assert (tag_name_len <= 16);
  char s[32];
  int i = 1;
  s[0] = 0x1f;
  memcpy (s + 1, tag_name, tag_name_len);
  i += tag_name_len;
  while (value >= 0x40) {
    s[i++] = (unsigned char) ((value & 0x7f) + 0x80);
    value >>= 7;
  }
  s[i++] = (unsigned char) ((value & 0x3f) + 0x40);
  return word_hash (s, i);
}

int extract_words (const char *text, int len, int universal, pair_word_freqs_t *Q, int max_words, int tag_owner, long long item_id) {
  static char buff[512];
  int i, no_nw = 1;
  const char *prev = 0;
  int Qw = 0;
  unsigned field = 0x10000;

  if (universal) {
    Q[Qw].word = word_hash ("\x1f@@", 3);
    Q[Qw].freqs = 0;
    Qw++;
  }

  if (tag_owner && ((int) (item_id >> 32))) {
    int owner_id = (int) item_id;
    if (owner_id) {
      Q[Qw].word = owner_id > 0 ? make_tag ("O", 1, owner_id) : make_tag ("W", 1, -owner_id);
      Q[Qw].freqs = 0;
      Qw++;
    }
  }

  while (Qw < max_words && *text) {
    if (text == prev) {
      kprintf ("error at %.30s\n", text);
      exit (2);
    }
    prev = text - no_nw;
    int wl = no_nw ? 0 : get_notword (text);
    no_nw = 0;
    if (wl < 0) {
      break;
    }
    while (wl > 0 && *text != 0x1f) {
      if (*text == '\t') {
        field = 1;
      }
      text++;
      wl--;
    }
    if (*text == 0x1f) {
      wl = 1;
      while ((unsigned char) text[wl] >= 0x40) {
        wl++;
      }
      no_nw = 1;
    } else {
      wl = get_word (text);
    }
    if (!wl) {
      continue;
    }
    assert (wl > 0 && wl < 511);
    if (*text == 0x1f) {
      Q[Qw].word = word_hash (text, wl);
      Q[Qw].freqs = 0; /* we don't count tags */
      Qw++;
    } else {
      int wl2 = my_lc_str (buff, text, wl);
      assert (wl2 <= 510);
      Q[Qw].word = word_hash (buff, wl2);
      Q[Qw].freqs = field;
      Qw++;
    }
    text += wl;
  }

  int t = 0;
  if (Qw > 0) {
    qsort_q (Q, 0, Qw - 1);
    int k;
    for (i = 0; i < Qw; i = k) {
      unsigned freqs = Q[i].freqs;
      for (k = i + 1; k < Qw && Q[k].word == Q[i].word; k++) {
        freqs += Q[k].freqs;
      }
      Q[t].word = Q[i].word;
      Q[t].freqs = freqs;
      //vkprintf (3, "Q[%d].word = %llx, Q[%d].freqs = %u\n", t, Q[t].word, t, Q[t].freqs);
      t++;
    }
  }
  return t;
}

hash_t searchy_term_hash (char *text, int wl, int stem) {
  int wl2;
  char buff[512];
  if (stem && use_stemmer) {
    wl2 = my_lc_str (buff, text, wl);
    assert (wl2 <= 510);
    return searchy_word_hash (buff, wl2);
  }
  const int old_use_stemmer = use_stemmer;
  char *wptr = (char *) text + wl;
  const char old_char = *wptr;
  *wptr = '+';
  use_stemmer = 0;
  wl2 = my_lc_str (buff, text, wl + 1);
  *wptr = old_char;
  assert (wl2 <= 510);
  use_stemmer = old_use_stemmer;
  return searchy_word_hash (buff, wl2);
}

int searchy_extract_words (const char *text, int len, int universal, pair_word_freqs_t *Q, int max_words, int tag_owner, long long item_id, int *positions) {
  int no_nw = 1;
  const char *prev = 0;
  int Qw = 0;
  *positions = 0;

  if (universal) {
    Q[Qw].word = searchy_word_hash ("\x1f@@", 3);
    Q[Qw].freqs = 0;
    Qw++;
  }

  if (tag_owner && ((int) (item_id >> 32))) {
    int owner_id = (int) item_id;
    if (owner_id) {
      Q[Qw].word = owner_id > 0 ? make_tag ("O", 1, owner_id) : make_tag ("W", 1, -owner_id);
      Q[Qw].word &= 0x7fffffffffffffffULL;
      Q[Qw].freqs = 0;
      Qw++;
    }
  }

  while (Qw < max_words && *text) {
    if (text == prev) {
      kprintf ("error at %.30s\n", text);
      exit (2);
    }
    prev = text - no_nw;
    int wl = no_nw ? 0 : get_notword (text);
    no_nw = 0;
    if (wl < 0) {
      break;
    }
    while (wl > 0 && *text != 0x1f) {
      text++;
      wl--;
    }
    if (*text == 0x1f) {
      wl = 1;
      while ((unsigned char) text[wl] >= 0x40) {
        wl++;
      }
      no_nw = 1;
    } else {
      wl = get_word (text);
    }
    if (!wl) {
      continue;
    }
    assert (wl > 0 && wl < 511);
    if (*text == 0x1f) {
      Q[Qw].word = searchy_word_hash (text, wl);
      Q[Qw++].freqs = 0; /* we don't count tags */
    } else {
      (*positions)++;
      Q[Qw].word = searchy_term_hash ((char *) text, wl, 0);
      Q[Qw++].freqs = *positions;
      if (!no_nw && Qw < max_words) {
        Q[Qw].word = searchy_term_hash ((char *) text, wl, 1);
        if (Q[Qw].word != Q[Qw-1].word) {
          Q[Qw++].freqs = *positions;
        }
      }
    }
    text += wl;
  }

  return Qw;
}
