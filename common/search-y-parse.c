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

    Copyright 2013 Vkontakte Ltd
              2013 Anton Maydell
*/

#define _FILE_OFFSET_BITS 64

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include "server-functions.h"
#include "search-y-parse.h"
#include "word-split.h"
#include "stemmer-new.h"

int searchy_is_term (hash_t word) {
  return (word & 0x8000000000000000ULL) ? 1 : 0;
}

unsigned long long searchy_word_hash (const char *str, int len) {
  unsigned long long h = word_hash (str, len);
  h &= 0x7fffffffffffffffULL;
  if (*str != 0x1f) {
    h |= 0x8000000000000000ULL;
  }
  return h;
}

static unsigned long long searchy_make_tag (char *tag_name, int tag_name_len, unsigned int value) {
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
  return searchy_word_hash (s, i);
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

int searchy_extract_words (const char *text, int len, searchy_pair_word_position_t *Q, int max_words, int universal, int tag_owner, long long item_id, int *positions) {
  int no_nw = 1;
  const char *prev = 0;
  int Qw = 0;
  *positions = 0;

  if (universal) {
    Q[Qw].word = searchy_word_hash ("\x1f@@", 3);
    Q[Qw].position = 0;
    Qw++;
  }

  if (tag_owner && ((int) (item_id >> 32))) {
    int owner_id = (int) item_id;
    if (owner_id) {
      Q[Qw].word = owner_id > 0 ? searchy_make_tag ("O", 1, owner_id) : searchy_make_tag ("W", 1, -owner_id);
      Q[Qw].position = 0;
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
      Q[Qw++].position = 0; /* we don't count tags */
    } else {
      (*positions)++;
      Q[Qw].word = searchy_term_hash ((char *) text, wl, 0);
      Q[Qw++].position = *positions;
      if (!no_nw && Qw < max_words) {
        Q[Qw].word = searchy_term_hash ((char *) text, wl, 1);
        if (Q[Qw].word != Q[Qw-1].word) {
          Q[Qw++].position = *positions;
        }
      }
    }
    text += wl;
  }

  return Qw;
}

static int get_quot_type (char ch) {
  switch (ch) {
    case '"': return sqpt_double_quotation; /* use stemmer */
    case '\'': return sqpt_single_quotation;
    default: return sqpt_no_quotation;
  }
}

static searchy_query_phrase_t *searchy_phrase_alloc (searchy_query_t *Q, enum searchy_query_phrase_type type, int minus) {
  assert (!(minus & -2));
  searchy_query_phrase_t *P = zmalloc (sizeof (*P));
  P->type = type;
  P->words = 0;
  P->next = Q->phrases[minus];
  Q->phrases[minus] = P;
  P->H = Q->last_phrase ? Q->last_phrase->H + Q->last_phrase->words : Q->words_buf;
  return Q->last_phrase = P;
}

static void searchy_query_contains_too_many_words (searchy_query_t *Q) {
  snprintf (Q->error, sizeof (Q->error), "query contains too many words, SEARCHY_MAX_QUERY_WORDS is %d.", SEARCHY_MAX_QUERY_WORDS);
}

static int searchy_add_single_word (searchy_query_t *Q, const char *text, int len, enum searchy_query_phrase_type type, int minus) {
  if (Q->words >= SEARCHY_MAX_QUERY_WORDS) {
    searchy_query_contains_too_many_words (Q);
    return -1;
  }
  searchy_query_phrase_t *P = searchy_phrase_alloc (Q, type, minus);
  P->H[P->words++] = searchy_word_hash (text, len);
  Q->words++;
  return 0;
}

static int searchy_parse_phrase0 (searchy_query_t *Q, char *text, enum searchy_query_phrase_type type, int minus) {
  vkprintf (3, "%s: text:\"%s\", type:%d, minus:%d\n", __func__, text, type, minus);
  if (type == sqpt_double_quotation && !use_stemmer) {
    snprintf (Q->error, sizeof (Q->error), "since engine was run without stemmer, double quotes search queries are disabled");
    return -1;
  }
  int no_nw = 1, phrase_words = 0;
  char buff[512];
  hash_t W[SEARCHY_MAX_QUERY_WORDS];
  while (*text == 0x1f || *text == '?') {
    int i = 1;
    buff[0] = 0x1f;
    text++;
    while ((unsigned char) *text >= 0x40 && i < 32) {
      buff[i++] = *text++;
    }
    if (searchy_add_single_word (Q, buff, i, sqpt_no_quotation, minus) < 0) {
      return -1;
    }
    while (*text == '+') {
      text++;
    }
  }

  while (*text) {
    int wl = no_nw ? 0 : get_notword (text);
    vkprintf (3, "no_nw: %d, text: %s, wl: %d\n", no_nw, text, wl);
    no_nw = 0;
    if (wl < 0) {
      break;
    }
    while (wl > 0 && (*text != 0x1f && *text != '?')) {
      text++;
      wl--;
    }
    if (*text == 0x1f || *text == '?') {
      wl = 1;
      while ((unsigned char) text[wl] >= 0x40) {
        wl++;
      }
      if (wl <= 32) {
        memcpy (buff, text, wl);
        buff[0] = 0x1f;
        vkprintf (3, "add_query_word (%.*s)\n", wl, buff);
        if (searchy_add_single_word (Q, buff, wl, sqpt_no_quotation, minus) < 0) {
          return -1;
        }
      }
      no_nw = 1;
      text += wl;
      continue;
    } else {
      wl = get_word (text);
      vkprintf (3, "get_word('%s') returns %d.\n", text, wl);
    }

    if (!wl) {
      continue;
    }
    assert (wl < 511);
    vkprintf (3, "add_query_word (%.*s)\n", wl, text);
    int stem = !no_nw && (type == sqpt_double_quotation || (type == sqpt_no_quotation && use_stemmer));
    if (phrase_words >= SEARCHY_MAX_QUERY_WORDS) {
      searchy_query_contains_too_many_words (Q);
      return -1;
    }
    W[phrase_words++] = searchy_term_hash (text, wl, stem);
    text += wl;
  }
  
  if (phrase_words > 0) {
    if (phrase_words + Q->words > SEARCHY_MAX_QUERY_WORDS) {
      searchy_query_contains_too_many_words (Q);
      return -1;
    }
    if (type == sqpt_no_quotation) {
      int i;
      for (i = 0; i < phrase_words; i++) {
        searchy_query_phrase_t *P = searchy_phrase_alloc (Q, type, 0);
        P->H[0] = W[i];
        P->words = 1;
      }
    } else {
      searchy_query_phrase_t *P = searchy_phrase_alloc (Q, type, minus);
      memcpy (P->H, W, phrase_words * sizeof (W[0]));
      P->words = phrase_words;
    }
    Q->words += phrase_words;
  }
  return 0;
}

static int searchy_parse_phrase (searchy_query_t *Q, char *text, int text_len, int quot_type, int minus) {
  char *wptr = text + text_len;
  const char old_char = *wptr;
  *wptr = 0;
  int res = searchy_parse_phrase0 (Q, text, quot_type, minus);
  *wptr = old_char;
  return res;
}

static int searchy_is_minus_phrase (char *query, char *ptr) {
  if (ptr == query) {
    return 0;
  }
  return (ptr[-1] == '-') ? 1 : 0;
}

int searchy_query_parse (searchy_query_t *Q, char *query) {
  vkprintf (3, "%s: %s\n", __func__, query);
  char *text;
  int l;
  Q->error[0] = 0;
  Q->phrases[0] = NULL;
  Q->phrases[1] = NULL;
  Q->words = 0;
  Q->last_phrase = NULL;
  for (text = query; *text; text += l) {
    if (get_quot_type (*text) != sqpt_no_quotation) {
      for (l = 1; text[l] && text[l] != *text; l++) {}
      if (!text[l]) {
        snprintf (Q->error, sizeof (Q->error), "unclosed quot at position %d", (int) (text - query));
        return -1;
      }
      if (searchy_parse_phrase (Q, text + 1, l - 1, get_quot_type (text[l]), searchy_is_minus_phrase (query, text)) < 0) {
        return -1;
      }
      l++;
    } else {
      for (l = 1; text[l] && get_quot_type (text[l]) == sqpt_no_quotation; l++) {}
      if (searchy_parse_phrase (Q, text, l, sqpt_no_quotation, searchy_is_minus_phrase (query, text)) < 0) {
        return -1;
      }
    }
  }
/*
  if (!Q->words) {
    snprintf (Q->error, sizeof (Q->error), "no query words");
    return -1;
  }
*/
  return 0;
}

void searchy_query_phrase_free (searchy_query_phrase_t *P) {
  while (P) {
    searchy_query_phrase_t *W = P->next;
    zfree (P, sizeof (*P));
    P = W;
  }
}

void searchy_query_free (searchy_query_t *Q) {
  searchy_query_phrase_free (Q->phrases[0]);
  searchy_query_phrase_free (Q->phrases[1]);
}
