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

#define	_FILE_OFFSET_BITS	64

#include "utils.h"

extern size_t static_memory;

long get_memory_used (void) {
  return dl_get_memory_used() - static_memory;
}

#ifdef HINTS

char prep_buf[MAX_NAME_SIZE];
int prep_ibuf[MAX_NAME_SIZE];
int prep_ibuf_res[MAX_NAME_SIZE];
int *words_ibuf[MAX_NAME_SIZE];

long long *gen_hashes (char *s) {
  static long long h[MAX_NAME_SIZE + 1];

  if (strlen (s) >= MAX_NAME_SIZE / 2 - 1) {
    h[0] = 0;
    return h;
  }

  string_to_utf8 ((unsigned char *)s, prep_ibuf);

  const int yo = 1105, e = 1077, kl = 160;

  int in_kludge = 0;
  int hn = 0;
  long long yo_h = 0;
  long long cur_h = 0;

  int *v;
  for (v = prep_ibuf; *v; v++) {
    if (cur_h == 0 && *v == kl) {
      in_kludge = 1;
    }

    if (*v == '+') {
      if (in_kludge) {
        h[hn++] = cur_h;
        in_kludge = 0;
      }
      cur_h = 0;
      yo_h = 0;
    } else {
      cur_h = cur_h * HASH_MUL + *v;

      if (*v == yo) {
        yo_h = yo_h * HASH_MUL + e;
      } else {
        yo_h = yo_h * HASH_MUL + *v;
      }
    }

    if (!in_kludge) {
      if (cur_h) {
        h[hn++] = cur_h;
      }

      if (yo_h != cur_h && yo_h) {
        h[hn++] = yo_h;
      }
    }
  }

  assert (in_kludge == 0);
  assert (yo_h == 0);
  assert (cur_h == 0);
  h[hn++] = 0;
  assert (hn < MAX_NAME_SIZE);
  return h;
}

inline int stricmp_void (const void *x, const void *y) {
  const int *s1 = *(const int **)x;
  const int *s2 = *(const int **)y;
  while (*s1 == *s2 && *s1 != ' ')
    s1++, s2++;
  return *s1 - *s2;
}

inline int ispref (const int *s1, const int *s2) {
  while (*s1 == *s2 && *s1 != ' ')
    s1++, s2++;
  return *s1 == ' ';
}

int *prepare_str_UTF8 (int *x) {
  int *v = prep_ibuf;

  int i, n;
  for (i = 0; x[i]; i++) {
    v[i] = convert_prep (x[i]);
  }

  int j = 0;
//  yo, jo -> e
/*  for (i = 0; v[i]; i++) {
    if ((v[i] == 'y' || v[i] == 'j') && v[i + 1] == 'o') {
      v[j++] = 'e'; i++;
    } else {
      v[j++] = v[i];
    }
  }
  v[j] = 0;
  i = j;
  j = 0;*/

  n = i;
  for (i = 0; v[i] == ' '; i++) {
  }

  int k = 0;
  while (i < n) {
    words_ibuf[k++] = v + i;
    while (v[i] && v[i] != ' ') {
      i++;
    }
    while (v[i] == ' ') {
      i++;
    }
  }
  v[n] = ' ';

  j = 0;
  qsort (words_ibuf, k, sizeof (int *), stricmp_void);

  for (i = 0; i < k; i++) {
    if (i == 0 || !ispref (words_ibuf[j - 1], words_ibuf[i])) {
      words_ibuf[j++] = words_ibuf[i];
    } else {
      words_ibuf[j - 1] = words_ibuf[i];
    }
  }
  k = j;

  int *res = prep_ibuf_res;
  for (i = 0; i < k; i++) {
    int *tmp = words_ibuf[i];
    while (*tmp != ' ') {
      *res++ = *tmp++;
    }
    *res++ = '+';
  }
  *res++ = 0;

  assert (res - prep_ibuf_res < MAX_NAME_SIZE);
  return prep_ibuf_res;
}

char *prepare_str (char *x) {
  if (strlen (x) >= MAX_NAME_SIZE / 4) {
    return NULL;
  }

  string_to_utf8 ((unsigned char *)x, prep_ibuf);
  int *v = prepare_str_UTF8 (prep_ibuf);
  char *s = prep_buf;

  while (*v != 0) {
    s += put_char_utf8 (*v++, s);
  }
  *s++ = 0;

  assert (s - prep_buf < MAX_NAME_SIZE);

  char *res = dl_malloc (s - prep_buf);
  if (res == NULL) {
    return res;
  }

  memcpy (res, prep_buf, s - prep_buf);
  return res;
}

char *clean_str (char *x) {
  if (strlen (x) >= MAX_NAME_SIZE) {
    return x;
  }

  char *s = prep_buf;
  int skip;

  while (*x != 0) {
    skip = !strncmp (x, "amp+", 4) ||
           !strncmp (x, "gt+", 3) ||
           !strncmp (x, "lt+", 3) ||
           !strncmp (x, "quot+", 5) ||
           !strncmp (x, "ft+", 3) ||
           !strncmp (x, "feat+", 5) ||
           (((x[0] == '1' && x[1] == '9') || (x[0] == '2' && x[1] == '0')) && ('0' <= x[2] && x[2] <= '9') && ('0' <= x[3] && x[3] <= '9') && x[4] == '+') ||
           !strncmp (x, "092+", 4) ||
           !strncmp (x, "33+", 3) ||
           !strncmp (x, "34+", 3) ||
           !strncmp (x, "36+", 3) ||
           !strncmp (x, "39+", 3) ||
           !strncmp (x, "60+", 3) ||
           !strncmp (x, "62+", 3) ||
           !strncmp (x, "8232+", 5) ||
           !strncmp (x, "8233+", 5);
    do {
      *s = *x;
      if (!skip) {
        s++;
      }
    } while (*x++ != '+');
  }
  *s = 0;

  return prep_buf;
}

int is_letter (int x) {
  return ('a' <= x && x <='z') || ('A' <= x && x <= 'Z') || x < 0 || ('0' <= x  && x <= '9');
}

#define ADD_CHAR(x) { s[j++]=x; if (j >= MAX_NAME_SIZE) return NULL; }

char *prepare_str_old (char *x) {
  char *s = prep_buf;
  int i=0, j=0;

  while (x[i] && !is_letter (x[i])) {
    i++;
  }

  while (x[i]) {
    while (is_letter (x[i])) {
      ADD_CHAR(x[i++]);
    }
    while (x[i] && !is_letter (x[i])) {
      i++;
    }
    if (!x[i])
    {
      ADD_CHAR('+');
      break;
    }
    ADD_CHAR('+');
  }

  ADD_CHAR(0);

  char *res = dl_malloc (j);
  if (res == NULL) {
    return res;
  }
  memcpy (res, prep_buf, j);

  return res;
}
#undef ADD_CHAR

void vct_init (vector *v) {
  v->mem = dl_malloc (sizeof (int));
  v->mx = 1, v->n = 0;
}

void vct_add (vector *v, int x) {
  if (v->mx == v->n) {
    v->mem = dl_realloc (v->mem, sizeof (int) * v->mx * 2, sizeof (int) * v->mx);
    v->mx *= 2;
  }
  v->mem[v->n++] = x;
}

void vct_free (vector *v) {
  dl_free (v->mem, sizeof (int) * v->mx);
  v->mx = 0, v->n = 0;
}

int vct_back (vector *v) {
  if (v->n) {
    return v->mem[v->n - 1];
  }
  return 0;
}

void vct_set_add (vector *v, int val) {
  if (vct_back (v) != val) {
    vct_add (v, val);
  }
}

#endif

