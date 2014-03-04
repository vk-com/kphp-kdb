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

    Copyright 2012-2013 Vkontakte Ltd
              2012-2013 Anton Maydell
*/

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include "tl-utils.h"
#include "kdb-data-common.h"

#define MAX_ZMALLOC 16384

void *tl_zzmalloc (int size) {
  if (size <= MAX_ZMALLOC) {
    return zmalloc (size);
  } else {
    void *p = malloc (size);
    assert (p);
    return p;
  }
}

void *tl_zzmalloc0 (int size) {
  void *r = tl_zzmalloc (size);
  memset (r, 0, size);
  return r;
}

void tl_zzfree (void *src, int size) {
  if (!src) { return; }
  if (size <= MAX_ZMALLOC) {
    zfree (src, size);
  } else {
    free (src);
  }
}

/********************* cstr routines *********************/
char *cstr_dup (const char *const input) {
  int l = strlen (input);
  char *p = tl_zzmalloc (l + 1);
  strcpy (p, input);
  return p;
};

char *cstr_substr (const char *const input, int start, int end) {
  int l = end - start;
  char *s = tl_zzmalloc (l + 1);
  memcpy (s, input + start, l);
  s[l] = 0;
  return s;
}

void cstr_free (char **s) {
  char *p = *s;
  if (p) {
    tl_zzfree (p, strlen (p) + 1);
    *s = NULL;
  }
}

/* NOTICE: buf shouldn't be allocated by zmalloc */
/* removes double, leading, trailing spaces */
void cstr_remove_extra_spaces (char *buf) {
  char *s, *w, last;
  for (s = buf, w = buf, last = ' '; *s; s++) {
    if (*s == ' ' && last == ' ') {
      continue;
    }
    last = *w++ = *s;
  }
  /* remove trailing spaces */
  *w-- = 0;
  while (w >= buf && *w == ' ') {
    *w-- = 0;
  }
}

void tl_string_buffer_init (struct tl_buffer *b) {
  memset (b, 0, sizeof (*b));
}

static void tl_string_buffer_extend (struct tl_buffer *b) {
  if (verbosity >= 4) {
    fprintf (stderr, "tl_string_buffer_extend: b->size (%d)\n", b->size);
  }
  if (b->size == 0) {
    b->size = TL_STRING_BUFFER_MIN_SIZE;
    b->buff = malloc (b->size);
    b->pos = 0;
    return;
  }
  b->size *= 2;
  b->buff = realloc (b->buff, b->size);
  assert (b->buff);
}

void tl_string_buffer_append_char (struct tl_buffer *b, char ch) {
  if (b->pos == b->size) {
    tl_string_buffer_extend (b);
  }
  b->buff[b->pos++] = ch;
}

void tl_string_buffer_append_cstr (struct tl_buffer *b, const char *s) {
  while (*s) {
    tl_string_buffer_append_char (b, *s);
    s++;
  }
}

void tl_string_buffer_clear (struct tl_buffer *b) {
  b->pos = 0;
}

void tl_string_buffer_free (struct tl_buffer *b) {
  if (b->buff) {
    free (b->buff);
  }
  memset (b, 0, sizeof (*b));
}

char *tl_string_buffer_to_cstr (struct tl_buffer *b) {
  if (b->size == 0) {
    return cstr_dup ("");
  }
  char *s = tl_zzmalloc (b->pos+1);
  memcpy (s, b->buff, b->pos);
  s[b->pos] = 0;
  return s;
}

void tl_string_buffer_printf (struct tl_buffer *b, const char *format, ...) {
  int o = b->size - b->pos;
  if (o < 16) {
    tl_string_buffer_extend (b);
    o = b->size - b->pos;
  }
  for (;;) {
    va_list ap;
    va_start (ap, format);
    int l = vsnprintf (b->buff + b->pos, o, format, ap);
    va_end (ap);
    if (l <= o) {
      b->pos += l;
      return;
    }
    tl_string_buffer_extend (b);
    o = b->size - b->pos;
  }
}

void tl_string_buffer_vprintf (struct tl_buffer *b, const char *format, va_list ap) {
  int o = b->size - b->pos;
  if (o < 16) {
    tl_string_buffer_extend (b);
    o = b->size - b->pos;
  }
  for (;;) {
    va_list aq;
    va_copy (aq, ap);
    int l = vsnprintf (b->buff + b->pos, o, format, aq);
    va_end (aq);
    if (l <= o) {
      b->pos += l;
      return;
    }
    tl_string_buffer_extend (b);
    o = b->size - b->pos;
  }
}

/*
void tl_bclear (struct tl_buffer *b, char *buff, int size) {
  b->buff = buff;
  b->size = size;
  b->pos = 0;
}

void tl_bprintf (struct tl_buffer *b, const char *format, ...) {
  if (b->pos >= b->size) { return; }
  va_list ap;
  va_start (ap, format);
  b->pos += vsnprintf (b->buff + b->pos, b->size - b->pos, format, ap);
  va_end (ap);
}
*/

void tl_int_array_init (struct tl_int_array *a, int *buff, int size) {
  a->buff = buff;
  a->size = size;
  a->pos = 0;
}

int tl_int_array_append (struct tl_int_array *a, int i) {
  if (a->pos >= a->size) {
    return -1;
  }
  a->buff[(a->pos)++] = i;
  return 0;
}

int tl_int_array_append_long (struct tl_int_array *a, long long l) {
  if (a->pos >= a->size - 1) {
    return -1;
  }
  *((long long *) (&a->buff[a->pos])) = l;
  a->pos += 2;
  return 0;
}

int tl_int_array_append_double (struct tl_int_array *a, double d) {
  if (a->pos >= a->size - 1) {
    return -1;
  }
  *((double *) (&a->buff[a->pos])) = d;
  a->pos += 2;
  return 0;
}

int tl_int_array_append_string (struct tl_int_array *a, char *s) {
  int len = strlen (s);
  if (len >= 0x1000000) {
    return -1;
  }
  int l = len + ((len < 0xfe) ? 1 : 4);
  l = (l + 3) >> 2;
  if (a->pos + l > a->size) {
    return -1;
  }
  char *dest = (char *) &a->buff[a->pos];
  if (len < 0xfe) {
    *dest++ = len;
  } else {
    a->buff[a->pos] = (len << 8) + 0xfe;
    dest += 4;
  }
  memcpy (dest, s, len);
  dest += len;
  while ((long) dest & 3) {
    *dest++ = 0;
  }
  a->pos += l;
  assert ((void *) dest == (void *) &a->buff[a->pos]);
  return 0;
}

int tl_int_array_mark (struct tl_int_array *a) {
  return a->pos;
}

void tl_int_array_release (struct tl_int_array *a, int mark) {
  a->pos = mark;
}

int tl_fetch_string (int *in_ptr, int ilen, char **s, int *slen, int allocate_new_cstr) {
  *s = NULL;
  int *in_end = in_ptr + ilen;
  if (in_ptr >= in_end) {
    return -1;
  }
  unsigned l = *in_ptr;
  if ((l & 0xff) < 0xfe) {
    l &= 0xff;
    if (slen) {
      *slen = l;
    }
    if (in_end >= in_ptr + (l >> 2) + 1) {
      char *src = ((char *) in_ptr) + 1;
      if (allocate_new_cstr) {
        *s = tl_zzmalloc (l + 1);
        memcpy (*s, src, l);
        (*s)[l] = 0;
      } else {
        *s = src;
      }
      return (l >> 2) + 1;
    } else {
      return -1;
    }
  } else if ((l & 0xff) == 0xfe) {
    l >>= 8;
    if (slen) {
      *slen = l;
    }
    if (l >= 0xfe && in_end >= in_ptr + ((l + 7) >> 2)) {
      char *src = (char *) &in_ptr[1];
      if (allocate_new_cstr) {
        *s = tl_zzmalloc (l + 1);
        memcpy (*s, src, l);
        (*s)[l] = 0;
      } else {
        *s = src;
      }
      return (l + 7) >> 2;
    } else {
      return -1;
    }
  } else {
    return -1;
  }
}

static int get_hashtable_size (int n) {
  static const int p[] = {
  //23,29,37,41,47,53,59,67,79,89,101,113,127,149,167,191,211,233,257,283,313,347,383,431,479,541,599,659,727,809,907,
  1103,1217,1361,1499,1657,1823,2011,2213,2437,2683,2953,3251,3581,3943,4339,
  4783,5273,5801,6389,7039,7753,8537,9391,10331,11369,12511,13763,15149,16673,18341,20177,22229,
  24469,26921,29629,32603,35869,39461,43411,47777,52561,57829,63617,69991,76991,84691,93169,102497,
  112757,124067,136481,150131,165161,181693,199873,219871,241861,266051,292661,321947,354143, 389561, 428531,
  471389,518533,570389,627433,690187,759223,835207,918733,1010617,1111687,1222889,1345207,
  1479733,1627723,1790501,1969567,2166529,2383219,2621551,2883733,3172123,3489347,3838283,4222117,
  4644329,5108767,5619667,6181639,6799811,7479803,8227787,9050599,9955697,10951273,12046403,13251047};
  /*
  14576161,16033799,17637203,19400929,21341053,23475161,25822679,28404989,31245491,34370053,37807061,
  41587807,45746593,50321261,55353391,60888739,66977621,73675391,81042947,89147249,98061979,107868203,
  118655027,130520531,143572609,157929907,173722907,191095213,210204763,231225257,254347801,279782593,
  307760897,338536987,372390691,409629809,450592801,495652109,545217341,599739083,659713007,725684317,
  798252779,878078057,965885863,1062474559};
  */
  const int lp = sizeof (p) / sizeof (p[0]);
  int a = -1;
  int b = lp;
  n += n >> 1;
  while (b - a > 1) {
    int c = ((a + b) >> 1);
    if (p[c] <= n) { a = c; } else { b = c; }
  }
  if (a < 0) { a++; }
  assert (a < lp-1);
  return p[a];
}

struct tl_hashmap *tl_hashmap_alloc (int n) {
  assert (n >= TL_MIN_HASHMAP_SIZE);
  struct tl_hashmap *H = zmalloc (sizeof (struct tl_hashmap));
  H->size = get_hashtable_size (n);
  H->filled = 0;
  H->n = n;
  H->h = tl_zzmalloc0 (H->size * sizeof (void *));
  return H;
}

void tl_hashmap_free (struct tl_hashmap **V) {
  struct tl_hashmap *H = *V;
  if (H == NULL) {
    return;
  }
  tl_zzfree (H->h, (H->size * sizeof (void *)));
  zfree (H, sizeof (struct tl_hashmap));
  *V = NULL;
}

static void tl_hashmap_extend (struct tl_hashmap **V) {
  if (verbosity >= 4) {
    fprintf (stderr, "tl_hashmap_extend: old hash size is %d.\n", (*V)->size);
  }
  int i;
  struct tl_hashmap *H = tl_hashmap_alloc ((*V)->n * 2);
  H->compare = (*V)->compare;
  H->compute_hash = (*V)->compute_hash;
  for (i = 0; i < (*V)->size; i++) {
    if ((*V)->h[i] != NULL) {
      tl_hashmap_get_f (&H, (*V)->h[i], 1);
    }
  }
  tl_hashmap_free (V);
  *V = H;
}

void *tl_hashmap_get_f (struct tl_hashmap **V, void *p, int force) {
  assert (force >= 0);
  int h1, h2;
  struct tl_hashmap *H = *V;
  H->compute_hash (H, p, &h1, &h2);
  void *D;
  while ((D = H->h[h1]) != NULL) {
    if (!H->compare (D, p)) {
      return D;
    }
    h1 += h2;
    if (h1 >= H->size) { h1 -= H->size; }
  }
  if (!force) {
    return NULL;
  }
  if (H->filled == H->n) {
    tl_hashmap_extend (V);
    return tl_hashmap_get_f (V, p, force);
  }
  H->filled++;
  H->h[h1] = p;
  return p;
}
