/*
    This file is part of VK/KittenPHP-DB-Engine Library.

    VK/KittenPHP-DB-Engine Library is free software: you can redistribute it and/or modify
    it under the terms of the GNU Lesser General Public License as published by
    the Free Software Foundation, either version 2 of the License, or
    (at your option) any later version.

    VK/KittenPHP-DB-Engine Library is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Lesser General Public License for more details.

    You should have received a copy of the GNU Lesser General Public License
    along with VK/KittenPHP-DB-Engine Library.  If not, see <http://www.gnu.org/licenses/>.

    Copyright 2010-2013 Vkontakte Ltd
              2010-2013 Arseny Smirnov
              2010-2013 Aliaksei Levin
*/

#define _FILE_OFFSET_BITS 64

#include <assert.h>
#include <stdlib.h>
#include <time.h>

#include "dl-crypto.h"

const unsigned int N = 26 + 26 + 10 + 1;

inline char encode_char (char c) {
  if ('A' <= c && c <= 'Z') {
    return (char)(c - 'A');
  }
  if ('a' <= c && c <= 'z') {
    return (char)(c - 'a' + 26);
  }
  if ('0' <= c && c <= '9') {
    return (char)(c - '0' + 26 + 26);
  }
  if (c == '_') {
    return (char)(10 + 26 + 26);
  }
  assert (0);
}

inline char decode_char (char c) {
  assert (0 <= c && c < N);
  if (c < 26) {
    return (char)(c + 'A');
  }
  c = (char)(c - 26);
  if (c < 26) {
    return (char)(c + 'a');
  }
  c = (char)(c - 26);
  if (c < 10) {
    return (char)(c + '0');
  }
  return '_';
}

inline void encode_str (char *s, int sn) {
  int i;
  for (i = 0; i < sn; i++) {
    s[i] = encode_char (s[i]);
  }
}

inline void decode_str (char *s, int sn) {
  int i;
  for (i = 0; i < sn; i++) {
    s[i] = decode_char (s[i]);
  }
}

void apply_perm (char *s, int *perm, int n) {
  dbg ("In apply_perm s = %p, perm = %p, n = %d\n", s, perm, n);
  int i;
  for (i = 0; i < n; i++) {
    int j = perm[i];
    char t = s[i];
    s[i] = s[j];
    s[j] = t;
  }
  dbg ("After apply_perm s = %p, perm = %p, n = %d\n", s, perm, n);
}

void apply_perm_rev (char *s, int *perm, int n) {
  dbg ("In apply_perm_rev s = %p, perm = %p, n = %d\n", s, perm, n);
  int i;
  for (i = n - 1; i >= 0; i--) {
    int j = perm[i];
    char t = s[i];
    s[i] = s[j];
    s[j] = t;
  }
  dbg ("After apply_perm_rev s = %p, perm = %p, n = %d\n", s, perm, n);
}

int *rand_perm (int n) {
  int *v = dl_malloc ((size_t)n * sizeof (int)), i;

  usleep (1);
  struct timespec tv;
  assert (clock_gettime (CLOCK_REALTIME, &tv) >= 0);
  srand ((unsigned int)tv.tv_nsec * 123456789u + (unsigned int)tv.tv_sec * 987654321u);

  for (i = 0; i < n; i++) {
    v[i] = rand() % (i + 1);
  }

  return v;
}

void dl_crypto_init (dl_crypto *cr, int val_n, int rand_n, int hash_st, int hash_mul, int seed) {
  assert ((N & 1) == 1);

  cr->val_n = val_n;
  cr->rand_n = rand_n;
  cr->hash_st = hash_st;
  cr->hash_mul = hash_mul;

  srand ((unsigned int)seed);
  int n = val_n + rand_n;
  cr->perm_first = rand_perm (val_n);
  cr->perm_middle = rand_perm (n);
  cr->perm_last = rand_perm (n);
}

void dl_crypto_encode (dl_crypto *cr, char *s, char *t) {
  encode_str (s, cr->val_n);
  apply_perm (s, cr->perm_first, cr->val_n);

  int i;
  unsigned int h = cr->hash_st;
  for (i = 0; i < cr->rand_n; i++) {
    t[i] = (char)((unsigned int)rand() % N);
    h = h * cr->hash_mul + t[i];
  }

  for (i = 0; i < cr->val_n; i++) {
    t[i + cr->rand_n] = (char)((s[i] + N - h % N) % N);
    h = h * cr->hash_mul + s[i];
  }

  int n = cr->val_n + cr->rand_n;

  apply_perm (t, cr->perm_middle, n);

  h = 0;
  for (i = 0; i < n; i++) {
    char c = t[i];
    t[i] = (char)((t[i] + N - h % N) % N);
    h += c;
  }
  
  apply_perm (t, cr->perm_last, n);
  decode_str (t, n);
}

void dl_crypto_decode (dl_crypto *cr, char *s, char *t) {
  int n = cr->val_n + cr->rand_n;
  encode_str (s, n);
  apply_perm_rev (s, cr->perm_last, n);

  int i;
  unsigned int h = 0;
  for (i = 0; i < n; i++) {
    s[i] = (char)((s[i] + h) % N);
    h += s[i];
  }

  apply_perm_rev (s, cr->perm_middle, n);

  h = cr->hash_st;
  for (i = 0; i < cr->rand_n; i++) {
    h = h * cr->hash_mul + s[i];
  }

  for (i = 0; i < cr->val_n; i++) {
    t[i] = (char)((s[i + cr->rand_n] + N + h % N) % N);
    h = h * cr->hash_mul + t[i];
  }

  apply_perm_rev (t, cr->perm_first, cr->val_n);
  decode_str (t, cr->val_n);
}

void dl_crypto_free (dl_crypto *cr) {
  dl_free (cr->perm_first, sizeof (int) * (size_t)cr->val_n);
  dl_free (cr->perm_middle, sizeof (int) * (size_t)(cr->val_n + cr->rand_n));
  dl_free (cr->perm_last, sizeof (int) * (size_t)(cr->val_n + cr->rand_n));
}
