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

#include "perfect-hashing.h"
#include <stdlib.h>
#include <assert.h>
#include <stdio.h>
#include <string.h>

#include "utils.h"

/*
typedef struct {
  char *code;
  int *sums;
  int *used;
  int mul0, mul1, d;
} perfect_hash;
*/

int ph_encode (perfect_hash *h, unsigned char *s) {
  unsigned char *st = s;

  WRITE_INT(s, h->d);
  WRITE_INT(s, h->mul0);
  WRITE_INT(s, h->mul1);

  memcpy (s, h->code, get_code_len (h->d));
  s += get_code_len (h->d);
  memcpy (s, h->used, get_used_len (h->d));
  s += get_used_len (h->d);
  memcpy (s, h->sums, get_sums_len (h->d));
  s += get_sums_len (h->d);

  return s - st;
}

int ph_decode (perfect_hash *h, unsigned char *s) {
  unsigned char *st = s;

  READ_INT(s, h->d);
  READ_INT(s, h->mul0);
  READ_INT(s, h->mul1);

  h->code = s;
  s += get_code_len (h->d);
  h->used = (int *)s;
  s += get_used_len (h->d);
  h->sums = (int *)s;
  s += get_sums_len (h->d);

  return s - st;
}

int bits_cnt_tbl[1 << 16];

void init_bits_cnt_table() {
  static int f = 0;
  if (f) {
    return;
  }
  f = 1;

  int i;
  bits_cnt_tbl[0] = 0;
  for (i = 1; i < (1 << 16); i++) {
    bits_cnt_tbl[i] = 1 + bits_cnt_tbl[i & (i - 1)];
  }
}

inline int bits_cnt (int x) {
  return bits_cnt_tbl[x & 0xffff] + bits_cnt_tbl[(x >> 16) & 0xffff];
}

inline int poly_h (unsigned long long v, unsigned int mul, unsigned int mod) {
  unsigned long long res = 0;
  int i;
  for (i = 0; i < 4; i++) {
    res *= mul;
    res ^= (v >> (16 * i)) & (0xffff);
  }
  //res = v ^ (v * mul);

  return (res >> 32) % mod;
}


void ph_init (perfect_hash *h) {
  init_bits_cnt_table();
  h->code = NULL;
  h->used = NULL;
  h->sums = NULL;
  h->d = 0;
  h->mul0 = 0;
  h->mul1 = 0;
}

void ph_free (perfect_hash *h) {
  dl_free (h->code, get_code_len (h->d));
  dl_free (h->used, get_used_len (h->d));
  dl_free (h->sums, get_sums_len (h->d));
  ph_init (h);
}

int *va, *ne, *st, *was, *di;

int dfs (int v, int p, int d) {
  was[v] = 1;
  di[v] = d;
  int i;
  for (i = st[v]; i != -1; i = ne[i]) {
    if (va[i] != p) {
      if (was[va[i]] || !dfs (va[i], v, d + 1)) {
        return 0;
      }
    }
  }
  return 1;
}

void ph_generate (perfect_hash *h, long long *s, int n) {
//  fprintf (stderr, "gen %d\n", n);

  assert (h->code == NULL);
  int d = n * (1 + 0.1);

  h->d = d;
  h->code = dl_malloc0 (get_code_len (d));
  h->used = dl_malloc0 (get_used_len (d));
  assert (sizeof (int) == 4);
  h->sums = dl_malloc0 (get_sums_len (d));

  int en = 2 * d, vn = d * 2;

  va = dl_malloc (sizeof (int) * en),
  ne = dl_malloc (sizeof (int) * en);
  st = dl_malloc (sizeof (int) * (vn)),
  was = dl_malloc (sizeof (int) * (vn)),
  di = dl_malloc (sizeof (int) * (vn));


  int bad = 0;

  int mul0 = 301, mul1 = 303;
  while (1) {
    memset (st, -1, sizeof (int) * (2 * d));

//    fprintf (stderr, "try = %d\n", bad);

    int i;
    en = 0;
    for (i = 0; i < n; i++) {
      int h0 = poly_h (s[i], mul0, d), h1 = poly_h (s[i], mul1, d) + d;

  //    fprintf (stderr, "%d->%d\n", h0, h1);

      ne[en] = st[h0];
      st[h0] = en;
      va[en++] = h1;

      ne[en] = st[h1];
      st[h1] = en;
      va[en++] = h0;
    }

    memset (was, 0, sizeof (int) * vn);
    int f = 1;
    for (i = 0; i < d && f; i++) {
      if (!was[i]) {
        f &= dfs (i, -1, 0);
      }
    }


    if (f) {
      int un =0;
      for (i = 0; i < vn; i++) {
        if (was[i]) {
          if (di[i] % 4 == 1 || di[i] % 4 == 2) {
            set_bit (h->code, i);
          }
          if (di[i]) {
            set_bit (h->used, i);
            un++;
          }
        }
      }

//      fprintf (stderr, "used : %d / %d\n", un, n);
      int cur = 0;
      for (i = 0; i < vn; i++) {
        if ((i & 63) == 0) {
          h->sums[i >> 6] = cur;
        }
        if (get_bit (h->used, i)) {
          cur++;
        }
      }

      h->mul0 = mul0;
      h->mul1 = mul1;
      break;
    }
    bad++;

    mul0 = R(1, 1000000000);
    mul1 = R(1, 1000000000);
  }

  en = 2 * d;
  dl_free (va, sizeof (int) * en);
  dl_free (ne, sizeof (int) * en);
  dl_free (st, sizeof (int) * (vn));
  dl_free (was, sizeof (int) * (vn));
  dl_free (di, sizeof (int) * (vn));
//  fprintf (stderr, "return %d\n", bad);
}


int ph_h (perfect_hash *h, long long s) {
  int h0 = poly_h (s, h->mul0, h->d),
      h1 = poly_h (s, h->mul1, h->d);

  h1 += h->d;

  int i;
  if (get_bit (h->code, h0) ^ get_bit (h->code, h1)) {
    i = h1;
  } else {
    i = h0;
  }

//  int tt = i;

  int res = 0;//, j;
  res = h->sums[i >> 6];

  int left = (i & 63);
  i = (i >> 5) & -2;
  if (left >= 32) {
    res += bits_cnt (h->used[i++]);
    left -= 32;
  }

  res += bits_cnt (h->used[i] & ((1 << left) - 1));
/*
  int tres = 0;
  for (j = 0; j < tt; j++) {
    tres += get_bit (h->used, j);
  }
  fprintf (stderr, "%d : %d vs %d\n", tt, res, tres);
  assert (res == tres);
  */
  return res;
}

/*char * rand_str () {
  int n = R(10, 10), i;
  char * s = malloc (n + 1);
  s[n] = 0;
  for (i = 0; i < n; i++) {
    s[i] = R('a', 'z');
  }
  return s;
}

long long rand_long () {
 // return rand () % 100000;
  return (long long)rand() * RAND_MAX + rand();
}

unsigned char buff[100000];


int main (void) {
  int cnt = 100, i, j;

  perfect_hash h;
  ph_init (&h);

//  srand (233);

  while (cnt--) {
    long long s[400000];
    int n = R(1, 30000);
    for (i = 0; i < n; i++) {
      s[i] = rand_long();
    }

//    fprintf (stderr, "n = %d\n", n);
    ph_generate (&h, s, n);

    int t = ph_encode (&h, buff);
    fprintf (stderr, "size = %d\n", t);
    ph_free (&h);
    ph_decode (&h, buff);

    int l = 0, r = h.d - 1;
//    fprintf (stderr, "%d strings -> [%d..%d]\n", n, l, r);

    int *was = calloc (sizeof (int) * (h.d * 2), 1);
    for (i = 0; i < n; i++) {
      int x = ph_h (&h, s[i]);
      assert (l <= x && x <= r);
      assert (!was[x]);
      was[x] = 1;
    }

    free (was);

    h.code = NULL;
    h.used = NULL;
    h.sums = NULL;
  }
}*/

