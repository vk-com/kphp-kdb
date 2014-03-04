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

    Copyright 2011 Vkontakte Ltd
              2011 Anton Maydell
*/

#include <stdio.h>
#include <stdlib.h>
#include <assert.h>

#include "server-functions.h"
#include "kdb-data-common.h"
#include "suffix-array.h"

typedef struct {
  int *head;
  int *prev;
  int *value;
} buckets_t;

static void ss_bucket_sort (buckets_t *B, int *p, int n, int *R, int k) {
  int i, j, r;
  for (i = -1; i < n; i++) {
    B->head[i] = -1;
  }
  for (r = 0; r < n; r++) {
    j = p[r] + k;
    i = (j < n) ? R[j] : -1;
    B->value[r] = p[r];
    B->prev[r] = B->head[i];
    B->head[i] = r;
  }
  r = n - 1;
  for (i = n - 1; i >= -1; i--) {
    for (j = B->head[i]; j >= 0; j = B->prev[j]) {
      p[r] = B->value[j];
      r--;
    }
  }
  assert (r == -1);
}

static void suffix_array_sort (suffix_array_t *A) {
  unsigned char *y = A->y;
  const int n = A->n;
  int *p = A->p;
  int i, r, m;
  dyn_mark_t ss_mark;
  dyn_mark (ss_mark);
  int *c = zmalloc0 (4 * 256);
  for (i = 0; i < n; i++) {
    c[y[i]]++;
  }
  m = 0;
  for (i = 0; i < 256; i++) {
    if (c[i]) {
      c[i] = m++;
    }
  }
  /* m - cardalph (y) */
  buckets_t B;
  B.head = zmalloc (4 * (n + 1));
  B.head++;
  B.prev = zmalloc (4 * n);
  B.value = zmalloc (4 * n);
  int *R = zmalloc (4 * n), *W = zmalloc (4 * n);
  for (r = 0; r < n; r++) {
    p[r] = r;
  }
  int k = 1;
  for (i = 0; i < n; i++) {
    R[i] = c[y[i]];
  }
  ss_bucket_sort (&B, p, n, R, 0);
  i = m - 1;
  while (i < n - 1) {
    ss_bucket_sort (&B, p, n, R, k);
    ss_bucket_sort (&B, p, n, R, 0);
    W[p[0]] = i = 0;
    for (r = 1; r < n; r++) {
      int p1 = p[r], p2 = p[r-1];
      if (R[p1] != R[p2]) {
        W[p1] = ++i;
        continue;
      }
      p1 += k;
      p2 += k;
      if ((p1 < n ? R[p1] : -1) != (p2 < n ? R[p2] : -1)) {
        i++;
      }
      W[p[r]] = i;
    }
    int *tmp = R; R = W; W = tmp;
    k <<= 1;
  }
  dyn_release (ss_mark);
}

static int lcp_table (suffix_array_t *A, int d, int f) {
  if (d + 1 == f) { return A->lcp[f]; }
  int i = (d + f) >> 1, *r = A->lcp + (A->n + 1 + i);
  if (*r >= 0) { return *r; }
  int r1 = lcp_table (A, d, i), r2 = lcp_table (A, i, f);
  return *r = (r1 < r2) ? r1 : r2;
}

static void suffix_array_lcp_init (suffix_array_t *A) {
  int i, j, k;
  const int n = A->n;
  A->lcp = malloc (4 * (2 * n + 1));
  unsigned char *y = A->y;
  int *p = A->p;
  int *LCP = A->lcp;
  dyn_mark_t ss_mark;
  dyn_mark (ss_mark);
  int *R = zmalloc (4 * n);  
  for (i = 0; i < n; i++) {
    R[p[i]] = i;
  }
  int l = 0;
  for (j = 0; j < n; j++) {
    if (--l < 0) {
      l = 0;
    }
    i = R[j];
    if (i != 0) {
      k = p[i-1];
      while (j + l < n && k + l < n && y[j+l] == y[k+l]) {
        l++;
      }
    } else {
      l = 0;
    }
    LCP[i] = l;
  }
  LCP[n] = 0;
  dyn_release (ss_mark);
  for (i = n + n; i > n; i--) {
    LCP[i] = -1;
  }
  lcp_table (A, -1, n);
}

void suffix_array_init (suffix_array_t *A, unsigned char *y, int n) {
  vkprintf (3, "suffix_array_init (%.*s)\n", n, y);
  A->y = y;
  A->n = n;
  A->p = malloc (4 * n);
  suffix_array_sort (A);
  suffix_array_lcp_init (A);
}

void suffix_array_free (suffix_array_t *A) {
  free (A->p);
  free (A->lcp);
}

static inline int get_lcp (suffix_array_t *A, int d, int f) {
  return (d + 1 == f) ? A->lcp[f] : A->lcp[A->n + 1 + ((d + f) >> 1)];
}

int suffix_array_search (suffix_array_t *A, unsigned char *x, int m, int *common_length) {
  const int n = A->n;
  int d = -1, ld = 0;
  int f = n, lf = 0;
  while (d + 1 < f) {
    int i = (d + f) >> 1;
    const int lcp_di = get_lcp (A, d, i), lcp_if = get_lcp (A, i, f);
    vkprintf (3, "suffix_array_search (%.*s), d = %d, ld = %d, f = %d, lf = %d, lcp_di = %d, lcp_if = %d\n", m, x, d, ld, f, lf, lcp_di, lcp_if);    
    if (ld <= lcp_if && lcp_if < lf) {
      d = i;
      ld = lcp_if;
    } else if (ld <= lf && lf < lcp_if) {
      f = i;
    } else if (lf <= lcp_di && lcp_di < ld) {
      f = i;
      lf = lcp_di;
    } else if (lf <= ld && ld < lcp_di) {
      d = i;
    } else {
      const int li = n - A->p[i];
      int l = (ld >= lf) ? ld : lf, o;
      int max_o = li;
      if (max_o > m) { 
        max_o = m; 
      }
      max_o -= l;
      unsigned char *s = x + l, *t = A->y + (A->p[i] + l);
      for (o = 0; o < max_o && *s == *t; o++, s++, t++) {}
      l += o;
      if (l == m && l == li) {
        *common_length = m;
        return A->p[i];
      } else if (l == li || (l != m && *t < *s)) {
        d = i;
        ld = l;
      } else {
        f = i;
        lf = l;
      }
    }
  }
  if (ld >= lf) {
    *common_length = ld;
    return d >= 0 ? A->p[d] : -1;
  }
  *common_length = lf;
  return f < n ? A->p[f] : -1;
}

void suffix_array_dump (suffix_array_t *A) {
  int i;
  for (i = 0; i < A->n; i++) {
    fprintf (stderr, "%d: %.*s\n", i, A->n - A->p[i], A->y + A->p[i]);
  }
}

void suffix_array_check (suffix_array_t *A) {
  int i;
  for (i = 0; i + 1 < A->n; i++) {
    unsigned char *s = A->y + A->p[i];
    unsigned char *t = A->y + A->p[i+1];
    int o, l = A->n - A->p[i+1];
    if (l > A->n - A->p[i]) {
      l = A->n - A->p[i];
    }
    for (o = 0; o < l; o++) {
      if (*s != *t) { break; }
      s++;
      t++;
    }
    assert (s == A->y + A->n || *s < *t);
    assert (o == A->lcp[i+1]);
  }
  assert (A->lcp[0] == 0);
  assert (A->lcp[A->n] == 0);
}
