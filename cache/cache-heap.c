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

#include <stdlib.h>
#include "cache-heap.h"

static void heap_sift (cache_heap_t *self, void *E, int i) {
  while (1) {
    int j = i << 1;
    if (j > self->size) {
      break;
    }
    if (j < self->size && self->compare (self->H[j], self->H[j+1]) < 0) {
      j++;
    }
    if (self->compare (E, self->H[j]) >= 0) {
      break;
    }
    self->H[i] = self->H[j];
    i = j;
  }
  self->H[i] = E;
}

void cache_heap_insert (cache_heap_t *self, void *E) {
  if (self->size < self->max_size) {
    int i = ++(self->size);
    while (i > 1) {
      int j = i >> 1;
      if (self->compare (self->H[j], E) >= 0) {
        break;
      }
      self->H[i] = self->H[j];
      i = j;
    }
    self->H[i] = E;
  } else {
    if (self->compare (self->H[1], E) > 0) {
      heap_sift (self, E, 1);
    }
  }
}

int cache_heap_sort (cache_heap_t *self) {
  int r = self->size;
  while (self->size > 1) {
    void *E = self->H[1];
    heap_sift (self, self->H[(self->size)--], 1);
    self->H[self->size+1] = E;
  }
  self->size = 0;
  return r;
}

void *cache_heap_pop (cache_heap_t *self) {
  if (self->size == 0) {
    return NULL;
  }
  void *E = self->H[1];
  heap_sift (self, self->H[(self->size)--], 1);
  return E;
}

#ifdef TEST
#include <assert.h>
#include "server-functions.h"
static int test_top_pint_cmp (const void *a, const void *b) {
  int *x = *((int **) a), *y = *((int **) b);
  if (*x > *y) {
    return -1;
  } else if (*x < *y) {
    return 1;
  } else {
    return 0;
  }
}

static int test_top_int_cmp (const void *a, const void *b) {
  int x = *((int *) a), y = *((int *) b);
  if (x > y) {
    return -1;
  } else if (x < y) {
    return 1;
  } else {
    return 0;
  }
}

static void test_heap (int seed, int heap_size, int n) {
  kprintf ("test_heap (seed: %d, heap_size: %d, n: %d)\n", seed, heap_size, n);
  int i;
  cache_heap_t h;
  srand48 (seed);
  int **A = calloc (n, sizeof (A[0]));
  assert (A);
  for (i = 0; i < n; i++) {
    A[i] = malloc (4);
    *(A[i]) = lrand48 ();
  }
  h.size = 0;
  h.max_size = (heap_size < CACHE_MAX_HEAP_SIZE) ? heap_size : CACHE_MAX_HEAP_SIZE;
  h.compare = test_top_int_cmp;
  for (i = 0; i < n; i++) {
    cache_heap_insert (&h, A[i]);
  }
  int m = cache_heap_sort (&h);
  qsort (A, n, sizeof (A[0]), test_top_pint_cmp);
  for (i = 0; i < m; i++) {
    int *B = ((int *) h.H[i+1]);
    vkprintf (4, "i: %d (%d and %d)\n", i, (*(A[i])), *B);
    assert ((*(A[i])) ==  (*B));
  }
  for (i = 0; i < n; i++) {
    free (A[i]);
  }
  free (A);
}

void cache_heap_test (void) {
  test_heap (1, 2, 2);
  test_heap (2, 10, 100);
  test_heap (3, 10, 1000);
  test_heap (4, 10, 10000);
  test_heap (5, 1000, 10000);
  test_heap (5, 1000, 1000000);
  exit (0);
}
#endif
