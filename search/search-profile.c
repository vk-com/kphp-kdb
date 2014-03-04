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


#include "search-profile.h"
#include "utils.h"

#define SEARCH_QUERY_HEAP_SIZE 63
extern int now;

static int SQH_SIZE;
static search_query_heap_en_t SQH[SEARCH_QUERY_HEAP_SIZE+1];

static void search_query_copy (search_query_heap_en_t *D, search_query_heap_en_t *S) {
  D->cpu_time = S->cpu_time;
  D->res = S->res;
  D->expiration_time = S->expiration_time;
  strcpy (D->query, S->query);
}

static void search_query_heapify_front (search_query_heap_en_t *E, int i) {
  while (1) {
    int j = i << 1;
    if (j > SQH_SIZE) { break; }
    if (j < SQH_SIZE && SQH[j].cpu_time > SQH[j+1].cpu_time) { j++; }
    if (E->cpu_time <= SQH[j].cpu_time) { break; }
    search_query_copy (SQH + i, SQH + j);
    i = j;
  }
  search_query_copy (SQH + i, E);
}

static void search_query_heapify_back (search_query_heap_en_t *E, int i) {
  while (i > 1) {
    int j = (i >> 1);
    if (SQH[j].cpu_time <= E->cpu_time) { break; }
    search_query_copy (SQH + i, SQH + j);
    i = j;
  }
  search_query_copy (SQH + i, E);
}

static void search_query_heap_insert (search_query_heap_en_t *E) {
  if (SQH_SIZE == SEARCH_QUERY_HEAP_SIZE) {
    search_query_heapify_front (E, 1);
  } else {
    search_query_heapify_back (E, ++SQH_SIZE);
  }
}

static int cmp_search_queries (const void *a, const void *b) {
  const search_query_heap_en_t *A = *((const search_query_heap_en_t **) a);
  const search_query_heap_en_t *B = *((const search_query_heap_en_t **) b);
  if (A->cpu_time < B->cpu_time) { return -1; }
  if (A->cpu_time > B->cpu_time) { return  1; }
  return 0;
}

int search_query_worst (char *output, int olen) {
  int i;
  search_query_heap_en_t **A = alloca (SQH_SIZE * sizeof (A[0]));
  for (i = 0; i < SQH_SIZE; i++) {
    A[i] = SQH + i + 1;
  }
  qsort (A, SQH_SIZE, sizeof (A[0]), cmp_search_queries);
  char *p = output;
  for (i = SQH_SIZE - 1; i >= 0; i--) {
    if (strlen (A[i]->query) + 30 > olen) { break; }
    int l = sprintf (p, "%s\t%.6lf\t%d\t%d\n", A[i]->query, A[i]->cpu_time, A[i]->res, A[i]->expiration_time - SEARCH_QUERY_EXPIRATION_TIME);
    p += l;
    olen -= l;
  }
  return p - output;
}

void search_query_start (search_query_heap_en_t *E) {
  E->cpu_time = -get_rusage_time ();
}

void search_query_end (search_query_heap_en_t *E, int res, void *arg, void (*copy) (search_query_heap_en_t *, void *)) {
  E->cpu_time += get_rusage_time ();
  if (SQH_SIZE < SEARCH_QUERY_HEAP_SIZE || SQH[1].cpu_time < E->cpu_time) {
    copy (E, arg);
    E->res = res;
    E->expiration_time = now + SEARCH_QUERY_EXPIRATION_TIME;
    search_query_heap_insert (E);
  }
}

void search_query_remove_expired (void) {
  static int t = 0;
  if (now > t) {
    t = now + 60;
  } else {
    return;
  }
  int i;
  for (i = 1; i <= SQH_SIZE; ) {
    if (SQH[i].expiration_time < now) {
      SQH_SIZE--;
      search_query_heap_en_t *E = SQH + SQH_SIZE;
      if (i != SQH_SIZE) {
        if (SQH[i].cpu_time < E->cpu_time) {
          search_query_heapify_front (E, i);
        } else {
          search_query_heapify_back (E, i);
        }
      }
    } else {
      i++;
    }
  }
}

