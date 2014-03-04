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
              2011 Nikolai Durov
              2011 Andrei Lopatin
*/

#include <assert.h>
#include <stdio.h>
#include <string.h>
#include "aho-kmp.h"

int KA[AHO_MAX_S + 1], KB[AHO_MAX_S + 1];
char KS[AHO_MAX_S + 1];
int KN, KL;

static int L[AHO_MAX_N], C[AHO_MAX_N];
static char *S[AHO_MAX_N];


static void sort_str (int a, int b) {
  if (a >= b) {
    return;
  }
  int i = a, j = b, q;
  char *h = S[(a + b) >> 1], *t;

  do {
    while (strcmp (S[i], h) < 0) { ++i; }
    while (strcmp (h, S[j]) < 0) { --j; }
    if (i <= j) {
      t = S[i]; S[i] = S[j]; S[j] = t;
      q = L[i]; L[i++] = L[j]; L[j--] = q;
    }
  } while (i <= j);
  sort_str (a, j);
  sort_str (i, b);
}


int aho_prepare (int cnt, char *s[]) {
  int i, j;

  if (cnt <= 0 || cnt > AHO_MAX_N) {
    return -1;
  }

  int SL = 0; 
  
  for (i = 0; i < cnt; i++) {
    L[i] = strlen (s[i]);
    if (L[i] <= 0 || L[i] > AHO_MAX_L) {
      return -1;
    }
    SL += L[i] + 1;
  }

  if (SL >= AHO_MAX_S) {
    return -1;
  }

  for (i = 0; i < cnt; i++) {
    int N = L[i], q = 0;
    char *P = s[i];
    int A[N + 1];

    A[0] = -1;
    A[1] = 0;
    j = 1;
    while (j < N) {
      while (q >= 0 && P[j] != P[q]) {
        q = A[q];
      }
      A[++j] = ++q;
    }

    for (j = 0; j < cnt; j++) {
      if (i == j || L[j] < N) {
        continue;
      }

      q = 0;
      char *T = s[j];
      while (*T) {
        while (q >= 0 && *T != P[q]) {
          q = A[q];
        }
        if (++q == N) {
          break;
        }
        T++;
      }

      if (q == N) {
        break;
      }
    }

    if (q == N) {
      L[i] = 0;
    }
  }

  int p = 0;
  for (i = 0; i < cnt; i++) {
    if (L[i] > 0) {
      L[p] = L[i];
      S[p++] = s[i];
    }
  }

  KN = p;

  sort_str (0, KN - 1);

  int MaxL = 0;
  KL = 1;

  for (i = 0; i < KN; i++) {
    memcpy (KS + KL, S[i], L[i] + 1);
    S[i] = KS + KL;
    C[i] = 0;
    KL += L[i] + 1;
    if (L[i] > MaxL) {
      MaxL = L[i];
    }
  }
  assert (KL <= AHO_MAX_S);

  KA[0] = -1;

  int l;
  for (l = 0; l <= MaxL; l++) {
    int pc = -1, ps = -256, cc = -1, v = -1;
    
    for (i = 0; i < KN; i++) {
      if (L[i] < l) {
        continue;
      }
      int u = S[i] + l - KS;
      if (L[i] == l) {
        KB[u] = (1 << i);
      } else {
        KB[u] = 0;
      }
      if (pc != C[i]) {
        int q = KA[u-1];
        while (q > 0 && S[i][l-1] != KS[q]) {
          q = KA[q];
        }
        KA[u] = q + 1;
        pc = C[i];
        ps = S[i][l];
        cc = i;
        v = u;
      } else if (ps != S[i][l]) {
        KA[u] = KA[v];
        KA[v] = u;
        ps = S[i][l];
        cc = i;
        v = u;
      }
      C[i] = cc;
    }
  } 
  return KL;
}

void aho_dump (void) {
  int i;
  for (i = 0; i < KL; i++) {
    fprintf (stderr, "%3d:  '%c'\t%d\t%d\n", i, KS[i] ? KS[i] : '.', KA[i], KB[i]);
  }
}

int aho_search (char *str) {
  int q = 1, m = 0;
  char t;
  
  while ((t = *str) != 0) {
    while (t != KS[q]) {
      q = KA[q];
      if (!q) {
        break;
      }
    }
    m |= KB[++q];
    ++str;
  } 
  return m == (1 << KN) - 1;
  //return m;
}

/*
char s[256];

int main (int argc, char *argv[]) {
  assert (argc > 1);
  assert (aho_prepare (argc - 1, argv + 1) > 0);
  aho_dump ();
  while (fgets (s, sizeof (s), stdin)) {
    fprintf (stderr, "%08x\n", aho_search (s));
  }
  return 0;
}

*/
