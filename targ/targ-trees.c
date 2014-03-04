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

    Copyright 2011-2012 Vkontakte Ltd
              2011-2012 Nikolai Durov
              2011-2012 Andrei Lopatin
*/

#include <assert.h>
#include <string.h>
#include <stdlib.h>
#include "targ-trees.h"

treespace_t create_treespace (unsigned treespace_ints, int node_ints) {
  int free_queue_cnt = 256;
  unsigned long long tot_size = ((unsigned long long) treespace_ints + free_queue_cnt * 2) * 4 + sizeof (struct treespace_header);
  assert (node_ints >= 3 && node_ints <= 32);
  assert (tot_size < MAXLONG);
  assert (tot_size <= 0xffffffffULL * 4);
  assert (free_queue_cnt >= 8 && free_queue_cnt <= 4096 && !(free_queue_cnt & (free_queue_cnt - 1)));
  int *TS = malloc (tot_size);
  assert (TS);
  TS -= 8;
  TS_HEADER->magic = TS_MAGIC;
  TS_HEADER->node_ints = node_ints;
  TS_HEADER->alloc_ints = (tot_size >> 2);
  TS_HEADER->used_ints = (sizeof (struct treespace_header) >> 2) + free_queue_cnt * 2;
  TS_HEADER->free_queue_cnt = free_queue_cnt;
  memset (TS_HEADER->resvd2, 0, sizeof (TS_HEADER->resvd2));
  memset (TS_HEADER->free_queue, 0, free_queue_cnt * 8);
  return TS;
};

inline void alloc_new_intree_page (treespace_t TS) {
  unsigned s = TS_HEADER->used_ints;
  unsigned N = TS_HEADER->free_queue_cnt;
  unsigned sz = TS_HEADER->node_ints;
  assert (s + N*sz <= TS_HEADER->alloc_ints);
#if 0
  int *A = TS + s + (sz - 1)*N;
  int i, j;
  for (i = 0; i < N; i++) {
    j = lrand48() % (i + 1);
    A[i] = A[j];
    A[j] = i;
  }
  for (i = 0; i < N; i++, s += sz) {
    int q = A[i];
#else
  int q;
  for (q = 0; q < N; q++, s += sz) {
#endif
    TS_NODE(s)->left = TS_HEADER->free_queue[q*2];
    TS_HEADER->free_queue[q*2] = s;
    TS_HEADER->free_queue[q*2+1]++;
  }
  TS_HEADER->used_ints = s;
}

treeref_t new_intree_node (treespace_t TS) {
  // XA-XA 5 PA3
  int m = TS_HEADER->free_queue_cnt - 1;
  long q = lrand48() & m;
  int p = TS_HEADER->free_queue[q*2];
  if (!p) {
    int i = 5;
    while (--i) {
      q = lrand48() & m;
      p = TS_HEADER->free_queue[q*2];
      if (p) {
        break;
      }
    }
    if (!p) {
      assert (!TS_HEADER->free_queue[q*2+1]);
      alloc_new_intree_page (TS);
      p = TS_HEADER->free_queue[q*2];
      assert (p);
    }
  }
  TS_HEADER->free_queue[q*2] = TS_NODE(p)->left;
  assert (--TS_HEADER->free_queue[q*2+1] >= 0);
  return p;
}

inline void free_intree_node (treespace_t TS, treeref_t N) {
  int q = lrand48() & (TS_HEADER->free_queue_cnt - 1);
  TS_NODE(N)->left = TS_HEADER->free_queue[q*2];
  TS_HEADER->free_queue[q*2] = N;
  TS_HEADER->free_queue[q*2+1]++;
};

inline treeref_t intree_lookup (treespace_t TS, treeref_t T, int x) {
  while (T && TS_NODE(T)->x != x) {
    if (x < TS_NODE(T)->x) {
      T = TS_NODE(T)->left;
    } else {
      T = TS_NODE(T)->right;
    }
  }
  return T;
}

#define MULT 2654435769U
#define NODE_Y(TC,T) (((unsigned)TC->x + T) * MULT)
#define TS_NODE_Y(T) (NODE_Y(TS_NODE(T), T))


inline void intree_split (treespace_t TS, treeref_t T, int x, treeref_t *L, treeref_t *R) {
  while (T) {
    struct intree_node *TP = TS_NODE(T);
    if (x < TP->x) {
      *R = T;
      T = TP->left;
      R = &TP->left;
    } else {
      *L = T;
      T = TP->right;
      L = &TP->right;
    }
  }
  *L = *R = 0;
}

inline treeref_t intree_merge (treespace_t TS, treeref_t L, treeref_t R) {
  treeref_t T, *TP = &T;
  struct intree_node *LP, *RP;
  unsigned LY, RY;
  if (!L) {
    return R;
  }
  if (!R) {
    return L;
  }
  LP = TS_NODE(L);
  LY = NODE_Y(LP, L);
  RP = TS_NODE(R);
  RY = NODE_Y(RP, R);
  while (1) {
    if (LY > RY) {
      *TP = L;
      L = LP->right;
      if (!L) {
        LP->right = R;
        return T;
      }
      TP = &LP->right;
      LP = TS_NODE(L);
      LY = NODE_Y(LP, L);
    } else {
      *TP = R;
      R = RP->left;
      if (!R) {
        RP->left = L;
        return T;
      }
      TP = &RP->left;
      RP = TS_NODE(R);
      RY = NODE_Y(RP, R);
    }
  }
}


inline treeref_t intree_insert (treespace_t TS, treeref_t T, treeref_t N) {
  treeref_t Q = T, *QP = &Q;
  struct intree_node *NP = TS_NODE(N);
  int NX = NP->x; 
  unsigned NY = NODE_Y(NP, N);
  while (T) {
    struct intree_node *TC = TS_NODE(T);
    unsigned TY = NODE_Y(TC, T);
    if (NY >= TY) {
      break;
    }
    if (NX < TC->x) {
      QP = &TC->left;
      T = TC->left;
    } else {
      QP = &TC->right;
      T = TC->right;
    }
  }
  intree_split (TS, T, NX, &NP->left, &NP->right);
  *QP = N;
  return Q;
}

inline treeref_t intree_remove (treespace_t TS, treeref_t T, int x, treeref_t *N) {
  treeref_t Q = T, *QP = &Q;
  while (T) {
    struct intree_node *TC = TS_NODE(T);
    if (x == TC->x) {
      break;
    }
    if (x < TC->x) {
      QP = &TC->left;
      T = TC->left;
    } else {
      QP = &TC->right;
      T = TC->right;
    }
  }
  if (T) {
    struct intree_node *TC = TS_NODE(T);
    *QP = intree_merge (TS, TC->left, TC->right);
  }
  *N = T;
  return Q;
}

inline treeref_t intree_delete (treespace_t TS, treeref_t T, int x) {
  treeref_t R, N;
  R = intree_remove (TS, T, x, &N);
  if (N) {
    free_intree_node (TS, N);
  }
  return R;
}

inline treeref_t intree_incr_z (treespace_t TS, treeref_t T, int x, int dz, int *nodes_num) {
  treeref_t Q = T, *QP = &Q;
  if (!dz) {
    return T;
  }
  while (T) {
    struct intree_node *TC = TS_NODE(T);
    if (x == TC->x) {
      break;
    }
    if (x < TC->x) {
      QP = &TC->left;
      T = TC->left;
    } else {
      QP = &TC->right;
      T = TC->right;
    }
  }
  if (T) {
    struct intree_node *TC = TS_NODE(T);
    TC->z += dz;
    if (!TC->z) {
      *QP = intree_merge (TS, TC->left, TC->right);
      free_intree_node (TS, T);
      --*nodes_num;
    }
    return Q;
  } 

  T = new_intree_node (TS);
  struct intree_node *TC = TS_NODE(T);
  TC->x = x;
  TC->z = dz;
  ++*nodes_num;
  return intree_insert (TS, Q, T);
}

int intree_free (treespace_t TS, treeref_t T) {
  int res = 0;
  if (T) {
    res++;
    struct intree_node *TC = TS_NODE(T);
    res += intree_free (TS, TC->left);
    res += intree_free (TS, TC->right);
    free_intree_node (TS, T);
  }
  return res;
}

int intree_traverse (treespace_t TS, treeref_t T, intree_traverse_func_t traverse_node) {
  if (T) {
    struct intree_node *TC = TS_NODE(T);
    return intree_traverse (TS, TC->left, traverse_node) + traverse_node (TC) + intree_traverse (TS, TC->right, traverse_node);
  } else {
    return 0;
  }
}

int intree_unpack (treespace_t TS, treeref_t T, int *A) {
  int t;
  if (!T) { return 0; }
  struct intree_node *TC = TS_NODE (T);
  A += t = intree_unpack (TS, TC->left, A);
  *A++ = TC->x;
  return t + 1 + intree_unpack (TS, TC->right, A);
}

treeref_t intree_build_from_list (treespace_t TS, int *A, int nodes) {
  if (!nodes) {
    return 0;
  }
  static treeref_t st[128];
  static unsigned sty[128];
  int sp = 0, i, prev = 0;
  for (i = 0; i < nodes; i++) {
    assert (*A > prev);
    prev = *A;
    treeref_t N = new_intree_node (TS);
    struct intree_node *NP = TS_NODE (N);
    NP->x = *A++;
    NP->z = *A++;
    unsigned NY = NODE_Y(NP, N);
    int last = 0;
    while (sp) {
      if (NY <= sty[sp - 1]) {
	struct intree_node *TC = TS_NODE (st[sp - 1]);
	TC->right = N;
	break;
      }
      last = st[--sp];
    }
    NP->left = last;
    NP->right = 0;
    st[sp] = N;
    sty[sp] = NY;
    assert (++sp < 128);
  }

  return st[0];
}

static int intree_check_tree_internal (treespace_t TS, treeref_t T, int a, int b) {
  if (!T) {
    return 0;
  }
  struct intree_node *TC = TS_NODE (T);
  assert (TC->x >= a && TC->x <= b);
  return intree_check_tree_internal (TS, TC->left, a, TC->x - 1) + intree_check_tree_internal (TS, TC->right, TC->x + 1, b) + 1;
}

int intree_check_tree (treespace_t TS, treeref_t T) {
  return intree_check_tree_internal (TS, T, -1 << 31, ~(-1 << 31));
}

int get_treespace_free_stats (treespace_t TS) {
  int i;
  long long res = 0;
  for (i = 0; i < TS_HEADER->free_queue_cnt; i++) {
    res += TS_HEADER->free_queue[2 * i + 1];
  }
  return res;
}

int get_treespace_free_detailed_stats (treespace_t TS, int *where) {
  int i, N = TS_HEADER->free_queue_cnt;
  int *from = TS_HEADER->free_queue + 1;
  for (i = 0; i < N; i++) {
    *where++ = *from++;
    ++from;
  }
  return N;
}
