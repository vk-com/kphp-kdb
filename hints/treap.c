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

#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <assert.h>

#include "treap.h"

int my_rand (void) {
  return rand();
}

int free_nodes_cnt = 0, allocated_nodes_cnt = 0;
trp_node_ptr free_nodes;
size_t trp_memory = 0;

size_t trp_get_memory (void) {
  return trp_memory;
}

void trp_init_mem (int n) {
  assert (free_nodes_cnt == 0);
  assert (n > 0);

  trp_memory -= dl_get_memory_used();
  free_nodes = dl_malloc (sizeof (trp_node) * n);
  assert (free_nodes != NULL);
  trp_memory += dl_get_memory_used();

  allocated_nodes_cnt += n;
  free_nodes_cnt += n;

  int i;
  for (i = 0; i + 1 < n; i++) {
    free_nodes[i].l = &free_nodes[i + 1];
    free_nodes[i].r = NULL;
  }
  free_nodes[n - 1].l = free_nodes[n - 1].r = NULL;
}

void trp_init (treap *tr) {
  tr->size = 0;
  tr->root = NULL;
}

trp_node *get_new_node (void) {
  if (free_nodes_cnt == 0) {
    if (allocated_nodes_cnt < 10000) {
      if (allocated_nodes_cnt < 10) {
        trp_init_mem (10);
      } else {
        trp_init_mem (allocated_nodes_cnt);
      }
    } else {
      trp_init_mem (10000);
    }
  }

  free_nodes_cnt--;
  assert (free_nodes_cnt >= 0);

  trp_node_ptr res = free_nodes;
  free_nodes = free_nodes->l;
  res->l = NULL;

  return res;
}

void trp_split (trp_node_ptr v, int x, trp_node_ptr *a, trp_node_ptr *b) {
  while (v != NULL) {
    if (v->x > x) {
      *a = v;
      a = &v->r;
      v = v->r;
    } else {
      *b = v;
      b = &v->l;
      v = v->l;
    }
  }
  *a = *b = NULL;
}


void trp_merge (trp_node_ptr *tr, trp_node_ptr a, trp_node_ptr b) {
  while (a != NULL || b != NULL) {
     if (a == NULL || (b != NULL && b->y > a->y)) {
       *tr = b;
       tr = &b->l;
       b = b->l;
     } else {
       *tr = a;
       tr = &a->r;
       a = a->r;
     }
  }
  *tr = NULL;
}

int trp_del (treap *tr, int x) {
  trp_node_ptr *v = &tr->root;

  while (*v != NULL) {
    if ((*v)->x  == x) {
      trp_node_ptr t = *v;
      trp_merge (v, t->l, t->r);

      t->r = NULL;
      t->l = free_nodes;
      free_nodes = t;
      tr->size++;

      return t->y;
    } else if ((*v)->x > x) {
      v = &(*v)->r;
    } else if ((*v)->x < x) {
      v = &(*v)->l;
    }
  }
  return 0;
}

void trp_add (treap *tr, int x, int y) {
  trp_node_ptr *v = &tr->root;

  while (*v != NULL && (*v)->y >= y) {
    if ((*v)->x > x) {
      v = &(*v)->r;
    } else {
      v = &(*v)->l;
    }
  }
  trp_node_ptr u = get_new_node();
  tr->size--;
  u->x = x;
  u->y = y;
  trp_split (*v, x, &u->l, &u->r);
  *v = u;
}

void trp_incr (treap *tr, int x) {
  int y = trp_del (tr, x);
  trp_add (tr, x, (~(~((y) >> 16) + 1) << 16) | (my_rand() & 0xFFFF));
}

void trp_add_or_set (treap *tr, int x, int y) {
  trp_node_ptr *v = &tr->root;

  while (*v != NULL && ((*v)->y >= y)) {
    if (((*v)->x ^ x) <= 1) {
      (*v)->x = x;
      return;
    } else if ((*v)->x > x) {
      v = &(*v)->r;
    } else if ((*v)->x < x) {
      v = &(*v)->l;
    }
  }
  trp_node_ptr vv = *v;

  while (vv != NULL) {
    if ((vv->x ^ x) <= 1) {
      vv->x = x;
      return;
    } else if (vv->x > x) {
      vv = vv->r;
    } else if (vv->x < x) {
      vv = vv->l;
    }
  }

  trp_node_ptr u = get_new_node();
  tr->size--;
  u->x = x;
  u->y = y;
  trp_split (*v, x, &u->l, &u->r);
  *v = u;
}

trp_node_ptr trp_conv_from_array (int *a, int n) {
  static trp_node_ptr stack[600];
//  assert (n <= 50);

  int sn = 0, i;

  stack[0] = NULL;

  for (i = 0; i < n; i++) {
    trp_node_ptr new_el = get_new_node();
    new_el->x = a[i];
    new_el->y = my_rand();
    new_el->r = NULL;
    while (sn && stack[sn - 1]->y < new_el->y) {
      sn--;
    }
    if (sn) {
      new_el->l = stack[sn - 1]->r;
      stack[sn - 1]->r = new_el;
    } else {
      new_el->l = stack[0];
    }
    stack[sn++] = new_el;
  }

  return stack[0];
}

trp_node_ptr trp_conv_from_array_rev (int *a, int n) {
  static trp_node_ptr stack[600];
//  assert (n <= 50);

  int sn = 0, i;

  stack[0] = NULL;

  for (i = n - 1; i >= 0; i--) {
    trp_node_ptr new_el = get_new_node();
    new_el->x = a[i];
    new_el->y = my_rand();
    new_el->r = NULL;
    while (sn && stack[sn - 1]->y < new_el->y) {
      sn--;
    }
    if (sn) {
      new_el->l = stack[sn - 1]->r;
      stack[sn - 1]->r = new_el;
    } else {
      new_el->l = stack[0];
    }
    stack[sn++] = new_el;
  }

  return stack[0];
}

int trp_dfs (trp_node_ptr v, int *res) {
  int *beg = res;
  if (v != NULL) {
    res += trp_dfs (v->r, res);
    *res++ = v->x;
    res += trp_dfs (v->l, res);
  }
  return res - beg;
}

int trp_conv_to_array_rev (treap tr, int *res) {
  assert (trp_dfs (tr.root, res) == -tr.size);
  return -tr.size;
}

int trp_has (treap tr, int x) {
//  x *= 2;
  trp_node_ptr v = tr.root;

  while (v != NULL) {
    if ((v->x ^ x) <= 1) {
      return v->x & 1 ? 1 : -1;
    } else if (v->x > x) {
      v = v->r;
    } else if (v->x < x) {
      v = v->l;
    }
  }
  return 0;
}

void trp_free (trp_node_ptr v) {
  if (v == NULL) {
    return;
  }
  trp_free (v->l);
  trp_free (v->r);
  v->r = NULL;
  v->l = free_nodes;
  free_nodes = v;

  free_nodes_cnt++;
}

void out (trp_node_ptr v, int d) {
  if (v == NULL) {
    return;
  }
  if (v->l != NULL) {
    assert (v->l->x > v->x);
    assert (v->l->y <= v->y);
    out (v->l, d + 2);
  }
  fprintf (stderr, "%*s(%d;%d)\n", d, "", v->x, v->y);
  if (v->r != NULL) {
    assert (v->r->x < v->x);
    assert (v->r->y <= v->y);
    out (v->r, d + 2);
  }
}
void outf (trp_node_ptr v) {
  if (v == NULL) {
    return;
  }
  outf (v->l);
  fprintf (stderr, "%+d ", v->x / 2 * (v->x & 1 ? +1 : -1));
  outf (v->r);
}

void trp_debug_print (trp_node_ptr v, FILE *f) {
  if (v == NULL) {
    return;
  }
  trp_debug_print (v->l, f);
  fprintf (f, " %+d", v->x / 2 * (v->x & 1 ? +1 : -1));
  trp_debug_print (v->r, f);
}

int cmp_int (const void *a, const void *b) {
  return *(const int *)a - *(const int *)b;
}

