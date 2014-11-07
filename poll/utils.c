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

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>


#include "utils.h"
#include "word-split.h"
#include "stemmer-new.h"
#include "dl-hashable.h"


int free_nodes_cnt = 0, allocated_nodes_cnt = 0;
size_t treap_memory = 0;
treap_node_ptr free_nodes;

size_t treap_get_memory (void) {
  return treap_memory;
}

void treap_init_mem (int n) {
  assert (free_nodes_cnt == 0);
  assert (n > 0);

  treap_memory -= dl_get_memory_used();
  free_nodes = dl_malloc (sizeof (treap_node) * n);
  assert (free_nodes != NULL);
  treap_memory += dl_get_memory_used();

  allocated_nodes_cnt += n;
  free_nodes_cnt += n;

  int i;
  for (i = 0; i + 1 < n; i++) {
    free_nodes[i].l = &free_nodes[i + 1];
    free_nodes[i].r = NULL;
  }
  free_nodes[n - 1].l = free_nodes[n - 1].r = NULL;
}

void treap_init (treap *tr) {
  tr->size = 0;
  tr->root = NULL;
}

treap_node *get_new_node (void) {
  if (free_nodes_cnt == 0) {
    if (allocated_nodes_cnt < 10000) {
      if (allocated_nodes_cnt < 10) {
        treap_init_mem (10);
      } else {
        treap_init_mem (allocated_nodes_cnt);
      }
    } else {
      treap_init_mem (10000);
    }
  }

  free_nodes_cnt--;
  assert (free_nodes_cnt >= 0);

  treap_node_ptr res = free_nodes;
  free_nodes = free_nodes->l;
  res->l = NULL;

  return res;
}

void treap_split (treap_node_ptr v, ll x, treap_node_ptr *a, treap_node_ptr *b) {
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


void treap_merge (treap_node_ptr *tr, treap_node_ptr a, treap_node_ptr b) {
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

int treap_del (treap *tr, ll x) {
  treap_node_ptr *v = &tr->root;

  while (*v != NULL) {
    if ((*v)->x  == x) {
      treap_node_ptr t = *v;
      treap_merge (v, t->l, t->r);

      t->r = NULL;
      t->l = free_nodes;
      free_nodes = t;
      free_nodes_cnt++;
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

void treap_add (treap *tr, ll x, int val, int y) {
  treap_node_ptr *v = &tr->root;

  while (*v != NULL && ((*v)->y >= y)) {
    if ((*v)->x == x) {
      (*v)->val = val;
      return;
    } else if ((*v)->x > x) {
      v = &(*v)->r;
    } else if ((*v)->x < x) {
      v = &(*v)->l;
    }
  }
  treap_node_ptr vv = *v;

  while (vv != NULL) {
    if (vv->x == x) {
      vv->val = val;
      return;
    } else if (vv->x > x) {
      vv = vv->r;
    } else if (vv->x < x) {
      vv = vv->l;
    }
  }

  treap_node_ptr u = get_new_node();
  tr->size--;
  u->x = x;
  u->val = val;
  u->y = y;
  treap_split (*v, x, &u->l, &u->r);
  *v = u;
}

int treap_conv_to_array (treap_node_ptr v, pli *a, int n) {
  if (v == NULL) {
    return 0;
  }
  int ln = treap_conv_to_array (v->l, a, n);
  a += ln;
  n -= ln;
  if (n) {
    a->x = v->x;
    a->y = v->val;
    a++;
    n--;
    ln++;
  }
  ln += treap_conv_to_array (v->r, a, n);
  return ln;
}

treap_node_ptr treap_fnd (treap *t, ll x) {
  treap_node_ptr v = t->root;

  while (v != NULL) {
    if (v->x == x) {
      return v;
    } else if (v->x > x) {
      v = v->r;
    } else if (v->x < x) {
      v = v->l;
    }
  }
  return NULL;
}

void treap_free_dfs (treap_node_ptr v) {
  if (v == NULL) {
    return;
  }
  treap_free_dfs (v->l);
  treap_free_dfs (v->r);
  v->r = NULL;
  v->l = free_nodes;
  free_nodes = v;

  free_nodes_cnt++;
}

void treap_free (treap *t) {
  if (t != NULL) {
    treap_free_dfs (t->root);
    t->root = NULL;
    t->size = -0;
  }
}
