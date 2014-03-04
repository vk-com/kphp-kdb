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

    Copyright 2012-2013	Vkontakte Ltd
              2012      Sergey Kopeliovich <Burunduk30@gmail.com>
              2012      Anton Timofeev <atimofeev@vkontakte.ru>
*/
/**
 * Author:  Sergey Kopeliovich (Burunduk30@gmail.com)
 * Created: 16.03.2012
 */

#define _FILE_OFFSET_BITS 64

#include <stdlib.h>
#include <assert.h>

#include "antispam-db-treap.h"
#include "antispam-common.h"

static tree_t *new_tree_node (int x, int y) {
  tree_t *p = zmalloc (sizeof (tree_t));
  p->left = p->right = 0;
  p->x = x;
  p->y = y;
  return p;
}

static void tree_split_sk (tree_t **L, tree_t **R, tree_t *T, int x) {
  while (T) {
    if (x < T->x) {
      *R = T;
      R = &T->left;
      T = *R;
    } else {
      *L = T;
      L = &T->right;
      T = *L;
    }
  }
  *L = *R = 0;
}

static tree_t *tree_merge_sk (tree_t *L, tree_t *R) {
  tree_t *res, **t = &res;
  if (!L) {
    return R;
  }
  if (!R) {
    return L;
  }

  while (1) {
    if (L->y > R->y) {
      *t = L;
      t = &L->right;
      L = *t;
      if (!L) {
        *t = R;
        return res;
      }
    } else {
      *t = R;
      t = &R->left;
      R = *t;
      if (!R) {
        *t = L;
        return res;
      }
    }
  }
}

// Suppose that there is no 'x' in the tree
void tree_insert_sk (tree_t **V, int x, trie_node_t *v) {
  tree_t *P;
  int y = lrand48();
  while (*V && (*V)->y >= y) {
    V = (x < (*V)->x ? &(*V)->left : &(*V)->right);
  }
  P = new_tree_node (x, y);
  P->data = v;
  tree_split_sk (&P->left, &P->right, *V, x);
  *V = P;
}

// Suppose that there is 'x' in the tree
trie_node_t *tree_delete_sk (tree_t **V, int x) {
  tree_t *P;
  trie_node_t *res = 0;
  while (*V && (*V)->x != x)
    V = (x < (*V)->x ? &(*V)->left : &(*V)->right);
  assert (*V);

  P = tree_merge_sk ((*V)->left, (*V)->right);
  res = (*V)->data;
  zfree (*V, sizeof (tree_t));
  *V = P;
  return res;
}

trie_node_t *tree_find_sk (tree_t *V, int x) {
  while (V && V->x != x) {
    V = (V->x > x ? V->left : V->right);
  }
  return V ? V->data : 0;
}
