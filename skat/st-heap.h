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

    Copyright 2012 Vkontakte Ltd
              2012 Sergey Kopeliovich <Burunduk30@gmail.com>
              2012 Anton Timofeev <atimofeev@vkontakte.ru>
*/

/**
 * Author:  Anton  Timofeev    (atimofeev@vkontakte.ru)
 *          Heap "template" implementation: Min element stored in root.
 *          There are different implementations for several comparisons,
 *          See end of the file for more details.
 * Created: 30.03.2012
 */

#pragma once

#include <assert.h>
#include <stdlib.h>
#include <string.h>

#include "dl-utils.h"

#define DECLARE_HEAP(value_t, name)                                            \
void name##_down   (value_t *heap, int heap_size, int v);                      \
void name##_up     (value_t *heap, int v);                                     \
void name##_update (value_t *heap, int heap_size, value_t val);                \
void name##_add    (value_t *heap, int heap_size, value_t val);                \
int  name##_amend   (value_t *heap, int heap_size, int limit, value_t val);    \
void name##_build (value_t *heap, int heap_size);                              \
void name##_sort (value_t *heap, int heap_size);                               \

#define DEFINE_HEAP_IMPL(value_t, name, key, leq, op)                          \
void name##_down (value_t *heap, int heap_size, int v) {                       \
  while (v <= heap_size) {                                                     \
    int x = v << 1;                                                            \
    if (x + 1 <= heap_size && leq (heap[x + 1] key op heap[x] key)) {          \
      ++x;                                                                     \
    }                                                                          \
    if (x > heap_size || leq (heap[v] key op heap[x] key)) {                   \
      break;                                                                   \
    }                                                                          \
    dl_swap (heap[v], heap[x]);                                                \
    v = x;                                                                     \
  }                                                                            \
}                                                                              \
void name##_up (value_t *heap, int v) {                                        \
  while (v > 1) {                                                              \
    int p = v >> 1;                                                            \
    if (leq (heap[p] key op heap[v] key)) {                                    \
      break;                                                                   \
    }                                                                          \
    dl_swap (heap[v], heap[p]);                                                \
    v = p;                                                                     \
  }                                                                            \
}                                                                              \
void name##_update (value_t *heap, int heap_size, value_t val) {               \
  if (!(leq (val key op heap[1] key))) {                                       \
    heap[1] = val;                                                             \
    name##_down (heap, heap_size, 1);                                          \
  }                                                                            \
}                                                                              \
void name##_add (value_t *heap, int heap_size, value_t val) {                  \
  heap[++heap_size] = val;                                                     \
  name##_up (heap, heap_size);                                                 \
}                                                                              \
int name##_amend (value_t *heap, int heap_size, int limit, value_t val) {      \
  if (heap_size >= limit) {                                                    \
    name##_update (heap, heap_size, val);                                      \
    return heap_size;                                                          \
  }                                                                            \
  name##_add (heap, heap_size, val);                                           \
  return heap_size + 1;                                                        \
}                                                                              \
void name##_build (value_t *heap, int heap_size) {                             \
  int i;                                                                       \
  for (i = (heap_size >> 1); i >= 1; --i) {                                    \
    name##_down (heap, heap_size, i);                                          \
  }                                                                            \
}                                                                              \
void name##_sort (value_t *heap, int heap_size) {                              \
  name##_build (heap, heap_size);                                              \
  while (heap_size > 1) {                                                      \
    dl_swap (heap[1], heap[heap_size]);                                        \
    name##_down (heap, --heap_size, 1);                                        \
  }                                                                            \
}                                                                              \

#define DEFINE_HEAP(value_t, name) \
  DEFINE_HEAP_IMPL (value_t, name, , , <=)

#define DEFINE_HEAP_LEQ(value_t, name, oper) \
  DEFINE_HEAP_IMPL (value_t, name, , , oper)

#define DEFINE_HEAP_LEQ_KEY(value_t, name, key, oper) \
  DEFINE_HEAP_IMPL (value_t, name, .key, , oper)

#define DEFINE_HEAP_LEQ_PTR_KEY(value_t, name, key, oper) \
  DEFINE_HEAP_IMPL (value_t, name, ->key, , oper)

#define ST_HEAP_OP_COMMA ,
#define DEFINE_HEAP_LEQ_FUNC(value_t, name, leq) \
  DEFINE_HEAP_IMPL (value_t, name, , leq, ST_HEAP_OP_COMMA)
