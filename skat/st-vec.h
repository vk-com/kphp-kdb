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
 * Created: 29.03.2012
 */

#pragma once

#include <assert.h>
#include <string.h>

#include "dl-utils.h"

/**
 * Provides basic vector (array with variable length) functionality.
 * To careful use only: implementation is not functions but defines.
 * Parameters with underscore prefix _ allows side effect, sample:
 *   st_vec_pb (my_vec_with_ids, ++next_id + 3 + magic());
 */

#define ST_VEC_T(v) typeof (v)
#define ST_VEC_EL_T(v) typeof (*(v).first)
#define ST_VEC_EL_PTR(v) typeof ((v).first)
#define ST_VEC_EL_SIZE(v) (sizeof (ST_VEC_EL_T (v)))

#define DECLARE_VEC(value_t, name)                                             \
typedef struct name {                                                          \
  long capacity, size;                                                         \
  value_t *first;                                                              \
} name##_t;                                                                    \

#define st_vec_create(v, _capacity) {                                          \
  (v).capacity = (_capacity);                                                  \
  (v).size = 0;                                                                \
  assert ((v).capacity >= 0);                                                  \
  if ((v).capacity > 0) {                                                      \
    (v).first = dl_malloc ((v).capacity * ST_VEC_EL_SIZE (v));                 \
    assert ((v).first != 0);                                                   \
  } else {                                                                     \
    (v).first = 0;                                                             \
  }                                                                            \
}                                                                              \

#define st_vec_destroy(v) st_vec_clear (v)
#define st_vec_clear(v) {                                                      \
  if ((v).first != 0) {                                                        \
    dl_free ((v).first, (v).capacity * ST_VEC_EL_SIZE (v));                    \
  }                                                                            \
  (v).first = 0;                                                               \
  (v).size = (v).capacity = 0;                                                 \
}                                                                              \

#define st_vec_capacity(v) ((v).capacity)
#define st_vec_size(v)     ((v).size)
#define st_vec_at(v, i)    ((v).first[i])
#define st_vec_last(v)     ((v).first + (v).size)
#define st_vec_to_array(v) ((v).first)

// Internal structure. Do not use it, please.
#define st_vec_reserve_impl(v, need) {                                         \
  if (need > (v).capacity) {                                                   \
    ST_VEC_EL_T (v) *mem = dl_malloc (need * ST_VEC_EL_SIZE (v));              \
    assert (mem != 0);                                                         \
    memcpy (mem, (v).first, (v).size * ST_VEC_EL_SIZE (v));                    \
    dl_free ((v).first, (v).capacity * ST_VEC_EL_SIZE (v));                    \
    (v).capacity = need;                                                       \
    (v).first = mem;                                                           \
  }                                                                            \
}                                                                              \

#define st_vec_reserve(v, _need) {                                             \
  long need = (_need);                                                         \
  st_vec_reserve_impl (v, need);                                               \
}                                                                              \

// Allocated memory size never decreases!
#define st_vec_resize(v, _need) {                                              \
  long need = (_need);                                                         \
  assert (need >= 0);                                                          \
  st_vec_reserve_impl (v, need);                                               \
  (v).size = need;                                                             \
}                                                                              \

#define st_vec_resize0(v, _need) {                                             \
  long size_old = (v).size;                                                    \
  st_vec_resize (v, _need);                                                    \
  if ((v).size > size_old) {                                                   \
    memset ((v).first + size_old, 0,                                           \
      ((v).size - size_old) * ST_VEC_EL_SIZE (v));                             \
  }                                                                            \
}

#define st_vec_grow(v, _need) {                                                \
  long need = (_need);                                                         \
  assert (need >= 0);                                                          \
  long growed = (v).size;                                                      \
  while (growed < need) {                                                      \
    growed = (growed << 1) + 1;                                                \
  }                                                                            \
  st_vec_reserve_impl (v, growed);                                             \
  (v).size = need;                                                             \
}                                                                              \

#define st_vec_pb(v, _val) {                                                   \
  if ((v).capacity == (v).size) {                                              \
    st_vec_reserve (v, ((v).capacity << 1) + 1);                               \
  }                                                                            \
  (v).first[(v).size++] = (_val);                                              \
}                                                                              \

#define st_vec_push_back(v, _val) st_vec_pb (v, _val)

#define st_vec_for(i, it, v) {                                                 \
  long i = 0;                                                                  \
  ST_VEC_EL_PTR(v) it = (v).first;                                             \
  for (; i != (v).size; ++i, ++it) {                                           \

#define st_vec_for_i(i, v) {                                                   \
  long i = 0;                                                                  \
  for (; i != (v).size; ++i) {                                                 \

#define st_vec_for_it(it, v) {                                                 \
  ST_VEC_EL_PTR(v) it = (v).first;                                             \
  ST_VEC_EL_PTR(v) it##_last = (v).first + (v).size;                           \
  for (; it != it##_last; ++it) {                                              \

