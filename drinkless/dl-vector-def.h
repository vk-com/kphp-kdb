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

    Copyright 2010-2013 Vkontakte Ltd
              2010-2013 Arseny Smirnov
              2010-2013 Aliaksei Levin
*/

#pragma once
#include "dl-comparable.h"

#define vector(x,y)        vector_(x,y)
#define vector_init(y)     vector_init_(y)
#define vector_pb(y,z)     vector_pb_(y,z)
#define vector_zpb(y,z)    vector_zpb_(y,z)
#define vector_resize(y,z) vector_resize_(y,z)
#define vector_pack(y)     vector_pack_(y)
#define vector_back(y)     vector_back_(y)
#define vector_free(y)     vector_free_(y)

#define vector_hpb(x,y,z)  vector_hpb_(x, y, z)
#define vector_hpop(x,y)   vector_hpop_(x, y)

#define VECTOR_DEFAULT_SIZE 1
#define vector_(x,y) int y ## _size, y ## _buf; x *y
#define vector_init_(y) y ## _size = 0, y ## _buf = VECTOR_DEFAULT_SIZE, y = dl_malloc (y ## _buf * sizeof(y[0]))
#define vector_pb_(y,z) {                                                 \
  if (y ## _size >= y ## _buf) {                                          \
    int _nbuf = 2 * y ## _buf + 1;                                        \
    y = dl_realloc (y, _nbuf * sizeof (y[0]), y ## _buf * sizeof (y[0])); \
    y ## _buf = _nbuf;                                                    \
  }                                                                       \
  y[y ## _size++] = z;                                                    \
}
#define vector_zpb_(y,z) ({                                               \
  int need = y ## _size + z;                                              \
  if (need > y ## _buf) {                                                 \
    int _nbuf = 2 * y ## _buf + 1;                                        \
    if (need > _nbuf) {                                                   \
      _nbuf = need;                                                       \
    }                                                                     \
    y = dl_realloc (y, _nbuf * sizeof (y[0]), y ## _buf * sizeof (y[0])); \
    y ## _buf = _nbuf;                                                    \
  }                                                                       \
  __typeof (y) res = y + y ## _size;                                      \
  y ## _size += z;                                                        \
  res;                                                                    \
})
#define vector_resize_(y,z) {                                             \
  y = dl_realloc (y, z * sizeof (y[0]), y ## _buf * sizeof (y[0]));       \
  y ## _buf = z;                                                          \
  if (y ## _size > y ## _buf) {                                           \
    y ## _size = y ## _buf;                                               \
  }                                                                       \
}
#define vector_pack_(y) {                                                      \
    y = dl_realloc (y, y ## _size * sizeof (y[0]), y ## _buf * sizeof (y[0])); \
    y ## _buf = y ##_size;                                                     \
}
#define vector_back_(y) (y[y ## _size - 1])
#define vector_free_(y) dl_free (y, y ## _buf * sizeof(y[0]))

#define vector_swap(a, b, c) (c) = (a), (a) = (b), (b) = (c)
#define vector_cmp(a, b, x) DL_SCMP(x)(a, b)

#define vector_hpb_(x, y, z) ({                                           \
  int i = y ## _size, pi;                                                 \
  vector_pb (y, z);                                                       \
  __typeof (*y) tmp;                                                      \
  while (i > 0 && vector_cmp (y[pi = (i - 1) / 2], y[i], x)) {            \
    vector_swap (y[i], y[pi], tmp);\
    i = pi;\
  } \
})

#define vector_hpop_(x, y) ({\
  assert (y ## _size > 0);\
  __typeof (*y) res = y[0], tmp;\
  y ## _size--;\
  vector_swap (y[0], y[y ## _size], tmp);\
  int i = 0, bi = 0;\
  do {\
    i = bi;\
    int ni = 2 * i + 1; \
    if (ni < y ## _size && vector_cmp (y[bi], y[ni], x)) {\
      bi = ni;\
    }\
    ni++;\
    if (ni < y ## _size && vector_cmp (y[bi], y[ni], x)) {\
      bi = ni;\
    }\
    vector_swap (y[i], y[bi], tmp);\
  } while (i != bi);\
  \
  res;\
})
