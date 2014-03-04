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

#include "dl-default.h"

#define DL_HASH(T) DL_ADD_SUFF (dl_hash, T)
#define DL_HASHI(T) DL_ADD_SUFF (dl_hashi, T)

#define dl_hash_int(x) ((x) * 2092391717 + 1)
#define dl_hash_inti(x) (x)
#define dl_hash_ll(x) ((x) * 759203823431245141ll + 1)
#define dl_hash_lli(x) ((int)(x))
#define dl_hash_dl_string(ts) ({ \
  long long _h = 3213211;        \
  const char *_s = ts;           \
  while (*_s) {                  \
    _h = _h * 999983 + *_s++;    \
  }                              \
  _h;                            \
})
