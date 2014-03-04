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
 * Author:  Sergey Kopeliovich (Burunduk30@gmail.com)
 * Created: 12.06.2012
 */

#pragma once

#include <assert.h>
#include <math.h>

#include "st-utils.h"

#define DECLARE_NUMERIC_FUNCTIONS(type, suffix)  \
  type st_ln##suffix (type x);                   \
  type st_min_element##suffix (int n, type *a);  \
  type st_max_element##suffix (int n, type *a);  \
  type st_accumulate##suffix (int n, type *a);

#define DEFINE_NUMERIC_FUNCTIONS(type, suffix)   \
  type st_log2##suffix (type x) {                \
    return log (x) * 1.442695040888963407359925; \
  }                                              \
  type st_min_element##suffix (int n, type *a) { \
    assert(n > 0);                               \
    type res = a[0];                             \
    int i;                                       \
    for (i = 1; i < n; i++) {                    \
      st_relax_min(res, a[i]);                   \
    }                                            \
    return res;                                  \
  }                                              \
  type st_max_element##suffix (int n, type *a) { \
    assert(n > 0);                               \
    type res = a[0];                             \
    int i;                                       \
    for (i = 1; i < n; i++) {                    \
      st_relax_max(res, a[i]);                   \
    }                                            \
    return res;                                  \
  }                                              \
  type st_accumulate##suffix (int n, type *a) {  \
    type res = 0;                                \
    int i;                                       \
    for (i = 0; i < n; i++) {                    \
      res += a[i];                               \
    }                                            \
    return res;                                  \
  }

DECLARE_NUMERIC_FUNCTIONS(int, )
DECLARE_NUMERIC_FUNCTIONS(long long, ll)
DECLARE_NUMERIC_FUNCTIONS(double, f)
DECLARE_NUMERIC_FUNCTIONS(long double, lf)
