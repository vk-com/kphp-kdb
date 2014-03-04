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

#define DL_CMP(T) DL_ADD_SUFF (dl_cmp, T)
#define DL_SCMP(T) DL_ADD_SUFF (dl_scmp, T)
#define DL_QCMP(T) DL_ADD_SUFF (dl_qcmp, T)
#define DL_EQ(T) DL_ADD_SUFF (dl_eq, T)

#define dl_scmp_std(x, y) (x < y)

#define dl_scmp_int(x, y) dl_scmp_std(x, y)
#define dl_scmp_double(x, y) dl_scmp_std(x, y)
#define dl_scmp_char(x, y) dl_scmp_std(x, y)
#define dl_scmp_ll(x, y) dl_scmp_std(x, y)
#define dl_scmp_ull(x, y) dl_scmp_std(x, y)
#define dl_scmp_uint(x, y) dl_scmp_std(x, y)
#define dl_scmp_vptr(x, y) dl_scmp_std((long)x, (long)y)
#define dl_scmp_string(x, y) (strcmp (x, y) < 0)

#define dl_cmp_std(x, y) (x < y ? -1 : x > y)

#define dl_cmp_int(x, y) dl_cmp_std(x, y)
#define dl_cmp_double(x, y) dl_cmp_std(x, y)
#define dl_cmp_char(x, y) dl_cmp_std(x, y)
#define dl_cmp_ll(x, y) dl_cmp_std(x, y)
#define dl_cmp_ull(x, y) dl_cmp_std(x, y)
#define dl_cmp_uint(x, y) dl_cmp_std(x, y)
#define dl_cmp_vptr(x, y) dl_cmp_std((long)x, (long)y)
#define dl_cmp_string(x, y) (strcmp (x, y))

#define dl_qcmp_int(x, y) dl_cmp_int(*(int *)x, *(int *)y)
#define dl_qcmp_double(x, y) dl_cmp_double(*(double *)x, *(double *)y)
#define dl_qcmp_char(x, y) dl_cmp_char(*(char *)x, *(char *)y)
#define dl_qcmp_ll(x, y) dl_cmp_ll(*(ll *)x, *(ll *)y)
#define dl_qcmp_ull(x, y) dl_cmp_ull(*(ull *)x, *(ull *)y)
#define dl_qcmp_uint(x, y) dl_cmp_uint(*(uint *)x, *(uint *)y)
#define dl_qcmp_string(x, y) dl_cmp_int(*(string *)x, *(string *)y)

#define dl_eq_std(x, y) ((x) == (y))

#define dl_eq_int(x, y) dl_eq_std(x, y)
#define dl_eq_double(x, y) dl_eq_std(x, y)
#define dl_eq_char(x, y) dl_eq_std(x, y)
#define dl_eq_ll(x, y) dl_eq_std(x, y)
#define dl_eq_ull(x, y) dl_eq_std(x, y)
#define dl_eq_uint(x, y) dl_eq_std(x, y)
#define dl_eq_vptr(x, y) dl_eq_std(x, y)
#define dl_eq_string(x, y) (!strcmp (x, y))
