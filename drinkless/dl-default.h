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

typedef long long ll;
typedef unsigned int uint;
typedef unsigned long long ull;
typedef void *vptr;
typedef char *dl_string;
typedef int empty_type[0];

#define DL_ADD_SUFF_(A, B) A##_##B
#define DL_ADD_SUFF(A, B) DL_ADD_SUFF_(A, B)

#define DL_CAT_(A, B) A##B
#define DL_CAT(A, B) DL_CAT_(A, B)
#define DL_STR(A) DL_STR_(A)
#define DL_STR_(A) # A
#define DL_EMPTY(A...)

#define R(l, r) ((l) + rand() % ((r) - (l) + 1))

#define ON 123456
#define OFF 789012


/* Hash table */
#define OPEN 312421
#define CHAIN 784392


#define likely(x) __builtin_expect ((x),1)
#define unlikely(x) __builtin_expect ((x),0)
