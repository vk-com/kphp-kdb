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

#include "dl-vector-config.h"

typedef struct {
  TA *v;
  int v_size, v_buf;
} TNAME;

int DL_ADD_SUFF (TNAME, size) (TNAME *v);

void DL_ADD_SUFF (TNAME, pb) (TNAME *v, TA z);
TA* DL_ADD_SUFF (TNAME, zpb) (TNAME *v, int z);
TA DL_ADD_SUFF (TNAME, back) (TNAME *v);

void DL_ADD_SUFF (TNAME, resize) (TNAME *v, int z);
void DL_ADD_SUFF (TNAME, pack) (TNAME *v);

void DL_ADD_SUFF (TNAME, init) (TNAME *v);
void DL_ADD_SUFF (TNAME, free) (TNAME *v);

