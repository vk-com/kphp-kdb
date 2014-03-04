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

#define _FILE_OFFSET_BITS 64

/* DL_VECTOR type
 *
 * defines needed:
 *   TA --  vector <TA>
 *   TNAME -- will be name of type
 *
 * after:
 *   v
 */


/* TODO:
 *   -define TB if TA declared
 *
 */

#include "dl-vector.h"

int DL_ADD_SUFF (TNAME, size) (TNAME *v) {
  return v->v_size;
}

void DL_ADD_SUFF (TNAME, pb) (TNAME *v, TA z) {
  vector_pb (v->v, z);
}

TA* DL_ADD_SUFF (TNAME, zpb) (TNAME *v, int z) {
  return vector_zpb (v->v, z);
}

TA DL_ADD_SUFF (TNAME, back) (TNAME *v) {
  return vector_back (v->v);
}

void DL_ADD_SUFF (TNAME, init) (TNAME *v) {
  vector_init (v->v);
}

void DL_ADD_SUFF (TNAME, resize) (TNAME *v, int z) {
  vector_resize (v->v, z);
}

void DL_ADD_SUFF (TNAME, pack) (TNAME *v) {
  vector_pack (v->v);
}

void DL_ADD_SUFF (TNAME, free) (TNAME *v) {
  vector_free (v->v);
}
