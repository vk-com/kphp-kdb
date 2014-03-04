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

/* DL_PAIR type
 *
 * defines needed:
 *   TA, TB --  pair <TA, TB>
 *   CMP_A, CMP_B -- to compare TA and TB. By default DL_CMP(TA)is used
 *   TNAME -- will be name of type
 *
 * after:
 *   DL_CMP(TNAME) and DL_QCMP(TNAME) functions will be defined | + COMPARABLE
 */


/* TODO:
 *   -define TB if TA declared
 *
 */

#include "dl-pair.h"

#if !defined (DL_COMPARABLE_OFF)

  int DL_QCMP(TNAME)(const void *_a, const void *_b) {
    int x = CMP_A(((TNAME *) _a)->x, ((TNAME *) _b)->x);
    return x ? x : CMP_B(((TNAME *) _a)->y, ((TNAME *) _b)->y);
  }

  int DL_CMP(TNAME)(TNAME a, TNAME b) {
    int x = CMP_A(a.x, b.x);
    return x ? x : CMP_B(a.y, b.y);
  }

  int DL_EQ(TNAME)(TNAME a, TNAME b) {
    return EQ_A(a.x, b.x) && EQ_B(a.y, b.y);
  }

#endif
