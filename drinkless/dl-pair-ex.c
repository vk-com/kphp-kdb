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

#include <stdio.h>
#include <stdlib.h>
#include "dl-default.h"

/*
 * pair <int, int>
 */

#define TA int
#define TB int
#define TNAME pair_int_int
#include "dl-pair.c"
#include "dl-undef.h"

#define TA pair_int_int
#define TB pair_int_int
#define TNAME pair_p_p
#include "dl-pair.c"
#include "dl-undef.h"

int main (void) {
  pair_int_int pa, pb;
  pa.x = 1;
  pa.y = 2;
  pb.x = 1;
  pb.y = 3;
  fprintf (stderr, "%d (-1)\n", DL_CMP(pair_int_int)(pa, pb));
  fprintf (stderr, "%d (+1)\n", DL_CMP(pair_int_int)(pb, pa));
  fprintf (stderr, "%d (0)\n", DL_CMP(pair_int_int)(pa, pa));

  pair_p_p px, py;
  px.x = pa;
  py.x = pa;
  px.y = pa;
  py.y = pb;

  fprintf (stderr, "%d (-1)\n", DL_CMP(pair_p_p)(px, py));
  fprintf (stderr, "%d (+1)\n", DL_CMP(pair_p_p)(py, px));
  fprintf (stderr, "%d (0)\n", DL_CMP(pair_p_p)(px, px));

  return 0;
}
