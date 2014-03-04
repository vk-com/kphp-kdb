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

#if (!defined (TA) || !defined (TB) || !defined (TNAME))
  #error TA or TB is not declared
#endif

#define DL_PAIR_WAS

#include "dl-default.h"
#include "dl-comparable.h"

#ifndef CMP_A
  #define CMP_A DL_CMP(TA)
#endif

#ifndef CMP_B
  #define CMP_B DL_CMP(TB)
#endif

#ifndef EQ_A
  #define EQ_A DL_EQ(TA)
#endif

#ifndef EQ_B
  #define EQ_B DL_EQ(TB)
#endif

