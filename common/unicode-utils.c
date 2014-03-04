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

    Copyright 2013 Vkontakte Ltd
              2013 Arseny Smirnov
              2013 Aliaksei Levin
*/

#define FILE_OFFSET_BITS 64

#include <assert.h>
#include <stdlib.h>
#include <string.h>

#include "unicode-utils.h"
#include "unicode-utils-auto.h"
#include "utf8_utils.h"

/* Search generated ranges for specified character */
static int binary_search_ranges (const int *ranges, int r, int code) {
  if ((unsigned int)code > 0x10ffff) {
    return 0;
  }
  
  int l = 0;
  while (l < r) {
    int m = ((l + r + 2) >> 2) << 1;
    if (ranges[m] <= code) {
      l = m;
    } else {
      r = m - 2;
    }
  }

  int t = ranges[l + 1];
  if (t < 0) {
    return code - ranges[l] + (~t);
  }
  if (t <= 0x10ffff) {
    return t;
  }
  switch (t - 0x200000) {
    case 0:
      return (code & -2);
    case 1:
      return (code | 1);
    case 2:
      return ((code - 1) | 1);
    default:
      assert (0);
      exit (1);
  }
}

/* Convert character to upper case */
int unicode_toupper (int code) {
  if ((unsigned int)code < (unsigned int)TABLE_SIZE) {
    return to_upper_table[code];
  } else {
    return binary_search_ranges (to_upper_table_ranges, to_upper_table_ranges_size, code);
  }
}

/* Convert character to lower case */
int unicode_tolower (int code) {
  if ((unsigned int)code < (unsigned int)TABLE_SIZE) {
    return to_lower_table[code];
  } else {
    return binary_search_ranges (to_lower_table_ranges, to_lower_table_ranges_size, code);
  }
}

/* Convert character to title case */
int unicode_totitle (int code) {
  if ((unsigned int)(code - 0x1c4) < 9u ||
      (unsigned int)(code - 0x1f1) < 3u) {
    return ((code * 685) >> 11) * 3;
  }

  return unicode_toupper (code);
}
