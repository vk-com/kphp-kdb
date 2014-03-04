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
 * Author:  Anton  Timofeev    (atimofeev@vkontakte.ru)
 * Created: 28.04.2012
 */

#include <assert.h>
#include <string.h>

#include "st-hash.h"

hash_t st_hash (int n, void const *data) {
  assert (n >= 0);

  char const *s = data;
  hash_t h = 239 * ST_HASH_PRIME_UL + n;
  while (n--) {
    h = h * ST_HASH_PRIME_UL + *s++;
  }
  return h;
}

hash_t st_hash_str (const char *s) {
  return st_hash (strlen (s), s);
}
