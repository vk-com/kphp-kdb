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
 * Created: 22.03.2012
 */

#define _FILE_OFFSET_BITS 64

#include <string.h>

#include "st-hash-set.h"

#define LIST(h, x) h[x % ST_HASH_SET_SIZE]

DEFINE_LIST_NEQ (hash_t, list_hash)

void hash_set_add (hash_set_t h, hash_t x) {
  list_hash_add (&LIST (h, x), x);
}

void hash_set_del (hash_set_t h, hash_t x) {
  list_hash_delete (&LIST (h, x), x);
}

bool hash_set_contains (hash_set_t h, hash_t x) {
  return list_hash_contains (LIST (h, x), x);
}

void hash_set_init (hash_set_t h) {
  int i;
  for (i = 0; i < ST_HASH_SET_SIZE; i++) {
    h[i] = 0;
  }
}

void hash_set_clear (hash_set_t h) {
  int i;
  for (i = 0; i < ST_HASH_SET_SIZE; i++) {
    list_hash_clear (&h[i]);
    assert (h[i] == 0);
  }
}
