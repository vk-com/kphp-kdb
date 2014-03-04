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

#include "dl-utils.h"

typedef struct {
  int *perm_first, *perm_middle, *perm_last;
  int hash_st, hash_mul;
  int val_n;
  int rand_n;
} dl_crypto;

void dl_crypto_init (dl_crypto *cr, int val_n, int rand_n, int hash_st, int hash_mul, int seed);
void dl_crypto_encode (dl_crypto *cr, char *s, char *t);
void dl_crypto_decode (dl_crypto *cr, char *s, char *t);
void dl_crypto_free (dl_crypto *cr);
