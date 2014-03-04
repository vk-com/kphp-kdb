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

#pragma once

#ifndef hash_t
typedef unsigned long long hash_t;
#define hash_t hash_t
#endif

// Default capacity for hashed containers as hash_map, hash_set and etc.
#define ST_HASH_SET_SIZE ((int)2e6 + 3)

// Useful prime numbers for hashing. But not so hackneyed as 1e9+7, 1e9+9
#define ST_HASH_PRIME_UL  1083741833UL // multiplier (for HASH computing)
#define ST_HASH_PRIME_ULL 56235515617499ULL // multiplier (for HASH computing)

#define HASH_JOIN(h, a) (h = HASH_ITER(h, a))
#define HASH_ITER(h, a) (ST_HASH_PRIME_ULL * (hash_t)(h) + (hash_t)(a))
#define HASH_SINGLE(a)       HASH_ITER(a, 3)
#define HASH_DOUBLE(a, b)    HASH_ITER(HASH_SINGLE (a), b)
#define HASH_TRIPLE(a, b, c) HASH_ITER(HASH_DOUBLE (a, b), c)

// to get HASH-value by any object
hash_t st_hash (int size_in_bytes, void const *data);
hash_t st_hash_str (char const *s); // consider '\0' at the end
