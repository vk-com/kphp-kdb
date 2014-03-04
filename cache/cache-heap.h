/*
    This file is part of VK/KittenPHP-DB-Engine.

    VK/KittenPHP-DB-Engine is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 2 of the License, or
    (at your option) any later version.

    VK/KittenPHP-DB-Engine is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with VK/KittenPHP-DB-Engine.  If not, see <http://www.gnu.org/licenses/>.

    This program is released under the GPL with the additional exemption
    that compiling, linking, and/or using OpenSSL is allowed.
    You are free to remove this exemption from derived works.

    Copyright 2012-2013 Vkontakte Ltd
              2012-2013 Anton Maydell
*/

#ifndef __CACHE_HEAP_H__
#define __CACHE_HEAP_H__

//#define TEST

#define CACHE_MAX_HEAP_SIZE 100000
typedef struct {
  int (*compare)(const void *, const void *);
  int size, max_size;
  void *H[CACHE_MAX_HEAP_SIZE+1];
} cache_heap_t;

void cache_heap_insert (cache_heap_t *self, void *E);
void *cache_heap_pop (cache_heap_t *self);
int cache_heap_sort (cache_heap_t *self);

#ifdef TEST
void cache_heap_test (void);
#endif

#endif

