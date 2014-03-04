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

    Copyright 2010-2013 Vkontakte Ltd
              2010-2013 Arseny Smirnov
              2010-2013 Aliaksei Levin
*/

#ifndef _UTILS_H_
#define _UTILS_H_

#include <fcntl.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <unistd.h>
#include <assert.h>

#include "../common/utf8_utils.h"

#define MAX_MEMORY 2000000000l

#ifndef HASH_MUL
#define HASH_MUL (999983)
#endif

#define WRITE_LONG(s, x) *(long long*)(s) = x; s += sizeof (long long)
#define WRITE_INT(s, x) *(int*)(s) = x; s += sizeof (int)
#define WRITE_SHORT(s, x) *(short*)(s) = x; s += sizeof (short)

#define READ_LONG(s, x) x = *(long long*)(s); s += sizeof (long long)
#define READ_INT(s, x) x = *(int*)(s); s += sizeof (int)

typedef int ll;

void *qmalloc (size_t x);
void *qmalloc0 (size_t x);

void *qrealloc (void *p, size_t x, size_t old);

void qfree (void *p, size_t x);

long get_memory_used (void);

typedef struct {
  int *mem;
  int mx, n;
} vector;

void bayes_string_to_utf8 (unsigned char *s, int *v);

#endif
