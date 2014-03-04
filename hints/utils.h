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

#pragma once

#include <fcntl.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <unistd.h>
#include <assert.h>

#ifndef NOHINTS
#  define HINTS
#endif

#include "dl-utils.h"

#define MAX_MEMORY 2000000000l

long get_memory_used (void);

#ifdef HINTS
#include "../common/utf8_utils.h"
#include "utf8_utils.h"

#define MAX_NAME_SIZE 4096

long long *gen_hashes (char *s);
int *prepare_str_UTF8 (int *x);
char *prepare_str (char *x);
char *clean_str (char *x);

typedef struct {
  int *mem;
  int mx, n;
} vector;

void vct_init (vector *v);
void vct_add (vector *v, int x);
void vct_free (vector *v);
int vct_back (vector *v);
void vct_set_add (vector *v, int val);

#endif
