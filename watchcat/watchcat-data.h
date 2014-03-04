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

    Copyright 2011-2013 Vkontakte Ltd
              2011-2013 Arseny Smirnov
              2011-2013 Aliaksei Levin
*/

#pragma once

#include "kdb-data-common.h"
#include "net-connections.h"

#include <stddef.h>

#include "utils.h"

#include "dl-utils.h"

typedef struct {
  int q_id;
  long long w_id;
} addr;

#define MAX_ANS 100000

extern long watchcats_memory, keys_memory;
extern int watchcats_cnt, keys_cnt;

extern addr ans[MAX_ANS];
extern int ans_n;

void free_by_time (int mx);
char *gen_addrs (char *s);
void init_all (void);
void free_all (void);
void subscribe_watchcat (long long id, char *s, int q_id, int timeout);

#define MAX_MEMORY 2000000000
