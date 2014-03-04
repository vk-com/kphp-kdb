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

#include <stddef.h>

#include "dl.h"

#define FIDS_LIMIT 1000000

#define SIMILARITY_MAGIC 0x3ad3f238

#pragma pack(push,4)

typedef struct {
  int magic;
  int created_at;
  int objs_type;
  int objs_limit;
  int hints_limit;
  int file_type;
  int total_scores;
  int has_names;

  int reserved[24];

  vector_int fids;
  float *f_mul;
  union {
    float *CC;
    score *C;
  };
  int *C_index;

  union {
    map_int_int fid_id;
    map_string_int fid_id_str;
  };
  char *fid_names;
  char **fid_names_begins;
  int fid_names_cnt;
} similarity_index_header;

#pragma pack(pop)

#pragma pack(push,2)

typedef struct {
  short len;
  int id;
} user_dump;

#pragma pack(pop)

