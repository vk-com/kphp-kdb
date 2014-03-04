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

    Copyright 2012-2013	Vkontakte Ltd
              2012      Sergey Kopeliovich <Burunduk30@gmail.com>
              2012      Anton Timofeev <atimofeev@vkontakte.ru>
*/
/**
 * Author:  Sergey Kopeliovich (Burunduk30@gmail.com)
 *          Anton  Timofeev    (atimofeev@vkontakte.ru)
 * Created: 22.03.2012
 */

#pragma once

#include "kdb-binlog-common.h"

#ifndef ANTISPAM_SCHEMA_BASE
#  define ANTISPAM_SCHEMA_BASE 0x91a70000
#endif
#define ANTISPAM_SCHEMA_V1     0x91a70101

// Add/Set pattern log events interval:
// [LEV_ANTISPAM_***_PATTERN + 1, LEV_ANTISPAM_***_PATTERN + MAX_PATTERN_LEN]
// Pattern string length stored in last 16 bits of lev_type_t
// with '\0' at the end inclusive
#define LEV_ANTISPAM_ADD_PATTERN_V1 0xe05d0000
#define LEV_ANTISPAM_SET_PATTERN_V1 0x1fa20000
#define LEV_ANTISPAM_ADD_PATTERN_V2 0x67c80000
#define LEV_ANTISPAM_SET_PATTERN_V2 0xd9ad0000
// Delete pattern log event
#define LEV_ANTISPAM_DEL_PATTERN 0x46bf9d3a

#pragma pack(push,4)

typedef unsigned int   ip_t;
typedef unsigned int   uahash_t;
typedef unsigned short flags_t;

// version1: 1 byte for flags
typedef struct antispam_pattern_v1 {
  int      id;
  ip_t     ip;
  uahash_t uahash;
  unsigned char flags;
} antispam_pattern_v1_t;

typedef struct lev_antispam_add_pattern_v1 {
  lev_type_t type;
  antispam_pattern_v1_t p;

  // length is stored in last 16 bits of 'type'
  // with '\0' at the end exclusive
  char str[0];
} lev_antispam_add_pattern_v1_t;

// version2: 2 bytes for flags
typedef struct antispam_pattern {
  int      id;
  ip_t     ip;     // 0 = *
  uahash_t uahash; // 0 = *

  // flags taken from sql dump as is:
  //   (~flags & 48) --> no_simplify
  //   ( flags & 16) --> sp_full_simplify
  //   ( flags & 32) --> sp_simplify
  flags_t  flags;
} antispam_pattern_t;

typedef struct lev_antispam_add_pattern_v2 {
  lev_type_t type;
  antispam_pattern_t pattern;

  // length is stored in last 16 bits of 'type'
  // with '\0' at the end exclusive
  char str[0];
} lev_antispam_add_pattern_v2_t;

typedef struct lev_antispam_del_pattern {
  lev_type_t type;
  int id;
} lev_antispam_del_pattern_t;

#pragma pack(pop)
