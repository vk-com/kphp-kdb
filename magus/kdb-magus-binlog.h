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

#ifndef __KDB_MAGUS_BINLOG_H__
#define __KDB_MAGUS_BINLOG_H__

#include "kdb-binlog-common.h"

#include <stddef.h>

#pragma pack(push,4)

#ifndef MAGUS_SCHEMA_BASE
#define MAGUS_SCHEMA_BASE 0x7a9c0000
#endif

#define MAGUS_SCHEMA_V1 0x7a9c0101

#define LEV_MAGUS_ADD_EXCEPTION 0x3bcdaed0
#define LEV_MAGUS_ADD_EXCEPTION_STRING 0x3fce4200

struct lev_magus_add_exception {
  lev_type_t type;
  int user_id, fid;
};

struct lev_magus_add_exception_string {
  lev_type_t type;
  int user_id;
  short text_len;
  char text[0];
};

#pragma pack(pop)

#endif
