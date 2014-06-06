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

#ifndef __KDB_ISEARCH_BINLOG_H__
#define __KDB_ISEARCH_BINLOG_H__

#include "kdb-binlog-common.h"

#ifndef	LOGS_SCHEMA_BASE
#define	LOGS_SCHEMA_BASE	0x21090000
#endif

#define	LOGS_SCHEMA_V1	0x21090101

//#define FN 5 //default
#define FN 4
#define LEV_LOGS_ADD_EVENT 0x7be18da3

#define LEV_LOGS_CREATE_TYPE 0x182f3a28
#define LEV_LOGS_ADD_FIELD 0x2716d72a

#define LEV_LOGS_SET_COLOR 0x75a3bc00

#pragma	pack(push,4)

struct lev_logs_create_type {
  lev_type_t type;
  short text_len;
  char text[1];
};

struct lev_logs_add_field {
  lev_type_t type;
  short text_len;
  char text[1];
};

struct lev_logs_add_event {
  lev_type_t type;
  int std_val[FN - 1];
  short text_len;
  char text[1];
};

struct lev_logs_set_color {
  lev_type_t type;
  long long field_value;
  int cnt;
  int and_mask;
  int xor_mask;
};

#pragma	pack(pop)

#endif
