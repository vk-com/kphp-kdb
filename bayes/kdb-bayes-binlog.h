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

#ifndef __KDB_BAYES_BINLOG_H__
#define __KDB_BAYES_BINLOG_H__

#include "kdb-binlog-common.h"

#pragma	pack(push,4)

#ifndef	BAYES_SCHEMA_BASE
#define	BAYES_SCHEMA_BASE	0x3ad50000
#endif

#define	BAYES_SCHEMA_V1	0x3ad50101

#define LEV_BAYES_SET			        0x4323bcd0
#define LEV_BAYES_BLACK_LIST			0x1b1acc18

struct lev_bayes_set {
  lev_type_t type;
  short text_len;
  char text[1];
};

struct lev_bayes_black_list {
  lev_type_t type;
  short text_len;
  char text[1];
};

#pragma	pack(pop)

#endif
