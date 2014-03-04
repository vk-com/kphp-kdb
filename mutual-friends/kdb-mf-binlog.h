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

    Copyright 2010-2013	Vkontakte Ltd
              2010-2013 Arseny Smirnov
              2010-2013 Aliaksei Levin
*/
#ifndef __KDB_MF_BINLOG_H__
#define __KDB_MF_BINLOG_H__

#include "kdb-binlog-common.h"

#include <stddef.h>

#pragma	pack(push,4)

#ifndef	MF_SCHEMA_BASE
#define	MF_SCHEMA_BASE	0xafe60000
#endif

#define	MF_SCHEMA_V1	0xafe60101

#define LEV_MF_ADD_EXCEPTION	  0x6cab2a02
#define LEV_MF_DEL_EXCEPTION	  0xafc58234
#define LEV_MF_CLEAR_EXCEPTIONS	0xb734c523

struct lev_mf_del_exception {
  lev_type_t type;
  int user_id, friend_id;
};

struct lev_mf_add_exception {
  lev_type_t type;
  int user_id, friend_id;
};

struct lev_mf_clear_exceptions {
  lev_type_t type;
  int user_id;
};

#pragma	pack(pop)

#endif
