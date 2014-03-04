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

    Copyright 2011-2012 Vkontakte Ltd
              2011-2012 Arseny Smirnov
              2011-2012 Aliaksei Levin
*/

#pragma once

#include "kdb-binlog-common.h"

#ifndef SUPPORT_SCHEMA_BASE
#define SUPPORT_SCHEMA_BASE 0x1fec0000
#endif

#define	SUPPORT_SCHEMA_V1	0x1fec0101

#define LEV_SUPPORT_ADD_ANSWER 0x1c930000
#define LEV_SUPPORT_SET_MARK 0x23cda4ef
#define LEV_SUPPORT_DELETE_ANSWER 0x3ca21bef


#pragma	pack(push,4)

struct lev_support_add_answer {
  lev_type_t type;
  int user_id;
  int agent_id;
  int mark;
  char question_with_answer[0];
};

struct lev_support_set_mark {
  lev_type_t type;
  int user_id;
  int mark;
};

struct lev_support_delete_answer {
  lev_type_t type;
  int user_id;
};

#pragma	pack(pop)

