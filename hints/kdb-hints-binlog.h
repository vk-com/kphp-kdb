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

#ifndef __KDB_HINTS_BINLOG_H__
#define __KDB_HINTS_BINLOG_H__

#include "kdb-binlog-common.h"

#pragma pack(push,4)

#ifndef HINTS_SCHEMA_BASE
#define HINTS_SCHEMA_BASE 0x4fad0000
#endif

#define HINTS_SCHEMA_V1 0x4fad0101

#define LEV_HINTS_ADD_USER_OBJECT                   0x39e4fdfc
#define LEV_HINTS_SET_USER_OBJECT_TYPE              0x7aef0000
#define LEV_HINTS_ADD_OBJECT_TEXT                   0x1124d8cc
#define LEV_HINTS_SET_OBJECT_TYPE                   0x6cae0000
#define LEV_HINTS_DEL_USER_OBJECT                   0x3dada300
#define LEV_HINTS_DEL_OBJECT_TEXT                   0x5be4fd00
#define LEV_HINTS_SET_USER_INFO                     0x4ad43500
#define LEV_HINTS_SET_USER_OBJECT_RATING            0x35a14000
#define LEV_HINTS_INCREMENT_USER_OBJECT_RATING      0x4f45ab00
#define LEV_HINTS_INCREMENT_USER_OBJECT_RATING_CHAR 0x29ad0000
#define LEV_HINTS_NULLIFY_USER_RATING               0x4c47f000
#define LEV_HINTS_SET_USER_RATING_STATE             0x2456ea00
#define LEV_HINTS_USER_OBJECT_WINNER                0x63421057
#define LEV_HINTS_ADD_USER_OBJECT_RATING            0x3712ec20

struct lev_hints_add_user_object {
  lev_type_t type;
  int user_id;
  int object_id;
  short text_len;
  unsigned char object_type;
  char text[0];
};

struct lev_hints_set_user_object_type {
  lev_type_t type;
  int user_id;
  int object_id;
};

struct lev_hints_add_object_text {
  lev_type_t type;
  int object_id;
  short text_len;
  unsigned char object_type;
  char text[0];
};

struct lev_hints_set_object_type {
  lev_type_t type;
  int object_id;
};

struct lev_hints_del_user_object {
  lev_type_t type;
  int user_id;
  int object_id;
};

struct lev_hints_del_object_text {
  lev_type_t type;
  int object_id;
};

struct lev_hints_set_user_info {
  lev_type_t type;
  int user_id;
};

struct lev_hints_set_user_object_rating {
  lev_type_t type;
  int user_id;
  int object_id;
  float val;
};

struct lev_hints_increment_user_object_rating {
  lev_type_t type;
  int user_id;
  int object_id;
  int cnt;
};

struct lev_hints_increment_user_object_rating_char {
  lev_type_t type;
  int user_id;
  int object_id;
};

struct lev_hints_nullify_user_rating {
  lev_type_t type;
  int user_id;
};

struct lev_hints_set_user_rating_state {
  lev_type_t type;
  int user_id;
};

struct lev_hints_user_object_winner {
  lev_type_t type;
  int user_id;
  int winner_id;
  short losers_cnt;
  unsigned char object_type;
  int losers[0];
};

struct lev_hints_add_user_object_rating {
  lev_type_t type;
  int user_id;
  int object_id;
  short text_len;
  unsigned char object_type;
  char text[0];
};

#pragma pack(pop)

#endif
