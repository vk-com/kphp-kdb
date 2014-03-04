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
              2010-2011 Nikolai Durov
              2010-2011 Andrei Lopatin
              2011-2013 Vitaliy Valtman
              2011-2013 Anton Maydell
*/

#ifndef __KDB_NEWS_BINLOG_H__
#define __KDB_NEWS_BINLOG_H__

#include "kdb-binlog-common.h"

#pragma	pack(push,4)

#ifndef	NEWS_SCHEMA_BASE
#define	NEWS_SCHEMA_BASE	0x53c40000
#endif

#define	NEWS_SCHEMA_USER_V1	0x53c40101
#define	NEWS_SCHEMA_GROUP_V1	0x53c40201
#define	NEWS_SCHEMA_COMMENT_V1	0x53c40301
#define NEWS_SCHEMA_NOTIFY_V1 0x53c40401
#define NEWS_SCHEMA_RECOMMEND_V1 0x53c40501


#define	LEV_NEWS_USERDEL	0x31bec562
#define	LEV_NEWS_PRIVACY	0x19a841c4
#define	LEV_NEWS_ITEM		0x6b5dfe00
#define	LEV_NEWS_COMMENT	0x378a3d00
#define	LEV_NEWS_PLACEDEL	0x5012ac00
#define	LEV_NEWS_HIDEITEM	0x42fd3400
#define	LEV_NEWS_SHOWITEM	0x2d39e600
#define LEV_NEWS_NOTIFY 0x38aba100
#define	LEV_NEWS_HIDEUSERITEM	0x6aa32f00
#define	LEV_NEWS_SHOWUSERITEM	0x7ba28700
#define LEV_BOOKMARK_INSERT 0x247faa00
#define LEV_BOOKMARK_DELETE 0x5a78fc00
#define LEV_NEWS_RECOMMEND 0x1c692200
#define LEV_NEWS_SET_RECOMMEND_RATE 0xe8752f00

struct lev_userdel {
  lev_type_t type;
  int user_id;
};

struct lev_privacy {
  lev_type_t type;
  int user_id;
  int privacy;
};

struct lev_news_item {
  lev_type_t type;
  int user_id;
  int user;
  int group;
  int owner;
  int place;
  int item;
};

struct lev_news_comment {
  lev_type_t type;
  int user;
  int group;
  int owner;
  int place;
  int item;
};

struct lev_news_place_delete {
  lev_type_t type;
  int owner;
  int place;
};

struct lev_news_comment_hide {
  lev_type_t type;
  int owner;
  int place;
  int item;
};

struct lev_news_user_comment_hide {
  lev_type_t type;
  int owner;
  int place;
  int item;
  int user_id;
};

struct lev_bookmark_insert {
  lev_type_t type;
  int user_id;
  int owner;
  int place;
};


struct lev_user_generic {
  lev_type_t type;
  int user_id;
  int a, b, c, d, e;
};

struct lev_news_notify {
  lev_type_t type;
  int user_id;
  int user;
  int owner;
  int place;
  int item;
};

struct lev_news_recommend {
  lev_type_t type;
  int user_id;
  int owner;
  int place;
  int action;
  int item;
  //float rate;
  int item_creation_time;
};

struct lev_news_set_recommend_rate {
  lev_type_t type;
  int action;
  double rate;
};

#pragma	pack(pop)

#endif
