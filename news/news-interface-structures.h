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

    Copyright 2013 Vkontakte Ltd
              2013 Vitaliy Valtman
              2013 Anton Maydell
*/

#ifndef __NEWS_INTERFACE_STRUCTURES__
#define __NEWS_INTERFACE_STRUCTURES__

#pragma pack(push,4)

struct tl_news_update {
  int id;
  int type;
  int user;
  int group;
  int owner;
  int place;
  int item;
};

struct tl_news_delete {
  int id;
};

struct tl_news_set_privacy_mask {
  int id;
  int mask;
};

struct tl_news_get_privacy_mask {
  int id;
};

struct tl_news_get_raw_updates {
  int type_mask;
  int start_date;
  int end_date;
  int num;
  int user_list[0];
};

struct tl_cnews_update {
   int type;
   int owner;
   int place;
   int user;
   int group;
   int item;
};

struct tl_cnews_delete_updates {
  int type;
  int owner;
  int place;
};

struct tl_cnews_delete_update {
  int type;
  int owner;
  int place;
  int item;
};

struct tl_cnews_undelete_update {
  int type;
  int owner;
  int place;
  int item;
};

struct tl_cnews_get_raw_updates {
  int start_date;
  int end_date;
  int num;
  int object_list[0];
};

struct tl_cnews_get_raw_user_updates {
  int type_mask;
  int start_date;
  int end_date;
  int user_id;
};

struct tl_cnews_add_bookmark {
  int type;
  int owner;
  int place;
  int user_id;
};

struct tl_cnews_del_bookmark {
  int type;
  int owner;
  int place;
  int user_id;
};

struct tl_nnews_update {
  int id;
  int type;
  int user;
  int owner;
  int place;
  int item;
};

struct tl_nnews_delete_updates {
  int type;
  int owner;
  int place;
};

struct tl_nnews_delete_update {
  int type;
  int owner;
  int place;
  int item;
};

struct tl_nnews_undelete_update {
  int type;
  int owner;
  int place;
  int item;
};

struct tl_nnews_delete_user_update {
  int user_id;
  int type;
  int owner;
  int place;
  int item;
};

struct tl_nnews_undelete_user_update {
  int user_id;
  int type;
  int owner;
  int place;
  int item;
};

struct tl_nnews_get_updates {
  int user_id;
  int mask;
  int date;
  int end_date;
  int timestamp;
  int limit;
};

struct tl_nnews_get_grouped_updates {
  int user_id;
  int mask;
  int date;
  int end_date;
  int grouping;
  int timestamp;
  int limit;
};

struct tl_rnews_update {
  int id;
  int type;
  int owner;
  int place;
  int action;
  int item;
  int item_creation_time;
};

struct tl_rnews_set_rate {
  int type;
  int action;
  double rate;
};

struct tl_rnews_get_rate {
  int type;
  int action;
};

struct tl_rnews_get_raw_updates {
  int type_mask;
  int start_date;
  int end_date;
  int id;
  int t;
  int timestamp;
  int num;
  int user_list[0];
};

#pragma pack(pop)

#endif

