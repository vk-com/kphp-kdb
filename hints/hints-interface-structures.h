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

#pragma once

struct tl_hints_nullify_rating {
  int user_id;
};

struct tl_hints_set_rating_state {
  int user_id;
  int rating_enabled;
};

struct tl_hints_set_info {
  int user_id;
  int info;
};

struct tl_hints_set_winner {
  int user_id;
  int type;
  int winner;
  int rating_num;
  int losers_cnt;
  int losers[0];
};

struct tl_hints_set_rating {
  int user_id;
  int type;
  int object_id;
  int rating_num;
  float rating;
};

#ifdef HINTS
struct tl_hints_set_text {
  int user_id;
  int type;
  int object_id;
  int text_len;
  char text[0];
};
#endif

struct tl_hints_set_type {
  int user_id;
  int type;
  int object_id;
  int new_type;
};

#ifdef HINTS
struct tl_hints_set_text_global {
  int type;
  int object_id;
  int text_len;
  char text[0];
};
#endif

struct tl_hints_set_type_global {
  int type;
  int object_id;
  int new_type;
};

struct tl_hints_get_info {
  int user_id;
};

struct tl_hints_sort {
  int user_id;
  int limit;
  int rating_num;
  int objects_cnt;
  int need_rand;
  long long objects[0];
};

#ifdef HINTS
struct tl_hints_get_hints {
  int user_id;
  int type;
  int limit;
  int rating_num;
  char need_rating;
  char need_text;
  char need_latin;
  int query_len;
  char query[0];
};

struct tl_hints_get_object_text {
  int user_id;
  int type;
  int object_id;
};
#endif

struct tl_hints_delete_object {
  int user_id;
  int type;
  int object_id;
};

struct tl_hints_delete_object_global {
  int type;
  int object_id;
};

struct tl_hints_increment_rating {
  int user_id;
  int type;
  int object_id;
  int cnt;
  int rating_num;
};


struct tl_rating_add_object {
  int user_id;
  int type;
  int object_id;
};

struct tl_rating_get_hints {
  int user_id;
  int type;
  int limit;
  int rating_num;
  char need_rating;
  char need_rand;
  int exceptions_cnt;
  long long exceptions[0];
};

