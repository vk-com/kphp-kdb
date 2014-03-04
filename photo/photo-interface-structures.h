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

#pragma once

struct tl_change_photo {
  int user_id;
  int photo_id;
  long long mask;
  int changes_len;
  char changes[0];
};

struct tl_change_album {
  int user_id;
  int album_id;
  long long mask;
  int changes_len;
  char changes[0];
};

struct tl_increment_photo_field {
  int user_id;
  int photo_id;
  int cnt;
  char field_name[0];
};

struct tl_increment_album_field {
  int user_id;
  int album_id;
  int cnt;
  char field_name[0];
};

struct tl_set_volume_server {
  int volume_id;
  int server_id;
};

struct tl_delete_location_engine {
  int user_id;
  int photo_id;
  int rotate;
  char size;
  int original;
};

struct tl_delete_location {
  int user_id;
  int photo_id;
  int original;
};

struct tl_change_location_server {
  int user_id;
  int photo_id;
  int original;
  int server_num;
  int server_id;
};

struct tl_save_photo_location {
  int user_id;
  int photo_id;
};

struct tl_restore_photo_location {
  int user_id;
  int photo_id;
};

struct tl_rotate_photo {
  int user_id;
  int photo_id;
  int dir;
};

struct tl_change_photo_order {
  int user_id;
  int photo_id;
  int id_near;
  int is_next;
};

struct tl_change_album_order {
  int user_id;
  int album_id;
  int id_near;
  int is_next;
};

struct tl_new_photo_force {
  int user_id;
  int album_id;
  int photo_id;
};

struct tl_new_photo {
  int user_id;
  int album_id;
};

struct tl_new_album_force {
  int user_id;
  int album_id;
};

struct tl_new_album {
  int user_id;
};

struct tl_get_photos_overview {
  int user_id;
  int offset;
  int limit;
  int is_reverse;
  int need_count;
  long long mask;
  int albums_cnt;
  int albums[0];
};

struct tl_get_photos_count {
  int user_id;
  int album_id;
  char condition[0];
};

struct tl_get_albums_count {
  int user_id;
  char condition[0];
};

struct tl_get_photos {
  int user_id;
  int album_id;
  int offset;
  int limit;
  int is_reverse;
  int need_count;
  long long mask;
  char condition[0];
};

struct tl_get_albums {
  int user_id;
  int offset;
  int limit;
  int is_reverse;
  int need_count;
  long long mask;
  char condition[0];
};

struct tl_get_photo {
  int user_id;
  int photo_id;
  long long mask;
  int force;
};

struct tl_get_album {
  int user_id;
  int album_id;
  long long mask;
  int force;
};

struct tl_restore_photo {
  int user_id;
  int photo_id;
};
struct tl_delete_photo {
  int user_id;
  int photo_id;
};

struct tl_delete_album {
  int user_id;
  int album_id;
};
