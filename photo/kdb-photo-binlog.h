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

#include "kdb-binlog-common.h"

#pragma	pack(push,4)

#ifndef	PHOTO_SCHEMA_BASE
#define	PHOTO_SCHEMA_BASE	0x5f180000
#endif

#define	PHOTO_SCHEMA_V1	0x5f180101

#define LEV_PHOTO_CREATE_PHOTO_FORCE 0x23510000
#define LEV_PHOTO_CREATE_ALBUM_FORCE 0x6cab3127
#define LEV_PHOTO_CREATE_PHOTO 0x41ba0000
#define LEV_PHOTO_CREATE_ALBUM 0x21c5ade3
#define LEV_PHOTO_CHANGE_PHOTO 0x56ab0000
#define LEV_PHOTO_CHANGE_ALBUM 0x70130000
#define LEV_PHOTO_CHANGE_PHOTO_ORDER 0x201003ba
#define LEV_PHOTO_CHANGE_ALBUM_ORDER 0x741efacd
#define LEV_PHOTO_INCREM_PHOTO_FIELD 0x4ca3de00
#define LEV_PHOTO_INCREM_ALBUM_FIELD 0x1798ab00
#define LEV_PHOTO_DELETE_PHOTO 0x456cf192
#define LEV_PHOTO_DELETE_ALBUM 0x761abfe2
#define LEV_PHOTO_RESTORE_PHOTO 0x6bc1456f

#define LEV_PHOTO_SET_VOLUME 0x7adb3157
#define LEV_PHOTO_ADD_PHOTO_LOCATION 0x23bcad30
#define LEV_PHOTO_ADD_PHOTO_LOCATION_ENGINE_OLD 0x54efdc00
#define LEV_PHOTO_ADD_PHOTO_LOCATION_ENGINE 0x5ac71900
#define LEV_PHOTO_CHANGE_PHOTO_LOCATION_SERVER 0x631e217c
#define LEV_PHOTO_DEL_PHOTO_LOCATION 0x5abc2d3e
#define LEV_PHOTO_DEL_PHOTO_LOCATION_ENGINE 0x32873000
#define LEV_PHOTO_SAVE_PHOTO_LOCATION 0x34ffab12
#define LEV_PHOTO_RESTORE_PHOTO_LOCATION 0x15723ab2
#define LEV_PHOTO_ROTATE_PHOTO 0x45ab13cd


struct lev_photo_create_photo_force {
  lev_type_t type;
  int user_id;
  int album_id;
  int photo_id;
};

struct lev_photo_create_album_force {
  lev_type_t type;
  int user_id;
  int album_id;
};

struct lev_photo_create_photo {
  lev_type_t type;
  int user_id;
  int album_id;
};

struct lev_photo_create_album {
  lev_type_t type;
  int user_id;
};

struct lev_photo_delete_photo {
  lev_type_t type;
  int user_id;
  int photo_id;
};

struct lev_photo_delete_album {
  lev_type_t type;
  int user_id;
  int album_id;
};

struct lev_photo_change_photo_order {
  lev_type_t type;
  int user_id;
  int photo_id;
  int photo_id_near;
};

struct lev_photo_change_album_order {
  lev_type_t type;
  int user_id;
  int album_id;
  int album_id_near;
};

struct lev_photo_change_data {
  lev_type_t type;
  int user_id;
  int data_id;
  int mask;
  char changes[0];
};

struct lev_photo_increm_data {
  lev_type_t type;
  int user_id;
  int data_id;
  int cnt;
};

struct lev_photo_restore_photo {
  lev_type_t type;
  int user_id;
  int photo_id;
};


struct lev_photo_set_volume {
  lev_type_t type;
  int volume_id;
  int server_id;
};

struct lev_photo_add_photo_location {
  lev_type_t type;
  int user_id;
  int photo_id;
  int server_id;
  int server_id2;
  int orig_owner_id;
  int orig_album_id;
  char photo[0];
};

struct lev_photo_add_photo_location_engine_old {
  lev_type_t type;
  int user_id;
  int photo_id;
  int volume_id;
  int local_id;
  unsigned long long secret;
};

struct lev_photo_add_photo_location_engine {
  lev_type_t type;
  int user_id;
  int photo_id;
  int volume_id;
  int local_id;
  int extra;
  unsigned long long secret;
};

struct lev_photo_change_photo_location_server {
  lev_type_t type;
  int user_id;
  int photo_id;
  int server_id;
};

struct lev_photo_del_photo_location {
  lev_type_t type;
  int user_id;
  int photo_id;
};

struct lev_photo_del_photo_location_engine {
  lev_type_t type;
  int user_id;
  int photo_id;
};

struct lev_photo_restore_photo_location {
  lev_type_t type;
  int user_id;
  int photo_id;
};

struct lev_photo_save_photo_location {
  lev_type_t type;
  int user_id;
  int photo_id;
};

struct lev_photo_rotate_photo {
  lev_type_t type;
  int user_id;
  int photo_id;
};



#pragma	pack(pop)

