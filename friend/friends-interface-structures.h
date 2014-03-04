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
*/

#ifndef __FRIENDS_INTERFACE_STRUCTURES__
#define __FRIENDS_INTERFACE_STRUCTURES__

struct tl_delete_user {
  int uid;
};

struct tl_get_friends {
  int uid;
  int mode;
  int mask;
};

struct tl_get_friends_cnt {
  int uid;
  int mode;
  int mask;
};

struct tl_get_recent_friends {
  int uid;
  int num;
};

struct tl_set_cat_list {
  int uid;
  int cat;
  int num;
  int list[0];
};

struct tl_delete_cat {
  int uid;
  int cat;
};

struct tl_get_friend {
  int uid;
  int fid;
};

struct tl_set_friend {
  int uid;
  int fid;
  int xor_mask;
  int and_mask;
  int is_set;
  int is_incr;
};

struct tl_delete_friend {
  int uid;
  int fid;
};

struct tl_get_friend_req {
  int uid;
  int fid;
};

struct tl_set_friend_req {
  int uid;
  int fid;
  int cat;
  int force;
};

struct tl_delete_friend_req {
  int uid;
  int fid;
};

struct tl_delete_requests {
  int uid;
};

struct tl_get_requests {
  int uid;
  int num;
};

struct tl_set_privacy {
  int uid;
  privacy_key_t privacy_key;
  int len;
  int force;
  char text[0];
};

struct tl_get_privacy {
  int uid;
  privacy_key_t privacy_key;
};

struct tl_delete_privacy {
  int uid;
  privacy_key_t privacy_key;
};

struct tl_check_privacy {
  int uid;
  int reqid;
  privacy_key_t privacy_key;
};

struct tl_check_privacy_list {
  int uid;
  int reqid;
  int num;
  privacy_key_t privacy_key[0];
};

struct tl_common_friends {
  int uid;
  int uid2;
};

struct tl_common_friends_num {
  int uid;
  int num;
  int uid_list[0];
};
#endif
