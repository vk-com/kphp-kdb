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
              2010-2013 Nikolai Durov
              2013 Vitaliy Valtman
*/

#ifndef __LISTS_INTERFACE_STRUCTURES__
#define __LISTS_INTERFACE_STRUCTURES__


struct tl_datedistr {
  var_list_id_t list_id;
  int mode;
  int min_date;
  int max_date;
  int step;
};

struct tl_list_sorted {
  var_list_id_t list_id;
  int xor_mask;
  int and_mask;
  int mode;
  int limit;
};

struct tl_list_sum {
  var_list_id_t list_id;
  int has_weights;
  int id_ints;
  int mode;
  int count;
  int arr[0];
};

struct tl_list_intersect {
  var_list_id_t list_id;
  int limit;
  int is_intersect;
  int id_ints;
  int mode;
  int count;
  int arr[0];
};

struct tl_list_count {
  var_list_id_t list_id;
  int cnt;
};

struct tl_list_get {
  var_list_id_t list_id;
  int mode;
  int offset;
  int limit;
};

struct tl_list_entry_get {
  var_list_id_t list_id;
  var_object_id_t object_id;
  int mode;
};

struct tl_list_entry_get_int {
  var_list_id_t list_id;
  var_object_id_t object_id;
  int is_long;
  int offset;
};

struct tl_list_entry_get_text {
  var_list_id_t list_id;
  var_object_id_t object_id;
};

struct tl_list_entry_get_pos {
  var_list_id_t list_id;
  var_object_id_t object_id;
};

struct tl_sublist_delete {
  var_list_id_t list_id;
  int xor_mask;
  int and_mask;
};

struct tl_list_set_flags {
  var_list_id_t list_id;
  int xor_mask;
  int and_mask;
  int or_mask;
  int nand_mask;
};

struct tl_list_entry_incr_or_create {
  var_list_id_t list_id;
  var_object_id_t object_id;
  long long value;
  int flags;
};

struct tl_list_entry_set_value {
  var_list_id_t list_id;
  var_object_id_t object_id;
  long long value;
  int flags;
};

struct tl_list_entry_set_flags {
  var_list_id_t list_id;
  var_object_id_t object_id;
  int or_mask;
  int nand_mask;
};

struct tl_list_entry_set_text {
  var_list_id_t list_id;
  var_object_id_t object_id;
  int len;
  char text[0];
};

struct tl_list_entry_set {
  var_list_id_t list_id;
  var_object_id_t object_id;
  int op;
  int mode;
  long long value;
  int flags;
  int ip;
  int front_ip;
  int port;
  int ua_hash;
  int text_len;
  char text[0];
};

struct tl_object_delete {
  var_object_id_t object_id;
};

struct tl_list_entry_delete {
  var_list_id_t list_id;
  var_object_id_t object_id;
};

struct tl_list_delete {
  var_list_id_t list_id;
};
#endif

