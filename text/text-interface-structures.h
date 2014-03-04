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

#ifndef __TEXT_INTERFACE_STRUCTURES_H__
#define __TEXT_INTERFACE_STRUCTURES_H__

struct tl_delete_secret {
  int uid;
};

struct tl_get_secret {
  int uid;
};

struct tl_set_secret {
  int uid;
  char secret[9];
};

struct tl_online_friends {
  int uid;
  int mode;
};

struct tl_online {
  int uid;
  int offline;
  int size;
  int data[0];
};

struct tl_phistory {
  int uid;
  int timestamp;
  int limit;
};

struct tl_get_ptimestamp {
  int uid;
};

struct tl_history_action_base {
  int uid;
  int event_id;
  int who;
  int data;
};

struct tl_history_action_ex {
  int uid;
  int event_id;
  int text_len;
  char text[0];
};

struct tl_history {
  int uid;
  int limit;
  int timestamp;
};

struct tl_get_timestamp {
  int uid;
  int force;
};

struct tl_search {
  int uid;
  int num;
  int peer_id;
  int and_mask;
  int or_mask;
  int min_time;
  int max_time;
  int text_len;
  int flags;

  char text[0];
};

struct tl_load_userdata {
  int uid;
  int force;
};

struct tl_delete_userdata {
  int uid;
};

struct tl_get_userdata {
  int uid;
};

struct tl_replace_message_text {
  int uid;
  int local_id;
  int text_len;
  char text[0];
};

struct tl_set_extra_mask {
  int new_mask;
};

struct tl_get_extra_mask {
};

struct tl_set_Extra {
  int uid;
  int local_id;
  int k;
  long long value;
};

struct tl_incr_Extra {
  int uid;
  int local_id;
  int k;
  long long value;
};

struct tl_get_Extra {
  int uid;
  int local_id;
  int k;
};

struct tl_set_extra {
  int uid;
  int local_id;
  int k;
  int value;
};

struct tl_incr_extra {
  int uid;
  int local_id;
  int k;
  int value;
};

struct tl_get_extra {
  int uid;
  int local_id;
  int k;
};

struct tl_set_flags {
  int uid;
  int local_id;
  int flags;
};

struct tl_incr_flags {
  int uid;
  int local_id;
  int flags;
  int decr;
};

struct tl_get_flags {
  int uid;
  int local_id;
};

struct tl_delete_first_messages {
  int uid;
  int min_local_id;
};

struct tl_delete_message {
  int uid;
  int local_id;
};

struct tl_send_message {
  int uid;
  int mode;
  long long legacy_id;
  struct {
    struct lev_add_message M;
    int extra[16];
  } Z;
  char text[0];
};

struct tl_peermsg_type {
};

struct tl_sublist_types {
};

struct tl_sublist_pos {
  int uid;
  int and_mask;
  int or_mask;
  int local_id;
};

struct tl_sublist {
  int uid;
  int and_mask;
  int or_mask;
  int mode;
  int from;
  int to;
};

struct tl_top_msg_list {
  int uid;
  int from;
  int to;
};

struct tl_peer_msg_list_pos {
  int uid;
  int peer_id;
  int local_id;
};

struct tl_peer_msg_list {
  int uid;
  int peer_id;
  int from;
  int to;
};

struct tl_convert_legacy_id {
  int uid;
  int legacy_id;
};

struct tl_convert_random_id {
  int uid;
  long long random_id;
};

struct tl_get_message {
  int uid;
  int local_id;
  int mode;
  int max_len;
};
#endif
