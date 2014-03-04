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

#ifndef __SEARCH_INTERFACE_STRUCTURES_H__
#define __SEARCH_INTERFACE_STRUCTURES_H__

struct tl_search {
  int flags;

  int rate_type;
  int limit;
  double opttag;
  double relevance;

  int restr_num;
  int rate_weights_num;
  int decay_rate_type;
  double decay_weight;
  int hash_rating;
  int text_len;
  int data[0];
};

struct tl_change_rates {
  int rate_type;
  int n;
  int data[0];
};

struct tl_incr_rate_by_hash {
  int rate_type;
  int rate;
  int n;
  long long data[0];
};

struct tl_delete_with_hashes {
  int n;
  long long data[0];
};

struct tl_delete_with_hash {
  long long hash;
};

struct tl_add_item_tags {
  long long item_id;
  int size;
  char text[0];
};

struct tl_set_item {
  long long item_id;
  int rate;
  int sate;
  int size;
  char text[0];
};

struct tl_delete_item {
  long long item_id;
};

struct tl_get_hash {
  long long item_id;
};

struct tl_set_hash {
  long long item_id;
  long long hash;
};

struct tl_incr_rate {
  long long item_id;
  int rate_type;
  int rate_value;
};

struct tl_set_rate {
  long long item_id;
  int rate_type;
  int rate_value;
};

struct tl_get_rate {
  long long item_id;
  int rate_type;
};
#endif
