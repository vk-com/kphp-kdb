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
              2011-2013 Vitaliy Valtman
                   2011 Anton Maydell
*/

#ifndef __PMEMCACHED_INTERFACE_STRUCTURES_H__
#define __PMEMCACHED_INTERFACE_STRUCTURES_H__
struct tl_pmemcached_get {
  int key_len;
  char key[0];
};

struct tl_pmemcached_delete {
  int key_len;
  char key[0];
};

struct tl_pmemcached_set {
  int op;
  int flags;
  int delay;
  int key_len;
  int value_len;
  char data[0];
};

struct tl_pmemcached_incr {
  long long value;
  int key_len;
  char key[0];
};

struct tl_pmemcached_get_wildcard {
  void *value_buf;
  char *last_key;
  int last_key_len;
  int prefix_key_len;
  int data_sent;
  int keys_sent;
  int key_len;
  char key[0];
};
#endif
