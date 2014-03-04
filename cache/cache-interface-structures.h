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

    Copyright 2012-2013 Vkontakte Ltd
              2012-2013 Anton Maydell
*/

#ifndef __CACHE_INTERFACE_STRUCTURES__
#define __CACHE_INTERFACE_STRUCTURES__

struct tl_cache_access {
  int cache_id;
  char url[0];
};

struct tl_cache_set_file_size {
  long long size;
  int cache_id;
  char url[0];
};

struct tl_cache_set_yellow_time_remaining {
  int cache_id;
  int time;
  int local_url_off;
  char data[0];
};

struct tl_cache_set_new_local_copy {
  int cache_id;
  int act;
  int local_url_off;
  char data[0];
};

struct tl_cache_enable_disk {
  int act;
  int cache_id;
  int node_id;
  int server_id;
  int disk_id;
};

struct tl_cache_delete_disk {
  int cache_id;
  int node_id;
  int server_id;
  int disk_id;
};

struct tl_cache_convert {
  int cache_id;
  char url[0];
};

struct tl_cache_get_file_size {
  int cache_id;
  char url[0];
};

struct tl_cache_get_local_copies {
  int cache_id;
  char url[0];
};

struct tl_cache_get_yellow_time {
  int cache_id;
  int local_url_off;
  char data[0];
};

struct tl_cache_get_top_access {
  int order;
  int cache_id;
  int t;
  int limit;
  int flags;
  int min_rate;
};

struct tl_cache_get_bottom_disk {
  int order;
  int cache_id;
  int t;
  int limit;
  int flags;
  int node_id;
  int server_id;
  int disk_id;
};

struct tl_cache_get_acounter {
  int cache_id;
  int t;
  char url[0];
};

struct tl_cache_get_server_stats {
  int cache_id;
  int sorting_flags;
};

#endif
