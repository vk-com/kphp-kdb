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
              2013 Anton Maydell
*/

#ifndef __CACHE_INTERFACE_STRUCTURES__
#define __CACHE_INTERFACE_STRUCTURES__

#pragma pack(push,4)

struct tl_weights_incr {
  int vector_id;
  int coord_id;
  int value;
};

struct tl_weights_set_half_life {
  int coord_id;
  int half_life;
};

struct tl_weights_at {
  int vector_id;
  int coord_id;
};

struct tl_weights_get_vector {
  int vector_id;
};

struct tl_weights_subscribe {
  int vector_rem;
  int vector_mod;
  int coord_ids_num;
  int updates_start_time;
  int updates_seek_limit;
  int updates_limit;
  int small_updates_seek_limit;
  int small_updates_limit;
  int coord_ids[0];
};

struct tl_subscription_stop {

};

#pragma pack(pop)

#endif
