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
                   2010 Nikolai Durov
                   2010 Andrei Lopatin
              2011-2013 Vitaliy Valtman
              2011-2013 Anton Maydell
*/

#ifndef __STATSX_INTERFACE_STRUCTURES_H__
#define __STATSX_INTERFACE_STRUCTURES_H__

struct tl_incr {
  long long counter_id;
  int op;
  int custom_version;
  int user_id;
  int mode;
  int sex;
  int age;
  int mstatus;
  int polit;
  int section;
  int city;
  int geoip_country;
  int country;
  int source;
};

struct tl_incr_subcnt {
  long long counter_id;
  int op;
  int custom_version;
  int subcnt_id;
};

struct tl_get_views {
  long long counter_id;
  int version;
};

struct tl_get_visitors {
  long long counter_id;
  int version;
};

struct tl_enable_counter {
  long long counter_id;
  int disable;
};

struct tl_set_timezone {
  long long counter_id;
  int offset;
};

struct tl_get_counter {
  long long counter_id;
  int mode;
  int version;
};

struct tl_get_versions {
  long long counter_id;
};

struct tl_get_monthly_visitors {
  long long counter_id;
};

struct tl_get_monthly_views {
  long long counter_id;
};

struct tl_get_counters_sum {
  long long counter_id;
  int from;
  int to;
};
#endif
