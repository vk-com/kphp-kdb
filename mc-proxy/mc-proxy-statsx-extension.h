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
*/

#ifndef __MC_PROXY_STATSX_EXTENSION_H__
#define	__MC_PROXY_STATSX_EXTENSION_H__

#include "mc-proxy.h"
#include "mc-proxy-merge-extension.h"

#define MAX_SEX 2
#define MAX_AGE 8
#define MAX_MSTATUS 8
#define MAX_POLIT 8
#define MAX_SECTION 16
#define MAX_CITIES  10000
#define MIN_CITIES  4
#define MAX_SEX_AGE (MAX_SEX*MAX_AGE)
#define MIN_COUNTRIES 2
#define MAX_COUNTRIES 63
#define MIN_GEOIP_COUNTRIES 2
#define MAX_GEOIP_COUNTRIES 63
#define MAX_TYPES 10
#define MAX_SOURCE 16
#define MAX_SUBCOUNTER 64

struct statsx_gather_extra {
  int views;
  int unique_visitors;
  int deletes; 
  int created_at;
  int valid_until;
  int visitors_sex[MAX_SEX];
  int visitors_age[MAX_AGE];
  int visitors_mstatus[MAX_MSTATUS];
  int visitors_polit[MAX_POLIT];
  int visitors_section[MAX_SECTION];
  int visitors_cities[2 * MAX_CITIES + 1];
  int visitors_sex_age[MAX_SEX_AGE];
  int visitors_countries[2 * MAX_COUNTRIES];
  int visitors_geoip_countries[2 * MAX_GEOIP_COUNTRIES];
  int visitors_source[MAX_SOURCE];
  int subcnt[MAX_SUBCOUNTER];
  int Q_raw;
  int flags;
};

#define FLAG_COUNTER 1
#define FLAG_ONE_INT 2
#define FLAG_UNION 4
#define FLAG_DOUBLE 8
#define FLAG_BAD_SERVERS 16
#define FLAG_NO_SERIALIZE 32
#define FLAG_MONTHLY 64

#endif
