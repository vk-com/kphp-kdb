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

#ifndef __STATSX_TL_H__
#define __STATSX_TL_H__
#include "TL/constants.h"

#define TL_STATSX_SEX (1 << 0)
#define TL_STATSX_AGE (1 << 1)
#define TL_STATSX_MSTATUS (1 << 2)
#define TL_STATSX_POLIT (1 << 3)
#define TL_STATSX_SECTION (1 << 4)
#define TL_STATSX_CITY (1 << 5)
#define TL_STATSX_GEOIP_COUNTRY (1 << 6)
#define TL_STATSX_COUNTRY (1 << 7)
#define TL_STATSX_SOURCE (1 << 8)
#define TL_STATSX_VIEWS (1 << 9)
#define TL_STATSX_VISITORS (1 << 10)
#define TL_STATSX_SEX_AGE (1 << 11)
#define TL_STATSX_MONTHLY (1 << 12)
#define TL_STATSX_WEEKLY (1 << 13)
#define TL_STATSX_DELETES (1 << 14)
#define TL_STATSX_VERSION (1 << 15)
#define TL_STATSX_EXPIRES (1 << 16)
#define TL_STATSX_EXTRA (1 << 17)
#endif
