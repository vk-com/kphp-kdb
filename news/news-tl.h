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
              2013 Anton Maydell
*/

#ifndef __NEWS_TL_H__
#define __NEWS_TL_H__

#include "TL/constants.h"

#define TL_NEWS_FLAG_TYPE (1 << 0)
#define TL_NEWS_FLAG_USER_ID (1 << 2)
#define TL_NEWS_FLAG_DATE (1 << 4)
#define TL_NEWS_FLAG_TAG (1 << 6)
#define TL_NEWS_FLAG_USER (1 << 8)
#define TL_NEWS_FLAG_GROUP (1 << 10)
#define TL_NEWS_FLAG_OWNER (1 << 12)
#define TL_NEWS_FLAG_PLACE (1 << 14)
#define TL_NEWS_FLAG_ITEM (1 << 16)
#endif
