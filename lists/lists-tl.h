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

#ifndef __LISTS_TL_H__
#define __LISTS_TL_H__
/*
#define TL_LIST_DELETE 0x879ce2e3

#define TL_LIST_ENTRY_DELETE 0xd04f767a
#define TL_LIST_OBJECT_DELETE 0xf8289655

#define TL_LIST_ENTRY_SET 0x6c5ea359
#define TL_LIST_ENTRY_ADD 0x5e4e0595
#define TL_LIST_ENTRY_REPLACE 0x46e9f3d5

#define TL_LIST_ENTRY_SET_TEXT 0xe688bf77

#define TL_LIST_ENTRY_SET_FLAGS 0x00798bc4
#define TL_LIST_ENTRY_CHANGE_FLAGS 0x42e00d32
#define TL_LIST_ENTRY_INCR_FLAGS 0x6b8442c6
#define TL_LIST_ENTRY_DECR_FLAGS 0xb878b49a

#define TL_LIST_ENTRY_SET_VALUE 0x59fce4ae
#define TL_LIST_ENTRY_INCR_VALUE 0xa4075e15
#define TL_LIST_ENTRY_DECR_VALUE 0x3a9c8cf9
#define TL_LIST_ENTRY_INCR_OR_CREATE 0x71fa7282
#define TL_LIST_ENTRY_DECR_OR_CREATE 0x07930da9

#define TL_LIST_SET_FLAGS 0xf0aa9c2a
#define TL_LIST_CHANGE_FLAGS 0x6f6c5fda
#define TL_LIST_CHANGE_FLAGS_EX 0x0b492927

#define TL_SUBLIST_DELETE 0x28491039
#define TL_SUBLIST_DELETE_EX 0x5767e95f

#define TL_LIST_ENTRY_GET 0x78afe46c
#define TL_LIST_ENTRY_GET_FLAGS 0x7a731db7
#define TL_LIST_ENTRY_GET_DATE 0x24c6216e
#define TL_LIST_ENTRY_GET_GLOBAL_ID 0x20f09910
#define TL_LIST_ENTRY_GET_VALUE 0xc559fdd6
#define TL_LIST_ENTRY_GET_TEXT 0xaa53d269
#define TL_LIST_ENTRY_GET_POS 0xa417d47c

#define TL_LIST_GET 0x90b75292
#define TL_LIST_GET_LIMIT 0xf5f11398
#define TL_LIST_GET_FULL 0x19ecd4e1
#define TL_LIST_GET_FULL_LIMIT 0x1af623c1

#define TL_LIST_COUNT 0xd55f9296
#define TL_SUBLIST_COUNT 0x3b687155
#define TL_SUBLISTS_COUNT 0x4685bee4

#define TL_LIST_INTERSECT 0xd79a3438
#define TL_LIST_INTERSECT_LIMIT 0x22f2a7e7
#define TL_LIST_INTERSECT_FULL 0xd29f0428
#define TL_LIST_INTERSECT_FULL_LIMIT 0xdee58818

#define TL_LIST_INTERSECT_WILD 0x2fcf378c
#define TL_LIST_INTERSECT_WILD_LIMIT 0x36706bba
#define TL_LIST_INTERSECT_WILD_FULL 0x41ea201c
#define TL_LIST_INTERSECT_WILD_FULL_LIMIT 0xc6ac88ce

#define TL_LIST_SUBTRACT 0x8e925d31
#define TL_LIST_SUBTRACT_LIMIT 0x8dda7311

#define TL_LIST_SUM 0x2d7c883a
#define TL_LIST_SUM_WILD 0xf1d75c0e
#define TL_LIST_SUM_WEIGHTED 0xb36506cf
#define TL_LIST_SUM_WILD_WEIGHTED 0x7c9a5f4c

#define TL_LIST_SORTED 0xe649273a
#define TL_LIST_SORTED_LIMIT 0xb46e9d8a
#define TL_LIST_SORTED_FULL 0xee32258f
#define TL_LIST_SORTED_FULL_LIMIT 0xf5206424

#define TL_DATEDISTR 0x0f11f0cb*/

#include "TL/constants.h"

#define TL_LIST_FLAG_FLAGS (1 << 6)
#define TL_LIST_FLAG_DATE (1 << 7)
#define TL_LIST_FLAG_GLOBAL_ID (1 << 8)
#define TL_LIST_FLAG_VALUE (1 << 9)
#define TL_LIST_FLAG_TEXT (1 << 10)
#define TL_LIST_FLAG_IP (1 << 11)
#define TL_LIST_FLAG_FRONT_IP (1 << 12)
#define TL_LIST_FLAG_PORT (1 << 13)
#define TL_LIST_FLAG_UA_HASH (1 << 14)
#define TL_LIST_FLAG_OBJECT_ID (1 << 15)
#endif
