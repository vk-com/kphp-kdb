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

#ifndef __SEARCH_TL_H__
#define __SEARCH_TL_H__

#include "TL/constants.h"

#define FLAG_SORT (1 << 1)
#define FLAG_SORT_DESC (1 << 2)
#define FLAG_RESTR (1 << 3)
#define FLAG_EXACT_HASH (1 << 4)
#define FLAG_GROUP_HASH (1 << 5)
#define FLAG_HASH_CHANGE (1 << 6)
#define FLAG_RELEVANCE (1 << 7)
#define FLAG_TITLE (1 << 8)
#define FLAG_OPTTAG (1 << 9)
#define FLAG_CUSTOM_RATE_WEIGHT (1 << 10)
#define FLAG_CUSTOM_PRIORITY_WEIGHT (1 << 11)
#define FLAG_DECAY (1 << 12)
#define FLAG_EXTENDED_MODE (1 << 13)
#define FLAG_OCCURANCE_COUNT (1 << 14)
#define FLAG_RAND (1 << 15)
#define FLAG_WEAK_SEARCH (1 << 16)
#define FLAG_SEARCHX (1 << 17)
#define FLAG_RETRY_SEARCH (1 << 30)
#endif
