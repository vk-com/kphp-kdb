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

#ifndef __PMEMCACHED_INDEX_COMMON_H__
#define __PMEMCACHED_INDEX_COMMON_H__
#include "am-stats.h"
struct index_entry* index_get (const char *key, int key_len);
struct index_entry* index_get_next (const char *key, int key_len);
struct index_entry* index_get_num (int n, int use_aio);
#define index_entry_next(x) index_get_next (x->data, x->key_len)
void custom_prepare_stats (stats_buffer_t *sb);
void free_metafiles ();

#endif
