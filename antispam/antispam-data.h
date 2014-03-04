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

    Copyright 2012-2013	Vkontakte Ltd
              2012      Sergey Kopeliovich <Burunduk30@gmail.com>
              2012      Anton Timofeev <atimofeev@vkontakte.ru>
*/
/**
 * Author:  Anton Timofeev (atimofeev@vkontakte.ru)
 *          Log events support and memcached exterface.
 * Created: 22.03.2012
 */

#pragma once

#include "kfs.h"

/**
 * Memcached "exterface"
 */

// str - is not zero ended string!
bool do_add_pattern (antispam_pattern_t p, int str_len, char *str, bool replace);
bool do_del_pattern (int id);

/**
 * Working with Index and databases
 */

// Override weak one from 'binlog/kdb-binlog-common.c'
int init_antispam_data (int schema);

void init_all (kfs_file_handle_t Index);
int save_index (void);
void finish_all (void);
