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
 * Author:  Anton  Timofeev    (atimofeev@vkontakte.ru)
 *          Engine common parts forward declaration.
 * Created: 01.04.2012
 */

#pragma once

/**
 * Required variables by engine kernel
 */

extern int verbosity;
extern int verbosity;
extern long long binlog_loaded_size;
extern double binlog_load_time;
extern int jump_log_ts;
extern unsigned jump_log_crc32;
extern long long jump_log_pos;
extern double index_load_time;
extern char *progname;
extern char *username, *binlogname, *logname;
extern long long max_binlog_size;

/**
 * Engine initialization common patterns
 */

// Try to change user and exit if fails.
void antispam_change_user (void);

// This part of initialization is very common with dump importing.
void antispam_engine_common_init_part (char const* index_fname);
