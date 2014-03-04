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

    Copyright 2011-2012 Vkontakte Ltd
              2011-2012 Arseny Smirnov
              2011-2012 Aliaksei Levin
*/

#pragma once

#include "kfs.h"
#include "kdb-data-common.h"
#include "server-functions.h"

#define MAX_MEMORY 2000000000
#define	SUPPORT_INDEX_MAGIC 0x234cd125

#define MAX_ANSWERS 1000000

#define MAX_LEN 32767
#define MAX_RES 100

extern int index_mode;
extern long max_memory;
extern int header_size;
extern int binlog_readed;

extern int jump_log_ts;
extern long long jump_log_pos;
extern unsigned int jump_log_crc32;

extern int answers_cnt;

int do_add_answer (int user_id, int agent_id, int mark, int len, char *question_with_answer);
int do_set_mark (int user_id, int mark);
int do_delete_answer (int user_id);

char *get_answer (int user_id, int agent_id, int len, char *question, int cnt, int with_question);

int init_all (kfs_file_handle_t Index);
int save_index (void);
void free_all (void);

