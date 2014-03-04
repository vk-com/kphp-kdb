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

    Copyright 2008 Nikolai Durov
              2008 Andrei Lopatin
*/

#ifndef __MSG_SEARCH_DATA_H__
#define __MSG_SEARCH_DATA_H__

#define MAIL_INDEX_BLOCK_MAGIC	0x11ef55aa
#define	MAX_METAINDEX_USERS	(1L << 21)

#pragma	pack(push,4)

/* metaindex file entry */

typedef struct user_header {
  int magic;
  int user_id;
  int hash_cnt;
  int list_cnt;
} user_header_t;

/* index file user subsection header */

typedef struct userlist_entry {
  int user_id;
  int hash_cnt;
  int list_cnt;
  int file_no;
  long long offset;
} userlist_entry_t;
  
typedef unsigned long long hash_t;
typedef struct pair pair_t;

struct pair {
  hash_t hash;
  int order;
  int message_id;
};

#pragma	pack(pop)

#endif
