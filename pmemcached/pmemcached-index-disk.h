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

#ifndef __PMEMCACHED_INDEX_DISK_H__
#define __PMEMCACHED_INDEX_DISK_H__

#include "pmemcached-data.h"
#include "net-aio.h"

#define MAX_INDEX_ITEMS 10000000
#define MAX_INDEX_BINARY_DATA 100000000
#define MAX_METAFILES 1000000
#define MAX_METAFILE_SIZE 10000000
#define MAX_METAFILE_ELEMENTS 1000000
//#define MAX_METAFILES_LOADED 100

#define	PMEMCACHED_INDEX_MAGIC_OLD 0x4823dbcb
#define	PMEMCACHED_INDEX_MAGIC 0x57834af0

#pragma	pack(push,4)
struct metafile_header_old {
  long long global_offset;
  int metafile_size;
  int local_offset;
  int nrecords;
  short key_len;
  char key[0];
};

struct metafile_header {
  long long global_offset;
  int metafile_size;
  int local_offset;
  int nrecords;
  unsigned crc32;
  short key_len;
  char key[0];
};

struct metafile {
  struct metafile_header *header;
  struct aio_connection *aio;
  long long *local_offsets;
  char *data;
};

struct metafile_store {
  int nrecords;
  int data_len;
  char data[0];
};


typedef struct {
/* strange numbers */
  int magic;
  int created_at;
  long long log_pos0;
  long long log_pos1;
  unsigned long long file_hash;
  int log_timestamp;
  unsigned int log_pos0_crc32;
  unsigned int log_pos1_crc32;

  int nrecords;
} index_header;



struct index_entry* index_get (const char *key, int key_len);

#pragma	pack(pop)

#endif
