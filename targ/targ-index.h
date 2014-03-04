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

    Copyright 2009-2013 Vkontakte Ltd
              2009-2013 Nikolai Durov
              2009-2013 Andrei Lopatin
*/

#ifndef __TARG_INDEX_H__
#define __TARG_INDEX_H__

#include "kdb-data-common.h"
#include "kdb-targ-binlog.h"
#include "kfs.h"
#include "net-aio.h"
#include "targ-trees.h"
#include "targ-index-layout.h"

extern long long idx_bytes, idx_loaded_bytes;
extern int idx_fresh_ads, idx_stale_ads, idx_words, idx_max_uid, idx_recent_views;
extern int allocated_metafiles;
extern long long allocated_metafile_bytes;

extern long long index_bytes, index_loaded_bytes;

typedef struct core_metafile core_mf_t;

struct core_metafile {
  struct advert *ad;
  struct aio_connection *aio;
  long long pos;
  int len;
  char data[0];
};

extern struct aio_connection *WaitAio;
extern conn_query_type_t aio_metafile_query_type;

int load_index (kfs_file_handle_t Index);
int write_index (int writing_binlog);

int load_ancient_ad (struct advert *A);

#define TMP_WDATA_INTS	4096

extern unsigned tmp_word_data[TMP_WDATA_INTS], *tmp_word_data_cur;

static inline void clear_tmp_word_data (void) {
  tmp_word_data_cur = tmp_word_data;
}

int get_idx_word_list_len (hash_t word);
unsigned char *idx_word_lookup (hash_t word, int *max_bytes);

int load_stats_file (char *stats_filename);
int save_stats_file (char *stats_filename);

#endif
