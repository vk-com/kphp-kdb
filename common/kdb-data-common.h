/*
    This file is part of VK/KittenPHP-DB-Engine Library.

    VK/KittenPHP-DB-Engine Library is free software: you can redistribute it and/or modify
    it under the terms of the GNU Lesser General Public License as published by
    the Free Software Foundation, either version 2 of the License, or
    (at your option) any later version.

    VK/KittenPHP-DB-Engine Library is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Lesser General Public License for more details.

    You should have received a copy of the GNU Lesser General Public License
    along with VK/KittenPHP-DB-Engine Library.  If not, see <http://www.gnu.org/licenses/>.

    Copyright 2009-2013 Vkontakte Ltd
              2009-2013 Nikolai Durov
              2009-2013 Andrei Lopatin
*/

#ifndef __KDB_DATA_COMMON_H__
#define __KDB_DATA_COMMON_H__

#include "kdb-binlog-common.h"

#ifdef	_LP64
# define PTRSIZE	8
# define PTRSHIFT	3
#else
# define PTRSIZE	4
# define PTRSHIFT	2
#endif

#define	PTR_INTS	(PTRSIZE / 4)
#define	PAGE_SIZE	4096

#ifdef	_LP64 
# define DYNAMIC_DATA_BUFFER_SIZE	(6000L << 20)
# define DYNAMIC_DATA_BIG_BUFFER_SIZE	(14000L << 20)
#else
# define DYNAMIC_DATA_BUFFER_SIZE	(2000L << 20)
# define DYNAMIC_DATA_BIG_BUFFER_SIZE	(2000L << 20)
#endif

#define	MAX_RECORD_WORDS	4096

extern long dynamic_data_buffer_size;

/* common data structures */

#ifndef	hash_t
typedef unsigned long long hash_t;
#define	hash_t	hash_t
#endif

typedef struct hash_list {
  int len;
  int size;
  hash_t A[1];
} hash_list_t;

int in_hashlist (hash_t x, hash_list_t *L);

/* dynamic data management */

typedef char *dyn_mark_t[2];

extern char *dyn_first, *dyn_cur, *dyn_top, *dyn_last;
extern int wasted_blocks, freed_blocks;
extern long wasted_bytes, freed_bytes;

extern int FreeCnt[MAX_RECORD_WORDS+8], UsedCnt[MAX_RECORD_WORDS+8];
extern int SplitBlocks[MAX_RECORD_WORDS+8];
extern int NewAllocations[MAX_RECORD_WORDS+8][4];

void init_dyn_data (void);
void dyn_clear_low (void);
void dyn_clear_high (void);
void dyn_clear_free_blocks (void);

void dyn_mark (dyn_mark_t dyn_state);
void dyn_release (dyn_mark_t dyn_state);

void *dyn_alloc (long size, int align);
void *dyn_top_alloc (long size, int align);
void dyn_free (void *ptr, long size, int align);

void *zmalloc (long size);
void *zmalloc0 (long size);
void zfree (void *ptr, long size);
void *ztmalloc (long size);
void *ztmalloc0 (long size);
char *zstrdup (const char *const s);

long dyn_free_bytes (void);
void dyn_assert_free (long size);
void dyn_update_stats (void);

void dyn_garbage_collector (void);

// amount of used memory in bytes
long dyn_used_memory (void);

#endif
