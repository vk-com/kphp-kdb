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
                   2011 Vitaliy Valtman
              2011-2012 Anton Maydell
*/

#ifndef __SEARCH_UTILS_H__
#define __SEARCH_UTILS_H__

#include "kdb-data-common.h"
#include <assert.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/types.h>
#include "search-index-layout.h"

#define likely(x) __builtin_expect((x),1)
#define unlikely(x) __builtin_expect((x),0)


double get_rusage_time (void);

long long get_malloc_memory_used (void);

extern int popcount_short (int x);
int get_ith_setbit (int mask, int i);
int get_bitno (int mask, int i);
int get_bitno_sparse (int mask, int i);

extern int tot_memory_used;

void zzcheck_memory_leaks (void);
void *zzmalloc0 (int size);
void *zzmalloc (int size);
void zzfree (void *src, int size);
void *zzrealloc (void *p, int old_len, int new_len);
void *zzrealloc_ushort_mask (void *src, int mask_old, int mask_new, int size_of_element);

extern unsigned idx_crc32_complement;
void flushout (void);
void clearin (void);
void writeout (const void *D, size_t len);
void *readin (size_t len);
void readadv (size_t len);
void bread (void *b, size_t len);
void set_read_file (int read_fd);
void set_write_file (int write_fd);

/* returns prime number which greater than 1.5n and not greater than 1.1 * 1.5 * n */
int get_hashtable_size (int n);

struct hashset_ll {
  int size;
  int filled;
  int n;
  unsigned long long *h;
};

int hashset_ll_init (struct hashset_ll *H, int n);
void hashset_ll_free (struct hashset_ll *H);
int hashset_ll_insert (struct hashset_ll *H, long long id);
int hashset_ll_get (struct hashset_ll *H, long long id);

struct hashset_int {
  int size;
  int filled;
  int n;
  unsigned int *h;
};

int hashset_int_init (struct hashset_int *H, int n);
void hashset_int_free (struct hashset_int *H);
int hashset_int_insert (struct hashset_int *H, int id);
int hashset_int_get (struct hashset_int *H, int id);

struct hashmap_ll_int_entry {
  long long key;
  int value;
  int extra; /* align purpose */
};

struct hashmap_ll_int {
  int size;
  int filled;
  int n;
  struct hashmap_ll_int_entry *h;
};

int hashmap_ll_int_init (struct hashmap_ll_int *H, int n);
void hashmap_ll_int_free (struct hashmap_ll_int *H);
/* return hashtable slot index */
int hashmap_ll_int_get (struct hashmap_ll_int *H, long long id, int *p_slot_idx);

struct hashmap_int_int_entry {
  int key;
  int value;
};

struct hashmap_int_int {
  int size;
  int filled;
  int n;
  struct hashmap_int_int_entry *h;
};

int hashmap_int_int_init (struct hashmap_int_int *H, int n);
void hashmap_int_int_free (struct hashmap_int_int *H);
/* return hashtable slot index */
int hashmap_int_int_get (struct hashmap_int_int *H, int id, int *p_slot_idx);

#endif
