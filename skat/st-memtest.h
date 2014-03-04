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

    Copyright 2012 Vkontakte Ltd
              2012 Sergey Kopeliovich <Burunduk30@gmail.com>
              2012 Anton Timofeev <atimofeev@vkontakte.ru>
*/

/**
 * Author:  Anton  Timofeev    (atimofeev@vkontakte.ru)
 *          zmalloc, dl_malloc and etc correctness test.
 *          Include this file in every *.c that need to be tested,
 *          and run 'mt_test()' after all memory supposed to be deallocated.
 * Created: 28.04.2012
 */

#pragma once

void mt_test (void);

#ifdef ST_MEMTEST_ENABLED

// This files MUST be included before redefinition below!
#include "kdb-data-common.h" // zmalloc
#include "dl-utils.h"        // dl_malloc

#define MT_MAX_CALLS (1 << 24)
typedef struct {
  void *ptr;
  char *msg;
  int size;
  int line;
  int ts;
  int type; // 1: open, -1: close
} mt_mem_event_t;

extern mt_mem_event_t *mt_first, *mt_last;
extern int mt_time;
extern void *mt_tmp_ptr;
extern int mt_tmp_size;

#define REDECLARE_ALLOC(_alloc, _size) ( \
  assert(mt_last - mt_first < MT_MAX_CALLS), \
  mt_tmp_size   = (int)(_size), \
  mt_last->ptr  = _alloc (mt_tmp_size), \
  mt_last->msg  = "file: " __FILE__ ", method: " #_alloc " (" #_size ")", \
  mt_last->size = mt_tmp_size, \
  mt_last->line = __LINE__, \
  mt_last->type = 1, \
  mt_last->ts   = ++mt_time, \
  mt_last++->ptr \
)
#define REDECLARE_DEALLOC(_dealloc, _ptr, _size) ( \
  assert(mt_last - mt_first < MT_MAX_CALLS), \
  mt_tmp_ptr    = _ptr, \
  mt_tmp_size    = (int)(_size), \
  mt_last->ptr  = mt_tmp_ptr, \
  mt_last->msg  = "file: " __FILE__ ", method: " #_dealloc " (" #_ptr ", " #_size ")", \
  mt_last->size = mt_tmp_size, \
  mt_last->line = __LINE__, \
  mt_last->type = -1, \
  mt_last->ts   = ++mt_time, \
  _dealloc (mt_tmp_ptr, mt_tmp_size), \
  ++mt_last \
)

#define zmalloc(size)      REDECLARE_ALLOC   (zmalloc,    size)
#define zmalloc0(size)     REDECLARE_ALLOC   (zmalloc0,   size)
#define zfree(ptr, size)   REDECLARE_DEALLOC (zfree, ptr, size)

#define dl_malloc(size)    REDECLARE_ALLOC   (dl_malloc,    size)
#define dl_malloc0(size)   REDECLARE_ALLOC   (dl_malloc0,   size)
#define dl_free(ptr, size) REDECLARE_DEALLOC (dl_free, ptr, size)

#endif // ST_MEMTEST_ENABLED
