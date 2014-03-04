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
 *          zmalloc, dl_malloc and etc correctness test implementaion module.
 *          Look into the 'st-memtest.h' for more explanation.
 * Created: 28.04.2012
 */

#define _FILE_OFFSET_BITS 64

#ifndef ST_MEMTEST_ENABLED

#include "st-utils.h"
extern int verbosity;
void mt_test (void) {
  if (verbosity >= 2) {
    st_printf ("$3mt_test disabled$^ (should be compiled with -D\"ST_MEMTEST_ENABLED\")\n");
  }
}

#else

#include <stdio.h>
#include <stdlib.h>

#include "st-utils.h"
#include "st-memtest.h"

static mt_mem_event_t mt_all[MT_MAX_CALLS];
mt_mem_event_t *mt_first = mt_all, *mt_last = mt_all;
int mt_time = 0;
void *mt_tmp_ptr = 0;
int mt_tmp_size = 0;

int mt_compare_to (const void *_a, const void *_b) {
  const mt_mem_event_t *a = _a, *b = _b;
  if (a->ptr < b->ptr) {
    return -1;
  }
  if (a->ptr > b->ptr) {
    return 1;
  }
  return a->ts - b->ts;
}

void mt_test (void) {
  int failed = 0, warned = 0;
  qsort ((void*)mt_first, mt_last - mt_first, sizeof (mt_mem_event_t), mt_compare_to);
  mt_mem_event_t *me = mt_first;
  int total_leaks = 0;
  while (me < mt_last) {
    mt_mem_event_t *me1 = me + 1;
    if (me->size <= 0) {
      if (me->ptr != 0 || me->type != -1) {
        if (++warned <= 5) {
          fprintf (stderr, "Memory %s warning: line: %d, size: %d, %s.\n", (me->type == 1) ? "alloc" : "dealloc", me->line, me->size, me->msg);
        } else if (warned == 6) {
          fprintf (stderr, "... output terminated after 5 warnings.\n");
        }
      }
      ++me;
    } else if (me->type != 1 || me1 == mt_last || me1->type != -1 || me->ptr != me1->ptr || me->size != me1->size) {
      if (++failed <= 30) {
        fprintf (stderr, "Memory %s error: line: %d, size: %d, %s.\n", (me->type == 1) ? "alloc" : "dealloc", me->line, me->size, me->msg);
      } else if (failed == 31) {
        fprintf (stderr, "... output terminated after 30 errors.\n");
      }
      if (me->type == 1) {
        total_leaks += me->size;
      }
      ++me;
    } else {
      me += 2;
    }
  }
  if (failed || total_leaks != 0) {
    st_printf ("Memory test: $1FAILED$^, calls: %d, leaks: $1%d$^ bytes.\n", (int)(mt_last - mt_first), total_leaks);
  } else if (warned) {
    st_printf ("Memory test: $3WARNED$^, calls: %d, warnings: %d.\n", (int)(mt_last - mt_first), warned);
  } else {
    st_printf ("Memory test: $2SUCCEED$^, calls: %d.\n", (int)(mt_last - mt_first));
  }
}

#endif // ST_MEMTEST_ENABLED
