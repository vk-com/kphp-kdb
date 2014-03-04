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

#define	_FILE_OFFSET_BITS	64

#include <assert.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/types.h>
#include <aio.h>
#include <errno.h>

#include "crc32.h"
#include "kdb-data-common.h"
#include "kfs-layout.h"

extern int verbosity, now;

/* common data structures */

int in_hashlist (hash_t x, hash_list_t *L) {
  int a, b, c;
  if (!L) { return 0; }
  a = -1;  b = L->len;
  while (b - a > 1) {
    c = (a + b) >> 1;
    if (L->A[c] < x) {
      a = c;
    } else if (L->A[c] > x) {
      b = c;
    } else {
      return c + 1;
    }
  }
  return 0;
}

/* dynamic data management */

#define	DYN_FREE_MAGIC	0xaa11ef55

void *FreeBlocks[MAX_RECORD_WORDS+8], *FreeBlocksAligned[MAX_RECORD_WORDS+8];
int FreeCnt[MAX_RECORD_WORDS+8], UsedCnt[MAX_RECORD_WORDS+8];
int SplitBlocks[MAX_RECORD_WORDS+8];
int NewAllocations[MAX_RECORD_WORDS+8][4];

int wasted_blocks, freed_blocks;
long wasted_bytes, freed_bytes;

char *dyn_first, *dyn_cur, *dyn_top, *dyn_last, *dyn_data_malloc;
char *dyn_mark_bottom, *dyn_mark_top;
long dynamic_data_buffer_size = DYNAMIC_DATA_BUFFER_SIZE;
// char DynData[(DYNAMIC_DATA_BUFFER_SIZE)+16];

typedef struct dyn_free_block dyn_free_block_t;

struct dyn_free_block {
  dyn_free_block_t *next;
  int magic;
};

#define	FB(p)	((struct dyn_free_block *) (p))
#define	NX(p)	(FB(p)->next)
#define	MAGIC(p)	(FB(p)->magic)

void dyn_clear_free_blocks (void) {
  memset (FreeBlocks, 0, sizeof (FreeBlocks));
  memset (FreeBlocksAligned, 0, sizeof (FreeBlocksAligned));
  memset (FreeCnt, 0, sizeof (FreeCnt));
  memset (SplitBlocks, 0, sizeof (SplitBlocks));
}

void init_dyn_data (void) {
  dyn_data_malloc = malloc (dynamic_data_buffer_size+PAGE_SIZE);
  if (!dyn_data_malloc) {
    fprintf (stderr, "fatal: unable to allocate %ld bytes for dyn_data heap: %m\n", dynamic_data_buffer_size+PAGE_SIZE);
  }
  assert (dyn_data_malloc);

  dyn_first = dyn_data_malloc + ((PAGE_SIZE - (long) dyn_data_malloc) & (PAGE_SIZE - 1));
  dyn_cur = dyn_first;
  dyn_top = dyn_last = dyn_first + dynamic_data_buffer_size;
  /* study carefully two next lines */
  /*
  if (!log_schema || !replay_logevent) {
    replay_logevent = default_replay_logevent;
  }
  */
}

void dyn_mark (dyn_mark_t dyn_state) {
  if (dyn_state) {
    dyn_state[0] = dyn_mark_bottom;
    dyn_state[1] = dyn_mark_top;
  }
  dyn_mark_bottom = dyn_cur;
  dyn_mark_top = dyn_top;
}

void dyn_release (dyn_mark_t dyn_state) {
  dyn_cur = dyn_mark_bottom;
  dyn_top = dyn_mark_top;
  if (dyn_state) {
    dyn_mark_bottom = dyn_state[0];
    dyn_mark_top = dyn_state[1];
  } else {
    dyn_mark_bottom = dyn_mark_top = 0;
  }
}


void dyn_clear_low (void) {
  dyn_cur = dyn_mark_bottom ? dyn_mark_bottom : dyn_first;
}

void dyn_clear_high (void) {
  dyn_top = dyn_mark_top ? dyn_mark_top : dyn_last;
}

static inline void dyn_free_block (void *p, int words) {
  assert (words >= PTR_INTS);
  FreeCnt[words]++;
  if (!((long) p & 7)) { 
    NX(p) = FreeBlocksAligned[words];
    FreeBlocksAligned[words] = p;
  } else {
    NX(p) = FreeBlocks[words];
    FreeBlocks[words] = p;
  }
  if (words > PTR_INTS) {
    MAGIC(p) = DYN_FREE_MAGIC;
  }
}

void *dyn_alloc (long size, int align) {
  int tmp;
  char *r = 0;
  assert (size >= 0 && (unsigned long) size < (unsigned long) dynamic_data_buffer_size);
  size = (size + 3) & -4;
  if (dyn_mark_top) {
    r = dyn_cur + ((align - (long) dyn_cur) & (align - 1));
    if (dyn_top <= r || dyn_top < r + size) {
      if (verbosity > 0) {
        fprintf (stderr, "unable to allocate %ld bytes\n", size);
      }
      return 0;
    }
    dyn_cur = r + size;
    return r;
  }
  if (size < PTRSIZE) {
    size = PTRSIZE;
  }
  long t = ((unsigned long) size >> 2);
  if (t < MAX_RECORD_WORDS && FreeBlocks[t] && align == 4) {
    r = FreeBlocks[t];
    assert (r >= dyn_first && r <= dyn_last - size && !(((long) r) & 3));
    if (t > PTR_INTS) {
      assert (MAGIC(r) == DYN_FREE_MAGIC);
      MAGIC(r) = 0;
    }
    FreeBlocks[t] = NX(r);
    FreeCnt[t]--;
    UsedCnt[t]++;
    NewAllocations[t][1]++;
    return r;
  }
  if (t < MAX_RECORD_WORDS && FreeBlocksAligned[t] && (align == 4 || align == 8)) {
    r = FreeBlocksAligned[t];
    assert (r >= dyn_first && r <= dyn_last - size && !(((long) r) & 7));
    if (t > PTR_INTS) {
      assert (MAGIC(r) == DYN_FREE_MAGIC);
      MAGIC(r) = 0;
    }
    FreeBlocksAligned[t] = NX(r);
    FreeCnt[t]--;
    UsedCnt[t]++;
    NewAllocations[t][1]++;
    return r;
  }

  if (t < MAX_RECORD_WORDS) {
    tmp = SplitBlocks[t];
    if (tmp) {
      if (tmp > 0) {
	assert (tmp >= t + PTR_INTS);
	if (FreeBlocks[tmp] && (PTR_INTS == 1 || align == 4 || tmp >= t + 5)) {
	  r = FreeBlocks[tmp];
	} else if (FreeBlocksAligned[tmp]) {
	  r = FreeBlocksAligned[tmp];
	} else {
	  tmp = -t - PTR_INTS;
	}
      } 
      if (tmp < 0) {
	tmp = -tmp;
	int tmp2 = tmp + 5;
	if (tmp2 > MAX_RECORD_WORDS - 1) {
	  tmp2 = MAX_RECORD_WORDS - 1;
	}
	while (++tmp < tmp2) {
	  if (FreeBlocks[tmp] && (PTR_INTS == 1 || align == 4 || tmp >= t + 5)) {
	    r = FreeBlocks[tmp];
	    break;
	  } else if (FreeBlocksAligned[tmp]) {
	    r = FreeBlocksAligned[tmp];
	    break;
	  }
	}
	if (tmp < MAX_RECORD_WORDS) {
	  SplitBlocks[t] = -tmp;
	}
      }
    }
  }

  if (t < MAX_RECORD_WORDS && r && (align == 4 || align == 8)) {
    char *q = r + size;
    assert (tmp > t);
    assert (r >= dyn_first && r <= dyn_last - size && !(((long) r) & 3));
    if (t > PTR_INTS) {
      assert (MAGIC(r) == DYN_FREE_MAGIC);
      MAGIC(r) = 0;
    }
    if (r == FreeBlocks[tmp]) {
      FreeBlocks[tmp] = NX(r);
    } else {
      FreeBlocksAligned[tmp] = NX(r);
    }
    FreeCnt[tmp]--;
    UsedCnt[t]++;
    NewAllocations[t][2]++;
    if (align == 4 || !((long) r & 7)) {
      tmp -= t;
      dyn_free_block (q, tmp);
      return r;
    } else if ((tmp - t) & 1) {
      tmp -= t;
      dyn_free_block (r, tmp);
      return r + tmp*4;
    } else {
      int z = 2 * PTR_INTS - 1;
      dyn_free_block (r, z);
      r += z*4;
      q += z*4;
      tmp -= t + z;
      assert (tmp >= 0);
      if (tmp > 0) {
        dyn_free_block (q, tmp);
      }
      return r;
    }
  }
  r = dyn_cur + ((align - (long) dyn_cur) & (align - 1));
  if (dyn_top <= r || dyn_top < r + size) {
    if (verbosity > 0) {
      fprintf (stderr, "unable to allocate %ld bytes\n", size);
    }
    return 0;
  }
  if (t < MAX_RECORD_WORDS) {
    NewAllocations[t][0]++;
    UsedCnt[t]++;
  }
  dyn_cur = r + size;
  return r;
}

void *dyn_top_alloc (long size, int align) {
  char *r;
  assert (size >= 0 && (unsigned long) size < (unsigned long) dynamic_data_buffer_size);
  size = (size + 3) & -4;
  r = dyn_top - size;
  r -= ((long) r) & (align - 1);
  if (r < dyn_cur || r > dyn_top) {
    if (verbosity > 0) {
      fprintf (stderr, "unable to allocate %ld bytes\n", size);
    }
    return 0;
  }
  long t = ((unsigned long) size >> 2);
  if (t < MAX_RECORD_WORDS && !dyn_mark_top) {
    UsedCnt[t]++;
    NewAllocations[t][0]++;
  }
  dyn_top = r;
  return r;
}

void dyn_free (void *p, long size, int align) {
  assert (!(align & 3));
  assert (!((long) p & (align - 1)));
  assert (size >= 0 && (unsigned long) size < (unsigned long) dynamic_data_buffer_size);
  size = (size + 3) & -4;
  if (size < PTRSIZE && ((char *) p >= dyn_mark_top || (char *) p < dyn_mark_bottom)) {
    size = PTRSIZE;
  }
  assert (((char *) p >= dyn_first && (char *) p <= dyn_cur - size) || ((char *) p >= dyn_top && (char *) p <= dyn_last - size));
  long t = (size >> 2);
  if ((char *) p < dyn_mark_top && (char *) p >= dyn_mark_bottom) {
    return;
  } 
  if (t >= MAX_RECORD_WORDS) {
    wasted_blocks++;
    wasted_bytes += size;
    return;
  }
  UsedCnt[t]--;
  dyn_free_block (p, t);
}

void dyn_update_stats (void) {
  int i;
  freed_blocks = freed_bytes = 0;
  for (i = 1; i < MAX_RECORD_WORDS; i++) {
    freed_blocks += FreeCnt[i];
    freed_bytes += i*FreeCnt[i];
  }
  freed_bytes <<= 2;
}

void dyn_garbage_collector (void) {
  int i, cval, cmax = -1, cmptr = 0;
  for (i = MAX_RECORD_WORDS + 3; i >= PTR_INTS; i--) {
    cval = FreeCnt[i] * i;
    if (cval > cmax) {
      cmax = cval;
      cmptr = i;
    }
    SplitBlocks[i-PTR_INTS] = cmptr;
  }
}

void *zmalloc (long size) {
  void *res = dyn_alloc (size, PTRSIZE);
  assert (res);
  return res;
}

void *zmalloc0 (long size) {
  void *res = dyn_alloc (size, PTRSIZE);
  assert (res);
  memset (res, 0, size);
  return res;
}

void zfree (void *ptr, long size) {
  dyn_free (ptr, size, 4);
}

void *ztmalloc (long size) {
  void *res = dyn_top_alloc (size, PTRSIZE);
  assert (res);
  return res;
}

void *ztmalloc0 (long size) {
  void *res = dyn_top_alloc (size, PTRSIZE);
  assert (res);
  memset (res, 0, size);
  return res;
}

char *zstrdup (const char *const s) {
  char *p = zmalloc (strlen (s) + 1);
  strcpy (p, s);
  return p;
}

long dyn_free_bytes (void) {
  return dyn_top - dyn_cur;
}

void dyn_assert_free (long size) {
  assert (dyn_top - dyn_cur >= size);
}

long dyn_used_memory (void) {
  dyn_update_stats();
  return (dyn_cur - dyn_first) - freed_bytes;
}

