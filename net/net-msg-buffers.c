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

    Copyright 2012-2013 Vkontakte Ltd
              2012-2013 Nikolai Durov
              2012-2013 Andrei Lopatin
*/

#define	_FILE_OFFSET_BITS	64

#include <assert.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/types.h>
#include <stddef.h>

#include "net-msg-buffers.h"

int verbosity;

long long total_used_buffers_size;
int total_used_buffers;
long max_allocated_buffer_bytes, allocated_buffer_bytes;

int allocated_buffer_chunks, max_buffer_chunks;

int buffer_size_values;

struct msg_buffers_chunk ChunkHeaders[MAX_BUFFER_SIZE_VALUES];

int default_buffer_sizes[] = { 48, 512, 2048 };

int free_std_msg_buffer (struct msg_buffers_chunk *C, struct msg_buffer *X);

void init_buffer_chunk_headers (void) {
  int i;
  struct msg_buffers_chunk *CH;
  assert (!buffer_size_values);
  for (i = 0, CH = ChunkHeaders; i < sizeof (default_buffer_sizes) / sizeof (int); i++, CH++) {
    CH->magic = MSG_CHUNK_HEAD_MAGIC;
    CH->buffer_size = default_buffer_sizes[i];
    CH->ch_next = CH->ch_prev = CH;
    CH->free_buffer = 0;
    assert (!i || default_buffer_sizes[i] > default_buffer_sizes[i-1]);
  }
  assert (i);
  buffer_size_values = i;
}

inline void prepare_bs_inv (struct msg_buffers_chunk *C) {
  int x = C->buffer_size + 16;
  int i = __builtin_ctz (x);
  x >>= i;
  x = 1 - x;
  int y = 1;
  while (x) {
    y *= 1 + x;
    x *= x;
  }
  C->bs_inverse = y;
  C->bs_shift = i;
}

struct msg_buffers_chunk *alloc_new_msg_buffers_chunk (struct msg_buffers_chunk *CH) {
  assert (CH->magic == MSG_CHUNK_HEAD_MAGIC);
  if (allocated_buffer_chunks >= max_buffer_chunks) {
    // ML
    return 0;
  }
  struct msg_buffers_chunk *C = malloc (MSG_BUFFERS_CHUNK_SIZE);
  if (!C) {
    return 0;
  }

  int buffer_size = CH->buffer_size, two_power, chunk_buffers;
  int buffer_hd_size = buffer_size + BUFF_HD_BYTES;
  int align = buffer_hd_size & -buffer_hd_size;
  if (align < 8) {
    align = 8;
  }
  if (align > 64) {
    align = 64;
  }

  int t = (MSG_BUFFERS_CHUNK_SIZE - offsetof (struct msg_buffers_chunk, free_cnt)) / (buffer_hd_size + 4);
  two_power = 1;
  while (two_power <= t) {
    two_power <<= 1;
  }

  chunk_buffers = (MSG_BUFFERS_CHUNK_SIZE - offsetof (struct msg_buffers_chunk, free_cnt) - two_power * 4 - align) / buffer_hd_size;
  assert (chunk_buffers > 0 && chunk_buffers < 65536 && chunk_buffers <= two_power);

  C->magic = MSG_CHUNK_USED_MAGIC;
  C->buffer_size = buffer_size;
  C->free_buffer = free_std_msg_buffer;
  C->ch_head = CH;
  
  C->ch_next = CH->ch_next;
  C->ch_prev = CH;
  CH->ch_next = C;
  C->ch_next->ch_prev = C;

  C->first_buffer = (struct msg_buffer *) (((long) C + offsetof (struct msg_buffers_chunk, free_cnt) + two_power * 4 + align - 1) & -align);
  assert ((char *) (C->first_buffer) + chunk_buffers * buffer_hd_size <= (char *) C + MSG_BUFFERS_CHUNK_SIZE);

  C->two_power = two_power;
  C->tot_buffers = chunk_buffers;

  CH->tot_buffers += chunk_buffers;
  CH->free_buffers += chunk_buffers;
  CH->tot_chunks++;

  allocated_buffer_chunks++;
  allocated_buffer_bytes += MSG_BUFFERS_CHUNK_SIZE;

  prepare_bs_inv (C);
  
  int i;
  for (i = 0; i < chunk_buffers; i++) {
    C->free_cnt[two_power+i] = 1;
  }
  memset (&C->free_cnt[two_power + chunk_buffers], 0, (two_power - chunk_buffers) * 2);

  for (i = two_power - 1; i > 0; i--) {
    C->free_cnt[i] = C->free_cnt[2*i] + C->free_cnt[2*i+1];
  }

  return C;
};

void free_msg_buffers_chunk_internal (struct msg_buffers_chunk *C, struct msg_buffers_chunk *CH) {
  assert (C->magic == MSG_CHUNK_USED_MAGIC);
  assert (CH->magic == MSG_CHUNK_HEAD_MAGIC);
  assert (C->buffer_size == CH->buffer_size);
  assert (C->tot_buffers == C->free_cnt[1]);
  assert (CH == C->ch_head);

  C->magic = 0;
  C->ch_head = 0;
  C->ch_next->ch_prev = C->ch_prev;
  C->ch_prev->ch_next = C->ch_next;

  CH->tot_buffers -= C->tot_buffers;
  CH->free_buffers -= C->tot_buffers;
  CH->tot_chunks--;
  assert (CH->tot_chunks >= 0);

  allocated_buffer_chunks--;
  allocated_buffer_bytes -= MSG_BUFFERS_CHUNK_SIZE;

  memset (C, 0, sizeof (struct msg_buffers_chunk));
  free (C);
}


void free_msg_buffers_chunk (struct msg_buffers_chunk *C) {
  assert (C->magic == MSG_CHUNK_USED_MAGIC);
  assert (C->free_cnt[1] == C->tot_buffers);

  int i;
  struct msg_buffers_chunk *CH = 0;
  for (i = 0; i < buffer_size_values; i++) {
    if (ChunkHeaders[i].buffer_size == C->buffer_size) {
      CH = &ChunkHeaders[i];
      break;
    }
  }

  assert (CH);

  free_msg_buffers_chunk_internal (C, CH);
}

int init_msg_buffers (long max_buffer_bytes) {
  if (!max_buffer_bytes) {
    max_buffer_bytes = max_allocated_buffer_bytes ?: MSG_DEFAULT_MAX_ALLOCATED_BYTES;
  }

  assert (max_buffer_bytes >= 0 && max_buffer_bytes <= MSG_MAX_ALLOCATED_BYTES);
  assert (max_buffer_bytes >= allocated_buffer_chunks * MSG_BUFFERS_CHUNK_SIZE);

  max_allocated_buffer_bytes = max_buffer_bytes;
  max_buffer_chunks = (unsigned long) max_buffer_bytes / MSG_BUFFERS_CHUNK_SIZE;

  if (!buffer_size_values) {
    init_buffer_chunk_headers ();
  }

  return 1;
}

inline int get_buffer_no (struct msg_buffers_chunk *C, struct msg_buffer *X) {
  unsigned x = ((char *) X - (char *) C->first_buffer);
  x >>= C->bs_shift;
  x *= C->bs_inverse;
  assert (x <= (unsigned) C->tot_buffers && (char *) X == (char *) C->first_buffer + (C->buffer_size + 16) * x);
  return x;
}

struct msg_buffer *alloc_msg_buffer_internal (struct msg_buffer *neighbor, struct msg_buffers_chunk *CH) {
  assert (CH->magic == MSG_CHUNK_HEAD_MAGIC);
  struct msg_buffers_chunk *CF = CH->ch_next, *C = CF;
  if (C == CH) {
    C = alloc_new_msg_buffers_chunk (CH);
    if (!C) {
      return 0;
    }
    assert (C == CH->ch_next && C != CH && C->free_cnt[1]);
  } else while (C != CH && !C->free_cnt[1]) {
    CH->ch_next = C->ch_next;
    C->ch_next->ch_prev = CH;
    C->ch_next = CH;
    C->ch_prev = CH->ch_prev;
    C->ch_prev->ch_next = C;
    CH->ch_prev = C;
    C = CH->ch_next;
    if (C == CF) {
      C = alloc_new_msg_buffers_chunk (CH);
      if (!C) {
	return 0;
      }
      assert (C == CH->ch_next && C != CH && C->free_cnt[1]);
      break;
    }
  }

  int two_power = C->two_power, i = 1;

  if (neighbor && neighbor->chunk == C) {
    int x = get_buffer_no (C, neighbor);
    if (verbosity > 2) {
      fprintf (stderr, "alloc_msg_buffer: allocating neighbor buffer for %d\n", x);
    }
    int j = 1, k = 0, l = 0, r = two_power;
    while (i < two_power) {
      i <<= 1;
      int m = (l + r) >> 1;
      if (x < m) {
	if (C->free_cnt[i] > 0) {
	  r = m;
	  if (C->free_cnt[i+1] > 0) {
	    j = i + 1;
	  }
	} else {
	  l = m;
	  i++;
	}
      } else if (C->free_cnt[i+1] > 0) {
	l = m;
	i++;
      } else {
	k = i = j;
	while (i < two_power) {
	  i <<= 1;
	  if (!C->free_cnt[i]) {
	    i++;
	  }
	  assert (-- C->free_cnt[i] >= 0);
	}
	break;
      }
    }
    if (!k) {
      k = i;
    }
    while (k > 0) {
      assert (-- C->free_cnt[k] >= 0);
      k >>= 1;
    }
  } else {
    int j = C->free_cnt[1] < 16 ? C->free_cnt[1] : 16;
    j = ((long long) lrand48() * j) >> 31;
    assert (j >= 0 && j < C->free_cnt[1]);
    while (i < two_power) {
      assert (-- C->free_cnt[i] >= 0);
      i <<= 1;
      if (C->free_cnt[i] <= j) {
	j -= C->free_cnt[i];
	i++;
      }
    }
    assert (-- C->free_cnt[i] == 0);
  }

  -- CH->free_buffers;

  i -= two_power;
  if (verbosity > 2) {
    fprintf (stderr, "alloc_msg_buffer(%d): tot_buffers = %d, free_buffers = %d\n", i, C->tot_buffers, C->free_buffers);
  }
  assert (i >= 0 && i < C->tot_buffers);

  struct msg_buffer *X = (struct msg_buffer *) ((char *) C->first_buffer + i * (C->buffer_size + 16));

  X->chunk = C;
  X->refcnt = 1;
  X->magic = MSG_BUFFER_USED_MAGIC;

  total_used_buffers_size += C->buffer_size;
  total_used_buffers ++;
  // assert (get_buffer_no (C, X) == i);

  return X;
}

/* allocates buffer of at least given size, -1 = maximal */
struct msg_buffer *alloc_msg_buffer (struct msg_buffer *neighbor, int size_hint) {
  if (!buffer_size_values) {
    init_buffer_chunk_headers ();
  }
  int si = buffer_size_values - 1;
  if (size_hint >= 0) {
    while (si > 0 && ChunkHeaders[si-1].buffer_size >= size_hint) {
      si--;
    }
  }
  return alloc_msg_buffer_internal (neighbor, &ChunkHeaders[si]);
}

int free_std_msg_buffer (struct msg_buffers_chunk *C, struct msg_buffer *X) {
  assert (!X->refcnt && X->magic == MSG_BUFFER_USED_MAGIC && C->magic == MSG_CHUNK_USED_MAGIC && X->chunk == C);
  int x = get_buffer_no (C, X);
  int two_power = C->two_power;
  if (verbosity > 2) {
    fprintf (stderr, "free_msg_buffer(%d)\n", x);
  }
  x += two_power;
  assert (!C->free_cnt[x]);
  do {
    assert (++C->free_cnt[x] > 0);
  } while (x >>= 1);

  X->magic = MSG_BUFFER_FREE_MAGIC;
  X->refcnt = -0x40000000;
  ++ C->ch_head->free_buffers;
  
  total_used_buffers_size -= C->buffer_size;
  total_used_buffers --;

  if (!C->free_cnt[1] && C->ch_head->free_buffers * 4 >= C->tot_buffers * 5) {
    free_msg_buffers_chunk (C);
  }

  return 1;
}

int free_msg_buffer (struct msg_buffer *X) {
  assert (!X->refcnt);
  struct msg_buffers_chunk *C = X->chunk;
  assert (C->magic == MSG_CHUNK_USED_MAGIC);
  return C->free_buffer (C, X);
}
