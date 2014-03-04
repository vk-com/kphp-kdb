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

#pragma once

#define MSG_STD_BUFFER	2048
#define	MSG_SMALL_BUFFER	512
#define	MSG_TINY_BUFFER	48

#define	MSG_BUFFERS_CHUNK_SIZE	((1L << 21) - 64)

#define MSG_DEFAULT_MAX_ALLOCATED_BYTES	(1L << 28)

#ifdef _LP64
#define MSG_MAX_ALLOCATED_BYTES	(1L << 40)
#else
#define MSG_MAX_ALLOCATED_BYTES	(1L << 30)
#endif

#define MSG_BUFFER_FREE_MAGIC	0x4abdc351
#define MSG_BUFFER_USED_MAGIC	0x72e39317
#define	MSG_BUFFER_SPECIAL_MAGIC	0x683caad3

#define MSG_CHUNK_USED_MAGIC	0x5c75e681
#define MSG_CHUNK_HEAD_MAGIC	0x2dfecca3

#define	MAX_BUFFER_SIZE_VALUES	16

#define	BUFF_HD_BYTES	(offsetof (struct msg_buffer, data))

struct msg_buffer {
  struct msg_buffers_chunk *chunk;
#ifndef _LP64
  int resvd;
#endif
  int refcnt;
  int magic;
  char data[0];
};

struct msg_buffers_chunk {
  int magic;
  int buffer_size;
  int (*free_buffer)(struct msg_buffers_chunk *C, struct msg_buffer *B);
  struct msg_buffers_chunk *ch_next, *ch_prev;
  struct msg_buffers_chunk *ch_head;
  struct msg_buffer *first_buffer;
  int two_power; /* least two-power >= tot_buffers */
  int tot_buffers;
  int bs_inverse;
  int bs_shift;
  union {
    struct {
      int tot_chunks;
      int free_buffers;
    };
    unsigned short free_cnt[0];
  };
};

int init_msg_buffers (long max_buffer_bytes);

struct msg_buffer *alloc_msg_buffer (struct msg_buffer *neighbor, int size_hint);

int free_msg_buffer (struct msg_buffer *buffer);
