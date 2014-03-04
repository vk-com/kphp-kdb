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
	           2013 Vitaliy Valtman
*/

#pragma once

#include <stdlib.h>
#include <sys/uio.h>
#include "net-msg-buffers.h"
#include "crypto/aesni256.h"
#include "kdb-data-common.h"

/*
 *	MESSAGE PARTS (struct msg_part)
 */

struct msg_part {
  // fields inherited from msg_buffer
  struct msg_buffers_chunk *chunk;
#ifndef _LP64
  int resvd;
#endif
  int refcnt;
  int magic;
  // fields specific to msg_part
  struct msg_part *next;
  struct msg_buffer *part;
  int offset;  // data offset inside part->data
  int len;     // part length in bytes
};

extern int rwm_total_msg_parts;
extern int rwm_total_msgs;

#ifdef MSG_PART_ZMALLOC
static inline struct msg_part *alloc_msg_part (void) { return (struct msg_part *) zmalloc (sizeof (struct msg_part)); }
static inline void free_msg_part (struct msg_part *mp) { rwm_total_msg_parts --; zfree (mp, sizeof (*mp)); }
#else
static inline struct msg_part *alloc_msg_part (void) { return (struct msg_part *) malloc (sizeof (struct msg_part)); }
static inline void free_msg_part (struct msg_part *mp) { rwm_total_msg_parts --; free (mp); }
#endif
struct msg_part *new_msg_part (struct msg_part *neighbor, struct msg_buffer *X);

/*
 *	RAW MESSAGES (struct raw_message) = chains of MESSAGE PARTs
 */

// ordinary raw message (changing refcnt of pointed msg_parts)
#define	RM_INIT_MAGIC	0x23513473
// temp raw message (doesn't change refcnts of pointed msg_parts), used for fast read iterators
#define	RM_TMP_MAGIC	0x52a717f3

#define	RM_PREPEND_RESERVE	128

struct raw_message {
  struct msg_part *first, *last;	// 'last' doesn't increase refcnt of pointed msg_part
  int total_bytes;	// bytes in the chain (extra bytes ignored even if present)
  int magic;		// one of RM_INIT_MAGIC, RM_TMP_MAGIC
  int first_offset;	// offset of first used byte inside first buffer data
  int last_offset;	// offset after last used byte inside last buffer data
};

/* NB: struct raw_message itself is never allocated or freed by the following functions since 
	it is usually part (field) of a larger structure
*/

int rwm_free (struct raw_message *raw);
int rwm_init (struct raw_message *raw, int alloc_bytes);
int rwm_create (struct raw_message *raw, void *data, int alloc_bytes);
void rwm_clone (struct raw_message *dest_raw, struct raw_message *src_raw);

int rwm_push_data (struct raw_message *raw, const void *data, int alloc_bytes);
int rwm_push_data_front (struct raw_message *raw, const void *data, int alloc_bytes);
int rwm_fetch_data (struct raw_message *raw, void *data, int bytes);
int rwm_fetch_lookup (struct raw_message *raw, void *buf, int bytes);
int rwm_fetch_data_back (struct raw_message *raw, void *data, int bytes);
int rwm_fetch_lookup_back (struct raw_message *raw, void *data, int bytes);
int rwm_trunc (struct raw_message *raw, int len);
int rwm_union (struct raw_message *raw, struct raw_message *tail);
int rwm_split (struct raw_message *raw, struct raw_message *tail, int bytes);
int rwm_split_head (struct raw_message *head, struct raw_message *raw, int bytes);
void *rwm_prepend_alloc (struct raw_message *raw, int alloc_bytes);
void *rwm_postpone_alloc (struct raw_message *raw, int alloc_bytes);

void rwm_clean (struct raw_message *raw);
void rwm_clear (struct raw_message *raw);
int rwm_check (struct raw_message *raw);
int fork_message_chain (struct raw_message *raw);

int rwm_prepare_iovec (const struct raw_message *raw, struct iovec *iov, int iov_len, int bytes);
unsigned rwm_calculate_crc32c (struct raw_message *raw, int bytes);
int rwm_dump_sizes (struct raw_message *raw);
int rwm_dump (struct raw_message *raw);
unsigned rwm_crc32c (struct raw_message *raw, int bytes);
unsigned rwm_crc32 (struct raw_message *raw, int bytes);

int rwm_process (struct raw_message *raw, int bytes, void (*process_block)(void *extra, const void *data, int len), void *extra);
int rwm_process_and_advance (struct raw_message *raw, int bytes, void (*process_block)(void *extra, const void *data, int len), void *extra);
int rwm_sha1 (struct raw_message *raw, int bytes, unsigned char output[20]);
int rwm_encrypt_decrypt (struct raw_message *raw, int bytes, struct vk_aes_ctx *ctx, unsigned char iv[32]);
int rwm_encrypt_decrypt_cbc (struct raw_message *raw, int bytes, struct vk_aes_ctx *ctx, unsigned char iv[16]);
int rwm_encrypt_decrypt_to (struct raw_message *raw, struct raw_message *res, int bytes, struct vk_aes_ctx *ctx, void (*crypt)(struct vk_aes_ctx *ctx, const void *src, void *dst, int l, unsigned char *iv), unsigned char *iv);
