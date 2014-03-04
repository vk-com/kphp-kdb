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

#define        _FILE_OFFSET_BITS        64

#include <assert.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/types.h>
#include <stddef.h>
#include <sys/uio.h>
#include <openssl/sha.h>
#include "server-functions.h"

#include "net-msg.h"
#include "crc32c.h"
#include "crc32.h"
#include "crypto/aesni256.h"

int rwm_total_msgs;
int rwm_total_msg_parts;

struct msg_part *new_msg_part (struct msg_part *neighbor, struct msg_buffer *X) {
  struct msg_part *mp = alloc_msg_part ();
  rwm_total_msg_parts ++;
  assert (mp);
  mp->refcnt = 1;
  mp->next = 0;
  mp->part = X;
  mp->offset = 0;
  mp->len = 0;
  return mp;
}

static int msg_part_decref (struct msg_part *mp) {
  struct msg_part *mpn;
  int cnt = 0;
  while (mp) {
    if (--mp->refcnt) {
      assert (mp->refcnt > 0);
      break;
    }
    int buffer_refcnt = --mp->part->refcnt;
    if (!buffer_refcnt) {
      free_msg_buffer (mp->part);
      cnt++;
    } else {
      assert (buffer_refcnt > 0);
    }
    mpn = mp->next;
    mp->part = 0;
    mp->next = 0;
    free_msg_part (mp); // for now, msg_part is always malloc'd
    mp = mpn;
  }
  return cnt;
}

// struct raw_message itself is not freed since it is usually part of a larger structure
int rwm_free (struct raw_message *raw) {
  struct msg_part *mp = raw->first;
  int t = raw->magic;
  assert (raw->magic == RM_INIT_MAGIC || raw->magic == RM_TMP_MAGIC);
  rwm_total_msgs --;
  memset (raw, 0, sizeof (*raw));
  return t == RM_TMP_MAGIC ? 0 : msg_part_decref (mp);
}

int fork_message_chain (struct raw_message *raw) {
  assert (raw->magic == RM_INIT_MAGIC);
  struct msg_part *mp = raw->first, **mpp = &raw->first, *mpl = 0;
  int copy_last = 0, res = 0, total_bytes = raw->total_bytes;
  if (!mp) {
    return 0;
  }
  if (raw->first_offset != mp->offset) {
    if (mp->refcnt == 1) {
      int delta = raw->first_offset - mp->offset;
      assert (delta > 0 && mp->len >= delta && mp->len <= mp->part->chunk->buffer_size);
      mp->offset += delta;
      mp->len -= delta;
    }
  }
  while (mp != raw->last && mp->refcnt == 1) {
    total_bytes -= mp->len;
    mpp = &mp->next;
    mpl = mp;
    mp = mp->next;
    assert (mp);
  }
  if (mp->refcnt != 1) {
    --mp->refcnt;
    while (!copy_last) {
      assert (mp);
      struct msg_part *mpc = new_msg_part (mpl, mp->part);
      mpc->part->refcnt++;
      mpc->offset = mp->offset;
      mpc->len = mp->len;

      if (mp == raw->first && raw->first_offset != mp->offset) {
        int delta = raw->first_offset - mp->offset;
        assert (delta > 0 && mp->len >= delta && mp->len <= mp->part->chunk->buffer_size);
        mpc->offset += delta;
        mpc->len -= delta;
      }

      if (mp == raw->last) {
        mpc->len = raw->last_offset - mpc->offset;
        assert (mpc->len >= 0);
        copy_last = 1;
        raw->last = mpc;
      }
      *mpp = mpc;
      total_bytes -= mpc->len;
      ++res;
    
      mpp = &mpc->next;
      mpl = mpc;
      mp = mp->next;
    }
  } else {
    assert (mp == raw->last);
    if (raw->last_offset != mp->offset + mp->len) {
      mp->len = raw->last_offset - mp->offset;
      assert (mp->len >= 0);
    }
    total_bytes -= mp->len;
    msg_part_decref (mp->next);
    mp->next = 0;
  }
  if (total_bytes) {
    fprintf (stderr, "total_bytes = %d\n", total_bytes);
  }
  assert (!total_bytes);
  return res;
}

void rwm_clean (struct raw_message *raw) {
  assert (raw->magic == RM_INIT_MAGIC || raw->magic == RM_TMP_MAGIC);
  raw->first = raw->last = 0;
  raw->first_offset = raw->last_offset = 0;
  raw->total_bytes = 0;
}

void rwm_clear (struct raw_message *raw) {
  assert (raw->magic == RM_INIT_MAGIC || raw->magic == RM_TMP_MAGIC);
  if (raw->first) {
    msg_part_decref (raw->first);
  }
  rwm_clean (raw);
}

void rwm_clone (struct raw_message *dest_raw, struct raw_message *src_raw) {
  assert (src_raw->magic == RM_INIT_MAGIC || src_raw->magic == RM_TMP_MAGIC);
  memcpy (dest_raw, src_raw, sizeof (struct raw_message));
  if (src_raw->magic == RM_INIT_MAGIC && src_raw->first) {
    src_raw->first->refcnt++;
  }
  rwm_total_msgs ++;
}

int rwm_push_data (struct raw_message *raw, const void *data, int alloc_bytes) {
  assert (raw->magic == RM_INIT_MAGIC);
  assert (alloc_bytes >= 0);
  if (!alloc_bytes) {
    return 0;
  }
  struct msg_part *mp, *mpl;
  int res = 0;
  if (!raw->first) {
    struct msg_buffer *X = alloc_msg_buffer (0, alloc_bytes >= MSG_SMALL_BUFFER - RM_PREPEND_RESERVE ? MSG_STD_BUFFER : MSG_SMALL_BUFFER);
    if (!X) {
      return 0;
    }
    mp = new_msg_part (0, X);
    int prepend = RM_PREPEND_RESERVE;
    if (alloc_bytes <= MSG_STD_BUFFER) {
      if (prepend > MSG_STD_BUFFER - alloc_bytes) {
        prepend = MSG_STD_BUFFER - alloc_bytes;
      }
    }
    mp->offset = prepend;
    int sz = X->chunk->buffer_size - prepend;
    raw->first = raw->last = mp;
    raw->first_offset = prepend;
    if (sz >= alloc_bytes) {
      mp->len = alloc_bytes;
      raw->total_bytes = alloc_bytes;
      raw->last_offset = alloc_bytes + prepend;
      if (data) {
        memcpy (X->data + prepend, data, alloc_bytes);
      }
      return alloc_bytes;
    }
    mp->len = sz;
    alloc_bytes -= sz;
    raw->total_bytes = sz;
    raw->last_offset = sz + prepend;
    res = sz;
    if (data) {
      memcpy (X->data + prepend, data, sz);
      data += sz;
    }
  } else {
    mp = raw->last;
    assert (mp);
    if (mp->next || raw->last_offset != mp->offset + mp->len) {
      assert (raw->last_offset <= mp->offset + mp->len);
      // trying to append bytes to a sub-message of a longer chain, have to fork the chain
      fork_message_chain (raw);
      mp = raw->last;
    }
    assert (mp && !mp->next && raw->last_offset == mp->offset + mp->len);
    struct msg_buffer *X = mp->part;
    if (X->refcnt == 1) {
      int buffer_size = X->chunk->buffer_size;
      int sz = buffer_size - raw->last_offset;
      assert (sz >= 0 && sz <= buffer_size);
      if (sz > 0) {
        // can allocate sz bytes inside the last buffer in chain itself
        if (sz >= alloc_bytes) {
          if (data) {
            memcpy (X->data + raw->last_offset, data, alloc_bytes);
          }
          raw->total_bytes += alloc_bytes;
          raw->last_offset += alloc_bytes;
          mp->len += alloc_bytes;
          return alloc_bytes;
        }
        if (data) {
          memcpy (X->data + raw->last_offset, data, sz);
          data += sz;
        }
        raw->total_bytes += sz;
        raw->last_offset += sz;
        mp->len += sz;
        alloc_bytes -= sz;
      }
      res = sz;
    }
  }
  while (alloc_bytes > 0) {
    mpl = mp;
    struct msg_buffer *X = alloc_msg_buffer (mpl->part, raw->total_bytes + alloc_bytes >= MSG_STD_BUFFER ? MSG_STD_BUFFER : MSG_SMALL_BUFFER);
    if (!X) {
      return res;
    }
    mp = new_msg_part (mpl, X);
    mpl->next = raw->last = mp;
    int buffer_size = X->chunk->buffer_size;
    if (buffer_size >= alloc_bytes) {
      mp->len = alloc_bytes;
      raw->total_bytes += alloc_bytes;
      raw->last_offset = alloc_bytes;
      if (data) {
        memcpy (X->data, data, alloc_bytes);
      }
      return res + alloc_bytes;
    }
    mp->len = buffer_size;
    alloc_bytes -= buffer_size;
    raw->total_bytes += buffer_size;
    raw->last_offset = buffer_size;
    res += buffer_size;
    if (data) {
      memcpy (X->data, data, buffer_size);
      data += buffer_size;
    }
  }
  return res;
}

int rwm_push_data_front (struct raw_message *raw, const void *data, int alloc_bytes) {
  assert (raw->magic == RM_INIT_MAGIC);
  assert (alloc_bytes >= 0);
  if (!alloc_bytes) {
    return 0;
  }
  struct msg_part *mp = 0;
  int r = alloc_bytes;
  if (raw->first) {
    struct msg_buffer *X = raw->first->part;
    if (raw->first->refcnt > 1) {
      if (raw->first_offset != raw->first->offset) {
        mp = new_msg_part (raw->first, X);
        X->refcnt ++;
        mp->offset = raw->first_offset;
        mp->len = raw->first->len + (raw->first->offset - raw->first_offset);
        raw->first->refcnt --;
        mp->next = raw->first->next;
        if (mp->next) {
          mp->next->refcnt ++;
        }
        if (raw->last == raw->first) {
          raw->last = mp;
          int delta = mp->offset + mp->len - raw->last_offset;
          assert (delta >= 0);
          mp->len -= delta;
        }
        raw->first = mp;
      } else {
        mp = raw->first;
      }
    } else {
      int delta = raw->first_offset - raw->first->offset;
      assert (delta >= 0);
      raw->first->offset += delta;
      raw->first->len -= delta;
      mp = raw->first;
    }
    if (X->refcnt == 1 && mp->refcnt == 1) {
      int size = raw->first_offset;
      if (alloc_bytes > size) {
        memcpy (X->data, data + (alloc_bytes - size), size);
        alloc_bytes -= size;
        raw->first->len += size;
        raw->total_bytes += size;
        raw->first->offset = 0;
        raw->first_offset = 0;
      } else {
        memcpy (X->data + size - alloc_bytes, data, alloc_bytes);
        raw->first->len += alloc_bytes;
        raw->first->offset -= alloc_bytes;
        raw->first_offset = raw->first->offset;
        raw->total_bytes += alloc_bytes;
        return r;
      }
    }
  }
  while (alloc_bytes) {
    struct msg_buffer *X = alloc_msg_buffer (raw->first ? raw->first->part : 0, alloc_bytes >= MSG_SMALL_BUFFER ? MSG_STD_BUFFER : MSG_SMALL_BUFFER);
    assert (X);
    int size = X->chunk->buffer_size;
    mp = new_msg_part (raw->first, X);
    mp->next = raw->first;
    raw->first = mp;

    if (alloc_bytes > size) {
      memcpy (X->data, data + (alloc_bytes - size), size);
      alloc_bytes -= size;
      mp->len = size;
      mp->offset = 0;
      raw->total_bytes += size;
      if (!raw->last) {
        raw->last = mp;
        raw->last_offset = mp->offset + mp->len;
      }
    } else {
      memcpy (X->data + size - alloc_bytes, data, alloc_bytes);
      mp->len = alloc_bytes;
      mp->offset = (size - alloc_bytes);
      raw->first_offset = mp->offset;
      raw->total_bytes += alloc_bytes;
      if (!raw->last) {
        raw->last = mp;
        raw->last_offset = mp->offset + mp->len;
      }
      return r;
    }
  }
  assert (0);
  return r;
}

int rwm_create (struct raw_message *raw, void *data, int alloc_bytes) {
  rwm_total_msgs ++;
  memset (raw, 0, sizeof (*raw));
  raw->magic = RM_INIT_MAGIC;
  return rwm_push_data (raw, data, alloc_bytes);
}

int rwm_init (struct raw_message *raw, int alloc_bytes) {
  return rwm_create (raw, 0, alloc_bytes);
}

void *rwm_prepend_alloc (struct raw_message *raw, int alloc_bytes) {
  assert (raw->magic == RM_INIT_MAGIC);
  assert (alloc_bytes >= 0);
  if (!alloc_bytes || alloc_bytes > MSG_STD_BUFFER) {
    return 0;
  }
  // struct msg_part *mp, *mpl;
  // int res = 0;
  if (!raw->first) {
    rwm_push_data (raw, 0, alloc_bytes);
    return raw->first->part->data + raw->first_offset;
  }
  if (raw->first->refcnt == 1) {
    if (raw->first_offset != raw->first->offset) {
      int delta = raw->first_offset - raw->first->offset;
      assert (delta >= 0);
      raw->first->offset += delta;
      raw->first->len -= delta;
    }
    if (raw->first->offset >= alloc_bytes && raw->first->part->refcnt == 1) {
      raw->first->offset -= alloc_bytes;
      raw->first->len += alloc_bytes;
      raw->first_offset -= alloc_bytes;
      raw->total_bytes += alloc_bytes;
      return raw->first->part->data + raw->first_offset;
    }
  } else {
    if (raw->first_offset != raw->first->offset) {
      struct msg_part *mp = new_msg_part (raw->first, raw->first->part);
      raw->first->part->refcnt ++;
      mp->next = raw->first->next;
      if (mp->next) { mp->next->refcnt ++; }
      raw->first->refcnt --;

      mp->offset = raw->first_offset;
      mp->len = raw->first->len - (raw->first_offset - raw->first->offset);

      if (raw->last == raw->first) {
        raw->last = mp;
      }
      raw->first = mp;
    }
  }
  assert (raw->first_offset == raw->first->offset);
  struct msg_buffer *X = alloc_msg_buffer (raw->first ? raw->first->part : 0, alloc_bytes);
  assert (X);  
  int size = X->chunk->buffer_size;
  assert (size >= alloc_bytes);
  struct msg_part *mp = new_msg_part (raw->first, X);
  mp->next = raw->first;
  raw->first = mp;
  mp->len = alloc_bytes;
  mp->offset = size - alloc_bytes;
  raw->first_offset = mp->offset;
  raw->total_bytes += alloc_bytes;
  return raw->first->part->data + mp->offset;
}

void *rwm_postpone_alloc (struct raw_message *raw, int alloc_bytes) {
  assert (raw->magic == RM_INIT_MAGIC);
  assert (alloc_bytes >= 0);
  if (!alloc_bytes || alloc_bytes > MSG_STD_BUFFER) {
    return 0;
  }
  // struct msg_part *mp, *mpl;
  // int res = 0;
  if (!raw->first) {
    rwm_push_data (raw, 0, alloc_bytes);
    return raw->first->part->data + raw->first_offset;
  }

  if (raw->last->next || raw->last->len + raw->last->offset != raw->last_offset) {
    fork_message_chain (raw);
  }
  struct msg_part *mp = raw->last;
  int size = mp->part->chunk->buffer_size;
  if (size - mp->len - mp->offset >= alloc_bytes && mp->part->refcnt == 1) {
    raw->total_bytes += alloc_bytes;
    mp->len += alloc_bytes;
    raw->last_offset += alloc_bytes;
    return mp->part->data + mp->offset + mp->len - alloc_bytes;
  }
  struct msg_buffer *X = alloc_msg_buffer (mp->part, alloc_bytes);
  assert (X);
  size = X->chunk->buffer_size;
  assert (size >= alloc_bytes);
  
  mp = new_msg_part (raw->first, X);
  raw->last->next = mp;
  raw->last = mp;
  
  mp->len = alloc_bytes;
  mp->offset = 0;
  raw->last_offset = alloc_bytes;
  raw->total_bytes += alloc_bytes;
  
  return mp->part->data;
}


int rwm_prepare_iovec (const struct raw_message *raw, struct iovec *iov, int iov_len, int bytes) {
  assert (raw->magic == RM_INIT_MAGIC || raw->magic == RM_TMP_MAGIC);
  if (bytes > raw->total_bytes) {
    bytes = raw->total_bytes;
  }
  assert (bytes >= 0);
  int res = 0, total_bytes = raw->total_bytes, first_offset = raw->first_offset;
  struct msg_part *mp = raw->first;
  while (bytes > 0) {
    assert (mp);
    if (res == iov_len) {
      return -1;
    }
    int sz = (mp == raw->last ? raw->last_offset : mp->offset + mp->len) - first_offset;
    assert (sz >= 0 && sz <= mp->len);
    if (bytes < sz) {
      iov[res].iov_base = mp->part->data + first_offset;
      iov[res++].iov_len = bytes;
      return res;
    }
    iov[res].iov_base = mp->part->data + first_offset;
    iov[res++].iov_len = sz;
    bytes -= sz;
    total_bytes -= sz;
    if (!mp->next) {
      assert (mp == raw->last && !bytes && !total_bytes);
      return res;
    }
    mp = mp->next;
    first_offset = mp->offset;
  }
  return res;
}

int rwm_fetch_data_back (struct raw_message *raw, void *data, int bytes) {
  assert (raw->magic == RM_INIT_MAGIC || raw->magic == RM_TMP_MAGIC);
  if (bytes > raw->total_bytes) {
    bytes = raw->total_bytes;
  }
  assert (bytes >= 0);
  if (!bytes) {
    return 0;
  }

  if (bytes < raw->last_offset - (raw->last == raw->first ? raw->first_offset : raw->last->offset)) {
    if (data) {
      memcpy (data, raw->last->part->data + raw->last_offset - bytes, bytes);
    }
    raw->last_offset -= bytes;
    raw->total_bytes -= bytes;
    if (!raw->total_bytes) {
      rwm_clear (raw);
    }
    return bytes;
  }

  int skip = raw->total_bytes - bytes;
  int res = 0;
  struct msg_part *mp = raw->first;
  while (bytes > 0) {
    assert (mp);
    int sz = (mp == raw->last ? raw->last_offset : mp->offset + mp->len) - (mp == raw->first ? raw->first_offset : mp->offset);
    assert (sz >= 0 && sz <= mp->len);
    if (skip <= sz) {
      raw->last = mp;
      raw->last_offset = (mp == raw->first ? raw->first_offset : mp->offset) + skip;

//      int t = mp->len + mp->offset - raw->last_offset;
      int t = sz - skip;
      if (t > bytes) { t = bytes; }
      if (data) {
        memcpy (data, mp->part->data + raw->last_offset, t);
      }
      bytes -= (t);
      res += (t);
    
      if (data) {
        struct msg_part *m = mp->next;
        while (bytes) {
          assert (m);
          int t = (m->len > bytes) ? bytes : m->len;
          memcpy (data + res, m->part->data + m->offset, t);
          bytes -= t;
          res += t;
          m = m->next;
        }
      } else {
        res += bytes;
        bytes = 0;
      }
    } else {
      assert (mp != raw->last);
      skip -= sz;
      mp = mp->next;
    }
  }
  raw->total_bytes -= res;
  if (!raw->total_bytes) {
    msg_part_decref (raw->first);
    raw->first = raw->last = 0;
    raw->first_offset = raw->last_offset = 0;
  }
  return res;
}

int rwm_fetch_lookup_back (struct raw_message *raw, void *data, int bytes) {
  assert (raw->magic == RM_INIT_MAGIC || raw->magic == RM_TMP_MAGIC);
  if (bytes > raw->total_bytes) {
    bytes = raw->total_bytes;
  }
  assert (bytes >= 0);
  if (!bytes) {
    return 0;
  }

  if (bytes < raw->last_offset - (raw->last == raw->first ? raw->first_offset : raw->last->offset)) {
    if (data) {
      memcpy (data, raw->last->part->data + raw->last_offset - bytes, bytes);
    }
    return bytes;
  }

  int skip = raw->total_bytes - bytes;
  int res = 0;
  struct msg_part *mp = raw->first;
  while (bytes > 0) {
    assert (mp);
    int sz = (mp == raw->last ? raw->last_offset : mp->offset + mp->len) - (mp == raw->first ? raw->first_offset : mp->offset);
    assert (sz >= 0 && sz <= mp->len);
    if (skip <= sz) {
      int t = sz - skip;
      if (t > bytes) { t = bytes; }
      if (data) {
        memcpy (data, mp->part->data + raw->last_offset, t);
      }
      bytes -= (t);
      res += (t);
    
      if (data) {
        struct msg_part *m = mp->next;
        while (bytes) {
          assert (m);
          int t = (m->len > bytes) ? bytes : m->len;
          memcpy (data + res, m->part->data + m->offset, t);
          bytes -= t;
          res += t;
          m = m->next;
        }
      } else {
        res += bytes;
        bytes = 0;
      }
    } else {
      assert (mp != raw->last);
      skip -= sz;
      mp = mp->next;
    }
  }
  return res;
}

int rwm_trunc (struct raw_message *raw, int len) {
  if (len >= raw->total_bytes) { 
    return raw->total_bytes;
  }
  rwm_fetch_data_back (raw, 0, raw->total_bytes - len);
  return len;
}

int rwm_split (struct raw_message *raw, struct raw_message *tail, int bytes) {
  assert (bytes >= 0);
  rwm_total_msgs ++;
  tail->magic = raw->magic;
  if (bytes >= raw->total_bytes) { 
    tail->first = tail->last = 0;
    tail->first_offset = tail->last_offset = 0;
    tail->total_bytes = 0;
    return bytes == raw->total_bytes ? 0 : -1;
  }
  if (raw->total_bytes - bytes <= raw->last_offset - raw->last->offset) {
    int s = raw->total_bytes - bytes;
    raw->last_offset -= s;
    raw->total_bytes -= s;
    tail->first = tail->last = raw->last;
    tail->first->refcnt ++;
    tail->first_offset = raw->last_offset;
    tail->last_offset = raw->last_offset + s;
    tail->total_bytes = s;
    return 0;
  }
  tail->total_bytes = raw->total_bytes - bytes;
  raw->total_bytes = bytes;
  struct msg_part *mp = raw->first;
  while (bytes) {
    assert (mp);
    int sz = (mp == raw->last ? raw->last_offset : mp->offset + mp->len) - (mp == raw->first ? raw->first_offset : mp->offset);
    if (sz < bytes) {
      bytes -= sz;
      mp = mp->next;
    } else {
      tail->last = raw->last;
      tail->last_offset = raw->last_offset;
      raw->last = mp;
      raw->last_offset = (mp == raw->first ? raw->first_offset : mp->offset) + bytes;
      tail->first = mp;
      tail->first_offset = raw->last_offset;
      mp->refcnt ++;
      bytes = 0;
    }
  }
  return 0;
}

int rwm_split_head (struct raw_message *head, struct raw_message *raw, int bytes) {
  *head = *raw;
  return rwm_split (head, raw, bytes);
}

int rwm_union (struct raw_message *raw, struct raw_message *tail) {
//  assert (raw != tail);
  if (!raw->last) {
    *raw = *tail;
    rwm_total_msgs --;
    tail->magic = 0;
    return 0;
  } else if (tail->first) {
    
    if (raw->last->next || raw->last->len + raw->last->offset != raw->last_offset) {
      fork_message_chain (raw);
    }

    if (tail->first && tail->first->part == raw->last->part && tail->first_offset == raw->last_offset) {
      raw->last->next = tail->first->next;
      if (tail->first->next) {
        tail->first->next->refcnt ++;
      }
      int end = (tail->first == tail->last) ? tail->last_offset : tail->first->offset + tail->first->len;
      raw->last->len += end - tail->first_offset;

      raw->last_offset = tail->last_offset;
      raw->last = tail->last;
      raw->total_bytes += tail->total_bytes;
    } else {

      struct msg_part *mp = 0;
      if (tail->first->refcnt != 1 && tail->first_offset != tail->first->offset) {
        mp = new_msg_part (raw->last, tail->first->part);
        mp->next = tail->first->next;
        if (mp->next) { mp->next->refcnt ++; }
        mp->offset = tail->first_offset;
        mp->len = tail->first->len - tail->first_offset + tail->first->offset;
        mp->part->refcnt ++;
        raw->last->next = mp;
      } else {
        raw->last->next = tail->first;
        tail->first->refcnt ++;
        tail->first->len = tail->first->offset + tail->first->len - tail->first_offset;
        tail->first->offset = tail->first_offset;
      }

      raw->total_bytes += tail->total_bytes;
      if (tail->first != tail->last || !mp) {
        raw->last = tail->last;
        raw->last_offset = tail->last_offset;
      } else {
        raw->last = mp;
        raw->last_offset = tail->last_offset;
      }
    }
  }
  rwm_free (tail);
  return 0;
}

int rwm_dump_sizes (struct raw_message *raw) {
  if (!raw->first) { 
    fprintf (stderr, "( ) # %d\n", raw->total_bytes);
    assert (!raw->total_bytes);
  } else {
    int total_size  = 0;
    struct msg_part *mp = raw->first;
    fprintf (stderr, "(");
    while (mp != 0) {
      int size = (mp == raw->last ? raw->last_offset : mp->offset + mp->len) - (mp == raw->first ? raw->first_offset : mp->offset);
      fprintf (stderr, " %d", size);
      total_size += size;
      if (mp == raw->last) { break; }
      mp = mp->next;
    }
    assert (mp == raw->last);
    fprintf (stderr, " ) # %d\n", raw->total_bytes);
    assert (total_size == raw->total_bytes);
  }
  return 0;
}

int rwm_check (struct raw_message *raw) {
  assert (raw->magic == RM_INIT_MAGIC || raw->magic == RM_TMP_MAGIC);
  if (!raw->first) { 
    assert (!raw->total_bytes);
  } else {
    int total_size  = 0;
    struct msg_part *mp = raw->first;
    while (mp != 0) {
      int size = (mp == raw->last ? raw->last_offset : mp->offset + mp->len) - (mp == raw->first ? raw->first_offset : mp->offset);
      total_size += size;
      if (mp == raw->last) { break; }
      mp = mp->next;
    }
    assert (mp == raw->last);
    assert (total_size == raw->total_bytes);
  }
  return 0;
}

int rwm_dump (struct raw_message *raw) {
  assert (raw->magic == RM_INIT_MAGIC || raw->magic == RM_TMP_MAGIC);
  struct raw_message t;
  rwm_clone (&t, raw);
  static char R[10004];
  int r = rwm_fetch_data (&t, R, 10004);
  int x = (r > 10000) ? 10000 : r;
  hexdump (R, R + x);
  if (r > x) {
    fprintf (stderr, "%d bytes not printed\n", raw->total_bytes - x);
  }
  rwm_free (&t);
  return 0;
}

int rwm_process_and_advance (struct raw_message *raw, int bytes, void (*process_block)(void *extra, const void *data, int len), void *extra) {
  assert (raw->magic == RM_INIT_MAGIC || raw->magic == RM_TMP_MAGIC);
  if (bytes > raw->total_bytes) {
    bytes = raw->total_bytes;
  }
  assert (bytes >= 0);
  if (!bytes) {
    return 0;
  }
  int res = 0;
  struct msg_part *mp = raw->first, *mpn;
  while (bytes > 0) {
    assert (mp);
    int sz = (mp == raw->last ? raw->last_offset : mp->offset + mp->len) - raw->first_offset;
    assert (sz >= 0 && sz <= mp->len);
    if (bytes < sz) {
      if (bytes > 0) {
        process_block (extra, mp->part->data + raw->first_offset, bytes);
      }
      raw->first_offset += bytes;
      raw->total_bytes -= bytes;
      return res + bytes;
    }
    if (sz > 0) {
      process_block (extra, mp->part->data + raw->first_offset, sz);
    }
    res += sz;
    bytes -= sz;
    raw->total_bytes -= sz;
    mpn = mp->next;
    int deleted = 0;
    if (raw->magic == RM_INIT_MAGIC && !--mp->refcnt) {
      int buffer_refcnt = --mp->part->refcnt;
      if (!buffer_refcnt) {
        free_msg_buffer (mp->part);
      } else {
        assert (buffer_refcnt > 0);
      }
      mp->part = 0;
      mp->next = 0;
      free_msg_part (mp);
      deleted = 1;
    }
    if (!mpn || raw->first == raw->last) {
      if (mpn && deleted) {
        msg_part_decref (mpn);
      }
      assert (mp == raw->last && !bytes && !raw->total_bytes);
      raw->first = raw->last = 0;
      raw->first_offset = raw->last_offset = 0;
      return res;
    }
    mp = mpn;
    raw->first = mp;
    raw->first_offset = mp->offset;
    assert (mp->refcnt > 0);
    if (!deleted) {
      mp->refcnt ++;
    }
  }
  return res;
}

int rwm_process (struct raw_message *raw, int bytes, void (*process_block)(void *extra, const void *data, int len), void *extra) {
  assert (bytes >= 0);
  if (bytes > raw->total_bytes) {
    bytes = raw->total_bytes;    
  }
  if (!bytes) { return 0; }
  int x = bytes;
  if (raw->first) {
    struct msg_part *mp = raw->first;
    while (mp) {
      int start = (mp == raw->first) ? raw->first_offset : mp->offset;
      int len = (mp == raw->last) ? raw->last_offset - start: mp->len + mp->offset - start;
      if (len >= bytes) {
        process_block (extra, mp->part->data + start, bytes);
        bytes = 0;
        break;
      } 
      process_block (extra, mp->part->data + start, len);
      bytes -= len;
      mp = mp->next;
    }
    assert (!bytes);
  }
  return x;
}

int rwm_sha1 (struct raw_message *raw, int bytes, unsigned char output[20]) {
  static SHA_CTX ctx;

  SHA1_Init (&ctx);
  int res = rwm_process (raw, bytes, (void *)SHA1_Update, &ctx);
  SHA1_Final (output, &ctx);

  return res;
}

void crc32c_process (void *extra, const void *data, int len) {
  unsigned crc32c = *(unsigned *)extra;
  *(unsigned *)extra = crc32c_partial (data, len, crc32c);
}

unsigned rwm_crc32c (struct raw_message *raw, int bytes) {
  unsigned crc32c = ~0;

  assert (rwm_process (raw, bytes, (void *)crc32c_process, &crc32c) == bytes);

  return ~crc32c;
}

void crc32_process (void *extra, const void *data, int len) {
  unsigned crc32 = *(unsigned *)extra;
  *(unsigned *)extra = crc32_partial (data, len, crc32);
}

unsigned rwm_crc32 (struct raw_message *raw, int bytes) {
  unsigned crc32 = ~0;

  assert (rwm_process (raw, bytes, (void *)crc32_process, &crc32) == bytes);

  return ~crc32;
}

void rwm_process_memcpy (void *extra, const void *data, int len) {
  char **d = extra;
  memcpy (*d, data, len);
  *d += len;
}

void rwm_process_nop (void *extra, const void *data, int len) {
}

int rwm_fetch_data (struct raw_message *raw, void *buf, int bytes) {
  if (buf) {
    return rwm_process_and_advance (raw, bytes, rwm_process_memcpy, &buf);
  } else {
    return rwm_process_and_advance (raw, bytes, rwm_process_nop, 0);
  }
}

int rwm_fetch_lookup (struct raw_message *raw, void *buf, int bytes) {
  if (buf) {
    return rwm_process (raw, bytes, rwm_process_memcpy, &buf);
  } else {
    return rwm_process (raw, bytes, rwm_process_nop, 0);
  }
}

int rwm_fork_deep (struct raw_message *raw) {
  if (!raw->first) {
    return 0;
  }
  struct msg_part **mpp = &raw->first;
  struct msg_part *mp = raw->first;
  struct msg_part *prev = 0;
  int remaining = raw->total_bytes;
  while (mp) {
    int start = (mp == raw->first) ? raw->first_offset : mp->offset;
    int len = (mp == raw->last) ? raw->last_offset - start : mp->len + mp->offset - start;
    if (mp->refcnt > 1) {
      mp->refcnt --;
      int x = 0;
      while (remaining) {
        int alloc_size = remaining >= MSG_SMALL_BUFFER ? MSG_STD_BUFFER : MSG_SMALL_BUFFER;
        struct msg_buffer *b = alloc_msg_buffer (mp->part, alloc_size);
        int t = alloc_size > remaining ? remaining : alloc_size;
        x = 0;
        while (t) {
          if (len < t) {
            memcpy (b->data + x, mp->part->data + start, len);
            x += len;
            t -= len;
            mp = mp->next;
            assert (mp);
            start = mp->offset;
            len = (mp == raw->last) ? raw->last_offset - start : mp->len;
          } else {
            memcpy (b->data + x, mp->part->data + start, t);
            x += t;
            start += t;
            len -= t;
            t = 0;
          }
        }
        struct msg_part *np = new_msg_part (prev, b);        
        if (mpp == &raw->first) {
          raw->first_offset = 0;
        }
        *mpp = np;
        prev = np;
        remaining -= x;
        np->offset = 0;
        np->len = x;
      }
      assert (mp == raw->last && start == raw->last_offset && !len);
      raw->last = prev;
      raw->last_offset = x;
      return 1;
    } else {
      if (mp->part->refcnt > 1) {
        mp->part->refcnt --;
        struct msg_buffer *b = alloc_msg_buffer (mp->part, mp->part->chunk->buffer_size);
        assert (b);
        memcpy (b->data + mp->offset, mp->part->data + mp->offset, mp->len);
        mp->part = b;
      }
      remaining -= len;
    }
    if (mp == raw->last) {
      assert (!remaining);
      break;
    }
    prev = mp;
    mpp = &mp->next;
    mp = mp->next;
  }
  return 0;
}

struct rwm_encrypt_decrypt_tmp  {
  char buf[16];
  int bp;
  int buf_left;
  int left;
  struct raw_message *raw;
  struct vk_aes_ctx *ctx;
  void (*crypt)(struct vk_aes_ctx *, const void *, void *, int, unsigned char *);
  unsigned char *iv;
};

void rwm_process_encrypt_decrypt (struct rwm_encrypt_decrypt_tmp *x, const void *data, int len) {
  struct raw_message *res = x->raw;
  if (!x->buf_left) {
    struct msg_buffer *X = alloc_msg_buffer (res->last->part, x->left >= MSG_STD_BUFFER ? MSG_STD_BUFFER : x->left);
    struct msg_part *mp = new_msg_part (res->last, X);
    res->last->next = mp;
    res->last = mp;
    res->last_offset = 0;
    x->buf_left = X->chunk->buffer_size;
  }
  x->left -= len;
  if (x->bp) {
    if (len >= 16 - x->bp) {
      memcpy (x->buf + x->bp, data, 16 - x->bp);
      len -= 16 - x->bp;
      data += 16 - x->bp;
      x->bp = 0;      
      x->crypt (x->ctx, x->buf, res->last->part->data + res->last_offset, 16, x->iv);
      res->last->len += 16;
      res->last_offset += 16;
      res->total_bytes += 16;
      x->buf_left -= 16;
    } else {
      memcpy (x->buf + x->bp, data, len);
      x->bp += len;
      return;
    }
  }
  if (len & 15) {
    int l = len & ~15;
    memcpy (x->buf, data + l, len - l);
    x->bp = len - l;
    len = l;
  }
  while (1) {
    if (!x->buf_left) {
      struct msg_buffer *X = alloc_msg_buffer (res->last->part, x->left + len >= MSG_STD_BUFFER ? MSG_STD_BUFFER : x->left + len);
      struct msg_part *mp = new_msg_part (res->last, X);
      res->last->next = mp;
      res->last = mp;
      res->last_offset = 0;
      x->buf_left = X->chunk->buffer_size;
    }
    if (len <= x->buf_left) {
      x->crypt (x->ctx, data, (res->last->part->data + res->last_offset), len, x->iv);
      res->last->len += len;
      res->last_offset += len;
      res->total_bytes += len;
      x->buf_left -= len;
      return;
    } else {
      int t = x->buf_left;
      x->crypt (x->ctx, data, res->last->part->data + res->last_offset, t, x->iv);
      res->last->len += t;
      res->last_offset += t;
      res->total_bytes += t;
      data += t;
      len -= t;
      x->buf_left -= t;
    }
  }
}


int rwm_encrypt_decrypt_to (struct raw_message *raw, struct raw_message *res, int bytes, struct vk_aes_ctx *ctx, void (*crypt)(struct vk_aes_ctx *ctx, const void *src, void *dst, int l, unsigned char *iv), unsigned char *iv) {
  assert (bytes >= 0);
  if (bytes > raw->total_bytes) {
    bytes = raw->total_bytes;
  }
  bytes &= ~15;
  if (!bytes) {
    return 0;
  }
  if (res->last && (res->last->next || res->last->offset != res->last_offset)) {
    fork_message_chain (res);
  }
  if (!res->last || res->last->part->refcnt != 1) {
    int l = res->last ? bytes : bytes + RM_PREPEND_RESERVE;
    struct msg_buffer *X = alloc_msg_buffer (res->last ? res->last->part : 0, l >= MSG_STD_BUFFER ? MSG_STD_BUFFER : l);
    struct msg_part *mp = new_msg_part (res->last, X);
    if (res->last) {
      res->last->next = mp;
      res->last = mp;
      res->last_offset = 0;
    } else {
      res->last = res->first = mp;
      res->last_offset = res->first_offset = mp->offset = RM_PREPEND_RESERVE;
      mp->len = 0;
    }
  }
  static struct rwm_encrypt_decrypt_tmp t;
  assert (!t.bp);
  t.crypt = crypt;
  t.buf_left = res->last->part->chunk->buffer_size - res->last_offset;
  t.raw = res;
  t.ctx = ctx;
  t.iv = iv;
  t.left = bytes;
  return rwm_process_and_advance (raw, bytes, (void *)rwm_process_encrypt_decrypt, &t);
}

int _rwm_encrypt_decrypt (struct raw_message *raw, int bytes, struct vk_aes_ctx *ctx, int mode, unsigned char *iv) {
  assert (bytes >= 0);
  if (bytes > raw->total_bytes) {
    bytes = raw->total_bytes;
  }
  bytes &= ~15;
  if (!bytes) {
    return 0;
  }
  int s = bytes;
  rwm_fork_deep (raw);
//  assert (raw->total_bytes % 16 == 0);

  struct msg_part *mp = raw->first;
  int start = (mp == raw->first) ? raw->first_offset : mp->offset;
  int len = (mp == raw->last) ? raw->last_offset - start : mp->len + mp->offset - start;  
  while (bytes) {
    assert (start >= 0);
    assert (len >= 0);
    while (len >= 16) {
      int l = len < bytes ? (len & ~15) : bytes;
      if (!mode) {
        ctx->ige_crypt (ctx, (void *)(mp->part->data + start), (void *)(mp->part->data + start), l, iv);
      } else {
        ctx->cbc_crypt (ctx, (void *)(mp->part->data + start), (void *)(mp->part->data + start), l, iv);
      }
      start += l;
      bytes -= l;
      len = len & 15;
    }
    if (len && bytes) {
      static unsigned char c[16];
      int p = 0;
      int _len = len;
      int _start = start;
      struct msg_part *_mp = mp;
      while (p < 16) {
        int x = (len > 16 - p) ? 16 - p : len;
        memcpy (c + p, mp->part->data + start, x);
        p += x;        
        if (len == x) {
          mp = mp->next;
          if (mp) {
            start = mp->offset;
            len = (mp == raw->last) ? raw->last_offset - start : mp->len;
          } else {
            start = -1;
            len = -1;
            assert (p == 16);
          }
        } else {
          break;
        }
      }
      if (!mode) {
        ctx->ige_crypt (ctx, c, c, 16, iv);
      } else {
        ctx->cbc_crypt (ctx, c, c, 16, iv);
      }
      len = _len;
      start = _start;
      mp = _mp;
      p = 0;
      while (p < 16) {
        int x = (len > 16 - p) ? 16 - p : len;
        memcpy (mp->part->data + start, c + p, x);
        p += x;        
        if (len == x) {
          mp = mp->next;
          if (mp) {
            start = mp->offset;
            len = (mp == raw->last) ? raw->last_offset - start : mp->len;
          } else {
            start = -1;
            len = -1;
            assert (p == 16);
          }
        } else {
          start += x;
          len -= x;
          break;
        }
      }
      bytes -= 16;
    } else {
      mp = mp->next;
      if (mp) {
        start = mp->offset;
        len = (mp == raw->last) ? raw->last_offset - start : mp->len;
      } else {
        start = -1;
        len = -1;
      }
    }    
  }
//  assert (!mp || (raw->last->next == mp) || (mp == raw->last && start == raw->last_offset && !len));
  return s;
}

int rwm_encrypt_decrypt (struct raw_message *raw, int bytes, struct vk_aes_ctx *ctx, unsigned char iv[32]) {
  return _rwm_encrypt_decrypt (raw, bytes, ctx, 0, iv);
}

int rwm_encrypt_decrypt_cbc (struct raw_message *raw, int bytes, struct vk_aes_ctx *ctx, unsigned char iv[16]) {
  return _rwm_encrypt_decrypt (raw, bytes, ctx, 1, iv);
}
