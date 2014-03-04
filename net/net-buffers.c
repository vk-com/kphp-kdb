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
              2008-2011 Nikolai Durov
              2008-2011 Andrei Lopatin
              2012-2013 Vitaliy Valtman
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
#include "crc32.h"

#include "net-buffers.h"

#define NET_BUFFER_SIZE	(1 << 14)
#define NET_BUFFERS	(1 << 14)
#define NET_BUFFER_ALIGN	(1 << 12)

#define	NETBUFF_DATA_SIZE	(NET_BUFFER_SIZE - sizeof(netbuffer_t) + sizeof(int))

extern int verbosity;

netbuffer_t NB_Head;
int NB_used, NB_free, NB_alloc, NB_max, NB_size = NET_BUFFER_SIZE;

char *NB_Alloc, NetBufferSpace[NET_BUFFER_ALIGN + NET_BUFFER_SIZE*NET_BUFFERS];


void init_netbuffers (void) {
  NB_max = NET_BUFFERS;
  NB_Head.state = NB_MAGIC_ALLOCA;
  NB_Head.prev = NB_Head.next = &NB_Head;
  NB_Alloc = &NetBufferSpace[(NET_BUFFER_ALIGN - (long) NetBufferSpace) & (NET_BUFFER_ALIGN - 1)];
}

netbuffer_t *init_builtin_buffer (netbuffer_t *H, char *buf, int len) {
  assert (len >= 0 && len < (1L << 28));
  assert (buf || !len);
  assert (H);
  H->state = NB_MAGIC_HEAD;
  H->parent = 0;
  H->prev = H->next = H;
  H->rptr = H->wptr = H->start = buf;
  H->pptr = 0;
  H->end = buf + len;
  H->extra = 0;
  H->total_bytes = 0;
  H->unprocessed_bytes = 0;
  return H;
}

netbuffer_t *alloc_buffer (void) {
  netbuffer_t *R;
  if (!NB_Head.state) { 
    init_netbuffers(); 
  }
  if (!NB_free && NB_alloc >= NET_BUFFERS) {
    if (verbosity >= 0) {
      fprintf (stderr, "out of network buffers: allocated=%d, %p..%p, free=0\n", NB_alloc, NetBufferSpace, NB_Alloc);
    }
    return 0;
  }
  R = NB_Head.next;
  if (R != &NB_Head) {
    assert (NB_free > 0);
    assert (R->state == NB_MAGIC_FREE);
    R->next->prev = &NB_Head;
    NB_Head.next = R->next;
    NB_free--;
  } else {
    assert (!NB_free && NB_alloc < NET_BUFFERS);
    R = (netbuffer_t *) NB_Alloc;
    R->end = NB_Alloc += NET_BUFFER_SIZE;
    NB_alloc++;
    assert (NB_Alloc <= NetBufferSpace + NET_BUFFER_SIZE*NET_BUFFERS + NET_BUFFER_ALIGN);
  }
  NB_used++;
  R->state = NB_MAGIC_BUSY;
  R->prev = R->next = 0;
  R->parent = 0;
  R->start = ((char *) R) + offsetof(netbuffer_t, extra);
  R->rptr = R->wptr = R->start;
  R->pptr = 0;
  R->end = ((char *) R) + NET_BUFFER_SIZE;
  return R;
}

netbuffer_t *alloc_head_buffer (void) {
  netbuffer_t *R = alloc_buffer();
  if (!R) {
    return 0;
  }
  R->rptr = R->wptr = R->start += sizeof(netbuffer_t) - offsetof(netbuffer_t, extra);
  R->pptr = 0;
  R->extra = 0;
  R->total_bytes = 0;
  R->unprocessed_bytes = 0;
  R->state = NB_MAGIC_BUSYHEAD;
  R->prev = R->next = R;

  return R;
} 

int free_buffer (netbuffer_t *R) {
  netbuffer_t *S = NB_Head.next;
  assert (R->state == NB_MAGIC_BUSY || (R->state == NB_MAGIC_BUSYHEAD && R->prev == R && R->next == R));
  R->rptr = R->wptr = R->pptr = R->end = 0;
  R->state = NB_MAGIC_FREE;
  R->prev = &NB_Head;
  R->next = S;
  S->prev = R;
  NB_Head.next = R;
  NB_used--;
  NB_free++;
  return 0;
}

char *get_write_ptr (netbuffer_t *H, int len) {
  netbuffer_t *X = H->prev, *Y;
  assert ((unsigned long) len < NET_BUFFER_SIZE);
  assert (H->state == NB_MAGIC_HEAD || H->state == NB_MAGIC_BUSYHEAD);
  if (X->wptr + len <= X->end) {
    return X->wptr;
  }
  Y = alloc_buffer();
  if (!Y) { return 0; }
  H->extra++;
  X->next = Y;
  Y->prev = X;
  Y->next = H;
  H->prev = Y;
  if (X->pptr) {
    Y->pptr = Y->rptr;
  }
  assert (Y->wptr + len <= Y->end);
  return Y->wptr;
}

void free_all_buffers (netbuffer_t *H) {
  netbuffer_t *X, *Y;
  if (!H) { return; }
  assert (H->state == NB_MAGIC_HEAD || H->state == NB_MAGIC_BUSYHEAD);
  X = H->next;
  while (X != H) {
    assert (H->extra > 0);
    Y = X->next;
    free_buffer (X);
    H->extra--;
    X = Y;
  }
  assert (!H->extra);
  H->total_bytes = 0;
  H->unprocessed_bytes = 0;
  H->prev = H->next = H;
  H->rptr = H->wptr = H->start;
  H->pptr = 0;

  if (H->state == NB_MAGIC_BUSYHEAD) {
    free_buffer (H);
  }
}

void free_unused_buffers (netbuffer_t *H) {
  netbuffer_t *X = H->next, *Y;
  assert (H->state == NB_MAGIC_HEAD || H->state == NB_MAGIC_BUSYHEAD);
  if (H->rptr == H->wptr) {
    H->rptr = H->wptr = H->start;
    if (H->pptr) {
      H->pptr = H->wptr;
    }
  }
  while (X != H && X->rptr == X->wptr) {
    //fprintf (stderr, "freeing unused write buffer %p (prev=%p) for connection %d, total=%d\n",
    //	X, X->prev, H->extra);
    H->next = Y = X->next;
    Y->prev = H;
    assert (H->extra > 0);
    free_buffer(X);
    H->extra--;
    X = Y;
  }
  if (X == H) {
    assert (!H->extra);
    return;
  }
  X = H->prev;
  assert (X != H);
  while (X->rptr == X->wptr) {
    //fprintf (stderr, "freeing unused write buffer %p (prev=%p) for connection %d, total=%d\n",
    //	X, X->prev, H->extra);
    H->prev = Y = X->prev;
    assert (Y != H && H->extra > 1);
    Y->next = H;
    free_buffer(X);
    H->extra--;
    X = Y;
    assert (X != H);
  }
}

void advance_write_ptr (netbuffer_t *H, int offset) {
  netbuffer_t *X = H->prev;
  assert (H->state == NB_MAGIC_HEAD || H->state == NB_MAGIC_BUSYHEAD);
  assert (offset > 0 && offset <= NET_BUFFER_SIZE);
  X->wptr += offset;
  assert (X->wptr <= X->end);
  if (!X->pptr) {
    H->total_bytes += offset;
  } else {
    H->unprocessed_bytes += offset;
  }
}

void advance_read_ptr (netbuffer_t *H, int offset) {
  netbuffer_t *X = H, *Y;
  assert (H->state == NB_MAGIC_HEAD || H->state == NB_MAGIC_BUSYHEAD);
  assert (offset >= 0 && offset <= NET_BUFFER_SIZE);
  if (H->rptr == H->wptr) {
    X = H->next;
    X->rptr += offset;
    assert (X->rptr <= X->wptr);
    assert (!X->pptr || X->rptr <= X->pptr);
    if (X->rptr == X->wptr) {
      Y = X->next;
      assert (H->extra > 0);	// may fail if buffer chain empty and offset=0
      H->next = Y;
      Y->prev = H;
      free_buffer (X);
      H->extra--;
    }
  } else {
    H->rptr += offset;
    assert (H->rptr <= H->wptr);
    assert (!H->pptr || H->rptr <= H->pptr);
    if (H->rptr == H->wptr) {
      H->rptr = H->wptr = H->start;
      if (H->pptr) {
        H->pptr = H->wptr;
      }
    }
  }
  H->total_bytes -= offset;
}

int advance_skip_read_ptr (netbuffer_t *H, int len) {
  netbuffer_t *X = H, *Y;
  int s, t = 0, w = 0;
  if (X->wptr == X->rptr) {
    X = X->next;
  }
  while (len > 0) {
    s = X->wptr - X->rptr;
    if (X->pptr && X->pptr < X->wptr) {
      s = X->pptr - X->rptr;
      t = 1;
    }
    if (s > len) { s = len; }
    if (s > 0) {
      w += s;
      X->rptr += s;
      len -= s;
    }
    if (X->rptr == X->wptr) {
      if (X == H) { 
	X->rptr = X->wptr = X->start;
	if (X->pptr) {
	  X->pptr = X->wptr;
	}
	X = X->next;
      } else {
	Y = X->next;
	assert (H->extra > 0);
	Y->prev = H;
	H->next = Y;
	free_buffer (X);
	H->extra--;
	X = Y;
      }
      if (X == H) { break; }
    }
    if (!len || t) { break; }
  }
  H->total_bytes -= w;
  return w;
}

int force_ready_bytes (netbuffer_t *H, int sz) {
  int u, v, w;
  netbuffer_t *X, *Y;
  assert (H->state == NB_MAGIC_HEAD || H->state == NB_MAGIC_BUSYHEAD);
  assert (sz >= 0);
  u = H->wptr - H->rptr;
  if (H->pptr && H->pptr != H->wptr) {
    assert (H->rptr <= H->pptr && H->pptr < H->wptr);
    return H->pptr - H->rptr;
  }
  if (sz <= u || H->next == H) { return u; }
  if (sz > NET_BUFFER_SIZE) { sz = NET_BUFFER_SIZE; }
  X = H->next;
  v = (X->pptr ? X->pptr : X->wptr) - X->rptr;
  assert ((unsigned) v <= NET_BUFFER_SIZE);
  if (u + v >= sz || X->next == H) {
    if (!v) { return u; }
    if (u <= X->rptr - X->start) {
      X->rptr -= u;
      memcpy (X->rptr, H->rptr, u);
      H->rptr = H->wptr = H->start;
      if (H->pptr) {
        H->pptr = H->start;
      }
      return u + v;
    }
    if (v > sz - u) { v = sz - u; }
    w = H->end - H->wptr;
    if (v > w) {
      memmove (H->start, H->rptr, u);
      H->rptr = H->start;
      H->wptr = H->start + u;
      w = H->end - H->wptr;
    }
    if (v > w) { v = w; }
    memcpy (H->wptr, X->rptr, v);
    X->rptr += v;
    H->wptr += v;
    if (H->pptr) {
      H->pptr = H->wptr;
    }
    if (X->rptr == X->wptr) {
      Y = X->next;
      assert (H->extra > 0);
      H->next = Y;
      Y->prev = H;
      free_buffer (X);
      H->extra--;
    }
    return u + v;
  }
  // this case is quite rare
  if (u > 0) {
    memmove (H->start, H->rptr, u);
  }
  H->rptr = H->wptr = H->start;
  if (H->pptr) {
    H->pptr = H->wptr;
  }
  if (sz > H->end - H->start) {
    sz = H->end - H->start;
  }
  w = read_in (H, H->wptr + u, sz - u);
  v = u + w;
  H->total_bytes += w;
  H->wptr += v;
  if (H->pptr) {
    H->pptr = H->wptr;
  }
  return v;
}

int get_ready_bytes (netbuffer_t *H) {
  int u;
  assert (H->state == NB_MAGIC_HEAD || H->state == NB_MAGIC_BUSYHEAD);
  u = H->wptr - H->rptr;
  if (!u) {
    H = H->next;
  }
  return (H->pptr ? H->pptr : H->wptr) - H->rptr;
}

// TODO: replace with { return H->total_bytes; }
// current version is for debug purposes ONLY
int get_total_ready_bytes (netbuffer_t *H) {
  netbuffer_t *X;
  int u, v, t = 0;
  assert (H->state == NB_MAGIC_HEAD || H->state == NB_MAGIC_BUSYHEAD);
  u = (H->pptr ? H->pptr : H->wptr) - H->rptr;
  assert (u >= 0);
  X = H->next;
  while (X != H) {
    v = X->wptr - X->rptr;
    if (X->pptr && X->pptr < X->wptr) {
      v = X->pptr - X->rptr;
      t = 1;
    }
    assert (v >= 0 && v <= NET_BUFFER_SIZE);
    u += v;
    if (t) {
      break;
    }
    X = X->next;
  }
  assert (u == H->total_bytes);
  return u;
}

int get_write_space (netbuffer_t *H) {
  assert (H->state == NB_MAGIC_HEAD || H->state == NB_MAGIC_BUSYHEAD);
  return H->prev->end - H->prev->wptr;
}

char *get_read_ptr (netbuffer_t *H) {
  assert (H->state == NB_MAGIC_HEAD || H->state == NB_MAGIC_BUSYHEAD);
  if (H->rptr < H->wptr) {
    return H->rptr != H->pptr ? H->rptr : 0;
  }
  netbuffer_t *X = H->next;
  if (X != H && X->rptr < (X->pptr ? X->pptr : X->wptr)) {
    return X->rptr;
  }
  return 0;
}

int write_out (netbuffer_t *H, const void *__data, int len) {
  netbuffer_t *X = H->prev, *Y;
  int s, w = 0;
  const char *data = __data;

  while (len > 0) {
    s = X->end - X->wptr;
    if (s > len) { s = len; }
    if (s > 0) {
      memcpy (X->wptr, data, s);
      w += s;
      X->wptr += s;
      data += s;
      len -= s;
    }
    if (!len) { break; }
    Y = alloc_buffer();
    if (!Y) { break; }
    X->next = Y;
    Y->prev = X;
    Y->next = H;
    H->prev = Y;
    H->extra++;
    if (X->pptr) {
      Y->pptr = Y->rptr;
    }
    X = Y;
  }
  if (H->pptr) {
    H->unprocessed_bytes += w;
  } else {
    H->total_bytes += w;
  }
  return w;
}

int read_in (netbuffer_t *H, void *__data, int len) {
  netbuffer_t *X = H, *Y;
  int s, w = 0;
  char *data = __data;
  if (X->wptr == X->rptr) {
    X = X->next;
  }
  while (len > 0) {
    s = (X->pptr ? X->pptr : X->wptr) - X->rptr;
    if (s > len) { s = len; }
    if (s > 0) {
      memcpy (data, X->rptr, s);
      w += s;
      X->rptr += s;
      data += s;
      len -= s;
    }
    if (X->rptr == X->wptr) {
      if (X == H) { 
	X->rptr = X->wptr = X->start;
	if (X->pptr) {
	  X->pptr = X->wptr;
	}
	X = X->next;
      } else {
	Y = X->next;
	assert (H->extra > 0);
	Y->prev = H;
	H->next = Y;
	free_buffer (X);
	H->extra--;
	X = Y;
      }
      if (X == H) { break; }
    } else if (X->rptr == X->pptr) {
      break;
    }
    if (!len) { break; }
  }
  H->total_bytes -= w;
  return w;
}

int copy_through (netbuffer_t *HD, netbuffer_t *H, int len) {
  netbuffer_t *X = H, *Y;
  int s, w = 0;
  if (X->wptr == X->rptr) {
    X = X->next;
  }
  while (len > 0) {
    s = (X->pptr ? X->pptr : X->wptr) - X->rptr;
    if (s > len) { s = len; }
    if (s > 0) {
      w += write_out (HD, X->rptr, s);
      X->rptr += s;
      len -= s;
      H->total_bytes -= s;
    }
    if (X->rptr == X->wptr) {
      if (X == H) { 
	X->rptr = X->wptr = X->start;
	if (X->pptr) {
	  X->pptr = X->wptr;
	}
	X = X->next;
      } else {
	Y = X->next;
	assert (H->extra > 0);
	Y->prev = H;
	H->next = Y;
	free_buffer (X);
	H->extra--;
	X = Y;
      }
      if (X == H) { break; }
    } else if (X->rptr == X->pptr) {
      break;
    }
  }
  return w;
}

int copy_through_nondestruct (netbuffer_t *HD, netbuffer_t *H, int len) {
  netbuffer_t *X = H;
  int s, w = 0;
  char *rptr;
  if (X->wptr == X->rptr) {
    X = X->next;
  }
  rptr = X->rptr;
  while (len > 0) {
    s = (X->pptr ? X->pptr : X->wptr) - rptr;
    if (s > len) { s = len; }
    if (s > 0) {
      w += write_out (HD, rptr, s);
      rptr += s;
      len -= s;
    }
    if (rptr == X->wptr) {
      X = X->next;
      rptr = X->rptr;
      if (X == H) { break; }
    } else if (rptr == X->pptr) {
      break;
    }
  }
  return w;
}

int read_back (netbuffer_t *H, void *__data, int len) {
  netbuffer_t *X = H->prev, *Y;
  int s, w = 0;
  char *data = __data ? (char *) __data + len : 0;
  if (H->pptr) {
    if (len > H->unprocessed_bytes) {
      len = H->unprocessed_bytes;
    }
  } else {
    if (len > H->total_bytes) {
      len = H->total_bytes;
    }
  }

  if (X->wptr == X->rptr && X != H) {
    X = X->prev;
  }

  while (len > 0) {
    s = X->wptr - (X->pptr ? X->pptr : X->rptr);
    if (s > len) { s = len; }
    if (s > 0) {
      X->wptr -= s;
      if (data) {
	memcpy (data -= s, X->wptr, s);
      }
      w += s;
      len -= s;
    }
    if (X->rptr == X->wptr) {
      if (X == H) { 
	X->rptr = X->wptr = X->start;
	if (X->pptr) {
	  X->pptr = X->wptr;
	}
	break;
      } else {
	Y = X->prev;
	assert (H->extra > 0);
	Y->next = H;
	H->prev = Y;
	free_buffer (X);
	H->extra--;
	X = Y;
      }
    } else if (X->wptr == X->pptr) {
      break;
    }
  }

  if (H->pptr) {
    H->unprocessed_bytes -= w;
  } else {
    H->total_bytes -= w;
  }
  return w;
}

int read_back_nondestruct (netbuffer_t *H, void *__data, int len) {
  netbuffer_t *X = H->prev;
  int s, w = 0;
  char *data = __data ? (char *) __data + len : 0;
  if (H->pptr) {
    if (len > H->unprocessed_bytes) {
      len = H->unprocessed_bytes;
    }
  } else {
    if (len > H->total_bytes) {
      len = H->total_bytes;
    }
  }

  if (X->wptr == X->rptr && X != H) {
    X = X->prev;
  }

  while (len > 0) {
    s = X->wptr - (X->pptr ? X->pptr : X->rptr);
    if (s > len) { s = len; }
    if (s > 0) {
      if (data) {
        memcpy (data -= s, X->wptr - s, s);
      }
      w += s;
      len -= s;
    }
    if (len > 0) {
      X = X->prev;
    }
  }

  return w;
}

unsigned count_crc32_back_partial (netbuffer_t *H, int len, unsigned complement_crc32) {
  netbuffer_t *X = H->prev;
  int total_bytes = X->pptr ? H->unprocessed_bytes : H->total_bytes;
  if (len > total_bytes) {
    fprintf (stderr, "**ERROR** len=%d total_bytes=%d\n", len, total_bytes);
  }
  assert (len <= total_bytes);

  if (X->wptr == X->rptr && X != H) {
    X = X->prev;
  }

  while (len > 0) {
    int s = X->wptr - (X->pptr ? X->pptr : X->rptr);
    len -= s;
    if (len > 0) {
      X = X->prev;
    }
  }
  complement_crc32 = crc32_partial ((X->pptr ? X->pptr : X->rptr) - len, X->wptr - (X->pptr ? X->pptr : X->rptr) + len, complement_crc32);
  X = X->next;
  while (X != H) {
    complement_crc32 = crc32_partial ((X->pptr ? X->pptr : X->rptr), X->wptr - (X->pptr ? X->pptr : X->rptr), complement_crc32);
    X = X->next;
  }
  return complement_crc32;  
}

inline int nbit_clear (nb_iterator_t *I) {
  I->cur = I->head = 0;
  I->cptr = 0;
  return 0;
}

int nbit_rewind (nb_iterator_t *I) {
  netbuffer_t *H = I->head;
  if (!H) {
    return nbit_clear (I);
  }
  assert (H->state == NB_MAGIC_HEAD || H->state == NB_MAGIC_BUSYHEAD);

  netbuffer_t *X = H;
  I->head = H;
  while (X->rptr == X->wptr && X->next != H) {
    X = X->next;
  }
  I->cur = X;
  I->cptr = X->rptr;
  assert (X->rptr <= X->wptr);
  assert (!X->pptr || (X->rptr <= X->pptr && X->pptr <= X->wptr));
  return 0;
}

int nbit_set (nb_iterator_t *I, netbuffer_t *H) {
  I->head = H;
  return nbit_rewind (I);
}

inline int nbit_clearw (nbw_iterator_t *IW) {
  return nbit_clear (IW);
}

int nbit_rewindw (nbw_iterator_t *IW) {
  netbuffer_t *H = IW->head;
  if (!H) {
    return nbit_clearw (IW);
  }
  assert (H->state == NB_MAGIC_HEAD || H->state == NB_MAGIC_BUSYHEAD);

  netbuffer_t *X = H->prev;
  
  IW->cur = X;
  IW->cptr = X->wptr;
  
  assert (X->rptr <= X->wptr);
  assert (!X->pptr || (X->rptr <= X->pptr && X->pptr <= X->wptr));
  return 0;
}

int nbit_setw (nbw_iterator_t *IW, netbuffer_t *H) {
  IW->head = H;
  return nbit_rewindw (IW);
}

int nbit_ready_bytes (nb_iterator_t *I) {
  netbuffer_t *H = I->head, *X = I->cur;
  int limit, v;
  if (!H) {
    return 0;
  }
  assert (X->rptr <= X->wptr);
  assert (X->rptr <= I->cptr && I->cptr <= X->wptr);
  if (I->cptr < X->wptr) {
    return (X->pptr ? X->pptr : X->wptr) - I->cptr;
  }
  assert (H->state == NB_MAGIC_HEAD || H->state == NB_MAGIC_BUSYHEAD);
  limit = H->extra + 1;
  assert (limit <= NET_BUFFERS + 12);

  X = X->next;
  while (X != H) {
    assert (--limit >= 0);
    v = X->wptr - X->rptr;
    assert ((unsigned) v <= NET_BUFFER_SIZE);
    if (v) {
      I->cur = X;
      I->cptr = X->rptr;
      if (X->pptr) {
        v = X->pptr - X->rptr;
      }
      return v;
    }
    X = X->next;
  }
  return 0;
}

void *nbit_get_ptr (nb_iterator_t *I) {
  netbuffer_t *H = I->head, *X = I->cur;
  int limit;
  if (!H) {
    return 0;
  }
  assert (X->rptr <= I->cptr && I->cptr <= X->wptr && (!X->pptr || I->cptr <= X->pptr));
  if (I->cptr < X->wptr) {
    return I->cptr == X->pptr ? 0 : I->cptr;
  }
  assert (H->state == NB_MAGIC_HEAD || H->state == NB_MAGIC_BUSYHEAD);
  limit = H->extra + 1;
  assert (limit <= NET_BUFFERS + 12);

  X = X->next;
  while (X != H) {
    assert (--limit >= 0);
    if (X->rptr < X->wptr) {
      I->cur = X;
      I->cptr = X->rptr;
      return I->cptr == X->pptr ? 0 : I->cptr;
    }
    X = X->next;
  }
  return 0;
}


int nbit_total_ready_bytes (nb_iterator_t *I) {
  netbuffer_t *H = I->head, *X;
  int limit, u, v;
  if (!H) {
    return 0;
  }
  assert (H->state == NB_MAGIC_HEAD || H->state == NB_MAGIC_BUSYHEAD);
  limit = H->extra + 1;
  assert (limit <= NET_BUFFERS + 12);

  X = I->cur;
  char *pptr = X->pptr ? X->pptr : X->wptr;
  assert (X->rptr <= I->cptr && I->cptr <= pptr);
  u = pptr - I->cptr;
  assert (u <= NET_BUFFER_SIZE);
  X = X->next;
  while (X != H && X->pptr != X->rptr) {
    assert (--limit >= 0);
    pptr = X->pptr ? X->pptr : X->wptr;
    v = pptr - X->rptr;
    assert (v >= 0 && v <= NET_BUFFER_SIZE);
    u += v;
    X = X->next;
  }
  assert (u <= H->total_bytes);
  return u;
}

int nbit_advance (nb_iterator_t *I, int offset) {
  netbuffer_t *X = I->cur, *H = I->head;
  int u, v = 0, t = 0;
  char *p = I->cptr;

  if (!offset || !I->cur) {
    return 0;
  }

  assert (X->rptr <= p && p <= X->wptr);
  assert (!X->pptr || p <= X->pptr);

  if (offset > 0) {
    u = X->wptr - p;
    if (X->pptr && X->pptr != X->wptr) {
      u = X->pptr - p;
      t = 1;
    }
    v = 0;

    do {
      if (u > offset) {
        I->cptr = p + offset;
        I->cur = X;
        return v + offset;
      }
      v += u;
      offset -= u;
      if (t) {
        I->cptr = X->pptr;
        I->cur = X;
        return v;
      }
      if (X->next == H) {
        assert (!X->pptr || X->pptr == X->wptr);
        I->cptr = X->wptr;
        I->cur = X;
        return v;
      }
      X = X->next;
      p = X->rptr;
      u = X->wptr - p;
      if (X->pptr && X->pptr != X->wptr) {
        u = X->pptr - p;
        t = 1;
      }
      assert ((unsigned) u <= NET_BUFFER_SIZE);
    } while (offset > 0);

    I->cptr = p;
    I->cur = X;
    return v;

  } else {
    u = p - X->rptr;
    v = 0;

    do {
      if (u + offset >= 0) {
        I->cptr = p + offset;
        I->cur = X;
        return v + offset;
      }
      v -= u;
      offset += u;
      if (X == H) {
        I->cptr = X->rptr;
        I->cur = X;
        return v;
      }
      X = X->prev;
      p = X->wptr;
      assert (!X->pptr || p == X->pptr);
      u = p - X->rptr;
      assert ((unsigned) u <= NET_BUFFER_SIZE);
    } while (offset < 0);

    I->cptr = p;
    I->cur = X;
    return v;
  }
}

int nbit_read_in (nb_iterator_t *I, void *__data, int len) {
  netbuffer_t *H = I->head, *X = I->cur;
  int s, w = 0;
  char *data = __data, *p = I->cptr;

  assert (X->rptr <= p && p <= X->wptr);
  assert (!X->pptr || p <= X->pptr);

  while (len > 0) {
    s = (X->pptr ? X->pptr : X->wptr) - p;
    assert ((unsigned) s <= NET_BUFFER_SIZE);
    if (s > len) { s = len; }
    if (s > 0) {
      memcpy (data, p, s);
      w += s;
      p += s;
      data += s;
      len -= s;
    }
    if (!len || p != X->wptr || X->next == H) { 
      break; 
    }
    X = X->next;
    p = X->rptr;
  }
  if (p == X->wptr && X->next != H) {
    X = X->next;
    p = X->rptr;
  }
  I->cptr = p;
  I->cur = X;
  return w;
}

int nbit_copy_through (netbuffer_t *XD, nb_iterator_t *I, int len) {
  netbuffer_t *H = I->head, *X = I->cur;
  int s, w = 0;
  char *p = I->cptr;

  assert (X->rptr <= p && p <= X->wptr);
  assert (!X->pptr || p <= X->pptr);

  while (len > 0) {
    s = (X->pptr ? X->pptr : X->wptr) - p;
    assert ((unsigned) s <= NET_BUFFER_SIZE);
    if (s > len) { s = len; }
    if (s > 0) {
      write_out (XD, p, s);
      w += s;
      p += s;
      len -= s;
    }
    if (!len || p != X->wptr || X->next == H) { 
      break; 
    }
    X = X->next;
    p = X->rptr;
  }
  if (p == X->wptr && X->next != H) {
    X = X->next;
    p = X->rptr;
  }
  I->cptr = p;
  I->cur = X;
  return w;
}

int nbit_copy_through_nondestruct (netbuffer_t *XD, nb_iterator_t *I, int len) {
  netbuffer_t *H = I->head, *X = I->cur;
  int s, w = 0;
  char *p = I->cptr;

  assert (X->rptr <= p && p <= X->wptr);
  assert (!X->pptr || p <= X->pptr);

  while (len > 0) {
    s = (X->pptr ? X->pptr : X->wptr) - p;
    assert ((unsigned) s <= NET_BUFFER_SIZE);
    if (s > len) { s = len; }
    if (s > 0) {
      write_out (XD, p, s);
      w += s;
      p += s;
      len -= s;
    }
    if (!len || p != X->wptr || X->next == H) { 
      break; 
    }
    X = X->next;
    p = X->rptr;
  }
  return w;
}

int nbit_write_out (nbw_iterator_t *IW, void *__data, int len) {
  netbuffer_t *H = IW->head, *X = IW->cur;
  int s, w = 0;
  char *data = __data, *p = IW->cptr;

  assert (X->rptr <= p && p <= X->wptr);
  assert (p >= X->pptr);

  while (len > 0) {
    s = X->wptr - p;
    assert ((unsigned) s <= NET_BUFFER_SIZE);
    if (s > len) { s = len; }
    if (s > 0) {
      memcpy (p, data, s);
      w += s;
      p += s;
      data += s;
      len -= s;
    }
    if (!len || X->next == H) { 
      break; 
    }
    X = X->next;
    p = X->rptr;
    assert (!X->pptr || X->pptr == p);
  }
  if (p == X->wptr && X->next != H) {
    X = X->next;
    p = X->rptr;
  }
  IW->cptr = p;
  IW->cur = X;
  return w;
}

void *nb_alloc (netbuffer_t *H, int len) {
  netbuffer_t *X, *Y;
  int s, t;
  void *res;
  if (!H || len < 0 || len > NETBUFF_DATA_SIZE) { return 0; }
  assert (H->state == NB_MAGIC_HEAD || H->state == NB_MAGIC_BUSYHEAD);
  assert (!H->pptr);

  X = H->prev;
  t = -((long) X->wptr) & 3;
  s = X->end - X->wptr; 

  if (len + t <= s) {
    res = X->wptr += t;
    X->wptr += len;
    H->total_bytes += len;
    return res;
  }

  Y = alloc_buffer();
  if (!Y) { return 0; }

  X->next = Y;
  Y->prev = X;
  Y->next = H;
  H->prev = Y;
  H->extra++;

  s = Y->end - Y->wptr; 

  if (len <= s) {
    res = Y->wptr;
    Y->wptr += len;
    H->total_bytes += len;
    return res;
  }

  return 0;
}

void *nbr_alloc (nb_allocator_t pH, int len) {
  if (!*pH) {
    *pH = alloc_head_buffer();
  }
  return nb_alloc (*pH, len);
}

int dump_buffer (netbuffer_t *X, int num, int offset) {
  char *ptr;
  int i, s, c;
  assert (X->state == NB_MAGIC_HEAD || X->state == NB_MAGIC_BUSYHEAD || X->state == NB_MAGIC_BUSY);
  fprintf (stderr, "Dumping buffer #%d in chain at offset %d, addr=%p, size=%ld, start=%p, read=%p, pptr=%p, write=%p, end=%p\n",
    num, offset, X, (long)(X->end - X->start), X->start, X->rptr, X->pptr, X->wptr, X->end);
  ptr = X->start;
  while (ptr < X->end) {
    s = X->end - ptr;
    if (s > 16) { 
      s = 16;
    }
    fprintf (stderr, "%08x", (int) (ptr - X->start + offset));
    for (i = 0; i < 16; i++) {
      c = ' ';
      if (ptr + i == X->rptr) {
        c = '[';
      }
      if (ptr + i == X->wptr) {
        c = (c == '[' ? '|' : ']');
      }
      if (i == 8) {
        fputc (' ', stderr);
      }
      if (i < s) {
        fprintf (stderr, "%c%02x", c, (unsigned char) ptr[i]);
      } else {
        fprintf (stderr, "%c  ", c);
      }
    }
    c = ' ';
    if (ptr + 16 == X->end) {
      if (ptr + 16 == X->rptr) {
        c = '[';
      }
      if (ptr + 16 == X->wptr) {
        c = (c == '[' ? '|' : ']');
      }
      
    }
    fprintf (stderr, "%c  ", c);
    for (i = 0; i < s; i++) {
      putc ((unsigned char) ptr[i] < ' ' ? '.' : ptr[i], stderr);
    }
    putc ('\n', stderr);
    ptr += 16;
  }
  return X->end - X->start;
}

void dump_buffers (netbuffer_t *H) {
  netbuffer_t *X;
  int a = 0, b = 0;
  assert (H->state == NB_MAGIC_HEAD || H->state == NB_MAGIC_BUSYHEAD);
  fprintf (stderr, "\n\nDumping buffer chain at %p, %d extra buffers, %d total bytes, %d unprocessed bytes\n", H, H->extra, H->total_bytes, H->unprocessed_bytes);
  b += dump_buffer (H, a, b);
  for (X = H->next; X != H; X = X->next) {
    b += dump_buffer (X, ++a, b);
  }
  fprintf (stderr, "\nEND (dumping buffer chain at %p)\n\n", H);
}




int mark_all_processed (netbuffer_t *H) {
  netbuffer_t *X;
  assert (H->state == NB_MAGIC_HEAD || H->state == NB_MAGIC_BUSYHEAD);
  assert (!H->pptr);
  H->pptr = H->wptr;
  for (X = H->next; X != H; X = X->next) {
    assert (!X->pptr);
    X->pptr = X->wptr;
  }
  H->unprocessed_bytes = 0;
  return 0;
}


int mark_all_unprocessed (netbuffer_t *H) {
  netbuffer_t *X;
  assert (H->state == NB_MAGIC_HEAD || H->state == NB_MAGIC_BUSYHEAD);
  assert (!H->pptr);
  H->pptr = H->rptr;
  for (X = H->next; X != H; X = X->next) {
    assert (!X->pptr);
    X->pptr = X->rptr;
  }
  H->unprocessed_bytes = H->total_bytes;
  H->total_bytes = 0;
  return 0;
}


int release_all_unprocessed (netbuffer_t *H) {
  netbuffer_t *X;
  assert (H->state == NB_MAGIC_HEAD || H->state == NB_MAGIC_BUSYHEAD);
  assert (H->pptr);
  H->pptr = 0;
  for (X = H->next; X != H; X = X->next) {
    assert (X->pptr);
    X->pptr = 0;
  }
  H->total_bytes += H->unprocessed_bytes;
  H->unprocessed_bytes = 0;
  return 0;
}



int nb_start_process (nb_processor_t *P, netbuffer_t *H) {
  P->head = H;
  netbuffer_t *X;
  for (X = H->prev; X != H && X->pptr == X->rptr; X = X->prev) {
    assert (X->pptr);
  };
  while (X->next != H && X->pptr == X->wptr) {
    X = X->next;
  }

  P->cur = X;

  if (X->pptr == X->wptr) {
    assert (!H->unprocessed_bytes);
    P->ptr0 = 0;
    P->ptr1 = 0;
    P->len0 = 0;
    P->len1 = 0;
    return 0;
  }

  P->ptr0 = X->pptr;
  P->len0 = X->wptr - X->pptr;

  X = X->next;
  if (X == H || X->pptr == X->wptr) {
    P->ptr1 = 0;
    P->len1 = 0;
  } else {
    P->ptr1 = X->pptr;
    P->len1 = X->wptr - X->pptr;
  }

  assert (P->len0 + P->len1 <= H->unprocessed_bytes);

  return P->len0 + P->len1;
}


int nb_advance_process (nb_processor_t *P, int offset) {
  netbuffer_t *H = P->head, *X = P->cur;
  int s, w = 0;

  assert (H && offset >= 0);

  while (offset > 0 || X->pptr == X->wptr) {
    s = X->wptr - X->pptr;
    if (s > offset) { s = offset; }
    if (s > 0) {
      w += s;
      X->pptr += s;
      offset -= s;
    }
    if (X->pptr == X->wptr) {
      X = X->next;
      if (X == H) { 
        assert (!offset);
        X = 0;
        break; 
      }
    }
  }

  H->total_bytes += w;
  H->unprocessed_bytes -= w;

  if (X == P->cur) {
    P->ptr0 += w;
    P->len0 -= w;
    assert (P->len0 > 0 && P->ptr0 == X->pptr);
    return P->len0 + P->len1;
  }

  if (!X) {
    assert (!H->unprocessed_bytes);
    P->cur = H->prev;
    P->ptr0 = 0;
    P->ptr1 = 0;
    P->len0 = 0;
    P->len1 = 0;
    return 0;
  }
  
  P->cur = X;

  P->ptr0 = X->pptr;
  P->len0 = X->wptr - X->pptr;

  assert (P->len0 > 0);

  X = X->next;
  if (X == H || X->pptr == X->wptr) {
    P->ptr1 = 0;
    P->len1 = 0;
  } else {
    P->ptr1 = X->pptr;
    P->len1 = X->wptr - X->pptr;
  }

  assert (P->len0 + P->len1 <= H->unprocessed_bytes);

  return P->len0 + P->len1;
}
