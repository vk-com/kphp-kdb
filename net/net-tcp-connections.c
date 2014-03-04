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
              2008-2013 Nikolai Durov
              2008-2013 Andrei Lopatin
                   2013 Vitaliy Valtman
*/

#include <sys/uio.h>
#include <stdlib.h>
#include <assert.h>
#include <stdio.h>

#include "net-connections.h"
#include "net-msg.h"
#include "net-msg-buffers.h"
#include "crypto/aesni256.h"
#include "net-crypto-aes.h"


#define MAX_TCP_RECV_BUFFERS 128
#define TCP_RECV_BUFFER_SIZE 1024

int tcp_free_connection_buffers (struct connection *c) {
  rwm_free (&c->in);
  rwm_free (&c->in_u);
  rwm_free (&c->out);
  rwm_free (&c->out_p);
  return 0;
}

int tcp_prepare_iovec (struct iovec *iov, int *iovcnt, int maxcnt, struct raw_message *raw) {
  int t = rwm_prepare_iovec (raw, iov, maxcnt, raw->total_bytes);
  if (t < 0) {
    *iovcnt = maxcnt;
    int i;
    t = 0;
    for (i = 0; i < maxcnt; i++) {
      t += iov[i].iov_len;
    }
    assert (t < raw->total_bytes);
    return t;
  } else {
    *iovcnt = t;
    return raw->total_bytes;
  }
}

int tcp_recv_buffers_num;
int tcp_recv_buffers_total_size;
struct iovec tcp_recv_iovec[MAX_TCP_RECV_BUFFERS + 1];
struct msg_buffer *tcp_recv_buffers[MAX_TCP_RECV_BUFFERS];
int total_used_buffers;
int prealloc_tcp_buffers (void) {
  assert (!tcp_recv_buffers_num);   

  int i;
  for (i = MAX_TCP_RECV_BUFFERS - 1; i >= 0; i--) {
    struct msg_buffer *X = alloc_msg_buffer ((tcp_recv_buffers_num) ? tcp_recv_buffers[i + 1] : 0, TCP_RECV_BUFFER_SIZE);
    if (!X) {
      vkprintf (0, "**FATAL**: cannot allocate tcp receive buffer\n");
      exit (2);
    }
    vkprintf (3, "allocated %d byte tcp receive buffer #%d at %p\n", X->chunk->buffer_size, i, X);
    tcp_recv_buffers[i] = X;
    tcp_recv_iovec[i + 1].iov_base = X->data;
    tcp_recv_iovec[i + 1].iov_len = X->chunk->buffer_size;
    ++ tcp_recv_buffers_num;
    tcp_recv_buffers_total_size += X->chunk->buffer_size;
  }
  return tcp_recv_buffers_num;
}

/* returns # of bytes in c->Out remaining after all write operations;
   anything is written if (1) C_WANTWR is set 
                      AND (2) c->Out.total_bytes > 0 after encryption 
                      AND (3) C_NOWR is not set
   if c->Out.total_bytes becomes 0, C_WANTWR is cleared ("nothing to write") and C_WANTRD is set
   if c->Out.total_bytes remains >0, C_WANTRD is cleared ("stop reading until all bytes are sent")
*/ 
int tcp_server_writer (struct connection *c) {
  int r, s, t = 0, check_watermark;

  assert (c->status != conn_connecting);

  if (c->crypto) {
    assert (c->type->crypto_encrypt_output (c) >= 0);
  }

  struct raw_message *out = c->crypto ? &c->out_p : &c->out;
  do {
    check_watermark = (out->total_bytes >= c->write_low_watermark);
    while ((c->flags & C_WANTWR) != 0) {
      // write buffer loop
      s = out->total_bytes;
      if (!s) {
        c->flags &= ~C_WANTWR;
        break;
      }

      if (c->flags & C_NOWR) {
        break;
      }

      static struct iovec iov[64];
      int iovcnt = -1;

      s = tcp_prepare_iovec (iov, &iovcnt, 64, out);
      assert (iovcnt > 0 && s > 0);

      r = writev (c->fd, iov, iovcnt);

      if (verbosity > 0) {
        fprintf (stderr, "send/writev() to %d: %d written out of %d in %d chunks\n", c->fd, r, s, iovcnt);
        if (r < 0) {
          perror ("send()");
        }
      }

      if (r > 0) {
        rwm_fetch_data (out, 0, r);
        t += r;
      }

      if (r < s) {
        c->flags |= C_NOWR;
      }

    }

    if (t) {
      if (check_watermark && out->total_bytes < c->write_low_watermark && c->type->ready_to_write) {
        c->type->ready_to_write (c);
        t = 0;
        if (c->crypto) {
          assert (c->type->crypto_encrypt_output (c) >= 0);
        }
        if (out->total_bytes > 0) {
          c->flags |= C_WANTWR;
        }
      }
    }
  } while ((c->flags & (C_WANTWR | C_NOWR)) == C_WANTWR);

  if (out->total_bytes) {
    c->flags &= ~C_WANTRD;
  } else if (c->status != conn_write_close && !(c->flags & C_FAILED)) {
    c->flags |= C_WANTRD;
  }

  return out->total_bytes;
}


/* reads and parses as much as possible, and returns:
   0 : all ok
   <0 : have to skip |res| bytes before invoking parse_execute
   >0 : have to read that much bytes before invoking parse_execute
   -1 : if c->error has been set
   NEED_MORE_BYTES=0x7fffffff : need at least one byte more 
*/
int tcp_server_reader (struct connection *c) {
  int res = 0, r, r1, s;

  struct raw_message *in = c->crypto ? &c->in_u : &c->in;
  struct raw_message *cin = &c->in;
  
  while (1) {
    /* check whether it makes sense to try to read from this socket */
    int try_read = (c->flags & C_WANTRD) && !(c->flags & (C_NORD | C_FAILED | C_STOPREAD)) && !c->error;
    /* check whether it makes sense to invoke parse_execute() even if no new bytes are read */
    int try_reparse = (c->flags & C_REPARSE) && (c->status == conn_expect_query || c->status == conn_reading_query || c->status == conn_wait_answer || c->status == conn_reading_answer) && !c->skip_bytes;
    if (!try_read && !try_reparse) {
      break;
    }

    if (try_read) {
      /* Reader */
      if (c->status == conn_write_close) {
        rwm_clear (&c->in);
        rwm_clear (&c->in_u);
        c->flags &= ~C_WANTRD;
        break;
      }

      if (!tcp_recv_buffers_num) {
        prealloc_tcp_buffers ();
      }
     
      if (in->last && in->last->next) {
        fork_message_chain (in);
      }
      int p;      
      if (c->basic_type != ct_pipe) {
        s = tcp_recv_buffers_total_size;
        if (in->last && in->last_offset != in->last->part->chunk->buffer_size) {
          tcp_recv_iovec[0].iov_len = in->last->part->chunk->buffer_size - in->last_offset;
          tcp_recv_iovec[0].iov_base = in->last->part->data +  in->last_offset;
          p = 0;
        } else {
          p = 1;
        }
        r = readv (c->fd, tcp_recv_iovec + p, MAX_TCP_RECV_BUFFERS + 1 - p);
      } else {
        p = 1;
        s = tcp_recv_iovec[1].iov_len;
        r = read (c->fd, tcp_recv_iovec[1].iov_base, tcp_recv_iovec[1].iov_len);
      }

      if (r < s) { 
        c->flags |= C_NORD; 
      }

      if (verbosity > 0) {
        fprintf (stderr, "recv() from %d: %d read out of %d. Crypto = %d\n", c->fd, r, s, c->crypto != 0);
        if (r < 0 && errno != EAGAIN) {
          perror ("recv()");
        }
      }

      if (r > 0) {
        struct msg_part *mp = 0;
        if (!in->last) {
          assert (p == 1);
          mp = new_msg_part (0, tcp_recv_buffers[p - 1]);
          assert (tcp_recv_buffers[p - 1]->data == tcp_recv_iovec[p].iov_base);
          mp->offset = 0;
          mp->len = r > tcp_recv_iovec[p].iov_len ? tcp_recv_iovec[p].iov_len : r;
          r -= mp->len;
          in->first = in->last = mp;
          in->total_bytes = mp->len;
          in->first_offset = 0;
          in->last_offset = mp->len;
          p ++;
        } else {
          assert (in->last->offset + in->last->len == in->last_offset);
          if (p == 0) {
            int t = r > tcp_recv_iovec[0].iov_len ? tcp_recv_iovec[0].iov_len : r;
            in->last->len += t;
            in->total_bytes += t;
            in->last_offset += t;
            r -= t;
            p ++;
          }
        }
        
        assert (in->last && !in->last->next);

        while (r > 0) {
          mp = new_msg_part (0, tcp_recv_buffers[p - 1]);
          mp->offset = 0;
          mp->len = r > tcp_recv_iovec[p].iov_len ? tcp_recv_iovec[p].iov_len : r;
          r -= mp->len;
          in->last->next = mp;
          in->last = mp;
          in->last_offset = mp->len + mp->offset;
          in->total_bytes += mp->len;
          p ++;
        }
        assert (!r);

        int i;
        for (i = 0; i < p - 1; i++) {
          struct msg_buffer *X = alloc_msg_buffer (tcp_recv_buffers[i], TCP_RECV_BUFFER_SIZE);
          if (!X) {
            vkprintf (0, "**FATAL**: cannot allocate udp receive buffer\n");
            exit (2);
          }
          tcp_recv_buffers[i] = X;
          tcp_recv_iovec[i + 1].iov_base = X->data;
          tcp_recv_iovec[i + 1].iov_len = X->chunk->buffer_size;
        }

        s = c->skip_bytes;

        if (s && c->crypto) {
          assert (c->type->crypto_decrypt_input (c) >= 0);
        }

        r1 = c->in.total_bytes;

        if (s < 0) {
          // have to skip s more bytes
          if (r1 > -s) {
            r1 = -s;
          }

          rwm_fetch_data (cin, 0, r1);
          c->skip_bytes = s += r1;

          if (verbosity > 2) {
            fprintf (stderr, "skipped %d bytes, %d more to skip\n", r1, -s);
          }
          if (s) {
            continue;
          }
        }

        if (s > 0) {
          // need to read s more bytes before invoking parse_execute()
          if (r1 >= s) {
            c->skip_bytes = s = 0;
          }
          
          vkprintf (1, "fetched %d bytes, %d available bytes, %d more to load\n", r, r1, s ? s - r1 : 0);
          if (s) {
            continue;
          }
        }
      }
    } else {
      r = 0x7fffffff;
    }

    if (c->crypto) {
      assert (c->type->crypto_decrypt_input (c) >= 0);
    }

    while (!c->skip_bytes && (c->status == conn_expect_query || c->status == conn_reading_query ||
                              c->status == conn_wait_answer || c->status == conn_reading_answer)) {
      /* Parser */
      int conn_expect = (c->status - 1) | 1; // one of conn_expect_query and conn_wait_answer; using VALUES of these constants!
      c->flags &= ~C_REPARSE;
      if (!cin->total_bytes) {
        /* encrypt output; why here? */
        if (c->crypto) {
          assert (c->type->crypto_encrypt_output (c) >= 0);
        }
        return 0;
      }
      if (c->status == conn_expect) {
        c->parse_state = 0;
        c->status++;  // either conn_reading_query or conn_reading_answer
      }
      res = c->type->parse_execute (c);
      // 0 - ok/done, >0 - need that much bytes, <0 - skip bytes, or NEED_MORE_BYTES
      if (!res) {
        if (c->status == conn_expect + 1) {  // either conn_reading_query or conn_reading_answer
          c->status--;
        }
        if (c->error) {
          return -1;
        }
      } else if (res != NEED_MORE_BYTES) {
        // have to load or skip abs(res) bytes before invoking parse_execute
        if (res < 0) {
          assert (!cin->total_bytes);
          res -= cin->total_bytes;
        } else {
          res += cin->total_bytes;
        }
        c->skip_bytes = res;
        break;
      }
    }

    if (r <= 0) {
      break;
    }
  }

  if (c->crypto) {
    /* encrypt output once again; so that we don't have to check c->Out.unprocessed_bytes afterwards */
    assert (c->type->crypto_encrypt_output (c) >= 0);
  }

  return res;
}

/* 0 = all ok, >0 = so much more bytes needed to encrypt last block */
int tcp_aes_crypto_encrypt_output (struct connection *c) {
  struct aes_crypto *T = c->crypto;
  assert (c->crypto);
  struct raw_message *out = &c->out;

  int l = out->total_bytes;
  l &= ~15;
  if (l) {
/*    assert (rwm_encrypt_decrypt_cbc (out, l, &T->write_aeskey, T->write_iv) == l);    
    if (out->total_bytes & 15) {
      struct raw_message x;
      rwm_split_head (&x, out, l);
      rwm_union (&c->out_p, &x);
    } else {
      rwm_union (&c->out_p, &c->out);
      rwm_init (&c->out, 0);
    }*/
    assert (rwm_encrypt_decrypt_to (&c->out, &c->out_p, l, &T->write_aeskey, (void *)T->write_aeskey.cbc_crypt, T->write_iv) == l);
  }

  return (-out->total_bytes) & 15;
}

/* 0 = all ok, >0 = so much more bytes needed to decrypt last block */
int tcp_aes_crypto_decrypt_input (struct connection *c) {
  struct aes_crypto *T = c->crypto;
  assert (c->crypto);
  struct raw_message *in = &c->in_u;

  int l = in->total_bytes;
  l &= ~15;
  if (l) {
    #if 1
      assert (rwm_encrypt_decrypt_to (&c->in_u, &c->in, l, &T->read_aeskey, (void *)T->read_aeskey.cbc_crypt, T->read_iv) == l);
    #else
    assert (rwm_encrypt_decrypt_cbc (in, l, &T->read_aeskey, T->read_iv) == l);
    if (in->total_bytes & 15) {
      struct raw_message x;
      rwm_split_head (&x, in, l);
      rwm_union (&c->in, &x);
    } else {
      rwm_union (&c->in, &c->in_u);
      rwm_init (&c->in_u, 0);
    }
    #endif
  }

  return (-in->total_bytes) & 15;
}

/* returns # of bytes needed to complete last output block */
int tcp_aes_crypto_needed_output_bytes (struct connection *c) {
  assert (c->crypto);
  return -c->out.total_bytes & 15;
}
