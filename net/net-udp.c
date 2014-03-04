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

    Copyright 2013 Vkontakte Ltd
              2013 Nikolai Durov
              2013 Andrei Lopatin
*/

#define	_FILE_OFFSET_BITS	64

#include <assert.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <stddef.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <sys/uio.h>
#include <aio.h>
#include <netdb.h>
#include <linux/errqueue.h>

#include "crc32.h"
#include "server-functions.h"
#include "net-events.h"
#include "net-msg-buffers.h"
#include "net-udp.h"

#define	UDP_CDATA_MAX_SIZE	256

/*
 *
 *	sendmmsg / recvmmsg SUPPORT / EMULATION
 *
 */

#ifndef __NR_sendmmsg
# ifdef _LP64
#  define	__NR_sendmmsg	307
# else
#  define	__NR_sendmmsg	345
# endif
#endif

#ifndef __NR_recvmmsg
# ifdef _LP64
# define	__NR_recvmmsg	299
# else
# define	__NR_recvmmsg	337
# endif
#endif

int our_sendmmsg (int sockfd, struct our_mmsghdr *msgvec, unsigned int vlen,
	     unsigned int flags) {
  if (sendmmsg_supported > 0) {
    return syscall (__NR_sendmmsg, sockfd, msgvec, vlen, flags);
  } else if (sendmmsg_supported < 0) {
    errno = ENOSYS;
    return -1;
  }
  int res = syscall (__NR_sendmmsg, sockfd, msgvec, vlen, flags);
  if (res < 0 && errno == ENOSYS) {
    if (verbosity > 0) {
      fprintf (stderr, "warning: sendmmsg() not supported, falling back to sendmsg()\n");
      errno = ENOSYS;
    }
    sendmmsg_supported = -1;
  } else {
    sendmmsg_supported = 1;
  }
  return res;
}

int our_recvmmsg (int sockfd, struct our_mmsghdr *msgvec, unsigned int vlen,
	      unsigned int flags, struct timespec *timeout) {
  if (recvmmsg_supported > 0) {
    return syscall (__NR_recvmmsg, sockfd, msgvec, vlen, flags, timeout);
  } else if (recvmmsg_supported < 0) {
    errno = ENOSYS;
    return -1;
  }
  int res = syscall (__NR_recvmmsg, sockfd, msgvec, vlen, flags, timeout);
  if (res < 0 && errno == ENOSYS) {
    if (verbosity > 0) {
      fprintf (stderr, "warning: recvmmsg() not supported, falling back to recvmsg()\n");
      errno = ENOSYS;
    }
    recvmmsg_supported = -1;
  } else {
    recvmmsg_supported = 1;
  }
  return res;
}

/*
 *
 *	GENERIC UDP SERVER FUNCTIONS
 *
 */

struct msg_buffer *udp_recv_buffers[MAX_UDP_RECV_BUFFERS+1];
int udp_recv_buffers_num, udp_recv_msgvec_size;

struct our_mmsghdr udp_recv_msgvec[MAX_UDP_RECV_BUFFERS];
struct iovec udp_recv_iovec[MAX_UDP_RECV_BUFFERS];
union sockaddr_in46 udp_recv_addr[MAX_UDP_RECV_BUFFERS];
char udp_recv_cdata[MAX_UDP_RECV_BUFFERS][UDP_CDATA_MAX_SIZE];

unsigned our_ip[MAX_OUR_IPS]; // only even indices used, 0 unused, 2 = localhost
unsigned char our_ipv6[MAX_OUR_IPS][16]; // only odd indices used, 1 = localhost
int used_our_ip, used_our_ipv6;

/* should be in some system header file */
struct in6_pktinfo {
	struct in6_addr	ipi6_addr;
	int		ipi6_ifindex;
};

unsigned char outbound_src_ip_cdata[MAX_OUR_IPS][CMSG_SPACE(sizeof (struct in6_pktinfo))+16];
struct msghdr outbound_src_ip_hdr[MAX_OUR_IPS];

static void fill_outbound_control (int x) {
  struct msghdr *hdr = outbound_src_ip_hdr + x;
  hdr->msg_control = outbound_src_ip_cdata[x];
  hdr->msg_controllen = sizeof (outbound_src_ip_cdata[x]);
  struct cmsghdr *cmsg = CMSG_FIRSTHDR (hdr);
  if (!(x & 1)) {
    hdr->msg_controllen = cmsg->cmsg_len = CMSG_LEN (sizeof (struct in_pktinfo));
    cmsg->cmsg_level = IPPROTO_IP;
    cmsg->cmsg_type = IP_PKTINFO;
    struct in_pktinfo *pi = (void *) CMSG_DATA (cmsg);
    pi->ipi_spec_dst.s_addr = htonl (our_ip[x]);
  } else {
    hdr->msg_controllen = cmsg->cmsg_len = CMSG_LEN (sizeof (struct in6_pktinfo));
    cmsg->cmsg_level = IPPROTO_IPV6;
    cmsg->cmsg_type = IPV6_PKTINFO;
    struct in6_pktinfo *pi = (void *) CMSG_DATA (cmsg);
    memcpy (pi->ipi6_addr.s6_addr, our_ipv6[x], 16);
  }
}

int lookup_our_ip (unsigned ip) {
  int i;
  for (i = 2; i <= used_our_ip; i += 2) {
    if (our_ip[i] == ip) {
      return i;
    }
  }
  if (!used_our_ip) {
    used_our_ip = 2;
    our_ip[2] = 0x7f000001;
    set_4in6 (our_ipv6[2], our_ip[2]);
    fill_outbound_control (2);
    if (our_ip[2] == ip) {
      return 2;
    }
  } else if (used_our_ip >= MAX_OUR_IPS - 2) {
    return 0;
  }
  used_our_ip += 2;
  our_ip[used_our_ip] = ip;
  set_4in6 (our_ipv6[used_our_ip], ip);
  fill_outbound_control (used_our_ip);
  return used_our_ip;
}

int lookup_our_ipv6 (const unsigned char ipv6[16]) {
  int i;
  for (i = 1; i <= used_our_ipv6; i += 2) {
    if (!memcmp (our_ipv6[i], ipv6, 16)) {
      return i;
    }
  }
  if (!used_our_ipv6) {
    used_our_ipv6 = 1;
    our_ipv6[1][15] = 1;
    fill_outbound_control (1);
    if (!memcmp (our_ipv6[1], ipv6, 16)) {
      return 1;
    }
  } else if (used_our_ipv6 >= MAX_OUR_IPS - 2) {
    return 0;
  }
  used_our_ipv6 += 2;
  memcpy (our_ipv6[used_our_ipv6], ipv6, 16);
  fill_outbound_control (used_our_ipv6);
  return used_our_ipv6;
}



int free_udp_message (struct udp_message *msg) {
  int res = rwm_free (&msg->raw);
  msg->next = (void *) -1;
  free (msg);
  return res;
}

int prealloc_udp_buffers (void) {
  assert (!udp_recv_buffers_num);   

  int i;
  for (i = MAX_UDP_RECV_BUFFERS - 1; i >= 0; i--) {
    struct msg_buffer *X = alloc_msg_buffer ((udp_recv_buffers_num && (lrand48() & 7)) ? udp_recv_buffers[i+1] : 0, UDP_RECV_BUFFER_SIZE);
    if (!X) {
      vkprintf (0, "**FATAL**: cannot allocate udp receive buffer\n");
      exit (2);
    }
    vkprintf (3, "allocated %d byte udp receive buffer #%d at %p\n", X->chunk->buffer_size, i, X);
    udp_recv_buffers[i] = X;
    udp_recv_iovec[i].iov_base = X->data;
    udp_recv_iovec[i].iov_len = X->chunk->buffer_size;
    ++ udp_recv_buffers_num;
  }

  int dg_cnt = 0, acc_size = 0, st = 0;
  struct our_mmsghdr *hdr = udp_recv_msgvec;

  for (i = 0; i < udp_recv_buffers_num; i++) {
    acc_size += udp_recv_iovec[i].iov_len;
    if (acc_size >= MAX_UDP_RECV_DATAGRAM) {
      hdr->msg_hdr.msg_iov = udp_recv_iovec + st;
      hdr->msg_hdr.msg_iovlen = i + 1 - st;
      hdr->msg_hdr.msg_flags = 0;
      hdr->msg_hdr.msg_control = udp_recv_cdata[dg_cnt];
      hdr->msg_hdr.msg_controllen = UDP_CDATA_MAX_SIZE;
      hdr->msg_hdr.msg_name = udp_recv_addr + (dg_cnt++);
      hdr->msg_hdr.msg_namelen = sizeof (udp_recv_addr[0]);
      st = i + 1;
      hdr++;
      acc_size = 0;
    }
  }

  assert (dg_cnt > 0);
  udp_recv_msgvec_size = dg_cnt;

  return dg_cnt;
}

/* returns # of packets read or -1 */
int udp_try_read (struct udp_socket *u, int get_errors) {
  static char buf2[256];
  vkprintf (2, "in udp_try_read(%d,%d)\n", u->fd, get_errors);

  if (!udp_recv_buffers_num) {
    prealloc_udp_buffers ();
  }

  int res, flags = MSG_DONTWAIT;
  if (get_errors) {
    flags |= MSG_ERRQUEUE;
  }

  if (recvmmsg_supported < 0) {
    res = recvmsg (u->fd, &udp_recv_msgvec[0].msg_hdr, flags);
    if (res < 0) {
      // vkprintf (2, "recvmmsg(): %m\n");
      return res;
    }
    udp_recv_msgvec[0].msg_len = res;
    res = 1;
  } else {
    res = our_recvmmsg (u->fd, udp_recv_msgvec, udp_recv_msgvec_size, flags, 0);
    if (res < 0) {
      // vkprintf (2, "our_recvmmsg(): %m\n");
      return res;
    }
  }

  vkprintf (2, "recvmmsg: received %d datagrams\n", res);

  if (!res) {
    return 0;
  }

  int i, j, k;
  struct our_mmsghdr *hdr = udp_recv_msgvec;

  for (i = 0, hdr = udp_recv_msgvec; i < res; i++, hdr++) {
    int msg_len = hdr->msg_len;
    vkprintf (2, "datagram #%d: %d data bytes, %d control (from %s:%d)\n", i, msg_len, (int) hdr->msg_hdr.msg_controllen, udp_recv_addr[i].a4.sin_family == AF_INET ?
	      conv_addr (udp_recv_addr[i].a4.sin_addr.s_addr, buf2) : conv_addr6 ((void *) &udp_recv_addr[i].a6.sin6_addr, buf2), 
	      ntohs (udp_recv_addr[i].a4.sin_port));
    assert (msg_len <= MAX_UDP_RECV_DATAGRAM);
    assert (!(hdr->msg_hdr.msg_flags & (MSG_EOR)));
    assert (!(hdr->msg_hdr.msg_flags & (MSG_TRUNC)));
    assert (!(hdr->msg_hdr.msg_flags & (MSG_CTRUNC)));
    assert (!(hdr->msg_hdr.msg_flags & (MSG_EOR | MSG_TRUNC | MSG_CTRUNC | MSG_OOB)));

    struct udp_message *msg = malloc (sizeof (struct udp_message));
    assert (msg);
    msg->raw.total_bytes = msg_len;
    msg->raw.first_offset = 0;
    msg->raw.last_offset = 0;
    msg->raw.first = msg->raw.last = 0;
    msg->raw.magic = RM_INIT_MAGIC;
    rwm_total_msgs ++;
    k = j = hdr->msg_hdr.msg_iov - udp_recv_iovec;
    while (msg_len > 0) {
      struct msg_part *mp = new_msg_part (0, udp_recv_buffers[j]);
      int part_len = udp_recv_iovec[j++].iov_len;
      if (part_len > msg_len) {
	part_len = msg_len;
      }
      mp->len = part_len;
      msg_len -= part_len;
      if (!msg->raw.first) {
	msg->raw.first = mp;
      } else {
	msg->raw.last->next = mp;
      }
      msg->raw.last = mp;
      msg->raw.last_offset = part_len;
    }
    assert (!msg_len);

    if (!(u->flags & U_IPV6)) {
      assert (udp_recv_addr[i].a4.sin_family == AF_INET);
      set_4in6 (msg->ipv6, udp_recv_addr[i].a4.sin_addr.s_addr);
      msg->port = ntohs (udp_recv_addr[i].a4.sin_port);
    } else {
      assert (udp_recv_addr[i].a6.sin6_family == AF_INET6);
      memcpy (msg->ipv6, &udp_recv_addr[i].a6.sin6_addr, 16);
      msg->port = ntohs (udp_recv_addr[i].a6.sin6_port);
    }
    msg->next = 0;
    msg->our_ip_idx = 0;
    msg->flags = 0;

    struct cmsghdr *cmsg;
    struct sock_extended_err *ee = 0;
    for (cmsg = CMSG_FIRSTHDR (&hdr->msg_hdr); cmsg; cmsg = CMSG_NXTHDR(&hdr->msg_hdr, cmsg)) {
      if (cmsg->cmsg_type == IP_PKTINFO && cmsg->cmsg_level == IPPROTO_IP) {
	struct in_pktinfo *pi = (void *) CMSG_DATA (cmsg);
	msg->our_ip_idx = lookup_our_ip (ntohl (pi->ipi_spec_dst.s_addr));
	vkprintf (2, "(destination address %s, index %d)\n", conv_addr (pi->ipi_spec_dst.s_addr, buf2), msg->our_ip_idx);
	// pi->ipi_spec_dst is the destination struct in_addr
	// pi->ipi_addr is the receiving interface struct in_addr
      } else if (cmsg->cmsg_type == IPV6_PKTINFO && cmsg->cmsg_level == IPPROTO_IPV6) {
	struct in6_pktinfo *pi = (void *) CMSG_DATA (cmsg);
	msg->our_ip_idx = lookup_our_ipv6 (pi->ipi6_addr.s6_addr);
	vkprintf (2, "(destination address %s, index %d)\n", conv_addr6 (pi->ipi6_addr.s6_addr, buf2), msg->our_ip_idx);
      } else if ((cmsg->cmsg_type == IP_RECVERR && cmsg->cmsg_level == IPPROTO_IP) ||
		 (cmsg->cmsg_type == IPV6_RECVERR && cmsg->cmsg_level == IPPROTO_IPV6)) {
	ee = (void *) CMSG_DATA (cmsg);
      }
    }

    if (ee) {
      struct sockaddr_in *addr = (struct sockaddr_in *) SO_EE_OFFENDER (ee);
      msg->flags |= UMF_ERROR;
      assert ((hdr->msg_hdr.msg_flags & MSG_ERRQUEUE) && get_errors);
      int *ptr = rwm_prepend_alloc (&msg->raw, 36);
      assert (ptr);
      *ptr = 32;
      memcpy (ptr + 1, ee, 16);
      switch (addr->sin_family) {
      case AF_INET:
	set_4in6 ((unsigned char *) (ptr + 5), addr->sin_addr.s_addr);
	break;
      case AF_INET6:
	memcpy (ptr + 5, &((struct sockaddr_in6 *) addr)->sin6_addr, 16);
	break;
      default:
	memset (ptr + 5, 0, 16);
	break;
      }
      vkprintf (2, "(received error message: errno=%d origin=%d type=%d code=%d origin=%s source=[%s]:%d)\n", ee->ee_errno, ee->ee_origin, ee->ee_type, ee->ee_code, show_ipv6 ((unsigned char *)(ptr + 5)), show_ipv6 (msg->ipv6), msg->port);
    } else {
      assert (!(hdr->msg_hdr.msg_flags & MSG_ERRQUEUE) && !get_errors);
    }
    u->recv_queue_bytes += msg->raw.total_bytes;
    u->recv_queue_len++;

    if (!u->recv_queue) {
      u->recv_queue = u->recv_queue_last = msg;
    } else {
      u->recv_queue_last->next = msg;
      u->recv_queue_last = msg;
    }

    hdr->msg_hdr.msg_controllen = UDP_CDATA_MAX_SIZE;

    while (j > k) {
      struct msg_buffer *X = alloc_msg_buffer (udp_recv_buffers[j], UDP_RECV_BUFFER_SIZE);
      if (!X) {
	vkprintf (0, "**FATAL**: cannot allocate udp receive buffer\n");
	exit (2);
      }
      --j;
      vkprintf (3, "allocated %d byte udp receive buffer #%d at %p\n", X->chunk->buffer_size, j, X);
      udp_recv_buffers[j] = X;
      udp_recv_iovec[j].iov_base = X->data;
      udp_recv_iovec[j].iov_len = X->chunk->buffer_size;
    }
  }

  return res;
}

int udp_reader (struct udp_socket *u) {
  int tot_rcv = 0, tot_rcv_err = 0, have_errors = (u->flags & U_ERRQ);
  vkprintf (2, "in udp_reader(%d,%d)\n", u->fd, have_errors);
  while (!(u->flags & U_NORD) || have_errors) {
    int res = udp_try_read (u, have_errors);
    if (res < 0) {
      if (errno == ENOSYS || errno == EINTR) {
	continue;
      }
      if (errno == EAGAIN) {
	if (have_errors) {
	  have_errors = 0;
	  continue;
	}
	u->flags |= U_NORD;
	break;
      }
      have_errors = U_ERRQ;
      continue;
      // kprintf ("**FATAL**: while reading from udp socket %d: %m\n", u->fd);
      // exit (2);
    } else if (!res) {
      if (have_errors) {
	have_errors = 0;
	continue;
      }
      u->flags |= U_NORD;
      break;
    } else {
      tot_rcv += res;
      if (have_errors) {
	tot_rcv_err += res;
      }
    }
  }
  u->flags = (u->flags & ~U_ERRQ) | have_errors;
  vkprintf (2, "udp_reader(%d): %d packets received, %d error packets\n", u->fd, tot_rcv, tot_rcv_err);
  return tot_rcv;
}

struct our_mmsghdr udp_send_msgvec[MAX_UDP_SEND_BUFFERS];
struct iovec udp_send_iovec[MAX_UDP_SEND_BUFFERS];
struct sockaddr_in udp_send_addr[MAX_UDP_SEND_BUFFERS];
struct sockaddr_in6 udp_send_addr6[MAX_UDP_SEND_BUFFERS];

int prepare_write_chain (struct udp_message *m, int max_messages) {
  int iovec_ptr = 0, msgvec_ptr = 0;
  struct our_mmsghdr *mm = udp_send_msgvec;

  while (m && msgvec_ptr < max_messages) {
    int res = rwm_prepare_iovec (&m->raw, udp_send_iovec + iovec_ptr, MAX_UDP_SEND_BUFFERS - iovec_ptr, MAX_UDP_SEND_DATAGRAM);
    if (res < 0) {
      break;
    }
    if (is_4in6 (m->ipv6)) {
      struct sockaddr_in *dest = mm->msg_hdr.msg_name = udp_send_addr + msgvec_ptr;
      dest->sin_family = AF_INET;
      dest->sin_port = htons (m->port);
      dest->sin_addr.s_addr = extract_4in6 (m->ipv6);
      mm->msg_hdr.msg_namelen = sizeof (struct sockaddr_in);
    } else {
      struct sockaddr_in6 *dest = mm->msg_hdr.msg_name = udp_send_addr6 + msgvec_ptr;
      dest->sin6_family = AF_INET6;
      dest->sin6_port = htons (m->port);
      memcpy (&dest->sin6_addr, m->ipv6, 16);
      mm->msg_hdr.msg_namelen = sizeof (struct sockaddr_in6);
    }
    mm->msg_hdr.msg_iov = udp_send_iovec + iovec_ptr;
    mm->msg_hdr.msg_iovlen = res;
    mm->msg_hdr.msg_flags = MSG_DONTWAIT;
    mm->msg_len = m->raw.total_bytes;

    int t = m->our_ip_idx;
    if (t > 0 && t < MAX_OUR_IPS && outbound_src_ip_hdr[t].msg_controllen) {
      mm->msg_hdr.msg_control = outbound_src_ip_hdr[t].msg_control;
      mm->msg_hdr.msg_controllen = outbound_src_ip_hdr[t].msg_controllen;
    } else {
      mm->msg_hdr.msg_control = 0;
      mm->msg_hdr.msg_controllen = 0;
    }

    iovec_ptr += res;
    ++msgvec_ptr;
    ++mm;
    m = m->next;
  }
  return msgvec_ptr;
}

int udp_writer (struct udp_socket *u) {
  int res, count, total = 0, force_sendmsg = 0, drop_bad = 0;
  vkprintf (2, "udp_writer: %d bytes in %d messages in queue\n", u->send_queue_bytes, u->send_queue_len);

  while (u->send_queue && !(u->flags & U_ERRQ)) {
    if (sendmmsg_supported < 0 || force_sendmsg) {
      count = prepare_write_chain (u->send_queue, 1);
      assert (count == 1);

      res = sendmsg (u->fd, &udp_send_msgvec[0].msg_hdr, MSG_DONTWAIT);
      if (res < 0) {
	if (errno == EINTR || errno == EAGAIN) {
	  continue;
	}
	int keep_errno = errno;
	if (errno == EPERM) {
	  vkprintf (2, "error in sendmsg() to [%s]:%d: %s, dropping message\n", show_ipv6 (u->send_queue->ipv6), u->send_queue->port, strerror (keep_errno));
	  u->type->process_send_error (u, u->send_queue);
	  drop_bad = 1;
	} else if (errno == ECONNREFUSED || errno == EHOSTUNREACH || errno == EHOSTDOWN || errno == ENETUNREACH || errno == ENONET || errno == EMSGSIZE || errno == EOPNOTSUPP || errno == ENOPROTOOPT || errno == ENOBUFS) {
	  u->flags |= U_ERRQ;
	  break;
	} else {
	  fprintf (stderr, "error in sendmsg() to [%s]:%d: %s\n", show_ipv6 (u->send_queue->ipv6), u->send_queue->port, strerror (keep_errno));
	  u->flags |= U_NOWR | U_ERROR;
	  return res;
	}
      } else {
	udp_send_msgvec[0].msg_len = res;
      }
      force_sendmsg = 0;
      res = 1;
    } else {
      count = prepare_write_chain (u->send_queue, MAX_UDP_SEND_BUFFERS);
      assert (count > 0);

      res = our_sendmmsg (u->fd, udp_send_msgvec, count, MSG_DONTWAIT);
      if (res < 0) {
	if (sendmmsg_supported < 0 || errno == EINTR || errno == EAGAIN) {
	  continue;
	}
	if (errno == EPERM) {
	  force_sendmsg = 1;
	  continue;
	} else if (errno == ECONNREFUSED || errno == EHOSTUNREACH || errno == EHOSTDOWN || errno == ENETUNREACH || errno == ENONET || errno == EMSGSIZE || errno == EOPNOTSUPP || errno == ENOPROTOOPT || errno == ENOBUFS) {
	  u->flags |= U_ERRQ;
	  break;
	} else {
	  fprintf (stderr, "error in sendmmsg(): %m\n");
	  u->flags |= U_NOWR | U_ERROR;
	  return res;
	}
      }
    }
    if (!res) {
      u->flags |= U_NOWR;
    }
    if (!drop_bad) {
      total += res;
    }
    int i;
    for (i = 0; i < res; i++) {
      struct udp_message *m = u->send_queue;
      assert (udp_send_msgvec[i].msg_len == m->raw.total_bytes || drop_bad);
      u->send_queue_bytes -= m->raw.total_bytes;
      u->send_queue_len--;
      u->send_queue = m->next;
      free_udp_message (m);
    }
    if (!u->send_queue) {
      u->send_queue_last = 0;
    }
    vkprintf (2, "sendmmsg: %d datagrams %s (%d total); %d bytes in %d messages still in queue\n", res, drop_bad ? "dropped" : "sent", total, u->send_queue_bytes, u->send_queue_len);
    drop_bad = 0;
  }
  return total;
}

int udp_process_incoming (struct udp_socket *u) {
  vkprintf (2, "udp_process_incoming(%d): %d messages in receive queue (%d bytes); %d messages in send queue (%d bytes)\n", u->fd, u->recv_queue_len, u->recv_queue_bytes, u->send_queue_len, u->send_queue_bytes);
  struct udp_message *msg_cur, *msg_next = u->recv_queue;
  if (!msg_next) {
    return 0;
  }
  do {
    msg_cur = msg_next;
    msg_next = msg_next->next;
    msg_cur->next = 0;
    int res;
    if (msg_cur->flags & UMF_ERROR) {
      res = u->type->process_error_msg (u, msg_cur);
    } else {
      res = u->type->process_msg (u, msg_cur); // >0 -- just remove from queue, <= 0 -- free as well
    }
    if (res <= 0) {
      free_udp_message (msg_cur);
    }
  } while (msg_next);
  u->recv_queue_bytes = u->recv_queue_len = 0;
  u->recv_queue = u->recv_queue_last = 0;
  return 0;
}

int server_receive_udp (int fd, void *data, event_t *ev) {
  struct udp_socket *u = (struct udp_socket *) data;
  assert (u);
  assert (u->type);
  assert (u->ev == ev && u->fd == fd);

  if (ev->ready & EVT_FROM_EPOLL) {
    // update U_NORD / U_NOWR only if we arrived from epoll(); ev->ready &= ~EVT_FROM_EPOLL;
    u->flags &= ~U_NORW;
    if ((ev->state & EVT_READ) && !(ev->ready & EVT_READ)) {
      u->flags |= U_NORD;
    }
    if ((ev->state & EVT_WRITE) && !(ev->ready & EVT_WRITE)) {
      u->flags |= U_NOWR;
    }
  }

  vkprintf (1, "in server_receive_udp(%d) : ev->ready=%d, flags=%d\n", fd, ev->ready, u->flags);
  u->flags |= U_WORKING;

  while (!(u->flags & U_NORD) || (u->send_queue && !(u->flags & U_NOWR)) || (u->flags & U_ERRQ)) {
    if (!(u->flags & U_NORD) || (u->flags & U_ERRQ)) {
      udp_reader (u);
    }
    if (u->recv_queue_len > 0) {
      udp_process_incoming (u);
    }
    if (u->send_queue && !(u->flags & (U_NOWR | U_ERRQ))) {
      udp_writer (u);
    }
  }

  u->flags &= ~U_WORKING;
  assert (!(u->flags & U_ERROR));

  return EVT_LEVEL | EVT_READ | EVT_SPEC | (u->send_queue ? EVT_WRITE : 0);
}

// puts msg into write queue of u
int udp_queue_message (struct udp_socket *u, struct udp_message *msg) {
  int need_wakeup = 0;
  assert (msg->raw.magic == RM_INIT_MAGIC); // or just return 0 ?
  if (msg->raw.magic != RM_INIT_MAGIC || msg->raw.total_bytes > MAX_UDP_SEND_DATAGRAM) {
    return 0;
  }
  if (!u->send_queue) {
    u->send_queue = u->send_queue_last = msg;
    need_wakeup = 1;
  } else {
    u->send_queue_last->next = msg;
    u->send_queue_last = msg;
  }
  u->send_queue_bytes += msg->raw.total_bytes;
  u->send_queue_len++;
  msg->next = 0;
  if (!(u->flags & U_WORKING)) {
    // maybe wake up u ?
    if (need_wakeup && !u->ev->in_queue) {
      put_event_into_heap (u->ev);
    }
  }
  return 1;
}

/* UDP port initialisation */

int process_msg_noop (struct udp_socket *u, struct udp_message *msg) {
  return -1;
}

int listening_udp_sockets;
struct udp_socket *UDPSockets[MAX_UDP_PORTS];

int check_udp_functions (udp_type_t *type) {
  if (type->magic != UDP_FUNC_MAGIC) {
    return -1;
  }
  if (!type->title) {
    type->title = "(unknown)";
  }
  if (!type->process_msg) {
    type->process_msg = process_msg_noop;
  }
  if (!type->process_error_msg) {
    type->process_error_msg = process_msg_noop;
  }
  if (!type->process_send_error) {
    type->process_send_error= process_msg_noop;
  }
  /*
  if (!type->run) {
    type->run = server_read_write;
  }
  */
  return 0;
}

struct udp_socket *init_udp_port (int sfd, int port, udp_type_t *type, void *extra, int mode) {
  if (check_udp_functions (type) < 0) {
    return 0;
  }

  assert (sfd > 2 && sfd < MAX_EVENTS && listening_udp_sockets < MAX_UDP_PORTS);

  if (listening_udp_sockets >= MAX_UDP_PORTS) {
    return 0;
  }

  int a = -1, b = listening_udp_sockets;
  while (b - a > 1) {
    int c = (a + b) >> 1;
    if (sfd < UDPSockets[c]->fd) {
      b = c;
    } else {
      a = c;
    }
  }

  if (a >= 0 && UDPSockets[a]->fd == sfd) {
    assert (UDPSockets[a]->fd != sfd);
  }

  int opt = 1;
  if (setsockopt (sfd, IPPROTO_IP, IP_PKTINFO, &opt, sizeof (opt)) < 0) {
    fprintf (stderr, "setsockopt for %d failed: %m\n", sfd);
  }
  if ((mode & SM_IPV6) && setsockopt (sfd, IPPROTO_IPV6, IPV6_RECVPKTINFO, &opt, sizeof (opt)) < 0) {
    fprintf (stderr, "setsockopt for %d failed: %m\n", sfd);
  }

  struct udp_socket *u = malloc (sizeof (struct udp_socket));
  assert (u);

  memset (u, 0, sizeof (*u));
  u->fd = sfd;
  u->flags = mode & SM_IPV6 ? U_IPV6 : 0;
  u->type = type;
  u->extra = extra;
  u->ev = Events + sfd;
  u->our_port = port;

  memmove (UDPSockets + b + 1, UDPSockets + b, (listening_udp_sockets - b) * sizeof (UDPSockets[0]));
  UDPSockets[b] = u;

  epoll_sethandler (sfd, -5, server_receive_udp, u);
  epoll_insert (sfd, EVT_LEVEL | EVT_READ | EVT_SPEC);
  listening_udp_sockets++;

  return u;
}
