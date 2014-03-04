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

#pragma	once

#include "net-msg.h"
#include "net-events.h"
#include "server-functions.h"

/* default UDP receive buffer size (larger datagrams are kept in several buffers) */
#define	UDP_RECV_BUFFER_SIZE	(2048/4)
/* allocated UDP receive buffers, to be used in one recvmmsg operation */
#define	MAX_UDP_RECV_BUFFERS	256
#define	MAX_UDP_SEND_BUFFERS	1024
/* maximal allowed UDP datagram size, 8192 by standard */
#define	MAX_UDP_RECV_DATAGRAM	8192
#define	MAX_UDP_SEND_DATAGRAM	8192


/* recvmmsg / sendmmsg support/emulation */

struct our_mmsghdr {
  struct msghdr msg_hdr;  /* Message header */
  unsigned int  msg_len;  /* Number of received bytes for header */
};

int sendmmsg_supported, recvmmsg_supported;

int our_sendmmsg (int sockfd, struct our_mmsghdr *msgvec, unsigned int vlen, unsigned int flags);
int our_recvmmsg (int sockfd, struct our_mmsghdr *msgvec, unsigned int vlen, unsigned int flags, struct timespec *timeout);

/* main UDP socket functions */

#define	MAX_OUR_IPS	32
extern unsigned our_ip[MAX_OUR_IPS];
extern unsigned char our_ipv6[MAX_OUR_IPS][16];
extern int used_our_ip, used_our_ipv6;

struct udp_socket;
struct udp_message;

#define	UDP_FUNC_MAGIC	0xa5a5ef11

typedef struct udp_functions {
  int magic;
  char *title;
  //  int (*connected)(struct udp_socket *u);
  int (*process_msg)(struct udp_socket *u, struct udp_message *msg);
  int (*process_error_msg)(struct udp_socket *u, struct udp_message *msg);
  int (*process_send_error)(struct udp_socket *u, struct udp_message *msg);
} udp_type_t;

#define	UMF_ERROR	0x8000

struct udp_message {
  struct udp_message *next;
  unsigned char ipv6[16];
  int port;
  short our_ip_idx;	/* index in our_ip if even, our_ipv6 if odd */
  short flags;		/* UMF_* */
  struct raw_message raw;
};

struct udp_socket {
  int fd;
  int flags;
  unsigned our_port;
  unsigned our_ipv4;
  udp_type_t *type;
  event_t *ev;
  void *extra;
  unsigned char our_ipv6[16];
  // send queue (list of messages)
  int send_queue_len, send_queue_bytes;
  struct udp_message *send_queue, *send_queue_last;
  // receive queue (list of messages)
  int recv_queue_len, recv_queue_bytes;
  struct udp_message *recv_queue, *recv_queue_last;
};

#define	U_NORD		1
#define	U_NOWR		2
#define	U_NORW		(U_NORD | U_NOWR)
#define	U_ERROR		8
#define	U_WORKING	16
#define	U_ERRQ		64
#define	U_IPV6		128

#define	MAX_UDP_PORTS	32

struct udp_socket *init_udp_port (int fd, int port, udp_type_t *type, void *extra, int mode);
int udp_queue_message (struct udp_socket *u, struct udp_message *msg);
int server_receive_udp (int fd, void *data, event_t *ev);
int lookup_our_ip (unsigned ip);
int check_udp_functions (udp_type_t *type);
