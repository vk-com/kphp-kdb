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
              2013 Vitaliy Valtman
*/
#ifndef __NET_UDP_TARGETS__
#define __NET_UDP_TARGETS__
#include <fcntl.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <sys/uio.h>

#include "pid.h"
#include "net-udp.h"
#include "net-msg.h"
#include "crypto/aesni256.h"

#define UDP_CRYPTO_NONE 0
#define UDP_CRYPTO_AES 1

#define UDP_PACKET_SIZE 1200
#define UDP_HEADER_SIZE 200

#define UDP_MAX_BODY_SIZE 1000
#define UDP_SEND_BODY_SIZE 512

// #define RX_TIMEOUT 0.05
// #define TX_TIMEOUT 0.1
// #define CONFIRM_TIMEOUT 0.05
#define RX_TIMEOUT 0.01
#define TX_TIMEOUT 0.04
#define CONFIRM_TIMEOUT 0.03

#define STOP_TIMEOUT 5
#define FAIL_TIMEOUT 30

#define MAX_TIMEOUT 600

#define UDP_ALLOW_UNENC 1
#define UDP_ALLOW_ENC 2
#define UDP_ALLOW_HASH 4
#define UDP_WAIT 8

#define UDP_ERROR_CRC32C -1
#define UDP_ERROR_INCONSISTENT_HEADER -2
#define UDP_ERROR_PARSE -3

#define UDP_WINDOW_SIZE (1 << 16)
#define STOP_WINDOW_SIZE UDP_WINDOW_SIZE
#define START_WINDOW_SIZE (UDP_WINDOW_SIZE >> 1)

#define UDP_PROTO_VERSION 1

enum upd_target_state {
  udp_unknown,
  udp_stopped,
  udp_failed,
  udp_ok
};

struct udp_target;
struct udp_msg;

struct udp_target_methods {
  int (*on_receive) (struct udp_msg *);
  int (*check_perm) (struct process_id *, int init_crypto, struct udp_message *msg);
};

struct tree_udp_msg;
struct tree_udp_msg_constructor;
struct tree_int;
struct tree_udp_target;
struct queue_raw_message;
struct queue_tmp_msg;

struct udp_target {
  struct process_id PID;
  int generation;
  long long hash;
  int ipv6[4];

  enum upd_target_state state;
  int port;
  int flags;
  int aes_ctx_initialized;

  int received_prefix;
  int send_num;
  int max_confirmed;
  int resend_state;
 

  int window_size;
  int unack_size;
  int proto_version;

  double last_ack;
  double last_received;
  double rx_timeout;
  double tx_timeout;

  struct udp_target_set *ST;
  struct tree_udp_msg *sent;
  struct tree_int *received_tree;
  struct tree_int *confirm_tree;
  struct tree_udp_msg_constructor *constructors;
  struct queue_raw_message *out_queue;
  struct udp_target_methods *methods;
  struct queue_tmp_msg *window_queue;

  struct udp_socket *socket;
  
  struct vk_aes_ctx aes_ctx_enc;
  struct vk_aes_ctx aes_ctx_dec;

  struct event_timer rx_timer;
  struct event_timer tx_timer;
  struct event_timer confirm_timer;
  struct event_timer flush_timer;
};

struct udp_target_set {
  int ip;
  short port;
  short state;
  union {
    struct udp_target *S;
    struct {
      struct udp_target *last;
      struct tree_udp_target_by_fpid *T;
      int T_count;
    };
    struct {
      short dst_port;
      short pid;
      int utime;
      int generation;
      unsigned char dst_ipv6[16];
    };
  };
};

struct udp_packet_header {
  struct udp_target *T;
  struct udp_session *S;
  int encr_init_vector[16];
  int sha1[16];
  int random_block[16];
  int packet_num;
  int msg_num;
  int zero_pad_size;
};

struct udp_msg {
  int msg_num;
  union {
    struct udp_target *S;
    long long prev_next;
  };
  unsigned char ipv6[16];
  short port;
  struct raw_message raw;
};

struct udp_msg_constructor {
  int msg_num;
  int total_parts;
  int parts;
  struct raw_message msgs[0];
};

extern struct udp_target_methods *default_udp_target_methods;

int udp_target_process_msg (struct udp_socket *u, struct udp_message *msg);
int udp_target_flush (struct udp_target *S);
int udp_target_send (struct udp_target *S, struct raw_message *msg, int clone);
struct udp_target *udp_target_create_by_pid (struct process_id *PID, /*struct udp_target_methods *methods, */int *was_created);
struct udp_target *udp_target_create (struct process_id *PID, unsigned char ipv6[16], int port, int *was_created);
struct udp_target *udp_target_set_choose_target (struct udp_target_set *ST);
struct udp_target *udp_target_lookup (struct process_id *PID);

extern struct udp_socket *default_udp_socket;

#define UDP_FUNC(u) ((struct udp_target_methods *)((u)->extra))
#endif
