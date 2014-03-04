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
#include <stdlib.h>
#include <assert.h>
#include <string.h>
#include <unistd.h>
#include <stddef.h>

#include "kdb-data-common.h"
#include "net-udp-targets.h"
#include "TL/constants.h"
#include "net-crypto-aes.h"
#include "crc32c.h"

#include "vv-tree.h"
#include "vv-tl-parse.h"
#include "vv-io.h"

//#define USE_SHA1

struct udp_target_methods *default_udp_target_methods;
struct udp_socket *default_udp_socket;
int udp_error_crc32;
int udp_error_magic;
int udp_error_bad_flags;
int udp_error_pid;
int udp_error_parse;
int udp_error_crypto;
int udp_error_time;

int udp_tx_timeout_cnt;
int udp_rx_timeout_cnt;

long long udp_data_sent;
long long udp_data_body_sent;
long long udp_packets_sent;
long long udp_encrypted_packets_sent;
long long udp_unencrypted_packets_sent;

long long udp_data_received;
long long udp_data_body_received;
long long udp_packets_received;
long long udp_encrypted_packets_received;
long long udp_unencrypted_packets_received;

double udp_drop_probability;

#define udp_target_cmp_pid(a,b) (memcmp (&(a)->PID, &(b)->PID, 6))
#define udp_target_cmp_fpid(a,b) ((a)->PID.utime != (b)->PID.utime ? (a)->PID.utime - (b)->PID.utime : (a)->PID.pid - (b)->PID.pid)
#define udp_target_cmp_hash(a,b) (memcmp (&(a)->hash, &(b)->hash, 8))
#define udp_target_set_cmp(a,b) (memcmp ((a), (b), 6))
#define udp_msg_cmp(a,b) ((a)->msg_num - (b)->msg_num)

DEFINE_TREE_STD_ALLOC(udp_target_by_pid,struct udp_target *,udp_target_cmp_pid,int,std_int_compare)
struct tree_udp_target_by_pid *udp_target_by_pid;

DEFINE_TREE_STD_ALLOC(udp_target_by_fpid,struct udp_target *,udp_target_cmp_fpid,int,std_int_compare)

DEFINE_TREE_STD_ALLOC(udp_target_by_hash,struct udp_target *,udp_target_cmp_hash,int,std_int_compare)
struct tree_udp_target_by_hash *udp_target_by_hash;

DEFINE_TREE_STD_ALLOC(udp_msg,struct udp_msg *,udp_msg_cmp,int,std_int_compare)
DEFINE_TREE_STD_ALLOC(udp_msg_constructor,struct udp_msg_constructor *,udp_msg_cmp,int,std_int_compare)

DEFINE_TREE_STD_ALLOC(int,int,std_int_compare,int,std_int_compare)

DEFINE_TREE_STD_ALLOC(udp_target_set,struct udp_target_set *,udp_target_set_cmp,int,std_int_compare)

DEFINE_QUEUE (raw_message,struct raw_message)

struct tmp_msg {
  struct udp_message *msg;
  int value_bytes;
};

DEFINE_QUEUE (tmp_msg,struct tmp_msg)

struct tree_udp_target_set *udp_target_set_tree;

void udp_target_update_hash (struct udp_target *S);

struct udp_target *udp_target_alloc (struct process_id *PID, int generation, unsigned char dst_ipv6[16], short dst_port) {
  vkprintf (2, "udp_target_alloc: PID = " PID_PRINT_STR ", generation = %d\n", PID_TO_PRINT (PID), generation);
  struct udp_target *S;
  assert (PID->ip);
  S = zmalloc0 (sizeof (*S));
  S->PID = *PID;
  memcpy (S->ipv6, dst_ipv6, 16);
  S->port = dst_port;
  S->received_prefix = -1;
  S->max_confirmed = -1;
  S->generation = generation;

  S->rx_timeout = RX_TIMEOUT;
  S->tx_timeout = TX_TIMEOUT;

  S->flags = UDP_ALLOW_UNENC | UDP_ALLOW_ENC;

  udp_target_update_hash (S);
  return S;
}

void udp_target_set_expand (struct udp_target_set *ST) {
  assert (ST && ST->state == 1);
  struct process_id P;
  P.ip = ST->ip;
  P.port = ST->port;
  P.pid = ST->pid;
  P.utime = ST->utime;

  struct udp_target *S = udp_target_alloc (&P, ST->generation, ST->dst_ipv6, ST->dst_port);
  
  ST->state = 2;
  ST->S = S;
  S->ST = ST;
}

void udp_target_set_to_tree (struct udp_target_set *ST) {
  assert (ST->state == 2);
  struct udp_target *S = ST->S;
  ST->last = S;
  ST->T = 0;
  ST->T = tree_insert_udp_target_by_fpid (ST->T, S, lrand48 ());
  ST->T_count = 1;
  ST->state = 3;
}

struct udp_target *udp_target_get_by_pid (struct process_id *PID) {
  struct udp_target_set **_S = tree_lookup_value_udp_target_set (udp_target_set_tree, (void *)PID);
  if (!_S) { return 0; }
  struct udp_target_set *ST = *_S;
  struct udp_target *S;
  struct udp_target **SS;
  switch (ST->state) {
  case 0:
    return 0;
  case 1:
    udp_target_set_expand (ST);
  case 2:
    S = ST->S;
    if (!PID->pid && !PID->utime) {
      return S;
    } else if (!S->PID.pid && !S->PID.utime) {
      return S;
    } else if (matches_pid (&S->PID, PID) == 2) {    
      return S;
    } else {
      return 0;
    }
  case 3:
    if (!PID->pid && !PID->utime) {
      return ST->last;
    } else {
      SS = tree_lookup_value_udp_target_by_fpid (ST->T, (void *)PID);
      return SS ? *SS : 0;
    }
  default:
    assert (0);
    return 0;
  }
}

struct udp_target *udp_target_lookup (struct process_id *PID) {
  return udp_target_get_by_pid (PID);
}

struct udp_target *udp_target_set_choose_target (struct udp_target_set *ST) {
  assert (ST);
  switch (ST->state) {
  case 0:
    return 0;
  case 1:
    udp_target_set_expand (ST);
  case 2:
    return ST->S;
  case 3:
    return ST->last;
  default:
    assert (0);
    return 0;
  }
}

struct udp_target *udp_target_get_by_hash (long long hash) {
  struct udp_target **S = tree_lookup_value_udp_target_by_hash (udp_target_by_hash, (struct udp_target *)(((char *)&hash) - offsetof (struct udp_target, hash)));
  return S ? *S : 0;
}

struct udp_target *__udp_target_create (struct process_id *PID, int generation, unsigned char dst_ipv6[16], short dst_port, int *was_created) {
  vkprintf (2, "__udp_target_create: PID = " PID_PRINT_STR ", generation = %d\n", PID_TO_PRINT (PID), generation);
  struct udp_target_set **_S = tree_lookup_value_udp_target_set (udp_target_set_tree, (void *)PID);
  struct udp_target_set *ST;
  if (!_S) {
    ST = zmalloc (sizeof (*ST));
    ST->ip = PID->ip;
    ST->port = PID->port;
    ST->state = 0;
    udp_target_set_tree = tree_insert_udp_target_set (udp_target_set_tree, ST, lrand48 ());
    vkprintf (2, "ST allocated\n");
  } else {
    ST = *_S;
  }
  struct udp_target *S;
  struct udp_target **SS;
  vkprintf (2, "ST->state = %d\n", ST->state);
  if (ST->state == 2) {
    vkprintf (2, "ST->S->PID = " PID_PRINT_STR "\n", PID_TO_PRINT (&ST->S->PID));
  }
  switch (ST->state) {
  case 0:
    ST->state = 2;
    ST->S = udp_target_alloc (PID, generation, dst_ipv6, dst_port);
    ST->S->ST = ST;
    if (was_created) { *was_created = 1; }
    return ST->S;
  case 1:
    udp_target_set_expand (ST);
  case 2:
    S = ST->S;
    if (matches_pid (&S->PID, PID) || matches_pid (PID, &S->PID)) {
      if (was_created) { *was_created = 0; }
      return S;
    } else {
      udp_target_set_to_tree (ST);
      S = udp_target_alloc (PID, generation, dst_ipv6, dst_port);
      vkprintf (2, "S->PID = " PID_PRINT_STR "\n", PID_TO_PRINT (&S->PID));
      ST->T = tree_insert_udp_target_by_fpid (ST->T, S, lrand48 ());
      ST->T_count ++;
      S->ST = ST;
      if (was_created) { *was_created = 1; }
      return S;
    }
  case 3:
    if (!PID->pid && !PID->utime) {
      if (was_created) { *was_created = 0; }
      return ST->last;
    } else {
      SS = tree_lookup_value_udp_target_by_fpid (ST->T, (void *)PID);
      if (SS) { 
        if (was_created) { *was_created = 0; }
        return *SS; 
      }
      S = udp_target_alloc (PID, generation, dst_ipv6, dst_port);
      ST->T = tree_insert_udp_target_by_fpid (ST->T, S, lrand48 ());
      ST->T_count ++;
      S->ST = ST;
      if (was_created) { *was_created = 1; }
      return S;
    }
  default:
    assert (0);
    return 0;
  }
}

struct udp_target *udp_target_create_by_pid (struct process_id *PID, int *was_created) {
  static unsigned char ipv6[16];
  set_4in6 (ipv6, htonl (PID->ip));  
  return __udp_target_create (PID, 0, ipv6, PID->port, was_created);
}

struct udp_target *udp_target_create (struct process_id *PID, unsigned char ipv6[16], int port, int *was_created) {
  return __udp_target_create (PID, 0, ipv6, port, was_created);
}

void udp_target_update_hash (struct udp_target *S) {
  static int P[7];
  int x = memcmp (&PID, &S->PID, 12);
  if (x < 0) {
    memcpy (P, &PID, 12);
    memcpy (P + 3, &S->PID, 12);
  } else {
    memcpy (P, &S->PID, 12);
    memcpy (P + 3, &PID, 12);
  }
  P[6] = S->generation;
  if (S->hash) {
    vkprintf (2, "delete: hash = %lld\n", S->hash);
    udp_target_by_hash  = tree_delete_udp_target_by_hash (udp_target_by_hash, S);
  }
  S->hash = crc64 (P, 28);
  vkprintf (2, "insert: hash = %lld\n", S->hash);
  udp_target_by_hash = tree_insert_udp_target_by_hash (udp_target_by_hash, S, lrand48 ());
}

void udp_target_update_pid (struct udp_target *S, struct process_id *PID, int generation) {
  vkprintf (2, "udp_target_update_pid: PID = " PID_PRINT_STR ", generation = %d\n", PID_TO_PRINT (PID), generation);
  assert (!memcmp (&S->PID, PID, 6));
  if (!memcmp (PID, &S->PID, sizeof (struct process_id))) {
    S->generation = generation;
    udp_target_update_hash (S);
  } else {
    struct udp_target_set *ST = S->ST;
    assert (ST);
    switch (ST->state) {
    case 0:
      assert (0);
      return;
    case 1:
      udp_target_set_expand (ST);
    case 2:
      assert (ST->S == S);
      S->PID = *PID;
      S->generation = generation;
      udp_target_update_hash (S);
      return;
    case 3:
      ST->T = tree_delete_udp_target_by_fpid (ST->T, S);
      S->PID = *PID;
      S->generation = generation;
      ST->T = tree_insert_udp_target_by_fpid (ST->T, S, lrand48 ());
      udp_target_update_hash (S);
      return;
    default:
      assert (0);
    }
  }
}

void udp_target_renew (struct udp_target *S, struct process_id *pid, int generation);
void udp_target_set_generation (struct udp_target *S, int generation) {
  if (S->generation >= generation) { return; }
  udp_target_renew (S, &S->PID, generation);
}

void __clear_udp_msg_constructor (struct udp_msg_constructor *M) {
  int i;
  for (i = 0; i < M->total_parts; i++) {
    if (M->msgs[i].magic) {
      rwm_free (&M->msgs[i]);
    }
  }
  free (M);
}

void udp_msg_free (struct udp_msg *msg);
void udp_target_renew (struct udp_target *S, struct process_id *pid, int generation) {
  udp_target_update_pid (S, pid, generation);
  S->received_prefix = -1;
  S->confirm_tree = tree_clear_int (S->confirm_tree);  
  S->received_tree = tree_clear_int (S->received_tree);
  tree_act_udp_msg_constructor (S->constructors, __clear_udp_msg_constructor);
  S->constructors = tree_clear_udp_msg_constructor (S->constructors);
  tree_act_udp_msg (S->sent, udp_msg_free);
  S->sent = tree_clear_udp_msg (S->sent);
  S->aes_ctx_initialized = 0;
  S->send_num = 0;
  S->last_ack = 0;
  S->max_confirmed = -1;
  S->window_size = 0;
  S->flags = UDP_ALLOW_UNENC | UDP_ALLOW_ENC;
  S->unack_size = 0;
  S->proto_version = 0;
  
  S->rx_timeout = RX_TIMEOUT;
  S->tx_timeout = TX_TIMEOUT;

  while (S->out_queue) {
    struct raw_message raw = queue_get_first_raw_message (S->out_queue)->x;
    rwm_free (&raw);
    S->out_queue = queue_del_first_raw_message (S->out_queue);
  }

  while (S->window_queue) {
    struct tmp_msg x = queue_get_first_tmp_msg (S->window_queue)->x;
    rwm_free (&x.msg->raw);
    free (x.msg);    
    S->window_queue = queue_del_first_tmp_msg (S->window_queue);
  }

  if (S->tx_timer.h_idx) {
    remove_event_timer (&S->tx_timer);
  }
  if (S->rx_timer.h_idx) {
    remove_event_timer (&S->rx_timer);
  }
  if (S->confirm_timer.h_idx) {
    remove_event_timer (&S->confirm_timer);
  }
}

void udp_target_free (struct udp_target *S) {
  if (S->hash) {
    udp_target_by_hash  = tree_delete_udp_target_by_hash (udp_target_by_hash, S);
  }
  S->confirm_tree = tree_clear_int (S->confirm_tree);
  S->received_tree = tree_clear_int (S->received_tree);
  tree_act_udp_msg_constructor (S->constructors, __clear_udp_msg_constructor);
  S->constructors = tree_clear_udp_msg_constructor (S->constructors);
  tree_act_udp_msg (S->sent, udp_msg_free);
  S->sent = tree_clear_udp_msg (S->sent);

  while (S->out_queue) {
    struct raw_message raw = queue_get_first_raw_message (S->out_queue)->x;
    rwm_free (&raw);
    S->out_queue = queue_del_first_raw_message (S->out_queue);
  }

  while (S->window_queue) {
    struct tmp_msg x = queue_get_first_tmp_msg (S->window_queue)->x;
    rwm_free (&x.msg->raw);
    free (x.msg);    
    S->window_queue = queue_del_first_tmp_msg (S->window_queue);
  }

  if (S->tx_timer.h_idx) {
    remove_event_timer (&S->tx_timer);
  }
  if (S->rx_timer.h_idx) {
    remove_event_timer (&S->rx_timer);
  }
  if (S->confirm_timer.h_idx) {
    remove_event_timer (&S->confirm_timer);
  }
  zfree (S, sizeof (*S));
}

void udp_target_delete_by_pid (struct process_id *pid) {
  struct udp_target_set **_S = tree_lookup_value_udp_target_set (udp_target_set_tree, (void *)pid);
  if (!_S) { return; }
  struct udp_target_set *ST = *_S;
  switch (ST->state) {
  case 0:
    return;
  case 1:
    if (ST->utime == pid->utime && ST->pid == pid->pid) {
      ST->state = 0;
    }
    return;
  case 2:
    if (ST->S->PID.utime == pid->utime && ST->S->PID.pid == pid->pid) {
      udp_target_free (ST->S);
      ST->state = 0;
    }
    return;
  case 3:
    {
      int ok = ST->last->PID.utime != pid->utime || ST->last->PID.pid != pid->pid;

      struct udp_target **_S = tree_lookup_value_udp_target_by_fpid (ST->T, (void *)pid);
      if (_S) {
        struct udp_target *S = *_S;

        assert (ST->T_count >= 2);
        ST->T = tree_delete_udp_target_by_fpid (ST->T, S);
        ST->T_count --;
        udp_target_free (S);
        
        S = tree_get_min_udp_target_by_fpid (ST->T)->x;

        if (ST->T_count == 1) {
          tree_clear_udp_target_by_fpid (ST->T);
          ST->state = 2;
          ST->S = S;
        } else {
          if (!ok) { 
            ST->last = S;
          }
        }
      }
      return;
    }
  default:
    assert (0);
    return;
    
  }

}

void udp_target_delete_before (struct process_id *pid, int utime) {
  struct udp_target_set **_S = tree_lookup_value_udp_target_set (udp_target_set_tree, (void *)pid);
  if (!_S) { return; }
  struct udp_target_set *ST = *_S;
  switch (ST->state) {
  case 0:
    return;
  case 1:
    if (ST->utime < utime) {
      ST->state = 0;
    }
    return;
  case 2:
    if (ST->S->PID.utime < utime && ST->S->PID.utime) {
      udp_target_free (ST->S);
      ST->state = 0;
    }
    return;
  case 3:
    {
      int ok = ST->last->PID.utime < utime;
      struct tree_udp_target_by_fpid *L, *R;
      struct process_id P;
      P.utime = utime ;
      P.pid = 0;
      tree_split_udp_target_by_fpid (&L, &R, ST->T, (void *)&P);
      ST->T = R;
      ST->T_count -= tree_count_udp_target_by_fpid (L);
      tree_act_udp_target_by_fpid (L, udp_target_free);
      tree_clear_udp_target_by_fpid (L);

      assert (ST->T_count >= 0);
      if (ST->T_count == 0) {
        ST->state = 0;
      } else if (ST->T_count == 1) {
        struct udp_target *S = ST->T->x;
        tree_clear_udp_target_by_fpid (ST->T);
        ST->state = 2;
        ST->S = S; 
      } else {
        if (ok) {
          ST->last = tree_get_min_udp_target_by_fpid (ST->T)->x;
        }
      }
      return;
    }
  default:
    assert (0);
    return;
    
  }
}

void udp_msg_free (struct udp_msg *msg) {
  assert (msg);
  rwm_free (&msg->raw);
  zfree (msg, sizeof (*msg));
}

struct udp_msg *udp_msg_create (struct raw_message *raw, int packet_id, struct udp_target *S) {
  struct udp_msg *msg = zmalloc (sizeof (*msg));
  msg->msg_num = packet_id;
  msg->S = S;
  msg->raw = *raw;
  return msg;
}


void resend_range (struct udp_target *S, int from, int to, int all);
void start_tx_timer (struct udp_target *S);
int tx_gateway (struct event_timer *ev) {
  struct udp_target *S = (void *)(((char *)ev) - offsetof (struct udp_target, tx_timer));
  S->tx_timeout *= 1.5;
  //vkprintf (0, "TX TIMEOUT\n");
  if (S->tx_timeout > MAX_TIMEOUT) {
    S->tx_timeout = MAX_TIMEOUT;
  }
  if (S->tx_timeout > STOP_TIMEOUT) {
    S->state = udp_stopped;
  }
  if (S->tx_timeout > FAIL_TIMEOUT) {
    S->state = udp_failed;
  }

  if (S->max_confirmed + 1 <= S->send_num - 1) {
    //vkprintf (0, "Resending %d .. %d\n", S->max_confirmed + 1, S->send_num - 1);
    resend_range (S, S->max_confirmed + 1, S->send_num - 1, S->tx_timeout == 1.5 * TX_TIMEOUT);
    S->resend_state = 1;
  }

  if (S->max_confirmed + 1 < S->send_num) {
    start_tx_timer (S);
  }

  udp_tx_timeout_cnt ++;
  return 0;
}

void start_tx_timer (struct udp_target *S) {
  struct event_timer *ev = &S->tx_timer;
  ev->wakeup_time = precise_now + S->tx_timeout;
  ev->wakeup = tx_gateway;
  assert (!ev->h_idx);
  ev->h_idx = 0;
  insert_event_timer (ev);
}

void stop_tx_timer (struct udp_target *S) {
  remove_event_timer (&S->tx_timer);
}
int flush_gateway (struct event_timer *ev) {
  struct udp_target *S = (void *)(((char *)ev) - offsetof (struct udp_target, flush_timer));

  udp_target_flush (S);
  return 0;
}

void start_flush_timer (struct udp_target *S) {
  struct event_timer *ev = &S->flush_timer;
  ev->wakeup_time = precise_now;
  ev->wakeup = flush_gateway;
  assert (!ev->h_idx);
  ev->h_idx = 0;
  insert_event_timer (ev);
}

void stop_flush_timer (struct udp_target *S) {
  remove_event_timer (&S->flush_timer);
}

struct udp_target *__S;

void udp_msg_confirm_free (struct udp_msg *msg) {
  __S->window_size -= msg->raw.total_bytes;
  udp_msg_free (msg);
}

void udp_target_restart_send (struct udp_target *S);

void udp_target_ack (struct udp_target *S, int x) {
  if (x >= S->send_num) {
    return;
  }
  if (x > S->max_confirmed) {
    stop_tx_timer (S);
    S->max_confirmed = x;
    assert (x < S->send_num);
    if (x != S->send_num - 1) {
      start_tx_timer (S);
    }
    S->resend_state = 0;
  }
  S->last_ack = precise_now;
  struct udp_msg **M = tree_lookup_value_udp_msg (S->sent, (void *)&x);
  if (!M) {
    return;
  }
  struct udp_msg *msg = *M;
  S->sent = tree_delete_udp_msg (S->sent, msg);
  vkprintf (4, "S->sent: delete message %d\n", x);
  __S = S;
  udp_msg_confirm_free (msg);
  if (S->window_size <= START_WINDOW_SIZE && (S->flags & UDP_WAIT)) {
    udp_target_restart_send (S);
  }
}

void udp_target_ack_prefix (struct udp_target *S, int x) {
  if (x >= S->send_num) {
    return;
  }
  if (x > S->max_confirmed) {
    stop_tx_timer (S);
    S->max_confirmed = x;
    assert (x < S->send_num);
    if (x != S->send_num - 1) {
      start_tx_timer (S);
    }
    S->resend_state = 0;
  }
  S->last_ack = precise_now;
  struct tree_udp_msg *L, *R;
  tree_split_udp_msg (&L, &R, S->sent, (void *)&x);
  S->sent = R;
  __S = S;
  tree_act_udp_msg (L, udp_msg_confirm_free);
  tree_clear_udp_msg (L);
  vkprintf (4, "S->sent: delete messages up to %d\n", x);
  if (S->window_size <= START_WINDOW_SIZE && (S->flags & UDP_WAIT)) {
    udp_target_restart_send (S);
  }
}

void udp_target_ack_range (struct udp_target *S, int x, int y) {
  if (y >= S->send_num) {
    return;
  }
  if (y > S->max_confirmed) {
    stop_tx_timer (S);
    S->max_confirmed = y;
    assert (y < S->send_num);
    if (y != S->send_num - 1) {
      start_tx_timer (S);
    }
    S->resend_state = 0;
  }
  S->last_ack = precise_now;
  struct tree_udp_msg *L, *R, *M1, *M2;
  tree_split_udp_msg (&M1, &R, S->sent, (void *)&y);
  x--;
  tree_split_udp_msg (&L, &M2, M1, (void *)&x);

  S->sent = tree_merge_udp_msg (L, R);
  __S = S;
  tree_act_udp_msg (M2, udp_msg_confirm_free);
  tree_clear_udp_msg (M2);
  vkprintf (4, "S->sent: delete messages in range [%d, %d]\n", x, y);
  if (S->window_size <= START_WINDOW_SIZE && (S->flags & UDP_WAIT)) {
    udp_target_restart_send (S);
  }
}

int msg_is_dup (struct udp_target *S, int x) {
  return (x <= S->received_prefix || tree_lookup_int (S->received_tree, x));
}

void start_rx_timer (struct udp_target *S);

int __f;
int *__a;
int __c;

void __rx_do (int t) {
  if (__f < t && __c < 50) {
    __a[2 * __c + 0] = __f;
    __a[2 * __c + 1] = t - 1;
    __c ++;
  }
  __f = t + 1;
}

int rx_gateway (struct event_timer *ev) {
  struct udp_target *S = (void *)(((char *)ev) - offsetof (struct udp_target, rx_timer));
  S->rx_timeout *= 1.5;
  //vkprintf (0, "RX TIMEOUT\n");
  if (S->rx_timeout > MAX_TIMEOUT) {
    S->rx_timeout = MAX_TIMEOUT;
  }
  if (S->rx_timeout > STOP_TIMEOUT) {
    S->state = udp_stopped;
  }
  if (S->rx_timeout > FAIL_TIMEOUT) {
    S->state = udp_failed;
  }
  assert (S->received_tree);
//  int x = tree_get_min_int (S->received_tree)->x;
//  assert (x > S->received_prefix + 1);
//  vkprintf (1, "Ask: resend all messages from %d to %d from pid " PID_PRINT_STR "\n", S->received_prefix + 1, x - 1, PID_TO_PRINT (&S->PID));
  __f = S->received_prefix + 1;
  int a[102];
  __a = a + 2;
  __c = 0;
  tree_act_int (S->received_tree, __rx_do);
  assert (__c > 0);

  a[0] = TL_UDP_RESEND_REQUEST_EXT;
  a[1] = __c;
/*  a[1] = S->received_prefix + 1;
  a[2] = x - 1;*/
  struct raw_message r;
  rwm_create (&r, a, 8 * (__c + 1));
  udp_target_send (S, &r, 0);
  udp_target_flush (S);
  start_rx_timer (S);
  udp_rx_timeout_cnt ++;
  return 0;
}

void start_rx_timer (struct udp_target *S) {
  struct event_timer *ev = &S->rx_timer;
  ev->wakeup_time = precise_now + S->rx_timeout;
  ev->wakeup = rx_gateway;
  assert (!ev->h_idx);
  ev->h_idx = 0;
  insert_event_timer (ev);
}

void stop_rx_timer (struct udp_target *S) {
  remove_event_timer (&S->rx_timer);
}

void udp_confirms_send (struct udp_target *S);
int confirm_gateway (struct event_timer *ev) {
  struct udp_target *S = (void *)(((char *)ev) - offsetof (struct udp_target, confirm_timer));
  udp_confirms_send (S);
  return 0;
}

void start_confirm_timer (struct udp_target *S) {
  struct event_timer *ev = &S->confirm_timer;
  ev->wakeup_time = precise_now + CONFIRM_TIMEOUT;
  ev->wakeup = confirm_gateway;
  assert (!ev->h_idx);
  ev->h_idx = 0;
  insert_event_timer (ev);
}

void stop_confirm_timer (struct udp_target *S) {
  remove_event_timer (&S->confirm_timer);
}

void add_received (struct udp_target *S, int x) {
  if (x == S->received_prefix + 1) {
    S->received_prefix ++;
    if (S->received_tree) {
      while (S->received_tree) {
        int y = tree_get_min_int (S->received_tree)->x;
        assert (y >= S->received_prefix + 1);
        if (y == S->received_prefix + 1) {
          S->received_tree = tree_delete_int (S->received_tree, y);
          S->received_prefix ++;
        } else {
          break;
        }
      }
      if (!S->received_tree) {
        stop_rx_timer (S);
      }
    }
  } else {
    if (!S->received_tree) {
      start_rx_timer (S);
    }
    S->received_tree = tree_insert_int (S->received_tree, x, lrand48 ());
  }
}

void add_force_confirm (struct udp_target *S, int x) {
  if (!S->confirm_timer.h_idx) {
    start_confirm_timer (S);
  }
  if (!tree_lookup_int (S->confirm_tree, x)) {
    S->confirm_tree = tree_insert_int (S->confirm_tree, x, lrand48 ());
    S->unack_size ++;
    if (S->unack_size >= START_WINDOW_SIZE / 2000) {
      udp_confirms_send (S);
    }
  }
}

struct udp_msg *msg_constructor_add (struct raw_message *raw, int packet_id, int part, int total_parts, struct udp_target *S) {
  assert (packet_id >= 0);
  add_force_confirm (S, packet_id + part);
  if (msg_is_dup (S, packet_id + part)) {
    vkprintf (1, "msg_is_dup: duplicate message %d. S->send_num = %d, S->max_confirmed = %d\n", packet_id, S->send_num, S->max_confirmed);
    rwm_free (raw);
    return 0;
  }
  add_received (S, packet_id + part);
  struct udp_msg_constructor **_M = tree_lookup_value_udp_msg_constructor (S->constructors, (void *)&packet_id);
  struct udp_msg_constructor *M;
  if (_M) {
    M = *_M;
  } else {
    M = malloc (sizeof (*M) + total_parts * sizeof (struct raw_message));
    M->msg_num = packet_id;
    M->total_parts = total_parts;
    M->parts = total_parts;
    memset (M->msgs, 0, total_parts * sizeof (struct raw_message));
    S->constructors = tree_insert_udp_msg_constructor (S->constructors, M, lrand48 ());
  }
//  rwm_clone (&M->msgs[part], raw);
  M->msgs[part] = *raw;
  M->parts --;
  vkprintf (3, "packet_id = %d. %d parts left\n", packet_id, M->parts);
  if (M->parts) { return 0; }
  struct raw_message *r = &M->msgs[0];
  int i;
  for (i = 1; i < M->total_parts; i++) {
    assert (M->msgs[i].magic);
    rwm_union (r, &M->msgs[i]);
  }

  struct udp_msg *msg = udp_msg_create (r, packet_id, S);

  S->constructors = tree_delete_udp_msg_constructor (S->constructors, M);
  free (M);
  
  return msg;
}

struct udp_msg *msg_create (struct raw_message *raw, int packet_id, struct udp_target *S) {
  if (packet_id >= 0) {
    add_force_confirm (S, packet_id);
    if (msg_is_dup (S, packet_id)) {
      vkprintf (1, "msg_is_dup: duplicate message %d. S->send_num = %d, S->max_confirmed = %d\n", packet_id, S->send_num, S->max_confirmed);
      rwm_free (raw);
      return 0;
    }
    add_received (S, packet_id);
  }
  struct udp_msg *msg = udp_msg_create (raw, packet_id, S);
  return msg;
}

int default_udp_check_perm (/*struct udp_target *S,*/ struct process_id *remote_pid, int crypto_flags, struct udp_message *msg) {
  vkprintf (3, "default_udp_check_perm: crypto_flags = 0x%02x\n", crypto_flags & 0xff);
//  int allow_unenc = (remote_pid->ip == PID.ip);
  int allow_unenc = UDP_ALLOW_UNENC;
  int crypto_mode = crypto_flags & 127;
  if (crypto_mode != UDP_CRYPTO_NONE && crypto_mode != UDP_CRYPTO_AES) {
    vkprintf (1, "unknown crypto protocol %d\n", crypto_mode);
    return -1;
  }
  if (remote_pid->ip != PID.ip) {
    allow_unenc = 0;
  }
  if (!(crypto_flags & 128)) {
    allow_unenc = 0;
  }
  if (msg->our_ip_idx != 1 && msg->our_ip_idx != 2) {
    allow_unenc = 0;
  }
  int h1 = (crypto_flags >> 8) & 0xfff;
  //int h2 = (crypto_flags >> 20) & 0xfff;

  int allow_enc = UDP_ALLOW_ENC;
  if (crypto_mode == UDP_CRYPTO_NONE) {
    allow_enc = 0;
  }

  if ((h1 != (get_crypto_key_id () & 0xfff)) /*&& (h2 != (get_crypto_key_id () & 0xfff))*/) {
    allow_enc = 0;
  }
  vkprintf (3, "check_perm returns %d\n", allow_unenc | allow_enc);
  return allow_unenc | allow_enc;
}

int decrypt_udp_message (struct raw_message *msg, struct vk_aes_ctx *ctx, unsigned char *key) {
  if (msg->total_bytes & 15) {
    vkprintf (1, "Length of encrypted part is not multiple of 16\n");
    return -1;
  }

  static unsigned char t[32];
  memcpy (t, key, 32);
  assert (!(msg->total_bytes & 15));
  if (verbosity >= 4) {
    rwm_dump (msg);
  }
  assert (rwm_encrypt_decrypt (msg, msg->total_bytes, ctx, key) == msg->total_bytes);

  if (verbosity >= 4) {
    rwm_dump (msg);
  }

#ifdef USE_SHA1
  static unsigned char c[20];
  assert (rwm_sha1 (msg, msg->total_bytes, c) == msg->total_bytes);
  if (memcmp (c, t, 16)) {
    vkprintf (1, "Sha1 mismatch\n");
    return -1;
  }
#endif
  return 0;
}

int work_resend_request (struct udp_msg *msg);
int work_resend_request_ext (struct udp_msg *msg);
void work_obsolete_pid (struct udp_msg *msg);
void work_obsolete_hash (struct udp_msg *msg);
void work_obsolete_generation (struct udp_msg *msg);

int receive_message (struct udp_socket *u, struct udp_target *S, struct udp_msg *msg) {
  if (msg->raw.total_bytes >= 4) {
    //rwm_dump_sizes (&msg->raw);
    int op;
    assert (rwm_fetch_lookup (&msg->raw, &op, 4) == 4);
    switch (op) {
    case TL_UDP_RESEND_REQUEST:
      work_resend_request (msg);
      return 0;
    case TL_UDP_RESEND_REQUEST_EXT:
      work_resend_request_ext (msg);
      return 0;
    case TL_UDP_NOP:
      return 0;
    case TL_UDP_DISABLE_ENCRYPTION:
      return 0;
    case TL_UDP_OBSOLETE_PID:
      work_obsolete_pid (msg);
      return 0;
    case TL_UDP_OBSOLETE_HASH:
      work_obsolete_hash (msg);
      return 0;
    case TL_UDP_OBSOLETE_GENERATION:
      work_obsolete_generation (msg);
      return 0;
    default:
      UDP_FUNC (u)->on_receive (msg);
      return 0;
    }
  } else {
    UDP_FUNC (u)->on_receive (msg);
//    S->methods->on_receive (msg);
    return 0;
  }
}

static void send_error_obsolete_pid (struct udp_socket *u, struct udp_message *msg, struct process_id *remote_pid, struct process_id *local_pid, int generation);
static void send_error_obsolete_hash (struct udp_socket *u, struct udp_message *msg, long long hash);
static void send_error_obsolete_generation (struct udp_socket *u, struct udp_message *msg, struct udp_target *S, int generation);
static void send_error_wrong_pid (struct udp_socket *u, struct udp_message *msg, struct process_id *remote_pid, struct process_id *pid);
static void send_error_bad_packet (struct udp_socket *u, struct udp_message *msg, int error_code);
static void send_error_unsupported_encr (struct udp_socket *u, struct udp_message *msg, struct process_id *remote_pid, int generation, struct process_id *local_pid);

extern int rwm_total_msgs;
int udp_target_process_msg (struct udp_socket *u, struct udp_message *msg) {
  udp_packets_received ++;
  udp_data_received += msg->raw.total_bytes;

  assert (u->extra);
  assert (msg->raw.magic == RM_INIT_MAGIC);
  struct raw_message *raw = &msg->raw;
  int size = raw->total_bytes;
  vkprintf (2, "got message: len = %d\n", size);
  int y;
  assert (rwm_fetch_data_back (raw, &y, 4) == 4);
  unsigned crc32c = rwm_crc32c (raw, size - 4);
  if (y != crc32c) {
    vkprintf (1, "Crc32c check failed: calculated 0x%08x, read 0x%08x\n", crc32c, y);
    udp_error_crc32 ++;
    return 0;
  }



  vkprintf (3, "Crc32c check ok (crc32c = %08x)\n",  crc32c);
  tl_fetch_init_raw_message (raw, size - 4);

  int x = tl_fetch_int ();
  if (x != TL_NET_UDP_PACKET_UNENC_HEADER) {
    vkprintf (1, "Expected TL_NET_UDP_PACKET_UNENC_HEADER (0x%08x), presented 0x%08x\n", TL_NET_UDP_PACKET_UNENC_HEADER, x);
    udp_error_magic ++;
    return 0;
  }

  int flags = tl_fetch_int ();
  vkprintf (3, "flags = %d\n", flags);
  if (!(flags & 7)) {
    send_error_bad_packet (u, msg, UDP_ERROR_CRC32C);
    vkprintf (1, "Expected some of bits 0/1/2: flags = 0x%08x\n", flags);
    udp_error_bad_flags ++;
    return 0;
  }
  
  int prefix_size = 0;

  if (flags & 1) { prefix_size += 28; }
  if (flags & 2) { prefix_size += 16; }
  if (flags & 4) { prefix_size += 8; }
  if (flags & 8) { prefix_size += 4; }
  //if (flags & 16) { prefix_size += 16; }
  //if (flags & 32) { prefix_size += 16; }

  if (TL_IN_REMAINING < prefix_size) {
    send_error_bad_packet (u, msg, UDP_ERROR_PARSE);
    vkprintf (1, "not enougth data\n");
    udp_error_parse ++;
    return 0;
  }

  char prefix_buf[100];
  tl_fetch_lookup_data (prefix_buf, prefix_size);

  struct udp_target *S = 0;

  struct process_id local_pid;
  struct process_id remote_pid;
  int generation = -1;
  memset (&local_pid, 0, sizeof (local_pid));
  memset (&remote_pid, 0, sizeof (remote_pid));
  long long hash = 0;
  if (flags & 1) {
    tl_fetch_raw_data (&remote_pid, 12);
    tl_fetch_raw_data (&local_pid, 12);
    generation = tl_fetch_int ();
  }
  if (flags & 2) {
    short T[6];
    tl_fetch_raw_data (T, 12);
    struct process_id l;
    struct process_id r;
    if (msg->our_ip_idx & 1) {
      send_error_wrong_pid (u, msg, &remote_pid, &local_pid);
      vkprintf (1, "Can not use short pid with ipv6 connection\n");
      udp_error_pid ++;
      return 0;
    }
    r.ip = *(int *)(msg->ipv6 + msg->our_ip_idx);
    r.port = msg->port;
    r.pid = T[0];
    r.utime = *(int *)(T + 1);
    l.ip = u->our_ipv4;
    l.port = u->our_port;
    l.pid = T[3];
    l.utime = *(int *)(T + 4);
    int _generation = tl_fetch_int ();
    if (flags & 1) {
      if (memcmp (&local_pid, &l, 12) || memcmp (&remote_pid, &r, 12) || generation != _generation) {
        send_error_bad_packet (u, msg, UDP_ERROR_INCONSISTENT_HEADER);
        vkprintf (1, "Pids in sections 0 and 1 differ\n");
        udp_error_pid ++;
        return 0;
      }
    } else {
      memcpy (&local_pid, &l, 12);
      memcpy (&remote_pid, &r, 12);
      generation = _generation;
    }
  }
  if (flags & 4) {
    hash = tl_fetch_long ();
    S = udp_target_get_by_hash (hash);
    if (!S) {
      send_error_obsolete_hash (u, msg, hash);
      vkprintf (1, "Can not find target with hash %lld\n", hash);
      udp_error_pid ++;
      return 0;
    }
    if (flags & 3) {
      if (memcmp (&local_pid, &PID, 12) || memcmp (&remote_pid, &S->PID, 12) || generation != S->generation) {
        send_error_bad_packet (u, msg, UDP_ERROR_INCONSISTENT_HEADER);
        vkprintf (1, "Pids in sections 0 and 1 differ from hash\n");
        udp_error_pid ++;
        return 0;
      }
    } else {
      remote_pid = S->PID;
      local_pid = PID;
      generation = S->generation;
    }
  }
  if (tl_fetch_error ()) {
    vkprintf (1, "%s\n", TL_ERROR);
    udp_error_parse ++;
    return 0;
  }

  int created = 0;
  if (!S) {
    if (!matches_pid (&PID, &local_pid)) {
      if (local_pid.ip != PID.ip || local_pid.port != PID.port) {
        send_error_wrong_pid (u, msg, &remote_pid, &local_pid);
        vkprintf (1, "Message is sent to " IP_PRINT_STR ":%d\n", IP_TO_PRINT (local_pid.ip), (int)local_pid.port);
      } else {
        send_error_obsolete_pid (u, msg, &remote_pid, &local_pid, generation);
        vkprintf (1, "Message is sent to " PID_PRINT_STR ":%d\n", PID_TO_PRINT (&local_pid), (int)local_pid.port);
      }
      udp_error_pid ++;
      return 0;
    }
    if (matches_pid (&PID, &local_pid) == 2) {
      S = udp_target_get_by_pid (&remote_pid);
      if (S && S->generation > generation) {
        send_error_obsolete_generation (u, msg, S, generation);
        udp_error_pid ++;
        return 0;
      }
      if (S && (matches_pid (&remote_pid, &S->PID) != 2 || S->generation != generation)) {
        S = 0;
      }
    }
    if (!S) {
      created = 2;
    }
    /*created = 1;
    S = udp_target_get_by_pid (&remote_pid);
    if (S && S->generation > generation) {
      send_error_obsolete_generation (u, msg, S, generation);
      udp_error_pid ++;
      return 0;
    }
    if (!S) {
      int w = 0;
      S = udp_target_create_by_pid_g (&remote_pid, generation, default_udp_target_methods, &w);
      assert (w);
      assert (S);
      created = 2;
    }*/
  }

  vkprintf (1, "Message from " PID_PRINT_STR " to " PID_PRINT_STR "\n", PID_TO_PRINT (&remote_pid), PID_TO_PRINT (&local_pid));
  vkprintf (1, "Out pid is " PID_PRINT_STR "\n", PID_TO_PRINT (&PID));
//  assert (S);
  struct vk_aes_ctx *ad = 0;
  int a_need_init = 0;
  if (flags & 128) {
//    if ((!created || (created == 1 && !memcmp (&S->PID, &remote_pid, 12) && !memcmp (&PID, &local_pid, 12) && S->generation == generation)) && S->aes_ctx_initialized) {
    if (S && S->aes_ctx_initialized) {
      ad = &S->aes_ctx_dec;
      a_need_init = !S->aes_ctx_initialized;
    } else {     
      static struct vk_aes_ctx _ad;
      ad = &_ad;
      memset (ad, 0, sizeof (_ad));
      a_need_init = 1;
    }
  }

  int v;
  if (flags & 8) {
    int crypto_init = tl_fetch_int ();
    if (UDP_FUNC(u)->check_perm) {
      v = UDP_FUNC(u)->check_perm (&remote_pid, crypto_init, msg);
    } else {
      v = default_udp_check_perm (&remote_pid, crypto_init, msg);
    }
    if (v < 0) {
      vkprintf (1, "Crypro error: %d\n", v);
      udp_error_crypto ++;
      return 0;
    }
    if (!(v & 3)) {
      vkprintf (1, "check_perm returned 0\n");
      udp_error_crypto ++;
      return 0;
    }
    assert (v & (3));
    assert (!(v & ~(3)));
  } else {
    if (!S) {
      vkprintf (1, "Flag 3 for new target is mandatory\n");
      udp_error_bad_flags ++;
      return 0;
    }
    v = S->flags & 3;
  }
  if (!(v & UDP_ALLOW_UNENC) && !(flags & 128)) {
    vkprintf (1, "Udp target supports only encrypted messages, but unencrypted message found\n");
    udp_error_crypto ++;
    return 0;
  }
  if (!(v & UDP_ALLOW_ENC) && (flags & 128)) {
    vkprintf (1, "Udp target supports only unencrypted messages, but encrypted message found\n");
    udp_error_crypto ++;
    send_error_unsupported_encr (u, msg, &remote_pid, generation, &local_pid);
    return 0;
  }

  if ((flags & 128) && !(flags & (16 | 32))) {
    vkprintf (1, "Message has encrypted part, but has not iv\n");
    udp_error_crypto ++;
    return 0;
  }
  if (flags & (16 | 32)) {
    if ((flags & (16 | 32)) != (16 | 32)) {
      vkprintf (1, "Should have both 5 and 6 sections (may be only for now\n");
      udp_error_crypto ++;
      return 0;
    }
  }

  unsigned char  key[32];
  int key_is_sha1 = 0;
  if (flags & (16 | 32)) {
    tl_fetch_raw_data (key, 32);
    key_is_sha1 = 1;
  }

  int encr_flags;
  static struct aes_key_data A;
  if ((flags & 128)) {
    if (a_need_init) { 
      aes_create_udp_keys (&A, &local_pid, &remote_pid, generation);
      vk_aes_set_decrypt_key (ad, A.read_key, 256); 
      a_need_init = 2;
    }
  
    if ((TL_IN_REMAINING & 0xf)) {
      vkprintf (1, "Size of encrypted data should be multiple of 16\n");
      udp_error_crypto ++;
      return 0;
    }
    if (decrypt_udp_message (TL_IN_RAW_MSG, ad,  key) < 0) {
      vkprintf (1, "Could not decrypt message\n");
      udp_error_crypto ++;
      return 0;
    }
    encr_flags = tl_fetch_int ();

    if ((encr_flags & 127) != (flags & 127)) {
      vkprintf (1, "Bits 0--6 in encrypted and unencrypted parts must be the same\n");
      udp_error_crypto ++;
      return 0;
    }

    if (TL_IN_REMAINING < prefix_size) {
      vkprintf (1, "Not enougth data to fetch prefix from encrypted part\n");
      udp_error_crypto ++;
      return 0;
    }


    static char b[100];
    rwm_fetch_data_back (TL_IN_RAW_MSG, b, prefix_size);

    if (memcmp (b, prefix_buf, prefix_size)) {
      vkprintf (1, "Prefix in encrypted and unencrypted parts differs\n");
      udp_error_crypto ++;
      return 0;
    }
    TL_IN_REMAINING -= prefix_size;
  } else {
    encr_flags = (flags & ~255);
  }

  if (encr_flags & 128) {
    vkprintf (1, "Error: encr flags can not contain bit 7\n");
    udp_error_bad_flags ++;
    return 0;
  }

  if ((!local_pid.pid || !local_pid.utime) && !(encr_flags & 512)) {
    vkprintf (1, "Unixtime is mandatory for packets with short process id\n");
    udp_error_bad_flags ++;
    return 0;
  }
  if (encr_flags & 512) {
    int x = tl_fetch_int ();
    if (x > now + 30 || x < now - 30) {
      vkprintf (1, "Packet is too old or too new (created_at = %d, now = %d)\n", x, now);
      udp_error_time ++;
      return 0;
    }
  }

  int proto_version = S ? S->proto_version : 1;
  if (!proto_version) { proto_version = 1; }
  int peer_proto_version = 0;

  if (encr_flags & 1024) {
    int x = tl_fetch_int ();
    peer_proto_version = x & 0xffff;
    proto_version = (x >> 16) & 0xffff;    
  }

  int zero_pad_size = 0;
  if (encr_flags & (1 << 28)) { zero_pad_size += 4; }
  if (encr_flags & (1 << 29)) { zero_pad_size += 8; }
  
  if (zero_pad_size) {
    if (TL_IN_REMAINING < zero_pad_size) {
      vkprintf (1, "Not enougth data to fetch zero padding\n");
      udp_error_crypto ++;
      return 0;
    }
    static char b[100];
    vkprintf (3, "fetching padding: pad_size = %d\n", zero_pad_size);
    assert (rwm_fetch_data_back (TL_IN_RAW_MSG, b, zero_pad_size) == zero_pad_size);
    int i;
    for (i = 0; i < zero_pad_size; i++) {
      if (b[i] != 0) {
        vkprintf (1, "padding with non-zeroes\n");
        udp_error_crypto ++;
        return 0;
      }
    }
    TL_IN_REMAINING -= zero_pad_size;
  }

  if (encr_flags & (1 << 8)) {
    tl_fetch_skip_string_data (16);
  }

  int ack_one = 0;
  int ack_prefix = 0;
  int ack_from = 0;
  int ack_to = 0;
  int ack_num = 0;
  static int ack[255];

  if (encr_flags & (1 << 12)) {
    ack_one = tl_fetch_int ();
  }

  if (encr_flags & (1 << 13)) {
    ack_prefix = tl_fetch_int ();
  }

  if (encr_flags & (1 << 14)) {
    ack_from = tl_fetch_int ();
    ack_to = tl_fetch_int ();
  }

  if (encr_flags & (1 << 15)) {
    ack_num = tl_fetch_int ();
    if (ack_num < 0 || ack_num > 255) {
      vkprintf (1, "maximal number of ack is 255, presented %d\n", ack_num);
      udp_error_parse ++;
      return 0;
    }
    tl_fetch_raw_data (ack, ack_num * 4);
  }

  int packet_id = 0;
  int packet_num = 0;

  if ((encr_flags & ((1 << 20) | (1 << 21))) == ((1 << 20) | (1 << 21))) {
    vkprintf (1, "Error: presented both bits 20 and 12\n");
    udp_error_bad_flags ++;
    return 0;
  }

  int next_parts = 0;
  int prev_parts = 0;

  if (encr_flags & (1 << 20)) {
    packet_id = tl_fetch_int ();
    packet_num = 1;
    if (packet_id < -1) {
      vkprintf (1, "Error: packet_id is negative: %d\n", packet_id);
      udp_error_parse ++;
      return 0;
    }
  }

  if (encr_flags & (1 << 21)) {
    packet_id = tl_fetch_int ();
    packet_num = tl_fetch_int ();
    if (packet_id < 0) {
      vkprintf (1, "Error: packet_id is negative: %d\n", packet_id);
      udp_error_parse ++;
      return 0;
    }
  }

  if (encr_flags & (1 << 22)) {
    prev_parts = tl_fetch_int ();
  }

  if (encr_flags & (1 << 23)) {
    next_parts = tl_fetch_int ();
  }

  if (encr_flags & (1 << 24)) {
    packet_id = tl_fetch_int ();
    int x = tl_fetch_int ();
    packet_num = x & 0xff;
    prev_parts = (x >> 8) & 0xfff;
    next_parts = (x >> 20) & 0xfff;
  }

  if ((encr_flags & ((1 << 26) | (1 << 27))) == ((1 << 26) | (1 << 27))) {
    vkprintf (1, "Error: presented both bits 26 and 17\n");
    udp_error_bad_flags ++;
    return 0;
  }

  if (packet_num != 1 && (encr_flags & (1 << 26))) {
    vkprintf (1, "Error: section 26 in multi-packet\n");
    udp_error_bad_flags ++;
    return 0;
  }
  
  if (packet_num && !(encr_flags & ((1 << 26) | (1 << 27)))) {
    vkprintf (1, "None of sections 26 and 27 is presented\n");
    udp_error_bad_flags ++;
    return 0;
  }

  if (tl_fetch_error ()) {
    vkprintf (1, "Error %s\n", TL_ERROR);
    udp_error_parse ++;
    return 0;
  }

  static struct raw_message msgs[256];
  
  vkprintf (2, "packet_num = %d, packet_id = %d, prev = %d, next = %d\n", packet_num, packet_id, prev_parts, next_parts);
  if (encr_flags & (1 << 26)) {
    assert (packet_num == 1);
    int len = TL_IN_REMAINING;
    assert (len >= 0);
    struct raw_message *r = &msgs[0];
    rwm_clone (r, TL_IN_RAW_MSG);
    rwm_trunc (r, len);
    assert (r->total_bytes == len);
    tl_fetch_skip_string_data (len);
  }

  if (encr_flags & (1 << 27)) {
    int i;
    for (i = 0; i < packet_num; i++) {
      int l = tl_fetch_int ();
      if (tl_fetch_error ()) {
        int j;
        for (j = 0; j  < i; j++) {
          rwm_free (&msgs[j]);
        }
        vkprintf (1, "Error %s\n", TL_ERROR);
        udp_error_parse ++;
        return 0;
      }
      if (l < 0 || l > TL_IN_REMAINING || (l & 3)) {
        int j;
        for (j = 0; j  < i; j++) {
          rwm_free (&msgs[j]);
        }
        vkprintf (1, "invalid packet len %d (remaining %d)\n", l, TL_IN_REMAINING);
        udp_error_parse ++;
        return 0;
      }
      assert (TL_IN_RAW_MSG->total_bytes == TL_IN_REMAINING);
      rwm_clone (&msgs[i], TL_IN_RAW_MSG);
      rwm_trunc (&msgs[i], l);
      assert (msgs[i].total_bytes == l);
      tl_fetch_skip_string_data (l);
      assert (msgs[i].total_bytes == l);
      { 
        int k;
        for (k = 0; k <= i; k++) {
          if (msgs[i].total_bytes < 0) {
            int j;
            fprintf (stderr, "packet_num = %d\n", packet_num);
            fprintf (stderr, "i = %d\n", i);
            for (j = 0; j <= i; j++) {
              fprintf (stderr, "%d bytes\n", msgs[j].total_bytes);
            }
            rwm_dump (&msgs[k]);
            rwm_dump_sizes (&msgs[k]);
          }
          assert (msgs[k].total_bytes >= 0);
        }
      }
    }
    if (TL_IN_REMAINING) {
      int j;
      for (j = 0; j < packet_num; j++) {
        rwm_free (&msgs[j]);
      }
      vkprintf (1, "extra data in the end of packet (%d bytes)\n", TL_IN_REMAINING);
      udp_error_parse ++;
      return 0;
    }
  }

  if (tl_fetch_error ()) {
    vkprintf (1, "Parse error: %s\n", TL_ERROR);
    int j;
    for (j = 0; j < packet_num; j++) {
      rwm_free (&msgs[j]);
    }
    udp_error_parse ++;
    return 0;
  }

  { 
    int i;
    for (i = 0; i < packet_num; i++) {
      if (msgs[i].total_bytes < 0) {
        int j;
        fprintf (stderr, "packet_num = %d\n", packet_num);
        for (j = 0; j < packet_num; j++) {
          fprintf (stderr, "%d bytes\n", msgs[j].total_bytes);
        }
        rwm_dump (&msgs[i]);
        rwm_dump_sizes (&msgs[i]);
      }
      assert (msgs[i].total_bytes >= 0);
    }
  }

  //
  //
  // Parse end 
  // No changes made up to this point
  //
  //
  
  if (peer_proto_version && S) {
    int t = peer_proto_version > UDP_PROTO_VERSION ? UDP_PROTO_VERSION : peer_proto_version;
    S->proto_version = t;
  }

  if (flags & 128) {
    udp_encrypted_packets_received ++;
  } else {
    udp_unencrypted_packets_received ++;
  }

  udp_target_delete_before (&remote_pid, remote_pid.utime);

  int i;
  if (packet_id >= 0) {
    if (!S) {
      S = udp_target_get_by_pid (&remote_pid);
      if (!S) {
        S = __udp_target_create (&remote_pid, generation, msg->ipv6, msg->port, 0);
        //      S = udp_target_create_by_pid_g (&remote_pid, generation, 0);
        assert (S);
      } else {
        int t = matches_pid (&remote_pid, &S->PID);
        if (!t) {
          udp_target_renew (S, &remote_pid, generation);
        } else {
          udp_target_update_pid (S, &remote_pid, generation);
        }
      }
      if (a_need_init == 2 && matches_pid (&local_pid, &PID) == 2) {
        vk_aes_set_encrypt_key (&S->aes_ctx_enc, A.write_key, 256); 
        vk_aes_set_decrypt_key (&S->aes_ctx_dec, A.read_key, 256); 
        S->aes_ctx_initialized = 1;
        vkprintf (2, "init_crypto: local_pid = " PID_PRINT_STR ", remote_pid = " PID_PRINT_STR "\n", PID_TO_PRINT (&local_pid), PID_TO_PRINT (&remote_pid));
      } else {
        S->aes_ctx_initialized = 0;
      }
    } else {
      if (a_need_init == 2 && matches_pid (&local_pid, &PID) == 2) {
        vk_aes_set_encrypt_key (&S->aes_ctx_enc, A.write_key, 256); 
        vk_aes_set_decrypt_key (&S->aes_ctx_dec, A.read_key, 256); 
        S->aes_ctx_initialized = 1;
        vkprintf (2, "init_crypto: local_pid = " PID_PRINT_STR ", remote_pid = " PID_PRINT_STR "\n", PID_TO_PRINT (&local_pid), PID_TO_PRINT (&remote_pid));
      }
    }

    if (encr_flags & (1 << 12)) {
      udp_target_ack (S, ack_one);
    }
    if (encr_flags & (1 << 13)) {
      udp_target_ack_prefix (S, ack_prefix);
    }
    if (encr_flags & (1 << 14)) {
      udp_target_ack_range (S, ack_from, ack_to);
    }
    if (encr_flags & (1 << 15)) {
      for (i = 0; i < ack_num; i++) {
        udp_target_ack (S, ack[i]);
      }
    }
  }

  if (!S) {
    S = udp_target_get_by_pid (&remote_pid);
    if (!S) {
      S = __udp_target_create (&remote_pid, generation, msg->ipv6, msg->port, 0);
      assert (S);
    } else {
      int t = matches_pid (&remote_pid, &S->PID);
      if (!t) {
        udp_target_renew (S, &remote_pid, generation);
      } else {
        udp_target_update_pid (S, &remote_pid, generation);
      }
    }
  }

  if (S) {
    S->state = udp_ok;
    S->rx_timeout = RX_TIMEOUT;
    S->tx_timeout = TX_TIMEOUT;
    if ((S->flags & 3) != v) {
      S->flags = (S->flags & ~3) | v;
    }
    if (!S->socket) {
      S->socket = u;
    }
  }


  for (i = 0; i < packet_num; i++) {
    struct raw_message *r = &msgs[i];
    struct udp_msg *M = 0;
    int p = (i > 0) ? 0 : prev_parts;
    int n = (i < packet_num - 1) ? 0 : next_parts;
    assert (r->total_bytes >= 0);
    udp_data_body_received += r->total_bytes;
    if (p || n) {
      M = msg_constructor_add (r, packet_id + i - p, p, p + n + 1, S);
    } else {
      M = msg_create (r, packet_id + i, S);
    }
    if (M) {
      vkprintf (2, "Delivering message\n");
      memcpy (M->ipv6, msg->ipv6, 16);
      M->port = msg->port;
      receive_message (u, S, M);
      udp_msg_free (M);
    }
  }

  return 0;
}

int udp_target_send (struct udp_target *S, struct raw_message *msg, int clone) {
  assert (S);
  assert (msg);
  if (!S->flush_timer.h_idx) {
    start_flush_timer (S);
  }

  struct raw_message r;
  if (clone) {
    rwm_clone (&r, msg);
  } else {
    r = *msg;
    memset (msg, 0, sizeof (*msg));
  }
  
  S->out_queue = queue_add_raw_message (S->out_queue, r);
  assert (S->out_queue);
  return 0;
}

static struct raw_message out_buf_msg;
static int out_buf_init;
static int out_buf_first_int;
static int out_buf_size;
static int out_buf_mode;
static int out_buf_prev;
static int out_buf_next;
static int out_buf_packets;
static int out_buf_start;
static struct udp_target *out_buf_target;
static int out_buf_bytes;
static int out_buf_prev_last;
static int out_buf_next_last;

int out_buf_clean (void) {
  return !out_buf_init && !out_buf_prev && !out_buf_next;
}

int out_buf_remaining (void) {
  return UDP_MAX_BODY_SIZE - out_buf_size;
}

void out_buf_set_mode (int mode) {
  out_buf_mode = mode;
}

void out_buf_set_prev (int prev) {
  assert (!out_buf_prev);
  out_buf_prev = prev;
  out_buf_prev_last = 1;
}

void out_buf_set_next (int next) {
  assert (!out_buf_next);
  out_buf_next = next;
  out_buf_next_last = 1;
}

void out_buf_set_target (struct udp_target *S) {
  out_buf_target = S;
}

void __out_buf_add (struct raw_message *raw, int num) {
  out_buf_bytes += raw->total_bytes;
  if (!out_buf_init) {
    out_buf_msg = *raw;
    //rwm_clone (&out_buf_msg, raw);
    out_buf_first_int = raw->total_bytes;
    out_buf_packets = 1; 
    out_buf_size = raw->total_bytes;
    out_buf_start = num;
    out_buf_init = 1;
    memset (raw, 0, sizeof (*raw));
  } else {
//    struct raw_message t;
//    rwm_clone (&t, raw);
    int x = raw->total_bytes;
    rwm_push_data (&out_buf_msg, &x, 4);
    rwm_union (&out_buf_msg, raw);

    out_buf_packets ++;
    out_buf_size += x + 4;
    assert ((out_buf_size) == out_buf_msg.total_bytes);
  }
}

void out_buf_add (struct raw_message *raw) {
  assert (raw->total_bytes > 0);
  struct udp_msg *msg = zmalloc (sizeof (*msg));
  struct udp_target *S = out_buf_target;
  msg->S = out_buf_target;
  msg->msg_num = S->send_num ++;
  rwm_clone (&msg->raw, raw);
  int prev = (out_buf_prev_last ? out_buf_prev : 0);
  int next = out_buf_next_last ? out_buf_next : 0;
  msg->prev_next = ((prev * 1ll) << 32) + next;
  out_buf_prev_last = 0;
  out_buf_next_last = 0;
  S->sent = tree_insert_udp_msg (S->sent, msg, lrand48 ());
  vkprintf (4, "S->sent: added number %d\n", msg->msg_num);
  if (S->send_num - 2 == S->max_confirmed) {
    if (!S->tx_timer.h_idx) {
      start_tx_timer (S);
    }
  }

  __out_buf_add (raw, S->send_num - 1);
}

int R[10000];
int Rpos;

void __dump_R (int x) {
  assert (Rpos < 10000);
  R[Rpos ++] = x;
}

void out_buf_flush (int force) {
  static int head[100];
  static int mid[100];
  static int tail[100];
  int head_pos = 0;
  int mid_pos = 0;
  int tail_pos = 0;

  head[head_pos ++] = TL_NET_UDP_PACKET_UNENC_HEADER;
  head[head_pos ++] = 0;

  struct udp_target *S = out_buf_target;
  int encr = get_crypto_key_id () != 0 && ((S->flags & UDP_ALLOW_ENC));
  int *flags = head + 1;

  if (out_buf_target->last_ack) {
    head[1] |= 4;
    *(long long *)(head + head_pos) = S->hash;
    head_pos += 2;
  } else {
    head[1] |= 1;
    struct process_id *pp = (void *)(head + head_pos);
    pp[0] = PID;
    pp[1] = S->PID;
    head_pos += 6;
    head[1] |= 8;
    head[head_pos ++] = S->generation;
    if (encr) {
      head[head_pos++] = UDP_CRYPTO_AES | 128 | ((get_crypto_key_id () & 0xfff) << 8);
    } else {
      head[head_pos++] = UDP_CRYPTO_NONE | 128;
    }
  }

  if (encr) {
    head[1] |= 128;
    flags = mid;
    *flags = 0;
    mid_pos ++;
  }

  if (!S->PID.pid || !S->PID.utime) {
    *flags |= 512;
    mid[mid_pos ++] = now;
  }

  if (!S->last_ack) {
    *flags |= 1024;
    mid[mid_pos ++] = (1 << 16) + (UDP_PROTO_VERSION);
  }

  if (S->confirm_tree) {
    if (S->confirm_timer.h_idx) {
      stop_confirm_timer (S);
    }
    Rpos = 0;
    tree_act_int (S->confirm_tree, __dump_R);
    int p = 0;
    if (R[0] <= S->received_prefix) {
      (*flags) |= (1 << 13);
      mid[mid_pos ++] = S->received_prefix;
      vkprintf (4, "Confirmed up to %d\n", R[0]);
    }
    while (p < Rpos && R[p] <= S->received_prefix) { p ++; }
    if (p < Rpos) {
      int o = p;
      while (p < Rpos && R[p] == R[p - 1] + 1) { p ++; }
      if (p - o > 1) {
        (*flags) |= (1 << 14);
        mid[mid_pos ++] = R[o];
        mid[mid_pos ++] = R[p - 1];
      } else {
        p = o;
      }
    }
    if (p < Rpos) {
      int x = Rpos - p;
      if (x > 50) { x = 50; }
      (*flags) |= (1 << 15);
      mid[mid_pos ++] = x;
      memcpy (mid + mid_pos, R + p, x * 4);
      mid_pos += x;
      p += x;
      int i;
      for (i = 0; i < Rpos; i++) {
        vkprintf (4, "Confirmed %d\n", R[p - x + i]);
      }
      
    }
    struct tree_int *L, *RR;
    tree_split_int (&L, &RR, S->confirm_tree, R[p - 1]);
    S->confirm_tree = RR;
    tree_clear_int (L);
    if (RR) {
      start_confirm_timer (S);
    }
    S->unack_size = tree_count_int (S->confirm_tree);
  }

  assert (out_buf_packets >= 0);
  if (out_buf_packets == 0) {
  } else  if (out_buf_packets == 1) {
    (*flags) |= (1 << 20);
    mid[mid_pos ++] = out_buf_start;
  } else {
    (*flags) |= (1 << 21);
    mid[mid_pos ++] = out_buf_start;
    mid[mid_pos ++] = out_buf_packets;
  }

  if (out_buf_prev) {
    (*flags) |= (1 << 22);
    mid[mid_pos ++] = out_buf_prev;
  }
  if (out_buf_next) {
    (*flags) |= (1 << 23);
    mid[mid_pos ++] = out_buf_next;
  }

  if (out_buf_packets == 0) {
  } else if (out_buf_packets == 1) {
    (*flags) |= (1 << 26);
  } else {
    (*flags) |= (1 << 27);
    mid[mid_pos ++] = out_buf_first_int;
  }

  int len = mid_pos + tail_pos + (out_buf_init ? (out_buf_msg.total_bytes >> 2) : 0);
  if (encr) {
    len += (head_pos - 2);
  }
  if (len & 1) {
    (*flags) |= (1 << 28);
    tail[tail_pos ++] = 0;
    len ++;
  }
  if (len & 2) {
    (*flags) |= (1 << 29);
    *(long long *)(tail + tail_pos) = 0;
    tail_pos += 2;
    len += 2;
  }

  if (encr) {
    memcpy (tail + tail_pos, head + 2, (head_pos - 2) * 4);
    tail_pos += (head_pos - 2);
  }
 

  struct vk_aes_ctx *enc = 0; 
  static unsigned char out[32];
  if (encr) {
    if (S->aes_ctx_initialized) {
      enc = &S->aes_ctx_enc;      
    } else {
      static struct vk_aes_ctx t;
      enc = &t;
      memset (enc, 0, sizeof (t));
      static struct aes_key_data A;
      aes_create_udp_keys (&A, &PID, &S->PID, S->generation);
      vk_aes_set_encrypt_key (enc, A.write_key, 256); 
    }
    head[1] |= 16;    
    head[1] |= 32;
    *flags |= (head[1] & 127);
  }
  if (!out_buf_init) {
    rwm_create (&out_buf_msg, 0, 0);
  }
  struct raw_message *z = &out_buf_msg;  
  assert (rwm_push_data_front (z, mid, mid_pos * 4) == mid_pos * 4);
  assert (rwm_push_data (z, tail, tail_pos * 4) == tail_pos * 4);

  if (encr) {
    int i;
#ifdef USE_SHA1
    assert (rwm_sha1 (&m, m.total_bytes, out) == m.total_bytes);
#else
    for (i = 0; i < 4; i++) {
      *(int *)(out + 4 * i) = lrand48 ();
    }
#endif
    memcpy (head + head_pos, out, 16);
    head_pos += 4;
    for (i = 0; i < 4; i++) {
      int t = lrand48 ();
      *(int *)(out + 16 + 4 * i) = t;
      head[head_pos ++] = t;
    }
    assert (z->total_bytes % 16 == 0);
    assert (rwm_encrypt_decrypt (z, z->total_bytes, enc, out) == z->total_bytes);
  }

  assert (rwm_push_data_front (z, head, head_pos * 4) == head_pos * 4);

  unsigned crc32c = rwm_crc32c (z, z->total_bytes);
  assert (rwm_push_data (z, &crc32c, 4) == 4);

  if (verbosity >= 3) {
    rwm_dump (z);
  }

  struct udp_message *a = malloc (sizeof (*a));
  a->raw = *z;
  a->next = 0;
  memcpy (a->ipv6, S->ipv6, 16);
  a->port = S->port;
  a->our_ip_idx = 0;

  if (force || !(S->flags & UDP_WAIT)) {
    udp_packets_sent ++;
    udp_data_sent += a->raw.total_bytes;
    udp_data_body_sent += out_buf_bytes;
    if (udp_drop_probability && drand48 () < udp_drop_probability) {
      //vkprintf (0, "Dropped\n");
      rwm_free (&a->raw);
      free (a);    
    } else {
      udp_queue_message (S->socket ? S->socket : default_udp_socket, a);
    }
    if (!force) {
      S->window_size += out_buf_bytes;
      if (S->window_size >= STOP_WINDOW_SIZE) {
        //vkprintf (0, "WINDOW START\n");
        S->flags |= UDP_WAIT;
      }
    }
  } else {
    struct tmp_msg x = {
      .msg = a,
      .value_bytes = out_buf_bytes
    };
    S->window_queue = queue_add_tmp_msg (S->window_queue, x);
  }

  if (encr) {
    udp_encrypted_packets_sent ++;
  } else {
    udp_unencrypted_packets_sent ++;
  }


  out_buf_init = 0;
  out_buf_size = 0;
  out_buf_prev = 0;
  out_buf_next = 0;
  out_buf_packets = 0;
  out_buf_bytes = 0;
}

void out_buf_send_error (struct process_id *local_pid, struct process_id *remote_pid, int generation, unsigned char ipv6[16], int port, struct raw_message *raw) {
  static int head[100];
  static int mid[100];
  static int tail[100];
  int head_pos = 0;
  int mid_pos = 0;
  int tail_pos = 0;

  head[head_pos ++] = TL_NET_UDP_PACKET_UNENC_HEADER;
  head[head_pos ++] = 0;

  int encr = get_crypto_key_id () != 0;
  int *flags = head + 1;

  head[1] |= 1;
  struct process_id *pp = (void *)(head + head_pos);
  if (local_pid) {
    pp[0] = *local_pid;
  } else {
    memset (&pp[0], 0, sizeof (pp[0]));
  }
  if (remote_pid) {
    pp[1] = *remote_pid;
  } else {
    memset (&pp[1], 0, sizeof (pp[0]));
  }
  head_pos += 6;
  head[1] |= 8;
  head[head_pos ++] = generation;
  if (get_crypto_key_id ()) {
    head[head_pos++] = UDP_CRYPTO_AES | 128 | ((get_crypto_key_id () & 0xfff) << 8);
  } else {
    head[head_pos++] = UDP_CRYPTO_NONE | 128;
  }

  if (encr) {
    head[1] |= 128;
    flags = mid;
    *flags = 0;
    mid_pos ++;
  }
  
  (*flags) |= 512;
  mid[mid_pos ++] = now;

  (*flags) |= (1 << 20);
  mid[mid_pos ++] = -1;

  (*flags) |= (1 << 26);

  int len = mid_pos + tail_pos + (raw->total_bytes >> 2);
  if (encr) {
    len += (head_pos - 2);
  }
  if (len & 1) {
    (*flags) |= (1 << 28);
    tail[tail_pos ++] = 0;
    len ++;
  }
  if (len & 2) {
    (*flags) |= (1 << 29);
    *(long long *)(tail + tail_pos) = 0;
    tail_pos += 2;
    len += 2;
  }

  if (encr) {
    memcpy (tail + tail_pos, head + 2, (head_pos - 2) * 4);
    tail_pos += (head_pos - 2);
  }
 

  struct vk_aes_ctx *enc = 0; 
  static unsigned char out[32];
  if (encr) {
    static struct vk_aes_ctx t;
    enc = &t;
    memset (enc, 0, sizeof (t));
    static struct aes_key_data A;
    aes_create_udp_keys (&A, &pp[0], &pp[1], generation);
    vk_aes_set_encrypt_key (enc, A.write_key, 256); 
    head[1] |= 16;    
    head[1] |= 32;
    *flags |= (head[1] & 127);
  }
  struct raw_message *z = raw;
  assert (rwm_push_data_front (z, mid, mid_pos * 4) == mid_pos * 4);
  assert (rwm_push_data (z, tail, tail_pos * 4) == tail_pos * 4);

  if (encr) {
    int i;
#ifdef USE_SHA1
    assert (rwm_sha1 (&m, m.total_bytes, out) == m.total_bytes);
#else
    for (i = 0; i < 4; i++) {
      *(int *)(out + 4 * i) = lrand48 ();
    }
#endif
    memcpy (head + head_pos, out, 16);
    head_pos += 4;
    for (i = 0; i < 4; i++) {
      int t = lrand48 ();
      *(int *)(out + 16 + 4 * i) = t;
      head[head_pos ++] = t;
    }
    assert (z->total_bytes % 16 == 0);
    assert (rwm_encrypt_decrypt (z, z->total_bytes, enc, out) == z->total_bytes);
  }

  assert (rwm_push_data_front (z, head, head_pos * 4) == head_pos * 4);

  unsigned crc32c = rwm_crc32c (z, z->total_bytes);
  assert (rwm_push_data (z, &crc32c, 4) == 4);

  if (verbosity >= 3) {
    rwm_dump (z);
  }

  struct udp_message *a = malloc (sizeof (*a));
  a->raw = *z;
  a->next = 0;
  memcpy (a->ipv6, ipv6, 16);
  a->port = port;
  a->our_ip_idx = 0;

  if (udp_drop_probability && drand48 () < udp_drop_probability) {
    //vkprintf (0, "Dropped\n");
    rwm_free (&a->raw);
    free (a);    
  } else {
    udp_queue_message (default_udp_socket, a);
  }
  
  udp_packets_sent ++;
  udp_data_sent += a->raw.total_bytes;
  udp_data_body_sent += raw->total_bytes;
}

int udp_target_flush (struct udp_target *S) {
  assert (out_buf_clean ());
  assert (S);
  int cc = 0;

  out_buf_set_mode (S->flags);
  out_buf_set_target (S);


  while (S->out_queue) {    
    struct raw_message r = queue_get_first_raw_message (S->out_queue)->x;
    S->out_queue = queue_del_first_raw_message (S->out_queue);

    if (r.total_bytes <= out_buf_remaining ()) {
      out_buf_add (&r);
    } else {
      if (out_buf_remaining () < 512) {
        out_buf_flush (0);
        cc ++;
      }
      if (r.total_bytes <= out_buf_remaining ()) {
        out_buf_add (&r);
      } else {
        int x = (r.total_bytes - out_buf_remaining () + UDP_MAX_BODY_SIZE - 1) / UDP_MAX_BODY_SIZE + 1;
        int i;
        for (i = 0; i < x; i++) {
          if (i != 0) {
            out_buf_set_prev (i);
          }
          if (i != x - 1) {
            out_buf_set_next (x - i - 1);
          }
          int y = out_buf_remaining ();
          assert (i == 0 || y == UDP_MAX_BODY_SIZE);
          if (y >= r.total_bytes) {
            assert (i == x - 1);
            out_buf_add (&r);
          } else {
            assert (i < x - 1);
            struct raw_message t;
            rwm_split_head (&t, &r, y);
            out_buf_add (&t);
            out_buf_flush (0);
            cc ++;
          }
        }
      }
    }
  }
  if (!out_buf_clean ()) {
    out_buf_flush (0);
    cc ++;
  }
  assert (out_buf_clean ());
  if (S->flush_timer.h_idx) {
    stop_flush_timer (S);
  }
  return cc;
}

void resend_range (struct udp_target *S, int from, int to, int all) {
  vkprintf (2, "Ans: all from %d to %d\n", from, to);
  vkprintf (1, "S->PID = " PID_PRINT_STR "\n", PID_TO_PRINT (&S->PID));
  out_buf_set_mode (S->flags);
  out_buf_set_target (S);
  assert (out_buf_clean ());
  int i;  
  for (i = from; i <= to; i++) {
//    tree_check_udp_msg (S->sent);
    struct udp_msg **M = tree_lookup_value_udp_msg (S->sent, (void *)&i);
    if (!M) {
      vkprintf (1, "No Message with number %d\n", i);
      if (!out_buf_clean ()) {
        out_buf_flush (1);
        if (!all) { return; }
      }
      continue; 
    }
    struct udp_msg *msg = *M;
    int prev = msg->prev_next >> 32;
    int next = msg->prev_next;
    if (out_buf_size + msg->raw.total_bytes > UDP_MAX_BODY_SIZE) {
      out_buf_flush (1);
      if (!all) { return; }
    }
    if (prev && !out_buf_clean ()) {
      out_buf_flush (1);
      if (!all) { return; }
    }
    if (prev) {
      out_buf_set_prev (prev);
    }
    if (next) {
      out_buf_set_next (next);
    }
    struct raw_message t;
    rwm_clone (&t, &msg->raw);
    __out_buf_add (&t, msg->msg_num);
    if (next) {
      out_buf_flush (1);
      if (!all) { return; }
    }
  }
  if (out_buf_size) {
    out_buf_flush (1);
  }
  assert (out_buf_clean ());
}

int work_resend_request (struct udp_msg *msg) {
  vkprintf (2, "work_resend_request: len = %d\n", msg->raw.total_bytes);
  if (msg->raw.total_bytes != 12) { return 0; }
  int P[3];
  assert (rwm_fetch_data (&msg->raw, P, 12) == 12);
  assert (P[0] == TL_UDP_RESEND_REQUEST);
  //vkprintf (0, "resend request: P[1] = %d, P[2] = %d\n", P[1], P[2]);
  if (P[1] > P[2]) { return 0; }
  resend_range (msg->S, P[1], P[2], 1);
  return 0;
}

int work_resend_request_ext (struct udp_msg *msg) {
  vkprintf (2, "work_resend_request: len = %d\n", msg->raw.total_bytes);
  if (msg->raw.total_bytes > 102 * 4) { return 0; }
  int len =  msg->raw.total_bytes;
  static int P[102];
  assert (rwm_fetch_data (&msg->raw, P, len) == len);  
  if ((P[1] + 1) * 8 != len) { return 0; }
  assert (P[0] == TL_UDP_RESEND_REQUEST_EXT);
  int i;
  //vkprintf (0, "resend request: num = %d\n", P[1]);
  for (i = 0; i < P[1]; i++) {
    if (P[2 + 2 * i] <= P[2 + 2 * i + 1]) {
      resend_range (msg->S, P[2 + 2 * i], P[2 + 2 * i + 1], 1);
    }
  }
  return 0;
}

void work_obsolete_pid (struct udp_msg *msg) {
  if (msg->raw.total_bytes != 32) {
    return;
  }
  struct process_id P1;
  struct process_id P2;
  int generation;
  struct raw_message *raw = &msg->raw;
  
  rwm_fetch_data (raw, 0, 4); 
  rwm_fetch_data (raw, &P1, 12);
  rwm_fetch_data (raw, &P2, 12);
  rwm_fetch_data (raw, &generation, 4);

  udp_target_delete_by_pid (&P1);
  
  __udp_target_create (&P2, generation, msg->ipv6, msg->port, 0);
}

void work_obsolete_hash (struct udp_msg *msg) {
  if (msg->raw.total_bytes != 24) {
    return;
  }
  long long hash;
  struct raw_message *raw = &msg->raw;

  
  rwm_fetch_data (raw, 0, 4); 
  rwm_fetch_data (raw, &hash, 8);
  struct process_id real_pid;
  rwm_fetch_data (raw, &real_pid, 12);


  struct udp_target *S = udp_target_get_by_hash (hash);
  udp_target_create (&real_pid, S ? (unsigned char *)S->ipv6 : msg->ipv6, S ? S->port : msg->port, 0);
  if (!S) { return; }
  udp_target_delete_by_pid (&S->PID);
}

void work_obsolete_generation (struct udp_msg *msg) {
  if (msg->raw.total_bytes != 20) {
    return;    
  }
  struct process_id P;
  int generation;
  struct raw_message *raw = &msg->raw;
  
  rwm_fetch_data (raw, 0, 4); 
  rwm_fetch_data (raw, &P, 12);
  rwm_fetch_data (raw, &generation, 4);

  struct udp_target *S = udp_target_get_by_pid (&P);
  if (S) {
    udp_target_set_generation (S, generation);
  }
}

void udp_confirms_send (struct udp_target *S) {
  out_buf_set_mode (S->flags);
  out_buf_set_target (S);
  assert (out_buf_clean ());
  while (S->confirm_tree) {
    out_buf_flush (1);
    assert (out_buf_clean ());
  }
}

void udp_target_restart_send (struct udp_target *S) {
  //vkprintf (0, "WINDOW STOP\n");
  S->flags &= ~UDP_WAIT;
  while (S->window_queue && !(S->flags & UDP_WAIT)) {
    struct tmp_msg x = queue_get_first_tmp_msg (S->window_queue)->x;
    S->window_queue = queue_del_first_tmp_msg (S->window_queue);
    struct udp_message *a = x.msg;
    if (udp_drop_probability && drand48 () < udp_drop_probability) {
      //vkprintf (0, "Dropped\n");
      rwm_free (&a->raw);
      free (a);    
    } else {
      udp_queue_message (S->socket, a);
    }
    udp_packets_sent ++;
    udp_data_sent += a->raw.total_bytes;
    udp_data_body_sent += x.value_bytes;
    S->window_size += x.value_bytes;;
    if (S->window_size >= STOP_WINDOW_SIZE) {
      S->flags |= UDP_WAIT;
      //vkprintf (0, "WINDOW START\n");
    }    
  }
}

void send_error_obsolete_pid (struct udp_socket *u, struct udp_message *msg, struct process_id *remote_pid, struct process_id *local_pid, int generation) {
  static int z[10];
  z[0] = TL_UDP_OBSOLETE_PID;
  *(struct process_id *)(z + 1) = *local_pid;
  *(struct process_id *)(z + 4) = *local_pid;  
  z[7] = generation;

  struct raw_message raw;
  assert (rwm_create (&raw, z, 32) == 32);

  out_buf_send_error (&PID, remote_pid, generation, msg->ipv6, msg->port, &raw);
}

void send_error_obsolete_hash (struct udp_socket *u, struct udp_message *msg, long long hash) {
  static int z[10];
  z[0] = TL_UDP_OBSOLETE_HASH;
  *(long long *)(z + 1) = hash;
  *(struct process_id *)(z + 3) = PID;
  
  struct raw_message raw;
  assert (rwm_create (&raw, z, 24) == 24);

  out_buf_send_error (&PID, 0, 0, msg->ipv6, msg->port, &raw);
}

void send_error_obsolete_generation (struct udp_socket *u, struct udp_message *msg, struct udp_target *S, int generation) {
  static int z[10];
  z[0] = TL_UDP_OBSOLETE_GENERATION;
  *(struct process_id *)(z + 1) = PID;
  z[4] = generation;
  
  struct raw_message raw;
  assert (rwm_create (&raw, z, 20) == 20);

  out_buf_send_error (&PID, &S->PID, generation, msg->ipv6, msg->port, &raw);
}

void send_error_wrong_pid (struct udp_socket *u, struct udp_message *msg, struct process_id *remote_pid, struct process_id *pid) {
}

void send_error_bad_packet (struct udp_socket *u, struct udp_message *msg, int error_code) {
}

void send_error_unsupported_encr (struct udp_socket *u, struct udp_message *msg, struct process_id *remote_pid, int generation, struct process_id *local_pid) {
  static int z[10];
  z[0] = TL_UDP_DISABLE_ENCRYPTION;
  
  struct raw_message raw;
  assert (rwm_create (&raw, z, 20) == 20);

  out_buf_send_error (&PID, remote_pid, generation, msg->ipv6, msg->port, &raw);
}
