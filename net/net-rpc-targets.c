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

#include "net-rpc-targets.h"
#include "vv-tree.h"
#include "net-rpc-common.h"
#include "net-rpc-server.h"
#include <memory.h>
#include <assert.h>
#include "kdb-data-common.h"
#include <stdlib.h>
#include <unistd.h>
#include <stdio.h>

#define IP_PRINT_STR "%d.%d.%d.%d"
#define IP_TO_PRINT(x) ((x) >> 24) & 0xff, ((x) >> 16) & 0xff, ((x) >> 8) & 0xff, ((x) >> 0) & 0xff

#define rpc_target_cmp(a,b) ((a)->PID.port ? memcmp (&(a)->PID, &(b)->PID, 6) : memcmp (&(a)->PID, &(b)->PID, sizeof (struct process_id))) 

DEFINE_TREE_STD_ALLOC(rpc_target, struct rpc_target *, rpc_target_cmp, int, std_int_compare)
struct tree_rpc_target *rpc_target_tree;
unsigned rpc_targets_cur_host;

struct rpc_target *rpc_target_alloc (struct process_id PID) {
  struct rpc_target *S = zmalloc (sizeof (*S));
  S->PID = PID;
  S->target = 0;
  S->first = S->last = (struct connection *)S;
  S->inbound_num = 0;
  rpc_target_tree = tree_insert_rpc_target (rpc_target_tree, S, lrand48 ());
  return S;
}

void rpc_target_free (struct rpc_target *S) {
  rpc_target_tree = tree_delete_rpc_target (rpc_target_tree, S);
  zfree (S, sizeof (*S));
}


struct rpc_target *Sarr[10000];
int SarrPos;

void __rpc_target_set_host (struct rpc_target *S) {
  if (!S->PID.ip) {
    assert (SarrPos < 10000);
    Sarr[SarrPos ++] = S;
  }
}

static void st_update_host (void) {
  if (!PID.ip || PID.ip == rpc_targets_cur_host) {
    return;
  }
  if (rpc_targets_cur_host) {
    fprintf (stderr, "Changing ip during work: ip = " IP_PRINT_STR ", new_ip = " IP_PRINT_STR "\n", IP_TO_PRINT (rpc_targets_cur_host), IP_TO_PRINT (PID.ip));
    assert (!rpc_targets_cur_host);
  }
  assert (PID.ip);
  rpc_targets_cur_host = PID.ip;
  tree_act_rpc_target (rpc_target_tree, __rpc_target_set_host);
  int i;
  for (i = 0; i < SarrPos; i++) {
    struct rpc_target *S = Sarr[i];
    rpc_target_tree = tree_delete_rpc_target (rpc_target_tree, S);
    S->PID.ip = rpc_targets_cur_host;
    rpc_target_tree = tree_insert_rpc_target (rpc_target_tree, S, lrand48 ());
  }
}

void rpc_target_insert_conn (struct connection *c) {
  if (c->target) { return; }
  st_update_host ();
  static struct rpc_target t;
  t.PID = RPCS_DATA(c)->remote_pid;
  if (rpc_targets_cur_host && !t.PID.ip) {
    t.PID.ip = rpc_targets_cur_host;
  }
  struct tree_rpc_target *T = tree_lookup_rpc_target (rpc_target_tree, &t);
  struct rpc_target *S = T ? T->x : 0;
  if (!S) {
    S = rpc_target_alloc (t.PID);
  }
  struct connection *d = S->first;
  while (d != (struct connection *)S) {
    if (d == c) {
      return;
    }
    d = d->next;
  }
  assert (!c->next);
  assert (!c->prev);
  c->next = (void *)S;
  c->prev = S->last;
  c->next->prev = c;
  c->prev->next = c;
  S->inbound_num ++;
}

void rpc_target_delete_conn (struct connection *c) {
  st_update_host ();
  if (!c->next) { return; }
  assert (c->next);
  assert (c->prev);
  c->next->prev = c->prev;
  c->prev->next = c->next;
  c->next = c->prev = 0;
  static struct rpc_target t;
  t.PID = RPCS_DATA(c)->remote_pid;
  if (rpc_targets_cur_host && !t.PID.ip) {
    t.PID.ip = rpc_targets_cur_host;
  }
  struct tree_rpc_target *T = tree_lookup_rpc_target (rpc_target_tree, &t);
  struct rpc_target *S = T ? T->x : 0;
  assert (S);
  assert ((S->inbound_num --) > 0);;
  if (!S->inbound_num && !S->target) {
    rpc_target_free (S);
  }
}

void rpc_target_insert_target_ext (struct conn_target *targ, unsigned ip) {
  st_update_host ();
  static struct rpc_target t;
  t.PID.ip = ip;
  if (!t.PID.ip) { return; }
  if (t.PID.ip == 0x7f000001) {
    t.PID.ip = 0;
  }
  if (rpc_targets_cur_host && !t.PID.ip) {
    t.PID.ip = rpc_targets_cur_host;
  }
  t.PID.port = targ->port;
  struct tree_rpc_target *T = tree_lookup_rpc_target (rpc_target_tree, &t);
  struct rpc_target *S = T ? T->x : 0;
  if (!S) {
    S = rpc_target_alloc (t.PID);
  }
  assert (!S->target || S->target == targ);
  S->target = targ;
}

void rpc_target_insert_target (struct conn_target *targ) {
  rpc_target_insert_target_ext (targ, ntohl (targ->target.s_addr));
/*  st_update_host ();
  static struct rpc_target t;
  t.PID.ip = ntohl (targ->target.s_addr);
  if (!t.PID.ip) { return; }
  if (t.PID.ip == 0x7f000001) {
    t.PID.ip = 0;
  }
  if (rpc_targets_cur_host && !t.PID.ip) {
    t.PID.ip = rpc_targets_cur_host;
  }
  t.PID.port = targ->port;
  struct tree_rpc_target *T = tree_lookup_rpc_target (rpc_target_tree, &t);
  struct rpc_target *S = T ? T->x : 0;
  if (!S) {
    S = rpc_target_alloc (t.PID);
  }
  assert (!S->target || S->target == targ);
  S->target = targ;*/
}

struct rpc_target *rpc_target_lookup (struct process_id *PID) {
  assert (PID);
  st_update_host ();
  static struct rpc_target t;
  t.PID = *PID;
  if (rpc_targets_cur_host && !t.PID.ip) {
    t.PID.ip = rpc_targets_cur_host;
  }
  struct tree_rpc_target *T = tree_lookup_rpc_target (rpc_target_tree, &t);
  return T ? T->x : 0;
}

struct rpc_target *rpc_target_lookup_hp (unsigned ip, int port) {
  st_update_host ();
  static struct rpc_target t;
  t.PID.ip = ip;
  if (t.PID.ip == 0x7f000001) {
    t.PID.ip = 0;
  }
  if (rpc_targets_cur_host && !t.PID.ip) {
    t.PID.ip = rpc_targets_cur_host;
  }
  t.PID.port = port;
  struct tree_rpc_target *T = tree_lookup_rpc_target (rpc_target_tree, &t);
  return T ? T->x : 0;
}

struct rpc_target *rpc_target_lookup_target (struct conn_target *targ) {
  if (targ->custom_field == -1) {
    return 0;
  }
  st_update_host ();
  static struct rpc_target t;
  t.PID.ip = targ->custom_field; //ntohl (targ->target.s_addr);
  if (t.PID.ip == 0x7f000001) {
    t.PID.ip = 0;
  }
  if (rpc_targets_cur_host && !t.PID.ip) {
    t.PID.ip = rpc_targets_cur_host;
  }
  t.PID.port = targ->port;
  struct tree_rpc_target *T = tree_lookup_rpc_target (rpc_target_tree, &t);
  return T ? T->x : 0;
}

struct connection *rpc_target_choose_connection (struct rpc_target *S, struct process_id *PID) {
  if (!S) {
    return 0;
  }
  struct connection *c, *d = 0;
  int r, u = 10000;
  if (S->target) {
    for (c = S->target->first_conn; c != (struct connection *) (S->target); c = c->next) {
      r = server_check_ready (c);
      if (r == cr_ok) {
        if (!PID || matches_pid (&RPCS_DATA(c)->remote_pid, PID) >= 1) {
          return c;
        }
      } else if (r == cr_stopped && c->unreliability < u) {
        if (!PID || matches_pid (&RPCS_DATA(c)->remote_pid, PID) >= 1) {
          u = c->unreliability;
          d = c;
        }
      }
    }
  }
  for (c = S->first; c != (struct connection *) (S); c = c->next) {
    r = server_check_ready (c);
    if (r == cr_ok) {
      if (!PID || matches_pid (&RPCS_DATA(c)->remote_pid, PID) >= 1) {
        return c;
      }
    } else if (r == cr_stopped && c->unreliability < u) {
      if (!PID || matches_pid (&RPCS_DATA(c)->remote_pid, PID) >= 1) {
        u = c->unreliability;
        d = c;
      }
    }
  }
  return d;
}

int rpc_target_choose_random_connections (struct rpc_target *S, struct process_id *PID, int limit, struct connection *buf[]) {
  if (!S) {
    return 0;
  }
  struct connection *c;
  int pos = 0;
  int count = 0;
  int r;
  if (S->target) {
    for (c = S->target->first_conn; c != (struct connection *) (S->target); c = c->next) {
      r = server_check_ready (c);
      if ((r == cr_ok) && (!PID || matches_pid (&RPCS_DATA(c)->remote_pid, PID) >= 1)) {
        if (pos < limit) {
          buf[pos ++] = c;
        } else {
          int t = lrand48 () % (count + 1);
          if (t < limit) {
            buf[t] = c;
          }
        }
        count ++;
      }
    }
  }
  for (c = S->first; c != (struct connection *) (S); c = c->next) {
    r = server_check_ready (c);
    if ((r == cr_ok) && (!PID || matches_pid (&RPCS_DATA(c)->remote_pid, PID) >= 1)) {
      if (pos < limit) {
        buf[pos ++] = c;
      } else {
        int t = lrand48 () % (count + 1);
        if (t < limit) {
          buf[t] = c;
        }
      }
      count ++;
    }
  }
  return pos;
}

int rpc_target_get_state (struct rpc_target *S, struct process_id *PID) {
  if (!S) {
    return 0;
  }
  int best = -1;
  int u = 10000;
  int r;
  struct connection *c;
  if (S->target) {
    for (c = S->target->first_conn; c != (struct connection *) (S->target); c = c->next) {
      r = server_check_ready (c);
      if (r == cr_ok) {
        if (!PID || matches_pid (&RPCS_DATA(c)->remote_pid, PID) >= 1) {
          return 1;
        }
      } else if (r == cr_stopped && c->unreliability < u) {
        if (!PID || matches_pid (&RPCS_DATA(c)->remote_pid, PID) >= 1) {
          best = 0;
        }
      }
    }
  }
  for (c = S->first; c != (struct connection *) (S); c = c->next) {
    r = server_check_ready (c);
    if (r == cr_ok) {
      if (!PID || matches_pid (&RPCS_DATA(c)->remote_pid, PID) >= 1) {
        return 1;
      }
    } else if (r == cr_stopped && c->unreliability < u) {
      if (!PID || matches_pid (&RPCS_DATA(c)->remote_pid, PID) >= 1) {
        best = 0;
      }
    }
  }
  return best;
}
