/*
    This file is part of VK/KittenPHP-DB-Engine.

    VK/KittenPHP-DB-Engine is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 2 of the License, or
    (at your option) any later version.

    VK/KittenPHP-DB-Engine is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with VK/KittenPHP-DB-Engine.  If not, see <http://www.gnu.org/licenses/>.

    This program is released under the GPL with the additional exemption 
    that compiling, linking, and/or using OpenSSL is allowed.
    You are free to remove this exemption from derived works.

    Copyright 2013 Vkontakte Ltd
              2013 Anton Maydell
*/

#define	_FILE_OFFSET_BITS	64

#include <stdlib.h>
#include <string.h>
#include <netdb.h>

#include "targ-data.h"
#include "kdb-data-common.h"
#include "net-rpc-client.h"

#include "am-amortization.h"
#include "vv/vv-tl-parse.h"
#include "TL/constants.h"

int targ_weights_rpcc_execute (struct connection *c, int op, int len);
int rpcc_ready (struct connection *c);
struct rpc_client_functions weights_rpc_client = {
  .execute = targ_weights_rpcc_execute,
  .check_ready = server_check_ready,
  .flush_packet = rpcc_flush_packet,
  .rpc_check_perm = rpcc_default_check_perm,
  .rpc_init_crypto = rpcc_init_crypto,
  .rpc_start_crypto = rpcc_start_crypto,
  .rpc_ready = rpcc_ready
};

const int weights_coords = 32;
int targ_weights_last_update_time;
const int updates_limit = 100, small_updates_limit = 1000;
long long tot_weights_vector_bytes, weights_small_updates, weights_updates;
int tot_weights_vectors;
static time_amortization_table_t **TAT;

static inline int get_vector_size (void) {
  return sizeof (targ_weights_vector_t) + sizeof (double) * weights_coords;
}

targ_weights_vector_t *targ_weights_vector_alloc (void) {
  int sz = get_vector_size ();
  tot_weights_vectors++;
  tot_weights_vector_bytes += sz;
  return zmalloc0 (sz);
}

void targ_weights_vector_free (targ_weights_vector_t *V) {
  int sz = get_vector_size ();
  tot_weights_vectors--;
  tot_weights_vector_bytes -= sz;
  zfree (V, sz);
}

int targ_weights_create_target (const char *address) {
  vkprintf (3, "%s: address: '%s'\n", __func__, address);
  char *colon = strchr (address, ':');
  if (colon == NULL) {
    kprintf ("%s: address doesn't contain color ('%s').\n", __func__, address);
    return -1;
  }
  char *p;
  errno = 0;
  int port = strtol (colon + 1, &p, 10);
  if (errno) {
    kprintf ("%s: fail to parse port in address ('%s'). %m\n", __func__, address);
    return -1;
  }
  *colon = 0;
  struct hostent *h = gethostbyname (address);
  *colon = ':';
  if (!h || h->h_addrtype != AF_INET || h->h_length != 4 || !h->h_addr_list || !h->h_addr) {
    kprintf ("%s: cannot resolve %s\n", __func__, address);
    return -1;
  }
  static struct conn_target ct;
  ct.min_connections = ct.max_connections = 1;
  ct.type = &ct_rpc_client;
  ct.extra = (void *) &weights_rpc_client;
  ct.reconnect_timeout = 1;
  ct.target = *((struct in_addr *) h->h_addr);
  ct.port = port;
  create_target (&ct, 0);
  return 0;
}

static long long next_qid (void) {
  static long long qid = 0;
  while (!qid) {
    qid = (((long long) lrand48 ()) << 31) | lrand48 ();
  }
  qid++;
  if (qid < 0) {
    qid = 1;
  }
  return qid;
}

/* RPC client functions */
static long long subscription_qid;

int targ_weights_receive_half_lifes (struct connection *c, int len) {
  vkprintf (3, "%s: connection %d (%s:%d), len: %d\n", __func__, c->fd, show_remote_ip (c), c->remote_port, len);
  nb_iterator_t R;
  nbit_set (&R, &c->In);
  tl_fetch_init_iterator (&R, len - 4);
  struct tl_query_header header;
  if (tl_fetch_query_answer_header (&header) < 0) {
    return SKIP_ALL_BYTES;
  }
  if (header.qid != subscription_qid) {
    return SKIP_ALL_BYTES;
  }
  const int _ = tl_fetch_int ();
  if (_ != TL_MAYBE_TRUE) {
    return SKIP_ALL_BYTES;
  }
  if (tl_fetch_int () != weights_coords) {
    return SKIP_ALL_BYTES;
  }
  int *a = alloca (weights_coords * 4);
  tl_fetch_raw_data (a, 4 * weights_coords);
  if (tl_fetch_error ()) {
    return SKIP_ALL_BYTES;
  }
  if (tl_fetch_end () < 0) {
    return SKIP_ALL_BYTES;
  }
  if (TAT == NULL) {
    TAT = zmalloc0 (sizeof (TAT[0]) * weights_coords);
  }
  int i, j;
  for (i = 0; i < weights_coords; i++) {
    if (TAT[i] && TAT[i]->T == a[i]) {
      continue;
    }
    time_amortization_table_free (&TAT[i]);
    for (j = 0; j < weights_coords; j++) {
      if (TAT[j] && TAT[j]->T == a[i]) {
        TAT[i] = TAT[j];
        TAT[i]->refcnt++;
        break;
      }
    }
    if (j >= weights_coords) {
      TAT[i] = time_amortization_table_alloc (a[i]);
    }
  }
  vkprintf (1, "%s: time amortization tables were initialized.\n", __func__);
  return SKIP_ALL_BYTES;
}

int targ_weights_rpcc_execute (struct connection *c, int op, int len) {
  vkprintf (3, "%s: connection %d (%s:%d), op: %d, len: %d\n", __func__, c->fd, show_remote_ip (c), c->remote_port, op, len);
  switch (op) {
    case RPC_REQ_RESULT: return targ_weights_receive_half_lifes (c, len);
    case RPC_INVOKE_REQ: return default_tl_rpcs_execute (c, op, len);
  }
  return SKIP_ALL_BYTES;
}

int rpcc_ready (struct connection *c) {
  int i;
  vkprintf (2, "%s: connection %d\n", __func__, c->fd);
  tl_store_init (c, subscription_qid = next_qid ());
  tl_store_int (TL_WEIGHTS_SUBSCRIBE);
  tl_store_int (log_split_min); // vector_rem
  tl_store_int (log_split_mod); // vector_mod
  tl_store_int (weights_coords);
  for (i = 0; i < weights_coords; i++) {
    tl_store_int (i);
  }
  tl_store_int (targ_weights_last_update_time);
  tl_store_int (10 * updates_limit);
  tl_store_int (updates_limit);
  tl_store_int (10 * small_updates_limit);
  tl_store_int (small_updates_limit);
  tl_store_end_ext (RPC_INVOKE_REQ);
  return 0;
}

int targ_weights_small_update (int vector_id, int coord_id, int relaxation_time, int value) {
  if (coord_id < 0 || coord_id >= weights_coords || TAT == NULL) {
    return -1;
  }
  user_t *U = get_user (vector_id);
  if (U == NULL) {
    return -1;
  }
  if (U->weights == NULL) {
    U->weights = targ_weights_vector_alloc ();
  }
  const int dt = relaxation_time - U->weights->relaxation_time;
  if (dt > 0) {
    int i;
    if (targ_weights_last_update_time < relaxation_time) {
      targ_weights_last_update_time = relaxation_time;
    }
    for (i = 0; i < weights_coords; i++) {
      U->weights->values[i] *= time_amortization_table_fast_exp (TAT[i], dt);
    }
    U->weights->relaxation_time = relaxation_time;
  }
  U->weights->values[coord_id] = value * (1.0 / 1073741824.0);
  weights_small_updates++;
  return 0;
}

int targ_weights_update (int vector_id, int relaxation_time, int coords, int *values) {
  if (coords != weights_coords || TAT == NULL) {
    return -1;
  }
  user_t *U = get_user (vector_id);
  if (U == NULL) {
    return -1;
  }
  if (U->weights == NULL) {
    U->weights = targ_weights_vector_alloc ();
  }
  int i;
  for (i = 0; i < weights_coords; i++) {
    U->weights->values[i] = values[i] * (1.0 / 1073741824.0);
  }
  weights_updates++;
  return 0;
}

double targ_weights_at (targ_weights_vector_t *weights, int coord_id) {
  if (weights == NULL || coord_id < 0 || coord_id >= weights_coords || TAT == NULL) {
    return 0.0;
  }
  const int dt = log_last_ts - weights->relaxation_time;
  double r = weights->values[coord_id];
  if (dt > 0) {
    r *= time_amortization_table_fast_exp (TAT[coord_id], dt);
  }
  return r;
}
