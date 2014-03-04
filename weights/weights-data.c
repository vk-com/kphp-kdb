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
#include <stddef.h>
#include <math.h>
#include <assert.h>
#include "kdb-data-common.h"
#include "server-functions.h"
#include "am-hash.h"
#include "am-amortization.h"
#include "weights-data.h"
#include "net-connections.h"
#include "vv-tl-parse.h"
#include "kdb-weights-binlog.h"
#include "TL/constants.h"
#include "crc32c.h"

static weights_vector_t vectors = {
  .prev = &vectors,
  .next = &vectors
};

static weights_subscription_t subscriptions = {
  .prev = &subscriptions,
  .next = &subscriptions
};

int tot_subscriptions;

/******************** time amortization ********************/
time_amortization_table_t *TAT[WEIGHTS_MAX_COORDS];

static int weights_set_half_life (struct lev_weights_set_half_life *E) {
  const int coord_id = E->type & 0xff, T = E->half_life;
  vkprintf (3, "%s: coord_id: %d, half_life: %d\n", __func__, coord_id, E->half_life);
  assert (coord_id >= 0 && coord_id < WEIGHTS_MAX_COORDS);
  if (TAT[coord_id] && TAT[coord_id]->T == T) {
    return 0;
  }
  time_amortization_table_free (&TAT[coord_id]);
  int i;
  for (i = 0; i < WEIGHTS_MAX_COORDS; i++) {
    if (TAT[i] && TAT[i]->T == T) {
      TAT[coord_id] = TAT[i];
      break;
    }
  }
  if (i >= WEIGHTS_MAX_COORDS) {
    TAT[coord_id] = time_amortization_table_alloc (T);
    tot_amortization_tables++;
  } else {
    TAT[coord_id]->refcnt++;
  }
  return 1;
}

static void init_amortization_tables (void) {
  int i;
  for (i = 0; i < WEIGHTS_MAX_COORDS; i++) {
    if (TAT[i] == NULL) {
      struct lev_weights_set_half_life E;
      E.type = i;
      E.half_life = 86400 * 30;
      weights_set_half_life (&E);
    }
  }
}

void weights_half_life_stats (weights_half_life_stat_t *S) {
  int i;
  S->min = INT_MAX;
  S->max = INT_MIN;
  S->avg = 0.0;
  for (i = 0; i < WEIGHTS_MAX_COORDS; i++) {
    assert (TAT[i]);
    const int t = TAT[i]->T;
    if (S->min > t) {
      S->min = t;
    }
    if (S->max < t) {
      S->max = t;
    }
    S->avg += t;
  }
  S->avg /= WEIGHTS_MAX_COORDS;
}

/*****************************************************/

static inline int weights_double_to_int (double x) {
  return (int) round (x * 1073741824.0);
}

static weights_counters_t *weights_vector_find_counters (weights_vector_t *V, int coord_id) {
  const int k = coord_id >> 5;
  if (!(((int) V->counters_mask) & (1 << k))) {
    return NULL;
  }
  int mask = V->counters_mask;
  weights_counters_t *C = &V->head;
  while (1) {
    assert (mask);
    int i = ffs (mask) - 1; /* least significant) bit set */
    if (i == k) {
      return C;
    }
    mask ^= 1 << i;
    C = C->next;
  }
  return NULL;
}

static int weights_vector_to_array (weights_vector_t *V, int output[WEIGHTS_MAX_COORDS]) {
  const int dt = log_last_ts - V->relaxation_time;
  double r[WEIGHTS_MAX_COORDS];
  int i, n = 0;
  weights_counters_t *C = &V->head;
  int mask = V->counters_mask;
  while (C) {
    assert (mask);
    i = ffs (mask) - 1; /* least significant) bit set */
    mask ^= 1 << i;
    i *= 32;
    if (n < i) {
      memset (r + n, 0, (i - n) * sizeof (C->values[0]));
    }
    n = i;
    assert (n + 32 <= WEIGHTS_MAX_COORDS);
    memcpy (r + n, C->values, 32 * sizeof (C->values[0]));
    if (dt > 0) {
      i = 0;
      do {
        r[n + i] *= time_amortization_table_fast_exp (TAT[n+i], dt);
      } while (++i < 32);
    }
    n += 32;
    C = C->next;
  }
  assert (!mask);
  for (i = 0; i < n; i++) {
    output[i] = weights_double_to_int (r[i]);
    //fprintf (stderr, "r[%d] = %.6lf, output[%d] = %d\n", i, r[i], i, output[i]);
  }
  while (n > 0 && !output[n-1]) {
    n--;
  }
  return n;
}

/******************* cyclic buffer *******************/
#define WEIGHTS_CYCLIC_BUFFER_SIZE 0x400000
#define WEIGHTS_CB_CACHE_SIZE      0x40000

typedef struct {
  int vector_id;
  int coord_id;
  int timestamp;
  int refcnt;
} weights_cyclic_buffer_en_t;

static weights_cyclic_buffer_en_t CB[WEIGHTS_CYCLIC_BUFFER_SIZE];
static weights_cyclic_buffer_en_t CC[WEIGHTS_CB_CACHE_SIZE];
static unsigned int c_wptr = 0;

static int cyclic_buffer_cache_store (int vector_id, int coord_id, int timestamp) {
  unsigned int h = (vector_id * 63617 + coord_id) * 63617 + timestamp;
  h %= WEIGHTS_CB_CACHE_SIZE;
  weights_cyclic_buffer_en_t *C = CC + h;
  if (C->vector_id == vector_id && C->timestamp == timestamp && C->coord_id == coord_id) {
    return 0;
  }
  C->vector_id = vector_id;
  C->coord_id = coord_id;
  C->timestamp = timestamp;
  return 1;
}

static void cyclic_buffer_add (int vector_id, int coord_id, int timestamp) {
  if (!cyclic_buffer_cache_store (vector_id, coord_id, timestamp)) {
    return;
  }
  if (CB[c_wptr].refcnt) {
    weights_subscription_t *S;
    for (S = subscriptions.next; S != &subscriptions; S = S->next) {
      if (S->c_rptr == c_wptr) {
        assert (S->type == st_small_updates);
        S->type = st_big_updates;
        S->c_rptr = -1;
      }
    }
  }
  CB[c_wptr].vector_id = vector_id;
  CB[c_wptr].coord_id = coord_id;
  CB[c_wptr].timestamp = timestamp;
  CB[c_wptr].refcnt = 0;
  c_wptr = (c_wptr + 1) % WEIGHTS_CYCLIC_BUFFER_SIZE;
}

/******************* subscriptions *******************/
static void subsription_insert (weights_subscription_t *u, weights_subscription_t *V, weights_subscription_t *v) {
  u->next = V; V->prev = u;
  v->prev = V; V->next = v;
}

static void subscription_delete (weights_subscription_t *S) {
  weights_subscription_t *u = S->prev, *v = S->next;
  u->next = v;
  v->prev = u;
  S->prev = S->next = NULL;
}

static void subscription_free (weights_subscription_t *S) {
  vkprintf (3, "%s: (connection %d, %s:%d)\n", __func__, S->c->fd, show_remote_ip (S->c), S->c->remote_port);
  subscription_delete (S);
  assert (S->last);
  if (S->last != &vectors) {
    (S->last->subscription_refcnt)--;
    assert (S->last->subscription_refcnt >= 0);
  }
  if (S->type == st_small_updates) {
    assert (S->c_rptr >= 0 && S->c_rptr < WEIGHTS_CYCLIC_BUFFER_SIZE);
    CB[S->c_rptr].refcnt--;
    assert (CB[S->c_rptr].refcnt >= 0);
  }
  zfree (S->coord_ids, 4 * (S->coord_ids[0] + 1));
  S->coord_ids = NULL;
  zfree (S, sizeof (weights_subscription_t));
  tot_subscriptions--;
}

int weights_subscription_stop (struct connection *c) {
  weights_subscription_t *S;
  for (S = subscriptions.next; S != &subscriptions; S = S->next) {
    if (S->c == c && S->conn_generation == c->generation) {
      subscription_free (S);
      return 0;
    }
  }
  return -1;
}

int igcd (int a, int b) {
  int c;
  if (a < b) {
    c = a;
    a = b;
    b = c;
  }
  while (b) {
    c = a % b;
    a = b;
    b = c;
  }
  return a;
}

int weights_check_vector_split (int vector_rem, int vector_mod) {
  // x * split_mod + split_min = y * vector_mod + vector_rem
  // x * split_mod - y * vector_mod = vector_rem - split_min
  int g = igcd (log_split_mod, vector_mod);
  if ((vector_rem - log_split_min) % g) {
    return -1;
  }
  return 0;
}

static void weights_subscription_choose_updates (weights_subscription_t *S, int updates_start_time) {
  vkprintf (3, "%s: (connection %d, %s:%d), updates_start_time: %d\n", __func__, S->c->fd, show_remote_ip (S->c), S->c->remote_port, updates_start_time);
  S->type = st_big_updates;
  S->c_rptr = c_wptr;
  if (!CB[c_wptr].vector_id) {
    S->c_rptr = 0;
    if (c_wptr <= 0) {
      vkprintf (2, "Subscribes on big updates (connection %d, %s:%d)\n", S->c->fd, show_remote_ip (S->c), S->c->remote_port);
      S->c_rptr = -1;
      return;
    }
  }
  if (CB[S->c_rptr].timestamp >= updates_start_time) {
    vkprintf (2, "Subscribes on big updates (connection %d, %s:%d)\n", S->c->fd, show_remote_ip (S->c), S->c->remote_port);
    S->c_rptr = -1;
    return;
  }
  S->type = st_small_updates;
  CB[S->c_rptr].refcnt++;
  vkprintf (2, "Subscribes on small updates (connection %d, %s:%d)\n", S->c->fd, show_remote_ip (S->c), S->c->remote_port);
  return;
}

int weights_subscribe (struct connection *c, int coords, int *coord_ids, int vector_rem, int vector_mod, int updates_start_time, int updates_seek_limit, int updates_limit, int small_updates_seek_limit, int small_updates_limit, int half_life[WEIGHTS_MAX_COORDS]) {
  int j;
  vkprintf (2, "%s: connection %d (%s:%d), vector_rem: %d, vector_mode: %d, updates_start_time: %d, big_limits (%d,%d), small_limits (%d, %d)\n",
    __func__, c->fd, show_remote_ip (c), c->remote_port,
    vector_rem, vector_mod, updates_start_time, updates_seek_limit, updates_limit, small_updates_seek_limit,  small_updates_limit);
  if (weights_check_vector_split (vector_rem, vector_mod) < 0) {
    return -1;
  }
  if (!(coords >= 0 && coords <= WEIGHTS_MAX_COORDS)) {
    return -1;
  }
  for (j = 0; j < coords; j++) {
    if (!(coord_ids[j] >= 0 && coord_ids[j] < WEIGHTS_MAX_COORDS)) {
      return -1;
    }
    assert (TAT[coord_ids[j]]);
    half_life[j] = TAT[coord_ids[j]]->T;
  }

  if (!weights_subscription_stop (c)) {
    vkprintf (2, "Remove old subscription for connection %d (%s:%d).\n", c->fd, show_remote_ip (c), c->remote_port);
  }

  weights_subscription_t *S = zmalloc0 (sizeof (weights_subscription_t));
  tot_subscriptions++;
  subsription_insert (subscriptions.prev, S, &subscriptions);

  S->c = c;
  S->conn_generation = c->generation;
  S->coord_ids = zmalloc (4 * (coords + 1));
  S->coord_ids[0] = coords;
  memcpy (S->coord_ids + 1, coord_ids, 4 * coords);
  for (j = 1; j <= S->coord_ids[0]; j++) {
    S->bitset_coords[S->coord_ids[j] >> 5] |= 1U << (S->coord_ids[j] & 31);
  }
  S->rem = vector_rem;
  S->mod = vector_mod;
  S->updates_start_time = updates_start_time;
  S->updates_seek_limit = updates_seek_limit;
  S->updates_limit = updates_limit;
  S->small_updates_seek_limit = small_updates_seek_limit;
  S->small_updates_limit = small_updates_limit;
  S->last = &vectors;
  S->last->subscription_refcnt++;
  weights_subscription_choose_updates (S, updates_start_time);
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

static int weights_subscription_big_updates (weights_subscription_t *S) {
  vkprintf (3, "%s: (connection %d, %s:%d)\n", __func__, S->c->fd, show_remote_ip (S->c), S->c->remote_port);
  const int weights_max_update_limit = (0x100000 - 8) / (4 * S->coord_ids[0] + 12);
  int limit = S->updates_limit < weights_max_update_limit ? S->updates_limit : weights_max_update_limit;
  if (limit <= 0) {
    limit = weights_max_update_limit;
  }
  weights_vector_t **A = alloca (limit * sizeof (A[0]));
  int seek_limit = S->updates_seek_limit, n = 0;
  while (seek_limit > 0) {
    if (S->last->next == &vectors) {
      break;
    }
    S->last->subscription_refcnt--;
    S->last = S->last->next;
    S->last->subscription_refcnt++;
    seek_limit--;
    if (S->last->relaxation_time < S->updates_start_time || S->last->vector_id % S->mod != S->rem) {
      continue;
    }
    A[n++] = S->last;
    if (n >= limit) {
      break;
    }
  }
  if (!n) {
    return 0;
  }
  int i, j;
  tl_store_init (S->c, next_qid ());
  tl_store_int (TL_WEIGHTS_SEND_UPDATES);
  tl_store_int (S->coord_ids[0]);
  tl_store_int (n);
  for (i = 0; i < n; i++) {
    int x[WEIGHTS_MAX_COORDS];
    const int m = weights_vector_to_array (A[i], x);
    vkprintf (4, "%s: vector_id: %d, m = %d\n", __func__, A[i]->vector_id, m);
    tl_store_int (A[i]->vector_id);
    tl_store_int (A[i]->relaxation_time);
    for (j = 1; j <= S->coord_ids[0]; j++) {
      tl_store_int ((S->coord_ids[j] < m) ? x[S->coord_ids[j]] : 0);
    }
  }
  tl_store_end_ext (RPC_INVOKE_REQ);
  return n;
}

static int weights_subscription_small_updates (weights_subscription_t *S) {
  vkprintf (3, "%s: (connection %d, %s:%d)\n", __func__, S->c->fd, show_remote_ip (S->c), S->c->remote_port);
  const int weights_max_small_update_limit = (0x100000 - 8) / 16;
  int limit = S->small_updates_limit < weights_max_small_update_limit ? S->small_updates_limit : weights_max_small_update_limit;
  if (limit <= 0) {
    limit = weights_max_small_update_limit;
  }
  int seek_limit = S->small_updates_seek_limit;
  int n = 0;
  int *pn = NULL;
  while (--seek_limit >= 0) {
    int j = (S->c_rptr + 1) % WEIGHTS_CYCLIC_BUFFER_SIZE;
    if (j == c_wptr) {
      break;
    }
    CB[S->c_rptr].refcnt--;
    assert (CB[S->c_rptr].refcnt >= 0);
    S->c_rptr = j;
    weights_cyclic_buffer_en_t *E = &CB[S->c_rptr];
    E->refcnt++;
    if (E->timestamp < S->updates_start_time || E->vector_id % S->mod != S->rem) {
      continue;
    }
    j = E->coord_id;
    if (!(S->bitset_coords[j>>5] & (1U << (j & 31)))) {
      continue;
    }
    weights_vector_t *V = get_vector_f (E->vector_id, 0);
    assert (V);
    j = E->coord_id;
    weights_counters_t *C = weights_vector_find_counters (V, j);
    j &= 31;
    assert (C);
    if ((unsigned short) E->timestamp != C->t[j]) {
      continue;
    }
    if (!n) {
      tl_store_init (S->c, next_qid ());
      tl_store_int (TL_WEIGHTS_SEND_SMALL_UPDATES);
      pn = (int *) tl_store_get_ptr (4);
    }
    tl_store_int (E->vector_id);
    tl_store_int (E->coord_id);
    tl_store_int (V->relaxation_time);
    tl_store_int (weights_double_to_int (C->values[j]));
    n++;
    if (j >= limit) {
      break;
    }
  }
  if (!n) {
    return 0;
  }
  assert (pn);
  *pn = n;
  tl_store_end_ext (RPC_INVOKE_REQ);
  return n;
}

static int weights_subscription_updates (weights_subscription_t *S) {
  if (S->conn_generation != S->c->generation) {
    subscription_free (S);
    return -1;
  }

  if (S->type == st_big_updates) {
    vkprintf (4, "%s: S->last->vector_id = %d, S->last->relaxation_time = %d, S->last:%p, &vectors:%p\n", __func__, S->last->vector_id, S->last->relaxation_time, S->last, &vectors);
    weights_subscription_choose_updates (S, S->updates_start_time > S->last->relaxation_time ? S->updates_start_time : S->last->relaxation_time );
  }

  if (S->type == st_big_updates) {
    return weights_subscription_big_updates (S);
  }
  return weights_subscription_small_updates (S);
}

/******************** vector double linked lists *******************/

static void del_use (weights_vector_t *V) {
  vkprintf (3, "%s: vector_id = %d, V->subscription_refcnt = %d\n", __func__, V->vector_id, V->subscription_refcnt);
  if (V->subscription_refcnt > 0) {
    int t = 0;
    weights_subscription_t *p;
    for (p = subscriptions.next; p != &subscriptions; p = p->next) {
      if (p->last == V) {
        vkprintf (3, "%s: vector (%d) was last for the subscription from connection %d (%s:%d)\n",
          __func__, V->vector_id, p->c->fd, show_remote_ip (p->c), p->c->remote_port);
        t++;
        p->last = p->last->prev;
        p->last->subscription_refcnt++;
      }
    }
    assert (t == V->subscription_refcnt);
    V->subscription_refcnt = 0;
  }
  weights_vector_t *u = V->prev, *v = V->next;
  u->next = v;
  v->prev = u;
  V->prev = V->next = NULL;
}

static void insert_use (weights_vector_t *u, weights_vector_t *V, weights_vector_t *v) {
  u->next = V; V->prev = u;
  v->prev = V; V->next = v;
}

static void add_use_front (weights_vector_t *V) {
  insert_use (&vectors, V, vectors.next);
}

static void add_use_back (weights_vector_t *V) {
  insert_use (vectors.prev, V, &vectors);
}

/******************** vector hash table ********************/
int vector_hash_prime;
static weights_vector_t **H;
int tot_vectors, tot_counters_arrays;

static void weights_hash_init (void) {
  if (H != NULL) {
    return;
  }
  if (!vector_hash_prime) {
    vector_hash_prime = WEIGHTS_DEFAULT_HASH_SIZE;
  }
  vector_hash_prime = am_choose_hash_prime (vector_hash_prime);
  H = zmalloc0 (sizeof (H[0]) * vector_hash_prime);
}

weights_vector_t *get_vector_f (int vector_id, int force) {
  const int h = vector_id % vector_hash_prime;
  weights_vector_t **p = &H[h], *V;
  while (*p) {
    V = *p;
    if (V->vector_id == vector_id) {
      *p = V->hnext;
      if (force >= 0) {
        V->hnext = H[h];
        H[h] = V;
      } else {
        del_use (V);
      }
      return V;
    }
    p = &V->hnext;
  }
  if (force > 0) {
    tot_vectors++;
    tot_counters_arrays++;
    V = zmalloc0 (sizeof (weights_vector_t));
    V->counters_mask = 1;
    V->vector_id = vector_id;
    V->hnext = H[h];
    add_use_front (V);
    return H[h] = V;
  }
  return NULL;
}

static void weights_vector_relax (weights_vector_t *V) {
  const int dt = log_last_ts - V->relaxation_time;
  if (dt < 0) {
    return;
  }
  if (dt > 0) {
    weights_counters_t *C = &V->head;
    int mask = V->counters_mask;
    do {
      int o = ffs (mask) - 1;
      mask ^= 1 << o;
      o *= 32;
      int j;
      for (j = 0; j < 32; j++, o++) {
        C->values[j] *= time_amortization_table_fast_exp (TAT[o], dt);
      }
      C = C->next;
    } while (C);
    assert (!mask);
    V->relaxation_time = log_last_ts;
  }
  del_use (V);
  add_use_back (V);
}

static int weights_counters_is_zero (weights_counters_t *C) {
  int i;
  for (i = 0; i < 32; i++) {
    if (fabs (C->values[i]) > 1e-9) {
      return 0;
    }
  }
  return 1;
}

static weights_counters_t *alloc_counter (weights_vector_t *V, weights_counters_t *prev, weights_counters_t *cur, int k) {
  tot_counters_arrays++;
  weights_counters_t *D = zmalloc0 (sizeof (weights_counters_t));
  D->next = cur;
  assert (prev);
  prev->next = D;
  V->counters_mask |= 1 << k;
  return D;
}

int weights_incr (struct lev_weights_incr *E) {
  const int coord_id = E->type & 0xff;
  vkprintf (3, "%s: vector_id: %d, coord_id: %d, value: %d\n", __func__, E->vector_id, coord_id, E->value);
  cyclic_buffer_add (E->vector_id, coord_id, log_last_ts);
  weights_vector_t *V = get_vector_f (E->vector_id, 1);
  const int k = coord_id >> 5;
  int i, mask = V->counters_mask;
  weights_counters_t *C = &V->head, *prev = NULL;
  while (1) {
    if (!mask) {
      assert (C == NULL);
      C = alloc_counter (V, prev, C, k);
      break;
    }
    i = ffs (mask) - 1;
    if (i == k) {
      break;
    }
    mask ^= 1 << i;
    if (k < i) {
      C = alloc_counter (V, prev, C, k);
      break;
    }
    prev = C;
    C = C->next;
  }
  assert (C);
  weights_vector_relax (V);
  //x # y := (x + y) / (1 + xy)
  i = coord_id & 31;
  C->t[i] = (unsigned short) log_last_ts;
  double *x = &C->values[i];
  const double y = E->value * (1.0 / 1073741824.0);
  *x = ((*x) + y) / (1.0 + (*x) * y);
  int res = weights_double_to_int (*x);
  return res;
}

int weights_at (int vector_id, int coord_id, int *value) {
  weights_vector_t *V = get_vector_f (vector_id, 0);
  if (V == NULL) {
    return -1;
  }
  weights_counters_t *C = weights_vector_find_counters (V, coord_id);
  if (C == NULL) {
    vkprintf (4, "%s(vector_id=%d, coord_id=%d): weights_vector_find_counters returns NULL.\n", __func__, vector_id, coord_id);
    *value = 0;
    return 0;
  }
  double x = C->values[coord_id & 31];
  int dt = log_last_ts - V->relaxation_time;
  if (dt > 0) {
    x *= time_amortization_table_fast_exp (TAT[coord_id], dt);
  }
  *value = weights_double_to_int (x);
  return 0;
}

int weights_get_vector (int vector_id, int output[WEIGHTS_MAX_COORDS]) {
  weights_vector_t *V = get_vector_f (vector_id, 0);
  if (V == NULL) {
    return -1;
  }
  return weights_vector_to_array (V, output);
}

static int weights_replay_logevent (struct lev_generic *E, int size);
int init_weights_data (int schema) {
  replay_logevent = weights_replay_logevent;
  init_amortization_tables ();
  weights_hash_init ();
  return 0;
}

static int weights_le_start (struct lev_start *E) {
  if (E->schema_id != WEIGHTS_SCHEMA_V1) {
    kprintf ("LEV_START schema_id isn't to WEIGHTS_SCHEMA_V1.\n");
    return -1;
  }
  if (E->extra_bytes) {
    kprintf ("LEV_START extra_bytes isn't zero.\n");
    return -1;
  }
  log_split_min = E->split_min;
  log_split_max = E->split_max;
  log_split_mod = E->split_mod;
  assert (log_split_mod > 0 && log_split_min >= 0 && log_split_min + 1 ==  log_split_max && log_split_max <= log_split_mod);
  return 0;
}

static int weights_replay_logevent (struct lev_generic *E, int size) {
  int s;
  switch (E->type) {
    case LEV_START:
      if (size < 24 || E->b < 0 || E->b > 4096) { return -2; }
      s = 24 + ((E->b + 3) & -4);
      if (size < s) { return -2; }
      return weights_le_start ((struct lev_start *) E) >= 0 ? s : -1;
    case LEV_NOOP:
    case LEV_TIMESTAMP:
    case LEV_CRC32:
    case LEV_ROTATE_FROM:
    case LEV_ROTATE_TO:
    case LEV_TAG:
      return default_replay_logevent (E, size);
    case LEV_WEIGHTS_INCR...LEV_WEIGHTS_INCR + 0xff:
      s = sizeof (struct lev_weights_incr);
      if (size < s) { return -2; }
      weights_incr ((struct lev_weights_incr *) E);
      return s;
    case LEV_WEIGHTS_SET_HALF_LIFE...LEV_WEIGHTS_SET_HALF_LIFE + 0xff:
      s = sizeof (struct lev_weights_set_half_life);
      if (size < s) { return -2; }
      weights_set_half_life ((struct lev_weights_set_half_life *) E);
      return s;
  }
  kprintf ("unknown log event type %08x at position %lld\n", E->type, log_cur_pos ());
  return -3;
}

/******************** do functions  ********************/

int do_weights_set_half_life (int coord_id, int half_life) {
  if (coord_id < 0 || coord_id >= WEIGHTS_MAX_COORDS || half_life <= 0) {
    return -1;
  }
  struct lev_weights_set_half_life *E = alloc_log_event (LEV_WEIGHTS_SET_HALF_LIFE + coord_id, sizeof (*E), half_life);
  int r = weights_set_half_life (E);
  if (r <= 0) {
    unalloc_log_event (sizeof (*E));
  }
  return r;
}

int do_weights_incr (int vector_id, int coord_id, int value) {
  //vkprintf (3, "%s: vector_id = %d, coord_id = %d, value = %d\n", __func__, vector_id, coord_id, value);
  assert (coord_id >= 0 && coord_id < WEIGHTS_MAX_COORDS);
  struct lev_weights_incr *E = alloc_log_event (LEV_WEIGHTS_INCR + coord_id, sizeof (*E), vector_id);
  E->value = value;
  return weights_incr (E);
}

void weights_subscriptions_work (void) {
  weights_subscription_t *S = subscriptions.next, *W;
  while (S != &subscriptions) {
    if (NB_used >= NB_max * 0.75) {
      //move processed subscriptions to the end of the subsriptions list
      S = S->prev;
      while (S != &subscriptions) {
        W = S->prev;
        subscription_delete (S);
        subsription_insert (subscriptions.prev, S, &subscriptions);
        S = W;
      }
      break;
    }
    W = S->next;
    int r = weights_subscription_updates (S);
    if (verbosity >= 2) {
      if (r > 0) {
        kprintf ("%s: Send %d %s updates to the connection %d (%s:%d)\n", __func__, r,
          S->type == st_big_updates ? "" : "small", S->c->fd, show_remote_ip (S->c), S->c->remote_port);
      } else if (r < 0) {
        kprintf ("%s: Stop sending updates to the connection %d\n", __func__, S->c->fd);
      } else {
        kprintf ("%s: No fresh %s updates to the connection %d (%s:%d)\n", __func__,
          S->type == st_big_updates ? "" : "small", S->c->fd, show_remote_ip (S->c), S->c->remote_port);
      }
    }
    S = W;
  }
}

/******************** snapshot  ********************/
//#define WEIGHTS_INDEX_MAGIC_V1 0x64e14913
#define WEIGHTS_INDEX_MAGIC_V2 0x64e14914
#pragma	pack(push,4)
typedef struct {
/* strange numbers */
  int magic;
  int created_at;
  long long log_pos0;
  long long log_pos1;
  unsigned long long file_hash;
  int log_timestamp;
  unsigned int log_pos0_crc32;
  unsigned int log_pos1_crc32;
  int log_split_min;
  int log_split_max;
  int log_split_mod;
  int c_wptr, cb_entries;
  int tot_vectors;
  unsigned int body_crc32c[8];
} weights_index_header_t;
#pragma	pack(pop)

long long jump_log_pos;
int jump_log_ts;
unsigned jump_log_crc32;
//static int newidx_fd, idx_fd, index_exists;
static kfs_snapshot_write_stream_t SWS;

#define	BUFFSIZE 0x1000000

static char *Buff = NULL, *rptr = NULL, *wptr = NULL;
static unsigned int idx_crc32c_complement;

static void flushout (void) {
  int s;
  if (rptr < wptr) {
    s = wptr - rptr;
    kfs_sws_write (&SWS, rptr, s);
  }
  rptr = wptr = Buff;
}

static void clearin (void) {
  if (Buff == NULL) {
    Buff = malloc (BUFFSIZE);
    assert (Buff);
  }
  rptr = wptr = Buff + BUFFSIZE;
  idx_crc32c_complement = -1;
}

static void freein (void) {
  if (Buff) {
    free (Buff);
    Buff = rptr = wptr = NULL;
  }
}

static int writeout (const void *D, size_t len) {
  idx_crc32c_complement = crc32c_partial (D, len, idx_crc32c_complement);
  const int res = len;
  const char *d = D;
  while (len > 0) {
    int r = Buff + BUFFSIZE - wptr;
    if (r > len) {
      r = len;
    }
    memcpy (wptr, d, r);
    d += r;
    wptr += r;
    len -= r;
    if (len > 0) {
      flushout ();
    }
  }
  return res;
}

static void *readin (size_t len) {
  assert (len >= 0);
  if (rptr + len <= wptr) {
    return rptr;
  }
  if (wptr < Buff + BUFFSIZE) {
    return 0;
  }
  memcpy (Buff, rptr, wptr - rptr);
  wptr -= rptr - Buff;
  rptr = Buff;
  wptr += kfs_read_file (Snapshot, wptr, Buff + BUFFSIZE - wptr);
  if (rptr + len <= wptr) {
    return rptr;
  } else {
    return 0;
  }
}

static void readadv (size_t len) {
  assert (len >= 0 && len <= wptr - rptr);
  idx_crc32c_complement = crc32c_partial (rptr, len, idx_crc32c_complement);
  rptr += len;
}

void bread (void *b, size_t len) {
  void *p = readin (len);
  assert (p != NULL);
  memcpy (b, p, len);
  readadv (len);
}

int save_index (int writing_binlog) {
  int header_buff[1024];
  assert (sizeof (weights_index_header_t) <= sizeof (header_buff) - 4);
  weights_index_header_t *header = (weights_index_header_t *) header_buff;
  memset (header, 0, sizeof (header_buff));
  int i;

  if (!kfs_sws_open (&SWS, engine_snapshot_replica, log_cur_pos (), jump_log_pos)) {
    return 0;
  }

  header->magic = 0;
  header->created_at = time (NULL);
  header->log_pos1 = log_cur_pos ();
  header->log_timestamp = log_last_ts;
  if (writing_binlog) {
    relax_write_log_crc32 ();
  } else {
    relax_log_crc32 (0);
  }
  header->log_pos1_crc32 = ~log_crc32_complement;

  header->log_split_min = log_split_min;
  header->log_split_max = log_split_max;
  header->log_split_mod = log_split_mod;

  assert (lseek (SWS.newidx_fd, sizeof (header_buff), SEEK_SET) == sizeof (header_buff));
  /* half life array */
  clearin ();
  for (i = 0; i < WEIGHTS_MAX_COORDS; i++) {
    assert (TAT[i]);
    writeout (&TAT[i]->T, 4);
  }
  flushout ();
  header->body_crc32c[0] = ~idx_crc32c_complement;

  /* cyclic buffer data */
  clearin ();
  header->c_wptr = c_wptr;
  header->cb_entries = CB[c_wptr].vector_id ? WEIGHTS_CYCLIC_BUFFER_SIZE : c_wptr;
  for (i = 0; i < header->cb_entries; i++) {
    writeout (&CB[i].vector_id, 4);
    writeout (&CB[i].coord_id, 4);
    writeout (&CB[i].timestamp, 4);
  }
  flushout ();
  header->body_crc32c[1] = ~idx_crc32c_complement;
  /* vectors data */
  clearin ();
  header->tot_vectors = tot_vectors;
  int t = 0;
  weights_vector_t *V;
  for (V = vectors.next; V != &vectors; V = V->next) {
    assert (V->vector_id);
    writeout (&V->vector_id, 4);
    writeout (&V->relaxation_time, 4);
    int mask = V->counters_mask, m = 0;
    weights_counters_t *D[16], *C = &V->head;
    int k = 0;
    while (mask) {
      i = ffs (mask) - 1;
      mask ^= 1 << i;
      assert (C);
      if (!k || !weights_counters_is_zero (C)) {
        m |= 1 << i;
        D[k++] = C;
      }
      C = C->next;
    }
    writeout (&m, 4);
    for (i = 0; i < k; i++) {
      C = D[i];
      writeout (C->values, sizeof (C->values));
      writeout (C->t, sizeof (C->t));
    }
    t++;
  }
  assert (t == tot_vectors);
  flushout ();
  header->body_crc32c[2] = ~idx_crc32c_complement;

  freein ();
  header->magic = WEIGHTS_INDEX_MAGIC_V2;
  header_buff[1023] = compute_crc32c (header_buff, 1023 * 4);
  assert (lseek (SWS.newidx_fd, 0, SEEK_SET) == 0);
  kfs_sws_write (&SWS, header_buff, sizeof (header_buff));
  kfs_sws_close (&SWS);
  return 0;
}

int load_index (void) {
  int i;
  int header_buff[1024];
  assert (sizeof (weights_index_header_t) <= sizeof (header_buff) - 4);
  weights_index_header_t *header = (weights_index_header_t *) header_buff;
  if (Snapshot == NULL) {
    jump_log_ts = 0;
    jump_log_pos = 0;
    jump_log_crc32 = 0;
    return 0;
  }

  kfs_read_file (Snapshot, header_buff, sizeof (header_buff));

  if (header->magic != WEIGHTS_INDEX_MAGIC_V2) {
    kprintf ("[%s] index file is not for weights-engine\n", Snapshot->info->filename);
    return -1;
  }

  if (header_buff[1023] != (int) compute_crc32c (header_buff, 1023 * 4)) {
    kprintf ("[%s] index header is broken (crc32c isn't matched)\n", Snapshot->info->filename);
    return -1;
  }
  clearin ();
  /* half life array */
  for (i = 0; i < WEIGHTS_MAX_COORDS; i++) {
    int *p = readin (4); assert (p);
    assert ((*p) > 0);
    struct lev_weights_set_half_life E;
    E.type = i;
    E.half_life = *p;
    weights_set_half_life (&E);
    readadv (4);
  }
  if (~idx_crc32c_complement != header->body_crc32c[0]) {
    kprintf ("[%s] half life section is corrupted (crc32c isn't matched)\n", Snapshot->info->filename);
    return -1;
  }
  /* cyclic buffer data */
  idx_crc32c_complement = -1;
  assert (header->cb_entries >= 0 && header->cb_entries <= WEIGHTS_CYCLIC_BUFFER_SIZE);
  for (i = 0; i < header->cb_entries; i++) {
    int *p = readin (12); assert (p);
    CB[i].vector_id = p[0];
    CB[i].coord_id = p[1];
    CB[i].timestamp = p[2];
    CB[i].refcnt = 0;
    readadv (12);
  }
  c_wptr = header->c_wptr;
  if (CB[c_wptr].vector_id) {
    i = c_wptr;
    do {
      cyclic_buffer_cache_store (CB[i].vector_id, CB[i].coord_id, CB[i].timestamp);
      i = (i + 1) % WEIGHTS_CYCLIC_BUFFER_SIZE;
    } while (i != c_wptr);
  } else {
    for (i = 0; i < c_wptr; i++) {
      cyclic_buffer_cache_store (CB[i].vector_id, CB[i].coord_id, CB[i].timestamp);
    }
  }

  if (~idx_crc32c_complement != header->body_crc32c[1]) {
    kprintf ("[%s] cyclic buffer section is corrupted (crc32c isn't matched)\n", Snapshot->info->filename);
    return -1;
  }

  /* vectors data */
  idx_crc32c_complement = -1;

  if (header->tot_vectors > WEIGHTS_DEFAULT_HASH_SIZE) {
    vector_hash_prime = header->tot_vectors;
  }
  init_weights_data (WEIGHTS_SCHEMA_V1);

  assert (!tot_vectors);
  for (i = 0; i < header->tot_vectors; i++) {
    int *p = readin (12); assert (p);
    //fprintf (stderr, "%s: %d %d %d\n", __func__, p[0], p[1], p[2]);
    weights_vector_t *V = get_vector_f (p[0], 1);
    assert (V);
    del_use (V);
    add_use_back (V);
    if (tot_vectors != (i + 1)) {
      kprintf ("%s: tot_vectors (%d) != %d, duplicate vector_id (%d) in snapshot '%s'?\n",
        __func__, tot_vectors, i + 1, p[0], Snapshot->info->filename);
      assert (0);
    }
    V->relaxation_time = p[1];
    int j, mask = p[2];
    readadv (12);
    if (mask & 0xffffff00) {
      kprintf ("%s: illegal counters mask (0x%08x), high 24 bits aren't zero\n", __func__, mask);
      return -1;
    }
    if (!(mask & 1)) {
      kprintf ("%s: illegal counters mask (0x%08x), lower 1 bit isn't set\n", __func__, mask);
      return -1;
    }
    V->counters_mask = mask;
    weights_counters_t *C = &V->head;
    j = 0;
    while (mask) {
      if (j > 0) {
        weights_counters_t *D = zmalloc (sizeof (weights_counters_t));
        tot_counters_arrays++;
        D->next = NULL;
        C->next = D;
        C = D;
      }
      bread (C->values, sizeof (C->values));
      bread (C->t, sizeof (C->t));
      j++;
      mask &= mask - 1;
    }
  }

  if (~idx_crc32c_complement != header->body_crc32c[2]) {
    kprintf ("[%s] vectors section is corrupted (crc32c isn't matched)\n", Snapshot->info->filename);
    return -1;
  }

  log_split_min = header->log_split_min;
  log_split_max = header->log_split_max;
  log_split_mod = header->log_split_mod;

  jump_log_pos = header->log_pos1;
  jump_log_crc32 = header->log_pos1_crc32;
  jump_log_ts = header->log_timestamp;

  vkprintf (4, "jump_log_pos: %lld\n", jump_log_pos);
  vkprintf (4, "jump_log_ts: %d\n", jump_log_ts);
  freein ();
  return 0;
}
