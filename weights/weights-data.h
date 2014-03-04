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

#ifndef __WEIGHTS_DATA_H__
#define __WEIGHTS_DATA_H__

#include "kfs.h"

#define WEIGHTS_MAX_COORDS 256
//#define WEIGHTS_MAX_UPDATE_LIMIT 4096
#define WEIGHTS_MAX_SMALL_UPDATE_LIMIT 32768
#define WEIGHTS_DEFAULT_HASH_SIZE 1000000

typedef struct weights_counters {
  double values[32];
  unsigned short t[32];
  struct weights_counters *next;
} weights_counters_t;

typedef struct weights_vector {
  struct weights_vector *hnext;
  struct weights_vector *prev, *next; /* double linked list for relaxation time ordering */
  int vector_id;
  int relaxation_time;
  short subscription_refcnt;
  unsigned short counters_mask;
  weights_counters_t head;
} weights_vector_t;

enum substription_type {
  st_big_updates = 0,
  st_small_updates = 1
};

typedef struct weights_subscription {
  weights_vector_t *last;
  struct weights_subscription *prev, *next;
  int *coord_ids;
  struct connection *c;
  unsigned int bitset_coords[WEIGHTS_MAX_COORDS / 32];
  int conn_generation;
  int rem;
  int mod;
  int updates_start_time;
  int updates_seek_limit;
  int updates_limit;
  /* small updates */
  int c_rptr;
  int small_updates_seek_limit;
  int small_updates_limit;
  enum substription_type type;
} weights_subscription_t;

extern int tot_vectors, tot_amortization_tables, tot_counters_arrays, tot_subscriptions, vector_hash_prime;

weights_vector_t *get_vector_f (int vector_id, int force);

int do_weights_set_half_life (int coord_id, int half_life);
int do_weights_incr (int vector_id, int coord_id, int value);
int weights_at (int vector_id, int coord_id, int *value);
int weights_get_vector (int vector_id, int output[WEIGHTS_MAX_COORDS]);

int weights_check_vector_split (int vector_rem, int vector_mod);
int weights_subscribe (struct connection *c, int coords, int *coord_ids, int vector_rem, int vector_mod, int updates_start_time, int updates_seek_limit, int updates_limit, int small_updates_seek_limit, int small_updates_limit, int half_life[WEIGHTS_MAX_COORDS]);
void weights_subscriptions_work (void);
int weights_subscription_stop (struct connection *c);

extern long long jump_log_pos;
extern int jump_log_ts;
extern unsigned jump_log_crc32;

int load_index (void);
int save_index (int writing_binlog);

typedef struct {
  int min;
  int max;
  double avg;
} weights_half_life_stat_t;

void weights_half_life_stats (weights_half_life_stat_t *S);

#endif
