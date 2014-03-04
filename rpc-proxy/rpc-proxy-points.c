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

    Copyright 2012-2013 Vkontakte Ltd
              2009-2012 Nikolai Durov (original mc-proxy code)
              2009-2012 Andrei Lopatin (original mc-proxy code)
              2012-2013 Vitaliy Valtman
*/

#include "rpc-proxy.h"
#include "net-rpc-targets.h"
#include "vv-tree.h"
#include "net-rpc-server.h"
#include "net-rpc-client.h"

#include <math.h>
#include <assert.h>

#define MAX_RETRIES 10

int PE_NUM;

struct rpc_point_descr {
  unsigned ip;
  short sugar;
  short port;
};

static int cmp_points (rpc_point_t *A, rpc_point_t *B) {
  unsigned h1 = htonl (A->B->methods->get_host (A->B));
  unsigned h2 = htonl (B->B->methods->get_host (B->B));
  int p1 = A->B->methods->get_port (A->B);
  int p2 = B->B->methods->get_port (B->B);
  if (A->x < B->x) {
    return -1;
  } else if (A->x > B->x) {
    return 1;
  } else if (h1 < h2) {
    return -1;
  } else if (h1 > h2) {
    return 1;
  } else {
    return p1 - p2;
  }
}

static void sort_points (rpc_point_t *A, int N) {
  int i, j;
  rpc_point_t h, t;
  if (N <= 0) {
    return;
  }
  if (N == 1) {
    if (cmp_points (&A[0], &A[1]) > 0) {
      t = A[0];
      A[0] = A[1];
      A[1] = t;
    }
    return;
  }
  i = 0;
  j = N;
  h = A[j >> 1];
  do {
    while (cmp_points (&A[i], &h) < 0) { i++; }
    while (cmp_points (&A[j], &h) > 0) { j--; }
    if (i <= j) {
      t = A[i];  A[i++] = A[j];  A[j--] = t;
    }
  } while (i <= j);
  sort_points (A+i, N-i);
  sort_points (A, j);
}

#pragma pack(push,4)
struct points_extra {
  int points_num;
  rpc_point_t points[0];
};
#pragma pack(pop)

static void build_rpc_points (struct rpc_cluster *C, int num, int points_num) {
  assert (C->proto == PROTO_TCP);
  if (verbosity >= 3) {
    fprintf (stderr, "--> started\n");
  }

  int N = C->tot_buckets, K = points_num, NK = N*K, j, i;

  struct points_extra *E = malloc (sizeof (struct points_extra) + sizeof (rpc_point_t) * points_num * N);
  assert (E);
  E->points_num = points_num;
  C->extensions_extra[num] = E;

/*  int custom_fields_save[N];
  for (i = 0; i < N; i++) {
    custom_fields_save[i] = C->buckets[i]->custom_field;
  }*/

  //double t1 = get_utime (CLOCK_MONOTONIC);

  static struct rpc_point_descr Point;

  assert (N > 0 && K > 0 && N <= 100000 && K <= 1000);

  rpc_point_t *ptr = E->points;
 
  for (i = 0; i < N; i++) {
    Point.ip = ntohl (C->buckets[i].methods->get_host (&C->buckets[i]));
    Point.port = C->buckets[i].methods->get_port (&C->buckets[i]);
//    C->buckets[i]->custom_field = i;
    for (j = 0; j < K; j++) {
      ptr->B = &C->buckets[i];
      Point.sugar = j;
      ptr->x = crc64 (&Point, sizeof (Point)) * 7047438495421301423LL;
      ptr++;
    }
  }

//  double t2 = get_utime (CLOCK_MONOTONIC);
  sort_points (E->points, NK - 1);
//  double t3 = get_utime (CLOCK_MONOTONIC);

  /*if (verbosity >= 3) {
    long long CC[N];

    for (i = 0; i < N; i++) {
      CC[i] = 0;
    }

    for (i = 0; i < NK - 1; i++) {
      if (i > NK - 100) {
        fprintf (stderr, "%llu %d\n", E->points[i].x, E->points[i].target->custom_field);
      }
    }
  
    double D = 0, S = ((double)(1LL << 32)) * (double)(1LL << 32), Z = S / N;

    for (i = 0; i < NK - 1; i++) {
      CC[E->points[i].target->custom_field] += E->points[i+1].x - E->points[i].x;
      if ((E->points[i+1].x - E->points[i].x) / Z > 100 || (E->points[i+1].x - E->points[i].x) / Z < 0) {
        fprintf (stderr, "%d: %llu %d\n", i, E->points[i].x, E->points[i].target->custom_field);
        fprintf (stderr, "%d: %llu %d\n", i + 1, E->points[i + 1].x, E->points[i + 1].target->custom_field);
      }
    }
    CC[E->points[NK - 1].target->custom_field] += E->points[0].x - E->points[NK - 1].x;

    long long min = 0x7fffffffffffffffLL, max = 0;

    for (i = 0; i < N; i++) {
      if (i > N - 100) {
        fprintf (stderr, "%.6f\n", CC[i] / Z);
      }
      if (CC[i] > max) { 
        max = CC[i];
      }
      if (CC[i] < min) { 
        min = CC[i];
      }
      D += (double) CC[i] * CC[i];
    }
    double t4 = get_utime (CLOCK_MONOTONIC);

    fprintf (stderr, "\nN=%d K=%d avg=%.3f dev=%.3f min=%.3f max=%.3f\n", N, K, 1.0, sqrt (D/(N*Z*Z) - 1.0), min / Z, max / Z);
    fprintf (stderr, "Eval time %.3f, sort time %.3f, stat time %.3f\n", t2 - t1, t3 - t2, t4 - t3);
  }*/

/*  for (i = 0; i < N; i++) {
    C->buckets[i]->custom_field = custom_fields_save[i];
  }*/
}

int rpc_points_extension (void **IP, void **Data) {
  char *key = *Data;
  int key_len = (long)*(Data + 1);
  unsigned long long x = crc64 (key, key_len);
  
  rpc_point_t *points = ((struct points_extra *)CC->extensions_extra[PE_NUM])->points;
  int points_num = ((struct points_extra *)CC->extensions_extra[PE_NUM])->points_num;
  
  int a = -1, b = CC->tot_buckets * points_num, c;
  
  while (b - a > 1) {
    c = (a + b) >> 1;
    if (x < points[c].x) {
      b = c;
    } else {
      a = c;
    }
  }

  assert (points_num > 0);

  int i;
  for (i = 0; i < MAX_RETRIES; i++) {
    if (a < 0) {
      a += points_num;
    }
    if (points[a].B->methods->get_state (points[a].B) >= 0) {
      *Data = points[a].B;
      return 0;
    }
    a--;
  }
  *Data = 0;
  return -1;
}

EXTENSION_ADD(points) {
  if (Z->lock & (1 << RPC_FUN_STRING_FORWARD)) {
    return -1;
  }
  if (!param_len || !param) {
    return -1;
  }
  int x = atoi (param);
  if (x <= 0 || x > 1000) {
    return -1;
  }
  Z->lock |= (1 << RPC_FUN_STRING_FORWARD);

  assert (Z->funs_last[RPC_FUN_STRING_FORWARD] > 0);
  Z->funs[RPC_FUN_STRING_FORWARD][--Z->funs_last[RPC_FUN_STRING_FORWARD]] = rpc_points_extension;
  if (flags & 1) {
    build_rpc_points (C, PE_NUM, x);
  }
  return 0;
}

EXTENSION_DEL(points) {
  struct points_extra *P = C->extensions_extra[PE_NUM];
  free (P);
  return 0;
}

EXTENSION_REGISTER_DEL_NUM(points,0,PE_NUM)
