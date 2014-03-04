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

    Copyright 2011-2013 Vkontakte Ltd
              2011-2013 Arseny Smirnov
              2011-2013 Aliaksei Levin
*/

#define _FILE_OFFSET_BITS 64

#include <assert.h>
#include <unistd.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>

#include "watchcat-data.h"
#include "dl.h"
#include "utils.h"

int my_verbosity = 0;

long watchcats_memory, keys_memory;
int watchcats_cnt, keys_cnt;

typedef struct t_key wkey;
typedef struct t_watchcat watchcat;


struct t_key {
  watchcat *parent;
  wkey *next_time, *prev_time;

  /*treap inside!*/
  int x, y;
  wkey *l, *r;

  int timeout;
};

typedef wkey *wkey_ptr;

typedef struct {
  int size;
  wkey_ptr root;
} wkey_set;

struct t_watchcat {
  long long id;
  watchcat *prev, *next;
  int vn;
  watchcat *parent;

  wkey_set keys;
  watchcat_query_t *query;
};

inline void free_watchcat_q (watchcat *w);
inline void free_watchcat (watchcat *w);
inline void del_entry_time (wkey *entry);
inline void add_entry_time (wkey *entry, int timeout);
inline watchcat *watchcat_q_del (watchcat *e);

hset_llp h_watchcat, h_watchcat_q;

/***
  Wkeys
 ***/

int my_rand (void) {
  return rand();
}

void wkey_set_init (wkey_set *tr) {
  tr->size = 0;
  tr->root = NULL;
}

wkey *wkey_mem;

wkey *alloc_wkey (void) {
  keys_cnt++;

  wkey *res;
  if (wkey_mem != NULL) {
    res = wkey_mem;
    wkey_mem = wkey_mem->l;
  } else {
    keys_memory += sizeof (wkey);
    res = dl_malloc (sizeof (wkey));
  }
  memset (res, 0, sizeof (wkey));
  return res;
}

void free_wkey (wkey *w) {
  keys_cnt--;

  w->l = wkey_mem;
  wkey_mem = w;
}

void trp_split (wkey_ptr v, int x, wkey_ptr *a, wkey_ptr *b) {
  while (v != NULL) {
    if (v->x > x) {
      *a = v;
      a = &v->r;
      v = v->r;
    } else {
      *b = v;
      b = &v->l;
      v = v->l;
    }
  }
  *a = *b = NULL;
}

void trp_merge (wkey_ptr *tr, wkey_ptr a, wkey_ptr b) {
  while (a != NULL || b != NULL) {
     if (a == NULL || (b != NULL && b->y > a->y)) {
       *tr = b;
       tr = &b->l;
       b = b->l;
     } else {
       *tr = a;
       tr = &a->r;
       a = a->r;
     }
  }
  *tr = NULL;
}

void trp_del (wkey_set *tr, int x) {
  wkey_ptr *v = &tr->root;

  while (*v != NULL) {
    if ((*v)->x  == x) {
      wkey_ptr t = *v;
      trp_merge (v, t->l, t->r);

      free_wkey (t);
      tr->size--;

      return;
    } else if ((*v)->x > x) {
      v = &(*v)->r;
    } else if ((*v)->x < x) {
      v = &(*v)->l;
    }
  }
  assert (0);
}

wkey *trp_add (wkey_set *tr, int x, int y, watchcat *p) {
  wkey_ptr *v = &tr->root;

  while (*v != NULL && ((*v)->y >= y)) {
    if ((*v)->x == x) {
      return *v;
    } else if ((*v)->x > x) {
      v = &(*v)->r;
    } else if ((*v)->x < x) {
      v = &(*v)->l;
    }
  }
  wkey_ptr vv = *v;

  while (vv != NULL) {
    if (vv->x == x) {
      return vv;
    } else if (vv->x > x) {
      vv = vv->r;
    } else if (vv->x < x) {
      vv = vv->l;
    }
  }
  wkey_ptr u = alloc_wkey();
  //fprintf (stderr, "watchcat allocated (%d %d %p)\n", x, y, p);
  tr->size++;
  u->x = x;
  u->y = y;
  u->parent = p;
  trp_split (*v, x, &u->l, &u->r);
  *v = u;

  return u;
}

inline void wkey_fix (wkey *k, int timeout) {
  if (k->next_time != NULL) {
    del_entry_time (k);
  }
  add_entry_time (k, timeout);
}

inline void del_wkey (wkey *k) {
  watchcat *w = k->parent;

  if (k->next_time != NULL) {
    del_entry_time (k);
  }

  trp_del (&w->keys, k->x);

  //TODO can we replace next line with if (w->keys.root == NULL) and delete size from set
  if (w->keys.size == 0) {
    watchcat *q = watchcat_q_del (w);

    if (q->next == q) {
      hset_llp_del (&h_watchcat_q, &q->id);
      free_watchcat_q (q);
    }

    if (my_verbosity > 1) {
      fprintf (stderr, "Del watchcat %lld\n", w->id);
    }

    hset_llp_del (&h_watchcat, &w->id);
    free_watchcat (w);
  }
}

/***
  Watchcats
 ***/

inline size_t get_watchcat_size_d (void) {
  return sizeof (watchcat);
}

inline size_t get_watchcat_size_q (void) {
  return offsetof (watchcat, keys);
}

inline watchcat *alloc_watchcat() {
  int mem = get_watchcat_size_d();
  watchcats_cnt++;
  watchcats_memory += mem;

  watchcat *res = dl_malloc (mem);
  res->query = NULL;
  return res;
}

inline void free_watchcat (watchcat *w) {
  int mem = get_watchcat_size_d();
  watchcats_cnt--;
  watchcats_memory -= mem;

  if (w->query != NULL) {
    free_watchcat_query (w->query);
    w->query = NULL;
  }
  dl_free (w, mem);
}

inline watchcat *alloc_watchcat_q (void) {
  return dl_malloc (get_watchcat_size_q());
}

inline void free_watchcat_q (watchcat *w) {
  dl_free (w, get_watchcat_size_q());
}

inline watchcat *get_watchcat_q (long long id, int force) {
  watchcat **b;
  if (force) {
    b = (watchcat **)hset_llp_add (&h_watchcat_q, &id);
    if (*b == (watchcat *)&id) {
      watchcat *w = alloc_watchcat_q();

      w->id = id;
      w->vn = 0;
      w->next = w->prev = w;

      *b = w;
    }
    return *b;
  } else {
    b = (watchcat **)hset_llp_get (&h_watchcat_q, &id);
    return b == NULL ? NULL : *b;
  }
}

inline void watchcat_q_add (watchcat *f, watchcat *e) {
  watchcat *b = f->prev;

  e->next = f;
  f->prev = e;

  e->prev = b;
  b->next = e;

  e->parent = f;
  if (f->vn < 2000000000) {
    f->vn++;
  }
}


inline watchcat *watchcat_q_del (watchcat *e) {
  e->next->prev = e->prev;
  e->prev->next = e->next;

  e->parent->vn--;

  return e->next;
}

inline watchcat *get_watchcat (long long id, watchcat_query_t *query) {
  watchcat **b;
  b = (watchcat **)hset_llp_add (&h_watchcat, &id);
  if (*b == (watchcat *)&id) {
    watchcat *w = alloc_watchcat();
    w->id = id;
    w->query = query;

    wkey_set_init (&w->keys);

    if (my_verbosity > 1) {
      fprintf (stderr, "watchcat_created id = %lld\n", id);
    }

    int i;
    watchcat *bq = NULL;

    int done = 0;
    for (i = 0; i < query->phrases_cnt && !done; i++) {
      watchcat_query_phrase_t *phrase = &query->phrases[i];
      if (phrase->minus_flag) {
        continue;
      }
      int j;
      for (j = 0; j < phrase->words && !done; j++) {
        watchcat *q = get_watchcat_q (phrase->H[j], 1);
        if (bq == NULL || bq->vn > q->vn) {
          bq = q;
        }
        if (bq->vn == 0) {
          done = 1;
        }
      }
    }

    assert (bq != NULL);
    if (my_verbosity > 1) {
      fprintf (stderr, "added to %lld queue\n", bq->id);
    }
    watchcat_q_add (bq, w);

    *b = w;
  } else {
    free_watchcat_query (query);
  }
  return *b;
}

/***
  Timeouts
 ***/

#define TIME_TABLE_RATIO_EXP (4)
#define TIME_TABLE_SIZE_EXP (18 - TIME_TABLE_RATIO_EXP)
#define TIME_TABLE_SIZE (1 << TIME_TABLE_SIZE_EXP)
#define TIME_TABLE_MASK (TIME_TABLE_SIZE - 1)
#define GET_TIME_ID(x) (((unsigned int)(x) >> TIME_TABLE_RATIO_EXP) & TIME_TABLE_MASK)
#define MAX_TIME_GAP ((60 * 60) >> TIME_TABLE_RATIO_EXP)

int last_del_time;
wkey *time_st[TIME_TABLE_SIZE];

inline void del_entry_time (wkey *entry) {
  entry->next_time->prev_time = entry->prev_time;
  entry->prev_time->next_time = entry->next_time;
}

inline void add_entry_time (wkey *entry, int timeout) {
  int new_timeout = max (get_utime (CLOCK_MONOTONIC) + timeout, entry->timeout);

  entry->timeout = new_timeout;

  wkey *f = time_st[GET_TIME_ID (new_timeout)];
  wkey *y = f->prev_time;

  entry->next_time = f;
  f->prev_time = entry;

  entry->prev_time = y;
  y->next_time = entry;
}

void free_by_time (int mx) {
  int en = GET_TIME_ID (get_utime (CLOCK_MONOTONIC));
  wkey *st = time_st[last_del_time];

  while (en - last_del_time > MAX_TIME_GAP || last_del_time - en > TIME_TABLE_SIZE - MAX_TIME_GAP ||
         (mx-- && last_del_time != en)) {
    if (st->next_time != st) {
      if (my_verbosity > 1) {
        fprintf (stderr, "del entry %p by time (key = %d) gap = %d\n", st->next_time, st->next_time->x, en - last_del_time);
      }
      del_wkey (st->next_time);
    } else {
      if (++last_del_time == TIME_TABLE_SIZE) {
        last_del_time = 0;
      }
      st = time_st[last_del_time];
    }
  }
}

/***
  Process keys
 ***/

long long prep_lbuf_res[MAX_NAME_SIZE];

int gen_hashes (char *x) {
  char *v = prepare_watchcat_str (x, 0);
  if (v == NULL) {
    prep_lbuf_res[0] = 0;
    return 0;
  }

  int i;

  long long *u = prep_lbuf_res;
  int un = 0;

  for (i = 0; v[i]; ) {
    long long h = 3213211;

    while (v[i] && v[i] != '+') {
      h = h * 999983 + v[i];
      i++;
    }
    if (v[i]) {
      i++;
    }

    if (h == 0) {
      h = 1;
    }
    u[un++] = h;
  }

  assert (un < MAX_NAME_SIZE);

  dl_qsort_ll (u, un);
  un = dl_unique_ll (u, un);

  return un;
}

void subscribe_watchcat (long long id, char *s, int q_id, int timeout) {
  if (my_verbosity > 1) {
    fprintf (stderr, "subscribe wathcat %lld (%s), q_id = %d, timeout = %d\n", id, s, q_id, timeout);
  }


  watchcat_query_t *query = create_watchcat_query (s);
  if (watchcat_query_hash_impl (query) == -1) {
    free_watchcat_query (query);
    return;
  }
  watchcat *w = get_watchcat (id, query);

  wkey *k = trp_add (&w->keys, q_id, rand(), w);
  wkey_fix (k, timeout);
}

// v inside u
// TODO: uncomment
inline int check (long long *v, int vn, long long *u, int un) {
  int i, j;

  for (i = 0, j = 0; i < vn; i++) {
    while (j + 1 < un && u[j] < v[i]) {
      j++;
    }
    if (u[j] != v[i]) {
      return 0;
    }
  }
  return 1;
  /*
  for (i = 0; i < vn; i++) {
    int l = -1, r = un, c;
    while (l + 1 < r) {
      c = (l + r) / 2;
      if (u[c] <= v[i]) {
        l = i;
      } else {
        r = i;
      }
    }
    if (l == -1) {
      return 0;
    }
  }
  return 1;
  */
}



addr ans[MAX_ANS];
int ans_n;

void dfs (wkey *v, long long id) {
  if (v == NULL || ans_n >= MAX_ANS) {
    return;
  }
  ans[ans_n].w_id = id;
  ans[ans_n].q_id = v->x;
  ans_n++;

  dfs (v->l, id);
  dfs (v->r, id);
}

void add_to_ans (watchcat *w) {
  if (my_verbosity > 2) {
    fprintf (stderr, "add to ans %p\n", w);
  }
  dfs (w->keys.root, w->id);
}

watchcat_entry_t Entry;
void process_entry (watchcat_entry_t *entry) {
  int i;
  for (i = 0; i < entry->n; i++) {
    if (i == 0 || entry->by_hash[i].word != entry->by_hash[i - 1].word) {
      watchcat *st = get_watchcat_q (entry->by_hash[i].word, 0), *q;
      if (st != NULL) {
        q = st;
        assert (q->next != st);
        while (q->next != st) {
          q = q->next;

          if (check_watchcat_query (entry, q->query)) {
            add_to_ans (q);
          }
        }
      }
    }
  }
}

char *gen_addrs (char *s) {
  // s == text 0x1 info
  char *t = s;
  int f = 0;
  while (*t) {
    if (*t++ == 1) {
      f = 1;
      t[-1] = 0;
      break;
    }
  }

  watchcat_prepare_entry (&Entry, s, strlen (s));

  if (f) {
    t[-1] = 1;
  } else {
    t = s;
  }

  ans_n = 0;
  process_entry(&Entry);
  dl_qsort_addr (ans, ans_n);
  return t;
}

void init_all (void) {
  hset_llp_init (&h_watchcat);
  hset_llp_init (&h_watchcat_q);

  int i;
  for (i = 0; i < TIME_TABLE_SIZE; i++) {
    time_st[i] = alloc_wkey();
    time_st[i]->next_time = time_st[i]->prev_time = time_st[i];
  }
  keys_cnt = 0;
  keys_memory = 0;

  last_del_time = GET_TIME_ID (get_utime (CLOCK_MONOTONIC));
}
void free_all (void) {
  if (verbosity) {
    keys_cnt += TIME_TABLE_SIZE;
    int i;
    for (i = 0; i < TIME_TABLE_SIZE; i++) {
      while (time_st[i]->next_time != time_st[i]) {
        del_wkey (time_st[i]->next_time);
      }
      free_wkey (time_st[i]);
    }
    assert (keys_cnt == 0);

    while (wkey_mem != NULL) {
      wkey *k = wkey_mem;
      wkey_mem = wkey_mem->l;
      dl_free (k, sizeof (wkey));
    }

    hset_llp_free (&h_watchcat);
    hset_llp_free (&h_watchcat_q);
  }
}
