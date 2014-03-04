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
              2009-2013 Nikolai Durov (original news-merge.c)
              2009-2013 Andrei Lopatin (original news-merge.c)
              2011-2013 Vitaliy Valtman
*/

#define	_FILE_OFFSET_BITS	64

#include <assert.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>

#include "net-events.h"
#include "net-buffers.h"
#include "net-connections.h"
#include "net-memcache-server.h" 

#include "mc-proxy-news-recommend-extension.h"
#include "net-memcache-client.h"
#include "net-parse.h"
#include "mc-proxy.h"

#define	SPLIT_FACTOR	100
#define	COMM_SPLIT_FACTOR	100

#define	PHP_MAX_RES	10000

/*
 *
 *		NEWS MERGE ENGINE
 *
 */

extern int verbosity;
static int split_factor = SPLIT_FACTOR;

/* stats counters */

#define STATS_BUFF_SIZE	(1 << 20)
//static int stats_buff_len;
//static char stats_buff[STATS_BUFF_SIZE];


/* merge data structures */

typedef struct news_item item_t;

#pragma pack(push,4)
struct news_id {
  int type;
  int owner;
  int place;
};

struct news_item {
  struct news_id id;
  int nusers;
  double weight;
};
#pragma pack(pop)

typedef struct gather_heap_entry {
  struct news_id id;
  int *cur, *last;
  int remaining;
} gh_entry_t;

static gh_entry_t GH_E[MAX_CLUSTER_SERVERS];
static gh_entry_t *GH[MAX_CLUSTER_SERVERS+1];
static int GH_N, GH_total;

static void clear_gather_heap (int mode) {
  GH_N = 0;
  GH_total = 0;
}

static inline void load_heap_v (gh_entry_t *H) {
  int *data = H->cur;
  H->id.type = data[0];
  H->id.owner = data[1];
  H->id.place = data[2];
}

static int news_id_compare (struct news_id id1, struct news_id id2) {
  if (id1.type != id2.type) { return id1.type - id2.type; }
  if (id1.owner != id2.owner) { return id1.owner - id2.owner; }
  return id1.place - id2.place;
}

static int gather_heap_insert (struct gather_entry *GE) {
  int cnt, cnt2, sz;
  gh_entry_t *H;
  assert (GH_N < MAX_CLUSTER_SERVERS);
  if (GE->num <= 0 || GE->res_read < 8 || !GE->data) {
    return 0;
  }
  cnt2 = cnt = (GE->res_read >> 2);
  sz = 6;
  cnt /= sz;
  cnt2 = cnt * sz;
  if (verbosity >= 3) {
    fprintf (stderr, "gather_heap_insert: %d elements (size %d)\n", cnt, (GE->res_read >> 2) - 1);
  }
  if (cnt <= 0) { 
    return 0;
  }
  GH_total += cnt;

  H = &GH_E[GH_N];
  H->remaining = cnt;
  H->cur = GE->data;
  H->last = GE->data + GE->res_bytes / 4 + 1;
  load_heap_v (H);

  int i = ++GH_N, j;
  struct news_id id = H->id;
  while (i > 1) {
    j = (i >> 1);
    if (news_id_compare (GH[j]->id, id) <= 0) { break; }
    GH[i] = GH[j];
    i = j;
  }
  GH[i] = H;

  return 1;
}

static int *get_gather_heap_head (void) {
  return GH_N ? GH[1]->cur : 0;
}

static void gather_heap_advance (void) {
  gh_entry_t *H;
  int sz = 6;
  if (!GH_N) { return; }
  H = GH[1];
  H->cur += sz;
  if (!--H->remaining) {
    H = GH[GH_N--];
    if (!GH_N) { return; }
  } else {
    if (H->cur >= H->last) {
      assert (0);
    }
    load_heap_v (H);
  }
  int i = 1, j;
  struct news_id id = H->id;
  while (1) {
    j = i*2;
    if (j > GH_N) { break; }
    if (j < GH_N && news_id_compare (GH[j + 1]->id, GH[j]->id) < 0) {j ++; }
    if (news_id_compare (id, GH[j]->id) <= 0) { break; }
    GH[i] = GH[j];
    i = j;
  }
  GH[i] = H;
}
/* end (merge structures) */

struct union_heap_entry {
  struct news_item entry;
};

int UH_allocated;
int UH_limit;
int UH_size;
struct union_heap_entry *UH;
int UH_total;

static void union_heap_init (int size) {
  if (UH_allocated < size) {
    if (UH_allocated) {
      free (UH);
    }
    UH = malloc (sizeof (struct union_heap_entry) * (size + 1));
    assert (UH);
    UH_allocated = size;
  }
  assert (size <= UH_allocated);
  UH_limit = size;
  UH_size = 0;
  UH_total = 0;
}

static void union_heap_insert (struct news_item x) {
  assert (UH_limit >= UH_size);
  if (UH_limit == UH_size) {
    if (UH[1].entry.weight > x.weight) {
      return;
    }
    int i = 1, j;
    while (1) {
      j = i*2;
      if (j > UH_size) { break; }
      if (j < UH_size && UH[j + 1].entry.weight < UH[j].entry.weight) {j ++; }
      if (x.weight <= UH[j].entry.weight) { break; }
      UH[i] = UH[j];
      i = j;
    }
    UH[i].entry = x;
  } else {
    int i = ++UH_size, j;
    while (i > 1) {
      j = (i >> 1);
      if (UH[j].entry.weight < x.weight) { break; }
      UH[i] = UH[j];
      i = j;
    }
    UH[i].entry = x;
  }
}

static void union_heap_to_array (void) {  
  int p;
  UH_total = UH_size;
  for (p = UH_size; p >= 2; p--) {
    struct news_item x = UH[p].entry;
    assert (UH[1].entry.weight <= x.weight);
    UH[p].entry = UH[1].entry;
    UH_size --;
    UH_limit = UH_size;
    union_heap_insert (x);
  }
}

static inline int flush_output (struct connection *c) {
  return MCC_FUNC (c)->flush_query (c);
}
    
/* -------- LIST GATHER/MERGE ------------- */

int compare_weight (const void *a, const void *b) {
  const struct news_item *A = a;
  const struct news_item *B = b;
  if (A->weight < B->weight) { return 1; }
  if (A->weight > B->weight) { return -1; }
  return 0;
}

int newsr_merge_end_query (struct connection *c, const char *key, int key_len, void *E, struct gather_entry *data, int tot_num) {
  struct newsr_gather_extra *extra = E;
  int Q_raw = extra->raw;
  int Q_limit = extra->max_res;
  int res = 0;

  clear_gather_heap (0);
  int i;
  for (i = 0; i < tot_num; i++) if (data[i].res_bytes > 0) {
  	//fprintf (stderr, "!!!");
    gather_heap_insert (&data[i]);
  } else {
    if (verbosity >= 4) {
      fprintf (stderr, "Dropping result %d (num = %d)\n", i, data[i].num);
    }
  }
  
  //static struct news_item R[PHP_MAX_RES * 10];
  union_heap_init (Q_limit);
  res = 0;
  struct news_item last_item = {.id.type = 0, .id.owner = 0, .id.owner = 0, .weight = 0};
  //long long last_res = 0;
  //while (res < PHP_MAX_RES * 10 - 1) {
  while (1) {
    struct news_item *t = (void *)get_gather_heap_head ();
    if (!t) {
      if (res && last_item.nusers >= extra->user_limit) {
        union_heap_insert (last_item);
      }
      break;
    }
    vkprintf (4, "type = %d, owner = %d, place = %d\n", t->id.type, t->id.owner, t->id.place);
    if (res && !news_id_compare (last_item.id, t->id)) {
      assert (res > 0);
      last_item.weight += t->weight;
      last_item.nusers += t->nusers;
    } else {
      if (res > 0 && last_item.nusers >= extra->user_limit) {
        union_heap_insert (last_item);
      }
      res ++; 
      last_item = *t;
    }
    gather_heap_advance ();
  }
  
  union_heap_to_array ();
  //qsort (R, res, sizeof (struct news_item), compare_weight);
  assert (res >= UH_total);
  res = UH_total;
  for (i = 0; i < res; i++) {
    vkprintf (4, "Item #%d: weight = %lf\n", i, UH[i + 1].entry.weight);
  }


  write_out (&c->Out, "VALUE ", 6);
  write_out (&c->Out, key, key_len);

  char *ptr = get_write_ptr (&c->Out, 512);
  if (!ptr) return -1;
  char *s = ptr + 448;

  memcpy (ptr, " 0 .........\r\n", 14);
  char *size_ptr = ptr + 3;
  ptr += 14;
  int w = 0, t;
  if (!Q_raw) {
    ptr += w = sprintf (ptr, "%d", res);
  } else {
    *((int *) ptr) = res;
    ptr += w = 4;
  }

  for (i = 0; i < Q_limit && i < res; i ++) {
    if (ptr >= s) {
      advance_write_ptr (&c->Out, ptr - (s - 448));
      ptr = get_write_ptr (&c->Out, 512);
      if (!ptr) return -1;
      s = ptr + 448;
    }
    
    if (!Q_raw) {
      *ptr++ = ',';  w++;
      w += t = sprintf (ptr, "%d,%d,%d", UH[i + 1].entry.id.type, UH[i + 1].entry.id.owner, UH[i + 1].entry.id.place);
      ptr += t;
    } else {
      memcpy (ptr, &UH[i], sizeof (struct news_item));
      w += sizeof (struct news_item);
      ptr += sizeof (struct news_item);
    }
  }
  size_ptr[sprintf (size_ptr, "% 9d", w)] = '\r';
  memcpy (ptr, "\r\n", 2);
  ptr+=2;
  advance_write_ptr (&c->Out, ptr - (s - 448));
  return 0;
}


/* -------- END (LIST GATHER/MERGE) ------- */


#define	MAX_QUERY	131072
static int QL, Q[MAX_QUERY], QN[MAX_QUERY];

#define NEWSR_STORE_MAGIC 0x637f8894
struct keep_mc_store {
  int magic;
  int len;
  int tag;
  char text[0];
};

struct keep_mc_store mc_store;

int newsr_data_store (struct connection *c, int op, const char *key, int len, int flags, int expire, int bytes) {
  //int user_id;
  int act = 0;

  //key[len] = 0;

  if (verbosity > 0) {
    fprintf (stderr, "mc_store: op=%d, key=\"%s\", flags=%d, expire=%d, bytes=%d, noreply=%d\n", op, key, flags, expire, bytes, 0);
  }

  if (bytes >= 0 && bytes < 1048576) {
    if (!c->Tmp) {
      c->Tmp = alloc_head_buffer();
      assert (c->Tmp);
    }
    mc_store.magic = NEWSR_STORE_MAGIC;
    int tag;
    if (sscanf (key, "urlist%d", &tag) != 1 || tag >= 0) {
      act = 1;
    }
    if (!act) {
      mc_store.tag = tag;
      int s = np_news_parse_list (Q, MAX_QUERY, 2, &c->In, bytes);
      if (s >= 1) {
        mc_store.len = s;
        write_out (c->Tmp, &mc_store, sizeof (struct keep_mc_store));
        write_out (c->Tmp, Q, s * 8);
      } else {
        act = 1;
      }
    }
  } else {
    act = 1;
  }

  if (act) {
    write_out (&c->Out, "NOT_STORED\r\n", 12);
    flush_output (c);
  } else {
    write_out (&c->Out, "STORED\r\n", 8);
    flush_output (c);
  }

  return !act;
}

#define MAX_RES	131072

//static int R_cnt, *R_end;
static int R[MAX_RES + 1];
static int Rlen[MAX_RES];
static int Rfirst[MAX_RES];
static int R_common_len;
int newsr_generate_preget_query (char *buff, const char *key, int key_len, void *E, int n) {
  if (!Rlen[n]) return 0;
  struct newsr_gather_extra* extra = E;
  int len = Rlen[n];
  int j, x = Rfirst[n], k = R_common_len;
  assert (R[0] == 0x31303030);
  for (j = 0; j < len; j++) {
    assert (x >= 0);
    assert (k + 2 <= MAX_RES + 1);
    R[k++] = Q[2 * x + 0];
    R[k++] = Q[2 * x + 1];
    x = QN[x];
  }
	
  assert (x == -1 && len > 0);
  const int data_len = k << 2;
  int r = sprintf (buff, "set urlist%d 0 0 %d\r\n", extra->request_tag, data_len);
  memcpy (buff + r, R, data_len);
  r += data_len;
  return r;
}

int newsr_generate_new_key (char *buff, char *key, int len, void *E) {
  struct newsr_gather_extra *extra = E;
  int r = 0;
  r = sprintf (buff, "%%raw_recommend_updates%d_%d,%d_%d_%d_%d:%d", extra->mask, extra->st_time, extra->end_time, extra->id, extra->t, extra->timestamp, extra->request_tag);
  return r;
}

static void set_rlen () {
  int i, x;

  for (i = 0; i < CC->tot_buckets; i++) {
    Rlen[i] = 0;
    Rfirst[i] = -1;
  }


  split_factor = CC->tot_buckets ? CC->tot_buckets : 0;
  R[0] = 0x31303030;
  R_common_len = 1;
  for (i = QL-1; i >= 0; i--) {
    if (Q[2 * i + 1] < 0) {
      assert (R_common_len + 2 <= MAX_RES + 1);
      R[R_common_len++] = Q[2*i];
      R[R_common_len++] = Q[2*i+1];
    } else {
      x = Q[2 * i];

      if (x < 0) { x = -x; }
      x %= split_factor;

      if (x < CC->tot_buckets) {
        QN[i] = Rfirst[x];
        Rfirst[x] = i;
        Rlen[x] ++;
      }
    }
  }
}



static int QLsave = 0, tagsave = 0, Qsave[MAX_QUERY];

void newsr_load_saved_data (struct connection *c) {
  struct keep_mc_store *Data = 0;
  
  if (!c->Tmp) {
    tagsave = 0;
    vkprintf (4, "newsr_load_saved_data: !c->Tmp\n");
    return;
  }

  nb_iterator_t R;
  nbit_set (&R, c->Tmp);
  int l = nbit_read_in (&R, &mc_store, sizeof (struct keep_mc_store));
  if (l != sizeof (struct keep_mc_store)) {
    tagsave = 0;
    vkprintf (4, "newsr_load_saved_data: l = %d\n", l);
    return;
  }
  Data = &mc_store;
  assert (Data->magic = NEWSR_STORE_MAGIC);

  QLsave = 0;
  tagsave = Data->tag;

  QLsave = (Data->len < MAX_QUERY ? Data->len : MAX_QUERY);
  assert (nbit_read_in (&R, Qsave, QLsave * 8) == QLsave * 8);
}

void *newsr_store_gather_extra (const char *key, int key_len) {
  int mask = -1, timestamp = 0, st_time = 0, end_time = 0, id = 0, raw = 0, t, x = 0, tag, limit = 0, user_limit = 0;
  char sign_timestamp[2];
  sign_timestamp[0] = '>';

  if (*key == '%') {
    raw = 1;
  }

  if ((sscanf (key + raw, "raw_recommend_updates%d_%d,%d_%d_%d_%d:%n", &mask, &st_time, &end_time, &id, &t, &timestamp, &x) == 6) && x > 0) {
    x += raw;
  } else if ((sscanf (key + raw, "recommend_updates%d_%d,%d_%d_%d>%d:%n", &mask, &st_time, &end_time, &id, &t, &timestamp, &x) == 6) && x > 0) {
    x += raw;
  } else if ((sscanf (key + raw, "recommend_updates%d_%d,%d_%d_%d:%n", &mask, &st_time, &end_time, &id, &t, &x) == 5) && x > 0) {
    x += raw;
  } else if ((sscanf (key + raw, "recommend_updates%d_%d,%d_%d_%d#%d:%n", &mask, &st_time, &end_time, &id, &t, &limit, &x) == 6) && x > 0) {
    x += raw;
  } else if ((sscanf (key + raw, "recommend_updates%d_%d,%d_%d_%d%1[<>]%d#%d:%n", &mask, &st_time, &end_time, &id, &t, sign_timestamp, &timestamp, &limit, &x) == 8) && x > 0) {
    x += raw;
  } else if ((sscanf (key + raw, "recommend_updates%d_%d,%d_%d_%d%1[<>]%d#%d@%d:%n", &mask, &st_time, &end_time, &id, &t, sign_timestamp, &timestamp, &limit, &user_limit, &x) == 9) && x > 0) {
    x += raw;
  }

  if (timestamp < 0) {
    return 0;
  }

  if (sign_timestamp[0] == '<') {
    timestamp *= -1;
  }

  if (!x) {
    vkprintf (4, "Error in parse: no correct query\n");
    return 0;
  }

  if (limit <= 0 || limit > PHP_MAX_RES) {
    limit = PHP_MAX_RES;
  }

  char *ptr;
  Q[0] = strtol (key+x, &ptr, 10);
  if (Q[0] < 0 && ptr == key + key_len && QLsave && Q[0] == tagsave) {
    tag = tagsave;
    QL = QLsave;
    memcpy (Q, Qsave, QL * 8);
  } else {
    QL = np_parse_list_str (Q, MAX_QUERY, 2, key + x, key_len - x);
    tag = lrand48() | (-1 << 30) | 1;
  }
  vkprintf (4, "newsr_store_gather_extra: tag = %d, QL = %d, tagsave = %d\n", tag, QL, tagsave);

  if (QL <= 0) {
    vkprintf (4, "Error in parse: can not parse list, QL = %d\n", QL);
    return 0;
  }

  if (end_time <= 0) {
    end_time = 0x7fffffff;
  }

  if (tag >= 0 || !mask) {
    vkprintf (4, "Tag = %d, mask = %d\n", tag, mask);
    return 0;
  }

  set_rlen ();

  struct newsr_gather_extra* extra = zzmalloc (sizeof (struct newsr_gather_extra));
  extra->request_tag = tag;
  extra->mask = mask;
  extra->timestamp = timestamp;
  extra->st_time = st_time;
  extra->end_time = end_time;
  extra->max_res = limit;
  extra->id = id;
  extra->t = t;
  extra->raw = raw;
  extra->user_limit = user_limit;
  return extra;
}

int newsr_check_query (int type, const char *key, int key_len) {
  if (verbosity) {
    fprintf (stderr, "newsr_check: type = %d, key = %s, key_len = %d\n", type, key, key_len);
  }
  int l = eat_at (key, key_len);
  key += l;
  key_len -= l;
  if (type == mct_get) {
    return (key_len >= 22 && (!strncmp (key, "raw_recommend_updates", 21) || !strncmp (key, "%raw_recommend_updates", 22))) ||
           (key_len >=  8 && (!strncmp (key, "recommend_updates", 17) || !strncmp (key, "%recommend_updates", 18)));
  } else if (type == mct_set || type == mct_replace || type == mct_add) {
    return (key_len >= 6 && !strncmp (key, "urlist", 6));
  } else {
    return 1;
  }
}


int newsr_free_gather_extra (void *E) {
  zzfree (E, sizeof (struct newsr_gather_extra));
  return 0;
}

int newsr_use_preget_query (void *extra) {
  vkprintf (4, "newsr: request_tag = %d\n", ((struct newsr_gather_extra *)extra)->request_tag);
  return ((struct newsr_gather_extra *)extra)->request_tag;
}

struct mc_proxy_merge_functions newsr_extension_functions = {
  .free_gather_extra = newsr_free_gather_extra,
  .merge_end_query = newsr_merge_end_query,
  .generate_preget_query = newsr_generate_preget_query,
  .generate_new_key = newsr_generate_new_key,
  .store_gather_extra = newsr_store_gather_extra,
  .check_query = newsr_check_query,
  .merge_store = newsr_data_store,
  .load_saved_data = newsr_load_saved_data,
  .use_preget_query = newsr_use_preget_query
};

struct mc_proxy_merge_conf newsr_extension_conf = {
  .use_at = 1,
  .use_preget_query = 1
};
