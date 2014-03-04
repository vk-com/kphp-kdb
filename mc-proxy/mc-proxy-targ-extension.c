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
              2009-2013 Nikolai Durov (original targ-merge.c)
              2009-2013 Andrei Lopatin (original targ-merge.c)
              2011-2013 Vitaliy Valtman
*/

#define	_FILE_OFFSET_BITS	64
#include <assert.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/types.h>
#include <aio.h>
#include <netdb.h>

#include "net-connections.h"
#include "net-memcache-server.h"
#include "net-memcache-client.h"
#include "net-buffers.h"
#include "net-parse.h"
#include "net-events.h"
#include "mc-proxy-targ-extension.h"
#include "estimate-split.h"

#define MAX_QUERY 65536
#define PHP_MAX_RES 60000
#define TARG_STORE_MAGIC 0x54389534
struct keep_mc_store {
  int magic;
  int tag;
  int len;
  int act;
  char text[0];
};

#define Q_SORT_MASK 255
#define Q_DOUBLE (1 << 8)
#define Q_REVERSE (1 << 9)
#define Q_RAW (1 << 10)
#define Q_SUM_LIST (1 << 11)
#define Q_SEARCH (1 << 12)
#define Q_RECEIVE_RAW (1 << 13)
#define Q_SECOND (1 << 14)
#define Q_UNION (1 << 15)
#define Q_IGNORE_SORT_MASK (1 << 16)
#define Q_NEED_SORT (1 << 17)


struct keep_mc_store mc_store;
int Q[2 * MAX_QUERY + 1], Qsave[2 * MAX_QUERY + 1];
int tag_save, QLsave;
int Q_order, Q_raw, Q_limit;

#define TARG_GATHER_MAGIC 0x89034
struct targ_gather_extra {
  int magic;
  int flags;
  int slice_limit;
  int limit;
  int extra_start;
  int extra_end;
};

extern int verbosity;


static inline int flush_output (struct connection *c) {
//  fprintf (stderr, "flush_output (%d)\n", c->fd);
  return MCC_FUNC (c)->flush_query (c);
}

static inline unsigned long long make_value64 (int value, int x) {
  unsigned int a = value; a -= (unsigned int) INT_MIN;
  unsigned int b = x; b -= (unsigned int) INT_MIN;
  return (((unsigned long long) a) << 32) | b;
}

typedef struct gather_heap_entry {
  unsigned long long value64;
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
  if (Q_order & Q_UNION) {
    int *data = H->cur;
    int x /*, value*/;
    x = data[0];
    //value = (Q_order & Q_DOUBLE) ? data[1] : x;

    //if (Q_order & Q_SECOND) {
    //  int t = value; value = x; x = t;
    //}
    if (Q_order & Q_REVERSE) {
      /* -(-2147483648) == -2147483648 */
      H->value64 = x;
      //H->value64 = make_value64 (-(value+1),-(x+1));
      //H->value64 = make_value64 (-(value+1),-(x+1));
    } else {
      H->value64 = -x;
      //H->value64 = make_value64 (value, x);
    }
  } else {
    assert (Q_order & Q_SEARCH);
    int *data = H->cur;
    int x, value;
    x = data[0];
    value = (Q_order & Q_DOUBLE) ? data[1] : x;
  
    if (Q_order & Q_REVERSE) {
      /* -(-2147483648) == -2147483648 */
      H->value64 = make_value64 (-(value+1),-(x+1));
    } else {
      H->value64 = make_value64 (value, x);
    }
  }
}



static int gather_heap_insert (struct gather_entry *GE) {
  int cnt, cnt2, sz;
  gh_entry_t *H;
  assert (GH_N < MAX_CLUSTER_SERVERS);
  if (GE->num <= 0 || GE->res_read < 4 || !GE->data) {
    vkprintf (4, "num = %d, res_read = %d, data = %p\n", GE->num, GE->res_read, GE->data);
    return 0;
  }
  cnt2 = cnt = (GE->res_read >> 2) - 1;
  sz = (Q_order & Q_DOUBLE) ? 2 : 1;
  
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
  unsigned long long value64 = H->value64;
  while (i > 1) {
    j = (i >> 1);
    if (GH[j]->value64 <= value64) { break; }
    GH[i] = GH[j];
    i = j;
  }
  GH[i] = H;

  return 1;
}

static int *get_gather_heap_head (void) {
	if (verbosity >= 4) {
		fprintf (stderr, "get_gather_heap_head: GH->last = %p, top_int = %d\n", GH[1]->last, GH[1]->cur ? *GH[1]->cur : -1 );
	}
  return GH_N ? GH[1]->cur : 0;
}

static void gather_heap_advance (void) {
  gh_entry_t *H;
  int sz;
  sz = (Q_order & Q_DOUBLE) ? 2 : 1;

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
  unsigned long long value64 = H->value64;
  while (1) {
    j = i*2;
    if (j > GH_N) { break; }
    if (j < GH_N && GH[j+1]->value64 < GH[j]->value64) { j++; }
    if (value64 <= GH[j]->value64) { break; }
    GH[i] = GH[j];
    i = j;
  }
  GH[i] = H;
}

static void gather_sum_res (struct gather_entry *GE) {
  int cnt = (GE->res_read >> 2) - 1;
  int *ptr = GE->data;
  int i;

  if (cnt > PHP_MAX_RES) { cnt = PHP_MAX_RES; }
  if (cnt > Q_limit) { Q_limit = cnt; }
  for (i = 0; i < cnt; i++) {
    Q[i] += *ptr++;
  }
}

static int second_int_compare (const void *a, const void *b) {
  return *(((const int *)a) + 1) - *(((const int *)b) + 1);
}

static int second_int_compare_reverse (const void *a, const void *b) {
  return *(((const int *)b) + 1) - *(((const int *)a) + 1);
}

static int first_int_compare (const void *a, const void *b) {
  return *(((const int *)a)) - *(((const int *)b));
}

//static int first_int_compare_reverse (const void *a, const void *b) {
//  return *(((const int *)b)) - *(((const int *)a));
//}

static int targ_merge_end_query (struct connection *c, const char *key, int key_len, void *E, struct gather_entry *data, int tot_num) {
  struct targ_gather_extra *extra = E;
  static char buff[2048];
  int w, i;
  int res;
  char *ptr, *size_ptr, *s;
  int len = key_len;

  Q_limit = extra->limit;
  Q_raw = extra->flags & Q_RAW;
  Q_order = extra->flags;
  int sum_mode = extra->flags & Q_SUM_LIST;
  int union_mode = extra->flags & Q_UNION;
  int have_extras = (Q_order & Q_DOUBLE);
  if (!sum_mode && !union_mode) {
    assert (extra->flags & Q_SEARCH);
  }

  if (verbosity >= 4) {
    fprintf (stderr, "Q_order = %d, Q_limit = %d, sum_mode = %d, union_mode = %d\n", Q_order, Q_limit, sum_mode, union_mode);
  }

  if (sum_mode) {
    //if (Q_limit <= 0) { Q_limit = 1000; }
    //if (Q_limit > 10000) { Q_limit = 10000; } 
    Q_limit = 0;
    memset (Q, 0, PHP_MAX_RES * sizeof(int));
    Q_order = 0;
  } else {
    clear_gather_heap (Q_order);
  }

  int union_res = 0;
  if (union_mode && (Q_order & Q_NEED_SORT)) {
    for (i = 0; i < tot_num; i++) if (data[i].num >= 0) {
      assert (Q_order & Q_REVERSE);
      vkprintf (4, "Sorting data\n");
      qsort (data[i].data, (data[i].res_read - 4) / 8,  8, first_int_compare);
    }
  }
  if (union_mode) {
    for (i = 0; i < tot_num; i++) if (data[i].num >= 0 && data[i].res_read >= 4) {
      union_res += data[i].num;
    }
  }
  res = 0;
  for (i = 0; i < tot_num; i++) if (data[i].num >= 0) {
    res += data[i].num;
    assert (data[i].num >= 0);
    if (verbosity >= 4) {
      fprintf (stderr, "Using result %d (num = %d)\n", i, data[i].num);
    }
    if (!sum_mode) {
      vkprintf (4, "Gather heap insert\n");
      gather_heap_insert (&data[i]);
    } else {
      if (Q_limit > PHP_MAX_RES) { Q_limit = PHP_MAX_RES; }
      gather_sum_res (&data[i]);
    }
  } else {
    if (verbosity >= 4) {
      fprintf (stderr, "Dropping result %d (num = %d)\n", i, data[i].num);
    }
  }

  if (verbosity > 0) {
    fprintf (stderr, "result = %d\n", res);
  }
  
  if (union_mode && res > 0) {
    vkprintf (4, "Union merge - start\n");
    assert (have_extras);
    int *last = 0;
    res = 0;
    while (res < PHP_MAX_RES - 1) {
      vkprintf (4, "last = %p, Q = %p, res = %d\n", last, Q, res);
      int *cur = get_gather_heap_head ();
      vkprintf (4, "cur = %p, last = %p, Q = %p, res = %d\n", cur, last, Q, res);
      if (!cur) {
        break;
      }
      vkprintf (4, "cur = %p, last = %p, Q = %p, res = %d, *cur = %d, *last = %d\n", cur, last, Q, res, cur ? *cur : -1, last ? *last : -1);
      if (!last || *last != *cur) {
        last = &Q[2 * (res ++)];
        last[0] = cur[0];
        last[1] = cur[1];
      } else {
        last[1] += cur[1];
      }
      gather_heap_advance ();
    }
    if (Q_order & Q_SECOND) {
      if (!(Q_order & Q_REVERSE)) {
        qsort (Q, res, 8, second_int_compare);
      } else {
        qsort (Q, res, 8, second_int_compare_reverse);
      }
    }
    if (Q_limit > res) {
      Q_limit = res;
    }
    vkprintf (4, "Union merge - success\n");
  }

  if (!Q_limit || !res) {
    w = sprintf (buff, "%d", union_mode ? union_res : res);
    return return_one_key (c, key, buff, w);
  }

  write_out (&c->Out, "VALUE ", 6);
  write_out (&c->Out, key, len);

  ptr = get_write_ptr (&c->Out, 512);
  if (!ptr) return -1;
  s = ptr + 448;

  memcpy (ptr, " 0 .........\r\n", 14);
  size_ptr = ptr + 3;
  ptr += 14;
  if (!Q_raw) {
    ptr += w = sprintf (ptr, "%d", union_mode ? union_res : res);
  } else {
    *((int *) ptr) = union_mode ? union_res : res;
    ptr += w = 4;
  }


  int id_bytes = 4;
  //if (TARG_EXTENSION && key[at_l] == 'p') {
  //  while (Q_limit && !R[Q_limit-1]) { Q_limit--; }
  //}
  for (i = 0; i < Q_limit; ) {
    int t, *Res = !sum_mode ? !union_mode ? get_gather_heap_head () : Q + 2 * i : Q + i;
    if (!Res) { break; }
    
    if (ptr >= s) {
      advance_write_ptr (&c->Out, ptr - (s - 448));
      ptr = get_write_ptr (&c->Out, 512);
      if (!ptr) return -1;
      s = ptr + 448;
    }
    
    if (!Q_raw) {
      *ptr++ = ',';  w++;
      w += t = sprintf (ptr, "%d", Res[0]);
      ptr += t;
      if (have_extras) {
        w += t = sprintf (ptr, ",%d", Res[1]);
        ptr += t;
      }
    } else {
      t = (have_extras ? id_bytes + 4 : id_bytes);
      memcpy (ptr, Res, t);
      w += t;
      ptr += t;
    }
    if (!sum_mode) {
      gather_heap_advance ();
    }
    i++;
  }
  
  size_ptr[sprintf (size_ptr, "% 9d", w)] = '\r';
  memcpy (ptr, "\r\n", 2);
  ptr+=2;
  advance_write_ptr (&c->Out, ptr - (s - 448));

  return 0;
}

static int targ_free_gather_extra (void *E) {
  zzfree (E, sizeof (struct targ_gather_extra));
  return 0;
}

static int regenerate_search_extra (char *buff, int flags, int slice_limit) {
  int l = 0;
  buff[l ++] = '#';
  if (flags & Q_RECEIVE_RAW) {
    buff[l ++] = '%';
  } else {
    assert (0);
  }
  char sort = flags & Q_SORT_MASK;
  if (sort && !(flags & Q_IGNORE_SORT_MASK)) {
    buff[l ++] = sort;
  }
  if (slice_limit) {
    l += sprintf (buff + l, "%d", slice_limit);
  }
  return l;
}

static int targ_generate_new_key (char *buff, char *key, int key_len, void *E) {
  struct targ_gather_extra *extra = E;
  assert (extra->magic == TARG_GATHER_MAGIC);
  int l = 0;
  memcpy (buff + l, key, extra->extra_start);
  l += extra->extra_start;
  l += regenerate_search_extra (buff + l, extra->flags, extra->slice_limit);
  memcpy (buff + l, key + extra->extra_end, key_len - extra->extra_end);
  l += key_len - extra->extra_end;
  return l;
}


static int targ_generate_preget_query (char *buff, const char *key, int key_len, void *E, int n) {
  if (!tag_save) {
    return 0;
  }
  if (tag_save > 0) {
    int r = sprintf (buff, "set temp%d 0 0 %d\r\n", tag_save, QLsave);
    memcpy (buff + r, Qsave, QLsave);
    return r + QLsave;
  } else if (tag_save < 0) {
    int i;
    int len = 0;
    Q[0] = 0x30303030;
    for (i = 0; i < QLsave; i++) if (Qsave[i] % CC->tot_buckets == n) {
      Q[++len] = Qsave[i];
    }
    int r = sprintf (buff, "set xtemp%d 0 0 %d\r\n", tag_save, 4 * len + 4);
    memcpy (buff + r, Q, 4 * len + 4);
    return r + 4 * len + 4;
  } else {
    return 0;
  }
}

static const char *parse_search_extra_common (const char *key, int key_len, char c, int *limit, int *flags) {
  const char *ptr = key;
  *flags = 0;
  char *qtr;
  *limit = 0;
  while (ptr <= key + key_len) {
    switch (*ptr) {
      case 'i': 
        *flags &= ~Q_REVERSE;
        *flags &= ~Q_DOUBLE;
        *flags = (*flags & ~255) | (unsigned char)*ptr;
        break;
      case 'I':
        *flags |= Q_REVERSE;
        *flags &= ~Q_DOUBLE;
        *flags = (*flags & ~255) | (unsigned char)*ptr;
        break;
      case 'r': 
        *flags &= ~Q_REVERSE;
        *flags |= Q_DOUBLE;
        *flags = (*flags & ~255) | (unsigned char)*ptr;
        break;
      case 'R':
        *flags |= Q_REVERSE;
        *flags |= Q_DOUBLE;
        *flags = (*flags & ~255) | (unsigned char)*ptr;
        break;
      case '%':
        *flags |= Q_RAW;
        break;
      case '0'...'9':
        *limit = strtol (ptr, &qtr, 10);
        ptr = qtr - 1;
        break;
      default:
        if (*ptr == c && ptr == key + key_len) {
          return ptr;
        } else {
          *limit = 0;
          *flags = -1;
          return key;
        }
    }
    ptr ++;
  }
  *limit = 0;
  *flags = -1;
  return key;
}

static int parse_search_extra (const char *key, int key_len, int *limit, int *extra_start, int *extra_end) {
  int flags;
  int search = (key[key_len - 1] == ')');
  if (search) {
    int t = key_len - 1;
    while (t >= 0 && key[t] != '#' && key[t] != '(') {
      t --;
    }
    if (t < 0) {
      return -1;
    }
    if (key[t] == '#') {
      *extra_start = t;
      t ++;
      int flags = -1;
      *extra_end = parse_search_extra_common (key + t, key_len - t - 1, ')', limit, &flags) - key;
      if (flags != -1) {
        return flags;
      }
    }
    t = 0;
    while (t < key_len && key[t] != '(') {
      t ++;
    }
    if (t == key_len) {
      return -1;
    }
    int st = t;
    while (t >= 0 && key[t] != '#') {
      t --;
    }
    if (t < 0) {
      *extra_start = st;
      *extra_end = st;
      return 0;
    }
    *extra_start = t;
    t ++;
    int flags = -1;
    *extra_end = parse_search_extra_common (key + t, st - t, '(', limit, &flags) - key;
    if (flags != -1) {
      return flags;
    }
    return 0;
  } else {
    int t = key_len - 1;
    while (t >= 0 && key[t] != '#') {
      t --;
    }
    if (t < 0) {
      *extra_start = key_len;
      *extra_end = key_len;
      return 0;
    }
    *extra_start = t;
    t ++;
    *extra_end = parse_search_extra_common (key + t, key_len - t, 0, limit, &flags) - key;
    if (flags == -1) {
      flags = 0;
    }
    return flags;
  }
}

static void *targ_store_gather_extra (const char *key, int key_len) {
  struct targ_gather_extra* extra = zzmalloc (sizeof (struct targ_gather_extra));
  extra->magic = TARG_GATHER_MAGIC;
  if (key_len >= 6 && !strncmp (key, "search", 6)) {
    int limit = 0; 
    extra->flags = parse_search_extra (key, key_len, &limit, &extra->extra_start, &extra->extra_end);
    if (extra->flags == -1) {
      zzfree (extra, sizeof (struct targ_gather_extra));
      return 0;
    }
    extra->flags |= Q_RECEIVE_RAW | Q_SEARCH;
    if (limit >= PHP_MAX_RES) {
      limit = PHP_MAX_RES;
    }
    if (limit <= 0) {
      limit = 0;
    }
    extra->limit = limit;
    extra->slice_limit = estimate_split (extra->limit, CC->tot_buckets);
  } else {
    int limit = 0; 
    extra->flags = parse_search_extra (key, key_len, &limit, &extra->extra_start, &extra->extra_end);
    if (extra->flags == -1) {
      zzfree (extra, sizeof (struct targ_gather_extra));
      return 0;
    }
    if (key_len >= 7 && !strncmp (key, "recent_", 7)) {
      extra->flags |= Q_RECEIVE_RAW | Q_UNION;
      if (extra->flags & Q_DOUBLE) {
        extra->flags |= Q_SECOND;
        extra->flags |= Q_NEED_SORT;
      } else {
        extra->flags |= Q_DOUBLE;
        extra->flags |= Q_REVERSE;
      }
    } else {
      extra->flags |= Q_RECEIVE_RAW | Q_SUM_LIST;
    }
    extra->limit = limit;
    extra->slice_limit = limit + 100;
  }
  return extra;
}

static int targ_use_preget_query (void *extra) {
  return tag_save;
}


static int targ_check_query (int type, const char *key, int key_len) {
  if (verbosity) {
    fprintf (stderr, "targ_check: type = %d, key = %s, key_len = %d\n", type, key, key_len);
  }
  int l = eat_at (key, key_len);
  key += l;
  key_len -= l;
  if (type == mct_get) {
    return (key_len >= 6 && !strncmp (key, "target", 6)) || 
           (key_len >= 6 && !strncmp (key, "search", 6)) ||
           (key_len >= 6 && !strncmp (key, "prices", 6)) ||
           (key_len >= 11 && !strncmp (key, "deletegroup", 11)) ||
           (key_len >= 10 && !strncmp (key, "ad_pricing", 10)) ||
           (key_len >= 13 && !strncmp (key, "targ_audience", 13)) ||
           (key_len >= 8 && !strncmp (key, "audience", 8)) ||
           ((key_len >= 3 && !strncmp (key, "ad_", 3)) && !(key_len >= 8 && !strncmp (key, "ad_query", 8)) && !(key_len >= 7 && !strncmp (key, "ad_info", 7))) ||
           (key_len >= 18 && !strncmp (key, "recent_views_stats", 18)) ||
           (key_len >= 17 && !strncmp (key, "recent_ad_viewers", 17));
  } else if (type == mct_set || type == mct_replace || type == mct_add) {
    return (key_len >= 4 && !strncmp (key, "temp", 4)) || (key_len >= 5 && !strncmp (key, "xtemp", 5));
  } else {
    return 1;
  }
}

static int targ_store (struct connection *c, int op, const char *key, int len, int flags, int expire, int bytes) {
  vkprintf (1, "mc_store: op=%d, key=\"%s\", flags=%d, expire=%d, bytes=%d, noreply=%d\n", op, key, flags, expire, bytes, 0);

  int tag = 0;
  int act = 0;
  if (sscanf (key, "temp%d", &tag) >= 1 && tag > 0) {
    act = 1;
  } else if (sscanf (key, "xtemp%d", &tag) >= 1 && tag < 0) {
    act = 2;
  }

  if (act) {
    if (!c->Tmp) {
      c->Tmp = alloc_head_buffer();
      assert (c->Tmp);
    }
    mc_store.magic = TARG_STORE_MAGIC;
    mc_store.tag = tag;
    mc_store.act = act;
    int s = -1;
    if (act == 2) {
      s = np_news_parse_list (Q, MAX_QUERY, 1, &c->In, bytes);
      mc_store.len = s;
    } else if (act == 1) {
      s = bytes;
      assert (read_in (&c->In, Q, s) == s);
      mc_store.len = s;
    }
    if (s >= 0) {
      write_out (c->Tmp, &mc_store, sizeof (struct keep_mc_store));
      if (act == 2) {
        write_out (c->Tmp, Q, s * 4);
      } else if (act == 1) {
        write_out (c->Tmp, Q, s);
      }
    } else {
      act = 0;
    }
  }

  if (!act) {
    write_out (&c->Out, "NOT_STORED\r\n", 12);
    flush_output (c);
  } else {
    write_out (&c->Out, "STORED\r\n", 8);
    flush_output (c);
  }

  return act;
}

static void targ_load_saved_data (struct connection *c) {
  struct keep_mc_store *Data = 0;
  
  if (!c->Tmp) { 
    tag_save = 0;
    return;
  }

  nb_iterator_t R;
  nbit_set (&R, c->Tmp);
  int l = nbit_read_in (&R, &mc_store, sizeof (struct keep_mc_store));
  if (l != sizeof (struct keep_mc_store)) {
    tag_save = 0;
    return; 
  }
  Data = &mc_store;
  assert (Data->magic = TARG_STORE_MAGIC);

  QLsave = 0;
  tag_save = Data->tag;
  //act_save = Data->act;

  QLsave = (Data->len < MAX_QUERY ? Data->len : MAX_QUERY);
  if (tag_save < 0) {
    assert (nbit_read_in (&R, Qsave, QLsave * 4) == QLsave * 4);
  } else if (tag_save > 0) {
    assert (nbit_read_in (&R, Qsave, QLsave) == QLsave);
  }
}

struct mc_proxy_merge_functions targ_extension_functions = {
  .free_gather_extra = targ_free_gather_extra,
  .merge_end_query = targ_merge_end_query,
  .generate_preget_query = targ_generate_preget_query,
  .generate_new_key = targ_generate_new_key,
  .store_gather_extra = targ_store_gather_extra,
  .check_query = targ_check_query,
  .merge_store = targ_store,
  .load_saved_data = targ_load_saved_data,
  .use_preget_query = targ_use_preget_query
};

struct mc_proxy_merge_conf targ_extension_conf = {
  .use_at = 0,
  .use_preget_query = 1
};
