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

#include "mc-proxy-news-extension.h"
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
static char stats_buff[STATS_BUFF_SIZE];


/* merge data structures */

typedef struct news_item item_t;

struct news_item {
  item_t *gnext;
  int g_flags;
  int day;
  int user_id;
  int date;
  int tag;
  int type;
  int user;
  int group;
  int owner;
  int place;
  int item;
};

/* end (merge structures) */


static inline int flush_output (struct connection *c) {
//  fprintf (stderr, "flush_output (%d)\n", c->fd);
  return MCC_FUNC (c)->flush_query (c);
}
    
/* -------- LIST GATHER/MERGE ------------- */

#define	MAX_ITEMS	1048576

static item_t *X[MAX_ITEMS];
static item_t X_src[MAX_ITEMS];
static int XN;

/*
+1	owner+item
+2	owner+place
+4	group by (type+day+)user_id
+8	cancel enter+leave pairs (type+day+user_id+item)
+16	cancel multiple records (type+day+user_id, leave only the last one)
+32	group by (type+day+)item
+64	return total items number (not necessarily distinct)
+128	group by (type+day+owner+)place
+256 user_id+item
*/

static int UG_TypeReduce[] = {
0,1,2,3,4,5,6,8,
8,10,10,12,12,13,14,15,
16,17,18,19,20,21,22,23,
24,25,26,27,28,29,30,31
};

static int UG_TypeFlags[] = {
0,7,5,7,5,1,4,12,
12,12,12,40,40,5,68,5,
7,0,68,256+128+2,7,0,0,0,
0,0,0,0,0,0,0,0
};

static int Comm_TypeReduce[] = {
0,1,2,3,4,5,6,8,
8,10,10,12,12,13,14,15,
16,17,18,19,20,21,22,23,
24,25,26,27,28,29,30,31
};

static int Comm_TypeFlags[] = {
0,0,0,0,0,0,0,0,
0,0,0,0,0,0,0,0,
0,0,0,0,130,130,130,130,
130,0,0,0,0,0,0,0
};

static int *TypeReduce = UG_TypeReduce, *TypeFlags = UG_TypeFlags;

#define	HASH_SIZE	2390021
#define MAX_HASH_ITERATIONS	1000

static item_t *HX[HASH_SIZE];
static int HD[HASH_SIZE], Hs, Ht;

int ihash_lookup (item_t *A, int mode) {
  int t = A->type, tt = mode ? t : TypeReduce[t];
  item_t *B;
  unsigned h = tt * 17239;
  unsigned h2 = tt;

  if (A->day + Hs > Ht) {
    Ht = A->day + Hs;
  }

  if (TypeFlags[t] & 128) {
    h += 239 * A->place;
    h2 += 17 * A->place;
    if (TypeFlags[t] & 2) {
      h += 17 * A->owner;
      h2 += 239 * A->owner;
      mode |= 8;
    }
    mode |= 16;
  } else {
    if (!mode || !(TypeFlags[t] & 32)) {
      h += A->user_id * 239;
      h2 += A->user_id * 10000;
      mode |= 2;
    }
    if ((mode & 1) ? (TypeFlags[t] & 32) : (TypeFlags[t] & 8)) {
      h += 666 * A->item;
      h2 += 13 * A->item;
      if (TypeFlags[t] & 1) {
        h += 17 * A->owner;
        h2 += 239 * A->owner;
        mode |= 8;
      }
      mode |= 4;
    }
  }

  h %= HASH_SIZE;
  h2 = (h2 % (HASH_SIZE - 1)) + 1;

  int hash_iterations = 0;

  while (1) {
    if (HD[h] != Ht || !HX[h]) {
      break;
    }
    B = HX[h];
    if (B->type == A->type || (!(mode & 1) && TypeReduce[B->type] == tt)) {
      if (!(mode & 2) || (A->user_id == B->user_id)) {
        if (!(mode & 4) || (A->item == B->item)) {
          if (!(mode & 8) || (A->owner == B->owner)) {
            if (!(mode & 16) || (A->place == B->place)) {
              return h;
            }
          }
        }
      }
    }
    h += h2;
    if (h >= HASH_SIZE) {
      h -= HASH_SIZE;
    }
    assert (++hash_iterations <= MAX_HASH_ITERATIONS);
  }

  HD[h] = Ht;
  HX[h] = 0;
  return h;
}



void isort (int a, int b) {
  int i, j, h;
  item_t *t;
  if (a >= b) { return; }
  i = a;  j = b;  h = X[(a+b)>>1]->date;
  do {
    while (X[i]->date > h) { i++; }
    while (X[j]->date < h) { j--; }
    if (i <= j) {
      t = X[i];  X[i++] = X[j];  X[j--] = t;
    }
  } while (i <= j);
  isort (a, j);
  isort (i, b);
}

#define MAX_RES	65536

//static int R_cnt, *R_end;
static int R[MAX_RES];
static int Rlen[MAX_RES];
static int Rfirst[MAX_RES];

int merge_items (struct news_gather_extra *extra, int raw, struct gather_entry *data, int tot_num) {
  int day = -1, i, j;
  int item_size;
  item_t *A = 0, *B;
  if ((NEWS_UG_EXTENSION || NEWS_G_EXTENSION)) {
    item_size = 36;
  } else {
    item_size = 32;
  }
  int cur_num = -1;
  int remaining_items = 0;

  if (!raw && extra->date) {
    day = extra->date % 86400;
  }

  XN = 0;
  if (verbosity >= 4) {
    fprintf (stderr, "merge_items: tot_num = %d\n", tot_num);
    if (tot_num > 0) {
      fprintf (stderr, "merge_items: bytes[0] = %d\n", data[0].res_bytes);
    }
  }
  char *ptr = 0;
  while (1) {
    if (!remaining_items) {
      cur_num ++;
      while (cur_num < tot_num && data[cur_num].res_bytes < item_size) {
        cur_num ++;
      }
      if (cur_num == tot_num) {
        break;
      }
      assert (data[cur_num].res_bytes < 0 || data[cur_num].res_bytes % item_size == 0);
      ptr = (char *)data[cur_num].data;      
      remaining_items = data[cur_num].res_bytes / item_size - 1;
    } else {
      remaining_items--;
      ptr += item_size;
    }

    if (verbosity >= 4) {
      fprintf (stderr, "in merge: cur_num = %d, remaining = %d\n", cur_num, remaining_items);
    }

    if ((NEWS_UG_EXTENSION || NEWS_G_EXTENSION)) {
      memcpy (&X_src[XN].user_id, ptr, item_size);
    } else {
      memcpy (&X_src[XN].date, ptr, item_size);
      X_src[XN].user_id = 0;
    }
    X[XN] = &X_src[XN];
    assert (X[XN]->type >= 0 && X[XN]->type < 32);
    X[XN]->g_flags = 0;
    X[XN]->gnext = 0;
    X[XN]->day = (day >= 0 ? (X[XN]->date - day) / 86400 : 0);
    XN++;
    if (XN == MAX_ITEMS) {
      break;
    }
  }

  isort (0, XN-1);

  if (raw || !XN) {
    return XN;
  }

  if (Hs > 1000000000) {
    Hs = Ht = 0;
    memset (HD, 0, sizeof(HD));
  }

  Hs = ++Ht;
  j = 0;

  for (i = XN - 1; i >= 0; i--) {
    A = X[i];
    int h = ihash_lookup (A, 0);
    B = HX[h];
//    fprintf (stderr, "[1] A=%p(%d:%d:%d:%d:%d); B=%p(%d:%d:%d:%d:%d); h=%d\n", A, A->type, A->user_id, A->owner, A->place, A->item, B, B ? B->type : 0, B ? B->user_id : 0, B ? B->owner : 0, B ? B->place : 0, B ? B->item : 0, h);
    if (B && (TypeFlags[A->type] & 24)) {
//      fprintf (stderr, "   B removed\n");
      B->g_flags |= 2;
      j++;
    }
    HX[h] = A;      
  }

  Hs = ++Ht;
  for (i = XN - 1; i >= 0; i--) {
    A = X[i];
    if (!(A->g_flags) && (TypeFlags[A->type] & 164)) {
      int h = ihash_lookup (A, 1);
      B = HX[h];
//      fprintf (stderr, "[2] A=%p(%d:%d:%d:%d:%d); B=%p(%d:%d:%d:%d:%d); h=%d\n", A, A->type, A->user_id, A->owner, A->place, A->item, B, B ? B->type : 0, B ? B->user_id : 0, B ? B->owner : 0, B ? B->place : 0, B ? B->item : 0, h);
      if (B) {
//        fprintf (stderr, "   B added into chain\n");
        B->g_flags |= 1;
        A->gnext = B;
        j++;
      }
      HX[h] = A;      
    }
  }

  if (j) {
    for (i = 0, j = 0; i < XN; i++) {
      A = X[i];
      if (!A->g_flags) {
        X[j++] = X[i];
      }
    }
    XN = j;
  }

  return XN;

}

#define	MAX_C_NUM	1024

struct collection {
  int num;
  long long A[MAX_C_NUM];
};

static struct collection cUsers, cGroups, cPlaces, cItems;

static void cAdd (struct collection *C, int item, int owner) {
  long long x = (unsigned) item + (((long long) owner) << 32);
  int i;
  for (i = 0; i < C->num; i++) {
    if (C->A[i] == x) {
      return;
    }
  }
  if (C->num < MAX_C_NUM) {
    C->A[C->num++] = x;
  }
}

static char *serialize_collection (char *ptr, int no, struct collection *C, int grouping, int need_owner, int total) {
  static char buff[32];
  int s, n, i;

  assert (C->num > 0);
  n = C->num;

  if (n == 1 && total <= 1) {
    if (!need_owner) {
      return ptr + sprintf (ptr, "i:%d;i:%d;", no, (int) C->A[0]);
    } else {
      s = sprintf (buff, "%d_%d", (int) (C->A[0] >> 32), (int) C->A[0]);
      return ptr + sprintf (ptr, "i:%d;s:%d:\"%s\";", no, s, buff);
    }
  }

  if (n > grouping) {
    n = grouping;
  }

  ptr += sprintf (ptr, "i:%d;a:%d:{i:0;i:%d;", no, n+1, total ? total : C->num);
  
  for (i = 0; i < n; i++) {
    if (!need_owner) {
      ptr += sprintf (ptr, "i:%d;i:%d;", i+1, (int) C->A[i]);
    } else {
      s = sprintf (buff, "%d_%d", (int) (C->A[i] >> 32), (int) C->A[i]);
      ptr += sprintf (ptr, "i:%d;s:%d:\"%s\";", i+1, s, buff);
    }
  }

  *ptr++ = '}';
  return ptr;
}


static char *serialize_item_group (char *ptr, item_t *A, int no, int grouping) {
  int ug_mode = (NEWS_UG_EXTENSION || NEWS_G_EXTENSION) ? 0 : 1;
  int t, s, c, q = (ug_mode <= 0);
  static char buff[32];
  item_t *B;
  ptr += sprintf (ptr, "i:%d;a:%d:{", no, 7+q);
  t = A->type;
  assert (t >= 0 && t < 32);

  if (!A->gnext) {
    if (q) {
      ptr += sprintf (ptr, "i:0;i:%d;i:1;i:%d;i:2;i:%d;i:3;i:%d;i:4;i:%d;i:5;i:%d;", 
	     A->type, A->user_id, A->date, A->tag, A->user, A->group);
    } else {
      ptr += sprintf (ptr, "i:0;i:%d;i:1;i:%d;i:2;i:%d;i:3;i:%d;i:4;i:%d;", 
	     A->type, A->date, A->tag, A->user, A->group);
    }
    if (A->owner && (TypeFlags[t] & 2)) {
      s = sprintf (buff, "%d_%d", A->owner, A->place);
      ptr += sprintf (ptr, "i:%d;s:%d:\"%s\";", 5+q, s, buff);
    } else {
      ptr += sprintf (ptr, "i:%d;i:%d;", 5+q, A->place);
    }
    if (A->owner && (TypeFlags[t] & 1)) {
      s = sprintf (buff, "%d_%d", A->owner, A->item);
      ptr += sprintf (ptr, "i:%d;s:%d:\"%s\";", 6+q, s, buff);
    } else if (A->user_id && (TypeFlags[t] & 256)){
      s = sprintf (buff, "%d_%d", A->user_id, A->item);
      ptr += sprintf (ptr, "i:%d;s:%d:\"%s\";", 6+q, s, buff);
    } else {
      ptr += sprintf (ptr, "i:%d;i:%d;", 6+q, A->item);
    }
    *ptr++ = '}';
    return ptr;
  }

  cUsers.num = cGroups.num = cPlaces.num = cItems.num = 0;
  for (B = A, c = 0; B; B = B->gnext, c++) {
    assert (B->type == t);
    cAdd (&cUsers, B->user, 0);
    cAdd (&cGroups, B->group, 0);
    cAdd (&cPlaces, B->place, TypeFlags[t] & 2 ? B->owner : 0);
    cAdd (&cItems, B->item, TypeFlags[t] & 1 ? B->owner : TypeFlags[t] & 256 ? B->user_id : 0);
  }

  if (q) {
    ptr += sprintf (ptr, "i:0;i:%d;i:1;i:%d;i:2;i:%d;i:3;i:%d;", 
			  A->type, A->user_id, A->date, A->tag);
  } else {
    ptr += sprintf (ptr, "i:0;i:%d;i:1;i:%d;i:2;i:%d;", 
			  A->type, A->date, A->tag);
  }
  ptr = serialize_collection (ptr, 3+q, &cUsers, grouping, 0, 0);
  ptr = serialize_collection (ptr, 4+q, &cGroups, grouping, 0, 0);
  ptr = serialize_collection (ptr, 5+q, &cPlaces, grouping, TypeFlags[t] & 2, 0);
  ptr = serialize_collection (ptr, 6+q, &cItems, grouping, TypeFlags[t] & 257, TypeFlags[t] & 64 ? c : 0);

  *ptr++ = '}';
  return ptr;

}


/* -------- END (LIST GATHER/MERGE) ------- */

#define	MAX_QUERY	131072
static int QL, Q[MAX_QUERY], QN[MAX_QUERY];

int news_merge_end_query (struct connection *c, const char *key, int key_len, void *E, struct gather_entry *data, int tot_num) {
  int w, i, t;
  int res, mode, raw;
  char *ptr, *size_ptr, *s;
  int ug_mode = (NEWS_UG_EXTENSION || NEWS_G_EXTENSION) ? 0 : 1;
  int rec_size = (ug_mode <= 0 ? 9 : 8);
  struct news_gather_extra* extra = E;
  TypeFlags = (ug_mode > 0) ? Comm_TypeFlags : UG_TypeFlags;
  TypeReduce = (ug_mode > 0) ? Comm_TypeReduce : UG_TypeReduce;

  int at_l = eat_at (key, key_len);
  mode = at_l + (key[at_l] == '%');
  raw = (key[mode] == 'r');

  if (extra->user_mode == 2 && ug_mode > 0) {
    QL = 0;
    for (i = 0; i < tot_num; i++) {
      int x = data[i].res_bytes / 4;
      if (x + QL > MAX_QUERY) {
        x = MAX_QUERY - QL;
      }
      x = x - (x % 3);
      for (t = 0; t < x; t++) {
        Q[QL + t] = data[i].data[t];
      }
      QL += x;
    }
    return return_one_key_list (c, key, key_len, 0x7fffffff, -mode, Q, QL);
  }

  res = merge_items (extra, raw || !extra->grouping, data, tot_num);

  if (verbosity > 0) {
    fprintf (stderr, "merge result = %d\n", res);
  }

  if (res < 0) {
    return return_one_key (c, key, "ERROR_MERGE", 11);
  }

  if (!raw && res > extra->max_res && extra->max_res > 0) {
    res = extra->max_res;
  }

  write_out (&c->Out, "VALUE ", 6);
  write_out (&c->Out, key, key_len);

  ptr = get_write_ptr (&c->Out, 16000);
  if (!ptr) return -1;
  s = ptr + 12000;

  memcpy (ptr, " 0 .........\r\n", 14);

  size_ptr = ptr + 3;
  ptr += 14;

  int x;

  if (!raw) {
    size_ptr[-2]++;

    ptr += w = sprintf (ptr, "a:%d:{", res);
    assert (ptr < s);
    advance_write_ptr (&c->Out, ptr - (s - 12000));

    for (x = 0; x < res; x++) {
      item_t *A = X[x];
      int t = serialize_item_group (stats_buff, A, x, extra->grouping) - stats_buff;
      assert (t <= STATS_BUFF_SIZE);
      assert (write_out (&c->Out, stats_buff, t) == t);
      w += t;
    }

    ptr = get_write_ptr (&c->Out, 16000);
    if (!ptr) return -1;
    s = ptr + 12000;
    *ptr++ = '}';
    w++;

  } else {
    w = 0;
    /*if (!mode) {
      ptr += w = sprintf (ptr, "%d", res);
    } else {
      *((int *) ptr) = res;
      ptr += w = 4;
    }*/
    for (x = 0; x < res; x++) {
      item_t *A = X[x];
      if (ptr >= s) {
        if (ptr > s + 4000) {
          fprintf (stderr, "pointers: %p %p\n", ptr, s);
          assert (ptr <= s + 4000);
        }
        advance_write_ptr (&c->Out, ptr - (s - 12000));
        ptr = get_write_ptr (&c->Out, 16000);
        if (!ptr) return -1;
        s = ptr + 12000;
      }
      if (!mode) {
        for (i = 0; i < rec_size; i++) {
          t = 0;
          if (i || x) {          
            *(ptr++) = ',';
            t = 1;
          }
          if ((NEWS_UG_EXTENSION || NEWS_G_EXTENSION)) {
            t += sprintf (ptr + t, "%d", (&A->user_id)[i]);
          } else {
            t += sprintf (ptr + t, "%d", (&A->user_id)[i + 1]);
          }
          w += t;
          ptr += t;
        } 
      } else {
        if ((NEWS_UG_EXTENSION || NEWS_G_EXTENSION)) {
          memcpy (ptr, &A->user_id, rec_size*4);
        } else {
          memcpy (ptr, &A->date, rec_size*4);
        }
        w += rec_size*4;
        ptr += rec_size*4;
      }
    }
  }

  size_ptr [sprintf (size_ptr, "% 9d", w)] = '\r';
  //memcpy (ptr, "\r\nEND\r\n", 7);
  memcpy (ptr, "\r\n", 2);
  ptr += 2;
  if (ptr > s + 4000) {
    fprintf (stderr, "pointers: %p %p\n", ptr, s);
    assert (ptr <= s + 4000);
  }
  advance_write_ptr (&c->Out, ptr - (s - 12000));

  return 1;
}


#define NEWS_STORE_MAGIC 0x637f8894
struct keep_mc_store {
  int magic;
  int user_id;
  int no_reply;
  int len;
  int op;
  int type;
  int cat;
  char text[0];
};

struct keep_mc_store mc_store;

int news_data_store (struct connection *c, int op, const char *key, int len, int flags, int expire, int bytes) {
  int user_id;
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
    mc_store.magic = NEWS_STORE_MAGIC;
    if ((NEWS_UG_EXTENSION || NEWS_G_EXTENSION) && op == 1 && sscanf (key, "userlist%d ", &user_id) == 1 && user_id < 0) {
    } else if (NEWS_COMM_EXTENSION && op == 1 && sscanf (key, "objectlist%d ", &user_id) == 1 && user_id < 0) {
    } else {
      act = 1;
    }
    if (!act) {
      mc_store.user_id = user_id;
      if (NEWS_COMM_EXTENSION) {
        int s = np_news_parse_list (Q, MAX_QUERY, 3, &c->In, bytes);
        mc_store.len = s;
        if (s >= 0) {
          write_out (c->Tmp, &mc_store, sizeof (struct keep_mc_store));
          write_out (c->Tmp, Q, s * 12);
        } else {
          act = 1;
        }
      } else if ((NEWS_UG_EXTENSION || NEWS_G_EXTENSION)) {
        int s = np_news_parse_list (Q, MAX_QUERY, 1, &c->In, bytes);
        mc_store.len = s;
        if (s >= 0) {
          write_out (c->Tmp, &mc_store, sizeof (struct keep_mc_store));
          write_out (c->Tmp, Q, s * 4);
        } else {
          act = 1;
        }
      } else {
        assert (0);
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

int news_generate_preget_query (char *buff, const char *key, int key_len, void *E, int n) {
  if (!Rlen[n]) return 0;
  struct news_gather_extra* extra = E;
  int len = Rlen[n];
  int j, x = Rfirst[n];
  R[0] = ((NEWS_UG_EXTENSION || NEWS_G_EXTENSION) ? 0x30303030 : 0x32303030);
  assert (1 + len * ((NEWS_UG_EXTENSION || NEWS_G_EXTENSION) ? 1 : 3) <= MAX_RES);
  for (j = 0; j < len; j++) {
    assert (x >= 0);
    if ((NEWS_UG_EXTENSION || NEWS_G_EXTENSION)){
      R[j+1] = Q[x];
    } else {
      R[3*j+1] = Q[3*x];
      R[3*j+2] = Q[3*x+1];
      R[3*j+3] = Q[3*x+2];
    }
    x = QN[x];
  }
	
  assert (x == -1 && len > 0);
	if (NEWS_COMM_EXTENSION) {
	  len *= 3;
	}
  int r;
  assert (NEWS_COMM_EXTENSION || (NEWS_UG_EXTENSION || NEWS_G_EXTENSION));
  int ug_mode = (NEWS_UG_EXTENSION || NEWS_G_EXTENSION) ? 0 : 1;
  r = sprintf (buff, "set %slist%d 0 0 %d\r\n", ug_mode <= 0 ? "user" : "object", extra->request_tag, len*4+4);
  memcpy (buff + r, R, len * 4 + 4);
  r += len * 4 + 4;
  return r;
}

int news_generate_new_key (char *buff, char *key, int len, void *E) {
  struct news_gather_extra *extra = E;
  int r = 0;

  if ((NEWS_UG_EXTENSION || NEWS_G_EXTENSION)) {
    r = sprintf (buff, "%%raw_updates%d[%d,%d]:%d", extra->mask, extra->st_time, extra->end_time, extra->request_tag);
  } else {
    if (!extra->user_mode) {
      r = sprintf (buff, "%%raw_comm_updates[%d,%d]:%d", extra->st_time, extra->end_time, extra->request_tag);
    } else if (extra->user_mode == 1) {
      r = sprintf (buff, "%%raw_user_comm_updates[%d,%d]:%d,%d", extra->st_time, extra->end_time, extra->user_id, extra->mask);
    } else if (extra->user_mode == 2) {
      r = sprintf (buff, "%%raw_user_comm_bookmarks%d,%d", extra->user_id, extra->mask);
    } else {
      assert (0);
    }
  }
  return r;
}

void set_rlen () {
  int i, x, y;

  for (i = 0; i < CC->tot_buckets; i++) {
    Rlen[i] = 0;
    Rfirst[i] = -1;
  }

//  int f = (NEWS_UG_EXTENSION && CC->tot_buckets > split_factor);
  split_factor = (NEWS_UG_EXTENSION) ? CC->tot_buckets / 2 : CC->tot_buckets;

  for (i = QL-1; i >= 0; i--) {
  	if ((NEWS_UG_EXTENSION || NEWS_G_EXTENSION)) {
	    x = y = Q[i];
	  } else {
	  	if ((CC->cluster_mode & 0xf) == CLUSTER_MODE_THIRDINT) {
		    x = y = Q[3 * i + 2];
	  	} else if ((CC->cluster_mode & 0xf) == CLUSTER_MODE_SECONDINT) {	
		    x = y = Q[3 * i + 1];
		  } else {
		  	assert ((CC->cluster_mode & 0xf) == CLUSTER_MODE_FIRSTINT);
		  	x = y = Q[3 * i];
		  }
	  }
    if (NEWS_UG_EXTENSION) {
      if (x >= 0) {
        x %= split_factor;
      } else {
        x = (-x % split_factor) + split_factor;
      }
    } else {
      if (x < 0) { x = -x; }
      x %= split_factor;
    }
    if (x < CC->tot_buckets) {
      QN[i] = Rfirst[x];
      Rfirst[x] = i;
      Rlen[x] ++;
    }
  }
}



static int QLsave = 0, uidsave = 0, Qsave[MAX_QUERY];

void news_load_saved_data (struct connection *c) {
  struct keep_mc_store *Data = 0;
  
  if (!c->Tmp) { 
    uidsave = 0;
    return;
  }

  nb_iterator_t R;
  nbit_set (&R, c->Tmp);
  int l = nbit_read_in (&R, &mc_store, sizeof (struct keep_mc_store));
  if (l != sizeof (struct keep_mc_store)) {
    uidsave = 0;
    return; 
  }
  Data = &mc_store;
  assert (Data->magic = NEWS_STORE_MAGIC);

  QLsave = 0;
  uidsave = Data->user_id;

  QLsave = (Data->len < MAX_QUERY ? Data->len : MAX_QUERY);
  if ((NEWS_UG_EXTENSION || NEWS_G_EXTENSION)) {
    assert (nbit_read_in (&R, Qsave, QLsave * 4) == QLsave * 4);
  } else {
    assert (nbit_read_in (&R, Qsave, QLsave * 12) == QLsave * 12);
  }

  //free_tmp_buffers (c);
}

void *news_ug_store_gather_extra (const char *key, int key_len) {
  int list_tag;
  int mask = -1, grouping = 0, date = 0, st_time = 0, end_time = 0, x = 0, raw = 0, max_res = -1;

  if (*key == '%') {
    raw = 1;
  }

  if ((sscanf (key+raw, "raw_updates%d[%d,%d]:%n", &mask, &st_time, &end_time, &x) >= 3 || sscanf (key+raw, "raw_updates%n%d", &x, &list_tag) >= 1) && x > 0) {
    if (x == 11) { mask = -1; }
    x += raw;
  } else if (sscanf (key+raw, "updates%d_%d,%d_%d>%d#%d:%n", &mask, &date, &end_time, &grouping, &st_time, &max_res, &x) >= 6) {
    x += raw;
  } else if (sscanf (key+raw, "updates%d_%d,%d_%d>%d:%n", &mask, &date, &end_time, &grouping, &st_time, &x) >= 5) {
    x += raw;
  } else if (sscanf (key+raw, "updates%d_%d,%d_%d#%d:%n", &mask, &date, &end_time, &grouping, &max_res, &x) >= 5) {
    x += raw;
    st_time = date;
  } else if (sscanf (key+raw, "updates%d_%d,%d_%d:%n", &mask, &date, &end_time, &grouping, &x) >= 4) {
    x += raw;
    st_time = date;
  }

  if (!x) {
    return 0;
  }

  QL = np_parse_list_str (Q, MAX_QUERY, 1, key + x, key_len - x);
  if (QL == 1 && Q[0] < 0 && uidsave == Q[0] && QLsave) {
    list_tag = Q[0];
    QL = QLsave;
    memcpy (Q, Qsave, QL * 4);
  } else {
    list_tag = lrand48() | (-1 << 30);
  }

  if (QL <= 0) {
    return 0;
  }

  if (end_time <= 0) {
    end_time = 0x7fffffff;
  }

  if (list_tag >= 0 || !mask) {
    return 0;
  }

  set_rlen ();

  struct news_gather_extra* extra = zzmalloc (sizeof (struct news_gather_extra));
  extra->request_tag = list_tag;
  extra->mask = mask;
  extra->grouping = grouping;
  extra->date = date;
  extra->st_time = st_time;
  extra->end_time = end_time;
  extra->max_res = (max_res < PHP_MAX_RES && max_res > 0) ? max_res : PHP_MAX_RES;
  extra->user_mode = 0;
  return extra;
}

void *news_comm_store_gather_extra (const char *key, int key_len) {
  int list_tag;
  int grouping = 0, date = 0, st_time = 0, end_time = 0, x = 0, raw = 0, max_res = -1, user_mode = 0, user_id = 0, mask = 0xffffffff;
  char *ptr;

  if (*key == '%') {
    raw = 1;
  }

  if ((sscanf (key+raw, "raw_comm_updates[%d,%d]:%n", &st_time, &end_time, &x) >= 2 || sscanf (key+raw, "raw_comm_updates%n%d", &x, &list_tag) >= 1) && x > 0) {
    x += raw;
  } else if (sscanf (key+raw, "comm_updates%d,%d_%d>%d#%d:%n", &date, &end_time, &grouping, &st_time, &max_res, &x) >= 5) {
    x += raw;
  } else if (sscanf (key+raw, "comm_updates%d,%d_%d>%d:%n", &date, &end_time, &grouping, &st_time, &x) >= 4) {
    x += raw;
  } else if (sscanf (key+raw, "comm_updates%d,%d_%d#%d:%n", &date, &end_time, &grouping, &max_res, &x) >= 4) {
    x += raw;
    st_time = date;
  } else if (sscanf (key+raw, "comm_updates%d,%d_%d:%n", &date, &end_time, &grouping, &x) >= 3) {
    x += raw;
    st_time = date;
  } else if ((sscanf (key+raw, "raw_user_comm_updates[%d,%d]:%n", &st_time, &end_time, &x) >= 2 || sscanf (key+raw, "raw_user_comm_updates%n%d", &x, &list_tag) >= 1) && x > 0) {
    x += raw;                   
    user_mode = 1;
  } else if (sscanf (key+raw, "user_comm_updates%d,%d_%d>%d#%d:%n", &date, &end_time, &grouping, &st_time, &max_res, &x) >= 5) {
    x += raw;
    user_mode = 1;
  } else if (sscanf (key+raw, "user_comm_updates%d,%d_%d>%d:%n", &date, &end_time, &grouping, &st_time, &x) >= 4) {
    x += raw;
    user_mode = 1;
  } else if (sscanf (key+raw, "user_comm_updates%d,%d_%d#%d:%n", &date, &end_time, &grouping, &max_res, &x) >= 4) {
    x += raw;
    st_time = date;
    user_mode = 1;
  } else if (sscanf (key+raw, "user_comm_updates%d,%d_%d:%n", &date, &end_time, &grouping, &x) >= 3) {
    x += raw;
    st_time = date;
    user_mode = 1;
  } else if (sscanf (key+raw, "raw_user_comm_bookmarks%n", &x) >= 0) {
    x += raw;
    user_mode = 2;
  }

  //fprintf (stderr, "raw = %d, x = %d, user_mode = %d\n", raw, x, user_mode);
  if (x <= 1) {
    return 0;
  }

  if (!user_mode) {
    ptr = 0;
    list_tag = strtol (key + x, &ptr, 10);

    if (list_tag < 0 && ptr == key + key_len && list_tag == uidsave && QLsave) {
      QL = QLsave;
      memcpy (Q, Qsave, QL * 12);
    } else {
      QL = np_parse_list_str (Q, MAX_QUERY, 3, key + x, key_len - x);
      list_tag = lrand48() | (-1 << 30);
    }

    if (QL <= 0) {
      return 0;
    }

    if (list_tag >= 0) {
      return 0;
    }

    set_rlen ();
  } else {
    ptr = 0;
    user_id = strtol (key + x, &ptr, 10);
    if (verbosity) {
      fprintf (stderr, "news_comm_store_gather_extra: user_id = %d\n", user_id);
    }
    if (ptr < key + key_len && *ptr == ',') {
      ptr ++;
      mask = strtol (ptr, &ptr, 10);
    }
    if (ptr != key + key_len || user_id <= 0) {
      return 0;
    }
  }


  if (end_time <= 0) {
    end_time = 0x7fffffff;
  }

  struct news_gather_extra* extra = zzmalloc (sizeof (struct news_gather_extra));
  extra->request_tag = list_tag;
  extra->mask = mask;
  extra->grouping = grouping;
  extra->date = date;
  extra->st_time = st_time;
  extra->end_time = end_time;
  extra->max_res = (max_res < PHP_MAX_RES && max_res > 0) ? max_res : PHP_MAX_RES;
  extra->user_mode = user_mode;
  extra->user_id = user_id;
  return extra;
}

int news_ug_check_query (int type, const char *key, int key_len) {
  if (verbosity) {
    fprintf (stderr, "news_ug_check: type = %d, key = %s, key_len = %d\n", type, key, key_len);
  }
  int l = eat_at (key, key_len);
  key += l;
  key_len -= l;
  if (type == mct_get) {
    return (key_len >= 12 && (!strncmp (key, "raw_updates", 11) || !strncmp (key, "%raw_updates", 12))) ||
           (key_len >=  8 && (!strncmp (key, "updates", 7) || !strncmp (key, "%updates", 8)));
  } else if (type == mct_set || type == mct_replace || type == mct_add) {
    return (key_len >= 8 && !strncmp (key, "userlist", 8));
  } else {
    return 1;
  }
}


int news_comm_check_query (int type, const char *key, int key_len) {
  if (verbosity) {
    fprintf (stderr, "news_comm_check: type = %d, key = %s, key_len = %d\n", type, key, key_len);
  }
  int l = eat_at (key, key_len);
  key += l;
  key_len -= l;
  if (type == mct_get) {
    return (key_len >= 17 && (!strncmp (key, "raw_comm_updates", 16) || !strncmp (key, "%raw_comm_updates", 17))) ||
           (key_len >= 13 && (!strncmp (key, "comm_updates", 12) || !strncmp (key, "%comm_updates", 13))) ||
           (key_len >= 21 && !strncmp (key, "raw_user_comm_updates", 21)) || (key_len >= 17 && !strncmp (key, "user_comm_updates", 17)) || 
           (key_len >= 22 && !strncmp (key, "%raw_user_comm_updates", 22)) || (key_len >= 18 && !strncmp (key, "%user_comm_updates", 18)) ||
           (key_len >= 23 && (!strncmp (key, "raw_user_comm_bookmarks", 23) || !strncmp (key, "%raw_user_comm_bookmarks", 23)));
  } else if (type == mct_set || type == mct_replace || type == mct_add) {
    return (key_len >= 10 && !strncmp (key, "objectlist", 10));
  } else {
    return 1;
  }
}


int news_free_gather_extra (void *E) {
  zzfree (E, sizeof (struct news_gather_extra));
  return 0;
}

int news_use_preget_query (void *extra) {
  return !((struct news_gather_extra *)extra)->user_mode;
}

struct mc_proxy_merge_functions news_comm_extension_functions = {
  .free_gather_extra = news_free_gather_extra,
  .merge_end_query = news_merge_end_query,
  .generate_preget_query = news_generate_preget_query,
  .generate_new_key = news_generate_new_key,
  .store_gather_extra = news_comm_store_gather_extra,
  .check_query = news_comm_check_query,
  .merge_store = news_data_store,
  .load_saved_data = news_load_saved_data,
  .use_preget_query = news_use_preget_query
};

struct mc_proxy_merge_functions news_ug_extension_functions = {
  .free_gather_extra = news_free_gather_extra,
  .merge_end_query = news_merge_end_query,
  .generate_preget_query = news_generate_preget_query,
  .generate_new_key = news_generate_new_key,
  .store_gather_extra = news_ug_store_gather_extra,
  .check_query = news_ug_check_query,
  .merge_store = news_data_store,
  .load_saved_data = news_load_saved_data,
  .use_preget_query = news_use_preget_query
};

struct mc_proxy_merge_conf news_comm_extension_conf = {
  .use_at = 1,
  .use_preget_query = 1
};

struct mc_proxy_merge_conf news_ug_extension_conf = {
  .use_at = 1,
  .use_preget_query = 1
};
