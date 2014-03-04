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
              2011-2013 Vitaliy Valtman
*/

#define _FILE_OFFSET_BITS 64

#include <assert.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/types.h>
#include <aio.h>
#include <netdb.h>

#include "md5.h"
#include "net-events.h"
#include "net-buffers.h"
#include "net-connections.h"
#include "server-functions.h"
#include "estimate-split.h"
//#include "utils.h"
#include "net-memcache-server.h"
#include "net-memcache-client.h"
#include "mc-proxy-search-extension.h"
#include "kdb-data-common.h"

#define VERSION_STR "mc-proxy-merge-0.2.00"

extern int verbosity;
extern int get_search_queries;
int search_id = -239;
long long total_search_queries;
long long gather_timeouts;
extern int get_targets;
extern struct conn_target *get_target[];
extern struct connection *get_connection[];

#define STATS_BUFF_SIZE (16 << 10)

#define GATHER_QUERY(q) ((struct gather_data *)(q)->extra)

int serv_id (struct connection *c) { 
  int i;
  for (i = 0; i < CC->tot_buckets; i++) {
    if (CC->buckets[i] == c->target) {
      return i;
    }
  }
  return -1;
}
/********************** memory allocation routines ***********/
#define MAX_ZALLOC 1000
long long total_memory_used = 0;
long long malloc_memory_used = 0;
long long zalloc_memory_used = 0;

void *zzmalloc (int size) {
  total_memory_used += size;
  if (size < MAX_ZALLOC) {
    zalloc_memory_used += size;
    return zmalloc (size);
  } else {
    malloc_memory_used += size;
    return malloc (size);
  }
}

void *zzmalloc0 (int size) {
  total_memory_used += size;
  if (size < MAX_ZALLOC) {
    zalloc_memory_used += size;
    return zmalloc0 (size);
  } else {
    malloc_memory_used += size;
    return calloc (size, 1);
  }
}

void zzfree (void *src, int size) {
  if (!src) { return; }
  total_memory_used -= size;
  if (size < MAX_ZALLOC) {
    zalloc_memory_used -= size;
    zfree (src, size);
  } else {
    malloc_memory_used -= size;
    free (src);
  }
}
/*************************************************************/

int free_gather_extra (void *E);
int merge_end_query (struct connection *c, const char *key, int key_len, void *E, struct gather_entry *data, int tot_num);
int generate_new_key (char *buff, char *key, int len, void *E);
void *store_gather_extra (const char *key, int key_len);
int check_query (int type, const char *key, int key_len);
int generate_preget_query (char *buff, const char *key, int key_len, void *E, int n);
int merge_store (struct connection *c, int op, const char *key, int key_len, int expire, int flags, int bytes);
void load_saved_data (struct connection *c);
int should_use_preget_query (void *extra);
int use_at ();
int use_preget_query ();


int active_gathers;

static void free_gather (struct gather_data *G) {
  if (!G) {
    return;
  }
  assert ((G->magic & GD_MAGIC_MASK) == GD_MAGIC);
  if (G->extra) {
    free_gather_extra (G->extra);
  }

  G->magic = 0;
  assert (active_gathers > 0);
  if (verbosity >= 2) {
    fprintf (stderr, "Gather structure %p freed.\n", G);
  }

  int i;
  for (i = 0; i < G->tot_num; i++) {
    if (G->List[i].res_bytes) {
      zzfree (G->List[i].data, G->List[i].res_bytes + 4);
    }
  }
  zzfree (G->orig_key, G->orig_key_len + 1);  
  zzfree (G->new_key, G->new_key_len + 1);
  zzfree (G, G->size);
  active_gathers--;
} 

const char *cur_key; 
int cur_key_len;

// parses client's result and finds master connection for c
int get_key (const char *cmd, int len) {
  const char *ptr = cmd, *to = cmd + len;

  if (!len) {
    return 0;
  }

  if (len < 5 || strncmp (cmd, "VALUE", 5)) {
    return 0;
  }
  ptr += 5;

  while (ptr < to && *ptr == ' ') {
    ptr++;
  }
  if (ptr >= to) {
    return 0;
  }
  const char *key = ptr;
  while (ptr < to && (unsigned char) *ptr > ' ') {
    ptr++;
  }
  if (ptr >= to) {
    return 0;
  }
  cur_key = key;
  cur_key_len = ptr - key;
  return 1;
}

static int client_result_alloc (struct gather_entry *E, char **to) {
  //int b = E->res_read;
  //assert (b >= 4 && b <= E->res_bytes);
  if (!E->res_bytes) {
    return 0;
  }
  E->data = zzmalloc (E->res_bytes + 4);
  *to = (char *) E->data;
  return E->res_bytes + 4;
}


#define DATA_BUFF_LEN (1 << 24)
char data_buff[DATA_BUFF_LEN];		// must be >= 3M !

// reads data from connection c into the gather structure
static int client_read_special (struct conn_query *q, char *data, int data_len) {
  struct connection *c = q->outbound;
  assert (c);
  int x = serv_id (c), t = 0, s;
  assert ( x != -1 );
  char *st, *to = 0, *ptr;
  struct gather_entry *E;

  data_len -= 2;
  if (verbosity > 0) {
    fprintf (stderr, "in client_read_special for %d, %d unread bytes\n", c->fd, data_len);
  }

  if (x < 0 || x >= CC->tot_buckets) {
    fprintf (stderr, "serv_id = %d\n", x);
  }
  assert (x >= 0 && x < CC->tot_buckets);
    
  ptr = st = data;

  struct gather_data *G = GATHER_QUERY ((struct conn_query*)q->extra);

  
  s = data_len;

  if (G && G->tot_num > x) {
    if (data_len == 4 && *(int *)"b:0;" == *(int *)data) {
      E = &G->List[x];
      E->num = -1;
    } else {
      E = &G->List[x];
      E->res_bytes = data_len;
      E->res_read = 0;


      s = data_len; 


      if (SEARCH_EXTENSION || TARG_EXTENSION || SEARCHX_EXTENSION || HINTS_MERGE_EXTENSION) {
        E->num = *((int *) ptr);
        E->res_read = 4;
        if (E->num >= 0 && E->num <= 0x1000000 && s >= 4) {
          ptr += 4;
          s -= 4;
          if (verbosity >= 4) {
            fprintf (stderr, "got %d from %d\n", E->num, x);
          }
        } else {
          if (verbosity >= 4) {
            fprintf (stderr, "Bad result for %d\n", c->fd);
          }
          E->num = -2;
          E->res_bytes += 4;
        }
      }

      client_result_alloc (E, &to);

      if (s > 0) {
        memcpy (to, ptr, s);
        ptr += s;
        if (!(SEARCH_EXTENSION || TARG_EXTENSION || SEARCHX_EXTENSION || HINTS_MERGE_EXTENSION)) {
          E->num = s;
        }
        E->res_read += s;
        if (verbosity >= 4) {
        fprintf (stderr, "data size %d\n", E->res_read);
        }
      }

      s = ptr - st;


      t += s;
      //c->unread_res_bytes -= s;
      //free_unused_buffers (&c->In);
      assert (s == data_len);
    }
  } else {
    fprintf (stderr, "something is wrong with gather structure.\n");
    return 0;
  }

  E = &G->List[x];
  assert (E->res_read == E->res_bytes);
  if (verbosity >= 4) {
    fprintf (stderr, "read %d of %d in gather %p (q=%p)\n", G->ready_num + 1, G->wait_num, G, q);
  }
  assert (++G->ready_num <= G->wait_num);

  return t;
}


/*
 * end (client)
 */


int delete_search_query (struct conn_query *q);
int search_query_timeout (struct conn_query *q);
int end_search_query (struct conn_query *q);


conn_query_type_t search_query_type = {
.magic = CQUERY_FUNC_MAGIC,
.title = "mc-proxy-search-query",
.parse_execute = server_failed,
.close = delete_search_query,
.wakeup = search_query_timeout
};


int delete_search_query (struct conn_query *q) {
  free_gather (GATHER_QUERY(q));
  delete_conn_query (q);
  zfree (q, sizeof (*q));
  return 0;
}

int search_query_timeout (struct conn_query *q) {
  if (verbosity >= 3) {
    fprintf (stderr, "Query %p timeout.\n", q);
  }
  struct gather_data *G = GATHER_QUERY(q);
  fprintf (stderr, "Query on key %s (outbound key %s) timeout\n", G->orig_key, G->new_key);
  gather_timeouts++;
  end_search_query (q);
  delete_search_query (q);
  //query_complete (q->requester, 1);
  return 0;
}

struct conn_query *create_inbound_query (struct gather_data *G, struct connection *c, double timeout) {
  struct conn_query *Q = zmalloc (sizeof (struct conn_query));

  Q->custom_type = MCS_DATA(c)->query_type;
  Q->outbound = c;
  Q->requester = c;
  Q->start_time = c->query_start_time;
  Q->extra = G;
  Q->cq_type = &search_query_type;
  Q->timer.wakeup_time = (timeout > 0 ? Q->start_time + timeout : 0);

  insert_conn_query (Q);

  if (verbosity > 1) {
    fprintf (stderr, "after insert_conn_query(). Q->start_time = %lf, Q->timer.wakeup_time=%lf. cur_time=%lf\n", Q->start_time, Q->timer.wakeup_time, get_utime_monotonic ());
  }

  return Q;
}


static inline int flush_output (struct connection *c) {
//  fprintf (stderr, "flush_output (%d)\n", c->fd);
  return MCC_FUNC (c)->flush_query (c);
}


static inline void accumulate_query_timeout (struct connection *c, double query_timeout) {
  if (c->last_query_timeout < query_timeout) {
    c->last_query_timeout = query_timeout;
    assert (query_timeout < 32);
  }
}

struct connection *get_target_conn_simple (struct conn_target *S, struct connection *c) {
  struct connection *d;
  int x;
  if (S->custom_field) {
    x = S->custom_field;
    if (x < 0) {
      x = -x;
    }
    x--;
    assert ((unsigned) x < (unsigned) get_targets);
    assert (get_target[x] == S);
    d = get_connection[x];
    if (S->custom_field > 0) {
      S->custom_field = - S->custom_field;
      write_out (&d->Out, "\r\n", 2);
      /* create query structure related to c & d */
      struct conn_query *Q = create_query (d, c, CC->get_timeout + 0.2);
      if (/* !i && */ c->Tmp) {
        Q->custom_type |= 0x1000;
      }
      flush_output (d);
      d->last_query_sent_time = precise_now;
      accumulate_query_timeout (d, CC->get_timeout);
    } else {
    }
  } else {
    d = get_target_connection (S);
    if (!d) {
      return 0;
    }
    assert (get_targets < MAX_CLUSTER_SERVERS);
    x = get_targets++;
    S->custom_field = -(x+1);
    get_target[x] = S;
    get_connection[x] = d;
  }
  return d;
}

void change_conn_simple (struct conn_target *S, struct connection *c) {
  assert (S->custom_field < 0);
  S->custom_field = -S->custom_field;
}
struct connection *get_target_conn (struct conn_target *S) {
  struct connection *d;
  int x;
  if (S->custom_field) {
    x = S->custom_field;
    if (x < 0) {
      x = -x;
    }
    x--;
    assert ((unsigned) x < (unsigned) get_targets);
    assert (get_target[x] == S);
    d = get_connection[x];
    if (S->custom_field > 0) {
      write_out (&d->Out, " ", 1);
    } else {
      write_out (&d->Out, "get ", 4);
      S->custom_field = - S->custom_field;
    }
  } else {
    d = get_target_connection (S);
    if (!d) {
      return 0;
    }
    assert (get_targets < MAX_CLUSTER_SERVERS);
    x = get_targets++;
    S->custom_field = x+1;
    get_target[x] = S;
    get_connection[x] = d;
    write_out (&d->Out, "get ", 4);
  }
  return d;
}

#define MAX_KEY_LEN 8192
char new_key_buff [MAX_KEY_LEN + 15];
#define MAX_PREGET_QUERY_LEN (1 << 22)
char preget_query_buff [MAX_PREGET_QUERY_LEN];

struct conn_query* get_conn_query (struct connection *c, const char *data, int len) {
  if (!get_key (data, len)) {
    return 0;
  }
  struct conn_query *q = c->first_query;
  while (q != (struct conn_query*)c) {
    struct connection *C = q->requester;
    if (C->generation != q->req_generation) {
      q = q->next;
      continue;
    }
    struct conn_query *Q_fake = (struct conn_query*)q->extra;
    struct conn_query *Q = C->first_query;
    while (Q != (struct conn_query*)C) {
      if (Q == Q_fake) {
        break;
      }
      Q = Q->next;
    }
    if (Q == (struct conn_query*)C) {
      q = q->next;
      continue;
    }
    struct gather_data *G = GATHER_QUERY((struct conn_query*)q->extra);
    if (G->new_key_len == cur_key_len && !memcmp (G->new_key, cur_key, cur_key_len)) {
      return q;
    }
    q = q->next;
  }
  if (verbosity) {
    fprintf (stderr, "No target gather found. Dropping request.\n");
  }
  return 0;
}

int check_gather_query (struct connection *c, struct conn_query *qu) {
  struct connection *C = qu->requester;
  struct conn_query *Q = C->first_query;
  if (C->generation != qu->req_generation) {
    return 0;
  }
  while (Q != (struct conn_query*)C) {
    assert (Q);
    if (Q == (struct conn_query *)(qu->extra)) {
      return 1;
    }
    Q = Q->next;
  }
  return 0;
}
int do_search_query (struct connection *c, char *key, int len) {
  if (verbosity) {
    fprintf (stderr, "do_search_query key = %s len = %d\n", key, len);
  }
  //char *q_end;
  //char buff[512];
  int w, i = -1;
  struct gather_data *G;

  w = sizeof (struct gather_data) + (CC->tot_buckets - 1) * sizeof (struct gather_entry);
  G = zzmalloc (w);
  assert (G);
  memset (G, 0, w);
  G->size = w;

  G->c = c;
  G->magic = GD_MAGIC;

  //c->gather = G;
  //c->state |= C_INQUERY;

  active_gathers++;

  G->tot_num = CC->tot_buckets;
  G->wait_num = 0;
  G->ready_num = 0;
  G->error_num = 0;
  G->start_time = get_utime(CLOCK_MONOTONIC);

  G->orig_key = zzmalloc (len + 1);
  memcpy (G->orig_key, key, len + 1);
  G->orig_key_len = len;

  load_saved_data (c);


  int al = eat_at (key, len);
  G->extra = store_gather_extra (key + al, len - al);
  if (!G->extra) {
    vkprintf (1, "error in query parse\n");
  }

  int ilen = 0;
  if (use_at ()) {
    ilen = sprintf (new_key_buff, "%d@", search_id++);
  }

  if (G->extra) {
    ilen += generate_new_key (new_key_buff + ilen, key + al, len - al, G->extra);
  }
  G->new_key_len = ilen;
  G->new_key = zzmalloc (ilen + 1);
  memcpy (G->new_key, new_key_buff, ilen + 1);

  if (verbosity > 0) {
    fprintf (stderr, "sending to %d servers query 'get %s'\n", CC->tot_buckets, G->new_key);
  }

  //struct conn_query *q = 
  struct conn_query *Q = 0;

  int upq = use_preget_query () && (G->extra && should_use_preget_query (G->extra));

  if (verbosity) {
    if (upq) {
      fprintf (stderr, "using preget query\n");
    } else {
      fprintf (stderr, "Not using preget query\n");
    }
  }
  for (i = 0; i < CC->tot_buckets; i++) {
    if (!G->extra || G->new_key_len > 950) {
      break;
    }
    struct connection *d = 0;
    G->List[i].res_bytes = 0;
    G->List[i].num = -1;
    if (verbosity >= 4) {
      fprintf (stderr, "Creating connection to target %d key=%s\n", i, G->new_key);
    }

    int send_get = 1;
      
    if (upq) {
      int preget_len = generate_preget_query (preget_query_buff, key + al, len - al, G->extra, i);
      if (verbosity >= 4) {
        fprintf (stderr, "preget_query = %s (len = %d)\n", preget_query_buff, preget_len); 
      }
      if (preget_len) {
        d = get_target_conn_simple (CC->buckets[i], c);
        if (d) {
          write_out (&d->Out, preget_query_buff, preget_len);
          write_out (&d->Out, "\r\n", 2);
          create_query_type (d, c, CC->get_timeout + 0.2, mct_set | 0x2000);
          create_query_type (d, c, CC->get_timeout + 0.2, mct_get | 0x2000);

          write_out (&d->Out, "get ", 4);
          //change_conn_simple (CC->buckets[i], c);
          
        }
      } else {
        //d = get_target_conn (CC->buckets[i]);
        send_get = 0;
      }
    } else {
      d = get_target_conn (CC->buckets[i]);
    }
    if (send_get && d) {
      G->List[i].num = -1;
      G->wait_num++;
      write_out (&d->Out, G->new_key, G->new_key_len);
      struct conn_query *q = create_query (d, c, CC->get_timeout + 0.2);
      if (!Q) {
        Q = create_inbound_query (G, c, CC->get_timeout > 0.4 ? CC->get_timeout - 0.1 : CC->get_timeout * 0.75);
      }
      q->extra = Q;
      if (upq) {
        write_out (&d->Out, "\r\n", 2);
      }
      if (upq) {
	      flush_output (d);
        d->last_query_sent_time = precise_now;
        accumulate_query_timeout (d, CC->get_timeout);
	    }
    }
  }

  if (G->wait_num && G->extra) {
    //c->status = conn_wait_net;
	  get_search_queries++;
    total_search_queries++;

	  if (verbosity >= 3) {
  	  fprintf (stderr, "Created gather G=%p. G->wait_num=%d\n", G, G->wait_num);
	  }
	} else {
    free_gather (G);
		//end_search_query (Q);
    //query_complete (c, 1);
    //delete_search_query (Q);
	}

  return 0;
}



int end_search_query (struct conn_query *q) {
  if (verbosity >= 4) {
    fprintf (stderr, "in end_search_query q=%p\n", q);
  }
  struct connection *c = q->outbound;
  assert (c->generation == q->req_generation);
  int w, i;
  char *key, *ptr;
  struct gather_data *G = GATHER_QUERY(q);
  struct gather_entry *D = 0;

  assert (G);
  assert ((G->magic & GD_MAGIC_MASK) == GD_MAGIC);

  key = G->orig_key;

  /* sum results */
  for (i = 0; i < G->tot_num; i++) {
    if (G->List[i].num == -2) {
      D = &G->List[i];
    }
  }

  if (D) {
    /* have error */
    w = D->res_read - 4;
    if (w > 0) {
      ptr = (char *) D->data;
      ptr[w] = 0;
      if (verbosity > 0) {
        fprintf (stderr, "got error message: %s\n", ptr);
      }
      return return_one_key (c, key, ptr, w);
    }
    return return_one_key (c, key, "ERROR_UNKNOWN", 13);
  }

  //int al = eat_at (G->orig_key, G->orig_key_len);
  //merge_end_query (c, G->orig_key + al, G->orig_key_len - al, G->extra, G->List, G->tot_num);
  merge_end_query (c, G->orig_key, G->orig_key_len, G->extra, G->List, G->tot_num);
  //query_complete (c, 1);
  // c->status = conn_ready;

  return 0;
}


int search_merge (struct connection *c, int data_len) {
  assert (data_len <= DATA_BUFF_LEN);
  
  assert (read_in (&c->In, data_buff, data_len) == data_len);
  if (verbosity >= 4) {
    int i;
    for (i = 0; i < data_len; i++) 
      fprintf (stderr, "%c[%d]", data_buff[i], data_buff[i]);
    fprintf (stderr, "\n");
  }
  free_unused_buffers (&c->In);

  int data_shift = 0;
  while (data_shift < data_len && data_buff[data_shift] != 13) {
    data_shift++;
  }
  if (data_shift >= data_len-1) {
    fprintf (stderr, "data_shift = %d\n", data_shift);
    return -1;
  }
  data_shift += 2;
  if (data_buff[data_shift-1] != 10) {
    fprintf (stderr, "data_buff[] = %d\n", data_buff[data_shift-1]);
    return -1;
  }

  struct conn_query *q = get_conn_query (c, data_buff, data_len);

  if (!q) {
    fprintf (stderr, "Error in search_merge: cannot find query for answer. Dropping answer.\n");
    return 0;
  }

  if (!q->requester || q->req_generation != q->requester->generation) {
    fprintf (stderr, "Error in search_merge: generations do not match. Dropping answer. (key = %s).\n", cur_key);
    query_complete_custom (q, 0);
    return 0;
  }

  struct conn_query *Q = (struct conn_query*)q->extra; 
  if (!Q) {
    fprintf (stderr, "Error in search_merge: no parent query. Dropping answer. (key = %s).\n", cur_key);
    query_complete_custom (q, 0);
    return 0;
  }
  assert (q->requester == Q->requester);
  assert (Q->extra);


  CC = ((struct memcache_server_functions *) (Q->requester)->extra)->info;
  assert (CC && &CC->mc_proxy_inbound == (Q->requester)->extra);
  
  client_read_special (q, data_buff + data_shift, data_len - data_shift);
  query_complete_custom (q, 1);

  if (verbosity >= 4) {
    fprintf (stderr, "end of search_merge\n");
  }

  struct gather_data *G = GATHER_QUERY(Q);
  if (verbosity >= 2) {
  	fprintf (stderr, "got answer %d of %d in %.08f seconds\n", G->ready_num, G->wait_num, get_utime(CLOCK_MONOTONIC) - G->start_time);
  }

  if (G->wait_num == G->ready_num) {
    end_search_query (Q);
    if (verbosity >= 4) {
      fprintf (stderr, "All answers gathered. Deleting master query.\n");
    }
    //query_complete (Q->requester, 1);
    delete_search_query (Q);
  }

  return 1;
} 


int search_skip (struct connection *c, struct conn_query *q) {

  int res = check_gather_query (c, q);

  if (!res) {
    fprintf (stderr, "Error in search_merge: cannot find query for answer. Dropping answer.\n");
    query_complete_custom (q, 0);
    return 0;
  }

  if (!q->requester || q->req_generation != q->requester->generation) {
    fprintf (stderr, "Error in search_merge: generations do not match. Dropping answer. (key = %s).\n", cur_key);
    query_complete_custom (q, 0);
    return 0;
  }

  struct conn_query *Q = (struct conn_query*)q->extra; 
  if (!Q) {
    fprintf (stderr, "Error in search_merge: no parent query. Dropping answer. (key = %s).\n", cur_key);
    query_complete_custom (q, 0);
    return 0;
  }
  assert (q->requester == Q->requester);
  assert (Q->extra);
  struct gather_data *G = GATHER_QUERY(Q);
  
  
  int x = serv_id (q->outbound);
  assert (x != -1);
  if (x < 0 || x >= CC->tot_buckets) {
    fprintf (stderr, "serv_id = %d\n", x);
  }
  assert (x >= 0 && x < CC->tot_buckets);
  G->List[x].num = -3;

  query_complete_custom (q, 1);
  G->ready_num ++;
  
  if (verbosity >= 2) {
  	fprintf (stderr, "got empty answer %d of %d in %.08f seconds\n", G->ready_num, G->wait_num, get_utime(CLOCK_MONOTONIC) - G->start_time);
  }

  if (G->wait_num == G->ready_num) {
    end_search_query (Q);
    if (verbosity >= 4) {
      fprintf (stderr, "All answers gathered. Deleting master query.\n");
    }
    //query_complete (Q->requester, 1);
    delete_search_query (Q);
  }

  return 1;
} 
int search_check (int type, const char *key, int key_len) {
  return check_query (type, key, key_len);
}

int search_create (struct connection *c, char *key, int key_len) {
  return do_search_query (c, key, key_len);
}

int search_store (struct connection *c, int op, const char *key, int key_len, int expire, int flags, int bytes) {
  return merge_store (c, op, key, key_len, expire, flags, bytes);
}



int search_stats (char *buff, int len) {
  return snprintf (buff, len, 
    "extension\tmerge\n"
    "memory_used\t%lld\n"
    "active_gathers\t%d\n"
    "total_search_queries\t%lld\n"
    "timeout_search_gathers\t%lld\n"
    "version\t" VERSION_STR " compiled at " __DATE__ " " __TIME__ " by gcc " __VERSION__ " "
#ifdef __LP64__
     "64-bit"
#else
     "32-bit"
#endif
      "\n",
    total_memory_used,
    active_gathers,
    total_search_queries,
    gather_timeouts);
}


int default_free_gather_extra (void *E) {
  if (E) {
    fprintf (stderr, "Memory leak in default_free_gather_extra.\n");
  }
  return 0;
}

int default_merge_end_query (struct connection *c, const char *key, int key_len, void *E, struct gather_entry *data, int tot_num) {
  assert (0);
  return return_one_key (c, key, "ERROR: use default for merge_end_query", 38);
}



int default_generate_new_key (char *buff, char *key, int len, void *E) {
  memcpy (buff, key, len);
  return len;
}

void *default_store_gather_extra (const char *key, int key_len) {
  return 0;
}

int default_check_query (int type, const char *key, int key_len) {
  return 0;
}

int default_generate_preget_query (char *buff, const char *key, int key_len, void *E, int n) {
  return 0;
}

int default_merge_store (struct connection *c, int op, const char *key, int key_len, int expire, int flags, int bytes) {
  return 0;
}

void default_load_saved_data (struct connection *c) {}

int default_use_preget_query (void *extra) {
  return 0;
}

#define MAX_MERGE_EXTENSIONS


struct mc_proxy_merge_functions default_merge_functions = {
  .free_gather_extra = default_free_gather_extra,
  .merge_end_query = default_merge_end_query,
  .generate_new_key = default_generate_new_key,
  .store_gather_extra = default_store_gather_extra,
  .check_query = default_check_query
};
extern struct mc_proxy_merge_functions search_extension_functions;
extern struct mc_proxy_merge_functions searchx_extension_functions;
extern struct mc_proxy_merge_functions targ_extension_functions;
extern struct mc_proxy_merge_functions news_ug_extension_functions;
extern struct mc_proxy_merge_functions news_comm_extension_functions;
extern struct mc_proxy_merge_functions statsx_extension_functions;
extern struct mc_proxy_merge_functions friends_extension_functions;
extern struct mc_proxy_merge_functions hints_merge_extension_functions;
extern struct mc_proxy_merge_functions newsr_extension_functions;
extern struct mc_proxy_merge_functions random_merge_extension_functions;

struct mc_proxy_merge_conf default_merge_conf = {
  .use_at = 1,
  .use_preget_query = 0
};
extern struct mc_proxy_merge_conf search_extension_conf;
extern struct mc_proxy_merge_conf searchx_extension_conf;
extern struct mc_proxy_merge_conf targ_extension_conf;
extern struct mc_proxy_merge_conf news_ug_extension_conf;
extern struct mc_proxy_merge_conf news_comm_extension_conf;
extern struct mc_proxy_merge_conf statsx_extension_conf;
extern struct mc_proxy_merge_conf friends_extension_conf;
extern struct mc_proxy_merge_conf hints_merge_extension_conf;
extern struct mc_proxy_merge_conf newsr_extension_conf;
extern struct mc_proxy_merge_conf random_merge_extension_conf;

struct mc_proxy_merge_functions *get_extension_functions () {
  if (SEARCH_EXTENSION) {
    return &search_extension_functions;
  } else if (SEARCHX_EXTENSION) {
    return &searchx_extension_functions;
  } else if (TARG_EXTENSION) {
    return &targ_extension_functions;
  } else if (NEWS_UG_EXTENSION) {  
    return &news_ug_extension_functions;
  } else if (NEWS_G_EXTENSION) {  
    return &news_ug_extension_functions;
  } else if (NEWS_COMM_EXTENSION) {
    return &news_comm_extension_functions;
  } else if (STATSX_EXTENSION) {
    return &statsx_extension_functions;
  } else if (FRIENDS_EXTENSION) {
    return &friends_extension_functions;
  } else if (HINTS_MERGE_EXTENSION) {
    return &hints_merge_extension_functions;
  } else if (NEWSR_EXTENSION) {
    return &newsr_extension_functions;
  } else if (RANDOM_MERGE_EXTENSION) {
    return &random_merge_extension_functions;
  } else {  
    return &default_merge_functions;
  }
}

struct mc_proxy_merge_conf *get_extension_conf () {
  if (SEARCH_EXTENSION) {
    return &search_extension_conf;
  } else if (SEARCHX_EXTENSION) {
    return &searchx_extension_conf;
  } else if (TARG_EXTENSION) {
    return &targ_extension_conf;
  } else if (NEWS_UG_EXTENSION) {  
    return &news_ug_extension_conf;
  } else if (NEWS_G_EXTENSION) {  
    return &news_ug_extension_conf;
  } else if (NEWS_COMM_EXTENSION) {
    return &news_comm_extension_conf;
  } else if (STATSX_EXTENSION) {
    return &statsx_extension_conf;
  } else if (FRIENDS_EXTENSION) {
    return &friends_extension_conf;
  } else if (HINTS_MERGE_EXTENSION) {
    return &hints_merge_extension_conf;
  } else if (NEWSR_EXTENSION) {
    return &newsr_extension_conf;
  } else if (RANDOM_MERGE_EXTENSION) {
    return &random_merge_extension_conf;
  } else {
    return &default_merge_conf;
  }
}


int free_gather_extra (void *E) {
  struct mc_proxy_merge_functions *func = get_extension_functions ();
  return func->free_gather_extra (E);
}

int merge_end_query (struct connection *c, const char *key, int key_len, void *E, struct gather_entry *data, int tot_num) {
  struct mc_proxy_merge_functions *func = get_extension_functions ();
  return func->merge_end_query (c, key, key_len, E, data, tot_num);
}

int generate_new_key (char *buff, char *key, int len, void *E) {
  struct mc_proxy_merge_functions *func = get_extension_functions ();
  return func->generate_new_key (buff, key, len, E);
}

void *store_gather_extra (const char *key, int key_len) {
  struct mc_proxy_merge_functions *func = get_extension_functions ();
  return func->store_gather_extra (key, key_len);
}

int check_query (int type, const char *key, int key_len) {
  if (verbosity) {
    fprintf (stderr, "check_query\n");
  }
  struct mc_proxy_merge_functions *func = get_extension_functions ();
  return func->check_query (type, key, key_len);
}

int generate_preget_query (char *buff, const char *key, int key_len, void *E, int n) {
  struct mc_proxy_merge_functions *func = get_extension_functions ();
  return func->generate_preget_query (buff, key, key_len, E, n);  
}

int merge_store (struct connection *c, int op, const char *key, int key_len, int expire, int flags, int bytes) {
  struct mc_proxy_merge_functions *func = get_extension_functions ();
  return func->merge_store (c, op, key, key_len, expire, flags, bytes);
}

void load_saved_data (struct connection *c) {
  struct mc_proxy_merge_functions *func = get_extension_functions ();
  return func->load_saved_data (c);
}

int should_use_preget_query (void *extra) {
  struct mc_proxy_merge_functions *func = get_extension_functions ();
  return func->use_preget_query (extra);
}

int use_at () {
  struct mc_proxy_merge_conf *conf = get_extension_conf ();
  return conf->use_at;
}

int use_preget_query () {
  struct mc_proxy_merge_conf *conf = get_extension_conf ();
  return conf->use_preget_query;
}

int eat_at (const char *key, int key_len) {
  if (!key_len) {
    return 0;
  }
  int p = 0; 
  if (key[p] == '-') {
    p++;
  }
  while (p < key_len && key[p] >= '0' && key[p] <= '9') {
    p++;
  }
  if (p == key_len || p == 0 || key[p] != '@') {
    return 0;
  } else {
    p++;
    return p;
  }
}
