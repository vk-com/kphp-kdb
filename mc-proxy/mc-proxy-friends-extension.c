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

#define	_FILE_OFFSET_BITS	64

#include <assert.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>

#include "net-events.h"
#include "net-buffers.h"
#include "net-connections.h"
#include "net-memcache-server.h" 

#include "mc-proxy-friends-extension.h"
#include "net-memcache-client.h"
#include "net-parse.h"
#include "mc-proxy.h"

#define	SPLIT_FACTOR	100
#define	COMM_SPLIT_FACTOR	200

#define	PHP_MAX_RES	10000

/*
 *
 *		NEWS MERGE ENGINE
 *
 */

extern int verbosity;


#define STATS_BUFF_SIZE	(1 << 20)
//static int stats_buff_len;
//static char stats_buff[STATS_BUFF_SIZE];


/* merge data structures */

static inline int flush_output (struct connection *c) {
//  fprintf (stderr, "flush_output (%d)\n", c->fd);
  return MCC_FUNC (c)->flush_query (c);
}
    
/* -------- LIST GATHER/MERGE ------------- */

#define	MAX_USERLIST_NUM 16383

static int userlist[MAX_USERLIST_NUM+1];
static int resultlist[MAX_USERLIST_NUM+1];
static int user_num;
static int list_id;


#define	MAX_QUERY	131072
//static int QL, Q[MAX_QUERY], QN[MAX_QUERY];

int compare (const void * a, const void * b) {
  return ( *(int*)a - *(int*)b );
}

int friends_merge_end_query (struct connection *c, const char *key, int key_len, void *E, struct gather_entry *data, int tot_num) {
  int i, j;
  struct friends_gather_extra* extra = E;
  if (extra->query_type == 1) {
    for (j = 0; j < extra->num; j++) {
      resultlist[j] = 0;
    }
    for (i = 0; i < tot_num; i++) {
      if (data[i].res_bytes == 4 * extra->num) {
      	for (j = 0; j < extra->num; j++) {
          resultlist[j] += data[i].data[j];
        }        
      }
    }
    return_one_key_list (c, key, key_len, 0x7fffffff, -extra->raw, resultlist, extra->num); 
  } else if (extra->query_type == 2) {
    int res = 0;
    for (i = 0; i < tot_num; i++) {
      if (data[i].res_bytes >= 4) {
      	for (j = 0; j < data[i].data[0]; j++) {
          if (res < MAX_USERLIST_NUM) {
            resultlist[res++] = data[i].data[j + 1];
      	  }
        }        
      }
    }  	
    qsort (resultlist, res, sizeof (int), compare);
    return_one_key_list (c, key, key_len, res, -extra->raw, resultlist, res); 
  } else {
    assert (0);
  }
  return 1;
}


#define FRIENDS_STORE_MAGIC 0x249ab123
struct keep_mc_store {
  int magic;
  int list_id;
  int num;
};

int friends_data_store (struct connection *c, int op, const char *key, int len, int flags, int expire, int bytes)
{
  int user_id = 0;
  struct keep_mc_store *Data = 0;

  //key[len] = 0;

  if (verbosity > 0) {
    fprintf (stderr, "mc_store: op=%d, key=\"%s\", flags=%d, expire=%d, bytes=%d, noreply=%d\n", op, key, flags, expire, bytes, 0);
  }

  if (bytes >= 0 && bytes < 1048576) {
    if (sscanf (key, "userlist%d", &user_id) == 1 && user_id < 0) {
      if (!c->Tmp) {
        c->Tmp = alloc_head_buffer();
        assert (c->Tmp);
      }
      Data = (struct keep_mc_store *) c->Tmp->start;
      Data->magic = FRIENDS_STORE_MAGIC;
      Data->list_id = user_id;
      Data->num = np_news_parse_list (userlist, MAX_USERLIST_NUM, 1, &c->In, bytes);
      advance_write_ptr (c->Tmp, sizeof (struct keep_mc_store));
      if (Data->num > 0 && user_id < 0) {
        write_out (c->Tmp, userlist, Data->num * 4);
      }
    } else {
      advance_skip_read_ptr (&c->In, bytes);
    }
  } else {
    advance_skip_read_ptr (&c->In, bytes);
  }

  if (!Data || Data->num <= 0 || user_id >= 0) {
    write_out (&c->Out, "NOT_STORED\r\n", 12);
    flush_output (c);
    free_tmp_buffers (c);
  } else {
    write_out (&c->Out, "STORED\r\n", 8);
    flush_output (c);
  }

  return bytes;
}

int friends_generate_preget_query (char *buff, const char *key, int key_len, void *E, int n) {
  if (!user_num) return 0;
  struct friends_gather_extra* extra = E;
  if (extra->query_type != 1) {
  	return 0;
  }

  int r = sprintf (buff, "set userlist%d 0 0 %d\r\n0000", extra->list_id, user_num * 4 + 4);
  memcpy (buff + r, userlist, user_num * 4);
  r += user_num * 4;
  return r;
}

int friends_generate_new_key (char *buff, char *key, int len, void *E) {
	if (*key != '%') {
	  return sprintf (buff, "%%%s", key);
	} else {
	  return sprintf (buff, "%s", key);
	}
}

void friends_load_saved_data (struct connection *c) {
  struct keep_mc_store *Data = 0;
  if (c->Tmp) {
    Data = (struct keep_mc_store *) c->Tmp->start;
    assert (Data->magic == FRIENDS_STORE_MAGIC);
    user_num = Data->num;
  } else {
    user_num = 0;
    list_id = 0;
    return;
  }
  nb_iterator_t R;
  nbit_set (&R, c->Tmp);
  assert (nbit_advance (&R, sizeof (struct keep_mc_store)) == sizeof (struct keep_mc_store));
  assert (nbit_read_in (&R, userlist, 4 * Data->num) == 4 * Data->num);
  list_id = Data->list_id;
  //free_tmp_buffers (c);
}

static int QL;
static int Q[MAX_QUERY];
void *friends_store_gather_extra (const char *key, int key_len) {
  int user_id;
  int x = 0;
  int raw = 0;
  int query_type = 0;
  int t;

  if (*key == '%') {
    raw = 1;
  }

  if (sscanf (key+raw, "common_friends_num%d:%n", &user_id, &x) >= 1){
    x += raw;
    query_type = 1;
  } else if (sscanf (key + raw, "common_friends%d,%d%n", &user_id, &t, &x) >= 2) {
    x += raw;
    query_type = 2;
    user_num = 1;
  } else {
    x = 0;
  }
  if (x <= 1) {
    return 0;
  }

  if (query_type == 1) {
    if (verbosity >= 4) {
      fprintf (stderr, "list_id = %d\n", list_id);
    }
    QL = np_parse_list_str (Q, MAX_QUERY, 1, key + x, key_len - x);
    if (QL == 1 && Q[0] < 0 && list_id == Q[0]) {
    } else {
      list_id = 0;
      if (QL > 0) {
	      memcpy (userlist, Q, QL * 4);
      	user_num = QL;
      }
    }
    if (QL <= 0) {
      return 0;
    }
  }

  struct friends_gather_extra* extra = zzmalloc (sizeof (struct friends_gather_extra));
  extra->num = user_num + 1;
  extra->list_id = list_id;
  extra->raw = raw;
  extra->query_type = query_type;
  return extra;
}

int friends_check_query (int type, const char *key, int key_len) {
  int l = eat_at (key, key_len);
  key += l;
  key_len -= l;
  if (type == mct_get) {
    return (key_len >= 19 && (!strncmp (key, "common_friends_num", 18) || !strncmp (key, "%common_friends_num", 19))) || 
           (key_len >= 15 && (!strncmp (key, "common_friends", 14) || !strncmp (key, "%common_friends", 15)));
  } else if (type == mct_set || type == mct_replace || type == mct_add) {
    return (key_len >= 8 && !strncmp (key, "userlist", 8));
  } else {
    return 0;
  }
}



int friends_free_gather_extra (void *E) {
  zzfree (E, sizeof (struct friends_gather_extra));
  return 0;
}

int friends_use_preget_query (void *extra) {
  return (((struct friends_gather_extra *)extra)->query_type == 1) && (((struct friends_gather_extra *)extra)->list_id < 0);
}

struct mc_proxy_merge_functions friends_extension_functions = {
  .free_gather_extra = friends_free_gather_extra,
  .merge_end_query = friends_merge_end_query,
  .generate_preget_query = friends_generate_preget_query,
  .generate_new_key = friends_generate_new_key,
  .store_gather_extra = friends_store_gather_extra,
  .check_query = friends_check_query,
  .merge_store = friends_data_store,
  .load_saved_data = friends_load_saved_data,
  .use_preget_query = friends_use_preget_query
};

struct mc_proxy_merge_conf friends_extension_conf = {
  .use_at = 0,
  .use_preget_query = 1
};
