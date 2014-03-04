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
#include "net-memcache-server.h"
#include "net-memcache-client.h"
#include "mc-proxy-merge-extension.h"
#include "kdb-data-common.h"

#include "mc-proxy-statsx-extension.h"

#define max(x,y) ((x) < (y) ? (y) : (x))

typedef struct gather_heap_entry {
  unsigned long long value64;
  int *cur, *last;
  int remaining;
} gh_entry_t;

static gh_entry_t GH_E[MAX_CLUSTER_SERVERS];
static gh_entry_t *GH[MAX_CLUSTER_SERVERS+1];
static int GH_N, GH_total, GH_mode;

static void clear_gather_heap (int mode) {
  GH_N = 0;
  GH_total = 0;
  GH_mode = mode;
}

static inline void load_heap_v (gh_entry_t *H) {
  int *data = H->cur;
  H->value64 = -*data;
}



static int gather_heap_insert (struct gather_entry *GE) {
  int cnt, cnt2, sz;
  gh_entry_t *H;
  assert (GH_N < MAX_CLUSTER_SERVERS);
  if (GE->res_read < 4 || !GE->data) {
    if (verbosity >= 3) {
      fprintf (stderr, "GE->num = %d, GE->res_read = %d, GE->data = %p\n", GE->num, GE->res_read, GE->data);
    }
    return 0;
  }
  cnt2 = cnt = (GE->res_read >> 2);
  sz = (GH_mode & FLAG_DOUBLE) ? 2 : 1;
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
		fprintf (stderr, "get_gather_heap_head: GH->last = %p (value64 = %016llx)\n", GH[1]->last, GH[1]->value64);
	}
  return GH_N ? GH[1]->cur : 0;
}

static void gather_heap_advance (void) {
  gh_entry_t *H;
  int sz = (GH_mode & FLAG_DOUBLE) ? 2 : 1;
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

int statsx_check_query (int type, const char *key, int key_len) {
  if (type != mct_get) {
    return 0;
  }
  int l = eat_at (key, key_len);
  key += l;
  key_len -= l;
  if (key[0] == '%') {
    key ++;
    key_len --;
  }
  return ((key_len >= 7) && !strncmp (key, "counter", 7)) || ((key_len >= 5) && !strncmp (key, "views", 5)) || ((key_len >= 8) && !strncmp (key, "visitors", 8)) || (key_len >= 8 && !strncmp (key, "versions", 8)) ||
         (key_len >= 13 && !strncmp (key, "monthly_views", 13)) || (key_len >= 16 && !strncmp (key, "monthly_visitors", 16));
}

int statsx_one_int (const char *key, int key_len) {
  int l = eat_at (key, key_len);
  key += l;
  key_len -= l;
  if (key[0] == '%') {
    key ++;
    key_len --;
  }
  return ((key_len >= 5) && !strncmp (key, "views", 5)) || ((key_len >= 8) && !strncmp (key, "visitors", 8));
}

void *statsx_store_gather_extra (const char *key, int key_len) {
  struct statsx_gather_extra *extra = zzmalloc0 (sizeof (struct statsx_gather_extra));
  extra->Q_raw = key[0] == '%';
  if (!strncmp (key, "counter", 7)) {
    extra->flags = FLAG_COUNTER;
    if (key[key_len - 1] == '?' || key[key_len - 2] == '?') {
      extra->flags |= FLAG_NO_SERIALIZE;
    }
  } else if (!strncmp (key, "views", 5) || !strncmp (key, "visitors", 8)) {
    extra->flags = FLAG_ONE_INT;
  } else if (!strncmp (key, "versions", 8)) {
    extra->flags = FLAG_UNION;
  } else if (!strncmp (key, "monthly_views", 13) || !strncmp (key, "monthly_visitors", 16)) {
    extra->flags = FLAG_UNION | FLAG_DOUBLE | FLAG_MONTHLY;
  }
  if (key[key_len - 1] == '!' || key[key_len - 2] == '!') {
    extra->flags |= FLAG_BAD_SERVERS;
  }
  return extra;
}

int statsx_free_gather_extra (void *E) {
  zzfree (E, sizeof (struct statsx_gather_extra));
  return 0;
}

int statsx_merge_generate_new_key (char *buff, char *key, int key_len, void *E) {
  int l;
  if (key[0] == '%') {
    l = sprintf (buff, "%s", key);
  } else {
    l = sprintf (buff, "%%%s", key);
  }
  if (buff[l - 1] == '!') {
    buff[--l] = 0;
  }
  return l;
}


int *unserialize_list_raw (int *ptr, int *data, int num) {
  int x = *(ptr++);
  assert (x <= num);
  int i;
  for (i = 0; i < x; i++) {
    data[i] += *(ptr++);
  }
  return ptr;
}

int *unserialize_list2_raw (int *ptr, int *data, int num) {
  int x = *(ptr++);
  assert (x <= num);
  int i, j;
  for (i = 0; i < x; i++) {
    int y = *(ptr++);
    int z = *(ptr++);
    int result = -1;
    for (j = 0; j < num; j ++) if (data[2 * j]  == y) {
      data[2 * j + 1] += z;
      result = j;
      break;
    }
    if (result < 0 && data[2 * (num - 1) + 1] < z) {
      data[2 * (num - 1) + 1] = z;
      data[2 * (num - 1)] = y;
      result = (num - 1);
    }
    while (result > 0) {
      if (data[2 * result + 1] > data[2 * result - 1]) {
        int t;
        t = data[2 * result + 1]; data[2 * result + 1] = data[2 * result - 1]; data[2 * result - 1] = t;
        t = data[2 * result + 0]; data[2 * result + 0] = data[2 * result - 2]; data[2 * result - 2] = t;
        result --;
      } else {
        break;
      }
    }
  }
  return ptr;
}

int *unserialize_list2_raw_new (int *ptr, int *data, int num) {
  int x = *(ptr ++);
  int i;
  for (i = 0; i < x; i++) {
    if (*data == num) {
      ptr += 2;
    } else {
      data[2 * (*data) + 1] = *(ptr ++);
      data[2 * (*data) + 2] = *(ptr ++);
      (*data) ++;
    }
  }
  return ptr;
}

int comparel (const void * a, const void * b){ 
  return ( *(int*)a - *(int*)b );
}

int comparer (const void * a, const void * b){ 
  return -( *((int*)a + 1)- *((int*)b + 1) );
}



void prepare_list2_raw_new (int *data, int num) {
  assert (*data <= num);
  qsort (data + 1, *data, 8, comparel);
  int i;
  int cl = 0;
  for (i = 0; i < *data; i++) {
    if (i == 0 || data[2 * i + 1] != data[2 * i - 1]) {
      data[2 * cl + 1] = data[2 * i + 1];
      data[2 * cl + 2] = data[2 * i + 2];
      cl ++;
    } else {
      data[2 * cl] += data[2 * i + 2];
    }
  }
  *data = cl;
  qsort (data + 1, *data, 8, comparer);
}

void gather_sum_res (struct gather_entry *e, struct statsx_gather_extra *C) {
  int *ptr = e->data;
  C->views += *(ptr++);
  C->unique_visitors += *(ptr++);
  C->deletes += *(ptr++);
  C->created_at = max (C->created_at, *ptr);
  ptr++;
  C->valid_until = max (C->valid_until, *ptr);
  ptr++;
  ptr = unserialize_list_raw (ptr, C->visitors_sex, MAX_SEX);
  ptr = unserialize_list_raw (ptr, C->visitors_age, MAX_AGE); 
  ptr = unserialize_list_raw (ptr, C->visitors_mstatus, MAX_MSTATUS); 
  ptr = unserialize_list_raw (ptr, C->visitors_polit, MAX_POLIT); 
  ptr = unserialize_list_raw (ptr, C->visitors_section, MAX_SECTION); 
  ptr = unserialize_list_raw (ptr, C->visitors_sex_age, MAX_AGE * 2); 
  ptr = unserialize_list2_raw_new (ptr, C->visitors_cities, MAX_CITIES);
  ptr = unserialize_list2_raw (ptr, C->visitors_countries, MAX_COUNTRIES);
  ptr = unserialize_list2_raw (ptr, C->visitors_geoip_countries, MAX_GEOIP_COUNTRIES);
  ptr = unserialize_list_raw (ptr, C->visitors_source, MAX_SOURCE);
  ptr = unserialize_list_raw (ptr, C->subcnt, MAX_SUBCOUNTER);
}

//#define BUFF_SIZE 2000000
static char buff[2000000];
static int Q_raw;
static int Q_bad_servers;
static int bad_servers;

int is_null (int *list, int num) {
  int i;
  for (i = 0; i < num; i++) if (list[i]) {
    return 0;
  }
  return 1;
}

int is_null_new (int *list, int num) {
  return !(*list);
}

int not_null (int *list, int num) {
  int i;
  int cnt = 0;
  for (i = 0; i < num; i++) if (list[i]) {
    cnt ++;
  }
  return cnt;
}

static char *serialize_list (char *ptr, const char *name, int *list, int num) {
  int i, cnt = 0;
  for (i = 0; i < num; i++) {
    if (list[i]) { cnt++; }
  }
  if (!cnt) {
    return ptr;
  }
  ptr += sprintf (ptr, "s:%ld:\"%s\";a:%d:{", (long) strlen(name), name, cnt);
  for (i = 0; i < num; i++) {
    if (list[i]) {
      ptr += sprintf (ptr, "i:%d;i:%d;", i+1, list[i]);
    }
  }
  *ptr++ = '}';
  return ptr;
}

static int *serialize_list_raw (int *ptr, int *list, int num) {
  int i;
  if (is_null (list, num)) {
    *(ptr++) = 0;
    return ptr;
  }
  *(ptr++) = num;
  for (i = 0; i < num; i++) {
    *(ptr++) = list[i];
  }
  return ptr;
}


char *serialize_list2 (char *ptr, const char *name, int *list, int num) {
  int x = 0;
  while (x < num && list[2 * x + 1]) {
    x ++;
  }
  if (!x) {
    return ptr;
  }
  ptr += sprintf (ptr, "s:%ld:\"%s\";a:%d:{", (long) strlen (name), name, x);
  int i;
  for (i = 0; i < 2*x; i++) {
    ptr += sprintf (ptr, "i:%d;", list[i]);
  }
  *ptr++ = '}';
  return ptr;
}

int *serialize_list2_raw (int *ptr, int *list, int num) {
  int i;
  if (is_null (list, 2 * num)) {
    *(ptr++) = 0;
    return ptr;
  }
  *(ptr++) = num;
  for (i = 0; i < 2*num; i++) {
    *(ptr++) = list[i];
  }
  return ptr;
}

static char *serialize_list2_new (char *ptr, const char *name, int *list, int num) {
  if (!*list) {
    return ptr;
  }
  ptr += sprintf (ptr, "s:%ld:\"%s\";a:%d:{", (long) strlen (name), name, *list);
  int i;
  for (i = 0; i < 2 * (*list); i++) {
    ptr += sprintf (ptr, "i:%d;", list[i + 1]);
  }
  *ptr++ = '}';
  return ptr;
}

static int *serialize_list2_raw_new (int *ptr, int *list, int num) {
  int i;
  *(ptr++) = *list;
  for (i = 0; i < 2 * (*list); i++) {
    *(ptr++) = list[i + 1];
  }
  return ptr;
}

static char *serialize_list2a (char *ptr, const char *name, int *list, int num) {
  int x = 0;
  while (x < num && list[2 * x + 1]) {
    x ++;
  }
  if (!x) {
    return ptr;
  }
  ptr += sprintf (ptr, "s:%ld:\"%s\";a:%d:{", (long) strlen (name), name, x);
  int i;
  for (i = 0; i < 2*x; i++) {
    ptr += (i&1) ? sprintf (ptr, "i:%d;", list[i]) : sprintf(ptr, "s:3:\"%c%c%c\";", (char)((list[i]>>16) & 0xff), (char)((list[i]>>8) & 0xff), (char)(list[i] & 0xff));
  }
  *ptr++ = '}';
  return ptr;
}

static int *serialize_list2a_raw (int *ptr, int *list, int num) {
  int i;
  if (is_null (list, 2 * num)) {
    *(ptr++) = 0;
    return ptr;
  }
  *(ptr++) = num;
  for (i = 0; i < 2*num; i++) {
    *(ptr++) = list[i];
  }
  return ptr;
}

static char *serialize_subcnt_list (char *ptr, struct statsx_gather_extra *C) {
  /* 
     don't output broken stats (dirty hack)
     php: mktime (12, 0, 0, 2, 2, 2011) == 1296637200
          Feb 02 2011, 12:00
  */
  int num = not_null (C->subcnt, MAX_SUBCOUNTER);
  if (!num) {
    return ptr;
  }
  if (C->valid_until < 1296637200) {
    ptr += sprintf (ptr, "s:5:\"extra\";a:0:{}");
    return ptr;
  }
  int i;
  ptr += sprintf (ptr, "s:5:\"extra\";a:%d:{", num);
  for (i = 0; i < MAX_SUBCOUNTER; i++) 
    if (C->subcnt[i]) {
      ptr += sprintf (ptr, "i:%d;i:%d;", i, C->subcnt[i]);
    }
  *ptr++ = '}';
  return ptr;
}

static int *serialize_subcnt_list_raw (int *ptr, struct statsx_gather_extra *C) {
  /* 
     don't output broken stats (dirty hack)
     php: mktime (12, 0, 0, 2, 2, 2011) == 1296637200
          Feb 02 2011, 12:00
  */
  if (C->valid_until < 1296637200) {
    *(ptr++)  = 0;
    return ptr;
  }
  if (is_null (C->subcnt, MAX_SUBCOUNTER)) {
    *(ptr++)  = 0;
    return ptr;
  }
  *(ptr++) = MAX_SUBCOUNTER;
  int i;
  for (i = 0; i < MAX_SUBCOUNTER; i++) {
    *(ptr++) = C->subcnt[i];
  }
  return ptr;
}

int get_counter_serialized_raw (char *buffer, struct statsx_gather_extra *C) {
  int *ptr = (int *)buffer;
  *(ptr++) = C->views;
  *(ptr++) = C->unique_visitors;
  *(ptr++) = C->deletes;
  *(ptr++) = C->created_at;
  *(ptr++) = C->valid_until;
  ptr = serialize_list_raw (ptr, C->visitors_sex, MAX_SEX); 
  ptr = serialize_list_raw (ptr, C->visitors_age, MAX_AGE); 
  ptr = serialize_list_raw (ptr, C->visitors_mstatus, MAX_MSTATUS); 
  ptr = serialize_list_raw (ptr, C->visitors_polit, MAX_POLIT); 
  ptr = serialize_list_raw (ptr, C->visitors_section, MAX_SECTION); 
  ptr = serialize_list_raw (ptr, C->visitors_sex_age, MAX_AGE * 2); 
  ptr = serialize_list2_raw_new (ptr, C->visitors_cities, MAX_CITIES);
  ptr = serialize_list2a_raw (ptr, C->visitors_countries, MAX_COUNTRIES);
  ptr = serialize_list2a_raw (ptr, C->visitors_geoip_countries, MAX_GEOIP_COUNTRIES);
  ptr = serialize_list_raw (ptr, C->visitors_source, MAX_SOURCE);
  ptr = serialize_subcnt_list_raw (ptr, C);
  return (char *)ptr - buffer;
}

int get_counter_serialized (char *buffer, struct statsx_gather_extra *C) {
  prepare_list2_raw_new (C->visitors_cities, MAX_CITIES);
  if (Q_raw) {
    return get_counter_serialized_raw (buffer, C);
  }
  int cnt = 6 + (Q_bad_servers != 0);
  char *ptr = buffer;
  if (!is_null (C->visitors_age, MAX_AGE)) { cnt++; }
  if (!is_null (C->visitors_mstatus, MAX_MSTATUS)) { cnt++; }
  if (!is_null (C->visitors_polit, MAX_POLIT)) { cnt++; }
  if (!is_null (C->visitors_section, MAX_SECTION)) { cnt++; }
  if (!is_null_new (C->visitors_cities, 2 * MAX_CITIES)) { cnt++; }
  if (!is_null (C->visitors_sex_age, MAX_SEX_AGE)) { cnt++; }
  if (!is_null (C->visitors_countries, 2 * MAX_COUNTRIES)) { cnt++; }
  if (!is_null (C->visitors_geoip_countries, 2 * MAX_GEOIP_COUNTRIES)) { cnt++; }
  if (!is_null (C->visitors_source, MAX_SOURCE)) { cnt++; }
  if (!is_null (C->subcnt, MAX_SUBCOUNTER)) { cnt++; }
  ptr += sprintf (ptr, "a:%d:{s:5:\"views\";i:%d;s:8:\"visitors\";i:%d;s:7:\"deletes\";i:%d;"
    "s:7:\"created\";i:%d;s:7:\"expires\";i:%d;"
    "s:3:\"sex\";a:2:{i:1;i:%d;i:2;i:%d;}", 
    cnt, C->views, C->unique_visitors, C->deletes, C->created_at, C->valid_until, 
    C->visitors_sex[0], C->visitors_sex[1]);
  if (Q_bad_servers) {
    ptr += sprintf (ptr, "s:11:\"bad_servers\";i:%d;", bad_servers);
  }
  ptr = serialize_list (ptr, "age", C->visitors_age, MAX_AGE); 
  ptr = serialize_list (ptr, "marital_status", C->visitors_mstatus, MAX_MSTATUS); 
  ptr = serialize_list (ptr, "political_views", C->visitors_polit, MAX_POLIT); 
  ptr = serialize_list (ptr, "section", C->visitors_section, MAX_SECTION); 
  ptr = serialize_list (ptr, "sex_age", C->visitors_sex_age, MAX_SEX_AGE); 
  ptr = serialize_list2_new (ptr, "cities", C->visitors_cities, MAX_CITIES);
  ptr = serialize_list2a (ptr, "countries", C->visitors_countries, MAX_COUNTRIES);
  ptr = serialize_list2a (ptr, "geoip_countries", C->visitors_geoip_countries, MAX_GEOIP_COUNTRIES);
  ptr = serialize_list (ptr, "sources", C->visitors_source, MAX_SOURCE);
  ptr = serialize_subcnt_list (ptr, C);
  *ptr++ = '}';
  *ptr = 0;
  if (verbosity >= 4) {
    fprintf (stderr, "%s\n", buffer);
  }
  return ptr - buffer;
}


int statsx_merge_end_query (struct connection *c, const char *key, int key_len, void *E, struct gather_entry *data, int tot_num) {
  struct statsx_gather_extra *extra = E;
  Q_raw = extra->Q_raw;

  int i;
  int one_int = extra->flags & FLAG_ONE_INT;
  int union_mode = extra->flags & FLAG_UNION;
  Q_bad_servers = extra->flags & FLAG_BAD_SERVERS;
  int to_serialize = !(extra->flags & FLAG_NO_SERIALIZE);
  int monthly = extra->flags & FLAG_MONTHLY;
  int counter_mode = extra->flags & FLAG_COUNTER;
  bad_servers = 0;
  
  if (verbosity >= 4) {
	  fprintf (stderr, "one_int = %d, union_mode = %d\n", one_int, union_mode);
	}
  int res = 0;

  clear_gather_heap (extra->flags & FLAG_DOUBLE);

  for (i = 0; i < tot_num; i++) if (data[i].res_bytes > 0) {
  	//fprintf (stderr, "!!!");
    if (!one_int) {
      if (!union_mode) {
        gather_sum_res (&data[i], extra);
      } else {
        gather_heap_insert (&data[i]);
      }
    } else {
      if (data[i].res_bytes >= 4) {
        res += data[i].data[0];
      }
    }
  } else {
    if (data[i].num == -1) {
      bad_servers ++;
    }
    if (verbosity >= 4) {
      fprintf (stderr, "Dropping result %d (num = %d)\n", i, data[i].num);
    }
  }
  

  if (union_mode) {
    res = 0;
    int last = -1;
    long long last_res = 0;
    if (!Q_raw && Q_bad_servers) {
      if (res) {
        buff[res ++] = ',';
      }
      res += sprintf (buff + res, "%d", bad_servers);
    }
    while (res < 100000) {
      int *t = get_gather_heap_head ();
      if (!t) {
        break;
      }
      if (last == *t) {
        if (monthly) {
          last_res += *(t + 1);
        }
        gather_heap_advance ();
        continue;
      }
      if (last != -1) {
        if (monthly) {
          if (Q_raw) {
            *(int *)(buff + res) = last_res;
            res += 4;
          } else {
            if (res) {
              buff[res ++] = ',';
            }
            res += sprintf (buff + res, "%lld", last_res);
          }
        }
        if (Q_raw) {
          *(int *)(buff + res) = last;
          res += 4;
        } else {
          if (res) {
            buff[res ++] = ',';
          }
          if (!monthly) {
            res += sprintf (buff + res, "%d", last);
          } else {
            res += sprintf (buff + res, "%d,%d", last % 100, last / 100);
          }
        }
      }
      last = *t;
      last_res = *(t + 1);
      gather_heap_advance ();
    }
    if (last != -1) {
      if (monthly) {
        if (Q_raw) {
          *(int *)(buff + res) = last_res;
          res += 4;
        } else {
          if (res) {
            buff[res ++] = ',';
          }
          res += sprintf (buff + res, "%lld", last_res);
        }
      }
      if (Q_raw) {
        *(int *)(buff + res) = last;
        res += 4;
      } else {
        if (res) {
          buff[res ++] = ',';
        }
        if (!monthly) {
          res += sprintf (buff + res, "%d", last);
        } else {
          res += sprintf (buff + res, "%d,%d", last % 100, last / 100);
        }
      }
    }
  }

  if (counter_mode) {
    res = get_counter_serialized (buff, extra);
  }
  


  if (one_int) {
    to_serialize = 0;
    if (!Q_raw) {
      if (!Q_bad_servers) {
        res = sprintf (buff, "%d", res);
      } else {
        res = sprintf (buff, "%d,%d", bad_servers, res);
      }
    } else {      
      *(int *)buff = res;
      res = 4;
    }
  }
  if (to_serialize && !Q_raw && !union_mode) {
    write_out (&c->Out, buff+res, sprintf (buff+res, "VALUE %s 1 %d\r\n", key, res));
  } else {
    write_out (&c->Out, buff+res, sprintf (buff+res, "VALUE %s 0 %d\r\n", key, res));
  }
  write_out (&c->Out, buff, res);
  write_out (&c->Out, "\r\n", 2);

  return 0;
}


struct mc_proxy_merge_conf statsx_extension_conf = {
  .use_at = 1,
  .use_preget_query = 0
};

struct mc_proxy_merge_functions statsx_extension_functions = {
  .free_gather_extra = statsx_free_gather_extra,
  .merge_end_query = statsx_merge_end_query,
  .generate_preget_query = default_generate_preget_query,
  .generate_new_key = statsx_merge_generate_new_key,
  .store_gather_extra = statsx_store_gather_extra,
  .check_query = statsx_check_query,
  .merge_store = default_merge_store,
  .load_saved_data = default_load_saved_data
};

