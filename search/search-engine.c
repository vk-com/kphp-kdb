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

    Copyright 2009-2013 Vkontakte Ltd
              2009-2010 Nikolai Durov
              2009-2010 Andrei Lopatin
              2010-2013 Vitaliy Valtman
              2010-2013 Anton Maydell
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
#include <signal.h>

#include "md5.h"
#include "net-events.h"
#include "net-buffers.h"
#include "server-functions.h"
#include "search-data.h"
#include "word-split.h"
#include "stemmer-new.h"
#include "net-connections.h"
#include "net-memcache-server.h"
#include "net-crypto-aes.h"
#include "utils.h"
#include "search-value-buffer.h"
#include "search-profile.h"
#include "am-stats.h"
#include "net-rpc-server.h"
#include "net-rpc-common.h"
#include "vv-tl-parse.h"
#include "search-tl.h"
#include "vv-tl-aio.h"

#include "search-interface-structures.h"

#ifndef COMMIT
#define COMMIT "unknown"
#endif

#define	VERSION_STR	"search-engine-0.4.1-r11"
const char FullVersionStr[] = VERSION_STR " compiled at " __DATE__ " " __TIME__ " by gcc " __VERSION__ " "
#ifdef __LP64__
  "64-bit"
#else
  "32-bit"
#endif
" after commit " COMMIT;

#define mytime() (get_utime(CLOCK_MONOTONIC))

#define MAX_QUERY_LEN 10000
#define MAX_CHANGE_RATES 100000
#define MAX_TAG_LEN 10000
#define MAX_TEXT_LEN 10000

/*
 *
 *		SEARCH ENGINE
 *
 */

struct in_addr settings_addr;
int udp_enabled;

/* stats counters */
int start_time;
static long long search_queries, delete_queries, update_queries, delete_items_with_hash_queries,
  minor_update_queries, increment_queries, extended_search_queries, relevance_search_queries,
  delete_hash_queries, delete_hash_query_items,
  add_tags_queries,
  hashlist_assign_max_queries, change_many_rates_queries, delete_items_with_rate_queries;

static int return_empty_record_on_error = 1;

static double binlog_load_time, index_load_time;
static double worst_delete_items_with_hash_time, worst_delete_items_with_rate_time, worst_hashlist_assign_max_time, worst_change_many_rates_time;
static int delete_many_rates_mask = 0;
static char *allowed_deleted_by_rate = "";

#define STATS_BUFF_SIZE	(1 << 20)
static char stats_buff[STATS_BUFF_SIZE];

conn_type_t ct_search_engine_server = {
  .magic = CONN_FUNC_MAGIC,
  .title = "search_engine_server",
  .accept = accept_new_connections,
  .init_accepted = mcs_init_accepted,
  .run = server_read_write,
  .reader = server_reader,
  .writer = server_writer,
  .parse_execute = mcs_parse_execute,
  .close = server_close_connection,
  .free_buffers = free_connection_buffers,
  .init_outbound = server_failed,
  .connected = server_failed,
  .wakeup = 0,
  .alarm = 0,
#ifndef NOAES
  .crypto_init = aes_crypto_init,
  .crypto_free = aes_crypto_free,
  .crypto_encrypt_output = aes_crypto_encrypt_output,
  .crypto_decrypt_input = aes_crypto_decrypt_input,
  .crypto_needed_output_bytes = aes_crypto_needed_output_bytes,
#endif
};

int memcache_store (struct connection *c, int op, const char *key, int key_len, int flags, int delay, int size);
int memcache_get (struct connection *c, const char *key, int key_len);
int memcache_incr (struct connection *c, int op, const char *key, int key_len, long long arg);
int memcache_delete (struct connection *c, const char *key, int key_len);
int memcache_stats (struct connection *c);

struct memcache_server_functions memcache_methods = {
  .execute = mcs_execute,
  .mc_store = memcache_store,
  .mc_get_start = mcs_get_start,
  .mc_get = memcache_get,
  .mc_get_end = mcs_get_end,
  .mc_incr = memcache_incr,
  .mc_delete = memcache_delete,
  .mc_version = mcs_version,
  .mc_stats = memcache_stats,
  .mc_check_perm = mcs_default_check_perm,
  .mc_init_crypto = mcs_init_crypto
};

struct rpc_server_functions rpc_methods = {
  .execute = default_tl_rpcs_execute,
  .check_ready = server_check_ready,
  .flush_packet = rpcs_flush_packet,
  .rpc_check_perm = rpcs_default_check_perm,
  .rpc_init_crypto = rpcs_init_crypto,
  .rpc_close = default_tl_close_conn,
  .memcache_fallback_type = &ct_search_engine_server,
  .memcache_fallback_extra = &memcache_methods
};


int Q_raw;

void copy_key (search_query_heap_en_t *E, void *key) {
  sprintf (E->query, "%.*s", SEARCH_QUERY_MAX_SIZE - 1, (char *) key);
}

int do_perform_query (const char *key, int len) {
  if (len < LAST_SEARCH_QUERY_BUFF_SIZE) {
    memcpy (last_search_query, key, len);
    last_search_query[len] = 0;
  } else {
    strcpy (last_search_query, "TOO LONG QUERY");
  }

  search_query_heap_en_t E;
  search_query_start (&E);
  int res = perform_query ();
  search_query_end (&E, res, (void *) key, copy_key);
  vkprintf (1, "perform_query result = %d\n", res);
  return res;
}

static int return_error (struct connection *c, const char *key, int len, int dog_len, char *q_end) {
  vkprintf (1, "ERROR near '%.256s'\n", q_end);
  if (return_empty_record_on_error) {
    struct value_buffer b;
    if (!value_buffer_init (&b, c, key - dog_len, len + dog_len, Q_raw, 64)) {
      return 0;
    }
    b.output_int (&b, 0);
    return value_buffer_return (&b);
  } else {
    return return_one_key (c, key - dog_len, stats_buff, sprintf (stats_buff, "ERROR near '%.256s'\n", q_end));
  }
}

int do_relevance_search_query (struct connection *c, const char *key, int len, int dog_len) {
  int i, Q_raw, err = 0;

  char *q_end = parse_relevance_search_query ((char *) key, &Q_raw, &err, 1);

  if (err < 0) {
    vkprintf (1, "parse_relevance_search_query (%.*s) returns error code %d.\n", len, key, err);
    return return_error (c, key, len, dog_len, q_end);
  }

  search_queries++;
  relevance_search_queries++;

  int res = do_perform_query (key, len);

  struct value_buffer b;
  if (!value_buffer_init (&b, c, key - dog_len, len + dog_len, Q_raw, 64)) {
    return 0;
  }
  if (Q_hash_group_mode) {
    assert (Q_limit > 0);
    b.output_int (&b, R_tot);
    for (i = 0; i < R_cnt; i++) {
      if (!value_buffer_flush (&b) ) { return -1; }
      b.output_char (&b, ',');
      b.output_item_id (&b, R[i]->item_id);
      b.output_char (&b, ',');
      b.output_int (&b, RV[i]);
      b.output_char (&b, ',');
      b.output_hash (&b, extract_hash_item (R[i]));
    }
    return value_buffer_return (&b);
  }

  b.output_int (&b, res);
  for (i = 0; i < R_cnt; i++) {
    if (!value_buffer_flush (&b) ) { return -1; }
    b.output_char (&b, ',');
    b.output_item_id (&b, R[i]->item_id);
    b.output_char (&b, ',');
    b.output_int (&b, RV[i]);
  }
  return value_buffer_return (&b);
}

int do_search_query (struct connection *c, const char *key, int len, int dog_len) {
  static char buff[2048];
  int i = -1;
  int res;
  char *ptr, *q_end;
  vkprintf (1, "got: %s\n", key);
  if (key[6] == 'x' || key[6] == 'u') {
    return do_relevance_search_query (c, key, len, dog_len);
  }

  res = 0;
  ptr = (char*) (key + 6);

  Q_limit = 0;
  Q_raw = 0;
  Q_order = 0;
  Q_words = 0;
  Q_extmode = 0;
  Q_hash_group_mode = 0;
  Q_min_priority = 0;
  q_end = 0;

  if (*ptr == '#') {
    ptr++;
    while (!q_end && *ptr != '(' && *ptr != '[') {

      int sm = get_sorting_mode (*ptr);
      if (sm >= 0) {
      	Q_order = (Q_order & (FLAG_ENTRY_SORT_SEARCH | FLAG_PRIORITY_SORT_SEARCH)) | sm;
      	ptr++;
        continue;
      }

      switch (*ptr) {
      case '%':
      	Q_raw = 1;
        ptr++;
        break;
      case 'P':
      	Q_order |= FLAG_ENTRY_SORT_SEARCH;
      	ptr++;
      	break;
      case 'U':
        Q_hash_group_mode = 1;
      	ptr++;
      	break;
      case 'V':
        Q_order |= FLAG_ONLY_TITLE_SEARCH;
        Q_min_priority = 1;
        ptr++;
        break;
      case 'T':
        Q_order |= FLAG_PRIORITY_SORT_SEARCH;
        ptr++;
        break;
      case 'X':
        Q_extmode = 1;
        ptr++;
        break;
      case '?':
      	Q_order = 17;
      	ptr++;
      	break;
      case '0' ... '9':
      	Q_limit = strtol (ptr, (char **)&ptr, 10);
      	if (Q_limit < 0) { Q_limit = 0; }
        if (Q_limit > MAX_RES) { Q_limit = MAX_RES; }
      	break;
      default:
        q_end = ptr;
      }
    }
  }

  if (*ptr != '(' && *ptr != '[' && !q_end) {
    q_end = ptr;
  }

  q_end = parse_query ((char*)ptr, 1);

  if (q_end) {
    vkprintf (1, "ERROR near '%.256s'\n", q_end);
    return return_error (c, key, len, dog_len, q_end);
  }

  if (Q_hash_group_mode && Q_limit <= 0) {
    return return_one_key (c, key - dog_len, buff, sprintf (buff, "ERROR: search U-mode without selection limit is disabled!"));
  }

  search_queries++;
  extended_search_queries += Q_extmode;

  res = do_perform_query (key, len);

  struct value_buffer b;
  if (!value_buffer_init (&b, c, key - dog_len, len + dog_len, Q_raw, 64)) {
    return 0;
  }
  int tp = Q_order & 0xff;
  const int output_rating = tp < MAX_RATES || tp == MAX_RATES + 1 || (Q_order & (FLAG_ENTRY_SORT_SEARCH | FLAG_PRIORITY_SORT_SEARCH));
  if (Q_hash_group_mode) {
    if (Q_limit <= 0) {
      if (!Q_raw) {
        b.output_int (&b, res);
      } else {
        if (res < 0) {
          b.output_int (&b, res);
        } else {
          b.output_int (&b, R_tot_undef_hash);
          //b.output_char (&b, ',');
          b.output_int (&b, hs.filled);
          for (i = 0; i < hs.size; i++) {
            if (hs.h[i] != 0) {
              if (!value_buffer_flush (&b) ) { return -1; }
              //b.output_char (&b, ',');
              b.output_long (&b, hs.h[i]);
            }
          }
        }
      }
      if (hs.h != 0) {
        hashset_ll_free (&hs);
      }
    } else {
      b.output_int (&b, R_tot);
      for (i = 0; i < R_cnt; i++) {
        if (!value_buffer_flush (&b) ) { return -1; }
        b.output_char (&b, ',');
        b.output_item_id (&b, R[i]->item_id);
        if (output_rating) {
          b.output_char (&b, ',');
          b.output_int (&b, RV[i]);
        }
        b.output_char (&b, ',');
        b.output_hash (&b, extract_hash_item (R[i]));
      }
    }
    return value_buffer_return (&b);
  }

  b.output_int (&b, res);
  for (i = 0; i < R_cnt; i++) {
    if (!value_buffer_flush (&b) ) { return -1; }
    b.output_char (&b, ',');
    b.output_item_id (&b, R[i]->item_id);
    if (output_rating) {
      b.output_char (&b, ',');
      b.output_int (&b, RV[i]);
    }
  }
  return value_buffer_return (&b);
}

static int do_get_hash (struct connection *c, const char *key, int len, int dog_len) {
  int owner_id, short_id, op = 0;
  long long item_id;
  static char value[32];
  if (sscanf (key, "hash%d_%d ", &owner_id, &short_id) == 2) {
    if (owner_id && short_id > 0) {
      item_id = (((long long) short_id) << 32) + (unsigned) owner_id;
      op = 1;
    }
  } else if (sscanf (key, "hash%d ", &short_id) == 1) {
    if (short_id > 0) {
      item_id = short_id;
      op = 1;
    }
  }
  if (op) {
    long long hash;
    if (get_hash (&hash, item_id)) {
      if (hash == 0) { return return_one_key (c, key - dog_len, "0", 1); }
      else { return return_one_key (c, key - dog_len, value, sprintf (value, "%016llx", hash)); }
    }
  }
  return 0;
}

static int do_contained (struct connection *c, const char *key, int len, int dog_len) {
  int owner_id, short_id, op = 0, x = -1;
  char ch = 0;
  long long item_id;
  static char value[32];
  char *expr;

  if (sscanf (key + 9, "%d_%d%n%c", &owner_id, &short_id, &x, &ch) >= 3 && ch == '(') {
    if (owner_id && short_id > 0 && x >= 0) {
      item_id = (((long long) short_id) << 32) + (unsigned) owner_id;
      expr = (char *) (key + 9 + x);
      op = 1;
    }
  }

  x = -1;
  ch = 0;
  if (!op && sscanf (key + 9, "%d%n%c", &short_id, &x, &ch) >= 2 && ch == '(') {
    if (short_id > 0) {
      item_id = short_id;
      expr = (char *) (key + 9 + x);
      op = 1;
    }
  }

  if (op) {
    int r = do_contained_query (item_id, &expr);
    if (r >= 0) {
      return return_one_key (c, key - dog_len, value, sprintf (value, "%d", r));
    } else {
      return return_one_key (c, key - dog_len, value, sprintf (value, "ERROR near '%.256s'\n", expr));
    }
  }
  return 0;
}

static int do_get_rate (struct connection *c, const char *key, int len, int dog_len) {
  int rates[2];
  long long item_id = 0;
  int owner_id, short_id, op = 0;
  static char value[32];

  if (sscanf (key+1, "ate%d_%d ", &owner_id, &short_id) == 2) {
    if (owner_id && short_id > 0) {
      item_id = (((long long) short_id) << 32) + (unsigned) owner_id;
      op = 1;
    }
  } else if (sscanf (key+1, "ate%d ", &short_id) == 1) {
    if (short_id > 0) {
      item_id = short_id;
      op = 1;
    }
  }

  if (op) {
    int p = get_sorting_mode (*key);

    vkprintf (4, "p = %d, *key = %c\n", p, *key);

    if (p >= 0 && p < MAX_RATES) {
      if (get_single_rate (rates, item_id, p)) {
        return return_one_key (c, key - dog_len, value, sprintf (value, "%d", rates[0]));
      }
    }
    op = 0;
  }

  if (sscanf (key, "rates%d_%d ", &owner_id, &short_id) == 2) {
    if (owner_id && short_id > 0) {
      item_id = (((long long) short_id) << 32) + (unsigned) owner_id;
      op = 1;
    }
  } else if (sscanf (key, "rates%d ", &short_id) == 1) {
    if (short_id > 0) {
      item_id = short_id;
      op = 1;
    }
  }

  if (op && get_rates (rates, item_id)) {
    return return_one_key (c, key - dog_len, value, sprintf (value, "%d,%d", rates[0], rates[1]));
  }

  return 0;
}

int search_prepare_stats (struct connection *c);

int memcache_get (struct connection *c, const char *key, int key_len) {
  int dog_len = get_at_prefix_length (key, key_len);
  key += dog_len;
  key_len -= dog_len;

  if (key_len >= 5 && !memcmp (key, "stats", 5)) {
    int len = search_prepare_stats (c);
    return return_one_key (c, key - dog_len, stats_buff, len);
  }

  if (key_len >= 8 && !memcmp (key, "search", 6)) {
    return do_search_query (c, key, key_len, dog_len);
  }

  if (key_len >= 4 && (!memcmp (key+1, "ate", 3))) {
    do_get_rate (c, key, key_len, dog_len);
    return 0;
  }

  if (key_len >= 4 && (!memcmp (key, "hash", 4))) {
    do_get_hash (c, key, key_len, dog_len);
    return 0;
  }

  if (key_len >= 9 && !memcmp (key, "contained", 9)) {
    do_contained (c, key, key_len, dog_len);
  }

  if (key_len == 20 && !memcmp (key, "worst_search_queries", 20)) {
    int len = search_query_worst (stats_buff, sizeof (stats_buff));
    return return_one_key (c, key - dog_len, stats_buff, len);
  }

  if (verbosity >= 1) {
    fprintf (stderr, "unknown query \"get %s\"\n", key);
  }
  return 0;
}

int memcache_delete (struct connection *c, const char *key, int key_len) {
  int owner_id, item_id;
  vkprintf (1, "delete \"%s\"\n", key);
  int res = 0;
  long long h = 0;
  char d, e;
  if (sscanf (key, "itemswithhash%llx", &h) == 1) {
    delete_hash_queries++;
    res = do_delete_items_with_hash (h);
    delete_hash_query_items += res;
  } else if (sscanf (key, "item%d_%d ", &owner_id, &item_id) == 2 && owner_id && item_id > 0) {
    delete_queries++;
    vkprintf (1, "delete_item (%d,%d)\n", owner_id, item_id);
    vkprintf (3, "delete: item_id=%lld\n", ((long long) item_id << 32) + (unsigned) owner_id);
    res = do_delete_item (((long long) item_id << 32) + (unsigned) owner_id);
  } else if (sscanf (key, "item%d ", &item_id) == 1 && item_id > 0) {
    delete_queries++;
    vkprintf (1, "delete_item (%d,%d)\n", 0, item_id);
    vkprintf (3, "delete: item_id=%lld\n", (long long) item_id);
    res = do_delete_item (item_id);
  } else if (sscanf (key, "reset_all_%cate%c", &d, &e) == 2 && e == 's') {
    int p = get_sorting_mode (d);
    if (p >= 0 && p < MAX_RATES) {
      delete_queries++;
      res = do_reset_all_rates (p);
    }
  }
  if (res > 0) {
    write_out (&c->Out, "DELETED\r\n", 9);
  } else {
    write_out (&c->Out, "NOT_FOUND\r\n", 11);
  }
  return 0;
}


static int increment_rate (struct connection *c, const char *key, int len, long long drate) {
  int rate;
  long long item_id = 0;
  int owner_id, short_id, op = 0, res;
  static char value[32];
  int p = get_sorting_mode (*key);

  vkprintf (4, "p = %d, *key=%c\n", p, *key);

  if (p >= 0 && p < MAX_RATES) {
    if (sscanf (key+1, "ate%d_%d", &owner_id, &short_id) == 2) {
      if (owner_id && short_id > 0) {
        item_id = (((long long) short_id) << 32) + (unsigned) owner_id;
        op = p+1;
      }
    } else if (sscanf (key+1, "ate%d", &short_id) == 1) {
      if (short_id > 0) {
        item_id = short_id;
        op = p+1;
      }
    }
  }

  vkprintf (3, "incr: item_id=%lld\n", (long long)item_id);

  if (op > 0) {
    switch(op) {
      case 1:
        res = do_incr_rate(item_id, drate);
        break;
      case 2:
        res = do_incr_rate2(item_id, drate);
        break;
      default:
        res = do_incr_rate_new (item_id, p, drate);
        break;
    }
    if (res) {
      if (!get_single_rate (&rate, item_id, p)) {
        return -1;
      }
      write_out (&c->Out, value, sprintf (value, "%d\r\n", rate));
      return 0;
    }
  }

  write_out (&c->Out, "NOT_FOUND\r\n", 11);
  return 0;
}


int memcache_incr (struct connection *c, int op, const char *key, int len, long long arg) {
  if (len >= 4 && (!strncmp (key+1, "ate", 3))) {
    if (increment_rate (c, key, len, (op == 0) ? arg : -arg)) {
      return 0;
    }
  }

  if (verbosity > 1) {
    fprintf (stderr, "error in incr query (%s)\n", key);
  }
  return 0;

}


#define VALUE_BUFF_SIZE 1000000
char value_buff [VALUE_BUFF_SIZE];

int do_assign_max_multiple_hash (struct connection *c, int size, int rate_id, int value) {
  char *a = value_buff;
  assert (read_in (&c->In, a, size) == size);
  a[size] = 0;
  int i, n = 1;
  for (i = 0; i < size; i++) {
    if (a[i] == ',') {
      n++;
    }
  }
  if (n < 10) {
    n = 10;
  }
  struct hashset_ll H;
  if (!hashset_ll_init (&H, n)) {
    return 0;
  }
  char *p = a;
  while (*p) {
    char *q = strchr (p, ',');
    if (q != NULL) {
      *q = 0;
    }
    long long u;
    if (sscanf (p, "%llx", &u) != 1) {
      break;
    }
    if (H.filled >= n) { break; }
    vkprintf (3, "u = %llx\n", u);
    if (u) {
      hashset_ll_insert (&H, u);
    }
    if (q == NULL) {
      break;
    }
    p = q + 1;
  }

  int r = do_assign_max_rate_using_hashset (&H, rate_id, value);
  hashset_ll_free (&H);
  return r;
}

static int int_list_foreach (const char *s, void *ctx, void (*func)(void *, int)) {
  int k = 0;
  const char *p = s;
  errno = 0;
  while (*p) {
    k++;
    char *q;
    int u = (int) strtol (p, &q, 10);
    if (errno) {
      vkprintf (2, "%s: couln't parse '%s'. %m\n", __func__, p);
      return -1;
    }
    func (ctx, u);
    if (!(*q)) {
      break;
    }
    if (*q != ',') {
      vkprintf (2, "%s: expected comma or NUL, but '%c' found\n", __func__, *q);
      return -1;
    }
    p = q + 1;
  }
  return k;
}

typedef struct {
  struct hashmap_int_int HII;
  int b[2];
  int k;
} ctx_change_many_rates_t;

static void func_change_many_rates (void *ctx, int i) {
  ctx_change_many_rates_t *self = (ctx_change_many_rates_t *) ctx;
  self->b[self->k++] = i;
  if (self->k == 2) {
    int slot;
    if (self->b[0]) {
      if (!hashmap_int_int_get (&self->HII, self->b[0], &slot)) {
        self->HII.h[slot].key = self->b[0];
        self->HII.filled++;
      }
      self->HII.h[slot].value = self->b[1];
    }
    self->k = 0;
  }
}

int do_change_many_rates (struct connection *c, int size, int rate_id) {
  vkprintf (4, "%s: (c:%p, size:%d, rate_id:%d)\n", __func__, c, size, rate_id);
  char *a = value_buff;
  assert (read_in (&c->In, a, size) == size);
  a[size] = 0;
  int i, n = 1;
  for (i = 0; i < size; i++) {
    if (a[i] == ',') {
      n++;
    }
  }

  if (n & 1) {
    vkprintf (2, "%s: list length(%d) is odd.\n", __func__, n);
    return 0;
  }

  n >>= 1;

  if (n < 10) {
    n = 10;
  }

  ctx_change_many_rates_t ctx;
  memset (&ctx, 0, sizeof (ctx));
  if (!hashmap_int_int_init (&ctx.HII, n)) {
    return 0;
  }

  if (int_list_foreach (value_buff, &ctx, func_change_many_rates) < 0) {
    hashmap_int_int_free (&ctx.HII);
    return 0;
  }

  int r = do_change_multiple_rates_using_hashmap (&ctx.HII, rate_id);
  hashmap_int_int_free (&ctx.HII);
  return r;
}

int do_delete_multiple_hash (struct connection *c, int size) {
  char *a = value_buff;
  assert (read_in (&c->In, a, size) == size);
  a[size] = 0;
  int i, n = 1;
  for (i = 0; i < size; i++) {
    if (a[i] == ',') {
      n++;
    }
  }
  if (n < 10) {
    n = 10;
  }
  struct hashset_ll H;
  if (!hashset_ll_init (&H, n)) {
    return 0;
  }
  char *p = a;
  while (*p) {
    char *q = strchr (p, ',');
    if (q != NULL) {
      *q = 0;
    }
    long long u;
    if (sscanf (p, "%llx", &u) != 1) {
      break;
    }
    if (H.filled >= n) { break; }
    vkprintf (3, "u = %llx\n", u);
    if (u) {
      hashset_ll_insert (&H, u);
    }
    if (q == NULL) {
      break;
    }
    p = q + 1;
  }
  int t = do_delete_items_with_hash_using_hashset (&H);
  delete_hash_query_items += t;
  hashset_ll_free (&H);
  return 1;
}

typedef struct {
  struct hashmap_int_int HII;
  int b[2];
  int k;
} ctx_delete_multiple_t;

static void func_delete_multiple_rate (void *ctx, int i) {
  struct hashset_int *H = (struct hashset_int *) ctx;
  if (i) {
    hashset_int_insert (H, i);
  }
}

int do_delete_multiple_rate (struct connection *c, int size, int rate_id) {
  char *a = value_buff;
  assert (read_in (&c->In, a, size) == size);
  a[size] = 0;
  int i, n = 1;
  for (i = 0; i < size; i++) {
    if (a[i] == ',') {
      n++;
    }
  }
  if (n < 10) {
    n = 10;
  }
  struct hashset_int H;
  if (!hashset_int_init (&H, n)) {
    return 0;
  }

  if (int_list_foreach (value_buff, &H, func_delete_multiple_rate) < 0) {
    hashset_int_free (&H);
    return 0;
  }

  int t = do_delete_items_with_rate_using_hashset (&H, rate_id);
  delete_hash_query_items += t;
  hashset_int_free (&H);
  return 1;
}

int do_add (struct connection *c, long long item_id, int size) {
  assert (read_in (&c->In, value_buff, size) == size);
  value_buff[size] = 0;
  return do_add_item_tags (value_buff, size, item_id);
}

int do_store (struct connection *c, long long item_id, int rate, int rate2, int op, int size) {
  int res = 0;
  long long hash;

  assert (read_in (&c->In, value_buff, size) == size);
  value_buff[size] = 0;

  switch (op) {
  case 0:
    if (size >= 0) {
      res = do_change_item (value_buff, size, item_id, rate, rate2);
    } //else {
      //res = do_change_item_long (&c->In, Data->len, Data->item_id, Data->rate, Data->rate2);
    //}
    break;
  case 1 ... MAX_RATES:
    if (sscanf(value_buff, "%d", &rate) == 1) {
      res = do_set_rate_new (item_id, op - 1, rate);
    }
   break;
  case (MAX_RATES+1):
    if (sscanf (value_buff, "%d,%d ", &rate, &rate2) == 2) {
      res = do_set_rates (item_id, rate, rate2);
    }
    break;
  case (MAX_RATES+2):
    /*
    i = 16;
    if (i >= size) { i = size; }
    value_buff[i--] = 0;
    while (i >= 0) {
      value_buff[i] = tolower(value_buff[i]);
      i--;
    }
    */
    if (sscanf (value_buff, "%16llx", &hash) == 1) {
      res = do_set_hash (item_id, hash);
    }
    break;

  default:
    break;
  }

  return res;

}


int memcache_store (struct connection *c, int op, const char *key, int key_len, int flags, int delay, int size) {
  int res = 0;
  int rate, rate2, owner_id, item_id;
  long long ritem_id = 0;
  int cur_op;
  int ok = 0;
  int no_reply = 0;
  char ch;
  double perform_query_time;
  vkprintf (1, "mc_store: op=%d, key=\"%s\", flags=%d, expire=%d, bytes=%d, noreply=%d\n", op, key, flags, delay, size, no_reply);

  if (binlog_disabled) {
    return -2;
  }

  if (op == 1 && key_len == 22 && !memcmp (key, "delete_items_with_", 18) && size >= 0 && size < VALUE_BUFF_SIZE) {
    if (!memcmp (key + 18, "hash", 4)) {
      delete_items_with_hash_queries++;
      perform_query_time = -get_rusage_time ();
      res = do_delete_multiple_hash (c, size);
      perform_query_time += get_rusage_time ();
      if (worst_delete_items_with_hash_time < perform_query_time) {
        worst_delete_items_with_hash_time = perform_query_time;
      }
      return (res > 0) ? 1 : 0;
    } else if (!memcmp (key + 19, "ate", 3)) {
      int p = get_sorting_mode (key[18]);
      vkprintf (4, "%s: p = %d\n", __func__, p);
      if (p >= 0 && p < MAX_RATES && ((1 << p) & delete_many_rates_mask)) {
        delete_items_with_rate_queries++;
        perform_query_time = -get_rusage_time ();
        res = do_delete_multiple_rate (c, size, p);
        perform_query_time += get_rusage_time ();
        if (worst_delete_items_with_rate_time < perform_query_time) {
          worst_delete_items_with_rate_time = perform_query_time;
        }
        return (res > 0) ? 1 : 0;
      }
    }
  }

  if (op == 1 && key_len >= 20 && !memcmp (key, "hashlist_assign_max_", 20) && sscanf (key + 20, "%cate%d", &ch, &rate) == 2 && size >= 0 && size < VALUE_BUFF_SIZE) {
    int p = get_sorting_mode (ch);
    if (p >= 0 && p < MAX_RATES) {
      hashlist_assign_max_queries++;
      perform_query_time = -get_rusage_time ();
      res = do_assign_max_multiple_hash (c, size, p, rate);
      perform_query_time += get_rusage_time ();
      if (worst_hashlist_assign_max_time < perform_query_time) {
        worst_hashlist_assign_max_time = perform_query_time;
      }
      return res;
    }
  }

  if (op == 1 && key_len >= 17 && !memcmp (key, "change_many_", 12) && !memcmp (key + 13, "ates", 4) && size >= 0 && size < VALUE_BUFF_SIZE) {
    int p = get_sorting_mode (key[12]);
    vkprintf (4, "%s: p = %d\n", __func__, p);
    if (p >= 0 && p < MAX_RATES && ((1 << p) & delete_many_rates_mask)) {
      change_many_rates_queries++;
      perform_query_time = -get_rusage_time ();
      res = do_change_many_rates (c, size, p);
      perform_query_time += get_rusage_time ();
      if (worst_change_many_rates_time < perform_query_time) {
        worst_change_many_rates_time = perform_query_time;
      }
      return res;
    }
  }

  owner_id = 0;
  if (size >= 65536) {
    return -2;
  }

  if (op == 3 && size >= 0 && size < 256) {
    if (key_len >= 4 && !memcmp (key, "tags", 4)) {
      if (sscanf (key + 4, "%d_%d", &owner_id, &item_id) == 2 && owner_id && item_id > 0) {
        ok = 1;
      } else if (sscanf (key, "%d ", &item_id) == 1 && item_id > 0) {
        owner_id = 0;
        ok = 1;
      }
      if (ok) {
        add_tags_queries++;
        ritem_id = owner_id ? ((long long) item_id << 32) + (unsigned) owner_id : item_id;
        res = do_add (c, ritem_id, size);
        return (res > 0) ? 1 : 0;
      }
    }
  }

  if (op == 1 && size >= 0 && size < 65536 &&
     ((sscanf (key, "item%d#%d,%d ", &item_id, &rate, &rate2) == 3 && item_id > 0)
      || (sscanf (key, "item%d_%d#%d,%d ", &owner_id, &item_id, &rate, &rate2) == 4 && owner_id && item_id > 0)
    )) {
    update_queries++;
    ritem_id = owner_id ? ((long long) item_id << 32) + (unsigned) owner_id : item_id;
    vkprintf (3, "store: item_id=%016llx\n", ritem_id);
    cur_op = 0;
    ok = 1;
  }

  if (!ok && op == 1 && size >= 0 && size < 256) {
    int act = 0;
    owner_id = 0;
    if (sscanf (key+1, "ate%d_%d ", &owner_id, &item_id) == 2 && owner_id && item_id > 0) {
      act = 1;
    } else if (sscanf (key+1, "ate%d ", &item_id) == 1 && item_id > 0) {
      owner_id = 0;
      act = 1;
    }

    if (act) {
      int p = get_sorting_mode (*key);
      vkprintf (4, "p = %d, *key=%c\n", p, *key);
      act = (p >= 0 && p < MAX_RATES) ? (p+1) : 0;
    }

    if (!act && sscanf (key, "rates%d_%d ", &owner_id, &item_id) == 2 && owner_id && item_id > 0) {
      act = MAX_RATES+1;
    } else if (sscanf (key, "rates%d ", &item_id) == 1 && item_id > 0) {
      owner_id = 0;
      act = MAX_RATES+1;
    }

    if (!act && sscanf (key, "hash%d_%d ", &owner_id, &item_id) == 2 && owner_id && item_id > 0) {
      act = MAX_RATES+2;
    } else if (sscanf (key, "hash%d ", &item_id) == 1 && item_id > 0) {
      owner_id = 0;
      act = MAX_RATES+2;
    }

    if (act) {
      minor_update_queries++;
      rate = 0;
      rate2 = 0;
      cur_op = act;
      ritem_id = owner_id ? ((long long) item_id << 32) + (unsigned) owner_id : item_id;
      ok = 1;
    }
  }

  vkprintf (3, "store: item_id=%016llx, cur_op = %d\n", ritem_id, cur_op);

  if (ok) {
    res = do_store (c, ritem_id, rate, rate2, cur_op, size);
  } else {
    return -2;
  }

  return (res > 0) ? 1 : 0;
}

int memcache_stats (struct connection *c) {
  int len = search_prepare_stats (c);
  write_out (&c->Out, stats_buff, len);
  write_out (&c->Out, "END\r\n", 5);
  return 0;
}

int search_prepare_stats (struct connection *c) {
  stats_buffer_t sb;
  sb_prepare (&sb, c, stats_buff, STATS_BUFF_SIZE);
  sb_memory (&sb, AM_GET_MEMORY_USAGE_SELF);
  sb_printf (&sb, "malloc_memory_used\t%lld\n", get_malloc_memory_used ());
  SB_BINLOG;
  SB_INDEX;
  sb_printf (&sb,
    "index_items\t%d\n"
    "index_words\t%d\n"
    "index_hapax_legomena\t%d\n"
    "index_items_with_hash\t%d\n"
    "index_data_bytes\t%ld\n",
    idx_items, idx_words, idx_hapax_legomena,
    idx_items_with_hash, idx_bytes);

  int k;
  long long golomb_cb = Header.compression_bytes[0] + Header.compression_bytes[1];
  for (k = 0; k < 4; k++) {
    long long t1 = Header.compression_bytes[2*k+0],
              t2 = Header.compression_bytes[2*k+1],
              t = t1 + t2;
    if (t > 0) {
      if (golomb_cb > 0) {
        sb_printf (&sb, "%s\t%lld(%.6lf%%)=%lld(%.6lf%%)+%lld(%.6lf%%)\n",
                       list_get_compression_method_description (k),
                       t, safe_div (100.0 * t, golomb_cb),
                       t1, safe_div (100.0 * t1, Header.compression_bytes[0]),
                       t2, safe_div (100.0 * t2, Header.compression_bytes[1]));
      } else {
        sb_printf (&sb, "%s\t%lld=%lld+%lld\n",
                       list_get_compression_method_description (k),
                       t,
                       t1,
                       t2);
      }
    }
  }

  sb_printf (&sb,
      "index_compression_methods\t%s+%s\n"
      "left_subtree_size_threshold\t%d\n",
    list_get_compression_method_description (Header.word_list_compression_methods[0]),
    list_get_compression_method_description (Header.word_list_compression_methods[1]),
    Header.left_subtree_size_threshold);

  SB_PRINT_QUERIES(search_queries);
  SB_PRINT_QUERIES(extended_search_queries);
  SB_PRINT_QUERIES(relevance_search_queries);
  SB_PRINT_QUERIES(delete_queries);
  SB_PRINT_QUERIES(delete_hash_queries);
  SB_PRINT_QUERIES(update_queries);
  SB_PRINT_QUERIES(minor_update_queries);
  SB_PRINT_QUERIES(increment_queries);
  SB_PRINT_QUERIES(delete_items_with_hash_queries);
  SB_PRINT_QUERIES(hashlist_assign_max_queries);
  SB_PRINT_QUERIES(change_many_rates_queries);
  SB_PRINT_QUERIES(add_tags_queries);
  sb_printf (&sb,
      "items\t%d\n"
      "items_marked_as_deleted\t%d\n"
      "items_freed\t%d\n"
      "index_items_deleted\t%d\n"
      "tree_nodes_allocated\t%d\n"
      "tree_nodes_unused\t%d\n"
      "rebuild_hashmap_calls\t%lld\n"
      "last_search_query\t%s\n",
    tot_items,
    del_items,
    tot_freed_deleted_items,
    mod_items,
    alloc_tree_nodes,
    free_tree_nodes,
    rebuild_hashmap_calls,
    last_search_query);
  SB_PRINT_I64(delete_hash_query_items);
  SB_PRINT_I64(assign_max_set_rate_calls);
  SB_PRINT_I64(change_multiple_rates_set_rate_calls);
  SB_PRINT_TIME(worst_delete_items_with_hash_time);
  SB_PRINT_TIME(worst_hashlist_assign_max_time);
  SB_PRINT_TIME(worst_change_many_rates_time);

  SB_PRINT_I32(use_stemmer);
  SB_PRINT_I32(universal);
  SB_PRINT_I32(hashtags_enabled);
  SB_PRINT_I32(wordfreqs_enabled);
  SB_PRINT_I32(import_only_mode);
  SB_PRINT_I32(stemmer_version);
  SB_PRINT_I32(word_split_version);
  SB_PRINT_I32(word_split_utf8);
  SB_PRINT_I32(tag_owner);
  SB_PRINT_I32(listcomp_version);
  SB_PRINT_I32(creation_date);
  SB_PRINT_I32(delete_many_rates_mask);
  SB_PRINT_I32(return_empty_record_on_error);

  sb_printf (&sb, "version\t%s\n", FullVersionStr);

  return sb.pos;
}

static long long tl_get_item_id (void) {
  int n = tl_fetch_int ();
  if (n != 1 && n != 2) {
    tl_fetch_set_error ("number of ints in id should be 1 or 2", TL_ERROR_VALUE_NOT_IN_RANGE);
    return -1;
  }
  int owner_id = 0, item_id;
  if (n == 2) {
    owner_id = tl_fetch_int ();
  }
  item_id = tl_fetch_int ();
  if (tl_fetch_error ()) {
    return -1;
  }
  if (item_id <= 0) {
    tl_fetch_set_error ("item_id should be positive", TL_ERROR_VALUE_NOT_IN_RANGE);
    return -1;
  }
  return (n == 2) ? ((long long) item_id << 32) + (unsigned) owner_id : item_id;
}

TL_DO_FUN(set_rate)
  int res = do_set_rate_new (e->item_id, e->rate_type, e->rate_value);
  tl_store_int (res <= 0 ? TL_BOOL_FALSE : TL_BOOL_TRUE);
TL_DO_FUN_END

TL_PARSE_FUN(set_rate)
  e->item_id = tl_get_item_id ();
  if (e->item_id == -1) { return 0; }
  e->rate_type = tl_fetch_int ();
  if (e->rate_type < 0 || e->rate_type >= MAX_RATES) {
    tl_fetch_set_error_format (TL_ERROR_VALUE_NOT_IN_RANGE, "Rate_type should be in range (0...%d)", MAX_RATES - 1);
    return 0;
  }
  e->rate_value = tl_fetch_int ();
TL_PARSE_FUN_END

TL_DO_FUN(incr_rate)
  int res = do_incr_rate_new (e->item_id, e->rate_type, e->rate_value);
  tl_store_int (res <= 0 ? TL_BOOL_FALSE : TL_BOOL_TRUE);
TL_DO_FUN_END

TL_PARSE_FUN(incr_rate, int decrement)
  e->item_id = tl_get_item_id ();
  if (e->item_id == -1) { return 0; }
  e->rate_type = tl_fetch_int ();
  if (e->rate_type < 0 || e->rate_type >= MAX_RATES) {
    tl_fetch_set_error_format (TL_ERROR_VALUE_NOT_IN_RANGE, "Rate_type should be in range (0...%d)", MAX_RATES - 1);
    return 0;
  }
  e->rate_value = tl_fetch_int ();
  if (decrement) {
    e->rate_value *= -1;
  }
TL_PARSE_FUN_END

TL_DO_FUN(get_rate)
  int r = 0;
  int res = get_single_rate (&r, e->item_id, e->rate_type);
  if (res <= 0) {
    tl_store_int (TL_MAYBE_FALSE);
  } else {
    tl_store_int (TL_MAYBE_TRUE);
    tl_store_int (r);
  }
TL_DO_FUN_END

TL_PARSE_FUN(get_rate, void)
  e->item_id = tl_get_item_id ();
  if (e->item_id == -1) { return 0; }
  e->rate_type = tl_fetch_int ();
  if (e->rate_type < 0 || e->rate_type >= MAX_RATES) {
    tl_fetch_set_error_format (TL_ERROR_VALUE_NOT_IN_RANGE, "Rate_type should be in range (0...%d)", MAX_RATES - 1);
    return 0;
  }
TL_PARSE_FUN_END

TL_DO_FUN(set_hash)
  int res = do_set_hash (e->item_id, e->hash);
  tl_store_int (res <= 0 ? TL_BOOL_FALSE : TL_BOOL_TRUE);
TL_DO_FUN_END

TL_PARSE_FUN(set_hash)
  e->item_id = tl_get_item_id ();
  if (e->item_id == -1) { return 0; }
  e->hash = tl_fetch_long ();
TL_PARSE_FUN_END

TL_DO_FUN(get_hash)
  long long hash;
  int x = get_hash (&hash, e->item_id);
  if (x <= 0) {
    tl_store_int (TL_MAYBE_FALSE);
  } else {
    tl_store_int (TL_MAYBE_TRUE);
    tl_store_long (hash);
  }
TL_DO_FUN_END

TL_PARSE_FUN(get_hash)
  e->item_id = tl_get_item_id ();
  if (e->item_id == -1) { return 0; }
TL_PARSE_FUN_END

TL_DO_FUN(delete_item)
  int res = do_delete_item (e->item_id);
  tl_store_int (res <= 0 ? TL_BOOL_FALSE : TL_BOOL_TRUE);
TL_DO_FUN_END

TL_PARSE_FUN(delete_item, void)
  e->item_id = tl_get_item_id ();
  if (e->item_id == -1) { return 0; }
TL_PARSE_FUN_END

TL_DO_FUN(set_item)
  int res = do_change_item (e->text, e->size, e->item_id, e->rate, e->sate);
  tl_store_int (res <= 0 ? TL_BOOL_FALSE : TL_BOOL_TRUE);
TL_DO_FUN_END

TL_PARSE_FUN(set_item, void)
  e->item_id = tl_get_item_id ();
  if (e->item_id == -1) { return 0; }
  e->rate = tl_fetch_int ();
  e->sate = tl_fetch_int ();
  e->size = tl_fetch_string0 (e->text, MAX_TEXT_LEN);
  extra->size += e->size + 1;
TL_PARSE_FUN_END

TL_DO_FUN(add_item_tags)
  int res = do_add_item_tags (e->text, e->size, e->item_id);
  tl_store_int (res <= 0 ? TL_BOOL_FALSE : TL_BOOL_TRUE);
TL_DO_FUN_END

TL_PARSE_FUN(add_item_tags)
  e->item_id = tl_get_item_id ();
  if (e->item_id == -1) { return 0; }
  e->size = tl_fetch_string0 (e->text, MAX_TAG_LEN);
  extra->size += e->size + 1;
TL_PARSE_FUN_END

TL_DO_FUN(delete_with_hash)
  int res = do_delete_items_with_hash (e->hash);
  tl_store_int (TL_BOOL_STAT);
  tl_store_int (res >= 0);
  tl_store_int (res < 0);
  tl_store_int (0);
TL_DO_FUN_END

TL_PARSE_FUN(delete_with_hash)
  e->hash = tl_fetch_long ();
TL_PARSE_FUN_END

TL_DO_FUN(delete_with_hashes)
  struct hashset_ll H;
  if (!hashset_ll_init (&H, e->n < 10 ? 10 : e->n)) {
    tl_fetch_set_error_format (TL_ERROR_INTERNAL, "Can not create hashset with size = %d", e->n < 10 ? 10 : e->n);
    return -1;
  }
  int i;
  for (i = 0; i < e->n; i++) {
    hashset_ll_insert (&H, e->data[i]);
  }
  int t = do_delete_items_with_hash_using_hashset (&H);
  delete_hash_query_items += t;
  hashset_ll_free (&H);
  tl_store_int (TL_BOOL_STAT);
  tl_store_int (t >= 0);
  tl_store_int (t < 0);
  tl_store_int (0);
TL_DO_FUN_END

TL_PARSE_FUN(delete_with_hashes)
  e->n = tl_fetch_int ();
  if (e->n < 0 || e->n >= MAX_CHANGE_RATES) {
    tl_fetch_set_error_format (TL_ERROR_VALUE_NOT_IN_RANGE, "size should be in range (0...%d)", MAX_CHANGE_RATES - 1);
    return 0;
  }
  tl_fetch_raw_data ((char *)e->data, 8 * e->n);
  extra->size += 8 * e->n;
TL_PARSE_FUN_END

TL_DO_FUN(incr_rate_by_hash)
  struct hashset_ll H;
  if (!hashset_ll_init (&H, e->n < 10 ? 10 : e->n)) {
    tl_fetch_set_error_format (TL_ERROR_INTERNAL, "Can not create hashset with size = %d", e->n < 10 ? 10 : e->n);
    return -1;
  }
  int i;
  for (i = 0; i < e->n; i++) {
    hashset_ll_insert (&H, e->data[i]);
  }
  int t = do_assign_max_rate_using_hashset (&H, e->rate_type, e->rate);
  hashset_ll_free (&H);
  tl_store_int (TL_BOOL_STAT);
  tl_store_int (t >= 0);
  tl_store_int (t < 0);
  tl_store_int (0);
TL_DO_FUN_END


TL_PARSE_FUN(incr_rate_by_hash)
  e->rate_type = tl_fetch_int ();
  if (e->rate_type < 0 || e->rate_type >= MAX_RATES) {
    tl_fetch_set_error_format (TL_ERROR_VALUE_NOT_IN_RANGE, "Rate_type should be in range (0...%d)", MAX_RATES - 1);
    return 0;
  }
  e->rate = tl_fetch_int ();
  e->n = tl_fetch_int ();
  if (e->n < 0 || e->n >= MAX_CHANGE_RATES) {
    tl_fetch_set_error_format (TL_ERROR_VALUE_NOT_IN_RANGE, "size should be in range (0...%d)", MAX_CHANGE_RATES - 1);
    return 0;
  }
  tl_fetch_raw_data ((char *)e->data, 8 * e->n);
  extra->size += 8 * e->n;
TL_PARSE_FUN_END

TL_DO_FUN(change_rates)
  struct hashset_ll H;
  if (!hashset_ll_init (&H, e->n < 10 ? 10 : e->n)) {
    tl_fetch_set_error_format (TL_ERROR_INTERNAL, "Can not create hashset with size = %d", e->n < 10 ? 10 : e->n);
    return -1;
  }
  int i;
  ctx_change_many_rates_t ctx;
  memset (&ctx, 0, sizeof (ctx));
  if (!hashmap_int_int_init (&ctx.HII, e->n)) {
    tl_fetch_set_error_format (TL_ERROR_INTERNAL, "Can not create hashmap with size = %d", e->n < 10 ? 10 : e->n);
    return -1;
  }
  for (i = 0; i < e->n; i++) {
    func_change_many_rates (&ctx, e->data[2 * i + 0]);
    func_change_many_rates (&ctx, e->data[2 * i + 1]);
  }
  hashmap_int_int_free (&ctx.HII);

  int t = do_change_multiple_rates_using_hashmap (&ctx.HII, e->rate_type);
  hashmap_int_int_free (&ctx.HII);

  tl_store_int (TL_BOOL_STAT);
  tl_store_int (t >= 0);
  tl_store_int (t < 0);
  tl_store_int (0);
TL_DO_FUN_END

TL_PARSE_FUN(change_rates)
  e->rate_type = tl_fetch_int ();
  if (e->rate_type < 0 || e->rate_type >= MAX_RATES) {
    tl_fetch_set_error_format (TL_ERROR_VALUE_NOT_IN_RANGE, "Rate_type should be in range (0...%d)", MAX_RATES - 1);
    return 0;
  }
  e->n = tl_fetch_int ();
  if (e->n < 0 || e->n >= MAX_CHANGE_RATES) {
    tl_fetch_set_error_format (TL_ERROR_VALUE_NOT_IN_RANGE, "size should be in range (0...%d)", MAX_CHANGE_RATES - 1);
    return 0;
  }
  tl_fetch_raw_data (e->data, 8 * e->n);
  extra->size += 8 * e->n;
TL_PARSE_FUN_END

extern int Q_hash_rating;
extern int Q_relevance;
extern double Q_Relevance_Power;
extern double Q_K_Opt_Tag;

TL_DO_FUN(search)
  Q_limit = e->limit;
  if (Q_limit < 0) { Q_limit = 0; }
  Q_raw = 0;
  Q_order = 0;
  Q_words = 0;
  Q_extmode = 0;
  Q_hash_group_mode = 0;
  Q_min_priority = 0;

  Q_min_priority = 0;
  Q_relevance = 0;

  if (e->flags & FLAG_SEARCHX) {
    Q_order = (MAX_RATES + 2) | FLAG_REVERSE_SEARCH;
    Q_order |= FLAG_PRIORITY_SORT_SEARCH;
  } else {
    if (e->flags & FLAG_SORT) {
      Q_order |= e->rate_type;
      if (e->flags & FLAG_SORT_DESC) {
        Q_order |= FLAG_REVERSE_SEARCH;
      }
    } else {
      if (e->flags & FLAG_RAND) {
        Q_order |= 17;
      } else {
        Q_order |= MAX_RATES;
        if (e->flags & FLAG_SORT_DESC) {
          Q_order |= FLAG_REVERSE_SEARCH;
        }
      }
    }
  }
  if (e->flags & FLAG_OCCURANCE_COUNT) {
    Q_order |= FLAG_ENTRY_SORT_SEARCH;
  }
  if (e->flags & FLAG_TITLE) {
    Q_order |= FLAG_ONLY_TITLE_SEARCH;
    Q_min_priority = 1;
  }
  if (e->flags & FLAG_WEAK_SEARCH) {
    Q_order |= FLAG_PRIORITY_SORT_SEARCH;
  }
  if (e->flags & FLAG_EXTENDED_MODE) {
    Q_extmode = 1;
  }
  if (e->flags & FLAG_GROUP_HASH) {
    Q_hash_group_mode = 1;
    if (Q_limit <= 0) {
      tl_fetch_set_error ("Hash grouping is impossible with limit = 0", TL_ERROR_QUERY_INCORRECT);
      return -1;
    }
  }

  int *data = e->data;
  init_ranges ();
  int i;
  for (i = 0; i < e->restr_num; i++) {
    add_range (data[0], data[1], data[2]);
    data += 3;
  }
  sort_ranges ();

  if (wordfreqs_enabled && e->relevance) {
    Q_relevance = 1;
    Q_Relevance_Power = e->relevance * 0.5;
  }

  Q_K_Opt_Tag = e->opttag;


  init_rate_weights ();
  for (i = 0; i < e->rate_weights_num; i++) {
    if (*(double *)(data + 1) <= 0) {
      tl_fetch_set_error_format (TL_ERROR_VALUE_NOT_IN_RANGE, "Weight should be positive (value %lf)", *(double *)(data + 1));
      return -1;
    }
    add_rate_weight (data[0], *(double *)(data + 1), data[2]);
    data += 4;
  }

  if (e->flags & FLAG_DECAY) {
    add_decay (e->decay_rate_type, e->decay_weight);
  }

  normalize_query_rate_weights ();

  Q_hash_rating = e->hash_rating;

  assert (data == e->data + 3 * e->restr_num + 4 * e->rate_weights_num);

  char *q_end = 0;
  if (e->flags & FLAG_SEARCHX) {
    q_end = parse_relevance_search_query_raw ((char *) data);
  } else {
    q_end = parse_query ((char *) data, 0);
  }
  if (q_end) {
    tl_fetch_set_error_format (TL_ERROR_QUERY_INCORRECT, "Error near %.256s", q_end);
    return -1;
  }

  int res = perform_query ();
  tl_store_int (TL_SEARCH_RESULT);
  int n = 1;
  for (i = 0; i < R_cnt; i++) {
    if (R[i]->item_id >> 32) {
      n = 2;
      break;
    }
  }
  tl_store_int (n);
  if (Q_limit > 0 && Q_hash_group_mode) {
    tl_store_int (R_tot);
  } else {
    tl_store_int (res);
  }
  vkprintf (2, "text = %s\n", stats_buff);
  vkprintf (2, "res = %d, R_cnt = %d, R_tot = %d\n", res, R_cnt, R_tot);
  tl_store_int (R_cnt);
  for (i = 0; i < R_cnt; i++) {
    if (n == 1) {
      tl_store_int (R[i]->item_id);
    } else {
      tl_store_int (R[i]->item_id);
      tl_store_int (R[i]->item_id >> 32);
    }
    if (e->flags & FLAG_SORT) {
      tl_store_int (RV[i]);
    }
    if (e->flags & FLAG_GROUP_HASH) {
      tl_store_long (extract_hash_item (R[i]));
    }
  }
TL_DO_FUN_END

TL_PARSE_FUN(search,void)
  search_queries ++;
  e->flags = tl_fetch_int ();
  e->limit = tl_fetch_int ();

  if (!(e->flags & FLAG_SEARCHX)) {
    if (e->flags & (FLAG_HASH_CHANGE | FLAG_RELEVANCE | FLAG_OPTTAG | FLAG_CUSTOM_RATE_WEIGHT | FLAG_DECAY | FLAG_CUSTOM_PRIORITY_WEIGHT)) {
      tl_fetch_set_error_format (TL_ERROR_QUERY_INCORRECT, "Trying to use features only enabled in searchx (flags %08x)", e->flags);
      return 0;
    }
  } else {
    if (e->flags & (FLAG_SORT | FLAG_SORT_DESC | FLAG_EXTENDED_MODE | FLAG_OCCURANCE_COUNT | FLAG_RAND)) {
      tl_fetch_set_error_format (TL_ERROR_QUERY_INCORRECT, "Trying to use features disabled in searchx (flags %08x)", e->flags);
      return 0;
    }
    relevance_search_queries ++;
  }

  if (e->flags & FLAG_SORT) {
    e->rate_type = tl_fetch_int ();
    if (e->rate_type < 0 || e->rate_type >= MAX_RATES) {
      tl_fetch_set_error_format (TL_ERROR_VALUE_NOT_IN_RANGE, "rate_type should be in range (0...%d)", MAX_RATES);
      return 0;
    }
  } else {
    e->rate_type = -1;
  }

  if (e->flags & FLAG_RESTR) {
    e->restr_num = tl_fetch_int ();
    if (e->restr_num < 0 || e->restr_num > MAX_RATES + 2) {
      tl_fetch_set_error_format (TL_ERROR_VALUE_NOT_IN_RANGE, "maximal number of restrictions is %d", MAX_RATES + 2);
      return 0;
    }
    tl_fetch_raw_data ((char *)(e->data), 12 * e->restr_num);
  } else {
    e->restr_num = 0;
  }

  if (e->flags & FLAG_EXACT_HASH) {
    long long hash = tl_fetch_long ();
    e->data[3 * e->restr_num + 0] = 14;
    e->data[3 * e->restr_num + 1] = (int)hash;
    e->data[3 * e->restr_num + 2] = (int)hash;
    e->data[3 * e->restr_num + 3] = 15;
    e->data[3 * e->restr_num + 4] = (int)(hash >> 32);
    e->data[3 * e->restr_num + 5] = (int)(hash >> 32);
    e->restr_num += 2;
  }

  extra->size += 12 * e->restr_num;

  if (e->flags & FLAG_RELEVANCE) {
    e->relevance = tl_fetch_double ();
    if ((e->relevance < 0.25 || e->relevance > 4) && e->relevance) {
      tl_fetch_set_error_format (TL_ERROR_VALUE_NOT_IN_RANGE, "Relevance shoud be in range (0.25 ... 4) or 0 (value %lf)", e->relevance);
      return 0;
    }
  } else {
    e->relevance = 0;
  }

  if (e->flags & FLAG_OPTTAG) {
    e->opttag = tl_fetch_double ();
  } else {
    e->opttag = 1;
  }

  if (e->flags & FLAG_CUSTOM_RATE_WEIGHT) {
    e->rate_weights_num = tl_fetch_int ();
    if (e->rate_weights_num < 0 || e->rate_weights_num > MAX_RATE_WEIGHTS) {
      tl_fetch_set_error_format (TL_ERROR_VALUE_NOT_IN_RANGE, "Number of weights must not exceed %d (value %d)", MAX_RATE_WEIGHTS, e->rate_weights_num);
      return 0;
    }
    int *data = e->data + e->restr_num * 3;
    tl_fetch_raw_data ((char *)data, 16 * e->rate_weights_num);
    extra->size += 16 * e->rate_weights_num;
  } else {
    e->rate_weights_num = 0;
  }

  if (e->flags & FLAG_CUSTOM_PRIORITY_WEIGHT) {
    int *data = e->data + e->restr_num * 3 + e->rate_weights_num * 4;
    data[0] = -1;
    *(double *)(data + 1) = tl_fetch_double ();
    data[3] = 0;
    e->rate_weights_num ++;
    extra->size += 16;
  }

  if (e->flags & FLAG_DECAY) {
    e->decay_rate_type = tl_fetch_int ();
    e->decay_weight = tl_fetch_double ();
    tl_fetch_int ();
  } else {
    e->decay_rate_type = -1;
  }

  if (e->flags & FLAG_HASH_CHANGE) {
    e->hash_rating = tl_fetch_int ();
    if (e->hash_rating < 0 || e->hash_rating >= MAX_RATES) {
      tl_fetch_set_error_format (TL_ERROR_VALUE_NOT_IN_RANGE, "rate_type should be in range (0...%d)", MAX_RATES - 1);
      return 0;
    }
  } else {
    e->hash_rating = -1;
  }
  char *text = (char *)(e->data + e->restr_num * 3 + e->rate_weights_num * 4);
  e->text_len = tl_fetch_string0 (text, MAX_QUERY_LEN);
  extra->size += e->text_len + 1;
TL_PARSE_FUN_END

struct tl_act_extra *search_parse_function (long long actor_id) {
  if (actor_id != 0) {
    tl_fetch_set_error ("Search only support actor_id = 0", TL_ERROR_WRONG_ACTOR_ID);
    return 0;
  }
  int op = tl_fetch_int ();
  if (tl_fetch_error ()) {
    return 0;
  }
  switch (op) {
  case TL_SEARCH_SET_RATE:
    return tl_set_rate ();
  case TL_SEARCH_INCR_RATE:
    return tl_incr_rate (0);
  case TL_SEARCH_DECR_RATE:
    return tl_incr_rate (1);
  case TL_SEARCH_GET_RATE:
    return tl_get_rate ();
  case TL_SEARCH_SET_HASH:
    return tl_set_hash ();
  case TL_SEARCH_GET_HASH:
    return tl_get_hash ();
  case TL_SEARCH_DELETE_ITEM:
    return tl_delete_item ();
  case TL_SEARCH_SET_ITEM:
    return tl_set_item ();
  case TL_SEARCH_ADD_TAGS:
    return tl_add_item_tags ();
  case TL_SEARCH_DELETE_WITH_HASH:
    return tl_delete_with_hash ();
  case TL_SEARCH_DELETE_WITH_HASHES:
    return tl_delete_with_hashes ();
  case TL_SEARCH_CHANGE_RATES:
    return tl_change_rates ();
  case TL_SEARCH_SEARCH:
    return tl_search ();
  case TL_SEARCH_INCR_RATE_BY_HASH:
    return tl_incr_rate_by_hash ();
  default:
    tl_fetch_set_error_format (TL_ERROR_UNKNOWN_FUNCTION_ID, "Unknown op %08x", op);
    return 0;
  }
}

int sfd;
void reopen_logs (void) {
  int fd;
  fflush (stdout);
  fflush (stderr);
  if ((fd = open ("/dev/null", O_RDWR, 0)) != -1) {
    dup2 (fd, 0);
    dup2 (fd, 1);
    dup2 (fd, 2);
    if (fd > 2) {
      close (fd);
    }
  }
  if (logname && (fd = open (logname, O_WRONLY|O_APPEND|O_CREAT, 0640)) != -1) {
    dup2 (fd, 1);
    dup2 (fd, 2);
    if (fd > 2) {
      close (fd);
    }
  }
  vkprintf (1, "logs reopened.\n");
}

static void sigint_immediate_handler (const int sig) {
  static const char message[] = "SIGINT handled immediately.\n";
  kwrite (2, message, sizeof (message) - (size_t)1);
  exit (1);
}

static void sigterm_immediate_handler (const int sig) {
  static const char message[] = "SIGTERM handled immediately.\n";
  kwrite (2, message, sizeof (message) - (size_t)1);
  exit (1);
}

static void sigint_handler (const int sig) {
  static const char message[] = "SIGINT handled.\n";
  kwrite (2, message, sizeof (message) - (size_t)1);
  pending_signals |= 1 << sig;
  signal (sig, sigint_immediate_handler);
}

static void sigterm_handler (const int sig) {
  static const char message[] = "SIGTERM handled.\n";
  kwrite (2, message, sizeof (message) - (size_t)1);
  pending_signals |= 1 << sig;
  signal (sig, sigterm_immediate_handler);
}

volatile int sighup_cnt = 0, sigusr1_cnt = 0;

static void sighup_handler (const int sig) {
  static const char message[] = "got SIGHUP.\n";
  kwrite (2, message, sizeof (message) - (size_t)1);
  sighup_cnt++;
  signal (SIGHUP, sighup_handler);
}

static void sigusr1_handler (const int sig) {
  static const char message[] = "got SIGUSR1.\n";
  kwrite (2, message, sizeof (message) - (size_t)1);
  sigusr1_cnt++;
  signal (SIGUSR1, sigusr1_handler);
}

#ifdef LOGGING_INTO_MEMORY_BUFFER
static void sigabrt_handler (const int sig) {
  fprintf (stderr, "got SIGABRT\n");
  logging_buffer_flush (2);
  flush_binlog_last();
  sync_binlog (2);
  exit (134);
}
#endif

void cron (void) {
  create_all_outbound_connections ();
  flush_binlog ();
  search_query_remove_expired ();
}

static void delete_many_rates_mask_init (void) {
  const char *s = allowed_deleted_by_rate;
  while (*s) {
    int i = get_sorting_mode (*s);
    if (i >= 0 && i < MAX_RATES) {
      delete_many_rates_mask |= 1 << i;
    }
    s++;
  }
}

void start_server (void) {
  int i;
  int prev_time;

  int old_sigusr1_cnt = 0, old_sighup_cnt = 0;

  init_epoll();
  init_netbuffers();
  if (udp_enabled) {
    init_msg_buffers (0);
  }

  prev_time = 0;

  if (daemonize) {
    setsid ();
    reopen_logs ();
  }

  if (!sfd) {
    sfd = server_socket (port, settings_addr, backlog, 0);
  }

  if (sfd < 0) {
    kprintf ("cannot open server socket at port %d: %m\n", port);
    exit (1);
  }

  if (change_user (username) < 0) {
    kprintf ("fatal: cannot change user to %s\n", username ? username : "(none)");
    exit (1);
  }

  if (binlogname && !binlog_disabled) {
    assert (append_to_binlog (Binlog) == log_readto_pos);
  }

  tl_parse_function = search_parse_function;
  tl_aio_timeout = 2.0;
  //init_listening_connection (sfd, &ct_search_engine_server, &memcache_methods);
  init_listening_connection (sfd, &ct_rpc_server, &rpc_methods);
  if (udp_enabled) {
    add_udp_socket (port, 0);
  }


  if (binlog_disabled && binlog_fd >= 0) {
    epoll_pre_event = read_new_events;
  }

  #ifdef LOGGING_INTO_MEMORY_BUFFER
  signal (SIGABRT, sigabrt_handler);
  #endif

  signal (SIGINT, sigint_handler);
  signal (SIGTERM, sigterm_handler);
  signal (SIGUSR1, sigusr1_handler);
  signal (SIGPIPE, SIG_IGN);
  signal (SIGIO, SIG_IGN);
  if (daemonize) {
    signal (SIGHUP, sighup_handler);
  }

  int quit_steps = 0;

  delete_many_rates_mask_init ();

  for (i = 0; !pending_signals ; i++) {
    if (verbosity > 1 && !(i & 255)) {
      kprintf ("epoll_work(): %d out of %d connections, network buffers: %d used, %d out of %d allocated\n",
	       active_connections, maxconn, NB_used, NB_alloc, NB_max);
    }
    epoll_work (57);

    tl_restart_all_ready ();
    if (old_sighup_cnt != sighup_cnt) {
      old_sighup_cnt = sighup_cnt;
      vkprintf (1, "start_server: sighup_cnt = %d.\n", old_sighup_cnt);
      sync_binlog (2);
    }

    if (old_sigusr1_cnt != sigusr1_cnt) {
      old_sigusr1_cnt = sigusr1_cnt;
      vkprintf (1, "start_server: sigusr1_cnt = %d.\n", old_sigusr1_cnt);
      sync_binlog (2);
      reopen_logs ();
    }

    if (now != prev_time) {
      prev_time = now;
      cron ();
    }

    if (epoll_pre_event) {
      epoll_pre_event ();
    }

    if (quit_steps && !--quit_steps) break;
  }

  epoll_close (sfd);
  close (sfd);

  flush_binlog_last ();
  sync_binlog (2);
  kprintf ("Terminated (pending_signals = 0x%llx).\n", pending_signals);
}

/*
 *
 *		MAIN
 *
 */

void usage (void) {
  printf ("usage: %s [-v] [-r] [-i] [-t] [-A] [-S] [-p<port>] [-u<username>] [-b<backlog>] [-c<max-conn>] [-a<binlog-name>] [-l<log-name>] <huge-index-file> [<metaindex-file>]\n"
	  "\t%s\n",
	  //      "\t-\n",
	  progname,
	  FullVersionStr);
  parse_usage ();
  exit(2);
}

long long parse_memory_limit (int option, const char *limit) {
  long long x;
  char c = 0;
  if (sscanf (optarg, "%lld%c", &x, &c) < 1) {
    kprintf ("Parsing limit for option '%c' fail: %s\n", option, limit);
    exit (1);
  }
  switch (c | 0x20) {
    case ' ': break;
    case 'k':  x <<= 10; break;
    case 'm':  x <<= 20; break;
    case 'g':  x <<= 30; break;
    case 't':  x <<= 40; break;
    default: kprintf ("Parsing limit fail. Unknown suffix '%c'.\n", c); exit (1);
  }
  return x;
}

int f_parse_option (int val) {
  int x;
  switch (val) {
  case 'A':
    universal = 1;
    break;
  case 'C':
    binlog_check_mode = 1;
    break;
  case 'D':
    creation_date = 0;
    break;
  case 'F':
    return_empty_record_on_error = 0;
    break;
  case 'I':
    import_only_mode = 1;
    break;
  case 'M':
    x = atoi (optarg);
    if (x >= 1 && x <= 5) {
      MAX_MISMATCHED_WORDS = x;
    }
    break;
  case 'O':
    tag_owner = 1;
    break;
  case 'R':
    allowed_deleted_by_rate = optarg;
    break;
  case 'S':
    use_stemmer = 1;
    break;
  case 'U':
    word_split_utf8 = 1;
    break;
  case 'W':
    wordfreqs_enabled = 1;
    break;
  case 't':
    hashtags_enabled = 1;
    break;
  default:
    return -1;
  }
  return 0;
}

char *aes_pwd_file;
int main (int argc, char *argv[]) {
  int i;

  daemonize = 1;
  set_debug_handlers ();

  progname = argv[0];


  parse_option ("universal-tag", no_argument, 0, 'A', "enables universal tag");
  parse_option ("binlog-check", no_argument, 0, 'C', "enables binlog check");
  parse_option ("mtime", no_argument, 0, 'D', "store item modification time (not creation time)");
  parse_option ("enable-errors", no_argument, 0, 'F', "return error strings on bad search queries in memcache mode");
  parse_option ("import-only", no_argument, 0, 'I', "import-only mode");
  parse_option ("mismatched-words", required_argument, 0, 'M', "max mismatched word (default %d)", MAX_MISMATCHED_WORDS);
  parse_option ("owner-tag", no_argument, 0, 'O', "owner tag");
  parse_option ("enable-delete-by-rate", no_argument, 0, 'R', "allow delete by rate queries");
  parse_option ("stemmer", no_argument, 0, 'S', "enable stemmer");
  parse_option ("utf8", no_argument, 0, 'U', "enable utf8 word split");
  parse_option ("wordfreq", no_argument, 0, 'W', "store word frequiences in item");
  parse_option ("hashtags", no_argument, 0, 't', "enable hashtags");

  
  parse_engine_options_long (argc, argv, f_parse_option);
  if (argc != optind + 1) {
    usage();
    return 2;
  }

  init_is_letter();
  if (hashtags_enabled) {
    enable_is_letter_sigils ();
  }
  if (use_stemmer) {
    stem_init ();
  }

  signal (SIGUSR1, sigusr1_handler);

  if (raise_file_rlimit (maxconn + 16) < 0) {
    kprintf ("fatal: cannot raise open file limit to %d\n", maxconn + 16);
    exit (1);
  }


  if (port < PRIVILEGED_TCP_PORTS) {
    sfd = server_socket (port, settings_addr, backlog, 0);
    if (sfd < 0) {
      kprintf ("cannot open server socket at port %d: %m\n", port);
      exit (1);
    }
  }

  aes_load_pwd_file (aes_pwd_file);

  if (change_user(username) < 0) {
    kprintf ("fatal: cannot change user to %s\n", username ? username : "(none)");
    exit (1);
  }

  init_dyn_data();
  if (udp_enabled) {
    init_server_PID (get_my_ipv4 (), port);
  }

  if (engine_preload_filelist ( argv[optind], binlogname) < 0) {
    kprintf ("cannot open binlog files for %s\n", binlogname ? binlogname : argv[optind]);
    exit (1);
  }

  vkprintf (3, "engine_preload_filelist done\n");

  //Snapshot reading
  Snapshot = open_recent_snapshot (engine_snapshot_replica);

  if (Snapshot) {
    engine_snapshot_name = strdup (Snapshot->info->filename);
    engine_snapshot_size = Snapshot->info->file_size;
    vkprintf (1, "load index file %s (size %lld)\n", engine_snapshot_name, engine_snapshot_size);

    index_load_time = -mytime();

    i = load_index (Snapshot);

    if (i < 0) {
      kprintf ("load_index returned fail code %d. Skipping index.\n", i);
      memset (&Header, 0, sizeof (Header));
    }

    index_load_time += mytime();
  } else {
    engine_snapshot_name = NULL;
    engine_snapshot_size = 0;
    index_load_time = 0;
  }


  //Binlog reading
  Binlog = open_binlog (engine_replica, jump_log_pos);
  if (!Binlog) {
    kprintf ("fatal: cannot find binlog for %s, log position %lld\n", engine_replica->replica_prefix, jump_log_pos);
    exit (1);
  }

  binlogname = Binlog->info->filename;

  vkprintf (1, "replaying binlog file %s (size %lld) from position %lld\n", binlogname, Binlog->info->file_size, jump_log_pos);

  binlog_load_time = -mytime();

  clear_log ();

  init_log_data (jump_log_pos, jump_log_ts, jump_log_crc32);

  vkprintf (1, "replay log events started\n");
  i = replay_log (0, 1);
  vkprintf (1, "replay log events finished\n");

  binlog_load_time += mytime();

  if (!binlog_disabled) {
    clear_read_log();
  }

  if (i == -2) {
    long long true_readto_pos = log_readto_pos - Binlog->info->log_pos + Binlog->offset;
    kprintf ("REPAIR: truncating %s at log position %lld (file position %lld)\n", Binlog->info->filename, log_readto_pos, true_readto_pos);
    if (truncate (Binlog->info->filename, true_readto_pos) < 0) {
      perror ("truncate()");
      exit (2);
    }
  } else if (i < 0) {
    kprintf ("fatal: error reading binlog\n");
    exit (1);
  }

  //if (verbosity) {
  //  fprintf (stderr, "replay binlog file: done, log_pos=%lld, alloc_mem=%ld, time %.06lfs\n",
  //           (long long) log_pos, (long) get_memory_used (), binlog_load_time);
  //}


  clear_write_log();
  start_time = time(0);

  start_server ();

  return 0;
}

