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
              2011-2013 Anton Maydell
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
#include "search-x-data.h"
#include "word-split.h"
#include "stemmer-new.h"
#include "net-connections.h"
#include "net-memcache-server.h"
#include "net-crypto-aes.h"
#include "utils.h"
#include "search-value-buffer.h"
#include "am-stats.h"

#define	VERSION_STR	"search-x-engine-0.05"
const char FullVersionStr[] = VERSION_STR " compiled at " __DATE__ " " __TIME__ " by gcc " __VERSION__ " "
#ifdef __LP64__
  "64-bit"
#else
  "32-bit"
#endif
;

#define TCP_PORT 11211

#define MAX_NET_RES	(1L << 16)
#define mytime() (get_utime(CLOCK_MONOTONIC))


/*
 *
 *		SEARCH ENGINE
 *
 */

int verbosity = 0, test_mode = 0, daemonize = 1, backlog = BACKLOG, port = TCP_PORT;
struct in_addr settings_addr;

// unsigned char is_letter[256];
char *progname = "search-engine", *username, *binlogname, *indexname, *logname;
char metaindex_fname_buff[256], binlog_fname_buff[256];

/* stats counters */
int start_time;
long long binlog_loaded_size;
long long search_queries, delete_queries, update_queries,
  minor_update_queries, increment_queries;
long long tot_response_words, tot_response_bytes;
double binlog_load_time, index_load_time;

/*
double max_perform_query_time = 0.0;
#define STATS_SEARCH_QUERY_BUFFER_SIZE 512
char worst_search_query[STATS_SEARCH_QUERY_BUFFER_SIZE];
int worst_search_query_res;
*/

#define STATS_BUFF_SIZE	(16 << 10)

static char stats_buff[STATS_BUFF_SIZE];

conn_type_t ct_search_engine_server = {
  .magic = CONN_FUNC_MAGIC,
  .title = "searchx_engine_server",
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

#define SEARCH_QUERY_HEAP_SIZE 255
struct search_query_heap_en {
  char *query;
  double cpu_time;
  int res;
};
int SQH_SIZE;
struct search_query_heap_en SQH[SEARCH_QUERY_HEAP_SIZE+1];
void search_query_heap_insert (struct search_query_heap_en *E) {
  if (SQH_SIZE == SEARCH_QUERY_HEAP_SIZE) {
    zzfree (SQH[1].query, strlen(SQH[1].query)+1);
    int i = 1;
    while (1) {
      int j = i << 1;
      if (j > SQH_SIZE) { break; }
      if (j < SQH_SIZE && SQH[j].cpu_time > SQH[j+1].cpu_time) { j++; }
      if (E->cpu_time <= SQH[j].cpu_time) { break; }
      memcpy (SQH + i, SQH + j, sizeof (*E));
      i = j;
    }
    memcpy (SQH + i, E, sizeof (*E));
  } else {
    int i = ++SQH_SIZE;
    while (i > 1) {
      int j = (i >> 1);
      if (SQH[j].cpu_time <= E->cpu_time) { break; }
      memcpy (SQH + i, SQH + j, sizeof (*E));
      i = j;
    }
    memcpy (SQH + i, E, sizeof (*E));
  }
}

int Q_raw;

int cmp_search_queries (const void *a, const void *b) {
  const struct search_query_heap_en *A = (const struct search_query_heap_en *) a;
  const struct search_query_heap_en *B = (const struct search_query_heap_en *) b;
  if (A->cpu_time < B->cpu_time) { return -1; }
  if (A->cpu_time > B->cpu_time) { return  1; }
  return 0;
}

int worst_search_queries (struct connection *c, const char *key, int len, int dog_len) {
  qsort (SQH + 1, SQH_SIZE, sizeof (SQH[0]), cmp_search_queries);
  int i = 0, j;
  for (j = SQH_SIZE; j >= 1; j--) {
    if (i + strlen(SQH[j].query) + 30 > STATS_BUFF_SIZE) { break; }
    i += sprintf (stats_buff + i, "%s\t%.9lf\t%d\n", SQH[j].query, SQH[j].cpu_time, SQH[j].res);
  }
  return return_one_key (c, key - dog_len, stats_buff, i);
}

int convert_rating (double x) {
  if (x < 0) { return 0; }
  if (x > 1) { return 2147483647; }
  return (int) (x * 2147483647.0 + 0.5);
}

int do_search_query (struct connection *c, const char *key, int len, int dog_len) {
  static char buff[2048];
  int i = -1;
  int res;

  if (strncmp (key, "search", 6)) {
    return 0;
  }

  if (verbosity > 0) {
    fprintf (stderr, "got: %s\n", key);
  }

  res = 0;
  int r;
  char *q_end = parse_query ((char*) key, &Q_raw, &r);

  if (r < 0) {
    vkprintf (1, "parse_query (%.*s) returns error code %d.\n", len, key, r);
    vkprintf (1, "ERROR near '%.256s'\n", q_end);
    return return_one_key (c, key - dog_len, buff, sprintf (buff, "ERROR near '%.256s'\n", q_end));
  }

  R_cnt = 0;
  search_queries++;

  if (len < LAST_SEARCH_QUERY_BUFF_SIZE) {
    memcpy (last_search_query, key, len);
    last_search_query[len] = 0;
  } else {
    strcpy (last_search_query, "TOO LONG QUERY");
  }

  double perform_query_time = -get_rusage_time ();
  res = perform_query();
  perform_query_time += get_rusage_time ();

  if (SQH_SIZE < SEARCH_QUERY_HEAP_SIZE || SQH[1].cpu_time < perform_query_time) {
    struct search_query_heap_en E;
    E.query = zzmalloc (len+1);
    strcpy (E.query, key);
    E.cpu_time = perform_query_time;
    E.res = res;
    search_query_heap_insert (&E);
  }
  /*
  if (perform_query_time > max_perform_query_time) {
    max_perform_query_time = perform_query_time;
    strncpy (worst_search_query, key, STATS_SEARCH_QUERY_BUFFER_SIZE - 1);
    worst_search_query[STATS_SEARCH_QUERY_BUFFER_SIZE - 1] = 0;
    worst_search_query_res = res;
  }
  */

  if (verbosity > 0) {
    fprintf (stderr, "result = %d\n", res);
  }

  struct value_buffer b;
  if (!value_buffer_init (&b, c, key - dog_len, len + dog_len, Q_raw, 64)) {
    return -1;
  }
  if (Q_hash_group_mode) {
    assert (Q_limit > 0);
    b.output_int (&b, R_tot);
    for (i = 0; i < R_cnt; i++) {
      if (!value_buffer_flush (&b) ) { return -1; }
      b.output_char (&b, ',');
      b.output_item_id (&b, R[i]->item_id);
      b.output_char (&b, ',');
      b.output_int (&b, convert_rating (RR[i]));
      b.output_char (&b, ',');
      b.output_hash (&b, get_hash_item (R[i]));
    }
    return value_buffer_return (&b);
  }

  b.output_int (&b, res);
  for (i = 0; i < R_cnt; i++) {
    if (!value_buffer_flush (&b) ) { return -1; }
    b.output_char (&b, ',');
    b.output_item_id (&b, R[i]->item_id);
    b.output_char (&b, ',');
    b.output_int (&b, convert_rating (RR[i]));
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

int searchx_prepare_stats (struct connection *c);

int memcache_get (struct connection *c, const char *key, int key_len) {
  int dog_len = get_at_prefix_length (key, key_len);
  key += dog_len;
  key_len -= dog_len;

  if (key_len >= 5 && !strncmp (key, "stats", 5)) {
    int len = searchx_prepare_stats (c);
    return return_one_key (c, key - dog_len, stats_buff, len);
  }

  if (key_len >= 8 && !strncmp (key, "search", 6)) {
    return do_search_query (c, key, key_len, dog_len);
  }

  if (key_len >= 4 && (!strncmp (key+1, "ate", 3))) {
    do_get_rate (c, key, key_len, dog_len);
    return 0;
  }

  if (key_len >= 4 && (!strncmp (key, "hash", 4))) {
    do_get_hash (c, key, key_len, dog_len);
    return 0;
  }

  if (key_len == 20 && !memcmp (key, "worst_search_queries", 20)) {
    return worst_search_queries (c, key, key_len, dog_len);
  }

  if (verbosity >= 1) {
    fprintf (stderr, "unknown query \"get %s\"\n", key);
  }
  return 0;
}

int memcache_delete (struct connection *c, const char *key, int key_len) {
  int owner_id, item_id;

  if (verbosity > 0) {
    fprintf (stderr, "delete \"%s\"\n", key);
  }

  int res = 0;

  switch (*key) {
  case 'i':
    if (sscanf (key, "item%d_%d ", &owner_id, &item_id) == 2 && owner_id && item_id > 0) {
      delete_queries++;
      if (verbosity >= 1) {
      	fprintf (stderr, "delete_item (%d,%d)\n", owner_id, item_id);
      }

      vkprintf (3, "delete: item_id=%lld\n", ((long long) item_id << 32) + (unsigned) owner_id);

      res = do_delete_item (((long long) item_id << 32) + (unsigned) owner_id);
    } else if (sscanf (key, "item%d ", &item_id) == 1 && item_id > 0) {
      delete_queries++;
      vkprintf (1, "delete_item (%d,%d)\n", 0, item_id);
      vkprintf (3, "delete: item_id=%lld\n", (long long)item_id);

      res = do_delete_item (item_id);
    }
    break;
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

char value_buff [65537];

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
  int rate, rate2, owner_id, item_id;
  long long ritem_id = 0;
  int cur_op;
  int ok = 0;
  int no_reply = 0;

  if (verbosity > 0) {
    fprintf (stderr, "mc_store: op=%d, key=\"%s\", flags=%d, expire=%d, bytes=%d, noreply=%d\n", op, key, flags, delay, size, no_reply);
  }

  owner_id = 0;
  if (size >= 65536) {
    return -2;
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

  int res = 0;
  if (ok) {
    res = do_store (c, ritem_id, rate, rate2, cur_op, size);
  }

  if (res > 0) {
    return 1;
  } else {
    return 0;
  }
}

int memcache_stats (struct connection *c) {
  int len = searchx_prepare_stats (c);
  write_out (&c->Out, stats_buff, len);
  write_out (&c->Out, "END\r\n", 5);
  return 0;
}

int searchx_prepare_stats (struct connection *c) {
  int uptime = now - start_time;
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

  sb_printf (&sb,
      "queries_search\t%lld\n"
      "qps_search\t%.3f\n"
      "queries_delete\t%lld\n"
      "qps_delete\t%.3f\n"
      "queries_update\t%lld\n"
      "qps_update\t%.3f\n"
      "queries_minor_update\t%lld\n"
      "qps_minor_update\t%.3f\n"
      "queries_increment\t%lld\n"
      "qps_increment\t%.3f\n"
      "items\t%d\n"
      "items_marked_as_deleted\t%d\n"
      "items_freed\t%d\n"
      "index_items_deleted\t%d\n"
      "tree_nodes_allocated\t%d\n"
      "tree_nodes_unused\t%d\n"
      "rebuild_hashmap_calls\t%lld\n"
      "last_search_query\t%s\n"
      "use_stemmer\t%d\n"
      "hashtags_enabled\t%d\n"
      "version\t%s\n",
    search_queries,
    safe_div(search_queries, uptime),
    delete_queries,
    safe_div(delete_queries, uptime),
    update_queries,
    safe_div(update_queries, uptime),
    minor_update_queries,
    safe_div(minor_update_queries, uptime),
    increment_queries,
    safe_div(increment_queries, uptime),
    tot_items,
    del_items,
    tot_freed_deleted_items,
    mod_items,
    alloc_tree_nodes,
    free_tree_nodes,
    rebuild_hashmap_calls,
    last_search_query,
    use_stemmer,
    hashtags_enabled,
    FullVersionStr);

  return sb.pos;
}

int sfd;
void reopen_logs (void) {
  int fd;
  fflush(stdout);
  fflush(stderr);
  if ((fd = open ("/dev/null", O_RDWR, 0)) != -1) {
    dup2(fd, 0);
    dup2(fd, 1);
    dup2(fd, 2);
    if (fd > 2) {
      close(fd);
    }
  }
  if (logname && (fd = open (logname, O_WRONLY|O_APPEND|O_CREAT, 0640)) != -1) {
    dup2(fd, 1);
    dup2(fd, 2);
    if (fd > 2) {
      close(fd);
    }
  }
  if (verbosity > 0) {
    fprintf (stderr, "logs reopened.\n");
  }
}

static void sigint_immediate_handler (const int sig) {
  exit (1);
}

static void sigint_handler (const int sig) {
  pending_signals |= (1LL << sig);
  signal (sig, sigint_immediate_handler);
}

static void sighup_handler (const int sig) {
  fprintf (stderr, "got SIGHUP.\n");
  sync_binlog (2);
  signal (SIGHUP, sighup_handler);
}

static void sigusr1_handler (const int sig) {
  fprintf (stderr, "got SIGUSR1, rotate logs.\n");
  sync_binlog (2);
  reopen_logs ();
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
  dyn_garbage_collector ();
}

void start_server (void) {
  int i;
  int prev_time;

  init_epoll();
  init_netbuffers();

  prev_time = 0;

  if (daemonize) {
    setsid();
  }

  if (!sfd) {
    sfd = server_socket (port, settings_addr, backlog, 0);
  }

  if (sfd < 0) {
    fprintf(stderr, "cannot open server socket at port %d: %m\n", port);
    exit (1);
  }

  if (change_user (username) < 0) {
    fprintf (stderr, "fatal: cannot change user to %s\n", username ? username : "(none)");
    exit (1);
  }

  if (binlogname && !binlog_disabled) {
    assert (append_to_binlog (Binlog) == log_readto_pos);
  }

  init_listening_connection (sfd, &ct_search_engine_server, &memcache_methods);

  if (binlog_disabled && binlog_fd >= 0) {
    epoll_pre_event = read_new_events;
  }

  #ifdef LOGGING_INTO_MEMORY_BUFFER
  signal (SIGABRT, sigabrt_handler);
  #endif

  signal (SIGINT, sigint_handler);
  signal (SIGTERM, sigint_handler);
  signal (SIGUSR1, sigusr1_handler);
  signal (SIGPIPE, SIG_IGN);
  signal (SIGIO, SIG_IGN);
  if (daemonize) {
    signal (SIGHUP, sighup_handler);
  }

  int quit_steps = 0;

  for (i = 0; !pending_signals ; i++) {
    if (verbosity > 0 && !(i & 255)) {
      fprintf (stderr, "epoll_work(): %d out of %d connections, network buffers: %d used, %d out of %d allocated\n",
	       active_connections, maxconn, NB_used, NB_alloc, NB_max);
    }
    epoll_work (57);

    if (now != prev_time) {
      prev_time = now;
      cron();
    }

    if (epoll_pre_event) {
      epoll_pre_event ();
    }

    if (quit_steps && !--quit_steps) break;
  }
  kprintf ("Terminated (pending_signals = 0x%llx).\n", pending_signals);

  epoll_close (sfd);
  close (sfd);

  flush_binlog_last ();
  sync_binlog (2);
}

/*
 *
 *		MAIN
 *
 */

void usage (void) {
  printf ("usage: %s [-v] [-r] [-i] [-t] [-A] [-S] [-p<port>] [-u<username>] [-b<backlog>] [-c<max-conn>] [-a<binlog-name>] [-l<log-name>] <huge-index-file> [<metaindex-file>]\n"
      "\t%s\n"
      "\tPerforms search queries using given indexes\n"
      "\tIf <metaindex-file> is not specified, <huge-index-file>.idx is used\n"
      "\t-v\toutput statistical and debug information into stderr\n"
      "\t-r\tread-only binlog (don't log new events)\n"
      "\t-t\tenable tags (*word @word #word are considered words)\n"
      "\t-A\tenable universal tag\n"
      "\t-S\tuse stemmer\n"
      "\t-U\tenable UTF-8\n"
      "\t-T\ttest mode (don't increase rlimits, don't daemonize)\n",
      progname,
      FullVersionStr
      );
  exit(2);
}

int main (int argc, char *argv[]) {
  int i;

  set_debug_handlers();

  progname = argv[0];
  while ((i = getopt (argc, argv, "a:b:c:l:p:rdChtu:vASTU")) != -1) {
    switch (i) {
    case 'v':
      verbosity++;
      break;
    case 'r':
      binlog_disabled = 1;  /* DEBUG only */
      break;
    case 'h':
      usage();
      return 2;
    case 'C':
      binlog_check_mode = 1;
      break;
    case 'b':
      backlog = atoi(optarg);
      if (backlog <= 0) backlog = BACKLOG;
      break;
    case 'c':
      maxconn = atoi(optarg);
      if (maxconn <= 0 || maxconn > MAX_CONNECTIONS) {
      	maxconn = MAX_CONNECTIONS;
      }
      break;
    case 'p':
      port = atoi(optarg);
      break;
    case 'u':
      username = optarg;
      break;
    case 'a':
      binlogname = optarg;
      break;
    case 'l':
      logname = optarg;
      break;
    case 'A':
      universal = 1;
      break;
    case 'S':
      use_stemmer = 1;
      break;
    case 'U':
      word_split_utf8 = 1;
      break;
    case 'T':
      test_mode = 1;
      break;
    case 't':
      hashtags_enabled = 1;
      break;
    case 'd':
      daemonize ^= 1;
      break;
    }
  }
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


  if (raise_file_rlimit(maxconn + 16) < 0) {
    fprintf (stderr, "fatal: cannot raise open file limit to %d\n", maxconn + 16);
    exit (1);
  }


  if (port < PRIVILEGED_TCP_PORTS) {
    sfd = server_socket (port, settings_addr, backlog, 0);
    if (sfd < 0) {
      fprintf (stderr, "cannot open server socket at port %d: %m\n", port);
      exit (1);
    }
  }

  aes_load_pwd_file (0);

  if (change_user(username) < 0) {
    fprintf (stderr, "fatal: cannot change user to %s\n", username ? username : "(none)");
    exit(1);
  }

  init_dyn_data();

  if (engine_preload_filelist ( argv[optind], binlogname) < 0) {
    fprintf (stderr, "cannot open binlog files for %s\n", binlogname ? binlogname : argv[optind]);
    exit (1);
  }

  vkprintf (3, "engine_preload_filelist done\n");

  //Snapshot reading
  Snapshot = open_recent_snapshot (engine_snapshot_replica);

  if (Snapshot) {
    engine_snapshot_name = strdup (Snapshot->info->filename);
    engine_snapshot_size = Snapshot->info->file_size;

    if (verbosity) {
      fprintf (stderr, "load index file %s (size %lld)\n", engine_snapshot_name, engine_snapshot_size);
    }
    index_load_time = -mytime();

    i = load_index (Snapshot);

    if (i < 0) {
      fprintf (stderr, "load_index returned fail code %d. Skipping index.\n", i);
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
    fprintf (stderr, "fatal: cannot find binlog for %s, log position %lld\n", engine_replica->replica_prefix, jump_log_pos);
    exit (1);
  }

  binlogname = Binlog->info->filename;

  if (verbosity) {
    fprintf (stderr, "replaying binlog file %s (size %lld) from position %lld\n", binlogname, Binlog->info->file_size, jump_log_pos);
  }
  binlog_load_time = -mytime();

  clear_log();

  init_log_data (jump_log_pos, jump_log_ts, jump_log_crc32);

  if (verbosity) { fprintf (stderr, "replay log events started\n");}
  i = replay_log (0, 1);
  if (verbosity) { fprintf (stderr, "replay log events finished\n");}

  binlog_load_time += mytime();
  binlog_loaded_size = log_readto_pos - jump_log_pos;

  if (!binlog_disabled) {
    clear_read_log();
  }

  if (i == -2) {
    long long true_readto_pos = log_readto_pos - Binlog->info->log_pos + Binlog->offset;
    fprintf (stderr, "REPAIR: truncating %s at log position %lld (file position %lld)\n", Binlog->info->filename, log_readto_pos, true_readto_pos);
    if (truncate (Binlog->info->filename, true_readto_pos) < 0) {
      perror ("truncate()");
      exit (2);
    }
  } else if (i < 0) {
    fprintf (stderr, "fatal: error reading binlog\n");
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

