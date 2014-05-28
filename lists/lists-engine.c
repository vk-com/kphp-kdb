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

    Copyright 2010-2013 Vkontakte Ltd
              2010-2013 Nikolai Durov
              2010-2013 Andrei Lopatin
              2011-2013 Vitaliy Valtman
*/

#define  _FILE_OFFSET_BITS  64

#include <assert.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/types.h>
#include <ctype.h>
#include <sys/wait.h>
#include <aio.h>

#include "md5.h"
#include "crc32.h"
#include "net-events.h"
#include "net-buffers.h"
#include "server-functions.h"
#include "common-data.h"
#include "kdb-lists-binlog.h"
#include "lists-data.h"
#include "net-connections.h"
#include "net-memcache-server.h"
#include "net-crypto-aes.h"
#include "net-aio.h"
#include "net-events.h"
#include "am-stats.h"

#include "vv-tl-parse.h"
#include "vv-tl-aio.h"
#include "rpc-const.h"
#include "lists-tl.h"

#include "lists-interface-structures.h"

#ifdef LISTS_Z
# define VERSION_STR  "lists-x-engine-0.3"
#else
# define VERSION_STR  "lists-engine-0.3"
#endif

#define TCP_PORT 11211

#define MAX_NET_RES  (1L << 16)

#ifdef LISTS_Z
typedef int input_list_id_t[MAX_LIST_ID_INTS];
typedef int input_object_id_t[MAX_OBJECT_ID_INTS];
#else
# define input_list_id_t list_id_t
# define input_object_id_t object_id_t
#endif

#ifdef LISTS64
# define idout  "%lld"
# define scanf_dptr(x) ((int *)(x)), ( ( (int *)(x) ) + 1)
# define scanf_dstr "%d:%d"
# define SCANF_DINTS  2
# define SCANF_DINTS_AM  2
# define MAX_OBJECT_RES (MAX_RES / 2)
# define out_list_id(x)  (x)
# define out_object_id(x)  (x)
#elif (defined (LISTS_Z))
# define idout "%s"
# define scanf_dstr  "%n%*[0-9:-]%n"
# define scanf_dptr(x) (&((*(x))[0])), (&((*(x))[1]))
# define SCANF_DINTS  0
# define SCANF_DINTS_AM  -1
# define MAX_OBJECT_RES (MAX_RES / (object_id_ints))
extern char *out_list_id (list_id_t list_id);
extern char *out_object_id (object_id_t object_id);
#else
# define idout  "%d"
# define scanf_dstr "%d"
# define scanf_dptr(x) (x)
# define SCANF_DINTS  1
# define SCANF_DINTS_AM  1
# define MAX_OBJECT_RES MAX_RES
# define out_list_id(x)  (x)
# define out_object_id(x)  (x)
#endif

#ifdef VALUES64
# define valin "%lld"
#else
# define valin "%d"
#endif

#define DEFAULT_MEMORY_FOR_METAFILES 1000000000
long long memory_for_metafiles = DEFAULT_MEMORY_FOR_METAFILES;
/*
 *
 *    LISTS ENGINE
 *
 */

long long idx_min_free_heap = IDX_MIN_FREE_HEAP;
int verbosity = 0;
extern int ignored_list2;
int metafile_mode;
char metafiles_order = 0;
int disable_revlist;
int return_false_if_not_found;
int ignore_mode;
int udp_enabled;

char *aes_pwd_file;
// unsigned char is_letter[256];
char *progname = "lists-engine", *username, *binlogname, *logname;
char *newidxname;
char metaindex_fname_buff[256], binlog_fname_buff[256];

/* stats counters */
int cache_miss;
int start_time;
long long binlog_loaded_size;
long long cache_misses, cache_hits, netw_queries;
long long delete_queries, delete_object_queries, increment_queries;
long long tot_response_words, tot_response_bytes;
double cache_missed_qt, cache_hit_qt, binlog_load_time, index_load_time;

int w_split_rem, w_split_mod;
int dump_sums, child_pid;
int force_write_index;

int export_flags, do_export;

#define STATS_BUFF_SIZE  (1 << 20)
int stats_buff_len;
char stats_buff[STATS_BUFF_SIZE];

int max_text_len = 255;

int backlog = BACKLOG, port = TCP_PORT, maxconn = MAX_CONNECTIONS, daemonize = 1, is_server = 0, test_mode = 0;
struct in_addr settings_addr;
int active_connections;

int list_engine_wakeup (struct connection *c);
int list_engine_alarm (struct connection *c);

char *ignore_string;
int ignore_timestamp;

conn_type_t ct_lists_engine_server = {
  .magic = CONN_FUNC_MAGIC,
  .title = "lists_engine_server",
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
  .wakeup = list_engine_wakeup,
  .alarm = list_engine_alarm,
#ifndef NOAES
  .crypto_init = aes_crypto_init,
  .crypto_free = aes_crypto_free,
  .crypto_encrypt_output = aes_crypto_encrypt_output,
  .crypto_decrypt_input = aes_crypto_decrypt_input,
  .crypto_needed_output_bytes = aes_crypto_needed_output_bytes,
#endif
};

int memcache_store (struct connection *c, int op, const char *key, int key_len, int flags, int delay, int size);
int memcache_get_start (struct connection *c);
int memcache_get (struct connection *c, const char *key, int key_len);
int memcache_get_end (struct connection *c, int key_count);
int memcache_incr (struct connection *c, int op, const char *key, int key_len, long long arg);
int memcache_delete (struct connection *c, const char *key, int key_len);
int memcache_stats (struct connection *c);


struct memcache_server_functions memcache_methods = {
  .execute = mcs_execute,
  .mc_store = memcache_store,
  .mc_get_start = memcache_get_start,
  .mc_get = memcache_get,
  .mc_get_end = memcache_get_end,
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
  .memcache_fallback_type = &ct_lists_engine_server,
  .memcache_fallback_extra = &memcache_methods
};



int list_engine_wakeup (struct connection *c) {
  if (verbosity > 1) {
    fprintf (stderr, "list_engine_wakeup (%p)\n", c);
  }

  c->generation = ++conn_generation;
  c->pending_queries = 0;
  c->status = conn_reading_query;

  clear_connection_timeout (c);

  if (c->Out.total_bytes > 0) {
    c->flags |= C_WANTWR;
  }

  return 0;
}


int list_engine_alarm (struct connection *c) {
  if (verbosity > 1) {
    fprintf (stderr, "list_engine connection %d timeout alarm, %d queries pending, status=%d\n", c->fd, c->pending_queries, c->status);
  }
  assert (c->status == conn_wait_aio);
  c->flags |= C_INTIMEOUT;
  return list_engine_wakeup (c);
}


//extern struct aio_connection *WaitAio;
extern conn_query_type_t aio_metafile_query_type;
double aio_t = 4;
int memcache_wait (struct connection *c) {
  if (!WaitAioArrPos) {
    return 0;
  }
  if (verbosity > 2) {
    fprintf (stderr, "wait for aio..\n");
  }
  if (c->flags & C_INTIMEOUT) {
    if (verbosity > 1) {
      fprintf (stderr, "memcache: IN TIMEOUT (%p)\n", c);
    }
    WaitAioArrClear ();
    return 0;
  }
  if (c->Out.total_bytes > 8192) {
    c->flags |= C_WANTWR;
    c->type->writer (c);
  }
  int i;
  for (i = 0; i < WaitAioArrPos; i++) {
    conn_schedule_aio (WaitAioArr[i], c, 1.1 * aio_t, &aio_metafile_query_type);
  }
  set_connection_timeout (c, aio_t);
  WaitAioArrClear ();
  //WaitAio = 0;
  return 1;
}




int quit_steps;

extern long long tot_metafiles_memory;
extern long long tot_metafiles_marked_memory;
extern long long metafiles_load_success;
extern int metafiles_loaded;
extern long long metafiles_load_errors;
extern int metafiles_marked;
extern long long tot_aio_loaded_bytes;
extern int postponed_operations_performed;
extern int postponed_operations_total;
extern long long postponed_operations_size;
extern long long revlist_preloaded_bytes;
extern long long revlist_index_preloaded_bytes;
extern int data_metafiles_loaded;
extern int revlist_metafiles_loaded;
extern long long tot_lost_aio_bytes;
extern long long malloc_memory;

int lists_prepare_stats (struct connection *c) {
  int uptime = now - start_time;
  stats_buffer_t sb;
  sb_prepare (&sb, c, stats_buff, STATS_BUFF_SIZE);
  sb_memory (&sb, AM_GET_MEMORY_USAGE_SELF);
  sb_printf (&sb, "malloc_memory\t%lld\n", malloc_memory);
  SB_BINLOG;
  SB_INDEX;
  sb_printf (&sb,
      "index_lists\t%d\n"
      "index_data_bytes\t%lld\n"
      "index_loaded_bytes\t%lld\n"
      "index_last_global_id\t%d\n"
      "revlist_preloaded_bytes\t%lld\n"
      "revlist_index_preloaded_bytes\t%lld\n"
      "list_id_ints\t%d\n"
      "object_id_ints\t%d\n"
      "value_ints\t%d\n"
      "tree_nodes\t%d %d %d %d\n"
      "queries_delete\t%lld\n"
      "qps_delete\t%.3f\n"
      "queries_delete_object\t%lld\n"
      "qps_delete_object\t%.3f\n"
      "increment_queries\t%lld\n"
      "qps_increment\t%.3f\n"
      "max_lists\t%d\n"
      "lists\t%d\n"
      "list_entries\t%ld\n"
      "last_global_id\t%d\n"
      "last_query_time\t%.6f\n"
      "total_metafiles_bytes\t%lld\n"
      "total_marked_metafiles_bytes\t%lld\n"
      "metafiles_loaded\t%d\n"
      "metafiles_marked\t%d\n"
      "metafiles_load_success\t%lld\n"
      "metafiles_load_fail\t%lld\n"
      "active_aio_queries\t%lld\n"
      "avg_aio_query_time\t%lf\n"
      "data_metafiles_loaded\t%d\n"
      "revlist_metafiles_loaded\t%d\n"
      "postponed_operations_made\t%d\n"
      "postponed_operations_total\t%d\n"
      "postponed_operations_memory\t%lld\n"
      "total_aio_loaded_bytes\t%lld\n"
      "total_aio_wasted_bytes\t%lld\n"
      "version\t" VERSION_STR " compiled at " __DATE__ " " __TIME__ " by gcc " __VERSION__ " "
#ifdef __LP64__
      "64-bit"
#else
      "32-bit"
#endif
      " after commit " COMMIT "\n",
      idx_lists, idx_bytes, idx_loaded_bytes,
      idx_last_global_id,
       revlist_preloaded_bytes, revlist_index_preloaded_bytes,
      list_id_ints, object_id_ints, (int) VALUE_INTS,
      alloc_small_nodes, alloc_global_nodes, alloc_large_nodes, alloc_ltree_nodes,
      delete_queries,
      safe_div (delete_queries, uptime),
      delete_object_queries,
      safe_div(delete_object_queries, uptime),
      increment_queries,
      safe_div(increment_queries, uptime),
      max_lists,
      tot_lists,
      tot_list_entries,
      last_global_id,
      c ? c->last_query_time : 0,
      tot_metafiles_memory + tot_metafiles_marked_memory,
      tot_metafiles_marked_memory,
      metafiles_loaded,
      metafiles_marked,
      metafiles_load_success,
      metafiles_load_errors,
      active_aio_queries,
      tot_aio_queries > 0 ? total_aio_time / tot_aio_queries : 0,
      data_metafiles_loaded, revlist_metafiles_loaded,
      postponed_operations_performed,
      postponed_operations_total,
      postponed_operations_size,
      tot_aio_loaded_bytes,
      tot_lost_aio_bytes
      );
  return sb.pos;
}

char *NOT_FOUND_STRING = "";
int NOT_FOUND_STRING_LEN = 0;
int NOT_FOUND_STRING_FLAGS = 0;

int return_one_key_flags (struct connection *c, const char *key, char *val, int vlen, int flags) {
  static char buff[65536];
  int l = sprintf (buff, "VALUE %s %d %d\r\n", key, flags, vlen);
  assert (l <= 65536);
  write_out (&c->Out, buff, l);
  write_out (&c->Out, val, vlen);
  write_out (&c->Out, "\r\n", 2);
  return 0;
}

int return_key_list_value (struct connection *c, const char *key, int key_len, int res, int mode, const int *R, int R_cnt, int rec_size, int text_offset, int long_offset) {
  int w, i, j = 0;

  if (verbosity > 0) {
    fprintf (stderr, "result = %d\n", res);
  }

  if (!R_cnt) {
    if (mode < 0) {
      w = 4;
      *((int *) stats_buff) = res;
    } else {
      w = sprintf (stats_buff, "%d", res);
    }
    return return_one_key (c, key, stats_buff, w);
  }

  write_out (&c->Out, "VALUE ", 6);
  write_out (&c->Out, key, key_len);

  char *ptr = get_write_ptr (&c->Out, 1024);
  if (!ptr) return -1;
  char *s = ptr + 512;

  memcpy (ptr, " 0 .........\r\n", 14);
  char *size_ptr = ptr + 3;

  ptr += 14;
  if (mode >= 0) {
    w = sprintf (ptr, "%d", res);
  } else {
    w = 4;
    *((int *) ptr) = res;
  }
  ptr += w;
  for (i = 0; i < R_cnt; i++) {
    int t;
    if (ptr >= s) {
      advance_write_ptr (&c->Out, ptr - (s - 512));
      ptr = get_write_ptr (&c->Out, 1024);
      if (!ptr) return -1;
      s = ptr + 512;
    }
    if (mode >= 0) {
      *ptr++ = (j > 0 && j < object_id_ints ? ':' : ',');
      w++;
      if (j == text_offset) {
        char *p = *((char **) (R + i));
        t = R[i + PTR_INTS];
        int k;
        for (k = 0; k < t; k++) {
          if (p[k] == ',') { ptr[k] = 12; }
          else if (p[k] == 12) { ptr[k] = ' '; }
          else { ptr[k] = p[k]; }
        }
        w += t;
        j += PTR_INTS;
        i += PTR_INTS;
      } else if (j == long_offset) {
        w += t = sprintf (ptr, "%lld", *(long long *) (R + i));
        i++;
        j++;
      } else {
        w += t = sprintf (ptr, "%d", R[i]);
      }
    } else {
      w += t = 4;
      *((int *) ptr) = R[i];
    }
    if (++j == rec_size) {
      j = 0;
    }
    ptr += t;
  }
  size_ptr[sprintf (size_ptr, "% 9d", w)] = '\r';
  memcpy (ptr, "\r\n", 2);
  ptr += 2;
  advance_write_ptr (&c->Out, ptr - (s - 512));

  return 0;
}

//note whether need to change int order or no
static int parse_list (int *Res, int max_size, netbuffer_t *In, int bytes, int have_weights, int *id_ints) {
  char *ptr = 0, *ptr_e = 0;
  #define MAX_INT 0x7fffffff
  int j = MAX_INT, r = 0, s = 0;
  int found = 0;
  *id_ints = -1;
  unsigned sgn;
  long long x;
  if (!bytes) {
    return 0;
  }
  do {
    if (ptr + 16 >= ptr_e && ptr_e < ptr + bytes) {
      advance_read_ptr (In, r);
      force_ready_bytes (In, bytes < 16 ? bytes : 16);
      ptr = get_read_ptr (In);
      r = get_ready_bytes (In);
      if (r > bytes) {
        r = bytes;
      }
      ptr_e = ptr + r;
      r = 0;
    }
    assert (ptr < ptr_e);
    x = 0;
    sgn = 0x7fffffff;
    if (*ptr == '-' && ptr + 1 < ptr_e) {
      ptr++;
      sgn++;
      r++;
      bytes--;
    }
    if (*ptr < '0' || *ptr > '9') {
      advance_skip_read_ptr (In, r + bytes);
      return -1;
    }
    while (ptr < ptr_e && *ptr >= '0' && *ptr <= '9') {
      x = x*10 + (*ptr++ - '0');
      if (x > sgn) {
        advance_skip_read_ptr (In, r + bytes);
        return -1;
      }
      r++;
      bytes--;
    }
    if (s >= max_size || (bytes > 0 && (ptr == ptr_e))) {
      advance_skip_read_ptr (In, r + bytes);
      return -1;
    }
    if (bytes > 0) {
      if (found) {
        if (*ptr != ((j == 1) ? ',' : (j == have_weights + 1) ? '#' : ':')) {
          advance_skip_read_ptr (In, r + bytes);
          return -1;
        }
      } else {
        if (*ptr == (have_weights ? '#' : ',')) {
          found = 1;
          *id_ints = MAX_INT - j + 1;
          j = have_weights + 1;
        } else if (*ptr != ':') {
          advance_skip_read_ptr (In, r + bytes);
          return -1;
        }
      }
    } else {
      if (!found && !have_weights) {
        found = 1;
        *id_ints = MAX_INT - j + 1;
        j = have_weights + 1;
      }
    }
    Res[s++] = (sgn & 1 ? x : -x);
    if (!bytes) {
      advance_read_ptr (In, r);
      return j == 1 ? s : -1;
    }
    assert (*ptr == (j == 1 ? ',' : (j == have_weights + 1 ? '#' : ':')));
    ptr++;
    r++;
    if (!--j) {
      j = *id_ints + have_weights;
    }
  } while (--bytes > 0);
  assert (!bytes);
  advance_read_ptr (In, r);
  return -1;
}

static inline void clear_input_list_id (input_list_id_t *p) {
#ifdef LISTS_Z
  (*p)[0] = -1;
  (*p)[1] = -1;
#else
  *p = 0;
#endif
}

static inline void clear_input_object_id (input_object_id_t *p) {
#ifdef LISTS_Z
  (*p)[0] = -1;
  (*p)[1] = -1;
#else
  *p = 0;
#endif
}


#ifdef LISTS_Z
static int convert_ints_array (const char *s, input_list_id_t list_id, int num) {
  int i;
  char *cur, *tmp, *end;
  assert (list_id[0] >= -1);
  if (list_id[0] < 0 || list_id[1] <= list_id[0]) {
    return 0;
  }
  if (verbosity > 1) {
    fprintf (stderr, "convert_ints_array[%d..%d = '%.*s'],%d\n", list_id[0], list_id[1], list_id[1] - list_id[0], s + list_id[0], num);
  }
  cur = (char *) s + list_id[0];
  end = (char *) s + list_id[1];
  for (i = 0; i < num; i++) {
    list_id[i] = strtol(cur, &tmp, 10);
    if (tmp == cur || tmp > end) {
      return 0;
    }
    cur = tmp;
    if (i < num - 1) {
      if (*cur++ != ':') {
        return 0;
      }
    }
  }
  return cur == end;
}
#endif


static inline int convert_input_list_id (const char *s, input_list_id_t list_id) {
#ifdef LISTS_Z
  return convert_ints_array (s, list_id, list_id_ints);
#else
  return 1;
#endif
}

static inline int convert_input_object_id (const char *s, input_object_id_t object_id) {
#ifdef LISTS_Z
  return convert_ints_array (s, object_id, object_id_ints);
#else
  return 1;
#endif
}

static inline void eat_at (const char *key, int key_len, char **new_key, int *new_len) {
  *new_key = (char *)key;
  *new_len = key_len;

  if ((*key >= '0' && *key <= '9') || (*key == '-' && key[1] >= '0' && key[1] <= '9')) {
    key++;
    while (*key >= '0' && *key <= '9') {
      key++;
    }
    if (*key++ == '@') {
      *new_len = key + key_len - *new_key;
      *new_key = (char *)key;
    }
  }
}


int memcache_store (struct connection *c, int op, const char *key, int key_len, int flags, int delay, int size) {
  c->flags &= ~C_INTIMEOUT;
  static input_list_id_t list_id;
  static input_object_id_t object_id;

  clear_input_list_id (&list_id);
  clear_input_object_id (&object_id);

  int xor_mask = 0, and_mask = 7, tag;
  if (verbosity > 1) {
    fprintf (stderr, "memcache_store: key='%s', key_len=%d, value_len=%d\n", key, key_len, size);
  }

  free_tmp_buffers (c);

  if (size >= 131072) {
    return -2;
  }

  char *new_key;
  int new_len;

  eat_at (key, key_len, &new_key, &new_len);

  int have_weights = (new_key[0] == 'x');

  if (sscanf (new_key, have_weights ? "xtemp%d" : "temp%d", &tag) == 1 && tag && op == mct_set) {
    int id_ints = -1;
    int s = parse_list (R, MAX_RES, &c->In, size, have_weights, &id_ints);
    int res = 0;
    if (s >= 0 && id_ints <= object_id_ints && id_ints >= 0) {
      assert (id_ints >= 0);
      if (!c->Tmp) {
        c->Tmp = alloc_head_buffer();
        assert (c->Tmp);
      }
      assert (!(s % (id_ints + have_weights)));
      write_out (c->Tmp, &have_weights, 4);
      write_out (c->Tmp, &tag, 4);
      write_out (c->Tmp, &id_ints, 4);
      write_out (c->Tmp, R, s*4);
      res = 1;
    }
    if (verbosity > 0) {
      fprintf (stderr, "stored large temp list %d: size = %d, first entry = %d, have_weights = %d, res = %d, id_ints = %d\n", tag, s, R[0], have_weights, res, id_ints);
    }
    return res;
  }

  if (size > 1024  + max_text_len) {
    return -2;
  }

  nb_iterator_t R;
  nbit_set (&R, &c->In);
  assert (nbit_read_in (&R, stats_buff, size) == size);
  stats_buff[size] = 0;

  int res = 0;

  switch (new_key[0]) {
  case 'e':
    if (sscanf (new_key, "entry" scanf_dstr "_" scanf_dstr, scanf_dptr (&list_id), scanf_dptr (&object_id)) >= 2 * SCANF_DINTS /* && list_id && object_id */) {
      int flags, extra[4];
      value_t value = 0;
      if (convert_input_list_id (new_key, list_id) && convert_input_object_id (new_key, object_id)) {
        memset (extra, 0, 16);
        if (sscanf (stats_buff, "%d," valin ",%d,%d,%d,%d", &flags, &value, extra, extra+1, extra+2, extra+3) >= 1) {
          res =  do_add_list_entry (list_id, object_id, op - mct_set, flags, value, extra);
        }
      }
    }
    break;

  case 'f':
    if (op != mct_add && sscanf (new_key, "flags" scanf_dstr "_" scanf_dstr, scanf_dptr (&list_id), scanf_dptr (&object_id)) >= 2 * SCANF_DINTS /* && list_id && object_id */) {
      int nand_mask = -1, or_mask;
      if (convert_input_list_id (new_key, list_id) && convert_input_object_id (new_key, object_id)) {
        if (sscanf (stats_buff, "%d,%d", &or_mask, &nand_mask) >= 1) {
          res = do_change_entry_flags (list_id, object_id, or_mask, nand_mask);
        }
      }
    }
    break;

  case 'v':
    if (op != mct_add && sscanf (new_key, "value" scanf_dstr "_" scanf_dstr, scanf_dptr (&list_id), scanf_dptr (&object_id)) >= 2 * SCANF_DINTS) {
      if (convert_input_list_id (new_key, list_id) && convert_input_object_id (new_key, object_id)) {
        res = do_change_entry_value (list_id, object_id, atoll (stats_buff), 0) != (-1LL << 63);
        if (!res) {
          res = -2;
        }
      }
    }
    break;

  case 'l':
    if (op != mct_add && sscanf (new_key, "listflags" scanf_dstr ",%d,%d", scanf_dptr (&list_id), &xor_mask, &and_mask) >= SCANF_DINTS + 1) {
      int nand_mask = -1, or_mask;
      if (convert_input_list_id (new_key, list_id)) {
        if (sscanf (stats_buff, "%d,%d", &or_mask, &nand_mask) >= 1) {
          res = do_change_sublist_flags (list_id, xor_mask, and_mask, ~(nand_mask | or_mask), or_mask);
        }
      }
    }
    break;

  case 't':
    if (op != mct_add && sscanf (new_key, "text" scanf_dstr "_" scanf_dstr, scanf_dptr (&list_id), scanf_dptr (&object_id)) >= 2 * SCANF_DINTS) {
      if (convert_input_list_id (new_key, list_id) && convert_input_object_id (new_key, object_id)) {
        res = do_change_entry_text (list_id, object_id, stats_buff, size);
      }
    }
    break;
  }

  if (res == -2 && memcache_wait (c)) {
    return 0;
  }

  advance_skip_read_ptr (&c->In, size);
  return res > 0;
}

int exec_delete (struct connection *c, const char *str, int len) {
  static input_list_id_t list_id;
  static input_object_id_t object_id;

  clear_input_list_id (&list_id);
  clear_input_object_id (&object_id);

  int xor_mask = -1, and_mask = 7;

  while (*str == ' ') { str++; }

  if (verbosity > 0) {
    fprintf (stderr, "delete \"%s\"\n", str);
  }

  free_tmp_buffers (c);

  switch (*str) {
  case 'l':
    if (sscanf (str, "list" scanf_dstr ",%d,%d ", scanf_dptr (&list_id), &xor_mask, &and_mask) >= SCANF_DINTS_AM) {
      if (convert_input_list_id (str, list_id)) {
        delete_queries++;
        if (xor_mask == -1 && and_mask == 7) {
          return do_delete_list (list_id);
        } else {
          return do_delete_sublist (list_id, xor_mask, and_mask);
        }
      }
    }
    break;
  case 'o':
    if (sscanf (str, "object" scanf_dstr " ", scanf_dptr (&object_id)) >= SCANF_DINTS && !disable_revlist) {
      if (convert_input_object_id (str, object_id)) {
        delete_queries++;
        delete_object_queries++;
        return do_delete_object (object_id);
      }
    }
    break;
  case 'e':
    if (sscanf (str, "entry" scanf_dstr "_" scanf_dstr " ", scanf_dptr (&list_id), scanf_dptr (&object_id)) >= 2 * SCANF_DINTS) {
      if (convert_input_list_id (str, list_id) && convert_input_object_id (str, object_id)) {
        delete_queries++;
        return do_delete_list_entry (list_id, object_id);
      }
    }
    break;
  }
  return 0;
}


int memcache_delete (struct connection *c, const char *key, int key_len) {
  c->flags &= ~C_INTIMEOUT;
  char *new_key;
  int new_len;

  eat_at (key, key_len, &new_key, &new_len);

  int res = exec_delete (c, new_key, new_len);

  if (res == -2 && memcache_wait (c)) {
    return 0;
  }

  if (res > 0) {
    write_out (&c->Out, "DELETED\r\n", 9);
  } else {
    write_out (&c->Out, "NOT_FOUND\r\n", 11);
  }
  return 0;
}

static void exec_get_entry (struct connection *c, const char *new_key, int key_len, const char *old_key) {
  static input_list_id_t list_id;
  static input_object_id_t object_id;

  clear_input_list_id (&list_id);
  clear_input_object_id (&object_id);

  if (sscanf (new_key, "entry" scanf_dstr "_" scanf_dstr " ", scanf_dptr (&list_id), scanf_dptr (&object_id)) >= 2 * SCANF_DINTS) {
    if (convert_input_list_id (new_key, list_id) && convert_input_object_id (new_key, object_id)) {
      int res = fetch_list_entry (list_id, object_id, R);
      char *text = *((char **) (R + 10));
      int text_len = R[12];
      if (verbosity > 1) {
        fprintf (stderr, "get_entry(" idout "," idout ") = %d [ %d %d %d %d ... ]\n", out_list_id (list_id), out_object_id (object_id), res, R[0], R[1], R[2], R[3]);
      }
      if (res == -2 && memcache_wait (c)) {
        return;
      }
      if (text && text_len && res >= 6) {
        int t = sprintf(stats_buff, "%d,%lld,%d,%lld,%d,%d,%d,%d,", R[0], RR[2], R[1], RR[1], R[6], R[7], R[8], R[9]);
        memcpy (stats_buff + t, text, text_len);
        return_one_key (c, old_key, stats_buff, text_len + t);
        return;
      } else if (res >= 10) {
        return_one_key (c, old_key, stats_buff, sprintf(stats_buff, "%d,%lld,%d,%lld,%d,%d,%d,%d", R[0], RR[2], R[1], RR[1], R[6], R[7], R[8], R[9]));
        return;
      } else if (res >= 6) {
        return_one_key (c, old_key, stats_buff, sprintf(stats_buff, "%d,%lld,%d,%lld", R[0], RR[2], R[1], RR[1]));
        return;
      }
      if (return_false_if_not_found) {
        return_one_key_flags (c, old_key, NOT_FOUND_STRING, NOT_FOUND_STRING_LEN, NOT_FOUND_STRING_FLAGS);
        return;
      }
    }
  }
  return;
}

static void exec_get_entry_pos (struct connection *c, const char *new_key, int key_len, const char *old_key) {
  static input_list_id_t list_id;
  static input_object_id_t object_id;

  clear_input_list_id (&list_id);
  clear_input_object_id (&object_id);

  if (sscanf (new_key, "entry_pos" scanf_dstr "_" scanf_dstr " ", scanf_dptr (&list_id), scanf_dptr (&object_id)) >= 2 * SCANF_DINTS) {
    if (convert_input_list_id (new_key, list_id) && convert_input_object_id (new_key, object_id)) {
      int res = get_entry_position (list_id, object_id);
      if (verbosity > 1) {
        fprintf (stderr, "get_entry_pos(" idout "," idout ") = %d\n", out_list_id (list_id), out_object_id (object_id), res);
      }
      if (res == -2 && memcache_wait (c)) {
        return;
      }
      return_one_key (c, old_key, stats_buff, sprintf(stats_buff, "%d", res));
      return;
    }
  }
  return;
}

static void exec_get_entry_sublist_pos (struct connection *c, const char *new_key, int key_len, const char *old_key) {
  static input_list_id_t list_id;
  static input_object_id_t object_id;

  clear_input_list_id (&list_id);
  clear_input_object_id (&object_id);

  int mode = 0;

  if (sscanf (new_key, "entry_sublist_pos" scanf_dstr "_" scanf_dstr "_%d ", scanf_dptr (&list_id), scanf_dptr (&object_id), &mode) >= 2 * SCANF_DINTS + 1) {
    if (convert_input_list_id (new_key, list_id) && convert_input_object_id (new_key, object_id)) {
      mode &= 15;
      int res = get_entry_sublist_position (list_id, object_id, mode);

      if (verbosity > 1) {
        fprintf (stderr, "get_entry_sublist_pos(" idout "," idout ",%d) = %d\n", out_list_id (list_id), out_object_id (object_id), mode, res);
      }      
      if (res == -2 && memcache_wait (c)) {
        return;
      }

      return_one_key (c, old_key, stats_buff, sprintf(stats_buff, "%d", res));
      return;
    }
  }
  return;
}

static void exec_get_text (struct connection *c, const char *new_key, int key_len, const char *old_key) {
  static input_list_id_t list_id;
  static input_object_id_t object_id;

  clear_input_list_id (&list_id);
  clear_input_object_id (&object_id);

  if (sscanf (new_key, "text" scanf_dstr "_" scanf_dstr " ", scanf_dptr (&list_id), scanf_dptr (&object_id)) >= 2 * SCANF_DINTS) {
    if (convert_input_list_id (new_key, list_id) && convert_input_object_id (new_key, object_id)) {
      int res = fetch_list_entry (list_id, object_id, R);
      char *text = *((char **) (R + 10));
      int text_len = R[12];
      if (verbosity > 1) {
        fprintf (stderr, "get_entry(" idout "," idout ") = %d [ %d %d %d %d ... ]\n", out_list_id (list_id), out_object_id (object_id), res, R[0], R[1], R[2], R[3]);
      }
      if (res == -2 && memcache_wait (c)) {
        return;
      }
      if (res > 0 && text && text_len) {
        return_one_key (c, old_key, text, text_len);
        return;
      }
      if (return_false_if_not_found) {
        return_one_key_flags (c, old_key, NOT_FOUND_STRING, NOT_FOUND_STRING_LEN, NOT_FOUND_STRING_FLAGS);
        return;
      }
    }
  }
  return;
}

static void exec_get_flags (struct connection *c, const char *new_key, int key_len, const char *old_key) {
  static input_list_id_t list_id;
  static input_object_id_t object_id;

  clear_input_list_id (&list_id);
  clear_input_object_id (&object_id);

  if (sscanf (new_key, "flags" scanf_dstr "_" scanf_dstr " ", scanf_dptr (&list_id), scanf_dptr (&object_id)) >= 2 * SCANF_DINTS) {
    if (convert_input_list_id (new_key, list_id) && convert_input_object_id (new_key, object_id)) {
      int res = fetch_list_entry (list_id, object_id, R);
      if (verbosity > 1) {
        fprintf (stderr, "get_entry(" idout "," idout ") = %d [ %d %d %d %d ... ]\n", out_list_id (list_id), out_object_id (object_id), res, R[0], R[1], R[2], R[3]);
      }
      if (res == -2 && memcache_wait (c)) {
        return;
      }
      if (res >= 1) {
        return_one_key (c, old_key, stats_buff, sprintf (stats_buff, "%d", R[0]));
        return;
      }
      if (return_false_if_not_found) {
        return_one_key_flags (c, old_key, NOT_FOUND_STRING, NOT_FOUND_STRING_LEN, NOT_FOUND_STRING_FLAGS);
        return;
      }
    }
  }
  return;
}

static void exec_get_value (struct connection *c, const char *new_key, int key_len, const char *old_key) {
  static input_list_id_t list_id;
  static input_object_id_t object_id;

  clear_input_list_id (&list_id);
  clear_input_object_id (&object_id);

  if (sscanf (new_key, "value" scanf_dstr "_" scanf_dstr " ", scanf_dptr (&list_id), scanf_dptr (&object_id)) >= 2 * SCANF_DINTS) {
    if (convert_input_list_id (new_key, list_id) && convert_input_object_id (new_key, object_id)) {
      int res = fetch_list_entry (list_id, object_id, R);
      if (verbosity > 1) {
        fprintf (stderr, "get_entry(" idout "," idout ") = %d [ %d %d %d %d ... ]\n", out_list_id (list_id), out_object_id (object_id), res, R[0], R[1], R[2], R[3]);
      }
      if (res == -2 && memcache_wait (c)) {
        return;
      }
      if (res >= 6) {
        return_one_key (c, old_key, stats_buff, sprintf (stats_buff, "%lld", RR[2]));
        return;
      }
      if (return_false_if_not_found) {
        return_one_key_flags (c, old_key, NOT_FOUND_STRING, NOT_FOUND_STRING_LEN, NOT_FOUND_STRING_FLAGS);
        return;
      }
    }
  }
  return;
}

#ifdef VALUES64
#define UNUSED_VALUE (-1LL << 63)
#else
#define UNUSED_VALUE (-1 << 31)
#endif

static void exec_get_incr_value (struct connection *c, const char *new_key, int key_len, const char *old_key) {
  static input_list_id_t list_id;
  static input_object_id_t object_id;
  int flags = -1;
  value_t value = UNUSED_VALUE;

  clear_input_list_id (&list_id);
  clear_input_object_id (&object_id);

  if (sscanf (new_key, "incr_value" scanf_dstr "_" scanf_dstr "_%d+=" valin " ", scanf_dptr (&list_id), scanf_dptr (&object_id), &flags, &value) >= 2 * SCANF_DINTS + 2) {
    if (convert_input_list_id (new_key, list_id) && convert_input_object_id (new_key, object_id) && flags >= 0 && value != UNUSED_VALUE) {
      long long res = do_add_incr_value (list_id, object_id, flags, value, 0);
      if ((res == -1LL << 63) && memcache_wait (c)) {
        return;
      }
      if (res != -1LL << 63) {
        return_one_key (c, old_key, stats_buff, sprintf (stats_buff, "%lld", res));
        return;
      }
      if (return_false_if_not_found) {
        return_one_key_flags (c, old_key, NOT_FOUND_STRING, NOT_FOUND_STRING_LEN, NOT_FOUND_STRING_FLAGS);
        return;
      }
    }
  }
  return_one_key (c, old_key, "FAILED", 6);
}


static void exec_get_count (struct connection *c, const char *new_key, int key_len, const char *old_key) {
  static input_list_id_t list_id;
  int cnt_id = -1;

  clear_input_list_id (&list_id);

  if (sscanf (new_key, "count" scanf_dstr ",%d ", scanf_dptr (&list_id), &cnt_id) >= SCANF_DINTS_AM) {
    if (convert_input_list_id (new_key, list_id)) {
      int res = get_list_counter (list_id, cnt_id);
      if (verbosity > 1) {
        fprintf (stderr, "get_counter(" idout ",%d) = %d\n", out_list_id (list_id), cnt_id, res);
      }
      if (res == -2 && memcache_wait (c)) {
        return;
      }
      if (res >= 0) {
        return_one_key (c, old_key, stats_buff, sprintf(stats_buff, "%d", res));
        return;
      }
      if (return_false_if_not_found) {
        return_one_key_flags (c, old_key, NOT_FOUND_STRING, NOT_FOUND_STRING_LEN, NOT_FOUND_STRING_FLAGS);
        return;
      }
    }
  }
  return;
}

static void exec_get_counts (struct connection *c, const char *new_key, int key_len, const char *old_key) {
  static input_list_id_t list_id;

  clear_input_list_id (&list_id);

  if (sscanf (new_key, "counts" scanf_dstr " ", scanf_dptr (&list_id)) >= SCANF_DINTS) {
    if (convert_input_list_id (new_key, list_id)) {
      int res = fetch_list_counters (list_id, R);
      if (verbosity > 1) {
        fprintf (stderr, "get_counters(" idout ") = %d [ %d %d %d %d %d ]\n", out_list_id (list_id), res, R[0], R[1], R[2], R[3], R[4]);
      }
      if (res == -2 && memcache_wait (c)) {
        return;
      }
      if (res == 9) {
        return_one_key (c, old_key, stats_buff, sprintf(stats_buff, "%d,%d,%d,%d,%d,%d,%d,%d,%d", R[0], R[1], R[2], R[3], R[4], R[5], R[6], R[7], R[8]));
        return;
      } else if (res == 5) {
        return_one_key (c, old_key, stats_buff, sprintf(stats_buff, "%d,%d,%d,%d,%d", R[0], R[1], R[2], R[3], R[4]));
        return;
      }
      if (return_false_if_not_found) {
        return_one_key_flags (c, old_key, NOT_FOUND_STRING, NOT_FOUND_STRING_LEN, NOT_FOUND_STRING_FLAGS);
        return;
      }
    }
  }
  return;
}

static void exec_get_list (struct connection *c, const char *new_key, int key_len, const char *old_key, int old_key_len) {
  static input_list_id_t list_id;

  clear_input_list_id (&list_id);

  int mode = 0, limit = -1, offset = 0;
  if (sscanf (new_key, "list" scanf_dstr ",%d#%d,%d ", scanf_dptr (&list_id), &mode, &limit, &offset) >= SCANF_DINTS_AM) {
    if (convert_input_list_id (new_key, list_id)) {
      int res = prepare_list (list_id, mode, limit, offset);
      if (verbosity > 1) {
        fprintf (stderr, "prepare_list(" idout ",%d,%d,%d) = %d\n", out_list_id (list_id), mode, limit, offset, res);
      }
      if (res == -2 && memcache_wait (c)) {
        return;
      }
      if (res >= 0) {
        return_key_list_value (c, old_key, old_key_len, res, 0, R, R_end - R, R_entry_size, mode & 1024 ? R_entry_size - PTR_INTS - 1 : -1, (VALUE_INTS == 2 && (mode & 512)) ? value_offset : -1);
        return;
      }
      if (return_false_if_not_found) {
        return_one_key_flags (c, old_key, NOT_FOUND_STRING, NOT_FOUND_STRING_LEN, NOT_FOUND_STRING_FLAGS);
        return;
      }
    }
  }
  return;
}

static void exec_get_sorted_list (struct connection *c, const char *new_key, int key_len, const char *old_key, int old_key_len) {
  static input_list_id_t list_id;

  clear_input_list_id (&list_id);

  int mode, xor_mask, and_mask, limit = -1;
  if (sscanf (new_key, "sortedlist" scanf_dstr ",%d,%d,%d#%d ", scanf_dptr (&list_id), &xor_mask, &and_mask, &mode, &limit) >= SCANF_DINTS_AM + 4 && limit > 0) {
    if (convert_input_list_id (new_key, list_id)) {
      int res = prepare_value_sorted_list (list_id, xor_mask, and_mask, mode, limit);
      if (verbosity > 1) {
        fprintf (stderr, "prepare_value_sorted_list(" idout ",%d,%d,%d,%d) = %d\n", out_list_id (list_id), xor_mask, and_mask, mode, limit, res);
      }
      if (res == -2 && memcache_wait (c)) {
        return;
      }
      if (res >= 0) {
        return_key_list_value (c, old_key, old_key_len, res, 0, R, R_end - R, R_entry_size, mode & 1024 ? R_entry_size - PTR_INTS - 1 : -1, (VALUE_INTS == 2 && (mode & 512)) ? value_offset : -1);
        return;
      }
      if (return_false_if_not_found) {
        return_one_key_flags (c, old_key, NOT_FOUND_STRING, NOT_FOUND_STRING_LEN, NOT_FOUND_STRING_FLAGS);
        return;
      }
    }
  }
  return;
}

static void exec_get_datedistr (struct connection *c, const char *new_key, int key_len, const char *old_key, int old_key_len) {
  static input_list_id_t list_id;

  clear_input_list_id (&list_id);

  int mode, min_date, max_date, step = -1;
  if (sscanf (new_key, "datedistr" scanf_dstr ",%d#%d,%d,%d ", scanf_dptr (&list_id), &mode, &min_date, &max_date, &step) >= SCANF_DINTS_AM + 4 && mode >= 0 && mode <= 15 && step > 0 && max_date > min_date && min_date >= step && !((max_date - min_date) % step) && (max_date - min_date) / step <= MAX_RES) {
    if (convert_input_list_id (new_key, list_id)) {
      int res = prepare_list_date_distr (list_id, mode, min_date, max_date, step);
      if (verbosity > 1) {
        fprintf (stderr, "prepare_list_date_distr(" idout ",%d,%d,%d,%d) = %d\n", out_list_id (list_id), mode, min_date, max_date, step, res);
      }
      if (res == -2 && memcache_wait (c)) {
        return;
      }
      if (res >= 0) {
        return_key_list_value (c, old_key, old_key_len, res, 0, R, R_end - R, 1, -1, -1);
        return;
      }
      if (return_false_if_not_found) {
        return_one_key_flags (c, old_key, NOT_FOUND_STRING, NOT_FOUND_STRING_LEN, NOT_FOUND_STRING_FLAGS);
        return;
      }
    }
  }
  return;
}

static inline int my_rand (int N) {
  return ((unsigned long long) lrand48() * N) >> 31;
}

static int randomize_result (array_object_id_t *A, int N, int M, int result_entry_ints) {
  int i, j = 0;
  for (i = 0; i < N; i++) {
    if (my_rand(N-i) < M) {
      memcpy (A + j*result_entry_ints, A + i*result_entry_ints, result_entry_ints * 4);
      j++;
      if (!--M) {
        return j;
      }
    }
  }
  assert (!M);
  return j;
}

static void exec_get_intersect (struct connection *c, const char *new_key, int key_len, int shift, const char *old_key, int old_key_len) {
  static input_list_id_t list_id;
  int mode, tag, have_weights, t_tag, RL, randcnt = 0, id_ints;
  nb_iterator_t it;

  clear_input_list_id (&list_id);

  if (!c->Tmp || nbit_set (&it, c->Tmp) < 0 || nbit_read_in (&it, &have_weights, 4) < 4 || nbit_read_in (&it, &t_tag, 4) < 4 || nbit_read_in (&it, &id_ints, 4) < 4) {
    return;
  }

  if (sscanf (new_key + shift, scanf_dstr ",%d,%d#%d", scanf_dptr (&list_id), &mode, &tag, &randcnt) >= 1 * SCANF_DINTS + 2 && mode >= 0 && (mode & 63) <= SUBCATS && !(mode & ~0x7cf) && tag == t_tag) {
    if (convert_input_list_id (new_key + shift, list_id)) {
      RL = (get_total_ready_bytes (c->Tmp) >> 2) - 3;
      if (verbosity > 1) {
        fprintf (stderr, "found temp list %d, %d ints, have_weights = %d, id_ints = %d\n", tag, RL, have_weights, id_ints);
      }

      if (RL > MAX_RES) { RL = MAX_RES; }

      int result_entry_ints = OBJECT_ID_INTS;

      if (mode & 64) { result_entry_ints++; }
      if (mode & 128) { result_entry_ints++; }
      if (mode & 256) { result_entry_ints++; }
      if (mode & 512) { result_entry_ints += VALUE_INTS; }
      if (mode & 1024) { result_entry_ints += PTR_INTS + 1; }

      int maxval = (MAX_RES / result_entry_ints) * (id_ints + have_weights);

      if (RL > maxval) { RL = maxval; }

      int *R_start = R + (MAX_RES - RL);

      int x = nbit_read_in (&it, R_start, RL*4);
      assert (x == RL*4);

      /*if (verbosity > 1 && RL > 0) {
        fprintf (stderr, "first entries: %d %d %d %d %d %d %d\n", R[], R[], R[], R[], R[], R[], R[]);
      }*/

      assert (!(RL % (id_ints + have_weights)));
      RL /= id_ints + have_weights;

      if (shift == 7) {
        long long res;
        res = prepare_list_sum (list_id, mode, (array_object_id_t *)R_start, RL, have_weights, id_ints);
        if (res == -2 && memcache_wait (c)) {
          return;
        }
        return_one_key (c, old_key, stats_buff, sprintf (stats_buff, "%lld", res));
        return;
      }

      int res;
      res = (shift == 9) ? prepare_list_intersection (list_id, mode, (array_object_id_t *)R_start, RL, have_weights, id_ints) :
      prepare_list_subtraction (list_id, mode, (array_object_id_t *)R_start, RL, have_weights, id_ints);

      if (res == -2 && memcache_wait (c)) {
        return;
      }

      assert (res <= MAX_OBJECT_RES);
      if (randcnt > 0 && res > 0 && randcnt < res) {
        res = randomize_result ((array_object_id_t *) R, res, randcnt, result_entry_ints);
      }


      if (verbosity > 1) {
        fprintf (stderr, "prepare_list_%.*sion(" idout ",%d,%d) = %d\n", shift, new_key, out_list_id (list_id), mode, RL, res);
      }

      if (res >= 0) {
        assert (res <= MAX_OBJECT_RES);
        assert (R_entry_size == result_entry_ints);
        return_key_list_value (c, old_key, old_key_len, res, 0, R, res * result_entry_ints, R_entry_size, mode & 1024 ? R_entry_size - PTR_INTS - 1 : -1, (VALUE_INTS == 2 && (mode & 512)) ? value_offset : -1);
        return;
      }
    }
  }
  return;
}

int memcache_get (struct connection *c, const char *key, int key_len) {
  if (verbosity > 1) {
    fprintf (stderr, "memcache_get: key='%s'\n", key);
  }

  char *new_key;
  int new_len;

  eat_at (key, key_len, &new_key, &new_len);

  if (new_len >= 9 && !strncmp (new_key, "datedistr", 9)) {
    exec_get_datedistr (c, new_key, new_len, key, key_len);
    return 0;
  }

  if (new_len >= 9 && !strncmp (new_key, "intersect", 9)) {
    exec_get_intersect (c, new_key, new_len, 9, key, key_len);
    return 0;
  }

  if (new_len >= 8 && !strncmp (new_key, "subtract", 8)) {
    exec_get_intersect (c, new_key, new_len, 8, key, key_len);
    return 0;
  }

  if (new_len >= 7 && !strncmp (new_key, "sumlist", 7)) {
    exec_get_intersect (c, new_key, new_len, 7, key, key_len);
    return 0;
  }

  if (new_len >= 4 && !strncmp (new_key, "list", 4)) {
    exec_get_list (c, new_key, new_len, key, key_len);
    return 0;
  }

  if (new_len >= 10 && !strncmp (new_key, "sortedlist", 10)) {
    exec_get_sorted_list (c, new_key, new_len, key, key_len);
    return 0;
  }

  if (new_len >= 9 && !strncmp (new_key, "entry_pos", 9)) {
    exec_get_entry_pos (c, new_key, new_len, key);
    return 0;
  }

  if (new_len >= 17 && !strncmp (new_key, "entry_sublist_pos", 17)) {
    exec_get_entry_sublist_pos (c, new_key, new_len, key);
    return 0;
  }

  if (new_len >= 5 && !strncmp (new_key, "entry", 5)) {
    exec_get_entry (c, new_key, new_len, key);
    return 0;
  }

  if (new_len >= 5 && !strncmp (new_key, "flags", 5)) {
    exec_get_flags (c, new_key, new_len, key);
    return 0;
  }

  if (new_len >= 5 && !strncmp (new_key, "value", 5)) {
    exec_get_value (c, new_key, new_len, key);
    return 0;
  }

  if (new_len >= 4 && !strncmp (new_key, "text", 4)) {
    exec_get_text (c, new_key, new_len, key);
    return 0;
  }

  if (new_len >= 6 && !strncmp (new_key, "counts", 6)) {
    exec_get_counts (c, new_key, new_len, key);
    return 0;
  }

  if (new_len >= 5 && !strncmp (new_key, "count", 5)) {
    exec_get_count (c, new_key, new_len, key);
    return 0;
  }

  if (new_len >= 10 && !strncmp (new_key, "incr_value", 10)) {
    exec_get_incr_value (c, new_key, new_len, key);
  }

  if (new_len >= 16 && !strncmp (new_key, "free_block_stats", 16)) {
    return_one_key_list (c, key, key_len, MAX_RECORD_WORDS, 0, FreeCnt, MAX_RECORD_WORDS);
    return 0;
  }

  if (new_len >= 16 && !strncmp (new_key, "used_block_stats", 16)) {
    return_one_key_list (c, key, key_len, MAX_RECORD_WORDS, 0, UsedCnt, MAX_RECORD_WORDS);
    return 0;
  }

  if (new_len >= 16 && !strncmp (new_key, "allocation_stats", 16)) {
    return_one_key_list (c, key, key_len, MAX_RECORD_WORDS, 0, NewAllocations[0], MAX_RECORD_WORDS * 4);
    return 0;
  }

  if (new_len >= 17 && !strncmp (new_key, "split_block_stats", 17)) {
    return_one_key_list (c, key, key_len, MAX_RECORD_WORDS, 0, SplitBlocks, MAX_RECORD_WORDS);
    return 0;
  }

  if (new_len >= 5 && !strncmp (new_key, "stats", 5)) {
    int len = lists_prepare_stats (c);
    return_one_key (c, key, stats_buff, len);
    return 0;
  }

  return 0;
}

int memcache_get_end (struct connection *c, int key_count) {
  vkprintf (1, "memcache_get_end: %d keys requested\n", key_count);
  free_tmp_buffers (c);
  return mcs_get_end (c, key_count);
}

int memcache_get_start (struct connection *c) {
  c->flags &= ~C_INTIMEOUT;
  vkprintf (1, "memcache_get_start\n");
  return 0;
}



int memcache_stats (struct connection *c) {
  int len = lists_prepare_stats (c);
  write_out (&c->Out, stats_buff, len);
  write_out (&c->Out, "END\r\n", 5);
  return 0;
}


int memcache_incr (struct connection *c, int op, const char *key, int key_len, long long arg) {
  c->flags &= ~C_INTIMEOUT;
  static input_list_id_t list_id;
  static input_object_id_t object_id;

  clear_input_list_id (&list_id);
  clear_input_object_id (&object_id);

  int res;
  if (verbosity > 1) {
    fprintf (stderr, "memcache_incr: op=%d, key='%s', val=%lld\n", op, key, arg);
  }

  free_tmp_buffers (c);

  char *new_key;
  int new_len;

  eat_at (key, key_len, &new_key, &new_len);

  if (new_len >= 5 && !memcmp (new_key, "flags", 5)) {
    res = -1;
    if (sscanf (new_key, "flags" scanf_dstr "_" scanf_dstr, scanf_dptr (&list_id), scanf_dptr (&object_id)) >= 2 * SCANF_DINTS) {
      if (convert_input_list_id (new_key, list_id) && convert_input_object_id (new_key, object_id)) {
        ++increment_queries;
        res = do_change_entry_flags (list_id, object_id, op ? 0 : arg, op ? arg : 0);
      }
    }
    if (res == -2 && memcache_wait (c)) {
      return 0;
    }
    if (res >= 0) {
      write_out (&c->Out, stats_buff, sprintf(stats_buff, "%d\r\n", res));
    } else {
      write_out (&c->Out, "NOT_FOUND\r\n", 11);
    }
    return 0;
  }

  if (new_len >= 5 && !memcmp (new_key, "value", 5)) {
    long long res = -1LL << 63;
    if (sscanf (new_key, "value" scanf_dstr "_" scanf_dstr, scanf_dptr (&list_id), scanf_dptr (&object_id)) >= 2 * SCANF_DINTS) {
      if (convert_input_list_id (new_key, list_id) && convert_input_object_id (new_key, object_id)) {
        ++increment_queries;
        res = do_change_entry_value (list_id, object_id, op ? -arg : arg, 1);
      }
    }
    if (res == (-1LL << 63) && memcache_wait (c)) {
      return 0;
    }
    if (res != (-1LL << 63)) {
      write_out (&c->Out, stats_buff, sprintf(stats_buff, "%lld\r\n", res));
    } else {
      write_out (&c->Out, "NOT_FOUND\r\n", 11);
    }
    return 0;
  }

  write_out (&c->Out, "NOT_FOUND\r\n", 11);
  return 0;
}

void tl_store_list_object_id (const int *R, const int *R_end, int mode, int res) {
  const int *Rptr = R;
  int i = 0;
  for (i = 0; i < res; i++) {
    tl_store_string_data ((void *)Rptr, 4 * object_id_ints);
    Rptr += object_id_ints;
  }
}

void tl_store_list_object_full (const int *R, const int *R_end, int mode, int res) {
  const int *Rptr = R;
  int i = 0;
  for (i = 0; i < res; i++) {
    if (mode & (1 << 15)) {
      tl_store_string_data ((void *)Rptr, 4 * object_id_ints);
    }
    Rptr += object_id_ints;
    if (mode & 64) { tl_store_int (*(Rptr ++)); }
    if (mode & 128) { tl_store_int (*(Rptr ++)); }
    if (mode & 256) {
      tl_store_long (*Rptr);
      Rptr ++;
    }
    if (mode & 512) {
      if (VALUE_INTS == 2) {
        tl_store_long (*(long long *)Rptr);
        Rptr += 2;
      } else {
        tl_store_long (*Rptr);
        Rptr ++;
      }
    }
    if (mode & 1024) { 
      char *text = *(char **)Rptr;
      Rptr += PTR_INTS;
      int text_len = *(Rptr ++);
      tl_store_string (text, text_len);
    }
  }
}
/*
int tl_do_entry_get (list_id_t list_id, object_id_t object_id) {
  int res = fetch_list_entry (list_id, object_id, R);
  if (res == -2) { return -2; }
  if (res < 0) {
    tl_store_int (TL_LIST_ENTRY_NOT_FOUND);
    return 0;
  }
  int text_len = R[12];
  if (res > 6 || text_len) {
    tl_store_int (res > 6 ? TL_LIST_ENTRY_FULL : TL_LIST_ENTRY);
    tl_store_int (R[0]);
    tl_store_long (*(long long *)(R + 4));
    tl_store_int (R[1]);
    tl_store_long (*(long long *)(R + 2));
    if (res > 6) {
      tl_store_int (R[6]);
      tl_store_int (R[7]);
      tl_store_int (R[8]);
      tl_store_int (R[9]);
    }
    char **text = (char **)(R + 10);
    tl_store_string (text ? *text : 0, text_len);
  } else {
    tl_store_int (TL_LIST_ENTRY_SHORT);
    tl_store_int (R[0]);
    tl_store_long (*(long long *)(R + 4));
    tl_store_int (R[1]);
    tl_store_long (*(long long *)(R + 2));
  }


  return 0;
}

int tl_do_list_get (list_id_t list_id, int mode, int limit, int offset) {
  mode &= 63;
  int res = prepare_list (list_id, mode, limit, offset);
  if (res == -2) { return -2; }
  if (res < 0) { return -1; }
  tl_store_int (TL_OBJECT_ID_LIST_COUNT);
  tl_store_int (res);
  tl_store_int ((R_end - R) / R_entry_size);
  tl_store_list_object_id (R, R_end, mode, (R_end - R) / R_entry_size);
  return 0;
}
*/

int tl_store_object_id (object_id_t id);

int tl_do_list_delete (struct tl_act_extra *extra) {
  struct tl_list_delete *e = (void *)extra->extra;
  int res = do_delete_list (e->list_id);
  if (res == -2) { return -2; }
  tl_store_int (res <= 0 ? TL_BOOL_FALSE : TL_BOOL_TRUE);
  return 0;
}

int tl_do_list_entry_delete (struct tl_act_extra *extra) {
  struct tl_list_entry_delete *e = (void *)extra->extra;
  int res = do_delete_list_entry (e->list_id, e->object_id);
  if (res == -2) { return -2; }
  tl_store_int (res <= 0 ? TL_BOOL_FALSE : TL_BOOL_TRUE);
  return 0;
}

int tl_do_object_delete (struct tl_act_extra *extra) {
  struct tl_object_delete *e = (void *)extra->extra;
  int res = do_delete_object (e->object_id);
  assert (res != -2);
  tl_store_int (TL_BOOL_STAT);
  tl_store_int (res > 0);
  tl_store_int (res <= 0);
  tl_store_int (0);
  return 0;
}

int tl_do_list_entry_set (struct tl_act_extra *extra) {
  struct tl_list_entry_set *e = (void *)extra->extra;
  int res;
  if (e->text_len < 0) {
    res = do_add_list_entry (e->list_id, e->object_id, e->op, e->flags, e->value, &e->ip);
    if (res == -2) { return -2; }
    if (res <= 0) {
      tl_store_int (TL_BOOL_FALSE);
      return 0;
    }
  } else {
    res = do_add_list_entry (e->list_id, e->object_id, e->op == 0 ? 3 : e->op , e->flags, e->value, &e->ip);
  
    if (res == -2) { return -2; }
    if (res <= 0) {
      tl_store_int (TL_BOOL_FALSE);
      return 0;
    }

    res = do_change_entry_text (e->list_id, e->object_id, e->text, e->text_len);
    assert (res >= 0);
  }

  tl_store_int (TL_BOOL_TRUE);
  return 0;
}

int tl_do_list_entry_set_text (struct tl_act_extra *extra) {  
  struct tl_list_entry_set_text *e = (void *)extra->extra;
  int res = do_change_entry_text (e->list_id, e->object_id, e->text, e->len);
  if (res == -2) { return -2; }
  tl_store_int (res <= 0 ? TL_BOOL_FALSE : TL_BOOL_TRUE);
  return 0;
}

int tl_do_list_entry_set_flags (struct tl_act_extra *extra) {
  struct tl_list_entry_set_flags *e = (void *)extra->extra;
  int res = do_change_entry_flags (e->list_id, e->object_id, e->or_mask, e->nand_mask);
  if (res == -2) { return -2; }
  tl_store_int (res <= 0 ? TL_BOOL_FALSE : TL_BOOL_TRUE);
  return 0;
}

int tl_do_list_entry_set_value (struct tl_act_extra *extra) {
  struct tl_list_entry_set_value *e = (void *)extra->extra;
  long long res = do_change_entry_value (e->list_id, e->object_id, e->value, e->flags);
  if (res == (-1ll << 63) && WaitAioArrPos) { return -2; }
  if (res == (-1ll << 63)) {
    tl_store_int (TL_MAYBE_FALSE);
  } else {
    tl_store_int (TL_MAYBE_TRUE);
    tl_store_long (res);
  }
  return 0;
}

int tl_do_list_entry_incr_or_create (struct tl_act_extra *extra) {
  struct tl_list_entry_incr_or_create *e = (void *)extra->extra;
  long long res = do_add_incr_value (e->list_id, e->object_id, e->flags, e->value, 0);
  if (res == (-1ll << 63) && WaitAioArrPos) { return -2; }
  if (res == (-1ll << 63)) {
    tl_store_int (TL_MAYBE_FALSE);
  } else {
    tl_store_int (TL_MAYBE_TRUE);
    tl_store_long (res);
  }
  return 0;
}

int tl_do_list_set_flags (struct tl_act_extra *extra) {
  struct tl_list_set_flags *e = (void *)(extra->extra);
  long long res = do_change_sublist_flags (e->list_id, e->xor_mask, e->and_mask, ~(e->nand_mask | e->or_mask), e->or_mask);
  if (res == -2) { return -2; }
  tl_store_int (res <= 0 ? TL_BOOL_FALSE : TL_BOOL_TRUE);
  return 0;
}

int tl_do_sublist_delete (struct tl_act_extra *extra) {
  struct tl_sublist_delete *e = (void *)(extra->extra);
  //vkprintf (1, "delete_sublist " idout " xor_mask = %d, and_mask = %d\n", out_list_id (e->list_id), e->xor_mask, e->and_mask);
  long long res = do_delete_sublist (e->list_id, e->xor_mask, e->and_mask);
  if (res == -2) { return -2; }
  tl_store_int (res <= 0 ? TL_BOOL_FALSE : TL_BOOL_TRUE);
  return 0;
}

int tl_do_list_entry_get (struct tl_act_extra *extra) {
  struct tl_list_entry_get *e = (void *)(extra->extra);
  int res = fetch_list_entry (e->list_id, e->object_id, R);
  if (res == -2) { return -2; }
  if (res < 0) {    
    tl_store_int (TL_MAYBE_FALSE);
    return 0;
  }
  assert (res == 6);
  int text_len = R[12];

  tl_store_int (TL_MAYBE_TRUE);
  //tl_store_int (text_len > 0 ? 31 << 6 : 15 << 6);

  if (e->mode & (1 << 15)) {
    tl_store_object_id (e->object_id);
  }
  if (e->mode & (1 << 6)) {
    tl_store_int (R[0]);
  }
  if (e->mode & (1 << 7)) {
    tl_store_int (R[1]);
  }
  if (e->mode & (1 << 8)) { 
    tl_store_long (*(long long *)(R + 2));
  }
  if (e->mode & (1 << 9)) {
    tl_store_long (*(long long *)(R + 4));
  }
  if (e->mode & (1 << 10)) {
    char **text = (char **)(R + 10);
    tl_store_string (text ? *text : 0, text_len);
  }
  return 0;
}

int tl_do_list_entry_get_int (struct tl_act_extra *extra) {
  struct tl_list_entry_get_int *e = (void *)(extra->extra);
  int res = fetch_list_entry (e->list_id, e->object_id, R);
  if (res == -2) { return -2; }
  if (res < 0) {    
    tl_store_int (TL_MAYBE_FALSE);
    return 0;
  }
  tl_store_int (TL_MAYBE_TRUE);
  if (e->is_long) {
    tl_store_long (*(long long *)(R + e->offset));
  } else {
    tl_store_int (R[e->offset]);
  }
  return 0;
}

int tl_do_list_entry_get_text (struct tl_act_extra *extra) {
  struct tl_list_entry_get_text *e = (void *)(extra->extra);
  int res = fetch_list_entry (e->list_id, e->object_id, R);
  if (res == -2) { return -2; }
  if (res < 0) {    
    tl_store_int (TL_MAYBE_FALSE);
    return 0;
  }
  tl_store_int (TL_MAYBE_TRUE);
  int text_len = R[12];
  char **text = (char **)(R + 10);
  assert (text || !text_len); 
  tl_store_string (text ? *text : 0, text_len); 
  return 0;
}

int tl_do_list_entry_get_pos (struct tl_act_extra *extra) {
  struct tl_list_entry_get_pos *e = (void *)(extra->extra);
  int res = get_entry_position (e->list_id, e->object_id);
  if (res == -2) { return -2; }
  if (res < 0) {
    return res;
  }
  tl_store_int (TL_INT);
  tl_store_int (res);
  return 0;
}

int tl_do_list_get (struct tl_act_extra *extra) {
  struct tl_list_get *e = (void *)(extra->extra);
  int res = prepare_list (e->list_id, e->mode, e->limit, e->offset);
  if (res == -2) { return -2; }
  if (res < 0) { return res; }

  tl_store_int (TL_VECTOR_TOTAL);
  tl_store_int (res);
  tl_store_int ((R_end - R) / R_entry_size);
  tl_store_list_object_full (R, R_end, e->mode, (R_end - R) / R_entry_size);
  return 0;
}

int tl_do_list_count (struct tl_act_extra *extra) {
  struct tl_list_count *e = (void *)(extra->extra);
  int res = get_list_counter (e->list_id, e->cnt);
  if (res == -2) { return -2; }
  //if (res < 0) { return res; }
  tl_store_int (TL_INT);
  tl_store_int (res >= 0 ? res : 0);
  return 0;
}

int tl_do_sublists_count (struct tl_act_extra *extra) {
  struct tl_list_count *e = (void *)(extra->extra);
  int res = fetch_list_counters (e->list_id, R);
  if (res == -2) { return -2; }
  if (res < 0) { return res; }
  if (res > 9) { res = 9; }
  int i;
  tl_store_int (TL_TUPLE);
  for (i = 0; i < res; i++) {
    tl_store_int (R[i]);
  }
  for (i = res; i < 9; i++) {
    tl_store_int (0);
  }
  return 0;
}

int tl_do_list_intersect (struct tl_act_extra *extra) {
  struct tl_list_intersect *e = (void *)(extra->extra);

  int result_entry_ints = object_id_ints;  
  int res = (e->is_intersect ? prepare_list_intersection : prepare_list_subtraction) (e->list_id, e->mode & 63, (array_object_id_t *)e->arr, e->count, 0, e->id_ints);

  if (res == -2) { return -2; }
  if (res < 0) { return -1; }
  tl_store_int (TL_VECTOR_TOTAL);
  tl_store_int (res);
  res = (R_end - R) / result_entry_ints;

  if (e->limit > 0 && res > 0 && e->limit < res) {
    res = randomize_result ((array_object_id_t *) R, res, e->limit, result_entry_ints);
  } 
  tl_store_int (res);
  tl_store_list_object_id (R, R_end, e->mode & 63, res);
  return 0;
}

int tl_do_list_intersect_full (struct tl_act_extra *extra) {
  struct tl_list_intersect *e = (void *)(extra->extra);
  int result_entry_ints = object_id_ints;  
  if (e->mode & 64) { result_entry_ints ++; }
  if (e->mode & 128) { result_entry_ints ++; }
  if (e->mode & 256) { result_entry_ints ++; }
  if (e->mode & 512) { result_entry_ints += VALUE_INTS; }
  if (e->mode & 1024) { result_entry_ints += PTR_INTS + 1; }
  int res = (e->is_intersect ? prepare_list_intersection : prepare_list_subtraction) (e->list_id, e->mode, (array_object_id_t *)e->arr, e->count, 0, e->id_ints);
  if (res == -2) { return -2; }
  if (res < 0) { return -1; }
  tl_store_int (TL_VECTOR_TOTAL);
  tl_store_int (res);
  res = (R_end - R) / result_entry_ints;
  if (e->limit > 0 && res > 0 && e->limit < res) {
    res = randomize_result ((array_object_id_t *) R, res, e->limit, result_entry_ints);
  } 
  tl_store_int (res);
  tl_store_list_object_full (R, R_end, e->mode, res);
  return 0;
}

int tl_do_list_sum (struct tl_act_extra *extra) {
  struct tl_list_sum *e = (void *)(extra->extra);
  long long res = prepare_list_sum (e->list_id, e->mode, (array_object_id_t *)e->arr, e->count, e->has_weights, e->id_ints);
  if (res == -2) {
    return -2;
  }
  if (res < 0) {
    return res;
  }
  tl_store_int (TL_LONG);
  tl_store_long (res);
  return 0;
}

int tl_do_list_sorted (struct tl_act_extra *extra) {
  struct tl_list_sorted *e = (void *)(extra->extra);
  int res = prepare_value_sorted_list (e->list_id, e->xor_mask, e->and_mask, e->mode & 63, e->limit);
  if (res == -2) {
    return -2;
  }
  if (res < 0) { return -1; }
  tl_store_int (TL_VECTOR_TOTAL);
  tl_store_int (res);
  tl_store_int ((R_end - R) / R_entry_size);
  tl_store_list_object_id (R, R_end, e->mode & 63, (R_end - R) / R_entry_size);
  return 0;
}

int tl_do_list_sorted_full (struct tl_act_extra * extra) {
  struct tl_list_sorted *e = (void *)(extra->extra);
  int res = prepare_value_sorted_list (e->list_id, e->xor_mask, e->and_mask, e->mode, e->limit);
  if (res == -2) {
    return -2;
  }
  if (res < 0) { return -1; }
  tl_store_int (TL_VECTOR_TOTAL);
  tl_store_int (res);
  tl_store_int ((R_end - R) / R_entry_size);
  tl_store_list_object_full (R, R_end, e->mode, (R_end - R) / R_entry_size);
  return 0;
}

int tl_do_datedistr (struct tl_act_extra *extra) {
  struct tl_datedistr *e = (void *)extra->extra;
  vkprintf (1, "tl_do_datedist: mode = %d, min_date = %d, max_date = %d, step = %d\n", e->mode, e->min_date, e->max_date, e->step);
  if ((e->mode & ~15) || e->step <= 0 || e->min_date >= e->max_date || e->min_date < e->step || ((e->max_date - e->min_date) % e->step) || (e->max_date - e->min_date) / e->step > MAX_RES) {
    tl_fetch_set_error ("Invalid params", TL_ERROR_VALUE_NOT_IN_RANGE);
    return -1;
  }
  int res = prepare_list_date_distr (e->list_id, e->mode, e->min_date, e->max_date, e->step);
  if (res == -2) {
    return -2;
  }
  if (res < 0) { return -1; }
  tl_store_int (TL_VECTOR);
  //tl_store_int (TL_MAYBE_TRUE);
  tl_store_int (res);
  assert (R_end - R == res);
  tl_store_string_data ((char *)R, (R_end - R) * 4);
  vkprintf (1, "tl_do_datedistr: res = %d\n", res);
  return 0;
}


int tl_store_object_id (object_id_t id) {
#ifdef LISTS_Z
  tl_store_string_data ((char *)id, 4 * object_id_ints);
#elif defined (LISTS64)
  tl_store_long (id);
#else
  tl_store_int (id);
#endif
  return 0;
}

int fetch_list_id (var_list_id_t *list_id) {
  assert (list_id);
  if (tl_fetch_check (4 * list_id_ints) < 0) {
    return -1;    
  }
#ifdef LISTS_Z
  tl_fetch_raw_data (*list_id, 4 * list_id_ints);
#elif defined (LISTS64)
  tl_fetch_raw_data (list_id, 8);
#else
  tl_fetch_raw_data (list_id, 4);
#endif
  if (conv_list_id (*list_id) < 0) {
    tl_fetch_set_error_format (TL_ERROR_BAD_VALUE, "list_id in not from this engine. log_split_mod = %d, log_split_min = %d, log_split_max = %d", log_split_mod, log_split_min, log_split_max);
    return -1;
  }
  return 0;
}

int fetch_object_id (var_object_id_t *object_id) {
  assert (object_id);
  if (tl_fetch_check (4 * object_id_ints) < 0) {
    return -1;    
  }
#ifdef LISTS_Z
  tl_fetch_raw_data (*object_id, 4 * object_id_ints);
#elif defined (LISTS64)
  tl_fetch_raw_data (object_id, 8);
#else
  tl_fetch_raw_data (object_id, 4);
#endif
  return 0;
}

void copy_list_id (var_list_id_t *dst, list_id_t src) {
  assert (dst);
#ifdef LISTS_Z
  memcpy (*dst, src, 4 * list_id_ints);
#else
  *dst = src;
#endif
}

void copy_object_id (var_object_id_t *dst, object_id_t src) {
  assert (dst);
#ifdef LISTS_Z
  memcpy (*dst, src, 4 * object_id_ints);
#else
  *dst = src;
#endif
}


int tl_fetch_check_list_id_size (void) {
  int x = tl_fetch_int ();
  if (x != list_id_ints) {
    tl_fetch_set_error_format (TL_ERROR_BAD_VALUE, "Wrong number of ints in list_id: should be %d, present %d", list_id_ints, x);
    return -1;
  }
  return 0;
}

int tl_fetch_check_object_id_size (void) {
  int x = tl_fetch_int ();
  if (x != object_id_ints) {
    tl_fetch_set_error_format (TL_ERROR_BAD_VALUE, "Wrong number of ints in object_id: should be %d, present %d", object_id_ints, x);
    return -1;
  }
  return 0;
}

#define CHECK_LIST_INTS if (tl_fetch_check_list_id_size () < 0) { return 0; }
#define CHECK_OBJECT_INTS if (tl_fetch_check_object_id_size () < 0) { return 0; }
#define CHECK_LIST_OBJECT_INTS CHECK_LIST_INTS CHECK_OBJECT_INTS

/*
int tl_entry_get (int op) {
  CHECK_LIST_OBJECT_INTS;
  if (tl_fetch_error ()) {
    return -1;
  }
  var_list_id_t list_id;
  if (fetch_list_id (&list_id) < 0) {
    return -1;
  }
  var_object_id_t object_id;
  if (fetch_object_id (&object_id) < 0) {
    return -1;
  }

  tl_fetch_end ();
  if (tl_fetch_error ()) {
    return -1;
  }


  int x = tl_do_entry_get (list_id, object_id);
  if (x == -2) {
    assert (WaitAio);
    tl_memcache_wait (tl_saved_query_init_small (op, tl_entry_get_restart, list_id, object_id, 0, 0, 0, 0));
    return -2;
  }
  return 0;
}

int tl_list_get (int op) {
  CHECK_LIST_OBJECT_INTS;
  var_list_id_t list_id;
  if (fetch_list_id (&list_id) < 0) {
    return -1;
  }
  static var_object_id_t object_id;
  int mode = 0;
  int limit = -1;
  int offset = 0;
  mode = tl_fetch_int ();
  if (op == TL_LIST_GET_LIMIT) {
    limit = tl_fetch_int ();
    offset = tl_fetch_int ();
  }

  tl_fetch_end ();
  if (tl_fetch_error ()) {
    return -1;
  }

  int x = tl_do_list_get (list_id, mode, limit, offset);
  if (x == -2) {
    assert (WaitAio);
    tl_memcache_wait (tl_saved_query_init_small (op, tl_list_get_restart, list_id, object_id, mode, limit, offset, 0));
    return -2;
  }
  return 0;
}

*/

struct tl_act_extra *tl_list_delete (void) {
  struct tl_act_extra *extra = tl_act_extra_init (stats_buff, sizeof (struct tl_list_delete), tl_do_list_delete);
  struct tl_list_delete *e = (void *)extra->extra;
  CHECK_LIST_OBJECT_INTS;
  if (fetch_list_id (&e->list_id) < 0) {
    return 0;
  }
  tl_fetch_end ();
  if (tl_fetch_error ()) {
    return 0;
  }
  return extra;
}

struct tl_act_extra *tl_list_entry_delete (void) {
  struct tl_act_extra *extra = tl_act_extra_init (stats_buff, sizeof (struct tl_list_entry_delete), tl_do_list_entry_delete);
  struct tl_list_entry_delete *e = (void *)extra->extra;
  CHECK_LIST_OBJECT_INTS;
  if (fetch_list_id (&e->list_id) < 0) {
    return 0;
  }
  if (fetch_object_id (&e->object_id) < 0) {
    return 0;
  }

  tl_fetch_end ();
  if (tl_fetch_error ()) {
    return 0;
  }
  return extra;
}

struct tl_act_extra *tl_object_delete (void) {
  struct tl_act_extra *extra = tl_act_extra_init (stats_buff, sizeof (struct tl_object_delete), tl_do_object_delete);
  struct tl_object_delete *e = (void *)extra->extra;
  CHECK_LIST_OBJECT_INTS;
  if (fetch_object_id (&e->object_id) < 0) {
    return 0;
  }
  tl_fetch_end ();
  if (tl_fetch_error ()) {
    return 0;
  }
  delete_object_queries ++; 
  return extra;
}

struct tl_act_extra *tl_list_entry_set (int op) {
  struct tl_act_extra *extra = tl_act_extra_init (stats_buff, sizeof (struct tl_list_entry_set), tl_do_list_entry_set);
  struct tl_list_entry_set *e = (void *)extra->extra;
  CHECK_LIST_OBJECT_INTS;
  if (fetch_list_id (&e->list_id) < 0) {
    return 0;
  }
  e->mode = tl_fetch_int ();
  if (tl_fetch_error ()) {
    return 0;
  }
  if (!(e->mode & TL_LIST_FLAG_OBJECT_ID)) {
    tl_fetch_set_error_format (TL_ERROR_BAD_VALUE, "entry must contain object id (mode = 0x%08x)", e->mode);
    return 0;
  }
  if (e->mode & (TL_LIST_FLAG_DATE | TL_LIST_FLAG_GLOBAL_ID)) {
    tl_fetch_set_error_format (TL_ERROR_BAD_VALUE, "entry can not contain date and global_id (mode = 0x%08x)", e->mode);
    return 0;
  }
/*  if (e->mode & (TL_LIST_FLAG_TEXT)) {
    tl_fetch_set_error_format (TL_ERROR_BAD_VALUE, "text in set_entry is unsupported. Sorry. (mode = 0x%08x)", e->mode);
    return 0;
  }*/
  if (fetch_object_id (&e->object_id) < 0) {
    return 0;
  }
  e->flags = (e->mode & TL_LIST_FLAG_FLAGS) ? tl_fetch_int () : 0;
  e->value = (e->mode & TL_LIST_FLAG_VALUE) ? tl_fetch_long () : 0;
#ifdef VALUES64
  if (e->value != (int)e->value && (e->mode & (TL_LIST_FLAG_IP | TL_LIST_FLAG_FRONT_IP | TL_LIST_FLAG_PORT | TL_LIST_FLAG_UA_HASH))) {
    tl_fetch_set_error_format (TL_ERROR_BAD_VALUE, "can not set 64-bit value with ip/front_ip/port/ua_hash. (mode = 0x%08x, value = %lld)", e->mode, e->value);
    return 0;
  }
#else
  if (e->value != (int)e->value) {
    tl_fetch_set_error_format (TL_ERROR_BAD_VALUE, "64-bit value can not be used in this version of lists (value = %lld)", e->value);
    return 0;
  }
#endif
  e->text_len = (e->mode & TL_LIST_FLAG_TEXT) ? tl_fetch_string0 (e->text, max_text_len - 1) : -1;
  extra->size += e->text_len + 1;
  e->ip = (e->mode & TL_LIST_FLAG_IP) ? tl_fetch_int () : 0;
  e->front_ip = (e->mode & TL_LIST_FLAG_FRONT_IP) ? tl_fetch_int () : 0;
  e->port = (e->mode & TL_LIST_FLAG_PORT) ? tl_fetch_int () : 0;
  e->ua_hash = (e->mode & TL_LIST_FLAG_UA_HASH) ? tl_fetch_int () : 0;
  e->op = op;
  tl_fetch_end ();
  if (tl_fetch_error ()) {
    return 0;
  }
  return extra;
}

struct tl_act_extra *tl_list_entry_set_text (void) {
  struct tl_act_extra *extra = tl_act_extra_init (stats_buff, sizeof (struct tl_list_entry_set_text), tl_do_list_entry_set_text);
  struct tl_list_entry_set_text *e = (void *)extra->extra;
  CHECK_LIST_OBJECT_INTS;
  if (fetch_list_id (&e->list_id) < 0) {
    return 0;
  }
  if (fetch_object_id (&e->object_id) < 0) {
    return 0;
  }

  e->len = tl_fetch_string0 (e->text, max_text_len - 1);
  extra->size += e->len + 1;
  tl_fetch_end ();
  if (tl_fetch_error ()) {
    return 0;
  }
  return extra;
}

struct tl_act_extra *tl_list_entry_set_flags (int op) {
  struct tl_act_extra *extra = tl_act_extra_init (stats_buff, sizeof (struct tl_list_entry_set_flags), tl_do_list_entry_set_flags);
  struct tl_list_entry_set_flags *e = (void *)extra->extra;
  CHECK_LIST_OBJECT_INTS;
  if (fetch_list_id (&e->list_id) < 0) {
    return 0;
  }
  if (fetch_object_id (&e->object_id) < 0) {
    return 0;
  }
  
  e->or_mask = op != 3 ? tl_fetch_int () : 0;
  e->nand_mask = op == 0 ? -1 : op == 2 ? 0 : tl_fetch_int ();

  tl_fetch_end ();
  if (tl_fetch_error ()) {
    return 0;
  }
  return extra;
}

struct tl_act_extra *tl_list_entry_set_value (int op) {
  struct tl_act_extra *extra = tl_act_extra_init (stats_buff, sizeof (struct tl_list_entry_set_value), tl_do_list_entry_set_value);
  struct tl_list_entry_set_value *e = (void *)extra->extra;
  CHECK_LIST_OBJECT_INTS;
  if (fetch_list_id (&e->list_id) < 0) {
    return 0;
  }
  if (fetch_object_id (&e->object_id) < 0) {
    return 0;
  }
  
  e->flags = op != 0;
  e->value = op == 2 ? -tl_fetch_long () : tl_fetch_long ();

  tl_fetch_end ();
  if (tl_fetch_error ()) {
    return 0;
  }

  return extra;
}

struct tl_act_extra *tl_list_entry_incr_or_create (int decr) {
  struct tl_act_extra *extra = tl_act_extra_init (stats_buff, sizeof (struct tl_list_entry_incr_or_create), tl_do_list_entry_incr_or_create);
  struct tl_list_entry_incr_or_create *e = (void *)extra->extra;
  CHECK_LIST_OBJECT_INTS;
  if (fetch_list_id (&e->list_id) < 0) {
    return 0;
  }
  if (fetch_object_id (&e->object_id) < 0) {
    return 0;
  }
  
  e->flags = tl_fetch_int ();
  e->value = decr ? -tl_fetch_long () : tl_fetch_long ();

  tl_fetch_end ();
  if (tl_fetch_error ()) {
    return 0;
  }

  return extra;
}

struct tl_act_extra *tl_list_set_flags (int type) {
  struct tl_act_extra *extra = tl_act_extra_init (stats_buff, sizeof (struct tl_list_set_flags), tl_do_list_set_flags);
  struct tl_list_set_flags *e = (void *)extra->extra;
  CHECK_LIST_OBJECT_INTS;
  if (fetch_list_id (&e->list_id) < 0) {
    return 0;
  }

  e->xor_mask = tl_fetch_int ();
  e->and_mask = type == 2 ? tl_fetch_int () : 7;
  e->or_mask = tl_fetch_int ();
  e->nand_mask = type != 0 ? tl_fetch_int () : -1;

  tl_fetch_end ();
  if (tl_fetch_error ()) {
    return 0;
  }
  return extra;
}

struct tl_act_extra *tl_sublist_delete (int ex) {
  struct tl_act_extra *extra = tl_act_extra_init (stats_buff, sizeof (struct tl_sublist_delete), tl_do_sublist_delete);
  struct tl_sublist_delete *e = (void *)extra->extra;
  CHECK_LIST_OBJECT_INTS;
  if (fetch_list_id (&e->list_id) < 0) {
    return 0;
  }
  
  e->xor_mask = tl_fetch_int ();
  e->and_mask = ex ? tl_fetch_int () : 7;

  tl_fetch_end ();
  if (tl_fetch_error ()) {
    return 0;
  }

  return extra;
}

struct tl_act_extra *tl_list_entry_get (int (*act)(struct tl_act_extra *)) {
  struct tl_act_extra *extra = tl_act_extra_init (stats_buff, sizeof (struct tl_list_entry_get), act);
  struct tl_list_entry_get *e = (void *)extra->extra;
  CHECK_LIST_OBJECT_INTS;
  if (fetch_list_id (&e->list_id) < 0) {
    return 0;
  }
  if (fetch_object_id (&e->object_id) < 0) {
    return 0;
  }
  e->mode = tl_fetch_int ();
  tl_fetch_end ();
  if (tl_fetch_error ()) {
    return 0;
  }
  if (e->mode & 63) {
    tl_fetch_set_error_format (TL_ERROR_BAD_VALUE, "In get object flags 0..5 not supported");
    return 0;
  }
  if (e->mode & (TL_LIST_FLAG_IP | TL_LIST_FLAG_FRONT_IP | TL_LIST_FLAG_PORT | TL_LIST_FLAG_UA_HASH)) {
    tl_fetch_set_error_format (TL_ERROR_BAD_VALUE, "Get ip/front_ip/port/ua_hash not supported (yet?). (mode = 0x%08x)", e->mode);
    return 0;
  }
  return extra;
}

struct tl_act_extra *tl_list_entry_get_int (int is_long, int offset) {
  struct tl_act_extra *extra = tl_act_extra_init (stats_buff, sizeof (struct tl_list_entry_get_int), tl_do_list_entry_get_int);
  struct tl_list_entry_get_int *e = (void *)extra->extra;
  CHECK_LIST_OBJECT_INTS;
  if (fetch_list_id (&e->list_id) < 0) {
    return 0;
  }
  if (fetch_object_id (&e->object_id) < 0) {
    return 0;
  }
  e->is_long = is_long;
  e->offset = offset;
  tl_fetch_end ();
  if (tl_fetch_error ()) {
    return 0;
  }
  return extra;
}

struct tl_act_extra *tl_list_entry_get_text (void) {
  struct tl_act_extra *extra = tl_act_extra_init (stats_buff, sizeof (struct tl_list_entry_get_text), tl_do_list_entry_get_text);
  struct tl_list_entry_get_text *e = (void *)extra->extra;
  CHECK_LIST_OBJECT_INTS;
  if (fetch_list_id (&e->list_id) < 0) {
    return 0;
  }
  if (fetch_object_id (&e->object_id) < 0) {
    return 0;
  }
  tl_fetch_end ();
  if (tl_fetch_error ()) {
    return 0;
  }
  return extra;
}

struct tl_act_extra *tl_list_entry_get_pos (void) {
  struct tl_act_extra *extra = tl_act_extra_init (stats_buff, sizeof (struct tl_list_entry_get_pos), tl_do_list_entry_get_pos);
  struct tl_list_entry_get_pos *e = (void *)extra->extra;
  CHECK_LIST_OBJECT_INTS;
  if (fetch_list_id (&e->list_id) < 0) {
    return 0;
  }
  if (fetch_object_id (&e->object_id) < 0) {
    return 0;
  }
  tl_fetch_end ();
  if (tl_fetch_error ()) {
    return 0;
  }
  return extra;
}

struct tl_act_extra *tl_list_get (int full, int limit) {
  struct tl_act_extra *extra = tl_act_extra_init (stats_buff, sizeof (struct tl_list_get), tl_do_list_get);
  struct tl_list_get *e = (void *)extra->extra;
  CHECK_LIST_OBJECT_INTS;
  if (fetch_list_id (&e->list_id) < 0) {
    return 0;
  }
  e->mode = tl_fetch_int ();
  if (!full && (e->mode & ~(63 | (1 << 15)))) {
    tl_fetch_set_error_format (TL_ERROR_BAD_VALUE, "in non-full get list only modes 0..63 and bit 15 are supported");
    return 0;
  }
  if (!full) {
    e->mode |= (1 << 15);
  }
  if (limit) {
    e->limit = tl_fetch_int ();
    e->offset = tl_fetch_int ();
  } else {
    e->limit = -1;
    e->offset = 0;
  }
  tl_fetch_end ();
  if (tl_fetch_error ()) {
    return 0;
  }
  return extra;
}

struct tl_act_extra *tl_list_count (int sublist) {
  struct tl_act_extra *extra = tl_act_extra_init (stats_buff, sizeof (struct tl_list_count), sublist == 2 ? tl_do_sublists_count : tl_do_list_count);
  struct tl_list_count *e = (void *)extra->extra;

  CHECK_LIST_OBJECT_INTS;
  if (fetch_list_id (&e->list_id) < 0) {
    return 0;
  }

  if (sublist == 1) {
    e->cnt = tl_fetch_int ();
  } else {
    e->cnt = -1;
  }

  tl_fetch_end ();
  if (tl_fetch_error ()) {
    return 0;
  }
  return extra;
}

struct tl_act_extra *tl_list_intersect (int is_intersect, int is_wild, int is_limit, int is_full) {
  struct tl_act_extra *extra = tl_act_extra_init (stats_buff, sizeof (struct tl_list_intersect), is_full ? tl_do_list_intersect_full : tl_do_list_intersect);
  struct tl_list_intersect *e = (void *)extra->extra;

  CHECK_LIST_OBJECT_INTS;
  if (fetch_list_id (&e->list_id) < 0) {
    return 0;
  }

  e->id_ints = is_wild ? tl_fetch_int () : object_id_ints;
  e->mode = tl_fetch_int ();
  e->count = tl_fetch_int ();
  e->is_intersect = is_intersect;

  int bytes = 4 * (e->id_ints);
  if (bytes * e->count > (1 << 20) || e->count < 0) {
    tl_fetch_set_error_format (TL_ERROR_VALUE_NOT_IN_RANGE, "Too long query. Total %d bytes", bytes * e->count);
    return 0;
  }
  if (tl_fetch_string_data ((char *)e->arr, bytes * e->count) < 0) {
    return 0;
  }
  extra->size += bytes * (e->count + 1);
  
  e->limit = is_limit ? tl_fetch_int () : -1;

  tl_fetch_end ();
  if (tl_fetch_error ()) {
    return 0;
  }
  return extra;  
}

struct tl_act_extra *tl_list_sum (int is_wild, int has_weights) {
  struct tl_act_extra *extra = tl_act_extra_init (stats_buff, sizeof (struct tl_list_sum), tl_do_list_sum);
  struct tl_list_sum *e = (void *)extra->extra;

  CHECK_LIST_OBJECT_INTS;
  if (fetch_list_id (&e->list_id) < 0) {
    return 0;
  }

  e->id_ints = is_wild ? tl_fetch_int () : object_id_ints;
  e->mode = tl_fetch_int ();
  e->count = tl_fetch_int ();
  e->has_weights = has_weights;

  int bytes = 4 * (e->id_ints + (has_weights != 0));
  if (bytes * e->count > (1 << 20) || e->count < 0) {
    tl_fetch_set_error_format (TL_ERROR_VALUE_NOT_IN_RANGE, "Too long query. Total %d bytes", bytes * e->count);
    return 0;
  }
  if (tl_fetch_string_data ((char *)e->arr, bytes * e->count) < 0) {
    return 0;
  }
  extra->size += bytes * (e->count + 1);

  tl_fetch_end ();
  if (tl_fetch_error ()) {
    return 0;
  }
  return extra;  
}

struct tl_act_extra *tl_list_sorted (int is_full, int has_limit) {
  struct tl_act_extra *extra = tl_act_extra_init (stats_buff, sizeof (struct tl_list_sorted), is_full ? tl_do_list_sorted_full : tl_do_list_sorted);
  struct tl_list_sorted *e = (void *)extra->extra;
  
  CHECK_LIST_OBJECT_INTS;
  if (fetch_list_id (&e->list_id) < 0) {
    return 0;
  }
  
  e->xor_mask = tl_fetch_int ();
  e->and_mask = tl_fetch_int ();
  e->mode = tl_fetch_int ();
  e->limit = has_limit ? tl_fetch_int () : MAX_RES;
  
  tl_fetch_end ();
  if (tl_fetch_error ()) {
    return 0;
  }
  return extra;
}

struct tl_act_extra *tl_datedistr (void) {
  struct tl_act_extra *extra = tl_act_extra_init (stats_buff, sizeof (struct tl_datedistr), tl_do_datedistr);
  struct tl_datedistr *e = (void *)extra->extra;

  CHECK_LIST_OBJECT_INTS;
  if (fetch_list_id (&e->list_id) < 0) {
    return 0;
  }
  e->mode = tl_fetch_int ();
  e->min_date = tl_fetch_int ();
  e->max_date = tl_fetch_int ();
  e->step = tl_fetch_int ();
  
  tl_fetch_end ();
  if (tl_fetch_error ()) {
    return 0;
  }
  return extra;
}

struct tl_act_extra *lists_parse_function (long long actor_id) {
  if (actor_id != 0) {
    tl_fetch_set_error ("Lists only support actor_id = 0", TL_ERROR_WRONG_ACTOR_ID);
    return 0;
  }
  int op = tl_fetch_int ();
  if (tl_fetch_error ()) {
    return 0;
  }
  switch (op) {
  case TL_LISTS_DELETE_LIST:
    return tl_list_delete ();
  case TL_LISTS_DELETE_ENTRY:
    return tl_list_entry_delete ();
  case TL_LISTS_DELETE_OBJECT:
    return tl_object_delete ();
  case TL_LISTS_SET_ENTRY:
    return tl_list_entry_set (0);
  case TL_LISTS_ADD_ENTRY:
    return tl_list_entry_set (2);
  case TL_LISTS_REPLACE_ENTRY:
    return tl_list_entry_set (1);
  case TL_LISTS_SET_ENTRY_TEXT:
    return tl_list_entry_set_text ();
  case TL_LISTS_SET_FLAGS:
    return tl_list_entry_set_flags (0);
  case TL_LISTS_CHANGE_FLAGS:
    return tl_list_entry_set_flags (1);
  case TL_LISTS_INCR_FLAGS:
    return tl_list_entry_set_flags (2);
  case TL_LISTS_DECR_FLAGS:
    return tl_list_entry_set_flags (3);
  case TL_LISTS_SET_VALUE:
    return tl_list_entry_set_value (0);
  case TL_LISTS_INCR_VALUE:
    return tl_list_entry_set_value (1);
  case TL_LISTS_DECR_VALUE:
    return tl_list_entry_set_value (2);
  case TL_LISTS_INCR_VALUE_OR_CREATE:
    return tl_list_entry_incr_or_create (0);
  case TL_LISTS_DECR_VALUE_OR_CREATE:
    return tl_list_entry_incr_or_create (1);
  case TL_LISTS_SET_LIST_FLAGS:
    return tl_list_set_flags (0);
  case TL_LISTS_CHANGE_LIST_FLAGS:
    return tl_list_set_flags (1);
  case TL_LISTS_CHANGE_LIST_FLAGS_EX:
    return tl_list_set_flags (2);
  case TL_LISTS_DELETE_SUBLIST:
    return tl_sublist_delete (0);
  case TL_LISTS_DELETE_SUBLIST_EX:
    return tl_sublist_delete (1);
  case TL_LISTS_GET_ENTRY:
    return tl_list_entry_get (tl_do_list_entry_get);
  case TL_LISTS_GET_ENTRY_FLAGS:
    return tl_list_entry_get_int (0, 0);
  case TL_LISTS_GET_ENTRY_DATE:
    return tl_list_entry_get_int (0, 1);
  case TL_LISTS_GET_ENTRY_GLOBAL_ID:
    return tl_list_entry_get_int (1, 2);
  case TL_LISTS_GET_ENTRY_VALUE:
    return tl_list_entry_get_int (1, 4);
  case TL_LISTS_GET_ENTRY_TEXT:
    return tl_list_entry_get_text ();
  case TL_LISTS_GET_ENTRY_POS:
    return tl_list_entry_get_pos ();
  case TL_LISTS_GET_LIST:
    return tl_list_get (0, 0);
  case TL_LISTS_GET_LIST_LIMIT:
    return tl_list_get (0, 1);
  case TL_LISTS_GET_LIST_FULL:
    return tl_list_get (1, 0);
  case TL_LISTS_GET_LIST_FULL_LIMIT:
    return tl_list_get (1, 1);
  case TL_LISTS_GET_LIST_COUNT:
    return tl_list_count (0);
  case TL_LISTS_GET_SUBLIST_COUNT:
    return tl_list_count (1);
  case TL_LISTS_GET_SUBLISTS_COUNT:
    return tl_list_count (2);
  case TL_LISTS_INTERSECT:
    return tl_list_intersect (1, 0, 0, 0);
  case TL_LISTS_INTERSECT_LIMIT:
    return tl_list_intersect (1, 0, 1, 0);
  case TL_LISTS_INTERSECT_FULL:
    return tl_list_intersect (1, 0, 0, 1);
  case TL_LISTS_INTERSECT_FULL_LIMIT:
    return tl_list_intersect (1, 0, 1, 1);
  case TL_LISTS_INTERSECT_WILD:
    return tl_list_intersect (1, 1, 0, 0);
  case TL_LISTS_INTERSECT_WILD_LIMIT:
    return tl_list_intersect (1, 1, 1, 0);
  case TL_LISTS_INTERSECT_WILD_FULL:
    return tl_list_intersect (1, 1, 0, 1);
  case TL_LISTS_INTERSECT_WILD_FULL_LIMIT:
    return tl_list_intersect (1, 1, 1, 1);
  case TL_LISTS_SUBTRACT:
    return tl_list_intersect (0, 0, 0, 0);
  case TL_LISTS_SUBTRACT_LIMIT:
    return tl_list_intersect (0, 0, 1, 0);
  case TL_LISTS_SUMLIST:
    return tl_list_sum (0, 0);
  case TL_LISTS_SUMLIST_WILD:
    return tl_list_sum (1, 0);
  case TL_LISTS_SUMLIST_WEIGHTED:
    return tl_list_sum (0, 1);
  case TL_LISTS_SUMLIST_WEIGHTED_WILD:
    return tl_list_sum (1, 1);
  case TL_LISTS_SORTED_LIST:
    return tl_list_sorted (0, 0);
  case TL_LISTS_SORTED_LIST_LIMIT:
    return tl_list_sorted (0, 1);
  case TL_LISTS_SORTED_LIST_FULL:
    return tl_list_sorted (1, 0);
  case TL_LISTS_SORTED_LIST_FULL_LIMIT:
    return tl_list_sorted (1, 1);
  case TL_LISTS_DATEDISTR:
    return tl_datedistr ();
  default:
    tl_fetch_set_error_format (TL_ERROR_UNKNOWN_FUNCTION_ID, "Unknown op %08x", op);
    return 0;
  }
}

void reopen_logs(void) {
  int fd;
  fflush(stdout);
  fflush(stderr);
  if ((fd = open("/dev/null", O_RDWR, 0)) != -1) {
    dup2(fd, 0);
    dup2(fd, 1);
    dup2(fd, 2);
    if (fd > 2)
      close(fd);
  }
  if (logname && (fd = open(logname, O_WRONLY|O_APPEND|O_CREAT, 0640)) != -1) {
    dup2(fd, 1);
    dup2(fd, 2);
    if (fd > 2)
      close(fd);
  }
  if (verbosity > 0) {
    fprintf (stderr, "logs reopened.\n");
  }
}

static void sigint_immediate_handler (const int sig) {
  fprintf (stderr, "SIGINT handled immediately.\n");
  //  flush_binlog_last();
  //  sync_binlog (2);
  exit (1);
}

static void sigterm_immediate_handler (const int sig) {
  fprintf (stderr, "SIGTERM handled immediately.\n");
  //  flush_binlog_last();
  //  sync_binlog (2);
  exit (1);
}

static void sigint_handler (const int sig) {
  fprintf (stderr, "SIGINT handled.\n");
  pending_signals |= (1 << SIGINT);
  signal(SIGINT, sigint_immediate_handler);
}

static void sigterm_handler (const int sig) {
  fprintf (stderr, "SIGTERM handled.\n");
  pending_signals |= (1 << SIGTERM);
  signal(SIGTERM, sigterm_immediate_handler);
}

static void sighup_handler (const int sig) {
  fprintf (stderr, "got SIGHUP.\n");
  // sync_binlog (2);
  signal(SIGHUP, sighup_handler);
}

static void sigusr1_handler (const int sig) {
  fprintf (stderr, "got SIGUSR1, rotate logs.\n");
  reopen_logs();
  // sync_binlog (2);
  signal(SIGUSR1, sigusr1_handler);
}

volatile int sigpoll_cnt;
static void sigpoll_handler (const int sig) {
  sigpoll_cnt++;
  signal(SIGPOLL, sigpoll_handler);
}

static void sigrtmax_handler (const int sig) {
  fprintf(stderr, "got SIGUSR3, write index.\n");
  force_write_index = 1;
}


void check_child_status (void) {
  if (!child_pid) {
    return;
  }
  int status = 0;
  int res = waitpid (child_pid, &status, WNOHANG);
  if (res == child_pid) {
    if (WIFEXITED (status) || WIFSIGNALED (status)) {
      if (verbosity > 0) {
        fprintf (stderr, "child process %d terminated: exited = %d, signaled = %d, exit code = %d\n",
          child_pid, WIFEXITED (status) ? 1 : 0, WIFSIGNALED (status) ? 1 : 0, WEXITSTATUS (status));
      }
      child_pid = 0;
    }
  } else if (res == -1) {
    if (errno != EINTR) {
      fprintf (stderr, "waitpid (%d): %m\n", child_pid);
      child_pid = 0;
    }
  } else if (res) {
    fprintf (stderr, "waitpid (%d) returned %d???\n", child_pid, res);
  }
}

int sfd;

void fork_write_index (void) {
  if (child_pid) {
    if (verbosity > 0) {
      fprintf (stderr, "process with pid %d already generates index, skipping\n", child_pid);
    }
    return;
  }

  flush_binlog_ts ();

  int res = fork ();

  if (res < 0) {
    fprintf (stderr, "fork: %m\n");
  } else if (!res) {
    binlogname = 0;
    close (sfd);
    res = write_index (!binlog_disabled);
    exit (res);
  } else {
    if (verbosity > 0) {
      fprintf (stderr, "created child process pid = %d\n", res);
    }
    child_pid = res;
  }

  force_write_index = 0;
}


void cron (void) {
  flush_binlog();
  if (force_write_index) {
    fork_write_index ();
  }
  check_child_status();
}



void start_server (void) {
  char buf[64];
  int i;
  int prev_time;

  init_epoll();
  init_netbuffers();
  if (udp_enabled) {
    init_msg_buffers (0);
  }

  prev_time = 0;

  if (!sfd) {
    sfd = server_socket (port, settings_addr, backlog, 0);
  }

  if (sfd < 0) {
    fprintf(stderr, "cannot open server socket at port %d: %m\n", port);
    exit(3);
  }

  if (verbosity) {
    fprintf (stderr, "created listening socket at %s:%d, fd=%d\n", conv_addr(settings_addr.s_addr, buf), port, sfd);
  }

  if (daemonize) {
    setsid();
  }

  if (change_user(username) < 0) {
    fprintf (stderr, "fatal: cannot change user to %s\n", username ? username : "(none)");
    exit(1);
  }

  if (binlogname && !binlog_disabled) {
    assert (append_to_binlog (Binlog) == log_readto_pos);
  }

//  init_listening_connection (sfd, &ct_lists_engine_server, &memcache_methods);
  init_listening_connection (sfd, &ct_rpc_server, &rpc_methods);
  if (udp_enabled) {
    add_udp_socket (port, 0);
  }

  if (binlog_disabled && binlog_fd >= 0) {
    epoll_pre_event = read_new_events;
  }

  signal(SIGINT, sigint_handler);
  signal(SIGTERM, sigterm_handler);
  signal(SIGUSR1, sigusr1_handler);
  signal(SIGRTMAX, sigrtmax_handler);
  signal(SIGPIPE, SIG_IGN);
  //signal (SIGIO, SIG_IGN);
  signal (SIGPOLL, sigpoll_handler);

  if (daemonize) {
    signal(SIGHUP, sighup_handler);
    reopen_logs();
  }

  for (i = 0; ; i++) {
    if (verbosity > 0 && !(i & 255)) {
      fprintf (stderr, "epoll_work(): %d out of %d connections, network buffers: %d used, %d out of %d allocated\n",
         active_connections, maxconn, NB_used, NB_alloc, NB_max);
    }

    fetch_binlog_data (binlog_disabled ? log_cur_pos() : log_write_pos ());
    epoll_work (20);
    tl_restart_all_ready ();

    if (now != prev_time) {
      prev_time = now;
      cron();
    }

    flush_binlog ();
    cstatus_binlog_pos (binlog_disabled ? log_cur_pos() : log_write_pos (), binlog_disabled);

    if (epoll_pre_event) {
      epoll_pre_event();
    }

    check_all_aio_completions ();
    
    if (pending_signals) {
      break;
    }

    if (quit_steps && !--quit_steps) break;
  }

  if (verbosity > 0) {
    fprintf (stderr, "quitting because of pending signals = %llx\n", pending_signals);
  }

  epoll_close (sfd);
  close(sfd);

  flush_binlog_last ();

  sync_binlog (2);
}

/*
 *
 *    MAIN
 *
 */

void usage (void) {
  printf ("usage: %s [-v] [-r] [-p<port>] [-u<username>] [-b<backlog>] [-c<max-conn>] [-a<binlog-name>] [-l<log-name>] [-M] [-m<memory-for-metafiles>] [-I<ignore>] <huge-index-file>\n"
      "\t" VERSION_STR " compiled at " __DATE__ " " __TIME__ " by gcc " __VERSION__ " "
#ifdef __LP64__
    "64-bit"
#else
    "32-bit"
#endif
    " after commit " COMMIT "\n"
    "\tPerforms lists storage and retrieval queries using given indexes\n"
    ,
    progname);
  parse_usage ();
  exit(2);
}

extern int aio_errors_verbosity;
static int dump_metafile_sizes = 0;

int f_parse_option (int val) {
  long long x;
  char c;
  switch (val) {
  case 'A':
    aio_errors_verbosity ++;
    break;
  case 'i':
    ignored_list2 = atoi(optarg);
    break;
  case 'n':
    negative_list_id_offset = atoi(optarg);
    if (negative_list_id_offset < 0) {
      negative_list_id_offset = -negative_list_id_offset;
    }
    break;
  case 'T':
    ++test_mode;
    break;
  case 'w':
  case 1000:
    newidxname = optarg;
    index_mode = 1;
    break;
  case 'N':
    disable_revlist ++;
    break;
  case 'R':
    binlog_repairing = atoi (optarg);
    break;
  case 'L':
    debug_list_id = atoi(optarg);
    break;
  case 'W':
    assert (sscanf(optarg, "%d,%d", &w_split_rem, &w_split_mod) == 2);
    assert (w_split_mod > 0 && w_split_mod <= 10000 && w_split_rem >= 0 && w_split_rem < w_split_mod);
    do_export = 1;
    break;
  case 'S':
    dump_sums = 1;
    break;
  case 'f':
    memcache_auto_answer_mode ++;
    break;
  case 'H': case 'O':
    c = 0;
    assert (sscanf (optarg, "%lld%c", &x, &c) >= 1);
    switch (c | 0x20) {
      case 'k':  x <<= 10; break;
      case 'm':  x <<= 20; break;
      case 'g':  x <<= 30; break;
      case 't':  x <<= 40; break;
      default: assert (c == 0x20);
    }
    if (val == 'B' && x >= 1024 && x < (1LL << 60)) {
      max_binlog_size = x;
    } else if (val == 'H' && x >= (1LL << 20) && x <= (sizeof(long) == 4 ? (3LL << 30) : (20LL << 30))) {
      dynamic_data_buffer_size = x;
    } else if (val == 'O' && x >= (1LL << 20) && x <= (sizeof (long) == 4 ? (2LL << 30) : (20LL << 30))) {
      idx_min_free_heap = x;
    }
    break;
  case 'E':
    export_flags = atoi (optarg);
    do_export = 1;
    break;
  case 'm':
    memory_for_metafiles = 1024 * 1024 * (long long) atoi (optarg);
    break;
  case 'Y':
    dump_metafile_sizes ++;
    break;
  case 'F':
    return_false_if_not_found ++;
    break;
  case 'y':
    max_text_len = atoi (optarg);
    if (max_text_len < 0) { max_text_len = 0; }
    if (max_text_len > (1 << 17)) { max_text_len = (1 << 17); }
    break;
  case 'I':
    ignore_string = optarg;
    ignore_mode = 1;
    break;
  case 'M':
    metafile_mode ++;
    break;
  case 'U':
    metafiles_order = atoi(optarg);
    assert(metafiles_order == 0 || metafiles_order == 1);
    break;
  default:
    return -1;
  }
  return 0;
}

int main (int argc, char *argv[]) {
  int i;
  //long long x;
  //char c;

  set_debug_handlers ();
  signal (SIGRTMAX, sigrtmax_handler);

  progname = argv[0];
  i = strlen (progname);

  if (i >= 5 && !strcmp (progname + i - 5, "index")) {
    index_mode = 1;
  }

  parse_option ("aio-errors-verbosity", no_argument, 0, 'A', "increases aio verbosity");
  parse_option ("ignored-list", required_argument, 0, 'i', "ignores specified list_id");
  parse_option ("negative-list-id-offset", required_argument, 0, 'n', "sets negative list_id offset");
  parse_option ("test-mode", no_argument, 0, 'T', "works in test mode (what's this?)");
  parse_option ("index", no_argument, 0, 1000, "reindex");
  parse_option (0, required_argument, 0, 'w', "reindex");
  parse_option ("disable-revlist", no_argument, 0, 'N', "disable revlists");
  parse_option ("repair-binlog", required_argument, 0, 'R', "try to repair binlog");
  parse_option ("debug-list", required_argument, 0, 'L', "debugs only specified list");
  parse_option ("remove-dates", no_argument, 0, 'D', "removes all dates");
  parse_option ("export-list-id", required_argument, 0, 'W', "Argument in format \"rem,mod\". Exports lists with specified rem mod mod ");
  parse_option ("dump-list-sizes", no_argument, 0, 'S', "Dumps list sizes");
  parse_option (0, no_argument, 0, 'f', "Send serialized false on non-answered memcache queries");
  parse_option (0, required_argument, 0, 'H', "zmalloc heap size");
  parse_option ("low-mem-reindex", required_argument, 0, 'O', "Free memory size for starting reindex");
  parse_option ("export-flags", required_argument, 0, 'E', "Exports list with specified flags");
  parse_option ("metafiles-memory", required_argument, 0, 'm', "Memory for metafiles (in MiB)");
  parse_option ("dump-metafiles-sizes", no_argument, 0, 'Y', "Dumps metafiles sizes");
  parse_option ("return-false", no_argument, 0, 'F', "Returns false if not found");
  parse_option ("max-text-len", required_argument, 0, 'y', "Sets max text len (default %d)", max_text_len);
  parse_option ("ignore-actions", required_argument, 0, 'I', "Argument in format timestamp#list1_object1,list2_object2,... Ignores operations with these objects since timestamp");
  parse_option ("metafiles", no_argument, 0, 'M', "Increases metafile mode:\n mode = 1 - default metafile mode \n mode = 2 - metafiles with changes never unload");
  parse_option ("metafiles-order", required_argument, 0, 'U', "Order of metafiles in snapshot (default %d):\n order = 0 - by list Id \n order = 1 - by last creation of entry in list", metafiles_order);


  parse_engine_options_long (argc, argv, f_parse_option);
  if (argc != optind + 1 && argc != optind + 2) {
    usage();
    return 2;
  }

  signal (SIGUSR1, sigusr1_handler);
  signal (SIGRTMAX, sigrtmax_handler);

  assert (!(metafile_mode & 1) || !(metafile_mode & 2));
  if (!w_split_mod && !index_mode && raise_file_rlimit(maxconn + 16) < 0 && !test_mode) {
    fprintf (stderr, "fatal: cannot raise open file limit to %d\n", maxconn+16);
    exit(1);
  }

  if (!w_split_mod && !index_mode && port < PRIVILEGED_TCP_PORTS) {
    sfd = server_socket (port, settings_addr, backlog, 0);
    if (sfd < 0) {
      fprintf(stderr, "cannot open server socket at port %d: %m\n", port);
      exit(1);
    }
  }

  aes_load_pwd_file (aes_pwd_file);

  if (change_user (username) < 0) {
    fprintf (stderr, "fatal: cannot change user to %s\n", username ? username : "(none)");
    exit(1);
  }

  init_dyn_data ();
  if (udp_enabled) {
    init_server_PID (get_my_ipv4 (), port);
  }
  log_ts_interval = 5;

  if (engine_preload_filelist (argv[optind], binlogname) < 0) {
    fprintf (stderr, "cannot open binlog files for %s\n", binlogname ? binlogname : argv[optind]);
    exit (1);
  }

  Snapshot = open_recent_snapshot (engine_snapshot_replica);

  if (Snapshot) {
    engine_snapshot_name = Snapshot->info->filename;
    engine_snapshot_size = Snapshot->info->file_size;

    if (verbosity) {
      fprintf (stderr, "load index file %s (size %lld)\n", engine_snapshot_name, engine_snapshot_size);
    }
    index_load_time = get_utime(CLOCK_MONOTONIC);

    i = load_index ();

    index_load_time = get_utime(CLOCK_MONOTONIC) - index_load_time;

    if (i < 0) {
      fprintf (stderr, "fatal: error %d while loading index file %s\n", i, engine_snapshot_name);
      exit(1);
    }

    if (verbosity) {
      fprintf (stderr, "load index: done, jump_log_pos=%lld, jump_log_crc32 = %u, jump_log_ts = %d, alloc_mem=%ld out of %ld, time %.06lfs\n",
         jump_log_pos, jump_log_crc32, jump_log_ts, (long) (dyn_cur - dyn_first), (long) (dyn_last - dyn_first), index_load_time);
    }

    if (dump_metafile_sizes) {
      dump_msizes ();
    }

  } else {
    init_hash_table (0);
  }

  if (ignore_mode) {
    char *s = ignore_string;
    ignore_timestamp = strtol (s, &s, 10);
    if (*s != '#') {
      fprintf (stderr, "Ignore string: '#' expected\n");
      exit (2);
    }
    s ++;
    while (*s) {
      static input_list_id_t list_id;
      static input_object_id_t object_id;

      clear_input_list_id (&list_id);
      clear_input_object_id (&object_id);
      
      int x = -1;
      if (sscanf (s, scanf_dstr "_" scanf_dstr "%n", scanf_dptr (&list_id), scanf_dptr (&object_id), &x) >= 2 * SCANF_DINTS /* && list_id && object_id */) {
        if (x > 0 && convert_input_list_id (s, list_id) && convert_input_object_id (s, object_id)) {
          ignore_list_object_add (list_id, object_id);
        } else {
          fprintf (stderr, "Could not parse list_object\n");
          exit (2);
        }
      }

      s += x;

      if (*s && *s != ',') {
        fprintf (stderr, "Comma expected\n");
        exit (2);
      }
      if (*s) { s ++; }
    }
  }

  Binlog = open_binlog (engine_replica, jump_log_pos);
  if (!Binlog) {
    fprintf (stderr, "fatal: cannot find binlog for %s, log position %lld\n", engine_replica->replica_prefix, jump_log_pos);
    exit (1);
  }

  binlogname = Binlog->info->filename;

  init_common_data (0, index_mode ? CD_INDEXER : CD_ENGINE);
  cstatus_binlog_name (engine_replica->replica_prefix);

  if (verbosity) {
    fprintf (stderr, "replaying binlog file %s (size %lld)\n", binlogname, Binlog->info->file_size);
  }
  binlog_load_time = get_utime(CLOCK_MONOTONIC);

  clear_log();

/*  set_log_data(fd[2], fsize[2]); */
  init_log_data (jump_log_pos, jump_log_ts, jump_log_crc32);

  i = replay_log (0, 1);

  binlog_load_time = get_utime(CLOCK_MONOTONIC) - binlog_load_time;
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

  if (verbosity) {
    fprintf (stderr, "replay binlog file: done, log_pos=%lld, now=%d, alloc_mem=%ld out of %ld, time %.06lfs\n",
             (long long) log_pos, now, (long) (dyn_cur - dyn_first), (long) (dyn_last - dyn_first), binlog_load_time);
  }
  
  ignore_mode = 0;

  clear_write_log();
  start_time = time(0);

  if (ignored_list2 == -1) {
    ignored_list2 = 0;
  }

  if (index_mode) {
    /* create_new_snapshot (Binlog, log_readto_pos); */
    return write_index (0);
  }

  if (do_export) {
    dump_all_lists (export_flags, w_split_rem, w_split_mod);
    return 0;
  }

  if (dump_sums) {
    long long total = dump_all_value_sums ();
    printf ("TOTAL\t%lld\n", total);
    return 0;
  }

  tl_parse_function = lists_parse_function;
  tl_aio_timeout = 2.0;
  start_server();

  return 0;
}
