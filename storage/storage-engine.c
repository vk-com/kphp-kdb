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
#include <stdio.h>
#include <stdlib.h>
#include <stddef.h>
#include <stdarg.h>
#include <string.h>
#include <ctype.h>
#include <unistd.h>
#include <fcntl.h>
#include <aio.h>
#include <dirent.h>
#include <pthread.h>
#include <sys/types.h>
#include <sys/wait.h>

#include "kfs.h"
#include "kdb-data-common.h"
#include "server-functions.h"
#include "net-aio.h"
#include "net-connections.h"
#include "net-http-server.h"
#include "net-memcache-server.h"
#include "net-rpc-server.h"
#include "net-rpc-common.h"
#include "net-crypto-aes.h"
#include "storage-data.h"
#include "kdb-storage-binlog.h"
#include "base64.h"
#include "storage-rpc.h"
#include "net-rpc-client.h"
#include "am-stats.h"

#ifndef COMMIT
#define COMMIT "unknown"
#endif

#define	VERSION_STR	"storage-engine-0.13-r36"
const char FullVersionStr[] = VERSION_STR " compiled at " __DATE__ " " __TIME__ " by gcc " __VERSION__ " "
#ifdef __LP64__
  "64-bit"
#else
  "32-bit"
#endif
" after commit " COMMIT;

#define TCP_PORT 11211

#define mytime() (get_utime(CLOCK_MONOTONIC))


/*
 *
 *		STORAGE ENGINE
 *
 */

static struct in_addr settings_addr;
static int NGINX_PORT = 80;
static int md5_mode = 0;
static int max_immediately_reply_filesize = 0;
static int required_volumes_at_startup = 1000;
static char *groupname;
static char *choose_binlog_options = "sah";

/* stats counters */
static long long get_misses, cmd_version, cmd_stats;
static long long metafiles_cache_hits, x_accel_redirects, tot_aio_fsync_queries, redirect_retries;
static long long get_queries, get_file_queries, get_hide_queries, get_volume_misses;
static long long one_pix_transparent_errors, too_many_aio_connections_errors;

static double binlog_load_time = 0.0, index_load_time = 0.0, aio_query_timeout_value = 2.9;
static double booting_time = 0.0, scandir_time = 0.0, reoder_binlog_files_time = 0.0, append_to_binlog_time = 0.0, binlog_index_loading_time = 0.0, open_replicas_time = 0.0;
static int index_mode = 0;
static int attachment = 0;
static long long index_volume_id = 0;
static int cs_id = 0;
static int fsync_step_delay = 5;
static int max_zmalloc_bytes = 32 << 20;
static double connect_timeout = 3.0;

volatile int force_interrupt;
volatile int force_write_index;

#define STATS_BUFF_SIZE	(16 << 10)
#define VALUE_BUFF_SIZE	(1 << 16)
static char stats_buff[STATS_BUFF_SIZE];
static char value_buff[VALUE_BUFF_SIZE];

static int storage_engine_wakeup (struct connection *c);
static int storage_engine_alarm (struct connection *c);

conn_type_t ct_storage_engine_server = {
  .magic = CONN_FUNC_MAGIC,
  .title = "storage_engine_server",
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
  .wakeup = storage_engine_wakeup,
  .alarm = storage_engine_alarm,
#ifndef NOAES
  .crypto_init = aes_crypto_init,
  .crypto_free = aes_crypto_free,
  .crypto_encrypt_output = aes_crypto_encrypt_output,
  .crypto_decrypt_input = aes_crypto_decrypt_input,
  .crypto_needed_output_bytes = aes_crypto_needed_output_bytes,
#endif
};

int memcache_get_start (struct connection *c);
int memcache_get_end (struct connection *c, int key_count);

int memcache_store (struct connection *c, int op, const char *key, int key_len, int flags, int delay, int size);
int memcache_get (struct connection *c, const char *key, int key_len);
int memcache_stats (struct connection *c);
int rpcs_execute (struct connection *c, int op, int len);

struct memcache_server_functions storage_memcache_inbound = {
  .execute = mcs_execute,
  .mc_store = memcache_store,
  .mc_get_start = memcache_get_start,
  .mc_get = memcache_get,
  .mc_get_end = memcache_get_end,
  .mc_incr = mcs_incr,
  .mc_delete = mcs_delete,
  .mc_version = mcs_version,
  .mc_stats = memcache_stats,
  .mc_check_perm = mcs_default_check_perm,
  .mc_init_crypto = mcs_init_crypto
};

static int storage_rpc_wakeup (struct connection *c);

struct rpc_server_functions storage_rpc_server = {
  .execute = rpcs_execute,
  .check_ready = server_check_ready,
  .flush_packet = rpcs_flush_packet,
  .rpc_check_perm = rpcs_default_check_perm,
  .rpc_init_crypto = rpcs_init_crypto,
  .rpc_wakeup = storage_rpc_wakeup,
  .rpc_alarm = storage_rpc_wakeup,
  .memcache_fallback_type = &ct_storage_engine_server,
  .memcache_fallback_extra = &storage_memcache_inbound
};

int rpcc_execute (struct connection *c, int op, int len);
int rpcc_ready (struct connection *c);

struct rpc_client_functions storage_rpc_client = {
  .execute = rpcc_execute,
  .check_ready = server_check_ready,
  .flush_packet = rpcc_flush_packet,
  .rpc_check_perm = rpcc_default_check_perm,
  .rpc_init_crypto = rpcc_init_crypto,
  .rpc_start_crypto = rpcc_start_crypto,
  .rpc_ready = rpcc_ready
};

struct conn_target default_ct = {
  .min_connections = 1,
  .max_connections = 1,
  .type = &ct_rpc_client,
  .extra = (void *)&storage_rpc_client,
  .reconnect_timeout = 17
};

int storage_prepare_stats (struct connection *c);

static inline void init_tmp_buffers (struct connection *c) {
  free_tmp_buffers (c);
  c->Tmp = alloc_head_buffer ();
  assert (c->Tmp);
}



/************************************ Gather ****************************************/
int delete_write_thread_query (struct conn_query *q);
int write_thread_query_timeout (struct conn_query *q);

conn_query_type_t write_thread_query_type = {
  .magic = CQUERY_FUNC_MAGIC,
  .title = "write_thread-query",
  .parse_execute = server_failed,
  .close = delete_write_thread_query,
  .wakeup = write_thread_query_timeout
};

int active_gathers;
long long gather_timeouts;

#define GATHER_QUERY(q) ((struct gather_data *)(q)->extra)
typedef struct {
  unsigned char md5[16];
  unsigned long long secret;
  int exit_code;
  int terminated;
} write_thread_out_t;

typedef struct gather_data {
  long long req_id;
  struct connection *c;
  struct gather_data *next, *prev;
  pthread_t writing_thread;
  struct conn_query *q;
  volume_t *V;
  char *key;
  char *filename;
  unsigned char *filedata;
  int key_len;
  int filedata_len;
  int content_type;
  write_thread_out_t Out;
} write_thread_arg_t;

write_thread_arg_t *active_write_threads;

void free_gather (struct gather_data *G) {
  if (active_write_threads == G) {
    active_write_threads = G->next;
  }
  if (G->prev) {
    G->prev->next = G->next;
  }
  if (G->next) {
    G->next->prev = G->prev;
  }

  if (G->key != NULL) {
    tszfree (G->key, G->key_len + 1);
  }
  if (G->filename != NULL) {
    tszfree (G->filename, strlen (G->filename) + 1);
  }
  if (G->filedata != NULL) {
    tszfree (G->filedata, G->filedata_len);
  }
  zfree (G, sizeof (struct gather_data));

  active_gathers--;
}

static int memcache_return_file_location (struct gather_data *G) {
  if (G->Out.exit_code < 0) {
    vkprintf (0, "%s: do_copy_doc returns %d.\n", G->key, G->Out.exit_code);
    return 0;
  }
  int l;
  char secret_b64url[12];
  l = base64url_encode ((unsigned char *) &G->Out.secret, 8, secret_b64url, sizeof (secret_b64url));
  if (md5_mode) {
    char md5_b64url[23];
    l = base64url_encode (G->Out.md5, 16, md5_b64url, sizeof (md5_b64url));
    assert (!l);
    l = sprintf (value_buff, "%s,%s", secret_b64url, md5_b64url);
  } else {
    l = sprintf (value_buff, "%s,%x", secret_b64url, G->Out.exit_code);
  }
  return return_one_key (G->c, G->key, value_buff, l);
}

static int rpc_return_file_location (struct gather_data *G);

static int rpc_mode (struct gather_data *G) {
  if (strcmp (G->key, "RPC")) {
    return 0;
  }
  return 1;
}

static int return_file_location (struct gather_data *G) {
  return rpc_mode (G) ? rpc_return_file_location (G) : memcache_return_file_location (G);
}

int delete_write_thread_query (struct conn_query *q) {
  struct gather_data *G = GATHER_QUERY(q);
  if (!G->Out.terminated) {
    return 0;
  }
  if (G->c->generation == q->req_generation) {
    return_file_location (G);
  }
  delete_conn_query (q);
  free_gather (G);
  zfree (q, sizeof (struct conn_query));
  return 0;
}

int write_thread_query_timeout (struct conn_query *q) {
  vkprintf (3, "Query %p timeout.\n", q);
  struct gather_data *G = GATHER_QUERY(q);
  kprintf ("Query on key %.*s timeout\n", G->key_len, G->key);
  gather_timeouts++;
  delete_write_thread_query (q);
  return 0;
}

int write_thread_check_completion (write_thread_arg_t *A) {
  if (A->Out.terminated) {
    delete_write_thread_query (A->q);
    return 1;
  }
  return 0;
}

int write_thread_check_all_completions (void) {
  int sum = 0;
  write_thread_arg_t *p = active_write_threads;
  while (p != NULL) {
    write_thread_arg_t *n = p->next;
    sum += write_thread_check_completion (p);
    p = n;
  }
  if (sum) {
    vkprintf (2, "write_thread_check_all_completion returns %d.\n", sum);
  }
  return sum;
}

static void *write_thread (void *arg) {
  write_thread_arg_t *a = (write_thread_arg_t *) arg;
  volume_t *V = a->V;
  pthread_mutex_lock (&V->mutex_write);
  a->Out.exit_code = do_copy_doc (V, &a->Out.secret, a->filename, a->filedata, a->filedata_len, a->content_type, a->Out.md5);
  pthread_mutex_unlock (&V->mutex_write);

  __sync_fetch_and_or (&a->Out.terminated, 1);
  //a->Out.terminated = 1;
  pthread_exit (0);
  return 0;
}

struct conn_query *rpc_create_inbound_query (struct gather_data *G, struct connection *c, double timeout) {
  vkprintf (4, "%s: (G: %p, connection %d, timeout: %.3lf)\n", __func__, G, c->fd, timeout);
  struct conn_query *Q = zmalloc0 (sizeof (struct conn_query));

  Q->custom_type = 0;
  Q->outbound = c;
  Q->requester = &Connections[0];
  Q->start_time = get_utime (CLOCK_MONOTONIC);
  Q->extra = G;
  Q->cq_type = &write_thread_query_type;
  Q->timer.wakeup_time = (timeout > 0 ? Q->start_time + timeout : 0);

  insert_conn_query (Q);

  vkprintf (1, "%s: after insert_conn_query(). Q->start_time = %lf, Q->timer.wakeup_time=%lf. cur_time=%lf\n", __func__, Q->start_time, Q->timer.wakeup_time, get_utime_monotonic ());

  Q->req_generation = c->generation;

  return Q;
}

struct conn_query *create_inbound_query (struct gather_data *G, struct connection *c, double timeout) {
  struct conn_query *Q = zmalloc0 (sizeof (struct conn_query));

  Q->custom_type = MCS_DATA(c)->query_type;
  Q->outbound = c;
  Q->requester = c;
  Q->start_time = get_utime (CLOCK_MONOTONIC);
  Q->extra = G;
  Q->cq_type = &write_thread_query_type;
  Q->timer.wakeup_time = (timeout > 0 ? Q->start_time + timeout : 0);

  insert_conn_query (Q);

  vkprintf (1, "%s: after insert_conn_query(). Q->start_time = %lf, Q->timer.wakeup_time=%lf. cur_time=%lf\n", __func__, Q->start_time, Q->timer.wakeup_time, get_utime_monotonic ());

  return Q;
}

struct rpc_get_file_data {
  long long req_id;
  unsigned long long secret;
  metafile_t *meta;
  struct connection *out;
  struct conn_target *targ;
  int offset;
  int limit;
  int *fwd_header;
  int ct;
  int fwd_pid;
  int fwd_start_time;
  int fwd_header_ints;
};

struct rpc_forward_query {
  struct rpc_forward_query *next, *prev;
  double deadline;
  struct rpc_get_file_data *L;
};

static struct connection *find_target_connection (struct rpc_get_file_data *L);

static int storage_engine_wakeup (struct connection *c) {
  vkprintf (2, "%s: connection %d\n", __func__, c->fd);
  struct mcs_data *D = MCS_DATA(c);
  c->generation = ++conn_generation;
  c->pending_queries = 0;
  c->status = conn_expect_query;
  assert (D->query_type == mct_get);
  clear_connection_timeout (c);
  write_out (&c->Out, "END\r\n", 5);
  if (c->Out.total_bytes > 0) {
    c->flags |= C_WANTWR;
  }
  return 0;
}

int storage_engine_alarm (struct connection *c) {
  vkprintf (1, "%s: connection %d timeout alarm, %d queries pending, status=%d\n", __func__, c->fd, c->pending_queries, c->status);
  assert (c->status == conn_wait_aio);
  c->flags |= C_INTIMEOUT;
  return storage_engine_wakeup (c);
}

int memcache_get_start (struct connection *c) {
  c->flags &= ~C_INTIMEOUT;
  return 0;
}

int memcache_get_end (struct connection *c, int key_count) {
  c->flags &= ~C_INTIMEOUT;
  vkprintf (3, "memcache_get_end (%p, %d)\n" , c, key_count);
  if (c->pending_queries > 0) {
    c->status = conn_wait_aio;
    set_connection_timeout (c, 3.0);
    return 0;
  }
  write_out (&c->Out, "END\r\n", 5);
  return 0;
}

int gen_volumes_list (void) {
  int i = 0;
  char *p = value_buff;
  for (i = 0; i < volumes; i++) {
    int o = value_buff + VALUE_BUFF_SIZE - p;
    if (o <= 0) {
      return -1;
    }
    int l = snprintf (p, o, i ? ",%lld" : "%lld", Volumes[i]->volume_id);
    if (l >= o) {
      return -2;
    }
    p += l;
  }
  return p - value_buff;
}

int create_write_thread (struct connection *c, long long req_id, volume_t *V, const char *key, const char *filename, unsigned char *filedata, int filedata_len, int content_type) {
  pthread_attr_t attr;
  pthread_attr_init (&attr);
  pthread_attr_setdetachstate (&attr, PTHREAD_CREATE_DETACHED);
  pthread_attr_setstacksize (&attr, 4 << 20);
  active_gathers++;
  struct gather_data *A = zmalloc0 (sizeof (struct gather_data));

  A->req_id = req_id;
  A->c = c;
  A->V = V;
  A->key_len = strlen (key);
  A->key = tszmalloc (A->key_len + 1);
  strcpy (A->key, key);
  if (filename) {
    A->filename = tszmalloc (strlen (filename) + 1);
    strcpy (A->filename, filename);
  }

  A->filedata = filedata;
  A->filedata_len = filedata_len;
  A->content_type = content_type;

  int r = pthread_create (&A->writing_thread, &attr, write_thread, (void *) A);

  if (r) {
    vkprintf (1, "create_write_thread: pthread_create failed. %m\n");
    pthread_attr_destroy (&attr);
    free_gather (A);
    return -1;
  }

  pthread_attr_destroy (&attr);

  c->query_start_time = mytime ();

  A->q = (rpc_mode (A) ? rpc_create_inbound_query : create_inbound_query) (A, c, 600.0);

  A->next = active_write_threads;
  if (active_write_threads) {
    active_write_threads->prev = A;
  }
  active_write_threads = A;
  return 0;
}

int memcache_get (struct connection *c, const char *key, int key_len) {
  vkprintf (3, "memcache_get (c = %p, %.*s)\n", c, key_len, key);
  get_queries++;
  int dog_len = get_at_prefix_length (key, key_len);
  key += dog_len;
  key_len -= dog_len;

  long long volume_id;
  char path[1024];
  int has_path = 0;
  int hide_local_id;

  if (key_len >= 4 && !memcmp (key, "file", 4) && sscanf (key+4, "%lld,%1023s", &volume_id, path) == 2) {
    get_file_queries++;
    has_path = 1;
  }

  if (key_len >= 4 && !memcmp (key, "hide", 4) && sscanf (key+4, "%lld,%x", &volume_id, &hide_local_id) == 2) {
    vkprintf (4, "HIDE\n");
    get_hide_queries++;
    assert (snprintf (path, sizeof (path), "**hide doc**%x", hide_local_id) < sizeof (path));
    has_path = 1;
  }

  if (has_path) {
    vkprintf (4, "path: %s\n", path);
    volume_t *V = get_volume_f (volume_id, 0);
    if (V == NULL) {
      get_volume_misses++;
      get_misses++;
      return 0;
    }

    if (V->disabled || binlog_disabled || force_write_index) {
      return 0;
    }
    create_write_thread (c, 0, V, key + dog_len, path, NULL, 0, ct_unknown);
    return 0;
  }

  int l;
  double percent = 0.0;
  long long file_size;
  if (key_len >= 10 && !memcmp (key, "check_file", 10) && (l = sscanf (key + 10, "%lld,%lld,%lf", &volume_id, &file_size, &percent)) >= 2) {
    if (l == 2) {
      percent = 99.0;
    }
    if (percent < 0.0) {
      percent = 0.0;
    }
    if (percent > 100.0) {
      percent = 100.0;
    }
    volume_t *V = get_volume_f (volume_id, 0);
    if (V == NULL) {
      get_volume_misses++;
      get_misses++;
      return 0;
    }
    if (V->disabled || binlog_disabled || force_write_index) {
      return 0;
    }
    l = storage_volume_check_file (V, percent * 0.01, file_size);
    return return_one_key (c, key - dog_len, value_buff, sprintf (value_buff, "%d", l));
  }

  if (key_len == 7 && !memcmp (key, "volumes", 7) && (l = gen_volumes_list ()) >= 0) {
    return return_one_key (c, key - dog_len, value_buff, l);
  }

  if (key_len >= 6 && !memcmp (key, "volume", 6) && sscanf (key + 6, "%lld", &volume_id) == 1 &&
      (l = get_volume_serialized (value_buff, volume_id) ) >= 0) {
    write_out (&c->Out, value_buff+l, sprintf (value_buff+l, "VALUE %s 1 %d\r\n", key - dog_len, l));
    write_out (&c->Out, value_buff, l);
    write_out (&c->Out, "\r\n", 2);
    return 0;
  }

  if (key_len >= 6 && !memcmp (key, "Volume", 6) && sscanf (key + 6, "%lld", &volume_id) == 1 &&
      (l = get_volume_text (value_buff, volume_id)) >= 0) {
    return return_one_key (c, key - dog_len, value_buff, l);
  }

  if (key_len == 4 && !memcmp (key, "dirs", 4) && !get_dirs_serialized (value_buff)) {
    l = strlen (value_buff);
    write_out (&c->Out, value_buff+l, sprintf (value_buff+l, "VALUE %s 1 %d\r\n", key - dog_len, l));
    write_out (&c->Out, value_buff, l);
    write_out (&c->Out, "\r\n", 2);
    return 0;
  }

  if (key_len >= 5 && !strncmp (key, "stats", 5)) {
    int len = storage_prepare_stats (c);
    return return_one_key (c, key - dog_len, stats_buff, len);
  }

  int dir_id = -1;

  if (key_len >= 11 && !memcmp (key, "enable_file", 11) && sscanf (key + 11, "%lld,%d", &volume_id, &dir_id) == 2) {
    volume_t *V = get_volume_f (volume_id, 0);
    if (V == NULL) {
      get_misses++;
      return 0;
    }
    l = storage_enable_binlog_file (V, dir_id);
    if (l == STORAGE_ERR_SIZE_MISMATCH) {
      return return_one_key (c, key - dog_len, "2", 1);
    }
    if (l < 0) {
      vkprintf (0, "enable_file%lld,%d returns error code %d.\n", volume_id, dir_id, l);
    }
    return return_one_key (c, key - dog_len, l < 0 ? "0" : "1", 1);
  }

  if (key_len >= 12 && !memcmp (key, "disable_file", 12) && sscanf (key + 12, "%lld,%d", &volume_id, &dir_id) == 2) {
    volume_t *V = get_volume_f (volume_id, 0);
    if (V == NULL) {
      get_misses++;
      return 0;
    }
    l = storage_close_binlog_file (V, dir_id);
    return return_one_key (c, key - dog_len, l < 0 ? "0" : "1", 1);
  }

  if (key_len >= 7 && !memcmp (key, "scandir", 7)) {
    char *msg;
    int dir_id = get_dir_id_by_name (key + 7);
    if (dir_id < 0) {
      msg = "Path wasn't found";
      return return_one_key (c, key - dog_len, msg, strlen (msg));
    }
    int r = storage_scan_dir (dir_id);
    if (r == STORAGE_ERR_SCANDIR_MULTIPLE) {
      msg = "Couldn't scandir more than once";
      return return_one_key (c, key - dog_len, msg, strlen (msg));
    } else {
      return return_one_key (c, key - dog_len, value_buff, sprintf (value_buff, "%d", r));
    }
  }

/*
  if (key_len >= 11 && !memcmp (key, "disable_dir", 11) && sscanf (key + 11, "%d", &dir_id) == 1) {
    l = change_dir_write_status (dir_id, 1);
    if (l < 0) {
      return 0;
    }
    get_hits++;
    return return_one_key (c, key - dog_len, value_buff, sprintf (value_buff, "%d", l));
  }

  if (key_len >= 10 && !memcmp (key, "enable_dir", 10) && sscanf (key + 10, "%d", &dir_id) == 1) {
    l = change_dir_write_status (dir_id, 0);
    if (l < 0) {
      return 0;
    }
    get_hits++;
    return return_one_key (c, key - dog_len, value_buff, sprintf (value_buff, "%d", l));
  }
*/
  get_misses++;
  return 0;
}

int memcache_store (struct connection *c, int op, const char *key, int key_len, int flags, int delay, int size) {
  return -2;
}

int memcache_stats (struct connection *c) {
  cmd_stats++;
  int len = storage_prepare_stats (c);
  write_out (&c->Out, stats_buff, len);
  write_out (&c->Out, "END\r\n", 5);

  return 0;
}
/********************* HTTP Methods *****************************/
static int http_wait (struct connection *c, struct aio_connection *WaitAio) {
  vkprintf (2, "%s: connection %d, WaitAio: %p\n", __func__, c->fd, WaitAio);
/*
  if (c->flags & C_INTIMEOUT) {
    if (verbosity > 1) {
      fprintf (stderr, "memcache_get: IN TIMEOUT (%p)\n", c);
    }
    return 0;
  }
  if (c->Out.total_bytes > 8192) {
    c->flags |= C_WANTWR;
    c->type->writer (c);
  }
*/
  if (!WaitAio) {
    vkprintf (1, "%s: WaitAio=0 - no memory to load user metafile, query dropped.\n", __func__);
    return 0;
  }
  conn_schedule_aio (WaitAio, c, aio_query_timeout_value, &aio_metafile_query_type);
  set_connection_timeout (c, aio_query_timeout_value - 0.2);
  return 0;
}

static int http_x_accel_redirect (struct connection *c, const char *filename, long long offset, char base64url_secret[12], int content_type) {
  static char location[256] = "X-Accel-Redirect: ";
  int r = snprintf (location + 18, 238, "%s:%llx:%s:%x\r\n", filename, offset, base64url_secret, content_type);
  if (r >= 238) {
    vkprintf (1, "location buffer overflow\n");
    return -500;
  }
  write_basic_http_header (c, 307, 0, -1, location, ContentTypes[content_type]);
  x_accel_redirects++;
  return 0;
}

static int http_x_accel_redirect_attachment (struct connection *c, const char *filename, long long offset, char base64url_secret[12], int content_type) {
  static char location[512] =
    "Content-Disposition: attachment\r\n"
    "X-Accel-Redirect: ";
  int r = snprintf (location + 18 + 33, 271, "%s:%llx:%s:%x\r\n", filename, offset, base64url_secret, content_type);
  if (r >= 271) {
    vkprintf (1, "location buffer overflow\n");
    return -500;
  }
  write_basic_http_header (c, 307, 0, -1, location, ContentTypes[content_type]);
  x_accel_redirects++;
  return 0;
}

int strpos(char *haystack, char *needle) {
   char *p = strstr(haystack, needle);
   if (p)
      return p - haystack;
   return -1;
}

int hts_execute (struct connection *c, int op);
int hts_wakeup (struct connection *c);
struct http_server_functions http_methods = {
  .execute = hts_execute,
  .ht_wakeup = hts_wakeup,
  .ht_alarm = hts_wakeup
};

#define	MAX_POST_SIZE	4096

static char *qUri, *qHeaders;
static int qUriLen, qHeadersLen;

static int http_try_x_accel_redirect (struct connection *c, metafile_t *meta, long long secret, int content_type, int error_code, long long *stat_cnt) {
  if (stat_cnt) {
    (*stat_cnt)++;
  }
  redirect_retries++;
  const long long offset = meta->offset, offset_end = offset + (meta->size - offsetof (metafile_t, data));
  const int forbidden_dirmask = 1 << meta->B->dir_id;
  volume_t *V = get_volume_f (meta->B->volume_id, 0);

  update_binlog_read_stat (meta, 0); //also unload metafile m, if refcnt is zero
  meta = NULL;

  storage_binlog_file_t *B = choose_reading_binlog (V, offset, offset_end, forbidden_dirmask);
  if (B == NULL) {
    return error_code;
  }
  char base64url_secret[12];
  int r = base64url_encode ((unsigned char *) &secret, 8, base64url_secret, 12);
  assert (!r);
  return http_x_accel_redirect (c, B->abs_filename, offset, base64url_secret, content_type);
}

long long redirect_retries_meta_aio, redirect_retries_corrupted, redirect_retries_secret, redirect_retries_type,
          redirect_retries_content_type, redirect_retries_local_id;

static void write_http_doc (struct connection *c, void *data, int size, int mtime, int content_type) {
  static char headers[128] =
    "Expires: Thu, 31 Dec 2037 23:55:55 GMT\r\n"
    "Cache-Control: max-age=315360000\r\n"
    "Last-Modified: *****************************\r\n";
  gen_http_date (headers + (40 + 34 + 15), mtime);
  write_basic_http_header (c, 200, 0, size, headers, ContentTypes[content_type]);
  write_out (&c->Out, data, size);
}

int http_return_file (struct connection *c, metafile_t *meta, long long secret, int content_type) {
  if (meta == NULL) {
    vkprintf (1, "http_return_file: meta == NULL\n");
    return -500;
  }
  meta->refcnt--;
  if (meta->aio) {
    vkprintf (2, "Metafile timeout error %s, offset: %lld, volume_id: %lld\n", meta->B->abs_filename, meta->offset, meta->B->volume_id);
    return http_try_x_accel_redirect (c, meta, secret, content_type, -500, &redirect_retries_meta_aio);
  }
  if (meta->corrupted) {
    vkprintf (2, "Metafile read error %s, offset: %lld, volume_id: %lld\n", meta->B->abs_filename, meta->offset, meta->B->volume_id);
    return http_try_x_accel_redirect (c, meta, secret, content_type, -500, &redirect_retries_corrupted);
  }

  const struct lev_storage_file *E = (struct lev_storage_file *) &meta->data[0];

  if ((secret ^ E->secret) & STORAGE_SECRET_MASK) {
    vkprintf (2, "Metafiles 2 high bytes of secret is corrupted, %s, offset: %lld, volume_id: %lld\n", meta->B->abs_filename, meta->offset, meta->B->volume_id);
    return http_try_x_accel_redirect (c, meta, secret, content_type, -500, &redirect_retries_secret);
  }

  if (E->type != LEV_STORAGE_FILE) {
    vkprintf (2, "E->type isn't LEV_STORAGE_FILE %s, offset: %lld, volume_id: %lld\n", meta->B->abs_filename, meta->offset, meta->B->volume_id);
    return http_try_x_accel_redirect (c, meta, secret, content_type, -500, &redirect_retries_type);
  }
  if (E->content_type >= ct_last || E->content_type < 0) {
    vkprintf (2, "Illegal E->content_type in %s, offset: %lld, volume_id: %lld\n", meta->B->abs_filename, meta->offset, meta->B->volume_id);
    return http_try_x_accel_redirect (c, meta, secret, content_type, -500, &redirect_retries_content_type);
  }
  if (E->local_id != meta->local_id) {
    vkprintf (2, "local_id not matched %s, offset: %lld, volume_id: %lld\n", meta->B->abs_filename, meta->offset, meta->B->volume_id);
    return http_try_x_accel_redirect (c, meta, secret, content_type, -500, &redirect_retries_local_id);
  }
  update_binlog_read_stat (meta, 1);
  if (E->secret != secret) {
    vkprintf (2, "secret not matched %s, offset: %lld, volume_id: %lld\n", meta->B->abs_filename, meta->offset, meta->B->volume_id);
    return -403;
  }
  if (E->content_type != content_type) {
    vkprintf (2, "E->content_type (%d) != content_type (%d) %s, offset: %lld, volume_id: %lld\n", E->content_type, content_type, meta->B->abs_filename, meta->offset, meta->B->volume_id);
    return -404;
  }
  vkprintf (3, "E->size = %d\n", E->size);

  write_http_doc (c, (void *) E->data, E->size, E->mtime, E->content_type);
  return 0;
}

static inline int base64url_to_secret (const char *input, unsigned long long *secret) {
  int r = base64url_decode (input, (unsigned char *) secret, 8);
  if (r < 0) {
    return r;
  }
  if (r != 8) {
    return -7;
  }
  return 0;
}

int http_error_one_pix_transparent (struct connection *c) {
  one_pix_transparent_errors++;
  static const unsigned char one_pix_transparent_png[82] = {
    137, 80, 78, 71, 13, 10, 26, 10, 0, 0, 0, 13, 73, 72, 68, 82,
    0, 0, 0, 1, 0, 0, 0, 1, 8, 4, 0, 0, 0, 181, 28, 12,
    2, 0, 0, 0, 2, 98, 75, 71, 68, 0, 0, 170, 141, 35, 50, 0,
    0, 0, 11, 73, 68, 65, 84, 8, 215, 99, 96, 96, 0, 0, 0, 3,
    0, 1, 32, 213, 148, 199, 0, 0, 0, 0, 73, 69, 78, 68, 174, 66,
    96, 130
  };
  write_http_doc (c, (void *) one_pix_transparent_png, 82, 545774400, ct_png);
  return 0;
}

int hts_execute (struct connection *c, int op) {
  struct hts_data *D = HTS_DATA(c);
  static char ReqHdr[MAX_HTTP_HEADER_SIZE];

  vkprintf (1, "in hts_execute: connection #%d, op=%d, header_size=%d, data_size=%d, http_version=%d\n",
	     c->fd, op, D->header_size, D->data_size, D->http_ver);
  if (D->data_size >= MAX_POST_SIZE) {
    return -413;
  }

  if (D->query_type != htqt_get && D->query_type != htqt_head) {
    D->query_flags &= ~QF_KEEPALIVE;
    return -501;
  }

  assert (D->header_size <= MAX_HTTP_HEADER_SIZE);
  assert (read_in (&c->In, &ReqHdr, D->header_size) == D->header_size);

  qHeaders = ReqHdr + D->first_line_size;
  qHeadersLen = D->header_size - D->first_line_size;
  assert (D->first_line_size > 0 && D->first_line_size <= D->header_size);

  if (verbosity > 1) {
    fprintf (stderr, "===============\n%.*s\n==============\n", D->header_size, ReqHdr);
    fprintf (stderr, "%d,%d,%d,%d\n", D->host_offset, D->host_size, D->uri_offset, D->uri_size);

    fprintf (stderr, "hostname: '%.*s'\n", D->host_size, ReqHdr + D->host_offset);
    fprintf (stderr, "URI: '%.*s'\n", D->uri_size, ReqHdr + D->uri_offset);
  }

  qUri = ReqHdr + D->uri_offset;
  qUriLen = D->uri_size;
  if (qUriLen > 4095) {
    return -404;
  }

  qUri[qUriLen] = 0;

  int local_id;
  unsigned long long secret = 0;
  char ext[4], base64url_md5[32];
  unsigned char md5[16];
  volume_t *P;
  unsigned long long offset;
  int r = -239;
  long long volume_id;
  int filesize = -1;
  int dl = -1;

  if(attachment){
    dl = strpos(qUri, "dl=1");
  }

  char base64url_secret[12];
  if (md5_mode) {
    if (sscanf (qUri, "/v%lld/%31[0-9A-Za-z_-]/%11[0-9A-Za-z_-].%3s", &volume_id, base64url_md5, base64url_secret, ext) != 4 || base64url_to_secret (base64url_secret, &secret)) {
      vkprintf (3, "couldn't parse URI\n");
      return http_error_one_pix_transparent (c);
    }
    r = base64url_decode (base64url_md5, md5, 16);
    if (r != 16) {
      vkprintf (3, "r(%d) != 16\n", r);
      return http_error_one_pix_transparent (c);
    }
    r = do_md5_get_doc (volume_id, md5, secret, &P, &offset);
  } else {
    int argc = sscanf (qUri, "/v%lld/%x/%11[0-9A-Za-z_-].%3s", &volume_id, &local_id, base64url_secret, ext);
    if (argc != 4) {
      vkprintf (3, "couldn't parse URI (argc = %d)\n", argc);
      return http_error_one_pix_transparent (c);
    }
    r = base64url_to_secret (base64url_secret, &secret);
    if (r) {
      vkprintf (3, "base64_url_to_secret (%s) returns error code %d.\n", base64url_secret, r);
      return http_error_one_pix_transparent (c);
    }
    r = do_get_doc (volume_id, local_id, secret, &P, &offset, &filesize);
  }

  if (r) {
    vkprintf (3, "secret = %llx\n", secret);
    vkprintf (1, "do_get_doc returns error code %d.\n", r);
    return http_error_one_pix_transparent (c);
  }
  int content_type = ext_to_content_type (ext);
  if (content_type >= ct_last || content_type < 0) {
    vkprintf (1, "content_type %d is out of range\n", content_type);
    return http_error_one_pix_transparent (c);
  }

  char date_buff[64];
  if (get_http_header (qHeaders, qHeadersLen, date_buff, sizeof (date_buff), "IF-MODIFIED-SINCE", 17) >= 0) {
    write_basic_http_header (c, 304, 0, -1, NULL, ContentTypes[content_type]);
    return 0;
  }

  int immediately_reply = (filesize >= 0 && filesize <= max_immediately_reply_filesize);
  storage_binlog_file_t *B;
  if (!immediately_reply) {
    long long end = offset;
    end += sizeof (struct lev_storage_file);
    if (filesize >= 0) {
      end += filesize;
    }
    B = choose_reading_binlog (P, offset, end, 0);
    if (B == NULL) {
      return -400;
    }
    if(dl != -1){
        return http_x_accel_redirect_attachment (c, B->abs_filename, offset, base64url_secret, content_type);
    }

    return http_x_accel_redirect (c, B->abs_filename, offset, base64url_secret, content_type);
  } else {
    metafile_t *meta;
    r = metafile_load (P, &meta, &B, volume_id, local_id, filesize, offset);
    if (r == -2) {
      //aio stuff
      int t[2];
      memcpy (&t[0], &secret, 8);
      D->extra_int = t[0];
      D->extra_int2 = t[1];
      D->extra_int3 = content_type;
      D->extra = meta;
      meta->refcnt++;
      return http_wait (c, meta->aio);
    } else if (r == STORAGE_ERR_TOO_MANY_AIO_CONNECTIONS) {
      assert (B);
      too_many_aio_connections_errors++;

      if(dl != -1) {
        return http_x_accel_redirect_attachment (c, B->abs_filename, offset, base64url_secret, content_type);
      }

      return http_x_accel_redirect (c, B->abs_filename, offset, base64url_secret, content_type);
    } else {
      metafiles_cache_hits++;
      return http_return_file (c, meta, secret, content_type);
    }
  }
}

int hts_wakeup (struct connection *c) {
  vkprintf (3, "hts_wakeup (%p, c->status = %d)\n", c, c->status);
  struct hts_data *D = HTS_DATA(c);
  int t[2];
  t[0] = D->extra_int;
  t[1] = D->extra_int2;
  long long secret;
  memcpy (&secret, &t[0], 8);
  int content_type = D->extra_int3;

  assert (c->status == conn_expect_query || c->status == conn_wait_aio);
  c->status = conn_expect_query;
  clear_connection_timeout (c);

  if (!(D->query_flags & QF_KEEPALIVE)) {
    c->status = conn_write_close;
    c->parse_state = -1;
  }
  metafile_t *meta = D->extra;
  int res = http_return_file (c, meta, secret, content_type);
  if (res < 0) {
    write_http_error (c, -res);
  }
  return 0;
}

/********************* RPC-server Methods *****************************/

struct rpc_forward_query forward_queries = { .prev = &forward_queries, .next = &forward_queries };

static void create_new_forward_query (struct rpc_get_file_data *L) {
  vkprintf (3, "create_new_forward_query (%p)\n", L);
  struct rpc_forward_query *q = zmalloc0 (sizeof (struct rpc_forward_query));
  q->L = L;
  q->deadline = get_utime_monotonic () + connect_timeout;
  struct rpc_forward_query *v = &forward_queries, *u = forward_queries.prev;
  u->next = q; q->prev = u;
  v->prev = q; q->next = v;
}

static void rpc_out_file_data (struct rpc_get_file_data *L, const struct lev_storage_file *E) {
  int sz = E->size - L->offset;
  if (sz > L->limit) {
    sz = L->limit;
  }
  char *p = (char *) E->data;
  if (sz < 0) {
    sz = 0;
  } else {
    p += L->offset;
  }
  rpc_out_cstring (p, sz);
}

static void rpc_send_file_content (struct connection *c, struct rpc_get_file_data *L) {
  vkprintf (2, "%s: <req_id = %lld>, c->fd: %d.\n", __func__, L->req_id, c->fd);
  metafile_t *meta = L->meta;
  const struct lev_storage_file *E = (struct lev_storage_file *) &meta->data[0];
  rpc_clear_packet (1);      //12
  rpc_out_long (L->req_id); //20
  rpc_out_int (TL_STORAGE_FILE_CONTENT);
  rpc_out_long (meta->B->volume_id); //32
  rpc_out_int (meta->local_id); // 36
  rpc_out_int (L->ct);             // 40
  rpc_out_int (E->mtime);       // 44
  rpc_out_file_data (L, E);
  rpc_send_packet (c, RPC_REQ_RESULT);
}

static void rpc_forward_file_content (struct connection *F, struct rpc_get_file_data *L) {
  vkprintf (2, "%s: <req_id = %lld>\n", __func__, L->req_id);
  metafile_t *meta = L->meta;
  const struct lev_storage_file *E = (struct lev_storage_file *) &meta->data[0];
  rpc_clear_packet (0);      //12
  rpc_out_ints (L->fwd_header, L->fwd_header_ints);
  rpc_out_int (L->ct);
  rpc_out_int (E->mtime);
  rpc_out_file_data (L, E);
  rpc_send_packet (F, 0);

  rpc_clear_packet (1);
  rpc_out_long (L->req_id);
  rpc_out_int (TL_STORAGE_FILE_FORWARDED_INFO);
  rpc_out_long (meta->B->volume_id);
  rpc_out_int (meta->local_id); // 36
  rpc_out_int (L->ct);             // 40
  rpc_out_int (E->mtime);       // 44
  rpc_send_packet (L->out, RPC_REQ_RESULT);
}

static struct connection *find_target_connection (struct rpc_get_file_data *L) {
  struct conn_target *targ = L->targ;
  struct connection *c;
  vkprintf (4, "find_target_connection: targ->outbound_connections = %d\n", targ->outbound_connections);
  if (!targ->outbound_connections) {
    return 0;
  }
  c = targ->first_conn;
  while (1) {
    vkprintf (4, "find_target_connection: c = %p, c->fd: %d, server_check_ready (c) = %d, c->type = %p, &ct_rpc_client = %p\n",
      c, c ? c->fd : -1, server_check_ready (c), c->type, &ct_rpc_client);
    if (server_check_ready (c) == cr_ok && c->type == &ct_rpc_client) {
      struct rpcc_data *D = RPCC_DATA (c);
      if (D->remote_pid.pid == L->fwd_pid && D->remote_pid.utime == L->fwd_start_time) {
        vkprintf (4, "find_target_connection(%p): PID matched\n", c);
        return c;
      }
      vkprintf (4, "D->remote_pid.ip: %d, D->remote_pid:port: %d, D->remote_pid.pid:%d, L->fwd_pid:%d, D->remote_pid.utime:%d, L->fwd_start_time:%d\n", D->remote_pid.ip, D->remote_pid.port, D->remote_pid.pid, L->fwd_pid, D->remote_pid.utime, L->fwd_start_time);
    }
    if (c == targ->last_conn) { break;}
    c = c->next;
  }
  return 0;
}

static int forward_query_check_completion (struct rpc_forward_query *F, double t) {
  int r = 0;
  struct rpc_get_file_data *L = F->L;
  struct connection *C = find_target_connection (L);
  if (C) {
    rpc_forward_file_content (C, L);
    r = 1;
  } else if (t >= F->deadline) {
    rpc_send_file_content (L->out, L);
    r = 1;
  }
  if (r) {
    struct rpc_forward_query *u = F->prev, *v = F->next;
    u->next = v;
    v->prev = u;
  }
  return r;
}

static int forward_query_check_all_completions (void) {
  struct rpc_forward_query *q = forward_queries.next;
  if (q == &forward_queries) {
    return 0;
  }
  int sum = 0;
  double t = get_utime_monotonic ();
  while (q != &forward_queries) {
    struct rpc_forward_query *w = q->next;
    sum += forward_query_check_completion (q, t);
    q = w;
  }
  if (sum) {
    vkprintf (2, "forward_query_check_all_completion returns %d.\n", sum);
  }
  return sum;
}

/************************************ RPC AIO ****************************************/
static int storage_rpc_wakeup (struct connection *c) {
  kprintf ("%s: who called wakeup for connection %d?\n", __func__, c->fd);
  assert (0);
}

static int rpc_storage_aio_query_wakeup (struct conn_query *q);
static int rpc_storage_aio_query_complete (struct conn_query *q);

conn_query_type_t rpc_aio_metafile_query_type = {
  .magic = CQUERY_FUNC_MAGIC,
  .title = "storage-rpc-aio-metafile-query",
  .wakeup = rpc_storage_aio_query_wakeup,
  .close = rpc_storage_aio_query_complete,
  .complete = rpc_storage_aio_query_complete
};

static int rpc_create_aio_query (struct aio_connection *a, struct connection *c, double timeout, struct conn_query_functions *cq, void *extra) {
  struct conn_query *Q = zmalloc (sizeof (struct conn_query));

  vkprintf (2, "%s (%p[%d], %p[%d]): Q=%p\n", __func__, a, a->fd, c, c->fd, Q);

  Q->custom_type = 0;
  Q->outbound = (struct connection *) a;
  Q->requester = &Connections[0];
  Q->start_time = /*c->query_start_time*/get_utime (CLOCK_MONOTONIC);
  Q->extra = extra;
  Q->cq_type = cq;
  Q->timer.wakeup_time = (timeout > 0 ? Q->start_time + timeout : 0);

  if (verbosity > 1 && a->first_query != (struct conn_query *) a) {
    kprintf ("!NOTICE! inserting second query to %p\n", a);
  }

  insert_conn_query (Q);
  active_aio_queries++;
  tot_aio_queries++;

  Q->req_generation = c->generation;

  vkprintf (2, "%s: after insert_conn_query()\n", __func__);
  return 1;
}

static int rpc_conn_schedule_aio (struct aio_connection *a, struct connection *c, double timeout, struct conn_query_functions *cq, void *extra) {
  vkprintf (2, "%s: a: %p, c: %p,\n", __func__, a, c);
  rpc_create_aio_query (a, c, timeout, cq, extra);
  return -1;
}

static int rpc_wait (struct connection *c, struct aio_connection *WaitAio, struct rpc_get_file_data *L) {
  vkprintf (2, "%s: connection %d, WaitAio:%p\n", __func__, c->fd, WaitAio);
  if (!WaitAio) {
    vkprintf (1, "%s: WaitAio=0 - no memory to load user metafile, query dropped.\n", __func__);
    return 0;
  }
  rpc_conn_schedule_aio (WaitAio, c, aio_query_timeout_value, &rpc_aio_metafile_query_type, L);
  return 0;
}

static void rpc_get_file_data_free (struct rpc_get_file_data *L) {
  if (L->meta) {
    L->meta->refcnt--;
  }
  if (L->fwd_header) {
    zfree (L->fwd_header, 4 * L->fwd_header_ints);
  }
  zfree (L, sizeof (struct rpc_get_file_data));
}

static int rpc_return_file (struct connection *c, struct rpc_get_file_data *L) {
  if (L == NULL) {
    vkprintf (2, "%s: L == NULL\n", __func__);
    return -1;
  }
  metafile_t *meta = L->meta;
  if (meta == NULL) {
    vkprintf (2, "%s: meta == NULL\n", __func__);
    rpc_send_error (c, L->req_id, STORAGE_ERR_NULL_POINTER_EXCEPTION, "%s: meta == NULL", __func__);
    return -1;
  }
  //meta->refcnt--;
  if (meta->aio) {
    vkprintf (2, "%s: Metafile timeout error %s, offset: %lld, volume_id: %lld\n", __func__, meta->B->abs_filename, meta->offset, meta->B->volume_id);
    rpc_send_error (c, L->req_id, STORAGE_ERR_TIMEOUT, "Metafile timeout error %s, offset: %lld, volume_id: %lld\n", meta->B->abs_filename, meta->offset, meta->B->volume_id);
    return -1;
  }

  if (meta->corrupted) {
    vkprintf (2, "%s: Metafile read error %s, offset: %lld, volume_id: %lld\n", __func__, meta->B->abs_filename, meta->offset, meta->B->volume_id);
    rpc_send_error (c, L->req_id, STORAGE_ERR_READ, "Metafile read error %s, offset: %lld, volume_id: %lld\n", meta->B->abs_filename, meta->offset, meta->B->volume_id);
    return -1;
  }

  const struct lev_storage_file *E = (struct lev_storage_file *) &meta->data[0];

  if ((L->secret ^ E->secret) & STORAGE_SECRET_MASK) {
    vkprintf (2, "%s: Metafiles 2 high bytes of secret is corrupted, %s, offset: %lld, volume_id: %lld\n", __func__, meta->B->abs_filename, meta->offset, meta->B->volume_id);
    rpc_send_error (c, L->req_id, STORAGE_ERR_READ, "Metafiles 2 high bytes of secret is corrupted, %s, offset: %lld, volume_id: %lld\n", meta->B->abs_filename, meta->offset, meta->B->volume_id);
    return -1;
  }

  if (E->type != LEV_STORAGE_FILE) {
    vkprintf (2, "%s: E->type isn't LEV_STORAGE_FILE %s, offset: %lld, volume_id: %lld\n", __func__, meta->B->abs_filename, meta->offset, meta->B->volume_id);
    rpc_send_error (c, L->req_id, STORAGE_ERR_READ, "E->type isn't LEV_STORAGE_FILE %s, offset: %lld, volume_id: %lld\n", meta->B->abs_filename, meta->offset, meta->B->volume_id);
    return -1;
  }

  if (E->content_type >= ct_last || E->content_type < 0) {
    vkprintf (2, "%s: Illegal E->content_type(%d) in %s, offset: %lld, volume_id: %lld\n", __func__, E->content_type, meta->B->abs_filename, meta->offset, meta->B->volume_id);
    rpc_send_error (c, L->req_id, STORAGE_ERR_READ, "Illegal E->content_type(%d) in %s, offset: %lld, volume_id: %lld\n", E->content_type, meta->B->abs_filename, meta->offset, meta->B->volume_id);
    return -1;
  }

  if (E->local_id != meta->local_id) {
    vkprintf (2, "%s: local_id isn't matched %s, offset: %lld, volume_id: %lld\n", __func__, meta->B->abs_filename, meta->offset, meta->B->volume_id);
    rpc_send_error (c, L->req_id, STORAGE_ERR_READ, "local_id isn't matched %s, offset: %lld, volume_id: %lld\n", meta->B->abs_filename, meta->offset, meta->B->volume_id);
    return -1;
  }

  update_binlog_read_stat (meta, 1);
  if (E->secret != L->secret) {
    vkprintf (2, "%s: secret isn't matched %s, offset: %lld, volume_id: %lld\n", __func__, meta->B->abs_filename, meta->offset, meta->B->volume_id);
    rpc_send_error (c, L->req_id, STORAGE_ERR_PERM, "wrong secret");
    return -1;
  }
  /*
  if (E->content_type != content_type) {
    vkprintf (2, "E->content_type (%d) != content_type (%d) %s, offset: %lld, volume_id: %lld\n", E->content_type, content_type, meta->B->abs_filename, meta->offset, meta->B->volume_id);
    return -404;
  }
  vkprintf (3, "E->size = %d\n", E->size);
  */
  L->ct = content_type_to_file_type (E->content_type);
  if (!L->ct) {
    rpc_send_error (c, L->req_id, STORAGE_ERR_SERIALIZATION, "Illegal E->content_type(%d)", E->content_type);
    return -1;
  }

  if (L->targ) {
    struct connection *C = find_target_connection (L);
    if (C) {
      rpc_forward_file_content (C, L);
    } else {
      create_new_forward_query (L);
      return 1; /* don't free L */
    }
  } else {
    rpc_send_file_content (c, L);
  }
  return 0;
}

static int rpc_storage_aio_query_complete (struct conn_query *q) {
  vkprintf (2, "%s: (q:%p)\n", __func__, q);
  struct rpc_get_file_data *L = q->extra;
  struct connection *c = L->out;
  if (c->generation != q->req_generation) {
    vkprintf (2, "%s: connection %d generation is %d, but query req_generation is %d.\n",
      __func__, c->fd, c->generation, q->req_generation);
  } else {
    int r = rpc_return_file (c, L);
    if (r <= 0) {
      rpc_get_file_data_free (L);
    }
  }
  delete_aio_query (q);
  return 0;
}

static int rpc_storage_aio_query_wakeup (struct conn_query *q) {
  vkprintf (2, "%s: (q:%p)\n", __func__, q);
  struct rpc_get_file_data *L = q->extra;
  assert (L);
  struct connection *c = q->requester;
  if (c->generation != q->req_generation) {
    vkprintf (2, "%s: connection %d generation is %d, but query req_generation is %d.\n",
      __func__, c->fd, c->generation, q->req_generation);
  } else {
    rpc_send_error (c, L->req_id, STORAGE_ERR_TIMEOUT, "Aio timeout error");
  }
  return aio_query_timeout (q);
}

static int rpc_return_file_location (struct gather_data *G) {
  vkprintf (2, "%s: <req_id = %lld>, c->fd: %d\n", __func__, G->req_id, G->c->fd);
  if (G->Out.exit_code < 0) {
    rpc_send_error (G->c, G->req_id, G->Out.exit_code, "do_copy_doc returns %d", G->Out.exit_code);
    return 0;
  }
  assert (!md5_mode);
  rpc_clear_packet (1);
  rpc_out_long (G->req_id);
  rpc_out_int (TL_STORAGE_FILE_LOCATION);
  rpc_out_long (G->V->volume_id);
  rpc_out_int (G->Out.exit_code);
  rpc_out_long (G->Out.secret);
  rpc_send_packet (G->c, RPC_REQ_RESULT);
  return 0;
}

int rpcs_execute_get_volumes (struct connection *c, long long req_id, int len) {
  int i;
  vkprintf (2, "%s: <req_id = %lld>, c->fd: %d, len: %d\n", __func__, req_id, c->fd, len);
  rpc_readin (c, len);
  if (!rpc_end_of_fetch ()) {
    return rpc_send_error (c, req_id, STORAGE_ERR_SERIALIZATION, "packet contains extra data");
  }
  rpc_clear_packet (1);
  rpc_out_long (req_id);
  rpc_out_int (TL_STORAGE_VOLUMES);
  //rpc_out_int (TL_VECTOR);
  rpc_out_int (volumes);
  for (i = 0; i < volumes; i++) {
    rpc_out_long (Volumes[i]->volume_id);
  }
  return rpc_send_packet (c, RPC_REQ_RESULT);
}

int rpcs_execute_check_file (struct connection *c, long long req_id, int len) {
  vkprintf (2, "%s: <req_id = %lld>, c->fd: %d, len: %d\n", __func__, req_id, c->fd, len);
  rpc_readin (c, len);
  struct storage_rpc_check_file CF;
  if (rpc_fetch_ints ((void *) &CF, sizeof (CF) / 4) < 0) {
    return rpc_send_error (c, req_id, STORAGE_ERR_SERIALIZATION, "packet is too short, expected storage.checkFile");
  }
  if (!rpc_end_of_fetch ()) {
    return rpc_send_error (c, req_id, STORAGE_ERR_SERIALIZATION, "packet contains extra data");
  }

  if (binlog_disabled) {
    advance_skip_read_ptr (&c->In, len);
    return rpc_send_error (c, req_id, STORAGE_ERR_WRITE_IN_READONLY_MODE, "read only");
  }

  if (force_write_index) {
    advance_skip_read_ptr (&c->In, len);
    return rpc_send_error (c, req_id, STORAGE_ERR_XXX, "force_write_index");
  }

  volume_t *V = get_volume_f (CF.volume_id, 0);
  if (V == NULL) {
    advance_skip_read_ptr (&c->In, len);
    return rpc_send_error (c, req_id, STORAGE_ERR_UNKNOWN_VOLUME_ID, "volume_id %lld isn't found", CF.volume_id);
  }

  if (V->disabled) {
    advance_skip_read_ptr (&c->In, len);
    return rpc_send_error (c, req_id, STORAGE_ERR_BINLOG_DISABLED, "volume_id %llu is disabled", CF.volume_id);
  }
  int l = storage_volume_check_file (V, CF.max_disk_used_space_percent, CF.file_size);
  rpc_clear_packet (1);
  rpc_out_long (req_id);
  rpc_out_int (l ? TL_BOOL_TRUE : TL_BOOL_FALSE);
  return rpc_send_packet (c, RPC_REQ_RESULT);
}

int rpcs_execute_upload_file (struct connection *c, long long req_id, int len, int ver) {
  vkprintf (2, "%s: <req_id = %lld>, c->fd: %d, len: %d\n", __func__, req_id, c->fd, len);
  long long volume_id;
  assert (ver >= 0 && ver <= 1);
  //volume_id + crc32
  if (len < 12 + 4 * ver) {
    advance_skip_read_ptr (&c->In, len);
    return -__LINE__;
  }

  if (md5_mode) {
    advance_skip_read_ptr (&c->In, len);
    return rpc_send_error (c, req_id, STORAGE_ERR_NOT_IMPLEMENTED, "RPC uploading file request isn't implemented for md5 mode");
  }

  if (binlog_disabled) {
    advance_skip_read_ptr (&c->In, len);
    return rpc_send_error (c, req_id, STORAGE_ERR_WRITE_IN_READONLY_MODE, "read only");
  }

  if (force_write_index) {
    advance_skip_read_ptr (&c->In, len);
    return rpc_send_error (c, req_id, STORAGE_ERR_XXX, "force_write_index");
  }

  assert (read_in (&c->In, &volume_id, 8) == 8);
  vkprintf (4, "volume_id: %lld\n", volume_id);
  len -= 8;
  int content_type = ct_unknown;
  if (ver == 1) {
    int ft;
    assert (read_in (&c->In, &ft, 4) == 4);
    vkprintf (4, "file_type: %d\n", ft);
    len -= 4;
    content_type = file_type_to_content_type (ft);
    if (content_type > ct_last) {
      advance_skip_read_ptr (&c->In, len);
      return rpc_send_error (c, req_id, STORAGE_ERR_UNKNOWN_TYPE, "unknown file type 0x%08x", ft);
    }
  }

  volume_t *V = get_volume_f (volume_id, 0);
  if (V == NULL) {
    advance_skip_read_ptr (&c->In, len);
    return rpc_send_error (c, req_id, STORAGE_ERR_UNKNOWN_VOLUME_ID, "volume_id %lld isn't found", volume_id);
  }

  if (V->disabled) {
    advance_skip_read_ptr (&c->In, len);
    return rpc_send_error (c, req_id, STORAGE_ERR_BINLOG_DISABLED, "volume_id %llu is disabled", volume_id);
  }

  unsigned char *file_data = tszmalloc (len - 4);
  assert (file_data != NULL);
  int r = read_in (&c->In, file_data, len - 4);
  if (verbosity >= 4) {
    int i;
    for (i = 0; i < r; i++) {
      fprintf (stderr, "%02x ", (int) file_data[i]);
    }
    fprintf (stderr, "\n");
    fflush (stderr);
  }
  vkprintf (4, "read %d bytes of data, expected %d bytes\n", r, len - 4);
  assert (r == len - 4);
  advance_skip_read_ptr (&c->In, 4);
  r = create_write_thread (c, req_id, V, "RPC", NULL, file_data, len - 4, content_type);
  if (r < 0) {
    return rpc_send_error (c, req_id, r, "create_write_thread failed");
  }

  return 0;
}

int rpcs_execute_get_file (struct connection *c, long long req_id, int len, int direct, int partial) {
  vkprintf (2, "%s: <req_id = %lld>, c->fd: %d, len: %d, direct: %d\n", __func__, req_id, c->fd, len, direct);
  rpc_readin (c, len);
  struct storage_rpc_file_location L;
  if (rpc_fetch_ints ((void *) &L, sizeof (L) / 4) < 0) {
    return rpc_send_error (c, req_id, STORAGE_ERR_SERIALIZATION, "packet is too short, expected FILE_LOCATION");
  }
  if (L.magic != TL_STORAGE_FILE_LOCATION) {
    return rpc_send_error (c, req_id, STORAGE_ERR_SERIALIZATION, "expected FILE_LOCATION magic, but %d found", L.magic);
  }
  if (md5_mode) {
    return rpc_send_error (c, req_id, STORAGE_ERR_NOT_IMPLEMENTED, "RPC uploading file request isn't implemented for md5 mode");
  }
  struct storage_rpc_partial P;
  if (partial) {
    if (rpc_fetch_ints ((void *) &P, sizeof (P) / 4) < 0) {
      return rpc_send_error (c, req_id, STORAGE_ERR_SERIALIZATION, "packet is too short, expected offset & limit");
    }
  } else {
    P.offset = 0;
    P.limit = INT_MAX;
  }

  volume_t *V;
  unsigned long long offset;
  int filesize = -1;
  int r = do_get_doc (L.volume_id, L.local_id, L.secret, &V, &offset, &filesize);
  if (r < 0) {
    return rpc_send_error (c, req_id, r, "do_get_doc returns %d", r);
  }
  int max_filesize = MAX_PACKET_LEN - 52;
  int *a = NULL;
  struct storage_rpc_forward F;
  if (!direct) {
    if (rpc_fetch_ints ((int *) &F, sizeof (F) / 4) < 0) {
      return rpc_send_error (c, req_id, STORAGE_ERR_SERIALIZATION, "packet is too short, expected GlobalPid");
    }
    if (F.fwd_header_ints > 4096) {
      return rpc_send_error (c, req_id, STORAGE_ERR_SERIALIZATION, "fwd_header_ints(%d) is too big", F.fwd_header_ints);
    }
    a = zmalloc (F.fwd_header_ints * 4);
    if (rpc_fetch_ints (a, F.fwd_header_ints) < 0) {
      zfree (a, sizeof (F.fwd_header_ints * 4));
      a = NULL;
      return rpc_send_error (c, req_id, STORAGE_ERR_SERIALIZATION, "packet is too short, fwd_header");
    }
    max_filesize = MAX_PACKET_LEN - 4 * (F.fwd_header_ints + 8);
  }

  if (filesize > max_filesize) {
    if (a) {
      zfree (a, sizeof (F.fwd_header_ints * 4));
      a = NULL;
    }
    return rpc_send_error (c, req_id, STORAGE_ERR_BIG_FILE, "file is too big (%d bytes)", filesize);
  }

  if (!rpc_end_of_fetch ()) {
    if (a) {
      zfree (a, sizeof (F.fwd_header_ints * 4));
      a = NULL;
    }
    return rpc_send_error (c, req_id, STORAGE_ERR_SERIALIZATION, "packet contains extra data");
  }

  struct rpc_get_file_data *E = zmalloc0 (sizeof (struct rpc_get_file_data));
  E->out = c;
  E->req_id = req_id;
  E->secret = L.secret;
  E->offset = P.offset;
  E->limit = P.limit;
  if (!direct) {
    default_ct.target.s_addr = ntohl (F.fwd_address);
    default_ct.port = F.fwd_port;
    E->targ = create_target (&default_ct, 0);
    E->fwd_pid = F.fwd_pid;
    E->fwd_start_time = F.fwd_start_time;
    E->fwd_header_ints = F.fwd_header_ints;
    E->fwd_header = a;
    a = NULL;
  }
  storage_binlog_file_t *B;
  r = metafile_load (V, &E->meta, &B, L.volume_id, L.local_id, filesize, offset);
  E->meta->refcnt++;
  if (r == -2) {
    //aio stuff
    rpc_wait (c, E->meta->aio, E);
  } else {
    metafiles_cache_hits++;
    if (rpc_return_file (c, E) <= 0) {
      rpc_get_file_data_free (E);
    }
  }

  return 0;
}

int rpcs_execute (struct connection *c, int op, int len) {
  static int P[6];
  vkprintf (2, "rpcs_execute: fd=%d, op=%x, len=%d\n", c->fd, op, len);

  if (len < 28 || (len % 4)) {
    return SKIP_ALL_BYTES;
  }

  assert (read_in (&c->In, P, 24) == 24);
  len -= 24;
  if (op != RPC_INVOKE_REQ) {
    advance_skip_read_ptr (&c->In, len);
    return -__LINE__;
  }

  long long req_id;
  memcpy (&req_id, P + 3, 8);
  op = P[5];

  vkprintf (2, "%s: <req_id = %lld>\n", __func__, req_id);

  switch (op) {
    case TL_STORAGE_CHECK_FILE: return rpcs_execute_check_file (c, req_id, len);
    case TL_STORAGE_UPLOAD_FILE: return rpcs_execute_upload_file (c, req_id, len, 0);
    case TL_STORAGE_UPLOAD_FILE_EXT: return rpcs_execute_upload_file (c, req_id, len, 1);
    case TL_STORAGE_GET_FILE: return rpcs_execute_get_file (c, req_id, len, 1, 0);
    case TL_STORAGE_GET_FILE_INDIRECT: return rpcs_execute_get_file (c, req_id, len, 0, 0);
    case TL_STORAGE_GET_PART: return rpcs_execute_get_file (c, req_id, len, 1, 1);
    case TL_STORAGE_GET_PART_INDIRECT: return rpcs_execute_get_file (c, req_id, len, 0, 1);
    case TL_STORAGE_GET_VOLUMES: return rpcs_execute_get_volumes (c, req_id, len);
    default:
      advance_skip_read_ptr (&c->In, len);
      return -__LINE__;
  }
}

int rpcc_execute (struct connection *c, int op, int len) {
  vkprintf (1, "rpcc_execute: fd=%d, op=%x, len=%d\n", c->fd, op, len);
  return SKIP_ALL_BYTES;
}

int rpcc_ready (struct connection *c) {
  int r = forward_query_check_all_completions ();
  if (r > 0) {
    vkprintf (3, "rpcc_ready: forward_query_check_all_completions () returns %d.\n", r);
  }
  return 0;
}

int storage_prepare_stats (struct connection *c) {
  stats_buffer_t sb;
  sb_prepare (&sb, c, stats_buff, STATS_BUFF_SIZE);
  sb_memory (&sb, AM_GET_MEMORY_USAGE_SELF + AM_GET_MEMORY_USAGE_OVERALL);

  SB_PRINT_TIME(booting_time);
  SB_PRINT_TIME(scandir_time);
  SB_PRINT_TIME(reoder_binlog_files_time);
  SB_PRINT_TIME(open_replicas_time);
  SB_PRINT_TIME(binlog_index_loading_time);
  SB_PRINT_TIME(append_to_binlog_time);
  SB_PRINT_TIME(binlog_load_time);
  SB_PRINT_TIME(index_load_time);
  sb_printf (&sb,
      "volumes\t%d\n"
      "binlog_disabled\t%d\n"
      "index_loaded_bytes\t%lld\n"
      "index_size\t%lld\n"
/*
      "binlog_original_size\t%lld\n"
      "binlog_loaded_bytes\t%lld\n"
      "binlog_load_time\t%.6fs\n"
      "current_binlog_size\t%lld\n"
      "binlog_uncommitted_bytes\t%d\n"
      "binlog_path\t%s\n"
      "binlog_first_timestamp\t%d\n"
      "binlog_read_timestamp\t%d\n"
      "binlog_last_timestamp\t%d\n"
      "index_loaded_bytes\t%lld\n"
      "index_size\t%lld\n"
      "index_path\t%s\n"
      "index_load_time\t%.6fs\n"
*/
      "cmd_version\t%lld\n"
      "cmd_stats\t%lld\n"
      "tree_nodes\t%d\n"
      "tot_docs\t%lld\n"
      "idx_docs\t%lld\n"
      "md5_mode\t%d\n"
      "max_immediately_reply_filesize\t%d\n"
      "metafiles\t%d\n"
      "metafiles_unloaded\t%lld\n"
      "metafiles_load_errors\t%lld\n"
      "metafiles_crc32_errors\t%lld\n"
      "metafiles_cancelled\t%lld\n"
      "choose_reading_binlog_errors\t%lld\n"
      "metafiles_bytes\t%d\n"
      "max_metafiles_bytes\t%d\n"
      "tot_aio_queries\t%lld\n"
      "active_aio_queries\t%lld\n"
      "expired_aio_queries\t%lld\n"
      "avg_aio_query_time\t%.6f\n"
      "aio_bytes_loaded\t%lld\n"
      "aio_query_timeout\t%.3lfs\n"
      "metafiles_cache_hits\t%lld\n",
    volumes,
    binlog_disabled,
    snapshot_size,
    index_size,
/*
    binlog_loaded_size,
    log_readto_pos - jump_log_pos,
    binlog_load_time,
    log_pos,
    log_uncommitted,
    binlogname ? (sizeof(binlogname) < 250 ? binlogname : "(too long)") : "(none)",
    log_first_ts,
    log_read_until,
    log_last_ts,
		idx_loaded_bytes,
		engine_snapshot_size,
    engine_snapshot_name,
    index_load_time,
*/
    cmd_version,
    cmd_stats,
    alloc_tree_nodes,
    tot_docs,
    idx_docs,
    md5_mode,
    max_immediately_reply_filesize,
    metafiles,
    metafiles_unloaded,
    metafiles_load_errors,
    metafiles_crc32_errors,
    metafiles_cancelled,
    choose_reading_binlog_errors,
    metafiles_bytes,
    max_metafiles_bytes,
    tot_aio_queries,
    active_aio_queries,
    expired_aio_queries,
    safe_div (total_aio_time, tot_aio_queries),
    tot_aio_loaded_bytes,
    aio_query_timeout_value,
    metafiles_cache_hits
    );
  SB_PRINT_I32(max_aio_connections_per_disk);

  SB_PRINT_QUERIES(http_queries);
  SB_PRINT_QUERIES(get_queries);
  SB_PRINT_QUERIES(get_file_queries);
  SB_PRINT_QUERIES(get_hide_queries);
  SB_PRINT_I64(get_misses);
  SB_PRINT_I64(get_volume_misses);

  SB_PRINT_I64(x_accel_redirects);
  SB_PRINT_I64(one_pix_transparent_errors);
  SB_PRINT_I64(too_many_aio_connections_errors);
  SB_PRINT_I64(redirect_retries);
  SB_PRINT_I64(redirect_retries_meta_aio);
  SB_PRINT_I64(redirect_retries_corrupted);
  SB_PRINT_I64(redirect_retries_secret);
  SB_PRINT_I64(redirect_retries_type);
  SB_PRINT_I64(redirect_retries_content_type);
  SB_PRINT_I64(redirect_retries_local_id);

  SB_PRINT_I64(statvfs_calls);

  SB_PRINT_I32(active_gathers);
  SB_PRINT_I64(gather_timeouts);
  SB_PRINT_I64(tot_aio_fsync_queries);
  SB_PRINT_I32(bad_image_cache_max_living_time);

  sb_printf (&sb, "choose_binlog_options\t%s\n", choose_binlog_options);
  sb_printf (&sb, "version\t%s\n", FullVersionStr);

  return sb.pos;
}

int sfd, http_sfd = -1, http_port;

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
  if (verbosity > 0) {
    fprintf (stderr, "logs reopened.\n");
  }
}

volatile int sigpoll_cnt;
static void sigpoll_handler (const int sig) {
  sigpoll_cnt++;
  signal (SIGPOLL, sigpoll_handler);
}

static void sigrtmax_handler (const int sig) {
  static const char message[] = "got SIGUSR3, write index.\n";
  kwrite (2, message, sizeof (message) - (size_t)1);
  force_write_index = 1;
}

static void sigint_handler (const int sig) {
  static const char message[] = "SIGINT handled.\n";
  kwrite (2, message, sizeof (message) - (size_t)1);
  force_interrupt = 1;
  pending_signals |= 1 << sig;
}

static void sigterm_handler (const int sig) {
  static const char message[] = "SIGTERM handled.\n";
  kwrite (2, message, sizeof (message) - (size_t)1);
  force_interrupt = 1;
  pending_signals |= 1 << sig;
}

static void sighup_handler (const int sig) {
  static const char message[] = "got SIGHUP.\n";
  kwrite (2, message, sizeof (message) - (size_t)1);
/*
  sync_binlog (2);
*/
  signal (SIGHUP, sighup_handler);
}

volatile int force_reopen_logs = 0;

static void sigusr1_handler (const int sig) {
  static const char message[] = "got SIGUSR1.\n";
  kwrite (2, message, sizeof (message) - (size_t)1);
/*
  sync_binlog (2);
*/
  force_reopen_logs++;
  signal (SIGUSR1, sigusr1_handler);
}

struct aiocb aio_fsync_cbp = {
  .aio_fildes = -1,
};

static storage_binlog_file_t *last_fsync_binlog_file;
int fsync_volume_idx = 0, fsync_binlog_idx = 0;

int get_aio_inprogress (void) {
  if (aio_fsync_cbp.aio_fildes < 0) {
    return 0;
  }
  int err = aio_error (&aio_fsync_cbp);
  if (err == EINPROGRESS) {
    return 1;
  }
  int res = aio_return (&aio_fsync_cbp);
  if (res < 0) {
    vkprintf (0, "aio_fsync (%s) fails. %s\n", last_fsync_binlog_file->abs_filename, strerror (res));
    update_binlog_fsync_stat (last_fsync_binlog_file, 0);
  } else {
    update_binlog_fsync_stat (last_fsync_binlog_file, 1);
  }
  aio_fsync_cbp.aio_fildes = -1;
  return 0;
}
static int last_fsync_step_time;
static void fsync_step (void) {
  if (binlog_disabled || get_aio_inprogress ()) {
    return;
  }
  if (now - last_fsync_step_time < fsync_step_delay) {
    return;
  }
  last_fsync_step_time = now;
  storage_binlog_file_t *B;
  while (1) {
    B = dirty_binlog_queue_pop ();
    if (B == NULL) {
      return;
    }
    if (B->fd_wronly >= 0) {
      break;
    }
  }

  aio_fsync_cbp.aio_fildes = B->fd_wronly;
  aio_fsync_cbp.aio_sigevent.sigev_notify = SIGEV_NONE;
  aio_fsync (O_SYNC, &aio_fsync_cbp);
  tot_aio_fsync_queries++;
  last_fsync_binlog_file = B;
}

int child_pid;

void check_child_status (void) {
  if (!child_pid) {
    return;
  }
  int status = 0;
  int res = waitpid (child_pid, &status, WNOHANG);
  if (res == child_pid) {
    if (WIFEXITED (status) || WIFSIGNALED (status)) {
      vkprintf (1, "child process %d terminated: exited = %d, signaled = %d, exit code = %d\n",
                   child_pid, WIFEXITED (status) ? 1 : 0, WIFSIGNALED (status) ? 1 : 0, WEXITSTATUS (status));
      child_pid = 0;
    }
  } else if (res == -1) {
    if (errno != EINTR) {
      kprintf ("waitpid (%d): %m\n", child_pid);
      child_pid = 0;
    }
  } else if (res) {
    kprintf ("waitpid (%d) returned %d???\n", child_pid, res);
  }
}
static int save_index_process = 0;
void fork_write_index (void) {
  if (child_pid) {
    vkprintf (1, "process with pid %d already generates index, skipping\n", child_pid);
    return;
  }
  if (active_gathers > 0) {
    vkprintf (1, "active_gathers = %d, skipping(waiting) generating index\n", active_gathers);
    return;
  }
  //flush_binlog_ts ();
  int res = fork ();
  if (res < 0) {
    kprintf ("fork: %m\n");
  } else if (!res) {
    int i, res = 0;
    //binlogname = 0;
    save_index_process = 1;
    for (i = 0; i < volumes; i++) {
      volume_t *V = Volumes[i];
      if (save_index (V)) {
        res |= 1;
      }
    }
    exit (res);
  } else {
    vkprintf (1, "created child process pid = %d\n", res);
    child_pid = res;
  }
  force_write_index = 0;
}

void cron (void) {
  storage_volumes_relax_astat ();
  create_all_outbound_connections ();
  fsync_step ();

  if (force_write_index) {
    fork_write_index ();
  }
  check_child_status ();
  dyn_garbage_collector ();
}

void start_server (void) {
  int i;
  int prev_time;
  int old_reopen_logs = 0;

  init_epoll();
  init_netbuffers();

  prev_time = 0;

  if (daemonize) {
    setsid ();
    reopen_logs ();
  }

/*
  for (i = 0; i < volumes; i++) {
    storage_open_binlogs (Volumes[i]);
  }
*/
  append_to_binlog_time = -mytime ();
  if (!binlog_disabled) {
    for (i = 0; i < volumes; i++) {
      int r = storage_append_to_binlog (Volumes[i]);
      if (r < 0) {
        kprintf ("[v%lld] storage_append_to_binlog returns error code %d.\n", Volumes[i]->volume_id, r);
        exit (1);
      }
    }
  }
  append_to_binlog_time += mytime ();

  struct connection *c = &Connections[0];
  c->pending_queries = 0x7fffffff;

  init_listening_connection (sfd, &ct_rpc_server, &storage_rpc_server);

  if (http_sfd >= 0) {
    init_listening_connection (http_sfd, &ct_http_server, &http_methods);
  }

  signal (SIGINT, sigint_handler);
  signal (SIGTERM, sigterm_handler);
  signal (SIGUSR1, sigusr1_handler);
  signal (SIGPIPE, SIG_IGN);
  signal (SIGIO, SIG_IGN);
  signal (SIGPOLL, sigpoll_handler);
  signal (SIGRTMAX, sigrtmax_handler);
  if (daemonize) {
    signal (SIGHUP, sighup_handler);
  }

  int quit_steps = 0;

  booting_time += mytime ();

  for (i = 0; ; i++) {
    if (!(i & 255)) {
      vkprintf (1, "epoll_work(): %d out of %d connections, network buffers: %d used, %d out of %d allocated\n",
	       active_connections, maxconn, NB_used, NB_alloc, NB_max);
    }
    epoll_work (57);

    if (force_interrupt) {
      if (!save_index_process) {
        kprintf ("Waiting write threads terminates.\n");
        int k = 0;
        while (active_write_threads != NULL) {
          sleep (1);
          k += write_thread_check_all_completions ();
        }
        kprintf ("%d threads has been terminated.\n", k);
        kprintf ("Start sync.\n");
        sync ();
        kprintf ("Sync has been completed.\n");
      }
      exit (0);
    }

    if (force_reopen_logs != old_reopen_logs) {
      old_reopen_logs = force_reopen_logs;
      kprintf ("reopen_logs (), force_reopen_logs counter is equal to %d.\n", old_reopen_logs);
      reopen_logs ();
    }

    if (sigpoll_cnt > 0) {
      vkprintf (2, "after epoll_work(), sigpoll_cnt=%d\n", sigpoll_cnt);
      sigpoll_cnt = 0;
    }

    /* !!! */
    check_all_aio_completions ();
    write_thread_check_all_completions ();
    forward_query_check_all_completions ();

    if (now != prev_time) {
      prev_time = now;
      cron();
    }
    if (quit_steps && !--quit_steps) break;
  }

  epoll_close (sfd);
  close (sfd);

}

/*
 *
 *		MAIN
 *
 */


void usage (void) {
  printf ("usage: %s [-v] [-r] [-i] [-p<port>] [-H<http-port>] [-n<nginx-port>] [-u<username>] [-g<groupname>] [-c<max-conn>] <binlogs-dirname>]\n"
    "\t%s\n"
	  "\t-i\tindex mode (docs bodies only in binlog)\n"
	  "\t-I<volume_id>\tsingle volume index mode\n"
	  "\t-v\toutput statistical and debug information into stderr\n"
	  "\t-r\tread-only binlog (don't log new events)\n"
	  "\t-a\tenable attachment file ([?|&]dl=1)\n"
    "\t-R<filesize>\tsets max_immediately_reply_filesize, could be end by 'k', 'm', etc. (default: %d)\n"
    "\t-M<max_metafiles_size>\tcould be end by 'k', 'm', etc. (default: %d)\n"
    "\t-Z<max_zmalloc_memory>\tcould be end by 'k', 'm', etc. (default: %d)\n"
    "\t\tzmalloc memory used only for aio_connections\n"
    "\t-T<aio_query_timeout>\tset aio query timeout (default: %.3lf)\n"
    "\t-F\tdisable crc32 check after loading metafile\n"
    "\t-V<required-volumes-number-at-startup>\t(default: %d)\n"
    "\t-L<bad-image-cache-max_living-time>\t(default: %ds)\n"
    "\t-A<max_aio_read_connection>\tlimit number of aio read connection for one disk (default: 0 - no limit)\n"
    "\t-C<choose_binlog_criterions>\t(default: '%s')\n"
    "\t\t\t's' - minimal consecutive file failures,\n"
    "\t\t\t'a' - minimal aio read connections for disk,\n"
    "\t\t\t'h' - minimal amortization hour file failures,\n"
    "\t\t\t't' - minimal total file failures)\n"
    "\t-E<N,cs,md5_mode,prefix>\tcreate N empty binlogs and write config-file\n"
    "\t\t\t\t(volume_id = cs * 1000 + log_split_min)\n",
	  progname,
    FullVersionStr,
    max_immediately_reply_filesize,
    max_metafiles_bytes, max_zmalloc_bytes, aio_query_timeout_value, required_volumes_at_startup, bad_image_cache_max_living_time, choose_binlog_options);
  exit (2);
}

void scan_dir (const char *const path, int dir_id) {
  Dirs[dir_id].path = zstrdup (path);
  Dirs[dir_id].scanned = 0;
  int r = storage_scan_dir (dir_id);
  if (r < 0) {
    kprintf ("storage_scan_dir (%d) returns error code %d.\n", dir_id, r);
  }
}

int main (int argc, char *argv[]) {
  char c;
  long long x;
  double d;
  int i, j;
  int NVOLUMES = 0;
  set_debug_handlers ();
  char *prefix = NULL;
  progname = strrchr (argv[0], '/');
  progname = (progname == NULL) ? argv[0] : progname + 1;
  while ((i = getopt (argc, argv, "A:C:E:FH:I:L:M:R:T:V:Z:b:c:dg:hil:n:p:ru:v:a")) != -1) {
    switch (i) {
      case 'A':
        max_aio_connections_per_disk = atoi (optarg);
        if (max_aio_connections_per_disk < 0) {
          kprintf ("invalid -%c option (%s), max_aio_connections should be not negative.\n", i, optarg);
          usage ();
        }
      break;
      case 'C':
        choose_binlog_options = optarg;
      break;
      case 'E':
        if (sscanf (optarg, "%d,%d,%d,%1023s", &i, &cs_id, &md5_mode, value_buff) == 4 && i >= 1 && i <= MAX_VOLUMES) {
          NVOLUMES = i;
          prefix = strdup (value_buff);
        }
        break;
      case 'F':
        use_crc32_check = 0;
        break;
      case 'H':
        http_port = atoi (optarg);
        break;
      case 'I':
        if (sscanf (optarg, "%lld", &x) == 1) {
          index_volume_id = x;
        }
        index_mode = 1;
        break;
      case 'L':
        j = atoi (optarg);
        if (j > bad_image_cache_min_living_time) {
          bad_image_cache_max_living_time = j;
        }
        break;
      case 'M':
      case 'R':
      case 'Z':
        c = 0;
        assert (sscanf (optarg, "%lld%c", &x, &c) >= 1);
        switch (c | 0x20) {
          case 'k':  x <<= 10; break;
          case 'm':  x <<= 20; break;
          case 'g':  x <<= 30; break;
          case 't':  x <<= 40; break;
          default: assert (c == 0x20);
        }
        if (i == 'R' && x >= 1 && x <= (1LL << 20)) {
          max_immediately_reply_filesize = x;
        }
        if (i == 'M' && x >= (1 << 20) && x <= (1 << 30)) {
          max_metafiles_bytes = x;
        }
        if (i == 'Z' && x >= (1 << 20) && x <= (1 << 30)) {
          max_zmalloc_bytes = x;
        }
        break;
      case 'T':
        errno = 0;
        d = strtod (optarg, NULL);
        if (!errno && d >= 0.5) {
          if (d > 30.0) {
            d = 30.0;
          }
          aio_query_timeout_value = d;
        }
        break;
      case 'V':
        i = atoi (optarg);
        if (i >= 0) {
          required_volumes_at_startup = i;
        }
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
      case 'd':
        daemonize ^= 1;
        break;
      case 'g':
        groupname = optarg;
        break;
      case 'h':
        usage ();
        return 2;
      case 'i':
        index_mode = 1;
        break;
      case 'a':
        attachment = 1;
        break;
      case 'l':
        logname = optarg;
        break;
      case 'n':
        NGINX_PORT = atoi (optarg);
        break;
      case 'p':
        port = atoi(optarg);
        break;
      case 'r':
        binlog_disabled = 1;
        break;
      case 'u':
        username = optarg;
        break;
      case 'v':
        verbosity++;
        break;
      default:
        kprintf ("Unimplemented option '-%c'\n", i);
        usage ();
      break;
    }
  }

  if (!NVOLUMES && argc < optind + 1) {
    usage ();
    return 2;
  }

  if (strlen (argv[0]) >= 5 && memcmp (argv[0] + strlen (argv[0]) - 5, "index" , 5) == 0) {
    index_mode = 1;
  }

  if (raise_file_rlimit (maxconn + 16) < 0) {
    fprintf (stderr, "fatal: cannot raise open file limit to %d\n", maxconn + 16);
    exit (1);
  }

  if (NVOLUMES) {
    int r = make_empty_binlogs (NVOLUMES, prefix, md5_mode, cs_id);
    if (r < 0) {
      kprintf ("make_empty_binlogs (%d, %s, %s) return exit code %d\n", NVOLUMES, prefix, argv[optind], r);
      exit (1);
    }
    free (prefix);
    exit (0);
  }

  booting_time = -mytime ();

  aes_load_pwd_file (0);

  if (change_user_group (username, groupname) < 0) {
    kprintf ("fatal: cannot change user to %s, group to %s\n", username ? username : "(none)", groupname ? groupname : "(none)");
    exit (1);
  }

  //dynamic_data_buffer_size = 0x7fffffff - max_metafiles_bytes - 2 * max_immediately_reply_filesize;
  dynamic_data_buffer_size = max_zmalloc_bytes;
  init_dyn_data ();
  vkprintf (4, "dyn_first: %p\n", dyn_first);

  if (storage_parse_choose_binlog_option (choose_binlog_options) < 0) {
    kprintf ("Parsing choose binlog options failed.\n");
    usage ();
  }

  if (!index_mode) {
    sfd = server_socket (port, settings_addr, backlog, 0);
    if (sfd < 0) {
      kprintf ("cannot open server socket at port %d: %m\n", port);
      exit (1);
    }
    if (http_port > 0 && http_sfd < 0) {
      http_sfd = server_socket (http_port, settings_addr, backlog, 0);
      if (http_sfd < 0) {
        kprintf ("cannot open http server socket at port %d: %m\n", http_port);
        exit (1);
      }
    }
  }

  dirs = argc - optind;
  if (dirs > MAX_DIRS) {
    kprintf ("too many directories (MAX_DIRS = %d)\n", MAX_DIRS);
    exit (1);
  }

  for (i = optind; i < argc; i++) {
    for (j = i + 1; j < argc; j++) {
      if (strcmp (argv[i], argv[j]) > 0) {
        char *t = argv[i]; argv[i] = argv[j]; argv[j] = t;
      }
    }
  }

  scandir_time = -mytime ();
  for (i = optind; i < argc; i++) {
    scan_dir (argv[i], i - optind);
  }
  scandir_time += mytime ();

  if (!volumes) {
    kprintf ("No binglogs found.\n");
    exit (1);
  }

  generate_sorted_volumes_array ();
  if (volumes < required_volumes_at_startup) {
    kprintf ("Found %d volumes instead of %d. You could decrease required at startup volumes number using -V switch.\n", volumes, required_volumes_at_startup);
    exit (1);
  }

  volume_t *V;

  vkprintf (1, "Found %d different volume_id.\n", volumes);
  int k;

  reoder_binlog_files_time = -mytime ();
  for (k = 0; k < volumes; k++) {
    storage_reoder_binlog_files (Volumes[k]);
  }
  reoder_binlog_files_time += mytime ();

  open_replicas_time = -mytime ();
  storage_open_replicas ();
  open_replicas_time += mytime ();

  if (index_volume_id) {
    for (k = 0; k < volumes; k++) {
      if (index_volume_id == Volumes[k]->volume_id) {
        break;
      }
    }
    if (k >= volumes) {
      kprintf ("volume_id %lld wasn't found.\n", index_volume_id);
      exit (1);
    }
  }

  binlog_index_loading_time = -mytime ();
  for (k = 0; k < volumes; k++) {
    V = Volumes[k];
    if (index_volume_id && index_volume_id != V->volume_id) {
      continue;
    }
    V->Snapshot = open_recent_snapshot (V->engine_snapshot_replica);
    V->index_load_time = -mytime();
    i = load_index (V);
    if (i < 0) {
      kprintf ("[v%lld] load_index returned fail code %d.\n", V->volume_id, i);
      exit (1);
    }
    close_snapshot (V->Snapshot, 1);
    V->Snapshot = NULL;
    V->index_load_time += mytime();
    index_load_time += V->index_load_time;
    vkprintf (1, "[v%lld] load index finished (%.6lf seconds), %lld bytes read\n", V->volume_id, V->index_load_time, V->snapshot_size);

    V->binlog_load_time = -mytime ();
    V->Binlog = open_binlog (V->engine_replica, 0);
    if (!V->Binlog) {
      kprintf ("fatal: cannot find binlog for %s, log position 0\n", V->engine_replica->replica_prefix);
      exit (1);
    }
    vkprintf (3, "V->Binlog->fd = %d, V->jump_log_pos = %lld\n", V->Binlog->fd, V->jump_log_pos);
    i = storage_replay_log (V, V->jump_log_pos);
    if (i < 0) {
      kprintf ("strorage_replay_log returns error code %d\n", i);
      exit (1);
    }
    V->binlog_load_time += mytime ();
    binlog_load_time += V->binlog_load_time;
    close_binlog (V->Binlog, 1);
    if (index_mode) {
      int r = save_index (V);
      if (r < 0) {
        kprintf ("save_index (volume: %lld) fail with exit code %d.\n", V->volume_id, r);
        exit (1);
      }
    }
    //close_replica (V->engine_replica);
  }
  binlog_index_loading_time += mytime ();
  md5_mode = Volumes[0]->md5_mode;
  for (k = 1; k < volumes; k++) {
    if (Volumes[k]->md5_mode != md5_mode) {
      kprintf ("Volumes[%d].md5_mode(%d) != %d\n", k, Volumes[k]->md5_mode, md5_mode);
      exit (1);
    }
  }

  if (index_mode) {
    return 0;
  }

  start_time = time (0);

  start_server ();

  return 0;
}

