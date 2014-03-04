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

    Copyright 2012-2013 Vkontakte Ltd
              2012-2013 Arseny Smirnov
              2012-2013 Aliaksei Levin
*/

#define _FILE_OFFSET_BITS 64

#include "php-queries.h"
#include "php-runner.h"
#include "php_script.h"

#include "runtime/drivers.h"
#include "runtime/interface.h"

extern long long cur_qres_id, first_qres_id;


#define MAX_NET_ERROR_LEN 128

static char last_net_error[MAX_NET_ERROR_LEN + 1];

static void save_last_net_error (const char *s) {
  if (s == NULL) {
    last_net_error[0] = 0;
    return;
  }

  int l = strlen (s);
  if (l >= MAX_NET_ERROR_LEN) {
    l = MAX_NET_ERROR_LEN - 1;
  }
  memcpy (last_net_error, s, l);
  last_net_error[l] = 0;
}

const char *engine_get_last_net_error (void) {
  return last_net_error;
}

#undef MAX_NET_ERROR_LEN

/** create connection query **/
php_query_http_load_post_answer_t *php_query_http_load (char *buf, int min_len, int max_len) {
  assert (PHPScriptBase::is_running);

  //DO NOT use query after script is terminated!!!
  php_query_http_load_post_t q;
  q.base.type = PHPQ_HTTP_LOAD_POST;
  q.buf = buf;
  q.min_len = min_len;
  q.max_len = max_len;

  PHPScriptBase::current_script->ask_query ((void *)&q);

  return (php_query_http_load_post_answer_t *)q.base.ans;
}

int engine_http_load_long_query (char *buf, int min_len, int max_len) {
  php_query_http_load_post_answer_t *ans = php_query_http_load (buf, min_len, max_len);
  assert (min_len <= ans->loaded_bytes && ans->loaded_bytes <= max_len);
  return ans->loaded_bytes;
}


/***
 QUERY MEMORY ALLOCATOR
 ***/

#include <map>

#include <cassert>

long long qmem_generation = 0;

namespace qmem {
  // constants
  const int static_pages_n = 2;
  const size_t page_size = (1 << 22), max_mem = (1 << 27);
  const int max_pages_n = 1000;
  char static_memory[static_pages_n][page_size];

  //variables
  void *pages[max_pages_n];
  size_t pages_size[max_pages_n], cur_mem, used_mem;
  int pages_n;

  typedef enum {st_empty, st_inited} state_t;
  state_t state;

  std::multimap <size_t, void *> left;

  //private functions
  inline void reg (void *ptr, size_t ptr_size) {
    left.insert (std::make_pair (ptr_size, ptr));
  }

  void *alloc_at_least (size_t n, size_t use) {

    size_t alloc_size = n >= page_size ? n : page_size;

    if (pages_n == max_pages_n || alloc_size > max_mem || alloc_size + cur_mem > max_mem) {
      assert ("NOT ENOUGH MEMORY\n");
      return NULL;
    }

    void *ptr = pages[pages_n] = malloc (alloc_size);
    assert (ptr != NULL);

    pages_size[pages_n] = alloc_size;

    if (use < alloc_size) {
      reg ((char *)ptr + use, alloc_size - use);
    }

    pages_n++;
    return ptr;
  }
}

//public functions
void qmem_init (void) {
  using namespace qmem;

  assert (state == st_empty);

  assert (left.empty());

  cur_mem = 0;
  pages_n = static_pages_n;
  for (int i = 0; i < static_pages_n; i++) {
    pages[i] = static_memory[i];
    pages_size[i] = page_size;
    cur_mem += page_size;

    reg (pages[i], pages_size[i]);
  }

  state = st_inited;
}


void *qmem_malloc (size_t n) {
  using namespace qmem;

  std::multimap<size_t, void *>::iterator i = left.lower_bound (n);
  used_mem += n;
  if (i == left.end()) {
    return alloc_at_least (n, n);
  }

  void *ptr = i->second;
  size_t ptr_size = i->first;

  left.erase (i);

  ptr_size -= n;
  if (ptr_size > 0) {
    reg ((char *)ptr + n, ptr_size);
  }

  return ptr;
}

void *qmem_malloc_tmp (size_t n) {
  using namespace qmem;

  std::multimap<size_t, void *>::iterator i = left.lower_bound (n);
  if (i == left.end()) {
    return alloc_at_least (n, 0);
  }

  void *ptr = i->second;

  return ptr;
}

void *qmem_malloc0 (size_t n) {
  void *res = qmem_malloc (n);
  if (res != NULL) {
    memset (res, 0, n);
  }
  return res;
}


void qmem_free_ptrs (void) {
  using namespace qmem;

  if (used_mem + used_mem > cur_mem) {
    left.clear();
    used_mem = 0;
    for (int i = 0; i < pages_n; i++) {
      reg (pages[i], pages_size[i]);
    }
  }

  qmem_generation++;
}

void qmem_clear (void) {
  using namespace qmem;

  assert (state == st_inited);

  left.clear();
  used_mem = 0;
  for (int i = static_pages_n; i < pages_n; i++) {
    free (pages[i]);
  }

  state = st_empty;

  qmem_generation++;
}

/** qmem_pstr **/
const char* qmem_pstr (char const *msg, ...) {
  const int maxlen = 5000;
  static char s[maxlen];
  va_list args;

  va_start (args, msg);
  int len = vsnprintf (s, maxlen, msg, args);
  va_end (args);

  if (len >= maxlen) {
    len = maxlen - 1;
  }

  char *res = (char *)qmem_malloc (len + 1);
  memcpy (res, s, len);
  res[len] = 0;

  return res;
}

/** str_buffer **/

str_buf_t *str_buf_create (void) {
  str_buf_t *buf = (str_buf_t *)qmem_malloc (sizeof (str_buf_t));
  assert (buf != NULL);
  buf->buf_len = 0;
  buf->len = 0;

  return buf;
}

void str_buf_append (str_buf_t *buf, data_reader_t *reader) {
  int need = reader->len + buf->len;
  if (need >= buf->buf_len) {
    need = need * 2 + 1;
    char *new_buf = (char *)qmem_malloc (need);
    assert (new_buf != NULL);
    memcpy (new_buf, buf->buf, buf->len);
    buf->buf = new_buf;
    buf->buf_len = need;
  }

  reader->read (reader, buf->buf + buf->len);
  buf->len += reader->len;
}

char *str_buf_cstr (str_buf_t *buf) {
  buf->buf[buf->len] = 0;
  return buf->buf;
}

int str_buf_len (str_buf_t *buf) {
  return buf->len;
}

/** chain **/
void chain_conn (chain_t *a, chain_t *b) {
  a->next = b;
  b->prev = a;
}

chain_t *chain_create (void) {
  chain_t *chain = (chain_t *)qmem_malloc (sizeof (chain_t));
  assert (chain != NULL);
  chain_conn (chain, chain);
  return chain;
}

void chain_append (chain_t *chain, data_reader_t *reader) {
  chain_t *node = (chain_t *)qmem_malloc (sizeof (chain_t));
  assert (node != NULL);

  node->buf = (char *)qmem_malloc (reader->len);
  assert (node->buf != NULL);
  node->len = reader->len;
  reader->read (reader, node->buf);

  chain_conn (chain->prev, node);
  chain_conn (node, chain);
}

/***
  QUERIES
 ***/

/** test x^2 query **/
php_query_x2_answer_t *php_query_x2 (int x) {
  assert (PHPScriptBase::is_running);

  //DO NOT use query after script is terminated!!!
  php_query_x2_t q;
  q.base.type = PHPQ_X2;
  q.val = x;

  PHPScriptBase::current_script->ask_query ((void *)&q);

  return (php_query_x2_answer_t *)q.base.ans;
}

/** create connection query **/
php_query_connect_answer_t *php_query_connect (const char *host, int port, protocol_t protocol) {
  assert (PHPScriptBase::is_running);

  //DO NOT use query after script is terminated!!!
  php_query_connect_t q;
  q.base.type = PHPQ_CONNECT;
  q.host = host;
  q.port = port;
  q.protocol = protocol;

  PHPScriptBase::current_script->ask_query ((void *)&q);

  return (php_query_connect_answer_t *)q.base.ans;
}

int engine_mc_connect_to (const char *host, int port) {
  php_query_connect_answer_t *ans = php_query_connect (host, port, p_memcached);
  return ans->connection_id;
}

int engine_db_proxy_connect (void) {
  php_query_connect_answer_t *ans = php_query_connect ("unknown", -1, p_sql);
  return ans->connection_id;
}

int engine_rpc_connect_to (const char *host, int port) {
  php_query_connect_answer_t *ans = php_query_connect (host, port, p_rpc);
  return ans->connection_id;
}

/** net query **/


php_net_query_packet_answer_t *php_net_query_packet (
    int connection_id, const char *data, int data_len,
    int timeout_ms, protocol_t protocol, int extra_type) {
  php_net_query_packet_t q;
  q.base.type = PHPQ_NETQ | NETQ_PACKET;

  q.connection_id = connection_id;
  q.data = data;
  q.data_len = data_len;
  q.timeout = timeout_ms * 0.001;
  q.protocol = protocol;
  q.extra_type = extra_type;

  PHPScriptBase::current_script->ask_query ((void *)&q);

  return (php_net_query_packet_answer_t *)q.base.ans;
}

//typedef enum {st_ansgen_done, st_ansgen_error, st_ansgen_wait} ansgen_state_t;
//typedef enum {ap_any, ap_get, ap_err, ap_done} mc_ansgen_packet_state_t;
//typedef enum {nq_error, nq_ok} nq_state_t;

void read_str (data_reader_t *reader, void *dest) {
  //reader->readed = 1;
  memcpy (dest, reader->extra, reader->len);
}

/** net answer generator **/

bool net_ansgen_is_alive (net_ansgen_t *base_self) {
  return base_self->qmem_req_generation == qmem_generation;
}

void net_ansgen_timeout (net_ansgen_t *base_self) {
//  fprintf (stderr, "mc_ansgen_packet_timeout %p\n", base_self);

  assert (base_self->state == st_ansgen_wait);

  if (net_ansgen_is_alive (base_self)) {
    base_self->ans->state = nq_error;
    base_self->ans->res = "Timeout";
    base_self->qmem_req_generation = -1;
  }
}


void net_ansgen_error (net_ansgen_t *base_self, const char *val) {
//  fprintf (stderr, "mc_ansgen_packet_error %p\n", base_self);

  assert (base_self->state == st_ansgen_wait);

  if (net_ansgen_is_alive (base_self)) {
    base_self->ans->state = nq_error;
    base_self->ans->res = val;
  }

  base_self->state = st_ansgen_error;
}

void net_ansgen_set_desc (net_ansgen_t *base_self, const char *val) {
  if (net_ansgen_is_alive (base_self)) {
    base_self->ans->desc = val;
  }
}


/** memcached answer generators **/

static data_reader_t end_reader, stored_reader, notstored_reader;
void init_reader (data_reader_t *reader, const char *s) {
  reader->len = (int)strlen (s);
  reader->extra = (void *)s;
  reader->readed = 0;
  reader->read = read_str;
}

void init_readers() {
  init_reader (&end_reader, "END\r\n");
  init_reader (&stored_reader, "STORED\r\n");
  init_reader (&notstored_reader, "NOT_STORED\r\n");
}

void mc_ansgen_packet_xstored (mc_ansgen_t *mc_self, int is_stored) {
  mc_ansgen_packet_t *self = (mc_ansgen_packet_t *)mc_self;
  net_ansgen_t *base_self = (net_ansgen_t *)mc_self;

  if (self->state == ap_any) {
    self->state = ap_store;
  }
  if (self->state != ap_store) {
    base_self->func->error (base_self, "Unexpected STORED");
  } else {
    if (net_ansgen_is_alive (base_self)) {
      str_buf_append (self->str_buf, is_stored ? &stored_reader : &notstored_reader);
      base_self->ans->state = nq_ok;
      base_self->ans->res = str_buf_cstr (self->str_buf);
      base_self->ans->res_len = str_buf_len (self->str_buf);
    }
    base_self->state = st_ansgen_done;
  }
}


void mc_ansgen_packet_value (mc_ansgen_t *mc_self, data_reader_t *reader) {
//  fprintf (stderr, "mc_ansgen_packet_value\n");
  mc_ansgen_packet_t *self = (mc_ansgen_packet_t *)mc_self;
  net_ansgen_t *base_self = (net_ansgen_t *)mc_self;

  if (self->state == ap_any) {
    self->state = ap_get;
  }
  if (self->state != ap_get) {
    base_self->func->error (base_self, "Unexpected VALUE");
  } else if (net_ansgen_is_alive (base_self)) {
    str_buf_append (self->str_buf, reader);
  }
}
void mc_ansgen_packet_version (mc_ansgen_t *mc_self, data_reader_t *reader) {
  mc_ansgen_packet_t *self = (mc_ansgen_packet_t *)mc_self;
  net_ansgen_t *base_self = (net_ansgen_t *)mc_self;

  if (self->state != ap_version) {
    return;
  }

  if (net_ansgen_is_alive (base_self)) {
    str_buf_append (self->str_buf, reader);
    base_self->ans->state = nq_ok;
    base_self->ans->res = str_buf_cstr (self->str_buf);
    base_self->ans->res_len = str_buf_len (self->str_buf);
  }
  base_self->state = st_ansgen_done;
}

void mc_ansgen_packet_set_query_type (mc_ansgen_t *mc_self, int query_type) {
  mc_ansgen_packet_t *self = (mc_ansgen_packet_t *)mc_self;
  net_ansgen_t *base_self = (net_ansgen_t *)mc_self;

  mc_ansgen_packet_state_t new_state = ap_any;
  if (query_type == 1) {
    new_state = ap_version;
  }
  if (new_state == ap_any) {
    return;
  }
  if (self->state == ap_any) {
    self->state = new_state;
  }
  if (self->state != new_state) {
    base_self->func->error (base_self, "Can't determine query type");
  }
}

void mc_ansgen_packet_other (mc_ansgen_t *mc_self, data_reader_t *reader) {
//  fprintf (stderr, "mc_ansgen_packet_value\n");
  mc_ansgen_packet_t *self = (mc_ansgen_packet_t *)mc_self;
  net_ansgen_t *base_self = (net_ansgen_t *)mc_self;

  if (self->state == ap_any) {
    self->state = ap_other;
  }
  if (self->state != ap_other) {
    base_self->func->error (base_self, "Unexpected \"other\" command");
  } else {
    if (net_ansgen_is_alive (base_self)) {
      str_buf_append (self->str_buf, reader);
      base_self->ans->state = nq_ok;
      base_self->ans->res = str_buf_cstr (self->str_buf);
      base_self->ans->res_len = str_buf_len (self->str_buf);
    }
    base_self->state = st_ansgen_done;
  }
}

void mc_ansgen_packet_end (mc_ansgen_t *mc_self) {
//  fprintf (stderr, "mc_ansgen_packet_end\n");
  mc_ansgen_packet_t *self = (mc_ansgen_packet_t *)mc_self;
  net_ansgen_t *base_self = (net_ansgen_t *)mc_self;

  assert (base_self->state == st_ansgen_wait);
  if (self->state == ap_any) {
    self->state = ap_get;
  }

  if (self->state != ap_get) {
    base_self->func->error (base_self, "Unexpected END");
  } else {
    if (net_ansgen_is_alive (base_self)) {
      str_buf_append (self->str_buf, &end_reader);

      base_self->ans->state = nq_ok;
      base_self->ans->res = str_buf_cstr (self->str_buf);
      base_self->ans->res_len = str_buf_len (self->str_buf);
    }
    base_self->state = st_ansgen_done;
  }

}

void mc_ansgen_packet_free (net_ansgen_t *base_self) {
  mc_ansgen_packet_t *self = (mc_ansgen_packet_t *)base_self;
  free (self);
}

net_ansgen_func_t *get_mc_net_ansgen_functions() {
  static bool inited = false;
  static net_ansgen_func_t f;
  if (!inited) {
    f.error = net_ansgen_error;
    f.timeout = net_ansgen_timeout;
    f.set_desc = net_ansgen_set_desc;
    f.free = mc_ansgen_packet_free;
    inited = true;
  }
  return &f;
}

mc_ansgen_func_t *get_mc_ansgen_functions() {
  static bool inited = false;
  static mc_ansgen_func_t f;
  if (!inited) {
    f.value = mc_ansgen_packet_value;
    f.end = mc_ansgen_packet_end;
    f.xstored = mc_ansgen_packet_xstored;
    f.other = mc_ansgen_packet_other;
    f.version = mc_ansgen_packet_version;
    f.set_query_type = mc_ansgen_packet_set_query_type;
    inited = true;
  }
  return &f;
}

mc_ansgen_t *mc_ansgen_packet_create (void) {
  mc_ansgen_packet_t *ansgen = (mc_ansgen_packet_t *)malloc (sizeof (mc_ansgen_packet_t));

  ansgen->base.func = get_mc_net_ansgen_functions();
  ansgen->func = get_mc_ansgen_functions();

  ansgen->base.qmem_req_generation = qmem_generation;
  ansgen->base.state = st_ansgen_wait;
  ansgen->base.ans = (php_net_query_packet_answer_t *)qmem_malloc0 (sizeof (php_net_query_packet_answer_t));
  assert (ansgen->base.ans != NULL);

  ansgen->state = ap_any;


  ansgen->str_buf = str_buf_create();

  return (mc_ansgen_t *)ansgen;
}

/*** sql answer generator ***/

void sql_ansgen_packet_set_writer (sql_ansgen_t *sql_self, command_t *writer) {
  sql_ansgen_packet_t *self = (sql_ansgen_packet_t *)sql_self;
  net_ansgen_t *base_self = (net_ansgen_t *)sql_self;

  assert (base_self->state == st_ansgen_wait);
  assert (self->state == sql_ap_init);

  self->writer = writer;
  self->state = sql_ap_wait_conn;
}

void sql_ansgen_packet_ready (sql_ansgen_t *sql_self, void *data) {
  sql_ansgen_packet_t *self = (sql_ansgen_packet_t *)sql_self;
  net_ansgen_t *base_self = (net_ansgen_t *)sql_self;

  assert (base_self->state == st_ansgen_wait);
  assert (self->state == sql_ap_wait_conn);

  if (self->writer != NULL) {
    self->writer->run (self->writer, data);
  }
  self->state = sql_ap_wait_ans;
}

void sql_ansgen_packet_add_packet (sql_ansgen_t *sql_self, data_reader_t *reader) {
  sql_ansgen_packet_t *self = (sql_ansgen_packet_t *)sql_self;
  net_ansgen_t *base_self = (net_ansgen_t *)sql_self;

  assert (base_self->state == st_ansgen_wait);
  assert (self->state == sql_ap_wait_ans);

  if (net_ansgen_is_alive (base_self)) {
    chain_append (self->chain, reader);
  }
}

void sql_ansgen_packet_done (sql_ansgen_t *sql_self) {
  sql_ansgen_packet_t *self = (sql_ansgen_packet_t *)sql_self;
  net_ansgen_t *base_self = (net_ansgen_t *)sql_self;

  assert (base_self->state == st_ansgen_wait);
  assert (self->state == sql_ap_wait_ans);

  if (net_ansgen_is_alive (base_self)) {
    base_self->ans->state = nq_ok;
    base_self->ans->chain = self->chain;
  }

  base_self->state = st_ansgen_done;
}

void sql_ansgen_packet_free (net_ansgen_t *base_self) {
  sql_ansgen_packet_t *self = (sql_ansgen_packet_t *)base_self;
  if (self->writer != NULL) {
    self->writer->free (self->writer);
    self->writer = NULL;
  }
  free (self);
}

net_ansgen_func_t *get_sql_net_ansgen_functions() {
  static bool inited = false;
  static net_ansgen_func_t f;
  if (!inited) {
    f.error = net_ansgen_error;
    f.timeout = net_ansgen_timeout;
    f.set_desc = net_ansgen_set_desc;
    f.free = sql_ansgen_packet_free;
    inited = true;
  }
  return &f;
}

sql_ansgen_func_t *get_sql_ansgen_functions() {
  static bool inited = false;
  static sql_ansgen_func_t f;
  if (!inited) {
    f.set_writer = sql_ansgen_packet_set_writer;
    f.ready = sql_ansgen_packet_ready;
    f.packet = sql_ansgen_packet_add_packet;
    f.done = sql_ansgen_packet_done;
    inited = true;
  }
  return &f;
}

sql_ansgen_t *sql_ansgen_packet_create (void) {
  sql_ansgen_packet_t *ansgen = (sql_ansgen_packet_t *)malloc (sizeof (sql_ansgen_packet_t));

  ansgen->base.func = get_sql_net_ansgen_functions();
  ansgen->func = get_sql_ansgen_functions();

  ansgen->base.qmem_req_generation = qmem_generation;
  ansgen->base.state = st_ansgen_wait;
  ansgen->base.ans = (php_net_query_packet_answer_t *)qmem_malloc0 (sizeof (php_net_query_packet_answer_t));
  assert (ansgen->base.ans != NULL);

  ansgen->state = sql_ap_init;
  ansgen->writer = NULL;


  ansgen->chain = chain_create();

  return (sql_ansgen_t *)ansgen;
}

/*** net_send generator ***/
void net_send_ansgen_free (net_ansgen_t *base_self) {
  net_send_ansgen_t *self = (net_send_ansgen_t *)base_self;
  if (self->writer != NULL) {
    self->writer->free (self->writer);
    self->writer = NULL;
  }
  free (self);
}


void net_send_ansgen_set_writer (net_send_ansgen_t *self, command_t *writer) {
  net_ansgen_t *base_self = (net_ansgen_t *)self;

  assert (base_self->state == st_ansgen_wait);
  //assert (self->state == sql_ap_init);

  self->writer = writer;
  //self->state = sql_ap_wait_conn;
}

void net_send_ansgen_send_and_finish (net_send_ansgen_t *self, void *data) {
  net_ansgen_t *base_self = (net_ansgen_t *)self;

  assert (base_self->state == st_ansgen_wait);
  //assert (self->state == sql_ap_wait_conn);

  if (self->writer != NULL) {
    self->writer->run (self->writer, data);
  }
  //self->state = sql_ap_wait_ans;

  if (net_ansgen_is_alive (base_self)) {
    base_self->ans->state = nq_ok;
    base_self->ans->result_id = self->qres_id;
  }

  base_self->state = st_ansgen_done;
}


net_ansgen_func_t *get_net_send_net_ansgen_functions() {
  static bool inited = false;
  static net_ansgen_func_t f;
  if (!inited) {
    f.error = net_ansgen_error;
    f.timeout = net_ansgen_timeout;
    f.set_desc = net_ansgen_set_desc;
    f.free = net_send_ansgen_free;
    inited = true;
  }
  return &f;
}

net_send_ansgen_func_t *get_net_send_ansgen_functions() {
  static bool inited = false;
  static net_send_ansgen_func_t f;
  if (!inited) {
    f.set_writer = net_send_ansgen_set_writer;
    f.send_and_finish = net_send_ansgen_send_and_finish;
    inited = true;
  }
  return &f;
}


net_send_ansgen_t *net_send_ansgen_create (void) {
  net_send_ansgen_t *ansgen = (net_send_ansgen_t *)malloc (sizeof (net_send_ansgen_t));

  ansgen->base.func = get_net_send_net_ansgen_functions();
  ansgen->func = get_net_send_ansgen_functions();

  ansgen->base.qmem_req_generation = qmem_generation;
  ansgen->base.state = st_ansgen_wait;
  ansgen->base.ans = (php_net_query_packet_answer_t *)qmem_malloc0 (sizeof (php_net_query_packet_answer_t));
  assert (ansgen->base.ans != NULL);

  ansgen->writer = NULL;

  return ansgen;
}

/*** net_get generator ***/
void net_get_ansgen_free (net_ansgen_t *base_self) {
  net_get_ansgen_t *self = (net_get_ansgen_t *)base_self;
  free (self);
}


void net_get_ansgen_answer (net_get_ansgen_t *self, qres_t *qres) {
  net_ansgen_t *base_self = (net_ansgen_t *)self;

  assert (base_self->state == st_ansgen_wait);
  //assert (self->state == sql_ap_wait_conn);

  if (qres == NULL || !qres_is_ready (qres)) {
    base_self->func->error (base_self, "No result found");
    return;
  }

  if (qres->type == qr_ans) {
    if (qres->state == qr_failed) {
      base_self->func->error (base_self, "No result found");
      return;
    }
    if (net_ansgen_is_alive (base_self)) {
      base_self->ans->state = nq_ok;
      base_self->ans->res = qres->data;
      base_self->ans->res_len = qres->data_len;
    } else {
      //fprintf (stderr, "qmem_gen [have = %lld] [need = %lld]\n", base_self->qmem_req_generation, qmem_generation);
      //fprintf (stderr, "%lld [%lld;%lld)\n", qres->id, first_qres_id, cur_qres_id);
      //assert ("dead ansgen and alive qres" && 0);
    }
  } else if (qres->type == qr_watchcat) {
    long long id = qres_next_id (qres);
    if (net_ansgen_is_alive (base_self)) {
      base_self->ans->state = nq_ok;
      base_self->ans->result_id = id;
    } else {
      //fprintf (stderr, "qmem_gen [have = %lld] [need = %lld]\n", base_self->qmem_req_generation, qmem_generation);
      //fprintf (stderr, "%lld [%lld;%lld)\n", qres->id, first_qres_id, cur_qres_id);
      //assert ("dead ansgen and alive qres" && 0);
    }
  } else {
    assert (0);
  }

  base_self->state = st_ansgen_done;
}

void net_get_ansgen_set_id (net_get_ansgen_t *self, long long request_id) {
  net_ansgen_t *base_self = (net_ansgen_t *)self;
  assert (base_self->state == st_ansgen_wait);
  self->request_id = request_id;

  qres_t *qres = get_qres (request_id, qr_ans);
  if (qres != NULL) {
    qres_readed (qres);
  }
}

double net_get_ansgen_try_wait (net_get_ansgen_t *self, double precise_now) {
  net_ansgen_t *base_self = (net_ansgen_t *)self;

  assert (base_self->state == st_ansgen_wait);

  qres_t *qres = get_qres (self->request_id, qr_any);

  if (qres == NULL) {
    base_self->func->error (base_self, "Invalid query id");
    return 0;
  }
  bool is_ready = qres_is_ready (qres);
  if (!is_ready && qres->timeout <= precise_now) {
    base_self->func->timeout (base_self);
    return 0;
  }

  if (is_ready) {
    self->func->answer (self, qres);
    return 0;
  }

  return qres->timeout;
}

void net_get_ansgen_done (net_get_ansgen_t *self) {
  net_ansgen_t *base_self = (net_ansgen_t *)self;

  assert (base_self->state == st_ansgen_wait);

  qres_t *qres = get_qres (self->request_id, qr_any);
  self->func->answer (self, qres);
}


net_ansgen_func_t *get_net_get_net_ansgen_functions() {
  static bool inited = false;
  static net_ansgen_func_t f;
  if (!inited) {
    f.error = net_ansgen_error;
    f.timeout = net_ansgen_timeout;
    f.set_desc = net_ansgen_set_desc;
    f.free = net_get_ansgen_free;
    inited = true;
  }
  return &f;
}

net_get_ansgen_func_t *get_net_get_ansgen_functions() {
  static bool inited = false;
  static net_get_ansgen_func_t f;
  if (!inited) {
    f.answer = net_get_ansgen_answer;
    f.set_id = net_get_ansgen_set_id;
    f.try_wait = net_get_ansgen_try_wait;
    f.done = net_get_ansgen_done;
    inited = true;
  }
  return &f;
}


net_get_ansgen_t *net_get_ansgen_create (void) {
  net_get_ansgen_t *ansgen = (net_get_ansgen_t *)malloc (sizeof (net_get_ansgen_t));

  ansgen->base.func = get_net_get_net_ansgen_functions();
  ansgen->func = get_net_get_ansgen_functions();

  ansgen->base.qmem_req_generation = qmem_generation;
  ansgen->base.state = st_ansgen_wait;
  ansgen->base.ans = (php_net_query_packet_answer_t *)qmem_malloc0 (sizeof (php_net_query_packet_answer_t));
  assert (ansgen->base.ans != NULL);

  ansgen->request_id = -1;

  return ansgen;
}

/*** results container ***/
const int max_result_n = 2000000;
qres_t *qresults[max_result_n];
void (*got_result) (long long id) = NULL;
long long cur_qres_id, first_qres_id;

long long next_qres_id (void) {
  if (cur_qres_id >= first_qres_id + max_result_n) {
    return -1;
  }
  return cur_qres_id++;
}

qres_t *create_qres (void) {
  long long id = next_qres_id();
  if (id < 0) {
    return NULL;
  }

  qres_t *qres = (qres_t *)calloc (sizeof (qres_t), 1);
  qresults[id - first_qres_id] = qres;

  qres->id = id;

  return qres;
}

qres_t *get_qres (long long qres_id, qres_type_t type) {
  long long id = qres_id - first_qres_id;
  if (id >= max_result_n || id < 0) {
    return NULL;
  }

  qres_t *res = qresults[id];
  if (res == NULL || !(res->type & type)) {
    return NULL;
  }

  return res;
}


long long create_qres_ans (void) {
  qres_t *qres = create_qres();
  if (qres == NULL) {
    return -1;
  }

  qres->type = qr_ans;
  qres->state = qr_wait;

  return qres->id;
}

void qres_conn (qres_t *a, qres_t *b) {
  a->next = b;
  b->prev = a;
}

long long create_qres_watchcat (long long *ids, int ids_n) {
  qres_t *qres = create_qres();
  if (qres == NULL) {
    return -1;
  }

  qres->type = qr_watchcat;

  qres_conn (qres, qres);

  for (int i = 0; i < ids_n; i++) {
    qres_t *to = get_qres (ids[i], qr_ans);

    if (to == NULL || to->watchcat != NULL || to->ref_cnt != 0) {
      continue;
    }

    if (qres->timeout < to->timeout) {
      qres->timeout = to->timeout;
    }

    qres->ref_cnt++;
    to->watchcat = qres;
    if (to->state != qr_wait) {
      qres_conn (to, qres->next);
      qres_conn (qres, to);
    }
  }

  return qres->id;
}

void qres_ready (qres_t *qres) {
  assert (qres->prev == NULL);

  qres_t *w = qres->watchcat;
  if (w != NULL) {
    qres_conn (qres, w->next);
    qres_conn (w, qres);

    got_result (w->id);
  }

  got_result (qres->id);
}

int qres_is_ready (qres_t *qres) {
  if (qres->type == qr_ans) {
    return qres->state != qr_wait;
  } else if (qres->type == qr_watchcat) {
    return qres->ref_cnt == 0 || qres->next != qres;
  } else {
    assert (0);
  }
}

long long qres_next_id (qres_t *qres) {
  assert (qres->type == qr_watchcat);
  if (qres->next != qres) {
    return qres->next->id;
  }
  return -1;
}

int qres_save (qres_t *qres, char *data, int data_len) {
  assert (qres->type == qr_ans);
  if (qres->state != qr_wait) {
    return -1;
  }
  qres->state = qr_ok;

  qres->data = data;
  qres->data_len = data_len;

  qres_ready (qres);
  return 0;
}

int qres_error (qres_t *qres) {
  assert (qres->type == qr_ans);
  if (qres->state != qr_wait) {
    return -1;
  }
  qres->state = qr_failed;

  qres_ready (qres);
  return 0;
}

int qres_readed (qres_t *qres) {
  if (qres->type == qr_ans) {
    qres->ref_cnt = 1;
    if (qres->watchcat != NULL) {
      qres->watchcat->ref_cnt--;
      qres->watchcat = NULL;

      if (qres->prev != NULL) {
        qres_conn (qres->prev, qres->next);
      }

      qres->prev = qres->next = NULL;
    }
  }
  return 0;
}

void qres_free (qres_t *qres) {
  if (qres != NULL) {
    if (qres->data != NULL) {
      free (qres->data);
      qres->data = NULL;
      qres->data_len = 0;
    }

    free (qres);
  }
}

void qresults_clean (void) {
  long long n = cur_qres_id - first_qres_id;

  if (n > max_result_n) {
    n = max_result_n;
  }

  for (int i = 0; i < n; i++) {
    qres_free (qresults[i]);
    qresults[i] = NULL;
  }

  first_qres_id = cur_qres_id;
}

void qres_set_timeout (qres_t *qres, double timeout) {
  qres->timeout = timeout;
}

double qres_get_timeout (qres_t *qres) {
  return qres->timeout;
}


/*** main functions ***/

void engine_mc_run_query (int host_num, const char *request, int request_len, int timeout_ms, int query_type, void (*callback) (const char *result, int result_len)) {
  php_net_query_packet_answer_t *res = php_net_query_packet (host_num, request, request_len, timeout_ms, p_memcached, query_type | (PNETF_IMMEDIATE * (callback == NULL)));
  if (res->state == nq_error) {
    if (callback != NULL) {
      fprintf (stderr, "engine_mc_run_query error: %s [%s]\n", res->desc ? res->desc : "", res->res);
    }
    save_last_net_error (res->res);
  } else {
    assert (res->res != NULL);
    if (callback != NULL) {
      callback (res->res, res->res_len);
    }
  }
}

void engine_sql_run_query (int host_num, const char *request, int request_len, int timeout_ms, void (*callback) (const char *result, int result_len)) {
  php_net_query_packet_answer_t *res = php_net_query_packet (host_num, request, request_len, timeout_ms, p_sql, 0);
  if (res->state == nq_error) {
    fprintf (stderr, "engine_sql_run_query error: %s [%s]\n", res->desc ? res->desc : "", res->res);
    save_last_net_error (res->res);
  } else {
    assert (res->chain != NULL);
    chain_t *cur = res->chain->next;
    while (cur != res->chain) {
      //fprintf (stderr, "sql_callback [len = %d]\n", cur->len);
      callback (cur->buf, cur->len);
      cur = cur->next;
    }
  }
}

long long engine_rpc_send_query (int host_num, const char *request, int request_len, int timeout_ms) {
  php_net_query_packet_answer_t *res = php_net_query_packet (host_num, request, request_len, timeout_ms, p_rpc, 0);
  if (res->state == nq_error) {
    fprintf (stderr, "engine_rpc_send_query error: %s [%s]\n", res->desc ? res->desc : "", res->res);
    save_last_net_error (res->res);
    return 0;
  } else {
    return res->result_id;
  }
}

void rpc_answer_ (const char *res, int res_len) {
  assert (PHPScriptBase::is_running);
  php_query_rpc_answer q;
  q.base.type = PHPQ_RPC_ANSWER;
  q.data = res;
  q.data_len = res_len;

  PHPScriptBase::current_script->ask_query ((void *)&q);
}

void rpc_send_session_message_ (long long auth_key_id, long long session_id, const char *res, int res_len) {
  assert (PHPScriptBase::is_running);
  php_query_rpc_message q;
  q.base.type = PHPQ_RPC_MESSAGE;
  q.data = res;
  q.data_len = res_len;
  q.auth_key_id = auth_key_id;
  q.session_id = session_id;

  PHPScriptBase::current_script->ask_query ((void *)&q);
}

php_net_query_packet_answer_t *php_net_query_get (int connection_id, const char *data, int data_len, int timeout_ms, protocol_t protocol) {
  php_net_query_packet_t q;
  q.base.type = PHPQ_NETQ | NETQ_PACKET;

  q.connection_id = connection_id;
  q.data = data;
  q.data_len = data_len;
  q.timeout = timeout_ms * 0.001;
  q.protocol = protocol;

  PHPScriptBase::current_script->ask_query ((void *)&q);

  return (php_net_query_packet_answer_t *)q.base.ans;
}


void engine_rpc_get_result (long long request_id, void (*callback) (const int *data, int data_len)) {
  php_net_query_packet_answer_t *res = php_net_query_packet (-1, (char *)&request_id, sizeof (long long), -1, p_get, 0);
  if (res->state == nq_error) {
    fprintf (stderr, "engine_rpc_get_result error: [%s]\n", res->res);
    save_last_net_error (res->res);
  } else {
    assert (res->res != NULL);
    callback ((int *)res->res, res->res_len / (int)sizeof (int));
  }
}

php_query_create_queue_answer_t* php_net_create_queue (long long *request_ids, int request_ids_len) {
  php_query_create_queue_t q;
  q.base.type = PHPQ_CREATE_QUEUE;

  q.request_ids = request_ids;
  q.request_ids_len = request_ids_len;

  PHPScriptBase::current_script->ask_query ((void *)&q);

  return (php_query_create_queue_answer_t *)q.base.ans;
}

php_query_queue_empty_answer_t* php_net_queue_empty (long long queue_id) {
  php_query_queue_empty_t q;
  q.base.type = PHPQ_QUEUE_EMPTY;

  q.queue_id = queue_id;

  PHPScriptBase::current_script->ask_query ((void *)&q);

  return (php_query_queue_empty_answer_t *)q.base.ans;
}

long long engine_rpc_create_queue (long long *request_ids, int request_ids_len) {
  php_query_create_queue_answer_t *res = php_net_create_queue (request_ids, request_ids_len);
  return res->queue_id;
}

bool engine_rpc_is_queue_empty (long long queue_id) {
  //php_query_queue_empty_answer_t *res = php_net_queue_empty (queue_id);
  //return res->empty;
  qres_t *qres = get_qres (queue_id, qr_watchcat);
  if (qres != NULL) {
    return qres_next_id (qres) != -1;
  }
  return true;
}

long long engine_rpc_get_next_request_id (long long queue_id) {
  php_net_query_packet_answer_t *res = php_net_query_packet (-1, (char *)&queue_id, sizeof (long long), -1, p_get_id, 0);
  if (res->state == nq_error) {
    fprintf (stderr, "engine_rpc_get_result error: [%s]\n", res->res);
    save_last_net_error (res->res);
    return -1;
  } else {
    return res->result_id;
  }
}

void script_error_ (void) {
  PHPScriptBase::error ("script_error called");
}

void http_set_result_ (int return_code, const char *headers, int headers_len, const char *body, int body_len, int exit_code) {
  script_result res;
  res.return_code = return_code;
  res.exit_code = exit_code;
  res.headers = headers;
  res.headers_len = headers_len;
  res.body = body;
  res.body_len = body_len;

  PHPScriptBase::current_script->set_script_result (&res);
}

void init_drivers (void) {
  init_readers();
  http_load_long_query = engine_http_load_long_query;
  get_last_net_error = engine_get_last_net_error;
  mc_connect_to = engine_mc_connect_to;
  db_proxy_connect = engine_db_proxy_connect;
  rpc_connect_to = engine_rpc_connect_to;
  mc_run_query = engine_mc_run_query;
  db_run_query = engine_sql_run_query;
  rpc_send_query = engine_rpc_send_query;
  rpc_get_result = engine_rpc_get_result;
  rpc_answer = rpc_answer_;
  rpc_send_session_message = rpc_send_session_message_;
  rpc_create_queue = engine_rpc_create_queue;
  rpc_is_queue_empty = engine_rpc_is_queue_empty;
  rpc_get_next_request_id = engine_rpc_get_next_request_id;
  script_error = script_error_;
  http_set_result = http_set_result_;

  cur_qres_id = lrand48() | ((lrand48() & 0xFFFF) << 16);
  first_qres_id = cur_qres_id;
}

