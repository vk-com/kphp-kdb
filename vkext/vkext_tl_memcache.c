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
              2012-2013 Vitaliy Valtman
*/

#include <php.h>
#include "vkext.h"
#include "vkext_rpc.h"
#include "vkext_tl_parse.h"
#include "vkext_rpc_include.h"

#define MEMCACHE_ERROR 0x7ae432f5
#define MEMCACHE_VALUE_NOT_FOUND 0x32c42422
#define MEMCACHE_VALUE_LONG 0x9729c42
#define MEMCACHE_VALUE_STRING 0xa6bebb1a
#define MEMCACHE_FALSE 0xbc799737
#define MEMCACHE_TRUE 0x997275b5
#define MEMCACHE_SET 0xeeeb54c4
#define MEMCACHE_ADD 0xa358f31c
#define MEMCACHE_REPLACE 0x2ecdfaa2
#define MEMCACHE_INCR 0x80e6c950
#define MEMCACHE_DECR 0x6467e0d9
#define MEMCACHE_DELETE 0xab505c0a 
#define MEMCACHE_GET 0xd33b13ae

static char error_buf[1000];

struct memcache_value {
  int type;
  union {
    long long lval;
    struct {
      int len;
      char *data;
    } strval;
  } val;
  int flags;
};

long long do_memcache_send_get (struct rpc_connection *c, const char *key, int key_len, double timeout) {
  do_rpc_clean ();
//  do_rpc_store_header (1, 0);
  do_rpc_store_int (MEMCACHE_GET);
  do_rpc_store_string (key, key_len);
  struct rpc_query *q;
  if (!(q = do_rpc_send_noflush (c, timeout))) {
    return -1;
  }
  return q->qid;
}

long long do_memcache_send_store (struct rpc_connection *c, const char *key, int key_len, const char *value, int value_len, int flags, int delay, int op, double timeout) {
  do_rpc_clean ();
//  do_rpc_store_header (1, 0);
  do_rpc_store_int (op);
  do_rpc_store_string (key, key_len);
  do_rpc_store_int (flags);
  do_rpc_store_int (delay);
  do_rpc_store_string (value, value_len);
  struct rpc_query *q;
  if (!(q = do_rpc_send_noflush (c, timeout))) {
    return -1;
  }
  return q->qid;
}

long long do_memcache_send_incr (struct rpc_connection *c, const char *key, int key_len, long long val, int op, double timeout) {
  do_rpc_clean ();
//  do_rpc_store_header (1, 0);
  do_rpc_store_int (op);
  do_rpc_store_string (key, key_len);
  do_rpc_store_long (val);
  struct rpc_query *q;
  if (!(q = do_rpc_send_noflush (c, timeout))) {
    return -1;
  }
  return q->qid;
}

long long do_memcache_send_delete (struct rpc_connection *c, const char *key, int key_len, double timeout) {
  do_rpc_clean ();
  do_rpc_store_header (1, 0);
  do_rpc_store_int (MEMCACHE_DELETE);
  do_rpc_store_string (key, key_len);
  struct rpc_query *q;
  if (!(q = do_rpc_send_noflush (c, timeout))) {
    return -1;
  }
  return q->qid;
}

struct memcache_value do_memcache_parse_value (void) {
  struct memcache_value value;
  tl_parse_init ();
  int x = tl_parse_int ();
  switch (x) {
  case MEMCACHE_VALUE_NOT_FOUND:
    value.type = MEMCACHE_VALUE_NOT_FOUND;
    break;
  case MEMCACHE_VALUE_LONG:
    value.type = MEMCACHE_VALUE_LONG;
    value.val.lval = tl_parse_long ();
    value.flags = tl_parse_int ();
    break;
  case MEMCACHE_VALUE_STRING:
    value.type = MEMCACHE_VALUE_STRING;
    value.val.strval.len = tl_parse_string (&value.val.strval.data);
    value.flags = tl_parse_int ();
    break;
  case MEMCACHE_ERROR:
    value.type = MEMCACHE_ERROR;
    tl_parse_long ();
    tl_parse_int ();
    value.val.strval.len = tl_parse_string (&value.val.strval.data);
    break;
  case MEMCACHE_TRUE:
    value.type = MEMCACHE_TRUE;
    break;
  case MEMCACHE_FALSE:
    value.type = MEMCACHE_FALSE;
    break;
  default:
    snprintf (error_buf, 1000, "Unknown magic %x", x);
    tl_set_error (error_buf);
    break;
  }
  tl_parse_end ();
  if (tl_parse_error ()) {
    value.type = MEMCACHE_ERROR;
    value.val.strval.data = tl_parse_error ();
    value.val.strval.len = strlen (value.val.strval.data);
  }
  return value;
}

struct memcache_value do_memcache_act (struct rpc_connection *c, long long qid, int flags, double timeout) {
  struct memcache_value value;
  if (qid < 0) {
    value.type = MEMCACHE_ERROR;
    value.val.strval.data = global_error;
    value.val.strval.len = value.val.strval.data ? strlen (value.val.strval.data) : 0;
    return value;
  }
  if ((flags & 1) && /*do_rpc_flush_server (c->server, timeout) < 0*/ do_rpc_flush (timeout)) {
    value.type = MEMCACHE_ERROR;
    value.val.strval.data = global_error;
    value.val.strval.len = value.val.strval.data ? strlen (value.val.strval.data) : 0;
    return value;
  }
  if (do_rpc_get_and_parse (qid, timeout) < 0) {
    value.type = MEMCACHE_ERROR;
    value.val.strval.data = "timeout";
    value.val.strval.len = strlen (value.val.strval.data);
    return value;
  }
  return do_memcache_parse_value ();
}

struct memcache_value do_memcache_get (struct rpc_connection *c, const char *key, int key_len, double timeout) {
  long long qid = do_memcache_send_get (c, key, key_len, timeout);
  return do_memcache_act (c, qid, 1, timeout);
}

struct memcache_value do_memcache_store (struct rpc_connection *c, const char *key, int key_len, const char *value, int value_len, int flags, int delay, int op, double timeout) {
  assert (op == MEMCACHE_SET || op == MEMCACHE_REPLACE || op == MEMCACHE_ADD);
  long long qid = do_memcache_send_store (c, key, key_len, value, value_len, flags, delay, op, timeout);
  return do_memcache_act (c, qid, 1, timeout);
}

struct memcache_value do_memcache_incr (struct rpc_connection *c, const char *key, int key_len, long long val, int op, double timeout) {
  assert (op == MEMCACHE_INCR || op == MEMCACHE_DECR);
  long long qid = do_memcache_send_incr (c, key, key_len, val, op, timeout);
  return do_memcache_act (c, qid, 1, timeout);
}

struct memcache_value do_memcache_delete (struct rpc_connection *c, const char *key, int key_len, double timeout) {
  long long qid = do_memcache_send_delete (c, key, key_len, timeout);
  return do_memcache_act (c, qid, 1, timeout);
}

void vk_memcache_get (INTERNAL_FUNCTION_PARAMETERS) {
  ADD_CNT (parse);
  START_TIMER (parse);
  int argc = ZEND_NUM_ARGS ();
  zval **z[5];
  if (argc < 2) {
    END_TIMER (parse);
    RETURN_FALSE;
  }
  if (zend_get_parameters_array_ex (argc > 3 ? 3 : argc, z) == FAILURE) {
    END_TIMER (parse);
    RETURN_FALSE;
  }
 
  int fd = parse_zend_long (z[0]);
  //struct rpc_server *server = rpc_server_get (fd);
  struct rpc_connection *c = rpc_connection_get (fd);
//  if (!c || !c->server || c->server->status != rpc_status_connected) {
  if (!c || !c->servers) {
    END_TIMER (parse);
    RETURN_FALSE;
  }
  int l;
  char *key = parse_zend_string (z[1], &l);

  double timeout = 0.3;
  if (argc > 2) {
    timeout = parse_zend_double (z[2]);
  }
  update_precise_now ();
  timeout += precise_now;
  END_TIMER (parse);

  struct memcache_value value = do_memcache_get (c, key, l, timeout);

  switch (value.type) {
  case MEMCACHE_VALUE_LONG:
    RETURN_LONG (value.val.lval);
    break;
  case MEMCACHE_VALUE_STRING:
    ADD_RMALLOC (value.val.strval.len + 1);
    VV_STR_RETURN_DUP (value.val.strval.data, value.val.strval.len);
    break;
  case MEMCACHE_ERROR:
    fprintf (stderr, "Error %s\n", value.val.strval.data ? value.val.strval.data : "Unknown error");
  case MEMCACHE_TRUE:
  case MEMCACHE_FALSE:
  case MEMCACHE_VALUE_NOT_FOUND:
  default:
    RETURN_FALSE;
  }
}

void vk_memcache_multiget (INTERNAL_FUNCTION_PARAMETERS) {
  ADD_CNT (parse);
  START_TIMER (parse);
  int argc = ZEND_NUM_ARGS ();
  zval **z[5];
  if (argc < 2) {
    END_TIMER (parse);
    RETURN_FALSE;
  }
  if (zend_get_parameters_array_ex (argc > 3 ? 3 : argc, z) == FAILURE) {
    END_TIMER (parse);
    RETURN_FALSE;
  }
 
  int fd = parse_zend_long (z[0]);
  struct rpc_connection *c = rpc_connection_get (fd);
//  if (!c || !c->server || c->server->status != rpc_status_connected) {
  if (!c || !c->servers) {
    END_TIMER (parse);
    RETURN_FALSE;
  }
  if (Z_TYPE_PP (z[1]) != IS_ARRAY) {
    END_TIMER (parse);
    RETURN_FALSE;
  }
  double timeout = 0.3;
  if (argc > 2) {
    timeout = parse_zend_double (z[2]);
  }
  update_precise_now ();
  timeout += precise_now;
  
  array_init (return_value);
  HashPosition pos;
  zend_hash_internal_pointer_reset_ex (Z_ARRVAL_PP (z[1]), &pos);
  zval **zkey;
  int num = zend_hash_num_elements (Z_ARRVAL_PP (z[1]));
  long long *qids = malloc (sizeof (long long) * num);
  zval ***keys = malloc (sizeof (void *) * num);
  int cc = 0;
  while (zend_hash_get_current_data_ex(Z_ARRVAL_PP (z[1]), (void **)&zkey, &pos) == SUCCESS) {
    convert_to_string_ex (zkey);
    long long  qid = do_memcache_send_get (c, Z_STRVAL_PP (zkey), Z_STRLEN_PP (zkey), timeout);
    if (qid >= 0) {
      keys[cc   ] = zkey;
      qids[cc ++] = qid;
    } else {
      free (qids);
      free (keys);
      zval_dtor(return_value);
      END_TIMER (parse);
      RETURN_FALSE;
      return;
    }
    zend_hash_move_forward_ex (Z_ARRVAL_PP(z[1]), &pos);
  }
  END_TIMER (parse);
//  if (do_rpc_flush_server (c->server, timeout) < 0) {
  if (do_rpc_flush (timeout)) {
    free (qids);
    free (keys);
    zval_dtor(return_value);
    RETURN_FALSE;
    return;
  }
  if (!cc) {
    free (qids);
    free (keys);
    return;
  }
  int i;
  for (i = 0; i < cc; i++) {
    long long qid = do_rpc_get_any_qid (timeout);
    if (qid < 0) { 
      break;
    }
    assert (qid - qids[0] < cc);
    int k = qid - qids[0];
    if (do_rpc_get_and_parse (qid, timeout) < 0) {
      continue;
    }
    struct memcache_value value = do_memcache_parse_value ();
    switch (value.type) {
    case MEMCACHE_VALUE_STRING:
      add_assoc_stringl_ex (return_value, Z_STRVAL_PP (keys[k]), Z_STRLEN_PP (keys[k]) + 1, value.val.strval.data, value.val.strval.len, 1);
      break;
    case MEMCACHE_VALUE_LONG:
      add_assoc_long (return_value, Z_STRVAL_PP (keys[k]), value.val.lval);
      break;
    }
  }
  free (qids);
  free (keys);
}

void vk_memcache_store (int op, INTERNAL_FUNCTION_PARAMETERS) {
  ADD_CNT (parse);
  START_TIMER (parse);
  int argc = ZEND_NUM_ARGS ();
  zval **z[6];
  if (argc < 3) {
    END_TIMER (parse);
    RETURN_FALSE;
  }
  if (zend_get_parameters_array_ex (argc > 6 ? 6 : argc, z) == FAILURE) {
    END_TIMER (parse);
    RETURN_FALSE;
  }

  int fd = parse_zend_long (z[0]);
  struct rpc_connection *c = rpc_connection_get (fd);
//  if (!c || !c->server || c->server->status != rpc_status_connected) {
  if (!c || !c->servers) {
    END_TIMER (parse);
    RETURN_FALSE;
  }
  int key_len;
  char *key = parse_zend_string (z[1], &key_len);
  int value_len;
  char *value = parse_zend_string (z[2], &value_len);
  int flags = 0;
  if (argc >= 4) { flags = parse_zend_long (z[3]); }
  int delay = 0;
  if (argc >= 5) { delay = parse_zend_long (z[4]); }
  double timeout = 0.3;
  if (argc >= 6) { timeout = parse_zend_double (z[2]); }
  update_precise_now ();
  timeout += precise_now;
  END_TIMER (parse);

  if (op == 0) { 
    op = MEMCACHE_SET;
  } else if (op == 1) {
    op = MEMCACHE_ADD;
  } else {
    op = MEMCACHE_REPLACE;
  }

  struct memcache_value ans = do_memcache_store (c, key, key_len, value, value_len, flags, delay, op, timeout);
  switch (ans.type) {
  case MEMCACHE_TRUE:
    RETURN_TRUE;
    break;
  case MEMCACHE_ERROR:
    fprintf (stderr, "Error %s\n", ans.val.strval.data ? ans.val.strval.data : "Unknown error");
  case MEMCACHE_VALUE_LONG:
  case MEMCACHE_VALUE_STRING:
  case MEMCACHE_FALSE:
  case MEMCACHE_VALUE_NOT_FOUND:
  default:
    RETURN_FALSE;
  }
}

void vk_memcache_incr (int op, INTERNAL_FUNCTION_PARAMETERS) {
  ADD_CNT (parse);
  START_TIMER (parse);
  int argc = ZEND_NUM_ARGS ();
  zval **z[5];
  if (argc < 3) {
    END_TIMER (parse);
    RETURN_FALSE;
  }
  if (zend_get_parameters_array_ex (argc > 4 ? 4 : argc, z) == FAILURE) {
    END_TIMER (parse);
    RETURN_FALSE;
  }
 
  int fd = parse_zend_long (z[0]);
  struct rpc_connection *c = rpc_connection_get (fd);
//  if (!c || !c->server || c->server->status != rpc_status_connected) {
  if (!c || !c->servers) {
    END_TIMER (parse);
    RETURN_FALSE;
  }
  int l;
  char *key = parse_zend_string (z[1], &l);
  long long val = parse_zend_long (z[2]);

  double timeout = 0.3;
  if (argc > 3) {
    timeout = parse_zend_double (z[3]);
  }
  update_precise_now ();
  timeout += precise_now;
  END_TIMER (parse);
  if (op == 0) {
    op = MEMCACHE_INCR;
  } else {
    op = MEMCACHE_DECR;
  }
  struct memcache_value value = do_memcache_incr (c, key, l, val, op, timeout);
  switch (value.type) {
  case MEMCACHE_VALUE_LONG:
    RETURN_LONG (value.val.lval);
    break;
  case MEMCACHE_VALUE_STRING:
    ADD_RMALLOC (value.val.strval.len + 1);
    VV_STR_RETURN_DUP (value.val.strval.data, value.val.strval.len);
    break;
  case MEMCACHE_ERROR:
    fprintf (stderr, "Error %s\n", value.val.strval.data ? value.val.strval.data : "Unknown error");
  case MEMCACHE_TRUE:
  case MEMCACHE_FALSE:
  case MEMCACHE_VALUE_NOT_FOUND:
  default:
    RETURN_FALSE;
  }
}

void vk_memcache_delete (INTERNAL_FUNCTION_PARAMETERS) {
  ADD_CNT (parse);
  START_TIMER (parse);
  int argc = ZEND_NUM_ARGS ();
  zval **z[5];
  if (argc < 2) {
    END_TIMER (parse);
    RETURN_FALSE;
  }
  if (zend_get_parameters_array_ex (argc > 3 ? 3 : argc, z) == FAILURE) {
    END_TIMER (parse);
    RETURN_FALSE;
  }
 
  int fd = parse_zend_long (z[0]);
  struct rpc_connection *c = rpc_connection_get (fd);
  //if (!c || !c->server || c->server->status != rpc_status_connected) {
  if (!c || !c->servers) {
    END_TIMER (parse);
    RETURN_FALSE;
  }
  int l;
  char *key = parse_zend_string (z[1], &l);

  double timeout = 0.3;
  if (argc > 2) {
    timeout = parse_zend_double (z[2]);
  }
  update_precise_now ();
  timeout += precise_now;
  END_TIMER (parse);
  struct memcache_value value = do_memcache_delete (c, key, l, timeout);
  switch (value.type) {
  case MEMCACHE_TRUE:
    RETURN_TRUE;
    break;
  case MEMCACHE_ERROR:
    fprintf (stderr, "Error %s\n", value.val.strval.data ? value.val.strval.data : "Unknown error");
  case MEMCACHE_VALUE_STRING:
  case MEMCACHE_VALUE_LONG:
  case MEMCACHE_FALSE:
  case MEMCACHE_VALUE_NOT_FOUND:
  default:
    RETURN_FALSE;
  }
}
