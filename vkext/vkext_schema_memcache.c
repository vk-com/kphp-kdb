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
#include "vkext_rpc.h"
#include "vkext_rpc_include.h"
#include "vkext_schema_memcache.h"
#include "vkext_tl_parse.h"
#include "../vv/vv-tree.h"
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include "crc32.h"
#include <zend.h>

#ifndef Z_ADDREF_P
#define Z_ADDREF_P(ptr) (ptr)->refcount++;
#define Z_ADDREF_PP(ptr) Z_ADDREF_P(*(ptr))
#endif



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

#define TLS_SCHEMA 0xf19d9e38
#define TLS_TYPE 0x12eb4386
#define TLS_COMBINATOR 0x5c0a1ed5
#define TLS_COMBINATOR_LEFT_BUILTIN 0xcd211f63
#define TLS_COMBINATOR_LEFT 0x4c12c6d9
#define TLS_COMBINATOR_RIGHT 0xd325b367
#define TLS_ARG 0x46afe232
#define TLS_TREE_NAT_CONST 0xc09f07d7
#define TLS_TREE_NAT_VAR 0x90ea6f58
#define TLS_TREE_TYPE_VAR 0x1caa237a
#define TLS_TREE_ARRAY 0x80479360
#define TLS_TREE_TYPE 0x10f32190

#define TLS_SCHEMA_V2 0x3a2f9be2
#define TLS_COMBINATOR_RIGHT_V2 0x2c064372
#define TLS_ARG_V2 0x29dfe61b
#define TLS_EXPR_TYPE 0xecc9da78
#define TLS_EXPR_NAT 0xdcb49bd8

#define TLS_NAT_CONST 0xdcb49bd8
#define TLS_NAT_VAR 0x4e8a14f0
#define TLS_TYPE_VAR 0x0142ceae
#define TLS_ARRAY 0xd9fb20de
#define TLS_TYPE_EXPR 0xc1863d08

#if HAVE_ASSERT_H
#undef NDEBUG
#include <assert.h>
#endif /* HAVE_ASSERT_H */

//#define VLOG

#ifdef VLOG
#define Z fprintf (stderr, "%d\n", __LINE__);
#else
#define Z
#endif
int total_ref_cnt;
int persistent_tree_nodes;
int dynamic_tree_nodes;
extern int verbosity;
int total_tl_working;
int total_tree_nodes_existed;

/* HASH Tables {{{ */

static unsigned string_hash (const char *s) {
  unsigned h = 0;
  while (*s) {
    h = h * 17 + *s;
    s ++;
  }
  return h;
}

#define tl_type_id_cmp(a,b) (strcmp ((a)->id, (b)->id))
#define tl_type_id_hash(a) (string_hash ((a)->id))
DEFINE_HASH_STDNOZ_ALLOC(tl_type_id,struct tl_type *,tl_type_id_cmp,tl_type_id_hash);

/*struct hash_elem_tl_type_id *tl_type_id_arr[(1 << 12)];
struct hash_table_tl_type_id tl_type_id_hash_table = {
  .size = (1 << 12),
  .E = tl_type_id_arr,
  .mask = (1 << 12) - 1
};*/

#define tl_type_name_cmp(a,b) ((a)->name - (b)->name)
#define tl_type_name_hash(a) ((a)->name)
DEFINE_HASH_STDNOZ_ALLOC(tl_type_name,struct tl_type *,tl_type_name_cmp,tl_type_name_hash);

/*struct hash_elem_tl_type_name *tl_type_name_arr[(1 << 12)];
struct hash_table_tl_type_name tl_type_name_hash_table = {
  .size = (1 << 12),
  .E = tl_type_name_arr,
  .mask = (1 << 12) - 1
};*/

DEFINE_HASH_STDNOZ_ALLOC(tl_fun_id,struct tl_combinator *,tl_type_id_cmp,tl_type_id_hash);

/*struct hash_elem_tl_fun_id *tl_fun_id_arr[(1 << 12)];
struct hash_table_tl_fun_id tl_fun_id_hash_table = {
  .size = (1 << 12),
  .E = tl_fun_id_arr,
  .mask = (1 << 12) - 1
};*/

DEFINE_HASH_STDNOZ_ALLOC(tl_fun_name,struct tl_combinator *,tl_type_name_cmp,tl_type_name_hash);

/*struct hash_elem_tl_fun_name *tl_fun_name_arr[(1 << 12)];
struct hash_table_tl_fun_name tl_fun_name_hash_table = {
  .size = (1 << 12),
  .E = tl_fun_name_arr,
  .mask = (1 << 12) - 1
};*/

struct tl_config {
  int fn, cn, tn, pos;
  struct tl_type **tps;
  struct tl_combinator **fns;
  struct hash_table_tl_fun_name *ht_fname;
  struct hash_table_tl_fun_id *ht_fid;
  struct hash_table_tl_type_name *ht_tname;
  struct hash_table_tl_type_id *ht_tid;
  int working_queries;
};

struct tl_config *cur_config;
#define CONFIG_LIST_SIZE 10
struct tl_config *config_list[CONFIG_LIST_SIZE];
int config_list_pos;

struct tl_type *tl_type_get_by_id (const char *s) {
  assert (cur_config);
  struct tl_type t;
  t.id = (char *)s;
  //struct hash_elem_tl_type_id *h = hash_lookup_tl_type_id (&tl_type_id_hash_table, &t);
  struct hash_elem_tl_type_id *h = hash_lookup_tl_type_id (cur_config->ht_tid, &t);
  return h ? h->x : 0;
}

struct tl_combinator *tl_fun_get_by_id (const char *s) {
  assert (cur_config);
  struct tl_combinator c;
  c.id = (char *)s;
  //struct hash_elem_tl_fun_id *h = hash_lookup_tl_fun_id (&tl_fun_id_hash_table, &c);
  struct hash_elem_tl_fun_id *h = hash_lookup_tl_fun_id (cur_config->ht_fid, &c);
  return h ? h->x : 0;
}

struct tl_combinator *tl_fun_get_by_name (int name) {
  assert (cur_config);
  struct tl_combinator c;
  c.name = name;
  //struct hash_elem_tl_fun_name *h = hash_lookup_tl_fun_name (&tl_fun_name_hash_table, &c);
  struct hash_elem_tl_fun_name *h = hash_lookup_tl_fun_name (cur_config->ht_fname, &c);
  return h ? h->x : 0;
}

struct tl_type *tl_type_get_by_name (int name) {
  assert (cur_config);
  struct tl_type t;
  t.name = name;
  //struct hash_elem_tl_type_name *h = hash_lookup_tl_type_name (&tl_type_name_hash_table, &t);
  struct hash_elem_tl_type_name *h = hash_lookup_tl_type_name (cur_config->ht_tname, &t);
  return h ? h->x : 0;
}

void tl_type_insert_name (struct tl_type *t) {
  assert (cur_config);
  if (t->name) {
    assert (!tl_type_get_by_name (t->name));
    //hash_insert_tl_type_name (&tl_type_name_hash_table, t);
    hash_insert_tl_type_name (cur_config->ht_tname, t);
  } else {
    if (!tl_type_get_by_name (t->name)) {
      //hash_insert_tl_type_name (&tl_type_name_hash_table, t);
      hash_insert_tl_type_name (cur_config->ht_tname, t);
    }
  }
}

void tl_type_insert_id (struct tl_type *t) {
  assert (cur_config);
  assert (!tl_type_get_by_id (t->id));
  //hash_insert_tl_type_id (&tl_type_id_hash_table, t);
  hash_insert_tl_type_id (cur_config->ht_tid, t);
}

void tl_fun_insert_id (struct tl_combinator *t) {
  assert (cur_config);
  assert (!tl_fun_get_by_id (t->id));
  //hash_insert_tl_fun_id (&tl_fun_id_hash_table, t);
  hash_insert_tl_fun_id (cur_config->ht_fid, t);
}

void tl_fun_insert_name (struct tl_combinator *t) {
  assert (cur_config);
  assert (!tl_fun_get_by_name (t->name));
  //hash_insert_tl_fun_name (&tl_fun_name_hash_table, t);
  hash_insert_tl_fun_name (cur_config->ht_fname, t);
}

/* }}} */

/* {{{ PHP arrays interaction */
zval **get_field (zval *arr, const char *id, int num) {
  ADD_CNT (get_field);
  START_TIMER (get_field);
  assert (arr);
//  fprintf (stderr, "arr = %p, type = %d\n", arr, (int)Z_TYPE_PP (arr));
  if (Z_TYPE_P (arr) != IS_ARRAY) {
//    fprintf (stderr, "=(\n");
    END_TIMER (get_field);
    return 0;
  }
//  fprintf (stderr, ".\n");
//  fprintf (stderr, "%s\n", id);
//  fprintf (stderr, "%d\n", id ?(int) strlen (id) : -2);
  zval **t = 0;
  if (id && strlen (id) && zend_hash_find (Z_ARRVAL_P (arr), (char *) id, strlen (id) + 1, (void **)&t) != FAILURE) {
    assert (t);
    END_TIMER (get_field);
    return t;
  }
  if (zend_hash_index_find (Z_ARRVAL_P (arr), num, (void *)&t) != FAILURE) {
    assert (t);
    END_TIMER (get_field);
    return t;
  }
  END_TIMER (get_field);
  return 0;
}

void set_field (zval **arr, zval *val, const char *id, int num) {
  ADD_CNT (set_field);
  START_TIMER (set_field);
  if (!*arr) {
    MAKE_STD_ZVAL (*arr);
    array_init (*arr);
  }
  assert (val);
  assert (*arr && Z_TYPE_PP (arr) == IS_ARRAY);
#ifdef VLOG
  fprintf (stderr, "set_field: num:%d val_type:%d arr:%p\n", num, Z_TYPE_P (val), *arr);
#endif
  if (id && strlen (id)) {
    add_assoc_zval (*arr, (char *) id, val);
  } else {
    add_index_zval (*arr, num, val);
  }
  END_TIMER (set_field);
}

void set_field_string (zval **arr, char *val, const char *id, int num) {
  ADD_CNT (set_field);
  START_TIMER (set_field);
  if (!*arr) {
    MAKE_STD_ZVAL (*arr);
    array_init (*arr);
  }
  assert (val);
  assert (*arr && Z_TYPE_PP (arr) == IS_ARRAY);

  if (id && strlen (id)) {
    add_assoc_string (*arr, (char *) id, val, 1);
  } else {
    add_index_string (*arr, num, val, 1);
  }
  END_TIMER (set_field);
}

void set_field_bool (zval **arr, int val, const char *id, int num) {
  ADD_CNT (set_field);
  START_TIMER (set_field);
  if (!*arr) {
    MAKE_STD_ZVAL (*arr);
    array_init (*arr);
  }
  if (id && strlen (id)) {
    add_assoc_bool (*arr, (char *) id, val);
  } else {
    add_index_bool (*arr, num, val);
  }
  END_TIMER (set_field);
}

void set_field_int (zval **arr, int val, const char *id, int num) {
  ADD_CNT (set_field);
  START_TIMER (set_field);
  if (!*arr) {
    MAKE_STD_ZVAL (*arr);
    array_init (*arr);
  }
  if (id && strlen (id)) {
    add_assoc_long (*arr, (char *) id, val);
  } else {
    add_index_long (*arr, num, val);
  }
  END_TIMER (set_field);
}

int get_array_size (zval **arr) {
  return zend_hash_num_elements (Z_ARRVAL_P (*arr));
}
/* }}} */

#define use_var_nat_full_form(x) 1

long long var_nat_const_to_int (void *x) {
  if (((long)x) & 1) {
    return (((long)x) + 0x80000001l) / 2;
  } else {
    return ((struct tl_tree_nat_const *)x)->value;
  }
}

void *int_to_var_nat_const (long long x) {
  if (use_var_nat_full_form (x)) {
    struct tl_tree_nat_const *T = zzemalloc (sizeof (*T));
    T->self.ref_cnt = 1;
    T->self.flags = 0;
    T->self.methods = &tl_nat_const_full_methods;
    T->value = x;
    total_ref_cnt ++;
    dynamic_tree_nodes ++;
    total_tree_nodes_existed ++;
    return T;
  } else {
    return (void *)(long)(x * 2 - 0x80000001l);
  }
}

void *int_to_var_nat_const_init (long long x) {
  if (use_var_nat_full_form (x)) {
    struct tl_tree_nat_const *T = zzmalloc (sizeof (*T));
    ADD_PMALLOC (sizeof (*T));
    T->self.ref_cnt = 1;
    T->self.flags = 0;
    T->self.methods = &tl_pnat_const_full_methods;
    T->value = x;
    total_ref_cnt ++;
    persistent_tree_nodes ++;
    total_tree_nodes_existed ++;
    return T;
  } else {
    return (void *)(long)(x * 2 - 0x80000001l);
  }
}


int get_constructor (struct tl_type *t, char *id) {
  int i;
  for (i = 0; i < t->constructors_num; i++) if (!strcmp (t->constructors[i]->id, id)) {
    return i;
  }
  return -1;
}

int get_constructor_by_name (struct tl_type *t, int name) {
  int i;
  for (i = 0; i < t->constructors_num; i++) if (t->constructors[i]->name == name) {
    return i;
  }
  return -1;
}

#define MAX_VARS 100000
struct tl_tree *__vars[MAX_VARS];
struct tl_tree **last_var_ptr;
struct tl_tree **get_var_space (struct tl_tree **vars, int n) {
//  fprintf (stderr, "get_var_space: %d\n", n);
  if (vars - n < __vars) { return 0; }
  if (last_var_ptr > vars - n) {
    last_var_ptr = vars - n;
  }
  return vars - n;
}


void tl_parse_on_rinit (void) {
  last_var_ptr = __vars + MAX_VARS;
}

void tl_parse_on_rshutdown (void) {
  while (last_var_ptr < __vars + MAX_VARS) {
    if (*last_var_ptr) {
      DEC_REF (*last_var_ptr);
      *last_var_ptr = 0;
    }
    last_var_ptr ++;
  }
}

#define MAX_SIZE 100000
void *_Data[MAX_SIZE];

#define MAX_DEPTH 10000

typedef void *(*fpr_t)(void **IP, void **Data, zval **arr, struct tl_tree **vars);
#define TLUNI_NEXT return (*(fpr_t *) IP) (IP + 1, Data, arr, vars);
#define TLUNI_START(IP,Data,arr,vars) (*(fpr_t *) IP) (IP + 1, Data, arr, vars)

#define TLUNI_OK (void *)1l
#define TLUNI_FAIL (void *)0

void **fIP;

/* {{{ Interface functions */

struct tl_tree *store_function (zval **arr) {
  assert (arr);
  zval **r = get_field (*arr, "_", 0);
  if (!r) { return 0;}
  struct tl_combinator *c;
  char *s = 0;
  int l = 0;
  if (Z_TYPE_PP (r) == IS_STRING) {
    int l;
    s = parse_zend_string (r, &l);
    c = tl_fun_get_by_id (s);
  } else {
    l = parse_zend_long (r);
    c = tl_fun_get_by_name (l);
  }
  if (!c) { 
    #ifdef VLOG
      if (Z_TYPE_PP (r) == IS_STRING) {
        fprintf (stderr, "Function %s not found\n", s);
      } else {
        fprintf (stderr, "Function %d not found\n", l);
      }
    #endif
    return 0; 
  }
#ifdef VLOG
  fprintf (stderr, "Storing functions %s\n", c->id);
#endif
  struct tl_tree **vars = get_var_space (__vars + MAX_VARS, c->var_num);
  if (!vars) {
    return 0;
  }
  static zval *_arr[MAX_DEPTH];
  _arr[0] = *arr;
  void *res = TLUNI_START (c->IP, _Data, _arr, vars);
#ifdef VLOG
  if (res) {
    void *T = res;
    fprintf (stderr, "Store end: T->id = %s, T->ref_cnt = %d, T->flags = %d\n", ((struct tl_tree_type *)T)->type->id, ((struct tl_tree_type *)T)->self.ref_cnt, ((struct tl_tree_type *)T)->self.flags);
  }
#endif
  if (res) {
    struct tl_tree_type *T = zzemalloc (sizeof (*T));
    T->self.flags = 0;
    T->self.ref_cnt = 1;
    T->self.methods = &tl_type_methods;
    T->type = tl_type_get_by_id ("ReqResult");
    T->children_num = 1;
    T->children = zzemalloc (sizeof (*T->children));
    *T->children = res;
    res = T;
    dynamic_tree_nodes ++;
    total_ref_cnt ++;
  }
//  assert (((struct tl_tree *)res)->ref_cnt > 0);
  return res;
}

zval **fetch_function (struct tl_tree *T) {
#ifdef VLOG
  int *cptr;
  for (cptr = (int *)inbuf->rptr; cptr < (int *)inbuf->wptr; cptr ++) {
    fprintf (stderr, "Int %d (%08x)\n", *cptr, *cptr);
  }
#endif
  assert (T);
  struct tl_tree **vars = __vars + MAX_VARS;
  static zval *_arr[MAX_DEPTH];
  *_arr = 0;
  *(_Data) = T;
#ifdef VLOG
  fprintf (stderr, "Fetch begin: T->id = %s, T->ref_cnt = %d, T->flags = %d\n", ((struct tl_tree_type *)T)->type->id, ((struct tl_tree_type *)T)->self.ref_cnt, ((struct tl_tree_type *)T)->self.flags);
#endif
//  INC_REF (T);
  int x = do_rpc_lookup_int ();
  if (x == RPC_REQ_ERROR) {
    assert (tl_parse_int () == RPC_REQ_ERROR); 
    tl_parse_long ();
    int error_code = tl_parse_int ();
    char *s = 0;
    int l = tl_parse_string (&s);
//    fprintf (stderr, "Error_code %d: error %.*s\n", error_code, l, s);
    MAKE_STD_ZVAL (*_arr);
    array_init (*_arr);
    char *x = l >= 0 ? estrndup (s, l) : estrdup ("unknown");
    set_field_string (_arr, x, "__error", 0);
    efree (x);
    set_field_int (_arr, error_code, "__error_code", 0);
    if (s) { free (s); }
    DEC_REF (T);

    return _arr;
  }
  void *res = TLUNI_START (fIP, _Data + 1, _arr, vars);
//  DEC_REF (T);
  if (res == TLUNI_OK) {
    tl_parse_end ();
    if (tl_parse_error ()) {
      res = TLUNI_FAIL;
    }
  }
  if (res == TLUNI_OK) {
    if (!*_arr) {
      MAKE_STD_ZVAL (*_arr);
      ZVAL_BOOL (*_arr, 1);
    }
    return _arr;
  } else {
    if (*_arr) {
      zval_dtor (*_arr);
    }
    MAKE_STD_ZVAL (*_arr);    
    ZVAL_BOOL (*_arr, 0);
    return _arr;
  }
}

void _extra_dec_ref (struct rpc_query *q) {
  if (q->extra) {
    total_tl_working --;
  }
  DEC_REF (q->extra);
  q->extra = 0;
  q->extra_free = 0;
}

struct rpc_query *vk_memcache_query_one (struct rpc_connection *c, double timeout, zval **arr) {
  do_rpc_clean ();
  START_TIMER (tmp);
  void *res = store_function (arr);
  END_TIMER (tmp);
  if (!res) { return 0; }
  struct rpc_query *q;
  if (!(q = do_rpc_send_noflush (c, timeout))) {
    return 0;
  }
  q->extra = res;
  q->extra_free = _extra_dec_ref;
  total_tl_working ++;
  return q;
}

zval **vk_memcache_query_result_one (struct tl_tree *T) {
  tl_parse_init ();
  START_TIMER (tmp);
  zval **r = fetch_function (T);
  END_TIMER (tmp);
  return r;
}

void vk_memcache_query_many (struct rpc_connection *c, zval **arr, double timeout, zval **r) {
  HashPosition pos;
  zend_hash_internal_pointer_reset_ex (Z_ARRVAL_PP (arr), &pos);
  zval **zkey;
  //int num = zend_hash_num_elements (Z_ARRVAL_PP (arr));
  //i nt cc = 0;
  array_init (*r);
  while (zend_hash_get_current_data_ex (Z_ARRVAL_PP (arr), (void **)&zkey, &pos) == SUCCESS) {
    char *key;
    unsigned int key_len;
    unsigned long index;
    if (zend_hash_get_current_key_ex (Z_ARRVAL_PP (arr), &key, &key_len, &index, 1, &pos) == HASH_KEY_IS_STRING) {
      index = 0;      
    } else {
      key = 0;
    }
    struct rpc_query *q = vk_memcache_query_one (c, timeout, zkey);
    zend_hash_move_forward_ex (Z_ARRVAL_PP(arr), &pos);
    if (key) {
      if (q) {
        add_assoc_long (*r, key, q->qid);
      } else {
        add_assoc_bool (*r, key, 0);
      }
    } else {
      if (q) {
        add_index_long (*r, index, q->qid);
      } else {
        add_index_bool (*r, index, 0);
      }
    }
  }
  //long long x = do_rpc_queue_create (cc, qids);
  //free (qids);
  //return r;
}

void vk_memcache_query (INTERNAL_FUNCTION_PARAMETERS) {
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

  double timeout = argc > 2 ? parse_zend_double (z[2]) : c->default_query_timeout;
  update_precise_now ();
  timeout += precise_now;
  
  if (Z_TYPE_PP (z[1]) != IS_ARRAY) {
    RETURN_FALSE;
    return;
  }
  END_TIMER (parse);

  vk_memcache_query_many (c, z[1], timeout, &return_value);
//  if (do_rpc_flush_server (c->server, timeout) < 0) {
  if (do_rpc_flush (timeout) < 0) {
    zval_dtor (return_value);
    RETURN_FALSE;
  }
}

void vk_memcache_query_result_many (struct rpc_queue *Q, double timeout, zval **r) {
  array_init (*r);
  int size = 0;
  while (!do_rpc_queue_empty (Q)) {
    long long qid = do_rpc_queue_next (Q, timeout);
    if (qid <= 0) {
      return;
    }
    struct rpc_query *q = rpc_query_get (qid);
    struct tl_tree *T = q->extra;
    INC_REF (T);
    
    if (do_rpc_get_and_parse (qid, timeout) < 0) {
      continue;
    }
    size ++;    
    zval **x = vk_memcache_query_result_one (T);
    if (x) {
      add_index_zval (*r, qid, *x);
    } else {
      add_index_bool (*r, qid, 0);
    }
  }
}

void vk_memcache_query_result1 (INTERNAL_FUNCTION_PARAMETERS) {
  ADD_CNT (parse);
  START_TIMER (parse);
  int argc = ZEND_NUM_ARGS ();
  zval **z[5];
  if (argc < 1) {
    END_TIMER (parse);
    RETURN_FALSE;
  }
  if (zend_get_parameters_array_ex (argc > 1 ? 2 : argc, z) == FAILURE) {
    END_TIMER (parse);
    RETURN_FALSE;
  }
  long long Qid = parse_zend_long (z[0]);
  struct rpc_queue *Q = rpc_queue_get (Qid);
  if (!Q) {
    END_TIMER (parse);
    RETURN_FALSE;
  }
  update_precise_now ();
  double timeout = (argc < 2) ? Q->timeout : precise_now + parse_zend_double (z[1]);
  END_TIMER (parse);

  vk_memcache_query_result_many (Q, timeout, &return_value);
}

void vk_memcache_query_result (INTERNAL_FUNCTION_PARAMETERS) {
  ADD_CNT (parse);
  START_TIMER (parse);
  int argc = ZEND_NUM_ARGS ();
  zval **z[5];
  if (argc < 1) {
    END_TIMER (parse);
    RETURN_FALSE;
  }
  if (zend_get_parameters_array_ex (argc > 1 ? 2 : argc, z) == FAILURE) {
    END_TIMER (parse);
    RETURN_FALSE;
  }  
  zval **arr = z[0];
  if (Z_TYPE_PP (arr) != IS_ARRAY) {
    RETURN_FALSE;
  }
  HashPosition pos;
  zend_hash_internal_pointer_reset_ex (Z_ARRVAL_PP (arr), &pos);
  zval **zkey;

  int max_size  = zend_hash_num_elements (Z_ARRVAL_PP (arr));
  long long qids[max_size];
  int size = 0;

  //i nt cc = 0;
  while (zend_hash_get_current_data_ex (Z_ARRVAL_PP (arr), (void **)&zkey, &pos) == SUCCESS) {
    assert (size < max_size);
    qids[size ++] = parse_zend_long (zkey);
    if (!qids[size - 1]) {
      size --;
    }
    zend_hash_move_forward_ex (Z_ARRVAL_PP (arr), &pos);
  }
  long long Qid = do_rpc_queue_create (size, qids);
  if (!Qid) {
    END_TIMER (parse);
    RETURN_FALSE;
  }
  struct rpc_queue *Q = rpc_queue_get (Qid);
  assert (Q);
  update_precise_now ();
  double timeout = (argc < 2) ? Q->timeout : precise_now + parse_zend_double (z[1]);
  END_TIMER (parse);

  zval *_R;
  zval **R = &_R;
  MAKE_STD_ZVAL (_R);
  vk_memcache_query_result_many (Q, timeout, R);

  zend_hash_internal_pointer_reset_ex (Z_ARRVAL_PP (arr), &pos);
  //int num = zend_hash_num_elements (Z_ARRVAL_PP (arr));
  //i nt cc = 0;
  array_init (return_value);
  while (zend_hash_get_current_data_ex (Z_ARRVAL_PP (arr), (void **)&zkey, &pos) == SUCCESS) {
    char *key;
    unsigned int key_len;
    unsigned long index;
    if (zend_hash_get_current_key_ex (Z_ARRVAL_PP (arr), &key, &key_len, &index, 1, &pos) == HASH_KEY_IS_STRING) {
      index = 0;      
    } else {
      key = 0;
    }
    
    long long qid = parse_zend_long (zkey);
    if (qid > 0) {
      zval **r;
      if (zend_hash_index_find (Z_ARRVAL_PP (R), qid, (void **)&r) == FAILURE) {
        r = 0;
      } else {
        Z_ADDREF_PP (r);
      }
      if (key) {
        if (r) {
          add_assoc_zval (return_value, key, *r);
        } else {
          add_assoc_bool (return_value, key, 0);
        }
      } else {
        if (r) {
          add_index_zval (return_value, index, *r);
        } else {
          add_index_bool (return_value, index, 0);
        }
      }
    }
    zend_hash_move_forward_ex (Z_ARRVAL_PP (R), &pos);
  }
  zval_dtor (*R);
}
/* }}} */

/**** Simple code functions {{{ ****/

void *tls_push (void **IP, void **Data, zval **arr, struct tl_tree **vars) {
  *(Data ++) = *(IP ++);
  INC_REF (*(Data - 1));
  TLUNI_NEXT;
}

void *tls_dup (void **IP, void **Data, zval **arr, struct tl_tree **vars) {
  void *x = *(Data - 1);
  *(Data ++) = x;
  TLUNI_NEXT;
}

void *tls_pop (void **IP, void **Data, zval **arr, struct tl_tree **vars) {
   --Data;
   TLUNI_NEXT;
}

void *tls_arr_pop (void **IP, void **Data, zval **arr, struct tl_tree **vars) {
  --arr;
  TLUNI_NEXT;
}

void *tls_arr_push (void **IP, void **Data, zval **arr, struct tl_tree **vars) {
  ++arr;
  *arr = 0;
  TLUNI_NEXT;
}

void *tls_ret (void **IP, void **Data, zval **arr, struct tl_tree **vars) {
  return *(IP ++);
}

void *tls_store_int (void **IP, void **Data, zval **arr, struct tl_tree **vars) {
  do_rpc_store_int ((long)*(IP ++));
  TLUNI_NEXT;
}

/*** }}} */

/**** Combinator store code {{{ ****/


void *tlcomb_store_const_int (void **IP, void **Data, zval **arr, struct tl_tree **vars) {
  do_rpc_store_int ((long)*(IP ++));
  TLUNI_NEXT;
}

void *tlcomb_skip_const_int (void **IP, void **Data, zval **arr, struct tl_tree **vars) {
  int a = tl_parse_int ();
  if (tl_parse_error ()) { return 0; }
  if (a != (long)*(IP ++)) { return 0; }
  TLUNI_NEXT;
}

/****
 *
 * Data [data] => [data] result
 * IP
 *
 ****/

void *tlcomb_store_any_function (void **IP, void **Data, zval **arr, struct tl_tree **vars) {
  zval **r = get_field (*arr, "_", 0);
  if (!r) { return 0;}
  int l;
  char *s = parse_zend_string (r, &l);
  struct tl_combinator *c = tl_fun_get_by_id (s);
#ifdef VLOG
  fprintf (stderr, "Storing functions %s\n", c->id);
#endif
  if (!c) { return 0; }
  struct tl_tree **new_vars = get_var_space (vars, c->var_num);
  if (!new_vars) {
    return 0;
  }
  void *res = TLUNI_START (c->IP, Data, arr, new_vars);
  if (!res) { return 0; }
  *(Data ++) = res;
  TLUNI_NEXT;
}

/****
 *
 * Data [data] result => [data]
 * IP
 *
 ****/

void *tlcomb_fetch_type (void **IP, void **Data, zval **arr, struct tl_tree **vars) {
  struct tl_tree_type *e = *(--Data);
  assert (e);
  struct tl_type *t = e->type;
  assert (t);
#ifdef VLOG
  fprintf (stderr, "Fetching type %s. Flags %d\n", t->id, e->self.flags);
#endif
  int n = -1;
  if (e->self.flags & FLAG_BARE) {
    if (t->constructors_num == 1) {
       n = 0;
    }
  } else {
    int p = tl_parse_save_pos ();
    int x = tl_parse_int ();
    if (tl_parse_error ()) { 
      DEC_REF (e); 
      return 0; 
    } 
    #ifdef VLOG
    fprintf (stderr, "cosntructor name = %08x\n", x);
    #endif
    n = get_constructor_by_name (t, x);
    if (n < 0) {
      #ifdef VLOG
      fprintf (stderr, "Checking for default constructor\n");
      #endif
      if (t->flags & FLAG_DEFAULT_CONSTRUCTOR) {
        n = t->constructors_num - 1;
        tl_parse_restore_pos (p);
      }
    }
    if (n < 0) {      
      DEC_REF (e);
      return 0;
    }
  }
  if (n >= 0) {  
#ifdef VLOG
    fprintf (stderr, "Fetching constructor %s\n", t->constructors[n]->id);
#endif
    *(Data) = e;
    struct tl_tree **new_vars = get_var_space (vars, t->constructors[n]->var_num);
    if (!new_vars) { return 0; }
    if (TLUNI_START (t->constructors[n]->fIP, Data + 1, arr, new_vars) != TLUNI_OK) {
      DEC_REF (e);
      return 0;
    }
    if (!(e->self.flags & FLAG_BARE) && (t->constructors_num > 1) && !(t->flags & FLAG_NOCONS)) {
      set_field_string (arr, t->constructors[n]->id, "_", 0);
    }
    DEC_REF (e);
    TLUNI_NEXT;
  } else {
#ifdef VLOG
    fprintf (stderr, "Fetching any constructor\n");
#endif
    int k = tl_parse_save_pos ();
    for (n = 0; n < t->constructors_num; n++) {
      *(Data) = e;
      struct tl_tree **new_vars = get_var_space (vars, t->constructors[n]->var_num);
      if (!new_vars) { return 0; }
      void *r = TLUNI_START (t->constructors[n]->IP, Data + 1, arr, new_vars);
      if (r == TLUNI_OK) {
        if (!(e->self.flags & FLAG_BARE) && !(t->flags & FLAG_NOCONS)) {
          set_field_string (arr, t->constructors[n]->id, "_", 0);
        }
        DEC_REF (e);
        TLUNI_NEXT;
      }
      assert (tl_parse_restore_pos (k));
      tl_parse_clear_error ();
    }
    DEC_REF (e);
    return 0;
  }
}

/****
 *
 * Data: [data] result => [data]
 * IP  : 
 *
 ****/
void *tlcomb_store_type (void **IP, void **Data, zval **arr, struct tl_tree **vars) {
  //struct tl_type *t = *(IP ++);
  struct tl_tree_type *e = *(--Data);
  assert (e);
  struct tl_type *t = e->type;
  assert (t);
#ifdef VLOG
  fprintf (stderr, "Storing type %s. *arr = %p\n", t->id, *arr);
#endif

  int n = -1;
  if (t->constructors_num > 1) {
    zval **v = get_field (*arr, "_", 0);    
    if (v) {
      int sl;
      char *s = parse_zend_string (v, &sl);
      n = get_constructor (t, s);
      if (n < 0) {
        DEC_REF (e);
        return 0;
      }
    }
  } else {
    n = 0;
  }
  if (n >= 0) {  
#ifdef VLOG
    fprintf (stderr, "Storing constructor %s\n", t->constructors[n]->id);
#endif
    *(Data) = e;
    if (!(e->self.flags & FLAG_BARE) && strcmp (t->constructors[n]->id, "_")) {
      do_rpc_store_int (t->constructors[n]->name);
    }
    struct tl_tree **new_vars = get_var_space (vars, t->constructors[n]->var_num);
    if (!new_vars) { return 0; }
    if (TLUNI_START (t->constructors[n]->IP, Data + 1, arr, new_vars) != TLUNI_OK) {
      DEC_REF (e);
      return 0;
    }
    DEC_REF (e);
    TLUNI_NEXT;
  } else {
#ifdef VLOG
    fprintf (stderr, "Storing any constructor\n");
#endif
    int k = do_rpc_store_get_pos ();
    for (n = 0; n < t->constructors_num; n++) {
      if (!(e->self.flags & FLAG_BARE) && strcmp (t->constructors[n]->id, "_")) {
        do_rpc_store_int (t->constructors[n]->name);
      }
      *(Data) = e;
      struct tl_tree **new_vars = get_var_space (vars, t->constructors[n]->var_num);
      if (!new_vars) { return 0; }
      void *r = TLUNI_START (t->constructors[n]->IP, Data + 1, arr, new_vars);
      if (r == TLUNI_OK) {
        DEC_REF (e);
        TLUNI_NEXT;
      }
      assert (do_rpc_store_set_pos (k));
    }
    DEC_REF (e);
    return 0;
  }
}

/****
 *
 * Data: [data] => [data]
 * IP  : id num
 *
 ***/
void *tlcomb_store_field (void **IP, void **Data, zval **arr, struct tl_tree **vars) {
  char *id = *(IP ++);
  int num = (long)*(IP ++);
#ifdef VLOG
  fprintf (stderr, "store_field: id = %s, num = %d\n", id, num);
#endif

  zval **v = get_field (*arr, id, num);

#ifdef VLOG
  fprintf (stderr, "store_field: field %p\n", v);
#endif
  if (!v) { return 0; }
  *(++arr) = *v;
  TLUNI_NEXT;
}


/****
 *
 * Data: [data] => [data]
 *
 ***/

void *tlcomb_store_field_end (void **IP, void **Data, zval **arr, struct tl_tree **vars) {
  --arr;
  TLUNI_NEXT;
}


/****
 *
 * Data: [data] => [data]
 * IP: id num
 *
 ***/

void *tlcomb_fetch_field_end (void **IP, void **Data, zval **arr, struct tl_tree **vars) {
  char *id = *(IP ++);
  int num = (long)*(IP ++);
  //assert (*arr);
  if (!*arr) {
    MAKE_STD_ZVAL (*arr);
    array_init (*arr);
  }
  set_field ((arr - 1), *arr, id, num);
  arr --;
  TLUNI_NEXT;
}


/****
 *
 * Data: [data] arity => [data]
 * IP  : newIP
 *
 ***/
void *tlcomb_store_array (void **IP, void **Data, zval **arr, struct tl_tree **vars) {
  void **newIP = *(IP ++);
  int multiplicity = var_nat_const_to_int (*(--Data));
#ifdef VLOG  
  fprintf (stderr, "multiplicity %d. *arr = %p\n", multiplicity, *arr);
#endif
  DEC_REF (*Data);
  int i;
  for (i = 0; i < multiplicity; i++) {
    zval **w = get_field (*arr, 0, i);
    #ifdef VLOG  
      fprintf (stderr, "field = %p\n", w ? *w : 0);
    #endif
    if (!w) { return 0; }
    *(++arr) = *w;
    if (TLUNI_START (newIP, Data, arr, vars) != TLUNI_OK) {
      -- arr;
      return 0;
    }
    -- arr;
  }
  TLUNI_NEXT;
}

/****
 *
 * Data: [data] arity => [data]
 * IP  : newIP
 *
 ***/
void *tlcomb_fetch_array (void **IP, void **Data, zval **arr, struct tl_tree **vars) {
  void **newIP = *(IP ++);
  int multiplicity = var_nat_const_to_int (*(--Data));
#ifdef VLOG
  fprintf (stderr, "multiplicity %d\n", multiplicity);
#endif
  DEC_REF (*Data);
  int i;
  ALLOC_INIT_ZVAL (*arr);
  array_init (*arr);
  for (i = 0; i < multiplicity; i++) {
    *(++arr) = 0;
    if (TLUNI_START (newIP, Data, arr, vars) != TLUNI_OK) {
      -- arr;
      return 0;
    }
    add_index_zval (*(arr - 1), i, *arr);
    -- arr;
  }
  TLUNI_NEXT;
}

/*****
 *
 * Data [data] => [data]
 * IP: 
 *
 *****/
void *tlcomb_store_int (void **IP, void **Data, zval **arr, struct tl_tree **vars) {
  int a = parse_zend_long (arr);
#ifdef VLOG
  fprintf (stderr, "Got int %d (0x%08x)\n", a, a);
#endif
  do_rpc_store_int (a);
  TLUNI_NEXT;
}

/*****
 *
 * Data [data] => [data]
 * IP: 
 *
 *****/
void *tlcomb_fetch_int (void **IP, void **Data, zval **arr, struct tl_tree **vars) {
  int a = tl_parse_int ();
#ifdef VLOG
  fprintf (stderr, "!!!%d (0x%08x). error %s\n", a, a, tl_parse_error () ? tl_parse_error () : "none");
#endif
  if (tl_parse_error ()) { return 0; }
  MAKE_STD_ZVAL (*arr);
  ZVAL_LONG (*arr, a);
  TLUNI_NEXT;
}

/*****
 *
 * Data [data] => [data]
 * IP: 
 *
 *****/
void *tlcomb_fetch_long (void **IP, void **Data, zval **arr, struct tl_tree **vars) {
  long long a = tl_parse_long ();
  if (tl_parse_error ()) { return 0; }
  MAKE_STD_ZVAL (*arr);
  ZVAL_LONG (*arr, a);
  TLUNI_NEXT;
}

/*****
 *
 * Data [data] => [data]
 * IP: 
 *
 *****/
void *tlcomb_fetch_double (void **IP, void **Data, zval **arr, struct tl_tree **vars) {
  double a = tl_parse_double ();
  if (tl_parse_error ()) { return 0; }
  MAKE_STD_ZVAL (*arr);
  ZVAL_DOUBLE (*arr, a);
  TLUNI_NEXT;
}

/*****
 *
 * Data [data] => [data]
 * IP: 
 *
 *****/
void *tlcomb_fetch_false (void **IP, void **Data, zval **arr, struct tl_tree **vars) {
  if (tl_parse_error ()) { return 0; }
#ifdef VLOG
  fprintf (stderr, "fetch false\n");
#endif
  MAKE_STD_ZVAL (*arr);
  ZVAL_FALSE (*arr);
  TLUNI_NEXT;
}

/*****
 *
 * Data [data] => [data]
 * IP: 
 *
 *****/
void *tlcomb_fetch_true (void **IP, void **Data, zval **arr, struct tl_tree **vars) {
  if (tl_parse_error ()) { return 0; }
#ifdef VLOG
  fprintf (stderr, "fetch true\n");
#endif
  MAKE_STD_ZVAL (*arr);
  ZVAL_TRUE (*arr);
  TLUNI_NEXT;
}

/*****
 *
 * Data [data] => [data]
 * IP: newIP
 *
 *****/
void *tlcomb_fetch_vector (void **IP, void **Data, zval **arr, struct tl_tree **vars) {
  int n = tl_parse_int ();
  if (tl_parse_error ()) { return 0; }
  ALLOC_INIT_ZVAL (*arr);
  array_init (*arr);
  void **newIP = *(IP ++);

#ifdef VLOG
  fprintf (stderr, "multiplicity %d\n", n);
#endif
  int i;
  for (i = 0; i < n; i++) {
    *(++arr) = 0;
    if (TLUNI_START (newIP, Data, arr, vars) != TLUNI_OK) {
      -- arr;
      return 0;
    }
    add_index_zval (*(arr - 1), i, *arr);
    -- arr;
  }
  TLUNI_NEXT;
}

/*****
 *
 * Data [data] => [data]
 * IP: newIP
 *
 *****/
void *tlcomb_fetch_maybe (void **IP, void **Data, zval **arr, struct tl_tree **vars) {
  void **newIP = *(IP ++);
  if (TLUNI_START (newIP, Data, arr, vars) != TLUNI_OK) {
    -- arr;
    return 0;
  }
  TLUNI_NEXT;
}

/*****
 *
 * Data [data] => [data]
 * IP: 
 *
 *****/
void *tlcomb_fetch_string (void **IP, void **Data, zval **arr, struct tl_tree **vars) {
  char *a;
  int l = tl_eparse_string (&a);
  if (tl_parse_error ()) { return 0; }
  MAKE_STD_ZVAL (*arr);
  (*arr)->type = IS_STRING;
  (*arr)->value.str.len = l;
  (*arr)->value.str.val = a;
  TLUNI_NEXT;
}

/*****
 *
 * Data [data] => [data]
 * IP: var_num
 *
 *****/
void *tlcomb_store_var_num (void **IP, void **Data, zval **arr, struct tl_tree **vars) {
  int var_num = (long)*(IP ++);
  int a = parse_zend_long (arr);
  if (vars[var_num]) {
    DEC_REF (vars[var_num]);
  }
  vars[var_num] = int_to_var_nat_const (a);
  do_rpc_store_int (a);
  TLUNI_NEXT;
}

/*****
 *
 * Data [data] => [data]
 * IP: var_num
 *
 *****/
void *tlcomb_fetch_var_num (void **IP, void **Data, zval **arr, struct tl_tree **vars) {
  int x = tl_parse_int ();
#ifdef VLOG
  fprintf (stderr, "%d\n", x);
#endif
  if (tl_parse_error ()) {
    return 0;
  }
  MAKE_STD_ZVAL (*arr);
  ZVAL_LONG (*arr, x);
  int var_num = (long)*(IP ++);
  if (vars[var_num]) {
    DEC_REF (vars[var_num]);
  }
  vars[var_num] = int_to_var_nat_const (x);
  
  TLUNI_NEXT;
}

/*****
 *
 * Data [data] => [data]
 * IP: var_num
 *
 *****/
void *tlcomb_fetch_check_var_num (void **IP, void **Data, zval **arr, struct tl_tree **vars) {
  int x = tl_parse_int ();
#ifdef VLOG
  fprintf (stderr, "%d\n", x);
#endif
  if (tl_parse_error ()) {
    return 0;
  }
  int var_num = (long)*(IP ++);
  if (x != var_nat_const_to_int (vars[var_num])) {
    return 0;
  }
  TLUNI_NEXT;
}

/*****
 *
 * Data [data] => [data]
 * IP: var_num flags
 *
 *****/
void *tlcomb_store_var_type (void **IP, void **Data, zval **arr, struct tl_tree **vars) {
  int var_num = (long)*(IP ++);
  int flags = (long)*(IP ++);
  int l;
  char *a = parse_zend_string (arr, &l);
  if (!a) { return 0; }
  if (vars[var_num]) {
    DEC_REF (vars[var_num]);
    vars[var_num] = 0;
  }
  struct tl_type *t = tl_type_get_by_id (a);
  if (!t) { return 0; }

  struct tl_tree_type *x = zzemalloc (sizeof (*x));
  dynamic_tree_nodes ++;
  total_tree_nodes_existed ++;
  x->self.ref_cnt = 1;
  total_ref_cnt ++;
  x->self.flags = flags;
  x->self.methods = &tl_type_methods;
  x->children_num = 0;
  x->type = t;
  x->children = 0;
  vars[var_num] = (void *)x;  
  TLUNI_NEXT;
}

/*****
 *
 * Data [data] => [data]
 * IP: 
 *
 *****/
void *tlcomb_store_long (void **IP, void **Data, zval **arr, struct tl_tree **vars) {
  long long a = parse_zend_long (arr);
  do_rpc_store_long (a);
  TLUNI_NEXT;
}

/*****
 *
 * Data [data] => [data]
 * IP: 
 *
 *****/
void *tlcomb_store_double (void **IP, void **Data, zval **arr, struct tl_tree **vars) {
  double a = parse_zend_double (arr);
  do_rpc_store_double (a);
  TLUNI_NEXT;
}

/*****
 *
 * Data [data] => [data]
 * IP: 
 *
 *****/
void *tlcomb_store_vector (void **IP, void **Data, zval **arr, struct tl_tree **vars) {
  void **newIP = *(IP ++);
  if (Z_TYPE_PP (arr) != IS_ARRAY) {
    return 0;
  }
  
  int multiplicity = get_array_size (arr);
  do_rpc_store_int (multiplicity);

  int i;
  for (i = 0; i < multiplicity; i++) {
    zval **w = get_field (*arr, 0, i);
    #ifdef VLOG  
      fprintf (stderr, "field = %p\n", w ? *w : 0);
    #endif
    if (!w) { return 0; }
    *(++arr) = *w;
    if (TLUNI_START (newIP, Data, arr, vars) != TLUNI_OK) {
      -- arr;
      return 0;
    }
    -- arr;
  }
  TLUNI_NEXT;
}

/*****
 *
 * Data [data] => [data]
 * IP: 
 *
 *****/
void *tlcomb_store_string (void **IP, void **Data, zval **arr, struct tl_tree **vars) {
  int len;
  char *s = parse_zend_string (arr, &len);
  do_rpc_store_string (s, len);
  TLUNI_NEXT;
}

/*****
 *
 * Data [data] => [data]
 * IP: var_num bit_num shift
 *
 *****/
void *tlcomb_check_bit (void **IP, void **Data, zval **arr, struct tl_tree **vars) {
  long n = (long)*(IP ++);
  long b = (long)*(IP ++);
  long o = (long)*(IP ++);
#ifdef VLOG  
  fprintf (stderr, "check_bit. var_num = %ld, bit_num = %ld, offset = %ld\n", n, b, o);
#endif
  long x = var_nat_const_to_int (vars[n]);
#ifdef VLOG  
  fprintf (stderr, "check_bit. var_num = %ld, bit_num = %ld, offset = %ld, var_value = %ld\n", n, b, o, x);
#endif
  if (!(x & (1 << b))) {
    IP += o;
  }
  TLUNI_NEXT;
}

/*** }}} ***/

/**** Uniformize code {{{ ****/

/*****
 *
 * Data: [Data] res => [Data] childn ... child2 child1
 * IP: type
 *
 *****/
void *tluni_check_type (void **IP, void **Data, zval **arr, struct tl_tree **vars) {
  struct tl_tree_type *res = *(--Data);
//  fprintf (stderr, "res = %p, res->type = %p, *IP = %p, *IP->id = %s\n", res, res->type, *IP, ((struct tl_type *)*IP)->id);

  if (TL_TREE_METHODS (res)->type ((void *)res) != NODE_TYPE_TYPE) {  return 0; }
  if (res->type != *(IP ++)) {  return 0; }
//  if ((res->self.flags & FLAGS_MASK) != (long)*(IP ++)) { return 0; }

  int i;
  for (i = res->children_num - 1; i >= 0; i--) {
    *(Data ++ ) = res->children[i];
  }
  TLUNI_NEXT;
}

/*****
 *
 * Data: [Data] res => [Data]
 * IP: const
 *
 *****/
void *tluni_check_nat_const (void **IP, void **Data, zval **arr, struct tl_tree **vars) {
  if (*(IP ++) != *(--Data)) {  return 0; }
  TLUNI_NEXT;
}

/*****
 *
 * Data: [Data] res => [Data] childn ... child2 child1 multiplicity
 * IP: array
 *
 *****/
void *tluni_check_array (void **IP, void **Data, zval **arr, struct tl_tree **vars) {
  struct tl_tree_array *res = *(--Data);
  if (TL_TREE_METHODS (res)->type ((void *)res) != NODE_TYPE_ARRAY) {  return 0; }
  if (res->args_num != (long)*(IP ++)) {  return 0; }
  int i;
  for (i = res->args_num - 1; i >= 0; i--) {
    *(Data ++) = res->args[i];
  }
  *(Data ++) = res->multiplicity;
  TLUNI_NEXT;
}

/*****
 *
 * Data [Data] child => [Data] type
 * IP arg_name
 *
 *****/
void *tluni_check_arg (void **IP, void **Data, zval **arr, struct tl_tree **vars) {
  struct arg *res = *(--Data);
  const char *id = *(IP ++);
  if (!id) {
    if (res->id) {  return 0; }
  } else {
    if (!res->id) {  return 0; }
    if (strcmp (id, res->id)) {  return 0; }
  }
  *(Data ++) = res->type;
  TLUNI_NEXT;
}

/*****
 *
 * Data [Data] value => [Data]
 * IP var_num add_value
 *
 *****/
void *tluni_set_nat_var (void **IP, void **Data, zval **arr, struct tl_tree **vars) {
  long p = (long)*(IP ++);  
  if (vars[p]) {
    DEC_REF (vars[p]);
  }
  vars[p] = 0;
  void *a = *(--Data);
  void *x = int_to_var_nat_const (var_nat_const_to_int (a) + (long)*(IP ++));
  //DEC_REF (a);
  //void *x = *(--Data) + 2 * (long)*(IP ++);
  //fprintf (stderr, "c = %lld\n", (long long)var_nat_const_to_int (x));
  //if (!TL_IS_NAT_VAR (x)) {  return 0; }
  if (var_nat_const_to_int (x) < 0) {  
    DEC_REF (x);
    return 0; 
  }
  vars[p] = x;
  TLUNI_NEXT;
}

/*****
 *
 * Data [Data] value => [Data]
 * IP var_num
 *
 *****/
void *tluni_set_type_var (void **IP, void **Data, zval **arr, struct tl_tree **vars) {
  long p = (long)*(IP ++);
  if (vars[p]) {
    DEC_REF (vars[p]);
  }
  vars[p] = 0;
//  fprintf (stderr, "p = %ld, var = %p, var->type = %s\n", p, vars[p], ((struct tl_tree_type *)vars[p])->type->id);
  vars[p] = *(--Data);
  if (TL_IS_NAT_VAR (vars[p])) {  return 0; }
  INC_REF (vars[p]);
  TLUNI_NEXT;
}


/*****
 *
 * Data [Data] value => [Data]
 * IP var_num add_value
 *
 *****/
void *tluni_check_nat_var (void **IP, void **Data, zval **arr, struct tl_tree **vars) {
  long p = (long)*(IP ++);
  //void *x = *(--Data) + 2 * (long)*(IP ++);
  void *x = int_to_var_nat_const (var_nat_const_to_int (--Data) + (long)*(IP ++));
  if (vars[p] != x) {
    DEC_REF (x);
    return 0;
  }
  DEC_REF (x);
  TLUNI_NEXT;
}


/*****
 *
 * Data [Data] value => [Data]
 * IP var_num
 *
 *****/
void *tluni_check_type_var (void **IP, void **Data, zval **arr, struct tl_tree **vars) {
  struct tl_tree *x = *(--Data);
  struct tl_tree *y = vars[(long)*(IP ++)];
  if (!TL_TREE_METHODS (y)->eq (y, x)) {
    return 0;
  }
  TLUNI_NEXT;
}


/*****
 *
 * Data [Data] multiplicity type1 ... typen => [Data] array
 * IP flags args_num idn ... id1
 *
 *****/
void *tlsub_create_array (void **IP, void **Data, zval **arr, struct tl_tree **vars) {
  struct tl_tree_array *x = zzemalloc (sizeof (*x));
  dynamic_tree_nodes ++;
  total_tree_nodes_existed ++;
  x->self.ref_cnt = 1;
  x->self.flags = (long)*(IP ++);
  x->self.methods = &tl_array_methods;
  x->args_num = (long)*(IP ++);
  int i;
  x->args = zzemalloc (sizeof (void *) * x->args_num);
  for (i = x->args_num - 1; i >= 0; i--) {
    x->args[i] = zzemalloc (sizeof (*x->args[i]));
    x->args[i]->id = *(IP ++ );
    x->args[i]->type = *(--Data);
//    TL_TREE_METHODS (x->args[i]->type)->inc_ref (x->args[i]->type);
  }
  x->multiplicity = *(--Data);
#ifdef VLOG
  fprintf (stderr, "Create array\n");
#endif
//  TL_TREE_METHODS (x->multiplicity)->inc_ref (x->multiplicity);
  *(Data ++) = x;
  TLUNI_NEXT;
}


/*****
 *
 * Data [Data] type1 ... typen  => [Data] type
 * IP flags type_ptr
 *
 *****/
void *tlsub_create_type (void **IP, void **Data, zval **arr, struct tl_tree **vars) {
  struct tl_tree_type *x = zzemalloc (sizeof (*x));
  dynamic_tree_nodes ++;
  total_tree_nodes_existed ++;
  x->self.ref_cnt = 1;
  total_ref_cnt ++;
  x->self.flags = (long)*(IP ++);
  x->self.methods = &tl_type_methods;
  x->type = *(IP ++);
#ifdef VLOG
  fprintf (stderr, "Create type %s. flags = %d\n", x->type->id, x->self.flags);
#endif
  x->children_num = x->type->arity;
  x->children = zzemalloc (sizeof (void *) * x->children_num);
  int i;
  for (i = x->children_num - 1; i >= 0; i--) {
    x->children[i] = *(--Data);
//    TL_TREE_METHODS (x->children[i])->inc_ref (x->children[i]);
  }
  *(Data ++) = x;
//  fprintf (stderr, "create type %s\n", x->type->id);
  TLUNI_NEXT;
}

void *tlsub_ret_ok (void **IP, void **Data, zval **arr, struct tl_tree **vars) {
  return TLUNI_OK;
}

void *tlsub_ret (void **IP, void **Data, zval **arr, struct tl_tree **vars) {
  struct tl_tree *x = *(--Data);
//  TL_TREE_METHODS(x)->inc_ref (x);
  return x;
}

void *tlsub_ret_zero (void **IP, void **Data, zval **arr, struct tl_tree **vars) {
  return 0;
}

/*void *tlsub_push_const (void **IP, void **Data, zval **arr, struct tl_tree **vars) {
  *(Data ++) = *(IP ++);
  TLUNI_NEXT;
}*/

void *tlsub_push_type_var (void **IP, void **Data, zval **arr, struct tl_tree **vars) {
  *(Data ++) = vars[(long)*(IP ++)];
#ifdef VLOG
  fprintf (stderr, "Push type var\n");
#endif
  INC_REF (*(Data - 1));
  TLUNI_NEXT;
}

void *tlsub_push_nat_var (void **IP, void **Data, zval **arr, struct tl_tree **vars) {
  int p = (long)*(IP ++);
//  *(Data ++) = ((void *)vars[p]) + 2 * (long) *(IP ++);
//  fprintf (stderr, "vars[p] = %p\n", vars[p]);
  *(Data ++) = int_to_var_nat_const (var_nat_const_to_int (vars[p]) + (long) *(IP ++));
#ifdef VLOG
  fprintf (stderr, "Push nat var. Data = %lld\n", var_nat_const_to_int (*(Data - 1)));
#endif
  TLUNI_NEXT;
}
/* }}} */

/**** Default tree methods {{{ ****/


int tl_tree_eq_type (struct tl_tree *_x, struct tl_tree *_y) {
  if (TL_TREE_METHODS (_y)->type ((void *)_y) != NODE_TYPE_TYPE) { return 0; }
  struct tl_tree_type *x = (void *)_x;
  struct tl_tree_type *y = (void *)_y;
  if ((x->self.flags & FLAGS_MASK) != (y->self.flags & FLAGS_MASK)) { return 0; }
  if (x->type != y->type) { return 0; }
  int i;
  for (i = 0; i < x->children_num; i++) {
    if (!TL_TREE_METHODS (x->children[i])->eq (x->children[i], y->children[i])) {
      return 0;
    }
  }
  return 1;
}

int tl_tree_eq_array (struct tl_tree *_x, struct tl_tree *_y) {
  if (TL_TREE_METHODS (_y)->type ((void *)_y) != NODE_TYPE_ARRAY) { return 0; }
  struct tl_tree_array *x = (void *)_x;
  struct tl_tree_array *y = (void *)_y;
  if (x->self.flags != y->self.flags) { return 0; }
  if (x->args_num != y->args_num) { return 0; }
  int i;
  for (i = 0; i < x->args_num; i++) {
    if (!x->args[i]->id) {
      if (y->args[i]->id) { return 0; }
    } else {
      if (!y->args[i]->id) { return 0; }
      if (strcmp (x->args[i]->id, y->args[i]->id)) { return 0; }
    }
    if (!TL_TREE_METHODS (x->args[i]->type)->eq (x->args[i]->type, y->args[i]->type)) {
      return 0;
    }
  }
  return 1;
}

int tl_tree_eq_nat_const (struct tl_tree *x, struct tl_tree *y) {
  //return (x == y);
  return var_nat_const_to_int (x) == var_nat_const_to_int (y);
}

void tl_tree_inc_ref_type (struct tl_tree *x) {
  total_ref_cnt ++;
  x->ref_cnt ++;
  //fprintf (stderr, "Inc_ref: type %s\n", ((struct tl_tree_type *)x)->type->id);
}

void tl_tree_inc_ref_array (struct tl_tree *x) {
  total_ref_cnt ++;
  //fprintf (stderr, "Inc_ref: array\n");
  x->ref_cnt ++;
}

void tl_tree_inc_ref_nat_const (struct tl_tree *x) {
}

void tl_tree_inc_ref_nat_const_full (struct tl_tree *x) {
  total_ref_cnt ++;
  x->ref_cnt ++;
}

void tl_tree_dec_ref_type (struct tl_tree *_x) {
  total_ref_cnt --;
  struct tl_tree_type *x = (void *)_x;
#ifdef VLOG
  fprintf (stderr, "Dec_ref: type %s. Self ref_cnt = %d, children_num = %d\n", x->type->id, x->self.ref_cnt, x->children_num);
#endif
  x->self.ref_cnt --;
  if (x->self.ref_cnt > 0) { return; }
  dynamic_tree_nodes --;
  int i;
  for (i = 0; i < x->children_num; i++) {
    DEC_REF (x->children[i]);
  }
  zzefree (x->children, sizeof (void *) * x->children_num);
  zzefree (x, sizeof (*x));
}

void tl_tree_dec_ref_array (struct tl_tree *_x) {
  total_ref_cnt --;
#ifdef VLOG
  fprintf (stderr, "Dec_ref: array\n");
#endif
  struct tl_tree_array *x = (void *)_x;
  x->self.ref_cnt --;
  if (x->self.ref_cnt > 0) { return; }
  dynamic_tree_nodes --;
  int i;
  DEC_REF (x->multiplicity);
  for (i = 0; i < x->args_num; i--) {
    DEC_REF (x->args[i]->type);
    zzefree (x->args[i], sizeof (*x->args[i]));
  }
  zzefree (x->args, sizeof (void *) * x->args_num);
  zzefree (x, sizeof (*x));
}

void tl_tree_dec_ref_ptype (struct tl_tree *_x) {
  total_ref_cnt --;
  struct tl_tree_type *x = (void *)_x;
#ifdef VLOG
  fprintf (stderr, "Dec_ref: persistent type %s. Self ref_cnt = %d, children_num = %d\n", x->type->id, x->self.ref_cnt, x->children_num);
#endif
  x->self.ref_cnt --;
  if (x->self.ref_cnt > 0) { return; }
  persistent_tree_nodes --;
  int i;
  for (i = 0; i < x->children_num; i++) {
    DEC_REF (x->children[i]);
  }
  zzfree (x->children, sizeof (void *) * x->children_num);
  ADD_PFREE (sizeof (void *) * x->children_num);
  zzfree (x, sizeof (*x));
  ADD_PFREE (sizeof (*x));
}

void tl_tree_dec_ref_pvar_type (struct tl_tree *_x) {
  total_ref_cnt --;
  struct tl_tree_var_type *x = (void *)_x;
#ifdef VLOG
  fprintf (stderr, "Dec_ref: persistent var_type\n");
#endif
  x->self.ref_cnt --;
  if (x->self.ref_cnt > 0) { return; }
  persistent_tree_nodes --;
  zzfree (x, sizeof (*x));
  ADD_PFREE (sizeof (*x));
}

void tl_tree_dec_ref_pvar_num (struct tl_tree *_x) {
  total_ref_cnt --;
  struct tl_tree_var_num *x = (void *)_x;
#ifdef VLOG
  fprintf (stderr, "Dec_ref: persistent var_num\n");
#endif
  x->self.ref_cnt --;
  if (x->self.ref_cnt > 0) { return; }
  persistent_tree_nodes --;
  zzfree (x, sizeof (*x));
  ADD_PFREE (sizeof (*x));
}

void tl_tree_dec_ref_parray (struct tl_tree *_x) {
  total_ref_cnt --;
#ifdef VLOG
  fprintf (stderr, "Dec_ref: persistent array\n");
#endif
  struct tl_tree_array *x = (void *)_x;
  x->self.ref_cnt --;
  if (x->self.ref_cnt > 0) { return; }
  persistent_tree_nodes --;
  int i;
  DEC_REF (x->multiplicity);
  for (i = 0; i < x->args_num; i++) {
    DEC_REF (x->args[i]->type);
    if (x->args[i]->id) {
      ADD_PFREE (strlen (x->args[i]->id));
      zzstrfree (x->args[i]->id);
//      free (x->args[i]->id);
    }
    zzfree (x->args[i], sizeof (*x->args[i]));
    ADD_PFREE (sizeof (*(x->args[i])));
  }
  zzfree (x->args, sizeof (void *) * x->args_num);
  ADD_PFREE (sizeof (void *) * x->args_num);
  zzfree (x, sizeof (*x));
  ADD_PFREE (sizeof (*x));
}

void tl_tree_dec_ref_nat_const (struct tl_tree *_x) {
  return;
}

void tl_tree_dec_ref_nat_const_full (struct tl_tree *_x) {
  total_ref_cnt --;
  _x->ref_cnt --;
  if (_x->ref_cnt > 0) { return; }
  dynamic_tree_nodes --;
  zzefree (_x, sizeof (struct tl_tree_nat_const));
  return;
}

void tl_tree_dec_ref_pnat_const_full (struct tl_tree *_x) {
  _x->ref_cnt --;
  total_ref_cnt --;
  if (_x->ref_cnt > 0) { return; }
  persistent_tree_nodes --;
  zzfree (_x, sizeof (struct tl_tree_nat_const));
  ADD_FREE (sizeof (struct tl_tree_nat_const));
  return;
}

int tl_tree_type_type (struct tl_tree *x) {
  return NODE_TYPE_TYPE;
}

int tl_tree_type_array (struct tl_tree *x) {
  return NODE_TYPE_ARRAY;
}

int tl_tree_type_nat_const (struct tl_tree *x) {
  return NODE_TYPE_NAT_CONST;
}

int tl_tree_type_var_num (struct tl_tree *x) {
  return NODE_TYPE_VAR_NUM;
}

int tl_tree_type_var_type (struct tl_tree *x) {
  return NODE_TYPE_VAR_TYPE;
}

struct tl_tree_methods tl_var_num_methods = {
  .eq = 0,
  .dec_ref = 0,
  .inc_ref = 0,
  .type = tl_tree_type_var_num
};

struct tl_tree_methods tl_var_type_methods = {
  .eq = 0,
  .dec_ref = 0,
  .inc_ref = 0,
  .type = tl_tree_type_var_type
};

struct tl_tree_methods tl_type_methods = {
  .eq = tl_tree_eq_type,
  .dec_ref = tl_tree_dec_ref_type,
  .inc_ref = tl_tree_inc_ref_type,
  .type = tl_tree_type_type
};

struct tl_tree_methods tl_nat_const_methods = {
  .eq = tl_tree_eq_nat_const,
  .dec_ref = tl_tree_dec_ref_nat_const,
  .inc_ref = tl_tree_inc_ref_nat_const,
  .type = tl_tree_type_nat_const
};

struct tl_tree_methods tl_array_methods = {
  .eq = tl_tree_eq_array,
  .dec_ref = tl_tree_dec_ref_array,
  .inc_ref = tl_tree_inc_ref_array,
  .type = tl_tree_type_array
};

struct tl_tree_methods tl_ptype_methods = {
  .eq = tl_tree_eq_type,
  .dec_ref = tl_tree_dec_ref_ptype,
  .inc_ref = tl_tree_inc_ref_type,
  .type = tl_tree_type_type
};

struct tl_tree_methods tl_parray_methods = {
  .eq = tl_tree_eq_array,
  .dec_ref = tl_tree_dec_ref_parray,
  .inc_ref = tl_tree_inc_ref_array,
  .type = tl_tree_type_array
};

struct tl_tree_methods tl_pvar_num_methods = {
  .eq = 0,
  .dec_ref = tl_tree_dec_ref_pvar_num,
  .inc_ref = 0,
  .type = tl_tree_type_var_num
};

struct tl_tree_methods tl_pvar_type_methods = {
  .eq = 0,
  .dec_ref = tl_tree_dec_ref_pvar_type,
  .inc_ref = 0,
  .type = tl_tree_type_var_type
};

struct tl_tree_methods tl_nat_const_full_methods = {
  .eq = tl_tree_eq_nat_const,
  .dec_ref = tl_tree_dec_ref_nat_const_full,
  .inc_ref = tl_tree_inc_ref_nat_const_full,
  .type = tl_tree_type_nat_const
};

struct tl_tree_methods tl_pnat_const_full_methods = {
  .eq = tl_tree_eq_nat_const,
  .dec_ref = tl_tree_dec_ref_pnat_const_full,
  .inc_ref = tl_tree_inc_ref_nat_const_full,
  .type = tl_tree_type_nat_const
};

/* }}} */

/* gen IP {{{ */
void **IP_dup (void **x, int l) {
  void **r = zzmalloc (sizeof (void *) * l);
  ADD_PMALLOC (sizeof (void *) * l);
  memcpy (r, x, sizeof (void *) * l);
  return r;
}

int gen_uni (struct tl_tree *t, void **IP, int max_size, int *vars);
int gen_uni_arg (struct arg *arg, void **IP, int max_size, int *vars) {
  if (max_size <= 10) { return -1; }
  int l = 0;
  IP[l ++] = tluni_check_arg;
  IP[l ++] = arg->id;
  int y = gen_uni (arg->type, IP + l, max_size - l, vars);
  if (y < 0) { return -1;}
  l += y;
  return l;
}

int gen_uni (struct tl_tree *t, void **IP, int max_size, int *vars) {
  if (max_size <= 10) { return -1; }
  assert (t);
  int x = TL_TREE_METHODS (t)->type (t);
  int l = 0;
  int i;
  int j;
  struct tl_tree_type *t1;
  struct tl_tree_array *t2;
  int y;
  switch (x) {
  case NODE_TYPE_TYPE:
    t1 = (void *)t;
    IP[l ++] = tluni_check_type;
    IP[l ++] = ((struct tl_tree_type *)t)->type;
    for (i = 0; i < t1->children_num; i++) {
      y = gen_uni (t1->children[i], IP + l, max_size - l, vars);
      if (y < 0) { return -1; }
      l += y;
    }
    return l;
  case NODE_TYPE_NAT_CONST:
    IP[l ++] = tluni_check_nat_const;
    IP[l ++] = t;
    return l;
  case NODE_TYPE_ARRAY:
    t2 = (void *)t;
    IP[l ++] = tluni_check_array;
    IP[l ++] = t;
    y = gen_uni (t2->multiplicity, IP + l, max_size - l, vars);
    if (y < 0) { return -1; }
    l += y;
    for (i = 0; i < t2->args_num; i++) {
      y += gen_uni_arg (t2->args[i], IP + l, max_size - l, vars);
      if (y < 0) { return -1; }
      l += y;
    }
    return l;
  case NODE_TYPE_VAR_TYPE:
    i = ((struct tl_tree_var_type *)t)->var_num;
    if (!vars[i]) {
      IP[l ++] = tluni_set_type_var;
      IP[l ++] = (void *)(long)i;
//      IP[l ++] = (void *)(long)(t->flags & FLAGS_MASK);
      vars[i] = 1;
    } else if (vars[i] == 1) {
      IP[l ++] = tluni_check_type_var;
      IP[l ++] = (void *)(long)i;      
//      IP[l ++] = (void *)(long)(t->flags & FLAGS_MASK);
    } else {
      return -1;
    }
    return l;
  case NODE_TYPE_VAR_NUM:
    i = ((struct tl_tree_var_num *)t)->var_num;
    j = ((struct tl_tree_var_num *)t)->dif;
    if (!vars[i]) {
      IP[l ++] = tluni_set_nat_var;
      IP[l ++] = (void *)(long)i;
      IP[l ++] = (void *)(long)j;
      vars[i] = 2;
    } else if (vars[i] == 2) {
      IP[l ++] = tluni_check_nat_var;
      IP[l ++] = (void *)(long)i;
      IP[l ++] = (void *)(long)j;
    } else {
      return -1;
    }
    return l;
  default:
    assert (0);
    return -1;
  }
}

int gen_create (struct tl_tree *t, void **IP, int max_size, int *vars) {
  if (max_size <= 10) { return -1; }
  int x = TL_TREE_METHODS (t)->type (t);
  int l = 0;
  if (!TL_IS_NAT_VAR (t) && (t->flags & FLAG_NOVAR)) {
    IP[l ++] = tls_push;
//    TL_TREE_METHODS (t)->inc_ref (t);
    IP[l ++] = t;
    return l;
  }
  int i;
  int y;
  struct tl_tree_type *t1;
  struct tl_tree_array *t2;
  switch (x) {
  case NODE_TYPE_TYPE:
    t1 = (void *)t;
    for (i = 0; i < t1->children_num; i++) {
      y = gen_create (t1->children[i], IP + l, max_size - l, vars);
      if (y < 0) { return -1; }
      l += y;
    }
    if (l + 10 >= max_size) { return -1; }
    IP[l ++] = tlsub_create_type;
    IP[l ++] = (void *)(long)(t1->self.flags & FLAGS_MASK);
    IP[l ++] = t1->type;
    return l;
  case NODE_TYPE_NAT_CONST:
    IP[l ++] = tls_push;
    IP[l ++] = t;
    return l;
  case NODE_TYPE_ARRAY:
    t2 = (void *)t;
    assert (t2->multiplicity);
    y = gen_create (t2->multiplicity, IP + l, max_size - l, vars);
    if (y < 0) { return -1; }
    l += y;

    for (i = 0; i < t2->args_num; i++) {
      assert (t2->args[i]);
      //y = gen_field (t2->args[i], IP + l, max_size - l, vars, i);
      y = gen_create (t2->args[i]->type, IP + l, max_size - l, vars);
      if (y < 0) { return -1; }
      l += y;
    }
    if (l + 10 + t2->args_num >= max_size) { return -1; }

    IP[l ++] = tlsub_create_array;
    IP[l ++] = (void *)(long)(t2->self.flags & FLAGS_MASK);
    IP[l ++] = (void *)(long)t2->args_num;
    for (i = t2->args_num - 1; i >= 0; i--) {
      IP[l ++] = t2->args[i]->id;
    }
    return l;
  case NODE_TYPE_VAR_TYPE:
    IP[l ++] = tlsub_push_type_var;
    IP[l ++] = (void *)(long)((struct tl_tree_var_type *)t)->var_num;
    //IP[l ++] = (void *)(long)(t->flags & FLAGS_MASK);
    return l;
  case NODE_TYPE_VAR_NUM:
    IP[l ++] = tlsub_push_nat_var;
    IP[l ++] = (void *)(long)((struct tl_tree_var_num *)t)->var_num;
    IP[l ++] = (void *)(long)((struct tl_tree_var_num *)t)->dif;
    return l;
  default:
    assert (0);
    return -1;
  }
}

int gen_field (struct arg *arg, void **IP, int max_size, int *vars, int num, int flat);
int gen_array_store (struct tl_tree_array *a, void **IP, int max_size, int *vars) {
  if (max_size <= 10) { return -1; }
  int l = 0;
  int i;
  if (a->args_num > 1) {
    for (i = 0; i < a->args_num; i++) {
      int x = gen_field (a->args[i], IP + l, max_size - l, vars, i, 0); 
      if (x < 0) { return -1; }
      l += x;
    }
  } else {
    int x = gen_field (a->args[0], IP + l, max_size - l, vars, 0, 1); 
    if (x < 0) { return -1; }
    l += x;
  }
  if (max_size - l <= 10) { return -1; }
  IP[l ++] = tlsub_ret_ok;
//  c->IP = IP_dup (IP, l);
  return l;
}

int gen_field_fetch (struct arg *arg, void **IP, int max_size, int *vars, int num, int flat);
int gen_array_fetch (struct tl_tree_array *a, void **IP, int max_size, int *vars) {
  if (max_size <= 10) { return -1; }
  int l = 0;
  if (a->args_num > 1) {
    int i;
    for (i = 0; i < a->args_num; i++) {
      int x = gen_field_fetch (a->args[i], IP + l, max_size - l, vars, i, 0); 
      if (x < 0) { return -1; }
      l += x;
    }
  } else {
    int x = gen_field_fetch (a->args[0], IP + l, max_size - l, vars, 0, 1); 
    if (x < 0) { return -1; }
    l += x;
  }
  if (max_size - l <= 10) { return -1; }
  IP[l ++] = tlsub_ret_ok;
//  c->IP = IP_dup (IP, l);
  return l;
}

int gen_constructor_store (struct tl_combinator *c, void **IP, int max_size);
int gen_constructor_fetch (struct tl_combinator *c, void **IP, int max_size);
int gen_field (struct arg *arg, void **IP, int max_size, int *vars, int num, int flat) {
  assert (arg);
  if (max_size <= 10) { return -1; }
  int l = 0;
  if (arg->exist_var_num >= 0) {
    IP[l ++] = tlcomb_check_bit;
    IP[l ++] = (void *)(long)arg->exist_var_num;
    IP[l ++] = (void *)(long)arg->exist_var_bit;
    IP[l ++] = 0;
  }
  if (!flat) {
    IP[l ++] = tlcomb_store_field;
    IP[l ++] = arg->id;
    IP[l ++] = (void *)(long)num;
  } else {
    assert (!num);
  }
  if (arg->var_num >= 0) {
    assert (TL_TREE_METHODS (arg->type)->type (arg->type) == NODE_TYPE_TYPE);
    int t = ((struct tl_tree_type *)arg->type)->type->name;
    if (t == NAME_VAR_TYPE) {
      IP[l ++] = tlcomb_store_var_type;
      IP[l ++] = (void *)(long)arg->var_num;
      IP[l ++] = (void *)(long)(arg->type->flags & FLAGS_MASK);
    } else {
      assert (t == NAME_VAR_NUM);
      IP[l ++] = tlcomb_store_var_num;
      IP[l ++] = (void *)(long)arg->var_num;
    }
  } else {
    int t = TL_TREE_METHODS (arg->type)->type (arg->type);
    if ((t == NODE_TYPE_TYPE || t == NODE_TYPE_VAR_TYPE)) {
      struct tl_tree_type *t1 = (void *)(arg->type);
        
      if (0 && t == NODE_TYPE_TYPE && t1->type->arity == 0 && t1->type->constructors_num == 1) {      
        if (!(t1->self.flags & FLAG_BARE)) {
          IP[l ++] = tlcomb_store_const_int;
          IP[l ++] = (void *)(long)t1->type->constructors[0]->name;
        }
        if (!t1->type->constructors[0]->IP_len) {
          if (gen_constructor_store (t1->type->constructors[0], IP + l, max_size - l) < 0) {
            return -1;
          }
        }
        void **_IP = t1->type->constructors[0]->IP;
        assert (_IP[0] == tluni_check_type);
        assert (_IP[1] == t1->type); 
        if (l + t1->type->constructors[0]->IP_len + 10 > max_size) { return -1; }
        memcpy (IP + l, _IP + 2, sizeof (void *) * (t1->type->constructors[0]->IP_len - 2));
        
        l += t1->type->constructors[0]->IP_len - 2;
        assert (IP[l - 1] == tlsub_ret_ok);
        l --;
      } else {
        int r = gen_create (arg->type, IP + l, max_size - l, vars);
        if (r < 0) { return -1; }
        l += r;
        if (l + 10 > max_size) { return -1; }
        IP[l ++] = tlcomb_store_type;    
      }
    } else {
      assert (t == NODE_TYPE_ARRAY);
      int r = gen_create (((struct tl_tree_array *)arg->type)->multiplicity, IP + l, max_size - l, vars);
      if (r < 0) { return -1; }
      l += r;
      if (l + 10 > max_size) { return -1; }
      IP[l ++] = tlcomb_store_array;
      void *newIP[1000];
      int v = gen_array_store (((struct tl_tree_array *)arg->type), newIP, 1000, vars);
      if (v < 0) { return -1; }
      IP[l ++] = IP_dup (newIP, v);
    }
  }
  if (l + 10 > max_size) { return -1; }
  if (!flat) {
    IP[l ++] = tls_arr_pop;
  }
  if (arg->exist_var_num >= 0) {
    IP[3] = (void *)(long)(l - 4);
  }
  return l;
}

int gen_field_fetch (struct arg *arg, void **IP, int max_size, int *vars, int num, int flat) {
  assert (arg);
  if (max_size <= 30) { return -1; }
  int l = 0;
  if (arg->exist_var_num >= 0) {
    IP[l ++] = tlcomb_check_bit;
    IP[l ++] = (void *)(long)arg->exist_var_num;
    IP[l ++] = (void *)(long)arg->exist_var_bit;
    IP[l ++] = 0;
//    fprintf (stderr, "r = %d\n", l);
//    fprintf (stderr, "n = %ld\n", (long)(IP[1]));
//    fprintf (stderr, "b = %ld\n", (long)(IP[2]));
  }
  if (!flat) {
    IP[l ++] = tls_arr_push;  
  }
  if (arg->var_num >= 0) {
    assert (TL_TREE_METHODS (arg->type)->type (arg->type) == NODE_TYPE_TYPE);
    int t = ((struct tl_tree_type *)arg->type)->type->name;
    if (t == NAME_VAR_TYPE) {
      fprintf (stderr, "Not supported yet\n");
      assert (0);
    } else {
      assert (t == NAME_VAR_NUM);
      if (vars[arg->var_num] == 0) {
        IP[l ++] = tlcomb_fetch_var_num;
        IP[l ++] = (void *)(long)arg->var_num;
        vars[arg->var_num] = 2;
      } else if (vars[arg->var_num] == 2) {
        IP[l ++] = tlcomb_fetch_check_var_num;
        IP[l ++] = (void *)(long)arg->var_num;
      } else {
        return -1;
      }
    }
  } else {
    int t = TL_TREE_METHODS (arg->type)->type (arg->type);
    if (t == NODE_TYPE_TYPE || t == NODE_TYPE_VAR_TYPE) {
      struct tl_tree_type *t1 = (void *)(arg->type);
        
      if (0 && t == NODE_TYPE_TYPE && t1->type->arity == 0 && t1->type->constructors_num == 1) {      
        if (!(t1->self.flags & FLAG_BARE)) {
          IP[l ++] = tlcomb_skip_const_int;
          IP[l ++] = (void *)(long)t1->type->constructors[0]->name;
        }
        if (!t1->type->constructors[0]->fIP_len) {
          if (gen_constructor_fetch (t1->type->constructors[0], IP + l, max_size - l) < 0) {
            return -1;
          }
        }
        void **_IP = t1->type->constructors[0]->fIP;
        assert (_IP[0] == tluni_check_type);
        assert (_IP[1] == t1->type); 
        if (l + t1->type->constructors[0]->fIP_len + 10 > max_size) { return -1; }
        memcpy (IP + l, _IP + 2, sizeof (void *) * (t1->type->constructors[0]->fIP_len - 2));
        
        l += t1->type->constructors[0]->fIP_len - 2;
        assert (IP[l - 1] == tlsub_ret_ok);
        l --;
      } else {
        int r = gen_create (arg->type, IP + l, max_size - l, vars);
        if (r < 0) { return -1; }
        l += r;
        if (l + 10 > max_size) { return -1; }
        IP[l ++] = tlcomb_fetch_type;    
      }
    } else {
      assert (t == NODE_TYPE_ARRAY);
      int r = gen_create (((struct tl_tree_array *)arg->type)->multiplicity, IP + l, max_size - l, vars);
      if (r < 0) { return -1; }
      l += r;
      if (l + 10 > max_size) { return -1; }
      IP[l ++] = tlcomb_fetch_array;
      void *newIP[1000];
      int v = gen_array_fetch (((struct tl_tree_array *)arg->type), newIP, 1000, vars);
      if (v < 0) { return -1; }
      IP[l ++] = IP_dup (newIP, v);
    }
  }
  if (l + 10 > max_size) { return -1; }
  if (!flat) {
    IP[l ++] = tlcomb_fetch_field_end;
    IP[l ++] = arg->id;
    IP[l ++] = (void *)(long)num;
  }
  if (arg->exist_var_num >= 0) {
    IP[3] = (void *)(long)(l - 4);
//    fprintf (stderr, "r = %d\n", l);
//    fprintf (stderr, "n = %ld\n", (long)(IP[1]));
//    fprintf (stderr, "b = %ld\n", (long)(IP[2]));
  }
  return l;
}

int gen_field_excl (struct arg *arg, void **IP, int max_size, int *vars, int num) {
  assert (arg);
  if (max_size <= 10) { return -1; }
  int l = 0;
  IP[l ++] = tlcomb_store_field;
  IP[l ++] = arg->id;
  IP[l ++] = (void *)(long)num;

  //fprintf (stderr, "arg->var_num = %d, arg->flags = %x\n", arg->var_num, arg->flags);
  assert (arg->var_num < 0);
  int t = TL_TREE_METHODS (arg->type)->type (arg->type);
  assert (t == NODE_TYPE_TYPE || t == NODE_TYPE_VAR_TYPE);
  IP[l ++] = tlcomb_store_any_function;
  int x = gen_uni (arg->type, IP + l, max_size - l, vars);
  if (x < 0) { return -1;}
  l += x;
  if (max_size - l <= 10) { return -1; }

  IP[l ++] = tls_arr_pop;
  return l;
}

int gen_constructor_store (struct tl_combinator *c, void **IP, int max_size) {
  assert (IP);
  if (c->IP) { return c->IP_len; }
  if (max_size <= 10) { return -1; }
  int l = 0;
  assert (!c->IP);
  int i;
  int vars[c->var_num];
  memset (vars, 0, sizeof (int) * c->var_num);
  int x = gen_uni (c->result, IP + l, max_size - l, vars);
  if (x < 0) { return -1; }
  l += x;
  if (c->name == NAME_INT) {
    IP[l ++] = tlcomb_store_int;
    IP[l ++] = tlsub_ret_ok;
    c->IP = IP_dup (IP, l);
    c->IP_len = l;
    return l;
  } else if (c->name == NAME_LONG) {
    IP[l ++] = tlcomb_store_long;
    IP[l ++] = tlsub_ret_ok;
    c->IP = IP_dup (IP, l);
    c->IP_len = l;
    return l;
  } else if (c->name == NAME_STRING) {
    IP[l ++] = tlcomb_store_string;
    IP[l ++] = tlsub_ret_ok;
    c->IP = IP_dup (IP, l);
    c->IP_len = l;
    return l;
  } else if (c->name == NAME_DOUBLE) {
    IP[l ++] = tlcomb_store_double;
    IP[l ++] = tlsub_ret_ok;
    c->IP = IP_dup (IP, l);
    c->IP_len = l;
    return l;
  } else if (c->name == NAME_VECTOR) {
    IP[l ++] = tlcomb_store_vector;
    static void *tIP[4];
    tIP[0] = tlsub_push_type_var;
    tIP[1] = (long)0;
    tIP[2] = tlcomb_store_type;
    tIP[3] = tlsub_ret_ok; 
    IP[l ++] = IP_dup (tIP, 4);
    IP[l ++] = tlsub_ret_ok;
    c->IP = IP_dup (IP, l);
    c->IP_len = l;
    return l;
  }
  if (verbosity >= 2) {
    fprintf (stderr, "c->id = %s, c->args_num = %d, c->args = %p\n", c->id, c->args_num, c->args);
  }

  if (TYPE (c->result) == NODE_TYPE_TYPE) {
    struct tl_type *t = ((struct tl_tree_type *)(c->result))->type;
    if (t->constructors_num == 1) {
      int x = -1;
      int z = 0;
      for (i = 0; i < c->args_num; i++) if (!(c->args[i]->flags & FLAG_OPT_VAR)) {
        z ++;
        x = i;
      }
      if (z == 1) { 
        x = gen_field (c->args[x], IP + l, max_size - l, vars, 0, 1);
        if (x < 0) { return -1; }
        l += x;
        if (max_size - l <= 10) { return -1; }
        IP[l ++] = tlsub_ret_ok;
        c->IP = IP_dup (IP, l);
        c->IP_len = l;
        return l;
      }
    }
  }

  assert (!c->args_num || (c->args && c->args[0]));
  for (i = 0; i < c->args_num; i++) if (!(c->args[i]->flags & FLAG_OPT_VAR)) {
//  fprintf (stderr, "%d\n", __LINE__);
    x = gen_field (c->args[i], IP + l, max_size - l, vars, i + 1, 0);
//  fprintf (stderr, "%d\n", __LINE__);
    if (x < 0) { return -1; }
    l += x;
//    fprintf (stderr, ".");
  }
  if (max_size - l <= 10) { return -1; }
  IP[l ++] = tlsub_ret_ok;
  c->IP = IP_dup (IP, l);
  c->IP_len = l;
  return l;
}

int gen_function_store (struct tl_combinator *c, void **IP, int max_size) {
  if (max_size <= 10) { return -1; }
  assert (!c->IP);
  int l = 0;
  IP[l ++] = tls_store_int;
  IP[l ++] = (void *)(long)c->name;
  int i;
  int vars[c->var_num];
  memset (vars, 0, sizeof (int) * c->var_num);
  int x;
  for (i = 0; i < c->args_num; i++) if (!(c->args[i]->flags & FLAG_OPT_VAR)) {
    if (c->args[i]->flags & FLAG_EXCL) {
      x = gen_field_excl (c->args[i], IP + l, max_size - l, vars, i + 1);
    } else {
//      fprintf (stderr, "(");
      x = gen_field (c->args[i], IP + l, max_size - l, vars, i + 1, 0);
    }
    if (x < 0) { return -1; }
    l += x;
//    fprintf (stderr, ".");
  }
  int r = gen_create (c->result, IP + l, max_size - l, vars);
  if (r < 0) { return -1; }
  l += r;
  if (max_size - l <= 10) { return -1; }
  IP[l ++] = tlsub_ret;
  c->IP = IP_dup (IP, l);
  c->IP_len = l;
  return l;
}

int gen_constructor_fetch (struct tl_combinator *c, void **IP, int max_size) {
  if (c->fIP) { return c->fIP_len; }
  if (max_size <= 10) { return -1; }
  int l = 0;
  assert (!c->fIP);
  int i;
  int vars[c->var_num];
  memset (vars, 0, sizeof (int) * c->var_num);
  int x = gen_uni (c->result, IP + l, max_size - l, vars);
  if (x < 0) { return -1; }
  l += x;
  if (c->name == NAME_INT) {
    IP[l ++] = tlcomb_fetch_int;
    IP[l ++] = tlsub_ret_ok;
    c->fIP = IP_dup (IP, l);
    c->fIP_len = l;
    return l;
  } else if (c->name == NAME_LONG) {
    IP[l ++] = tlcomb_fetch_long;
    IP[l ++] = tlsub_ret_ok;
    c->fIP = IP_dup (IP, l);
    c->fIP_len = l;
    return l;
  } else if (c->name == NAME_STRING) {
    IP[l ++] = tlcomb_fetch_string;
    IP[l ++] = tlsub_ret_ok;
    c->fIP = IP_dup (IP, l);
    c->fIP_len = l;
    return l;
  } else if (c->name == NAME_DOUBLE) {
    IP[l ++] = tlcomb_fetch_double;
    IP[l ++] = tlsub_ret_ok;
    c->fIP = IP_dup (IP, l);
    c->fIP_len = l;
    return l;
  } else if (c->name == NAME_VECTOR) {
    IP[l ++] = tlcomb_fetch_vector;
    static void *tIP[4];
    tIP[0] = tlsub_push_type_var;
    tIP[1] = (long)0;
    tIP[2] = tlcomb_fetch_type;
    tIP[3] = tlsub_ret_ok; 
    IP[l ++] = IP_dup (tIP, 4);
    IP[l ++] = tlsub_ret_ok;
    c->fIP = IP_dup (IP, l);
    c->fIP_len = l;
    return l;
  } else if (c->name == NAME_MAYBE_TRUE) {
    IP[l ++] = tlcomb_fetch_maybe;
    static void *tIP[4];
    tIP[0] = tlsub_push_type_var;
    tIP[1] = (long)0;
    tIP[2] = tlcomb_fetch_type;
    tIP[3] = tlsub_ret_ok; 
    IP[l ++] = IP_dup (tIP, 4);
    IP[l ++] = tlsub_ret_ok;
    c->fIP = IP_dup (IP, l);
    c->fIP_len = l;
    return l;
  } else if (c->name == NAME_MAYBE_FALSE || c->name == NAME_BOOL_FALSE) {
    IP[l ++] = tlcomb_fetch_false;
    IP[l ++] = tlsub_ret_ok;
    c->fIP = IP_dup (IP, l);
    c->fIP_len = l;
    return l;
  } else if (c->name == NAME_BOOL_TRUE) {
    IP[l ++] = tlcomb_fetch_true;
    IP[l ++] = tlsub_ret_ok;
    c->fIP = IP_dup (IP, l);
    c->fIP_len = l;
    return l;
  }

  if (TYPE (c->result) == NODE_TYPE_TYPE) {
    struct tl_type *t = ((struct tl_tree_type *)(c->result))->type;
    if (t->constructors_num == 1) {
      int x = -1;
      int z = 0;
      for (i = 0; i < c->args_num; i++) if (!(c->args[i]->flags & FLAG_OPT_VAR)) {
        z ++;
        x = i;
      }
      if (z == 1) { 
        x = gen_field_fetch (c->args[x], IP + l, max_size - l, vars, x + 1, 1);
        if (x < 0) { return -1; }
        l += x;
        if (max_size - l <= 10) { return -1; }
        IP[l ++] = tlsub_ret_ok;
        c->fIP = IP_dup (IP, l);
        c->fIP_len = l;
        return l;
      }
    }
  }
  for (i = 0; i < c->args_num; i++) if (!(c->args[i]->flags & FLAG_OPT_VAR)) {
    x = gen_field_fetch (c->args[i], IP + l, max_size - l, vars, i + 1, 0);
    if (x < 0) { return -1; }
    l += x;
//    fprintf (stderr, ".");
  }
  if (max_size - l <= 10) { return -1; }
  IP[l ++] = tlsub_ret_ok;
  c->fIP = IP_dup (IP, l);
  c->fIP_len = l;
  return l;
}

int gen_function_fetch (void **IP, int max_size) {
  if (max_size <= 10) { return -1; }
  int l = 0;
  IP[l ++] = tlcomb_fetch_type;
  IP[l ++] = tlsub_ret_ok;
  fIP = IP_dup (IP, l);
  return 2;
}
/* }}} */

/* {{{ Destructors */
int tl_constructors;
int tl_types;
int tl_functions;

void delete_combinator (struct tl_combinator *c) {
  if (!c) { return; }
  if (c->is_fun) {
    tl_functions --;
  } else {
    tl_constructors --;
  }
  if (c->IP) {
    zzfree (c->IP, c->IP_len);
    ADD_PFREE (c->IP_len);
  }
  if (c->fIP) {
    zzfree (c->fIP, c->fIP_len);
    ADD_PFREE (c->fIP_len);
  }
  if (c->result) {
    DEC_REF (c->result);
  }
  if (c->id) {
    ADD_PFREE (strlen (c->id));
    zzstrfree (c->id);
  }
  int i;
  if (c->args_num && c->args) {
    for (i = 0; i < c->args_num; i++) if (c->args[i]) {
      if (c->args[i]->type) {
        DEC_REF (c->args[i]->type);
      }
      zzfree (c->args[i], sizeof (*c->args[i]));
      ADD_PFREE (sizeof (*c->args[i]));
    }
    zzfree (c->args, sizeof (void *) * c->args_num);
    ADD_PFREE (sizeof (void *) * c->args_num);
  }
  zzfree (c, sizeof (*c)); 
  ADD_PFREE (sizeof (*c));
}

void delete_type (struct tl_type *t) {
  if (!t) { return; }
  if (t->id) {
    ADD_PFREE (strlen (t->id));
    zzstrfree (t->id);
  }
  if (t->constructors_num && t->constructors) {
    int i;
    for (i = 0; i < t->extra; i++) {
      delete_combinator (t->constructors[i]);
    }
    zzfree (t->constructors, t->constructors_num * sizeof (void *));
    ADD_PFREE (t->constructors_num * sizeof (void *));
  }
  tl_types --;
  zzfree (t, sizeof (*t));
  ADD_PFREE (sizeof (*t));
}

void tl_config_delete (struct tl_config *config) {
  config_list[config->pos] = 0;
  int size = sizeof (struct hash_table_tl_fun_id);
  hash_clear_tl_fun_id (config->ht_fid);
  hash_clear_tl_type_id (config->ht_tid);
  hash_clear_tl_fun_name (config->ht_fname);
  hash_clear_tl_type_name (config->ht_tname);
  zzfree (config->ht_tid->E, sizeof (void *) * (1 << 12));
  zzfree (config->ht_tid, size);
  zzfree (config->ht_fid->E, sizeof (void *) * (1 << 12));
  zzfree (config->ht_fid, size);
  zzfree (config->ht_tname->E, sizeof (void *) * (1 << 12));
  zzfree (config->ht_tname, size);
  zzfree (config->ht_fname->E, sizeof (void *) * (1 << 12));
  zzfree (config->ht_fname, size);
  ADD_PFREE ((size + (1 << 12) * sizeof (void *)) * 4);
  int i;
  for (i = 0; i < config->tn; i++) {
    if (config->tps[i]) {
      delete_type (config->tps[i]);
    }
  }
  zzfree (config->tps, config->tn * sizeof (void *));
  ADD_PFREE (config->tn * sizeof (void *));
  for (i = 0; i < config->fn; i++) {
    if (config->fns[i]) {
      delete_combinator (config->fns[i]);
    }
  }
  zzfree (config->fns, config->fn * sizeof (void *));
  ADD_PFREE (config->fn * sizeof (void *));

  zzfree (config, sizeof (*config));
  ADD_PFREE (sizeof (*config));
}

void tl_delete_old_configs (void) {
  int i;
  for (i = 0; i < CONFIG_LIST_SIZE; i++) if (config_list[i] && config_list[i] != cur_config) {
    tl_config_delete (config_list[i]);
  }
}

void tl_config_alloc (void) {
  cur_config = zzmalloc0 (sizeof (*cur_config));
  ADD_PMALLOC (sizeof (*cur_config));
  config_list_pos ++;
  if (config_list_pos >= CONFIG_LIST_SIZE) {
    config_list_pos -= CONFIG_LIST_SIZE;
  }
  config_list[config_list_pos] = cur_config;
  int size = sizeof (struct hash_table_tl_fun_id);
  cur_config->ht_tid = zzmalloc (size);
  cur_config->ht_tid->size = (1 << 12);
  cur_config->ht_tid->mask = (1 << 12) - 1;
  cur_config->ht_tid->E = zzmalloc0 (sizeof (void *) * (1 << 12));
  cur_config->ht_fid = zzmalloc (size);
  cur_config->ht_fid->size = (1 << 12);
  cur_config->ht_fid->mask = (1 << 12) - 1;
  cur_config->ht_fid->E = zzmalloc0 (sizeof (void *) * (1 << 12));
  cur_config->ht_tname = zzmalloc (size);
  cur_config->ht_tname->size = (1 << 12);
  cur_config->ht_tname->mask = (1 << 12) - 1;
  cur_config->ht_tname->E = zzmalloc0 (sizeof (void *) * (1 << 12));
  cur_config->ht_fname = zzmalloc (size);
  cur_config->ht_fname->size = (1 << 12);
  cur_config->ht_fname->mask = (1 << 12) - 1;
  cur_config->ht_fname->E = zzmalloc0 (sizeof (void *) * (1 << 12));
  cur_config->pos = config_list_pos;

  ADD_PMALLOC (size * 4);
  ADD_PMALLOC (sizeof (void *) * (1 << 12) * 4);
}

void tl_config_back (void) {
  tl_config_delete (cur_config);
  config_list_pos --;
  if (config_list_pos < 0) {
    config_list_pos += CONFIG_LIST_SIZE;
  }
  cur_config = config_list[config_list_pos];
}
/* }}} */

/* read_cfg {{{ */
int __fd;
unsigned long long config_crc64 = -1;
int schema_version;
/*
int read_int (void) {
  int x = 0;
  assert (read (__fd, &x, 4) == 4);
  config_crc64 = ~crc64_partial (&x, 4, ~config_crc64);
  return x;
}

int lookup_int (void) {
  int x = 0;
  assert (read (__fd, &x, 4) == 4);
  lseek (__fd, -4, SEEK_CUR);
  return x;
}

int try_read_int (void) {
  int x = 0;
  int z = read (__fd, &x, 4);
  if (z == 4) {
    config_crc64 = ~crc64_partial (&x, 4, ~config_crc64);
    return x;
  } else {
    return 0;
  }
}

long long read_long (void) {
  long long x = 0;
  assert (read (__fd, &x, 8) == 8);
  config_crc64 = ~crc64_partial (&x, 8, ~config_crc64);
  return x;
}

char *read_string (void) {
  if (schema_version < 1) {
    int x = read_int ();
    if (x == 0) { return 0; }
    char *s = zzmalloc (x + 1);
    ADD_PMALLOC (x + 1);
    s[x] = 0;
    assert (read (__fd, s, x) == x);
    config_crc64 = ~crc64_partial (s, x, ~config_crc64);
    return s;
  } else {
    int x = read_char ();
  }
}*/

struct tl_tree *read_tree (int *var_num);
struct tl_tree *read_nat_expr (int *var_num);
struct tl_tree *read_type_expr (int *var_num);
int read_args_list (struct arg **args, int args_num, int *var_num);
struct tl_tree *read_num_const (int *var_num) {
  void *res = (void *)int_to_var_nat_const_init (schema_version >= 2 ? tl_parse_int () : tl_parse_long ());
  return tl_parse_error () ? 0 : res;
}

struct tl_tree *read_num_var (int *var_num) {
  struct tl_tree_var_num *T = zzmalloc (sizeof (*T));
  ADD_PMALLOC (sizeof (*T));
  T->self.flags = 0;
  T->self.ref_cnt = 1;
  total_ref_cnt ++;
  persistent_tree_nodes ++;
  total_tree_nodes_existed ++;
  T->self.methods = &tl_pvar_num_methods;;  
  if (schema_version >= 2) {
    T->dif = tl_parse_int ();
  } else {
    T->dif = tl_parse_long ();
  }
  T->var_num = tl_parse_int ();
  if (tl_parse_error ()) {
    return 0;
  }
  if (T->var_num >= *var_num) {
    *var_num = T->var_num + 1;
  }
  if (T->self.flags & FLAG_NOVAR) {
    return 0;
  }
  return (void *)T;
}

struct tl_tree *read_type_var (int *var_num) {
  struct tl_tree_var_type *T = zzmalloc0 (sizeof (*T));
  ADD_PMALLOC (sizeof (*T));
//  T->self.flags = 0;
  T->self.ref_cnt = 1;
  total_ref_cnt ++;
  persistent_tree_nodes ++;
  total_tree_nodes_existed ++;
  T->self.methods = &tl_pvar_type_methods;
  T->var_num = tl_parse_int ();
  T->self.flags = tl_parse_int ();
  if (tl_parse_error ()) {
    return 0;
  }
  if (T->var_num >= *var_num) {  
    *var_num = T->var_num + 1;
  }
  if (T->self.flags & (FLAG_NOVAR | FLAG_BARE)) {
    return 0;
  }
  return (void *)T;
}

struct tl_tree *read_array (int *var_num) {
  struct tl_tree_array *T = zzmalloc0 (sizeof (*T));
  ADD_PMALLOC (sizeof (*T));
  T->self.ref_cnt = 1;
  total_ref_cnt ++;
  persistent_tree_nodes ++;
  total_tree_nodes_existed ++;
  T->self.methods = &tl_parray_methods;
  T->self.flags = 0;
  if (schema_version >= 2) {
    T->multiplicity = read_nat_expr (var_num);
  } else {
    T->multiplicity = read_tree (var_num);
  }
  if (!T->multiplicity) {
    return 0;
  }
  T->args_num = tl_parse_int ();
  if (T->args_num <= 0 || T->args_num > 1000 || tl_parse_error ()) {
    return 0;
  }
  T->args = zzmalloc0 (sizeof (void *) * T->args_num);
  ADD_PMALLOC (sizeof (void *) * T->args_num);
  if (read_args_list (T->args, T->args_num, var_num) < 0) {
    return 0;
  }
  T->self.flags |= FLAG_NOVAR;
  int i;
  for (i = 0; i < T->args_num; i++) {
    if (!(T->args[i]->flags & FLAG_NOVAR)) {
      T->self.flags &= ~FLAG_NOVAR;
    }
  }
  return (void *)T;
}

struct tl_tree *read_type (int *var_num) {
  struct tl_tree_type *T = zzmalloc0 (sizeof (*T));
  ADD_PMALLOC (sizeof (*T));
  T->self.ref_cnt = 1;
  total_ref_cnt ++;
  persistent_tree_nodes ++;
  total_tree_nodes_existed ++;
  T->self.methods = &tl_ptype_methods;
 
  T->type = tl_type_get_by_name (tl_parse_int ());
  if (!T->type) {
    return 0;
  }
  T->self.flags = tl_parse_int ();
  T->children_num = tl_parse_int ();
  if (verbosity >= 2) {
    fprintf (stderr, "T->flags = %d, T->chilren_num = %d\n", T->self.flags, T->children_num);
  }
  if (tl_parse_error () || T->type->arity != T->children_num) {
    return 0;
  }
  if (T->children_num < 0 || T->children_num > 1000) {
    return 0;
  }
  T->children = zzmalloc0 (sizeof (void *) * T->children_num);
  ADD_PMALLOC (sizeof (void *) * T->children_num);
  int i;
  T->self.flags |= FLAG_NOVAR;
  for (i = 0; i < T->children_num; i++) {
    if (schema_version >= 2) {
      int t = tl_parse_int ();
      if (tl_parse_error ()) {
        return 0;
      }
      if (t == TLS_EXPR_NAT) {
        if (!(T->type->params_types & (1 << i))) {
          return 0;
        }
        T->children[i] = read_nat_expr (var_num);
      } else if (t == TLS_EXPR_TYPE) {
        if ((T->type->params_types & (1 << i))) {
          return 0;
        }
        T->children[i] = read_type_expr (var_num);
      } else {
        return 0;
      }
    } else {
      T->children[i] = read_tree (var_num);
    }
    if (!T->children[i]) {
      return 0;
    }
    if (!TL_IS_NAT_VAR (T->children[i]) && !(T->children[i]->flags & FLAG_NOVAR)) {
      T->self.flags &= ~FLAG_NOVAR;
    }
  }
  return (void *)T;
}

struct tl_tree *read_tree (int *var_num) {
  int x = tl_parse_int ();
  if (verbosity >= 2) {
    fprintf (stderr, "read_tree: constructor = 0x%08x\n", x);
  }
  switch (x) {
  case TLS_TREE_NAT_CONST:
    return read_num_const (var_num);
  case TLS_TREE_NAT_VAR:
    return read_num_var (var_num);
  case TLS_TREE_TYPE_VAR:
    return read_type_var (var_num);
  case TLS_TREE_TYPE:
    return read_type (var_num);
  case TLS_TREE_ARRAY:
    return read_array (var_num);
  default:
    if (verbosity) {
      fprintf (stderr, "x = %d\n", x);
    }
    return 0;
  }    
}

struct tl_tree *read_type_expr (int *var_num) {
  int x = tl_parse_int ();
  if (verbosity >= 2) {
    fprintf (stderr, "read_type_expr: constructor = 0x%08x\n", x);
  }
  switch (x) {
  case TLS_TYPE_VAR:
    return read_type_var (var_num);
  case TLS_TYPE_EXPR:
    return read_type (var_num);
  case TLS_ARRAY:
    return read_array (var_num);
  default:
    if (verbosity) {
      fprintf (stderr, "x = %d\n", x);
    }
    return 0;
  }     
}

struct tl_tree *read_nat_expr (int *var_num) {
  int x = tl_parse_int ();
  if (verbosity >= 2) {
    fprintf (stderr, "read_nat_expr: constructor = 0x%08x\n", x);
  }
  switch (x) {
  case TLS_NAT_CONST:
    return read_num_const (var_num);
  case TLS_NAT_VAR:
    return read_num_var (var_num);
  default:
    if (verbosity) {
      fprintf (stderr, "x = %d\n", x);
    }
    return 0;
  }     
}

struct tl_tree *read_expr (int *var_num) {
  int x = tl_parse_int ();
  if (verbosity >= 2) {
    fprintf (stderr, "read_nat_expr: constructor = 0x%08x\n", x);
  }
  switch (x) {
  case TLS_EXPR_NAT:
    return read_nat_expr (var_num);
  case TLS_EXPR_TYPE:
    return read_type_expr (var_num);
  default:
    if (verbosity) {
      fprintf (stderr, "x = %d\n", x);
    }
    return 0;
  }
}

int read_args_list (struct arg **args, int args_num, int *var_num) {
  int i;
  for (i = 0; i < args_num; i++) {
    args[i] = zzmalloc0 (sizeof (struct arg));
    args[i]->exist_var_num = -1;
    args[i]->exist_var_bit = 0;
    ADD_PMALLOC (sizeof (struct arg));
    if (schema_version == 1) {
      if (tl_parse_int () != TLS_ARG) {
        return -1;
      }
    } else {
      if (tl_parse_int () != TLS_ARG_V2) {
        return -1;
      }
    }
    if (tl_parse_string (&args[i]->id) < 0) {
      return -1;
    }
    ADD_MALLOC (strlen (args[i]->id));
    ADD_PMALLOC (strlen (args[i]->id));
    args[i]->flags = tl_parse_int ();
    if (schema_version >= 2) {
      if (args[i]->flags & 2) {
        args[i]->flags &= ~2;
        args[i]->flags |= (1 << 20);
      }
      if (args[i]->flags & 4) {
        args[i]->flags &= ~4;
        args[i]->var_num = tl_parse_int ();
      } else {
        args[i]->var_num = -1;
      }
    } else {
      args[i]->var_num = tl_parse_int ();
    }
    if (args[i]->var_num >= *var_num) {
      *var_num = args[i]->var_num + 1;
    }
    if (args[i]->flags & FLAG_OPT_FIELD) {
      args[i]->exist_var_num = tl_parse_int ();
      args[i]->exist_var_bit = tl_parse_int ();
    }
    if (schema_version >= 2) {
      args[i]->type = read_type_expr (var_num);
    } else {
      args[i]->type = read_tree (var_num);
    }
    if (!args[i]->type) {
      return -1;
    }
    if (args[i]->var_num < 0 && args[i]->exist_var_num < 0 && (TL_IS_NAT_VAR(args[i]->type) || (args[i]->type->flags & FLAG_NOVAR))) {
      args[i]->flags |= FLAG_NOVAR;
    }
  }
  return 1;
}

int read_combinator_args_list (struct tl_combinator *c) {
  c->args_num = tl_parse_int ();
  if (verbosity >= 2) {
    fprintf (stderr, "c->id = %s, c->args_num = %d\n", c->id, c->args_num);
  }
  if (c->args_num < 0 || c->args_num > 1000) {
    return -1;
  }
  c->args = zzmalloc0 (sizeof (void *) * c->args_num);
  c->var_num = 0;
  ADD_PMALLOC (sizeof (void *) * c->args_num);
  return read_args_list (c->args, c->args_num, &c->var_num);
}

int read_combinator_right (struct tl_combinator *c) {
  if (schema_version >= 2) {
    if (tl_parse_int () != TLS_COMBINATOR_RIGHT_V2 || tl_parse_error ()) {
      return -1;
    }
    c->result = read_type_expr (&c->var_num);
  } else {
    if (tl_parse_int () != TLS_COMBINATOR_RIGHT || tl_parse_error ()) {
      return -1;
    }
    c->result = read_tree (&c->var_num);
  }
  if (!c->result) {
    return -1;
  }
  return 1;
}

int read_combinator_left (struct tl_combinator *c) {
  
  int x = tl_parse_int ();
  if (tl_parse_error ()) {
    return -1;
  }

  if (x == TLS_COMBINATOR_LEFT_BUILTIN) {
    c->args_num = 0;
    c->var_num = 0;
    c->args = 0;
    return 1;
  } else if (x == TLS_COMBINATOR_LEFT) {
    return read_combinator_args_list (c);
  } else {
    return -1;
  }
}

struct tl_combinator *read_combinators (int v) {
  struct tl_combinator *c = zzmalloc0 (sizeof (*c));
  c->name = tl_parse_int ();
  if (tl_parse_error () || tl_parse_string (&c->id) < 0) {
    zzfree (c, sizeof (*c));
    return 0;
  }
  ADD_MALLOC (strlen (c->id));
  ADD_PMALLOC (strlen (c->id));
  int x = tl_parse_int ();
  struct tl_type *t = tl_type_get_by_name (x);
  if (!t && (x || v != 3)) {
    ADD_PFREE (strlen (c->id));
    zzfree (c->id, strlen (c->id));
    zzfree (c, sizeof (*c));
    return 0;
  }
  assert (t || (!x && v == 3));
  if (v == 2) {
    if (t->extra >= t->constructors_num) {
      zzfree (c, sizeof (*c));
      return 0;
    }
    assert (t->extra < t->constructors_num);
    t->constructors[t->extra ++] = c;
    tl_constructors ++;
    c->is_fun = 0;
  } else {
    assert (v == 3);
    tl_fun_insert_id (c);
    tl_fun_insert_name (c);
    tl_functions ++;
    c->is_fun = 1;
  }
  if (read_combinator_left (c) < 0) { 
    //delete_combinator (c);
    return 0; 
  }
  if (read_combinator_right (c) < 0) { 
    //delete_combinator (c);
    return 0; 
  }
  ADD_PMALLOC (sizeof (*c));
  return c;
}

struct tl_type *read_types (void) {
  struct tl_type *t = zzmalloc0 (sizeof (*t));
  t->name = tl_parse_int ();
  if (tl_parse_error () || tl_parse_string (&t->id) < 0) {
    zzfree (t, sizeof (*t));
    return 0;
  }
  ADD_MALLOC (strlen (t->id));
  ADD_PMALLOC (strlen (t->id));
  
  t->constructors_num = tl_parse_int ();
  if (tl_parse_error () || t->constructors_num < 0 || t->constructors_num > 1000) {
    free (t->id);
    zzfree (t, sizeof (*t));
    return 0;
  }
  t->constructors = zzmalloc0 (sizeof (void *) * t->constructors_num);
  t->flags = tl_parse_int ();
  if (!strcmp (t->id, "Maybe") || !strcmp (t->id, "Bool")) {
    t->flags |= FLAG_NOCONS;
  }
  t->arity = tl_parse_int ();
  t->params_types = tl_parse_long (); // params_types
  t->extra = 0;
  if (tl_parse_error ()) {
    free (t->id);
    zzfree (t->constructors, sizeof (void *) * t->constructors_num);
    zzfree (t, sizeof (*t));
    return 0;
  }
  tl_type_insert_name (t);
  tl_type_insert_id (t);
  tl_types ++;
  ADD_PMALLOC (sizeof (*t));
  ADD_PMALLOC (sizeof (void *) * t->constructors_num);
  //fprintf (stderr, "Adding type %s. Name %d\n", t->id, t->name);
  return t;
}


int MAGIC = 0x850230aa;
char *tl_config_name = 0;
int tl_config_date = 0;
int tl_config_version = 0;

int get_schema_version (int a) {
  if (a == TLS_SCHEMA) {
    return 1;
  } if (a == TLS_SCHEMA_V2) {
    return 2;
  } else {
    return -1;
  }
}

int renew_tl_config (void) {
  if (verbosity >= 2) {
    fprintf (stderr, "Starting config renew\n");
  }
  tl_parse_init ();
  int x = tl_parse_int ();
  if (verbosity >= 2) {
    fprintf (stderr, "Schema 0x%08x\n", x);
  }
  schema_version = get_schema_version (x);
  if (verbosity >= 2) {
    fprintf (stderr, "Schema version %d\n", schema_version);
  }
  if (schema_version <= 0 || tl_parse_error ()) {
    return -1;
  }

  int new_tl_config_version = tl_parse_int ();
  int new_tl_config_date = tl_parse_int ();

  if (tl_parse_error ()) {
    return -1;
  }

  long long new_crc64 = crc64_partial (inbuf->rptr, ((char *)inbuf->wptr) - (char *)inbuf->rptr,-1ll);
  if (new_tl_config_version < tl_config_version || (new_tl_config_version == tl_config_version && new_tl_config_date < tl_config_date) || config_crc64 == new_crc64) {
    return 0;
  }

  tl_config_alloc ();
//  int x;

//  struct tl_type *tps [10000];
  int tn = 0;
//  struct tl_combinator *fns [10000];
  int fn = 0;
  int cn;
  int i;

  tn = tl_parse_int ();
  if (tn < 0 || tn > 10000 || tl_parse_error ()) {
    tl_config_back ();
    return -1;
  }
  
  cur_config->tps = zzmalloc0 (sizeof (void *) * tn);
  cur_config->tn = tn;
  ADD_PMALLOC (tn * sizeof (void *));
  struct tl_type **tps = cur_config->tps;
  if (verbosity >= 2) {
    fprintf (stderr, "Found %d types\n", tn);
  }

  for (i = 0; i < tn; i++) {
    if (tl_parse_int () != TLS_TYPE) { 
      tl_config_back ();
      return -1; 
    }
    tps[i] = read_types ();
    if (!tps[i]) { 
      tl_config_back ();
      return -1; 
    }
  }

  cn = tl_parse_int ();  
  if (cn < 0 || tl_parse_error ()) {
    tl_config_back ();
    return -1;
  }
  cur_config->cn = cn;

  if (verbosity >= 2) {
    fprintf (stderr, "Found %d constructors\n", cn);
  }

  for (i = 0; i < cn; i++) {
    if (tl_parse_int () != TLS_COMBINATOR) { 
      tl_config_back ();
      return -1; 
    }
    if (!read_combinators (2)) {
      tl_config_back ();
      return -1;
    }
  }
  fn = tl_parse_int ();
  if (fn < 0 || fn > 10000 || tl_parse_error ()) {
    tl_config_back ();
    return -1;
  }
  cur_config->fn = fn;
  cur_config->fns = zzmalloc0 (sizeof (void *) * fn);
  ADD_PMALLOC (fn * sizeof (void *));
  struct tl_combinator **fns = cur_config->fns;
  
  if (verbosity >= 2) {
    fprintf (stderr, "Found %d functions\n", fn);
  }

  for (i = 0; i < fn; i++) {
    if (tl_parse_int () != TLS_COMBINATOR) { 
      tl_config_back ();
      return -1; 
    }
    fns[i] = read_combinators (3);
    if (!fns[i]) { 
      tl_config_back ();
      return -1; 
    }
  }
  tl_parse_end ();
  if (tl_parse_error ()) {
    tl_config_back ();
    return -1;
  }
  static void *IP[10000];
  if (gen_function_fetch (IP, 100) < 0) {
    return -2;
  }
  for (i = 0; i < tn; i++) {
    if (tps[i]->extra < tps[i]->constructors_num) {
      tl_config_back ();
      return -1;
    }
  }
  int j;
  for (i = 0; i < tn; i++) {
    for (j = 0; j < tps[i]->constructors_num; j ++) {
      if (gen_constructor_store (tps[i]->constructors[j], IP, 10000) < 0) {
        return -2;
      }
      if (gen_constructor_fetch (tps[i]->constructors[j], IP, 10000) < 0) {
        return -2;
      }
    }
  }  
  for (i = 0; i < fn; i++) {
    if (gen_function_store (fns[i], IP, 10000) < 0) {
      return -2;
    }
  }
  if (tl_config_name) {
    ADD_PFREE (strlen (tl_config_name));
    zzstrfree (tl_config_name);
  }
  tl_config_name = 0;
  config_crc64 = new_crc64;
  tl_config_version = new_tl_config_version ;
  tl_config_date = new_tl_config_date;
  return 1;
}

#define MAX_TL_CONFIG_SIZE (1 << 20)
int read_tl_config (const char *name) {
  int fd = open (name, O_RDONLY);
//  fprintf (stderr, "fd = %d\n", fd);
  if (fd < 0) { return -1; }
  long long x = lseek (fd, 0, SEEK_END);
  if (x < 0) { return -1; }
  if (x > MAX_TL_CONFIG_SIZE) {
    return -1;
  }
  if (verbosity >= 2) {
    fprintf (stderr, "File found. Name %s. size = %lld\n", name, x);
  }
  char *s = malloc (x);
  assert (lseek (fd, 0, SEEK_SET) == 0);
  assert (read (fd, s, x) == x);
  close (fd);
  do_rpc_parse (s, x);
  int res = renew_tl_config ();
  free (s);
  if (res > 0) {
    tl_config_name = zzstrdup (name);
    ADD_PMALLOC (strlen (tl_config_name));
  }

  return res;
}
/* }}} */
