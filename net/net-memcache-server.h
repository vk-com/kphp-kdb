/*
    This file is part of VK/KittenPHP-DB-Engine Library.

    VK/KittenPHP-DB-Engine Library is free software: you can redistribute it and/or modify
    it under the terms of the GNU Lesser General Public License as published by
    the Free Software Foundation, either version 2 of the License, or
    (at your option) any later version.

    VK/KittenPHP-DB-Engine Library is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Lesser General Public License for more details.

    You should have received a copy of the GNU Lesser General Public License
    along with VK/KittenPHP-DB-Engine Library.  If not, see <http://www.gnu.org/licenses/>.

    Copyright 2009-2013 Vkontakte Ltd
              2009-2013 Nikolai Durov
              2009-2013 Andrei Lopatin
*/

#ifndef	__NET_MEMCACHE_SERVER__
#define	__NET_MEMCACHE_SERVER__


struct memcache_server_functions {
  void *info;
  int (*execute)(struct connection *c, int op);		/* invoked from parse_execute() */
  int (*mc_store)(struct connection *c, int op, const char *key, int key_len, int flags, int delay, int size);
  int (*mc_get_start)(struct connection *c);
  int (*mc_get)(struct connection *c, const char *key, int key_len);
  int (*mc_get_end)(struct connection *c, int key_count);
  int (*mc_incr)(struct connection *c, int op, const char *key, int key_len, long long arg);
  int (*mc_delete)(struct connection *c, const char *key, int key_len);
  int (*mc_stats)(struct connection *c);
  int (*mc_version)(struct connection *c);
  int (*mc_wakeup)(struct connection *c);
  int (*mc_alarm)(struct connection *c);
  int (*mc_check_perm)(struct connection *c);	/* 1 = allow unencrypted, 2 = allow encrypted */
  int (*mc_init_crypto)(struct connection *c, char *auth_str, int auth_len);  /* 1 = ok, NONCE sent; -1 = no crypto */
};

/* in conn->custom_data */
struct mcs_data {
  int query_len;
  int query_type;
  int query_flags;
  int key_offset;
  int key_len;
  int arg_num;
  long long args[4];
  int clen;
  char comm[16];
  int get_count;
  int crypto_flags;				/* 1 = allow unencrypted, 2 = allow encrypted, 8 = encryption ON */
  int complete_count;
  int last_complete_count;
};

/* for mcs_data.query_type */
enum mc_query_type {
  mct_none,
  mct_set,
  mct_replace,
  mct_add,
  mct_get,
  mct_get_resume,
  mct_incr,
  mct_decr,
  mct_delete,
  mct_stats,
  mct_version,
  mct_empty,
  mct_set_resume,
  mct_replace_resume,
  mct_add_resume,
  mct_CLOSE,
  mct_QUIT
};

#define	MCS_DATA(c)	((struct mcs_data *) ((c)->custom_data))
#define	MCS_FUNC(c)	((struct memcache_server_functions *) ((c)->extra))

extern conn_type_t ct_memcache_server;
extern struct memcache_server_functions default_memcache_server;

// generate 'false' answers for get queries without explicit answer automatically :
// 0 = disabled (default), 1 = enabled for all, -1 = enabled for !-keys
extern int memcache_auto_answer_mode;  

int mcs_execute (struct connection *c, int op);
int mcs_store (struct connection *c, int op, const char *key, int key_len, int flags, int delay, int size);
int mcs_get_start (struct connection *c);
int mcs_get (struct connection *c, const char *key, int key_len);
int mcs_get_end (struct connection *c, int key_count);
int mcs_incr (struct connection *c, int op, const char *key, int key_len, long long arg);
int mcs_delete (struct connection *c, const char *key, int key_len);
int mcs_stats (struct connection *c);
int mcs_version (struct connection *c);
int mcs_do_wakeup (struct connection *c);
int mcs_parse_execute (struct connection *c);
int mcs_alarm (struct connection *c);
int mcs_init_accepted (struct connection *c);
int mcs_default_check_perm (struct connection *c);
int mcs_init_crypto (struct connection *c, char *auth_key, int auth_len);

void mcs_pad_response (struct connection *c);

/* useful functions */
int return_one_key (struct connection *c, const char *key, char *val, int vlen);
int return_one_key_list (struct connection *c, const char *key, int key_len, int res, int mode, const int *R, int R_cnt);
int return_one_key_list_long (struct connection *c, const char *key, int key_len, int res, int mode, const long long *R, int R_cnt);
int return_one_key_false (struct connection *c, const char *key, int key_len);

/* END */
#endif
