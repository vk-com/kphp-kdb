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

#ifndef	__NET_MEMCACHE_CLIENT__
#define	__NET_MEMCACHE_CLIENT__


struct memcache_client_functions {
  void *info;
  int (*execute)(struct connection *c, int op);		/* invoked from parse_execute() */
  int (*check_ready)(struct connection *c);		/* invoked from mc_client_check_ready() */
  int (*flush_query)(struct connection *c);		/* execute this to push query to server */
  int (*connected)(struct connection *c); /* arseny30: invoked from mcc_connected() */
  int (*mc_check_perm)(struct connection *c);		/* 1 = allow unencrypted, 2 = allow encrypted */
  int (*mc_init_crypto)(struct connection *c);  	/* 1 = ok, DELETE sent; -1 = no crypto */
  int (*mc_start_crypto)(struct connection *c, char *key, int key_len);  /* 1 = ok, DELETE sent; -1 = no crypto */
};

extern conn_type_t ct_memcache_client;
extern struct memcache_client_functions default_memcache_client;

/* in conn->custom_data */
struct mcc_data {
  int response_len;
  int response_type;
  int response_flags;
  int key_offset;
  int key_len;
  int arg_num;
  long long args[4];
  int clen;
  char comm[16];
  char nonce[16];
  int nonce_time;
  int crypto_flags;					/* 1 = allow unencrypted, 2 = allow encrypted, 4 = DELETE sent, waiting for NONCE/NOT_FOUND, 8 = encryption ON */
};

/* for mcc_data.response_type */
enum mc_response_type {
  mcrt_none,
  mcrt_empty,
  mcrt_VALUE,
  mcrt_NONCE,
  mcrt_NUMBER,
  mcrt_VERSION,
  mcrt_CLIENT_ERROR,
  mcrt_SERVER_ERROR,
  mcrt_STORED,
  mcrt_NOTSTORED,
  mcrt_DELETED,
  mcrt_NOTFOUND,
  mcrt_ERROR,
  mcrt_END
};

#define	MCC_DATA(c)	((struct mcc_data *) ((c)->custom_data))
#define	MCC_FUNC(c)	((struct memcache_client_functions *) ((c)->extra))


int mcc_execute (struct connection *c, int op);
int mc_client_check_ready (struct connection *c);
int mcc_init_outbound (struct connection *c);

int mcc_flush_query (struct connection *c);
int mcc_default_check_perm (struct connection *c);
int mcc_init_crypto (struct connection *c);
int mcc_start_crypto (struct connection *c, char *key, int key_len);

/* useful functions */
/*int return_one_key (struct connection *c, const char *key, char *val, int vlen);
int return_one_key_list (struct connection *c, const char *key, int key_len, int res, int mode, const int *R, int R_cnt);*/

/* END */
#endif
