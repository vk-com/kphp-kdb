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

    Copyright 2010-2013 Vkontakte Ltd
              2010-2013 Nikolai Durov
              2010-2013 Andrei Lopatin
                   2013 Vitaliy Valtman
*/

#ifndef	__NET_RPC_SERVER__
#define	__NET_RPC_SERVER__

#include "net-rpc-common.h"

struct rpc_server_functions {
  void *info;
  int (*execute)(struct connection *c, int op, int len);/* invoked from parse_execute() */
  int (*check_ready)(struct connection *c);		/* invoked from rpc_client_check_ready() */
  int (*flush_packet)(struct connection *c);		/* execute this to push response to client */
  int (*rpc_check_perm)(struct connection *c);	/* 1 = allow unencrypted, 2 = allow encrypted */
  int (*rpc_init_crypto)(struct connection *c, struct rpc_nonce_packet *P);  /* 1 = ok; -1 = no crypto */
  void *nop;
  int (*rpc_wakeup)(struct connection *c);
  int (*rpc_alarm)(struct connection *c);
  int (*rpc_ready)(struct connection *c);
  int (*rpc_close)(struct connection *c, int who);
  int max_packet_len, mode_flags;
  void *memcache_fallback_type, *memcache_fallback_extra;
  void *http_fallback_type, *http_fallback_extra;
};

#define	RPCS_DATA(c)	((struct rpcs_data *) ((c)->custom_data))
#define	RPCS_FUNC(c)	((struct rpc_server_functions *) ((c)->extra))

extern conn_type_t ct_rpc_server;
extern conn_type_t ct_rpc_ext_server;
extern struct rpc_server_functions default_rpc_server;

int rpcs_default_execute (struct connection *c, int op, int len);
int rpcs_do_wakeup (struct connection *c);
int rpcs_parse_execute (struct connection *c);
int rpcs_alarm (struct connection *c);
int rpcs_init_accepted (struct connection *c);
int rpcs_init_accepted_nohs (struct connection *c);
int rpcs_default_check_perm (struct connection *c);
int rpcs_init_crypto (struct connection *c, struct rpc_nonce_packet *P);
int rpcs_flush_packet (struct connection *c);
int rpcs_close_connection (struct connection *c, int who);
int rpcs_flush (struct connection *c);

void init_server_PID (unsigned ip, int port);

/* END */
#endif
