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

#ifndef	__NET_RPC_CLIENT__
#define	__NET_RPC_CLIENT__

#include "net-rpc-common.h"

struct rpc_client_functions {
  void *info;
  int (*execute)(struct connection *c, int op, int len);	/* invoked from parse_execute() */
  int (*check_ready)(struct connection *c);		/* invoked from rpc_client_check_ready() */
  int (*flush_packet)(struct connection *c);		/* execute this to push query to server */
  int (*rpc_check_perm)(struct connection *c);		/* 1 = allow unencrypted, 2 = allow encrypted */
  int (*rpc_init_crypto)(struct connection *c);  	/* 1 = ok; -1 = no crypto */
  int (*rpc_start_crypto)(struct connection *c, char *nonce, int key_select);  /* 1 = ok; -1 = no crypto */
  int (*rpc_wakeup)(struct connection *c);
  int (*rpc_alarm)(struct connection *c);
  int (*rpc_ready)(struct connection *c);
  int (*rpc_close)(struct connection *c, int who);
  int max_packet_len, mode_flags;
};

extern conn_type_t ct_rpc_client;
extern conn_type_t ct_rpc_ext_client;
extern struct rpc_client_functions default_rpc_client;

#define	RPCC_DATA(c)	((struct rpcc_data *) ((c)->custom_data))

#define	RPCC_DATA(c)	((struct rpcc_data *) ((c)->custom_data))
#define	RPCC_FUNC(c)	((struct rpc_client_functions *) ((c)->extra))


int rpcc_default_execute (struct connection *c, int op, int len);
int rpc_client_check_ready (struct connection *c);
int rpcc_init_outbound (struct connection *c);
int rpcc_init_outbound_nohs (struct connection *c);
int rpcc_default_check_ready (struct connection *c);

int rpcc_flush_packet (struct connection *c);
int rpcc_flush_packet_later (struct connection *c);
int rpcc_default_check_perm (struct connection *c);
int rpcc_init_crypto (struct connection *c);
int rpcc_start_crypto (struct connection *c, char *nonce, int key_select);

int rpcc_parse_execute (struct connection *c);
int rpcc_connected (struct connection *c);
int rpcc_close_connection (struct connection *c, int who);
int rpcc_flush (struct connection *c);

/* END */
#endif
