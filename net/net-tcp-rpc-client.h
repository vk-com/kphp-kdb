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

#ifndef __NET_TCP_RPC_CLIENT_H__
#define __NET_TCP_RPC_CLIENT_H__

struct tcp_rpc_client_functions {
  void *info;
  int (*execute)(struct connection *c, int op, struct raw_message *raw);	/* invoked from parse_execute() */
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
extern conn_type_t ct_tcp_rpc_client;
int tcp_rpcc_parse_execute (struct connection *c);
int tcp_rpcc_compact_parse_execute (struct connection *c);
int tcp_rpcc_connected (struct connection *c);
int tcp_rpcc_connected_nohs (struct connection *c);
int tcp_rpcc_close_connection (struct connection *c, int who);
int tcp_rpcc_init_outbound (struct connection *c);
int tcp_rpc_client_check_ready (struct connection *c);
void tcp_rpcc_flush_crypto (struct connection *c);
int tcp_rpcc_flush (struct connection *c);
int tcp_rpcc_flush_packet (struct connection *c);
int tcp_rpcc_default_check_perm (struct connection *c);
int tcp_rpcc_init_crypto (struct connection *c);
int tcp_rpcc_start_crypto (struct connection *c, char *nonce, int key_select);

#define	TCP_RPCC_FUNC(c)	((struct tcp_rpc_client_functions *) ((c)->extra))

#endif

