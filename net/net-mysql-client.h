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

    Copyright 2009-2011 Vkontakte Ltd
              2009-2011 Nikolai Durov
              2009-2011 Andrei Lopatin
*/

#ifndef	__NET_MYSQL_CLIENT__
#define	__NET_MYSQL_CLIENT__

#include "mysql.h"

struct mysql_client_functions {
  void *info;
  int server_charset;
  int (*execute)(struct connection *c, int op);		/* invoked from parse_execute() */
  int (*sql_authorized) (struct connection *c);
  int (*sql_becomes_ready) (struct connection *c);
  int (*sql_wakeup)(struct connection *c);
  int (*check_ready)(struct connection *c);		/* invoked from mc_client_check_ready() */
  int (*sql_flush_packet)(struct connection *c, int packet_len);		/* execute this to push query to server */
  int (*sql_ready_to_write)(struct connection *c);
  int (*sql_check_perm)(struct connection *c);	/* 1 = allow unencrypted, 2 = allow encrypted */
  int (*sql_init_crypto)(struct connection *c, char *init_buff, int init_len);  /* >0 = ok, bytes written; -1 = no crypto */
};


enum sql_response_state {
  resp_first,			/* waiting for the first packet */
  resp_reading_fields,
  resp_reading_rows,
  resp_done
};

/* in conn->custom_data */
struct sqlc_data {
  int auth_state;
  int auth_user;
  int packet_state;
  int packet_len;
  int packet_seq;
  int response_state;
  int client_flags;
  int max_packet_size;
  int table_offset;
  int table_len;
  int server_capabilities;
  int server_language;
  int clen;
  int packet_padding;
  int block_size;
  int crypto_flags;
  char comm[16];
  char version[8];
  int extra_flags;
};

#define	SQLC_DATA(c)	((struct sqlc_data *) ((c)->custom_data))
#define	SQLC_FUNC(c)	((struct mysql_client_functions *) ((c)->extra))

extern conn_type_t ct_mysql_client;
extern struct mysql_client_functions default_mysql_client;

int sqlc_parse_execute (struct connection *c);
int sqlc_do_wakeup (struct connection *c);
int sqlc_execute (struct connection *c, int op);
int sqlc_init_outbound (struct connection *c);
int sqlc_password (struct connection *c, const char *user, char buffer[20]);

int sqlc_flush_packet (struct connection *c, int packet_len);
int sqlc_default_check_perm (struct connection *c);
int sqlc_init_crypto (struct connection *c, char *init_buff, int init_len);

extern char *sql_username;
extern char *sql_password;
extern char *sql_database;


/* END */
#endif
