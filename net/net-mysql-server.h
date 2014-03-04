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

#ifndef	__NET_MYSQL_SERVER__
#define	__NET_MYSQL_SERVER__

#include "mysql.h"

struct mysql_server_functions {
  void *info;
  int server_charset;
  int (*execute)(struct connection *c, int op);		/* invoked from parse_execute() */
  int (*sql_password)(struct connection *c, const char *user, char buffer[20]);
  int (*sql_wakeup)(struct connection *c);
  int (*sql_alarm)(struct connection *c);
  int (*sql_flush_packet)(struct connection *c, int packet_len);		/* execute this to push query to server */
  int (*sql_ready_to_write)(struct connection *c);
  int (*sql_check_perm)(struct connection *c);	/* 1 = allow unencrypted, 2 = allow encrypted */
  int (*sql_init_crypto)(struct connection *c, char *init_buff, int init_len);  /* >0 = ok, bytes written; -1 = no crypto */
  int (*sql_start_crypto)(struct connection *c, char *auth_str, int auth_len);  /* 1 = ok; -1 = no crypto */
};

/* in conn->custom_data ; current size is 124 bytes! */
struct sqls_data {
  int auth_state;
  int auth_user;
  int query_state;
  int packet_state;
  int packet_len;
  int packet_seq, output_packet_seq;
  int client_flags;
  int max_packet_size;
  int table_offset;
  int table_len;
  int clen;
  char comm[16];
  char scramble[21];
  char resvd[3];
  int packet_padding;
  int block_size;
  int crypto_flags;
  int nonce_time;
  char nonce[16];
  int custom;
};

enum sqls_query_state {
  query_none,
  query_running,
  query_ok,
  query_failed,
  query_wait_target,
};


#define	SQLS_DATA(c)	((struct sqls_data *) ((c)->custom_data))
#define	SQLS_FUNC(c)	((struct mysql_server_functions *) ((c)->extra))

extern conn_type_t ct_mysql_server;
extern struct mysql_server_functions default_mysql_server;

int sqls_init_accepted (struct connection *c);
int sqls_parse_execute (struct connection *c);
int sqls_do_wakeup (struct connection *c);
int sqls_execute (struct connection *c, int op);
int sqls_password (struct connection *c, const char *user, char buffer[20]);
int sqls_builtin_execute (struct connection *c, int op);

int sqls_flush_packet (struct connection *c, int packet_len);
int sqls_default_check_perm (struct connection *c);
int sqls_init_crypto (struct connection *c, char *init_buff, int init_len);
int sqls_start_crypto (struct connection *c, char *key, int key_len);


/* useful */

int write_lcb (struct connection *c, unsigned long long l);
int send_ok_packet (struct connection *c, unsigned long long affected_rows,
                    unsigned long long insert_id, int server_status, 
                    int warning_count, const char *message, int msg_len,
                    int sequence_number);
int send_error_packet (struct connection *c, int error_no,
                       int sql_state, const char *message, int msg_len,
                       int sequence_number);



/* END */
#endif
