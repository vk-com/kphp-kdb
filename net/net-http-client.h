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

    Copyright 2012 Vkontakte Ltd
              2012 Nikolai Durov
              2012 Andrei Lopatin
*/

#ifndef	__NET_HTTP_CLIENT__
#define	__NET_HTTP_CLIENT__

#include "net-connections.h"
#define	MAX_HTTP_HEADER_SIZE	16384

struct http_client_functions {
  void *info;
  int (*execute)(struct connection *c, int op);		/* invoked from parse_execute() */
  int (*htc_wakeup)(struct connection *c);
  int (*htc_alarm)(struct connection *c);
  int (*htc_close)(struct connection *c, int who);
  int (*htc_becomes_ready)(struct connection *c);
  int (*htc_check_ready)(struct connection *c);
};

#define	HTTP_V09	9
#define	HTTP_V10	0x100
#define	HTTP_V11	0x101

/* in conn->custom_data */
struct htc_data {
  int response_code;
  int response_flags;
  int response_words;
  int header_size;
  int first_line_size;
  int data_size;
  int location_offset;
  int location_size;
  int http_ver;
  int wlen;
  char word[16];
  int extra_int;
  int extra_int2;
  int extra_int3;
  int extra_int4;
  void *extra;
};

/* for htc_data.response_type */
enum htc_response_type {
  htrt_none = 0,
  htrt_error = -1,
  htrt_ok = 200,
  htrt_forbidden = 403,
  htrt_not_found = 404,
};

#define	RF_ERROR	1
#define RF_LOCATION	2
#define RF_DATASIZE	4
#define	RF_CONNECTION	8
#define	RF_KEEPALIVE	0x100

#define	HTC_DATA(c)	((struct htc_data *) ((c)->custom_data))
#define	HTC_FUNC(c)	((struct http_client_functions *) ((c)->extra))

extern conn_type_t ct_http_client;
extern struct http_client_functions default_http_client;

int htc_execute (struct connection *c, int op);
int htc_do_wakeup (struct connection *c);
int htc_parse_execute (struct connection *c);
int htc_alarm (struct connection *c);
int htc_init_accepted (struct connection *c);
int htc_close_connection (struct connection *c, int who);

extern int outbound_http_connections;
extern long long outbound_http_queries, http_bad_response_headers;


/* useful functions */
int get_http_header (const char *qHeaders, const int qHeadersLen, char *buffer, int b_len, const char *arg_name);

#define	HTTP_DATE_LEN	29
void gen_http_date (char date_buffer[29], int time);
int gen_http_time (char *date_buffer, int *time);
char *cur_http_date (void);
int write_basic_http_header (struct connection *c, int code, int date, int len, const char *add_header, const char *content_type);
int write_http_error (struct connection *c, int code);

/* END */
#endif
