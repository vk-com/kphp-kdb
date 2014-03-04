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
              2012-2013 Arseny Smirnov
              2012-2013 Aliaksei Levin
*/

#pragma once


//C interface
#ifdef __cplusplus
#include <cstring>
extern "C" {
#else
#include <string.h>
#endif


/** http_query_data **/
typedef struct {
  char *uri, *get, *headers, *post, *request_method;
  int uri_len, get_len, headers_len, post_len, request_method_len;
  int keep_alive;
  unsigned int ip;
  unsigned int port;
} http_query_data;

http_query_data *http_query_data_create (const char *qUri, int qUriLen, const char *qGet, int qGetLen, const char *qHeaders,
  int qHeadersLen, const char *qPost, int qPostLen, const char *request_method, int keep_alive, unsigned int ip, unsigned int port);
void http_query_data_free (http_query_data *d);

/** rpc_query_data **/
typedef struct {
  int *data, len;

  long long req_id;

  /** PID **/
  unsigned ip;
  short port;
  short pid;
  int utime;
} rpc_query_data;

rpc_query_data *rpc_query_data_create (int *data, int len, long long req_id, unsigned int ip, short port, short pid, int utime);
void rpc_query_data_free (rpc_query_data *d);

/** php_query_data **/
typedef struct {
  http_query_data *http_data;
  rpc_query_data *rpc_data;
} php_query_data;

php_query_data *php_query_data_create (http_query_data *http_data, rpc_query_data *rpc_data);
void php_query_data_free (php_query_data *d);


/** script_t **/
typedef struct {
  void (*run) (php_query_data *, void *mem, size_t mem_size);
  void (*clear) (void);
} script_t;

script_t *get_script (const char *name);
void set_script (const char *name, void (*run)(php_query_data *, void *, size_t), void (*clear) (void));

/** script result **/

typedef struct {
  int return_code, exit_code;
  const char *headers, *body;
  int headers_len, body_len;
} script_result;

#ifdef __cplusplus
}
#endif
