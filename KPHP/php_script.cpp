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

#define _FILE_OFFSET_BITS 64
#include "php_script.h"

#include <map>
#include <string>
#include <cassert>

#include "dl-utils-lite.h"

std::map <std::string, script_t *> scripts;

script_t *get_script (const char *name) {
  std::map <std::string, script_t *>::iterator i = scripts.find (name);
  if (i != scripts.end()) {
    return i->second;
  }
  return NULL;
}

void set_script (const char *name, void (*run)(php_query_data *, void *mem, size_t mem_size), void (*clear) (void)) {
  static int cnt = 0;
  script_t *script = new script_t;
  script->run = run;
  script->clear = clear;
  assert (scripts.insert (std::pair <std::string, script_t *> (name, script)).second);
  assert (scripts.insert (std::pair <std::string, script_t *> (std::string ("#") + dl_int_to_str (cnt++), script)).second);
}

http_query_data *http_query_data_create (const char *qUri, int qUriLen, const char *qGet, int qGetLen, const char *qHeaders,
  int qHeadersLen, const char *qPost, int qPostLen, const char *request_method, int keep_alive, unsigned int ip, unsigned int port) {
  http_query_data *d = (http_query_data *)dl_malloc (sizeof (http_query_data));

  //TODO remove memdup completely. We can just copy pointers
  d->uri = (char *)dl_memdup (qUri, qUriLen);
  d->get = (char *)dl_memdup (qGet, qGetLen);
  d->headers = (char *)dl_memdup (qHeaders, qHeadersLen);
  if (qPost != NULL) {
    d->post = (char *)dl_memdup (qPost, qPostLen);
  } else {
    d->post = NULL;
  }

  d->uri_len = qUriLen;
  d->get_len = qGetLen;
  d->headers_len = qHeadersLen;
  d->post_len = qPostLen;

  d->request_method = (char *)dl_memdup (request_method, strlen (request_method));
  d->request_method_len = (int)strlen (request_method);

  d->keep_alive = keep_alive;

  d->ip = ip;
  d->port = port;

  return d;
}

void http_query_data_free (http_query_data *d) {
  if (d == NULL) {
    return;
  }

  dl_free (d->uri, d->uri_len);
  dl_free (d->get, d->get_len);
  dl_free (d->headers, d->headers_len);
  dl_free (d->post, d->post_len);

  dl_free (d, sizeof (http_query_data));
}

rpc_query_data *rpc_query_data_create (int *data, int len, long long req_id, unsigned int ip, short port, short pid, int utime) {
  rpc_query_data *d = (rpc_query_data *)dl_malloc (sizeof (rpc_query_data));

  d->data = (int *)dl_memdup (data, sizeof (int) * len);
  d->len = len;

  d->req_id = req_id;

  d->ip = ip;
  d->port = port;
  d->pid = pid;
  d->utime = utime;

  return d;
}

void rpc_query_data_free (rpc_query_data *d) {
  if (d == NULL) {
    return;
  }

  dl_free (d->data, d->len * sizeof (int));
  dl_free (d, sizeof (rpc_query_data));
}

php_query_data *php_query_data_create (http_query_data *http_data, rpc_query_data *rpc_data) {
  php_query_data *d = (php_query_data *)dl_malloc (sizeof (php_query_data));

  d->http_data = http_data;
  d->rpc_data = rpc_data;

  return d;
}

void php_query_data_free (php_query_data *d) {
  http_query_data_free (d->http_data);
  rpc_query_data_free (d->rpc_data);

  dl_free (d, sizeof (php_query_data));
}
