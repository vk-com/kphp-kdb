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

    Copyright 2011 Vkontakte Ltd
              2011 Anton Maydell
*/

#define _FILE_OFFSET_BITS 64

#define NGX_HTTP_STORAGE_MODULE

#include <ngx_config.h>
#include <ngx_core.h>
#include <ngx_http.h>
#include "storage-content.h"
#include "kdb-storage-binlog.h"

static char* ngx_http_storage (ngx_conf_t *cf, ngx_command_t *cmd, void *conf);

static const char base64url_tbl[256] = {
  0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
  0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
  0,0,0,0,0,0,0,0,0,0,0,0,0,62,0,0,
  52,53,54,55,56,57,58,59,60,61,0,0,0,0,0,0,
  0,0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,
  15,16,17,18,19,20,21,22,23,24,25,0,0,0,0,63,
  0,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,
  41,42,43,44,45,46,47,48,49,50,51,0,0,0,0,0,
  0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
  0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
  0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
  0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
  0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
  0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
  0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
  0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0
};

static unsigned long long decode_secret (unsigned char *s) {
  unsigned char t[9];
  unsigned long long u;
  unsigned char *p = &t[0];
  int i;
  for (i = 0; i < 3; i++) {
    int o = base64url_tbl[*s++];
    o <<= 6;
    o |= base64url_tbl[*s++];
    o <<= 6;
    o |= base64url_tbl[*s++];
    o <<= 6;
    o |= base64url_tbl[*s++];
    p[2] = o & 255;
    o >>= 8;
    p[1] = o & 255;
    p[0] = o >> 8;
    p += 3;
  }
  memcpy (&u, t, 8);
  return u;
}

static ngx_command_t  ngx_http_storage_commands[] = {
  { ngx_string ("storage"),
    NGX_HTTP_LOC_CONF|NGX_CONF_NOARGS,
    ngx_http_storage,
    0,
    0,
    NULL
  },
  ngx_null_command
};

static ngx_http_module_t  ngx_http_storage_module_ctx = {
    NULL,                          /* preconfiguration */
    NULL,                          /* postconfiguration */

    NULL,                          /* create main configuration */
    NULL,                          /* init main configuration */

    NULL,                          /* create server configuration */
    NULL,                          /* merge server configuration */

    NULL,                          /* create location configuration */
    NULL                           /* merge location configuration */
};

ngx_module_t  ngx_http_storage_module = {
    NGX_MODULE_V1,
    &ngx_http_storage_module_ctx, /* module context */
    ngx_http_storage_commands,    /* module directives */
    NGX_HTTP_MODULE,              /* module type */
    NULL,                         /* init master */
    NULL,                         /* init module */
    NULL,                         /* init process */
    NULL,                         /* init thread */
    NULL,                         /* exit thread */
    NULL,                         /* exit process */
    NULL,                          /* exit master */
    NGX_MODULE_V1_PADDING
};

#define PATH_BUFFSIZE 1024
//#define DBG { ngx_log_error (NGX_LOG_ERR, r->connection->log, 0, "%d", __LINE__); }

static ngx_int_t ngx_http_storage_handler (ngx_http_request_t *r) {
  ngx_int_t     rc;
  ngx_buf_t    *b;
  ngx_chain_t   out;
  ngx_uint_t level;

  if (!(r->method & (NGX_HTTP_GET | NGX_HTTP_HEAD))) {
    return NGX_HTTP_NOT_ALLOWED;
  }

  rc = ngx_http_discard_request_body (r);
  if (rc != NGX_OK && rc != NGX_AGAIN) {
    return rc;
  }

  if (r->headers_in.if_modified_since) {
    return NGX_HTTP_NOT_MODIFIED;
  }

  char filename[PATH_BUFFSIZE];
  unsigned int i;
  for (i = 0; i < r->uri.len; i++) {
    if (r->uri.data[i] == ':') {
      break;
    }
  }

  if (i >= r->uri.len) {
    ngx_log_error (NGX_LOG_ERR, r->connection->log, 0, "secret not found");
    return NGX_HTTP_NOT_FOUND;
  }

  if (i >= PATH_BUFFSIZE) {
    ngx_log_error (NGX_LOG_ERR, r->connection->log, 0, "path too long");
    return NGX_HTTP_NOT_FOUND;
  }
  memcpy (filename, r->uri.data, i);
  filename[i] = 0;
  ngx_str_t path;
  path.len = i;
  path.data = (u_char *)  filename;

  long long offset;
  int content_type;
  char base64url_secret[12];
  int parsed_args = sscanf ((char*) &r->uri.data[i+1], "%llx:%11[0-9A-Za-z_-]:%x", &offset, base64url_secret, &content_type);
  if (parsed_args != 3) {
    ngx_log_error (NGX_LOG_ERR, r->connection->log, 0, "offset:secret:type not found (parsed_args = %d, %s)", parsed_args, (char *) &r->uri.data[i+1]);
    return NGX_HTTP_NOT_FOUND;
  }
  unsigned long long secret = decode_secret ((unsigned char *) base64url_secret);

  ngx_http_core_loc_conf_t *clcf = ngx_http_get_module_loc_conf (r, ngx_http_core_module);
  ngx_open_file_info_t of;
  ngx_memzero (&of, sizeof (ngx_open_file_info_t));

  of.directio = clcf->directio;
  of.valid = clcf->open_file_cache_valid;
  of.min_uses = clcf->open_file_cache_min_uses;
  of.errors = clcf->open_file_cache_errors;
  of.events = clcf->open_file_cache_events;


  if (ngx_open_cached_file (clcf->open_file_cache, &path, &of, r->pool) != NGX_OK) {
    switch (of.err) {
      case 0:
        return NGX_HTTP_INTERNAL_SERVER_ERROR;
      case NGX_ENOENT:
      case NGX_ENOTDIR:
      case NGX_ENAMETOOLONG:
        level = NGX_LOG_ERR;
        rc = NGX_HTTP_NOT_FOUND;
        break;
      case NGX_EACCES:
        level = NGX_LOG_ERR;
        rc = NGX_HTTP_FORBIDDEN;
        break;
      default:
        level = NGX_LOG_CRIT;
        rc = NGX_HTTP_INTERNAL_SERVER_ERROR;
        break;
    }

    if (rc != NGX_HTTP_NOT_FOUND || clcf->log_not_found) {
      ngx_log_error (level, r->connection->log, of.err, "%s \"%s\" failed", of.failed, path.data);
    }
    return rc;
  }

  if (!of.is_file) {
    if (ngx_close_file (of.fd) == NGX_FILE_ERROR) {
      ngx_log_error (NGX_LOG_ALERT, r->connection->log, ngx_errno, ngx_close_file_n " \"%s\" failed", path.data);
    }
    return NGX_DECLINED;
  }

  int fd = of.fd;
  /*
  if (fd < 0) {
    ngx_log_error (NGX_LOG_ERR, r->connection->log, 0, "couldn't open %s", filename);
    return NGX_HTTP_NOT_FOUND;
  }
  */
  //ngx_read_file (of.file, &E, sizeof (E), offset);

  struct lev_storage_file E;
  if (sizeof (E) != pread (fd, &E, sizeof (E), offset)) {
    ngx_log_error (NGX_LOG_ERR, r->connection->log, 0, "read fail");
    //close (fd);
    return NGX_HTTP_INTERNAL_SERVER_ERROR;
  }

  if (E.content_type >= ct_last || E.content_type < 0) {
    ngx_log_error (NGX_LOG_ERR, r->connection->log, 0, "illegal content type");
    //close (fd);
    return NGX_HTTP_INTERNAL_SERVER_ERROR;
  }

  if (E.type != LEV_STORAGE_FILE) {
    ngx_log_error (NGX_LOG_ERR, r->connection->log, 0, "illegal E.type");
    //close (fd);
    return NGX_HTTP_INTERNAL_SERVER_ERROR;
  }

  if (E.secret != secret) {
    ngx_log_error (NGX_LOG_ERR, r->connection->log, 0, "secret doesn't match");
    //close (fd);
    return NGX_HTTP_FORBIDDEN;
  }

  if (E.content_type != content_type) {
    ngx_log_error (NGX_LOG_ERR, r->connection->log, 0, "content type doesn't match");
    //close (fd);
    return NGX_HTTP_FORBIDDEN;
  }

  char *ct = ContentTypes[content_type];

  r->headers_out.content_type.len = strlen (ct);
  r->headers_out.content_type.data = (u_char *) ct;
  r->headers_out.status = NGX_HTTP_OK;
  r->headers_out.content_length_n = E.size;
  r->allow_ranges = 1;
  r->headers_out.last_modified_time = E.mtime;

  if (r->method == NGX_HTTP_HEAD) {
    rc = ngx_http_send_header (r);
    if (rc == NGX_ERROR || rc > NGX_OK || r->header_only) {
      return rc;
    }
  }

  b = ngx_pcalloc (r->pool, sizeof (ngx_buf_t));
  if (b == NULL) {
    ngx_log_error (NGX_LOG_ERR, r->connection->log, 0, "Failed to allocate response buffer.");
    //close (fd);
    return NGX_HTTP_INTERNAL_SERVER_ERROR;
  }

  b->file = ngx_pcalloc (r->pool, sizeof (ngx_file_t));
  if (b->file == NULL) {
    return NGX_HTTP_INTERNAL_SERVER_ERROR;
  }


  rc = ngx_http_send_header (r);
  if (rc == NGX_ERROR || rc > NGX_OK || r->header_only) {
    return rc;
  }

  out.buf = b;
  out.next = NULL;

  b->file_pos = offset + sizeof (E);
  b->file_last = b->file_pos + E.size;

  b->in_file = 1;
  b->last_buf = 1;
  b->file->fd = of.fd;
  b->file->name.len = path.len;
  b->file->name.data = ngx_pstrdup (r->pool, &path);
  b->file->log = r->connection->log;
  b->file->directio = of.is_directio;

  return ngx_http_output_filter (r, &out);
}

static char *ngx_http_storage (ngx_conf_t *cf, ngx_command_t *cmd, void *conf) {
  ngx_http_core_loc_conf_t  *clcf = ngx_http_conf_get_module_loc_conf (cf, ngx_http_core_module);
  clcf->handler = ngx_http_storage_handler;
  return NGX_CONF_OK;
}


