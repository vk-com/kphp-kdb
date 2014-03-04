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

    Copyright 2013 Vkontakte Ltd
              2013 Vitaliy Valtman
*/

#include "rpc-proxy/rpc-proxy.h"
#include "net-rpc-targets.h"
#include "net-rpc-server.h"
#include "net-rpc-client.h"
#include "news-tl.h"
#include "rpc-proxy/rpc-proxy-merge.h"
#include "rpc-proxy-merge-news.h"
#include "rpc-proxy-merge-news-r.h"

int G_NEWS_SCHEMA_NUM;

int ug_news_forward (void) {
  int op = tl_fetch_lookup_int ();
  if (op == TL_NEWS_GET_GROUPED_UPDATES) {
    merge_forward (&ugnews_gather_methods);
    return 0;
  } else if (op == TL_NEWS_GET_RAW_UPDATES) {
    merge_forward (&ugnews_raw_gather_methods);
    return 0;
  } else {
    return default_firstint_forward ();
  }
}

int g_news_forward (void) {
  return ug_news_forward ();
}

int c_news_forward (void) {
  int op = tl_fetch_lookup_int ();
  if (op == TL_CNEWS_GET_GROUPED_UPDATES) {
    merge_forward (&cnews_gather_methods);
    return 0;
  } else if (op == TL_CNEWS_GET_RAW_UPDATES) {
    merge_forward (&cnews_raw_gather_methods);
    return 0;
  } else if (op == TL_CNEWS_GET_GROUPED_USER_UPDATES) {
    merge_forward (&cnews_user_gather_methods);
    return 0;
  } else if (op == TL_CNEWS_GET_RAW_USER_UPDATES) {
    merge_forward (&cnews_raw_user_gather_methods);
    return 0;
  } else {
    return default_tuple_forward (3);
  }
}

int r_news_forward (void) {
  int op = tl_fetch_lookup_int ();
  if (op == TL_RNEWS_SET_RATE) {
    default_query_diagonal_forward ();
    return 0;
  } else if (op == TL_RNEWS_GET_RAW_UPDATES) {
    merge_forward (&rnews_raw_gather_methods);
    return 0;
  } else if (op == TL_RNEWS_GET_GROUPED_UPDATES) {
    merge_forward (&rnews_gather_methods);
    return 0;
  } else {
    return default_firstint_forward ();
  }
}

int n_news_forward (void) {
  int op = tl_fetch_lookup_int ();
  if (op == TL_NNEWS_DELETE_UPDATES || op == TL_NNEWS_DELETE_UPDATE || op == TL_NNEWS_UNDELETE_UPDATE) {
    default_query_diagonal_forward ();
    return 0;
  } else {
    return default_firstint_forward ();
  }
}

#define REG(x,X) \
  SCHEMA_ADD(x ## _news) { \
    if (C->methods.forward) { \
      return -1; \
    } \
    C->methods.forward = x ## _news_forward; \
    return 0; \
  } \
  SCHEMA_REGISTER(x ## _news,0)

#define REGE(x,X) \
  SCHEMA_ADD(x ## _news) { \
    if (C->methods.forward) { \
      return -1; \
    } \
    C->methods.forward = x ## _news_forward; \
    return 0; \
  } \
  SCHEMA_REGISTER_NUM(x ## _news,0,X ## _NEWS_SCHEMA_NUM)

REG(ug,UG)
REG(c,C)
REG(r,R)
REG(n,N)
REGE(g,G)
