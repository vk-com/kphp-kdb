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

    Copyright 2011-2013 Vkontakte Ltd
              2011-2013 Arseny Smirnov
              2011-2013 Aliaksei Levin
*/

#include "rpc-proxy/rpc-proxy.h"
#include "net-rpc-targets.h"
#include "net-rpc-server.h"
#include "net-rpc-client.h"
#include "photo-tl.h"

#include <assert.h>


int photo_forward (void) {
  int op = tl_fetch_lookup_int ();
  if (op == TL_PHOTO_SET_VOLUME_SERVER) {
    default_query_diagonal_forward ();
    return 0;
  } else {
    return default_firstint_forward ();
  }
}

SCHEMA_ADD(photo) {
  if (C->methods.forward) {
    return -1;
  }
  C->methods.forward = photo_forward;
  return 0;
}
SCHEMA_REGISTER(photo,0)
