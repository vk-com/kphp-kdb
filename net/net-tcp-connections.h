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

    Copyright 2009-2013 Vkontakte Ltd
              2008-2013 Nikolai Durov
              2008-2013 Andrei Lopatin
                   2013 Vitaliy Valtman
*/

#ifndef __NET_TCP_CONNECTIONS_H__
#define __NET_TCP_CONNECTIONS_H__

#include "net-connections.h"
int tcp_server_writer (struct connection *c);
int tcp_free_connection_buffers (struct connection *c);
int tcp_server_reader (struct connection *c);
int tcp_aes_crypto_decrypt_input (struct connection *c);
int tcp_aes_crypto_encrypt_output (struct connection *c);
int tcp_aes_crypto_needed_output_bytes (struct connection *c);
#endif
