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

#ifndef __SEARCH_VALUE_BUFFER_H__
#define __SEARCH_VALUE_BUFFER_H__

#include "net-connections.h"
#include "net-buffers.h"

struct value_buffer {
  int n474;
  int w;
  char *size_ptr, *s, *wptr;
  struct connection *c;
  void (*output_int) (struct value_buffer *B, int t);
  void (*output_long) (struct value_buffer *B, long long t);
  void (*output_char) (struct value_buffer *B, char ch);
  void (*output_item_id) (struct value_buffer *B, long long item_id);
  void (*output_hash) (struct value_buffer *B, long long hash);
};

int value_buffer_init (struct value_buffer *B, struct connection *c, const char *key, int len, int mode, int flush_margin);
int value_buffer_flush (struct value_buffer *B);
int value_buffer_return (struct value_buffer *B);

#endif
