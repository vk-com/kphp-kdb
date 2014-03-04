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

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "search-value-buffer.h"

static void value_buffer_output_hex_long_text (struct value_buffer *B, long long t) {
  int o;
  B->w += o = sprintf (B->wptr, "%llx", t);
  B->wptr += o;
}

static void value_buffer_output_long_text (struct value_buffer *B, long long t) {
  int o;
  B->w += o = sprintf (B->wptr, "%lld", t);
  B->wptr += o;
}

static void value_buffer_output_int_raw (struct value_buffer *B, int t) {
  memcpy (B->wptr, &t, 4);
  B->w += 4;
  B->wptr += 4;
}

static void value_buffer_output_long_raw (struct value_buffer *B, long long t) {
  memcpy (B->wptr, &t, 8);
  B->w += 8;
  B->wptr += 8;
}

static void value_buffer_output_int_text (struct value_buffer *B, int t) {
  B->w += t = sprintf (B->wptr, "%d", t);
  B->wptr += t;
}

static void value_buffer_output_char_text (struct value_buffer *B, char ch ) {
  *(B->wptr) = ch;
  B->w++;
  B->wptr++;
}

#define	SHORT_ID(__x)	((int) ((__x) >> 32))

static void value_buffer_output_item_id_text (struct value_buffer *B, long long item_id) {
  int t = SHORT_ID (item_id);
  if (t) {
    value_buffer_output_int_text (B, (int) item_id);
    value_buffer_output_char_text (B, '_');
    value_buffer_output_int_text (B, (int) t);
  } else {
    value_buffer_output_int_text (B, (int) item_id);
  }
}

static void value_buffer_output_char_noop (struct value_buffer *B, char ch) {
}

int value_buffer_init (struct value_buffer *B, struct connection *c, const char *key, int len, int mode, int flush_margin) {
  B->n474 = 512 - flush_margin;
  if (!mode) {
    B->output_int = &value_buffer_output_int_text;
    B->output_long = &value_buffer_output_long_text;
    B->output_char = &value_buffer_output_char_text;
    B->output_item_id = &value_buffer_output_item_id_text;
    B->output_hash = &value_buffer_output_hex_long_text;
  } else {
    B->output_int = value_buffer_output_int_raw;
    B->output_long = value_buffer_output_long_raw;
    B->output_char = &value_buffer_output_char_noop;
    B->output_item_id = &value_buffer_output_long_raw;
    B->output_hash = &value_buffer_output_long_raw;
  }
  write_out (&c->Out, "VALUE ", 6);
  write_out (&c->Out, key, len);
  B->w = 0;
  B->wptr = get_write_ptr (&c->Out, 512);
  if (!B->wptr) return 0;
  B->c = c;
  B->s = B->wptr + B->n474;
  memcpy (B->wptr, " 0 .........\r\n", 14);
  B->size_ptr = B->wptr + 3;
  B->wptr += 14;
  return 1;
}

int value_buffer_flush (struct value_buffer *B) {
  if (B->wptr >= B->s) {
    advance_write_ptr (&B->c->Out, B->wptr - (B->s - B->n474));
    B->wptr = get_write_ptr (&B->c->Out, 512);
    if (!B->wptr) return 0;
    B->s = B->wptr + B->n474;
  }
  return 1;
}

int value_buffer_return (struct value_buffer *B) {
  B->size_ptr[sprintf (B->size_ptr, "% 9d", B->w)] = '\r';
  memcpy (B->wptr, "\r\n", 2);
  B->wptr += 2;
  advance_write_ptr (&B->c->Out, B->wptr - (B->s - B->n474));
  return 0;
}

