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

    Copyright 2011 Vkontakte Ltd
	      2011 Anton Maydell
*/

#ifndef __VK_NET_PARSE_H__
#define __VK_NET_PARSE_H__

#include "net-buffers.h"

struct nb_reader {
  nb_iterator_t it;
  unsigned char *p, *ptr_s, *ptr_e;
  int bytes;
  int bytes_read;
};

/* nb_reader acts as nb_iterator (it doens't move netbuffer pointers) */
/* bytes - how many bytes reader could read from netbuffer */
void nb_reader_set (struct nb_reader *I, struct netbuffer *In, const int bytes);

/* return -1 at the end of netbuffer */
inline int nb_reader_getc (struct nb_reader *I);

/* *x - contains parsed int32 */
/* *c - contains last not digit character read, (-1) means end of buffer */
/* returns 1 on success, 0 on failure */
int nb_reader_parse_int (struct nb_reader *I, int *x, int *c);

int np_news_parse_list (int *Res, const int max_size, const int arity, netbuffer_t *In, const int bytes);
int np_parse_list_str (int *Res, const int max_size, const int arity, const char *ptr, const int bytes);
#endif
