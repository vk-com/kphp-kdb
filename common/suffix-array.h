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

#ifndef __SUFFIX_ARRAY_H__
#define __SUFFIX_ARRAY_H__

typedef struct {
  unsigned char *y;
  int *p;
  int *lcp;
  int n;
} suffix_array_t;

void suffix_array_init (suffix_array_t *A, unsigned char *y, int n);
void suffix_array_free (suffix_array_t *A);

/* suffix_array_search tries to maximize output parameter common_length
   A->y[r:r+common_length] == x[0:common_length] (python syntax)
   where r is returned value by suffix_array_search
*/
int suffix_array_search (suffix_array_t *A, unsigned char *x, int m, int *common_length);

/* dump all sorted suffixes to the stderr */
void suffix_array_dump (suffix_array_t *A);
void suffix_array_check (suffix_array_t *A);

#endif
