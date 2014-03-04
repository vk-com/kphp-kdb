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
              2013 Anton Maydell
*/

#ifndef __SEARCH_PROFILE_H__
#define __SEARCH_PROFILE_H__

#define SEARCH_QUERY_MAX_SIZE 512
#define SEARCH_QUERY_EXPIRATION_TIME 86400

typedef struct {
  double cpu_time;
  int res;
  int expiration_time;
  char query[SEARCH_QUERY_MAX_SIZE];
} search_query_heap_en_t;

void search_query_start (search_query_heap_en_t *E);
void search_query_end (search_query_heap_en_t *E, int res, void *arg, void (*copy) (search_query_heap_en_t *, void *));
void search_query_remove_expired (void);
int search_query_worst (char *output, int olen);

#endif
