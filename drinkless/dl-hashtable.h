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

    Copyright 2010-2013 Vkontakte Ltd
              2010-2013 Arseny Smirnov
              2010-2013 Aliaksei Levin
*/

#include "dl-hashtable-config.h"

void DL_ADD_SUFF (TNAME, init) (hash_table *table);


#if MAP == ON
int  DL_ADD_SUFF (TNAME, pairs) (hash_table *HNAME, DATA_T *a, RDATA_T *b, int mx);
#endif

int  DL_ADD_SUFF (TNAME, values) (hash_table *HNAME, RDATA_T *b, int mx);

void DL_ADD_SUFF (TNAME, set_size) (hash_table_ *ht, int size);

inline RDATA_T* DL_ADD_SUFF (TNAME, add) (hash_table *ht, DATA_T data);

inline RDATA_T* DL_ADD_SUFF (TNAME, get) (hash_table *ht, DATA_T data);

inline void DL_ADD_SUFF (TNAME, del) (hash_table *ht, DATA_T data);

#if IMPLEMENTATION == OPEN && WAIT_FREE == OFF
  int DL_ADD_SUFF (TNAME, get_encoded_size) (hash_table *ht);
  int DL_ADD_SUFF (TNAME, encode) (hash_table *ht, char *buff, int max_size);
  void DL_ADD_SUFF (TNAME, decode) (hash_table *ht, char *buff, int size);
#endif

void DL_ADD_SUFF (TNAME, free) (hash_table *ht);

long DL_ADD_SUFF (TNAME, get_memory_used) (hash_table *ht);
int  DL_ADD_SUFF (TNAME, used) (hash_table *ht);

void DL_ADD_SUFF (TNAME, foreach) (hash_table *ht, callback_func_t f);
void DL_ADD_SUFF (TNAME, pack) (hash_table *ht);
