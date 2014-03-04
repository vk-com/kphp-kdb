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

#define DL_HASHTABLE_WAS

#if (!defined (DATA_T) || !defined (TNAME))
#  error DATA_T or TNAME is not declared
#endif


#ifndef IMPLEMENTATION
#  define IMPLEMENTATION OPEN
#else
#  if IMPLEMENTATION != OPEN && IMPLEMENTATION != CHAIN
#    error Wrong IMPLEMENTATION chosen
#  endif
#endif

#ifndef RESIZABLE
#  if IMPLEMENTATION == OPEN
#    define RESIZABLE ON
#  else
#    define RESIZABLE OFF
#  endif
#else
#  if RESIZABLE != ON && RESIZABLE != OFF
#    error Wrong RESIZABLE chosen
#  endif
#endif

#if !defined (C1) || !defined (C2)
#  define C1 5 / 3
#  define C2 3 / 2
#endif

#ifndef STRICT
#  define STRICT ON
#else
#  if STRICT != ON && STRICT != OFF
#    error Wrong STRICT chosen
#  endif
#endif

#ifndef STORE_HASH
#  define STORE_HASH ON
#else
#  if STORE_HASH != ON && STORE_HASH != OFF
#    error Wrong STORE_HASH chosen
#  endif
#endif

#ifndef MIN_SIZE
#  define MIN_SIZE 100
#endif

#if MIN_SIZE >= MIN_SIZE * C2 || MIN_SIZE < 0
#  error Wrong MIN_SIZE chosen
#endif

#ifndef WAIT_FREE
#  define WAIT_FREE OFF
#else
#  if WAIT_FREE != ON && WAIT_FREE != OFF
#    error Wrong WAIT_FREE chosen
#  endif
#endif

#if WAIT_FREE == ON
#  define HNAME ht_
#else
#  define HNAME ht
#endif

#if !defined (MAP)
#  define MAP OFF
#endif

#if MAP == ON
#  if !defined (RDATA_T)
#    error RDATA_T undefined
#  endif
#else
#  if MAP != OFF
#    error Wrong MAP chosen
#  endif
#endif

#define hash_entry DL_ADD_SUFF (TNAME, entry)
#define hash_entry_x DL_ADD_SUFF (TNAME, entry_x)
#define hash_table TNAME

#if WAIT_FREE == ON
#  define hash_table_ DL_CAT (TNAME, _)
#else
#  define hash_table_ TNAME
#endif

#define hash_entry_ptr DL_ADD_SUFF (TNAME, entry_ptr)

#if IMPLEMENTATION == CHAIN
#  define bucket_t hash_entry_ptr
#else
#  define bucket_t hash_entry
#endif


//TODO fix it!
#define KEY_T long long

#if MAP == ON
#  define RDATA rdata
#else
#  define RDATA data
#  ifdef RDATA_T
#    error RDATA_T defined but MAP if OFF
#  endif
#  define RDATA_T DATA_T
#endif

typedef struct hash_entry_x hash_entry;
typedef hash_entry* hash_entry_ptr;

#define callback_func_t DL_ADD_SUFF (hash_table, callback_function_t)
#if MAP == ON
typedef void (*callback_func_t) (DATA_T *x, RDATA_T *y);
#else
typedef void (*callback_func_t) (DATA_T *x);
#endif

#pragma pack(push, 4)

struct hash_entry_x {
  DATA_T data;

#if MAP == ON
  RDATA_T rdata;
#endif

#if STORE_HASH == ON
  KEY_T key;
#endif

#if IMPLEMENTATION == CHAIN
  hash_entry_ptr next;
#endif
};

#if IMPLEMENTATION == OPEN && STRICT == OFF && !defined (NOZERO)
#define ZERO 1
#else
#define ZERO 0
#endif


typedef struct {
  int size, used;
#if ZERO == 1
  int zero;
#endif

  bucket_t *e;
} hash_table_;

//#define size __size
//#define used __used
//#define zero __zero
//#define e __e

#if WAIT_FREE == ON
typedef struct {
  hash_table_ *h;
} hash_table;
#endif

#pragma pack(pop)

#if ZERO == 1
#  define NEW_KEY(data) ({KEY_T _key = DL_HASH(DATA_T)(data); if (_key == 0) _key = 1; _key;})
#else
#  define NEW_KEY(data) (DL_HASH(DATA_T)(data))
#endif


#if STORE_HASH == ON
#  define KEY(entry) ((entry).key)
#else
#  define KEY(entry) NEW_KEY((entry).data)
#endif

#define HEQ_A(entry, key) (KEY(entry) == (key))

#if STRICT == ON
#  define HEQ_B(entry, _data) && (DL_EQ(DATA_T)((entry).data, _data))
#else
#  define HEQ_B(entry, _data)
#endif

#define HEQ(entry, data, key) (HEQ_A(entry, key) HEQ_B(entry, data))

#define CHOOSE_BUCKET(key, size) ((unsigned int)(key) % (size))

#if IMPLEMENTATION == OPEN
#  if STORE_HASH == ON
#    define _EMPTY(entry) (!entry.key)
#    define NEW_EMPTY(key, data) (!(key))
#  else
#    define _EMPTY(entry) EMPTY(entry.data)
#    define NEW_EMPTY(key, data) EMPTY(data)//TODO remove EMPTY define and assert that EMPTY(0) == true
#  endif
#  define FND(ht, data, key) int i = CHOOSE_BUCKET(key, ht->size); \
                             while (!_EMPTY(ht->e[i]) && !HEQ(ht->e[i], data, key)) {                \
                               if (unlikely (++i == ht->size)) {                                     \
                                 i = 0;                                                              \
                               }                                                                     \
                             }
#else
#  define FND(ht, data, key) int i = CHOOSE_BUCKET(key, ht->size);             \
                                                                               \
                             hash_entry *curr = ht->e[i];                      \
                             while (curr != NULL) {                            \
                               if (HEQ((*curr), data, key)) {                  \
                                 break;                                        \
                               }                                               \
                               curr = curr->next;                              \
                             }
#endif
