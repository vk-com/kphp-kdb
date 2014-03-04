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

#define _FILE_OFFSET_BITS 64

#include "dl-default.h"
#include "dl-utils.h"

#include <assert.h>
#include <string.h>

/*
 * Hash table.
 *
 * defines needed:
 *   TNAME - type name
 *   DATA_T - type of data. NB! When open addressation is chosen, empty entries will contain allocated variables of type DATA_T
 *      DL_HASH(DATA_T) will be used as hash function
 *   IMPLEMENTATION - can be OPEN(one array) or CHAIN(lists)
 *   RESIZABLE - ON or OFF. By default it is ON for OPEN and OFF for CHAIN
 *   C1 C2 - contants for resizing. Do not touch them if you don't know their meaning
 *   STRICT - ON or OFF. Check data for equality or not. If ON then DL_EQ(DATA_T) must be defined. OFF by default
 *   STORE_HASH - ON or OFF. If OFF hash will be calculated online (usefull if hash is the same as value)
 *   WAIT_FREE - ON or OFF. If ON then there will be 4096 hashtables. TODO: make 4096 not constant
 *   MAP - ON or OFF. If ON RDATA_T must be defined
 */

//TODO: (key * const) % 4097

#include "dl-hashtable.h"

void DL_ADD_SUFF (TNAME, init) (hash_table *HNAME) {
#if WAIT_FREE == ON
  ht_->h = dl_malloc (sizeof (hash_table_) * 4096);
  int i;
  for (i = 0; i < 4096; i++) {
    hash_table_ *ht = &ht_->h[i];
#endif

  ht->size = 0;
  ht->used = 0;

#if ZERO == 1
  ht->zero = 0;
#endif

  ht->e = NULL;

#if WAIT_FREE == ON
  }
#endif
}

#if MAP == ON
int DL_ADD_SUFF (TNAME, pairs) (hash_table *HNAME, DATA_T *a, RDATA_T *b, int mx) {
  int bi = 0, j;

#if WAIT_FREE == ON
  int i;
  for (i = 0; i < 4096; i++) {
    hash_table_ *ht = &ht_->h[i];
#endif

  #if IMPLEMENTATION == OPEN
    for (j = 0; j < ht->size; j++) {
      if (!_EMPTY(ht->e[j])) {
        if (bi < mx) {
          a[bi] = ht->e[j].data;
          b[bi] = ht->e[j].RDATA;
        }
        bi++;
      }
    }
    #if ZERO == 1
      if (ht->zero) {
        if (bi < mx) {
          a[bi] = ht->e[ht->size].data;
          b[bi] = ht->e[ht->size].RDATA;
        }
        bi++;
      }
    #endif
  #else
    for (j = 0; j < ht->size; j++) {
      hash_entry_ptr cur = ht->e[j];
      while (cur) {
        if (bi < mx) {
          a[bi] = cur->data;
          b[bi] = cur->RDATA;
        }
        bi++;
        cur = cur->next;
      }
    }
  #endif

#if WAIT_FREE == ON
  }
#endif

  return bi;
}
#endif


int DL_ADD_SUFF (TNAME, values) (hash_table *HNAME, RDATA_T *b, int mx)  {
  int bi = 0, j;

#if WAIT_FREE == ON
  int i;
  for (i = 0; i < 4096; i++) {
    hash_table_ *ht = &ht_->h[i];
#endif

  #if IMPLEMENTATION == OPEN
    for (j = 0; j < ht->size; j++) {
      if (!_EMPTY(ht->e[j])) {
        if (bi < mx) {
          b[bi] = ht->e[j].RDATA;
        }
        bi++;
      }
    }
    #if ZERO == 1
      if (ht->zero) {
        if (bi < mx) {
          b[bi] = ht->e[ht->size].RDATA;
        }
        bi++;
      }
    #endif
  #else
    for (j = 0; j < ht->size; j++) {
      hash_entry_ptr cur = ht->e[j];
      while (cur) {
        if (bi < mx) {
          b[bi] = cur->RDATA;
        }
        bi++;
        cur = cur->next;
      }
    }
  #endif

#if WAIT_FREE == ON
  }
#endif

  return bi;
}

void DL_ADD_SUFF (TNAME, set_size) (hash_table_ *ht, int size) {
  if (size == ht->size) {
    return;
  }
  assert (size);
  size |= 1;
  if (size % 5 == 0) {
    size += 2;
  }
//  fprintf (stderr, "set_size : %d\n", size);

  bucket_t *e = dl_malloc0 ((size + ZERO) * sizeof (bucket_t)), *old_e = ht->e;
  assert (e != NULL);

  ht->e = e;

  int old_size = ht->size;
  ht->size = size;

  int j;
#if IMPLEMENTATION == OPEN
  for (j = 0; j < old_size; j++) {
    if (!_EMPTY(old_e[j])) {
      int i = CHOOSE_BUCKET(KEY(old_e[j]), size);
      while (!_EMPTY(e[i])) {
        if (unlikely (++i == size)) {
          i = 0;
        }
      }
      e[i] = old_e[j];
    }
  }
#if ZERO == 1
  if (ht->zero) {
    e[size] = old_e[old_size];
  }
#endif
#else
  for (j = 0; j < old_size; j++) {
    hash_entry_ptr cur = old_e[j], next;
    while (cur) {
      int i = CHOOSE_BUCKET(KEY(*cur), size);
      next = cur->next;

      cur->next = e[i];
      e[i] = cur;

      cur = next;
    }
  }
#endif
  if (old_size) {
    dl_free (old_e, (old_size + ZERO) * sizeof (bucket_t));
  }

  //fprintf (stderr, "new_size = %d\n", ht->size);
}

inline RDATA_T* DL_ADD_SUFF (TNAME, add) (hash_table *HNAME, DATA_T data) {
  KEY_T key = NEW_KEY(data);

#if WAIT_FREE == ON
  hash_table_ *ht = &ht_->h[key & 4095];
#endif

#if RESIZABLE == ON
//  fprintf (stderr, "used = %d, size = %d\n", ht->used, ht->size);
  if ((ht->used + 1) * C1 >= ht->size) {
    int new_size = ht->size * C2;
    if (new_size < MIN_SIZE) {
      new_size = MIN_SIZE;
    }
    DL_ADD_SUFF (TNAME, set_size) (ht, new_size);
  }
#endif

  if (NEW_EMPTY(key, data)) {
#if ZERO == 1
    int i = ht->size;
    if (!ht->zero) {
#     if STORE_HASH == ON
        ht->e[i].key = key;
#     endif

#     if MAP == ON
        memset (&ht->e[i].RDATA, 0, sizeof (ht->e[i].RDATA));
#     endif

      ht->e[i].data = data;
      ht->zero = 1;
    }
    return &ht->e[i].RDATA;
#else
    assert (0);
#endif
  }

  FND (ht, data, key)

#if IMPLEMENTATION == OPEN
  if (_EMPTY(ht->e[i])) {
#   if STORE_HASH == ON
      ht->e[i].key = key;
#   endif

#   if MAP == ON
      memset (&ht->e[i].RDATA, 0, sizeof (ht->e[i].RDATA));
#   endif

    ht->e[i].data = data;
    ht->used++;
  }
  return &ht->e[i].RDATA;
#else
  if (curr == NULL) {
    ht->used++;

    hash_entry *st = dl_malloc0 (sizeof (hash_entry));
    st->data = data;

#  if STORE_HASH == ON
     st->key = key;
#  endif

    st->next = ht->e[i];
    curr = ht->e[i] = st;
  }
  return &curr->RDATA;
#endif
}

inline RDATA_T* DL_ADD_SUFF (TNAME, get) (hash_table *HNAME, DATA_T data) {
  KEY_T key = NEW_KEY(data);

#if WAIT_FREE == ON
  hash_table_ *ht = &ht_->h[key & 4095];
#endif

  if (NEW_EMPTY(key, data)) {
#if ZERO == 1
    if (ht->zero) {
      return &ht->e[ht->size].RDATA;
    }
#endif
    return NULL;
  }

  if (ht->used == 0) {
    return NULL;
  }

  FND (ht, data, key)

#if IMPLEMENTATION == OPEN
  if (_EMPTY(ht->e[i])) {
    return NULL;
  }
  return &ht->e[i].RDATA;
#else
  if (curr == NULL) {
    return NULL;
  }
  return &curr->RDATA;
#endif
}

inline void DL_ADD_SUFF (TNAME, del) (hash_table *HNAME, DATA_T data) {
  KEY_T key = NEW_KEY(data);

#if WAIT_FREE == ON
  hash_table_ *ht = &ht_->h[key & 4095];
#endif

  if (NEW_EMPTY(key, data)) {
#if ZERO == 1
    memset (&ht->e[ht->size], 0, sizeof (ht->e[ht->size]));
    ht->zero = 0;
#endif
    return;
  }

  if (ht->used == 0) {
    return;
  }
#if IMPLEMENTATION == CHAIN
  int i = CHOOSE_BUCKET(key, ht->size);

  hash_entry **curr = &ht->e[i];
  while (*curr != NULL) {
    if (HEQ((**curr), data, key)) {
      break;
    }
    curr = &(*curr)->next;
  }

  if (*curr != NULL) {
    hash_entry *tmp = *curr;

    *curr = (*curr)->next;

    dl_free (tmp, sizeof (hash_entry));
    ht->used--;
  }
#else
  FND (ht, data, key)

  int size = ht->size;
#define FIXD(a) ((a) >= size ? (a) - size : (a))
#define FIXU(a, m) ((a) <= (m) ? (a) + size : (a))
  if (!_EMPTY(ht->e[i])) {
    ht->used--;

    memset (&ht->e[i], 0, sizeof (ht->e[i]));
    int j, rj, ri = i;
    for (j = i + 1; 1; j++) {
      rj = FIXD(j);
      if (_EMPTY(ht->e[rj])) {
        break;
      }

      int bucket = CHOOSE_BUCKET(KEY(ht->e[rj]), size);
      int wnt = FIXU(bucket, i);

      if (wnt > j || wnt <= i) {
        hash_entry t = ht->e[ri];
        ht->e[ri] = ht->e[rj];
        ht->e[rj] = t;
        ri = rj;
        i = j;
      }
    }
  }
#undef FIXU
#undef FIXD
#endif
}

void DL_ADD_SUFF (TNAME, free) (hash_table *HNAME) {
#if WAIT_FREE == ON
  int i;
  for (i = 0; i < 4096; i++) {
    hash_table_ *ht = &ht_->h[i];
#endif


#if IMPLEMENTATION == CHAIN
  int j;
  for (j = 0; j < ht->size; j++) {
    hash_entry *curr = ht->e[j];
    while (curr != NULL) {
      hash_entry* tmp = curr;
      curr = curr->next;
      dl_free (tmp, sizeof (hash_entry));
    }
  }
#endif

  if (ht->e != NULL) {
    dbg ("FREE A {\n");
    dl_free (ht->e, (ht->size + ZERO) * sizeof (bucket_t));
    dbg ("}\n");
    ht->used = 0;
    ht->size = 0;
    ht->e = NULL;
    #if ZERO == 1
      ht->zero = 0;
    #endif
  }

#if WAIT_FREE == ON
  }
  dbg ("FREE B {\n");
  dl_free (ht_-> h, sizeof (hash_table_) * 4096);
  dbg ("}");
#endif
}


#if IMPLEMENTATION == OPEN && WAIT_FREE == OFF
int DL_ADD_SUFF (TNAME, get_encoded_size) (hash_table *ht) {
  return (ht->size + ZERO) * sizeof (bucket_t) + sizeof (int) * (2 + ZERO);
}

int DL_ADD_SUFF (TNAME, encode) (hash_table *ht, char *buff, int max_size) {
  assert ((ht->size + ZERO) * sizeof (bucket_t) + sizeof (int) * (2 + ZERO) <= (size_t)max_size);
  char *s = buff;

  WRITE_INT (s, ht->size);
  WRITE_INT (s, ht->used);

#if ZERO == 1
  WRITE_INT (s, ht->zero);
#endif

  memcpy (s, ht->e, (ht->size + ZERO) * sizeof (bucket_t));
  s += (ht->size + ZERO) * sizeof (bucket_t);

  return (int)(s - buff);
}

void DL_ADD_SUFF (TNAME, decode) (hash_table *ht, char *buff, int size) {
  READ_INT (buff, ht->size);
  READ_INT (buff, ht->used);

#if ZERO == 1
  READ_INT (buff, ht->zero);
#endif

  int left = sizeof (bucket_t) * (ht->size + ZERO);
  assert (left + sizeof (int) * (2 + ZERO) == (size_t)size);

  ht->e = dl_malloc (left);
  memcpy (ht->e, buff, left);
}
#endif

long DL_ADD_SUFF (TNAME, get_memory_used) (hash_table *HNAME) {
  long long res = 0;

#if WAIT_FREE == ON
  res += sizeof (hash_table_) * 4096;
  int i;
  for (i = 0; i < 4096; i++) {
    hash_table_ *ht = &ht_->h[i];
#endif

  res += /*sizeof (*ht) +*/ (ht->size + ZERO * !!ht->size) * sizeof (ht->e[0]);
#if IMPLEMENTATION == CHAIN
  res += ht->used * sizeof (hash_entry);
#endif


#if WAIT_FREE == ON
  }
#endif

  return res;
}

int DL_ADD_SUFF (TNAME, used) (hash_table *HNAME) {
  long long res = 0;

#if WAIT_FREE == ON
  int i;
  for (i = 0; i < 4096; i++) {
    hash_table_ *ht = &ht_->h[i];
#endif

    res += ht->used;
#if ZERO == 1
    res += ht->zero;
#endif

#if WAIT_FREE == ON
  }
#endif

  return res;
}

void DL_ADD_SUFF (TNAME, foreach) (hash_table *HNAME, callback_func_t f) {
  int j;

#if WAIT_FREE == ON
  int i;
  for (i = 0; i < 4096; i++) {
    hash_table_ *ht = &ht_->h[i];
#endif

  #if IMPLEMENTATION == OPEN
    for (j = 0; j < ht->size; j++) {
      if (!_EMPTY(ht->e[j])) {
#if MAP == ON
        f (&ht->e[j].data, &ht->e[j].RDATA);
#else
        f (&ht->e[j].RDATA);
#endif
      }
    }
    #if ZERO == 1
      if (ht->zero) {
#if MAP == ON
        f (&ht->e[ht->size].data, &ht->e[ht->size].RDATA);
#else
        f (&ht->e[ht->size].RDATA);
#endif
      }
    #endif
  #else
    for (j = 0; j < ht->size; j++) {
      hash_entry_ptr cur = ht->e[j];
      while (cur) {
#if MAP == ON
        f (&cur->data, &cur->RDATA);
#else
        f (&cur->RDATA);
#endif
        cur = cur->next;
      }
    }
  #endif

#if WAIT_FREE == ON
  }
#endif

}

void DL_ADD_SUFF (TNAME, pack) (hash_table *HNAME) {
#if RESIZABLE != ON || 1
  assert ("pack not supported yet" && 0);
#endif

#if WAIT_FREE == ON
  int i;
  for (i = 0; i < 4096; i++) {
    hash_table_ *ht = &ht_->h[i];
#endif

   int new_size = ht->used * C2;
   if (new_size < MIN_SIZE) {
     new_size = MIN_SIZE;
   }
   DL_ADD_SUFF (TNAME, set_size) (ht, new_size);

#if WAIT_FREE == ON
  }
#endif
}

