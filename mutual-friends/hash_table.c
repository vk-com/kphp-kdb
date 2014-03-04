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

    Copyright 2010-2013	Vkontakte Ltd
              2010-2013 Arseny Smirnov
              2010-2013 Aliaksei Levin
*/
#define	_FILE_OFFSET_BITS	64

#include "hash_table.h"

inline int poly_h (void *h, int size) {
  unsigned long long res = 0;
  int i;
  for (i = 0; i < 4; i++) {
    res *= 239017;
    res ^= ((long)h >> ((sizeof (long) / 4) * i)) & (0xffff);
    res *= 999987;
    res ^= (size >> (8 * i)) & (0xffff);
  }

  return res >> 16;
}

inline void htbl_check (hash_table *table) {
  int need = poly_h (table->h, table->size);
  if (need != table->hash) {
    fprintf (stderr, "Wrong hashtable: size = %d, h = %p.", table->size, table->h);
    fflush (stderr);
  }
  assert (need == table->hash);
}

hash_entry_ptr htbl_free_entries = NULL;
int htbl_allocated_cnt = 0;

size_t htbl_get_memory (void) {
  return htbl_allocated_cnt * sizeof (hash_entry);
}

void htbl_init (hash_table *table) {
  table->size = 0;
  table->h = NULL;
  table->hash = poly_h (NULL, 0);
}

void htbl_set_size (hash_table *table, int size) {
  assert (size > 0);
  assert (table->h == NULL);

  table->h = qmalloc0 (sizeof (hash_entry *) * size);
  assert (table->h != NULL);

  table->size = size;
  table->hash = poly_h (table->h, table->size);
}

void htbl_init_mem (int n) {
  assert (htbl_free_entries == NULL);
  assert (n > 0);

  htbl_free_entries = qmalloc (sizeof (hash_entry) * n);
  assert (htbl_free_entries != NULL);

  htbl_allocated_cnt += n;

  int i;
  for (i = 0; i + 1 < n; i++) {
    htbl_free_entries[i].next_entry = &htbl_free_entries[i + 1];
  }
  htbl_free_entries[n - 1].next_entry = NULL;
}

hash_entry_ptr htbl_get_entry (void) {
  if (htbl_free_entries == NULL) {
    if (1 <= htbl_allocated_cnt && htbl_allocated_cnt < 10000) {
      htbl_init_mem (htbl_allocated_cnt);
    } else {
      htbl_init_mem (10000);
    }
  }

  assert (htbl_free_entries != NULL);

  hash_entry *res = htbl_free_entries;
  htbl_free_entries = htbl_free_entries->next_entry;
  res->next_entry = NULL;

  return res;
}

void htbl_free (hash_table *table) {
  htbl_check (table);
  if (table == NULL) {
    return;
  }

  int n = table->size, i;
  hash_entry_ptr *h = table->h;
  for (i = 0; i < n; i++) {
    hash_entry_ptr curr = h[i];
    if (curr != NULL) {
      while (curr->next_entry != NULL) {
        curr->data = 0;
        curr = curr->next_entry;
      }
      curr->data = 0;
      curr->next_entry = htbl_free_entries;
      htbl_free_entries = h[i];
      h[i] = NULL;
    }
  }

  qfree (table->h, sizeof (hash_entry_ptr) * table->size);
  table->h = NULL;
  table->size = 0;
  table->hash = poly_h (table->h, table->size);
}


int htbl_del (hash_table *table, int key) {
  int i = (int)key % table->size, res = 0;
  if (i < 0) {
    i += table->size;
  }

  hash_entry **curr = &table->h[i];
  while ((*curr) != NULL) {
    if ((*curr)->key == key) {
      hash_entry *t = *curr;
      *curr = (*curr)->next_entry;
      res = t->data;
      t->data = 0;
      t->next_entry = htbl_free_entries;
      htbl_free_entries = t;

      return res;
    }
    curr = &(*curr)->next_entry;
  }

  return res;
}

int *htbl_find (hash_table *table, int key) {
  htbl_check (table);

  int i = (int)key % table->size;
  if (i < 0) {
    i += table->size;
  }

  hash_entry *curr = table->h[i];
  while (curr != NULL) {
    if (curr->key == key) {
      return &(curr->data);
    }
    curr = curr->next_entry;
  }

  return NULL;
}

int *htbl_find_or_create (hash_table *table, int key) {
  htbl_check (table);

  int i = (int)key % table->size;
  if (i<0) {
    i += table->size;
  }

  hash_entry *curr = table->h[i];
  while (curr != NULL) {
    if (curr->key == key) {
      return &(curr->data);
    }
    curr = curr->next_entry;
  }

  curr = htbl_get_entry();
  curr->key = key;
  curr->data = 0;
  curr->next_entry = table->h[i];
  table->h[i] = curr;

  return &(curr->data);
}


inline void ltbl_check (lookup_table *table) {
  int need = poly_h (table->rev, table->size);
  if (need != table->hash) {
    fprintf (stderr, "Wrong hashtable: size = %d, h = %p.", table->size, table->rev);
    fflush (stderr);
  }
  assert (need == table->hash);

  htbl_check (&table->to);
}

void ltbl_init (lookup_table *table) {
  htbl_init (&(table->to));
  table->rev = NULL;
  table->size = 0;
  table->currId = 1;
  table->hash = poly_h (table->rev, table->size);
}

void ltbl_set_size (lookup_table *table, int size) {
  htbl_set_size (&(table->to), size);
  table->rev = qmalloc0 (sizeof (int) * size);
  table->size = size;
  table->hash = poly_h (table->rev, table->size);
}

void ltbl_free (lookup_table *table) {
  ltbl_check (table);
  htbl_free (&(table->to));
  qfree (table->rev, sizeof (int) * table->size);

  table->rev = NULL;
  table->size = 0;
  table->currId = 1;
  table->hash = poly_h (table->rev, table->size);
}

int ltbl_add (lookup_table *table, int key) {
  ltbl_check (table);

  assert (table->size > 0);

  int x;
  if ( (x = ltbl_get_to (table, key)) ) {
    return x;
  }

  if (table->currId >= table->size) {
    assert (table->currId == table->size);

    int len = table->size;

    table->size = table->currId * 2;
    table->rev = qrealloc (table->rev, sizeof (int) * table->size, sizeof (int) * len);
//    fprintf (stderr, "realloc done\n");
    assert (table->rev != NULL);
    assert (0 < table->size && table->size < 100000000);
    table->hash = poly_h (table->rev, table->size);
    memset (table->rev + len, 0, sizeof (int) * (table->size - len));
  }
  assert (table->currId < table->size);

  table->rev[table->currId] = key;
  //htbl_add (&(table->to), key, table->currId);
  *(htbl_find_or_create (&(table->to), key)) = table->currId;

  return table->currId++;
}

int ltbl_get_to (lookup_table *table, int key) {
  ltbl_check (table);
  assert (table->size > 0);

  int *x = htbl_find (&(table->to), key);

  if (x != NULL) {
    return *x;
  }

  return 0;
}

int ltbl_get_rev (lookup_table *table, int val) {
  ltbl_check (table);
  assert (table->size > 0);

  if (val <= 0 || val >= table->currId) {
    return 0;
  }

  return table->rev[val];
}
