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

    Copyright 2010-2013 Vkontakte Ltd
              2010-2013 Arseny Smirnov
              2010-2013 Aliaksei Levin
*/

#define	_FILE_OFFSET_BITS	64

#include "hash_table.h"

int write_only;

hash_entry_ptr htbl_free_entries = NULL;
int htbl_allocated_cnt = 0;
size_t htbl_memory = 0;

size_t htbl_get_memory (void) {
  return htbl_memory;
}

void htbl_init (hash_table *table) {
  table->size = 0;
  table->h = NULL;
}

void htbl_set_size (hash_table *table, int size) {
  assert (table->h == NULL);

  table->h = dl_malloc0 (sizeof (hash_entry *) * size);
  assert (table->h != NULL);

  table->size = size;
}

void htbl_init_mem (int n) {
  assert (htbl_free_entries == NULL);
  assert (n > 0);

  htbl_memory -= dl_get_memory_used();
  htbl_free_entries = dl_malloc (sizeof (hash_entry) * n);
  assert (htbl_free_entries != NULL);
  htbl_memory += dl_get_memory_used();

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
  if (table == NULL) {
    return;
  }

  int n = table->size, i;
  hash_entry_ptr *h = table->h;
  for (i = 0; i < n; i++) {
    hash_entry_ptr curr = h[i];
    if (curr != NULL) {
      while (curr->next_entry != NULL) {
        chg_free (&curr->data);
        curr = curr->next_entry;
      }
      chg_free (&curr->data);
      curr->next_entry = htbl_free_entries;
      htbl_free_entries = h[i];
      h[i] = NULL;
    }
  }

  dl_free (table->h, sizeof (hash_entry_ptr) * table->size);
  table->h = NULL;
  table->size = 0;
}

void htbl_no_free (hash_table *table) {
  if (table == NULL) {
    return;
  }

  int n = table->size, i;
  hash_entry_ptr *h = table->h;
  for (i = 0; i < n; i++) {
    hash_entry_ptr curr = h[i];
    if (curr != NULL) {
      while (curr->next_entry != NULL) {
        curr = curr->next_entry;
      }
      curr->next_entry = htbl_free_entries;
      htbl_free_entries = h[i];
      h[i] = NULL;
    }
  }

  dl_free (table->h, sizeof (hash_entry_ptr) * table->size);
  table->h = NULL;
  table->size = 0;
}

changes *htbl_find (hash_table *table, long long key) {
  int i = (unsigned int)key % table->size;

  hash_entry *curr = table->h[i];
  while (curr != NULL) {
    if (curr->key == key) {
      return &(curr->data);
    }
    curr = curr->next_entry;
  }

  return NULL;
}

changes *htbl_find_or_create (hash_table *table, long long key) {
  int i = (unsigned int)key % table->size;

  hash_entry *curr = table->h[i];
  while (curr != NULL) {
    if (curr->key == key) {
      return &(curr->data);
    }
    curr = curr->next_entry;
  }

  curr = htbl_get_entry();
  curr->key = key;
  CHG_INIT(curr->data);
  curr->next_entry = table->h[i];
  table->h[i] = curr;

  return &(curr->data);
}

void htbl_add (hash_table *table, long long key, int val) {
  chg_add (htbl_find_or_create (table, key), val * 2 + 1);
}

void htbl_del (hash_table *table, long long key, int val) {
  chg_del (htbl_find_or_create (table, key), val * 2 + 1);
}


#ifdef HINTS
int htbl_vct_allocated_cnt = 0;
size_t htbl_vct_memory = 0;

size_t htbl_vct_get_memory (void) {
  return htbl_vct_memory;
}

hash_entry_vct_ptr htbl_vct_free_entries = NULL;

void htbl_vct_init (hash_table_vct *table) {
  table->size = 0;
  table->h = NULL;
}

void htbl_vct_set_size (hash_table_vct *table, int size) {
  assert (table->h == NULL);

  table->h = dl_malloc0 (sizeof (hash_entry_vct *) * size);
  assert (table->h != NULL);

  table->size = size;
}

void htbl_vct_init_mem (int n) {
  assert (htbl_vct_free_entries == NULL);
  assert (n > 0);

  htbl_vct_memory -= dl_get_memory_used();
  htbl_vct_free_entries = dl_malloc (sizeof (hash_entry_vct) * n);
  assert (htbl_vct_free_entries != NULL);
  htbl_vct_memory += dl_get_memory_used();

  htbl_vct_allocated_cnt += n;

  int i;
  for (i = 0; i + 1 < n; i++) {
    htbl_vct_free_entries[i].next_entry = &htbl_vct_free_entries[i + 1];
  }
  htbl_vct_free_entries[n - 1].next_entry = NULL;
}

hash_entry_vct_ptr htbl_vct_get_entry (void) {
  if (htbl_vct_free_entries == NULL) {
    if (1 <= htbl_vct_allocated_cnt && htbl_vct_allocated_cnt < 10000) {
      htbl_vct_init_mem (htbl_vct_allocated_cnt);
    } else {
      htbl_vct_init_mem (10000);
    }
  }

  assert (htbl_vct_free_entries != NULL);

  hash_entry_vct *res = htbl_vct_free_entries;
  htbl_vct_free_entries = htbl_vct_free_entries->next_entry;
  res->next_entry = NULL;

  return res;
}

void htbl_vct_free (hash_table_vct *table) {
  if (table == NULL) {
    return;
  }

  int n = table->size, i;
  hash_entry_vct_ptr *h = table->h;
  for (i = 0; i < n; i++) {
    hash_entry_vct_ptr curr = h[i];
    if (curr != NULL) {
      while (curr->next_entry != NULL) {
        vct_free (&curr->data);
        curr = curr->next_entry;
      }
      vct_free (&curr->data);
      curr->next_entry = htbl_vct_free_entries;
      htbl_vct_free_entries = h[i];
      h[i] = NULL;
    }
  }

  dl_free (table->h, sizeof (hash_entry_vct_ptr) * table->size);
  table->h = NULL;
  table->size = 0;
}

vector *htbl_vct_find (hash_table_vct *table, long long key) {
  int i = (unsigned int)key % table->size;

  hash_entry_vct *curr = table->h[i];
  while (curr != NULL) {
    if (curr->key == key) {
      return &(curr->data);
    }
    curr = curr->next_entry;
  }

  return NULL;
}

vector *htbl_vct_find_or_create (hash_table_vct *table, long long key) {
  int i = (unsigned int)key % table->size;

  hash_entry_vct *curr = table->h[i];
  while (curr != NULL) {
    if (curr->key == key) {
      return &(curr->data);
    }
    curr = curr->next_entry;
  }

  curr = htbl_vct_get_entry();
  curr->key = key;
  vct_init (&curr->data);
  curr->next_entry = table->h[i];
  table->h[i] = curr;

  return &(curr->data);
}

void htbl_vct_add (hash_table_vct *table, long long key, int val) {
  vct_set_add (htbl_vct_find_or_create (table, key), val);
}
#else
size_t htbl_vct_get_memory (void) {
  return 0;
}
#endif


void ltbl_init (lookup_table *table) {
  htbl_init (&(table->to));
  table->rev = NULL;
  table->size = 0;
  table->currId = 1;
}

void ltbl_set_size (lookup_table *table, int size) {
  htbl_set_size (&(table->to), size);
  table->rev = dl_malloc0 (sizeof (long long) * size);
  table->size = size;
}

void ltbl_free (lookup_table *table) {
  htbl_no_free (&(table->to));
  dl_free (table->rev, sizeof (long long) * table->size);

  table->rev = NULL;
  table->size = 0;
  table->currId = 1;
}

int ltbl_add (lookup_table *table, long long key) {
  int x;

  if ( (x = ltbl_get_to (table, key)) ) {
    return x;
  }

  if (table->currId >= table->size) {
    int len = table->size;

    table->size = table->currId * 2;
//    fprintf (stderr, "%d %d %d\n", sizeof (long long) * table->size, len, table->size - len);
    table->rev = dl_realloc (table->rev, sizeof (long long) * table->size, sizeof (long long) * len);
//    fprintf (stderr, "realloc done\n");
    memset (table->rev + len, 0, sizeof (long long) * (table->size - len));
  }

  table->rev[table->currId] = key;
  //htbl_add (&(table->to), key, table->currId);
  *(long *)(htbl_find_or_create (&(table->to), key)) = table->currId;

  return table->currId++;
}

int ltbl_get_to (lookup_table *table, long long key) {
  changes *x = htbl_find (&(table->to), key);

  if (x != NULL) {
    //return -(long)(*x) / 2;
    return (long)(*x);
  }

  return 0;
}

long long ltbl_get_rev (lookup_table *table, int val) {
  if (val <= 0 || val >= table->size) {
    return 0;
  }

  return table->rev[val];
}

change_list_ptr chg_list_free = NULL;

size_t chg_list_memory = 0;

size_t chg_list_get_memory (void) {
  return chg_list_memory;
}

void chg_list_init_mem (int n) {
  assert (chg_list_free == NULL);
  assert (n > 0);

  chg_list_memory -= dl_get_memory_used();
  chg_list_free = dl_malloc (sizeof (change_list) * n);
  assert (chg_list_free != NULL);
  chg_list_memory += dl_get_memory_used();

  int i;
  for (i = 0; i + 1 < n; i++) {
    chg_list_free[i].next = &chg_list_free[i + 1];
  }
  chg_list_free[n - 1].next = NULL;
}

change_list_ptr chg_list_get_entry (void) {
  if (chg_list_free == NULL) {
    chg_list_init_mem (10000);
  }

  assert (chg_list_free != NULL);

  change_list_ptr res = chg_list_free;
  chg_list_free = chg_list_free->next;
  res->next = NULL;

  return res;
}

void chg_list_init (change_list_ptr *st, change_list_ptr *en) {
  change_list_ptr v = chg_list_get_entry();
  v->type = 0;
  v->x = 0;
  v->s = NULL;
  v->timestamp = 0;
  v->next = NULL;
  *st = *en = v;
}


inline change_list_ptr chg_list_add (change_list_ptr *st, change_list_ptr *en, int type, int x) {
  static int autoincrement = 0;

  change_list_ptr v = chg_list_get_entry();
  v->type = type;
  v->x = x;
  v->timestamp = now;
  v->number = autoincrement++;

  v->next = NULL;
  if (*st == NULL) {
    *st = v;
  } else {
    (*en)->next = v;
  }
  *en = v;

  return v;
}

void chg_list_add_int (change_list_ptr *st, change_list_ptr *en, int type, int x, int cnt) {
  if (!write_only) {
    chg_list_add (st, en, type, x)->cnt = cnt;
  }
}

void chg_list_add_string (change_list_ptr *st, change_list_ptr *en, int type, int x, char *s) {
  if (!write_only) {
    chg_list_add (st, en, type, x)->s = s;
  }
}

void chg_list_add_rating (change_list_ptr *st, change_list_ptr *en, int type, int x, rating val) {
  if (!write_only) {
    chg_list_add (st, en, type, x)->val = val;
  }
}
