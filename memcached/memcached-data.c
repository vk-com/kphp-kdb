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

    Copyright 2010-2012 Vkontakte Ltd
              2010-2012 Arseny Smirnov
              2010-2012 Aliaksei Levin
*/

#define _FILE_OFFSET_BITS 64

#include <assert.h>
#include <unistd.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>

#include "memcached-data.h"

hash_entry_t entry_buffer[MAX_HASH_TABLE_SIZE + TIME_TABLE_SIZE + 1];

int buffer_stack[MAX_HASH_TABLE_SIZE], buffer_stack_size;
int hash_st[HASH_TABLE_SIZE];
int time_st[TIME_TABLE_SIZE];

int last_del_time;

long long malloc_mem;

extern long max_memory;
long long del_by_LRU;

long long get_hash (const char *s, int sn) {
  long long h = 239;
  int i;

  for (i = 0; i < sn; i++) {
    h = h * 999983 + s[i];
  }

  return h;
}

void init_hash_table (void) {
  int i;

  memset (hash_st, -1, sizeof (hash_st));

  for (i = 1; i <= MAX_HASH_TABLE_SIZE; i++) {
    buffer_stack[i - 1] = i;
  }
  buffer_stack_size = MAX_HASH_TABLE_SIZE;

  for (i = 0; i < TIME_TABLE_SIZE; i++) {
    time_st[i] = MAX_HASH_TABLE_SIZE + i + 1;

    entry_buffer[time_st[i]].next_time = time_st[i];
    entry_buffer[time_st[i]].prev_time = time_st[i];
  }

  entry_buffer[0].next_used = 0;
  entry_buffer[0].prev_used = 0;

  last_del_time = GET_TIME_ID (get_utime (CLOCK_MONOTONIC));
  malloc_mem = 0;
  del_by_LRU = 0;
}

void del_entry_used (int x) {
  hash_entry_t *entry = &entry_buffer[x];
  entry_buffer[entry->next_used].prev_used = entry->prev_used;
  entry_buffer[entry->prev_used].next_used = entry->next_used;
}

void add_entry_used (int x) {
  int y = entry_buffer[0].prev_used;
  hash_entry_t *entry = &entry_buffer[x];

  entry->next_used = 0;
  entry_buffer[0].prev_used = x;

  entry->prev_used = y;
  entry_buffer[y].next_used = x;
}

void del_entry_time (int x) {
  hash_entry_t *entry = &entry_buffer[x];
  entry_buffer[entry->next_time].prev_time = entry->prev_time;
  entry_buffer[entry->prev_time].next_time = entry->next_time;
}

void add_entry_time (int x) {
  hash_entry_t *entry = &entry_buffer[x];

  int f = time_st[GET_TIME_ID (entry->exp_time)];
  int y = entry_buffer[f].prev_time;

  entry->next_time = f;
  entry_buffer[f].prev_time = x;

  entry->prev_time = y;
  entry_buffer[y].next_time = x;
}

void add_entry (int x) {
  int i = GET_ENTRY_ID (entry_buffer[x].key_hash);

  entry_buffer[x].next_entry = hash_st[i];
  hash_st[i] = x;
}

void del_entry (int x) {
  hash_entry_t *entry = &entry_buffer[x];

  del_entry_used (x);
  del_entry_time (x);

  zzfree (entry->key, entry->key_len + 1);
  zzfree (entry->data, entry->data_len + 1);

  int *i = &hash_st[GET_ENTRY_ID (entry->key_hash)];

  while (*i != x && *i != -1) {
    i = &(entry_buffer[*i].next_entry);
  }

  assert (*i == x);

  *i = entry->next_entry;

  buffer_stack[buffer_stack_size++] = x;
}


int get_entry (const char *key, int key_len, long long hash) {
  int i = hash_st[GET_ENTRY_ID (hash)];

  while (i != -1 && (hash != entry_buffer[i].key_hash || key_len != entry_buffer[i].key_len ||
                     strncmp (key, entry_buffer[i].key, key_len) != 0)) {
    i = entry_buffer[i].next_entry;
  }

  if (i != -1 && entry_buffer[i].exp_time < get_utime (CLOCK_MONOTONIC)) {
    del_entry (i);
    i = -1;
  }

  return i;
}

int get_entry_no_check (long long hash) {
  int i = hash_st[GET_ENTRY_ID (hash)];

  while (i != -1 && hash != entry_buffer[i].key_hash) {
    i = entry_buffer[i].next_entry;
  }

  return i;
}

hash_entry_t *get_entry_ptr (int x) {
  return entry_buffer + x;
}

int free_LRU (void) {
  int used_st = entry_buffer[0].next_used;
  if (used_st == 0) {
    return -1;
  }

  del_by_LRU++;
  del_entry (used_st);

  return 0;
}

void free_by_time (int mx) {
  int en = GET_TIME_ID (get_utime (CLOCK_MONOTONIC)), st = time_st[last_del_time];

  while (en - last_del_time > MAX_TIME_GAP || last_del_time - en > TIME_TABLE_SIZE - MAX_TIME_GAP ||
         (mx-- && last_del_time != en)) {
    if (entry_buffer[st].next_time != st) {
      if (verbosity > 0) {
        fprintf (stderr, "del entry %d by time (key = %s) gap = %d\n", entry_buffer[st].next_time, entry_buffer[entry_buffer[st].next_time].key, en - last_del_time);
      }
      del_entry (entry_buffer[st].next_time);
    } else {
      if (++last_del_time == TIME_TABLE_SIZE) {
        last_del_time = 0;
      }
      st = time_st[last_del_time];
    }
  }
}


int get_new_entry (void) {
  if (buffer_stack_size == 0) {
    assert (free_LRU() == 0);
  }

  return buffer_stack[--buffer_stack_size];
}

int return_one_key_flags (struct connection *c, const char *key, char *val, int vlen, int flags) {
  static char buff[65536];
  int l = sprintf (buff, "VALUE %s %d %d\r\n", key, flags, vlen);
  assert (l <= 65536);
  write_out (&c->Out, buff, l);
  write_out (&c->Out, val, vlen);
  write_out (&c->Out, "\r\n", 2);
  return 0;
}


void *zzmalloc (int size) {
  void *res;

  while (get_memory_used() > max_memory && free_LRU() == 0) {
  }

  assert (get_memory_used() <= max_memory);

  if (size < MAX_ZMALLOC_MEM)
  {
    while (!(res = dyn_alloc (size, PTRSIZE)) && free_LRU() == 0) {
    }
  } else {
    while (!(res = malloc (size)) && free_LRU() == 0) {
    }
    malloc_mem += size;
  }

  assert (res);
  return res;
}

void zzfree (void *ptr, int size) {
  if (size < MAX_ZMALLOC_MEM) {
    zfree (ptr, size);
  } else {
    malloc_mem -= size;
    free (ptr);
  }
}

int get_entry_cnt (void) {
  return MAX_HASH_TABLE_SIZE - buffer_stack_size;
}

long get_memory_used (void) {
  return malloc_mem + (dyn_cur - dyn_first);
}

long get_min_memory (void) {
  return (sizeof (entry_buffer) + sizeof (buffer_stack) + sizeof (hash_st) + sizeof (time_st)) / 1048576 + 1;
}

long get_min_memory_bytes (void) {
  return (sizeof (entry_buffer) + sizeof (buffer_stack) + sizeof (hash_st) + sizeof (time_st)) ;
}

long long get_del_by_LRU (void) {
  return del_by_LRU;
}

long long get_time_gap (void) {
  return (GET_TIME_ID (get_utime (CLOCK_MONOTONIC)) - last_del_time) << TIME_TABLE_RATIO_EXP;
}

void write_stats (void) {
  int now = get_utime (CLOCK_MONOTONIC);

  int x = entry_buffer[0].next_used;
  struct hash_entry *entry = &entry_buffer[x];

  if (x == 0) {
    fprintf (stderr, "Memcached is empty\n");
    return;
  }

  char *key = zzmalloc (1024 + 3);
  key[0] = ' ';

  int j, k;
  while (x != 0 && entry->key[0] != ' ') {
    del_entry_used (x);
    del_entry_time (x);

    zzfree (entry->data, entry->data_len + 1);

    int *i = &hash_st[GET_ENTRY_ID (entry->key_hash)];

    while (*i != x && *i != -1) {
      i = &(entry_buffer[*i].next_entry);
    }

    assert (*i == x);

    *i = entry->next_entry;

    for (j = 0; j < entry->key_len && (entry->key[j] < '0' || entry->key[j] > '9') && entry->key[j] != '-' && entry->key[j] != ':'; j++) {
    }

    while (j > 1 && entry->key[j - 1] == '_') {
      j--;
    }

    int _count = 0, need_reduce_j = 0;
    for (k = j; k < entry->key_len; k++) {
      if (entry->key[k] == '_' && entry->key[k - 1] != '_') {
        _count++;
      }
      if (_count == 0 && entry->key[k] >= 'a' && entry->key[k] <= 'f') {
        need_reduce_j = 1;
      }
    }

    if (need_reduce_j) {
      while (j > 2 && entry->key[j - 1] >= 'a' && entry->key[j - 1] <= 'f') {
        j--;
      }
    }

    int key_len;

    if (j < entry->key_len && j < 1024 && entry->key_len > 0) {
      key_len = j + 3;

      memcpy (key + 1, entry->key, j);
      if (_count > 9) {
        _count = 9;
      }

      key[j + 1] = '0' + _count;
      key[j + 2] = (entry->key[j] != '_') * 2 + (entry->key[entry->key_len - 1] != '_') + '0';
    } else {
      key_len = 6;
      memcpy (key + 1, "OTHER", 5);
    }

    int size = key_len - 1 + 6 * sizeof (int) + 1;
    int left = entry->exp_time - now;

    long long key_hash = get_hash (key, key_len);
    int y = get_entry (key, key_len, key_hash);

    hash_entry_t *new_entry;

    if (y != -1) {
      new_entry = get_entry_ptr (y);

      del_entry_used (y);
    } else {
      y = get_new_entry ();
      new_entry = get_entry_ptr (y);

      new_entry->key = zzmalloc (key_len + 1);
      memcpy (new_entry->key, key, key_len);
      new_entry->key[key_len] = 0;

      new_entry->key_len = key_len;
      new_entry->key_hash = key_hash;

      add_entry (y);

      new_entry->data = zzmalloc (size);
      memset (new_entry->data, 0, sizeof (int) * 6);

      memcpy (new_entry->data + 6 * sizeof (int), new_entry->key + 1, key_len);
      new_entry->data_len = size - 1;
      new_entry->exp_time = 86400 + now;

      add_entry_time (y);
    }

    int *t = (int *)new_entry->data;
    t[0]++;
    t[1] += entry->key_len;
    t[2] += entry->data_len;
    t[3] += left / 500;
    t[4] += left % 500;
    if (left > t[5]) {
      t[5] = left;
    }

    add_entry_used (y);

    zzfree (entry->key, entry->key_len + 1);
    buffer_stack[buffer_stack_size++] = x;

    x = entry_buffer[0].next_used;
    entry = &entry_buffer[x];
  }

  zzfree (key, 1024 + 3);

  fprintf (stderr, "   quantity\ttot_key_len\ttot_val_len\t tot_memory\tmean_memory\t  mean_exp_time\tmax_exp_time\tprefix\n");
  int total[6] = {0};

  while (x != 0) {
    del_entry_used (x);
    del_entry_time (x);

    int *i = &hash_st[GET_ENTRY_ID (entry->key_hash)];

    while (*i != x && *i != -1) {
      i = &(entry_buffer[*i].next_entry);
    }

    assert (*i == x);

    *i = entry->next_entry;

    int *t = (int *)entry->data;

    fprintf (stderr, "%11d\t%11d\t%11d\t%11d\t%11.1lf\t%15.6lf\t%12d\t%s\n", t[0], t[1], t[2], t[1] + t[2] + (2 + (int)sizeof (struct hash_entry)) * t[0],
                                                                                   (t[1] + t[2]) * 1.0 / t[0] + (2 + sizeof (struct hash_entry)),
                                                                                   (t[3] * 500.0 + t[4]) / t[0], t[5], (char *)(t + 6));

    for (j = 0; j < 5; j++) {
      total[j] += t[j];
    }
    if (t[5] > total[5]) {
      total[5] = t[5];
    }

    zzfree (entry->key, entry->key_len + 1);
    zzfree (entry->data, entry->data_len + 1);
    buffer_stack[buffer_stack_size++] = x;

    x = entry_buffer[0].next_used;
    entry = &entry_buffer[x];
  }

  int *t = total;
  fprintf (stderr, "%11d\t%11d\t%11d\t%11d\t%11.1lf\t%15.6lf\t%12d\t%s\n", t[0], t[1], t[2], t[1] + t[2] + (2 + (int)sizeof (struct hash_entry)) * t[0],
                                                                                 (t[1] + t[2]) * 1.0 / t[0] + (2 + sizeof (struct hash_entry)),
                                                                                 (t[3] * 500.0 + t[4]) / t[0], t[5], "TOTAL");
}
