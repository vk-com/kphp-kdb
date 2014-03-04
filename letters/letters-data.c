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

    Copyright 2011-2013 Vkontakte Ltd
              2011-2013 Arseny Smirnov
              2011-2013 Aliaksei Levin
*/

#define _FILE_OFFSET_BITS 64

#define _XOPEN_SOURCE 500

#include <aio.h>
#include <stdarg.h>
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

#include "letters-data.h"

char debug_buff[1 << 26];
char *ds;
int debug_error;

void debug (char const *msg, ...) {
  if (ds - debug_buff < (1 << 26) - 2 * MAX_LETTER_SIZE - 2000) {
    va_list args;

    va_start (args, msg);
    ds += vsprintf (ds, msg, args);
    va_end (args);
  } else {
    debug_error = 1;
  }
}

long max_memory = MAX_MEMORY;

int log_drive = 0;

long long expired_letters;
long long letter_stat[MAX_PRIORITY][6]; //added, deleted, in_memory, read, write, sync
const char *letter_stat_name[6] = {"letters_added", "letters_deleted", "letters_in_memory", "pread_calls", "write_calls", "aio_fsync_calls"};

int engine_num = -1;
int total_engines = -1;


index_header header;
char *memory_buf[MAX_PRIORITY];
char *write_buf[MAX_PRIORITY];

#include "dl-hashable.h"

/*
 *
 * strict_hash_map <ll, vptr>
 *
 */

#define DATA_T ll
#define RDATA_T vptr
#define TNAME map_ll_vptr
#define IMPLEMENTATION OPEN
#define STORE_HASH OFF
#define STRICT OFF
#define MAP ON
#define EMPTY(p) (p == 0)

#include "dl-hashtable.c"
#include "dl-undef.h"


/*
 *
 * strict_hash_map <ll, int>
 *
 */

#define DATA_T ll
#define RDATA_T int
#define TNAME map_ll_int
#define IMPLEMENTATION OPEN
#define STORE_HASH OFF
#define STRICT OFF
#define MAP ON
#define NOZERO
#define EMPTY(p) (p == 0)

#include "dl-hashtable.c"
#include "dl-undef.h"

#include "dl-vector-def.h"


/*
 *
 * pair <ll, int>
 *
 */

#define TA ll
#define TB int
#define TNAME pair_ll_int
#include "dl-pair.c"
#include "dl-undef.h"


int task_deletes_begin;
vector (pair_ll_int, task_deletes);
map_ll_int task_id_to_delete_time;

void remove_expired_deletes_by_task_id (void) {
  while (task_deletes_begin < task_deletes_size && task_deletes[task_deletes_begin].y < now - MAX_DELAY - 2) {
    int *y = map_ll_int_get (&task_id_to_delete_time, task_deletes[task_deletes_begin].x);
    assert (y);
    if (*y == task_deletes[task_deletes_begin].y) {
      map_ll_int_del (&task_id_to_delete_time, task_deletes[task_deletes_begin].x);
    }
    task_deletes_begin++;
  }

  if (task_deletes_begin > 4 * task_deletes_size / 5 + 1) {
    memcpy (task_deletes, task_deletes + task_deletes_begin, (task_deletes_size - task_deletes_begin) * sizeof (task_deletes[0]));
    task_deletes_size -= task_deletes_begin;
    task_deletes_begin = 0;
  }
}

int is_deleted_task (long long task_id, int time_received) {
  remove_expired_deletes_by_task_id();

  if (task_id != 0) {
    int *y = map_ll_int_get (&task_id_to_delete_time, task_id);
    if (y != NULL && *y >= time_received) {
      return 1;
    }
  }
  return 0;
}

int delete_letters_by_task_id (long long task_id) {
  remove_expired_deletes_by_task_id();

  if (task_id != 0) {
    int *y = map_ll_int_add (&task_id_to_delete_time, task_id);
    assert (y);
    if (*y != now) {
      *y = now;
      pair_ll_int task_delete = {
        .x = task_id,
        .y = now
      };

      vector_pb (task_deletes, task_delete);
    }
    return 1;
  }
  return 0;
}


#pragma pack(push, 4)

typedef struct {
  long long magic;
  int data_len;
  char data[0];
} letter;

typedef struct temp_letter_t temp_letter;

struct temp_letter_t {
  int priority;
  int real_priority;
  int time_received;
  long long task_id;
  int time_sent;
  long long id;
  long long drive_l;
  letter *l;
  temp_letter *next, *prev;
};

#pragma pack(pop)

void temp_letter_init (temp_letter *head_letter) {
  head_letter->id = head_letter->drive_l = head_letter->priority = -1;
  head_letter->l = NULL;
  head_letter->next = head_letter;
  head_letter->prev = head_letter;
}

#define MAX_FIELDS 99

letter *letter_buf;
letter *letter_buf_get;

int field_n;
char *field_name[MAX_FIELDS];
char *field_value[MAX_FIELDS];
int field_name_len[MAX_FIELDS];
int field_value_len[MAX_FIELDS];

int get_fields (char *l) {
  int n;
  int len = strlen (l);
  int add;
  field_n = -1;
  if (sscanf (l, "a:%d%n", &n, &add) != 1 || l[add] != ':' || l[add + 1] != '{' || l[len - 1] != '}' || n < 0 || n > MAX_FIELDS || len > 2 * MAX_LETTER_SIZE + 1000) {
    return -1;
  }
  int i, j = add + 2;
  for (i = 0; i < n; i++) {
    int clen;
    if (sscanf (l + j, "s:%d%n", &clen, &add) != 1 || l[j + add] != ':' || l[j + add + 1] != '"') {
      return -1;
    }
    j += add + 2;
    field_name[i] = l + j;
    field_name_len[i] = clen;

    j += clen;

    if (j >= len || l[j] != '"' || l[j + 1] != ';') {
      return -1;
    }
    j += 2;

    switch (l[j]) {
      case 'b':
        if (l[j + 1] == ':' && (l[j + 2] == '0' || l[j + 2] == '1') && l[j + 3] == ';') {
          l[j] = 'i';

          field_value[i] = l + j + 2;
          field_value_len[i] = 1;

          j += 4;
        } else {
          return -1;
        }
        break;
      case 'd': {
        double value;
        if (sscanf (l + j, "d:%lf%n", &value, &add) != 1 || add < 3 || l[j + add] != ';') {
          return -1;
        }
        field_value[i] = l + j + 2;
        field_value_len[i] = add - 2;

        j += add + 1;
        break;
      }
      case 'i':
        if (sscanf (l + j, "i:%d%n", &clen, &add) != 1 || add < 3 || l[j + add] != ';') {
          return -1;
        }
        field_value[i] = l + j + 2;
        field_value_len[i] = add - 2;

        j += add + 1;
        break;
      case 's':
        if (sscanf (l + j, "s:%d%n", &clen, &add) != 1 || l[j + add] != ':' || l[j + add + 1] != '"') {
          return -1;
        }
        j += add + 2;
        field_value[i] = l + j;
        field_value_len[i] = clen;

        j += clen;

        if (j >= len) {
          return -1;
        }

        if (l[j++] != '"') {
          return -1;
        }
        if (l[j++] != ';') {
          return -1;
        }
        break;
      default:
        return -1;
    }
  }

  if (j != len - 1) {
    return -1;
  }
  return field_n = n;
}

//works only after get_fields
int get_field (const char *name) {
  int l = strlen (name), i;
  for (i = 0; i < field_n; i++) {
    if (field_name_len[i] == l && !strncmp (field_name[i], name, l)) {
      return i;
    }
  }
  return -1;
}

#define FIELDS_N 6
#define FIELDS_REQUIRED_N 1
const char *field_names[FIELDS_N] = {
  "priority",
  "task_id",
  "send_after",
  "date_sent",
  "id",
  "error_code"
};

#if MAX_FIELDS >= 100 || MAX_FIELDS < FIELDS_N
#  error Wrong MAX_FIELDS
#endif

void upd_mx (one_header *data) {
  if (data->drive_old_mx < data->drive_mx) {
    assert (data->drive_r < data->drive_old_mx && data->drive_l <= data->drive_r);
  }
  data->drive_old_mx = data->drive_mx;
}

#define DELAYED_TABLE_SIZE_EXP 13
#define DELAYED_TABLE_SIZE (1 << DELAYED_TABLE_SIZE_EXP)
#define DELAYED_TABLE_MASK (DELAYED_TABLE_SIZE - 1)
#define GET_DELAYED_ID(x) ((unsigned int)(x) & DELAYED_TABLE_MASK)

#if DELAYED_TABLE_SIZE < 2 * MAX_DELAY
#  error Wrong DELAYED_TABLE_SIZE
#endif

temp_letter head_letter;
temp_letter delayed_head_letter[DELAYED_TABLE_SIZE];
int last_process_delayed_time;

map_ll_vptr id_to_letter, drive_l_to_letter[MAX_PRIORITY];
long long cur_id;

inline int get_letter_size (letter *l) {
  assert (l->data_len >= 0 && l->data_len < 2 * MAX_LETTER_SIZE + 1000);
  assert ((offsetof (letter, data) + l->data_len + sizeof (int)) % sizeof (long long) == 0);
  return offsetof (letter, data) + l->data_len + sizeof (int) + sizeof (LETTER_END) + sizeof (FILE_END);
}

temp_letter *letter_get (int priority) {
  one_header *data = &header.data[priority];

  if (data->memory_buf_l == data->memory_buf_r && data->memory_buf_l != 0) {
    assert (0);
    data->memory_buf_l = data->memory_buf_r = 0;
  }
  if (data->memory_buf_l > 11 * data->memory_buf_mx / 15) {
    memcpy (memory_buf[priority], memory_buf[priority] + data->memory_buf_l, data->memory_buf_r - data->memory_buf_l);
    data->memory_buf_r -= data->memory_buf_l;
    data->memory_buf_l = 0;
  }

  letter *l = (letter *)(memory_buf[priority] + data->memory_buf_l);
  long long real_drive_r = (data->drive_r + sizeof (long long)) % data->drive_old_mx;
  long long read_from = data->memory_r - sizeof (long long);
  int sub = sizeof (long long);

  assert (data->memory_l != data->memory_r);
  assert (read_from >= 0);

  while ((data->memory_buf_r - data->memory_buf_l < (long long)(sizeof (long long) + offsetof (letter, data)) || data->memory_buf_r - data->memory_buf_l < get_letter_size (l)) && read_from != data->drive_r) {
    if (data->memory_r == data->drive_old_mx) {
      read_from = 0;
      sub = 0;
    }

    long long to_read = data->memory_buf_mx - data->memory_buf_r;
    if (read_from < real_drive_r) {
      if (to_read > real_drive_r - read_from) {
        to_read = real_drive_r - read_from;
      }
    } else {
      if (to_read > data->drive_old_mx - read_from) {
        to_read = data->drive_old_mx - read_from;
      }
    }
    assert (to_read % sizeof (long long) == 0 && to_read > 0);
    letter_stat[priority][3]++;
    if (pread (fd[2 + priority], memory_buf[priority] + data->memory_buf_r - sub, to_read, read_from) != to_read) {
      fprintf (stderr, "!!! %d : [%lld, %lld], %lld, [%lld, %lld], [%lld, %lld], %lld, %lld, %d %d\n", priority, data->memory_buf_l, data->memory_buf_r, to_read, data->memory_l, data->memory_r, data->drive_l, data->drive_r, data->drive_old_mx, read_from, sub, data->memory_buf_r - data->memory_buf_l < (long long)(sizeof (long long) + offsetof (letter, data)) ? -1 : get_letter_size (l));
      assert (0);
    } else if (log_drive) {
      kprintf ("read  %9lld bytes from position %12lld in buffer %d\n", to_read, read_from, priority);
    }
    data->memory_r = (read_from + to_read) % data->drive_old_mx;
    if (data->memory_r == 0) {
      data->memory_r = data->drive_old_mx;
    }
    data->memory_buf_r += to_read - sub;
  }

  if (data->memory_buf_r - data->memory_buf_l < (long long)(sizeof (long long) + offsetof (letter, data))) {
    assert (data->memory_buf_r - data->memory_buf_l == sizeof (long long));
    if (*(long long *)(memory_buf[priority] + data->memory_buf_l) != FILE_END) {
      fprintf (stderr, "%d : [%lld, %lld], [%lld/%lld], [%lld, %lld], [%lld, %lld], %lld\n", priority,
                       data->drive_l, data->drive_r,
                       data->drive_old_mx, data->drive_mx,
                       data->memory_l, data->memory_r,
                       data->memory_buf_l, data->memory_buf_r,
                       data->memory_buf_mx);
    }

    assert (*(long long *)(memory_buf[priority] + data->memory_buf_l) == FILE_END);
    return NULL;
  }
  int data_len = get_letter_size (l);
  assert (data->memory_buf_r - data->memory_buf_l >= data_len);
  memcpy (letter_buf_get, memory_buf[priority] + data->memory_buf_l, data_len - sizeof (long long));
  assert (letter_buf_get->magic == LETTER_BEGIN);

  crc32_check_and_repair ((unsigned char *)letter_buf_get->data, letter_buf_get->data_len,
                          (unsigned int *)(letter_buf_get->data + letter_buf_get->data_len), 1);

  assert (*(long long *)(letter_buf_get->data + letter_buf_get->data_len + sizeof (int)) == LETTER_END);

  data->memory_buf_l += data_len - sizeof (long long);

  if (get_fields (letter_buf_get->data) < 4) {
    fprintf (stderr, "|%d|%s|\n", get_fields (letter_buf_get->data), letter_buf_get->data);
    assert (0);
  }
  int id_i = get_field (field_names[FIELDS_N - 2]);
  assert (id_i >= 0);

  temp_letter *res = dl_malloc (sizeof (temp_letter));
  res->priority = priority;
  res->time_sent = now;
//  sscanf (field_value[id_i], "%lld", &res->id);
  res->id = (++cur_id) * total_engines + engine_num;
  sprintf (field_value[id_i], "%019lld", res->id);
  field_value[id_i][19] = '"';

  res->drive_l = data->memory_l;
  res->l = dl_malloc (data_len);
  memcpy (res->l, letter_buf_get, data_len);

  temp_letter *list_add_to;
  if (priority > 0) {
    list_add_to = &head_letter;

    res->real_priority = priority;
    res->time_received = now;
    res->task_id = 0;
  } else {
    int priority_i = get_field (field_names[0]);
    assert (priority_i != -1);
    sscanf (field_value[priority_i], "%d", &res->real_priority);

    int time_received_i = get_field (field_names[FIELDS_N - 3]);
    assert (time_received_i != -1);
    sscanf (field_value[time_received_i], "%d", &res->time_received);

    int task_id_i = get_field (field_names[FIELDS_N - 5]);
    if (task_id_i == -1) {
      res->task_id = 0;
    } else {
      sscanf (field_value[task_id_i], "%lld", &res->task_id);
    }

    int send_after_i = get_field (field_names[FIELDS_N - 4]);
    int send_after;
    if (send_after_i == -1) {
      send_after = now;
    } else {
      sscanf (field_value[send_after_i], "%d", &send_after);
      if (send_after < now) {
        send_after = now;
      }
    }

    list_add_to = delayed_head_letter + GET_DELAYED_ID(send_after);
  }
  list_add_to->prev->next = res;
  res->prev = list_add_to->prev;

  list_add_to->prev = res;
  res->next = list_add_to;

  assert (map_ll_vptr_get (&id_to_letter, res->id) == NULL);
  assert (map_ll_vptr_get (&drive_l_to_letter[priority], res->drive_l + 1) == NULL);
  *map_ll_vptr_add (&id_to_letter, res->id) = (void *)res;
  *map_ll_vptr_add (&drive_l_to_letter[priority], res->drive_l + 1) = (void *)res;

  data->memory_l = (data->memory_l + data_len - sizeof (long long)) % data->drive_old_mx;

  letter_stat[priority][2]++;

  return res;
}

void write_to (int priority, const void *letter_buf, int count) {
//  fprintf (stderr, "!!! %d %p %d\n", priority, letter_buf, count);
  assert (write (fd[2 + priority], letter_buf, count) == (ssize_t)count);
  if (log_drive) {
    kprintf ("write %9d bytes  to  position %12lld in buffer %d\n", count, header.data[priority].drive_r, priority);
  }
  letter_stat[priority][4]++;
}

int overflow_cnt[MAX_PRIORITY];
void letter_add (int priority, int delay) {
  if (delay > 0) {
    priority = 0;
  }

  one_header *data = &header.data[priority];

  int data_len = strlen (letter_buf->data) + 1;
  while ((offsetof (letter, data) + data_len + sizeof (int)) % sizeof (long long) != 0) {
    letter_buf->data[data_len++] = 0;
  }
  letter_buf->data_len = data_len;
  *(int *)(letter_buf->data + data_len) = compute_crc32 (letter_buf->data, data_len);
  *(long long *)(letter_buf->data + data_len + sizeof (int)) = LETTER_END;
  *(long long *)(letter_buf->data + data_len + sizeof (int) + sizeof (long long)) = FILE_END;

  int write_len = get_letter_size (letter_buf) + 2 * sizeof (long long);
  while ((data->drive_r + write_len <= data->drive_mx && (data->drive_l > data->drive_r && data->drive_l <= data->drive_r + write_len)) ||
         (data->drive_r + write_len  > data->drive_mx && (data->drive_l > data->drive_r || data->drive_l <= data->drive_r + write_len - data->drive_mx))) {
    if (priority != MAX_PRIORITY - 1) {
      if (overflow_cnt[priority]++ % 1000 == 0) {
        fprintf (stderr, "Warning!!! Buffer for letters with priority %d overflows.\n", priority);
      }
      expired_letters++;
    }
    if (data->memory_l == data->drive_l) {
      assert (letter_get (priority)->drive_l == data->drive_l);
    }

    void **ptr = map_ll_vptr_get (&drive_l_to_letter[priority], data->drive_l + 1);
    assert (ptr);
    temp_letter *l = (temp_letter *)*ptr;
    delete_letter (l->id);
  }

  write_len -= 2 * sizeof (long long);
  assert (write_len % sizeof (long long) == 0 && write_len > 0 && write_len < 2 * MAX_LETTER_SIZE + 500);

  if (write_len + data->memory_buf_r + (long long)sizeof (long long) < data->memory_buf_mx && data->memory_r - (long long)sizeof (long long) == data->drive_r) {
    assert (data->memory_l != data->memory_r);

    memcpy (memory_buf[priority] + data->memory_buf_r - sizeof (long long), letter_buf, write_len);
    data->memory_r = (data->memory_r + write_len - sizeof (long long)) % data->drive_mx;
    if (data->memory_r == 0) {
      data->memory_r = data->drive_mx;
    }
    data->memory_buf_r += write_len - sizeof (long long);
  }

  if (data->drive_r + write_len <= data->drive_mx) {
    if (data->drive_r + write_len > data->drive_old_mx) {
      upd_mx (data);
    }
    write_to (priority, letter_buf, write_len);
    data->drive_r += write_len - sizeof (long long);
  } else {
    upd_mx (data);
    int left = data->drive_mx - data->drive_r;
    write_to (priority, letter_buf, left);
    assert (lseek (fd[2 + priority], 0, SEEK_SET) == 0);
    data->drive_r = write_len - left - sizeof (long long);
    assert (data->drive_r >= 0);
    write_to (priority, ((char *)letter_buf) + left, data->drive_r + sizeof (long long));
  }
  assert (lseek (fd[2 + priority], data->drive_r, SEEK_SET) == data->drive_r);
  letter_stat[priority][0]++;
}

long long get_drive_buffer_size (int priority) {
  long long l = header.data[priority].drive_l;
  long long r = header.data[priority].drive_r;
  if (l <= r) {
    return r - l;
  } else {
    return r + header.data[priority].drive_old_mx - l;
  }
}

long long get_drive_buffer_mx (int priority) {
  return header.data[priority].drive_mx;
}

int add_letter (int delay, long long task_id, char *let) {
  if (get_fields (let) < 1) {
    return 0;
  }

  int i;
  for (i = 0; i < FIELDS_REQUIRED_N; i++) {
    if (get_field (field_names[i]) == -1) {
      return 0;
    }
  }

  for (i = FIELDS_REQUIRED_N; i < FIELDS_N; i++) {
    if (get_field (field_names[i]) != -1) {
      return 0;
    }
  }

  int l = strlen (let);
  char *s = letter_buf->data;
  memcpy (s, let, l + 1);

//  if (field_n < 10) {
  if (s[3] == ':') {
    for (i = l; i > 2; i--) {
      s[i] = s[i - 1];
    }
    s[2] = '0';
  } else {
    l--;
  }

  int priority_num = get_field (field_names[0]);
  if (field_value_len[priority_num] != 1) {
    return 0;
  }
  int priority = field_value[priority_num][0] - '0';
  if (priority <= 0 || priority >= MAX_PRIORITY || l >= MAX_LETTER_SIZE || field_n + 3 >= MAX_FIELDS) {
    return 0;
  }

  field_n += FIELDS_N - FIELDS_REQUIRED_N;
  s[2] = field_n / 10 + '0';
  s[3] = field_n % 10 + '0';

  l += sprintf (s + l, "s:%d:\"%s\";s:20:\"%020lld\";"
                       "s:%d:\"%s\";s:10:\"%010d\";"
                       "s:%d:\"%s\";s:10:\"%010d\";"
                       "s:%d:\"%s\";s:19:\"%019lld\";"
                       "s:%d:\"%s\";s:0000000000:\"\";}",
                       (int)strlen (field_names[FIELDS_N - 5]), field_names[FIELDS_N - 5], task_id,
                       (int)strlen (field_names[FIELDS_N - 4]), field_names[FIELDS_N - 4], now + delay - 1,
                       (int)strlen (field_names[FIELDS_N - 3]), field_names[FIELDS_N - 3], now,
                       (int)strlen (field_names[FIELDS_N - 2]), field_names[FIELDS_N - 2], 0ll,
                       (int)strlen (field_names[FIELDS_N - 1]), field_names[FIELDS_N - 1]);

  s[l] = 0;

  letter_add (priority, delay);
  return 1;
}

int add_letter_priority (long long id, int priority, int delay, const char *error) {
  if (strlen (error) >= MAX_LETTER_SIZE) {
    return 0;
  }

  void **ptr = map_ll_vptr_get (&id_to_letter, id);
  if (ptr != NULL) {
    temp_letter *l = (temp_letter *)*ptr;
    assert (l != NULL && l->l != NULL && l->next != NULL);

    int len = strlen (l->l->data);
    assert (len < l->l->data_len);
    memcpy (letter_buf->data, l->l->data, len + 1);

    assert (get_fields (letter_buf->data) >= 4);

    int priority_i = get_field (field_names[0]);
    assert (priority_i != -1 && field_value_len[priority_i] == 1);
    field_value[priority_i][0] = '0' + priority;

    int send_after_i = get_field (field_names[FIELDS_N - 4]);
    if (send_after_i != -1) {
      assert (send_after_i == field_n - 4 && field_value_len[send_after_i] == 10);
      field_value[send_after_i][sprintf (field_value[send_after_i], "%010d", now + delay - 1)] = '"';
    } else {
      delay = 0;
    }

    int time_sent_i = get_field (field_names[FIELDS_N - 3]);
    assert (time_sent_i == field_n - 3 && field_value_len[time_sent_i] == 10);
    field_value[time_sent_i][sprintf (field_value[time_sent_i], "%010d", now)] = '"';

    int error_i = get_field (field_names[FIELDS_N - 1]);
    assert (error_i == field_n - 1);
    sprintf (field_value[error_i] - 12, "%010d:\"%s\";}", (int)strlen (error), error);

    delete_letter (id);
    letter_add (priority, delay);
    return 1;
  }

  return 0;
}

int delete_letter (long long id) {
  void **ptr = map_ll_vptr_get (&id_to_letter, id);
  if (ptr != NULL) {
    temp_letter *l = (temp_letter *)*ptr;
    assert (l != NULL);
    assert (l->id == id);

    map_ll_vptr_del (&id_to_letter, l->id);

    l->prev->next = l->next;
    l->next->prev = l->prev;
    l->prev = NULL;
    l->next = NULL;

    one_header *data = &header.data[l->priority];
    while (l != NULL && data->drive_l == l->drive_l && l->next == NULL) {
      int size = get_letter_size (l->l);

      data->drive_l += size - sizeof (long long);
//      if (header.data->drive_l >= data->drive_old_mx) {
      if (data->drive_l >= data->drive_old_mx) {
        data->drive_l -= data->drive_old_mx;
        if (data->drive_old_mx < data->drive_mx) {
          upd_mx (data);
        }
      }

      int pr = l->priority;
      letter_stat[pr][1]++;
      letter_stat[pr][2]--;
      map_ll_vptr_del (&drive_l_to_letter[pr], l->drive_l + 1);
      dl_free (l->l, size);
      dl_free (l, sizeof (temp_letter));

      ptr = map_ll_vptr_get (&drive_l_to_letter[pr], data->drive_l + 1);
      if (ptr != NULL) {
        l = (temp_letter *)*ptr;
      } else {
        l = NULL;
      }
    }
    return 1;
  }
  return 0;
}

char *get_letters (int min_priority, int max_priority, int cnt, int immediate_delete) {
  if (max_priority >= MAX_PRIORITY) {
    max_priority = MAX_PRIORITY - 1;
  }
  if (min_priority <= 0) {
    min_priority = 1;
  }

  ds = debug_buff;
  debug ("a:0000000:{");
  int i = 0;
  while (min_priority <= max_priority && i < cnt && ds - debug_buff < (1 << 26) - 4 * MAX_LETTER_SIZE - 4000 && dl_get_memory_used() < max_memory) {
    temp_letter *temp = letter_get (min_priority);
    if (temp == NULL) {
      min_priority++;
    } else {
      debug ("i:%d;%s", i++, temp->l->data);
      if (immediate_delete) {
        delete_letter (temp->id);
      }
    }
  }
  debug ("}");
  sprintf (debug_buff + 2, "%07d", i);
  debug_buff[9] = ':';
  if (debug_error) {
    fprintf (stderr, "Debug error!!!\n");
    return "a:0:{}";
  }
  return debug_buff;
}

void process_delayed_letters (int all, int skip) {
  while (letter_get (0) != NULL) {
  }

  int en = GET_DELAYED_ID(now + 1 + all * MAX_DELAY);

  while (last_process_delayed_time != en) {
    temp_letter *cur_letter = &delayed_head_letter[last_process_delayed_time];
    if (cur_letter->next != cur_letter) {
      cur_letter = cur_letter->next;

      int len = strlen (cur_letter->l->data);
      assert (len < cur_letter->l->data_len);
      memcpy (letter_buf->data, cur_letter->l->data, len + 1);

      assert (get_fields (letter_buf->data) >= 4);

      int real_priority = skip ? 0 : cur_letter->real_priority;
      long long task_id = cur_letter->task_id;
      int time_received = cur_letter->time_received;

      delete_letter (cur_letter->id);
      //DO NOT USE LETTER AFTER DELETE

      if (!is_deleted_task (task_id, time_received)) {
        letter_add (real_priority, 0);
      }
    } else {
      if (++last_process_delayed_time == DELAYED_TABLE_SIZE) {
        last_process_delayed_time = 0;
      }
    }
  }
  if (--last_process_delayed_time == -1) {
    last_process_delayed_time = DELAYED_TABLE_SIZE - 1;
  }
}

void letter_delete_time (int gap, const char *error) {
  while (head_letter.next != &head_letter && head_letter.next->time_sent <= now - gap) {
    add_letter_priority (head_letter.next->id, head_letter.next->priority, 0, error);
    expired_letters++;
  }

  if (gap < 0) {
    process_delayed_letters (1, 1);
  }
}

long long letters_clear (int priority) {
  if (priority == 0) {
    long long result = get_drive_buffer_size (priority);
    process_delayed_letters (1, 0);
    return result;
  }

  long long result = get_drive_buffer_size (priority);
  one_header *data = &header.data[priority];

  temp_letter *cur_letter = &head_letter;
  while (cur_letter->next != &head_letter) {
    if (cur_letter->next->priority == priority) {
      delete_letter (cur_letter->next->id);
      expired_letters++;
    } else {
      cur_letter = cur_letter->next;
    }
  }

  data->drive_l = data->drive_r;
  data->memory_l = data->drive_l;
  data->memory_r = (data->drive_r + sizeof (long long)) % data->drive_old_mx;
  data->memory_buf_l = 0;
  data->memory_buf_r = sizeof (long long);
  *((long long *)memory_buf[priority]) = FILE_END;

  assert (lseek (fd[2 + priority], data->drive_r, SEEK_SET) == data->drive_r);
  assert (*(long long *)(memory_buf[priority] + data->memory_buf_l) == FILE_END);

  return result;
}

int load_index (char *index_name) {
  if (index_name == NULL || dl_open_file (0, index_name, -1) < 0) {
    header.magic = LETTERS_INDEX_MAGIC;
    header.created_at = time (NULL);

    int i;
    for (i = 0; i < MAX_PRIORITY; i++) {
      header.data[i].drive_l = header.data[i].drive_r = header.data[i].drive_old_mx = 0;
    }
    return 0;
  }

  assert (lseek (fd[0], 0, SEEK_SET) == 0);

  int size = sizeof (index_header);
  int r = read (fd[0], &header, size);
  if (r != size) {
    fprintf (stderr, "error reading header from index file: read %d bytes instead of %d: %m\n", r, size);
  }
  assert (r == size);

  if (header.magic != LETTERS_INDEX_MAGIC) {
    fprintf (stderr, "bad letters index file header\n");
    assert (0);
  }

  if (verbosity > 1) {
    fprintf (stderr, "header loaded\n");
  }

  dl_close_file (0);
  return 1;
}

int save_index (void) {
  header.magic = LETTERS_INDEX_MAGIC;
  header.created_at = time (NULL);

  assert (lseek (fd[1], 0, SEEK_SET) == 0);
  assert (write (fd[1], &header, sizeof (header)) == sizeof (header));

  return 1;
}

struct aiocb sync_aiocb[MAX_PRIORITY + 1];
int sync_active[MAX_PRIORITY + 1], sync_last[MAX_PRIORITY + 1];

void flush_all (int force) {
  save_index();

  double tm = -get_utime (CLOCK_MONOTONIC);
  int i;
  int res;
  int was_error = 0;

  for (i = 0; i <= MAX_PRIORITY; i++) {
    if (sync_active[i]) {
      res = aio_error (&sync_aiocb[i]);
      if (res == EINPROGRESS && now > sync_active[i] + 120) {
        fprintf (stderr, "%d aio_sync didn't complete for 120 seconds, invoking fsync()!\n", now);
        aio_cancel (sync_aiocb[i].aio_fildes, &sync_aiocb[i]);
        fsync (sync_aiocb[i].aio_fildes);
        was_error = 1;
      }
      if (res != EINPROGRESS) {
        sync_active[i] = 0;
        res = aio_return (&sync_aiocb[i]);
        if (res >= 0) {
          sync_last[i] = now;
          sync_aiocb[i].aio_fildes = -1;
          if (verbosity > 1) {
            fprintf (stderr, "%d aio_fsync() completed successfully\n", now);
          }
        } else {
          fprintf (stderr, "%d error syncing %s: %m\n", now, fnames[i + 1]);
          fsync (sync_aiocb[i].aio_fildes);
          was_error = 1;
        }
      }
    }

    if (!sync_active[i] && !force) {
      memset (&sync_aiocb[i], 0, sizeof (struct aiocb));
      sync_aiocb[i].aio_fildes = fd[i + 1];
      if (aio_fsync (O_DSYNC, &sync_aiocb[i]) < 0) {
        fprintf (stderr, "%d aio_fsync() for %s failed: %m\n", now, fnames[i + 1]);
        fsync (fd[i + 1]);
        was_error = 1;
      }
      sync_active[i] = now;
      if (i > 0) {
        letter_stat[i - 1][5]++;
      }
      if (verbosity > 1 || log_drive) {
        kprintf ("%d queued aio_fsync() for %s\n", now, fnames[i + 1]);
      }
    }
    if (force) {
      if (fsync (fd[i + 1]) < 0) {
        fprintf (stderr, "%d error syncing %s: %m", now, fnames[i + 1]);
      } else {
        sync_last[i] = now;
        if (verbosity > 1) {
          fprintf (stderr, "%d sync ok for %s\n", now, fnames[i + 1]);
        }
      }
    }
//    assert (fsync (fd[i + 1]) >= 0);
  }

  if (was_error) {
    was_error = 0;
    for (i = 0; i <= MAX_PRIORITY; i++) {
      if (fsync (fd[i + 1]) < 0) {
        fprintf (stderr, "%d error resyncing %s: %m", now, fnames[i + 1]);
        was_error = 1;
      }
    }
    if (was_error) {
      exit (3);
    }
  }

  tm += get_utime (CLOCK_MONOTONIC);
  if (tm >= 0.1) {
    fprintf (stderr, "Warning. fsync time = %.9lf at time %d\n", tm, now);
  }
}

int init_all (char *index_name, long long size[MAX_PRIORITY]) {
  int i;
  map_ll_vptr_init (&id_to_letter);
  for (i = 0; i < MAX_PRIORITY; i++) {
    map_ll_vptr_init (&drive_l_to_letter[i]);
  }

  task_deletes_begin = 0;
  vector_init (task_deletes);
  map_ll_int_init (&task_id_to_delete_time);

  int f = load_index (index_name);

  long long total_size = 0;
  for (i = 1; i < MAX_PRIORITY; i++) {
    if (size[i] < 16 * MAX_LETTER_SIZE) {
      size[i] = 16 * MAX_LETTER_SIZE;
    }
    total_size += size[i];
  }
  size[0] = max (max (header.data[0].drive_mx, 16 * MAX_LETTER_SIZE), min (total_size, max_memory) / 4);
  total_size += size[0];
  for (i = 0; i < MAX_PRIORITY; i++) {
    header.data[i].drive_mx = size[i] / 16 * 16;
    header.data[i].memory_buf_l = 0;
    header.data[i].memory_buf_r = 8;
    header.data[i].memory_buf_mx = (long long)(max_memory / 2.0 * size[i] / total_size) / 16 * 16;
    if (header.data[i].memory_buf_mx < 8 * MAX_LETTER_SIZE) {
      header.data[i].memory_buf_mx = 8 * MAX_LETTER_SIZE;
    }
    memory_buf[i] = dl_malloc (header.data[i].memory_buf_mx);
    *((long long *)memory_buf[i]) = FILE_END;
  }

  temp_letter_init (&head_letter);
  for (i = 0; i < DELAYED_TABLE_SIZE; i++) {
    temp_letter_init (&delayed_head_letter[i]);
  }
  last_process_delayed_time = GET_DELAYED_ID (now);

  letter_buf = dl_malloc (2 * MAX_LETTER_SIZE + 1100);
  letter_buf_get = dl_malloc (2 * MAX_LETTER_SIZE + 1100);
  letter_buf->magic = LETTER_BEGIN;

  dl_open_file (1, index_name, 1);

  for (i = 0; i < MAX_PRIORITY; i++) {
    one_header *data = &(header.data[i]);
    sprintf (debug_buff, "%s%d", index_name, i);
    dl_open_file (2 + i, debug_buff, 1);

    if (fsize[2 + i] < data->drive_old_mx) {
      data->drive_old_mx = fsize[2 + i];
    }

    if (data->drive_old_mx == 0) {
      long long tmp = FILE_END;
      assert (write (fd[2 + i], (char *)&tmp, sizeof (long long)) == sizeof (long long));
      tmp = 0;
      assert (write (fd[2 + i], (char *)&tmp, sizeof (long long)) == sizeof (long long));
      data->drive_old_mx = 2 * sizeof (long long);
    }

    if (data->drive_r >= data->drive_old_mx || data->drive_l >= data->drive_old_mx) {
      data->drive_r = data->drive_l = 0;
    }

    assert (lseek (fd[2 + i], data->drive_r, SEEK_SET) == data->drive_r);

    long long new_drive_r = data->drive_r, to_read, new_drive_l = data->drive_l;
    long long j, total_r = 0, total_l = 0;
    do {
      to_read = min (data->drive_old_mx - new_drive_r, data->memory_buf_mx);
      if (to_read % sizeof (long long) != 0) {
        fprintf (stderr, "Assert will fail: drive_old_mx = %lld, new_drive_r = %lld, memory_buf_mx = %lld\n", data->drive_old_mx, new_drive_r, data->memory_buf_mx);
      }
      assert (to_read % sizeof (long long) == 0);
      assert (read (fd[2 + i], memory_buf[i], to_read) == to_read);
      for (j = 0; j < to_read && *(long long *)(memory_buf[i] + j) != FILE_END; j += 8) ;

      assert (new_drive_r >= data->drive_r || new_drive_r + j < data->drive_r);
      assert (data->drive_r != 0 || new_drive_r + j < data->drive_old_mx);

      new_drive_r = (new_drive_r + j) % data->drive_old_mx;
      if (new_drive_r == 0) {
        assert (lseek (fd[2 + i], 0, SEEK_SET) == 0);
      }
      total_r += j;
    } while (j == to_read);

    if ((data->drive_l <= new_drive_r + (long long)sizeof (long long) && new_drive_r < data->drive_r) ||
        (data->drive_l > data->drive_r && data->drive_l <= data->drive_r + total_r + (long long)sizeof (long long))) {
      new_drive_l = (new_drive_r + 2 * sizeof (long long)) % data->drive_old_mx;
      assert (lseek (fd[2 + i], new_drive_l, SEEK_SET) == new_drive_l);
      do {
        to_read = min (data->drive_old_mx - new_drive_l, data->memory_buf_mx);
        if (to_read % sizeof (long long) != 0) {
          fprintf (stderr, "Assert will fail 2: drive_old_mx = %lld, new_drive_l = %lld, memory_buf_mx = %lld\n", data->drive_old_mx, new_drive_l, data->memory_buf_mx);
        }
        assert (to_read % sizeof (long long) == 0);
        assert (read (fd[2 + i], memory_buf[i], to_read) == to_read);
        for (j = 0; j < to_read && *(long long *)(memory_buf[i] + j) != LETTER_BEGIN; j += 8) ;

        new_drive_l = (new_drive_l + j) % data->drive_old_mx;
        if (new_drive_l == 0) {
          assert (lseek (fd[2 + i], 0, SEEK_SET) == 0);
        }

        total_l += j;
        assert (total_l < data->drive_old_mx);
      } while (j == to_read);
    }

    if (total_r) {
      fprintf (stderr, "Warning!!! Buffer %d was broken. %10lld bytes was restored, %10lld bytes was skipped. Old drive_r = %12lld, new drive_r = %12lld, old drive_l = %12lld, new drive_l = %12lld.\n", i, total_r, total_l, data->drive_r, new_drive_r, data->drive_l, new_drive_l);
    }

    data->drive_l = new_drive_l;
    data->drive_r = new_drive_r;
    assert (lseek (fd[2 + i], data->drive_r, SEEK_SET) == data->drive_r);

    header.data[i].memory_l = header.data[i].drive_l;
    header.data[i].memory_r = header.data[i].drive_l + sizeof (long long);
  }

  return f;
}

int get_sync_delay (void) {
  int i;
  int res = 0;

  for (i = 0; i <= MAX_PRIORITY; i++) {
    if (now - sync_last[i] > res) {
      res = now - sync_last[i];
    }
  }
  return res;
}

void free_all (void) {
  //TODO truncate files
  letter_delete_time (-1, "Engine restart");

  flush_all (1);

  int i;
  dl_close_file (1);
  for (i = 0; i < MAX_PRIORITY; i++) {
    dl_close_file (2 + i);
  }

  if (verbosity > 0) {
    map_ll_vptr_free (&id_to_letter);
    for (i = 0; i < MAX_PRIORITY; i++) {
      map_ll_vptr_free (&drive_l_to_letter[i]);
    }

    vector_free (task_deletes);
    map_ll_int_free (&task_id_to_delete_time);

    for (i = 0; i < MAX_PRIORITY; i++) {
      dl_free (memory_buf[i], header.data[i].memory_buf_mx);
    }

    dl_free (letter_buf, 2 * MAX_LETTER_SIZE + 1100);
    dl_free (letter_buf_get, 2 * MAX_LETTER_SIZE + 1100);
    fprintf (stderr, "Memory left: %lld\n", dl_get_memory_used());
//    while (1) {}
    assert (dl_get_memory_used() == 0);
  }
}
