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
#define _FILE_OFFSET_BITS 64

#include "mf-data.h"

long long del_by_LRU;

char *suggname, *newindexname;

int jump_log_ts;
long long jump_log_pos;
unsigned int jump_log_crc32;

int index_mode, dump_mode, binlog_readed;
long max_memory = MAX_MEMORY;
int cur_users;
int index_users, header_size;
long long allocated_metafile_bytes;
int all_sugg_cnt;


void bind_user_metafile (user *u);
void unbind_user_metafile (user *u);

index_header header;
user *users, *LRU_head;
lookup_table user_table;
void load_user_metafile (user *u, int local_id, int no_aio);

int user_loaded (user *u) {
  return u->metafile_len >= 0 && u->aio == NULL;
}

int local_uid (int user_id) {
  if (user_id <= 0) {
    return -1;
  }
  if (user_id % log_split_mod != log_split_min) {
    return -1;
  }

  return ltbl_add (&user_table, user_id);
}

user *conv_uid (int user_id) {
  int local_id = local_uid (user_id);
  if (local_id == -1) {
    return NULL;
  }

  assert (local_id < user_table.to.size);
  return &users[local_id];
}

void dump_lru (char *s, int x) {
  fprintf (stderr, "%s", s);
  user *u = LRU_head->next_used;
  int f = 0;
  while (u != LRU_head) {
    fprintf (stderr, "%d ", ltbl_get_rev (&user_table, (int)(u - users)));
    int y = ltbl_get_rev (&user_table, (int)(u - users));
    assert (y != x);
    u = u->next_used;
    f |= x == -y;
  }
  if (x < 0) {
    assert (f);
  }
  fprintf (stderr, "\n");
}

void del_user_used (user *u) {
  if (verbosity > 3) {
    char tmp[100];
    sprintf (tmp, "?(-%d)", ltbl_get_rev (&user_table, (int)(u - users)));
    dump_lru (tmp, -ltbl_get_rev (&user_table, (int)(u - users)));
  }
  assert (u->prev_used != NULL);
  assert (u->next_used != NULL);

  u->next_used->prev_used = u->prev_used;
  u->prev_used->next_used = u->next_used;

  u->prev_used = NULL;
  u->next_used = NULL;
  if (verbosity > 3) {
    dump_lru ("!", 0);
  }
}

void add_user_used (user *u) {
  if (verbosity > 3) {
    char tmp[100];
    sprintf (tmp, "?(+%d)", ltbl_get_rev (&user_table, (int)(u - users)));
    dump_lru (tmp, ltbl_get_rev (&user_table, (int)(u - users)));
  }

  assert (u != NULL);
  assert (user_loaded (u));

  assert (u->prev_used == NULL);
  assert (u->next_used == NULL);

  user *y = LRU_head->prev_used;

  u->next_used = LRU_head;
  LRU_head->prev_used = u;

  u->prev_used = y;
  y->next_used = u;
  if (verbosity > 3) {
    dump_lru ("!", 0);
  }
}


int unload_user_metafile (user *u);

void user_unload (user *u) {
  assert (u != NULL);

  if (!user_loaded (u)) {
    fprintf (stderr, "%d\n", ltbl_get_rev (&user_table, (int)(u - users)));
    assert (0);
    return;
  }

  del_user_used (u);
  unload_user_metafile (u);
  cur_users--;
  assert (!user_loaded (u));
}

int user_LRU_unload (void) {
  user *u = LRU_head->next_used;
  if (u == LRU_head) {
    return -1;
  }

  del_by_LRU++;
  user_unload (u);
  return 0;
}

long long get_del_by_LRU (void) {
  return del_by_LRU;
}

int add_common_friends (int uid, int add, int *a, int an) {
  user *u = conv_uid (uid);

  if (u == NULL) {
    return 0;
  }

  int i;
  for (i = 0; i < an; i++) {
    if (rand() % an < 300) {
      trp_incr (&u->sugg, a[i], add);
    }
  }

  return 1;
}

int add_exception (struct lev_mf_add_exception *E) {
  user *u = conv_uid (E->user_id);

  if (u == NULL) {
    return 0;
  }

  if (1 <= E->friend_id && E->friend_id < 500000000) {
    chg_add (&u->new_exceptions, 2 * E->friend_id + 1);
  } else {
//    fprintf (stderr, "WARNING: add exceptions %d %d\n", E->user_id, E->friend_id);
    expired_aio_queries += 100;
  }

  return 1;
}

int do_add_exception (int uid, int fid) {
  struct lev_mf_add_exception *E =
    alloc_log_event (LEV_MF_ADD_EXCEPTION, sizeof (struct lev_mf_add_exception), 0);

  E->user_id = uid;
  E->friend_id = fid;

  return add_exception (E);
}

int del_exception (struct lev_mf_del_exception *E) {
  user *u = conv_uid (E->user_id);

  if (u == NULL) {
    return 0;
  }

  if (1 <= E->friend_id && E->friend_id < 500000000) {
    chg_del (&u->new_exceptions, 2 * E->friend_id + 1);
  } else {
//    fprintf (stderr, "WARNING: del exceptions %d %d\n", E->user_id, E->friend_id);
    expired_aio_queries += 100;
  }

  return 1;
}

int do_del_exception (int uid, int fid) {
  struct lev_mf_del_exception *E =
    alloc_log_event (LEV_MF_DEL_EXCEPTION, sizeof (struct lev_mf_del_exception), 0);

  E->user_id = uid;
  E->friend_id = fid;

  return del_exception (E);
}


int user_has_exception (user *u, int val) {
  int i;

  if ( (i = chg_has (u->new_exceptions, val)) ) {
      return i > 0;
  }

  int *a = (int *)u->metafile;
  if (a == NULL) {
    return 0;
  }

  const int int_size = (int)sizeof (int);
  assert (u->metafile_len >= int_size);
  int n = a[0];

  assert (u->metafile_len >= int_size * (1 + n));
  assert (u->metafile_len <= int_size * (1 + n));
  if (n <= 5) {
    for (i = 1; i <= n; i++) {
      if (a[i] == val) {
        return 1;
      }
    }
    return 0;
  } else {
    int l = 1, r = n + 1, c;
    while (l + 1 < r) {
      c = (l + r) / 2;
      if (a[c] >= val) {
        l = c;
      } else {
        r = c;
      }
    }
    return a[l] == val;
  }
}


#define MAX_HEAP (MAX_CNT * 2 + 10 + MAX_EXCEPTIONS)

trp_node *heap[MAX_HEAP];
int heap_size;

void heap_fix_down (void) {
  int j, k, t;
  trp_node *tmp;
  k = 1;
  while (1) {
    j = k;
    t = j * 2;
    if (t <= heap_size && heap[t]->y > heap[k]->y) {
      k = t;
    }
    t++;
    if (t <= heap_size && heap[t]->y > heap[k]->y) {
      k = t;
    }
    if (k != j) {
      tmp = heap[k], heap[k] = heap[j], heap[j] = tmp;
    } else {
      break;
    }
  }
}

void heap_fix_up (int j) {
  trp_node *tmp;
  int k = 0;

  while (j > 1 && heap[j]->y > heap[k = j / 2]->y) {
    tmp = heap[j], heap[j] = heap[k], heap[k] = tmp;
    j = k;
  }
}

void heap_add (trp_node *v) {
  if (heap_size + 1 >= MAX_HEAP) {
    active_aio_queries |= (1 << 20);
  }

  if (v != NULL && heap_size + 1 < MAX_HEAP) {
    heap[++heap_size] = v;
    heap_fix_up (heap_size);
  }
}

trp_node *heap_get (void) {
  assert (heap_size > 0);

  trp_node *tmp = heap[1];
  heap[1] = heap[heap_size--];
  heap_fix_down();

  return tmp;
}

void heap_init (void) {
  heap_size = 0;
}



void test_user_unload (int uid) {
  if (verbosity > 2) {
    fprintf (stderr, "test user unload %d\n", uid);
  }

  user *u = conv_uid (uid);
  if (u == NULL) {
    return;
  }

  if (user_loaded (u)) {
    del_user_used (u);
    unbind_user_metafile (u);
    cur_users--;
  }

  assert (!user_loaded (u));
}


int get_suggestions (int uid, int mx_cnt, int min_common, int *res) {
  user *u = conv_uid (uid);
  int local_id = local_uid (uid);

  if (mx_cnt > MAX_CNT) {
    mx_cnt = MAX_CNT;
  }

  if (u == NULL) {
    res[0] = 0;
    return 0;
  }

  min_common *= 4;
  if (min_common < 4) {
    min_common = 4;
  }

  load_user_metafile (u, local_id, NOAIO);

//  fprintf (stderr, "after load_user_metafile. u->metafile_len = %d\n", u->metafile_len);

  if (!user_loaded (u)) {
    return -2;
  }

  del_user_used (u);
  add_user_used (u);

  heap_init ();
  int n = 0;

  heap_add (u->sugg.root);
  while (heap_size && mx_cnt) {
    trp_node *v = heap_get();

    if ((v->y >> 16) < min_common) {
      //TODO: comment for testing
      break;
    }

    if (!user_has_exception (u, v->x)) {
      res[n * 2 + 1] = v->x;
      res[n * 2 + 2] = v->y >> 16;
      n++;

      mx_cnt--;
    }
    heap_add (v->l);
    heap_add (v->r);
  }

  res[0] = n;

  return 1;
}

int clear_exceptions (struct lev_mf_clear_exceptions *E) {
  user *u = conv_uid (E->user_id);
  int local_id = local_uid (E->user_id);

  if (u == NULL) {
    return 0;
  }

  load_user_metafile (u, local_id, NOAIO || index_mode || !binlog_readed);

  if (!user_loaded (u)) {
    return -2;
  }

  del_user_used (u);
  add_user_used (u);

  if (u->metafile != NULL) {
    assert (1 <= local_id && local_id <= header.user_cnt);

    qfree (u->metafile, u->metafile_len);
    u->metafile = NULL;
    u->metafile_len = 0;
    header.user_index[local_id].size = 0;
  }
  chg_free (&u->new_exceptions);
  return 1;
}

int do_clear_exceptions (int uid) {
  struct lev_mf_clear_exceptions *E =
    alloc_log_event (LEV_MF_CLEAR_EXCEPTIONS, sizeof (struct lev_mf_clear_exceptions), 0);

  E->user_id = uid;

  return clear_exceptions (E);
}


// BINLOG

int mf_replay_logevent (struct lev_generic *E, int size);

int init_mf_data (int schema) {
  replay_logevent = mf_replay_logevent;

  return 0;
}

void try_init_local_uid (void) {
  static int was = 0;
  static int old_log_split_min, old_log_split_max, old_log_split_mod;

  if (was) {
//    fprintf (stderr, "%d vs %d | %d vs %d | %d vs %d\n", old_log_split_min, log_split_min, old_log_split_max, log_split_max, old_log_split_mod, log_split_mod);
    assert (old_log_split_min == log_split_min && old_log_split_max == log_split_max && old_log_split_mod == log_split_mod);
    return;
  }

  int i;
  for (i = 1; i <= header.user_cnt; i++) {
    local_uid (header.user_index[i].id);
  }

  was = 1;
  old_log_split_min = log_split_min;
  old_log_split_max = log_split_max;
  old_log_split_mod = log_split_mod;

  log_schema = MF_SCHEMA_V1;
  init_mf_data (log_schema);
}

static int mf_le_start (struct lev_start *E) {
  if (E->schema_id != (int)MF_SCHEMA_V1) {
    return -1;
  }

  log_split_min = E->split_min;
  log_split_max = E->split_max;
  log_split_mod = E->split_mod;
  assert (log_split_mod > 0 && log_split_min >= 0 && log_split_min + 1 == log_split_max && log_split_max <= log_split_mod);

  try_init_local_uid();

  return 0;
}

int _eventsLeft = 0;

int mf_replay_logevent (struct lev_generic *E, int size) {
//  fprintf (stderr, "mf_replay_logevent %lld\n", log_cur_pos());
//  fprintf (stderr, "%x\n", E->type);
  if (index_mode) {
    if ((_eventsLeft && --_eventsLeft == 0) || get_memory_used() > MEMORY_CHANGES_PERCENT * max_memory) {
      save_index (newindexname);
      exit (13);
    }
  }

  int s;
  switch (E->type) {
  case LEV_START:
    if (size < 24 || E->b < 0 || E->b > 4096) { return -2; }
    s = 24 + ((E->b + 3) & -4);
    if (size < s) { return -2; }
    return mf_le_start ((struct lev_start *) E) >= 0 ? s : -1;
  case LEV_NOOP:
  case LEV_TIMESTAMP:
  case LEV_CRC32:
  case LEV_ROTATE_FROM:
  case LEV_ROTATE_TO:
  case LEV_TAG:
    return default_replay_logevent (E, size);
  case LEV_MF_ADD_EXCEPTION:
    STANDARD_LOG_EVENT_HANDLER(lev_mf, add_exception);
  case LEV_MF_DEL_EXCEPTION:
    STANDARD_LOG_EVENT_HANDLER(lev_mf, del_exception);
  case LEV_MF_CLEAR_EXCEPTIONS:
    STANDARD_LOG_EVENT_HANDLER(lev_mf, clear_exceptions);
  }

  fprintf (stderr, "unknown log event type %08x at position %lld\n", E->type, log_cur_pos());

  return -1;
}


/*
 *
 *         MUTUAL FRIENDS INDEX
 *
 */
struct aio_connection *WaitAio;
int onload_user_metafile (struct connection *c, int read_bytes);

conn_type_t ct_metafile_aio = {
  .magic = CONN_FUNC_MAGIC,
  .wakeup_aio = onload_user_metafile
};

conn_query_type_t aio_metafile_query_type = {
.magic = CQUERY_FUNC_MAGIC,
.title = "mf-data-aio-metafile-query",
.wakeup = aio_query_timeout,
.close = delete_aio_query,
.complete = delete_aio_query
};


void load_user_metafile (user *u, int local_id, int no_aio) {
  static struct aio_connection empty_aio_conn;

  WaitAio = NULL;

  if (user_loaded (u)) {
    return;
  }

  if (local_id > header.user_cnt || header.user_index[local_id].size == 0) {
    u->metafile = NULL;
    u->metafile_len = 0;

    empty_aio_conn.extra = u;
    empty_aio_conn.basic_type = ct_aio;
    u->aio = &empty_aio_conn;

    onload_user_metafile ((struct connection *)(&empty_aio_conn), u->metafile_len);
    return;
  }

  if (u->aio != NULL) {
    check_aio_completion (u->aio);
    if (u->aio != NULL) {
      WaitAio = u->aio;
      return;
    }

    if (u->metafile) {
      return;
    } else {
      fprintf (stderr, "Previous AIO query failed for user at %p, scheduling a new one\n", u);
    }
  }

  u->metafile_len = header.user_index[local_id].size;
  u->metafile = NULL;

  while (1) {
    u->metafile = qmalloc (u->metafile_len);
    if (u->metafile != NULL) {
      break;
    }

    fprintf (stderr, "no space to load metafile - cannot allocate %d bytes (%lld currently used)\n", u->metafile_len, allocated_metafile_bytes);
  }

  allocated_metafile_bytes += u->metafile_len;

  if (verbosity > 2) {
    fprintf (stderr, "*** Scheduled reading user data from index at position %lld, %d bytes, noaio = %d\n", header.user_index[local_id].shift, u->metafile_len, no_aio);
  }

  assert (1 <= local_id && local_id <= header.user_cnt);
  if (no_aio) {
    double disk_time = -get_utime (1);

    assert (lseek (fd[0], header.user_index[local_id].shift, SEEK_SET) == header.user_index[local_id].shift);
    int size = header.user_index[local_id].size;
    int r = read (fd[0], u->metafile, size);
    if (r != size) {
      fprintf (stderr, "error reading header from index file: read %d bytes instead of %d at position %lld: %m\n", r, size, header.user_index[local_id].shift);
    }
    assert (r == size);

    disk_time += get_utime (1);
    if (verbosity > 2) {
      fprintf (stderr, "  disk time = %.6lf\n", disk_time);
    }

    empty_aio_conn.extra = u;
    empty_aio_conn.basic_type = ct_aio;
    u->aio = &empty_aio_conn;

    assert (u->aio != NULL);

    onload_user_metafile ((struct connection *)(&empty_aio_conn), u->metafile_len);

    return;
  } else {
    u->aio = create_aio_read_connection (fd[0], u->metafile, header.user_index[local_id].shift, u->metafile_len, &ct_metafile_aio, u);
    assert (u->aio != NULL);
    WaitAio = u->aio;
  }

  return;
}

int onload_user_metafile (struct connection *c, int read_bytes) {
  if (verbosity > 2) {
    fprintf (stderr, "onload_user_metafile(%p,%d)\n", c, read_bytes);
  }

  struct aio_connection *a = (struct aio_connection *)c;
  user *u = (user *) a->extra;

  assert (a->basic_type == ct_aio);
  assert (u != NULL);

  if (u->aio != a) {
    fprintf (stderr, "assertion (u->aio == a) will fail\n");
    fprintf (stderr, "%p != %p\n", u->aio, a);
  }

  assert (u->aio == a);

  if (read_bytes != u->metafile_len) {
    if (verbosity > 0) {
      fprintf (stderr, "ERROR reading user: read %d bytes out of %d: %m\n", read_bytes, u->metafile_len);
    }

    qfree (u->metafile, u->metafile_len);
    allocated_metafile_bytes -= u->metafile_len;
    u->metafile = NULL;
    u->metafile_len = -1;
    u->aio = NULL;
    return 0;
  }
  assert (read_bytes == u->metafile_len);

  if (verbosity > 2) {
    fprintf (stderr, "*** Read user: read %d bytes\n", read_bytes);
  }

  u->aio = NULL;

  bind_user_metafile (u);

  add_user_used (u);
  cur_users++;

  while (allocated_metafile_bytes > max_memory * MEMORY_USER_PERCENT) {
    if (user_LRU_unload() == -1) {
      exit (1);
    }
  }

  return 1;
}

int unload_user_metafile (user *u) {
  assert (u != NULL);

  int user_id = 0;

  if (verbosity > 1) {
    user_id = ltbl_get_rev (&user_table, (int)(u - users));
  }

  if (verbosity > 2) {
    fprintf (stderr, "unload_user_metafile(%d)\n", user_id);
  }

  if (!u || !user_loaded (u)) {
    if (verbosity > 1) {
      fprintf (stderr, "cannot unload user metafile (%d)\n", user_id);
      assert (0);
    }
    return 0;
  }

  allocated_metafile_bytes -= u->metafile_len;

  unbind_user_metafile (u);

  if (verbosity > 2) {
    fprintf (stderr, "unload_user_metafile(%d) END\n", user_id);
  }

  return 1;
}


void bind_user_metafile (user *u) {
  if (verbosity > 2) {
    fprintf (stderr, "bind user metafile local id = %d (%p)\n", (int)(u - users), u);
  }
  int local_id = (int)(u - users); // magic. sorry.

  if (u->metafile == NULL) {
    return;
  }

  assert (1 <= local_id && local_id <= header.user_cnt);

  assert (u->metafile_len >= (int)sizeof (int));
  int exc_size = (((int *)u->metafile)[0] + 1) * sizeof (int),
      sug_size = u->metafile_len - exc_size;
  assert (sug_size >= 0);

  if (sug_size && !index_mode) {
    assert (sug_size % (2 * sizeof (int)) == sizeof (int));

    int *sugg = (int *)(u->metafile + exc_size), n = sugg[0];
    //fprintf (stderr, "n = %d, sug_size = %d\n", n, sug_size);
    //assert (sug_size == sizeof (int) * (n * 2 + 1));
    n = (sug_size / sizeof (int) - 1) / 2;
    all_sugg_cnt += n;

    if (n > MAX_SUGGESTIONS) {
      n = MAX_SUGGESTIONS;
    }

    int i;
    for (i = 0; i < n; i++) {
      assert (1 <= sugg[2 * i + 2] && sugg[2 * i + 2] < 500000000);
      assert (1 <= sugg[2 * i + 1] && sugg[2 * i + 1] < 100000);

      trp_incr (&u->sugg, sugg[2 * i + 2], sugg[2 * i + 1]);
    }
    qrealloc (u->metafile, exc_size, u->metafile_len);
    u->metafile_len = exc_size;
    header.user_index[local_id].size = exc_size;
    allocated_metafile_bytes -= sug_size;
  }
}

void unbind_user_metafile (user *u) {
  if (u == NULL) {
    return;
  }

  if (verbosity > 2) {
    fprintf (stderr, "unbind_user_metafile\n");
  }

  if (u->metafile != NULL) {
    qfree (u->metafile, u->metafile_len);
  }

  u->metafile = NULL;
  u->metafile_len = -1;
}

long long get_index_header_size (index_header *header) {
  return sizeof (index_header) + sizeof (user_index_data) * (header->user_cnt + 1);
}

int load_header (char *indexname) {
  if (open_file (0, indexname, -1) == -1) {

    header.user_cnt = 0;
    header.user_index = NULL;

    header.log_pos0 = 0;
    header.log_pos1 = 0;
    header.log_timestamp = 0;
    header.log_split_min = 0;
    header.log_split_max = 1000000000;
    header.log_split_mod = 1;
    header.log_pos0_crc32 = 0;
    header.log_pos1_crc32 = 0;

    header.created_at = time (NULL);
    header_size = sizeof (index_header);

    return 0;
  }

  // read header
  assert (lseek (fd[0], 0, SEEK_SET) == 0);

  int size = sizeof (index_header) - sizeof (long);
  int r = read (fd[0], &header, size);
  if (r != size) {
    fprintf (stderr, "error reading header from index file: read %d bytes instead of %d at position 0: %m\n", r, size);
  }
  assert (r == size);

  size = sizeof (user_index_data) * (header.user_cnt + 1);
  header.user_index = qmalloc0 (size);

  r = read (fd[0], header.user_index, size);
  if (r != size) {
    fprintf (stderr, "error reading header from index file: read %d bytes instead of %d: %m\n", r, size);
  }
  assert (r == size);

  assert (header.log_split_max);
  log_split_min = header.log_split_min;
  log_split_max = header.log_split_max;
  log_split_mod = header.log_split_mod;

  header_size = get_index_header_size (&header) - sizeof (long);
  if (verbosity > 1) {
    fprintf (stderr, "header loaded %d %d %d\n", log_split_min, log_split_max, log_split_mod);
    fprintf (stderr, "ok\n");
  }
  return 1;
}

void free_header (index_header *header) {
  if (header->user_index != NULL) {
    qfree (header->user_index, sizeof (user_index_data) * (header->user_cnt + 1));
  }
}


int cmp_xx (const void * _a, const void * _b)
{
  int a = *(int *)_a, b = *(int *)_b;
  if (a < b) {
    return -1;
  } else if (a > b) {
    return +1;
  }
  return 0;
}


void save_dump (char *indexname) {
  open_file (1, indexname, 2);

  int user_cnt = user_table.currId - 1;

  int *buff = qmalloc (sizeof (int) * (MAX_EXCEPTIONS * 2 + 20)),
      *ids = qmalloc (sizeof (int) * (user_cnt * 2 + 20));

  int i;
  int idn = 0;

  for (i = 1; i <= user_cnt; i++) {
    ids[idn++] = ltbl_get_rev (&user_table, i);
    ids[idn++] = i;
  }

  qsort (ids, idn >> 1, sizeof (int) * 2, cmp_xx);

  static data_iterator it;

  for (i = 0; i < idn; i += 2) {
    int lid = ids[i + 1], id = ids[i];

    user *u = conv_uid (id);
    assert (u == users + lid);

    load_user_metafile (u, lid, 1);
    assert (u->metafile_len != -1);

    if (verbosity > 1) {
      fprintf (stderr, "user_id = %d (%d)\n", id, lid);
    }

    int inf = (1 << 30) - 2;
    data_iter_init (&it, (unsigned char *)u->metafile, u->new_exceptions, inf, u->metafile_len, 0, inf);

    int cnt = 0, val;

    while (cnt + 1 < 2 * MAX_EXCEPTIONS && (val = data_iter_val_and_next (&it)) ) {
      buff[cnt++] = id;
      buff[cnt++] = val;
    }

    write (fd[1], buff, sizeof (int) * cnt);
    assert (user_LRU_unload () != -1);
  }

  qfree (buff, sizeof (int) * (MAX_EXCEPTIONS * 2 + 20)),
  qfree (ids, sizeof (int) * (user_cnt * 2 + 20));

  assert (close (fd[1]) >= 0);
  fd[1] = -1;
}

void save_index (char *indexname) {
  if (dump_mode) {
    return save_dump (indexname);
  }

  long long *sugg_shift = NULL;
  int *sugg_size = NULL;
  int tsz = 0;
  if (suggname != NULL) {
    tsz = user_table.size * 2;
    sugg_shift = qmalloc0 (sizeof (long long) * tsz);
    sugg_size = qmalloc0 (sizeof (int) * tsz);

    assert (open_file (3, suggname, -1) >= 0);
  #define MAX_INIT 30000
    static int buff[MAX_INIT];

    int id, n;
    user *u;

    long long sz, curr = 0;
    assert (read (fd[3], &sz, sizeof (long long)) == sizeof (long long));
    curr += sizeof (long long);

    assert (sz == fsize[3]);

    while (read (fd[3], &id, sizeof (int)) == sizeof (int)) {
      curr += sizeof (int);

      assert (read (fd[3], &n, sizeof (int)) == sizeof (int));
      assert (0 <= n && n * 2 < MAX_INIT);
      int tn = n;
      if (tn > MAX_SUGGESTIONS) {
        tn = MAX_SUGGESTIONS;
      }
      int rlen = sizeof (int) * 2 * tn,
          len  = sizeof (int) * 2 * n;

      assert (read (fd[3], buff, len) == len);

      u = conv_uid (id);
      if (u == NULL) {
        curr += len + sizeof (int);
        continue;
      }

      int local_id = local_uid (id);
//      fprintf (stderr, "%d->%d\n", id, local_id);
      assert (1 <= local_id && local_id < tsz);
      sugg_size[local_id] = rlen + sizeof (int);
      sugg_shift[local_id] = curr;

      curr += len + sizeof (int);
    }
  #undef MAX_INIT
  }

  index_header header;
  memset (&header, 0, sizeof (header));

  header.user_cnt = user_table.currId - 1;
  assert (header.user_cnt >= 0);

  open_file (1, indexname, 2);
  header.user_index = qmalloc0 (sizeof (user_index_data) * (header.user_cnt + 1));
  header.created_at = time (NULL);

  long long fCurr = get_index_header_size (&header) - sizeof (long);

  static data_iterator it;

  assert (lseek (fd[1], fCurr, SEEK_SET) == fCurr);

  int *buff = qmalloc (sizeof (int) * (MAX_EXCEPTIONS + 20 + MAX_SUGGESTIONS * 2));

  // for each user
  int u_id;
  for (u_id = 1; u_id <= header.user_cnt; u_id++) {
    if (verbosity > 1) {
      fprintf (stderr, "u_id = %d\n", u_id);
    }
    user *u = &users[u_id];

    load_user_metafile (u, u_id, 1);
    assert (u->metafile_len != -1);

    header.user_index[u_id].id = ltbl_get_rev (&user_table, u_id);
    if (verbosity > 1) {
      fprintf (stderr, "user_id = %d\n", header.user_index[u_id].id);
    }
    header.user_index[u_id].shift = fCurr;

    assert (local_uid (header.user_index[u_id].id) != -1);

    int inf = (1 << 30) - 2;
    data_iter_init (&it, (unsigned char *)u->metafile, u->new_exceptions, inf, u->metafile_len, 0, inf);

    int cnt = 1, val;

//    fprintf (stderr, "gen exceptions\n");
    while ( cnt < MAX_EXCEPTIONS && (val = data_iter_val_and_next (&it)) ) {
      buff[cnt++] = val;
    }
    buff[0] = cnt - 1;
//    fprintf (stderr, "end of gen exceptions\n");

    int buff_sz = sizeof (int) * cnt;

    if (suggname != NULL && sugg_size[u_id]) {
      assert (buff_sz + sugg_size[u_id] < (int)sizeof (int) * (MAX_EXCEPTIONS + 20 + MAX_SUGGESTIONS * 2));

      assert (lseek (fd[3], sugg_shift[u_id], SEEK_SET) == sugg_shift[u_id]);
      assert (read (fd[3], buff + cnt, sugg_size[u_id]) == sugg_size[u_id]);
      buff_sz += sugg_size[u_id];
    }

    // write user
    assert (write (fd[1], buff, buff_sz) == buff_sz);
    header.user_index[u_id].size = buff_sz;
    fCurr += header.user_index[u_id].size;
    assert (user_LRU_unload () != -1);
  }

  // write header
  header.log_pos1 = log_cur_pos();
  header.log_timestamp = log_last_ts;
  header.log_split_min = log_split_min;
  header.log_split_max = log_split_max;
  header.log_split_mod = log_split_mod;
  relax_log_crc32 (0);
  header.log_pos1_crc32 = ~log_crc32_complement;

  assert (lseek (fd[1], 0, SEEK_SET) == 0);
  assert (write (fd[1], &header, sizeof (header) - sizeof (long)) == (sizeof (header) - sizeof (long)));
  assert (write (fd[1], header.user_index, sizeof (user_index_data) * (header.user_cnt + 1)) == (ssize_t)sizeof (user_index_data) * (header.user_cnt + 1));

  free_header (&header);

  assert (fsync (fd[1]) >= 0);
  assert (close (fd[1]) >= 0);
  fd[1] = -1;

  if (suggname != NULL) {
    qfree (sugg_shift, sizeof (long long) * tsz);
    qfree (sugg_size, sizeof (int) * tsz);
  }

  qfree (buff, sizeof (int) * (MAX_EXCEPTIONS + 20 + MAX_SUGGESTIONS * 2));
}

void load_suggestions (char *suggname) {
  assert (open_file (3, suggname, -1) >= 0);
#define MAX_INIT 30000
  static int buff[MAX_INIT];

  int id, n;
  user *u;

  long long sz;
  assert (read (fd[3], &sz, sizeof (long long)) == sizeof (long long));
  assert (sz == fsize[3]);

  while (read (fd[3], &id, sizeof (int)) == sizeof (int)) {
    assert (read (fd[3], &n, sizeof (int)) == sizeof (int));
    assert (0 <= n && n * 2 < MAX_INIT);
    int len = sizeof (int) * 2 * n;
    assert (read (fd[3], buff, len) == len);

    u = conv_uid (id);
    if (u == NULL) {
      continue;
    }

    if (n > MAX_SUGGESTIONS) {
      n = MAX_SUGGESTIONS;
    }

    int i;
    for (i = 0; i < n; i++) {
      trp_incr (&u->sugg, buff[2 * i + 1], buff[2 * i]);
    }
  }

  assert (close (fd[3]) >= 0);
  fd[3] = -1;
#undef MAX_INIT
}

void user_init (user *u) {
  u->metafile = NULL;
  u->metafile_len = -1;
  u->aio = NULL;
  u->next_used = NULL;
  u->prev_used = NULL;

  CHG_INIT (u->new_exceptions);
  trp_init (&u->sugg);
}

void init_all (char *indexname) {
  int i;

  log_ts_exact_interval = 1;

  ltbl_init (&user_table);

  int f = load_header (indexname);

  jump_log_ts = header.log_timestamp;
  jump_log_pos = header.log_pos1;
  jump_log_crc32 = header.log_pos1_crc32;

  int user_cnt = index_users = header.user_cnt;

  if (user_cnt < 2400000) {
    user_cnt = 2400000;
  }

  assert (user_cnt >= 2400000);

  user_cnt *= 1.1;

  while (user_cnt % 2 == 0 || user_cnt % 5 == 0) {
    user_cnt++;
  }

  ltbl_set_size (&user_table, user_cnt);

  users = qmalloc (sizeof (user) * user_cnt);

  for (i = 0; i < user_cnt; i++) {
    user_init (&users[i]);
  }

  LRU_head = users;
  LRU_head->next_used = LRU_head->prev_used = LRU_head;

  if (f) {
    try_init_local_uid();
  }
}

void free_all (void) {
}
