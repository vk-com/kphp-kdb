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

#include <aio.h>
#include <assert.h>
#include <math.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#include "magus-data.h"

#include "dl-utils.h"

int index_mode;
long long max_memory = MAX_MEMORY;
int cur_users;
int index_users, header_size;
int binlog_readed;

char file_name[50];
int types[256];
int dumps[256];
similarity_index_header sim_header[256];

int jump_log_ts;
long long jump_log_pos;
unsigned jump_log_crc32;
long long allocated_metafile_bytes;

char _EMPTY__METAFILE[4] = {0};
char *EMPTY__METAFILE = (char *)_EMPTY__METAFILE;

long long del_by_LRU;

vector_score obj_scores[256];


/*
 *
 *    USERS
 *
 */


hset_intp h_users;
index_header header;
user *users, *LRU_head;
int cur_local_id, user_cnt;

void bind_user_metafile (user *u);
void unbind_user_metafile (user *u);

void load_user_metafile (user *u, int local_id, int no_aio);

int user_loaded (user *u) {
  return u != NULL && u->metafile_len >= 0 && u->aio == NULL;
}

user *conv_uid (int user_id) {
  if (user_id <= 0) {
    return NULL;
  }
  if (user_id % log_split_mod != log_split_min) {
    return NULL;
  }
  user **u = (user **)hset_intp_add (&h_users, &user_id);
  if (*u == (user *)&user_id) {
    assert (cur_local_id + 1 < user_cnt);

    *u = &users[++cur_local_id];
    (*u)->id = user_id;
    (*u)->local_id = cur_local_id;
  }
  return *u;
}

void del_user_used (user *u) {
  assert (u->prev_used != NULL);
  assert (u->next_used != NULL);

  u->next_used->prev_used = u->prev_used;
  u->prev_used->next_used = u->next_used;

  u->prev_used = NULL;
  u->next_used = NULL;
}

void add_user_used (user *u) {
  assert (u != NULL);
  assert (user_loaded (u));

  assert (u->prev_used == NULL);
  assert (u->next_used == NULL);

  user *y = LRU_head->prev_used;

  u->next_used = LRU_head;
  LRU_head->prev_used = u;

  u->prev_used = y;
  y->next_used = u;
}

int unload_user_metafile (user *u);

void user_unload (user *u) {
  assert (user_loaded (u));

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

int *user_get_metafile (user *u, int type) {
  assert (header.multiple_types >= 0 && header.multiple_types < 256 && u->metafile);

  if (header.multiple_types != 0) {
    return header.multiple_types == type ? (int *)u->metafile : NULL;
  }

  int *tmp = (int *)u->metafile;
  while (*tmp != 0) {
    if ((*tmp >> 23) == type) {
      return ((int *)u->metafile) + *tmp - (type << 23);
    }
    tmp++;
  }
  return (int *)EMPTY__METAFILE;
}



inline static float conv_val (float val) {
  if (val < 1.0) {
    val = 1.0;
  }
  if (val > 1e2) {
    val = 1e2;
  }
  return val;
  return sqrt (val);
  return log (1.0 + 3 * val);
}

int user_get_hints (user *u, int type, int cnt, int fn, score *f_score, char *res) {
  assert (u != NULL && type > 0 && type < 256 && cnt > 0 && fn < MAX_FIDS);

  if (fn > MIN_OBJS) {
    int i;
    float mi = 1e2;
    for (i = 0; i < fn; i++) {
      if (f_score[i].val < mi) {
        mi = f_score[i].val;
      }
    }
    for (i = 0; i < fn; i++) {
      f_score[i].val -= mi;
    }
  }

  if (verbosity > 0) {
    int i;

    for (i = 0; i < fn; i++) {
      fprintf (stderr, "%d %f\n", f_score[i].fid, f_score[i].val);
    }
  }

  if (cnt > MAX_RES) {
    cnt = MAX_RES;
  }

  load_user_metafile (u, u->local_id, NOAIO);

  if (!user_loaded (u)) {
    return -2;
  }

  int *a = user_get_metafile (u, type);
  if (a == NULL) {
    assert (0);
    res[0] = 0;
    return -1;
  }

  del_user_used (u);
  add_user_used (u);

  static float m[FIDS_LIMIT];

  similarity_index_header *header = &sim_header[type];
  int fids_size = header->fids.v_size;
  assert (fids_size == header->objs_limit);
  float *f_mul = header->f_mul;

  memset (m, 0, sizeof (float) * fids_size);

#define UPDATE_SCORE(_fid, _val, mul) {                          \
    float c_mul = conv_val (_val) * max (f_mul[_fid], 0.02f) * mul; \
    if (header->file_type == 0) {                                \
      float *c_cur = header->CC + (_fid * fids_size);            \
                                                                 \
      int local_j;                                               \
      for (local_j = 0; local_j < fids_size; local_j++) {        \
        m[local_j] -= c_mul * c_cur[local_j];                    \
      }                                                          \
    } else {                                                     \
      score *c_cur = header->C + header->C_index[_fid],          \
            *c_end = header->C + header->C_index[_fid + 1];      \
                                                                 \
      for (; c_cur < c_end; c_cur++) {                           \
        m[c_cur->fid] -= c_mul * c_cur->val;                     \
      }                                                          \
    }                                                            \
    m[_fid] += 1e10;                                             \
  }
//      fprintf (stderr, "%7d -> %7d %8.6lf %8.6lf %9.6lf\n", _fid, c_cur->fid, c_cur->val, f_mul[_fid], f_mul[_fid] * c_cur->val * 100);
//        if (c_mul * c_cur->val >= 0.1) fprintf (stderr, "%d %.6lf %.6lf %.6lf %.6lf %.6lf %.6lf\n", c_cur->fid, log (3.0 * _val + 1.0), f_mul[_fid], mul, c_cur->val, _val, c_mul * c_cur->val);


  float lower_bound = f_score[0].val * 0.36787f;
  int i, j, good = 0;
  for (i = 0; i < fn; i++) {
    if (f_score[i].fid >= 0) {
      assert (f_score[i].fid < fids_size);
      int fid = f_score[i].fid;
      if ((f_score[i].val > lower_bound || good < MAX_OBJS) && (f_score[i].val > 1.0 || good < MIN_OBJS)) {
        UPDATE_SCORE (fid, f_score[i].val, 1.0);
        good++;
      }

      m[fid] += 1e10;
    }
  }

  static int new_exc[MAX_EXCEPTIONS];
  int new_size = hset_int_values (&u->new_exceptions, new_exc, MAX_EXCEPTIONS);
  if (new_size > MAX_EXCEPTIONS) {
    new_size = MAX_EXCEPTIONS;
  }
  int n = a[0];

  for (j = 1; j <= n; j++) {
    UPDATE_SCORE (a[j], 1.0, -good / 2.0 / sqrt (new_size + n));
  }

  for (j = 0; j < new_size; j++) {
    int exc_type = new_exc[j] >> 23;
    if (exc_type == type) {
      int exc = new_exc[j] - (exc_type << 23);
      UPDATE_SCORE (exc, 1.0, -good / 2.0 / sqrt (new_size + n));
    }
  }

#define fix_down()                            \
  k = j = 1;                                  \
  do {                                        \
    t = p[k], p[k] = p[j], p[j] = t;          \
                                              \
    j = k;                                    \
    t = k * 2 + 1;                            \
    if (t <= p_size) {                        \
      if (m[p[t]] > m[p[k]]) {                \
        k = t;                                \
      }                                       \
                                              \
      t--;                                    \
      if (m[p[t]] > m[p[k]]) {                \
        k = t;                                \
      }                                       \
    } else {                                  \
      t--;                                    \
      if (t <= p_size && m[p[t]] > m[p[k]]) { \
        k = t;                                \
      }                                       \
    }                                         \
  } while (k != j);


  static int p[MAX_RES + 1];
  int p_size = 0, k, t;
  for (i = 0; i < cnt && i < fids_size; i++) {
    p[++p_size] = i;
    j = p_size;
    while (j > 1 && m[p[j]] > m[p[k = j / 2]]) {
      t = p[j], p[j] = p[k], p[k] = t;
      j = k;
    }
  }

  for (i = cnt; i < fids_size; i++) {
    if (m[p[1]] > m[i]) {
      p[1] = i;

      fix_down();
    }
  }

  while (p_size) {
    new_exc[p_size] = p[1];
    p[1] = p[p_size--];
    fix_down();
  }

  j = 0;
  if (header->has_names) {
    for (i = 1; i <= cnt && i <= fids_size && m[new_exc[i]] <= -1e-9; i++) {
      char *s = header->fid_names_begins[new_exc[i]];
      int *ss = (int *)(s + strlen (s) + 1);
//      fprintf (stderr, "found %3d: %6d %9.6lf %9.6lf %s\n", i, new_exc[i], f_mul[new_exc[i]], m[new_exc[i]], s);
      res += sprintf (res, "%s %d_%d,", s, ss[0], ss[1]);
      j = 1;
    }
  } else {
    for (i = 1; i <= cnt && i <= fids_size && m[new_exc[i]] <= -1e-9; i++) {
      res += sprintf (res, "%d,", header->fids.v[new_exc[i]]);
      j = 1;
    }
  }

  res -= j;
  *res = 0;

  return 1;
}


int get_objs_hints (int uid, int type, int fn, char *user_objs, char *res) {
  user *u = conv_uid (uid);
  dbg ("GET_OBJS_HINTS: uid = %d, type = %d, fn = %d, user_objs = %s\n", uid, type, fn, user_objs);

  if (verbosity > 0) {
    fprintf (stderr, "get_objs_hints(%d:%d|%d) %s\n", type, fn, types[type], user_objs);
  }
  if (u == NULL || type <= 0 || type >= 256 || strlen (user_objs) >= MAX_OBJS_LEN || !types[type]) {
    res[0] = 0;
    return -1;
  }

  int cnt, cur = 0, cur_add;
  static score f_score[MAX_FIDS];
  static char obj_name[MAX_OBJS_LEN + 1];

  int i, ti;
  for (i = ti = 0; ti < fn && i < MAX_FIDS; ti++) {
    int *t;
    if (sim_header[type].has_names) {
      if (user_objs[cur] != ',') {
        res[0] = 0;
        return -1;
      }

      int j;
      for (j = 1; user_objs[cur + j] && user_objs[cur + j] != ','; j++) {
        obj_name[j - 1] = user_objs[cur + j];
      }
      obj_name[j - 1] = 0;

      cur += j;
      if (sscanf (user_objs + cur, ",%f%n", &f_score[i].val, &cur_add) != 1) {
        res[0] = 0;
        return -1;
      }
      t = map_string_int_get (&sim_header[type].fid_id_str, obj_name);
    } else {
      if (sscanf (user_objs + cur, ",%d,%f%n", &f_score[i].fid, &f_score[i].val, &cur_add) != 2) {
        res[0] = 0;
        return -1;
      }
      t = map_int_int_get (&sim_header[type].fid_id, f_score[i].fid);
    }
    if (t != NULL) {
      f_score[i].fid = *t;
      i++;
    }
    cur += cur_add;
  }

  if (sscanf (user_objs + cur, "#%d%n", &cnt, &cur_add) != 1 || cnt <= 0 || user_objs[cur + cur_add]) {
    res[0] = 0;
    return -1;
  }

  if (!types[type]) {
    res[0] = 0;
    return 0;
  }

  return user_get_hints (u, type, cnt, i, f_score, res);
}


int get_objs (int uid, int type, int cnt, char *res) {
  return 0;

//not supported
/*
  user *u = conv_uid (uid);

  if (u == NULL || u->objs == NULL || type <= 0 || type >= 256 || !types[type]) {
    res[0] = 0;
    return -1;
  }

  return user_get_hints (u, type, cnt, u->objs_n, u->objs, res);
*/
}

int add_exception (struct lev_magus_add_exception *E) {
  user *u = conv_uid (E->user_id);

  if (u == NULL) {
    return 0;
  }

  int type = E->type - LEV_MAGUS_ADD_EXCEPTION;
  assert (0 < type && type < 256);

  assert (types[type] && !sim_header[type].has_names);

  int *fid = map_int_int_get (&sim_header[type].fid_id, E->fid);
  if (fid == NULL && index_mode) {
    int *cnt = &sim_header[type].fid_names_cnt;
    fid = map_int_int_add (&sim_header[type].fid_id, E->fid);
    *fid = *cnt;
    *cnt = (*cnt) + 1;
  }
  if (fid == NULL) {
    return 0;
  }
  assert (0 <= *fid && *fid < (1 << 23));

  hset_int_add (&u->new_exceptions, (type << 23) + *fid);

  return 1;
}

int add_exception_string (struct lev_magus_add_exception_string *E) {
  user *u = conv_uid (E->user_id);

  if (u == NULL) {
    return 0;
  }

  int type = E->type - LEV_MAGUS_ADD_EXCEPTION_STRING;
  assert (0 < type && type < 256);

  assert (types[type] && sim_header[type].has_names);

  int *fid = map_string_int_get (&sim_header[type].fid_id_str, E->text);
  if (fid == NULL && index_mode) {
    int *cnt = &sim_header[type].fid_names_cnt;
    assert (*cnt < sim_header[type].objs_limit * 2 + 1024000 * index_mode + 1);
    sim_header[type].fid_names_begins[*cnt] = dl_strdup (E->text);
    fid = map_string_int_add (&sim_header[type].fid_id_str, sim_header[type].fid_names_begins[*cnt]);
    *fid = *cnt;
    *cnt = (*cnt) + 1;
  }
  if (fid == NULL) {
    return 0;
  }

  assert (0 <= *fid && *fid < (1 << 23));

  hset_int_add (&u->new_exceptions, (type << 23) + *fid);

  return 1;
}


/*
 *
 *           BINLOG
 *
 */


int do_add_exception (int uid, int type, char *fid) {
  int text_len = strlen (fid);
  if (type <= 0 || type >= 256 || !types[type] || text_len >= 4096 || text_len == 0) {
    return 0;
  }
  if (sim_header[type].has_names) {
    int *tmp = map_string_int_get (&sim_header[type].fid_id_str, fid);
    if (tmp == NULL) {
      return 0;
    }
    fid = sim_header[type].fid_names_begins[*tmp];
    text_len = strlen (fid);

    struct lev_magus_add_exception_string *E =
      alloc_log_event (LEV_MAGUS_ADD_EXCEPTION_STRING + type, offsetof (struct lev_magus_add_exception_string, text) + 1 + text_len, 0);

    E->user_id = uid;
    E->text_len = text_len;
    memcpy (E->text, fid, sizeof (char) * (text_len + 1));

    return add_exception_string (E);
  } else {
    int cur, fid_int;
    if (!sscanf (fid, "%d%n", &fid_int, &cur) != 1 || fid[cur]) {
      return 0;
    }

    struct lev_magus_add_exception *E =
      alloc_log_event (LEV_MAGUS_ADD_EXCEPTION + type, sizeof (struct lev_magus_add_exception), 0);

    E->user_id = uid;
    E->fid = fid_int;

    return add_exception (E);
  }
}

int magus_replay_logevent (struct lev_generic *E, int size);

int init_magus_data (int schema) {
  replay_logevent = magus_replay_logevent;
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
    user *u = conv_uid (header.user_index[i].id);
    assert (u != NULL);
  }

  was = 1;
  old_log_split_min = log_split_min;
  old_log_split_max = log_split_max;
  old_log_split_mod = log_split_mod;

  log_schema = MAGUS_SCHEMA_V1;
  init_magus_data (log_schema);
}

static int magus_le_start (struct lev_start *E) {
  if (E->schema_id != MAGUS_SCHEMA_V1) {
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

int magus_replay_logevent (struct lev_generic *E, int size) {
//  fprintf (stderr, "magus_replay_logevent %lld\n", log_cur_pos());
//  fprintf (stderr, "%x\n", E->type);
  if (index_mode) {
    if ((_eventsLeft && --_eventsLeft == 0) || dl_get_memory_used() > max_memory * 3 / 2) {
      save_index();
      exit (13);
    }
  }

  int s;
  switch (E->type) {
  case LEV_START:
    if (size < 24 || E->b < 0 || E->b > 4096) { return -2; }
    s = 24 + ((E->b + 3) & -4);
    if (size < s) { return -2; }
    return magus_le_start ((struct lev_start *) E) >= 0 ? s : -1;
  case LEV_NOOP:
  case LEV_TIMESTAMP:
  case LEV_CRC32:
  case LEV_ROTATE_FROM:
  case LEV_ROTATE_TO:
  case LEV_TAG:
    return default_replay_logevent (E, size);
  case LEV_MAGUS_ADD_EXCEPTION + 1 ... LEV_MAGUS_ADD_EXCEPTION + 255:
    STANDARD_LOG_EVENT_HANDLER(lev_magus, add_exception);
  case LEV_MAGUS_ADD_EXCEPTION_STRING + 1 ... LEV_MAGUS_ADD_EXCEPTION_STRING + 255:
    if (size < (int)sizeof (struct lev_magus_add_exception_string)) {
      return -2;
    }
    s = ((struct lev_magus_add_exception_string *) E)->text_len;
    if (s < 0 || s > 4095) {
      return -4;
    }
    s += 1 + offsetof (struct lev_magus_add_exception_string, text);
    if (size < s) {
      return -2;
    }
    add_exception_string ((struct lev_magus_add_exception_string *)E);
    return s;
  }

  fprintf (stderr, "unknown log event type %08x at position %lld\n", E->type, log_cur_pos());

  return -1;
}


/*
 *
 *         MAGUS INDEX AIO
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
.title = "magus-data-aio-metafile-query",
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

  if (local_id > header.user_cnt || header.user_index[local_id].size <= 4) {
    u->metafile = EMPTY__METAFILE;
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

    if (u->metafile != NULL) {
      return;
    } else {
      fprintf (stderr, "Previous AIO query failed for user at %p, scheduling a new one\n", u);
    }
  }

  u->metafile_len = header.user_index[local_id].size;
  u->metafile = dl_malloc (u->metafile_len);
  if (u->metafile == NULL) {
    fprintf (stderr, "no space to load metafile - cannot allocate %d bytes (%lld currently used)\n", u->metafile_len, dl_get_memory_used());
    assert (0);
  }
  allocated_metafile_bytes += u->metafile_len;

  if (verbosity > 2) {
    fprintf (stderr, "*** Scheduled reading user data from index %d at position %lld, %d bytes, no_aio = %d\n", fd[0], header.user_index[local_id].shift, u->metafile_len, no_aio);
  }

  assert (1 <= local_id && local_id <= header.user_cnt);
  if (no_aio) {
    double disk_time = -get_utime (CLOCK_MONOTONIC);

    long long lseek_res = lseek (fd[0], header.user_index[local_id].shift, SEEK_SET);
    assert (lseek_res == header.user_index[local_id].shift);
    int size = header.user_index[local_id].size;
    int r = read (fd[0], u->metafile, size);
    if (r != size) {
      fprintf (stderr, "error reading user %d from index file: read %d bytes instead of %d at position %lld: %m\n", local_id, r, size, header.user_index[local_id].shift);
      assert (r == size);
    }

    disk_time += get_utime (CLOCK_MONOTONIC);
    if (verbosity > 2) {
      fprintf (stderr, "  disk time = %.6lf\n", disk_time);
    }

    empty_aio_conn.extra = u;
    empty_aio_conn.basic_type = ct_aio;
    u->aio = &empty_aio_conn;

    assert (u->aio != NULL);

    onload_user_metafile ((struct connection *)(&empty_aio_conn), u->metafile_len);
  } else {
    u->aio = create_aio_read_connection (fd[0], u->metafile, header.user_index[local_id].shift, u->metafile_len, &ct_metafile_aio, u);
    assert (u->aio != NULL);
    WaitAio = u->aio;
  }

  return;
}

int onload_user_metafile (struct connection *c, int read_bytes) {
  if (verbosity > 2) {
    fprintf (stderr, "onload_user_metafile (%p,%d)\n", c, read_bytes);
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

    dl_free (u->metafile, u->metafile_len);
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
      assert (0);
    }
  }

  return 1;
}

int unload_user_metafile (user *u) {
  assert (u != NULL);

  int user_id = 0;

  if (verbosity > 1) {
    user_id = u->id;
  }

  if (verbosity > 2) {
    fprintf (stderr, "unload_user_metafile (%d)\n", user_id);
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
    fprintf (stderr, "unload_user_metafile (%d) END\n", user_id);
  }

  return 1;
}

void bind_user_metafile (user *u) {
  if (verbosity > 2) {
    fprintf (stderr, "bind user metafile local id = %d (%p)\n", (int)(u - users), u);
  }
  int local_id = (int)(u - users);

  if (u->metafile == NULL || u->metafile == EMPTY__METAFILE) {
    return;
  }

  assert (1 <= local_id && local_id <= header.user_cnt);

  assert (u->metafile_len >= (int)sizeof (int));

  static int buf[MAX_EXCEPTIONS * 257];

  int *meta_int = (int *)u->metafile, i, n;
  int curd, curt, curc = 0, exception_types;
  if (header.multiple_types > 0) {
    curd = 0;
    curt = header.multiple_types;
    exception_types = -1;
  } else {
    curd = meta_int[curc];
    curt = curd >> 23;
    curd -= curt << 23;
    for (exception_types = 0; meta_int[exception_types] != 0; exception_types++) {
    }
    assert (exception_types >= 2);
    buf[exception_types + 1] = 0;
  }
  int t = exception_types + 1;

  do {
    assert (types[curt]);
    if (exception_types >= 2) {
      buf[curc++] = (curt << 23) + t;
    }

    char *s = u->metafile + curd;
    READ_INT(s, n);

    int nt = t++;
    for (i = 0; i < n; i++) {
      int *num;
      if (sim_header[curt].has_names) {
        char *ss;
        READ_STRING(s, ss);
        num = map_string_int_get (&sim_header[curt].fid_id_str, ss);
        if (num == NULL && index_mode) {
          int *cnt = &sim_header[curt].fid_names_cnt;
          assert (*cnt < sim_header[curt].objs_limit * 2 + 1024000 * index_mode + 1);
          sim_header[curt].fid_names_begins[*cnt] = dl_strdup (ss);
          num = map_string_int_add (&sim_header[curt].fid_id_str, sim_header[curt].fid_names_begins[*cnt]);
          *num = *cnt;
          *cnt = (*cnt) + 1;
        }
      } else {
        int ss;
        READ_INT(s, ss);
        num = map_int_int_get (&sim_header[curt].fid_id, ss);
        if (num == NULL && index_mode) {
          int *cnt = &sim_header[curt].fid_names_cnt;
          assert (*cnt < sim_header[curt].objs_limit * 2 + 1024000 * index_mode + 1);
          num = map_int_int_add (&sim_header[curt].fid_id, ss);
          *num = *cnt;
          *cnt = (*cnt) + 1;
        }
      }
      if (num != NULL) {
        buf[t++] = *num;
      }
    }
    buf[nt] = t - nt - 1;
    if (exception_types >= 2) {
      curd = meta_int[curc];
      curt = curd >> 23;
      curd -= curt << 23;
    }
  } while (curd != 0);
  dl_free (u->metafile, u->metafile_len);
  u->metafile_len = t * sizeof (int);
  u->metafile = dl_malloc (u->metafile_len);
  memcpy (u->metafile, buf, u->metafile_len);
}

void unbind_user_metafile (user *u) {
  assert (u != NULL);

  if (verbosity > 2) {
    fprintf (stderr, "unbind_user_metafile\n");
  }

  if (u->metafile != NULL && u->metafile != EMPTY__METAFILE) {
    dl_free (u->metafile, u->metafile_len);
  }

  u->metafile = NULL;
  u->metafile_len = -1;
}


/*
 *
 *         MAGUS INDEX
 *
 */


void load_similarity (int t) {
  assert (0 < t && t < 256);
  assert (types[t]);

  if (verbosity > 1) {
    fprintf (stderr, "Begin of loading similarity %d\n", t);
  }

  sprintf (file_name, "similarity.%d", t);
  dl_open_file (4, file_name, 0);

  similarity_index_header *header = &sim_header[t];
  assert (!header->objs_type);

  long long cur = 0;
  size_t len = sizeof (int) * 32, read_len;
  read_len = read (fd[4], header, len);
  assert (read_len == len);
  cur += len;

  int fn = header->objs_limit;

  if (verbosity > 0) {
    fprintf (stderr, "fn = %d\nfile_type = %d\nhas_names = %d\n", fn, header->file_type, header->has_names);
  }

  assert (fn <= FIDS_LIMIT);
  assert (header->objs_type == t);
  assert (header->magic == SIMILARITY_MAGIC);

  vector_int *fids = &header->fids;
  vector_int_init (fids);

  len = sizeof (int) * fn;
  read_len = read (fd[4], vector_int_zpb (fids, fn), len);
  assert (read_len == len);
  cur += len;

  len = sizeof (float) * fn;
  header->f_mul = dl_malloc (len);
  assert (header->f_mul);
  read_len = read (fd[4], header->f_mul, len);
  assert (read_len == len);
  cur += len;

  if (header->file_type == 0) {
    assert (header->total_scores == fn * fn);

    len = sizeof (float) * header->total_scores;
    header->CC = dl_malloc (len);
    assert (header->CC);
    read_len = read (fd[4], header->CC, len);
    assert (read_len == len);
    cur += len;
  } else {
    assert (header->file_type == 1);

    len = sizeof (int) * (fn + 1);
    header->C_index = dl_malloc (len);
    assert (header->C_index);
    read_len = read (fd[4], header->C_index, len);
    assert (read_len == len);
    cur += len;

    len = sizeof (score) * header->total_scores;
    header->C = dl_malloc (len);
    assert (header->C);
    read_len = read (fd[4], header->C, len);
    assert (read_len == len);
    cur += len;
  }

  if (header->has_names) {
    char tmp[25];
    map_string_int_init (&header->fid_id_str);

    len = header->has_names;
    header->fid_names = dl_malloc (len);
    assert (header->fid_names);
    read_len = read (fd[4], header->fid_names, len);
    assert (read_len == len);
    cur += len;

    header->fid_names_begins = dl_malloc0 (sizeof (char *) * (fn * (index_mode + 1) + 1024000 * index_mode + 1));
    assert (header->fid_names_begins);
    char *s = header->fid_names - 1;
    int i;
    for (i = 0; i < fn; i++) {
      *(map_string_int_add (&header->fid_id_str, ++s)) = i;
      header->fid_names_begins[i] = s;
      while (*s) {
        s++;
      }
      sprintf (tmp, "%d_%d", ((int *)(s+1))[0], ((int *)(s+1))[1]);
      *(map_string_int_add (&header->fid_id_str, tmp)) = i;
      s += 2 * sizeof (int);
    }
    header->fid_names_cnt = fn;
    assert (s - header->fid_names + 1 == len);
  } else {
    int i;
    map_int_int_init (&header->fid_id);
    for (i = 0; i < fn; i++) {
      *(map_int_int_add (&header->fid_id, fids->v[i])) = i;
    }
  }

  assert (cur == fsize[4]);

  dl_close_file (4);

  if (verbosity > 1) {
    fprintf (stderr, "End of loading sims\n");
  }
}

void load_dump (int t) {
  assert (0 < t && t < 256);
  assert (dumps[t]);

  if (verbosity > 1) {
    fprintf (stderr, "Begin of loading dump %d\n", t);
  }

//not supported now
/*
  size_t len, read_len;

  sprintf (file_name, "dump.%d", t);
  dl_open_file (4, file_name, 0);

  char c = '0';
  int usn = 0, i;
  do {
    usn = usn * 10 + c - '0';
    len = sizeof (char);
    read_len = read (fd[4], &c, len);
    assert (read_len == len);
  } while ('0' <= c && c <= '9');

  if (verbosity) {
    fprintf (stderr, "usn = %d\n", usn);
  }

  len = sizeof (user) * usn;

  vector (user, users);
  vector_init (users);

  read_len = read (fd[4], vector_zpb (users, usn), len);
  assert (read_len == len);

  int scn = 0;
  for (i = 0; i < users_size; i++) {
    scn += users[i].len;
  }

  if (verbosity) {
    fprintf (stderr, "scn = %d\n", scn);
  }

  len = sizeof (score) * scn;
  vector_init (scores);
  read_len = read (fd[4], vector_zpb (scores, scn), len);
  assert (read_len == len);

  int ns = 0, lu = 0, j, ls = 0;
  for (i = 0; i < users_size; i++) {
    int sn = users[i].len, os = ns;

    for (j = 0; j < sn; j++, ls++) {
      if (map_int_int_get (&fid_id, scores[ls].fid) != NULL) {
        scores[ns++] = scores[ls];
      }
    }

    if (ns - os > 0) {
      users[lu].len = ns - os;
      users[lu++].id = users[i].id;
    } else {
      ns = os;
    }
  }
  vector_resize (scores, ns);
  vector_resize (users, lu);

  scn = 0;
  for (i = 0; i < users_size; i++) {
    user *u = conv_uid (users[i].id);
    assert (u != NULL);
    u->objs = scores + scn;
    u->objs_n = users[i].len;

    scn += users[i].len;

    assert (u->objs_n <= MAX_FIDS);
  }

  vector_free (users);

  dl_close_file (4);
*/
  if (verbosity > 1) {
    fprintf (stderr, "End of loading dump %d\n", t);
  }
}

long long get_index_header_size (index_header *header) {
  return sizeof (index_header) + sizeof (user_index_data) * (header->user_cnt + 1);
}

int load_header (kfs_file_handle_t Index) {
  if (Index == NULL) {
    fd[0] = -1;

    header.user_cnt = 0;
    header.user_index = NULL;

    header.log_pos0 = 0;
    header.log_pos1 = 0;
    header.log_timestamp = 0;
    header.log_split_min = 0;
    header.log_split_max = 1;
    header.log_split_mod = 1;
    header.log_pos0_crc32 = 0;
    header.log_pos1_crc32 = 0;

    header.multiple_types = 0;

    header.created_at = time (NULL);
    header_size = sizeof (index_header);

    return 0;
  }

  fd[0] = Index->fd;
  int offset = Index->offset;

  //read header
  long long lseek_res = lseek (fd[0], offset, SEEK_SET);
  assert (lseek_res == offset);

  int size = sizeof (index_header) - sizeof (long);
  int r = read (fd[0], &header, size);
  if (r != size) {
    fprintf (stderr, "error reading header from index file: read %d bytes instead of %d at position %d: %m\n", r, size, offset);
    assert (r == size);
  }
  header.multiple_types += 41;

  size = sizeof (user_index_data) * (header.user_cnt + 1);
  header.user_index = dl_malloc (size);

  r = read (fd[0], header.user_index, size);
  if (r != size) {
    fprintf (stderr, "error reading header from index file: read %d bytes instead of %d: %m\n", r, size);
    assert (r == size);
  }

  assert (header.log_split_max);
  log_split_min = header.log_split_min;
  log_split_max = header.log_split_max;
  log_split_mod = header.log_split_mod;

  header_size = get_index_header_size (&header) - sizeof (long);

  if (verbosity > 1) {
    fprintf (stderr, "header loaded %d %d %d %d\n", fd[0], log_split_min, log_split_max, log_split_mod);
    fprintf (stderr, "ok\n");
  }
  return 1;
}

void free_header (index_header *header) {
  if (header->user_index != NULL) {
    dl_free (header->user_index, sizeof (user_index_data) * (header->user_cnt + 1));
  }
}

int cmp_int (const void *_a, const void *_b) {
  int a = *(int *)_a, b = *(int *)_b;
  if (a < b) {
    return -1;
  }
  return a > b;
}

int save_index (void) {
  char *newidxname = NULL;

  if (engine_snapshot_replica) {
    newidxname = get_new_snapshot_name (engine_snapshot_replica, log_cur_pos(), engine_snapshot_replica->replica_prefix);
  }

  if (!newidxname || newidxname[0] == '-') {
    fprintf (stderr, "cannot write index: cannot compute its name\n");
    exit (1);
  }

  if (log_cur_pos() == jump_log_pos) {
    fprintf (stderr, "skipping generation of new snapshot %s for position %lld: snapshot for this position already exists\n",
        newidxname, jump_log_pos);
    return 0;
  }

  if (verbosity > 0) {
    fprintf (stderr, "creating index %s at log position %lld\n", newidxname, log_cur_pos());
  }

  if (!binlog_disabled) {
    relax_write_log_crc32();
  } else {
    relax_log_crc32 (0);
  }

  index_header header;
  memset (&header, 0, sizeof (header));

  header.user_cnt = cur_local_id;
  assert (header.user_cnt >= 0);

  fd[1] = open (newidxname, O_CREAT | O_TRUNC | O_WRONLY | O_EXCL, 0660);
  if (fd[1] < 0) {
    fprintf (stderr, "cannot create new index file %s: %m\n", newidxname);
    exit (1);
  }

  header.magic = MAGUS_INDEX_MAGIC;

  header.user_index = dl_malloc0 (sizeof (user_index_data) * (header.user_cnt + 1));
  header.created_at = time (NULL);

  long long fCurr = get_index_header_size (&header) - sizeof (long);
  long long lseek_res = lseek (fd[1], fCurr, SEEK_SET);
  assert (lseek_res == fCurr);
  int *buff = dl_malloc ((MAX_EXCEPTIONS + 1) * sizeof (int));
  char *buff_str = dl_malloc ((MAX_EXCEPTIONS + 2) * 4096);
  int *buff_str_int = (int *)buff_str;
  int *new_buff = dl_malloc (MAX_EXCEPTIONS * sizeof (int));

  int i;
  int exception_types = 0;
  for (i = 1; i < 256; i++) {
    if (types[i]) {
      exception_types++;
      header.multiple_types = i;
    }
  }
  if (exception_types >= 2) {
    header.multiple_types = 0;
    buff[exception_types] = 0;
    buff_str_int[exception_types] = 0;
  } else {
    exception_types = -1;
  }
  header.multiple_types -= 41;

  //for each user
  int u_id;
  for (u_id = 1; u_id <= header.user_cnt; u_id++) {
    if (verbosity > 1) {
      fprintf (stderr, "u_id = %d, fd = %d\n", u_id, fd[1]);
    }
    user *u = &users[u_id];

    load_user_metafile (u, u_id, 1);
    assert (u->metafile_len != -1);

    header.user_index[u_id].id = u->id;
    if (verbosity > 1) {
      fprintf (stderr, "user_id = %d\n", header.user_index[u_id].id);
    }
    header.user_index[u_id].shift = fCurr;

    assert (conv_uid (header.user_index[u_id].id) == u);

    int new_size = hset_int_values (&u->new_exceptions, new_buff, MAX_EXCEPTIONS);
//    assert (new_size == hset_int_size (&u->new_exceptions));
    if (new_size > MAX_EXCEPTIONS) {
//      fprintf (stderr, "User %d has %d exceptions.\n", header.user_index[u_id].id, new_size);
      new_size = MAX_EXCEPTIONS;
    }
    assert (new_size <= MAX_EXCEPTIONS);

    qsort (new_buff, new_size, sizeof (int), cmp_int);

    int t = exception_types + 1;
    int curt = 0, i2 = 0, n2 = new_size;
    for (i = 1; i < 256; i++) {
      if (types[i]) {
        if (exception_types >= 2) {
          buff[curt++] = (i << 23) + t;
        }
        int nt = t++;
        int *old_buff = user_get_metafile (u, i);
        int n1 = old_buff++[0];
        if (n1 > MAX_EXCEPTIONS - new_size) {
          n1 = MAX_EXCEPTIONS - new_size;
        }

        int i1 = 0;
        while ((i1 < n1 || (i2 < n2 && (new_buff[i2] >> 23) == i)) && t <= MAX_EXCEPTIONS) {
          int new_val = new_buff[i2] - (i << 23);
          int new_end = i2 < n2 && (new_buff[i2] >> 23) == i;
          if (i1 == n1 || (new_end && old_buff[i1] > new_val)) {
            buff[t++] = new_val;
            i2++;
          } else if (!new_end || old_buff[i1] < new_val) {
            buff[t++] = old_buff[i1++];
          } else {
            buff[t++] = old_buff[i1++];
            i2++;
          }
        }
        assert (i1 == n1);
        buff[nt] = t - nt - 1;
      }
    }
    assert (i2 == n2);

    char *s = buff_str + sizeof (int) * (exception_types + 1);

    curt = 0;
    for (i = 1; i < 256; i++) {
      if (types[i]) {
        int d = 0;
        if (exception_types >= 2) {
          buff_str_int[curt] = (i << 23) + (s - buff_str);
          d = buff[curt] - (i << 23);
        }
        assert (d >= exception_types + 1 && d < (1 << 23));
        int n = buff[d++];

        WRITE_INT(s, n);
        while (n > 0) {
          if (sim_header[i].has_names) {
            WRITE_STRING(s, sim_header[i].fid_names_begins[buff[d]]);
            s++;
          } else {
            WRITE_INT(s, buff[d]);
          }
          d++;
          n--;
        }

        curt++;
      }
    }

    int buff_sz = s - buff_str;

//  write user
    size_t write_len = write (fd[1], buff_str, buff_sz);
    assert (write_len == buff_sz);
    header.user_index[u_id].size = buff_sz;
    fCurr += header.user_index[u_id].size;
    if (user_LRU_unload() == -1) {
      assert (0);
    }
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
  assert (write (fd[1], &header, sizeof (header) - sizeof (long)) == (ssize_t)(sizeof (header) - sizeof (long)));
  assert (write (fd[1], header.user_index, sizeof (user_index_data) * (header.user_cnt + 1)) == (ssize_t)(sizeof (user_index_data) * (header.user_cnt + 1)));

  dl_free (buff, (MAX_EXCEPTIONS + 1) * sizeof (int));
  dl_free (buff_str, (MAX_EXCEPTIONS + 2) * 4096);
  dl_free (new_buff, MAX_EXCEPTIONS * sizeof (int));
  free_header (&header);

  assert (fsync (fd[1]) >= 0);
  assert (close (fd[1]) >= 0);
  fd[1] = -1;

  if (rename_temporary_snapshot (newidxname)) {
    fprintf (stderr, "cannot rename new index file from %s: %m\n", newidxname);
    unlink (newidxname);
    exit (1);
  }

  print_snapshot_name (newidxname);
  return 0;
}

void user_init (user *u) {
  u->id = 0;
  u->local_id = 0;
  u->metafile = NULL;
  u->metafile_len = -1;
  hset_int_init (&u->new_exceptions);

  u->aio = NULL;
  u->next_used = NULL;
  u->prev_used = NULL;
}

int init_all (kfs_file_handle_t Index) {
  int i;

  if (verbosity > 1) {
    fprintf (stderr, "Init_all started\n");
  }

  log_ts_exact_interval = 1;

  hset_intp_init (&h_users);

  int f = load_header (Index);

  jump_log_ts = header.log_timestamp;
  jump_log_pos = header.log_pos1;
  jump_log_crc32 = header.log_pos1_crc32;

  user_cnt = index_users = header.user_cnt;

  if (user_cnt < 10000000) {
    user_cnt = 10000000;
  }

  assert (user_cnt >= 10000000);
  user_cnt = (int)(user_cnt * 1.1);

  while (user_cnt % 2 == 0 || user_cnt % 5 == 0) {
    user_cnt++;
  }

  users = dl_malloc (sizeof (user) * user_cnt);

  for (i = 0; i < user_cnt; i++) {
    user_init (&users[i]);
  }

  LRU_head = users;
  LRU_head->next_used = LRU_head->prev_used = LRU_head;

  if (f) {
    try_init_local_uid();
  }

  if (verbosity > 1) {
    fprintf (stderr, "Init_all finished\n");
  }

  return f;
}

void free_all (void) {
  if (verbosity > 0) {
    while (user_LRU_unload() != -1) {
    }

    int i, j;
    for (i = 0; i < user_cnt; i++) {
      hset_int_free (&users[i].new_exceptions);
    }

    for (i = 1; i < 256; i++) {
      if (types[i]) {
        similarity_index_header *header = &sim_header[i];
        int fn = header->objs_limit;

        vector_int_free (&header->fids);

        dl_free (header->f_mul, sizeof (float) * fn);

        if (header->file_type == 0) {
          dl_free (header->CC, sizeof (float) * header->total_scores);
        } else {
          dl_free (header->C_index, sizeof (int) * (fn + 1));
          dl_free (header->C, sizeof (score) * header->total_scores);
        }

        if (header->has_names) {
          dl_free (header->fid_names, header->has_names);

          for (j = fn; j < header->fid_names_cnt; j++) {
            dl_strfree (header->fid_names_begins[j]);
          }
          dl_free (header->fid_names_begins, sizeof (char *) * (fn * (index_mode + 1) + 1024000 * index_mode + 1));
          map_string_int_free (&header->fid_id_str);
        } else {
          map_int_int_free (&header->fid_id);
        }
      }
      if (dumps[i]) {
//        vector_free (obj_scores[i]);
      }
    }

    dl_free (users, sizeof (user) * user_cnt);

    hset_intp_free (&h_users);
    free_header (&header);

    fprintf (stderr, "Memory left: %lld\n", dl_get_memory_used());
    assert (dl_get_memory_used() == 0);
  }
}
