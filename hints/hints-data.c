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

#define _FILE_OFFSET_BITS 64

#include <stddef.h>

#include "hints-data.h"

#include "dl.def"

#ifdef NOHINTS
int add_on_increment = 1;
#endif
int immediate_mode;
int index_mode;
long max_memory = MAX_MEMORY;
long static_memory;
int cur_users;
int index_users, header_size, indexed_users;
int dump_mode, dump_type[256];
long long friend_changes;
long long total_cache_time;
int max_cache_time, min_cache_time = 999999999;
int keep_not_alive = 0;
int no_changes = 0;

int estimate_users = 1000000;
int MAX_CNT = 100000;
int MAX_USR_SIZE;

int max_cnt_type[256];

int jump_log_ts;
long long jump_log_pos;
unsigned jump_log_crc32;

char *index_name, *new_binlog_name;

int MAX_RATING = 1000;
int RATING_NORM = 4 * 7 * 24 * 60 * 60;

int rating_num = 1;
int fading = 1;

long long allocated_metafile_bytes;

#ifdef HINTS
long long words_per_request[6];
#endif
long long bad_requests;

dl_zout new_binlog;

int ratingT;
int log_now;

int stat_global[256][3];

static int EMPTY__METAFILE__[] = {0};
static char *EMPTY__METAFILE = (char *)EMPTY__METAFILE__;

#ifdef HINTS
#define MEM_FLAG (int)(1u << 31)
#endif


int check_user_id (int user_id) {
  return (user_id > 0 && user_id % log_split_mod == log_split_min);
}

int check_type (int type) {
  return (0 < type && type < 256);
}

int check_object_id (int object_id) {
  return (0 < object_id);
}

int check_rating (rating val) {
  if (fading) {
    return val >= 0;
  } else {
    return fabsf (val) < MAX_RATING;
  }
}

int check_rating_num (int num) {
  return (num >= 0 && num < rating_num);
}

int check_text_len (int text_len) {
  return (text_len >= -1 && text_len < 4096);
}


#define USER_LOADED 1
#define GET_USER_INFO(x) ((((x) & 30) >> 1) - 2)
#define SET_USER_INFO(x, y) ((x) ^ ( ((GET_USER_INFO(x) + 2) ^ (y + 2)) << 1 ))
#define GET_USER_RATING_STATE(x) (((x) >> 5) & 1)
#define SET_USER_RATING_STATE(x, y) ((x) ^ ( (GET_USER_RATING_STATE(x) ^ (y)) << 5 ))

#define MEMORY_CHANGES_PERCENT (0.75)
#define MIN_MEMORY_USER_PERCENT (0.25)

typedef struct userx user;

struct userx {
  int load_time;

  char *metafile;
  int metafile_len;
#ifdef HINTS
  void *mtf_h;
  int *mtf_i, mtf_n;

  perfect_hash h;
#endif

  int types_cnt;
  char *known_types;

  struct aio_connection *aio;

#ifdef HINTS
  // hash_table of prefixes for changes
  hash_table pref_table;

  char *object_data;
  int *object_indexes;
#else
  changes object_chg;
#endif
  long long *object_type_ids;
  rating *object_old_ratings;
  int object_old_n;

  // lookup_table of objects for new objects
  lookup_table object_table;
  // size of object buffers for new objects
  int object_size;
#ifdef HINTS
  // names of new objects
  char **object_names;
  vector changed_objs;
#endif
  // ratings of new objects
  rating *object_ratings;

  // list of changes
  change_list_ptr chg_list_cur, chg_list_st, chg_list_en;
  change_list_ptr chg_list_global;

  // flags
  char flags;

  // LRU
  user *next_used, *prev_used;
};


#define	HINTS_INDEX_MAGIC 0x4f191588


#pragma pack(push,4)

typedef struct {
  int id;
  int size;
  int flags;
  long long shift;
} user_index_data;

typedef struct {
/* strange numbers */
  int magic;
  int created_at;
  long long log_pos0;
  long long log_pos1;
  unsigned long long file_hash;
  unsigned long long slice_hash;
  int log_timestamp;
  int log_split_min, log_split_max, log_split_mod;
  long long list_index_offset;
  long long list_data_offset;
  long long revlist_data_offset;
  long long extra_data_offset;
  long long data_end_offset;
  int tot_lists;
  int last_global_id;
  unsigned int log_pos0_crc32;
  unsigned int log_pos1_crc32;
  int perf_hash;
  int nohints;
  int rating_num;
  int fading;

  int has_crc32;

  int reserved[29];

  unsigned int user_index_crc32;
  unsigned int header_crc32;

  int user_cnt;
  user_index_data *user_index;
} index_header;

#pragma pack(pop)


lookup_table user_table;
user *users;
change_list_ptr global_changes_st, global_changes_en;
index_header header;

user *LRU_head;

long changes_memory = 0;
long long changes_count = 0;
long long del_by_LRU;


char *load_user_metafile (user *u, int local_user_id, int no_aio);
int unload_user_metafile (user *u);

int get_local_user_id (int user_id) {
  if (user_id <= 0) {
    return -1;
  }
  if (user_id % log_split_mod != log_split_min) {
    return -1;
  }
  if (write_only) {
    return 1;
  }

  return ltbl_get_to (&user_table, user_id);
}

int local_user_id (int user_id) {
  if (user_id <= 0) {
    return -1;
  }
  if (user_id % log_split_mod != log_split_min) {
    return -1;
  }
  if (write_only) {
    return 1;
  }

  return ltbl_add (&user_table, user_id);
}

user *conv_user_id (int user_id) {
  int local_id = local_user_id (user_id);
  if (local_id == -1) {
    return NULL;
  }

  assert (local_id + 1 < user_table.to.size);

  user *u = &users[local_id];

  if (immediate_mode == 1) {
    assert (load_user_metafile (u, local_id, 1));
  }

  return u;
}


#ifdef HINTS
LIST mtf_get (user *u, long long key, int *len) {
  int n = u->mtf_n;

  if (n == 0) {
    *len = 0;
    return NULL;
  }

  int *ind = u->mtf_i;

  if (header.perf_hash) {
    short *h = u->mtf_h;

    int l = ph_h (&u->h, key);
    if (l < n && (short)key == h[l]) {
      *len = ind[l + 1] - ind[l];
      return (LIST)(u->metafile + ind[l]);
    } else {
      *len = 0;
      return NULL;
    }
  } else {
    long long *h = u->mtf_h;

    int l = 0, r = n, c;
    while (l + 1 < r) {
      c = (l + r) >> 1;
      if (h[c] <= key) {
        l = c;
      } else {
        r = c;
      }
    }

    if (h[l] == key) {
      *len = ind[l + 1] - ind[l];
      return (LIST)(u->metafile + ind[l]);
    } else {
      *len = 0;
      return NULL;
    }
  }
}
#endif

inline void check (user *u, int local_id) {
  assert (local_id > 0);
  if (local_id > u->object_old_n) {
    assert (ltbl_get_rev (&u->object_table, local_id - u->object_old_n));
  }
}

long long user_get_object_type_id (user *u, int local_id) {
  check (u, local_id);

  if (local_id <= u->object_old_n) {
    return u->object_type_ids[local_id];
  } else {
    return ltbl_get_rev (&u->object_table, local_id - u->object_old_n);
  }
}

rating *user_get_object_rating (user *u, int local_id, int num) {
  check (u, local_id);
  assert (0 <= num && num < rating_num);

  if (local_id <= u->object_old_n) {
    return &u->object_old_ratings[local_id * rating_num + num];
  } else {
    return &u->object_ratings[(local_id - u->object_old_n) * rating_num + num];
  }
}

rating user_get_now_object_rating (user *u, int local_id, int num) {
  if (GET_USER_RATING_STATE(u->flags) == 0) {
    return 0.0;
  }

  rating r = *user_get_object_rating (u, local_id, num);
  if (fading) {
    r *= expf (((rating)(ratingT - now)) / RATING_NORM);
  }
  return r;
}

rating user_get_object_weight (user *u, int local_id, int num) {
  check (u, local_id);

  return *user_get_object_rating (u, local_id, num);
}

int user_get_object_local_id_type_id (user *u, long long type_id) {
  if (u->object_old_n) {
    int l = 1, r = u->object_old_n + 1, c;
    while (l + 1 < r) {
      c = (l + r) >> 1;
      if (u->object_type_ids[c] <= type_id) {
        l = c;
      } else {
        r = c;
      }
    }

    if (u->object_type_ids[l] == type_id) {
      return l;
    }
  }

  int res = ltbl_get_to (&u->object_table, type_id);
  if (res == 0) {
    return 0;
  }
  return res + u->object_old_n;
}

int user_get_object_local_id (user *u, int type, int object_id) {
  return user_get_object_local_id_type_id (u, TYPE_ID(type, object_id));
}

#ifdef HINTS
char *user_get_object_name (user *u, int local_id) {
  check (u, local_id);

  if (local_id <= u->object_old_n) {
    if (u->object_indexes[local_id] & MEM_FLAG) {
      return *((char **)(u->object_data + (u->object_indexes[local_id] & ~MEM_FLAG)));
    } else {
      return (u->object_data + u->object_indexes[local_id]);
    }
  } else {
    return u->object_names[local_id - u->object_old_n];
  }
}
#endif

inline int rating_cmp (rating a, rating b) {
  rating res = a - b;
  if (fabsf (res) < 1e-7f) {
    return 0;
  }
  if (res < 0) {
    return -1;
  }
  return 1;
}

void rating_incr (rating *a, int cnt, int now_time) {
  if (fading) {
    rating dt = ((rating)(now_time - ratingT)) / RATING_NORM;
    *a += expf (dt) * cnt;
  } else {
    *a += cnt - 128;
    if (*a < -MAX_RATING) {
      *a = -MAX_RATING;
    }
    if (*a > MAX_RATING) {
      *a = MAX_RATING;
    }
  }
}

int object_cmp (user *u, int a, int b, int num) {
  int x = 0;
  if (GET_USER_RATING_STATE(u->flags) == 1) {
    x = rating_cmp (*user_get_object_rating (u, a, num), *user_get_object_rating (u, b, num));
  }
  if (x != 0) {
    return x;
  }

  // TODO optimize if needed
  long long a1 = user_get_object_type_id (u, a);
  long long b1 = user_get_object_type_id (u, b);

  if (a1 < b1) {
    return 1;
  } else if (a1 > b1) {
    return -1;
  }

  return 0;
}

void del_user_used (user *u) {
  u->next_used->prev_used = u->prev_used;
  u->prev_used->next_used = u->next_used;
}

void add_user_used (user *u) {
  user *y = LRU_head->prev_used;

  u->next_used = LRU_head;
  LRU_head->prev_used = u;

  u->prev_used = y;
  y->next_used = u;
}

void user_unload (user *u) {
  assert (u != NULL);
  if (!(u->flags & USER_LOADED)) {
    assert (0);
    return;
  }

  u->flags ^= USER_LOADED;
  unload_user_metafile (u);
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

void test_user_unload (int user_id) {
  user *u = conv_user_id (user_id);
  if (u == NULL) {
    return;
  }

  if (u->flags & USER_LOADED) {
    u->flags ^= USER_LOADED;
    unload_user_metafile (u);
  }
}

void user_upd_object_size (user *u) {
  if (u->object_table.currId >= u->object_size) {
    int len = u->object_size;

    u->object_size = u->object_table.currId * 2;
#ifdef HINTS
    u->object_names = dl_realloc (u->object_names, sizeof (char *) * u->object_size, sizeof (char *) * len);
    memset (u->object_names + len, 0, sizeof (char *) * (u->object_size - len));
#endif

    u->object_ratings = dl_realloc (u->object_ratings, sizeof (rating) * u->object_size * rating_num, sizeof (rating) * len * rating_num);
    memset (u->object_ratings + len * rating_num, 0, sizeof (rating) * (u->object_size - len) * rating_num);
  }
}

void user_init (user *u) {
#ifdef HINTS
  htbl_init (&u->pref_table);
#endif
  ltbl_init (&u->object_table);
  chg_list_init (&u->chg_list_st, &u->chg_list_en);
  u->object_size = 0;
#ifdef HINTS
  u->object_names = NULL;
#endif
  u->object_ratings = NULL;
  u->chg_list_cur = u->chg_list_st;
  u->chg_list_global = global_changes_st;
  u->flags = SET_USER_INFO(0, -1);
  u->flags = SET_USER_RATING_STATE(u->flags, 1);

  u->next_used = NULL;
  u->prev_used = NULL;

  u->metafile = NULL;
  u->metafile_len = 0;
  u->aio = 0;

#ifdef HINTS
  u->object_data = NULL;
  u->object_indexes = NULL;
#else
  CHG_INIT (u->object_chg);
#endif
  u->object_type_ids = NULL;
  u->object_old_n = 0;
}

inline int user_has_local_object (user *u, int local_id) {
  if (!local_id) {
    return 0;
  }

#ifdef HINTS
  if (user_get_object_name (u, local_id) != NULL) {
#else
  if (chg_has (u->object_chg, local_id) >= 0) {
#endif
    return local_id;
  }

  return 0;
}

inline int user_has_object (user *u, int type, int id) {
  int local_id = user_get_object_local_id (u, type, id);

  return user_has_local_object (u, local_id);
}

#ifdef HINTS
int get_user_object_text (int user_id, int type, long long object_id, char **text) {
  if (!check_user_id (user_id) || !check_type (type) || !check_object_id (object_id)) {
    bad_requests++;
    return -1;
  }

  int local_user_id = get_local_user_id (user_id);
  if (local_user_id == 0) {
    return 0;
  }
  assert (local_user_id > 0);

  user *u = conv_user_id (user_id);
  assert (u != NULL);

  if (load_user_metafile (u, local_user_id, NOAIO) == NULL) {
    return -2;
  }

  int local_id = user_get_object_local_id (u, type, object_id);
  if (!local_id) {
    return 0;
  }

  char *name = user_get_object_name (u, local_id);
  if (name == NULL) {
    return 0;
  }

  *text = clean_str (name);
  return 1;
}
#endif

void user_do_change (user *u, change_list_ptr change) {
/*  {
    int id = ltbl_get_rev (&user_table, (int)(u - users)), x = change->x, type = change->type, tm = change->timestamp, num = change->number;
    char *s = change->s;
    fprintf (stderr, "do change user = %d %d:%d, timestamp = %d, number = %d <", id, type, x, tm, num);
    fprintf (stderr, "%ld><", (long)s);

    if (s == NULL || change->x <= 0 || type >= 256) {
      fprintf (stderr, "NULL>\n");
    } else {
      fprintf (stderr, "%s>\n", s);
    }
  }*/

  int type = change->type;
  int local_id;

  if (change->x == 0) {
    if (type == 0) {
      //Rating nullify
      int object_n, num;
      for (object_n = u->object_old_n + u->object_table.currId - 1; object_n > 0; object_n--) {
        for (num = 0; num < rating_num; num++) {
          *user_get_object_rating (u, object_n, num) = 0.0f;
        }
      }
    } else if (type == 1 || type == 2) {
      //Set rating state
      u->flags = SET_USER_RATING_STATE(u->flags, type - 1);
    } else {
      assert (0);
    }
    return;
  }

  if (change->x < 0) {
    if (GET_USER_RATING_STATE(u->flags) == 0) {
      return;
    }

    int num = type / 256;
    int x = -change->x;
    type %= 256;
    local_id = user_has_object (u, type, x);

#ifdef NOHINTS
    if (!local_id && add_on_increment) {
      change_list tmp;
      tmp.next = NULL;
      tmp.s = NULL;
      tmp.x = x;
      tmp.type = type;
      tmp.timestamp = change->timestamp;
      tmp.number = change->number;

      user_do_change (u, &tmp);
      local_id = user_has_object (u, type, x);
    }
#endif

    if (verbosity > 1) {
      fprintf (stderr, "Incrementing object of type %d, object_id = %d, local_id = %d\n", type, x, local_id);
    }

    if (local_id != 0) {
      if (num >= MAX_RATING_NUM) { //set obj rating
        if (fading) {
          *user_get_object_rating (u, local_id, num - MAX_RATING_NUM) = (rating)(change->val * expf (((rating)(change->timestamp - ratingT)) / RATING_NORM));
        } else {
          rating *a = user_get_object_rating (u, local_id, num - MAX_RATING_NUM);
          *a = change->val;
          if (*a < -MAX_RATING) {
            *a = -MAX_RATING;
          }
          if (*a > MAX_RATING) {
            *a = MAX_RATING;
          }
        }
      } else {
        rating_incr (user_get_object_rating (u, local_id, num), change->cnt, change->timestamp);
      }
    }
    return;
  }

  if (type >= 256) { // global change type
    type -= 256;
    int new_type = change->cnt;

    assert (0 < type && type < 256);
    assert (0 < new_type && new_type < 256);

    local_id = user_get_object_local_id (u, type, change->x);

    if (verbosity > 1) {
      fprintf (stderr, "Changing type of object with type %d, object_id = %d, local_id = %d to type %d\n", type, change->x, local_id, new_type);
    }

    if (user_has_local_object (u, local_id)) {
      change_list tmp;
      tmp.next = NULL;
#ifdef HINTS
      tmp.s = user_get_object_name (u, local_id);
#else
      tmp.s = NULL;
#endif
      tmp.x = change->x;
      tmp.type = new_type;
      tmp.timestamp = change->timestamp;
      tmp.number = change->number;

      user_do_change (u, &tmp);


      int new_local_id = user_get_object_local_id (u, new_type, change->x);
      if (new_local_id == 0) {
        return;
      }

      int num;
      for (num = 0; num < rating_num; num++) {
        *user_get_object_rating (u, new_local_id, num) = *user_get_object_rating (u, local_id, num);
      }

#ifdef HINTS
      tmp.s = NULL;
#endif
      tmp.type = -type;

      user_do_change (u, &tmp);
    }

    return;
  }

  int is_del;
  if (type < 0) {
    is_del = 1;
    type = -type;
  } else {
    is_del = 0;
  }

#ifdef HINTS
  char *s, **s_ptr = NULL;
  int s_in_mtf = 0;
#endif

  if (is_del == 1) {
    assert (change->s == NULL);
    local_id = user_has_object (u, type, change->x);

    if (verbosity > 1) {
      fprintf (stderr, "Deleting object of type %d, object_id = %d, local_id = %d\n", type, change->x, local_id);
    }

    if (local_id == 0) {
      return; // object doesn't exist (do not need to delete)
    }

#ifdef HINTS
    if (local_id <= u->object_old_n) {
      s_ptr = ((char **)(u->object_data + (u->object_indexes[local_id] & ~MEM_FLAG)));
      if (u->object_indexes[local_id] & MEM_FLAG) {
        s = *s_ptr;
      } else {
        s_in_mtf = 1;
        s = (u->object_data + u->object_indexes[local_id]);
        u->object_indexes[local_id] |= MEM_FLAG;
        vct_add (&u->changed_objs, local_id);
      }
    } else {
      s_ptr = &u->object_names[local_id - u->object_old_n];
      s = *s_ptr;
    }
#else
    chg_del (&u->object_chg, local_id * 2 + 1);
#endif
  } else {
#ifdef HINTS
    assert (change->s != NULL);
#else
    assert (change->s == NULL);
#endif

    local_id = user_get_object_local_id (u, type, change->x);
    if (verbosity > 1) {
      fprintf (stderr, "Adding object of type %d, object_id = %d, local_id = %d\n", type, change->x, local_id);
    }

    if (user_has_local_object (u, local_id)) {
#ifdef HINTS
      change_list tmp;
      tmp.next = NULL;
      tmp.s = NULL;
      tmp.x = change->x;
      tmp.type = -type;
      tmp.timestamp = change->timestamp;
      tmp.number = change->number;

      user_do_change (u, &tmp);
#else
      return;
#endif
    } else {
      if (local_id == 0) {
        if (u->object_old_n + u->object_table.currId + 1 < MAX_CNT - (max_cnt_type[type] < MAX_CNT) * MAX_CNT / 10) {
          local_id = ltbl_add (&u->object_table, TYPE_ID(type, change->x)) + u->object_old_n;

          user_upd_object_size (u);

          if (fading) {
            int num;

            for (num = 0; num < rating_num; num++) {
              rating_incr (user_get_object_rating (u, local_id, num), 1, change->timestamp);
            }
          }

          if (type == 10) {
            friend_changes++;
          }
        } else {
          return;
        }

        if (verbosity > 1) {
          fprintf (stderr, "Adding object of type %d, object_id = %d, local_id = %d\n", type, change->x, local_id);
        }
      }
    }
#ifdef NOHINTS
    chg_add (&u->object_chg, local_id * 2 + 1);
#endif

    user_upd_object_size (u);

#ifdef HINTS
    assert (user_get_object_name (u, local_id) == NULL);

    int len = (strlen (change->s) + 1) * sizeof (char);
    s = dl_malloc (len);
    memcpy (s, change->s, len);

    if (local_id <= u->object_old_n) {
      *((char **)(u->object_data + (u->object_indexes[local_id] & ~MEM_FLAG))) = s;

      assert (u->object_indexes[local_id] & MEM_FLAG);
    } else {
      u->object_names[local_id - u->object_old_n] = s;
    }
#endif
  }

#ifdef HINTS
  assert (s != NULL);

//  fprintf (stderr, "   %s for object id = %d <%s>\n", is_del ? "del" : "add", local_id, s);

  long long *v = gen_hashes (s);
  int i;
  for (i = 0; v[i]; i++) {
    if (is_del) {
      htbl_del (&u->pref_table, v[i], local_id);
    } else {
      htbl_add (&u->pref_table, v[i], local_id);
    }
  }

  if (is_del) {
    if (!s_in_mtf) {
      dl_free (s, (strlen (s) + 1) * sizeof (char));
    }
    *s_ptr = NULL;
  }
#endif
}

// TODO object_id may be long long
#ifdef HINTS
int user_add_object (user *u, int type, long long object_id, char *buf) {
#else
int user_add_object (user *u, int type, long long object_id) {
#endif
  if (!check_type (type) || !check_object_id (object_id)) {
    return 0;
  }
  if (write_only || no_changes) {
    return 1;
  }

#ifdef HINTS
  changes_memory -= dl_get_memory_used();
  char *clone = prepare_str (buf);
  changes_memory += dl_get_memory_used();
  if (clone == NULL) {
    if (verbosity > 1) {
      fprintf (stderr, "botva %s\n", buf);
    }
    return 0;
  }

//  fprintf (stderr, "add[%lld] %d:%d <%s>|<%s> %p\n", user_table.rev[u - users], type, (int)object_id, clone, buf, clone);
  chg_list_add_string (&u->chg_list_st, &u->chg_list_en, +type, (int)object_id, clone);
#else
  chg_list_add_string (&u->chg_list_st, &u->chg_list_en, +type, (int)object_id, NULL);
#endif
  changes_count++;

  return 1;
}

int user_set_object_type (user *u, int object_type, long long object_id, int new_object_type) {
  if (!check_type (object_type) || !check_type (new_object_type) || !check_object_id (object_id)) {
    return 0;
  }
  if (write_only || no_changes) {
    return 1;
  }

  chg_list_add_int (&u->chg_list_st, &u->chg_list_en, object_type + 256, (int)object_id, new_object_type);
  changes_count++;

  return 1;
}

int user_del_object (user *u, int type, long long object_id, int force) {
  if (!check_type (type) || !check_object_id (object_id)) {
    return 0;
  }
  if (!force) {
    if (write_only || no_changes) {
      return 1;
    }
  }


  chg_list_add_string (&u->chg_list_st, &u->chg_list_en, -type, (int)object_id, NULL);
  changes_count++;

  return 1;
}

void user_apply_changes (user *u) {
  assert (u->flags & USER_LOADED);
  while (u->chg_list_cur->next != NULL || u->chg_list_global->next != NULL) {
    if (u->chg_list_global->next == NULL || (u->chg_list_cur->next != NULL &&
        u->chg_list_cur->next->number < u->chg_list_global->next->number)) {
      u->chg_list_cur = u->chg_list_cur->next;
      user_do_change (u, u->chg_list_cur);
    } else {
      u->chg_list_global = u->chg_list_global->next;
      int type = u->chg_list_global->type;
      if (type >= 256) {
        type -= 256;
      } else if (type < 0) {
        type = -type;
      }
      if (user_has_object (u, type, u->chg_list_global->x)) {
        user_do_change (u, u->chg_list_global);
      }
    }
  }
}

int *buff;

#ifdef HINTS
int *user_find_words (user *u, int *v, int type, int need_latin) {
#else
int *user_find_words (user *u, int type) {
#endif
  int i;

  int l, r;
  if (type == -1) {
    l = 0;
    r = u->object_old_n;
  } else {
    i = 0;
    while (i < u->types_cnt && u->known_types[i] != type) {
      i++;
    }
    if (i == u->types_cnt) {
      l = r = 0;
    } else {
      int *types_bound = (int *)(u->known_types + u->types_cnt);
      r = types_bound[i];
      if (i == 0) {
        l = 0;
      } else {
        l = types_bound[i - 1];
      }
    }
  }

#ifdef HINTS
  int pluses = 0;
  for (i = 0; v[i] && pluses < 5; i++) {
    pluses += v[i] == '+';
  }
  words_per_request[pluses]++;

//  fprintf (stderr, "%d %d\n", l, r);
//  fprintf (stderr, "find : %s\n", s);
  if (v[0] == 0) {
#endif
    buff[0] = 0;
    int object_n = u->object_old_n + u->object_table.currId - 1;

    for (i = l + 1; i <= r && buff[0] < MAX_CNT; i++) {
      if (user_has_local_object (u, i)) {
        buff[++buff[0]] = i;
      }
    }

    for (i = u->object_old_n + 1; i <= object_n && buff[0] < MAX_CNT; i++) {
      if (user_has_local_object (u, i)) {
        buff[++buff[0]] = i;
      }
    }

    assert (buff[0] <= MAX_CNT);
    if (verbosity > 1) {
      fprintf (stderr, "%d\n", buff[0]);
    }
    return buff;
#ifdef HINTS
  }

  static uni_iterator g[MAX_WORDS];

  int gn = 0;

  LIST tmp_list;
  changes tmp_changes, *ttt;

  int st = 0;

  i = 0;
  while (v[i] && gn + 1 < MAX_WORDS) {
    if (v[i] == '+') {
      v[i] = 0;
      long long h[MAX_NAME_SIZE];
      int hn, thn;

      if (v[st] == 160 || need_latin) {
        h[0] = 0;
        while (v[st]) {
          h[0] = h[0] * HASH_MUL + v[st++];
        }
        hn = 1;
      } else {
        translit_from_en_to_ru (v + st, h, &hn);
        translit_from_ru_to_en (v + st, h + hn, &thn);
        hn += thn;
      }

      st = i + 1;

      assert (hn < MAX_NAME_SIZE);

      int j;
      int k;
      long long tmp;
      for (j = 0; j + 1 < hn; j++) {
        for (k = hn - 1; k > j; k--) {
          if (h[k - 1] > h[k]) {
            tmp = h[k - 1];
            h[k - 1] = h[k];
            h[k] = tmp;
          }
        }
      }

      thn = 0;
      for (j = 0; j < hn; j++) {
        if (j == 0 || h[j] != h[j - 1]) {
          h[thn++] = h[j];
        }
      }
      hn = thn;

      if (hn > MAX_WORDS) {
        fprintf (stderr, "Max number of transliterations exceeded on request");
        for (j = st; j < i; j++) {
          fprintf (stderr, " %d", v[j]);
        }
        fprintf (stderr, "\n");
      }

      g[gn].n = 0;
      g[gn].val = -1;
      g[gn].l = l;
      g[gn].r = r;

      for (j = 0; j < hn && j < MAX_WORDS; j++) {
        int len;
        tmp_list = mtf_get (u, h[j], &len);
//        fprintf (stderr, "search changes hash = %lld\n", h[j]);
        ttt = htbl_find (&u->pref_table, h[j]);
        if (ttt != NULL) {
          tmp_changes = *ttt;
        } else {
          tmp_changes = NULL;
        }

//      fprintf (stderr, "search (%lld) list:%lld changes:%lld\n", h[j], (long long)tmp_list, (long long)tmp_changes);
        uni_iter_add (&g[gn], tmp_list, tmp_changes, u->object_old_n, len);
      }
//      fprintf (stderr, "----%d\n", g[gn].n);
      if (g[gn].n) {
        gn++;
      } else {
        gn = 0;
        break;
      }

      v[i] = '+';
    }
    i++;
  }

  return uni_iter_intersect (g, gn, MAX_CNT);
#endif
}

void fix_down (user *u, int *heap, int heap_size, int num) {
  int j, k, t;

  k = 1;
  while (1) {
    j = k;
    t = j * 2;
    if (t <= heap_size && object_cmp (u, heap[t], heap[k], num) < 0) {
      k = t;
    }
    t++;
    if (t <= heap_size && object_cmp (u, heap[t], heap[k], num) < 0) {
      k = t;
    }
    if (k != j) {
      t = heap[k], heap[k] = heap[j], heap[j] = t;
    } else {
      break;
    }
  }
}

#ifdef HINTS
inline int bad_letters (int x) {
  switch (x) {
  case 1093://õ
  case 1098://ú
  case 1078://æ
  case 1101://ý
  case 1073://á
  case 1102://þ
  case 1105://¸
  case 1061://Õ
  case 1066://Ú
  case 1046://Æ
  case 1069://Ý
  case 1041://Á
  case 1070://Þ
  case 1025://¨
    return 1;
  }
  return 0;
}
#endif



#define MAX_K 6
rating *dp[MAX_K + 1], *d;

//choose $k random elements from $n elements of array $a with 
//weights $weight and write result to array $res
//also may change in any way arrays $a and $weight
//returns number of choosed objects = min (n, k)
int get_random (int k, int n, int *a, rating *weight, int *res) {
  assert (k <= MAX_CNT && n <= MAX_CNT);

/*
#define MAX_T 100000
#define MUL_T 0.39894228040143267793994605993438

  static int inited = 0;
  static double integral[MAX_T + 1];

  if (!inited) {
    double t = 0.0, kh = 5.0, h = kh / MAX_T;
    int k;

    for (k = MAX_T; k >= 0; k--, kh -= h) {
      t += MUL_T * h * exp (-0.5 * kh * kh);
      integral[k] = t;
    }
    inited = 1;
  }
*/

  int i, j;
  if (k >= n || k <= 0) {
    for (j = 0; j < n; j++) {
      res[j] = a[j];
    }
    for (j = 0; j < n; j++) {
      int t = res[j];
      int r = rand() % (j + 1);
      res[j] = res[r];
      res[r] = t;
    }
    return n;
  }

  assert (n > 1);
  if (!fading) {
    float d = 0.0;
    for (j = 0; j < n; j++) {
      d += weight[j] * weight[j];
    }
    d = sqrt (d / (n - 1));

    if (fabs (d) < 1e-5) {
      for (j = 0; j < n; j++) {
        weight[j] = 1.0;
      }
    } else {
      d = 1.0 / d;
      rating min_weight = k * 1.0 / n, max_weight = n * 1.0 / k;
      for (j = 0; j < n; j++) {
        rating x = weight[j] * d;
        // fprintf (stderr, "%d %lf\n", j, (double)weight[j]);
        if (x < 0) {
          weight[j] = exp (-x * x);
          if (weight[j] < min_weight) {
            weight[j] = min_weight;
          }
        } else {
          weight[j] = exp (x * x);
          if (weight[j] > max_weight) {
            weight[j] = max_weight;
          }
        }

        // fprintf (stderr, "%d %lf\n", j, (double)weight[j]);
      }
    }
/*    rating mi = weight[0], ma = weight[0];
    for (j = 1; j < n; j++) {
      if (weight[j] > ma) {
        ma = weight[j];
      }
      if (weight[j] < mi) {
        mi = weight[j];
      }
    }

    if (fabs (mi - ma) < 1e-5) {
      for (j = 0; j < n; j++) {
        weight[j] = 1.0;
      }
    } else {
      rating max_weight = n / 6;
      if (max_weight < 3) {
        max_weight = 3;
      }
      for (j = 0; j < n; j++) {
        weight[j] = (weight[j] - mi) / (ma - mi) * max_weight;
        if (weight[j] < 1) {
          weight[j] = 1;
        }
      }
    }*/
  } else {
    for (i = 0; i < n; i++) {
      if (weight[i] < 0.1f) {
        weight[i] = 0.1f;
      }
    }
  }

  int fast = (long long)2 * k * k < n;
  if (k > MAX_K || fast) {
    rating *dp0 = dp[0];

    if ((long long)k * n > 1000000 || fast) {
      dp0[0] = 0;
      for (j = 0; j < n; j++) {
        dp0[j + 1] = weight[j] + dp0[j];
      }

      for (j = 1; j <= n; j++) {
        dp0[j] /= dp0[n];
      }

      for (i = 0; i < k; ) {
        rating R = (rand() * 1.0 + 0.5) / (RAND_MAX + 1.0);
        int l = 0, r = n;
        while (l + 1 < r) {
          int m = (l + r) >> 1;
          if (dp0[m] > R) {
            r = m;
          } else {
            l = m;
          }
        }
        if (weight[l] >= 0) {
          res[i++] = l;
          weight[l] = -1;
        } else if (fast) {
          while (--i >= 0) {
            weight[res[i]] = 1;
          }
          i++;
        }
      }

      for (i = 0; i < k; i++) {
        res[i] = a[res[i]];
      }
      return k;
    }

    for (j = 0; j <= n; j++) {
      dp0[j] = 1;
    }
    d[0] = 1;

    for (i = 0; i < k; i++) {
      rating prev = dp0[0], new_prev;
      dp0[0] = 0;
      for (j = 0; j < n; j++) {
        new_prev = dp0[j + 1];
        dp0[j + 1] = weight[j] * prev + dp0[j];
        prev = new_prev;
      }
      d[i + 1] = dp0[n];
    }

    dp0[0] = 0;
    for (j = 0; j < n; j++) {
      rating f = 1, w = weight[j];
      for (i = 1; i <= k; i++) {
        f = d[i] - w * f;
      }
      dp0[j + 1] = d[k] - f + dp0[j];
    }

    for (j = 1; j <= n; j++) {
      dp0[j] /= dp0[n];
    }

    for (i = 0; i < k; ) {
      rating R = (rand() * 1.0 + 0.5) / (RAND_MAX + 1.0);
      int l = 0, r = n;
      while (l + 1 < r) {
        int m = (l + r) >> 1;
        if (dp0[m] > R) {
          r = m;
        } else {
          l = m;
        }
      }
      if (weight[l] >= 0) {
        res[i++] = a[l];
        weight[l] = -1;
      }
    }
  } else {
    for (j = 0; j <= n; j++) {
      dp[0][j] = 1;
    }

    for (i = 1; i <= k; i++) {
      dp[i][0] = 0.0;
      for (j = 1; j <= n; j++) {
        dp[i][j] = weight[j - 1] * dp[i - 1][j - 1] + dp[i][j - 1];
      }
    }

    for (i = 0; i <= k; i++) {
      dp[i][0] = dp[i][n];
    }

    for (j = 1; j <= n; j++) {
      rating w = weight[j - 1];
      for (i = 1; i <= k; i++) {
        dp[i][j] = dp[i][0] - w * dp[i - 1][j];
      }
    }

    for (i = k; i >= 1; i--) {
      rating R = (rand() * 1.0 + 0.5) / (RAND_MAX + 1.0) * dp[i][0] * i;

      int r = 0;
      while (R > 0 && r < n) {
        R -= dp[i][0] - dp[i][++r];
      }
      assert (r > 0);

      res[i - 1] = a[r - 1];
      int l;
      for (l = 0; l < i; l++) {
        dp[l][0] = dp[l][r];
        dp[l][r] = dp[l][n];
      }
      rating w = weight[r - 1];
      for (j = 1; j < n; j++) {
        for (l = 1; l < i; l++) {
          dp[l][j] -= w * dp[l - 1][j];
        }
      }
      a[r - 1] = a[n - 1];
      weight[r - 1] = weight[n - 1];
      n--;
    }
  }
  return k;
/*
#undef MAX_T
#undef MUL_T
*/
}

int *heap;
rating *weight;

long long *objects_typeids_to_sort;
int *objects_to_sort;

int sort_user_objects (int user_id, int object_cnt, long long *obj, int max_cnt, int num, int need_rand) {
  if (!check_user_id (user_id) || !check_rating_num (num)) {
    bad_requests++;
    return -1;
  }

  int local_user_id = get_local_user_id (user_id);
  if (local_user_id == 0) {
    return 0;
  }
  assert (local_user_id > 0);

  user *u = conv_user_id (user_id);
  assert (u != NULL);

  if (load_user_metafile (u, local_user_id, NOAIO) == NULL) {
    return -2;
  }

  assert (obj != NULL);

  int i, j, k, t;

  if (object_cnt > MAX_CNT) {
    object_cnt = MAX_CNT;
  }

  if (max_cnt > MAX_CNT) {
    max_cnt = MAX_CNT;
  }

  if (max_cnt < 0) {
    max_cnt = 0;
  }

  int n = 0;
  for (i = 0; i < object_cnt; i++) {
    int lid = user_get_object_local_id_type_id (u, obj[i]);
    if (lid) {
      objects_to_sort[n++] = lid;
    }
  }

  int heap_size = 0;
  if (max_cnt) {
    for (i = 0; i < n; i++) {
      if (need_rand) {
        heap[++heap_size] = objects_to_sort[i];
      } else {
        if (heap_size < max_cnt) {
          heap[++heap_size] = objects_to_sort[i];
          j = heap_size;
          while (j > 1 && object_cmp (u, heap[j], heap[k = j / 2], num) < 0) {
            t = heap[j], heap[j] = heap[k], heap[k] = t;
            j = k;
          }
        } else if (object_cmp (u, heap[1], objects_to_sort[i], num) < 0) {
          heap[1] = objects_to_sort[i];
          fix_down (u, heap, heap_size, num);
        }
      }
    }
  }

  if (need_rand) {
    for (i = 1; i <= heap_size; i++) {
      weight[i - 1] = user_get_object_weight (u, heap[i], num);
    }
    n = get_random (max_cnt, heap_size, heap + 1, weight, objects_to_sort);
  } else {
    n = heap_size;
    while (heap_size) {
      objects_to_sort[heap_size - 1] = heap[1];
      heap[1] = heap[heap_size--];
      fix_down (u, heap, heap_size, num);
    }
  }

  for (i = 0; i < n; i++) {
    obj[i] = user_get_object_type_id (u, objects_to_sort[i]);
  }

  return n;
}


#ifdef HINTS
int *user_get_hints (user *u, char *query, int type, int max_cnt, int num, int need_latin, int *ans_len, int *found_cnt) {
#else
int *user_get_hints (user *u, int *exc, int exc_cur, int type, int max_cnt, int num, int need_rand, int *ans_len, int *found_cnt) {
#endif
  if (max_cnt > MAX_CNT) {
    max_cnt = MAX_CNT;
  }

  if (max_cnt < 0) {
    max_cnt = 0;
  }

  int *ans = NULL;
  int heap_size = 0, i;
  *found_cnt = 0;
#ifdef HINTS
  static int v[MAX_NAME_SIZE];
  string_to_utf8 ((unsigned char *)query, v);

  int try;
  for (try = 0; try < 2; try++) {
    int *s = prepare_str_UTF8 (v);
    assert (s != NULL);
#endif

    if (verbosity > 3) {
      fprintf (stderr, "start user find words\n");
    }

#ifdef HINTS
    ans = user_find_words (u, s, type, need_latin);
#else
    ans = user_find_words (u, type);
#endif

    if (verbosity > 3) {
      fprintf (stderr, "finish user find words\n");
    }
//    fprintf (stderr, "a[0] = %d\n", ans[0]);

    for (i = 1; i <= ans[0]; i++) {
      if (type == -1 || ans[i] <= u->object_old_n || TYPE(ltbl_get_rev (&u->object_table, ans[i] - u->object_old_n)) == type) {
#ifdef NOHINTS
        if (exc[ans[i]] != exc_cur) {
          if (need_rand) {
            heap[++heap_size] = ans[i];
            (*found_cnt)++;
          } else {
#endif
            if (heap_size < max_cnt) {
              int j, k, t;

              heap[++heap_size] = ans[i];
              j = heap_size;
              while (j > 1 && object_cmp (u, heap[j], heap[k = j / 2], num) < 0) {
                t = heap[j], heap[j] = heap[k], heap[k] = t;
                j = k;
              }
            } else if (object_cmp (u, heap[1], ans[i], num) < 0) {
              heap[1] = ans[i];
              fix_down (u, heap, heap_size, num);
            }
            (*found_cnt)++;
#ifdef NOHINTS
          }
        }
#endif
      }
    }

#ifdef HINTS
    if (*found_cnt == 0 && !need_latin) {
      int inside_tag = 0;
      for (i = 0; v[i]; i++) {
        if (bad_letters (v[i])) {
          try = 1;
          break;
        }

        if (v[i] == '+') {
          inside_tag = 0;
        }
        if (v[i] == 160 && (i == 0 || v[i - 1] == '+')) {
          inside_tag = 1;
        }
        if (!inside_tag) {
          v[i] = convert_language (v[i]);
        }
      }
    } else {
      try = 1;
    }
  }
#endif

#ifdef NOHINTS
  if (need_rand) {
    for (i = 1; i <= heap_size; i++) {
      weight[i - 1] = user_get_object_weight (u, heap[i], num);
    }
    *ans_len = get_random (max_cnt, heap_size, heap + 1, weight, ans);
  } else {
#endif
    *ans_len = heap_size;
    while (heap_size) {
      ans[heap_size - 1] = heap[1];
      heap[1] = heap[heap_size--];
      fix_down (u, heap, heap_size, num);
    }
#ifdef NOHINTS
  }
#endif

  return ans;
}


#ifdef NOHINTS
int *exc;
int exc_cur;
#endif

#ifdef HINTS
int get_user_hints (int user_id, int max_buf_len, char *buf, int type, int max_cnt, int num, int need_rating, int need_text, int need_latin, int need_raw_format) {
#else
int get_user_hints (int user_id, int max_buf_len, char *buf, int type, int max_cnt, int num, int need_rating, int need_rand, int need_raw_format) {
#endif
  if (!check_user_id (user_id) || max_buf_len <= 2 || (type != -1 && !check_type (type)) || !check_rating_num (num)) {
    bad_requests++;
    return -1;
  }

#ifdef HINTS
  if (strlen (buf) > MAX_NAME_SIZE - 2) {
    bad_requests++;
    return -1;
  }
#endif

  int local_user_id = get_local_user_id (user_id);
  if (local_user_id == 0) {
    buf[0] = '0';
    buf[1] = 0;
    return 1;
  }
  assert (local_user_id > 0);

  user *u = conv_user_id (user_id);
  assert (u != NULL);

  if (load_user_metafile (u, local_user_id, NOAIO) == NULL) {
    return -2;
  }

#ifdef NOHINTS
  if (++exc_cur == 2000000000) {
    exc_cur = 1;
    memset (exc, 0, sizeof (int) * (MAX_CNT + 1));
  }
  int add = 0, cur_add = -1;
  do {
    add += cur_add + 1;

    int exc_type;
    long long exc_object_id;
    if (sscanf (buf + add, "%d,%lld%n", &exc_type, &exc_object_id, &cur_add) == 2) {
      exc[user_get_object_local_id (u, exc_type, exc_object_id)] = exc_cur;
    }
  } while (buf[add + cur_add] == ',');
#endif

  int n, found_cnt;
#ifdef HINTS
  int *ans = user_get_hints (u, buf, type, max_cnt, num, need_latin, &n, &found_cnt);
#else
  int *ans = user_get_hints (u, exc, exc_cur, type, max_cnt, num, need_rand, &n, &found_cnt);
#endif
  assert (ans != NULL);

  //<$found_cnt>,<$type1>,<$object_id1>,<$type2>,<$object_id2>,...\n

  if (need_raw_format) {
    int *st = (int *)buf,
        *ptr = st;
    *ptr++ = found_cnt;

    assert (sizeof (int) == sizeof (float));
    int i;
    for (i = 0; i < n; i++) {
      long long h = user_get_object_type_id (u, ans[i]);
      *ptr++ = TYPE(h);
      *ptr++ = ID(h);

      float rating = (float)user_get_now_object_rating (u, ans[i], num);
      //fprintf (stderr, "RATING = %lf --> %d\n", rating, (int)(rating * 1000));
      *(float *)ptr = rating;
      ptr++;

      if (ptr - st > max_buf_len / sizeof (int) - 1024) {
        fprintf (stderr, "Output limit exceeded.\n");
        break;
      }
    }
    return (ptr - st) * sizeof (int);
  } else {
    int len = sprintf (buf, "%d", found_cnt);
    int i;
    for (i = 0; i < n; i++) {
      long long h = user_get_object_type_id (u, ans[i]);
      len += sprintf (buf + len, ",%d,%d", TYPE(h), ID(h));
      if (need_rating) {
        len += sprintf (buf + len, ",%.6lf", (double)user_get_now_object_rating (u, ans[i], num));
      }
#ifdef HINTS
      if (need_text) {
        len += sprintf (buf + len, ",%s", clean_str (user_get_object_name (u, ans[i])));
      }
#endif
      if (len > max_buf_len - 10000) {
        fprintf (stderr, "Output limit exceeded.\n");
        break;
      }
//      fprintf (stderr, "%s %e\n", user_get_object_name (u, user_get_object_local_id_type_id (u, h)), user_get_now_object_rating (u, ans[i], num));
    }
    return len;
  }
}

#ifdef HINTS
int rpc_get_user_hints (int user_id, int query_len, char *query, int type, int max_cnt, int num, int need_rating, int need_text, int need_latin) {
#else
int rpc_get_user_hints (int user_id, int exceptions_cnt, long long *exceptions, int type, int max_cnt, int num, int need_rating, int need_rand) {
#endif
  if (!check_user_id (user_id) || (type != -1 && !check_type (type)) || !check_rating_num (num)) {
    bad_requests++;
    return -1;
  }

#ifdef HINTS
  if (query_len > MAX_NAME_SIZE - 2) {
    bad_requests++;
    return -1;
  }
#endif

  tl_store_int (TL_VECTOR_TOTAL);
  
  int local_user_id = get_local_user_id (user_id);
  if (local_user_id == 0) {
    tl_store_int (0);
    tl_store_int (0);
    return 0;
  }
  assert (local_user_id > 0);

  user *u = conv_user_id (user_id);
  assert (u != NULL);

  if (load_user_metafile (u, local_user_id, NOAIO) == NULL) {
    return -2;
  }

  int i;
#ifdef NOHINTS
  if (++exc_cur == 2000000000) {
    exc_cur = 1;
    memset (exc, 0, sizeof (int) * (MAX_CNT + 1));
  }
  for (i = 0; i < exceptions_cnt; i++) {
    exc[user_get_object_local_id_type_id (u, exceptions[i])] = exc_cur;
  }
#endif

  int n, found_cnt;
#ifdef HINTS
  int *ans = user_get_hints (u, query, type, max_cnt, num, need_latin, &n, &found_cnt);
#else
  int *ans = user_get_hints (u, exc, exc_cur, type, max_cnt, num, need_rand, &n, &found_cnt);
#endif
  assert (ans != NULL);

  tl_store_int (found_cnt);
  tl_store_int (n);

  for (i = 0; i < n; i++) {
    long long h = user_get_object_type_id (u, ans[i]);
    tl_store_int (TYPE(h));
    tl_store_int (ID(h));

    if (need_rating) {
      tl_store_double (user_get_now_object_rating (u, ans[i], num));
    }
#ifdef HINTS
    if (need_text) {
      const char *text = clean_str (user_get_object_name (u, ans[i]));
      tl_store_string (text, strlen (text));
    }
#endif
//      fprintf (stderr, "%s %e\n", clean_str (user_get_object_name (u, user_get_object_local_id_type_id (u, h))), user_get_now_object_rating (u, ans[i], num));
  }
  return n;
}


int get_user_info (int user_id) {
  int local_user_id = get_local_user_id (user_id);
  if (local_user_id == -1) {
    return -3;
  }

  int result;
  if (local_user_id == 0) {
    result = -1;
  } else {
    user *cur_user = conv_user_id (user_id);
    assert (cur_user);

    result = GET_USER_INFO(cur_user->flags);
  }

  if (result == -1 && get_changes_memory() > max_memory * MEMORY_CHANGES_PERCENT) {
    result = -2;
  }

  return result;
}


int user_set_object_rating (user *u, unsigned char object_type, long long object_id, rating val, int num) {
  if (!check_rating (val) || !check_type (object_type) || !check_object_id (object_id) || !check_rating_num (num)) {
    return 0;
  }
  if (write_only || no_changes) {
    return 1;
  }

  if (1333411200 <= now && now <= 1334016000 && val > 1e-3 && object_type == 21 && num == 0) {
    return 1;
  }

  chg_list_add_rating (&u->chg_list_st, &u->chg_list_en, + (object_type + 256 * num + 256 * MAX_RATING_NUM), -(int)(object_id), val);
  changes_count++;

  return 1;
}

int add_user_object (struct lev_hints_add_user_object *E) {
  if (!check_text_len (E->text_len)) {
    return 0;
  }

  user *cur_user = conv_user_id (E->user_id);
  if (cur_user != NULL) {
#ifdef HINTS
    return user_add_object (cur_user, E->object_type, E->object_id, E->text);
#else
    return user_add_object (cur_user, E->object_type, E->object_id);
#endif
  }

  return 0;
}

int add_user_object_rating (struct lev_hints_add_user_object_rating *E) {
  int cur_rating_num = E->type - LEV_HINTS_ADD_USER_OBJECT_RATING;
  int text_len = E->text_len - sizeof (float) * cur_rating_num;

  if (!check_text_len (text_len)) {
    return 0;
  }

  user *cur_user = conv_user_id (E->user_id);
  if (cur_user != NULL) {
    int res, num;
    float *ratings;
#ifdef HINTS
    res = user_add_object (cur_user, E->object_type, E->object_id, E->text);
#else
    res = user_add_object (cur_user, E->object_type, E->object_id);
#endif
    ratings = (float *)(E->text + 1 + text_len);

    for (num = 0; num < rating_num && num < cur_rating_num; num++) {
      res &= user_set_object_rating (cur_user, E->object_type, E->object_id, (rating)ratings[num], num);
    }

    return res;
  }

  return 0;
}

#ifdef HINTS
int do_add_user_object (int user_id, int object_type, long long object_id, int text_len, char *text) {
  if (!check_text_len (text_len)) {
    return 0;
  }
#else
int do_add_user_object (int user_id, int object_type, long long object_id) {
#endif
  if (!check_type (object_type) || !check_object_id (object_id)) {
    return 0;
  }

  struct lev_hints_add_user_object *E =
#ifdef HINTS
    alloc_log_event (LEV_HINTS_ADD_USER_OBJECT, offsetof (struct lev_hints_add_user_object, text) + 1 + text_len, 0);
#else
    alloc_log_event (LEV_HINTS_ADD_USER_OBJECT, offsetof (struct lev_hints_add_user_object, text), 0);
#endif

  E->user_id = user_id;
  E->object_type = object_type;
  E->object_id = object_id;
#ifdef HINTS
  E->text_len = text_len;
  memcpy (E->text, text, sizeof (char) * (text_len + 1));
#else
  E->text_len = -1;
#endif

  return add_user_object (E);
}

int set_user_object_type (struct lev_hints_set_user_object_type *E) {
  user *cur_user = conv_user_id (E->user_id);
  if (cur_user != NULL) {
    return user_set_object_type (cur_user, (E->type >> 8) & 255, E->object_id, E->type & 255);
  }

  return 0;
}

int do_set_user_object_type (int user_id, int object_type, long long object_id, int new_object_type) {
  if (!check_type (object_type) || !check_type (new_object_type) || !check_object_id (object_id)) {
    return 0;
  }

  struct lev_hints_set_user_object_type *E =
    alloc_log_event (LEV_HINTS_SET_USER_OBJECT_TYPE + 256 * object_type + new_object_type, sizeof (struct lev_hints_set_user_object_type), 0);

  E->user_id = user_id;
  E->object_id = object_id;

  return set_user_object_type (E);
}

#ifdef HINTS
int add_object_text (struct lev_hints_add_object_text *E) {
  if (!check_type (E->object_type) || !check_object_id (E->object_id) || !check_text_len (E->text_len)) {
    return 0;
  }
  if (write_only || no_changes) {
    return 1;
  }

  changes_memory -= dl_get_memory_used();
  char *clone = prepare_str (E->text);
  changes_memory += dl_get_memory_used();
  if (clone == NULL) {
    if (verbosity > 1) {
      fprintf (stderr, "botva %s\n", E->text);
    }
    return 0;
  }

  stat_global[(int)E->object_type][1]++;

  chg_list_add_string (&global_changes_st, &global_changes_en, (int)E->object_type, (int)E->object_id, clone);
  changes_count++;

  return 1;
}

int do_add_object_text (int object_type, long long object_id, int text_len, char *text) {
  if (!check_type (object_type) || !check_text_len (text_len) || !check_object_id (object_id)) {
    return 0;
  }

  struct lev_hints_add_object_text *E =
    alloc_log_event (LEV_HINTS_ADD_OBJECT_TEXT, offsetof (struct lev_hints_add_object_text, text) + 1 + text_len, 0);

  E->object_type = object_type;
  E->object_id = object_id;
  E->text_len = text_len;
  memcpy (E->text, text, sizeof (char) * (text_len + 1));

  return add_object_text (E);
}
#endif

int set_object_type (struct lev_hints_set_object_type *E) {
  int object_type = (E->type >> 8) & 255;
  int new_object_type = E->type & 255;
  if (!check_object_id (E->object_id) || object_type == 0 || new_object_type == 0) {
    return 0;
  }

  if (write_only || no_changes || object_type == new_object_type) {
    return 1;
  }

  stat_global[object_type][2]++;

  chg_list_add_int (&global_changes_st, &global_changes_en, object_type + 256, (int)E->object_id, new_object_type);
  changes_count++;

  return 1;
}

int do_set_object_type (int object_type, long long object_id, int new_object_type) {
  if (!check_type (object_type) || !check_type (new_object_type) || !check_object_id (object_id)) {
    return 0;
  }
  if (object_type == new_object_type) {
    return 1;
  }

  struct lev_hints_set_object_type *E =
    alloc_log_event (LEV_HINTS_SET_OBJECT_TYPE + 256 * object_type + new_object_type, sizeof (struct lev_hints_set_object_type), 0);

  E->object_id = object_id;

  return set_object_type (E);
}

int del_user_object (struct lev_hints_del_user_object *E) {
  user *cur_user = conv_user_id (E->user_id);

  if (cur_user != NULL) {
    return user_del_object (cur_user, E->type & 0xff, E->object_id, 0);
  }

  return 0;
}

int do_del_user_object (int user_id, int object_type, long long object_id) {
  if (!check_type (object_type) || !check_object_id (object_id)) {
    return 0;
  }

  struct lev_hints_del_user_object *E =
    alloc_log_event (LEV_HINTS_DEL_USER_OBJECT + object_type, sizeof (struct lev_hints_del_user_object), 0);

  E->user_id = user_id;
  E->object_id = object_id;

  return del_user_object (E);
}

int del_object_text (struct lev_hints_del_object_text *E) {
  if ((E->type & 0xff) == 0 || !check_object_id (E->object_id)) {
    return 0;
  }

  if (write_only || no_changes) {
    return 1;
  }

  stat_global[E->type & 0xff][0]++;

  chg_list_add_string (&global_changes_st, &global_changes_en, -(E->type & 0xff), (int)E->object_id, NULL);
  changes_count++;

  return 1;
}

int do_del_object_text (int object_type, long long object_id) {
  if (!check_type (object_type) || !check_object_id (object_id)) {
    return 0;
  }

  struct lev_hints_del_object_text *E =
    alloc_log_event (LEV_HINTS_DEL_OBJECT_TEXT + object_type, sizeof (struct lev_hints_del_object_text), 0);

  E->object_id = object_id;

  return del_object_text (E);
}

int set_user_info (struct lev_hints_set_user_info *E) {
  user *cur_user = conv_user_id (E->user_id);

  if (cur_user == NULL) {
    return 0;
  }

  cur_user->flags = SET_USER_INFO(cur_user->flags, (E->type & 0xff) - 2);

  return 1;
}

int do_set_user_info (int user_id, int info) {
  if (info > 13 || info < -2 || info == 0) {
    return 0;
  }

  struct lev_hints_set_user_info *E =
    alloc_log_event (LEV_HINTS_SET_USER_INFO + info + 2, sizeof (struct lev_hints_set_user_info), user_id);

  E->user_id = user_id;

  return set_user_info (E);
}

int set_user_object_rating (struct lev_hints_set_user_object_rating *E) {
  user *cur_user = conv_user_id (E->user_id);

  if (cur_user != NULL) {
    int num = (E->type - LEV_HINTS_SET_USER_OBJECT_RATING) >> 8;

    return user_set_object_rating (cur_user, E->type & 0xff, E->object_id, E->val, num);
  }

  return 0;
}

int do_set_user_object_rating (int user_id, int object_type, long long object_id, float new_rating, int num) {
  if (!check_type (object_type) || !check_rating_num (num) || !check_object_id (object_id) || !check_rating (new_rating)) {
    return 0;
  }

  struct lev_hints_set_user_object_rating *E =
    alloc_log_event (LEV_HINTS_SET_USER_OBJECT_RATING + (num << 8) + object_type, sizeof (struct lev_hints_set_user_object_rating), 0);

  E->user_id = user_id;
  E->object_id = object_id;
  E->val = new_rating;

  return set_user_object_rating (E);
}

int user_object_rating_increment (user *u, unsigned char object_type, long long object_id, int cnt, int num) {
  if (!check_rating (cnt) || object_type == 0 || !check_object_id (object_id) || !check_rating_num (num)) {
    return 0;
  }

  if (write_only || no_changes) {
    return 1;
  }

  chg_list_add_int (&u->chg_list_st, &u->chg_list_en, +(object_type + 256 * num), -(int)(object_id), cnt);
  changes_count++;

  return 1;
}

int increment_user_object_rating (struct lev_hints_increment_user_object_rating *E) {
  user *cur_user = conv_user_id (E->user_id);
  if (cur_user != NULL) {
    return user_object_rating_increment (cur_user, E->type & 0xff, E->object_id, E->cnt, (E->type - LEV_HINTS_INCREMENT_USER_OBJECT_RATING) >> 8);
  }

  return 0;
}

int increment_user_object_rating_char (struct lev_hints_increment_user_object_rating_char *E) {
  user *cur_user = conv_user_id (E->user_id);
  if (cur_user != NULL) {
    return user_object_rating_increment (cur_user, E->type & 0xff, E->object_id, (E->type & 0xff00) >> 8, (E->type - LEV_HINTS_INCREMENT_USER_OBJECT_RATING_CHAR) >> 16);
  }

  return 0;
}

int do_increment_user_object_rating (int user_id, int object_type, long long object_id, int cnt, int num) {
  if (!fading) {
    cnt += 128;
  }
  if (!check_rating (cnt) || !check_type (object_type) || !check_rating_num (num) || !check_object_id (object_id)) {
    return 0;
  }

  if (cnt & -0x100) {
    struct lev_hints_increment_user_object_rating *E =
      alloc_log_event (LEV_HINTS_INCREMENT_USER_OBJECT_RATING + object_type + (num << 8), sizeof (struct lev_hints_increment_user_object_rating), user_id);

    E->user_id = user_id;
    E->object_id = object_id;
    E->cnt = cnt;

    return increment_user_object_rating (E);
  } else {
    struct lev_hints_increment_user_object_rating_char *E =
      alloc_log_event (LEV_HINTS_INCREMENT_USER_OBJECT_RATING_CHAR + object_type + (cnt << 8) + (num << 16), sizeof (struct lev_hints_increment_user_object_rating_char), user_id);

    E->user_id = user_id;
    E->object_id = object_id;

    return increment_user_object_rating_char (E);
  }
}

int nullify_user_rating (struct lev_hints_nullify_user_rating *E) {
  user *cur_user = conv_user_id (E->user_id);
  if (cur_user != NULL) {
    chg_list_add_string (&cur_user->chg_list_st, &cur_user->chg_list_en, 0, 0, NULL);
    return 1;
  }

  return 0;
}

int do_nullify_user_rating (int user_id) {
  struct lev_hints_nullify_user_rating *E =
    alloc_log_event (LEV_HINTS_NULLIFY_USER_RATING, sizeof (struct lev_hints_nullify_user_rating), user_id);

  E->user_id = user_id;

  return nullify_user_rating (E);
}

int user_set_rating_state (struct lev_hints_set_user_rating_state *E) {
  int state = (E->type & 0xff);
  if (state >= 2 || state < 0) {
    return 0;
  }

  user *cur_user = conv_user_id (E->user_id);
  if (cur_user != NULL) {
    chg_list_add_string (&cur_user->chg_list_st, &cur_user->chg_list_en, state + 1, 0, NULL);
    return 1;
  }

  return 0;
}

int do_set_user_rating_state (int user_id, int state) {
  if (state >= 2 || state < 0) {
    return 0;
  }

  struct lev_hints_set_user_rating_state *E =
    alloc_log_event (LEV_HINTS_SET_USER_RATING_STATE + state, sizeof (struct lev_hints_set_user_rating_state), user_id);

  E->user_id = user_id;

  return user_set_rating_state (E);
}

int user_object_winner (struct lev_hints_user_object_winner *E) {
  if (!fading) {
    user *cur_user = conv_user_id (E->user_id);
    if (cur_user != NULL) {
      int num = E->type - LEV_HINTS_USER_OBJECT_WINNER;
      int i;
      int res = user_object_rating_increment (cur_user, E->object_type, E->winner_id, E->losers_cnt + 128, num);
      for (i = 0; i < E->losers_cnt; i++) {
        res = res & user_object_rating_increment (cur_user, E->object_type, E->losers[i], -1 + 128, num);
      }
      return res;
    }
  }
  return 0;
}

int do_user_object_winner (int user_id, int object_type, int num, long long winner, int losers_cnt, int *losers) {
  if (!check_type (object_type) || losers_cnt <= 0 || losers_cnt >= 16000 || winner <= 0 || !check_rating_num (num)) {
    return 0;
  }

  struct lev_hints_user_object_winner *E =
    alloc_log_event (LEV_HINTS_USER_OBJECT_WINNER + num, offsetof (struct lev_hints_user_object_winner, losers) + losers_cnt * sizeof (int), user_id);

  E->user_id = user_id;
  E->winner_id = (int)winner;
  E->losers_cnt = (short)losers_cnt;
  E->object_type = (unsigned char)object_type;
  memcpy (E->losers, losers, losers_cnt * sizeof (int));

  return user_object_winner (E);
}

// BINLOG

int hints_replay_logevent (struct lev_generic *E, int size);

int init_hints_data (int schema) {
  replay_logevent = hints_replay_logevent;

  return 0;
}

void try_init_local_user_id (void) {
  static int was = 0;
  static int old_log_split_min, old_log_split_max, old_log_split_mod;

  if (was) {
//    fprintf (stderr, "%d vs %d | %d vs %d | %d vs %d\n", old_log_split_min, log_split_min, old_log_split_max, log_split_max, old_log_split_mod, log_split_mod);
    assert (old_log_split_min == log_split_min && old_log_split_max == log_split_max && old_log_split_mod == log_split_mod);
    return;
  }

  int i;
  for (i = 1; i <= header.user_cnt; i++) {
    local_user_id (header.user_index[i].id);
  }

  was = 1;
  old_log_split_min = log_split_min;
  old_log_split_max = log_split_max;
  old_log_split_mod = log_split_mod;

  log_schema = HINTS_SCHEMA_V1;
  init_hints_data (log_schema);
}

static int hints_le_start (struct lev_start *E) {
  if (E->schema_id != HINTS_SCHEMA_V1) {
    return -1;
  }

  log_split_min = E->split_min;
  log_split_max = E->split_max;
  log_split_mod = E->split_mod;
  assert (log_split_mod > 0 && log_split_min >= 0 && log_split_min + 1 == log_split_max && log_split_max <= log_split_mod);

  try_init_local_user_id();

  return 0;
}

int _eventsLeft = 0;
//int a[1006];

int hints_replay_logevent (struct lev_generic *E, int size) {
  log_now = now;
//  fprintf (stderr, "hints_replay_logevent %lld\n", log_cur_pos());
//  fprintf (stderr, "%x\n", E->type);
/*  if (a[1005]++ % 100000 == 0) {
    int i;
    for (i=0; i<10; i++)
      fprintf (stderr, "%d ", a[i]);
    fprintf (stderr, "\n");
  }*/

  if (index_mode) {
    if ((_eventsLeft && --_eventsLeft == 0) || get_changes_memory() > MEMORY_CHANGES_PERCENT * max_memory) {
      save_index();
      exit (13);
    }
  }

  int s;
/*
  static int ttt;
  if (E->a == 123456789 && ((E->type & -65536) != LEV_HINTS_INCREMENT_USER_OBJECT_RATING_CHAR)) {
    fprintf (stderr, "%4d %x\n", ttt++, E->type);
    if (E->type == LEV_HINTS_ADD_USER_OBJECT) {
      if (size < sizeof (struct lev_hints_add_user_object)) {
        return -2;
      }
      s = ((struct lev_hints_add_user_object *) E)->text_len;
      if (!check_text_len (s)) {
        struct lev_hints_add_user_object *EE = (struct lev_hints_add_user_object *)E;
        fprintf (stderr, "Bad log event, user_id = %d, object_id = %d, object_type = %d, text_len = %d\n", EE->user_id, EE->object_id, EE->object_type, EE->text_len);
      }
      if (s < -1) {
        return -4;
      }
      s += 1 + offsetof (struct lev_hints_add_user_object, text);
      if (size < s) {
        return -2;
      }
//      struct lev_hints_add_user_object *EE = (struct lev_hints_add_user_object *)E;
//      fprintf (stderr, "      Log event, user_id = %d, object_id = %d, object_type = %d, text_len = %d, text = \"%s\".\n", EE->user_id, EE->object_id, EE->object_type, EE->text_len, EE->text);
    }

    if ((E->type & -256) == LEV_HINTS_DEL_USER_OBJECT) {
      fprintf (stderr, "      Delete object, user_id = %d, object_id = %d, object_type = %d, time = %d.\n", E->a, E->b, E->type & 255, now);
//      if ((E->type & 255) != 10 && (E->type & 255) != 17)
//        fprintf (stderr, "      %d.\n", E->type & 255);
    }
  }
*/
/*
  int cnt = 0;
  if (E->a == 123456789) {
    fprintf (stderr, "%5d [%d] %15s %10d %10d %10d\n", ++cnt, now,
        (E->type >> 16) == 0x39e4 ? "add_user_object" :
        (E->type >> 16) == 0x1124 ? "add_object_text" :
        (E->type >> 16) == 0x6cae ? "set_object_type" :
        (E->type >> 16) == 0x3dad ? "del_user_object" :
        (E->type >> 16) == 0x5be4 ? "del_object_text" :
        (E->type >> 16) == 0x4ad4 ? "  set_user_info" :
        (E->type >> 16) == 0x35a1 ? " set_object_rating" :
        (E->type >> 16) == 0x4f45 ? "    rating_incr" :
        (E->type >> 16) == 0x29ad ? " rating_incr_ch" :
        (E->type >> 16) == 0x4c47 ? " rating_nullify" :
        (E->type >> 16) == 0x2456 ? "      set_state" :
        (E->type >> 16) == 0x6342 ? "         winner" :
        (E->type >> 16) == 0x3712 ? "add_with_rating" :
                                    "        unknown" ,
        E->b, E->c,
        (E->type >> 16) == 0x39e4 ? ((struct lev_hints_add_user_object *)E)->object_type :
        (E->type >> 16) == 0x3dad ? E->type & 255 :
        (E->type >> 16) == 0x29ad ? E->type & 255 :
                                    E->d);
  }*/

  switch (E->type) {
  case LEV_START:
    if (size < 24 || E->b < 0 || E->b > 4096) { return -2; }
    s = 24 + ((E->b + 3) & -4);
    if (size < s) { return -2; }
    return hints_le_start ((struct lev_start *) E) >= 0 ? s : -1;
  case LEV_NOOP:
  case LEV_TIMESTAMP:
  case LEV_CRC32:
  case LEV_ROTATE_FROM:
  case LEV_ROTATE_TO:
  case LEV_TAG:
    return default_replay_logevent (E, size);
  case LEV_HINTS_ADD_USER_OBJECT:
/*    if (E->a == 123456789 && (((struct lev_hints_add_user_object *) E)->object_type == 21 || ((struct lev_hints_add_user_object *) E)->object_type == 11)) {
      char buf[1000];
      dl_print_local_time (buf, 1000, now);
      fprintf (stderr, "at time %s added     %6s %9d\n", buf, ((struct lev_hints_add_user_object *) E)->object_type == 15 ? "public" : "group", E->b);
    }*/
    if (size < (int)sizeof (struct lev_hints_add_user_object)) {
      return -2;
    }
    s = ((struct lev_hints_add_user_object *) E)->text_len;
    if (!check_text_len (s)) {
      struct lev_hints_add_user_object *EE = (struct lev_hints_add_user_object *)E;
      fprintf (stderr, "Bad log event, user_id = %d, object_id = %d, object_type = %d, text_len = %d\n", EE->user_id, EE->object_id, EE->object_type, EE->text_len);
    }
    if (s < -1) {
      return -4;
    }
    s += 1 + offsetof (struct lev_hints_add_user_object, text);
    if (size < s) {
      return -2;
    }
    add_user_object ((struct lev_hints_add_user_object *)E);
    return s;
  case LEV_HINTS_SET_USER_OBJECT_TYPE ... LEV_HINTS_SET_USER_OBJECT_TYPE + 0xffff:
    STANDARD_LOG_EVENT_HANDLER(lev_hints, set_user_object_type);
  case LEV_HINTS_ADD_OBJECT_TEXT:
    if (size < (int)sizeof (struct lev_hints_add_object_text)) {
      return -2;
    }
    s = ((struct lev_hints_add_object_text *) E)->text_len;
    if (!check_text_len (s)) {
      struct lev_hints_add_object_text *EE = (struct lev_hints_add_object_text *)E;
      fprintf (stderr, "Bad log event, object_id = %d, object_type = %d, text_len = %d\n", EE->object_id, EE->object_type, EE->text_len);
    }
    if (s < -1) {
      return -4;
    }
    s += 1 + offsetof (struct lev_hints_add_object_text, text);
    if (size < s) {
      return -2;
    }
#ifdef HINTS
    add_object_text ((struct lev_hints_add_object_text *)E);
#endif
    return s;
  case LEV_HINTS_SET_OBJECT_TYPE ... LEV_HINTS_SET_OBJECT_TYPE + 0xffff:
    STANDARD_LOG_EVENT_HANDLER(lev_hints, set_object_type);
  case LEV_HINTS_DEL_USER_OBJECT ... LEV_HINTS_DEL_USER_OBJECT + 0xff:
/*    if (E->a == 123456789 && ((E->type & 0xff) == 21 || (E->type & 0xff) == 11)) {
      char buf[1000];
      dl_print_local_time (buf, 1000, now);
      fprintf (stderr, "at time %s deleted   %6s %9d\n", buf, (E->type & 0xff) == 15 ? "public" : "group", E->b);
    }*/
    STANDARD_LOG_EVENT_HANDLER(lev_hints, del_user_object);
  case LEV_HINTS_DEL_OBJECT_TEXT ... LEV_HINTS_DEL_OBJECT_TEXT + 0xff:
    STANDARD_LOG_EVENT_HANDLER(lev_hints, del_object_text);
  case LEV_HINTS_SET_USER_INFO ... LEV_HINTS_SET_USER_INFO + 0xff:
    STANDARD_LOG_EVENT_HANDLER(lev_hints, set_user_info);
  case LEV_HINTS_SET_USER_OBJECT_RATING + 1 ... LEV_HINTS_SET_USER_OBJECT_RATING + 0xff + ((MAX_RATING_NUM - 1) << 8):
/*    if (E->a == 123456789 && ((E->type & 0xff) == 21 || (E->type & 0xff) == 11)) {
      char buf[1000];
      dl_print_local_time (buf, 1000, now);
      fprintf (stderr, "at time %s set       %6s %9d to %f\n", buf, (E->type & 0xff) == 15 ? "public" : "group", E->b, *(float *)&E->c);
    }*/
    STANDARD_LOG_EVENT_HANDLER(lev_hints, set_user_object_rating);
  case LEV_HINTS_INCREMENT_USER_OBJECT_RATING ... LEV_HINTS_INCREMENT_USER_OBJECT_RATING + 0xff + ((MAX_RATING_NUM - 1) << 8):
    STANDARD_LOG_EVENT_HANDLER(lev_hints, increment_user_object_rating);
  case LEV_HINTS_INCREMENT_USER_OBJECT_RATING_CHAR ... LEV_HINTS_INCREMENT_USER_OBJECT_RATING_CHAR + 0xffff + ((MAX_RATING_NUM - 1) << 16):
/*    if (E->a == 123456789 && ((E->type & 0xff) == 21 || (E->type & 0xff) == 11)) {
      char buf[1000];
      dl_print_local_time (buf, 1000, now);
      fprintf (stderr, "at time %s increment %6s %9d on %d\n", buf, (E->type & 0xff) == 15 ? "public" : "group", E->b, (E->type & 0xff00) >> 8);
    }*/
    STANDARD_LOG_EVENT_HANDLER(lev_hints, increment_user_object_rating_char);
  case LEV_HINTS_NULLIFY_USER_RATING:
    STANDARD_LOG_EVENT_HANDLER(lev_hints, nullify_user_rating);
  case LEV_HINTS_SET_USER_RATING_STATE ... LEV_HINTS_SET_USER_RATING_STATE + 0xff:
    if (size < (int)sizeof (struct lev_hints_set_user_rating_state)) {
      return -2;
    }
    user_set_rating_state ((struct lev_hints_set_user_rating_state *)E);
    return sizeof (struct lev_hints_set_user_rating_state);
  case LEV_HINTS_USER_OBJECT_WINNER ... LEV_HINTS_USER_OBJECT_WINNER + MAX_RATING_NUM - 1:
    if (size < (int)sizeof (struct lev_hints_user_object_winner)) {
      return -2;
    }
    s = ((struct lev_hints_user_object_winner *) E)->losers_cnt * sizeof (int);
    if (s < 0 || s > 65536) {
      return -4;
    }
    s += offsetof (struct lev_hints_user_object_winner, losers);
    if (size < s) {
      return -2;
    }
    user_object_winner ((struct lev_hints_user_object_winner *)E);
    return s;
  case LEV_HINTS_ADD_USER_OBJECT_RATING + 1 ... LEV_HINTS_ADD_USER_OBJECT_RATING + MAX_RATING_NUM:
    if (size < (int)sizeof (struct lev_hints_add_user_object_rating)) {
      return -2;
    }
    s = ((struct lev_hints_add_user_object_rating *) E)->text_len;
    if (s < -1) {
      return -4;
    }
    s += 1 + offsetof (struct lev_hints_add_user_object_rating, text);
    if (size < s) {
      return -2;
    }
    add_user_object_rating ((struct lev_hints_add_user_object_rating *)E);
    return s;
  }

  fprintf (stderr, "unknown log event type %08x at position %lld\n", E->type, log_cur_pos());

  return -1;
}

int hints_replay_logevent_dump (struct lev_generic *E, int size) {
  int s;
  switch (E->type) {
  case LEV_START:
    assert (0);
  case LEV_NOOP:
  case LEV_TIMESTAMP:
  case LEV_CRC32:
  case LEV_ROTATE_FROM:
  case LEV_ROTATE_TO:
  case LEV_TAG:
    return default_replay_logevent (E, size);
  case LEV_HINTS_ADD_USER_OBJECT:
    if (size < (int)sizeof (struct lev_hints_add_user_object)) {
      return -2;
    }
    s = ((struct lev_hints_add_user_object *) E)->text_len;
    if (!check_text_len (s)) {
      struct lev_hints_add_user_object *EE = (struct lev_hints_add_user_object *)E;
      fprintf (stderr, "Bad log event, user_id = %d, object_id = %d, object_type = %d, text_len = %d\n", EE->user_id, EE->object_id, EE->object_type, EE->text_len);
    }
    if (s < -1) {
      return -4;
    }
    s += 1 + offsetof (struct lev_hints_add_user_object, text);
    if (size < s) {
      return -2;
    }
    dl_zout_log_event_write (&new_binlog, E, s);
    return s;
  case LEV_HINTS_SET_USER_OBJECT_TYPE ... LEV_HINTS_SET_USER_OBJECT_TYPE + 0xffff:
    DUMP_LOG_EVENT_HANDLER(new_binlog, lev_hints_set_user_object_type);
  case LEV_HINTS_ADD_OBJECT_TEXT:
    if (size < (int)sizeof (struct lev_hints_add_object_text)) {
      return -2;
    }
    s = ((struct lev_hints_add_object_text *) E)->text_len;
    if (!check_text_len (s)) {
      struct lev_hints_add_object_text *EE = (struct lev_hints_add_object_text *)E;
      fprintf (stderr, "Bad log event, object_id = %d, object_type = %d, text_len = %d\n", EE->object_id, EE->object_type, EE->text_len);
    }
    if (s < 0) {
      return -4;
    }
    s += 1 + offsetof (struct lev_hints_add_object_text, text);
    if (size < s) {
      return -2;
    }
    dl_zout_log_event_write (&new_binlog, E, s);
    return s;
  case LEV_HINTS_SET_OBJECT_TYPE ... LEV_HINTS_SET_OBJECT_TYPE + 0xffff:
    DUMP_LOG_EVENT_HANDLER(new_binlog, lev_hints_set_object_type);
  case LEV_HINTS_DEL_USER_OBJECT ... LEV_HINTS_DEL_USER_OBJECT + 0xff:
    DUMP_LOG_EVENT_HANDLER(new_binlog, lev_hints_del_user_object);
  case LEV_HINTS_DEL_OBJECT_TEXT ... LEV_HINTS_DEL_OBJECT_TEXT + 0xff:
    DUMP_LOG_EVENT_HANDLER(new_binlog, lev_hints_del_object_text);
  case LEV_HINTS_SET_USER_INFO ... LEV_HINTS_SET_USER_INFO + 0xff:
    DUMP_LOG_EVENT_HANDLER(new_binlog, lev_hints_set_user_info);
  case LEV_HINTS_INCREMENT_USER_OBJECT_RATING ... LEV_HINTS_INCREMENT_USER_OBJECT_RATING + 0xff + (MAX_RATING_NUM << 8):
    DUMP_LOG_EVENT_HANDLER(new_binlog, lev_hints_increment_user_object_rating);
  case LEV_HINTS_INCREMENT_USER_OBJECT_RATING_CHAR ... LEV_HINTS_INCREMENT_USER_OBJECT_RATING_CHAR + 0xffff + (MAX_RATING_NUM << 16):
    DUMP_LOG_EVENT_HANDLER(new_binlog, lev_hints_increment_user_object_rating_char);
  case LEV_HINTS_NULLIFY_USER_RATING:
    DUMP_LOG_EVENT_HANDLER(new_binlog, lev_hints_nullify_user_rating);
  case LEV_HINTS_SET_USER_RATING_STATE ... LEV_HINTS_SET_USER_RATING_STATE + 0xff:
    if (size < (int)sizeof (struct lev_hints_set_user_rating_state)) {
      return -2;
    }
    dl_zout_log_event_write (&new_binlog, E, sizeof (struct lev_hints_set_user_rating_state));
    return sizeof (struct lev_hints_set_user_rating_state);
  case LEV_HINTS_USER_OBJECT_WINNER ... LEV_HINTS_USER_OBJECT_WINNER + MAX_RATING_NUM - 1:
    if (size < (int)sizeof (struct lev_hints_user_object_winner)) {
      return -2;
    }
    s = ((struct lev_hints_user_object_winner *) E)->losers_cnt * sizeof (int);
    if (s < 0 || s > 65536) {
      return -4;
    }
    s += offsetof (struct lev_hints_user_object_winner, losers);
    if (size < s) {
      return -2;
    }
    dl_zout_log_event_write (&new_binlog, E, s);
    return s;
  case LEV_HINTS_ADD_USER_OBJECT_RATING + 1 ... LEV_HINTS_ADD_USER_OBJECT_RATING + MAX_RATING_NUM:
    if (size < (int)sizeof (struct lev_hints_add_user_object_rating)) {
      return -2;
    }
    s = ((struct lev_hints_add_user_object_rating *) E)->text_len;
    if (s < -1) {
      return -4;
    }
    s += 1 + offsetof (struct lev_hints_add_user_object_rating, text);
    if (size < s) {
      return -2;
    }
    dl_zout_log_event_write (&new_binlog, E, s);
    return s;
  }

  fprintf (stderr, "unknown log event type %08x at position %lld\n", E->type, log_cur_pos());

  return -1;
}


/*
 *
 *         HINTS INDEX
 *
 */


char *buffer, *h_buff, *object_buf;
int *i_buff, *new_obj, *object_indexes, *ph_val, *ph_cnt;
long long *new_h, *type_ids;
rating *object_ratings, *object_ratings_to_sort;


long long get_index_header_size (index_header *header) {
  return sizeof (index_header) + sizeof (user_index_data) * (header->user_cnt + 1);
}

int load_header (kfs_file_handle_t Index) {
  if (Index == NULL) {
    header.magic = HINTS_INDEX_MAGIC;

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
#ifdef HINTS
    header.perf_hash = 0;
    header.nohints = 0;
#else
    header.perf_hash = -1;
    header.nohints = 1;
#endif

    header.created_at = time (NULL);
    header_size = sizeof (index_header);

    return 0;
  }

  fd[0] = Index->fd;
  int offset = Index->offset;
  //read header
  assert (lseek (fd[0], offset, SEEK_SET) == offset);

  int size = sizeof (index_header) - sizeof (long);
  int r = read (fd[0], &header, size);
  if (r < size) {
    fprintf (stderr, "error reading header from index file: read %d bytes instead of %d at position 0: %m\n", r, size);
    assert (r == size);
    return 0;
  }

  if (header.magic != HINTS_INDEX_MAGIC) {
    fprintf (stderr, "bad hints index file header\n");
    fprintf (stderr, "magic = 0x%08x // offset = %d\n", header.magic, offset);
    assert (0);
  }

#ifdef HINTS
  assert (header.nohints == 0);
#else
  assert (header.nohints == 1);
  assert (header.perf_hash == -1);
#endif

  size = sizeof (user_index_data) * (header.user_cnt + 1);
  header.user_index = dl_malloc0 (size);
  if (header.rating_num) {
    assert (header.rating_num == rating_num);
  }
  assert (header.fading + fading == 1);

  r = read (fd[0], header.user_index, size);
  if (r < size) {
    fprintf (stderr, "error reading header from index file: read %d bytes instead of %d: %m\n", r, size);
    assert (r == size);
    return 0;
  }

  if (header.has_crc32) {
    crc32_check_and_repair (header.user_index, size, &header.user_index_crc32, 1);
    header.user_index_crc32 = 0;
    unsigned int t = header.header_crc32;
    header.header_crc32 = 0;
    crc32_check_and_repair (&header, sizeof (index_header) - sizeof (long), &t, 1);
  }

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
    dl_free (header->user_index, sizeof (user_index_data) * (header->user_cnt + 1));
  }
}



rating qfind (rating *a, int l, int r, int k) {
  assert (l <= k && k <= r);

  if (l == r) {
    return a[l];
  }

  rating c = a[l + rand() % (r - l + 1)], t;
  int i = l, j = r;

  while (i <= j) {
    while (a[i] > c) {
      i++;
    }
    while (c > a[j]) {
      j--;
    }
    if (i <= j) {
      t = a[i];
      a[i] = a[j];
      a[j] = t;
      i++;
      j--;
    }
  }

  if (k <= j) {
    return qfind (a, l, j, k);
  }
  return qfind (a, j + 1, r, k);
}

int cmp_long (const void * _a, const void * _b) {
  long long a = *(long long *)_a, b = *(long long *)_b;
  if (a < b) {
    return -1;
  } else if (a > b) {
    return +1;
  }
  return 0;
}

int cmp_int_inv (const void * _a, const void * _b) {
  int a = *(int *)_a, b = *(int *)_b;
  if (a < b) {
    return +1;
  } else if (a > b) {
    return -1;
  }
  return 0;
}

#ifdef HINTS
int write_string (char *a, char *s) {
  int need = 8;
  while (*s) {
    *a++ = *s++;
    need--;
  }
  *a++ = 0;
  need--;
  while (need > 0) {
    *a++ = 0;
    need--;
  }

  return 8 - need;
}
#endif

user *__u;
int cmp_obj (const void * _a, const void * _b) {
  int x = *(int *)_a, y = *(int *)_b;
  long long a = ltbl_get_rev (&__u->object_table, x), b = ltbl_get_rev (&__u->object_table, y);
  if (a < b) {
    return -1;
  } else if (a > b) {
    return +1;
  }
  return 0;
}

#ifdef HINTS
int rebuild_flag = 1;
#endif

#define DUMP_SHIFT 5
int save_index (void) {
  double htime = 0;

#ifdef HINTS
  assert (rebuild_flag == 1);
  assert (!(header.perf_hash == 1 && rebuild_flag == 0));
#endif

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

  if (binlogname) {
    if (!binlog_disabled) {
      relax_write_log_crc32();
    } else {
      relax_log_crc32 (0);
    }
  }

  int now = time (NULL);

  int new_binlog_fd = 1u << 31;

  if (new_binlog_name != NULL) {
    new_binlog_fd = DUMP_SHIFT + 256;
    fd[new_binlog_fd] = open (new_binlog_name, O_CREAT | O_TRUNC | O_WRONLY, 0660);

    if (fd[new_binlog_fd] < 0) {
      fprintf (stderr, "cannot create new binlog file %s: %m\n", new_binlog_name);
      exit (1);
    }

    dl_zout_init (&new_binlog, new_binlog_fd, 4 * 1048576);
    struct lev_start *E =
      dl_zout_alloc_log_event (&new_binlog, LEV_START, 6 * sizeof (int));
    E->schema_id = HINTS_SCHEMA_V1;
    E->extra_bytes = 0;
    E->split_mod = log_split_mod;
    E->split_min = log_split_min;
    E->split_max = log_split_max;

    struct lev_timestamp *ET =
      dl_zout_alloc_log_event (&new_binlog, LEV_TIMESTAMP, 2 * sizeof (int));
    ET->timestamp = now;
  }

  index_header header;
  memset (&header, 0, sizeof (header));

  header.magic = HINTS_INDEX_MAGIC;

  header.user_cnt = user_table.currId - 1;
  assert (header.user_cnt >= 0);

  char *dump_index[256] = {NULL}, *dump_s[256] = {NULL};
  dl_zout *dump_out[256] = {NULL};
#ifdef HINTS
  char *dump_names[256] = {NULL};
  map_string_int dump_table[256];
  int dump_id[256] = {0}, dump_names_size[256] = {0}, dump_names_buf_size[256] = {0};
#endif
  int d_type;
  if (dump_mode) {
    char dump_name[105];
    for (d_type = 1; d_type < 256; d_type++) {
      if (dump_type[d_type]) {
        int cur_dump = d_type + DUMP_SHIFT;
        sprintf (dump_name, "%s.dump%03d.%d", index_name, log_split_min, d_type);
        fd[cur_dump] = open (dump_name, O_CREAT | O_TRUNC | O_WRONLY, 0660);

        if (fd[cur_dump] < 0) {
          fprintf (stderr, "cannot create dump file %s: %m\n", dump_name);
          exit (1);
        }
        dump_out[d_type] = dl_malloc (sizeof (dl_zout));
        dl_zout_init (dump_out[d_type], cur_dump, 4 * 1048576);

        int user_len = sprintf (dump_name, "%d\n", header.user_cnt);
        int size = user_len + (sizeof (short) + sizeof (int)) * header.user_cnt;

        dump_index[d_type] = dl_malloc0 (size);
        memcpy (dump_index[d_type], dump_name, user_len);
        dump_s[d_type] = dump_index[d_type] + user_len;

        assert (lseek (fd[cur_dump], size, SEEK_SET) == size);

#ifdef HINTS
        if (dump_type[d_type] >= 2) {
          map_string_int_init (&dump_table[d_type]);
          dump_names_size[d_type] = sizeof (int);
          dump_names_buf_size[d_type] = 101;
          dump_names[d_type] = dl_malloc (sizeof (char) * dump_names_buf_size[d_type]);
        }
#endif
      }
    }
  }

  fd[1] = open (newidxname, O_CREAT | O_TRUNC | O_WRONLY | O_EXCL, 0660);

  if (fd[1] < 0) {
    fprintf (stderr, "cannot create new index file %s: %m\n", newidxname);
    exit (1);
  }

  dl_zout out;
  dl_zout_init (&out, 1, 10 * 1048576);

  header.user_index = dl_malloc0 (sizeof (user_index_data) * (header.user_cnt + 1));
  header.created_at = now;

  long long fCurr = get_index_header_size (&header) - sizeof (long);

#ifdef NOHINTS
  header.perf_hash = -1;
  header.nohints = 1;
#else
  header.perf_hash = rebuild_flag;
  header.nohints = 0;
#endif
  header.rating_num = rating_num;
  header.fading = 1 - fading;

  char *s, *hs;

  assert (lseek (fd[1], fCurr, SEEK_SET) == fCurr);

  rating mult = expf (((rating)(ratingT - header.created_at)) / RATING_NORM);
  rating mult_now = expf (((rating)(ratingT - log_now)) / RATING_NORM);

  // for each user
  int u_id;
  for (u_id = 1; u_id <= header.user_cnt; u_id++) {
    if (verbosity > 1) {
      fprintf (stderr, "u_id = %d\n", u_id);
    }
    s = buffer;
#ifdef HINTS
    hs = h_buff + sizeof (int) * 2;
#else
    hs = h_buff;
#endif

    double st = -get_utime (CLOCK_MONOTONIC);
    user *u = &users[u_id];
    assert (load_user_metafile (u, u_id, 1));

    header.user_index[u_id].id = ltbl_get_rev (&user_table, u_id);

    header.user_index[u_id].shift = fCurr;
    assert (local_user_id (header.user_index[u_id].id) != -1);

    if (verbosity > 1) {
      fprintf (stderr, "user_id = %d\n", header.user_index[u_id].id);
      fprintf (stderr, "  loaded %.6lf\n", st + get_utime (CLOCK_MONOTONIC));
    }

    int i, j;

    //renumerate objects
    int new_o = u->object_table.currId - 1;
    for (j = 1; j <= new_o; j++) {
      new_obj[j - 1] = j;
    }

    __u = u;
    qsort (new_obj, new_o, sizeof (int), cmp_obj);

    int alive_user = (u->object_table.currId > 1) || !fading || keep_not_alive;
    for (i = 1; i <= u->object_old_n && !alive_user; i++) {
      for (j = 0; j < rating_num; j++) {
        alive_user |= (*user_get_object_rating (u, i, j) * mult_now >= MIN_RATING);
      }
    }

    if (!alive_user && u->object_old_n > 0) {
      if (verbosity > 1) {
        fprintf (stderr, "Deleting user %d (local_user_id %d) as not alive.\n", header.user_index[u_id].id, u_id);
      }

      for (i = 1; i <= u->object_old_n; i++) {
        long long type_id = u->object_type_ids[i];
        user_del_object (u, TYPE(type_id), ID(type_id), 1);
      }

      u->flags = SET_USER_INFO(u->flags, -1);

      user_apply_changes (u);
    }

    int tn = 0;
    int num;

#ifdef HINTS
    char *object_data = object_buf;
    for (i = 1, j = 0; i <= u->object_old_n + 1; i++) {
      long long curr = i <= u->object_old_n ? u->object_type_ids[i] : (long long)((1ull << 63) - 1), t = 0ll;
      char *s;

      while (j < new_o && curr > (t = ltbl_get_rev (&u->object_table, new_obj[j]))) {
        s = u->object_names[new_obj[j]];

        if (s != NULL) {
          type_ids[++tn] = t;

          for (num = 0; num < rating_num; num++) {
            object_ratings[tn * rating_num + num] = u->object_ratings[new_obj[j] * rating_num + num];
          }
          object_indexes[tn] = object_data - object_buf;
          object_data += write_string (object_data, s);
        }
        j++;
      }
      assert (j == new_o || curr < t);
      if (i != u->object_old_n + 1) {
        if (u->object_indexes[i] & MEM_FLAG) {
          s = *((char **)(u->object_data + (u->object_indexes[i] & ~MEM_FLAG)));
        } else {
          s = u->object_data + u->object_indexes[i];
        }
        if (s != NULL) {
          type_ids[++tn] = curr;

          for (num = 0; num < rating_num; num++) {
            object_ratings[tn * rating_num + num] = u->object_old_ratings[i * rating_num + num];
          }
          object_indexes[tn] = object_data - object_buf;
          object_data += write_string (object_data, s);
        }
      }
    }
#else
/*    for (i = 1, j = 0; i <= u->object_old_n + 1; i++) {
      long long curr = i <= u->object_old_n ? u->object_type_ids[i] : (1ull << 63) - 1, t = 0ll;

      while (j < new_o && curr > (t = ltbl_get_rev (&u->object_table, new_obj[j]))) {
        if (chg_has (u->object_chg, new_obj[j] + u->object_old_n) == 1) {
          type_ids[++tn] = t;
          for (num = 0; num < rating_num; num++) {
            object_ratings[tn * rating_num + num] = u->object_ratings[new_obj[j] * rating_num + num];
          }
        }
        j++;
      }
      assert (j == new_o || curr < t);
      if (i != u->object_old_n + 1) {
        if (chg_has (u->object_chg, i) >= 0) {
          type_ids[++tn] = curr;
          for (num = 0; num < rating_num; num++) {
            object_ratings[tn * rating_num + num] = u->object_old_ratings[i * rating_num + num];
          }
        }
      }
    }*/

    int chg_n = chg_conv_to_array (u->object_chg, buff), chg_i = 0;
    while (chg_n > 0 && buff[chg_n - 1] / 2 > u->object_old_n) {
      if ((buff[--chg_n] & 1) == 0) {
        u->object_ratings[(buff[chg_n] / 2 - u->object_old_n) * rating_num] = (rating)-1e10f;
      }
    }
    buff[chg_n] = 0;

    for (i = 1, j = 0; i <= u->object_old_n + 1; i++) {
      long long curr = i <= u->object_old_n ? u->object_type_ids[i] : (long long)((1ull << 63) - 1), t = 0ll;

      while (j < new_o && curr > (t = ltbl_get_rev (&u->object_table, new_obj[j]))) {
        if (u->object_ratings[new_obj[j] * rating_num] >= -1e9f) {
          type_ids[++tn] = t;
          for (num = 0; num < rating_num; num++) {
            object_ratings[tn * rating_num + num] = u->object_ratings[new_obj[j] * rating_num + num];
          }
        }
        j++;
      }
      assert (j == new_o || curr < t);
      if (i != u->object_old_n + 1) {
        if ((buff[chg_i] / 2 != i) || (buff[chg_i++] & 1)) {
          type_ids[++tn] = curr;
          for (num = 0; num < rating_num; num++) {
            object_ratings[tn * rating_num + num] = u->object_old_ratings[i * rating_num + num];
          }
        }
      }
    }
#endif

    if (verbosity > 1) {
      fprintf (stderr, "  obj table built %.6lf\n", st + get_utime (CLOCK_MONOTONIC));
    }

    int new_tn = 0;
#ifdef HINTS
    char *new_object_data = object_buf, *cur_object_data = object_buf;
#endif
    for (i = 1; i <= tn; ) {
      int t = TYPE(type_ids[i]);
      for (j = i + 1; j <= tn && TYPE(type_ids[j]) == t; j++) {
      }

      float min_rating = -1e9f;
      if (max_cnt_type[t] < MAX_CNT) {
        if (j - i > max_cnt_type[t]) {
          int k;
          for (k = i; k != j; k++) {
            object_ratings_to_sort[k - i] = object_ratings[k * rating_num];
          }

          min_rating = qfind (object_ratings_to_sort, 0, j - i - 1, max_cnt_type[t]);
        } else {
          min_rating = MIN_RATING / mult_now;
        }
      }

      for ( ; i != j; i++) {
        if (object_ratings[i * rating_num] > min_rating) {
          type_ids[++new_tn] = type_ids[i];

          for (num = 0; num < rating_num; num++) {
            object_ratings[new_tn * rating_num + num] = object_ratings[i * rating_num + num];
          }

#ifdef HINTS
          object_indexes[new_tn] = new_object_data - object_buf;
          int l = write_string (new_object_data, cur_object_data);
          new_object_data += l;
          cur_object_data += l;
        } else {
          cur_object_data += write_string (cur_object_data, cur_object_data);
#endif
        }
      }
    }

    tn = new_tn;

    assert (tn <= MAX_CNT);

#ifdef HINTS
    assert (cur_object_data == object_data);
    object_data = new_object_data;
#endif

    if (verbosity > 1) {
      fprintf (stderr, "  expired objects deleted %.6lf, %d objects left\n", st + get_utime (CLOCK_MONOTONIC), tn);
    }

    int h_size = 0;
#ifdef HINTS
    int hn = 0;

    htime -= get_utime (CLOCK_MONOTONIC);

    int nn = 0;
    if (rebuild_flag == 1) {
      hash_table_vct pref;
      htbl_vct_init (&pref);
      htbl_vct_set_size (&pref, tn * 15);

      for (i = tn; i >= 1; i--) {
        long long *v = gen_hashes (object_buf + object_indexes[i]);
        for (j = 0; v[j]; j++) {
          htbl_vct_add (&pref, v[j], i);
        }
      }

      if (verbosity > 1) {
        fprintf (stderr, "  vectors of hashes built %.6lf, total prefixes: %d\n", st + get_utime (CLOCK_MONOTONIC), pref.size);
      }

      for (j = 0; j < pref.size; j++) {
        hash_entry_vct_ptr curr = pref.h[j];
        while (curr != NULL) {
          new_h[nn++] = curr->key;
          curr = curr->next_entry;
        }
      }
//      qsort (new_h, nn, sizeof (long long), cmp_long);

      if (verbosity > 1) {
        fprintf (stderr, "  list of hashes built %.6lf, total values: %d\n", st + get_utime (CLOCK_MONOTONIC), nn);
      }

      perfect_hash ph;
      ph_init (&ph);
      ph_generate (&ph, new_h, nn);

      if (verbosity > 1) {
        fprintf (stderr, "  perfect hashing generated %.6lf\n", st + get_utime (CLOCK_MONOTONIC));
      }

      long long *hs_l = (long long *)hs;

      for (i = 0; i < nn; i++) {
        hs_l[ph_h (&ph, new_h[i])] = new_h[i];
      }

      for (i = 0; i < nn; i++) {
        vector *v = htbl_vct_find (&pref, hs_l[i]);
        assert (ph_h (&ph, hs_l[i]) == i);
        assert (v != NULL);
        i_buff[hn] = s - buffer;
        s += LIST_(encode_list) (v->mem, v->n, tn, (unsigned char *)s);
        WRITE_SHORT(hs, (short)hs_l[i]);
        hn++;
      }

      htbl_vct_free (&pref);
      i_buff[hn] = s - buffer;

      assert (nn == hn);
      h_size = sizeof (short) * hn;
      int ph_size = ph_encode (&ph, (unsigned char *)hs);

      ph_free (&ph);

      hs += ph_size;
      h_size += ph_size;
    }

    htime += get_utime (CLOCK_MONOTONIC);
#endif
    assert (alive_user || tn == 0);

    if (verbosity > 1) {
      fprintf (stderr, "  main index built %.6lf\n", st + get_utime (CLOCK_MONOTONIC));
    }

    static int types[256];
    int cnt = 0;

    memset (types, 0, sizeof (types));
    for (i = 1; i <= tn; i++) {
      int t = TYPE(type_ids[i]);
      assert (0 <= t && t < 256);
      if (types[t] == 0) {
        cnt++;
      }
      types[t] = i;
    }

    WRITE_INT(hs, cnt);
    h_size += sizeof (int) + (sizeof (char) + sizeof (int)) * cnt;

    for (i = 0; i < 256; i++) {
      if (types[i]) {
        *hs++ = (char)i;
      }
    }

    for (i = 0; i < 256; i++) {
      if (types[i]) {
        WRITE_INT (hs, types[i]);
      }
    }

    if (fading) {
      for (j = rating_num; j <= tn * rating_num; j++) {
        object_ratings[j] *= mult;
      }
    }

    if (new_binlog_name != NULL) {
      int user_id = header.user_index[u_id].id;
      if (GET_USER_INFO(u->flags) != -1) {
        struct lev_hints_set_user_info *E =
          dl_zout_alloc_log_event (&new_binlog, LEV_HINTS_SET_USER_INFO + GET_USER_INFO(u->flags) + 2, sizeof (struct lev_hints_set_user_info));
        E->user_id = user_id;
      }
      if (GET_USER_RATING_STATE(u->flags) != 1) {
        struct lev_hints_set_user_rating_state *E =
          dl_zout_alloc_log_event (&new_binlog, LEV_HINTS_SET_USER_RATING_STATE + GET_USER_RATING_STATE(u->flags), sizeof (struct lev_hints_set_user_rating_state));
        E->user_id = user_id;
      }

      for (i = 1; i <= tn; i++) {
        int text_len = -1;
#ifdef HINTS
        char *text = object_buf + object_indexes[i];
        text_len = strlen (text);
#endif

        struct lev_hints_add_user_object_rating *E =
          dl_zout_alloc_log_event (&new_binlog, LEV_HINTS_ADD_USER_OBJECT_RATING + rating_num,
                                   offsetof (struct lev_hints_add_user_object_rating, text) + 1 + text_len + rating_num * sizeof (float));

        E->user_id = user_id;
        E->object_type = TYPE(type_ids[i]);
        E->object_id = ID(type_ids[i]);
        E->text_len = text_len + rating_num * sizeof (float);

#ifdef HINTS
        memcpy (E->text, text, sizeof (char) * (text_len + 1));
#endif
        float *ratings = (float *)(E->text + text_len + 1);
        for (num = 0; num < rating_num; num++) {
          ratings[num] = object_ratings[i * rating_num + num];
        }
      }
    }

#ifdef HINTS
    h_size += sizeof (int) * 2;
    int offset = h_size + (hn + 1) * sizeof (int);
    for (j = 0; j <= hn; j++) {
      i_buff[j] += offset - sizeof (int);
    }

    ((int *)h_buff)[0] = offset - sizeof (int) + (long)(s - buffer);
    ((int *)h_buff)[1] = hn;
#endif

    dl_zout_flush (&out);
    out.crc32_complement = -1;

    dl_zout_write (&out, h_buff, h_size);
#ifdef HINTS
    dl_zout_write (&out, i_buff, (hn + 1) * sizeof (int));
#endif

    header.user_index[u_id].flags = u->flags;

    // write objects ids
    WRITE_INT(s, tn);
    dl_zout_write (&out, buffer, (long)(s - buffer));
    dl_zout_write (&out, type_ids, (tn + 1) * sizeof (long long));
#ifdef HINTS
    dl_zout_write (&out, object_indexes, (tn + 1) * sizeof (int));
    dl_zout_write (&out, object_ratings, (tn + 1) * sizeof (rating) * rating_num);
    dl_zout_write (&out, object_buf, (long)(object_data - object_buf));

    assert (h_size < MAX_USR_SIZE);
    assert ((hn + 1) * (int)sizeof (int) < MAX_USR_SIZE);
    assert ((long)(object_data - object_buf) < MAX_USR_SIZE);
#else
    dl_zout_write (&out, object_ratings, (tn + 1) * sizeof (rating) * rating_num);
#endif
    assert ((long)(s - buffer) < MAX_USR_SIZE);

    dl_zout_flush (&out);
    int metafile_crc32 = ~out.crc32_complement;
    dl_zout_write (&out, &metafile_crc32, sizeof (unsigned int));

    // write user
    header.user_index[u_id].size = (long)(s - buffer) +
                                   sizeof (unsigned int) +
#ifdef HINTS
                                   offset +
                                   (long)(object_data - object_buf) +
                                   (tn + 1) * sizeof (int) +
#else
                                   h_size +
#endif
                                   (tn + 1) * (sizeof (long long) + sizeof (rating) * rating_num);

    fCurr += header.user_index[u_id].size;

    if (dump_mode) {
      for (d_type = 1; d_type < 256; d_type++) {
        if (dump_type[d_type]) {
          short written = 0;
          if (types[d_type] > 0) {
            int l = 1, r = types[d_type];
            for (i = d_type - 1; i >= 0; i--) {
              if (types[i]) {
                l = types[i] + 1;
                break;
              }
            }

            if (r - l > 32766) {
              r = l + 32766;
            }

            s = buffer;
            for (i = l; i <= r; i++) {
              int num = -1;
              if (dump_type[d_type] == 1) {
                num = ID(type_ids[i]);
#ifdef HINTS
              } else {
                char *name = clean_str (object_buf + object_indexes[i]);
                int len = strlen (name) + 1;
                if (len > 1) {
                  int *t = map_string_int_add (&dump_table[d_type], name);
                  if (*t == 0) {
                    *t = ++dump_id[d_type];
                    assert (dump_id[d_type] < 400000000);
                    while (len + 2 + 2 * (int)sizeof (int) + dump_names_size[d_type] > dump_names_buf_size[d_type]) {
                      int new_size = sizeof (char) * (dump_names_buf_size[d_type] * 2 + 1);
                      dump_names[d_type] = dl_realloc (dump_names[d_type], new_size, sizeof (char) * dump_names_buf_size[d_type]);
                      dump_names_buf_size[d_type] = new_size;
                    }

                    memcpy (dump_names[d_type] + dump_names_size[d_type], name, len);
                    dump_names_size[d_type] += len;
                    char *s = dump_names[d_type] + dump_names_size[d_type];
                    WRITE_INT(s, header.user_index[u_id].id);
                    WRITE_INT(s, (int)ID(type_ids[i]));
                    dump_names_size[d_type] += 2 * sizeof (int);
                  }
                  num = *t;
                }
#endif
              }

              if (num >= 0) {
                WRITE_INT(s, num);
                *(rating *)s = object_ratings[i * rating_num];
                if (dump_type[d_type] >= 3) {
                  *(rating *)s = 3.0f;
                }
                s += sizeof (rating);
                written++;
              }
            }
            dl_zout_write (dump_out[d_type], buffer, (long)(s - buffer));
          }

          WRITE_SHORT(dump_s[d_type], written);
          WRITE_INT(dump_s[d_type], header.user_index[u_id].id);
        }
      }
    }

    if (new_binlog_name != NULL && rand() % 1000 == 0) {
      dl_zout_flush (&new_binlog);

      struct lev_crc32 *E =
        dl_zout_alloc_log_event (&new_binlog, LEV_CRC32, sizeof (struct lev_crc32));
      E->timestamp = now;
      E->pos = new_binlog.written;
      E->crc32 = ~new_binlog.crc32_complement;
    }

    user_LRU_unload();
  }
  dl_zout_free (&out);

//  write header

  if (verbosity > 0) {
    fprintf (stderr, "index prefixes time : %.7lf s\n", htime);
  }

/* fill info about binlog */
  header.log_pos1 = log_cur_pos();
  header.log_timestamp = log_last_ts;
  header.log_split_min = log_split_min;
  header.log_split_max = log_split_max;
  header.log_split_mod = log_split_mod;
  //flush_binlog_ts();
  relax_log_crc32 (0);
  header.log_pos1_crc32 = ~log_crc32_complement;

  if (new_binlog_name != NULL) {
    dl_zout_flush (&new_binlog);

    struct lev_crc32 *E =
      dl_zout_alloc_log_event (&new_binlog, LEV_CRC32, sizeof (struct lev_crc32));
    E->timestamp = now;
    E->pos = new_binlog.written;
    E->crc32 = ~new_binlog.crc32_complement;

    header.log_pos1 = E->pos;
    header.log_timestamp = E->timestamp;
    header.log_pos1_crc32 = E->crc32;
  }

  header.has_crc32 = 1;
  header.header_crc32 = compute_crc32 (&header, sizeof (index_header) - sizeof (long));
  header.user_index_crc32 = compute_crc32 (header.user_index, sizeof (user_index_data) * (header.user_cnt + 1));

  assert (lseek (fd[1], 0, SEEK_SET) == 0);
  assert (write (fd[1], &header, sizeof (header) - sizeof (long)) == (ssize_t)(sizeof (header) - sizeof (long)));
  assert (write (fd[1], header.user_index, sizeof (user_index_data) * (header.user_cnt + 1)) == (ssize_t)sizeof (user_index_data) * (header.user_cnt + 1));

  if (dump_mode) {
    for (d_type = 1; d_type < 256; d_type++) {
      if (dump_type[d_type]) {
        int cur_dump = d_type + DUMP_SHIFT;
#ifdef HINTS
        if (dump_type[d_type] >= 2) {
          *(int *)dump_names[d_type] = dump_id[d_type];
          int size = dump_names_size[d_type];
          dl_zout_write (dump_out[d_type], dump_names[d_type], size);

          dl_free (dump_names[d_type], sizeof (char) * dump_names_buf_size[d_type]);
          map_string_int_free (&dump_table[d_type]);
        }
#endif
        dl_zout_free (dump_out[d_type]);
        dl_free (dump_out[d_type], sizeof (dl_zout));

        assert (lseek (fd[cur_dump], 0, SEEK_SET) == 0);
        int size = dump_s[d_type] - dump_index[d_type];
        assert (write (fd[cur_dump], dump_index[d_type], size) == size);

        dl_free (dump_index[d_type], size);
        dump_index[d_type] = NULL;

        assert (fsync (fd[cur_dump]) >= 0);
        assert (close (fd[cur_dump]) >= 0);
        fd[cur_dump] = -1;
      }
    }
  }

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

  if (new_binlog_name != NULL) {
    replay_logevent = hints_replay_logevent_dump;
    read_new_events();

    dl_zout_flush (&new_binlog);

    struct lev_crc32 *E =
      dl_zout_alloc_log_event (&new_binlog, LEV_CRC32, sizeof (struct lev_crc32));
    E->timestamp = now;
    E->pos = new_binlog.written;
    E->crc32 = ~new_binlog.crc32_complement;

    dl_zout_free (&new_binlog);

    assert (fsync (fd[new_binlog_fd]) >= 0);
    assert (close (fd[new_binlog_fd]) >= 0);
    fd[new_binlog_fd] = -1;
  }

  return 0;
}
#undef DUMP_SHIFT


int onload_user_metafile (struct connection *c, int read_bytes);
void bind_user_metafile (user *u);
void unbind_user_metafile (user *u);

conn_type_t ct_metafile_aio = {
  .magic = CONN_FUNC_MAGIC,
  .wakeup_aio = onload_user_metafile
};


inline char *get_user_metafile (user *u) {
  if (u->aio != NULL || u->metafile == NULL) {
    return NULL;
  }

  user_apply_changes (u);

  del_user_used (u);
  add_user_used (u);

  return u->metafile;
}


char *load_user_metafile (user *u, int local_user_id, int no_aio) {
  if (verbosity > 2) {
    fprintf (stderr, "load user (%p) metafile local_user_id = %d, no_aio = %d, (%p) wakeup = %p\n", u, local_user_id, no_aio, u->object_table.to.h, onload_user_metafile);
  }

  int unloaded_metafiles = 0;

  static struct aio_connection empty_aio_conn;

  WaitAioArrClear();

  if (u->metafile != NULL && u->aio == NULL) {
    return get_user_metafile (u);
  }

  if (local_user_id > header.user_cnt) {
    u->object_size = 0;

    ltbl_set_size (&u->object_table, 1000);

    u->metafile = EMPTY__METAFILE;
    u->metafile_len = 0;
    u->object_old_n = 0;

    empty_aio_conn.extra = u;
    empty_aio_conn.basic_type = ct_aio;
    u->aio = &empty_aio_conn;

    onload_user_metafile ((struct connection *)(&empty_aio_conn), u->metafile_len);

    return u->metafile;
  }

  if (u->aio) {
    check_aio_completion (u->aio);
    if (u->aio) {
      WaitAioArrAdd (u->aio);
      return NULL;
    }

    if (u->metafile != NULL) {
      return get_user_metafile (u);
    } else {
      fprintf (stderr, "Previous AIO query failed for user at %p, scheduling a new one. Active AIO queries : %lld.\n", u, active_aio_queries);
    }
  }

  u->metafile_len = header.user_index[local_user_id].size;
  assert (u->metafile == NULL);

  while (1) {
    u->metafile = dl_malloc (u->metafile_len);
    if (u->metafile != NULL) {
      break;
    }

    if (user_LRU_unload() == -1) {
      fprintf (stderr, "no space to load metafile - cannot allocate %d bytes (%lld currently used)\n", u->metafile_len, allocated_metafile_bytes);
      return NULL;
    }
    unloaded_metafiles++;
  }

  if (verbosity > 2 && unloaded_metafiles) {
    fprintf (stderr, "!NOTICE! had to unload %d metafiles to allocate %d bytes, now %lld\n", unloaded_metafiles, u->metafile_len, allocated_metafile_bytes);
  }

  allocated_metafile_bytes += u->metafile_len;

  if (verbosity > 2) {
    fprintf (stderr, "*** Scheduled reading user data from index at position %lld, %d bytes\n", header.user_index[local_user_id].shift, u->metafile_len);
  }

  if (no_aio) {
    double disk_time = -get_utime (1);

    assert (lseek (fd[0], header.user_index[local_user_id].shift, SEEK_SET) == header.user_index[local_user_id].shift);
    int size = header.user_index[local_user_id].size;
    int r = read (fd[0], u->metafile, size);
    if (r < size) {
      fprintf (stderr, "error reading user %d from index file: read %d bytes instead of %d at position %lld: %m\n", local_user_id, r, size, header.user_index[local_user_id].shift);
      assert (r == size);
      return NULL;
    }

    disk_time += get_utime (1);
    if (verbosity > 2) {
      fprintf (stderr, "  disk time = %.6lf\n", disk_time);
    }

    empty_aio_conn.extra = u;
    empty_aio_conn.basic_type = ct_aio;
    u->aio = &empty_aio_conn;

    onload_user_metafile ((struct connection *)(&empty_aio_conn), u->metafile_len);

    return u->metafile;
  } else {
    u->aio = create_aio_read_connection (fd[0], u->metafile, header.user_index[local_user_id].shift, u->metafile_len, &ct_metafile_aio, u);
    assert (u->aio);
    WaitAioArrAdd (u->aio);
  }

  return 0;
}

int onload_user_metafile (struct connection *c, int read_bytes) {
  if (verbosity > 2) {
    fprintf (stderr, "onload_user_metafile (%p,%d)\n", c, read_bytes);
  }

  struct aio_connection *a = (struct aio_connection *)c;
  user *u = (user *) a->extra;

  if (verbosity > 2) {
     fprintf (stderr, "onload user metafile local id = %d (%p), hash (%p)\n", (int)(u - users), u, u->object_table.to.h);
  }

  assert (a->basic_type == ct_aio);

  if (u->aio != a) {
    fprintf (stderr, "assertion (u->aio == a) will fail\n");
    fprintf (stderr, "%p != %p\n", u->aio, a);
  }

  assert (u->aio == a);

  if (read_bytes < u->metafile_len) {
    if (verbosity >= 0) {
      fprintf (stderr, "ERROR reading user: read %d bytes out of %d: %m\n", read_bytes, u->metafile_len);
    }

    dl_free (u->metafile, u->metafile_len);
    u->metafile = NULL;
    u->aio = NULL;
    allocated_metafile_bytes -= u->metafile_len;
    u->metafile_len = 0;

    return 0;
  }

  if (verbosity > 2) {
    fprintf (stderr, "*** Read user: read %d bytes\n", read_bytes);
  }

  u->aio = NULL;

  assert (read_bytes == u->metafile_len);

  int t = u->metafile_len;
  bind_user_metafile (u);

  u->metafile_len = t;

  u->flags ^= USER_LOADED;
  u->chg_list_global = global_changes_st;
  u->chg_list_cur = u->chg_list_st;

  add_user_used (u);
  cur_users++;
#ifdef HINTS
  vct_init (&u->changed_objs);
#endif

  while (!index_mode && dl_get_memory_used() > max_memory && !((long long)get_memory_used() - trp_get_memory() - htbl_get_memory() -
            htbl_vct_get_memory() - changes_memory - chg_list_get_memory() < max_memory * MIN_MEMORY_USER_PERCENT)) {
    if (user_LRU_unload() == -1) {
      exit (1);
    }

    if (verbosity > 2) {
      fprintf (stderr, "memory used: %ld\n", get_memory_used());
      fprintf (stderr, "static memory: %ld\n", (long)static_memory);
      fprintf (stderr, "xx memory: %ld\n ", (long)(htbl_vct_get_memory() + trp_get_memory() + chg_list_get_memory()));
      fprintf (stderr, "user memory :%lf vs %lf\n", (double)(get_memory_used() - htbl_get_memory() - htbl_vct_get_memory() - trp_get_memory() -
                             chg_list_get_memory() - changes_memory), max_memory * MIN_MEMORY_USER_PERCENT);

      fprintf (stderr, "after user_LRU_unload : memory used = %ld, changes_memory = %ld, %ld\n", get_memory_used(), changes_memory, max_memory);
    }
  }

  user_apply_changes (u);

  return 1;
}

int unload_user_metafile (user *u) {
  assert (u != NULL);

  int user_id = -1;

  if (verbosity > 1) {
    user_id = (int)ltbl_get_rev (&user_table, (int)(u - users));
  }

  if (verbosity > 2) {
    fprintf (stderr, "unload_user_metafile (%d)\n", user_id);
  }

  if (!u->metafile || u->aio) {
    if (verbosity > 1) {
      fprintf (stderr, "cannot unload user metafile (%d)\n", user_id);
    }
    return 0;
  }

  del_user_used (u);
  cur_users--;

  allocated_metafile_bytes -= u->metafile_len;

  unbind_user_metafile (u);

  if (verbosity > 2) {
    fprintf (stderr, "unload_user_metafile (%d) END\n", user_id);
  }

  return 1;
}

void bind_user_metafile (user *u) {
  u->load_time = now;
  if (verbosity > 2) {
    fprintf (stderr, "bind user metafile local id = %d (%p), hash (%p)\n", (int)(u - users), u, u->object_table.to.h);
  }

#ifdef HINTS
  int pref_n = 0;
  change_list_ptr temp = u->chg_list_st;
  while (temp->next != NULL) {
    temp = temp->next;
    if (temp->x > 0) {
      pref_n++;
    }
  }

  pref_n *= 15;
  if (verbosity > 2) {
    fprintf (stderr, "pref_n = %d\n", pref_n);
  }

  int htbl_size = pref_n * 1.25;
  if (htbl_size < 100) {
    htbl_size = 100;
  }
  htbl_set_size (&u->pref_table, htbl_size);
#endif

  if (u->metafile == EMPTY__METAFILE) {
#ifdef HINTS
    u->mtf_i = NULL;
    u->mtf_h = NULL;
    u->mtf_n = 0;
#endif
    return;
  }

  if (header.has_crc32) {
    crc32_check_and_repair (u->metafile, u->metafile_len - sizeof (unsigned int), (unsigned int *)(u->metafile + u->metafile_len - sizeof (unsigned int)), 1);
  }

#ifdef HINTS
  int pref_metafile_len;
  READ_INT (u->metafile, pref_metafile_len);
#endif
  char *s = u->metafile, *t = u->metafile;

#ifdef HINTS
  u->mtf_n = ((int *)t)[0];
  u->mtf_h = (void *)(t + sizeof (int));

  if (header.perf_hash) {
    t += sizeof (int) + sizeof (short) * u->mtf_n;
    ph_init (&u->h);
    t += ph_decode (&u->h, (unsigned char *)t);
  } else {
    t += sizeof (int) + sizeof (long long) * u->mtf_n;
  }
#endif

  READ_INT(t, u->types_cnt);
  u->known_types = t;
  t += (sizeof (int) + sizeof (char)) * u->types_cnt;

#ifdef HINTS
  u->mtf_i = (int *)t;

  s += pref_metafile_len;

  if (verbosity > 2) {
    int n = u->mtf_n;
    fprintf (stderr, "Number of hashes = %d, size of golomb codes = %d\n", 12 * n,
      u->mtf_i[n] - u->mtf_i[0]);
  }
#else
  s = t;
#endif

  // read objects ids
  int object_n, t_object_n;
  READ_INT(s, object_n);

  u->object_type_ids = (long long *)s;
#ifdef HINTS
  u->object_indexes = (int *)(u->object_type_ids + (object_n + 1));
  u->object_old_ratings = (rating *)(u->object_indexes + (object_n + 1));
  u->object_data = (char *)(u->object_old_ratings + (object_n + 1) * rating_num);

  if (verbosity > 2) {
    fprintf (stderr, "Number of objects = %d, size of objects names = %d\n", 16 * object_n,
      u->object_indexes[object_n]-u->object_indexes[1]);
  }
#else
  u->object_old_ratings = (rating *)(u->object_type_ids + (object_n + 1));
  assert (u->metafile_len == (int)((sizeof (int) + sizeof (char)) * u->types_cnt + 2 * sizeof (int) +
                                   (sizeof (rating) * rating_num + sizeof (long long)) * (object_n + 1) +
                                    sizeof (unsigned int) * header.has_crc32));
  CHG_INIT (u->object_chg);
#endif
  u->object_old_n = object_n;

  t_object_n = 100;
  if (t_object_n < 100) {
    t_object_n = 100;
  }

  ltbl_set_size (&u->object_table, t_object_n);
}

void unbind_user_metafile (user *u) {
  if (u == NULL) {
    return;
  }
  int cache_time = now - u->load_time;
  total_cache_time += cache_time;
  if (cache_time > max_cache_time) {
    max_cache_time = cache_time;
  }
  if (cache_time < min_cache_time) {
    min_cache_time = cache_time;
  }

  if (verbosity > 2) {
    fprintf (stderr, "unbind_user_metafile\n");
  }

  dl_free (u->object_ratings, u->object_size * sizeof (rating) * rating_num);

#ifdef HINTS
  int i;
  for (i = 1; i < u->object_table.currId; i++) {
    if (u->object_names[i] != NULL) {
      dl_free (u->object_names[i], (strlen (u->object_names[i]) + 1) * sizeof (char));
    }
  }
  dl_free (u->object_names, u->object_size * sizeof (char *));

  for (i = 0; i < u->changed_objs.n; i++) {
    char *s = *((char **)(u->object_data + (u->object_indexes[u->changed_objs.mem[i]] & ~MEM_FLAG)));
    if (s != NULL) {
      dl_free (s, (strlen (s) + 1) * sizeof (char));
    }
  }
  vct_free (&u->changed_objs);

  u->object_names = NULL;
  htbl_free (&u->pref_table);
#else
  chg_free (&u->object_chg);
#endif

  u->object_size = 0;
  u->object_ratings = NULL;

  ltbl_free (&u->object_table);

  u->next_used = NULL;
  u->prev_used = NULL;

  if (u->metafile != EMPTY__METAFILE) {
#ifdef HINTS
    dl_free (u->metafile - sizeof (int), u->metafile_len);
#else
    dl_free (u->metafile, u->metafile_len);
#endif
  }

  u->metafile = NULL;
  u->metafile_len = 0;
#ifdef HINTS
  u->mtf_i = NULL;
  u->mtf_h = NULL;
  u->mtf_n = 0;

  u->object_data = NULL;
  u->object_indexes = NULL;
#endif
  u->object_type_ids = NULL;
  u->object_old_n = 0;
}


void update_user_info (void) {
  int i;
  for (i = 1; i <= header.user_cnt; i++) {
    if (GET_USER_INFO (users[i].flags) != 1) {
      users[i].flags = SET_USER_INFO (users[i].flags, -1);
    } else {
      indexed_users++;
    }
  }
}


long get_changes_memory (void) {
  return changes_memory + chg_list_get_memory();
}

long long get_del_by_LRU (void) {
  return del_by_LRU;
}

int get_new_users (void) {
  return user_table.currId - 1 - index_users;
}

int get_global_stats (char *buff) {
  char *s = buff;
  int i;
  buff += sprintf (buff, "GLOBAL_STAT:\n");
  for (i = 0; i < 256; i++) {
    if (stat_global[i][0] || stat_global[i][1] || stat_global[i][2]) {
      buff += sprintf (buff, "type (%d) : del %d, add %d, change %d\n", i, stat_global[i][0], stat_global[i][1], stat_global[i][2]);
    }
  }
  return buff - s;
}


int init_all (kfs_file_handle_t Index) {
  int i;

  if (verbosity > 3) {
    fprintf (stderr, "sizeof (user) = %ld\n", (long)sizeof (user));
  }

  log_ts_exact_interval = 1;

  buff = dl_malloc ((MAX_CNT + 1) * sizeof (int));

  if (index_mode) {
    if (200ll * MAX_CNT < 2000000000ll) {
      MAX_USR_SIZE = 200 * MAX_CNT;
    } else {
      MAX_USR_SIZE = 2000000000;
    }

    buffer = dl_malloc (MAX_USR_SIZE);
    new_obj = dl_malloc (MAX_USR_SIZE);
    type_ids = dl_malloc ((MAX_CNT + 1) * sizeof (long long));
    object_ratings = dl_malloc ((MAX_CNT + 1) * sizeof (rating) * MAX_RATING_NUM);
    object_ratings_to_sort = dl_malloc ((MAX_CNT + 1) * sizeof (rating));
    h_buff = dl_malloc (MAX_USR_SIZE);

#ifdef HINTS
    i_buff = dl_malloc (MAX_USR_SIZE);
    new_h = dl_malloc (MAX_USR_SIZE);
    object_indexes = dl_malloc ((MAX_CNT + 1) * sizeof (int));
    object_buf = dl_malloc (MAX_USR_SIZE);
#endif
  } else {
    int i;
    for (i = 0; i <= MAX_K; i++) {
      dp[i] = dl_malloc ((MAX_CNT + 1) * sizeof (rating));
    }
    d = dl_malloc ((MAX_CNT + 1) * sizeof (rating));
    weight = dl_malloc (MAX_CNT * sizeof (rating));

    heap = dl_malloc ((MAX_CNT + 1) * sizeof (int));
    objects_typeids_to_sort = dl_malloc (MAX_CNT * sizeof (long long));
    objects_to_sort = dl_malloc (MAX_CNT * sizeof (int));

#ifdef HINTS
    intersect_buff = dl_malloc ((MAX_CNT + 1) * sizeof (int));
#else
    exc = dl_malloc0 ((MAX_CNT + 1) * sizeof (int));
#endif
  }

  chg_list_init (&global_changes_st, &global_changes_en);
  ltbl_init (&user_table);

  int f = load_header (Index);

  ratingT = header.created_at;

  jump_log_ts = header.log_timestamp;
  jump_log_pos = header.log_pos1;
  jump_log_crc32 = header.log_pos1_crc32;

  int user_cnt = index_users = header.user_cnt;

  if (user_cnt < estimate_users) {
    user_cnt = estimate_users;
  }

  assert (user_cnt >= estimate_users);

  user_cnt *= 1.1;
  while (user_cnt % 2 == 0 || user_cnt % 5 == 0) {
    user_cnt++;
  }

  if (write_only) {
    user_cnt = 3;
  }

  ltbl_set_size (&user_table, user_cnt);

  users = dl_malloc (sizeof (user) * user_cnt);
  for (i = 0; i < user_cnt; i++) {
    user_init (&users[i]);
  }

  for (i = 1; i <= header.user_cnt; i++) {
    users[i].flags = SET_USER_INFO         (users[i].flags, GET_USER_INFO         (header.user_index[i].flags));
    users[i].flags = SET_USER_RATING_STATE (users[i].flags, GET_USER_RATING_STATE (header.user_index[i].flags));
  }

  LRU_head = users;
  LRU_head->next_used = LRU_head->prev_used = LRU_head;

  if (f) {
    try_init_local_user_id();
  }

  return f;
}

/*
void test_mem (void) {
  assert ((long)(get_memory_used() - trp_get_memory() - htbl_get_memory() - htbl_vct_get_memory() -
            changes_memory - chg_list_get_memory()) == 0);
}
*/

extern char *history_q[MAX_HISTORY + 1];
extern int history_l, history_r;

void free_all (void) {
  if (verbosity) {
    while (user_LRU_unload() != -1) {
    }

    while (history_l != history_r) {
      dl_strfree (history_q[history_l]);
      history_q[history_l++] = 0;
      if (history_l > MAX_HISTORY) {
        history_l = 0;
      }
    }

    fprintf (stderr, "Memory left : %ld\n", (long)(get_memory_used() - trp_get_memory() - htbl_get_memory() - htbl_vct_get_memory() -
             changes_memory - chg_list_get_memory()));
    fprintf (stderr, "memory used = %ld, trp_memory = %ld, htbl_memory = %ld, changes_memory = %ld, mtf = %lld, vct = %ld\n",
             get_memory_used(), (long)trp_get_memory(), (long)htbl_get_memory(), (long)get_changes_memory(), allocated_metafile_bytes, (long)htbl_vct_get_memory());

    assert ((long)(get_memory_used() - trp_get_memory() - htbl_get_memory() - htbl_vct_get_memory() -
                   changes_memory - chg_list_get_memory()) == 0);

    dl_free (buff, (MAX_CNT + 1) * sizeof (int));

    if (index_mode) {
      dl_free (buffer, MAX_USR_SIZE);
      dl_free (new_obj, MAX_USR_SIZE);
      dl_free (type_ids, (MAX_CNT + 1) * sizeof (long long));
      dl_free (object_ratings, (MAX_CNT + 1) * sizeof (rating) * MAX_RATING_NUM);
      dl_free (object_ratings_to_sort, (MAX_CNT + 1) * sizeof (rating));
      dl_free (h_buff, MAX_USR_SIZE);

#ifdef HINTS
      dl_free (i_buff, MAX_USR_SIZE);
      dl_free (new_h, MAX_USR_SIZE);
      dl_free (object_indexes, (MAX_CNT + 1) * sizeof (int));
      dl_free (object_buf, MAX_USR_SIZE);
#endif
    } else {
      int i;
      for (i = 0; i <= MAX_K; i++) {
        dl_free (dp[i], (MAX_CNT + 1) * sizeof (rating));
      }
      dl_free (d, (MAX_CNT + 1) * sizeof (rating));
      dl_free (weight, MAX_CNT * sizeof (rating));

      dl_free (heap, (MAX_CNT + 1) * sizeof (int));
      dl_free (objects_typeids_to_sort, MAX_CNT * sizeof (long long));
      dl_free (objects_to_sort, MAX_CNT * sizeof (int));

#ifdef HINTS
      dl_free (intersect_buff, (MAX_CNT + 1) * sizeof (int));
#else
      dl_free (exc, (MAX_CNT + 1) * sizeof (int));
#endif
    }

    ltbl_free (&user_table);
    free_header (&header);
  }
}
