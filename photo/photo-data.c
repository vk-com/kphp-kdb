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
#define _GNU_SOURCE

#include <ctype.h>
#include <stddef.h>
#include <string.h>

#include "base64.h"

#include "utils.h"

#include "kdb-photo-binlog.h"
#include "photo-data.h"

#if MAX_RESULT < MAX_PHOTOS_DYN || MAX_RESULT < MAX_ALBUMS
#  error Wrong MAX_RESULT
#endif

int import_dump_mode, index_mode, dump_unimported_mode, repair_binlog_mode;
long max_memory = MAX_MEMORY;
int index_users, header_size;
int binlog_readed;
int write_only;

int jump_log_ts;
long long jump_log_pos;
unsigned int jump_log_crc32;
char *index_name;

long long cmd_load_user = 0, cmd_unload_user = 0;
double cmd_load_user_time = 0.0, cmd_unload_user_time = 0.0;
double max_cmd_load_user_time = 0.0, max_cmd_unload_user_time = 0.0;

long long total_photos;

const char *mode_names[MODE_MAX] = {"photo", "video", "audio"};
mode_type mode = MODE_PHOTO;


#define DEBUG_BUFF_SIZE (1 << 26)

char debug_buff[DEBUG_BUFF_SIZE];
char *ds;
int debug_error;

void debug (char const *msg, ...) {
  va_list args;

  va_start (args, msg);
  int left = DEBUG_BUFF_SIZE - 1 - (ds - debug_buff);
  int wr = vsnprintf (ds, left, msg, args);

  if (wr < left) {
    ds += wr;
  } else {
    ds += left;
    debug_error = 1;
  }
}

void debug_init (void) {
  ds = debug_buff;
  debug_error = 0;
  debug_buff[0] = 0;
}

int photo_replay_logevent (struct lev_generic *E, int size);


/***
  Types
 ***/

inline int is_letter (char x) {
  return ('a' <= x && x <= 'z') || ('A' <= x && x <= 'Z') || ('0' <= x && x <= '9') || ('_' == x);
}

int is_name (char *s) {
  if (s == NULL || *s == 0 || strlen (s) >= MAX_NAME_LEN) {
    return 0;
  }
  while (*s) {
    if (!is_letter (*s)) {
      return 0;
    }
    s++;
  }
  return 1;
}

const char *ttt[] = {"int", "long", "double", "string", "raw"};

inline int get_type (char *b) {
  int i;
  for (i = 0; i < 5; i++) {
    if (strcmp (b, ttt[i]) == 0) {
      return i;
    }
  }

  return -1;
}

#pragma pack(push, 4)

typedef struct {
  int mask;
  short data_len;
  short sn;
  char data[0];
} event;

typedef struct {
  char type;
  short shift;

  int is_const;
  int id;
  char *name;
} field_desc;

typedef struct {
  int field_i, field_n;
  int shifts_len;
  char *name;
  field_desc *fields;
} type_desc;


typedef struct {
  enum {ch_event, ch_fields} tp;
  union {
    event *e;
    field *f;
  };
  int fn;
} my_change;


typedef event my_dyn_object;
typedef event my_object;

#pragma pack(pop)

type_desc types[MAX_TYPE];
#define MAX_NAME_LEN 256

int eq_total;
long long events_memory;

inline int get_event_size (event *e) {
  return offsetof (event, data) + e->data_len;
}

void event_free (event *e) {
  if (e == NULL) {
    return;
  }

  long long size = get_event_size (e);
  events_memory -= size;

  eq_total--;
//  dbg ("event_free (addr = %p) (size = %lld)\n", e, size);
  dl_free (e, size);
}

event *event_malloc (int data_len) {
  int size = offsetof (event, data) + data_len;

  events_memory += size;
  eq_total++;

  event *e = dl_malloc (size);
  e->data_len = data_len;
//  dbg ("event_malloc (addr = %p) (size = %d/%d)\n", e, size, get_event_size (e));
  return e;
}

event *event_dup (event *e) {
  int size = get_event_size (e);
  event *ne = dl_malloc (size);
  memcpy (ne, e, size);
  return ne;
}

#define GET_INT(e, f) ((int *)GET_FIELD (e, f))
#define GET_LONG(e, f) ((long long *)GET_FIELD (e, f))
#define GET_DOUBLE(e, f) ((double *)GET_FIELD (e, f))
#define GET_STRING(e, f) (GET_FIELD (e, f))
#define GET_RAW(e, f) (GET_FIELD (e, f))

#define RAW_LEN(v) (*(short *)(v))
#define RAW_RLEN(v) (*(short *)(v) + sizeof (short))
#define RAW_DATA(v) ((char *)(v) + sizeof (short))

//ONLY FOR TEMPORARY RAW DATA
char *raw_tmp_create (char *v, int n) {
  assert (0 <= n && n < 32768);
  char *t = tmp_mem_alloc (n + sizeof (short));
  RAW_LEN(t) = n;
  memcpy (t + sizeof (short), v, n);
  return t;
}

inline char *GET_FIELD (event *e, field_desc *f) {
  if (likely (f->shift < e->sn)) {
    if (f->type == t_string || f->type == t_raw) {
      assert ((int)*(short *)&e->data[f->shift] >= 0);
      return &e->data[e->sn + (int)*(short *)&e->data[f->shift]];
    } else {
      assert (e->data[f->shift] != -1);
      return &e->data[e->sn + (int)(unsigned char)e->data[f->shift]];
    }
  }
  return NULL;
}

char *get_types (void) {
  debug_init();

  int i, j, f = 0;
  for (i = 1; i < MAX_TYPE; i++) {
    if (types[i].name != NULL) {
      if (f) {
        debug ("\n");
      } else {
        f = 1;
      }
      debug ("[%s]:%d\n", types[i].name, i);
      for (j = 0; j < types[i].field_i; j++) {
        debug ("  %s:%s\n", types[i].fields[j].name, ttt[(int)types[i].fields[j].type]);
      }
    }
  }

  return debug_buff;
}

int write_str (char *d, int mx_len, char *s) {
  int tmp = mx_len;
  while (*s) {
#define C(x, y) case x: if (likely (--mx_len > 0)) *d++ = '\\'; if (likely (--mx_len > 0)) *d++ = y; break;
    switch (*s) {
      C('"', '"');
      C('\\', '\\');
      C('/', '/');
      C('\b', 'b');
      C('\f', 'f');
      C('\n', 'n');
      C('\r', 'r');
      C('\t', 't');
      default:
        if (likely (--mx_len > 0)) *d++ = *s;
    }
#undef C
    s++;
  }
  *d = 0;
  return  tmp - mx_len;
}

inline int has_field (event *e, int id) {
  return ((long long)e->mask >> id) & 1;
}

int get_field_id_len (type_desc *t, char *s, int s_len) {
  int i;
  for (i = 0; i < t->field_i; i++) {
    if (!strncmp (t->fields[i].name, s, s_len) && t->fields[i].name[s_len] == 0) {
      return i;
    }
  }

  return -1;
}

int get_field_id (type_desc *t, char *s) {
  return get_field_id_len (t, s, strlen (s));
}

void type_free (type_desc *t) {
  dl_strfree (t->name);
  int j;
  for (j = 0; j < t->field_i; j++) {
    dl_strfree (t->fields[j].name);
  }
  dl_free (t->fields, sizeof (field_desc) * t->field_n);
}

void add_field (type_desc *desc, char *name, int type, int is_const) {
  assert (type >= 0);
  if (desc->field_i == desc->field_n) {
    int nn = desc->field_n + 1;
    if (unlikely (nn <= 0)) {
      nn = 1;
    }
    desc->fields = dl_realloc0 (desc->fields, sizeof (field_desc) * nn, sizeof (field_desc) * desc->field_n);
    desc->field_n = nn;
  } else {
    assert (0);
  }

  int i = desc->field_i++;
  desc->fields[i].type = type;
  desc->fields[i].name = dl_strdup (name);
  desc->fields[i].is_const = is_const;
  desc->fields[i].id = i;

  desc->fields[i].shift = desc->shifts_len;
  desc->shifts_len += (type == t_string || type == t_raw) ? 2 : 1;
}

char *create_type_buf (char *buff) {
  int commas[2], cn = 0, i, j;
  char *name, *val, *desc;

  for (i = 0; buff[i] && cn < 2; i++) {
    if (buff[i] == ',') {
      commas[cn++] = i;
      buff[i] = 0;
    }
  }
  assert (cn == 2);

  name = buff;
  val = buff + commas[0] + 1;
  desc = buff + commas[1] + 1;

  int type_id;
  if (sscanf (val, "%d", &type_id) != 1) {
    char *res = dl_pstr ("Can't parse type_id as number ([%s] found).", val);
    return res;
  }

  if (!(1 <= type_id && type_id < MAX_TYPE)) {
    return dl_pstr ("Type_id [%d] not in range [1;%d].", type_id, MAX_TYPE - 1);
  }

  if (types[type_id].name != NULL) {
    return dl_pstr ("Type_id [%d] is already used for [%s].", type_id, types[type_id].name);
  }

  if (!is_name (name)) {
    return dl_pstr ("Not a valid name [%s] for type_name.", name);
  }

  char *fnames[MAX_FIELDS];
  int ftypes[MAX_FIELDS];
  int fconst[MAX_FIELDS];
  int fn = 0;

  i = 0;
  while (desc[i]) {
    j = i;
    while (desc[j] != ':' && desc[j]) {
      j++;
    }
    if (!desc[j]) {
      return dl_pstr ("Can't parse type_description ¹%d (':' between field_name and field_type not found)", fn + 1);
    }

    int k = j + 1;
    while (desc[k] != ';' && desc[k]) {
      k++;
    }

    desc[j] = 0;
    desc[k] = 0;

    char *a = desc + i, *b = desc + j + 1;

    int is_const = 0;
    if (*a == '!') {
      a++;
      is_const = 1;
    }
    if (!is_name (a)) {
      return dl_pstr ("Not a valid name [%s] for field_name.", a);
    }

    int type = get_type (b);
    if (type == -1) {
      return dl_pstr ("Not a valid type [%s] for field_type.", a);
    }

    if (fn >= MAX_FIELDS) {
      return dl_pstr ("Max number(%d) of fields for type [%s] exceeded. Contact soft developers for help.", MAX_FIELDS, name);
    }

    fconst[fn] = is_const;
    fnames[fn] = a;
    ftypes[fn++] = type;


    i = k + 1;
  }

  for (i = 0; i < fn; i++) {
    for (j = i + 1; j < fn; j++) {
      if (strcmp (fnames[i], fnames[j]) == 0) {
        return dl_pstr ("The same field_name [%s] used for fields ¹%d and ¹%d.", fnames[i], i, j);
      }
    }
  }

  types[type_id].name = dl_strdup (name);

  types[type_id].fields = NULL;
  types[type_id].shifts_len = 0;
  types[type_id].field_i = types[type_id].field_n = 0;

  for (i = 0; i < fn; i++) {
    add_field (&types[type_id], fnames[i], ftypes[i], fconst[i]);
  }

  return "OK";
}

int photo_type_id,
    photo_type_owner_id,
    photo_type_album_id,
    photo_type__location,
    photo_type__original_location,
    album_type_id,
    album_type_owner_id;


void init_types (void) {
  tmp_mem_init();

  switch (mode) {
    case MODE_PHOTO:
      assert (create_type_buf (tmp_mem_strdup (
                     "photo," DL_STR (PHOTO_TYPE) ","
                     "!id:int;"//"id:const int;"
                     "album_id:int;"
                     "!owner_id:int;"//"owner_id:const int;"
                     "user_id:int;"
                     "height:int;"
                     "width:int;"
                     "_location:raw;"
                     "_original_location:raw"
                     ))[0] == 'O');

//Add new fields only to the end of description

      assert (create_type_buf (tmp_mem_strdup (
                     "album," DL_STR (ALBUM_TYPE) ","
                     "!id:int;"//"id:const int;"
                     "!owner_id:int;"//"owner_id:const int;"
                     "title:string;"
                     "description:string"
                     ))[0] == 'O');
//Add new fields only to the end of description
      break;
    case MODE_VIDEO:
      assert (create_type_buf (tmp_mem_strdup (
                     "photo," DL_STR (PHOTO_TYPE) ","
                     "!id:int;"//"id:const int;"
                     "album_id:int;"
                     "!owner_id:int;"//"owner_id:const int;"
                     "date:int;"
                     "title:string;"
                     "description:string;"
                     "_location:raw;"
                     "flags:int"
                     ))[0] == 'O');
//Add new fields only to the end of description

      assert (create_type_buf (tmp_mem_strdup (
                     "album," DL_STR (ALBUM_TYPE) ","
                     "!id:int;"//"id:const int;"
                     "!owner_id:int;"//"owner_id:const int;"
                     "title:string"
                     ))[0] == 'O');
//Add new fields only to the end of description
      break;
    case MODE_AUDIO:
      assert (create_type_buf (tmp_mem_strdup (
                     "photo," DL_STR (PHOTO_TYPE) ","
                     "!id:int;"//"id:const int;"
                     "album_id:int;"
                     "!owner_id:int;"//"owner_id:const int;"
                     "uploaded:int;"
                     "duration:int;"
                     "_location:raw"
                     ))[0] == 'O');
//Add new fields only to the end of description

      assert (create_type_buf (tmp_mem_strdup (
                     "album," DL_STR (ALBUM_TYPE) ","
                     "!id:int;"//"id:const int;"
                     "!owner_id:int;"//"owner_id:const int;"
                     "title:string"
                     ))[0] == 'O');
//Add new fields only to the end of description
      break;
    default:
      assert ("Mode not supported" && 0);
  }

#define SAVE_FIELD_ID_(field_name, type_name, TYPE_NAME) type_name ## _type_ ## field_name = get_field_id (&types[TYPE_NAME ## _TYPE], #field_name)
#define SAVE_FIELD_ID(field_name, type_name, TYPE_NAME) SAVE_FIELD_ID_(field_name, type_name, TYPE_NAME)
  SAVE_FIELD_ID(                id, photo, PHOTO);
  SAVE_FIELD_ID(          owner_id, photo, PHOTO);
  SAVE_FIELD_ID(          album_id, photo, PHOTO);
  SAVE_FIELD_ID(         _location, photo, PHOTO);
  SAVE_FIELD_ID(_original_location, photo, PHOTO);

  SAVE_FIELD_ID(                id, album, ALBUM);
  SAVE_FIELD_ID(          owner_id, album, ALBUM);
#undef SAVE_FIELD_ID
}

int get_photo_id (event *e) {
  return *GET_INT (e, &types[PHOTO_TYPE].fields[photo_type_id]);
}

int get_album_id (event *e) {
  return *GET_INT (e, &types[ALBUM_TYPE].fields[album_type_id]);
}

void free_types (void) {
  int i;
  for (i = 0; i < MAX_TYPE; i++) {
    if (types[i].name != NULL) {
      type_free (&types[i]);
    }
  }
}


/***
  PHP-array and TL
 ***/

int base64url_to_secret (const char *input, unsigned long long *secret) {
  int r = base64url_decode (input, (unsigned char *)secret, 8);
  if (r < 0) {
    return r;
  }
  if (r != 8) {
    return -7;
  }
  return 0;
}


#define MAX_LOCATIONS ((26 * 4 + 1) * 2)

typedef struct {
  int is_location_engine;
  int v_fid;
  union {
    struct {
      char size;
      int rotate;
      int server;
      int volume_id;
      int local_id;
      int extra;
      unsigned long long secret;
    } location_engine;
    struct {
      int server_id;
      int server_id2;
      int orig_owner_id;
      int orig_album_id;
      int photo_len;
      char photo[127];
    } location_old;
  };
} location;

int location_changes_n;
location location_changes[MAX_LOCATIONS];

int field_changes_n;
field field_changes[MAX_FIELDS + 1];

int tmp_field_changes_n;
field tmp_field_changes[MAX_FIELDS + 1];

#define TMP_ADD_CHANGE(fid, tp, val)                             \
  tmp_field_changes[tmp_field_changes_n].type = DL_CAT (t_, tp); \
  tmp_field_changes[tmp_field_changes_n].v_fid = fid;            \
  tmp_field_changes[tmp_field_changes_n].DL_CAT (v_, tp) = val;  \
  tmp_field_changes_n++;

int php_get_fields (int type_id, char *l) {
  type_desc *t = &types[type_id];
  int n;
  int len = strlen (l);
  int add;
  field_changes_n = -1;
  if (sscanf (l, "a:%d%n", &n, &add) != 1 || l[add] != ':' || l[add + 1] != '{' || l[len - 1] != '}' || n < 0 || n > MAX_FIELDS || len >= MAX_EVENT_SIZE) {
    return -1;
  }
  int i, j = add + 2;

  for (i = 0; i < n; i++) {
    int clen;
    if (sscanf (l + j, "s:%d%n", &clen, &add) != 1 || l[j + add] != ':' || l[j + add + 1] != '"') {
      return -1;
    }
    j += add + 2;

    if (j + 3 + clen >= len) {
      return -1;
    }

    int id = get_field_id_len (t, l + j, clen);

    if (id < 0) {
      return -1;
    }

    field_changes[i].v_fid = id;
    int f_type = field_changes[i].type = t->fields[id].type;

    // we can't change raw fields from php
    if (f_type == t_raw) {
      return -1;
    }

    j += clen;

    if (l[j++] != '"') {
      return -1;
    }
    if (l[j++] != ';') {
      return -1;
    }

    if (l[j] != 's' && l[j] != 'i' && l[j] != 'd') {
      return -1;
    }

    int is_str;
    if (l[j] == 's') {
      is_str = 1;
      j++;
      if (sscanf (l + j, ":%d%n", &clen, &add) != 1 || l[j + add] != ':') {
        return -1;
      }
      j += add;

      if (j + clen + 2 >= len) {
        return -1;
      }
    } else {
      is_str = 0;
      j++;
    }

    if (l[j++] != ':') {
      return -1;
    }

    if (is_str) {
      if (l[j++] != '"') {
        return -1;
      }
    }

    int field_end = 0;
    if (f_type == t_string) {
      if (!is_str) {
        clen = 0;
        if (j + clen + 1 < len && l[j + clen] != ';') {
          clen++;
        }
      }

      field_changes[i].v_string = l + j;
      field_changes[i].v_string_len = clen;

      field_end = clen;
    } else {
      switch (f_type) {
        case t_int:
          if (sscanf (l + j, "%d%n", &field_changes[i].v_int, &field_end) != 1) {
            return -1;
          }
          break;
        case t_long:
          if (sscanf (l + j, "%lld%n", &field_changes[i].v_long, &field_end) != 1) {
            return -1;
          }
          break;
        case t_double:
          if (sscanf (l + j, "%lf%n", &field_changes[i].v_double, &field_end) != 1) {
            return -1;
          }
          break;
        default:
          assert (0);
      }
    }

    j += field_end;

    if (is_str) {
      if (field_end != clen) {
        return -1;
      }

      if (l[j++] != '"') {
        return -1;
      }
    }

    if (l[j++] != ';') {
      return -1;
    }
  }

  if (j != len - 1) {
    return -1;
  }
  return field_changes_n = n;
}

int rpc_get_fields (int type_id, long long mask, char *s, int len) {
  if (len % sizeof (int) != 0 || len == 0) {
    return -1;
  }

  type_desc *t = &types[type_id];
  if ((mask & ((1ll << t->field_i) - 1)) != mask) {
    tl_fetch_set_error_format (TL_ERROR_BAD_VALUE, "Wrong mask = %llx specified", mask);
    return -1;
  }

  int *a = (int *)s;
  int *a_end = (int *)(s + len);

  int id;
  field_changes_n = 0;
  location_changes_n = 0;
  for (id = 0; id < t->field_i; id++) {
    if ((mask >> id) & 1) {
      field_changes[field_changes_n].v_fid = id;
      int f_type = field_changes[field_changes_n].type = t->fields[id].type;

      switch (f_type) {
        case t_int:
          if (a_end - a < 1) {
            return -1;
          }
          field_changes[field_changes_n].v_int = *a;
          a++;
          break;
        case t_long:
          if (a_end - a < 2) {
            return -1;
          }
          field_changes[field_changes_n].v_long = *(long long *)a;
          a += 2;
          break;
        case t_double:
          if (a_end - a < 2) {
            return -1;
          }
          field_changes[field_changes_n].v_double = *(double *)a;
          a += 2;
          break;
        case t_string: {
          if (a_end == a) {
            return -1;
          }
          char *str = (char *)a;
          a++;
          int result_len = (unsigned char)*str++;
          if (result_len < 254) {
            if (a_end - a < (result_len >> 2)) {
              return -1;
            }
            a += (result_len >> 2);
          } else if (result_len == 254) {
            result_len = (unsigned char)str[0] + ((unsigned char)str[1] << 8) + ((unsigned char)str[2] << 16);
            str += 3;
            if (a_end - a < ((result_len + 3) >> 2)) {
              return -1;
            }
            a += ((result_len + 3) >> 2);
          } else {
            return -1;
          }

          field_changes[field_changes_n].v_string_len = result_len;
          field_changes[field_changes_n].v_string = str;
          break;
        }
        case t_raw: {
          if (a_end - a < 1) {
            return -1;
          }
          int cnt = *a++;
          while (cnt--) {
            if (location_changes_n == MAX_LOCATIONS) {
              tl_fetch_set_error_format (TL_ERROR_BAD_VALUE, "Too much locations specified");
              return -1;
            }
            if (a_end - a < 1) {
              return -1;
            }
            location_changes[location_changes_n].v_fid = id;
            int type = *a++;
            if (type == TL_PHOTO_PHOTO_LOCATION || type == TL_PHOTO_AUDIO_LOCATION || type == TL_PHOTO_VIDEO_LOCATION) {
              location_changes[location_changes_n].is_location_engine = 0;
              if (a_end - a < 5) {
                return -1;
              }
              location_changes[location_changes_n].location_old.server_id = *a++;
              location_changes[location_changes_n].location_old.server_id2 = *a++;
              location_changes[location_changes_n].location_old.orig_owner_id = *a++;
              location_changes[location_changes_n].location_old.orig_album_id = *a++;

              char *str = (char *)a;
              a++;
              int photo_len = (unsigned char)*str++;
              if (photo_len < 127) {
                if (a_end - a < (photo_len >> 2)) {
                  return -1;
                }
                a += (photo_len >> 2);
              } else {
                return -1;
              }

              location_changes[location_changes_n].location_old.photo_len = photo_len;
              memcpy (location_changes[location_changes_n].location_old.photo, str, photo_len);
            } else if (type == TL_PHOTO_PHOTO_LOCATION_STORAGE || type == TL_PHOTO_AUDIO_LOCATION_STORAGE || type == TL_PHOTO_VIDEO_LOCATION_STORAGE) {
              location_changes[location_changes_n].is_location_engine = 1;
              if (a_end - a < 7) {
                return -1;
              }
              location_changes[location_changes_n].location_engine.rotate = *a++;
              int size = *a++;
              if ((size & 255) != 1 || (size >> 16) != 0) {
                tl_fetch_set_error_format (TL_ERROR_BAD_VALUE, "Wrong size specified. Size must contain exactly one symbol 'a' - 'z'");
                return -1;
              }
              location_changes[location_changes_n].location_engine.size = (size >> 8) & 255;
              location_changes[location_changes_n].location_engine.server = *a++;
              location_changes[location_changes_n].location_engine.volume_id = *a++;
              location_changes[location_changes_n].location_engine.local_id = *a++;
              location_changes[location_changes_n].location_engine.extra = *a++;

              char *str = (char *)a;
              a++;
              int secret_len = (unsigned char)*str++;
              char base64url_secret[12];
              if (secret_len < 12) {
                if (a_end - a < (secret_len >> 2)) {
                  return -1;
                }
                a += (secret_len >> 2);
              } else {
                tl_fetch_set_error_format (TL_ERROR_BAD_VALUE, "Url_secret is too long");
                return -1;
              }

              memcpy (base64url_secret, str, secret_len);
              base64url_secret[secret_len] = 0;

              if (base64url_to_secret (base64url_secret, &location_changes[location_changes_n].location_engine.secret) != 0) {
                tl_fetch_set_error_format (TL_ERROR_BAD_VALUE, "Can't decode url_secret");
                return -1;
              }
            } else {
              return -1;
            }

            location_changes_n++;
          }
          field_changes_n--;
          break;
        }
      }

      field_changes_n++;
    }
  }

  if (a != a_end) {
    tl_fetch_set_error_format (TL_ERROR_BAD_VALUE, "Not all data fetched");
    return -1;
  }
  return field_changes_n;
}


typedef struct {
  int tn, tp[MAX_FIELDS + 1], id[MAX_FIELDS + 1], size, type_id;
  char tval[MAX_FIELDS + 1][8];
} event_dump;

void event_dump_init (event_dump *d, int type_id) {
  d->tn = 0;
  d->type_id = type_id;
  d->size = types[type_id].shifts_len;
}

// type_id, tn ans size MUST be already initialized
int event_dump_from_event (event_dump *d, event *e, int type_id, event *v) {
  int i, j;

  if (e == NULL) {
    return 0;
  }

  assert (d->type_id == type_id);

//  d->tn = 0;
//  d->size = 0;
//  d->type_id = type_id;

  field_desc *f = types[type_id].fields;

  int tn = d->tn;

  for (i = 0; i < e->sn; i++) {
    if (has_field (e, i)) {
      char *dest;
      int flag = 0, add = 0;
      if (v != NULL && has_field (v, i) && f[i].type != t_string && f[i].type != t_raw) {
        dest = GET_FIELD (v, &f[i]);
      } else {
        dest = d->tval[d->tn];
        flag = 1;
      }

      j = tn;
      switch (f[i].type) {
      case t_int:
        *(int *)dest = *GET_INT (e, &f[i]);
        add = sizeof (int);
        break;
      case t_long:
        *(long long *)dest = *GET_LONG (e, &f[i]);
        add = sizeof (long long);
        break;
      case t_double:
        *(double *)dest = *GET_DOUBLE (e, &f[i]);
        add = sizeof (double);
        break;
      case t_string:
        //TODO: it can be optimized
        for (j = 0; j < tn; j++) {
          if (d->id[j] == i) {
            break;
          }
        }

        if (j == tn) {
          *(char **)d->tval[d->tn] = tmp_mem_strdup (GET_STRING (e, &f[i]));
          add = strlen (*(char **)d->tval[d->tn]) + 1;
        }
        break;
      case t_raw:
        for (j = 0; j < tn; j++) {
          if (d->id[j] == i) {
            break;
          }
        }

        if (j == tn) {
          char *x = GET_RAW (e, &f[i]);
          add = RAW_RLEN (x);
          *(char **)d->tval[d->tn] = tmp_mem_dup (x, add);
        }
        break;
      }
      if (flag && j == tn) {
        d->id[d->tn] = i;
        d->tp[d->tn] = f[i].type;
        d->size += add;
        d->tn++;
      }
    }
  }
  return 0;
}


//apply changes to e if possible
int event_dump_from_str (event_dump *d, char *buff, int type_id, event *e) {
  assert (d->type_id == type_id);

  static const char delim = 1;

  char *desc = buff;

  int valn[MAX_FIELDS], fn = 0;
  char *val[MAX_FIELDS];

  int i = 0, j, k;
  int dn = strlen (desc);
  desc[dn + 1] = 0;
  while (desc[i]) {
    if (fn == MAX_FIELDS) {
      return -1;
    }

    j = i;
    while (desc[j] != ':' && desc[j]) {
      j++;
    }
    if (desc[j] != ':') {
      return -1;
    }

    desc[j] = 0;

    int fid = get_field_id_len (&types[type_id], desc + i, j - i);

    if (fid < 0) {
      return -1;
    }

    d->id[fn] = fid;

    k = ++j;
    while (desc[k] != delim && desc[k]) {
      k++;
    }
    desc[k] = 0;

    d->tp[fn] = types[type_id].fields[fid].type;

    //we can't change raw fields from string
    if (d->tp[fn] == t_raw) {
      return -1;
    }

    valn[fn] = k - j;
    val[fn++] = desc + j;


    i = k + 1;
  }

  field_desc *f = types[type_id].fields;


  for (i = 0; i < fn;  i++) {
    char *dest;
    int flag = 0;
    if (e != NULL && has_field (e, d->id[i]) && d->tp[i] != t_string) {
      dest = GET_FIELD (e, &f[d->id[i]]);
    } else {
      dest = d->tval[d->tn];
      flag = 1;
    }
    int nn, add = 0;
    switch (d->tp[i]) {
    case t_int:
      if (sscanf (val[i], "%d%n", (int *)dest, &nn) != 1 || nn != valn[i]) {
        return -1;
      }
      add = 4;
      break;
    case t_long:
      if (sscanf (val[i], "%lld%n", (long long *)dest, &nn) != 1 || nn != valn[i]) {
        return -1;
      }
      add = 8;
      break;
    case t_double:
      if (sscanf (val[i], "%lf%n", (double *)dest, &nn) != 1 || nn != valn[i]) {
        return -1;
      }
      add = 8;
      break;
    case t_string:
      *(char **)dest = tmp_mem_strdup (val[i]);
      add = valn[i] + 1;
      break;
    default:
      assert (0);
    }
    if (flag) {
      d->tp[d->tn] = d->tp[i];
      d->id[d->tn] = d->id[i];
      d->size += add;
      d->tn++;
    }
  }
//  d->size += types[type_id].shifts_len;
//  d->type_id = type_id;

  return 0;
}

//apply changes to e if possible
int event_dump_from_field (event_dump *d, field *f, int type_id, event *e) {
  assert (d->type_id == type_id);

  field_desc *ff = types[type_id].fields;
  char *dest;
  int flag = 0;
  if (e != NULL && has_field (e, f->v_fid) && f->type != t_string && f->type != t_raw) {
    dest = GET_FIELD (e, &ff[f->v_fid]);
  } else {
    dest = d->tval[d->tn];
    flag = 1;
  }

  int add = 0;
  switch (f->type) {
  case t_int:
    *(int *)dest = f->v_int;
    add = 4;
    break;
  case t_long:
    *(long long *)dest = f->v_long;
    add = 8;
    break;
  case t_double:
    *(double *)dest = f->v_double;
    add = 8;
    break;
  case t_string:
    *(char **)dest = tmp_mem_strdupn (f->v_string, f->v_string_len);
    add = f->v_string_len + 1;
    break;
  case t_raw:
    add = f->v_raw_len + sizeof (short);
    *(char **)dest = raw_tmp_create (f->v_raw, f->v_raw_len);
    break;
  default:
    assert (0);
  }

  if (flag) {
    d->tp[d->tn] = f->type;
    d->id[d->tn] = f->v_fid;
    d->size += add;
    d->tn++;
  }

  //d->size += types[type_id].shifts_len;

  return 0;
}


event *create_event (event_dump *d) {
  pair_p_int v[MAX_FIELDS];
  int i;
  for (i = 0; i < d->tn; i++) {
    v[i].y = i;
    v[i].x.x = d->tp[i];
    v[i].x.y = d->id[i];
  }
  qsort (v, d->tn, sizeof (pair_p_int), DL_QCMP(pair_p_int));
  for (i = 1; i < d->tn; i++) {
    if (d->id[v[i].y] == d->id[v[i - 1].y]) {
      dbg ("create_event: %s dublicated\n", types[d->type_id].fields[d->id[v[i - 1].y]].name);
      return NULL;
    }
  }

  event *e;
  e = event_malloc (d->size);

  e->mask = 0;
  e->sn = types[d->type_id].shifts_len;
  memset (e->data, -1, e->sn);
  char *s = e->data + e->sn, *st = s;

  int j;
  for (j = 0; j < d->tn; j++) {
    int shift = types[d->type_id].fields[v[j].x.y].shift;
    i = v[j].y;

    e->mask |= 1 << d->id[i];
    char *t;
    switch (d->tp[i]) {
    case t_int:
      e->data[shift] = s - st;
      WRITE_INT (s, *(int *)d->tval[i]);
      break;
    case t_long:
    case t_double:
      e->data[shift] = s - st;
      WRITE_LONG (s, *(long long *)d->tval[i]);
      break;
    case t_string:
      *(short *)&e->data[shift] = s - st;
      t = *(char **)d->tval[i];
      while (*t) {
        *s++ = *t++;
      }
      *s++ = 0;
      break;
    case t_raw:
      *(short *)&e->data[shift] = s - st;
      t = *(char **)d->tval[i];
      int len = RAW_RLEN (t);
      memcpy (s, t, len);
      s += len;
      break;
    default:
      assert (0);
    }
  }

  return e;
}

inline void event_fix (event **e, event *v) {
  if (*e != v && v != NULL) {
    event_free (*e);
    *e = v;
  }
}

event *event_update_str (event *e, char *chg, int type_id) {
  if (chg == NULL) {
    return e;
  }

  tmp_mem_init();

  event_dump d;
  event_dump_init (&d, type_id);

  if (event_dump_from_str (&d, chg, type_id, e) < 0) {
    return NULL;
  }

  if (d.tn) {
    assert (event_dump_from_event (&d, e, type_id, NULL) > -1);
    e = create_event (&d);
    assert (e != NULL);
  }
  return e;
}

event *event_update_php (event *e, field *f, int fn, int type_id) {
  if (fn == 0) {
    return e;
  }

  tmp_mem_init();

  event_dump d;
  event_dump_init (&d, type_id);

  int i;
  for (i = 0; i < fn; i++) {
    if (event_dump_from_field (&d, &f[i], type_id, e) < 0) {
      return NULL;
    }
  }
//  dbg ("event_update_php: (tn = %d) (fn = %d)\n", d.tn, fn);
  if (d.tn) {
    assert (event_dump_from_event (&d, e, type_id, NULL) > -1);

    e = create_event (&d);
    assert (e != NULL);
  }
  return e;
}

event *event_update_event (event *e, event *v, int type_id) {
  if (v == NULL) {
    return e;
  }

  tmp_mem_init();

  event_dump d;
  event_dump_init (&d, type_id);

  assert (event_dump_from_event (&d, v, type_id, e) > -1);

  if (d.tn) {
    event_dump_from_event (&d, e, type_id, NULL);

    e = create_event (&d);
    assert (e != NULL);
  }
  return e;
}



char *event_to_str (event *e, int type_id, int *ids, int n) {
  static char buff[2 * MAX_EVENT_SIZE + 1];
  char *s = buff, *en = buff + 2 * MAX_EVENT_SIZE + 1;
  type_desc *t = &types[type_id];

  int i, ii;
  int L, R, tp;
  if (ids == NULL) {
    tp = 0;
    L = 0;
    R = t->field_i;
  } else {
    tp = 1;
    L = 0;
    R = n;
  }

  s += snprintf (s, max (en - s, 0), "{");
  int w = 0;

  for (ii = L; ii < R; ii++) {
    if (tp) {
      i = ids[ii];
    } else {
      i = ii;
    }
    if (i < 0 || has_field (e, i)) {
      field_desc *f = &t->fields[i];
      char *alias = f->name;
      if (w) {
        s += snprintf (s, max (en - s, 0), ",");
      }
      w = 1;
      switch (f->type) {
        case t_int:
          s += snprintf (s, max (en - s, 0), "\"%s\":\"%d\"", alias, *GET_INT(e, f));
          break;
        case t_long:
          s += snprintf (s, max (en - s, 0), "\"%s\":\"%lld\"", alias, *GET_LONG(e, f));
          break;
        case t_double:
          s += snprintf (s, max (en - s, 0), "\"%s\":\"%.6lf\"", alias, *GET_DOUBLE(e, f));
          break;
        case t_string:
          s += snprintf (s, max (en - s, 0), "\"%s\":\"", alias);
          s += write_str (s, max (en - s, 0), GET_STRING(e, f));
          s += snprintf (s, max (en - s, 0), "\"");
          break;
        case t_raw:
          s += snprintf (s, max (en - s, 0), "\"%s\":\"<raw data>\"", alias);
          break;
        default:
          assert (0);
      }
    }
  }
  s += snprintf (s, max (en - s, 0), "}");

  if (s > en) {
    debug_error = 1;
  }
  return buff;
}

inline int event_get_photo_id (actual_object *o) {
  if (o->obj == NULL) {
    assert (o->dyn != NULL);
    return get_photo_id (o->dyn);
  }

  return get_photo_id (o->obj);
}

inline int event_get_album_id (actual_object *o) {
  if (o->obj == NULL) {
    assert (o->dyn != NULL);
    return get_album_id (o->dyn);
  }

  return get_album_id (o->obj);
}

int event_get_ordering (actual_object *o) {
  //TODO
  return -1;
}

int get_server (int vid);

int event_get_field_int (actual_object *o, int type, int field_num) {
  if (field_num != -1) {
    field_desc *f = &types[type].fields[field_num];

    if (o->dyn != NULL && has_field (o->dyn, field_num)) {
      return *GET_INT (o->dyn, f);
    } else if (o->obj != NULL && has_field (o->obj, field_num)) {
      return *GET_INT (o->obj, f);
    }
  }
  return INT_MIN;
}

char *event_get_location (actual_object *o, int original_location, int *len) {
  event *e = o->obj, *e2 = o->dyn;
  int field_num = original_location ? photo_type__original_location : photo_type__location;

  if (field_num != -1) {
    field_desc *f = &types[PHOTO_TYPE].fields[field_num];

    char *x = NULL;
    if (e2 != NULL && has_field (e2, field_num)) {
      x = GET_RAW (e2, f);
    } else if (e != NULL && has_field (e, field_num)) {
      x = GET_RAW (e, f);
    }
    if (x != NULL) {
      *len = RAW_LEN (x);
      return RAW_DATA (x);
    }
  }
  *len = 0;
  return "";
}

char location_field_name[21] = "original_location";

char *event_to_array (actual_object *o, int type_id, int *ids, int n) {
  event *e = o->obj, *e2 = o->dyn;

  static char buff[2 * MAX_EVENT_SIZE + 1], buff2[101];
  char *s = buff, *en = buff + 2 * MAX_EVENT_SIZE;
  type_desc *t = &types[type_id];

  int i, ii;
  int L, R, tp;
  if (ids == NULL || n == 0) {
    tp = 0;
    L = 0;
    R = t->field_i + 2;
  } else {
    tp = 1;
    L = 0;
    R = n;
  }

  int add, fi = 0;
  s += snprintf (s, max (en - s, 0), "a:%n000000:{", &add);

  int need_location = 0;
  for (ii = L; ii < R; ii++) {
    if (tp) {
      i = ids[ii];
    } else {
      if (ii < t->field_i) {
        i = ii;
      } else {
        i = MAX_FIELDS + 128 * (ii - t->field_i);
      }
    }
    assert (i <= MAX_FIELDS + 256);
    if (i == MAX_FIELDS + 256) {
      char *key = "ordering";
      int ordering = event_get_ordering (o);

      fi++;
      s += snprintf (s, max (en - s, 0), "s:%d:\"%s\";", (int)strlen (key), key);
      s += snprintf (s, max (en - s, 0), "i:%d;", ordering);
    } else if (i >= MAX_FIELDS) {
      int len;
      int original = i >= MAX_FIELDS + 128;
      char *loc = event_get_location (o, original, &len);

      if (loc == NULL || len <= 1) {
        continue;
      }

      if (loc[0] < 0) {
        int cur = -(loc[0] + mode) + 4 * sizeof (int) + 1;
        loc += cur;
      }

      int need = (i - MAX_FIELDS) & 127, found = 0;
      int vid = 0, local_id = 0, sid = 0, j, extra;
      char size = 0, diff;
      unsigned long long secret;

      for (j = *loc++; j > 0; j--) {
        READ_CHAR(loc, size);
        assert (size);
        if ((size > need || size < -need) && need) {
          break;
        }

        if (size < 0) {
          size *= -1;
          READ_CHAR(loc, diff);
          local_id += diff;
        } else {
          READ_INT(loc, vid);
          READ_INT(loc, local_id);
          sid = get_server (vid);
        }
        READ_INT(loc, extra);
        READ_LONG(loc, secret);
        if (need * (size - need) == 0 /* && abs (size) < 32 */ ) {
          char *key = location_field_name;
          key[17] = 'a' - 1 + (size & 31);
          key[18] = (size >> 5) + '0';
          key[19] = 0;

          if (!original) {
            key += 9;
          }

          char local_id_str[9];
          int local_id_len = snprintf (local_id_str, 9, "%x", local_id);
          assert (local_id_len < 9);

          char base64url_secret[12];
          assert (!base64url_encode ((unsigned char *)&secret, 8, base64url_secret, 12));

          fi++;
          s += snprintf (s, max (en - s, 0), "s:%d:\"%s\";", (int)strlen (key), key);
          s += snprintf (s, max (en - s, 0), "a:5:{"
                                             "s:6:\"server\";i:%d;"
                                             "s:9:\"volume_id\";i:%d;"
                                             "s:15:\"volume_local_id\";s:%d:\"%s\";"
                                             "s:5:\"extra\";i:%d;"
                                             "s:6:\"secret\";s:%d:\"%s\";"
                                             "}",
                                             sid,
                                             vid,
                                             local_id_len, local_id_str,
                                             extra,
                                             (int)strlen (base64url_secret), base64url_secret
                                             );


          if (need) {
            found = 1;
            break;
          }
        }
      }

      need_location |= (!need || !found) << original;
    } else {
      event *ev = NULL;
      if (e2 != NULL && has_field (e2, i)) {
        ev = e2;
      } else if (e != NULL && has_field (e, i)) {
        ev = e;
      }
      if (ev != NULL) {
        field_desc *f = &t->fields[i];

        char *key = f->name;
        char *value;
        assert (f->name != NULL);
        if (f->type == t_raw || *f->name == '_') {
          continue;
        }

        if (f->type == t_string) {
          value = GET_STRING(ev, f);
        } else {
          switch (f->type) {
            case t_int: {
              int value = *GET_INT(ev, f);

              snprintf (buff2, 100, "%d", value);
              break;
            }
            case t_long:
              snprintf (buff2, 100, "%lld", *GET_LONG(ev, f));
              break;
            case t_double:
              snprintf (buff2, 100, "%.6lf", *GET_DOUBLE(ev, f));
              break;
            default:
              assert (0);
          }
          value = buff2;
        }

        fi++;
        s += snprintf (s, max (en - s, 0), "s:%d:\"%s\";", (int)strlen (key), key);
        s += snprintf (s, max (en - s, 0), "s:%d:\"%s\";", (int)strlen (value), value);
      }
    }
  }
  if (o->deleted) {
    fi++;
    s += snprintf (s, max (en - s, 0), "s:7:\"deleted\";i:1;");
  }
  int original;
  for (original = 0; original <= 1; original++) {
    if ((need_location >> original) & 1) {
      char *key = location_field_name;
      key[17] = 0;

      if (!original) {
        key += 9;
      }

      int len;
      char *loc = event_get_location (o, original, &len);

      if (loc != NULL && len > 1 && loc[0] < 0) {
        len = -(loc[0] + mode);
        loc += 1;

        int sid, sid2, orig_oid, orig_aid;
        READ_INT(loc, sid);
        READ_INT(loc, sid2);
        READ_INT(loc, orig_oid);
        READ_INT(loc, orig_aid);

        fi++;
        s += snprintf (s, max (en - s, 0), "s:%d:\"%s\";", (int)strlen (key), key);
        s += snprintf (s, max (en - s, 0), "a:5:{"
                                           "s:6:\"server\";i:%d;"
                                           "s:7:\"server2\";i:%d;"
                                           "s:7:\"user_id\";i:%d;"
                                           "s:%d:\"%s\";i:%d;"
                                           "s:5:\"%s\";s:%d:\"",
                                           sid,
                                           sid2,
                                           orig_oid,
                                           mode == MODE_PHOTO ? 10 : 6, mode == MODE_PHOTO ? "orig_album" : "source", orig_aid,
                                           mode_names[mode], len
                                           );

        memcpy (s, loc, min (max (en - s, 0), len));
        s += len;
        s += snprintf (s, max (en - s, 0), "\";}");
      }
    }
  }

  s += snprintf (s, max (en - s, 0), "}");

  if (s > en) {
    debug_error = 1;
  } else {
    sprintf (buff + add, "%06d", fi);
    buff[add + 6] = ':';
  }

//  dbg ("[%s]\n", buff);
  return buff;
}

void event_to_rpc (actual_object *o, int type_id, long long mask) {
  event *e = o->obj, *e2 = o->dyn;
  type_desc *t = &types[type_id];

  int type_name = 0;
  int i;
  for (i = 0; i < t->field_i; i++) {
    if ((mask >> i) & 1) {
      if (e2 == NULL || !has_field (e2, i)) {
        if (e == NULL || !has_field (e, i)) {
          mask -= (1ll << i);
        }
      }
    }
  }

  int add_mask = (o->deleted ? (1 << 29) : 0);
  if (mask >= (1 << 29)) {
    tl_store_int ((mask & ((1 << 29) - 1)) + add_mask + (1 << 30));
    tl_store_int (mask >> 29);
  } else {
    tl_store_int (mask + add_mask);
  }

  for (i = 0; i < t->field_i; i++) {
    if ((mask >> i) & 1) {
      event *ev = NULL;
      if (e2 != NULL && has_field (e2, i)) {
        ev = e2;
      } else if (e != NULL && has_field (e, i)) {
        ev = e;
      }
      assert (ev);

      field_desc *f = &t->fields[i];
      switch (f->type) {
        case t_int: {
          int value = *GET_INT(ev, f);

          tl_store_int (value);
          break;
        }
        case t_long:
          tl_store_long (*GET_LONG(ev, f));
          break;
        case t_double:
          tl_store_double (*GET_DOUBLE(ev, f));
          break;
        case t_string: {
          char *res = GET_STRING(ev, f);
          tl_store_string (res, strlen (res));
          break;
        }
        case t_raw: {
          char *loc = RAW_DATA (GET_RAW (ev, f));
          assert (loc != NULL);

          char *loc_begin = loc;
          int count = 0;
          if (loc[0] < 0) {
            loc += -(loc[0] + mode) + 4 * sizeof (int) + 1;
            count = 1;
          }
          count += *loc;
          loc = loc_begin;
          tl_store_int (count);


          if (loc[0] < 0) {
            switch (mode) {
              case MODE_PHOTO:
                type_name = TL_PHOTO_PHOTO_LOCATION;
                break;
              case MODE_VIDEO:
                type_name = TL_PHOTO_VIDEO_LOCATION;
                break;
              case MODE_AUDIO:
                type_name = TL_PHOTO_AUDIO_LOCATION;
                break;
              default:
                assert (0);
            }
            tl_store_int (type_name);

            int length = -(loc[0] + mode);
            loc++;

            tl_store_raw_data (loc, 4 * sizeof (int));
            loc += 4 * sizeof (int);

            tl_store_string (loc, length);
            loc += length;
          }

          switch (mode) {
            case MODE_PHOTO:
              type_name = TL_PHOTO_PHOTO_LOCATION_STORAGE;
              break;
            case MODE_VIDEO:
              type_name = TL_PHOTO_VIDEO_LOCATION_STORAGE;
              break;
            case MODE_AUDIO:
              type_name = TL_PHOTO_AUDIO_LOCATION_STORAGE;
              break;
            default:
              assert (0);
          }

          int vid = 0, local_id = 0, sid = 0, j, extra;
          char size = 0, diff;
          unsigned long long secret;

          for (j = *loc++; j > 0; j--) {
            READ_CHAR(loc, size);
            assert (size);

            if (size < 0) {
              size *= -1;
              READ_CHAR(loc, diff);
              local_id += diff;
            } else {
              READ_INT(loc, vid);
              READ_INT(loc, local_id);
              sid = get_server (vid);
            }
            READ_INT(loc, extra);
            READ_LONG(loc, secret);

            char base64url_secret[12];
            assert (!base64url_encode ((unsigned char *)&secret, 8, base64url_secret, 12));

            int rotate = (size >> 5);
            char size_str[2];
            size_str[0] = (size & 31) + 'a' - 1;
            size_str[1] = 0;

            tl_store_int (type_name);
            tl_store_int (rotate);
            tl_store_string (size_str, 1);
            tl_store_int (sid);
            tl_store_int (vid);
            tl_store_int (local_id);
            tl_store_int (extra);
            tl_store_string (base64url_secret, strlen (base64url_secret));
          }
          break;
        }
        default:
          assert (0);
      }
    }
  }
}

void event_to_binlog (dl_zout *new_binlog, actual_object *o, int type_id, int event_type) {
  event *e = o->obj;
  assert (e != NULL);
  assert (o->dyn == NULL);
  assert (!o->deleted);

  type_desc *t = &types[type_id];

  int i, mask = 0, len = 0, id = 0, owner_id = 0;
  for (i = 0; i < t->field_i; i++) {
    if (has_field (e, i)) {
      field_desc *f = &t->fields[i];

      char *key = f->name;
      if (!strcmp (key, "id")) {
        id = *GET_INT (e, f);
      } else if (!strcmp (key, "owner_id")) {
        owner_id = *GET_INT (e, f);
      } else {
        mask |= 1u << i;
        switch (f->type) {
          case t_int:
            len += sizeof (int);
            break;
          case t_long:
            len += sizeof (long long);
            break;
          case t_double:
            len += sizeof (double);
            break;
          case t_string:
            len += sizeof (short) + strlen (GET_STRING(e, f));
            break;
          case t_raw:
            len += sizeof (short) + RAW_LEN (GET_RAW(e, f));
            break;
          default:
            assert (0);
        }
      }
    }
  }
  assert (id != 0 && owner_id != 0);
  if (len == 0) {
    fprintf (stderr, "%s %d of owner_id %d is broken.\n", type_id == PHOTO_TYPE ? "Photo" : "Album", id, owner_id);
    return;
  }
  assert (2 <= len && len < MAX_EVENT_SIZE);

  int size = offsetof (struct lev_photo_change_data, changes) + len;
  struct lev_photo_change_data *E =
    dl_zout_alloc_log_event (new_binlog, event_type + (mode << 16) + len, size);

  E->user_id = owner_id;
  E->data_id = id;
  E->mask = mask;

  char *s = (char *)E->changes;

  int l = t_int, r = t_string, tt;
  for (tt = 0; tt < 2; tt++) {
    for (i = 0; i < 32; i++) {
      if ((mask >> i) & 1) {
        field_desc *f = &t->fields[i];
        if (l <= (int)f->type && (int)f->type < r) {
          switch (f->type) {
            case t_int:
              WRITE_INT(s, *GET_INT(e, f));
              break;
            case t_long:
              WRITE_LONG(s, *GET_LONG(e, f));
              break;
            case t_double:
              WRITE_DOUBLE(s, *GET_DOUBLE(e, f));
              break;
            case t_string: {
              char *str = GET_STRING(e, f);
              short str_len = strlen (str);
              WRITE_SHORT(s, str_len);
              memcpy (s, str, str_len);
              s += str_len;
              break;
            }
            case t_raw: {
              char *raw = GET_RAW(e, f), *str = RAW_DATA(raw);
              short str_len = RAW_LEN(raw);
              WRITE_SHORT(s, str_len);
              memcpy (s, str, str_len);
              s += str_len;
              break;
            }
            default:
              assert (0);
          }
        }
      }
    }
    l += t_string;
    r += t_string;
  }
  assert (s == (char *)E + size);
}


void test_events (void) {
  fprintf (stderr, "testing events\n");
  event *e = NULL;

  event_fix (&e, event_update_str (e, strdup ("id:2"), 1));
  dbg ("[%s]\n", event_to_str (e, 1, NULL, 0));

  event_fix (&e, event_update_str (e, strdup ("id:1\001caption:in-the-middle-of-nowhere"), 1));
  dbg ("[%s]\n", event_to_str (e, 1, NULL, 0));

  event_fix (&e, event_update_str (e, strdup ("caption:down-the-rabbit-hole"), 1));
  dbg ("[%s]\n", event_to_str (e, 1, NULL, 0));

  event_fix (&e, event_update_str (e, strdup ("id:7\001caption:oppa"), 1));
  dbg ("[%s]\n", event_to_str (e, 1, NULL, 0));

  dbg ("----------------------\n");

  event *e1 = NULL, *e2 = NULL;
  event_fix (&e1, event_update_str (e1, strdup ("id:0\001album_id:171420\001caption:empty-info"), 1));
  event_fix (&e2, event_update_str (e2, strdup ("id:2"), 1));
  dbg ("e1 = %p, e2 = %p\n", e1, e2);
  dbg ("[%s]", event_to_str (e1, 1, NULL, 0));
  dbg (" + [%s]\n", event_to_str (e2, 1, NULL, 0));
  event_fix (&e1, event_update_event (e1, e2, 1));
  dbg ("[%s]\n", event_to_str (e1, 1, NULL, 0));

  assert (0);
  return;
}


/***
  USERS
 ***/


inline int my_add_change (dyn_object **_o, change *_c, int type_id) {
  my_dyn_object **o =  (my_dyn_object **)_o;
  my_change *c = (my_change *)_c;

  event *ne;
  if (c->tp == ch_event) {
    if (*o == NULL) {
      ne = c->e;
    } else {
      ne = event_update_event (*o, c->e, type_id);
      event_free (c->e);
    }
  } else if (c->tp == ch_fields) {
    ne = event_update_php (*o, c->f, c->fn, type_id);
  } else {
    assert ("wtf" && 0);
  }

  if (*o != ne) {
    event_free (*o);
    *o = ne;
  }
  return 0;
}

int photo_add_change (dyn_object **_o, change *_c) {
  return my_add_change (_o, _c, PHOTO_TYPE);
}

int album_add_change (dyn_object **_o, change *_c) {
  return my_add_change (_o, _c, ALBUM_TYPE);
}

void my_free_dyn (int *i, dyn_object **_o) {
  my_dyn_object *o =  *(my_dyn_object **)_o;
  event_free (o);
}

inline int my_object_save (actual_object *o, char *buf, int buf_size, int type_id) {
  if (type_id == PHOTO_TYPE) {
    total_photos++;
  }

  event *old = o->obj == (event *)EMPTY__METAFILE ? NULL : o->obj, *new = old;
  event *e = event_update_event (new, o->dyn, type_id);

  if (0) {
    int k;
    for (k = 0; k < 63; k++) {
      if (!has_field (e, k)) {
        continue;
      }

      field_desc *f = &types[type_id].fields[k];
      fprintf (stderr, "%15s: ", f->name);

      switch (f->type) {
        case t_int:
          fprintf (stderr, "%d", *GET_INT(e, f));
          break;
        case t_long:
          fprintf (stderr, "%lld", *GET_LONG(e, f));
          break;
        case t_double:
          fprintf (stderr, "%.6lf", *GET_DOUBLE(e, f));
          break;
        case t_string: {
          char *p = GET_STRING(e, f);
          while (*p) {
            switch (*p) {
              case '\t':
              case '\n':
              case '\\':
                fputc ('\\', stderr);
              default:
                fputc (*p++, stderr);
            }
          }
          break;
        }
        case t_raw:
          fprintf (stderr, "%d", RAW_LEN (GET_RAW (e, f)));
          break;
        default:
          assert (0);
      }
      fprintf (stderr, "\n");
    }
    fprintf (stderr, "\n");
  }

  int len = get_event_size (e);
  assert (len <= buf_size);
  memcpy (buf, e, len);

  if (e != new) {
    event_free (e);
  }
  if (new != old) {
    event_free (new);
  }
  return len;
}

int photo_object_save (actual_object *o, char *buf, int buf_size) {
  return my_object_save (o, buf, buf_size, PHOTO_TYPE);
}

int album_object_save (actual_object *o, char *buf, int buf_size) {
  return my_object_save (o, buf, buf_size, ALBUM_TYPE);
}

#pragma pack(push,4)

DL_BEGIN_PREDICATE (predicate_field_cmp)
  field_desc *field;
  union {
    long long v_long;
    int v_int;
  };
DL_END_PREDICATE (predicate_field_cmp)

DL_BEGIN_PREDICATE (predicate_logical)
  predicate *lhs;
  predicate *rhs;
DL_END_PREDICATE (predicate_logical)

data_functions photo_func = {
  .magic = 0x12345,
  .title = "shalala",
  .add_change = photo_add_change,
  .object_save = photo_object_save,
  .free_dyn = my_free_dyn
};

data_functions album_func = {
  .magic = 0x54321,
  .title = "shalala",
  .add_change = album_add_change,
  .object_save = album_object_save,
  .free_dyn = my_free_dyn
};



typedef struct user_changesx user_changes;

struct user_changesx {
  user_changes *next;
  int now;
  int real_len;
  struct lev_generic E;
};

typedef struct userx user;

struct userx {
  int id, local_id, current_photo_id, current_album_id;

// who knows what will be stored here
  char *metafile;
  int metafile_len;

  /* from metafile */
  lookup album_by_photo;
  data d;
  int *albums_id, *albums_offset, albums_n;

  set_int deleted_albums;
  map_int_vptr photos;

  user_changes *first, *last;

  struct aio_connection *aio;
  user *next_used, *prev_used;
};

typedef struct {
  int id;
  int current_photo_id, current_album_id;
  int size;
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
  unsigned int log_pos0_crc32;
  unsigned int log_pos1_crc32;
  long long volumes_len;
  long long volumes_st;
  int not_used;
  int not_used2;
  long long total_photos;

  int reserved[30];

  int user_cnt;
  user_index_data *user_index;
} index_header;

#pragma pack(pop)

set_intp h_users;
index_header header;
user *users, *LRU_head;
int cur_local_id, user_cnt, cur_users, del_by_LRU;


#define predicate_field_cmp_eval(name, operation)                                                 \
int DL_ADD_SUFF (predicate_field_cmp_eval, name) (predicate_field_cmp *self, actual_object *ao) { \
  event *e;                                                                                       \
  if (ao->dyn != NULL && has_field (ao->dyn, self->field->id)) {                                  \
    e = ao->dyn;                                                                                  \
  } else if (ao->obj != NULL && has_field (ao->obj, self->field->id)) {                           \
    e = ao->obj;                                                                                  \
  } else {                                                                                        \
    return 0;                                                                                     \
  }                                                                                               \
                                                                                                  \
  switch (self->field->type) {                                                                    \
    case t_int:                                                                                   \
      return *GET_INT(e, self->field) operation self->v_int;                                      \
    case t_long:                                                                                  \
      return *GET_LONG(e, self->field) operation self->v_long;                                    \
    default:                                                                                      \
      assert (0);                                                                                 \
  }                                                                                               \
}

predicate_field_cmp_eval(less            , < )
predicate_field_cmp_eval(equal           , ==)
predicate_field_cmp_eval(greater         , > )
predicate_field_cmp_eval(less_or_equal   , <=)
predicate_field_cmp_eval(not_equal       , !=)
predicate_field_cmp_eval(greater_or_equal, >=)
predicate_field_cmp_eval(binary_and      , & )

int predicate_field_cmp_has (predicate_field_cmp *self, actual_object *ao) {
  return (ao->dyn != NULL && has_field (ao->dyn, self->field->id)) ||
         (ao->obj != NULL && has_field (ao->obj, self->field->id));
}

int predicate_logical_eval_and (predicate_logical *self, actual_object *ao) {
  return DL_CALL (self->lhs, eval, ao) && DL_CALL (self->rhs, eval, ao);
}

int predicate_logical_eval_or (predicate_logical *self, actual_object *ao) {
  return DL_CALL (self->lhs, eval, ao) || DL_CALL (self->rhs, eval, ao);
}

#define MAX_PREDICATES 100
#define MAX_CONDITION_LEN 32768

predicate_field_cmp predicates_cmp[MAX_PREDICATES];
predicate_logical predicates_logical[MAX_PREDICATES];
int i_cmp, i_logical;

int close_bracket[MAX_CONDITION_LEN];

int op_priority (char c) {
  switch (c) {
    case '&':
      return 1;
    case '|':
      return 2;
    default:
      assert (0);
  }
}

//function may rewrite condition[r]
predicate *predicate_parce (char *condition, int l, int r, int type_id) {
//  fprintf (stderr, "%s %d %d\n", condition, l, r);
  while (l < r && close_bracket[l] == r) {
    l++;
    r--;
  }
  if (l > r) {
    return NULL;
  }

  int m = -1, i;
  for (i = l; i < r; i = (close_bracket[i] == -1 ? i : close_bracket[i]) + 1) {
    if ((condition[i] == '|' && condition[i + 1] == '|') ||
        (condition[i] == '&' && condition[i + 1] == '&')) {
      if (m == -1 || op_priority (condition[m]) <= op_priority (condition[i])) {
        m = i;
      }
    }
  }

//  fprintf (stderr, "l = %d, m = %d, r = %d\n", l, m, r);
  if (m < l) {
    char *value = condition + l;
    assert (i_cmp < MAX_PREDICATES);
    predicate_field_cmp *predicate_cmp = predicates_cmp + (i_cmp++);

    if (*value == '?') {
      char *end = condition + r + 1;
      value++;
      while (value != end && is_letter (*value)) {
        value++;
      }
      if (value != end) {
        return NULL;
      }
      *value = 0;

      predicate_cmp->eval = (__typeof (predicate_cmp->eval)) predicate_field_cmp_has;
      type_desc *t = &types[type_id];
      int field_id = get_field_id (t, condition + l + 1);
      if (field_id < 0) {
        return NULL;
      }
      predicate_cmp->field = &t->fields[field_id];
      return (predicate *)predicate_cmp;
    }

    while (is_letter (*value)) {
      value++;
    }
    int add = 1;
    predicate_cmp->eval = NULL;
    switch (*value) {
      case '&':
        predicate_cmp->eval = (__typeof (predicate_cmp->eval)) predicate_field_cmp_eval_binary_and;
        break;
      case '=':
        if (value[1] == '=') {
          add++;
          if (value[2] == '=') {
            add++;
          }
        }
        predicate_cmp->eval = (__typeof (predicate_cmp->eval)) predicate_field_cmp_eval_equal;
        break;
      case '<':
        if (value[1] == '=') {
          predicate_cmp->eval = (__typeof (predicate_cmp->eval)) predicate_field_cmp_eval_less_or_equal;
          add = 2;
        } else if (value[1] == '>') {
          predicate_cmp->eval = (__typeof (predicate_cmp->eval)) predicate_field_cmp_eval_not_equal;
          add = 2;
        } else {
          predicate_cmp->eval = (__typeof (predicate_cmp->eval)) predicate_field_cmp_eval_less;
        }
        break;
      case '>':
        if (value[1] == '=') {
          predicate_cmp->eval = (__typeof (predicate_cmp->eval)) predicate_field_cmp_eval_greater_or_equal;
          add = 2;
        } else {
          predicate_cmp->eval = (__typeof (predicate_cmp->eval)) predicate_field_cmp_eval_greater;
        }
        break;
      case '!':
        if (value[1] == '=') {
          predicate_cmp->eval = (__typeof (predicate_cmp->eval)) predicate_field_cmp_eval_not_equal;
          add = 2;
        }
        break;
    }

    long long val;
    int pos = -1;
    if (predicate_cmp->eval == NULL || sscanf (value + add, "%lld%n", &val, &pos) < 1 || (value - condition) + add + pos != r + 1) {
      return NULL;
    }

    type_desc *t = &types[type_id];
    int field_id = get_field_id_len (t, condition + l, value - condition - l);
    if (field_id < 0) {
      return NULL;
    }
    predicate_cmp->field = &t->fields[field_id];

    switch (predicate_cmp->field->type) {
      case t_int:
        predicate_cmp->v_int = val;
        break;
      case t_long:
        predicate_cmp->v_long = val;
        break;
      default:
        return NULL;
    }

    return (predicate *)predicate_cmp;
  }

  predicate_logical *predicate_log = predicates_logical + (i_logical++);

  if (condition[m] == '&') {
    predicate_log->eval = (__typeof (predicates_logical[0].eval)) predicate_logical_eval_and;
  } else {
    predicate_log->eval = (__typeof (predicates_logical[0].eval)) predicate_logical_eval_or;
  }
  predicate_log->lhs = predicate_parce (condition, l, m - 1, type_id);
  predicate_log->rhs = predicate_parce (condition, m + 2, r, type_id);

  if (predicate_log->lhs == NULL || predicate_log->rhs == NULL) {
    return NULL;
  }

  return (predicate *)predicate_log;
}

predicate *predicate_init (char *condition, int type_id) {
  if (condition == NULL || condition[0] == 0 || strlen (condition) + 1 > MAX_CONDITION_LEN) {
    return NULL;
  }

  int i;
  int stack_pos[MAX_PREDICATES * 2 + 2], stack_n = 0;
  int op_cnt = 0;
  for (i = 0; condition[i] && op_cnt + 1 < MAX_PREDICATES && stack_n < 2 * MAX_PREDICATES + 1; i++) {
    close_bracket[i] = -1;

    if (condition[i] == '(') {
      stack_pos[stack_n++] = i;
    } else if (condition[i] == ')') {
      if (stack_n == 0) {
        return NULL;
      }
      close_bracket[stack_pos[--stack_n]] = i;
    } else {
      if ((condition[i] == '|' && condition[i + 1] == '|') ||
          (condition[i] == '&' && condition[i + 1] == '&')) {
        op_cnt++;
      }
    }
  }
  if (condition[i]) {
    wrn("MAX_PREDICATES exceeded on condition %s\n", condition);
    return NULL;
  }
  if (stack_n != 0) {
    return NULL;
  }

  i_cmp = i_logical = 0;
  return predicate_parce (condition, 0, strlen (condition) - 1, type_id);
}


void load_user_metafile (user *u, int local_id, int no_aio);
int unload_user_metafile (user *u);

int user_loaded (user *u) {
  return u != NULL && u->metafile_len >= 0 && u->aio == NULL;
}

void user_add_change (user *u, struct lev_generic *E, int len) {
  if (import_dump_mode) {
    return;
  }
  assert (!write_only);

  user_changes *cur = dl_malloc (offsetof (user_changes, E) + len);
  cur->next = NULL;
  cur->now = now;
  //TODO: remove(?) real_len
  cur->real_len = len;
  memcpy (&cur->E, E, len);
  if (u->first == NULL) {
    u->first = u->last = cur;
  } else {
    u->last->next = cur;
    u->last = cur;
  }
}

void user_process_changes (user *u) {
  assert (user_loaded (u));

  int old_now = now;
  while (u->first != NULL) {
    user_changes *cur = u->first;
    now = cur->now;
    int len = photo_replay_logevent (&cur->E, 1000000000);
    u->first = cur->next;
    dl_free (cur, offsetof (user_changes, E) + len);
  }
  now = old_now;
  u->last = NULL;//TODO in fact we don't need this
}


void user_free_changes (user *u) {
  while (u->first != NULL) {
    user_changes *cur = u->first;
    u->first = cur->next;
    dl_free (cur, offsetof (user_changes, E) + cur->real_len);
  }
  u->last = NULL;//TODO in fact we don't need this
}


int check_user_id (int user_id) {
  return (user_id != 0 && dl_abs (user_id) % log_split_mod == log_split_min);
}

user *conv_uid_get (int user_id) {
  if (user_id == 0) {
    return NULL;
  }
  if (dl_abs (user_id) % log_split_mod != log_split_min) {
    return NULL;
  }
  if (import_dump_mode) {
    return users;
  }

  user **u = (user **)set_intp_get (&h_users, &user_id);
  if (u == NULL) {
    return NULL;
  }
  return *u;
}

user *conv_uid (int user_id) {
  if (user_id == 0) {
    return NULL;
  }
  if (dl_abs (user_id) % log_split_mod != log_split_min) {
    return NULL;
  }
  if (import_dump_mode) {
    return users;
  }

  user **u = (user **)set_intp_add (&h_users, &user_id);
  if (*u == (user *)&user_id) {
    assert (cur_local_id + 1 < user_cnt);

    *u = &users[++cur_local_id];
    (*u)->id = user_id;
    (*u)->local_id = cur_local_id;

    if (cur_local_id <= header.user_cnt) {
      assert (user_id == header.user_index[cur_local_id].id);
      (*u)->current_photo_id = header.user_index[cur_local_id].current_photo_id;
      (*u)->current_album_id = header.user_index[cur_local_id].current_album_id;
    } else {
      (*u)->current_photo_id = MIN_NEW_PHOTO_ID;
      (*u)->current_album_id = MIN_NEW_ALBUM_ID;
    }
//    dbg ("ID = %d, current_album_id = %d\n", user_id, (*u)->current_album_id);

    if (!write_only) {
      data_init (&(*u)->d, &album_func);
    }
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


int check_photo_id (int photo_id) {
  return photo_id > 0;
}

int check_album_id (int album_id) {
  return album_id >= -256 && album_id != 0;
}


data *user_get_album_data (user *u) {
//  dbg ("user_get_album_data %d\n", u->id);

  data *res = &u->d;
  assert (data_loaded (res));
  return res;
}

data *user_get_photo_data (user *u, int aid) {
//  dbg ("user_get_photo_data %d: album_id = %d\n", u->id, aid);

  data *albums = user_get_album_data (u);

  int lid = data_get_local_id_by_id (albums, aid);
  if (lid == -1) {
    dbg ("unexisted album %d\n", aid);
    return NULL;
  }

  data **d = (data **)map_int_vptr_add (&u->photos, aid);
  if (*d == NULL) {
    *d = dl_malloc (sizeof (data));
    data_init (*d, &photo_func);
  }

//  dbg ("load data if needed\n");

  if (!data_loaded (*d)) {
    int i = u->albums_n;
    if (set_int_get (&u->deleted_albums, aid) == NULL) {
      i = dl_find_int (u->albums_id, u->albums_n, aid);
    }
    if (i != u->albums_n) {
      data_load (*d, u->metafile + u->albums_offset[i], u->albums_offset[i + 1] - u->albums_offset[i]);
    } else {
      data_load (*d, EMPTY__METAFILE, EMPTY__METAFILE_LEN);
    }
  }

  return *d;
}

int user_get_aid_by_pid (user *u, int pid) {
//  dbg ("user_get_aid_by_pid %d: photo_id = %d\n", u->id, pid);

  assert (user_loaded (u));
  int res = lookup_conv (&u->album_by_photo, pid, 0);

//  dbg ("photo_id = %d, album_id = %d\n", pid, res);
//  can be deleted, I hope
  if (res != 0) {
    data *albums = user_get_album_data (u);
    assert (data_get_local_id_by_id (albums, res) != -1);
  }

  return res;
}

actual_object result_obj[MAX_RESULT];

int user_get_photos_count (user *u, int aid) {
//  dbg ("user_get_photos_count %d: album_id = %d\n", u->id, aid);

  assert (user_loaded (u));
  data *d = user_get_photo_data (u, aid);
  if (d == NULL) {
    return -1;
  }

  return data_get_cnt (d);
}

int user_get_photos_count_pred (user *u, int aid, predicate *pred) {
//  dbg ("user_get_photos_count_pred %d: album_id = %d\n", u->id, aid);

  assert (user_loaded (u));
  data *d = user_get_photo_data (u, aid);
  if (d == NULL) {
    return -1;
  }

  if (pred == NULL) {
    return data_slice (d, result_obj, MAX_RESULT, 0);
  } else {
    return data_slice_filtered (d, result_obj, MAX_RESULT, 0, pred);
  }
}

int user_get_albums_count (user *u) {
//  dbg ("user_get_albums_count %d\n", u->id);

  assert (user_loaded (u));
  data *d = user_get_album_data (u);

  return data_get_cnt (d);
}

int user_get_albums_count_pred (user *u, predicate *pred) {
//  dbg ("user_get_albums_count_pred %d\n", u->id);

  assert (user_loaded (u));
  data *d = user_get_album_data (u);

  if (pred == NULL) {
    return data_slice (d, result_obj, MAX_RESULT, 0);
  } else {
    return data_slice_filtered (d, result_obj, MAX_RESULT, 0, pred);
  }
}

inline int user_change_data_lid (data *d, int lid, field *field_changes, int field_changes_n) {
//  dbg ("user_change_data_lid ???: local_id = %d, field_changes_n = %d\n", lid, field_changes_n);

  assert (d != NULL && lid >= 0);

  my_change ch;
  ch.tp = ch_fields;
  ch.f = field_changes;
  ch.fn = field_changes_n;

//  dbg ("data add change %d\n", lid);
  return data_add_change (d, &ch, lid);
}


inline int user_change_data (data *d, int id, field *field_changes, int field_changes_n, int force) {
//  dbg ("user_change_data ???: id = %d, field_changes_n = %d\n", id, field_changes_n);

  assert (d != NULL);
  int lid = data_get_local_id_by_id (d, id);
  if (lid == -1) {
    return 0;
  }

  if (!force && data_get_hidden_state (d, id) != 0) {
    return 0;
  }

  return user_change_data_lid (d, lid, field_changes, field_changes_n);
}


int user_create_album_internal (user *u, int id) {
  dbg ("user_create_album_internal %d: album_id = %d\n", u->id, id);

  data *d = user_get_album_data (u);
  assert (d != NULL);

  int res = data_add_object (d, id);
  if (res >= 0) {
    tmp_field_changes_n = 0;
    TMP_ADD_CHANGE (album_type_owner_id, int, u->id);
    TMP_ADD_CHANGE (album_type_id, int, id);

    user_change_data_lid (d, res, tmp_field_changes, tmp_field_changes_n);

    return 1;
  }
  return 1;
}

int user_create_album_force (user *u, int aid) {
//  dbg ("user_create_album_force %d: album_id = %d\n", u->id, aid);
  if (!check_album_id (aid)) {
    return -1;
  }

  if (aid > u->current_album_id) {
    return -1;
  }

  if (write_only) {
    return 1;
  }

  assert (user_loaded (u));

  if (user_get_albums_count (u) >= MAX_ALBUMS) {
    return -1;
  }

  return user_create_album_internal (u, aid);
}

int user_create_album (user *u) {
//  dbg ("user_create_album %d\n", u->id);

  if (write_only) {
    return u->current_album_id++;
  }

  assert (user_loaded (u));

  if (user_get_albums_count (u) >= MAX_ALBUMS) {
    u->current_album_id++;
    return 0;
  }

//  dbg ("create_album (user = %d)\n", u->id);

  if (!user_create_album_internal (u, u->current_album_id)) {
    u->current_album_id++;
    return 0;
  }
  return u->current_album_id++;
}

int user_create_photo_internal (user *u, data *d, int aid, int cnt, int pid) {
  dbg ("user_create_photo_internal %d: album_id = %d, cnt = %d, photo_id = %d\n", u->id, aid, cnt, pid);

  int res = 0;
  while (cnt--) {
    int id = pid++;

    int taid = lookup_conv (&u->album_by_photo, id, 0);
    dbg ("force pid = %d, album = %d\n", id, taid);

    //TODO: restore, del photo if existed hidden
    if (taid != 0) {
      data *da = user_get_photo_data (u, taid);
      assert (da != NULL);
      if (data_get_hidden_state (da, id) != 0) {
        data_del (da, id);
        taid = 0;
      }
    }

    if (taid == 0) {
      int lid = data_add_object (d, id);

      if (lid > -1) {
        tmp_field_changes_n = 0;
        TMP_ADD_CHANGE (photo_type_owner_id, int, u->id);
        TMP_ADD_CHANGE (photo_type_album_id, int, aid);
        TMP_ADD_CHANGE (photo_type_id, int, id);

        user_change_data_lid (d, lid, tmp_field_changes, tmp_field_changes_n);

        lookup_set (&u->album_by_photo, id, aid);

        res++;
      }
    } else {
      res += (taid == aid);
    }
  }

  return res;
}

int user_create_photo_force (user *u, int aid, int cnt, int pid) {
//  dbg ("user_create_photo_force %d: album_id = %d, cnt = %d, photo_id = %d\n", u->id, aid, cnt, pid);

  if (!check_photo_id (pid) || !check_album_id (aid) || pid + cnt > u->current_photo_id) {
    return -1;
  }

  if (write_only) {
    return 1;
  }

  assert (user_loaded (u));

  if (aid < 0) {
    user_create_album_force (u, aid);
  }

  data *d = user_get_photo_data (u, aid);
  if (d == NULL || data_get_cnt (d) + cnt > GET_MAX_PHOTOS (aid)) {
    return -1;
  }

  return user_create_photo_internal (u, d, aid, cnt, pid);
}


int user_create_photo (user *u, int aid, int cnt) {
//  dbg ("user_create_photo %d: album_id = %d, cnt = %d\n", u->id, aid, cnt);
  if (!check_album_id (aid)) {
    return 0;
  }

  if (write_only) {
    int res = u->current_photo_id;

    u->current_photo_id += cnt;

//    dbg ("write only\n");
    return res;
  }

  assert (user_loaded (u));

  int res = u->current_photo_id;

  if (aid < 0) {
    user_create_album_force (u, aid);
  }

  data *d = user_get_photo_data (u, aid);
  if (d == NULL || data_get_cnt (d) + cnt > GET_MAX_PHOTOS (aid)) {
    u->current_photo_id += cnt;
//    dbg ("user_create_photo %d: failed\n", u->id);
    return 0;
  }

  int ret = user_create_photo_internal (u, d, aid, cnt, res);
  assert (ret == cnt);

  u->current_photo_id += cnt;
//  dbg ("user_create_photo %d: done (photo_id = %d)", u->id, res);
  return res;
}

inline int user_get (data *d, int n, int offset, predicate *pred) {
  assert (d != NULL);
  if (n > MAX_RESULT) {
    n = MAX_RESULT;
  }

  if (pred == NULL) {
    return data_slice (d, result_obj, n, offset);
  } else {
    return data_slice_filtered (d, result_obj, n, offset, pred);
  }
}

//if fails returns -1 else number of found photos. Writes pointers to returned events into result_obj.
int user_get_photos_overview (user *u, int *albums, int albums_n, int offset, int limit, const int reverse) {
  static data_iterator its[MAX_ALBUMS];

  assert (user_loaded (u));
  assert (reverse == 1 || reverse == 0);

  if (offset < 0) {
    offset = 0;
  }

  if (offset > MAX_RESULT) {
    offset = MAX_RESULT;
  }

  if (limit <= 0) {
    return 0;
  }

  if (limit > MAX_RESULT) {
    limit = MAX_RESULT;
  }

  if (albums_n > MAX_ALBUMS) {
    albums_n = MAX_ALBUMS;
  }

  int i, its_n = 0;
  for (i = 0; i < albums_n; i++) {
    data *d = user_get_photo_data (u, albums[i]);
    if (d != 0) {
      data_iterator_init (&its[its_n++], d, -1 + 2 * reverse);
    }
  }

  limit = iter_merge (its, its_n, result_obj, limit, offset);

  for (i = 0; i < its_n; i++) {
    data_iterator_free (&its[i]);
  }
  return limit;
}

//if fails returns -1 else number of found photos. Writes pointers to returned events into result_obj.
int user_get_photos (user *u, int aid, int offset, int limit, predicate *pred) {
  assert (user_loaded (u));

  if (offset < 0) {
    offset = 0;
  }

  if (offset > MAX_RESULT) {
    offset = MAX_RESULT;
  }

  if (limit <= 0) {
    return 0;
  }

  if (limit > MAX_RESULT) {
    limit = MAX_RESULT;
  }

  data *d = user_get_photo_data (u, aid);
  if (d == NULL) {
    return -1;
  }
  return user_get (d, limit, offset, pred);
}

//if fails returns -1 else number of found albums. Writes pointers to returned events into result_obj.
int user_get_albums (user *u, int offset, int limit, predicate *pred) {
  assert (user_loaded (u));

  if (offset < 0) {
    offset = 0;
  }

  if (offset > MAX_RESULT) {
    offset = MAX_RESULT;
  }

  if (limit <= 0) {
    return 0;
  }

  if (limit > MAX_RESULT) {
    limit = MAX_RESULT;
  }

  data *d = user_get_album_data (u);
  return user_get (d, limit, offset, pred);
}

inline int user_get_obj (data *d, int id, const int force, actual_object *o) {
  int local_id = data_get_local_id_by_id (d, id);
  if (local_id < 0 || ((o->deleted = data_get_hidden_state (d, id)) != 0 && !force) || data_get_actual_object (d, local_id, o) < 0) {
    return -1;
  }
  return 0;
}

int user_get_photo (user *u, int pid, const int force, actual_object *o) {
  assert (user_loaded (u));

  int aid = user_get_aid_by_pid (u, pid);
  if (aid == 0) {
    dbg ("no album found (pid->aid)\n");
    return -1;
  }
  data *d = user_get_photo_data (u, aid);
  if (d == NULL) {
    dbg ("no album data found\n");
    return -1;
  }
  return user_get_obj (d, pid, force, o);
}


int user_get_album (user *u, int aid, const int force, actual_object *o) {
  assert (user_loaded (u));

  data *d = user_get_album_data (u);
  return user_get_obj (d, aid, force, o);
}

int user_delete_photo (user *u, int pid) {
  if (write_only) {
    return 1;
  }

  assert (user_loaded (u));

  int aid = user_get_aid_by_pid (u, pid);
  if (aid == 0) {
    return 0;
  }
  data *d = user_get_photo_data (u, aid);
  if (d == NULL) {
    return 0;
  }
  if (data_get_hidden_state (d, pid) != 0) {
    return 0;
  }

  if (data_hide_expected_size(d) >= MAX_HIDE) {
    assert (data_del (d, pid) > -1);
    lookup_set (&u->album_by_photo, pid, 0);
  } else {
    assert (data_hide (d, pid, HIDE_ITERS) > -1);
  }

  return 1;
}

int user_restore_photo (user *u, int pid) {
  if (write_only) {
    return 1;
  }

  int aid = user_get_aid_by_pid (u, pid);
  if (aid == 0) {
    return 0;
  }
  data *d = user_get_photo_data (u, aid);
  if (d == NULL) {
    return 0;
  }
  if (data_get_hidden_state (d, pid) == 0) {
    return 0;
  }
  if (data_get_cnt (d) >= GET_MAX_PHOTOS (aid)) {
    return 0;
  }
  assert (data_restore (d, pid) > -1);
  return 1;
}

int user_delete_album (user *u, int aid) {
  if (write_only) {
    return 1;
  }

  assert (user_loaded (u));

  data *d = user_get_album_data (u);
  data *da = user_get_photo_data (u, aid);
  if (da == NULL) {
    return 0;
  }

  static int ids[MAX_PHOTOS_DYN + MAX_HIDE + 1];
  int i, n;

  n = data_get_ids (da, ids, MAX_PHOTOS_DYN + MAX_HIDE + 1);
  assert (n < MAX_PHOTOS_DYN + MAX_HIDE + 1);
  dbg ("deleting album (aid = %d) containing %d photos\n", aid, n);
  for (i = 0; i < n; i++) {
//    dbg ("del from album_by_photo %d->?\n", ids[i]);
    lookup_set (&u->album_by_photo, ids[i], 0);
  }

  data_del (d, aid);
  set_int_add (&u->deleted_albums, aid);
  data_unload (da);
  data_free (da);
  dl_free (da, sizeof (data));
  map_int_vptr_del (&u->photos, aid);

  return 1;
}


inline int user_change_order (data *d, int id, int id_near, int is_next) {
//  dbg ("user_change_order ???: id = %d, id_near = %d, is_next = %d\n", id, id_near, is_next);

  assert (!write_only);

  if (data_get_hidden_state (d, id) != 0 || (id_near != 0 && data_get_hidden_state (d, id_near) != 0)) {
    return 0;
  }

  int i, j;

//  dbg ("user_change_order (id = %d) (id_near = %d)\n", id, id_near);
  int local_id = data_get_local_id_by_id (d, id);
  i = data_get_pos_by_local_id (d, local_id);
  if (i < 0) {
    return i;
  }

  int local_id_near;
  if (id_near != 0) {
    local_id_near = data_get_local_id_by_id (d, id_near);
    j = data_get_pos_by_local_id (d, local_id_near);
    if (j < 0) {
      return j;
    }
    if (is_next) {
      j--;
      local_id_near = data_get_local_id_by_pos (d, j);
    }
//    dbg ("user_change_order (i = %d) (j = %d)\n", i, j);
    if (i > j) {
      j++;
    }
  } else {
    if (is_next) {
      j = data_get_cnt (d) - 1;
      local_id_near = data_get_local_id_by_pos (d, j);
    } else {
      j = 0;
      local_id_near = -1;
    }
  }

  return data_move_new (d, i, j, local_id, local_id_near);
}

int user_change_photo_order (user *u, int pid, int pid_near, int is_next) {
//  dbg ("user_change_photo_order %d: photo_id = %d, photo_id_near = %d, is_next = %d\n", u->id, pid, pid_near, is_next);

  if (write_only) {
    return 1;
  }

  assert (user_loaded (u));

  int aid = user_get_aid_by_pid (u, pid);
  if (aid == 0) {
    return 0;
  }

  if (pid_near != 0 && aid != user_get_aid_by_pid (u, pid_near)) {
    return 0;
  }

  data *d = user_get_photo_data (u, aid);
  if (d == NULL) {
    return 0;
  }

  return user_change_order (d, pid, pid_near, is_next) > -1;
}

int user_change_album_order (user *u, int aid, int aid_near, int is_next) {
//  dbg ("user_change_album_order %d: album_id = %d, album_id_near = %d, is_next = %d\n", u->id, aid, aid_near, is_next);

  if (write_only) {
    return 1;
  }

  assert (user_loaded (u));

  data *d = user_get_album_data (u);
  return user_change_order (d, aid, aid_near, is_next) > -1;
}

int user_change_photo (user *u, int pid, field *field_changes, int field_changes_n) {
  dbg ("user_change_photo %d: photo_id = %d, field_changes_n = %d\n", u->id, pid, field_changes_n);

  if (write_only) {
    return 1;
  }

  assert (user_loaded (u));

  int aid = user_get_aid_by_pid (u, pid);
  if (aid == 0) {
    return 0;
  }
  data *d = user_get_photo_data (u, aid);
  if (d == NULL) {
    return 0;
  }

  if (data_get_hidden_state (d, pid) != 0) {
    return 0;
  }

  int i;
  for (i = 0; i < field_changes_n; i++) {
    assert (field_changes[i].v_fid != photo_type_id && field_changes[i].v_fid != photo_type_owner_id);

    // change album
    if (field_changes[i].v_fid == photo_type_album_id) {
      int naid = field_changes[i].v_int;
      if (aid == naid) {
        continue;
      }

      dbg ("Move (photo_id = %d) to (album_id = %d) (field_changes_n = %d)\n", pid, field_changes[i].v_int, field_changes_n);

      if (!check_album_id (naid)) {
        return 0;
      }

      if (naid < 0) {
        user_create_album_force (u, naid);
      }

      data *da = user_get_photo_data (u, naid);
      if (da == NULL) {
        return 0;
      }

      if (data_get_cnt (da) + 1 > GET_MAX_PHOTOS (naid)) {
        return 0;
      }

      actual_object o;
      int lid = data_get_local_id_by_id (d, pid);
      assert (lid >= 0);

      assert (data_get_actual_object (d, lid, &o) >= 0);

      event *ne = event_update_event (o.obj, o.dyn, PHOTO_TYPE);
      if (ne == o.obj) {
        ne = event_dup (ne);
      }

      data_del (d, pid);

      lid = data_add_object (da, pid);
      assert (lid >= 0);

      my_change ch;
      ch.tp = ch_event;
      ch.e = ne;

      data_add_change (da, (change *)&ch, lid);
      lookup_set (&u->album_by_photo, pid, naid);

      d = da;

//      dbg ("Move (photo_id = %d) to (album_id = %d) (field_changes_n = %d)\n", pid, field_changes[i].v_int, field_changes_n);
    }
  }

  return user_change_data (d, pid, field_changes, field_changes_n, 0) > -1;
}


char *user_photo_get_location (user *u, int pid, int original, const int force, int *len) {
  assert (user_loaded (u) && !write_only);

//  dbg ("user_photo_get_location (uid = %d) (pid = %d)\n", u->id, pid);

  actual_object o;
  if (user_get_photo (u, pid, force, &o) < 0) {
    return NULL;
  }
  return event_get_location (&o, original, len);
}

int user_photo_set_location (user *u, int pid, int original, char *loc, int len) {
//  dbg ("user_photo_set_location %d: photo_id = %d, len = %d\n", u->id, pid, len);

  assert (user_loaded (u) && !write_only);

  int aid = user_get_aid_by_pid (u, pid);
  assert (aid != 0);

  data *d = user_get_photo_data (u, aid);
  if (d == NULL) {
    return 0;
  }

  tmp_field_changes_n = 0;
  tmp_field_changes[tmp_field_changes_n].v_raw_len = len;
  TMP_ADD_CHANGE (original ? photo_type__original_location : photo_type__location, raw, loc);

  user_change_data (d, pid, tmp_field_changes, tmp_field_changes_n, 1);

  return 1;
}

char location_buf[MAX_LOCATIONS * sizeof (location) + 3];

int user_add_photo_location (user *u, int pid, int original, int sid, int sid2, int orig_oid, int orig_aid, char *photo) {
//  dbg ("user_add_photo_location %d: photo_id = %d, photo = %s\n", u->id, pid, photo);

  if (write_only) {
    return 1;
  }

  assert (user_loaded (u));
  int l = strlen (photo);
  assert (check_photo (photo, l));

  int len;
  char *loc = user_photo_get_location (u, pid, original, 1, &len);

  if (loc == NULL) {
    return 0;
  }

  char *s = location_buf;
  if (len > 0 && loc[0] < 0) {
    int cur = -(loc[0] + mode) + 4 * sizeof (int) + 1;
    len -= cur;
    loc += cur;
  }

  *s++ = -l - mode;
  WRITE_INT(s, sid);
  WRITE_INT(s, sid2);
  WRITE_INT(s, orig_oid);
  WRITE_INT(s, orig_aid);
  WRITE_STRING(s, photo);

  if (len == 0) {
    *s++ = 0;
  } else {
    memcpy (s, loc, len);
  }

  return user_photo_set_location (u, pid, original, location_buf, (s - location_buf) + len);
}

#define save_location(s, size, vid, local_id, extra, secret) { \
  assert (size > new_size);                                    \
  if (vid == new_vid &&                                        \
      local_id >= new_local_id - 127 &&                        \
      local_id <= new_local_id + 127) {                        \
    WRITE_CHAR(s, -(size));                                    \
    WRITE_CHAR(s, local_id - new_local_id);                    \
  } else {                                                     \
    WRITE_CHAR(s, size);                                       \
    WRITE_INT(s, vid);                                         \
    WRITE_INT(s, local_id);                                    \
  }                                                            \
  WRITE_INT(s, extra);                                         \
  WRITE_LONG(s, secret);                                       \
  new_size = size;                                             \
  new_vid = vid;                                               \
  new_local_id = local_id;                                     \
  count++;                                                     \
}


int user_add_photo_location_engine (user *u, int pid, int original, char size, int rotate, int vid, int local_id, int extra, unsigned long long secret) {
  if (vid <= 0 || local_id <= 0 || !check_photo_id (pid) || size < 'a' || size > 'z' || rotate < 0 || rotate > 3) {
    return 0;
  }

  if (write_only) {
    return 1;
  }

  assert (user_loaded (u));

//  dbg ("user_add_photo_location_engine (uid = %d) (pid = %d) (size = %c) (vid = %d) (local_id = %d) (extra = %d) (secret = %llu)\n",
//       u->id, pid, size, vid, local_id, extra, secret);
  int len;
  char *loc = user_photo_get_location (u, pid, original, 1, &len);

  if (loc == NULL) {
    return 0;
  }

  char *s = location_buf;

  if (len > 0 && loc[0] < 0) {
    int cur = -(loc[0] + mode) + 4 * sizeof (int) + 1;
    memcpy (s, loc, cur);
    s += cur;
    loc += cur;
  }
  char *t = s++;

  size -= 'a' - 1;
  size += (rotate << 5);

  char count = 0;
  int old_vid = 0, old_local_id = 0, old_extra = 0, new_vid = 0, new_local_id = 0, i;
  char old_size = 0, new_size = 0, diff;
  unsigned long long old_secret = 0;
  for (i = len ? *loc++ : 0; i > 0; i--) {
    READ_CHAR(loc, old_size);
    assert (old_size);
    if (old_size < 0) {
      old_size *= -1;
      READ_CHAR(loc, diff);
      old_local_id += diff;
    } else {
      READ_INT(loc, old_vid);
      READ_INT(loc, old_local_id);
    }
    READ_INT(loc, old_extra);
    READ_LONG(loc, old_secret);
    if (size <= old_size) {
      save_location (s, size, vid, local_id, extra, secret);

      if (size != old_size) {
        save_location (s, old_size, old_vid, old_local_id, old_extra, old_secret);
      }

      size = 127;
    } else {
      save_location (s, old_size, old_vid, old_local_id, old_extra, old_secret);
    }
  }
  if (size != 127) {
    save_location (s, size, vid, local_id, extra, secret);
  }
  *t = count;

  return user_photo_set_location (u, pid, original, location_buf, (s - location_buf));
}

int user_change_photo_location_server (user *u, int pid, int original, int server_num, int sid) {
  assert (0 <= server_num && server_num <= 1);

  if (write_only) {
    return 1;
  }

  assert (user_loaded (u));

  int len;
  char *loc = user_photo_get_location (u, pid, original, 1, &len);

  if (loc == NULL || len == 0 || loc[0] >= 0) {
    return 0;
  }

  memcpy (location_buf, loc, len);
  assert (len >= 1 + 2 * sizeof (int));
  int *server = (int *)(location_buf + 1);
  server[server_num] = sid;

  return user_photo_set_location (u, pid, original, location_buf, len);
}

int user_del_photo_location (user *u, int pid, int original) {
  if (write_only) {
    return 1;
  }

  assert (user_loaded (u));

  int len;
  char *loc = user_photo_get_location (u, pid, original, 1, &len);

  if (loc == NULL) {
    return 0;
  }
  if (len == 0 || loc[0] >= 0) {
    return 1;
  }

  char *s = location_buf;
  int cur = -(loc[0] + mode) + 4 * sizeof (int) + 1;
  len -= cur;
  loc += cur;

  assert (len > 0);
  memcpy (s, loc, len);

  return user_photo_set_location (u, pid, original, location_buf, len);
}

int user_del_photo_location_engine (user *u, int pid, int original, char size, int rotate) {
  if ((size != -1 && size < 'a') || size > 'z' || rotate < -1 || rotate > 3) {
    return 0;
  }

  if (write_only) {
    return 1;
  }

  assert (user_loaded (u));

  int len;
  char *loc = user_photo_get_location (u, pid, original, 1, &len);

  if (loc == NULL) {
    return 0;
  }

  char *s = location_buf;

  if (len > 0 && loc[0] < 0) {
    int cur = -(loc[0] + mode) + 4 * sizeof (int) + 1;
    memcpy (s, loc, cur);
    s += cur;
    loc += cur;
  }
  char *t = s++;

  char count = 0;
  int old_vid = 0, old_local_id = 0, old_extra = 0, new_vid = 0, new_local_id = 0, i;
  char old_size = 0, new_size = 0, diff;
  unsigned long long old_secret = 0;
  for (i = len ? *loc++ : 0; i > 0; i--) {
    READ_CHAR(loc, old_size);
    assert (old_size);
    if (old_size < 0) {
      old_size *= -1;
      READ_CHAR(loc, diff);
      old_local_id += diff;
    } else {
      READ_INT(loc, old_vid);
      READ_INT(loc, old_local_id);
    }
    READ_INT(loc, old_extra);
    READ_LONG(loc, old_secret);
    if ((rotate != -1 && rotate != (old_size >> 5)) || (size != -1 && size - 'a' + 1 != (old_size & 31))) {
      save_location (s, old_size, old_vid, old_local_id, old_extra, old_secret);
    }
  }
  *t = count;

  return user_photo_set_location (u, pid, original, location_buf, (s - location_buf));
}

int user_save_photo_location (user *u, int pid) {
  if (write_only) {
    return 1;
  }

  assert (user_loaded (u));

  int len;
  char *loc = user_photo_get_location (u, pid, 1, 1, &len);

  if (len > 1) {
    return 0;
  }

  loc = user_photo_get_location (u, pid, 0, 1, &len);

  if (loc == NULL || len <= 1) {
    return 1;
  }

  user_photo_set_location (u, pid, 1, loc, len);
  user_photo_set_location (u, pid, 0, "", 1);

  return 1;
}

int user_restore_photo_location (user *u, int pid) {
  if (write_only) {
    return 1;
  }

  assert (user_loaded (u));

  int len;
  char *loc = user_photo_get_location (u, pid, 1, 1, &len);
  if (loc == NULL || len <= 1) {
    return 0;
  }

  user_photo_set_location (u, pid, 0, loc, len);
  user_photo_set_location (u, pid, 1, "", 1);

  return 1;
}

int user_rotate_photo (user *u, int pid, int dir) {
  if ((dir != 1 && dir != 3) || !check_photo_id (pid)) {
    return 0;
  }

  if (write_only) {
    return 1;
  }

  assert (user_loaded (u));

  int len;
  char *loc = user_photo_get_location (u, pid, 0, 1, &len);

  if (loc == NULL) {
    return 0;
  }

  char *s = location_buf;

  if (len > 0 && loc[0] < 0) {
    int cur = -(loc[0] + mode) + 4 * sizeof (int) + 1;
    memcpy (s, loc, cur);
    s += cur;
    loc += cur;

    int cur_dir = 0;
    if ((s[-1] >= '1' && s[-1] <= '3') && (s[-2] >= 'w' && s[-2] <= 'z')) {
      cur_dir = s[-1];
      s--;
      (*location_buf)++;
    }

    if (s[-1] < 'w' || s[-1] > 'z') {
      *s++ = 'x';
      (*location_buf)--;
    }

    cur_dir += dir;
    if (cur_dir % 4) {
      *s++ = '0' + cur_dir % 4;
      (*location_buf)--;
    }
  }
  char *t = s++;

  char count = 0;
  int pass;
  int new_vid = 0, new_local_id = 0;
  char new_size = 0;
  char *loc_begin = loc;

  for (pass = 0; pass < 2; pass++) {
    loc = loc_begin;
    int old_vid = 0, old_local_id = 0, old_extra = 0, i;
    char old_size = 0, diff;
    unsigned long long old_secret = 0;

    for (i = len ? *loc++ : 0; i > 0; i--) {
      READ_CHAR(loc, old_size);
      assert (old_size);
      if (old_size < 0) {
        old_size *= -1;
        READ_CHAR(loc, diff);
        old_local_id += diff;
      } else {
        READ_INT(loc, old_vid);
        READ_INT(loc, old_local_id);
      }
      READ_INT(loc, old_extra);
      READ_LONG(loc, old_secret);

      int new_dir = (old_size >> 5) + dir;
      if ((new_dir >= 4) ^ pass) {
        save_location (s, ((new_dir & 3) << 5) + (old_size & 31), old_vid, old_local_id, old_extra, old_secret);
      }
    }
  }
  *t = count;
  assert ((len == 0 && count == 0) || *loc_begin == count);

  return user_photo_set_location (u, pid, 0, location_buf, (s - location_buf));
}


int user_change_album (user *u, int aid, field *field_changes, int field_changes_n) {
  dbg ("user_change_album %d: album_id = %d, field_changes_n = %d\n", u->id, aid, field_changes_n);

  if (write_only) {
    return 1;
  }

  assert (user_loaded (u));

  data *d = user_get_album_data (u);
  assert (d != NULL);

  int i;
  for (i = 0; i < field_changes_n; i++) {
//    dbg ("change field #%d\n", field_changes[i].v_fid);
    assert (field_changes[i].v_fid != album_type_id && field_changes[i].v_fid != album_type_owner_id);
  }

  return user_change_data (d, aid, field_changes, field_changes_n, 0) > -1;
}


int user_increm_photo_field (user *u, int pid, int field_id, int cnt) {
  dbg ("user_increm_photo_field %d: photo_id = %d, field_id = %d, cnt = %d\n", u->id, pid, field_id, cnt);

  if (write_only) {
    return 1;
  }

  assert (user_loaded (u));

  int aid = user_get_aid_by_pid (u, pid);
  if (aid == 0) {
    return 0;
  }
  data *d = user_get_photo_data (u, aid);
  if (d == NULL) {
    return 0;
  }

  if (data_get_hidden_state (d, pid) != 0) {
    return 0;
  }

  if (field_id == photo_type_album_id) {
    return 0;
  }

  int lid = data_get_local_id_by_id (d, pid);
  if (lid == -1) {
    return 0;
  }

  actual_object o;
  if (data_get_actual_object (d, lid, &o) < 0) {
    return 0;
  }

  tmp_field_changes_n = 0;
  int old_value = event_get_field_int (&o, PHOTO_TYPE, field_id);
  if (old_value == INT_MIN) {
    old_value = 0;
  }
  TMP_ADD_CHANGE (field_id, int, old_value + cnt);

  return user_change_data (d, pid, tmp_field_changes, tmp_field_changes_n, 0) > -1;
}

int user_increm_album_field (user *u, int aid, int field_id, int cnt) {
  dbg ("user_increm_album_field %d: album_id = %d, field_id = %d, cnt = %d\n", u->id, aid, field_id, cnt);

  if (write_only) {
    return 1;
  }

  assert (user_loaded (u));

  data *d = user_get_album_data (u);
  assert (d != NULL);

  int lid = data_get_local_id_by_id (d, aid);
  if (lid == -1) {
    return 0;
  }

  actual_object o;
  if (data_get_actual_object (d, lid, &o) < 0) {
    return 0;
  }

  tmp_field_changes_n = 0;
  int old_value = event_get_field_int (&o, PHOTO_TYPE, field_id);
  if (old_value == INT_MIN) {
    old_value = 0;
  }
  TMP_ADD_CHANGE (field_id, int, old_value + cnt);

  return user_change_data (d, aid, tmp_field_changes, tmp_field_changes_n, 0) > -1;
}


int check_photo (char *photo, int photo_len) {
  if (photo_len >= 126) {
    return 0;
  }
  assert (photo_len >= 0);

  switch (mode) {
    case MODE_PHOTO:
      return 1;
    case MODE_VIDEO:
      return 1;
    case MODE_AUDIO:
      return 1;
    default:
      assert ("Unsupported mode" && 0);
  }
}


int create_photo_force (user *u, struct lev_photo_create_photo_force *E, int size) {
  int cnt = E->type - (mode << 16) - LEV_PHOTO_CREATE_PHOTO_FORCE;
  assert (cnt > 0 && cnt <= MAX_PHOTOS);

  return user_create_photo_force (u, E->album_id, cnt, E->photo_id) == cnt;
}

int create_album_force (user *u, struct lev_photo_create_album_force *E, int size) {
  return user_create_album_force (u, E->album_id) == 1;
}

int create_photo (user *u, struct lev_photo_create_photo *E, int size) {
  int cnt = E->type - (mode << 16) - LEV_PHOTO_CREATE_PHOTO;
  assert (cnt > 0 && cnt <= MAX_PHOTOS);

  return user_create_photo (u, E->album_id, cnt);
}

int create_album (user *u, struct lev_photo_create_album *E, int size) {
  return user_create_album (u);
}

int delete_photo (user *u, struct lev_photo_delete_photo *E, int size) {
  return user_delete_photo (u, E->photo_id);
}

int delete_album (user *u, struct lev_photo_delete_album *E, int size) {
  return user_delete_album (u, E->album_id);
}

int restore_photo (user *u, struct lev_photo_restore_photo *E, int size) {
  return user_restore_photo (u, E->photo_id);
}

#define MAX_RETURN_FIELDS (MAX_FIELDS + 128 + 1)
int return_fields[MAX_RETURN_FIELDS];

int get_fields (type_desc *t, char *fields) {
  if (!fields[0]) {
    return 0;
  }

  int i = 0, j, res = 0;
  for (j = i; (j == 0 || fields[j - 1] == ',') && res < MAX_RETURN_FIELDS; i = ++j) {
    while (fields[j] != ',' && fields[j]) {
      j++;
    }
//    dbg ("  look for (field = <%s>)\n", fields + i);
    return_fields[res] = get_field_id_len (t, fields + i, j - i);
    if (return_fields[res] < 0) {
      if ((!strncmp (fields + i, "location", 8) || !strncmp (fields + i, "original_location", 17)) && t->name[0] == 'p') {
        int add = 0;
        if (fields[i] == 'o') {
          i += 9;
          add = 128;
        }

        if (j - i == 8) {
          return_fields[res++] = MAX_FIELDS + add;
        } else {
          if (j - i > 26 + 8 + 1) {
            return -1;
          }
          i += 8;
          int t = j, rotate = 0;
          if ('0' <= fields[t - 1] && fields[t - 1] <= '3') {
            t--;
            rotate = fields[t] - '0';
          }
          if (i == t) {
            return -1;
          }
          while (i != t && res < MAX_RETURN_FIELDS) {
            if (fields[i] < 'a' || fields[i] > 'z') {
              return -1;
            }
            return_fields[res++] = MAX_FIELDS + add + (fields[i++] - 'a' + 1) + (rotate << 5);
          }
        }
      } else if (j - i == 8 && !strncmp (fields + i, "ordering", 8)) {
        return_fields[res++] = MAX_FIELDS + 256;
      } else {
        return -1;
      }
    } else {
      res++;
    }
  }
  return res;
}

int get_photos_overview (int uid, char *albums_id, int offset, int limit, char *fields, const int reverse, const int count, char **result) {
  dbg ("get_photo_overview: (uid = %d) (albums = %s) (offset = %d) (limit = %d)\n", uid, albums_id, offset, limit);

  static int albums[MAX_ALBUMS + 1];

  assert (!index_mode && !write_only);

  *result = debug_buff;
  debug_init();

  user *u = conv_uid_get (uid);
  int fields_n = get_fields (&types[PHOTO_TYPE], fields);

  if (u == NULL || albums_id[0] == 0 || fields_n < 0) {
    return 0;
  }

  if (offset < 0) {
    offset = 0;
  }

  if (offset > MAX_RESULT) {
    offset = MAX_RESULT;
  }

  if (limit <= 0) {
    limit = 0;
  }

  if (limit > MAX_RESULT) {
    limit = MAX_RESULT;
  }

  load_user_metafile (u, u->local_id, NOAIO);
  if (!user_loaded (u)) {
    return -2;
  }

  int i, n = 0, mul = 1;
  albums[n] = 0;
  for (i = 0; albums_id[i] && n < MAX_ALBUMS; i++) {
    if (albums_id[i] == '-' && albums[n] == 0 && mul == 1) {
      mul = -1;
    } else if ('0' <= albums_id[i] && albums_id[i] <= '9') {
      albums[n] = albums[n] * 10 + albums_id[i] - '0';
    } else if (albums_id[i] == ',' && albums[n] != 0) {
      albums[n++] *= mul;
      albums[n] = 0;
      mul = 1;
    } else {
      return 0;
    }
  }
  if (albums[n] == 0) {
    return 0;
  }
  albums[n++] *= mul;

  dl_qsort_int (albums, n);
  n = dl_unique_int (albums, n);
  albums[n] = 0;

  int res = user_get_photos_overview (u, albums, n, offset, limit, reverse);
//  dbg ("res = %d\n", res);

  if (res < 0) {
    res = 0;
  }
  if (res > limit) {
    res = limit;
  }

  debug ("a:%d:{", res + count);
  if (count) {
    int total = 0;
    for (i = 0; i < n; i++) {
      int cur = user_get_photos_count (u, albums[i]);
      if (cur > 0) {
        total += cur;
      }
    }
    debug ("s:5:\"count\";i:%d;", total);
  }
  for (i = 0; i < res; i++) {
    int pid = event_get_photo_id (&result_obj[i]);

    if (fields_n == 0) {
      debug ("i:%d;i:%d;", pid, pid);
    } else {
      debug ("i:%d;%s", pid, event_to_array (&result_obj[i], PHOTO_TYPE, return_fields, fields_n));
    }
  }
  debug ("}");

  if (debug_error) {
    return 0;
  }

  return 1;
}

int get_photos_count (int uid, int aid, char *condition) {
  assert (!index_mode && !write_only);

  user *u = conv_uid_get (uid);
  if (u == NULL) {
    return -1;
  }

  load_user_metafile (u, u->local_id, NOAIO);
  if (!user_loaded (u)) {
    return -2;
  }

  predicate *pred = predicate_init (condition, PHOTO_TYPE);

  if (pred == NULL) {
    return user_get_photos_count (u, aid);
  } else {
    return user_get_photos_count_pred (u, aid, pred);
  }
}

int get_albums_count (int uid, char *condition) {
  assert (!index_mode && !write_only);

  user *u = conv_uid_get (uid);
  if (u == NULL) {
    return -1;
  }

  load_user_metafile (u, u->local_id, NOAIO);
  if (!user_loaded (u)) {
    return -2;
  }

  predicate *pred = predicate_init (condition, ALBUM_TYPE);

  int res = user_get_albums (u, 0, MAX_ALBUMS, pred);

  int i, real_res = 0;
  for (i = 0; i < res; i++) {
    int aid = event_get_album_id (&result_obj[i]);

    if (aid > 0) {
      real_res++;
    }
  }

  return real_res;
}

int get_photos (int uid, int aid, int offset, int limit, char *fields, const int reverse, const int count, char *condition, char **result) {
  dbg ("get photos: (uid = %d) (aid = %d) (offset = %d) (limit = %d) (reverse = %d)\n", uid, aid, offset, limit, reverse);
  assert (!index_mode && !write_only);

  *result = debug_buff;
  debug_init();

  user *u = conv_uid_get (uid);
  int fields_n = get_fields (&types[PHOTO_TYPE], fields);

  if (u == NULL || fields_n < 0) {
    return 0;
  }

  if (offset < 0) {
    offset = 0;
  }

  if (offset > MAX_RESULT) {
    offset = MAX_RESULT;
  }

  if (limit < 0) {
    limit = 0;
  }

  if (limit > MAX_RESULT) {
    limit = MAX_RESULT;
  }

  load_user_metafile (u, u->local_id, NOAIO);
  if (!user_loaded (u)) {
    return -2;
  }

  predicate *pred = predicate_init (condition, PHOTO_TYPE);

  int i, ii, total, real_total;
  if (reverse) {
    total = user_get_photos_count_pred (u, aid, pred);
    real_total = total;

    if (total < 0) {
      return 0;
    }

    offset = total - offset - limit;
    if (offset < 0) {
      limit += offset;
      offset = 0;
    }
    if (limit < 0) {
      limit = 0;
    }
  } else {
    if (count && pred != NULL) {
      total = user_get_photos_count_pred (u, aid, pred);
      real_total = total;

      if (total < 0) {
        return 0;
      }

      total -= offset;

      if (total < 0) {
        total = 0;
      }
    } else {
      total = user_get_photos (u, aid, offset, limit, pred);
      real_total = user_get_photos_count (u, aid);
    }
  }

  if (total < 0) {
    return 0;
  }
  if (total > limit) {
    total = limit;
  }

  debug ("a:%d:{", total + count);
  if (count) {
    debug ("s:5:\"count\";i:%d;", real_total);
  }
  for (ii = 0; ii < total; ii++) {
    if (reverse) {
      i = total - ii - 1 + offset;
    } else {
      if (count && pred != NULL) {
        i = ii + offset;
      } else {
        i = ii;
      }
    }

    int pid = event_get_photo_id (&result_obj[i]);

    if (fields_n == 0) {
      debug ("i:%d;i:%d;", pid, pid);
    } else {
      debug ("i:%d;%s", pid, event_to_array (&result_obj[i], PHOTO_TYPE, return_fields, fields_n));
    }
  }
  debug ("}");

  if (debug_error) {
    return 0;
  }

  return 1;
}

int get_albums (int uid, int offset, int limit, char *fields, const int reverse, const int count, char *condition, char **result) {
  dbg ("get_albums (uid = %d) (fields = %s) (offset = %d) (limit = %d)\n", uid, fields, offset, limit);
  assert (!index_mode && !write_only);

  *result = debug_buff;
  debug_init();

  user *u = conv_uid_get (uid);
  int fields_n = get_fields (&types[ALBUM_TYPE], fields);

  if (u == NULL || fields_n < 0) {
    return 0;
  }

  if (offset < 0) {
    offset = 0;
  }

  if (offset > MAX_RESULT) {
    offset = MAX_RESULT;
  }

  if (limit <= 0) {
    limit = 0;
  }

  if (limit > MAX_RESULT) {
    limit = MAX_RESULT;
  }

  load_user_metafile (u, u->local_id, NOAIO);
  if (!user_loaded (u)) {
    return -2;
  }

  predicate *pred = predicate_init (condition, ALBUM_TYPE);

  int i, ii, total, real_total;
  if (reverse) {
    total = user_get_albums_count_pred (u, pred);
    real_total = total;
    assert (total >= 0);

    offset = total - offset - limit;
    if (offset < 0) {
      limit += offset;
      offset = 0;
    }
    if (limit < 0) {
      limit = 0;
    }
  } else {
    if (count && pred != NULL) {
      total = user_get_albums_count_pred (u, pred);
      real_total = total;
      assert (total >= 0);

      total -= offset;

      if (total < 0) {
        total = 0;
      }
    } else {
      total = user_get_albums (u, offset, limit, pred);
      real_total = user_get_albums_count (u);
      assert (total >= 0);
    }
  }

  if (total > limit) {
    total = limit;
  }

  debug ("a:%d:{", total + count);
  if (count) {
    debug ("s:5:\"count\";i:%d;", real_total);
  }
  for (ii = 0; ii < total; ii++) {
    if (reverse) {
      i = total - ii - 1 + offset;
    } else {
      if (count && pred != NULL) {
        i = ii + offset;
      } else {
        i = ii;
      }
    }

    int aid = event_get_album_id (&result_obj[i]);

    if (fields_n == 0) {
      debug ("i:%d;i:%d;", aid, count ? user_get_photos_count (u, aid) : aid);
    } else {
      debug ("i:%d;%s", aid, event_to_array (&result_obj[i], ALBUM_TYPE, return_fields, fields_n));
    }
  }
  debug ("}");

  if (debug_error) {
    return 0;
  }

  return 1;
}

int get_photo (int uid, int pid, const int force, char *fields, char **result) {
  assert (!index_mode && !write_only);

  user *u = conv_uid_get (uid);
  int fields_n = get_fields (&types[PHOTO_TYPE], fields);

  if (u == NULL || fields_n < 0 || !check_photo_id (pid)) {
    return 0;
  }

  load_user_metafile (u, u->local_id, NOAIO);
  if (!user_loaded (u)) {
    return -2;
  }

  actual_object o;
  if (user_get_photo (u, pid, force, &o) < 0) {
    return 0;
  }

  *result = event_to_array (&o, PHOTO_TYPE, return_fields, fields_n);

  if (debug_error) {
    return 0;
  }

  return 1;
}

int get_album (int uid, int aid, const int force, char *fields, char **result) {
  assert (!index_mode && !write_only);

  user *u = conv_uid_get (uid);
  int fields_n = get_fields (&types[ALBUM_TYPE], fields);

  if (u == NULL || fields_n < 0) {
    return 0;
  }

  load_user_metafile (u, u->local_id, NOAIO);
  if (!user_loaded (u)) {
    return -2;
  }

  actual_object o;
  if (user_get_album (u, aid, force, &o) < 0) {
    return 0;
  }

  *result = event_to_array (&o, ALBUM_TYPE, return_fields, fields_n);

  if (debug_error) {
    return 0;
  }

  return 1;
}


int change_photo_order (user *u, struct lev_photo_change_photo_order *E, int size) {
  return user_change_photo_order (u, E->photo_id, E->photo_id_near, E->type - (mode << 16) - LEV_PHOTO_CHANGE_PHOTO_ORDER);
}

int change_album_order (user *u, struct lev_photo_change_album_order *E, int size) {
  return user_change_album_order (u, E->album_id, E->album_id_near, E->type - (mode << 16) - LEV_PHOTO_CHANGE_ALBUM_ORDER);
}

int change_data (struct lev_photo_change_data *E, int size) {
  user *u = conv_uid (E->user_id);

//  dbg ("CHANGE DATA (E->mask = %d)\n", E->mask);
  if (u == NULL) {
    return 0;
  }

  if (write_only) {
    return 1;
  }

  if (!user_loaded (u)) {
    user_add_change (u, (struct lev_generic *)E, size);
    return 1;
  }

  type_desc *t_desc;
  switch (E->type - (mode << 16)) {
    case LEV_PHOTO_CHANGE_PHOTO + 2 ... LEV_PHOTO_CHANGE_PHOTO + MAX_EVENT_SIZE:
      t_desc = &types[PHOTO_TYPE];
      break;
    case LEV_PHOTO_CHANGE_ALBUM + 2 ... LEV_PHOTO_CHANGE_ALBUM + MAX_EVENT_SIZE:
      t_desc = &types[ALBUM_TYPE];
      break;
    default:
      assert (0);
  }

  char *s = (char *)E->changes;

  field_changes_n = 0;

  int i, l = t_int, r = t_string, t;
  for (t = 0; t < 2; t++) {
    for (i = 0; i < 32; i++) {
      if ((E->mask >> i) & 1) {
        field_desc *cur_f = &t_desc->fields[i];
        if (l <= cur_f->type && cur_f->type < r) {
          field *cur = &field_changes[field_changes_n++];
          cur->type = cur_f->type;
          cur->v_fid = i;

          switch (cur_f->type) {
            case t_int:
              READ_INT(s, cur->v_int);
              break;
            case t_long:
              READ_LONG(s, cur->v_long);
              break;
            case t_double:
              READ_DOUBLE(s, cur->v_double);
              break;
            case t_string:
              READ_SHORT(s, cur->v_string_len);
              cur->v_string = s;
              s += cur->v_string_len;
              break;
            case t_raw:
              READ_SHORT(s, cur->v_raw_len);
              cur->v_raw = s;
              s += cur->v_raw_len;
              break;
          }

          if (cur_f->is_const) {
            field_changes_n--;
          }
        }
      }
    }
    l += t_string;
    r += t_string;
  }
  assert (s == (char *)E + size);

  switch (E->type - (mode << 16)) {
    case LEV_PHOTO_CHANGE_PHOTO + 2 ... LEV_PHOTO_CHANGE_PHOTO + MAX_EVENT_SIZE:
      return user_change_photo (u, E->data_id, field_changes, field_changes_n);
    case LEV_PHOTO_CHANGE_ALBUM + 2 ... LEV_PHOTO_CHANGE_ALBUM + MAX_EVENT_SIZE:
      return user_change_album (u, E->data_id, field_changes, field_changes_n);
    default:
      assert (0);
  }
}

int increm_data (user *u, struct lev_photo_increm_data *E, int size) {
  if (u == NULL) {
    return 0;
  }

  if (write_only) {
    return 1;
  }

  int field_id = E->type & 255;

  type_desc *t_desc;
  switch (E->type - (mode << 16) - field_id) {
    case LEV_PHOTO_INCREM_PHOTO_FIELD:
      t_desc = &types[PHOTO_TYPE];
      break;
    case LEV_PHOTO_INCREM_ALBUM_FIELD:
      t_desc = &types[ALBUM_TYPE];
      break;
    default:
      assert (0);
      return 0;
  }

  assert (field_id < t_desc->field_i);
  if (t_desc->fields[field_id].is_const) {
    return 0;
  }
  if (t_desc->fields[field_id].type != t_int) {
    return 0;
  }

  if (!user_loaded (u)) {
    user_add_change (u, (struct lev_generic *)E, size);
    return 1;
  }

  switch (E->type - (mode << 16) - field_id) {
    case LEV_PHOTO_INCREM_PHOTO_FIELD:
      return user_increm_photo_field (u, E->data_id, field_id, E->cnt);
    case LEV_PHOTO_INCREM_ALBUM_FIELD:
      return user_increm_album_field (u, E->data_id, field_id, E->cnt);
    default:
      assert (0);
  }
}

/***
  LOCATION
 ***/

map_int_int volumes;

int set_volume (struct lev_photo_set_volume *E) {
  int *t = map_int_int_add (&volumes, E->volume_id);
  if (t != NULL) {
    *t = E->server_id;
    return 1;
  }
  return 0;
}

int get_server (int vid) {
  int *t = map_int_int_get (&volumes, vid);
  if (t != NULL) {
    return *t;
  }
  return vid / 1000;
}

int add_photo_location (user *u, struct lev_photo_add_photo_location *E, int size) {
  int original = E->type - (mode << 16) - LEV_PHOTO_ADD_PHOTO_LOCATION;
  return user_add_photo_location (u, E->photo_id, original >> 10, E->server_id, E->server_id2, E->orig_owner_id, E->orig_album_id, E->photo);
}

int add_photo_location_engine_old (user *u, struct lev_photo_add_photo_location_engine_old *E, int size) {
  int original_size_rotate = E->type - (mode << 16) - LEV_PHOTO_ADD_PHOTO_LOCATION_ENGINE_OLD;
  return user_add_photo_location_engine (u, E->photo_id, original_size_rotate >> 10, original_size_rotate & 255, (original_size_rotate >> 8) & 3, E->volume_id, E->local_id, 0, E->secret);
}

int add_photo_location_engine (user *u, struct lev_photo_add_photo_location_engine *E, int size) {
  int original_size_rotate = E->type - (mode << 16) - LEV_PHOTO_ADD_PHOTO_LOCATION_ENGINE;
  return user_add_photo_location_engine (u, E->photo_id, original_size_rotate >> 10, original_size_rotate & 255, (original_size_rotate >> 8) & 3, E->volume_id, E->local_id, E->extra, E->secret);
}

int change_photo_location_server (user *u, struct lev_photo_change_photo_location_server *E, int size) {
  int original_server_num = E->type - (mode << 16) - LEV_PHOTO_CHANGE_PHOTO_LOCATION_SERVER;
  return user_change_photo_location_server (u, E->photo_id, original_server_num >> 1, original_server_num & 1, E->server_id);
}

int del_photo_location (user *u, struct lev_photo_del_photo_location *E, int size) {
  int original = E->type - (mode << 16) - LEV_PHOTO_DEL_PHOTO_LOCATION;
  return user_del_photo_location (u, E->photo_id, original >> 10);
}

int del_photo_location_engine (user *u, struct lev_photo_del_photo_location *E, int size) {
  int original_size_rotate = E->type - (mode << 16) - LEV_PHOTO_DEL_PHOTO_LOCATION_ENGINE;
  return user_del_photo_location_engine (u, E->photo_id, (original_size_rotate >> 8) / 5, (original_size_rotate & 255) - 1, (original_size_rotate >> 8) % 5 - 1);
}

int save_photo_location (user *u, struct lev_photo_save_photo_location *E, int size) {
  return user_save_photo_location (u, E->photo_id);
}

int restore_photo_location (user *u, struct lev_photo_restore_photo_location *E, int size) {
  return user_restore_photo_location (u, E->photo_id);
}

int rotate_photo (user *u, struct lev_photo_rotate_photo *E, int size) {
  return user_rotate_photo (u, E->photo_id, E->type - (mode << 16) - LEV_PHOTO_ROTATE_PHOTO);
}

/***
  BINLOG
 ***/

typedef int make_lev_func (user *u, void *E, int size);

int process_lev (int can_load_metafile, struct lev_generic *E, int size, make_lev_func *f) {
  user *u = conv_uid (E->a);
  if (u == NULL) {
    return 0;
  }
  if (can_load_metafile && binlog_readed && !write_only) {
    load_user_metafile (u, u->local_id, NOAIO);
    if (!user_loaded (u)) {
      unalloc_log_event (size);
      return -2;
    }
    return f (u, E, size);
  } else {
    if (user_loaded (u) || write_only) {
      return f (u, E, size);
    } else {
      user_add_change (u, E, size);
      return 1;
    }
  }
}

#define MY_LOG_EVENT_HANDLER(f, can_load_metafile) ({int _t = process_lev (can_load_metafile, (struct lev_generic *)E, size, (make_lev_func *)f); assert (1 || _t > 0); _t;})

int init_photo_data (int schema) {
  replay_logevent = photo_replay_logevent;
  return 0;
}

int do_create_photo_force (int uid, int aid, int cnt, int pid) {
  if (cnt <= 0 || cnt > MAX_PHOTOS) {
    return 0;
  }

  int size = sizeof (struct lev_photo_create_photo_force);
  struct lev_photo_create_photo_force *E =
    alloc_log_event (LEV_PHOTO_CREATE_PHOTO_FORCE + (mode << 16) + cnt, size, uid);

  E->user_id = uid;
  E->album_id = aid;
  E->photo_id = pid;

  return MY_LOG_EVENT_HANDLER (create_photo_force, 1);
}

int do_create_album_force (int uid, int aid) {
  if (!check_album_id (aid)) {
    return 0;
  }
  int size = sizeof (struct lev_photo_create_album_force);
  struct lev_photo_create_album_force *E =
    alloc_log_event (LEV_PHOTO_CREATE_ALBUM_FORCE + (mode << 16), size, uid);

  E->user_id = uid;
  E->album_id = aid;

  return MY_LOG_EVENT_HANDLER (create_album_force, 1);
}

int do_create_photo (int uid, int aid, int cnt) {
  if (cnt <= 0 || cnt > MAX_PHOTOS) {
    return 0;
  }

  int size = sizeof (struct lev_photo_create_photo);
  struct lev_photo_create_photo *E =
    alloc_log_event (LEV_PHOTO_CREATE_PHOTO + (mode << 16) + cnt, size, uid);

  E->user_id = uid;
  E->album_id = aid;

  return MY_LOG_EVENT_HANDLER (create_photo, 1);
}

int do_create_album (int uid) {
  int size = sizeof (struct lev_photo_create_album);
  struct lev_photo_create_album *E =
    alloc_log_event (LEV_PHOTO_CREATE_ALBUM + (mode << 16), size, uid);

  E->user_id = uid;

  return MY_LOG_EVENT_HANDLER (create_album, 1);
}

int do_change_photo_order (int uid, int pid, int pid_near, int is_next) {
  if (!check_photo_id (pid) || pid_near < 0 || pid_near == pid) {
    return 0;
  }
  int size = sizeof (struct lev_photo_change_photo_order);
  struct lev_photo_change_photo_order *E =
    alloc_log_event (LEV_PHOTO_CHANGE_PHOTO_ORDER + (mode << 16) + is_next, size, uid);

  E->user_id = uid;
  E->photo_id = pid;
  E->photo_id_near = pid_near;

  return MY_LOG_EVENT_HANDLER (change_photo_order, 1);
}

int do_change_album_order (int uid, int aid, int aid_near, int is_next) {
  if (aid == 0 || aid_near == aid) {
    return 0;
  }

  int size = sizeof (struct lev_photo_change_album_order);
  struct lev_photo_change_album_order *E =
    alloc_log_event (LEV_PHOTO_CHANGE_ALBUM_ORDER + (mode << 16) + is_next, size, uid);

  E->user_id = uid;
  E->album_id = aid;
  E->album_id_near = aid_near;

  return MY_LOG_EVENT_HANDLER (change_album_order, 1);
}

int do_change_data (int uid, int did, int type) {
  int i, mask = 0, len = 0, pos[MAX_FIELDS];
  for (i = 0; i < field_changes_n; i++) {
    field *cur = &field_changes[i];
    if ((mask >> cur->v_fid) & 1) {
      return 0;
    }
    mask |= 1u << cur->v_fid;
    pos[cur->v_fid] = i;
    switch (cur->type) {
      case t_int:
        len += sizeof (int);
        break;
      case t_long:
        len += sizeof (long long);
        break;
      case t_double:
        len += sizeof (double);
        break;
      case t_string:
        len += sizeof (short) + cur->v_string_len;
        break;
      case t_raw:
        len += sizeof (short) + cur->v_raw_len;
        break;
    }
  }

  assert (2 <= len && len < MAX_EVENT_SIZE);
  int size = offsetof (struct lev_photo_change_data, changes) + len;

  struct lev_photo_change_data *E =
    alloc_log_event (type + (mode << 16) + len, size, uid);

  E->user_id = uid;
  E->data_id = did;
  E->mask = mask;

  char *s = (char *)E->changes;

  int l = t_int, r = t_string, t;
  for (t = 0; t < 2; t++) {
    for (i = 0; i < 32; i++) {
      if ((mask >> i) & 1) {
        field *cur = &field_changes[pos[i]];
        if (l <= (int)cur->type && (int)cur->type < r) {
          switch (cur->type) {
            case t_int:
              WRITE_INT(s, cur->v_int);
              break;
            case t_long:
              WRITE_LONG(s, cur->v_long);
              break;
            case t_double:
              WRITE_DOUBLE(s, cur->v_double);
              break;
            case t_string:
              WRITE_SHORT(s, cur->v_string_len);
              memcpy (s, cur->v_string, cur->v_string_len);
              s += cur->v_string_len;
              break;
            case t_raw:
              WRITE_SHORT(s, cur->v_raw_len);
              memcpy (s, cur->v_raw, cur->v_raw_len);
              s += cur->v_raw_len;
              break;
          }
        }
      }
    }
    l += t_string;
    r += t_string;
  }
  assert (s == (char *)E + size);

  return change_data (E, size);
}

int do_change_photo (int uid, int pid, char *changes) {
  if (changes == NULL) {
    return 0;
  }

  int l = strlen (changes);
  if (!check_photo_id (pid) || l == 0 || l >= MAX_EVENT_SIZE) {
    return 0;
  }

  if (php_get_fields (PHOTO_TYPE, changes) < 0) {
    return 0;
  }

  if (field_changes_n == 0) {
    return 1;
  }

  return do_change_data (uid, pid, LEV_PHOTO_CHANGE_PHOTO);
}

int do_change_album (int uid, int aid, char *changes) {
  if (changes == NULL) {
    return 0;
  }

  int l = strlen (changes);
  if (!check_album_id (aid) || l == 0 || l >= MAX_EVENT_SIZE) {
    return 0;
  }


  if (php_get_fields (ALBUM_TYPE, changes) < 0) {
    return 0;
  }

  if (field_changes_n == 0) {
    return 1;
  }

  return do_change_data (uid, aid, LEV_PHOTO_CHANGE_ALBUM);
}

int do_increment_data (int uid, int did, int field_id, int cnt, int type) {
  assert (0 <= field_id && field_id < MAX_FIELDS);

#if MAX_FIELDS >= 255
#  error Wrong MAX_FIELDS
#endif

  int size = sizeof (struct lev_photo_increm_data);
  struct lev_photo_increm_data *E =
    alloc_log_event (type + (mode << 16) + field_id, size, uid);

  E->user_id = uid;
  E->data_id = did;
  E->cnt = cnt;

  return MY_LOG_EVENT_HANDLER (increm_data, 0);
}

int do_increment_photo_field (int uid, int pid, char *field_name, int cnt) {
  if (field_name == NULL) {
    return 0;
  }

  if (!check_photo_id (pid)) {
    return 0;
  }

  int field_id = get_field_id (types + PHOTO_TYPE, field_name);
  if (field_id < 0) {
    return 0;
  }

  return do_increment_data (uid, pid, field_id, cnt, LEV_PHOTO_INCREM_PHOTO_FIELD);
}

int do_increment_album_field (int uid, int aid, char *field_name, int cnt) {
  if (field_name == NULL) {
    return 0;
  }

  if (!check_album_id (aid)) {
    return 0;
  }

  int field_id = get_field_id (types + ALBUM_TYPE, field_name);
  if (field_id < 0) {
    return 0;
  }

  return do_increment_data (uid, aid, field_id, cnt, LEV_PHOTO_INCREM_ALBUM_FIELD);
}


int do_delete_photo (int uid, int pid) {
  int size = sizeof (struct lev_photo_delete_photo);
  struct lev_photo_delete_photo *E =
    alloc_log_event (LEV_PHOTO_DELETE_PHOTO + (mode << 16), size, uid);

  E->user_id = uid;
  E->photo_id = pid;

  return MY_LOG_EVENT_HANDLER (delete_photo, 0);
}

int do_delete_album (int uid, int aid) {
  int size = sizeof (struct lev_photo_delete_album);
  struct lev_photo_delete_album *E =
    alloc_log_event (LEV_PHOTO_DELETE_ALBUM + (mode << 16), size, uid);

  E->user_id = uid;
  E->album_id = aid;

  return MY_LOG_EVENT_HANDLER (delete_album, 0);
}

int do_restore_photo (int uid, int pid) {
  int size = sizeof (struct lev_photo_restore_photo);
  struct lev_photo_restore_photo *E = alloc_log_event (LEV_PHOTO_RESTORE_PHOTO + (mode << 16), size, uid);

  E->user_id = uid;
  E->photo_id = pid;

  return MY_LOG_EVENT_HANDLER (restore_photo, 1);
}


int do_set_volume (int vid, int sid) {
  int size = sizeof (struct lev_photo_set_volume);
  struct lev_photo_set_volume *E =
    alloc_log_event (LEV_PHOTO_SET_VOLUME + (mode << 16), size, vid);

  E->volume_id = vid;
  E->server_id = sid;

  return set_volume (E);
}

int do_add_photo_location (int uid, int pid, int original, int sid, int sid2, int orig_oid, int orig_aid, char *photo, int l) {
  assert (photo != NULL);

  if (!check_photo (photo, l) || original < 0 || original > 1) {
    dbg ("check_photo failed\n");
    return 0;
  }

  int size = offsetof (struct lev_photo_add_photo_location, photo) + l + 1;
  struct lev_photo_add_photo_location *E =
    alloc_log_event (LEV_PHOTO_ADD_PHOTO_LOCATION + (mode << 16) + l + 1 + (original << 10), size, uid);

  E->user_id = uid;
  E->photo_id = pid;
  E->server_id = sid;
  E->server_id2 = sid2;
  E->orig_owner_id = orig_oid;
  E->orig_album_id = orig_aid;
  memcpy (E->photo, photo, l);
  E->photo[l] = 0;

  return MY_LOG_EVENT_HANDLER (add_photo_location, 1);
}

int do_add_photo_location_engine (int uid, int pid, int original, char sz, int rotate, int vid, int local_id, int extra, unsigned long long secret) {
  if (sz < 'a' || sz > 'z' || rotate < 0 || rotate > 3 || original < 0 || original > 1) {
    return 0;
  }

  int size = sizeof (struct lev_photo_add_photo_location_engine);
  struct lev_photo_add_photo_location_engine *E =
    alloc_log_event (LEV_PHOTO_ADD_PHOTO_LOCATION_ENGINE + (mode << 16) + sz + (rotate << 8) + (original << 10), size, uid);

  E->user_id = uid;
  E->photo_id = pid;
  E->volume_id = vid;
  E->local_id = local_id;
  E->extra = extra;
  E->secret = secret;

  return MY_LOG_EVENT_HANDLER (add_photo_location_engine, 1);
}

int do_change_photo_location_server (int uid, int pid, int original, int server_num, int sid) {
  if (original < 0 || original > 1 || server_num < 0 || server_num > 1) {
    dbg ("change_photo_location_server failed\n");
    return 0;
  }

  int size = sizeof (struct lev_photo_change_photo_location_server);

  struct lev_photo_add_photo_location *E =
    alloc_log_event (LEV_PHOTO_CHANGE_PHOTO_LOCATION_SERVER + (mode << 16) + (original << 1) + server_num, size, uid);

  E->user_id = uid;
  E->photo_id = pid;
  E->server_id = sid;

  return MY_LOG_EVENT_HANDLER (change_photo_location_server, 1);
}

int do_del_photo_location (int uid, int pid, int original) {
  if (original < 0 || original > 1) {
    return 0;
  }

  int size = sizeof (struct lev_photo_del_photo_location);
  struct lev_photo_del_photo_location *E = alloc_log_event (LEV_PHOTO_DEL_PHOTO_LOCATION + (mode << 16) + (original << 10), size, uid);

  E->user_id = uid;
  E->photo_id = pid;

  return MY_LOG_EVENT_HANDLER (del_photo_location, 1);
}

int do_del_photo_location_engine (int uid, int pid, int original, char sz, int rotate) {
  if ((sz != -1 && sz < 'a') || sz > 'z' || rotate < -1 || rotate > 3 || original < 0 || original > 1) {
    return 0;
  }

  int size = sizeof (struct lev_photo_del_photo_location_engine);
  struct lev_photo_del_photo_location_engine *E = alloc_log_event (LEV_PHOTO_DEL_PHOTO_LOCATION_ENGINE + (mode << 16) + sz + (rotate << 8) + 257 + ((original * 5) << 8), size, uid);

  E->user_id = uid;
  E->photo_id = pid;

  return MY_LOG_EVENT_HANDLER (del_photo_location_engine, 1);
}

int do_save_photo_location (int uid, int pid) {
  if (!check_photo_id (pid)) {
    return 0;
  }

  int size = sizeof (struct lev_photo_save_photo_location);
  struct lev_photo_save_photo_location *E =
    alloc_log_event (LEV_PHOTO_SAVE_PHOTO_LOCATION + (mode << 16), size, uid);

  E->user_id = uid;
  E->photo_id = pid;

  return MY_LOG_EVENT_HANDLER (save_photo_location, 1);
}

int do_restore_photo_location (int uid, int pid) {
  if (!check_photo_id (pid)) {
    return 0;
  }

  int size = sizeof (struct lev_photo_restore_photo_location);
  struct lev_photo_restore_photo_location *E =
    alloc_log_event (LEV_PHOTO_RESTORE_PHOTO_LOCATION + (mode << 16), size, uid);

  E->user_id = uid;
  E->photo_id = pid;

  return MY_LOG_EVENT_HANDLER (restore_photo_location, 1);
}

int do_rotate_photo (int uid, int pid, int dir) {
  if (dir == -1) {
    dir = 3;
  }

  if ((dir != 1 && dir != 3) || !check_photo_id (pid)) {
    return 0;
  }

  int size = sizeof (struct lev_photo_rotate_photo);
  struct lev_photo_rotate_photo *E =
    alloc_log_event (LEV_PHOTO_ROTATE_PHOTO + (mode << 16) + dir, size, uid);

  E->user_id = uid;
  E->photo_id = pid;

  return MY_LOG_EVENT_HANDLER (rotate_photo, 1);
}


int tl_do_change_photo (struct tl_act_extra *extra) {
  struct tl_change_photo *e = (struct tl_change_photo *)extra->extra;

  assert (check_photo_id (e->photo_id) && 0 < e->changes_len && e->changes_len < MAX_EVENT_SIZE);

  user *u = conv_uid (e->user_id);
  assert (u != NULL);

  load_user_metafile (u, u->local_id, NOAIO);
  if (!user_loaded (u)) {
    return -2;
  }

  if (rpc_get_fields (PHOTO_TYPE, e->mask, e->changes, e->changes_len) < 0) {
    tl_fetch_set_error_format (TL_ERROR_BAD_VALUE, "Can't parse changes");
    return -1;
  }

  int res = 1;
  if (field_changes_n != 0) {
    res &= do_change_data (e->user_id, e->photo_id, LEV_PHOTO_CHANGE_PHOTO);
  }
  int i;
  for (i = 0; i < location_changes_n; i++) {
    location *cur = &location_changes[i];
    if (cur->is_location_engine) {
      res &= do_add_photo_location_engine (
               e->user_id,
               e->photo_id,
               cur->v_fid == photo_type__original_location,
               cur->location_engine.size,
               cur->location_engine.rotate,
               cur->location_engine.volume_id,
               cur->location_engine.local_id,
               cur->location_engine.extra,
               cur->location_engine.secret);
    } else {
      res &= do_add_photo_location (
               e->user_id,
               e->photo_id,
               cur->v_fid == photo_type__original_location,
               cur->location_old.server_id,
               cur->location_old.server_id2,
               cur->location_old.orig_owner_id,
               cur->location_old.orig_album_id,
               cur->location_old.photo,
               cur->location_old.photo_len);
    }
  }

  tl_store_int (res ? TL_BOOL_TRUE : TL_BOOL_FALSE);
  return 0;
}

int tl_do_change_album (struct tl_act_extra *extra) {
  struct tl_change_album *e = (struct tl_change_album *)extra->extra;

  assert (check_album_id (e->album_id) && 0 < e->changes_len && e->changes_len < MAX_EVENT_SIZE);

  if (rpc_get_fields (ALBUM_TYPE, e->mask, e->changes, e->changes_len) < 0) {
    tl_fetch_set_error_format (TL_ERROR_BAD_VALUE, "Can't parse changes");
    return -1;
  }

  int res = 1;
  if (field_changes_n != 0) {
    res &= do_change_data (e->user_id, e->album_id, LEV_PHOTO_CHANGE_ALBUM);
  }
  assert (location_changes_n == 0);

  tl_store_int (res ? TL_BOOL_TRUE : TL_BOOL_FALSE);
  return 0;
}

int tl_do_get_photos_overview (struct tl_act_extra *extra) {
  struct tl_get_photos_overview *e = (struct tl_get_photos_overview *)extra->extra;

  assert (check_user_id (e->user_id));
  assert (!index_mode && !write_only);

  user *u = conv_uid_get (e->user_id);
  if (u == NULL) {
    if (e->need_count) {
      tl_store_int (TL_VECTOR_TOTAL);
      tl_store_int (0);
    } else {
      tl_store_int (TL_VECTOR);
    }
    tl_store_int (0);
    return 0;
  }

  load_user_metafile (u, u->local_id, NOAIO);
  if (!user_loaded (u)) {
    return -2;
  }

  if (e->offset < 0) {
    e->offset = 0;
  }

  if (e->offset > MAX_RESULT) {
    e->offset = MAX_RESULT;
  }

  if (e->limit <= 0) {
    e->limit = 0;
  }

  if (e->limit > MAX_RESULT) {
    e->limit = MAX_RESULT;
  }

  dl_qsort_int (e->albums, e->albums_cnt);
  e->albums_cnt = dl_unique_int (e->albums, e->albums_cnt);

  int res = user_get_photos_overview (u, e->albums, e->albums_cnt, e->offset, e->limit, e->is_reverse);
  if (res < 0) {
    res = 0;
  }
  if (res > e->limit) {
    res = e->limit;
  }

  int i;
  if (e->need_count) {
    tl_store_int (TL_VECTOR_TOTAL);

    int total = 0;
    for (i = 0; i < e->albums_cnt; i++) {
      int cur = user_get_photos_count (u, e->albums[i]);
      if (cur > 0) {
        total += cur;
      }
    }

    tl_store_int (total);
  } else {
    tl_store_int (TL_VECTOR);
  }

  tl_store_int (res);
  for (i = 0; i < res; i++) {
    event_to_rpc (&result_obj[i], PHOTO_TYPE, e->mask);
  }

  return 0;
}

int tl_do_get_photos (struct tl_act_extra *extra) {
  struct tl_get_photos *e = (struct tl_get_photos *)extra->extra;

  assert (check_user_id (e->user_id) && check_album_id (e->album_id));
  assert (!index_mode && !write_only);

  user *u = conv_uid_get (e->user_id);
  if (u == NULL) {
    if (e->need_count) {
      tl_store_int (TL_VECTOR_TOTAL);
      tl_store_int (0);
    } else {
      tl_store_int (TL_VECTOR);
    }
    tl_store_int (0);
    return 0;
  }

  load_user_metafile (u, u->local_id, NOAIO);
  if (!user_loaded (u)) {
    return -2;
  }

  if (e->offset < 0) {
    e->offset = 0;
  }

  if (e->offset > MAX_RESULT) {
    e->offset = MAX_RESULT;
  }

  if (e->limit <= 0) {
    e->limit = 0;
  }

  if (e->limit > MAX_RESULT) {
    e->limit = MAX_RESULT;
  }

  predicate *pred = predicate_init (e->condition, PHOTO_TYPE);
  if (e->condition[0] != 0 && pred == NULL) {
    tl_fetch_set_error_format (TL_ERROR_BAD_VALUE, "Wrong condition \"%s\" specified", e->condition);
    return -1;
  }

  int i, ii, total, real_total;
  if (e->is_reverse) {
    total = user_get_photos_count_pred (u, e->album_id, pred);
    real_total = total;

    if (total < 0) {
      tl_fetch_set_error_format (TL_ERROR_BAD_VALUE, "There is no album %d with owner_id %d", e->album_id, e->user_id);
      return -1;
    }

    e->offset = total - e->offset - e->limit;
    if (e->offset < 0) {
      e->limit += e->offset;
      e->offset = 0;
    }
    if (e->limit < 0) {
      e->limit = 0;
    }
  } else {
    if (e->need_count && pred != NULL) {
      total = user_get_photos_count_pred (u, e->album_id, pred);
      real_total = total;

      if (total < 0) {
        tl_fetch_set_error_format (TL_ERROR_BAD_VALUE, "There is no album %d with owner_id %d", e->album_id, e->user_id);
        return -1;
      }

      total -= e->offset;

      if (total < 0) {
        total = 0;
      }
    } else {
      total = user_get_photos (u, e->album_id, e->offset, e->limit, pred);
      real_total = user_get_photos_count (u, e->album_id);
    }
  }

  if (total < 0) {
    tl_fetch_set_error_format (TL_ERROR_BAD_VALUE, "There is no album %d with owner_id %d", e->album_id, e->user_id);
    return -1;
  }
  if (total > e->limit) {
    total = e->limit;
  }

  if (e->need_count) {
    tl_store_int (TL_VECTOR_TOTAL);
    tl_store_int (real_total);
  } else {
    tl_store_int (TL_VECTOR);
  }

  tl_store_int (total);
  for (ii = 0; ii < total; ii++) {
    if (e->is_reverse) {
      i = total - ii - 1 + e->offset;
    } else {
      if (e->need_count && pred != NULL) {
        i = ii + e->offset;
      } else {
        i = ii;
      }
    }

    event_to_rpc (&result_obj[i], PHOTO_TYPE, e->mask);
  }

  return 0;
}

int tl_do_get_albums (struct tl_act_extra *extra) {
  struct tl_get_albums *e = (struct tl_get_albums *)extra->extra;

  assert (check_user_id (e->user_id));
  assert (!index_mode && !write_only);

  user *u = conv_uid_get (e->user_id);
  if (u == NULL) {
    if (e->need_count) {
      tl_store_int (TL_VECTOR_TOTAL);
      tl_store_int (0);
    } else {
      tl_store_int (TL_VECTOR);
    }
    tl_store_int (0);
    return 0;
  }

  load_user_metafile (u, u->local_id, NOAIO);
  if (!user_loaded (u)) {
    return -2;
  }

  if (e->offset < 0) {
    e->offset = 0;
  }

  if (e->offset > MAX_RESULT) {
    e->offset = MAX_RESULT;
  }

  if (e->limit <= 0) {
    e->limit = 0;
  }

  if (e->limit > MAX_RESULT) {
    e->limit = MAX_RESULT;
  }

  predicate *pred = predicate_init (e->condition, ALBUM_TYPE);
  if (e->condition[0] != 0 && pred == NULL) {
    tl_fetch_set_error_format (TL_ERROR_BAD_VALUE, "Wrong condition \"%s\" specified", e->condition);
    return -1;
  }

  int i, ii, total, real_total;
  if (e->is_reverse) {
    total = user_get_albums_count_pred (u, pred);
    real_total = total;
    assert (total >= 0);

    e->offset = total - e->offset - e->limit;
    if (e->offset < 0) {
      e->limit += e->offset;
      e->offset = 0;
    }
    if (e->limit < 0) {
      e->limit = 0;
    }
  } else {
    if (e->need_count && pred != NULL) {
      total = user_get_albums_count_pred (u, pred);
      real_total = total;
      assert (total >= 0);

      total -= e->offset;

      if (total < 0) {
        total = 0;
      }
    } else {
      total = user_get_albums (u, e->offset, e->limit, pred);
      real_total = user_get_albums_count (u);
      assert (total >= 0);
    }
  }

  if (total > e->limit) {
    total = e->limit;
  }

  if (e->need_count) {
    tl_store_int (TL_VECTOR_TOTAL);
    tl_store_int (real_total);
  } else {
    tl_store_int (TL_VECTOR);
  }

  tl_store_int (total);
  for (ii = 0; ii < total; ii++) {
    if (e->is_reverse) {
      i = total - ii - 1 + e->offset;
    } else {
      if (e->need_count && pred != NULL) {
        i = ii + e->offset;
      } else {
        i = ii;
      }
    }

    event_to_rpc (&result_obj[i], ALBUM_TYPE, e->mask);
  }

  return 0;
}

int tl_do_get_photo (struct tl_act_extra *extra) {
  struct tl_get_photo *e = (struct tl_get_photo *)extra->extra;

  assert (check_user_id (e->user_id) && check_photo_id (e->photo_id));
  assert (!index_mode && !write_only);

  user *u = conv_uid_get (e->user_id);
  if (u == NULL) {
    tl_store_int (TL_MAYBE_FALSE);
    return 0;
  }

  load_user_metafile (u, u->local_id, NOAIO);
  if (!user_loaded (u)) {
    return -2;
  }

  actual_object o;
  if (user_get_photo (u, e->photo_id, e->force, &o) < 0) {
    tl_store_int (TL_MAYBE_FALSE);
    return 0;
  }

  tl_store_int (TL_MAYBE_TRUE);
  event_to_rpc (&o, PHOTO_TYPE, e->mask);
  return 0;
}

int tl_do_get_album (struct tl_act_extra *extra) {
  struct tl_get_album *e = (struct tl_get_album *)extra->extra;

  assert (check_user_id (e->user_id) && check_album_id (e->album_id));
  assert (!index_mode && !write_only);

  user *u = conv_uid_get (e->user_id);
  if (u == NULL) {
    tl_store_int (TL_MAYBE_FALSE);
    return 0;
  }

  load_user_metafile (u, u->local_id, NOAIO);
  if (!user_loaded (u)) {
    return -2;
  }

  actual_object o;
  if (user_get_album (u, e->album_id, e->force, &o) < 0) {
    tl_store_int (TL_MAYBE_FALSE);
    return 0;
  }

  tl_store_int (TL_MAYBE_TRUE);
  event_to_rpc (&o, ALBUM_TYPE, e->mask);
  return 0;
}


void try_init_local_uid (void) {
  static int was = 0;
  static int old_log_split_min, old_log_split_max, old_log_split_mod;

  if (was) {
//    fprintf (stderr, "%d vs %d | %d vs %d | %d vs %d\n", old_log_split_min, log_split_min, old_log_split_max, log_split_max, old_log_split_mod, log_split_mod);
    assert (old_log_split_min == log_split_min && old_log_split_max == log_split_max && old_log_split_mod == log_split_mod);
    return;
  } else {
    was = 1;
    old_log_split_min = log_split_min;
    old_log_split_max = log_split_max;
    old_log_split_mod = log_split_mod;
  }

  //TODO why this is here?
  int i;
  for (i = 1; i <= header.user_cnt; i++) {
    assert (conv_uid (header.user_index[i].id) != NULL);
  }

  log_schema = PHOTO_SCHEMA_V1;
  init_photo_data (log_schema);
}

static int photo_le_start (struct lev_start *E) {
  if (E->schema_id != PHOTO_SCHEMA_V1) {
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
#define MY_REPLAY_LOG_EVENT_HANDLER(x, y)               \
  if (size < (int)sizeof (struct DL_ADD_SUFF (x, y))) { \
    return -2;                                          \
  }                                                     \
  size = sizeof (struct DL_ADD_SUFF (x, y));            \
  MY_LOG_EVENT_HANDLER (y, 0);                          \
  return size;

int photo_replay_logevent (struct lev_generic *E, int size) {
  if (index_mode) {
    if (((_eventsLeft && --_eventsLeft == 0) || dl_get_memory_used() > max_memory) && !binlog_readed) {
      binlog_readed = 1;
      save_index (NULL);
      exit (13);
    }
  }

  int old_size, s;
  switch (E->type) {
  case LEV_START:
    if (size < 24 || E->b < 0 || E->b > 4096) { return -2; }
    s = 24 + ((E->b + 3) & -4);
    if (size < s) { return -2; }
    return photo_le_start ((struct lev_start *) E) >= 0 ? s : -1;
  case LEV_NOOP:
  case LEV_TIMESTAMP:
  case LEV_CRC32:
  case LEV_ROTATE_FROM:
  case LEV_ROTATE_TO:
  case LEV_TAG:
    return default_replay_logevent (E, size);
  }

  switch (E->type - (mode << 16)) {
  case LEV_PHOTO_CREATE_PHOTO_FORCE ... LEV_PHOTO_CREATE_PHOTO_FORCE + MAX_PHOTOS:
    MY_REPLAY_LOG_EVENT_HANDLER(lev_photo, create_photo_force);
  case LEV_PHOTO_CREATE_ALBUM_FORCE ... LEV_PHOTO_CREATE_ALBUM_FORCE + MAX_ALBUMS:
    MY_REPLAY_LOG_EVENT_HANDLER(lev_photo, create_album_force);
  case LEV_PHOTO_CREATE_PHOTO ... LEV_PHOTO_CREATE_PHOTO + MAX_PHOTOS:
    MY_REPLAY_LOG_EVENT_HANDLER(lev_photo, create_photo);
  case LEV_PHOTO_CREATE_ALBUM ... LEV_PHOTO_CREATE_ALBUM + MAX_ALBUMS:
    MY_REPLAY_LOG_EVENT_HANDLER(lev_photo, create_album);
  case LEV_PHOTO_DELETE_PHOTO:
    MY_REPLAY_LOG_EVENT_HANDLER(lev_photo, delete_photo);
  case LEV_PHOTO_DELETE_ALBUM:
    MY_REPLAY_LOG_EVENT_HANDLER(lev_photo, delete_album);
  case LEV_PHOTO_RESTORE_PHOTO:
    MY_REPLAY_LOG_EVENT_HANDLER(lev_photo, restore_photo);
  case LEV_PHOTO_CHANGE_PHOTO_ORDER:
  case LEV_PHOTO_CHANGE_PHOTO_ORDER + 1:
    MY_REPLAY_LOG_EVENT_HANDLER(lev_photo, change_photo_order);
  case LEV_PHOTO_CHANGE_ALBUM_ORDER:
  case LEV_PHOTO_CHANGE_ALBUM_ORDER + 1:
    MY_REPLAY_LOG_EVENT_HANDLER(lev_photo, change_album_order);
  case LEV_PHOTO_CHANGE_PHOTO + 2 ... LEV_PHOTO_CHANGE_PHOTO + MAX_EVENT_SIZE:
    if (size < (int)sizeof (struct lev_photo_change_data)) {
      return -2;
    }
    s = E->type - (mode << 16) - LEV_PHOTO_CHANGE_PHOTO + offsetof (struct lev_photo_change_data, changes);
    if (size < s) {
      return -2;
    }
    change_data ((struct lev_photo_change_data *)E, s);
    return s;
  case LEV_PHOTO_CHANGE_ALBUM + 2 ... LEV_PHOTO_CHANGE_ALBUM + MAX_EVENT_SIZE:
    if (size < (int)sizeof (struct lev_photo_change_data)) {
      return -2;
    }
    s = E->type - (mode << 16) - LEV_PHOTO_CHANGE_ALBUM + offsetof (struct lev_photo_change_data, changes);
    if (size < s) {
      return -2;
    }
    change_data ((struct lev_photo_change_data *)E, s);
    return s;
  case LEV_PHOTO_INCREM_PHOTO_FIELD ... LEV_PHOTO_INCREM_PHOTO_FIELD + MAX_FIELDS - 1:
  case LEV_PHOTO_INCREM_ALBUM_FIELD ... LEV_PHOTO_INCREM_ALBUM_FIELD + MAX_FIELDS - 1:
    MY_REPLAY_LOG_EVENT_HANDLER(lev_photo, increm_data);
  case LEV_PHOTO_SET_VOLUME:
    STANDARD_LOG_EVENT_HANDLER(lev_photo, set_volume);
  case LEV_PHOTO_ADD_PHOTO_LOCATION + 1 ... LEV_PHOTO_ADD_PHOTO_LOCATION + 127:
  case LEV_PHOTO_ADD_PHOTO_LOCATION + 1 + (1 << 10) ... LEV_PHOTO_ADD_PHOTO_LOCATION + 127 + (1 << 10):
    if (size < (int)sizeof (struct lev_photo_add_photo_location)) {
      return -2;
    }
    old_size = size;
    size = ((E->type - (mode << 16) - LEV_PHOTO_ADD_PHOTO_LOCATION) & 127) + offsetof (struct lev_photo_add_photo_location, photo);
    if (old_size < size) {
      return -2;
    }
    //add_photo_location ((struct lev_photo_add_photo_location *)E, s);
    MY_LOG_EVENT_HANDLER (add_photo_location, 0);
    return size;
  case LEV_PHOTO_ADD_PHOTO_LOCATION_ENGINE_OLD + 'a' ... LEV_PHOTO_ADD_PHOTO_LOCATION_ENGINE_OLD + 'z' + (3 << 8) + (1 << 10):
    MY_REPLAY_LOG_EVENT_HANDLER(lev_photo, add_photo_location_engine_old);
  case LEV_PHOTO_ADD_PHOTO_LOCATION_ENGINE + 'a' ... LEV_PHOTO_ADD_PHOTO_LOCATION_ENGINE + 'z' + (3 << 8) + (1 << 10):
    MY_REPLAY_LOG_EVENT_HANDLER(lev_photo, add_photo_location_engine);
  case LEV_PHOTO_CHANGE_PHOTO_LOCATION_SERVER ... LEV_PHOTO_CHANGE_PHOTO_LOCATION_SERVER + 3:
    MY_REPLAY_LOG_EVENT_HANDLER(lev_photo, change_photo_location_server);
  case LEV_PHOTO_DEL_PHOTO_LOCATION:
  case LEV_PHOTO_DEL_PHOTO_LOCATION + (1 << 10):
    MY_REPLAY_LOG_EVENT_HANDLER(lev_photo, del_photo_location);
  case LEV_PHOTO_DEL_PHOTO_LOCATION_ENGINE ... LEV_PHOTO_DEL_PHOTO_LOCATION_ENGINE + 'z' + (3 << 8) + 257 + (5 << 8):
    MY_REPLAY_LOG_EVENT_HANDLER(lev_photo, del_photo_location_engine);
  case LEV_PHOTO_SAVE_PHOTO_LOCATION:
    MY_REPLAY_LOG_EVENT_HANDLER(lev_photo, save_photo_location);
  case LEV_PHOTO_RESTORE_PHOTO_LOCATION:
    MY_REPLAY_LOG_EVENT_HANDLER(lev_photo, restore_photo_location);
  case LEV_PHOTO_ROTATE_PHOTO + 1:
  case LEV_PHOTO_ROTATE_PHOTO + 3:
    MY_REPLAY_LOG_EVENT_HANDLER(lev_photo, rotate_photo);
  }

  fprintf (stderr, "unknown log event type %08x at position %lld\n", E->type, log_cur_pos());

  return -1;
}


/***
  AIO
 ***/

int onload_user_metafile (struct connection *c, int read_bytes);
void bind_user_metafile (user *u);
void unbind_user_metafile (user *u);

conn_type_t ct_metafile_aio = {
  .magic = CONN_FUNC_MAGIC,
  .wakeup_aio = onload_user_metafile
};

long long allocated_metafile_bytes;

void load_user_metafile (user *u, int local_id, int no_aio) {
  static struct aio_connection empty_aio_conn;

  WaitAioArrClear();

  if (user_loaded (u)) {
    del_user_used (u);
    add_user_used (u);
    return;
  }

  //TODO: review second condition
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
      WaitAioArrAdd (u->aio);
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
    fprintf (stderr, "no space to load metafile - cannot allocate %d bytes (%lld currently used)\n", u->metafile_len, (long long)dl_get_memory_used());
    assert (0);
  }
  allocated_metafile_bytes += u->metafile_len;

  if (verbosity > 2) {
    fprintf (stderr, "*** Scheduled reading user data from index %d at position %lld, %d bytes, noaio = %d\n", fd[0], header.user_index[local_id].shift, u->metafile_len, no_aio);
  }

  assert (1 <= local_id && local_id <= header.user_cnt);
  if (no_aio) {
    double disk_time = -get_utime (CLOCK_MONOTONIC);

    //assert (lseek (fd[0], header.user_index[local_id].shift, SEEK_SET) == header.user_index[local_id].shift);
    int size = header.user_index[local_id].size;
    int r = pread (fd[0], u->metafile, size, header.user_index[local_id].shift);
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
    WaitAioArrAdd (u->aio);
  }

  return;
}

int onload_user_metafile (struct connection *c, int read_bytes) {
  INIT;

  if (verbosity > 2) {
    fprintf (stderr, "onload_user_metafile (%p,%d)\n", c, read_bytes);
  }

  struct aio_connection *a = (struct aio_connection *)c;
  user *u = (user *) a->extra;

  assert (a->basic_type == ct_aio);
  assert (u != NULL);
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
    RETURN(load_user, 0);
  }
  assert (read_bytes == u->metafile_len);

  if (verbosity > 2) {
    fprintf (stderr, "*** Read user: read %d bytes\n", read_bytes);
  }

  u->aio = NULL;

  bind_user_metafile (u);
  user_process_changes (u);

  add_user_used (u);
  cur_users++;

  //fix:
  while (allocated_metafile_bytes > max_memory * MEMORY_USER_PERCENT || (0 && cur_users > 1)) {
    assert (user_LRU_unload() != -1);
  }

  RETURN(load_user, 1);
}

int unload_user_metafile (user *u) {
  INIT;

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
    RETURN(unload_user, 0);
  }

  allocated_metafile_bytes -= u->metafile_len;

  unbind_user_metafile (u);

  if (verbosity > 2) {
    fprintf (stderr, "unload_user_metafile (%d) END\n", user_id);
  }

  RETURN(unload_user, 1);
}

void bind_user_metafile (user *u) {
  if (verbosity > 2) {
    fprintf (stderr, "bind user metafile local id = %d (%p)\n", (int)(u - users), u);
  }
  dbg ("bind user metafile local id = %d (%p)\n", (int)(u - users), u);
  int local_id = (int)(u - users);

  if (u->metafile == NULL || u->metafile == EMPTY__METAFILE) {
    assert (u->metafile != NULL);
    dbg ("empty metafile found\n");

    data_load (&u->d, EMPTY__METAFILE, sizeof (int));
    lookup_load (&u->album_by_photo, NULL, 0);
    u->albums_n = 0;

    return;
  }

  assert (u->metafile_len >= (int)sizeof (int));
  unsigned int crc;

  //dbg ("local_id = %d\n", local_id);
  assert (1 <= local_id && local_id <= header.user_cnt);
  //  data d;
  //  lookup album_by_photo;
  //  int albums_n, *albums_id, *albums_offset;
  //  ...
  //  album datas
  char *s = u->metafile;
  int len = u->metafile_len;

  READ_INT (s, crc);
//  dbg ("crc = %u, len = %d\n", crc, u->metafile_len);
  crc32_check_and_repair (s, u->metafile_len - sizeof (int), &crc, 1);

  //TODO add asserts
  READ_INT (s, len);
//  dbg ("data_len = %d\n", len);
  data_load (&u->d, s, len);
  s += len;

  READ_INT (s, u->albums_n);
//  dbg ("albums_n = %d\n", u->albums_n);
  u->albums_id = (int *)s;
  s += sizeof (int) * u->albums_n;
  u->albums_offset = (int *)s;
  s += sizeof (int) * (u->albums_n + 1);

  s = u->metafile + u->albums_offset[u->albums_n];

  READ_INT (s, len);
//  dbg ("lookup_len = %d\n", len);
  lookup_load (&u->album_by_photo, s, len);
  s += len;

  assert (s <= u->metafile + u->metafile_len);
}

int todel[MAX_ALBUMS], todel_n;

void check_data (int *i, vptr *_d) {
  data *d = (data *) *_d;
  if (data_unload (d)) {
    assert (todel_n < MAX_ALBUMS);
    data_free (d);
    dl_free (d, sizeof (data));
    todel[todel_n++] = *i;
  }
}

void unbind_user_metafile (user *u) {
  assert (u != NULL);

  if (verbosity > 2) {
    fprintf (stderr, "unbind_user_metafile\n");
  }
  todel_n = 0;
  map_int_vptr_foreach (&u->photos, check_data);
  int i;
  for (i = 0; i < todel_n; i++) {
    map_int_vptr_del (&u->photos, todel[i]);
  }

 // map_int_vptr_pack (&u->photos);

  data_unload (&u->d);
  lookup_unload (&u->album_by_photo);

  if (u->metafile != NULL && u->metafile != EMPTY__METAFILE) {
    dl_free (u->metafile, u->metafile_len);
  }

  u->metafile = NULL;
  u->metafile_len = -1;
}

void data_free_foreach (int *a, void **v) {
  data_free (*(data **)v);
  dl_free (*(data **)v, sizeof (data));
}

void user_unloaded_free (user *u) {
  user_free_changes (u);

  lookup_free (&u->album_by_photo);
  set_int_free (&u->deleted_albums);
  map_int_vptr_foreach (&u->photos, data_free_foreach);
  map_int_vptr_free (&u->photos);
  data_free (&u->d);
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

    header.created_at = time (NULL);
    header_size = sizeof (index_header);

    return 0;
  }

  dbg ("load_header\n");

  fd[0] = Index->fd;
  int offset = Index->offset;

  //read header
  assert (lseek (fd[0], offset, SEEK_SET) == offset);

  int size = sizeof (index_header) - sizeof (long);
  int r = read (fd[0], &header, size);
  if (r != size) {
    fprintf (stderr, "error reading header from index file: read %d bytes instead of %d at position %d: %m\n", r, size, offset);
    assert (r == size);
  }

  if (header.magic != PHOTO_INDEX_MAGIC) {
    fprintf (stderr, "bad photo index file header\n");
    fprintf (stderr, "magic = 0x%08x // offset = %d\n", header.magic, offset);
    assert (0);
  }

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

  int buf_len = header.volumes_len;
  char *buf = dl_malloc (buf_len);

  dbg ("load volumes (len = %lld) (st = %lld)\n", header.volumes_len, header.volumes_st);
  assert (lseek (fd[0], header.volumes_st, SEEK_SET));
  assert (read (fd[0], buf, header.volumes_len) == header.volumes_len);
  map_int_int_decode (&volumes, buf, header.volumes_len);

  dl_free (buf, buf_len);

  return 1;
}

void free_header (index_header *header) {
  if (header->user_index != NULL) {
    dl_free (header->user_index, sizeof (user_index_data) * (header->user_cnt + 1));
  }
}


int save_index (char *dump_fields_str) {
  static int albums[MAX_RESULT];
  int dump_fields[MAX_FIELDS], dump_fields_cnt = 0;
  field_desc *dump_field_descs[MAX_FIELDS];
  FILE *dump_f = NULL;

  char *newidxname = NULL;

  dbg ("save_index\n");
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

  if (dump_fields_str != NULL && cur_local_id) {
    char *s = dump_fields_str, *t;
    do {
      t = strchrnul (s, ',');
      int cur_field = get_field_id_len (&types[PHOTO_TYPE], s, t - s);
      if (cur_field == -1) {
        fprintf (stderr, "Can't find field \"%.*s\"\n", (int)(t - s), s);
        exit (1);
      }

      field_desc *cur_field_desc = &types[PHOTO_TYPE].fields[cur_field];
      if (cur_field_desc->type == t_raw) {
        fprintf (stderr, "Can't dump raw types\n");
        exit (1);
      }

      dump_fields[dump_fields_cnt] = cur_field;
      dump_field_descs[dump_fields_cnt++] = cur_field_desc;
      s = t + 1;
    } while (dump_fields_cnt < MAX_FIELDS && *t);

    if (*t) {
      fprintf (stderr, "Maximal number of dump fields (%d) exceeded\n", MAX_FIELDS);
      exit (1);
    }

    char dump_name[100];
    assert (snprintf (dump_name, 100, "%s.dump", index_name) < 100);

    dump_f = fopen (dump_name, "w");
    assert (dump_f);
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

  header.magic = PHOTO_INDEX_MAGIC;

  header.user_index = dl_malloc0 (sizeof (user_index_data) * (header.user_cnt + 1));
  header.created_at = time (NULL);

  long long fCurr = get_index_header_size (&header) - sizeof (long);
  assert (lseek (fd[1], fCurr, SEEK_SET) == fCurr);

  int save_buffer_len = 1 << 27;
  char *save_buffer = dl_malloc (save_buffer_len);

  total_photos = 0;
  int extra_photos_max_n = MAX_PHOTOS_DYN - MAX_PHOTOS + MAX_HIDE;
  size_t extra_photos_size = sizeof (int) * extra_photos_max_n;
  int *extra_photos = dl_malloc (extra_photos_size);
  //for each user
  int u_id;
  for (u_id = 1; u_id <= header.user_cnt; u_id++) {
    if (verbosity > 1) {
      fprintf (stderr, "u_id = %d, fd = %d\n", u_id, fd[1]);
    }
    user *u = &users[u_id];

    load_user_metafile (u, u_id, 1);
    assert (user_loaded (u));

    dbg ("save_user %d (current_photo_id = %d) (current_album_id = %d)\n", u->id, u->current_photo_id, u->current_album_id);

    if (dump_f != NULL) {
      int i, albums_n = user_get_albums (u, 0, MAX_RESULT, NULL);
      assert (albums_n != -1);
      for (i = 0; i < albums_n; i++) {
        albums[i] = event_get_album_id (&result_obj[i]);
      }

      for (i = 0; i < albums_n; i++) {
        data *d = user_get_photo_data (u, albums[i]);
        assert (d != NULL);

        int j, photos_n = user_get_photos (u, albums[i], 0, MAX_RESULT, NULL);
        assert (photos_n != -1);
        for (j = 0; j < photos_n; j++) {
          event *e = result_obj[j].obj, *e2 = result_obj[j].dyn;
          int k;
          for (k = 0; k < dump_fields_cnt; k++) {
            event *ev = NULL;
            if (e2 != NULL && has_field (e2, dump_fields[k])) {
              ev = e2;
            } else if (e != NULL && has_field (e, dump_fields[k])) {
              ev = e;
            }
            if (ev != NULL) {
              field_desc *f = dump_field_descs[k];

              switch (f->type) {
                case t_int:
                  fprintf (dump_f, "%d", *GET_INT(ev, f));
                  break;
                case t_long:
                  fprintf (dump_f, "%lld", *GET_LONG(ev, f));
                  break;
                case t_double:
                  fprintf (dump_f, "%.6lf", *GET_DOUBLE(ev, f));
                  break;
                case t_string: {
                  char *p = GET_STRING(ev, f);
                  while (*p) {
                    switch (*p) {
                      case '\t':
                      case '\n':
                      case '\\':
                        fputc ('\\', dump_f);
                      default:
                        fputc (*p++, dump_f);
                    }
                  }
                  break;
                }
                default:
                  assert (0);
              }
            } else {
              fputc ('\\', dump_f);
              fputc ('N', dump_f);
            }
            fputc ("\t\n"[k + 1 == dump_fields_cnt], dump_f);
          }
        }
      }
    }

    header.user_index[u_id].id = u->id;
    header.user_index[u_id].current_photo_id = u->current_photo_id;
    header.user_index[u_id].current_album_id = u->current_album_id;
    if (verbosity > 1) {
      fprintf (stderr, "user_id = %d\n", header.user_index[u_id].id);
    }

    assert (conv_uid (header.user_index[u_id].id) != NULL);

//  write user
//  data d;
//  lookup album_by_photo;
//  int albums_n, *albums_id, *albums_offset;
//  ...
//  album datas

    int i, *t;
    char *buff = save_buffer;
    int buff_size = sizeof (int), buff_mx = save_buffer_len;

    assert (buff_size + (int)sizeof (int) <= buff_mx);
    t = (int *)(buff + buff_size);
    buff_size += sizeof (int);
    buff_size += *t = data_save (&u->d, buff + buff_size, buff_mx - buff_size, -1, NULL, NULL, 0);
    dbg ("data_size = %d\n", *t);

    for (i = 0; i < u->albums_n; i++) {
      user_get_photo_data (u, u->albums_id[i]);
    }

    int nn = map_int_vptr_used (&u->photos);
    int *a = dl_malloc (sizeof (int) * nn);
    data **b = dl_malloc (sizeof (vptr) * nn);
    pair_int_vptr *c = dl_malloc (sizeof (pair_int_vptr) * nn);

    assert (map_int_vptr_pairs (&u->photos, a, (vptr *)b, nn) == nn);
    for (i = 0; i < nn; i++) {
      c[i].x = a[i];
      c[i].y = b[i];
    }
    dl_qsort_pair_int_vptr (c, nn);

    assert (buff_size + (int)sizeof (int) + (int)sizeof (int) * (nn * 2 + 1) <= buff_mx);
    *(int *)(buff + buff_size) = nn;
    buff_size += sizeof (int);

    int albums_id_shift = buff_size;
    int *albums_id = (int *)(buff + albums_id_shift);
    buff_size += sizeof (int) * nn;

    int albums_offset_shift = buff_size;
    int *albums_offset = (int *)(buff + albums_offset_shift);
    buff_size += sizeof (int) * (nn + 1);

    dbg ("for albums_header: %lld\n", (long long)(sizeof (int) + sizeof (int) * nn + sizeof (int) * (nn + 1)));

    for (i = 0; i < nn; i++) {
      albums_id[i] = c[i].x;
      albums_offset[i] = buff_size;

      int extra_photos_n = 0;
      buff_size += data_save (c[i].y, buff + buff_size, buff_mx - buff_size, MAX_PHOTOS, extra_photos, &extra_photos_n, extra_photos_max_n);

      assert (extra_photos_n <= extra_photos_max_n);
      int j;
      for (j = 0; j < extra_photos_n; j++) {
        lookup_set (&u->album_by_photo, extra_photos[j], 0);
      }

      if (buff_mx - buff_size < (4 << 20)) {
        assert (buff_mx < (1 << 29));

        save_buffer = buff = dl_realloc (buff, buff_mx * 2, buff_mx);
        buff_mx *= 2;
        save_buffer_len *= 2;

        albums_id = (int *)(buff + albums_id_shift);
        albums_offset = (int *)(buff + albums_offset_shift);
      }
    }
    albums_offset[nn] = buff_size;

    assert (buff_size + (int)sizeof (int) <= buff_mx);
    t = (int *)(buff + buff_size);
    buff_size += sizeof (int);
    buff_size += *t = lookup_save (&u->album_by_photo, buff + buff_size, buff_mx - buff_size, 0);
    dbg ("lookup_size = %d\n", *t);

    dbg ("total: %d\n", buff_size);


    dl_free (a, sizeof (int) * nn);
    dl_free (b, sizeof (vptr) * nn);
    dl_free (c, sizeof (pair_int_vptr) * nn);

    header.user_index[u_id].shift = fCurr;
    header.user_index[u_id].size = buff_size;

    *(int *)buff = compute_crc32 (buff + sizeof (int), buff_size - sizeof (int));
    assert (write (fd[1], buff, buff_size) == buff_size);
    fCurr += header.user_index[u_id].size;

    assert (user_LRU_unload() != -1);
    user_unloaded_free (u);
  }

  if (dump_f != NULL) {
    fclose (dump_f);
  }

  int volumes_len = map_int_int_encode (&volumes, save_buffer, save_buffer_len);
  header.volumes_st = fCurr;
  header.volumes_len = volumes_len;
  assert (write (fd[1], save_buffer, volumes_len) == volumes_len);
  fCurr += volumes_len;

  header.total_photos = total_photos;

  dl_free (save_buffer, save_buffer_len);
  dl_free (extra_photos, extra_photos_size);

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
  assert (write (fd[1], header.user_index, sizeof (user_index_data) * (header.user_cnt + 1)) == (ssize_t)sizeof (user_index_data) * (header.user_cnt + 1));

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

int dump_unimported (void) {
  dbg ("dump_unimported\n");

  static int albums[MAX_RESULT];

  //for each user
  int u_id;
  for (u_id = 1; u_id <= cur_local_id; u_id++) {
    if (verbosity > 1) {
      fprintf (stderr, "u_id = %d\n", u_id);
    }
    user *u = &users[u_id];

    load_user_metafile (u, u_id, 1);
    assert (user_loaded (u));

    dbg ("dump_user %d (current_photo_id = %d) (current_album_id = %d)\n", u->id, u->current_photo_id, u->current_album_id);

    int owner_id = u->id;
    if (verbosity > 1) {
      fprintf (stderr, "user_id = %d\n", owner_id);
    }

    assert (conv_uid (owner_id) != NULL);

    int i, albums_n = user_get_albums (u, 0, MAX_RESULT, NULL);
    assert (albums_n != -1);
    for (i = 0; i < albums_n; i++) {
      albums[i] = event_get_album_id (&result_obj[i]);
    }

    char s[256];
    for (i = 0; i < albums_n; i++) {
      int j, photos_n = user_get_photos (u, albums[i], 0, MAX_RESULT, NULL);
      assert (photos_n != -1);
      for (j = 0; j < photos_n; j++) {
        int pid = event_get_photo_id (&result_obj[j]);

        int original;
        for (original = 0; original < 2; original++) {
          int len;
          char *loc = event_get_location (&result_obj[j], original, &len);
          if (loc != NULL && len > 1 && loc[0] < 0 && len <= -(loc[0] + mode) + 4 * sizeof (int) + 3) {
            len = -(loc[0] + mode);
            loc += 1;

            int sid, sid2, orig_oid, orig_aid;
            READ_INT(loc, sid);
            READ_INT(loc, sid2);
            READ_INT(loc, orig_oid);
            READ_INT(loc, orig_aid);

            if (sid2 != 0 && sid == 0) {
              sid = sid2;
            }

            if (dump_unimported_mode == 2 && orig_oid == owner_id && orig_aid == albums[i]) {
              continue;
            }

            assert (0 <= len && len < 255);
            memcpy (s, loc, len);
            s[len] = 0;

            printf ("%d\t%d\t%d\t%d\t%d\t%s%s\n", sid, owner_id, pid, orig_oid, orig_aid, s, original ? "?" : "");
          }
        }
      }
    }

    assert (user_LRU_unload() != -1);
    user_unloaded_free (u);
  }

  return 0;
}

int repair_binlog (void) {
  assert (0 && "volumes not supported");
  dbg ("repair_binlog\n");

  char new_binlog_name[256];
  if (snprintf (new_binlog_name, 256, "%s.bin", index_name) >= 256) {
    fprintf (stderr, "Too long index_name.");
    exit (1);
  }

  fd[1] = open (new_binlog_name, O_CREAT | O_TRUNC | O_WRONLY, 0660);

  if (fd[1] < 0) {
    fprintf (stderr, "cannot create new binlog file %s: %m\n", new_binlog_name);
    exit (1);
  }

  dl_zout new_binlog;
  dl_zout_init (&new_binlog, 1, 4 * 1048576);
  struct lev_start *E =
    dl_zout_alloc_log_event (&new_binlog, LEV_START, 6 * sizeof (int));
  E->schema_id = PHOTO_SCHEMA_V1;
  E->extra_bytes = 0;
  E->split_mod = log_split_mod;
  E->split_min = log_split_min;
  E->split_max = log_split_max;

  struct lev_timestamp *ET =
    dl_zout_alloc_log_event (&new_binlog, LEV_TIMESTAMP, 2 * sizeof (int));
  ET->timestamp = now;

  static int albums[MAX_RESULT];

  //for each user
  int u_id;
  for (u_id = 1; u_id <= cur_local_id; u_id++) {
    if (verbosity > 1) {
      fprintf (stderr, "u_id = %d\n", u_id);
    }
    user *u = &users[u_id];

    load_user_metafile (u, u_id, 1);
    assert (user_loaded (u));

    dbg ("repair_user %d (current_photo_id = %d) (current_album_id = %d)\n", u->id, u->current_photo_id, u->current_album_id);

    int owner_id = u->id;
    if (verbosity > 1) {
      fprintf (stderr, "user_id = %d\n", owner_id);
    }

    assert (conv_uid (owner_id) != NULL);

    int i, albums_n = user_get_albums (u, 0, MAX_RESULT, NULL);
    assert (albums_n != -1);
    for (i = 0; i < albums_n; i++) {
      albums[i] = event_get_album_id (&result_obj[i]);

      if (albums[i] > 0) {
        struct lev_photo_create_album_force *E =
          dl_zout_alloc_log_event (&new_binlog, LEV_PHOTO_CREATE_ALBUM_FORCE + (mode << 16), sizeof (struct lev_photo_create_album_force));

        E->user_id = owner_id;
        E->album_id = albums[i];

        event_to_binlog (&new_binlog, &result_obj[i], ALBUM_TYPE, LEV_PHOTO_CHANGE_ALBUM);
      }
    }

    for (i = 0; i < albums_n; i++) {
      int j, photos_n = user_get_photos (u, albums[i], 0, MAX_RESULT, NULL);
      assert (photos_n != -1);
      for (j = 0; j < photos_n; j++) {
        struct lev_photo_create_photo_force *E =
          dl_zout_alloc_log_event (&new_binlog, LEV_PHOTO_CREATE_PHOTO_FORCE + (mode << 16) + 1, sizeof (struct lev_photo_create_photo_force));

        E->user_id = owner_id;
        E->album_id = albums[i];
        E->photo_id = event_get_photo_id (&result_obj[j]);

        event_to_binlog (&new_binlog, &result_obj[j], PHOTO_TYPE, LEV_PHOTO_CHANGE_PHOTO);
      }
    }

    dl_zout_flush (&new_binlog);
    struct lev_crc32 *E =
      dl_zout_alloc_log_event (&new_binlog, LEV_CRC32, sizeof (struct lev_crc32));
    E->timestamp = now;
    E->pos = new_binlog.written;
    E->crc32 = ~new_binlog.crc32_complement;

    assert (user_LRU_unload() != -1);
    user_unloaded_free (u);
  }

  dl_zout_free (&new_binlog);

  assert (fsync (fd[1]) >= 0);
  assert (close (fd[1]) >= 0);
  fd[1] = -1;

  return 0;
}

void user_init (user *u) {
  u->id = 0;
  u->local_id = 0;
  u->metafile = NULL;
  u->metafile_len = -1;

  u->aio = NULL;
  u->next_used = NULL;
  u->prev_used = NULL;

  if (!write_only) {
    data_init (&u->d, &photo_func);
    lookup_init (&u->album_by_photo);
    map_int_vptr_init (&u->photos);
    set_int_init (&u->deleted_albums);
  }
}

int init_all (kfs_file_handle_t Index) {
  int i;

  if (verbosity > 1) {
    fprintf (stderr, "Init_all started\n");
  }

  log_ts_exact_interval = 1;

  map_int_int_init (&volumes);
  set_intp_init (&h_users);
  init_types();

  int f = load_header (Index);

  jump_log_ts = header.log_timestamp;
  jump_log_pos = header.log_pos1;
  jump_log_crc32 = header.log_pos1_crc32;

  user_cnt = index_users = header.user_cnt;
  total_photos = header.total_photos;

  if (user_cnt < 200000) {
    user_cnt = 200000;
  }

  assert (user_cnt >= 200000);
  user_cnt *= 1.1;
  //fix:
  //user_cnt = 20;

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

    if (!write_only && !index_mode) {
      int i;
      for (i = 1; i < user_cnt; i++) {
        user *u = &users[i];
        user_unloaded_free (u);
      }
    }
    dl_free (users, sizeof (user) * user_cnt);

    map_int_int_free (&volumes);
    set_intp_free (&h_users);
    free_header (&header);

    free_types();

    utils_free();

    fprintf (stderr, "Memory left: %lld\n", dl_get_memory_used());
    assert (dl_get_memory_used() == 0);
  }
}
