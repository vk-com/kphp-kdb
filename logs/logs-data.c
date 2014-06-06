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

#include <assert.h>
#include <float.h>
#include <math.h>
#include <stdarg.h>
#include <stdio.h>
#include <stddef.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#include "logs-data.h"
#include "../common/utf8_utils.h"
#include "../common/string-processing.h"
#include "dl.h"

#ifdef LLONG_MAX
#undef LLONG_MAX
#endif

#ifdef LLONG_MIN
#undef LLONG_MIN
#endif

#define LLONG_MAX 0x7FFFFFFFFFFFFFFFll
#define LLONG_MIN 0x8000000000000000ll

char debug_buff[1 << 24];
char *ds;
int debug_error;

enum {t_int = 0, t_long = 1, t_double = 2, t_string = 3};

//int std_t[FN + 4]     = {     0,      0,     0,                 1,    0, 0, 1, 2, 3}; // default
//char *field_names[FN] = {"time", "type", "uid", "some_long_field", NULL};

int std_t[FN + 4]     = {     0,      0,    1,    0, 0, 1, 2, 3};
char *field_names[FN] = {"time", "type", "id", NULL};

#define common_fields 0
char *common_field_names[common_fields] = {};
int   common_field_types[common_fields] = {};

//#define common_fields 2 // default
//char *common_field_names[common_fields] = {"ip_lo", "ip_hi"};
//int   common_field_types[common_fields] = { t_long,  t_long};
//two first always must be time and type

void debug (char const *msg, ...) {
  if (ds - debug_buff < 15 * (1 << 20)) {
    va_list args;

    va_start (args, msg);
    ds += vsprintf (ds, msg, args);
    va_end (args);
  } else {
    debug_error = 1;
  }
}

#define MAX_BUFF (1 << 27)
char buff[MAX_BUFF];

int index_mode;
int test_mode;
int dump_index_mode;
int my_verbosity;
long long max_memory = MAX_MEMORY;
long long query_memory = 500000000;
int header_size;
int binlog_readed;
int write_only;
int dump_mode = 0, dump_type = 0, from_ts = 0, to_ts = 2147483647;
long events_memory;
int mean_event_size = 108;

int jump_log_ts;
long long jump_log_pos;
unsigned int jump_log_crc32;

char *dump_query = NULL;
char *stat_queries_file = NULL;

index_header header;


int read_long (const char *s, long long *x, int *pos) {
  const char *start = s;

  int mul = 1;
  if (s[0] == '-' || s[0] == '+') {
    if (s[0] == '-') {
      mul = -1;
    }
    s++;
  }

  const char *num_start = s;
  unsigned long long val = 0;
  while ('0' <= *s && *s <= '9') {
    val = val * 10 + (*s++ - '0');
  }

  *x = val * mul;
  *pos = s - start;

  return s > num_start;
}

int read_long_only (const char *s, long long *x) {
  int mul = 1;
  if (s[0] == '-' || s[0] == '+') {
    if (s[0] == '-') {
      mul = -1;
    }
    s++;
  }

  const char *num_start = s;
  unsigned long long val = 0;
  while ('0' <= *s && *s <= '9') {
    val = val * 10 + (*s++ - '0');
  }

  *x = val * mul;

  return s > num_start;
}


int local_uid (int user_id) {
  user_id %= log_split_mod;
  if (user_id < 0) {
    user_id += log_split_mod;
  }

  if (user_id != log_split_min) {
    return -1;
  }

  return 1;
}


inline int is_letter (char x) {
  return ('a' <= x && x <= 'z') || ('A' <= x && x <= 'Z') || ('_' == x);
}

inline int is_digit (char x) {
  return (x >= '0' && x <= '9');
}

int is_name (char *s) {
  if (s == NULL || *s == 0 || strlen (s) >= MAX_NAME_LEN || !is_letter (*s)) {
    return 0;
  }
  s++;
  while (*s) {
    if (!is_letter (*s) && !is_digit (*s)) {
      return 0;
    }
    s++;
  }
  return 1;
}

#define MAX_EVENT_SIZE 32760

const char *ttt[] = {"int", "long", "double", "string"};
const int ttt_size[] = {4, 8, 8};

inline int get_type (char *b) {
  int i;
  for (i = 0; i < 4; i++) {
    if (strcmp (b, ttt[i]) == 0) {
      return i;
    }
  }
  int max_len, end = -1;
  if (sscanf (b, "string[%d]%n", &max_len, &end) == 1 && end != -1 && !b[end] && 1 <= max_len && max_len < MAX_EVENT_SIZE) {
    return t_string + max_len;
  }

  return -1;
}

#pragma pack(push, 4)

typedef struct {
  union {
    struct {
      int timestamp;
      int type;
    };
    int std_val[FN];
  };
  int q_prev[FN];
  long long mask;
  short data_len;
  short sn;
  char data[0];
} event;

typedef struct {
  short type;

  short shift;

  int max_len;

  char *name;
} field_desc;

typedef struct {
  int field_i, field_n;
  int shifts_len;
  char *name;
  field_desc *fields;
} type_desc;

#pragma pack(pop)


#define MAX_COLOR_CNT 65537


map_int_int color_int[FN];
map_ll_int color_ll[FN];

long get_colors_memory (void) {
  long res = 0;
  int i;
  for (i = 0; i < FN; i += 1 + std_t[i]) {
    if (std_t[i]) {
      res += map_ll_int_get_memory_used (&color_ll[i]);
    } else {
      res += map_int_int_get_memory_used (&color_int[i]);
    }
  }
  return res;
}

int get_color (int field_num, long long field_value) {
  assert (0 <= field_num && field_num < FN);

  if (std_t[field_num]) {
    int *t = map_ll_int_get (&color_ll[field_num], field_value);
    if (t != NULL) {
      return *t;
    }
  } else {
    int *t = map_int_int_get (&color_int[field_num], field_value);
    if (t != NULL) {
      return *t;
    }
  }

  return 0;
}

int set_color (struct lev_logs_set_color *E) {
  if (!write_only || index_mode) {
    int field_num = E->type - LEV_LOGS_SET_COLOR;
    long long field_value = E->field_value;
    int cnt = E->cnt;
    int i;

    assert (0 <= field_num && field_num < FN);

    for (i = 0; i < cnt; i++) {
      int new_color = (get_color (field_num, field_value) & E->and_mask) ^ E->xor_mask;
      if (new_color == 0) {
        if (std_t[field_num]) {
          map_ll_int_del (&color_ll[field_num], field_value + i);
        } else {
          map_int_int_del (&color_int[field_num], field_value + i);
        }
      } else {
        if (std_t[field_num]) {
          *map_ll_int_add (&color_ll[field_num], field_value + i) = new_color;
        } else {
          *map_int_int_add (&color_int[field_num], field_value + i) = new_color;
        }
      }
    }
  }

  return 1;
}


long long type_size[MAX_TYPE];
type_desc types[MAX_TYPE];
map_ll_int map_type_id;

long long MAX_EV, MAX_EVENT_MEM;
char dump_event_buf[MAX_EVENT_SIZE * 2 + 100];

event **eq;
int eq_l = 1, eq_r = 1;
int eq_n, eq_total;

map_int_int q_st_int[FN];
map_ll_int q_st_ll[FN];

long get_q_st_memory (void) {
  long res = 0;
  int i;
  for (i = 0; i < FN; i++) {
    if (std_t[i]) {
      res += map_ll_int_get_memory_used (&q_st_ll[i]);
    } else {
      res += map_int_int_get_memory_used (&q_st_int[i]);
    }
  }
  return res;
}

int get_time (void) {
  if (eq_n) {
    return eq[eq_l]->std_val[0];
  }
  return now;
}

#define QINC(v) if (++v == MAX_EV) {v = 1;}
#define QDEC(v) if (--v == 0) {v = MAX_EV - 1;}

inline int get_event_size (event *e) {
  return offsetof (event, data) + e->data_len;
}

inline int get_q_prev (int i, int qid) {
  if (qid == -1) {
    if (i == eq_l) {
      return -1;
    }
    QDEC (i);
    return i;
  }

  event *eqi = eq[i];
  assert (eqi != NULL);
  int ni = eqi->q_prev[qid];
  event *eqni = eq[ni];

  //TODO why we need to compare std_val?
  if (eqni != NULL && eqi->timestamp >= eqni->timestamp - 3601) {
    if (std_t[qid] == 0 && eqi->std_val[qid] == eqni->std_val[qid]) {
      return ni;
    }
    if (std_t[qid] == 1 && *(long long *)&eqi->std_val[qid] == *(long long *)&eqni->std_val[qid]) {
      return ni;
    }
  }
  return -1;
}

inline int get_q_st (int qid, long long val) {
  if (qid == -1 && eq_n) {
    int t = eq_r;
    QDEC(t);
    return t;
  }

  if (0 <= qid && qid < FN) {
    if (std_t[qid] == 0) {
      int *t = map_int_int_get (&q_st_int[qid], val);
      if (t != NULL && eq[*t] != NULL && eq[*t]->std_val[qid] == val) {
        return *t;
      }
    } else {
      int *t = map_ll_int_get (&q_st_ll[qid], val);
      if (t != NULL && eq[*t] != NULL && (*(long long *)&eq[*t]->std_val[qid]) == val) {
        return *t;
      }
    }
  }

  return -1;
}

void upd_q_st_add (event *e, int id) {
  int i;
  for (i = 1; i < FN; i += 1 + std_t[i]) {
    if (std_t[i]) {
      int *t = map_ll_int_add (&q_st_ll[i], *(long long *)&e->std_val[i]);
      e->q_prev[i] = *t;
      *t = id;
    } else {
      int *t = map_int_int_add (&q_st_int[i], e->std_val[i]);
      e->q_prev[i] = *t;
      *t = id;
    }
  }
}

void upd_q_st_delete (event *e, int id) {
  int i;
  for (i = 1; i < FN; i += 1 + std_t[i]) {
    if (std_t[i]) {
      int *tmp = map_ll_int_get (&q_st_ll[i], *(long long *)&e->std_val[i]);

      if (tmp != NULL && *tmp == id) {
        map_ll_int_del (&q_st_ll[i], *(long long *)&e->std_val[i]);
      }
    } else {
      int *tmp = map_int_int_get (&q_st_int[i], e->std_val[i]);

      if (tmp != NULL && *tmp == id) {
        map_int_int_del (&q_st_int[i], e->std_val[i]);
      }
    }
  }
}

char *events_mem;
long evm_l = 0, evm_r = 0, evm_mx;

void event_free (void) {
  assert (eq_n);
  if (*(int *)(events_mem + evm_l) == -1) {
    evm_l = 0;
  }
  event *e = (event *)(events_mem + evm_l);

  assert (eq[eq_l] == e);
  upd_q_st_delete (e, eq_l);
  eq_n--;
  eq[eq_l] = NULL;
  QINC(eq_l);
  type_size[e->type] -= get_event_size (e);
  events_memory -= get_event_size (e);
  evm_l += get_event_size (e);
}

event *event_malloc (int data_len) {
  eq_total++;

  int len = offsetof (event, data) + data_len;
  events_memory += len;
  assert (len + len < evm_mx);

  if (evm_r + len >= evm_mx) {
    while (evm_r <= evm_l) {
      event_free();
    }

    *(int *)(events_mem + evm_r) = -1;
    evm_r = 0;
  }

  while ((evm_r <= evm_l && evm_r + len >= evm_l - 1 && eq_n) || eq_n >= MAX_EV - 5) {
    event_free();
  }

  event *e = (event *)(events_mem + evm_r);
  evm_r += len;

  e->timestamp = now;
  e->data_len = data_len;

  eq[eq_r] = e;
  QINC(eq_r);
  eq_n++;

  return e;
}

int use_default = 0;
int default_long = 0;
char *default_string = "";

#define GET_INT(e, f) ((int *)GET_FIELD (e, f))
#define GET_LONG(e, f) ((long long *)GET_FIELD (e, f))
#define GET_DOUBLE(e, f) ((double *)GET_FIELD (e, f))

// description of standard fields
field_desc std_desc[FN], empty_desc[FN];

inline char *GET_FIELD (event *e, field_desc *f) {
  if (f->type < 0) {
    return (char *)&e->std_val[f->type + FN];
  }

  if (unlikely(use_default)) {
    if (f->shift >= e->sn || e->data[f->shift] == -1) {
      return (char *)&default_long;
    }
  }

//  assert (f->shift < e->sn);
//    assert (e->data[f->shift] != -1);
  return &e->data[e->sn + (int)(unsigned char)e->data[f->shift]];
}

inline char *GET_STRING (event *e, field_desc *f) {
  if (unlikely(use_default)) {
    if (f->shift >= e->sn || *(short *)&e->data[f->shift] == -1) {
      return default_string;
    }
  }

//  assert (f->shift < e->sn);
//    assert ((int)*(short *)&e->data[f->shift] >= 0);
  return &e->data[e->sn + (int)*(short *)&e->data[f->shift]];
}

char *get_type_size (int type) {
  int first, last;
  if (type == -1) {
    first = 1;
    last = MAX_TYPE;
  } else if (1 <= type && type < MAX_TYPE) {
    first = type;
    last = type + 1;
  } else {
    return "";
  }

  ds = debug_buff;

  int i;
  for (i = first; i < last; i++) {
    if (types[i].name != NULL) {
      debug ("[%40s](%5d) : %9lld\n", types[i].name, i, type_size[i]);
    }
  }

  *ds = 0;
  return debug_buff;
}

char *get_types (void) {
  ds = debug_buff;

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

  *ds = 0;
  return debug_buff;
}

int write_str (char *d, int mx_len, char *s) {
  int tmp = mx_len;
  while (*s) {
#define C(x, y) case x: if (likely(--mx_len > 0)) *d++ = '\\'; if (likely(--mx_len > 0)) *d++ = y; break;
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
        if (likely(--mx_len > 0)) *d++ = *s;
    }
#undef C
    s++;
  }
  *d = 0;
  return tmp - mx_len;
}

inline int has_field (event *e, int id) {
  return use_default || ((e->mask >> id) & 1);
}


int get_field_id (type_desc *t, char *s) {
  int i;
  for (i = -FN; i < t->field_i; i += 1 + (i < 0 && std_t[i + FN])) {
    if (!strcmp (t->fields[i].name, s)) {
      return i;
    }
  }

  return -FN - 1;
}

void type_free (type_desc *t) {
  dl_strfree (t->name);
  int j;
  for (j = 0; j < t->field_i; j++) {
    dl_strfree (t->fields[j].name);
  }
  dl_free (t->fields - FN, sizeof (field_desc) * (t->field_n + FN));
}

void add_field (type_desc *desc, char *name, int type) {
  assert (type >= 0);
  desc->field_i += FN;
  desc->field_n += FN;
  desc->fields -= FN;
  if (desc->field_i == desc->field_n) {
    int nn = desc->field_n + 1;
    if (nn == 0) {
      nn = 1;
    }
    desc->fields = dl_realloc0 (desc->fields, sizeof (field_desc) * nn, sizeof (field_desc) * desc->field_n);
    desc->field_n = nn;
  } else {
    assert (0);
  }
  desc->field_i -= FN;
  desc->field_n -= FN;
  desc->fields += FN;

  int i = desc->field_i++;
  if (type > t_string) {
    int max_len = type - t_string;
    assert (0 < max_len && max_len < MAX_EVENT_SIZE);
    desc->fields[i].max_len = max_len;
    type = t_string;
  } else {
    desc->fields[i].max_len = 0;
  }
  desc->fields[i].type = type;
  desc->fields[i].name = dl_strdup (name);

  desc->fields[i].shift = desc->shifts_len;
  desc->shifts_len += type == t_string ? 2 : 1;
}

int get_type_id (char *s) {
  int type_id;
  int sn = strlen (s);
  if (s[0] >= '1' && s[0] <= '9') {
    int len;
    sscanf (s, "%d%n", &type_id, &len);
    if (len != sn) {
      return 0;
    }

    if (!(1 <= type_id && type_id < MAX_TYPE)) {
      return 0;
    }

    if (types[type_id].name == NULL) {
      return 0;
    }
  } else {
    if (!is_name (s)) {
      return 0;
    }

    int *ptmp = map_ll_int_get (&map_type_id, dl_strhash (s));
    if (ptmp == NULL) {
      return 0;
    }

    type_id = *ptmp;
  }
  return type_id;
}

char *add_field_log (struct lev_logs_add_field *E) {
  strcpy (buff, E->text);
  int i = 0;

  while (buff[i] != ',' && buff[i]) {
    i++;
  }

  if (buff[i] != ',') {
    return dl_pstr ("Wrong number of parameters.");
  }

  buff[i] = 0;

  int type_id;

  if (buff[0] >= '1' && buff[0] <= '9') {
    int len;
    sscanf (buff, "%d%n", &type_id, &len);
    if (len != i) {
      return dl_pstr ("Not a valid name [%s] for type_id.", buff);
    }

    if (!(1 <= type_id && type_id < MAX_TYPE)) {
      return dl_pstr ("Type_id [%d] not in range [1;%d].", type_id, MAX_TYPE - 1);
    }

    if (types[type_id].name == NULL) {
      return dl_pstr ("Type [%d] doesn't exist. You need to create it first.", type_id);
    }
  } else {
    if (!is_name (buff)) {
      return dl_pstr ("Not a valid name [%s] for type_id.", buff);
    }

    int *ptmp = map_ll_int_get (&map_type_id, dl_strhash (buff));
    if (ptmp == NULL) {
      return dl_pstr ("There is no type named [%s].", buff);
    }

    type_id = *ptmp;
  }

  int j = i + 1;
  while (buff[j] != ':' && buff[j]) {
    j++;
  }

  if (buff[j] != ':') {
    return dl_pstr ("Not enough parameters in field description.");
  }
  buff[j] = 0;

  int type = get_type (buff + j + 1);
  if (type == -1) {
    return dl_pstr ("Wrond type of new field [%s].", buff + j + 1);
  }
  char *name = buff + i + 1;

  if (!is_name (name)) {
    return dl_pstr ("Not a valid name [%s] for field_name.", name);
  }

  if (type < t_string) {
    int size = ttt_size[type];
    for (i = 0; i < types[type_id].field_i; i++) {
      if (types[type_id].fields[i].type < t_string) {
        size += ttt_size[types[type_id].fields[i].type];
      }
    }

    if (size >= 248) {
      return dl_pstr ("Max size (248) of nunber fields of type [%s] exceeded. Contact soft developers for help.", types[type_id].name);
    }
  }

  if (types[type_id].field_i >= 64) {
    return dl_pstr ("Max number (64) of fields for type [%s] already created. Contact soft developers for help.", buff);
  }

  for (i = 0; i < types[type_id].field_i; i++) {
    if (strcmp (types[type_id].fields[i].name, name) == 0) {
      return dl_pstr ("Field with name [%s] already exists in type [%s].", name, buff);
    }
  }

  add_field (&types[type_id], name, type);
  return "OK";
}

char *create_type_buf (char *buff, int text_len) {
  int commas[2], cn = 0, i, j;
  char *name, *val, *desc;

  for (i = 0; i < text_len && cn < 2; i++) {
    if (buff[i] == ',') {
      commas[cn++] = i;
      buff[i] = 0;
    }
  }
  assert (cn == 2);

  name = buff;
  val = buff + commas[0] + 1;
  desc = buff + commas[1] + 1;
  buff[text_len + 1] = 0;

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

  long long hash = dl_strhash (name);
  int *ptmp = map_ll_int_get (&map_type_id, hash);
  if (ptmp != NULL) {
    return dl_pstr ("Type [%s] already exists and has type_id [%d].", name, *ptmp);
  }

  char *fnames[64];
  int ftypes[64];
  int fn;
  int size = 0;

  for (fn = 0; fn < common_fields; fn++)
  {
    fnames[fn] = common_field_names[fn];
    ftypes[fn] = common_field_types[fn];

    size += ttt_size[ftypes[fn]];
  }

  i = 0;
  while (desc[i]) {
    j = i;
    while (desc[j] != ':' && desc[j]) {
      j++;
    }
    if (!desc[j]) {
      return dl_pstr ("Can't parse type_description �%d (':' between field_name and field_type not found)", fn + 1);
    }

    int k = j + 1;
    while (desc[k] != ';' && desc[k]) {
      k++;
    }

    desc[j] = 0;
    desc[k] = 0;

    char *a = desc + i, *b = desc + j + 1;

    if (!is_name (a)) {
      return dl_pstr ("Not a valid name [%s] for field_name.", a);
    }

    int type = get_type (b);
    if (type == -1) {
      return dl_pstr ("Not a valid type [%s] for field_type.", a);
    }

    if (type < t_string) {
      size += ttt_size[type];
    }

    if (size >= 248) {
      return dl_pstr ("Max size (248) of nunber fields of type [%s] exceeded. Contact soft developers for help.", name);
    }

    if (fn >= 64) {
      return dl_pstr ("Max number (64) of fields for type [%s] exceeded. Contact soft developers for help.", name);
    }

    fnames[fn] = a;
    ftypes[fn++] = type;

    i = k + 1;
  }

  for (i = 0; i < fn; i++) {
    for (j = i + 1; j < fn; j++) {
      if (strcmp (fnames[i], fnames[j]) == 0) {
        return dl_pstr ("The same field_name [%s] used for fields �%d and �%d.", fnames[i], i, j);
      }
    }
  }

  *map_ll_int_add (&map_type_id, hash) = type_id;
  types[type_id].name = dl_strdup (name);

  types[type_id].fields = dl_malloc (sizeof (field_desc) * FN);
  for (i = 0; i < FN; i += 1 + std_t[i]) {
    types[type_id].fields[i] = std_desc[i];
//    fprintf (stderr, "%d %d %p\n", type_id, i, std_desc[i].name);
  }
  types[type_id].fields += FN;
  types[type_id].shifts_len = 0;
  types[type_id].field_i = types[type_id].field_n = 0;

  for (i = 0; i < fn; i++) {
    add_field (&types[type_id], fnames[i], ftypes[i]);
  }

  return "OK";
}

char *create_type (struct lev_logs_create_type *E) {
  strcpy (buff, E->text);
  return create_type_buf (buff, E->text_len);
}

struct token_t;
struct node_t;

typedef struct {
  char *s;
  struct token_t *token_list;
  struct node_t *root;
} expression;

expression dump_expr;

#define MAX_STATS 1000

int stat_type[MAX_STATS];
expression stat_expr[MAX_STATS];
int stats_cnt;
long long stat_result[MAX_STATS];

inline int check_query (expression *expr, event *e);


int add_event (struct lev_logs_add_event *E) {
  static int max_now = -1;

  int type_id = E->std_val[0];
  if (types[type_id].name == NULL) {
    return 0;
  }

  strcpy (buff, E->text);

  static const char delim = 1;

  char *desc = buff;

  int id[64], tp[64], valn[64], val_realn[64], fn = 0, size = 0;
  char *val[64];

  int i = 0, j, k;
  int dn = strlen (desc);
  desc[dn + 1] = 0;
  if (verbosity > 3 && binlog_readed) {
    fprintf (stderr, "desc = \"%s\"\n", desc);
  }

  while (desc[i]) {
    if (fn == 64) {
      return 0;
    }

    j = i;
    while (desc[j] != ':' && desc[j]) {
      j++;
    }
    if (desc[j] != ':') {
      return 0;
    }

    desc[j] = 0;

    int fid = get_field_id (&types[type_id], desc + i);

    if (fid < 0) {
      return 0;
    }

    id[fn] = fid;

    k = ++j;
    while (desc[k] != delim && desc[k]) {
      k++;
    }
    desc[k] = 0;

    tp[fn] = types[type_id].fields[fid].type;
    if (types[type_id].fields[fid].max_len) {
      valn[fn] = types[type_id].fields[fid].max_len;
    } else {
      valn[fn] = k - j;
    }
    val_realn[fn] = k - j;
    val[fn++] = desc + j;

    if (verbosity > 2) {
      fprintf (stderr, "  Found field %s of type %d number %d with value %s of length %d and maximal length of %d.\n",
        desc + i, tp[fn - 1], fid, desc + j, val_realn[fn - 1], valn[fn - 1]);
    }

    i = k + 1;
  }

  int need_dump = 1;
  static char tval[64][10];
  for (i = 0; i < fn; i++) {
    int nn;
    switch (tp[i]) {
    case t_int:
      if (sscanf (val[i], "%d%n", (int *)tval[i], &nn) != 1 || nn != valn[i]) {
        return 0;
      }
      size += 4;
      break;
    case t_long:
      if (read_long (val[i], (long long *)tval[i], &nn) != 1 || nn != valn[i]) {
        return 0;
      }
      size += 8;
      break;
    case t_double:
      if (sscanf (val[i], "%lf%n", (double *)tval[i], &nn) != 1 || nn != valn[i]) {
        return 0;
      }
      size += 8;
      break;
    case t_string:
      size += valn[i] + 1;
      break;
    }
  }

  size += types[type_id].shifts_len;

  pair_p_int v[64];
  for (i = 0; i < fn; i++) {
    v[i].y = i;
    v[i].x.x = tp[i] < t_string ? 0 : tp[i];
    v[i].x.y = id[i];
  }
  qsort (v, fn, sizeof (pair_p_int), DL_QCMP(pair_p_int));
  for (j = 1; j < fn; j++) {
    if (id[v[j].y] == id[v[j - 1].y]) {
      return 0;
    }
  }


  int tid = eq_r;
  event *e = NULL;
  if ((dump_mode && (dump_mode == 2 || dump_query != NULL)) || !write_only) {
    if (dump_mode) {
      e = (event *)dump_event_buf;
    } else {
      e = event_malloc (size);
      type_size[type_id] += offsetof (event, data) + size;
    }

    for (j = 1; j < FN; j++) {
      e->std_val[j] = E->std_val[j - 1];
    }

    if (now > max_now) {
      max_now = now;
    }
    assert (max_now != -1);
    e->std_val[0] = max_now;

    e->mask = 0;
    e->sn = types[type_id].shifts_len;
    memset (e->data, -1, e->sn);
    char *s = e->data + e->sn, *st = s;

    for (j = 0; j < fn; j++) {
      int shift = types[type_id].fields[v[j].x.y].shift;
      i = v[j].y;

      e->mask |= 1ll << id[i];

      switch (tp[i]) {
      case t_int:
        e->data[shift] = s - st;
        WRITE_INT (s, *(int *)tval[i]);
        break;
      case t_double:
      case t_long:
        e->data[shift] = s - st;
        WRITE_LONG (s, *(long long *)tval[i]);
        break;
      case t_string:
        *(short *)&e->data[shift] = s - st;
        for (k = 0; k < val_realn[i] && k < valn[i]; k++) {
          *s++ = val[i][k];
        }
        *s++ = 0;
      }
    }
  }

  if (dump_mode) {
    if (dump_mode == 1 && now >= from_ts && now <= to_ts && need_dump && dump_type * (type_id - dump_type) == 0 && (dump_query == NULL || check_query (&dump_expr, e))) {
      int size = offsetof (struct lev_logs_add_event, text) + 1 + E->text_len;
      E->type = size - sizeof (int);
      assert (fwrite (&now, sizeof (int), 1, stdout) == 1);
      assert (fwrite (E, size, 1, stdout) == 1);
      E->type = LEV_LOGS_ADD_EVENT;
    }

    if (dump_mode == 2 && now >= from_ts && now <= to_ts) {
      int i;
      for (i = 0; i < stats_cnt; i++) {
        if (stat_type[i] == type_id && check_query (&stat_expr[i], e)) {
          stat_result[i]++;
        }
      }
    }

    if (now > to_ts) {
      if (dump_mode == 2) {
        print_stats();
      }

      exit (0);
    }
  }

  if (write_only) {
    return 1;
  }

  upd_q_st_add (e, tid);

  return 1;
}

/*
 *
 *  CALCULATOR=)
 *
 */

type_desc *_cur_type;
event *_cur_event;

typedef enum {t_arif, t_bin, t_logic, t_cmp, t_conv, t_str} op_t;
typedef enum            { op_now = 0, op_usub, op_comp,  op_not, op_sort,    op_upp,    op_low,   op_deunicode,   op_simpl,       op_fsimpl, op_type_to_id, op_length, op_color, op_bit_count, op_int, op_long, op_double, op_string,  op_in, op_mul, op_div, op_rem, op_add, op_sub, op_shl, op_shr,  op_l,  op_m, op_le, op_me, op_eq, op_neq, op_band, op_xor, op_bor,  op_and,   op_or, op_rb, op_lb, op_seg, op_conv, t_var, t_fid, end_token} token_type;
static int priority[] = {          0,       0,       0,       0,       0,         0,         0,              0,          0,               0,             0,         0,        0,            0,      0,       0,         0,         0,      1,      3,      3,      3,      6,      6,      9,      9,    12,    12,    12,    12,    15,     15,      18,     21,     24,      27,      30,    33,    36};
char *desc[] =          {      "now",    "--",     "~",     "!",  "sort", "toupper", "tolower",    "deunicode", "simplify", "full_simplify",  "type_to_id",  "length",  "color",  "bit_count",  "int",  "long",  "double",  "string",   "in",    "*",    "/",    "%",    "+",     "-",  "<<",   ">>",   "<",   ">",  "<=",  ">=",   "=",   "!=",     "&",    "^",    "|",    "&&",    "||",   ")",   "(",  "seg",  "conv", "var", "tfid"};
op_t op_type[] =        {     t_conv,  t_arif,   t_bin, t_logic,   t_str,     t_str,     t_str,          t_str,      t_str,           t_str,        t_conv,    t_conv,   t_conv,       t_conv, t_conv,  t_conv,    t_conv,    t_conv, t_conv, t_arif, t_arif, t_arif, t_arif,  t_arif, t_bin,  t_bin, t_cmp, t_cmp, t_cmp, t_cmp, t_cmp,  t_cmp,   t_bin,  t_bin,  t_bin, t_logic, t_logic,    -1,    -1,  t_cmp,  t_conv};
typedef enum {v_int, v_long, v_double, v_string, v_cstring, v_fid, v_err} val_type;

//TODO regexp
//arseny30: Ha-ha!

#define IS_OP(x) ((x) < t_var)
#define IS_CONST(x) ((x) <= op_now)
#define IS_PREFIX_UN(x) ((x) <= op_string && (x) > op_now)
#define IS_UN(x) ((x) <= op_in && (x) > op_now)
#define IS_STR(x) ((x) == v_string || (x) == v_cstring)

typedef struct token_t token;
struct token_t {
  token_type type;

  val_type dtype;

  int v_fid;

  union {
    int v_int, v_INT;
    double v_double, v_DOUBLE;
    long long v_long, v_LONG;
    char *v_string, *v_cstring;
  };

  int param_int;
  set_ll param_set;
};

typedef struct node_t node;
struct node_t {
  token tok;

  int is_const;

  struct node_t **v;
  int vn;

  int *kmp;
  int kn;
};

token *get_token_list (char *s) {
  int sn = strlen (s) + 2;
  token *buff = dl_malloc (sn * sizeof (token));
  int bn = 0;

#define FAIL \
             do {                                          \
               int i;                                      \
               for (i = 0; i < bn; i++) {                  \
                 if (buff[i].type == t_var &&              \
                     buff[i].dtype == v_string) {          \
                   dl_strfree (buff[i].v_string);          \
                 }                                         \
                 if (buff[i].type == op_in) {              \
                   set_ll_free (&buff[i].param_set);       \
                 }                                         \
               }                                           \
               dl_free (buff, sn * sizeof (token));        \
               return NULL;                                \
             } while (0)

#define U(x) {buff[bn++].type = x; break;}
#define C(y, x) {if (s[1] == y) {buff[bn++].type = x; s++; break;}}
#define V(x, y) {buff[bn].type = t_var, buff[bn].dtype = x, buff[bn++].x = y; break;}
#define F(x, y) {int z = strlen (x); if (!strncmp (s, x, z)) {buff[bn++].type = y; s += z - 1; break;}}
  int f = 1;
  while (*s) {
    if (verbosity > 1) {
      fprintf (stderr, "get token list %s\n", s);
    }
    char *st, c;
    long long x;
    int len, val;
    int nf = 1, tf;
    double dst, den, dv;
    switch (*s) {
      case '~': if (f) {U (op_comp);} FAIL;
      case '!': C ('=', op_neq); if (f) {U (op_not);} FAIL;
      case '*': U (op_mul);
      case '/': U (op_div);
      case '%': U (op_rem);
      case '+': U (op_add);
      case '-':
        if (f) {
          U (op_usub);
        } else {
          U (op_sub);
        }
      case '=': C ('=', op_eq); U (op_eq);
      case '<': C ('<', op_shl); C ('>', op_neq); C ('=', op_le); U (op_l);
      case '>': C ('>', op_shr); C ('=', op_me); U (op_m);
      case '&': C ('&', op_and); U (op_band);
      case '^': U (op_xor);
      case '|': C ('|', op_or); U (op_bor);
      case '(': U (op_lb);
      case ')': nf = 0; U (op_rb);
      case '@':
        s++;
        F ("sort"          , op_sort      );
        F ("toupper"       , op_upp       );
        F ("tolower"       , op_low       );
        F ("deunicode"     , op_deunicode );
        F ("simplify"      , op_simpl     );
        F ("full_simplify" , op_fsimpl    );
        F ("bit_count"     , op_bit_count );
        F ("int"           , op_int       );
        F ("long"          , op_long      );
        F ("double"        , op_double    );
        F ("string"        , op_string    );
        F ("length"        , op_length    );
        F ("type_to_id"    , op_type_to_id);
        F ("now"           , op_now       );
        if (!strncmp (s, "color", 5) && '0' <= s[5] && s[5] <= '9') {
          int field_num = s[5] - '0', i = 6;
          while ('0' <= s[i] && s[i] <= '9' && i < 14) {
            field_num = field_num * 10 + s[i++] - '0';
          }
          if (0 <= field_num && field_num < FN) {
            buff[bn].param_int = field_num;
            buff[bn++].type = op_color;
            s += i - 1;
            break;
          }
        }
        FAIL;
        break;
      case '0' ... '9':
        x = 0;
        len = 0;
        while ('0' <= *s && *s <= '9') {
          x = x * 10 + *s++ - '0';
          len++;
        }
        s--;
        nf = 0;
        if (INT_MIN <= x && x <= INT_MAX && len <= 10) {
          V (v_int, x);
        } else {
          V (v_long, x);
        }
      case '.':
        dst = den = 0;
        dv = 1;
        tf = 0;

        if (bn && buff[bn - 1].type == t_var && (buff[bn - 1].dtype == v_int || buff[bn - 1].dtype == v_long)) {
          bn--;
          if (buff[bn].dtype == v_int) {
            dst = buff[bn].v_int;
          } else {
            dst = buff[bn].v_long;
          }
          tf = 1;
        }
        s++;
        len = 0;
        while ('0' <= *s && *s <= '9') {
          dv *= 10;
          den += (*s++ - '0') / dv;
          len++;
        }
        s--;
        if (len == 0 && !tf) {
          //fprintf (stderr, "case '.'\n");
          FAIL;
        }
        V(v_double, dst + den);
      case '\'':
        st = ++s;
        while (*s && *s != '\'') {
          s++;
        }
        if (*s == 0) {
          //fprintf (stderr, "case ' : no ' : %s\n", st);
          FAIL;
        }
        *s = 0;
        st = dl_strdup (st);
        *s = '\'';
        nf = 0;
        V (v_string, st);
        break;
      case 'a' ... 'z':
      case 'A' ... 'Z':
      case '_':
        st = s;
        while (is_letter (*s) || is_digit (*s)) {
          s++;
        }
        c = *s;
        *s = 0;

        if (!strcmp (st, "IN")) {
          *s = c;
          while (*s == ' ') {
            s++;
          }
          if (*s != '(') {
            FAIL;
          }

          buff[bn].type = op_in;
          set_ll_init (&buff[bn].param_set);

          do {
            do {
              s++;
            } while (*s == ' ');
            if (*s == ')') {
              break;
            }

            int sign = 1;
            if (*s == '+') {
              s++;
            } else if (*s == '-') {
              sign = -1;
              s++;
            }
            x = 0;
            while ('0' <= *s && *s <= '9') {
              x = x * 10 + *s++ - '0';
            }

            while (*s == ' ') {
              s++;
            }

            set_ll_add (&buff[bn].param_set, x * sign);
          } while (*s == ',');

          if (*s != ')') {
            FAIL;
          }

          bn++;
          break;
        }

        val = get_field_id (_cur_type, st);
        *s = c;
        if (val < -FN) {
          //fprintf (stderr, "case alph: %s\n", st);
          FAIL;
        }
        s--;
        nf = 0;
        V (v_fid, val);
        break;
      case ' ':
        nf = f;
        break;
      default:
        //fprintf (stderr, "can't find %c (%s)\n", *s, s);
        FAIL;
    }
    s++;
    f = nf;
  }

#undef U
#undef C
#undef V
#undef F
#undef FAIL
  buff[bn++].type = end_token;
  dl_realloc (buff, bn * sizeof (token), sn * sizeof (token));

  return buff;
}

char token_buff[MAX_EVENT_SIZE + 1];

// DEPRECATED
char *token_to_str (token *tok) {
  char *s = token_buff;

  s += sprintf (s, "[%s|%d] ", desc[tok->type], tok->dtype);
  switch (tok->dtype) {
    case v_int:
      s += sprintf (s, "%s", "v_int"); break;
    case v_double:
      s += sprintf (s, "%s", "v_double"); break;
    case v_long:
      s += sprintf (s, "%s", "v_long"); break;
    case v_string:
      s += sprintf (s, "%s", "v_string"); break;
    case v_cstring:
      s += sprintf (s, "%s", "v_cstring"); break;
    case v_fid:
      s += sprintf (s, "%s", "v_fid"); break;
    default:
      break;
  }

#define F(d, t) case t: s += sprintf (s, d, tok->t); break
  if (tok->type == t_var || tok->type == t_fid) {
    switch (tok->dtype) {
      F("%d", v_int);
      F("%lf", v_double);
      F("%lld", v_long);
      F("%s", v_string);
      F("%s", v_cstring);
      F("%d", v_fid);
      default:
        assert (0);
    }
  }
  return token_buff;
}

char *token_val_to_str (token *tok) {
  char *s = token_buff;

  switch (tok->dtype) {
    F("%d", v_int);
    F("%lf", v_double);
    F("%lld", v_long);
    F("%s", v_string);
    F("%s", v_cstring);
    default:
      assert (0);
  }
#undef F
  return token_buff;
}

inline node *new_node (void) {
  return dl_malloc0 (sizeof (node));
}

void del_node (node *v) {
  if (v == NULL) {
    return;
  }
  int i = 0;
  for (i = 0; i < v->vn; i++) {
    del_node (v->v[i]);
    v->v[i] = NULL;
  }

  dl_free (v->v, sizeof (node *) * v->vn);
  v->v = NULL;

  if (v->kmp != NULL) {
    dl_free (v->kmp, sizeof (int) * v->kn);
    v->kmp = NULL;
    v->kn = 0;
  }

  if (v->tok.type == t_var && v->tok.dtype == v_string) {
    if (verbosity > 3) {
      fprintf (stderr, "free string [%s]\n", v->tok.v_string);
    }
    dl_strfree (v->tok.v_string);
    if (verbosity > 3) {
      fprintf (stderr, "ok\n");
    }
  }

  dl_free (v, sizeof (node));
}

int upd (node **s1, token *s2, int *_s1n, int *_s2n, int p) {
#define s1n (*_s1n)
#define s2n (*_s2n)
  while (s2n && priority[s2[s2n - 1].type] <= p) {
    node *v = new_node();
    v->tok.type = s2[--s2n].type;
    v->tok.param_int = s2[s2n].param_int;
    v->tok.param_set = s2[s2n].param_set;
    if (v->tok.type == op_rb || v->tok.type == op_lb) {
      del_node (v);
      return 0;
    }
    if (IS_CONST(v->tok.type)) {
      v->vn = 0;
    } else if (IS_UN(v->tok.type)) { // unary
      v->vn = 1;
    } else {
      v->vn = 2;
    }
    v->v = dl_malloc0 (sizeof (node *) * v->vn);
    if (s1n < v->vn) {
      del_node (v);
      return 0;
    }
    s1n -= v->vn;
    int i;
    for (i = 0; i < v->vn; i++) {
      v->v[i] = s1[s1n + i];
    }
    s1[s1n++] = v;
  }
#undef s1n
#undef s2n

  return 1;
}

node *gen_tree (token *v) {
  if (v == NULL) {
    return NULL;
  }

  token *t = v;
  while (t->type != end_token) {
    t++;
  }
  int n = t - v, i;

  int tsize = n;

  node **s1 = dl_malloc0 (tsize * sizeof (node *));
  token *s2 = dl_malloc (tsize * sizeof (token));
  int s1n = 0, s2n = 0;

#define FAIL for (i = 0; i < s1n; i++) { \
               del_node (s1[i]);         \
             }                           \
             OK;                         \
             return NULL

#define OK dl_free (s1, tsize * sizeof (node *));    \
           dl_free (s2, tsize * sizeof (token))

  for (i = 0; i < n; i++) {
    if (IS_OP(v[i].type)) { // op
      if (v[i].type == op_lb) {
        s2[s2n++] = v[i];
      } else {
        if (!IS_PREFIX_UN(v[i].type) && !upd (s1, s2, &s1n, &s2n, priority[v[i].type])) {
          FAIL;
        }
        if (v[i].type == op_rb) {
          if (!s2n || s2[s2n - 1].type != op_lb) {
            FAIL;
          }
          s2n--;
        } else {
          s2[s2n++] = v[i];
        }
      }
    } else {
      s1[s1n] = new_node();
      s1[s1n++]->tok = v[i];
    }
  }
  if (!upd (s1, s2, &s1n, &s2n, priority[op_rb]) || s1n != 1 || s2n) {
    FAIL;
  }
  node *res = s1[0];
  OK;

#undef FAIL
#undef OK
  return res;
}

node **mem;
int mn_mx;

void collect (node *v, token_type t) {
  if (v->tok.type == t) {
    collect (v->v[0], t);
    collect (v->v[1], t);
    del_node (v);
  } else {
    *mem++ = v;
  }
}

inline val_type get_op_type (val_type a, token_type op) {
  switch (op) {
    case op_color:
    case op_in:
    case op_bit_count:
      if (a != v_int && a != v_long) {
        return v_err;
      }
    case op_now:
    case op_int:
      return v_int;
    case op_long:
      return v_long;
    case op_double:
      return v_double;
    case op_string:
      return v_cstring;
    default:
      break;
  }

  if (IS_STR(a)) {
    if (op_type[op] == t_cmp || op == op_shl || op == op_length || op == op_type_to_id) {
      return v_int;
    }
    if (op_type[op] == t_str) {
      return v_cstring;
    }
    return v_err;
  }

  switch (op_type[op]) {
    case t_arif:
      if (op == op_rem && a == v_double) {
        return v_err;
      }
      return a;
    case t_logic:
      if (a == v_double) {
        return v_err;
      }
      return v_int;
    case t_bin:
      if (a == v_double) {
        return v_err;
      }
      return a;
    case t_cmp:
      return v_int;
    default:
      return v_err;
  }
  return v_err;
}


node *tree_conv (node *v, val_type new_type) {
  node *r = new_node();
  r->v = dl_malloc0 (sizeof (node *));
  r->vn = 1;
  r->v[0] = v;

  if (IS_STR (new_type)) {
    r->tok.type = op_string;
    r->tok.dtype = v_cstring;
  } else {
    r->tok.type = op_conv;
    r->tok.dtype = new_type;
  }

  r->is_const = v->is_const;

  return r;
}

void node_pfree (node *v) {
  if (v->is_const) {
    int i;
    for (i = 0; i < v->vn; i++) {
      del_node (v->v[i]);
    }
    dl_free (v->v, sizeof (node *) * v->vn);
    v->vn = 0;
    v->v = NULL;
    v->tok.type = t_var;
  }
}

inline int f_bit_count_int (int x) {
  return __builtin_popcount (x);
}

inline int f_bit_count_long (long long x) {
  return __builtin_popcountll (x);
}

char *f_tostring (token *tok, val_type tp) {
  char *t = sp_str_pre_alloc (MAX_EVENT_SIZE + 1);

  if (t != NULL) {
#define F(s, dtype)                                   \
      case dtype:                                     \
        snprintf (t, MAX_EVENT_SIZE, s, tok->dtype);  \
        break;

    switch (tok->dtype) {
      F("%d", v_int);
      F("%lf", v_double);
      F("%lld", v_long);
      F("%s", v_string);
      F("%s", v_cstring);
      default:
        break;
    }

#undef F
  }

  return t;
}

int recalc_node (node *v) {
  if (v->vn == 0) {
    switch (v->tok.type) {
      case t_fid:
        if (v->tok.v_fid >= 0 && !has_field (_cur_event, v->tok.v_fid)) {
          return 0;
        }
        switch (v->tok.dtype) {
          case v_int:
            v->tok.v_int = *GET_INT (_cur_event, &_cur_type->fields[v->tok.v_fid]);
            return 1;
          case v_long:
            v->tok.v_long = *GET_LONG (_cur_event, &_cur_type->fields[v->tok.v_fid]);
            return 1;
          case v_double:
            v->tok.v_double = *GET_DOUBLE (_cur_event, &_cur_type->fields[v->tok.v_fid]);
            return 1;
          case v_string:
            //fprintf (stderr, "look for %d\n", v->tok.v_fid);
            v->tok.v_string = GET_STRING (_cur_event, &_cur_type->fields[v->tok.v_fid]);
            //fprintf (stderr, "recalced[%s]\n", v->tok.v_string);
            return 1;
          default:
            assert (0);
        }
        break;
      case op_now:
        v->tok.v_int = now;
        break;
      case t_var:
        break;
      default:
        assert (0);
    }
  } else {
    val_type op_t = v->v[0]->tok.dtype, res_t = v->tok.dtype;

#define T(x) T_(x)
#define T_(x) v_##x
#define res v->tok
#define op1 v->v[0]->tok
#define op2 v->v[1]->tok
#define U(x, y, z) res.T(y) = z op1.T(x); break
#define B(x, y, z) res.T(y) = op1.T(x) z op2.T(x); break
#define C(z) res.T(int) = strcmp (op1.T(string), op2.T(string)) z 0; break

//    fprintf (stderr, "%s %d %d\n", token_to_str (&v->tok), v->v[0]->tok.v_int, v->v[1]->tok.v_int);

    if (IS_STR(op_t) || IS_STR(res_t)) {
//      assert (res_t == v_int);

      switch (v->tok.type) {
        case op_l: C(<);
        case op_m: C(>);
        case op_le: C(<=);
        case op_me: C(>=);
        case op_eq: C(==);
        case op_neq: C(!=);
        case op_shl:
          //if (v->v[0]->kmp != NULL) {
            //fprintf (stderr, "optimized\n");
          //  res.T(int) = dl_strstr_kmp (op1.T(string), v->v[0]->kmp, op2.T(string)) != NULL; break;
          //} else {
            //fprintf (stderr, "stl (%s, %s)\n", op1.T(string), op2.T(string));
            res.T(int) = strstr (op2.T(string), op1.T(string)) != NULL; break;
          //}
        case op_sort:
          res.T(string) = sp_sort (op1.T(string));
          return res.T(string) != NULL;
        case op_upp:
          res.T(string) = sp_to_upper (op1.T(string));
          return res.T(string) != NULL;
        case op_low:
          res.T(string) = sp_to_lower (op1.T(string));
          return res.T(string) != NULL;
        case op_deunicode:
          res.T(string) = sp_deunicode (op1.T(string));
          return res.T(string) != NULL;
        case op_simpl:
          res.T(string) = sp_simplify (op1.T(string));
          return res.T(string) != NULL;
        case op_fsimpl:
          res.T(string) = sp_full_simplify (op1.T(string));
          return res.T(string) != NULL;
        case op_int:
          return sscanf (op1.T(string), "%d", &res.T(int));
        case op_long:
          return read_long_only (op1.T(string), &res.T(long));
        case op_double:
          return sscanf (op1.T(string), "%lf", &res.T(double));
        case op_string:
          assert (IS_STR(res_t));
          res.T(string) = f_tostring (&op1, op_t);
          return res.T(string) != NULL;
        case op_length:
          res.T(int) = strlen (op1.T(string));
          return 1;
        case op_type_to_id:
          res.T(int) = get_type_id (op1.T(string));
          return res.T(int) != 0;
        default:
          assert (0);
      }
    } else {

#define D1_(x, y)                  \
    switch (v->tok.type) {         \
       case op_int:                \
       case op_long:               \
       case op_double:             \
       case op_conv:               \
         res.T(y) = op1.T(x);      \
         break;

#define D2_(x, y)                  \
       case op_usub: U (x, y, -);  \
       case  op_not: U (x, y, !);  \
       case  op_mul: B (x, y, *);  \
       case  op_div: if (op2.T(y) == 0) {return 0;} B (x, y, /);  \
       case  op_add: B (x, y, +);  \
       case  op_sub: B (x, y, -);  \
       case    op_l: B (x, y, <);  \
       case    op_m: B (x, y, >);  \
       case   op_le: B (x, y, <=); \
       case   op_me: B (x, y, >=); \
       case   op_eq: B (x, y, ==); \
       case  op_neq: B (x, y, !=); \
       case  op_seg: assert (0);

#define D3_(x, y)                  \
       case  op_in: res.T(y) = set_ll_get (&res.param_set, op1.T(x)) != NULL; break; \
       case  op_color: res.T(y) = get_color (res.param_int, op1.T(x)); break; \
       case  op_bit_count: res.T(y) = f_bit_count_ ## x (op1.T(x)); break; \
       case  op_rem: if (op2.T(y) == 0) {return 0;} B (x, y, %);  \
       case op_comp: U (x, y, ~);  \
       case op_band: B (x, y, &);  \
       case  op_xor: B (x, y, ^);  \
       case  op_bor: B (x, y, |);  \
       case  op_shl: B (x, y, <<); \
       case  op_shr: B (x, y, >>);

#define D(x, y)                    \
       D1_ (x, y)                  \
       D2_ (x, y)                  \
       D3_ (x, y)                  \
       default:                    \
         assert (0);               \
   }                               \
   break;

#define D2(x, y)                   \
       D1_ (x, y)                  \
       D2_ (x, y)                  \
       default:                    \
         assert (0);               \
   }                               \
   break;

#define D3(x, y)                   \
       D1_ (x, y)                  \
       default:                    \
         assert (0);               \
   }                               \
   break;

      switch (op_t) {
        case v_int:
          switch (res_t) {
            case v_int:
              D (int, int);
            case v_long:
              D3 (int, long);
            case v_double:
              D3 (int, double);
            default:
              assert (0);
          }
          break;
        case v_long:
          switch (res_t) {
            case v_int:
              D (long, int);
            case v_long:
              D (long, long);
            case v_double:
              D3 (long, double);
            default:
              assert (0);
          }
          break;
        case v_double:
          switch (res_t) {
            case v_int:
              D2 (double, int);
            case v_long:
              D3 (double, long);
            case v_double:
              D2 (double, double);
            default:
              assert (0);
          }
          break;
        default:
          assert (0);
      }
    }
  }

#undef T
#undef T_
#undef res
#undef op1
#undef op2
#undef U
#undef B
#undef D1_
#undef D2_
#undef D
#undef D2
#undef C
  return 1;
}

inline int to_bool (node *e) {
  if (e->tok.dtype == v_int) {
    return e->tok.v_int != 0;
  }
  return e->tok.v_long != 0;
}

int simplify_tree (node *v) {
  if (v->tok.type == op_and || v->tok.type == op_or) {
/*    node **a = mem;
    collect (v, v->tok.type);
//    int an = mem - a;

    mem = a;
    //simplify
    //token_type t = get_type ()
    assert (0);*/

    int need = v->tok.type == op_and ? 0 : 1;

    int is_const = 1;
    v->tok.dtype = v_int;
    int i;

    for (i = 0; i < v->vn; i++) {
      if (!simplify_tree (v->v[i])) {
        return 0;
      }
      is_const &= v->v[i]->is_const;

      if (v->v[i]->tok.dtype != v_int && v->v[i]->tok.dtype != v_long) {
        return 0;
      }

      if (v->v[i]->is_const) {
        int x = to_bool (v->v[i]);
        if (x == need) {
          v->tok.v_int = need;
          v->is_const = is_const;
//TODO why do not do node_pfree here?
// arseny30: Checking...
          node_pfree (v);
          return 1;
        }
//TODO can we delete node v->v[i] here?
      }
    }
    v->is_const = is_const;
    if (is_const) {
      v->tok.v_int = 1 - need;
    }

    node_pfree (v);
    return 1;
  } else {
    if (v->vn == 0 && !IS_CONST(v->tok.type)) { // one var
      assert (v->tok.type == t_var);

      v->is_const = v->tok.dtype != v_fid;

      if (v->tok.dtype == v_fid) {
        v->tok.type = t_fid;
        v->tok.dtype = std_t[_cur_type->fields[v->tok.v_fid].type + FN];
      }
    } else {
      int i;
      int is_const = 1;
      val_type conv = v_int;

      for (i = 0; i < v->vn; i++) {
        if (!simplify_tree (v->v[i])) {
          return 0;
        }
        is_const &= v->v[i]->is_const;
        conv = max (conv, v->v[i]->tok.dtype);
      }

      if (v->tok.type == op_shr && IS_STR(conv)) {
        void *tmp;
        tmp = v->v[0], v->v[0] = v->v[1], v->v[1] = tmp;
        v->tok.type = op_shl;
      }

      if (conv == v_string) {
        conv = v_cstring;
      }
      for (i = 0; i < v->vn; i++) {
        if (v->v[i]->tok.dtype != conv) {
          int f1 = IS_STR(conv), f2 = IS_STR(v->v[i]->tok.dtype);
          if (f1 && f2) {
            continue;
          }

          v->v[i] = tree_conv (v->v[i], conv);

          if (v->v[i]->is_const) {
            if (!recalc_node (v->v[i])) {
              return 0;
            }
          }
        }
      }
      v->tok.dtype = get_op_type (conv, v->tok.type);
      if (v->tok.dtype == v_err) {
        return 0;
      }
      v->is_const = is_const;

      if (is_const) {
        if (!recalc_node (v)) {
          return 0;
        }
        node_pfree (v);
      } else {
        /*
        if (v->tok.type == op_shl && IS_STR(v->v[0]->tok.dtype) && v->v[0]->is_const) {
          v->v[0]->kn = strlen (v->v[0]->tok.v_string) + 1;
          v->v[0]->kmp = dl_malloc (sizeof (int) * v->v[0]->kn);
//          fprintf (stderr, "allocated %p %d\n", v->v[0]->kmp, v->v[0]->kn);
          dl_kmp (v->v[0]->tok.v_string, v->v[0]->kmp);
        }
        */
      }
    }
  }

  return 1;
}


inline int recalc_tree_ (node *v) {
  int i;

  int need = -1;
  if (v->tok.type == op_and) {
    need = 0;
  }
  if (v->tok.type == op_or) {
    need = 1;
  }

  if (need != -1) {
    for (i = 0; i < v->vn; i++) {
      if (!recalc_tree_ (v->v[i])) {
        return 0;
      }

      int x = to_bool (v->v[i]);
      if (x == need) {
        v->tok.v_int = need;
        return 1;
      }
    }

    v->tok.v_int = 1 - need;
    return 1;
  } else {
    for (i = 0; i < v->vn; i++) {
      if (!recalc_tree_ (v->v[i])) {
        return 0;
      }
    }

    return recalc_node (v);
  }
}

inline int recalc_tree (node *v, event *cur_event) {
  sp_init();
  _cur_event = cur_event;

  return recalc_tree_ (v);
}

void node_debug (node *v, int d) {
  if (v == NULL) {
    return;
  }
  if (v->vn == 2) {
    node_debug (v->v[1], d + 2);
  }
  fprintf (stderr, "%*s%s\n", d, "", token_to_str (&v->tok));
  if (v->vn) {
    node_debug (v->v[0], d + 2);
  }
}

long long mask_tree (node *v) {
  if (v == NULL) {
    return 0;
  }
  long long res = 0;
  if (v->tok.type == t_fid) {
    if (v->tok.v_fid >= 0) {
      res |= 1ll << v->tok.v_fid;
    }
  }
  int i;
  for (i = 0; i < v->vn; i++) {
    res |= mask_tree (v->v[i]);
  }

  return res;
}

void del_token_list (token *v) {
  if (v == NULL) {
    return;
  }

  token *t = v;
  while (t->type != end_token) {
    if (t->type == op_in) {
      set_ll_free (&t->param_set);
    }

    t++;
  }

  dl_free (v, (t - v + 1) * sizeof (token));
}


char err_buff[10000];

int expression_prepare (node *v) {
  if (v == NULL) {
    return 1;
  }
  if (v->tok.dtype == v_cstring && v->tok.v_cstring != NULL) {
    v->tok.v_string = dl_strdup (v->tok.v_cstring);
    v->tok.dtype = v_string;
  }
  int i;
  for (i = 0; i < v->vn; i++) {
    if (!expression_prepare (v->v[i])) {
      return 0;
    }
  }
  return 1;
}

#define FAIL(args...)  {          \
  char *e = err_buff;             \
  e += sprintf (e, "[{\"error\":\""); \
  e += sprintf (e,  ## args);     \
  e += sprintf (e, "\"}]");       \
  return err_buff;                \
}

char *expression_compile (expression *e, char *s, type_desc *cur_type) {
  sp_init();
  _cur_type = cur_type;

  e->s = s;
  e->token_list = get_token_list (s);

  if (e->token_list == NULL) {
    FAIL ("can't split query [%s] into tokens", s);
  }

  e->root = gen_tree (e->token_list);
  if (e->root == NULL) {
    del_token_list (e->token_list);
    FAIL ("can't parse query [%s] as arithmetic expression", s);
  }

  int res = simplify_tree (e->root);
  if (!res) {
    del_token_list (e->token_list);
    del_node (e->root);
    FAIL ("can't parse query [%s] as arithmetic expression", s);
  }

  res = expression_prepare (e->root);
  if (!res) {
    del_token_list (e->token_list);
    del_node (e->root);
    FAIL ("can't prepare expression [%s]", s);
  }

  return NULL;
}
#undef FAIL

void expression_free (expression *e) {
  del_token_list (e->token_list);
  del_node (e->root);
}

char sl_mem[MAX_SELECT * sizeof (int)];
int sl_mem_n;

void sl_mem_init (void) {
  sl_mem_n = 0;
}

inline void *sl_mem_alloc (int n) {
  char *res = sl_mem + sl_mem_n;
  sl_mem_n += n;
  assert (sl_mem_n <= MAX_SELECT * (int)sizeof (int));
  return res;
}


char *sl_name[] = {"ALL", "COUNT", "MAX", "MIN", "SUM", "EXPR"};
typedef enum {st_select, st_count, st_max, st_min, st_sum} select_t;

typedef struct {
  union {
    expression *expr;
    int id;
  };
  char *alias;
  enum {sln_expr, sln_id} type;
} sl_node;

typedef struct {
  select_t type;
  sl_node *nodes;

  int nodes_n;
  char *alias;
} select_list;


long long event_to_hash (event *e, select_list *sl) {
  assert (sl);

  type_desc *t = &types[e->type];

  if (sl->nodes_n == 1 && sl->nodes[0].type == sln_id) {
    field_desc *f = &t->fields[sl->nodes[0].id];
    switch (std_t[f->type + FN]) {
      case t_int:
        return *GET_INT(e, f);
      case t_long:
        return *GET_LONG(e, f);
      case t_double:
        return (long long)floor ((*GET_DOUBLE(e, f)) * 1e6 + 0.5);
      case t_string:
        return dl_strhash (GET_STRING(e, f));
      default:
        assert (0);
    }
  }

  int i, ii;
  int L, R;
  if (sl->nodes_n < 0) {
    L = -FN;
    R = t->field_i;
  } else {
    L = 0;
    R = sl->nodes_n;
  }

  long long h = 0;
#define H_LONG(x)   (h = h * 3999999995199999997ll + (x))
#define H_DOUBLE(x) (h = h * 1231772829388291121ll + (long long)floor ((x) * 1e6 + 0.5))
#define H_STR(x)    (h = h * 2311928816762378187ll + dl_strhash (x))

  int cur_t = 0;
  expression *expr = NULL;
  for (ii = L; ii < R; ii++) {
    if (L == 0) {
      if (sl->nodes[ii].type == sln_id) {
        i = sl->nodes[ii].id;
        cur_t = 0;
      } else {
        expr = sl->nodes[ii].expr;
        cur_t = 1;
      }
    } else {
      i = ii;
      ii += ii < 0 && std_t[ii + FN];
      cur_t = 0;
    }
    if (cur_t == 0) {
      if (i < 0 || has_field (e, i)) {
        field_desc *f = &t->fields[i];
        switch (std_t[f->type + FN]) {
          case t_int:
            H_LONG (*GET_INT(e, f));
            break;
          case t_long:
            H_LONG (*GET_LONG(e, f));
            break;
          case t_double:
            H_DOUBLE (*GET_DOUBLE(e, f));
            break;
          case t_string:
            H_STR (GET_STRING(e, f));
            break;
          default:
            assert (0);
        }
      }
    } else {
      if (recalc_tree (expr->root, e)) {
        token *t = &expr->root->tok;
        switch (t->dtype) {
          case v_int:
            H_LONG (t->v_int);
            break;
          case v_long:
            H_LONG (t->v_long);
            break;
          case v_double:
            H_DOUBLE (t->v_double);
            break;
          case v_string:
          case v_cstring:
            H_STR (t->v_string);
            break;
          default:
            assert (0);
        }
      }
    }
  }

  return h;
}
#undef H_LONG
#undef H_STR
#undef H_DOUBLE

char *event_to_str (event *e, select_list *sl) {
  static char buff[2 * MAX_EVENT_SIZE + 1];
  char *s = buff, *en = buff + 2 * MAX_EVENT_SIZE + 1;
  type_desc *t = &types[e->type];

  int i, ii;
  int L, R;
  if (sl == NULL || sl->nodes_n < 0) {
    assert (sl);
    L = -FN;
    R = t->field_i;
  } else {
    L = 0;
    R = sl->nodes_n;
  }

  s += snprintf (s, max (en - s, 0), "{");
  int w = 0;

  int cur_t = 0;
  expression *expr = NULL;
  char *alias;
  for (ii = L; ii < R; ii++) {
    if (L == 0) {
      if (sl->nodes[ii].type == sln_id) {
        i = sl->nodes[ii].id;
        cur_t= 0;
        alias = sl->nodes[ii].alias;
      } else {
        expr = sl->nodes[ii].expr;
        alias = sl->nodes[ii].alias;
        cur_t = 1;
      }
    } else {
      i = ii;
      ii += ii < 0 && std_t[ii + FN];
      cur_t = 0;
      alias = NULL;
    }
    if (cur_t == 0) {
      if (i < 0 || has_field (e, i)) {
        //fprintf (stderr, "i = %d\n", i);
        field_desc *f = &t->fields[i];
        if (alias == NULL) {
          alias = f->name;
        }
        //fprintf (stderr, "%s : %d\n", f->name, f->type);
        if (w) {
          s += snprintf (s, max (en - s, 0), ",");
        }
        w = 1;

        if (f->type == -FN + 1 && sl->nodes_n == -2) {
          s += snprintf (s, max (en - s, 0), "\"%s\":\"", alias);
          s += write_str (s, max (en - s, 0), types[*GET_INT(e, f)].name);
          s += snprintf (s, max (en - s, 0), "\"");
          continue;
        }

        switch (std_t[f->type + FN]) {
          case t_int:
            s += snprintf (s, max (en - s, 0), "\"%s\":\"%d\"", alias, *GET_INT(e, f));
            break;
          case t_double:
            s += snprintf (s, max (en - s, 0), "\"%s\":\"%.6lf\"", alias, *GET_DOUBLE(e, f));
            break;
          case t_long:
            s += snprintf (s, max (en - s, 0), "\"%s\":\"%lld\"", alias, *GET_LONG(e, f));
            break;
          case t_string:
            s += snprintf (s, max (en - s, 0), "\"%s\":\"", alias);
            s += write_str (s, max (en - s, 0), GET_STRING(e, f));
            s += snprintf (s, max (en - s, 0), "\"");
            break;
          default:
            assert (0);
        }
      }
    } else {
      if (recalc_tree (expr->root, e)) {
        if (w) {
          s += snprintf (s, max (en - s, 0), ",");
        } else {
          w = 1;
        }

        assert (alias != NULL);
        s += snprintf (s, max (en - s, 0), "\"%s\":\"", alias);
        s += write_str (s, max (en - s, 0), token_val_to_str (&expr->root->tok));
        s += snprintf (s, max (en - s, 0), "\"");
      }
    }
  }
  s += snprintf (s, max (en - s, 0), "}");

  if (s > en) {
    debug_error = 1;
  }
  return buff;
}


inline int check_query (expression *expr, event *e) {
  if (recalc_tree (expr->root, e)) {
    return expr->root->tok.v_int;
  }
  return 0;
}

#define FAIL(args...)  {              \
  char *e = err_buff;                 \
  e += sprintf (e, "[{\"error\":\""); \
  e += sprintf (e,  ## args);         \
  e += sprintf (e, "\"}]");           \
  return err_buff;                    \
}

int *ans;

#define DISTINCT_FLAG 1
#define LENGTH_FLAG 2

int cmp_long (const void *a, const void *b) {
  if (*(long long *)a < *(long long *)b) {
    return -1;
  }
  return *(long long *)a > *(long long *)b;
}

int global_sort_direction, global_sort_field, global_rcid, global_sort_type, global_sl_n;
long long *global_data;

int cmp_event (const void *a, const void *b) {
  int i1 = *(int *)a, i2 = *(int *)b;

  if (global_sort_type == t_double) {
    double val1 = *(double *)&global_data[i1 + global_rcid * global_sort_field],
           val2 = *(double *)&global_data[i2 + global_rcid * global_sort_field];
    if (val1 != val2) {
      return (2 * (val1 > val2) - 1) * global_sort_direction;
    }
    if (i1 != i2) {
      return (2 * (i1 > i2) - 1) * global_sort_direction;
    }
    return 0;
  }
  long long val1 = global_data[i1 + global_rcid * global_sort_field],
            val2 = global_data[i2 + global_rcid * global_sort_field];
  if (val1 != val2) {
    return (2 * (val1 > val2) - 1) * global_sort_direction;
  }
  if (i1 != i2) {
    return (2 * (i1 > i2) - 1) * global_sort_direction;
  }
  return 0;
}

char *run_select (select_list *sl, int sl_n, int flag, int type_id, int xfid, long long val, char *where, char *agr_cond, int limit, int offset, int time_limit, int sort_field, int sort_direction) {
  if (limit < 0 || limit > MAX_SELECT) {
    if (flag & LENGTH_FLAG) {
      limit = INT_MAX;
    } else {
      limit = MAX_SELECT;
    }
  }
  if (limit == 0) {
    return "[]";
  }

  int i, j, length = 0;

  if (write_only) {
    FAIL ("this engine is write only");
  }

  long long need_mask = 0;
  int t;
  expression **exprs = NULL, where_expr;
  int expr_n = 0;
  for (t = 0; t < 2; t++) {
    for (i = 0; i < sl_n; i++) {
      for (j = 0; j < sl[i].nodes_n; j++) {
        if (sl[i].nodes[j].type == sln_id) {
          if (sl[i].nodes[j].id >= 0) {
            need_mask |= 1ll << sl[i].nodes[j].id;
          }
        } else {
          assert (sl[i].nodes[j].type == sln_expr);

          if (t) {
            exprs[expr_n++] = sl[i].nodes[j].expr;
          } else {
            expr_n++;
          }
        }
      }
    }
    if (!t) {
      exprs = sl_mem_alloc (sizeof (expression *) * (expr_n + 1));
      exprs[0] = &where_expr;
      expr_n = 1;
    }
  }
  sl++;
  sl_n--;

  if (sort_field < 0 || sort_field >= sl_n) {
    sort_direction = 0;
    sort_field = 0;
  }

  if (verbosity > 2) {
    fprintf (stderr, "run select:");

    //TODO: may be...
    /*
    fprintf (stderr, "fids:");
    int i;
    for (i = 0; i < fn; i++) {
      fprintf (stderr, "%d%c", fid[i], ",\n"[i + 1 == fn]);
    }
    fprintf (stderr, "expressions(%d):", expr_n);
    for (i = 0; i < expr_n; i++) {
      fprintf (stderr, "%s%c", exprs[i]->s, ",\n"[i + 1 == fn]);
    } */
    fprintf (stderr, "flag = %d\n", flag);
    fprintf (stderr, "type_id = %d xfid = %d sval = %lld\n", type_id, xfid, val);
    fprintf (stderr, "where = %s\n", where);
    fprintf (stderr, "agr_cond = %s\n", agr_cond);
    fprintf (stderr, "limit = %d\n", limit);
  }

  assert (xfid != -FN + 1);

  int qid = -1;
  if (type_id != 0 && xfid == 0) {
    xfid = -FN + 1;
    val = type_id;
  }

  if (xfid < 0) {
    qid = xfid + FN;
  }

  assert (0 <= type_id && type_id < MAX_TYPE);
  type_desc agr_type, *main_type = &types[type_id];

  char *err;
  if ((err = expression_compile (exprs[0], where, main_type))) {
    return err;
  }

  if (IS_STR(exprs[0]->root->tok.dtype)) {
    expression_free (exprs[0]);
    FAIL ("can't convert result of WHERE query (string) to int");
  }
  if (exprs[0]->root->tok.dtype != v_int) {
    exprs[0]->root = tree_conv (exprs[0]->root, v_int);
  }
  node *root = exprs[0]->root;

  for (i = 1; i < expr_n; i++) {
    if ((err = expression_compile (exprs[i], exprs[i]->s, main_type))) {
      while (i--) {
        expression_free (exprs[i]);
      }
      return err;
    }
  }

//  fprintf (stderr, "calc agregate_types\n");
  int *agr_types = sl_mem_alloc (sizeof (int) * sl_n);
  for (i = 0; i < sl_n; i++) {
    if (sl[i].type == st_count) {
      agr_types[i] = t_int;
    } else {
      assert (sl[i].nodes_n == 1);

//TODO think about using t_int instead of t_long when possible
      if (sl[i].nodes[0].type == sln_id) {
        agr_types[i] = std_t[main_type->fields[sl[i].nodes[0].id].type + FN];
      } else {
        switch (sl[i].nodes[0].expr->root->tok.dtype) {
          case v_double:
            agr_types[i] = t_double;
            break;
          case v_int:
            agr_types[i] = t_int;
            break;
          case v_long:
            agr_types[i] = t_long;
            break;
          default:
            while (expr_n--) {
              expression_free (exprs[expr_n]);
            }
            FAIL ("can't agregate strings in agregate function %s", sl_name[sl[i].type]);
        }
      }
    }
    assert (agr_types[i] >= t_int);
//    fprintf (stderr,  "type = %d\n", agr_types[i]);
  }

//  fprintf (stderr, "done\n");

  event *agr_event = NULL;
  expression agr_expr;
  size_t agr_event_size = 0;
  int *agr_id = NULL, agr_n = 0;
  int agr_limit = 1;

  if (sort_direction != 0 && agr_cond == NULL) {
    agr_cond = "1";
  }

  if (agr_cond != NULL) {
    agr_id = sl_mem_alloc (sl_n * sizeof (int));

    memset (&agr_type, 0, sizeof (agr_type));
    agr_type.fields = dl_malloc (sizeof (field_desc) * FN);
    for (i = 0; i < FN; i += 1 + std_t[i]) {
      agr_type.fields[i] = empty_desc[i];
    }
    agr_type.fields += FN;

    for (i = 0; i < sl_n && agr_n < 32; i++) {
      if (sl[i].alias != NULL) {
        //fprintf (stderr, "add field : %s\n", sl[i].alias);
        add_field (&agr_type, sl[i].alias, max (agr_types[i], t_long));
        agr_id[agr_n++] = i;
      }
    }

    if ((err = expression_compile (&agr_expr, agr_cond, &agr_type))) {
      type_free (&agr_type);
      for (i = 0; i < expr_n; i++) {
        expression_free (exprs[i]);
      }
      return err;
    }

    if (agr_expr.root->is_const) {
      type_free (&agr_type);

      if (!agr_expr.root->tok.v_int) {
        expression_free (&agr_expr);
        for (i = 0; i < expr_n; i++) {
          expression_free (exprs[i]);
        }

        return "[]";
      }

      expression_free (&agr_expr);
      agr_cond = NULL;

      if (sort_direction) {
        agr_limit = limit;
        limit = MAX_EV;
      }
    } else {
      agr_event_size = offsetof (event, data) + sizeof (long long) * 2 * agr_n;
      agr_event = dl_malloc0 (agr_event_size);
      agr_event->sn = agr_n;
      agr_event->mask = -1;
      for (i = 0; i < agr_n; i++) {
        agr_event->data[i] = i * sizeof (long long);
      }

      agr_limit = limit;
      limit = MAX_EV;
    }
    _cur_type = main_type;
  }

//  IMPORTANT: don't comment debug in this function
  ds = debug_buff;

  debug ("[");
  int f = 0;

  set_ll was;
  set_ll_init (&was);

  map_ll_int h_id;
  map_ll_int_init (&h_id);

  vector (int, q_id);
  vector (int, t_id);
  vector_init (q_id);
  vector_init (t_id);

  double tt = 0;
  if (test_mode) {
    tt = cputime();
  }

//  fprintf (stderr, "START\n");

  for (i = 0; i < expr_n; i++) {
    need_mask |= mask_tree (exprs[i]->root);
  }

  int collect_mode = sl_n;

  int cid = 0, q_st;
  for (q_st = get_q_st (qid, val); q_st != -1 && limit > 0 && eq[q_st]->timestamp > time_limit; q_st = get_q_prev (q_st, qid)) {
    if ((use_default || (eq[q_st]->mask & need_mask) == need_mask) && (!type_id || eq[q_st]->type == type_id) &&
        recalc_tree (root, eq[q_st]) && root->tok.v_int) {
      long long h = 0;
      if (collect_mode || (flag & DISTINCT_FLAG)) {
        h = event_to_hash (_cur_event, &sl[-1]);
      }
      if (collect_mode) {
        int *pid;
        if (cid - offset < limit) {
          pid = map_ll_int_add (&h_id, h);
          if (*pid == 0) {
            if (cid >= offset) {
              ans[cid - offset] = q_st;
            }
            *pid = ++cid;
          }
        } else {
          pid = map_ll_int_get (&h_id, h);
        }

        if (pid != NULL && *pid > offset) {
          vector_pb (q_id, q_st);
          vector_pb (t_id, *pid - 1 - offset);
        }
      } else {
        if (flag & DISTINCT_FLAG) {
          int used = set_ll_used (&was);
          set_ll_add (&was, h);
          if (used == set_ll_used (&was)) {
            continue;
          }
        }

        if (offset <= 0) {
          limit--;

          if (flag & LENGTH_FLAG) {
            length++;
          } else {
            if (f) {
              debug (",");
            } else {
              f = 1;
            }

            char *s = event_to_str (_cur_event, &sl[-1]);
            debug ("%s", s);
          }
        } else {
          offset--;
        }
      }
    }
  }

  set_ll_free (&was);
  map_ll_int_free (&h_id);

  if (test_mode) {
    tt = cputime() - tt;
    if (tt >= 0.01) {
      fprintf (stderr, "    first section : %.6lf\n", tt);
    }
    tt = cputime();
  }

  int en = q_id_size;
  if (collect_mode) {
    int rcid = cid - offset;
    long long *data = dl_malloc0 (sizeof (long long) * rcid * sl_n);
    set_ll *v = NULL;

    for (j = 0; j < sl_n; j++) {
      if (sl[j].type == st_count) {
        if (sl[j].nodes_n < 0) {
          //TODO bad memory management, very bad!!!
          for (i = 0; i < en; i++) {
            data[t_id[i] + rcid * j]++;
          }
        } else {
          if (v == NULL) {
            v = dl_malloc (sizeof (set_ll) * rcid);
          }
          for (i = 0; i < rcid; i++) {
            set_ll_init (&v[i]);
          }

          for (i = 0; i < en; i++) {
            long long h = event_to_hash (eq[q_id[i]], &sl[j]);

            set_ll_add (&v[t_id[i]], h);
          }

          for (i = 0; i < rcid; i++) {
            data[i + rcid * j] = set_ll_used (&v[i]);
          }

          for (i = 0; i < rcid; i++) {
            set_ll_free (&v[i]);
          }
        }

        continue;
      }

      int is_expr = (sl[j].nodes[0].type == sln_expr);

      field_desc *fi = NULL;
      expression *ex = NULL;
      if (is_expr) {
        ex = sl[j].nodes[0].expr;
      } else {
        fi = &_cur_type->fields[sl[j].nodes[0].id];
      }

      static char def_buf[sizeof (long long)], *def_val = def_buf;
#define SET(x, y) *(x *)def_val = y;       \
        for (i = 0; i < rcid; i++) {       \
          *(x *)&data[i + rcid * j] = y;   \
        }

      int tp = agr_types[j];

      if (sl[j].type == st_min) {
        if (tp == t_double) {
          SET (double, DBL_MAX);
        } else {
          SET (ll, LLONG_MAX);
        }
      } else if (sl[j].type == st_max) {
        if (tp == t_double) {
          SET (double, DBL_MIN);
        } else {
          SET (ll, LLONG_MIN);
        }
      }
#undef SET

#define GO(x, y, z) _GO (x, y, z)

#define EX(e, x, y) ( recalc_tree (ex->root, e) ? ex->root->tok.v_ ## x : *(y *)def_val )

#define FI(e, x, y) (y)(*GET_ ## x (e, fi))

#define C(c, F, x, y, G)                                                            \
        case c:                                                                     \
          for (i = 0; i < en; i++) {                                                \
            y tmp = G(eq[q_id[i]], x, y), *val = (y *)&data[t_id[i] + rcid * j];    \
            F(*val, tmp);                                                           \
          }                                                                         \
        break;

#define _ADD(x, y) x += y;
#define _MIN(x, y) if (x > y) {x = y;}
#define _MAX(x, y) if (x < y) {x = y;}

#define _GO(x, y, G)              \
      switch (sl[j].type) {       \
        C(st_sum, _ADD, x, y, G); \
        C(st_max, _MAX, x, y, G); \
        C(st_min, _MIN, x, y, G); \
        default:                  \
          break;                  \
      }

#define TP(G)                        \
         switch (tp) {               \
           case t_int:               \
             GO (INT, ll, G);        \
             break;                  \
           case t_long:              \
             GO (LONG, ll, G);       \
             break;                  \
           case t_double:            \
             GO (DOUBLE, double, G); \
             break;                  \
           default:                  \
             assert (0);             \
         }

       if (is_expr) {
         TP (EX);
       } else {
         TP (FI);
       }
    }

#undef _ADD
#undef _MIN
#undef _MAX
#undef C
#undef GO
#undef _GO
#undef EX
#undef FI
#undef TP

//fprintf (stderr, "ok...\n");
    if (v != NULL) {
      dl_free (v, sizeof (set_ll) * rcid);
      v = NULL;
    }

    if (test_mode) {
      fprintf (stderr, "    second section : %.6lf\n", cputime() - tt);
      tt = cputime();
    }

    int *perm = dl_malloc (sizeof (int) * (cid - offset));
    for (i = 0; i < cid - offset; i++) {
      perm[i] = i;
    }
    if (sort_direction) {
      global_sort_direction = sort_direction;
      global_sort_field = sort_field;
      global_rcid = cid - offset;
      global_sl_n = sl_n;
      global_data = data;
      global_sort_type = agr_types[sort_field];

      qsort (perm, global_rcid, sizeof (int), cmp_event);

      if (test_mode) {
        fprintf (stderr, "    second and half section : %.6lf\n", cputime() - tt);
        tt = cputime();
      }
    }

    int ii;
    int fi = 0;
    for (ii = offset; ii < cid && agr_limit > 0; ii++) {
      int i = perm[ii - offset];

      if (agr_cond != NULL) {
        for (j = 0; j < agr_n; j++) {
          int sj = agr_id[j];
          *GET_LONG (agr_event, &agr_type.fields[j]) = data[i + rcid * sj];
        }

        _cur_type = &agr_type;
        if (!recalc_tree (agr_expr.root, agr_event) || !agr_expr.root->tok.v_int) {
          continue;
        }
        _cur_type = main_type;

        agr_limit--;
      } else {
        if (sort_direction) {
          agr_limit--;
        }
      }

      if (flag & LENGTH_FLAG) {
        length++;
      } else {
        char *e = event_to_str (eq[ans[i]], &sl[-1]);

        int sn = strlen (e);
        char *s = e + sn - 1;

        int tf = !!(sl[-1].nodes_n);

        for (j = 0; j < sl_n; j++) {
          if (tf) {
            *s++ = ',';
          } else {
            tf = 1;
          }

          s += sprintf (s, "\"");
          if (sl[j].alias == NULL) {
            s += sprintf (s, "%s(", sl_name[sl[j].type]);
            int k;
            for (k = 0; k < sl[j].nodes_n; k++) {
              s += sprintf (s, "%s%c", _cur_type->fields[sl[j].nodes[k].id].name, ",)"[k + 1 == sl[j].nodes_n]);
            }

            if (sl[j].nodes_n < 0) {
              s += sprintf (s, "*)");
            }
          } else {
            WRITE_STRING (s, sl[j].alias);
          }
          if (agr_types[j] == t_double) {
            s += sprintf (s, "\":\"%.6lf\"", *(double *)&data[i + rcid * j]);
          } else {
            s += sprintf (s, "\":\"%lld\"", data[i + rcid * j]);
          }
        }
        s += sprintf (s, "}");


        if (fi) {
          debug (",");
        } else {
          fi = 1;
        }
        debug ("%s", e);
      }
    }
    dl_free (data, sizeof (long long) * rcid * sl_n);
    dl_free (perm, sizeof (int) * (cid - offset));

    if (test_mode) {
      fprintf (stderr, "    third section : %.6lf\n", cputime() - tt);
    }
  }

  if (flag & LENGTH_FLAG) {
    debug ("{\"length\":\"%d\"}", length);
  }
  debug ("]");

  for (i = 0; i < expr_n; i++) {
    expression_free (exprs[i]);
  }

  if (agr_cond != NULL) {
    dl_free (agr_event, agr_event_size);
    type_free (&agr_type);
    expression_free (&agr_expr);
  }

  vector_free (q_id);
  vector_free (t_id);

  return debug_buff;
}

void del_spaces (char *s) {
  if (s != NULL) {
    char *r = s;
    int bal = 0;
    while (*s) {
      if (*s != ' ' || bal) {
        *r++ = *s;
      }
      if (*s == '[') {
        bal++;
      }
      if (*s == ']') {
        bal--;
      }
      s++;
    }
    *r = 0;
  }
}

void split (char *s, char **f, int *fn, int limit) {
  int bal = 0;
  int cur = 0;
  int ins = 0;
#define add(s) if (cur < limit) {f[cur++] = s;} else {*fn = 0; return;}

  *fn = 0;
  add (s);
  while (*s) {
    if (*s == '\'') {
      ins ^= 1;
    } else if (*s == '(') {
      if (!ins) {
        bal++;
      }
    } else if (*s == ')') {
      if (!ins) {
        bal--;
      }
    } else if (*s == ',' && bal == 0 && !ins) {
      *s = 0;
      add (s + 1);
    }
    s++;
  }

  *fn = cur;
#undef add
}

void split_by_as (char *s, char **name) {
  int bal = 0;

  *name = NULL;

  while (*s) {
    if (*s == '(' || *s == '[') {
      bal++;
    } else if (*s == ')' || *s == ']') {
      bal--;
    } else if (*s == ' ' && *(s + 1) == 'A' && *(s + 2) == 'S' && *(s + 3) == ' ' && bal == 0) {
      *s = 0;
      *name = s + 4;
      break;
    }
    s++;
  }
}

char *logs_select (char *s, int sn) {
#undef dbg
#define dbg(arg...) if (verbosity > 3) {fprintf (stderr, ## arg);}
  char *s_tmp = s;

  if (sn > MAX_QUERY_LEN) {
    FAIL("too large query, maximum is %d", MAX_QUERY_LEN);
  }
  if (verbosity > 2) {
    fprintf (stderr, "query = %s, len = %d\n", s, sn);
  }

  if (my_verbosity) {
    fprintf (stderr, "QUERY : [%s]\n", s);
  }

  dl_log_add (LOG_DEF, 3, "QUERY : [%s]\n", s);

  enum {kbefore_select = 0, kwhat, kfrom, kwhere, klimit, ksort};

#define keyn 5
  char *key_w[keyn]   = {"SELECT ", " FROM ", " WHERE ", " LIMIT ", " ORDER BY "};
  int   key_len[keyn] = {        7,        6,         7,         7,           10};
  char *key_s[keyn + 1];
  int cur_key = 0;
  char *st = s;
  int i, j;

  key_s[0] = st;
  while (*s) {
    if (strncmp (s, key_w[cur_key], key_len[cur_key]) == 0) {
      *s = 0;
      s = st = s + key_len[cur_key];
      key_s[++cur_key] = st;

      if (cur_key == keyn) {
         break;
      }
    } else {
      s++;
    }
  }

  dbg ("cur_key = %d\n", cur_key);

  if (cur_key < kwhere) {
    FAIL("can't find %sclause in query", key_w[cur_key]);
  }

  for (i = cur_key + 1; i <= keyn; i++) {
    key_s[i] = s_tmp + sn;
  }

  for (i = 0; i <= keyn; i++) {
    if (i != kwhere && i != kwhat) {
      del_spaces (key_s[i]);
    }
  }

  dbg ("before_select = %s\n"
                "what = %s\n"
                "from = %s\n"
               "where = %s\n"
               "limit = %s\n"
             "sort_by = %s\n", key_s[kbefore_select], key_s[kwhat], key_s[kfrom], key_s[kwhere], key_s[klimit], key_s[ksort]);

  if (strlen (key_s[kbefore_select]) != 0) {
    FAIL("query must begin with %s", key_w[0]);
  }

  if (strlen (key_s[kwhat]) == 0) {
    FAIL("empty %sclause", key_w[kwhat - 1]);
  }

  if (strlen (key_s[kfrom]) == 0) {
    FAIL("empty%sclause", key_w[kfrom - 1]);
  }

  if (strlen (key_s[kwhere]) == 0) {
    FAIL("empty%sclause", key_w[kwhere - 1]);
  }

  // FROM
  int type_id = 0, xfid = 0;
  long long val = 0;
  char *type = key_s[kfrom];
  if (type[0] != '*' || type[1] != 0) {
    char *fids[2];
    int fn;
    split (type, fids, &fn, 2);
    if (fn == 0) {
      FAIL ("too much arguments in%s", key_w[kfrom - 1]);
    }

    for (j = 0; j < fn; j++) {
      type = fids[j];

      int eq = -1;
      for (i = 0; type[i]; i++) {
        if (type[i] == '=') {
          eq = i;
        }
      }

      if (eq == -1) {
        if (type_id != 0) {
          FAIL ("double declaration of type in%s", key_w[kfrom - 1]);
        }
        type_id = get_type_id (type);
        if (type_id == 0) {
          FAIL ("can't find type [%s]", type);
        }
      } else {
        if (xfid != 0) {
          FAIL ("double declaration of field in%s", key_w[kfrom - 1]);
        }

        type[eq] = 0;
        xfid = get_field_id (&types[type_id], type);
        if (xfid < -FN) {
          FAIL ("can't find field [%s]", type);
        }
        if (xfid == -FN || xfid >= 0) {
          FAIL ("can't use field [%s] in%s", type, key_w[kfrom - 1]);
        }
        type += eq + 1;
        i -= eq + 1;

        if (xfid == -FN + 1) {
          if (type_id != 0) {
            FAIL ("double declaration of type in%s", key_w[kfrom - 1]);
          }
          type_id = get_type_id (type);
          if (type_id == 0) {
            FAIL ("can't find type [%s]", type);
          }
          xfid = 0;
        } else {
          int len;
          if (read_long (type, &val, &len) != 1 || len != i) {
            FAIL ("can't parse [%s] as long", type);
          }
        }
      }
    }
  }

  dbg ("TYPE_ID = %d, XFID = %d, VAL = %lld\n", type_id, xfid, val);

  //SELECT
  sl_mem_init();

  //TODO replace MAX_SELECT with something smaller
  //TODO set maximal number of aliases to 32
  static select_list sl_buff[MAX_SELECT];
  int flag = 0;
  int sl_n = 1;
  {
    static char *fids[MAX_SELECT];
    static char *fid_names[MAX_SELECT];

    int fn;
    static char *fids2[MAX_SELECT];
    int fn2;
    char *tmp = key_s[kwhat];

    use_default = 0;

    if (strncmp (tmp, "DEFAULT ", 8) == 0) {
      tmp += 8;
      use_default = 1;
    }

    if (strncmp (tmp, "DISTINCT ", 9) == 0) {
      tmp += 9;
      flag |= DISTINCT_FLAG;
    }

    if (strncmp (tmp, "LENGTH ", 7) == 0) {
      tmp += 7;
      flag |= LENGTH_FLAG;
    }

    if (!(flag & DISTINCT_FLAG)) {
      if (strncmp (tmp, "DISTINCT ", 9) == 0) {
        tmp += 9;
        flag |= DISTINCT_FLAG;
      }
    }

    if (strlen (tmp) == 0) {
      FAIL ("empty %sclause", key_w[kwhat - 1]);
    }

    split (tmp, fids, &fn, MAX_SELECT);
    if (fn == 0) {
      FAIL ("too much return fields in %sclause [%s]", key_w[kwhat - 1], tmp);
    }

    for (i = 0; i < fn; i++) {
      split_by_as (fids[i], &fid_names[i]);
      del_spaces (fids[i]);
      del_spaces (fid_names[i]);
      if (fid_names[i] != NULL && !is_name (fid_names[i])) {
        FAIL ("can't use [%s] as field alias", fid_names[i]);
      }
    }

    for (i = 0; i < fn; i++) {
      char *si = fid_names[i] != NULL ? fid_names[i] : fids[i];
      for (j = i + 1; j < fn; j++) {
        char *sj = fid_names[j] != NULL ? fid_names[j] : fids[j];
        if (!strcmp (si, sj)) {
          FAIL ("duplicate return field name [%s]", si);
        }
      }
    }

    //removing agregate functions
    int tn = 0;
    for (i = 0; i < fn; i++) {
      dbg ("[%s]\n", fids[i]);

      int len = strlen (fids[i]);
      if (len == 0) {
        FAIL ("empty field name");
      }

      if (fids[i][len - 1] == ')') {
        char *s = fids[i];

        s[len - 1] = 0;

        select_t tp;
        for (tp = 1; tp <= 4; tp++) {
          int l = strlen (sl_name[tp]);
          if (strncmp (s, sl_name[tp], l) == 0 && s[l] == '(') {
            s += l + 1;
            break;
          }
        }
        if (tp == 5) {
          FAIL ("unknown agregate function [%s]", s);
        }

        if (strlen (s) == 0) {
          FAIL ("empty argument in agregate function %s", sl_name[tp]);
        }

        split (s, fids2, &fn2, MAX_SELECT);
        if (fn2 == 0) {
          FAIL ("too much arguments in agregate function %s(%s)", sl_name[tp], s);
        }

        if (fn2 > 1 && tp != st_count) {
          FAIL ("too much (%d) arguments in agregate function %s(%s)", fn2, sl_name[tp], s);
        }

        select_list *cur = &sl_buff[sl_n++];
        cur->type = tp;
        cur->alias = fid_names[i];

        if (fn2 == 1 && fids2[0][0] == '*' && fids2[0][1] == 0) {
          if (tp != st_count) {
            FAIL ("can't use * as argument in agregate function %s", sl_name[tp]);
          }
          cur->nodes_n = -1;
        } else {
          cur->nodes_n = fn2;
          cur->nodes = sl_mem_alloc (fn2 * sizeof (sl_node));

          for (j = 0; j < fn2; j++) {
            cur->nodes[j].alias = NULL;
            if (fids2[j][0] == '[') {
              int len = strlen (fids2[j]);
              if (fids2[j][len - 1] != ']') {
                FAIL ("wrong expression [%s] in agregate function %s of %squery", fids2[j], sl_name[tp], key_w[kwhat - 1]);
              }
              if (cur->alias == NULL) {
                FAIL ("need alias for agregate function %s with expression [%s]", sl_name[tp], fids2[j]);
              }
              fids2[j][len - 1] = 0;
              cur->nodes[j].type = sln_expr;
              cur->nodes[j].expr = sl_mem_alloc (sizeof (expression));
              cur->nodes[j].expr->s = fids2[j] + 1;
            } else {
              int id = get_field_id (&types[type_id], fids2[j]);
              if (id < -FN) {
                FAIL ("can't find field [%s]", fids2[j]);
              }
              if (tp != st_count && types[type_id].fields[id].type == t_string) {
                FAIL ("can't use string argument [%s] in agregate function %s", fids2[j], sl_name[tp]);
              }
              cur->nodes[j].type = sln_id;
              cur->nodes[j].id = id;
            }
          }
        }
      } else {
        fid_names[tn] = fid_names[i];
        fids[tn++] = fids[i];
      }
    }
    fn = tn;

    select_list *cur = &sl_buff[0];
    cur->type = st_select;

    if (fn == 1 && fids[0][0] == '*' && fids[0][1] == 0) {
      cur->nodes_n = -1;
      if (fid_names[0] != NULL) {
        FAIL ("can't use alias [%s] for *", fid_names[0]);
      }
    } else if (fn == 1 && fids[0][0] == '*' && fids[0][1] == 't' && fids[0][2] == 0) {
      cur->nodes_n = -2;
      if (fid_names[0] != NULL) {
        FAIL ("can't use alias [%s] for *t", fid_names[0]);
      }
    } else {
      cur->nodes_n = fn;
      cur->nodes = sl_mem_alloc (fn * sizeof (sl_node));

      for (i = 0; i < fn; i++) {
        cur->nodes[i].alias = fid_names[i];
        if (fids[i][0] == '[') {
          int len = strlen (fids[i]);
          if (fids[i][len - 1] != ']') {
            FAIL ("wrong expression [%s] in %squery", fids[i], key_w[kwhat - 1]);
          }
          if (cur->nodes[i].alias == NULL) {
            FAIL ("need alias for expression [%s]", fids[i]);
          }
          fids[i][len - 1] = 0;
          cur->nodes[i].type = sln_expr;
          cur->nodes[i].expr = sl_mem_alloc (sizeof (expression));
          cur->nodes[i].expr->s = fids[i] + 1;
        } else {
          int id = get_field_id (&types[type_id], fids[i]);
          dbg ("look for %s = %d\n", fids[i], id);
          if (id < -FN) {
            FAIL ("can't find field [%s]", fids[i]);
          }
          cur->nodes[i].type = sln_id;
          cur->nodes[i].id = id;
        }
      }
    }
  }

  if (verbosity > 3) {
    for (i = 0; i < sl_n; i++) {
      select_list *cur = &sl_buff[i];
      dbg ("tp = %d(%s), %d:{", cur->type, sl_name[cur->type], cur->nodes_n);

      for (j = 0; j < cur->nodes_n; j++) {
        if (cur->nodes[j].type != sln_expr) {
          dbg ("%s%c", types[type_id].fields[cur->nodes[j].id].name, ",}"[j + 1 == cur->nodes_n]);
        } else {
          dbg ("%s%c", cur->nodes[j].expr->s, ",}"[j + 1 == cur->nodes_n]);
        }
      }

      dbg ("\n");
    }
  }

  // LIMIT
  int offset = 0, limit = -1, time_limit = 0;
  if (key_s[klimit][0]) {
    char *lim = key_s[klimit];
    int len;
    if (*lim == 'T') {
      lim++;

      for (i = 0; lim[i] && lim[i] != ','; i++) {
      }
      char c = lim[i];
      lim[i] = 0;
      if (sscanf (lim, "%d%n", &time_limit, &len) != 1 || len != i) {
        FAIL ("can't parse as time [%s]", lim);
      }
      lim += i;
      if (c && lim[1]) {
        lim++;
      }
    }

    if (*lim) {
      int comma = -1;
      for (i = 0; lim[i]; i++) {
        if (lim[i] == ',') {
          comma = i;
        }
      }
      if (comma == -1) {
        if (sscanf (lim, "%d%n", &limit, &len) != 1 || len != i) {
          FAIL ("can't parse limit [%s]", lim);
        }
      } else {
        if (sscanf (lim, "%d,%d%n", &offset, &limit, &len) != 2 || len != i) {
          FAIL ("can't parse limit [%s]", lim);
        }
      }
    }
  }

  if (time_limit < 0) {
    time_limit += now;
  }

  dbg ("\n\nLIMIT = %d, OFFSET = %d, TIME_LIMIT = %d\n", limit, offset, time_limit);

  // WHERE
  char *tmp[2] = {NULL, NULL};
  int tmp_n;
  split (key_s[kwhere], tmp, &tmp_n, 2);
  if (tmp_n == 0) {
    FAIL ("too much expressions in%s", key_w[kwhere - 1]);
  }
  if (strlen (tmp[0]) == 0) {
    FAIL ("can't use empty condition in%s", key_w[kwhere - 1]);
  }
  if (tmp_n == 2 && strlen (tmp[1]) == 0) {
    FAIL ("can't use empty agregate condition in%s", key_w[kwhere - 1]);
  }

  // SORT_BY
  int sort_field = -1, sort_direction = 1;
  if (key_s[ksort][0]) {
    char *sort_part[2] = {NULL, NULL};
    int sort_part_n;
    split (key_s[ksort], sort_part, &sort_part_n, 2);
    if (sort_part_n == 0) {
      FAIL ("too much parts in%s", key_w[ksort - 1]);
    }
    if (!is_name (sort_part[0])) {
      FAIL ("wrong sort alias name [%s] in%s", sort_part[0], key_w[ksort - 1]);
    }

    for (i = 1; i < sl_n; i++) {
      if (sl_buff[i].alias != NULL && !strcmp (sl_buff[i].alias, sort_part[0])) {
        sort_field = i - 1;
      }
    }
    if (sort_field == -1) {
      FAIL ("can't find agregate function with alias [%s] for sorting", sort_part[0]);
    }

    if (sort_part_n == 2) {
      if (strcmp (sort_part[1], "ASC") && strcmp (sort_part[1], "DESC")) {
        FAIL ("wrong sort direction [%s] specified in%s, only 'ASC' and 'DESC' are acceptable.", sort_part[1], key_w[kwhere - 1]);
      }
      if (!strcmp (sort_part[1], "DESC")) {
        sort_direction = -1;
      }
    }
  } else {
    sort_direction = 0;
  }

  char *res = run_select (sl_buff, sl_n, flag, type_id, xfid, val, tmp[0], tmp[1], limit, offset, time_limit, sort_field, sort_direction);

  // debug_error: internal error level
  // sp_errno   : 'common/string-processing.h' error level
  int error_code = max (sp_errno, debug_error);
  if (error_code) {
    debug_error = 0;
    sp_errno = 0;
    switch (error_code) {
      case 1:
        FAIL ("length of result is too big");
      case 2:
        FAIL ("buffer overflow while calculating expression");
      default:
        FAIL ("unknown error");
    }
  }

  return res;
#undef FAIL
#undef keyn
#undef dbg
}

/*
 *
 *           BINLOG
 *
 */

char *do_create_type (const char *s) {
  int text_len = strlen (s);
  if (text_len >= MAX_EVENT_SIZE) {
    return dl_pstr ("Type description is too long (maximal is %d symbols).", MAX_EVENT_SIZE - 1);
  }
  int comma_cnt = 0;
  int i;
  for (i = 0; i < text_len; i++) {
    comma_cnt += s[i] == ',';
  }
  if (comma_cnt != 2) {
    return dl_pstr ("Wrong number of parameters %d instead of 3.", comma_cnt + 1);
  }

  struct lev_logs_create_type *E =
    alloc_log_event (LEV_LOGS_CREATE_TYPE, offsetof (struct lev_logs_create_type, text) + 1 + text_len, 0);

  E->text_len = text_len;
  memcpy (E->text, s, sizeof (char) * (text_len + 1));

  return create_type (E);
}

char *do_add_field (const char *s) {
  int text_len = strlen (s);
  if (text_len >= MAX_EVENT_SIZE) {
    return dl_pstr ("New field description is too long (maximal is %d symbols).", MAX_EVENT_SIZE - 1);
  }

  struct lev_logs_add_field *E =
    alloc_log_event (LEV_LOGS_ADD_FIELD, offsetof (struct lev_logs_add_field, text) + 1 + text_len, 0);

  E->text_len = text_len;
  memcpy (E->text, s, sizeof (char) * (text_len + 1));

  return add_field_log (E);
}


int do_add_event (char *type_s, int params[FN - 2], char *desc) {
  int type_id = get_type_id (type_s);
  if (verbosity > 3) {
    fprintf (stderr, "type_id = %d\n", type_id);
  }
  if (!type_id) {
    return 0;
  }

  int text_len = strlen (desc);
  if (text_len >= MAX_EVENT_SIZE) {
    return 0;
  }

  struct lev_logs_add_event *E =
    alloc_log_event (LEV_LOGS_ADD_EVENT, offsetof (struct lev_logs_add_event, text) + 1 + text_len, 0);

  E->std_val[0] = type_id;
  memcpy (E->std_val + 1, params, sizeof (int) * (FN - 2));
  E->text_len = text_len;
  memcpy (E->text, desc, sizeof (char) * (text_len + 1));
  if (verbosity > 3) {
    fprintf (stderr, "??%s\n", E->text);
  }

  return add_event (E);
}


int do_set_color (int field_num, long long field_value, int cnt, int and_mask, int xor_mask) {
  if (field_num < 0 || field_num >= FN || cnt <= 0 || cnt >= MAX_COLOR_CNT) {
    return 0;
  }

  struct lev_logs_set_color *E =
    alloc_log_event (LEV_LOGS_SET_COLOR + field_num, sizeof (struct lev_logs_set_color), 0);

  E->field_value = field_value;
  E->cnt = cnt;
  E->and_mask = and_mask;
  E->xor_mask = xor_mask;

  return set_color (E);
}


int logs_replay_logevent (struct lev_generic *E, int size);

int init_logs_data (int schema) {
  replay_logevent = logs_replay_logevent;
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

  was = 1;
  old_log_split_min = log_split_min;
  old_log_split_max = log_split_max;
  old_log_split_mod = log_split_mod;

  log_schema = LOGS_SCHEMA_V1;
  init_logs_data (log_schema);
}

static int logs_le_start (struct lev_start *E) {
  if (E->schema_id != LOGS_SCHEMA_V1) {
    return -1;
  }

  log_split_min = E->split_min;
  log_split_max = E->split_max;
  log_split_mod = E->split_mod;
  assert (log_split_mod > 0 && log_split_min >= 0 && log_split_min + 1 == log_split_max && log_split_max <= log_split_mod);

  try_init_local_uid();

  return 0;
}

int log_events;

vector(ll, log_position);
vector(int, log_timestamp);
vector(int, log_crc32);

vector(ll, time_table_log_position);
vector(int, time_table_log_timestamp);
vector(int, time_table_log_crc32);

#define INDEX_STEP 100000

int logs_replay_logevent (struct lev_generic *E, int size) {
  int s;

  if (time_table_log_position_size == 0 || vector_back (time_table_log_timestamp) + TIME_STEP <= log_read_until) {
    vector_pb (time_table_log_position, log_cur_pos());
    vector_pb (time_table_log_timestamp, log_read_until);
    relax_log_crc32 (0);
    vector_pb (time_table_log_crc32, ~log_crc32_complement);
  }

  if (log_events++ % INDEX_STEP == 0) {
//    fprintf (stderr, "Index_step %d: %d %lld %d\n", log_position_size, log_events, log_cur_pos(), log_read_until);
    vector_pb (log_position, log_cur_pos());
    vector_pb (log_timestamp, log_read_until);
    relax_log_crc32 (0);
    vector_pb (log_crc32, ~log_crc32_complement);
  }

  switch (E->type) {
  case LEV_START:
    if (size < 24 || E->b < 0 || E->b > 4096) { return -2; }
    s = 24 + ((E->b + 3) & -4);
    if (size < s) { return -2; }
    return logs_le_start ((struct lev_start *) E) >= 0 ? s : -1;
  case LEV_NOOP:
  case LEV_TIMESTAMP:
  case LEV_CRC32:
  case LEV_ROTATE_FROM:
  case LEV_ROTATE_TO:
  case LEV_TAG:
    return default_replay_logevent (E, size);
  case LEV_LOGS_CREATE_TYPE:
    if (size < (int)sizeof (struct lev_logs_create_type)) {
      return -2;
    }
    s = ((struct lev_logs_create_type *) E)->text_len;
    if (s < 0) {
      return -4;
    }

    s += 1 + offsetof (struct lev_logs_create_type, text);
    if (size < s) {
      return -2;
    }
    create_type ((struct lev_logs_create_type *)E);
    return s;
  case LEV_LOGS_ADD_FIELD:
    if (size < (int)sizeof (struct lev_logs_add_field)) {
      return -2;
    }
    s = ((struct lev_logs_add_field *) E)->text_len;
    if (s < 0) {
      return -4;
    }

    s += 1 + offsetof (struct lev_logs_add_field, text);
    if (size < s) {
      return -2;
    }
    add_field_log ((struct lev_logs_add_field *)E);
    return s;
  case LEV_LOGS_ADD_EVENT:
    if (size < (int)sizeof (struct lev_logs_add_event)) {
      return -2;
    }
    s = ((struct lev_logs_add_event *) E)->text_len;
    if (s < 0) {
      return -4;
    }

    s += 1 + offsetof (struct lev_logs_add_event, text);
    if (size < s) {
      return -2;
    }
    add_event ((struct lev_logs_add_event *)E);
    return s;
  case LEV_LOGS_SET_COLOR ... LEV_LOGS_SET_COLOR + FN - 1:
    STANDARD_LOG_EVENT_HANDLER(lev_logs, set_color);
  }

  fprintf (stderr, "unknown log event type %08x at position %lld\n", E->type, log_cur_pos());

  return -1;
}


/*
 *
 *         LOGS INDEX
 *
 */


long long get_index_header_size (index_header *header) {
  return sizeof (index_header);
}

int load_index (kfs_file_handle_t Index) {
  if (Index == NULL) {
    assert(!dump_index_mode);
    header.magic = LOGS_INDEX_MAGIC;

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

    header.log_timestamp0 = 0;

    int i;
    for (i = 0; i < FN; i += 1 + std_t[i]) {
      if (std_t[i]) {
        map_ll_int_init (&color_ll[i]);
      } else {
        map_int_int_init (&color_int[i]);
      }
    }

    return 0;
  }

  fd[0] = Index->fd;
  int offset = Index->offset;
  //fsize = Index->info->file_size - Index->offset;
  //read header
  assert (lseek (fd[0], offset, SEEK_SET) == offset);

  int size = sizeof (index_header);
  int r = read (fd[0], &header, size);
  if (r != size) {
    fprintf (stderr, "error reading header from index file: read %d bytes instead of %d at position %d: %m\n", r, size, offset);
  }
  assert (r == size);

  if (header.magic != LOGS_INDEX_MAGIC) {
    fprintf (stderr, "bad logs index file header\n");
    assert (0);
  }

  assert (header.log_split_max);
  log_split_min = header.log_split_min;
  log_split_max = header.log_split_max;
  log_split_mod = header.log_split_mod;

  header_size = get_index_header_size (&header);

  assert (header.types_data_len < MAX_BUFF);
  assert (read (fd[0], buff, header.types_data_len) == header.types_data_len);

  char *s = buff;
  int i;
  for (i = 0; i < header.total_types; i++) {
    assert ( (s - buff) < header.types_data_len);
    int l = strlen (s);
    create_type_buf (s, l);
    s += l + 2;
  }

  assert ("Outdated executable file. Update me or delete index. " && header.index_type <= INDEX_TYPE);
  if (header.index_type >= 1) {
    int sum = sizeof (long long) + 2 * sizeof (int);
    assert (header.time_table_size * sum < MAX_BUFF);
    assert (read (fd[0], buff, header.time_table_size * sum) == header.time_table_size * sum);
    s = buff;
    long long x;
    int y, z;

    for (i = 0; i < header.time_table_size; i++) {
      READ_LONG(s, x);
      READ_INT(s, y);
      READ_INT(s, z);

      vector_pb (time_table_log_position, x);
      vector_pb (time_table_log_timestamp, y);
      vector_pb (time_table_log_crc32, z);
    }

    if (header.index_type >= 2) {
      int colors_sizes[FN];
      assert (read (fd[0], colors_sizes, FN * sizeof (int)) == FN * sizeof (int));

      for (i = 0; i < FN; i += 1 + std_t[i]) {
        if (colors_sizes[i]) {
          assert (read (fd[0], buff, colors_sizes[i]) == colors_sizes[i]);

          if (std_t[i]) {
            map_ll_int_decode (&color_ll[i], buff, colors_sizes[i]);
          } else {
            map_int_int_decode (&color_int[i], buff, colors_sizes[i]);
          }
        } else {
          if (std_t[i]) {
            map_ll_int_init (&color_ll[i]);
          } else {
            map_int_int_init (&color_int[i]);
          }
        }
      }
    }
  } else {
    assert (!dump_index_mode);
  }

  if (verbosity > 1) {
    fprintf (stderr, "header loaded %d %d %d\n", log_split_min, log_split_max, log_split_mod);
    fprintf (stderr, "ok\n");
  }

  if (dump_index_mode) {
    for (i = 0; i < time_table_log_position_size; i++) {
      printf ("%lld\t%d\t%d\n", time_table_log_position[i], time_table_log_timestamp[i], time_table_log_crc32[i]);
    }
    exit(0);
  }

  return 1;
}

void load_dump (char *dump_name) {
  dl_zin dump;
  static char E_buff[100000];
  struct lev_logs_add_event *E = (struct lev_logs_add_event *)E_buff;
  E->type = LEV_LOGS_ADD_EVENT;

  dl_open_file (17, dump_name, 0);
  dl_zin_init (&dump, 17, 5000000);

  int size;
  long long bad_events = 0;
  while (dl_zin_read (&dump, buff, sizeof (int) * 2) == sizeof (int) * 2) {
    now = ((int *)buff)[0];
    size = ((int *)buff)[1];
    assert (dl_zin_read (&dump, ((char *)E) + offsetof (struct lev_logs_add_event, std_val), size) == size);

    if (!add_event (E)) {
      bad_events++;
    }
  }
  if (bad_events > 0) {
    fprintf (stderr, "WARNING!!! %lld events wasn't recognized. Perhaps you are using outdated index.\n", bad_events);
  }
  assert (dump.r_left == 0);

  dl_zin_free (&dump);
  assert (close (fd[17]) >= 0);
}

int save_index (void) {
  char *newidxname = NULL;

  assert (binlog_disabled);

  int i = 0;
  if (log_position_size > MAX_EV / INDEX_STEP) {
    i = log_position_size - 1 - MAX_EV / INDEX_STEP;
  }
  while (i + 1 < log_position_size && log_cur_pos() > max_memory + log_position[i + 1]) {
    i++;
  }

  long long log_cur_pos_ = log_position[i];
//  fprintf (stderr, "Generating new index using data from %d/%d: time = %d/%d, pos = %lld/%lld\n", i, log_position_size, log_timestamp[i], log_last_ts, log_cur_pos_, log_cur_pos());

  if (engine_snapshot_replica) {
    newidxname = get_new_snapshot_name (engine_snapshot_replica, log_cur_pos_, engine_snapshot_replica->replica_prefix);
  }

  if (!newidxname || newidxname[0] == '-') {
    fprintf (stderr, "cannot write index: cannot compute its name\n");
    exit (1);
  }

  if (!binlog_disabled) {
    relax_write_log_crc32();
  } else {
    relax_log_crc32 (0);
  }

  if (i == 0 && log_cur_pos_ != 0) {
    fprintf (stderr, "skipping generation of new snapshot %s for position %lld: snapshot for this position already exists\n",
      newidxname, log_cur_pos_);
    return 0;
  }

  index_header header;
  memset (&header, 0, sizeof (header));

  header.log_pos1 = log_cur_pos_;
  header.log_timestamp = log_timestamp[i];
  header.log_split_min = log_split_min;
  header.log_split_max = log_split_max;
  header.log_split_mod = log_split_mod;
  header.log_pos1_crc32 = log_crc32[i];

  header.log_pos0 = log_cur_pos();
  header.log_timestamp0 = log_last_ts;
  header.log_pos0_crc32 = ~log_crc32_complement;

  if (verbosity > 0) {
    fprintf (stderr, "creating index %s at log position %lld\n", newidxname, log_cur_pos_);
  }

  fd[1] = open (newidxname, O_CREAT | O_TRUNC | O_WRONLY | O_EXCL, 0660);
  if (fd[1] < 0) {
    fprintf (stderr, "cannot create new index file %s: %m\n", newidxname);
    exit (1);
  }

  header.magic = LOGS_INDEX_MAGIC;
  header.created_at = time (NULL);

  long long fCurr = get_index_header_size (&header);
  assert (lseek (fd[1], fCurr, SEEK_SET) == fCurr);

  header.total_types = 0;
  char *s = buff;
  for (i = 1; i < MAX_TYPE; i++) {
    if (types[i].name != NULL) {
      assert ((s - buff) + (MAX_NAME_LEN + 10) * 33 < MAX_BUFF);

      WRITE_ALL (s, "%s,%d,", types[i].name, i);

      int j, written = 0;
      for (j = common_fields; j < types[i].field_i; j++) {
        if (written) {
          WRITE_CHAR (s, ';');
        }
        written = 1;
        WRITE_ALL (s, "%s:%s", types[i].fields[j].name, ttt[(int)types[i].fields[j].type]);
      }
      *s++ = 0;
      *s++ = 0;
      header.total_types++;
    }
  }
  header.types_data_len = (s - buff) * sizeof (char);
  assert (write (fd[1], buff, header.types_data_len) == header.types_data_len);

  header.index_type = INDEX_TYPE;

  if (INDEX_TYPE >= 1) {
    int sum = sizeof (long long) + 2 * sizeof (int);
    header.time_table_size = time_table_log_timestamp_size;

    assert (header.time_table_size * sum < MAX_BUFF);

    s = buff;
    for (i = 0; i < header.time_table_size; i++) {
      WRITE_LONG(s, time_table_log_position[i]);
      WRITE_INT(s, time_table_log_timestamp[i]);
      WRITE_INT(s, time_table_log_crc32[i]);
    }

    assert (write (fd[1], buff, header.time_table_size * sum) == header.time_table_size * sum);

    if (INDEX_TYPE >= 2) {
      int colors_sizes[FN];

      for (i = 0; i < FN; i += 1 + std_t[i]) {
        if (std_t[i]) {
          colors_sizes[i] = map_ll_int_used (&color_ll[i]) > 0 ? map_ll_int_get_encoded_size (&color_ll[i]) : 0;
        } else {
          colors_sizes[i] = map_int_int_used (&color_int[i]) > 0 ? map_int_int_get_encoded_size (&color_int[i]) : 0;
        }
      }

      assert (write (fd[1], colors_sizes, FN * sizeof (int)) == FN * sizeof (int));

      for (i = 0; i < FN; i += 1 + std_t[i]) {
        if (colors_sizes[i]) {
          if (std_t[i]) {
            assert (map_ll_int_encode (&color_ll[i], buff, MAX_BUFF) == colors_sizes[i]);
          } else {
            assert (map_int_int_encode (&color_int[i], buff, MAX_BUFF) == colors_sizes[i]);
          }

          assert (write (fd[1], buff, colors_sizes[i]) == colors_sizes[i]);
        }
      }
    }
  }

  //write header
  assert (lseek (fd[1], 0, SEEK_SET) == 0);
  assert (write (fd[1], &header, sizeof (header)) == sizeof (header));

  assert (fsync (fd[1]) >= 0);
  assert (close (fd[1]) >= 0);

  if (rename_temporary_snapshot (newidxname)) {
    fprintf (stderr, "cannot rename new index file from %s: %m\n", newidxname);
    unlink (newidxname);
    exit (1);
  }

  print_snapshot_name (newidxname);
  return 0;
}


void load_stat_queries (void) {
  if (stat_queries_file == NULL) {
    return;
  }

  FILE *f = fopen (stat_queries_file, "r");
  if (f == NULL) {
    fprintf (stderr, "Can't open file %s with queries.\n", stat_queries_file);
    exit (1);
  }
  char query[MAX_QUERY_LEN];
  for (stats_cnt = 0; stats_cnt < MAX_STATS; stats_cnt++) {
    if (fgets (query, MAX_QUERY_LEN, f) == NULL) {
      break;
    }

    if (strlen (query) <= 1) {
      stats_cnt--;
      continue;
    }
    query[strlen (query) - 1] = 0;

    static char *qs[3];
    int qn;
    split (query, qs, &qn, 2);
    if (qn == 0) {
      fprintf (stderr, "Too much commas in query %s.\n", query);
      exit (1);
    }

    stat_type[stats_cnt] = get_type_id (qs[0]);
    if (stat_type[stats_cnt] == 0) {
      fprintf (stderr, "Can't find type [%s].\n", qs[0]);
      exit (1);
    }

    char *err = expression_compile (&stat_expr[stats_cnt], qs[1], &types[stat_type[stats_cnt]]);
    if (err != NULL) {
      fprintf (stderr, "Cannot compile expression [%s] : error [%s]\n", qs[1], err);
      exit (1);
    }

    if (IS_STR(stat_expr[stats_cnt].root->tok.dtype)) {
      fprintf (stderr, "Cannot convert result of query[%s] (string) to int\n", qs[1]);
      exit (1);
    }
    stat_expr[stats_cnt].root = tree_conv (stat_expr[stats_cnt].root, v_int);
  }
  if (stats_cnt == MAX_STATS) {
    fprintf (stderr, "Max number of stat_queries %d exceeded. Aborting.\n", MAX_STATS);
    exit (1);
  }
}

void print_stats (void) {
  int i;
  for (i = 0; i < stats_cnt; i++) {
    printf ("%lld\n", stat_result[i]);
  }
}


int init_all (kfs_file_handle_t Index) {
  assert (sizeof (long long) == sizeof (double));

  log_ts_exact_interval = 1;

  MAX_EVENT_MEM = max_memory - query_memory;
  MAX_EV = MAX_EVENT_MEM / mean_event_size;
  evm_mx = MAX_EVENT_MEM - 5;
  assert (MAX_EVENT_MEM >= (1 << 26));

  vector_init (log_position);
  vector_init (log_timestamp);
  vector_init (log_crc32);

  vector_init (time_table_log_position);
  vector_init (time_table_log_timestamp);
  vector_init (time_table_log_crc32);

  map_ll_int_init (&map_type_id);

  if (!write_only) {
    events_mem = dl_malloc (MAX_EVENT_MEM);
    eq = dl_malloc (sizeof (event *) * MAX_EV);
    ans = dl_malloc (sizeof (int) * MAX_EV);
  }

  int i;
  for (i = 0; i < FN; i += 1 + std_t[i]) {
    std_desc[i].type = i - FN;
    std_desc[i].name = field_names[i];

    empty_desc[i].type = i - FN;
    empty_desc[i].name = "";
  }

  /* init virtual event */
  types[0].name = dl_strdup ("virtual_event");
  types[0].fields = dl_malloc (sizeof (field_desc) * FN);
  for (i = 0; i < FN; i += 1 + std_t[i]) {
    types[0].fields[i] = std_desc[i];
  }
  types[0].fields += FN;
  for (i = 0; i < common_fields; i++) {
    add_field (types, common_field_names[i], common_field_types[i]);
  }

  for (i = 0; i < FN; i += 1 + std_t[i]) {
    if (std_t[i]) {
      map_ll_int_init (&q_st_ll[i]);
    } else {
      map_int_int_init (&q_st_int[i]);
    }
  }

  int f = load_index (Index);

  if (dump_mode) {
    load_stat_queries();

    assert (INDEX_TYPE);
    if (header.index_type == 0) {
      fprintf (stderr, "You need to generate new index first.\n");
      exit (1);
    }
    int i;
    for (i = 0; i + 1 < time_table_log_timestamp_size && time_table_log_timestamp[i + 1] < from_ts; i++) {
    }

    if (strlen (dump_query) == 0) {
      fprintf (stderr, "Dump query can't be empty.\n");
      exit (1);
    }

    char *qs[3];
    int qn;
    split (dump_query, qs, &qn, 2);
    if (qn == 0) {
      fprintf (stderr, "Too much commas in dump query.\n");
      exit (1);
    }

    if (qn == 2 && strcmp (qs[1], "1")) {
      dump_query = qs[1];
    } else {
      dump_query = NULL;
    }

    int pos;
    if (sscanf (qs[0], "%d%n", &dump_type, &pos) != 1 || qs[0][pos]) {
      fprintf (stderr, "Can't parse [%s] as integer.\n", qs[0]);
      exit (1);
    }

    if (!(0 <= dump_type && dump_type < MAX_TYPE)) {
      fprintf (stderr, "Dump_type [%d] not in range [0;%d].\n", dump_type, MAX_TYPE - 1);
      exit (1);
    }

    if (dump_type != 0 && types[dump_type].name == NULL) {
      fprintf (stderr, "Dump_type [%d] is unknown.\n", dump_type);
      exit (1);
    }

    if (dump_query != NULL) {
      char *err = expression_compile (&dump_expr, dump_query, &types[dump_type]);
      if (err != NULL) {
        fprintf (stderr, "Cannot compile expression [%s] : error [%s]\n", dump_query, err);
        exit (1);
      }

      if (IS_STR(dump_expr.root->tok.dtype)) {
        fprintf (stderr, "Cannot convert result of dump_query[%s] (string) to int\n", dump_query);
        exit (1);
      }
      dump_expr.root = tree_conv (dump_expr.root, v_int);
    }

    jump_log_ts = time_table_log_timestamp[i];
    jump_log_pos = time_table_log_position[i];
    jump_log_crc32 = time_table_log_crc32[i];
//    fprintf (stderr, "%d %lld %d\n", jump_log_ts, jump_log_pos, jump_log_crc32);
  } else if (index_mode && INDEX_TYPE >= 1 && header.index_type == 0) {
    jump_log_ts = 0;
    jump_log_pos = 0;
    jump_log_crc32 = 0;
  } else if (write_only && !index_mode) {
    jump_log_ts = header.log_timestamp0;
    jump_log_pos = header.log_pos0;
    jump_log_crc32 = header.log_pos0_crc32;
  } else {
    jump_log_ts = header.log_timestamp;
    jump_log_pos = header.log_pos1;
    jump_log_crc32 = header.log_pos1_crc32;
  }
//  fprintf (stderr, "%d %lld %d\n", jump_log_ts, jump_log_pos, jump_log_crc32);

  if (f) {
    try_init_local_uid();
  }
  return f;
}

extern char *history_q[MAX_HISTORY + 1];
extern int history_l, history_r;

void free_all (void) {
  if (verbosity > 0 || test_mode) {
    vector_free (log_position);
    vector_free (log_timestamp);
    vector_free (log_crc32);

    vector_free (time_table_log_position);
    vector_free (time_table_log_timestamp);
    vector_free (time_table_log_crc32);

    while (history_l != history_r) {
      dl_strfree (history_q[history_l]);
      history_q[history_l++] = 0;
      if (history_l > MAX_HISTORY) {
        history_l = 0;
      }
    }

    while (eq_n) {
      event_free();
    }
    assert (events_memory == 0);

    if (!write_only) {
      dl_free (events_mem, MAX_EVENT_MEM);
      dl_free (eq, sizeof (event *) * MAX_EV);
      dl_free (ans, sizeof (int) * MAX_EV);
    }

    int i;
    for (i = 0; i < FN; i += 1 + std_t[i]) {
      if (std_t[i]) {
        map_ll_int_free (&color_ll[i]);
        map_ll_int_free (&q_st_ll[i]);
      } else {
        map_int_int_free (&color_int[i]);
        map_int_int_free (&q_st_int[i]);
      }
    }

    for (i = 0; i < MAX_TYPE; i++) {
      if (types[i].name != NULL) {
        type_free (&types[i]);
      }
    }

    map_ll_int_free (&map_type_id);

    if (dump_mode && dump_query != NULL) {
      expression_free (&dump_expr);
    }

    fprintf (stderr, "Memory left: %lld\n", dl_get_memory_used());
//    while (1) {}
    assert (dl_get_memory_used() == 0);
  }
}
