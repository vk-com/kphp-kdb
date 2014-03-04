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

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <time.h>
#include <assert.h>
#include <limits.h>
#include <float.h>

double precise_now;
#define DL_CAT_(a, b) a ## b
#define DL_CAT(a, b) DL_CAT_ (a, b)
typedef long long ll;


typedef struct {
  enum {stat_timer_off = 0, stat_timer_on} state;
  double on_timestamp;
} stat_timer_t;

#define STAT_DESC_LEN 100

typedef enum {ll_t, double_t} type_t;

#define TYPE_NAME(type) DL_CAT (type, _t)

#define STAT_UNIT_NAME(type) struct DL_CAT (type, _stat_unit_t)
#define STAT_UNIT_NAME_X(type) DL_CAT (type, _stat_unit_x_t)
#define STAT_UNIT_DFLT(type) DL_CAT (type, _stat_unit_dflt)
#define DEF_STAT_UNIT(type)       \
typedef STAT_UNIT_NAME(type) STAT_UNIT_NAME_X (type);\
STAT_UNIT_NAME (type)  {                  \
  long long cnt;                  \
  double timestamp;               \
  type sum, max_val, min_val;     \
  char max_desc[STAT_DESC_LEN],   \
       min_desc[STAT_DESC_LEN];   \
};\
STAT_UNIT_NAME (type) STAT_UNIT_DFLT (type)
#define INIT_STAT_UNIT_DFLT(type, type_min, type_max) ({\
  STAT_UNIT_DFLT (type).cnt = 0;\
  STAT_UNIT_DFLT (type).sum = 0;\
  STAT_UNIT_DFLT (type).min_val = type_max;\
  STAT_UNIT_DFLT (type).max_val = type_min;\
})

#define STAT_TIMED_NAME(type) struct DL_CAT (type, _stat_timed_t)
#define STAT_TIMED_DFLT(type) DL_CAT (type, _stat_timed_dflt)
#define DEF_STAT_TIMED(type) \
STAT_TIMED_NAME(type) { \
  STAT_UNIT_NAME (type) first, second;\
  double timeout;\
};\
STAT_TIMED_NAME (type) STAT_TIMED_DFLT (type);
#define INIT_STAT_TIMED_DFLT(type)({\
  STAT_TIMED_DFLT (type).first = STAT_UNIT_DFLT (type);\
  STAT_TIMED_DFLT (type).second = STAT_UNIT_DFLT (type);\
})


#define TIMED_STAT_CNT 2
double stat_rotate_timeout[TIMED_STAT_CNT] = {2, 10};

#define STAT_NAME(type) struct DL_CAT (type, _stat_t)
#define STAT_DFLT(type) DL_CAT (type, _stat_dflt)
#define DEF_STAT(type) \
STAT_NAME (type) {             \
  type_t type_id;              \
  STAT_UNIT_NAME_X(type) *dflt;\
  stat_timer_t timer;          \
  const char *format;          \
  const char *desc;            \
  STAT_UNIT_NAME (type) stat;  \
  STAT_TIMED_NAME (type) timed_stat[TIMED_STAT_CNT];\
};\
STAT_NAME (type) STAT_DFLT (type)
#define INIT_STAT_DFLT(type, type_format) ({\
  STAT_DFLT (type).type_id = TYPE_NAME (type);\
  STAT_DFLT (type).format = type_format;\
  STAT_DFLT (type).stat = STAT_UNIT_DFLT (type);\
  STAT_DFLT (type).dflt = &STAT_UNIT_DFLT (type);\
  int i;\
  for (i = 0; i < TIMED_STAT_CNT; i++) {\
    STAT_DFLT (type).timed_stat[i] = STAT_TIMED_DFLT (type);\
    STAT_DFLT (type).timed_stat[i].timeout = stat_rotate_timeout[i];\
  }\
})

#define DEF_STAT_ALL(type)\
  DEF_STAT_UNIT (type);\
  DEF_STAT_TIMED (type);\
  DEF_STAT (type);


#define INIT_STAT_DFLT_ALL(type, type_min, type_max, type_format)\
  INIT_STAT_UNIT_DFLT (type, type_min, type_max);\
  INIT_STAT_TIMED_DFLT (type);\
  INIT_STAT_DFLT (type, type_format);


DEF_STAT_ALL (ll);
DEF_STAT_ALL (double);

void init_stat_dflt() {
  INIT_STAT_DFLT_ALL (ll, LLONG_MIN, LLONG_MAX, "%Ld");
  INIT_STAT_DFLT_ALL (double, DBL_MIN, DBL_MAX, "%.4lf");
}

#define STAT_DESC_IMPL(name, val, desc) ({   \
  (name).cnt++;                           \
  (name).sum += val;                      \
  if ((name).min_val > val) {             \
    (name).min_val = val;                 \
    strncpy ((name).min_desc, desc, STAT_DESC_LEN);\
  }                                     \
  if ((name).max_val < val) {             \
    (name).max_val = val;                 \
    strncpy ((name).max_desc, desc, STAT_DESC_LEN);\
  }                                     \
})


#define STAT_DESC(name, val, desc) ({\
  STAT_DESC_IMPL (name.stat, val, desc);\
  int i;\
  for (i = 0; i < TIMED_STAT_CNT; i++) {\
    STAT_DESC_IMPL (name.timed_stat[i].first, val, desc);\
    STAT_DESC_IMPL (name.timed_stat[i].second, val, desc);\
  }\
})

#define STAT(name, val) STAT_DESC (name, val, "unknown")

#define STAT_TIMER_ON(name) ({                 \
  assert (name.timer.state == stat_timer_off); \
  name.timer.state = stat_timer_on;            \
  name.timer.on_timestamp = precise_now;       \
})

#define STAT_TIMER_OFF_DESC(name, desc) ({\
  assert (name.timer.state == stat_timer_on);\
  STAT_DESC (name, precise_now - name.timer.on_timestamp, desc);\
  name.timer.state = stat_timer_off;\
})

#define STAT_TIMER_OFF(name) STAT_TIMER_OFF_DESC (name, "unknown");

#define MAX_STAT_CNT 1000
void *stats[MAX_STAT_CNT];
int stats_cnt;
#define INIT_STAT(name, type) \
  name = STAT_DFLT (type);\
  name.desc = #name;\
  assert (stats_cnt < MAX_STAT_CNT);\
  stats[stats_cnt++] = (void *)&name;

#define STAT_AVG(name) ((name).sum / (name).cnt)
#define STAT_MIN(name) ((name).min_val)
#define STAT_MIN_DESC(name) ((name).min_desc)
#define STAT_MAX(name) ((name).max_val)
#define STAT_MAX_DESC(name) ((name).max_desc)
#define STAT_SUM(name) ((name).sum)
#define STAT_CNT(name) ((name).cnt)

#define STAT_ROTATE_IMPL(name, now, dflt, name_str) ({\
  if (name.second.timestamp + name.timeout <= now) {\
    name.first = name.second;\
    name.second = dflt;\
    name.second.timestamp = now;\
  }\
})
#define STAT_ROTATE(name, now) ({\
  int roti;\
  for (roti = 0; roti < TIMED_STAT_CNT; roti++) {\
    STAT_ROTATE_IMPL ((name).timed_stat[roti], now, *(name).dflt, (name).desc);\
  }\
})

typedef struct {
  const char *desc, *age, *cnt, *max_val, *max_desc, *min_val, *min_desc, *sum, *avg;
} stat_text_t;


#define STAT_BUF_LEN 1000
static char stat_buf[STAT_BUF_LEN];
static const char *empty_s = "";
static int stat_buf_len;
#define STAT_BUF_CLEAR() (stat_buf_len = 0)
#define STAT_PRINT(x, val) ({\
  const char *res = empty_s;\
  if (stat_buf_len < STAT_BUF_LEN) {\
    char *cur = stat_buf + stat_buf_len;\
    int new_stat_buf_len = stat_buf_len + snprintf (cur, STAT_BUF_LEN - stat_buf_len, x, val) + 1;\
    if (new_stat_buf_len < STAT_BUF_LEN) {\
      res = cur;\
      stat_buf_len = new_stat_buf_len;\
    }\
  }\
  res;\
})

#define STAT_OUT_(s, callback, x, name_str, now) ({\
  STAT_BUF_CLEAR();\
  stat_text_t res;\
  res.desc = name_str;\
  res.cnt = STAT_PRINT ("%Ld", s.cnt);\
  res.max_val = STAT_PRINT (x, s.max_val);\
  res.max_desc = s.max_desc;\
  res.min_val = STAT_PRINT (x, s.min_val);\
  res.min_desc = s.min_desc;\
  res.sum = STAT_PRINT (x, s.sum);\
  res.avg = STAT_PRINT ("%.4lf", (double)s.sum / s.cnt);\
  if (s.timestamp != 0) {\
    res.age =  STAT_PRINT ("%.2lfs", (now - s.timestamp));\
  } else {\
    res.age = "";\
  }\
  callback (&res);\
})

#define STAT_OUT(name, callback, now) ({\
  STAT_OUT_ ((name).stat, callback, (name).format, (name).desc, now);\
  int outi;\
  for (outi = 0; outi < TIMED_STAT_CNT; outi++) {\
    STAT_OUT_ ((name).timed_stat[outi].first, callback, (name).format, (name).desc, now);\
  }\
})



typedef void (*stat_print_callback_f) (stat_text_t *);
void print_stats (stat_print_callback_f callback, double now) {
  int i;
  for (i = 0; i < stats_cnt; i++) {
    type_t type_id = *(type_t *)stats[i];
    switch (type_id) {
      case ll_t:
        STAT_OUT (*(STAT_NAME (ll) *)stats[i], callback, now);
        break;
      case double_t:
        STAT_OUT (*(STAT_NAME (double) *)stats[i], callback, now);
        break;
      default:
        assert (0);
    }
  }
}

void rotate_stats (double now) {
  int i;
  for (i = 0; i < stats_cnt; i++) {
    type_t type_id = *(type_t *)stats[i];
    switch (type_id) {
      case ll_t:
        STAT_ROTATE (*(STAT_NAME (ll) *)stats[i], now);
        break;
      case double_t:
        STAT_ROTATE (*(STAT_NAME (double) *)stats[i], now);
        break;
      default:
        assert (0);
    }
  }
}

/** Example of usage **/
void print_stat_callback (stat_text_t *s) {
  fprintf (stderr, "%s %s [sum = %s] [avg = %s] [max = %s|%s]\n", s->desc, s->age, s->sum, s->avg, s->max_val, s->max_desc);
}

void update_now() {
  precise_now = (double)clock() / CLOCKS_PER_SEC;
}


STAT_NAME (ll) floyd_cnt, floyd_n;
STAT_NAME (double) floyd_gen, floyd_run;

void init_stats (void) {
  init_stat_dflt();
  INIT_STAT (floyd_cnt, ll);
  INIT_STAT (floyd_n, ll);
  INIT_STAT (floyd_gen, double);
  INIT_STAT (floyd_run, double);
}

int main (void) {
  init_stats();

  while (1) {
    int test_n = 100, test_i, i, j, k;
    for (test_i = 0; test_i < test_n; test_i++) {
      STAT (floyd_cnt, 1);
      STAT_TIMER_ON (floyd_gen);
      int n = rand() % 500;

      char tmp[20];
      sprintf (tmp, "len=%d", n);

      int **a = malloc (sizeof (int *) * n);
      for (i = 0; i < n; i++) {
        a[i] = malloc (sizeof (int) * n);
        for (j = 0; j < n; j++) {
          a[i][j] = rand() % 100;
        }
      }
      update_now();
      STAT_TIMER_OFF_DESC (floyd_gen, tmp);

      STAT_DESC (floyd_n, n, tmp);

      STAT_TIMER_ON (floyd_run);
      for (k = 0; k < n; k++) {
        for (i = 0; i < n; i++) {
          for (j = 0; j < n; j++) {
            int x = a[i][k] + a[k][j];
            if (x < a[i][j]) {
              a[i][j] = x;
            }
          }
        }
      }
      update_now();
      STAT_TIMER_OFF_DESC (floyd_run, tmp);

      for (i = 0; i < n; i++) {
        free (a[i]);
      }
      free (a);

      rotate_stats (precise_now);
    }

    fprintf (stderr, "--------------------\n");
    print_stats (print_stat_callback, precise_now);
  }
  return 0;
}
