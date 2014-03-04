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

#pragma once

#ifdef __cplusplus
extern "C" {
#endif

#include <signal.h>
#include <stddef.h>
#include <time.h>
#include <unistd.h>

#include "server-functions.h"

#include "dl-default.h"
#include "dl-utils-lite.h"


#include <unistd.h>

#include "kdb-binlog-common.h"

#include "dl-default.h"


#define MAX_FN 2005

/* Buffered input/output */
typedef struct {
  char *buf, *ptr;
  int buf_len, id, left;
  long long written;
  int use_crc32;
  unsigned int crc32_complement;
} dl_zout;

void dl_zout_init (dl_zout *f, int id, int len);
void dl_zout_flush (dl_zout *f);
void *dl_zout_alloc_log_event (dl_zout *f, int type, int bytes);
struct lev_crc32 *dl_zout_write_lev_crc32 (dl_zout *f);
int dl_zout_log_event_write (dl_zout *f, const void *src, int len);
int dl_zout_write (dl_zout *f, const void *src, int len);
off_t dl_zout_pos (dl_zout *f);
void dl_zout_set_crc32_flag (dl_zout *f, int flag);
void dl_zout_reset_crc32 (dl_zout *f);
unsigned int dl_zout_get_crc32 (dl_zout *f);
void dl_zout_free (dl_zout *f);

#define DL_ZOUT(f, val)                     \
({                                          \
  if (unlikely((f)->left < sizeof (val))) { \
    dl_zout_flush (f);                      \
  }                                         \
  *(__typeof (val) *)(f)->ptr = val;        \
  (f)->ptr += sizeof (val);                 \
  (f)->left -= sizeof (val);                \
  sizeof (val);                             \
})

typedef struct {
  char *buf, *ptr;
  int buf_len, id, left;
  off_t r_left;
} dl_zin;

void dl_zin_init (dl_zin *f, int id, int len);
int dl_zin_read (dl_zin *f, void *dest, int len);
off_t dl_zin_pos (dl_zin *f);
void dl_zin_free (dl_zin *f);


#define DL_ZIN(f, val)                       \
({                                           \
  int res = 0;                               \
  if (unlikely((f)->left < sizeof (val))) {  \
    res = dl_zin_read(f, &val, sizeof (val));\
  } else {                                   \
    res = sizeof (val);                      \
    val = *(__typeof (val) *)(f)->ptr;       \
    (f)->ptr += sizeof (val);                \
    (f)->left -= sizeof (val);               \
  }                                          \
  res;                                       \
})

extern char *fnames[MAX_FN];
extern int fd[MAX_FN];
extern long long fsize[MAX_FN];
extern off_t fpos[MAX_FN];

int dl_open_file (int x, const char *fname, int creat);
void dl_close_file (int x);
off_t dl_file_seek (int x, off_t offset, int whence);


//#define DL_DEBUG_MEM 1

//#define DL_DEBUG

#ifdef DL_DEBUG
//*
#define DL_DEBUG_HST
#define DL_DEBUG_DEPTH
#define DL_DEBUG_OUT
//*/
#endif

#ifndef HASH_MUL
#define HASH_MUL (999983)
#endif

#define SDV(x, y) x = (typeof (x))((char *)(x) + (y))
//TODO why not #define SDV(x, y) x += y ?

#define WRITE_DOUBLE(s, x) *(double *)(s) = x; SDV (s, sizeof (double))
#define WRITE_LONG(s, x) *(long long *)(s) = x; SDV (s, sizeof (long long))
#define WRITE_INT(s, x) *(int *)(s) = x; SDV (s, sizeof (int))
#define WRITE_UINT(s, x) *(unsigned int *)(s) = x; SDV (s, sizeof (unsigned int))
#define WRITE_SHORT(s, x) *(short *)(s) = x; SDV (s, sizeof (short))
#define WRITE_CHAR(s, x) *s++ = x;
#define WRITE_T(s, x, type) *(type *)(s) = x, SDV (s, sizeof (type))
#define WRITE_STRING(s, x) SDV (s, sprintf (s, "%s", x))
#define WRITE_ALL(s, args...) SDV (s, sprintf (s, ## args))

#define READ_DOUBLE(s, x) x = *(double *)(s); SDV (s, sizeof (double))
#define READ_LONG(s, x) x = *(long long *)(s); SDV (s, sizeof (long long))
#define READ_INT(s, x) x = *(int *)(s); SDV (s, sizeof (int))
#define READ_UINT(s, x) x = *(unsigned int *)(s); SDV (s, sizeof (unsigned int))
#define READ_SHORT(s, x) x = *(short *)(s); SDV (s, sizeof (short))
#define READ_CHAR(s, x) x = *s++;
#define READ_STRING(s, x) x = s; SDV (s, strlen (x) + 1)
#define READ_T(s, x, type) x = *(type *)(s); SDV (s, sizeof (type))

#define MAX_FN 2005

#ifndef max
#define max(x, y) ((x) > (y) ? (x) : (y))
#endif
#ifndef min
#define min(x, y) ((x) < (y) ? (x) : (y))
#endif

#define dl_swap(a, b) { typeof (a) t_tmp___ = a; a = b; b = t_tmp___; }

#define dl_abs(x) ({  \
  typeof (x) y_tmp___ = x;  \
  y_tmp___ >= 0 ? y_tmp___ : -y_tmp___; \
})

#define cputime() (clock() * 1.0 / CLOCKS_PER_SEC)//get_utime (CLOCK_PROCESS_CPUTIME_ID)
#define mytime() (dl_get_utime (CLOCK_MONOTONIC))

#define INIT double cmd_time = -mytime()
#define RETURN(x, y) {                       \
  cmd_time += mytime() - 1e-6;               \
  int result_value = (y);                    \
  if (result_value != -2) {                  \
    cmd_ ## x++;                             \
  }                                          \
  cmd_ ## x ## _time += cmd_time;            \
  if (cmd_time > max_cmd_ ## x ## _time) {   \
    max_cmd_ ## x ## _time = cmd_time;       \
  }                                          \
  return result_value;                       \
}


#define STANDARD_LOG_EVENT_HANDLER(x, y)                \
  if (size < (int)sizeof (struct DL_ADD_SUFF (x, y))) { \
    return -2;                                          \
  }                                                     \
  y ((struct DL_ADD_SUFF (x, y) *)E);                   \
  return sizeof (struct DL_ADD_SUFF (x, y));

#define DUMP_LOG_EVENT_HANDLER(z, xy)                   \
  if (size < (int)sizeof (struct xy)) {                 \
    return -2;                                          \
  }                                                     \
  dl_zout_write (&z, E, sizeof (struct xy));            \
  return sizeof (struct xy);


#define DL_LOG_SIZE 1000000

enum {LOG_DEF, LOG_HISTORY, LOG_WARNINGS, LOG_CRITICAL, LOG_ID_MX};
void dl_log_set_verb (int log_id, int verbosity_stderr, int verbosity_log);
void dl_log_add (int log_id, int verb, const char *s, ...) __attribute__ ((format (printf, 3, 4)));
void dl_log_dump (int log_id, int verb);
int dl_log_dump_to_buf (int log_id, int verb_min, int verb_max, char *buf, int buf_n, int line_mx);

void dl_set_debug_handlers (void);
int dl_get_stack_depth (void);

void dl_init_stack_size();
size_t dl_get_stack_size();

#ifdef DL_DEBUG_DEPTH
#  define dbg(str, args...) dl_log_add (LOG_DEF, 3, "%*s" str, dl_get_stack_depth(), "", ##args)
#else
#  define dbg(str, args...) dl_log_add (LOG_DEF, 3, "[%s:%d:%s] " str, __FILE__, __LINE__, __func__, ##args)
#endif

#ifdef DL_DEBUG_HST
#  define hst(args...) dl_log_add (LOG_HISTORY, 1, ##args), dl_log_add (LOG_DEF, 1, ##args)
#else
#  define hst(args...) dl_log_add (LOG_HISTORY, 1, ##args)
#endif



#define wrn(args...) dl_log_add (LOG_WARNINGS, 0, ##args)

#define critical(args...) dl_log_add (LOG_CRITICAL, 0, ##args)

#define GET_LOG                                                                                   \
  if (key_len >= 3 && !strncmp (key, "log", 3)) {                                                 \
    int log_id, lines, verb_max, verb_min, nn;                                                    \
    nn = sscanf (key + 3, "%d,%d,%d,%d", &log_id, &lines, &verb_max, &verb_min);                  \
    if (nn < 1) {                                                                                 \
      log_id = LOG_DEF;                                                                           \
    }                                                                                             \
    if (nn < 2) {                                                                                 \
      lines = 1000;                                                                               \
    }                                                                                             \
    if (nn < 3) {                                                                                 \
      verb_max = 0x7F;                                                                            \
    }                                                                                             \
    if (nn < 4) {                                                                                 \
      verb_min = 0;                                                                               \
    }                                                                                             \
    if (0 <= log_id && log_id < LOG_ID_MX) {                                                      \
      int len = dl_log_dump_to_buf (log_id, verb_min, verb_max, buf, MAX_VALUE_LEN, lines);       \
      return_one_key (c, old_key, buf, len);                                                      \
    }                                                                                             \
    return 0;                                                                                     \
  }

#define SET_LOG_VERBOSITY                                                                         \
  if (key_len >= 17 && !strncmp (key, "set_log_verbosity", 17)) {                                 \
    int log_id, verbosity_stderr, verbosity_log;                                                  \
    int nn = sscanf (key + 17, "%d,%d,%d", &log_id, &verbosity_stderr, &verbosity_log);           \
    if (nn < 1) {                                                                                 \
      log_id = LOG_DEF;                                                                           \
    }                                                                                             \
    if (nn < 2) {                                                                                 \
      verbosity_stderr = 0;                                                                       \
    }                                                                                             \
    if (nn < 3) {                                                                                 \
      verbosity_log = 0;                                                                          \
    }                                                                                             \
    if (0 <= log_id && log_id < LOG_ID_MX && 0 <= verbosity_stderr && verbosity_stderr <= 0x7F && \
        0 <= verbosity_log && verbosity_log <= 0x7F) {                                            \
      dl_log_set_verb (log_id, verbosity_stderr, verbosity_log);                                  \
                                                                                                  \
      snprintf (buf, MAX_VALUE_LEN, "Verbosity of log %d is set to %d (stderr) and "              \
                         "%d (in memory log).", log_id, verbosity_stderr, verbosity_log);         \
                                                                                                  \
      return_one_key (c, key, buf, strlen (buf));                                                 \
    }                                                                                             \
    return 0;                                                                                     \
  }


int dl_print_local_time (char *buf, int buf_size, time_t timestamp);

long long dl_strhash (const char *s);
char *dl_strstr (const char *a, const char *b);
char *dl_strstr_kmp (const char *a, int *kmp, const char *b);
void dl_kmp (const char *a, int *kmp);

//TODO: move to separate file

#define DL_PREDICATE(base) DL_ADD_SUFF (dl_predicate, base)

#define DL_BEGIN_PREDICATE(name) \
  typedef struct DL_ADD_SUFF (name, t) {\
    int (*eval) (struct DL_ADD_SUFF (name, t) *self, void *data);
#define DL_END_PREDICATE(name) } name;

DL_BEGIN_PREDICATE(predicate)
DL_END_PREDICATE(predicate)

#define DL_CALL(p, func, args...) (p->func (p, ##args))

#undef dbg
#define dbg(...)

/*** PROFILE (from VV) ***/

//#if defined(__i386__)
//static inline unsigned long long rdtsc(void)
//{
  //unsigned long long int x;
     //__asm__ volatile ("rdtsc" : "=A" (x));
     //return x;
//}
//#elif defined(__x86_64__)
//static inline unsigned long long rdtsc(void)
//{
  //unsigned hi, lo;
  //__asm__ __volatile__ ("rdtsc" : "=a"(lo), "=d"(hi));
  //return ( (unsigned long long)lo)|( ((unsigned long long)hi)<<32 );
//}
//#endif


#define DECLARE_STAT(x) long long x ## _cnt; long long x ## _ticks; double x ## _time;
#define TACT_SPEED (1e-6 / 2266.0)
#define PRINT_STAT(x) "%20s %20lld %20lld %10.6f %10.6f %lld\n", #x, stats.x ## _cnt, stats.x ## _ticks, (double)stats.x ## _ticks * TACT_SPEED, stats.x ## _time, (long long)(stats.x ## _ticks / (stats.x ## _cnt ? stats.x ## _cnt : 1))


//#define DEBUG_TIMER
#define DEBUG_TICKS
#define DEBUG_CNT
#define PRINT_DEBUG_INFO


#ifdef DEBUG_TIMER
  #ifdef DEBUG_TICKS
    #define START_TIMER(x) stats.x ## _time -= get_utime_monotonic (); stats.x ## _ticks -= rdtsc ();
    #define END_TIMER(x) stats.x ## _time += get_utime_monotonic (); stats.x ## _ticks += rdtsc () + 1;
  #else
    #define START_TIMER(x) stats.x ## _time -= get_utime_monotonic ();
    #define END_TIMER(x) stats.x ## _time += get_utime_monotonic ();
  #endif
#else
  #ifdef DEBUG_TICKS
    #define START_TIMER(x) stats.x ## _ticks -= rdtsc ();
    #define END_TIMER(x) stats.x ## _ticks += rdtsc () + 1;
  #else
    #define START_TIMER(x) ;
    #define END_TIMER(x) ;
  #endif
#endif

#ifdef DEBUG_TICKS
  #define START_TICKS(x) stats.x ## _ticks -= rdtsc ();
  #define END_TICKS(x) stats.x ## _ticks += rdtsc () + 1;
#else
  #define START_TICKS(x) ;
  #define END_TICKS(x) ;
#endif

#ifdef DEBUG_CNT
#define ADD_CNT(x) stats.x ## _cnt ++;
#else
#define ADD_CNT(x) ;
#endif

#ifdef __cplusplus
}
#endif
