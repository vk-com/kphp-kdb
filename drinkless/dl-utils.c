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

#define _FILE_OFFSET_BITS 64

#include <assert.h>
#include <execinfo.h>
#include <fcntl.h>
#include <signal.h>
#include <stdarg.h>
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/ioctl.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#include "crc32.h"

#include "dl-utils.h"

//TODO very-very bad
extern int now;
extern int verbosity;
extern char *progname;


char *fnames[MAX_FN];
int fd[MAX_FN];
long long fsize[MAX_FN];
off_t fpos[MAX_FN];
char fread_only[MAX_FN];


int dl_open_file (int x, const char *fname, int creat) {
  if (x < 0 || x >= MAX_FN) {
    fprintf (stderr, "%s: cannot open %s, bad local fid %d: %m\n", progname, fname, x);
    return -1;
  }

  fnames[x] = dl_strdup (fname);
  int options;
  if (creat > 0) {
    options = O_RDWR | O_CREAT;
    if (creat == 2) {
      options |= O_TRUNC;
    }
  } else {
    fread_only[x] = 1;
    options = O_RDONLY;
  }

  fd[x] = open (fname, options, 0600);
  if (creat < 0 && fd[x] < 0) {
    fprintf (stderr, "%s: cannot open %s: %m\n", progname, fname);
    return -1;
  }
  if (fd[x] < 0) {
    fprintf (stderr, "%s: cannot open %s: %m\n", progname, fname);
    exit (1);
  }
  fsize[x] = lseek (fd[x], 0, SEEK_END);
  if (fsize[x] < 0) {
    fprintf (stderr, "%s: cannot seek %s: %m\n", progname, fname);
    exit (2);
  }
  lseek (fd[x], 0, SEEK_SET);
  if (verbosity) {
    fprintf (stderr, "opened file %s, fd=%d, size=%lld\n", fname, fd[x], fsize[x]);
  }
  fpos[x] = 0;
  return fd[x];
}

off_t dl_file_seek (int x, off_t offset, int whence) {
  assert (0 <= x && x < MAX_FN);
  assert (fd[x] != -1);

  off_t res = lseek (fd[x], offset, whence);
  if (res != (off_t)-1) {
    fpos[x] = res;
  }
  return res;
}

void dl_close_file (int x) {
  assert (0 <= x && x < MAX_FN);
  assert (fd[x] != -1);

  if (!fread_only[x]) {
    assert (fsync (fd[x]) >= 0);
  } else {
    fread_only[x] = 0;
  }
  assert (close (fd[x]) >= 0);
  fd[x] = -1;

  fsize[x] = 0;
  fpos[x] = 0;
  dl_free (fnames[x], strlen (fnames[x]) + 1);
  fnames[x] = NULL;
}

void dl_zout_raw_init (dl_zout *f) {
  memset (f, 0, sizeof (dl_zout));
}

void dl_zout_free_buffer (dl_zout *f) {
  if (f->buf != NULL) {
    dl_free (f->buf, f->buf_len);
    f->buf = NULL;
    f->buf_len = 0;
  }
}

void dl_zout_set_buffer_len (dl_zout *f, int len) {
  assert (f->ptr == f->buf);

  if (f->buf_len != len) {
    dl_zout_free_buffer (f);
    f->buf_len = len;
    assert ("Too small buffer for output" && f->buf_len > 8);
    f->buf = dl_malloc ((size_t)f->buf_len);
  }

  f->ptr = f->buf;
  f->left = f->buf_len;
}

void dl_zout_set_crc32_flag (dl_zout *f, int flag) {
  f->use_crc32 = flag;
  f->crc32_complement = 0xFFFFFFFF;
}

void dl_zout_reset_crc32 (dl_zout *f) {
  //TODO: make it with f->crc32_ptr
  dl_zout_flush (f);
  f->crc32_complement = 0xFFFFFFFF;
}

unsigned int dl_zout_get_crc32 (dl_zout *f) {
  //TODO: make it with f->crc32_ptr
  dl_zout_flush (f);
  return ~f->crc32_complement;
}

void dl_zout_set_file_id (dl_zout *f, int fid) {
  f->id = fid;
  f->written = 0;
}

void dl_zout_init (dl_zout *f, int id, int len) {
  dl_zout_raw_init (f);

  dl_zout_set_file_id (f, id);
  dl_zout_set_buffer_len (f, len);
  dl_zout_set_crc32_flag (f, 1);
}

static int dl_zout_write_impl (dl_zout *f, const void *src, int len) {
  assert (write (fd[f->id], src, (size_t)len) == len);
  fpos[f->id] += len;

  if (f->use_crc32) {
    f->crc32_complement = crc32_partial (src, len, f->crc32_complement);
  }
  f->written += len;
  return len;
}

void dl_zout_flush (dl_zout *f) {
  ssize_t d = f->ptr - f->buf;
  if (d) {
    dl_zout_write_impl (f, f->buf, d);

    f->ptr = f->buf;
    f->left = f->buf_len;
  }
}

void *dl_zout_alloc_log_event (dl_zout *f, int type, int bytes) {
  int adj_bytes = -bytes & 3;
  bytes = (bytes + 3) & -4;

  if (bytes > f->left) {
    dl_zout_flush (f);
  }

  assert (bytes >= 4 && bytes <= f->left);

  void *EV = f->ptr;

  f->ptr += bytes;
  f->left -= bytes;

  *(unsigned int *)EV = (unsigned int)type;

  if (adj_bytes) {
    memset (f->ptr - adj_bytes, adj_bytes, (size_t)adj_bytes);
  }

  return EV;
}

struct lev_crc32 *dl_zout_write_lev_crc32 (dl_zout *f) {
  dl_zout_flush (f);

  struct lev_crc32 *E = dl_zout_alloc_log_event (f, LEV_CRC32, sizeof (struct lev_crc32));
  E->timestamp = now;
  E->pos = f->written;
  E->crc32 = ~f->crc32_complement;

  return E;
}

int dl_zout_log_event_write (dl_zout *f, const void *src, int len) {
  int adj_bytes = -len & 3;

  while (len) {
    int cur = len;
    if (f->left < len) {
      cur = f->left;
    }
    memcpy (f->ptr, src, (size_t)cur);
    f->ptr += cur;
    f->left -= cur;

    if ((len -= cur)) {
      dl_zout_flush (f);
      src += cur;
    }
  }

  if (f->left < adj_bytes) {
    dl_zout_flush (f);
  }
  memset (f->ptr, adj_bytes, (size_t)adj_bytes);
  f->ptr += adj_bytes;
  f->left -= adj_bytes;

  return len;
}

int dl_zout_write (dl_zout *f, const void *src, int len) {
  if (unlikely (len > f->buf_len)) {
    dl_zout_flush (f);
    return dl_zout_write_impl (f, src, len);
  }
  int save_len = len;
  while (len) {
    int cur = len;
    if (f->left < len) {
      cur = f->left;
    }
    memcpy (f->ptr, src, (size_t)cur);
    f->ptr += cur;
    f->left -= cur;

    if ((len -= cur)) {
      dl_zout_flush (f);
      src += cur;
    }
  }
  return save_len;
}

off_t dl_zout_pos (dl_zout *f) {
  return fpos[f->id] + (f->ptr - f->buf);
}

void dl_zout_free (dl_zout *f) {
  dl_zout_flush (f); //save for legacy
  dl_zout_free_buffer (f);
}

void dl_zin_init (dl_zin *f, int id, int len) {
  f->buf_len = len;
  f->id = id;

  assert ("Too small buffer for input" && f->buf_len > 8);
  f->ptr = f->buf = dl_malloc ((size_t)f->buf_len);
  f->left = 0;

  off_t cur = lseek (fd[f->id], 0, SEEK_CUR),
        end = lseek (fd[f->id], 0, SEEK_END);
  lseek (fd[f->id], cur, SEEK_SET);
  f->r_left = end - cur;
}

static inline int dl_zin_flush (dl_zin *f) {
  assert (f->left == 0);
  if (likely(f->r_left)) {
    int cur = (int) min ((off_t)f->buf_len, f->r_left);
    assert (read (fd[f->id], f->buf, (size_t)cur) == cur);
    fpos[f->id] += cur;
    f->r_left -= cur;
    f->left = cur;
    f->ptr = f->buf;
    return cur;
  } else {
    return 0;
  }
}


int dl_zin_read (dl_zin *f, void *dest, int len) {
  int tmp = len;
  while (len) {
    int cur = len;
    if (cur > f->left) {
      cur = f->left;
    }
    memcpy (dest, f->ptr, (size_t)cur);
    f->ptr += cur;
    f->left -= cur;

    if ((len -= cur) && !dl_zin_flush (f)) {
      return tmp - len;
    }
    dest += cur;
  }
  return tmp;
}

off_t dl_zin_pos (dl_zin *f) {
  return fpos[f->id] - f->left;
}

void dl_zin_free (dl_zin *f) {
  dl_free (f->buf, (size_t)f->buf_len);
}


int dl_get_stack_depth (void) {
  #define max_depth 50
  #define uncounted_depth 12
  static void *tmp[max_depth];
  int res = backtrace (tmp, max_depth) - uncounted_depth;
  return res < 0 ? 0 : res;
}


#if DL_DEBUG_MEM >= 1
#  define MEM_POS  {\
  void *buffer[64]; \
  int nptrs = backtrace (buffer, 4); \
  fprintf (stderr, "\n------- Stack Backtrace -------\n"); \
  backtrace_symbols_fd (buffer + 1, nptrs - 1, 2); \
  fprintf (stderr, "-------------------------------\n"); \
}
#else
#  define MEM_POS
#endif

struct dl_log_t {
  char s[DL_LOG_SIZE];
  char v[DL_LOG_SIZE];
  int f, i;
  int verbosity_stderr, verbosity_log;
} dl_log[LOG_ID_MX];

void dl_log_set_verb (int log_id, int verbosity_stderr, int verbosity_log) {
  dl_log[log_id].verbosity_stderr = verbosity_stderr;
  dl_log[log_id].verbosity_log = verbosity_log;
}

void dl_log_add (int log_id, int verb, const char *s, ...) {
  assert (0 <= log_id && log_id < LOG_ID_MX);

  static char tmp[DL_LOG_SIZE];
  va_list args;

  if (verb > dl_log[log_id].verbosity_stderr && verb > dl_log[log_id].verbosity_log) {
    return;
  }

  va_start (args, s);

  static time_t old_timestamp = -1;
  time_t timestamp = now ? now : time (NULL);
  static int len = 0;
  if (timestamp != old_timestamp) {
    old_timestamp = timestamp;

    len = dl_print_local_time (tmp, DL_LOG_SIZE, timestamp);
    assert (len < DL_LOG_SIZE);
  }

  vsnprintf (tmp + len, (size_t)(DL_LOG_SIZE - len), s, args);

  va_end (args);

  if (verb <= dl_log[log_id].verbosity_stderr) {
    fprintf (stderr, "%s", tmp);
  }

  if (verb <= dl_log[log_id].verbosity_log) {
    char *t = tmp;
    while (*t) {
      dl_log[log_id].s[dl_log[log_id].i] = *t++;
      dl_log[log_id].v[dl_log[log_id].i++] = (char)verb;
      if (dl_log[log_id].i == DL_LOG_SIZE) {
        dl_log[log_id].i = 0;
        dl_log[log_id].f = 1;
      }
    }
  }
}

void dl_log_dump (int log_id, int verb) {
  assert (0 <= log_id && log_id < LOG_ID_MX);
  int i = (dl_log[log_id].f ? dl_log[log_id].i : 0);

  do {
    if (dl_log[log_id].s[i] && verb >= dl_log[log_id].v[i]) {
      putc (dl_log[log_id].s[i], stderr);
    }
    if (++i == DL_LOG_SIZE) {
      i = 0;
    }
  } while (dl_log[log_id].i != i);
}

int dl_log_dump_to_buf (int log_id, int verb_min, int verb_max, char *buf, int buf_n, int line_mx)  {
  assert (0 <= log_id && log_id < LOG_ID_MX);

  int i = dl_log[log_id].i, bi = 0;

  do {
    if (--i == -1) {
      i = DL_LOG_SIZE - 1;
    }

    char c = dl_log[log_id].s[i];
    if (unlikely (c == 0)) {
      break;
    }
    if (verb_max >= dl_log[log_id].v[i] && dl_log[log_id].v[i] >= verb_min) {
      if (c == '\n') {
        if (--line_mx < 0) {
          break;
        }
      }
      buf[bi++] = c;
    }
  } while (dl_log[log_id].i != i && bi + 1 < buf_n);
  buf[bi] = 0;
  i = 0;
  int j = bi - 1;
  while (i < j) {
    char t = buf[i];
    buf[i] = buf[j];
    buf[j] = t;
    i++, j--;
  }

  return bi;
}


static void *stack_bottom_ptr = NULL;
void *dl_cur_stack() {
  int x;
  void *ptr = &x;
  return ptr;
}
void dl_init_stack_size() {
  stack_bottom_ptr = dl_cur_stack();
}

size_t dl_get_stack_size() {
  size_t res = (char *)dl_cur_stack() - (char *)stack_bottom_ptr;
  return -res;
}

void dl_runtime_handler (const int sig) {
  //signal (sig, SIG_DFL);

  fprintf (stderr, "%s caught, terminating program\n", sig == SIGSEGV ? "SIGSEGV" : "SIGABRT");
  fprintf (stderr, "----------------- LOG BEGINS -----------------\n");
  dl_log_dump (LOG_DEF, 0x7f);
  fprintf (stderr, "-----------------  HISTORY   -----------------\n");
  dl_log_dump (LOG_HISTORY, 0x7f);
  fprintf (stderr, "-----------------  WARNINGS  -----------------\n");
  dl_log_dump (LOG_WARNINGS, 0x7f);
  fprintf (stderr, "-----------------  CRITICAL  -----------------\n");
  dl_log_dump (LOG_CRITICAL, 0x7f);
  fprintf (stderr, "----------------- LOG   ENDS -----------------\n");
  dl_print_backtrace();
  dl_print_backtrace_gdb();

  _exit (EXIT_FAILURE);
}

void dl_set_debug_handlers (void) {
//  signal (SIGFPE, dl_runtime_handler);
  dl_signal (SIGSEGV, dl_runtime_handler);
  dl_signal (SIGABRT, dl_runtime_handler);

  //TODO: move this somewhere else
#ifdef DL_DEBUG_OUT
  dl_log_set_verb (LOG_DEF, 3, 3);
#else
  dl_log_set_verb (LOG_DEF, 0, 3);
#endif
  dl_log_set_verb (LOG_HISTORY, 0, 0x7F);
  dl_log_set_verb (LOG_WARNINGS, 0, 9);
  dl_log_set_verb (LOG_CRITICAL, 0, 9);
}


int dl_print_local_time (char *buf, int buf_size, time_t timestamp) {
  struct tm t;
  assert (localtime_r (&timestamp, &t));
  assert (buf_size > 0);
  return snprintf (buf, (size_t)buf_size, "[%4d-%02d-%02d %02d:%02d:%02d local] ", t.tm_year + 1900, t.tm_mon + 1, t.tm_mday, t.tm_hour, t.tm_min, t.tm_sec);
}

long long dl_strhash (const char *s) {
  unsigned long long h = 0;

  while (*s) {
    h = h * HASH_MUL + (unsigned long long)*s++;
  }

  return (long long)h;
}

char *dl_strstr_kmp (const char *a, int *kmp, const char *b) {
  int i, j = 0;
  for (i = 0; b[i]; i++) {
    while (j && a[j] != b[i])  {
      j = kmp[j];
    }
    if (a[j] == b[i]) {
      j++;
    }

    if (!a[j]) {
      return (char *)(b + i - j + 1);
    }
  }
  return NULL;
}

void dl_kmp (const char *a, int *kmp) {
  if (kmp == NULL) {
    return;
  }
  int i, j = 0;
  kmp[0] = 0;
  for (i = 0; a[i]; i++) {
    while (j && a[i] != a[j]) {
      j = kmp[j];
    }
    if (i != j && a[i] == a[j]) {
      j++;
    }
    kmp[i + 1] = j;
  }
}

char *dl_strstr (const char *a, const char *b) {
  return (char *)strstr (a, b);
}
