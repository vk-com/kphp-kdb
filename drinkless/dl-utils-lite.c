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

#include "dl-utils-lite.h"

#include <assert.h>
#include <execinfo.h>
#include <fcntl.h>
#include <stdarg.h>
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

static size_t dl_memory_used = 0;
inline size_t predict_malloc_memory (size_t x) {
  if (x == (size_t)0) {
    return x;//this is very bad prediction, but right
  }
  size_t res = x + 3 * sizeof (long) - 1;
  res -= res % (2 * sizeof (long));
  if (res < 4 * sizeof (long)) {
    return 4 * sizeof (long);
  }
  return res;
}

void *dl_malloc (size_t x) {
  dl_memory_used += predict_malloc_memory (x);

#ifdef DL_DEBUG_MEM
  MEM_POS
  void *res = malloc (x);
  fprintf (stderr, "a\t%lld\t%p\n", (long long)x, res);
  return res;
#else
  return malloc (x);
#endif
}
void *dl_malloc0 (size_t x) {
  dl_memory_used += predict_malloc_memory (x);

#ifdef DL_DEBUG_MEM
  MEM_POS
  void *res = calloc (x, 1);
  fprintf (stderr, "c\t%lld\t%p\n", (long long)x, res);
  return res;
#else
  return calloc (x, 1);
#endif
}
void *dl_realloc (void *p, size_t x, size_t old) {
  dl_memory_used += predict_malloc_memory (x) - predict_malloc_memory (old);

#ifdef DL_DEBUG_MEM
  MEM_POS
  fprintf (stderr, "!r\t%lld\t%lld\t%p\n", (long long)old, (long long)x, p);
  void *res = realloc (p, x);
  fprintf (stderr, "r\t%lld\t%lld\t%p\t%p\n", (long long)old, (long long)x, p, res);
  return res;
#else
  return realloc (p, x);
#endif
}
void *dl_realloc0 (void *p, size_t x, size_t old) {
  dl_memory_used += predict_malloc_memory (x) - predict_malloc_memory (old);

#ifdef DL_DEBUG_MEM
  MEM_POS
  void *res = realloc (p, x);
  if (res != NULL && x > old) {
    memset ((char *)res + old, 0, x - old);
  }

  fprintf (stderr, "r\t%lld\t%lld\t%p\t%p\n", (long long)old, (long long)x, p, res);
  return res;
#else
  void *new_p = realloc (p, x);
  if (new_p != NULL && x > old) {
    memset ((char *)new_p + old, 0, x - old);
  }

  return new_p;
#endif
}
void dl_free (void *p, size_t x) {
  if (p == NULL) {
    return;
  }
  dl_memory_used -= predict_malloc_memory (x);

#ifdef DL_DEBUG_MEM
  MEM_POS
  fprintf (stderr, "f\t%lld\t%p\n", (long long)x, p);
#endif

  return free (p);
}
void* dl_memdup (const void *src, size_t x) {
  void *res = dl_malloc (x);
  assert (res != NULL);
  memcpy (res, src, x);
  return res;
}
long long dl_get_memory_used (void) {
  return (long long)dl_memory_used;
}
char* dl_strdup (const char *s) {
  size_t sn = strlen (s);

  char *t = (char *)dl_malloc (sn + 1);
  assert (t != NULL);
  memcpy (t, s, sn + 1);

  return t;
}
void dl_strfree (char *s) {
  if (s != NULL) {
    dl_free (s, strlen (s) + 1);
  }
}

const char *dl_int_to_str (int x) {
  static char tmp[50];
  snprintf (tmp, sizeof (tmp), "%d", x);
  return tmp;
}

const char *dl_ll_to_hex (long long x) {
  static char tmp[50];
  snprintf (tmp, sizeof (tmp), "%llx", x);
  return tmp;
}

double dl_get_utime (int clock_id) {
  struct timespec T;
#if _POSIX_TIMERS
  assert (clock_gettime (clock_id, &T) >= 0);
  return (double)T.tv_sec + (double)T.tv_nsec * 1e-9;
#else
#error "No high-precision clock"
  return (double)time();
#endif
}

double dl_time() {
  return dl_get_utime (CLOCK_MONOTONIC);
}

void dl_print_backtrace (void) {
  void *buffer[64];
  int nptrs = backtrace (buffer, 64);
  fprintf (stderr, "\n------- Stack Backtrace -------\n");
  backtrace_symbols_fd (buffer, nptrs, 2);
  fprintf (stderr, "-------------------------------\n");
}

void dl_print_backtrace_gdb (void) {
  char pid_buf[30];
  sprintf (pid_buf, "%d", getpid());
  char name_buf[512];
  ssize_t res = readlink ("/proc/self/exe", name_buf, 511);
  if (res >= 0) {
    name_buf[res] = 0;
    int child_pid = fork();
    if (child_pid < 0) {
      fprintf (stderr, "Can't fork() to run gdb\n");
      _exit (0);
    }
    if (!child_pid) {
      dup2 (2, 1); //redirect output to stderr
      fprintf (stdout, "stack trace for %s pid = %s\n", name_buf, pid_buf);
      execlp ("gdb", "gdb", "--batch", "-n", "-ex", "thread", "-ex", "bt", name_buf, pid_buf, NULL);
      _exit (0); /* If gdb failed to start */
    } else {
      waitpid (child_pid, NULL, 0);
    }
  } else {
    fprintf (stderr, "can't get name of executable file to pass to gdb\n");
  }
}

void dl_assert__ (const char *expr, const char *file_name, const char *func_name,
                  int line, const char *desc, int use_perror) {
  fprintf (stderr, "dl_assert failed [%s : %s : %d]: ", file_name, func_name, line);
  fprintf (stderr, "%s\n", desc);
  if (use_perror) {
    perror ("perror description");
  }
  abort();
}

static sigset_t old_mask;
static int old_mask_inited = 0;

sigset_t dl_get_empty_sigset (void) {
  sigset_t mask;
  sigemptyset (&mask);
  return mask;
}

void dl_sigaction (int sig, void (*handler) (int), sigset_t mask, int flags, void (*action) (int, siginfo_t *, void *)) {
  struct sigaction act;
  memset (&act, 0, sizeof (act));
  act.sa_mask = mask;
  act.sa_flags = flags;

  if (handler != NULL) {
    act.sa_handler = handler;
  }
  if (action != NULL) {
    act.sa_sigaction = action;
  }

  int err = sigaction (sig, &act, NULL);
  dl_passert (err != -1, "failed sigaction");
}

void dl_signal (int sig, void (*handler) (int)) {
  dl_sigaction (sig, handler, dl_get_empty_sigset(), SA_ONSTACK | SA_RESTART, NULL);
}

void dl_restore_signal_mask (void) {
  dl_assert (old_mask_inited != 0, "old_mask in not inited");
  int err = sigprocmask (SIG_SETMASK, &old_mask, NULL);
  dl_passert (err != -1, "failed to restore signal mask");
}

void dl_block_all_signals (void) {
  sigset_t mask;
  sigfillset (&mask);
  int err = sigprocmask (SIG_SETMASK, &mask, &old_mask);
  old_mask_inited = 1;
  dl_passert (err != -1, "failed to block all signals");
}

void dl_allow_all_signals (void) {
  sigset_t mask;
  sigemptyset (&mask);
  int err = sigprocmask (SIG_SETMASK, &mask, &old_mask);
  old_mask_inited = 1;
  dl_passert (err != -1, "failed to allow all signals");
}

int dl_block_signal (const int sig) {
  sigset_t mask;
  sigemptyset (&mask);
  int err = sigaddset (&mask, sig);
  if (err < 0) {
    return -1;
  }
  err = sigprocmask (SIG_BLOCK, &mask, NULL);
  dl_passert (err != -1, dl_pstr ("failed to block signal %d", sig));

  return 0;
}

int dl_allow_signal (const int sig) {
  sigset_t mask;
  sigemptyset (&mask);
  int err = sigaddset (&mask, sig);
  if (err < 0) {
    return -1;
  }
  err = sigprocmask (SIG_UNBLOCK, &mask, NULL);
  dl_passert (err != -1, dl_pstr ("failed to allow signal %d", sig));

  return 0;
}

static void runtime_handler (const int sig) {
  fprintf (stderr, "%s caught, terminating program\n", sig == SIGSEGV ? "SIGSEGV" : "SIGABRT");
  dl_print_backtrace();
  dl_print_backtrace_gdb();
  _exit (EXIT_FAILURE);
}

void dl_set_default_handlers (void) {
  dl_signal (SIGSEGV, runtime_handler);
  dl_signal (SIGABRT, runtime_handler);
}

char* dl_pstr (char const *msg, ...) {
  static char s[5000];
  va_list args;

  va_start (args, msg);
  vsnprintf (s, 5000, msg, args);
  va_end (args);

  return s;
}

/** Memory and cpu stats **/
int get_mem_stats (pid_t pid, mem_info_t *info) {
#define TMEM_SIZE 10000
  static char mem[TMEM_SIZE];
  snprintf (mem, TMEM_SIZE, "/proc/%lu/status", (unsigned long)pid);
  int fd = open (mem, O_RDONLY);

  if (fd == -1) {
    return 0;
  }

  int size = (int)read (fd, mem, TMEM_SIZE - 1);
  if (size <= 0) {
    close (fd);
    return 0;
  }
  mem[size] = 0;

  char *s = mem;
  while (*s) {
    char *st = s;
    while (*s != 0 && *s != '\n') {
      s++;
    }
    unsigned long long *x = NULL;
    if (strncmp (st, "VmPeak", 6) == 0) {
      x = &info->vm_peak;
    }
    if (strncmp (st, "VmSize", 6) == 0) {
      x = &info->vm;
    }
    if (strncmp (st, "VmHWM", 5) == 0) {
      x = &info->rss_peak;
    }
    if (strncmp (st, "VmRSS", 5) == 0) {
      x = &info->rss;
    }
    if (x != NULL) {
      while (st < s && *st != ' ') {
        st++;
      }
      *x = (unsigned long long)-1;

      if (st < s) {
        sscanf (st, "%llu", x);
      }
    }
    if (*s == 0) {
      break;
    }
    s++;
  }

  close (fd);
  return 1;
#undef TMEM_SIZE
}

int get_pid_info (pid_t pid, pid_info_t *info) {
#define TMEM_SIZE 10000
  static char mem[TMEM_SIZE];
  snprintf (mem, TMEM_SIZE, "/proc/%lu/stat", (unsigned long)pid);
  int fd = open (mem, O_RDONLY);

  if (fd == -1) {
    return 0;
  }

  int size = (int)read (fd, mem, TMEM_SIZE - 1);
  if (size <= 0) {
    close (fd);
    return 0;
  }
  mem[size] = 0;

  char *s = mem;
  int pass_cnt = 0;

  while (pass_cnt < 22) {
    if (pass_cnt == 12) {
      sscanf (s, "%llu", &info->utime);
    }
    if (pass_cnt == 13) {
      sscanf (s, "%llu", &info->stime);
   }
    if (pass_cnt == 14) {
      sscanf (s, "%llu", &info->cutime);
    }
    if (pass_cnt == 15) {
      sscanf (s, "%llu", &info->cstime);
    }
    if (pass_cnt == 21) {
      sscanf (s, "%llu", &info->starttime);
    }
    while (*s && *s != ' ') {
      s++;
    }
    if (*s == ' ') {
      s++;
      pass_cnt++;
    } else {
      dl_assert (0, "unexpected end of proc file");
      break;
    }
  }

  close (fd);
  return 1;
#undef TMEM_SIZE
}

unsigned long long get_pid_start_time (pid_t pid) {
  pid_info_t info;
  unsigned long long res = 0;
  if (get_pid_info (pid, &info)) {
    res = info.starttime;
  }

  return res;
}

int get_cpu_total (unsigned long long *cpu_total) {
#define TMEM_SIZE 10000
  static char mem[TMEM_SIZE];
  snprintf (mem, TMEM_SIZE, "/proc/stat");
  int fd = open (mem, O_RDONLY);

  if (fd == -1) {
    return 0;
  }

  int size = (int)read (fd, mem, TMEM_SIZE - 1);
  if (size <= 0) {
    close (fd);
    return 0;
  }

  unsigned long long sum = 0, cur = 0;
  int i;
  for (i = 0; i < size; i++) {
    int c = mem[i];
    if (c >= '0' && c <= '9') {
      cur = cur * 10 + (unsigned long long)c - '0';
    } else {
      sum += cur;
      cur = 0;
      if (c == '\n') {
        break;
      }
    }
  }

  *cpu_total = sum;

  close (fd);
  return 1;
#undef TMEM_SIZE
}

#define BB0(x) x,
#define BB1(x) BB0(x) BB0(x+1) BB0(x+1) BB0(x+2)
#define BB2(x) BB1(x) BB1(x+1) BB1(x+1) BB1(x+2)
#define BB3(x) BB2(x) BB2(x+1) BB2(x+1) BB2(x+2)
#define BB4(x) BB3(x) BB3(x+1) BB3(x+1) BB3(x+2)
#define BB5(x) BB4(x) BB4(x+1) BB4(x+1) BB4(x+2)
#define BB6(x) BB5(x) BB5(x+1) BB5(x+1) BB5(x+2)
#define BB7(x) BB6(x) BB6(x+1) BB6(x+1) BB6(x+2)
#define BB8(x) BB7(x) BB7(x+1) BB7(x+1) BB7(x+2)
static int dl_bitcnt16[] = {BB8(0)};

int dl_bitcnt (unsigned int x) {
  return dl_bitcnt16[x & 0xFFFF] + dl_bitcnt16[x >> 16];
}

