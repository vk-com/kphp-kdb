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
#include <signal.h>

#ifndef likely
#define likely(x) __builtin_expect ((x),1)
#endif
#ifndef ulikely
#define unlikely(x) __builtin_expect ((x),0)
#endif

#ifdef __cplusplus
extern "C" {
#endif

void *dl_malloc (size_t x);
void *dl_malloc0 (size_t x);
void *dl_realloc (void *p, size_t x, size_t old);
void *dl_realloc0 (void *p, size_t x, size_t old);
void *dl_memdup (const void *src, size_t x);
void dl_free (void *p, size_t x);
long long dl_get_memory_used (void);

char* dl_strdup (const char *s);
void dl_strfree (char *s);

const char *dl_int_to_str (int x);
const char *dl_ll_to_hex (long long x);

double dl_get_utime (int clock_id);
double dl_time();

sigset_t dl_get_empty_sigset (void);
void dl_sigaction (int sig, void (*handler) (int), sigset_t mask, int flags, void (*action) (int, siginfo_t *, void *));
void dl_signal (int sig, void (*handler) (int));
void dl_restore_signal_mask (void);
void dl_block_all_signals (void);
void dl_allow_all_signals (void);
int dl_block_signal (const int sig);
int dl_allow_signal (const int sig);

void dl_print_backtrace (void);
void dl_print_backtrace_gdb (void);

void dl_set_default_handlers();

char* dl_pstr (char const *message, ...) __attribute__ ((format (printf, 1, 2)));

void dl_assert__ (const char *expr, const char *file_name, const char *func_name,
                  int line, const char *desc, int use_perror);

#define dl_assert_impl(f, str, use_perror) \
  if (unlikely(!(f))) {\
    dl_assert__ (#f, __FILE__, __FUNCTION__, __LINE__, str, use_perror);\
  }

#define dl_assert(f, str) dl_assert_impl (f, str, 0)
#define dl_passert(f, str) dl_assert_impl (f, str, 1)
#define dl_unreachable(str) dl_assert (0, str)
#define dl_fail(str) dl_assert (0, str); exit(1);
#define dl_pcheck(cmd) dl_passert (cmd >= 0, "call failed : "  #cmd)

typedef struct {
  unsigned long long utime;
  unsigned long long stime;
  unsigned long long cutime;
  unsigned long long cstime;
  unsigned long long starttime;
} pid_info_t;
typedef struct {
  unsigned long long vm_peak;
  unsigned long long vm;
  unsigned long long rss_peak;
  unsigned long long rss;
} mem_info_t;

int get_mem_stats (pid_t pid, mem_info_t *info);
int get_pid_info (pid_t pid, pid_info_t *info);
unsigned long long get_pid_start_time (pid_t pid);
int get_cpu_total (unsigned long long *cpu_total);

/* RDTSC */
#if defined(__i386__)
static __inline__ unsigned long long dl_rdtsc(void) {
  unsigned long long int x;
  __asm__ volatile ("rdtsc" : "=A" (x));
  return x;
}
#elif defined(__x86_64__)
static __inline__ unsigned long long dl_rdtsc(void) {
  unsigned hi, lo;
  __asm__ __volatile__ ("rdtsc" : "=a"(lo), "=d"(hi));
  return ( (unsigned long long)lo)|( ((unsigned long long)hi)<<32 );
}
#endif

#ifdef __cplusplus
}
#endif
