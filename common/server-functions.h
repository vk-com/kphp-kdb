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

    Copyright 2009-2013 Vkontakte Ltd
              2008-2013 Nikolai Durov
              2008-2013 Andrei Lopatin
              2011-2013 Oleg Davydov
              2012-2013 Arseny Smirnov
              2012-2013 Aliaksei Levin
              2012-2013 Anton Maydell
                   2013 Vitaliy Valtman
*/

#ifndef __SERVER_FUNCTIONS_H__
#define __SERVER_FUNCTIONS_H__

#include <unistd.h>
#include <getopt.h>

#ifdef __cplusplus
extern "C" {
#endif

double get_utime (int clock_id);
int change_user (char *username);
int change_user_group (char *username, char *groupname);
int raise_file_rlimit (int maxfiles);
void save_pid (const pid_t pid, const char *pid_file);
void remove_pidfile (const char *pid_file);

int fast_backtrace (void **buffer, int size);

void print_backtrace (void);
void sigsegv_debug_handler (const int sig);
void sigabrt_debug_handler (const int sig);
void set_debug_handlers (void);

int adjust_oom_score (int oom_score_adj);

extern long long precise_time;  // (long long) (2^16 * precise unixtime)
extern long long precise_time_rdtsc; // when precise_time was obtained

long long get_precise_time (unsigned precision);

int hexdump (void *start, void *end);

// write message with timestamp and pid, safe to call inside handler
ssize_t kwrite (int fd, const void *buf, size_t count);

void kprintf_multiprocessing_mode_enable (void);
// print message with timestamp
void kprintf (const char *format, ...) __attribute__ ((format (printf, 1, 2)));
#define vkprintf(verbosity_level, format, ...) do { \
    if ((verbosity_level) > verbosity) {	    \
      break; \
    } \
    kprintf ((format), ##__VA_ARGS__);		\
  } while (0)

#define	BACKLOG	8192
#define MAX_CONNECTIONS	65536

extern int quit_steps, start_time;
extern int backlog, port, maxconn, daemonize, verbosity, binlog_disabled, enable_ipv6, keyring_enabled;
extern char *logname, *username, *progname, *aes_pwd_file;


#define	ENGINE_STD_GETOPT	"b:c:dhl:p:ru:v"
int process_engine_option (int opt);
int parse_engine_options_long (int argc, char **argv, int (*execute)(int val));
int parse_usage (void);
void parse_option (const char *name, int arg, int *var, int val, char *help, ...) __attribute__ ((format (printf, 5, 6)));;
void remove_parse_option (int val);

/* RDTSC */

#if defined(__i386__)

static __inline__ unsigned long long rdtsc(void) {
  unsigned long long int x;
  __asm__ volatile ("rdtsc" : "=A" (x));
  return x;
}
#elif defined(__x86_64__)

static __inline__ unsigned long long rdtsc(void) {
  unsigned hi, lo;
  __asm__ __volatile__ ("rdtsc" : "=a"(lo), "=d"(hi));
  return ( (unsigned long long)lo)|( ((unsigned long long)hi)<<32 );
}

#endif

typedef struct {
  int ebx, ecx, edx, computed;
} vk_cpuid_t;
vk_cpuid_t *vk_cpuid (void);

#ifdef __cplusplus
}
#endif


#endif
