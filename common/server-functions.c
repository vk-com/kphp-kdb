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

#define _GNU_SOURCE
#include <assert.h>
#include <stdio.h>
#include <fcntl.h>
#include <stdarg.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <pwd.h>
#include <grp.h>
#include <limits.h>
#include <sys/resource.h>
#include <time.h>
#include <execinfo.h>
#include <signal.h>
#include <sys/time.h>
#include <sys/file.h>
#include <errno.h>
#include <getopt.h>
#include "net-events.h"

#include "server-functions.h"

#define	DEFAULT_ENGINE_USER	"kitten"

int backlog __attribute ((weak));
int maxconn __attribute ((weak));
int enable_ipv6 __attribute__ ((weak));
char *aes_pwd_file __attribute__ ((weak));
int udp_enabled __attribute__ ((weak));
char *binlogname __attribute__ ((weak));
int memcache_auto_answer_mode __attribute__ ((weak));
long long max_binlog_size __attribute__ ((weak));
long long dynamic_data_buffer_size __attribute__ ((weak)); 
double udp_drop_probability __attribute__ ((weak));
int keyring_enabled;

int quit_steps, start_time;
int backlog = BACKLOG, port, maxconn = MAX_CONNECTIONS, daemonize, verbosity, binlog_disabled;
char *username, *logname, *progname;

long long precise_time;
long long precise_time_rdtsc;

double get_utime (int clock_id) {
  struct timespec T;
#if _POSIX_TIMERS
  assert (clock_gettime (clock_id, &T) >= 0);
  double res = T.tv_sec + (double) T.tv_nsec * 1e-9;
#else
#error "No high-precision clock"
  double res = time ();
#endif
  if (clock_id == CLOCK_REALTIME) {
    precise_time = (long long) (res * (1LL << 32));
    precise_time_rdtsc = rdtsc ();
  }
  return res;
}

long long get_precise_time (unsigned precision) {
  unsigned long long diff = rdtsc() - precise_time_rdtsc;
  if (diff > precision) {
    get_utime (CLOCK_REALTIME);
  }
  return precise_time;
}

int hexdump (void *start, void *end) {
  char *ptr = start, c;
  while (ptr < (char *) end) {
    int s = (char *) end - ptr, i;
    if (s > 16) { 
      s = 16;
    }
    fprintf (stderr, "%08x", (int) (ptr - (char *) start));
    for (i = 0; i < 16; i++) {
      c = ' ';
      if (i == 8) {
        fputc (' ', stderr);
      }
      if (i < s) {
        fprintf (stderr, "%c%02x", c, (unsigned char) ptr[i]);
      } else {
        fprintf (stderr, "%c  ", c);
      }
    }
    c = ' ';
    fprintf (stderr, "%c  ", c);
    for (i = 0; i < s; i++) {
      putc ((unsigned char) ptr[i] < ' ' ? '.' : ptr[i], stderr);
    }
    putc ('\n', stderr);
    ptr += 16;
  }
  return end - start;
}

int change_user_group (char *username, char *groupname) {
  struct passwd *pw;
  /* lose root privileges if we have them */
  if (getuid() == 0 || geteuid() == 0) {
    if (username == 0 || *username == '\0') {
      username = DEFAULT_ENGINE_USER;
    }
    if ((pw = getpwnam (username)) == 0) {
      kprintf ("change_user_group: can't find the user %s to switch to\n", username);
      return -1;
    }
    gid_t gid = pw->pw_gid;
    if (setgroups (1, &gid) < 0) {
      kprintf ("change_user_group: failed to clear supplementary groups list: %m\n");
      return -1;
    }

    if (groupname) {
      struct group *g = getgrnam (groupname);
      if (g == NULL) {
        kprintf ("change_user_group: can't find the group %s to switch to\n", groupname);
        return -1;
      }
      gid = g->gr_gid;
    }

    if (setgid (gid) < 0) {
      kprintf ("change_user_group: setgid (%d) failed. %m\n", (int) gid);
      return -1;
    }

    if (setuid (pw->pw_uid) < 0) {
      kprintf ("change_user_group: failed to assume identity of user %s\n", username);
      return -1;
    }
  }
  return 0;
}

int change_user (char *username) {
  struct passwd *pw;
  /* lose root privileges if we have them */
  if (getuid() == 0 || geteuid() == 0) {
    if (username == 0 || *username == '\0') {
      username = DEFAULT_ENGINE_USER;
//      fprintf (stderr, "can't run as root without the -u switch\n");
//      return -1;
    }
    if ((pw = getpwnam (username)) == 0) {
      kprintf ("can't find the user %s to switch to\n", username);
      return -1;
    }
    gid_t gid = pw->pw_gid;
    if (setgroups(1, &gid) < 0) {
      kprintf ("failed to clear supplementary groups list: %m\n");
      return -1;
    }
    if (initgroups(username, gid) != 0) {
      kprintf ("failed to load groups of user %s: %m\n", username);
      return -1;
    }
    if (setgid (pw->pw_gid) < 0 || setuid (pw->pw_uid) < 0) {
      kprintf ("failed to assume identity of user %s\n", username);
      return -1;
    }
  }
  return 0;
}

int raise_file_rlimit (int maxfiles) {
  struct rlimit rlim;
  
  if (getrlimit(RLIMIT_NOFILE, &rlim) != 0) {
    kprintf ("failed to getrlimit number of files\n");
    return -1;
  } else {
    if (rlim.rlim_cur < maxfiles)
      rlim.rlim_cur = maxfiles + 3;
    if (rlim.rlim_max < rlim.rlim_cur)
      rlim.rlim_max = rlim.rlim_cur;
    if (setrlimit(RLIMIT_NOFILE, &rlim) != 0) {
      kprintf ("failed to set rlimit for open files. Try running as root or requesting smaller maxconns value.\n");
      return -1;
    }
  }
  return 0;
}


void save_pid (const pid_t pid, const char *pid_file) {
  FILE *fp;
  if (pid_file == NULL)
    return;

  if ((fp = fopen(pid_file, "w")) == NULL) {
    kprintf ("Could not open the pid file %s for writing\n", pid_file);
    return;
  }

  fprintf(fp,"%ld\n", (long)pid);
  if (fclose(fp) == -1) {
    kprintf ("Could not close the pid file %s.\n", pid_file);
    return;
  }
}

void remove_pidfile (const char *pid_file) {
  if (pid_file == NULL)
    return;

  if (unlink(pid_file) != 0) {
    kprintf ("Could not remove the pid file %s.\n", pid_file);
  }

}

int adjust_oom_score (int oom_score_adj) {
  static char path[64], str[16];
  assert (sprintf (path, "/proc/%d/oom_score_adj", getpid()) < 64);
  int l = sprintf (str, "%d", oom_score_adj);
  assert (l <= 15);
  int fd = open (path, O_WRONLY | O_TRUNC);
  if (fd < 0) {
    kprintf ("cannot write to %s : %m\n", path);
    return -1;
  }
  int w = write (fd, str, l);
  if (w < 0) {
    kprintf ("cannot write to %s : %m\n", path);
    close (fd);
    return -1;
  }
  close (fd);
  return (w == l);
}

void print_backtrace (void) {
  void *buffer[64];
  int nptrs = backtrace (buffer, 64);
  kwrite (2, "\n------- Stack Backtrace -------\n", 33);
  backtrace_symbols_fd (buffer, nptrs, 2);
  kwrite (2, "-------------------------------\n", 32);
}

void sigsegv_debug_handler (const int sig) {
  kwrite (2, "SIGSEGV caught, terminating program\n", 36);
  print_backtrace ();
  exit (EXIT_FAILURE);
}

void sigabrt_debug_handler (const int sig) {
  kwrite (2, "SIGABRT caught, terminating program\n", 36);
  print_backtrace ();
  exit (EXIT_FAILURE);
}

void sigfpe_debug_handler (const int sig) {
  kwrite (2, "SIGFPE caught, terminating program\n", 35);
  print_backtrace ();
  exit (EXIT_FAILURE);
}

void set_debug_handlers (void) {
  signal (SIGSEGV, sigsegv_debug_handler);
  signal (SIGABRT, sigabrt_debug_handler);
  signal (SIGFPE, sigfpe_debug_handler);
}

void usage (void) __attribute ((weak));

void usage (void) {
  printf ("usage: %s [-v] [-d] [-r] [-p<port>] [-u<username>] [-b<backlog>] [-c<max-conn>] [-l<log-name>] <kfs-binlog-name>\n"
	  "\tPerforms generic RPC server/client operations\n"
	  "\t-v\toutput statistical and debug information into stderr\n"
	  "\t-r\tread-only binlog (don't log new events)\n",
	  progname ? progname : "SOMETHING");
  exit(2);
}

int process_engine_option (int opt) {
  switch (opt) {      
  case 'v':
    verbosity++;
    break;
  case 'h':
    usage ();
    return 2;
  case 'b':
    backlog = atoi (optarg);
    if (backlog <= 0) backlog = BACKLOG;
    break;
  case 'c':
    maxconn = atoi (optarg);
    if (maxconn <= 0 || maxconn > MAX_CONNECTIONS) {
      maxconn = MAX_CONNECTIONS;
    }
    break;
  case 'p':
    port = atoi (optarg);
    break;
  case 'u':
    username = optarg;
    break;
  case 'l':
    logname = optarg;
    break;
  case 'd':
    daemonize ^= 1;
    break;
  case 'r':
    binlog_disabled++;
    break;
  default:
    usage ();
    exit (1);
  }
  return 0;
}

struct option global_longopts[10000] = {
  { "verbosity", optional_argument, 0, 'v' },
  { "help", no_argument, 0, 'h' },
  { "backlog", required_argument, 0, 'b'},
  { "connections", required_argument, 0, 'c'},
  { "port", required_argument, 0, 'p'},
  { "user", required_argument, 0, 'u'},
  { "log", required_argument, 0, 'l'},
  { "daemonize", optional_argument, 0, 'd'},
  { "replica", optional_argument, 0, 'r'},
  { "nice", required_argument, 0, 202},
  { "aes-pwd", required_argument, 0, 200},
  { "ipv6", no_argument, 0, '6'},
  { "udp", no_argument, 0, 201},
  { "binlogname", required_argument, 0, 'a'},
  { "memcache-auto-answer", no_argument, 0, 203},
  { "binlog-size", required_argument, 0, 'B'},
  { "heap", required_argument, 0, 204},
  { "udp-drop-probability", required_argument, 0, 205},
  { "keyring", no_argument, 0, 206},
};

char *global_longopts_help[10000] = {
  "sets or increases verbosity level",
  "prints help and exits",
  "sets backlog",
  "sets maximal connections number",
  "sets listening port number",
  "sets user name to make setuid",
  "sets log file name",
  "changes between daemonize/not daemonize mode",
  "starts in replica mode",
  "sets niceness",
  "sets pwd file",
  "enables ipv6 connections",
  "enables udp message support",
  "custom binlog name",
  "sends serialized false on non-answered memcache queries",
  "maximal size of binlog slice",
  "sets zmalloc heap size",
  NULL,
  "reads keyring from stdin for binlog/snapshot encryption",
};

char global_optstring[20000] = "";

char *global_optstring_help[] = {
};

int global_optstring_pos;

int parse_usage (void) {
  int s = 0;
  int max = 0;
  while (global_longopts[s].name || global_longopts[s].val) {
    int cur = 0;
    int v = global_longopts[s].val;
    if (global_longopts[s].name) {
      cur += 2 + strlen (global_longopts[s].name);
      if (v >= 33 && v <= 127) {
        cur ++;
      }
    }
    if (v >= 33 && v <= 127) {
      cur += 2;
    }
    if (global_longopts[s].has_arg == required_argument) {
      cur += 6;
    } else if (global_longopts[s].has_arg == optional_argument) {
      cur += 6;
    }
    if (cur > max) { 
      max = cur; 
    }
    s ++;
  }
  s = 0;
  while (global_longopts[s].name || global_longopts[s].val) {
    int cur = 0;
    int v = global_longopts[s].val;
    printf ("\t");
    if (global_longopts[s].name) {
      printf ("--%s", global_longopts[s].name);
      cur += 2 + strlen (global_longopts[s].name);
      if (v >= 33 && v <= 127) {
        cur ++;
        printf ("/");
      }
    }
    if (v >= 33 && v <= 127) {
      printf ("-%c", (char)v);
      cur += 2;
    }
    if (global_longopts[s].has_arg == required_argument) {
      printf (" <arg>");
      cur += 6;
    } else if (global_longopts[s].has_arg == optional_argument) {
      printf (" {arg}");
      cur += 6;
    }
    while (cur < max) { 
      printf (" ");
      cur ++;
    }
    printf ("\t");
    if (global_longopts_help[s]) {
      char *e = global_longopts_help[s];
      while (*e) {
        printf ("%c", *e);
        if (*e == '\n') {
          printf ("\t");
          int i;
          for (i = 0; i < max; i++) {
            printf (" ");
          }
          printf ("\t");
        }
        e ++;
      }
      printf ("\n");
//      printf ("%s\n", global_longopts_help[s]);
    } else {
      printf ("no help provided\n");
    }
    s ++;
  }
  for (s = 0; s < global_optstring_pos; s ++) {
    printf ("\t-%c\t\t", global_optstring[s]);
    if (global_optstring_help[s]) {
      printf ("%s\n", global_optstring_help[s]);
    } else {
      printf ("no help provided\n");
    }
  }
  return 0;
}

int find_parse_option (int val) {
  int s = 0;
  while (global_longopts[s].name || global_longopts[s].val) { 
    if (global_longopts[s].val == val) { return s; }
    s ++;
  }
  return -1;
}

void parse_option (const char *name, int arg, int *var, int val, char *help, ...) {
  int l = find_parse_option (val);
  if (l >= 0) {
    if (val >= 33 && val <= 127) {
      fprintf (stderr, "Duplicate option `%c`\n", (char)val);
    } else {
      fprintf (stderr, "Duplicate option %d\n", val);
    }
    usage ();
  }

  assert (name || val);
  int s = 0;
  while (global_longopts[s].name || global_longopts[s].val) { s ++; }
  global_longopts[s].name = name;
  global_longopts[s].has_arg = arg;
  global_longopts[s].flag = var;
  global_longopts[s].val = val;
  if (help) {
    va_list ap;
    va_start (ap, help);
    vasprintf (&global_longopts_help[s], help, ap);
    va_end (ap);
  }
}

static int removed_options[4];

void remove_parse_option (int val) {
  int t = find_parse_option (val);
  int s = 0;
  while (global_longopts[s].name || global_longopts[s].val) { s ++; }
  if (val >= 0 && val <= 127) {
    removed_options[val / 32] |= 1 << (val & 31);
  }

  if (t >= 0) {
    while (t + 1 != s) {
      global_longopts[t] = global_longopts[t + 1];
      global_longopts_help[t] = global_longopts_help[t + 1];
      t++;
    }
    memset (&global_longopts[s - 1], 0, sizeof (*global_longopts));
    char *x;
    x = global_longopts_help[t];
    global_longopts_help[t] = global_longopts_help[s - 1];
    global_longopts_help[s - 1] = x;
  }
}

static long long parse_memory_limit (const char *s) {
  long long x;
  char c = 0;
  if (sscanf (s, "%lld%c", &x, &c) < 1) {
    kprintf ("Parsing limit for option fail: %s\n", s);
    usage ();
    exit (1);
  }
  switch (c | 0x20) {
    case ' ': break;
    case 'k':  x <<= 10; break;
    case 'm':  x <<= 20; break;
    case 'g':  x <<= 30; break;
    case 't':  x <<= 40; break;
    default: 
      kprintf ("Parsing limit fail. Unknown suffix '%c'.\n", c); 
      usage ();
      exit (1);
  }
  return x;
}

int parse_engine_options_long (int argc, char **argv,  int (*execute)(int val)) {
  int s = 0;
  while (global_longopts[s].name || global_longopts[s].val) { s ++; }
/*  assert (s + num < 10000);
  memcpy (global_longopts + s, longopts, sizeof (struct option) * num);
  memcpy (global_longopts_help + s, longopts_help, sizeof (void *) * num);*/

/*  int x = strlen (global_optstring);
  int y = strlen (optstring);
  assert ( x + y + s + num <= 20000);

  memcpy (global_optstring + x, optstring, y);
  memcpy (global_optstring_help + x, optstring_help, y * sizeof (void *));

  global_optstring_pos = x + y;
  int t = x + y;*/

  

  int t = 0;

  int i;  
  for (i = 0; i < s; i++) {
    if (global_longopts[i].val >= 33 && global_longopts[i].val <= 127) {
      global_optstring[t ++] = global_longopts[i].val;
      if (global_longopts[i].has_arg == required_argument) {
        global_optstring[t ++] = ':';
      }
    }
  }
  int ss = s;
  for (i = 0; i < s; i++) {
    if (!global_longopts[i].name) {
      struct option t = global_longopts[s - 1];
      global_longopts[s - 1] = global_longopts[i];
      global_longopts[i] = t;
      char *x;
      x = global_longopts_help[i];
      global_longopts_help[i] = global_longopts_help[s - 1];
      global_longopts_help[s - 1] = x;
      s --;
    }
  }
  s = ss;

  long long x;
  while (1) {
    int option_index = 0;
    int c = getopt_long (argc, argv, global_optstring, global_longopts, &option_index);
    if (c == -1) { break; }
    if (c > 0 && c < 128 && (removed_options[c / 32] & (1 << (c & 31)))) {
      if (execute (c) < 0) {
        printf ("Unknown option c = %d\n", c);
        usage ();
        exit (2);
      }
    } else {
      switch (c) {
      case 0:
        break;
      case 'v':
        if (!optarg) {
          verbosity++;
        } else {
          verbosity = atoi (optarg);
        }
        break;
      case 'h':
        usage ();
        exit (2);
      case 'b':
        backlog = atoi (optarg);
        if (backlog <= 0) backlog = BACKLOG;
        break;
      case 'c':
        maxconn = atoi (optarg);
        if (maxconn <= 0 || maxconn > MAX_CONNECTIONS) {
          maxconn = MAX_CONNECTIONS;
        }
        break;
      case 'p':
        port = atoi (optarg);
        break;
      case 'u':
        username = optarg;
        break;
      case 'l':
        logname = optarg;
        break;
      case 'd':
        if (!optarg) {
          daemonize ^= 1;
        } else {
          daemonize = atoi (optarg) != 0;
        }
        break;
      case 'r':
        if (!optarg) {
          binlog_disabled ++;;
        } else {
          binlog_disabled = atoi (optarg);
        }
        break;
      case 'a':
        binlogname = optarg;
        break;
      case 'B':
        x = parse_memory_limit (optarg);
        if (x >= 1024 && x < (1LL << 60)) {
          max_binlog_size = x;
        }
        break;
      case '6':
        enable_ipv6 = SM_IPV6;
        break;
      case 200:
        aes_pwd_file = optarg;
        break;
      case 201:
        udp_enabled ++;
        break;
      case 202:
        errno = 0;
        nice (atoi (optarg));
        if (errno) {
          perror ("nice");
        }
        break;
      case 203:
        memcache_auto_answer_mode ++;
        break;
      case 204:
        x = parse_memory_limit (optarg);
        if (x >= (1LL << 20) && x <= (sizeof(long) == 4 ? (3LL << 30) : (100LL << 30))) {
          dynamic_data_buffer_size = x;
        }
        break;        
      case 205:
        udp_drop_probability = atof (optarg);
        break;
      case 206:
        keyring_enabled = 1;
        break;
      default:
        if (execute (c) < 0) {
          printf ("Unknown option c = %d\n", c);
          usage ();
          exit (2);
        }
        break;
      }
    }
  }
  return 0;
}

inline void kwrite_print_int (char **s, const char *name, int name_len, int i) {
  if (i < 0) {
    i = INT_MAX;
  }

  *--*s = ' ';
  *--*s = ']';

  do {
    *--*s = i % 10 + '0';
    i /= 10;
  } while (i > 0);

  *--*s = ' ';

  while (--name_len >= 0) {
    *--*s = name[name_len];
  }

  *--*s = '[';
}

ssize_t kwrite (int fd, const void *buf, size_t count) {
  int old_errno = errno;

#define S_BUF_SIZE 100
  char s[S_BUF_SIZE], *s_begin = s + S_BUF_SIZE;

  kwrite_print_int (&s_begin, "time", 4, time (NULL));
  kwrite_print_int (&s_begin, "pid" , 3, getpid ());

  assert (s_begin >= s);

  size_t s_count = s + S_BUF_SIZE - s_begin;
  ssize_t result = s_count + count;
  while (s_count > 0) {
    errno = 0;
    ssize_t res = write (fd, s_begin, s_count);
    if (errno && errno != EINTR) {
      errno = old_errno;
      return res;
    }
    if (!res) {
      break;
    }
    if (res >= 0) {
      s_begin += res;
      s_count -= res;
    }
  }

  while (count > 0) {
    errno = 0;
    ssize_t res = write (fd, buf, count);
    if (errno && errno != EINTR) {
      errno = old_errno;
      return res;
    }
    if (!res) {
      break;
    }
    if (res >= 0) {
      buf += res;
      count -= res;
    }
  }

  errno = old_errno;
  return result;
#undef S_BUF_SIZE
}


static int kprintf_multiprocessing_mode = 0;
static char mp_kprintf_buf[PIPE_BUF];

void kprintf_multiprocessing_mode_enable (void) {
  kprintf_multiprocessing_mode = 1;
}

void kprintf (const char *format, ...) {
  const int old_errno = errno;
  struct tm t;
  struct timeval tv;

  if (gettimeofday (&tv, NULL) || !localtime_r (&tv.tv_sec, &t)) {
    memset (&t, 0, sizeof (t));
  }

  if (kprintf_multiprocessing_mode) {
    while (1) {
      if (flock (2, LOCK_EX) < 0) {
        if (errno == EINTR) {
          continue;
        }
        errno = old_errno;
        return;
      }
      break;
    }
    int n = snprintf (mp_kprintf_buf, sizeof (mp_kprintf_buf), "[%d][%4d-%02d-%02d %02d:%02d:%02d.%06d local] ", getpid (), t.tm_year + 1900, t.tm_mon + 1, t.tm_mday, t.tm_hour, t.tm_min, t.tm_sec, (int) tv.tv_usec);
    if (n < sizeof (mp_kprintf_buf) - 1) {
      errno = old_errno;
      va_list ap;
      va_start (ap, format);
      n += vsnprintf (mp_kprintf_buf + n, sizeof (mp_kprintf_buf) - n, format, ap);
      va_end (ap);
    }
    if (n >= sizeof (mp_kprintf_buf)) {
      n = sizeof (mp_kprintf_buf) - 1;
      if (mp_kprintf_buf[n-1] != '\n') {
        mp_kprintf_buf[n++] = '\n';
      }
    }
    while (write (2, mp_kprintf_buf, n) < 0 && errno == EINTR);
    while (flock (2, LOCK_UN) < 0 && errno == EINTR);
    errno = old_errno;
  } else {
    fprintf (stderr, "[%4d-%02d-%02d %02d:%02d:%02d.%06d local] ", t.tm_year + 1900, t.tm_mon + 1, t.tm_mday, t.tm_hour, t.tm_min, t.tm_sec, (int) tv.tv_usec);
    errno = old_errno;
    va_list ap;
    va_start (ap, format);
    vfprintf (stderr, format, ap);
    va_end (ap);
  }
}

vk_cpuid_t *vk_cpuid (void) {
  static vk_cpuid_t cached = { .computed = 0 };
  if (!cached.computed++) {
    int a;
    asm ("cpuid\n\t"
        : "=a" (a), "=b" (cached.ebx) , "=c" (cached.ecx), "=d" (cached.edx)
        : "0" (1)
    );
  }
  return &cached;
}
