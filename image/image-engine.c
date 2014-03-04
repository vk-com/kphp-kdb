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
              2011-2013 Anton Maydell
*/

#define	_FILE_OFFSET_BITS	64

#include <assert.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <ctype.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/mman.h>
#include <sys/wait.h>
#include <sys/stat.h>
#include <signal.h>

#include "net-events.h"
#include "server-functions.h"
#include "net-connections.h"
#include "net-memcache-server.h"
#include "net-crypto-aes.h"
#include "image-data.h"
#include "kdb-data-common.h"
#include "am-stats.h"

#define	MAX_VALUE_LEN	(1 << 20)

#define	VERSION "1.35-r4"
#define	VERSION_STR "image-engine-"VERSION

#ifndef COMMIT
#define COMMIT "unknown"
#endif

const char FullVersionStr[] = VERSION_STR " compiled at " __DATE__ " " __TIME__ " by gcc " __VERSION__ " "
#ifdef __LP64__
  "64-bit"
#else
  "32-bit"
#endif
" after commit " COMMIT;

#define TCP_PORT 11211
#define UDP_PORT 11211

#define MAX_NET_RES	(1L << 16)

#define mytime() get_utime (CLOCK_MONOTONIC)

#define MAX_CHILD_PROCESS 32
#define MAX_THREADS 32

/*
 *
 *		MEMCACHED PORT
 *
 */


int mcp_get (struct connection *c, const char *key, int key_len);
int mcp_wakeup (struct connection *c);
int mcp_alarm (struct connection *c);
int mcp_stats (struct connection *c);
int mcp_check_ready (struct connection *c);

conn_type_t ct_image_engine_server = {
  .magic = CONN_FUNC_MAGIC,
  .title = "image_engine_server",
  .accept = accept_new_connections,
  .init_accepted = mcs_init_accepted,
  .run = server_read_write,
  .reader = server_reader,
  .writer = server_writer,
  .parse_execute = mcs_parse_execute,
  .close = server_close_connection,
  .free_buffers = free_connection_buffers,
  .init_outbound = server_failed,
  .connected = server_failed,
  .wakeup = 0,
  .alarm = 0,
#ifndef NOAES
  .crypto_init = aes_crypto_init,
  .crypto_free = aes_crypto_free,
  .crypto_encrypt_output = aes_crypto_encrypt_output,
  .crypto_decrypt_input = aes_crypto_decrypt_input,
  .crypto_needed_output_bytes = aes_crypto_needed_output_bytes,
#endif
};

struct in_addr settings_addr;
int daemonize = 0;
char *progname = NULL, *username, *groupname, *logname, *binlogname;
static int job_nice = 0, main_nice = 0;
static int max_load_image_area = -1;
static const long long max_virtual_memory = sizeof (long) == 4 ? (3LL << 30) : (20LL << 30);
static long long memory_limit = 256 << 20, map_limit;
static long long rss_memory_limit = 1 << 30;
static const long long vmsize_limit = (2LL << 30) - 1;
static int threads_limit = 8;

static int result_living_time = 60 * 60;
static int requests_in_hash;

static long long cmd_get, cmd_set, get_hits, get_missed, cmd_version, cmd_stats;
static long long complete_tasks;
//static long long nzec_bad_area_load_image;
static long long mmap_errors, forth_output_errors;
static int last_mmap_errno;
static double max_task_time;
int max_process_number = 0;
int max_all_results = 16 << 20, all_results_memory = 0;
long long unloaded_results, results_unload_bytes;
#define STATS_BUFF_SIZE	(16 << 10)
char stats_buffer[STATS_BUFF_SIZE];

int memcache_store (struct connection *c, int op, const char *key, int key_len, int flags, int delay, int size);
int memcache_get (struct connection *c, const char *key, int key_len);
int memcache_stats (struct connection *c);
int memcache_version (struct connection *c);

struct memcache_server_functions memcache_methods = {
  .execute = mcs_execute,
  .mc_store = memcache_store,
  .mc_get_start = mcs_get_start,
  .mc_get = memcache_get,
  .mc_get_end = mcs_get_end,
  .mc_incr = mcs_incr,
  .mc_delete = mcs_delete,
  .mc_version = memcache_version,
  .mc_stats = memcache_stats,
  .mc_check_perm = mcs_default_check_perm,
  .mc_init_crypto = mcs_init_crypto
};


/************************ Forth request structures ***********************************/

struct forth_request {
  long long prog_id;
  unsigned long long priority;
  struct forth_request *hnext; /* hash */
  struct forth_request *next, *prev;  /* list unused forth_requests */
  char *prog; /* contain program before image_exec, and result after */
  int size;
  int res;
  int timeout;
};

pid_t P[MAX_CHILD_PROCESS];
int SHM[MAX_CHILD_PROCESS], shm_no;
struct forth_request *RP[MAX_CHILD_PROCESS];
int child_process = 0;

#define MAX_QUEUE_SIZE 0x40000
#define REQ_HASH_SIZE 0x40000

#define REQ_WAITING (239)
#define REQ_RUNNING (REQ_WAITING+1)

static struct forth_request *H[REQ_HASH_SIZE];
static struct forth_request Q[MAX_QUEUE_SIZE];
static struct forth_request *HEAP[MAX_QUEUE_SIZE+1];
int queue_size = 0;

struct forth_request with_output_free_list = {.prev = &with_output_free_list, .next = &with_output_free_list};
struct forth_request without_output_free_list = {.prev = &without_output_free_list, .next = &without_output_free_list};

void tlist_insert (struct forth_request *L, struct forth_request *T) {
  struct forth_request *u = L->prev, *v = L;
  u->next = T; T->prev = u;
  v->prev = T; T->next = v;
  L->size++;
}

void tlist_remove (struct forth_request *L, struct forth_request *T) {
  vkprintf (4, "tlist_remove (%p)\n", T);
  struct forth_request *u = T->prev, *v = T->next;
  if (u == NULL && v == NULL) {
    return;
  }
  assert (u != NULL && v != NULL);
  u->next = v;
  v->prev = u;
  T->prev = T->next = NULL;
  L->size--;
  assert (L->size >= 0);
}

int stop_image_processing = 0;

pthread_mutex_t queue_cond_mutex;
pthread_cond_t queue_cond;

static inline void heapify_front (struct forth_request *E, int i) {
  while (1) {
    int j = i << 1;
    if (j > queue_size) { break; }
    if (j < queue_size && HEAP[j]->priority > HEAP[j+1]->priority) {
      j++;
    }
    if (E->priority <= HEAP[j]->priority) { break; }
    HEAP[i] = HEAP[j];
    i = j;
  }
  HEAP[i] = E;
}

static void hash_req_insert (struct forth_request *R) {
  long long prog_id = R->prog_id;
  unsigned h = ((unsigned) prog_id) & (REQ_HASH_SIZE - 1);
  R->hnext = H[h];
  H[h] = R;
  requests_in_hash++;
}

static int hash_req_remove (struct forth_request *R) {
  long long prog_id = R->prog_id;
  unsigned h = ((unsigned) prog_id) & (REQ_HASH_SIZE - 1);
  struct forth_request **p = H + h, *V;
  while (*p) {
    V = *p;
    if (V == R) {
      *p = V->hnext;
      requests_in_hash--;
      return 1;
    }
    p = &V->hnext;
  }
  return 0;
}

static struct forth_request *hash_get_req_f (long long prog_id) {
  unsigned h = ((unsigned) prog_id) & (REQ_HASH_SIZE - 1);
  struct forth_request **p = H + h, *V;
  while (*p) {
    V = *p;
    if (V->prog_id == prog_id) {
      *p = V->hnext;
      V->hnext = H[h];
      H[h] = V;
      return V;
    }
    p = &V->hnext;
  }
  return 0;
}

static void forth_request_free_prog (struct forth_request *E) {
  if (E->prog != NULL) {
    unloaded_results++;
    results_unload_bytes += E->size + 1;
    all_results_memory -= E->size + 1;
    free (E->prog);
    E->prog = NULL;
    E->size = 0;
  }
}

static void forth_request_unload_prog (struct forth_request *E) {
  tlist_remove (&with_output_free_list, E);
  assert (E->prog != NULL);
  forth_request_free_prog (E);
  assert (E->prog == NULL);
  tlist_insert (&without_output_free_list, E);
  hash_req_remove (E);
}

static void forth_request_run_output_gc (void) {
  struct forth_request *p, *w;
  for (p = with_output_free_list.next; p != &with_output_free_list; p = w) {
    if (now <= p->timeout) {
      break;
    }
    w = p->next;
    forth_request_unload_prog (p);
  }
  for (p = with_output_free_list.next; all_results_memory > max_all_results && p != &with_output_free_list; p = w) {
    w = p->next;
    forth_request_unload_prog (p);
  }
}

static void queue_add (struct forth_request *E) {
  tlist_insert (E->prog != NULL ? &with_output_free_list : &without_output_free_list, E);
}

static struct forth_request *queue_poll () {
  struct forth_request *E = without_output_free_list.next;
  if (E == &without_output_free_list) {
    E = with_output_free_list.next;
    if (E == &with_output_free_list) {
      return NULL;
    }
    forth_request_unload_prog (E);
  }
  tlist_remove (&without_output_free_list, E);
  return E;
}

static void req_structures_init (void) {
  int i;
  memset (H, 0, sizeof (H));
  for (i = 0; i < MAX_QUEUE_SIZE; i++) {
    Q[i].res = EXIT_SUCCESS;
    Q[i].prog_id = 0;
    Q[i].prog = NULL;
    queue_add (Q+i);
  }
}

/************************ Failure stats ***********************************/
struct stat_failure {
  long long total;
  long long prog_id;
  char *prog;
  int size;
};

static struct stat_failure sigabrt, sigsegv, sigother, sigkill, sigxcpu, sigterm, not_zero_exit_code;

static void stat_failure_incr (struct stat_failure *S, struct forth_request *E) {
  S->total++;
  S->prog_id = E->prog_id;
  if (S->prog) {
    free (S->prog);
  }
  S->prog = E->prog;
  S->size = E->size;
  E->prog = NULL;
  E->size = 0;
}

static int stat_failure_write (struct connection *c, const char *key, struct stat_failure *S) {
  vkprintf (4, "stat_failure_write (S->prog: %s, S->size: %d)\n", S->prog, S->size);
  if (S->prog == NULL) {
    return 0;
  }
  return return_one_key (c, key, S->prog, S->size);
}

int memcache_store (struct connection *c, int op, const char *key, int key_len, int flags, int delay, int size) {
  cmd_set++;
  vkprintf (3, "memcache_store: key='%s', key_len=%d, value_len=%d, flags = %d, exp_time = %d\n", key, key_len, size, flags, delay);
  long long prog_id = 0;
  int t = 0;
  if (op != mct_set || sscanf (key, "request%lld,%d", &prog_id, &t) < 1) {
    return -2;
  }
  if (size < MAX_VALUE_LEN) {
    struct forth_request *E = queue_poll ();
    assert (E != NULL);
    E->next = NULL;
    E->prog_id = prog_id;
    E->prog = malloc (size + 1);
    E->size = size;
    assert (E->prog);
    E->res = REQ_WAITING;
    hash_req_insert (E);
    assert (read_in (&c->In, E->prog, size) == size);
    E->prog[size] = 0;
    E->priority = now;
    E->priority += t;
    int i = ++queue_size;
    assert (i <= MAX_QUEUE_SIZE);
    while (i > 1) {
      int j = (i >> 1);
      if (HEAP[j]->priority <= E->priority) { break; }
      HEAP[i] = HEAP[j];
      i = j;
    }
    HEAP[i] = E;
    return 1;
  }
  return -2;
}

int image_prepare_stats (struct connection *c);

int return_one_key_int (struct connection *c, const char *key, int i) {
  static char buff[256], s[32];
  int vlen = sprintf (s, "i:%d;", i);
  int l = sprintf (buff, "VALUE %s 1 %d\r\n", key, vlen);
  write_out (&c->Out, buff, l);
  write_out (&c->Out, s, vlen);
  write_out (&c->Out, "\r\n", 2);
  return 0;
}

static int memcache_write_res (struct connection *c, const char *key, int res) {
  if (res == REQ_WAITING) {
    get_hits++;
    return return_one_key_int (c, key, 2);
  }
  if (res == REQ_RUNNING) {
    get_hits++;
    return return_one_key_int (c, key, 3);
  }
  if (res == EXIT_SUCCESS) {
    get_hits++;
    return return_one_key_int (c, key, 0);
  }
  if (res == EXIT_FAILURE) {
    get_hits++;
    return return_one_key_int (c, key, 1);
  }
  get_missed++;
  return 0;
}

int memcache_get (struct connection *c, const char *key, int key_len) {
  vkprintf (3, "memcache_get_process: key='%s'\n", key);
  cmd_get++;
  long long prog_id;
  int res = -1;

  /* the most frequent request */
  if (key_len >= 8 && !memcmp (key, "request", 7) && sscanf (key + 7, "%lld", &prog_id) == 1) {
    struct forth_request *R = hash_get_req_f (prog_id);
    if (R != NULL) {
      res = R->res;
    }
    return memcache_write_res (c, key, res);
  }

  if (key_len >= 7 && !memcmp (key, "output", 6) && sscanf (key + 6, "%lld", &prog_id) == 1) {
    struct forth_request *R = hash_get_req_f (prog_id);
    if (R != NULL) {
      res = R->res;
    }
    if ((res == EXIT_SUCCESS || res == EXIT_FAILURE) && R->prog != NULL) {
      get_hits++;
      return_one_key (c, key, R->prog, R->size);
      if (R->size >= 1024) {
        forth_request_unload_prog (R);
      }
      return 0;
    }
  }

  int signo;
  if (key_len >= 18 && !memcmp (key, "last_failure_prog", 17) && sscanf (key + 17, "%d", &signo) == 1) {
    vkprintf (4, "get(\"error\", signo: %d)\n", signo);
    switch (signo) {
      case 0: return stat_failure_write (c, key, &not_zero_exit_code);
      case SIGABRT: return stat_failure_write (c, key, &sigabrt);//6
      case SIGKILL: return stat_failure_write (c, key, &sigkill);//9
      case SIGSEGV: return stat_failure_write (c, key, &sigsegv);//11
      case SIGTERM: return stat_failure_write (c, key, &sigterm);//15
      case SIGXCPU: return stat_failure_write (c, key, &sigxcpu);//24
      default: return stat_failure_write (c, key, &sigother);
    }
  }

  if (key_len >= 5 && !strncmp (key, "stats", 5)) {
    int stats_len = image_prepare_stats (c);
    return_one_key (c, key, stats_buffer, stats_len);
    return 0;
  }
  get_missed++;
  return 0;
}

int memcache_version (struct connection *c) {
  cmd_version++;
  write_out (&c->Out, "VERSION "VERSION"\r\n", 9 + sizeof(VERSION));
  return 0;
}

//extern long long malloc_mem;
//extern long long zalloc_mem;

static int child_check_limits (pid_t pid) {
  long long a[2];
  if (am_get_memory_usage (pid, a, 2) < 0) {
    return -1;
  }
  if (a[0] > vmsize_limit) {
    vkprintf (1, "[%d] exceeds vmsize_limit\n", (int) pid);
    return 1;
  }
  if (a[1] > rss_memory_limit) {
    vkprintf (1, "[%d] exceeds rss_memory_limit\n", (int) pid);
    return 1;
  }
  return 0;
}

#define SB_FSPRINT_I64(x) sb_printf (&sb, "%s\t%lld\n", #x, x.total)

int image_prepare_stats (struct connection *c) {
  int number_task_in_queue = queue_size;
  const double child_working_time = get_rusage_time (RUSAGE_CHILDREN);
  const double main_working_time = get_rusage_time (RUSAGE_SELF);

  stats_buffer_t sb;
  sb_prepare (&sb, c, stats_buffer, STATS_BUFF_SIZE);
  sb_memory (&sb, AM_GET_MEMORY_USAGE_SELF + AM_GET_MEMORY_USAGE_OVERALL);

  sb_printf (&sb,
        "all_results_memory\t%d\n"
        "max_all_results_memory\t%d\n"
        "cmd_get\t%lld\n"
        "cmd_set\t%lld\n"
        "get_hits\t%lld\n"
        "get_misses\t%lld\n"
        "cmd_version\t%lld\n"
        "cmd_stats\t%lld\n"
        "max_process_number\t%d\n"
        "working_child_process\t%d\n"
        "number_tasks_in_queue\t%d\n"
        "complete_tasks\t%lld\n"
        "main_process_rusage_time\t%.6lf\n"
        "average_task_time\t%.6lf\n"
        "max_task_time\t%.6lf\n",
        all_results_memory,
        max_all_results,
        cmd_get,
        cmd_set,
        get_hits,
        get_missed,
        cmd_version,
        cmd_stats,
        max_process_number,
        child_process,
        number_task_in_queue,
        complete_tasks,
        main_working_time,
        safe_div (child_working_time, complete_tasks),
        max_task_time
        );

   SB_FSPRINT_I64(not_zero_exit_code);
   SB_FSPRINT_I64(sigabrt);
   SB_FSPRINT_I64(sigsegv);
   SB_FSPRINT_I64(sigkill);
   SB_FSPRINT_I64(sigxcpu);
   SB_FSPRINT_I64(sigterm);
   SB_FSPRINT_I64(sigother);

   SB_PRINT_I64(unloaded_results);
   SB_PRINT_I64(results_unload_bytes);
   const int queue_with_output_size = with_output_free_list.size;
   SB_PRINT_I32(queue_with_output_size);
   const int queue_without_output_size = without_output_free_list.size;
   SB_PRINT_I32(queue_without_output_size);
   SB_PRINT_I32(requests_in_hash);
   SB_PRINT_I64(mmap_errors);
   SB_PRINT_I32(last_mmap_errno);
   SB_PRINT_I64(forth_output_errors);
   SB_PRINT_I32(daemonize);
   SB_PRINT_I32(main_nice);
   SB_PRINT_I32(job_nice);
   SB_PRINT_I32(max_load_image_area);
   SB_PRINT_I64(vmsize_limit);
   SB_PRINT_I64(rss_memory_limit);
   SB_PRINT_I64(memory_limit);
   SB_PRINT_I64(map_limit);
   SB_PRINT_I32(threads_limit);
   int shared_memory_size = MAX_SHARED_MEMORY_SIZE;
   SB_PRINT_I32(shared_memory_size);
   sb_printf (&sb, "version\t%s\n", FullVersionStr);

   return sb.pos;
}

int memcache_stats (struct connection *c) {
  cmd_stats++;
  int stats_len = image_prepare_stats (c);
  write_out (&c->Out, stats_buffer, stats_len);
  write_out (&c->Out, "END\r\n", 5);
  return 0;
}

/*
 *
 *      END (PROXY MEMCACHE SERVER)
 *
 */

void reopen_logs (void) {
  int fd;
  fflush (stdout);
  fflush (stderr);
  if ((fd = open ("/dev/null", O_RDWR, 0)) != -1) {
    dup2 (fd, 0);
    dup2 (fd, 1);
    dup2 (fd, 2);
    if (fd > 2) {
      close (fd);
    }
  }
  if (logname && (fd = open (logname, O_WRONLY|O_APPEND|O_CREAT, 0640)) != -1) {
    dup2 (fd, 1);
    dup2 (fd, 2);
    if (fd > 2) {
      close (fd);
    }
  }
  vkprintf (1, "logs reopened.\n");
}

static int interrupted_by_signal (void) {
  static const int interrupting_signal_mask = (1 << SIGTERM) | (1 << SIGINT);
  return ((int) pending_signals) & interrupting_signal_mask;
}

static void sigint_immediate_handler (const int sig) {
  kwrite (2, "SIGINT handled immediately.\n", 28);
  exit (1);
}

static void sigterm_immediate_handler (const int sig) {
  kwrite (2, "SIGTERM handled immediately.\n", 29);
  exit (1);
}

static void main_sig_handler (const int sig) {
  /* since next operator isn't atomic for 32-bit version, */
  /* sigaction function was used for blocking other signals changing pending_signal variable (sa_mask field) */
  pending_signals |= 1LL << sig;
  if (sig == SIGINT) {
    signal (SIGINT, sigint_immediate_handler);
  }
  if (sig == SIGTERM) {
    signal (SIGTERM, sigterm_immediate_handler);
  }
}

static void pending_signals_clear_bit (const sigset_t *ss, const int sig) {
  sigset_t old;
  int r = sigprocmask (SIG_BLOCK, ss, &old);
  assert (!r);
  pending_signals &= ~(1LL << sig);
  r = sigprocmask (SIG_SETMASK, &old, NULL);
  assert (!r);
}

void sigterm_child_handler (const int sig) {
  kwrite (2, "child caught SIGTERM\n", 21);
  print_backtrace ();
  signal (sig, SIG_DFL);
  raise (sig);
}

void sigsegv_child_handler (const int sig) {
  kwrite (2, "SIGSEGV caught, terminating program\n", 36);
  print_backtrace ();
  signal (sig, SIG_DFL);
  raise (sig);
}

void sigabrt_child_handler (const int sig) {
  kwrite (2, "SIGABRT caught, terminating program\n", 36);
  print_backtrace ();
  signal (sig, SIG_DFL);
  raise (sig);
}

void cron (void) {
  create_all_outbound_connections ();
}

static void copy_shared_memory_output (const long long prog_id, int shm_descriptor, struct forth_request *E) {
  if (shm_descriptor < 0) { return; }
  struct forth_output *S = (struct forth_output *) mmap (NULL, MAX_SHARED_MEMORY_SIZE, PROT_READ, MAP_SHARED, shm_descriptor, 0);
  if (S == MAP_FAILED) {
    last_mmap_errno = errno;
    S = NULL;
    vkprintf (1, "copy_shared_memory_output, mmap failed\n%m\n");
    mmap_errors++;
  }
  if (S != NULL) {
    if (S->prog_id != prog_id || (S->l < 0 || S->l > (int) sizeof (S->s))) {
      forth_output_errors++;
      E->size = 0;
      E->prog = strdup ("");
      assert (E->prog);
    } else {
      if (max_task_time < S->working_time) {
        max_task_time = S->working_time;
      }
      E->size = S->l;
      E->prog = malloc (E->size + 1);
      assert (E->prog);
      memcpy (E->prog, S->s, E->size);
      E->prog[E->size] = 0;
    }
    E->timeout = now + result_living_time;
    munmap (S, MAX_SHARED_MEMORY_SIZE);
    all_results_memory += E->size + 1;
    forth_request_run_output_gc ();
  }
}

int shared_memory_init (void) {
  int i;
  shm_no = (max_process_number > 0) ? max_process_number : -1;
  assert (shm_no > 0);
  for (i = 0; i < shm_no; i++) {
    char shm_filename[128];
    /* For portable use, name should have an initial slash (/) and contain no embedded slashes. */
    int l = snprintf (shm_filename, sizeof (shm_filename) - 2, "/img%d_%d_%d_%d", getpid (), now, port, i);
    assert (l < (int) sizeof (shm_filename));
    shm_filename[l] = 0;
    SHM[i] = shm_open (shm_filename, O_RDWR | O_CREAT | O_EXCL, 0640);
    if (SHM[i] < 0) {
      kprintf ("Couldn't create share memory object %s\n%m\n", shm_filename);
      shm_no = i;
      return 0;
    }
    ftruncate (SHM[i], MAX_SHARED_MEMORY_SIZE);
    if (shm_unlink (shm_filename) < 0) {
      kprintf ("shm_unlink (%s) failed. %m\n", shm_filename);
      shm_no = i;
      return 0;
    }
  }
  return 1;
}

int sfd;

void start_server (void) {
  int i;
  int prev_time;

  init_epoll ();
  init_netbuffers ();

  prev_time = 0;

  image_reserved_words_hashtable_init ();
  vkprintf (4, "progname: %s\n", progname);
  map_limit = 0;
  if (map_limit > max_virtual_memory) {
    map_limit = max_virtual_memory;
  }
  image_init (progname, max_load_image_area, memory_limit, map_limit, 0, threads_limit);

  if (daemonize) {
    setsid ();
    reopen_logs ();
  }

  if (change_user_group (username, groupname) < 0) {
    kprintf ("fatal: cannot change user to %s, group to %s\n", username ? username : "(none)", groupname ? groupname : "(none)");
    exit (1);
  }

  if (!shared_memory_init ()) {
    exit (1);
  }

  init_listening_connection (sfd, &ct_image_engine_server, &memcache_methods);

  signal (SIGXCPU, SIG_DFL);
  signal (SIGPIPE, SIG_IGN);
  signal (SIGIO, SIG_IGN);

  sigset_t signal_set;
  sigemptyset (&signal_set);
  sigaddset (&signal_set, SIGINT);
  sigaddset (&signal_set, SIGTERM);
  sigaddset (&signal_set, SIGUSR1);
  sigaddset (&signal_set, SIGCHLD);
  if (daemonize) {
    sigaddset (&signal_set, SIGHUP);
  }
  struct sigaction act;
  act.sa_handler = main_sig_handler;
  act.sa_mask = signal_set;
  act.sa_flags = SA_NOCLDSTOP;
  for (i = 1; i <= SIGRTMAX; i++) {
    if (sigismember (&signal_set, i)) {
      if (sigaction (i, &act, NULL) < 0) {
        kprintf ("sigaction (%d) failed. %m\n", i);
        exit (1);
      }
    }
  }

  if (verbosity > 0) {
    char *cwd = getcwd (stats_buffer, STATS_BUFF_SIZE - 10);
    if (cwd != NULL) {
      kprintf ("Current working directory: %s\n", cwd);
    }
  }

  for (i = 0; ; i++) {
    if (verbosity > 0 && !(i & 1023)) {
      kprintf ("epoll_work(): %d out of %d connections, network buffers: %d used, %d out of %d allocated\n",
          active_connections, maxconn, NB_used, NB_alloc, NB_max);
    }

    epoll_work (11);
    if (now != prev_time) {
      prev_time = now;
      cron();
    }

    if (interrupted_by_signal ()) {
      break;
    }

    if (pending_signals & (1LL << SIGCHLD)) {
      pending_signals_clear_bit (&signal_set, SIGCHLD);
      vkprintf (1, "got SIGCHLD.\n");
      //transaction_check_child_status ();
    }

    if (pending_signals & (1LL << SIGHUP)) {
      pending_signals_clear_bit (&signal_set, SIGHUP);
      vkprintf (1, "got SIGHUP.\n");
      //sync_binlog (2);
    }

    if (pending_signals & (1LL << SIGUSR1)) {
      pending_signals_clear_bit (&signal_set, SIGUSR1);
      vkprintf (1, "got SIGUSR1, rotate logs.\n");
      reopen_logs ();
      //sync_binlog (2);
    }

    while (child_process > 0) {
      int status;
      pid_t pid = waitpid (-1, &status, WNOHANG);
      if (pid <= 0) {
        break;
      }
      int j;
      for (j = 0; j < child_process; j++) {
        if (P[j] == pid) {
          char *error = NULL;
          complete_tasks++;
          struct forth_request *E = RP[j];
          E->res = EXIT_FAILURE;
          if (WIFEXITED(status)) {
            E->res = WEXITSTATUS(status);
            if (E->res != EXIT_SUCCESS) {
              E->res = EXIT_FAILURE;
              stat_failure_incr (&not_zero_exit_code, E);
            }
          } else if (WIFSIGNALED(status)) {
            int t = WTERMSIG(status);
            vkprintf (3, "WTERMSIG(status): %d, pid: %d\n", t, (int) pid);
            switch (t) {
              case SIGABRT:
                stat_failure_incr (&sigabrt, E);
                error = "SIGABRT";
              break;
              case SIGSEGV:
                stat_failure_incr (&sigsegv, E);
                error = "SIGSEGV";
              break;
              case SIGKILL:
                stat_failure_incr (&sigkill, E);
                error = "SIGKILL";
              break;
              case SIGXCPU:
                stat_failure_incr (&sigxcpu, E);
                error = "SIGXCPU";
              break;
              case SIGTERM:
                stat_failure_incr (&sigterm, E);
                error = "SIGTERM";
              break;
              default:
                stat_failure_incr (&sigother, E);
                error = "SIGOTHER";
              break;
            }
          }
          if (E->prog != NULL) {
            free (E->prog);
            E->size = 0;
          }
          --child_process;
          P[j] = P[child_process];
          RP[j] = RP[child_process];

          int tmp = SHM[j]; SHM[j] = SHM[child_process]; SHM[child_process] = tmp;
          if (error == NULL) {
            copy_shared_memory_output (E->prog_id, SHM[child_process], E);
          } else {
            E->size = strlen (error);
            E->prog = strdup (error);
            assert (E->prog);
            E->timeout = now + result_living_time;
            all_results_memory += E->size + 1;
            forth_request_run_output_gc ();
          }
          queue_add (E);
          break;
        }
      }
    }

    for (i = 0; i < child_process; i++) {
      if (RP[i]->priority || child_check_limits (P[i]) > 0) {
        if (RP[i]->priority) {
          kill (P[i], SIGKILL);
        } else {
          kill (P[i], SIGTERM);
          RP[i]->priority |= 1;
        }
      }
    }

    while (child_process < max_process_number && queue_size > 0) {
      pid_t pid = fork ();
      struct forth_request *E = HEAP[1];
      assert (E->prog);
      queue_size--;
      if (!pid) {
        /* child process */
        signal (SIGTERM, sigterm_child_handler);
        signal (SIGSEGV, sigsegv_child_handler);
        signal (SIGABRT, sigabrt_child_handler);
        close (sfd); sfd = -1; /* image-engine doesn't restart when port is busy */
        struct rlimit mlimit, cpu_limit;
        cpu_limit.rlim_cur = cpu_limit.rlim_max = 20 * threads_limit;
        if (setrlimit (RLIMIT_CPU, &cpu_limit) < 0) {
          vkprintf (1, "setrlimit (CPU_LIMIT (%lld,%lld)) fail. %m", (long long) cpu_limit.rlim_cur, (long long) cpu_limit.rlim_max);
        }
        mlimit.rlim_cur = mlimit.rlim_max = vmsize_limit;
        if (setrlimit (RLIMIT_AS, &mlimit) < 0) {
          vkprintf (1, "setrlimit (RLIMIT_AS (%lld,%lld)) fail. %m", (long long) mlimit.rlim_cur, (long long) mlimit.rlim_max);
        }
        if (job_nice > main_nice && nice (job_nice - main_nice) < 0) {
          vkprintf (1, "nice (%d) fail. %m\n", job_nice - main_nice);
        }
        char *cmd = E->prog;
        int r = image_exec (E->prog_id, cmd, strlen (cmd), (int) getpid (), SHM[child_process]);
        image_done (); //remove temporary files created by GraphicsMagick
        exit (r);
      } else {
        /* parent */
        E->priority = 0;
        P[child_process] = pid;
        RP[child_process] = E;
        child_process++;
        E->res = REQ_RUNNING;
        heapify_front (HEAP[queue_size + 1], 1);
      }
    }
    if (quit_steps && !--quit_steps) break;
  }

  epoll_close (sfd);
  close (sfd);

  image_done ();
  kprintf ("Main process terminated (pending_signals: 0x%llx).\n", pending_signals);
}

/*
 *
 *		MAIN
 *
 */

void usage (void) {
  printf ("usage: image-engine [-d] [-v] [-p<port>] [-u<username>] [-g<groupname>] [-c<max-conn>] [-l<log_name>] [-P<max-child-process-number>][-H<max-all-child-process-result>] [-n<nice>] [-N<nice>] [-A<area>] [-M<memory>]\n"
          "%s\n"
          "[-p<port>]\tTCP port number to listen on (default: %d)\n"
          "[-u<username>]\tassume identity of <username> (only when run as root)\n"
          "[-c<max_conn>]\tmax simultaneous connections, default is %d\n"
          "[-v]\t\tverbose\n"
          "[-h]\t\tprint this help and exit\n"
          "[-b<backlog>]\n"
          "[-l<log_name>]\tlog... about something\n"
          "[-P<max-child-process-number>]\t\n"
          "[-H<max-all-child-process-result>]\tdefault 16m\n"
          "[-n<nice>]\tset main process nice (range from -20 to 19)\n"
          "[-N<nice>]\tset job process nice (range from -20 to 19)\n"
          "[-M<memory>]\tset GraphicsMagick memory limit (default: 256m)\n"
          "[-R<memory>]\tset resident set size limit (default: 1g)\n"
          "[-A<area>]\tset max load image area in pixels\n"
          "[-T<threads>]\tset threads limit (default: %d)\n",
          FullVersionStr,
          TCP_PORT,
          MAX_CONNECTIONS,
          threads_limit
         );
  exit (2);
}

int main (int argc, char *argv[]) {
  int i, k;
  long long x;
  char c;

  set_debug_handlers ();
  progname = argv[0];
  while ((i = getopt (argc, argv, "A:H:M:N:P:R:T:b:c:g:l:n:p:dhu:v")) != -1) {
    switch (i) {
     case 'A':
     case 'H':
     case 'M':
     case 'R':
       c = 0;
       assert (sscanf (optarg, "%lld%c", &x, &c) >= 1);
       switch (c | 0x20) {
         case 'k':  x <<= 10; break;
         case 'm':  x <<= 20; break;
         case 'g':  x <<= 30; break;
         case 't':  x <<= 40; break;
         default: assert (c == 0x20);
       }
       if (i == 'A' && x >= (1 << 20) && x <= (1 << 30)) {
         max_load_image_area = x;
       }
       if (i == 'H' && x >= (1LL << 20) && x <= max_virtual_memory) {
         max_all_results = x;
       }
       if (i == 'M' && x >= (128LL << 20) && x <= max_virtual_memory) {
         memory_limit = x;
       }
       if (i == 'R' && x >= (64LL << 20) && x <= rss_memory_limit) {
         rss_memory_limit = x;
       }
       break;
    case 'N':
      job_nice = atoi (optarg);
      break;
    case 'P':
      k = atoi (optarg);
      if (k >= 1 && k <= MAX_CHILD_PROCESS) {
        max_process_number = k;
      }
      break;
    case 'T':
      k = atoi (optarg);
      if (k >= 1 && k <= MAX_THREADS) {
        threads_limit = k;
      }
      break;
    case 'v':
      verbosity++;
      break;
    case 'g':
      groupname = optarg;
      break;
    case 'h':
      usage ();
      return 2;
    case 'b':
      backlog = atoi (optarg);
      if (backlog <= 0) {
        backlog = BACKLOG;
      }
      break;
    case 'c':
      maxconn = atoi (optarg);
      if (maxconn <= 0 || maxconn > MAX_CONNECTIONS) {
        maxconn = MAX_CONNECTIONS;
      }
      break;
    case 'n':
      main_nice = atoi (optarg);
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
    }
  }

  if (max_process_number) {
    kprintf_multiprocessing_mode_enable ();
  } else {
    kprintf ("max-child-process-number isn't stricly positive\n");
    usage ();
  }

  if (job_nice < main_nice) {
    kprintf ("Job process nice is smaller than main process nice.\nSee man 2 nice: Only the super user may specify a negative increment, or priority increase.\n");
    usage ();
  }

  if (main_nice < -20 || main_nice > 19) {
    kprintf ("Main process nice should be in the range -20 to 19.\n");
    exit (1);
  }

  if (job_nice < -20 || job_nice > 19) {
    kprintf ("Job process nice should be in the range -20 to 19.\n");
    exit (1);
  }

  if (nice (main_nice) < 0) {
    kprintf ("nice (%d) fail. %m\n", main_nice);
    exit (1);
  }

  vkprintf (3, "Command line parsed\n");

  if (!username && maxconn == MAX_CONNECTIONS && geteuid ()) {
    maxconn = 1000; //not for root
  }

  dynamic_data_buffer_size = 1 << 22;

  if (raise_file_rlimit (maxconn + 16) < 0) {
    kprintf ("fatal: cannot raise open file limit to %d\n", maxconn + 16);
    exit (1);
  }

  aes_load_pwd_file (0);

  if (change_user_group (username, groupname) < 0) {
    kprintf ("fatal: cannot change user to %s, group to %s\n", username ? username : "(none)", groupname ? groupname : "(none)");
    exit (1);
  }

  init_dyn_data();

  sfd = server_socket (port, settings_addr, backlog, 0);
  if (sfd < 0) {
    kprintf ("cannot open server socket at port %d: %m\n", port);
    exit (1);
  }

  req_structures_init ();
  start_time = time(0);

  start_server ();
  return 0;
}

