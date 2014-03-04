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

#define _FILE_OFFSET_BITS 64

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <stddef.h>
#include <stdarg.h>
#include <limits.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <time.h>
#include <zlib.h>
#include <sys/prctl.h>
#include <openssl/sha.h>

#include "net-connections.h"
#include "net-events.h"
#include "crc32.h"
#include "base64.h"
#include "copyexec-data.h"
#include "server-functions.h"
#include "kdb-data-common.h"
#include "net-crypto-rsa.h"
#include "../filesys/filesys-utils.h"


#define MAX_LOGEVENT_SIZE 0x100000

char *public_key_prefix = NULL;
char *tmp_dir = NULL;
int instance_mask = 0;
long long aux_log_readto_pos, aux_log_read_start;
int tot_ignored, tot_interrupted, tot_cancelled, tot_terminated, tot_failed, tot_decryption_failed, tot_io_failed;
int fd_aux_binlog = -1;
int max_output_size = 16384;
int first_transaction_id;
int sfd;

pid_t main_pid, results_client_pid;
int main_process_creation_time, results_client_creation_time;

/* if this variable is set, copyexec-engine will exit on decryption, mkdir, chdir, fork failures
   otherwise copyexec-engine will alloc logevent with status ts_io_failed or ts_decryption_failed
*/
static const int exit_on_failure = 1;

static void copyexec_abort (void) {
  assert (main_pid == getpid ());
  flush_binlog_last ();
  sync_binlog (2);
  exit (1);
}

static void handle_failure (void) {
  if (exit_on_failure) {
    copyexec_abort ();
  }
}

#ifdef SUPERUSER
static int check_file_permissions (char *filename, struct stat *st, int level) {
  if (st->st_uid) {
    vkprintf (level, "%s: root isn't file owner.\n", filename);
    if (!level) {
      exit (1);
    }
    return -1;
  }

  if (st->st_gid) {
    vkprintf (level, "%s: gid isn't equal to 0.\n", filename);
    if (!level) {
      exit (1);
    }
    return -2;
  }

  if (st->st_mode & S_IRWXO) {
    vkprintf (level, "%s: other has file permissions (%c%c%c), mode = 0%06o.\n", filename,
             (st->st_mode & S_IROTH) ? 'r':'-', (st->st_mode & S_IWOTH) ? 'w':'-', (st->st_mode & S_IXOTH) ? 'x':'-',
             (int) st->st_mode);
    if (!level) {
      exit (1);
    }
    return -3;
  }
  return 0;
}

static void check_superuser_exe (void) {
  char a[PATH_MAX];
  struct stat st;
  if (stat ("/proc/self/exe", &st) < 0) {
    kprintf ("stat (\"/proc/self/exe\" failed. %m\n");
    return;
  }
  int n = readlink ("/proc/self/exe", a, PATH_MAX);
  if (n < 0 || n >= PATH_MAX) {
    strcpy (a, "copyexec-engine");
  } else {
    a[n] = 0;
  }
  check_file_permissions (a, &st, 0);
}

void check_superuser (void) {
  if (geteuid ()) {
    kprintf ("error: you cannot perform this operation unless you are root.\n");
    exit (1);
  }
  check_superuser_exe ();
}

void check_superuser_main_binlog (void) {
  struct stat st;
  if (fstat (Binlog->fd, &st) < 0) {
    kprintf ("fstat (Binlog->fd) failed. Binlog->info->filename: %s. %m\n", Binlog->info->filename);
    exit (1);
  }
  check_file_permissions (Binlog->info->filename, &st, 0);
}
#else
static int check_file_permissions (char *filename, struct stat *st, int level) { return 0; }
void check_superuser (void) { }
void check_superuser_main_binlog (void) { }
#endif

static char transaction_dir[PATH_MAX], cur_transaction_binlog_name[PATH_MAX];
static int fd_transaction_logevents;
static int cur_transaction_id, cmd_id;
static int cur_transaction_result;
//static int cur_transaction_last_cmd_status;
static char cur_transaction_name[20], cur_transaction_hash[28];

typedef struct transaction {
  long long binlog_pos;
  long long st_dev;
  long long st_ino;
  struct transaction *next, *prev; //running transaction list
  struct transaction *hnext;
  int creation_time;
  int transaction_id;
  int mask;
  int result;
  pid_t pid;
  enum transaction_status status;
} transaction_t;

typedef transaction_t tlist_t;

tlist_t child_running_list, auto_running_list;

void tlist_clear (tlist_t *L) {
  L->next = L->prev = L;
}

void running_lists_init (void) {
  tlist_clear (&child_running_list);
  vkprintf (4, "tlist_clear (&auto_running_list)\n");
  tlist_clear (&auto_running_list);
}

void tlist_insert (tlist_t *L, transaction_t *T) {
  transaction_t *u = L->prev, *v = L;
  u->next = T; T->prev = u;
  v->prev = T; T->next = v;
}

void tlist_remove (transaction_t *T) {
  vkprintf (4, "tlist_remove (%p)\n", T);
  transaction_t *u = T->prev, *v = T->next;
  if (u == NULL && v == NULL) {
    return;
  }
  assert (u != NULL && v != NULL);
  u->next = v;
  v->prev = u;
  T->prev = T->next = NULL;
}

static int get_tlist_size (tlist_t *L) {
  int r = 0;
  transaction_t *T;
  for (T = L->next; T != L; T = T->next) {
    r++;
  }
  return r;
}

void get_running_lists_size (int *child_size, int *auto_size) {
  *child_size = get_tlist_size (&child_running_list);
  *auto_size = get_tlist_size (&auto_running_list);
}

/****************************************** IO ********************************************************/
#define	BUFFSIZE	16777216
static char Buff[BUFFSIZE], *rptr, *wptr;
static int fd_close_success_level = 3;
static void fd_close (int *fd) {
  if (*fd >= 0) {
    while (1) {
      if (close (*fd) < 0) {
        if (errno == EINTR) {
          continue;
        }
        vkprintf (2, "close (%d) failed. %m\n", *fd);
        return;
      }
      vkprintf (fd_close_success_level, "fd = %d: was successfully closed.\n", *fd);
      *fd = -1;
      return;
    }
  }
  vkprintf (3, "skip closing negative fd = %d\n", *fd);
}

static void clearin (void) {
  rptr = wptr = Buff + BUFFSIZE;
}

static void *readin (int fd, size_t len) {
  vkprintf (4, "readin (fd: %d, len: %lld)\n", fd, (long long) len);
  assert (len >= 0);
  if (rptr + len <= wptr) {
    return rptr;
  }
  if (wptr < Buff + BUFFSIZE) {
    return 0;
  }
  memcpy (Buff, rptr, wptr - rptr);
  wptr -= rptr - Buff;
  rptr = Buff;
  int r;
  do {
    r = read (fd, wptr, Buff + BUFFSIZE - wptr);
  } while (r < 0 && errno == EINTR);
  if (r < 0) {
    kprintf ("error reading file: %m\n");
  } else {
    wptr += r;
  }
  if (rptr + len <= wptr) {
    return rptr;
  } else {
    return 0;
  }
}

static void readadv (size_t len) {
  assert (len >= 0 && len <= (size_t) (wptr - rptr) );
  rptr += len;
}

/****************************************** process ********************************************************/
static int argc_copy;
static char **argv_copy;

void copy_argv (int argc, char *argv[]) {
  int i;
  argc_copy = argc;
  argv_copy = calloc (argc, sizeof (argv_copy[0]));
  for (i = 0; i < argc; i++) {
    argv_copy[i] = argv[i];
  }
}

int interrupted_by_signal (void) {
  static const int interrupting_signal_mask = (1 << SIGTERM) | (1 << SIGINT);
  return ((int) pending_signals) & interrupting_signal_mask;
}

void change_process_name (char *new_name) {
  /* change process name in top */
  if (prctl (PR_SET_NAME, (unsigned long) new_name, 0, 0, 0) < 0) {
    vkprintf (1, "changing process name to %s failed. %m\n", new_name);
  }
  /* change process name in ps */
  int i;
  for (i = 1; i < argc_copy; i++) {
    memset (argv_copy[i], 0, strlen (argv_copy[i]));
  }
  i = strlen (new_name) - strlen (argv_copy[0]);
  if (i < 0) {
    i = 0;
  }
  strncpy (argv_copy[0], new_name + i, strlen (argv_copy[0]));
}

static void transaction_change_process_name (int transaction_id) {
  assert (snprintf (cur_transaction_name, sizeof (cur_transaction_name), "cpexec-t%07d", transaction_id % 10000000) < (int) sizeof (cur_transaction_name));
  change_process_name (cur_transaction_name);
}

void copyexec_main_process_init (void) {
  main_pid = getpid ();
  main_process_creation_time = get_process_creation_time (main_pid);
  int t = time (NULL) - main_process_creation_time;
  if (!(t > -10 && t < 10)) {
    vkprintf (1, "copyexec_main_process_init: |time (NULL) - get_process_creation_time (getpid ())| >= 10\n");
  }
  running_lists_init ();
}

int get_booting_time (void) {
  int fd = open ("/proc/stat", O_RDONLY);
  if (fd < 0) {
    kprintf ("get_booting_time: open (\"/proc/stat\", O_RDONLY) failed. %m\n");
    exit (1);
  }
  Buff[0] = '\n';
  int s = read (fd, Buff + 1, BUFFSIZE - 1);
  if (s < 0) {
    fd_close (&fd);
    kprintf ("get_booting_time: read failed. %m\n");
    exit (1);
  }
  fd_close (&fd);
  if (s == BUFFSIZE - 1) {
    kprintf ("get_booting_time: BUFFSIZE (%d) too small.\n", BUFFSIZE);
    exit (1);
  }
  Buff[s] = 0;
  char *p;
  p = Buff;
  do {
    p = strstr (p, "\nbtime");
    if (p == NULL) {
      vkprintf (1, "%s\n", Buff);
      kprintf ("get_booting_time: btime substring didn't find.\n");
      exit (1);
    }
    int booting_time;
    if (sscanf (p + 6, "%d", &booting_time) == 1) {
      vkprintf (2, "booting time is %d.\n", booting_time);
      return booting_time;
    }
    p += 6;
  } while (1);
  return 0;
}

struct {
  long clk_tck;
  int booting_time;
} gpct = {.booting_time = 0};

int get_process_creation_time (pid_t pid) {
  char a[32];
  /* avoid zombi deadlock */
  if (kill (pid, 0) < 0) {
    vkprintf (3, "get_process_creation_time: kill (%d, 0) failed. %m\n", (int) pid);
    return 0;
  }

  if (!gpct.booting_time) {
    gpct.booting_time = get_booting_time ();
    gpct.clk_tck = sysconf (_SC_CLK_TCK);
    vkprintf (4, "sysconf (_SC_CLK_TCK) = %ld\n", gpct.clk_tck);
    assert (gpct.clk_tck > 0);
  }

  assert (snprintf (a, sizeof (a), "/proc/%d/stat", (int) pid) < (int) sizeof (a));
  int fd = open (a, O_RDONLY);
  if (fd < 0) {
    vkprintf (2, "get_process_creation_time: open (\"%s\", O_RDONLY) failed. %m\n", a);
    return 0;
  }
  int s = read (fd, Buff, BUFFSIZE);
  if (s < 0) {
    fd_close (&fd);
    vkprintf (2, "get_process_creation_time: read from \"%s\" failed. %m\n", a);
    return 0;
  }
  int t = fd_close_success_level;
  fd_close_success_level += 2;
  fd_close (&fd);
  fd_close_success_level = t;
  if (s == BUFFSIZE) {
    vkprintf (2, "get_process_creation_time: buffer overflowed.\n");
    return 0;
  }
  Buff[s] = 0;
  long long start_time;
  /* start_time is 22nd argument in /proc/${PID}/stat */
  if (sscanf (Buff, "%*s%*s%*s%*s%*s%*s%*s%*s%*s%*s%*s%*s%*s%*s%*s%*s%*s%*s%*s%*s%*s%lld", &start_time) != 1) {
    vkprintf (2, "get_process_creation_time: sscanf failed.\n");
    vkprintf (2, "Buff:\n%s\n", Buff);
    return 0;
  }
  return gpct.booting_time + (start_time / gpct.clk_tck);
}

/**************************************** transaction functions *******************************************/

static int check_is_running (transaction_t *T) {
  return T->creation_time && (T->creation_time == get_process_creation_time (T->pid));
}

#define HASH_MASK 0x3fff
int tot_memory_transactions;
transaction_t *H[HASH_MASK+1];

transaction_t *get_transaction_f (int id, int force) {
  int h = id & HASH_MASK;
  transaction_t **p = &(H[h]), *q;
  while (1) {
    q = *p;
    if (q == NULL) {
      break;
    }
    if (q->transaction_id == id) {
      *p = q->hnext;
      if (force >= 0) {
        q->hnext = H[h];
        H[h] = q;
      }
      return q;
    }
    p = &(q->hnext);
  }
  if (force > 0) {
    //q = zmalloc0 (sizeof (transaction_t));
    q = calloc (1, sizeof (transaction_t));
    if (q == NULL) {
      kprintf ("get_transaction_f: calloc returns NULL. %m\n");
      return NULL;
    }
    tot_memory_transactions++;
    q->transaction_id = id;
    q->hnext = H[h];
    H[h] = q;
    return q;
  }
  return NULL;
}

static inline int transaction_want_exec (transaction_t *T) {
  if (T->status == ts_running) {
    return 0;
  }
  if (T->status == ts_decryption_failed) {
    return 1;
  }

  if (T->status == ts_cancelled) {
    if (T->mask & MASK_RERUN_TRANSACTION) {
      return 1;
    }
    if (T->result == (((int) SIGINT) << 8) || T->result == (((int) SIGTERM) << 8)) {
      return 1;
    }
    return 0;
  }

  return (T->status == ts_interrupted) && (T->mask & MASK_RERUN_TRANSACTION);
}

static transaction_t *get_first_want_exec_transaction (void) {
  int i;
  transaction_t *T = NULL, *U = NULL;
  for (i = 0; i <= HASH_MASK; i++) {
    transaction_t *P;
    for (P = H[i]; P != NULL; P = P->hnext) {
      if (transaction_want_exec (P)) {
        if (!T || T->binlog_pos > P->binlog_pos) {
          T = P;
        }
      } else {
        if (!U || U->binlog_pos < P->binlog_pos) {
          U = P;
        }
      }
    }
  }
  return T ? T : U;
}

static void transaction_free (transaction_t *P) {
  free (P);
  tot_memory_transactions--;
}

static void free_old_transactions (long long start_pos) {
  int i;
  for (i = 0; i <= HASH_MASK; i++) {
    transaction_t *head = NULL, *tail = NULL, *P, *W;
    for (P = H[i]; P != NULL; P = W) {
      W = P->hnext;
      if (P->binlog_pos >= start_pos || P->status == ts_running) {
        P->hnext = NULL;
        if (head) {
          tail->hnext = P;
          tail = P;
        } else {
          head = tail = P;
        }
      } else {
        transaction_free (P);
      }
    }
    H[i] = head;
  }
}

static void get_aux_binlog_jump_log_pos (long long *pos) {
  transaction_t *T = get_first_want_exec_transaction ();
  if (T) {
    *pos = T->binlog_pos;
    transactions = T->transaction_id - 1;
    free_old_transactions (T->binlog_pos);
  } else {
    *pos = 0;
    transactions = 0;
  }
}

static void incr_status_counter (enum transaction_status status, int delta) {
  switch (status) {
    case ts_ignored:
      tot_ignored += delta;
      break;
    case ts_interrupted:
      tot_interrupted += delta;
      break;
    case ts_cancelled:
      tot_cancelled += delta;
      break;
    case ts_terminated:
      tot_terminated += delta;
      break;
    case ts_failed:
      tot_failed += delta;
      break;
    case ts_decryption_failed:
      tot_decryption_failed += delta;
      break;
    case ts_io_failed:
      tot_io_failed += delta;
      break;
    default:
      break;
  }
}

static void transaction_change_status (transaction_t *T, enum transaction_status status) {
  if (T->status == status) {
    vkprintf (1, "transaction_change_status: T->status == status (%d), T->transacion_id: %d\n", status, T->transaction_id);
    return;
  }
  if (T->status != ts_unset) {
    incr_status_counter (T->status, -1);
  }
  T->status = status;
  incr_status_counter (T->status, 1);
}

void do_set_status (struct lev_copyexec_main_transaction_status *E) {
  int status = E->type & 0xff;
  vkprintf (3, "tr%d set status %d.\n", E->transaction_id, (int) status);
  transaction_t *T = get_transaction_f (E->transaction_id, 1);
  assert (T != NULL);
  transaction_change_status (T, status);
  T->binlog_pos = E->binlog_pos;
  T->mask = E->mask;
  T->pid = E->pid;
  T->creation_time = E->creation_time;
  T->result = E->result;
  T->st_dev = E->st_dev;
  T->st_ino = E->st_ino;
}

struct buffered_logevent {
  void *data;
};

void *alloc_buffered_logevent (struct buffered_logevent *L, lev_type_t type, int size) {
  size = (size + 3) & -4;
  assert (size >= 4);
  L->data = calloc (size + 8, 1);
  assert (L->data != NULL);
  memcpy (L->data, &size, 4);
  memcpy (L->data + 8, &type, 4);
  return L->data + 8;
}

static void flush_logevent_to_file (struct buffered_logevent *L) {
  int size;
  memcpy (&size, L->data, 4);
  unsigned logevent_crc32 = compute_crc32 (L->data + 8, size);
  memcpy (L->data + 4, &logevent_crc32, 4);
  size += 8;
  int bytes_written;
  do {
    bytes_written = write (fd_transaction_logevents, L->data, size);
  } while (bytes_written < 0 && errno == EINTR);
  assert (bytes_written == size);
  vkprintf (3, "%d bytes to logevent file from tr%d was written (%.4s)\n", size, cur_transaction_id, ((char *) L->data) + 8);
  free (L->data);
  L->data = NULL;
}

static void get_transaction_dir (transaction_t *T, char dir[PATH_MAX]) {
  assert (snprintf (dir, PATH_MAX, "%s/%08d", tmp_dir, T->transaction_id) < PATH_MAX);
}

static void get_transaction_hash (transaction_t *T, char output[28]) {
  unsigned char a[16], b[20];
  vkprintf (4, "{0} = %lld, {1} = %d, {2} = %d\n", T->binlog_pos, T->transaction_id, T->mask);
  memcpy (a, &T->binlog_pos, 8);
  memcpy (a + 8, &T->transaction_id, 4);
  memcpy (a + 12, &T->mask, 4);
  SHA1 (a, 16, b);
  int r = base64url_encode (b, 20, output, 40);
  assert (!r);
  assert (strlen (output) == 27);
}

static void get_transaction_binlog_name (transaction_t *T, char binlog_name[PATH_MAX]) {
  char dir[PATH_MAX], b[28];
  get_transaction_dir (T, dir);
  get_transaction_hash (T, b);
  assert (snprintf (binlog_name, PATH_MAX, "%s/.binlog.%s", dir, b) < PATH_MAX);
}

static int read_transaction_binlog (transaction_t *T) {
  int fd;
  char binlog_name[PATH_MAX];
  get_transaction_binlog_name (T, binlog_name);
  while (1) {
    fd = open (binlog_name, O_RDONLY);
    if (fd < 0 && errno == EINTR) {
      continue;
    }
    break;
  }

  if (fd < 0) {
    vkprintf (1, "read_transaction_binlog: open (%s) failed. %m\n", binlog_name);
    return COPYEXEC_ERR_OPEN;
  }

  struct stat st;
  if (fstat (fd, &st) < 0) {
    vkprintf (1, "read_transaction_binlog: fstat (%d) failed. %m\n", fd);
    fd_close (&fd);
    return COPYEXEC_ERR_FSTAT;
  }

  if ((long long) st.st_dev != T->st_dev) {
    vkprintf (1, "read_transaction_binlog: st_dev doesn't matched\n");
    fd_close (&fd);
    return COPYEXEC_ERR_ST_DEV;
  }

  if ((long long) st.st_ino != T->st_ino) {
    vkprintf (1, "read_transaction_binlog: st_ino doesn't matched\n");
    fd_close (&fd);
    return COPYEXEC_ERR_ST_INO;
  }

  clearin ();
  int stop = 0, res = COPYEXEC_ERR_UNKNOWN;
  do {
    int le_size;
    unsigned le_crc32;
    void *p = readin (fd, 4);
    if (p == 0) {
      res = COPYEXEC_ERR_READIN;
      break;
    }
    memcpy (&le_size, p, 4); readadv (4);
    if (le_size < 0 || le_size >= MAX_LOGEVENT_SIZE) {
      res = COPYEXEC_ERR_SIZE;
      break;
    }
    p = readin (fd, 4);
    if (p == 0) {
      res = COPYEXEC_ERR_READIN;
      break;
    }
    memcpy (&le_crc32, p, 4); readadv (4);
    p = readin (fd, le_size);
    if (p == 0) {
      res = COPYEXEC_ERR_READIN;
      break;
    }

    if (compute_crc32 (p, le_size) != le_crc32) {
      res = COPYEXEC_ERR_CRC32;
      break;
    }

    struct lev_generic *E = (struct lev_generic *) p;
    if ((E->type & 0xffffff00) == LEV_COPYEXEC_MAIN_TRANSACTION_STATUS) {
      transaction_change_status (T, E->type & 0xff);
      switch (T->status) {
        case ts_interrupted:
        case ts_cancelled:
        case ts_terminated:
        case ts_failed:
          stop = 1;
        default:
          res = COPYEXEC_ERR_STATUS;
          break;
      }
    }
    struct lev_generic *A = alloc_log_event (E->type, le_size, E->a);
    memcpy (A, E, le_size);
    if (compute_uncommitted_log_bytes () > (1 << 18)) {
      flush_binlog_forced (1);
    }
    readadv (le_size);
  } while (!stop);

  fd_close (&fd);

  if (!stop) {
    return res;
  }

  return 0;
}

static void delete_transaction_dir (void) {
  vkprintf (3, "delete_transaction_dir (\"%s\")\n", transaction_dir);
  errno = 0;
  int r = delete_file (transaction_dir);
  if (r < 0) {
    vkprintf (1, "delete_file (\"%s\") returns error code %d. %m\n", transaction_dir, r);
  }
}

static struct lev_copyexec_main_transaction_status *transaction_alloc_log_event_status (transaction_t *T) {
  const int sz = sizeof (struct lev_copyexec_main_transaction_status);
  struct lev_copyexec_main_transaction_status *E = alloc_log_event (LEV_COPYEXEC_MAIN_TRANSACTION_STATUS + T->status, sz, T->transaction_id);
  char *p = (char *) E;
  memset (p + 8, 0, sz - 8);
  return E;
}

static void transaction_finish_execution (transaction_t *T, int erase) {
  vkprintf (4, "transaction_finish_execute (T:%p, T->transaction_id: %d, erase:%d)\n", T, T->transaction_id, erase);
  int r = read_transaction_binlog (T);
  if (r < 0) {
    vkprintf (3, "read_transaction_binlog returns %d error code.\n", r);
    transaction_change_status (T, ts_io_failed);
    struct lev_copyexec_main_transaction_status *E = transaction_alloc_log_event_status (T);
    E->mask = T->mask;
    E->pid = T->pid;
    E->creation_time = T->creation_time;
    E->result = T->result = -r;
    E->st_dev = T->st_dev;
    E->st_ino = T->st_ino;
  } else {
    vkprintf (3, "transaction_finish_execution: transaction_id = %d, status = %d.\n", T->transaction_id, (int) T->status);
  }
  tlist_remove (T);
  get_transaction_dir (T, transaction_dir);
  delete_transaction_dir ();

  if (erase) {
    get_transaction_f (T->transaction_id, -1);
    transaction_free (T);
  }
}

static int cmp_transaction (const void *a, const void *b) {
  const transaction_t *x = *((const transaction_t **) a), *y = *((const transaction_t **) b);
  if (x->transaction_id < y->transaction_id) {
    return -1;
  }
  if (x->transaction_id > y->transaction_id) {
    return 1;
  }
  return 0;
}

int find_running_transactions (void) {
  int i, r = 0, t = 0;
  tlist_t terminated_list;
  tlist_clear (&terminated_list);
  transaction_t *P;
  for (i = 0; i <= HASH_MASK; i++) {
    for (P = H[i]; P != NULL; P = P->hnext) {
      if (P->status == ts_running) {
        if (check_is_running (P)) {
          vkprintf (4, "tlist_insert (&auto_running_list, %p)\n", P);
          tlist_insert (&auto_running_list, P);
          r++;
        } else {
          vkprintf (4, "tlist_insert (&auto_terminated_list, %p)\n", P);
          tlist_insert (&terminated_list, P);
          t++;
        }
      }
    }
  }

  if (t > 0) {
    transaction_t **A = calloc (t, sizeof (A[0]));
    assert (A != NULL);
    for (P = terminated_list.next, i = 0; P != &terminated_list; P = P->next) {
      A[i++] = P;
    }
    qsort (A, t, sizeof (A[0]), cmp_transaction);
    for (i = 0; i < t; i++) {
      vkprintf (4, "find_running_transactions: call transaction_finish_execution\n");
      transaction_finish_execution (A[i], 0);
    }
    free (A);
    vkprintf (3, "found %d terminated transaction(s)\n", t);
  }

  return r;
}


int transaction_check_child_status (void) {
  int res = 0;
  while (1) {
    int status;
    int p = waitpid (-1, &status, WNOHANG);
    if (p < 0) {
      if (errno == EINTR) {
        continue;
      }
      vkprintf (4, "waitpid returns %d. %m\n", p);
      break;
    }

    if (!p) {
      vkprintf (4, "waitpid returns %d.\n", p);
      return res;
    }

    vkprintf (3, "waitpid returns %d.\n", p);

    if (p == results_client_pid) {
      if (WIFEXITED(status)) {
        vkprintf (1, "copyexec-client terminated with exit code %d.\n", WEXITSTATUS(status));
      } else if (WIFSIGNALED(status)) {
        vkprintf (1, "copyexec-client was killed by signal %d.\n", WTERMSIG(status));
      } else {
        vkprintf (1, "copyexec-client terminated (waitpid status: 0x%x)\n", status);
      }
      continue;
    }

    transaction_t *T;
    for (T = child_running_list.next; T != &child_running_list; T = T->next) {
      if (T->pid == p) {
        vkprintf (4, "transaction_check_child_status: call transaction_finish_execution\n");
        transaction_finish_execution (T, 1);
        res++;
        break;
      }
    }
  }

  if (res > 0) {
    vkprintf (3, "transaction_check_child_status: %d transaction(s) finished.\n", res);
  }
  return res;
}

int transaction_child_kill (int sig) {
  int res = 0;
  transaction_t *T;
  for (T = child_running_list.next; T != &child_running_list; T = T->next) {
    if (check_is_running (T)) {
      if (!kill (T->pid, sig)) {
        res++;
      }
    }
  }
  if (res > 0) {
    vkprintf (3, "transaction_child_kill: send signal %d to %d transaction(s).\n", sig, res);
  }
  return res;
}

int transaction_auto_kill (int sig) {
  int res = 0;
  transaction_t *T;
  for (T = auto_running_list.next; T != &auto_running_list; T = T->next) {
    if (check_is_running (T)) {
      if (!kill (T->pid, sig)) {
        res++;
      }
    }
  }
  if (res > 0) {
    vkprintf (3, "transaction_auto_kill: send signal %d to %d transaction(s).\n", sig, res);
  }
  return res;
}

int transaction_check_auto_status (void) {
  int res = 0;
  transaction_t *T, *W;

  for (T = auto_running_list.next; T != &auto_running_list; T = W) {
    W = T->next;
    if (!check_is_running (T)) {
      vkprintf (4, "transaction_check_auto_status: call transaction_finish_execution\n");
      transaction_finish_execution (T, 1);
    }
  }
  if (res > 0) {
    vkprintf (3, "transaction_check_auto_status: %d transaction(s) finished.\n", res);
  }
  return res;
}

typedef struct {
  char *buf;
  int size;
  int cutted_size;
} parted_output_t;

static int read_parted_stdout (int fd, parted_output_t *O) {
  O->buf = NULL;
  O->size = 0;
  O->cutted_size = 0;

  struct stat st;
  if (fstat (fd, &st) < 0) {
    fd_close (&fd);
    return -__LINE__;
  }
  O->size = st.st_size;
  O->cutted_size = (O->size <= max_output_size) ? (O->size) : max_output_size;
  O->buf = malloc (O->cutted_size);
  if (O->buf == NULL) {
    fd_close (&fd);
    return -__LINE__;
  }
  if (lseek (fd, 0, SEEK_SET) != 0) {
    free (O->buf);
    fd_close (&fd);
    return -__LINE__;
  }
  if (O->size <= max_output_size) {
    if (read (fd, O->buf, O->cutted_size) != O->cutted_size) {
      O->cutted_size = 0;
      free (O->buf);
      O->buf = NULL;
      fd_close (&fd);
      return -__LINE__;
    }
  } else {
    const int head_size = (max_output_size - 5) >> 1, tail_size = (max_output_size - 5) - head_size;
    void *b = O->buf;
    if (read (fd, b, head_size) != head_size) {
      O->cutted_size = 0;
      free (O->buf);
      O->buf = NULL;
      fd_close (&fd);
      return -__LINE__;
    }
    if (lseek (fd, -tail_size, SEEK_END) == (off_t) -1) {
      O->cutted_size = 0;
      free (O->buf);
      O->buf = NULL;
      fd_close (&fd);
      return -__LINE__;
    }

    b += head_size;
    memcpy (b, "\n...\n", 5);
    b += 5;
    if (read (fd, b, tail_size) != tail_size) {
      O->cutted_size = 0;
      free (O->buf);
      O->buf = NULL;
      fd_close (&fd);
      return -__LINE__;
    }
  }
  fd_close (&fd);
  return 0;
}

int check_mask (int mask) {
  return ((mask & (MASK_RERUN_TRANSACTION | MASK_IMPORTANT_TRANSACTION | MASK_WAITING_TRANSACTION)) == 0);
}

/************************************ Transactions ************************************************************/

int decrypt_transaction (unsigned char *input, int ilen, int key_id, long long offset, int transaction_id, unsigned char **output, int *olen) {
  *output = NULL;
  *olen = -1;

  char public_key_name[PATH_MAX];
  assert (snprintf (public_key_name, PATH_MAX, "%s%d", public_key_prefix, key_id) < PATH_MAX);

  struct stat st;
  if (stat (public_key_name, &st) < 0) {
    vkprintf (3, "stat (\"%s\") failed. %m\n", public_key_name);
    return COPYEXEC_ERR_STAT;
  }

  if (check_file_permissions (public_key_name, &st, 3) < 0) {
    return COPYEXEC_ERR_PUBKEY_PERMISSIONS;
  }

  void *b;
  int blen;
  int r = rsa_decrypt (public_key_name, 1, input, ilen, &b, &blen, 1);

  if (r < 0) {
    kprintf ("rsa_decrypt returns error code %d.\n", r);
    return COPYEXEC_ERR_DECRYPT;
  }

  if (blen & 3) {
    kprintf ("decrypted transaction len isn't aligned.\n");
    return COPYEXEC_ERR_ALIGN;
  }

  struct lev_copyexec_aux_transaction_header *B = b;
  if (blen < (int) (sizeof (struct lev_copyexec_aux_transaction_header) + sizeof (struct lev_copyexec_aux_transaction_footer))) {
    free (b);
    kprintf ("decrypted transaction len too short.\n");
    return COPYEXEC_ERR_SIZE;
  }

  if ((int) B->size != blen) {
    free (b);
    kprintf ("rsa_decrypted olen didn't match to transaction header size field (position: %lld).\n", offset);
    return COPYEXEC_ERR_DECRYPT;
  }

  if (B->transaction_id != transaction_id) {
    free (b);
    kprintf ("transaction_id didn't match, expected %d. (position: %lld).\n", transaction_id, offset);
    return COPYEXEC_ERR_DECRYPT;
  }

  if (B->binlog_pos != offset) {
    free (b);
    kprintf ("transaction pos didn't match (position: %lld).\n", offset);
    return COPYEXEC_ERR_DECRYPT;
  }
  struct lev_copyexec_aux_transaction_footer *C = b + blen - 20;

  unsigned char decrypted_sha[20];
  SHA1 (b, blen - 20, decrypted_sha);
  if (memcmp (decrypted_sha, C->sha1, 20)) {
    free (b);
    kprintf ("transaction sha1 didn't match (position: %lld).\n", offset);
    return COPYEXEC_ERR_SHA1;
  }
  *output = b;
  *olen = blen;
  return 0;
}

#define ERR_BUFF_SIZE 4096
static void write_transaction_err_logevent (const char *format, ...) {
  char err_buff[ERR_BUFF_SIZE];
  va_list ap;
  va_start (ap, format);
  int l = vsnprintf (err_buff, ERR_BUFF_SIZE, format, ap);
  va_end (ap);
  if (l > ERR_BUFF_SIZE) {
    l = ERR_BUFF_SIZE;
  }
  vkprintf (3, "%.*s\n", l, err_buff);
  struct buffered_logevent L;
  struct lev_copyexec_main_transaction_err *A = alloc_buffered_logevent (&L, LEV_COPYEXEC_MAIN_TRANSACTION_ERROR, sizeof (*A) + l);
  A->transaction_id = cur_transaction_id;
  A->error_msg_size = l;
  memcpy (A->data, err_buff, l);
  flush_logevent_to_file (&L);
}

/*
static void alloc_transaction_err_logevent (int transaction_id, const char *format, ...) {
  char err_buff[ERR_BUFF_SIZE];
  va_list ap;
  va_start (ap, format);
  int l = vsnprintf (err_buff, ERR_BUFF_SIZE, format, ap);
  va_end (ap);
  if (l > ERR_BUFF_SIZE) {
    l = ERR_BUFF_SIZE;
  }
  vkprintf (3, "%.*s\n", l, err_buff);
  struct lev_copyexec_main_transaction_err *E = alloc_log_event (LEV_COPYEXEC_MAIN_TRANSACTION_ERROR, sizeof (*E) + l, cur_transaction_id);
  E->transaction_id = transaction_id;
  E->error_msg_size = l;
  memcpy (E->data, err_buff, l);
}
*/

static void sigusr1_handler (const int sig) {
  reopen_logs ();
}

volatile pid_t sigusr2_pid = 0;

static void sigusr2_handler (const int sig) {
  sigusr2_pid = getpid ();
}

void set_sigusr1_handler (void) {
  struct sigaction act;
  sigset_t signal_set;
  sigemptyset (&signal_set);
  act.sa_handler = sigusr1_handler;
  act.sa_mask = signal_set;
  act.sa_flags = 0;
  assert (!sigaction (SIGUSR1, &act, NULL));
}

void set_sigusr2_handler (void) {
  struct sigaction act;
  sigset_t signal_set;
  sigemptyset (&signal_set);
  act.sa_handler = sigusr2_handler;
  act.sa_mask = signal_set;
  act.sa_flags = 0;
  assert (!sigaction (SIGUSR2, &act, NULL));
}

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
}

static pid_t cmd_pid;
volatile int transaction_interrupting_signal = 0;
volatile pid_t sigterm_receiver_pid = 0;

void copyexec_main_sig_handler (const int sig) {
  /* since next operator isn't atomic for 32-bit version, */
  /* sigaction function was used for blocking other signals changing pending_signal variable (sa_mask field) */
  pending_signals |= 1LL << sig;
  if (sig == SIGTERM || sig == SIGINT) {
    sigterm_receiver_pid = getpid ();
  }
}

static void copyexec_transaction_process_sigint_handler (const int sig) {
  if (!transaction_interrupting_signal) {
    transaction_interrupting_signal = sig;
  }
  signal (sig, SIG_IGN);
}

static int do_exec_create_output_file (int cmd_id, char *suffix) {
  char filename[128];
  assert (snprintf (filename, sizeof (filename), ".c%d.%s.%s", cmd_id, cur_transaction_hash, suffix) < (int) sizeof (filename));
  int fd = open (filename, O_RDWR | O_CREAT | O_TRUNC | O_EXCL, 0000);
  if (fd < 0) {
    vkprintf (1, "create_output_file: creat (%s, O_RDWR | O_CREAT | O_TRUNC | O_EXCL, 0000) failed. %m\n", filename);
    return -1;
  }

  if (unlink (filename) < 0) {
    vkprintf (1, "create_output_file: unlink (%s) failed. %m\n", filename);
    fd_close (&fd);
    return -2;
  }

  if (fcntl (fd, F_SETFD, FD_CLOEXEC) < 0) {
    vkprintf (1, "create_output_file: fcntl (%d, F_SETFD, FD_CLOEXEC) failed. %m\n", fd);
    fd_close (&fd);
    return -3;
  }

  return fd;
}

static int do_exec (struct lev_copyexec_aux_transaction_cmd_exec *E, const int exec_check, const int update_transaction_result) {
  vkprintf (3, "do_exec (%.*s)\n", E->command_size, E->data);
  char *cmd = malloc (E->command_size + 1);
  if (cmd == NULL) {
    return -__LINE__;
  }
  memcpy (cmd, E->data, E->command_size);
  cmd[E->command_size] = 0;
  vkprintf (3, "cmd: %s\n", cmd);
  int fd_out = do_exec_create_output_file (cmd_id, "out");
  if (fd_out < 0) {
    free (cmd);
    return -__LINE__;
  }

  int fd_err = do_exec_create_output_file (cmd_id, "err");
  if (fd_err < 0) {
    free (cmd);
    fd_close (&fd_out);
    return -__LINE__;
  }

  cmd_pid = fork ();
  if (cmd_pid < 0) {
    vkprintf (1, "do_exec: fork failed. %m\n");
    free (cmd);
    fd_close (&fd_out);
    fd_close (&fd_err);
    return -__LINE__;
  }

  if (cmd_pid > 0) {
    free (cmd);
    //logevent (begin executing command)
    cmd_id++;
    struct buffered_logevent L;
    struct lev_copyexec_main_command_begin *A = alloc_buffered_logevent (&L, LEV_COPYEXEC_MAIN_COMMAND_BEGIN, sizeof (*A) + E->command_size);
    A->transaction_id = cur_transaction_id;
    A->command_id = cmd_id;
    A->pid = cmd_pid;
    A->command_size = E->command_size;
    memcpy (A->data, E->data, E->command_size);
    flush_logevent_to_file (&L);
    //wait
    pid_t pid;
    int status;
    while (1) {
      if (transaction_interrupting_signal) {
        //man kill: if pid equals 0, then sig is sent to every process in the process group of the calling process.
        kill (0, transaction_interrupting_signal);
      }

      pid = waitpid (cmd_pid, &status, 0);
      if (pid < 0 && errno == EINTR) {
        continue;
      }

      if (pid != cmd_pid) {
        kprintf ("waitpid returns %d, but cmd_pid = %d. %m\n", pid, cmd_pid);
        assert (pid == cmd_pid);
      }

      //cur_transaction_last_cmd_status = status;
      if (update_transaction_result) {
        const unsigned x = status & 0xffff, exit_code = x >> 8, signal_code = x & 0xff;
        cur_transaction_result = (signal_code << 8) | exit_code;
      }
      break;
    }
    //logevent (end executing command)
    parted_output_t O, E;
    int r = read_parted_stdout (fd_out, &O);
    if (r < 0) {
      vkprintf (1, "transaction_id: %d, cmd_id: %d, return_parted_stdout (stdout) failed at the line: %d\n", cur_transaction_id, cmd_id, -r);
    }
    r = read_parted_stdout (fd_err, &E);
    if (r < 0) {
      vkprintf (1, "transaction_id: %d, cmd_id: %d, return_parted_stdout (stderr) failed at the line: %d\n", cur_transaction_id, cmd_id, -r);
    }
    struct lev_copyexec_main_command_end *B = alloc_buffered_logevent (&L, LEV_COPYEXEC_MAIN_COMMAND_END, sizeof (*B) + O.cutted_size + E.cutted_size);
    B->transaction_id = cur_transaction_id;
    B->command_id = cmd_id;
    B->pid = cmd_pid;
    cmd_pid = 0;
    B->status = status;
    B->stdout_size = O.size;
    B->stderr_size = E.size;
    B->saved_stdout_size = O.cutted_size;
    B->saved_stderr_size = E.cutted_size;
    void *b = B->data;
    if (O.buf) {
      memcpy (b, O.buf, O.cutted_size);
      b += O.cutted_size;
      free (O.buf);
    }
    if (E.buf) {
      memcpy (b, E.buf, E.cutted_size);
      free (E.buf);
    }
    flush_logevent_to_file (&L);
    if (!WIFEXITED(status) || WEXITSTATUS(status)) {
      return exec_check ? -__LINE__ : 0;
    }
  } else {

    fd_close (&fd_transaction_logevents);

    while (1) {
      if (dup2 (fd_out, 1) < 0 && errno == EINTR) {
        continue;
      }
      break;
    }
    while (1) {
      if (dup2 (fd_err, 2) < 0 && errno == EINTR) {
        continue;
      }
      break;
    }
    signal (SIGINT, SIG_DFL);
    signal (SIGTERM, SIG_DFL);
    signal (SIGUSR1, SIG_DFL);
    signal (SIGHUP, SIG_DFL);
    signal (SIGCHLD, SIG_DFL);
    execl ("/bin/sh", "/bin/sh", "-c", cmd, NULL);
    exit (254);
  }
  return 0;
}

static char unpack_filename_buff[PATH_MAX];

static int unpack_file (struct lev_copyexec_aux_transaction_cmd_file *E) {
  unpack_filename_buff[0] = 0;
  if (E->filename_size < 0 || E->filename_size >= PATH_MAX) {
    return -__LINE__;
  }
  memcpy (unpack_filename_buff, E->data, E->filename_size);
  unpack_filename_buff[E->filename_size] = 0;
  if (strchr (unpack_filename_buff, '/') != NULL) {
    return -__LINE__;
  }
  void *d = malloc (E->size);
  if (d == NULL) {
    return -__LINE__;
  }

  uLongf destLen = E->size;
  if (Z_OK != uncompress (d, &destLen, (unsigned char *) E->data + E->filename_size, E->compressed_size)) {
    free (d);
    return -__LINE__;
  }

  if (destLen != E->size || compute_crc32 (d, E->size) != E->crc32) {
    free (d);
    return -__LINE__;
  }

  int fd = open (unpack_filename_buff, O_CREAT | O_TRUNC | O_WRONLY | O_EXCL, E->mode);
  if (fd < 0) {
    return -__LINE__;
  }

  if (write (fd, d, E->size) != E->size) {
    free (d);
    fd_close (&fd);
    return -__LINE__;
  }
  free (d);
  fd_close (&fd);
  //copy file attributes
  struct stat st;
  st.st_mode = E->mode;
  st.st_uid = E->uid;
  st.st_gid = E->gid;
  st.st_atime = E->actime;
  st.st_mtime = E->modtime;
  if (lcopy_attrs (unpack_filename_buff, &st) < 0) {
    return -__LINE__;
  }
  return 0;
}

static int do_file (struct lev_copyexec_aux_transaction_cmd_file *E) {
  int r = unpack_file (E);
  if (r < 0) {
    write_transaction_err_logevent ("unpack_file (%s) failed at the line %d", unpack_filename_buff, -r);
    cur_transaction_result = 0x00ff;
    //cur_transaction_last_cmd_status = 0xff00; /* terminated with exit code = 255 */
  }
  return r;
}

static inline int interrupted_by_cancelled (void) {
  return !cmd_id && sigusr2_pid && sigusr2_pid == getpid ();
}

static int transaction_wait (transaction_t *T) {
  while (check_is_running (T) && !transaction_interrupting_signal) {
    if (interrupted_by_cancelled ()) {
      return 0;
    }
    struct timespec t;
    t.tv_sec  = 0;
    t.tv_nsec = 50000000; /* sleep 0.05 secs */
    nanosleep (&t, NULL);
  }
  return 0;
}

static int transaction_cancel (transaction_t *T) {
  if (!check_is_running (T)) {
    vkprintf (3, "transaction_cancel: tr%d has terminated.\n", T->transaction_id);
    return 0;
  }
  int r = kill (T->pid, SIGUSR2);
  if (r < 0) {
    vkprintf (2, "transaction_cancel: kill (%d) failed. %m\n", T->pid);
  }
  return r;
}

static int do_result (int result) {
  cur_transaction_result = result;
  return 0;
}

static int do_wait (int transaction_id) {
  transaction_t *T = get_transaction_f (transaction_id, 0);
  if (T == NULL) {
    return 0;
  }
  return transaction_wait (T);
}

static int do_cancel (int transaction_id) {
  transaction_t *T = get_transaction_f (transaction_id, 0);
  if (T == NULL) {
    return 0;
  }
  return transaction_cancel (T);
}

static int do_kill (int signal, int transaction_id) {
  transaction_t *T = get_transaction_f (transaction_id, 0);
  if (T == NULL) {
    return 0;
  }

  if (!check_is_running (T)) {
    return 0;
  }

  if (kill (T->pid, signal) < 0) {
    return 0;
  }

  return transaction_wait (T);
}

int transaction_replay_logevent (struct lev_generic *E, int size) {
  int s;
  vkprintf (3, "transaction_replay_logevent (E->type = 0x%x, size = %d)\n", E->type, size);

  switch (E->type) {
    case LEV_COPYEXEC_AUX_TRANSACTION_CMD_FILE:
      s = sizeof (struct lev_copyexec_aux_transaction_cmd_file);
      if (size < s) {
        return -2;
      }
      s += ((struct lev_copyexec_aux_transaction_cmd_file *) E)->filename_size + ((struct lev_copyexec_aux_transaction_cmd_file *) E)->compressed_size;
      if (size < s) {
        return -2;
      }
      if (do_file ((struct lev_copyexec_aux_transaction_cmd_file *) E) < 0) {
        return -4;
      }
      return s;
    case LEV_COPYEXEC_AUX_TRANSACTION_CMD_EXEC:
      s = sizeof (struct lev_copyexec_aux_transaction_cmd_exec);
      if (size < s) {
        return -2;
      }
      s += ((struct lev_copyexec_aux_transaction_cmd_exec *) E)->command_size;
      if (size < s) {
        return -2;
      }
      if (do_exec (((struct lev_copyexec_aux_transaction_cmd_exec *) E), 0, 0) < 0) {
        return -5;
      }
      return s;
    case LEV_COPYEXEC_AUX_TRANSACTION_CMD_EXEC_CHECK:
      s = sizeof (struct lev_copyexec_aux_transaction_cmd_exec);
      if (size < s) {
        return -2;
      }
      s += ((struct lev_copyexec_aux_transaction_cmd_exec *) E)->command_size;
      if (size < s) {
        return -2;
      }
      if (do_exec (((struct lev_copyexec_aux_transaction_cmd_exec *) E), 1, 0) < 0) {
        return -6;
      }
      return s;
    case LEV_COPYEXEC_AUX_TRANSACTION_CMD_EXEC_RESULT:
      s = sizeof (struct lev_copyexec_aux_transaction_cmd_exec);
      if (size < s) {
        return -2;
      }
      s += ((struct lev_copyexec_aux_transaction_cmd_exec *) E)->command_size;
      if (size < s) {
        return -2;
      }
      if (do_exec (((struct lev_copyexec_aux_transaction_cmd_exec *) E), 0, 1) < 0) {
        return -7;
      }
      return s;
    case LEV_COPYEXEC_AUX_TRANSACTION_CMD_WAIT:
      s = sizeof (struct lev_copyexec_aux_transaction_cmd_wait);
      if (size < s) {
        return -2;
      }
      do_wait (((struct lev_copyexec_aux_transaction_cmd_wait *) E)->transaction_id);
      return s;
    case LEV_COPYEXEC_AUX_TRANSACTION_CMD_CANCEL:
      s = sizeof (struct lev_copyexec_aux_transaction_cmd_wait);
      if (size < s) {
        return -2;
      }
      do_cancel (((struct lev_copyexec_aux_transaction_cmd_wait *) E)->transaction_id);
      return s;
    case LEV_COPYEXEC_AUX_TRANSACTION_CMD_KILL:
      s = sizeof (struct lev_copyexec_aux_transaction_cmd_kill);
      if (size < s) {
        return -2;
      }
      do_kill (((struct lev_copyexec_aux_transaction_cmd_kill *) E)->signal, ((struct lev_copyexec_aux_transaction_cmd_kill *) E)->transaction_id);
      return s;
    case LEV_COPYEXEC_AUX_TRANSACTION_CMD_RESULT:
      s = sizeof (struct lev_copyexec_aux_transaction_cmd_result);
      if (size < s) {
        return -2;
      }
      do_result (((struct lev_copyexec_aux_transaction_cmd_result *) E)->result);
      return s;
  }
  return -3;
}

static int synchronization_point_transaction (void *b, int blen) {
  const int tp = LEV_COPYEXEC_AUX_TRANSACTION_SYNCHRONIZE;
  return (blen == 24 + sizeof (struct lev_copyexec_aux_transaction_header) && !memcmp (b + sizeof (struct lev_copyexec_aux_transaction_header), &tp, 4));
}

static int last_synchronization_point_id;
static long long last_synchronization_point_pos, last_decryption_failed_transaction_pos;

void replay_synchronization_point (replay_transaction_info_t *T) {
  unsigned char *b;
  int blen;
  int r = decrypt_transaction (T->input, T->ilen, T->key_id, T->binlog_pos, T->transaction_id, &b, &blen);
  if (r < 0) {
    last_decryption_failed_transaction_pos = T->binlog_pos;
    return;
  }
  if (synchronization_point_transaction (b, blen)) {
    struct lev_copyexec_aux_transaction_header *B = (struct lev_copyexec_aux_transaction_header *) b;
    if (!(B->mask & instance_mask)) {
      vkprintf (3, "Skip synchronization point. (transaction_id: %d, mask: 0x%x, instance_mask: 0x%x)\n", T->transaction_id, B->mask, instance_mask);
      free (b);
      return;
    }
    assert (last_synchronization_point_id < T->transaction_id);
    last_synchronization_point_id = T->transaction_id;
    last_synchronization_point_pos = T->binlog_pos;
  }
  free (b);
}

int find_last_synchronization_point (void) {
  last_synchronization_point_id = 0;
  last_decryption_failed_transaction_pos = last_synchronization_point_pos = 0;
  int r = copyexec_aux_replay_binlog (0, replay_synchronization_point);
  if (r < 0) {
    vkprintf (1, "find_last_synchronization_point: copyexec_aux_replay_binlog returns %d.\n", r);
  }
  if (last_decryption_failed_transaction_pos > last_synchronization_point_pos) {
    kprintf ("Couldn't decrypt transaction at %lld pos, last synchronization point pos is %lld.\n", last_decryption_failed_transaction_pos, last_synchronization_point_pos);
    exit (1);
  }
  return last_synchronization_point_id;
}

static void alloc_io_failed_logevent (transaction_t *T, int result) {
  if (result < 0) {
    result *= -1;
  }
  transaction_change_status (T, ts_io_failed);
  struct lev_copyexec_main_transaction_status *E = transaction_alloc_log_event_status (T);
  E->binlog_pos = T->binlog_pos;
  E->mask = T->mask;
  E->result = T->result = result;
}

static int fd_try_close (int fd) {
  while (1) {
    if (close (fd) < 0) {
      if (errno == EINTR) {
        continue;
      }
      return 0;
    }
    return 1;
  }
}

static void close_unused_fd (int fd) {
  int i, r = 0;
  for (i = 3; i <= max_connection; i++) {
    if (i == fd) {
      continue;
    }
    r += fd_try_close (i);
  }

  for ( ; i < fd; i++) {
    if (fd_try_close (i) > 0) {
      vkprintf (1, "close_unused_fd: found resourse leakage (fd:%d).\n", i);
      r++;
    }
  }

  vkprintf (3, "close_unused_fd: close %d fd(s)\n", r);
}

void exec_transaction (replay_transaction_info_t *I) {
  if (I->transaction_id < first_transaction_id) {
    vkprintf (3, "skip tr%d (first_transaction_id = %d)\n", I->transaction_id, first_transaction_id);
    return;
  }

  vkprintf (3, "exec_transaction (id = %d)\n", I->transaction_id);
  // check status of current transaction
  transaction_t *T = get_transaction_f (I->transaction_id, 0);
  if (T != NULL) {
    if (!transaction_want_exec (T)) {
      vkprintf (3, "skip tr%d (status = %d)\n", I->transaction_id, T->status);
      return;
    }
  }

  if (T == NULL) {
    T = get_transaction_f (I->transaction_id, 1);
    if (T == NULL) {
      copyexec_abort ();
    }
    T->status = ts_unset;
  }
  T->binlog_pos = I->binlog_pos;

  // try decrypt transaction
  unsigned char *b;
  int blen;
  int r = decrypt_transaction (I->input, I->ilen, I->key_id, I->binlog_pos, I->transaction_id, &b, &blen);
  if (r < 0) {
    kprintf ("decrypt_transaction (key_id: %d, binlog_pos: %lld, transaction_id: %d) returns %s.\n", I->key_id, I->binlog_pos, I->transaction_id, copyexec_strerror (r));
    handle_failure ();

    transaction_change_status (T, ts_decryption_failed);
    struct lev_copyexec_main_transaction_status *E = transaction_alloc_log_event_status (T);
    E->binlog_pos = T->binlog_pos;
    E->result = T->result = -r;
    return;
  }

  vkprintf (3, "transaction was successfully decrypted.\n");

  // check transaction mask
  struct lev_copyexec_aux_transaction_header *B = (struct lev_copyexec_aux_transaction_header *) b;
  T->mask = B->mask;
  if (!(T->mask & instance_mask) || synchronization_point_transaction (b, blen)) {
    transaction_change_status (T, ts_ignored);
    struct lev_copyexec_main_transaction_status *E = transaction_alloc_log_event_status (T);
    E->binlog_pos = T->binlog_pos;
    E->mask = T->mask;
    free (b);
    return;
  }

  // mkdir transaction_dir
  get_transaction_dir (T, transaction_dir);
  vkprintf (3, "transaction_dir = %s\n", transaction_dir);

  if (T->status != ts_unset) {
    vkprintf (3, "exec_transaction: T->status = %d (T->status != ts_unset).\n", (int) T->status);
    delete_transaction_dir ();
  }

  if (mkdir (transaction_dir, 0700) < 0) {
    kprintf ("mkdir %s failed. %m\n", transaction_dir);
    handle_failure ();
    alloc_io_failed_logevent (T, COPYEXEC_ERR_MKDIR);
    free (b);
    return;
  }

  // tmp_dir isn't relative, since realpath function was used in copyexec-engine.c
  // chdir transaction_dir
  if (chdir (transaction_dir) < 0) {
    kprintf ("chdir %s failed. %m\n", transaction_dir);
    handle_failure ();
    alloc_io_failed_logevent (T, COPYEXEC_ERR_CHDIR);
    delete_transaction_dir ();
    free (b);
    return;
  }

  get_transaction_binlog_name (T, cur_transaction_binlog_name);
  vkprintf (3, "[tr%d] binlog_name: %s\n", T->transaction_id, cur_transaction_binlog_name);

  while (1) {
    fd_transaction_logevents = open (cur_transaction_binlog_name, O_WRONLY | O_CREAT | O_TRUNC | O_EXCL, 0400);//TODO: check 0400 mode
    if (fd_transaction_logevents < 0 && errno == EINTR) {
      continue;
    }
    break;
  }

  if (fd_transaction_logevents < 0) {
    kprintf ("creat (%s) failed. %m\n", cur_transaction_binlog_name);
    handle_failure ();
    alloc_io_failed_logevent (T, COPYEXEC_ERR_CREAT);
    delete_transaction_dir ();
    free (b);
    return ;
  }

  struct stat st;
  if (fstat (fd_transaction_logevents, &st) < 0) {
    kprintf ("fstat (%s) failed. %m\n", cur_transaction_binlog_name);
    handle_failure ();
    alloc_io_failed_logevent (T, COPYEXEC_ERR_FSTAT);
    delete_transaction_dir ();
    free (b);
    return;
  }

  sigset_t signal_set;
  sigemptyset (&signal_set);
  sigaddset (&signal_set, SIGINT);
  sigaddset (&signal_set, SIGTERM);
  struct sigaction act;
  act.sa_handler = copyexec_transaction_process_sigint_handler;
  act.sa_mask = signal_set;
  act.sa_flags = 0;

  // fork
  pid_t p = fork ();
  if (p < 0) {
    kprintf ("fork () failed. %m\n");
    handle_failure();
    alloc_io_failed_logevent (T, COPYEXEC_ERR_FORK);
    delete_transaction_dir ();
    free (b);
    return;
  }

  if (p > 0) {
    T->creation_time = get_process_creation_time (p);
    free (b);
    fd_close (&fd_transaction_logevents);
    T->pid = p;
    transaction_change_status (T, ts_running);
    tlist_insert (&child_running_list, T);
    struct lev_copyexec_main_transaction_status *A = transaction_alloc_log_event_status (T);
    A->binlog_pos = T->binlog_pos;
    A->mask = T->mask;
    A->pid = p;
    A->creation_time = T->creation_time;
    A->st_dev = T->st_dev = (long long) st.st_dev;
    A->st_ino = T->st_ino = (long long) st.st_ino;
    return;
  }

  assert (!sigaction (SIGINT, &act, NULL));
  assert (!sigaction (SIGTERM, &act, NULL));

  set_sigusr1_handler ();

  //did any SIGINT or SIGTERM signal sent before sigaction call?
  if (sigterm_receiver_pid == getpid () && !transaction_interrupting_signal) {
    transaction_interrupting_signal = SIGTERM;
  }

  setsid ();

  /* close unused in transaction process files */
  close_binlog (Binlog, 1);
  fd_close (&fd_aux_binlog);
  fd_close (&epoll_fd);
  if (sfd) {
    fd_close (&sfd);
  }
  close_unused_fd (fd_transaction_logevents);

  T->pid = getpid ();
  get_transaction_hash (T, cur_transaction_hash);
  cmd_id = 0;
  cur_transaction_id = I->transaction_id;
  transaction_change_process_name (cur_transaction_id);

  if (T->mask & MASK_WAITING_TRANSACTION) {
    transaction_t *x;
    //wait auto transactions
    for (x = auto_running_list.next; x != &auto_running_list; x = x->next) {
      if (x->transaction_id < cur_transaction_id) {
        do_wait (x->transaction_id);
      }
    }
    //wait child transactions
    for (x = child_running_list.next; x != &child_running_list; x = x->next) {
      if (x->transaction_id < cur_transaction_id) { /* this check is needed for the case rerunable transaction */
        do_wait (x->transaction_id);
      }
    }
  }


  void *bend = b + blen - 20, *E = b + sizeof (struct lev_copyexec_aux_transaction_header);
  int res = 0;

  // main loop (replay_log)
  while (E < bend && !transaction_interrupting_signal) {
    if (interrupted_by_cancelled ()) {
      transaction_interrupting_signal = SIGUSR2;
      break;
    }
    int sz = transaction_replay_logevent (E, bend - E);
    if (sz < 0) {
      res = sz;
      break;
    }
    sz = (sz + 3) & -4;
    E += sz;
  }

  free (b);
  if (res < 0) {
    vkprintf (1, "transaction: %d, res = %d\n", cur_transaction_id, res);
  }

  lev_type_t tp = LEV_COPYEXEC_MAIN_TRANSACTION_STATUS;
  if (transaction_interrupting_signal) {
    if (cmd_id) {
      tp += ts_interrupted;
    } else {
      tp += ts_cancelled;
      cur_transaction_result = transaction_interrupting_signal << 8;
    }
  } else {
    tp += (res < 0) ? ts_failed : ts_terminated;
  }

  struct buffered_logevent L;
  struct lev_copyexec_main_transaction_status *A = alloc_buffered_logevent (&L, tp, sizeof (*A));
  A->transaction_id = T->transaction_id;
  A->binlog_pos = T->binlog_pos;
  A->mask = T->mask;
  A->pid = getpid ();
  A->creation_time = get_process_creation_time (A->pid);
  A->result = T->result = cur_transaction_result;
  A->st_dev = (long long) st.st_dev;
  A->st_ino = (long long) st.st_ino;

  flush_logevent_to_file (&L);

  fsync (fd_transaction_logevents);
  fd_close (&fd_transaction_logevents);

  exit (0);
}
/****************************************** main binlog **************************************************************/

int copyexec_main_le_start (struct lev_start *E) {
  vkprintf (3, "copyexec_main_le_start (schema: 0x%x\n", E->schema_id);
  if (E->schema_id != COPYEXEC_MAIN_SCHEMA_V1) {
    return -1;
  }
  log_split_min = E->split_min;
  log_split_max = E->split_max;
  log_split_mod = E->split_mod;
  assert (log_split_mod > 0 && log_split_min >= 0 && log_split_min + 1 ==  log_split_max && log_split_max <= log_split_mod);
  assert (E->extra_bytes == 20);
  memcpy (&main_volume_id, E->str, 8);
  vkprintf (3, "main_volume_id: %llu\n", main_volume_id);

  if (aux_volume_id && main_volume_id != aux_volume_id) {
    kprintf ("main_volume_id isn't equal to aux_volume_id");
    exit (1);
  }

  memcpy (&random_tag, E->str + 8, 8);
  vkprintf (3, "random_tag: %llu\n", random_tag);

  int i;
  memcpy (&i, E->str + 16, 4);

  if (i != instance_mask) {
    kprintf ("given instance-mask isn't equal to main binlog instance-mask.\n");
    exit (1);
  }

  return 0;
}

/****************************************** replay aux binlog **************************************************************/
int transactions = 0;
unsigned long long main_volume_id, aux_volume_id, random_tag;

static int copyexec_aux_le_start (struct lev_start *E) {
  if (E->schema_id != COPYEXEC_AUX_SCHEMA_V1) {
    return -1;
  }
  log_split_min = E->split_min;
  log_split_max = E->split_max;
  log_split_mod = E->split_mod;
  assert (E->extra_bytes == 8);
  assert (log_split_mod > 0 && log_split_min >= 0 && log_split_min + 1 ==  log_split_max && log_split_max <= log_split_mod);

  memcpy (&aux_volume_id, E->str, 8);
  if (main_volume_id && main_volume_id != aux_volume_id) {
    kprintf ("aux_volume_id isn't equal to main_volume_id");
    exit (1);
  }

  return 0;
}

int copyexec_aux_binlog_readonly_open (const char *const aux_binlog_name) {
  if (aux_binlog_name == NULL || (fd_aux_binlog = open (aux_binlog_name, O_RDONLY)) < 0) {
    kprintf ("Couldn't open aux binlog \"%s\" in read only mode. %m\n", aux_binlog_name);
    return -1;
  }

  return 0;
}

void copyexec_aux_binlog_seek (void) {
  get_aux_binlog_jump_log_pos (&aux_log_read_start);
  vkprintf (3, "after get_aux_binlog_jump_log_pos: aux_log_readto_start = %lld, transactions = %d\n", aux_log_read_start, transactions);
  aux_log_readto_pos = aux_log_read_start;
}

static int fd_read (int fd, void *a, long long size) {
  long long bytes_read;
  do {
    bytes_read = read (fd, a, size);
  } while (bytes_read < 0 && errno == EINTR);
  if (bytes_read < 0) {
    kprintf ("copyexec_aux_replay_binlog: read returns %lld instead of %lld. %m\n", bytes_read, size);
    copyexec_abort ();
  }
  if (bytes_read != size) {
    kprintf ("copyexec_aux_replay_binlog: read returns %lld instead of %lld. %m\n", bytes_read, size);
    return -1;
  }
  return 0;
}

int copyexec_aux_replay_binlog (long long start_pos, void (*fp_replay_transaction)(replay_transaction_info_t *)) {
  struct lev_copyexec_aux_transaction L;
  struct stat st;
  if (fstat (fd_aux_binlog, &st) < 0) {
    kprintf ("copyexec_aux_replay_binlog: fstat failed. %m\n");
    copyexec_abort ();
  }
  long long end_pos = st.st_size;

  if (end_pos < 32) {
    kprintf ("copyexec_aux_replay_binlog: aux binlog too short. Skip reading aux binlog.\n");
    aux_log_readto_pos = 0;
    return -__LINE__;
  }

  if (!aux_volume_id) {
    errno = 0;
    if (lseek (fd_aux_binlog, 0, SEEK_SET) != 0) {
      kprintf ("copyexec_aux_replay_binlog: lseek failed. %m\n");
      copyexec_abort ();
    }
    struct lev_start *E = malloc (32);
    assert (E);
    if (fd_read (fd_aux_binlog, E, 32) < 0) {
      copyexec_abort ();
    }
    if (E->type != LEV_START) {
      kprintf ("copyexec_aux_replay_binlog: LEV_START expected.\n");
      copyexec_abort ();
    }
    if (copyexec_aux_le_start (E) < 0) {
      kprintf ("copyexec_aux_replay_binlog: copyexec_aux_le_start failed.\n");
      copyexec_abort ();
    }
    free (E);
    assert (aux_volume_id);
  }

  if (!start_pos) {
    start_pos = 32;
  }

  if (start_pos >= end_pos) {
    vkprintf (3, "copyexec_aux_replay_binlog: no updates.\n");
    aux_log_readto_pos = start_pos;
    return 0;
  }

  errno = 0;
  if (lseek (fd_aux_binlog, start_pos, SEEK_SET) != start_pos) {
    kprintf ("copyexec_aux_replay_binlog: lseek failed. %m\n");
    copyexec_abort ();
  }
  int res = 0;
  while (start_pos < end_pos && !interrupted_by_signal ()) {
    if (fd_read (fd_aux_binlog, &L, sizeof (L)) < 0) {
      res = -__LINE__;
      break;
    }
    vkprintf (4, "offset: %lld, L.type = 0x%x\n",  start_pos, L.type);
    if (L.type != LEV_COPYEXEC_AUX_TRANSACTION) {
      kprintf ("copyexec_aux_replay_binlog: LEV_COPYEXEC_AUX_TRANSACTION expected.\n");
      copyexec_abort ();
    }
    if (L.crc32 != compute_crc32 (&L, offsetof (struct lev_copyexec_aux_transaction, crc32))) {
      kprintf ("copyexec_aux_replay_binlog: crc32 failed.\n");
      copyexec_abort ();
    }
    unsigned s = (L.size + 3) & -4;
    if (fp_replay_transaction != NULL) {
      void *a = malloc (s);
      if (a == NULL) {
        kprintf ("copyexec_aux_replay_binlog: malloc (%u) returns NULL.\n", s);
        copyexec_abort ();
      }
      if (fd_read (fd_aux_binlog, a, s) < 0) {
        free (a);
        res = -__LINE__;
        break;
      }
      if (L.crypted_body_crc64 != crc64 (a, s)) {
        kprintf ("copyexec_aux_replay_binlog: crc64 failed.\n");
        copyexec_abort ();
      }
      replay_transaction_info_t I;
      I.input = a;
      I.ilen  = L.size;
      I.key_id = L.key_id;
      I.binlog_pos = start_pos;
      I.transaction_id = transactions + 1;
      fp_replay_transaction (&I);
      free (a);
    } else {
      errno = 0;
      if (lseek (fd_aux_binlog, s, SEEK_CUR) != (off_t) ((start_pos + sizeof (L)) + s)) {
        vkprintf (1, "copyexec_aux_replay_binlog: lseek failed. %m\n");
        res = -__LINE__;
        break;
      }
    }
    start_pos += sizeof (L) + s;
    transactions++;
  }
  aux_log_readto_pos = start_pos;
  return res;
}

/***************************** aux snapshot ***********************************************************/
#define COPYEXEC_COMMIT_INDEX_MAGIC 0x345d328c

#pragma	pack(push,4)
typedef struct {
/* strange numbers */
  int magic;
  int created_at;
  long long log_pos0;
  long long log_pos1;
  unsigned long long file_hash;
  int log_timestamp;
  unsigned int log_pos0_crc32;
  unsigned int log_pos1_crc32;

  unsigned long long volume_id;
  int transactions;

  unsigned header_crc32;
} aux_index_header;

#pragma	pack(pop)


int aux_load_index (kfs_file_handle_t Index) {
  aux_index_header header;
  if (Index == NULL) {
    transactions = 0;
    return 0;
  }
  int idx_fd = Index->fd;
  if (read (idx_fd, &header, sizeof (aux_index_header)) != sizeof (aux_index_header)) {
    kprintf ("read failed. %m\n");
    return -2;
  }

  if (header.magic != COPYEXEC_COMMIT_INDEX_MAGIC) {
    kprintf ("index file is not for copyexec-commit\n");
    return -1;
  }

  if (header.header_crc32 != compute_crc32 (&header, offsetof (aux_index_header, header_crc32))) {
    kprintf ("Snapshot header, CRC32 failed.\n");
    return -3;
  }

  transactions = header.transactions;
  aux_volume_id = header.volume_id;
  aux_log_readto_pos = header.log_pos1;

  vkprintf (3, "transactions = %d\n", transactions);

  return 0;
}

int aux_save_index (int writing_binlog) {
  char *newidxname = NULL;

  if (engine_snapshot_replica) {
    newidxname = get_new_snapshot_name (engine_snapshot_replica, aux_log_readto_pos, engine_snapshot_replica->replica_prefix);
  }

  if (!newidxname || newidxname[0] == '-') {
    kprintf ("cannot write index: cannot compute its name\n");
    exit (1);
  }

  int newidx_fd = open (newidxname, O_CREAT | O_TRUNC | O_WRONLY | O_EXCL, 0660);

  if (newidx_fd < 0) {
    kprintf ("cannot create new index file %s: %m\n", newidxname);
    exit (1);
  }

  aux_index_header header;
  memset (&header, 0, sizeof (header));
  header.magic = COPYEXEC_COMMIT_INDEX_MAGIC;
  header.created_at = time (NULL);
  header.log_pos1 = aux_log_readto_pos;
  header.transactions = transactions;
  header.volume_id = aux_volume_id;
  header.header_crc32 = compute_crc32 (&header, offsetof (aux_index_header, header_crc32));
  assert (write (newidx_fd, &header, sizeof (header)) == sizeof (header));
  assert (!fsync (newidx_fd));
  assert (!close (newidx_fd));

  vkprintf (3, "index written ok\n");

  if (rename_temporary_snapshot (newidxname)) {
    kprintf ("cannot rename new index file from %s: %m\n", newidxname);
    unlink (newidxname);
    exit (1);
  }

  return 0;
}

/**************************************** empty binlog *******************************************************/
void make_empty_binlog (const char *binlog_name, const char *const volume_name, int schema_id, const void *const lev_start_extra, int lev_start_extra_len, const void *const extra_logevents, int extra_len) {
  if (lev_start_extra == NULL) {
    lev_start_extra_len = 0;
  }
  char a[PATH_MAX];
  assert (snprintf (a, PATH_MAX, "%s.bin", binlog_name) < PATH_MAX);
  int sz = 32 + lev_start_extra_len;
  struct lev_start *E = malloc (sz);
  memset (E, 0, sz);
  E->type = LEV_START;
  E->schema_id = schema_id;
  E->extra_bytes = 8 + lev_start_extra_len;
  E->split_mod = 1;
  E->split_min = 0;
  E->split_max = 1;
  int l = strlen (volume_name);
  if (l > 8) {
    l = 8;
  }
  if (l > 0) {
    memcpy (E->str, volume_name, l);
  }

  unsigned long long id;
  memcpy (&id, E->str, 8);
  assert (id != 0);

  if (aux_volume_id && id != aux_volume_id) {
    kprintf ("volume_id != aux_volume_id\n");
    exit (1);
  }
  if (main_volume_id && id != main_volume_id) {
    kprintf ("volume_id != main_volume_id\n");
    exit (1);
  }

  if (lev_start_extra) {
    memcpy (E->str + 8, lev_start_extra, lev_start_extra_len);
  }

  int fd = open (a, O_CREAT | O_WRONLY | O_EXCL, 0660);
  if (fd < 0) {
    kprintf ("open (%s, O_CREAT | O_WRONLY | O_EXCL, 0660) failed. %m\n", a);
    assert (fd >= 0);
  }
  assert (write (fd, E, sz) == sz);
  if (extra_logevents) {
    assert (write (fd, extra_logevents, extra_len));
  }
  assert (fsync (fd) >= 0);
  assert (close (fd) >= 0);
}
