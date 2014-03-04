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

    Copyright 2012-2013 Vkontakte Ltd
              2012-2013 Arseny Smirnov
              2012-2013 Aliaksei Levin
*/

#define _FILE_OFFSET_BITS 64

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <semaphore.h>
#include <pthread.h>
#include <errno.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <sys/prctl.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <sys/signalfd.h>
#include <poll.h>
#include <string>
#include <algorithm>

#include "php-master.h"

extern "C" {
#include "kdb-data-common.h"
#include "server-functions.h"
#include "net-events.h"
#include "net-connections.h"
#include "net-memcache-server.h"
#include "net-rpc-client.h"
#include "net-rpc-server.h"
#include "net-rpc-common.h"

#include "vv-tl-parse.h"
#include "vv-tl-aio.h"

#include "php-engine-vars.h"
#include "dl-utils-lite.h"

extern const char *engine_tag;
}

//do not kill more then MAX_KILL at the same time
#define MAX_KILL 5

//ATTENTION: the file has .cpp extention due to usage of "pthread_mutexattr_setrobust_np" which is by some strange reason unsupported for .c
//ATTENTION: do NOT change this structures without changing the magic
#define SHARED_DATA_MAGIC 0x3b720002
#define PHP_MASTER_VERSION "0.1"

static int save_verbosity;

typedef struct {
  int valid_flag;

  pid_t pid;
  unsigned long long start_time;

  long long rate;
  long long generation;

  int running_workers_n;
  int dying_workers_n;

  int to_kill;
  long long to_kill_generation;

  int own_http_fd;
  int http_fd_port;
  int ask_http_fd_generation;
  int sent_http_fd_generation;

  int reserved[50];
} master_data_t;

typedef struct {
  int magic;
  int is_inited;
  int init_owner;

  pthread_mutex_t main_mutex;
  pthread_cond_t main_cond;

  int id;
  master_data_t masters[2];
} shared_data_t;


static std::string socket_name, shmem_name;
static int cpu_cnt;

static sigset_t mask;
static sigset_t orig_mask;
static sigset_t empty_mask;

static shared_data_t *shared_data;
static master_data_t *me, *other;

static double my_now;


/*** Shared data functions ***/
void mem_info_add (mem_info_t *dst, const mem_info_t &other) {
  dst->vm_peak += other.vm_peak;
  dst->vm += other.vm;
  dst->rss_peak += other.rss_peak;
  dst->rss += other.rss;
}


/*** Stats ***/
long tot_workers_started;
long tot_workers_dead;
long tot_workers_strange_dead;
long workers_killed;
long workers_hung;
long workers_terminated;
long workers_failed;
void acc_stats_update (acc_stats_t *to, const acc_stats_t &from) {
  to->tot_queries += from.tot_queries;
  to->worked_time += from.worked_time;
  to->net_time += from.net_time;
  to->script_time += from.script_time;
  to->tot_script_queries += from.tot_script_queries;
  to->tot_idle_time += from.tot_idle_time;
  to->tot_idle_percent += from.tot_idle_percent;
  to->a_idle_percent += from.a_idle_percent;
  to->cnt++;
}

std::string acc_stats_to_str (std::string pid_s, const acc_stats_t &acc) {
  char buf[1000];
  std::string res;

  int cnt = acc.cnt;
  if (cnt == 0) {
    cnt = 1;
  }

  sprintf (buf, "tot_queries%s\t%ld\n", pid_s.c_str(), acc.tot_queries);
  res += buf;
  sprintf (buf, "worked_time%s\t%.3lf\n", pid_s.c_str(), acc.worked_time);
  res += buf;
  sprintf (buf, "net_time%s\t%.3lf\n", pid_s.c_str(), acc.net_time);
  res += buf;
  sprintf (buf, "script_time%s\t%.3lf\n", pid_s.c_str(), acc.script_time);
  res += buf;
  sprintf (buf, "tot_script_queries%s\t%ld\n", pid_s.c_str(), acc.tot_script_queries);
  res += buf;
  sprintf (buf, "tot_idle_time%s\t%.3lf\n", pid_s.c_str(), acc.tot_idle_time);
  res += buf;
  sprintf (buf, "tot_idle_percent%s\t%.3lf%%\n", pid_s.c_str(), acc.tot_idle_percent / cnt);
  res += buf;
  sprintf (buf, "recent_idle_percent%s\t%.3lf%%\n", pid_s.c_str(), acc.a_idle_percent / cnt);
  res += buf;

  return res;
}

struct CpuStatTimestamp {
  double timestamp;

  unsigned long long utime;
  unsigned long long stime;
  unsigned long long total_time;

  CpuStatTimestamp(){
  }

  CpuStatTimestamp (double timestamp, unsigned long long utime,
                    unsigned long long stime, unsigned long long total_time)
    : timestamp (timestamp), utime (utime), stime (stime), total_time (total_time) {
  }
};

struct CpuStat {
  double cpu_usage;
  double cpu_u_usage;
  double cpu_s_usage;

  CpuStat()
  : cpu_usage (0),
    cpu_u_usage (0),
    cpu_s_usage (0) {
  }
  CpuStat (const CpuStatTimestamp &from, const CpuStatTimestamp &to) {
    unsigned long long total_diff = to.total_time - from.total_time;
    cpu_u_usage = (double)(to.utime - from.utime) / (double)total_diff;
    cpu_s_usage = (double)(to.stime - from.stime) / (double)total_diff;
    cpu_usage = cpu_u_usage + cpu_s_usage;
  }
};

struct CpuStatSegment {
  typedef CpuStat Stat;
  CpuStatTimestamp first, last;
  void init (const CpuStatTimestamp &from) {
    first = from;
    last = from;
  }
  void update (const CpuStatTimestamp &from) {
    last = from;
  }
  double duration() {
    return last.timestamp - first.timestamp;
  }
  Stat get_stat() {
    return Stat (first, last);
  }
};

struct MiscStat {
  int running_workers_max;
  double running_workers_avg;
};

struct MiscStatTimestamp {
  double timestamp;
  int running_workers;
  MiscStatTimestamp (double timestamp, int running_workers)
    : timestamp (timestamp), running_workers (running_workers) {
    }
};

struct MiscStatSegment {
  typedef MiscStat Stat;

  unsigned long long stat_cnt;
  unsigned long long running_workers_sum;
  int running_workers_max;
  double first_timestamp, last_timestamp;

  void update (const MiscStatTimestamp &from) {
    last_timestamp = from.timestamp;
    stat_cnt++;
    running_workers_sum += from.running_workers;
    if (from.running_workers > running_workers_max) {
      running_workers_max = from.running_workers;
    }
  }
  void init (const MiscStatTimestamp &from) {
    stat_cnt = 0;
    running_workers_max = 0;
    running_workers_sum = 0;
    first_timestamp = from.timestamp;
    update (from);
  }
  double duration() {
    return last_timestamp - first_timestamp;
  }
  MiscStat get_stat() {
    MiscStat res;
    res.running_workers_max = running_workers_max;
    res.running_workers_avg = stat_cnt != 0 ? (double)running_workers_sum / (double)stat_cnt: -1.0;
    return res;
  }
};


template <class StatSegment, class StatTimestamp>
struct StatImpl {

  StatSegment first, second;
  double period;
  bool is_inited;

  StatImpl ()
    : period ((double)60 * 60 * 24 * 100000), is_inited (false) {
  }

  void set_period (double new_period) {
    period = new_period;
  }

  void add_timestamp (const StatTimestamp &new_timestamp) {
    if (!is_inited) {
      first.init (new_timestamp);
      second.init (new_timestamp);
      is_inited = true;
      return;
    }
    first.update (new_timestamp);
    second.update (new_timestamp);

    if (second.duration() > period) {
      first = second;
      second.init (new_timestamp);
    }
  }

  typename StatSegment::Stat get_stat (void) {
    if (!is_inited) {
      return typename StatSegment::Stat();
    }
    return first.get_stat();
  }
};

const int periods_n = 4;
const double periods_len[] = {0, 60, 60 * 10, 60 * 60};
const char *periods_desc[] = {"now", "1m", "10m", "1h"};
struct Stats {
  std::string engine_stats;
  std::string cpu_desc, misc_desc;
  php_immediate_stats_t istats;
  mem_info_t mem_info;
  StatImpl <CpuStatSegment, CpuStatTimestamp> cpu[periods_n];
  StatImpl <MiscStatSegment, MiscStatTimestamp> misc[periods_n];
  acc_stats_t acc_stats;

  Stats() {
    for (int i = 0; i < periods_n; i++) {
      cpu[i].set_period (periods_len[i]);
      misc[i].set_period (periods_len[i]);
    }

    for (int i = 0; i < periods_n; i++) {
      if (!cpu_desc.empty()) {
        cpu_desc += ",";
      }
      cpu_desc += periods_desc[i];
    }
    for (int i = 1; i < periods_n; i++) {
      if (!misc_desc.empty()) {
        misc_desc += ",";
      }
      misc_desc += periods_desc[i];
    }
    memset (&istats, 0, sizeof (istats));
    memset (&acc_stats, 0, sizeof (acc_stats));
  }
  void update (const CpuStatTimestamp &cpu_timestamp) {
    for (int i = 0; i < periods_n; i++) {
      cpu[i].add_timestamp (cpu_timestamp);
    }
  }
  void update (const MiscStatTimestamp &misc_timestamp) {
    for (int i = 1; i < periods_n; i++) {
      misc[i].add_timestamp (misc_timestamp);
    }
  }

  std::string to_string (int pid, bool full_flag = true, bool is_main = false) {
    std::string res;

    if (full_flag) {
      res += engine_stats;
    }

    char buffer[1000];

    std::string pid_s;
    if (!is_main) {
      assert (snprintf (buffer, 1000, " %d", pid) < 1000);
      pid_s = buffer;
    }

//    sprintf (buffer, "is_running = %d\n", istats.is_running);
//    sprintf (buffer, "is_paused = %d\n", istats.is_wait_net);

    std::string cpu_vals;
    for (int i = 0; i < periods_n; i++) {
      CpuStat s = cpu[i].get_stat();

      sprintf (buffer, " %6.2lf%%", cpu_cnt * (s.cpu_usage * 100));
      cpu_vals += buffer;
    }
    res += "cpu_usage" + pid_s + "(" + cpu_desc + ")\t" + cpu_vals + "\n";
    sprintf (buffer, "VM%s\t%lluKb\n", pid_s.c_str(), mem_info.vm);
    res += buffer;
    sprintf (buffer, "VM_max%s\t%lluKb\n", pid_s.c_str(), mem_info.vm_peak);
    res += buffer;
    sprintf (buffer, "RSS%s\t%lluKb\n", pid_s.c_str(), mem_info.rss);
    res += buffer;
    sprintf (buffer, "RSS_max%s\t%lluKb\n", pid_s.c_str(), mem_info.rss_peak);
    res += buffer;

    if (is_main) {
      std::string running_workers_max_vals;
      std::string running_workers_avg_vals;
      for (int i = 1; i < periods_n; i++) {
        MiscStat s = misc[i].get_stat();
        sprintf (buffer, " %7.3lf", s.running_workers_avg);
        running_workers_avg_vals += buffer;
        sprintf (buffer, " %7d", s.running_workers_max);
        running_workers_max_vals += buffer;
      }
      res += "running_workers_avg" + pid_s + "(" + misc_desc + ")\t" + running_workers_avg_vals + "\n";
      res += "running_workers_max" + pid_s + "(" + misc_desc + ")\t" + running_workers_max_vals + "\n";
    }

    if (full_flag) {
      res += acc_stats_to_str (pid_s, acc_stats);
    }

    return res;
  }
};

void init_mutex (pthread_mutex_t *mutex) {
  pthread_mutexattr_t attr;

  int err;
  err = pthread_mutexattr_init (&attr);
  assert (err == 0 && "failed to init mutexattr");
  err = pthread_mutexattr_setrobust_np (&attr, PTHREAD_MUTEX_ROBUST_NP);
  assert (err == 0 && "failed to setrobust_np for mutex");
  err = pthread_mutexattr_setpshared (&attr, PTHREAD_PROCESS_SHARED);
  assert (err == 0 && "failed to setpshared for mutex");

  err = pthread_mutex_init (mutex, &attr);
  assert (err == 0 && "failed to init mutex");
}

void shared_data_init (shared_data_t *data) {
  vkprintf (2, "Init shared data: begin\n");
  init_mutex (&data->main_mutex);

  data->magic = SHARED_DATA_MAGIC;
  data->is_inited = 1;
  vkprintf (2, "Init shared data: end\n");
}


shared_data_t *get_shared_data (const char *name) {
  int ret;
  vkprintf (2, "Get shared data: begin\n");
  int fid = shm_open (name, O_RDWR, 0777);
  int init_flag = 0;
  if (fid == -1) {
    if (errno == ENOENT) {
      vkprintf (1, "shared memory entry is not exists\n");
      vkprintf (1, "create shared memory\n");
      fid = shm_open (name, O_RDWR | O_CREAT | O_EXCL, 0777);
      if (fid == -1) {
        vkprintf (1, "failed to create shared memory\n");
        assert (errno == EEXIST && "failed to created shared memory for unknown reason");
        vkprintf (1, "somebody created it before us! so lets just open it\n");
        fid = shm_open (name, O_RDWR, 0777);
        assert (fid != -1 && "failed to open shared memory");
      } else {
        init_flag = 1;
      }
    }
  }
  assert (fid != -1);

  size_t mem_len = sizeof (shared_data_t);
  ret = ftruncate (fid, mem_len);
  assert (ret == 0 && "failed to ftruncate shared memory");

  shared_data_t *data = (shared_data_t *)mmap (0, mem_len, PROT_READ | PROT_WRITE, MAP_SHARED, fid, 0);
  assert (data != MAP_FAILED && "failed to mmap shared memory");

  if (init_flag) {
    shared_data_init (data);
  } else {
    if (data->is_inited == 0) {
      vkprintf (1, "somebody is trying to init shared data right now. lets wait for a second\n");
      sleep (1);
    }
    assert (data->is_inited == 1 && "shared data is not inited yet");

    //TODO: handle this without assert. Use magic in shared_data_t
    assert (data->magic == SHARED_DATA_MAGIC && "wrong magic in shared data");
  }

  vkprintf (1, "Get shared data: end\n");
  return data;
}

void shared_data_lock (shared_data_t *data) {
  int err = pthread_mutex_lock (&data->main_mutex);
  if (err != 0) {
    if (err == EOWNERDEAD) {
      vkprintf (1, "owner of shared memory mutex is dead. trying to make mutex and memory consitent\n");

      err = pthread_mutex_consistent_np (&data->main_mutex);
      assert (err == 0 && "failed to make mutex constistent_np");
    } else {
      assert (0 && "unknown mutex lock error");
    }
  }
}

void shared_data_unlock (shared_data_t *data) {
  int err = pthread_mutex_unlock (&data->main_mutex);
  assert (err == 0 && "unknown mutex unlock error");
}

void master_data_remove_if_dead (master_data_t *master) {
  if (master->valid_flag) {
    unsigned long long start_time = get_pid_start_time (master->pid);
    if (start_time != master->start_time) {
      master->valid_flag = 0;
      dl_assert (me == NULL || master != me, dl_pstr ("[start_time = %llu] [master->start_time = %llu]",
            start_time, master->start_time));
    }
  }
}

void shared_data_update (shared_data_t *shared_data) {
  master_data_remove_if_dead (&shared_data->masters[0]);
  master_data_remove_if_dead (&shared_data->masters[1]);
}

void shared_data_get_masters (shared_data_t *shared_data, master_data_t **me, master_data_t **other) {
  *me = NULL;

  if (shared_data->masters[0].valid_flag == 0) {
    *me = &shared_data->masters[0];
    *other = &shared_data->masters[1];
  } else if (shared_data->masters[1].valid_flag == 0) {
    *me = &shared_data->masters[1];
    *other = &shared_data->masters[0];
  }
}

void master_init (master_data_t *me, master_data_t *other) {
  assert (me != NULL);
  memset (me, 0, sizeof (*me));

  if (other->valid_flag) {
    me->rate = other->rate + 1;
  } else {
    me->rate = 1;
  }

  me->pid = getpid();
  me->start_time = get_pid_start_time (me->pid);
  assert (me->start_time != 0);

  if (other->valid_flag) {
    me->generation = other->generation + 1;
  } else {
    me->generation = 1;
  }

  me->running_workers_n = 0;
  me->dying_workers_n = 0;
  me->to_kill = 0;

  me->own_http_fd = 0;
  me->http_fd_port = -1;

  me->ask_http_fd_generation = 0;
  me->sent_http_fd_generation = 0;

  me->valid_flag = 1; //NB: must be the last operation.
}

void run_master();

static volatile long long local_pending_signals = 0;

static void sigusr1_handler (const int sig) {
  const char message[] = "got SIGUSR1, rotate logs.\n";
  kwrite (2, message, sizeof (message) - (size_t)1);

  local_pending_signals |= (1ll << sig);
  dl_signal (sig, sigusr1_handler);
}

struct worker_stats_t {
  long long total_cpu_time;
  long long used_cpu_time;
  long long total_queries;
  long long total_time;
};

typedef enum {mst_on, mst_off} master_state_t;
#define MAX_WORKER_STATS_LEN 10000
//TODO: save it as pointer
typedef struct {
  int pipe_read;
  int pipe_in_packet_num;
  int pipe_out_packet_num;
  struct connection *reader;
  struct connection pending_stat_queue;
} pipe_info_t;

typedef struct worker_info_tmp_t {
  worker_info_tmp_t *next_worker;

  int generation;
  pid_t pid;
  int is_dying;

  double last_activity_time;
  double start_time;
  double kill_time;
  int kill_flag;

  int stats_flag;

  pipe_info_t pipes[2];

  pid_info_t my_info;
  int valid_my_info;

  int logname_id;

  Stats *stats;
} worker_info_t;

static worker_info_t *workers[MAX_WORKERS];
static int worker_ids[MAX_WORKERS];
static int worker_ids_n;
static Stats stats;
static unsigned long long dead_stime, dead_utime;
static int me_workers_n;
static int me_running_workers_n;
static int me_dying_workers_n;
static double last_failed;
static int *http_fd;
static int http_fd_port;
static int (*try_get_http_fd)(void);
static master_state_t state;

static int signal_fd;

static int changed = 0;
static int failed = 0;
static int socket_fd = -1;
static int to_kill = 0, to_run = 0, to_exit = 0;
static long long generation;
static int receive_fd_attempts_cnt = 0;

static worker_info_t *free_workers = NULL;

void worker_init (worker_info_t *w) {
  w->stats = new Stats();
  w->valid_my_info = 0;
}

void worker_free (worker_info_t *w) {
  delete w->stats;
  w->stats = NULL;
}

worker_info_t *new_worker() {
  worker_info_t *w = free_workers;
  if (w == NULL) {
    w = (worker_info_t *)zmalloc0 (sizeof (worker_info_t));
  } else {
    free_workers = free_workers->next_worker;
  }
  worker_init (w);
  return w;
}

int get_logname_id() {
  assert (worker_ids_n > 0);
  return worker_ids[--worker_ids_n];
}
void add_logname_id (int id) {
  assert (worker_ids_n < MAX_WORKERS);
  worker_ids[worker_ids_n++] = id;
}

void delete_worker (worker_info_t *w) {
  add_logname_id (w->logname_id);
  if (w->valid_my_info) {
    dead_utime += w->my_info.utime;
    dead_stime += w->my_info.stime;
  }
  worker_free (w);
  w->next_worker = free_workers;
  free_workers = w;
}

void start_master (int *new_http_fd, int (*new_try_get_http_fd)(void), int new_http_fd_port) {
  save_verbosity = verbosity;
  if (verbosity < 1) {
    //verbosity = 1;
  }
  for (int i = MAX_WORKERS - 1; i >= 0; i--) {
    add_logname_id (i);
  }

  std::string s = cluster_name;
  for (int i = 0; i < (int)s.size(); i++) {
    if (!((s[i] >= 'A' && s[i] <= 'Z') || (s[i] >= 'a' && s[i] <= 'z') || s[i] == '_')) {
      s[i] = '_';
    }
  }

  shmem_name = std::string() + "/" + s + "_kphp_shm";
  socket_name = std::string() + s + "_kphp_fd_transfer";

  dl_assert (shmem_name.size() < NAME_MAX, "too long name for shared memory file");
  dl_assert (socket_name.size() < NAME_MAX, "too long socket name (for fd transfer)");
  printf ("[%s] [%s]\n", shmem_name.c_str(), socket_name.c_str());

  http_fd = new_http_fd;
  http_fd_port = new_http_fd_port;
  try_get_http_fd = new_try_get_http_fd;

  vkprintf (1, "start master: begin\n");

  sigemptyset (&empty_mask);

  //currently all signals are blocked

  sigemptyset (&mask);
  sigaddset (&mask, SIGCHLD);
  sigaddset (&mask, SIGPOLL);

  signal_fd = signalfd (-1, &mask, SFD_NONBLOCK);
  dl_passert (signal_fd >= 0, "failed to create signalfd");

  dl_signal (SIGUSR1, sigusr1_handler);

  //allow all signals except SIGPOLL and SIGCHLD
  if (sigprocmask (SIG_SETMASK, &mask, &orig_mask) < 0) {
    perror ("sigprocmask");
    _exit (1);
  }


  //TODO: other signals, daemonize, change user...
  if (shared_data == NULL) {
    shared_data = get_shared_data (shmem_name.c_str());
  }

  int attempts_to_start = 2;
  int is_inited = 0;
  while (attempts_to_start-- > 0) {
    vkprintf (1, "attemt to init master. [left attempts = %d]\n", attempts_to_start);
    shared_data_lock (shared_data);

    shared_data_update (shared_data);
    shared_data_get_masters (shared_data, &me, &other);

    if (me != NULL) {
      master_init (me, other);
      is_inited = 1;
    }

    shared_data_unlock (shared_data);

    if (!is_inited) {
      vkprintf (1, "other restart is in progress. sleep 5 seconds. [left attempts = %d]\n", attempts_to_start);
      sleep (5);
    } else {
      break;
    }
  }

  if (!is_inited) {
    vkprintf (0, "Failed to init master. It seems that two other masters are running\n");
    _exit (1);
  }

  vkprintf (1, "start master: end\n");

  run_master();
}

void terminate_worker (worker_info_t *w) {
  vkprintf (1, "kill_worker: send SIGTERM to [pid = %d]\n", (int)w->pid);
  kill (w->pid, SIGTERM);
  w->is_dying = 1;
  w->kill_time = my_now + 35;
  w->kill_flag = 0;
  workers_terminated++;

  me_running_workers_n--;
  me_dying_workers_n++;

  changed = 1;
}

int kill_worker (void) {
  int i;
  for (i = 0; i < me_workers_n; i++) {
    if (!workers[i]->is_dying) {
      terminate_worker (workers[i]);
      return 1;
    }
  }

  return 0;
}

#define MAX_HANGING_TIME 65.0

void kill_hanging_workers (void) {
  int i;

  static double last_terminated = -1;
  if (last_terminated + 30 < my_now) {
    for (i = 0; i < me_workers_n; i++) {
      if (!workers[i]->is_dying && workers[i]->last_activity_time + MAX_HANGING_TIME <= my_now) {
        vkprintf (1, "No stats recieved from worker [pid = %d]. Terminate it\n", (int)workers[i]->pid);
        workers_hung++;
        terminate_worker (workers[i]);
        last_terminated = my_now;
        break;
      }
    }
  }

  for (i = 0; i < me_workers_n; i++) {
    if (workers[i]->is_dying && workers[i]->kill_time <= my_now && workers[i]->kill_flag == 0) {
      vkprintf (1, "kill_hanging_worker: send SIGKILL to [pid = %d]\n", (int)workers[i]->pid);
      kill (workers[i]->pid, SIGKILL);
      workers_killed++;

      workers[i]->kill_flag = 1;

      changed = 1;
    }
  }
}

void workers_send_signal (int sig) {
  int i;
  for (i = 0; i < me_workers_n; i++) {
    if (!workers[i]->is_dying) {
      kill (workers[i]->pid, sig);
    }
  }
}

void pipe_on_get_packet (pipe_info_t *p, int packet_num) {
  assert (packet_num > p->pipe_in_packet_num);
  p->pipe_in_packet_num = packet_num;
  struct connection *c = &p->pending_stat_queue;
  while (c->first_query != (struct conn_query *)c) {
    struct conn_query *q = c->first_query;
    dl_assert (q != NULL, "...");
    dl_assert (q->requester != NULL, "...");
    //    fprintf (stderr, "processing delayed query %p for target %p initiated by %p (%d:%d<=%d)\n", q, c->target, q->requester, q->requester->fd, q->req_generation, q->requester->generation);
    if (q->requester->generation == q->req_generation) {
      int need_packet_num = *(int *)&q->extra;
      //vkprintf (1, "%d vs %d\n", need_packet_num, packet_num);

      //use sign of difference to handle int overflow
      int diff = packet_num - need_packet_num;
      assert (abs (diff) < 1000000000);
      if (diff >= 0) {
        assert (diff == 0);
        //TODO: use PMM_DATA (q->requester);
        q->cq_type->close (q);
      } else {
        break;
      }
    } else {
      q->cq_type->close (q);
    }
  }
}

void worker_set_stats (worker_info_t *w, const char *data) {
  acc_stats_t *acc_stats = (acc_stats_t *) data;
  w->stats->acc_stats = *acc_stats;

  data += sizeof (acc_stats_t);
  w->stats->engine_stats = data;
}

void worker_set_immediate_stats (worker_info_t *w, php_immediate_stats_t *istats) {
  w->stats->istats = *istats;
}


/*** PIPE connection ***/
struct pr_data {
  worker_info_t *worker;
  int worker_generation;
  pipe_info_t *pipe_info;
};
#define PR_DATA(c) ((struct pr_data *)(RPCC_DATA (c) + 1))

int pr_execute (struct connection *c, int op, int len) {
  vkprintf (3, "pr_execute: fd=%d, op=%d, len=%d\n", c->fd, op, len);

  int head[5];

  nb_iterator_t Iter;

  char *data;
  int data_len;

  switch (op) {
    case RPC_PHP_FULL_STATS:
    case RPC_PHP_IMMEDIATE_STATS:
      assert (len % (int)sizeof (int) == 0);
      len /= (int)sizeof (int);
      assert (len >= 3);

      nbit_set (&Iter, &c->In);
      assert (nbit_read_in (&Iter, head, sizeof (int) * 3) == sizeof (int) * 3);

      int packet_num = head[1];
      //long long id = *(long long *)(&head[3]);

      data_len = len - 3 - 1;
      data = (char *)malloc (sizeof (int) * data_len);
      assert (nbit_read_in (&Iter, data, data_len * (int)sizeof (int)));

      nbit_clear (&Iter);

      worker_info_t *w = PR_DATA (c)->worker;
      pipe_info_t *p = PR_DATA (c)->pipe_info;
      if (w->generation == PR_DATA (c)->worker_generation) {
        if (op == RPC_PHP_FULL_STATS) {
          w->last_activity_time = my_now;
          worker_set_stats (w, data);
        }
        if (op == RPC_PHP_IMMEDIATE_STATS) {
          worker_set_immediate_stats (w, (php_immediate_stats_t *)data);
        }

        pipe_on_get_packet (p, packet_num);
      } else {
        vkprintf (1, "connection [%p:%d] will be closed soon\n", c, c->fd);
      }

      free (data);

      break;
  }

  return SKIP_ALL_BYTES;

}

struct rpc_client_functions pipe_reader_methods;
void init_pipe_reader_methods (void) {
  pipe_reader_methods.execute = pr_execute;
}

struct connection *create_pipe_reader (int pipe_fd, conn_type_t *type, void *extra) {
  //fprintf (stderr, "create_pipe_reader [%d]\n", pipe_fd);

  if (check_conn_functions (type) < 0) {
    return NULL;
  }
  if (pipe_fd >= MAX_CONNECTIONS || pipe_fd < 0) {
    return NULL;
  }
  event_t *ev;
  struct connection *c;

  ev = Events + pipe_fd;
  c = Connections + pipe_fd;
  memset (c, 0, sizeof (struct connection));
  c->fd = pipe_fd;
  c->ev = ev;
  //c->target = NULL;
  c->generation = ++conn_generation;
  c->flags = C_WANTRD;
  init_builtin_buffer (&c->In, c->in_buff, BUFF_SIZE);
  init_builtin_buffer (&c->Out, c->out_buff, BUFF_SIZE);
  c->timer.wakeup = conn_timer_wakeup_gateway;
  c->type = type;
  c->extra = extra;
  c->basic_type = ct_pipe; //why not?
  c->status = conn_wait_answer;
  active_connections++;
  c->first_query = c->last_query = (struct conn_query *) c;

  //assert (c->type->init_outbound (c) >= 0);

  //fprintf (stderr, "epoll_sethandler\n");
  epoll_sethandler (pipe_fd, 0, server_read_write_gateway, c);
  //fprintf (stderr, "epoll_insert");
  epoll_insert (pipe_fd, (c->flags & C_WANTRD ? EVT_READ : 0) | (c->flags & C_WANTWR ? EVT_WRITE : 0) | EVT_SPEC);

  //fprintf (stderr, "exit create_pipe_reader [c = %p]\n", c);
  return c;
}

void init_pipe_info (pipe_info_t *info, worker_info_t *worker, int pipe) {
  info->pipe_read = pipe;
  info->pipe_out_packet_num = -1;
  info->pipe_in_packet_num = -1;
  struct connection *reader = create_pipe_reader (pipe, &ct_rpc_client, (void *)&pipe_reader_methods);
  if (reader != NULL) {
    PR_DATA (reader)->worker = worker;
    PR_DATA (reader)->worker_generation = worker->generation;
    PR_DATA (reader)->pipe_info = info;
  } else {
    vkprintf (0, "failed to create pipe_reader [fd = %d]", pipe);
  }
  info->reader = reader;

  struct connection *c = &info->pending_stat_queue;
  c->first_query = c->last_query = (struct conn_query *)c;
  c->generation = ++conn_generation;
  c->pending_queries = 0;
}

void clear_pipe_info (pipe_info_t *info) {
  info->pipe_read = -1;
  info->reader = NULL;
}

int run_worker (void) {
  dl_block_all_signals();

  int err;
  assert (me_workers_n < MAX_WORKERS);

  int new_pipe[2];
  err = pipe (new_pipe);
  dl_assert (err != -1, "failed to create a pipe");
  int new_fast_pipe[2];
  err = pipe (new_fast_pipe);
  dl_assert (err != -1, "failed to create a pipe");

  tot_workers_started++;
  pid_t new_pid = fork();
  assert (new_pid != -1 && "failed to fork");

  int logname_id = get_logname_id();
  if (new_pid == 0) {
    prctl (PR_SET_PDEATHSIG, SIGKILL); // TODO: or SIGTERM
    if (getppid() != me->pid) {
      vkprintf (0, "parent is dead just after start\n");
      exit (123);
    }
    //verbosity = 0;
    verbosity = save_verbosity;
    pid = getpid();

    master_pipe_write = new_pipe[1];
    master_pipe_fast_write = new_fast_pipe[1];
    close (new_pipe[0]);

    //Epoll_close should clear internal structures but shouldn't change epoll_fd.
    //The same epoll_fd will be used by master
    //Solution: close epoll_fd first
    //Problems: "epoll_ctl(): Invalid argument" is printed to stderr
    close (epoll_fd);
    epoll_fd = 0;

    init_epoll();


    for (int i = 0; i < MAX_CONNECTIONS; i++) {
      struct connection *conn = Connections + i;
      if (conn->status == conn_none) {
        continue;
      }
      force_clear_connection (conn);
    }

    active_outbound_connections = 0;
    active_connections = 0;
    //TODO: fill other stats with zero
    //

    signal_fd = -1;
    master_sfd = -1;

    if (logname_pattern) {
      char buf[100];
      snprintf (buf, 100, logname_pattern, logname_id);
      logname = strdup (buf);
    }

    return 1;
  }

  dl_restore_signal_mask();

  vkprintf (1, "new worker launched [pid = %d]\n", (int)new_pid);

  worker_info_t *worker = workers[me_workers_n++] = new_worker();
  worker->pid = new_pid;

  worker->is_dying = 0;
  worker->generation = ++conn_generation;
  worker->start_time = my_now;
  worker->logname_id = logname_id;
  worker->last_activity_time = my_now;


  init_pipe_info (&worker->pipes[0], worker, new_pipe[0]);
  init_pipe_info (&worker->pipes[1], worker, new_fast_pipe[0]);

  close (new_pipe[1]);
  close (new_fast_pipe[1]);

  me_running_workers_n++;

  changed = 1;

  return 0;
}

void remove_worker (pid_t pid) {
  int i;

  vkprintf (2, "remove workers [pid = %d]\n", (int)pid);
  for (i = 0; i < me_workers_n; i++) {
    if (workers[i]->pid == pid) {
      if (workers[i]->is_dying) {
        me_dying_workers_n--;
      } else {
        me_running_workers_n--;
        last_failed = my_now;
        failed++;
        workers_failed++;
      }

      clear_pipe_info (&workers[i]->pipes[0]);
      clear_pipe_info (&workers[i]->pipes[1]);
      delete_worker (workers[i]);

      me_workers_n--;
      workers[i] = workers[me_workers_n];

      vkprintf (1, "worker_removed: [running = %d] [dying = %d]\n", me_running_workers_n, me_dying_workers_n);
      return;
    }
  }

  assert (0 && "trying to remove unexisted worker");
}

void update_workers (void) {
  while (1) {
    int status;
    pid_t pid = waitpid (-1, &status, WNOHANG);
    if (pid > 0) {
      if (!WIFEXITED (status)) {
        tot_workers_strange_dead++;
      }
      tot_workers_dead++;
      remove_worker (pid);
      changed = 1;
    } else {
      break;
    }
  }
}


/*** send fd via unix socket ***/
void init_sockaddr_un (struct sockaddr_un *unix_socket_addr, const char *name) {
  memset (unix_socket_addr, 0, sizeof (*unix_socket_addr));
  unix_socket_addr->sun_family = AF_LOCAL;
  dl_assert (strlen (name) < sizeof (unix_socket_addr->sun_path), "too long socket name");
  strcpy (unix_socket_addr->sun_path, name);
}

static const struct sockaddr_un *get_socket_addr () {

  static struct sockaddr_un unix_socket_addr;
  static int inited = 0;

  if (!inited) {
    init_sockaddr_un (&unix_socket_addr, socket_name.c_str());
    inited = 1;
  }

  return &unix_socket_addr;
}

static int send_fd_via_socket (int fd) {
  int unix_socket_fd = socket (AF_LOCAL, SOCK_DGRAM, 0);
  dl_passert (fd >= 0, "failed to create socket");

  struct msghdr msg;
  char ccmsg[CMSG_SPACE(sizeof (fd))];
  struct cmsghdr *cmsg;
  struct iovec vec;  /* stupidity: must send/receive at least one byte */
  const char *str = "x";
  int rv;

  msg.msg_name = (struct sockaddr*)get_socket_addr();
  msg.msg_namelen = sizeof (*get_socket_addr());

  vec.iov_base = (void *)str;
  vec.iov_len = 1;
  msg.msg_iov = &vec;
  msg.msg_iovlen = 1;

  /* old BSD implementations should use msg_accrights instead of
   * msg_control; the interface is different. */
  msg.msg_control = ccmsg;
  msg.msg_controllen = sizeof (ccmsg);
  cmsg = CMSG_FIRSTHDR (&msg);
  cmsg->cmsg_level = SOL_SOCKET;
  cmsg->cmsg_type = SCM_RIGHTS;
  cmsg->cmsg_len = CMSG_LEN (sizeof (fd));
  *(int*)CMSG_DATA (cmsg) = fd;
  msg.msg_controllen = cmsg->cmsg_len;

  msg.msg_flags = 0;

  rv = (sendmsg (unix_socket_fd, &msg, 0) != -1);
  if (rv) {
    //why?
    //close (fd);
  }
  return rv;
}

static int sock_dgram (const char *path) {
  int err = unlink (path);
  dl_passert (err >= 0 || errno == ENOENT, dl_pstr ("failed to unlink %s", path));

  int fd = socket (PF_UNIX, SOCK_DGRAM, 0);
  fcntl (fd, F_SETFL, O_NONBLOCK);
  dl_passert (fd != -1, "failed to create a socket");
  err = bind (fd, (struct sockaddr *)get_socket_addr(), sizeof (*get_socket_addr()));
  dl_passert (err >= 0, "failed to bind socket");
  return fd;
}

/* receive a file descriptor over file descriptor fd */
static int receive_fd (int fd) {
  struct msghdr msg;
  struct iovec iov;
  char buf[1];
  int rv;
  int connfd = -1;
  char ccmsg[CMSG_SPACE (sizeof (connfd))];
  struct cmsghdr *cmsg;

  iov.iov_base = buf;
  iov.iov_len = 1;

  msg.msg_name = 0;
  msg.msg_namelen = 0;
  msg.msg_iov = &iov;
  msg.msg_iovlen = 1;
  /* old BSD implementations should use msg_accrights instead of
   * msg_control; the interface is different. */
  msg.msg_control = ccmsg;
  msg.msg_controllen = sizeof (ccmsg); /* ? seems to work... */

  rv = (int)recvmsg (fd, &msg, 0);
  if (rv == -1) {
    perror ("recvmsg");
    return -1;
  }

  cmsg = CMSG_FIRSTHDR (&msg);
  if (!cmsg->cmsg_type == SCM_RIGHTS) {
    fprintf (stderr, "got control message of unknown type %d\n",
      cmsg->cmsg_type);
    return -1;
  }
  return *(int*)CMSG_DATA (cmsg);
}


/*** Memcached interface for stats ***/
int php_master_get (struct connection *c, const char *key, int key_len);
int php_master_version (struct connection *c);
int php_master_wakeup (struct connection *c);
int php_master_get_end (struct connection *c, int key_cnt);
memcache_server_functions php_master_methods;
rpc_server_functions php_rpc_master_methods;

void init_php_master_methods (void) {
  php_master_methods.execute = mcs_execute;
  php_master_methods.mc_store = mcs_store;
  php_master_methods.mc_get_start = mcs_get_start;
  php_master_methods.mc_get = php_master_get;
  php_master_methods.mc_get_end = php_master_get_end;
  php_master_methods.mc_incr = mcs_incr;
  php_master_methods.mc_delete = mcs_delete;
  php_master_methods.mc_version = php_master_version;
  php_master_methods.mc_stats = mcs_stats;
  php_master_methods.mc_check_perm = mcs_default_check_perm;
  php_master_methods.mc_init_crypto = mcs_init_crypto;
  php_master_methods.mc_wakeup = php_master_wakeup;
  php_master_methods.mc_alarm = php_master_wakeup;

  php_rpc_master_methods.execute = default_tl_rpcs_execute;
  php_rpc_master_methods.check_ready = server_check_ready;
  php_rpc_master_methods.flush_packet = rpcs_flush_packet;
  php_rpc_master_methods.rpc_check_perm = rpcs_default_check_perm;
  php_rpc_master_methods.rpc_init_crypto = rpcs_init_crypto;
  php_rpc_master_methods.memcache_fallback_type = &ct_memcache_server;
  php_rpc_master_methods.memcache_fallback_extra = &php_master_methods;
}

struct pmm_data{
  int full_flag;
  int worker_pid;
  int need_end;
};

#define PMM_DATA(c) ((struct pmm_data *) (MCS_DATA(c) + 1))
int delete_stats_query (struct conn_query *q);
struct conn_query_functions stats_cq_func;

void stats_cq_func_init (void) {
  stats_cq_func.magic = CQUERY_FUNC_MAGIC;
  stats_cq_func.title = (char *)"stats-cq-query";
  stats_cq_func.wakeup = delete_stats_query;
  stats_cq_func.close = delete_stats_query;
};

struct stats_query_data {
  worker_info_t *worker;
  int pipe_out_packet_num;
};

int delete_stats_query (struct conn_query *q) {
  vkprintf (2, "delete_stats_query(%p,%p)\n", q, q->requester);

  delete_conn_query (q);
  zfree (q, sizeof (*q));
  return 0;
}

struct conn_query *create_stats_query (struct connection *c, pipe_info_t *pipe_info) {
  struct conn_query *q = (struct conn_query *)zmalloc (sizeof (struct conn_query));

  q->custom_type = 0;
  q->outbound = &pipe_info->pending_stat_queue;
  q->requester = c;
  q->start_time = precise_now;

  *(int *)&q->extra = pipe_info->pipe_out_packet_num;

  q->cq_type = &stats_cq_func;
  q->timer.wakeup_time = precise_now + 1;
  vkprintf (2, "create stats query [q = %p] [requester = %p]\n", q, q->requester);

  insert_conn_query (q);

  return q;
}

void create_stats_queries (struct connection *c, int op, int worker_pid) {
  int i;

  sigval to_send;
  to_send.sival_int = op;
  for (i = 0; i < me_workers_n; i++) {
    workers[i]->stats_flag = 0;
    if (!workers[i]->is_dying && (worker_pid < 0 || workers[i]->pid == worker_pid)) {
      pipe_info_t *pipe_info = NULL;
      if (op & SPOLL_SEND_IMMEDIATE_STATS) {
        pipe_info = &workers[i]->pipes[1];
      } else if (op & SPOLL_SEND_FULL_STATS) {
        pipe_info = &workers[i]->pipes[0];
      }
      dl_assert (pipe_info != NULL, "bug in code");
      sigqueue (workers[i]->pid, SIGSTAT, to_send);
      pipe_info->pipe_out_packet_num++;
      vkprintf (1, "create_stats_query [worker_pid = %d], [packet_num = %d]\n", workers[i]->pid, pipe_info->pipe_out_packet_num);
      if (c != NULL) {
        create_stats_query (c, pipe_info);
      }
    }
  }
  if (c != NULL) {
    c->status = conn_wait_net;
  }
}

int return_one_key_key (struct connection *c, const char *key) {
  std::string tmp;
  tmp += "VALUE ";
  tmp += key;
  write_out (&c->Out, tmp.c_str(), (int)tmp.size());
  return 0;
}

int return_one_key_val (struct connection *c, const char *val, int vlen) {
  char tmp[300];
  int l = sprintf (tmp, " 0 %d\r\n", vlen);
  assert (l < 300);
  write_out (&c->Out, tmp, l);
  write_out (&c->Out, val, vlen);
  write_out (&c->Out, "\r\n", 2);
  return 0;
}

int update_mem_stats (void);

std::string php_master_prepare_stats (bool full_flag, int worker_pid) {
  std::string res, header;
  header = stats.to_string (me == NULL ? 0 : (int)me->pid, false, true);
  int total_workers_n = 0;
  int running_workers_n = 0;
  int paused_workers_n = 0;
  static char buf[1000];

  acc_stats_t acc_stats;
  memset (&acc_stats, 0, sizeof (acc_stats));

  double min_uptime = 1e9;
  double max_uptime = -1;
  for (int i = 0; i < me_workers_n; i++) {
    worker_info_t *w = workers[i];
    if (!w->is_dying) {
      total_workers_n++;
      running_workers_n += w->stats->istats.is_running;
      paused_workers_n += w->stats->istats.is_wait_net;
      double worker_uptime = my_now - w->start_time;
      if (min_uptime > worker_uptime) {
        min_uptime = worker_uptime;
      }
      if (max_uptime < worker_uptime) {
        max_uptime = worker_uptime;
      }

      if (full_flag) {
        acc_stats_update (&acc_stats, w->stats->acc_stats);
      }

      if (worker_pid == -1 || w->pid == worker_pid) {
        sprintf (buf, "worker_uptime %d\t%.0lf\n", (int)w->pid, worker_uptime);
        res += buf;
        res += w->stats->to_string (w->pid, full_flag);
        res += "\n";
      }
    }
  }

  sprintf (buf, "uptime\t\%d\n", now - start_time);
  header += buf;
  sprintf (buf, "version\t%s\n", full_version_str);
  header += buf;
  if (engine_tag != NULL) {
    sprintf (buf + sprintf (buf, "kphp_version\t%s", engine_tag) - 2, "\n");
    header += buf;
  }
  sprintf (buf, "cluster_name\t\%s\n", cluster_name);
  header += buf;
  sprintf (buf, "min_worker_uptime\t%.0lf\n", min_uptime);
  header += buf;
  sprintf (buf, "max_worker_uptime\t%.0lf\n", max_uptime);
  header += buf;
  sprintf (buf, "total_workers\t%d\n", total_workers_n);
  header += buf;
  sprintf (buf, "running_workers\t%d\n", running_workers_n);
  header += buf;
  sprintf (buf, "paused_workers\t%d\n", paused_workers_n);
  header += buf;
  sprintf (buf, "dying_workers\t%d\n", me_dying_workers_n);
  header += buf;
  sprintf (buf, "tot_workers_started\t%ld\n", tot_workers_started);
  header += buf;
  sprintf (buf, "tot_workers_dead\t%ld\n", tot_workers_dead);
  header += buf;
  sprintf (buf, "tot_workers_strange_dead\t%ld\n", tot_workers_strange_dead);
  header += buf;
  sprintf (buf, "workers_killed\t%ld\n", workers_killed);
  header += buf;
  sprintf (buf, "workers_hung\t%ld\n", workers_hung);
  header += buf;
  sprintf (buf, "workers_terminated\t%ld\n", workers_terminated);
  header += buf;
  sprintf (buf, "workers_failed\t%ld\n", workers_failed);
  header += buf;

  if (full_flag) {
    header += acc_stats_to_str (std::string(), acc_stats);
  }
  return header + res;
}


int php_master_wakeup (struct connection *c) {
  if (c->status == conn_wait_net) {
    c->status = conn_expect_query;
  }

  pmm_data *D = PMM_DATA (c);
  bool full_flag = D->full_flag;
  int worker_pid = D->worker_pid;

  update_workers();
  update_mem_stats();

  std::string res = php_master_prepare_stats (full_flag, worker_pid);
  return_one_key_val (c, (char *)res.c_str(), (int)res.size());
  if (D->need_end) {
    write_out (&c->Out, "END\r\n", 5);
  }

  mcs_pad_response (c);
  c->flags |= C_WANTWR;

  return 0;
}

inline void eat_at (const char *key, int key_len, char **new_key, int *new_len) {
  if (*key == '^' || *key == '!') {
    key++;
    key_len--;
  }

  *new_key = (char *)key;
  *new_len = key_len;

  if ((*key >= '0' && *key <= '9') || (*key == '-' && key[1] >= '0' && key[1] <= '9')) {
    key++;
    while (*key >= '0' && *key <= '9') {
      key++;
    }

    if (*key++ == '@') {
      if (*key == '^' || *key == '!') {
        key++;
      }

      *new_len -= (int)(key - *new_key);
      *new_key = (char *)key;
    }
  }
}
int php_master_get (struct connection *c, const char *old_key, int old_key_len) {
  char *key;
  int key_len;
  eat_at (old_key, old_key_len, &key, &key_len);
  //stats
  //full_stats
  //workers_pids
  if (key_len == 12 && strncmp (key, "workers_pids", 12) == 0) {
    std::string res;
    for (int i = 0; i < workers_n; i++) {
      if (!workers[i]->is_dying) {
        char buf[30];
        sprintf (buf, "%d", workers[i]->pid);
        if (!res.empty()) {
          res += ",";
        }
        res += buf;
      }
    }
    return_one_key (c, old_key, (char *)res.c_str(), (int)res.size());
    return 0;
  }
  if (key_len >= 5 && strncmp (key, "stats", 5) == 0) {
    key += 5;
    pmm_data *D = PMM_DATA (c);
    D->full_flag = 0;
    if (strncmp (key, "_full", 5) == 0) {
      key += 5;
      D->full_flag = 1;
      D->worker_pid = -1;
      sscanf (key, "%d", &D->worker_pid);
    } else if (strncmp (key, "_fast", 5) == 0) {
      D->worker_pid = -2;
      key += 5;
      sscanf (key, "%d", &D->worker_pid);
    } else {
      D->full_flag = 1;
      D->worker_pid = -2;
    }

    create_stats_queries (c, SPOLL_SEND_STATS | SPOLL_SEND_IMMEDIATE_STATS, -1);
    if (D->full_flag) {
      create_stats_queries (c, SPOLL_SEND_STATS | SPOLL_SEND_FULL_STATS, D->worker_pid);
    }

    D->need_end = 1;
    return_one_key_key (c, old_key);
    if (c->pending_queries == 0) {
      D->need_end = 0;
      php_master_wakeup (c);
    }
    return 0;
  }
  return SKIP_ALL_BYTES;
}

int php_master_get_end (struct connection *c, int key_cnt) {
  if (c->status != conn_wait_net) {
    write_out (&c->Out, "END\r\n", 5);
  }
  return 0;
}

int php_master_version (struct connection *c) {
  write_out (&c->Out, "VERSION "PHP_MASTER_VERSION"\r\n", 9 + sizeof (PHP_MASTER_VERSION));
  return 0;
}

void php_master_rpc_stats (void) {
  std::string res (1 << 12, 0);
  res.resize (prepare_stats (NULL, &res[0], (1 << 12) - 2));
  res += php_master_prepare_stats (true, -1);
  tl_store_stats (res.c_str(), 0);
}


/*** Main loop functions ***/
void run_master_off() {
  vkprintf (2, "state: mst_off\n");
  assert (other->valid_flag);
  vkprintf (2, "other->to_kill_generation > me->generation --- %lld > %lld\n", other->to_kill_generation, me->generation);

  if (other->valid_flag && other->ask_http_fd_generation > me->generation) {
    vkprintf (1, "send http fd\n");
    send_fd_via_socket (*http_fd);
    //TODO: process errors
    me->sent_http_fd_generation = generation;
    changed = 1;
  }

  if (other->to_kill_generation > me->generation) {
    to_kill = other->to_kill;
  }

  if (me_running_workers_n + me_dying_workers_n == 0) {
    to_exit = 1;
    changed = 1;
    me->valid_flag = 0;
  }
}

void run_master_on() {
  vkprintf (2, "state: mst_on\n");

  static double prev_attempt = 0;
  if (!master_sfd_inited && !other->valid_flag && prev_attempt + 1 < my_now) {
    prev_attempt = my_now;
    if (master_port > 0 && master_sfd < 0) {
      master_sfd = server_socket (master_port, settings_addr, backlog, 0);
      if (master_sfd < 0) {
        static int failed_cnt = 0;

        failed_cnt++;
        if (failed_cnt > 2000) {
          vkprintf (-1, "cannot open master server socket at port %d: %m\n", master_port);
          exit (1);
        }
      } else {
        PID.port = (short)master_port;
        tl_stat_function = php_master_rpc_stats;
        init_listening_connection (master_sfd, &ct_rpc_server, &php_rpc_master_methods);
        master_sfd_inited = 1;
      }
    }
  }

  int need_http_fd = http_fd != NULL && *http_fd == -1;
  if (need_http_fd) {
    int can_ask_http_fd = other->valid_flag && other->own_http_fd && other->http_fd_port == me->http_fd_port;
    if (!can_ask_http_fd) {
      vkprintf (1, "Get http_fd via try_get_http_fd()\n");
      assert (try_get_http_fd != NULL && "no pointer for try_get_http_fd found");
      *http_fd = try_get_http_fd();
      assert (*http_fd != -1 && "failed to get http_fd");
      me->own_http_fd = 1;
      need_http_fd = false;
    } else {
      if (me->ask_http_fd_generation != 0 && other->sent_http_fd_generation > me->generation) {
        vkprintf (1, "read http fd\n");
        *http_fd = receive_fd (socket_fd);
        vkprintf (1, "http_fd = %d\n", *http_fd);

        if (*http_fd == -1) {
          vkprintf (1, "wait for a second...\n");
          sleep (1);
          *http_fd = receive_fd (socket_fd);
          vkprintf (1, "http_fd = %d\n", *http_fd);
        }


        if (*http_fd != -1) {
          dl_assert (*http_fd != -1, "failed to recieve http_fd");
          me->own_http_fd = 1;
          need_http_fd = false;
        } else {
          failed = 1;
        }
      } else {
        dl_assert (receive_fd_attempts_cnt < 4, dl_pstr ("failed to recieve http_fd: %d attempts are done\n", receive_fd_attempts_cnt));
        receive_fd_attempts_cnt++;

        vkprintf (1, "ask for http_fd\n");
        if (socket_fd != -1) {
          close (socket_fd);
        }
        socket_fd = sock_dgram (socket_name.c_str());
        me->ask_http_fd_generation = generation;
        changed = 1;
      }
    }
  }

  if (!need_http_fd) {
    int total_workers = me_running_workers_n + me_dying_workers_n + (other->valid_flag ? other->running_workers_n + other->dying_workers_n: 0);
    to_run = std::max (0, workers_n - total_workers);

    if (other->valid_flag) {
      int set_to_kill = std::max (std::min (MAX_KILL - other->dying_workers_n, other->running_workers_n), 0);

      if (set_to_kill > 0) {
        vkprintf (1, "[set_to_kill = %d]\n", set_to_kill);
        me->to_kill = set_to_kill;
        me->to_kill_generation = generation;
        changed = 1;
      }
    }
  }
}

int signal_epoll_handler (int fd, void *data, event_t *ev) {
  //empty
  vkprintf (2, "signal_epoll_handler\n");
  struct signalfd_siginfo fdsi;
  //fprintf (stderr, "A\n");
  int s = (int)read (signal_fd, &fdsi, sizeof (struct signalfd_siginfo));
  //fprintf (stderr, "B\n");
  if (s == -1) {
    if (0 && errno == EAGAIN) {
      vkprintf (1, "strange... no signal found\n");
      return 0;
    }
    dl_passert (0, "read signalfd_siginfo");
  }
  dl_assert (s == sizeof (struct signalfd_siginfo), dl_pstr ("got %d bytes of %d expected", s, (int)sizeof (struct signalfd_siginfo)));

  vkprintf (2, "some signal recieved\n");
  return 0;
}

int update_mem_stats (void) {
  get_mem_stats (me->pid, &stats.mem_info);
  for (int i = 0; i < me_workers_n; i++) {
    worker_info_t *w = workers[i];

    if (get_mem_stats (w->pid, &w->stats->mem_info) != 1) {
      continue;
    }
    mem_info_add (&stats.mem_info, w->stats->mem_info);
  }
  return 0;
}

static void cron (void) {
  unsigned long long cpu_total = 0;
  unsigned long long utime = 0;
  unsigned long long stime = 0;
  bool err;
  err = get_cpu_total (&cpu_total);
  dl_assert (err, "get_cpu_total failed");

  int running_workers = 0;
  for (int i = 0; i < me_workers_n; i++) {
    worker_info_t *w = workers[i];
    bool err;
    err = get_pid_info (w->pid, &w->my_info);
    w->valid_my_info = 1;
    if (!err) {
      continue;
    }

    CpuStatTimestamp cpu_timestamp (my_now, w->my_info.utime, w->my_info.stime, cpu_total);
    utime += w->my_info.utime;
    stime += w->my_info.stime;
    w->stats->update (cpu_timestamp);
    running_workers += w->stats->istats.is_running;
  }
  MiscStatTimestamp misc_timestamp (my_now, running_workers);
  stats.update (misc_timestamp);

  utime += dead_utime;
  stime += dead_stime;
  CpuStatTimestamp cpu_timestamp (my_now, utime, stime, cpu_total);
  stats.update (cpu_timestamp);

  create_stats_queries (NULL, SPOLL_SEND_STATS | SPOLL_SEND_IMMEDIATE_STATS, -1);
  static double last_full_stats = -1;
  if (last_full_stats + MAX_HANGING_TIME * 0.25 < my_now) {
    last_full_stats = my_now;
    create_stats_queries (NULL, SPOLL_SEND_STATS | SPOLL_SEND_FULL_STATS, -1);
  }
}

void run_master() {
  int err;
  int prev_time = 0;

  cpu_cnt = (int)sysconf (_SC_NPROCESSORS_ONLN);
  me->http_fd_port = http_fd_port;
  me->own_http_fd = http_fd != NULL && *http_fd != -1;

  err = epoll_sethandler (signal_fd, 0, signal_epoll_handler, NULL);
  dl_assert (err >= 0, "epoll_sethalder failed");
  err = epoll_insert (signal_fd, EVT_READ);
  dl_assert (err >= 0, "epoll_insert failed");


  //it is C++, not C :(
  init_php_master_methods();
  init_pipe_reader_methods();
  stats_cq_func_init();

  while (1) {
    vkprintf (2, "run_master iteration: begin\n");
    my_now = dl_time();

    changed = 0;
    failed = 0;
    to_kill = 0;
    to_run = 0;
    to_exit = 0;

    update_workers();

    shared_data_lock (shared_data);
    shared_data_update (shared_data);

    //calc state
    dl_assert (me->valid_flag && me->pid == getpid(), dl_pstr ("[me->valid_flag = %d] [me->pid = %d] [getpid() = %d]",
          me->valid_flag, me->pid, getpid()));
    if (other->valid_flag == 0 || me->rate > other->rate) {
      state = mst_on;
    } else {
      state = mst_off;
    }

    //calc generation
    generation = me->generation;
    if (other->valid_flag && other->generation > generation) {
      generation = other->generation;
    }
    generation++;

    if (state == mst_off) {
      run_master_off();
    } else if (state == mst_on) {
      run_master_on();
    } else {
      dl_unreachable ("unknown master state\n");
    }

    me->generation = generation;

    if (to_kill != 0 || to_run != 0) {
      vkprintf (1, "[to_kill = %d] [to_run = %d]\n", to_kill, to_run);
    }
    while (to_kill-- > 0) {
      kill_worker();
    }
    while (to_run-- > 0 && !failed) {
      if (run_worker()) {
        return;
      }
    }
    kill_hanging_workers();

    me->running_workers_n = me_running_workers_n;
    me->dying_workers_n = me_dying_workers_n;

    if (changed && other->valid_flag) {
      vkprintf (1, "wakeup other master [pid = %d]\n", (int)other->pid);
      kill (other->pid, SIGPOLL);
    }

    shared_data_unlock (shared_data);

    if (to_exit) {
      vkprintf (1, "all workers killed. exit\n");
      _exit (0);
    }

    if (local_pending_signals & (1ll << SIGUSR1)) {
      local_pending_signals &= ~(1ll << SIGUSR1);

      reopen_logs();
      workers_send_signal (SIGUSR1);
    }


    vkprintf (2, "run_master iteration: end\n");

    //struct timespec timeout;
    //timeout.tv_sec = failed ? 1 : 10;
    //timeout.tv_nsec = 0;

    //old solution:
    //sigtimedwait (&mask, NULL, &timeout);
    //new solution:
    //pselect. Has the opposite behavior, allows all signals in mask during its execution.
    //So we should use empty_mask instead of mask
    //pselect (0, 0, 0, 0, &timeout, &empty_mask);

    //int timeout = (failed ? 1 : 10) * 1000;
    int timeout = 1000;
    epoll_work (timeout);

    tl_restart_all_ready();

    if (now != prev_time) {
      prev_time = now;
      cron();
    }
  }
}
