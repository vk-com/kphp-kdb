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

    Copyright 2013 Nikolai Durov
              2013 Andrei Lopatin
*/

#define	_FILE_OFFSET_BITS	64

#include <assert.h>
#include <stdio.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <string.h>
#include <pwd.h>
#include "pid.h"
#include "server-functions.h"
#include "common-data.h"
#include "md5.h"

int cdata_shm_fd = -1, cdata_my_pid;
long long cdata_size;

char cdata_shm_name[128];

struct engine_shared_data *CData;
struct proc_data *MyCData;

struct proc_user_info CDataUserInfo;
struct proc_monitor_info CDataMonitorInfo;

static inline void begin_update_copy (struct proc_user_info *S) {
  ++S->seq_no;
  __sync_synchronize();
}

static inline void end_update_copy (struct proc_user_info *S) {
  __sync_synchronize();
  S->seq_no++;
  __sync_synchronize();
}

void update_user_info_copy (struct proc_user_info *S, struct proc_user_info *L) {
  L->seq_no = S->seq_no + 1;
  begin_update_copy (S);
  memcpy ((char *) S, (char *) L, 128);
  end_update_copy (S);
}

int cstatus_binlog_pos (long long binlog_pos, int binlog_read_only) {
  if (!MyCData) {
    return -1;
  }
  CDataUserInfo.binlog_pos = binlog_pos;
  int flags = (CDataUserInfo.flags & ~(CD_BINLOG_R | CD_BINLOG_W)) | (binlog_read_only ? CD_BINLOG_R : (binlog_pos ? CD_BINLOG_W : 0));
  CDataUserInfo.flags = flags;
  CDataUserInfo.updated_at = get_precise_time (1000000);

  int i;
  for (i = 0; i < 2; i++) {
    struct proc_user_info *S = MyCData->uinfo + i;
    begin_update_copy (S);
    S->binlog_pos = binlog_pos;
    S->updated_at = CDataUserInfo.updated_at;
    S->flags = flags;
    end_update_copy (S);
  }

  return 1;
}

int cstatus_set (int set_flags, int clear_flags) {
  if (!MyCData) {
    return -1;
  }
  int flags = (CDataUserInfo.flags & ~clear_flags) | set_flags;
  CDataUserInfo.flags = flags;
  CDataUserInfo.updated_at = get_precise_time (1000000);

  int i;
  for (i = 0; i < 2; i++) {
    struct proc_user_info *S = MyCData->uinfo + i;
    begin_update_copy (S);
    S->updated_at = CDataUserInfo.updated_at;
    S->flags = flags;
    end_update_copy (S);
  }

  return flags;
}


int store_common_data (void) {
  if (!MyCData) {
    return -1;
  }
  CDataUserInfo.updated_at = get_precise_time (1000000);
  update_user_info_copy (MyCData->uinfo, &CDataUserInfo);
  update_user_info_copy (MyCData->uinfo + 1, &CDataUserInfo);
  return 1;
}


int cstatus_binlog_name (char *binlog_name) {
  if (!MyCData) {
    return -1;
  }
  static unsigned char md5buf[16];
  if (!strncmp (binlog_name, "/var/lib/engine/", 16)) {
    binlog_name += 16;
  }
  vkprintf (2, "Stored common data binlog name %s\n", binlog_name);
  int l = strlen (binlog_name);
  md5 ((unsigned char *)binlog_name, l, md5buf);
  if (CDataUserInfo.binlog_name_hash == *(long long *)md5buf) {
    return 0;
  } else {
    CDataUserInfo.binlog_name_hash = *(long long *)md5buf;
    int res = store_common_data ();
    CData->e_hdr.rescan_binlog_table = 1;
    __sync_synchronize ();
    return res;
  }
}

int fetch_process_data (struct proc_user_info *buff, int pid) {
  struct proc_data *stat = get_proc_status (pid);
  int i, lim;
  for (i = 1, lim = 0; lim < 20; i ^= 1, lim++) {
    int seq_no = stat->uinfo[i].seq_no;
    if (seq_no & 1) {
      continue;
    }
    __sync_synchronize ();
    memcpy (buff, &stat->uinfo[i], sizeof (*buff));
    __sync_synchronize ();
    if (stat->uinfo[i].seq_no == seq_no) {
      return buff->pid == pid && !(buff->flags & CD_ZOMBIE) ? 2 : 1;
    }
  }
  return 0;
}

int fetch_common_data (void) {
  return -1;
}

int init_my_cdata (int p_flags) {
  memset (&CDataUserInfo, 0, sizeof (CDataUserInfo));
  init_common_PID ();
  CDataUserInfo.pid = PID.pid;
  CDataUserInfo.ip = PID.ip;
  CDataUserInfo.port = PID.port;
  CDataUserInfo.start_utime = PID.utime;
  CDataUserInfo.flags = p_flags;
  return store_common_data ();
}

int init_common_data (int min_cdata_size, int p_flags) {
  assert (sizeof (struct proc_user_info) == 128 && sizeof (struct proc_monitor_info) == 128);
  if (!*cdata_shm_name) {
    struct passwd *user = getpwuid (geteuid ());
    if (!user) {
      fprintf (stderr, "init_common_data: can not determine user name: %m\n");
      return -1;
    }
    assert (sprintf (cdata_shm_name, CDATA_SHM_NAME ".%s", user->pw_name) < 128);
  }
  if (cdata_shm_fd < 0) {
    cdata_shm_fd = shm_open (cdata_shm_name, O_CREAT | O_RDWR, 0600);
    if (cdata_shm_fd < 0) {
      fprintf (stderr, "shm_open (%s) failed: %m\n", cdata_shm_name);
      return -1;
    }
  }
  cdata_size = lseek (cdata_shm_fd, 0, SEEK_END);
  if (cdata_size < 0) {
    fprintf (stderr, "cannot lseek() on shared memory descriptor: %m\n");
    return 0;
  }
  if (min_cdata_size < MIN_CDATA_SIZE) {
    min_cdata_size = MIN_CDATA_SIZE;
  }
  if (cdata_size < min_cdata_size) {
    if (ftruncate (cdata_shm_fd, min_cdata_size) < 0) {
      fprintf (stderr, "cannot truncate shared memory to %d bytes: %m\n", min_cdata_size);
    }
  }
  cdata_size = lseek (cdata_shm_fd, 0, SEEK_END);
  if (cdata_size < min_cdata_size) {
    fprintf (stderr, "failed to grow shared memory: its size %lld is smaller than %d bytes\n", cdata_size, min_cdata_size);
    return 0;
  }
  void *addr = mmap (0, cdata_size, PROT_READ | PROT_WRITE, MAP_SHARED, cdata_shm_fd, 0);
  if (addr == MAP_FAILED) {
    fprintf (stderr, "cannot map shared memory: %m\n");
    return 0;
  }
  CData = addr;
  if (!CData->e_hdr.eh_magic) {
    CData->e_hdr.eh_magic = EH_MAGIC_NONE;
  }
  assert (CData->e_hdr.eh_magic == EH_MAGIC_NONE || CData->e_hdr.eh_magic == EH_MAGIC_CONTROL);
  cdata_my_pid = getpid ();
  if (cdata_my_pid <= 0 || cdata_my_pid >= CDATA_PIDS) {
    fprintf (stderr, "pid %d is larger than maximal %d\n", cdata_my_pid, CDATA_PIDS - 1);
    return 0;
  }
  MyCData = &CData->pdata[cdata_my_pid];

  int res = init_my_cdata (p_flags);

  CData->e_hdr.rescan_pid_table = 1;
  __sync_synchronize ();

  vkprintf (2, "initialized engine common data shared memory %s (size %lld) for pid %d [res=%d]\n", cdata_shm_name, cdata_size, cdata_my_pid, res);

  return res;
}

static long long fbd_binlog_name_hash;
static int fbd_last_wptr;
static long long fbd_copy[BINLOGPOS_QUEUE_SIZE];

struct pos_time_pair fbd_data[BINLOGPOS_QUEUE_SIZE];
int fbd_rptr, fbd_wptr;

static void fbd_clear_data (void) {
  fbd_rptr = fbd_wptr = 0;
  fbd_last_wptr = 0;
  fbd_binlog_name_hash = 0;
}

int fetch_binlog_data (long long my_binlog_pos) {
  if (!CData || !CDataUserInfo.binlog_name_hash) {
    return -1;
  }
  int data_ptr = MyCData->minfo[0].binlog_data_offset;
  if (!data_ptr || (data_ptr & 15)) {
    return -1;
  }
  struct binlog_timepos_heap_entry *HE = conv_mon_ptr (data_ptr);
  if (!HE || HE->hdr.magic != BINLOGPOS_QUEUE_MAGIC || HE->refcnt <= 0 || HE->binlog_name_hash != CDataUserInfo.binlog_name_hash) {
    return -1;
  }
  if (CDataUserInfo.binlog_name_hash != fbd_binlog_name_hash) {
    fbd_clear_data ();
    fbd_binlog_name_hash = CDataUserInfo.binlog_name_hash;
  }
  int rptr = HE->rptr, wptr = HE->wptr, sptr = fbd_last_wptr;
  unsigned count = wptr - rptr;
  if (count > BINLOGPOS_QUEUE_SIZE + 2 || (rptr & 1) || (wptr & 1) || (sptr & 1)) {
    return 0;
  }
  if ((unsigned) (sptr - rptr) > count) {
    sptr = fbd_last_wptr = rptr;
  }
  count = (unsigned) (wptr - sptr);
  assert (count <= BINLOGPOS_QUEUE_SIZE);
  if (count > BINLOGPOS_QUEUE_SIZE - 32) {
    count = BINLOGPOS_QUEUE_SIZE - 32;
    sptr = wptr - count;
  }
  if (!count) {
    return 0;
  }
  long long *to = fbd_copy;
  int a = sptr & (BINLOGPOS_QUEUE_SIZE - 1);
  int b = wptr & (BINLOGPOS_QUEUE_SIZE - 1);
  if (a > b) {
    int c = BINLOGPOS_QUEUE_SIZE - a;
    memcpy (to, HE->queue + a, c * 8);
    memcpy (to + c, HE->queue, b * 8);
    assert (b + c == count);
  } else {
    memcpy (to, HE->queue + a, count * 8);
    assert (b - a == count);
  }
  __sync_synchronize ();
  int new_rptr = HE->rptr;
  if (HE->hdr.magic != BINLOGPOS_QUEUE_MAGIC || HE->refcnt <= 0 || HE->binlog_name_hash != CDataUserInfo.binlog_name_hash) {
    return 0;
  }
  if ((unsigned) (new_rptr - rptr) >= (unsigned) (wptr - rptr)) { 
    return 0;
  }
  a = new_rptr - sptr;
  if (a < 0) {
    a = 0;
  }
  if (a & 1) {
    a++;
  }
  fbd_last_wptr = wptr;
  long long c_time = get_precise_time (100000);
  b = 0;
  for (; (unsigned) a < count; a += 2) {
    long long b_pos = fbd_copy[a];
    long long b_time = fbd_copy[a+1];
    vkprintf (3, "imported binlog data for name hash %016llx: position %lld (current%+lld), time %016llx (current%+.6lf)\n",
	      fbd_binlog_name_hash, b_pos, b_pos - my_binlog_pos, b_time, (b_time - c_time)/(1.0 * (1LL << 32)));
    if (b_time < c_time - (8LL << 32) || b_time > c_time + (8LL << 32)) {
      continue;
    }
    if (b_pos < my_binlog_pos - (1LL << 20) || b_pos > my_binlog_pos + (1LL << 20)) {
      continue;
    }
    fbd_data[fbd_wptr].pos = b_pos;
    fbd_data[fbd_wptr].time = b_time;
    fbd_wptr = (fbd_wptr + 1) & (BINLOGPOS_QUEUE_SIZE - 1);
    if (fbd_wptr == fbd_rptr) {
      fbd_rptr = (fbd_rptr + 1) & (BINLOGPOS_QUEUE_SIZE - 1);
    }
    b++;
  }
  return b;
}

static inline int lookup_binlog_binsearch (long long binlog_time, int a, int b) {
  int c;
  --a;
  while (b - a > 1) {
    c = (a + b) >> 1;
    if (fbd_data[c].time < binlog_time) {
      a = c;
    } else {
      b = c;
    }
  }
  return b;
} 

/* finds record with least binlog_time >= given, returns its pos or 0 */
long long lookup_binlog_time (long long binlog_time) {
  int res;
  if (fbd_rptr > fbd_wptr) {
    res = lookup_binlog_binsearch (binlog_time, fbd_rptr, BINLOGPOS_QUEUE_SIZE);
    if (res < BINLOGPOS_QUEUE_SIZE) {  
      return fbd_data[res].pos;
    }
    res = lookup_binlog_binsearch (binlog_time, 0, fbd_wptr);
  } else {
    res = lookup_binlog_binsearch (binlog_time, fbd_rptr, fbd_wptr);
  }
  return res == fbd_wptr ? 0 : fbd_data[res].pos;
}
