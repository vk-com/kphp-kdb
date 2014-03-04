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

    Copyright 2013 Nikolai Durov
              2013 Andrei Lopatin
*/

#define	_FILE_OFFSET_BITS	64

#include <assert.h>
#include <stdio.h>
#include <stddef.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <string.h>
#include <time.h>
#include <pwd.h>
#include "pid.h"
#include "server-functions.h"
#include "common-data.h"
#include "monitor/monitor-common.h"

int am_monitor = 0;
int have_monitor_heap = 0;

static inline void begin_update_monitor_data (struct proc_monitor_info *S) {
  ++S->seq_no;
  __sync_synchronize();
}

static inline void end_update_monitor_data (struct proc_monitor_info *S) {
  __sync_synchronize();
  S->seq_no++;
  __sync_synchronize();
}

int set_binlog_data_offset (struct proc_data *PData, int binlog_data_offset) {
  assert (PData);

  long long updated_at = get_precise_time (1000000);

  int i;
  for (i = 0; i < 2; i++) {
    struct proc_monitor_info *S = PData->minfo + i;
    begin_update_monitor_data (S);
    S->updated_at = updated_at;
    S->binlog_data_offset = binlog_data_offset;
    end_update_monitor_data (S);
  }

  return 0;
}



static int mon_heap_alloc (int size) {
  if (am_monitor && have_monitor_heap) {
    assert (size >= sizeof (struct engine_shared_heap_entry) && size < CDATA_HEAP_SIZE);
    size = (size + 15) & -16;
    int res = CData->e_hdr.monitor_heap_cur;
    assert (res >= offsetof (struct engine_shared_data, e_heap) && res <= offsetof (struct engine_shared_data, e_heap) + CDATA_HEAP_SIZE);
    if (res + size >= offsetof (struct engine_shared_data, e_heap) + CDATA_HEAP_SIZE) {
      return 0;
    }
    struct engine_shared_heap_entry *hdr = conv_mon_ptr (res);
    hdr->size = size;
    hdr->owner_pid = cdata_my_pid;
    hdr->owner_start_utime = CDataUserInfo.start_utime;
    hdr->magic = CDATA_HEAP_USED_MAGIC;
    CData->e_hdr.monitor_heap_cur = res + size;
    __sync_synchronize ();
    vkprintf (3, "monitor: allocating %d bytes in common data heap at %d\n", size, res);
    assert (CData->e_hdr.monitor_pid == cdata_my_pid);
    return res;
  }
  return 0;
}


static int mon_heap_free (int ptr) {
  vkprintf (3, "monitor: freeing block of common data heap at %d\n", ptr);
  assert (am_monitor && have_monitor_heap);
  assert (ptr >= offsetof (struct engine_shared_data, e_heap) && ptr < CData->e_hdr.monitor_heap_start + CDATA_HEAP_SIZE);
  struct engine_shared_heap_entry *hdr = conv_mon_ptr (ptr);
  assert (hdr->magic != CDATA_HEAP_FREE_MAGIC && hdr->magic > 0);
  hdr->magic = CDATA_HEAP_FREE_MAGIC;
  return 0;
}

static int try_reap (int pid) {
  struct proc_data *PData = get_proc_status (pid);
  if (PData->uinfo[0].flags & CD_ZOMBIE) {
    return 1;
  }
  int update_utime = ((int *)&PData->uinfo[0].updated_at)[1];
  int ctime = get_precise_time (1000000) >> 32;
  if ((unsigned)(ctime - update_utime) > 3) {
    static char buf[128];
    sprintf (buf, "/proc/%d", pid);
    if (!access (buf, F_OK)) {
      return 0;
    }
    __sync_or_and_fetch (&PData->uinfo[0].flags, CD_ZOMBIE);
    __sync_or_and_fetch (&PData->uinfo[1].flags, CD_ZOMBIE);
    __sync_or_and_fetch (&CData->e_hdr.rescan_pid_table, 1);
    vkprintf (1, "monitor: reaped process %d\n", pid);
    return 2;
  }
  return 0;
}

static int find_better_monitor_candidate (int priority) {
  int i;
  for (i = 0; i < CDATA_PIDS; i++) {
    struct proc_data *PData = get_proc_status (i);
    assert (PData);
    int pflags = PData->uinfo[0].flags;
    if (pflags & CD_ZOMBIE) {
      continue;
    }
    if ((pflags & CD_MON_PRIO_MASK) < priority) {
      continue;
    }
    if ((pflags & CD_MON_PRIO_MASK) == priority) {
      if (PData->uinfo[0].start_utime > CDataUserInfo.start_utime) {
	continue;
      }
      if (PData->uinfo[0].start_utime == CDataUserInfo.start_utime && i >= cdata_my_pid) {
	continue;
      }
    }
    try_reap (i);
    if (!(PData->uinfo[0].flags & CD_ZOMBIE)) {
      return i;  // better candidate exists
    }
  }
  return -1;
}

int active_pnum;
int active_pids[CDATA_PIDS+1], prev_active_pids[CDATA_PIDS+1];

int rescan_pid_table (void) {
  int i, j = 0, k = 0;
  if (!CData) {
    return -1;
  }
  if (am_monitor) {
    CData->e_hdr.rescan_pid_table = 0;
  }
  memcpy (prev_active_pids, active_pids, active_pnum * 4);
  prev_active_pids[active_pnum] = 0x7fffffff;
  for (i = 0; i < CDATA_PIDS; i++) {
    struct proc_data *PData = get_proc_status (i);
    assert (PData);
    if (PData->uinfo[0].pid == i && PData->uinfo[1].pid == i && PData->uinfo[0].start_utime > 0 && PData->uinfo[1].start_utime == PData->uinfo[0].start_utime && !(PData->uinfo[0].flags & CD_ZOMBIE) && !(PData->uinfo[1].flags & CD_ZOMBIE)) {
      // i is a good process
      while (prev_active_pids[j] < i) {
	vkprintf (1, "monitor: process %d deleted\n", prev_active_pids[j]);
	j++;
	CData->e_hdr.rescan_binlog_table = 1;
      }
      if (prev_active_pids[j] == i) {
	j++;
      } else {
	vkprintf (1, "monitor: found new process %d\n", i);
      }
      active_pids[k++] = i;
    }
  }
  while (prev_active_pids[j] < 0x7fffffff) {
    vkprintf (1, "monitor: process %d deleted\n", prev_active_pids[j]);
    j++;
    CData->e_hdr.rescan_binlog_table = 1;
  }
  active_pnum = k;
  active_pids[k] = 0x7fffffff;
  return k;
}

int mes_binlogs_num;
struct mon_binlog mes_binlogs[MES_BINLOGS_MAX];

int rescan_binlog_table (void) {
  int i, j, a, b, c;
  vkprintf (3, "monitor: rescanning binlog table\n");
  if (am_monitor) {
    CData->e_hdr.rescan_binlog_table = 0;
  }
  for (i = 0; i < mes_binlogs_num; i++) {
    mes_binlogs[i].mult = 0;
  }
  for (i = 0; i < active_pnum; i++) {
    struct proc_data *PData = get_proc_status (active_pids[i]);
    long long binlog_name_hash = PData->uinfo[0].binlog_name_hash;
    if (!binlog_name_hash || binlog_name_hash != PData->uinfo[1].binlog_name_hash) {
      continue;
    }
    a = -1;
    b = mes_binlogs_num;
    while (b - a > 1) {
      c = (a + b) >> 1;
      if (mes_binlogs[c].binlog_name_hash <= binlog_name_hash) {
	a = c;
      } else {
	b = c;
      }
    }
    int proc_bdata_offset = PData->minfo[0].binlog_data_offset;
    if (a >= 0 && mes_binlogs[a].binlog_name_hash == binlog_name_hash) {
      mes_binlogs[a].mult++;
      int my_bdata_offset = mes_binlogs[a].binlog_data_offset;
      if (proc_bdata_offset) {
	if (my_bdata_offset) {
	  if (my_bdata_offset != proc_bdata_offset && am_monitor) {
	    set_binlog_data_offset (PData, my_bdata_offset);
	  }
	  continue;
	}
	assert (!am_monitor);
	mes_binlogs[a].binlog_data_offset = proc_bdata_offset;
      } else if (am_monitor) {
	assert (my_bdata_offset);
	set_binlog_data_offset (PData, my_bdata_offset);
	vkprintf (3, "monitor: updating binlog info for process %d, binlog name hash %016llx to %d\n", active_pids[i], binlog_name_hash, my_bdata_offset);
      }
    } else {
      assert (mes_binlogs_num < MES_BINLOGS_MAX);
      memmove (mes_binlogs + b + 1, mes_binlogs + b, (mes_binlogs_num - b) * sizeof (struct mon_binlog));
      mes_binlogs_num++;
      mes_binlogs[b].binlog_name_hash = binlog_name_hash;
      mes_binlogs[b].mult = 1;
      if (proc_bdata_offset) {
	struct binlog_timepos_heap_entry *H = conv_mon_ptr (proc_bdata_offset);
	if (!H || H->hdr.magic != BINLOGPOS_QUEUE_MAGIC) {
	  proc_bdata_offset = 0;
	} else if (am_monitor) {
	  H->writer_pid = cdata_my_pid;
	}
      }
      mes_binlogs[b].binlog_data_offset = proc_bdata_offset;
      if (am_monitor) {
	if (!proc_bdata_offset) {
	  mes_binlogs[b].binlog_data_offset = proc_bdata_offset = mon_heap_alloc (sizeof (struct binlog_timepos_heap_entry));
	  assert (proc_bdata_offset);
	  struct binlog_timepos_heap_entry *H = conv_mon_ptr (proc_bdata_offset);
	  H->refcnt = 1;
	  H->binlog_name_hash = binlog_name_hash;
	  H->rptr = H->wptr = 0;
	  H->hdr.magic = BINLOGPOS_QUEUE_MAGIC;
	  H->writer_pid = cdata_my_pid;
	  set_binlog_data_offset (PData, proc_bdata_offset);
	  vkprintf (3, "monitor: allocating binlog info (process %d) for binlog name hash %016llx at %d\n", active_pids[i], binlog_name_hash, proc_bdata_offset);
	}
      }
    }
  }
  j = 0;
  for (i = 0; i < mes_binlogs_num; i++) {
    if (!mes_binlogs[i].mult) {
      int int_ptr = mes_binlogs[i].binlog_data_offset;
      vkprintf (3, "monitor: forgetting unused binlog name hash %016llx (data at %d)\n", mes_binlogs[i].binlog_name_hash, int_ptr);
      if (am_monitor) {
	assert (int_ptr);
	mon_heap_free (int_ptr);
      }
    } else {
      if (i != j) {
	mes_binlogs[j] = mes_binlogs[i];
      }
      j++;
    }
  }
  vkprintf (3, "monitor: %d active binlogs found\n", j);
  return mes_binlogs_num = j;
}

int update_binlog_postime_info (long long binlog_name_hash, long long binlog_pos, long long binlog_time) {
  int a = -1, b = mes_binlogs_num, c;
  while (b - a > 1) {
    c = (a + b) >> 1;
    if (mes_binlogs[c].binlog_name_hash <= binlog_name_hash) {
      a = c;
    } else {
      b = c;
    }
  }
  if (a < 0 || mes_binlogs[a].binlog_name_hash != binlog_name_hash) {
    return -1;
  }
  int offset = mes_binlogs[a].binlog_data_offset;
  if (offset <= 0 || offset > MIN_CDATA_SIZE) {
    return -1;
  }
  struct binlog_timepos_heap_entry *HE = conv_mon_ptr (offset);
  assert (HE);
  if (HE->hdr.magic != BINLOGPOS_QUEUE_MAGIC || HE->writer_pid != cdata_my_pid || HE->refcnt <= 0) {
    return -1;
  }
  int rptr = HE->rptr, wptr = HE->wptr;
  if ((rptr & 1) || (wptr & 1) || wptr > rptr + BINLOGPOS_QUEUE_SIZE) {
    return -1;
  }
  if (rptr < wptr + 2 - BINLOGPOS_QUEUE_SIZE) {
    HE->rptr = wptr + 2 - BINLOGPOS_QUEUE_SIZE;
    __sync_synchronize ();
  }
  HE->queue[wptr++ & (BINLOGPOS_QUEUE_SIZE - 1)] = binlog_pos;
  HE->queue[wptr++ & (BINLOGPOS_QUEUE_SIZE - 1)] = binlog_time;
  __sync_synchronize ();
  HE->wptr = wptr;
  __sync_synchronize ();
  return 1;
}

long long last_check_time;

int reap_all (void) {
  int cnt = 0, i;
  last_check_time = get_precise_time (1000000);
  for (i = 0; i < active_pnum; i++) {
    if (try_reap (active_pids[i]) > 1) {
      cnt++;
    }
  }
  return cnt;
}

int try_monitor_rpc (struct proc_data *PData) {
  //

  return -1;
}

int do_monitor_rpc (void) {
  int cnt = 0, i;
  last_check_time = get_precise_time (1000000);
  for (i = 0; i < active_pnum; i++) {
    if (try_monitor_rpc (get_proc_status (active_pids[i])) > 0) {
      cnt++;
    }
  }
  return cnt;
}

int monitor_work (void) {
  if (!am_monitor) {
    // maybe : try to do some RPC, try to reap old monitor, ...
    return 0;
  }
  assert (CData->e_hdr.monitor_pid == cdata_my_pid);
  if (CData->e_hdr.rescan_pid_table) {
    rescan_pid_table ();
  }
  if (CData->e_hdr.rescan_binlog_table) {
    rescan_binlog_table ();
  }
  if (CData->e_hdr.rpc_pending) {
    CData->e_hdr.rpc_pending = 0;
    do_monitor_rpc ();
  }
  if ((unsigned long long) get_precise_time (1000000) - last_check_time > (1 << 28)) {
    if (reap_all () > 0) {
      rescan_pid_table ();
    }
  }
  return 1;
}

int become_monitor (int prev_monitor_zombie) {
  assert (CData->e_hdr.monitor_pid == cdata_my_pid);
  if (CData->e_hdr.eh_magic == EH_MAGIC_NONE) {
    CData->e_hdr.eh_magic = EH_MAGIC_CONTROL;
  }
  CData->e_hdr.monitor_start_utime = CDataUserInfo.start_utime;
  CData->e_hdr.rescan_pid_table = 1;
  CData->e_hdr.rescan_binlog_table = 1;
  am_monitor = 1;

  if (!CData->e_hdr.monitor_heap_changing) {
    if (!CData->e_hdr.monitor_heap_start) {
      CData->e_hdr.monitor_heap_start = offsetof (struct engine_shared_data, e_heap);
      CData->e_hdr.monitor_heap_cur = CData->e_hdr.monitor_heap_start;
    }
  } else {
    CData->e_hdr.monitor_heap_start = CData->e_hdr.monitor_heap_cur;
    CData->e_hdr.monitor_heap_changing = 0;
  }
  
  assert (CData->e_hdr.monitor_heap_start >= offsetof (struct engine_shared_data, e_heap) &&
	  CData->e_hdr.monitor_heap_cur >= CData->e_hdr.monitor_heap_start &&
	  CData->e_hdr.monitor_heap_cur <= offsetof (struct engine_shared_data, e_heap) + CDATA_HEAP_SIZE);

  __sync_synchronize ();
  
  have_monitor_heap = 1;

  return monitor_work ();
}

int init_monitor (int priority) {
  if (priority & -4) {
    return -1;
  }
  if (!MyCData || !cdata_my_pid) {
    return -1;
  }
  if (!priority) {
    return 0;
  }
  priority *= CD_MON_PRIO1;
  cstatus_set (priority, CD_MON_PRIO_MASK);

  assert (CData->e_hdr.eh_magic == EH_MAGIC_NONE || CData->e_hdr.eh_magic == EH_MAGIC_CONTROL);

  int cur_monitor_pid = CData->e_hdr.monitor_pid;
  if (cur_monitor_pid) {
    try_reap (cur_monitor_pid);
    struct proc_data *MData = get_proc_status (cur_monitor_pid);
    if (MData && (!(MData->uinfo[0].flags & CD_ZOMBIE) || ((MData->uinfo[0].flags & CD_MON_PRIO_MASK) > priority))) {
      vkprintf (1, "other monitor exists with pid %d\n", cur_monitor_pid);
      return 2;   // monitor exists, but it is not us
    }
  }

  vkprintf (1, "waiting for one second before becoming monitor\n");

  int i;
  for (i = 0; i <= 20; i++) {
    if (time (0) != CDataUserInfo.start_utime) {
      break;
    }
    usleep (123456);
  }
  if (i == 21) {
    return -1;
  }

  i = find_better_monitor_candidate (priority);

  cur_monitor_pid = CData->e_hdr.monitor_pid;

  if (i >= 0) {
    vkprintf (1, "better monitor candidate %d exists (cur monitor pid is %d)\n", i, cur_monitor_pid);
    return cur_monitor_pid ? 2 : 1; // better candidate exists
  }

  int cur_monitor_is_zombie = 0;
  if (cur_monitor_pid) {
    struct proc_data *MData = get_proc_status (cur_monitor_pid);
    if (MData->uinfo[0].flags & CD_ZOMBIE) {
      cur_monitor_is_zombie = 1;
    }
  }

  if (!cur_monitor_pid || cur_monitor_is_zombie) {
    i = __sync_val_compare_and_swap (&CData->e_hdr.monitor_pid, cur_monitor_pid, cdata_my_pid);
    if (i == cur_monitor_pid) {
      vkprintf (1, "becoming monitor (pid %d), previous monitor was %d (%s)\n", cdata_my_pid, cur_monitor_pid, cur_monitor_is_zombie ? "zombie" : "alive");
      assert (CData->e_hdr.monitor_pid == cdata_my_pid);
      become_monitor (cur_monitor_is_zombie);
      return 3; // we are the monitor
    }
    cur_monitor_pid = CData->e_hdr.monitor_pid;
  }

  // have to send rpc to the other monitor
  vkprintf (0, "have to send RPC to the other monitor %d, but don't know how to do it\n", cur_monitor_pid);

  return -1;
}

int get_monitor_pid (void) {
  return CData ? CData->e_hdr.monitor_pid : 0;
}
