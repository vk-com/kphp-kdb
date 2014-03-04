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

#ifndef __COMMON_DATA_H__
#define __COMMON_DATA_H__

struct proc_user_info {
  int seq_no;
  int pid;
  long long updated_at;
  int ip;
  int port;
  int start_utime;
  int aux_data_offset;
  long long binlog_pos;
  long long binlog_name_hash;
  int flags;
  int status_int;
  int reserved[15];
  int req_id;  // 0 = none, odd = proc->monitor, even = monitor->proc
  int req_type;
  int req_arg;
}; // must be 128 bytes long

struct proc_monitor_info {
  int seq_no;
  int pid;
  long long updated_at;
  int binlog_data_offset;
  int reserved[24];
  int req_id;
  int req_type;
  int req_arg;
}; // must be 128 bytes long

struct proc_data {
  struct proc_user_info uinfo[2];
  struct proc_monitor_info minfo[2];
};

#define	EH_MAGIC_NONE	0xdeadbeef
#define	EH_MAGIC_CONTROL	0xabac11ef

#define	CDATA_SHM_NAME	"/engine-common-data"

struct engine_shared_header {
  int eh_magic;
  int reserved;
  int monitor_pid;		// update with lock cmpxchg8b if zero
  int monitor_start_utime;
  int monitor_heap_start;
  int monitor_heap_cur;
  int monitor_heap_changing;
  int resvd[9];
  int rescan_pid_table;
  int rpc_pending;
  int rescan_binlog_table;
};

#define	CDATA_PIDS	(1 << 15)
#define CDATA_HEAP_SIZE	(1 << 24)
#define MIN_HEAP_PTR	(1024 + CDATA_PIDS * 64 * 4)
#define	MAX_HEAP_PTR	(MIN_HEAP_PTR + CDATA_HEAP_SIZE)
#define	MIN_CDATA_SIZE	(MAX_HEAP_PTR + 4096)

#define CDATA_HEAP_USED_MAGIC	0x5ee81a07
#define CDATA_HEAP_FREE_MAGIC	0x7ca029e3

struct engine_shared_data { 
  union {
    struct engine_shared_header e_hdr;
    char c_hdr[1024];
  };
  struct proc_data pdata[CDATA_PIDS]; 
  char e_heap[0]; 
}; 

struct engine_shared_heap_entry {
  int size;
  int owner_pid;
  int owner_start_utime;
  int magic;
  char data[0];
};

#define	BINLOGPOS_QUEUE_SIZE	(1024*2)

#define	BINLOGPOS_QUEUE_MAGIC	0x1ab9ef5d

struct binlog_timepos_heap_entry {
  struct engine_shared_heap_entry hdr;
  int refcnt, writer_pid;
  long long binlog_name_hash;
  int rptr, wptr;
  long long queue[BINLOGPOS_QUEUE_SIZE];
};

extern int cdata_shm_fd, cdata_my_pid;

extern struct engine_shared_data *CData;
extern struct proc_data *MyCData;

extern struct proc_user_info CDataUserInfo;
extern struct proc_monitor_info CDataMonitorInfo;

#define	CD_ENGINE	1
#define	CD_INDEXER	2
#define	CD_BINLOG_R	4
#define	CD_BINLOG_W	8
#define	CD_MAINLOOP	16
#define	CD_BLOCKED     	32
#define	CD_QUERY	64
#define	CD_SLOWQUERY	128
#define	CD_MON_PRIO0	0	/* don't want to be monitor */
#define	CD_MON_PRIO1	0x100	/* may work as monitor, don't want to */
#define	CD_MON_PRIO2	0x200	/* higher monitor priority */
#define	CD_MON_PRIO3	0x300	/* true monitor */
#define	CD_MON_PRIO_MASK	0x300
#define	CD_IS_MONITOR	0x400	/* is current "monitor" = master of common data */
#define	CD_ZOMBIE	0x1000


int init_common_data (int min_cdata_size, int p_flags);
int store_common_data (void);
int fetch_common_data (void);

int fetch_process_data (struct proc_user_info *buff, int pid);

int cstatus_binlog_pos (long long binlog_pos, int binlog_read_only);
int cstatus_set (int set_flags, int clear_flags);
int cstatus_binlog_name (char *binlog_name);

static inline struct proc_data *get_proc_status (int pid) { return CData ? &CData->pdata[pid] : 0; }
static inline void *conv_mon_ptr (int ptr) { return ptr >= MIN_HEAP_PTR && ptr <= MAX_HEAP_PTR ? (char *)CData + ptr : 0; }

struct pos_time_pair {
  long long pos;
  long long time;
};

extern struct pos_time_pair fbd_data[BINLOGPOS_QUEUE_SIZE];
extern int fbd_rptr, fbd_wptr;

/* updates binlog pos/time data in fbd_data, fbd_rptr, fbd_wptr */
int fetch_binlog_data (long long my_binlog_pos);
/* finds record with least binlog_time >= given, returns its pos or 0 */
long long lookup_binlog_time (long long binlog_time);


#endif
