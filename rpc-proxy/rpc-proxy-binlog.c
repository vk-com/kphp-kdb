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
              2012-2013 Vitaliy Valtman
*/

#define _FILE_OFFSET_BITS 64

#include <unistd.h>
#include <stdio.h>

#include "kdb-binlog-common.h"
#include "kdb-data-common.h"
#include "kfs.h"
#include "rpc-proxy.h"
#include "common/pid.h"
#include "rpc-proxy-binlog.h"
#include "kdb-rpc-proxy-binlog.h"

long long jump_log_pos;
int jump_log_ts;
unsigned jump_log_crc32;
int ifd;
int binlog_mode_on;

void query_forget (long long qid);
void answer_forget (long long qid, struct process_id *pid);
void query_tx (long long new_qid, long long qid, struct process_id *pid, long long cluster_id, double timeout, int size, const int *data);
void answer_rx (long long qid);
void answer_tx (long long qid, struct process_id *pid, int op, int answer_len, int *answer);

int rpc_proxy_replay_logevent (struct lev_generic *E, int size) {
  switch (E->type) {
  case LEV_START:
    return (size < 24 ? -2 : 24);
  case LEV_NOOP:
  case LEV_TIMESTAMP:
  case LEV_CRC32:
  case LEV_ROTATE_TO:
  case LEV_ROTATE_FROM:
  case LEV_TAG:
  case LEV_CBINLOG_END:
    return default_replay_logevent (E, size);
  case LEV_QUERY_FORGET:
    {
      struct lev_query_forget *L = (void *)E;
      if (size < sizeof (*L)) { return -2; }
      query_forget (L->qid);
      return sizeof (*L);
    }
  case LEV_ANSWER_FORGET:
    {
      struct lev_answer_forget *L = (void *)E;
      if (size < sizeof (*L)) { return -2; }
      answer_forget (L->qid, &L->pid);
      return sizeof (*L);
    }

  case LEV_QUERY_TX:
    {
      struct lev_query_tx *L = (void *)E;
      if (size < sizeof (*L)) { return -2; }
      if (size < sizeof (*L) + L->data_size) { return -2; }
      query_tx (L->qid, L->old_qid, &L->pid, L->cluster_id, L->timeout, L->data_size, L->data);
      return sizeof (*L) + L->data_size;
    }    
  case LEV_ANSWER_TX:
    {
      struct lev_answer_tx *L = (void *)E;
      if (size < sizeof (*L)) { return -2; }
      if (size < sizeof (*L) + L->answer_len) { return -2; }
      answer_tx (L->qid, &L->pid, L->op, L->answer_len, L->answer);
      return sizeof (*L) + L->answer_len;
    }
  case LEV_ANSWER_RX:
    {
      struct lev_answer_rx *L = (void *)E;
      if (size < sizeof (*L)) { return -2; }
      answer_rx (L->qid);
      return sizeof (*L);
    }
  default:
    fprintf (stderr, "unknown log event type %08x at position %lld\n", E->type, log_cur_pos());
    return -1;
  }
}

int last[5];
int cur[5];

void update_index (long long pos, unsigned crc32, int timestamp) {
  cur[0] = RPC_PROXY_INDEX_MAGIC;
  *(long long *)(cur + 1) = pos;
  cur[3] = crc32;
  cur[4] = timestamp;
}

void write_index (void) {
  if (memcmp (last, cur, 20)) {
    lseek (ifd, 0, SEEK_SET);
    assert (write (ifd, cur, 20) == 20);
    memcpy (last, cur, 20);
  }
}

void flush_index (void) {
  fsync (ifd);
  close (ifd);
}

void load_index (kfs_file_handle_t index, const char *bname) {
  static int t[5];
  if (!index) {
    if (!binlog_disabled) {
      static char buf[1000];
      snprintf (buf, 1000, "%s.000000", bname);
      ifd = open (buf, O_WRONLY | O_TRUNC | O_CREAT, 0660);
      if (ifd < 0) {
        fprintf (stderr, "Error opening index: %m\n");
        exit (1);
      }
      t[0] = RPC_PROXY_INDEX_MAGIC;
      t[1] = 0;
      t[2] = 0;
      t[3] = 0;
      t[4] = 0;
      assert (write (ifd, t, 20) == 20);
    }
    return;
  }
  assert (read (index->fd, t, 20) == 20);
  if (t[0] != RPC_PROXY_INDEX_MAGIC) {
    fprintf (stderr, "Index is not from rpc-proxy\n");
    exit (3);
  }
  jump_log_pos = *(long long *)(t + 1);
  jump_log_crc32 = t[3];
  jump_log_ts = t[4];
  close (index->fd);
  if (!binlog_disabled) {
    ifd = open (index->info->filename, O_WRONLY);
  }
}


int init_rpc_proxy_data (int schema) {
  replay_logevent = rpc_proxy_replay_logevent;
  return 0;
}

void read_binlog (const char *bname) {
  assert (bname);
  assert (binlog_mode_on & 1);
  if (engine_preload_filelist (bname, binlogname) < 0) {
    fprintf (stderr, "cannot open binlog files for %s\n", binlogname ? binlogname : bname);
    exit (1);
  }
  
  replay_logevent = rpc_proxy_replay_logevent;


  log_ts_interval = 10;

  //Snapshot reading
  Snapshot = open_recent_snapshot (engine_snapshot_replica);

  if (Snapshot) {
    engine_snapshot_name = Snapshot->info->filename;
    engine_snapshot_size = Snapshot->info->file_size;

    if (verbosity) {
      fprintf (stderr, "load index file %s (size %lld)\n", engine_snapshot_name, engine_snapshot_size);
    }
  } else {
    engine_snapshot_name = NULL;
    engine_snapshot_size = 0;
  }

  load_index (Snapshot, bname);

  if (verbosity) {
    fprintf (stderr, "Reading binlog from position %lld\n", jump_log_pos);
  }

  log_ts_interval = 10;
  Binlog = open_binlog (engine_replica, jump_log_pos);
  if (!Binlog) {
    fprintf (stderr, "fatal: cannot find binlog for %s, log position %lld\n", engine_replica->replica_prefix, jump_log_pos);
    exit (1);
  }

  binlogname = Binlog->info->filename;

  if (verbosity) {
    fprintf (stderr, "replaying binlog file %s (size %lld)\n", binlogname, Binlog->info->file_size);
  }

  clear_log();

  init_log_data (jump_log_pos, jump_log_ts, jump_log_crc32);

  int i = replay_log (0, 1);

  if (i < 0) {
    fprintf (stderr, "fatal: error reading binlog\n");
    exit (1);
  } else {
    if (verbosity > 0) {
      fprintf (stderr, "read successfully\n");
    }
  }

  if (!binlog_disabled) {
    clear_read_log();
  }
  clear_write_log();
  
  if (!binlog_disabled) {
    assert (append_to_binlog (Binlog) >= log_readto_pos);
    binlog_fd = Binlog->fd;
  }
}
