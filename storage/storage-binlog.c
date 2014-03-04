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

#include <assert.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <fcntl.h>
#include <unistd.h>
#include <time.h>

#include "server-functions.h"
#include "kdb-storage-binlog.h"
#include "storage-data.h"
#include "base64.h"

double binlog_load_time;
long long jump_log_pos = 0;
FILE *out;
int secret_in_base64url = 0;

int dump_log_pos = 0;

#define mytime() get_utime (CLOCK_MONOTONIC)

/* replay log */

inline void dump_line_header (long long pos) {
  if (dump_log_pos) { fprintf (stdout, "%lld\t",  pos); }
}

int dump_storage_replay_log (volume_t *V, long long start_pos) {
  const char *name = V->Binlog->info->filename;
  if (start_pos != lseek (V->Binlog->fd, start_pos, SEEK_SET)) {
    kprintf ("Illegal binlog pos: %lld\n", start_pos);
    return -1;
  }
  V->cur_log_pos = start_pos;
  V->log_crc32 = V->jump_log_crc32;
  while (V->cur_log_pos < V->Binlog->info->file_size) {
    if (!V->cur_log_pos) {
      unsigned char lev_start_buff[LEV_STORAGE_START_SIZE];
      struct lev_start *E = (struct lev_start *) &lev_start_buff;
      int bytes_read = read (V->Binlog->fd, E, LEV_STORAGE_START_SIZE);
      if (LEV_STORAGE_START_SIZE != bytes_read) {
        kprintf ("[%s] binlog read fail (LEV_START), bytes_read = %d. %m\n", name, bytes_read);
        return -1;
      }
      if (E->type != LEV_START) {
        kprintf ("[%s] expected LEV_START but %x found.\n", name, E->type);
        return -1;
      }
      if (storage_le_start (V, E) < 0) {
        kprintf ("[%s] storage_le_start fail\n", name);
        return -1;
      }
      V->log_crc32 = ~crc32_partial (E, LEV_STORAGE_START_SIZE, ~(V->log_crc32));
      V->cur_log_pos += LEV_STORAGE_START_SIZE;
    } else {
      struct lev_storage_file E;
      int sz = sizeof (struct lev_storage_file);
      if (sz != read (V->Binlog->fd, &E, sz)) {
        kprintf ("[%s] binlog read fail (LEV_STORAGE_FILE). %m\n", name);
        return -1;
      }

      long long event_pos = V->cur_log_pos;
      if (E.type != LEV_STORAGE_FILE && E.type != LEV_STORAGE_HIDE_FILE) {
        kprintf ("[%s] expected LEV_STORAGE_FILE|LEV_STORAGE_HIDE_FILE, but %x found at pos %lld\n", name, E.type, event_pos);
        return -1;
      }

      if (E.type == LEV_STORAGE_HIDE_FILE && E.size != 0) {
        kprintf ("[%s] expected E.size equal to zero in LEV_STORAGE_HIDE_FILE case, pos %lld\n", name, event_pos);
        return -1;
      }

      V->log_crc32 = ~crc32_partial (&E, sz, ~V->log_crc32);
      V->log_crc32 = compute_crc32_combine (V->log_crc32, E.crc32, E.size);
      int l = (E.size + 3) & -4;
      if (l != E.size) {
        const int zero = 0;
        V->log_crc32 = ~crc32_partial (&zero, l - E.size, ~V->log_crc32);
      }

      V->cur_log_pos += sz + l;
      if (V->cur_log_pos != lseek (V->Binlog->fd, V->cur_log_pos, SEEK_SET)) {
        kprintf ("[%s] binlog lseek fail. %m\n", name);
        V->cur_log_pos = event_pos;
        return -1;
      }

      dump_line_header (event_pos);
      fprintf (out, "0x%x\t%d\t", E.type, E.local_id);
      if (secret_in_base64url) {
        char secret_b64url[12];
        int l = base64url_encode ((unsigned char *) &E.secret, 8, secret_b64url, sizeof (secret_b64url));
        assert (l >= 0);
        fprintf (out, "%s\t", secret_b64url);
      } else {
        fprintf (out, "0x%llx\t", E.secret);
      }
      fprintf (out, "%d\t%d\n", E.mtime, E.content_type);

      struct lev_crc32 C;
      sz = sizeof (struct lev_crc32);
      long r = read (V->Binlog->fd, &C, sz);
      if (sz != r) {
        kprintf ("[%s] binlog read fail (LEV_CRC32) (sz=%lu,r=%lu): %m\n", name, (long)sz, (long)r);
        return -1;
      }

      event_pos = V->cur_log_pos;
      if (C.type != LEV_CRC32) {
        kprintf ("[%s] expected LEV_CRC32, but %x found at pos %lld\n", name, C.type, event_pos);
        return -1;
      }
      now = C.timestamp;
      if (event_pos != C.pos) {
        kprintf ("[%s] LEV_CRC32 field pos (%lld) != log event offset (%lld)\n", name, C.pos, event_pos);
        return -1;
      }
      if (C.crc32 != V->log_crc32) {
        kprintf ("[%s] LEV_CRC32 field crc (%x) != V->log_crc32 (%x), offset = %lld\n", name, C.crc32, V->log_crc32, event_pos );
        return -1;
      }

      V->log_crc32 = ~crc32_partial (&C, sz, ~V->log_crc32);
      V->cur_log_pos += sz;
    }
  }
  V->log_readto_pos = V->cur_log_pos;
  return 0;
}

void usage (void) {
  fprintf (stderr, "storage-binlog [-p] [-u] [-v] [-h] <binlog>\n"
    "Dumps storage-binlog to stdout.\n"
    "\t-p\tdump log pos\n"
    "\t-u\tdump secret in base64url (default: hex)\n"
  );
  exit (2);
}

int main (int argc, char *argv[]) {
  int i;
  out = stdout;
  while ((i = getopt (argc, argv, "tphuv")) != -1) {
    switch (i) {
    case 'p':
      dump_log_pos = 1;
      break;
    case 'u':
      secret_in_base64url = 1;
      break;
    case 'v':
      verbosity++;
      break;
    case 'h':
      usage();
      return 2;
    }
  }

  if (optind >= argc) {
    usage();
    return 2;
  }

  volume_t V;
  memset (&V, 0, sizeof (V));

  if (engine_preload_filelist ( argv[optind], binlogname) < 0) {
    kprintf ("cannot open binlog files for %s\n", binlogname ? binlogname : argv[optind]);
    exit (1);
  }

  vkprintf (3, "engine_preload_filelist done\n");
  Binlog = V.Binlog = open_binlog (engine_replica, jump_log_pos);
  if (!V.Binlog) {
    kprintf ("fatal: cannot find binlog for %s, log position %lld\n", engine_replica->replica_prefix, jump_log_pos);
    exit (1);
  }

  binlogname = V.Binlog->info->filename;

  vkprintf (1, "replaying binlog file %s (size %lld)\n", binlogname, Binlog->info->file_size);
  binlog_load_time = -mytime();

  dump_storage_replay_log (&V, 0);

  vkprintf (1, "replay log events finished\n");

  binlog_load_time += mytime();

  return 0;
}
