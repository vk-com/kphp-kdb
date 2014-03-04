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

#include <stdio.h>
#include <string.h>
#include <assert.h>
#include "net-connections.h"
#include "net-memcache-server.h"
#include "net-crypto-aes.h"
#include "filesys-mount.h"
#include "filesys-data.h"
#include "kdb-data-common.h"
#include "filesys-memcache.h"

#define	MAX_KEY_LEN	1000
#define	MAX_VALUE_LEN	(1 << 20)

conn_type_t ct_filesys_engine_server = {
  .magic = CONN_FUNC_MAGIC,
  .title = "filesys_engine_server",
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

long long cmd_get, cmd_set, get_hits, get_missed, cmd_incr, cmd_decr, cmd_delete, cmd_version, cmd_stats;
char key_buff[MAX_KEY_LEN+1];
char value_buff[MAX_VALUE_LEN+1];

static int filesys_prepare_stats (struct connection *c) {
  dyn_update_stats ();
  char *stats_buffer = value_buff;
  int STATS_BUFF_SIZE = MAX_VALUE_LEN;
  int stats_len = snprintf (stats_buffer, STATS_BUFF_SIZE,
        "heap_allocated\t%ld\n"
        "heap_max\t%ld\n"
        "wasted_heap_blocks\t%d\n"
        "wasted_heap_bytes\t%ld\n"
        "free_heap_blocks\t%d\n"
        "free_heap_bytes\t%ld\n"
        "binlog_original_size\t%lld\n"
        "binlog_loaded_bytes\t%lld\n"
        "binlog_load_time\t%.6lfs\n"
        "current_binlog_size\t%lld\n"
        "binlog_path\t%s\n"
        "binlog_first_timestamp\t%d\n"
        "binlog_read_timestamp\t%d\n"
        "binlog_last_timestamp\t%d\n"
        "max_binlog_size\t%lld\n"
        "index_size\t%lld\n"
        "index_path\t%s\n"
        "index_load_time\t%.6lfs\n"
        "cmd_get\t%lld\n"
        "cmd_set\t%lld\n"
        "get_hits\t%lld\n"
        "get_misses\t%lld\n"
        "cmd_incr\t%lld\n"
        "cmd_decr\t%lld\n"
        "cmd_delete\t%lld\n"
        "cmd_version\t%lld\n"
        "cmd_stats\t%lld\n"
        "alloc_updates_tree_nodes\t%d\n"
        "directory_nodes\t%d\n"
        "inodes\t%d\n"
        "write_allocated_data\t%lld\n"
        "loaded_metafiles\t%d\n"
        "loaded_metafiles_bytes\t%lld\n"
        "max_loaded_metafiles_bytes\t%lld\n"
        "version\t" VERSION_STR " compiled at " __DATE__ " " __TIME__ " by gcc " __VERSION__ " "
#ifdef __LP64__
     "64-bit"
#else
     "32-bit"
#endif
        "\n",
        (long)(dyn_cur - dyn_first),
        (long)(dyn_last - dyn_first),
        wasted_blocks,
        wasted_bytes,
        freed_blocks,
        freed_bytes,
        binlog_loaded_size,
        log_readto_pos - jump_log_pos,
        binlog_load_time,
        log_pos,
        binlogname ? (strlen (binlogname) < 250 ? binlogname : "(too long)") : "(none)",
        log_first_ts,
        log_read_until,
        log_last_ts,
        max_binlog_size,
        index_size,
        engine_snapshot_name,
        index_load_time,
        cmd_get,
        cmd_set,
        get_hits,
        get_missed,
        cmd_incr,
        cmd_decr,
        cmd_delete,
        cmd_version,
        cmd_stats,
        alloc_tree_nodes,
        tot_directory_nodes,
        tot_inodes,
        tot_allocated_data,
        tot_loaded_metafiles,
        tot_loaded_index_data,
        max_loaded_index_data);

  if (stats_len >= STATS_BUFF_SIZE) {
    return STATS_BUFF_SIZE - 1;
  }

  stats_len += prepare_stats (c, stats_buffer + stats_len, STATS_BUFF_SIZE - stats_len);
  return stats_len;
}

static inline void init_tmp_buffers (struct connection *c) {
  free_tmp_buffers (c);
  c->Tmp = alloc_head_buffer ();
  assert (c->Tmp);
}

static int return_one_long (struct connection *c, const char *key, long long x) {
  static char s[32];
  return return_one_key (c, key, s, sprintf (s, "%lld", x));
}

int memcache_store (struct connection *c, int op, const char *key, int key_len, int flags, int delay, int size) {
  cmd_set++;
  inode_id_t inode;
  unsigned int offset = 0;
  int act = 0;
  int x = 0, y = 0;
  struct fuse_file_info fi;
  if (size < MAX_VALUE_LEN && key_len > 0 && op == mct_set) {
    switch (*key) {
      case 'c':
        if (!strncmp (key, "chmod", 5) && sscanf (key, "chmod%d", &x) >= 1) {
          act = 6;
        }
        if (!strncmp (key, "chown", 5) && sscanf (key, "chown%d,%d", &x, &y) >= 2) {
          act = 7;
        }
        break;
      case 'm':
        if (!strncmp (key, "mkdir", 5) && sscanf (key, "mkdir%d", &x) >= 1) {
          act = 1;
        }
        break;
      case 'p':
        if (!strcmp (key, "path")) {
          act = 3;
        }
      case 'r':
        if (!strcmp (key, "rmdir")) {
          act = 4;
        }
      case 'w':
        if (sscanf (key, "write%u,%lld", &offset, &inode) >= 2) {
          act = 2;
        }
        break;
      case 'u':
        if (!strcmp (key, "unlink")) {
          act = 5;
        }
    }
    int r = -11;
    if (act) {
      assert (read_in (&c->In, value_buff, size) == size);
      value_buff[size] = 0;
      switch (act) {
        case 1:
          r = ff_mkdir (value_buff, x);
          break;
        case 2:
          fi.fh = inode;
          r = ff_write (NULL, value_buff, size, offset, &fi);
          if (r >= 0) {
            r = 0;
          }
          break;
        case 3:
          init_tmp_buffers (c);
          write_out (c->Tmp, &size, sizeof (size));
          write_out (c->Tmp, value_buff, size);
          r = 0;
          break;
        case 4:
          r = ff_rmdir (value_buff);
          break;
        case 5:
          r = ff_unlink (value_buff);
          break;
        case 6:
          r = ff_chmod (value_buff, x);
          break;
        case 7:
          r = ff_chown (value_buff, x, y);
          break;
      }
      if (!r) {
        return 1;
      }
      if (verbosity > 0 && r < 0) {
        fprintf (stderr, "store: fail (act = %d, res = %d)\n", act, r);
      }
      return 0;
    }
  }
  return -2;
}

int parse_path (struct connection *c) {
  int sz;
  if (read_in (c->Tmp, &sz, sizeof (sz)) != sizeof (sz)) {
    return -1;
  }
  if (read_in (c->Tmp, value_buff, sz) != sz) {
    return -2;
  }
  value_buff[sz] = 0;
  return 0;
}

int memcache_get (struct connection *c, const char *key, int key_len) {
  if (verbosity >= 3) {
    fprintf (stderr, "memcache_get: key='%s'\n", key);
  }
  struct fuse_file_info fi;
  if (key_len >= 5 && !strncmp (key, "stats", 5)) {
    int stats_len = filesys_prepare_stats(c);
    return_one_key (c, key, value_buff, stats_len);
    return 0;
  }
  cmd_get++;
  if (key_len <= 0) {
    return 0;
  }
  int r = -11;
  int x;
  unsigned int offset, length;
  inode_id_t inode;

  switch (key[0]) {
    case 'c':
      if (!strncmp (key, "creat", 5) && sscanf (key, "creat%d", &x) >= 1 && !parse_path (c)) {
        r = ff_create (value_buff, x, &fi);
        if (!r) {
          return_one_long (c, key, fi.fh);
        }
      }
      break;
    case 'o':
      if (!strcmp (key, "open") && !parse_path (c)) {
        r = ff_open (value_buff, &fi);
        if (!r) {
          return_one_long (c, key, fi.fh);
        }
      }
      break;
    case 'r':
      if (!strncmp (key, "read", 4) && sscanf (key, "read%u,%u,%lld", &offset, &length, &inode) >= 3) {
        fi.fh = inode;
        r = ff_read (NULL, value_buff, length, offset, &fi);
        if (r >= 0) {
          return_one_key (c, key, value_buff, r);
        }
      }
  }

  free_tmp_buffers (c);
  return 0;
}

int memcache_version (struct connection *c) {
  cmd_version++;
  write_out (&c->Out, "VERSION "VERSION"\r\n", 9 + sizeof(VERSION));
  return 0;
}

int memcache_stats (struct connection *c) {
  cmd_stats++;
  int stats_len = filesys_prepare_stats (c);
  write_out (&c->Out, value_buff, stats_len);
  write_out (&c->Out, "END\r\n", 5);
  return 0;
}
