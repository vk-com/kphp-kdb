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
#define _GNU_SOURCE

#include <assert.h>
#include <stdio.h>
#include <stddef.h>
#include <stdlib.h>
#include <string.h>
#include <fcntl.h>
#include <signal.h>
#include <zlib.h>

#include "server-functions.h"
#include "net-connections.h"
#include "net-memcache-server.h"
#include "net-crypto-aes.h"
#include "kfs.h"
#include "kdb-data-common.h"
#include "kdb-filesys-binlog.h"
#include "filesys-utils.h"
#include "diff-patch.h"
#include "filesys-pending-operations.h"
#include "am-stats.h"

#define	VERSION_STR	"filesys-xfs-engine-0.13"
#define	BACKLOG	8192
#define TCP_PORT 11211
#define mytime() (get_utime(CLOCK_MONOTONIC))

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

const char *szTmpDirName = ".filesys-xfs-tmp";
const char *szBinlogpos = ".binlogpos";

int verbosity = 0;
double binlog_load_time, index_load_time;
long long binlog_loaded_size;
#define STATS_BUFF_SIZE	(16 << 10)
static char stats_buffer[STATS_BUFF_SIZE];

long long volume_id;
long long jump_log_pos;
int jump_log_ts;
unsigned jump_log_crc32;

int compression_level = 9;
static char *work_dir = NULL, *tmp_dir = NULL, *binlogpos_filename = NULL;
static int work_dir_length, tmp_dir_length;

static int start_transaction_id = 0;
static int transaction_id = 0;
static int transaction_file_no = 0;
static long long pending_saving_binlogpos_logpos = 0;
static long long binlogpos_pos = -1;
static int last_closed_transaction_id, last_closed_transaction_time;

conn_type_t ct_filesys_xfs_engine_server = {
  .magic = CONN_FUNC_MAGIC,
  .title = "filesys_xfs_engine_server",
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
#ifndef NOAES
  .crypto_init = aes_crypto_init,
  .crypto_free = aes_crypto_free,
  .crypto_encrypt_output = aes_crypto_encrypt_output,
  .crypto_decrypt_input = aes_crypto_decrypt_input,
  .crypto_needed_output_bytes = aes_crypto_needed_output_bytes,
#endif
};

int memcache_get (struct connection *c, const char *key, int key_len);
int memcache_stats (struct connection *c);

struct memcache_server_functions memcache_methods = {
  .execute = mcs_execute,
  .mc_store = mcs_store,
  .mc_get_start = mcs_get_start,
  .mc_get = memcache_get,
  .mc_get_end = mcs_get_end,
  .mc_incr = mcs_incr,
  .mc_delete = mcs_delete,
  .mc_version = mcs_version,
  .mc_stats = memcache_stats,
  .mc_check_perm = mcs_default_check_perm,
  .mc_init_crypto = mcs_init_crypto
};

int filesys_xfs_prepare_stats (struct connection *c) {
  stats_buffer_t sb;
  sb_prepare (&sb, c, stats_buffer, STATS_BUFF_SIZE);  
  sb_memory (&sb, AM_GET_MEMORY_USAGE_SELF);
  SB_BINLOG;
  SB_INDEX;

  sb_printf (&sb,
      "volume_id\t%lld\n"
      "work_dir\t%s\n"
      "snapshot_compression_level\t%d\n"
      "start_transaction_id\t%d\n"
      "transaction_id\t%d\n"
      "last_closed_transaction_id\t%d\n"
      "last_closed_transaction_time\t%d\n"
      "version\t%s\n",
    volume_id,
    work_dir,
    compression_level,
    start_transaction_id,
    transaction_id,
    last_closed_transaction_id,
    last_closed_transaction_time,
    FullVersionStr
  );
  return sb.pos;
}

int memcache_get (struct connection *c, const char *key, int key_len) {
  if (key_len >= 5 && !strncmp (key, "stats", 5)) {
    int stats_len = filesys_xfs_prepare_stats (c);
    return_one_key (c, key, stats_buffer, stats_len);
    return 0;
  }
  return 0;
}

int memcache_stats (struct connection *c) {
  int stats_len = filesys_xfs_prepare_stats (c);
  write_out (&c->Out, stats_buffer, stats_len);
  write_out (&c->Out, "END\r\n", 5);
  return 0;
}


/*
 *
 * GENERIC BUFFERED READ/WRITE
 *
 */

#define	BUFFSIZE	16777216

static int idx_fd, newidx_fd;
static char Buff[BUFFSIZE], *rptr = Buff, *wptr = Buff;
static long long bytes_read;

static void flushout (void) {
  int w, s;
  if (rptr < wptr) {
    s = wptr - rptr;
    w = write (newidx_fd, rptr, s);
    assert (w == s);
  }
  rptr = wptr = Buff;
}

static long long bytes_written;
static unsigned idx_crc32_complement;

static void clearin (void) {
  rptr = wptr = Buff + BUFFSIZE;
  bytes_read = 0;
  bytes_written = 0;
  idx_crc32_complement = -1;
}

static int writeout (const void *D, size_t len) {
  bytes_written += len;
  idx_crc32_complement = crc32_partial (D, len, idx_crc32_complement);
  const int res = len;
  const char *d = D;
  while (len > 0) {
    int r = Buff + BUFFSIZE - wptr;
    if (r > len) {
      r = len;
    }
    memcpy (wptr, d, r);
    d += r;
    wptr += r;
    len -= r;
    if (len > 0) {
      flushout ();
    }
  }
  return res;
}

static void *readin (size_t len) {
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
  int r = read (idx_fd, wptr, Buff + BUFFSIZE - wptr);
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
  assert (len >= 0 && len <= wptr - rptr);
  rptr += len;
}

static void bread (void *b, size_t len) {
  unsigned bs = BUFFSIZE >> 1;
  while (len > 0) {
    size_t l = len;
    if (l > bs) { l = bs; }
    void *p = readin (l);
    assert (p != NULL);
    memcpy (b, p, l);
    idx_crc32_complement = crc32_partial (b, l, idx_crc32_complement);
    b += l;
    readadv (l);
    bytes_read += l;
    len -= l;
  }
}

///////////////////////////////////////////////////////////////////////////////////////////

static void unlink_binlogpos (void) {
  if (binlogpos_pos >= 0) {
    if (unlink (binlogpos_filename)) {
      kprintf ("unlink (%s) failed. %m\n", binlogpos_filename);
      exit (1);
    }
    binlogpos_pos = -1;
  }
}

static void filesys_xfs_transaction_begin (struct lev_filesys_xfs_transaction *E) {
  pending_saving_binlogpos_logpos = 0;
  vkprintf (1, "Begin transaction %d.\n", E->id);
  int r = 0;
  pending_operations_reset (1);
  delete_file (tmp_dir);
  r = mkdir (tmp_dir, 0777);
  if (r < 0) {
    kprintf ("mkdir (%s) fail. %m\n", tmp_dir);
    exit (1);
  }
  transaction_id = E->id;
  transaction_file_no = 0;
}

static void filesys_xfs_transaction_end (struct lev_filesys_xfs_transaction *E) {
  assert (transaction_id == E->id);
  pending_operations_apply ();
  delete_file (tmp_dir);
  pending_saving_binlogpos_logpos = log_cur_pos () + 8;
  last_closed_transaction_id = transaction_id;
  last_closed_transaction_time = now;
}

#pragma	pack(push,4)
typedef struct {
  int chunk;
  int chunks;
  int M;
  char name[PATH_MAX];
  struct lev_filesys_xfs_file E;
  unsigned char *data;
  dyn_mark_t mrk;
} curfile_t;
#pragma	pack(pop)
curfile_t curfile;

static void curfile_dump (int level) {
  vkprintf (level, "curfile.name: %s\n", curfile.name);
  vkprintf (level, "curfile.chunk: %d\n", curfile.chunk);
  vkprintf (level, "curfile.chunks: %d\n", curfile.chunks);
  vkprintf (level, "curfile.M: %d\n", curfile.M);
  vkprintf (level, "curfile.E.old_size: %d\n", curfile.E.old_size);
  vkprintf (level, "curfile.E.patch_size: %d\n", curfile.E.patch_size);
  vkprintf (level, "curfile.E.mode: %d\n", curfile.E.mode);
  vkprintf (level, "curfile.E.actime: %d\n", curfile.E.actime);
  vkprintf (level, "curfile.E.modtime: %d\n", curfile.E.modtime);
  vkprintf (level, "curfile.E.old_crc32: %d\n", curfile.E.old_crc32);
  vkprintf (level, "curfile.E.patch_crc32: %d\n", curfile.E.patch_crc32);
  vkprintf (level, "curfile.E.new_crc32: %d\n", curfile.E.new_crc32);
  vkprintf (level, "curfile.E.uid: %d\n", (int) curfile.E.uid);
  vkprintf (level, "curfile.E.gid: %d\n", (int) curfile.E.gid);
  vkprintf (level, "curfile.E.filename_size: %d\n", (int) curfile.E.filename_size);
  vkprintf (level, "curfile.E.parts: %d\n", (int) curfile.E.parts);
}

/************************ curfile_data malloc **********************************/
static void *allocated_curfile_data_ptr = NULL;
static void curfile_data_alloc (unsigned parts) {
  dyn_mark (curfile.mrk);
  unsigned size = parts << 16;
  if (size > 0x1000000) {
    curfile.data = allocated_curfile_data_ptr = malloc (curfile.E.new_size);
    assert (curfile.data);
  } else {
    curfile.data = zmalloc (size);
  }
}

static void curfile_data_free (int release) {
  if (release) {
    dyn_release (curfile.mrk);
  }
  if (allocated_curfile_data_ptr != NULL) {
    free (allocated_curfile_data_ptr);
    allocated_curfile_data_ptr = NULL;
  }
}

/*********************************************************************************/

static void process_file (void) {
  void *allocated_ptr = NULL;
  struct stat st;
  st.st_mode = curfile.E.mode;
  st.st_atime = curfile.E.actime;
  st.st_mtime = curfile.E.modtime;
  st.st_uid = curfile.E.uid;
  st.st_gid = curfile.E.gid;
  int data_size = curfile.E.patch_size;
  if (curfile.data) {
    unsigned computed_crc32 = compute_crc32 (curfile.data, data_size);
    if (computed_crc32 != curfile.E.patch_crc32) {
      kprintf ("crc32 fails for %s, transcaction = %d\n", curfile.name, transaction_id);
      assert (computed_crc32 == curfile.E.new_crc32);
    }
  }

  if (curfile.E.type & XFS_FILE_FLAG_GZIP) {
    void *d = NULL;
    if (curfile.E.new_size > 0x1000000) {
      d = allocated_ptr = malloc (curfile.E.new_size);
      assert (d);
    } else {
      d = zmalloc (curfile.E.new_size);
    }
    uLongf destLen = curfile.E.new_size;
    assert (Z_OK == uncompress (d, &destLen, curfile.data, curfile.M));
    data_size = destLen;
    curfile.data = d;
  }

  if (curfile.E.type & XFS_FILE_FLAG_DIFF) {
    char full_oldpath[PATH_MAX];
    assert (snprintf (full_oldpath, PATH_MAX, "%s/%s", work_dir, curfile.name) < PATH_MAX);
    void *d = zmalloc (curfile.E.new_size);
    dyn_mark_t mrk;
    dyn_mark (mrk);
    file_t x;
    x.filename = full_oldpath;
    if (lstat (full_oldpath, &x.st)) {
      kprintf ("lstat (%s) failed. %m\n", full_oldpath);
      assert (0);
    }
    unsigned char *e;
    int L, r = get_file_content (full_oldpath, &x, &e, &L);
    if (r < 0) {
      kprintf ("get_file_content (%s) returns error code %d.\n", full_oldpath, r);
      assert (0);
    }

    if (curfile.E.old_size != L) {
      curfile_dump (0);
      kprintf ("found %d byte file \"%s\", but expected file size is equal to %d.\n", L, full_oldpath, curfile.E.old_size);
      assert (L == curfile.E.old_size);
    }

    if (compute_crc32 (e, L) != curfile.E.old_crc32) {
      curfile_dump (0);
      kprintf ("crc32 didn't matched for the file \"%s\"\n", full_oldpath);
      assert (compute_crc32 (e, L) == curfile.E.old_crc32);
    }

    r = vk_patch (e, L, curfile.data, data_size, d, curfile.E.new_size);
    vkprintf (4, "vk_patch returns %d.\n", r);
    if (r < 0) {
      kprintf ("vk_patch returns error code %d (filename = %s).\n", r, curfile.name);
      assert (0);
    }
    if (r != curfile.E.new_size) {
      kprintf ("vk_patch unpack %d bytes, expected %d bytes (filename = %s)\n.", r, curfile.E.new_size, curfile.name);
      assert (0);
    }
    dyn_release (mrk);
    data_size = curfile.E.new_size;
    curfile.data = d;
  }

  if (curfile.data && (curfile.E.type & (XFS_FILE_FLAG_GZIP | XFS_FILE_FLAG_DIFF))) {
    assert (compute_crc32 (curfile.data, curfile.E.new_size) == curfile.E.new_crc32);
  }

  pending_operation_copyfile (transaction_id, &transaction_file_no, curfile.name, curfile.data, data_size, &st, NULL, curfile.data ? curfile.mrk : NULL);
  vkprintf (4, "zero fill (%s)\n", curfile.name);

  curfile_data_free (0);

  memset (&curfile, 0, sizeof (curfile));
  if (allocated_ptr != NULL) {
    free (allocated_ptr);
  }
}

static void filesys_xfs_change_attrs (struct lev_filesys_xfs_change_attrs *E) {
  char name[PATH_MAX];
  assert (E->filename_size < PATH_MAX - 1);
  memcpy (name, E->filename, E->filename_size);
  name[E->filename_size] = 0;
  vkprintf (3, "name = %s\n", name);
  struct stat st;
  st.st_mode = E->mode;
  st.st_atime = E->actime;
  st.st_mtime = E->modtime;
  st.st_uid = E->uid;
  st.st_gid = E->gid;
  pending_operation_push (pending_operation_create (pot_copy_attrs, NULL, name, &st));
}

static void filesys_xfs_file (struct lev_filesys_xfs_file *E) {
  assert (curfile.E.type == 0);
  memcpy (&curfile.E, E, sizeof (struct lev_filesys_xfs_file));
  curfile.chunk = curfile.M = 0;
  curfile.chunks = curfile.E.parts;
  assert (E->filename_size < PATH_MAX - 1);
  memcpy (curfile.name, E->filename, E->filename_size);
  curfile.name[E->filename_size] = 0;
  if (!curfile.E.parts) {
    process_file ();
  } else {
    curfile_data_alloc (E->parts);
  }
}

static void filesys_xfs_file_chunk (struct lev_filesys_xfs_file_chunk *E) {
  assert (E->part == curfile.chunk);
  assert (E->part < curfile.chunks);
  int chunk_size = E->size + 1;
  assert (E->part == curfile.chunks - 1 || chunk_size == 0x10000);
  memcpy (&curfile.data[curfile.M], &E->data[0], chunk_size);
  curfile.M += chunk_size;
  curfile.chunk++;
  vkprintf (4, "%d chunk of %d chunks readed (%s).\n", (int) curfile.chunk, (int) curfile.chunks, curfile.name);
  if (curfile.chunk == curfile.chunks) {
    process_file ();
  }
}

static void filesys_xfs_file_remove (struct lev_filesys_rmdir *E) {
  char name[PATH_MAX];
  assert (E->dirpath_size < PATH_MAX);
  memcpy (name, E->dirpath, E->dirpath_size);
  name[E->dirpath_size] = 0;
  pending_operation_push (pending_operation_create (pot_remove, NULL, name, NULL));
}

static int filesys_xfs_le_start (struct lev_start *E) {
  if (E->schema_id != FILESYS_SCHEMA_V1) {
    return -1;
  }
  long long l;
  log_split_min = E->split_min;
  log_split_max = E->split_max;
  log_split_mod = E->split_mod;
  assert (log_split_mod > 0 && log_split_min >= 0 && log_split_min + 1 ==  log_split_max && log_split_max <= log_split_mod);
  assert (E->extra_bytes == 8);
  memcpy (&l, E->str, 8);
/*
  if (volume_id >= 0 && l != volume_id) {
    fprintf (stderr, "Binlog volume_id isn't matched.\n");
    exit (1);
  }
*/
  volume_id = l;
  return 0;
}

int filesys_xfs_replay_logevent (struct lev_generic *E, int size);

int init_filesys_data (int schema) {
  replay_logevent = filesys_xfs_replay_logevent;
  return 0;
}

int filesys_xfs_replay_logevent (struct lev_generic *E, int size) {
  vkprintf (4, "LE (type=%x, offset=%lld)\n", E->type, log_cur_pos ());
  int s;
  switch (E->type) {
    case LEV_START:
      if (size < 24 || E->b < 0 || E->b > 4096) { return -2; }
      s = 24 + ((E->b + 3) & -4);
      if (size < s) { return -2; }
      return filesys_xfs_le_start ((struct lev_start *) E) >= 0 ? s : -1;
    case LEV_NOOP:
    case LEV_TIMESTAMP:
    case LEV_CRC32:
    case LEV_ROTATE_FROM:
    case LEV_ROTATE_TO:
    case LEV_TAG:
      return default_replay_logevent (E, size);
    case LEV_FILESYS_XFS_BEGIN_TRANSACTION:
      if (size < 8) { return -2; }
      unlink_binlogpos ();
      filesys_xfs_transaction_begin ((struct lev_filesys_xfs_transaction *) E);
      return 8;
    case LEV_FILESYS_XFS_END_TRANSACTION:
      if (size < 8) { return -2; }
      unlink_binlogpos ();
      filesys_xfs_transaction_end ((struct lev_filesys_xfs_transaction *) E);
      return 8;
    case LEV_FILESYS_XFS_FILE:
    case LEV_FILESYS_XFS_FILE+XFS_FILE_FLAG_GZIP:
    case LEV_FILESYS_XFS_FILE+XFS_FILE_FLAG_DIFF:
      if (size < sizeof (struct lev_filesys_xfs_file)) {
        return -2;
      }
      s = sizeof (struct lev_filesys_xfs_file) + ((struct lev_filesys_xfs_file *) E)->filename_size;
      if (size < s) { return -2; }
      unlink_binlogpos ();
      filesys_xfs_file ((struct lev_filesys_xfs_file *) E);
      return s;
    case LEV_FILESYS_XFS_FILE_CHUNK:
      if (size < sizeof (struct lev_filesys_xfs_file_chunk)) {
        return -2;
      }
      s = sizeof (struct lev_filesys_xfs_file_chunk) + ((struct lev_filesys_xfs_file_chunk *) E)->size + 1;
      if (size < s) { return -2; }
      unlink_binlogpos ();
      filesys_xfs_file_chunk ((struct lev_filesys_xfs_file_chunk *) E);
      return s;
    case LEV_FILESYS_XFS_FILE_REMOVE:
      if (size < sizeof (struct lev_filesys_rmdir)) {
        return -2;
      }
      s = sizeof (struct lev_filesys_rmdir) + ((struct lev_filesys_rmdir *) E)->dirpath_size;
      if (size < s) { return -2; }
      unlink_binlogpos ();
      filesys_xfs_file_remove ((struct lev_filesys_rmdir *) E);
      return s;
    case LEV_FILESYS_XFS_CHANGE_ATTRS:
      if (size < sizeof (struct lev_filesys_xfs_change_attrs)) {
        return -2;
      }
      s = sizeof (struct lev_filesys_xfs_change_attrs) + ((struct lev_filesys_xfs_change_attrs *) E)->filename_size;
      if (size < s) { return -2; }
      unlink_binlogpos ();
      filesys_xfs_change_attrs ((struct lev_filesys_xfs_change_attrs *) E);
      return s;
  }

  kprintf ("unknown log event type %08x at position %lld\n", E->type, log_cur_pos ());

  return -3;
}

/************************************* index *****************************************************/
#define FILESYS_XFS_BINLOGPOS_MAGIC 0xd80ea5bf
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
  long long volume_id;
  unsigned int pending_operations;
  int transaction_id;
  int transaction_file_no;
  unsigned pending_operations_crc32;
  unsigned curfile_crc32;

  unsigned header_crc32; /* important: should be always last field of header */
} header_t;
header_t header;

long long last_snapshot_log_pos;
long long jump_log_pos;
int jump_log_ts;
unsigned jump_log_crc32;

static char *bread_str (void) {
  int i;
  bread (&i, 4);
  if (i == -1) {
    return NULL;
  }
  char *s = zmalloc (i+1);
  bread (s, i);
  s[i] = 0;
  int zero;
  int padded = ((i + 3) & -4) - i;
  bread (&zero, padded);
  return s;
}

static void writeout_str (char *s) {
  int i;
  if (s == NULL) {
    i = -1;
    writeout (&i, 4);
    return;
  }
  i = strlen (s);
  writeout (&i, 4);
  writeout (s, i);
  int zero = 0;
  int padded = ((i + 3) & -4) - i;
  writeout (&zero, padded);
}

static void compute_tmp_dir_name (void) {
  tmp_dir_length = work_dir_length + 1 + strlen (szTmpDirName);
  tmp_dir = zmalloc (tmp_dir_length + 1);
  assert (sprintf (tmp_dir, "%s/%s", work_dir, szTmpDirName) == tmp_dir_length);
}

static void compute_binlogpos_filename (void) {
  int binlogpos_filename_length = work_dir_length + 1 + strlen (szBinlogpos);
  binlogpos_filename = zmalloc (binlogpos_filename_length + 1);
  assert (sprintf (binlogpos_filename, "%s/%s", work_dir, szBinlogpos) == binlogpos_filename_length);
}

static int load_binlogpos (int called_from_load_index) {
  replay_logevent = filesys_xfs_replay_logevent;
  idx_fd = open (binlogpos_filename, O_RDONLY);
  if (idx_fd < 0) {
    kprintf ("couldn't open %s\n", binlogpos_filename);
    exit (1);
  }

  clearin ();
  bread (&header, sizeof (header));

  if (header.magic != FILESYS_XFS_BINLOGPOS_MAGIC) {
    kprintf ("index file is not for filesys-xfs-engine\n");
    return -1;
  }

  if (header.header_crc32 != compute_crc32 (&header, offsetof (header_t, header_crc32))) {
    kprintf ("CRC32 fail.\n");
    return -1;
  }

  jump_log_pos = header.log_pos1;
  if (called_from_load_index) {
    last_snapshot_log_pos = header.log_pos1;
  }

  jump_log_crc32 = header.log_pos1_crc32;
  jump_log_ts = header.log_timestamp;

  volume_id = header.volume_id;
  transaction_id = header.transaction_id;
  transaction_file_no = header.transaction_file_no;

  idx_crc32_complement = -1;
  int i;
  pending_operations_reset (1);
  vkprintf (3, "header.pending_operations = %d\n", header.pending_operations);
  for (i = 0; i < header.pending_operations; i++) {
    struct pending_operation *P = zmalloc (sizeof (struct pending_operation));
    int tp;
    bread (&tp, 4);
    P->type = tp;
    P->oldpath = bread_str ();
    P->newpath = bread_str ();
    bread (&P->st, sizeof (P->st));
    pending_operation_push (P);
  }
  if (~idx_crc32_complement != header.pending_operations_crc32) {
    kprintf ("pending_operations_crc32 didn't matched.\n");
    return -2;
  }
  idx_crc32_complement = -1;

  int data_offset = offsetof (curfile_t, data);
  bread (&curfile, data_offset);
  vkprintf (3, "curfile.E.parts = %d\n", curfile.E.parts);
  if (curfile.E.parts) {
    curfile_data_alloc (curfile.E.parts);
    unsigned t = curfile.chunk;
    t <<= 16;
    bread (curfile.data, t);
  } else {
    curfile.data = NULL;
  }

  if (~idx_crc32_complement != header.curfile_crc32) {
    kprintf ("curfile_crc32 didn't matched.\n");
    return -3;
  }

  close (idx_fd);
  binlogpos_pos = header.log_pos1;
  return 0;
}

static int load_index (kfs_file_handle_t Index) {
  if (Index == NULL) {
    return 0;
  }
  int r = tar_unpack (Index->fd, work_dir);
  if (r < 0) {
    kprintf ("tar_unpack (%s, %s) returns error code %d.\n", Index->info->filename, work_dir, r);
    exit (1);
  }
  r = load_binlogpos (1);
  if (r < 0) {
    kprintf ("load_binlogpos () returns error code %d.\n", r);
    exit (1);
  }
  return 0;
}

static int save_binlogpos (void) {
  memset (&header, 0, sizeof (header));
  vkprintf (1, "saving .binlogpos (log_cur_pos = %lld).\n", log_cur_pos ());
  if (log_cur_pos () == binlogpos_pos) {
    vkprintf (1, ".binlogpos for %lld log_pos exists. Skipping ...\n", binlogpos_pos);
    return 0;
  }
  newidx_fd = open (binlogpos_filename, O_CREAT | O_TRUNC | O_WRONLY | O_EXCL, 0660);

  if (newidx_fd < 0) {
    kprintf ("cannot create %s: %m\n", binlogpos_filename);
    exit (1);
  }

  lseek (newidx_fd, sizeof (header), SEEK_SET);
  clearin ();
  struct pending_operation *P;
  for (P = pol_head; P != NULL; P = P->next) {
    int tp = P->type;
    writeout (&tp, 4);
    writeout_str (P->oldpath);
    writeout_str (P->newpath);
    writeout (&P->st, sizeof (P->st));
    header.pending_operations++;
  }
  vkprintf (3, "header.pending_operations = %d\n", header.pending_operations);
  header.pending_operations_crc32 = ~idx_crc32_complement;
  flushout ();
  clearin ();
  vkprintf (4, "curfile.E.parts = %d\n", curfile.E.parts);
  vkprintf (4, "curfile.name = %s\n", curfile.name);
  int data_offset = offsetof (curfile_t, data);
  writeout (&curfile, data_offset);
  if (curfile.E.parts) {
    writeout (curfile.data, ((unsigned) curfile.chunk) << 16);
  }
  header.curfile_crc32 = ~idx_crc32_complement;
  flushout ();

  header.magic = FILESYS_XFS_BINLOGPOS_MAGIC;
  header.created_at = time (NULL);
  header.log_pos1 = log_cur_pos ();
  header.log_timestamp = log_read_until;
  relax_log_crc32 (0);
  header.log_pos1_crc32 = ~log_crc32_complement;
  header.volume_id = volume_id;
  header.transaction_id = transaction_id;
  header.transaction_file_no = transaction_file_no;
  header.header_crc32 = compute_crc32 (&header, offsetof (header_t, header_crc32));

  lseek (newidx_fd, 0, SEEK_SET);
  assert (write (newidx_fd, &header, sizeof (header)) == sizeof (header));

  assert (fsync (newidx_fd) >= 0);
  assert (close (newidx_fd) >= 0);
  binlogpos_pos = header.log_pos1;
  return 0;
}

int save_index (void) {
  char *newidxname = NULL;

  if (log_cur_pos () == last_snapshot_log_pos) {
    kprintf ("skipping generation of new snapshot (snapshot for this position already exists)\n");
    return 0;
  }

  if (engine_snapshot_replica) {
    newidxname = get_new_snapshot_name (engine_snapshot_replica, log_cur_pos(), engine_snapshot_replica->replica_prefix);
  }

  if (!newidxname || newidxname[0] == '-') {
    kprintf ("cannot write index: cannot compute its name\n");
    exit (1);
  }
  int r = save_binlogpos ();
  if (r < 0) {
    kprintf ("save_binlogpos returns error code %d.\n", r);
    exit (1);
  }
  r = tar_pack (newidxname, work_dir, compression_level);
  if (r < 0) {
    kprintf ("tar_pack (%s, %s, %d) return error code %d.\n", newidxname, work_dir, compression_level, r);
    exit (1);
  }

  if (rename_temporary_snapshot (newidxname)) {
    kprintf ("cannot rename new index file from %s: %m\n", newidxname);
    unlink (newidxname);
    exit (1);
  }

  print_snapshot_name (newidxname);

  last_snapshot_log_pos = log_cur_pos (); /* guard: don't save index twice for same_log_pos */
  return 0;
}

/*
 *
 *		MAIN
 *
 */

void usage (void) {
  fprintf (stderr, "%s\n", FullVersionStr);
  fprintf (stderr,
           "./filesys-xfs-engine [-v] [-i] [-x] [-O] [-p<port>] [-u<username>] [-b<backlog>] [-c<max-conn>] [-a<binlog-name>] [-l<log-name>] <extract-dir> <binlog>\n"
                   "\t-v\tincrease verbosity level\n"
                   "\t-H<heap-size>\tsets maximal zmalloc heap size, e.g. 4g\n"
                   "\t-1\tcompress snapshot faster\n"
                   "\t-9\tcompress snapshot better (default)\n"
                   "\t-O\tchange <extract-dir> permissions to 0700 for index loading & log replaying\n"
                   "\t-i\textract sources, create index and exit\n"
                   "\t-x\textract sources and exit\n"
                   "\n"
                   "\t\tOn startup, extract-dir should be empty or contain .binlogpos file.\n"
                   "\t\t.binlogpos is unlinked just before processing any filesys-xfs logevent.\n"
                   "\t\t.binlogpos file is created after exit (SIGTERM, SIGINT), after end of transaction (case no other logevents) or before creating snapshot.\n");
  exit (2);
}

static int test_dir_exist_and_empty (const char *const path, struct stat *b) {
  if (stat (path, b) || !S_ISDIR (b->st_mode)) {
    kprintf ("Directory %s doesn't exist\n", path);
    exit (1);
  }

  file_t *px;
  int n = getdir (path, &px, 0, 1);
  free_filelist (px);
  if (n < 0) {
    kprintf ("getdir (%s) returns error code %d.\n", path, n);
    exit (1);
  }
  if (n > 0) {
    vkprintf (1, "%s isn't empty directory!\n", path);
    return 0;
  }
  return 1;
}

int index_mode, sfd;

struct in_addr settings_addr;
int port = TCP_PORT;

void reopen_logs (void) {
  int fd;
  fflush (stdout);
  fflush (stderr);
  if ((fd = open ("/dev/null", O_RDWR, 0)) != -1) {
    dup2(fd, 0);
    dup2(fd, 1);
    dup2(fd, 2);
    if (fd > 2) {
      close(fd);
    }
  }
  if (logname && (fd = open (logname, O_WRONLY|O_APPEND|O_CREAT, 0640)) != -1) {
    dup2(fd, 1);
    dup2(fd, 2);
    if (fd > 2) {
      close (fd);
    }
  }
  vkprintf (1, "logs reopened.\n");
}

volatile int sigusr1_cnt = 0;

static void sigint_immediate_handler (const int sig) {
  static const char message[] = "SIGINT handled immediately.\n";
  kwrite (2, message, sizeof (message) - (size_t)1);
  exit (1);
}

static void sigterm_immediate_handler (const int sig) {
  static const char message[] = "SIGTERM handled immediately.\n";
  kwrite (2, message, sizeof (message) - (size_t)1);
  exit (1);
}

static void sigint_handler (const int sig) {
  static const char message[] = "SIGINT handled.\n";
  kwrite (2, message, sizeof (message) - (size_t)1);
  pending_signals |= 1 << sig;
  signal (sig, sigint_immediate_handler);
}

static void sigterm_handler (const int sig) {
  static const char message[] = "SIGTERM handled.\n";
  kwrite (2, message, sizeof (message) - (size_t)1);
  pending_signals |= 1 << sig;
  signal (sig, sigterm_immediate_handler);
}

static void sighup_handler (const int sig) {
  static const char message[] = "got SIGHUP.\n";
  kwrite (2, message, sizeof (message) - (size_t)1);
  signal (SIGHUP, sighup_handler);
}

static void sigusr1_handler (const int sig) {
  static const char message[] = "got SIGUSR1.\n";
  kwrite (2, message, sizeof (message) - (size_t)1);
  sigusr1_cnt++;
  signal (SIGUSR1, sigusr1_handler);
}

void sigabrt_handler (const int sig) {
  static const char message[] = "SIGABRT caught, terminating program\n";
  kwrite (2, message, sizeof (message) - (size_t)1);
  print_backtrace ();
  unlock_pid_file ();
  exit (EXIT_FAILURE);
}

volatile int force_write_index = 0;

static void sigrtmax_handler (const int sig) {
  static const char message[] = "got SIGUSR3, write index.\n";
  kwrite (2, message, sizeof (message) - (size_t)1);
  force_write_index = 1;
}

void cron (void) {
}

void start_server (void) {
  int i;
  int prev_time, old_sigusr1_cnt = 0;

  if (!sfd) {
    sfd = server_socket (port, settings_addr, backlog, 0);
  }

  if (sfd < 0) {
    kprintf ("cannot open server socket at port %d: %m\n", port);
    exit (3);
  }

  init_epoll ();
  init_netbuffers ();

  prev_time = 0;

  if (daemonize) {
    setsid();
  }

  if (change_user (username) < 0) {
    kprintf ("fatal: cannot change user to %s\n", username ? username : "(none)");
    exit (1);
  }

  if (binlogname && !binlog_disabled) {
    assert (append_to_binlog (Binlog) == log_readto_pos);
  }

  init_listening_connection (sfd, &ct_filesys_xfs_engine_server, &memcache_methods);

  if (binlog_disabled && binlog_fd >= 0) {
    epoll_pre_event = read_new_events;
  }

  signal (SIGABRT, sigabrt_handler);//unlink pid file
  signal (SIGINT, sigint_handler);
  signal (SIGTERM, sigterm_handler);
  signal (SIGUSR1, sigusr1_handler);
  signal (SIGPIPE, SIG_IGN);
  signal (SIGIO, SIG_IGN);
  signal (SIGRTMAX, sigrtmax_handler);

  if (daemonize) {
    signal (SIGHUP, sighup_handler);
  }

  int quit_steps = 0;

  for (i = 0; !pending_signals; i++) {
    if (!(i & 1023)) {
      vkprintf (1, "epoll_work(): %d out of %d connections, network buffers: %d used, %d out of %d allocated\n",
	       active_connections, maxconn, NB_used, NB_alloc, NB_max);
    }
    epoll_work (77);

    if (old_sigusr1_cnt != sigusr1_cnt) {
      old_sigusr1_cnt = sigusr1_cnt;
      vkprintf (1, "start_server: sigusr1_cnt = %d.\n", old_sigusr1_cnt);
      reopen_logs ();
    }

    if (log_cur_pos () == pending_saving_binlogpos_logpos) {
      save_binlogpos ();
      pending_saving_binlogpos_logpos = 0;
    }

    if (now != prev_time) {
      prev_time = now;
      cron ();
    }

    if (epoll_pre_event) {
      epoll_pre_event ();
    }

    if (force_write_index) {
      save_index ();
      force_write_index = 0;
    }

    if (quit_steps && !--quit_steps) break;
  }

  save_binlogpos ();

  epoll_close (sfd);
  close (sfd);
  kprintf ("Terminated (pending_signals = 0x%llx).\n", pending_signals);
}

void restore_work_dir_perms (int change_extract_dir_perms_during_replay_log, mode_t old_mode) {
  if (change_extract_dir_perms_during_replay_log) {
    if (chmod (work_dir, old_mode) < 0) {
      kprintf ("chmod 0%o \"%s\" fail. %m\n", (int) old_mode, work_dir);
    }
  }
}

int main (int argc, char *argv[]) {
  long long x;
  int i;
  int change_extract_dir_perms_during_replay_log = 0;
  char c;
  binlog_disabled = 1;
  set_debug_handlers ();
  progname = argv[0];
  while ((i = getopt (argc, argv, "H:Oa:b:c:el:p:dhu:vi0123456789")) != -1) {
    switch (i) {
    case '0'...'9':
      compression_level = i - '0';
      break;
    case 'H':
      c = 0;
      assert (sscanf (optarg, "%lld%c", &x, &c) >= 1);
      switch (c | 0x20) {
        case 'k':  x <<= 10; break;
        case 'm':  x <<= 20; break;
        case 'g':  x <<= 30; break;
        case 't':  x <<= 40; break;
        default: assert (c == 0x20);
      }
      if (x >= (1LL << 20) && x <= (sizeof(long) == 4 ? (3LL << 30) : (20LL << 30))) {
        dynamic_data_buffer_size = x;
      }
      break;
    case 'O':
      change_extract_dir_perms_during_replay_log = 1;
      break;
    case 'a':
      binlogname = optarg;
      break;
    case 'b':
      backlog = atoi(optarg);
      if (backlog <= 0) backlog = BACKLOG;
      break;
    case 'c':
      maxconn = atoi (optarg);
      if (maxconn <= 0 || maxconn > MAX_CONNECTIONS) {
        maxconn = MAX_CONNECTIONS;
      }
      break;
    case 'd':
      daemonize ^= 1;
      break;
    case 'h':
      usage ();
      return 2;
    case 'i':
      index_mode = 1;
      break;
    case 'l':
      logname = optarg;
      break;
    case 'p':
      port = atoi (optarg);
      break;
    case 'u':
      username = optarg;
      break;
    case 'v':
      verbosity++;
      break;
    case 'x':
      index_mode = 2;
      break;
    }
  }

  if (optind + 2 > argc) {
    usage ();
    exit (2);
  }

  if (!index_mode && strlen (argv[0]) >= 5 && memcmp ( argv[0] + strlen (argv[0]) - 5, "index" , 5) == 0) {
    index_mode = 1;
  }

  if (raise_file_rlimit (maxconn + 16) < 0) {
    kprintf ("fatal: cannot raise open file limit to %d\n", maxconn+16);
    exit (1);
  }

  if (!index_mode && port < PRIVILEGED_TCP_PORTS) {
    sfd = server_socket (port, settings_addr, backlog, 0);
    if (sfd < 0) {
      kprintf ("cannot open server socket at port %d: %m\n", port);
      exit (1);
    }
  }

  init_dyn_data ();
  aes_load_pwd_file (NULL);

  if (change_user (username) < 0) {
    kprintf ("fatal: cannot change user to %s\n", username ? username : "(none)");
    exit (1);
  }

  char *p = argv[optind];
  i = strlen (p);
  if (i == 0) {
    p = ".";
  } else if (p[i-1] == '/') {
    p[i - 1] = 0;
  }

  struct stat st;
  int work_dir_is_empty = test_dir_exist_and_empty (p, &st);

  work_dir_length = strlen (p);
  assert (work_dir_length < PATH_MAX);
  work_dir = zmalloc (work_dir_length + 1);
  strcpy (work_dir, p);

  if (lock_pid_file (work_dir) < 0) {
    exit (1);
  }
  atexit (unlock_pid_file);

  compute_tmp_dir_name ();
  compute_binlogpos_filename ();
  pending_operations_init (tmp_dir, work_dir);

  optind++;
  const mode_t work_dir_modified_mode = st.st_mode & ~(S_IRWXG | S_IRWXO);
  if (work_dir_modified_mode == st.st_mode) {
    change_extract_dir_perms_during_replay_log = 0;
  }

  if (change_extract_dir_perms_during_replay_log) {
    if (chmod (work_dir, work_dir_modified_mode) < 0) {
      kprintf ("chmod 0%o \"%s\" fail. %m\n", work_dir_modified_mode, work_dir);
      change_extract_dir_perms_during_replay_log = 0;
    }
  }

  if (engine_preload_filelist (argv[optind], binlogname) < 0) {
    kprintf ("cannot open binlog files for %s\n", binlogname ? binlogname : argv[optind]);
    exit (1);
  }

  //Snapshot reading
  Snapshot = open_recent_snapshot (engine_snapshot_replica);

  if (Snapshot) {
    engine_snapshot_name = Snapshot->info->filename;
    engine_snapshot_size = Snapshot->info->file_size;
    vkprintf (1, "load index file %s (size %lld)\n", engine_snapshot_name, engine_snapshot_size);
  } else {
    engine_snapshot_name = NULL;
    engine_snapshot_size = 0;
  }

  if (work_dir_is_empty) {
    index_load_time = -mytime();
    i = load_index (Snapshot);
    index_load_time += mytime();
    if (i < 0) {
      kprintf ("fatal: error %d while loading index file %s\n", i, engine_snapshot_name);
      restore_work_dir_perms (change_extract_dir_perms_during_replay_log, st.st_mode);
      exit (1);
    }
    vkprintf (1, "load index: done, jump_log_pos=%lld, time %.06lfs\n", jump_log_pos, index_load_time);
  } else {
    double t = -mytime();
    i = load_binlogpos (0);
    t += mytime();
    if (i < 0) {
      kprintf ("load_binlogpos () returns error code %d.\n", i);
      restore_work_dir_perms (change_extract_dir_perms_during_replay_log, st.st_mode);
      exit (1);
    }
    vkprintf (1, "load_binlogpos: done, jump_log_pos=%lld, time %.06lfs\n", jump_log_pos, t);
  }

  //Binlog reading
  Binlog = open_binlog (engine_replica, jump_log_pos);
  if (!Binlog) {
    kprintf ("fatal: cannot find binlog for %s, log position %lld\n", engine_replica->replica_prefix, jump_log_pos);
    restore_work_dir_perms (change_extract_dir_perms_during_replay_log, st.st_mode);
    exit (1);
  }
  binlogname = Binlog->info->filename;

  vkprintf (1, "replaying binlog file %s (size %lld) from position %lld\n", binlogname, Binlog->info->file_size, jump_log_pos);

  binlog_load_time = -mytime();

  clear_log ();

  init_log_data (jump_log_pos, jump_log_ts, jump_log_crc32);

  vkprintf (1, "replay log events started\n");

  i = replay_log (0, 1);
  vkprintf (1, "replay log events finished\n");
  binlog_load_time += mytime();
  binlog_loaded_size = log_readto_pos - jump_log_pos;

  restore_work_dir_perms (change_extract_dir_perms_during_replay_log, st.st_mode);
  if (i < 0) {
    kprintf ("fatal: error reading binlog, replay_log returns %d.\n", i);
    exit (1);
  }

  if (!binlog_disabled) {
    clear_read_log ();
  }

  clear_write_log ();

  if (index_mode) {
    if (index_mode == 1) {
      save_index ();
    }
  } else {
    start_time = time (NULL);
    start_transaction_id = transaction_id;
    start_server ();
  }

  return 0;
}

