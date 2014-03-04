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

    Copyright 2009-2013 Vkontakte Ltd
              2009-2012 Nikolai Durov
              2009-2012 Andrei Lopatin
              2012-2013 Anton Maydell
*/

#ifndef __KFS_H__
#define	__KFS_H__

#include "kfs-layout.h"
#include "crypto/aesni256.h"

#define	MAX_KFS_DATADIRS	16
#define	MAX_KFS_OPEN_FILES	64
#define	MAX_KFS_FNAME		256

struct kfs_data_directory {
  int state;
  int device;
  int files;
  int x;
  long long tot_fsize;
  char dirname[MAX_KFS_FNAME];
};

typedef struct kfs_replica *kfs_replica_handle_t;
typedef struct kfs_file *kfs_file_handle_t;

struct kfs_replica {
  kfs_hash_t replica_id_hash;
  char *replica_prefix;
  int replica_prefix_len;
  int binlog_num;
  struct kfs_file_info **binlogs;
  int snapshot_num;
  struct kfs_file_info **snapshots;
  vk_aes_ctx_t *ctx_crypto;
};

struct kfs_file_info {
  struct kfs_replica *replica;
  int refcnt;
  int kfs_file_type;
  char *filename;
  int filename_len;
  int flags;			// +8 = symlink, +1 = snapshot, +2 = temp, +4 = not_first, +16 = zipped
  char *suffix;			// points to ".123456.bin" in "data3/messages006.123456.bin"
  kfs_hash_t file_hash;
  long long file_size;		// -1 = unknown; may change afterwards
  long long log_pos;		// -1 = unknown
  long long min_log_pos;
  long long max_log_pos;
  struct kfs_file_header *khdr;
  char *start;			// pointer to first preloaded_bytes of this file
  unsigned char *iv;
  int preloaded_bytes;
  int kfs_headers;
  int mtime;
  int inode;
  int device;
};

struct kfs_file {
  struct kfs_file_info *info;
  int fd;			// -1 = not open
  int lock;			// 0 = unlocked, >=1 = read lock(s), -1 = write lock
  long long offset;
};

typedef struct kfs_snapshot_write_stream {
  kfs_replica_handle_t R;
  char *newidxname;
  unsigned char *iv;
  int newidx_fd;
} kfs_snapshot_write_stream_t;

int lock_whole_file (int fd, int mode);

kfs_replica_handle_t open_replica (const char *replica_name, int force);
int close_replica (kfs_replica_handle_t R);
int update_replica (kfs_replica_handle_t R, int force);

int kfs_close_file (kfs_file_handle_t F, int close_handle);

kfs_file_handle_t open_binlog (kfs_replica_handle_t Replica, long long log_pos);
kfs_file_handle_t next_binlog (kfs_file_handle_t log_handle);
kfs_file_handle_t create_next_binlog (kfs_file_handle_t log_handle, long long start_log_pos, kfs_hash_t new_file_hash);
kfs_file_handle_t create_next_binlog_ext (kfs_file_handle_t F, long long start_log_pos, kfs_hash_t new_file_hash, int allow_read, int zipped);
kfs_file_handle_t create_first_binlog (kfs_replica_handle_t R, char *start_data, int start_size, int strict_naming, int allow_read, int zipped);

long long append_to_binlog (kfs_file_handle_t log_handle);
long long append_to_binlog_ext (kfs_file_handle_t log_handle, int allow_read);
int binlog_is_last (kfs_file_handle_t F);
int close_binlog (kfs_file_handle_t log_handle, int close_handle);

long long get_binlog_start_pos (kfs_replica_handle_t R, int binlog_id, long long *end_pos);

kfs_file_handle_t open_snapshot (kfs_replica_handle_t R, int snapshot_index);  // index must be in [0, R->snapshot_num)
kfs_file_handle_t open_recent_snapshot (kfs_replica_handle_t Replica);  // file position is after kfs headers
kfs_file_handle_t create_new_snapshot (kfs_replica_handle_t Replica, long long log_pos);
char *get_new_snapshot_name (kfs_replica_handle_t R, long long log_pos, const char *replica_prefix);
int close_snapshot (kfs_file_handle_t log_handle, int close_handle);
int rename_temporary_snapshot (const char *name);
int print_snapshot_name (const char *name);

extern char *engine_replica_name, *engine_snapshot_replica_name;
extern kfs_replica_handle_t engine_replica, engine_snapshot_replica;
extern kfs_file_handle_t Snapshot, Binlog;
extern char *engine_snapshot_name;
extern long long engine_snapshot_size;

int engine_preload_filelist (const char *main_replica_name, const char *aux_replica_name);

int preload_file_info (struct kfs_file_info *FI);
/* for unpack bin.bz files */
kfs_file_handle_t kfs_open_file (const char *filename, int cut_backup_suffix);
kfs_binlog_zip_header_t *load_binlog_zip_header (struct kfs_file_info *FI);
int kfs_bz_get_chunks_no (long long orig_file_size);
int kfs_bz_compute_header_size (long long orig_file_size);
int kfs_bz_decode (kfs_file_handle_t F, long long off, void *dst, int *dest_len, int *disk_bytes_read);
int kfs_get_tag (unsigned char *start, int size, unsigned char tag[16]);
int kfs_file_compute_initialization_vector (struct kfs_file_info *FI);

/* SWS (snapshot write stream) functions */
/* exit(1) on failure, returns 1 on success, 0 if snapshot was already created for given position */
int kfs_sws_open (kfs_snapshot_write_stream_t *S, kfs_replica_handle_t R, long long log_pos, long long jump_log_pos);
void kfs_sws_close (kfs_snapshot_write_stream_t *S);
/* buff is corrupted in the encryption case after call of kfs_sws_write function */
void kfs_sws_write (kfs_snapshot_write_stream_t *S, void *buff, long long count);
void kfs_sws_safe_write (kfs_snapshot_write_stream_t *S, const void *buff, long long count);

/* for binlog/snapshot metafile decryption */
void kfs_buffer_crypt (kfs_file_handle_t F, void *buff, long long size, long long off);
/* for decryption snapshot in load_index functions */
long long kfs_read_file (kfs_file_handle_t F, void *buff, long long size);

#endif
