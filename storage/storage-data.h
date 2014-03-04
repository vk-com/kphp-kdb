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

#ifndef __STORAGE_DATA_H__
#define __STORAGE_DATA_H__

#include "kfs.h"
#include "net-buffers.h"
#include "net-connections.h"
#include "storage-content.h"

#define MAX_VOLUME_BINLOGS 4
#define MAX_DIRS 36

#define STORAGE_SECRET_MASK 0xFF00000000000000ULL

#define	STORAGE_INDEX_MAGIC	0xf8e56fc5

#define STORAGE_ERR_WRONG_SECRET (-13)
#define STORAGE_ERR_USER_NOT_FOUND (-14)
#define STORAGE_ERR_ALBUM_NOT_FOUND (-15)
#define STORAGE_ERR_DOC_NOT_FOUND (-16)
#define STORAGE_ERR_DOC_EXISTS (-17)
#define STORAGE_ERR_DOC_DELETED (-18)
#define STORAGE_ERR_COULDN_OPEN_FILE (-19)
#define STORAGE_ERR_FSTAT (-20)
#define STORAGE_ERR_LSEEK (-21)
#define STORAGE_ERR_WRITE (-22)
#define STORAGE_ERR_READ (-23)
#define STORAGE_ERR_CHDIR (-24)
#define STORAGE_ERR_OPENDIR (-25)
#define STORAGE_ERR_STAT (-26)
#define STORAGE_ERR_UNKNOWN_TYPE (-27)
#define STORAGE_ERR_ILLEGAL_LOCAL_ID (-28)
#define STORAGE_ERR_UNKNOWN_VOLUME_ID (-29)
#define STORAGE_ERR_OPEN (-30)
#define STORAGE_ERR_WRITE_IN_READONLY_MODE (-31)
#define STORAGE_ERR_OUT_OF_MEMORY (-32)
#define STORAGE_ERR_NO_WRONLY_BINLOGS (-33)
#define STORAGE_ERR_PWRITE (-34)
#define STORAGE_ERR_PATH_TOO_LONG (-35)
#define STORAGE_ERR_TOO_MANY_BINLOGS (-36)
#define STORAGE_ERR_DIR_NOT_FOUND (-37)
#define STORAGE_ERR_SIZE_MISMATCH (-38)
#define STORAGE_ERR_TAIL_DIFFER (-39)
#define STORAGE_ERR_SCANDIR_MULTIPLE (-40)
#define STORAGE_ERR_NULL_POINTER_EXCEPTION (-41)
#define STORAGE_ERR_SERIALIZATION (-42)
#define STORAGE_ERR_NOT_ENOUGH_DATA (-43)
#define STORAGE_ERR_BINLOG_DISABLED (-44)
#define STORAGE_ERR_NOT_IMPLEMENTED (-45)
#define STORAGE_ERR_TIMEOUT (-46)
#define STORAGE_ERR_PERM (-47)
#define STORAGE_ERR_BIG_FILE (-48)
#define STORAGE_ERR_SYNC (-49)
#define STORAGE_ERR_CLOSE (-50)
#define STORAGE_ERR_RENAME (-51)
#define STORAGE_ERR_TOO_MANY_AIO_CONNECTIONS (-52)
#define STORAGE_ERR_DIFFER (-53)

#define STORAGE_ERR_XXX (-999)

#pragma	pack(push,4)

struct storage_index_header {
  int magic;
  int created_at;
  long long log_pos0;
  long long log_pos1;
  unsigned long long file_hash;
  unsigned long long slice_hash;
  int log_timestamp;
  int log_split_min, log_split_max, log_split_mod;
  unsigned log_pos0_crc32;
  unsigned log_pos1_crc32;

  long long volume_id;
  int docs;
  int md5_mode;
};

#pragma	pack(pop)

typedef struct storage_md5_tree_node {
  long long x[2];
  unsigned long long offset;
  struct storage_md5_tree_node *left, *right;
  int y;
} md5_tree_t;

typedef struct {
  long long success;
  long long fails;
  int sequential_fails;
  int last_fail_time;
} stat_read_t;

typedef struct {
  long long old_value;
  double counter;
} amortization_stat_t;

typedef struct {
  char *path;
  stat_read_t st_read;
  stat_read_t st_fsync;
  long long disk_total_bytes;
  long long disk_free_bytes;
  int pending_aio_connections;
  int last_statvfs_time;
  int binlogs;
  char scanned;
  char disabled;
} storage_dir_t;

typedef struct storage_binlog_file {
  long long volume_id;
  unsigned long long binlog_file_id; /* used in bad image cash */
  stat_read_t st_read;
  amortization_stat_t as_read;
  stat_read_t st_fsync;
  long long size;
  struct storage_binlog_file *fsync_next;
  char *abs_filename;
  int dir_id;
  int mtime;
  int fd_rdonly;
  int fd_wronly;
  int priority;
  int prefix;
  int dirty;
} storage_binlog_file_t;

typedef struct metafile {
  long long offset;
  struct aio_connection *aio;
  struct metafile *prev, *next, *hnext;
  storage_binlog_file_t *B;
  int local_id;
  int size;
  //int timeout;
  int refcnt;
  int corrupted:1;
  int cancelled:1;
  int crc32_error:1;
  int padded:29;
  unsigned char data[0];
} metafile_t;

typedef struct {
  long long size;
  char *filename;
  int mtime;
  int priority;
  int fd_wronly;
} binlog_file_info_t;

typedef struct storage_volume {
  long long cur_log_pos;
  long long log_readto_pos;
  long long jump_log_pos;
  long long snapshot_size;
  long long index_size;
  long long volume_id;
  double binlog_load_time, index_load_time;
  kfs_replica_handle_t engine_replica;
  kfs_replica_handle_t engine_snapshot_replica;
  kfs_file_handle_t Binlog;
  kfs_file_handle_t Snapshot;
  pthread_mutex_t mutex_write;
  pthread_mutex_t mutex_insert;
  unsigned long long *Idx_Pos;
  unsigned long long **Pos;
  unsigned char *Md5_Docs;
  unsigned long long *Md5_Pos;
  md5_tree_t *Md5_Root;
  storage_binlog_file_t *B[MAX_VOLUME_BINLOGS];
  unsigned jump_log_crc32;
  unsigned log_crc32;
  int disabled;
  int binlogs;
  int wronly_binlogs;
  int docs;
  int idx_docs;
  int pos_capacity;
  int log_split_min, log_split_max, log_split_mod;
  int md5_mode;
} volume_t;

#define MAX_VOLUMES 5000
extern volume_t **Volumes;
extern storage_dir_t Dirs[MAX_DIRS];

extern const int bad_image_cache_min_living_time;
extern int bad_image_cache_max_living_time;
extern long long idx_loaded_bytes;
extern int use_crc32_check, wronly_binlogs_closed;
extern int volumes, dirs;
extern int docs_bodies_in_snapshot, reverse_order_docs_bodies;
extern int alloc_tree_nodes;
extern long long tot_docs;
extern long long idx_users, idx_albums, idx_docs, snapshot_size, index_size;
extern int metafiles, metafiles_bytes, max_metafiles_bytes, max_aio_connections_per_disk;
extern long long tot_aio_loaded_bytes, metafiles_unloaded, metafiles_load_errors, metafiles_crc32_errors, metafiles_cancelled,
                 choose_reading_binlog_errors;
extern long long statvfs_calls;
extern conn_query_type_t aio_metafile_query_type;

void *tszmalloc (long size);
void *tszmalloc0 (long size);
void tszfree (void *ptr, long size);

void dirty_binlog_queue_push (storage_binlog_file_t *B);
storage_binlog_file_t *dirty_binlog_queue_pop (void);

volume_t *get_volume_f (unsigned long long volume_id, int force);
volume_t **generate_sorted_volumes_array (void);

int metafile_load (volume_t *V, metafile_t **R, storage_binlog_file_t **PB, long long volume_id, int local_id, int filesize, long long offset);
int do_get_doc (long long volume_id, int local_id, unsigned long long secret, volume_t **V, unsigned long long *offset,int *filesize);
int do_md5_get_doc (long long volume_id, unsigned char md5[16], unsigned long long secret, volume_t **V, unsigned long long *offset);
int do_copy_doc (volume_t *V, unsigned long long *secret, const char *const filename, const unsigned char *const data, int data_len, int content_type, unsigned char md5[16]);
int load_index (volume_t *V);
int save_index (volume_t *V);

int storage_le_start (volume_t *V, struct lev_start *E);
int storage_parse_choose_binlog_option (const char *s);
storage_binlog_file_t *choose_reading_binlog (volume_t *V, long long offset, long long offset_end, int forbidden_dirmask);
int storage_add_binlog (const char *binlogname, int dir_id);
void storage_reoder_binlog_files (volume_t *V);
void storage_open_replicas (void);
int make_empty_binlogs (int N, char *prefix, int md5_mode, int cs_id);
int storage_append_to_binlog (volume_t *V);
int storage_replay_log (volume_t *V, long long start_pos);
extern void (*storage_volumes_relax_astat) (void);
void update_binlog_read_stat (metafile_t *meta, int success);
void update_binlog_fsync_stat (storage_binlog_file_t *B, int success);
int get_volume_serialized (char *buffer, long long volume_id);
int get_volume_text (char *buffer, long long volume_id);
int get_dirs_serialized (char *buffer);
int storage_enable_binlog_file (volume_t *V, int dir_id);
int storage_close_binlog_file (volume_t *V, int dir_id);
int get_dir_id_by_name (const char *const dirname);
int storage_scan_dir (int dir_id);
int storage_volume_check_file (volume_t *V, double max_disk_usage, long long file_size);
//int change_dir_write_status (int dir_id, int disabled);
#endif
