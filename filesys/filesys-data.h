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

#ifndef __FILESYS_DATA_H__
#define __FILESYS_DATA_H__

#include "kdb-filesys-binlog.h"
#include "kfs.h"

extern long long index_size;
extern int jump_log_ts;
extern unsigned jump_log_crc32;
extern long long jump_log_pos, binlog_loaded_size;
extern double binlog_load_time, index_load_time;
extern long long volume_id;
extern int tot_inodes, tot_directory_nodes, tot_loaded_metafiles, alloc_tree_nodes;
extern long long tot_loaded_index_data, tot_allocated_data;
extern long long max_loaded_index_data, max_allocated_data;

typedef struct filesys_tree {
  struct filesys_tree *left, *right;
  char *data;
  unsigned int block_offset;
  unsigned int block_length;
  int y;
} filesys_tree_t;

struct filesys_directory_node {
  inode_id_t inode;
  char *name;
  int mode;
  int modification_time;
  unsigned short uid;
  unsigned short gid;
  struct filesys_directory_node *next;
  struct filesys_directory_node *head;
  struct filesys_directory_node *parent;
};

#define INODE_RECORD_SIZE 28

struct filesys_inode {
  inode_id_t inode;
  struct filesys_inode *hnext;
  struct filesys_inode *prev; /* loaded at memory inodes list */
  struct filesys_inode *next;
  filesys_tree_t *updates;
  char *index_data;
  int modification_time;
  unsigned int filesize;
  unsigned int index_filesize;
  int reference_count;
  long long index_offset;
};

enum filesys_lookup_file_type {
  lf_rmdir = -2,
  lf_unlink = -1,
  lf_find = 0,
  lf_creat = 1
};

void dump_all_files (void);

struct filesys_directory_node *filesys_lookup_file (const char *name, enum filesys_lookup_file_type force);
struct filesys_inode *get_inode_f (inode_id_t inode, int force);

void set_memory_limit (long max_memory);

int do_check_perm (struct filesys_directory_node *D, int mode);
int do_chmod (const char *path, int mode);
int do_chown (const char *path, unsigned short uid, unsigned short gid);
int do_creat (const char *path, int mode, unsigned short uid, unsigned short gid, inode_id_t *inode);
int do_inode_read (unsigned int offset, unsigned int size, inode_id_t inode, char *output);
int do_inode_truncate (inode_id_t inode, unsigned int size);
int do_inode_write (unsigned int offset, unsigned int size, inode_id_t inode, const char *input);
int do_link (const char *src_filename, const char *dest_filename);
int do_mkdir (const char *dirname, int mode, unsigned short uid, unsigned short gid);
int do_read (unsigned int offset, unsigned int size, const char *filename, char *output);
int do_rename (const char *src_filename, const char *dest_filename);
int do_rmdir (const char *dirname);
int do_set_mtime (const char *path, int modification_time);
int do_symlink (const char *src_filename, const char *dest_filename);
int do_unlink (const char *filename);
int do_write (unsigned int offset, unsigned int size, char *input, const char *filename);
int load_index (kfs_file_handle_t Index);
int save_index (int writing_binlog);
#endif
