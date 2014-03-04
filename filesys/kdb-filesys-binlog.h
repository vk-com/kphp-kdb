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

#ifndef __KDB_FILESYS_BINLOG_H__
#define __KDB_FILESYS_BINLOG_H__

#include "kdb-binlog-common.h"

#pragma	pack(push,4)

#ifndef	FILESYS_SCHEMA_BASE
#define	FILESYS_SCHEMA_BASE	0x723d0000
#endif

#define	FILESYS_SCHEMA_V1	0x723d0101

#define LEV_FILESYS_CREATE 0x6185dd6f
#define LEV_FILESYS_WRITE 0x79c38046
#define LEV_FILESYS_MKDIR 0x29e0c53e
#define LEV_FILESYS_RMDIR 0x7f39bbbc
#define LEV_FILESYS_UNLINK 0xa6955619
#define LEV_FILESYS_RENAME 0x2e79f02d
#define LEV_FILESYS_CHMOD 0x6cd4a9f0
#define LEV_FILESYS_CHOWN 0x5cc02a0f
#define LEV_FILESYS_SET_MTIME 0xe349002b
#define LEV_FILESYS_LINK 0x66628fb9
#define LEV_FILESYS_FTRUNCATE 0xa1f08bf8
#define LEV_FILESYS_SYMLINK 0xa82270b6

typedef long long inode_id_t;

struct lev_filesys_create {
  lev_type_t type;
  int mode;
  unsigned short uid;
  unsigned short gid;
  unsigned short filename_size;
  char filename[0];
};

struct lev_filesys_write {
  lev_type_t type;
  inode_id_t inode;
  int offset;
  int data_size;
  char data[0];
};

/* also for link */
struct lev_filesys_rename {
  lev_type_t type;
  unsigned short src_filename_size;
  unsigned short dst_filename_size;
  char data[0];
};

struct lev_filesys_mkdir {
  lev_type_t type;
  int mode;
  unsigned short uid;
  unsigned short gid;
  unsigned short dirpath_size;
  char dirpath[0];
};

/* also for rmdir, unlink */
struct lev_filesys_rmdir {
  lev_type_t type;
  unsigned short dirpath_size;
  char dirpath[0];
};

/* also for set_mtime */
struct lev_filesys_chmod {
  lev_type_t type;
  int mode;
  unsigned short path_size;
  char path[0];
};

struct lev_filesys_ftruncate {
  lev_type_t type;
  inode_id_t inode;
  unsigned int size;
};


struct lev_filesys_chown {
  lev_type_t type;
  unsigned short uid;
  unsigned short gid;
  unsigned short path_size;
  char path[0];
};

/************************ XFS mode logevents *********************************/
#define LEV_FILESYS_XFS_BEGIN_TRANSACTION 0x620e0be9
#define LEV_FILESYS_XFS_END_TRANSACTION 0x4b1c46e6
#define LEV_FILESYS_XFS_FILE 0x7f2d8900
#define LEV_FILESYS_XFS_FILE_CHUNK 0xe357908a
#define LEV_FILESYS_XFS_FILE_REMOVE 0x4dd93348
#define LEV_FILESYS_XFS_CHANGE_ATTRS 0x2d2f194e

#define XFS_FILE_FLAG_GZIP 1
#define XFS_FILE_FLAG_DIFF 2
#define XFS_FILE_FLAG_SAME 4

struct lev_filesys_xfs_transaction {
  lev_type_t type;
  int id;
};

struct lev_filesys_xfs_file {
  lev_type_t type;
  int old_size;
  int patch_size;
  int new_size;
  int mode;
  unsigned actime;   /* access time */
  unsigned modtime;  /* modification time */
  unsigned old_crc32;
  unsigned patch_crc32;
  unsigned new_crc32;
  unsigned short uid;
  unsigned short gid;
  unsigned short filename_size;
  unsigned short parts;
  char filename[0];
};

struct lev_filesys_xfs_file_chunk {
  lev_type_t type;
  unsigned short part;
  unsigned short size;
  unsigned char data[0];
};

struct lev_filesys_xfs_change_attrs {
  lev_type_t type;
  int mode;
  unsigned actime;   /* access time */
  unsigned modtime;  /* modification time */
  unsigned short uid;
  unsigned short gid;
  unsigned short filename_size;
  char filename[0];
};

#pragma	pack(pop)

#endif

