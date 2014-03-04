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

#ifndef __FILESYS_UTILS_H__
#define __FILESYS_UTILS_H__

#include <sys/stat.h>

#define TAR_PACK_ERR_TOO_LONG_FILENAME (-20)
#define TAR_PACK_ERR_WRITE_HEADER (-21)
#define TAR_PACK_ERR_FILL_HEADER (-22)
#define TAR_PACK_ERR_GZWRITE (-23)
#define TAR_PACK_ERR_PATH_TOO_LONG (-24)
#define TAR_PACK_ERR_OPEN (-25)
#define TAR_PACK_ERR_DUP (-26)
#define TAR_UNPACK_ERR_OPEN (-30)
#define TAR_UNPACK_ERR_COPY_ATTRS (-31)
#define TAR_UNPACK_ERR_SYMLINK (-32)
#define TAR_UNPACK_ERR_MKDIR (-33)

typedef struct file {
  char *filename;
  struct file *next;
  struct stat st;
} file_t;

int getdir (const char *dirname, file_t **R, int sort, int hidden);
void free_filelist (file_t *p);
file_t *remove_file_from_list (file_t *x, const char *const filename);

int lcopy_attrs (char *filename, struct stat *S);
int delete_file (const char *path);
int clone_file (const char *const oldpath, const char *const newpath);
int get_file_content (char *dir, file_t *x, unsigned char **a, int *L);

int tar_pack (const char *const tar_filename, const char *const path, int compression_level);
int tar_unpack (int tar_gz_fd, const char *const path);

extern const char *const szPidFilename;
int lock_pid_file (const char *const dir);
void unlock_pid_file (void);

#endif
