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

#ifndef __FILESYS_MOUNT_H__
#define __FILESYS_MOUNT_H__

#include <fuse.h>
#include <pthread.h>

#define	VERSION "0.0.00"
#define	VERSION_STR "filesys " VERSION

extern pthread_t server_thread;
extern int ff_start_server, ff_stop_server, ff_sfd;

extern int ff_readonly;

int ff_access (const char *path, int mode);
int ff_chmod (const char *path, mode_t mode);
int ff_chown (const char *path, uid_t uid, gid_t gid);
int ff_create (const char *path, mode_t mode, struct fuse_file_info *fi);
int ff_getattr (const char *path, struct stat *stbuf);
void *ff_init (struct fuse_conn_info *conn);
int ff_mkdir (const char *path, mode_t mode);
int ff_open (const char *path, struct fuse_file_info *fi);
int ff_read (const char *path, char *buf, size_t size, off_t offset, struct fuse_file_info *fi);
int ff_readdir (const char *path, void *buf, fuse_fill_dir_t filler, off_t offset, struct fuse_file_info *fi);
int ff_readlink (const char *path, char *buf, size_t size);
int ff_rename (const char *from, const char *to);
int ff_rmdir (const char *path);
int ff_unlink (const char *path);
int ff_utimens (const char *path, const struct timespec tv[2]);
int ff_write (const char *path, const char *buf, size_t size, off_t offset, struct fuse_file_info *fi);

int ff_main (int argc, char **argv);

void ff_lock_all (void);
void ff_unlock_all (void);
#endif
