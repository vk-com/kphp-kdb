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
#define FUSE_USE_VERSION 26

#include <fuse.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <signal.h>
#include <unistd.h>

#include "filesys-mount.h"
#include "net-events.h"
#include "net-connections.h"
#include "net-memcache-server.h"
#include "kdb-data-common.h"
#include "server-functions.h"
#include "net-crypto-aes.h"
#include "kfs.h"
#include "filesys-data.h"
#include "filesys-memcache.h"

pthread_t ff_server_thread;
pthread_mutex_t ff_mutex;
int ff_start_server = 1;
int ff_stop_server = 0;
int ff_readonly = 0;

#define FF_INIT { pthread_mutex_lock (&ff_mutex); }
#define FF_RETURN(x) { pthread_mutex_unlock (&ff_mutex); return (x); }

int ff_getattr (const char *path, struct stat *stbuf) {
  FF_INIT
  vkprintf (2, "ff_getattr (%s)\n", path);
  memset (stbuf, 0, sizeof (struct stat));
  struct filesys_directory_node *D = filesys_lookup_file (path, lf_find);
  if (D == NULL) { FF_RETURN(-ENOENT) }
  stbuf->st_mode = D->mode;
  if (ff_readonly) {
    stbuf->st_mode &= ~0222;
  }
  stbuf->st_uid = D->uid;
  stbuf->st_gid = D->gid;
  stbuf->st_nlink = 1;
  stbuf->st_mtime = D->modification_time;
  if (D->inode < 0) {
    assert (stbuf->st_mode & S_IFDIR);
  } else {
    struct filesys_inode *I = get_inode_f (D->inode, lf_find);
    assert (I != NULL);
    assert (I->inode == D->inode);
    stbuf->st_size = I->filesize;
    stbuf->st_blksize = 1;
    stbuf->st_blocks = stbuf->st_size;
    stbuf->st_nlink = I->reference_count;
    stbuf->st_ino = I->inode;
    if (stbuf->st_mtime < I->modification_time) {
      stbuf->st_mtime = I->modification_time;
    }
    vkprintf (2, "name = %s, inode = %lld, size = %d\n", D->name, I->inode, (int) stbuf->st_size);
  }

  FF_RETURN(0)
}

int ff_readdir (const char *path, void *buf, fuse_fill_dir_t filler, off_t offset, struct fuse_file_info *fi) {
  FF_INIT
  vkprintf (2, "ff_readdir (%s)\n", path);
  struct filesys_directory_node *D = filesys_lookup_file (path, lf_find), *p;
  if (D == NULL) { FF_RETURN(-ENOENT); }

  if (do_check_perm (D, 4)) {
    FF_RETURN(-EACCES)
  }

  filler (buf, ".", NULL, 0);
  filler (buf, "..", NULL, 0);
  for (p = D->head; p != NULL; p = p->next) {
    filler (buf, p->name, NULL, 0);
  }

  if (verbosity >= 3) {
    fprintf (stderr, "After readdir\n");
    dump_all_files ();
  }
  FF_RETURN(0);
}

/* ff: filesys + fuse */

int ff_open (const char *path, struct fuse_file_info *fi) {
  FF_INIT
  vkprintf (2, "ff_open (%s, fi->flags = %d)\n", path, fi->flags);
  struct filesys_directory_node *D = filesys_lookup_file (path, lf_find);
  if (D == NULL) { FF_RETURN(-ENOENT) }
  if (D->inode < 0) { FF_RETURN(-EISDIR) }
  int m = 0;
  switch (fi->flags & O_ACCMODE) {
    case O_RDONLY:
      m = 4;
      break;
    case O_WRONLY:
      if (ff_readonly) {
        FF_RETURN(-EACCES)
      }
      m = 2;
      break;
    case O_RDWR:
      if (ff_readonly) {
        FF_RETURN(-EACCES)
      }
      m = 6;
      break;
    default:
      assert (0);
  }

  if (do_check_perm (D, m)) {
    FF_RETURN(-EACCES)
  }

  fi->fh = D->inode;

  FF_RETURN(0);
}

int ff_unlink (const char *path) {
  FF_INIT
  vkprintf (2, "ff_unlink (%s)\n", path);

  if (ff_readonly) {
    FF_RETURN (-EACCES)
  }

  int r = do_unlink (path);
  FF_RETURN(r)
}

int ff_write (const char *path, const char *buf, size_t size, off_t offset, struct fuse_file_info *fi) {
  FF_INIT
  vkprintf (2, "ff_write (%s, size = %u, offset = %u, fi->fh = %lld)\n", path, (unsigned) size, (unsigned) offset, (long long)fi->fh);

  if (ff_readonly) {
    FF_RETURN (-EACCES)
  }

  int r = do_inode_write (offset, size, fi->fh, buf);
  FF_RETURN(r)
}

int ff_read (const char *path, char *buf, size_t size, off_t offset, struct fuse_file_info *fi) {
  FF_INIT
  vkprintf (2, "ff_read (%s, size = %d, offset = %d, fi->fh = %lld\n", path, (int) size, (int) offset, (long long) fi->fh);
  int r = do_inode_read (offset, size, fi->fh, buf);
  if (r < 0) {
    memset (buf, 0, size);
    FF_RETURN(0)
  }
  vkprintf (2, "ff_read returns %d\n", r);

  FF_RETURN(r)
}

int ff_create (const char *path, mode_t mode, struct fuse_file_info *fi) {
  FF_INIT
  vkprintf (2, "ff_create (%s, mode = %d)\n", path, mode);

  if (ff_readonly) {
    FF_RETURN (-EACCES)
  }

  inode_id_t inode;
  int r = do_creat (path, mode, getuid (), getgid (), &inode);
  fi->fh = inode;
  FF_RETURN(r)
}

int ff_mkdir (const char *path, mode_t mode) {
  FF_INIT
  vkprintf (2, "ff_mkdir (%s, mode = %d)\n", path, (int) mode);

  if (ff_readonly) {
    FF_RETURN (-EACCES)
  }

  int r = do_mkdir (path, mode, getuid (), getgid ());
  FF_RETURN(r)
}

int ff_rmdir (const char *path) {
  FF_INIT
  vkprintf (2, "ff_rmdir (%s)\n", path);

  if (ff_readonly) {
    FF_RETURN (-EACCES)
  }

  int r = do_rmdir (path);
  FF_RETURN(r)
}

int ff_rename (const char *from, const char *to) {
  FF_INIT
  vkprintf (2, "ff_rename (%s, %s)\n", from, to);

  if (ff_readonly) {
    FF_RETURN (-EACCES)
  }

  int r = do_rename (from, to);
  FF_RETURN(r)
}

int ff_link (const char *from, const char *to) {
  FF_INIT
  vkprintf (2, "ff_link (%s, %s)\n", from, to);

  if (ff_readonly) {
    FF_RETURN (-EACCES)
  }

  int r = do_link (from, to);
  FF_RETURN(r)
}

int ff_readlink (const char *path, char *buf, size_t size) {
  FF_INIT
  vkprintf (2, "ff_readlink (%s, size = %d)\n", path, (int) size);
  struct filesys_directory_node *D = filesys_lookup_file (path, lf_find);
  if (do_check_perm (D, 4)) {
    FF_RETURN(-EACCES);
  }
  if (D == NULL || !(D->mode & S_IFLNK)) {
    FF_RETURN(-ENOENT)
  }
  if (D->inode < 0) {
    FF_RETURN(-EISDIR)
  }
  int l = do_inode_read (0, size - 1, D->inode, buf);
  if (l < 0) {
    FF_RETURN(l)
  }
  buf[l] = 0;

  FF_RETURN(0)
}

int ff_symlink (const char *from, const char *to) {
  FF_INIT
  vkprintf (2, "ff_symlink (%s, %s)\n", from, to);

  if (ff_readonly) {
    FF_RETURN (-EACCES)
  }

  int r = do_symlink (from, to);
  FF_RETURN(r)
}

int ff_access (const char *path, int mode) {
  FF_INIT
  vkprintf (2, "ff_access (%s, mode = %d)\n", path, mode);
  struct filesys_directory_node *D = filesys_lookup_file (path, lf_find);
  if (D == NULL) { FF_RETURN(-ENOENT); }
  if (!geteuid ()) {
    FF_RETURN(0)
  }
  vkprintf (2, "ff_access: D->mode = %d\n", D->mode);
  if (ff_readonly && (mode & W_OK)) {
    FF_RETURN(-EACCES)
  }
  int o = (D->mode >> ((D->uid == geteuid ()) ? 6 : (D->gid == getegid ()) ? 3 : 0)) & 7;
  if ((o & mode) != mode) {
    FF_RETURN(-EACCES)
  }
  FF_RETURN(0)
}

int ff_chmod (const char *path, mode_t mode) {
  FF_INIT
  vkprintf (2, "ff_chmod (%s, mode = %d)\n", path, mode);

  if (ff_readonly) {
    FF_RETURN (-EACCES)
  }

  int r = do_chmod (path, mode);
  FF_RETURN(r)
}

int ff_chown (const char *path, uid_t uid, gid_t gid) {
  FF_INIT
  vkprintf (2, "ff_chown (%s, uid = %d, gid = %d)\n", path, uid, gid);

  if (ff_readonly) {
    FF_RETURN (-EACCES)
  }

  int r = do_chown (path, uid, gid);
  FF_RETURN(r)
}

void ff_destroy (void *arg) {
  pthread_mutex_lock (&ff_mutex);
  vkprintf (2, "ff_destroy ()\n");
  ff_stop_server = 1;
  pthread_mutex_unlock (&ff_mutex);
  pthread_join (ff_server_thread, NULL);
  pthread_mutex_destroy (&ff_mutex);
}

int ff_utimens (const char *path, const struct timespec tv[2]) {
  FF_INIT
  vkprintf (2, "ff_utimes (%s, %d, %d)\n", path, (int) tv[0].tv_sec, (int) tv[1].tv_sec);

  if (ff_readonly) {
    FF_RETURN (-EACCES)
  }

  int r = do_set_mtime (path, tv[1].tv_sec);

  FF_RETURN(r)
}

int ff_truncate (const char *path, off_t size) {
  FF_INIT
  vkprintf (2, "ff_truncate (%s, %u)\n", path, (unsigned) size);
  if (ff_readonly) {
    FF_RETURN (-EACCES)
  }

  struct filesys_directory_node *D = filesys_lookup_file (path, lf_find);
  if (D == NULL) {
    FF_RETURN(-ENOENT)
  }

  if (D->inode < 0) {
    FF_RETURN(-EISDIR)
  }

  if (do_check_perm (D, 2)) {
    FF_RETURN(-EACCES);
  }

  int r = do_inode_truncate (D->inode, size);

  FF_RETURN(r)
}

int ff_ftruncate (const char *path, off_t size, struct fuse_file_info *fi) {
  FF_INIT
  vkprintf (2, "ff_ftruncate (%s, %u, fi->fh = %lld)\n", path, (unsigned) size, (long long) fi->fh);
  if (ff_readonly) {
    FF_RETURN (-EACCES)
  }

  int r = do_inode_truncate (fi->fh, size);

  FF_RETURN(r)
}

void cron (void) {
  create_all_outbound_connections ();

  ff_lock_all ();
  flush_binlog ();
  ff_unlock_all ();
 /*
  if (force_write_index) {
    fork_write_index ();
  }
  check_child_status ();
*/
}
int ff_sfd;
static void *start_server (void *arg) {
  int i, prev_time = 0;

  init_epoll ();
  init_netbuffers ();
  init_listening_connection (ff_sfd, &ct_filesys_engine_server, &memcache_methods);

  for (i = 0; !ff_stop_server ; i++) {
    if (verbosity > 0 && !(i & 255)) {
      fprintf (stderr, "epoll_work(): %d out of %d connections, network buffers: %d used, %d out of %d allocated\n",
	       active_connections, maxconn, NB_used, NB_alloc, NB_max);
    }
    epoll_work (67);

    if (now != prev_time) {
      prev_time = now;
      cron ();
    }

    if (epoll_pre_event) {
      epoll_pre_event();
    }
  }

  epoll_close (ff_sfd);
  close (ff_sfd);
  pthread_exit (NULL);
}

/*
int ff_setxattr (const char *path, const char *key, const char *value, size_t size, int flags) {
  FF_INIT
  lprintf (2, "ff_setxattr(%s,%s,%s,%u,%d)\n", path, key, value, (unsigned) size, flags);
  FF_RETURN(0)
}
*/

int ff_flush (const char *path, struct fuse_file_info *fi) {
  return 0;
}

void *ff_init (struct fuse_conn_info *conn) {
  vkprintf (2, "ff_init\n");
  pthread_mutex_init (&ff_mutex, NULL);
  if (ff_start_server) {
    pthread_attr_t attr;
    pthread_attr_init (&attr);
    pthread_attr_setdetachstate (&attr, PTHREAD_CREATE_JOINABLE);
    pthread_attr_setstacksize (&attr, 4 << 20);
    pthread_create (&ff_server_thread, &attr, start_server, NULL);
    pthread_attr_destroy (&attr);
    ff_start_server = 0;
  }
  return NULL;
}

static struct fuse_operations ff_oper = {
  .access = ff_access,
  .chmod = ff_chmod,
  .chown = ff_chown,
  .create = ff_create,
  .destroy = ff_destroy,
  .ftruncate = ff_ftruncate,
  .getattr = ff_getattr,
  .init = ff_init,
  .link = ff_link,
  .mkdir = ff_mkdir,
  .open = ff_open,
  .readdir = ff_readdir,
  .readlink = ff_readlink,
  .read = ff_read,
  .rename = ff_rename,
  .rmdir = ff_rmdir,
  .symlink = ff_symlink,
  .truncate = ff_truncate,
  .unlink = ff_unlink,
  .utimens = ff_utimens,
  .write = ff_write,

  //.setxattr = ff_setxattr,
  .flush = ff_flush
};


void ff_lock_all (void) {
  pthread_mutex_lock (&ff_mutex);
}

void ff_unlock_all (void) {
  pthread_mutex_unlock (&ff_mutex);
}

int ff_main (int argc, char **argv) {
  return fuse_main (argc, argv, &ff_oper, NULL);
}

