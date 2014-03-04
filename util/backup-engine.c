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

    Copyright 2010-2013 Vkontakte Ltd
              2010-2012 Nikolai Durov
              2010-2012 Andrei Lopatin
                   2013 Anton Maydell
*/

#define	_FILE_OFFSET_BITS	64

#include <assert.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <stddef.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <errno.h>
#include <dirent.h>
#include <signal.h>
#include <time.h>

#include "crc32.h"
#include "server-functions.h"
#include "kfs.h"
#include "kdb-binlog-common.h"

#define	VERSION_STR	"backup-engine-0.16"

#define	MTIME_THRESHOLD	86400

#define	MAX_DIRNAME_LEN	127
#define	MAX_FNAME_LEN	127
#define	MAX_FPATH_LEN	255
#define	MAX_DIR_FILES	1024

#define	BSIZE	(1 << 14)

volatile int sigusr1_cnt = 0;
static int logs_reopen_cnt = 0;

static char *srcdir, *dstdir, pidfile[MAX_FPATH_LEN+1];

static int force_different_devices, remove_useless_not_zipped_backups;

static int srcdev, dstdev, pidfile_created;

#define	COPYBUFF_SIZE	(1 << 24)

static char Buffer[COPYBUFF_SIZE];

static struct stat src_stat, dst_stat, tmp_stat;

static int check_dir (char *dirname, struct stat *buf, int create) {
  if (stat (dirname, buf) < 0) {
    if (errno != ENOENT || !create) {
      return -1;
    }
    if (mkdir (dirname, 0750) < 0) {
      return -1;
    }
    if (stat (dirname, buf) < 0) {
      return -1;
    }
  }
  if (!S_ISDIR (buf->st_mode) || strlen (dirname) > MAX_DIRNAME_LEN) {
    errno = ENOTDIR;
    return -1;
  }
  return buf->st_dev;
}

static void do_remove_pidfile (void) {
  if (pidfile_created) {
    pidfile_created = 0;
    unlink (pidfile);
  }
}

static int create_pidfile (void) {
  static char tmp_buff[32];
  int old_pid, fd;

  assert (sprintf (pidfile, "%s/backup.pid", dstdir) < MAX_FPATH_LEN);

  if (stat (pidfile, &tmp_stat) >= 0) {
    if (!S_ISREG (tmp_stat.st_mode)) {
      errno = EISDIR;
      return -1;
    }
    int fd = open (pidfile, O_RDONLY);
    if (fd < 0) {
      return -1;
    }
    int r = read (fd, tmp_buff, 16);
    close (fd);
    if (r < 0) {
      return -1;
    }
    if (r < 16) {
      tmp_buff[r] = 0;
      old_pid = atoi (tmp_buff);
      if (old_pid > 0) {
        sprintf (tmp_buff, "/proc/%d/", old_pid);
        if (stat (tmp_buff, &tmp_stat) >= 0) {
          kprintf ("fatal: destination directory %s already locked by process %d\n", dstdir, old_pid);
          exit (2);
        }
      }
    }
    kprintf ("warning: removing stale pid file %s\n", pidfile);
    if (unlink (pidfile) < 0 && errno != ENOENT) {
      return -1;
    }
  }

  fd = open (pidfile, O_WRONLY | O_CREAT | O_EXCL, 0640);
  if (fd < 0) {
    return -1;
  }

  pidfile_created = 1;
  atexit (do_remove_pidfile);	/* think about this */

  int s = sprintf (tmp_buff, "%d\n", getpid());
  if (write (fd, tmp_buff, s) != s) {
    close (fd);
    return -1;
  }
  close (fd);
  return 0;
}

#define FIF_DEST   1
#define FIF_ZIPPED 16
#define FIF_RDONLY 32
#define FIF_ERROR  128

struct file_info {
  long long fsize;
  long long orig_file_size;
  struct file_info *peer;
  char *head_data;
  char *tail_data;
  char *filename;
  mode_t mode;
  int fd;
  int flags;
  int inode;
  int mtime;
  unsigned head_crc32;
  unsigned tail_crc32;
  char filepath[MAX_FPATH_LEN+1];
};

static struct file_info src_files[MAX_DIR_FILES], dst_files[MAX_DIR_FILES];
static int src_fnum, dst_fnum, matches;

static int src_mtime_threshold;

static int check_backup_filename (char *filename, int len, char *suffix, int suffix_len) {
  if (len <= 10+suffix_len || memcmp (filename + len - (10+suffix_len), suffix, suffix_len)) {
    return -1;
  }
  int i;
  for (i = 1; i <= 10; i++) {
    if (filename[len-i] < '0' || filename[len-i] > '9') {
      return -1;
    }
  }
  return 0;
}

static int read_dir (struct file_info *files, int *files_num, char *dirname, int mode) {
  DIR *D = opendir (dirname);
  struct dirent *DE, *DEB;
  int dirname_len, len;
  struct file_info *FI;

  *files_num = 0;
  if (!D) {
    return -1;
  }

  dirname_len = strlen (dirname);
  assert (dirname_len > 1 && dirname_len < MAX_DIRNAME_LEN);
  if (dirname[dirname_len-1] == '/') {
    dirname_len--;
  }

  int dirent_len = offsetof (struct dirent, d_name) + pathconf (dirname, _PC_NAME_MAX) + 1;
  DEB = malloc (dirent_len);
  assert (DEB);

  while (1) {
    if (readdir_r (D, DEB, &DE) < 0) {
      return -1;
    }
    if (!DE) {
      break;
    }
    assert (DE == DEB);
    if (DE->d_type != DT_REG && DE->d_type != DT_UNKNOWN) {
      continue;
    }
    len = strlen (DE->d_name);
    if (len > MAX_FNAME_LEN) {
      continue;
    }
    int zipped = -1;
    if (mode) {
      if (!check_backup_filename (DE->d_name, len, ".bin.", 5)) {
        zipped = 0;
      } else if (!check_backup_filename (DE->d_name, len, ".bin.bz.", 8)) {
        zipped = 1;
      } else {
        continue;
      }
    } else {
      if (len > 4 && !memcmp (DE->d_name + len - 4, ".bin", 4)) {
        zipped = 0;
      } else if (len > 7 && !memcmp (DE->d_name + len - 7, ".bin.bz", 7)) {
        zipped = 1;
      } else {
        continue;
      }
    }
    assert (zipped >= 0);
    assert (*files_num < MAX_DIR_FILES);

    FI = files + *files_num;
    memset (FI, 0, sizeof (*FI));

    FI->fd = -1;
    FI->flags = mode;
    if (zipped) {
      FI->flags |= FIF_ZIPPED;
    }
    FI->inode = DE->d_ino;
    memcpy (FI->filepath, dirname, dirname_len);
    FI->filepath[dirname_len] = '/';
    FI->filename = FI->filepath + dirname_len + 1;
    memcpy (FI->filepath + dirname_len + 1, DE->d_name, len + 1);
    assert (dirname_len + len + 2 < MAX_FPATH_LEN);

    if (lstat (FI->filepath, &tmp_stat) < 0) {
      kprintf ("warning: unable to stat %s: %m\n", FI->filepath);
      continue;
    }

    FI->mode = tmp_stat.st_mode;

    if (!S_ISREG (tmp_stat.st_mode)) {
      continue;
    }

    FI->mtime = tmp_stat.st_mtime;
    if (FI->mtime < src_mtime_threshold) {
      vkprintf (1, "ignoring old file %s\n", FI->filepath);
      continue;
    }

    (*files_num)++;
    vkprintf (2, "found suitable file %s\n", FI->filepath);
  }

  closedir (D);
  free (DEB);

  return 0;
}

static void file_data_free (struct file_info *file) {
  if (file->head_data) {
    free (file->head_data);
    file->head_data = 0;
  }
  if (file->tail_data) {
    free (file->tail_data);
    file->tail_data = 0;
  }
}

static int invalidate_file (struct file_info *file) {
  file_data_free (file);
  if (file->fd >= 0) {
    close (file->fd);
    file->fd = -1;
  }
  file->flags |= FIF_ERROR;
  return -1;
}

static int all_flags (struct file_info *file, int mask) {
  return (file->flags & mask) == mask;
}

static int read_file_header (struct file_info *file) {
  int oflags = (file->flags & FIF_DEST) ? O_RDWR : O_RDONLY;

  if (!(file->mode & 0222)) {
    file->flags |= FIF_RDONLY;
  }

  if (all_flags (file, FIF_DEST | FIF_ZIPPED | FIF_RDONLY)) {
    oflags = O_RDONLY;
  }

  file->fd = open (file->filepath, oflags);

  if (file->fd < 0) {
    kprintf ("cannot open file %s with oflags %s. %m\n", file->filepath, O_RDWR == oflags ? "O_RDWR" : "O_RDONLY");
    return invalidate_file (file);
  }

  if (oflags == O_RDWR && (file->flags & FIF_DEST)) {
    assert (lock_whole_file (file->fd, F_WRLCK) > 0);
  }

  if (fstat (file->fd, &tmp_stat) < 0) {
    kprintf ("unable to stat handle %d: %m\n", file->fd);
    return invalidate_file (file);
  }

  if (tmp_stat.st_ino != file->inode) {
    kprintf ("file inode changed for %s from %d to %d, skipping\n", file->filepath, file->inode, (int) tmp_stat.st_ino);
    return invalidate_file (file);
  }

  file->head_crc32 = file->tail_crc32 = 0;
  file->fsize = lseek (file->fd, 0, SEEK_END);

  if (file->fsize < BSIZE * 2) {
    kprintf ("file %s too small, skipping\n", file->filepath);
    return invalidate_file (file);
  }

  assert (file->fsize >= 0);
  file->orig_file_size = -1LL;

  file->head_data = malloc (BSIZE);
  assert (file->head_data);

  assert (!lseek (file->fd, 0, SEEK_SET));

  int r = read (file->fd, file->head_data, BSIZE);
  if (r != BSIZE) {
    kprintf ("unable to read head of %s (handle %d, read %d of %d bytes): %m\n", file->filepath, file->fd, r, BSIZE);
    return invalidate_file (file);
  }

  int p = 0, ok = 0, cnt = 0;
  while (p + 4 <= BSIZE) {
    int M = *(int *)(file->head_data + p);
    if ((!(file->flags & FIF_ZIPPED) && (M == LEV_START || M == LEV_ROTATE_FROM)) ||
        ((file->flags & FIF_ZIPPED) && M == KFS_BINLOG_ZIP_MAGIC)) {
      ok = 1;
      break;
    }
    if (M != KFS_MAGIC || ++cnt > 2) {
      break;
    }

    if (file->flags & FIF_ZIPPED) {
      break;
    }

    p += sizeof (struct kfs_file_header);
  }

  if (!ok) {
    kprintf ("not a binlog file: %s\n", file->filepath);
    return invalidate_file (file);
  }

  if (file->flags & FIF_ZIPPED) {
    kfs_binlog_zip_header_t *H = (kfs_binlog_zip_header_t *) file->head_data;
    assert (sizeof (*H) <= BSIZE);
    file->orig_file_size = H->orig_file_size;
    int sz = kfs_bz_compute_header_size (H->orig_file_size);
    if (H->orig_file_size < 0 || (sz <= BSIZE && compute_crc32 (file->head_data, sz - 4) != *((unsigned int *) (&file->head_data[sz - 4])))) {
      kprintf ("zipped binlog '%s' with broken header\n", file->filepath);
      invalidate_file (file);
    }
  } else {
    file->orig_file_size = file->fsize;
  }

  file->head_crc32 = compute_crc32 (file->head_data, BSIZE);

  if (file->flags & FIF_DEST) {
    assert (lseek (file->fd, -BSIZE, SEEK_END) + BSIZE == file->fsize);
    file->tail_data = malloc (BSIZE);
    r = read (file->fd, file->tail_data, BSIZE);
    if (r != BSIZE) {
      kprintf ("unable to read tail of %s (handle %d, read %d of %d bytes): %m\n", file->filepath, file->fd, r, BSIZE);
      return invalidate_file (file);
    }
    file->tail_crc32 = compute_crc32 (file->tail_data, BSIZE);
  }

  vkprintf (2, "opened %s, size %lld, head crc32 %08x, tail crc32 %08x\n", file->filepath, file->fsize, file->head_crc32, file->tail_crc32);

  if (all_flags (file, FIF_DEST | FIF_ZIPPED) && oflags == O_RDONLY) {
    assert (!close (file->fd));
    file->fd = -1;
  }

  return 1;
}

static int read_file_headers (struct file_info *files, int fnum) {
  int res = 0, i;
  for (i = 0; i < fnum; i++) {
    if (read_file_header (files + i) >= 0) {
      res++;
    } else {
      files[i].flags |= FIF_ERROR;
    }
  }
  return res;
}

static int pair_matches (struct file_info *FS, struct file_info *FD) {
  int l1 = strlen (FS->filename), l2 = strlen (FD->filename), r;
  if (l2 != l1 + 11 || memcmp (FS->filename, FD->filename, l1) || FS->fsize < FD->fsize) {
    return 0;
  }

  if (FS->head_crc32 != FD->head_crc32) {
    return 0;
  }

  assert (lseek (FS->fd, FD->fsize - BSIZE, SEEK_SET) == FD->fsize - BSIZE);
  r = read (FS->fd, Buffer, BSIZE);
  if (r != BSIZE) {
    return 0;
  }
  return !memcmp (Buffer, FD->tail_data, BSIZE);
}

static inline int better_match (struct file_info *FD, struct file_info *FB) {
  if (FD->fsize > FB->fsize) {
    return 1;
  }
  if (FD->fsize < FB->fsize) {
    return -1;
  }
  return strcmp (FD->filename, FB->filename);
}

static struct file_info *create_peer (struct file_info *FS) {
  struct file_info *FI = dst_files + dst_fnum;
  int dirname_len = strlen (dstdir), len = strlen (FS->filename);
  int i, utime;

  assert (dst_fnum < MAX_DIR_FILES && dirname_len > 1 && dirname_len < MAX_DIRNAME_LEN);

  memset (FI, 0, sizeof (*FI));

  FI->fd = -1;
  FI->flags = FIF_DEST | (FS->flags & FIF_ZIPPED);

  if (dstdir[dirname_len - 1] == '/') {
    dirname_len--;
  }

  memcpy (FI->filepath, dstdir, dirname_len);
  FI->filepath[dirname_len] = '/';
  FI->filename = FI->filepath + dirname_len + 1;

  memcpy (FI->filepath + dirname_len + 1, FS->filename, len);
  assert (dirname_len + len + 13 < MAX_FPATH_LEN);

  utime = time (0);
  for (i = 0; i < 8; i++) {
    sprintf (FI->filename + len, ".%d", utime + i);
    FI->fd = open (FI->filepath, O_CREAT | O_EXCL | O_RDWR, 0640);
    if (FI->fd >= 0) {
      break;
    }
  }

  if (FI->fd < 0) {
    kprintf ("cannot create file %s: %m\n", FI->filepath);
    return 0;
  }

  FI->mtime = utime;
  assert (lock_whole_file (FI->fd, F_WRLCK) > 0);

  if (write (FI->fd, FS->head_data, BSIZE) < BSIZE) {
    kprintf ("cannot write to %s: %m\n", FI->filepath);
    invalidate_file (FI);
    return 0;
  }

  FI->fsize = BSIZE;
  dst_fnum++;

  return FI;
}

static int original_matches_zipped_backup (struct file_info *FS, struct file_info *FD) {
  assert (!(FS->flags & FIF_ZIPPED) && (FD->flags & FIF_ZIPPED));
  int r;
  const int l1 = strlen (FS->filename), l2 = strlen (FD->filename);
  if (l2 != l1 + 14 || memcmp (FS->filename, FD->filename, l1) || FS->orig_file_size != FD->orig_file_size) {
    return 0;
  }

  kfs_binlog_zip_header_t *H = (kfs_binlog_zip_header_t *) FD->head_data;
  if (memcmp (FS->head_data, H->first36_bytes, 36)) {
    return 0;
  }

  assert (lseek (FS->fd, FS->fsize - 36, SEEK_SET) == FS->fsize - 36);
  r = read (FS->fd, Buffer, 36);
  if (r != 36 || memcmp (Buffer, H->last36_bytes, 36)) {
    return 0;
  }

  return 1;
}

static void reopen_logs (void);

static int match_files (void) {
  struct file_info *FC, *FS, *FD, *FB, *FE;
  int i, j;

  matches = 0;

  for (i = 0, FS = FC = src_files; i < src_fnum; i++, FS++) {

    if (logs_reopen_cnt != sigusr1_cnt) {
      reopen_logs ();
    }

    if (FS->flags & FIF_ERROR) {
      continue;
    }
    FB = FE = NULL;
    for (j = 0, FD = dst_files; j < dst_fnum; j++, FD++) {
      if (FD->flags & FIF_ERROR) {
        continue;
      }

      if (FB == NULL && FE == NULL && all_flags (FD, FIF_ZIPPED | FIF_RDONLY) && !(FS->flags & FIF_ZIPPED) && original_matches_zipped_backup (FS, FD)) {
        FE = FD;
      }

      if (FD->peer) {
        continue;
      }

      if (pair_matches (FS, FD) && (!FB || better_match (FD, FB) > 0)) {
        FB = FD;
      }
    }

    if (FB == NULL) {
      if (FE != NULL) {
        kprintf ("skip creating new uncompressed backup for file '%s', since there is zipped backup '%s'\n", FS->filename, FE->filename);
        continue;
      }
      FB = create_peer (FS);
      if (!FB) {
        kprintf ("warning: unable to create peer for %s\n", FS->filepath);
        continue;
      }
    }
    matches++;
    FS->peer = FB;
    FB->peer = FC;
    assert (lseek (FS->fd, FB->fsize, SEEK_SET) == FB->fsize);
    vkprintf (2, "found peer %s %lld -> %s %lld\n", FS->filepath, FS->fsize, FB->filepath, FB->fsize);
    *FC++ = *FS;
  }

  src_fnum = matches;

  for (j = 0, FD = dst_files; j < dst_fnum; j++, FD++) {
    if (!FD->peer) {
      if (remove_useless_not_zipped_backups && !(FD->flags & FIF_ZIPPED)) {
        for (i = 0, FS = src_files; i < src_fnum; i++, FS++) {
          if (FS->peer && all_flags (FS, FIF_ZIPPED | FIF_RDONLY) && FS->fsize == FS->peer->fsize && all_flags (FS->peer, FIF_ZIPPED | FIF_RDONLY)) {
            char *s2 = FD->filename, *s1 = FS->filename;
            const int l2 = strlen (s2), l1 = strlen (s1);
            if (l2 == l1 - 3 + 11 && !memcmp (s1, s2, l1 - 3) && !strcmp (s1 + l1 - 3, ".bz")) {
              break;
            }
          }
        }
        if (i < src_fnum) {
          kprintf ("unlink useless backup '%s'\n", FD->filepath);
          if (unlink (FD->filepath) < 0) {
            kprintf ("unlink (\"%s\") failed. %m\n", FD->filepath);
          }
        }
      }
      invalidate_file (FD);
    } else {
      file_data_free (FD);
    }
  }

  for (i = 0, FS = src_files; i < src_fnum; i++, FS++) {
    file_data_free (FS);
  }

  return matches;
}

static int copy_iteration (void) {
  int i, res = 0;
  struct file_info *FS, *FD;

  for (FS = src_files, i = 0; i < src_fnum; i++, FS++) {
    if (logs_reopen_cnt != sigusr1_cnt) {
      reopen_logs ();
    }

    FD = FS->peer;
    assert (FD);
    if (FD->flags & FIF_RDONLY) {
      continue;
    }
    int r = read (FS->fd, Buffer, COPYBUFF_SIZE);
    assert (r >= 0);
    if (r > 0) {
      int w = write (FD->fd, Buffer, r);
      assert (r == w);
      res += (w == COPYBUFF_SIZE);
      vkprintf (2, "copied %d bytes from %d (%s) to %d (%s) at position %lld\n", r, FS->fd, FS->filepath, FD->fd, FD->filepath, FD->fsize);
      assert (!fsync (FD->fd));
      FD->fsize += r;
    }
    if (FS->fsize == FD->fsize && (FD->flags & FIF_ZIPPED) && all_flags (FS, FIF_ZIPPED | FIF_RDONLY)) {
      if (chmod (FD->filepath, 0440) < 0) {
        kprintf ("warning: chmod (\"%s\", 0440) failed. %m\n", FD->filepath);
      }
      assert (!close (FS->fd));
      assert (!close (FD->fd));
      FS->fd = FD->fd = -1;
      FD->flags |= FIF_RDONLY;
    }
  }
  return res;
}

static void reopen_logs (void) {
  logs_reopen_cnt = sigusr1_cnt;
  int fd;
  fflush (stdout);
  fflush (stderr);
  if ((fd = open ("/dev/null", O_RDWR, 0)) != -1) {
    dup2 (fd, 0);
    dup2 (fd, 1);
    dup2 (fd, 2);
    if (fd > 2)
      close (fd);
  }
  if (logname && (fd = open (logname, O_WRONLY|O_APPEND|O_CREAT, 0640)) != -1) {
    dup2 (fd, 1);
    dup2 (fd, 2);
    if (fd > 2)
      close (fd);
  }
  vkprintf (1, "logs reopened.\n");
}

static void sigint_handler (const int sig) {
  static const char message[] = "SIGINT handled.\n";
  kwrite (2, message, sizeof (message) - (size_t)1);
  exit (1);
}

static void sigterm_handler (const int sig) {
  static const char message[] = "SIGTERM handled.\n";
  kwrite (2, message, sizeof (message) - (size_t)1);
  exit (1);
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

/*
 *
 *		MAIN
 *
 */

void usage (void) {
  printf ("usage: %s [-v] [-u<username>] [-l<log-name>] [-A] <src-dir> <dest-dir>\n"
  	  "\t" VERSION_STR " compiled at " __DATE__ " " __TIME__ " by gcc " __VERSION__ " "
#ifdef __LP64__
	  "64-bit"
#else
	  "32-bit"
#endif
	  " after commit " COMMIT "\n"
    "\tPerforms binlog file replication/backup from disk to disk\n"
    "\t-i<seconds>\tdelay between loops\n"
    "\t-x<iterations>\texit after specified number of loops\n"
    "\t-f\tfast copy files: no delay between iterations when big chunks copied\n"
    "\t-D\tfail if both src-dir and dest-dir are on same device\n"
    "\t-A\tbackup all files, not only modified at most one day ago\n"
    "\t-R\tremove useless backups\n"
    "\t\tif dstdir contains full copy of *.bin.bz file and srcdir doesn't contain *.bin file then backup *.bin file will be removed\n"
    "\t-v\toutput statistical and debug information into stderr\n",
	  progname);
  exit (2);
}

int delay_seconds = 10, fast_copy, max_iterations = 6;

int main (int argc, char *argv[]) {
  int i;

  set_debug_handlers ();

  src_mtime_threshold = time (0) - MTIME_THRESHOLD;

  progname = argv[0];
  while ((i = getopt (argc, argv, "ADRdfhi:l:p:u:vx:")) != -1) {
    switch (i) {
    case 'A':
      src_mtime_threshold = 0;
      break;
    case 'D':
      force_different_devices = 1;
      break;
    case 'R':
      remove_useless_not_zipped_backups = 1;
      break;
    case 'd':
      daemonize ^= 1;
      break;
    case 'f':
      fast_copy = 1;
      break;
    case 'h':
      usage();
      return 2;
    case 'i':
      delay_seconds = atoi (optarg);
      if (delay_seconds <= 0) {
        delay_seconds = 1;
      }
      break;
    case 'l':
      logname = optarg;
      break;
    case 'p':
      port = atoi(optarg);
      break;
    case 'u':
      username = optarg;
      break;
    case 'v':
      verbosity++;
      break;
    case 'x':
      max_iterations = atoi (optarg);
      break;
    default:
      kprintf ("Unimplemented option %c\n", i);
      exit (2);
      break;
    }
  }
  if (argc != optind + 2) {
    usage();
    return 2;
  }

  srcdir = argv[optind];
  assert (srcdir);
  dstdir = argv[optind+1];
  assert (dstdir);

  umask (027);

  if (change_user (username) < 0) {
    kprintf ("fatal: cannot change user to %s\n", username ? username : "(none)");
    exit (1);
  }

  signal(SIGINT, sigint_handler);
  signal(SIGTERM, sigterm_handler);
  signal(SIGUSR1, sigusr1_handler);
  signal(SIGHUP, sighup_handler);

  srcdev = check_dir (srcdir, &src_stat, 0);
  if (srcdev < 0) {
    kprintf ("fatal: source directory %s not found: %m\n", srcdir);
    exit (1);
  }

  dstdev = check_dir (dstdir, &dst_stat, 1);
  if (dstdev < 0) {
    kprintf ("fatal: destination directory %s not found: %m\n", dstdir);
    exit (1);
  }

  if (srcdev == dstdev) {
    fprintf (stderr, "%s: both directories %s and %s are on same device dev%d-%d\n",
      force_different_devices ? "fatal" : "warning", srcdir, dstdir, srcdev >> 8, srcdev & 0xff);
    if (force_different_devices) {
      exit (1);
    }
  }

  if (create_pidfile () < 0) {
    kprintf ("fatal: cannot create lock file %s: %m\n", pidfile);
    exit (1);
  }

  if (read_dir (src_files, &src_fnum, srcdir, 0) < 0) {
    kprintf ("fatal: cannot read source directory %s: %m\n", srcdir);
    exit (1);
  }

  if (read_dir (dst_files, &dst_fnum, dstdir, 1) < 0) {
    kprintf ("fatal: cannot read destination directory %s: %m\n", dstdir);
    exit (1);
  }

  read_file_headers (src_files, src_fnum);
  read_file_headers (dst_files, dst_fnum);

  assert (src_fnum + dst_fnum <= MAX_DIR_FILES);

  match_files ();

  while (--max_iterations > 0) {
    if (!copy_iteration () || !fast_copy) {
      sleep (delay_seconds);
    } else {
      max_iterations++;
    }
  }

  return 0;
}
