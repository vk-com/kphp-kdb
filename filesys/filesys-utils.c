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
#define _LARGEFILE64_SOURCE

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>
#include <dirent.h>
#include <sys/time.h>
#include <sys/types.h>
#include <pwd.h>
#include <grp.h>
#include <zlib.h>

#include "kdb-data-common.h"
#include "server-functions.h"
#include "filesys-utils.h"

int lcopy_attrs (char *filename, struct stat *S)  {
  if (!S_ISLNK (S->st_mode)) {
    if (chmod (filename, S->st_mode) < 0) {
      vkprintf (1, "chmod for %s failed. %m\n", filename);
      return -1;
    }
  }
  if (lchown (filename, S->st_uid, S->st_gid) < 0) {
    vkprintf (1, "lchown for %s failed. %m\n", filename);
    return -1;
  }
  struct timeval times[2];
  memset (times, 0, sizeof (times));
  times[0].tv_sec = S->st_atime;
  times[1].tv_sec = S->st_mtime;
  errno = 0;
  if (lutimes (filename, times) < 0) {
    int err = errno;
    vkprintf (1, "lutimes for %s failed. %m\n", filename);
    if (err == ENOSYS && !S_ISLNK (S->st_mode)) {
      if (utimes (filename, times) < 0) {
        vkprintf (1, "utimes for %s failed. %m\n", filename);
        return -3;
      }
      errno = 0;
      return 0;
    }
    return -2;
  }
  return 0;
}

static int cmp_file (const void *x, const void *y) {
  const file_t *h1 = *(const file_t **) x;
  const file_t *h2 = *(const file_t **) y;
  return strcmp (h1->filename, h2->filename);
}

static void free_file (file_t *p) {
  if (p->filename) {
    zfree (p->filename, strlen (p->filename) + 1);
  }
  zfree (p, sizeof (*p));
}

file_t *remove_file_from_list (file_t *x, const char *const filename) {
  int t = 0;
  file_t **p = &x;
  while (*p != NULL) {
    file_t *V = *p;
    assert (V->filename);
    if (!strcmp (filename, V->filename)) {
      *p = V->next;
      free_file (V);
      return t ? x : *p;
    }
    p = &V->next;
    t++;
  }
  return x;
}

void free_filelist (file_t *p) {
  while (p != NULL) {
    file_t *w = p->next;
    free_file (p);
    p = w;
  }
}

int getdir (const char *dirname, file_t **R, int sort, int hidden) {
  *R = NULL;
  char path[PATH_MAX];
  int l = snprintf (path, PATH_MAX, "%s/", dirname);
  if (l >= PATH_MAX - 1) { return 0; }
  int max_filename_length = PATH_MAX - 1 - l;
  int n = 0;
  file_t *head = NULL, *p;
  DIR *D = opendir (dirname);
  if (D == NULL) {
    vkprintf (1, "opendir (%s) returns NULL.\n", dirname);
    return 0;
  }
  while (1) {
    errno = 0;
    struct dirent *entry = readdir (D);
    if (entry == NULL) {
      if (errno) {
        kprintf ("getdir (%s) failed. %m\n", dirname);
        exit (1);
      }
      break;
    }
    if (!strcmp (entry->d_name, ".") || !strcmp (entry->d_name, "..")) {
      continue;
    }

    if (!hidden && !strncmp (entry->d_name, ".", 1)) {
      vkprintf (1, "Skip %s in %s.\n", entry->d_name, dirname);
      continue;
    }

    if (strlen (entry->d_name) > max_filename_length) {
      vkprintf (1, "Skipping too long filename (%s/%s).\n", dirname, entry->d_name);
      continue;
    }

    strcpy (path + l, entry->d_name);
    p = zmalloc (sizeof (file_t));
    if (lstat (path, &p->st)) {
      kprintf ("lstat (%s) fail. %m\n", path);
      zfree (p, sizeof (file_t));
      continue;
    }
    p->filename = zstrdup (entry->d_name);
    p->next = head;
    head = p;
    n++;
  }
  closedir (D);
  if (!sort) {
    *R = head;
  } else if (n) {
    int i;
    dyn_mark_t s;
    dyn_mark (s);
    file_t **A = zmalloc (n * sizeof (file_t *));
    p = head;
    for (i = n - 1; i >= 0; i--) {
      A[i] = p;
      p = p->next;
    }
    assert (p == NULL);
    qsort (A, n, sizeof (A[0]), cmp_file);
    A[n-1]->next = NULL;
    for (i = 0; i < n - 1; i++) {
      A[i]->next = A[i+1];
    }
    *R = A[0];
    dyn_release (s);
  }
  return n;
}

static int rec_delete_file (const char *path, struct stat *S) {
  char a[PATH_MAX];
  if (S_ISLNK (S->st_mode)) {
    if (unlink (path)) {
      vkprintf (1, "unlink (%s) failed. %m\n", path);
      return -5;
    }
  } else if (S_ISDIR (S->st_mode)) {
    int l = strlen (path);
    file_t *px, *p;
    int n = getdir (path, &px, 0, 1);
    if (n < 0) {
      return -2;
    }
    for (p = px; p != NULL; p = p->next) {
      int m = l + 2 + strlen (p->filename);
      if (PATH_MAX < m) {
        return -6;
      }
      sprintf (a, "%s/%s", path, p->filename);
      if (rec_delete_file (a, &p->st) < 0) {
        return -3;
      }
    }
    free_filelist (px);
    if (rmdir (path)) {
      vkprintf (1, "rmdir (%s) failed. %m\n", path);
      return -7;
    }
  } else {
    if (unlink (path)) {
      return -4;
    }
  }
  return 0;
}

int delete_file (const char *path) {
  struct stat b;
  if (lstat (path, &b)) {
    return -1;
  }
  return rec_delete_file (path, &b);
}

static int rec_clone_file (const char *const src_path, const char *const dst_path, char *path, struct stat *S) {
  char a[PATH_MAX], b[PATH_MAX];
  if (S_ISLNK (S->st_mode)) {
    if (snprintf (a, PATH_MAX, "%s/%s", src_path, path) >= PATH_MAX) {
      return -1;
    }
    int r = readlink (a, b, PATH_MAX);
    if (r < 0 || r >= PATH_MAX) {
      return -4;
    }
    b[r] = 0;
    if (snprintf (a, PATH_MAX, "%s/%s", dst_path, path) >= PATH_MAX) {
      return -1;
    }
    if (symlink (b, a) < 0) {
      vkprintf (1, "symlink (%s, %s) failed. %m\n", b, a);
      return -5;
    }
    if (lcopy_attrs (a, S) < 0) {
      return -6;
    }
  } else if (S_ISDIR (S->st_mode)) {
    if (snprintf (a, PATH_MAX, "%s/%s", dst_path, path) >= PATH_MAX) {
      return -1;
    }
    if (mkdir (a, S->st_mode)) {
      vkprintf (1, "mkdir (%s, %d) fail. %m\n", a, S->st_mode);
      return -2;
    }
    if (snprintf (a, PATH_MAX, "%s/%s", src_path, path) >= PATH_MAX) {
      return -1;
    }
    file_t *px, *p;
    int n = getdir (a, &px, 0, 1);
    if (n < 0) {
      return -2;
    }
    for (p = px; p != NULL; p = p->next) {
      if (snprintf (a, PATH_MAX, "%s/%s", path, p->filename) >= PATH_MAX) {
        return -1;
      }
      if (rec_clone_file (src_path, dst_path, a, &p->st) < 0) {
        return -3;
      }
    }
    free_filelist (px);
    if (snprintf (a, PATH_MAX, "%s/%s", dst_path, path) >= PATH_MAX) {
      return -1;
    }
    if (lcopy_attrs (a, S) < 0) {
      return -7;
    }
  } else {
    if (snprintf (a, PATH_MAX, "%s/%s", src_path, path) >= PATH_MAX || snprintf (b, PATH_MAX, "%s/%s", dst_path, path) >= PATH_MAX) {
      return -1;
    }
    if (link (a, b) < 0) {
      vkprintf (1, "link (%s, %s) fail. %m\n", a, b);
      return -1;
    }
  }
  return 0;
}

int clone_file (const char *const oldpath, const char *const newpath) {
  struct stat b;
  if (stat (oldpath, &b)) {
    return -1;
  }
  return rec_clone_file (oldpath, newpath, "", &b);
}

static void tar_fill_ustar_magic (char b[512]) {
  strcpy (b + 257, "ustar");
  memcpy (b + 263, "00", 2);
}

static int tar_fill_longlink_header (char b[512], int link_length, char ext) {
  memset (b, 0, 512);
  strcpy (b, "././@LongLink");
  sprintf (b + 100, "%07o", 0);
  sprintf (b + 108, "%07o", 0);
  sprintf (b + 116, "%07o", 0);
  sprintf (b + 124, "%011o", link_length + 1);
  sprintf (b + 136, "%011llo", (unsigned long long) 0);
  tar_fill_ustar_magic (b);
  b[156] = ext;
  return 0;
}

static int tar_write_header (gzFile f, char b[512]) {
  unsigned chksum = 8 * 32; /* eight checksum bytes taken to be ascii spaces (decimal value 32) */
  int i;
  for (i = 0; i < 512; i++) {
    chksum += (unsigned char) b[i];
  }
  chksum &= 0777777;
  sprintf (b + 148, "%06o", chksum);
  b[155] = ' ';
  return gzwrite (f, b, 512) == 512 ? 0 : -1;
}

static int tar_write_long_link_header (gzFile f, char header[512], int link_length, const char *const data) {
  if (tar_write_header (f, header) < 0) {
    return TAR_PACK_ERR_WRITE_HEADER;
  }
  int i;
  for (i = 0; i < link_length + 1; i += 512) {
    int o = link_length + 1 - i;
    if (o > 512) {
      o = 512;
    }
    memset (header, 0, 512);
    memcpy (header, data + i, o);
    if (gzwrite (f, header, 512) != 512) {
      return TAR_PACK_ERR_GZWRITE;
    }
  }
  return 0;
}

static int tar_fill_header (gzFile f, char b[512], struct stat *S, const char *const filename) {
  memset (b, 0, 512);
  int l = strlen (filename);
  const int MAX_L = 99;
  int longname = 0;
  if (l > MAX_L) {
    char *p = strchr (filename + l - MAX_L, '/');
    if (p == NULL) {
      longname = 1;
    } else {
      int o = p - filename;
      if (o > 155) {
        longname = 1;
      } else {
        strcpy (b, filename + o + 1);
        memcpy (b + 345, filename, o);
      }
    }
  } else {
    strcpy (b, filename);
  }

  if (longname) {
    vkprintf (2, "too long full filename: %s\n", filename);
    l = strlen (filename);
    tar_fill_longlink_header (b, l, 'L');
    int r = tar_write_long_link_header (f, b, l, filename);
    if (r < 0) {
      return r;
    }
    memset (b, 0, 512);
    memcpy (b, filename, 100);
  }

  sprintf (b + 100, "%07o", S->st_mode);
  sprintf (b + 108, "%07o", S->st_uid);
  sprintf (b + 116, "%07o", S->st_gid);
  sprintf (b + 124, "%011llo", (unsigned long long) S->st_size);
  sprintf (b + 136, "%011llo", (unsigned long long) S->st_mtime);
  tar_fill_ustar_magic (b);

  b[156] = '0';
  if (S_ISLNK (S->st_mode)) {
    b[156] = '2';
  } else if (S_ISDIR (S->st_mode)) {
    b[156] = '5';
  }

  struct passwd *P = getpwuid (S->st_uid);
  strncpy (b + 265, P->pw_name, 32);
  struct group *G = getgrgid (S->st_gid);
  strncpy (b + 297, G->gr_name, 32);
  return 0;
}

static int rec_tar_pack (gzFile f, const char *const src_path, const char *const path, struct stat *S) {
  vkprintf (3, "rec_tar_pack (path = %s)\n", path);
  char a[PATH_MAX], b[PATH_MAX], header[512];
  if (S_ISLNK (S->st_mode)) {
    if (snprintf (a, PATH_MAX, "%s/%s", src_path, path) >= PATH_MAX) {
      return TAR_PACK_ERR_PATH_TOO_LONG;
    }
    int link_length = readlink (a, b, PATH_MAX);
    if (link_length < 0 || link_length >= PATH_MAX) {
      return -4;
    }
    b[link_length] = 0;
    if (link_length > 100) {
      tar_fill_longlink_header (header, link_length, 'K');
      if (tar_write_header (f, header) < 0) {
        return TAR_PACK_ERR_WRITE_HEADER;
      }
      int i;
      for (i = 0; i < link_length + 1; i += 512) {
        int o = link_length + 1 - i;
        if (o > 512) {
          o = 512;
        }
        memset (header, 0, 512);
        memcpy (header, b + i, o);
        if (gzwrite (f, header, 512) != 512) {
          return TAR_PACK_ERR_GZWRITE;
        }
      }
      link_length = 100;
    }
    if (tar_fill_header (f, header, S, path) < 0) {
      return TAR_PACK_ERR_FILL_HEADER;
    }
    memcpy (header + 157, b,  link_length);
    if (tar_write_header (f, header) < 0) {
      return TAR_PACK_ERR_WRITE_HEADER;
    }
  } else if (S_ISDIR (S->st_mode)) {
    if (path[0]) {
      char slash_ended_path[PATH_MAX];
      assert (snprintf (slash_ended_path, PATH_MAX, "%s/", path) < PATH_MAX);
      if (tar_fill_header (f, header, S, slash_ended_path) < 0) {
        return TAR_PACK_ERR_FILL_HEADER;
      }
      if (tar_write_header (f, header) < 0) {
        return TAR_PACK_ERR_WRITE_HEADER;
      }
    }
    if (snprintf (a, PATH_MAX, "%s/%s", src_path, path) >= PATH_MAX) {
      return TAR_PACK_ERR_PATH_TOO_LONG;
    }
    file_t *px, *p;
    int n = getdir (a, &px, 1, 1);
    if (n < 0) {
      return -2;
    }

    if (!path[0]) {
      px = remove_file_from_list (px, ".filesys-xfs-engine.pid");
    }

    for (p = px; p != NULL; p = p->next) {
      if (path[0]) {
        if (snprintf (a, PATH_MAX, "%s/%s", path, p->filename) >= PATH_MAX) {
          return TAR_PACK_ERR_PATH_TOO_LONG;
        }
      } else {
        if (snprintf (a, PATH_MAX, "%s", p->filename) >= PATH_MAX) {
          return TAR_PACK_ERR_PATH_TOO_LONG;
        }
      }
      int r = rec_tar_pack (f, src_path, a, &p->st);
      if (r < 0) {
        return r;
      }
    }
    free_filelist (px);
  } else {
    if (snprintf (a, PATH_MAX, "%s/%s", src_path, path) >= PATH_MAX) {
      return TAR_PACK_ERR_PATH_TOO_LONG;
    }
    if (tar_fill_header (f, header, S, path) < 0) {
      return TAR_PACK_ERR_FILL_HEADER;
    }
    if (tar_write_header (f, header) < 0) {
      return TAR_PACK_ERR_WRITE_HEADER;
    }
    int fd = open (a, O_RDONLY);
    if (fd < 0) {
      return TAR_PACK_ERR_OPEN;
    }
    int BUF_SIZE = 16 << 20;
    dyn_mark_t mrk;
    dyn_mark (mrk);
    char *buf = zmalloc (BUF_SIZE);
    off_t i = 0;
    while (i < S->st_size) {
      int o = S->st_size - i;
      if (o > BUF_SIZE) { o = BUF_SIZE; }
      int padded = (o + 511) & -512;
      assert (o == read (fd, buf, o));
      if (padded != o) {
        memset (buf + o, 0, padded - o);
      }
      if (gzwrite (f, buf, padded) != padded) {
        dyn_release (mrk);
        return TAR_PACK_ERR_GZWRITE;
      }
      i += padded;
    }
    dyn_release (mrk);
    close (fd);
  }
  return 0;
}

int tar_pack (const char *const tar_filename, const char *const path, int compression_level) {
  char mode[8];
  if (compression_level >= 1 && compression_level <= 9) {
    sprintf (mode, "wb%d", compression_level);
  } else {
    strcpy (mode, "wb");
  }

  int fd = open (tar_filename, O_CREAT | O_TRUNC | O_WRONLY | O_EXCL, 0660);
  if (fd < 0) { return TAR_PACK_ERR_OPEN; }
  int dd = dup (fd);
  if (dd < 0) {
    assert (close (fd) >= 0);
    return TAR_PACK_ERR_DUP;
  }

  gzFile f = gzdopen (dd, mode);
  struct stat b;
  if (stat (path, &b)) {
    return -1;
  }
  dyn_mark_t mrk;
  dyn_mark (mrk);
  int r = rec_tar_pack (f, path, "", &b);
  dyn_release (mrk);
  char buf[1024];
  memset (buf, 0 , 1024);
  assert (gzwrite (f, buf, 1024) == 1024);
  assert (gzclose (f) == Z_OK);
  assert (fsync (fd) >= 0);
  assert (close (fd) >= 0);
  return r;
}

static unsigned long long get_oct (const char buf[512], const int offset, const int len) {
  int i;
  unsigned long long r = 0;
  for (i = 0; i < len; i++) {
    int x = buf[offset+i] - '0';
    if (x < 0 || x >= 8) {
      kprintf ("found not octal digit(%c) at pos %d.\n", buf[offset+i], offset + i);
      assert (x >= 0 && x < 8);
    }
    r <<= 3;
    r |= x;
  }
  return r;
}

struct tar_unpack_dir_mtime_entry {
  struct tar_unpack_dir_mtime_entry *next;
  struct stat st;
  char dirname[0];
};
#define MAX_DIR_DEPTH 128
typedef struct {
  struct tar_unpack_dir_mtime_entry *e[MAX_DIR_DEPTH];
} tar_unpack_dir_mtime_t;

static void tar_unpack_dir_mtime_init (tar_unpack_dir_mtime_t *M) {
  memset (M, 0, sizeof (*M));
}

static void tar_unpack_dir_mtime_add (tar_unpack_dir_mtime_t *M, char *dir, struct stat *st) {
  int k = 0;
  char *s;
  for (s = dir; *s; s++) {
    if (*s == '/') {
      k++;
    }
  }
  if (k < MAX_DIR_DEPTH) {
    struct tar_unpack_dir_mtime_entry *E = zmalloc (sizeof (struct tar_unpack_dir_mtime_entry) + strlen (dir) + 1);
    memcpy (&E->st, st, sizeof (struct stat));
    strcpy (E->dirname, dir);
    E->next = M->e[k];
    M->e[k] = E;
  }
}

static int tar_unpack_check_header (const char header[512]) {
  int i, sum = 0;
  for (i = 0; i < 512; i++) {
    sum += (unsigned char) header[i];
  }
  if (!sum) {
    return 0;
  }
  int x = 0;
  for (i = 148; i < 156; i++) {
    x += (unsigned char) header[i];
  }
  sum += 32 * 8 - x;
  int chksum = get_oct (header, 148, 6);
  if (sum != chksum) {
    kprintf ("broken header, chksum = %d, but sum = %d\n", chksum, sum);
    assert (sum == chksum);
  }
  assert (!memcmp (header + 257, "ustar", 5));
  return 1;
}

int tar_unpack (int tar_gz_fd, const char *const path) {
  char full_filename[PATH_MAX], long_filename[PATH_MAX];
  int i, res = 0;
  dyn_mark_t mrk;
  dyn_mark (mrk);
  tar_unpack_dir_mtime_t M;
  tar_unpack_dir_mtime_init (&M);
  gzFile f = gzdopen (tar_gz_fd, "rb");
  char buf[512];
  int BUF_SIZE = 16 << 20;
  char *io_buff = zmalloc (BUF_SIZE);
  assert (io_buff != NULL);
  int headers = 0;
  while (gzread (f, buf, 512) == 512) {
    int longlink = 0, longname = 0;
    headers++;
    if (!tar_unpack_check_header (buf)) {
      break;
    }
    char tp = buf[156];
    off_t size = get_oct (buf, 124, 11);
    while (tp == 'K' || tp == 'L') {
      assert (!memcmp (buf, "././@LongLink", 13));
      int padded = (size + 511) & -512;
      if (tp == 'K') {
        longlink = 1;
        assert (padded == gzread (f, io_buff, padded));
      } else if (tp == 'L') {
        longname = 1;
        assert (padded <= PATH_MAX);
        assert (padded == gzread (f, long_filename, padded));
      }
      assert (gzread (f, buf, 512) == 512);
      tar_unpack_check_header (buf);
      tp = buf[156];
      size = get_oct (buf, 124, 11);
    }

    mode_t mode = get_oct (buf, 100, 7);
    uid_t uid = get_oct (buf, 108, 7);
    gid_t gid = get_oct (buf, 116, 7);
    time_t mtime = get_oct (buf, 136, 11);
    char ch;
    if (!longname) {
      memcpy (long_filename, buf + 345, 512 - 345);
      if (long_filename[0]) {
        strcat (long_filename, "/");
      }
      ch = buf[100];
      buf[100] = 0;
      strcat (long_filename, buf);
      buf[100] = ch;
    }
    assert (snprintf (full_filename, PATH_MAX, "%s/%s", path, long_filename) < PATH_MAX);
    vkprintf (2, "%s %07o %d %d %lld %u %c\n", full_filename, mode, uid, gid, (long long) size, (unsigned) mtime, tp);
    struct stat st;
    st.st_mode = mode;
    st.st_uid = uid;
    st.st_gid = gid;
    st.st_size = size;
    st.st_atime = st.st_mtime = mtime;
    int l = strlen (full_filename);
    off_t k = 0;
    int fd = -1;
    switch (tp) {
      case '0':
        k = 0;
        fd = open (full_filename, O_CREAT | O_TRUNC | O_WRONLY | O_EXCL, mode);
        if (fd < 0) {
          kprintf ("open (%s) failed. %m\n", full_filename);
          res = TAR_UNPACK_ERR_OPEN;
          goto exit;
        }
        while (k < size) {
          int o = size - k;
          if (o > BUF_SIZE) { o = BUF_SIZE; }
          int padded = (o + 511) & -512;
          assert (padded == gzread (f, io_buff, padded));
          assert (write (fd, io_buff, o) == o);
          k += padded;
        }
        assert (!close (fd));
        if (lcopy_attrs (full_filename, &st) < 0) {
          res = TAR_UNPACK_ERR_COPY_ATTRS;
          goto exit;
        }
        break;
      case '2':
        assert (S_ISLNK (mode));
        ch = buf[257];
        buf[257] = 0;
        char *oldpath = longlink ? io_buff : buf + 157;
        if (symlink (oldpath, full_filename)) {
          kprintf ("symlink (%s, %s) fail. %m\n", oldpath, full_filename);
          res = TAR_UNPACK_ERR_SYMLINK;
          goto exit;
        }
        buf[257] = ch;
        break;
      case '5':
        assert (S_ISDIR (mode));
        assert (l > 0 && full_filename[l-1] == '/');
        full_filename[l-1] = 0;
        long_filename[strlen (long_filename) - 1] = 0;
        if (mkdir (full_filename, mode)) {
          kprintf ("mkdir (%s, %07o) fail. %m\n", full_filename, mode);
          res = TAR_UNPACK_ERR_MKDIR;
          goto exit;
        }
        tar_unpack_dir_mtime_add (&M, long_filename, &st);
        break;
      default:
        kprintf ("unimplemented file type %c\n", tp);
        assert (0);
        break;
    }
  }

  exit:
  assert (gzclose (f) == Z_OK);
  if (!res) {
    for (i = MAX_DIR_DEPTH - 1; i >= 0; i--) {
      struct tar_unpack_dir_mtime_entry *p;
      for (p = M.e[i]; p != NULL; p = p->next) {
        assert (snprintf (full_filename, PATH_MAX, "%s/%s", path, p->dirname) < PATH_MAX);
        int r = lcopy_attrs (full_filename, &p->st);
        if (r < 0) {
          kprintf ("lcopy_attrs (%s) returns error code %d. %m\n", full_filename, r);
          res = -2;
          goto exit2;
        }
      }
    }
  }
  exit2:

  dyn_release (mrk);
  return res;
}

static unsigned char *read_whole_file (const char *path, int l) {
  int fd = open (path, O_RDONLY);
  if (fd < 0) {
    vkprintf (1, "Couldn't open %s for reading. %m\n", path);
    return NULL;
  }
  unsigned char *p = zmalloc (l > 0 ? l : 1);
  int bytes_read = read (fd, p, l);
  close (fd);
  if (bytes_read != l) {
    vkprintf (1, "read %d bytes of %d from %s. %m\n", bytes_read, l, path);
    return NULL;
  }
  return p;
}

int get_file_content (char *dir, file_t *x, unsigned char **a, int *L) {
  *a = NULL;
  *L = 0;
  if (S_ISLNK (x->st.st_mode)) {
    vkprintf (3, "link: %s\n", x->filename);
    *a = zmalloc (PATH_MAX);
    *L = readlink (dir, (char *) *a, PATH_MAX);
    if (*L < 0 || *L >= PATH_MAX - 1) {
      return -1;
    }
    (*a)[*L] = 0;
    (*L)++;
    return 0;
  }
  if (S_ISDIR (x->st.st_mode)) {
    vkprintf (3, "dir: %s\n", x->filename);
    *L = 0;
    return 0;
  }
  vkprintf (3, "file: %s\n", x->filename);
  *L = x->st.st_size;
  *a = read_whole_file (dir, *L);
  if (*a == NULL) {
    return -2;
  }
  return 0;
}

/******************************** PID file ************************************/
const char *const szPidFilename = ".filesys-xfs-engine.pid";
static char *pid_filename = NULL;

static int pid_fd = -1;
int lock_pid_file (const char *const dir) {
  char a[PATH_MAX];
  assert (snprintf (a, PATH_MAX, "%s/%s", dir, szPidFilename) < PATH_MAX);
  pid_fd = open (a, O_CREAT | O_WRONLY | O_EXCL, 0660);
  if (pid_fd < 0) {
    kprintf ("creating %s failed. %m\n", a);
    return -1;
  }
  if (lock_whole_file (pid_fd, F_WRLCK) < 0) {
    kprintf ("lock %s failed. %m\n", a);
    return -2;
  }
  pid_filename = zstrdup (a);
  int l = snprintf (a, PATH_MAX, "%lld\n", (long long) getpid ());
  assert (l < PATH_MAX);
  assert (write (pid_fd, a, l) == l);
  return 1;
}

void unlock_pid_file (void) {
  if (pid_fd >= 0) {
    unlink (pid_filename);
    close (pid_fd);
  }
}
/******************************************************************************/

