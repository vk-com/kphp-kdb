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

#include <assert.h>
#include <stdio.h>
#include <stddef.h>
#include <stdlib.h>
#include <string.h>
#include <fcntl.h>
#include <sys/types.h>
#include <signal.h>
#include <zlib.h>

#include "kfs.h"
#include "kdb-data-common.h"
#include "server-functions.h"
#include "kdb-filesys-binlog.h"
#include "net-crypto-aes.h"
#include "filesys-utils.h"
#include "diff-patch.h"
#include "filesys-pending-operations.h"

#define mytime() (get_utime(CLOCK_MONOTONIC))

#define	VERSION_STR	"filesys-commit-changes-0.10"
const char FullVersionStr[] = VERSION_STR " compiled at " __DATE__ " " __TIME__ " by gcc " __VERSION__ " "
#ifdef __LP64__
  "64-bit"
#else
  "32-bit"
#endif
;

int verbosity = 0;
int status = 0;
static int compress_level = 9, use_diff = 1, use_clone = 0, unlink_previous_snapshot = 0;
static char *recent_snapshort_name = NULL;
static double patch_timeout = 10.0;

static char old_dir[PATH_MAX], new_dir[PATH_MAX];
static int old_dir_length, new_dir_length, max_name_length;

static int transaction_id = 0, transaction_file_no = 0;
static long long transaction_log_pos;

static double safe_div (double x, double y) { return y > 0 ? x/y : 0; }

static int append_path (char *s, int l, const char *filename) {
  int o = PATH_MAX - l;
  assert (o > 0);
  int r = snprintf (s + l, o, "/%s", filename);
  if (r >= o - 1) {
    kprintf ("append_path fail (buffer overflow)\n");
    exit (1);
  }
  return l + r;
}

static char tmp_dir[PATH_MAX];
static long long all_files_bytes = 0, all_patch_bytes = 0;
static long long simple_files, gzipped_files, same_files, diff_files, symlinks, regulars, directories;
static long long events = 0, faults = 0, logevents_size = 0;

typedef struct memory_logevent {
  struct memory_logevent *next;
  int size;
  int data[0];
} memory_logevent_t;

memory_logevent_t *head_logevents, *tail_logevents;

static void *memory_alloc_log_event (lev_type_t type, int size) {
  size = (size + 3) & -4;
  memory_logevent_t *L = malloc (size + sizeof (memory_logevent_t));
  if (L == NULL) {
    kprintf ("Out of memory. Total allocated logevents data is %lld bytes.\n", logevents_size);
    exit (1);
  }
  logevents_size += size;
  events++;
  L->size = size;
  L->next = NULL;
  if (head_logevents == NULL) {
    head_logevents = tail_logevents = L;
  } else {
    tail_logevents->next = L;
    tail_logevents = L;
  }
  L->data[0] = type;
  return L->data;
}

static void *filesys_xfs_alloc_log_event (lev_type_t type, int size) {
  if (!events) {
    int *p = memory_alloc_log_event (LEV_FILESYS_XFS_BEGIN_TRANSACTION, 8);
    p[1] = transaction_id;
  }
  return memory_alloc_log_event (type, size);
}

void remove_file (int l, file_t *x) {
  char *local_path = old_dir + old_dir_length + 1;
  append_path (old_dir, l, x->filename);
  vkprintf (3, "Remove: %s\n", local_path);
  if (status) {
    printf ("Remove: %s\n", local_path);
  } else {
    char a[PATH_MAX];
    int r = 0;
    assert (snprintf (a, PATH_MAX, "%s/%s", tmp_dir, local_path) < PATH_MAX);
    if (use_clone) {
      r = delete_file (a);
    } else {
      pending_operation_push (pending_operation_create (pot_remove, NULL, local_path, NULL));
    }
    if (!r) {
      const int name_length = strlen (local_path);
      struct lev_filesys_rmdir *E = filesys_xfs_alloc_log_event (LEV_FILESYS_XFS_FILE_REMOVE, sizeof (struct lev_filesys_rmdir) + name_length);
      E->dirpath_size = name_length;
      memcpy (E->dirpath, local_path, name_length);
    } else {
      kprintf ("delete_file (%s/%s) failed (exit_code = %d).\n", tmp_dir, local_path, r);
      faults++;
    }
  }
  old_dir[l] = 0;
}

static int (*copy_file) (struct stat *S, char *name, unsigned char *buf, int l, int same) = NULL;

static struct static_pending_operation last_po = { .type = pot_null };

static int copy_file_po_mode (struct stat *S, char *name, unsigned char *buf, int l, int same) {
  if (same) {
    pending_operation_fill (&last_po, pot_copy_attrs, NULL, name, S);
  } else {
    pending_operation_copyfile (transaction_id, &transaction_file_no, name, buf, l, S, &last_po, NULL);
  }
  return 0;
}

static int copy_file_cloning_mode (struct stat *S, char *name, unsigned char *buf, int l, int same) {
  char a[PATH_MAX];
  if (snprintf (a, PATH_MAX, "%s/%s", tmp_dir, name) >= PATH_MAX) {
    return -1;
  }
  if (!same) {
    if (S_ISLNK (S->st_mode)) {
      unlink (a);
      if (symlink ((char *) buf, a) < 0) {
        vkprintf (1, "%m\n");
        return -7;
      }
    } else if (S_ISDIR (S->st_mode)) {
      if (mkdir (a, S->st_mode) < 0) {
        vkprintf (1, "mkdir (%s, %d) failed. %m\n", a, S->st_mode);
        return -6;
      }
    } else {
      const int flags = O_CREAT | O_TRUNC | O_WRONLY;
      int fd = open (a, flags, S->st_mode);
      if (fd < 0) {
        vkprintf (1, "open (%s, %d, %d) fail. %m\n", a, flags, S->st_mode);
        return -2;
      }
      int sz = write (fd, buf, l);
      if (sz != l) {
        vkprintf (1, "%d bytes of %d was written to %s. %m\n", sz, l, a);
        return -3;
      }
      if (close (fd)) {
        vkprintf (1, "closing %s failed. %m\n", a);
        return -4;
      }
    }
  }
  int r = lcopy_attrs (a, S);
  if (r < 0) {
    vkprintf (1, "lcopy_attrs returns error code %d.\n", r);
    return -5;
  }

  return 0;
}

typedef struct {
  int flags;
  unsigned old_crc32, patch_crc32, new_crc32;
  int old_file_length, patch_length, new_file_length;
  unsigned char *new_file_data, *old_file_data, *b;
} compress_file_t;

static int gzip_buff (unsigned char *a, int n, unsigned char **b, int *m) {
  *b = NULL;
  if (!compress_level) { return -1; }
  uLongf dest = compressBound (n);
  *b = ztmalloc (dest);
  if (compress2 (*b, &dest, a, n, compress_level) != Z_OK) {
    return -2;
  }
  *m = dest;
  return 0;
}

static int compress_file (compress_file_t *C, file_t *x, file_t *y) {
  int r;
  memset (C, 0, sizeof (*C));
  if (get_file_content (new_dir, x, &C->new_file_data, &C->new_file_length)) {
    return -1;
  }
  vkprintf (4, "C->new_file_length = %d\n", C->new_file_length);
  if (C->new_file_data) {
    C->new_crc32 = compute_crc32 (C->new_file_data, C->new_file_length);
  } else {
    return 0;
  }
  if (y) {
    if (get_file_content (old_dir, y, &C->old_file_data, &C->old_file_length)) {
      return -2;
    }
    if (C->old_file_data) {
      C->old_crc32 = compute_crc32 (C->old_file_data, C->old_file_length);
    }
  }
  vkprintf (4, "C->old_file_data = %p\n", C->old_file_data);
  C->patch_length = C->new_file_length;
  C->b = C->new_file_data;
  if (C->old_crc32 == C->new_crc32 && C->old_file_length == C->new_file_length && C->old_file_data && C->new_file_data && !memcmp (C->old_file_data, C->new_file_data, C->new_file_length)) {
    C->patch_length = 0;
    C->flags = XFS_FILE_FLAG_SAME;
    return 0;
  }
  unsigned char *d;

  int l;
  if (!gzip_buff (C->new_file_data, C->new_file_length, &d, &l) && l < C->patch_length) {
    C->b = d;
    C->patch_length = l;
    C->flags = XFS_FILE_FLAG_GZIP;
  }

  if (use_diff && C->new_file_data && C->old_file_data) {
    d = ztmalloc (C->patch_length);
    r = vk_diff (C->old_file_data, C->old_file_length, C->new_file_data, C->new_file_length, d, C->patch_length, compress_level, patch_timeout);
    if (r >= 0) {
      if (C->patch_length > r) {
        C->patch_length = r;
        C->b = d;
        C->flags = XFS_FILE_FLAG_DIFF;
      }
    } else {
      vkprintf (3, "vk_diff returns error code %d.\n", r);
    }
  }

  C->patch_crc32 = C->flags & (XFS_FILE_FLAG_GZIP|XFS_FILE_FLAG_DIFF) ? compute_crc32 (C->b, C->patch_length) : C->new_crc32;
  return 0;
}

static void incr_file_mode_stat (file_t *x) {
  if (S_ISLNK (x->st.st_mode)) {
    symlinks++;
  } else if (S_ISDIR (x->st.st_mode)) {
    directories++;
  } else {
    regulars++;
  }
}

static int alloc_file_logevent_and_copy (file_t *x, file_t *old) {
  compress_file_t C;
  int r = compress_file (&C, x, old);
  if (r < 0) {
    vkprintf (1, "compress_file (x->filename=%s, old=%p) returns error code %d.\n", x->filename, old, r);
    return -1;
  }
  switch (C.flags) {
    case 0:
      simple_files++;
      break;
    case XFS_FILE_FLAG_GZIP:
      gzipped_files++;
      break;
    case XFS_FILE_FLAG_SAME:
      same_files++;
      break;
    case XFS_FILE_FLAG_DIFF:
      diff_files++;
      break;
    default:
      assert (0);
  }

  char *name = new_dir + new_dir_length + 1;
  const int name_length = strlen (name);
  if (max_name_length < name_length) {
    max_name_length = name_length;
  }

  r = copy_file (&x->st, name, C.new_file_data, C.new_file_length, (C.flags & XFS_FILE_FLAG_SAME) ? 1 : 0); /* new inode should be created */
  if (r < 0) {
    vkprintf (1, "copy_file (%s) returns error code %d.\n", name, r);
    return -2;
  }

  incr_file_mode_stat (x);

  if (C.flags == XFS_FILE_FLAG_SAME) {
    struct lev_filesys_xfs_change_attrs *E = filesys_xfs_alloc_log_event (LEV_FILESYS_XFS_CHANGE_ATTRS, sizeof (struct lev_filesys_xfs_change_attrs) + name_length);
    E->mode = x->st.st_mode;
    E->actime = x->st.st_atime;
    E->modtime = x->st.st_mtime;
    E->uid = x->st.st_uid;
    E->gid = x->st.st_gid;
    E->filename_size = name_length;
    memcpy (E->filename, name, name_length);
  } else {
    const int M = C.patch_length;
    all_files_bytes += C.new_file_length;
    all_patch_bytes += M;
    vkprintf (2, "Add file %s\n", name);
    vkprintf (4, "C.flags = %x\n", C.flags);
    struct lev_filesys_xfs_file *E = filesys_xfs_alloc_log_event (LEV_FILESYS_XFS_FILE + C.flags, sizeof (struct lev_filesys_xfs_file) + name_length);
    E->old_size = C.old_file_length;
    E->patch_size = C.patch_length;
    E->new_size = C.new_file_length;
    E->mode = x->st.st_mode;
    E->actime = x->st.st_atime;
    E->modtime = x->st.st_mtime;
    E->uid = x->st.st_uid;
    E->gid = x->st.st_gid;
    E->filename_size = name_length;
    int parts = (M >> 16) + ((M & 0xffff) ? 1 : 0);
    E->parts = parts;
    E->old_crc32 = C.old_crc32;
    E->patch_crc32 = C.patch_crc32;
    E->new_crc32 = C.new_crc32;
    memcpy (E->filename, name, name_length);
    int i, part = 0;
    for (i = 0; i < M; i += 0x10000, part++) {
      int j = M - i;
      if (j > 0x10000) { j = 0x10000; }
      struct lev_filesys_xfs_file_chunk *F = filesys_xfs_alloc_log_event (LEV_FILESYS_XFS_FILE_CHUNK, sizeof (struct lev_filesys_xfs_file_chunk) + j);
      F->part = i >> 16;
      F->size = j - 1;
      memcpy (F->data, C.b + i, j);
    }
    assert (parts == part);
  }
  return 0;
}

static void print_attrib (const char *const attrib_name, char *ch) {
  fputc (*ch, stdout);
  printf ("%s", attrib_name);
  *ch = '|';
}

static void print_attrs (int attrib_mask) {
  if (attrib_mask >= 0) {
    char ch = '[';
    if (attrib_mask & 1) { print_attrib ("mode", &ch); }
    if (attrib_mask & 2) { print_attrib ("uid", &ch); }
    if (attrib_mask & 4) { print_attrib ("gid", &ch); }
    if (attrib_mask & 8) { print_attrib ("size", &ch); }
    if (attrib_mask & 16) { print_attrib ("mtime", &ch); }
    if (ch == '[') {
      fputc (ch, stdout);
    }
    fputc (']', stdout);
  }
  fputc ('\n', stdout);
}

static int change_attrs (int new_l, file_t *x, int attrib_mask) {
  append_path (new_dir, new_l, x->filename);
  if (status) {
    printf ("New attrs: %s ", new_dir + new_dir_length + 1);
    print_attrs (attrib_mask);
  } else {
    char *name = new_dir + new_dir_length + 1;
    const int name_length = strlen (name);
    if (use_clone) {
      char a[PATH_MAX];
      assert (snprintf (a, PATH_MAX, "%s/%s", tmp_dir, name) < PATH_MAX);
      int r = lcopy_attrs (a, &x->st);
      if (r < 0) {
        vkprintf (1, "lcopy_attrs returns error code %d.\n", r);
        return -2;
      }
    } else {
      pending_operation_push (pending_operation_create (pot_copy_attrs, NULL, name, &x->st));
    }

    if (attrib_mask >= 0) {
      incr_file_mode_stat (x);
    }
    struct lev_filesys_xfs_change_attrs *E = filesys_xfs_alloc_log_event (LEV_FILESYS_XFS_CHANGE_ATTRS, sizeof (struct lev_filesys_xfs_change_attrs) + name_length);
    E->mode = x->st.st_mode;
    E->actime = x->st.st_atime;
    E->modtime = x->st.st_mtime;
    E->uid = x->st.st_uid;
    E->gid = x->st.st_gid;
    E->filename_size = name_length;
    memcpy (E->filename, name, name_length);
  }
  new_dir[new_l] = 0;
  return 0;
}

static void add_file (int new_l, file_t *x, file_t *old, int attrib_mask) {
  const int old_l = new_l - new_dir_length + old_dir_length, o = append_path (new_dir, new_l, x->filename);
  append_path (old_dir, old_l, x->filename);
  vkprintf (3, "Add: %s\n", new_dir);
  if (status) {
    if (access (new_dir, R_OK) < 0) {
      kprintf ("reading access for the file '%s' failed. %m\n", new_dir);
      exit (1);
    }
    if (old != NULL) {
      printf ("Replace: %s ", new_dir + new_dir_length + 1);
      print_attrs (attrib_mask);
    } else {
      printf ("Add: %s\n", new_dir + new_dir_length + 1);
    }
  } else {
    dyn_mark_t mrk;
    dyn_mark (mrk);
    int r = alloc_file_logevent_and_copy (x, old);
    dyn_release (mrk);
    if (r < 0) {
      kprintf ("alloc_file_logevent_and_copy (%s, %s) returns error code %d.\n", new_dir, x->filename, r);
      faults++;
    }
    if (!use_clone && last_po.type != pot_null) {
      pending_operation_push (pending_operation_load (&last_po));
      last_po.type = pot_null;
    }
  }
  if (S_ISDIR (x->st.st_mode) && !S_ISLNK (x->st.st_mode)) {
    vkprintf (3, "Scanning %s\n", new_dir);
    file_t *py, *y;
    getdir (new_dir, &py, 0, 1);
    y = py;
    while (y != NULL) {
      add_file (o, y, NULL, 0);
      y = y->next;
    }
    free_filelist (py);
    change_attrs (new_l, x, -1); /* fix directory mtime */
  }
  old_dir[old_l] = 0;
  new_dir[new_l] = 0;
}

static int get_changed_attrs (struct stat *a, struct stat *b) {
  int attrib_mask = 0;
  if (a->st_mode != b->st_mode) {
    attrib_mask |= 1;
  }
  if (a->st_uid != b->st_uid) {
    attrib_mask |= 2;
  }
  if (a->st_gid != b->st_gid) {
    attrib_mask |= 4;
  }
  if (a->st_size != b->st_size) {
    attrib_mask |= 8;
  }
  if (a->st_mtime != b->st_mtime) {
    attrib_mask |= 16;
  }
  return attrib_mask;
}

static char *scan_ignore_list[4] = {".binlogpos", ".filesys-xfs-tmp", ".filesys-xfs-engine.pid", NULL};

static void rec_scan (int l1, int l2, int *changed) {
  vkprintf (3, "rec_scan (old_dir = %s, new_dir = %s)\n", old_dir, new_dir);
  *changed = 0;
  int r;
  file_t *x, *y, *px, *py;
  int nx = getdir (old_dir, &px, 1, 1);
  int ny = getdir (new_dir, &py, 1, 1);
  vkprintf (3, "l1 = %d, l2 = %d, nx = %d, ny = %d, px = %p, py = %p\n", l1, l2, nx, ny, px, py);

  if (l1 == old_dir_length) {
    for (r = 0; scan_ignore_list[r] != NULL; r++) {
      px = remove_file_from_list (px, scan_ignore_list[r]);
      py = remove_file_from_list (py, scan_ignore_list[r]);
    }
  }

  x = px;
  y = py;
  while (x != NULL && y != NULL) {
    vkprintf (4, "x->filename = %s, y->filename = %s\n", x->filename, y->filename);
    int c = strcmp (x->filename, y->filename);
    if (c < 0) {
      *changed = 1;
      remove_file (l1, x);
      x = x->next;
    } else if (c > 0) {
      *changed = 1;
      add_file (l2, y, NULL, 0);
      y = y->next;
    } else {
      int dx = S_ISDIR (x->st.st_mode) && !S_ISLNK (x->st.st_mode);
      int dy = S_ISDIR (y->st.st_mode) && !S_ISLNK (y->st.st_mode);
      if (dx) {
        if (dy) {
          int changed;
          rec_scan (append_path (old_dir, l1, x->filename), append_path (new_dir, l2, y->filename), &changed);
          int attrib_mask = get_changed_attrs (&x->st, &y->st);
          if ((attrib_mask & ~8) || changed) {
            r = change_attrs (l2, y, attrib_mask);
            if (r < 0) {
              kprintf ("change_attrs (%s/%s) returns error code %d.\n", new_dir, y->filename, r);
              faults++;
            }
          }
        } else {
          *changed = 1;
          remove_file (l1, x);
          add_file (l2, y, NULL, 0);
        }
      } else {
        if (dy) {
          *changed = 1;
          remove_file (l1, x);
          add_file (l2, y, NULL, 0);
        } else {
          int attrib_mask = get_changed_attrs (&x->st, &y->st);
          if (attrib_mask) {
            *changed = 1;
            add_file (l2, y, x, attrib_mask);
          }
        }
      }
      x = x->next;
      y = y->next;
    }
  }

  if (x != NULL || y != NULL) {
    *changed = 1;
  }

  while (x != NULL) {
    remove_file (l1, x);
    x = x->next;
  }
  while (y != NULL) {
    add_file (l2, y, NULL, 0);
    y = y->next;
  }

  old_dir[l1] = 0;
  new_dir[l2] = 0;
  free_filelist (px);
  free_filelist (py);
  vkprintf (3, "rec_scan (%d, %d) succesfully ended.\n", l1, l2);
}

static void do_transaction_begin () {
  transaction_log_pos = log_cur_pos ();
  transaction_id++;
  vkprintf (1, "Begin transaction Ox%X\n", transaction_id);
}

static void do_transaction_end () {
  if (!events) {
    kprintf ("Transaction didn't contain any event. Nothing output to the binlog.\n");
    return;
  }
  int *p = memory_alloc_log_event (LEV_FILESYS_XFS_END_TRANSACTION, 8);
  p[1] = transaction_id;
  vkprintf (1, "Transaction contains %lld events.\n", events);
}

static void rstrip_slash (char *s) {
  if (s != NULL && *s) {
    char *p = s + strlen (s) - 1;
    if (*p == '/') {
      *p = 0;
    }
  }
}

static int compute_tmp_dir_name (const char *const old) {
  char a[PATH_MAX];
  if (status) {
    tmp_dir[0] = 0;
    return 0;
  }
  if (tmp_dir[0]) {
    if (snprintf (a, PATH_MAX, "%s/.%lld_%u_%lld", tmp_dir, (long long) getpid (), (unsigned) time (0), log_cur_pos ()) >= PATH_MAX) {
      return -1;
    }
  } else {
    char *p = strrchr (old, '/');
    if (p == NULL) {
      if (snprintf (a, PATH_MAX, ".%lld_%u_%lld", (long long) getpid (), (unsigned) time (0), log_cur_pos ()) >= PATH_MAX) {
        return -1;
      }
      return 0;
    }
    if (snprintf (a, PATH_MAX, "%.*s/.%lld_%u_%lld",  (int) (p - old), old, (long long) getpid (), (unsigned) time (0), log_cur_pos ()) >= PATH_MAX) {
      return -1;
    }
  }
  strcpy (tmp_dir, a);
  return 0;
}

static int scan (const char *const old, const char *const new) {
  faults = 0;
  if (!status) {
    if (compute_tmp_dir_name (old) < 0) {
      kprintf ("compute_tmp_dir_name fail.\n");
      exit (1);
    }
    vkprintf (1, "Temporary directory name: %s\n", tmp_dir);
    if (use_clone) {
      double t = -mytime ();
      int r = clone_file (old, tmp_dir);
      if (r < 0) {
        kprintf ("clone_file (%s, %s) returns error code %d.\n", old, tmp_dir, r);
        exit (1);
      }
      t += mytime ();
      vkprintf (1, "Cloning master copy time = %.6lf seconds.\n", t);
      copy_file = copy_file_cloning_mode;
    } else {
      pending_operations_init (tmp_dir, old);
      pending_operations_reset (0);
      if (mkdir (tmp_dir, 0770) < 0) {
        kprintf ("mkdir (%s, 0770) failed. %m\n", tmp_dir);
        exit (1);
      }
      copy_file = copy_file_po_mode;
    }
    do_transaction_begin ();
  }
  old_dir_length = strlen (old);
  assert (old_dir_length < PATH_MAX-1);
  strcpy (old_dir, old);
  new_dir_length = strlen (new);
  assert (new_dir_length < PATH_MAX-1);
  strcpy (new_dir, new);
  int changed;
  rec_scan (old_dir_length, new_dir_length, &changed);

  if (!status) {
    do_transaction_end ();
    if (!faults) {
      if (use_clone) {
        int r = delete_file (old);
        if (r) {
          kprintf ("delete_file (%s) returns error code %d.\n", old, r);
          return -1;
        }
        r = rename (tmp_dir, old);
        if (r) {
          kprintf ("rename (%s, %s) returns error code %d.\n", tmp_dir, old, r);
          return -2;
        }
      } else {
        pending_operations_apply ();
        if (rmdir (tmp_dir) < 0) {
          kprintf ("rmdir (%s) failed. %m\n", tmp_dir);
        }
      }
    }
  }
  return (faults == 0 && (status || events > 0)) ? 0 : -1;
}

long long volume_id;

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

static int filesys_xfs_replay_logevent (struct lev_generic *E, int size) {
  vkprintf (3, "LE %x\n", E->type);
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
    case LEV_FILESYS_XFS_END_TRANSACTION:
      if (size < 8) { return -2;}
      transaction_id = E->a;
      return 8;
    case LEV_FILESYS_XFS_FILE:
    case LEV_FILESYS_XFS_FILE+XFS_FILE_FLAG_GZIP:
    case LEV_FILESYS_XFS_FILE+XFS_FILE_FLAG_DIFF:
      if (size < sizeof (struct lev_filesys_xfs_file)) {
        return -2;
      }
      s = sizeof (struct lev_filesys_xfs_file) + ((struct lev_filesys_xfs_file *) E)->filename_size;
      return (size < s) ? -2 : s;
    case LEV_FILESYS_XFS_FILE_CHUNK:
      if (size < sizeof (struct lev_filesys_xfs_file_chunk)) {
        return -2;
      }
      s = (sizeof (struct lev_filesys_xfs_file_chunk) + 1) + ((struct lev_filesys_xfs_file_chunk *) E)->size;
      return (size < s) ? -2 : s;
    case LEV_FILESYS_XFS_FILE_REMOVE:
      if (size < sizeof (struct lev_filesys_rmdir)) {
        return -2;
      }
      s = sizeof (struct lev_filesys_rmdir) + ((struct lev_filesys_rmdir *) E)->dirpath_size;
      return (size < s) ? -2 : s;
    case LEV_FILESYS_XFS_CHANGE_ATTRS:
      if (size < sizeof (struct lev_filesys_xfs_change_attrs)) {
        return -2;
      }
      s = sizeof (struct lev_filesys_xfs_change_attrs) + ((struct lev_filesys_xfs_change_attrs *) E)->filename_size;
      return (size < s) ? -2 : s;
  }

  kprintf ("unknown log event type %08x at position %lld\n", E->type, log_cur_pos());

  return -3;
}

/************************************* index *****************************************************/
#define FILESYS_XFS_FAKE_INDEX_MAGIC 0xb34a03ec
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
  int transaction_id;

  unsigned header_crc32;
} index_header;

long long jump_log_pos;
int jump_log_ts;
unsigned jump_log_crc32;

int load_index (kfs_file_handle_t Index) {
  if (Index == NULL) {
    return -1;
  }
  int idx_fd = Index->fd;
  index_header header;
  if (read (idx_fd, &header, sizeof (index_header)) != sizeof (index_header)) {
    kprintf ("read fail. %m\n");
    return -1;
  }

  if (header.magic != FILESYS_XFS_FAKE_INDEX_MAGIC) {
    kprintf ("index file is not for filesys-xfs-engine\n");
    return -1;
  }

  if (header.header_crc32 != compute_crc32 (&header, offsetof (index_header, header_crc32))) {
    kprintf ("CRC32 fail.\n");
    return -1;
  }

  //last_log_pos = header.log_pos1;

  jump_log_pos = header.log_pos1;
  jump_log_crc32 = header.log_pos1_crc32;
  jump_log_ts = header.log_timestamp;
  transaction_id = header.transaction_id;
  recent_snapshort_name = zstrdup (Index->info->filename);
  return 0;
}

int save_index (int writing_binlog) {
  char *newidxname = NULL;

  if (log_cur_pos() == jump_log_pos) {
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


  vkprintf (1, "creating index %s at log position %lld\n", newidxname, log_cur_pos());

  int newidx_fd = open (newidxname, O_CREAT | O_TRUNC | O_WRONLY | O_EXCL, 0660);

  if (newidx_fd < 0) {
    kprintf ("cannot create new index file %s: %m\n", newidxname);
    exit (1);
  }

  index_header header;
  memset (&header, 0, sizeof (header));
  header.magic = FILESYS_XFS_FAKE_INDEX_MAGIC;
  header.created_at = time (NULL);
  header.log_timestamp = log_read_until;
  header.log_pos1 = log_cur_pos ();
  if (writing_binlog) {
    relax_write_log_crc32 ();
  } else {
    relax_log_crc32 (0);
  }
  header.log_pos1_crc32 = ~log_crc32_complement;
  header.transaction_id = transaction_id;
  header.header_crc32 = compute_crc32 (&header, offsetof (index_header, header_crc32));
  assert (write (newidx_fd, &header, sizeof (header)) == sizeof (header));
  assert (!fsync (newidx_fd));
  assert (!close (newidx_fd));

  vkprintf (3, "index written ok\n");

  if (rename_temporary_snapshot (newidxname)) {
    kprintf ("cannot rename new index file from %s: %m\n", newidxname);
    unlink (newidxname);
    exit (1);
  }

  print_snapshot_name (newidxname);
  if (unlink_previous_snapshot && recent_snapshort_name) {
    unlink (recent_snapshort_name);
    vkprintf (1, "unlinked %s", recent_snapshort_name);
  }

  return 0;
}

static void sigabrt_handler (const int sig) {
  kprintf ("SIGABRT caught, terminating program\n");
  print_backtrace ();
  unlock_pid_file ();
  exit (EXIT_FAILURE);
}


/*
 *
 *		MAIN
 *
 */


void usage (void) {
  fprintf (stderr, "%s\n", FullVersionStr);
  fprintf (stderr, "./filesys-commit-changes [-v] [-t] [-T<temp-dir>] [-P] <master-dir> <new-dir> <binlog>\n"
                   "\t-0\tno gzip compression\n"
                   "\t-9\thighest gzip compression level\n"
                   "\t-P\tdisable diff/patch\n"
                   "\t-v\tincrease verbosity level\n"
                   "\t-t\tonly show diff, nothing append to binlog\n"
                   "\t-C\tuse cloning for synchronization master-dir (slower, but safer)\n"
                   "\t-T<temp-dir>\tshould be in the same filesystem as <master-dir>, by default is equal to <master-dir>/..\n"
                   "\t-U\tunlink previous fake snapshot\n"
         );
  exit (2);
}

static void test_dir_exist (const char *const path) {
  struct stat buf;
  if (stat (path, &buf) || !S_ISDIR (buf.st_mode)) {
    kprintf ("Directory %s doesn't exist\n", path);
    exit (1);
  }
}

static int filesys_preload_filelist (const char *main_replica_name) {
  int l = strlen (main_replica_name);
  engine_replica_name = strdup (main_replica_name);
  engine_snapshot_replica_name = malloc (l + 6);
  strcpy (engine_snapshot_replica_name, main_replica_name);
  strcpy (engine_snapshot_replica_name + l, "-fake");
  assert (engine_replica_name && engine_snapshot_replica_name);
  engine_replica = open_replica (engine_replica_name, 0);
  if (!engine_replica) {
    return -1;
  }
  engine_snapshot_replica = open_replica (engine_snapshot_replica_name, 1);
  if (!engine_snapshot_replica) {
    return 0;
  }
  return 1;
}

int main (int argc, char *argv[]) {
  int i;
  long long x;
  char c;
  set_debug_handlers ();
  tmp_dir[0] = 0;
  while ((i = getopt (argc, argv, "0123456789tvu:PT:CUB:")) != -1) {
    switch (i) {
    case 'U':
      unlink_previous_snapshot = 1;
      break;
    case 'C':
      use_clone = 1;
      break;
    case 'T':
      if (strlen (optarg) < PATH_MAX) {
        strcpy (tmp_dir, optarg);
        rstrip_slash (tmp_dir);
      }
      break;
    case 'P':
      use_diff = 0;
      break;
    case '0'...'9':
      compress_level = i - '0';
      break;
    case 't':
      status = 1;
      break;
    case 'v':
      verbosity++;
      break;
    case 'u':
      username = optarg;
      break;
    case 'B':
      c = 0;
      assert (sscanf (optarg, "%lld%c", &x, &c) >= 1);
      switch (c | 0x20) {
      case 'k':  x <<= 10; break;
      case 'm':  x <<= 20; break;
      case 'g':  x <<= 30; break;
      case 't':  x <<= 40; break;
      default: assert (c == 0x20);
      }
      if (i == 'B' && x >= 1024 && x < (1LL << 60)) {
        max_binlog_size = x;
      }
      break;
    }
  }

  if (optind + 2 > argc) {
    usage ();
    exit (2);
  }

  if (status) {
    binlog_disabled = 1;
  }

  init_dyn_data ();
  aes_load_pwd_file (NULL);

  if (change_user (username) < 0) {
    kprintf ("fatal: cannot change user to %s\n", username ? username : "(none)");
    exit (1);
  }

  vkprintf (3, "optind = %d, argc = %d\n", optind, argc);

  rstrip_slash (argv[optind]);
  rstrip_slash (argv[optind+1]);
  test_dir_exist (argv[optind]);
  test_dir_exist (argv[optind+1]);
  vkprintf (3, "test_dir_exist check completed.\n");

  if (!status) {
    if (optind + 3 > argc) {
      usage ();
      exit (2);
    }
    if (lock_pid_file (argv[optind]) < 0) {
      exit (1);
    }
    signal (SIGABRT, sigabrt_handler);
    atexit (unlock_pid_file);

    if (filesys_preload_filelist (argv[optind+2]) < 0) {
      kprintf ("cannot open binlog files for %s\n", argv[optind+2]);
      exit (1);
    }

    vkprintf (3, "engine_preload_filelist done\n");

    Snapshot = open_recent_snapshot (engine_snapshot_replica);

    if (Snapshot) {
      engine_snapshot_name = Snapshot->info->filename;
      engine_snapshot_size = Snapshot->info->file_size;
      vkprintf (1, "load index file %s (size %lld)\n", engine_snapshot_name, engine_snapshot_size);
      i = load_index (Snapshot);
      if (i < 0) {
        kprintf ("load_index returned fail code %d. Skipping index.\n", i);
        jump_log_ts = 0;
        jump_log_pos = 0;
        jump_log_crc32 = 0;
      }
    } else {
      engine_snapshot_name = NULL;
      engine_snapshot_size = 0;
    }

    //Binlog reading
    double binlog_load_time = -mytime();
    Binlog = open_binlog (engine_replica, jump_log_pos);
    if (!Binlog) {
      kprintf ("fatal: cannot find binlog for %s, log position %lld\n", engine_replica->replica_prefix, jump_log_pos);
      exit (1);
    }

    binlogname = Binlog->info->filename;

    clear_log ();

    init_log_data (jump_log_pos, jump_log_ts, jump_log_crc32);

    replay_logevent =  filesys_xfs_replay_logevent;
    i = replay_log (0, 1);

    if (i < 0) {
      kprintf ("replay_log returns %d.\n", i);
      exit (1);
    }

    binlog_load_time += mytime();
    //long long binlog_loaded_size = log_readto_pos - jump_log_pos;

    if (!binlog_disabled) {
      clear_read_log ();
    }

    clear_write_log ();
    start_time = time (0);

    if (binlogname && !binlog_disabled) {
      assert (append_to_binlog (Binlog) == log_readto_pos);
    }
  }

  if (scan (argv[optind], argv[optind+1]) < 0) {
    exit (1);
  }

  if (!status) {
    if (!geteuid () && change_user ("kitten") < 0) {
      kprintf ("fatal: fail to change user to kitten.\n");
      exit (1);
    }
    memory_logevent_t *p;
    for (p = head_logevents; p != NULL; p = p->next) {
      if (compute_uncommitted_log_bytes () > (1 << 23)) {
        flush_binlog_forced (0);
      }
      int *q = alloc_log_event (p->data[0], p->size, 0);
      memcpy (q, p->data, p->size);
    }
    flush_binlog_last ();
    sync_binlog (2);
    save_index (!binlog_disabled);
    vkprintf (1, "max_name_length = %d\n", max_name_length);
    vkprintf (1, "%lld simple_files, %lld gzipped_files, %lld same_files, %lld diff_files\n", simple_files, gzipped_files, same_files, diff_files);
    vkprintf (1, "Add/change %lld symlinks, %lld dirs, %lld regular files\n", symlinks, directories, regulars);
    vkprintf (1, "all_patch_bytes = %lld (%.3lf%%), all_files_bytes = %lld\n", all_patch_bytes, safe_div (100.0 * all_patch_bytes, all_files_bytes), all_files_bytes);
    long long extra_bytes = logevents_size - all_patch_bytes;
    vkprintf (1, "written %lld bytes to binlog, %lld (%.6lf%%) attributes, headers, etc. bytes  \n", logevents_size, extra_bytes, safe_div (100.0 * extra_bytes, logevents_size));
  }
  return 0;
}

