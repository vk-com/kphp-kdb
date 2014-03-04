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
#define _XOPEN_SOURCE 500

//#define DEBUG_SIMULATE_WRITE_BIT_ERROR

#include <stdio.h>
#include <stdlib.h>
#include <stddef.h>
#include <string.h>
#include <math.h>
#include <assert.h>
#include <unistd.h>
#include <fcntl.h>
#include <limits.h>
#include <aio.h>
#include <sys/stat.h>
#include <sys/statfs.h>
#include <sys/statvfs.h>
#include <errno.h>
#include <dirent.h>
#include <pthread.h>

#include "net-aio.h"
#include "md5.h"
#include "server-functions.h"
#include "kdb-data-common.h"
#include "kdb-storage-binlog.h"
#include "storage-data.h"

#define FILE_OFFSET_MASK 0x00FFFFFFFFFFFFFFULL

#define HASH_VOLUMES_MOD 0x2000

#define MIN_META_LIVING_TIME 10

int max_aio_connections_per_disk = 0;
int use_crc32_check = 1;
int wronly_binlogs_closed;

/*********************** Thread-safe zmalloc *******************************/

/* thread safe version of zmalloc was replaced by malloc call,
   since it is impossible to lock mutex for zfree call in check_aio_completion (net/net-aio.c)
*/

void *tszmalloc (long size) {
  void *res = malloc (size);
  assert (res);
  return res;
}

void *tszmalloc0 (long size) {
  void *res = calloc (size, 1);
  assert (res);
  return res;
}

void tszfree (void *ptr, long size) {
  free (ptr);
}

/************************************** Dirty binlogs queue ****************************************/
static pthread_mutex_t mutex_dirty_binlog_queue = PTHREAD_MUTEX_INITIALIZER;
storage_binlog_file_t *dirty_binlog_queue_head, *dirty_binlog_queue_tail;

void dirty_binlog_queue_push (storage_binlog_file_t *B) {
  if (B->dirty) {
    return;
  }
  B->dirty = 1;
  pthread_mutex_lock (&mutex_dirty_binlog_queue);
  B->fsync_next = NULL;
  if (dirty_binlog_queue_head == NULL) {
    dirty_binlog_queue_head = dirty_binlog_queue_tail = B;
  } else {
    dirty_binlog_queue_tail = dirty_binlog_queue_tail->fsync_next = B;
  }
  pthread_mutex_unlock (&mutex_dirty_binlog_queue);
}

storage_binlog_file_t *dirty_binlog_queue_pop (void) {
  storage_binlog_file_t *B = NULL;
  pthread_mutex_lock (&mutex_dirty_binlog_queue);
  if (dirty_binlog_queue_head) {
    B = dirty_binlog_queue_head;
    dirty_binlog_queue_head = dirty_binlog_queue_head->fsync_next;
    B->dirty = 0;
    B->fsync_next = NULL;
  }
  pthread_mutex_unlock (&mutex_dirty_binlog_queue);
  return B;
}

/*
void storage_mutex_init (void) {
  pthread_mutex_init (&mutex_tzmalloc, NULL);
  pthread_mutex_init (&mutex_dirty_binlog_queue, NULL);
}
*/

/********************** Bad read image cache ******************************/
#define BAD_IMAGE_CACHE_PRIME 249989
struct bad_image_cache_entry {
  unsigned long long binlog_file_id;
  long long offset;
  int timeout;
  int cached_time;
};

const int bad_image_cache_min_living_time = 60;
int bad_image_cache_max_living_time = 3600;

static struct bad_image_cache_entry CBI[BAD_IMAGE_CACHE_PRIME];

static int bad_image_cache_probe (storage_binlog_file_t *B, long long offset) {
  int idx = (B->binlog_file_id ^ (unsigned long long) offset) % BAD_IMAGE_CACHE_PRIME;
  assert (idx >= 0 && idx < BAD_IMAGE_CACHE_PRIME);
  struct bad_image_cache_entry *p = &CBI[idx];
  return p->binlog_file_id == B->binlog_file_id && p->offset == offset && now <= p->timeout;
}

static void bad_image_cache_store (storage_binlog_file_t *B, metafile_t *meta) {
  int idx = (B->binlog_file_id ^ (unsigned long long) meta->offset) % BAD_IMAGE_CACHE_PRIME;
  assert (idx >= 0 && idx < BAD_IMAGE_CACHE_PRIME);
  struct bad_image_cache_entry *p = &CBI[idx];
/*
  if (meta->crc32_error) {
    p->binlog_file_id = B->binlog_file_id;
    p->offset = meta->offset;
    p->timeout = INT_MAX;
    p->cached_time = bad_image_cache_max_living_time;
    return;
  }
*/
  if (meta->cancelled) {
    if (p->binlog_file_id == B->binlog_file_id && p->offset == meta->offset) {
      p->cached_time <<= 1;
      if (p->cached_time > bad_image_cache_max_living_time) {
        p->cached_time = bad_image_cache_max_living_time;
      }
    } else {
      p->cached_time = bad_image_cache_min_living_time;
    }
  } else {
    p->cached_time = bad_image_cache_max_living_time;
  }
  p->binlog_file_id = B->binlog_file_id;
  p->offset = meta->offset;
  p->timeout = now + p->cached_time;
}
/****************************************************************************/

int dirs;
storage_dir_t Dirs[MAX_DIRS];
long long statvfs_calls = 0;

int get_dir_id_by_name (const char *const dirname) {
  int i;
  for (i = 0; i < dirs; i++) {
    if (!strcmp (Dirs[i].path, dirname)) {
      return i;
    }
  }
  return -1;
}

int check_dir_size (storage_dir_t *D, double max_disk_usage, long long file_size) {
  if (!D->last_statvfs_time ||
       D->last_statvfs_time <= now - 3600 ||
       max_disk_usage * D->disk_total_bytes < (D->disk_total_bytes - D->disk_free_bytes) + file_size + (now - D->last_statvfs_time) * 200000000LL) {
    struct statvfs s;
    if (statvfs (D->path, &s) < 0) {
      vkprintf (2, "statvfs (\"%s\") fail. %m", D->path);
      return 0;
    }
    statvfs_calls++;
    D->last_statvfs_time = now;
    D->disk_total_bytes = (long long) s.f_frsize * (long long) s.f_blocks;
    D->disk_free_bytes = (long long) s.f_bsize * (long long) s.f_bavail;
    vkprintf (4, "path: %s, last_statvfs_time: %d, disk_total_bytes: %lld, disk_free_bytes: %lld\n", D->path, D->last_statvfs_time, D->disk_total_bytes, D->disk_free_bytes);
    return (max_disk_usage * D->disk_total_bytes >= (D->disk_total_bytes - D->disk_free_bytes) + file_size) ? 1 : 0;
  }
  return 1;
}

static volume_t *HP[HASH_VOLUMES_MOD];
volume_t **Volumes;

long long idx_docs = 0, tot_docs = 0;
long long snapshot_size = 0, index_size = 0;
int volumes = 0;

int storage_volume_check_file (volume_t *V, double max_disk_usage, long long file_size) {
  int k;
  for (k = 0; k < V->binlogs; k++) {
    if (V->B[k]->fd_wronly < 0) {
      return 0;
    }
  }
  for (k = 0; k < V->binlogs; k++) {
    if (!check_dir_size (&Dirs[V->B[k]->dir_id], max_disk_usage, file_size)) {
      return 0;
    }
  }
  return 1;
}

volume_t *get_volume_f (unsigned long long volume_id, int force) {
  unsigned int h1 = (unsigned int) volume_id % HASH_VOLUMES_MOD;
  if (HP[h1]) {
    if (HP[h1]->volume_id == volume_id) {
      return HP[h1];
    }
    if ( (h1 += 13) >= HASH_VOLUMES_MOD) {
      h1 -= HASH_VOLUMES_MOD;
    }
  }
  if (!force) {
    return NULL;
  }
  assert (volumes < MAX_VOLUMES);
  volumes++;
  volume_t *V = zmalloc0 (sizeof (volume_t));
  V->volume_id = volume_id;
  pthread_mutex_init (&V->mutex_write, NULL);
  pthread_mutex_init (&V->mutex_insert, NULL);
  return HP[h1] = V;
}

static int cmp_volumes (const void *x, const void *y) {
  const volume_t *a = *(const volume_t **) x;
  const volume_t *b = *(const volume_t **) y;
  if (a->volume_id < b->volume_id) {
    return -1;
  }
  if (a->volume_id > b->volume_id) {
    return 1;
  }
  return 0;
}

volume_t **generate_sorted_volumes_array (void) {
  int i, k = 0;
  Volumes = zmalloc0 (sizeof (Volumes[0]) * volumes);
  for (i = 0; i < HASH_VOLUMES_MOD; i++) {
    if (HP[i]) {
      Volumes[k++] = HP[i];
    }
  }
  assert (k == volumes);
  qsort (Volumes, volumes, sizeof (Volumes[0]), cmp_volumes);
  for (i = 1; i < volumes; i++) {
    assert (Volumes[i-1]->volume_id < Volumes[i]->volume_id);
  }
  return Volumes;
}

static int cmp_storage_binlog_file (const void *x, const void *y) {
  const storage_binlog_file_t *a = *(const storage_binlog_file_t **) x;
  const storage_binlog_file_t *b = *(const storage_binlog_file_t **) y;
  if (a->size > b->size) {
    return -1;
  }
  if (a->size < b->size) {
    return 1;
  }
  if (a->priority < b->priority) {
    return -1;
  }
  if (a->priority > b->priority) {
    return 1;
  }
  if (a->dir_id < b->dir_id) {
    return -1;
  }
  if (a->dir_id > b->dir_id) {
    return 1;
  }
  return 0;
}

#define PREFIX_IO_BUFFSIZE 32768

int equal_file_segment (storage_binlog_file_t *I1, storage_binlog_file_t *I2, long long off, int size) {
  assert (size >= 0 && size <= PREFIX_IO_BUFFSIZE);
  char a[PREFIX_IO_BUFFSIZE], b[PREFIX_IO_BUFFSIZE];
  struct aiocb cb[2];
  struct aiocb *pcb[2];
  char *names[2];
  names[0] = I1->abs_filename; names[1] = I2->abs_filename;
  memset (cb, 0, sizeof (cb));
  pcb[0] = cb;
  pcb[1] = cb + 1;
  cb[0].aio_lio_opcode = cb[1].aio_lio_opcode = LIO_READ;
  cb[0].aio_fildes = I1->fd_rdonly;
  cb[1].aio_fildes = I2->fd_rdonly;
  cb[0].aio_buf = a;
  cb[1].aio_buf = b;
  cb[0].aio_offset = cb[1].aio_offset = off;
  cb[0].aio_nbytes = cb[1].aio_nbytes = size;
  cb[0].aio_sigevent.sigev_notify = cb[1].aio_sigevent.sigev_notify = SIGEV_NONE;
  int i, r = 0;
  for (i = 0; ;i++) {
    errno = 0;
    r = lio_listio (LIO_WAIT, pcb, 2, NULL);
    if (!r) {
      break;
    }
    if (errno == EINTR) {
      assert (i < 10);
      continue;
    }
    if (errno != EIO) {
      kprintf ("%s (\"%s\", \"%s\", off: %lld, size: %d): call lio_listio failed. %m\n", __func__, names[0], names[1], off, size);
      exit (1);
    }
    break;
  }

  int w = 0;
  for (i = 0; i < 2; i++) {
    int res = aio_error (cb + i);
    assert (res != EINPROGRESS && res != ECANCELED);
    if (res) {
      kprintf ("read %d bytes from the file '%s' at offset %lld failed. %s\n", size, names[i], off, strerror (res));
      w = STORAGE_ERR_READ;
      assert (r);
    } else {
      res = aio_return (cb + i);
      if (res != size) {
        w = STORAGE_ERR_READ;
        kprintf ("read %d bytes of expected %d bytes from the file '%s' at offset %lld.\n", res, size, names[i], off);
      }
    }
  }

  if (w < 0) {
    return w;
  }

  if (memcmp (a, b, size)) {
    kprintf ("'%s' and '%s' are differ on the interval [%lld, %lld)\n", names[0], names[1], off, off + size);
    return STORAGE_ERR_DIFFER;
  }
  return 0;
}

int binlog_check (storage_binlog_file_t *I1, storage_binlog_file_t *I2) {
  if (I1->size != I2->size) {
    kprintf ("%s and %s has different size\n", I1->abs_filename, I2->abs_filename);
  }
  if (I1->size > I2->size) {
    storage_binlog_file_t *tmp = I1; I1 = I2; I2 = tmp;
  }
  int l = PREFIX_IO_BUFFSIZE;
  if (l > I1->size) {
    l = I1->size;
  }

  int r = equal_file_segment (I1, I2, 0, l);
  if (r < 0) {
    kprintf ("head 32Ki of '%s' and '%s' are differ.\n", I1->abs_filename, I2->abs_filename);
    return r;
  }

  if (I1->size > PREFIX_IO_BUFFSIZE) {
    r = equal_file_segment (I1, I2, I1->size - l, l);
    if (r < 0) {
      kprintf ("tail 32Ki of '%s' and '%s' are differ.\n", I1->abs_filename, I2->abs_filename);
      return r;
    }
  }
  return 0;
}

int read_volume_info (const char *const filename, long long *volume_id, long long *size, int *mtime) {
  int f = open (filename, O_RDONLY);
  if (f < 0) {
    kprintf ("%s: open (\"%s\", O_RDONLY) fail. %m\n", __func__, filename);
    return STORAGE_ERR_COULDN_OPEN_FILE;
  }
  struct stat buf;
  if (fstat (f, &buf) < 0) {
    close (f);
    return STORAGE_ERR_FSTAT;
  }
  *size = buf.st_size;
  *mtime = buf.st_mtime;
  char ebuf[LEV_STORAGE_START_SIZE];
  struct lev_start *E = (struct lev_start *) ebuf;
  int bytes_read = read (f, E, LEV_STORAGE_START_SIZE);
  close (f);
  if (LEV_STORAGE_START_SIZE != bytes_read) {
    kprintf ("read_volume_info: fail read LEV_START, bytes_read = %d. %m\n", bytes_read);
    return STORAGE_ERR_READ;
  }
  if (E->type != LEV_START) {
    kprintf ("read_volume_info: expected LEV_START but %x found.\n", E->type);
    return -1;
  }
  if (E->extra_bytes != (LEV_STORAGE_START_SIZE - 24)) {
    kprintf ("read_volume_info: illegal extra_bytes size = %d\n", E->extra_bytes);
    return -1;
  }
  unsigned char tmp[12];
  memcpy (&tmp[0], &E->str[0], 12);
  memcpy (volume_id, &tmp[0], 8);
  return 0;
}

int storage_enable_binlog_file (volume_t *V, int dir_id) {
  int k;
  for (k = 0; k < V->binlogs; k++) {
    storage_binlog_file_t *B = V->B[k];
    if (B->dir_id == dir_id) {
      if (B->fd_rdonly >= 0 || B->fd_wronly >= 0) {
        return -1;
      }
      int fd = open (B->abs_filename, O_RDONLY);
      if (fd < 0) {
        return STORAGE_ERR_OPEN;
      }
      struct stat buf;
      if (fstat (fd, &buf) < 0) {
        close (fd);
        return STORAGE_ERR_FSTAT;
      }
      if (binlog_disabled) {
        B->size = buf.st_size;
        B->mtime = buf.st_mtime;
        B->fd_rdonly = fd;
        return 0;
      }

      pthread_mutex_lock (&V->mutex_write);
      if (buf.st_size != V->cur_log_pos) {
        close (fd);
        pthread_mutex_unlock (&V->mutex_write);
        return STORAGE_ERR_SIZE_MISMATCH;
      }
      int i, o = 0, res = 0;
      for (i = 0; i < V->binlogs; i++) {
        if (V->B[i]->prefix && V->B[i]->size == V->cur_log_pos && V->B[i]->fd_rdonly >= 0) {
          o = 1;
          break;
        }
      }
      if (o) {
        unsigned char a[PREFIX_IO_BUFFSIZE], b[PREFIX_IO_BUFFSIZE];
        const int l = (PREFIX_IO_BUFFSIZE < buf.st_size) ? PREFIX_IO_BUFFSIZE : buf.st_size;
        if (l != pread (fd, a, l, V->cur_log_pos - l)) {
          res = STORAGE_ERR_READ;
        } else {
          for (i = 0; i < V->binlogs; i++) {
            if (V->B[i]->prefix && V->B[i]->size == V->cur_log_pos && V->B[i]->fd_rdonly >= 0) {
              if (l != pread (V->B[i]->fd_rdonly, b, l, V->cur_log_pos - l)) {
                res = STORAGE_ERR_READ;
                break;
              }
              if (memcmp (a, b, l)) {
                res = STORAGE_ERR_TAIL_DIFFER;
              }
              break;
            }
          }
        }
      }
      if (!res) {
        B->prefix = 1;
        B->size = buf.st_size;
        B->fd_rdonly = fd;
        B->fd_wronly = open (B->abs_filename, O_WRONLY);
        if (lock_whole_file (B->fd_wronly, F_WRLCK) <= 0) {
          close (B->fd_wronly);
          B->fd_wronly = -1;
        }
      }
      pthread_mutex_unlock (&V->mutex_write);
      return res;
    }
  }
  return STORAGE_ERR_DIR_NOT_FOUND;
}

int storage_close_binlog_file (volume_t *V, int dir_id) {
  int k;
  for (k = 0; k < V->binlogs; k++) {
    storage_binlog_file_t *B = V->B[k];
    if (B->dir_id == dir_id) {
      if (B->fd_rdonly >= 0) {
        close (B->fd_rdonly);
        B->fd_rdonly = -1;
      }
      pthread_mutex_lock (&V->mutex_write);
      if (B->fd_wronly >= 0) {
        close (B->fd_wronly);
        B->fd_wronly = -1;
      }
      pthread_mutex_unlock (&V->mutex_write);
      B->prefix = 0;
      B->size = -1;
      return 0;
    }
  }
  return -1;
}

/*
// V->mutex_write should be locked!
int storage_remove_binlog (volume_t *V, int dir_id) {
  int k;
  for (k = 0; k < V->binlogs; k++) {
    storage_binlog_file_t *B = V->B[k];
    if (B->dir_id == dir_id) {
      if (B->fd_rdonly >= 0) {
        close (B->fd_rdonly);
      }
      if (B->fd_wronly >= 0) {
        close (B->fd_wronly);
      }
      zfree (B->abs_filename, strlen (B->abs_filename) + 1);
      zfree (B, sizeof (*B));
      for (k++; k < V->binlogs; k++) {
        B[k-1] = B[k];
      }
      V->binlogs--;
      return 1;
    }
  }
  return 0;
}
*/

void storage_evaluate_priorities (volume_t *V) {
  int i, j, k, id[MAX_VOLUME_BINLOGS];
  for (i = 0; i < V->binlogs; i++) {
    id[i] = i;
  }
  for (i = 0; i < V->binlogs; i++) {
    for (j = i + 1; j < V->binlogs; j++) {
      if (V->B[id[i]]->dir_id > V->B[id[j]]->dir_id) {
        k = id[i]; id[i] = id[j]; id[j] = k;
      }
    }
  }
  for (k = 0; k < V->binlogs; k++) {
    V->B[id[k]]->priority = (V->volume_id + k) % (V->binlogs);
  }
}

int storage_add_binlog (const char *binlogname, int dir_id) {
  char real_filename_buf[PATH_MAX];
  char *abs_filename = realpath (binlogname, real_filename_buf);
  if (abs_filename == NULL) {
    kprintf ("absolute filename for binlog %s is too long. %m\n", binlogname);
    return STORAGE_ERR_PATH_TOO_LONG;
  }
  long long volume_id, size;
  int i, mtime;
  int r = read_volume_info (binlogname, &volume_id, &size, &mtime);
  if (r < 0) {
    kprintf ("read_volume_info (%s) return error code %d.\n", binlogname, r);
    return r;
  }
  volume_t *V = get_volume_f (volume_id, 1);
  for (i = 0; i < V->binlogs; i++) {
    if (!strcmp (abs_filename, V->B[i]->abs_filename)) {
      return i;
    }
  }
  if (V->binlogs == MAX_VOLUME_BINLOGS) {
    kprintf ("Found too many binlogs for volume_id = %lld (max binlogs = %d).\n", volume_id, MAX_VOLUME_BINLOGS);
    return STORAGE_ERR_TOO_MANY_BINLOGS;
  }

  for (i = 0; i < V->binlogs; i++) {
    if (V->B[i]->dir_id == dir_id) {
      kprintf ("More than one binlog file for volume_id = %lld in directory %s. Hint: your could change extention from .bin for something else for hiding old binlog files.\n", volume_id, Dirs[dir_id].path);
      return STORAGE_ERR_TOO_MANY_BINLOGS;
    }
  }

  pthread_mutex_lock (&V->mutex_write);
  storage_binlog_file_t *B = tszmalloc0 (sizeof (storage_binlog_file_t));
  V->B[V->binlogs] = B;
  B->volume_id = volume_id;
  B->dir_id = dir_id;
  B->binlog_file_id = dir_id;
  B->binlog_file_id <<= 56;
  B->binlog_file_id |= volume_id;
  B->abs_filename = strdup (abs_filename);
  B->size = size;
  B->mtime = mtime;
  B->fd_rdonly = B->fd_wronly = -1;
  V->binlogs++;
  pthread_mutex_unlock (&V->mutex_write);

  storage_evaluate_priorities (V);
  return V->binlogs - 1;
}

void storage_open_binlogs (volume_t *V) {
  int k;
  for (k = 0; k < V->binlogs; k++) {
    if (V->B[k]->fd_rdonly < 0) {
      V->B[k]->fd_rdonly = open (V->B[k]->abs_filename, O_RDONLY);
      if (V->B[k]->fd_rdonly < 0) {
        kprintf ("Couldn't open %s for reading. %m\n", V->B[k]->abs_filename);
        exit (1);
      }
    }
  }
}

void storage_reoder_binlog_files (volume_t *V) {
  int k;
  long long max_binlog_size = -1;
  for (k = 0; k < V->binlogs; k++) {
    if (max_binlog_size < V->B[k]->size) {
      max_binlog_size = V->B[k]->size;
    }
  }
  qsort (V->B, V->binlogs, sizeof (V->B[0]), cmp_storage_binlog_file);

  if (V->binlogs <= 0 || max_binlog_size != V->B[0]->size) {
    kprintf ("volume %lld: Can't select binlog.\n", V->volume_id);
    exit (1);
  }

  storage_open_binlogs (V);
  V->B[0]->prefix = 1;
  int i;
  for (i = 1; i < V->binlogs; i++) {
    if (!binlog_check (V->B[0], V->B[i])) {
      V->B[i]->prefix = 1;
    } else {
      V->B[i]->prefix = 0;
      close (V->B[i]->fd_rdonly);
      V->B[i]->fd_rdonly = -1;
    }
  }
}

void storage_open_replicas (void) {
  int i, c, d = 0;
  do {
    c = 0;
    for (i = 0; i < volumes; i++) {
      volume_t *V = Volumes[i];
      storage_binlog_file_t *B = V->B[0];
      if (B->dir_id > d) {
        c++;
      } else if (B->dir_id == d) {
        char p[PATH_MAX];
        int l = strlen (B->abs_filename);
        assert (l >= 4 && l < PATH_MAX);
        strcpy (p, B->abs_filename);
        p[l-4] = 0;
        vkprintf (2, "%s: call open_replica(%s)\n", __func__, p);
        V->engine_replica = open_replica (p, 0);
        V->engine_snapshot_replica = V->engine_replica;
        if (V->engine_replica == NULL) {
          kprintf ("volume %lld: Can't open binlog files for %s\n", V->volume_id, p);
          exit (1);
        }
      }
    }
    d++;
  } while (c);
}

/****************************** metafiles *********************************/
#define HASH_META_PRIME 99991
const int meta_header_size = sizeof (struct lev_storage_file) + offsetof (metafile_t, data);
metafile_t *M[HASH_META_PRIME];
int metafiles, metafiles_bytes, max_metafiles_bytes = 16 << 20;
long long tot_aio_loaded_bytes, metafiles_unloaded, metafiles_load_errors, metafiles_crc32_errors, metafiles_cancelled,
          choose_reading_binlog_errors;
static inline int get_meta_hash (long long volume_id, int local_id) {
  unsigned long long h = volume_id;
  h %= HASH_META_PRIME;
  h <<= 32;
  h |= local_id;
  return h % HASH_META_PRIME;
}

static metafile_t *get_meta_f (long long volume_id, int local_id, int *h, int force) {
  *h = get_meta_hash (volume_id, local_id);
  assert ((*h) >= 0 && (*h) < HASH_META_PRIME);
  metafile_t **p = M + (*h), *V;
  while (*p) {
    V = *p;
    if (V->B->volume_id == volume_id && V->local_id == local_id) {
      *p = V->hnext;
      if (force >= 0) {
        V->hnext = M[*h];
        M[*h] = V;
      }
      return V;
    }
    p = &V->hnext;
  }
  return NULL;
}

metafile_t lru_meta_lst[2] = {
  {.prev = &lru_meta_lst[0], .next = &lru_meta_lst[0]},
  {.prev = &lru_meta_lst[1], .next = &lru_meta_lst[1]}
};

int metafile_bucket_bytes[2];

static int get_meta_bucket (metafile_t *meta) {
  int r = 0;
  if (meta->size > 65536 + meta_header_size) {
    r++;
  }
  return r;
}

static void del_use (metafile_t *meta) {
  metafile_t *u = meta->prev, *v = meta->next;
  u->next = v;
  v->prev = u;
  meta->prev = meta->next = NULL;
  metafile_bucket_bytes[get_meta_bucket (meta)] -= meta->size;
  metafiles_bytes -= meta->size;
  metafiles--;
}

static void add_use (metafile_t *meta) {
  const int n = get_meta_bucket (meta);
  metafile_t *u = &lru_meta_lst[n], *v = lru_meta_lst[n].next;
  u->next = meta; meta->prev = u;
  v->prev = meta; meta->next = v;
  metafile_bucket_bytes[n] += meta->size;
  metafiles_bytes += meta->size;
  metafiles++;
}

static void reuse (metafile_t *meta) {
  del_use (meta);
  add_use (meta);
}

static void metafile_free (metafile_t *meta) {
  int h;
  vkprintf (3, "metafile_free (%p)\n", meta);
  del_use (meta);
  assert (get_meta_f (meta->B->volume_id, meta->local_id, &h, -1) != NULL);
  free (meta);
  metafiles_unloaded++;
}

static void unload_metafiles (int n) {
  int t = 0;
  metafile_t *p, *w;
  for (p = lru_meta_lst[n].prev; p != &lru_meta_lst[n] && metafile_bucket_bytes[n] > max_metafiles_bytes; ) {
    if (p->refcnt > 0) {
      p = p->prev;
      continue;
    }
    w = p->prev;
    metafile_free (p);
    t++;
    p = w;
  }

  if (metafile_bucket_bytes[n] > max_metafiles_bytes && !t) {
    vkprintf (2, "unload_metafile: max_metafiles_bytes = %d, metafile_bucket_bytes[%d] = %d, metafiles = %d\n", max_metafiles_bytes, n, metafile_bucket_bytes[n], metafiles);
  }
}

int onload_metafile (struct connection *c, int read_bytes) {
  vkprintf (2, "onload_metafile (%p, %d)\n", c, read_bytes);

  struct aio_connection *a = (struct aio_connection *) c;
  metafile_t *meta = (metafile_t *) a->extra;

  assert (a->basic_type == ct_aio);
  assert (meta != NULL);

  if (!(meta->aio == a)) {
    kprintf ("assertion (meta->aio == a) will fail, meta->aio:%p, a:%p\n", meta->aio, a);
    assert (meta->aio == a);
  }

  const int required_bytes = meta->size - offsetof (metafile_t, data);
  if (read_bytes != required_bytes) {
    vkprintf (1, "ERROR reading metafile (%s, volume_id = %lld, local_id = %d: read %d bytes out of %d: %m\n", meta->B->abs_filename, meta->B->volume_id, meta->local_id, read_bytes, required_bytes);
    metafiles_load_errors++;
    meta->corrupted = 1;
    if (aio_error (a->cb) == ECANCELED) {
      metafiles_cancelled++;
      meta->cancelled = 1;
    }
  } else {
    tot_aio_loaded_bytes += read_bytes;
    if (use_crc32_check) {
      const unsigned required_filesize = required_bytes - sizeof (struct lev_storage_file);
      struct lev_storage_file *E = (struct lev_storage_file *) meta->data;
      if (((E->size + 3) & -4) > required_filesize || E->crc32 != compute_crc32 (E->data, E->size)) {
        vkprintf (3, "E->size: %u, required_filesize: %u\n", E->size, required_filesize);
        metafiles_crc32_errors++;
        meta->corrupted = 1;
        meta->crc32_error = 1;
      }
    }
  }

  vkprintf (2, "*** Read metafile: read %d bytes\n", read_bytes);

  Dirs[meta->B->dir_id].pending_aio_connections--;
  meta->aio = NULL;
  meta->refcnt--;

  return 1;
}

conn_type_t ct_metafile_aio = {
  .magic = CONN_FUNC_MAGIC,
  .wakeup_aio = onload_metafile
};

conn_query_type_t aio_metafile_query_type = {
  .magic = CQUERY_FUNC_MAGIC,
  .title = "storage-aio-metafile-query",
  .wakeup = aio_query_timeout,
  .close = delete_aio_query,
  .complete = delete_aio_query
};

/******************************* read_stats **************************************/
static void binlog_relax_astat (storage_binlog_file_t *B, double e) {
  B->as_read.counter *= e;
  B->as_read.counter += B->st_read.fails - B->as_read.old_value;
  B->as_read.old_value = B->st_read.fails;
}

static void volume_relax_astat (volume_t *V, double e) {
  int i;
  for (i = 0; i < V->binlogs; i++) {
    binlog_relax_astat (V->B[i], e);
  }
}

static void noop (void) { }

void (*storage_volumes_relax_astat) (void) = noop;

static void volumes_relax_astat (void) {
  static int last_call_time = 0;
  double e = last_call_time ? exp ((-M_LN2 / 86400.0) * (now - last_call_time)) : 0.0;
  int i;
  for (i = 0; i < volumes; i++) {
    volume_relax_astat (Volumes[i], e);
  }
  last_call_time = now;
}

void update_read_stat (stat_read_t *S, int success) {
  if (success) {
    S->success++;
    S->sequential_fails = 0;
  } else {
    S->fails++;
    S->sequential_fails++;
    S->last_fail_time = now;
  }
}

void update_binlog_read_stat (metafile_t *meta, int success) {
  storage_binlog_file_t *B = meta->B;
  update_read_stat (&Dirs[B->dir_id].st_read, success);
  update_read_stat (&B->st_read, success);
  if (!success) {
    bad_image_cache_store (B, meta);
    if (meta->refcnt <= 0) {
      metafile_free (meta);
    }
  }
}

void update_binlog_fsync_stat (storage_binlog_file_t *B, int success) {
  update_read_stat (&Dirs[B->dir_id].st_fsync, success);
  update_read_stat (&B->st_fsync, success);
}
/*********************************************************************************/
#define MAX_CMP_CHOOSE_FUNCS 5
typedef storage_binlog_file_t *(*fcmp_t)(void **, storage_binlog_file_t *, storage_binlog_file_t *);
#define	NEXT	return (* (fcmp_t *) IP)(IP + 1, A, B);
void *CMP_IP[MAX_CMP_CHOOSE_FUNCS];
static storage_binlog_file_t *cmp_read_seq_fails (void **IP, storage_binlog_file_t *A, storage_binlog_file_t *B) {
  if (A->st_read.sequential_fails < B->st_read.sequential_fails) {
    return A;
  }
  if (A->st_read.sequential_fails > B->st_read.sequential_fails) {
    return B;
  }
  NEXT
}

static storage_binlog_file_t *cmp_pending_aio_connection (void **IP, storage_binlog_file_t *A, storage_binlog_file_t *B) {
  if (Dirs[A->dir_id].pending_aio_connections < Dirs[B->dir_id].pending_aio_connections) {
    return A;
  }
  if (Dirs[A->dir_id].pending_aio_connections > Dirs[B->dir_id].pending_aio_connections) {
    return B;
  }
  NEXT
}

static storage_binlog_file_t *cmp_amortization_read_fails (void **IP, storage_binlog_file_t *A, storage_binlog_file_t *B) {
  if (fabs (A->as_read.counter - B->as_read.counter) < 1.0) {
    NEXT;
  }
  return A->as_read.counter < B->as_read.counter ? A : B;
}

static storage_binlog_file_t *cmp_read_fails (void **IP, storage_binlog_file_t *A, storage_binlog_file_t *B) {
  if (A->st_read.fails < B->st_read.fails) {
    return A;
  }
  if (A->st_read.fails > B->st_read.fails) {
    return B;
  }
  NEXT
}
#undef NEXT

static storage_binlog_file_t *cmp_priority (void **IP, storage_binlog_file_t *A, storage_binlog_file_t *B) {
  if (A->priority < B->priority) {
    return A;
  }
  if (A->priority > B->priority) {
    return B;
  }
  return A;
}

int storage_parse_choose_binlog_option (const char *s) {
  int i, j, k = 0;
  for (i = 0; s[i]; i++) {
    for (j = 0; j < i; j++) {
      if (s[i] == s[j]) {
        break;
      }
    }
    if (j >= i) {
      continue; /* skip duplicates */
    }
    switch (s[i]) {
      case 'a': CMP_IP[k] = cmp_pending_aio_connection; break;
      case 'h':
        CMP_IP[k] = cmp_amortization_read_fails;
        storage_volumes_relax_astat = volumes_relax_astat;
      break;
      case 's': CMP_IP[k] = cmp_read_seq_fails; break;
      case 't': CMP_IP[k] = cmp_read_fails; break;
      default: kprintf ("%s: unknown compare option '%c'\n", __func__, s[i]); return -1;
    }
    k++;
  }
  assert (k < MAX_CMP_CHOOSE_FUNCS);
  CMP_IP[k] = cmp_priority;
  return 0;
}

storage_binlog_file_t *choose_reading_binlog (volume_t *V, long long offset, long long offset_end, int forbidden_dirmask) {
  int i;
  storage_binlog_file_t *R = NULL;
  for (i = 0; i < V->binlogs; i++) {
    if (!((1 << V->B[i]->dir_id) & forbidden_dirmask) && V->B[i]->fd_rdonly >= 0 && V->B[i]->size >= offset_end && !bad_image_cache_probe (V->B[i], offset)) {
      R = R ? (* (fcmp_t *) CMP_IP)(CMP_IP + 1, R, V->B[i]) : V->B[i];
    }
  }
  if (R == NULL) {
    choose_reading_binlog_errors++;
  }
  return R;
}

int metafile_load (volume_t *V, metafile_t **R, storage_binlog_file_t **PB, long long volume_id, int local_id, int filesize, long long offset) {
  *R = NULL;
  *PB = NULL;
  vkprintf (3, "load_metafile (volume_id = %lld, local_id = %d, filesize = %d, offset = %lld)\n", volume_id, local_id, filesize, offset);
  int h;
  metafile_t *meta = get_meta_f (volume_id, local_id, &h, 0);
  if (meta != NULL) {
    *R = meta;
    reuse (meta);
    if (meta->aio) {
      return -2;
    }
    return 0;
  }
  storage_binlog_file_t *B = *PB = choose_reading_binlog (V, offset, offset + sizeof (struct lev_storage_file) + filesize, 0);
  if (B == NULL) {
    return STORAGE_ERR_OPEN;
  }
  if (max_aio_connections_per_disk && Dirs[B->dir_id].pending_aio_connections >= max_aio_connections_per_disk) {
    return STORAGE_ERR_TOO_MANY_AIO_CONNECTIONS;
  }
  unload_metafiles (0);
  unload_metafiles (1);
  const int meta_size = filesize + meta_header_size;
  meta = malloc (meta_size);
  if (meta == NULL) {
    return STORAGE_ERR_OUT_OF_MEMORY;
  }
  memset (meta, 0, sizeof (*meta));
  meta->B = B;
  meta->offset = offset;
  meta->local_id = local_id;
  meta->size = meta_size;
  //meta->timeout = now + MIN_META_LIVING_TIME;
  meta->hnext = M[h];
  assert (meta->corrupted == 0);
  M[h] = meta;
  add_use (meta);

  const int sz = filesize + sizeof (struct lev_storage_file);
  meta->aio = create_aio_read_connection (B->fd_rdonly, &meta->data[0], offset, sz, &ct_metafile_aio, meta);
  Dirs[B->dir_id].pending_aio_connections++;
  meta->refcnt++;
  assert (meta->aio != NULL);

  *R = meta;
  return -2;
}

/****************************** md5 tree ***********************************/

int alloc_tree_nodes;

static md5_tree_t *md5_new_tree_node (unsigned char x[16], int y, unsigned long long offset) {
  md5_tree_t *V = tszmalloc0 (sizeof (md5_tree_t));
  assert (V);

  __sync_fetch_and_add (&alloc_tree_nodes, 1);

  memcpy (&V->x[0], &x[0], 16);
  V->y = y;
  V->offset = offset;
  return V;
}

static md5_tree_t *md5_tree_lookup (md5_tree_t *T, unsigned char x[16]) {
  while (T) {
    int c = memcmp (&x[0], &T->x[0], 16);
    if (c < 0) {
      T = T->left;
    } else if (c > 0) {
      T = T->right;
    } else {
      return T;
    }
  }
  return T;
}

static void md5_tree_split (md5_tree_t **L, md5_tree_t **R, md5_tree_t *T, unsigned char x[16]) {
  if (!T) { *L = *R = 0; return; }
  if (memcmp (&x[0], &T->x[0], 16) < 0) {
    *R = T;
    md5_tree_split (L, &T->left, T->left, x);
  } else {
    *L = T;
    md5_tree_split (&T->right, R, T->right, x);
  }
}

static md5_tree_t *md5_tree_insert (md5_tree_t *T, unsigned char x[16], int y, long long offset) {
  md5_tree_t *V;
  if (!T) {
    V = md5_new_tree_node (x, y, offset);
    return V;
  }
  if (T->y >= y) {
    if (memcmp (&x[0], &T->x[0], 16) < 0) {
      T->left = md5_tree_insert (T->left, x, y, offset);
    } else {
      T->right = md5_tree_insert (T->right, x, y, offset);
    }
    return T;
  }
  V = md5_new_tree_node (x, y, offset);
  md5_tree_split (&V->left, &V->right, T, x);
  return V;
}

static int md5_doc_idx_lookup (volume_t *V, unsigned char md5[16], unsigned long long secret, unsigned long long *file_pos) {
  int a, b, c;
  a = -1;
  b = V->idx_docs;
  while (b - a > 1) {
    c = ((a + b) >> 1);
    if (memcmp (V->Md5_Docs + (c << 4), &md5[0], 16) <= 0) { a = c; } else { b = c; }
  }
  if (a < 0 || memcmp (V->Md5_Docs + (a << 4), &md5[0], 16)) {
    return STORAGE_ERR_DOC_NOT_FOUND;
  }

  const unsigned long long pos = V->Md5_Pos[a];

  if ((secret ^ pos) & STORAGE_SECRET_MASK) {
    return STORAGE_ERR_WRONG_SECRET;
  }

  *file_pos = pos & FILE_OFFSET_MASK;

  return 0;
}

static int doc_lookup (volume_t *V, int local_id, unsigned long long secret, unsigned long long *file_pos) {
  pthread_mutex_lock (&V->mutex_insert);
  if (local_id < 1 || local_id > V->docs) {
    pthread_mutex_unlock (&V->mutex_insert);
    return STORAGE_ERR_ILLEGAL_LOCAL_ID;
  }
  unsigned long long pos;
  if (local_id <= V->idx_docs) {
    pos = V->Idx_Pos[local_id];
  } else {
    int i = local_id - V->idx_docs - 1;
    pos = V->Pos[i >> 11][i & 2047];
  }
  pthread_mutex_unlock (&V->mutex_insert);

  *file_pos = pos & FILE_OFFSET_MASK;

  if ((secret ^ pos) & STORAGE_SECRET_MASK) {
    return STORAGE_ERR_WRONG_SECRET;
  }

  return 0;
}

static int md5_doc_lookup (volume_t *V, unsigned char md5[16], unsigned long long secret, unsigned long long *file_pos) {
  int r = md5_doc_idx_lookup (V, md5, secret, file_pos);
  if (!r || r == STORAGE_ERR_WRONG_SECRET) {
    return r;
  }
  vkprintf (3, "md5_doc_idx_lookup returns exit code %d.\n", r);

  md5_tree_t *T = md5_tree_lookup (V->Md5_Root, md5);
  if (T == NULL) {
    return STORAGE_ERR_DOC_NOT_FOUND;
  }

  const unsigned long long pos = T->offset;
  vkprintf (4, "pos = %llx\n", pos);

  if ((secret ^ pos) & STORAGE_SECRET_MASK) {
    return STORAGE_ERR_WRONG_SECRET;
  }

  *file_pos = pos & FILE_OFFSET_MASK;

  return 0;
}

static int doc_hide (volume_t *V, int local_id) {
  pthread_mutex_lock (&V->mutex_insert);
  if (local_id < 1 || local_id > V->docs) {
    pthread_mutex_unlock (&V->mutex_insert);
    return STORAGE_ERR_ILLEGAL_LOCAL_ID;
  }
  unsigned long long *pos;
  if (local_id <= V->idx_docs) {
    pos = &(V->Idx_Pos[local_id]);
  } else {
    int i = local_id - V->idx_docs - 1;
    pos = &(V->Pos[i >> 11][i & 2047]);
  }
  *pos ^= STORAGE_SECRET_MASK;
  pthread_mutex_unlock (&V->mutex_insert);
  return 0;
}

static int doc_insert (volume_t *V, unsigned long long secret, unsigned long long file_pos) {
  const unsigned long long pos = file_pos | (secret & STORAGE_SECRET_MASK);
  tot_docs++;
  pthread_mutex_lock (&V->mutex_insert);
  int local_id = ++(V->docs);
  int i = local_id - V->idx_docs - 1;
  assert (i >= 0);
  int k = i & 2047;
  i >>= 11;
  if (!k) {
    unsigned long long *a = calloc (2048, 8);
    assert (a != NULL);
    a[0] = pos;
    if (i == V->pos_capacity) {
      int new_capacity = 1 + (V->pos_capacity << 1);
      unsigned long long **b = realloc (V->Pos, new_capacity * sizeof (V->Pos[0]));
      assert (b != NULL);
      V->Pos = b;
      V->pos_capacity = new_capacity;
    }
    assert (i < V->pos_capacity);
    V->Pos[i] = a;
  } else {
    assert (i < V->pos_capacity);
    V->Pos[i][k] = pos;
  }
  pthread_mutex_unlock (&V->mutex_insert);
  return local_id;
}

static int md5_doc_insert (volume_t *V, unsigned char md5[16], unsigned long long secret, unsigned long long file_pos) {
  vkprintf (3, "md5_doc_insert (secret = %llx, file_pos = %llx)\n", secret, file_pos);
  unsigned long long tmp;
  int r = md5_doc_lookup (V, md5, 0, &tmp);
  if (!r || r == STORAGE_ERR_WRONG_SECRET) {
    return STORAGE_ERR_DOC_EXISTS;
  }
  V->Md5_Root = md5_tree_insert (V->Md5_Root, md5, lrand48 (), file_pos | (secret & STORAGE_SECRET_MASK));
  V->docs++;
  tot_docs++;
  return 0;
}

int init_storage_data (int schema) {
  //replay_logevent = storage_replay_logevent;
  return 0;
}

int do_get_doc (long long volume_id, int local_id, unsigned long long secret, volume_t **V, unsigned long long *offset, int *filesize) {
  *V = get_volume_f (volume_id, 0);
  if (*V == NULL) {
    return STORAGE_ERR_UNKNOWN_VOLUME_ID;
  }
  int r = doc_lookup (*V, local_id, secret, offset);
  if (r) {
    return r;
  }

  if (filesize) {
    unsigned long long o = *offset;
    if (local_id < (*V)->docs) {
      r = doc_lookup (*V, local_id + 1, 0, &o);
    } else {
      pthread_mutex_lock (&(*V)->mutex_write);
      if (local_id < (*V)->docs) {
        pthread_mutex_unlock (&(*V)->mutex_write);
        r = doc_lookup (*V, local_id + 1, 0, &o);
      } else {
        assert (local_id == (*V)->docs);
        o = (*V)->cur_log_pos;
        pthread_mutex_unlock (&(*V)->mutex_write);
        r = 0;
      }
    }
    if (!r || r == STORAGE_ERR_WRONG_SECRET) {
      *filesize = (o - *offset) - (sizeof (struct lev_storage_file) + sizeof (struct lev_crc32));
    }
  }
  return 0;
}

int do_md5_get_doc (long long volume_id, unsigned char md5[16], unsigned long long secret, volume_t **V, unsigned long long *offset) {
  *V = get_volume_f (volume_id, 0);
  if (*V == NULL) {
    return STORAGE_ERR_UNKNOWN_VOLUME_ID;
  }
  int r = md5_doc_lookup (*V, md5, secret, offset);
  if (r) {
    return r;
  }
  return 0;
}

static unsigned long long make_secret (void) {
  unsigned long long r = 0;
  int i = 0;
  while (1) {
    r |= (lrand48 () >> 15) & 0xffff;
    if (++i == 4) {
      break;
    }
    r <<= 16;
  }
  return r;
}

static void storage_binlog_truncate (volume_t *V, off_t off) {
  int k;
  for (k = 0; k < V->binlogs; k++) {
    if (V->B[k]->fd_wronly >= 0) {
      ftruncate (V->B[k]->fd_wronly, off);
    }
  }
}

static int storage_volume_could_write (volume_t *V) {
  int k;
  for (k = 0; k < V->binlogs; k++) {
    if (V->B[k]->fd_wronly >= 0 && V->B[k]->st_fsync.fails == 0) {
      return 0;
    }
  }
  return STORAGE_ERR_NO_WRONLY_BINLOGS;
}

int storage_binlog_pwrite (volume_t *V, void *buf, size_t count, off_t offset, off_t truncate_offset) {
  int ok = 0, k;
  for (k = 0; k < V->binlogs; k++) {
    if (V->B[k]->fd_wronly >= 0) {
      if (pwrite (V->B[k]->fd_wronly, buf, count, offset) != count) {
        ftruncate (V->B[k]->fd_wronly, truncate_offset);
        V->B[k]->size = truncate_offset;
        close (V->B[k]->fd_wronly);
        wronly_binlogs_closed++;
        V->B[k]->fd_wronly = -2;
      } else {
        off_t bytes = count + offset;
        if (V->B[k]->size < bytes) {
          V->B[k]->size = bytes;
        }
        dirty_binlog_queue_push (V->B[k]);
        ok++;
      }
    }
  }
  if (!ok) {
    return STORAGE_ERR_PWRITE;
  }

  return 0;
}

#define COPY_DOC_IO_BUFFSIZE 0x100000

/* class for reading from file or memory binary serialized array */
struct storage_data_stream {
  int fd;
  const unsigned char *ptr;
  long long bytes_read;
  long long size;
};

static int storage_data_stream_init (struct storage_data_stream *self, const char *const filename, const unsigned char *const data, int data_len) {
  vkprintf (4, "storage_data_stream_init (self: %p, filename: %p, data: %p, data_len: %d)\n", self, filename, data, data_len);
  self->fd = -1;
  self->bytes_read = 0;
  self->ptr = data;
  if (filename != NULL) {
    self->fd = open (filename, O_RDONLY);
    if (self->fd < 0) {
      return STORAGE_ERR_COULDN_OPEN_FILE;
    }
    struct stat buf;
    if (fstat (self->fd, &buf) < 0) {
      close (self->fd);
      return STORAGE_ERR_FSTAT;
    }
    self->size = buf.st_size;
  } else {
    if (data == NULL) {
      return STORAGE_ERR_NULL_POINTER_EXCEPTION;
    }
    if (data_len < 1) {
      return STORAGE_ERR_NOT_ENOUGH_DATA;
    }
    if (*data <= 253) {
      self->size = *(self->ptr)++;
      if (self->size + 1 > data_len) {
        return STORAGE_ERR_NOT_ENOUGH_DATA;
      }
    } else if (*data == 254) {
      if (data_len < 4) {
        return STORAGE_ERR_NOT_ENOUGH_DATA;
      }
      unsigned int sz;
      memcpy (&sz, self->ptr, 4);
      sz >>= 8;
      self->size = sz;
      self->ptr += 4;
      if (self->size + 4 > data_len) {
        return STORAGE_ERR_NOT_ENOUGH_DATA;
      }
    } else {
      if (verbosity >= 4) {
        int i;
        fprintf (stderr, "data: ");
        for (i = 0; i < data_len; i++) {
          fprintf (stderr, "%d ", (int) data[i]);
        }
        fprintf (stderr, "\n");
        fflush (stderr);
      }
      return STORAGE_ERR_SERIALIZATION;
    }
  }
  return 0;
}

static int storage_data_stream_read (struct storage_data_stream *self, void *a, long long len) {
  vkprintf (4, "storage_data_stream_read (len: %lld)\n", len);
  if (len + self->bytes_read > self->size) {
    return STORAGE_ERR_READ;
  }
  if (self->fd >= 0) {
    if (read (self->fd, a, len) != len) {
      return STORAGE_ERR_READ;
    }
  } else {
    memcpy (a, self->ptr, len);
    self->ptr += len;
  }
  self->bytes_read += len;
  return 0;
}

static void storage_data_stream_close (struct storage_data_stream *self) {
  if (self->fd >= 0) {
    close (self->fd);
    self->fd = -1;
  }
}

int do_copy_doc (volume_t *V, unsigned long long *secret, const char *const filename, const unsigned char *const data, int data_len, int content_type, unsigned char md5[16]) {
  vkprintf (4, "do_copy_doc (V: %p, *secret: %p, filename: %p, data: %p, data_len: %d)\n", V, secret, filename, data, data_len);
  const int zero = 0;
  if (binlog_disabled) {
    return STORAGE_ERR_WRITE_IN_READONLY_MODE;
  }
  int r = storage_volume_could_write (V);
  if (r < 0) {
    return r;
  }
  const long long old_log_pos = V->cur_log_pos;
  long long cur_log_pos = old_log_pos;
  /* thread safe */
  unsigned char io_buff[COPY_DOC_IO_BUFFSIZE];
  int hide_local_id;
  md5_context ctx;
  md5_starts (&ctx);

  if (!V->md5_mode && filename && !strncmp (filename, "**hide doc**", 12) && sscanf (filename + 12, "%x", &hide_local_id) == 1) {
    struct lev_storage_file *E = (struct lev_storage_file *) io_buff;
    memset (E, 0, sizeof (*E));
    *secret = 0;
    md5_finish (&ctx, E->md5);
    memcpy (md5, E->md5, 16);
    r = doc_hide (V, hide_local_id);
    if (r < 0) {
      return r;
    }
    E->type = LEV_STORAGE_HIDE_FILE;
    E->local_id = hide_local_id;
    E->content_type = ct_unknown;
    E->mtime = now;
    struct lev_crc32 *C = (struct lev_crc32 *) (io_buff + sizeof (*E));
    cur_log_pos += sizeof (*E);
    unsigned log_crc32 = ~crc32_partial (E, sizeof (*E), ~V->log_crc32);
    C->type = LEV_CRC32;
    C->timestamp = now;
    C->pos = cur_log_pos;
    C->crc32 = log_crc32;
    cur_log_pos += sizeof (*C);
    V->log_crc32 = ~crc32_partial (C, sizeof (*C), ~log_crc32);
    V->cur_log_pos = cur_log_pos;
    r = storage_binlog_pwrite (V, io_buff, (sizeof (struct lev_storage_file) + sizeof (struct lev_crc32)), old_log_pos, old_log_pos);
    if (r < 0) {
      return r;
    }
    return 0;
  }
  struct storage_data_stream f;
  r = storage_data_stream_init (&f, filename, data, data_len);
  if (r < 0) {
    return r;
  }

  if (f.size <= (COPY_DOC_IO_BUFFSIZE - (sizeof (struct lev_storage_file) + sizeof (struct lev_crc32)))) {
    struct lev_storage_file *E = (struct lev_storage_file *) io_buff;
    E->type = LEV_STORAGE_FILE;
    E->secret = make_secret ();
    E->local_id = V->docs + 1;
    E->content_type = ct_unknown;
    E->size = f.size;
    E->mtime = now;
    E->crc32 = 0;
    int sz = sizeof (struct lev_storage_file);
    if (storage_data_stream_read (&f, io_buff + sz, E->size) < 0) {
      storage_data_stream_close (&f);
      return STORAGE_ERR_READ;
    }
    storage_data_stream_close (&f);
    unsigned char *W = io_buff + sz;
    int l = E->size;
    if (content_type != ct_unknown) {
      E->content_type = content_type;
    } else {
      E->content_type = detect_content_type (W, l);
      if (E->content_type == ct_unknown) {
        vkprintf (1, "unknown content type: %.*s\n", l < 32 ? l : 32, W);
        return STORAGE_ERR_UNKNOWN_TYPE;
      }
    }
    md5_update (&ctx, W, l);
    E->crc32 = compute_crc32 (W, l);
    int old_l = l;
    l = (l + 3) & -4;
    if (l != old_l) {
      memset (&W[old_l], 0, l - old_l);
    }

    md5_finish (&ctx, E->md5);

    unsigned long long t[2];
    memcpy (&t[0], E->md5, 16);
    *secret = E->secret ^= t[0] ^ t[1];

    memcpy (md5, E->md5, 16);
    cur_log_pos += ((sz + E->size + 3) & -4);

    unsigned log_crc32 = ~crc32_partial (io_buff, sz, ~V->log_crc32);
    log_crc32 = compute_crc32_combine (log_crc32, E->crc32, E->size);
    if (E->size & 3) {
      log_crc32 = ~crc32_partial (&zero, 4 - (E->size & 3), ~log_crc32);
    }

    struct lev_crc32 *C = (struct lev_crc32 *) (W + l);
    C->type = LEV_CRC32;
    C->timestamp = now;
    C->pos = cur_log_pos;
    C->crc32 = log_crc32;
    sz = sizeof (struct lev_crc32);
    r = storage_binlog_pwrite (V, io_buff, l + (sizeof (struct lev_storage_file) + sizeof (struct lev_crc32)), old_log_pos, old_log_pos);
    if (r < 0) {
      return r;
    }
    cur_log_pos += sz;
    log_crc32 = ~crc32_partial (C, sz, ~log_crc32);
    r = V->md5_mode ? md5_doc_insert (V, E->md5, E->secret, old_log_pos) : doc_insert (V, E->secret, old_log_pos);
    if (r < 0) {
      storage_binlog_truncate (V, old_log_pos);
      return r;
    }

    V->log_crc32 = log_crc32;
    V->cur_log_pos = cur_log_pos;
    return r;
  } else {
    struct lev_storage_file E;
    E.type = LEV_STORAGE_FILE;
    E.secret = make_secret ();
    E.local_id = V->docs + 1;
    E.content_type = ct_unknown;
    E.size = f.size;
    E.mtime = now;
    E.crc32 = 0;
    int sz = sizeof (struct lev_storage_file);
    int block_no = 0;
    long long off = cur_log_pos + sz;
    while (f.size > f.bytes_read) {
      long long l = f.size - f.bytes_read;
      if (l > COPY_DOC_IO_BUFFSIZE) {
        l = COPY_DOC_IO_BUFFSIZE;
      }
      if (storage_data_stream_read (&f, io_buff, l) < 0) {
        storage_binlog_truncate (V, old_log_pos);
        storage_data_stream_close (&f);
        return STORAGE_ERR_READ;
      }
      if (!(block_no++)) {
        /* detect content type */
        if (content_type != ct_unknown) {
          E.content_type = content_type;
        } else {
          E.content_type = detect_content_type (io_buff, l);
          if (E.content_type == ct_unknown) {
            vkprintf (1, "unknown content type: %.*s\n", l < 32 ? (int) l : 32, io_buff);
            storage_data_stream_close (&f);
            return STORAGE_ERR_UNKNOWN_TYPE;
          }
        }
      }

      md5_update (&ctx, io_buff, l);
      E.crc32 = ~crc32_partial (io_buff, l, ~E.crc32);
  #ifdef DEBUG_SIMULATE_WRITE_BIT_ERROR
      if (block_no == 1) {
        int zzz = lrand48 () % (8 * l);
        io_buff[zzz>>3] ^= 1 << (zzz & 7);
      }
  #endif
      int old_l = l;
      l = (l + 3) & -4;
      if (l != old_l) {
        memset (&io_buff[old_l], 0, l - old_l);
      }
      r = storage_binlog_pwrite (V, io_buff, l, off, old_log_pos);
      if (r < 0) {
        storage_data_stream_close (&f);
        return r;
      }
      off += l;
    }

    storage_data_stream_close (&f);

    if (E.content_type == ct_unknown) {
      vkprintf (1, "unknown content type: (block_no = %d)\n", block_no);
      return STORAGE_ERR_UNKNOWN_TYPE;
    }

    md5_finish (&ctx, E.md5);

    unsigned long long t[2];
    memcpy (&t[0], &E.md5[0], 16);
    *secret = E.secret ^= t[0] ^ t[1];

    r = storage_binlog_pwrite (V, &E, sz, old_log_pos, old_log_pos);
    if (r < 0) {
      return r;
    }

    memcpy (md5, E.md5, 16);
    cur_log_pos += ((sz + E.size + 3) & -4);

    unsigned log_crc32 = ~crc32_partial (&E, sz, ~V->log_crc32);
    log_crc32 = compute_crc32_combine (log_crc32, E.crc32, E.size);
    if (E.size & 3) {
      log_crc32 = ~crc32_partial (&zero, 4 - (E.size & 3), ~log_crc32);
    }

    struct lev_crc32 C;
    C.type = LEV_CRC32;
    C.timestamp = now;
    C.pos = cur_log_pos;
    C.crc32 = log_crc32;
    sz = sizeof (struct lev_crc32);

    r = storage_binlog_pwrite (V, &C, sz, cur_log_pos, old_log_pos);
    if (r < 0) {
      return r;
    }
    cur_log_pos += sz;
    log_crc32 = ~crc32_partial (&C, sz, ~log_crc32);
    r = V->md5_mode ? md5_doc_insert (V, E.md5, E.secret, old_log_pos) : doc_insert (V, E.secret, old_log_pos);
    if (r < 0) {
      storage_binlog_truncate (V, old_log_pos);
      return r;
    }

    V->log_crc32 = log_crc32;
    V->cur_log_pos = cur_log_pos;
    return r;
  }
}

int storage_le_start (volume_t *V, struct lev_start *E) {
  if (E->schema_id != STORAGE_SCHEMA_V1) {
    return -1;
  }
  if (E->extra_bytes != 12) {
    return -2;
  }
  V->log_split_min = E->split_min;
  V->log_split_max = E->split_max;
  V->log_split_mod = E->split_mod;
  /* dirty hack: hiding warning "array subscript is above array bounds" */
  unsigned char tmp[12];
  memcpy (&tmp[0], &E->str[0], 12);
  memcpy (&V->volume_id, &tmp[0], 8);
  memcpy (&V->md5_mode, &tmp[8], 4);
  vkprintf (3, "V->md5_mode = %d\n", V->md5_mode);
  /* end of hack */
  //assert (log_split_mod == P->log_split_mod);
  assert (V->log_split_mod > 0 && V->log_split_min >= 0 && V->log_split_min + 1 == V->log_split_max && V->log_split_max <= V->log_split_mod);

  return 0;
}


int storage_append_to_binlog (volume_t *V) {
  int k;
  V->wronly_binlogs = 0;
  for (k = 0; k < V->binlogs; k++) {
    if (V->B[k]->size < V->B[0]->size) {
      kprintf ("Skip \"%s\", since it is shorter than %s.\n", V->B[k]->abs_filename, V->B[0]->abs_filename);
      V->B[k]->fd_wronly = -1;
      continue;
    }
    if (!V->B[k]->prefix) {
      kprintf ("Skip \"%s\", since it isn't prefix of %s.\n", V->B[k]->abs_filename, V->B[0]->abs_filename);
      V->B[k]->fd_wronly = -1;
      continue;
    }
    V->B[k]->fd_wronly = open (V->B[k]->abs_filename, O_WRONLY);
    if (V->B[k]->fd_wronly < 0) {
      kprintf ("open (\"%s\", O_WRONLY) fail. %m", V->B[k]->abs_filename);
      V->B[k]->fd_wronly = -1;
      continue;
    }
    long long end_pos = lseek (V->B[k]->fd_wronly, 0, SEEK_END);
    if (end_pos != V->log_readto_pos) {
      kprintf ("lseek (\"%s\", 0, SEEK_END) returns %lld, but V->log_readto_pos is equal to %lld.\n", V->B[k]->abs_filename, end_pos, V->log_readto_pos);
      close (V->B[k]->fd_wronly);
      V->B[k]->fd_wronly = -1;
      continue;
    }
    if (lock_whole_file (V->B[k]->fd_wronly, F_WRLCK) <= 0) {
      kprintf ("lock_whole_file (%s, F_WRLCK) fail.\n", V->B[k]->abs_filename);
      exit (1);
    }
    V->wronly_binlogs++;
  }
  if (V->wronly_binlogs <= 0) {
    vkprintf (1, "binlogs: %d, wronly: %d\n", V->binlogs, V->wronly_binlogs);
    return STORAGE_ERR_NO_WRONLY_BINLOGS;
  }
  return 0;
}

int storage_replay_log (volume_t *V, long long start_pos) {
  const char *name = V->Binlog->info->filename;
  if (start_pos != lseek (V->Binlog->fd, start_pos, SEEK_SET)) {
    kprintf ("Illegal binlog pos: %lld\n", start_pos);
    return -1;
  }
  V->cur_log_pos = start_pos;
  V->log_crc32 = V->jump_log_crc32;
  while (V->cur_log_pos < V->Binlog->info->file_size) {
    if (!V->cur_log_pos) {
      unsigned char lev_start_buff[LEV_STORAGE_START_SIZE];
      struct lev_start *E = (struct lev_start *) &lev_start_buff;
      int bytes_read = read (V->Binlog->fd, E, LEV_STORAGE_START_SIZE);
      if (LEV_STORAGE_START_SIZE != bytes_read) {
        kprintf ("[%s] binlog read fail (LEV_START), bytes_read = %d. %m\n", name, bytes_read);
        return -1;
      }
      if (E->type != LEV_START) {
        kprintf ("[%s] expected LEV_START but %x found.\n", name, E->type);
        return -1;
      }
      if (storage_le_start (V, E) < 0) {
        kprintf ("[%s] storage_le_start fail\n", name);
        return -1;
      }
      V->log_crc32 = ~crc32_partial (E, LEV_STORAGE_START_SIZE, ~(V->log_crc32));
      V->cur_log_pos += LEV_STORAGE_START_SIZE;
    } else {
      struct lev_storage_file E;
      int sz = sizeof (struct lev_storage_file);
      if (sz != read (V->Binlog->fd, &E, sz)) {
        kprintf ("[%s] binlog read fail (LEV_STORAGE_FILE). %m\n", name);
        return -1;
      }

      long long event_pos = V->cur_log_pos;
      if (E.type != LEV_STORAGE_FILE && E.type != LEV_STORAGE_HIDE_FILE) {
        kprintf ("[%s] expected LEV_STORAGE_FILE|LEV_STORAGE_HIDE_FILE, but %x found at pos %lld\n", name, E.type, event_pos);
        return -1;
      }

      if (E.type == LEV_STORAGE_HIDE_FILE && E.size != 0) {
        kprintf ("[%s] expected E.size equal to zero in LEV_STORAGE_HIDE_FILE case, pos %lld\n", name, event_pos);
        return -1;
      }

      V->log_crc32 = ~crc32_partial (&E, sz, ~V->log_crc32);
      V->log_crc32 = compute_crc32_combine (V->log_crc32, E.crc32, E.size);
      int l = (E.size + 3) & -4;
      if (l != E.size) {
        const int zero = 0;
        V->log_crc32 = ~crc32_partial (&zero, l - E.size, ~V->log_crc32);
      }

      V->cur_log_pos += sz + l;
      if (V->cur_log_pos != lseek (V->Binlog->fd, V->cur_log_pos, SEEK_SET)) {
        kprintf ("[%s] binlog lseek fail. %m\n", name);
        V->cur_log_pos = event_pos;
        return -1;
      }

      if (E.type == LEV_STORAGE_FILE) {
        if (V->md5_mode) {
          md5_doc_insert (V, E.md5, E.secret, event_pos);
        } else {
          int r = doc_insert (V, E.secret, event_pos);
          if (r < 0 || r != E.local_id) {
            kprintf ("[%s] doc_insert returns exit code: %d (E.local_id = %d)\n", name, r, E.local_id);
            return -1;
          }
        }
      } else {
        assert (E.type == LEV_STORAGE_HIDE_FILE);
        assert (!V->md5_mode);
        int r = doc_hide (V, E.local_id);
        if (r < 0) {
          kprintf ("[%s] doc_hide returns exit code: %d, pos %lld\n", name, r, event_pos);
          return -1;
        }
      }

      struct lev_crc32 C;
      sz = sizeof (struct lev_crc32);
      long r = read (V->Binlog->fd, &C, sz);
      if (sz != r) {
        kprintf ("[%s] binlog read fail (LEV_CRC32) (sz=%lu,r=%lu): %m\n", name, (long)sz, (long)r);
        return -1;
      }

      event_pos = V->cur_log_pos;
      if (C.type != LEV_CRC32) {
        kprintf ("[%s] expected LEV_CRC32, but %x found at pos %lld\n", name, C.type, event_pos);
        return -1;
      }
      now = C.timestamp;
      if (event_pos != C.pos) {
        kprintf ("[%s] LEV_CRC32 field pos (%lld) != log event offset (%lld)\n", name, C.pos, event_pos);
        return -1;
      }
      if (C.crc32 != V->log_crc32) {
        kprintf ("[%s] LEV_CRC32 field crc (%x) != V->log_crc32 (%x), offset = %lld\n", name, C.crc32, V->log_crc32, event_pos );
        return -1;
      }

      V->log_crc32 = ~crc32_partial (&C, sz, ~V->log_crc32);
      V->cur_log_pos += sz;
    }
  }
  V->log_readto_pos = V->cur_log_pos;
  return 0;
}

/*
 *
 * GENERIC BUFFERED READ/WRITE
 *
 */

#define IO_BUFFSIZE 0x1000000
static unsigned char io_buff[IO_BUFFSIZE];

static unsigned char *rptr = io_buff, *wptr = io_buff;
static long long bytes_read;
static int idx_fd;

static int newidx_fds;
static int newidx_fd[MAX_VOLUME_BINLOGS];
static char *filename_newidx[MAX_VOLUME_BINLOGS];

static void flushout (void) {
  int w, s;
  if (rptr < wptr) {
    int i;
    s = wptr - rptr;
    for (i = 0; i < newidx_fds; i++) {
      if (newidx_fd[i] >= 0) {
        w = write (newidx_fd[i], rptr, s);
        if (w != s) {
          kprintf ("Write to %s failed (%d of %d) bytes written. %m\n" , filename_newidx[i], w, s);
          close (newidx_fd[i]);
          newidx_fd[i] = -1;
        }
      }
    }
  }
  rptr = wptr = io_buff;
}

int bytes_written;
unsigned idx_crc32_complement;

static void clearin (void) {
  rptr = wptr = io_buff + IO_BUFFSIZE;
  bytes_read = 0;
  bytes_written = 0;
  idx_crc32_complement = -1;
}

static int writeout (const void *D, size_t len) {
  bytes_written += len;
  idx_crc32_complement = crc32_partial (D, len, idx_crc32_complement);
  const int res = len;
  const char *d = D;
  while (len > 0) {
    int r = io_buff + IO_BUFFSIZE - wptr;
    if (r > len) {
      r = len;
    }
    memcpy (wptr, d, r);
    d += r;
    wptr += r;
    len -= r;
    if (len > 0) {
      flushout ();
    }
  }
  return res;
}

static void *readin (size_t len) {
  assert (len >= 0);
  if (rptr + len <= wptr) {
    return rptr;
  }
  if (wptr < io_buff + IO_BUFFSIZE) {
    return 0;
  }
  memcpy (io_buff, rptr, wptr - rptr);
  wptr -= rptr - io_buff;
  rptr = io_buff;
  int r = read (idx_fd, wptr, io_buff + IO_BUFFSIZE - wptr);
  if (r < 0) {
    fprintf (stderr, "error reading file: %m\n");
  } else {
    wptr += r;
  }
  if (rptr + len <= wptr) {
    return rptr;
  } else {
    return 0;
  }
}

static void readadv (size_t len) {
  assert (len >= 0 && len <= wptr - rptr);
  rptr += len;
}

void bread (void *b, size_t len) {
  void *p = readin (len);
  assert (p != NULL);
  memcpy (b, p, len);
  readadv (len);
  bytes_read += len;
  idx_crc32_complement = crc32_partial (p, len, idx_crc32_complement);
}

int load_index (volume_t *V) {
  int i;
  kfs_file_handle_t Index = V->Snapshot;
  if (Index == NULL) {
    V->index_size = 0;
    //jump_log_ts = 0;
    V->jump_log_pos = 0;
    V->jump_log_crc32 = 0;
    V->docs = V->idx_docs = 0;
    return 0;
  }
  idx_fd = Index->fd;
  struct storage_index_header header;
  clearin ();
  bread (&header, sizeof (struct storage_index_header));
  if (header.magic != STORAGE_INDEX_MAGIC) {
    kprintf ("index file is not for storage\n");
    return -1;
  }

  V->log_split_min = header.log_split_min;
  V->log_split_max = header.log_split_max;
  V->log_split_mod = header.log_split_mod;

  V->index_size = Index->info->file_size;
  V->jump_log_pos = header.log_pos1;
  V->jump_log_crc32 = header.log_pos1_crc32;

  V->docs = V->idx_docs = header.docs;
  V->volume_id = header.volume_id;
  V->md5_mode = header.md5_mode;

  if (V->md5_mode) {
    const unsigned sz_docs = header.docs << 4;
    const unsigned sz_pos = sz_docs >> 1;
    V->Md5_Docs = tszmalloc (sz_docs);
    bread (V->Md5_Docs, sz_docs);
    V->Md5_Pos = tszmalloc (sz_pos);
    bread (V->Md5_Pos, sz_pos);
    for (i = 1; i < header.docs; i++) {
      if (memcmp (&V->Md5_Docs[(i-1) << 4], &V->Md5_Docs[i << 4], 16) >= 0) {
        kprintf ("md5 table is corrupted in snapshot: %s\n", Index->info->filename);
        return -1;
      }
    }
  } else {
    const unsigned docs_sz = (header.docs + 1) << 3;
    V->Idx_Pos = tszmalloc (docs_sz);
    bread (V->Idx_Pos, docs_sz);
  }

  const unsigned c = ~idx_crc32_complement;
  struct lev_crc32 C;
  bread (&C, sizeof (struct lev_crc32));
  if (c != C.crc32) {
    kprintf ("crc32 not matched in snapshot: %s\n", Index->info->filename);
    return -2;
  }

  snapshot_size += V->snapshot_size = bytes_read;
  index_size += V->index_size = Index->info->file_size;
  idx_docs += V->idx_docs;
  tot_docs += V->idx_docs;
  return 0;
}

static void md5_merge_tree_with_index (volume_t *V, unsigned char *new_md5_docs, unsigned long long *new_md5_pos, int *pos_new_idx, int *pos_idx, md5_tree_t *T) {
  if (T->left) {
    md5_merge_tree_with_index (V, new_md5_docs, new_md5_pos, pos_new_idx, pos_idx, T->left);
  }
  while ( (*pos_idx) < V->idx_docs && memcmp (&V->Md5_Docs[(*pos_idx)<<4], &T->x[0], 16) < 0) {
    memcpy (&new_md5_docs[(*pos_new_idx)<<4], &V->Md5_Docs[(*pos_idx)<<4], 16);
    new_md5_pos[(*pos_new_idx)] = V->Md5_Pos[*pos_idx];
    (*pos_new_idx)++;
    (*pos_idx)++;
  }

  memcpy (&new_md5_docs[(*pos_new_idx)<<4], &T->x[0], 16);
  new_md5_pos[(*pos_new_idx)] = T->offset;
  (*pos_new_idx)++;

  if (T->right) {
    md5_merge_tree_with_index (V, new_md5_docs, new_md5_pos, pos_new_idx, pos_idx, T->right);
  }
}

static int snapshots_sync (void) {
  int k, ok = 0;
  for (k = 0; k < newidx_fds; k++) {
    if (newidx_fd[k] >= 0) {
      if (fsync (newidx_fd[k]) < 0) {
        kprintf ("%s syncing fail. %m\n", filename_newidx[k]);
        close (newidx_fd[k]);
        newidx_fd[k] = -1;
      } else {
        ok++;
      }
    }
  }
  return ok;
}

static int snapshots_close (void) {
  int k, ok = 0;
  for (k = 0; k < newidx_fds; k++) {
    if (newidx_fd[k] >= 0) {
      if (close (newidx_fd[k]) < 0) {
        newidx_fd[k] = -1;
      } else {
        ok++;
      }
    }
  }
  return ok;
}

static int snapshots_rename (void) {
  int k, ok = 0;
  for (k = 0; k < newidx_fds; k++) {
    if (newidx_fd[k] >= 0) {
      if (rename_temporary_snapshot (filename_newidx[k])) {
        kprintf ("cannot rename new index file from %s: %m\n", filename_newidx[k]);
        unlink (filename_newidx[k]);
        newidx_fd[k] = -1;
      } else {
        ok++;
      }
    }
  }
  return ok;
}

static void snapshots_printname (void) {
  int k;
  for (k = 0; k < newidx_fds; k++) {
    if (newidx_fd[k] >= 0) {
      print_snapshot_name (filename_newidx[k]);
    }
  }
}

static int snapshots_count (void) {
  int k, r = 0;
  for (k = 0; k < newidx_fds; k++) {
    if (newidx_fd[k] >= 0) {
      r++;
    }
  }
  return r;
}

static void snapshots_free (void) {
  int k;
  for (k = 0; k < newidx_fds; k++) {
    tszfree (filename_newidx[k], strlen (filename_newidx[k]) + 1);
    newidx_fd[k] = -1;
  }
}

static int snapshots_lseek (off_t offset) {
  int k, ok = 0;
  for (k = 0; k < newidx_fds; k++) {
    if (newidx_fd[k] >= 0 && lseek (newidx_fd[k], offset, SEEK_SET) == offset) {
      ok++;
    } else {
      if (newidx_fd[k] >= 0) {
        close (newidx_fd[k]);
        newidx_fd[k] = -1;
      }
    }
  }
  return ok;
}

static int snapshots_create (volume_t *V, char *newidxname) {
  int k, ok = 0;
  char *p = strrchr (newidxname, '/');
  if (p == NULL) {
    p = newidxname;
  } else {
    p++;
  }
  int l = strlen (p) + 1;
  newidx_fds = 0;
  for (k = 0; k < V->binlogs; k++) {
    storage_binlog_file_t *B = V->B[k];
    if (B->fd_rdonly < 0 && B->fd_wronly < 0) {
      continue;
    }
    int ql = l + 1 + strlen (Dirs[B->dir_id].path);
    char *q = tszmalloc (ql);
    assert (sprintf (q, "%s/%s", Dirs[B->dir_id].path, p) + 1 == ql);
    filename_newidx[newidx_fds] = q;
    newidx_fd[newidx_fds] = open (q, O_CREAT | O_TRUNC | O_WRONLY | O_EXCL, 0660);
    if (newidx_fd[newidx_fds] >= 0) {
      ok++;
    }
    newidx_fds++;
  }
  return ok;
}

int save_index (volume_t *V) {
  int i;
  char *newidxname = NULL;

  if (V->cur_log_pos == V->jump_log_pos) {
    kprintf ("[v%lld] skipping generation of new snapshot for position %lld: snapshot for this position already exists\n",
       V->volume_id, V->jump_log_pos);
    return 0;
  }

  if (V->engine_snapshot_replica) {
    newidxname = get_new_snapshot_name (V->engine_snapshot_replica, V->cur_log_pos, V->engine_snapshot_replica->replica_prefix);
  }

  if (!newidxname || newidxname[0] == '-') {
    kprintf ("cannot write index: cannot compute its name\n");
    return -1;
  }

  vkprintf (1, "creating index %s at log position %lld\n", newidxname, V->cur_log_pos);

  if (snapshots_create (V, newidxname) <= 0) {
    kprintf ("[v%lld] Couldn't create at least one snapshot for index name %s.\n", V->volume_id, newidxname);
    snapshots_free ();
    return STORAGE_ERR_OPEN;
  }

  struct storage_index_header header;
  memset (&header, 0, sizeof (header));

  header.magic = STORAGE_INDEX_MAGIC;
  header.log_pos1 = V->cur_log_pos;
  header.log_pos1_crc32 = V->log_crc32;

  vkprintf (3, "V->cur_log_pos = %lld\n", V->cur_log_pos);

  header.log_split_min = V->log_split_min;
  header.log_split_max = V->log_split_max;
  header.log_split_mod = V->log_split_mod;

  header.volume_id = V->volume_id;
  header.md5_mode = V->md5_mode;
  /*
  header.log_timestamp = log_read_until;
  if (writing_binlog) {
    relax_write_log_crc32 ();
  } else {
    relax_log_crc32 (0);
  }
  header.log_pos1_crc32 = ~log_crc32_complement;
  */
  const long long docs_offset = sizeof (header);
  if (snapshots_lseek (docs_offset) <= 0) {
    kprintf ("[v%lld] Couldn't lseek (%lld) at least one snapshot for index name %s.\n", V->volume_id, docs_offset, newidxname);
    snapshots_free ();
    return STORAGE_ERR_LSEEK;
  }

  clearin ();

  if (V->md5_mode) {
    const unsigned sz_docs = V->docs << 4;
    const unsigned sz_pos = sz_docs >> 1;
    unsigned char *new_md5_docs = tszmalloc (sz_docs);
    unsigned long long *new_md5_pos = tszmalloc (sz_pos);
    int pos_new_idx = 0, pos_idx = 0;
    md5_merge_tree_with_index (V, new_md5_docs, new_md5_pos, &pos_new_idx, &pos_idx, V->Md5_Root);
    while (pos_idx < V->idx_docs) {
      memcpy (&new_md5_docs[pos_new_idx<<4], &V->Md5_Docs[pos_idx<<4], 16);
      new_md5_pos[pos_new_idx] = V->Md5_Pos[pos_idx];
      pos_new_idx++;
      pos_idx++;
    }
    if (pos_new_idx != V->docs) {
      kprintf ("pos_new_idx = %d, V->docs = %d\n", pos_new_idx, V->docs);
      assert (pos_new_idx == V->docs);
    }
    writeout (new_md5_docs, sz_docs);
    tszfree (new_md5_docs, sz_docs);
    writeout (new_md5_pos, sz_pos);
    tszfree (new_md5_pos, sz_pos);
  } else {
    long long zero = 0;
    writeout (&zero, 8);
    if (V->idx_docs > 0) {
      writeout (&V->Idx_Pos[1], V->idx_docs << 3);
    }
    int new_docs = V->docs - V->idx_docs;
    for (i = 0; i < new_docs; i++) {
      writeout (&(V->Pos[i>>11][i&2047]), 8);
    }
  }
  header.docs = V->docs;

  const int len2 = bytes_written;
  off_t off = bytes_written + sizeof (header);
  const unsigned c2 = ~idx_crc32_complement;

  header.created_at = time (NULL);

  const unsigned c1 = compute_crc32 (&header, sizeof (header));
  struct lev_crc32 C;
  C.type = LEV_CRC32;
  C.timestamp = time (NULL);
  C.pos = off;
  C.crc32 = compute_crc32_combine (c1, c2, len2);
  int sz = sizeof (struct lev_crc32);
  writeout (&C, sz);
  flushout ();
  vkprintf (3, "writing binary data done\n");

  if (snapshots_lseek (0) <= 0) {
    kprintf ("[v%lld] Couldn't lseek (0) at least one snapshot for index name %s.\n", V->volume_id, newidxname);
    snapshots_free ();
    return STORAGE_ERR_LSEEK;
  }

  clearin ();
  writeout (&header, sizeof (header));
  flushout ();

  if (snapshots_sync () <= 0) {
    kprintf ("[v%lld] Couldn't sync at least one snapshot for index name %s\n", V->volume_id, newidxname);
    snapshots_free ();
    return STORAGE_ERR_SYNC;
  }

  if (snapshots_close () <= 0) {
    kprintf ("[v%lld] Couldn't close at least one snapshot for index name %s.\n", V->volume_id, newidxname);
    snapshots_free ();
    return STORAGE_ERR_CLOSE;
  }

  vkprintf (1, "[v%lld] writing index done\n", V->volume_id);

  if (snapshots_rename () <= 0) {
    kprintf ("[v%lld] Couldn't rename at least one snapshot for index name %s.\n", V->volume_id, newidxname);
    snapshots_free ();
    return STORAGE_ERR_RENAME;
  }

  snapshots_printname ();

  int r = snapshots_count ();
  snapshots_free ();
  if (r < newidx_fds) {
    kprintf ("[v%lld] created %d snapshots of %d.\n", V->volume_id, r, newidx_fds);
    return -1;
  }

  return 0;
}

int make_empty_binlogs (int N, char *prefix, int md5_mode, int cs_id) {
  char name[32];
  char value_buff[65536];
  sprintf (name, "%d", N - 1);
  sprintf (name, "%%s%%0%dd", (int) strlen (name));
  int i;
  long long volume_id = cs_id * 1000;
  for (i = 0; i < N; i++) {
    sprintf (value_buff, name, prefix, i);
    strcat (value_buff, ".bin");
    struct lev_start *E = malloc (LEV_STORAGE_START_SIZE);
    E->type = LEV_START;
    E->schema_id = STORAGE_SCHEMA_V1;
    E->extra_bytes = 12;
    E->split_mod = N;
    E->split_min = i;
    E->split_max = i+1;
    memcpy (E->str, &volume_id, 8);
    memcpy (&E->str[8], &md5_mode, 4);
    FILE *b = fopen (value_buff, "wb");
    if (b == NULL) {
      kprintf ("fopen (%s, \"wb\") failed\n", value_buff);
      return -2;
    }
    if (fwrite (E, 1, LEV_STORAGE_START_SIZE, b) != LEV_STORAGE_START_SIZE) {
      kprintf ("writing to %s failed\n", value_buff);
      return -3;
    }
    free (E);
    fclose (b);
    volume_id++;
  }
  return 0;
}

/********************************* PHP serialization **************************/
static int get_binlog_file_serialized (char *buffer, storage_binlog_file_t *B) {
  int mode = 0;
  if (B->fd_rdonly >= 0) { mode |= 1; }
  if (B->fd_wronly >= 0) { mode |= 2; }
  return sprintf (buffer, "a:12:{"
                         "s:4:\"mode\";i:%d;"
                         "s:4:\"path\";s:%d:\"%s\";"
                         "s:6:\"dir_id\";i:%d;"
                         "s:4:\"size\";i:%d;"
                         "s:19:\"read_last_fail_time\";i:%d;"
                         "s:21:\"read_sequential_fails\";i:%d;"
                         "s:12:\"read_success\";d:%lld;"
                         "s:10:\"read_fails\";d:%lld;"
                         "s:20:\"fsync_last_fail_time\";i:%d;"
                         "s:22:\"fsync_sequential_fails\";i:%d;"
                         "s:13:\"fsync_success\";d:%lld;"
                         "s:11:\"fsync_fails\";d:%lld;"
                       "}",
    mode,
    (int) strlen (B->abs_filename), B->abs_filename,
    B->dir_id,
    (int) B->size,
    B->st_read.last_fail_time,
    B->st_read.sequential_fails,
    B->st_read.success,
    B->st_read.fails,
    B->st_fsync.last_fail_time,
    B->st_fsync.sequential_fails,
    B->st_fsync.success,
    B->st_fsync.fails
    );
}

static int get_binlog_file_text (char *buffer, storage_binlog_file_t *B, int i) {
  int mode = 0;
  if (B->fd_rdonly >= 0) { mode |= 1; }
  if (B->fd_wronly >= 0) { mode |= 2; }
  char prefix[32];
  sprintf (prefix, "file%d.", i);
  return sprintf (buffer, "%smode\t%d\n"
                          "%spath\t%s\n"
                          "%sdir_id\t%d\n"
                          "%ssize\t%lld\n"
                          "%sread_last_fail_time\t%d\n"
                          "%sread_sequential_fails\t%d\n"
                          "%sread_success\t%lld\n"
                          "%sread_fails\t%lld\n"
                          "%slast_hour_read_fails\t%.6lf\n"
                          "%sfsync_last_fail_time\t%d\n"
                          "%sfsync_sequential_fails\t%d\n"
                          "%sfsync_success\t%lld\n"
                          "%sfsync_fails\t%lld\n",
    prefix, mode,
    prefix, B->abs_filename,
    prefix, B->dir_id,
    prefix, B->size,
    prefix, B->st_read.last_fail_time,
    prefix, B->st_read.sequential_fails,
    prefix, B->st_read.success,
    prefix, B->st_read.fails,
    prefix, B->as_read.counter,
    prefix, B->st_fsync.last_fail_time,
    prefix, B->st_fsync.sequential_fails,
    prefix, B->st_fsync.success,
    prefix, B->st_fsync.fails
    );
}

int get_volume_serialized (char *buffer, long long volume_id) {
  int i;
  char *p = buffer;
  volume_t *V = get_volume_f (volume_id, 0);
  if (V == NULL) {
    return STORAGE_ERR_UNKNOWN_VOLUME_ID;
  }
  p += sprintf (p, "a:%d:{"
                     "s:7:\"binlogs\";i:%d;"
                     "s:8:\"disabled\";i:%d;",
    V->binlogs + 2,
    V->binlogs,
    V->disabled
    );
  for (i = 0; i < V->binlogs; i++) {
    p += sprintf (p, "i:%d;", i);
    p += get_binlog_file_serialized (p, V->B[i]);
  }
  p += sprintf (p, "}");
  return p - buffer;
}

int get_volume_text (char *buffer, long long volume_id) {
  int i;
  char *p = buffer;
  volume_t *V = get_volume_f (volume_id, 0);
  if (V == NULL) {
    return STORAGE_ERR_UNKNOWN_VOLUME_ID;
  }
  p += sprintf (p, "binlogs\t%d\n"
                   "disabled\t%d\n"
                   "docs\t%d\n"
                   "idx_docs\t%d\n",
    V->binlogs,
    V->disabled,
    V->docs,
    V->idx_docs
    );
  for (i = 0; i < V->binlogs; i++) {
    p += get_binlog_file_text (p, V->B[i], i + 1);
  }
  return p - buffer;
}

static int get_dir_serialized (char *buffer, storage_dir_t *D) {
  struct statfs st;
  statfs (D->path, &st);
  double free_space_percent = (100.0 * st.f_bavail) / st.f_blocks;
  return sprintf (buffer, "a:14:{"
                            "s:4:\"path\";s:%d:\"%s\";"
                            "s:7:\"binlogs\";i:%d;"
                            "s:8:\"disabled\";i:%d;"
                            "s:7:\"scanned\";i:%d;"
                            "s:19:\"read_last_fail_time\";i:%d;"
                            "s:21:\"read_sequential_fails\";i:%d;"
                            "s:12:\"read_success\";d:%lld;"
                            "s:10:\"read_fails\";d:%lld;"
                            "s:20:\"fsync_last_fail_time\";i:%d;"
                            "s:22:\"fsync_sequential_fails\";i:%d;"
                            "s:13:\"fsync_success\";d:%lld;"
                            "s:11:\"fsync_fails\";d:%lld;"
                            "s:18:\"free_space_percent\";d:%.10lf;"
                            "s:23:\"pending_aio_connections\";i:%d;"
                          "}",
   (int) strlen (D->path), D->path,
   D->binlogs,
   (int) D->disabled,
   (int) D->scanned,
   D->st_read.last_fail_time,
   D->st_read.sequential_fails,
   D->st_read.success,
   D->st_read.fails,
   D->st_fsync.last_fail_time,
   D->st_fsync.sequential_fails,
   D->st_fsync.success,
   D->st_fsync.fails,
   free_space_percent,
   D->pending_aio_connections
   );
}

int get_dirs_serialized (char *buffer) {
  int i;
  char *p = buffer;
  p += sprintf (p, "a:%d:{", dirs);
  for (i = 0; i < dirs; i++) {
    p += sprintf (p, "i:%d;", i);
    p += get_dir_serialized (p, Dirs + i);
  }
  p += sprintf (p, "}");
  return 0;
}
/**************************** Lock/unlock directories **********************************/
int change_dir_write_status (int dir_id, int disabled) {
  int mask = 1 << dir_id;
  int a = disabled << dir_id;
  int i, j;
  int r = 0;
  if (dir_id < 0 || dir_id >= dirs) {
    return -1;
  }
  storage_dir_t *D = Dirs + dir_id;
  if (D->disabled == disabled) {
    return 0;
  }
  for (i = 0; i < volumes; i++) {
    volume_t *V = Volumes[i];
    storage_binlog_file_t *B = NULL;
    if ((V->disabled & mask) != a) {
      for (j = 0; j < V->binlogs; j++) {
        if (V->B[j]->dir_id == dir_id) {
          B = V->B[j];
          break;
        }
      }
      if (B) {
        V->disabled ^= mask;
        if (disabled) {
          if (B->fd_rdonly >= 0) {
            close (B->fd_rdonly);
            B->fd_rdonly = -1;
          }
          pthread_mutex_lock (&V->mutex_write);
          if (B->fd_wronly >= 0) {
            close (B->fd_wronly);
            B->fd_wronly = -1;
          }
          pthread_mutex_unlock (&V->mutex_write);
        } else {
          if (B->fd_rdonly < 0) {
            int fd = open (B->abs_filename, O_RDONLY);
            if (fd >= 0) {
              B->fd_rdonly = fd;
            }
          }
          pthread_mutex_lock (&V->mutex_write);
          if (B->fd_wronly < 0) {
            int fd = open (B->abs_filename, O_WRONLY);
            if (fd >= 0) {
              struct stat buf;
              if (!fstat (fd, &buf) && buf.st_size == V->cur_log_pos && lock_whole_file (fd, F_WRLCK)) {
                B->fd_wronly = fd;
              } else {
                vkprintf (1, "Didn't open %s in write mode.\n", B->abs_filename);
                close (fd);
              }
            }
          }
          pthread_mutex_unlock (&V->mutex_write);
          memset (&B->st_read, 0, sizeof (B->st_read));
          memset (&B->st_fsync, 0, sizeof (B->st_fsync));
        }
        r++;
      }
    }
  }
  D->disabled = disabled;
  if (!disabled) {
    memset (&D->st_read, 0, sizeof (D->st_read));
    memset (&D->st_fsync, 0, sizeof (D->st_fsync));
  }
  return r;
}

int storage_scan_dir (int dir_id) {
  char binlogname[PATH_MAX];
  if (Dirs[dir_id].scanned) {
    return STORAGE_ERR_SCANDIR_MULTIPLE;
  }
  DIR *D = opendir (Dirs[dir_id].path);
  if (D == NULL) {
    vkprintf (1, "storage_scan_dir: opendir (%s) fail. %m\n", Dirs[dir_id].path);
    return STORAGE_ERR_OPENDIR;
  }

  struct dirent *entry;
  int add_binlog_fails = 0;
  while ( (entry = readdir (D)) != NULL) {
    if (!strcmp (entry->d_name, ".") || !strcmp (entry->d_name, "..")) {
      continue;
    }
    int l = strlen (entry->d_name);
    if (l <= 4 || strcmp (entry->d_name + l - 4, ".bin")) {
      continue;
    }
    if (strlen (Dirs[dir_id].path) + 1 + l > sizeof (binlogname) - 1) {
      kprintf ("Binlog path too long\n");
      exit (1);
    }
    strcpy (binlogname, Dirs[dir_id].path);
    char *p = binlogname + strlen (binlogname);
    if (p[-1] != '/') {
      *p++ = '/';
    }
    strcpy (p, entry->d_name);
    int r = storage_add_binlog (binlogname, dir_id);
    if (r < 0) {
      add_binlog_fails++;
      continue;
    }
    Dirs[dir_id].binlogs++;
  }
  closedir (D);
  Dirs[dir_id].scanned = (Dirs[dir_id].binlogs > 0) ? 1 : 0;
  if (add_binlog_fails > 0) {
    kprintf ("storage_scan_dir (%d): there are %d add_binlog_fails.\n", dir_id, add_binlog_fails);
  }
  return 0;
}

static void rec_max_clique (int n, int *a, int k, int st, int sz, int *res, int *best) {
  int i;
  if (k == n) {
    if (sz < *res) {
      *res = sz;
      *best = st;
    }
    return;
  }
  rec_max_clique (n, a, k + 1, st, sz, res, best);
  for (i = 0; i < k; i++) {
    if (((1 << i) & st) && !(a[i] & (1 << k))) {
      return;
    }
  }
  rec_max_clique (n, a, k + 1, st | (1 << k), sz + 1, res, best);
}

int find_max_clique (int n, int *a) {
  int res = 0, best = -1;
  rec_max_clique (n, a, 0, 0, 0, &res, &best);
  return best;
}
