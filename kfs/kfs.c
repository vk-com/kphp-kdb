/*
    This file is part of VK/KittenPHP-DB-Engine Library.

    VK/KittenPHP-DB-Engine Library is free software: you can redistribute it and/or modify
    it under the terms of the GNU Lesser General Public License as published by
    the Free Software Foundation, either version 2 of the License, or
    (at your option) any later version.

    VK/KittenPHP-DB-Engine Library is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Lesser General Public License for more details.

    You should have received a copy of the GNU Lesser General Public License
    along with VK/KittenPHP-DB-Engine Library.  If not, see <http://www.gnu.org/licenses/>.

    Copyright 2009-2013 Vkontakte Ltd
              2009-2012 Nikolai Durov
              2009-2012 Andrei Lopatin
              2012-2013 Anton Maydell
*/

#define	_FILE_OFFSET_BITS	64

#include <assert.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <stddef.h>
#include <unistd.h>
#include <fcntl.h>
#include <dirent.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <aio.h>
#include <errno.h>
#include <zlib.h>
#include <openssl/sha.h>

#include "crc32.h"
#include "kdb-binlog-common.h"
#include "kdb-data-common.h"
#include "kfs-layout.h"
#include "kfs.h"
#include "xz_dec.h"

#define MAX_DIRNAME_LEN 127
#define MAX_FNAME_LEN 127

#define MAX_REPLICA_FILES 1024
#define START_BUFFER_SIZE 12288

extern int verbosity;

int lock_whole_file (int fd, int mode) {
  static struct flock L;
  L.l_type = mode;
  L.l_whence = SEEK_SET;
  L.l_start = 0;
  L.l_len = 0;
  if (fcntl (fd, F_SETLK, &L) < 0) {
    fprintf (stderr, "cannot lock file %d: %m\n", fd);
    return -1;
  }
  return 1;
}


/*
 (\.[0-9]{6,})?(\.tmp|\.bin|\.bin\.bz)?
 0 = binlog, 1 = snapshot;
 +2 = .tmp
 +4 = not-first (position present)
 +16 = zipped
 +32 = .enc
*/

static int classify_suffix (char *suffix, int suffix_len, long long *MMPos) {
  int c = 0;
  MMPos[0] = MMPos[1] = 0;
  if (!*suffix) {
    return 1;
  }
  if (*suffix != '.') {
    return -1;
  }
  int i = 1;
  while (suffix[i] >= '0' && suffix[i] <= '9') {
    i++;
  }
  if (i > 1) {
    if (i < 7) {
      return -1;
    }
    int power = (suffix[1] - '0') * 10 + (suffix[2] - '0');
    if (power > 18) {
      return -1;
    }

    long long base = strtoll (suffix + 3, 0, 10);

    int cnt = power - (i - 7);
    if (cnt < 0) {
      return -1;
    }

    if (power > 0 && suffix[3] == '0') {
      return -1;
    }

    MMPos[0] = base;
    MMPos[1] = base;

    int j;
    for (j = 0; j < cnt; j++) {
      MMPos[0] *= 10;
      MMPos[1] = MMPos[1] * 10 + 9;
    }

    suffix += i;
    if (!*suffix) {
      return 5;
    }
    if (*suffix != '.') {
      return -1;
    }
    c = 4;
  } else {
    MMPos[0] = 0;
    MMPos[1] = (1LL << 62);
  }

  switch (suffix[1]) {
  case 'b':
    if (!strcmp (suffix, ".bin")) {
      return c;
    }
    return strcmp (suffix, ".bin.bz") ? -1 : (c + 16);
  case 'e':
    return strcmp (suffix, ".enc") ? -1 : c + 32;
  case 't':
    return strcmp (suffix, ".tmp") ? -1 : c + 3;
  }
  return -1;
}

static struct kfs_file_info *t_binlogs[MAX_REPLICA_FILES], *t_snapshots[MAX_REPLICA_FILES];

static inline int cmp_file_info (const struct kfs_file_info *FI, const struct kfs_file_info *FJ) {
  if (FI->min_log_pos < FJ->min_log_pos) {
    return -1;
  } else if (FI->min_log_pos > FJ->min_log_pos) {
    return 1;
  } else {
    return strcmp (FI->filename, FJ->filename);
  }
}

/* storage-engine optimization (replacing O(n^2) sort by qsort) */
static int qsort_cmp_file_info (const void *x, const void *y) {
  return cmp_file_info (*(const struct kfs_file_info **) x, *(const struct kfs_file_info **) y);
}

static void kf_sort (struct kfs_file_info **T, int n) {
  qsort (T, n, sizeof (T[0]), qsort_cmp_file_info);
}

static void file_info_decref (struct kfs_file_info *FI) {
  --FI->refcnt;
  assert (FI->refcnt >= 0);
  if (FI->refcnt) {
    return;
  }
  if (FI->start) {
    free (FI->start);
  }
  if (FI->iv) {
    free (FI->iv);
  }
  if (FI->filename) {
    free (FI->filename);
  }
  free (FI);
}

static const char *basename (const char *path) {
  const char *p = strrchr (path, '/');
  return (p != NULL) ? p + 1 : path;
}

static int keyring_find_key (const char *name, int name_len, unsigned char **key) {
  return -1;
}

static int replica_create_crypto_context (kfs_replica_handle_t R) {
  if (R->ctx_crypto) {
    return 0;
  }
  unsigned char *key;
  const char *after_slash = basename (R->replica_prefix);
  int l2 = strlen (after_slash);
  int key_len = keyring_find_key (after_slash, l2, &key);
  if (key_len < 0) {
    return -1;
  }
  const int l1 = key_len / 4, l = 2 * l1 + l2;
  unsigned char *w = alloca (l);
  memcpy (w, key, l1);
  memcpy (w + l1, after_slash, l2);
  memcpy (w + l1 + l2, key + l1, l1);
  unsigned char t[20];
  unsigned char aes_key[32];
  SHA1 (w, l, t);
  memcpy (aes_key, t + 2, 16);
  memcpy (w, key + 2 * l1, l1);
  memcpy (w + l1, after_slash, l2);
  memcpy (w + l1 + l2, key + 3 * l1, l1);
  SHA1 (w, l, t);
  memcpy (aes_key + 16, t + 2, 16);
  R->ctx_crypto = malloc (sizeof (*R->ctx_crypto));
  vk_aes_set_encrypt_key (R->ctx_crypto, aes_key, 256);
  return 0;
}

static int compute_iv (kfs_replica_handle_t R, const char *filename, int filename_len, unsigned char **iv) {
  unsigned char *key;
  const char *after_slash = basename (R->replica_prefix);
  if (verbosity >= 2) {
    fprintf (stderr, "%s(replica:'%s', filename:'%.*s')\n", __func__, after_slash, filename_len, filename);
  }
  int key_len = keyring_find_key (after_slash, strlen (after_slash), &key);
  if (key_len < 0) {
    return -1;
  }
  const int l1 = key_len / 3, l = 2 * l1 + filename_len;
  unsigned char *w = alloca (l);
  memcpy (w, key, l1);
  memcpy (w + l1, filename, filename_len);
  memcpy (w + l1 + filename_len, key + key_len - l1, l1);
  unsigned char t[20];
  SHA1 (w, l, t);
  *iv = malloc (16);
  if (*iv == NULL) {
    fprintf (stderr, "%s: malloc failed. %m\n", __func__);
    return -1;
  }
  memcpy (*iv, t + 2, 16);
  return 0;
}

int kfs_file_compute_initialization_vector (struct kfs_file_info *FI) {
  kfs_replica_handle_t R = FI->replica;
  assert (R);
  if (R->ctx_crypto == NULL || FI->iv != NULL) {
    return 0;
  }
  const char *name = basename (FI->filename);
  if (compute_iv (R, name, strlen (name), &FI->iv) < 0) {
    return -1;
  }
  return 0;
}

kfs_replica_handle_t open_replica (const char *replica_name, int force) {
  const char *after_slash = strrchr (replica_name, '/');
  static char dirname[MAX_DIRNAME_LEN+1+MAX_FNAME_LEN+1];
  int dirname_len, filename_len;
  int encrypted = 0;

  static struct stat tmp_stat;

  if (after_slash) {
    after_slash++;
    dirname_len = after_slash - replica_name;
    assert (dirname_len < MAX_DIRNAME_LEN);
    memcpy (dirname, replica_name, dirname_len);
    dirname[dirname_len] = 0;
  } else {
    after_slash = replica_name;
    dirname_len = 2;
    dirname[0] = '.';
    dirname[1] = '/';
    dirname[2] = 0;
  }

  filename_len = strlen (after_slash);

  DIR *D = opendir (dirname);
  struct dirent *DE, *DEB;

  if (!D) {
    fprintf (stderr, "kfs_open_replica: cannot open directory %s: %m\n", dirname);
    return 0;
  }

  int dirent_len = offsetof (struct dirent, d_name) + pathconf (dirname, _PC_NAME_MAX) + 1;
  DEB = malloc (dirent_len);
  assert (DEB);

  int len, path_len, name_class, binlogs = 0, snapshots = 0;
  struct kfs_file_info *FI;
  struct kfs_replica *R = 0;

  if (force) {
    R = malloc (sizeof (*R));
    assert (R);
    memset (R, 0, sizeof (*R));
    R->replica_prefix = strdup (replica_name);
    R->replica_prefix_len = strlen (replica_name);
  }
  
  while (1) {
    if (readdir_r (D, DEB, &DE) < 0) {
      dirname[dirname_len] = 0;
      fprintf (stderr, "kfs_open_replica: error while reading directory %s: %m\n", dirname);
      return 0;
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
    if (len < filename_len || memcmp (DE->d_name, after_slash, filename_len)) {
      continue;
    }

    static long long MMPos[2];

    name_class = classify_suffix (DE->d_name + filename_len, len - filename_len, MMPos);

    if (name_class < 0 || (name_class & 3) == 2) {
      continue;
    }

    memcpy (dirname + dirname_len, DE->d_name, len+1);
    path_len = dirname_len + len;

    if (lstat (dirname, &tmp_stat) < 0) {
      fprintf (stderr, "warning: unable to stat %s: %m\n", dirname);
      continue;
    }

    if (S_ISLNK (tmp_stat.st_mode)) {
      name_class |= 8;
      if (stat (dirname, &tmp_stat) < 0) {
        fprintf (stderr, "warning: unable to stat %s: %m\n", dirname);
        continue;
      }
    }
      
    if (!S_ISREG (tmp_stat.st_mode)) {
      continue;
    }

    if (!R) {
      R = malloc (sizeof (*R));
      assert (R);
      memset (R, 0, sizeof (*R));
      R->replica_prefix = strdup (replica_name);
      R->replica_prefix_len = strlen (replica_name);
    }

    if (name_class & 32) {
      encrypted = 1;
      continue;
    }

    FI = malloc (sizeof (struct kfs_file_info));
    assert (FI);

    memset (FI, 0, sizeof (*FI));

    FI->mtime = tmp_stat.st_mtime;
    FI->refcnt = 1;
    FI->replica = R;
    FI->filename = strdup (dirname);
    FI->filename_len = path_len;
    FI->flags = name_class;
    FI->suffix = FI->filename + dirname_len + filename_len;

    FI->min_log_pos = MMPos[0];
    FI->max_log_pos = MMPos[1];

    switch (name_class & 3) {
    case 0:
      FI->kfs_file_type = kfs_binlog;
      break;
    case 1:
      FI->kfs_file_type = kfs_snapshot;
      break;
    case 3:
      FI->kfs_file_type = kfs_partial;
      break;
    }

    FI->file_size = tmp_stat.st_size;
    FI->inode = tmp_stat.st_ino;
    FI->device = tmp_stat.st_dev;

    if (FI->kfs_file_type == kfs_binlog) {
      assert (binlogs < MAX_REPLICA_FILES);
      t_binlogs[binlogs++] = FI;
    } else {
      assert (snapshots < MAX_REPLICA_FILES);
      t_snapshots[snapshots++] = FI;
    }

    FI->log_pos = (name_class & 7) ? -1 : 0;

    if (verbosity > 1) {
      fprintf (stderr, "found file %s, size %lld, name class %d, min pos %lld, max pos %lld\n", dirname, FI->file_size, name_class, FI->min_log_pos, FI->max_log_pos);
    }
  }

  closedir (D);
  free (DEB);

  if (binlogs) {
    int i, m = 0;
    kf_sort (t_binlogs, binlogs);
    for (i = 1; i < binlogs; i++) {
      if ((t_binlogs[i]->flags & 16) && t_binlogs[m]->min_log_pos == t_binlogs[i]->min_log_pos) {
        fprintf (stderr, "warning: skip possible duplicate zipped binlog file '%s', since file '%s' was already found.\n",
          t_binlogs[i]->filename,  t_binlogs[m]->filename);
        file_info_decref (t_binlogs[i]);
      } else {
        t_binlogs[++m] = t_binlogs[i];
      }
    }
    R->binlog_num = m + 1;
    R->binlogs = malloc (sizeof (void *) * R->binlog_num);
    assert (R->binlogs);
    memcpy (R->binlogs, t_binlogs, sizeof (void *) * R->binlog_num);
  }

  if (snapshots) {
    kf_sort (t_snapshots, snapshots);
    R->snapshot_num = snapshots;
    R->snapshots = malloc (sizeof (void *) * snapshots);
    assert (R->snapshots);
    memcpy (R->snapshots, t_snapshots, sizeof (void *) * snapshots);
  }


  if (verbosity > 1) {
    fprintf (stderr, "finished pre-loading KFS replica/slice %s: %d snapshots, %d binlogs\n", replica_name, snapshots, binlogs);
  }

  if (encrypted) {
    if (replica_create_crypto_context (R) < 0) {
      fprintf (stderr, "%s: unable to create encryption context for replica '%s'.\n", __func__, replica_name);
      close_replica (R);
      return NULL;
    }
  }

  return R;
}

static void replica_close_ctx_crypto (kfs_replica_handle_t R) {
  if (R->ctx_crypto) {
    memset (R->ctx_crypto, 0, sizeof (*R->ctx_crypto));
    free (R->ctx_crypto);
    R->ctx_crypto = NULL;
  }
}

int close_replica (kfs_replica_handle_t R) {
  int i;
  if (!R) {
    return 0;
  }
  for (i = 0; i < R->binlog_num; i++) {
    R->binlogs[i]->replica = 0;
    file_info_decref (R->binlogs[i]);
  }
  for (i = 0; i < R->snapshot_num; i++) {
    R->snapshots[i]->replica = 0;
    file_info_decref (R->snapshots[i]);
  }

  if (R->replica_prefix) {
    free (R->replica_prefix);
  }
  if (R->binlogs) {
    free (R->binlogs);
  }
  if (R->snapshots) {
    free (R->snapshots);
  }

  replica_close_ctx_crypto (R);

  free (R);
  return 0;
}

int update_replica (kfs_replica_handle_t R, int force) {
  if (!R) {
    return -1;
  }
  kfs_replica_handle_t RN = open_replica (R->replica_prefix, force);
  if (!RN) {
    return -1;
  }

  replica_close_ctx_crypto (R);
  R->ctx_crypto = RN->ctx_crypto;
  RN->ctx_crypto = NULL;

  int i = 0, j = 0;
  struct kfs_file_info *FI, *FJ, **T;

  while (i < RN->binlog_num && j < R->binlog_num) {
    FI = RN->binlogs[i];
    FJ = R->binlogs[j];
    int r = cmp_file_info (FI, FJ);
    if (r < 0) {
      i++;
    } else if (r > 0) {
      j++;
    } else {
      RN->binlogs[i++] = FJ;
      R->binlogs[j++] = FI;
      assert (FJ->file_size <= FI->file_size);
      FJ->file_size = FI->file_size;
    }
  }

  i = j = 0;
  while (i < RN->snapshot_num && j < R->snapshot_num) {
    FI = RN->snapshots[i];
    FJ = R->snapshots[j];
    int r = cmp_file_info (FI, FJ);
    if (r < 0) {
      i++;
    } else if (r > 0) {
      j++;
    } else {
      RN->snapshots[i++] = FJ;
      R->snapshots[j++] = FI;
      assert (FJ->file_size <= FI->file_size);
      FJ->file_size = FI->file_size;
    }
  }

  i = R->binlog_num;  R->binlog_num = RN->binlog_num;  RN->binlog_num = i;
  T = R->binlogs;     R->binlogs = RN->binlogs;        RN->binlogs = T;

  i = R->snapshot_num;  R->snapshot_num = RN->snapshot_num;  RN->snapshot_num = i;
  T = R->snapshots;     R->snapshots = RN->snapshots;        RN->snapshots = T;

  if (verbosity > 1) {
    fprintf (stderr, "finished reloading file list for replica %s: %d binlogs, %d snapshots (OLD: %d, %d)\n",
      R->replica_prefix, R->binlog_num, R->snapshot_num, RN->binlog_num, RN->snapshot_num);
  }

  close_replica (RN);

  for (i = 0; i < R->binlog_num; i++) {
    R->binlogs[i]->replica = R;
  }

  for (i = 0; i < R->snapshot_num; i++) {
    R->snapshots[i]->replica = R;
  }

  return 1;
}


static int check_kfs_header_basic (struct kfs_file_header *H) {
  assert (H->magic == KFS_MAGIC);
  if (compute_crc32 (H, 4092) != H->header_crc32) {
    return -1;
  }
  if (H->kfs_version != KFS_V01) {
    return -1;
  }
  return 0;
}

int kfs_bz_get_chunks_no (long long orig_file_size) {
  return (orig_file_size + (KFS_BINLOG_ZIP_CHUNK_SIZE - 1)) >> KFS_BINLOG_ZIP_CHUNK_SIZE_EXP;
}

int kfs_bz_compute_header_size (long long orig_file_size) {
  int chunks = kfs_bz_get_chunks_no (orig_file_size);
  return sizeof (kfs_binlog_zip_header_t) + 8 * chunks + 4;
}

static int process_first36_bytes (struct kfs_file_info *FI, int fd, int r, struct lev_start *E) {
  switch (E->type) {
    case LEV_START:
      assert (r >= sizeof (struct lev_start) - 4);
      FI->log_pos = 0;
      break;
    case LEV_ROTATE_FROM:
      assert (r >= sizeof (struct lev_rotate_from));
      FI->log_pos = ((struct lev_rotate_from *)E)->cur_log_pos;
      if (FI->khdr && FI->khdr->file_id_hash != ((struct lev_rotate_from *)E)->cur_log_hash) {
        fprintf (stderr, "warning: binlog file %s has different hash in header (%016llX) and continue record (%016llX)\n", FI->filename, FI->khdr->file_id_hash, ((struct lev_rotate_from *)E)->cur_log_hash);
        assert (close (fd) >= 0);
        return -2;
      }
      if (FI->khdr && FI->khdr->prev_log_hash != ((struct lev_rotate_from *)E)->prev_log_hash) {
        fprintf (stderr, "warning: binlog file %s has different hash of previous binlog in header (%016llX) and continue record (%016llX)\n", FI->filename, FI->khdr->prev_log_hash, ((struct lev_rotate_from *)E)->prev_log_hash);
        assert (close (fd) >= 0);
        return -2;
      }
      if (FI->khdr && FI->khdr->log_pos_crc32 != ((struct lev_rotate_from *)E)->crc32) {
        fprintf (stderr, "warning: binlog file %s has different crc32 in header (%08X) and continue record (%08X)\n", FI->filename, FI->khdr->log_pos_crc32, ((struct lev_rotate_from *)E)->crc32);
        assert (close (fd) >= 0);
        return -2;
      }
      if (FI->khdr && FI->khdr->prev_log_time != ((struct lev_rotate_from *)E)->timestamp) {
        fprintf (stderr, "warning: binlog file %s has different timestamp in header (%d) and continue record (%d)\n", FI->filename, FI->khdr->prev_log_time, ((struct lev_rotate_from *)E)->timestamp);
        assert (close (fd) >= 0);
        return -2;
      }
      break;
    default:
      fprintf (stderr, "warning: binlog file %s begins with wrong entry type %08x\n", FI->filename, E->type);
      assert (close (fd) >= 0);
      return -2;
  }
  return 0;
}

static int process_binlog_zip_header (struct kfs_file_info *FI, int fd, kfs_binlog_zip_header_t *H) {
  int r = FI->preloaded_bytes;
  assert (r >= sizeof (kfs_binlog_zip_header_t));
  if (H->orig_file_size < 36 || H->orig_file_size > KFS_BINLOG_ZIP_MAX_FILESIZE) {
    fprintf (stderr, "wrong binlog zip header in the file '%s', illegal orig_file_size (%lld)\n", FI->filename, H->orig_file_size);
    assert (close (fd) >= 0);
    return -1;
  }
  int header_size = kfs_bz_compute_header_size (H->orig_file_size);
  if (r < header_size) {
    FI->start = realloc (FI->start, header_size);
    assert (FI->start);
    H = (kfs_binlog_zip_header_t *) FI->start;
    int sz = header_size - r, w = read (fd, FI->start + r, sz);
    if (w < 0) {
      fprintf (stderr, "%s: fail to read %d bytes of chunk table for the file '%s'. %m\n", __func__, sz, FI->filename);
      assert (close (fd) >= 0);
      return -1;
    }
    if (w != sz) {
      fprintf (stderr, "%s: read %d of expected %d bytes of header for the file '%s'.\n", __func__, w, sz, FI->filename);
      assert (close (fd) >= 0);
      return -1;
    }
    if (FI->iv) {
      kfs_replica_handle_t R = FI->replica;
      assert (R && R->ctx_crypto);
      R->ctx_crypto->ctr_crypt (R->ctx_crypto, (unsigned char *) FI->start + r, (unsigned char *) FI->start + r, sz, FI->iv, r);
    }
    r = FI->preloaded_bytes = header_size;
  }

  int chunks = kfs_bz_get_chunks_no (H->orig_file_size);
  unsigned int *header_crc32 = (unsigned int *) &H->chunk_offset[chunks];

  if (compute_crc32 (H, header_size - 4) != *header_crc32) {
    fprintf (stderr, "%s: corrupted zipped binlog header in the file '%s', CRC32 failed.\n", __func__, FI->filename);
    assert (close (fd) >= 0);
    return -1;
  }
  return process_first36_bytes (FI, fd, 36, (struct lev_start *) H->first36_bytes);
}

int preload_file_info (struct kfs_file_info *FI) {
  if (!FI->start) {
    int fd = open (FI->filename, O_RDONLY);
    if (fd < 0) {
      fprintf (stderr, "Cannot open %s file %s: %m\n", FI->flags & 1 ? "snapshot" : "binlog", FI->filename);
      return -2;
    }
    if (kfs_file_compute_initialization_vector (FI) < 0) {
      fprintf (stderr, "Cannot compute AES initialization vector for %s file %s.\n", FI->flags & 1 ? "snapshot" : "binlog", FI->filename);
      return -2;
    }
    FI->start = malloc (START_BUFFER_SIZE);
    assert (FI->start);
    int r = read (fd, FI->start, START_BUFFER_SIZE);
    if (r < 0) {
      fprintf (stderr, "Cannot read %s file %s: %m\n", FI->flags & 1 ? "snapshot" : "binlog", FI->filename);
      assert (close (fd) >= 0);
      free (FI->start);
      FI->start = 0;
      return -2;
    }
    FI->preloaded_bytes = r;
    if (FI->iv) {
      kfs_replica_handle_t R = FI->replica;
      assert (R && R->ctx_crypto);
      R->ctx_crypto->ctr_crypt (R->ctx_crypto, (unsigned char *) FI->start, (unsigned char *) FI->start, r, FI->iv, 0ULL);
    }
    struct kfs_file_header *kfs_Hdr = (struct kfs_file_header *) FI->start;
    int headers = 0;

    if (r >= 4096 && kfs_Hdr[0].magic == KFS_MAGIC) {
      if (check_kfs_header_basic (kfs_Hdr) < 0) {
        fprintf (stderr, "bad kfs header #0\n");
        assert (close (fd) >= 0);
        free (FI->start);
        FI->start = 0;
        return -2;
      }
      headers++;
      if (r >= 8192 && kfs_Hdr[1].magic == KFS_MAGIC) {
        if (check_kfs_header_basic (kfs_Hdr + 1) < 0) {
          fprintf (stderr, "bad kfs header #1\n");
          assert (close (fd) >= 0);
          free (FI->start);
          FI->start = 0;
          return -2;
        }
        headers++;
        if (kfs_Hdr[1].header_seq_num == kfs_Hdr[0].header_seq_num) {
          assert (!memcmp (kfs_Hdr + 1, kfs_Hdr, 4096));
        }
      }
    }

    FI->khdr = headers ? kfs_Hdr : 0;
    if (headers > 1 && kfs_Hdr[1].header_seq_num > kfs_Hdr[0].header_seq_num) {
      FI->khdr++;
    }

    assert (!headers || FI->khdr->data_size + headers * 4096 == FI->khdr->raw_size);
    assert (!headers || FI->khdr->kfs_file_type == FI->kfs_file_type);

    FI->kfs_headers = headers;

    if (FI->kfs_file_type == kfs_binlog) {

      struct lev_start *E = (struct lev_start *) (kfs_Hdr + headers);

      r -= 4096 * headers;

      switch (E->type) {
      case LEV_START:
      case LEV_ROTATE_FROM:
        if (FI->flags & 16) {
          fprintf (stderr, "error: zipped binlog file '%s' starts from LEV_START or LEV_ROTATE_FROM.\n", FI->filename);
          assert (close (fd) >= 0);
          return -2;
        }
        if (process_first36_bytes (FI, fd, r, E) < 0) {
          return -2;
        }
        break;
      case KFS_BINLOG_ZIP_MAGIC:
        if (headers) {
          fprintf (stderr, "error: zipped binlog file '%s' contains KFS headers\n", FI->filename);
          assert (close (fd) >= 0);
          return -2;
        }
        if (!(FI->flags & 16)) {
          fprintf (stderr, "error: not zipped binlog file '%s' contains KFS_BINLOG_ZIP_MAGIC\n", FI->filename);
          assert (close (fd) >= 0);
          return -2;
        }
        if (process_binlog_zip_header (FI, fd, (kfs_binlog_zip_header_t *) E) < 0) {
          return -2;
        }
        break;
      default:
        fprintf (stderr, "warning: binlog file %s begins with wrong entry type %08x\n", FI->filename, E->type);
        assert (close (fd) >= 0);
        return -2;
      }

      if (FI->khdr && FI->khdr->log_pos != FI->log_pos) {
        fprintf (stderr, "warning: binlog file %s has different starting position in header (%lld) and starting record (%lld)\n", FI->filename, FI->khdr->log_pos, FI->log_pos);
        assert (close (fd) >= 0);
        return -2;
      }

      if (FI->log_pos < FI->min_log_pos || FI->log_pos > FI->max_log_pos) {
        fprintf (stderr, "warning: binlog file %s starts from position %lld (should be in %lld..%lld)\n", FI->filename, FI->log_pos, FI->min_log_pos, FI->max_log_pos);
        assert (close (fd) >= 0);
        return -2;
      }

      if (verbosity > 1) {
        fprintf (stderr, "preloaded binlog file info for %s (%lld bytes, %d headers), covering %lld..%lld, name corresponds to %lld..%lld\n", FI->filename, FI->file_size, headers, FI->log_pos, FI->log_pos + FI->file_size - 4096 * headers, FI->min_log_pos, FI->max_log_pos);
      }
    }

    if (FI->khdr && FI->replica) {
      if (!FI->khdr->replica_id_hash) {
        fprintf (stderr, "warning: binlog file %s has zero replica_id_hash, skipping\n", FI->filename);
        assert (close (fd) >= 0);
        return -2;
      }
      if (!FI->replica->replica_id_hash) {
        FI->replica->replica_id_hash = FI->khdr->replica_id_hash;
      } else if (FI->replica->replica_id_hash != FI->khdr->replica_id_hash) {
        fprintf (stderr, "warning: binlog file %s has incorrect replica_id_hash %016llx != %016llx\n", FI->filename, FI->khdr->replica_id_hash, FI->replica->replica_id_hash);
      }
    }

    assert (lseek (fd, 4096 * headers, SEEK_SET) == 4096 * headers);

    return fd;
  } else {
    return -1;
  }
}

kfs_binlog_zip_header_t *load_binlog_zip_header (struct kfs_file_info *FI) {
  if (!(FI->flags & 16) || FI->kfs_file_type != kfs_binlog) {
    fprintf (stderr, "%s: file '%s' isn't zipped binlog\n", __func__, FI->filename);
    return 0;
  }
  assert (FI->flags & 16);
  const int fd = preload_file_info (FI);
  if (fd == -2) {
    fprintf (stderr, "%s: preload_file_info for file '%s' failed.\n", __func__, FI->filename);
    return 0;
  }
  if (fd >= 0) {
    assert (!close (fd));
  }
  return ((kfs_binlog_zip_header_t *) FI->start);
}

static inline long long kfs_get_binlog_file_size (struct kfs_file_info *FI) {
  if (!(FI->flags & 16)) {
    return FI->file_size - 4096 * FI->kfs_headers;
  }
  kfs_binlog_zip_header_t *H = (kfs_binlog_zip_header_t *) FI->start;
  assert (H);
  return H->orig_file_size;
}

long long get_binlog_start_pos (kfs_replica_handle_t R, int binlog_id, long long *end_pos) {
  if (!R || binlog_id < 0 || binlog_id >= R->binlog_num) {
    return -1;
  }
  int fd = preload_file_info (R->binlogs[binlog_id]);
  if (fd == -2) {
    return -1;
  }
  if (fd >= 0) {
    assert (close (fd) >= 0);
  }
  struct kfs_file_info *FI = R->binlogs[binlog_id];
  assert (FI->kfs_file_type == kfs_binlog && FI->log_pos >= 0 && FI->file_size >= 0);
  if (end_pos) {
    *end_pos = FI->log_pos + kfs_get_binlog_file_size (FI);
  }
  return FI->log_pos;
} 


kfs_file_handle_t open_binlog (kfs_replica_handle_t R, long long log_pos) {
  if (!R) {
    return 0;
  }

  int l = -1, r = R->binlog_num;
  while (r - l > 1) { // Z[l].min_log_pos <= log_pos < Z[r].min_log_pos
    int m = (l + r) >> 1;
    if (R->binlogs[m]->min_log_pos <= log_pos) {
      l = m;
    } else {
      r = m;
    }
  }
  if (l < 0) {
    if (verbosity > 1) {
      fprintf (stderr, "asked for log_pos %lld, minimal possible log_pos %lld\n", log_pos, R->binlogs ? R->binlogs[0]->min_log_pos : -1);
    }
    return 0;
  }

  int p = l;

  //while (p >= 0 && log_pos <= R->binlogs[p]->max_log_pos + R->binlogs[p]->file_size) {
  while (p >= 0) {
    struct kfs_file_info *FI = R->binlogs[p];
    /* This cutoff works only for not zipped binlogs.
       In the zipped binlog case, we must preload zipped binlog header for obtaining original file size.
    */
    if (!(FI->flags & 16) && (log_pos > FI->max_log_pos + FI->file_size)) {
      break;
    }
    int fd = preload_file_info (FI);
    if (fd == -2) {
      --p;
      continue;
    }

    assert (FI->log_pos >= 0 && FI->file_size > 0);

    long long log_pos_end = FI->log_pos + kfs_get_binlog_file_size (FI);

    if (log_pos > log_pos_end) {
      if (fd >= 0) {
        assert (close (fd) >= 0);
      }
      break;
    }

    if (log_pos >= FI->log_pos) {
      /* found */
      if (verbosity > 1) {
        fprintf (stderr, "binlog position %lld found in file %s, covering %lld..%lld\n", log_pos, FI->filename, FI->log_pos, log_pos_end);
      }
      if (fd < 0) {
        fd = open (FI->filename, O_RDONLY);
        if (fd < 0) {
          fprintf (stderr, "cannot open binlog file %s: %m\n", FI->filename);
          return 0;
        }
      }

      long long file_pos = log_pos - FI->log_pos + 4096 * FI->kfs_headers;
      //fprintf (stderr, "FI->file_size:%lld, FI->flags: %d, FI->kfs_headers:%d, file:%s, file_pos: %lld, log_pos: %lld\n", FI->file_size, FI->flags, FI->kfs_headers, FI->filename, file_pos, log_pos);
      assert (lseek (fd, file_pos, SEEK_SET) == file_pos);

      struct kfs_file *F = malloc (sizeof (struct kfs_file));
      assert (F);
      memset (F, 0, sizeof (*F));
      F->info = FI;
      F->fd = fd;
      F->offset = FI->kfs_headers * 4096;
      FI->refcnt++;

      return F;
    }

    if (fd >= 0) {
      assert (close (fd) >= 0);
    }

    --p;
  }

  return 0;
}

int kfs_close_file (kfs_file_handle_t F, int close_handle) {
  if (!F) {
    return 0;
  }
  if (F->fd >= 0) {
    if (close_handle) {
      assert (close (F->fd) >= 0);
    }
    F->fd = -1;
  }
  if (F->info) {
    file_info_decref (F->info);
    F->info = 0;
  }
  free (F);
  return 0;
}

int close_binlog (kfs_file_handle_t F, int close_handle) {
  return kfs_close_file (F, close_handle);
}

kfs_file_handle_t next_binlog (kfs_file_handle_t F) {
  struct kfs_file_info *FI = F->info, *FI2;
  struct kfs_replica *R = FI->replica;

  assert (F->fd >= 0);
  assert (R);

  int l = -1, r = R->binlog_num;
  while (r - l > 1) { // Z[l].min_log_pos <= log_pos < Z[r].min_log_pos
    int m = (l + r) >> 1;
    if (R->binlogs[m]->min_log_pos <= FI->log_pos) {
      l = m;
    } else {
      r = m;
    }
  }

  while (l >= 0 && R->binlogs[l] != FI) {
    l--;
  }

  assert (l >= 0);

  long long log_pos = FI->log_pos + kfs_get_binlog_file_size (FI);

  if (verbosity > 2) {
    fprintf (stderr, "next_binlog(%p): FI=%p, R=%p, l=%d, binlogs=%d, log_pos=%lld\n", F, FI, R, l, R->binlog_num, log_pos);
  }

  l++;
  if (l == R->binlog_num) {
    return 0;
  }

  FI2 = R->binlogs[l];
  int fd = preload_file_info (FI2);

  if (fd == -2) {
    return 0;
  }

  if (FI2->log_pos != log_pos || log_pos <= 0) {
    return 0;
  }

  long long log_pos_end = log_pos + kfs_get_binlog_file_size (FI2);

  if (verbosity > 1) {
    fprintf (stderr, "next binlog file %s, covering %lld..%lld\n", FI2->filename, log_pos, log_pos_end);
  }
  if (fd < 0) {
    fd = open (FI2->filename, O_RDONLY);
    if (fd < 0) {
      fprintf (stderr, "cannot open binlog file %s: %m\n", FI2->filename);
      return 0;
    }
  }
  F = malloc (sizeof (struct kfs_file));
  assert (F);
  memset (F, 0, sizeof (*F));
  F->info = FI2;
  F->fd = fd;
  F->offset = FI2->kfs_headers * 4096;
  FI2->refcnt++;

  return F;
}

int binlog_is_last (kfs_file_handle_t F) {
  assert (F && F->info && F->info->log_pos >= 0 && F->info->kfs_file_type == kfs_binlog && F->info->replica);

  struct kfs_file_info *FI = F->info;
  struct kfs_replica *R = FI->replica;

  if (R && (!R->binlog_num || R->binlogs[R->binlog_num - 1] != FI)) {
    return 0;
  }
  return 1;
}

/* (re)opens binlog in write mode and positions at the end; it must be the last in chain
   returns resulting absolute log position or -1 */
long long append_to_binlog_ext (kfs_file_handle_t F, int allow_read) {
  assert (F && F->info && F->info->log_pos >= 0 && F->info->kfs_file_type == kfs_binlog && F->info->replica);

  int fd, old_fd = -1;
  struct kfs_file_info *FI = F->info;
  struct kfs_replica *R = FI->replica;

  if (F->fd >= 0) {
    old_fd = F->fd;
    assert (close (F->fd) >= 0);
    F->fd = -1;
  }

  if (R && (!R->binlog_num || R->binlogs[R->binlog_num - 1] != FI)) {
    fprintf (stderr, "cannot append to last read binlog file %s: newer binlog %s already exists\n", FI->filename, R->binlogs[R->binlog_num - 1]->filename);
    return -1;
  }

  fd = open (FI->filename, allow_read ? O_RDWR : O_WRONLY);
  if (fd < 0) {
    fprintf (stderr, "cannot reopen binlog file %s in write mode: %m\n", FI->filename);
    return -1;
  }
  if (old_fd > 0 && fd != old_fd) {
    assert (dup2 (fd, old_fd) == old_fd);
    close (fd);
    fd = old_fd;
  }
  long long file_size = lseek (fd, 0, SEEK_END);
  assert (file_size > FI->kfs_headers * 4096);
  FI->file_size = file_size;
  F->fd = fd;
  F->lock = 0;
  F->offset = FI->kfs_headers * 4096;

  if (lock_whole_file (fd, F_WRLCK) <= 0) {
    fprintf (stderr, "cannot lock binlog file %s for writing\n", FI->filename);
    return -1;
  }

  F->lock = -1;

  return FI->log_pos + file_size - F->offset;
}

long long append_to_binlog (kfs_file_handle_t F) {
  if (F->info->flags & 16) {
    fprintf (stderr, "%s: file '%s' is zipped binlog, couldn't append to it.\n", __func__, F->info->filename);
    return -1;
  }
  return append_to_binlog_ext (F, 0);
}

static char *Suffixes[4] = {"", ".bin", ".tmp", ".bin.bz"};


char *create_new_name (struct kfs_file_info *FI, long long log_pos, const char *replica_prefix, int suffix_id) {
  assert ((unsigned) suffix_id < 4);
  int ex = 0, replica_prefix_len;
  long long power = 10000;
  while (log_pos > power) {
    power *= 10;
    ex++;
  }
  static char buff[32];
  int l = sprintf (buff, "%02d%04lld", ex, log_pos);
  int l2 = 6;

  if (!FI) {
    assert (replica_prefix);
    replica_prefix_len = strlen (replica_prefix);
  } else {
    replica_prefix = FI->filename;
    replica_prefix_len = FI->suffix - FI->filename;
  }

  if (FI && (FI->flags & 4)) {
    char *suffix = FI->suffix;
    assert (*suffix == '.');
    suffix++;
    int s_len = 0;
    while (suffix[s_len] >= '0' && suffix[s_len] <= '9') {
      s_len++;
    }
    assert (s_len >= 6 && s_len <= 20);

    if (!memcmp (suffix, buff, 6)) {
      while (l2 <= s_len && suffix[l2-1] == buff[l2-1]) {
        l2++;
      }
      if (l2 > l) {
        fprintf (stderr, "cannot find name for file corresponding to binlog pos %lld: even %.*s.%s%s exists\n", log_pos, replica_prefix_len, replica_prefix, buff, Suffixes[suffix_id]);
        return 0;
      }
    }
  }

  assert ((unsigned) replica_prefix_len <= MAX_FNAME_LEN);

  int new_filename_len = replica_prefix_len + l2 + (suffix_id ? 5 : 1);  // .xxx.bin\0
  if (suffix_id == 3) {
    new_filename_len += 3; //.bz
  }
  char *filename = malloc (new_filename_len + 1);

  assert (filename);

  assert (sprintf (filename, "%.*s.%.*s%s", replica_prefix_len, replica_prefix, l2, buff, Suffixes[suffix_id]) == new_filename_len);

  return filename;
}

kfs_file_handle_t create_next_binlog_ext (kfs_file_handle_t F, long long start_log_pos, kfs_hash_t new_file_hash, int allow_read, int zipped) {
  struct kfs_file_info *FI = F->info, *FI2;
  struct kfs_replica *R = FI->replica;
  struct kfs_file *F2;

  assert (F->fd >= 0);
  assert (R);

  if (R->binlog_num <= 0 || R->binlogs[R->binlog_num - 1] != FI) {
    fprintf (stderr, "cannot create next binlog for %s: it is not last in chain\n", FI->filename);
    return 0;
  }

  if (start_log_pos < FI->log_pos + kfs_get_binlog_file_size (FI)) {
    fprintf (stderr, "incorrect start log position %lld for next binlog file\n", start_log_pos);
    return 0;
  }

  char *filename = create_new_name (FI, start_log_pos, 0, zipped ? 3 : 1);

  int fd = open (filename, (allow_read ? O_RDWR : O_WRONLY) | O_CREAT | O_EXCL, 0640);
  if (fd < 0) {
    fprintf (stderr, "cannot create next binlog file %s: %m\n", filename);
    return 0;
  }

  if (lock_whole_file (fd, F_WRLCK) <= 0) {
    fprintf (stderr, "cannot lock binlog file %s for writing\n", filename);
    assert (close (fd) >= 0);
    return 0;
  }

  if (verbosity > 0) {
    fprintf (stderr, "creating next binlog file %s\n", filename);
  }

  FI2 = malloc (sizeof (struct kfs_file_info));
  F2 = malloc (sizeof (struct kfs_file));

  assert (FI2 && F2);

  memset (FI2, 0, sizeof (*FI2));
  memset (F2, 0, sizeof (*F2));

  F2->info = FI2;
  F2->fd = fd;
  FI2->file_size = 0;
  F2->lock = -1;
  F2->offset = 0;

  FI2->replica = R;
  FI2->refcnt = 2;
  FI2->kfs_file_type = kfs_binlog;
  FI2->filename = filename;
  FI2->filename_len = strlen (filename);
  FI2->flags = 4 + (zipped ? 16 : 0);
  FI2->suffix = FI->suffix - FI->filename + filename;
  
  FI2->file_hash = new_file_hash;

  FI2->file_size = 0;

  FI2->log_pos = start_log_pos;

  assert (classify_suffix (FI2->suffix, strlen (FI2->suffix), &FI2->min_log_pos) == FI2->flags);
  assert (FI2->min_log_pos <= start_log_pos && start_log_pos <= FI2->max_log_pos);

  static struct stat tmp_stat;
  assert (fstat (fd, &tmp_stat) >= 0);

  FI2->mtime = tmp_stat.st_mtime;
  FI2->inode = tmp_stat.st_ino;
  FI2->device = tmp_stat.st_dev;

  R->binlogs = realloc (R->binlogs, (R->binlog_num + 1) * sizeof (void *));
  R->binlogs[R->binlog_num++] = FI2;

  if (R->ctx_crypto) {
    assert (!kfs_file_compute_initialization_vector (FI2));
  }

  return F2;
}

kfs_file_handle_t create_next_binlog (kfs_file_handle_t F, long long start_log_pos, kfs_hash_t new_file_hash) {
  return create_next_binlog_ext (F, start_log_pos, new_file_hash, 0, 0);
}

kfs_file_handle_t create_first_binlog (kfs_replica_handle_t R, char *start_data, int start_size, int strict_naming, int allow_read, int zipped) {
  assert (R);

  if (R->binlog_num != 0) {
    fprintf (stderr, "cannot create first binlog for %s: binlog files already exist\n", R->replica_prefix);
    return 0;
  }

  char *filename = malloc (R->replica_prefix_len + (strict_naming ? 7 + 4 + 1 : 4 + 1) + (zipped ? 3 : 0));
  assert (filename);
  memcpy (filename, R->replica_prefix, R->replica_prefix_len);
  char *ptr = filename + R->replica_prefix_len;
  if (strict_naming) {
    memcpy (ptr, ".000000.bin", 12);
  } else {
    memcpy (ptr, ".bin", 5);
  }

  if (zipped) {
    strcat (ptr, ".bz");
  }

  int fd = open (filename, (allow_read ? O_RDWR : O_WRONLY) | O_CREAT | O_EXCL, 0640);
  if (fd < 0) {
    fprintf (stderr, "cannot create next binlog file %s: %m\n", filename);
    return 0;
  }

  if (lock_whole_file (fd, F_WRLCK) <= 0) {
    fprintf (stderr, "cannot lock binlog file %s for writing\n", filename);
    assert (close (fd) >= 0);
    unlink (filename);
    return 0;
  }

  if (verbosity > 0) {
    fprintf (stderr, "creating next binlog file %s\n", filename);
  }

  if (start_size && write (fd, start_data, start_size) != start_size) {
    fprintf (stderr, "cannot write first %d bytes to new binlog file %s\n", start_size, filename);
    assert (close (fd) >= 0);
    unlink (filename);
    return 0;
  }

  struct kfs_file_info *FI2 = malloc (sizeof (struct kfs_file_info));
  struct kfs_file *F2 = malloc (sizeof (struct kfs_file));

  assert (FI2 && F2);

  memset (FI2, 0, sizeof (*FI2));
  memset (F2, 0, sizeof (*F2));

  F2->info = FI2;
  F2->fd = fd;
  FI2->file_size = start_size;
  F2->lock = -1;
  F2->offset = 0;

  FI2->replica = R;
  FI2->refcnt = 2;
  FI2->kfs_file_type = kfs_binlog;
  FI2->filename = filename;
  FI2->filename_len = strlen (filename);
  FI2->flags = (strict_naming ? 4 : 0) + (zipped ? 16 : 0);
  FI2->suffix = filename + R->replica_prefix_len;
  
  FI2->file_hash = 0;

  FI2->file_size = start_size;

  FI2->log_pos = 0;

  assert (classify_suffix (FI2->suffix, strlen (FI2->suffix), &FI2->min_log_pos) == FI2->flags);
  assert (!FI2->min_log_pos && FI2->max_log_pos >= 0);

  static struct stat tmp_stat;
  assert (fstat (fd, &tmp_stat) >= 0);

  FI2->mtime = tmp_stat.st_mtime;
  FI2->inode = tmp_stat.st_ino;
  FI2->device = tmp_stat.st_dev;
  assert (tmp_stat.st_size == start_size);

  R->binlogs = realloc (R->binlogs, sizeof (void *));
  R->binlogs[R->binlog_num++] = FI2;

  if (R->ctx_crypto) {
    assert (!kfs_file_compute_initialization_vector (FI2));
  }

  return F2;
}

kfs_file_handle_t open_snapshot (kfs_replica_handle_t R, int snapshot_index) {
  if (!R) {
    return 0;
  }
  assert (0 <= snapshot_index && snapshot_index < R->snapshot_num);

  if (R->snapshots[snapshot_index]->flags & 2) {
    return 0;
  }

  struct kfs_file_info *FI = R->snapshots[snapshot_index];

  int fd = preload_file_info (FI);

  if (fd == -2) {
    return 0;
  }

  if (verbosity > 1) {
    fprintf (stderr, "trying to open snapshot file %s\n", FI->filename);
  }
  if (fd < 0) {
    fd = open (FI->filename, O_RDONLY);
    if (fd < 0) {
      fprintf (stderr, "cannot open snapshot file %s: %m\n", FI->filename);
      return 0;
    }
    assert (lseek (fd, 4096 * FI->kfs_headers, SEEK_SET) == 4096 * FI->kfs_headers);
  }

  if (lock_whole_file (fd, F_RDLCK) <= 0) {
    assert (close (fd) >= 0);
    fprintf (stderr, "cannot lock snapshot file %s: %m\n", FI->filename);
    return 0;
  }

  struct kfs_file *F = malloc (sizeof (struct kfs_file));
  assert (F);
  memset (F, 0, sizeof (*F));
  F->info = FI;
  F->fd = fd;
  F->lock = 1;
  F->offset = FI->kfs_headers * 4096;
  FI->refcnt++;

  return F;
}

kfs_file_handle_t open_recent_snapshot (kfs_replica_handle_t R) {
  if (!R) {
    return 0;
  }
  if (verbosity > 1) {
    fprintf (stderr, "opening last snapshot file\n");
  }

  struct kfs_file *F = 0;
  int p = R->snapshot_num - 1;
  while (p >= 0 && (F = open_snapshot (R, p)) == 0) {
    --p;
  }
  return F;
}

int close_snapshot (kfs_file_handle_t F, int close_handle) {
  return kfs_close_file (F, close_handle);
}

kfs_file_handle_t create_new_snapshot (kfs_replica_handle_t Replica, long long log_pos) {
  return 0;
}

char *get_new_snapshot_name (kfs_replica_handle_t R, long long log_pos, const char *replica_prefix) {
  struct kfs_file_info *FI = 0;

  if (R && R->snapshot_num) {
    FI = R->snapshots[R->snapshot_num - 1];
  }

  return create_new_name (FI, log_pos, replica_prefix, 2);
}

int rename_temporary_snapshot (const char *name) {
  int l = strlen (name);
  assert (l >= 4 && !strcmp (name + l - 4, ".tmp") && l <= 256);
  static char tmpbuff[256];
  memcpy (tmpbuff, name, l - 4);
  tmpbuff[l - 4] = 0;
  if (verbosity > 1) {
    fprintf (stderr, "renaming temporary snapshot %s to %s\n", name, tmpbuff);
  }
  if (!access (tmpbuff, 0)) {
    fprintf (stderr, "fatal: snapshot %s already exists\n", tmpbuff);
    return -1;
  }
  return rename (name, tmpbuff);
}

int print_snapshot_name (const char *name) {
  int l = strlen (name);
  assert (l >= 4 && !strcmp (name + l - 4, ".tmp") && l <= 256);
  return printf ("%.*s\n", l - 4, name);
}


/* ---------------- SPECIAL KFS FUNCTIONS FOR ENGINE INITIALISATION ---------------- */

char *engine_replica_name, *engine_snapshot_replica_name;
kfs_replica_handle_t engine_replica, engine_snapshot_replica;

kfs_file_handle_t Snapshot, Binlog, NewSnapshot;

char *engine_snapshot_name;
long long engine_snapshot_size;

int engine_preload_filelist (const char *main_replica_name, const char *aux_replica_name) {
  int l = strlen (main_replica_name);
  if (!aux_replica_name || !*aux_replica_name || !strcmp (aux_replica_name, ".bin") 
     || !strcmp (aux_replica_name, main_replica_name)
     || (!strncmp (aux_replica_name, main_replica_name, l) && !strcmp (aux_replica_name + l, ".bin"))) {
    engine_snapshot_replica_name = engine_replica_name = strdup (main_replica_name);
  } else {
    int l2 = strlen (aux_replica_name);
    if (l2 > 4 && !strcmp (aux_replica_name + l2 - 4, ".bin")) {
      l2 -= 4;
    }
    engine_snapshot_replica_name = strdup (main_replica_name);
    if (aux_replica_name[0] == '.') {
      engine_replica_name = malloc (l + l2 + 1);
      assert (engine_replica_name);
      memcpy (engine_replica_name, main_replica_name, l);
      memcpy (engine_replica_name + l, aux_replica_name, l2);
      engine_replica_name[l+l2] = 0;
    } else {
      engine_replica_name = malloc (l2 + 1);
      assert (engine_replica_name);
      memcpy (engine_replica_name, aux_replica_name, l2);
      engine_replica_name[l2] = 0;
    }
  }
  assert (engine_replica_name && engine_snapshot_replica_name);

  engine_replica = open_replica (engine_replica_name, 0);
  if (!engine_replica) {
    return -1;
  }
  if (engine_snapshot_replica_name == engine_replica_name) {
    engine_snapshot_replica = engine_replica;
  } else {
    engine_snapshot_replica = open_replica (engine_snapshot_replica_name, 1);
    if (!engine_snapshot_replica) {
      return 0;
    }
  }

  return 1;
}

static struct kfs_file_info *kfs_file_info_alloc (const char *filename, int cut_backup_suffix) {
  struct stat st;
  if (stat (filename, &st) < 0) {
    fprintf (stderr, "error: unable to stat %s: %m\n", filename);
    return NULL;
  }

  struct kfs_file_info *FI = calloc (sizeof (*FI), 1);
  if (FI == NULL) {
    return NULL;
  }

  FI->mtime = st.st_mtime;
  FI->file_size = st.st_size;
  FI->inode = st.st_ino;
  FI->device = st.st_dev;
  FI->refcnt = 1;
  FI->filename = strdup (filename);
  FI->filename_len = strlen (filename);

  char *p = strrchr (FI->filename, '/');
  FI->suffix = p ? (p + 1) : FI->filename;
  p = strchr (FI->suffix, '.');
  if (p) {
    FI->suffix = p;
  }

  int l = strlen (FI->suffix);
  if (cut_backup_suffix) {
    if (l > 11 && FI->suffix[l-11] == '.') {
      int i;
      for (i = 1; i <= 10; i++) {
        if (FI->suffix[l-i] < '0' || FI->suffix[l-i] > '9') {
          break;
        }
      }
      if (i > 10) {
        l -= 11;
      }
    }
  }

  long long MMPos[2];
  const char last = FI->suffix[l];
  FI->suffix[l] = 0;
  FI->flags = classify_suffix (FI->suffix, l, MMPos);
  FI->suffix[l] = last;
  FI->min_log_pos = MMPos[0];
  FI->max_log_pos = MMPos[1];
  if (FI->flags >= 0) {
    switch (FI->flags & 3) {
      case 0:
        FI->kfs_file_type = kfs_binlog;
        break;
      case 1:
        FI->kfs_file_type = kfs_snapshot;
        break;
      case 3:
        FI->kfs_file_type = kfs_partial;
        break;
    }
  }
  return FI;
}

kfs_file_handle_t kfs_open_file (const char *filename, int cut_backup_suffix) {
  struct kfs_file_info *FI = kfs_file_info_alloc (filename, cut_backup_suffix);
  if (!FI) {
    return 0;
  }
  if (FI->flags < 0) {
    fprintf (stderr, "%s: classify suffix for file '%s' failed.\n", __func__, filename);
    file_info_decref (FI);
    return 0;
  }
  kfs_file_handle_t F = calloc (sizeof (struct kfs_file), 1);
  assert (F);
  F->fd = preload_file_info (FI);
  if (F->fd < 0) {
    fprintf (stderr, "%s: preload_file_info for file '%s' failed.\n", __func__, filename);
    file_info_decref (FI);
    free (F);
    return 0;
  }
  F->info = FI;
  F->offset = FI->kfs_headers * 4096;
  return F;
}

int kfs_bz_decode (kfs_file_handle_t F, long long off, void *dst, int *dest_len, int *disk_bytes_read) {
  if (verbosity >= 3) {
    fprintf (stderr, "%s: off = %lld, dst = %p, *dest_len = %d\n", __func__, off, dst, *dest_len);
  }
  if (disk_bytes_read) {
    *disk_bytes_read = 0;
  }
  struct kfs_file_info *FI = F->info;
  kfs_binlog_zip_header_t *H = (kfs_binlog_zip_header_t *) FI->start;
  assert (H);
  const int chunks = kfs_bz_get_chunks_no (H->orig_file_size), fd = F->fd;

  if (off < 0) {
    fprintf (stderr, "%s: negative file offset '%lld', file '%s'.\n", __func__, off, FI->filename);
    return -1;
  }

  if (off > H->orig_file_size) {
    fprintf (stderr, "%s: file offset '%lld' is greater than original file size '%lld', file '%s'.\n", __func__, off, H->orig_file_size, FI->filename);
    return -1;
  }

  if (off == H->orig_file_size) {
    *dest_len = 0;
    return 0;
  }

  const long long chunk_no_ll = off >> KFS_BINLOG_ZIP_CHUNK_SIZE_EXP;
  if (chunk_no_ll >= chunks) {
    fprintf (stderr, "%s: chunk_no (%lld) >= chunks (%d)\n", __func__, chunk_no_ll, chunks);
    return -1;
  }

  int chunk_no = (int) chunk_no_ll;
  int avail_out = *dest_len, written_bytes = 0;
  int o = off & (KFS_BINLOG_ZIP_CHUNK_SIZE - 1);

  while (chunk_no < chunks) {
    const long long chunk_size = (chunk_no < chunks - 1 ? H->chunk_offset[chunk_no + 1] : FI->file_size) - H->chunk_offset[chunk_no];
    const long long chunk_offset = H->chunk_offset[chunk_no];
    if (chunk_size <= 0) {
      fprintf (stderr, "%s: not positive chunk size (%lld), broken header(?), file: %s\n, chunk: %d, chunk_offset: %lld\n", __func__, chunk_size, FI->filename, chunk_no, chunk_offset);
      return -1;
    }
    if (chunk_size > KFS_BINLOG_ZIP_MAX_ENCODED_CHUNK_SIZE) {
      fprintf (stderr, "%s: chunk size (%lld) > KFS_BINLOG_ZIP_MAX_ENCODED_CHUNK_SIZE (%d), file: %s, chunk: %d, chunk_offset: %lld\n",
          __func__, chunk_size, KFS_BINLOG_ZIP_MAX_ENCODED_CHUNK_SIZE, FI->filename, chunk_no, chunk_offset);
      return -1;
    }

    int expected_output_bytes = chunk_no == chunks - 1 ? (H->orig_file_size & (KFS_BINLOG_ZIP_CHUNK_SIZE - 1)) : KFS_BINLOG_ZIP_CHUNK_SIZE;

    if (avail_out < expected_output_bytes) {
      break;
    }

    static unsigned char src[KFS_BINLOG_ZIP_MAX_ENCODED_CHUNK_SIZE];
    if (verbosity >= 3) {
      fprintf (stderr, "chunk_no: %d, chunks: %d, chunk_size: %lld, chunk_off: %lld\n", chunk_no, chunks, chunk_size, chunk_offset);
    }
    if (lseek (fd, chunk_offset, SEEK_SET) == (off_t) -1) {
      fprintf (stderr, "%s: lseek to chunk (%d), offset %lld of file '%s' failed. %m\n", __func__, chunk_no, chunk_offset, FI->filename);
      return -1;
    }
    ssize_t r = read (fd, src, chunk_size);
    if (r < 0) {
      fprintf (stderr, "%s: read chunk (%d), offset %lld of file '%s' failed. %m\n", __func__, chunk_no, chunk_offset, FI->filename);
      return -1;
    }
    if (disk_bytes_read) {
      *disk_bytes_read += r;
    }
    if (r != chunk_size) {
      fprintf (stderr, "%s: read only %lld of expected %lld bytes, chunk (%d), offset %lld, file '%s'.\n",
          __func__, (long long) r, chunk_size, chunk_no, chunk_offset, FI->filename);
      return -1;
    }
    if (FI->iv) {
      kfs_replica_handle_t R = FI->replica;
      assert (R && R->ctx_crypto);
      R->ctx_crypto->ctr_crypt (R->ctx_crypto, src, src, chunk_size, FI->iv, chunk_offset);
    }
    if (verbosity >= 1) {
      fprintf (stderr, "%s: read %lld bytes from the file '%s', chunk: %d.\n", __func__, (long long) r, FI->filename, chunk_no);
    }

    int m = expected_output_bytes;
    uLongf destLen;
    int res;
    switch (H->format & 15) {
      case kfs_bzf_zlib:
        destLen = m;
        res = uncompress (dst, &destLen, src, chunk_size);
        if (res != Z_OK) {
          fprintf (stderr, "%s: uncompress returns error code %d, chunk %d, offset %lld, file '%s'.\n", __func__, res, chunk_no, chunk_offset, FI->filename);
          return -1;
        }
        m = (int) destLen;
        break;
      case kfs_bzf_xz:
        res = xz_uncompress2 (dst, &m, src, chunk_size);
        if (res < 0) {
          fprintf (stderr, "%s: xz_uncompress returns error code %d, chunk %d, offset %lld, file '%s'.\n", __func__, res, chunk_no, chunk_offset, FI->filename);
          return -1;
        }
        break;
      default:
        fprintf (stderr, "%s: Unimplemented format '%d' in the file '%s'.\n", __func__, H->format & 15, FI->filename);
        return -1;
    }
    if (expected_output_bytes != m) {
      fprintf (stderr, "%s: expected chunks size is %d, but decoded bytes number is %d, file: '%s', chunk_no: %d, chunk_offset: %lld\n",  __func__, expected_output_bytes, m, FI->filename, chunk_no, chunk_offset);
      return -1;
    }

    int w = -1;
    if (o > 0) {
      w = m - o;
      if (w <= 0) {
        break;
      }
      memmove (dst, dst + o, w);
    } else {
      w = m;
    }
    assert (w >= 0);
    dst += w;
    avail_out -= w;
    written_bytes += w;
    o = 0;
    chunk_no++;
  }
  *dest_len = written_bytes;
  return 0;
}

int kfs_get_tag (unsigned char *start, int size, unsigned char tag[16]) {
  struct lev_start *E = (struct lev_start *) start;
  if (size < 24 || E->extra_bytes < 0 || E->extra_bytes > 4096) { return -2; }
  if (E->type != LEV_START) {
    return -1;
  }
  int s = 24 + ((E->extra_bytes + 3) & -4);
  if (size < s) { return -2; }
  start += s;
  size -= s;
  struct lev_tag *T = (struct lev_tag *) start;
  if (T->type != LEV_TAG) {
    return -1;
  }
  if (size < 20) {
    return -2;
  }
  memcpy (tag, T->tag, 16);
  return 0;
}

int kfs_sws_open (kfs_snapshot_write_stream_t *S, kfs_replica_handle_t R, long long log_pos, long long jump_log_pos) {
  S->R = R;
  S->newidxname = NULL;
  S->iv = NULL;
  if (R == NULL) {
    fprintf (stderr, "%s: R == NULL (engine snapshot replica wasn't open?).\n", __func__);
    exit (1);
  }
  S->newidxname = get_new_snapshot_name (R, log_pos, R->replica_prefix);
  if (!S->newidxname || S->newidxname[0] == '-') {
    fprintf (stderr, "%s: cannot write index: cannot compute its name\n", __func__);
    exit (1);
  }
  if (log_pos == jump_log_pos) {
    fprintf (stderr, "%s: skipping generation of new snapshot %s for position %lld: snapshot for this position already exists\n", __func__, S->newidxname, jump_log_pos);
    return 0;
  }
  if (R->ctx_crypto) {
    const char *name = basename (S->newidxname);
    int name_len = strlen (name) - 4; /* truncate '.tmp' */
    if (compute_iv (R, name, name_len, &S->iv) < 0) {
      fprintf (stderr, "%s: cannot compute initialization vector for file '%.*s'.\n", __func__, name_len, S->newidxname);  
      exit (1);
    }
  }
  if (verbosity >= 1) {
    fprintf (stderr, "%s: creating index %s at log position %lld\n", __func__, S->newidxname, log_pos);
  }
  S->newidx_fd = open (S->newidxname, O_CREAT | O_TRUNC | O_WRONLY | O_EXCL, 0660);
  if (S->newidx_fd < 0) {
    fprintf (stderr, "%s: cannot create new index file %s: %m\n", __func__, S->newidxname);
    exit (1);
  }
  return 1;
}

void kfs_sws_close (kfs_snapshot_write_stream_t *S) {
  if (S->iv) {
    free (S->iv);
    S->iv = NULL;
  }
  if (fsync (S->newidx_fd) < 0) {
    fprintf (stderr, "%s: fsyncing file '%s' failed. %m\n", __func__, S->newidxname);
    exit (1);
  }
  assert (!close (S->newidx_fd));
  S->newidx_fd = -1;
  if (rename_temporary_snapshot (S->newidxname)) {
    fprintf (stderr, "cannot rename new index file from %s: %m\n", S->newidxname);
    unlink (S->newidxname);
    exit (1);
  }
  print_snapshot_name (S->newidxname);
}

static void buffer_crypt (kfs_replica_handle_t R, unsigned char *buff, long long size, unsigned char iv[16], long long off) {
  assert (R && R->ctx_crypto && size >= 0);
  if (!size) {
    return;
  }
  while (1) {
    int w = 0x7ffffff0 < size ? 0x7ffffff0 : size;
    R->ctx_crypto->ctr_crypt (R->ctx_crypto, buff, buff, w, iv, off);
    size -= w;
    if (!size) {
      break;
    }
    buff += w;
    off += w;
  }
}

static void kfs_sws_crypt (kfs_snapshot_write_stream_t *S, void *buff, long long size) {
  if (S->iv) {
    long long off = lseek (S->newidx_fd, 0, SEEK_CUR);
    if (off < 0) {
      fprintf (stderr, "%s: lseek failed. %m\n", __func__);
      exit (1);
    }
    buffer_crypt (S->R, (unsigned char *) buff, size, S->iv, off);
  }
}

void kfs_sws_write (kfs_snapshot_write_stream_t *S, void *buff, long long count) {
  assert (count >= 0);
  if (!count) {
    return;
  }
  kfs_sws_crypt (S, buff, count);
  long long w = write (S->newidx_fd, buff, count);
  if (w < 0) {
    fprintf (stderr, "%s: write to the file '%s' failed. %m\n", __func__, S->newidxname);
    exit (1);
  }
  if (w != count) {
    fprintf (stderr, "%s: write only %lld bytes of expected %lld bytes to the file '%s'.\n", __func__, w, count, S->newidxname);
    exit (1);
  }
}

void kfs_sws_safe_write (kfs_snapshot_write_stream_t *S, const void *buff, long long count) {
  assert (count >= 0);
  if (!count) {
    return;
  }
  kfs_sws_write (S, (void *) buff, count);
  kfs_sws_crypt (S, (void *) buff, count); /* restore buff */
}

void kfs_buffer_crypt (kfs_file_handle_t F, void *buff, long long size, long long off) {
  assert (off >= 0);
  if (F && F->info && F->info->iv) {
    buffer_crypt (F->info->replica, (unsigned char *) buff, size, F->info->iv, off);
  }
}

long long kfs_read_file (kfs_file_handle_t F, void *buff, long long size) {
  long long off = -1;
  if (F->info && F->info->iv) {
    off = lseek (F->fd, 0, SEEK_CUR);
    if (off < 0) {
      fprintf (stderr, "%s: cannot obtain offset of the file '%s'. %m\n", __func__, F->info->filename);
      exit (1);
    }
  }
  long long r = read (F->fd, buff, size);
  if (r < 0) {
    fprintf (stderr, "%s: error reading file '%s'. %m\n", __func__, F->info->filename);
    exit (1);
  }
  if (off >= 0) {
    kfs_buffer_crypt (F, buff, r, off);
  }
  return r;
}
