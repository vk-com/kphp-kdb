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

    Copyright 2012-2013 Vkontakte Ltd
              2012-2013 Anton Maydell
*/

#define _FILE_OFFSET_BITS 64

#include <assert.h>
#include <stdio.h>
#include <stdarg.h>
#include <stddef.h>
#include <ctype.h>
#include <string.h>
#include <stdlib.h>
#include <fcntl.h>
#include <unistd.h>
#include <time.h>
#include <dirent.h>
#include <sys/stat.h>

#include "kdb-data-common.h"
#include "server-functions.h"
#include "kfs.h"
#include "md5.h"
#include "crc32.h"

#ifndef COMMIT
#define COMMIT "unknown"
#endif

#define	VERSION_STR	"check-binlog-1.22"
const char FullVersionStr[] = VERSION_STR " compiled at " __DATE__ " " __TIME__ " by gcc " __VERSION__ " "
#ifdef __LP64__
  "64-bit"
#else
  "32-bit"
#endif
" after commit " COMMIT;

#define mytime() get_utime (CLOCK_MONOTONIC)

static const char *BACKUP_ALL_SUBOPTIONS = "FLMdr";
static const int EXIT_ERROR_LIMIT = 100;
static int allow_first_rotate_from = 0;

static long long heap_memory = 128 << 20;
static int keep_going = 0, check_snapshot = 1, no_unixtime_suffix_in_backups = 0, quick_test = 0;
static long long max_log_errors = 0;

typedef enum {
  ll_none    = 0,
  ll_warning = 1,
  ll_error   = 2,
  ll_fatal   = 3
} log_level_t;

static void log_printf (log_level_t level, const char *format, ...)  __attribute__ ((format (printf, 2, 3)));
static void log_warning (const char *format, ...)  __attribute__ ((format (printf, 1, 2)));
static void log_error (const char *format, ...)  __attribute__ ((format (printf, 1, 2)));
static void log_fatal (const char *format, ...)  __attribute__ ((format (printf, 1, 2)));

#define PREVSIZE 64
#define BUFFSIZE 0x1000000

static struct {
  long long max_errors_limit;
  long long warnings;
  long long errors;
  long long fatals;
  long long total_bytes_read[3];
  double total_disk_reading_time[3];
  double total_sleeping_time[3];
  double estimated_binlog_growth;
} stats;
static long long disk_reading_limit = -1LL;
static double inv_disk_reading_limit;

struct file_read_info {
  double start_time;
  double end_time;
  struct file_read_info *next;
  size_t size;
};

 /* reading files with limited speed */
typedef struct {
  long long readto_off;
  long long orig_file_size;      /* original binlog slice size, KFS headers isn't counted */
  double growth_speed;
  kfs_file_handle_t F;
  int type; /* 0 - binlog, 1 - backup, 2 - snapshot */
  int buffsize;
} stream_t;

inline const char *type_to_str (int type) {
  static const char *msg_type[3] = {"binlog", "backup", "snapshot"};
  assert (type >= 0 && type < 3);
  return msg_type[type];
}

/******************** backups ********************/
static const char *backups_dir = NULL, *backups_suboptions = NULL;

typedef enum {
  bc_none    = 0,
  bc_partial = 1,
  bc_full    = 2
} backup_check_t;

struct {
  backup_check_t check_type;
  log_level_t check_level;
  int warn_bad_backups_which_could_be_deleted;
  int warn_redundant_good_backups;
  log_level_t shorter_middle_log_level;
  log_level_t shorter_last_log_level;
} backups_features;

typedef enum backup_status {
  bs_ok         = 0,
  bs_differ     = 1,
  bs_io_error   = 2,
  bs_larger     = 4,
  bs_same_inode = 8
} backup_status_t;

typedef struct backup_file {
  stream_t S;
  char *filename;
  struct backup_file *next, *hnext;
  int binlog_prefix_len;
  backup_status_t status;
} backup_file_t;

#define BACKUPS_HASH_PRIME 10007
static backup_file_t *B[BACKUPS_HASH_PRIME];

static int backup_parse_suboptions (void) {
  if (backups_suboptions == NULL) {
    kprintf ("Backup suboptions wasn't specified (use -B switch).\n");
    return -1;
  }
  if (!strcmp (backups_suboptions, "all")) {
    backups_suboptions = BACKUP_ALL_SUBOPTIONS ;
  }
  const char *s;
  int bc_opts = 0;
  for (s = backups_suboptions; *s; s++) {
    switch (*s) {
    case 'd':
      backups_features.warn_bad_backups_which_could_be_deleted = 1;
    break;
    case 'r':
      backups_features.warn_redundant_good_backups = 1;
    break;
    case 'L':
    case 'l':
      backups_features.shorter_last_log_level = isupper (*s) ? ll_error : ll_warning;
    break;
    case 'M':
    case 'm':
      backups_features.shorter_middle_log_level = isupper (*s) ? ll_error : ll_warning;
    break;
    case 'F':
    case 'f':
      bc_opts++;
      backups_features.check_type = bc_full;
      backups_features.check_level = isupper (*s) ? ll_error : ll_warning;
    break;
    case 'p':
    case 'P':
      bc_opts++;
      backups_features.check_type = bc_partial;
      backups_features.check_level = isupper (*s) ? ll_error : ll_warning;
    break;
    default:
      kprintf ("Unimplemented suboption: '%c'\n", *s);
      return -1;
    }
  }

  if (bc_opts != 1) {
    kprintf ("Backup suboptions should contain exactly one option from the list ['f', 'F', 'p', 'P'] or equal to 'all'.\n");
    return -1;
  }

  return 0;
}

static backup_file_t *backup_file_alloc (char *filename, int len) {
  //vkprintf (4, "%s: %s, %d\n", __func__, filename, len);
  backup_file_t *V = zmalloc0 (sizeof (backup_file_t));
  V->filename = zstrdup (filename);
  V->binlog_prefix_len = len;
  return V;
}

static void backup_file_free (backup_file_t *F) {
  assert (F && F->filename);
  if (F->next) {
    backup_file_free (F->next);
    F->next = 0;
  }
  zfree (F->filename, strlen (F->filename) + 1);
  zfree (F, sizeof (*F));
}

static void backup_hash_init (void) {
  int i;
  for (i = 0; i < 10007; i++) {
    backup_file_t *F, *W;
    for (F = B[i]; F; F = W) {
      W = F->hnext;
      backup_file_free (F);
    }
    B[i] = 0;
  }
}

backup_file_t *get_backup_file_f (char *filename, int len, int force) {
  vkprintf (4, "%s: filename: %.*s, force: %d\n", __func__, len, filename, force);

  if (len >= 3 && !memcmp (filename + (len - 3), ".bz", 3)) {
    len -= 3;
  }

  unsigned int h = 0;
  int i;
  for (i = 0; i < len; i++) {
    h = h * 239 + (filename[i]);
  }
  h %= BACKUPS_HASH_PRIME;
  backup_file_t **p = B + h, *V;
  while (*p) {
    V = *p;
    if (V->binlog_prefix_len == len && !memcmp (V->filename, filename, len)) {
      *p = V->hnext;
      if (force >= 0) {
        V->hnext = B[h];
        if (force > 0 && strcmp (V->filename, filename)) {
          backup_file_t *W = backup_file_alloc (filename, len);
          W->next = V;
          V = W;
        }
        B[h] = V;
      }
      return V;
    }
    p = &V->hnext;
  }
  if (force > 0) {
    V = backup_file_alloc (filename, len);
    V->hnext = B[h];
    return B[h] = V;
  }
  return NULL;
}

static int get_dir_modtime (const char *dirname) {
  struct stat st;
  if (stat (dirname, &st) < 0) {
    log_fatal ("%s: stat for directory '%s' failed. %m\n", __func__, dirname);
    exit (1);
  }
  assert (S_ISDIR (st.st_mode));
  return st.st_mtime;
}

static void find_backups (void) {
  if (!backups_dir) {
    return;
  }
  static int last_update_time = 0;
  int mtime = get_dir_modtime (backups_dir);
  if (last_update_time >= mtime) {
    return;
  }

  backup_hash_init ();

  DIR *D = opendir (backups_dir);
  struct dirent *DE, *DEB;

  if (D == NULL) {
    kprintf ("%s: cannot open directory %s: %m\n", __func__, backups_dir);
    exit (1);
  }

  int dirent_len = offsetof (struct dirent, d_name) + pathconf (backups_dir, _PC_NAME_MAX) + 1;
  DEB = zmalloc0 (dirent_len);
  while (1) {
    if (readdir_r (D, DEB, &DE) < 0) {
      kprintf ("%s: error while reading directory %s: %m\n", __func__, backups_dir);
      exit (1);
    }
    if (!DE) {
      break;
    }
    assert (DE == DEB);
    if (DE->d_type != DT_REG && DE->d_type != DT_UNKNOWN) {
      continue;
    }
    char *s = DE->d_name;
    const int len = strlen (s);
    int i = len;
    if (!no_unixtime_suffix_in_backups) {
      for (i = len - 1; i >= 0 && isdigit (s[i]); i--);
      if (i < 0 || i != len - 11 || s[i] != '.') {
        continue;
      }
    }
    backup_file_t *B = get_backup_file_f (DE->d_name, i, 1);
    assert (B);
  }
  closedir (D);
  last_update_time = time (NULL);
  zfree (DEB, dirent_len);
}

/* binlog file */
typedef struct {
  stream_t S;
  long long crc32_off;
  long long start_log_pos;
  long long total_correct_crc32_logevents, total_incorrect_crc32_logevents, total_incorrect_timestamp;
  long long stats_errors_before_reading;
  struct kfs_replica *R;
  kfs_file_handle_t binlog;
  backup_file_t *backups;
  unsigned int start_crc32, log_crc32;
  int corrupted;
  int unprocessed_bytes;
  int last;
  struct lev_start start;
  struct lev_rotate_from rotate_from;
  struct lev_rotate_to rotate_to;
} file_t;

static int buffsize = BUFFSIZE;
static unsigned char io_buff[PREVSIZE + BUFFSIZE];
static unsigned char backup_buff[BUFFSIZE];

/******************** stream ********************/
static int stream_same_inode (stream_t *A, stream_t *B) {
  return A->F->info->device == B->F->info->device && A->F->info->inode == B->F->info->inode;
}

static long long stream_tell (stream_t *S) {
  return S->readto_off + S->F->offset;
}

static int stream_eof (stream_t *S) {
  if (S->readto_off > S->orig_file_size) {
    log_fatal ("%s: file '%s' was read after it's size! readto_off: %lld, orig_file_size: %lld", __func__, S->F->info->filename, S->readto_off, (long long) S->orig_file_size);
    exit (1);
  }
  return S->readto_off == S->orig_file_size;
}

static int stream_is_zipped (stream_t *S) {
  assert (S->F && S->F->info && (S->F->info->kfs_file_type == kfs_binlog || S->F->info->kfs_file_type == kfs_snapshot));
  return (S->F->info->flags & 16) ? 1 : 0;
}

static void stream_check_hash (stream_t *S, long long computed_file_hash, long long rotate_to_cur_log_hash) {
  if (computed_file_hash == rotate_to_cur_log_hash) {
    return;
  }
  if (stream_is_zipped (S)) {
    kfs_binlog_zip_header_t *H = (kfs_binlog_zip_header_t *) S->F->info->start;
    if (H->file_hash != rotate_to_cur_log_hash) {
      log_fatal ("in %s '%s' zipped header file_hash field (0x%016llx) isn't equal to LEV_ROTATE_TO cur_log_hash field (0x%016llx).", type_to_str (S->type), S->F->info->filename, H->file_hash, rotate_to_cur_log_hash);
    } else {
      log_warning ("in %s '%s' computed file hash (0x%016llx) isn't equal LEV_ROTATE_TO cur_log_hash field (0x%016llx). KFS header probably was truncated during packing.", type_to_str (S->type), S->F->info->filename, computed_file_hash, rotate_to_cur_log_hash);
    }
  } else {
    struct kfs_file_info *FI = S->F->info;
    if (!FI->khdr) {
      log_error ("in %s '%s' computed file hash (0x%016llx) isn't equal LEV_ROTATE_TO cur_log_hash field (0x%016llx). No KFS headers.", type_to_str (S->type), S->F->info->filename, computed_file_hash, rotate_to_cur_log_hash);
    } else if (FI->khdr->file_id_hash != rotate_to_cur_log_hash) {
      log_error ("in %s '%s' computed file hash (0x%016llx) isn't equal LEV_ROTATE_TO cur_log_hash field (0x%016llx). file_id_hash in KFS headers is equal to 0x%016llx.",
      type_to_str (S->type), S->F->info->filename, computed_file_hash, rotate_to_cur_log_hash, FI->khdr->file_id_hash);
    }
  }
}

static int stream_seek (stream_t *S, long long off) {
  if (S->readto_off == off) {
    return 0;
  }
  if (!stream_is_zipped (S)) {
    if (lseek (S->F->fd, S->F->offset + off, SEEK_SET) != S->F->offset + off) {
      kprintf ("%s: lseek to offset %lld for file '%s' failed, file size is %lld bytes. %m\n", __func__, S->F->offset + off, S->F->info->filename, S->F->info->file_size);
      return -1;
    }
  }
  S->readto_off = off;
  return 0;
}

static void stream_close (stream_t *S) {
  if (S->F) {
    vkprintf (3, "%s: '%s'.\n", __func__, S->F->info->filename);
    kfs_close_file (S->F, 1);
    S->F = 0;
  }
}

static int stream_open (stream_t *S, kfs_file_handle_t H, const char *filename, int tp) {
  assert (tp >= 0 && tp < 3);
  assert (H || filename);
  assert (!H || !filename);
  memset (S, 0, sizeof (*S));
  if (!H) {
    H = kfs_open_file (filename, tp == 1 ? 1 : 0);
    if (!H) {
      vkprintf (1, "kfs_open_file (\"%s\", %d) returns NULL.\n", filename, tp);
      return -1;
    }
  } else {
    /* replica could be opened long time ago, so we need update file's stat */
    struct stat st;
    if (fstat (H->fd, &st) < 0) {
      kprintf ("%s: fstat for '%s' failed. %m\n", __func__, H->info->filename);
      return -1;
    }
    H->info->file_size = st.st_size;
    H->info->mtime = st.st_mtime;
  }
  S->F = H;
  S->type = tp;
  S->buffsize = buffsize;
  S->orig_file_size = S->F->info->file_size;

  if (tp != 2 && S->F->info->flags & 16) {
    S->buffsize = KFS_BINLOG_ZIP_CHUNK_SIZE;
    S->orig_file_size = ((kfs_binlog_zip_header_t *) S->F->info->start)->orig_file_size;
    assert (S->orig_file_size >= 0);
  } else {
    S->orig_file_size -= S->F->offset;
  }

  vkprintf (2, "open file '%s', file_size: %lld, original size: %lld\n", S->F->info->filename, S->F->info->file_size, S->orig_file_size);

  return 0;
}

static int stream_update (stream_t *S) {
  assert (S && S->F);
  struct kfs_file_info *FI = S->F->info;
  assert (FI);
  struct stat st;
  if (fstat (S->F->fd, &st) < 0) {
    log_fatal ("%s: fstat for '%s' failed, fd: %d. %m\n", __func__, FI->filename, S->F->fd);
    return -1;
  }
  if (st.st_size < FI->file_size) {
    log_fatal ("%s: file '%s' was unexpectedly truncated? size at open is %lld bytes, currect size is %lld bytes.",
      __func__, FI->filename, FI->file_size, (long long) st.st_size);
    return -1;
  }
  long long growth = st.st_size - FI->file_size;
  int dt = st.st_mtime - FI->mtime;
  FI->file_size = st.st_size;
  FI->mtime = st.st_mtime;
  if (dt > 0) {
    S->growth_speed = (long double) growth / (long double) dt;
  }
  if (!(FI->flags & 16)) {
    S->orig_file_size += growth;
  }
  return 0;
}

static int stream_read (stream_t *S, void *buff, size_t size) {
  int disk_bytes_read = size;
  if (stream_is_zipped (S)) {
    assert (buff == backup_buff || buff == &io_buff[PREVSIZE] || buff == io_buff);
    int s = KFS_BINLOG_ZIP_CHUNK_SIZE > size ? KFS_BINLOG_ZIP_CHUNK_SIZE : size;
    if (kfs_bz_decode (S->F, S->readto_off, buff, &s, &disk_bytes_read) < 0) {
      kprintf ("kfs_bz_decode file '%s' from offset %lld failed.\n", S->F->info->filename, S->readto_off);
      return -1;
    }
    if (s < size) {
      kprintf ("kfs_bz_decode file '%s' from offset %lld retuned %d bytes, expected %d bytes.\n", S->F->info->filename, S->readto_off, s, (int) size);
      return -1;
    }
  } else {
    ssize_t s = read (S->F->fd, buff, size);
    if (s != size) {
      if (s < 0) {
        kprintf ("read '%s' from offset %lld fail. %m\n", S->F->info->filename, stream_tell (S));
        return -1;
      } else {
        stats.total_bytes_read[S->type] += s;
        kprintf ("read '%s' from offset %lld. Read %lld bytes of expected %lld.\n", S->F->info->filename, stream_tell (S), (long long) s, (long long) size);
        vkprintf (2, "lseek (%d, 0, SEEK_CUR) = %lld\n", S->F->fd, (long long ) lseek (S->F->fd, 0, SEEK_CUR));
        return -1;
      }
    }
  }
  stats.total_bytes_read[S->type] += disk_bytes_read;
  S->readto_off += size;
  return size;
}

static int stream_read_limited (stream_t *F, void *buff, size_t size) {
  static struct {
    struct file_read_info *head, *tail;
  } I;
  assert (size <= F->buffsize);
  if (disk_reading_limit < 0) {
    double start_time = mytime ();
    int r = stream_read (F, buff, size);
    stats.total_disk_reading_time[F->type] += mytime () - start_time;
    return r;
  }
  #define N 2
  static double T[N] = {10.0, 1.0};
  const double cur_time = mytime (), cut_time = cur_time - T[0];
  struct file_read_info *p, *w;
  for (p = I.head; p != NULL && p->end_time < cut_time; p = w) {
    w = p->next;
    zfree (p, sizeof (*p));
  }
  I.head = p;
  if (p == NULL) {
    I.tail = NULL;
  }

  int i;
  const double avg_disk_reading_speed = stats.total_disk_reading_time[F->type] < 1e-6 ? (64 << 20) : stats.total_bytes_read[F->type] / stats.total_disk_reading_time[F->type];
  const double t_avg_reading_buff = size / avg_disk_reading_speed;
  vkprintf (4, "expected reading %lld bytes time is %.3lf seconds\n", (long long) size, t_avg_reading_buff);
  double sleeping_time = 0.0;
  for (i = 0; i < N; i++) {
    double ct = cur_time - T[i];
    double cs = 0.0; /* average bytes read last T[i] seconds */
    for (p = I.head; p != NULL && p->end_time < ct; p = p->next);
    while (p != NULL) {
      cs += p->start_time >= ct ? p->size : p->size * (p->end_time - ct) / (p->end_time - p->start_time);
      p = p->next;
    }
    vkprintf (4, "Last %.3lf seconds was read %.3lfMi in average\n", T[i], cs / 1048576.0);
    double t = (cs + size) * inv_disk_reading_limit - T[i] - t_avg_reading_buff;
    if (sleeping_time < t) {
      sleeping_time = t;
    }
    vkprintf (3, "According last %.3lf seconds stats, sleeping_time is %.3lf\n", T[i], t);
  }

  if (sleeping_time > 0) {
    long long u = (sleeping_time * 1000000) + 0.5;
    stats.total_sleeping_time[F->type] += u * 0.000001;
    vkprintf (3, "sleep for %lld microseconds\n", u);
    usleep (u % 1000000);
    u /= 1000000;
    if (u > 0) {
      sleep (u);
    }
  }
  p = zmalloc0 (sizeof (*p));
  p->size = size;
  if (I.tail) {
    I.tail->next = p;
  } else {
    I.head = p;
  }
  I.tail = p;
  p->start_time = mytime ();
  int r = stream_read (F, buff, size);
  p->end_time = mytime ();
  stats.total_disk_reading_time[F->type] += p->end_time - p->start_time;
  return r;
  #undef N
}

static char *basename (char *filename) {
  char *p = strrchr (filename, '/');
  return p ? (p + 1) : filename;
}

static int file_is_last (file_t *F) {
  return F->last;
}

static void reset_max_errors_limit (void) {
  stats.max_errors_limit = 0x7fffffffffffffffLL;
}

static int cmp_backups_by_size (const void *a, const void *b) {
  backup_file_t *x = *((backup_file_t **) a);
  backup_file_t *y = *((backup_file_t **) b);
  if (x->S.orig_file_size > y->S.orig_file_size) {
    return -1;
  }
  if (x->S.orig_file_size < y->S.orig_file_size) {
    return 1;
  }
  return 0;
}

char *backup_status_to_str (int status) {
  static char out[64];
  char *p = out;
  p[0] = p[1] = 0;
  if (status & bs_differ) {
    p += sprintf (p, "|differ");
  }
  if (status & bs_io_error) {
    p += sprintf (p, "|io_error");
  }
  if (status & bs_larger) {
    p += sprintf (p, "|larger");
  }
  if (status & bs_same_inode) {
    p += sprintf (p, "|same_inode");
  }
  assert (p < out + sizeof (out));
  return out+1;
}

static void file_backups_check (file_t *F) {
  if (!backups_dir) {
    return;
  }
  reset_max_errors_limit ();
  int i, n = 0;
  backup_file_t *p;
  for (p = F->backups; p != NULL; p = p->next) {
    n++;
  }
  backup_file_t **a;
  a = alloca ((n + 1) * sizeof (a[0]));
  n = 0;
  for (p = F->backups; p != NULL; p = p->next) {
    if (p->status == bs_ok) {
      a[n++] = p;
    }
  }
  const int correct_prefix = n;
  for (p = F->backups; p != NULL; p = p->next) {
    if (p->status != bs_ok) {
      vkprintf (1, "backup '%s', status (%s)\n", p->filename, backup_status_to_str (p->status));
      a[n++] = p;
    }
  }
  int correct_copies = 0;
  if (correct_prefix > 0) {
    qsort (a, correct_prefix, sizeof (a[0]), cmp_backups_by_size);
    int m = 0;
    for (m = 0; m < correct_prefix && a[m]->S.orig_file_size == F->S.orig_file_size; m++);
    correct_copies = m;
  }

  const int incorrect = n - correct_prefix;
  vkprintf (2, "%d correct copies, %d correct prefix and %d incorrect backups for '%s'.\n", correct_copies, correct_prefix - correct_copies, incorrect, F->S.F->info->filename);

  int c = file_is_last (F) ? correct_prefix : correct_copies;

  if (!c) {
    log_printf (backups_features.check_level, "No correct backups for binlog '%s'", F->S.F->info->filename);
  }

  if (c > 0 && backups_features.warn_bad_backups_which_could_be_deleted) {
    for (i = correct_prefix; i < n; i++) {
      log_warning ("incorrect backup '%s' for binlog '%s' could be deleted, since backup '%s' is correct", a[i]->S.F->info->filename, F->S.F->info->filename, a[0]->S.F->info->filename);
    }
  }

  if (c > 0 && backups_features.warn_redundant_good_backups) {
    for (i = 1; i < correct_copies; i++) {
      log_warning ("correct copy backup '%s' for binlog '%s' could be deleted, since backup '%s' is correct", a[i]->S.F->info->filename, F->S.F->info->filename, a[0]->S.F->info->filename);
    }
    for (; i < correct_prefix; i++) {
      log_warning ("correct prefix backup '%s' for binlog '%s' could be deleted, since backup '%s' is correct", a[i]->S.F->info->filename, F->S.F->info->filename, a[0]->S.F->info->filename);
    }
  }

  if (!file_is_last (F)) {
    if (backups_features.shorter_middle_log_level != ll_none) {
      for (i = 0; i < n; i++) {
        if (a[i]->S.orig_file_size < F->S.orig_file_size) {
          log_printf (backups_features.shorter_middle_log_level, "backup '%s' for binlog '%s' is shorter, backup size is %lld bytes, binlog size is %lld bytes.", a[i]->S.F->info->filename, F->S.F->info->filename, (long long) a[i]->S.orig_file_size, (long long) F->S.orig_file_size);
        }
      }
    }
  } else {
    int t = time (NULL);
    for (i = 0; i < n; i++) {
      int mt = t - a[i]->S.F->info->mtime;
      long long x = F->S.orig_file_size - a[i]->S.orig_file_size;
      int ok = -1;
      if (x < (256 << 10) && mt < 300) {
        ok = 1;
      } else if (x > 0 && mt >= 300) {
        ok = 0;
      } else {
        ok = (x < F->S.growth_speed * 600.0) ? 1 : 0;
      }
      if (ok < 1) {
        log_printf (backups_features.shorter_last_log_level, "backup '%s' for binlog '%s' is %lld bytes shorter and was modified %d seconds ago, backup size is %lld bytes, binlog size is %lld bytes.", a[i]->S.F->info->filename, F->S.F->info->filename, x, mt, (long long) a[i]->S.orig_file_size, (long long) F->S.orig_file_size);
      }
    }
  }
}

static int file_open (file_t *F) {
  backup_file_t *f;
  reset_max_errors_limit ();
  if (max_log_errors) {
    stats.max_errors_limit = stats.errors + max_log_errors;
    if (stats.max_errors_limit < 0) {
      reset_max_errors_limit ();
    }
  }

  find_backups ();
  /* open backup before openning binlog slice */
  char *p = basename (F->binlog->info->filename);
  F->backups = get_backup_file_f (p, strlen (p), 0);
  for (f = F->backups; f != NULL; f = f->next) {
    int l = strlen (f->filename) + strlen (backups_dir) + 1;
    char *a = zmalloc (l + 1);
    assert (snprintf (a, l + 1, "%s/%s", backups_dir, f->filename) == l);
    if (stream_open (&f->S, 0, a, 1) < 0) {
      f->status |= bs_io_error;
    }
  }

  if (stream_open (&F->S, F->binlog, 0, 0) < 0) {
    log_fatal ("openning binlog file '%s' in read-only mode failed.", F->binlog->info->filename);
    return -1;
  }

  if (F->S.orig_file_size % 4) {
    log_fatal ("size(%lld) of '%s' isn't multiple of 4.", (long long) F->S.orig_file_size, F->S.F->info->filename);
    return -1;
  }

  for (f = F->backups; f != NULL; f = f->next) {
    if (!f->status) {
      if (stream_same_inode (&F->S, &f->S)) {
        log_error ("backup '%s' and binlog '%s' have equal inodes.", f->S.F->info->filename, F->S.F->info->filename);
        f->status |= bs_same_inode;
      }
      if (f->S.orig_file_size > F->S.orig_file_size) {
        f->status |= bs_larger;
      }
      if (f->status == bs_ok && stream_is_zipped (&f->S) && stream_is_zipped (&F->S)) {
        if (f->S.orig_file_size != F->S.orig_file_size) {
          log_warning ("backup '%s' and binlog '%s' have different original file size (%lld and %lld).", f->S.F->info->filename, F->S.F->info->filename, f->S.orig_file_size, F->S.orig_file_size);
          f->status |= bs_differ;
        } else if (f->S.F->info->preloaded_bytes != F->S.F->info->preloaded_bytes) {
          log_warning ("backup '%s' and binlog '%s' have different header size (%d and %d).", f->S.F->info->filename, F->S.F->info->filename, f->S.F->info->preloaded_bytes, F->S.F->info->preloaded_bytes);
          f->status |= bs_differ;
        } else if (memcmp (f->S.F->info->start, F->S.F->info->start, F->S.F->info->preloaded_bytes)) {
          log_warning ("backup '%s' and binlog '%s' have different headers.", f->S.F->info->filename, F->S.F->info->filename);
          f->status |= bs_differ;
        }
      }
    }
  }

  for (f = F->backups; f != NULL; f = f->next) {
    if (f->status != bs_ok) {
      continue;
    }
    backup_file_t *g;
    for (g = f->next; g != NULL; g = g->next) {
      if (g->status == bs_ok && stream_same_inode (&f->S, &g->S)) {
        f->status |= bs_same_inode;
        g->status |= bs_same_inode;
        log_error ("backup '%s' and backup '%s' have equal inodes.", f->S.F->info->filename, g->S.F->info->filename);
      }
    }
  }

  return 0;
}

static void file_close (file_t *F) {
  close_binlog (F->binlog, 1);
  F->binlog = 0;
  backup_file_t *f;
  for (f = F->backups; f != NULL; f = f->next) {
    stream_close (&f->S);
  }
}

static int file_is_consistent_lev_rotate (file_t *U, file_t *V) {
  if (match_rotate_logevents (&U->rotate_to, &V->rotate_from) <= 0) {
    log_fatal ("'%s' and '%s' have unconsistent LEV_ROTATE_TO and LEV_ROTATE_FROM", U->S.F->info->filename, V->S.F->info->filename);
    return -1;
  }
  return 1;
}

static int stream_read_lev_rotate_to (stream_t *A, struct lev_rotate_to *RT) {
  if (A->orig_file_size < 36) {
    kprintf ("%s: file '%s' is too short.\n", __func__, A->F->info->filename);
    return -1;
  }
  const long long off = (A->orig_file_size - 36);
  if (stream_seek (A, off) < 0) {
    return -1;
  }
  int res = stream_read_limited (A, io_buff, 36);
  vkprintf (2, "read %d bytes from binlog '%s', offset: %lld\n", res, A->F->info->filename, off);
  if (res < 36) {
    kprintf ("%s: fail read %d tail bytes from file '%s', stream_read_limited returns %d.\n", __func__, 36, A->F->info->filename, res);
    return -1;
  }
  memcpy (RT, io_buff, 36);
  return 0;
}

static int file_binlog_read_lev_rotate_to (file_t *F) {
  if (stream_read_lev_rotate_to (&F->S, &F->rotate_to) < 0) {
    return -1;
  }

  if (F->rotate_to.type != LEV_ROTATE_TO) {
    return -1;
  }

  return 0;
}

static void file_relax_crc32 (file_t *F, void *p, int l) {
  assert (!quick_test);
  assert (!(l & 3));
  F->log_crc32 = ~crc32_partial (p, l, ~F->log_crc32);
  F->crc32_off += l;
}

/******************** log ********************/

static void log_vprintf (log_level_t level, const char *format, va_list ap) {
  switch (level) {
    case ll_none:
      return;
    case ll_warning:
      stats.warnings++;
    break;
    case ll_error:
      if (stats.errors == stats.max_errors_limit) {
        kprintf ("Too many errors, log truncated.\n");
      }
      stats.errors++;
      if (stats.errors > stats.max_errors_limit) {
        return;
      }
    break;
    case ll_fatal:
      stats.fatals++;
    break;
  }
  static const char *msg[4] = { "NONE: ", "WARN: ", "ERROR: ", "FATAL: "};
  static char log_buff[4096];
  int l = strlen (msg[level]);
  assert (l < sizeof (log_buff));
  memcpy (log_buff, msg[level], l);
  int o = sizeof (log_buff) - l;
  assert (o > 0);
  va_list aq;
  va_copy (aq, ap);
  l = vsnprintf (log_buff + l, o, format, aq);
  va_end (aq);
  assert (l < o);
  kprintf ("%s\n", log_buff);

  if (level == ll_fatal && !keep_going) {
    kprintf ("Stop checking binlogs after fatal error.\n");
    exit (1);
  }
}

static void log_printf (log_level_t level, const char *format, ...) {
  va_list ap;
  va_start (ap, format);
  log_vprintf (level, format, ap);
  va_end (ap);
}

static void log_warning (const char *format, ...)  {
  va_list ap;
  va_start (ap, format);
  log_vprintf (ll_warning, format, ap);
  va_end (ap);
}

static void log_error (const char *format, ...)  {
  va_list ap;
  va_start (ap, format);
  log_vprintf (ll_error, format, ap);
  va_end (ap);
}

static void log_fatal (const char *format, ...)  {
  va_list ap;
  va_start (ap, format);
  log_vprintf (ll_fatal, format, ap);
  va_end (ap);
}

typedef struct {
  long long last_correct_off;
  unsigned long long incorrect_logevents;
  long long last_incorrect_off[4];
  int found;
} crc32_stat_t;

static void file_crc32_correct_flush (file_t *F, crc32_stat_t *S) {
  if (quick_test) {
    return;
  }
  const long long u = S->last_correct_off + 20, v = F->crc32_off;
  if (v - u  >= 0x20000) {
    if (S->incorrect_logevents <= 1) {
      log_warning ("%lld incorrect and 0 correct LEV_CRC32 in the file '%s', file off [%lld, %lld), log pos [%lld, %lld)", S->incorrect_logevents, F->S.F->info->filename, u, v, F->start_log_pos + u, F->start_log_pos + v);
    }
    if (S->incorrect_logevents >= 3 && S->found) {
      log_error ("%lld incorrect and 0 correct LEV_CRC32 in the file '%s', file off [%lld, %lld), log pos [%lld, %lld)", S->incorrect_logevents, F->S.F->info->filename, u, v, F->start_log_pos + u, F->start_log_pos + v);
    }
  }
  S->found = 0;
  S->incorrect_logevents = 0;
  memset (S->last_incorrect_off, 0x7f, sizeof (S->last_incorrect_off));
}

static void file_crc32_correct_add (file_t *F, crc32_stat_t *S) {
  file_crc32_correct_flush (F, S);
  F->total_correct_crc32_logevents++;
  S->last_correct_off = F->crc32_off;
}

static inline int valid_timestamp (int timestamp) {
  static int end_timestamp = 0;
  if (!end_timestamp) {
    end_timestamp = time (NULL) + 86400;
    if (end_timestamp < 0) {
      end_timestamp = 0x7fffffff;
    }
  }
  //date -d "Jan 01 00:00:00 GMT 2006" +%s
  return 1136073600 <= timestamp && timestamp <= end_timestamp;
}

static void file_crc32_incorrect_add (file_t *F, crc32_stat_t *S) {
  F->total_incorrect_crc32_logevents++;
  S->incorrect_logevents++;
  int i = S->incorrect_logevents & 3;
  S->last_incorrect_off[i] = F->crc32_off;
  if (S->incorrect_logevents >= 3) {
    long long s = S->last_incorrect_off[i] - S->last_incorrect_off[(i+2)&3];
    assert (s >= 0);
    S->found = s <= 0x20000;
  }
}

static int backup_checked_read (backup_file_t *f, long long off, long long s, backup_status_t *status) {
  assert (f->S.F);
  if (stream_seek (&f->S, off) < 0) {
    *status = bs_io_error;
    return -1;
  }
  if (stream_read_limited (&f->S, backup_buff, s) < 0) {
    *status = bs_io_error;
    return -1;
  }

  vkprintf (2, "read %lld bytes from backup '%s', offset: %lld\n", (long long) s, f->S.F->info->filename, off);

  if (memcmp (io_buff + PREVSIZE, backup_buff, s)) {
    *status = bs_differ;
    return -1;
  }
  *status = bs_ok;
  return 0;
}

int file_binlog_read (struct kfs_replica *R, file_t *F, file_t *P) {
  unsigned char hash_buff[0x8000], *phb = hash_buff;
  crc32_stat_t cur_incorrect_crc32;
  memset (&cur_incorrect_crc32, 0, sizeof (cur_incorrect_crc32));

  memset (F, 0, sizeof (*F));
  F->R = R;
  if (P) {
    F->binlog = next_binlog (P->binlog);
    if (!F->binlog) {
      log_fatal ("next_binlog returns NULL after slice '%s'", P->binlog->info->filename);
      return -1;
    }
  } else {
    int fd = preload_file_info (R->binlogs[0]);
    if (fd == -2) {
      log_fatal ("preload_file_info for the first slice '%s' failed.", R->binlogs[0]->filename);
      return -1;
    }
    if (fd >= 0) {
      assert (!close (fd));
    }
    F->binlog = open_binlog (R, R->binlogs[0]->log_pos);
    if (!F->binlog) {
      log_fatal ("open_binlog from %lld log position failed", R->binlogs[0]->log_pos);
      return -1;
    }
    assert (F->binlog->info == R->binlogs[0]);
    if (!allow_first_rotate_from && R->binlogs[0]->log_pos) {
      log_fatal ("First available binlog slice '%s' starts from log position %lld. Give [-A] option if you want to check this binlog.", F->binlog->info->filename, R->binlogs[0]->log_pos);
      return -1;
    }
  }

  F->last = binlog_is_last (F->binlog);

  struct kfs_file_info *FI = F->binlog->info;
  F->stats_errors_before_reading = stats.errors;
  if (file_open (F) < 0) {
    return -1;
  }

  int k;
  for (k = 0; k < R->binlog_num; k++) {
    if (R->binlogs[k] == F->binlog->info) {
      break;
    }
  }
  assert (k < R->binlog_num);

  vkprintf (1, "(%d of %d) Open binlog file '%s' (size %lld)\n", k + 1, R->binlog_num, F->S.F->info->filename, (long long) F->S.orig_file_size);
  unsigned char *prev = io_buff, *buff = prev + PREVSIZE;
  int update_file_size = file_is_last (F) ? 1 : 0;
  backup_file_t *f;
  while (!stream_eof (&F->S)) {
    const long long remaining_bytes = F->S.orig_file_size - F->S.readto_off;
    size_t s = remaining_bytes < F->S.buffsize ? remaining_bytes : F->S.buffsize;
    if (remaining_bytes < F->S.buffsize && update_file_size) {
      for (f = F->backups; f != NULL; f = f->next) {
        if (f->status == bs_ok && stream_update (&f->S) < 0) {
          return -1;
        }
      }
      if (stream_update (&F->S) < 0) {
        return -1;
      }
      update_file_size = 0;
      continue;
    }
    long long old_readto_off = F->S.readto_off;
    if (stream_read_limited (&F->S, io_buff + PREVSIZE, s) < 0) {
      log_error ("%s: read %d bytes from offset %lld for the file '%s' failed.\n", __func__, (int) s, old_readto_off, F->S.F->info->filename);
      return -1;
    }
    vkprintf (2, "read %lld bytes from binlog '%s', offset: %lld\n", (long long) s, F->S.F->info->filename, old_readto_off);
    for (f = F->backups; f != NULL; f = f->next) {
      if (f->status != bs_ok || backups_features.check_type == bc_none) {
        continue;
      }
      int compare_backups = 0;
      const long long backup_remaining_bytes = f->S.orig_file_size - old_readto_off;
      if (backup_remaining_bytes <= 0) {
        continue;
      }
      switch (backups_features.check_type) {
        case bc_partial:
          compare_backups = !old_readto_off || backup_remaining_bytes < 2 * F->S.buffsize;
        break;
        case bc_full:
          compare_backups = 1;
        break;
        default: assert (0);
      }
      if (compare_backups) {
        const long long bs = backup_remaining_bytes < F->S.buffsize ? backup_remaining_bytes : F->S.buffsize;
        backup_status_t status;
        if (backup_checked_read (f, old_readto_off, bs, &status) < 0) {
          f->status |= status;
        }
      }
    }
    assert (F->S.readto_off <= F->S.orig_file_size);
    if (!old_readto_off) {
      if (!file_is_last (F)) {
        memcpy (phb, io_buff + PREVSIZE, 0x4000);
        phb += 0x4000;
      }
      int ss = 0;
      struct lev_generic *G = (struct lev_generic *) buff;
      switch (G->type) {
        case LEV_START:
          if (P) {
            log_fatal ("'%s' starts from unexpected LEV_START logevent.", FI->filename);
            return -1;
          }
          ss += 24 + ((G->b + 3) & -4);
          memcpy (&F->start, buff, 24);
          F->log_crc32 = F->start_crc32 = 0;
          F->start_log_pos = 0;
          break;
        case LEV_ROTATE_FROM:
          ss += 36;
          memcpy (&F->rotate_from, buff, 36);
          F->log_crc32 = F->start_crc32 = F->rotate_from.crc32;
          F->start_log_pos = F->rotate_from.cur_log_pos;
          break;
        default:
          log_fatal ("'%s' first logevent type(0x%08x) isn't LEV_START or LEV_ROTATE_FROM", FI->filename, G->type);
          return -1;
      }
      if (F->S.orig_file_size < ss) {
        log_fatal ("'%s' is too short (%lld bytes) for containing %s", F->S.F->info->filename, (long long) F->S.orig_file_size, G->type == LEV_START ? "LEV_START" : "LEV_ROTATE_FROM");
        return -1;
      }

      if (P) {
        assert (F->rotate_from.type == LEV_ROTATE_FROM);
        if (file_is_consistent_lev_rotate (P, F) < 0) {
          return -1;
        }
      }
      if (quick_test) {
        int tail_bytes = F->S.orig_file_size & (KFS_BINLOG_ZIP_CHUNK_SIZE - 1);
        while (tail_bytes < F->S.buffsize) {
          tail_bytes += KFS_BINLOG_ZIP_CHUNK_SIZE;
        }
        long long skip_off = F->S.orig_file_size - tail_bytes;
        if (F->S.readto_off < skip_off) {
          if (stream_seek (&F->S, skip_off) < 0) {
            log_fatal ("fail to seek to %lld offset for file '%s'", skip_off, F->S.F->info->filename);
            return -1;
          }
        }
        F->unprocessed_bytes = 0;
      } else {
        F->unprocessed_bytes = -ss;
        file_relax_crc32 (F, buff, ss);
      }
    }

    if (!file_is_last (F)) {
      long long a1 = F->S.orig_file_size - 0x4000, a2 = F->S.orig_file_size;
      if (a1 < old_readto_off) {
        a1 = old_readto_off;
      }
      if (a2 > old_readto_off + s) {
        a2 = old_readto_off + s;
      }
      if (a1 < a2) {
        assert (phb + (a2 - a1) <= hash_buff + 0x8000);
        memcpy (phb, io_buff + PREVSIZE + (a1 - old_readto_off), a2 - a1);
        phb += a2 - a1;
      }
    }

    unsigned int *end = (unsigned int *) (buff + s);
    if (stream_eof (&F->S)) {
      if (!file_is_last (F)) {
        memcpy (&F->rotate_to, buff + s - 36, 36);
        if (F->rotate_to.type != LEV_ROTATE_TO) {
          log_fatal ("'%s' isn't ended by LEV_ROTATE_TO", FI->filename);
          return -1;
        }
        end = (unsigned int *) (buff + s - 36);
      }
    }
    unsigned int *p = (unsigned int *) (buff - F->unprocessed_bytes);
    end -= 5; /* lev_crc32 (5 ints) */

    if (quick_test) {
      p = end + 5;
    }

    if (p <= end) {
      while (1) {
        unsigned int *q = p;
        while (p <= end && *p != LEV_CRC32) {
          p++;
        }
        int l = (((char *) p) - ((char *) q));
        if (l > 0) {
          file_relax_crc32 (F, q, l);
        }
        if (*p == LEV_CRC32) {
          struct lev_crc32 *C = (struct lev_crc32 *) p;
          if (valid_timestamp (C->timestamp)) {
            if (C->crc32 != F->log_crc32 || C->pos != F->crc32_off + F->start_log_pos) {
              file_crc32_incorrect_add (F, &cur_incorrect_crc32);
            } else {
              file_crc32_correct_add (F, &cur_incorrect_crc32);
            }
          } else {
            F->total_incorrect_timestamp++;
          }
          file_relax_crc32 (F, p, 4);
          p++;
        } else {
          break;
        }
      }
    }
    F->unprocessed_bytes = (buff + s) - ((unsigned char *) p);
    vkprintf (4, "'%s' F->unprocessed_bytes: %d\n", F->S.F->info->filename, F->unprocessed_bytes);
    assert (F->unprocessed_bytes >= 0 && F->unprocessed_bytes <= PREVSIZE);
    const int u = F->unprocessed_bytes > 36 ? F->unprocessed_bytes : 36;
    memcpy (buff - u, (buff + s) - u, u);
  }

  file_crc32_correct_flush (F, &cur_incorrect_crc32);

  if (F->rotate_to.type == LEV_ROTATE_TO && F->start_log_pos + F->S.readto_off != F->rotate_to.next_log_pos) {
    log_fatal ("'%s' LEV_ROTATE_TO next_los_pos (%lld) isn't equal sum of slice start pos and slice file size (%lld+%lld=%lld)", F->S.F->info->filename, F->rotate_to.next_log_pos, F->start_log_pos, F->S.readto_off, F->start_log_pos + F->S.readto_off);
    return -1;
  }

  vkprintf (3, "F->start_log_pos: %lld, F->rotate_to.type: 0x%08x, F->S.orig_file_size: %lld.\n", F->start_log_pos, F->rotate_to.type, F->S.orig_file_size);
  if (!file_is_last (F) && !F->start_log_pos && F->rotate_to.type == LEV_ROTATE_TO && F->S.orig_file_size >= 0x8000) {
    unsigned char tmp[16];
    long long file_hash;
    assert (phb == hash_buff + 0x8000);
    memset (phb - 16, 0, 16);
    md5 (hash_buff, 0x8000, tmp);
    memcpy (&file_hash, tmp, 8);
    vkprintf (3, "file_hash: 0x%016llx\n", file_hash);
    if (file_hash != F->rotate_to.cur_log_hash) {
      stream_check_hash (&F->S, file_hash, F->rotate_to.cur_log_hash);
      for (f = F->backups; f != NULL; f = f->next) {
        if (f->status == bs_ok) {
          stream_check_hash (&f->S, file_hash, F->rotate_to.cur_log_hash);
        }
      }
    }
  }

  if (quick_test) {
    return 0;
  }

  if (F->rotate_to.type == LEV_ROTATE_TO) {
    if (F->unprocessed_bytes > 36) {
      int l = F->unprocessed_bytes - 36;
      file_relax_crc32 (F, buff - F->unprocessed_bytes, l);
      F->unprocessed_bytes -= l;
    }

    if (F->rotate_to.crc32 != F->log_crc32) {
      log_fatal ("'%s' ROTATE_TO.crc32(0x%x) != computed log_crc32 (0x%x)", F->S.F->info->filename, F->rotate_to.crc32, F->log_crc32);
      return -1;
    }
  }
  vkprintf (4, "F->unprocessed_bytes: %d\n", F->unprocessed_bytes);

  if (F->unprocessed_bytes > 0) {
    file_relax_crc32 (F, buff - F->unprocessed_bytes, F->unprocessed_bytes);
    F->unprocessed_bytes = 0;
  }

  assert (F->crc32_off == F->S.readto_off);

  if (F->total_incorrect_crc32_logevents > F->total_correct_crc32_logevents) {
    log_level_t lvl = (!F->total_correct_crc32_logevents && !F->start_log_pos) ? ll_error : ll_fatal;
    log_printf (lvl, "'%s' contains %lld incorrect LEV_CRC32 and %lld correct LEV_CRC32", F->S.F->info->filename, F->total_incorrect_crc32_logevents, F->total_correct_crc32_logevents);
    if (lvl == ll_fatal) {
      return -1;
    }
  }

  if (F->total_incorrect_crc32_logevents > 0x7fffffff || F->S.orig_file_size + 1 < (F->total_incorrect_crc32_logevents << 32)) {
    log_warning ("'%s' is %lld bytes length and contains %lld incorrect LEV_CRC32", F->S.F->info->filename, (long long) F->S.orig_file_size, F->total_incorrect_crc32_logevents);
  }

  vkprintf (2, "'%s' contains %lld incorrect LEV_CRC32, %lld correct LEV_CRC32 and %lld LEV_CRC32 with invalid timestamp.\n", F->S.F->info->filename, F->total_incorrect_crc32_logevents, F->total_correct_crc32_logevents, F->total_incorrect_timestamp);

  return 0;
}

static int replica_updated (struct kfs_replica *R, int *last_update_time) {
  const char *replica_name = R->replica_prefix;
  const char *after_slash = strrchr (replica_name, '/');
  char dirname[PATH_MAX+1];
  int dirname_len;
  if (after_slash) {
    after_slash++;
    dirname_len = after_slash - replica_name;
    assert (dirname_len < PATH_MAX);
    memcpy (dirname, replica_name, dirname_len);
    dirname[dirname_len] = 0;
  } else {
    dirname_len = 2;
    dirname[0] = '.';
    dirname[1] = '/';
    dirname[2] = 0;
  }

  int mtime = get_dir_modtime (dirname);

  if (*last_update_time >= mtime) {
    return 0;
  }

  vkprintf (1, "%s: replica's directory was changed at %d, previous update replica time is %d\n", __func__, mtime, *last_update_time);
  update_replica (R, 0);
  *last_update_time = time (NULL);
  return 1;
}

int check_rotation_match (const char *src1, const char *src2) {
  stream_t A, B;
  if (stream_open (&A, 0, src1, 0) < 0) {
    kprintf ("stream open for file '%s' failed.\n", src1);
    return -1;
  }
  struct lev_rotate_to RT;
  if (stream_read_lev_rotate_to (&A, &RT) < 0) {
    return -1;
  }
  if (stream_open (&B, 0, src2, 0) < 0) {
    kprintf ("stream open for file '%s' failed.\n", src2);
    return -1;
  }
  if (B.orig_file_size < 36) {
    kprintf ("file '%s' is too short.\n", src2);
    return -1;
  }
  if (stream_read_limited (&B, io_buff, 36) < 36) {
    log_fatal ("fail read %d head bytes from file '%s'", 36, B.F->info->filename);
    return -1;
  }
  if (match_rotate_logevents (&RT, (struct lev_rotate_from *) io_buff) <= 0) {
    log_fatal ("rotation check for files '%s' and '%s' failed.\n", src1, src2);
    return -1;
  }
  return 0;
}

int check_snapshot_is_readable (char *filename) {
  reset_max_errors_limit ();
  stream_t S;
  if (stream_open (&S, 0, filename, 2) < 0) {
    log_error ("fail to open snapshot '%s'", filename);
    return -1;
  }
  while (!stream_eof (&S)) {
    const long long off = S.readto_off;
    const long long remaining_bytes = S.orig_file_size - off;
    size_t s = remaining_bytes < S.buffsize ? remaining_bytes : S.buffsize;
    if (stream_read_limited (&S, io_buff + PREVSIZE, s) < 0) {
      log_error ("reading shapshot '%s' from offset %lld fail.", filename, off);
      stream_close (&S);
      return -1;
    }
    vkprintf (2, "read %lld bytes from snapshot '%s', offset: %lld\n", (long long) s, filename, off);
  }
  stream_close (&S);
  return 0;
}

static long long parse_memory_limit (int option, const char *limit) {
  long long x;
  char c = 0;
  if (sscanf (optarg, "%lld%c", &x, &c) < 1) {
    kprintf ("Parsing limit for option '%c' fail: %s\n", option, limit);
    exit (1);
  }
  switch (c | 0x20) {
    case ' ': break;
    case 'k':  x <<= 10; break;
    case 'm':  x <<= 20; break;
    case 'g':  x <<= 30; break;
    case 't':  x <<= 40; break;
    default: kprintf ("Parsing limit fail. Unknown suffix '%c'.\n", c); exit (1);
  }
  return x;
}

void usage (void) {
  printf ("%s\n", FullVersionStr);
  printf (
    "check-binlog [-u<username>] [-c<max-conn>] [-v] [-b<backups_dir>] [-a<binlog-name>] <binlog>\n"
    "\tChecking binlog tool.\n"
    "\t[-v]\t\toutput statistical and debug information into stderr\n"
    "\t[-s<N>]\tlimit disk reading speed to N bytes in second, e.g. 32m, N > 1g means no limit\n"
    "\t[-q]\tquick test (only slice sizes and rotation consistency check)\n"
    "\t[-H<heap-size>]\tdefines maximum heap size (default: %lld bytes).\n"
    "\t[-I<buffsize>]\tset IO buffer size (default buffsize is 16Mb)\n"
    "\t\tfor bin.bz files buffsize is equal to KFS_BINLOG_ZIP_CHUNK_SIZE (%d bytes)\n"
    "\t[-i]\tdisable check that snapshot is readable in the case removed first binlog file, \n"
    "\t\tNOTICE: snapshot heuristics doesn't work in the case of news-engine, ...\n"
    "\t[-k]\t\tkeep going on fatal errors\n"
    "\t[-E<N>]\tshow only first <N> errors for each binlog\n"
    "\t[-S]\tbackups have same names as original binlogs (no suffix with dot and 10 digits), check storage-engine volumes\n"
    "\t[-A]\tallow first available slice starts from LEV_ROTATE_FROM (Example: adstat).\n"
    "\t[-b<backups_dir>]\tset backups dir\n"
    "\t[-B<backups_suboptions>]\tset backups suboptions\n"
    "Backup suboptions:\n"
    "\t'p'\tpartial check, warning in the case no correct backup\n"
    "\t'P'\tpartial check, error in the case no correct backup\n"
    "\t'f'\tfull check, warning in the case no correct backup\n"
    "\t'F'\tfull check, error in the case no correct backup\n"
    "\t'd'\twarn if there is incorrect backup and at least one correct backup\n"
    "\t'r'\twarn if there is multiple correct backups\n"
    "\t'm'\twarn shorter backup for not last binlog file\n"
    "\t'M'\terror shorter backup for not last binlog file\n"
    "\t'l'\twarn shorter backup for last binlog file\n"
    "\t'L'\terror shorter backup for last binlog file\n"
    "Specifying backups suboptions as 'all' is equivalent to '%s'.\n",
    heap_memory,
    KFS_BINLOG_ZIP_CHUNK_SIZE,
    BACKUP_ALL_SUBOPTIONS
  );
  printf ("\n$./check-binlog -m <src1.bin> <src2.bin>\n"
    "\tOnly rotate logevents consistency check.\n");
  exit (2);
}

static double get_speed (long long bytes, double t) {
  return bytes / (1048576.0 * t);
}

int main (int argc, char *argv[]) {
  int i;
  static int only_rotation_match_check = 0;
  long long x;
  if (getuid ()) {
    maxconn = 100;
  }
  set_debug_handlers ();
  while ((i = getopt (argc, argv, "AB:E:H:I:Sa:b:c:hikmqs:u:v")) != -1) {
    switch (i) {
    case 'A':
      allow_first_rotate_from = 1;
    break;
    case 'B':
      backups_suboptions = optarg;
    break;
    case 'E':
      max_log_errors = atoll (optarg);
      if (max_log_errors < 0) {
        max_log_errors = 0;
      }
    break;
    case 'H':
      x = parse_memory_limit (i, optarg);
      if (x >= (32 << 20) && x <= dynamic_data_buffer_size) {
        heap_memory = x;
      }
      break;
    case 'I':
      x = parse_memory_limit (i, optarg);
      if (x >= (1 << 20) && x <= BUFFSIZE) {
        buffsize = x;
      } else {
        fprintf (stderr, "Invalid option '-%c %s': buffer size isn't in range from 1Mi to 16Mi\n", (char) i, optarg);
        exit (2);
      }
    break;
    case 'S':
      no_unixtime_suffix_in_backups = 1;
    break;
    case 'a':
      binlogname = optarg;
    break;
    case 'b':
      backups_dir = optarg;
    break;
    case 'c':
      maxconn = atoi (optarg);
      if (maxconn <= 0 || maxconn > MAX_CONNECTIONS) {
      	maxconn = MAX_CONNECTIONS;
      }
    break;
    case 'h':
      usage ();
    break;
    case 'i':
      check_snapshot = 0;
    break;
    case 'k':
      keep_going = 1;
    break;
    case 'm':
      only_rotation_match_check = 1;
    break;
    case 'q':
      quick_test = 1;
    break;
    case 's':
      disk_reading_limit = parse_memory_limit (i, optarg);
      if (disk_reading_limit >= (1 << 30)) {
        disk_reading_limit = -1;
      }
      if (disk_reading_limit < (1 << 20)) {
        kprintf ("Illegal option '-%c %s', limit is too low.\n", i, optarg);
        exit (2);
      }
      inv_disk_reading_limit = 1.0 / (double) disk_reading_limit;
    break;
    case 'u':
      username = optarg;
    break;
    case 'v':
      verbosity++;
    break;
    default:
      fprintf (stderr, "Unimplemented option %c\n", i);
      exit (2);
    break;
    }
  }

  if (optind + 1 + only_rotation_match_check != argc) {
    usage ();
  }

  if (raise_file_rlimit (maxconn + 16) < 0) {
    kprintf ("fatal: cannot raise open file limit to %d\n", maxconn + 16);
    exit (1);
  }

  if (change_user (username) < 0) {
    kprintf ("fatal: cannot change user to %s\n", username ? username : "(none)");
    exit (1);
  }

  dynamic_data_buffer_size = heap_memory;
  init_dyn_data ();

  if (only_rotation_match_check) {
    assert (optind + 2 == argc);
    if (check_rotation_match (argv[optind], argv[optind+1]) < 0) {
      return 1;
    }
    return 0;
  }

  if (quick_test) {
    keep_going = 0;
    if (check_snapshot) {
      vkprintf (1, "Disable reading all snapshot check.\n");
      check_snapshot = 0;
    }
    if (buffsize != BUFFSIZE) {
      vkprintf (1, "Set default IO buffsize %d bytes.\n", BUFFSIZE);
      buffsize = BUFFSIZE;
    }
  }

  if (backups_dir) {
    if (backup_parse_suboptions () < 0) {
      exit (1);
    }
  }

  vkprintf (2, "%s\n", FullVersionStr);

  if (engine_preload_filelist (argv[optind], binlogname) < 0) {
    kprintf ("cannot open binlog files for %s\n", binlogname ? binlogname : argv[optind]);
    exit (1);
  }
  int last_update_time = time (NULL);

  struct kfs_replica *R = engine_replica;

  if (R == NULL || R->binlog_num == 0) {
    kprintf ("No binlogs found.\n");
    exit (1);
  }

  file_t *F = alloca (sizeof (*F)), *P = NULL;
  int tot_corrupted = 0, tot_files = 0;

  vkprintf (2, "engine_replica->binlog_num: %d\n", R->binlog_num);
  do {
    tot_files++;
    int r = file_binlog_read (R, F, P);
    long long x = stats.errors - F->stats_errors_before_reading;
    assert (x >= 0);
    if (x > 0) {
      kprintf ("Found %lld error%s in binlog '%s'.\n", x, x > 1 ? "s" : "", F->S.F->info->filename);
    }
    if (r < 0) {
      F->corrupted = 1;
      log_fatal ("file_binlog_read for file '%s' returned error code %d.\n", F->S.F->info->filename, r);
      if (!stream_eof (&F->S) && !file_is_last (F) && F->rotate_to.type != LEV_ROTATE_TO) {
        file_binlog_read_lev_rotate_to (F);
      }
    }

    if (F->rotate_from.type == LEV_ROTATE_FROM && F->rotate_to.type == LEV_ROTATE_TO &&
        F->rotate_from.cur_log_hash != F->rotate_to.cur_log_hash) {
      log_fatal ("'%s' cur_log_hash is different in LEV_ROTATE_TO and LEV_ROTATE_FROM", F->S.F->info->filename);
      F->corrupted = 1;
    }

    file_backups_check (F);

    if (F->corrupted) {
      tot_corrupted++;
    }

    if (!P && F->start.type != LEV_START) {
      if (F->rotate_from.type != LEV_ROTATE_FROM) {
        log_fatal ("'%s' didn't start from LEV_ROTATE", F->S.F->info->filename);
      }
      int j;
      int ok = 0;
      for (j = R->snapshot_num - 1; j >= 0; j--) {
        if (R->snapshots[j]->min_log_pos >= F[0].rotate_from.cur_log_pos) {
          if (check_snapshot) {
            if (!check_snapshot_is_readable (R->snapshots[j]->filename)) {
              ok++;
              break;
            }
          } else {
            ok++;
          }
        }
      }
      if (!ok) {
        log_fatal ("Good snapshot isn't found, first binlog '%s' starts from %lld log_pos", F->S.F->info->filename, F->start_log_pos);
        exit (1);
      }
    }

    if (!P) {
      P = alloca (sizeof (*P));
    } else {
      file_close (P);
    }
    file_t *T = P; P = F; F = T;
    if (file_is_last (P)) {
      break;
    }
    replica_updated (R, &last_update_time);
  } while (1);

  if (P && P->binlog && P->binlog->info && (P->binlog->info->flags & 16)) {
    log_fatal ("Last binlog '%s' is zipped.", P->binlog->info->filename);
  }

  if (tot_files >= 2) {
    file_close (F);
  }
  assert (P);
  file_close (P);

  for (i = 0; i < 3; i++) {
    if (stats.total_disk_reading_time[i] > 1e-6) {
      vkprintf (2, "Average %s disk reading speed is %.3lfMi/sec, limited reading speed is %.3lfMi/sec.\n",
        type_to_str (i),
        get_speed (stats.total_bytes_read[i], stats.total_disk_reading_time[i]),
        get_speed (stats.total_bytes_read[i], stats.total_sleeping_time[i] + stats.total_disk_reading_time[i]));
    }
  }

  if (stats.fatals) {
    kprintf ("%lld fatal errors\n", stats.fatals);
  }

  if (stats.errors) {
    kprintf ("%lld errors\n", stats.errors);
  }

  if (stats.warnings) {
    kprintf ("%lld warnings\n", stats.warnings);
  }

  if (stats.fatals) {
    return 1;
  }

  if (tot_corrupted) {
    return 1;
  }

  if (stats.errors >= EXIT_ERROR_LIMIT) {
    kprintf ("Exit code is 1, since number of errors exceeds %d.\n", EXIT_ERROR_LIMIT - 1);
    return 1;
  }

  return 0;
}
