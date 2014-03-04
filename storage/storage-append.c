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
#define _XOPEN_SOURCE 500

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <fcntl.h>
#include <errno.h>

#include "md5.h"
#include "crc32.h"
#include "base64.h"
#include "server-functions.h"
#include "kdb-storage-binlog.h"
#include "storage-content.h"

#define	VERSION_STR	"storage-append-0.06"
const char FullVersionStr[] = VERSION_STR " compiled at " __DATE__ " " __TIME__ " by gcc " __VERSION__ " "
#ifdef __LP64__
  "64-bit"
#else
  "32-bit"
#endif
;

static unsigned MAX_FILESIZE = 1U << 30;
static int exit_on_file_body_error = 1;
static int memory_repairing = 0, test_mode = 0, http_port = 80;

static int md5_check (struct lev_storage_file *E, unsigned char *a) {
  unsigned char m[16];
  md5 (a, E->size, m);
  return !memcmp (m, E->md5, 16);
}

/*
 *
 * GENERIC BUFFERED READ/WRITE
 *
 */

#define BUFFSIZE 0x1000000

char Buff[BUFFSIZE], *rptr = Buff, *wptr = Buff;
int wfd;

int flushout (void) {
  int w, s;
  while (rptr < wptr) {
    s = wptr - rptr;
    w = write (wfd, rptr, s);
    if (w != s) {
      kprintf ("write (%d, %p, %d) fail, written %d bytes, %m\n", wfd, rptr, s, w);
      if (w > 0 && w < s) {
        rptr += w;
        continue;
      }
      return -1;
    } else {
      break;
    }
  }
  rptr = wptr = Buff;
  return 0;
}

void clearin (void) {
  rptr = wptr = Buff + BUFFSIZE;
}

int writeout (const void *D, size_t len) {
  const char *d = D;
  while (len > 0) {
    int r = Buff + BUFFSIZE - wptr;
    if (r > len) {
      r = len;
    }
    memcpy (wptr, d, r);
    d += r;
    wptr += r;
    len -= r;
    if (len > 0) {
      if (flushout () < 0) {
        return -1;
      }
    }
  }
  return 0;
}

unsigned crc32_complement;

typedef struct {
  int fd;
  long long size;
  char *filename;
} file_t;

void file_open (file_t *F, char *filename, int mode, int exit_on_error) {
  F->filename = filename;
  F->fd = open (filename, mode);
  if (F->fd < 0) {
    kprintf ("%s: Couldn't open %s. %m\n", __func__, F->filename);
    if (!exit_on_error && errno == ENOENT) {
      return;
    }
    exit (1);
  }
  struct stat buf;
  if (fstat (F->fd, &buf) < 0) {
    close (F->fd);
    F->fd = -1;
    kprintf ("fstat fail (%s), %m\n", filename);
    exit (1);
  }
  F->size = buf.st_size;
}

#define STORAGE_LEV_START_SIZE 36

int vk_pread (file_t *F, void *a, int sz, long long off) {
  if (sz != pread (F->fd, a, sz, off)) {
    kprintf ("pread (%s, %d, %lld) failed. %m\n", F->filename, sz, off);
    return -1;
  }
  return 0;
}

static int local_id = -1;
typedef struct {
  int files;
  int file_crc32;
  int file_md5;
  int file_local_id;
  int padded_zero;
  int content_type;
  int bad_file_bodies;
} recover_stat_t;

int head_check (file_t *A, file_t *B) {
  if (B->size < STORAGE_LEV_START_SIZE) {
    return -10;
  }
  unsigned char a[STORAGE_LEV_START_SIZE], b[STORAGE_LEV_START_SIZE];
  if (vk_pread (A, a, STORAGE_LEV_START_SIZE, 0) < 0) {
    return -7;
  }
  if (vk_pread (B, b, STORAGE_LEV_START_SIZE, 0) < 0) {
    return -8;
  }
  if (memcmp (a, b, STORAGE_LEV_START_SIZE)) {
    kprintf ("%s isn't prefix of %s\n", B->filename, A->filename);
    return -9;
  }
  vkprintf (3, "First 36 bytes are equal (%s, %s).\n", A->filename, B->filename);
  crc32_complement = crc32_partial (a, STORAGE_LEV_START_SIZE, -1);
  return 0;
}

int prefix_check (file_t *A, file_t *B) {
  if (A->size < B->size) {
    kprintf ("%s is smaller than %s\n", A->filename, B->filename);
    return -2;
  }
  if (A->size < 20 || B->size < 20) {
    return -1;
  }

  int r = head_check (A, B);
  if (r < 0) {
    return r;
  }

  if (B->size == STORAGE_LEV_START_SIZE) {
    local_id = 0;
    return 0;
  }

  struct lev_crc32 C1, C2;
  int sz = sizeof (C1);
  long long off = B->size - sz;
  if (vk_pread (A, &C1, sz, off) < 0) {
    return -3;
  }
  if (vk_pread (B, &C2, sz, off) < 0) {
    return -4;
  }
  crc32_complement = ~C1.crc32;
  if (C1.type != LEV_CRC32) {
    kprintf ("didn't find LEV_CRC32 record in %s at offset %lld\n", A->filename, off);
    return -5;
  }

  if (memcmp (&C1, &C2, sz)) {
    kprintf ("last lev_crc32 record don't matched.\n");
    return -6;
  }
  vkprintf (3, "Last %d bytes are equal (%s, %s).\n", sz, A->filename, B->filename);
  crc32_complement = crc32_partial (&C1, sz, crc32_complement);
  return 0;
}

void storage_binlog_append (char *input_filename, char *output_filename) {
  vkprintf (3, "storage_binlog_append (%s, %s)\n", input_filename, output_filename);
  file_t A, B;
  file_open (&A, input_filename, O_RDONLY, 1);
  file_open (&B, output_filename, O_RDWR, 0);
  if (B.fd < 0) {
    unsigned char a[STORAGE_LEV_START_SIZE];
    if (A.size < STORAGE_LEV_START_SIZE) {
      kprintf ("%s too short (couldn't contain LEV_START logevent).\n", A.filename);
      exit (1);
    }

    if (vk_pread (&A, a, STORAGE_LEV_START_SIZE, 0) < 0)  {
      exit (1);
    }

    int fd = open (output_filename, O_WRONLY | O_CREAT, 0660);
    if (fd < 0) {
      kprintf ("Couldn't create %s\n", output_filename);
    }

    if (lock_whole_file (fd, F_WRLCK) <= 0) {
      kprintf ("lock_whole_file (%s, F_WRLCK) fail.\n", output_filename);
      exit (1);
    }

    if (STORAGE_LEV_START_SIZE != write (fd, a, STORAGE_LEV_START_SIZE)) {
      kprintf ("writing LEV_START to %s fail. %m\n", output_filename);
      exit (1);
    }
    close (fd);
    file_open (&B, output_filename, O_RDWR, 1);
  }
  if (lock_whole_file (B.fd, F_WRLCK) <= 0) {
    kprintf ("lock_whole_file (%s, F_WRLCK) fail.\n", B.filename);
    close (A.fd);
    close (B.fd);
    exit (1);
  }
  if (prefix_check (&A, &B) < 0) {
    close (A.fd);
    close (B.fd);
    exit (1);
  }
  long long cur_log_pos = B.size;
  int max_file_buf_size = -1;
  unsigned char *a = NULL;
  wfd = B.fd;
  if (B.size != lseek (B.fd, B.size, SEEK_SET)) {
    kprintf ("[%s:%lld] lseek failed. %m\n", B.filename, B.size);
    exit (1);
  }
  recover_stat_t recover_stat;
  memset (&recover_stat, 0, sizeof (recover_stat_t));
  int records = 0;
  while (cur_log_pos < A.size) {
    recover_stat_t w;
    memcpy (&w, &recover_stat, sizeof (recover_stat_t));
    const long long off = cur_log_pos;
    const unsigned old_crc32_complement = crc32_complement;
    struct lev_storage_file E;
    int sz = sizeof (E);
    if (vk_pread (&A, &E, sz, off) < 0) {
      break;
    }
    if (E.type != LEV_STORAGE_FILE && E.type != LEV_STORAGE_HIDE_FILE) {
      kprintf ("[%s:%lld] Expected LEV_STORAGE_FILE|LEV_STORAGE_HIDE_FILE, but %x found.\n", A.filename, off, E.type);
      break;
    }

    if (E.size > MAX_FILESIZE) {
      kprintf ("[%s:%lld] E.size = %u.\n", A.filename, off, E.size);
      break;
    }
    const int l = (E.size + 3) & -4, L = l + sizeof (struct lev_crc32);
    if (cur_log_pos + sz + L > A.size) {
      kprintf ("[%s:%lld] Illegal E.size = %u, input binlog too small.\n", A.filename, off, E.size);;
      break;
    }

    if (L > max_file_buf_size) {
      if (a != NULL) {
        free (a);
      }
      a = malloc (L);
      if (a == NULL) {
        kprintf ("Not enough memory for allocate file body, malloc (%d) failed.\n", L);
        break;
      }
      max_file_buf_size = L;
    }
    crc32_complement = crc32_partial (&E, sz, crc32_complement);
    if (vk_pread (&A, a, L, off + sz) < 0) {
      break;
    }
    //const unsigned computed_crc32 = crc32 (a, E.size);
    int cur_file_body_corrupted = 0;
    unsigned old_E_crc32 = E.crc32;
    int r = crc32_check_and_repair (a, E.size, &E.crc32, 0);
    if (r == 1) {
      if (md5_check (&E, a)) {
        w.files++;
      } else {
        vkprintf (3, "[%s:%lld] crc32_check_and_repair returns %d.\n", A.filename, off, r);
        cur_file_body_corrupted = 1;
      }
    } else if (r == 2 || r < 0) {
      if (md5_check (&E, a)) {
        w.file_crc32++;
      } else {
        vkprintf (3, "[%s:%lld] crc32_check_and_repair returns %d.\n", A.filename, off, r);
        cur_file_body_corrupted = 1;
      }
    }

    if (cur_file_body_corrupted) {
      kprintf ("[%s:%lld] E.crc32 = %x, but computed crc32 = %x\n", A.filename, off, old_E_crc32, E.crc32);
      if (exit_on_file_body_error) {
        break;
      } else {
        cur_file_body_corrupted = 1;
        w.bad_file_bodies++;
      }
    }
/*
    if (computed_crc32 != E.crc32) {
      int r = recover_file (&E, a, computed_crc32);
      if (r < 0) {
        vkprintf (3, "[%s:%lld] recover_file returns error code %d.\n", A.filename, off, r);
        if (md5_check (&E, a)) {
          E.crc32 = computed_crc32;
          w.file_crc32++;
        } else {
          kprintf ("[%s:%lld] E.crc32 = %x, but computed crc32 = %x\n", A.filename, off, E.crc32, computed_crc32);
          if (exit_on_file_body_error) {
            break;
          } else {
            cur_file_body_corrupted = 1;
            w.bad_file_bodies++;
          }
        }
      } else {
        w.files++;
      }
    }
*/
    crc32_complement = ~compute_crc32_combine (~crc32_complement, E.crc32, E.size);
    const unsigned zero = 0;
    int padded_zero_bytes = 0;
    if (l != E.size) {
      padded_zero_bytes = l - E.size;
      crc32_complement = crc32_partial (a + E.size, padded_zero_bytes, crc32_complement);
    }
    cur_log_pos += sz + l;

    struct lev_crc32 *C = (struct lev_crc32 *) (a + l);
    sz = sizeof (struct lev_crc32);
    if (C->type != LEV_CRC32) {
      kprintf ("[%s:%lld] Expected LEV_CRC32, but %x found.\n", A.filename, cur_log_pos, C->type);
      break;
    }
    if (C->pos != cur_log_pos) {
      kprintf ("[%s:%lld] C->pos (%lld) != cur_log_pos (%lld).\n", A.filename, cur_log_pos, C->pos, cur_log_pos);
      break;
    }
    int header_fix_attempts = 0, ct;
    unsigned char m[16];
    while (C->crc32 != ~crc32_complement && header_fix_attempts < 4) {
      int fix = 0;
      switch (header_fix_attempts) {
        case 0:
          if (padded_zero_bytes && memcmp (a + E.size, &zero, padded_zero_bytes)) {
            memcpy (a + E.size, &zero, padded_zero_bytes);
            fix = 1;
            w.padded_zero++;
          }
          break;
        case 1:
          if (local_id >= 0 && E.local_id != local_id + 1) {
            E.local_id = local_id + 1;
            fix = 1;
            w.file_local_id++;
          }
          break;
        case 2:
          if (!cur_file_body_corrupted) {
            ct = detect_content_type (a, l);
            if (E.content_type != ct) {
              E.content_type = ct;
              fix = 1;
              w.content_type++;
            }
          }
          break;
        case 3:
          if (!cur_file_body_corrupted) {
            md5 (a, E.size, m);
            if (memcmp (m, E.md5, 16)) {
              vkprintf (1, "Try fix file header md5, offset: %lld\n", off);
              memcpy (E.md5, m, 16);
              fix = 1;
              w.file_md5++;
            }
          }
          break;
      }
      if (fix) {
        crc32_complement = crc32_partial (&E, sizeof (struct lev_storage_file), old_crc32_complement);
        crc32_complement = ~compute_crc32_combine (~crc32_complement, E.crc32, E.size);
        crc32_complement = crc32_partial (a + E.size, padded_zero_bytes, crc32_complement);
      }
      header_fix_attempts++;
    }

    if (C->crc32 != ~crc32_complement) {
      kprintf ("[%s:%lld] C.crc32 (%x) != ~crc32_complement (%x), E.type = %x\n", A.filename, cur_log_pos, C->crc32, ~crc32_complement, E.type);
      break;
    }
/*
    if (E.content_type < 0 || E.content_type >= ct_last) {
      kprintf ("[%s:%lld] E.content_type (%d)\n", A.filename, off, E.content_type);
      break;
    }
*/

    crc32_complement = crc32_partial (C, sz, crc32_complement);
    clearin ();
    if (writeout (&E, sizeof (struct lev_storage_file)) < 0 ||
        writeout (a, L) < 0 ||
        flushout () < 0) {
      ftruncate (B.fd, off);
      break;
    }
    local_id = E.local_id;
    memcpy (&recover_stat, &w, sizeof (recover_stat_t));
    cur_log_pos += sizeof (struct lev_crc32);
    records++;
  }
  if (a != NULL) {
    free (a);
  }
  close (A.fd);
  assert (!fsync (B.fd));
  close (B.fd);
  vkprintf (1, "%d records were added.\n", records);
  vkprintf (1, "%d file(s) was succesfully recovered using crc32.\n", recover_stat.files);
  vkprintf (1, "%d crc32 file header field(s) was succesfully recovered using md5.\n", recover_stat.file_crc32);
  vkprintf (1, "%d md5 file header field(s) was succesfully recovered using LEV_CRC32.\n", recover_stat.file_md5);
  vkprintf (1, "%d local_id file header field(s) was succesfully recovered using LEV_CRC32.\n", recover_stat.file_local_id);
  vkprintf (1, "%d file's body zero padding was succesfully recovered using LEV_CRC32.\n", recover_stat.padded_zero);
  vkprintf (1, "%d content_type header field(s) was succesfully recovered using LEV_CRC32.\n", recover_stat.content_type);
  if (recover_stat.bad_file_bodies) {
    kprintf ("Copy %d files with wrong body.\n", recover_stat.bad_file_bodies);
  }
  vkprintf (2, "Max appended file size = %d.\n", max_file_buf_size);

  if (cur_log_pos != A.size) {
    kprintf ("Original file size = %lld, recovered file size = %lld.\n", A.size, cur_log_pos);
    exit (1);
  }
}

unsigned char one_pix_jpg[160] = {
255, 216, 255, 224, 0, 16, 74, 70, 73, 70, 0, 1, 1, 1, 0, 72,
0, 72, 0, 0, 255, 219, 0, 67, 0, 255, 255, 255, 255, 255, 255, 255,
255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 192, 0, 11, 8, 0, 1,
0, 1, 1, 1, 17, 0, 255, 196, 0, 20, 0, 1, 0, 0, 0, 0,
0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 3, 255, 196, 0, 20,
16, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
0, 0, 255, 218, 0, 8, 1, 1, 0, 0, 63, 0, 71, 255, 217,0
};

/*
static unsigned char one_pix_transparent_png[84] = {
  137, 80, 78, 71, 13, 10, 26, 10, 0, 0, 0, 13, 73, 72, 68, 82,
  0, 0, 0, 1, 0, 0, 0, 1, 8, 4, 0, 0, 0, 181, 28, 12,
  2, 0, 0, 0, 2, 98, 75, 71, 68, 0, 0, 170, 141, 35, 50, 0,
  0, 0, 11, 73, 68, 65, 84, 8, 215, 99, 96, 96, 0, 0, 0, 3,
  0, 1, 32, 213, 148, 199, 0, 0, 0, 0, 73, 69, 78, 68, 174, 66,
  96, 130, 0, 0
};
*/

typedef struct {
  long long log_pos;
  unsigned crc32_complement;
} crc32_logpos_t;

long long volume_id;
int last_mtime = 0;

int write_image (struct lev_storage_file *E, char *data, crc32_logpos_t *P) {
  clearin ();
  assert (E->type == LEV_STORAGE_FILE || (E->type == LEV_STORAGE_HIDE_FILE && !E->size));
  const unsigned zero = 0;
  int l = (E->size + 3) & -4;
  int padded_zero_bytes = l - E->size;
  assert (padded_zero_bytes >= 0 && padded_zero_bytes < 4);
  if (padded_zero_bytes) {
    assert (!memcmp (data + E->size, &zero, padded_zero_bytes));
  }
  P->crc32_complement = crc32_partial (E, sizeof (*E), P->crc32_complement);
  P->log_pos += sizeof (*E);
  P->crc32_complement = crc32_partial (data, l, P->crc32_complement);
  P->log_pos += l;
  struct lev_crc32 C;
  C.type = LEV_CRC32;
  C.pos = P->log_pos;
  C.crc32 = ~P->crc32_complement;
  C.timestamp = last_mtime;
  P->crc32_complement = crc32_partial (&C, sizeof (C), P->crc32_complement);
  P->log_pos += sizeof (C);
  if (writeout (E, sizeof (*E)) < 0 ||
      writeout (data, l) < 0 ||
      writeout (&C, sizeof (C)) < 0 ||
      flushout () < 0) {
    return -1;
  }

  if (test_mode) {
    char base64url_secret[12];
    int r = base64url_encode ((unsigned char *) &E->secret, 8, base64url_secret, 12);
    assert (!r);
    printf ("wget -O %d.jpg http://127.0.0.1:%d/v%lld/%x/%s.jpg\n", E->local_id, http_port, volume_id, E->local_id, base64url_secret);
  }
  return 0;
}

int storage_memory_repair (char *input_filename, char *output_filename) {
  long long bad_images = 0, good_images = 0, hide_logevents = 0;
  vkprintf (3, "%s (%s, %s)\n", __func__, input_filename, output_filename);
  last_mtime = 0;
  file_t A;
  file_open (&A, input_filename, O_RDONLY, 1);

  wfd = open (output_filename, O_WRONLY | O_CREAT, 0660);
  if (wfd < 0) {
    kprintf ("Couldn't create %s. %m\n", output_filename);
    exit (1);
  }

  if (lock_whole_file (wfd, F_WRLCK) <= 0) {
    kprintf ("lock_whole_file (%s, F_WRLCK) fail.\n", output_filename);
    exit (1);
  }

  if (A.size % 4) {
    kprintf ("%s: file '%s' size (%lld) isn't multiple of 4.\n", __func__, input_filename, A.size);
    return -1;
  }
  char *start = malloc (A.size), *a = start;
  if (a == NULL) {
    kprintf ("%s: Not enough memory for load input file ('%s'), file size is %lld bytes.\n",
      __func__, input_filename, A.size);
    return -1;
  }
  if (read (A.fd, a, A.size) != A.size) {
    kprintf ("%s: fail to read whole file '%s'. %m\n", __func__, input_filename);
    return -1;
  }

  crc32_logpos_t P;
  P.log_pos = STORAGE_LEV_START_SIZE;
  P.crc32_complement = crc32_partial (a, STORAGE_LEV_START_SIZE, -1);
  assert (*((unsigned *) a) == LEV_START);
  memcpy (&volume_id, a + 24, 8);

  if (STORAGE_LEV_START_SIZE != write (wfd, a, STORAGE_LEV_START_SIZE)) {
    kprintf ("%s: writing LEV_START to %s fail. %m\n", __func__, output_filename);
    return -1;
  }

  int last_local_id = 0;
  long long avail_bytes = A.size - STORAGE_LEV_START_SIZE;
  a += STORAGE_LEV_START_SIZE;

  while (avail_bytes >= sizeof (struct lev_storage_file) + 20) {
/*
    if (!last_local_id) {
      if (a != start + STORAGE_LEV_START_SIZE) {
        kprintf ("%s: there isn't LEV_STORAGE_FILE after LEV_START in file '%s'.\n",
          __func__, input_filename);
        return -1;
      }
    }
*/
    struct lev_storage_file *f = (struct lev_storage_file *) a;
    if (f->type != LEV_STORAGE_FILE && f->type != LEV_STORAGE_HIDE_FILE) {
      vkprintf (3, "%s: type isn't LEV_STORAGE_FILE or LEV_STORAGE_HIDE_FILE\n, off: %lld\n", __func__, (long long) (a - start));
      a += 4;
      avail_bytes -= 4;
      continue;
    }
    struct lev_crc32 *h = (struct lev_crc32 *) (a - 20);
    if (h->type != LEV_CRC32) {
      vkprintf (2, "%s: previous CRC32 logevent isn't found, off: %lld\n", __func__, (long long) (a - start));
      a += 4;
      avail_bytes -= 4;
      continue;
    }
    if (f->size < 0 || avail_bytes < (long long) sizeof (struct lev_storage_file) + f->size + 20LL) {
      vkprintf (2, "%s: size isn't good (local_id(?):%d), off: %lld\n", __func__,
        f->local_id, (long long) (a - start));
      a += 4;
      avail_bytes -= 4;
      continue;
    }
    int sz = (f->size + 3) & -4;
    struct lev_crc32 *t = (struct lev_crc32 *) (a + sizeof (struct lev_storage_file) + sz);
    if (t->type != LEV_CRC32) {
      vkprintf (2, "%s: next CRC32 logevent isn't found, off: %lld\n", __func__, (long long) (a - start));
      a += 4;
      avail_bytes -= 4;
      continue;
    }
    int ok = -1;
    if (a != start + STORAGE_LEV_START_SIZE) {
      ok = (crc32_partial (h, 20 + sizeof (struct lev_storage_file) + sz, ~h->crc32) == ~t->crc32);
    } else {
      ok = (crc32_partial (a, sizeof (struct lev_storage_file) + sz, P.crc32_complement) == ~t->crc32);
    }
    if (!ok) {
      vkprintf (2, "%s: crc32 between CRC32 logevent failed, off: %lld\n", __func__, (long long) (a - start));
      a += 4;
      avail_bytes -= 4;
      continue;
    }
    if (f->type == LEV_STORAGE_FILE) {
      unsigned char tmp[16];
      md5 (f->data, f->size, tmp);
      //TODO: try to fix one bit code
      if (memcmp (f->md5, tmp, 16)) {
        vkprintf (2, "%s: md5 is mismatched, off: %lld\n", __func__, (long long) (a - start));
        a += 4;
        avail_bytes -= 4;
        continue;
      }
    }
    if (f->type == LEV_STORAGE_HIDE_FILE && f->size) {
      vkprintf (2, "%s: LEV_STORAGE_FILE size isn't equal to 0, off: %lld\n", __func__, (long long) (a - start));
      a += 4;
      avail_bytes -= 4;
      continue;
    }
    if (f->type == LEV_STORAGE_FILE) {
      int i;
      if (last_mtime < f->mtime) {
        last_mtime = f->mtime;
      }
      assert (last_local_id < f->local_id);
      for (i = last_local_id + 1; i < f->local_id; i++) {
        struct lev_storage_file e;
        memcpy (&e, f, sizeof (e));
        e.secret = 0;
        e.size = 159;
        md5 (one_pix_jpg, e.size, e.md5);
        e.local_id = i;
        e.content_type = ct_jpeg;
        e.crc32 = compute_crc32 (one_pix_jpg, e.size);
        e.mtime = 437086800;
        vkprintf (1, "local_id = %d, bad image\n", i);
        bad_images++;
        if (write_image (&e, (char *) one_pix_jpg, &P) < 0) {
          return -1;
        }
      }
      last_local_id = f->local_id;
      good_images++;
    }
    if (f->type == LEV_STORAGE_HIDE_FILE) {
      hide_logevents++;
    }
    if (write_image (f, a + sizeof (struct lev_storage_file), &P) < 0) {
      return -1;
    }
    a += sizeof (struct lev_storage_file) + sz + 20;
    avail_bytes -= sizeof (struct lev_storage_file) + sz + 20;
  }
  assert (!fsync (wfd));
  long long off = lseek (wfd, 0, SEEK_CUR);
  assert (off >= 0);
  assert (!close (wfd));
  kprintf ("%lld bad images, %lld good_images, %lld hide logevents, lost %lld bytes.\n", bad_images, good_images, hide_logevents, (long long) A.size - off);
  fprintf (stderr, "NOTICE: all old indeces for volume %lld must be deleted.\n", volume_id);

  return 0;
}

void usage (void) {
  fprintf (stderr, "./storage-append [-v] [-1] <input-binlog> <output-binlog>\n"
                   "\t%s\n"
           "\tThis tool copies file from <input-binlog> to <output-binlog> until first error.\n"
           "\tIt could repair single bit error in file body.\n"
           "\tIf this tool write something to output, output will be ended by LEV_CRC32 logevent.\n"
           "\tOutput binlog should be ended by LEV_CRC32 or contains only LEV_START logevent (36 bytes).\n"
           "\tIf output binlog not exists, it will be created.\n"
           "\n"
           "\t[-1]\tKeep working if file body couldn't be recovered.\n"
           "\t[-m]\tTry to read <input-binlog> to memory and replace broken images with one-pix jpeg.\n"
           "\t\t<input-binlog> could be concatenated readable parts of broken storage volume.\n"
           "\t\tNOTICE:After -m fix, old index for repaired volume must be removed!\n"
           "\t[-t]\tExport to stdout wget download commands (for testing)\n"
           "\t[-H<http-port>]\tset storage-engine http port for [-t] mode\n",
           FullVersionStr);
}

int main (int argc, char *argv[]) {
  int i;
  while ((i = getopt (argc, argv, "1H:mtuv")) != -1) {
    switch (i) {
      case 'H':
        http_port = atoi (optarg);
      break;
      case 'm':
        memory_repairing = 1;
      break;
      case 't':
        test_mode = 1;
      break;
      case 'u':
        username = optarg;
      break;
      case 'v':
        verbosity++;
        break;
      case '1':
        exit_on_file_body_error = 0;
        break;
    }
  }
  if (argc < optind + 2) {
    usage ();
    return 1;
  }

  if (change_user (username) < 0) {
    kprintf ("fatal: cannot change user to %s\n", username ? username : "(none)");
    exit (1);
  }

  if (memory_repairing) {
    if (storage_memory_repair (argv[optind], argv[optind+1]) < 0) {
      unlink (argv[optind+1]);
      return 1;
    }
    return 0;
  }

  storage_binlog_append (argv[optind], argv[optind+1]);
  return 0;
}

