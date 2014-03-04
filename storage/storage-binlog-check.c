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
#include <stdlib.h>
#include <stddef.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <aio.h>
#include <dirent.h>
#include <pthread.h>
#include <sys/types.h>
#include <sys/wait.h>

#include "md5.h"
#include "crc32.h"
#include "server-functions.h"
#include "net-aio.h"
#include "kdb-storage-binlog.h"
#include "storage-data.h"


#define	VERSION_STR	"storage-binlog-check-0.01"
const char FullVersionStr[] = VERSION_STR " compiled at " __DATE__ " " __TIME__ " by gcc " __VERSION__ " "
#ifdef __LP64__
  "64-bit"
#else
  "32-bit"
#endif
;

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

static unsigned idx_crc32_complement;

static void clearin (void) {
  rptr = wptr = io_buff + IO_BUFFSIZE;
  bytes_read = 0;
  idx_crc32_complement = -1;
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

static void bread (void *b, size_t len) {
  void *p = readin (len);
  assert (p != NULL);
  memcpy (b, p, len);
  readadv (len);
  bytes_read += len;
  idx_crc32_complement = crc32_partial (p, len, idx_crc32_complement);
}

unsigned char file_buff[IO_BUFFSIZE];

void storage_binlog_check (char *filename) {
  int local_id = 0;
  volume_t *V, tmp_volume;
  V = &tmp_volume;
  memset (&tmp_volume, 0, sizeof (tmp_volume));
  const int fd = open (filename, O_RDONLY);
  if (fd < 0) {
    kprintf ("Couldn't open %s\n", filename);
    exit (1);
  }
  struct stat buf;
  if (fstat (fd, &buf) < 0) {
    close (fd);
    kprintf ("fstat fail, %m\n");
    exit (1);
  }
  long long file_size = buf.st_size;

  idx_fd = fd;
  clearin ();
  V->cur_log_pos = 0;
  V->log_crc32 = 0;
  while (V->cur_log_pos < file_size) {
    if (!V->cur_log_pos) {
      unsigned char lev_start_buff[LEV_STORAGE_START_SIZE];
      struct lev_start *E = (struct lev_start *) &lev_start_buff;
      bread (E, LEV_STORAGE_START_SIZE);
      if (E->type != LEV_START) {
        kprintf ("[%s] expected LEV_START but %x found.\n", filename, E->type);
        exit (1);
      }
      if (storage_le_start (V, E) < 0) {
        kprintf ("[%s] storage_le_start fail\n", filename);
        exit (1);
      }
      V->cur_log_pos += LEV_STORAGE_START_SIZE;
    } else {
      local_id++;
      struct lev_storage_file E;
      int sz = sizeof (E);
      bread (&E, sz);

      long long event_pos = V->cur_log_pos;
      if (E.type != LEV_STORAGE_FILE) {
        kprintf ("[%s:%lld] expected LEV_STORAGE_FILE, but %x found\n", filename, event_pos, E.type);
        exit (1);
      }

      if (E.local_id != local_id) {
        kprintf ("[%s:%lld] E.local_id = %d, but expected %d.\n", filename, event_pos, E.local_id, local_id);
        exit (1);
      }

      int k = E.size;
      unsigned crc32_complement = -1;
      md5_context ctx;
      md5_starts (&ctx);
      while (k > 0) {
        int o = k;
        if (o > IO_BUFFSIZE) {
          o = IO_BUFFSIZE;
        }
        bread (file_buff, o);
        crc32_complement = crc32_partial (file_buff, o, crc32_complement);
        md5_update (&ctx, file_buff, o);
        k -= o;
      }

      if (~crc32_complement != E.crc32) {
        kprintf ("[%s:%lld] LEV_STORAGE_FILE crc32 check fail.\n", filename, event_pos);
        exit (1);
      }

      unsigned char md5[16];
      md5_finish (&ctx, md5);
      if (memcmp (md5, E.md5, 16)) {
        kprintf ("[%s:%lld] LEV_STORAGE_FILE md5 check fail.\n", filename, event_pos);
        exit (1);
      }

      int l = 4 - (E.size & 3);
      if (l != 4) {
        int i;
        bread (file_buff, l);
        for (i = 0; i < l; i++) {
          if (file_buff[i]) {
            kprintf ("[%s:%lld] LEV_STORAGE_FILE doesn't padded by zeroes\n", filename, event_pos);
            exit (1);
          }
        }
      } else {
        l = 0;
      }

      V->cur_log_pos += sz + E.size + l;

      struct lev_crc32 C;
      sz = sizeof (struct lev_crc32);

      const unsigned log_crc32 = ~idx_crc32_complement;

      bread (&C, sz);
      event_pos = V->cur_log_pos;
      if (C.type != LEV_CRC32) {
        kprintf ("[%s:%lld] expected LEV_CRC32, but %x found.\n", filename, event_pos, C.type);
        exit (1);
      }

      if (event_pos != C.pos) {
        kprintf ("[%s:%lld] LEV_CRC32 field pos (%lld) != log event offset\n", filename, event_pos, C.pos);
        exit (1);
      }

      if (C.crc32 != log_crc32) {
        kprintf ("[%s:%lld] LEV_CRC32 field crc (%x) != V->log_crc32 (%x)", filename, event_pos, C.crc32, log_crc32);
        exit (1);
      }

      V->cur_log_pos += sz;
    }
  }
}

void usage (void) {
  fprintf (stderr, "./storage-binlog-check <input-binlog>\n"
                   "\t%s\n",
           FullVersionStr);
}

int main (int argc, char *argv[]) {
  if (argc < 2) {
    usage ();
    return 1;
  }
  storage_binlog_check (argv[1]);
  return 0;
}

