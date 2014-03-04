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

    Copyright 2013 Vkontakte Ltd
              2013 Anton Maydell
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

#define	VERSION_STR	"fix-rotateto-1.02"
const char FullVersionStr[] = VERSION_STR " compiled at " __DATE__ " " __TIME__ " by gcc " __VERSION__ " "
#ifdef __LP64__
  "64-bit"
#else
  "32-bit"
#endif
" after commit " COMMIT;

const char *fnames[3];
int fd[3];
long long fsize[3];

int open_file (int x, const char *fname, int creat) {
  fnames[x] = fname;
  fd[x] = open (fname, creat > 0 ? O_RDWR | O_CREAT | O_EXCL : O_RDONLY, 0640);
  if (fd[x] < 0) {
    kprintf ("%s: cannot open %s: %m\n", __func__, fname);
    exit (1);
  }
  fsize[x] = lseek (fd[x], 0, SEEK_END);
  if (fsize[x] < 0) {
    kprintf ("%s: cannot seek %s: %m\n", __func__, fname);
    exit (2);
  }
  lseek (fd[x], 0, SEEK_SET);
  vkprintf (1, "opened file %s, fd=%d, size=%lld\n", fname, fd[x], fsize[x]);
  return fd[x];
}

#define BUFF_SIZE 0x1000000

char buff[BUFF_SIZE];

static int fix_rotate_to (const char *src1, const char *src2, const char *dst) {
  const int fd2 = open_file (1, src2, 0);
  if (fd2 < 0) {
    return -1;
  }

  if (fsize[1] < 36) {
    kprintf ("%s: file '%s' size is too small (%lld bytes).\n", __func__, fnames[1], fsize[1]);
    return -1;
  }

  struct lev_rotate_from RF;
  ssize_t s = read (fd[1], &RF, 36);
  if (s != 36) {
    kprintf ("%s: fail to read LEV_ROTATE_TO from file '%s' (%d bytes was read)\n",
      __func__, fnames[1], (int) s);
    if (s < 0) {
      kprintf ("%m\n");
    }
    return -1;
  }
  if (RF.type != LEV_ROTATE_FROM) {
    kprintf ("%s: file '%s' isn't started from LEV_ROTATE_FROM logevent (type is 0x%08x).\n", __func__, fnames[1], RF.type);
    return -1;
  }

  const int fd1 = open_file (0, src1, 0);
  if (fd1 < 0) {
    return -1;
  }

  const long long read_till_pos = RF.cur_log_pos;

  const int fd3 = open_file (2, dst, 1);
  if (fd3 < 0) {
    exit (1); /* don't unlink destination file */
  }

  assert (lock_whole_file (fd3, F_WRLCK) > 0);

  long long read_off = 0;

  unsigned int log_crc32 = -1;
  long long start_log_pos = -1, cur_log_pos = -1;

  long long till_off = fsize[0];

  while (read_off < till_off) {
    int sz = till_off - read_off < BUFF_SIZE ? till_off - read_off : BUFF_SIZE;
    s = read (fd[0], buff, sz);
    if (s != sz) {
      kprintf ("%s: fail to read from file '%s' (%d bytes was read of expected %d bytes)",
          __func__, fnames[0], (int) s, sz);
      if (s < 0) {
        kprintf ("%m\n");
      }
      return -1;
    }
    vkprintf (2, "read %d bytes from file '%s'\n", (int) s, fnames[0]);

    if (!read_off) {
      struct lev_generic *G = (struct lev_generic *) buff;
      switch (G->type) {
        case LEV_START:
          assert (s >= 24);
          log_crc32 = 0;
          start_log_pos = 0;
          break;
        case LEV_ROTATE_FROM:
          assert (s >= 36);
          log_crc32 = ((struct lev_rotate_from *) buff)->crc32;
          start_log_pos = ((struct lev_rotate_from *) buff)->cur_log_pos;
          break;
        default:
          kprintf ("'%s' first logevent type(0x%08x) isn't LEV_START or LEV_ROTATE_FROM\n", fnames[0], G->type);
          return -1;
      }

      cur_log_pos = start_log_pos;
      till_off = read_till_pos - start_log_pos - 36;
      assert (read_off + s <= till_off);

      if (fsize[0] < till_off) {
        kprintf ("file '%s' is too short (%lld bytes), expected at least %lld bytes.\n", fnames[0], fsize[0], till_off);
        return -1;
      }
    }

    assert (write (fd[2], buff, s) == s);

    log_crc32 = ~crc32_partial (buff, s, ~log_crc32);
    cur_log_pos += s;
    read_off += s;
  }

  //assert (cur_log_pos == RF.

  struct lev_rotate_to RT;
  memset (&RT, 0, sizeof (RT));
  RT.type = LEV_ROTATE_TO;
  RT.timestamp = RF.timestamp;
  RT.next_log_pos = RF.cur_log_pos;
  RT.crc32 = log_crc32;
  RT.cur_log_hash = RF.prev_log_hash;
  RT.next_log_hash = RF.cur_log_hash;

  if (match_rotate_logevents (&RT, &RF) <= 0) {
    return -1;
  }

  assert (write (fd[2], &RT, 36) == 36);

  log_crc32 = ~crc32_partial (&RT, 36, ~log_crc32);
  if (log_crc32 != RF.crc32) {
    kprintf ("Computed crc32(0x%08x) for destination binlog '%s' isn't equal to crc32(0x%08x) in ROTATE_FROM in '%s'.\n",
      log_crc32, dst,  RF.crc32, src2);
    return -1;
  }

  int i;
  assert (!fsync (fd[2]));
  for (i = 0; i < 3; i++) {
    assert (!close (i));
  }

  return 0;
}

void usage (void) {
  printf ("%s\n", FullVersionStr);
  printf (
    "fix-rotateto [-u<username>] [-c<max-conn>] [-v] <src-binlog1> <src-binlog2> <dst-binlog1> \n"
    "\tTry to append ROTATE_TO logevent to <src-binlog1> using ROTATE_FROM logevent from <src-binlog2>.\n"
    "\tAfter running this tool always use check-binlog before deleting <src-binlog1>.\n"
    "\t[-v]\t\toutput statistical and debug information into stderr\n"
    "\t[-k]\t\tkeep <dst-binlog1> in case some error was occured.\n"
  );
  exit (2);
}

int main (int argc, char *argv[]) {
  int i;
  static int keep_bad_dest_file = 0;
  if (getuid ()) {
    maxconn = 10;
  }
  set_debug_handlers ();
  while ((i = getopt (argc, argv, "c:hku:v")) != -1) {
    switch (i) {
    case 'c':
      maxconn = atoi (optarg);
      if (maxconn <= 0 || maxconn > MAX_CONNECTIONS) {
      	maxconn = MAX_CONNECTIONS;
      }
    break;
    case 'h':
      usage ();
    break;
    case 'k':
      keep_bad_dest_file = 1;
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

  if (optind + 3 != argc) {
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

  if (fix_rotate_to (argv[optind], argv[optind+1], argv[optind+2]) < 0) {
    if (!keep_bad_dest_file) {
      if (!unlink (argv[optind+2])) {
        kprintf ("Bad destination file '%s' was succesfully deleted.\n", argv[optind+2]);
      }
    }
    return 1;
  }

  return 0;
}

