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

    Copyright 2012-2013 Vkontakte Ltd
              2012-2013 Anton Maydell
*/

#define	_FILE_OFFSET_BITS	64

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdarg.h>
#include <ctype.h>
#include <assert.h>
#include <errno.h>
#include <unistd.h>
#include <fcntl.h>
#include "kdb-data-common.h"
#include "am-stats.h"

static int read_whole_file (char *filename, void *output, int olen) {
  int fd = open (filename, O_RDONLY), n = -1;
  if (fd < 0) {
    vkprintf (1, "%s: open (\"%s\", O_RDONLY) failed. %m\n", __func__, filename);
    return -1;
  }
  do {
    n = read (fd, output, olen);
    if (n < 0) {
      if (errno == EINTR) {
        continue;
      }
      vkprintf (1, "%s: read from %s failed. %m\n", __func__, filename);
    }
    break;
  } while (1);
  while (close (fd) < 0 && errno == EINTR) {}
  if (n < 0)  {
    return -1;
  }
  if (n >= olen) {
    vkprintf (1, "%s: output buffer is too small (%d bytes).\n", __func__, olen);
    return -1;
  }
  unsigned char *p = output;
  p[n] = 0;
  return n;
}

static int parse_statm (const char *buf, long long *a, int m) {
  static long long page_size = -1;
  if (page_size < 0) {
    page_size = sysconf (_SC_PAGESIZE);
    assert (page_size > 0);
  }
  int i;
  if (m > 7) {
    m = 7;
  }
  const char *p = buf;
  char *q;
  errno = 0;
  for (i = 0; i < m; i++) {
    a[i] = strtoll (p, &q, 10);
    if (p == q || errno) {
      return -1;
    }
    a[i] *= page_size;
    p = q;
  }
  return 0;
}

int am_get_memory_usage (pid_t pid, long long *a, int m) {
  char proc_filename[32];
  char buf[4096];
  assert (snprintf (proc_filename, sizeof (proc_filename), "/proc/%d/statm",  (int) pid) < sizeof (proc_filename));
  if (read_whole_file (proc_filename, buf, sizeof (buf)) < 0) {
    return -1;
  }
  return parse_statm (buf, a, m);
}

int am_get_memory_stats (am_memory_stat_t *S, int flags) {
  if (!flags) {
    return -1;
  }
  long long a[6];
  memset (S, 0, sizeof (*S));

  if (flags & AM_GET_MEMORY_USAGE_SELF) {
    if (am_get_memory_usage (getpid (), a, 6) < 0) {
      return -1;
    }
    S->vm_size = a[0];
    S->vm_rss = a[1];
    S->vm_data = a[5];
  }

  if (flags & AM_GET_MEMORY_USAGE_OVERALL) {
    char buf[16384], *p;
    if (read_whole_file ("/proc/meminfo", buf, sizeof (buf)) < 0) {
      return -1;
    }
    vkprintf (4, "/proc/meminfo: %s\n", buf);
    char key[32], suffix[32];
    long long value;
    int r = 0;
    for (p = strtok (buf, "\n"); p != NULL; p = strtok (NULL, "\n")) {
      if (sscanf (p, "%31s%lld%31s", key, &value, suffix) == 3 && !strcmp (suffix, "kB")) {
        if (!strcmp (key, "MemFree:")) {
          S->mem_free = value << 10;
          r |= 1;
        } else if (!strcmp (key, "SwapTotal:")) {
          S->swap_total = value << 10;
          r |= 2;
        } else if (!strcmp (key, "SwapFree:")) {
          S->swap_free = value << 10;
          r |= 4;
        }
      }
    }
    if (r != 7) {
      return -1;
    }
    S->swap_used = S->swap_total - S->swap_free;
  }
  return 0;
}

/************************ stats buffer functions **********************************/
static void sb_truncate (stats_buffer_t *sb) {
  sb->buff[sb->size - 1] = 0;
  sb->pos = sb->size - 2;
  while (sb->pos >= 0 && sb->buff[sb->pos] != '\n') {
    sb->buff[sb->pos--] = 0;
  }
  sb->pos++;
}

void sb_prepare (stats_buffer_t *sb, struct connection *c, char *buff, int size) {
  sb->buff = buff;
  sb->size = size;
  sb->pos = prepare_stats (c, buff, size);
  if ((sb->pos == size - 1 && sb->buff[sb->pos]) || sb->pos >= size) {
    sb_truncate (sb);
  }
}

void sb_printf (stats_buffer_t *sb, const char *format, ...) {
  if (sb->pos >= sb->size) { return; }
  va_list ap;
  va_start (ap, format);
  sb->pos += vsnprintf (sb->buff + sb->pos, sb->size - sb->pos, format, ap);
  va_end (ap);
  if (sb->pos >= sb->size) {
    sb_truncate (sb);
  }
}
/************************************************************************************/

void sb_memory (stats_buffer_t *sb, int flags) {
  dyn_update_stats ();
  sb_printf (sb,
    "heap_allocated\t%ld\n"
    "heap_max\t%ld\n"
    "wasted_heap_blocks\t%d\n"
    "wasted_heap_bytes\t%ld\n"
    "free_heap_blocks\t%d\n"
    "free_heap_bytes\t%ld\n",
    (long)(dyn_cur - dyn_first) + (long) (dyn_last - dyn_top),
    (long)(dyn_last - dyn_first),
    wasted_blocks,
    wasted_bytes,
    freed_blocks,
    freed_bytes);

  am_memory_stat_t S;
  if (!am_get_memory_stats (&S, flags & AM_GET_MEMORY_USAGE_SELF)) {
    sb_printf (sb,
      "vmsize_bytes\t%lld\n"
      "vmrss_bytes\t%lld\n"
      "vmdata_bytes\t%lld\n",
    S.vm_size, S.vm_rss, S.vm_data);
  }

  if (!am_get_memory_stats (&S, flags & AM_GET_MEMORY_USAGE_OVERALL)) {
    sb_printf (sb,
        "memfree_bytes\t%lld\n"
        "swap_used_bytes\t%lld\n"
        "swap_total_bytes\t%lld\n",
    S.mem_free, S.swap_used, S.swap_total);
  }
}

void sb_print_queries (stats_buffer_t *sb, const char *const desc, long long q) {
  sb_printf (sb, "%s\t%lld\nqps_%s\t%.3lf\n", desc, q, desc, safe_div (q, now - start_time));
}

int get_at_prefix_length (const char *key, int key_len) {
  int i = 0;
  if (key_len > 0 && key[0] == '!') {
    i++;
  }
  if (i < key_len && key[i] == '-') {
    i++;
  }
  int j = i;
  while (j < key_len && isdigit (key[j])) {
    j++;
  }
  if (i == j) {
    return 0;
  }
  if (j < key_len && key[j] == '@') {
    return j + 1;
  }
  return 0;
}
