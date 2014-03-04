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

    Copyright 2012 Vkontakte Ltd
              2012 Sergey Kopeliovich <Burunduk30@gmail.com>
              2012 Anton Timofeev <atimofeev@vkontakte.ru>
*/

/**
 * Author:  Sergey Kopeliovich (Burunduk30@gmail.com)
 *          Anton  Timofeev    (atimofeev@vkontakte.ru)
 * Created: 21.03.2012
 * Updated: 01.04.2012
 */

#define _FILE_OFFSET_BITS 64

#include <stdio.h>
#include <stdlib.h>
#include <stdarg.h>
#include <string.h>
#include <assert.h>
#include <unistd.h>

#include "net-buffers.h"
#include "net-connections.h"

#include "st-utils.h"

#define MAX_STR_BUF_LEN (1 << 16)
static char str_buf[MAX_STR_BUF_LEN];

/**
 * Utility functions
 */

#define FONT_COLOR_TAG '$'
#define BKGR_COLOR_TAG '^'

int st_printf (char const* format, ...) {
  bool coloring = isatty (2/* fileno (stderr) */);

  va_list args;
  int ret = -1;

  int k = 0;
  char const *fptr = format;
  char *s = str_buf;
  while (*fptr && k + 10 < MAX_STR_BUF_LEN) {
    if ((fptr[0] == FONT_COLOR_TAG || fptr[0] == BKGR_COLOR_TAG)
        && '0' <= fptr[1] && fptr[1] <= '7') {
      if (coloring) {
        *s++ = '\e';
        *s++ = '[';
        *s++ = '3' + (fptr[0] == BKGR_COLOR_TAG);
        *s++ = fptr[1];
        *s++ = 'm';
      }
      fptr += 2;
    } else if (fptr[0] == FONT_COLOR_TAG && fptr[1] == BKGR_COLOR_TAG) {
      if (coloring) {
        *s++ = '\e';
        *s++ = '[';
        *s++ = '0';
        *s++ = 'm';
      }
      fptr += 2;
    } else {
      *s++ = *fptr++;
    }
  }
  *s = 0;

  va_start (args, format);
  ret = vfprintf (stderr, str_buf, args);
  va_end (args);
  return ret;
}

void st_print_tspan (int sec) {
  if (sec < 60) {
    fprintf (stderr, "%d", sec);
  } else if (sec < 3600) {
    int minutes = sec / 60;
    fprintf (stderr, "%dm", minutes);
      fprintf (stderr, "%ds", sec % 60);
  } else if (sec < 86400) {
    int hours = sec / 3600;
    fprintf (stderr, "%dh", hours);
      fprintf (stderr, "%dm", (sec % 3600) / 60);
  } else if (sec < 31536000) {
    int days = sec / 86400;
    fprintf (stderr, "%dd", days);
      fprintf (stderr, "%dh", (sec % 86400) / 3600);
  } else {
    fprintf (stderr, ">year");
  }
}

/**
 * Working with engine kernel wrappers
 */

// result of read_in will always be a string with length of len
// __data must have size at least (len + 1)
inline void st_safe_read_in (netbuffer_t *H, char *__data, int len) {
  assert (read_in (H, __data, len) == len);
  int i;
  for (i = 0; i < len; i++) {
    if (__data[i] == 0) {
      __data[i] = ' ';
    }
  }
  __data[len] = 0;
}

inline int st_return_one_key_flags (struct connection *c, const char *key, char *val, int vlen, int flags) {
  int l = snprintf (str_buf, MAX_STR_BUF_LEN, "VALUE %s %d %d\r\n", key, flags, vlen);
  assert (l <= MAX_STR_BUF_LEN);
  write_out (&c->Out, str_buf, l);
  write_out (&c->Out, val, vlen);
  write_out (&c->Out, "\r\n", 2);
  return 0;
}

/**
 * Writing engine stats
 */

void st_try_change_user (void)
{
  if (change_user (username) < 0) {
    fprintf (stderr, "fatal: cannot change user to %s\n", username ? username : "(default)");
    exit (1);
  }
  if (verbosity >= 2) {
    fprintf (stderr, "User changed to %s\n", username ? username : "(default)");
  }
}

void st_write_stats_line (char** buf_ptr, int width, char const* line_name, char const* format, ...) {
  va_list args;
  int spaces_cnt = width - strlen (line_name);
  if (spaces_cnt < 1) {
    spaces_cnt = 1;
  }

  *buf_ptr += sprintf (*buf_ptr, "%s%*c", line_name, spaces_cnt, ' ');
  va_start (args, format);
  *buf_ptr += vsprintf (*buf_ptr, format, args);
  va_end (args);
}

extern int log_split_min, log_split_max, log_split_mod;
void st_try_init_local_uid (void) {
  static int was = 0;
  static int old_log_split_min, old_log_split_max, old_log_split_mod;

  if (was) {
// DEBUG: fprintf (stderr, "%d vs %d | %d vs %d | %d vs %d\n", old_log_split_min, log_split_min, old_log_split_max, log_split_max, old_log_split_mod, log_split_mod);
    assert (old_log_split_min == log_split_min && old_log_split_max == log_split_max && old_log_split_mod == log_split_mod);
    return;
  }

  was = 1;
  old_log_split_min = log_split_min;
  old_log_split_max = log_split_max;
  old_log_split_mod = log_split_mod;
}
