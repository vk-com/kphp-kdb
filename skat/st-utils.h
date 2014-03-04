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
 *          Utility functions useful everywhere.
 * Created: 16.03.2012
 * Update:  01.04.2012
 */

#pragma once

#ifdef _WIN32
#  define lrand48 rand
#  define srand48 srand
#endif

/**
 * Meta
 */

#define st_end }}

/**
 * Debug
 */

#define ST_TERMINATE { st_printf ("$1Terminated at $3" __FILE__ ":%ld$1$^\n", (long)__LINE__); exit (0); }


/**
 * Custom types
 */

typedef char bool;
typedef unsigned char byte;

#define TRUE  1
#define FALSE 0

/**
 * Utility functions
 */

#define st_relax_min(minx, _x) { \
  typeof (_x) t = (_x);          \
  if ((minx) > t) {              \
    (minx) = t;                  \
  }                              \
}                                \

#define st_relax_max(maxx, _x) { \
  typeof (_x) t = (_x);          \
  if ((maxx) < t) {              \
    (maxx) = t;                  \
  }                              \
}                                \

#define DECLARE_TEMPLATE_MIN_MAX(type, suffix)  \
  inline static type st_min##suffix (type x, type y) { \
    return x < y ? x : y;                       \
  }                                             \
  inline static type st_max##suffix (type x, type y) { \
    return x > y ? x : y;                       \
  }                                             \

DECLARE_TEMPLATE_MIN_MAX(int, )
DECLARE_TEMPLATE_MIN_MAX(long long, ll)
DECLARE_TEMPLATE_MIN_MAX(double, f)

#define st_sqr(x) ((x) * (x))

/* Colored printf version, writes formatted string into stderr stream:
 *   $[0-7] pattern to change font color,
 *   ^[0-7] pattern to change background color,
 *   $^     pattern to restore default coloring.
 * Color ordering: BLACK(0), RED(1), GREEN(2), YELLOW(3), BLUE(4), MAGENTA(5), CYAN(6), GREY(7)
 * Use cases:
 *   st_printf ("$1^2red on green $4 blue on green number: %d ^3 blue on yellow $^ normal\n", 30303);
 */
int st_printf (char const* format, ...);

/* Write time span provided in seconds in human readable format. */
void st_print_tspan (int sec);

/**
 * Working with engine kernel wrappers
 */

// Try to change user, exit if failed
void st_try_change_user (void);

// result of read_in will always be a string with length of len
// __data must have size at least (len + 1)
struct netbuffer;
inline void st_safe_read_in (struct netbuffer *H, char *__data, int len);

struct connection;
inline int st_return_one_key_flags (struct connection *c, const char *key, char *val, int vlen, int flags);

// Try to init vars for 'st_local_uid' function,
// asserts if failed.
void st_try_init_local_uid (void);
