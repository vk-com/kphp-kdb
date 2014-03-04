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

#ifndef __IMAGE_DATA_H__
#define __IMAGE_DATA_H__

#include <sys/resource.h>
#define MAX_SHARED_MEMORY_SIZE (1<<20)

//#define EXIT_BAD_AREA 2

/* constant should be power of 2 (used in check_type) */
enum forth_literal_type {
  ft_int = 1,
  ft_str = 2,
  ft_image = 4
};

struct stack_entry {
  void *a;
  enum forth_literal_type tp;
};

#define MAX_ERROR_BUF_SIZE 4096
#define MAX_STACK_SIZE 16384

struct forth_stack {
  struct stack_entry x[MAX_STACK_SIZE];
  int top;
  int thread_id;
  int error_len;
  char error[MAX_ERROR_BUF_SIZE];
  struct forth_output *O;
};

struct forth_output {
  long long prog_id;
  double working_time;
  int l;
  char s[MAX_SHARED_MEMORY_SIZE - 20];
};

void image_init (char *prog_name, long long max_load_image_area, long long memory_limit, long long map_limit, long long disk_limit, int threads_limit);
void image_done (void);

/* value shoudbe at least value_len + 1 size */
/* return EXIT_SUCCESS on success execution */
/* thread_id used only for debug purpose */
int image_exec (long long prog_id, char *value, int value_len, int thread_id, int shm_descriptor);
void image_reserved_words_hashtable_init (void);
double get_rusage_time (int who);
#endif
