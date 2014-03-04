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

    Copyright 2013 Vkontakte Ltd
              2013 Nikolai Durov
              2013 Andrei Lopatin
*/

#include <assert.h>

#include "fast-backtrace.h"

extern void *__libc_stack_end;

struct stack_frame {
  struct stack_frame *bp;
  void *ip;
};

static __inline__ void *get_bp (void) {
  void *bp;
#if defined(__i386__)
  __asm__ volatile ("movl %%ebp, %[r]" : [r] "=r" (bp));
#elif defined(__x86_64__)
  __asm__ volatile ("movq %%rbp, %[r]" : [r] "=r" (bp));
#endif
  return bp;
}

int fast_backtrace (void **buffer, int size) {
  struct stack_frame *bp = get_bp ();
  int i = 0;
  while (i < size && (void *) bp <= __libc_stack_end && !((long) bp & (sizeof (long) - 1))) {
    void *ip = bp->ip;
    buffer[i++] = ip;
    struct stack_frame *p = bp->bp;
    if (p <= bp) {
      break;
    }
    bp = p;
  }
  return i;
}
