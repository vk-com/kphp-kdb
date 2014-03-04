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
              2012-2013 Arseny Smirnov
              2012-2013 Aliaksei Levin
*/

#define _FILE_OFFSET_BITS 64

#include <cstdlib>
#include <cstring>
#include <signal.h>
#include <cstdio>

#include <functional>
#include <map>
#include <utility>

#include "include.h"

void check_stack_overflow (void) __attribute__ ((weak));
void check_stack_overflow (void) {
  //pass;
}

namespace dl {

const size_type MAX_BLOCK_SIZE = 16384;


volatile int in_critical_section;
volatile long long pending_signals;


void enter_critical_section (void) {
  check_stack_overflow();
  php_assert (in_critical_section >= 0);
  in_critical_section++;
}

void leave_critical_section (void) {
  in_critical_section--;
  php_assert (in_critical_section >= 0);
  if (pending_signals && in_critical_section <= 0) {
    for (int i = 0; i < (int)sizeof (pending_signals) * 8; i++) {
      if ((pending_signals >> i) & 1) {
        raise (i);
      }
    }
  }
}

//custom allocator for multimap
template <class T>
class script_allocator {
public:
  typedef dl::size_type size_type;
  typedef ptrdiff_t difference_type;
  typedef T* pointer;
  typedef const T* const_pointer;
  typedef T& reference;
  typedef const T& const_reference;
  typedef T value_type;
  
  template <class U>
  struct rebind {
    typedef script_allocator<U> other;
  };
   
  script_allocator () throw() {
  }

  script_allocator (const script_allocator &) throw() {
  }

  template <class U>
  script_allocator (const script_allocator <U> &) throw() {
  }

  ~script_allocator() throw() {
  }
   
  pointer address (reference x) const {
    return &x;
  }

  const_pointer address (const_reference x) const {
    return &x;
  }

  pointer allocate (size_type n, void const * = 0) {
    php_assert (n == 1 && sizeof (T) < MAX_BLOCK_SIZE);
    return static_cast <pointer> (dl::allocate (sizeof (T)));
  }

  void deallocate (pointer p, size_type n) {
    php_assert (n == 1 && sizeof (T) < MAX_BLOCK_SIZE);
    dl::deallocate (static_cast <void *> (p), sizeof (T));
  }

  size_type max_size() const throw() { 
    return INT_MAX / sizeof (T); 
  }

  void construct (pointer p, const T& val) {
    new (p) T (val);
  }

  void destroy (pointer p) {
    p->~T();
  }
};


long long query_num = 0;
bool script_runned = false;
bool use_script_allocator = false;

bool allocator_inited = false;

size_t memory_begin;
size_t memory_end = LONG_MAX;
size_type memory_limit;
size_type memory_used;
size_type max_memory_used;
size_type max_real_memory_used;

size_type static_memory_used;

typedef std::multimap <size_type, void *, std::less <size_type>, script_allocator <std::pair <const size_type, void *> > > map_type;
char left_pieces_storage[sizeof (map_type)];
map_type *left_pieces = reinterpret_cast <map_type *> (left_pieces_storage);
void *piece, *piece_end;
void *free_blocks[MAX_BLOCK_SIZE >> 3];


size_type memory_get_total_usage (void) {
  return (size_type)((size_t)piece - memory_begin);
}


const bool stupid_allocator = false;


inline void allocator_add (void *block, size_type size) {
  if ((char *)block + size == (char *)piece) {
    piece = block;
    return;
  }

  if (size < MAX_BLOCK_SIZE) {
    size >>= 3;
    *(void **)block = free_blocks[size];
    free_blocks[size] = block;
  } else {
    left_pieces->insert (std::make_pair (size, block));
  }
}

void allocator_init (void *buf, size_type n) {
  in_critical_section = 0;
  pending_signals = 0;

  enter_critical_section();

  allocator_inited = true;
  use_script_allocator = false;
  memory_begin = (size_t)buf;
  memory_end = (size_t)buf + n;
  memory_limit = n;

  memory_used = 0;
  max_memory_used = 0;
  max_real_memory_used = 0;

  if (query_num == 0) {
//    static_memory_used = 0;//do not count shared memory
  }

  piece = buf;
  piece_end = (void *)((char *)piece + n);

  new (left_pieces_storage) map_type();
  memset (free_blocks, 0, sizeof (free_blocks));

  query_num++;
  script_runned = true;

  leave_critical_section();
}

inline void *allocate_stack (size_type n) {
  if ((char *)piece_end - (char *)piece >= n) {
    void *result = piece;
    piece = (void *)((char *)piece + n);
    return result;
  } else {
    php_warning ("Can't allocate %d bytes", (int)n);
    raise (SIGUSR2);
    return NULL;
  }
}

void *allocate (size_type n) {
  if (!allocator_inited) {
    return static_allocate (n);
  }

  if (!script_runned) {
    return NULL;
  }
  enter_critical_section();

  php_assert (n);

  void *result;
  if (stupid_allocator) {
    result = allocate_stack (n);
  } else {
    n = (n + 7) & -8;

    if (n < MAX_BLOCK_SIZE) {
      size_type nn = n >> 3;
      if (free_blocks[nn] == NULL) {
        result = allocate_stack (n);
        //fprintf (stderr, "allocate %d, chunk not found, allocating from stack at %p\n", n, result);
      } else {
        result = free_blocks[nn];
        //fprintf (stderr, "allocate %d, chunk found at %p\n", n, result);
        free_blocks[nn] = *(void **)result;
      }
    } else {
      typeof (left_pieces->end()) p = left_pieces->lower_bound (n);
      //fprintf (stderr, "allocate %d from %d, map size = %d\n", n, p == left_pieces->end() ? -1 : (int)p->first, (int)left_pieces->size());
      if (p == left_pieces->end()) {
        result = allocate_stack (n);
      } else {
        size_type left = p->first - n;
        result = p->second;
        left_pieces->erase (p);
        if (left) {
          allocator_add ((char *)result + n, left);
        }
      }
    }
  }

  if (result != NULL) {
    memory_used += n;
    if (memory_used > max_memory_used) {
      max_memory_used = memory_used;
    }
    size_type real_memory_used = (size_type)((size_t)piece - memory_begin);
    if (real_memory_used > max_real_memory_used) {
      max_real_memory_used = real_memory_used;
    }
  }

  leave_critical_section();
  return result;
}

void *allocate0 (size_type n) {
  if (!allocator_inited) {
    return static_allocate0 (n);
  }

  if (!script_runned) {
    return NULL;
  }

  return memset (allocate (n), 0, n);
}

void *reallocate (void *p, size_type new_n, size_type old_n) {
  if (!allocator_inited) {
    static_reallocate (&p, new_n, &old_n);
    return p;
  }

  if (!script_runned) {
    return p;
  }
  enter_critical_section();

  php_assert (new_n > old_n);

  //real reallocate
  size_type real_old_n = (old_n + 7) & -8;
  if ((char *)p + real_old_n == (char *)piece) {
    size_type real_new_n = (new_n + 7) & -8;
    size_type add = real_new_n - real_old_n;
    if ((char *)piece_end - (char *)piece >= add) {
      piece = (void *)((char *)piece + add);
      memory_used += add;
      if (memory_used > max_memory_used) {
        max_memory_used = memory_used;
      }
      size_type real_memory_used = (size_type)((size_t)piece - memory_begin);
      if (real_memory_used > max_real_memory_used) {
        max_real_memory_used = real_memory_used;
      }

      leave_critical_section();
      return p;
    }
  }

  void *result = allocate (new_n);
  if (result != NULL) {
    memcpy (result, p, old_n);
    deallocate (p, old_n);
  }

  leave_critical_section();
  return result;
}

void deallocate (void *p, size_type n) {
//  fprintf (stderr, "deallocate %d: allocator_inited = %d, script_runned = %d\n", n, allocator_inited, script_runned);
  if (!allocator_inited) {
    return static_deallocate (&p, &n);
  }

  if (stupid_allocator) {
    return;
  }

  if (!script_runned) {
    return;
  }
  enter_critical_section();

  n = (n + 7) & -8;
  //fprintf (stderr, "deallocate %d at %p\n", n, p);
  memory_used -= n;
  allocator_add (p, n);

  leave_critical_section();
}


void *static_allocate (size_type n) {
  php_assert (!query_num || !use_script_allocator);
  enter_critical_section();

  php_assert (n);

  void *result = malloc (n);
//  fprintf (stderr, "static allocate %d at %p\n", n, result);
  if (result == NULL) {
    php_warning ("Can't static_allocate %d bytes", (int)n);
    raise (SIGUSR2);
    leave_critical_section();
    return NULL;
  }

  static_memory_used += n;
  leave_critical_section();
  return result;
}

void *static_allocate0 (size_type n) {
  php_assert (!query_num);
  enter_critical_section();

  php_assert (n);

  void *result = calloc (1, n);
//  fprintf (stderr, "static allocate0 %d at %p\n", n, result);
  if (result == NULL) {
    php_warning ("Can't static_allocate0 %d bytes", (int)n);
    raise (SIGUSR2);
    leave_critical_section();
    return NULL;
  }

  static_memory_used += n;
  leave_critical_section();
  return result;
}

void static_reallocate (void **p, size_type new_n, size_type *n) {
//  fprintf (stderr, "static reallocate %d at %p\n", *n, *p);

  enter_critical_section();
  static_memory_used -= *n;

  void *old_p = *p;
  *p = realloc (*p, new_n);
  if (*p == NULL) {
    php_warning ("Can't static_reallocate from %d to %d bytes", (int)*n, (int)new_n);
    raise (SIGUSR2);
    *p = old_p;
    leave_critical_section();
    return;
  }
  *n = new_n;

  static_memory_used += *n;
  leave_critical_section();
}

void static_deallocate (void **p, size_type *n) {
//  fprintf (stderr, "static deallocate %d at %p\n", *n, *p);

  enter_critical_section();
  static_memory_used -= *n;

  free (*p);
  *p = NULL;
  *n = 0;

  leave_critical_section();
}

void *malloc_replace (size_t x) {
  size_t real_allocate = x + sizeof (size_t);
  php_assert (real_allocate >= sizeof (size_t));
  void *p;
  if (use_script_allocator) {
    p = allocate (real_allocate);
  } else {
    p = static_allocate (real_allocate);
  }
  if (p == NULL) {
    php_critical_error ("Not enough memory to continue");
  }
  *(size_t *)p = real_allocate;
  return (void *)((char *)p + sizeof (size_t));
}

void free_replace (void *p) {
  if (p == NULL) {
    return;
  }

  p = (void *)((char *)p - sizeof (size_t));
  if (use_script_allocator) {
    php_assert (memory_begin <= (size_t)p && (size_t)p < memory_end);
    deallocate (p, *(size_t *)p);
  } else {
    size_type n = *(size_t *)p;
    static_deallocate (&p, &n);
  }
}

}

//replace global operators new and delete for linked C++ code
void *operator new (std::size_t n) throw (std::bad_alloc) {
  return dl::malloc_replace (n);
}

void *operator new (std::size_t n, const std::nothrow_t &) throw() {
  return dl::malloc_replace (n);
}

void *operator new[] (std::size_t n) throw (std::bad_alloc) {
  return dl::malloc_replace (n);
}

void *operator new[] (std::size_t n, const std::nothrow_t &) throw() {
  return dl::malloc_replace (n);
}

void operator delete (void *p) throw() {
  return dl::free_replace (p);
}

void operator delete (void *p, const std::nothrow_t &) throw() {
  return dl::free_replace (p);
}

void operator delete[] (void *p) throw() {
  return dl::free_replace (p);
}

void operator delete[] (void *p, const std::nothrow_t &) throw() {
  return dl::free_replace (p);
}
