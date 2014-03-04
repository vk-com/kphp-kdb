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

#pragma once

#include <cstdlib>

namespace dl {

extern volatile int in_critical_section;
extern volatile long long pending_signals;

void enter_critical_section (void);
void leave_critical_section (void);


extern long long query_num;//engine query number. query_num == 0 before first query
extern bool script_runned;//betwen init_static and free_static
extern bool use_script_allocator;//use script allocator instead of static heap allocator

typedef unsigned int size_type;

extern size_t memory_begin;//begin of script memory arena
extern size_t memory_end;//end of script memory arena
extern size_type memory_limit;//size of script memory arena
extern size_type memory_used;//currently used script memory
extern size_type max_memory_used;//maxumum of used script memory
extern size_type max_real_memory_used;//maxumum of used and dirty script memory

extern size_type static_memory_used;

size_type memory_get_total_usage (void);//usage of script memory

void allocator_init (void *buf, size_type n);//init script allocator with arena of n bytes at buf

void *allocate (size_type n);//allocate script memory
void *allocate0 (size_type n);//allocate zeroed script memory
void *reallocate (void *p, size_type n, size_type old_n);//reallocate script memory
void deallocate (void *p, size_type n);//deallocate script memory

void *static_allocate (size_type n);//allocate heap memory (persistent between script runs)
void *static_allocate0 (size_type n);//allocate zeroed heap memory
void static_reallocate (void **p, size_type new_n, size_type *n);//reallocate heap memory
void static_deallocate (void **p, size_type *n);//deallocate heap memory

void *malloc_replace (size_t x);
void free_replace (void *p);

};
