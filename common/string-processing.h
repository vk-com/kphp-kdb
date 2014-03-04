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

    Copyright 2010-2012 Vkontakte Ltd
              2010-2012 Arseny Smirnov
              2010-2012 Aliaksei Levin
                   2012 Anton Timofeev
                   2012 Sergey Kopeliovich
*/

/**
 * Common string proccessing module.
 * Implementation moved from logs-engine.
 * Module prefix: 'sp' := 'string processing'
 *
 * Originally written:    Arseny Smirnov & Alexey Levin
 * Moved from log-engine: Anton Timofeev
 * Created: 01.04.2012
 */

#pragma once

// Max preallocated size
#define STRING_PROCESS_BUF_SIZE 1000000

extern int sp_errno;

/**
 * Simple memory management. Memory [pre]allocated from big static memory pool.
 * All memory allocating and deallocating - just pool pointer shifting.
 */

// Sets pool pointer to the begin. "frees" all allocated memory.
// Must be called before allocating functions.
inline void sp_init (void);

// Preallocates 'len + 1' bytes. In fact, doesn't allocates anything,
// just checks whether there is enough available memory.
char *sp_str_pre_alloc (int len);
// Allocates 'len' bytes.
char *sp_str_alloc (int len);

// Returns sorted s string.
char *sp_sort (char *s);

// Returns upper/lower case string for s in cp1251.
char *sp_to_upper (char *s);
char *sp_to_lower (char *s);

/**
 * Simplifications: look to source code to see full list of replacements.
 */

// Returns simplified s.
// Deletes all except digits, latin and russian letters in cp1251, lowercase letters.
char *sp_simplify (const char *s);

// Returns ultra-simplified s.
// Recognizes unicode characters encoded in cp1251 and html-entities. Remove diacritics
// from unicode characters, delete all except digits, latin and russian letters, lowercase
// letters. Unifies similar russian and english characters (i.e. ('n'|'ï') --> 'ï')
char *sp_full_simplify (const char *s);

// Converts all unicode characters encoded in cp1251 and html-entities into real cp1251,
// removing diacritics if possible. If converting is impossible - removes such characters.
char *sp_deunicode (char *s);
