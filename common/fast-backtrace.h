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
              2013 Aliaksei Levin
*/

#ifndef FAST_BACKTRACE_H__
#define FAST_BACKTRACE_H__

//#pragma GCC optimize("no-omit-frame-pointer")
//#define backtrace fast_backtrace

#ifdef __cplusplus
extern "C" {
#endif

int fast_backtrace (void **buffer, int size) __attribute__ ((noinline, optimize("no-omit-frame-pointer")));

#ifdef __cplusplus
}
#endif


#endif
