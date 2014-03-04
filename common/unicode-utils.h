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
              2013 Arseny Smirnov
              2013 Aliaksei Levin
*/

#pragma once

#ifdef __cplusplus
extern "C" {
#endif

/* Convert character to upper case */
int unicode_toupper (int code);

/* Convert character to lower case */
int unicode_tolower (int code);

/* Convert character to title case */
int unicode_totitle (int code);

#ifdef __cplusplus
}
#endif
