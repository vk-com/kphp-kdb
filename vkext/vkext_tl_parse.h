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
              2012-2013 Vitaliy Valtman
*/

#ifndef __VKEXT_TL_PARSE_H__
#define __VKEXT_TL_PARSE_H__

void tl_parse_init (void);
int tl_parse_int (void);
long long tl_parse_long (void);
double tl_parse_double (void);
int tl_parse_string (char **s);
int tl_eparse_string (char **s);
char *tl_parse_error (void);
void tl_set_error (const char *error);
void tl_parse_end (void);
int tl_parse_save_pos (void);
int tl_parse_restore_pos (int pos);
void tl_parse_clear_error (void);
#endif
