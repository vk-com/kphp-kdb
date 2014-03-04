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
              2011-2013 Vitaliy Valtman
*/

#ifndef __VKEXT_FLEX_H__
#define __VKEXT_FLEX_H__

#define CASE_NUMBER 8


struct node {
  short tail_len;
  short hyphen;
  int male_endings;
  int female_endings;
  int children_start;
  int children_end;
};

struct lang {
  const char *flexible_symbols;
  int names_start;
  int surnames_start;
  int cases_num;
  const int *children;
  const char **endings;
  struct node nodes[];
};

char *do_flex (const char *name, int name_len, const char *case_name, int case_name_len, int sex, const char *type, int type_len, int lang_id); 
#endif
