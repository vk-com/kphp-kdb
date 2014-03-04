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

    Copyright 2010-2011 Vkontakte Ltd
              2010-2011 Arseny Smirnov
              2010-2011 Aliaksei Levin
*/

#ifndef _STEMMER_H_
#define _STEMMER_H_

#include "word-split.h"

extern int stemmer_version;

//must be called once before other functions
void stem_init (void);

// return length of stemmed string in letters and necessity to delete penultimate letter
int stem_rus_win1251 (const char *s, int len, int *delete_penultimate_letter);
int stem_rus_utf8    (const char *s, int *delete_penultimate_letter);
int stem_rus_utf8i   (const  int *s, int *delete_penultimate_letter);

// return length of stemmed string in letters and write it into buffer
int stem_eng  (const char *s, char *res, int len);
int stem_engi (int *s);

extern int use_stemmer;
int my_lc_str (char *buff, const char *text, int len);

#endif
