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

    Copyright 2009-2013 Vkontakte Ltd
              2009-2013 Nikolai Durov
              2009-2013 Andrei Lopatin
*/

#ifndef __WORD_SPLIT_H__
#define __WORD_SPLIT_H__

#define	UTF8_TABLE_SIZE	0x800

extern int word_split_version, word_split_utf8;

extern unsigned char l_case[256];

void init_is_letter (void);
void enable_is_letter_sigils (void);
void enable_search_tag_sigil (void);
void init_letter_freq (void);

extern int (*sigil_word_detect)(int sigil, const char *str);

// returns length of word (in bytes) starting from pointer str
// get_word(): "word" = at most 127 alphanumeric characters, including at most 4 digits
// entities like "&#225;" or "&aacute;" are considered alphanumeric, but counted as several characters
// get_notword(): "not-a-word" = at most 4 non-alphanumeric entities ("&lt;" or "<br/>" is one entity), at most 31 bytes
int get_word (const char *str);
int get_notword (const char *str);
int get_word_utf8 (const char *str);
int get_notword_utf8 (const char *str);

// copy string & convert to lower case
// "to" must be able to keep len+1 bytes
void lc_str (char *to, const char *from, int len);
int get_str_class (const char *text, int len);
void lc_str_utf8 (char *to, const char *from, int len);
int get_str_class_utf8 (const char *text, int len);

unsigned long long word_hash (const char *str, int len);
unsigned long long word_crc64 (const char *str, int len);

#endif
