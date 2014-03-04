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

    Copyright 2011 Vkontakte Ltd
              2011 Anton Maydell
*/

#ifndef __SPELL_DATA_H__
#define __SPELL_DATA_H__

extern int spellers;
extern int use_aspell_suggestion;
extern int yo_hack;
extern long long yo_hack_stat[2], check_word_stat[2];

void spell_init (void);
void spell_done (void);
int spell_get_dicts (char *buf, int len);
int spell_check (char *original_text, int res[3], int destroy_original);
void spell_reoder_spellers (void);

#endif
