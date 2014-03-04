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

    Copyright 2011 Vkontakte Ltd
              2011 Nikolai Durov
              2011 Andrei Lopatin
*/

#ifndef __AHO_KMP_H__
#define	__AHO_KMP_H__

#define AHO_MAX_N 30
#define AHO_MAX_L 128
#define AHO_MAX_S 1024

extern int KA[AHO_MAX_S + 1], KB[AHO_MAX_S + 1];
extern char KS[AHO_MAX_S + 1];
extern int KN, KL;

int aho_prepare (int cnt, char *s[]);
int aho_search (char *str);

void aho_dump (void);

#endif
