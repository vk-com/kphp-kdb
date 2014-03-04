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

#ifndef __DIFF_PATCH_H__
#define __DIFF_PATCH_H__

#define DIFF_ERR_OUTPUT_BUFFER_OVERFLOW (-1)
#define DIFF_ERR_TIMEOUT (-2)
#define PATCH_ERR_HEADER_NOT_FOUND (-3)
#define PATCH_ERR_TEXTBUFF_TOO_BIG (-4)
#define PATCH_ERR_OLDPTR_OUT_OF_RANGE (-5)
#define PATCH_ERR_PATCHPTR_OUT_OF_RANGE (-6)
#define PATCH_ERR_OUTPUT_BUFFER_OVERFLOW (-7)
#define DIFF_ERR_MEMORY (-8)

/* on success returns number of bytes written to patch_buff */
int vk_diff (unsigned char *old_buff, int old_buff_size, unsigned char *new_buff, int new_buff_size, unsigned char *patch_buff, int patch_buff_size, int compress_level, double timeout);
int vk_patch (unsigned char *old_buff, int old_buff_size, unsigned char *patch_buff, int patch_buff_size, unsigned char *new_buff, int new_buff_size);
#endif

