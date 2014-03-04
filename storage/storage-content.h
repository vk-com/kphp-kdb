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
              2011-2013 Anton Maydell
*/

#ifndef __STORAGE_CONTENT_H__
#define __STORAGE_CONTENT_H__

enum ContentType {
  ct_jpeg = 0,
  ct_png = 1,
  ct_gif = 2,
  ct_pdf = 3,
  ct_mp3 = 4,
  ct_mov = 5,
  ct_partial = 6,
  ct_mp4 = 7,
  ct_webp = 8,
  ct_last = 9,
  ct_unknown = -1,
};

extern char *ContentTypes[];

int detect_content_type (const unsigned char *const buff, int size);
int ext_to_content_type (char ext[4]);
int content_type_to_file_type (enum ContentType content_type);
int file_type_to_content_type (int file_type);
#endif
