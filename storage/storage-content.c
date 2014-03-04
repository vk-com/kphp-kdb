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

#define _FILE_OFFSET_BITS 64

/* http://en.wikipedia.org/wiki/List_of_file_signatures */

#include <string.h>
#include "storage-content.h"
#include "TL/constants.h"

char *ContentTypes[] = {
  "image/jpeg",
  "image/png",
  "image/gif",
  "application/pdf",
  "audio/mpeg",
  "video/quicktime",
  "application/octet-stream",
  "video/mp4",
  "image/webp",
};

int detect_content_type (const unsigned char *const buff, int size) {
  const unsigned int *a = (const unsigned int *) buff;
  switch (buff[0]) {
    case 0:
      if (size >= 12) {
        if (a[0] == 0x14000000 && a[1] == 0x70797466 && a[2] == 0x20207471) {
          return ct_mov;
        }
        if (!(a[0] & 0x00ffffff) && a[1] == 0x70797466 && (a[2] & 0x00ffffff) == 0x0034706d) {
          return ct_mp4;
        }
      }
      break;
    case '%':
      if (size >= 4 && !memcmp (&buff[1], "PDF", 3)) {
        return ct_pdf;
      }
      break;
    case 'I':
      if (size >= 3 && !memcmp (&buff[1], "D3", 2)) {
        return ct_mp3;
      }
      break;
    case 'G':
      if (size >= 5 && !memcmp (&buff[1], "IF", 2)) {
        return ct_gif;
      }
      break;
    case 'R':
      if (size >= 4 && !memcmp (&buff[1], "IFF", 3)) {
        if (size >= 12 && !memcmp (&buff[8], "WEBP", 4)) {
          return ct_webp;
        }
        return ct_mp3;
      }
      break;
    case 0x89:
      if (size >= 4 && !memcmp (&buff[1], "PNG", 3)) {
        return ct_png;
      }
      break;
    case 0xff:
      if (size >= 2 && buff[1] == 0xd8) {
        return ct_jpeg;
      }
      if (size >= 2 && buff[1] >= 0xe0) {
        return ct_mp3;
      }
      break;
  }
  return ct_unknown;
}

int ext_to_content_type (char ext[4]) {
  int r = ext[0];
  r <<= 8;
  r |= ext[1];
  r <<= 8;
  r |= ext[2];
  r |= 0x202020; /* lowercase */
  switch (r) {
    case 0x62696e: return ct_partial; /* bin */
    case 0x676966: return ct_gif;
    case 0x6a7067: return ct_jpeg;
    case 0x6d7033: return ct_mp3;
    case 0x6d7034: return ct_mp4;
    case 0x6d6f76: return ct_mov;
    case 0x706466: return ct_pdf;
    case 0x706e67: return ct_png;
    case 0x776270: return ct_webp; /* wbp */
  }
  return ct_unknown;
}

int content_type_to_file_type (enum ContentType content_type) {
  switch (content_type) {
    case ct_unknown: return TL_STORAGE_FILE_UNKNOWN;
    case ct_jpeg: return TL_STORAGE_FILE_JPEG;
    case ct_gif: return TL_STORAGE_FILE_GIF;
    case ct_png: return TL_STORAGE_FILE_PNG;
    case ct_pdf: return TL_STORAGE_FILE_PDF;
    case ct_mp3: return TL_STORAGE_FILE_MP3;
    case ct_mov: return TL_STORAGE_FILE_MOV;
    case ct_partial: return TL_STORAGE_FILE_PARTIAL;
    case ct_mp4: return TL_STORAGE_FILE_MP4;
    case ct_webp: return TL_STORAGE_FILE_WEBP;
    case ct_last: break;
  }
  return 0;
}

int file_type_to_content_type (int file_type) {
  switch (file_type) {
    case TL_STORAGE_FILE_UNKNOWN: return ct_unknown;
    case TL_STORAGE_FILE_JPEG: return ct_jpeg;
    case TL_STORAGE_FILE_GIF: return ct_gif;
    case TL_STORAGE_FILE_PNG: return ct_png;
    case TL_STORAGE_FILE_PDF: return ct_pdf;
    case TL_STORAGE_FILE_MP3: return ct_mp3;
    case TL_STORAGE_FILE_MOV: return ct_mov;
    case TL_STORAGE_FILE_MP4: return ct_mp4;
    case TL_STORAGE_FILE_PARTIAL: return ct_partial;
    case TL_STORAGE_FILE_WEBP: return ct_webp;
  }
  return ct_last | 0x40000000;
}
