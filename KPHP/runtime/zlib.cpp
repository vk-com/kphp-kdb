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
              2012-2013 Arseny Smirnov
              2012-2013 Aliaksei Levin
*/

#define _FILE_OFFSET_BITS 64

#include <zlib.h>

#include "zlib.h"

#include "string_functions.h"//php_buf, TODO

static voidpf zlib_alloc (voidpf opaque, uInt items, uInt size) {
  int *buf_pos = (int *)opaque;
  php_assert (items != 0 && (PHP_BUF_LEN - *buf_pos) / items >= size);

  int pos = *buf_pos;
  *buf_pos += items * size;
  php_assert (*buf_pos <= PHP_BUF_LEN);
  return php_buf + pos;
}

static void zlib_free (voidpf opaque, voidpf address) {
}

const string_buffer *zlib_encode (const char *s, int s_len, int level, int encoding) {
  int buf_pos = 0;
  z_stream strm;
  strm.zalloc = zlib_alloc;
  strm.zfree = zlib_free;
  strm.opaque = &buf_pos;

  unsigned int res_len = (unsigned int)compressBound (s_len) + 30;
  static_SB.clean().reserve (res_len);

  dl::enter_critical_section();//OK
  int ret = deflateInit2 (&strm, level, Z_DEFLATED, encoding, MAX_MEM_LEVEL, Z_DEFAULT_STRATEGY);
  if (ret == Z_OK) {
    strm.avail_in = (unsigned int)s_len;
    strm.next_in = (Bytef *)s;
    strm.avail_out = res_len;
    strm.next_out = (Bytef *)static_SB.buffer();

    ret = deflate (&strm, Z_FINISH);
    deflateEnd (&strm);

    if (ret == Z_STREAM_END) {
      dl::leave_critical_section();

      static_SB.set_pos ((int)strm.total_out);
      return &static_SB;
    }
  }
  dl::leave_critical_section();

  php_warning ("Error during pack of string with length %d", s_len);

  static_SB.clean();
  return &static_SB;
}

string f$gzcompress (const string &s, int level) {
  if (level < -1 || level > 9) {
    php_warning ("Wrong parameter level = %d in function gzcompress", level);
    level = 6;
  }

  return zlib_encode (s.c_str(), s.size(), level, ZLIB_COMPRESS)->str();
}

string f$gzuncompress (const string &s) {
  unsigned long res_len = PHP_BUF_LEN;

  dl::enter_critical_section();//OK
  if (uncompress (reinterpret_cast <unsigned char *> (php_buf), &res_len, reinterpret_cast <const unsigned char *> (s.c_str()), (unsigned long)s.size()) == Z_OK) {
    dl::leave_critical_section();
    return string (php_buf, (dl::size_type)res_len);
  }
  dl::leave_critical_section();

  php_warning ("Error during unpack of string of length %d", (int)s.size());
  return string();
}

string f$gzencode (const string &s, int level) {
  if (level < -1 || level > 9) {
    php_warning ("Wrong parameter level = %d in function gzencode", level);
  }

  return zlib_encode (s.c_str(), s.size(), level, ZLIB_ENCODE)->str();
}

string f$gzdecode (const string &s) {
  dl::enter_critical_section();//OK

  z_stream strm;
  strm.zalloc = Z_NULL;
  strm.zfree = Z_NULL;
  strm.opaque = Z_NULL;
  strm.avail_in = s.size();
  strm.next_in = (Bytef *)s.c_str();
  strm.avail_out = PHP_BUF_LEN;
  strm.next_out = (Bytef *)php_buf;

  int ret = inflateInit2 (&strm, 15 + 32);
  if (ret != Z_OK) {
    dl::leave_critical_section();
    return string();
  }

  ret = inflate (&strm, Z_NO_FLUSH);
  switch (ret) {
    case Z_NEED_DICT:
    case Z_DATA_ERROR:
    case Z_MEM_ERROR:
    case Z_STREAM_ERROR:
      inflateEnd (&strm);
      dl::leave_critical_section();

      php_assert (ret != Z_STREAM_ERROR);
      return string();
  }

  int res_len = PHP_BUF_LEN - strm.avail_out;

  if (strm.avail_out == 0 && ret != Z_STREAM_END) {
    inflateEnd (&strm);
    dl::leave_critical_section();

    php_critical_error ("size of unpacked data is greater then %d. Can't decode.", PHP_BUF_LEN);
    return string();
  }

  inflateEnd (&strm);
  dl::leave_critical_section();
  return string (php_buf, res_len);
}

