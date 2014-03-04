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

#include "mbstring.h"

#include "common/unicode-utils.h"
#include "common/utf8_utils.h"

static int mb_detect_encoding (const string &encoding) {
  if (strstr (encoding.c_str(), "1251")) {
    return 1251;
  }
  if (strstr (encoding.c_str(), "-8")) {
    return 8;
  }
  return -1;
}

static int mb_UTF8_strlen (const char *s) {
  int res = 0;
  for (int i = 0; s[i]; i++) {
    if ((((unsigned char)s[i]) & 0xc0) != 0x80) {
      res++;
    }
  }
  return res;
}

static int mb_UTF8_advance (const char *s, int cnt) {
  php_assert (cnt >= 0);
  int i;
  for (i = 0; s[i] && cnt >= 0; i++) {
    if ((((unsigned char)s[i]) & 0xc0) != 0x80) {
      cnt--;
    }
  }
  if (cnt < 0) {
    i--;
  }
  return i;
}

static int mb_UTF8_get_offset (const char *s, int pos) {
  int res = 0;
  for (int i = 0; i < pos && s[i]; i++) {
    if ((((unsigned char)s[i]) & 0xc0) != 0x80) {
      res++;
    }
  }
  return res;
}

bool mb_UTF8_check (const char *s) {
  do {
#define CHECK(condition) if (!(condition)) {return false;}
    unsigned int a = (unsigned char)(*s++);
    if ((a & 0x80) == 0) {
      if (a == 0) {
        return true;
      }
      continue;
    }

    CHECK ((a & 0x40) != 0);

    unsigned int b = (unsigned char)(*s++);
    CHECK((b & 0xc0) == 0x80);
    if ((a & 0x20) == 0) {
      CHECK((a & 0x1e) > 0);
      continue;
    }

    unsigned int c = (unsigned char)(*s++);
    CHECK((c & 0xc0) == 0x80);
    if ((a & 0x10) == 0) {
      int x = (((a & 0x0f) << 6) | (b & 0x20));
      CHECK(x != 0 && x != 0x360);//surrogates
      continue;
    }

    unsigned int d = (unsigned char)(*s++);
    CHECK((d & 0xc0) == 0x80);
    if ((a & 0x08) == 0) {
      int t = (((a & 0x07) << 6) | (b & 0x30));
      CHECK(0 < t && t < 0x110);//end of unicode
      continue;
    }

    return false;
#undef CHECK
  } while (1);

  php_assert (0);
} 

bool f$mb_check_encoding (const string &str, const string &encoding) {
  int encoding_num = mb_detect_encoding (encoding);
  if (encoding_num < 0) {
    php_critical_error ("encoding \"%s\" doesn't supported in mb_check_encoding", encoding.c_str());
    return str.size();
  }

  if (encoding_num == 1251) {
    return true;
  }

  return mb_UTF8_check (str.c_str());
}


int f$mb_strlen (const string &str, const string &encoding) {
  int encoding_num = mb_detect_encoding (encoding);
  if (encoding_num < 0) {
    php_critical_error ("encoding \"%s\" doesn't supported in mb_strlen", encoding.c_str());
    return str.size();
  }

  if (encoding_num == 1251) {
    return str.size();
  }

  return mb_UTF8_strlen (str.c_str());
}


string f$mb_strtolower (const string &str, const string &encoding) {
  int encoding_num = mb_detect_encoding (encoding);
  if (encoding_num < 0) {
    php_critical_error ("encoding \"%s\" doesn't supported in mb_strtolower", encoding.c_str());
    return str;
  }

  int len = str.size();
  if (encoding_num == 1251) {
    string res (len, false);
    for (int i = 0; i < len; i++) {
      switch ((unsigned char)str[i]) {
        case 'A' ... 'Z':
          res[i] = (char)(str[i] + 'a' - 'A');
          break;
        case 0xC0 ... 0xDF:
          res[i] = (char)(str[i] + 'à' - 'À');
          break;
        case 0x81:
          res[i] = (char)0x83;
          break;
        case 0xA3:
          res[i] = (char)0xBC;
          break;
        case 0xA5:
          res[i] = (char)0xB4;
          break;
        case 0xA1:
        case 0xB2:
        case 0xBD:
          res[i] = (char)(str[i] + 1);
          break;
        case 0x80:
        case 0x8A:
        case 0x8C ... 0x8F:
        case 0xA8:
        case 0xAA:
        case 0xAF:
          res[i] = (char)(str[i] + 16);
          break;
        default:
          res[i] = str[i];
      }
    }

    return res;
  } else {
    string res (len * 3, false);
    const char *s = str.c_str();
    int res_len = 0;
    int p;
    unsigned int ch;
    while ((p = get_char_utf8 (&ch, s)) > 0) {
      s += p;
      res_len += put_char_utf8 (unicode_tolower (ch), &res[res_len]);
    }
    if (p < 0) {
      php_warning ("Incorrect UTF-8 string \"%s\" in function mb_strtolower", str.c_str());
    }
    res.shrink (res_len);

    return res;
  }
}

string f$mb_strtoupper (const string &str, const string &encoding) {
  int encoding_num = mb_detect_encoding (encoding);
  if (encoding_num < 0) {
    php_critical_error ("encoding \"%s\" doesn't supported in mb_strtoupper", encoding.c_str());
    return str;
  }

  int len = str.size();
  if (encoding_num == 1251) {
    string res (len, false);
    for (int i = 0; i < len; i++) {
      switch ((unsigned char)str[i]) {
        case 'a' ... 'z':
          res[i] = (char)(str[i] + 'A' - 'a');
          break;
        case 0xE0 ... 0xFF:
          res[i] = (char)(str[i] + 'À' - 'à');
          break;
        case 0x83:
          res[i] = (char)(0x81);
          break;
        case 0xBC:
          res[i] = (char)(0xA3);
          break;
        case 0xB4:
          res[i] = (char)(0xA5);
          break;
        case 0xA2:
        case 0xB3:
        case 0xBE:
          res[i] = (char)(str[i] - 1);
          break;
        case 0x98:
        case 0xA0:
        case 0xAD:
          res[i] = ' ';
          break;
        case 0x90:
        case 0x9A:
        case 0x9C ... 0x9F:
        case 0xB8:
        case 0xBA:
        case 0xBF:
          res[i] = (char)(str[i] - 16);
          break;
        default:
          res[i] = str[i];
      }
    }

    return res;
  } else {
    string res (len * 3, false);
    const char *s = str.c_str();
    int res_len = 0;
    int p;
    unsigned int ch;
    while ((p = get_char_utf8 (&ch, s)) > 0) {
      s += p;
      res_len += put_char_utf8 (unicode_toupper (ch), &res[res_len]);
    }
    if (p < 0) {
      php_warning ("Incorrect UTF-8 string \"%s\" in function mb_strtoupper", str.c_str());
    }
    res.shrink (res_len);

    return res;
  }
}

OrFalse <int> f$mb_strpos (const string &haystack, const string &needle, int offset, const string &encoding) {
  if (offset < 0) {
    php_warning ("Wrong offset = %d in function mb_strpos", offset);
    return false;
  }
  if ((int)needle.size() == 0) {
    php_warning ("Parameter needle is empty in function mb_strpos");
    return false;
  }

  int encoding_num = mb_detect_encoding (encoding);
  if (encoding_num < 0) {
    php_critical_error ("encoding \"%s\" doesn't supported in mb_strpos", encoding.c_str());
    return false;
  }

  if (encoding_num == 1251) {
    return f$strpos (haystack, needle, offset);
  }

  int UTF8_offset = mb_UTF8_advance (haystack.c_str(), offset);
  const char *s = (const char *)memmem (haystack.c_str() + UTF8_offset, haystack.size() - UTF8_offset, needle.c_str(), needle.size());
  if (s == NULL) {
    return false;
  }
  return mb_UTF8_get_offset (haystack.c_str() + UTF8_offset, (dl::size_type)(s - (haystack.c_str() + UTF8_offset))) + offset;
}

OrFalse <int> f$mb_stripos (const string &haystack, const string &needle, int offset, const string &encoding) {
  int encoding_num = mb_detect_encoding (encoding);
  if (encoding_num < 0) {
    php_critical_error ("encoding \"%s\" doesn't supported in mb_stripos", encoding.c_str());
    return false;
  }

  return f$mb_strpos (f$mb_strtolower (haystack, encoding), f$mb_strtolower (needle, encoding), offset, encoding);
}

string f$mb_substr (const string &str, int start, int length, const string &encoding) {
  int encoding_num = mb_detect_encoding (encoding);
  if (encoding_num < 0) {
    php_critical_error ("encoding \"%s\" doesn't supported in mb_substr", encoding.c_str());
    return str;
  }

  if (encoding_num == 1251) {
    return f$substr (str, start, length);
  }

  int len = mb_UTF8_strlen (str.c_str());
  if (start < 0) {
    start += len;
  }
  if (start > len) {
    start = len;
  }
  if (length < 0) {
    length = len - start + length;
  }
  if (length <= 0 || start < 0) {
    return string();
  }
  if (len - start < length) {
    length = len - start;
  }

  int UTF8_start  = mb_UTF8_advance (str.c_str(), start);
  int UTF8_length = mb_UTF8_advance (str.c_str() + UTF8_start, length);

  return string (str.c_str() + UTF8_start, UTF8_length);
}
