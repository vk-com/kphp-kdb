/*
    This file is part of VK/KittenDB-Engine.

    VK/KittenDB-Engine is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 2 of the License, or
    (at your option) any later version.

    VK/KittenDB-Engine is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with VK/KittenDB-Engine.  If not, see <http://www.gnu.org/licenses/>.

    This program is released under the GPL with the additional exemption 
    that compiling, linking, and/or using OpenSSL is allowed.
    You are free to remove this exemption from derived works.

    Copyright 2012-2013 Vkontakte Ltd
              2012-2013 Arseny Smirnov
              2012-2013 Aliaksei Levin
*/

#define _FILE_OFFSET_BITS 64

#include "string_functions.h"

#include <clocale>

#include "interface.h"

const string COLON (",", 1);
const string CP1251 ("1251", 4);
const string DOT (".", 1);
const string NEW_LINE ("\n", 1);
const string SPACE (" ", 1);
const string WHAT (" \n\r\t\v\0", 6);

static const string ONE ("1", 1);
static const string PERCENT ("%", 1);

char php_buf[PHP_BUF_LEN + 1];

const char lhex_digits[17] = "0123456789abcdef";
const char uhex_digits[17] = "0123456789ABCDEF";

int str_replace_count_dummy;

static inline const char *get_mask (const string &what) {
  static char mask[256];
  memset (mask, 0, 256);

  int len = what.size();
  for (int i = 0; i < len; i++) {
    unsigned char c = what[i];
    if (what[i + 1] == '.' && what[i + 2] == '.' && (unsigned char)what[i + 3] >= c) {
      memset (mask + c, 1, (unsigned char)what[i + 3] - c + 1);
      i += 3;
    } else if (c == '.' && what[i + 1] == '.') {
      php_warning ("Invalid '..'-range in string \"%s\" at position %d.", what.c_str(), i);
    } else {
      mask[c] = 1;
    }
  }

  return mask;
}

string f$addcslashes (const string &str, const string &what) {
  const char *mask = get_mask (what);

  int len = str.size();
  static_SB.clean().reserve (4 * len);

  for (int i = 0; i < len; i++) {
    unsigned char c = str[i];
    if (mask[c]) {
      static_SB.append_char ('\\');
      if (c < 32 || c > 126) {
        switch (c) {
          case '\n':
            static_SB.append_char ('n');
            break;
          case '\t':
            static_SB.append_char ('t');
            break;
          case '\r':
            static_SB.append_char ('r');
            break;
          case '\a':
            static_SB.append_char ('a');
            break;
          case '\v':
            static_SB.append_char ('v');
            break;
          case '\b':
            static_SB.append_char ('b');
            break;
          case '\f':
            static_SB.append_char ('f');
            break;
          default:
            static_SB.append_char (((c >> 6) + '0'));
            static_SB.append_char ((((c >> 3) & 7) + '0'));
            static_SB.append_char (((c & 7) + '0'));
        }
      } else {
        static_SB.append_char (c);
      }
    } else {
      static_SB.append_char (c);
    }
  }
  return static_SB.str();
}

string f$addslashes (const string &str) {
  int len = str.size();

  static_SB.clean().reserve (2 * len);
  for (int i = 0; i < len; i++) {
    switch (str[i]) {
      case '\0':
        static_SB.append_char ('\\');
        static_SB.append_char ('0');
        break;
      case '\'':
      case '\"':
      case '\\':
        static_SB.append_char ('\\');
      default:
        static_SB.append_char (str[i]);
    }
  }
  return static_SB.str();
}

string f$bin2hex (const string &str) {
  int len = str.size();
  string result (2 * len, false);

  for (int i = 0; i < len; i++) {
    result[2 * i] = lhex_digits[(str[i] >> 4) & 15];
    result[2 * i + 1] = lhex_digits[str[i] & 15];
  }

  return result;
}

string f$chop (const string &s, const string &what) {
  return f$rtrim (s, what);
}

string f$chr (int v) {
  return string (1, (char)v);
}

var f$count_chars (const string &str, int mode) {
  int chars[256] = {0};

  if ((unsigned int)mode > 4u) {
    php_warning ("Unknown mode %d", mode);
    return false;
  }

  int len = str.size();
  for (int i = 0; i < len; i++) {
    chars[(unsigned char)str[i]]++;
  }

  if (mode <= 2) {
    array <var> result;
    for (int i = 0; i < 256; i++) {
      if ((mode != 2 && chars[i] != 0) ||
          (mode != 1 && chars[i] == 0)) {
        result.set_value (i, chars[i]);
      }
    }
    return result;
  }

  string result;
  for (int i = 0; i < 256; i++) {
    if ((mode == 3) == (chars[i] != 0)) {
      result.push_back (char (i));
    }
  }
  return result;
}

string f$hex2bin (const string &str) {
  int len = str.size();
  if (len & 1) {
    php_warning ("Wrong argument \"%s\" supplied for function hex2bin", str.c_str());
    return string();
  }

  string result (len, false);
  for (int i = 0; i < len; i += 2) {
    int num_high = hex_to_int (str[i]);
    int num_low = hex_to_int (str[i + 1]);
    if (num_high == 16 || num_low == 16) {
      php_warning ("Wrong argument \"%s\" supplied for function hex2bin", str.c_str());
      return string();
    }
    result[i] = (char)((num_high << 4) + num_low);
  }

  return result;
}

static const int entities_size = 251;

static const char *ent_to_num_s[entities_size] = {
  "AElig", "Aacute", "Acirc", "Agrave", "Alpha", "Aring", "Atilde", "Auml", "Beta", "Ccedil",
  "Chi", "Dagger", "Delta", "ETH", "Eacute", "Ecirc", "Egrave", "Epsilon", "Eta", "Euml",
  "Gamma", "Iacute", "Icirc", "Igrave", "Iota", "Iuml", "Kappa", "Lambda", "Mu", "Ntilde",
  "Nu", "OElig", "Oacute", "Ocirc", "Ograve", "Omega", "Omicron", "Oslash", "Otilde", "Ouml",
  "Phi", "Pi", "Prime", "Psi", "Rho", "Scaron", "Sigma", "THORN", "Tau", "Theta",
  "Uacute", "Ucirc", "Ugrave", "Upsilon", "Uuml", "Xi", "Yacute", "Yuml", "Zeta", "aacute",
  "acirc", "acute", "aelig", "agrave", "alefsym", "alpha", "amp", "and", "ang", "aring",
  "asymp", "atilde", "auml", "bdquo", "beta", "brvbar", "bull", "cap", "ccedil", "cedil",
  "cent", "chi", "circ", "clubs", "cong", "copy", "crarr", "cup", "curren", "dArr",
  "dagger", "darr", "deg", "delta", "diams", "divide", "eacute", "ecirc", "egrave", "empty",
  "emsp", "ensp", "epsilon", "equiv", "eta", "eth", "euml", "euro", "exist", "fnof",
  "forall", "frac12", "frac14", "frac34", "frasl", "gamma", "ge", "gt", "hArr", "harr",
  "hearts", "hellip", "iacute", "icirc", "iexcl", "igrave", "image", "infin", "int", "iota",
  "iquest", "isin", "iuml", "kappa", "lArr", "lambda", "lang", "laquo", "larr", "lceil",
  "ldquo", "le", "lfloor", "lowast", "loz", "lrm", "lsaquo", "lsquo", "lt", "macr",
  "mdash", "micro", "middot", "minus", "mu", "nabla", "nbsp", "ndash", "ne", "ni",
  "not", "notin", "nsub", "ntilde", "nu", "oacute", "ocirc", "oelig", "ograve", "oline",
  "omega", "omicron", "oplus", "or", "ordf", "ordm", "oslash", "otilde", "otimes", "ouml",
  "para", "part", "permil", "perp", "phi", "pi", "piv", "plusmn", "pound", "prime",
  "prod", "prop", "psi", "rArr", "radic", "rang", "raquo", "rarr", "rceil",
  "rdquo", "real", "reg", "rfloor", "rho", "rlm", "rsaquo", "rsquo", "sbquo", "scaron",
  "sdot", "sect", "shy", "sigma", "sigmaf", "sim", "spades", "sub", "sube", "sum",
  "sup", "sup1", "sup2", "sup3", "supe", "szlig", "tau", "there4", "theta", "thetasym",
  "thinsp", "thorn", "tilde", "times", "trade", "uArr", "uacute", "uarr", "ucirc", "ugrave",
  "uml", "upsih", "upsilon", "uuml", "weierp", "xi", "yacute", "yen", "yuml", "zeta",
  "zwj", "zwnj"};

static int ent_to_num_i[entities_size] = {
  198, 193, 194, 192, 913, 197, 195, 196, 914, 199, 935, 8225, 916, 208, 201, 202, 200, 917, 919, 203,
  915, 205, 206, 204, 921, 207, 922, 923, 924, 209, 925, 338, 211, 212, 210, 937, 927, 216, 213, 214,
  934, 928, 8243, 936, 929, 352, 931, 222, 932, 920, 218, 219, 217, 933, 220, 926, 221, 376, 918, 225,
  226, 180, 230, 224, 8501, 945, 38, 8743, 8736, 229, 8776, 227, 228, 8222, 946, 166, 8226, 8745, 231, 184,
  162, 967, 710, 9827, 8773, 169, 8629, 8746, 164, 8659, 8224, 8595, 176, 948, 9830, 247, 233, 234, 232, 8709,
  8195, 8194, 949, 8801, 951, 240, 235, 8364, 8707, 402, 8704, 189, 188, 190, 8260, 947, 8805, 62, 8660, 8596,
  9829, 8230, 237, 238, 161, 236, 8465, 8734, 8747, 953, 191, 8712, 239, 954, 8656, 955, 9001, 171, 8592, 8968,
  8220, 8804, 8970, 8727, 9674, 8206, 8249, 8216, 60, 175, 8212, 181, 183, 8722, 956, 8711, 160, 8211, 8800, 8715,
  172, 8713, 8836, 241, 957, 243, 244, 339, 242, 8254, 969, 959, 8853, 8744, 170, 186, 248, 245, 8855, 246,
  182, 8706, 8240, 8869, 966, 960, 982, 177, 163, 8242, 8719, 8733, 968, 8658, 8730, 9002, 187, 8594, 8969,
  8221, 8476, 174, 8971, 961, 8207, 8250, 8217, 8218, 353, 8901, 167, 173, 963, 962, 8764, 9824, 8834, 8838, 8721,
  8835, 185, 178, 179, 8839, 223, 964, 8756, 952, 977, 8201, 254, 732, 215, 8482, 8657, 250, 8593, 251, 249,
  168, 978, 965, 252, 8472, 958, 253, 165, 255, 950, 8205, 8204};
/*
static int cp1251_to_utf8[128] = {
  0x402, 0x403,  0x201A, 0x453,  0x201E, 0x2026, 0x2020, 0x2021, 0x20AC, 0x2030, 0x409,  0x2039, 0x40A, 0x40C, 0x40B, 0x40F,
  0x452, 0x2018, 0x2019, 0x201C, 0x201D, 0x2022, 0x2013, 0x2014, 0x0,    0x2122, 0x459,  0x203A, 0x45A, 0x45C, 0x45B, 0x45F,
  0xA0,  0x40E,  0x45E,  0x408,  0xA4,   0x490,  0xA6,   0xA7,   0x401,  0xA9,   0x404,  0xAB,   0xAC,  0xAD,  0xAE,  0x407,
  0xB0,  0xB1,   0x406,  0x456,  0x491,  0xB5,   0xB6,   0xB7,   0x451,  0x2116, 0x454,  0xBB,   0x458, 0x405, 0x455, 0x457,
  0x410, 0x411,  0x412,  0x413,  0x414,  0x415,  0x416,  0x417,  0x418,  0x419,  0x41A,  0x41B,  0x41C, 0x41D, 0x41E, 0x41F,
  0x420, 0x421,  0x422,  0x423,  0x424,  0x425,  0x426,  0x427,  0x428,  0x429,  0x42A,  0x42B,  0x42C, 0x42D, 0x42E, 0x42F,
  0x430, 0x431,  0x432,  0x433,  0x434,  0x435,  0x436,  0x437,  0x438,  0x439,  0x43A,  0x43B,  0x43C, 0x43D, 0x43E, 0x43F,
  0x440, 0x441,  0x442,  0x443,  0x444,  0x445,  0x446,  0x447,  0x448,  0x449,  0x44A,  0x44B,  0x44C, 0x44D, 0x44E, 0x44F};
*/
static const char *cp1251_to_utf8_str[128] = {
  "&#1026;", "&#1027;",  "&#8218;", "&#1107;", "&#8222;",  "&hellip;", "&dagger;", "&Dagger;", "&euro;",  "&permil;", "&#1033;", "&#8249;", "&#1034;", "&#1036;", "&#1035;", "&#1039;",
  "&#1106;", "&#8216;",  "&#8217;", "&#8219;", "&#8220;",  "&bull;",   "&ndash;",  "&mdash;",  "˜",       "&trade;",  "&#1113;", "&#8250;", "&#1114;", "&#1116;", "&#1115;", "&#1119;",
  "&nbsp;",  "&#1038;",  "&#1118;", "&#1032;", "&curren;", "&#1168;",  "&brvbar;", "&sect;",   "&#1025;", "&copy;",   "&#1028;", "&laquo;", "&not;",   "&shy;",   "&reg;",   "&#1031;",
  "&deg;",   "&plusmn;", "&#1030;", "&#1110;", "&#1169;",  "&micro;",  "&para;",   "&middot;", "&#1105;", "&#8470;",  "&#1108;", "&raquo;", "&#1112;", "&#1029;", "&#1109;", "&#1111;",
  "&#1040;", "&#1041;",  "&#1042;", "&#1043;", "&#1044;",  "&#1045;",  "&#1046;",  "&#1047;",  "&#1048;", "&#1049;",  "&#1050;", "&#1051;", "&#1052;", "&#1053;", "&#1054;", "&#1055;",
  "&#1056;", "&#1057;",  "&#1058;", "&#1059;", "&#1060;",  "&#1061;",  "&#1062;",  "&#1063;",  "&#1064;", "&#1065;",  "&#1066;", "&#1067;", "&#1068;", "&#1069;", "&#1070;", "&#1071;",
  "&#1072;", "&#1073;",  "&#1074;", "&#1075;", "&#1076;",  "&#1077;",  "&#1078;",  "&#1079;",  "&#1080;", "&#1081;",  "&#1082;", "&#1083;", "&#1084;", "&#1085;", "&#1086;", "&#1087;",
  "&#1088;", "&#1089;",  "&#1090;", "&#1091;", "&#1092;",  "&#1093;",  "&#1094;",  "&#1095;",  "&#1096;", "&#1097;",  "&#1098;", "&#1099;", "&#1100;", "&#1101;", "&#1102;", "&#1103;"};

string f$htmlentities (const string &str) {
  int len = (int)str.size();
  static_SB.clean().reserve (6 * len);

  for (int i = 0; i < len; i++) {
    switch (str[i]) {
      case '&':
        static_SB.append_char ('&');
        static_SB.append_char ('a');
        static_SB.append_char ('m');
        static_SB.append_char ('p');
        static_SB.append_char (';');
        break;
      case '"':
        static_SB.append_char ('&');
        static_SB.append_char ('q');
        static_SB.append_char ('u');
        static_SB.append_char ('o');
        static_SB.append_char ('t');
        static_SB.append_char (';');
        break;
      case '<':
        static_SB.append_char ('&');
        static_SB.append_char ('l');
        static_SB.append_char ('t');
        static_SB.append_char (';');
        break;
      case '>':
        static_SB.append_char ('&');
        static_SB.append_char ('g');
        static_SB.append_char ('t');
        static_SB.append_char (';');
        break;
      default:
        if (str[i] < 0) {
          const char *utf8_str = cp1251_to_utf8_str[128 + str[i]];
          static_SB.append_unsafe (utf8_str, strlen (utf8_str));
        } else {
          static_SB.append_char (str[i]);
        }
    }
  }

  return static_SB.str();
}

string f$html_entity_decode (const string &str, int flags, const string &encoding) {
  if (flags >= 3) {
    php_critical_error ("unsupported parameter flags = %d in function html_entity_decode", flags);
  }

  bool utf8 = memchr (encoding.c_str(), '8', encoding.size()) != NULL;
  if (!utf8 && strstr (encoding.c_str(), "1251") == NULL) {
    php_critical_error ("unsupported encoding \"%s\" in function html_entity_decode", encoding.c_str());
    return str;
  }

  int len = str.size();
  string res (len * 7 / 4 + 4, false);
  char *p = &res[0];
  for (int i = 0; i < len; i++) {
    if (str[i] == '&') {
      int j = i + 1;
      while (j < len && str[j] != ';') {
        j++;
      }
      if (j < len) {
        if ((flags & ENT_QUOTES) && j == i + 5) {
          if (str[i + 1] == '#' && str[i + 2] == '0' && str[i + 3] == '3' && str[i + 4] == '9') {
            i += 5;
            *p++ = '\'';
            continue;
          }
        }
        if (!(flags & ENT_NOQUOTES) && j == i + 5) {
          if (str[i + 1] == 'q' && str[i + 2] == 'u' && str[i + 3] == 'o' && str[i + 4] == 't') {
            i += 5;
            *p++ = '\"';
            continue;
          }
        }

        int l = 0, r = entities_size;
        while (l + 1 < r) {
          int m = (l + r) >> 1;
          if (strncmp (str.c_str() + i + 1, ent_to_num_s[m], j - i - 1) < 0) {
            r = m;
          } else {
            l = m;
          }
        }
        if (strncmp (str.c_str() + i + 1, ent_to_num_s[l], j - i - 1) == 0) {
          int num = ent_to_num_i[l];
          i = j;
          if (utf8) {
            if (num < 128) {
              *p++ = (char)num;
            } else if (num < 0x800) {
              *p++ = (char)(0xc0 + (num >> 6));
              *p++ = (char)(0x80 + (num & 63));
            } else {
              *p++ = (char)(0xe0 + (num >> 12));
              *p++ = (char)(0x80 + ((num >> 6) & 63));
              *p++ = (char)(0x80 + (num & 63));
            }
          } else {
            if (num < 128) {
              *p++ = (char)num;
            } else {
              *p++ = '&';
              *p++ = '#';
              if (num >= 1000) {
                *p++ = (char)(num / 1000 % 10 + '0');
              }
              if (num >= 100) {
                *p++ = (char)(num / 100 % 10 + '0');
              }
              if (num >= 10) {
                *p++ = (char)(num / 10 % 10 + '0');
              }
              *p++ = (char)(num % 10 + '0');
              *p++ = ';';
            }
          }
          continue;
        }
      }
    }

    *p++ = str[i];
  }
  res.shrink ((dl::size_type)(p - res.c_str()));

  return res;
}

string f$htmlspecialchars (const string &str, int flags) {
  if (flags >= 3) {
    php_critical_error ("unsupported parameter flags = %d in function htmlspecialchars", flags);
  }

  int len = (int)str.size();
  static_SB.clean().reserve (6 * len);

  for (int i = 0; i < len; i++) {
    switch (str[i]) {
      case '&':
        static_SB.append_char ('&');
        static_SB.append_char ('a');
        static_SB.append_char ('m');
        static_SB.append_char ('p');
        static_SB.append_char (';');
        break;
      case '"':
        if (!(flags & ENT_NOQUOTES)) {
          static_SB.append_char ('&');
          static_SB.append_char ('q');
          static_SB.append_char ('u');
          static_SB.append_char ('o');
          static_SB.append_char ('t');
          static_SB.append_char (';');
        } else {
          static_SB.append_char ('"');
        }
        break;
      case '\'':
        if (flags & ENT_QUOTES) {
          static_SB.append_char ('&');
          static_SB.append_char ('#');
          static_SB.append_char ('0');
          static_SB.append_char ('3');
          static_SB.append_char ('9');
          static_SB.append_char (';');
        } else {
          static_SB.append_char ('\'');
        }
        break;
      case '<':
        static_SB.append_char ('&');
        static_SB.append_char ('l');
        static_SB.append_char ('t');
        static_SB.append_char (';');
        break;
      case '>':
        static_SB.append_char ('&');
        static_SB.append_char ('g');
        static_SB.append_char ('t');
        static_SB.append_char (';');
        break;
      default:
        static_SB.append_char (str[i]);
    }
  }

  return static_SB.str();
}

string f$htmlspecialchars_decode (const string &str, int flags) {
  if (flags >= 3) {
    php_critical_error ("unsupported parameter flags = %d in function htmlspecialchars_decode", flags);
  }

  int len = str.size();
  string res (len, false);
  char *p = &res[0];
  for (int i = 0; i < len; ) {
    if (str[i] == '&') {
      if (str[i + 1] == 'a' && str[i + 2] == 'm' && str[i + 3] == 'p' && str[i + 4] == ';') {
        *p++ = '&';
        i += 5;
      } else if (str[i + 1] == 'q' && str[i + 2] == 'u' && str[i + 3] == 'o' && str[i + 4] == 't' && str[i + 5] == ';' && !(flags & ENT_NOQUOTES)) {
        *p++ = '"';
        i += 6;
      } else if (str[i + 1] == '#' && str[i + 2] == '0' && str[i + 3] == '3' && str[i + 4] == '9' && str[i + 5] == ';' && (flags & ENT_QUOTES)) {
        *p++ = '\'';
        i += 6;
      } else if (str[i + 1] == 'l' && str[i + 2] == 't' && str[i + 3] == ';') {
        *p++ = '<';
        i += 4;
      } else if (str[i + 1] == 'g' && str[i + 2] == 't' && str[i + 3] == ';') {
        *p++ = '>';
        i += 4;
      } else {
        *p++ = '&';
        i++;
      }
    } else {
      *p++ = str[i];
      i++;
    }
  }
  res.shrink ((dl::size_type)(p - res.c_str()));

  return res;
}

string f$lcfirst (const string &str) {
  int n = str.size();
  if (n == 0) {
    return str;
  }

  string res (n, false);
  res[0] = (char)tolower (str[0]);
  memcpy (&res[1], &str[1], n - 1);

  return res;
}

string f$lcwords (const string &str) {
  int n = str.size();

  bool in_word = false;
  string res (n, false);
  for (int i = 0; i < n; i++) {
    int cur = str[i] | 0x20;
    if ('a' <= cur && cur <= 'z') {
      if (in_word) {
        res[i] = str[i];
      } else {
        res[i] = (char)cur;
        in_word = true;
      }
    } else {
      res[i] = str[i];
      in_word = false;
    }
  }

  return res;
}

int f$levenshtein (const string &str1, const string &str2) {
  int len1 = str1.size();
  int len2 = str2.size();

  const int MAX_LEN = 16384;
  if (len1 > MAX_LEN || len2 > MAX_LEN) {
    php_warning ("Too long strings of length %d and %d supplied for function levenshtein. Maximum allowed length is %d.", len1, len2, MAX_LEN);
    if (len1 > MAX_LEN) {
      len1 = MAX_LEN;
    }
    if (len2 > MAX_LEN) {
      len2 = MAX_LEN;
    }
  }

  static int dp[2][MAX_LEN + 1];

  for (int j = 0; j <= len2; j++) {
    dp[0][j] = j;
  }

  for (int i = 1; i <= len1; i++) {
    dp[i & 1][0] = i;
    for (int j = 1; j <= len2; j++) {
      if (str1[i - 1] == str2[j - 1]) {
        dp[i & 1][j] = dp[(i - 1) & 1][j - 1];
      } else {
        int res = dp[(i - 1) & 1][j - 1];
        if (dp[(i - 1) & 1][j] < res) {
          res = dp[(i - 1) & 1][j];
        }
        if (dp[i & 1][j - 1] < res) {
          res = dp[i & 1][j - 1];
        }
        dp[i & 1][j] = res + 1;
      }
    }
  }
  return dp[len1 & 1][len2];
}

string f$ltrim (const string &s, const string &what) {
  const char *mask = get_mask (what);

  int len = (int)s.size();
  if (len == 0 || !mask[(unsigned char)s[0]]) {
    return s;
  }

  int l = 1;
  while (l < len && mask[(unsigned char)s[l]]) {
    l++;
  }
  return string (s.c_str() + l, len - l);
}

string f$mysql_escape_string (const string &str) {
  int len = str.size();
  static_SB.clean().reserve (2 * len);
  for (int i = 0; i < len; i++) {
    switch (str[i]) {
      case '\0':
      case '\n':
      case '\r':
      case 26:
      case '\'':
      case '\"':
      case '\\':
        static_SB.append_char ('\\');
        //no break
      default:
        static_SB.append_char (str[i]);
    }
  }
  return static_SB.str();
}

string f$nl2br (const string &str, bool is_xhtml) {
  const char *br = is_xhtml ? "<br />" : "<br>";
  int br_len = (int)strlen (br);

  int len = str.size();

  static_SB.clean().reserve ((br_len + 1) * len);

  for (int i = 0; i < len; ) {
    if (str[i] == '\n' || str[i] == '\r') {
      static_SB.append_unsafe (br, br_len);
      if (str[i] + str[i + 1] == '\n' + '\r') {
        static_SB.append_char (str[i++]);
      }
    }
    static_SB.append_char (str[i++]);
  }

  return static_SB.str();
}

string f$number_format (double number, int decimals, const string &dec_point, const string &thousands_sep) {
  char *result_begin = php_buf + PHP_BUF_LEN;

  if ((unsigned int)decimals > 100u) {
    php_warning ("Wrong parameter decimals (%d) in function number_format", decimals);
    return string();
  }
  bool negative = false;
  if (number < 0) {
    negative = true;
    number *= -1;
  }

  double frac = number - floor (number);
  number -= frac;

  double mul = pow (10.0, (double)decimals);
  frac = round (frac * mul + 1e-9);

  int old_decimals = decimals;
  while (result_begin > php_buf && decimals--) {
    double x = floor (frac * 0.1 + 0.05);
    int y = (int)(frac - x * 10 + 0.05);
    if ((unsigned int)y >= 10u) {
      y = 0;
    }
    frac = x;

    *--result_begin = (char)(y + '0');
  }
  number += frac;

  if (old_decimals > 0) {
    int i = f$strlen (dec_point);
    while (result_begin > php_buf && i > 0) {
      *--result_begin = dec_point[--i];
    }
  }

  int digits = 0;
  do {
    if (digits && digits % 3 == 0) {
      int i = f$strlen (thousands_sep);
      while (result_begin > php_buf && i > 0) {
        *--result_begin = thousands_sep[--i];
      }
    }
    digits++;

    if (result_begin > php_buf) {
      double x = floor (number * 0.1 + 0.05);
      int y = (int)(number - x * 10 + 0.05);
      if ((unsigned int)y >= 10u) {
        y = 0;
      }
      number = x;

      *--result_begin = (char)(y + '0');
    }
  } while (result_begin > php_buf && number > 0.5);

  if (result_begin > php_buf && negative) {
    *--result_begin = '-';
  }

  if (result_begin > php_buf) {
    return string (result_begin, (dl::size_type)(php_buf + PHP_BUF_LEN - result_begin));
  } else {
    php_critical_error ("maximum length of result (%d) exceeded", PHP_BUF_LEN);
    return string();
  }
}

int f$ord (const string &s) {
  return (unsigned char)s[0];
}

string f$pack (const array <var> &a) {
  int n = a.count();
  if (n == 0) {
    php_warning ("pack must take at least 1 argument");
    return string();
  }

  static_SB.clean();
  const string pattern = a.get_value (0).to_string();
  int cur_arg = 1;
  for (int i = 0; i < (int)pattern.size(); ) {
    if (pattern[i] == '*') {
      if (i + 1 == (int)pattern.size()) {
        if (cur_arg == a.count()) {
          break;
        } else {
          i--;
        }
      } else {
        php_warning ("Misplaced symbol '*' in pattern \"%s\"", pattern.c_str());
        return string();
      }
    }
    char format = pattern[i++];
    int cnt = 1;
    if ('0' <= pattern[i] && pattern[i] <= '9') {
      cnt = 0;
      do {
        cnt = cnt * 10 + pattern[i++] - '0';
      } while ('0' <= pattern[i] && pattern[i] <= '9');

      if (cnt <= 0) {
        php_warning ("Wrong count specifier in pattern \"%s\"", pattern.c_str());
        return string();
      }
    } else if (pattern[i] == '*') {
      cnt = 0;
    }

    int arg_num = cur_arg++;
    if (arg_num >= a.count()) {
      php_warning ("Not enough parameters to call function pack");
      return string();
    }

    var arg = a.get_value (arg_num);

    if (arg.is_array()) {
      php_warning ("Argument %d of function pack is array", arg_num);
      return string();
    }

    char filler = 0;
    switch (format) {
      case 'A':
        filler = ' ';
      case 'a': {
        string arg_str = arg.to_string();
        int len = arg_str.size();
        if (!cnt) {
          cnt = len;
          i++;
        }
        static_SB.append (arg_str.c_str(), min (cnt, len));
        while (cnt > len) {
          static_SB += filler;
          cnt--;
        }
        break;
      }
      case 'h':
      case 'H': {
        string arg_str = arg.to_string();
        int len = arg_str.size();
        if (!cnt) {
          cnt = len;
          i++;
        }
        for (int j = 0; cnt > 0 && j < len; j += 2) {
          int num_high = hex_to_int (arg_str[j]);
          int num_low = cnt > 1 ? hex_to_int (arg_str[j + 1]) : 0;
          cnt -= 2;
          if (num_high == 16 || num_low == 16) {
            php_warning ("Wrong argument \"%s\" supplied for format '%c' in function pack", arg_str.c_str(), format);
            return string();
          }
          if (format == 'H') {
            static_SB += (char)((num_high << 4) + num_low);
          } else {
            static_SB += (char)((num_low << 4) + num_high);
          }
        }
        if (cnt > 0) {
          php_warning ("Type %c: not enough characters in string \"%s\" in function pack", format, arg_str.c_str());
        }
        break;
      }

      default:
        do {
          switch (format) {
            case 'c':
            case 'C':
              static_SB += (char)(arg.to_int());
              break;
            case 's':
            case 'S':
            case 'v': {
              unsigned short value = (short)arg.to_int();
              static_SB.append ((const char *)&value, 2);
              break;
            }
            case 'n': {
              unsigned short value = (short)arg.to_int();
              static_SB += (char)(value >> 8);
              static_SB += (char)(value & 255);
              break;
            }
            case 'i':
            case 'I':
            case 'l':
            case 'L':
            case 'V': {
              int value = arg.to_int();
              static_SB.append ((const char *)&value, 4);
              break;
            }
            case 'N': {
              unsigned int value = arg.to_int();
              static_SB += (char)(value >> 24);
              static_SB += (char)((value >> 16) & 255);
              static_SB += (char)((value >> 8) & 255);
              static_SB += (char)(value & 255);
              break;
            }
            case 'f': {
              float value = (float)arg.to_float();
              static_SB.append ((const char *)&value, sizeof (float));
              break;
            }
            case 'd': {
              double value = arg.to_float();
              static_SB.append ((const char *)&value, sizeof (double));
              break;
            }
            default:
              php_warning ("Format code \"%c\" not supported", format);
              return string();
          }

          if (cnt > 1) {
            arg_num = cur_arg++;
            if (arg_num >= a.count()) {
              php_warning ("Not enough parameters to call function pack");
              return string();
            }

            arg = a.get_value (arg_num);

            if (arg.is_array()) {
              php_warning ("Argument %d of function pack is array", arg_num);
              return string();
            }
          }
        } while (--cnt > 0);
    }
  }

  php_assert (cur_arg <= a.count());
  if (cur_arg < a.count()) {
    php_warning ("Too much arguments to call pack with format \"%s\"", pattern.c_str());
  }

  return static_SB.str();
}

int f$printf (const array <var> &a) {
  string to_print = f$sprintf (a);
  print (to_print.c_str(), to_print.size());
  return to_print.size();
}

string f$rtrim (const string &s, const string &what) {
  const char *mask = get_mask (what);

  int len = (int)s.size() - 1;
  if (len == -1 || !mask[(unsigned char)s[len]]) {
    return s;
  }

  while (len > 0 && mask[(unsigned char)s[len - 1]]) {
    len--;
  }

  return string (s.c_str(), len);
}

OrFalse <string> f$setlocale (int category, const string &locale) {
  const char *loc = locale.c_str();
  if (locale[0] == '0' && locale.size() == 1) {
    loc = NULL;
  }
  char *res = setlocale (category, loc);
  if (res == NULL) {
    return false;
  }
  return string (res, (dl::size_type)strlen (res));
}

string f$sprintf (const array <var> &a) {
  int n = a.count();
  if (n == 0) {
    php_warning ("sprintf must take at least 1 argument");
    return string();
  }

  string result;
  string pattern = a.get_value (0).to_string();
  int cur_arg = 1;
  bool error_too_big = false;
  for (int i = 0; i < (int)pattern.size(); i++) {
    if (pattern[i] != '%') {
      result.push_back (pattern[i]);
    } else {
      i++;

      int arg_num = 0, j;
      for (j = i; '0' <= pattern[j] && pattern[j] <= '9'; j++) {
        arg_num = arg_num * 10 + pattern[j] - '0';
      }
      if (pattern[j] == '$' && arg_num > 0) {
        i = j + 1;
      } else {
        arg_num = 0;
      }

      char sign = 0;
      if (pattern[i] == '+') {
        sign = pattern[i++];
      }

      char filler = ' ';
      if (pattern[i] == '0' || pattern[i] == ' ') {
        filler = pattern[i++];
      } else if (pattern[i] == '\'') {
        i++;
        filler = pattern[i++];
      }

      int pad_right = false;
      if (pattern[i] == '-') {
        pad_right = true;
        i++;
      }

      int width = 0;
      while ('0' <= pattern[i] && pattern[i] <= '9' && width < PHP_BUF_LEN) {
        width = width * 10 + pattern[i++] - '0';
      }

      if (width >= PHP_BUF_LEN) {
        error_too_big = true;
        break;
      }

      int precision = -1;
      if (pattern[i] == '.' && '0' <= pattern[i + 1] && pattern[i + 1] <= '9') {
        precision = pattern[i + 1] - '0';
        i += 2;
        while ('0' <= pattern[i] && pattern[i] <= '9' && precision < PHP_BUF_LEN) {
          precision = precision * 10 + pattern[i++] - '0';
        }
      }

      if (precision >= PHP_BUF_LEN) {
        error_too_big = true;
        break;
      }

      string piece;
      if (pattern[i] == '%') {
        piece = PERCENT;
      } else {
        if (arg_num == 0) {
          arg_num = cur_arg++;
        }

        if (arg_num >= a.count()) {
          php_warning ("Not enough parameters to call function sprintf with pattern \"%s\"", pattern.c_str());
          return string();
        }

        if ((dl::size_type)arg_num == 0) {
          php_warning ("Wrong parameter number 0 specified in function sprintf with pattern \"%s\"", pattern.c_str());
          return string();
        }

        const var &arg = a.get_value (arg_num);

        if (arg.is_array()) {
          php_warning ("Argument %d of function sprintf is array", arg_num);
          return string();
        }

        switch (pattern[i]) {
          case 'b': {
            unsigned int arg_int = arg.to_int();
            int cur_pos = 70;
            do {
              php_buf[--cur_pos] = (char)((arg_int & 1) + '0');
              arg_int >>= 1;
            } while (arg_int > 0);
            piece.assign (php_buf + cur_pos, 70 - cur_pos);
            break;
          }
          case 'c': {
            int arg_int = arg.to_int();
            if (arg_int <= -128 || arg_int > 255) {
              php_warning ("Wrong parameter for specifier %%c in function sprintf with pattern \"%s\"", pattern.c_str());
            }
            piece.assign (1, (char)arg_int);
            break;
          }
          case 'd': {
            int arg_int = arg.to_int();
            if (sign == '+' && arg_int >= 0) {
              piece = (static_SB.clean() + "+" + arg_int).str();
            } else {
              piece = string (arg_int);
            }
            break;
          }
          case 'u': {
            unsigned int arg_int = arg.to_int();
            int cur_pos = 70;
            do {
              php_buf[--cur_pos] = (char)(arg_int % 10 + '0');
              arg_int /= 10;
            } while (arg_int > 0);
            piece.assign (php_buf + cur_pos, 70 - cur_pos);
            break;
          }
          case 'e':
          case 'E':
          case 'f':
          case 'F':
          case 'g':
          case 'G': {
            double arg_float = arg.to_float();

            static_SB.clean() += '%';
            if (sign) {
              static_SB += sign;
            }
            if (precision >= 0) {
              static_SB += '.';
              static_SB += precision;
            }
            static_SB += pattern[i];

            int len = snprintf (php_buf, PHP_BUF_LEN, static_SB.c_str(), arg_float);
            if (len >= PHP_BUF_LEN) {
              error_too_big = true;
              break;
            }

            piece.assign (php_buf, len);
            break;
          }
          case 'o': {
            unsigned int arg_int = arg.to_int();
            int cur_pos = 70;
            do {
              php_buf[--cur_pos] = (char)((arg_int & 7) + '0');
              arg_int >>= 3;
            } while (arg_int > 0);
            piece.assign (php_buf + cur_pos, 70 - cur_pos);
            break;
          }
          case 's': {
            string arg_string = arg.to_string();

            static_SB.clean() += '%';
            if (precision >= 0) {
              static_SB += '.';
              static_SB += precision;
            }
            static_SB += 's';

            int len = snprintf (php_buf, PHP_BUF_LEN, static_SB.c_str(), arg_string.c_str());
            if (len >= PHP_BUF_LEN) {
              error_too_big = true;
              break;
            }

            piece.assign (php_buf, len);
            break;
          }
          case 'x':
          case 'X': {
            const char *hex_digits = (pattern[i] == 'x' ? lhex_digits : uhex_digits);
            unsigned int arg_int = arg.to_int();

            int cur_pos = 70;
            do {
              php_buf[--cur_pos] = hex_digits[arg_int & 15];
              arg_int >>= 4;
            } while (arg_int > 0);
            piece.assign (php_buf + cur_pos, 70 - cur_pos);
            break;
          }
          default:
            php_warning ("Unsupported specifier %%%c in sprintf with pattern \"%s\"", pattern[i], pattern.c_str());
            return string();
        }
      }

      result.append (f$str_pad (piece, width, string (1, filler), pad_right));
    }
  }

  if (error_too_big) {
    php_warning ("Too big result in function sprintf");
    return string();
  }

  return result;
}

string f$stripslashes (const string &str) {
  int len = str.size();
  int i;

  string result (len, false);
  char *result_c_str = &result[0];
  for (i = 0; i + 1 < len; i++) {
    if (str[i] == '\\') {
      i++;
      if (str[i] == '0') {
        *result_c_str++ = '\0';
        continue;
      }
    }

    *result_c_str++ = str[i];
  }
  if (i + 1 == len && str[i] != '\\') {
    *result_c_str++ = str[i];
  }
  result.shrink (result_c_str - result.c_str());
  return result;
}

int f$strcasecmp (const string &lhs, const string &rhs) {
  int n = min (lhs.size(), rhs.size()) + 1;
  for (int i = 0; i < n; i++) {
    if (tolower (lhs[i]) != tolower (rhs[i])) {
      return tolower (lhs[i]) - tolower (rhs[i]);
    }
  }
  return 0;
}

int f$strcmp (const string &lhs, const string &rhs) {
  return lhs.compare (rhs);
}

OrFalse <int> f$stripos (const string &haystack, const string &needle, int offset) {
  if (offset < 0) {
    php_warning ("Wrong offset = %d in function stripos", offset);
    return false;
  }
  if (offset >= (int)haystack.size()) {
    return false;
  }
  if ((int)needle.size() == 0) {
    php_warning ("Parameter needle is empty in function stripos");
    return false;
  }

  const char *s = strcasestr (haystack.c_str() + offset, needle.c_str());
  if (s == NULL) {
    return false;
  }
  return (int)(s - haystack.c_str());
}

OrFalse <string> f$stristr (const string &haystack, const string &needle, bool before_needle) {
  if ((int)needle.size() == 0) {
    php_warning ("Parameter needle is empty in function stristr");
    return false;
  }

  const char *s = strcasestr (haystack.c_str(), needle.c_str());
  if (s == NULL) {
    return false;
  }

  dl::size_type pos = (dl::size_type)(s - haystack.c_str());
  if (before_needle) {
    return haystack.substr (0, pos);
  }
  return haystack.substr (pos, haystack.size() - pos);
}

int f$strncmp (const string &lhs, const string &rhs, int len) {
  if (len < 0) {
    return 0;
  }
  return memcmp (lhs.c_str(), rhs.c_str(), min ((int)min (lhs.size(), rhs.size()) + 1, len));
}

OrFalse <string> f$strpbrk (const string &haystack, const string &char_list) {
  const char *pos = strpbrk (haystack.c_str(), char_list.c_str());
  if (pos == NULL) {
    return false;
  } else {
    return string (pos, (dl::size_type)(haystack.size() - (pos - haystack.c_str())));
  }
}

OrFalse <int> f$strpos (const string &haystack, const string &needle, int offset) {
  if (offset < 0) {
    php_warning ("Wrong offset = %d in function strpos", offset);
    return false;
  }
  if (offset > (int)haystack.size()) {
    return false;
  }
  if ((int)needle.size() <= 1) {
    if ((int)needle.size() == 0) {
      php_warning ("Parameter needle is empty in function strpos");
      return false;
    }

    const char *s = static_cast <const char *> (memchr (haystack.c_str() + offset, needle[0], haystack.size() - offset));
    if (s == NULL) {
      return false;
    }
    return (int)(s - haystack.c_str());
  }

  const char *s = static_cast <const char *> (memmem (haystack.c_str() + offset, haystack.size() - offset, needle.c_str(), needle.size()));
  if (s == NULL) {
    return false;
  }
  return (int)(s - haystack.c_str());
}

OrFalse <int> f$strrpos (const string &haystack, const string &needle, int offset) {
  const char *end = haystack.c_str() + haystack.size();
  if (offset < 0) {
    offset += (int)haystack.size() + 1;
    if (offset < 0) {
      return false;
    }

    end = haystack.c_str() + offset;
    offset = 0;
  }
  if (offset >= (int)haystack.size()) {
    return false;
  }
  if ((int)needle.size() == 0) {
    php_warning ("Parameter needle is empty in function strrpos");
    return false;
  }

  const char *s = static_cast <const char *> (memmem (haystack.c_str() + offset, haystack.size() - offset, needle.c_str(), needle.size())), *t;
  if (s == NULL || s >= end) {
    return false;
  }
  while ((t = static_cast <const char *> (memmem (s + 1, haystack.c_str() + haystack.size() - s - 1, needle.c_str(), needle.size()))) != NULL && t < end) {
    s = t;
  }
  return (int)(s - haystack.c_str());
}

OrFalse <int> f$strripos (const string &haystack, const string &needle, int offset) {
  const char *end = haystack.c_str() + haystack.size();
  if (offset < 0) {
    offset += (int)haystack.size() + 1;
    if (offset < 0) {
      return false;
    }

    end = haystack.c_str() + offset;
    offset = 0;
  }
  if (offset >= (int)haystack.size()) {
    return false;
  }
  if ((int)needle.size() == 0) {
    php_warning ("Parameter needle is empty in function strripos");
    return false;
  }

  const char *s = strcasestr (haystack.c_str() + offset, needle.c_str()), *t;
  if (s == NULL || s >= end) {
    return false;
  }
  while ((t = strcasestr (s + 1, needle.c_str())) != NULL && t < end) {
    s = t;
  }
  return (int)(s - haystack.c_str());
}

string f$strrev (const string &str) {
  int n = str.size();

  string res (n, false);
  for (int i = 0; i < n; i++) {
    res[n - i - 1] = str[i];
  }

  return res;
}

OrFalse <string> f$strstr (const string &haystack, const string &needle, bool before_needle) {
  if ((int)needle.size() == 0) {
    php_warning ("Parameter needle is empty in function strstr");
    return false;
  }

  const char *s = static_cast <const char *> (memmem (haystack.c_str(), haystack.size(), needle.c_str(), needle.size()));
  if (s == NULL) {
    return false;
  }

  dl::size_type pos = (dl::size_type)(s - haystack.c_str());
  if (before_needle) {
    return haystack.substr (0, pos);
  }
  return haystack.substr (pos, haystack.size() - pos);
}

string f$strtolower (const string &str) {
  int n = str.size();

  string res (n, false);
  for (int i = 0; i < n; i++) {
    res[i] = (char)tolower (str[i]);
  }

  return res;
}

string f$strtoupper (const string &str) {
  int n = str.size();

  string res (n, false);
  for (int i = 0; i < n; i++) {
    res[i] = (char)toupper (str[i]);
  }

  return res;
}

string f$strtr (const string &subject, const string &from, const string &to) {
  int n = subject.size();
  string result (n, false);
  for (int i = 0; i < n; i++) {
    const char *p = static_cast <const char *> (memchr (static_cast <const void *> (from.c_str()), (int)(unsigned char)subject[i], (size_t)from.size()));
    if (p == NULL || (dl::size_type)(p - from.c_str()) >= to.size()) {
      result[i] = subject[i];
    } else {
      result[i] = to[(dl::size_type)(p - from.c_str())];
    }
  }
  return result;
}

string f$str_pad (const string &input, int len, const string &pad_str, int pad_type) {
  int old_len = input.size();
  if (len <= old_len) {
    return input;
  }

  int pad_left = 0, pad_right = 0;
  if (pad_type == STR_PAD_RIGHT) {
    pad_right = len - old_len;
  } else if (pad_type == STR_PAD_LEFT) {
    pad_left = len - old_len;
  } else if (pad_type == STR_PAD_BOTH) {
    pad_left = (len - old_len) / 2;
    pad_right = (len - old_len + 1) / 2;
  } else {
    php_warning ("Wrong parameter pad_type in function str_pad");
    return input;
  }

  int pad_len = pad_str.size();
  if (pad_len == 0) {
    php_warning ("Wrong parameter pad_str (empty string) in function str_pad");
    return input;
  }

  string res (len, false);
  for (int i = 0; i < pad_left; i++) {
    res[i] = pad_str[i % pad_len];
  }
  memcpy (&res[pad_left], input.c_str(), old_len);
  for (int i = 0; i < pad_right; i++) {
    res[i + pad_left + old_len] = pad_str[i % pad_len];
  }

  return res;
}

string f$str_repeat (const string &s, int multiplier) {
  int len = (int)s.size();
  if (multiplier <= 0 || len == 0) {
    return string();
  }

  if (string::max_size / len < (dl::size_type)multiplier) {
    php_critical_error ("tried to allocate too big string of size %lld", (long long)multiplier * len);
  }

  if (len == 1) {
    return string (multiplier, s[0]);
  }

  string result (multiplier * len, false);
  if (len >= 5) {
    while (multiplier--) {
      memcpy (&result[multiplier * len], s.c_str(), len);
    }
  } else {
    for (int i = 0; i < multiplier; i++) {
      for (int j = 0; j < len; j++) {
        result[i * len + j] = s[j];
      }
    }
  }
  return result;
}

static string str_replace_char (char c, const string &replace, const string &subject, int &replace_count) {
  int count = 0;
  const char *piece = subject.c_str(), *piece_end = subject.c_str() + subject.size();
  string result;
  while (1) {
    const char *pos = (const char *)memchr (piece, c, piece_end - piece);
    if (pos == NULL) {
      if (count == 0) {
        return subject;
      }
      replace_count += count;
      result.append (piece, (dl::size_type)(piece_end - piece));
      return result;
    }

    ++count;

    result.append (piece, (dl::size_type)(pos - piece));
    result.append (replace);

    piece = pos + 1;
  }
  php_assert (0);//unreacheable
  return string();
}

static string str_replace (const string &search, const string &replace, const string &subject, int &replace_count) {
  if ((int)search.size() == 0) {
    php_warning ("Parameter search is empty in function str_replace");
    return subject;
  }

  int count = 0;
  const char *piece = subject.c_str(), *piece_end = subject.c_str() + subject.size();
  string result;
  while (1) {
    const char *pos = static_cast <const char *> (memmem (piece, piece_end - piece, search.c_str(), search.size()));
    if (pos == NULL) {
      if (count == 0) {
        return subject;
      }
      replace_count += count;
      result.append (piece, (dl::size_type)(piece_end - piece));
      return result;
    }

    ++count;

    result.append (piece, (dl::size_type)(pos - piece));
    result.append (replace);

    piece = pos + search.size();
  }
  php_assert (0);//unreacheable
  return string();
}

inline var str_replace_string (const var &search, const var &replace, const string &subject, int &replace_count) {
  if (search.is_array()) {
    var result = subject;

    string replace_value;
    array <var>::const_iterator cur_replace_val;
    if (replace.is_array()) {
      cur_replace_val = replace.begin();
    } else {
      replace_value = replace.to_string();
    }

    for (array <var>::const_iterator it = search.begin(); it != search.end(); ++it) {
      if (replace.is_array()) {
        if (cur_replace_val != replace.end()) {
          replace_value = f$strval (cur_replace_val.get_value());
          ++cur_replace_val;
        } else {
          replace_value = string();
        }
      }

      result = str_replace (f$strval (it.get_value()), replace_value, result.to_string(), replace_count);
    }

    return result;
  } else {
    if (replace.is_array()) {
      php_warning ("Parameter mismatch, search is a string while replace is an array");
      return false;
    }

    return str_replace (f$strval (search), f$strval (replace), subject, replace_count);
  }
}

string f$str_replace (const string &search, const string &replace, const string &subject, int &replace_count) {
  replace_count = 0;
  if ((int)search.size() == 1) {
    return str_replace_char (search[0], replace, subject, replace_count);
  } else {
    return str_replace (search, replace, subject, replace_count);
  }
}

var f$str_replace (const var &search, const var &replace, const var &subject, int &replace_count) {
  replace_count = 0;
  if (subject.is_array()) {
    array <var> result;
    for (array <var>::const_iterator it = subject.begin(); it != subject.end(); ++it) {
      var cur_result = str_replace_string (search, replace, it.get_value().to_string(), replace_count);
      if (!cur_result.is_null()) {
        result.set_value (it.get_key(), cur_result);
      }
    }
    return result;
  } else {
    return str_replace_string (search, replace, subject.to_string(), replace_count);
  }
}

array <string> f$str_split (const string &str, int split_length) {
  if (split_length <= 0) {
    php_warning ("Wrong parameter split_length = %d in function str_split", split_length);
    return f$arrayval (str);
  }

  array <string> result (array_size ((str.size() + split_length - 1) / split_length, 0, true));
  int i;
  for (i = 0; i + split_length <= (int)str.size(); i += split_length) {
    result.push_back (str.substr (i, split_length));
  }
  if (i < (int)str.size()) {
    result.push_back (str.substr (i, str.size() - i));
  }
  return result;
}

string f$substr (const string &str, int start, int length) {
  int len = str.size();
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

  return str.substr (start, length);
}

int f$substr_count (const string &haystack, const string &needle, int offset, int length) {
  if (offset < 0) {
    offset = (int)haystack.size() + offset;
    if (offset < 0) {
      offset = 0;
    }
  }
  if (offset >= (int)haystack.size()) {
    return 0;
  }
  if (length > (int)haystack.size() - offset) {
    length = haystack.size() - offset;
  }

  int ans = 0;
  const char *s = haystack.c_str() + offset, *end = haystack.c_str() + offset + length;
  if (needle.size() == 0) {
    php_warning ("Needle is empty in function substr_count");
    return (int)(end - s);
  }
  do {
    s = static_cast <const char *> (memmem (static_cast <const void *> (s), (size_t)(end - s), static_cast <const void *> (needle.c_str()), (size_t)needle.size()));
    if (s == NULL) {
      return ans;
    }
    ans++;
    s += needle.size();
  } while (true);
}

string f$trim (const string &s, const string &what) {
  const char *mask = get_mask (what);

  int len = (int)s.size();
  if (len == 0 || (!mask[(unsigned char)s[len - 1]] && !mask[(unsigned char)s[0]])) {
    return s;
  }

  while (len > 0 && mask[(unsigned char)s[len - 1]]) {
    len--;
  }

  if (len == 0) {
    return string();
  }

  int l = 0;
  while (mask[(unsigned char)s[l]]) {
    l++;
  }
  return string (s.c_str() + l, len - l);
}

string f$ucfirst (const string &str) {
  int n = str.size();
  if (n == 0) {
    return str;
  }

  string res (n, false);
  res[0] = (char)toupper (str[0]);
  memcpy (&res[1], &str[1], n - 1);

  return res;
}

string f$ucwords (const string &str) {
  int n = str.size();

  bool in_word = false;
  string res (n, false);
  for (int i = 0; i < n; i++) {
    int cur = str[i] & 0xdf;
    if ('A' <= cur && cur <= 'Z') {
      if (in_word) {
        res[i] = str[i];
      } else {
        res[i] = (char)cur;
        in_word = true;
      }
    } else {
      res[i] = str[i];
      in_word = false;
    }
  }

  return res;
}

array <var> f$unpack (const string &pattern, const string &data) {
  array <var> result;

  int data_len = data.size(), data_pos = 0;
  for (int i = 0; i < (int)pattern.size(); ) {
    if (data_pos >= data_len) {
      php_warning ("Not enougn data to unpack with format \"%s\"", pattern.c_str());
      return result;
    }
    char format = pattern[i++];
    int cnt = -1;
    if ('0' <= pattern[i] && pattern[i] <= '9') {
      cnt = 0;
      do {
        cnt = cnt * 10 + pattern[i++] - '0';
      } while ('0' <= pattern[i] && pattern[i] <= '9');

      if (cnt <= 0) {
        php_warning ("Wrong count specifier in pattern \"%s\"", pattern.c_str());
        return result;
      }
    } else if (pattern[i] == '*') {
      cnt = 0;
      i++;
    }

    const char *key_end = strchrnul (&pattern[i], '/');
    string key_prefix (pattern.c_str() + i, (dl::size_type)(key_end - pattern.c_str() - i));
    i = (int)(key_end - pattern.c_str());
    if (i < (int)pattern.size()) {
      i++;
    }

    if (cnt == 0 && i != (int)pattern.size()) {
      php_warning ("Misplaced symbol '*' in pattern \"%s\"", pattern.c_str());
      return result;
    }

    char filler = 0;
    switch (format) {
      case 'A':
        filler = ' ';
      case 'a': {
        if (cnt == 0) {
          cnt = data_len - data_pos;
        } else if (cnt == -1) {
          cnt = 1;
        }
        int read_len = cnt;
        if (read_len + data_pos > data_len) {
          php_warning ("Not enougn data to unpack with format \"%s\"", pattern.c_str());
          return result;
        }
        while (cnt > 0 && data[data_pos + cnt - 1] == filler) {
          cnt--;
        }

        if (key_prefix.size() == 0) {
          key_prefix = ONE;
        }

        result.set_value (key_prefix, string (data.c_str() + data_pos, cnt));

        data_pos += read_len;
        break;
      }
      case 'h':
      case 'H': {
        if (cnt == 0) {
          cnt = (data_len - data_pos) * 2;
        } else if (cnt == -1) {
          cnt = 1;
        }

        int read_len = (cnt + 1) / 2;
        if (read_len + data_pos > data_len) {
          php_warning ("Not enougn data to unpack with format \"%s\"", pattern.c_str());
          return result;
        }

        string value (cnt, false);
        for (int j = data_pos; cnt > 0; j++, cnt -= 2) {
          unsigned char ch = data[j];
          char num_high = lhex_digits[ch >> 4];
          char num_low = lhex_digits[ch & 15];
          if (format == 'h') {
            swap (num_high, num_low);
          }

          value[(j - data_pos) * 2] = num_high;
          if (cnt > 1) {
            value[(j - data_pos) * 2 + 1] = num_low;
          }
        }
        php_assert (cnt == 0 || cnt == -1);

        if (key_prefix.size() == 0) {
          key_prefix = ONE;
        }

        result.set_value (key_prefix, value);

        data_pos += read_len;
        break;
      }

      default: {
        if (key_prefix.size() == 0 && cnt == -1) {
          key_prefix = ONE;
        }
        int counter = 1;
        do {
          var value;
          int value_int;
          if (data_pos >= data_len) {
            php_warning ("Not enougn data to unpack with format \"%s\"", pattern.c_str());
            return result;
          }

          switch (format) {
            case 'c':
            case 'C':
              value_int = (int)data[data_pos++];
              if (format != 'c' && value_int < 0) {
                value_int += 256;
              }
              value = value_int;
              break;
            case 's':
            case 'S':
            case 'v':
              value_int = (unsigned char)data[data_pos];
              if (data_pos + 1 < data_len) {
                value_int |= data[data_pos + 1] << 8;
              }
              data_pos += 2;
              if (format != 's' && value_int < 0) {
                value_int += 65536;
              }
              value = value_int;
              break;
            case 'n':
              value_int = (unsigned char)data[data_pos] << 8;
              if (data_pos + 1 < data_len) {
                value_int |= (unsigned char)data[data_pos + 1];
              }
              data_pos += 2;
              value = value_int;
              break;
            case 'i':
            case 'I':
            case 'l':
            case 'L':
            case 'V':
              value_int = (unsigned char)data[data_pos];
              if (data_pos + 1 < data_len) {
                value_int |= (unsigned char)data[data_pos + 1] << 8;
                if (data_pos + 2 < data_len) {
                  value_int |= (unsigned char)data[data_pos + 2] << 16;
                  if (data_pos + 3 < data_len) {
                    value_int |= data[data_pos + 3] << 24;
                  }
                }
              }
              data_pos += 4;
              value = value_int;
              break;
            case 'N':
              value_int = (unsigned char)data[data_pos] << 24;
              if (data_pos + 1 < data_len) {
                value_int |= (unsigned char)data[data_pos + 1] << 16;
                if (data_pos + 2 < data_len) {
                  value_int |= (unsigned char)data[data_pos + 2] << 8;
                  if (data_pos + 3 < data_len) {
                    value_int |= (unsigned char)data[data_pos + 3];
                  }
                }
              }
              data_pos += 4;
              value = value_int;
              break;
            case 'f': {
              if (data_pos + (int)sizeof (float) > data_len) {
                php_warning ("Not enougn data to unpack with format \"%s\"", pattern.c_str());
                return result;
              }
              value = (double)*(float *)(data.c_str() + data_pos);
              data_pos += (int)sizeof (float);
              break;
            }
            case 'd': {
              if (data_pos + (int)sizeof (double) > data_len) {
                php_warning ("Not enougn data to unpack with format \"%s\"", pattern.c_str());
                return result;
              }
              value = *(double *)(data.c_str() + data_pos);
              data_pos += (int)sizeof (double);
              break;
            }
            default:
              php_warning ("Format code \"%c\" not supported", format);
              return result;
          }

          string key = key_prefix;
          if (cnt != -1) {
            key.append (string (counter++));
          }

          result.set_value (key, value);

          if (cnt == 0) {
            if (data_pos >= data_len) {
              return result;
            }
          }
        } while (cnt == 0 || --cnt > 0);
      }
    }
  }
  return result;
}

string f$wordwrap (const string &str, int width, string brk, bool cut) {
  if (width <= 0) {
    php_warning ("Wrong parameter width = %d in function wordwrap", width);
    return str;
  }

  string result;
  int first = 0, n = (int)str.size(), last_space = -1;
  for (int i = 0; i < n; i++) {
    if (str[i] == ' ') {
      last_space = i;
    }
    if (i >= first + width && (cut || last_space > first)) {
      if (last_space <= first) {
        result.append (str, first, i - first);
        first = i;
      } else {
        result.append (str, first, last_space - first);
        first = last_space + 1;
      }
      result.append (brk);
    }
  }
  result.append (str, first, str.size() - first);
  return result;
}
