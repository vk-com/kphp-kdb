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

#include <assert.h>
#include <stdio.h>
#include <string.h>
#include "word-split.h"
#include "crc32.h"
#include "md5.h"

int word_split_version = 2;
int word_split_utf8 = 0;

static unsigned char is_letter[256];
unsigned char l_case[256];

static unsigned char is_letter_utf8[UTF8_TABLE_SIZE];
int l_case_utf8[UTF8_TABLE_SIZE];

static int word_char_freq[256], notword_char_freq[256], word_len_freq[256], notword_len_freq[256];

int (*sigil_word_detect)(int sigil, const char *str);

void init_is_letter_utf8 (void) {
  int i;
  memset (is_letter_utf8, 0, sizeof (is_letter_utf8));

  for (i = '0'; i <= '9'; i++) {
    is_letter_utf8[i] = 4;
  }
  for (i = 'A'; i <= 'Z'; i++) {
    is_letter_utf8[i] = 8;
  }
  for (i = 'a'; i <= 'z'; i++) {
    is_letter_utf8[i] = 9;
  }
  is_letter_utf8[0x401] = 12;
  is_letter_utf8[0x451] = 13;
  is_letter_utf8[0x404] = 12;
  is_letter_utf8[0x454] = 13;
  is_letter_utf8[0x407] = 12;
  is_letter_utf8[0x457] = 13;
  is_letter_utf8[0x406] = 12;
  is_letter_utf8[0x456] = 13;
  is_letter_utf8[0x490] = 12;
  is_letter_utf8[0x491] = 13;
  is_letter_utf8[0x40e] = 12;
  is_letter_utf8[0x45e] = 13;
  
  for (i = 0x410; i <= 0x42f; i++) {
    is_letter_utf8[i] = 12;
  }
  for (i = 0x430; i <= 0x44f; i++) {
    is_letter_utf8[i] = 13;
  }

  for (i = 0; i < UTF8_TABLE_SIZE; i++) {
    l_case_utf8[i] = i;
  }

  for (i = 'A'; i <= 'Z'; i++) {
    l_case_utf8[i] = i + 0x20;
  }
  for (i = 0x410; i <= 0x42f; i++) {
    l_case_utf8[i] = i + 0x20;
  }

  l_case_utf8[0x401] = l_case_utf8[0x451] = 0x435;
  l_case_utf8[0x404] = 0x454;
  l_case_utf8[0x407] = 0x457;
  l_case_utf8[0x406] = 0x456;
  l_case_utf8[0x490] = 0x491;
  l_case_utf8[0x40e] = 0x45e;
}

void init_is_letter (void) {
  int i;

  memset (is_letter, 0, sizeof (is_letter));

  for (i = '0'; i <= '9'; i++) {
    is_letter[i] = 4;
  }
  for (i = 'A'; i <= 'Z'; i++) {
    is_letter[i] = 8;
  }
  for (i = 'a'; i <= 'z'; i++) {
    is_letter[i] = 9;
  }
  is_letter[0xa8] = 12;
  is_letter[0xb8] = 13;
  is_letter[0xaa] = 12;
  is_letter[0xba] = 13;
  is_letter[0xaf] = 12;
  is_letter[0xbf] = 13;
  is_letter[0xb2] = 12;
  is_letter[0xb3] = 13;
  is_letter[0xa5] = 12;
  is_letter[0xb4] = 13;
  is_letter[0xa1] = 12;
  is_letter[0xa2] = 13;
  
  for (i = 0xc0; i <= 0xdf; i++) {
    is_letter[i] = 12;
  }
  for (i = 0xe0; i <= 0xff; i++) {
    is_letter[i] = 13;
  }

  for (i = 0; i < 0x100; i++) {
    l_case[i] = i;
  }
  for (i = 'A'; i <= 'Z'; i++) {
    l_case[i] = i + 0x20;
  }
  for (i = 0xc0; i <= 0xdf; i++) {
    l_case[i] = i + 0x20;
  }
  l_case[0xa8] = l_case[0xb8] = 0xe5;
  l_case[0xaa] = 0xba;
  l_case[0xaf] = 0xbf;
  l_case[0xb2] = 0xb3;
  l_case[0xa5] = 0xb4;
  l_case[0xa1] = 0xa2;

  init_is_letter_utf8 ();
}

void init_letter_freq (void) {
  int i;

  memset (word_char_freq, 0, sizeof (word_char_freq));
  memset (notword_char_freq, 0, sizeof (notword_char_freq));
  memset (word_len_freq, 0, sizeof (word_len_freq));
  memset (notword_len_freq, 0, sizeof (notword_len_freq));

  for (i = 0; i < 0x100; i++) {
    if (is_letter[i]) {
      word_char_freq[i] = 1;
      if (is_letter[i] & 0xc0) {
        notword_char_freq[i] = 1;
      }
    } else {
      notword_char_freq[i] = 1;
    }
  }
  for (i = '0'; i <= '9'; i++) {
    notword_char_freq[i] = 1;
  }
  word_char_freq['&']++;
  word_char_freq['#']++;
  word_char_freq[';']++;

  for (i = 1; i < 160; i++) {
    word_len_freq[i] = 1;
  }

  for (i = 1; i < 40; i++) {
    notword_len_freq[i] = 1;
  }
}

int std_sigil_word_detect (int sigil, const char *str) {
  if (sigil == 0x1f) {
    int z = 0;
    while (z < 32 && str[z] < 0) {
      z++;
    }
    return (str[z] & 0x40) ? z + 1 : -1;
  }
  unsigned char *letter = word_split_utf8 ? is_letter_utf8 : is_letter;
  if (letter['.'] & 8) {
    return -1;
  }
  if (sigil != '[') {
    if (sigil != '#') {
      letter['.'] ^= 8;
      letter['-'] ^= 8;
    }
    int z = get_word (str);
    if (sigil != '#') {
      letter['.'] ^= 8;
      letter['-'] ^= 8;
    }
    return z > 0 ? z : -1;
  }
  if (*str != '[') {
    return -1;
  }
  const char *tmp = str + 1;

  while (
         (tmp <= str + 120) &&
         (
          (*tmp >= '0' && *tmp <= '9') || 
          (*tmp >= 'A' && *tmp <= 'Z') ||
          (*tmp >= 'a' && *tmp <= 'z') || 
          (*tmp == '_') ||
          (*tmp == '-')
         )
        ) {
    ++tmp;
  }

  if (*tmp == ']' && tmp[1] == ']') {
    return tmp + 2 - str;
  }

  return -1;
}

// returns length of word (in bytes) starting from pointer str
// get_word(): "word" = at most 127 alphanumeric characters, including at most 4 digits
// entities like "&#225;" or "&aacute;" are considered alphanumeric, but counted as several characters
// get_notword(): "not-a-word" = at most 4 non-alphanumeric entities ("&lt;" or "<br/>" is one entity), at most 31 bytes
int get_word (const char *str) {
  if (word_split_utf8) {
    return get_word_utf8 (str);
  }
  static int inside_star = 0;
  int b = 0, d = 0;
  int c = (unsigned char) *str;
  if (is_letter[c] & 0x40) {
    if (c == '*') {
      inside_star = 1;
    }
    int z = sigil_word_detect (c, str + 1);
    if (c == '*') {
      inside_star = 0;
    }
    if (z >= 0 && z < 127) {
      return z + 1;
    }
  }
  while (b <= 120) {
    c = (unsigned char) *str;
    if (is_letter[c] & 8) {
      str++;
      if (++b == 127) {
	break;
      }
    } else if (is_letter[c] & 4) {
      if (d == (inside_star ? 30 : 4)) {
	break;
      }
      str++;
      d++;
      b++;
    } else if (c == '&') {
      if (str[1] == '#') {
	int x, v = 0;
	for (x = 2; x <= 7 && str[x] <= '9' && str[x] >= '0'; x++) {
	  v *= 10;
	  v += str[x] - '0';
	}
	if (str[x] == ';' && v && v < 4096) {
	  x++;
          if (!b && v < 128 && (is_letter[v] & 0x40)) {
            int z = sigil_word_detect (v, str + x);
            if (z >= 0 && z < 120) {
              return z + x;
            }
          }
      	  if (v >= 0xc0 && v <= 0xff && v != 0xd7 && v != 0xf7) {
      	    str += x;
      	    b += x;
      	    continue;
      	  }
	}
      }	
      break;
    } else {
      break;
    }
  }
  return b;
}

int get_word_utf8 (const char *str) {
  static int inside_star = 0;
  int b = 0, d = 0;
  int c = (unsigned char) *str;
  if (is_letter_utf8[c] & 0x40) {
    if (c == '*') {
      inside_star = 1;
    }
    int z = sigil_word_detect (c, str + 1);
    if (c == '*') {
      inside_star = 0;
    }
    if (z >= 0 && z < 127) {
      return z + 1;
    }
  }
  while (b <= 120) {
    c = (unsigned char) *str;
    if (c >= 0xc2 && c <= 0xdf && (signed char) str[1] < -0x40) {
      c = ((c & 0x1f) << 6) | (str[1] & 0x3f);
      if (is_letter_utf8[c] & 8) {
	str += 2;
	b += 2;
	if (b >= 126) {
	  break;
	}
      } else {
	break;
      }
    } else if (is_letter_utf8[c] & 8) {
      str++;
      if (++b == 126) {
	break;
      }
    } else if (is_letter_utf8[c] & 4) {
      if (d == (inside_star ? 30 : 4)) {
	break;
      }
      str++;
      d++;
      b++;
      break;
    } else {
      break;
    }
  }
  return b;
}

int get_notword (const char *str) {
  if (word_split_utf8) {
    return get_notword_utf8 (str);
  }
  int b = 0, d = 0;
  int c;
  while (b <= 30 && d <= 4) {
    c = (unsigned char) *str;
    if (!c || (is_letter[c] & 0x3f)) {
      break;
    }
    if ((is_letter[c] & 0x40)) { /* && b removed see (http://vk.com/tasks?act=viewbug&bid=15344) */ 
      int z = sigil_word_detect (c, str + 1);
      if (z > 0 && z < 127) {
        break;
      }
    }
    if (c == '<' && str[1] == 'b') {
      if (!str[2]) {
	// str[0] = str[1] = ' ';
	return -1;
      } else if (str[2] == 'r') {
	if (str[3] == '>') {
	  str += 4;
	  b += 4;
	  d++;
	  continue;
	} else if (str[3] == '/' && str[4] == '>') {
	  str += 5;
	  b += 5;
	  d++;
	  continue;
	}
      }
    } else if (c == '&') {
      if (str[1] == '#') {
	int x, v = 0;
	for (x = 2; x <= 7 && str[x] <= '9' && str[x] >= '0'; x++) {
	  v *= 10;
	  v += str[x] - '0';
	}
	if (str[x] == ';' && v && v < 4096) {
	  x++;
          if (v < 128 && (is_letter[v] & 0x40) && b) {
            int z = sigil_word_detect (c, str + x);
            if (z >= 0 && z < 127) {
              break;
            }
          }
      	  if (v >= 0xc0 && v <= 0xff && v != 0xd7 && v != 0xf7) {
      	    break;
      	  }
	  b += x;
	  str += x;
	  d++;
	  continue;
	}
      } else {
	int x;
	for (x = 1; x <= 10 && str[x] >= 'a' && str[x] <= 'z'; x++) ;
	if (x > 1 && str[x] == ';') {
	  x++;
	  b += x;
	  str += x;
	  d++;
	  continue;
	}
      }
    }
    str++;
    b++;
    d++;
  }
  return b;
}

int get_notword_utf8 (const char *str) {
  int b = 0, d = 0;
  int c;
  while (b <= 30 && d <= 4) {
    c = (unsigned char) *str;
    if (!c || (is_letter_utf8[c] & 0x3f)) {
      break;
    }
    if ((is_letter_utf8[c] & 0x40)) { /* && b removed see (http://vk.com/tasks?act=viewbug&bid=15344) */ 
      int z = sigil_word_detect (c, str + 1);
      if (z > 0 && z < 127) {
        break;
      }
    }
    if (c >= 0xc2 && c <= 0xdf && (signed char) str[1] < -0x40) {
      c = ((c & 0x1f) << 6) | (str[1] & 0x3f);
      if (is_letter_utf8[c] & 8) {
	break;
      }
      str += 2;
      b += 2;
      d++;
      continue;
    }
    if (c == '<' && str[1] == 'b') {
      if (!str[2]) {
	// str[0] = str[1] = ' ';
	return -1;
      } else if (str[2] == 'r') {
	if (str[3] == '>') {
	  str += 4;
	  b += 4;
	  d++;
	  continue;
	} else if (str[3] == '/' && str[4] == '>') {
	  str += 5;
	  b += 5;
	  d++;
	  continue;
	}
      }
    }
    str++;
    b++;
    d++;
  }
  return b;
}

void lc_str_utf8 (char *to, const char *from, int len) {
  while (len > 0) {
    int c = (unsigned char) *from++;
    if (c >= 0xc2 && c < 0xe0 && len > 1 && (signed char) *from < -0x40) {
      c = ((c & 0x1f) << 6) | (*from++ & 0x3f);
      int d = l_case_utf8[c];
      *to++ = 0xc0 + (d >> 6);
      *to++ = 0x80 + (d & 0x3f);
      len -= 2;
    } else if (c < 0x80) {
      *to++ = l_case_utf8[c];
      len--;
    } else {
      *to++ = c;
      len--;
    }
  }
  *to = 0;
}

int get_str_class_utf8 (const char *text, int len) {
  int z = -1;
  while (len > 0) {
    int c = (unsigned char) *text++;
    if (c >= 0xc2 && c < 0xe0 && len > 1 && (signed char) *text < -0x40) {
      c = ((c & 0x1f) << 6) | (*text++ & 0x3f);
      len -= 2;
      z &= is_letter_utf8[c];
    } else if (c < 0x80) {
      z &= is_letter_utf8[c];
      len--;
    } else {
      return 0;
    }
  }
  return z;
}

void lc_str (char *to, const char *from, int len) {
  if (word_split_utf8) {
    lc_str_utf8 (to, from, len);
    return;
  }
  while (len > 0) {
    *to++ = l_case[(unsigned char) *from++];
    len--;
  }
  *to = 0;
}

int get_str_class (const char *text, int len) {
  if (word_split_utf8) {
    return get_str_class_utf8 (text, len);
  }
  int i, c = -1;
  for (i = 0; i < len; i++) {
    c &= is_letter[(unsigned char) text[i]];
  }
  return c;
}

unsigned long long word_hash (const char *str, int len) {
  union {
    unsigned char data[16];
    unsigned long long hash;
  } md5_h;

  if (len < 0) { 
    len = strlen (str); 
  }

  md5 ((unsigned char *) str, len, md5_h.data);

  return md5_h.hash;
}

unsigned long long word_crc64 (const char *str, int len) {
  if (len < 0) { 
    len = strlen (str); 
  }
  return crc64 (str, len);
}

static inline void declare_sigil (int c) {
  is_letter[c] |= 0x40;
  is_letter_utf8[c] |= 0x40;
}    

static inline void declare_special (int c) {
  is_letter[c] |= 0x80;
  is_letter_utf8[c] |= 0x80;
}    

void enable_is_letter_sigils (void) {
  declare_sigil ('@');
  declare_sigil ('#');
  declare_sigil ('$');
  declare_sigil ('%');
  declare_sigil ('*');
  declare_sigil ('[');
  declare_special (']');
  declare_special ('_');
  declare_special ('-');
  declare_special ('.');
  is_letter['_'] = 4;
  sigil_word_detect = std_sigil_word_detect;
}

void enable_search_tag_sigil (void) {
  declare_sigil (0x1f);
  sigil_word_detect = std_sigil_word_detect;
}
