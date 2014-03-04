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

    Copyright 2009-2010 Vkontakte Ltd
              2009-2010 Nikolai Durov
              2009-2010 Andrei Lopatin
*/

#include <string.h>
#include <assert.h>
#include "translit.h"

static inline char *trans_letter (char *str, int *len) {
  *len = 1;
   
  switch (*str) {
  case 0:  *len = 0;  return 0;
  case 'À': return "A";
  case 'Á': return "B"; 
  case 'Â': return "V"; 
  case 'Ã': return "G"; 
  case (char) 0xa5: return "G"; 
  case 'Ä': return "D"; 
  case 'Å': return "Ye"; 
  case 'ª': return "Ye"; 
  case '¨': return "Yo"; 
  case 'Æ': return "Zh"; 
  case 'Ç': return "Z"; 
  case 'È': return "I"; 
  case '¯': return "Yi"; 
  case (char) 0xb2: return "I"; 
  case 'É': return "J"; 
  case 'Ê': if (str[1] == 'ñ') { *len = 2;  return "X"; }
            else { return "K"; } 
  case 'Ë': return "L"; 
  case 'Ì': return "M"; 
  case 'Í': return "N"; 
  case 'Î': return "O"; 
  case 'Ï': return "P"; 
  case 'Ð': return "R"; 
  case 'Ñ': return "S"; 
  case 'Ò': return "T"; 
  case 'Ó': return "U"; 
  case 'Ô': return "F"; 
  case 'Õ': return "H"; 
  case 'Ö': return "Ts"; 
  case '×': return "Ch"; 
  case 'Ø': return "Sh"; 
  case 'Ù': return "Sh"; 
  case 'Ý': return "E"; 
  case 'Þ': return "Yu"; 
  case 'ß': return "Ya"; 
  case 'à': return "a"; 
  case 'á': return "b"; 
  case 'â': return "v"; 
  case 'ã': return "g"; 
  case (char) 0xb4: return "g"; 
  case 'ä': return "d"; 
  case 'å': return "e"; 
  case 'º': return "ye"; 
  case '¸': return "yo"; 
  case 'æ': return "zh"; 
  case 'ç': return "z"; 
  case 'è': if (str[1] == 'ÿ') { 
	      *len = 2;  
	      return "ia"; 
	    } else if (str[1] == 'é') {
	      *len = 2;
	      return "y"; 
	    } else { 
	      return "i"; 
	    } 
  case 'é': return "y"; 
  case '¿': return "yi"; 
  case (char) 0xb3: return "i"; 
  case 'ê': if (str[1] == 'ñ') { *len = 2;  return "x"; }
            else { return "k"; } 
  case 'ë': return "l"; 
  case 'ì': return "m"; 
  case 'í': return "n"; 
  case 'î': return "o"; 
  case 'ï': return "p"; 
  case 'ð': return "r"; 
  case 'ñ': return "s"; 
  case 'ò': return "t"; 
  case 'ó': return "u"; 
  case 'ô': return "f"; 
  case 'õ': return "h"; 
  case 'ö': return "ts"; 
  case '÷': return "ch"; 
  case 'ø': return "sh"; 
  case 'ù': return "sh"; 
  case 'ú': return ""; 
  case 'û': return "y"; 
  case 'ü': if (str[1] == 'å') { *len = 2;  return "ye"; }
            else { return ""; } 
  case 'ý': return "e"; 
  case 'þ': return "yu"; 
  case 'ÿ': return "ya"; 
  default: return 0;
  }
}

char *translit_str (char *buffer, int buff_size, char *str, int len) {
  char *ptr = str, *out = buffer, *str_e = str + len, *out_e = buffer + buff_size;
  while (ptr < str_e && *ptr) {
    int a = 0, b;
    char *tr = trans_letter (ptr, &a);
    if (tr) {
      b = strlen (tr);
    } else {
      b = 1;
      tr = ptr;
    }
    assert (a > 0 && b >= 0);
    if (out + b > out_e) {
      *out = 0;
      return buffer;
    }
    memcpy (out, tr, b);
    out += b;
    ptr += a;
  }
  *out = 0;
  return buffer;    
}

