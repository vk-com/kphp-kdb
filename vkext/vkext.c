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
              2011-2013 Vitaliy Valtman
*/

#include "config.h"
#include "php.h"
#include "php_ini.h"
#include "vkext.h"
#include "vkext_iconv.h"
#include "vkext_flex.h"
#include "vkext_rpc.h"
#include "vkext_tl_memcache.h"
#include "vkext_schema_memcache.h"
#include "strings.h"
#include "execinfo.h"
#include "signal.h"
#include "stddef.h"

#if HAVE_ASSERT_H
#undef NDEBUG
#include <assert.h>
#endif /* HAVE_ASSERT_H */

static zend_function_entry vkext_functions[] = {
  PHP_FE(vk_hello_world, NULL)
  PHP_FE(vk_upcase, NULL)
  PHP_FE(vk_utf8_to_win, NULL)
  PHP_FE(vk_win_to_utf8, NULL)
  PHP_FE(vk_flex, NULL)
  PHP_FE(vk_whitespace_pack, NULL)
  PHP_FE(new_rpc_connection, NULL)
  PHP_FE(rpc_clean, NULL)
  PHP_FE(rpc_send, NULL)
  PHP_FE(rpc_send_noflush, NULL)
  PHP_FE(rpc_flush, NULL)
  PHP_FE(rpc_get_and_parse, NULL)
  PHP_FE(rpc_get, NULL)
  PHP_FE(rpc_get_any_qid, NULL)
  PHP_FE(rpc_parse, NULL)
  PHP_FE(store_int, NULL)
  PHP_FE(store_long, NULL)
  PHP_FE(store_string, NULL)
  PHP_FE(store_double, NULL)
  PHP_FE(store_many, NULL)
  PHP_FE(store_header, NULL)
  PHP_FE(fetch_int, NULL)
  PHP_FE(fetch_long, NULL)
  PHP_FE(fetch_double, NULL)
  PHP_FE(fetch_string, NULL)
  PHP_FE(fetch_end, NULL)
  PHP_FE(fetch_eof, NULL)  
  PHP_FE(vk_set_error_verbosity, NULL)
  PHP_FE(rpc_queue_create, NULL)
  PHP_FE(rpc_queue_empty, NULL)
  PHP_FE(rpc_queue_next, NULL)
  PHP_FE(vk_clear_stats, NULL)  
  PHP_FE(vk_rpc_memcache_get, NULL)  
  PHP_FE(vk_rpc_memcache_get_multi, NULL)  
  PHP_FE(vk_rpc_memcache_set, NULL)  
  PHP_FE(vk_rpc_memcache_add, NULL)  
  PHP_FE(vk_rpc_memcache_replace, NULL)  
  PHP_FE(vk_rpc_memcache_incr, NULL)  
  PHP_FE(vk_rpc_memcache_decr, NULL)  
  PHP_FE(vk_rpc_memcache_delete, NULL)  
  PHP_FE(rpc_tl_query, NULL)  
  PHP_FE(rpc_tl_query_result, NULL)  
  PHP_FE(vkext_prepare_stats, NULL)
  PHP_FE(tl_config_load, NULL)
  PHP_FE(tl_config_load_file, NULL)
  {NULL, NULL, NULL}
};

PHP_INI_BEGIN ()
PHP_INI_ENTRY ("tl.conffile", 0, PHP_INI_ALL, 0)
PHP_INI_ENTRY ("vkext.ping_timeout", 0, PHP_INI_ALL, 0)
PHP_INI_END ()

zend_module_entry vkext_module_entry = {
  STANDARD_MODULE_HEADER,
  VKEXT_NAME,
  vkext_functions,
	PHP_MINIT(vkext),
	PHP_MSHUTDOWN(vkext),
	PHP_RINIT(vkext),
	PHP_RSHUTDOWN(vkext),
  NULL,
  VKEXT_VERSION,
  STANDARD_MODULE_PROPERTIES
};

ZEND_GET_MODULE(vkext)

int verbosity = 0;

char upcase_char (char c) {
  if (c >= 'a' && c <= 'z') {
    return c + 'A' - 'a';
  } else {
    return c;
  }
}

int utf8_to_win_char (int c) {
  if (c < 0x80) {
    return c;
  }
  if (c == 8238 || c == 8236 || c == 8235) return 0xda; 
  switch (c & ~0xff) {
    case 0x400: return utf8_to_win_convert_0x400[c & 0xff];
    case 0x2000: return utf8_to_win_convert_0x2000[c & 0xff];
    case 0xff00: return utf8_to_win_convert_0xff00[c & 0xff];
    case 0x2100: return utf8_to_win_convert_0x2100[c & 0xff];
    case 0x000: return utf8_to_win_convert_0x000[c & 0xff];
  }
  return -1;
}

double get_double_time_since_epoch() {
  struct timeval tv;
  gettimeofday(&tv, 0);
  return tv.tv_sec + 1e-6 * tv.tv_usec;
}

#define BUFF_LEN (1 << 16)
static char buff[BUFF_LEN];
char *wptr;
//int buff_pos;

char *result_buff;
int result_buff_len;
int result_buff_pos;
#define cur_buff_len ((wptr - buff) + result_buff_pos)


void init_buff () {
  //buff_pos = 0;
  wptr = buff;
  result_buff_len = 0;
  result_buff_pos = 0;
}

void free_buff () {
  if (result_buff_len) {
    efree (result_buff);
  }
}

void realloc_buff () {
  if (!result_buff_len) {
    result_buff = emalloc (BUFF_LEN);
    result_buff_len = BUFF_LEN;
  } else {
    result_buff = erealloc (result_buff, 2 * result_buff_len);
    result_buff_len *= 2;
  }
}

void flush_buff () {
  while (result_buff_pos + (wptr - buff) > result_buff_len) {
    realloc_buff ();
  }  
  memcpy (result_buff + result_buff_pos, buff, wptr - buff);
  result_buff_pos += (wptr - buff);
  wptr = buff;  
}

char *finish_buff () {
  if (result_buff_len) {
    flush_buff ();
    return result_buff;
  } else {
    return buff;
  }
}

#define likely(x) __builtin_expect((x),1) 
#define unlikely(x) __builtin_expect((x),0)
#define min(x,y) ((x) < (y) ? (x) : (y))
void write_buff (const char *s, int l) {
  while (l > 0) {
    if (unlikely (wptr == buff + BUFF_LEN)) {
      flush_buff ();
    }
    int ll = min (l, buff + BUFF_LEN -  wptr);
    memcpy (wptr, s, ll);
    wptr += ll;
    s += ll;
    l -= ll;
  }

}

int write_buff_get_pos (void) {
  return cur_buff_len;
}

void write_buff_set_pos (int pos) {
  if (pos > cur_buff_len) {
    return;
  }
  if (pos >= result_buff_pos) {
    wptr = (pos - result_buff_pos) + buff;
    return;
  }
  //result_buff_pos -= (pos - (wptr - buff));
  result_buff_pos = pos;
  wptr = buff;
}

void write_buff_char_pos (char c, int pos) {
  if (pos > cur_buff_len) {
    return;
  }
  if (pos >= result_buff_pos) {
    *((pos - result_buff_pos) + buff) = c;
    return;
  }
  //*(result_buff + result_buff_pos - (pos - (wptr - buff))) = c;
  *(result_buff + pos) = c;
}


void write_buff_char (char c) {
  if (unlikely (wptr == buff + BUFF_LEN)) {
    flush_buff ();
  }
  *(wptr ++) = c;
}

void write_buff_char_2 (char c1, char c2) {
  if (unlikely (wptr >= buff + BUFF_LEN - 1)) {
    flush_buff ();
  }
  *(wptr ++) = c1;
  *(wptr ++) = c2;
}

void write_buff_char_3 (char c1, char c2, char c3) {
  if (unlikely (wptr >= buff + BUFF_LEN - 2)) {
    flush_buff ();
  }
  *(wptr ++) = c1;
  *(wptr ++) = c2;
  *(wptr ++) = c3;
}

void write_buff_char_4 (char c1, char c2, char c3, char c4) {
  if (unlikely (wptr >= buff + BUFF_LEN - 3)) {
    flush_buff ();
  }
  *(wptr ++) = c1;
  *(wptr ++) = c2;
  *(wptr ++) = c3;
  *(wptr ++) = c4;
}

void write_buff_long (long x) {
  if (unlikely (wptr + 25 > buff + BUFF_LEN)) {
    flush_buff ();
  }
  wptr += sprintf (wptr, "%ld", x);
}

void write_buff_double (double x) {
  if (unlikely (wptr + 100 > buff + BUFF_LEN)) {
    flush_buff ();
  }
  wptr += sprintf (wptr, "%lf", x);
}


void write_utf8_char (int c);
int utf8_to_win (const char *s, int len, int max_len, int exit_on_error) {
  int st = 0;
  int acc = 0;
  int i;
  if (max_len && len > 3 * max_len) {
    len = 3 * max_len;
  }
  for (i = 0; i < len; i++) {
    if (max_len && cur_buff_len > max_len) {
      break;
    }
    unsigned char c = s[i];
    if (c < 0x80) {
      if (st) {
        if (exit_on_error) {
          return -1;
        }
        write_buff ("?1?", 3);
      }
      write_buff_char (c);
      st = 0;
      continue;
    }
    if ((c & 0xc0) == 0x80) {
      if (!st) {
        if (exit_on_error) {
          return -1;
        }
        write_buff ("?2?", 3);
        continue;
      }
      acc <<= 6;
      acc += c - 0x80;
      if (!--st) {
        if (exit_on_error && acc < 0x80) {
          return -1;
        }
        if (acc < 0x80) {
          write_buff ("?3?", 3);
        } else {
          int d = utf8_to_win_char (acc);
          if (d != -1 && d) {
            write_buff_char (d);
          } else {
            write_buff_char ('&');
            write_buff_char ('#');
            write_buff_long (acc);
            write_buff_char (';');
          }
        }
      }
      continue;
    }
    if ((c & 0xc0) == 0xc0) {
      if (st) {
        if (exit_on_error) {
          return -1;
        }
        write_buff ("?4?", 3);
      }
      c -= 0xc0;
      st = 0;
      if (c < 32) {
        acc = c;
        st = 1;
      } else if (c < 48) {
        acc = c - 32;
        st = 2;
      } else if (c < 56) {
        acc = c - 48;
        st = 3;
      } else {
        if (exit_on_error) {
          return -1;
        }
        write_buff ("?5?", 3);
      }
    }
  }
  if (st) {
    if (exit_on_error) {
      return -1;
    }
    write_buff ("?6?", 3);
  }
  return 1;
}


void write_utf8_char (int c) {
  if (c < 128) {
    write_buff_char (c);
    return;
  }
  if (c < 0x800) {
    write_buff_char_2 (0xc0 + (c >> 6), 0x80 + (c & 63));
    return;
  }  
  if (c < 0x10000) {
    write_buff_char_3 (0xe0 + (c >> 12), 0x80 + ((c >> 6) & 63), 0x80 + (c & 63));
    return;
  }
  if (c < 0x200000) {
    write_buff_char_4 (0xf0 + (c >> 18), 0x80 + ((c >> 12) & 63), 0x80 + ((c >> 6) & 63), 0x80 + (c & 63));
    return;
  }
}

void write_char_utf8 (int c) {
  //int c = win_to_utf8_convert [_c];
  if (!c) {
    //write_buff_char_2 ('&', '#');
    //write_buff_long (c);
    //write_buff_char (';');
    return;
  }
  /*if (c < 32 || c == 34 || c == 39 || c == 60 || c == 62 || c == 8232 || c == 8233) {
    write_buff_char_2 ('&', '#');
    write_buff_long (c);
    write_buff_char (';');
    return;
  }*/
  if (c < 128) {
    write_buff_char (c);
    return;
  }
  if (c < 0x800) {
    write_buff_char_2 (0xc0 + (c >> 6), 0x80 + (c & 63));
    return;
  }  
  if (c < 0x10000) {
    write_buff_char_3 (0xe0 + (c >> 12), 0x80 + ((c >> 6) & 63), 0x80 + (c & 63));
    return;
  }
  if (c >= 0x1f000 && c <= 0x1f6c0) {
    write_buff_char_4 (0xf0 + (c >> 18), 0x80 + ((c >> 12) & 63), 0x80 + ((c >> 6) & 63), 0x80 + (c & 63));
    return;
  }
  write_buff_char_2 ('$', '#');
  write_buff_long (c);
  write_buff_char (';');
}

int win_to_utf8 (const char *s, int len) {
  int i;
  int state = 0;
  int save_pos = -1;
  int cur_num = 0;
  for (i = 0; i < len; i++) {
    if (state == 0 && s[i] == '&') {
      save_pos = write_buff_get_pos ();
      cur_num = 0;
      state ++;
    } else if (state == 1 && s[i] == '#') {
      state ++;
    } else if (state == 2 && s[i] >= '0' && s[i] <= '9') {
      if (cur_num < 0x20000) {
        cur_num = s[i] - '0' + cur_num * 10;
      }
    } else if (state == 2 && s[i] == ';') {
      state ++;
    } else {
      state = 0;
    }
    if (state == 3 && (cur_num >= 32 && cur_num != 33 && cur_num != 34 && cur_num != 36 && cur_num != 39 && cur_num != 60 && cur_num != 62 && cur_num != 92 && cur_num != 8232 && cur_num != 8233 && cur_num < 0x20000)) {
      write_buff_set_pos (save_pos);
      assert (save_pos == write_buff_get_pos ());
      write_char_utf8 (cur_num);
    } else if (state == 3 && cur_num >= 0x10000) {
      write_char_utf8 (win_to_utf8_convert[(unsigned char)(s[i])]);
      write_buff_char_pos ('$', save_pos);
    } else {
      write_char_utf8 (win_to_utf8_convert[(unsigned char)(s[i])]);
    }
    if (state == 3) {      
      state = 0;
    }
  }
  return cur_buff_len;
}

void print_backtrace (void) {
  void *buffer[64];
  int nptrs = backtrace (buffer, 64);
  write (2, "\n------- Stack Backtrace -------\n", 33);
  backtrace_symbols_fd (buffer, nptrs, 2);
  write (2, "-------------------------------\n", 32);
}

void sigsegv_debug_handler (const int sig) {
  write (2, "SIGSEGV caught, terminating program\n", 36);
  print_backtrace ();
  exit (EXIT_FAILURE);
}

void sigabrt_debug_handler (const int sig) {
  write (2, "SIGABRT caught, terminating program\n", 36);
  print_backtrace ();
  exit (EXIT_FAILURE);
}

void set_debug_handlers (void) {
  signal (SIGSEGV, sigsegv_debug_handler);
  signal (SIGABRT, sigabrt_debug_handler);
}

PHP_MINIT_FUNCTION(vkext) {
  set_debug_handlers ();
  REGISTER_INI_ENTRIES();
  rpc_on_minit (module_number);
	return SUCCESS;
}

PHP_RINIT_FUNCTION(vkext) {
  rpc_on_rinit (module_number);
	return SUCCESS;
}

PHP_MSHUTDOWN_FUNCTION(vkext) {
  UNREGISTER_INI_ENTRIES();
	return SUCCESS;
}

PHP_RSHUTDOWN_FUNCTION(vkext) {
  rpc_on_rshutdown (module_number);
	return SUCCESS;
}

PHP_FUNCTION(vk_utf8_to_win) {
  char *text;
  long text_len = 0;
  long max_len = 0;
  long exit_on_error = 0;
  if (zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "s|lb", &text, &text_len, &max_len, &exit_on_error) == FAILURE) { 
     return;
  }
  init_buff (0);
  int r = utf8_to_win (text, text_len, max_len, exit_on_error);
  write_buff_char (0);
  char *res = finish_buff ();
  if (max_len && cur_buff_len > max_len) {
    res[max_len] = 0;
  }
  char *new_res;
  if (r >= 0) {
    new_res = estrdup (res);
  } else {
    new_res = estrdup (text);
  }
  free_buff ();
  RETURN_STRING (new_res, 0);
}

PHP_FUNCTION(vk_win_to_utf8) {
  char *text;
  long text_len = 0;
  if (zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "s", &text, &text_len) == FAILURE) { 
     return;
  }
  init_buff (0);
  win_to_utf8 (text, text_len);
  write_buff_char (0);
  char *res = finish_buff ();
  char *new_res = estrdup (res);
  free_buff ();
  RETURN_STRING (new_res, 0);
}

PHP_FUNCTION(vk_upcase) {
  char *text;
  long text_length = 0;
  if (zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "s", &text, &text_length) == FAILURE) { 
     return;
  }
  char *new_text = estrdup (text);
  int i;
  for (i = 0; i < text_length; i++) {
    new_text[i] = upcase_char (new_text[i]);
  }
  RETURN_STRING(new_text, 0);
}

PHP_FUNCTION(vk_flex) {
  char *name;
  long name_len = 0;
  char *case_name;
  long case_name_len = 0;
  long sex = 0;
  char *type;
  long type_len = 0;
  long lang_id = 0;
  if (zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "sslsl", &name, &name_len, &case_name, &case_name_len, &sex, &type, &type_len, &lang_id) == FAILURE) { 
     return;
  }
  if (verbosity) {
    fprintf (stderr, "name = %s, name_len = %ld\n", name, name_len);
    fprintf (stderr, "case_name = %s, case_name_len = %ld\n", case_name, case_name_len);
    fprintf (stderr, "sex = %ld\n", sex);
    fprintf (stderr, "type = %s, type_len = %ld\n", type, type_len);
    fprintf (stderr, "lang_id = %ld\n", lang_id);
  }
  char *res = do_flex (name, name_len, case_name, case_name_len, sex, type, type_len, lang_id);
  RETURN_STRING (res, 0);
}


char ws[256] = {0,0,0,0,0,0,0,0,0,1,1,0,0,1,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,1};
//char lb[256] = {0,0,0,0,0,0,0,0,0,0,1,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0};

static inline int is_html_opt_symb (char c) {
  return (c == '<' || c == '>' || c == '(' || c == ')' || c == '{' || c == '}' || c == '/' || c== '"' || c== ':' || c== ',' || c== ';');
}
static inline int is_space (char c) {
  return ws[(unsigned char)c];
  //return (c == '\n' || c == '\r' || c == ' ' || c == '\t');
}

static inline int is_linebreak (char c) {
  //return lb[(unsigned char)c];
  return c == '\n';
}

static inline int is_pre_tag (const char *s) {
  if (s[1] == 'p') {
    return s[2] == 'r' && s[3] == 'e' && s[4] == '>';
  } else if (s[1] == 'c') {
    return s[2] == 'o' && s[3] == 'd' && s[4] == 'e' && s[5] == '>';
  } else if (s[1] == '/') {
    if (s[1] == '/') {
      return -(s[3] == 'r' && s[4] == 'e' && s[5] == '>');
    } else {
      return -(s[3] == 'o' && s[4] == 'd' && s[5] == 'e' && s[6] == '>');
    }
  }
  /*if (*(int *)s == *(int *)"<pre" || *(int *)(s + 1) == *(int *)"code") {
    return 1;
  }
  if (*(int *)(s + 1) == *(int *)"/pre" || (s[1] == '/' && *(int *)(s + 2) == *(int *)"code")) {
    return -1;
  }*/
  /*if (!strncmp (s, "<pre>", 5) || !strncmp (s, "<code>", 6)) {
    return 1;
  }
  if (!strncmp (s, "</pre>", 6) || !strncmp (s, "</code>", 7)) {
    return -1;
  }*/
  return 0;
}

PHP_FUNCTION(vk_whitespace_pack) {
  char *text, *ctext;
  long text_length = 0;
  long html_opt = 0;
  if (zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "s|l", &text, &text_length, &html_opt) == FAILURE) { 
     return;
  }
  init_buff (text_length);
  //init_buff (0);
  int level = 0;
  //int i = 0;
  ctext = text;
  char *start = text;
  //fprintf (stderr, "%lf\n", get_double_time_since_epoch ());
  //const char *ctext = text;
  while (*text) {
    if (is_space (*text) && !level) {
      int linebreak = 0;
      while (is_space (*text) && *text) {
        if (is_linebreak (*text)) {
          linebreak = 1;
        }
        text ++;
      }
      if (!html_opt || ((ctext != start && !is_html_opt_symb (*(ctext - 1))) && (*text && !is_html_opt_symb (*text)))) {
      //if (0) {
        write_buff_char (linebreak ? '\n' : ' ');
      }      
      ctext = text;
    } else {
      while (1) {
        while ((level || !is_space (*text)) && *text) {
          if (*text == '<') {
            level += is_pre_tag (text);
          }
          if (level < 0) {
            level = 1000000000;
          }       
          text ++;
        }
        if (!html_opt && *text && !is_space (*(text + 1))) {
          text ++;
          continue;
        }
        break;
      }
      write_buff (ctext, text - ctext);
    }
    ctext = text;
  }
  //fprintf (stderr, "\n");
  write_buff_char (0);
  //fprintf (stderr, "%lf\n", get_double_time_since_epoch ());
  char *res = finish_buff ();
  //fprintf (stderr, "%lf\n", get_double_time_since_epoch ());
  char *new_res = estrdup (res);
  //fprintf (stderr, "%lf\n", get_double_time_since_epoch ());
  free_buff ();
  //fprintf (stderr, "%lf\n", get_double_time_since_epoch ());
  RETURN_STRING (new_res, 0);
}


PHP_FUNCTION(vk_hello_world) {
  char *s = emalloc (4);
  s[0] = 'A';
  s[1] = 0;
  s[2] = 'A';
  s[3] = 0;
  RETURN_STRING(s, 0);
}

PHP_FUNCTION(new_rpc_connection) {
  ADD_CNT(total);
  START_TIMER(total);
	php_new_rpc_connection (INTERNAL_FUNCTION_PARAM_PASSTHRU);
  END_TIMER(total);
}

PHP_FUNCTION(rpc_clean) {
  ADD_CNT(total);
  START_TIMER(total);
	php_rpc_clean (INTERNAL_FUNCTION_PARAM_PASSTHRU);
  END_TIMER(total);
}

PHP_FUNCTION(rpc_send) {
  ADD_CNT(total);
  START_TIMER(total);
	php_rpc_send (INTERNAL_FUNCTION_PARAM_PASSTHRU);
  END_TIMER(total);
}

PHP_FUNCTION(rpc_send_noflush) {
  ADD_CNT(total);
  START_TIMER(total);
	php_rpc_send_noflush (INTERNAL_FUNCTION_PARAM_PASSTHRU);
  END_TIMER(total);
}

PHP_FUNCTION(rpc_flush) {
  ADD_CNT(total);
  START_TIMER(total);
	php_rpc_flush (INTERNAL_FUNCTION_PARAM_PASSTHRU);
  END_TIMER(total);
}

PHP_FUNCTION(rpc_get_and_parse) {
  ADD_CNT(total);
  START_TIMER(total);
	php_rpc_get_and_parse (INTERNAL_FUNCTION_PARAM_PASSTHRU);
  END_TIMER(total);
}

PHP_FUNCTION(rpc_get) {
  ADD_CNT(total);
  START_TIMER(total);
	php_rpc_get (INTERNAL_FUNCTION_PARAM_PASSTHRU);
  END_TIMER(total);
}

PHP_FUNCTION(rpc_get_any_qid) {
  ADD_CNT(total);
  START_TIMER(total);
	php_rpc_get_any_qid (INTERNAL_FUNCTION_PARAM_PASSTHRU);
  END_TIMER(total);
}

PHP_FUNCTION(rpc_parse) {
  ADD_CNT(total);
  START_TIMER(total);
	php_rpc_parse (INTERNAL_FUNCTION_PARAM_PASSTHRU);
  END_TIMER(total);
}

PHP_FUNCTION(store_int) {
  ADD_CNT(total);
  START_TIMER(total);
	php_rpc_store_int (INTERNAL_FUNCTION_PARAM_PASSTHRU);
  END_TIMER(total);
}

PHP_FUNCTION(store_long) {
  ADD_CNT(total);
  START_TIMER(total);
	php_rpc_store_long (INTERNAL_FUNCTION_PARAM_PASSTHRU);
  END_TIMER(total);
}

PHP_FUNCTION(store_string) {
  ADD_CNT(total);
  START_TIMER(total);
	php_rpc_store_string (INTERNAL_FUNCTION_PARAM_PASSTHRU);
  END_TIMER(total);
}

PHP_FUNCTION(store_double) {
  ADD_CNT(total);
  START_TIMER(total);
	php_rpc_store_double (INTERNAL_FUNCTION_PARAM_PASSTHRU);
  END_TIMER(total);
}

PHP_FUNCTION(store_many) {
  ADD_CNT(total);
  START_TIMER(total);
	php_rpc_store_many (INTERNAL_FUNCTION_PARAM_PASSTHRU);
  END_TIMER(total);
}

PHP_FUNCTION(store_header) {
  ADD_CNT(total);
  START_TIMER(total);
	php_rpc_store_header (INTERNAL_FUNCTION_PARAM_PASSTHRU);
  END_TIMER(total);
}

PHP_FUNCTION(fetch_int) {
  ADD_CNT(total);
  START_TIMER(total);
	php_rpc_fetch_int (INTERNAL_FUNCTION_PARAM_PASSTHRU);
  END_TIMER(total);
}

PHP_FUNCTION(fetch_long) {
  ADD_CNT(total);
  START_TIMER(total);
	php_rpc_fetch_long (INTERNAL_FUNCTION_PARAM_PASSTHRU);
  END_TIMER(total);
}

PHP_FUNCTION(fetch_double) {
  ADD_CNT(total);
  START_TIMER(total);
	php_rpc_fetch_double (INTERNAL_FUNCTION_PARAM_PASSTHRU);
  END_TIMER(total);
}

PHP_FUNCTION(fetch_string) {
  ADD_CNT(total);
  START_TIMER(total);
	php_rpc_fetch_string (INTERNAL_FUNCTION_PARAM_PASSTHRU);
  END_TIMER(total);
}

PHP_FUNCTION(fetch_end) {
  ADD_CNT(total);
  START_TIMER(total);
	php_rpc_fetch_end (INTERNAL_FUNCTION_PARAM_PASSTHRU);
  END_TIMER(total);
}

PHP_FUNCTION(fetch_eof) {
  ADD_CNT(total);
  START_TIMER(total);
	php_rpc_fetch_end (INTERNAL_FUNCTION_PARAM_PASSTHRU);
  END_TIMER(total);
}

PHP_FUNCTION(rpc_queue_create) {
  ADD_CNT(total);
  START_TIMER(total);
	php_rpc_queue_create (INTERNAL_FUNCTION_PARAM_PASSTHRU);
  END_TIMER(total);
}

PHP_FUNCTION(rpc_queue_empty) {
  ADD_CNT(total);
  START_TIMER(total);
	php_rpc_queue_empty (INTERNAL_FUNCTION_PARAM_PASSTHRU);
  END_TIMER(total);
}

PHP_FUNCTION(rpc_queue_next) {
  ADD_CNT(total);
  START_TIMER(total);
	php_rpc_queue_next (INTERNAL_FUNCTION_PARAM_PASSTHRU);
  END_TIMER(total);
}

PHP_FUNCTION(vk_clear_stats) {
  memset (&stats, 0, offsetof (struct stats,malloc));
}

PHP_FUNCTION(vk_rpc_memcache_get) {
  ADD_CNT(total);
  START_TIMER(total);
	vk_memcache_get (INTERNAL_FUNCTION_PARAM_PASSTHRU);
  END_TIMER(total);
}

PHP_FUNCTION(vk_rpc_memcache_get_multi) {
  ADD_CNT(total);
  START_TIMER(total);
	vk_memcache_multiget (INTERNAL_FUNCTION_PARAM_PASSTHRU);
  END_TIMER(total);
}

PHP_FUNCTION(vk_rpc_memcache_set) {
  ADD_CNT(total);
  START_TIMER(total);
	vk_memcache_store (0, INTERNAL_FUNCTION_PARAM_PASSTHRU);
  END_TIMER(total);
}

PHP_FUNCTION(vk_rpc_memcache_add) {
  ADD_CNT(total);
  START_TIMER(total);
	vk_memcache_store (1, INTERNAL_FUNCTION_PARAM_PASSTHRU);
  END_TIMER(total);
}

PHP_FUNCTION(vk_rpc_memcache_replace) {
  ADD_CNT(total);
  START_TIMER(total);
	vk_memcache_store (2, INTERNAL_FUNCTION_PARAM_PASSTHRU);
  END_TIMER(total);
}

PHP_FUNCTION(vk_rpc_memcache_incr) {
  ADD_CNT(total);
  START_TIMER(total);
	vk_memcache_incr (0, INTERNAL_FUNCTION_PARAM_PASSTHRU);
  END_TIMER(total);
}

PHP_FUNCTION(vk_rpc_memcache_decr) {
  ADD_CNT(total);
  START_TIMER(total);
	vk_memcache_incr (1, INTERNAL_FUNCTION_PARAM_PASSTHRU);
  END_TIMER(total);
}

PHP_FUNCTION(vk_rpc_memcache_delete) {
  ADD_CNT(total);
  START_TIMER(total);
	vk_memcache_delete (INTERNAL_FUNCTION_PARAM_PASSTHRU);
  END_TIMER(total);
}

PHP_FUNCTION(rpc_tl_query) {
  ADD_CNT(total);
  START_TIMER(total);
	vk_memcache_query (INTERNAL_FUNCTION_PARAM_PASSTHRU);
  END_TIMER(total);
}

PHP_FUNCTION(rpc_tl_query_result) {
  ADD_CNT(total);
  START_TIMER(total);
	vk_memcache_query_result (INTERNAL_FUNCTION_PARAM_PASSTHRU);
  END_TIMER(total);
}

extern long error_verbosity;
PHP_FUNCTION(vk_set_error_verbosity) {
  ADD_CNT(total);
  START_TIMER(total);
  long t;
  if (zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "l", &t) == FAILURE) { 
     END_TIMER(total);
     return;
  }
  error_verbosity = t;
  END_TIMER(total);
  RETURN_TRUE;
}

PHP_FUNCTION(tl_config_load) {
  ADD_CNT(total);
  START_TIMER(total);
  php_tl_config_load (INTERNAL_FUNCTION_PARAM_PASSTHRU);
  END_TIMER(total);
}

PHP_FUNCTION(tl_config_load_file) {
  ADD_CNT(total);
  START_TIMER(total);
  php_tl_config_load_file (INTERNAL_FUNCTION_PARAM_PASSTHRU);
  END_TIMER(total);
}

PHP_FUNCTION(vkext_prepare_stats) {
  ADD_CNT(total);
  START_TIMER(total);
	php_vk_prepare_stats (INTERNAL_FUNCTION_PARAM_PASSTHRU);
  END_TIMER(total);
}
