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

#pragma once

#include "kphp_core.h"

#include "../php_script.h"

extern int (*http_load_long_query) (char *buf, int min_len, int max_len);

extern void (*http_set_result) (int return_code, const char *headers, int headers_len, const char *body, int body_len, int exit_code);

extern string_buffer *coub;//TODO static


void f$ob_clean (void);

bool f$ob_end_clean (void);

OrFalse <string> f$ob_get_clean (void);

string f$ob_get_contents (void);

void f$ob_start (const string &callback = string());

void f$header (const string &str, bool replace = true, int http_response_code = 0);

void f$setcookie (const string &name, const string &value, int expire = 0, const string &path = string(), const string &domain = string(), bool secure = false, bool http_only = false);

void f$setrawcookie (const string &name, const string &value, int expire = 0, const string &path = string(), const string &domain = string(), bool secure = false, bool http_only = false);

void f$register_shutdown_function (var (*f) (void));

void finish (int exit_code);

bool f$exit (const var &v = 0);

bool f$die (const var &v = 0);


OrFalse <int> f$ip2long (const string &ip);

OrFalse <string> f$ip2ulong (const string &ip);

string f$long2ip (int num);

template <class T>
inline string f$long2ip (const T &v);//shut up warning on converting to int

OrFalse <array <string> > f$gethostbynamel (const string &name);

OrFalse <string> f$inet_pton (const string &address);


int print (const char *s);

int print (const char *s, int s_len);

int print (const string &s);

int print (string_buffer &sb);

int dbg_echo (const char *s);

int dbg_echo (const char *s, int s_len);

int dbg_echo (const string &s);

int dbg_echo (string_buffer &sb);


bool f$get_magic_quotes_gpc (void);


string f$php_sapi_name (void);
OrFalse <array <string> > f$get_interfaces (bool ipv6 = false);


extern var v$_SERVER;
extern var v$_GET;
extern var v$_POST;
extern var v$_FILES;
extern var v$_COOKIE;
extern var v$_REQUEST;
extern var v$_ENV;

const int UPLOAD_ERR_OK = 0;
const int UPLOAD_ERR_INI_SIZE = 1;
const int UPLOAD_ERR_FORM_SIZE = 2;
const int UPLOAD_ERR_PARTIAL = 3;
const int UPLOAD_ERR_NO_FILE = 4;
const int UPLOAD_ERR_NO_TMP_DIR = 6;
const int UPLOAD_ERR_CANT_WRITE = 7;
const int UPLOAD_ERR_EXTENSION = 8;


bool f$is_uploaded_file (const string &filename);

bool f$move_uploaded_file (const string &oldname, const string &newname);

void f$parse_multipart (const string &post, const string &boundary);


void init_superglobals (php_query_data *data);


extern "C" {
void ini_set (const char *key, const char *value);

void read_engine_tag (const char *file_name);
}

bool f$ini_set (const string &s, const string &value);

OrFalse <string> f$ini_get (const string &s);


void init_static (void);

void free_static (void);

/*
 *
 *     IMPLEMENTATION
 *
 */

template <class T>
string f$long2ip (const T &v) {
  return f$long2ip (f$intval (v));
}
