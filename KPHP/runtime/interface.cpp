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

#include "interface.h"

#include <arpa/inet.h>
#include <netdb.h>

#include "array_functions.h"
#include "bcmath.h"
#include "datetime.h"
#include "drivers.h"
#include "exception.h"
#include "files.h"
#include "openssl.h"
#include "rpc.h"
#include "string_functions.h"
#include "url.h"
#include "zlib.h"

static enum {QUERY_TYPE_NONE, QUERY_TYPE_CONSOLE, QUERY_TYPE_HTTP, QUERY_TYPE_RPC} query_type;

static const string HTTP_DATE ("D, d M Y H:i:s \\G\\M\\T", 21);

static const int OB_MAX_BUFFERS = 50;
static int ob_cur_buffer;

static string_buffer oub[OB_MAX_BUFFERS];
string_buffer *coub;
static int http_need_gzip;

void f$ob_clean (void) {
  coub->clean();
}

bool f$ob_end_clean (void) {
  if (ob_cur_buffer == 0) {
    return false;
  }

  coub = &oub[--ob_cur_buffer];
  return true;
}

OrFalse <string> f$ob_get_clean (void) {
  if (ob_cur_buffer == 0) {
    return false;
  }

  string result = coub->str();
  coub = &oub[--ob_cur_buffer];

  return result;
}

string f$ob_get_contents (void) {
  return coub->str();
}

void f$ob_start (const string &callback) {
  if (ob_cur_buffer + 1 == OB_MAX_BUFFERS) {
    php_warning ("Maximum nested level of output buffering reached. Can't do ob_start(%s)", callback.c_str());
    return;
  }
  if (!callback.empty()) {
    if (ob_cur_buffer == 0 && callback == string ("ob_gzhandler", 12)) {
      http_need_gzip |= 4;
    } else {
      php_critical_error ("unsupported callback %s at buffering level %d", callback.c_str(), ob_cur_buffer + 1);
    }
  }

  coub = &oub[++ob_cur_buffer];
  coub->clean();
}

static int return_code;
static char headers_storage[sizeof (array <string>)];
static array <string> *headers = reinterpret_cast <array <string> *> (headers_storage);
static long long header_last_query_num = -1;

static void header (const char *str, int str_len, bool replace = true, int http_response_code = 0) {
  if (dl::query_num != header_last_query_num) {
    new (headers_storage) array <string>();
    header_last_query_num = dl::query_num;
  }

  if (str_len >= 10 && !strncasecmp (str, "HTTP/1.", 7)) {
    sscanf (str + 9, "%d", &return_code);
  } else {
    if (str_len >= 10 && !strncasecmp (str, "Location:", 9)) {
      return_code = 302;
    }

    const char *p = strchr (str, ':');
    if (p == NULL) {
      php_warning ("Wrong header line specified: \"%s\"", str);
      return;
    }
    string name = f$strtolower (f$trim (string (str, (dl::size_type)(p - str))));
    string value = string (str_len + 2, false);
    memcpy (value.buffer(), str, str_len);
    value[str_len] = '\r';
    value[str_len + 1] = '\n';

    if (replace || !headers->has_key (name)) {
      headers->set_value (name, value);
    } else {
      (*headers)[name].append (value);
    }
  }

  if (str_len && http_response_code > 0) {
    return_code = http_response_code;
  }
}

void f$header (const string &str, bool replace, int http_response_code) {
  header (str.c_str(), (int)str.size(), replace, http_response_code);
}

void f$setrawcookie (const string &name, const string &value, int expire, const string &path, const string &domain, bool secure, bool http_only) {
  string date = f$gmdate (HTTP_DATE, expire);

  static_SB_spare.clean();
  static_SB_spare + "Set-Cookie: " + name + '=';
  if (value.empty()) {
    static_SB_spare += "DELETED; expires=Thu, 01 Jan 1970 00:00:01 GMT";
  } else {
    static_SB_spare += value;

    if (expire != 0) {
      static_SB_spare + "; expires=" + date;
    }
  }
  if (!path.empty()) {
    static_SB_spare + "; path=" + path;
  }
  if (!domain.empty()) {
    static_SB_spare + "; domain=" + domain;
  }
  if (secure) {
    static_SB_spare + "; secure";
  }
  if (http_only) {
    static_SB_spare + "; HttpOnly";
  }
  header (static_SB_spare.c_str(), (int)static_SB_spare.size(), false);
}

void f$setcookie (const string &name, const string &value, int expire, const string &path, const string &domain, bool secure, bool http_only) {
  f$setrawcookie (name, f$urlencode (value), expire, path, domain, secure, http_only);
}

static inline const char *http_get_error_msg_text (int *code) {
  if (*code == 200) {
    return "OK";
  }
  switch (*code) {
    case 201: return "Created";
    case 202: return "Accepted";
    case 204: return "No Content";
    case 206: return "Partial Content";
    case 301: return "Moved Permanently";
    case 302: return "Found";
    case 303: return "See Other";
    case 304: return "Not Modified";
    case 307: return "Temporary Redirect";
    case 400: return "Bad Request";
    case 401: return "Unauthorized";
    case 403: return "Forbidden";
    case 404: return "Not Found";
    case 405: return "Method Not Allowed";
    case 406: return "Not Acceptable";
    case 408: return "Request Timeout";
    case 411: return "Length Required";
    case 413: return "Request Entity Too Large";
    case 414: return "Request-URI Too Long";
    case 418: return "I'm a teapot";
    case 480: return "Temporarily Unavailable";
    case 501: return "Not Implemented";
    case 502: return "Bad Gateway";
    case 503: return "Service Unavailable";
    default: *code = 500;
  }
  return "Internal Server Error";
}

static const string_buffer *get_headers (int content_length) {//can't use static_SB, returns pointer to static_SB_spare
  string date = f$gmdate (HTTP_DATE);
  static_SB_spare.clean() + "Date: " + date;
  header (static_SB_spare.c_str(), (int)static_SB_spare.size());

  static_SB_spare.clean() + "Content-Length: " + content_length;
  header (static_SB_spare.c_str(), (int)static_SB_spare.size());

  static_SB_spare.clean();
  const char *message = http_get_error_msg_text (&return_code);
  static_SB_spare + "HTTP/1.1 " + return_code + " " + message + "\r\n";

  php_assert (dl::query_num == header_last_query_num);

  const array <string> *arr = headers;
  for (array <string>::const_iterator p = arr->begin(); p != arr->end(); ++p) {
    static_SB_spare += p.get_value();
  }
  static_SB_spare += "\r\n";

  return &static_SB_spare;
}

void (*http_set_result) (int return_code, const char *headers, int headers_len, const char *body, int body_len, int exit_code) = NULL;

var (*shutdown_function) (void);
static bool finished;

void f$register_shutdown_function (var (*f) (void)) {
  shutdown_function = f;
}

void finish (int exit_code) {
  if (!finished && shutdown_function) {
    finished = true;
    shutdown_function();
  }

  php_assert (ob_cur_buffer >= 0);
  int first_not_empty_buffer = 0;
  while (first_not_empty_buffer < ob_cur_buffer && (int)oub[first_not_empty_buffer].size() == 0) {
    first_not_empty_buffer++;
  }

  for (int i = first_not_empty_buffer + 1; i <= ob_cur_buffer; i++) {
    oub[first_not_empty_buffer].append (oub[i].c_str(), oub[i].size());
  }

  switch (query_type) {
    case QUERY_TYPE_CONSOLE: {
      fflush (stderr);

      write_safe (1, oub[first_not_empty_buffer].buffer(), oub[first_not_empty_buffer].size());

      free_static();
      exit (exit_code);
      //unreachable
      break;
    }
    case QUERY_TYPE_HTTP: {
      php_assert (http_set_result != NULL);

      const string_buffer *compressed;
      if ((http_need_gzip & 5) == 5) {
        header ("Content-Encoding: gzip", 22, true);
        compressed = zlib_encode (oub[first_not_empty_buffer].c_str(), oub[first_not_empty_buffer].size(), 6, ZLIB_ENCODE);
      } else if ((http_need_gzip & 6) == 6) {
        header ("Content-Encoding: deflate", 25, true);
        compressed = zlib_encode (oub[first_not_empty_buffer].c_str(), oub[first_not_empty_buffer].size(), 6, ZLIB_COMPRESS);
      } else {
        compressed = &oub[first_not_empty_buffer];
      }

      const string_buffer *headers = get_headers (compressed->size());
      http_set_result (return_code, headers->buffer(), headers->size(), compressed->buffer(), compressed->size(), exit_code);
      //unreachable
      break;
    }
    case QUERY_TYPE_RPC: {
      //we need to finish script. TODO
      php_assert (http_set_result != NULL);
      http_set_result (0, NULL, 0, NULL, 0, 0);
      //unreachable
      break;
    }
    default:
      php_assert (0);
  }

  php_assert (0);
}

bool f$exit (const var &v) {
  if (v.is_string()) {
    *coub += v;
    finish (0);
  } else {
    finish (v.to_int());
  }
  return true;
}

bool f$die (const var &v) {
  return f$exit (v);
}


OrFalse <int> f$ip2long (const string &ip) {
  struct in_addr result;
  if (inet_pton (AF_INET, ip.c_str(), &result) != 1) {
    return false;
  }
  return (int)ntohl (result.s_addr);
}

OrFalse <string> f$ip2ulong (const string &ip) {
  struct in_addr result;
  if (inet_pton (AF_INET, ip.c_str(), &result) != 1) {
    return false;
  }

  char buf[25];
  int len = sprintf (buf, "%u", ntohl (result.s_addr));
  return string (buf, len);
}

string f$long2ip (int num) {
  static_SB.clean().reserve (100);
  for (int i = 3; i >= 0; i--) {
    static_SB += (num >> (i * 8)) & 255;
    if (i) {
      static_SB.append_char ('.');
    }
  }
  return static_SB.str();
}

OrFalse <array <string> > f$gethostbynamel (const string &name) {
  dl::enter_critical_section();//OK
  struct hostent *hp = gethostbyname (name.c_str());
  if (hp == NULL || hp->h_addr_list == NULL) {
    dl::leave_critical_section();
    return false;
  }
  dl::leave_critical_section();

  array <string> result;
  for (int i = 0; hp->h_addr_list[i] != 0; i++) {
    dl::enter_critical_section();//OK
    const char *ip = inet_ntoa (*(struct in_addr *)hp->h_addr_list[i]);
    dl::leave_critical_section();
    result.push_back (string (ip, (dl::size_type)strlen (ip)));
  }

  return result;
}

OrFalse <string> f$inet_pton (const string &address) {
  int af, size;
  if (strchr (address.c_str(), ':')) {
    af = AF_INET6;
    size = 16;
  } else if (strchr (address.c_str(), '.')) {
    af = AF_INET;
    size = 4;
  } else {
    php_warning ("Unrecognized address \"%s\"", address.c_str());
    return false;
  }

  char buffer[17] = {0};
  dl::enter_critical_section();//OK
  if (inet_pton (af, address.c_str(), buffer) <= 0) {
    dl::leave_critical_section();
    php_warning ("Unrecognized address \"%s\"", address.c_str());
    return false;
  }
  dl::leave_critical_section();

  return string (buffer, size);
}


int print (const char *s) {
  *coub += s;
  return 1;
}

int print (const char *s, int s_len) {
  coub->append (s, s_len);
  return 1;
}

int print (const string &s) {
  *coub += s;
  return 1;
}

int print (string_buffer &sb) {
  coub->append (sb.c_str(), sb.size());
  return 1;
}

int dbg_echo (const char *s) {
  fprintf (stderr, "%s", s);
  return 1;
}

int dbg_echo (const char *s, int s_len) {
  fwrite (s, s_len, 1, stderr);
  return 1;
}

int dbg_echo (const string &s) {
  fwrite (s.c_str(), s.size(), 1, stderr);
  return 1;
}

int dbg_echo (string_buffer &sb) {
  fwrite (sb.c_str(), sb.size(), 1, stderr);
  return 1;
}


bool f$get_magic_quotes_gpc (void) {
  return false;
}


string f$php_sapi_name (void) {
  return string ("Kitten PHP", 10);
}


var v$_SERVER  __attribute__ ((weak));
var v$_GET     __attribute__ ((weak));
var v$_POST    __attribute__ ((weak));
var v$_FILES   __attribute__ ((weak));
var v$_COOKIE  __attribute__ ((weak));
var v$_REQUEST __attribute__ ((weak));
var v$_ENV     __attribute__ ((weak));


static int http_load_long_query_dummy (char *buf, int min_len, int max_len) {
  php_critical_error ("http_load_long_query_dummy called");
  return 0;
}

int (*http_load_long_query) (char *buf, int min_len, int max_len) = http_load_long_query_dummy;


static char uploaded_files_storage[sizeof (array <int>)];
static array <int> *uploaded_files = reinterpret_cast <array <int> *> (uploaded_files_storage);
static long long uploaded_files_last_query_num = -1;

static const int MAX_FILES = 100;

bool f$is_uploaded_file (const string &filename) {
  return (dl::query_num == uploaded_files_last_query_num && uploaded_files->get_value (filename) == 1);
}

bool f$move_uploaded_file (const string &oldname, const string &newname) {
  if (!f$is_uploaded_file (oldname)) {
    return false;
  }

  dl::enter_critical_section();//NOT OK: uploaded_files
  if (f$rename (oldname, newname)) {
    uploaded_files->unset (oldname);
    dl::leave_critical_section();
    return true;
  }
  dl::leave_critical_section();

  return false;
}


class post_reader {
  char *buf;
  int post_len;
  int buf_pos;
  int buf_len;

  const string boundary;

  post_reader (const post_reader &);//DISABLE copy constructor
  post_reader operator = (const post_reader &);//DISABLE copy assignment

public:
  post_reader (const char *post, int post_len, const string &boundary): post_len (post_len), buf_pos (0), boundary (boundary) {
    if (post == NULL) {
      buf = php_buf;
      buf_len = 0;
    } else {
      buf = (char *)post;
      buf_len = post_len;
    }
  }

  int operator [] (int i) {
    php_assert (i >= buf_pos);
    php_assert (i <= post_len);
    if (i >= post_len) {
      return 0;
    }

    i -= buf_pos;
    while (i >= buf_len) {
      int left = post_len - buf_pos - buf_len;
      int chunk_size = (int)boundary.size() + 65536 + 10;
//      fprintf (stderr, "Load at pos %d. i = %d, buf_len = %d, left = %d, chunk_size = %d\n", i + buf_pos, i, buf_len, left, chunk_size);
      if (buf_len > 0) {
        int to_leave = chunk_size;
        int to_erase = buf_len - to_leave;

        php_assert (left > 0);
        php_assert (to_erase >= to_leave);

        memcpy (buf, buf + to_erase, to_leave);
        buf_pos += to_erase;
        i -= to_erase;

        buf_len = to_leave + http_load_long_query (buf + to_leave, min (to_leave, left), min (PHP_BUF_LEN - to_leave, left));
      } else {
        buf_len = http_load_long_query (buf, min (2 * chunk_size, left), min (PHP_BUF_LEN, left));
      }
    }

    return buf[i];
  }

  bool is_boundary (int i) {
    php_assert (i >= buf_pos);
    php_assert (i <= post_len);
    if (i >= post_len) {
      return true;
    }

    if (i > 0) {
      if ((*this)[i] == '\r') {
        i++;
      }
      if ((*this)[i] == '\n') {
        i++;
      } else {
        return false;
      }
    }
    if ((*this)[i] == '-' && (*this)[i + 1] == '-') {
      i += 2;
    } else {
      return false;
    }

    if (i + (int)boundary.size() > post_len) {
      return false;
    }

    if (i - buf_pos + (int)boundary.size() <= buf_len) {
      return !memcmp (buf + i - buf_pos, boundary.c_str(), boundary.size());
    }

    for (int j = 0; j < (int)boundary.size(); j++) {
      if ((*this)[i + j] != boundary[j]) {
        return false;
      }
    }
    return true;
  }

  int upload_file (const string &file_name, int &pos, int max_file_size) {
    php_assert (pos > 0 && buf_len > 0 && buf_pos <= pos && pos <= post_len);

    if (pos == post_len) {
      return -UPLOAD_ERR_PARTIAL;
    }

    if (!f$is_writeable (file_name)) {
      return -UPLOAD_ERR_CANT_WRITE;
    }

    dl::enter_critical_section();//OK
    int file_fd = open_safe (file_name.c_str(), O_WRONLY | O_TRUNC, 0644);
    if (file_fd < 0) {
      dl::leave_critical_section();
      return -UPLOAD_ERR_NO_FILE;
    }

    struct stat stat_buf;
    if (fstat (file_fd, &stat_buf) < 0) {
      close_safe (file_fd);
      dl::leave_critical_section();
      return -UPLOAD_ERR_CANT_WRITE;
    }
    dl::leave_critical_section();

    php_assert (S_ISREG (stat_buf.st_mode));

    if (buf_len == post_len) {
      int i = pos;
      while (!is_boundary (i)) {
        i++;
      }
      if (i == post_len) {
        dl::enter_critical_section();//OK
        close_safe (file_fd);
        dl::leave_critical_section();
        return -UPLOAD_ERR_PARTIAL;
      }

      int file_size = i - pos;
      if (file_size > max_file_size) {
        dl::enter_critical_section();//OK
        close_safe (file_fd);
        dl::leave_critical_section();
        return -UPLOAD_ERR_FORM_SIZE;
      }

      dl::enter_critical_section();//OK
      if (write_safe (file_fd, buf + pos, (size_t)file_size) < (ssize_t)file_size) {
        file_size = -UPLOAD_ERR_CANT_WRITE;
      }

      close_safe (file_fd);
      dl::leave_critical_section();
      return file_size;
    } else {
      long long file_size = 0;
      const char *s;

      while (true) {
        (*this)[pos];
        int i = pos - buf_pos;

        while (true) {
          php_assert (0 <= i && i <= buf_len);

          s = static_cast <const char *>(memmem (buf + i, buf_len - i, boundary.c_str(), boundary.size()));
          if (s == NULL) {
            break;
          } else {
            int r = s - buf;
            if (r > i + 2 && buf[r - 1] == '-' && buf[r - 2] == '-' && buf[r - 3] == '\n') {
              r -= 3;
              if (r > i && buf[r - 1] == '\r') {
                r--;
              }

              s = buf + r;
              break;
            } else {
              i = r + 1;
              continue;
            }
          }
        }
        if (s != NULL) {
          break;
        }

        int left = post_len - buf_pos - buf_len;
        int to_leave = (int)boundary.size() + 10;
        int to_erase = buf_len - to_leave;
        int to_write = to_erase - (pos - buf_pos);

//        fprintf (stderr, "Load at pos %d. buf_len = %d, left = %d, to_leave = %d, to_erase = %d, to_write = %d.\n", buf_len + buf_pos, buf_len, left, to_leave, to_erase, to_write);

        if (left == 0) {
          dl::enter_critical_section();//OK
          close_safe (file_fd);
          dl::leave_critical_section();
          return -UPLOAD_ERR_PARTIAL;
        }
        file_size += to_write;
        if (file_size > max_file_size) {
          dl::enter_critical_section();//OK
          close_safe (file_fd);
          dl::leave_critical_section();
          return -UPLOAD_ERR_FORM_SIZE;
        }

        php_assert (to_erase >= to_leave);

        dl::enter_critical_section();//OK
        if (write_safe (file_fd, buf + pos - buf_pos, (size_t)to_write) < (ssize_t)to_write) {
          close_safe (file_fd);
          dl::leave_critical_section();
          return -UPLOAD_ERR_CANT_WRITE;
        }
        dl::leave_critical_section();

        memcpy (buf, buf + to_erase, to_leave);
        buf_pos += to_erase;
        pos += to_write;

        buf_len = to_leave + http_load_long_query (buf + to_leave, min (PHP_BUF_LEN - to_leave, left), min (PHP_BUF_LEN - to_leave, left));
      }

      php_assert (s != NULL);

      dl::enter_critical_section();//OK
      int to_write = (int)(s - (buf + pos - buf_pos));
      if (write_safe (file_fd, buf + pos - buf_pos, (size_t)to_write) < (ssize_t)to_write) {
        close_safe (file_fd);
        dl::leave_critical_section();
        return -UPLOAD_ERR_CANT_WRITE;
      }
      pos += to_write;

      close_safe (file_fd);
      dl::leave_critical_section();

      return (int)(file_size + to_write);
    }
  }
};

static int parse_multipart_one (post_reader &data, int i) {
  string content_type ("text/plain", 10);
  string name;
  string filename;
  int max_file_size = INT_MAX;
  while (!data.is_boundary (i) && 33 <= data[i] && data[i] <= 126) {
    int j;
    string header_name;
    for (j = i; !data.is_boundary (j) && 33 <= data[j] && data[j] <= 126 && data[j] != ':'; j++) {
      header_name.push_back (data[j]);
    }
    if (data[j] != ':') {
      return j;
    }

    header_name = f$strtolower (header_name);
    i = j + 1;

    string header_value;
    do {
      while (!data.is_boundary (i + 1) && (data[i] != '\r' || data[i + 1] != '\n') && data[i] != '\n') {
        header_value.push_back (data[i++]);
      }
      if (data[i] == '\r') {
        i++;
      }
      if (data[i] == '\n') {
        i++;
      }

      if (data.is_boundary (i) || (33 <= data[i] && data[i] <= 126) || (data[i] == '\r' && data[i + 1] == '\n') || data[i] == '\n') {
        break;
      }
    } while (1);
    header_value = f$trim (header_value);

    if (!strcmp (header_name.c_str(), "content-disposition")) {
      if (strncmp (header_value.c_str(), "form-data;", 10)) {
        return i;
      }
      const char *p = header_value.c_str() + 10;
      while (1) {
        while (*p && *p == ' ') {
          p++;
        }
        if (*p == 0) {
          break;
        }
        const char *p_end = p;
        while (*p_end && *p_end != '=') {
          p_end++;
        }
        if (*p_end == 0) {
          break;
        }
        const string key = f$trim (string (p, (dl::size_type)(p_end - p)));
        p = ++p_end;
        while (*p_end && *p_end != ';') {
          p_end++;
        }
        string value = f$trim (string (p, (dl::size_type)(p_end - p)));
        if ((int)value.size() > 1 && value[0] == '"' && value[value.size() - 1] == '"') {
          value.assign (value, 1, value.size() - 2);
        }
        p = p_end;
        if (*p) {
          p++;
        }

        if (!strcmp (key.c_str(), "name")) {
          name = value;
        } else if (!strcmp (key.c_str(), "filename")) {
          filename = value;
        } else {
//          fprintf (stderr, "Unknown key %s\n", key.c_str());
        }
      }
    } else if (!strcmp (header_name.c_str(), "content-type")) {
      content_type = f$strtolower (header_value);
    } else if (!strcmp (header_name.c_str(), "max-file-size")) {
      max_file_size = header_value.to_int();
    } else {
//      fprintf (stderr, "Unknown header %s\n", header_name.c_str());
    }
  }
  if (data.is_boundary (i)) {
    return i;
  }
  if (data[i] == '\r') {
    i++;
  }
  if (data[i] == '\n') {
    i++;
  } else {
    return i;
  }

  if (filename.empty()) {
    if (!name.empty()) {
      string post_data;
      while (!data.is_boundary (i) && (int)post_data.size() < 65536) {
        post_data.push_back (data[i++]);
      }
      if ((int)post_data.size() < 65536) {
        if (!strncmp (content_type.c_str(), "application/x-www-form-urlencoded", 33)) {
          f$parse_str (post_data, v$_POST[name]);
        } else {
          //TODO
          v$_POST.set_value (name, post_data);
        }
      }
    }
  } else {
    int file_size;
    OrFalse <string> tmp_name;
    if (v$_FILES.count() < MAX_FILES) {
      if (dl::query_num != uploaded_files_last_query_num) {
        new (uploaded_files_storage) array <int>();
        uploaded_files_last_query_num = dl::query_num;
      }

      dl::enter_critical_section();//NOT OK: uploaded_files
      tmp_name = f$tempnam (string(), string());
      uploaded_files->set_value (tmp_name.val(), 1);
      dl::leave_critical_section();

      if (f$boolval (tmp_name)) {
        file_size = data.upload_file (tmp_name.val(), i, max_file_size);

        if (file_size < 0) {
          dl::enter_critical_section();//NOT OK: uploaded_files
          f$unlink (tmp_name.val());
          uploaded_files->unset (tmp_name.val());
          dl::leave_critical_section();
        }
      } else {
        file_size = -UPLOAD_ERR_NO_TMP_DIR;
      }
    } else {
      file_size = -UPLOAD_ERR_NO_FILE;
    }

    if (name.size() >= 2 && name[name.size() - 2] == '[' && name[name.size() - 1] == ']') {
      var &file = v$_FILES[name.substr (0, name.size() - 2)];
      if (file_size >= 0) {
        file[string ("name", 4)].push_back (filename);
        file[string ("type", 4)].push_back (content_type);
        file[string ("size", 4)].push_back (file_size);
        file[string ("tmp_name", 8)].push_back (tmp_name.val());
        file[string ("error", 5)].push_back (UPLOAD_ERR_OK);
      } else {
        file[string ("name", 4)].push_back (string());
        file[string ("type", 4)].push_back (string());
        file[string ("size", 4)].push_back (0);
        file[string ("tmp_name", 8)].push_back (string());
        file[string ("error", 5)].push_back (-file_size);
      }
    } else {
      var &file = v$_FILES[name];
      if (file_size >= 0) {
        file.set_value (string ("name", 4), filename);
        file.set_value (string ("type", 4), content_type);
        file.set_value (string ("size", 4), file_size);
        file.set_value (string ("tmp_name", 8), tmp_name.val());
        file.set_value (string ("error", 5), UPLOAD_ERR_OK);
      } else {
        file.set_value (string ("size", 4), 0);
        file.set_value (string ("tmp_name", 8), string());
        file.set_value (string ("error", 5), -file_size);
      }
    }
  }

  return i;
}

static bool parse_multipart (const char *post, int post_len, const string &boundary) {
  static const int MAX_BOUNDARY_LENGTH = 70;

  if (boundary.empty() || (int)boundary.size() > MAX_BOUNDARY_LENGTH) {
    return false;
  }
  php_assert (post_len >= 0);

  post_reader data (post, post_len, boundary);

  for (int i = 0; i < post_len; i++) {
//    fprintf (stderr, "!!!! %d\n", i);
    i = parse_multipart_one (data, i);

//    fprintf (stderr, "???? %d\n", i);
    while (!data.is_boundary (i)) {
      i++;
    }
    i += 2 + (int)boundary.size();

    while (i < post_len && data[i] != '\n') {
      i++;
    }
  }

  return true;
}

void f$parse_multipart (const string &post, const string &boundary) {
  parse_multipart (post.c_str(), (int)post.size(), boundary);
}


extern char **environ;

//TODO argc, argv
static void init_superglobals (const char *uri, int uri_len, const char *get, int get_len, const char *headers, int headers_len, const char *post, int post_len,
                               const char *request_method, int request_method_len, int remote_ip, int remote_port, int keep_alive,
                               const int *serialized_data, int serialized_data_len, long long rpc_request_id, int rpc_remote_ip, int rpc_remote_port, int rpc_remote_pid, int rpc_remote_utime) {
  rpc_parse (serialized_data, serialized_data_len);

  INIT_VAR(var, v$_SERVER );
  INIT_VAR(var, v$_GET    );
  INIT_VAR(var, v$_POST   );
  INIT_VAR(var, v$_FILES  );
  INIT_VAR(var, v$_COOKIE );
  INIT_VAR(var, v$_REQUEST);
  INIT_VAR(var, v$_ENV    );

  v$_SERVER  = array <var>();
  v$_GET     = array <var>();
  v$_POST    = array <var>();
  v$_FILES   = array <var>();
  v$_COOKIE  = array <var>();
  v$_REQUEST = array <var>();
  v$_ENV     = array <var>();

  string uri_str;
  if (uri_len) {
    uri_str.assign (uri, uri_len);
    v$_SERVER.set_value (string ("PHP_SELF", 8), uri_str);
    v$_SERVER.set_value (string ("SCRIPT_URL", 10), uri_str);
    v$_SERVER.set_value (string ("SCRIPT_NAME", 11), uri_str);
  }

  string get_str;
  if (get_len) {
    get_str.assign (get, get_len);
    f$parse_str (get_str, v$_GET);

    v$_SERVER.set_value (string ("QUERY_STRING", 12), get_str);

    array <var> argv_array (array_size (1, 0, true));
    argv_array.push_back (get_str);
    v$_SERVER.set_value (string ("argv", 4), argv_array);
    v$_SERVER.set_value (string ("argc", 4), 1);
  }
  if (uri) {
    if (get_len) {
      v$_SERVER.set_value (string ("REQUEST_URI", 11), (static_SB.clean() + uri_str + '?' + get_str).str());
    } else {
      v$_SERVER.set_value (string ("REQUEST_URI", 11), uri_str);
    }
  }

  http_need_gzip = 0;
  string content_type ("application/x-www-form-urlencoded", 33);
  string content_type_lower = content_type;
  if (headers_len) {
    int i = 0;
    while (i < headers_len && 33 <= headers[i] && headers[i] <= 126) {
      int j;
      for (j = i; j < headers_len && 33 <= headers[j] && headers[j] <= 126 && headers[j] != ':'; j++) {
      }
      if (headers[j] != ':') {
        break;
      }

      string header_name = f$strtolower (string (headers + i, j - i));
      i = j + 1;

      string header_value;
      do {
        while (i < headers_len && headers[i] != '\r' && headers[i] != '\n') {
          header_value.push_back (headers[i++]);
        }

        while (i < headers_len && (headers[i] == '\r' || headers[i] == '\n')) {
          i++;
        }

        if (i == headers_len || (33 <= headers[i] && headers[i] <= 126)) {
          break;
        }
      } while (1);
      header_value = f$trim (header_value);

      if (!strcmp (header_name.c_str(), "accept-encoding")) {
        if (strstr (header_value.c_str(), "gzip") != NULL) {
          http_need_gzip |= 1;
        }
        if (strstr (header_value.c_str(), "deflate") != NULL) {
          http_need_gzip |= 2;
        }
      } else if (!strcmp (header_name.c_str(), "cookie")) {
        array <string> cookie = explode (';', header_value);
        for (int t = 0; t < (int)cookie.count(); t++) {
          array <string> cur_cookie = explode ('=', f$trim (cookie[t]), 2);
          if ((int)cur_cookie.count() == 2) {
            parse_str_set_value (v$_COOKIE, cur_cookie[0], f$urldecode (cur_cookie[1]));
          }
        }
      } else if (!strcmp (header_name.c_str(), "host")) {
        v$_SERVER.set_value (string ("SERVER_NAME", 11), header_value);
      }

      if (!strcmp (header_name.c_str(), "content-type")) {
        content_type = header_value;
        content_type_lower = f$strtolower (header_value);
      } else if (!strcmp (header_name.c_str(), "content-length")) {
        //must be equal to post_len, ignored
      } else {
        string key (header_name.size() + 5, false);
        bool good_name = true;
        for (int i = 0; i < (int)header_name.size(); i++) {
          if ('a' <= header_name[i] && header_name[i] <= 'z') {
            key[i + 5] = (char)(header_name[i] + 'A' - 'a');
          } else if ('0' <= header_name[i] && header_name[i] <= '9') {
            key[i + 5] = header_name[i];
          } else if ('-' == header_name[i]) {
            key[i + 5] = '_';
          } else {
            good_name = false;
            break;
          }
        }
        if (good_name) {
          key[0] = 'H';
          key[1] = 'T';
          key[2] = 'T';
          key[3] = 'P';
          key[4] = '_';
          v$_SERVER.set_value (key, header_value);
        } else {
//          fprintf (stderr, "%s : %s\n", header_name.c_str(), header_value.c_str());
        }
      }
    }
  }

  string HTTP_X_REAL_SCHEME  ("HTTP_X_REAL_SCHEME", 18);
  string HTTP_X_REAL_HOST    ("HTTP_X_REAL_HOST", 16);
  string HTTP_X_REAL_REQUEST ("HTTP_X_REAL_REQUEST", 19);
  if (v$_SERVER.isset (HTTP_X_REAL_SCHEME) && v$_SERVER.isset (HTTP_X_REAL_HOST) && v$_SERVER.isset (HTTP_X_REAL_REQUEST)) {
    string script_uri (v$_SERVER.get_value (HTTP_X_REAL_SCHEME).to_string());
    script_uri.append ("://", 3);
    script_uri.append (v$_SERVER.get_value (HTTP_X_REAL_HOST).to_string());
    script_uri.append (v$_SERVER.get_value (HTTP_X_REAL_REQUEST).to_string());
    v$_SERVER.set_value (string ("SCRIPT_URI", 10), script_uri);
  }

  if (post_len > 0) {
    bool is_parsed = (post != NULL);
//    fprintf (stderr, "!!!%.*s!!!\n", post_len, post);
    if (strstr (content_type_lower.c_str(), "application/x-www-form-urlencoded")) {
      if (post != NULL) {
        f$parse_str (string (post, post_len), v$_POST);
      }
    } else if (strstr (content_type_lower.c_str(), "multipart/form-data")) {
      const char *p = strstr (content_type_lower.c_str(), "boundary");
      if (p) {
        p += 8;
        p = strchr (content_type.c_str() + (p - content_type_lower.c_str()), '=');
        if (p) {
//          fprintf (stderr, "!!%s!!\n", p);
          p++;
          const char *end_p = strchrnul (p, ';');
          if (*p == '"' && p + 1 < end_p && end_p[-1] == '"') {
            p++;
            end_p--;
          }
//          fprintf (stderr, "!%s!\n", p);
          is_parsed |= parse_multipart (post, post_len, string (p, (dl::size_type)(end_p - p)));
        }
      }
    } else {
      if (post != NULL) {
        dl::enter_critical_section();//OK
        raw_post_data.assign (post, post_len);
        dl::leave_critical_section();
      }
    }

    if (!is_parsed) {
      int loaded = 0;
      while (loaded < post_len) {
        int to_load = min (PHP_BUF_LEN, post_len - loaded);
        http_load_long_query (php_buf, to_load, to_load);
        loaded += to_load;
      }
    }

    v$_SERVER.set_value (string ("CONTENT_TYPE", 12), content_type);
  }

  double cur_time = microtime (true);
  v$_SERVER.set_value (string ("GATEWAY_INTERFACE", 17), string ("CGI/1.1", 7));
  if (remote_ip) {
    v$_SERVER.set_value (string ("REMOTE_ADDR", 11), f$long2ip (remote_ip));
  }
  if (remote_port) {
    v$_SERVER.set_value (string ("REMOTE_PORT", 11), remote_port);
  }
  if (rpc_request_id) {
    v$_SERVER.set_value (string ("RPC_REQUEST_ID", 14), f$strval (Long (rpc_request_id)));
    v$_SERVER.set_value (string ("RPC_REMOTE_IP", 13), rpc_remote_ip);
    v$_SERVER.set_value (string ("RPC_REMOTE_PORT", 15), rpc_remote_port);
    v$_SERVER.set_value (string ("RPC_REMOTE_PID", 14), rpc_remote_pid);
    v$_SERVER.set_value (string ("RPC_REMOTE_UTIME", 16), rpc_remote_utime);
  }
  if (request_method_len) {
    v$_SERVER.set_value (string ("REQUEST_METHOD", 14), string (request_method, request_method_len));
  }
  v$_SERVER.set_value (string ("REQUEST_TIME", 12), int (cur_time));
  v$_SERVER.set_value (string ("REQUEST_TIME_FLOAT", 18), cur_time);
  v$_SERVER.set_value (string ("SERVER_PORT", 11), string ("80", 2));
  v$_SERVER.set_value (string ("SERVER_PROTOCOL", 15), string ("HTTP/1.1", 8));
  v$_SERVER.set_value (string ("SERVER_SIGNATURE", 16), (static_SB.clean() + "Apache/2.2.9 (Debian) PHP/5.2.6-1+lenny10 with Suhosin-Patch Server at " + v$_SERVER[string ("SERVER_NAME", 11)] + " Port 80").str());
  v$_SERVER.set_value (string ("SERVER_SOFTWARE", 15), string ("Apache/2.2.9 (Debian) PHP/5.2.6-1+lenny10 with Suhosin-Patch", 60));

  if (environ != NULL) {
    for (int i = 0; environ[i] != NULL; i++) {
      const char *s = strchr (environ[i], '=');
      php_assert (s != NULL);
      v$_ENV.set_value (string (environ[i], (dl::size_type)(s - environ[i])), string (s + 1, (dl::size_type)strlen (s + 1)));
    }
  }

  v$_REQUEST.as_array ("", -1) += v$_GET.to_array();
  v$_REQUEST.as_array ("", -1) += v$_POST.to_array();
  v$_REQUEST.as_array ("", -1) += v$_COOKIE.to_array();

  if (uri != NULL) {
    if (keep_alive) {
      header ("Connection: keep-alive", 22);
    } else {
      header ("Connection: close", 17);
    }
  }
  php_assert (dl::in_critical_section == 0);
}

static http_query_data empty_http_data;
static rpc_query_data empty_rpc_data;

void init_superglobals (php_query_data *data) {
  http_query_data *http_data;
  rpc_query_data *rpc_data;
  if (data != NULL) {
    if (data->http_data == NULL) {
      php_assert (data->rpc_data != NULL);
      query_type = QUERY_TYPE_RPC;

      http_data = &empty_http_data;
      rpc_data = data->rpc_data;
    } else {
      php_assert (data->rpc_data == NULL);
      query_type = QUERY_TYPE_HTTP;

      http_data = data->http_data;
      rpc_data = &empty_rpc_data;
    }
  } else {
    query_type = QUERY_TYPE_CONSOLE;

    http_data = &empty_http_data;
    rpc_data = &empty_rpc_data;
  }

  init_superglobals (http_data->uri, http_data->uri_len, http_data->get, http_data->get_len, http_data->headers, http_data->headers_len,
                     http_data->post, http_data->post_len, http_data->request_method, http_data->request_method_len,
                     http_data->ip, http_data->port, http_data->keep_alive,
                     rpc_data->data, rpc_data->len, rpc_data->req_id, rpc_data->ip, rpc_data->port, rpc_data->pid, rpc_data->utime);
}


static char ini_vars_storage[sizeof (array <string>)];
static array <string> *ini_vars = NULL;

void ini_set (const char *key, const char *value) {
  php_assert (dl::query_num == 0);

  if (ini_vars == NULL) {
    new (ini_vars_storage) array <string> ();
    ini_vars = reinterpret_cast <array <string> *> (ini_vars_storage);
  }

  ini_vars->set_value (string (key, (dl::size_type)strlen (key)), string (value, (dl::size_type)strlen (value)));
}

OrFalse <string> f$ini_get (const string &s) {
  if (ini_vars != NULL && ini_vars->has_key (s)) {
    return ini_vars->get_value (s);
  }

  if (!strcmp (s.c_str(), "max_execution_time")) {
    return string ("30.0", 4);//TODO
  }
  if (!strcmp (s.c_str(), "memory_limit")) {
    return f$strval ((int)dl::memory_limit);//TODO
  }
  if (!strcmp (s.c_str(), "include_path")) {
    return string();//TODO
  }

  php_warning ("Unrecognized option %s in ini_get", s.c_str());
  //php_assert (0);
  return false;
}

bool f$ini_set (const string &s, const string &value) {
  if (!strcmp (s.c_str(), "soap.wsdl_cache_enabled") || !strcmp (s.c_str(), "include_path") || !strcmp (s.c_str(), "memory")) {
    php_warning ("Option %s not supported in ini_set", s.c_str());
    return true;
  }
  if (!strcmp (s.c_str(), "default_socket_timeout")) {
    return false;//TODO
  }
  if (!strcmp (s.c_str(), "error_reporting")) {
    return f$error_reporting (f$intval (value));
  }

  php_critical_error ("unrecognized option %s in ini_set", s.c_str());
  return false; //unreachable
}

void init_static (void) {
  bcmath_init_static();
  datetime_init_static();
  drivers_init_static();
  exception_init_static();
  files_init_static();
  openssl_init_static();
  rpc_init_static();
  
  shutdown_function = NULL;
  finished = false;

  php_warning_level = 2;
  php_disable_warnings = 0;

  static char engine_pid_buf[20];
  dl::enter_critical_section();//OK
  sprintf (engine_pid_buf, "] [%d] ", (int)getpid());
  dl::leave_critical_section();
  engine_pid = engine_pid_buf;

  ob_cur_buffer = -1;
  f$ob_start();

  //TODO
  header ("HTTP/1.0 200 OK", 15);
  header ("Server: nginx/0.3.33", 20);
  string date = f$gmdate (HTTP_DATE);
  static_SB_spare.clean() + "Date: " + date;
  header (static_SB_spare.c_str(), (int)static_SB_spare.size());
  header ("Content-Type: text/html; charset=windows-1251", 45);

  INIT_VAR(bool, empty_bool);
  INIT_VAR(int, empty_int);
  INIT_VAR(double, empty_float);
  INIT_VAR(string, empty_string);
  INIT_VAR(array <var>, *empty_array_var_storage);
  INIT_VAR(var, empty_var);

  php_assert (dl::in_critical_section == 0);
}

void free_static (void) {
  php_assert (dl::in_critical_section == 0);

  drivers_free_static();
  files_free_static();
  openssl_free_static();
  rpc_free_static();

  dl::enter_critical_section();//OK
  if (dl::query_num == uploaded_files_last_query_num) {
    const array <int> *const_uploaded_files = uploaded_files;
    for (array <int>::const_iterator p = const_uploaded_files->begin(); p != const_uploaded_files->end(); ++p) {
      unlink (p.get_key().to_string().c_str());
    }
    uploaded_files_last_query_num--;
  }
  
  CLEAR_VAR(string, empty_string);
  CLEAR_VAR(var, empty_var);

  dl::script_runned = false;
  php_assert (dl::use_script_allocator == false);
  dl::leave_critical_section();
}


#include <cassert>

void read_engine_tag (const char *file_name) {
  assert (dl::query_num == 0);

  struct stat stat_buf;
  int file_fd = open (file_name, O_RDONLY);
  if (file_fd < 0) {
    assert ("Can't open file with engine tag" && 0);
  }
  if (fstat (file_fd, &stat_buf) < 0) {
    assert ("Can't do fstat on file with engine tag" && 0);
  }

  const size_t MAX_SIZE = 40;
  char buf[MAX_SIZE + 3];

  size_t size = stat_buf.st_size;
  if (size > MAX_SIZE) {
    size = MAX_SIZE;
  }
  if (read_safe (file_fd, buf, size) < (ssize_t)size) {
    assert ("Can't read file with engine tag" && 0);
  }
  close (file_fd);

  for (size_t i = 0; i < size; i++) {
    if (buf[i] < 32 || buf[i] > 126) {
      buf[i] = ' ';
    }
  }

  char prev = ' ';
  size_t j = 0;
  for (size_t i = 0; i < size; i++) {
    if (buf[i] != ' ' || prev != ' ') {
      buf[j++] = buf[i];
    }

    prev = buf[i];
  }
  if (prev == ' ' && j > 0) {
    j--;
  }

  buf[j] = 0;
  ini_set ("error_tag", buf);

  buf[j++] = ' ';
  buf[j++] = '[';
  buf[j] = 0;

  engine_tag = strdup (buf);
}

