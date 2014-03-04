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
              2012-2013 Vitaliy Valtman
*/

#define _FILE_OFFSET_BITS 64

#include "rpc.h"

#include "rpc-const.h"

#include "drivers.h"
#include "exception.h"
#include "files.h"
#include "misc.h"
#include "string_functions.h"//lhex_digits TODO
#include "zlib.h"

static const int GZIP_PACKED = 0x3072cfa1;
static const int TL_RPC_DEST_ACTOR = 0x7568aabd;
static const int TL_RPC_DEST_ACTOR_FLAGS = 0xf0a5acf7;

static const string UNDERSCORE ("_", 1);

static const int *rpc_data_begin;
static const int *rpc_data;
static int rpc_data_len;

static string rpc_data_copy;


template <class T>
inline T store_parse_number (const string &v) {
  T result = 0;
  const char *s = v.c_str();
  int sign = 1;
  if (*s == '-') {
    s++;
    sign = -1;
  }
  while ('0' <= *s && *s <= '9') {
    result = result * 10 + (*s++ - '0');
  }
  return result * sign;
}

template <class T>
inline T store_parse_number (const var &v) {
  if (v.is_int()) {
    return (T)v.to_int();
  }
  return store_parse_number <T> (v.to_string());
}


template <class T>
inline T store_parse_number_unsigned (const string &v) {
  T result = 0;
  const char *s = v.c_str();
  while ('0' <= *s && *s <= '9') {
    result = result * 10 + (*s++ - '0');
  }
  return result;
}

template <class T>
inline T store_parse_number_unsigned (const var &v) {
  if (v.is_int()) {
    return (T)v.to_int();
  }
  return store_parse_number <T> (v.to_string());
}

template <class T>
inline T store_parse_number_hex (const string &v) {
  T result = 0;
  const char *s = v.c_str();
  while (1) {
    T next = -1;
    if ('0' <= *s && *s <= '9') {
      next = *s - '0';
    } else if ('A' <= *s && *s <= 'F') {
      next = *s - ('A' - 10);
    } else if ('a' <= *s && *s <= 'f') {
      next = *s - ('a' - 10);
    }
    if (next == (T)-1) {
      break;
    }

    result = result * 16 + next;
    s++;
  }
  return result;
}


void rpc_parse (const int *new_rpc_data, int new_rpc_data_len) {
  rpc_data_begin = new_rpc_data;
  rpc_data = new_rpc_data;
  rpc_data_len = new_rpc_data_len;
}

void f$rpc_parse (const string &rpc_data) {
  if (rpc_data.size() % sizeof (int) != 0) {
    php_warning ("Wrong parameter \"rpc_data\" passed to function rpc_parse");
    return;
  }

  dl::enter_critical_section();//OK
  rpc_data_copy = rpc_data;
  dl::leave_critical_section();

  rpc_parse ((const int *)rpc_data_copy.c_str(), rpc_data_copy.size() / sizeof (int));
}

void f$rpc_parse (const var &rpc_data) {
  if (!rpc_data.is_string()) {
    php_warning ("Parameter 1 of function rpc_parse must be a string, %s is given", rpc_data.get_type_c_str());
    return;
  }

  return f$rpc_parse (rpc_data.to_string());
}

int rpc_get_pos (void) {
  return (int)(long)(rpc_data - rpc_data_begin);
}

bool rpc_set_pos (int pos) {
  if (pos < 0 || rpc_data_begin + pos > rpc_data) {
    return false;
  }

  rpc_data_len += (int)(rpc_data - rpc_data_begin - pos);
  rpc_data = rpc_data_begin + pos;
  return true;
}


static inline void check_rpc_data_len (const string &file, int line, int len) {
  if (rpc_data_len < len) {
    THROW_EXCEPTION(Exception (file, line, string ("Not enough data to fetch", 24), -1));
    return;
  }
  rpc_data_len -= len;
}

int rpc_lookup_int (const string &file, int line) {
  TRY_CALL_VOID(int, (check_rpc_data_len (file, line, 1)));
  rpc_data_len++;
  return *rpc_data;
}

int f$fetch_int (const string &file, int line) {
  TRY_CALL_VOID(int, (check_rpc_data_len (file, line, 1)));
  return *rpc_data++;
}

UInt f$fetch_UInt (const string &file, int line) {
  TRY_CALL_VOID(UInt, (check_rpc_data_len (file, line, 1)));
  return UInt ((unsigned int)(*rpc_data++));
}

Long f$fetch_Long (const string &file, int line) {
  TRY_CALL_VOID(Long, (check_rpc_data_len (file, line, 2)));
  long long result = *(long long *)rpc_data;
  rpc_data += 2;

  return Long (result);
}

ULong f$fetch_ULong (const string &file, int line) {
  TRY_CALL_VOID(ULong, (check_rpc_data_len (file, line, 2)));
  unsigned long long result = *(unsigned long long *)rpc_data;
  rpc_data += 2;

  return ULong (result);
}

var f$fetch_unsigned_int (const string &file, int line) {
  TRY_CALL_VOID(var, (check_rpc_data_len (file, line, 1)));
  unsigned int result = *rpc_data++;

  if (result <= (unsigned int)INT_MAX) {
    return (int)result;
  }

  return f$strval (UInt (result));
}

var f$fetch_long (const string &file, int line) {
  TRY_CALL_VOID(var, (check_rpc_data_len (file, line, 2)));
  long long result = *(long long *)rpc_data;
  rpc_data += 2;

  if ((long long)INT_MIN <= result && result <= (long long)INT_MAX) {
    return (int)result;
  }

  return f$strval (Long (result));
}

var f$fetch_unsigned_long (const string &file, int line) {
  TRY_CALL_VOID(var, (check_rpc_data_len (file, line, 2)));
  unsigned long long result = *(unsigned long long *)rpc_data;
  rpc_data += 2;

  if (result <= (unsigned long long)INT_MAX) {
    return (int)result;
  }

  return f$strval (ULong (result));
}

string f$fetch_unsigned_int_hex (const string &file, int line) {
  TRY_CALL_VOID(string, (check_rpc_data_len (file, line, 1)));
  unsigned int result = *rpc_data++;

  char buf[8], *end_buf = buf + 8;
  for (int i = 0; i < 8; i++) {
    *--end_buf = lhex_digits[result & 15];
    result >>= 4;
  }

  return string (end_buf, 8);
}

string f$fetch_unsigned_long_hex (const string &file, int line) {
  TRY_CALL_VOID(string, (check_rpc_data_len (file, line, 2)));
  unsigned long long result = *(unsigned long long *)rpc_data;
  rpc_data += 2;

  char buf[16], *end_buf = buf + 16;
  for (int i = 0; i < 16; i++) {
    *--end_buf = lhex_digits[result & 15];
    result >>= 4;
  }

  return string (end_buf, 16);
}

string f$fetch_unsigned_int_str (const string &file, int line) {
  return f$strval (TRY_CALL (UInt, string, (f$fetch_UInt (file, line))));
}

string f$fetch_unsigned_long_str (const string &file, int line) {
  return f$strval (TRY_CALL (ULong, string, (f$fetch_ULong (file, line))));
}

double f$fetch_double (const string &file, int line) {
  TRY_CALL_VOID(double, (check_rpc_data_len (file, line, 2)));
  double result = *(double *)rpc_data;
  rpc_data += 2;

  return result;
}

string f$fetch_string (const string &file, int line) {
  TRY_CALL_VOID(string, (check_rpc_data_len (file, line, 1)));
  char *str = (char *)rpc_data;
  int result_len = (unsigned char)*str++;
  if (result_len < 254) {
    TRY_CALL_VOID(string, (check_rpc_data_len (file, line, result_len >> 2)));
    rpc_data += (result_len >> 2) + 1;
  } else if (result_len == 254) {
    result_len = (unsigned char)str[0] + ((unsigned char)str[1] << 8) + ((unsigned char)str[2] << 16);
    str += 3;
    TRY_CALL_VOID(string, (check_rpc_data_len (file, line, (result_len + 3) >> 2)));
    rpc_data += ((result_len + 7) >> 2);
  } else {
    THROW_EXCEPTION(Exception (file, line, string ("Can't fetch string, 255 found", 29), -3));
    return string();
  }

  return string (str, result_len);
}

bool f$fetch_eof (const string &file, int line) {
  return rpc_data_len == 0;
}

bool f$fetch_end (const string &file, int line) {
  if (rpc_data_len) {
    THROW_EXCEPTION(Exception (file, line, string ("Too much data to fetch", 22), -2));
    return false;
  }
  return true;
}


rpc_connection::rpc_connection (void):
    bool_value (false),
    host_num (-1),
    timeout_ms (-1),
    default_actor_id (-1),
    connect_timeout (-1),
    reconnect_timeout (-1) {
}

rpc_connection::rpc_connection (bool value):
    bool_value (value),
    host_num (-1),
    timeout_ms (-1),
    default_actor_id (-1),
    connect_timeout (-1),
    reconnect_timeout (-1) {
}

rpc_connection::rpc_connection (bool value, int host_num, int timeout_ms, long long default_actor_id, int connect_timeout, int reconnect_timeout):
    bool_value (value),
    host_num (host_num),
    timeout_ms (timeout_ms),
    default_actor_id (default_actor_id),
    connect_timeout (connect_timeout),
    reconnect_timeout (reconnect_timeout) {
}

rpc_connection& rpc_connection::operator = (bool value) {
  bool_value = value;
  return *this;
}


rpc_connection f$new_rpc_connection (string host_name, int port, const var &default_actor_id, double timeout, double connect_timeout, double reconnect_timeout) {
  int host_num = rpc_connect_to (host_name.c_str(), port);
  if (host_num < 0) {
    return rpc_connection();
  }

  return rpc_connection (true, host_num, timeout_convert_to_ms (timeout),
                         store_parse_number <long long> (default_actor_id),
                         timeout_convert_to_ms (connect_timeout), timeout_convert_to_ms (reconnect_timeout));
}

bool f$boolval (const rpc_connection &my_rpc) {
  return my_rpc.bool_value;
}

bool eq2 (const rpc_connection &my_rpc, bool value) {
  return my_rpc.bool_value == value;
}

bool eq2 (bool value, const rpc_connection &my_rpc) {
  return value == my_rpc.bool_value;
}

bool equals (bool value, const rpc_connection &my_rpc) {
  return equals (value, my_rpc.bool_value);
}

bool equals (const rpc_connection &my_rpc, bool value) {
  return equals (my_rpc.bool_value, value);
}

bool not_equals (bool value, const rpc_connection &my_rpc) {
  return not_equals (value, my_rpc.bool_value);
}

bool not_equals (const rpc_connection &my_rpc, bool value) {
  return not_equals (my_rpc.bool_value, value);
}


static string_buffer data_buf;
static const int data_buf_header_size = 2 * sizeof (long long) + 4 * sizeof (int);
static const int data_buf_header_reserved_size = sizeof (long long) + sizeof (int);

int rpc_stored;
static int rpc_pack_threshold;
static int rpc_pack_from;

void f$store_gzip_pack_threshold (int pack_threshold_bytes) {
  rpc_pack_threshold = pack_threshold_bytes;
}

void f$store_start_gzip_pack (void) {
  rpc_pack_from = data_buf.size();
}

void f$store_finish_gzip_pack (int threshold) {
  if (rpc_pack_from != -1 && threshold > 0) {
    int answer_size = data_buf.size() - rpc_pack_from;
    php_assert (rpc_pack_from % sizeof (int) == 0 && 0 <= rpc_pack_from && 0 <= answer_size);
    if (answer_size >= threshold) {
      const char *answer_begin = data_buf.c_str() + rpc_pack_from;
      const string_buffer *compressed = zlib_encode (answer_begin, answer_size, 6, ZLIB_ENCODE);

      if ((int)(compressed->size() + 2 * sizeof (int)) < answer_size) {
        data_buf.set_pos (rpc_pack_from);
        f$store_int (GZIP_PACKED);
        store_string (compressed->buffer(), compressed->size());
      }
    }
  }
  rpc_pack_from = -1;
}


template <class T>
inline bool store_raw (T v) {
  data_buf.append ((char *)&v, sizeof (v));
  return true;
}

bool store_header (long long cluster_id, int flags) {
  if (flags) {
    f$store_int (TL_RPC_DEST_ACTOR_FLAGS);
    store_long (cluster_id);
    f$store_int (flags);
  } else {
    f$store_int (TL_RPC_DEST_ACTOR);
    store_long (cluster_id);
  }
  return true;
}

bool f$store_header (const var &cluster_id, int flags) {
  return store_header (store_parse_number <long long> (cluster_id), flags);
}

bool store_error (int error_code, const char *error_text, int error_text_len) {
  f$rpc_clean (true);
  f$store_int (error_code);
  store_string (error_text, error_text_len);
  rpc_store (true);
  script_error();
  return true;
}

bool store_error (int error_code, const char *error_text) {
  return store_error (error_code, error_text, (int)strlen (error_text));
}

bool f$store_error (int error_code, const string &error_text) {
  return store_error (error_code, error_text.c_str(), (int)error_text.size());
}

bool f$store_int (int v) {
  return store_raw (v);
}

bool f$store_UInt (UInt v) {
  return store_raw (v.l);
}

bool f$store_Long (Long v) {
  return store_raw (v.l);
}

bool f$store_ULong (ULong v) {
  return store_raw (v.l);
}

bool store_unsigned_int (unsigned int v) {
  return store_raw (v);
}

bool store_long (long long v) {
  return store_raw (v);
}

bool store_unsigned_long (unsigned long long v) {
  return store_raw (v);
}

bool f$store_unsigned_int (const string &v) {
  return store_raw (store_parse_number_unsigned <unsigned int> (v));
}

bool f$store_long (const string &v) {
  return store_raw (store_parse_number <long long> (v));
}

bool f$store_unsigned_long (const string &v) {
  return store_raw (store_parse_number_unsigned <unsigned long long> (v));
}

bool f$store_unsigned_int_hex (const string &v) {
  return store_raw (store_parse_number_hex <unsigned int> (v));
}

bool f$store_unsigned_long_hex (const string &v) {
  return store_raw (store_parse_number_hex <unsigned long long> (v));
}

bool f$store_double (double v) {
  return store_raw (v);
}

bool store_string (const char *v, int v_len) {
  int all_len = v_len;
  if (v_len < 254) {
    data_buf += (char) (v_len);
    all_len += 1;
  } else if (v_len < (1 << 24)) {
    data_buf += (char) (254);
    data_buf += (char) (v_len & 255);
    data_buf += (char) ((v_len >> 8) & 255);
    data_buf += (char) ((v_len >> 16) & 255);
    all_len += 4;
  } else {
    php_critical_error ("trying to store too big string of length %lld", (unsigned long long)v_len);
  }
  data_buf.append (v, v_len);

  while (all_len % 4 != 0) {
    data_buf += (char)'\0';
    all_len++;
  }
  return true;
}

bool f$store_string (const string &v) {
  return store_string (v.c_str(), (int)v.size());
}

bool f$store_many (const array <var> &a) {
  int n = a.count();
  if (n == 0) {
    php_warning ("store_many must take at least 1 argument");
    return false;
  }

  string pattern = a.get_value (0).to_string();
  if (n != 1 + (int)pattern.size()) {
    php_warning ("Wrong number of arguments in call to store_many");
    return false;
  }

  for (int i = 1; i < n; i++) {
    switch (pattern[i - 1]) {
    case 's':
      f$store_string (a.get_value (i).to_string());
      break;
    case 'l':
      f$store_long (a.get_value (i));
      break;
    case 'd':
    case 'i':
      f$store_int (a.get_value (i).to_int());
      break;
    case 'f':
      f$store_double (a.get_value (i).to_float());
      break;
    default:
      php_warning ("Wrong symbol '%c' at position %d in first argument of store_many", pattern[i - 1], i - 1);
      break;
    }
  }

  return true;
}


bool f$store_finish (void) {
  return rpc_store (false);
}

bool f$rpc_clean (bool is_error) {
  data_buf.clean();
  f$store_int (-1); //reserve for TL_RPC_DEST_ACTOR
  store_long (-1); //reserve for session_id or actor_id
  f$store_int (-1); //reserve for length
  f$store_int (-1); //reserve for num
  f$store_int (-is_error); //reserve for type
  store_long (-1); //reserve for req_id or auth_key_id

  rpc_pack_from = -1;
  return true;
}

bool rpc_store (bool is_error) {
  if (rpc_stored) {
    return false;
  }

  if (!is_error) {
    rpc_pack_from = data_buf_header_size;
    f$store_finish_gzip_pack (rpc_pack_threshold);
  }

  f$store_int (-1); // reserve for crc32
  rpc_stored = 1;
  rpc_answer (data_buf.c_str() + data_buf_header_reserved_size, (int)(data_buf.size() - data_buf_header_reserved_size));
  return true;
}

static char rpc_result_string_storage[sizeof (OrFalse <string>)];
static OrFalse <string> *rpc_result_string;
static bool rpc_result_bool;

long long rpc_send (const rpc_connection &conn, double timeout) {
  if (unlikely (conn.host_num < 0)) {
    php_warning ("Wrong rpc_connection specified");
    return 0;
  }

  int timeout_ms;
  if (timeout > 0) {
    timeout_ms = timeout_convert_to_ms (timeout);
  } else {
    timeout_ms = conn.timeout_ms;
  }

  f$store_int (-1); // reserve for crc32
  php_assert (data_buf.size() % sizeof (int) == 0);

  int reserved = data_buf_header_reserved_size;
  if (conn.default_actor_id) {
    const char *answer_begin = data_buf.c_str() + data_buf_header_size;
    int x = *(int *)answer_begin;
    if (x != TL_RPC_DEST_ACTOR && x != TL_RPC_DEST_ACTOR_FLAGS) {
      reserved -= (int)(sizeof (int) + sizeof (long long));
      php_assert (reserved >= 0);
      *(int *)(answer_begin - sizeof (int) - sizeof (long long)) = TL_RPC_DEST_ACTOR;
      *(long long *)(answer_begin - sizeof (long long)) = conn.default_actor_id;
    }
  }

  return rpc_send_query (conn.host_num, data_buf.c_str() + reserved, (int)(data_buf.size() - reserved), timeout_ms);
}

var f$rpc_send (const rpc_connection &conn, double timeout) {
  if (conn.host_num < 0) {
    php_warning ("Wrong rpc_connection specified");
    return false;
  }

  return long_long_convert_to_var (rpc_send (conn, timeout));
}

void rpc_get_callback (const int *result, int result_len) {
  *rpc_result_string = string ((const char *)result, (dl::size_type)(result_len * sizeof (int)));
}

void rpc_get_and_parse_callback (const int *result, int result_len) {
  rpc_result_bool = true;
//  for (int i = 0; i < result_len; i++) {
//    fprintf (stderr, "%d: %x(%d)\n", i, result[i], result[i]);
//  }
  rpc_parse (result, result_len);
}

OrFalse <string> f$rpc_get (int request_id) {
  return rpc_get ((long long)request_id);
}

OrFalse <string> rpc_get (long long request_id) {
  *rpc_result_string = false;
  rpc_get_result (request_id, rpc_get_callback);
  return *rpc_result_string;
}

OrFalse <string> f$rpc_get (const string &request_id) {
  return rpc_get (store_parse_number <long long> (request_id));
}

bool f$rpc_get_and_parse (int request_id) {
  return rpc_get_and_parse ((long long)request_id);
}

bool rpc_get_and_parse (long long request_id) {
  rpc_result_bool = false;
  rpc_get_result (request_id, rpc_get_and_parse_callback);
  return rpc_result_bool;
}

bool f$rpc_get_and_parse (const string &request_id) {
  return rpc_get_and_parse (store_parse_number <long long> (request_id));
}

bool f$rpc_send_session_msg (int auth_key_id, int session_id) {
  return rpc_send_session_msg ((long long)auth_key_id, (long long)session_id);
}

bool rpc_send_session_msg (long long auth_key_id, long long session_id) {
  php_assert (data_buf.size() % sizeof (int) == 0);
  php_assert ((int)data_buf.size() >= data_buf_header_size);
  f$store_int (-1); // reserve for crc32
  *(long long *)(data_buf.c_str() + data_buf_header_size - sizeof (long long)) = session_id;
  rpc_send_session_message (auth_key_id, session_id, data_buf.c_str() + sizeof (int), (int)(data_buf.size() - sizeof (int)));
  return true;
}

bool f$rpc_send_session_msg (const string &auth_key_id, const string &session_id) {
  return rpc_send_session_msg (store_parse_number <long long> (auth_key_id), store_parse_number <long long> (session_id));
}

var f$rpc_get_any_qid (void) {
  return long_long_convert_to_var (rpc_get_next_request_id (0));
}

bool f$rpc_queue_empty (int queue_id) {
  return rpc_queue_empty ((long long)queue_id);
}

bool rpc_queue_empty (long long queue_id) {
  return rpc_is_queue_empty (queue_id);
}

bool f$rpc_queue_empty (const string &queue_id) {
  return rpc_queue_empty (store_parse_number <long long> (queue_id));
}

var f$rpc_queue_next (int queue_id) {
  return rpc_queue_next ((long long)queue_id);
}

var rpc_queue_next (long long queue_id) {
  return long_long_convert_to_var (rpc_get_next_request_id (queue_id));
}

var f$rpc_queue_next (const string &queue_id) {
  return rpc_queue_next (store_parse_number <long long> (queue_id));
}


int rpc_connect_to_dummy (const char *host_name, int port) {
  php_critical_error ("rpc_connect_to_dummy called");
}

int (*rpc_connect_to) (const char *host_name, int port) = rpc_connect_to_dummy;

long long rpc_send_query_dummy (int host_num, const char *request, int request_len, int timeout_ms) {
  php_critical_error ("rpc_send_query_dummy called");
}

long long (*rpc_send_query) (int host_num, const char *request, int request_len, int timeout_ms) = rpc_send_query_dummy;

void rpc_get_result_dummy (long long request_id, void (*callback) (const int *data, int data_len)) {
  php_critical_error ("rpc_get_result_dummy called");
}

void (*rpc_get_result) (long long request_id, void (*callback) (const int *data, int data_len)) = rpc_get_result_dummy;

void rpc_send_session_message_dummy (long long auth_key_id, long long session_id, const char *request, int request_len) {
  php_critical_error ("rpc_send_session_message_dummy called");
}

void (*rpc_send_session_message) (long long auth_key_id, long long session_id, const char *request, int request_len) = rpc_send_session_message_dummy;

void rpc_answer_dummy (const char *res, int res_len) {
  php_critical_error ("rpc_answer_dummy called");
}

void (*rpc_answer) (const char *res, int res_len) = rpc_answer_dummy;

long long rpc_create_queue_dummy (long long *request_ids, int request_ids_len) {
  php_critical_error ("rpc_create_queue_dummy called");
}

long long (*rpc_create_queue) (long long *request_ids, int request_ids_len) = rpc_create_queue_dummy;

bool rpc_is_queue_empty_dummy (long long queue_id) {
  php_critical_error ("rpc_is_queue_empty_dummy called");
}

bool (*rpc_is_queue_empty) (long long queue_id) = rpc_is_queue_empty_dummy;

long long rpc_get_next_request_id_dummy (long long queue_id) {
  php_critical_error ("rpc_get_next_request_id_dummy called");
}

long long (*rpc_get_next_request_id) (long long queue_id) = rpc_get_next_request_id_dummy;

void script_error_dummy (void) {
  php_critical_error ("script_error_dummy called");
}

void (*script_error) (void) = script_error_dummy;


/*
 *
 *  var wrappers
 *
 */


bool f$store_unsigned_int (const var &v) {
  return store_unsigned_int (store_parse_number_unsigned <unsigned int> (v));
}

bool f$store_long (const var &v) {
  return store_long (store_parse_number <long long> (v));
}

bool f$store_unsigned_long (const var &v) {
  return store_unsigned_long (store_parse_number_unsigned <unsigned long long> (v));
}

OrFalse <string> f$rpc_get (const var &v) {
  return rpc_get (store_parse_number <long long> (v));
}

bool f$rpc_get_and_parse (const var &v) {
  return rpc_get_and_parse (store_parse_number <long long> (v));
}

bool f$rpc_send_session_msg (const var &auth_key_id, const var &session_id) {
  return rpc_send_session_msg (store_parse_number <long long> (auth_key_id), store_parse_number <long long> (session_id));
}

bool f$rpc_queue_empty (const var &queue_id) {
  return rpc_queue_empty (store_parse_number <long long> (queue_id));
}

var f$rpc_queue_next (const var &queue_id) {
  return rpc_queue_next (store_parse_number <long long> (queue_id));
}


/*
 *
 *     RPC_TL_QUERY
 *
 */


int tl_parse_int (void) {
  return TRY_CALL(int, int, (f$fetch_int (string(), -1)));
}

long long tl_parse_long (void) {
  return TRY_CALL(long long, int, (f$fetch_Long (string(), -1).l));
}

double tl_parse_double (void) {
  return TRY_CALL(double, double, (f$fetch_double (string(), -1)));
}

string tl_parse_string (void) {
  return TRY_CALL(string, string, (f$fetch_string (string(), -1)));
}

void tl_parse_end (void) {
  TRY_CALL_VOID(void, (f$fetch_end (string(), -1)));
}

int tl_parse_save_pos (void) {
  return rpc_get_pos();
}

bool tl_parse_restore_pos (int pos) {
  return rpc_set_pos (pos);
}


const int NODE_TYPE_TYPE = 1;
const int NODE_TYPE_NAT_CONST = 2;
const int NODE_TYPE_VAR_TYPE = 3;
const int NODE_TYPE_VAR_NUM = 4;
const int NODE_TYPE_ARRAY = 5;

const int ID_VAR_NUM = 0x70659eff;
const int ID_VAR_TYPE = 0x2cecf817;
const int ID_INT = 0xa8509bda;
const int ID_LONG = 0x22076cba;
const int ID_DOUBLE = 0x2210c154;
const int ID_STRING = 0xb5286e24;
const int ID_VECTOR = 0x1cb5c415;
const int ID_DICTIONARY = 0x1f4c618f;
const int ID_MAYBE_TRUE = 0x3f9c8ef8;
const int ID_MAYBE_FALSE = 0x27930a7b;
const int ID_BOOL_FALSE = 0xbc799737;
const int ID_BOOL_TRUE = 0x997275b5;

const int FLAG_OPT_VAR = (1 << 17);
const int FLAG_EXCL = (1 << 18);
const int FLAG_OPT_FIELD = (1 << 20);
const int FLAG_NOVAR = (1 << 21);
const int FLAG_DEFAULT_CONSTRUCTOR = (1 << 25);
const int FLAG_BARE = (1 << 0);
const int FLAG_NOCONS = (1 << 1);
const int FLAGS_MASK = ((1 << 16) - 1);


struct tl_combinator;
struct tl_tree;

struct tl_type {
  int id;
  string name;
  int arity;
  int flags;
  int constructors_num;
  array <tl_combinator *> constructors;
};

struct arg {
  string name;
  int flags;
  int var_num;
  int exist_var_num;
  int exist_var_bit;
  tl_tree *type;
};

struct tl_combinator {
  int id;
  string name;
  int var_count;
  int type_id;
  array <arg> args;
  tl_tree *result;

  void **IP;
  void **fetchIP;
  int IP_len;
  int fetchIP_len;
};

class tl_tree {
public:
  int flags;

  tl_tree (int flags): flags (flags) {
  }

  virtual void print (int shift = 0) const = 0;

  virtual int get_type (void) const = 0;

  virtual bool equals (tl_tree *other) const = 0;

  virtual tl_tree *dup (void) const = 0;

  virtual void destroy (void) = 0;

  virtual ~tl_tree (void) {
  }
};

class tl_tree_type: public tl_tree {
public:
  tl_type *type;
  array <tl_tree *> children;

  tl_tree_type (int flags, tl_type *type, const array_size &s): tl_tree (flags),
                                                                type (type),
                                                                children (s) {
  }

  virtual void print (int shift = 0) const {
    fprintf (stderr, "%*sType %s(%x) at (%p)\n", shift, "", type->name.c_str(), type->id, this);
  }

  virtual int get_type (void) const {
    return NODE_TYPE_TYPE;
  }

  virtual bool equals (tl_tree *other_) const {
    if (other_->get_type() != NODE_TYPE_TYPE) {
      return false;
    }
    tl_tree_type *other = (tl_tree_type *)other_;
    if ((flags & FLAGS_MASK) != (other->flags & FLAGS_MASK) || type != other->type) {
      return false;
    }
    for (int i = 0; i < children.count(); i++) {
      if (!children.get_value (i)->equals (other->children.get_value (i))) {
        return false;
      }
    }
    return true;
  }

  virtual tl_tree *dup (void) const {
    tl_tree_type *T = (tl_tree_type *)dl::allocate (sizeof (tl_tree_type));
    //fprintf (stderr, "dup type %s (%p), result = %p\n", type->name.c_str(), this, T);
    new (T) tl_tree_type (flags, type, children.size());

    for (int i = 0; i < children.count(); i++) {
      T->children.set_value (i, children.get_value (i)->dup());
    }
    return T;
  }

  virtual void destroy (void) {
    for (int i = 0; i < children.count(); i++) {
      if (children.get_value (i) != NULL) {
        children.get_value (i)->destroy();
      }
    }

    this->~tl_tree_type();
    dl::deallocate (this, sizeof (*this));
  }
};

class tl_tree_nat_const: public tl_tree {
public:
  int num;

  tl_tree_nat_const (int flags, int num): tl_tree (flags),
                                          num (num) {
  }

  virtual void print (int shift = 0) const {
    fprintf (stderr, "%*sConst %d\n", shift, "", num);
  }

  virtual int get_type (void) const {
    return NODE_TYPE_NAT_CONST;
  }

  virtual bool equals (tl_tree *other) const {
    return other->get_type() == NODE_TYPE_NAT_CONST && num == ((tl_tree_nat_const *)other)->num;
  }

  virtual tl_tree *dup (void) const {
    tl_tree_nat_const *T = (tl_tree_nat_const *)dl::allocate (sizeof (tl_tree_nat_const));
    //fprintf (stderr, "dup nat const %d (%p), result = %p\n", num, this, T);
    new (T) tl_tree_nat_const (flags, num);

    return T;
  }

  virtual void destroy (void) {
    this->~tl_tree_nat_const();
    dl::deallocate (this, sizeof (*this));
  }
};

class tl_tree_var_type: public tl_tree {
public:
  int var_num;

  tl_tree_var_type (int flags, int var_num): tl_tree (flags),
                                             var_num (var_num) {
  }

  virtual void print (int shift = 0) const {
    fprintf (stderr, "%*sVariable type, var_num = %d.\n", shift, "", var_num);
  }

  virtual int get_type (void) const {
    return NODE_TYPE_VAR_TYPE;
  }

  virtual bool equals (tl_tree *other) const {
    php_assert (false);
    return false;
  }

  virtual tl_tree *dup (void) const {
    tl_tree_var_type *T = (tl_tree_var_type *)dl::allocate (sizeof (tl_tree_var_type));
    //fprintf (stderr, "dup var type (%p), result = %p\n", this, T);
    new (T) tl_tree_var_type (flags, var_num);

    return T;
  }

  virtual void destroy (void) {
    this->~tl_tree_var_type();
    dl::deallocate (this, sizeof (*this));
  }
};

class tl_tree_var_num: public tl_tree {
public:
  int var_num;
  int diff;

  tl_tree_var_num (int flags, int var_num, int diff): tl_tree (flags),
                                                      var_num (var_num),
                                                      diff (diff) {
  }

  virtual void print (int shift = 0) const {
    fprintf (stderr, "%*sVariable number, var_num = %d, diff = %d.\n", shift, "", var_num, diff);
  }

  virtual int get_type (void) const {
    return NODE_TYPE_VAR_NUM;
  }

  virtual bool equals (tl_tree *other) const {
    php_assert (false);
    return false;
  }

  virtual tl_tree *dup (void) const {
    tl_tree_var_num *T = (tl_tree_var_num *)dl::allocate (sizeof (tl_tree_var_num));
    //fprintf (stderr, "dup var num (%p), result = %p\n", this, T);
    new (T) tl_tree_var_num (flags, var_num, diff);

    return T;
  }

  virtual void destroy (void) {
    this->~tl_tree_var_num();
    dl::deallocate (this, sizeof (*this));
  }
};

class tl_tree_array: public tl_tree {
public:
  tl_tree *multiplicity;
  array <arg> args;

  tl_tree_array (int flags, tl_tree *multiplicity, const array_size &s): tl_tree (flags),
                                                                         multiplicity (multiplicity),
                                                                         args (s) {
  }

  tl_tree_array (int flags, tl_tree *multiplicity, const array <arg> &a): tl_tree (flags),
                                                                          multiplicity (multiplicity),
                                                                          args (a) {
  }

  virtual void print (int shift = 0) const {
    fprintf (stderr, "%*sArray, number of elements = ", shift, "");
    multiplicity->print();

    fprintf (stderr, "%*s    elements:", shift, "");
    for (int i = 0; i < args.count(); i++) {
      fprintf (stderr, "%*s    name = %s, var_num = %d\n", shift, "", args.get_value (i).name.c_str(), args.get_value (i).var_num);
      args.get_value (i).type->print (shift + 4);
    }
  }

  virtual int get_type (void) const {
    return NODE_TYPE_ARRAY;
  }

  virtual bool equals (tl_tree *other_) const {
    if (other_->get_type() != NODE_TYPE_ARRAY) {
      return false;
    }
    tl_tree_array *other = (tl_tree_array *)other_;
    if (flags != other->flags || args.count() != other->args.count()) {
      return false;
    }
    for (int i = 0; i < args.count(); i++) {
      if (args.get_value (i).name != other->args.get_value (i).name ||
          !args.get_value (i).type->equals (other->args.get_value (i).type)) {
        return false;
      }
    }
    return true;
  }

  virtual tl_tree *dup (void) const {
    tl_tree_array *T = (tl_tree_array *)dl::allocate (sizeof (tl_tree_array));
    //fprintf (stderr, "dup array (%p), result = %p\n", this, T);
    new (T) tl_tree_array (flags, multiplicity->dup(), args.size());

    for (int i = 0; i < args.count(); i++) {
      T->args[i] = args.get_value (i);
      T->args[i].type = T->args[i].type->dup();
    }

    return T;
  }

  virtual void destroy (void) {
    multiplicity->destroy();
    for (int i = 0; i < args.count(); i++) {
      if (args[i].type != NULL) {
        args[i].type->destroy();
      }
    }

    this->~tl_tree_array();
    dl::deallocate (this, sizeof (*this));
  }
};


const int TLS_SCHEMA_V2 = 0x3a2f9be2;
const int TLS_SCHEMA_V3 = 0xe4a8604b;
const int TLS_TYPE = 0x12eb4386;
const int TLS_COMBINATOR = 0x5c0a1ed5;
const int TLS_COMBINATOR_LEFT_BUILTIN = 0xcd211f63;
const int TLS_COMBINATOR_LEFT = 0x4c12c6d9;
const int TLS_COMBINATOR_RIGHT_V2 = 0x2c064372;
const int TLS_ARG_V2 = 0x29dfe61b;
const int TLS_TREE_NAT_CONST = 0xc09f07d7;
const int TLS_TREE_NAT_VAR = 0x90ea6f58;
const int TLS_TREE_TYPE_VAR = 0x1caa237a;
const int TLS_TREE_ARRAY = 0x80479360;
const int TLS_TREE_TYPE = 0x10f32190;

const int TLS_EXPR_TYPE = 0xecc9da78;

const int TLS_NAT_CONST = 0xdcb49bd8;
const int TLS_NAT_VAR = 0x4e8a14f0;
const int TLS_TYPE_VAR = 0x0142ceae;
const int TLS_ARRAY = 0xd9fb20de;
const int TLS_TYPE_EXPR = 0xc1863d08;


static struct {
  array <tl_type *> types;
  array <tl_type *> id_to_type;
  array <tl_type *> name_to_type;

  array <tl_combinator *> functions;
  array <tl_combinator *> id_to_function;
  array <tl_combinator *> name_to_function;

  tl_type *ReqResult;

  void **fetchIP;
} tl_config;


int get_constructor_by_name (const tl_type *t, const string &name) {
  for (int i = 0; i < t->constructors.count(); i++) {
    if (t->constructors.get_value (i)->name == name) {
      return i;
    }
  }
  return -1;
}

int get_constructor_by_id (const tl_type *t, int id) {
  for (int i = 0; i < t->constructors.count(); i++) {
    if (t->constructors.get_value (i)->id == id) {
      return i;
    }
  }
  return -1;
}

inline void tl_debug (const char *s, int n) {
//  fprintf (stderr, "%s\n", s);
}

const int MAX_VARS = 100000;
static tl_tree *vars_buffer[MAX_VARS];
static tl_tree **last_var_ptr;

tl_tree **get_var_space (tl_tree **vars, int n) {
  tl_tree **res = vars - n;

  php_assert (res >= vars_buffer);

  for (int i = 0; i < n; i++) {
    if (res[i] != NULL && res + i >= last_var_ptr) {
      res[i]->destroy();
    }
    res[i] = NULL;
  }
  while (last_var_ptr > res) {
    last_var_ptr = res;
  }

  return res;
}

void free_var_space (tl_tree **vars, int n) {
  for (int i = 0; i < n; i++) {
    if (vars[i] != NULL) {
      vars[i]->destroy();
      vars[i] = NULL;
    }
  }
}


static const int MAX_SIZE = 100000;
static void *Data_stack[MAX_SIZE];

static const int MAX_DEPTH = 10000;
static var var_stack[MAX_DEPTH];

var *last_arr_ptr;

void free_arr_space (void) {
  while (last_arr_ptr >= var_stack) {
    *last_arr_ptr-- = var();
  }
}

void clear_arr_space (void) {
  while (last_arr_ptr >= var_stack) {
    CLEAR_VAR(var, *last_arr_ptr);
    last_arr_ptr--;
  }
}

typedef tl_tree *tl_tree_ptr;
typedef void *void_ptr;

typedef void *(*function_ptr)(void **IP, void **Data, var *arr, tl_tree **vars);
#define TLUNI_NEXT return TRY_CALL(void *, void_ptr, ((*(function_ptr *) IP) (IP + 1, Data, arr, vars)))
#define TLUNI_START(IP, Data, arr, vars) TRY_CALL(void *, void_ptr, ((*(function_ptr *) IP) (IP + 1, Data, arr, vars)))
#define TLUNI_OK ((void *)1l)

static const char *tl_current_function_name;

tl_tree *store_function (const var &tl_object) {
  if (tl_config.fetchIP == NULL) {
    php_warning ("tl_rpc_query not supported due to missing TL scheme");
    return NULL;
  }
  if (!tl_object.is_array()) {
    php_warning ("Not an array passed to function rpc_tl_query");
    return NULL;
  }

  //fprintf (stderr, "Before STORE\n");
  var f = tl_object.get_value (UNDERSCORE);
  if (f.is_null()) {
    f = tl_object.get_value (0);
  }

  tl_combinator *c;
  if (unlikely (f.is_int())) {
    c = tl_config.id_to_function.get_value (f.to_int());
  } else {
    c = tl_config.name_to_function.get_value (f.to_string());
  }
  if (c == NULL) {
    php_warning ("Function \"%s\" not found in rpc_tl_query", f.to_string().c_str());
    return NULL;
  }

  tl_current_function_name = c->name.c_str();
//  fprintf (stderr, "Storing type %s\n", c->name.c_str());

  //fprintf (stderr, "Before ALLOCATE in STORE\n");
  new (var_stack) var (tl_object);

  tl_tree **vars = get_var_space (vars_buffer + MAX_VARS, c->var_count);
  tl_tree *res;
  last_arr_ptr = var_stack;
  //fprintf (stderr, "Before TLUNI_START in STORE\n");
#ifdef FAST_EXCEPTIONS
  res = (tl_tree *)((*(function_ptr *) c->IP) (c->IP + 1, Data_stack, var_stack, vars));
  if (CurException) {
    res = NULL;
    FREE_EXCEPTION;
  }
#else
  try {
    res = (tl_tree *)TLUNI_START (c->IP, Data_stack, var_stack, vars);
  } catch (Exception &e) {
    res = NULL;
  }
#endif
  //fprintf (stderr, "Before FREE in STORE\n");
  free_var_space (vars, c->var_count);
  free_arr_space();
  //fprintf (stderr, "After FREE in STORE\n");

  if (res != NULL) {
    tl_tree_type *T = (tl_tree_type *)dl::allocate (sizeof (tl_tree_type));
    new (T) tl_tree_type (0, tl_config.ReqResult, array_size (1, 0, true));
    T->children.push_back (res);

    res = T;
  }

  //fprintf (stderr, "After STORE\n");
  return res;
}

array <var> tl_fetch_error (const string &error, int error_code) {
  array <var> result;
  result.set_value (string ("__error", 7), error);
  result.set_value (string ("__error_code", 12), error_code);
  return result;
}

array <var> tl_fetch_error (const char *error, int error_code) {
  return tl_fetch_error (string (error, strlen (error)), error_code);
}

array <var> fetch_function (tl_tree *T) {
  if (tl_config.fetchIP == NULL) {
    php_warning ("tl_rpc_query_result not supported due to missing TL scheme");
    return tl_fetch_error ("TL scheme was not loaded", TL_ERROR_UNKNOWN_FUNCTION_ID);
  }
  //fprintf (stderr, "Before FETCH\n");
  php_assert (T != NULL);
  new (var_stack) var();

  int x = 0;

#ifdef FAST_EXCEPTIONS
  x = rpc_lookup_int (string(), -1);//TODO file, line
  if (x == RPC_REQ_ERROR && !CurException) {
    php_assert (tl_parse_int() == RPC_REQ_ERROR);
    if (!CurException) {
      tl_parse_long();
      if (!CurException) {
        int error_code = tl_parse_int();
        if (!CurException) {
          string error = tl_parse_string();
          if (!CurException) {
            return tl_fetch_error (error, error_code);
          }
        }
      }
    }
  }

  if (CurException) {
    array <var> result = tl_fetch_error (CurException->message, TL_ERROR_SYNTAX);
    FREE_EXCEPTION;
    return result;
  }
#else
  try {
    x = rpc_lookup_int (string(), -1);//TODO file, line
    if (x == RPC_REQ_ERROR) {
      php_assert (tl_parse_int() == RPC_REQ_ERROR);
      tl_parse_long();
      int error_code = tl_parse_int();
      string error = tl_parse_string();
      return tl_fetch_error (error, error_code);
    }
  } catch (Exception &e) {
    return tl_fetch_error (e.message, TL_ERROR_SYNTAX);
  }
#endif

  tl_debug (__FUNCTION__, -2);
  Data_stack[0] = T;
  string fetched_type = ((tl_tree_type *)T)->type->name;
  void *res;
  last_arr_ptr = var_stack;

  //fprintf (stderr, "Before TLUNI_START in FETCH\n");
#ifdef FAST_EXCEPTIONS
  res = (tl_tree *)((*(function_ptr *) tl_config.fetchIP) (tl_config.fetchIP + 1, Data_stack + 1, var_stack, vars_buffer + MAX_VARS));
  if (CurException) {
    free_arr_space();
    array <var> result = tl_fetch_error (CurException->message, TL_ERROR_SYNTAX);
    var_stack[0] = var();
    FREE_EXCEPTION;
    return result;
  }
#else
  try {
    res = TLUNI_START (tl_config.fetchIP, Data_stack + 1, var_stack, vars_buffer + MAX_VARS);
  } catch (Exception &e) {
    free_arr_space();
    var_stack[0] = var();
    return tl_fetch_error (e.message, TL_ERROR_SYNTAX);
  }
#endif
  //fprintf (stderr, "After TLUNI_START in FETCH\n");

  if (res == TLUNI_OK) {
    if (!f$fetch_eof (string(), -1)) {
      php_warning ("Not all data fetched during fetch type %s", fetched_type.c_str());
      var_stack[0] = var();
      return tl_fetch_error ("Not all data fetched", TL_ERROR_EXTRA_DATA);
    }
  } else {
    var_stack[0] = var();
    return tl_fetch_error ("Incorrect result", TL_ERROR_SYNTAX);
  }

  var result = var_stack[0];
  var_stack[0] = var();

  if (!result.is_array()) {
    return tl_fetch_error ("Result is not an array. How???", TL_ERROR_INTERNAL);
  }
  //fprintf (stderr, "After FETCH\n");
  return result.to_array();
}

static char rpc_tl_results_storage[sizeof (array <tl_tree *>)];
static array <tl_tree *> *rpc_tl_results = reinterpret_cast <array <tl_tree *> *> (rpc_tl_results_storage);
static long long rpc_tl_results_last_query_num = -1;

var f$rpc_tl_query_one (const rpc_connection &c, const var &tl_object, double timeout) {
  f$rpc_clean();

  tl_tree *result_tree = store_function (tl_object);
  if (result_tree == NULL) {
    return false;
  }

  var query_id_var = f$rpc_send (c, timeout);

  if (dl::query_num != rpc_tl_results_last_query_num) {
    new (rpc_tl_results_storage) array <tl_tree *>();
    rpc_tl_results_last_query_num = dl::query_num;
  }
  rpc_tl_results->set_value (query_id_var, result_tree);

  return query_id_var;
}

array <var> f$rpc_tl_query (const rpc_connection &c, const array <var> &tl_objects, double timeout) {
  array <var> result (tl_objects.size());
  for (typeof (tl_objects.begin()) it = tl_objects.begin(); it != tl_objects.end(); ++it) {
    result.set_value (it.get_key(), f$rpc_tl_query_one (c, it.get_value(), timeout));
  }
  //TODO flush

  return result;
}

array <var> rpc_tl_query_result_one (const var &query_id_var, long long query_id) {
  if (query_id <= 0) {
    return tl_fetch_error ("Wrong query_id", TL_ERROR_WRONG_QUERY_ID);
  }

  if (!rpc_get_and_parse (query_id)) {
    return tl_fetch_error (get_last_net_error(), TL_ERROR_UNKNOWN);
  }

  if (dl::query_num != rpc_tl_results_last_query_num) {
    return tl_fetch_error ("There was no TL queries in current script run", TL_ERROR_INTERNAL);
  }

  tl_tree *T = rpc_tl_results->get_value (query_id_var);
  rpc_tl_results->unset (query_id_var);

  array <var> tl_object = fetch_function (T);
//  fprintf (stderr, "!!! %s\n", f$serialize (tl_object).c_str());
  return tl_object;
}

array <var> f$rpc_tl_query_result_one (const var &query_id_var) {
  return rpc_tl_query_result_one (query_id_var, store_parse_number <long long> (query_id_var));
}

array <array <var> > f$rpc_tl_query_result (const array <var> &query_ids) {
  var queue_id_var = f$rpc_queue_create (query_ids);
  long long queue_id = store_parse_number <long long> (queue_id_var);

  array <array <var> > tl_objects_unsorted;
  while (true) {
    long long query_id = rpc_get_next_request_id (queue_id);
    if (query_id <= 0) {
      break;
    }
    var query_id_var = long_long_convert_to_var (query_id);

    tl_objects_unsorted[query_id_var] = rpc_tl_query_result_one (query_id_var, query_id);
  }

  array <array <var> > tl_objects (query_ids.size());
  for (array <var>::const_iterator it = query_ids.begin(); it != query_ids.end(); ++it) {
    const var &query_id_var = it.get_value();
    if (!tl_objects_unsorted.isset (query_id_var)) {
      if (f$longval (query_id_var).l <= 0) {
        tl_objects[it.get_key()] = tl_fetch_error ("Very wrong query_id", TL_ERROR_WRONG_QUERY_ID);
      } else {
        tl_objects[it.get_key()] = tl_fetch_error ((static_SB.clean() + "No answer received or duplicate/wrong query_id " + f$serialize (query_id_var)).str(), TL_ERROR_WRONG_QUERY_ID);
      }
    } else {
      tl_objects[it.get_key()] = tl_objects_unsorted[query_id_var];
    }
  }
  return tl_objects;
}


void *tls_push (void **IP, void **Data, var *arr, tl_tree **vars) {
  tl_debug (__FUNCTION__, -1);
  *(Data++) = (void *)((tl_tree *)(*(IP++)))->dup();
  TLUNI_NEXT;
}

void *tls_arr_pop (void **IP, void **Data, var *arr, tl_tree **vars) {
  tl_debug (__FUNCTION__, -1);
  *arr-- = var();
  last_arr_ptr = arr;
  TLUNI_NEXT;
}

void *tls_arr_push (void **IP, void **Data, var *arr, tl_tree **vars) {
  tl_debug (__FUNCTION__, -1);
  new (++arr) var();
  last_arr_ptr = arr;
  TLUNI_NEXT;
}

void *tls_store_int (void **IP, void **Data, var *arr, tl_tree **vars) {
  tl_debug (__FUNCTION__, -1);
  f$store_int ((int)(long)*(IP++));
  TLUNI_NEXT;
}

void *tlcomb_skip_const_int (void **IP, void **Data, var *arr, tl_tree **vars) {
  tl_debug (__FUNCTION__, -1);
  int a = TRY_CALL(int, void_ptr, tl_parse_int());
  if (a != (int)(long)*(IP++)) {
    return NULL;
  }
  TLUNI_NEXT;
}


/**** Combinator store code {{{ ****/

/****
 *
 * Data [data] => [data] result
 *
 ****/

void *tlcomb_store_any_function (void **IP, void **Data, var *arr, tl_tree **vars) {
  tl_debug (__FUNCTION__, -1);
  var v = arr->get_value (UNDERSCORE);
  if (v.is_null()) {
    v = arr->get_value (0);
    if (v.is_null()) {
      php_warning ("Function name not found in unserialize(\"%s\") during store type %s", f$serialize (*arr).c_str(), tl_current_function_name);
      return NULL;
    }
  }

  const string &name = v.to_string();
  const tl_combinator *c = tl_config.name_to_function.get_value (name);
  if (c == NULL) {
    php_warning ("Function %s not found during store type %s", name.c_str(), tl_current_function_name);
    return NULL;
  }
  tl_tree **new_vars = get_var_space (vars, c->var_count);

  void *res = TLUNI_START (c->IP, Data, arr, new_vars);
  free_var_space (new_vars, c->var_count);
  if (res == NULL) {
    return NULL;
  }
  *(Data++) = res;
  TLUNI_NEXT;
}

/****
 *
 * Data [data] result => [data]
 *
 ****/

void *tlcomb_fetch_type (void **IP, void **Data, var *arr, tl_tree **vars) {
  tl_debug (__FUNCTION__, -1);
  tl_tree_type *e = dynamic_cast <tl_tree_type *> ((tl_tree *)(*(--Data)));
  php_assert (e != NULL);
  tl_type *t = e->type;
  php_assert (t != NULL);
  bool is_bare = e->flags & FLAG_BARE;

  int l = -1;
  if (is_bare) {
    if (t->constructors_num == 1) {
      l = 0;
    }
  } else {
    int pos = tl_parse_save_pos();
    l = get_constructor_by_id (t, TRY_CALL(int, void_ptr, tl_parse_int()));
    if (l < 0 && (t->flags & FLAG_DEFAULT_CONSTRUCTOR)) {
      l = t->constructors_num - 1;
      tl_parse_restore_pos (pos);
    }
    if (l < 0) {
      e->destroy();
      return NULL;
    }
  }
  int r;
  if (l >= 0) {
    r = l + 1;
  } else {
    l = 0;
    r = t->constructors_num;
  }

  int k = tl_parse_save_pos();
  for (int n = l; n < r; n++) {
    if (r - l > 1) {
      *Data = e->dup();
    }
//    ((tl_tree *)Data[0])->print();
    tl_combinator *constructor = t->constructors.get_value (n);
    tl_tree **new_vars = get_var_space (vars, constructor->var_count);
    void *res = TLUNI_START (constructor->fetchIP, Data + 1, arr, new_vars);
    free_var_space (new_vars, constructor->var_count);
    if (res == TLUNI_OK) {
      if (!is_bare && (t->constructors_num > 1) && !(t->flags & FLAG_NOCONS)) {
        arr->set_value (UNDERSCORE, constructor->name);
      }
      if (r - l > 1) {
        e->destroy();
      }
      TLUNI_NEXT;
    }
    php_assert (tl_parse_restore_pos (k));
  }
  if (r - l > 1) {
    e->destroy();
  }
  return NULL;
}

/****
 *
 * Data: [data] result => [data]
 * IP  :
 *
 ****/
void *tlcomb_store_type (void **IP, void **Data, var *arr, tl_tree **vars) {
  tl_debug (__FUNCTION__, -1);
  tl_tree_type *e = (tl_tree_type *)(*(--Data));
  php_assert (e != NULL && e->get_type() == NODE_TYPE_TYPE);
  tl_type *t = e->type;
  php_assert (t != NULL);
//  fprintf (stderr, "%s\n", t->name.c_str());
  php_assert (t->constructors_num != 0);

  int l = -1;
  if (t->constructors_num > 1) {
    var v = arr->get_value (UNDERSCORE);
    if (v.is_null()) {
      v = arr->get_value (0);
    }
    if (!v.is_null()) {
      const string &s = v.to_string();
      l = get_constructor_by_name (t, s);
      if (l < 0) {
        php_warning ("Constructor %s not found during store type %s", s.c_str(), tl_current_function_name);
        e->destroy();
        return NULL;
      }
    }
  } else {
    l = 0;
  }
  int r;
  if (l >= 0) {
    r = l + 1;
  } else {
    l = 0;
    r = t->constructors_num;
  }

  int k = tl_parse_save_pos();
  for (int n = l; n < r; n++) {
    tl_combinator *constructor = t->constructors.get_value (n);
    if (!(e->flags & FLAG_BARE) && constructor->name != UNDERSCORE) {
      f$store_int (constructor->id);
    }
    if (r - l > 1) {
      *Data = e->dup();
    }
    tl_tree **new_vars = get_var_space (vars, constructor->var_count);
    void *res = TLUNI_START (constructor->IP, Data + 1, arr, new_vars);
    free_var_space (new_vars, constructor->var_count);
    if (res == TLUNI_OK) {
      if (r - l > 1) {
        e->destroy();
      }
      TLUNI_NEXT;
    }
    php_assert (tl_parse_restore_pos (k));
  }
  if (r - l > 1) {
    e->destroy();
  }
  php_warning ("Apropriate constructor doesn't found in unserialize(%s) during store type %s", f$serialize (*arr).c_str(), tl_current_function_name);
  return NULL;
}

/****
 *
 * IP  : id num
 *
 ***/
void *tlcomb_store_field (void **IP, void **Data, var *arr, tl_tree **vars) {
  tl_debug (__FUNCTION__, -1);
  const string *name = (const string *)(IP++);
  int num = (int)(long)*(IP++);

  var v = arr->get_value (*name);
  if (v.is_null()) {
    v = arr->get_value (num);
    if (v.is_null()) {
      php_warning ("Field \"%s\"(%d) not found during store type %s", name->c_str(), num, tl_current_function_name);
      return NULL;
    }
  }
  new (++arr) var (v);
  last_arr_ptr = arr;
  TLUNI_NEXT;
}


/****
 *
 * IP: id num
 *
 ***/

void *tlcomb_fetch_field_end (void **IP, void **Data, var *arr, tl_tree **vars) {
  tl_debug (__FUNCTION__, -1);
  const string *name = (const string *)(IP++);
  int num = (int)(long)*(IP++);

  if (arr->is_null()) {
    *arr = array <var> ();
  }
  if (name->size() != 0) {
    arr[-1].set_value (*name, *arr);
  } else {
    arr[-1].set_value (num, *arr);
  }
  *arr-- = var();
  last_arr_ptr = arr;
  TLUNI_NEXT;
}

/****
 *
 * Data: [data] arity => [data]
 * IP  : newIP
 *
 ***/
void *tlcomb_store_array (void **IP, void **Data, var *arr, tl_tree **vars) {
  tl_debug (__FUNCTION__, -1);
  void **newIP = (void **)*(IP++);
  if (!arr->is_array()) {
    php_warning ("Array expected, unserialize (\"%s\") found during store type %s", f$serialize (*arr).c_str(), tl_current_function_name);
    return NULL;
  }
  tl_tree_nat_const *c = (tl_tree_nat_const *)(*(--Data));
  php_assert (c != NULL && c->get_type() == NODE_TYPE_NAT_CONST);
  int multiplicity = c->num;
  c->destroy();
  for (int i = 0; i < multiplicity; i++) {
    if (!arr->isset (i)) {
      php_warning ("Field %d not found in array during store type %s", i, tl_current_function_name);
      return NULL;
    }
    var w = arr->get_value (i);
    new (++arr) var (w);
    last_arr_ptr = arr;
    if (TLUNI_START (newIP, Data, arr, vars) != TLUNI_OK) {
      *arr-- = var();
      last_arr_ptr = arr;
      return NULL;
    }
    *arr-- = var();
    last_arr_ptr = arr;
  }
  TLUNI_NEXT;
}

/****
 *
 * Data: [data] arity => [data]
 * IP  : newIP
 *
 ***/
void *tlcomb_fetch_array (void **IP, void **Data, var *arr, tl_tree **vars) {
  tl_debug (__FUNCTION__, -1);
  void **newIP = (void **)*(IP++);
  tl_tree_nat_const *c = (tl_tree_nat_const *)(*(--Data));
  php_assert (c != NULL && c->get_type() == NODE_TYPE_NAT_CONST);
  int multiplicity = c->num;
  c->destroy();
  *arr = array <var>();
  for (int i = 0; i < multiplicity; i++) {
    new (++arr) var();
    last_arr_ptr = arr;
    if (TLUNI_START (newIP, Data, arr, vars) != TLUNI_OK) {
      *arr-- = var();
      last_arr_ptr = arr;
      return NULL;
    }
    arr[-1].push_back (*arr);
    *arr-- = var();
    last_arr_ptr = arr;
  }
  TLUNI_NEXT;
}

void *tlcomb_fetch_int (void **IP, void **Data, var *arr, tl_tree **vars) {
  tl_debug (__FUNCTION__, -1);
  *arr = TRY_CALL(int, void_ptr, (f$fetch_int (string(), -1)));
  TLUNI_NEXT;
}

void *tlcomb_fetch_long (void **IP, void **Data, var *arr, tl_tree **vars) {
  tl_debug (__FUNCTION__, -1);
  *arr = TRY_CALL(var, void_ptr, (f$fetch_long (string(), -1)));
  TLUNI_NEXT;
}

void *tlcomb_fetch_double (void **IP, void **Data, var *arr, tl_tree **vars) {
  tl_debug (__FUNCTION__, -1);
  *arr = TRY_CALL(double, void_ptr, (f$fetch_double (string(), -1)));
  TLUNI_NEXT;
}

void *tlcomb_fetch_string (void **IP, void **Data, var *arr, tl_tree **vars) {
  tl_debug (__FUNCTION__, -1);
  *arr = TRY_CALL(string, void_ptr, (f$fetch_string (string(), -1)));
  TLUNI_NEXT;
}

void *tlcomb_fetch_false (void **IP, void **Data, var *arr, tl_tree **vars) {
  tl_debug (__FUNCTION__, -1);
  *arr = false;
  TLUNI_NEXT;
}

void *tlcomb_fetch_true (void **IP, void **Data, var *arr, tl_tree **vars) {
  tl_debug (__FUNCTION__, -1);
  *arr = true;
  TLUNI_NEXT;
}

/*****
 *
 * IP: newIP
 *
 *****/
void *tlcomb_fetch_vector (void **IP, void **Data, var *arr, tl_tree **vars) {
  tl_debug (__FUNCTION__, -1);
  int multiplicity = TRY_CALL(int, void_ptr, tl_parse_int());
  void **newIP = (void **)*(IP++);

  *arr = array <var> (array_size (multiplicity, 0, true));
  for (int i = 0; i < multiplicity; i++) {
    new (++arr) var();
    last_arr_ptr = arr;
    if (TLUNI_START (newIP, Data, arr, vars) != TLUNI_OK) {
      *arr-- = var();
      last_arr_ptr = arr;
      return NULL;
    }
    arr[-1].push_back (*arr);
    *arr-- = var();
    last_arr_ptr = arr;
  }
  TLUNI_NEXT;
}

/*****
 *
 * IP: newIP
 *
 *****/
void *tlcomb_fetch_dictionary (void **IP, void **Data, var *arr, tl_tree **vars) {
  tl_debug (__FUNCTION__, -1);
  int multiplicity = TRY_CALL(int, void_ptr, tl_parse_int());
  void **newIP = (void **)*(IP++);

  *arr = array <var> (array_size (0, multiplicity, false));
  for (int i = 0; i < multiplicity; i++) {
    new (++arr) var();
    last_arr_ptr = arr;

    string key = TRY_CALL(string, void_ptr, tl_parse_string());
    if (TLUNI_START (newIP, Data, arr, vars) != TLUNI_OK) {
      *arr-- = var();
      last_arr_ptr = arr;
      return NULL;
    }
    arr[-1].set_value (key, *arr);
    *arr-- = var();
    last_arr_ptr = arr;
  }
  TLUNI_NEXT;
}


/*****
 *
 * IP: newIP
 *
 *****/
void *tlcomb_fetch_maybe (void **IP, void **Data, var *arr, tl_tree **vars) {
  tl_debug (__FUNCTION__, -1);
  void **newIP = (void **)*(IP++);
  if (TLUNI_START (newIP, Data, arr, vars) != TLUNI_OK) {
    *arr-- = var();
    last_arr_ptr = arr;
    return NULL;
  }
  TLUNI_NEXT;
}

/*****
 *
 * IP: var_num
 *
 *****/
void *tlcomb_store_var_num (void **IP, void **Data, var *arr, tl_tree **vars) {
  tl_debug (__FUNCTION__, -1);
  int var_num = (int)(long)*(IP++);
  int num = f$intval (*arr);

  tl_tree_nat_const *T = (tl_tree_nat_const *)dl::allocate (sizeof (tl_tree_nat_const));
  new (T) tl_tree_nat_const (0, num);

  php_assert (vars[var_num] == NULL);
  vars[var_num] = T;
  f$store_int (num);
  TLUNI_NEXT;
}

/*****
 *
 * IP: var_num
 *
 *****/
void *tlcomb_fetch_var_num (void **IP, void **Data, var *arr, tl_tree **vars) {
  tl_debug (__FUNCTION__, -1);
  int num = TRY_CALL(int, void_ptr, tl_parse_int());
  *arr = num;
  int var_num = (int)(long)*(IP++);

  tl_tree_nat_const *T = (tl_tree_nat_const *)dl::allocate (sizeof (tl_tree_nat_const));
  new (T) tl_tree_nat_const (0, num);

  php_assert (vars[var_num] == NULL);
  vars[var_num] = T;

  TLUNI_NEXT;
}

/*****
 *
 * IP: var_num
 *
 *****/
void *tlcomb_fetch_check_var_num (void **IP, void **Data, var *arr, tl_tree **vars) {
  tl_debug (__FUNCTION__, -1);
  int num = TRY_CALL(int, void_ptr, tl_parse_int());
  int var_num = (int)(long)*(IP++);
  php_assert (vars[var_num] != NULL && vars[var_num]->get_type() == NODE_TYPE_NAT_CONST);

  if (num != ((tl_tree_nat_const *)vars[var_num])->num) {
    return NULL;
  }
  TLUNI_NEXT;
}

/*****
 *
 * IP: var_num flags
 *
 *****/
void *tlcomb_store_var_type (void **IP, void **Data, var *arr, tl_tree **vars) {
  php_assert ("Not supported" && 0);
  tl_debug (__FUNCTION__, -1);
  int var_num = (int)(long)*(IP++);
  int flags = (int)(long)*(IP++);
  string s = f$strval (*arr);
  tl_type *t = tl_config.name_to_type.get_value (s);
  if (t == NULL) {
    return NULL;
  }

  tl_tree_type *T = (tl_tree_type *)dl::allocate (sizeof (tl_tree_type));
  new (T) tl_tree_type (flags, t, array_size (0, 0, true));

  php_assert (vars[var_num] == NULL);
  vars[var_num] = T;
  TLUNI_NEXT;
}

void *tlcomb_store_int (void **IP, void **Data, var *arr, tl_tree **vars) {
  tl_debug (__FUNCTION__, -1);
  f$store_int (f$intval (*arr));
  TLUNI_NEXT;
}

void *tlcomb_store_long (void **IP, void **Data, var *arr, tl_tree **vars) {
  tl_debug (__FUNCTION__, -1);
  f$store_long (*arr);
  TLUNI_NEXT;
}

void *tlcomb_store_double (void **IP, void **Data, var *arr, tl_tree **vars) {
  tl_debug (__FUNCTION__, -1);
  f$store_double (f$floatval (*arr));
  TLUNI_NEXT;
}

void *tlcomb_store_string (void **IP, void **Data, var *arr, tl_tree **vars) {
  tl_debug (__FUNCTION__, -1);
  f$store_string (f$strval (*arr));
  TLUNI_NEXT;
}

/****
 *
 * IP  : newIP
 *
 ***/
void *tlcomb_store_vector (void **IP, void **Data, var *arr, tl_tree **vars) {
  tl_debug (__FUNCTION__, -1);

  void **newIP = (void **)*(IP++);

  if (!arr->is_array()) {
    php_warning ("Vector expected, unserialize (\"%s\") found during store type %s", f$serialize (*arr).c_str(), tl_current_function_name);
    return NULL;
  }
  int multiplicity = arr->count();
  f$store_int (multiplicity);

  for (int i = 0; i < multiplicity; i++) {
    if (!arr->isset (i)) {
      php_warning ("Field %d not found in vector during store type %s", i, tl_current_function_name);
      return NULL;
    }
    var w = arr->get_value (i);
    new (++arr) var (w);
    last_arr_ptr = arr;
    if (TLUNI_START (newIP, Data, arr, vars) != TLUNI_OK) {
      *arr-- = var();
      last_arr_ptr = arr;
      return NULL;
    }
    *arr-- = var();
    last_arr_ptr = arr;
  }
  TLUNI_NEXT;
}

/****
 *
 * IP  : newIP
 *
 ***/
void *tlcomb_store_dictionary (void **IP, void **Data, var *arr, tl_tree **vars) {
  tl_debug (__FUNCTION__, -1);

  void **newIP = (void **)*(IP++);

  if (!arr->is_array()) {
    php_warning ("Dictionary expected, unserialize (\"%s\") found during store type %s", f$serialize (*arr).c_str(), tl_current_function_name);
    return NULL;
  }
  int multiplicity = arr->count();
  f$store_int (multiplicity);

  const array <var> a = arr->to_array();
  for (array <var>::const_iterator p = a.begin(); p != a.end(); ++p) {
    f$store_string (f$strval (p.get_key()));

    new (++arr) var (p.get_value());
    last_arr_ptr = arr;
    if (TLUNI_START (newIP, Data, arr, vars) != TLUNI_OK) {
      *arr-- = var();
      last_arr_ptr = arr;
      return NULL;
    }
    *arr-- = var();
    last_arr_ptr = arr;
  }
  TLUNI_NEXT;
}

/*****
 *
 * IP: var_num bit_num shift
 *
 *****/
void *tlcomb_check_bit (void **IP, void **Data, var *arr, tl_tree **vars) {
  tl_debug (__FUNCTION__, -1);
  int var_num = (int)(long)*(IP++);
  int bit_num = (int)(long)*(IP++);
  int shift = (int)(long)*(IP++);

  tl_tree_nat_const *c = dynamic_cast <tl_tree_nat_const *> (vars[var_num]);
  php_assert (c != NULL);

//  fprintf (stderr, "Check bit %d of var %d and shift on %d. Var value = %d\n", bit_num, var_num, shift, c->num);

  if (!(c->num & (1 << bit_num))) {
    IP += shift;
  }
  TLUNI_NEXT;
}

/*****
 *
 * Data: [Data] res => [Data] childn ... child2 child1
 * IP: type
 *
 *****/
void *tluni_check_type (void **IP, void **Data, var *arr, tl_tree **vars) {
  tl_debug (__FUNCTION__, -1);
  tl_tree_type *res = (tl_tree_type *)(*(--Data));
  php_assert (res->get_type() == NODE_TYPE_TYPE);

  if (res->type != *(IP++)) {
    res->destroy();
    return NULL;
  }

  for (int i = res->children.count() - 1; i >= 0; i--) {
    *(Data++) = res->children[i];
  }

  res->~tl_tree_type();
  dl::deallocate (res, sizeof (tl_tree_type));
  TLUNI_NEXT;
}

/*****
 *
 * Data: [Data] res => [Data]
 * IP: const
 *
 *****/
void *tluni_check_nat_const (void **IP, void **Data, var *arr, tl_tree **vars) {
  tl_debug (__FUNCTION__, -1);
  tl_tree_nat_const *res = (tl_tree_nat_const *)(*(--Data));
  php_assert (res->get_type() == NODE_TYPE_NAT_CONST);

  if (res->num != (long)*(IP++)) {
    res->destroy();
    return NULL;
  }

  res->destroy();
  TLUNI_NEXT;
}

/*****
 *
 * Data: [Data] res => [Data] childn ... child2 child1 multiplicity
 * IP: array
 *
 *****/
void *tluni_check_array (void **IP, void **Data, var *arr, tl_tree **vars) {
  tl_debug (__FUNCTION__, -1);
  tl_tree_array *res = (tl_tree_array *)(*(--Data));
  php_assert (res->get_type() == NODE_TYPE_ARRAY);

  if (!res->equals ((tl_tree *)*(IP++))) {
    res->destroy();
    return NULL;
  }
  for (int i = res->args.count() - 1; i >= 0; i--) {
    *(Data++) = &res->args[i];
  }
  *(Data++) = res->multiplicity;

  void *result = TLUNI_START(IP, Data, arr, vars);
  res->~tl_tree_array();
  dl::deallocate (res, sizeof (tl_tree_array));
  return result;
}

/*****
 *
 * Data [Data] child => [Data] type
 * IP arg_name
 *
 *****/
void *tluni_check_arg (void **IP, void **Data, var *arr, tl_tree **vars) {
  tl_debug (__FUNCTION__, -1);
  arg *res = (arg *)(*(--Data));
  const string *name = (const string *)(IP++);
  php_assert (name != NULL);

  if (strcmp (name->c_str(), res->name.c_str())) {
    res->type->destroy();
    return NULL;
  }
  *(Data++) = res->type;
  TLUNI_NEXT;
}

/*****
 *
 * Data [Data] value => [Data]
 * IP var_num add_value
 *
 *****/
void *tluni_set_nat_var (void **IP, void **Data, var *arr, tl_tree **vars) {
  tl_debug (__FUNCTION__, -1);
  int var_num = (int)(long)*(IP++);

  tl_tree_nat_const *c = (tl_tree_nat_const *)(*(--Data));
  php_assert (c->get_type() == NODE_TYPE_NAT_CONST);
  c->num += (int)(long)*(IP++);

  if (c->num < 0) {
    c->destroy();
    return NULL;
  }

  php_assert (vars[var_num] == NULL);
  vars[var_num] = c;
  TLUNI_NEXT;
}

/*****
 *
 * Data [Data] value => [Data]
 * IP var_num
 *
 *****/
void *tluni_set_type_var (void **IP, void **Data, var *arr, tl_tree **vars) {
  tl_debug (__FUNCTION__, -1);
  int var_num = (int)(long)*(IP++);

  php_assert (vars[var_num] == NULL);
  vars[var_num] = (tl_tree *)(*(--Data));
  TLUNI_NEXT;
}


/*****
 *
 * Data [Data] value => [Data]
 * IP var_num add_value
 *
 *****/
void *tluni_check_nat_var (void **IP, void **Data, var *arr, tl_tree **vars) {
  tl_debug (__FUNCTION__, -1);
  int var_num = (int)(long)*(IP++);
  php_assert (vars[var_num] != NULL);
  if (vars[var_num]->get_type() != NODE_TYPE_NAT_CONST) {
    return NULL;
  }

  tl_tree_nat_const *v = (tl_tree_nat_const *)(vars[var_num]);
  tl_tree_nat_const *c = (tl_tree_nat_const *)(*(--Data));
  if (c->get_type() != NODE_TYPE_NAT_CONST || v->num != c->num + (int)(long)*(IP++)) {
    c->destroy();
    return NULL;
  }
  c->destroy();
  TLUNI_NEXT;
}


/*****
 *
 * Data [Data] value => [Data]
 * IP var_num
 *
 *****/
void *tluni_check_type_var (void **IP, void **Data, var *arr, tl_tree **vars) {
  tl_debug (__FUNCTION__, -1);
  tl_tree *x = (tl_tree *)(*(--Data));
  tl_tree *y = vars[(long)*(IP++)];
  php_assert (y != NULL);
  if (!y->equals (x)) {
    x->destroy();
    return NULL;
  }
  x->destroy();
  TLUNI_NEXT;
}


/*****
 *
 * Data [Data] multiplicity type_1 ... type_n => [Data] array
 * IP flags args_num name_n ... name_1
 *
 *****/
void *tlsub_create_array (void **IP, void **Data, var *arr, tl_tree **vars) {
  tl_debug (__FUNCTION__, -1);
  int flags = (int)(long)*(IP++);
  int args_num = (int)(long)*(IP++);

  tl_tree_array *T = (tl_tree_array *)dl::allocate (sizeof (tl_tree_array));
  new (T) tl_tree_array (flags, NULL, array_size (args_num, 0, true));

  for (int i = 0; i < args_num; i++) {
    T->args[i];//allocate vector
  }
  for (int i = args_num - 1; i >= 0; i--) {
    const string *name = (const string *)(IP++);
    T->args[i].name = *name;
    T->args[i].type = (tl_tree *)*(--Data);
  }
  T->multiplicity = (tl_tree *)*(--Data);
  *(Data++) = (void *)T;
  TLUNI_NEXT;
}


/*****
 *
 * Data [Data] type1 ... typen  => [Data] type
 * IP flags type_ptr
 *
 *****/
void *tlsub_create_type (void **IP, void **Data, var *arr, tl_tree **vars) {
  tl_debug (__FUNCTION__, -1);

  int flags = (int)(long)*(IP++);
  tl_type *type = (tl_type *)*(IP++);
  int children_num = type->arity;

  tl_tree_type *T = (tl_tree_type *)dl::allocate (sizeof (tl_tree_type));
  new (T) tl_tree_type (flags, type, array_size (children_num, 0, true));

  for (int i = 0; i < children_num; i++) {
    T->children[i] = NULL;//allocate vector
  }
  for (int i = children_num - 1; i >= 0; i--) {
    T->children[i] = (tl_tree *)*(--Data);
  }
  *(Data++) = T;
  TLUNI_NEXT;
}

void *tlsub_ret_ok (void **IP, void **Data, var *arr, tl_tree **vars) {
  tl_debug (__FUNCTION__, -1);
  return TLUNI_OK;
}

void *tlsub_ret (void **IP, void **Data, var *arr, tl_tree **vars) {
  tl_debug (__FUNCTION__, -1);
  return *(--Data);
}

void *tlsub_push_type_var (void **IP, void **Data, var *arr, tl_tree **vars) {
  tl_debug (__FUNCTION__, -1);
  *(Data++) = vars[(long)*(IP++)]->dup();
  TLUNI_NEXT;
}

void *tlsub_push_nat_var (void **IP, void **Data, var *arr, tl_tree **vars) {
  tl_debug (__FUNCTION__, -1);
  int var_num = (int)(long)*(IP++);
  php_assert (vars[var_num] != NULL && vars[var_num]->get_type() == NODE_TYPE_NAT_CONST);
  int num = ((tl_tree_nat_const *)vars[var_num])->num + (int)(long)*(IP++);

  tl_tree_nat_const *T = (tl_tree_nat_const *)dl::allocate (sizeof (tl_tree_nat_const));
  new (T) tl_tree_nat_const (vars[var_num]->flags, num);

  *(Data++) = T;
  TLUNI_NEXT;
}

#undef TLUNI_NEXT
#undef TLUNI_START
#undef TLUNI_OK


void **IP_dup (void **IP, int len) {
  php_assert (!dl::query_num && len > 0);
  void **IP_res = (void **)dl::allocate ((dl::size_type)sizeof (void *) * len);
  memcpy (IP_res, IP, sizeof (void *) * len);
  return IP_res;
}


int gen_uni (tl_tree *t, void **IP, int max_size, int *vars_int) {
  php_assert (max_size > 10);
  php_assert (t != NULL);
  int l = 0;
  switch (t->get_type()) {
    case NODE_TYPE_TYPE: {
      tl_tree_type *t1 = dynamic_cast <tl_tree_type *> (t);
      php_assert (t1 != NULL);
      IP[l++] = (void *)tluni_check_type;
      IP[l++] = (void *)t1->type;
      for (int i = 0; i < t1->children.count(); i++) {
        l += gen_uni (t1->children.get_value (i), IP + l, max_size - l, vars_int);
      }
      return l;
    }
    case NODE_TYPE_NAT_CONST: {
      tl_tree_nat_const *t1 = dynamic_cast <tl_tree_nat_const *> (t);
      php_assert (t1 != NULL);
      IP[l++] = (void *)tluni_check_nat_const;
      IP[l++] = (void *)(long)t1->num;
      return l;
    }
    case NODE_TYPE_ARRAY: {
      tl_tree_array *t1 = dynamic_cast <tl_tree_array *> (t);
      php_assert (t1 != NULL);
      IP[l++] = (void *)tluni_check_array;
      IP[l++] = (void *)t;
      l += gen_uni (t1->multiplicity, IP + l, max_size - l, vars_int);
      for (int i = 0; i < t1->args.count(); i++) {
        IP[l++] = (void *)tluni_check_arg;
        IP[l++] = *(void **)&t1->args[i].name;
        l += gen_uni (t1->args[i].type, IP + l, max_size - l, vars_int);
      }
      return l;
    }
    case NODE_TYPE_VAR_TYPE: {
      tl_tree_var_type *t1 = dynamic_cast <tl_tree_var_type *> (t);
      php_assert (t1 != NULL);

      int var_num = t1->var_num;
      if (!vars_int[var_num]) {
        IP[l++] = (void *)tluni_set_type_var;
        IP[l++] = (void *)(long)var_num;
        vars_int[var_num] = 1;
      } else if (vars_int[var_num] == 1) {
        IP[l++] = (void *)tluni_check_type_var;
        IP[l++] = (void *)(long)var_num;
      } else {
        php_assert (0);
      }
      return l;
    }
    case NODE_TYPE_VAR_NUM: {
      tl_tree_var_num *t1 = dynamic_cast <tl_tree_var_num *> (t);
      php_assert (t1 != NULL);

      int var_num = t1->var_num;
      if (!vars_int[var_num]) {
        IP[l++] = (void *)tluni_set_nat_var;
        IP[l++] = (void *)(long)var_num;
        IP[l++] = (void *)(long)t1->diff;
        vars_int[var_num] = 2;
      } else if (vars_int[var_num] == 2) {
        IP[l++] = (void *)tluni_check_nat_var;
        IP[l++] = (void *)(long)var_num;
        IP[l++] = (void *)(long)t1->diff;
      } else {
        php_assert (0);
      }
      return l;
    }
    default:
      php_assert (0);
  }
  return -1;
}

int gen_create (tl_tree *t, void **IP, int max_size, int *vars_int) {
  php_assert (max_size > 10);
  int l = 0;
  if (t->flags & FLAG_NOVAR) {
    IP[l++] = (void *)tls_push;
    IP[l++] = (void *)t;
    return l;
  }
  switch (t->get_type()) {
    case NODE_TYPE_TYPE: {
      tl_tree_type *t1 = (tl_tree_type *)t;
      for (int i = 0; i < t1->children.count(); i++) {
        l += gen_create (t1->children[i], IP + l, max_size - l, vars_int);
      }
      php_assert (max_size > l + 10);
      IP[l++] = (void *)tlsub_create_type;
      IP[l++] = (void *)(long)(t1->flags & FLAGS_MASK);
      IP[l++] = (void *)t1->type;
      return l;
    }
    case NODE_TYPE_ARRAY: {
      tl_tree_array *t1 = (tl_tree_array *)t;
      php_assert (t1->multiplicity != NULL);
      l += gen_create (t1->multiplicity, IP + l, max_size - l, vars_int);

      for (int i = 0; i < t1->args.count(); i++) {
        l += gen_create (t1->args[i].type, IP + l, max_size - l, vars_int);
      }
      php_assert (max_size > l + 10 + t1->args.count());

      IP[l++] = (void *)tlsub_create_array;
      IP[l++] = (void *)(long)(t1->flags & FLAGS_MASK);
      IP[l++] = (void *)(long)t1->args.count();
      for (int i = t1->args.count() - 1; i >= 0; i--) {
        IP[l++] = *(void **)&t1->args[i].name;
      }
      return l;
    }
    case NODE_TYPE_VAR_TYPE: {
      tl_tree_var_type *t1 = (tl_tree_var_type *)t;
      IP[l++] = (void *)tlsub_push_type_var;
      IP[l++] = (void *)(long)t1->var_num;
      return l;
    }
    case NODE_TYPE_VAR_NUM: {
      tl_tree_var_num *t1 = (tl_tree_var_num *)t;
      IP[l++] = (void *)tlsub_push_nat_var;
      IP[l++] = (void *)(long)t1->var_num;
      IP[l++] = (void *)(long)t1->diff;
      return l;
    }
    default:
      php_assert (false);
      return -1;
  }
}

int gen_field (const arg &arg, void **IP, int max_size, int *vars_int, int num, bool flat);
int gen_array_store (tl_tree_array *a, void **IP, int max_size, int *vars_int) {
  php_assert (max_size > 10);
  int l = 0;
  for (int i = 0; i < a->args.count(); i++) {
    l += gen_field (a->args[i], IP + l, max_size - l, vars_int, i, a->args.count() == 1);
  }
  php_assert (max_size > l + 1);
  IP[l++] = (void *)tlsub_ret_ok;
  return l;
}

int gen_field_fetch (const arg &arg, void **IP, int max_size, int *vars_int, int num, bool flat);
int gen_array_fetch (tl_tree_array *a, void **IP, int max_size, int *vars_int) {
  php_assert (max_size > 10);
  int l = 0;
  int args_num = a->args.count();
  for (int i = 0; i < args_num; i++) {
    l += gen_field_fetch (a->args[i], IP + l, max_size - l, vars_int, i, args_num == 1);
  }
  php_assert (max_size > l + 1);
  IP[l++] = (void *)tlsub_ret_ok;
  return l;
}

int gen_constructor_store (tl_combinator &c, void **IP, int max_size);
int gen_constructor_fetch (tl_combinator &c, void **IP, int max_size);

int gen_field (const arg &arg, void **IP, int max_size, int *vars_int, int num, bool flat) {
  php_assert (max_size > 10);
  int l = 0;
  if (arg.exist_var_num >= 0) {
    IP[l++] = (void *)tlcomb_check_bit;
    IP[l++] = (void *)(long)arg.exist_var_num;
    IP[l++] = (void *)(long)arg.exist_var_bit;
    IP[l++] = NULL;//temporary
  }
  if (!flat) {
    IP[l++] = (void *)tlcomb_store_field;
    IP[l++] = *(void **)&arg.name;
    IP[l++] = (void *)(long)num;
  }
  if (arg.var_num >= 0) {
    php_assert (arg.type->get_type() == NODE_TYPE_TYPE);
    tl_tree_type *arg_type = (tl_tree_type *)arg.type;
    int id = arg_type->type->id;
    if (id == ID_VAR_TYPE) {
      php_assert ("Not supported" && 0);
      IP[l++] = (void *)tlcomb_store_var_type;
      IP[l++] = (void *)(long)arg.var_num;
      IP[l++] = (void *)(long)(arg_type->flags & FLAGS_MASK);
    } else {
      php_assert (id == ID_VAR_NUM);
      IP[l++] = (void *)tlcomb_store_var_num;
      IP[l++] = (void *)(long)arg.var_num;
    }
  } else {
    int type = arg.type->get_type();
    if ((type == NODE_TYPE_TYPE || type == NODE_TYPE_VAR_TYPE)) {
      tl_tree_type *t1 = dynamic_cast <tl_tree_type *> ((tl_tree *)(arg.type));

      if (0 && t1 != NULL && t1->type->arity == 0 && t1->type->constructors_num == 1) {
        tl_combinator *constructor = t1->type->constructors[0];
        if (!(t1->flags & FLAG_BARE)) {
          IP[l++] = (void *)tls_store_int;
          IP[l++] = (void *)(long)constructor->id;
        }
        if (!constructor->IP_len) {
          php_assert (gen_constructor_store (*constructor, IP + l, max_size - l) > 0);
        }
        void **IP_ = constructor->IP;
        php_assert (constructor->IP_len >= 2);
        php_assert (IP_[0] == (void *)tluni_check_type);
        php_assert (IP_[1] == (void *)t1->type);
        php_assert (max_size >= l + constructor->IP_len + 10);
        memcpy (IP + l, IP_ + 2, sizeof (void *) * (constructor->IP_len - 2));

        l += constructor->IP_len - 2;
        php_assert (IP[l - 1] == (void *)tlsub_ret_ok);
        l--;
      } else {
        l += gen_create (arg.type, IP + l, max_size - l, vars_int);
        php_assert (max_size > 10 + l);
        IP[l++] = (void *)tlcomb_store_type;
      }
    } else {
      php_assert (type == NODE_TYPE_ARRAY);
      l += gen_create (((tl_tree_array *)arg.type)->multiplicity, IP + l, max_size - l, vars_int);
      php_assert (max_size > l + 10);
      IP[l++] = (void *)tlcomb_store_array;
      void *newIP[1000];
      IP[l++] = (void *)IP_dup (newIP, gen_array_store (((tl_tree_array *)arg.type), newIP, 1000, vars_int));
    }
  }
  php_assert (max_size > l + 10);
  if (!flat) {
    IP[l++] = (void *)tls_arr_pop;
  }
  if (arg.exist_var_num >= 0) {
    IP[3] = (void *)(long)(l - 4);
  }
  return l;
}

int gen_field_fetch (const arg &arg, void **IP, int max_size, int *vars_int, int num, bool flat) {
  php_assert (max_size > 30);
  int l = 0;
  if (arg.exist_var_num >= 0) {
    IP[l++] = (void *)tlcomb_check_bit;
    IP[l++] = (void *)(long)arg.exist_var_num;
    IP[l++] = (void *)(long)arg.exist_var_bit;
    IP[l++] = NULL;//temporary
  }
  if (!flat) {
    IP[l++] = (void *)tls_arr_push;
  }
  if (arg.var_num >= 0) {
    php_assert (arg.type->get_type() == NODE_TYPE_TYPE);
    int t = ((tl_tree_type *)arg.type)->type->id;
    if (t == ID_VAR_TYPE) {
      php_assert ("Not supported yet\n" && 0);
    } else {
      php_assert (t == ID_VAR_NUM);
      if (vars_int[arg.var_num] == 0) {
        IP[l++] = (void *)tlcomb_fetch_var_num;
        IP[l++] = (void *)(long)arg.var_num;
        vars_int[arg.var_num] = 2;
      } else if (vars_int[arg.var_num] == 2) {
        IP[l++] = (void *)tlcomb_fetch_check_var_num;
        IP[l++] = (void *)(long)arg.var_num;
      } else {
        php_assert (0);
        return -1;
      }
    }
  } else {
    int t = arg.type->get_type();
    if (t == NODE_TYPE_TYPE || t == NODE_TYPE_VAR_TYPE) {
      tl_tree_type *t1 = dynamic_cast <tl_tree_type *> ((tl_tree *)(arg.type));

      if (0 && t1 != NULL && t1->type->arity == 0 && t1->type->constructors_num == 1) {
        tl_combinator *constructor = t1->type->constructors[0];
        if (!(t1->flags & FLAG_BARE)) {
          IP[l++] = (void *)tlcomb_skip_const_int;
          IP[l++] = (void *)(long)constructor->id;
        }
        if (!constructor->fetchIP_len) {
          php_assert (gen_constructor_fetch (*constructor, IP + l, max_size - l) > 0);
        }
        void **IP_ = constructor->fetchIP;
        php_assert (constructor->fetchIP_len >= 2);
        php_assert (IP_[0] == (void *)tluni_check_type);
        php_assert (IP_[1] == (void *)t1->type);
        php_assert (max_size >= l + constructor->fetchIP_len + 10);
        memcpy (IP + l, IP_ + 2, sizeof (void *) * (constructor->fetchIP_len - 2));

        l += constructor->fetchIP_len - 2;
        php_assert (IP[l - 1] == (void *)tlsub_ret_ok);
        l--;
      } else {
        l += gen_create (arg.type, IP + l, max_size - l, vars_int);
        php_assert (max_size > l + 10);
        IP[l++] = (void *)tlcomb_fetch_type;
      }
    } else {
      php_assert (t == NODE_TYPE_ARRAY);
      l += gen_create (((tl_tree_array *)arg.type)->multiplicity, IP + l, max_size - l, vars_int);
      php_assert (max_size > l + 10);
      IP[l++] = (void *)tlcomb_fetch_array;
      void *newIP[1000];
      IP[l++] = IP_dup (newIP, gen_array_fetch (((tl_tree_array *)arg.type), newIP, 1000, vars_int));
    }
  }
  php_assert (max_size > l + 10);
  if (!flat) {
    IP[l++] = (void *)tlcomb_fetch_field_end;
    IP[l++] = *(void **)&arg.name;
    IP[l++] = (void *)(long)num;
  }
  if (arg.exist_var_num >= 0) {
    IP[3] = (void *)(long)(l - 4);
  }
  return l;
}

int gen_field_excl (const arg &arg, void **IP, int max_size, int *vars_int, int num) {
  php_assert (max_size > 10);
  int l = 0;
  IP[l++] = (void *)tlcomb_store_field;
  IP[l++] = *(void **)&arg.name;
  IP[l++] = (void *)(long)num;

  php_assert (arg.var_num < 0);
  int t = arg.type->get_type();
  php_assert (t == NODE_TYPE_TYPE || t == NODE_TYPE_VAR_TYPE);
  IP[l++] = (void *)tlcomb_store_any_function;
  l += gen_uni (arg.type, IP + l, max_size - l, vars_int);
  php_assert (max_size > 1 + l);

  IP[l++] = (void *)tls_arr_pop;
  return l;
}

int gen_constructor_store (tl_combinator &c, void **IP, int max_size) {
  if (c.IP != NULL) {
    return c.IP_len;
  }
  php_assert (max_size > 10);

  int vars_int[c.var_count];
  memset (vars_int, 0, sizeof (int) * c.var_count);
  int l = gen_uni (c.result, IP, max_size, vars_int);

  if (c.id == ID_INT) {
    IP[l++] = (void *)tlcomb_store_int;
  } else if (c.id == ID_LONG) {
    IP[l++] = (void *)tlcomb_store_long;
  } else if (c.id == ID_STRING) {
    IP[l++] = (void *)tlcomb_store_string;
  } else if (c.id == ID_DOUBLE) {
    IP[l++] = (void *)tlcomb_store_double;
  } else if (c.id == ID_VECTOR) {
    IP[l++] = (void *)tlcomb_store_vector;
    void *tIP[4];
    tIP[0] = (void *)tlsub_push_type_var;
    tIP[1] = (void *)(long)0;
    tIP[2] = (void *)tlcomb_store_type;
    tIP[3] = (void *)tlsub_ret_ok;
    IP[l++] = (void *)IP_dup (tIP, 4);
  } else if (c.id == ID_DICTIONARY) {
    IP[l++] = (void *)tlcomb_store_dictionary;
    void *tIP[4];
    tIP[0] = (void *)tlsub_push_type_var;
    tIP[1] = (void *)(long)0;
    tIP[2] = (void *)tlcomb_store_type;
    tIP[3] = (void *)tlsub_ret_ok;
    IP[l++] = (void *)IP_dup (tIP, 4);
  } else {
    int z = 0;
    if (c.result->get_type() == NODE_TYPE_TYPE) {
      tl_type *t = ((tl_tree_type *)c.result)->type;
      if (t->constructors_num == 1) {
        for (int i = 0; i < c.args.count(); i++) {
          if (!(c.args[i].flags & FLAG_OPT_VAR)) {
            z++;
          }
        }
      }
    }
    for (int i = 0; i < c.args.count(); i++) {
      if (!(c.args[i].flags & FLAG_OPT_VAR)) {
        l += gen_field (c.args[i], IP + l, max_size - l, vars_int, i + 1, z == 1);
      }
    }
    php_assert (max_size > 10);
  }

  IP[l++] = (void *)tlsub_ret_ok;
  c.IP = IP_dup (IP, l);
  c.IP_len = l;
  return l;
}

int gen_function_store (tl_combinator &c, void **IP, int max_size) {
  php_assert (max_size > 10);
  php_assert (c.IP == NULL);
  int l = 0;
  IP[l++] = (void *)tls_store_int;
  IP[l++] = (void *)(long)c.id;

  int vars_int[c.var_count];
  memset (vars_int, 0, sizeof (int) * c.var_count);
  for (int i = 0; i < c.args.count(); i++) {
    if (!(c.args[i].flags & FLAG_OPT_VAR)) {
      if (c.args[i].flags & FLAG_EXCL) {
        l += gen_field_excl (c.args[i], IP + l, max_size - l, vars_int, i + 1);
      } else {
        l += gen_field (c.args[i], IP + l, max_size - l, vars_int, i + 1, false);
      }
    }
  }
  l += gen_create (c.result, IP + l, max_size - l, vars_int);
  php_assert (max_size > 1 + l);
  IP[l++] = (void *)tlsub_ret;
  c.IP = IP_dup (IP, l);
  c.IP_len = l;
  return l;
}

int gen_constructor_fetch (tl_combinator &c, void **IP, int max_size) {
  if (c.fetchIP) {
    return c.fetchIP_len;
  }
  php_assert (max_size > 10);

  int vars_int[c.var_count];
  memset (vars_int, 0, sizeof (int) * c.var_count);
  int l = gen_uni (c.result, IP, max_size, vars_int);

  if (c.id == ID_INT) {
    IP[l++] = (void *)tlcomb_fetch_int;
  } else if (c.id == ID_LONG) {
    IP[l++] = (void *)tlcomb_fetch_long;
  } else if (c.id == ID_STRING) {
    IP[l++] = (void *)tlcomb_fetch_string;
  } else if (c.id == ID_DOUBLE) {
    IP[l++] = (void *)tlcomb_fetch_double;
  } else if (c.id == ID_VECTOR) {
    IP[l++] = (void *)tlcomb_fetch_vector;
    void *tIP[4];
    tIP[0] = (void *)tlsub_push_type_var;
    tIP[1] = (void *)(long)0;
    tIP[2] = (void *)tlcomb_fetch_type;
    tIP[3] = (void *)tlsub_ret_ok;
    IP[l++] = (void *)IP_dup (tIP, 4);
  } else if (c.id == ID_DICTIONARY) {
    IP[l++] = (void *)tlcomb_fetch_dictionary;
    void *tIP[4];
    tIP[0] = (void *)tlsub_push_type_var;
    tIP[1] = (void *)(long)0;
    tIP[2] = (void *)tlcomb_fetch_type;
    tIP[3] = (void *)tlsub_ret_ok;
    IP[l++] = (void *)IP_dup (tIP, 4);
  } else if (c.id == ID_MAYBE_TRUE) {
    IP[l++] = (void *)tlcomb_fetch_maybe;
    void *tIP[4];
    tIP[0] = (void *)tlsub_push_type_var;
    tIP[1] = (void *)(long)0;
    tIP[2] = (void *)tlcomb_fetch_type;
    tIP[3] = (void *)tlsub_ret_ok;
    IP[l++] = (void *)IP_dup (tIP, 4);
  } else if (c.id == ID_MAYBE_FALSE || c.id == ID_BOOL_FALSE) {
    IP[l++] = (void *)tlcomb_fetch_false;
  } else if (c.id == ID_BOOL_TRUE) {
    IP[l++] = (void *)tlcomb_fetch_true;
  } else {
    int z = 0;
    if (c.result->get_type() == NODE_TYPE_TYPE) {
      tl_type *t = ((tl_tree_type *)c.result)->type;
      if (t->constructors_num == 1) {
        for (int i = 0; i < c.args.count(); i++) {
          if (!(c.args[i].flags & FLAG_OPT_VAR)) {
            z++;
          }
        }
      }
    }
    for (int i = 0; i < c.args.count(); i++) {
      if (!(c.args[i].flags & FLAG_OPT_VAR)) {
        l += gen_field_fetch (c.args[i], IP + l, max_size - l, vars_int, i + 1, z == 1);
      }
    }
    php_assert (max_size > 10 + l);
  }

  IP[l++] = (void *)tlsub_ret_ok;
  c.fetchIP = IP_dup (IP, l);
  c.fetchIP_len = l;
  return l;
}

void gen_function_fetch (void ** &IP_res, void **IP, int l) {
  IP[0] = (void *)tlcomb_fetch_type;
  IP[1] = (void *)tlsub_ret_ok;
  IP_res = IP_dup (IP, l);
}


static int tl_schema_version = -1;

tl_tree *read_tree (int *var_count);
tl_tree *read_expr (int *var_count);
tl_tree *read_nat_expr (int *var_count);
array <arg> read_args_list (int *var_count);

tl_tree *read_num_const (int *var_count) {
  int num = TRY_CALL_EXIT(int, "Wrong TL-scheme specified.", tl_parse_int());

  tl_tree_nat_const *T = (tl_tree_nat_const *)dl::allocate (sizeof (tl_tree_nat_const));
  new (T) tl_tree_nat_const (FLAG_NOVAR, num);
  return T;
}

tl_tree *read_num_var (int *var_count) {
  int diff = TRY_CALL_EXIT(int, "Wrong TL-scheme specified.", tl_parse_int());
  int var_num = TRY_CALL_EXIT(int, "Wrong TL-scheme specified.", tl_parse_int());

  if (var_num >= *var_count) {
    *var_count = var_num + 1;
  }

  tl_tree_var_num *T = (tl_tree_var_num *)dl::allocate (sizeof (tl_tree_var_num));
  new (T) tl_tree_var_num (0, var_num, diff);
  return T;
}

tl_tree *read_type_var (int *var_count) {
  int var_num = TRY_CALL_EXIT(int, "Wrong TL-scheme specified.", tl_parse_int());
  int flags = TRY_CALL_EXIT(int, "Wrong TL-scheme specified.", tl_parse_int());

  if (var_num >= *var_count) {
    *var_count = var_num + 1;
  }
  php_assert (!(flags & (FLAG_NOVAR | FLAG_BARE)));

  tl_tree_var_type *T = (tl_tree_var_type *)dl::allocate (sizeof (tl_tree_var_type));
  new (T) tl_tree_var_type (flags, var_num);
  return T;
}

tl_tree *read_array (int *var_count) {
  int flags = FLAG_NOVAR;
  tl_tree *multiplicity = read_nat_expr (var_count);

  tl_tree_array *T = (tl_tree_array *)dl::allocate (sizeof (tl_tree_array));
  new (T) tl_tree_array (flags, multiplicity, read_args_list (var_count));

  for (int i = 0; i < T->args.count(); i++) {
    if (!(T->args.get_value (i).flags & FLAG_NOVAR)) {
      T->flags &= ~FLAG_NOVAR;
    }
  }
  return T;
}

tl_tree *read_type (int *var_count) {
  tl_type *type = tl_config.id_to_type.get_value (TRY_CALL_EXIT(int, "Wrong TL-scheme specified.", tl_parse_int()));
  php_assert (type != NULL);
  int flags = TRY_CALL_EXIT(int, "Wrong TL-scheme specified.", tl_parse_int()) | FLAG_NOVAR;
  php_assert (type->arity == TRY_CALL_EXIT(int, "Wrong TL-scheme specified.", tl_parse_int()));

  tl_tree_type *T = (tl_tree_type *)dl::allocate (sizeof (tl_tree_type));
  new (T) tl_tree_type (flags, type, array_size (type->arity, 0, true));

  for (int i = 0; i < type->arity; i++) {
    tl_tree *child = read_expr (var_count);

    T->children.push_back (child);
    if (!(child->flags & FLAG_NOVAR)) {
      T->flags &= ~FLAG_NOVAR;
    }
  }
  return T;
}

tl_tree *read_tree (int *var_count) {
  int tree_type = TRY_CALL_EXIT(int, "Wrong TL-scheme specified.", tl_parse_int());
  switch (tree_type) {
    case TLS_TREE_NAT_CONST:
      return read_num_const (var_count);
    case TLS_TREE_NAT_VAR:
      return read_num_var (var_count);
    case TLS_TREE_TYPE_VAR:
      return read_type_var (var_count);
    case TLS_TREE_TYPE:
      return read_type (var_count);
    case TLS_TREE_ARRAY:
      return read_array (var_count);
    default:
      fprintf (stderr, "tree_type = %d\n", tree_type);
      php_assert (0);
      return NULL;
  }
}

tl_tree *read_type_expr (int *var_count) {
  int tree_type = TRY_CALL_EXIT(int, "Wrong TL-scheme specified.", tl_parse_int());
  switch (tree_type) {
    case TLS_TYPE_VAR:
      return read_type_var (var_count);
    case TLS_TYPE_EXPR:
      return read_type (var_count);
    case TLS_ARRAY:
      return read_array (var_count);
    default:
      fprintf (stderr, "tree_type = %d\n", tree_type);
      php_assert (0);
      return NULL;
  }
}

tl_tree *read_nat_expr (int *var_count) {
  int tree_type = TRY_CALL_EXIT(int, "Wrong TL-scheme specified.", tl_parse_int());
  switch (tree_type) {
    case TLS_NAT_CONST:
      return read_num_const (var_count);
    case TLS_NAT_VAR:
      return read_num_var (var_count);
    default:
      fprintf (stderr, "tree_type = %d\n", tree_type);
      php_assert (0);
      return NULL;
  }
}

tl_tree *read_expr (int *var_count) {
  int tree_type = TRY_CALL_EXIT(int, "Wrong TL-scheme specified.", tl_parse_int());
  switch (tree_type) {
    case TLS_NAT_CONST:
      return read_nat_expr (var_count);
    case TLS_EXPR_TYPE:
      return read_type_expr (var_count);
    default:
      fprintf (stderr, "tree_type = %d\n", tree_type);
      php_assert (0);
      return NULL;
  }
}

array <arg> read_args_list (int *var_count) {
  int schema_flag_opt_field = 2 << (tl_schema_version >= 3);
  int schema_flag_has_vars = schema_flag_opt_field ^ 6;

  int args_num = TRY_CALL_EXIT(int, "Wrong TL-scheme specified.", tl_parse_int());
  array <arg> args = array <arg> (array_size (args_num, 0, true));
  for (int i = 0; i < args_num; i++) {
    arg cur_arg;

    php_assert (TRY_CALL_EXIT(int, "Wrong TL-scheme specified.", tl_parse_int()) == TLS_ARG_V2);

    cur_arg.name = TRY_CALL_EXIT(string, "Wrong TL-scheme specified.", tl_parse_string());
    cur_arg.flags = TRY_CALL_EXIT(int, "Wrong TL-scheme specified.", tl_parse_int());

    if (cur_arg.flags & schema_flag_opt_field) {
      cur_arg.flags &= ~schema_flag_opt_field;
      cur_arg.flags |= FLAG_OPT_FIELD;
    }
    if (cur_arg.flags & schema_flag_has_vars) {
      cur_arg.flags &= ~schema_flag_has_vars;
      cur_arg.var_num = TRY_CALL_EXIT(int, "Wrong TL-scheme specified.", tl_parse_int());
    } else {
      cur_arg.var_num = -1;
    }

    if (cur_arg.var_num >= *var_count) {
      *var_count = cur_arg.var_num + 1;
    }
    if (cur_arg.flags & FLAG_OPT_FIELD) {
      cur_arg.exist_var_num = TRY_CALL_EXIT(int, "Wrong TL-scheme specified.", tl_parse_int());
      cur_arg.exist_var_bit = TRY_CALL_EXIT(int, "Wrong TL-scheme specified.", tl_parse_int());
    } else {
      cur_arg.exist_var_num = -1;
      cur_arg.exist_var_bit = 0;
    }
    cur_arg.type = read_type_expr (var_count);
    if (cur_arg.var_num < 0 && cur_arg.exist_var_num < 0 && (cur_arg.type->flags & FLAG_NOVAR)) {
      cur_arg.flags |= FLAG_NOVAR;
    }

    args.push_back (cur_arg);
  }
  return args;
}


tl_combinator *read_combinator (void) {
  php_assert (TRY_CALL_EXIT(int, "Wrong TL-scheme specified.", tl_parse_int()) == TLS_COMBINATOR);

  tl_combinator *combinator = (tl_combinator *)dl::allocate (sizeof (tl_combinator));
  combinator->id = TRY_CALL_EXIT(int, "Wrong TL-scheme specified.", tl_parse_int());
  new (&combinator->name) string (TRY_CALL_EXIT(string, "Wrong TL-scheme specified.", tl_parse_string()));
  combinator->type_id = TRY_CALL_EXIT(int, "Wrong TL-scheme specified.", tl_parse_int());
  combinator->var_count = 0;

  int left_type = TRY_CALL_EXIT(int, "Wrong TL-scheme specified.", tl_parse_int());
  if (left_type == TLS_COMBINATOR_LEFT) {
    new (&combinator->args) array <arg> (read_args_list (&combinator->var_count));
  } else {
    new (&combinator->args) array <arg>();
    php_assert (left_type == TLS_COMBINATOR_LEFT_BUILTIN);
  }

  php_assert (TRY_CALL_EXIT(int, "Wrong TL-scheme specified.", tl_parse_int()) == TLS_COMBINATOR_RIGHT_V2);
  combinator->result = read_type_expr (&combinator->var_count);

  combinator->IP = NULL;
  combinator->fetchIP = NULL;
  combinator->IP_len = 0;
  combinator->fetchIP_len = 0;

  return combinator;
}

tl_type *read_type (void) {
  php_assert (TRY_CALL_EXIT(int, "Wrong TL-scheme specified.", tl_parse_int()) == TLS_TYPE);

  tl_type *type = (tl_type *)dl::allocate (sizeof (tl_type));
  type->id = TRY_CALL_EXIT(int, "Wrong TL-scheme specified.", tl_parse_int());
  new (&type->name) string (TRY_CALL_EXIT(string, "Wrong TL-scheme specified.", tl_parse_string()));
  type->constructors_num = TRY_CALL_EXIT(int, "Wrong TL-scheme specified.", tl_parse_int());
  new (&type->constructors) array <tl_combinator *> (array_size (type->constructors_num, 0, true));
  type->flags = TRY_CALL_EXIT(int, "Wrong TL-scheme specified.", tl_parse_int());
  if (!strcmp (type->name.c_str(), "Maybe") || !strcmp (type->name.c_str(), "Bool")) {
    type->flags |= FLAG_NOCONS;
  }
  type->arity = TRY_CALL_EXIT(int, "Wrong TL-scheme specified.", tl_parse_int());
  typedef long long ll;
  TRY_CALL_EXIT(ll, "Wrong TL-scheme specified.", tl_parse_long());//unused
  return type;
}


int get_schema_version (int a) {
  if (a == TLS_SCHEMA_V3) {
    return 3;
  }
  if (a == TLS_SCHEMA_V2) {
    return 2;
  }
  return -1;
}

void renew_tl_config (void) {
  php_assert (!dl::query_num);
  php_assert (tl_config.fetchIP == NULL);

  tl_schema_version = get_schema_version (TRY_CALL_EXIT(int, "Wrong TL-scheme specified.", tl_parse_int()));
  php_assert (tl_schema_version != -1);

  TRY_CALL_EXIT(int, "Wrong TL-scheme specified.", tl_parse_int());//date
  TRY_CALL_EXIT(int, "Wrong TL-scheme specified.", tl_parse_int());//version

  int types_n = TRY_CALL_EXIT(int, "Wrong TL-scheme specified.", tl_parse_int());
  tl_config.types = array <tl_type *> (array_size (types_n, 0, true));
  tl_config.id_to_type = array <tl_type *> (array_size (types_n, 0, false));
  tl_config.name_to_type = array <tl_type *> (array_size (0, types_n, false));

  for (int i = 0; i < types_n; i++) {
    tl_type *type = read_type();
    tl_config.types.push_back (type);
    tl_config.id_to_type.set_value (type->id, type);
    tl_config.name_to_type.set_value (type->name, type);
  }

  tl_config.ReqResult = tl_config.name_to_type.get_value (string ("ReqResult", 9));
  php_assert (tl_config.ReqResult != NULL);
  php_assert (tl_config.ReqResult->arity == 1);

  int constructors_n = TRY_CALL_EXIT(int, "Wrong TL-scheme specified.", tl_parse_int());

  for (int i = 0; i < constructors_n; i++) {
    tl_combinator *constructor = read_combinator();
    tl_config.id_to_type.get_value (constructor->type_id)->constructors.push_back (constructor);
  }

  int functions_n = TRY_CALL_EXIT(int, "Wrong TL-scheme specified.", tl_parse_int());
  tl_config.functions = array <tl_combinator *> (array_size (functions_n, 0, true));
  tl_config.id_to_function = array <tl_combinator *> (array_size (functions_n, 0, false));
  tl_config.name_to_function = array <tl_combinator *> (array_size (0, functions_n, false));

  for (int i = 0; i < functions_n; i++) {
    tl_combinator *function = read_combinator();
    tl_config.functions.push_back (function);
    tl_config.id_to_function.set_value (function->id, function);
    tl_config.name_to_function.set_value (function->name, function);
  }
  TRY_CALL_VOID_EXIT("Wrong TL-scheme specified.", tl_parse_end());

  for (int i = 0; i < types_n; i++) {
    php_assert (tl_config.types[i]->constructors.count() == tl_config.types[i]->constructors_num);
  }

  static void *IP[10000];

  gen_function_fetch (tl_config.fetchIP, IP, 10000);

  for (int i = 0; i < tl_config.types.count(); i++) {
    tl_type *cur_type = tl_config.types.get_value (i);
    for (int j = 0; j < cur_type->constructors_num; j++) {
      php_assert (gen_constructor_store (*cur_type->constructors.get_value (j), IP, 10000) > 0);
      php_assert (gen_constructor_fetch (*cur_type->constructors.get_value (j), IP, 10000) > 0);
    }
  }
  for (int i = 0; i < functions_n; i++) {
    php_assert (gen_function_store (*tl_config.functions.get_value (i), IP, 10000) > 0);
  }
}

extern "C" {
void read_tl_config (const char *file_name);
}

void read_tl_config (const char *file_name) {
  OrFalse <string> config = f$file_get_contents (string (file_name, (dl::size_type)strlen (file_name)));
  php_assert (f$boolval (config));
  f$rpc_parse (config.val());
  renew_tl_config();
}


void rpc_init_static (void) {
  INIT_VAR(string, rpc_data_copy);

  INIT_VAR(OrFalse <string>, *rpc_result_string_storage);
  rpc_result_string = reinterpret_cast <OrFalse <string> *> (rpc_result_string_storage);
  INIT_VAR(bool, rpc_result_bool);

  f$rpc_clean (false);
  rpc_stored = 0;
  rpc_pack_threshold = -1;
  rpc_pack_from = -1;

  last_var_ptr = vars_buffer + MAX_VARS;
}

void rpc_free_static (void) {
  CLEAR_VAR(string, rpc_data_copy);

  clear_arr_space();
}

