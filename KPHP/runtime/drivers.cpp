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

#include <cstdlib>
#include <ctime>

#include "drivers.h"
#include "array_functions.h"
#include "interface.h"
#include "misc.h"
#include "zlib.h"


static string_buffer drivers_SB (1024);

static const int MAX_TIMEOUT = 120;
static const int MAX_TIMEOUT_MS = MAX_TIMEOUT * 1000;
static const int DB_TIMEOUT_MS = MAX_TIMEOUT_MS;
static const int MAX_KEY_LEN = 1000;
static const int MAX_VALUE_LEN = (1 << 20);
static const int MAX_INPUT_VALUE_LEN = (1 << 24);

static const string UNDERSCORE ("_", 1);

int timeout_convert_to_ms (double timeout) {
  int timeout_ms = (int)(timeout * 1000 + 1);
  if (timeout_ms <= 0) {
    timeout_ms = 1;
  }
  if (timeout_ms >= MAX_TIMEOUT_MS) {
    timeout_ms = MAX_TIMEOUT_MS;
  }

  return timeout_ms;
}

var long_long_convert_to_var (long long result) {
  if (result <= 0) {
    return false;
  }

  if (result <= (long long)INT_MAX) {
    return (int)result;
  }

  return f$strval (ULong ((unsigned long long)result));
}


const char *get_last_net_error_dummy (void) {
  php_critical_error ("get_last_net_error_dummy called");
}

const char *(*get_last_net_error) (void) = get_last_net_error_dummy;


const char *mc_method;
static const char *mc_last_key;
static int mc_last_key_len;
static char mc_res_storage[sizeof (var)];
static var *mc_res;
static bool mc_bool_res;


const string mc_prepare_key (const string &key) {
  if (key.size() < 3) {
    php_warning ("Very short key \"%s\" in Memcache::%s", key.c_str(), mc_method);
  }

  bool bad_key = ((int)key.size() > MAX_KEY_LEN || key.empty());
  for (int i = 0; i < (int)key.size() && !bad_key; i++) {
    if ((unsigned int)key[i] <= 32u) {
      bad_key = true;
    }
  }
  if (!bad_key) {
    return key;
  }

  string real_key = key.substr (0, min ((dl::size_type)MAX_KEY_LEN, key.size()));//need a copy
  for (int i = 0; i < (int)real_key.size(); i++) {
    if ((unsigned int)real_key[i] <= 32u) {
      real_key[i] = '_';
    }
  }
  if (real_key.empty()) {
    php_warning ("Empty parameter key in Memcache::%s, key \"_\" used instead", mc_method);
    real_key = UNDERSCORE;
  } else {
    php_warning ("Wrong parameter key = \"%s\" in Memcache::%s, key \"%s\" used instead", key.c_str(), mc_method, real_key.c_str());
  }
  return real_key;
}


bool mc_is_immediate_query (const string &key) {
  return false;
//  return key[0] == '^' && (int)key.size() >= 2;
}


const char *mc_parse_value (const char *result, int result_len, const char **key, int *key_len, const char **value, int *value_len, int *flags, int *error_code) {
  if (strncmp (result, "VALUE", 5)) {
    *error_code = 1;
    return NULL;
  }
  int i = 5;

  while (result[i] == ' ') {
    i++;
  }
  if (i == result_len) {
    *error_code = 2;
    return NULL;
  }
  *key = result + i;
  while (i < result_len && result[i] != ' ') {
    i++;
  }
  if (i == result_len) {
    *error_code = 3;
    return NULL;
  }
  *key_len = (int)(result + i - *key);

  while (result[i] == ' ') {
    i++;
  }
  if (i == result_len) {
    *error_code = 4;
    return NULL;
  }
  *value = result + i;
  while (i < result_len && result[i] != ' ') {
    i++;
  }
  if (i == result_len) {
    *error_code = 5;
    return NULL;
  }
  if (!php_try_to_int (*value, (int)(result + i - *value), flags)) {
    *error_code = 6;
    return NULL;
  }

  while (result[i] == ' ') {
    i++;
  }
  if (i == result_len) {
    *error_code = 7;
    return NULL;
  }
  *value = result + i;
  while (i < result_len && result[i] != ' ' && result[i] != '\r') {
    i++;
  }
  if (result[i] != '\r' || result[i + 1] != '\n') {
    *error_code = 8;
    return NULL;
  }
  if (!php_try_to_int (*value, (int)(result + i - *value), value_len) || (unsigned int)(*value_len) >= (unsigned int)MAX_INPUT_VALUE_LEN) {
    *error_code = 9;
    return NULL;
  }

  i += 2;
  *value = result + i;
  if (i + *value_len + 2 > result_len) {
    *error_code = 10;
    return NULL;
  }
  i += *value_len;

  if (result[i] != '\r' || result[i + 1] != '\n') {
    *error_code = 11;
    return NULL;
  }
  *error_code = 0;
  return result + i + 2;
}

var mc_get_value (string result_str, int flags) {
  var result;
  if (flags & MEMCACHE_COMPRESSED) {
    flags ^= MEMCACHE_COMPRESSED;
    result_str = f$gzuncompress (result_str);
  }

  if (flags & MEMCACHE_SERIALIZED) {
    flags ^= MEMCACHE_SERIALIZED;
    result = f$unserialize (result_str);
  } else {
    result = result_str;
  }

  if (flags) {
//    php_warning ("Wrong parameter flags %d with value \"%s\" returned in Memcache::get", flags, result_str.c_str());
  }

  return result;
}

void mc_set_callback (const char *result, int result_len) {
  if (!strcmp (result, "ERROR\r\n")) {
    return;
  }

  if (!strcmp (result, "STORED\r\n")) {
    mc_bool_res = true;
    return;
  }
  if (!strcmp (result, "NOT_STORED\r\n")) {
    mc_bool_res = false;
    return;
  }

  php_warning ("Strange result \"%s\" returned from memcached in Memcache::%s with key %s", result, mc_method, mc_last_key);
  mc_bool_res = false;
}

void mc_multiget_callback (const char *result, int result_len) {
  if (!strcmp (result, "ERROR\r\n")) {
    return;
  }
  const char *full_result = result;

  while (*result) {
    switch (*result) {
      case 'V': {
        int key_len, value_len;
        const char *key, *value;
        int flags;
        int error_code;

        const char *new_result = mc_parse_value (result, result_len, &key, &key_len, &value, &value_len, &flags, &error_code);
        if (!new_result) {
          php_warning ("Wrong memcache response \"%s\" in Memcache::get with multikey %s and error code %d", result, mc_last_key, error_code);
          return;
        }
        php_assert (new_result > result);
        result_len -= (int)(new_result - result);
        php_assert (result_len >= 0);
        result = new_result;
        mc_res->set_value (string (key, key_len), mc_get_value (string (value, value_len), flags));
        break;
      }
      case 'E':
        if (result_len == 5 && !strncmp (result, "END\r\n", 5)) {
          return;
        }
      default:
        php_warning ("Wrong memcache response \"%s\" in Memcache::get with multikey %s", full_result, mc_last_key);
    }
  }
}

void mc_get_callback (const char *result, int result_len) {
  if (!strcmp (result, "ERROR\r\n")) {
    return;
  }
  const char *full_result = result;

  switch (*result) {
    case 'V': {
      int key_len, value_len;
      const char *key, *value;
      int flags;
      int error_code;

      const char *new_result = mc_parse_value (result, result_len, &key, &key_len, &value, &value_len, &flags, &error_code);
      if (!new_result) {
        php_warning ("Wrong memcache response \"%s\" in Memcache::get with key %s and error code %d", result, mc_last_key, error_code);
        return;
      }
      if (mc_last_key_len != key_len || memcmp (mc_last_key, key, (size_t)key_len)) {
        php_warning ("Wrong memcache response \"%s\" in Memcache::get with key %s", result, mc_last_key);
        return;
      }
      php_assert (new_result > result);
      result_len -= (int)(new_result - result);
      php_assert (result_len >= 0);
      result = new_result;
      *mc_res = mc_get_value (string (value, value_len), flags);
    }
    case 'E':
      if (result_len == 5 && !strncmp (result, "END\r\n", 5)) {
        return;
      }
    default:
      php_warning ("Wrong memcache response \"%s\" in Memcache::get with key %s", full_result, mc_last_key);
  }
}

void mc_delete_callback (const char *result, int result_len) {
  if (!strcmp (result, "ERROR\r\n")) {
    return;
  }

  if (!strcmp (result, "NOT_FOUND\r\n")) {
    mc_bool_res = false;
  } else if (!strcmp (result, "DELETED\r\n")) {
    mc_bool_res = true;
  } else {
    php_warning ("Strange result \"%s\" returned from memcached in Memcache::delete with key %s", result, mc_last_key);
    mc_bool_res = false;
  }
}

void mc_increment_callback (const char *result, int result_len) {
  if (!strcmp (result, "ERROR\r\n")) {
    return;
  }

  if (!strcmp (result, "NOT_FOUND\r\n")) {
    *mc_res = false;
  } else {
    if (result_len >= 2 && result[result_len - 2] == '\r' && result[result_len - 1] == '\n') {
      mc_res->assign (result, result_len - 2);
    } else {
      php_warning ("Wrong memcache response \"%s\" in Memcache::%sement with key %s", result, mc_method, mc_last_key);
    }
  }
}

void mc_version_callback (const char *result, int result_len) {
  if (!strcmp (result, "ERROR\r\n")) {
    return;
  }

  switch (*result) {
    case 'V': {
      if (!strncmp (result, "VERSION ", 8)) {
        if (result_len >= 10 && result[result_len - 2] == '\r' && result[result_len - 1] == '\n') {
          mc_res->assign (result + 8, result_len - 10);
          break;
        }
      }
    }
    default:
      php_warning ("Wrong memcache response \"%s\" in Memcache::getVersion", result);
  }
}


Memcache::host::host (void): host_num (-1),
                             host_weight (0),
                             timeout_ms (200) {
}

Memcache::host::host (int host_num, int host_weight, int timeout_ms): host_num (host_num),
                                                                      host_weight (host_weight),
                                                                      timeout_ms (timeout_ms) {
}


Memcache::host Memcache::get_host (const string &key) {
  php_assert (hosts.count() > 0);

  return hosts.get_value (f$array_rand (hosts));
}


bool Memcache::run_set (const string &key, const var &value, int flags, int expire) {
  if (hosts.count() <= 0) {
    php_warning ("There is no available server to run Memcache::%s with key \"%s\"", mc_method, key.c_str());
    return false;
  }

  const string real_key = mc_prepare_key (key);

  if (flags & ~MEMCACHE_COMPRESSED) {
    php_warning ("Wrong parameter flags = %d in Memcache::%s", flags, mc_method);
    flags &= MEMCACHE_COMPRESSED;
  }

  if ((unsigned int)expire > (unsigned int)(30 * 24 * 60 * 60)) {
    php_warning ("Wrong parameter expire = %d in Memcache::%s", expire, mc_method);
    expire = 0;
  }

  string string_value;
  if (f$is_array (value)) {
    string_value = f$serialize (value);
    flags |= MEMCACHE_SERIALIZED;
  } else {
    string_value = value.to_string();
  }

  if (flags & MEMCACHE_COMPRESSED) {
    string_value = f$gzcompress (string_value);
  }

  if (string_value.size() >= (dl::size_type)MAX_VALUE_LEN) {
    php_warning ("Parameter value has length %d and too large for storing in Memcache", (int)string_value.size());
    return false;
  }

  drivers_SB.clean();
  drivers_SB += mc_method;
  drivers_SB += ' ';
  drivers_SB += real_key;
  drivers_SB += ' ';
  drivers_SB += flags;
  drivers_SB += ' ';
  drivers_SB += expire;
  drivers_SB += ' ';
  drivers_SB += (int)string_value.size();
  drivers_SB += "\r\n";
  drivers_SB += string_value;
  drivers_SB += "\r\n";

  mc_bool_res = false;
  host cur_host = get_host (real_key);
  if (mc_is_immediate_query (real_key)) {
    mc_run_query (cur_host.host_num, drivers_SB.c_str(), drivers_SB.size(), cur_host.timeout_ms, 0, NULL);
    return true;
  } else {
    mc_last_key = real_key.c_str();
    mc_last_key_len = (int)real_key.size();
    mc_run_query (cur_host.host_num, drivers_SB.c_str(), drivers_SB.size(), cur_host.timeout_ms, 0, mc_set_callback);
    return mc_bool_res;
  }
}

var Memcache::run_increment (const string &key, const var &count) {
  if (hosts.count() <= 0) {
    php_warning ("There is no available server to run Memcache::%s with key \"%s\"", mc_method, key.c_str());
    return false;
  }

  const string real_key = mc_prepare_key (key);

  drivers_SB.clean();
  drivers_SB += mc_method;
  drivers_SB += ' ';
  drivers_SB += real_key;
  drivers_SB += ' ';
  if (count.is_int()) {
    drivers_SB += count;
  } else {
    string count_str = count.to_string();

    int i, negative = (count_str[0] == '-'), len = count_str.size();
    for (i = negative; '0' <= count_str[i] && count_str[i] <= '9'; i++) {
    }

    if (i < len || len == negative || len > 19 + negative) {
      php_warning ("Wrong parameter count = \"%s\" in Memcache::%sement, key %semented by 1 instead", count_str.c_str(), mc_method, mc_method);
      drivers_SB += '1';
    } else {
      drivers_SB += count_str;
    }
  }
  drivers_SB += "\r\n";

  *mc_res = false;
  host cur_host = get_host (real_key);
  if (mc_is_immediate_query (real_key)) {
    mc_run_query (cur_host.host_num, drivers_SB.c_str(), drivers_SB.size(), cur_host.timeout_ms, 0, NULL);
    return 0;
  } else {
    mc_last_key = real_key.c_str();
    mc_last_key_len = (int)real_key.size();
    mc_run_query (cur_host.host_num, drivers_SB.c_str(), drivers_SB.size(), cur_host.timeout_ms, 0, mc_increment_callback);
    return *mc_res;
  }
}

Memcache::Memcache (void): hosts (array_size (1, 0, true)) {
}

bool Memcache::addServer (const string &host_name, int port, bool persistent, int weight, int timeout, int retry_interval, bool status, const var &failure_callback, int timeoutms) {
  if (timeout <= 0) {
    timeout = 1;
  }
  if (timeout >= MAX_TIMEOUT) {
    timeout = MAX_TIMEOUT;
  }

  php_assert (MAX_TIMEOUT < 1000000);
  if (1 <= timeoutms && timeoutms <= 1000 * MAX_TIMEOUT) {
    timeout = timeoutms;
  } else {
    timeout *= 1000;
  }

  int host_num = mc_connect_to (host_name.c_str(), port);
  if (host_num >= 0) {
    hosts.push_back (host (host_num, weight, timeout));
    return true;
  }
  return false;
}

bool Memcache::connect (const string &host_name, int port, int timeout) {
  return addServer (host_name, port, false, 1, timeout);
}

bool Memcache::pconnect (const string &host_name, int port, int timeout) {
  return addServer (host_name, port, true, 1, timeout);
}

bool Memcache::rpc_connect (const string &host_name, int port, const var &default_actor_id, double timeout, double connect_timeout, double reconnect_timeout) {
  php_warning ("Method rpc_connect doesn't supported for object of class Memcache");
  return false;
}


bool Memcache::add (const string &key, const var &value, int flags, int expire) {
  mc_method = "add";
  return run_set (key, value, flags, expire);
}

bool Memcache::set (const string &key, const var &value, int flags, int expire) {
  mc_method = "set";
  return run_set (key, value, flags, expire);
}

bool Memcache::replace (const string &key, const var &value, int flags, int expire) {
  mc_method = "replace";
  return run_set (key, value, flags, expire);
}

var Memcache::get (const var &key_var) {
  mc_method = "get";
  if (f$is_array (key_var)) {
    if (hosts.count() <= 0) {
      php_warning ("There is no available server to run Memcache::get");
      return array <var> ();
    }

    drivers_SB.clean();
    drivers_SB += "get";
    bool is_immediate_query = true;
    for (array <var>::const_iterator p = key_var.begin(); p != key_var.end(); ++p) {
      const string key = p.get_value().to_string();
      const string real_key = mc_prepare_key (key);
      drivers_SB += ' ';
      drivers_SB += real_key;
      is_immediate_query = is_immediate_query && mc_is_immediate_query (real_key);
    }
    drivers_SB += "\r\n";

    *mc_res = array <var> (array_size (0, key_var.count(), false));
    host cur_host = get_host (string());
    if (is_immediate_query) {
      mc_run_query (cur_host.host_num, drivers_SB.c_str(), drivers_SB.size(), cur_host.timeout_ms, 0, NULL); //TODO wrong if we have no mc_proxy
    } else {
      mc_last_key = drivers_SB.c_str();
      mc_last_key_len = (int)drivers_SB.size();
      mc_run_query (cur_host.host_num, drivers_SB.c_str(), drivers_SB.size(), cur_host.timeout_ms, 0, mc_multiget_callback); //TODO wrong if we have no mc_proxy
    }
  } else {
    if (hosts.count() <= 0) {
      php_warning ("There is no available server to run Memcache::get with key \"%s\"", key_var.to_string().c_str());
      return false;
    }

    const string key = key_var.to_string();
    const string real_key = mc_prepare_key (key);

    drivers_SB.clean();
    drivers_SB += "get ";
    drivers_SB += real_key;
    drivers_SB += "\r\n";

    host cur_host = get_host (real_key);
    if (mc_is_immediate_query (real_key)) {
      *mc_res = true;
      mc_run_query (cur_host.host_num, drivers_SB.c_str(), drivers_SB.size(), cur_host.timeout_ms, 0, NULL);
    } else {
      *mc_res = false;
      mc_last_key = real_key.c_str();
      mc_last_key_len = (int)real_key.size();
      mc_run_query (cur_host.host_num, drivers_SB.c_str(), drivers_SB.size(), cur_host.timeout_ms, 0, mc_get_callback);
    }
  }
  return *mc_res;
}

bool Memcache::delete_ (const string &key) {
  mc_method = "delete";
  if (hosts.count() <= 0) {
    php_warning ("There is no available server to run Memcache::delete with key \"%s\"", key.c_str());
    return false;
  }

  const string real_key = mc_prepare_key (key);

  drivers_SB.clean();
  drivers_SB += "delete ";
  drivers_SB += real_key;
  drivers_SB += "\r\n";

  mc_bool_res = false;
  host cur_host = get_host (real_key);
  if (mc_is_immediate_query (real_key)) {
    mc_run_query (cur_host.host_num, drivers_SB.c_str(), drivers_SB.size(), cur_host.timeout_ms, 0, NULL);
    return true;
  } else {
    mc_last_key = real_key.c_str();
    mc_last_key_len = (int)real_key.size();
    mc_run_query (cur_host.host_num, drivers_SB.c_str(), drivers_SB.size(), cur_host.timeout_ms, 0, mc_delete_callback);
    return mc_bool_res;
  }
}

var Memcache::decrement (const string &key, const var &count) {
  mc_method = "decr";
  return run_increment (key, count);
}

var Memcache::increment (const string &key, const var &count) {
  mc_method = "incr";
  return run_increment (key, count);
}

var Memcache::getVersion (void) {
  static const char *version_str = "version\r\n";
  if (hosts.count() <= 0) {
    php_warning ("There is no available server to run Memcache::getVersion");
    return false;
  }

  *mc_res = false;
  host cur_host = get_host (string());
  mc_run_query (cur_host.host_num, version_str, (int)strlen (version_str), cur_host.timeout_ms, 1, mc_version_callback);

  return *mc_res;
}


RpcMemcache::host::host (void): conn(),
                                host_weight (0) {
}

RpcMemcache::host::host (const string &host_name, int port, int actor_id, int host_weight, int timeout_ms): conn (f$new_rpc_connection (host_name, port, actor_id, timeout_ms * 0.001)),
                                                                                                            host_weight (host_weight) {
}

RpcMemcache::host::host (const rpc_connection &c): conn (c),
                                                   host_weight (1) {
}

RpcMemcache::host RpcMemcache::get_host (const string &key) {
  php_assert (hosts.count() > 0);

  return hosts.get_value (f$array_rand (hosts));
}

RpcMemcache::RpcMemcache (void): hosts (array_size (1, 0, true)) {
}

bool RpcMemcache::addServer (const string &host_name, int port, bool persistent, int weight, int timeout, int retry_interval, bool status, const var &failure_callback, int timeoutms) {
  if (timeout <= 0) {
    timeout = 1;
  }
  if (timeout >= MAX_TIMEOUT) {
    timeout = MAX_TIMEOUT;
  }

  php_assert (MAX_TIMEOUT < 1000000);
  if (1 <= timeoutms && timeoutms <= 1000 * MAX_TIMEOUT) {
    timeout = timeoutms;
  } else {
    timeout *= 1000;
  }

  host new_host = host (host_name, port, retry_interval >= 100 ? retry_interval : 0, weight, timeout);
  if (new_host.conn.host_num >= 0) {
    hosts.push_back (new_host);
    return true;
  }
  return false;
}

bool RpcMemcache::connect (const string &host_name, int port, int timeout) {
  return addServer (host_name, port, false, 1, timeout);
}

bool RpcMemcache::pconnect (const string &host_name, int port, int timeout) {
  return addServer (host_name, port, true, 1, timeout);
}

bool RpcMemcache::rpc_connect (const string &host_name, int port, const var &default_actor_id, double timeout, double connect_timeout, double reconnect_timeout) {
  rpc_connection c = f$new_rpc_connection (host_name, port, default_actor_id, timeout, connect_timeout, reconnect_timeout);
  if (c.host_num >= 0) {
    hosts.push_back (host (c));
    return true;
  }
  return false;
}


bool RpcMemcache::add (const string &key, const var &value, int flags, int expire) {
  if (hosts.count() <= 0) {
    php_warning ("There is no available server to run RpcMemcache::add with key \"%s\"", key.c_str());
    return false;
  }

  const string real_key = mc_prepare_key (key);
  host cur_host = get_host (real_key);
  return f$rpc_mc_add (cur_host.conn, real_key, value, flags, expire);
}

bool RpcMemcache::set (const string &key, const var &value, int flags, int expire) {
  if (hosts.count() <= 0) {
    php_warning ("There is no available server to run RpcMemcache::set with key \"%s\"", key.c_str());
    return false;
  }

  const string real_key = mc_prepare_key (key);
  host cur_host = get_host (real_key);
  return f$rpc_mc_set (cur_host.conn, real_key, value, flags, expire);
}

bool RpcMemcache::replace (const string &key, const var &value, int flags, int expire) {
  if (hosts.count() <= 0) {
    php_warning ("There is no available server to run RpcMemcache::replace with key \"%s\"", key.c_str());
    return false;
  }

  const string real_key = mc_prepare_key (key);
  host cur_host = get_host (real_key);
  return f$rpc_mc_replace (cur_host.conn, real_key, value, flags, expire);
}

var RpcMemcache::get (const var &key_var) {
  if (f$is_array (key_var)) {
    if (hosts.count() <= 0) {
      php_warning ("There is no available server to run RpcMemcache::get");
      return array <var> ();
    }

    host cur_host = get_host (string());
    return f$rpc_mc_multiget (cur_host.conn, key_var.to_array());
  } else {
    if (hosts.count() <= 0) {
      php_warning ("There is no available server to run RpcMemcache::get with key \"%s\"", key_var.to_string().c_str());
      return false;
    }

    const string key = key_var.to_string();
    const string real_key = mc_prepare_key (key);

    host cur_host = get_host (real_key);
    return f$rpc_mc_get (cur_host.conn, real_key);
  }
}

bool RpcMemcache::delete_ (const string &key) {
  if (hosts.count() <= 0) {
    php_warning ("There is no available server to run RpcMemcache::delete with key \"%s\"", key.c_str());
    return false;
  }

  const string real_key = mc_prepare_key (key);
  host cur_host = get_host (real_key);
  return f$rpc_mc_delete (cur_host.conn, real_key);
}

var RpcMemcache::decrement (const string &key, const var &count) {
  if (hosts.count() <= 0) {
    php_warning ("There is no available server to run RpcMemcache::decrement with key \"%s\"", key.c_str());
    return false;
  }

  const string real_key = mc_prepare_key (key);
  host cur_host = get_host (real_key);
  return f$rpc_mc_decrement (cur_host.conn, real_key, count);
}

var RpcMemcache::increment (const string &key, const var &count) {
  if (hosts.count() <= 0) {
    php_warning ("There is no available server to run RpcMemcache::increment with key \"%s\"", key.c_str());
    return false;
  }

  const string real_key = mc_prepare_key (key);
  host cur_host = get_host (real_key);
  return f$rpc_mc_increment (cur_host.conn, real_key, count);
}

var RpcMemcache::getVersion (void) {
  if (hosts.count() <= 0) {
    php_warning ("There is no available server to run RpcMemcache::getVersion");
    return false;
  }

  php_warning ("Method getVersion doesn't supported for object of class RpcMemcache");
  return false;
}


bool f$memcached_addServer (const MyMemcache &mc, const string &host_name, int port, bool persistent, int weight, int timeout, int retry_interval, bool status, const var &failure_callback, int timeoutms) {
  if (mc.mc == NULL) {
    php_warning ("Memcache object is NULL in Memcache->addServer");
    return false;
  }
  return mc.mc->addServer (host_name, port, persistent, weight, timeout, retry_interval, status, failure_callback, timeoutms);
}

bool f$memcached_connect (const MyMemcache &mc, const string &host_name, int port, int timeout) {
  if (mc.mc == NULL) {
    php_warning ("Memcache object is NULL in Memcache->connect");
    return false;
  }
  return mc.mc->connect (host_name, port, timeout);
}

bool f$memcached_pconnect (const MyMemcache &mc, const string &host_name, int port, int timeout) {
  if (mc.mc == NULL) {
    php_warning ("Memcache object is NULL in Memcache->pconnect");
    return false;
  }
  return mc.mc->pconnect (host_name, port, timeout);
}

bool f$memcached_rpc_connect (const MyMemcache &mc, const string &host_name, int port, const var &default_actor_id, double timeout, double connect_timeout, double reconnect_timeout) {
  if (mc.mc == NULL) {
    php_warning ("Memcache object is NULL in Memcache->rpc_connect");
    return false;
  }
  return mc.mc->rpc_connect (host_name, port, default_actor_id, timeout, connect_timeout, reconnect_timeout);
}



bool f$memcached_add (const MyMemcache &mc, const string &key, const var &value, int flags, int expire) {
  if (mc.mc == NULL) {
    php_warning ("Memcache object is NULL in Memcache->add");
    return false;
  }
  return mc.mc->add (key, value, flags, expire);
}

bool f$memcached_set (const MyMemcache &mc, const string &key, const var &value, int flags, int expire) {
  if (mc.mc == NULL) {
    php_warning ("Memcache object is NULL in Memcache->set");
    return false;
  }
  return mc.mc->set (key, value, flags, expire);
}

bool f$memcached_replace (const MyMemcache &mc, const string &key, const var &value, int flags, int expire) {
  if (mc.mc == NULL) {
    php_warning ("Memcache object is NULL in Memcache->replace");
    return false;
  }
  return mc.mc->replace (key, value, flags, expire);
}

var f$memcached_get (const MyMemcache &mc, const var &key_var) {
  if (mc.mc == NULL) {
    php_warning ("Memcache object is NULL in Memcache->get");
    return key_var.is_array() ? var (array <var> ()) : var (false);
  }
  return mc.mc->get (key_var);
}

bool f$memcached_delete (const MyMemcache &mc, const string &key) {
  if (mc.mc == NULL) {
    php_warning ("Memcache object is NULL in Memcache->delete");
    return false;
  }
  return mc.mc->delete_ (key);
}

var f$memcached_decrement (const MyMemcache &mc, const string &key, const var &v) {
  if (mc.mc == NULL) {
    php_warning ("Memcache object is NULL in Memcache->decrement");
    return false;
  }
  return mc.mc->decrement (key, v);
}

var f$memcached_increment (const MyMemcache &mc, const string &key, const var &v) {
  if (mc.mc == NULL) {
    php_warning ("Memcache object is NULL in Memcache->increment");
    return false;
  }
  return mc.mc->increment (key, v);
}

var f$memcached_getVersion (const MyMemcache &mc) {
  if (mc.mc == NULL) {
    php_warning ("Memcache object is NULL in Memcache->getVersion");
    return false;
  }
  return mc.mc->getVersion();
}


bool f$boolval (const MyMemcache &my_mc) {
  return f$boolval (my_mc.bool_value);
}

bool eq2 (const MyMemcache &my_mc, bool value) {
  return my_mc.bool_value == value;
}

bool eq2 (bool value, const MyMemcache &my_mc) {
  return value == my_mc.bool_value;
}

bool equals (bool value, const MyMemcache &my_mc) {
  return equals (value, my_mc.bool_value);
}

bool equals (const MyMemcache &my_mc, bool value) {
  return equals (my_mc.bool_value, value);
}

bool not_equals (bool value, const MyMemcache &my_mc) {
  return not_equals (value, my_mc.bool_value);
}

bool not_equals (const MyMemcache &my_mc, bool value) {
  return not_equals (my_mc.bool_value, value);
}


MyMemcache& MyMemcache::operator = (bool value) {
  bool_value = value;
  mc = NULL;
  return *this;
}

MyMemcache::MyMemcache (bool value) {
  bool_value = value;
  mc = NULL;
}

MyMemcache::MyMemcache (MC_object *mc): bool_value (true),
                                        mc (mc) {
}

MyMemcache::MyMemcache (void): bool_value(),
                               mc (NULL) {
}



array <string> f$mcGetStats (const MyMemcache &MC) {
  var stats_result = f$memcached_get (MC, string ("#stats", 6));
  if (!stats_result) {
    return array <var> ();
  }
  array <string> stats = array <var> ();
  array <string> stats_array = explode ('\n', stats_result.to_string());
  for (int i = 0; i < (int)stats_array.count(); i++) {
    string row = stats_array[i];
    if (row.size()) {
      array <string> row_array = explode ('\t', row, 2);
      if (row_array.count() == 2) {
        stats[row_array[0]] = row_array[1];
      }
    }
  }
  return stats;
}

int f$mcGetClusterSize (const MyMemcache &MC) {
  static char cluster_size_cache_storage[sizeof (array <int>)];
  static array <int> *cluster_size_cache = reinterpret_cast <array <int> *> (cluster_size_cache_storage);

  static long long last_query_num = -1;
  if (dl::query_num != last_query_num) {
    new (cluster_size_cache_storage) array <int>();
    last_query_num = dl::query_num;
  }

  char p[25];
  string key (p, sprintf (p, "%p", MC.mc));

  if (cluster_size_cache->isset (key)) {
    return cluster_size_cache->get_value (key);
  }

  array <string> stats = f$mcGetStats (MC);
  int cluster_size = f$intval (stats.get_value (string ("cluster_size", 12)));
  cluster_size_cache->set_value (key, cluster_size);
  return cluster_size;
}



MyMemcache f$new_Memcache (void) {
  void *buf = dl::allocate (sizeof (Memcache));
  return MyMemcache (new (buf) Memcache());
}

MyMemcache f$new_RpcMemcache (void) {
  void *buf = dl::allocate (sizeof (Memcache));
  return MyMemcache (new (buf) RpcMemcache());
}


/*
 *
 *  RPC memcached interface
 *
 */


var f$rpc_mc_get (const rpc_connection &conn, const string &key, double timeout) {
  mc_method = "get";
  const string real_key = mc_prepare_key (key);
  int is_immediate = mc_is_immediate_query (real_key);

  f$rpc_clean();
  f$store_int (MEMCACHE_GET);
  store_string (real_key.c_str() + is_immediate, real_key.size() - is_immediate);

  long long request_id = rpc_send (conn, timeout);
  if (is_immediate) {
    return true;
  }
  if (!rpc_get_and_parse (request_id)) {
    return false;
  }

  int res = TRY_CALL(int, var, (f$fetch_int (string(), -1)));//TODO __FILE__ and __LINE__
  if (res != MEMCACHE_VALUE_STRING) {
    return false;
  }

  string value = TRY_CALL(string, var, (f$fetch_string (string(), -1)));
  int flags = TRY_CALL(int, var, (f$fetch_int (string(), -1)));
  return mc_get_value (value, flags);
}

bool rpc_mc_run_set (int op, const rpc_connection &conn, const string &key, const var &value, int flags, int expire, double timeout) {
  if (flags & ~MEMCACHE_COMPRESSED) {
    php_warning ("Wrong parameter flags = %d in Memcache::%s", flags, mc_method);
    flags &= MEMCACHE_COMPRESSED;
  }

  if ((unsigned int)expire > (unsigned int)(30 * 24 * 60 * 60)) {
    php_warning ("Wrong parameter expire = %d in Memcache::%s", expire, mc_method);
    expire = 0;
  }

  string string_value;
  if (f$is_array (value)) {
    string_value = f$serialize (value);
    flags |= MEMCACHE_SERIALIZED;
  } else {
    string_value = value.to_string();
  }

  if (flags & MEMCACHE_COMPRESSED) {
    string_value = f$gzcompress (string_value);
  }

  if (string_value.size() >= (dl::size_type)MAX_VALUE_LEN) {
    php_warning ("Parameter value has length %d and too large for storing in Memcache", (int)string_value.size());
    return false;
  }

  const string real_key = mc_prepare_key (key);
  int is_immediate = mc_is_immediate_query (real_key);

  f$rpc_clean();
  f$store_int (op);
  store_string (real_key.c_str() + is_immediate, real_key.size() - is_immediate);
  f$store_int (flags);
  f$store_int (expire);
  store_string (string_value.c_str(), string_value.size());

  long long request_id = rpc_send (conn, timeout);
  if (is_immediate) {
    return true;
  }
  if (!rpc_get_and_parse (request_id)) {
    return false;
  }

  int res = TRY_CALL(int, bool, (f$fetch_int (string(), -1)));//TODO __FILE__ and __LINE__
  return res == MEMCACHE_TRUE;
}

bool f$rpc_mc_set (const rpc_connection &conn, const string &key, const var &value, int flags, int expire, double timeout) {
  mc_method = "set";
  return rpc_mc_run_set (MEMCACHE_SET, conn, key, value, flags, expire, timeout);
}

bool f$rpc_mc_add (const rpc_connection &conn, const string &key, const var &value, int flags, int expire, double timeout) {
  mc_method = "add";
  return rpc_mc_run_set (MEMCACHE_ADD, conn, key, value, flags, expire, timeout);
}

bool f$rpc_mc_replace (const rpc_connection &conn, const string &key, const var &value, int flags, int expire, double timeout) {
  mc_method = "replace";
  return rpc_mc_run_set (MEMCACHE_REPLACE, conn, key, value, flags, expire, timeout);
}

var rpc_mc_run_increment (int op, const rpc_connection &conn, const string &key, const var &v, double timeout) {
  const string real_key = mc_prepare_key (key);
  int is_immediate = mc_is_immediate_query (real_key);

  f$rpc_clean();
  f$store_int (op);
  store_string (real_key.c_str() + is_immediate, real_key.size() - is_immediate);
  f$store_long (v);

  long long request_id = rpc_send (conn, timeout);
  if (is_immediate) {
    return 0;
  }
  if (!rpc_get_and_parse (request_id)) {
    return false;
  }

  int res = TRY_CALL(int, var, (f$fetch_int (string(), -1)));//TODO __FILE__ and __LINE__
  if (res == MEMCACHE_VALUE_LONG) {
    return TRY_CALL(var, var, (f$fetch_long (string(), -1)));
  }

  return false;
}

var f$rpc_mc_increment (const rpc_connection &conn, const string &key, const var &v, double timeout) {
  mc_method = "increment";
  return rpc_mc_run_increment (MEMCACHE_INCR, conn, key, v, timeout);
}

var f$rpc_mc_decrement (const rpc_connection &conn, const string &key, const var &v, double timeout) {
  mc_method = "decrement";
  return rpc_mc_run_increment (MEMCACHE_DECR, conn, key, v, timeout);
}

bool f$rpc_mc_delete (const rpc_connection &conn, const string &key, double timeout) {
  mc_method = "delete";
  const string real_key = mc_prepare_key (key);
  int is_immediate = mc_is_immediate_query (real_key);

  f$rpc_clean();
  f$store_int (MEMCACHE_DELETE);
  store_string (real_key.c_str() + is_immediate, real_key.size() - is_immediate);

  long long request_id = rpc_send (conn, timeout);
  if (is_immediate) {
    return true;
  }
  if (!rpc_get_and_parse (request_id)) {
    return false;
  }

  int res = TRY_CALL(int, bool, (f$fetch_int (string(), -1)));//TODO __FILE__ and __LINE__
  return res == MEMCACHE_TRUE;
}


int mysql_callback_state;

string *error_ptr;
int *errno_ptr;
int *affected_rows_ptr;
int *insert_id_ptr;
array <array <var> > *query_result_ptr;
bool *query_id_ptr;

int *field_cnt_ptr;
array <string> *field_names_ptr;

unsigned long long mysql_read_long_long (const unsigned char *&result, int &result_len, bool &is_null) {
  result_len--;
  if (result_len < 0) {
    return 0;
  }
  unsigned char c = *result;
  result++;
  if (c <= 250u) {
    return c;
  }
  unsigned long long value = 0;
  if (c == 251 || c == 255) {
    is_null = (c == 251);
    return value;
  }
  int lengths[3] = {2, 3, 8};
  int len = lengths[c - 252];
  result_len -= len;
  if (result_len < 0) {
    return 0;
  }

  int shift = 0;
  while (len--) {
    value += (unsigned long long)*result << shift;
    result++;
    shift += 8;
  }

  return value;
}

string mysql_read_string (const unsigned char *&result, int &result_len, bool &is_null, bool need_value = false) {
  if (result_len < 0) {
    return string();
  }

  long long value_len = mysql_read_long_long (result, result_len, is_null);
  if (is_null || result_len < value_len || !need_value) {
    result_len -= (int)value_len;
    result += value_len;
    return string();
  }
  string value ((const char *)result, (int)value_len);
  result_len -= (int)value_len;
  result += value_len;
  return value;
}

void mysql_query_callback (const char *result_, int result_len) {
//  fprintf (stderr, "%d %d\n", mysql_callback_state, result_len);
  if (*query_id_ptr == false || !strcmp (result_, "ERROR\r\n")) {
    *query_id_ptr = false;
    return;
  }

  const unsigned char *result = (const unsigned char *)result_;
  if (result_len < 4) {
    return;
  }
  int len = result[0] + (result[1] << 8) + (result[2] << 16);
  if (result_len < len + 4) {
    return;
  }
  if (len == 0) {
    *query_id_ptr = false;
    return;
  }

  bool is_null = false;
  result += 4;
  result_len -= 4;
  const unsigned char *result_end = result + len;
  switch (mysql_callback_state) {
    case 0:
      if (result[0] == 0) {
        mysql_callback_state = 5;

        ++result;
        result_len--;
        *affected_rows_ptr = (int)mysql_read_long_long (result, result_len, is_null);
        *insert_id_ptr = (int)mysql_read_long_long (result, result_len, is_null);
        if (result_len < 0 || is_null) {
          *query_id_ptr = false;
        }
        break;
      }
      if (result[0] == 255) {
        ++result;
        result_len--;
        *query_id_ptr = false;
        int message_len = len - 9;
        if (message_len < 0 || result[2] != '#') {
          return;
        }
        *errno_ptr = result[0] + (result[1] << 8);
        result += 8;
        result_len -= 8;
        error_ptr->assign ((const char *)result, message_len);
        return;
      }
      if (result[0] == 254) {
        ++result;
        result_len--;
        *query_id_ptr = false;
        return;
      }

      *field_cnt_ptr = (int)mysql_read_long_long (result, result_len, is_null);
      if (result < result_end) {
        mysql_read_long_long (result, result_len, is_null);
      }
      if (result_len < 0 || is_null || result != result_end) {
        *query_id_ptr = false;
        return;
      }
      *field_names_ptr = array <string> (array_size (*field_cnt_ptr, 0, true));

      mysql_callback_state = 1;
      break;
    case 1:
      if (result[0] == 254) {
        *query_id_ptr = false;
        return;
      }
      mysql_read_string (result, result_len, is_null);//catalog
      mysql_read_string (result, result_len, is_null);//db
      mysql_read_string (result, result_len, is_null);//table
      mysql_read_string (result, result_len, is_null);//org_table
      field_names_ptr->push_back (mysql_read_string (result, result_len, is_null, true));//name
      mysql_read_string (result, result_len, is_null);//org_name

      result_len -= 13;
      result += 13;

      if (result < result_end) {
        mysql_read_string (result, result_len, is_null);//default
      }

      if (result_len < 0 || result != result_end) {
        *query_id_ptr = false;
        return;
      }

      if (field_names_ptr->count() == *field_cnt_ptr) {
        mysql_callback_state = 2;
      }
      break;
    case 2:
      if (len != 5 || result[0] != 254) {
        *query_id_ptr = false;
        return;
      }
      result += 5;
      result_len -= 5;
      mysql_callback_state = 3;
      break;
    case 3:
      if (result[0] != 254) {
        array <var> row (array_size (*field_cnt_ptr, *field_cnt_ptr, false));
        for (int i = 0; i < *field_cnt_ptr; i++) {
          is_null = false;
          var value = mysql_read_string (result, result_len, is_null, true);
//          fprintf (stderr, "%p %p \"%s\" %d\n", result, result_end, value.to_string().c_str(), (int)is_null);
          if (is_null) {
            value = var();
          }
          if (result_len < 0 || result > result_end) {
            *query_id_ptr = false;
            return;
          }
//          row[i] = value;
          row[field_names_ptr->get_value (i)] = value;
        }
        if (result != result_end) {
          *query_id_ptr = false;
          return;
        }
        query_result_ptr->push_back (row);

        break;
      }
      mysql_callback_state = 4;
    case 4:
      if (len != 5 || result[0] != 254) {
        *query_id_ptr = false;
        return;
      }
      result += 5;
      result_len -= 5;

      mysql_callback_state = 5;
      break;
    case 5:
      *query_id_ptr = false;
      break;
  }
}


extern MyDB v$DB_Proxy;
static const char db_proxy_host[] = "127.0.0.1:3306";

bool DBNoDie = false;

db_driver::db_driver (void): old_failed (0),
                             failed (0),
                             return_die (false),

                             sql_host (db_proxy_host),

                             connection_id (-1),
                             connected (0),
                             next_timeout_ms (0),

                             last_query_id (0),
                             biggest_query_id (0),
                             error(),
                             errno_ (0),
                             affected_rows (0),
                             insert_id (0),
                             query_results(),
                             cur_pos(),
                             field_cnt (0),
                             field_names() {
  cur_pos.push_back (0);
  query_results.push_back (array <array <var> > ());
}

bool db_driver::is_down (bool check_failed) {
  if (check_failed) {
    TRY_CALL_VOID(bool, do_connect());
    return (connected == -1);
  }
  return false;
}

void db_driver::do_connect (void) {
  if (connection_id >= 0 || connected) {
    return;
  }

  connection_id = db_proxy_connect();
  if (connection_id < 0) {
    connected = -1;
    return;
  }
  connected = 1;
}

void db_driver::set_timeout (double new_timeout) {
  next_timeout_ms = timeout_convert_to_ms (new_timeout);
  if (next_timeout_ms == 30001) {
    next_timeout_ms = 30000;
  }
  if (next_timeout_ms < 100 || next_timeout_ms > 30000) {
    php_warning ("Wrong timeout %.6lf specified in dbSetTimeout", new_timeout);
  }
}

var db_driver::query (const string &query_str) {
  TRY_CALL_VOID(var, do_connect());
  if (connected < 0) {
    failed++;
    return false;
  }

  int cur_timeout_ms = 3000;
  drivers_SB.clean();
  if (next_timeout_ms > 0) {
    cur_timeout_ms = next_timeout_ms;

    char buf[100];
    snprintf (buf, 100, "/*? TIMEOUT %.1lf */ ", cur_timeout_ms * 0.001);
    drivers_SB += buf;
    next_timeout_ms = 0;
  }

  drivers_SB += query_str;
  string query_string = drivers_SB.str();

  bool query_id = mysql_query (query_string);
  int real_query_id = biggest_query_id;

  if (!query_id) {
    TRY_CALL_VOID(var, fatal_error (query_string));
  }

  return last_query_id = real_query_id;
}

OrFalse <array <var> > db_driver::fetch_row (const var &query_id_var) {
  if (connected < 0) {
    return false;
  }

  int query_id;
  if (!query_id_var.is_bool() && !query_id_var.is_int()) {
    php_warning ("Query_id has type %s, replacing with last_query_id", query_id_var.get_type_c_str());
    query_id = -1;
  } else {
    query_id = query_id_var.to_int();
  }

  if (!query_id) {
    return false;
  }

  if (query_id == -1) {
    query_id = last_query_id;
  }

  OrFalse <array <var> > res = mysql_fetch_array (query_id);
  return res;
}

int db_driver::get_affected_rows (void) {
  if (connected < 0) {
    return 0;
  }
  return affected_rows;
}

int db_driver::get_num_rows (void) {
  if (connected < 0) {
    return 0;
  }
  return query_results[last_query_id].count();
}

int db_driver::get_insert_id (void) {
  if (connected < 0) {
    return -1;
  }
  return insert_id;
}

void db_driver::fatal_error (const string &query) {
  if (DBNoDie || return_die) {
    failed++;
    return;
  }

  f$exit (0);
}

bool db_driver::mysql_query (const string &query) {
  if (query.size() > (1 << 24) - 10) {
    return false;
  }

  error = string();
  errno_ = 0;
  affected_rows = 0;
  insert_id = 0;
  array <array <var> > query_result;
  bool query_id = true;

  int packet_len = query.size() + 1;
  int len = query.size() + 5;

  string real_query (len, false);
  real_query[0] = (char)(packet_len & 255);
  real_query[1] = (char)((packet_len >> 8) & 255);
  real_query[2] = (char)((packet_len >> 16) & 255);
  real_query[3] = 0;
  real_query[4] = 3;
  memcpy (&real_query[5], query.c_str(), query.size());

  error_ptr = &error;
  errno_ptr = &errno_;
  affected_rows_ptr = &affected_rows;
  insert_id_ptr = &insert_id;
  query_result_ptr = &query_result;
  query_id_ptr = &query_id;

  field_cnt_ptr = &field_cnt;
  field_names_ptr = &field_names;

  mysql_callback_state = 0;
  db_run_query (connection_id, real_query.c_str(), len, DB_TIMEOUT_MS, mysql_query_callback);
  if (mysql_callback_state != 5 || !query_id) {
    return false;
  }

  php_assert (biggest_query_id < 2000000000);
  query_results[++biggest_query_id] = query_result;
  cur_pos[biggest_query_id] = 0;

  return true;
}

OrFalse <array <var> > db_driver::mysql_fetch_array (int query_id) {
  if (query_id <= 0 || query_id > biggest_query_id) {
    return false;
  }

  array <array <var> > &query_result = query_results[query_id];
  int &cur = cur_pos[query_id];
  if (cur >= (int)query_result.count()) {
    return false;
  }
  array <var> result = query_result[cur++];
  if (cur >= (int)query_result.count()) {
    query_result = query_results[0];
  }
  return result;
}


bool db_is_down (const MyDB &db, bool check_failed) {
  if (db.db == NULL) {
    php_warning ("DB object is NULL in DB->is_down");
    return false;
  }
  return db.db->is_down (check_failed);
}

void db_do_connect (const MyDB &db) {
  if (db.db == NULL) {
    php_warning ("DB object is NULL in DB->do_connect");
    return;
  }
  return db.db->do_connect();
}

void db_set_timeout (const MyDB &db, double new_timeout) {
  if (db.db == NULL) {
    php_warning ("DB object is NULL in DB->set_timeout");
    return;
  }
  return db.db->set_timeout (new_timeout);
}

var db_query (const MyDB &db, const string &query) {
  if (db.db == NULL) {
    php_warning ("DB object is NULL in DB->query");
    return false;
  }
  return db.db->query (query);
}

OrFalse <array <var> > db_fetch_row (const MyDB &db, const var &query_id_var) {
  if (db.db == NULL) {
    php_warning ("DB object is NULL in DB->fetch_row");
    return false;
  }
  return db.db->fetch_row (query_id_var);
}

int db_get_affected_rows (const MyDB &db) {
  if (db.db == NULL) {
    php_warning ("DB object is NULL in DB->get_affected_rows");
    return 0;
  }
  return db.db->get_affected_rows();
}

int db_get_num_rows (const MyDB &db) {
  if (db.db == NULL) {
    php_warning ("DB object is NULL in DB->get_num_rows");
    return 0;
  }
  return db.db->get_num_rows();
}

int db_get_insert_id (const MyDB &db) {
  if (db.db == NULL) {
    php_warning ("DB object is NULL in DB->get_insert_id");
    return -1;
  }
  return db.db->get_insert_id();
}


bool f$boolval (const MyDB &my_db) {
  return f$boolval (my_db.bool_value);
}

bool eq2 (const MyDB &my_db, bool value) {
  return my_db.bool_value == value;
}

bool eq2 (bool value, const MyDB &my_db) {
  return value == my_db.bool_value;
}

bool equals (bool value, const MyDB &my_db) {
  return equals (value, my_db.bool_value);
}

bool equals (const MyDB &my_db, bool value) {
  return equals (my_db.bool_value, value);
}

bool not_equals (bool value, const MyDB &my_db) {
  return not_equals (value, my_db.bool_value);
}

bool not_equals (const MyDB &my_db, bool value) {
  return not_equals (my_db.bool_value, value);
}


MyDB& MyDB::operator = (bool value) {
  bool_value = value;
  db = NULL;
  return *this;
}

MyDB::MyDB (bool value) {
  bool_value = value;
  db = NULL;
}


MyDB::MyDB (db_driver *db): bool_value (true),
                            db (db) {
}

MyDB::MyDB (void): bool_value(),
                   db (NULL) {
}


bool f$dbIsDown (bool check_failed) {
  return db_is_down (v$DB_Proxy, check_failed);
}

bool f$dbUseMaster (const string &table_name) {
  static char GlobalDbTablesMaster_storage[sizeof (array <bool>)];
  static array <bool> *GlobalDbTablesMaster = reinterpret_cast <array <bool> *> (GlobalDbTablesMaster_storage);

  static long long last_query_num = -1;
  if (dl::query_num != last_query_num) {
    new (GlobalDbTablesMaster_storage) array <bool>();
    last_query_num = dl::query_num;
  }

  if (table_name.empty()) {
    return false;
  }
  if (GlobalDbTablesMaster->isset (table_name)) {
    return GlobalDbTablesMaster->get_value (table_name);
  }
  if (TRY_CALL(bool, bool, db_is_down (v$DB_Proxy))) {
    return false;
  }
  bool old_return_die = v$DB_Proxy.db->return_die;
  int old_failed = v$DB_Proxy.db->failed;
  v$DB_Proxy.db->return_die = true;
  TRY_CALL(var, bool, db_query (v$DB_Proxy, (drivers_SB.clean() + "/*? UPDATE " + table_name + " */ SELECT 1 FROM " + table_name + " LIMIT 0").str()));
  bool is_ok = !(v$DB_Proxy.db->failed > old_failed);
  v$DB_Proxy.db->return_die = old_return_die;
  GlobalDbTablesMaster->set_value (table_name, is_ok);
  return is_ok;
}

bool f$dbIsTableDown (const string &table_name) {
  static char GlobalDbTablesDown_storage[sizeof (array <bool>)];
  static array <bool> *GlobalDbTablesDown = reinterpret_cast <array <bool> *> (GlobalDbTablesDown_storage);

  static long long last_query_num = -1;
  if (dl::query_num != last_query_num) {
    new (GlobalDbTablesDown_storage) array <bool>();
    last_query_num = dl::query_num;
  }

  if (table_name.empty()) {
    return false;
  }
  if (GlobalDbTablesDown->isset (table_name)) {
    return GlobalDbTablesDown->get_value (table_name);
  }
  if (TRY_CALL(bool, bool, db_is_down (v$DB_Proxy))) {
    return true;
  }
  bool old_return_die = v$DB_Proxy.db->return_die;
  int old_failed = v$DB_Proxy.db->failed;
  v$DB_Proxy.db->return_die = true;
  TRY_CALL(var, bool, db_query (v$DB_Proxy, (drivers_SB.clean() + "/*? UPDATE " + table_name + " */ SELECT 1 FROM " + table_name + " LIMIT 0").str()));
  bool is_down = (v$DB_Proxy.db->failed > old_failed);
  v$DB_Proxy.db->return_die = old_return_die;
  GlobalDbTablesDown->set_value (table_name, is_down);
  return is_down;
}

void f$dbSetTimeout (double new_timeout) {
  db_set_timeout (v$DB_Proxy, new_timeout);
}

bool f$dbGetReturnDie (void) {
  return v$DB_Proxy.db->return_die;
}

void f$dbSetReturnDie (bool return_die) {
  v$DB_Proxy.db->return_die = return_die;
}

bool f$dbFailed (void) {
  return v$DB_Proxy.db->failed;
}

var f$dbQuery (const string &the_query) {
  return db_query (v$DB_Proxy, the_query);
}

var f$dbQueryTry (const string &the_query, int tries_count) {
  var result = false;
  for (int i_try = 0; i_try < tries_count; i_try++) {
    if (i_try != 0) {
      int sleep_time = (i_try - 1) * 20 + 30;
      f$sleep (sleep_time);
    }
    bool db_old_failed = f$dbFailed();
    f$dbSetTimeout (30);
    var query_result = TRY_CALL(var, var, f$dbQuery (the_query));
    if (neq2 (query_result, false) && f$dbFailed() == db_old_failed) {
      result = query_result;
      break;
    }
  }
  return result;
}

OrFalse <array <var> > f$dbFetchRow (const var &query_id_var) {
  return db_fetch_row (v$DB_Proxy, query_id_var);
}

int f$dbAffectedRows (void) {
  return db_get_affected_rows (v$DB_Proxy);
}

int f$dbNumRows (void) {
  return db_get_num_rows (v$DB_Proxy);
}

void f$dbSaveFailed (void) {
  v$DB_Proxy.db->old_failed = v$DB_Proxy.db->failed;
}

bool f$dbHasFailed (void) {
  return (v$DB_Proxy.db->failed > v$DB_Proxy.db->old_failed);
}

int f$dbInsertedId (void) {
  return db_get_insert_id (v$DB_Proxy);
}


string f$mysql_error (void) {
  return v$DB_Proxy.db->error;
}

int f$mysql_errno (void) {
  return v$DB_Proxy.db->errno_;
}


void f$setDbNoDie (bool no_die) {
  DBNoDie = no_die;
}


void f$dbDeclare (void) {
  void *buf = dl::allocate (sizeof (db_driver));
  v$DB_Proxy = MyDB (new (buf) db_driver ());
}

MyDB f$new_db_decl (void) {
  f$dbDeclare();
  return v$DB_Proxy;
}


int mc_connect_to_dummy (const char *host_name, int port) {
  return -1;
}

void mc_run_query_dummy (int host_num, const char *request, int request_len, int timeout_ms, int query_type, void (*callback) (const char *result, int result_len)) {
}


int db_proxy_connect_dummy (void) {
  return -1;
}

void db_run_query_dummy (int host_num, const char *request, int request_len, int timeout_ms, void (*callback) (const char *result, int result_len)) {
}


int (*mc_connect_to) (const char *host_name, int port) = mc_connect_to_dummy;

void (*mc_run_query) (int host_num, const char *request, int request_len, int timeout_ms, int query_type, void (*callback) (const char *result, int result_len)) = mc_run_query_dummy;


int (*db_proxy_connect) (void) = db_proxy_connect_dummy;

void (*db_run_query) (int host_num, const char *request, int request_len, int timeout_ms, void (*callback) (const char *result, int result_len)) = db_run_query_dummy;


MyDB v$DB_Proxy __attribute__ ((weak));

void drivers_init_static (void) {
  INIT_VAR(MyDB, v$DB_Proxy);

  INIT_VAR(const char *, mc_method);
  INIT_VAR(bool, mc_bool_res);
  INIT_VAR(var, *mc_res_storage);
  mc_res = reinterpret_cast <var *> (mc_res_storage);

  DBNoDie = false;
}

void drivers_free_static (void) {
  CLEAR_VAR(MyDB, v$DB_Proxy);
}
