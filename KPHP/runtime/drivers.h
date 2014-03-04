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

#include "exception.h"
#include "rpc.h"

void drivers_init_static (void);
void drivers_free_static (void);

extern const char *(*get_last_net_error) (void);

extern int (*mc_connect_to) (const char *host_name, int port);
extern void (*mc_run_query) (int host_num, const char *request, int request_len, int timeout_ms, int query_type, void (*callback) (const char *result, int result_len));

extern int (*db_proxy_connect) (void);
extern void (*db_run_query) (int host_num, const char *request, int request_len, int timeout_ms, void (*callback) (const char *result, int result_len));


int timeout_convert_to_ms (double timeout);

const string mc_prepare_key (const string &key);

var mc_get_value (string result_str, int flags);

bool mc_is_immediate_query (const string &key);


const int MEMCACHE_SERIALIZED = 1;
const int MEMCACHE_COMPRESSED = 2;

class MC_object {
public:
  virtual bool addServer (const string &host_name, int port = 11211, bool persistent = true, int weight = 1, int timeout = 1, int retry_interval = 15, bool status = true, const var &failure_callback = var(), int timeoutms = -1) = 0;
  virtual bool connect (const string &host_name, int port = 11211, int timeout = 1) = 0;
  virtual bool pconnect (const string &host_name, int port = 11211, int timeout = 1) = 0;
  virtual bool rpc_connect (const string &host_name, int port, const var &default_actor_id = 0, double timeout = 0.3, double connect_timeout = 0.3, double reconnect_timeout = 17) = 0;

  virtual bool add (const string &key, const var &value, int flags = 0, int expire = 0) = 0;
  virtual bool set (const string &key, const var &value, int flags = 0, int expire = 0) = 0;
  virtual bool replace (const string &key, const var &value, int flags = 0, int expire = 0) = 0;

  virtual var get (const var &key_var) = 0;

  virtual bool delete_ (const string &key) = 0;

  virtual var decrement (const string &key, const var &v = 1) = 0;
  virtual var increment (const string &key, const var &v = 1) = 0;

  virtual var getVersion (void) = 0;
};

class Memcache: public MC_object {
private:
  class host {
  public:
    int host_num;
    int host_weight;
    int timeout_ms;

    host (void);
    host (int host_num, int host_weight, int timeout_ms);
  };

  array <host> hosts;


  inline host get_host (const string &key);

  bool run_set (const string &key, const var &value, int flags, int expire);

  var run_increment (const string &key, const var &count);

public:
  Memcache (void);

  bool addServer (const string &host_name, int port = 11211, bool persistent = true, int weight = 1, int timeout = 1, int retry_interval = 15, bool status = true, const var &failure_callback = var(), int timeoutms = -1);
  bool connect (const string &host_name, int port = 11211, int timeout = 1);
  bool pconnect (const string &host_name, int port = 11211, int timeout = 1);
  bool rpc_connect (const string &host_name, int port, const var &default_actor_id = 0, double timeout = 0.3, double connect_timeout = 0.3, double reconnect_timeout = 17);

  bool add (const string &key, const var &value, int flags = 0, int expire = 0);
  bool set (const string &key, const var &value, int flags = 0, int expire = 0);
  bool replace (const string &key, const var &value, int flags = 0, int expire = 0);

  var get (const var &key_var);

  bool delete_ (const string &key);

  var decrement (const string &key, const var &v = 1);
  var increment (const string &key, const var &v = 1);

  var getVersion (void);
};

class RpcMemcache: public MC_object {
private:
  class host {
  public:
    rpc_connection conn;
    int host_weight;

    host (void);
    host (const string &host_name, int port, int actor_id, int host_weight, int timeout_ms);
    host (const rpc_connection &c);
  };

  array <host> hosts;


  inline host get_host (const string &key);

public:
  RpcMemcache (void);

  bool addServer (const string &host_name, int port = 11211, bool persistent = true, int weight = 1, int timeout = 1, int retry_interval = 15, bool status = true, const var &failure_callback = var(), int timeoutms = -1);
  bool connect (const string &host_name, int port = 11211, int timeout = 1);
  bool pconnect (const string &host_name, int port = 11211, int timeout = 1);
  bool rpc_connect (const string &host_name, int port, const var &default_actor_id = 0, double timeout = 0.3, double connect_timeout = 0.3, double reconnect_timeout = 17);

  bool add (const string &key, const var &value, int flags = 0, int expire = 0);
  bool set (const string &key, const var &value, int flags = 0, int expire = 0);
  bool replace (const string &key, const var &value, int flags = 0, int expire = 0);

  var get (const var &key_var);

  bool delete_ (const string &key);

  var decrement (const string &key, const var &v = 1);
  var increment (const string &key, const var &v = 1);

  var getVersion (void);
};

class MyMemcache {
private:
  bool bool_value;
  MC_object *mc;

  MyMemcache (MC_object *mc);
public:
  MyMemcache (void);

  friend bool f$memcached_addServer (const MyMemcache &mc, const string &host_name, int port, bool persistent, int weight, int timeout, int retry_interval, bool status, const var &failure_callback, int timeoutms);
  friend bool f$memcached_connect (const MyMemcache &mc, const string &host_name, int port, int timeout);
  friend bool f$memcached_pconnect (const MyMemcache &mc, const string &host_name, int port, int timeout);
  friend bool f$memcached_rpc_connect (const MyMemcache &mc, const string &host_name, int port, const var &default_actor_id, double timeout, double connect_timeout, double reconnect_timeout);

  friend bool f$memcached_add (const MyMemcache &mc, const string &key, const var &value, int flags, int expire);
  friend bool f$memcached_set (const MyMemcache &mc, const string &key, const var &value, int flags, int expire);
  friend bool f$memcached_replace (const MyMemcache &mc, const string &key, const var &value, int flags, int expire);

  friend var f$memcached_get (const MyMemcache &mc, const var &key_var);

  friend bool f$memcached_delete (const MyMemcache &mc, const string &key);

  friend var f$memcached_decrement (const MyMemcache &mc, const string &key, const var &v);
  friend var f$memcached_increment (const MyMemcache &mc, const string &key, const var &v);

  friend var f$memcached_getVersion (const MyMemcache &mc);

  friend bool f$boolval (const MyMemcache &my_mc);
  friend bool eq2 (const MyMemcache &my_mc, bool value);
  friend bool eq2 (bool value, const MyMemcache &my_mc);
  friend bool equals (bool value, const MyMemcache &my_mc);
  friend bool equals (const MyMemcache &my_mc, bool value);
  friend bool not_equals (bool value, const MyMemcache &my_mc);
  friend bool not_equals (const MyMemcache &my_mc, bool value);

  MyMemcache& operator = (bool value);
  MyMemcache (bool value);

  friend array <string> f$mcGetStats (const MyMemcache &MC);

  friend int f$mcGetClusterSize (const MyMemcache &MC);


  friend MyMemcache f$new_Memcache (void);
  friend MyMemcache f$new_RpcMemcache (void);
};

bool f$memcached_addServer (const MyMemcache &mc, const string &host_name, int port = 11211, bool persistent = true, int weight = 1, int timeout = 1, int retry_interval = 15, bool status = true, const var &failure_callback = var(), int timeoutms = -1);
bool f$memcached_connect (const MyMemcache &mc, const string &host_name, int port = 11211, int timeout = 1);
bool f$memcached_pconnect (const MyMemcache &mc, const string &host_name, int port = 11211, int timeout = 1);
bool f$memcached_rpc_connect (const MyMemcache &mc, const string &host_name, int port, const var &default_actor_id = 0, double timeout = 0.3, double connect_timeout = 0.3, double reconnect_timeout = 17);

bool f$memcached_add (const MyMemcache &mc, const string &key, const var &value, int flags = 0, int expire = 0);
bool f$memcached_set (const MyMemcache &mc, const string &key, const var &value, int flags = 0, int expire = 0);
bool f$memcached_replace (const MyMemcache &mc, const string &key, const var &value, int flags = 0, int expire = 0);

var f$memcached_get (const MyMemcache &mc, const var &key_var);

bool f$memcached_delete (const MyMemcache &mc, const string &key);

var f$memcached_decrement (const MyMemcache &mc, const string &key, const var &v = 1);
var f$memcached_increment (const MyMemcache &mc, const string &key, const var &v = 1);

var f$memcached_getVersion (const MyMemcache &mc);

bool f$boolval (const MyMemcache &my_mc);
bool eq2 (const MyMemcache &my_mc, bool value);
bool eq2 (bool value, const MyMemcache &my_mc);
bool equals (bool value, const MyMemcache &my_mc);
bool equals (const MyMemcache &my_mc, bool value);
bool not_equals (bool value, const MyMemcache &my_mc);
bool not_equals (const MyMemcache &my_mc, bool value);

array <string> f$mcGetStats (const MyMemcache &MC);

int f$mcGetClusterSize (const MyMemcache &MC);

MyMemcache f$new_Memcache (void);
MyMemcache f$new_RpcMemcache (void);


var f$rpc_mc_get (const rpc_connection &conn, const string &key, double timeout = -1.0);

template <class T>
OrFalse <array <var> > f$rpc_mc_multiget (const rpc_connection &conn, const array <T> &keys, double timeout = -1.0);

bool f$rpc_mc_set (const rpc_connection &conn, const string &key, const var &value, int flags = 0, int expire = 0, double timeout = -1.0);

bool f$rpc_mc_add (const rpc_connection &conn, const string &key, const var &value, int flags = 0, int expire = 0, double timeout = -1.0);

bool f$rpc_mc_replace (const rpc_connection &conn, const string &key, const var &value, int flags = 0, int expire = 0, double timeout = -1.0);

var f$rpc_mc_increment (const rpc_connection &conn, const string &key, const var &v = 1, double timeout = -1.0);

var f$rpc_mc_decrement (const rpc_connection &conn, const string &key, const var &v = 1, double timeout = -1.0);

bool f$rpc_mc_delete (const rpc_connection &conn, const string &key, double timeout = -1.0);


bool f$dbIsDown (bool check_failed = true);

bool f$dbUseMaster (const string &table_name);

bool f$dbIsTableDown (const string &table_name);

template <class T>
bool f$dbUseMasterAll (const array <T> &tables_names);

template <class T>
bool f$dbIsTableDownAll (const array <T> &tables_names);

void f$dbSetTimeout (double new_timeout);

bool f$dbGetReturnDie (void);

void f$dbSetReturnDie (bool return_die = true);

bool f$dbFailed (void);

var f$dbQuery (const string &the_query);

var f$dbQueryTry (const string &the_query, int tries_count = 3);

OrFalse <array <var> > f$dbFetchRow (const var &query_id = -1);

int f$dbAffectedRows (void);

int f$dbNumRows (void);

void f$dbSaveFailed (void);

bool f$dbHasFailed (void);

int f$dbInsertedId (void);

template <class T>
array <string> f$dbInsertString (const array <T> &data, bool no_escape = false);

template <class T>
string f$dbUpdateString (const array <T> &data);

int f$dbId (void);

class db_driver {
private:
  int old_failed;
  int failed;
  bool return_die;

  const char *sql_host;

  int connection_id;
  int connected; // K.O.T.: 1 = connected, -1 = error, -2 = down
  bool is_proxy;
  int next_timeout_ms;

  int last_query_id;
  int biggest_query_id;
  string error;
  int errno_;
  int affected_rows;
  int insert_id;
  array <array <array <var> > > query_results;
  array <int> cur_pos;

  int field_cnt;
  array <string> field_names;

  void fatal_error (const string &query);

public:
  db_driver (void);

  bool is_down (bool check_failed = true);

  void do_connect (void);

  void set_timeout (double new_timeout = 0);

  var query (const string &query_str);

  OrFalse <array <var> > fetch_row (const var &query_id_var);

  int get_affected_rows (void);

  int get_num_rows (void);

  int get_insert_id (void);

  template <class T>
  array <string> compile_db_insert_string (const array <T> &data, bool no_escape = false);

  template <class T>
  string compile_db_update_string (const array <T> &data);


  bool mysql_query (const string &query);

  OrFalse <array <var> > mysql_fetch_array (int query_id);


  friend bool f$dbIsDown (bool check_failed);

  friend bool f$dbUseMaster (const string &table_name);

  friend bool f$dbIsTableDown (const string &table_name);

  template <class T>
  friend bool f$dbUseMasterAll (const array <T> &tables_names);

  template <class T>
  friend bool f$dbIsTableDownAll (const array <T> &tables_names);

  friend void f$dbSetTimeout (double new_timeout);

  friend bool f$dbGetReturnDie (void);

  friend void f$dbSetReturnDie (bool return_die);

  friend bool f$dbFailed (void);

  friend var f$dbQuery (const string &the_query);

  friend OrFalse <array <var> > f$dbFetchRow (const var &query_id_var);

  friend int f$dbAffectedRows (void);

  friend int f$dbNumRows (void);

  friend void f$dbSaveFailed (void);

  friend bool f$dbHasFailed (void);

  friend int f$dbInsertedId (void);

  friend int f$dbId (void);


  friend string f$mysql_error (void);

  friend int f$mysql_errno (void);
};


class MyDB {
private:
  bool bool_value;
  db_driver *db;

  MyDB (db_driver *db);
public:
  MyDB (void);

  friend bool db_is_down (const MyDB &db, bool check_failed);

  friend void db_do_connect (const MyDB &db);

  friend void db_set_timeout (const MyDB &db, double new_timeout);

  friend var db_query (const MyDB &db, const string &query);

  friend OrFalse <array <var> > db_fetch_row (const MyDB &db, const var &query_id_var);

  friend int db_get_affected_rows (const MyDB &db);

  friend int db_get_num_rows (const MyDB &db);

  friend int db_get_insert_id (const MyDB &db);

  template <class T>
  friend array <string> db_compile_db_insert_string (const MyDB &db, const array <T> &data, bool no_escape);

  template <class T>
  friend string db_compile_db_update_string (const MyDB &db, const array <T> &data);


  friend bool f$boolval (const MyDB &my_db);
  friend bool eq2 (const MyDB &my_db, bool value);
  friend bool eq2 (bool value, const MyDB &my_db);
  friend bool equals (bool value, const MyDB &my_db);
  friend bool equals (const MyDB &my_db, bool value);
  friend bool not_equals (bool value, const MyDB &my_db);
  friend bool not_equals (const MyDB &my_db, bool value);

  MyDB& operator = (bool value);
  MyDB (bool value);

  friend void f$dbDeclare (void);

  friend MyDB f$new_db_decl (void);


  friend bool f$dbIsDown (bool check_failed);

  friend bool f$dbUseMaster (const string &table_name);

  friend bool f$dbIsTableDown (const string &table_name);

  template <class T>
  friend bool f$dbUseMasterAll (const array <T> &tables_names);

  template <class T>
  friend bool f$dbIsTableDownAll (const array <T> &tables_names);

  friend void f$dbSetTimeout (double new_timeout);

  friend bool f$dbGetReturnDie (void);

  friend void f$dbSetReturnDie (bool return_die);

  friend bool f$dbFailed (void);

  friend var f$dbQuery (const string &the_query);

  friend OrFalse <array <var> > f$dbFetchRow (const var &query_id_var);

  friend int f$dbAffectedRows (void);

  friend int f$dbNumRows (void);

  friend void f$dbSaveFailed (void);

  friend bool f$dbHasFailed (void);

  friend int f$dbInsertedId (void);

  friend int f$dbId (void);


  friend string f$mysql_error (void);

  friend int f$mysql_errno (void);
};

bool db_is_down (const MyDB &db, bool check_failed = true);

void db_do_connect (const MyDB &db);

void db_set_timeout (const MyDB &db, double new_timeout = 0);

var db_query (const MyDB &db, const string &query);

OrFalse <array <var> > db_fetch_row (const MyDB &db, const var &query_id_var);

int db_get_affected_rows (const MyDB &db);

int db_get_num_rows (const MyDB &db);

int db_get_insert_id (const MyDB &db);

template <class T>
array <string> db_compile_db_insert_string (const MyDB &db, const array <T> &data, bool no_escape = false);

template <class T>
string db_compile_db_update_string (const MyDB &db, const array <T> &data);

bool f$boolval (const MyDB &my_db);
bool eq2 (const MyDB &my_db, bool value);
bool eq2 (bool value, const MyDB &my_db);
bool equals (bool value, const MyDB &my_db);
bool equals (const MyDB &my_db, bool value);
bool not_equals (bool value, const MyDB &my_db);
bool not_equals (const MyDB &my_db, bool value);


string f$mysql_error (void);

int f$mysql_errno (void);


void f$setDbNoDie (bool no_die = true);

void f$dbDeclare (void);

MyDB f$new_db_decl (void);


/*
 *
 *     IMPLEMENTATION
 *
 */


const int MEMCACHE_ERROR = 0x7ae432f5;
const int MEMCACHE_VALUE_NOT_FOUND = 0x32c42422;
const int MEMCACHE_VALUE_LONG = 0x9729c42;
const int MEMCACHE_VALUE_STRING = 0xa6bebb1a;
const int MEMCACHE_FALSE = 0xbc799737;
const int MEMCACHE_TRUE = 0x997275b5;
const int MEMCACHE_SET = 0xeeeb54c4;
const int MEMCACHE_ADD = 0xa358f31c;
const int MEMCACHE_REPLACE = 0x2ecdfaa2;
const int MEMCACHE_INCR = 0x80e6c950;
const int MEMCACHE_DECR = 0x6467e0d9;
const int MEMCACHE_DELETE = 0xab505c0a;
const int MEMCACHE_GET = 0xd33b13ae;

extern const char *mc_method;

template <class T>
OrFalse <array <var> > f$rpc_mc_multiget (const rpc_connection &conn, const array <T> &keys, double timeout) {
  mc_method = "multiget";

  array <string> query_names (array_size (keys.count(), 0, true));
  array <long long> request_ids (array_size (keys.count(), 0, true));
  long long first_request_id = 0;
  for (typeof (keys.begin()) it = keys.begin(); it != keys.end(); ++it) {
    const string key = f$strval (it.get_value());
    const string real_key = mc_prepare_key (key);
    int is_immediate = mc_is_immediate_query (real_key);

    f$rpc_clean();
    f$store_int (MEMCACHE_GET);
    store_string (real_key.c_str() + is_immediate, real_key.size() - is_immediate);

    long long request_id = rpc_send (conn, timeout);
    if (request_id > 0) {
      if (first_request_id == 0) {
        first_request_id = request_id;
      }
      if (!is_immediate) {
        request_ids.push_back (request_id);
      }
      query_names.push_back (key);
    } else {
      return false;
    }
  }
  //TODO flush

  int keys_n = request_ids.count();
  if (keys_n == 0) {
    return array <var> ();
  }

  long long queue_id = rpc_create_queue (&request_ids[0], keys_n);

  array <var> result (array_size (0, keys_n, false));
  for (int i = 0; i < keys_n; i++) {
    long long request_id = rpc_get_next_request_id (queue_id);
    if (request_id <= 0) {
      break;
    }
    int k = (int)(request_id - first_request_id);
    php_assert ((unsigned int)k < (unsigned int)keys_n);

    php_assert (rpc_get_and_parse (request_id));

    int res = TRY_CALL(int, bool, (f$fetch_int (string(), -1)));//TODO __FILE__ and __LINE__
    if (res != MEMCACHE_VALUE_STRING) {
      continue;
    }

    string value = TRY_CALL(string, bool, (f$fetch_string (string(), -1)));
    int flags = TRY_CALL(int, bool, (f$fetch_int (string(), -1)));
    result.set_value (query_names.get_value (k), mc_get_value (value, flags));
  }

  return result;
}


extern MyDB v$DB_Proxy;

var long_long_convert_to_var (long long result);

template <class T>
var f$rpc_queue_create (const array <T> &request_ids) {
  static_SB.clean();
  for (typename array <T>::const_iterator p = request_ids.begin(); p != request_ids.end(); ++p) {
    long long request_id = f$longval (p.get_value()).l;
    static_SB.append ((const char *)&request_id, sizeof (long long));
  }
  return long_long_convert_to_var (rpc_create_queue ((long long *)static_SB.c_str(), request_ids.count()));
}


template <class T>
array <string> db_driver::compile_db_insert_string (const array <T> &data, bool no_escape) {
  static_SB.clean();
  for (typename array <T>::const_iterator p = data.begin(); p != data.end(); ++p) {
    if (p != data.begin()) {
      static_SB += ',';
    }

    static_SB += p.get_key();
  }
  string field_names = static_SB.str();

  static_SB.clean();
  for (typename array <T>::const_iterator p = data.begin(); p != data.end(); ++p) {
    if (p != data.begin()) {
      static_SB += ',';
    }

    static_SB += '\'';
    const T &value = p.get_value();
    if (!no_escape) {
      const string v = f$strval (value);
      for (int i = 0; i < (int)v.size(); i++) {
        if (v[i] == '\'') {
          static_SB += '\\';
        }
        static_SB += v[i];
      }
    } else {
      static_SB += value;
    }
    static_SB += '\'';
  }
  string field_values = static_SB.str();

  array <string> result (array_size (2, 2, false));
  result.set_value (string ("FIELD_NAMES", 11), field_names);
  result.set_value (string ("FIELD_VALUES", 12), field_values);
  result.set_value (0, field_names);
  result.set_value (1, field_values);
  return result;
}

template <class T>
string db_driver::compile_db_update_string (const array <T> &data) {
  static_SB.clean();
  for (typename array <T>::const_iterator p = data.begin(); p != data.end(); ++p) {
    if (p != data.begin()) {
      static_SB += ',';
    }

    static_SB += p.get_key();
    static_SB += "='";
    const string v = f$strval (p.get_value());
    for (int i = 0; i < (int)v.size(); i++) {
      if (v[i] == '\'') {
        static_SB += '\\';
      }
      static_SB += v[i];
    }
    static_SB += '\'';
  }

  return static_SB.str();
}

template <class T>
array <string> db_compile_db_insert_string (const MyDB &db, const array <T> &data, bool no_escape) {
  if (db.db == NULL) {
    php_warning ("DB object is NULL in DB->compile_db_insert_string");
    return array <var> ();
  }
  return db.db->compile_db_insert_string (data, no_escape);
}

template <class T>
string db_compile_db_update_string (const MyDB &db, const array <T> &data) {
  if (db.db == NULL) {
    php_warning ("DB object is NULL in DB->compile_db_update_string");
    return string();
  }
  return db.db->compile_db_update_string (data);
}


template <class T>
bool f$dbUseMasterAll (const array <T> &tables_names) {
  bool is_ok = true;
  for (typename array <T>::const_iterator p = tables_names.begin(); p != tables_names.end(); ++p) {
    is_ok = TRY_CALL(bool, bool, f$dbUseMaster (f$strval (p.get_value()))) && is_ok;
  }
  return is_ok;
}

template <class T>
bool f$dbIsTableDownAll (const array <T> &tables_names) {
  bool is_down = false;
  for (typename array <T>::const_iterator p = tables_names.begin(); p != tables_names.end(); ++p) {
    is_down = TRY_CALL(bool, bool, f$dbIsTableDown (f$strval (p.get_value()))) || is_down;
  }
  return is_down;
}


template <class T>
array <string> f$dbInsertString (const array <T> &data, bool no_escape) {
  return db_compile_db_insert_string (v$DB_Proxy, data, no_escape);
}

template <class T>
string f$dbUpdateString (const array <T> &data) {
  return db_compile_db_update_string (v$DB_Proxy, data);
}
