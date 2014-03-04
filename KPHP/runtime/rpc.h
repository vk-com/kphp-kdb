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

#include "integer_types.h"

extern int (*rpc_connect_to) (const char *host_name, int port);
extern long long (*rpc_send_query) (int host_num, const char *request, int request_len, int timeout_ms);
extern void (*rpc_get_result) (long long request_id, void (*callback) (const int *data, int data_len));
extern void (*rpc_send_session_message) (long long auth_key_id, long long session_id, const char *request, int request_len);
extern void (*rpc_answer) (const char *res, int res_len);
extern long long (*rpc_create_queue) (long long *request_ids, int request_ids_len);
extern bool (*rpc_is_queue_empty) (long long queue_id);
extern long long (*rpc_get_next_request_id) (long long queue_id);
extern void (*script_error) (void);

void rpc_parse (const int *rpc_data, int rpc_data_len);

void f$rpc_parse (const string &rpc_data);

void f$rpc_parse (const var &rpc_data);

int rpc_get_pos (void);

bool rpc_set_pos (int pos);

int rpc_lookup_int (const string &file, int line);

int f$fetch_int (const string &file, int line);

UInt f$fetch_UInt (const string &file, int line);

Long f$fetch_Long (const string &file, int line);

ULong f$fetch_ULong (const string &file, int line);

var f$fetch_unsigned_int (const string &file, int line);

var f$fetch_long (const string &file, int line);

var f$fetch_unsigned_long (const string &file, int line);

string f$fetch_unsigned_int_hex (const string &file, int line);

string f$fetch_unsigned_long_hex (const string &file, int line);

string f$fetch_unsigned_int_str (const string &file, int line);

string f$fetch_unsigned_long_str (const string &file, int line);

double f$fetch_double (const string &file, int line);

string f$fetch_string (const string &file, int line);

bool f$fetch_eof (const string &file, int line);//TODO remove parameters

bool f$fetch_end (const string &file, int line);


struct rpc_connection {
  bool bool_value;
  int host_num;
  int timeout_ms;
  long long default_actor_id;
  int connect_timeout;
  int reconnect_timeout;

  rpc_connection (void);

  rpc_connection (bool value);

  rpc_connection (bool value, int host_num, int timeout_ms, long long default_actor_id, int connect_timeout, int reconnect_timeout);

  rpc_connection& operator = (bool value);
};

rpc_connection f$new_rpc_connection (string host_name, int port, const var &default_actor_id = 0, double timeout = 0.3, double connect_timeout = 0.3, double reconnect_timeout = 17);

bool f$boolval (const rpc_connection &my_rpc);

bool eq2 (const rpc_connection &my_rpc, bool value);

bool eq2 (bool value, const rpc_connection &my_rpc);

bool equals (bool value, const rpc_connection &my_rpc);

bool equals (const rpc_connection &my_rpc, bool value);

bool not_equals (bool value, const rpc_connection &my_rpc);

bool not_equals (const rpc_connection &my_rpc, bool value);


void f$store_gzip_pack_threshold (int pack_threshold_bytes);

void f$store_start_gzip_pack (void);

void f$store_finish_gzip_pack (int threshold);

bool f$store_header (const var &cluster_id, int flags = 0);

bool store_error (int error_code, const char *error_text);
bool f$store_error (int error_code, const string &error_text);

bool f$store_int (int v);

bool f$store_UInt (UInt v);

bool f$store_Long (Long v);

bool f$store_ULong (ULong v);

bool f$store_unsigned_int (const string &v);

bool store_long (long long v);
bool f$store_long (const string &v);

bool f$store_unsigned_long (const string &v);

bool f$store_unsigned_int_hex (const string &v);

bool f$store_unsigned_long_hex (const string &v);

bool f$store_double (double v);

bool store_string (const char *v, int v_len);
bool f$store_string (const string &v);

bool f$store_many (const array <var> &a);

bool f$store_finish (void);

bool f$rpc_clean (bool is_error = false);

bool rpc_store (bool is_error = false);

long long rpc_send (const rpc_connection &conn, double timeout);
var f$rpc_send (const rpc_connection &conn, double timeout = -1.0);

OrFalse <string> f$rpc_get (int request_id);
OrFalse <string> rpc_get (long long request_id);
OrFalse <string> f$rpc_get (const string &request_id);

bool f$rpc_get_and_parse (int request_id);
bool rpc_get_and_parse (long long request_id);
bool f$rpc_get_and_parse (const string &request_id);

bool f$rpc_send_session_msg (int auth_key_id, int session_id);
bool rpc_send_session_msg (long long auth_key_id, long long session_id);
bool f$rpc_send_session_msg (const string &auth_key_id, const string &session_id);

var f$rpc_get_any_qid (void);

template <class T>
var f$rpc_queue_create (const array <T> &request_ids);

bool f$rpc_queue_empty (int queue_id);
bool rpc_queue_empty (long long queue_id);
bool f$rpc_queue_empty (const string &queue_id);

var f$rpc_queue_next (int queue_id);
var rpc_queue_next (long long queue_id);
var f$rpc_queue_next (const string &queue_id);


bool f$store_unsigned_int (const var &v);

bool f$store_long (const var &v);

bool f$store_unsigned_long (const var &v);

OrFalse <string> f$rpc_get (const var &v);

bool f$rpc_get_and_parse (const var &v);

bool f$rpc_send_session_msg (const var &auth_key_id, const var &session_id);

bool f$rpc_queue_empty (const var &queue_id);

var f$rpc_queue_next (const var &queue_id);


var f$rpc_tl_query_one (const rpc_connection &c, const var &arr, double timeout = -1.0);

array <var> f$rpc_tl_query (const rpc_connection &c, const array <var> &arr, double timeout = -1.0);

array <var> f$rpc_tl_query_result_one (const var &query_id_var);

array <array <var> > f$rpc_tl_query_result (const array <var> &query_ids);


void rpc_init_static (void);

void rpc_free_static (void);

