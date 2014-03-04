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

#ifndef __VKEXT_H__
#define __VKEXT_H__

#define VKEXT_VERSION "0.84"
#define VKEXT_NAME "vk_extension"

#define USE_ZEND_ALLOC 1

PHP_MINIT_FUNCTION(vkext);
PHP_RINIT_FUNCTION(vkext);
PHP_MSHUTDOWN_FUNCTION(vkext);
PHP_RSHUTDOWN_FUNCTION(vkext);

PHP_FUNCTION(vk_hello_world);
PHP_FUNCTION(vk_upcase);
PHP_FUNCTION(vk_utf8_to_win);
PHP_FUNCTION(vk_win_to_utf8);
PHP_FUNCTION(vk_flex);
PHP_FUNCTION(vk_whitespace_pack);
PHP_FUNCTION(rpc_clean);
PHP_FUNCTION(rpc_send);
PHP_FUNCTION(rpc_send_noflush);
PHP_FUNCTION(rpc_flush);
PHP_FUNCTION(rpc_get_and_parse);
PHP_FUNCTION(rpc_get);
PHP_FUNCTION(rpc_get_any_qid);
PHP_FUNCTION(rpc_parse);
PHP_FUNCTION(new_rpc_connection);
PHP_FUNCTION(store_int);
PHP_FUNCTION(store_long);
PHP_FUNCTION(store_string);
PHP_FUNCTION(store_double);
PHP_FUNCTION(store_many);
PHP_FUNCTION(store_header);
PHP_FUNCTION(fetch_int);
PHP_FUNCTION(fetch_long);
PHP_FUNCTION(fetch_double);
PHP_FUNCTION(fetch_string);
PHP_FUNCTION(fetch_end);
PHP_FUNCTION(fetch_eof);
PHP_FUNCTION(vk_set_error_verbosity);
PHP_FUNCTION(rpc_queue_create);
PHP_FUNCTION(rpc_queue_empty);
PHP_FUNCTION(rpc_queue_next);
PHP_FUNCTION(vk_clear_stats);
PHP_FUNCTION(vk_rpc_memcache_get);
PHP_FUNCTION(vk_rpc_memcache_get_multi);
PHP_FUNCTION(vk_rpc_memcache_set);
PHP_FUNCTION(vk_rpc_memcache_add);
PHP_FUNCTION(vk_rpc_memcache_replace);
PHP_FUNCTION(vk_rpc_memcache_incr);
PHP_FUNCTION(vk_rpc_memcache_decr);
PHP_FUNCTION(vk_rpc_memcache_delete);
PHP_FUNCTION(rpc_tl_query);
PHP_FUNCTION(rpc_tl_query_result);
PHP_FUNCTION(vkext_prepare_stats);
PHP_FUNCTION(tl_config_load);
PHP_FUNCTION(tl_config_load_file);

#define PHP_WARNING(t) (NULL TSRMLS_CC, E_WARNING, t);
#define PHP_ERROR(t) (NULL TSRMLS_CC, E_ERROR, t);
#define PHP_NOTICE(t) (NULL TSRMLS_CC, E_NOTICE, t);

extern zend_module_entry vkext_module_entry;
#define phpext_vkext_ptr &vkext_module_ptr

void write_buff (const char *s, int l);
void write_buff_char (char c);
void write_buff_long (long x);
void write_buff_double (double x);
void print_backtrace (void);
#endif
