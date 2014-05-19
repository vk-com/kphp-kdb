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

array <string> f$hash_algos (void);

string f$hash (const string &algo, const string &s, bool raw_output = false);

string f$hash_hmac (const string &algo, const string &data, const string &key, bool raw_output = false);

string f$sha1 (const string &s, bool raw_output = false);

string f$md5 (const string &s, bool raw_output = false);

string f$gost (const string &s, bool raw_output = false);

string f$md5_file (const string &file_name, bool raw_output = false);

int f$crc32 (const string &s);

int f$crc32_file (const string &file_name);


bool f$openssl_public_encrypt (const string &data, string &result, const string &key);

bool f$openssl_public_encrypt (const string &data, var &result, const string &key);

bool f$openssl_private_decrypt (const string &data, string &result, const string &key);

bool f$openssl_private_decrypt (const string &data, var &result, const string &key);

OrFalse <string> f$openssl_pkey_get_private (const string &key, const string &passphrase = string());


void openssl_init_static (void);

void openssl_free_static (void);