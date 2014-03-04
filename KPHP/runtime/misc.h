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


OrFalse <string> f$iconv (const string &input_encoding, const string &output_encoding, const string &input_str);


void f$sleep (const int &seconds);

void f$usleep (const int &micro_seconds);


int f$posix_getpid (void);


string f$serialize (const var &v);

var f$unserialize (const string &v);

string f$json_encode (const var &v, bool simple_encode = false);

var f$json_decode (const string &v, bool assoc = false);

string f$print_r (const var &v, bool buffered = false);

void f$var_dump (const var &v);


/** For local usage only **/
int f$system (const string &query);
