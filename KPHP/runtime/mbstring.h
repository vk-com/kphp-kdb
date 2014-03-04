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

#include <climits>

#include "kphp_core.h"
#include "string_functions.h"

bool mb_UTF8_check (const char *s);

bool f$mb_check_encoding (const string &str, const string &encoding = CP1251);

int f$mb_strlen (const string &str, const string &encoding = CP1251);

string f$mb_strtolower (const string &str, const string &encoding = CP1251);

string f$mb_strtoupper (const string &str, const string &encoding = CP1251);

OrFalse <int> f$mb_strpos (const string &haystack, const string &needle, int offset = 0, const string &encoding = CP1251);

OrFalse <int> f$mb_stripos (const string &haystack, const string &needle, int offset = 0, const string &encoding = CP1251);

string f$mb_substr (const string &str, int start, int length = INT_MAX, const string &encoding = CP1251);
