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

bool f$checkdate (int month, int day, int year);

string f$date (const string &format, int timestamp = INT_MIN);

bool f$date_default_timezone_set (const string &s);

string f$date_default_timezone_get (void);

array <var> f$getdate (int timestamp = INT_MIN);

string f$gmdate (const string &format, int timestamp = INT_MIN);

int f$gmmktime (int h = INT_MIN, int m = INT_MIN, int s = INT_MIN, int month = INT_MIN, int day = INT_MIN, int year = INT_MIN);

array <var> f$localtime (int timestamp = INT_MIN, bool is_associative = false);

string microtime (void);

double microtime (bool get_as_float);

var f$microtime (bool get_as_float = false);

int f$mktime (int h = INT_MIN, int m = INT_MIN, int s = INT_MIN, int month = INT_MIN, int day = INT_MIN, int year = INT_MIN);

string f$strftime (const string &format, int timestamp = INT_MIN);

OrFalse <int> f$strtotime (const string &time_str, int timestamp = INT_MIN);

int f$time (void);


void datetime_init_static (void);