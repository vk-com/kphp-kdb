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

    Copyright      2013 Vkontakte Ltd
              2012-2013 Vitaliy Valtman (original mc-proxy-news-recommend.c)
              2012-2013 Anton Maydell (original mc-proxy-news-recommend.c)
                   2013 Vitaliy Valtman
*/

#ifndef __RPC_PROXY_MERGE_NEWS_R__
#define __RPC_PROXY_MERGE_NEWS_R__
extern struct gather_methods rnews_gather_methods;
extern struct gather_methods rnews_raw_gather_methods;

struct rnews_gather_extra {
  int type_mask;
  int date;
  int timestamp;
  int end_date;
  int acting_users_limit;
  int limit;
  int t;
  int id;
  int raw;
};

#endif

