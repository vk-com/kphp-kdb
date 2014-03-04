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

    Copyright 2013 Vkontakte Ltd
              2013 Vitaliy Valtman
*/

#ifndef __TARG_INTERFACE_STRUCTURES_H__
#define __TARG_INTERFACE_STRUCTURES_H__

struct tl_ad_enable {
  int ad;
  int price;
};

struct tl_ad_disable {
  int ad;
};

struct tl_ad_set_ctr {
  int ad;
  long long clicks;
  long long views;
};

struct tl_ad_set_sump {
  int ad;
  long long sump0;
  long long sump1;
  long long sump2;
};

struct tl_ad_set_ctr_sump {
  int ad;
  long long clicks;
  long long views;
  long long sump0;
  long long sump1;
  long long sump2;
};

struct tl_ad_set_aud {
  int ad;
  int aud;
};

struct tl_ad_limited_views {
  int ad;
  int max_views;
};

struct tl_ad_views_rate_limit {
  int ad;
  int rate_limit;
};

struct tl_ad_sites {
  int ad;
  int mask;
};

struct tl_ad_set_factor {
  int ad;
  int factor;  
};

struct tl_ad_set_domain {
  int ad;
  int domain;
};

struct tl_ad_set_categories {
  int ad;
  int category;
  int subcategory;
};

struct tl_ad_set_group {
  int ad;
  int group;
};

struct tl_ad_clicks {
  int ad;
};

struct tl_ad_ctr {
  int ad;
  int mask;
};

struct tl_ad_money {
  int ad;
};

struct tl_ad_views {
  int ad;
};


struct tl_user_view {
  int uid;
  int ad;
};
struct tl_ad_recent_views {
  int ad;
};

struct tl_recent_views_stats {
  int mode;
  int limit;
};

struct tl_recent_ad_viewers {
  int ad;
  int mode;
  int limit;
};

struct tl_ad_info {
  int ad;
};

struct tl_ad_query {
  int ad;
};

struct tl_user_groups {
  int uid;
};

struct tl_user_click {
  int uid;
  int ad;
  int price;
};

struct tl_user_flags {
  int uid;
};

struct tl_user_clicked_ad {
  int uid;
  int ad;
};

struct tl_delete_group {
  int gid;
};

struct tl_target {
  int ad;
  int mode;
  int price;
  int factor;
  int user_list_size;
  int query_len;
  int user_list[0];
};

struct tl_prices {
  int mode;
  int place;
  int and_mask;
  int xor_mask;
  int limit;
  int user_list_size;
  int query_len;
  int user_list[0];
};

struct tl_ad_pricing {
  int mode;
  int ad;
  int place;
  int and_mask;
  int xor_mask;
  int max_users;
  int limit;
  int user_list_size;
  int query_len;
  int user_list[0];
};

struct tl_targ_audience {
  int mode;
  int place;
  int cpv;
  int and_mask;
  int xor_mask;
  int max_users;
  int user_list_size;
  int query_len;
  int user_list[0];
};

struct tl_audience {
  int mode;
  int user_list_size;
  int query_len;
  int user_list[0];
};

struct tl_search {
  int mode;
  int limit;
  int user_list_size;
  int query_len;
  int user_list[0];
};

struct tl_set_custom {
  int uid;
  int type;
  int value;
};

struct tl_set_rates {
  int uid;
  int rate;
  int cute;
};

struct tl_set_username {
  int uid;
  int len;
  char name[0];
};

struct tl_set_user_group_types {
  int uid;
  int n;
  int data[0];
};

struct tl_set_country_city {
  int uid;
  int country;
  int city;
};

struct tl_set_birthday {
  int uid;
  int day;
  int month;
  int year;
};

struct tl_set_religion {
  int uid;
  int len;
  char name[0];
};

struct tl_set_hometown {
  int uid;
  int len;
  char name[0];
};

struct tl_set_proposal {
  int uid;
  int len;
  char name[0];
};

struct tl_weights_send_small_updates {
  int num;
  int data[];
};

struct tl_weights_send_updates {
  int n;
  int num;
  int data[];
};

struct tl_user_ads {
  int uid;
  int limit;
  int and_mask;
  int xor_mask;
  int flags;
  long long cat_mask;

};
#endif
