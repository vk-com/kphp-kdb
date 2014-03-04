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

    Copyright 2009-2013 Vkontakte Ltd
              2009-2013 Nikolai Durov
              2009-2013 Andrei Lopatin
*/

#ifndef __KDB_TARG_BINLOG_H__
#define __KDB_TARG_BINLOG_H__

#include "kdb-binlog-common.h"

#pragma	pack(push,4)

#ifndef	TARG_SCHEMA_BASE
#define	TARG_SCHEMA_BASE	0x6ba30000
#endif

#define	TARG_SCHEMA_V1	0x6ba30101

#define	LEV_TARG_USERNAME	0x23a88000
#define	LEV_TARG_USERFLAGS	0x76aa9300
#define	LEV_TARG_USER_HASPHOTO	0x76aa9300
#define	LEV_TARG_UNIVCITY	0x19acd032
#define	LEV_TARG_BIRTHDAY	0x72a43fe3
#define	LEV_TARG_SEX		0x2bbe2e3a
#define	LEV_TARG_OPERATOR	0x4628f413
#define	LEV_TARG_BROWSER	0x507abe8a
#define	LEV_TARG_REGION 	0x3c520a7b
#define LEV_TARG_HEIGHT		0x6a94bf28
#define LEV_TARG_SMOKING	0x5aeb29b3
#define LEV_TARG_ALCOHOL	0x2b837bca
#define LEV_TARG_PPRIORITY	0x736ad7b3
#define LEV_TARG_IIOTHERS	0x18e6cfac
#define LEV_TARG_CVISITED	0x40a3dcaa
#define LEV_TARG_GCOUNTRY	0x5e10c0da
#define LEV_TARG_CUSTOM		0x28ef1a00
#define LEV_TARG_CUSTOM1	(LEV_TARG_CUSTOM + 1)
#define LEV_TARG_CUSTOM2	(LEV_TARG_CUSTOM + 2)
#define LEV_TARG_CUSTOM3	(LEV_TARG_CUSTOM + 3)
#define LEV_TARG_CUSTOM4	(LEV_TARG_CUSTOM + 4)
#define LEV_TARG_CUSTOM5	(LEV_TARG_CUSTOM + 5)
#define LEV_TARG_CUSTOM6	(LEV_TARG_CUSTOM + 6)
#define LEV_TARG_CUSTOM7	(LEV_TARG_CUSTOM + 7)
#define LEV_TARG_CUSTOM8	(LEV_TARG_CUSTOM + 8)
#define LEV_TARG_CUSTOM9	(LEV_TARG_CUSTOM + 9)
#define LEV_TARG_CUSTOM10	(LEV_TARG_CUSTOM + 10)
#define LEV_TARG_CUSTOM11	(LEV_TARG_CUSTOM + 11)
#define LEV_TARG_CUSTOM12	(LEV_TARG_CUSTOM + 12)
#define LEV_TARG_CUSTOM13	(LEV_TARG_CUSTOM + 13)
#define LEV_TARG_CUSTOM14	(LEV_TARG_CUSTOM + 14)
#define LEV_TARG_CUSTOM15	(LEV_TARG_CUSTOM + 15)
#define LEV_TARG_TIMEZONE	0x27eadcb1
#define LEV_TARG_HIDDEN         0x39f6341d
#define LEV_TARG_RATECUTE	0x681aca23
#define LEV_TARG_RATE		0x681aca22
#define LEV_TARG_CUTE		0x681aca21
#define	LEV_TARG_ONLINE		0x478bd239
#define	LEV_TARG_MSTATUS	0x196d4acc
#define	LEV_TARG_POLITICAL	0x072fee12
#define	LEV_TARG_PRIVACY	0x128ac176
#define	LEV_TARG_DELUSER	0x3731aaa3
#define LEV_TARG_DELGROUP       0x7e01ab18
#define LEV_TARG_EDU_CLEAR	0x1ff31462
#define LEV_TARG_EDUCLEAR	LEV_TARG_EDU_CLEAR	//backward compatibility
#define LEV_TARG_EDUADD		0x12378dde
#define LEV_TARG_EDUADD_PRIM	0x12378ddf
#define	LEV_TARG_RELIGION	0x4e02ab00
#define	LEV_TARG_SCH_CLEAR	0x281ba124
#define	LEV_TARG_SCH_ADD	0x561ac700
#define	LEV_TARG_COMP_CLEAR	0x609a1552
#define	LEV_TARG_COMP_ADD	0x5eca1000
#define	LEV_TARG_ADDR_CLEAR	0x259cf167
#define	LEV_TARG_ADDR_ADD	0x301abca3
#define	LEV_TARG_ADDR_EXT_ADD	0x5d12d300
#define	LEV_TARG_GRTYPE_CLEAR	0x4bbd2152
#define	LEV_TARG_GRTYPE_ADD	0x6614a380
#define	LEV_TARG_INTERESTS	0x172aac00
#define	LEV_TARG_INTERESTS_CLEAR	0x6fa38800
#define	LEV_TARG_MIL_CLEAR	0x5212dc53
#define	LEV_TARG_MIL_ADD	0x39da59e5
#define	LEV_TARG_GROUP_CLEAR	0x27ef21d5
#define	LEV_TARG_GROUP_POS_CLEAR	0x27ef22d1
#define	LEV_TARG_GROUP_NEG_CLEAR	0x27ef22d2
#define	LEV_TARG_GROUP_ADD	0x612ad400
#define	LEV_TARG_GROUP_EXT_ADD	0x383acd56
#define	LEV_TARG_GROUP_DEL	0x427ca400
#define	LEV_TARG_GROUP_EXT_DEL	0x1f3219e5
#define	LEV_TARG_HOMETOWN	0x32b22c00
#define	LEV_TARG_PROPOSAL	0x5e574def
#define	LEV_TARG_PROPOSAL_CLEAR	0x481904d3
#define LEV_TARG_PROPOSAL_DEL	LEV_TARG_PROPOSAL_CLEAR	//backward compatibility
#define LEV_TARG_LANG_ADD	0x26be5e00
#define LEV_TARG_LANG_DEL	0x3a6e8a00
#define LEV_TARG_LANG_CLEAR	0x77aceb2e
#define LEV_TARG_USER_GROUP_TYPES	0x40e22bf3

#define	LEV_TARG_TARGET		0x2f237ab3
#define	LEV_TARG_AD_PRICE	0x28acd252
#define	LEV_TARG_AD_ON		0x53af3214
#define	LEV_TARG_AD_OFF		0x3cda2135
#define LEV_TARG_AD_RETARGET	0x1e8a7ccc
#define LEV_TARG_AD_SETCTR	0x4a821efa
#define LEV_TARG_AD_SETCTR_PACK	0x70a11c00
#define LEV_TARG_AD_SETAUD	0x66ee1002
#define LEV_TARG_AD_SETSUMP	0x58c425db
#define LEV_TARG_AD_SETFACTOR	0x3afac104
#define LEV_TARG_AD_SETDOMAIN		0x28763abc
#define LEV_TARG_AD_SETCATEGORIES	0x66ac0ea1
#define LEV_TARG_AD_SETGROUP		0x33aacece
#define LEV_TARG_AD_LIMIT_USER_VIEWS	0x4ecc0000
#define LEV_TARG_AD_LIMIT_RECENT_VIEWS	0x51eb0000
#define LEV_TARG_AD_DO_NOT_ALLOW_SITES	0x4003aeda
#define LEV_TARG_AD_DO_ALLOW_SITES	0x4003aedb
#define LEV_TARG_AD_SETSITEMASK	0x7c21ba00

#define	LEV_TARG_CLICK		0x641ac127
#define	LEV_TARG_CLICK_EXT	0x6863163e
#define	LEV_TARG_VIEWS		0x1ac44512
#define	LEV_TARG_USER_VIEW	0x44fc10ad
#define LEV_TARG_ONLINE_LITE	0x4e3a3ebc

#define LEV_TARG_STAT_LOAD	0x3ae6beda
#define LEV_TARG_GLOBAL_CLICK_STATS	0x5ab310ad

#define MAX_INTERESTS	15

struct lev_username {
  lev_type_t type;
  int user_id;
  char s[1];
};

struct lev_userflags {
  lev_type_t type;
  int user_id;
};

struct lev_univcity {
  lev_type_t type;
  int user_id;
  int uni_country;
  int uni_city;
};

struct lev_birthday {
  lev_type_t type;
  int user_id;
  char day;
  char month;
  short year;
};

struct lev_online {
  lev_type_t type;
  int user_id;
  int ip;
  int time;
};


struct lev_online_lite {
  lev_type_t type;
  int user_id;
};

struct lev_ratecute {
  lev_type_t type;
  int user_id;
  int rate;
  int cute;
};

struct lev_cute {
  lev_type_t type;
  int user_id;
  int cute;
};

struct lev_rate {
  lev_type_t type;
  int user_id;
  int rate;
};

struct lev_political {
  lev_type_t type;
  int user_id;
  int polit_views;
};

struct lev_mstatus {
  lev_type_t type;
  int user_id;
  int marital_status;
};

struct lev_user_generic {
  lev_type_t type;
  int user_id;
  int a, b, c, d, e;
};

struct lev_education {
  lev_type_t type;
  int user_id;
  int grad_year;
  int chair;
  int faculty;
  int university;
  int city;
  char country;
  char edu_form;
  char edu_status;
  char reserved;
};

struct lev_religion {
  lev_type_t type;
  int user_id;
  char str[1];
};

// country, city, cityName, school, schoolName, 
// grad, start, finish, class, spec, school_type

struct lev_school {
  lev_type_t type;
  int user_id;
  int city;
  int school;
  short start;
  short finish;
  short grad;
  unsigned char country;
  char sch_class;
  char sch_type;
  char spec[1];
};

// country, city, cityName, company, companyName, start, finish, position

struct lev_company {
  lev_type_t type;
  int user_id;
  int city;
  int company;
  short start;
  short finish;
  unsigned char country;
  char text[1];
};

struct lev_address {
  lev_type_t type;
  int user_id;
  int city;
  int district;
  int station;
  int street;
  unsigned char country;
  unsigned char atype;
};

struct lev_address_extended {
  lev_type_t type;
  int user_id;
  int city;
  int district;
  int station;
  int street;
  unsigned char country;
  unsigned char atype;
  char text[1];
};

struct lev_interests {
  lev_type_t type;
  int user_id;
  unsigned short len;
  char text[2];
};

struct lev_military {
  lev_type_t type;
  int user_id;
  int unit_id;
  short start;
  short finish;
};

struct lev_langs {
  lev_type_t type;
  int user_id;
  int langs[0];
};

struct lev_groups {
  lev_type_t type;
  int user_id;
  int groups[1];
};

#define MAX_USER_LEV_GROUPS	0x10000

struct lev_groups_ext {
  lev_type_t type;
  int user_id;
  int gr_num;
  int groups[1];
};

struct lev_hometown {
  lev_type_t type;
  int user_id;
  char text[1];
};

struct lev_proposal {
  lev_type_t type;
  int user_id;
  short len;
  char text[1];
};

struct lev_delete_user {
  lev_type_t type;
  int user_id;
};

struct lev_delete_group {
  lev_type_t type;
  int group_id;
};

/* ... */

struct lev_targ_target {
  lev_type_t type;
  int ad_id;
  int ad_price;
  short ad_query_len;
  char ad_query[1];
};

struct lev_targ_ad_price {
  lev_type_t type;
  int ad_id;
  int ad_price;
};

struct lev_targ_ad_on {
  lev_type_t type;
  int ad_id;
};

struct lev_targ_ad_off {
  lev_type_t type;
  int ad_id;
};

struct lev_targ_click {
  lev_type_t type;
  int ad_id;
  int user_id;
};

struct lev_targ_click_ext {
  lev_type_t type;
  int ad_id;
  int user_id;
  int price;
};

struct lev_targ_views {
  lev_type_t type;
  int ad_id;
  int views;
};

struct lev_targ_stat_load {
  lev_type_t type;
  int ad_id;
  int clicked;
  int click_money;
  int views;
  int l_clicked;
  int l_views;
};

struct lev_targ_user_view {
  lev_type_t type;
  int ad_id;
  int user_id;
};

struct lev_targ_ad_retarget {
  lev_type_t type;
  int ad_id;
};

struct lev_targ_ad_setaud {
  lev_type_t type;
  int ad_id;
  int aud;
};

struct lev_targ_ad_setint {
  lev_type_t type;
  int ad_id;
  int value;	// factor * 1e6
};

struct lev_targ_ad_setctr {
  lev_type_t type;
  int ad_id;
  int clicks;
  long long views;
};

struct lev_targ_ad_setctr_pack {
  lev_type_t type;
  int ad_id;
  int views;
};

struct lev_targ_ad_setsump {
  lev_type_t type;
  int ad_id;
  long long sump0, sump1, sump2;
};

struct views_clicks_ll {
  long long views;
  long long clicks;
};

struct lev_targ_global_click_stats {
  lev_type_t type;
  int len;
  struct views_clicks_ll stats[0];
};


struct lev_targ_user_group_types {
  lev_type_t type;
  int user_id;
  unsigned user_group_types[4];
};


#pragma	pack(pop)

#endif
