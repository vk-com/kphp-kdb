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

#ifndef __TARG_INDEX_LAYOUT_H__
#define __TARG_INDEX_LAYOUT_H__

#include "targ-trees.h"

#define TARG_INDEX_MAGIC	0x11ef0bca
#define TARG_INDEX_MAGIC_V2	0x11ef0b03

#pragma	pack(push,1)

struct targ_index_header {
  int magic;
  int created_at;
  long long log_pos;
  int log_timestamp;
  unsigned log_pos_crc32;
  int log_split_min, log_split_max, log_split_mod;
  int tot_users;
  int max_uid;
  int words;
  int ads;
  int stemmer_version;
  int word_split_version;
  int listcomp_version;
  long long stats_data_offset;
  long long word_directory_offset;
  long long user_data_offset;
  long long word_data_offset;
  long long fresh_ads_directory_offset;
  long long stale_ads_directory_offset;
  long long fresh_ads_data_offset;
  long long stale_ads_data_offset;
  long long data_end;
  long long tot_clicks, tot_views, tot_click_money;
  long long recent_views_data_offset;  // if non-zero, points between stats_data_offset and word_directory_offset
  int reserved[21];
  unsigned header_crc32;
};

#define INTERPOLATIVE_CODE_JUMP_SIZE	32

struct targ_index_word_directory_entry {
  hash_t word;
  int data_offset;  // offset from Header.word_data_offset
};

/* add new entries ONLY to the end of this list, before q_max */
enum query_type {
 q_none,
 q_not,
 q_and,
 q_or,
 q_true,
 q_false,
 q_spec,
 q_random,
 q_id,
 q_name,
 q_country,
 q_city,
 q_bday,
 q_bmonth,
 q_byear,
 q_political,
 q_sex,
 q_operator,
 q_browser,
 q_region,
 q_height,
 q_smoking,
 q_alcohol,
 q_ppriority,
 q_iiothers,
 q_hidden,
 q_cvisited,
 q_timezone,
 q_mstatus,
 q_interests,
 q_name_interests,
 q_religion,
 q_hometown,
 q_proposal,
 q_online,
 q_has_photo,
 q_privacy,
 q_birthday_today,
 q_birthday_tomorrow,
 q_birthday_soon,
 q_education,
 q_uni_country,
 q_uni_city,
 q_univ,
 q_faculty,
 q_chair,
 q_graduation,
 q_edu_form,
 q_edu_status,
 q_school,
 q_sch_country,
 q_sch_city,
 q_sch_id,
 q_sch_grad,
 q_sch_class,
 q_sch_spec,
 q_address,
 q_adr_country,
 q_adr_city,
 q_adr_district,
 q_adr_station,
 q_adr_street,
 q_adr_type,
 q_adr_house,
 q_adr_name,
 q_group,
 q_grp_type,
 q_grp_id,
 q_lang,
 q_lang_id,
 q_company,
 q_company_name,
 q_job,
 q_military,
 q_mil_unit,
 q_mil_start,
 q_mil_finish,
 q_uses_apps,
 q_pays_money,
 q_age,
 q_future_age,
 q_inlist,
 q_gcountry,
 q_custom1,
 q_custom2,
 q_custom3,
 q_custom4,
 q_custom5,
 q_custom6,
 q_custom7,
 q_custom8,
 q_custom9,
 q_custom10,
 q_custom11,
 q_custom12,
 q_custom13,
 q_custom14,
 q_custom15,
 q_max
};

#define TARG_INDEX_USER_STRUCT_V1_MAGIC	0x3acada11
#define	TARG_INDEX_USER_DATA_END	0x11300cad

struct targ_index_user_v1 {
  int user_struct_magic;
  int user_id;
  int user_active_ads_num, user_inactive_ads_num, user_clicked_ads_num;
  int rate;
  int last_visited;
  int uni_city;
  int region;
  unsigned user_group_types[4];
  char bday_day;
  char bday_month;
  short bday_year;
  char political;
  char sex;
  char mstatus;
  unsigned char uni_country;
  char cute;
  char privacy;
  char has_photo;
  unsigned char browser;
  unsigned short operator;
  short timezone;                           // timezone offset, Moscow-time based
  unsigned char height, smoking, alcohol, ppriority, iiothers, cvisited;
  char hidden;
  unsigned char gcountry;                   // was custom_fields[0]
  union {
    unsigned char custom_fields[15];
    struct {
      unsigned char custom1;
      unsigned char custom2;
      unsigned char custom3;
      unsigned char custom4;
      unsigned char custom5;
      unsigned char custom6;
      unsigned char custom7;
      unsigned char custom8;
      unsigned char custom9;
      unsigned char custom10;
      unsigned char custom11;
      unsigned char custom12;
      unsigned char custom13;
      unsigned char custom14;
      unsigned char custom15;
    };
  };
  unsigned short edu_num, sch_num, work_num, addr_num, inter_num, mil_num, grp_num, lang_num;
  char var_fields[0];
  //  hash_list_t *name_hashes;
  //  hash_list_t *inter_hashes;
  //  hash_list_t *religion_hashes;
  //  hash_list_t *hometown_hashes;
  //  hash_list_t *proposal_hashes;
  //  char *name;
  //  char *religion;
  //  char *hometown;
  //  char *proposal;
  //  struct education *edu;
  //  struct school *sch;
  //  struct company *work;
  //  struct address *addr;
  //  struct interest *inter;
  //  align 4
  //  struct military *mil;
  //  struct user_groups *grp;
  //  struct user_langs *langs;
  //  align 4
  //  active_ads: (int ad_id, int view_num)*user_active_ads_num;
  //  inactive_ads: (int ad_id, int view_num)*user_inactive_ads_num;
  //  clicked_ads: (int ad_id, int view_num)*user_clicked_ads_num;
};

// STRINGs are serialised as null-terminated strings
// HASH_LISTS are serialised as targ_index_hash_list

struct targ_index_hash_list {
  int hash_list_size;
  hash_t words[0];
};

#define MAX_AD_VIEWS	1024

struct targ_index_view_stats {
  struct {
    long long views;
    long long clicks;
  } l[MAX_AD_VIEWS], g[MAX_AD_VIEWS];
};

struct targ_index_education {
 int grad_year;
 int chair;
 int faculty;
 int university;
 int city;
 unsigned char country;
 char edu_form;
 char edu_status;
 char primary;
};

struct targ_index_school {
  int city;
  int school;
  short start;
  short finish;
  short grad;
  unsigned char country;
  char sch_class;
  char sch_type;
  char sch_var[0];  // HASHLIST spec_hashes, STRING spec
};

struct targ_index_company {
  int city;
  int company;
  short start;
  short finish;
  unsigned char country;
  char company_var[0];	// HASHLIST name_hashes, HASHLIST job_hashes, STRING company_name, STRING job
};

struct targ_index_address {
  int city;
  int district;
  int station;
  int street;
  unsigned char country;
  unsigned char atype;
  char address_var[0];  // HASHLIST house_hashes, HASHLIST name_hashes, STRING house, STRING name
};

struct targ_index_interest {
  unsigned char type;
  unsigned char flags;
  char text[0];		// STRING text
};

struct targ_index_military {
  int unit_id;
  short start;
  short finish;
};

struct targ_index_user_groups {
  int G[0];
};

struct targ_index_user_langs {
  short L[0];
};

struct targ_index_ads_directory_entry {
  int ad_id;
  long long ad_info_offset;
};

/* must be monotonic: V1 < V2 < V3 < ... */
#define TARG_INDEX_AD_STRUCT_MAGIC_V1 0xAD1DACEF
#define TARG_INDEX_AD_STRUCT_MAGIC_V2 0xADAD1CFE
#define TARG_INDEX_AD_STRUCT_MAGIC_V3 0xADADCFEA
#define TARG_INDEX_AD_STRUCT_MAGIC_V4 0xADADDADA

struct targ_index_advert_v1 {
  int ad_struct_magic;
  int ad_id;
  int flags;
  int price;
  int retarget_time;
  int disabled_since;
  int userlist_computed_at;
  int users;
  int ext_users;
  int l_clicked;
  long long click_money;
  int views;
  int l_clicked_old, l_views;
  long long g_clicked, g_views;
  char query[0];
};

struct targ_index_advert_v2 {
  int ad_struct_magic;
  int ad_id;
  int flags;
  int price;
  int retarget_time;
  int disabled_since;
  int userlist_computed_at;
  int users;
  int ext_users;
  int l_clicked;
  long long click_money;
  int views;
  int l_clicked_old, l_views;
  int g_clicked_old, g_clicked;
  long long g_views;
  long long l_sump0, l_sump1, l_sump2;  // fixed point, scaled by 2^32
  long long g_sump0, g_sump1, g_sump2;  // fixed point, scaled by 2^32
  char query[0];
};

struct targ_index_advert_v3 {
  int ad_struct_magic;
  int ad_id;
  int flags;
  int price;
  int retarget_time;
  int disabled_since;
  int userlist_computed_at;
  int users;
  int ext_users;
  int l_clicked;
  long long click_money;
  int views;
  int l_clicked_old, l_views;
  int g_clicked_old, g_clicked;
  long long g_views;
  long long l_sump0, l_sump1, l_sump2;  // fixed point, scaled by 2^32
  long long g_sump0, g_sump1, g_sump2;  // fixed point, scaled by 2^32
  int factor;  // fixed point, scaled by 10^6
  int recent_views_limit; // 0xffff -> infty
  char query[0];
};

struct targ_index_advert_v4 {
  int ad_struct_magic;
  int ad_id;
  int flags;
  int price;
  int retarget_time;
  int disabled_since;
  int userlist_computed_at;
  int users;
  int ext_users;
  int l_clicked;
  long long click_money;
  int views;
  int l_clicked_old, l_views;
  int g_clicked_old, g_clicked;
  long long g_views;
  long long l_sump0, l_sump1, l_sump2;  // fixed point, scaled by 2^32
  long long g_sump0, g_sump1, g_sump2;  // fixed point, scaled by 2^32
  int factor;  // fixed point, scaled by 10^6
  int recent_views_limit; // 0xffff -> infty
  // new in v4
  int domain;
  short category, subcategory;
  int group;
  // end v4
  char query[0];
};

// for fresh ads (follows struct targ_index_advert aligned by 4)
struct targ_index_ad_userlist {
  int uids[0];
};

// for ancient ads (follows struct targ_index_advert aligned by 4)
struct targ_index_ad_userviewlist {
  struct {
    int uid;
    int views_cnt;
  } user_views[0];
};

// for recent_views_data section (if present)
struct cyclic_views_entry {
  int ad_id;
  int user_id;
  int time;
  int reserved;
};



#pragma pack(pop)

#endif
