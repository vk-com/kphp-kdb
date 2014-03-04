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

#ifndef __TARG_DATA_H__
#define __TARG_DATA_H__

#include "kdb-data-common.h"
#include "kdb-targ-binlog.h"
#include "kfs.h"
#include "net-aio.h"
#include "targ-trees.h"
#include "targ-index-layout.h"
#include "targ-weights.h"

#define DETERMINISTIC_RANDOM	1

#define	MAX_USERS	(1 << 19)

#define MAX_ONLINE_DELAY	60

#define	MAX_QUERY_STRING_LEN	1023

#define MAX_LANGS	1025

#define MAX_AD_AUD	1000000000
#define MAX_AD_DOMAIN	2000000000
#define MAX_AD_CATEGORY	1023

#define MAX_G_CLICKS	2000000000
#define ESTIMATED_GAIN_EPS	0.01

#define GRANTED_CLICKS	1.0		// number of clicks after which personal ad stats have same weight as "global presets"
#define	INITIAL_LAMBDA	0.25		// estimated lambda ("efficiency") of a completely new ad
#define	INITIAL_INV_D	(GRANTED_CLICKS / (INITIAL_LAMBDA * INITIAL_LAMBDA))

#define CTR_GAIN_PRICE_MULTIPLIER	(1.0 / 0.0002)

#define	MONEY_SCALE		1000LL
#define	EXPECTED_GAIN_USERADS_MULTIPLIER	1e6

#define VIEW_GAIN_MULTIPLIER	1.0

#define	USERS_BITMAP_LONGS	((MAX_USERS+31)>>5)

#define	UNKNOWN	-1
#define	UNKNOWN_COUNTRY	0xff

#define LOCAL_TIMEZONE_SHIFT_MINUTES 240


#define	AD_TABLE_SIZE	(1 << 20)
#define MAX_ADS		(1 << 25)
// #define	ADS_PRIME	4000037

#define AD_ANCIENT_DELAY	(86400*3)
#define	AD_RECOMPUTE_INTERVAL	(86400*30)

#define	GSORT_HEAP_SIZE	65536	//should be <= 65536 and a power of two, short is used to save values

#define QUERY_STATS_PERIODICITY	8192

typedef int user_bitmap_t[USERS_BITMAP_LONGS];

typedef unsigned bitmap128_t[4];

extern int targeting_disabled, max_retarget_ad_id;
extern int tot_users, max_uid, tot_groups, tot_langs;
extern int max_group_id, min_group_id, max_lang_id, min_lang_id;
extern long long user_group_pairs, user_lang_pairs;

extern int user_creations;

extern int tot_userlists;
extern long long tot_userlists_size;

extern int ancient_ads_pending, ancient_ads_loading, ancient_ads_loaded, ancient_ads_aio_loaded;
extern long long ancient_ads_loaded_bytes, ancient_ads_aio_loaded_bytes;

extern int use_aio;
extern int delay_targeting;

extern int index_mode;

extern struct targ_index_view_stats AdStats;

typedef struct user user_t;

#define	USER_DEL	1

#define	MAX_POLITICAL	10
#define	MAX_SEX		2
#define MAX_REGION	10000000
#define MAX_HEIGHT	255
#define MAX_SMOKING	5
#define MAX_ALCOHOL	5
#define MAX_PPRIORITY	10
#define MAX_IIOTHERS	10
#define MAX_OPERATOR	10000
#define MAX_BROWSER	250
#define	MAX_MSTATUS	7
#define	MAX_PRIVACY	10
#define MAX_CVISITED	250
#define MAX_GCOUNTRY	255
#define MAX_CUSTOM1	255
#define MAX_CUSTOM2	255
#define MAX_CUSTOM3	255
#define MAX_CUSTOM4	255
#define MAX_CUSTOM5	255
#define MAX_CUSTOM6	255
#define MAX_CUSTOM7	255
#define MAX_CUSTOM8	255
#define MAX_CUSTOM9	255
#define MAX_CUSTOM10	255
#define MAX_CUSTOM11	255
#define MAX_CUSTOM12	255
#define MAX_CUSTOM13	255
#define MAX_CUSTOM14	255
#define MAX_CUSTOM15	255
#define	MAX_EDU_FORM	3
#define	MAX_EDU_STATUS	10
#define MAX_TIMEZONE    1439
#define MAX_HIDDEN      3
#define MAX_BIRTHDAY_SOON	7


#define MAX_HAS_PHOTO 255

/* word hash/tree */

extern int alloc_tree_nodes, free_tree_nodes, hash_word_nodes;

#define	HASH_BUCKETS	(1 << 20)
#define OLIST_COUNT	(1 << 11)

typedef struct tree tree_t;
typedef struct utree utree_t;
typedef struct olist olist_t;

struct hash_word {
  hash_t word;
  struct hash_word *next;
  int num, sum;
  treeref_t word_tree;
};

extern struct hash_word *Hash[HASH_BUCKETS];

struct hash_word *get_hash_node (hash_t word, int force);

static inline hash_t field_value_hash (int field_id, int value) {
  return (((hash_t) field_id << 32) | (unsigned) value);
}

int get_word_count_nomult (hash_t word);
int get_word_count_upperbound (hash_t word, int *is_exact);

struct utree {
  utree_t *left, *right;
  int x, y;
  utree_t *uplink;
};


struct olist {
  olist_t *next, *prev;
  int uid, last_visited;
};


struct olist_head {
  olist_t *first, *last;
};

/* user data */

struct user {
  utree_t *left_rate, *right_rate;
  int rate, cartesian_y;
  utree_t *uplink_rate;

  olist_t *online_next, *online_prev;
  int uid, last_visited;

  int user_id;
  treeref_t active_ads, inactive_ads, clicked_ads;
  unsigned user_group_types[4];
  char *name;
  char *religion;
  char *hometown;
  char *proposal;
  struct education *edu;
  struct school *sch;
  struct company *work;
  struct address *addr;
  struct interest *inter;
  struct military *mil;
  struct user_groups *grp;
  struct user_langs *langs;
  targ_weights_vector_t *weights;
  hash_list_t *name_hashes;
  hash_list_t *inter_hashes;
  hash_list_t *religion_hashes;
  hash_list_t *hometown_hashes;
  hash_list_t *proposal_hashes;
  int flags;
  int prev_user_creations;
  int uni_city;
  int region;
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
  unsigned char gcountry; //was: custom_field 0
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
};

struct education {
  struct education *next;
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

struct school {
  struct school *next;
  int city;
  int school;
  char *spec;
  hash_list_t *spec_hashes;
  short start;
  short finish;
  short grad;
  unsigned char country;
  char sch_class;
  char sch_type;
};

struct company {
  struct company *next;
  int city;
  int company;
  char *company_name;
  char *job;
  hash_list_t *name_hashes;
  hash_list_t *job_hashes;
  short start;
  short finish;
  unsigned char country;
};

struct address {
  struct address *next;
  int city;
  int district;
  int station;
  int street;
  char *house;
  char *name;
  hash_list_t *house_hashes;
  hash_list_t *name_hashes;
  unsigned char country;
  unsigned char atype;
};

struct interest {
  struct interest *next;
  unsigned char type;
  unsigned char flags;
  char text[0];
};

struct military {
  struct military *next;
  int unit_id;
  short start;
  short finish;
};

#define MIN_USER_GROUPS	4
#define MAX_USER_GROUPS	4096

struct user_groups {
  int tot_groups;
  int cur_groups;
  int G[0];
};

#define MIN_USER_LANGS	4
#define MAX_USER_LANGS	4096

struct user_langs {
  int tot_langs;
  int cur_langs;
  short L[0];
};

#ifdef _LP64
#define	DEFAULT_TREESPACE_INTS	((1U << 30) - (1U << 16))
//#define	DEFAULT_TREESPACE_INTS	((1U << 29) + (1U << 28) + (1U << 27))
#define DEFAULT_WORDSPACE_INTS	(1U << 27)
#else
#define	DEFAULT_TREESPACE_INTS	(1U << 28)
#define DEFAULT_WORDSPACE_INTS	(1U << 27)
#endif

extern user_t *User[MAX_USERS];
extern int UserRate[MAX_USERS];

extern utree_t *rate_tree;	// key = rating, from lowest to highest


treespace_t AdSpace, WordSpace;

/* ad data */

#define ADF_ON	1
#define ADF_ANCIENT	2
#define ADF_DELAYED	4
#define ADF_PERIODIC	8
#define ADF_OLDVIEWS	64
#define ADF_NEWVIEWS	128
#define ADF_ANYVIEWS	(ADF_OLDVIEWS | ADF_NEWVIEWS)
#define	ADF_NEWANCIENT	512	// used while indexing only
#define ADF_NOTLOADED	1024
#define ADF_LOADING	2048
#define ADF_LIMIT_VIEWS	65536
#define ADF_SITES_MASK_SHIFT	17
#define ADF_ALLOW_SITES	(1 << ADF_SITES_MASK_SHIFT)	// 131072
#define ADF_SITES_MASK	(0xff << ADF_SITES_MASK_SHIFT)

#define ADF_ALLOWED_INDEX_FLAGS	(ADF_ON | ADF_ANCIENT | ADF_PERIODIC | ADF_ANYVIEWS | ADF_LIMIT_VIEWS | ADF_SITES_MASK)

struct advert_head {
  struct advert *first, *last;
  int ad_id;
};

struct advert {
  struct advert *next, *prev;
  int ad_id;
  int disabled_since;
  struct advert *hash_next;
  int retarget_time;
  int userlist_computed_at;
  int flags;
  int price;
  int users;	// size of user_list
  int ext_users;
  struct core_metafile *mf;
  double factor;   // default 1.0
  double expected_gain;
  double lambda, delta;
  double l_sump0, l_sump1, l_sump2;
  double g_sump0, g_sump1, g_sump2;
  long long click_money;
  int l_clicked;
  int views;
  int pending_views;
  int prev_user_creations;
  int l_clicked_old, l_views;
  int g_clicked_old, g_clicked;
  long long g_views;
  char *query;
  int *user_list;	// present for non-ancient ads
  int recent_views;
  int recent_views_limit;  // 0xffff by default
  int domain;
  short category, subcategory;
  int group;
};

#define likely(x) __builtin_expect((x),1) 
#define unlikely(x) __builtin_expect((x),0)

extern struct advert *Ads[AD_TABLE_SIZE];
extern unsigned char AncientAdBitmap[MAX_ADS/8 + 1];

static inline int ad_is_ancient (int ad_id) {
  return (unsigned) ad_id < MAX_ADS ? (AncientAdBitmap[ad_id >> 3] >> (ad_id & 7)) & 1 : 0;
}

static inline int ad_became_ancient (struct advert *A) {
  return !(A->flags & ADF_ON) && A->disabled_since <= log_last_ts - AD_ANCIENT_DELAY;
}

static inline struct advert *get_ad (int ad_id) {
  struct advert *A;
  for (A = Ads[ad_id & (AD_TABLE_SIZE - 1)]; A && A->ad_id != ad_id; A = A->hash_next);
  return A;
}

struct advert *get_ad_f (int ad_id, int force);
double compute_estimated_gain (struct advert *A);

#define	CYCLIC_VIEWS_BUFFER_SIZE	(1 << 18)
#define VIEWS_STATS_INTERVAL		300

extern struct cyclic_views_entry CViews[CYCLIC_VIEWS_BUFFER_SIZE], *CV_r, *CV_w; 

void forget_old_views (void);
int get_recent_views_num (void);

#define	INIT_L_CLICKS	0.5
/* INIT_L_VIEWS must be int */
#define	INIT_L_VIEWS	2000
#define	INIT_L_CTR	((double) INIT_L_CLICKS / INIT_L_VIEWS)
#define	MIN_GAIN	(1e-4)

char *exact_strdup (const char *src, int len);

user_t *get_user (int user_id);
user_t *get_user_f (int user_id);

void rate_change (user_t *U, int new_rate);

extern int ocur_now;
void user_online_tree_insert (user_t *U);
void relax_online_tree (void);
int online_interval_unpack (int *A, int bt, int et);

int init_targ_data (int schema);

int do_set_user_group_types (int user_id, unsigned user_group_types[4]);
int do_set_user_single_group_type (int user_id, unsigned type);
int do_set_rate (int user_id, int rate);
int do_set_cute (int user_id, int cute);
int do_set_rate_cute (int user_id, int rate, int cute);
int do_set_has_photo (int user_id, int has_photo);
int get_has_photo (int user_id);
int do_set_username (int user_id, const char *text, int len);
int do_set_sex (int user_id, int sex);
int do_set_operator (int user_id, int oper);
int do_set_browser (int user_id, int browser);
int do_set_region (int user_id, int region);
int do_set_height (int user_id, int height);
int do_set_smoking (int user_id, int smoking);
int do_set_alcohol (int user_id, int alcohol);
int do_set_ppriority (int user_id, int ppriority);
int do_set_iiothers (int user_id, int iiothers);
int do_set_hidden (int user_id, int hidden);
int do_set_cvisited (int user_id, int cvisited);
int do_set_gcountry (int user_id, int gcountry);
int do_set_custom1 (int user_id, int custom1);
int do_set_custom2 (int user_id, int custom2);
int do_set_custom3 (int user_id, int custom3);
int do_set_custom4 (int user_id, int custom4);
int do_set_custom5 (int user_id, int custom5);
int do_set_custom6 (int user_id, int custom6);
int do_set_custom7 (int user_id, int custom7);
int do_set_custom8 (int user_id, int custom8);
int do_set_custom9 (int user_id, int custom9);
int do_set_custom10 (int user_id, int custom10);
int do_set_custom11 (int user_id, int custom11);
int do_set_custom12 (int user_id, int custom12);
int do_set_custom13 (int user_id, int custom13);
int do_set_custom14 (int user_id, int custom14);
int do_set_custom15 (int user_id, int custom15);
int do_set_timezone (int user_id, int timezone);
int do_set_country_city (int user_id, int country, int city);
int do_set_birthday (int user_id, int day, int month, int year);
int do_set_privacy (int user_id, int privacy);
int do_set_political (int user_id, int political);
int do_set_mstatus (int user_id, int status);
int do_set_religion (int user_id, const char *text, int len);
int do_set_hometown (int user_id, const char *text, int len);
int do_set_proposal (int user_id, const char *text, int len);

int do_set_school (int user_id, struct school *S);
int do_set_education (int user_id, struct education *E);
int do_set_work (int user_id, struct company *C);
int do_set_military (int user_id, int unit_id, int start, int finish);
int do_set_address (int user_id, struct address *A);
int do_set_interest (int user_id, const char *text, int len, int type);
int do_set_user_group (int user_id, int group_id);
int do_set_user_lang (int user_id, int lang_id);

int do_delete_user (int user_id);
int do_delete_education (int user_id);
int do_delete_addresses (int user_id);
int do_delete_work (int user_id);
int do_delete_schools (int user_id);
int do_delete_military (int user_id);
int do_delete_langs (int user_id);
int do_delete_interests (int user_id, int type);
int do_delete_user_group (int user_id, int group_id);
int do_delete_user_lang (int user_id, int lang_id);
int do_delete_proposal (int user_id);

int do_delete_groups (int user_id);
int do_delete_positive_groups (int user_id);
int do_delete_negative_groups (int user_id);

int do_set_user_groups (int user_id, int List[], int len);
int do_add_user_groups (int user_id, int List[], int len);
int do_del_user_groups (int user_id, int List[], int len);

int do_perform_targeting (int ad_id, int price, int factor, char *query);
int perform_pricing (int position, int flags, int and_mask, int xor_mask);
int perform_ad_pricing (int ad_id, int position, int flags, int and_mask, int xor_mask, int max_users);
int user_cpv_is_enough (int uid, int ad_position, int ad_cpv, int and_mask, int xor_mask);

int perform_delayed_ad_activation (void);

int compute_user_ads (int user_id, int limit, int flags, int and_mask, int xor_mask, long long cat_mask);

int do_ad_price_enable (int ad_id, int price);
int do_ad_disable (int ad_id);
int compute_ad_clicks (int ad_id);
int compute_ad_money (int ad_id);
int compute_ad_views (int ad_id);
int compute_ad_recent_views (int ad_id);
int compute_recent_views_stats (void);
int compute_recent_ad_viewers (int ad_id);
int compute_recent_user_ads (int user_id);
int compute_ad_info (int ad_id, long long *res);
int compute_ad_user_clicked (int user_id, int ad_id);
char *get_ad_query (int ad_id);

int user_ad_price (int uid, int position);

int compute_ad_ctr (int ad_id, long long *res);
int do_set_ad_ctr (int ad_id, long long g_clicks, long long g_views);
int do_set_ad_sump (int ad_id, long long g_sump0, long long g_sump1, long long g_sump2);
int do_set_ad_ctr_sump (int ad_id, long long g_clicks, long long g_views, long long g_sump0, long long g_sump1, long long g_sump2);
int do_set_ad_aud (int ad_id, int aud);

int do_register_user_click (int user_id, int ad_id, int price);
int do_register_user_view (int user_id, int ad_id);

int do_user_visit (int user_id, const char *addr, int len);

int do_set_global_click_stats (int len, struct views_clicks_ll *A);

int do_ad_limit_user_views (int ad_id, int views);
int do_ad_change_sites (int ad_id, int ext_sites);
int do_ad_limit_recent_views (int ad_id, int views);
int do_ad_set_factor (int ad_id, int factor);
int do_ad_set_domain (int ad_id, int domain);
int do_ad_set_categories (int ad_id, int category, int subcategory);
int do_ad_set_group (int ad_id, int group);

int get_user_rate (int user_id);
int get_user_groups (int user_id);

int retarget_dynamic_ads (void);
int retarget_all_dynamic_ads (void);
int process_lru_ads (void);

void reinsert_retarget_ad_last (struct advert *A);
void reinsert_lru_ad_last (struct advert *A);

int conv_user_id (int user_id);

extern int R_cnt, R_tot, R_position, R[MAX_USERS*2+2];

extern int debug_user_id;
extern long long collected_garbage_bytes[2], collected_garbage_facts[2];

extern int do_delete_group (int group_id);

extern int active_ad_nodes, inactive_ad_nodes, clicked_ad_nodes;

extern int tot_ads, tot_ad_versions, active_ads;
extern long long tot_clicks, tot_click_money, tot_views;

extern long long total_sump0;
extern double total_sump1, total_sump2;

#endif
