---types---
/////
//
// Targ
//
/////


// Bits
//  0 like modificator r
//  1 like modificator i
//  2 reverse
//  17 and/xor mask
//  18 max_users_per_server
//  19 user_list

targ.adViewsCnt ad:int views:int = targ.AdViewsCnt;
targ.adUserViewsCnt user:int views:int = targ.AdUserViewsCnt;

targ.tAdInfo ad_id:long flags:long price:long users:long views:long clicked:long click_money:long unk1:long unk2:long unk3:long unk4:long expected_gain:long ext_users:long unk5:long l_sump0:long l_sump1:long l_sump2:long
  g_sump0:long g_sump1:long g_sump2:long lambda:long delta:long recent_views:long recent_views_limit:long factor:long domain:long group:long category:long subcategory:long = targ.AdInfo;


targ.pricesResult {limit:#} total:int res:%(Tuple int limit) = targ.PricesResult limit;
targ.audienceResult total:int scanned:int top_place:int = targ.AudienceResult;
targ.searchResult {mode:#} uid:int rate:mode.0?int = targ.SearchResult mode;


targ.userAd {flags:#} ad:int expected_gain:flags.0?int price:flags.1?int viewed:flags.2?int = targ.UserAd flags;

---functions---


targ.adEnable ad:int = BoolStat;
targ.adEnablePrice ad:int price:int = BoolStat;
targ.adDisable ad:int = BoolStat;
targ.adSetCtr ad:int clicks:long views:long = BoolStat;
targ.adSetSump ad:int sump0:long sump1:long sump2:long = BoolStat;
targ.adSetCtrSump ad:int clicks:long views:long sump0:long sump1:long sump2:long = BoolStat;
targ.adSetAud ad:int aud:int = BoolStat;
targ.adLimitedViews ad:int max_views:int = BoolStat;
targ.adViewsRateLimit ad:int max_views:int = BoolStat;
targ.adSites ad:int mask:int = BoolStat;
targ.adSetFactor ad:int factor:int = BoolStat;
targ.adSetDomain ad:int domain:int = BoolStat;
targ.adSetCategories ad:int category:int subcategory:int = BoolStat;
targ.adSetGroup ad:int group:int = BoolStat;

targ.adClicks ad:int = Maybe int;
targ.adCtr ad:int = Maybe (%Tuple int 4);
targ.adSump ad:int = Maybe (%Tuple int 9);
targ.adCtrSump ad:int = Maybe (%Tuple int 13);
targ.adMoney ad:int = Maybe int;
targ.adViews ad:int = Maybe int;
targ.adRecentViews ad:int = Maybe int;

targ.recentViewsStats mode:# limit:int = VectorTotal %targ.AdViewsCnt;
targ.recentAdViewers ad:int mode:# limit:int = VectorTotal %targ.AdUserViewsCnt;

targ.adInfo ad:int = Maybe %targ.AdInfo;
targ.adQuery ad:int = Maybe string;



targ.recentUserAds uid:int mode:# limit:int = Vector int;
targ.recentUserAdsCnt uid:int mode:# limit:int = Vector (%targ.AdViewsCnt);


targ.userGroups uid:int = Maybe %(Vector int);
targ.userClick uid:int ad:int price:int = Bool;
targ.userView uid:int ad:int = Bool;
targ.userFlags uid:int = Maybe int;
targ.userClickedAd uid:int ad:int = Maybe int;

targ.deleteGroup gid:int = BoolStat;

targ.target mode:# ad:int cost_per_click:int discount:mode.16?int query:string = Maybe int;
targ.prices mode:# place:int and_mask:mode.17?int xor_mask:mode.17?int limit:# user_list:mode.19?%(Vector int) query:string = targ.PricesResult limit;
targ.adPricing mode:# ad:int place:int and_mask:mode.17?int xor_mask:mode.17?int max_users_per_server:mode.18?int limit:# = targ.PricesResult limit;
//Size of vector is 3K+3
targ.targAudience mode:# place:int cpv:int and_mask:mode.17?int xor_mask:mode.17?int max_users_per_server:mode.18?int user_list:mode.19?%(Vector int) aux_queries:%(Vector string) query:string = Vector %targ.AudienceResult;
targ.audience mode:# user_list:mode.19?%(Vector int) aux_queries:%(Vector string) query:string = Vector int;
targ.search mode:# limit:int user_list:mode.19?%(Vector int) query:string = VectorTotal (%targ.SearchResult mode);

targ.userAds uid:int limit:int flags:# and_mask:flags.17?int or_mask:flags.17?int cat_mask:long = Vector %(targ.UserAd flags);

targ.setSex uid:int value:int = Bool;
targ.setOperator uid:int value:int = Bool;
targ.setBrowser uid:int value:int = Bool;
targ.setRegion uid:int value:int = Bool;
targ.setHeight uid:int value:int = Bool;
targ.setSmoking uid:int value:int = Bool;
targ.setAlcohol uid:int value:int = Bool;
targ.setPpriority uid:int value:int = Bool;
targ.setIiothers uid:int value:int = Bool;
targ.setHidden uid:int value:int = Bool;
targ.setCvisited uid:int value:int = Bool;
targ.setGcountry uid:int value:int = Bool;
targ.setPrivacy uid:int value:int = Bool;
targ.setPolitical uid:int value:int = Bool;
targ.setMstatus uid:int value:int = Bool;
targ.setTimezone uid:int value:int = Bool;
targ.setRate uid:int value:int = Bool;
targ.setCute uid:int value:int = Bool;
targ.setFlags uid:int value:int = Bool;
targ.setUserSingleGroupType uid:int value:int = Bool;
targ.setUserLang uid:int value:int = Bool;

targ.setCustom uid:int type:int value:int = Bool;
targ.setRates uid:int rate:int cute:int = Bool;
targ.setUsername uid:int value:string = Bool;
targ.setUserGroupTypes uid:int types:%(Vector int) = Bool;

targ.setCountryCity uid:int country:int city:int = Bool;
targ.setBirthday uid:int day:int month:int year:int = Bool;
targ.setReligion uid:int value:string = Bool;
targ.setHometown uid:int value:string = Bool;
targ.setProposal uid:int value:string = Bool;
targ.setSchool uid:int country:int city:int school:int start:int finish:int grad:int sch_class:int sch_type:int spec:string = Bool;
targ.setEducation uid:int country:int city:int university:int faculty:int chair:int grad_year:int edu_form:int edu_status:int primary:int = Bool;
targ.setCompany uid:int country:int city:int company:int start:int finish:int company_name:string job:string = Bool;
targ.setMilitary uid:int unit_id:int start:int finish:int = Bool;
targ.setAddress uid:int atype:int country:int city:int district:int station:int street:int name:string house:string = Bool;
targ.setInterest uid:int type:int name:string = Bool;
targ.setUserGroup uid:int group:int = Bool;

targ.deleteUser uid:int = Bool;
targ.deleteEducation uid:int = Bool;
targ.deleteSchools uid:int = Bool;
targ.deleteWork uid:int = Bool;
targ.deleteAddresses uid:int = Bool;
targ.deleteMilitary uid:int = Bool;
targ.deleteGroups uid:int = Bool;
targ.deletePositiveGroups uid:int = Bool;
targ.deleteNegativeGroups uid:int = Bool;
targ.deleteLangs uid:int = Bool;
targ.deleteProposal uid:int = Bool;
targ.deleteInterests uid:int type:int = Bool;
targ.deleteUserGroup uid:int group:int = Bool;
targ.deleteUserLang uid:int lang:int = Bool;

targ.deleteUserGroups uid:int groups:%(Vector int) = Bool;
targ.setUserGroups uid:int groups:%(Vector int) = Bool;
targ.addUserGroups uid:int groups:%(Vector int) = Bool;

targ.userVisit uid:int name:string = Bool;
