---types---

/////
//
// Statsx
//
/////

// Bits
// 0  : sex
// 1  : age
// 2  : mstatus
// 3  : political_views
// 4  : section
// 5  : city
// 6  : geoip_country
// 7  : country
// 8  : source
// 9  : views
// 10 : visitors
// 11 : sex_age
// 12 : visitors last month
// 13 : visitors last 7 days
// 14 : deletes
// 15 : version
// 16 : expires
// 17 : extra

statsx.intKey id:int count:int = statsx.IntKey;
statsx.strKey id:string count:int = statsx.StrKey;
statsx.month year:int month:int count:int = statsx.Month;

statsx.counter {mode:#} sex:mode.0?(%Tuple int 2) age:mode.1?(%Tuple int 8) marital_status:mode.2?(%Tuple int 8) political_views:mode.3?(%Tuple int 8) section:mode.4?(%Tuple int 16) city:mode.5?(%Vector %statsx.IntKey)
                        geoip_country:mode.6?(%Vector %statsx.StrKey) country:mode.7?(%Vector %statsx.StrKey) source:mode.8?(%Tuple int 16) 
                        views:mode.9?int visitors:mode.10?int sex_age:mode.11?(%Tuple int 16) last_month_unique_visitors:mode.12?int last_week_unique_visitors:mode.13?int deletes:mode.14?int version:mode.15?int expires:mode.16?int
                        extra:mode.17?(%Vector %statsx.IntKey)
                        = statsx.Counter mode;

---functions---

statsx.incr cnt_id:long uid:int mode:# sex:mode.0?int age:mode.1?int marital_status:mode.2?int political_views:mode.3?int section:mode.4?int city:mode.5?int geoip_country:mode.6?string country:mode.7?string source:mode.8?int = Maybe int;
statsx.incrSubcnt cnt_id:long subcnt_id:int = Maybe int;

statsx.decr cnt_id:long uid:int mode:# sex:mode.0?int age:mode.1?int marital_status:mode.2?int political_views:mode.3?int section:mode.4?int city:mode.5?int geoip_country:mode.6?string country:mode.7?string source:mode.8?int = Maybe int;
statsx.decrSubcnt cnt_id:long subcnt_id:int = Maybe int;

statsx.incrVersion cnt_id:long version:int uid:int mode:# sex:mode.0?int age:mode.1?int marital_status:mode.2?int political_views:mode.3?int section:mode.4?int city:mode.5?int geoip_country:mode.6?string country:mode.7?string source:mode.8?int = Maybe int;
statsx.incrSubcntVersion cnt_id:long version:int subcnt_id:int = Maybe int;

statsx.decrVersion cnt_id:long version:int uid:int mode:# sex:mode.0?int age:mode.1?int marital_status:mode.2?int political_views:mode.3?int section:mode.4?int city:mode.5?int geoip_country:mode.6?string country:mode.7?string source:mode.8?int = Maybe int;
statsx.decrSubcntVersion cnt_id:long version:int subcnt_id:int = Maybe int;

statsx.getViews cnt_id:long = Maybe int;
statsx.getViewsVersion cnt_id:long version:int = Maybe int;

statsx.getVisitors cnt_id:long = Maybe int;
statsx.getVisitorsVersion cnt_id:long version:int = Maybe int;

statsx.enableCounter cnt_id:long = Bool;
statsx.disableCounter cnt_id:long = Bool;

statsx.setTimezone cnt_id:long offset:int = Bool;

statsx.getCounter cnt_id:long = Maybe (%statsx.Counter (262143));
statsx.getCounterVersion cnt_id:long version:int = Maybe (%statsx.Counter (262143));

statsx.getCounterMask cnt_id:long mode:# = Maybe (%statsx.Counter mode);
statsx.getCounterMaskVersion cnt_id:long mode:# version:int = Maybe (%statsx.Counter mode);

statsx.getVersions cnt_id:long = Maybe (%Vector int);

statsx.getMonthlyVisitors cnt_id:long = Maybe (%Vector %statsx.Month);
statsx.getMonthlyViews cnt_id:long = Maybe (%Vector %statsx.Month);

statsx.getCountersSum cnt_id:long from:int to:int = Maybe (%statsx.Counter (31 + 256 + 512 + 2048 + 16384));
