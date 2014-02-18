---types---

/////
//
// Cache
//
/////

//  Flag bits:
//    0  return acounter value
//    1  return file size
//    2  return all acounter values
//    3  filter - URL with unknown size
//    4  filter - URL without local copies
//    5  return yellow_enabled
//    6  return yellow_remaining

cache.topAccessRecord {fields_mask:#} url:string value:fields_mask.0?double size:fields_mask.1?long values:fields_mask.2?%(Vector double) =  cache.TopAccessRecord fields_mask;
cache.bottomDiskRecord {fields_mask:#} local_url:string global_url:string value:fields_mask.0?double size:fields_mask.1?long values:fields_mask.2?%(Vector double) yellow_enabled:fields_mask.5?int yellow_remaining:fields_mask.6?int = cache.BottomDiskRecord fields_mask; 
cache.yellowTime remaining:int enabled:int = cache.YellowTime;
cache.localCopy local_url:string node_id:int server_id:int disk_id:int flags:int cached_at:int = cache.LocalCopy;
cache.serverStat node_id:int server_id:int access_percent:double all_files_size:long files:long = cache.ServerStat; 

---functions---
cache.access cache_id:int url:string = Bool;
cache.setFileSize cache_id:int url:string size:long = Bool;
cache.setYellowTimeRemaining cache_id:int global_url:string local_url:string time:int = Bool;
cache.setNewLocalCopy cache_id:int global_url:string local_url:string = Bool;
cache.deleteLocalCopy cache_id:int global_url:string local_url:string = Bool;

cache.disableServer cache_id:int node_id:int server_id:int = Bool; 
cache.enableServer cache_id:int node_id:int server_id:int = Bool; 
cache.disableDisk cache_id:int node_id:int server_id:int disk_id:int = Bool; 
cache.enableDisk cache_id:int node_id:int server_id:int disk_id:int = Bool; 
cache.deleteServer cache_id:int node_id:int server_id:int = Bool; 
cache.deleteDisk cache_id:int node_id:int server_id:int disk_id:int = Bool; 

cache.convert cache_id:int url:string = Maybe string;
cache.getFileSize cache_id:int url:string = Maybe long;
cache.getLocalCopies cache_id:int url:string = Maybe (%Vector %cache.LocalCopy);
cache.getYellowTime cache_id:int global_url:string local_url:string = Maybe (%cache.YellowTime);

cache.getTopAccess cache_id:int t:int limit:int flags:# min_rate:int = Vector (%cache.TopAccessRecord flags);
cache.getBottomAccess cache_id:int t:int limit:int flags:# min_rate:int = Vector (%cache.TopAccessRecord flags);
cache.getBottomDisk cache_id:int t:int limit:int flags:# node_id:int server_id:int disk_id:int = Vector (%cache.BottomDiskRecord flags);
cache.getTopDisk cache_id:int t:int limit:int flags:# node_id:int server_id:int disk_id:int = Vector (%cache.BottomDiskRecord flags);

cache.getAcounter cache_id:int url:string t:int = Maybe double;
cache.getServerStats cache_id:int sorting_flags:int = Vector (%cache.ServerStat);
