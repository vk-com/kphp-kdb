---types---
/////
//
// Memcache
//
/////

memcache.not_found = memcache.Value;
memcache.strvalue value:string flags:int = memcache.Value;
memcache.longvalue value:long flags:int = memcache.Value;

---functions---

memcache.set key:string flags:int delay:int value:string = Bool;
memcache.add key:string flags:int delay:int value:string = Bool;
memcache.replace key:string flags:int delay:int value:string = Bool;
memcache.incr key:string value:long = memcache.Value;
memcache.decr key:string value:long = memcache.Value;
memcache.delete key:string = Bool;
memcache.get key:string = memcache.Value;

memcache.getWildcard prefix:string = Vector %(Map string string);
