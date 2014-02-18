boolFalse = Bool;
boolTrue = Bool;
Vector<long>;

//storage
volumes volumes:Vector long = Volumes;
file_location volume_id:long local_id:int secret:long = FileLocation;
file_JPEG = FileType;
file_GIF = FileType;
file_PNG = FileType;
file_PDF = FileType;
file_content volume_id:long local_id:int file_type:FileType mtime:int content:string = FileContent;
file_forwarded_info volume_id:long local_id:int file_type:FileType mtime:int = FileContent;
globalPid ip:int port:int pid:int start_time:int = GlobalPid;

//memcache
memcache.not_found = memcache.Value;
memcache.strvalue value:string flags:int = memcache.Value;
memcache.longvalue value:long flags:int = memcache.Value;

---functions---

//storage
get_volumes = Volumes;
upload_file volume_id:long file_data:string = FileLocation;
get_file location:FileLocation = FileContent;
get_file_indirect location:FileLocation fwd:globalPid fwd_header:vector int = FileContent;

//memcache
memcache.set key:string flags:int delay:int value:string = Bool;
memcache.add key:string flags:int delay:int value:string = Bool;
memcache.replace key:string flags:int delay:int value:string = Bool;
memcache.incr key:string value:long = memcache.Value;
memcache.decr key:string value:long = memcache.Value;
memcache.delete key:string = Bool;
memcache.get key:string = memcache.Value;
