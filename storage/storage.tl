---types---

/////
//
// Storage
//
/////

storage.volumes volumes:%(Vector long) = storage.Volumes;
storage.fileLocation volume_id:long local_id:int secret:long = storage.FileLocation;
storage.fileUnknown = storage.FileType;
storage.fileJpeg = storage.FileType;
storage.fileGif = storage.FileType;
storage.filePng = storage.FileType;
storage.filePdf = storage.FileType;
storage.fileMp3 = storage.FileType;
storage.fileMov = storage.FileType;
storage.filePartial = storage.FileType;
storage.fileMp4 = storage.FileType;
storage.fileWebp = storage.FileType;
storage.fileContent volume_id:long local_id:int file_type:storage.FileType mtime:int content:string = storage.FileContent;
storage.fileForwardedInfo volume_id:long local_id:int file_type:storage.FileType mtime:int = storage.FileContent;
storage.globalPid ip:int port:int pid:int start_time:int = storage.GlobalPid;

---functions---

storage.getVolumes = storage.Volumes;
storage.checkFile volume_id:long file_size:long max_disk_used_space_percent:double = Bool;
storage.uploadFile volume_id:long file_data:string = storage.FileLocation;
storage.uploadFileExt volume_id:long file_type:storage.FileType file_data:string = storage.FileLocation;
storage.getFile location:storage.FileLocation = storage.FileContent;
storage.getFileIndirect location:storage.FileLocation fwd:%storage.GlobalPid fwd_header:%(Vector int) = storage.FileContent;
storage.getPart location:storage.FileLocation offset:int limit:int = storage.FileContent;
storage.getPartIndirect location:storage.FileLocation offset:int limit:int fwd:%storage.GlobalPid fwd_header:%(Vector int) = storage.FileContent;
