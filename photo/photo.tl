---types---
/////
//
// Photo
//
/////

photo.photoLocation server:int server2:int user_id:int orig_album:int photo:string = photo.PhotoLocation;
                   
photo.audioLocation server:int server2:int user_id:int source:int audio:string = photo.AudioLocation;

photo.videoLocation server:int server2:int user_id:int source:int video:string = photo.VideoLocation;


photo.photoLocationStorage rotate:int size:string server:int volume_id:int local_id:int extra:int url_secret:string = photo.PhotoLocation;

photo.audioLocationStorage rotate:int size:string server:int volume_id:int local_id:int extra:int url_secret:string = photo.AudioLocation;

photo.videoLocationStorage rotate:int size:string server:int volume_id:int local_id:int extra:int url_secret:string = photo.VideoLocation;


// (fields_mask & (1 << 29)) == 1 means DELETED photo
// (fields_mask & (1 << 30)) is reserved for fields_mask2

photo.photo#2469893a fields_mask:#
                     id:fields_mask.0?int album_id:fields_mask.1?int owner_id:fields_mask.2?int user_id:fields_mask.3?int height:fields_mask.4?int width:fields_mask.5?int
                     location:fields_mask.6?(%Vector (photo.PhotoLocation)) original_location:fields_mask.7?(%Vector (photo.PhotoLocation)) = photo.Photo;

photo.audio#7ab683e6 fields_mask:#
                     id:fields_mask.0?int album_id:fields_mask.1?int owner_id:fields_mask.2?int uploaded:fields_mask.3?int
                     duration:fields_mask.4?int location:fields_mask.5?(%Vector (photo.AudioLocation)) = photo.Audio;

photo.video#cbb6b387 fields_mask:#
                     id:fields_mask.0?int album_id:fields_mask.1?int owner_id:fields_mask.2?int date:fields_mask.3?int title:fields_mask.4?string description:fields_mask.5?string 
                     location:fields_mask.6?(%Vector (photo.VideoLocation)) flags:fields_mask.7?int = photo.Video;

photo.photoAlbum#691a0984 fields_mask:#
                          id:fields_mask.0?int owner_id:fields_mask.1?int title:fields_mask.2?string description:fields_mask.3?string = photo.PhotoAlbum;

photo.audioAlbum#c5a21f77 fields_mask:#
                          id:fields_mask.0?int owner_id:fields_mask.1?int title:fields_mask.2?string = photo.AudioAlbum;

photo.videoAlbum#7bbe3606 fields_mask:#
                          id:fields_mask.0?int owner_id:fields_mask.1?int title:fields_mask.2?string = photo.VideoAlbum;

---functions---

//changes of owner_id and id will be ignored
//changes of location and original_location will be added to previous values, changes of other fields will replace previous values
photo.changePhoto         owner_id:int photo_id:int changes:%photo.Photo         = Bool;
photo.changePhotoAlbum    owner_id:int album_id:int changes:%photo.PhotoAlbum    = Bool;
photo.changeAudio         owner_id:int photo_id:int changes:%photo.Audio         = Bool;
photo.changeAudioAlbum    owner_id:int album_id:int changes:%photo.AudioAlbum    = Bool;
photo.changeVideo         owner_id:int photo_id:int changes:%photo.Video         = Bool;
photo.changeVideoAlbum    owner_id:int album_id:int changes:%photo.VideoAlbum    = Bool;

photo.incrementPhotoField    owner_id:int photo_id:int field:string cnt:int = Bool;
photo.incrementAudioField    owner_id:int photo_id:int field:string cnt:int = Bool;
photo.incrementVideoField    owner_id:int photo_id:int field:string cnt:int = Bool;
photo.incrementAlbumField    owner_id:int album_id:int field:string cnt:int = Bool;

photo.setVolumeServer volume_id:int server_id:int = BoolStat;

photo.deleteLocationStorage         owner_id:int photo_id:int rotate:int size:string = Bool;
photo.deleteLocation                owner_id:int photo_id:int = Bool;
photo.deleteOriginalLocationStorage owner_id:int photo_id:int rotate:int size:string = Bool;
photo.deleteOriginalLocation        owner_id:int photo_id:int = Bool;

photo.changeLocationServer owner_id:int photo_id:int new_server_id:int = Bool;
photo.changeOriginalLocationServer owner_id:int photo_id:int new_server_id:int = Bool;
photo.changeLocationServer2 owner_id:int photo_id:int new_server_id:int = Bool;
photo.changeOriginalLocationServer2 owner_id:int photo_id:int new_server_id:int = Bool;

photo.savePhotoLocation owner_id:int photo_id:int = Bool;
photo.restorePhotoLocation owner_id:int photo_id:int = Bool;

photo.rotatePhoto owner_id:int photo_id:int dir:int = Bool;

photo.changePhotoOrder owner_id:int photo_id:int id_near:int is_next:Bool = Bool;
photo.changeAudioOrder owner_id:int photo_id:int id_near:int is_next:Bool = Bool;
photo.changeVideoOrder owner_id:int photo_id:int id_near:int is_next:Bool = Bool;
photo.changeAlbumOrder owner_id:int album_id:int id_near:int is_next:Bool = Bool;

photo.newPhotoForce owner_id:int album_id:int photo_id:int = Bool;
photo.newAudioForce owner_id:int album_id:int photo_id:int = Bool;
photo.newVideoForce owner_id:int album_id:int photo_id:int = Bool;
photo.newAlbumForce owner_id:int album_id:int              = Bool;

photo.newPhoto owner_id:int album_id:int = Maybe Int;
photo.newAudio owner_id:int album_id:int = Maybe Int;
photo.newVideo owner_id:int album_id:int = Maybe Int;
photo.newAlbum owner_id:int              = Maybe Int;

photo.getPhotosOverview          owner_id:int album_ids:%(Vector int) fields_mask:# offset:int limit:int is_reverse:Bool = Vector (%photo.Photo);
photo.getPhotosOverviewCount     owner_id:int album_ids:%(Vector int) fields_mask:# offset:int limit:int is_reverse:Bool = VectorTotal (%photo.Photo);

photo.getPhotosCount             owner_id:int album_id:int                                                    condition:string = Maybe Int;
photo.getAudiosCount             owner_id:int album_id:int                                                    condition:string = Maybe Int;
photo.getVideosCount             owner_id:int album_id:int                                                    condition:string = Maybe Int;
photo.getAlbumsCount             owner_id:int                                                                 condition:string = Maybe Int;
                                                        
photo.getPhotos                  owner_id:int album_id:int fields_mask:# offset:int limit:int is_reverse:Bool condition:string = Vector (%photo.Photo);
photo.getPhotosWithCount         owner_id:int album_id:int fields_mask:# offset:int limit:int is_reverse:Bool condition:string = VectorTotal (%photo.Photo);
photo.getAudios                  owner_id:int album_id:int fields_mask:# offset:int limit:int is_reverse:Bool condition:string = Vector (%photo.Audio);
photo.getAudiosWithCount         owner_id:int album_id:int fields_mask:# offset:int limit:int is_reverse:Bool condition:string = VectorTotal (%photo.Audio);
photo.getVideos                  owner_id:int album_id:int fields_mask:# offset:int limit:int is_reverse:Bool condition:string = Vector (%photo.Video);
photo.getVideosWithCount         owner_id:int album_id:int fields_mask:# offset:int limit:int is_reverse:Bool condition:string = VectorTotal (%photo.Video);
                                                        
photo.getPhotoAlbums             owner_id:int              fields_mask:# offset:int limit:int is_reverse:Bool condition:string = Vector (%photo.PhotoAlbum);
photo.getPhotoAlbumsWithCount    owner_id:int              fields_mask:# offset:int limit:int is_reverse:Bool condition:string = VectorTotal (%photo.PhotoAlbum);
photo.getAudioAlbums             owner_id:int              fields_mask:# offset:int limit:int is_reverse:Bool condition:string = Vector (%photo.AudioAlbum);
photo.getAudioAlbumsWithCount    owner_id:int              fields_mask:# offset:int limit:int is_reverse:Bool condition:string = VectorTotal (%photo.AudioAlbum);
photo.getVideoAlbums             owner_id:int              fields_mask:# offset:int limit:int is_reverse:Bool condition:string = Vector (%photo.VideoAlbum);
photo.getVideoAlbumsWithCount    owner_id:int              fields_mask:# offset:int limit:int is_reverse:Bool condition:string = VectorTotal (%photo.VideoAlbum);


// (fields_mask & (1 << 29)) == 1 means also return DELETED photos
photo.getPhoto         owner_id:int photo_id:int fields_mask:# = Maybe (%photo.Photo);
photo.getAudio         owner_id:int photo_id:int fields_mask:# = Maybe (%photo.Audio);
photo.getVideo         owner_id:int photo_id:int fields_mask:# = Maybe (%photo.Video);

photo.getPhotoAlbum    owner_id:int album_id:int fields_mask:# = Maybe (%photo.PhotoAlbum);
photo.getAudioAlbum    owner_id:int album_id:int fields_mask:# = Maybe (%photo.AudioAlbum);
photo.getVideoAlbum    owner_id:int album_id:int fields_mask:# = Maybe (%photo.VideoAlbum);

photo.restorePhoto owner_id:int photo_id:int = Bool;
photo.restoreAudio owner_id:int photo_id:int = Bool;
photo.restoreVideo owner_id:int photo_id:int = Bool;

photo.deletePhoto  owner_id:int photo_id:int = Bool;
photo.deleteAudio  owner_id:int photo_id:int = Bool;
photo.deleteVideo  owner_id:int photo_id:int = Bool;
photo.deleteAlbum  owner_id:int album_id:int = Bool;

