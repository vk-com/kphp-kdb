---types---
///////
//
// Friends
//
///////

friends.friend {mode:#} id:int mask:mode.0?int time:mode.1?int = friends.Friend mode;

---functions---

friends.deleteUser uid:int = Bool;

friends.getFriendsId uid:int mask:int = Maybe %(Vector int);
friends.getFriends uid:int mask:int mode:# = Maybe %(Vector %(friends.Friend mode));

friends.getFriendsCnt uid:int mask:int = Maybe int;

friends.getRecentFriends uid:int num:int = Maybe %(VectorTotal %(friends.Friend 3));

friends.setCatList uid:int cat:int friends:%(Vector int) = Bool;
friends.deleteCat uid:int cat:int = Bool;

friends.getFriend uid:int id:int = Maybe int;
friends.setFriend uid:int friend:int mask:int = Bool;
friends.replaceFriend uid:int friend:int mask:int = Bool;
friends.deleteFriend uid:int friend:int = Bool;
friends.incrFriend uid:int friend:int mask:int = Maybe int;
friends.decrFriend uid:int friend:int mask:int = Maybe int;


friends.getFriendReq uid:int id:int = Maybe int;
friends.addFriendReq uid:int friend:int mask:int = Bool;
friends.setFriendReq uid:int friend:int mask:int = Bool;
friends.replaceFriendReq uid:int friend:int mask:int = Bool;
friends.deleteFriendReq uid:int friend:int = Bool;
//friends.incrFriendReq uid:int mask:int = Bool;
//friends.decrFriendReq uid:int mask:int = Bool;

friends.deleteReqs uid:int = Bool;
friends.getReqs uid:int limit:int = Maybe %(VectorTotal %(friends.Friend 3));

friends.setPrivacy uid:int key:string text:string = Bool;
friends.replacePrivacy uid:int key:string text:string = Bool;
friends.getPrivacy uid:int key:string = Maybe string;
friends.deletePrivacy uid:int key:string = Bool;

friends.checkPrivacy uid:int test_uid:int key:string = Maybe int;
friends.checkPrivacyList uid:int test_uid:int key_lest:%(Vector string) = Vector (Maybe int);

friends.getCommonFriendsNum uid:int users:%(Vector int) = Vector int;
friends.getCommonFriends uid:int uid2:int = Vector Int;

