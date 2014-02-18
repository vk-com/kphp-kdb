---types---

/////
//
// News
//
/////

//news  - Newsfeed
//nnews - Notification news
//cnews - Comment news
//rnews - Recommendations news

news.typesGroup {flags:#} {size:#} int:flags.0?int vector:flags.1?(%Tuple int size) = news.TypesGroup flags size;
news.userIdsGroup {flags:#} {size:#} int:flags.2?int vector:flags.3?(%Tuple int size) = news.UserIdsGroup flags size;
news.datesGroup {flags:#} {size:#} int:flags.4?int vector:flags.5?(%Tuple int size) = news.DatesGroup flags size;
news.tagsGroup {flags:#} {size:#} int:flags.6?int vector:flags.7?(%Tuple int size) = news.TagsGroup flags size;
news.usersGroup {flags:#} {size:#} int:flags.8?int vector:flags.9?(%Tuple int size) = news.UsersGroup flags size;
news.groupsGroup {flags:#} {size:#} int:flags.10?int vector:flags.11?(%Tuple int size) = news.GroupsGroup flags size;
news.ownersGroup {flags:#} {size:#} int:flags.12?int vector:flags.13?(%Tuple int size) = news.OwnersGroup flags size;
news.placesGroup {flags:#} {size:#} int:flags.14?int vector:flags.15?(%Tuple int size) = news.PlacesGroup flags size;
news.itemsGroup {flags:#} {size:#} int:flags.16?int vector:flags.17?(%Tuple int size) = news.ItemsGroup flags size;

news.updatesGroup flags:# total_count:int size:# 
  type:(%news.TypesGroup flags size) user_id:(%news.UserIdsGroup flags size) date:(%news.DatesGroup flags size)
  tag:(%news.TagsGroup flags size) user:(%news.UsersGroup flags size) group:(%news.GroupsGroup flags size)
  owner:(%news.OwnersGroup flags size) place:(%news.PlacesGroup flags size) item:(%news.ItemsGroup flags size)
    = news.UpdatesGroup;

//news.rawUpdatesRecord timestamp:int random_tag:int type:int user_id:int user:int group:int owner:int place:int item:int = news.RawUpdatesRecord;
news.rawUpdatesRecord user_id:int date:int random_tag:int type:int user:int group:int owner:int place:int item:int = news.RawUpdatesRecord;

cnews.objectId type:int owner:int place:int = cnews.ObjectId;
cnews.rawUpdatesRecord date:int random_tag:int type:int user:int group:int owner:int place:int item:int = cnews.RawUpdatesRecord;

nnews.getUpdatesRecord type:int owner:int place:int user:int item:int date:int = nnews.GetUpdatesRecord;
nnews.groupRecord user_id:int item:int date:int = nnews.GroupRecord;
//nnews.getGroupedUpdatesRecord type:int owner:int place:int groups:%(VectorTotal %nnews.GroupRecord) = nnews.GetGroupedUpdatesRecord;

//special meaning rates: -1 -> forbiden owner, -2 -> mandatory owner
rnews.ratedUser user:int rate:int = rnews.RatedUser;
rnews.rawUpdatesRecord type:int owner:int place:int acting_users:int weight:double = rnews.RawUpdatesRecord;

---functions---
news.update id:int type:int user:int group:int owner:int place:int item:int = Bool;
news.deleteUpdates id:int = Bool;
news.setPrivacyMask id:int type_mask:int = Bool;
news.getPrivacyMask id:int = Maybe int;

news.getRawUpdates type_mask:int start_date:int end_date:int user_list:%(Vector int) = Vector %news.RawUpdatesRecord;
news.getGroupedUpdates type_mask:int date:int timestamp:int end_date:int grouping:int limit:int user_list:%(Vector int) = Vector %news.UpdatesGroup;

cnews.update type:int owner:int place:int user:int group:int item:int = Bool;
cnews.deleteUpdates type:int owner:int place:int = Bool;
cnews.deleteUpdate type:int owner:int place:int item:int = Bool;
cnews.undeleteUpdate type:int owner:int place:int item:int = Bool;
cnews.getRawUpdates start_date:int end_date:int object_list:%(Vector %cnews.ObjectId) = Vector %cnews.RawUpdatesRecord;
cnews.getGroupedUpdates date:int timestamp:int end_date:int grouping:int limit:int object_list:%(Vector %cnews.ObjectId) = Vector %news.UpdatesGroup;
cnews.getRawUserUpdates type_mask:int start_date:int end_date:int user_id:int = Vector %cnews.RawUpdatesRecord;
cnews.getGroupedUserUpdates type_mask:int date:int timestamp:int end_date:int grouping:int limit:int user_id:int = Vector %news.UpdatesGroup;
cnews.addBookmark type:int owner:int place:int user_id:int = Bool;
cnews.delBookmark type:int owner:int place:int user_id:int = Bool;

nnews.update id:int type:int user:int owner:int place:int item:int = Bool;
nnews.deleteUpdates type:int owner:int place:int = BoolStat;
nnews.deleteUpdate type:int owner:int place:int item:int = BoolStat;
nnews.undeleteUpdate type:int owner:int place:int item:int = BoolStat;
nnews.deleteUserUpdate user_id:int type:int owner:int place:int item:int = Bool;
nnews.undeleteUserUpdate user_id:int type:int owner:int place:int item:int = Bool;
nnews.getUpdates user_id:int mask:int date:int end_date:int timestamp:int limit:int = Vector %nnews.GetUpdatesRecord;
nnews.getGroupedUpdates user_id:int mask:int date:int end_date:int grouping:int timestamp:int limit:int = Vector %news.UpdatesGroup;

rnews.update id:int type:int owner:int place:int action:int item:int item_creation_time:int = Bool;
rnews.setRate type:int action:int rate:double = BoolStat;
rnews.getRate type:int action:int = Maybe double;
rnews.getRawUpdates type_mask:int start_date:int end_date:int id:int t:int timestamp:int user_list:%(Vector %rnews.RatedUser) = Vector %rnews.RawUpdatesRecord;
rnews.getGroupedUpdates type_mask:int start_date:int end_date:int id:int t:int timestamp:int acting_users_limit:int limit:int user_list:%(Vector %rnews.RatedUser) = Vector %news.UpdatesGroup;
