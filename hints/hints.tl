---types---
/////
//
// Hints
//
/////

hints.object type:int object_id:int = hints.Object;
hints.objectRating type:int object_id:int rating:double = hints.ObjectRating;
hints.objectText type:int object_id:int text:string = hints.ObjectText;
hints.objectFull type:int object_id:int rating:double text:string = hints.ObjectFull;

rating.object type:int object_id:int = rating.Object;
rating.objectRating type:int object_id:int rating:double = rating.ObjectRating;

---functions---

hints.nullifyRating  user_id:int = Bool;
hints.setRatingState user_id:int rating_enabled:Bool = Bool;
hints.setInfo        user_id:int info:int = Bool;
hints.setWinner      user_id:int type:int winner:int losers:%(Vector int) rating_num:int = Bool;
hints.setRating      user_id:int type:int object_id:int rating_num:int rating:double = Bool;
hints.setText        user_id:int type:int object_id:int text:string = Bool;
hints.setType        user_id:int type:int object_id:int new_type:int = Bool;
hints.setTextGlobal              type:int object_id:int text:string = BoolStat;
hints.setTypeGlobal              type:int object_id:int new_type:int = BoolStat;

hints.getInfo   user_id:int = Maybe int;
hints.sort      user_id:int limit:int rating_num:int objects:%(Vector %hints.Object) = Vector (%hints.Object);
hints.getRandom user_id:int limit:int rating_num:int objects:%(Vector %hints.Object) = Vector (%hints.Object);

//type = -1 for all types
//latin - search without translit and changing keyboard layout
hints.getHints            user_id:int type:int limit:int rating_num:int query:string = VectorTotal (%hints.Object);
hints.getHintsRating      user_id:int type:int limit:int rating_num:int query:string = VectorTotal (%hints.ObjectRating);
hints.getHintsText        user_id:int type:int limit:int rating_num:int query:string = VectorTotal (%hints.ObjectText);
hints.getHintsFull        user_id:int type:int limit:int rating_num:int query:string = VectorTotal (%hints.ObjectFull);
hints.getHintsLatin       user_id:int type:int limit:int rating_num:int query:string = VectorTotal (%hints.Object);
hints.getHintsLatinRating user_id:int type:int limit:int rating_num:int query:string = VectorTotal (%hints.ObjectRating);
hints.getHintsLatinText   user_id:int type:int limit:int rating_num:int query:string = VectorTotal (%hints.ObjectText);
hints.getHintsLatinFull   user_id:int type:int limit:int rating_num:int query:string = VectorTotal (%hints.ObjectFull);

hints.getObjectText  user_id:int type:int object_id:int = Maybe %String;

hints.deleteObject       user_id:int type:int object_id:int = Bool;
hints.deleteObjectGlobal             type:int object_id:int = BoolStat;
hints.incrementRating    user_id:int type:int object_id:int cnt:int rating_num:int = Bool;


rating.nullifyRating  user_id:int = Bool;
rating.setRatingState user_id:int rating_enabled:Bool = Bool;
rating.setInfo        user_id:int info:int = Bool;
rating.setWinner      user_id:int type:int winner:int losers:%(Vector int) rating_num:int = Bool;
rating.setType        user_id:int type:int object_id:int new_type:int = Bool;
rating.setRating      user_id:int type:int object_id:int rating_num:int rating:double = Bool;
rating.addObject      user_id:int type:int object_id:int = Bool;
rating.setTypeGlobal              type:int object_id:int new_type:int = BoolStat;

rating.getInfo   user_id:int = Maybe int;
rating.sort      user_id:int limit:int rating_num:int objects:%(Vector %rating.Object) = Vector (%rating.Object);
rating.getRandom user_id:int limit:int rating_num:int objects:%(Vector %rating.Object) = Vector (%rating.Object);

//type = -1 for all types
rating.getHints             user_id:int type:int limit:int rating_num:int exceptions:%(Vector %rating.Object) = VectorTotal (%rating.Object);
rating.getHintsRating       user_id:int type:int limit:int rating_num:int exceptions:%(Vector %rating.Object) = VectorTotal (%rating.ObjectRating);
rating.getRandomHints       user_id:int type:int limit:int rating_num:int exceptions:%(Vector %rating.Object) = VectorTotal (%rating.Object);
rating.getRandomHintsRating user_id:int type:int limit:int rating_num:int exceptions:%(Vector %rating.Object) = VectorTotal (%rating.ObjectRating);

rating.deleteObject       user_id:int type:int object_id:int = Bool;
rating.deleteObjectGlobal             type:int object_id:int = BoolStat;
rating.incrementRating    user_id:int type:int object_id:int cnt:int rating_num:int = Bool;

