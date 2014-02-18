---types---
/////
//
// Lists
//
/////

//  Flag bits:
//    0..2 sublist num
//    3  use sublist
//    4  descending order
//    5  sort by date
//    6  return with flags
//    7  return with date
//    8  return with global_id
//    9  return with value
//    10 return with text
//    11 return with ip
//    12 return with front_ip
//    13 return with port
//    14 return with ua_hash
//    15 return object_id

lists.listId {n:#} %(Tuple int n) = lists.ListId n;
lists.objectId {m:#} %(Tuple int m) = lists.ObjectId m;
lists.weightedObjectId {m:#} id:(%lists.ObjectId m) weight:int = lists.WeightedObjectId m;

lists.objectFull {m:#} {fields_mask:#} object_id:fields_mask.15?%(lists.ObjectId m) flags:fields_mask.6?int date:fields_mask.7?int global_id:fields_mask.8?long value:fields_mask.9?long text:fields_mask.10?string 
      ip:fields_mask.11?int front_ip:fields_mask.12?int port:fields_mask.13?int ua_hash:fields_mask.14?int = lists.ObjectFull m fields_mask;

---functions---
lists.deleteList n:# m:# list_id:%(lists.ListId n) = Bool;
lists.deleteObject n:# m:# object_id:%(lists.ObjectId m) = BoolStat;
lists.deleteEntry n:# m:# list_id:%(lists.ListId n) object_id:%(lists.ObjectId m) = Bool;

lists.setEntry n:# m:# list_id:%(lists.ListId n) mode:# entry:%(lists.objectFull m mode) = Bool;
lists.addEntry n:# m:# list_id:%(lists.ListId n) mode:# entry:%(lists.objectFull m mode) = Bool;
lists.replaceEntry n:# m:# list_id:%(lists.ListId n) mode:# entry:%(lists.objectFull m mode) = Bool;


//lists.setEntry n:# m:# list_id:%(lists.ListId n) object_id:%(lists.ObjectId m) flags:int value:long ip:int front_ip:int port:int ua_hash:int = Bool;
//lists.setEntryShort n:# m:# list_id:%(lists.ListId n) object_id:%(lists.ObjectId m) flags:int value:long = Bool;
//lists.addEntry n:# m:# list_id:%(lists.ListId n) object_id:%(lists.ObjectId m) flags:int value:long ip:int front_ip:int port:int ua_hash:int = Bool;
//lists.addEntryShort n:# m:# list_id:%(lists.ListId n) object_id:%(lists.ObjectId m) flags:int value:long = Bool;
//lists.replaceEntry n:# m:# list_id:%(lists.ListId n) object_id:%(lists.ObjectId m) flags:int value:long ip:int front_ip:int port:int ua_hash:int = Bool;
//lists.replaceEntryShort n:# m:# list_id:%(lists.ListId n) object_id:%(lists.ObjectId m) flags:int value:long = Bool;

lists.setEntryText n:# m:# list_id:%(lists.ListId n) object_id:%(lists.ObjectId m) text:string = Bool;

lists.setFlags n:# m:# list_id:%(lists.ListId n) object_id:%(lists.ObjectId m) flags:int = Bool;
lists.changeFlags n:# m:# list_id:%(lists.ListId n) object_id:%(lists.ObjectId m) set_flags:int clear_flags:int = Bool;
lists.incrFlags n:# m:# list_id:%(lists.ListId n) object_id:%(lists.ObjectId m) flags:int = Bool;
lists.decrFlags n:# m:# list_id:%(lists.ListId n) object_id:%(lists.ObjectId m) flags:int = Bool;

lists.setValue n:# m:# list_id:%(lists.ListId n) object_id:%(lists.ObjectId m) value:long = Maybe long;
lists.incrValue n:# m:# list_id:%(lists.ListId n) object_id:%(lists.ObjectId m) value:long = Maybe long;
lists.decrValue n:# m:# list_id:%(lists.ListId n) object_id:%(lists.ObjectId m) value:long = Maybe long;
lists.incrValueOrCreate n:# m:# list_id:%(lists.ListId n) object_id:%(lists.ObjectId m) flags:int value:long = Maybe long;
lists.decrValueOrCreate n:# m:# list_id:%(lists.ListId n) object_id:%(lists.ObjectId m) flags:int value:long = Maybe long;

lists.setListFlags n:# m:# list_id:%(lists.ListId n) flags:int new_flags:int = Bool;
lists.changeListFlags n:# m:# list_id:%(lists.ListId n) flags:int new_or_mask:int new_nand_mask:int = Bool;
lists.changeListFlagsEx n:# m:# list_id:%(lists.ListId n) xor_mask:int and_mask:int new_or_mask:int new_nand_mask:int = Bool;

lists.deleteSublist n:# m:# list_id:%(lists.ListId n) flags:int = Bool;
lists.deleteSublistEx n:# m:# list_id:%(lists.ListId n) xor_mask:int and_mask:int = Bool;

lists.getEntry n:# m:# list_id:%(lists.ListId n) object_id:%(lists.ObjectId m) mode:# = Maybe (%lists.ObjectFull m mode);
lists.getEntryFlags n:# m:# list_id:%(lists.ListId n) object_id:%(lists.ObjectId m) = Maybe int;
lists.getEntryDate n:# m:# list_id:%(lists.ListId n) object_id:%(lists.ObjectId m) = Maybe int;
lists.getEntryValue n:# m:# list_id:%(lists.ListId n) object_id:%(lists.ObjectId m) = Maybe long;
lists.getEntryGlobalId n:# m:# list_id:%(lists.ListId n) object_id:%(lists.ObjectId m) = Maybe long;
lists.getEntryText n:# m:# list_id:%(lists.ListId n) object_id:%(lists.ObjectId m) = Maybe string;
lists.getEntryPos n:# m:# list_id:%(lists.ListId n) object_id:%(lists.ObjectId m) = Int;

lists.getList n:# m:# list_id:%(lists.ListId n) mode:int = VectorTotal (%lists.ObjectId m);
lists.getListLimit n:# m:# list_id:%(lists.ListId n) mode:int limit:int offset:int = VectorTotal (%lists.ObjectId m);
lists.getListFull n:# m:# list_id:%(lists.ListId n) mode:# = VectorTotal (%lists.ObjectFull m mode);
lists.getListFullLimit n:# m:# list_id:%(lists.ListId n) mode:#  limit:int offset:int= VectorTotal (%lists.ObjectFull m mode);

lists.getListCount n:# m:# list_id:%(lists.ListId n) = Int;
lists.getSublistCount n:# m:# list_id:%(lists.ListId n) mode:int = Int;
lists.getSublistsCount n:# m:# list_id:%(lists.ListId n) = Tuple int 9;

lists.intersect n:# m:# list_id:%(lists.ListId n) mode:int object_list:%(Vector %(lists.ObjectId m)) = VectorTotal (%lists.ObjectId m);
lists.intersectLimit n:# m:# list_id:%(lists.ListId n) mode:int object_list:%(Vector %(lists.ObjectId m)) limit:int = VectorTotal (%lists.ObjectId m);
lists.intersectFull n:# m:# list_id:%(lists.ListId n) mode:# object_list:%(Vector %(lists.ObjectId m)) = VectorTotal (%lists.ObjectFull m mode);
lists.intersectFullLimit n:# m:# list_id:%(lists.ListId n) mode:# object_list:%(Vector %(lists.ObjectId m)) limit:int = VectorTotal (%lists.ObjectFull m mode);

lists.intersectWild n:# m:# list_id:%(lists.ListId n) k:# mode:int object_list:%(Vector %(lists.ObjectId k)) = VectorTotal (%lists.ObjectId m);
lists.intersectWildLimit n:# m:# list_id:%(lists.ListId n) k:# mode:int object_list:%(Vector %(lists.ObjectId k)) limit:int = VectorTotal (%lists.ObjectId m);
lists.intersectWildFull n:# m:# list_id:%(lists.ListId n) k:# mode:# object_list:%(Vector %(lists.ObjectId k)) = VectorTotal (%lists.ObjectFull m mode);
lists.intersectWildFullLimit n:# m:# list_id:%(lists.ListId n) k:# mode:# object_list:%(Vector %(lists.ObjectId k)) limit:int = VectorTotal (%lists.ObjectFull m mode);

lists.subtract n:# m:# list_id:%(lists.ListId n) mode:int object_list:%(Vector %(lists.ObjectId m)) = VectorTotal (%lists.ObjectId m);
lists.subtractLimit n:# m:# list_id:%(lists.ListId n) mode:int object_list:%(Vector %(lists.ObjectId m)) limit:int = VectorTotal (%lists.ObjectId m);

lists.sumlist n:# m:# list_id:%(lists.ListId n) mode:int object_list:%(Vector %(lists.ObjectId m)) = Long;
lists.sumlistWild n:# m:# list_id:%(lists.ListId n) k:# mode:int object_list:%(Vector %(lists.ObjectId k)) = Long;
lists.sumlistWeighted n:# m:# list_id:%(lists.ListId n) mode:int object_list:%(Vector %(lists.WeightedObjectId m)) = Long;
lists.sumlistWeightedWild n:# m:# list_id:%(lists.ListId n) k:# mode:int object_list:%(Vector %(lists.WeightedObjectId k)) = Long;

lists.sortedList n:# m:# list_id:%(lists.ListId n) xor_mask:int and_mask:int mode:int = VectorTotal (%lists.ObjectId m);
lists.sortedListLimit n:# m:# list_id:%(lists.ListId n) xor_mask:int and_mask:int mode:int limit:int = VectorTotal (%lists.ObjectId m);
lists.sortedListFull n:# m:# list_id:%(lists.ListId n) xor_mask:int and_mask:int mode:# = VectorTotal (%lists.ObjectFull m mode);
lists.sortedListFullLimit n:# m:# list_id:%(lists.ListId n) xor_mask:int and_mask:int mode:# limit:int = VectorTotal (%lists.ObjectFull m mode);

lists.datedistr n:# m:# list_id:%(lists.ListId n) mode:int min_date:int max_date:int step:int = Vector int;

