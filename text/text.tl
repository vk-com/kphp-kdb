---types---
/////
//
// Text
//
/////

//  Flag bits:
//    0  return with global_id
//    1  return with legacy_id
//    2  return with peer msg_id
//    3  return with cludges
//    4  return with text
//    5  return with flags
//    6  return with date
//    7  return with peer_id
//    8  return with ip
//    9  return with port
//    10 return with front
//    11 return with ua_hash
//    12 not used
//    13 not used
//    14 return with local_id
//    15 return with random_id
//    16..23 return with extra0...extra7
//    24..27 return with Extra0...Extra3

//text.messageNotFound {mode:#} = text.Message mode;
text.message {mode:#} local_id:mode.14?int global_id:mode.0?int legacy_id:mode.1?int peer_msg_id:mode.2?int flags:mode.5?int date:mode.6?int peer_id:mode.7?int ip:mode.8?int 
  port:mode.9?int front:mode.10?int ua_hash:mode.11?long random_id:mode.15?long
  extra0:mode.16?int extra1:mode.17?int extra2:mode.18?int extra3:mode.19?int extra4:mode.20?int extra5:mode.21?int extra6:mode.22?int extra7:mode.23?int
  Extra0:mode.24?long Extra1:mode.25?long Extra2:mode.26?long Extra3:mode.27?long  
  kludges:mode.3?string text:mode.4?string
  = text.Message mode;

text.messageFullId peer_id:int local_id:int = text.MessageFullId;
text.sublistType and_mask:int or_mask:int = text.SublistType;

text.eventOld event_id:int who:int data:int = text.EventOld;
text.eventBase event_id:int who:int data:int = text.Event;
text.eventEx event_id:int data:string = text.Event;

text.eventListOld timestamp:int num:# data:num*[text.EventOld] = text.EventListOld;
text.eventList timestamp:int events:%(Vector text.Event) = text.EventList;

text.onlineFriend {mode:#} id:int timestamp:mode.0?int = text.OnlineFriend mode;

text.resultSizeList total_size:int size:# data:size*[int] = text.ResultSizeList;

---functions---

text.getMessageShort uid:int local_id:int = Maybe %(text.Message (1 + 32 + 64 + 128 + 4096 + 2048));
text.getMessage uid:int local_id:int flags:# max_len:int = Maybe %(text.Message flags);

text.convertLegacyId uid:int legacy_id:int = Maybe int;
text.convertRandomId uid:int random_id:long = Maybe int;
text.peerMsgList uid:int peer:int from:int to:int = Maybe %(VectorTotal int);
text.peerMsgListPos uid:int peer:int local_id:int = Maybe int;

text.topMsgList uid:int from:int to:int = Maybe %(VectorTotal (%text.messageFullId));

text.sublistShort uid:int sublist:%text.SublistType from:int to:int = Maybe %(VectorTotal int);
text.sublist uid:int mode:# sublist:%text.SublistType from:int to:int = Maybe %(VectorTotal (%text.Message mode));
text.sublistPos uid:int sublist:%text.SublistType local_id:int = Maybe int;

text.sublistTypes = Vector %text.SublistType;
text.peermsgType = text.SublistType;

text.sendMessage uid:int mode:# message:%(text.Message mode) = Maybe int;
text.deleteMessage uid:int local_id:int = Bool;

text.deleteFirstMessages uid:int min_local_id:int = Bool;

text.getMessageFlags uid:int local_id:int = Maybe int;
text.setMessageFlags uid:int local_id:int flags:int = Bool;
text.incrMessageFlags uid:int local_id:int flags:int = Maybe int;
text.decrMessageFlags uid:int local_id:int flags:int = Maybe int;

text.getMessageextra uid:int local_id:int k:int = Maybe int;
text.setMessageextra uid:int local_id:int k:int value:int = Bool;
text.incrMessageextra uid:int local_id:int k:int value:int = Maybe int;
text.decrMessageextra uid:int local_id:int k:int value:int = Maybe int;

text.getMessageExtra uid:int local_id:int k:int = Maybe long;
text.setMessageExtra uid:int local_id:int k:int value:long = Bool;
text.incrMessageExtra uid:int local_id:int k:int value:long = Maybe long;
text.decrMessageExtra uid:int local_id:int k:int value:long = Maybe long;

text.deleteUserdata uid:int = Bool;
text.getUserdata uid:int = Maybe int;
text.loadUserdata uid:int force:int = Bool;

text.replaceMessageText uid:int local_id:int new_text:string = Bool;

text.getExtraMask = Tuple int 3;

text.setExtraMask mask:int = BoolStat;

text.search uid:int num:int query:string = Maybe %(VectorTotal int);
text.searchEx uid:int num:int query:string flags:# sublist:flags.0?%text.SublistType min_time:flags.1?int max_time:flags.1?int peer_id:flags.2?int = Maybe %(VectorTotal int);

text.getTimestamp uid:int = Maybe int;
text.getForceTimestamp uid:int = Maybe int;

//text.historyOld uid:int timestamp:int limit:int = Maybe %text.EventListOld;
text.history uid:int timestamp:int limit:int = Maybe %text.EventList;

text.historyAction uid:int event:text.Event = Bool;

text.getPTimestamp uid:int = Maybe int;

text.pHistory uid:int timestamp:int limit:int = Maybe %text.EventList;

text.online uid:int friends:(%Vector int) = BoolStat;
text.offline uid:int friends:(%Vector int) = BoolStat;

text.onlineFriends uid:int mode:# = Maybe %(Vector %(text.OnlineFriend mode));
text.onlineFriendsId uid:int = Maybe %(Vector int);

text.setSecret uid:int s:string = Bool;
text.getSecret uid:int = Maybe string;
text.deleteSecret uid:int = Bool;
