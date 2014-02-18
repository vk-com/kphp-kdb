---types---
/////
//
// iSearch
//
/////

isearch.typeInfo n:# data:n*[type:int probability:double] = isearch.TypeInfo;

---functions---

isearch.addSearchQuery user_id:int type:int count:int query:string = Bool;

isearch.getHints query:string = Vector %String;
isearch.getTypes query:string = isearch.TypeInfo;
isearch.getSuggestion query:string = String;

isearch.getTop limit:int = Vector %String;
isearch.getBest limit:int = Vector %String;

isearch.addBlackList query:string = Bool;
isearch.deleteBlackList query:string = Bool;
isearch.getBlackList = Vector %String;
isearch.forceUpdateBlackList = Bool;
