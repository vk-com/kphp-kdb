---types---
/////
//
// Search
//
/////

//  Flag bits:
//    1  Sort
//    2  Sort descending (should be with +1)
//    3  Restrictions on rate/sate/bate ...
//    4  Search for exact hash
//    5  Group by hash
//    6  Use nate as hash
//    7  Use_relevance
//    8  Search only in title
//    9  Opttag coef
//    10 Rate weight
//    11 Priority weight
//    12 Decay time
//    13 Enable extended search mode
//    14 Enable occurance count
//    15 Random search
//    16 Weak search - up to 2 words missing

search.objectId {n:#} data:n*[int] = search.ObjectId n;
search.restrictions m:# data:m*[rate_type:int min_value:int max_value:int] = search.Restrictions;

search.oneResult {n:#} {flags:#} id:%(search.ObjectId n) rate:flags.1?int hash:flags.5?long = search.OneResult n flags;
search.result {flags:#} n:# results:%(VectorTotal %(search.OneResult n flags)) = search.Result flags;

search.oneNormalize rate_type:int weight:double fun:int = search.OneNormalize;
search.normalize n:# data:n*[search.OneNormalize] = search.Normalize;
---functions---

search.setRate n:# id:%(search.ObjectId n) rate_type:int rate_value:int = Bool;
search.incrRate n:# id:%(search.ObjectId n) rate_type:int rate_value:int = Bool;
search.decrRate n:# id:%(search.ObjectId n) rate_type:int rate_value:int = Bool;
search.getRate n:# id:%(search.ObjectId n) rate_type:int = Maybe int;

search.setHash n:# id:%(search.ObjectId n) hash:long = Bool;
search.getHash n:# id:%(search.ObjectId n) = Maybe long;

search.deleteItem  n:# id:%(search.ObjectId n) = Bool;
search.setItem n:# id:%(search.ObjectId n) rate:int sate:int text:string = Bool;

search.addTags n:# id:%(search.ObjectId n) tags:string = Bool;

search.deleteWithHash hash:long = BoolStat;
search.deleteWithHashes m:# hashes:m*[long] = BoolStat;

search.incrRateByHash rate_type:int rate:int m:# hashes:m*[long] = BoolStat;
search.changeRates m:# rate_type:int rates:m*[old:int new:int] = BoolStat;

search.search flags:# limit:int rate_type:flags.1?int restrictions:flags.3?%search.Restrictions hash:flags.4?long use_relevance:flags.7?double opttag_coeff:flags.9?double
  rate_normalize:flags.10?%search.Normalize priority_normalize:flags.11?double decay:flags.12?%search.OneNormalize hash_change:flags.6?int query:string = search.Result flags;
