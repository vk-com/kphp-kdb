---types---
/////
//
// SeqMap
//
/////

seqmap.key key:%(Vector int) = seqmap.Key;
seqmap.value value:%(Vector int) = seqmap.Value;
seqmap.keyValue key:%seqmap.Key value:%seqmap.Value = seqmap.KeyValue;

---functions---

seqmap.set key:%seqmap.Key value:%seqmap.Value time:int = Bool;
seqmap.add key:%seqmap.Key value:%seqmap.Value time:int = Bool;
seqmap.replace key:%seqmap.Key value:%seqmap.Value time:int = Bool;
seqmap.delete key:%seqmap.Key = Bool;

seqmap.get key:%seqmap.Key = Maybe %seqmap.Value;

seqmap.getRange left:%seqmap.Key right:%seqmap.Key limit:int = VectorTotal %seqmap.KeyValue;
seqmap.getWildcard key:%seqmap.Key limit:int = VectorTotal %seqmap.KeyValue;

seqmap.getRangeCount left:%seqmap.Key right:%seqmap.Key = Int;
seqmap.getWildcardCount key:%seqmap.Key = Int;
