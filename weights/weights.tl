---types---

/////
//
// Weights
//
/////

weights.update {n:#} vector_id:int relaxation_time:int vector:n*[int] = weights.Update n;
weights.smallUpdate vector_id:int coord_id:int relaxation_time:int value:int = weights.SmallUpdate;

---functions---
weights.setHalfLife coord_id:int half_life:int = BoolStat;
weights.incr vector_id:int coord_id:int value:int = Maybe int;
weights.at vector_id:int coord_id:int = Maybe int;
weights.getVector vector_id:int = Maybe (%Vector int);

//targ-engine to weights-engine
weights.subscribe vector_rem:int vector_mod:int coord_ids:%(Vector int) updates_start_time:int updates_seek_limit:int updates_limit:int small_updates_seek_limit:int small_updates_limit:int = Maybe %(Vector int);
//weights-engine to targ-engine
weights.sendUpdates n:# updates:(%Vector %(weights.Update n)) = Bool;
weights.sendSmallUpdates updates:(%Vector %weights.SmallUpdate) = Bool;
