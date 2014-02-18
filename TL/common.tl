/////
//
// Common Types
//
/////

// Builtin types
int ? = Int;
long ? = Long;
double ? = Double;
string ? = String;

// Boolean emulation
boolFalse = Bool;
boolTrue = Bool;

// Boolean for diagonal queries
boolStat statTrue:int statFalse:int statUnknown:int = BoolStat;

// Vector
vector {t:Type} # [t] = Vector t;
tuple {t:Type} {n:#} [t] = Tuple t n;
vectorTotal {t:Type} total_count:int vector:%(Vector t) = VectorTotal t;


statOne key:string value:string = StatOne;
stat vector:%(Vector %statOne) = Stat;
/////
//
// Result- (Maybe-) types
//
/////

resultFalse {t:Type} = Maybe t;
resultTrue {t:Type} result:t = Maybe t;


pair {X:Type} {Y:Type} a:X b:Y = Pair X Y;

map {X:Type} {Y:Type} key:X value:Y = Map X Y;

Empty False;

true = True;

pid host:int port_pid:int utime:int = Pid;

rpcInvokeReqExtra {flags:#} wait_binlog_pos:flags.16?long kitten_php_delay_ms:flags.17?int string_forward_keys:flags.18?%(Vector string) int_forward_keys:flags.19?%(Vector long) string_forward:flags.20?string int_forward:flags.21?long wait_binlog_time:flags.22?long custom_timeout_ms:flags.23?int  = RpcInvokeReqExtra flags;
rpcReqResultExtra {flags:#} binlog_pos:flags.0?long  binlog_time:flags.1?long = RpcReqResultExtra flags;


reqError {X:Type} error_code:int error:string = ReqResult X;
reqResultHeader#8cc84ce1 {X:Type} flags:# extra:%(RpcReqResultExtra flags) result:X = ReqResult X;
//reqResultFalse {X:Type} = ReqResult X;
_ {X:Type} result:X = ReqResult X;

rpcReqResult#63aeda4e {X:Type} query_id:long result:(ReqResult X) = RpcReqResult X;
rpcReqError#7ae432f5 {X:Type} query_id:long error_code:int error:string = RpcReqResult X;

---functions---

rpcDestActor#7568aabd {X:Type} actor_id:long query:!X = X;
rpcDestActorFlags#f0a5acf7 {X:Type} actor_id:long flags:# extra:%(RpcInvokeReqExtra flags) query:!X = X;
rpcDestFlags#e352035e {X:Type} flags:# extra:%(RpcInvokeReqExtra flags) query:!X = X;

rpcInvokeReq#2374df3d {X:Type} query_id:long query:!X = RpcReqResult X;




engine.nop  = True;
engine.stat = Stat;
