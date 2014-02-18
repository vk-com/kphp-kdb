---types---

rpcProxy.target host:int port:int actor_id:long = rpcProxy.Target;
rpcProxy.netTarget host:int port:int = rpcProxy.NetTarget;

---functions---
rpcProxy.getClusters = Vector long;
rpcProxy.getClusterSize actor_id:long = Maybe int;
rpcProxy.getClusterServers actor_id:long = Maybe (%Vector %rpcProxy.Target);
rpcProxy.getClusterStats actor_id:long = Maybe (%Stat);
rpcProxy.getBadTargets = Vector %rpcProxy.NetTarget;
