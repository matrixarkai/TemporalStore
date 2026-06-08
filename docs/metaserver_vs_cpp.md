# Metaserver Vs C++ TemporalStore

## C++ Metaserver Shape

The C++ metaserver is the production control plane:

- `ManageService` mutates metadata through Raft proposals
- `QueryService` lists leader, cluster state, servers, proxies, namespaces, tables, and partitions
- `HeartbeatService` tracks server/proxy liveness and returns proxy config changes
- metadata model includes location, server, proxy, namespace, table, partition set, partition, membership, and placement
- scheduler/balance routines create partitions, update membership, and rebalance load
- failure detector convicts unhealthy servers/proxies and can freeze resources
- snapshots persist the whole metabase

Primary files read:

- `/home/vj/temporalstore-native/src/metaserver_v2/service/manage_service.cc`
- `/home/vj/temporalstore-native/src/metaserver_v2/service/query_service.cc`
- `/home/vj/temporalstore-native/src/metaserver_v2/service/heartbeat_service.cc`
- `/home/vj/temporalstore-native/src/metaserver_v2/meta/table.h`
- `/home/vj/temporalstore-native/src/metaserver_v2/meta/partition.h`
- `/home/vj/temporalstore-native/src/metaserver_v2/meta/server.h`
- `/home/vj/temporalstore-native/src/server/heartbeat.cc`
- `/home/vj/temporalstore-native/src/client/meta_syncer.cc`

## Rust Metaserver Coverage

Rust now has a richer control-plane model. The HTTP metaserver can run either the
single-node metadata store or an in-process Raft-backed `MetaRaftCluster`
backend by setting `TS_META_RAFT=1` or `TS_META_RAFT_NODES=1,2,3`:

- backward-compatible `/register_shard` and `/shards/<id>`
- server register/list/freeze/drop
- server heartbeat with boot time, binary version, and shard load report
- proxy register/list/freeze/drop
- proxy heartbeat with config-change response
- namespace create/list
- table create/list
- table topology query with topology version and not-modified behavior
- table topology partitions with slot ranges, shard ids, primary endpoint, and replica endpoints
- load-aware replica placement with location diversity before same-location fill
- meta info/stats counters
- Raft-backed metadata mutation path for shard registration, server/proxy
  registration, namespace/table creation, load-finish, and freeze/drop actions
- `GET /meta/raft/status` for metaserver Raft leader/quorum/status inspection

New/expanded HTTP routes:

- `GET /meta/info`
- `GET /meta/stats`
- `GET /meta/raft/status`
- `POST /servers/register`
- `POST /servers/heartbeat`
- `GET /servers`
- `POST /servers/freeze`
- `POST /servers/drop`
- `POST /proxies/register`
- `POST /proxies/heartbeat`
- `GET /proxies`
- `POST /proxies/freeze`
- `POST /proxies/drop`
- `POST /namespaces`
- `GET /namespaces`
- `POST /tables`
- `GET /tables`
- `POST /tables/topology`

## Still Missing

Rust metaserver is still not a production C++ metaserver replacement:

- no brpc/protobuf wire compatibility
- no networked multi-process Raft transport for the metaserver yet; the current
  HTTP binary uses the local in-process MetaRaft model
- no full durable metabase snapshot/load in the HTTP metaserver beyond the local JSONL mutation log
- no exact C++ partition id encoding or partition-set hierarchy
- no full placement rule chain for host deduplication, cooldowns, or scheduler-owned repair actions
- no task scheduler for create/load/freeze/drop membership workflows
- no proxy group placement/config model beyond simple config version
- no safe-mode checks or frozen-resource cooldown policy

The current Rust metaserver is now a useful open-source control-plane skeleton for local and integration tests. It models the key metadata entities and topology response shape, but the production scheduling and Raft persistence machinery remains future work.
