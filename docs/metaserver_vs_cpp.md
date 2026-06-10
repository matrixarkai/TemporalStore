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
- table create/list/update/delete
- table serving options for pin-primary, replica-read policy, preferred location, drop percent,
  read/write retries, retry backoff, continuous-failure window, and connect/I/O timeouts
- table topology query with topology version and not-modified behavior
- dropped tables remain visible in table inventory but are excluded from namespace table counts and
  rejected by topology/open calls with `table_not_found`
- table topology partitions with slot ranges, shard ids, primary endpoint, and replica endpoints
- table topology also includes location-bearing endpoint metadata for primary/replicas while
  preserving the original address-only fields for compatibility
- load-aware replica placement with location and host diversity before same-location fill
- meta info/stats counters
- single-node metabase snapshot export/import and atomic local snapshot save/load
- scheduler admin surface for submit, run-next, snapshot, and restore of metaserver tasks
- optional local scheduler snapshot persistence through `TS_META_SCHEDULER_SNAPSHOT`
- stale server/proxy freezing with safe-mode cooldown timestamps, rejoin/heartbeat rejection while
  cooldown is active, and `GET /meta/safe_mode` reporting for blocked resources
- Raft-backed metadata mutation path for shard registration, server/proxy
  registration, namespace/table creation/deletion, load-finish, safe-mode stale freeze, and
  freeze/drop actions
- `GET /meta/raft/status` for metaserver Raft leader/quorum/status inspection

New/expanded HTTP routes:

- `GET /meta/info`
- `GET /meta/stats`
- `GET /meta/raft/status`
- `GET /meta/snapshot`
- `POST /meta/snapshot`
- `POST /meta/snapshot/restore`
- `POST /meta/snapshot/save`
- `POST /meta/snapshot/load`
- `GET /meta/scheduler`
- `GET /meta/scheduler/snapshot`
- `POST /meta/scheduler/submit`
- `POST /meta/scheduler/run_next`
- `POST /meta/scheduler/restore`
- `POST /servers/register`
- `POST /servers/heartbeat`
- `GET /servers`
- `POST /servers/freeze_stale`
- `POST /servers/freeze`
- `POST /servers/drop`
- `GET /meta/safe_mode`
- `POST /proxies/register`
- `POST /proxies/heartbeat`
- `GET /proxies`
- `POST /proxies/freeze`
- `POST /proxies/drop`
- `POST /namespaces`
- `GET /namespaces`
- `POST /tables`
- `POST /tables/update`
- `PATCH /tables`
- `POST /tables/delete`
- `DELETE /tables`
- `GET /tables`
- `POST /tables/topology`

## Still Missing

Rust metaserver is still not a production C++ metaserver replacement:

- no brpc/protobuf wire compatibility
- no networked multi-process Raft transport for the metaserver yet; the current
  HTTP binary uses the local in-process MetaRaft model
- no Raft-metaserver snapshot export/install wired through the networked Raft path yet
- no full C++ partition-set hierarchy; Rust now has the exact `PartitionId` bit layout and an
  opt-in encoded table topology path
- no full C++ placement rule chain beyond the implemented frozen-resource cooldown gate
- no background scheduler loop that executes create/load/freeze/drop workflows against real
  data-node processes
- no proxy group placement/config model beyond simple config version

The current Rust metaserver is now a useful open-source control-plane skeleton for local and integration tests. It models the key metadata entities and topology response shape, but the production scheduling and Raft persistence machinery remains future work.
