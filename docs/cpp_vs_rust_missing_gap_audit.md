# C++ TemporalStore Vs Rust Missing Gap Audit

## Bottom Line

The Rust code now covers the local correctness skeleton for TemporalStore-style serving:

- command API for common, string, hash, set, feature, sequence, and the implemented IPS/Risk subset
- local shard engine with page-address indexes
- local page segment files
- memory plus disk read-through cache
- whole index snapshot stream
- oplog stream
- index-log stream
- page stream
- shared-store checkpoint and oplog replay model
- production-wrapped data-node Raft behavior and in-process metaserver Raft behavior
- local Raft snapshot behavior
- ByteKV/ByteRaft-style Raft configuration defaults, validation, read options, oversized log rejection, and election prohibition
- ByteRaft-style local Raft status, local status, read-index guard, leader transfer, and Prometheus raft metrics
- ByteRaft-style commit-to-apply lag health reports and Prometheus apply-lag metrics for data and
  metaserver Raft
- proxy/client/metaserver/server binaries
- table-aware Rust client with typed string/hash/common methods, pipeline batching, HTTP timeout/retry options, optional direct route refresh, and primary/secondary endpoint selection from metaserver topology
- proxy wraps the Rust client library for route cache, timeout/retry options, and backend-error route refresh, with stats/config endpoints
- client key-to-shard routing, table open/close cache, stats, expanded typed methods, and multi-shard pipeline grouping
- metaserver namespace/table topology, server/proxy register/list/heartbeat, topology versioning, meta stats/info, optional Raft-backed HTTP mutation path, and production metaserver Raft runtime wrapper
- data-node checked execute/batch by load version, server registration, periodic heartbeat load reporting, and loaded-shard stats
- heartbeat load reports now include C++-style per-partition `partition_info` payloads in addition
  to compact placement load signals
- data-node worker runtime with bounded async queue, job status, dirty-object ids, dump/compact/GC hooks, and load-finish callback endpoint
- Redis RESP adapter, including conditional `SET NX/XX`, `GET`, `EX`, and `PX`
- Prometheus scrape output for shard records, cache, page-store, oplog, and data-node runtime counters
- S3-compatible snapshot store crate
- replica replay loop consuming checkpoint index/pages, index-log tail, and oplog tail with a persisted cursor
- remote HTTP stream source for replica replay over `/read_stream` and `/scan_stream`
- server-side `/replica/replay` endpoint for secondary data-node catch-up from a primary stream
  source
- opt-in background server replica replay loop for readonly/secondary catch-up from a configured
  primary stream source
- metaserver-discovered background replica replay: when no fixed primary is configured, the server
  polls `GET /shards/<shard_id>`, uses the returned route as the replay stream source, and skips
  replay if the route points back to the local advertised server
- background replica replay operational status: `/server/replica_replay_status`, Prometheus loop
  counters/gauges, consecutive-failure tracking, last error/report, and bounded failure backoff
- server startup shard load parity for readonly secondary mode, table name, shard URI, load version,
  routing-slot range, and local node id
- server membership-update finish callback to metaserver through `/partitions/finish_load` after a
  successful local membership install
- single-node metaserver metabase snapshot export/import plus atomic local snapshot save/load
- local shard/table/tenant read/write QPS admission through `Config` quota fields
- C++ common-module `EXPIRE` missing-key behavior from
  `temporalstore-small/src/extension/common/implement.cc`: engine returns not-found instead of
  creating orphan TTL metadata; RESP `EXPIRE` maps that not-found to integer `0`

It is still not production C++ TemporalStore parity. The largest missing areas for the current Rust target are production storage lifecycle, real Raft engine integration, recovery/chaos validation, and operational controls.

brpc and Thrift are intentionally not part of the Rust target. The Rust open-source target is Rust-native service RPC where needed, HTTP/JSON for admin/debug, RESP for Redis compatibility, and Prometheus text for metrics. S3 and ByteStore live-backend integration are also out of scope for the current parity push.

## What Was Closed In The Latest Pass

This pass repeated the Raft distributed replication and failover comparison three times against the
C++ Raft control protocol artifacts and the Rust standalone/integrated data-node Raft surfaces:

1. The raft-enabled `server` binary now exposes `POST /raft/control/accept_leadership`, matching
   the standalone `raft_node` control route.
2. The route rejects requests for another node id, catches up the local node, and transfers
   leadership to that local node.
3. A server-route regression test covers wrong-node rejection and successful local leadership
   acceptance.
4. The repeated review is captured in
   `docs/three_pass_raft_distributed_parity_2026_06_12.md`.

This closes a process-boundary control-route parity gap. It does not change the remaining
production caveat: the Rust Raft path is still a local-model/open-source control-plane parity layer,
not yet real OpenRaft/raft-rs FSM/storage with production mTLS and external chaos validation.

## What Was Closed In The Previous Pass

This pass repeated the client/proxy/data-node/metaserver comparison six times and closed one
practical local testing gap:

1. The Rust `client` binary now exposes direct commands for more implemented C++/Redis-style
   families: `exists`, `sdel`, `setnx`, `setxx`, `hmset`, `hmget`, `hincrby`, `hgetall`,
   `hlen`, `hdel`, `fappendnx`, `fappendxx`, `ipsrange`, `ipsremove`, `ipsdel`, `ipscount`,
   `riskquery`, `riskdetail`, `riskhset`, `cpcset`, `folset`, `folquery`, and `riskmanager`.
2. The Rust `client` binary now also supports `json '<command-json>'`, so local parity and scale
   tests can drive every implemented `Command` enum variant without waiting for a dedicated CLI
   subcommand.
3. The repeated review is captured in
   `docs/six_pass_client_proxy_datanode_meta_parity_2026_06_12.md`, with explicit remaining gaps
   for client, proxy, data-node, metaserver, Redis/function reachability, and scale validation.

These changes improve local Rust parity validation. They do not change the remaining production
gap statement: brpc/Thrift wire compatibility, tonic/prost service definitions, production storage
lifecycle, real OpenRaft/raft-rs FSM/storage, and multi-process operational workflows remain open.

## What Was Closed In The Previous Pass

This pass closed one of the hard readiness blockers instead of leaving it as a note:

1. `SingleNodeMeta::with_mutation_log(path)` now replays a durable JSONL metaserver mutation log.
2. Mutating metaserver operations append `MetaMutation` records before applying state changes.
3. The metaserver binary enables this with `TS_META_MUTATION_LOG`.
4. Recovery is test-backed for shard routes, registered servers/proxies, namespaces/tables,
   topology, and state changes.
5. Replica placement now keeps load-aware ordering but prefers distinct server locations before
   filling same-location replicas, matching one important C++ placement-rule behavior.
6. Failed server/proxy freeze/drop requests no longer append no-op durable metaserver mutations.
7. The metaserver binary can now run with an in-process Raft-backed metadata backend using `TS_META_RAFT=1` or `TS_META_RAFT_NODES=1,2,3`.
8. In Raft mode, HTTP mutations for shard registration, server/proxy registration, namespace/table creation, load-finish, and freeze/drop actions are proposed through `MetaRaftCluster`.
9. The Raft metaserver path is test-backed for full table-topology metadata replication, stale-server freeze as a replicated mutation, and no-majority mutation rejection.
10. The server binary now exposes C++-style data-Raft read-policy controls through
    `TS_DATA_RAFT_READ_MODE`, `TS_DATA_RAFT_BOUNDED_STALE_MAX_INDEX_LAG`, and
    `TS_DATA_RAFT_READ_INDEX_TIMEOUT_MS`. In raft server mode, `/execute` can route reads through
    leader/linearizable behavior or local bounded-stale replica reads instead of always forcing
    leader reads.
11. The Rust client router now has a C++ `RouterV1`-style read-replica round-robin policy via
    `ReplicaReadPolicy::RoundRobinReplica`; `FirstReplica` remains available for deterministic
    smoke tests and primary pinning still forces writes to the primary.
12. The data-node/server runtime now exposes C++ partition-thread ownership parity: runtime stats
    include worker-pool limits, `shard_worker_info()` reports `shard_id % worker_threads`, the
    server exposes `/server/shard_worker/<shard_id>` and `/server/queued_shard_workers`, and queued
    or in-flight jobs can be canceled through `POST /jobs/<job_id>/cancel`.
13. The Rust Raft local/prod wrapper now exposes the C++ `DataRaftConsensusBackend`
    `WaitForAppliedIndex(index, timeout_ms)` contract as `wait_for_applied_index(node_id, index,
    timeout_ms)`, using applied FSM index rather than only commit index. Tests cover immediate
    success, timeout on a lagging follower, and success after catch-up.
14. The standalone `raft_node` and raft-enabled `server` admin surfaces now expose
    `POST /raft/admin/wait_applied` so local process tests and operators can wait for a follower's
    FSM-applied index over HTTP, matching the C++ data-Raft backend control contract instead of
    requiring direct Rust object access.
15. The proxy now exposes table-aware serving routes, closing a C++ proxy/client layering gap:
    `POST /proxy/open_table` syncs namespace/table topology from metaserver, and
    `POST /proxy/table_execute` plus `POST /proxy/table_batch_execute` route commands through the
    Rust client library using key-to-shard table routing instead of requiring callers to provide a
    shard id. A regression test proves a proxy table write lands on the shard selected by the C++
    CRC64 routing formula.
16. The client/metaserver topology path now carries location-aware endpoint metadata. Newer clients
    can set `ClientOptions.local_location` or `TableOptions.preferred_location`; replica-read
    routing then prefers a same-location secondary before falling back to the configured first
    replica or round-robin policy. The old address-only topology fields remain intact for
    compatibility.
17. The client now has C++-style drop-percent traffic shedding at the table-routing layer:
    `ClientOptions.drop_percent` seeds `TableOptions.drop_percent`, and table execute/batch paths
    deterministically reject sampled keys with `traffic_dropped` before contacting a backend.
18. Distributed Raft now exposes the commit-to-apply health contract over the process/runtime
    serving surface: standalone `raft_node`, raft-enabled `server`, and the distributed Raft harness
    all serve `POST /raft/apply_health`. The local four-node harness now waits through that public
    route until every node reports zero apply lag after leader transfer, voter scale-down, voter
    scale-up, and replica reads.
19. The single-node metaserver now has C++-style metabase snapshot coverage: `MetaSnapshot`
    captures shard routes, latest shard snapshot refs, server/proxy inventory and states,
    namespaces, tables, counters, next table id, and topology version. `GET /meta/snapshot`,
    `POST /meta/snapshot[/restore]`, `POST /meta/snapshot/save`, and `POST /meta/snapshot/load`
    are test-backed.
20. The metaserver binary now exposes the deterministic scheduler model over HTTP:
    `GET /meta/scheduler`, `GET /meta/scheduler/snapshot`, `POST /meta/scheduler/submit`,
    `POST /meta/scheduler/run_next`, and `POST /meta/scheduler/restore`. Route tests cover
    priority ordering, retry-later backoff, snapshot export, and restore.
21. The metaserver scheduler can now persist its task snapshot locally with
    `TS_META_SCHEDULER_SNAPSHOT`; submit/run/restore operations atomically rewrite the scheduler
    snapshot file and startup reloads it.
22. Metaserver placement now prefers host diversity before same-host replica fill while preserving
    the lower-load ordering and falling back to same-host replicas when distinct hosts are
    insufficient.
23. Client close-table now matches the C++ `ClientImpl::CloseTable` plus
    `MetaSyncer::CloseTable` cleanup shape more closely: after unregistering the table from the
    local meta-sync table cache, Rust evicts cached shard routes so a later open cannot inherit
    stale routing from a closed table handle.
24. Client table execute/batch paths now match the C++ `TemporalStoreClient::Impl::WithRetry`
    status contract for open-source API calls: retryable backend statuses including
    `retry_later`, `partition_loading`, `meta_changed`, `topom_error`, `unavailable`,
    `deadline_exceeded`, and `internal` are retried with separate read/write budgets and linear
    backoff. Defaults mirror the C++ wrapper: one read retry, zero write retries.
25. Raft now has a deterministic version of the C++ metaserver `TrivialRoutine::MaybeTriggerSnapshot`
    behavior for both data-node and metaserver Raft: when `can_trigger_snapshot` is enabled and
    applied log bytes since the latest snapshot floor exceed `max_applied_log_bytes`, Rust reports
    the trigger reason and installs a compacting snapshot floor.
26. Proxy config update now matches the C++ `Proxy::UpdateConfig` duplicate-update guard: if the
    namespace and derived config version are unchanged, Rust returns a no-op report and preserves
    the current client/cache instead of rebuilding it.
27. Data-node shard config update now matches C++ `Partition::SetConfig` version semantics: stale
    config versions are rejected with `failed_precondition`, equal versions are no-ops, and only
    newer versions replace the active shard config.
28. Metaserver Raft snapshot admin routes are now wired through the Raft-backed metadata runtime:
    `GET /meta/snapshot`, `POST /meta/snapshot/save`, `POST /meta/snapshot/load`, and
    `POST /meta/snapshot/restore` operate on the Raft leader's committed metabase state instead of
    returning `raft_snapshot_unsupported`.
29. Data-node distributed Raft now exposes C++ `RaftControlService`-style process endpoints on both
    `raft_node` and raft-enabled `server`: `/raft/control/list_membership`,
    `/raft/control/add_node`, `/raft/control/remove_node`, and
    `/raft/control/trigger_snapshot`. These are backed by the existing safe membership-change path
    and snapshot trigger routine.
30. Data-node distributed Raft control now also exposes C++ `DataRaftConsensusBackend`-style
    `ReadIndex` and `TransferLeader` operations over HTTP: `/raft/control/read_index` and
    `/raft/control/transfer_leader` are available on both `raft_node` and raft-enabled `server`,
    with lagging/non-live target rejection delegated to the Raft cluster checks.
31. Server binary now exposes C++ `ServerService::Ping` parity through `GET/POST /ping` and
    `GET/POST /ServerService/Ping`, returning the common OK status used by the Rust HTTP surface.
32. Server binary now also exposes C++ `ServerService`-named JSON aliases for the partition-manager
    surface already implemented by Rust: `/ServerService/Load`, `/ServerService/Unload`,
    `/ServerService/ExecuteCmd`, `/ServerService/BatchExecuteCmd`, `/ServerService/SetConfig`,
    `/ServerService/GetConfig`, `/ServerService/GetInfo`, `/ServerService/GetStats`,
    `/ServerService/ReadPartitionStream`, `/ServerService/ScanPartitionStream`, and
    `/ServerService/UpdateMembership`.
33. The real OS-process `raft_secondary_replication_harness` now exercises the distributed
    `/raft/request_vote` peer route: it sends an authenticated stale-candidate vote request that
    must be rejected with `candidate_log_behind`, then sends an authenticated caught-up-candidate
    request that must be granted.
34. Server binary now exposes the remaining C++ `ServerService::ApplyDataRaftLog` RPC shape as
    `POST /ServerService/ApplyDataRaftLog`. The route uses the existing C++-style data-Raft log
    codec and committed-log applier, tracks per-shard applied raft/oplog indexes, and treats
    duplicate raft indexes as idempotent no-ops.
35. Distributed Raft peer receive paths now enforce RPC metadata auth at the process boundary:
    `raft_node`, raft-enabled `server`, and the local distributed harness use the authenticated
    HTTP handler for AppendEntries, RequestVote, InstallSnapshot, and snapshot chunks. The
    OS-process harness now verifies wrong-token `/raft/request_vote` is rejected before normal vote
    handling.
36. Metaserver now exposes C++ `MasterService`-named JSON aliases for implemented control-plane
    operations: `/MasterService/CreateTable`, `/MasterService/OpenTable`,
    `/MasterService/CloseTable`, `/MasterService/GetTableTopo`,
    `/MasterService/RegisterServer`, `/MasterService/UnRegisterServer`,
    `/MasterService/DeleteTable`, and `/MasterService/UpdateTable`. The route test covers
    C++ host/port registration, table creation via `table_options.partition_num`, open/table-topo
    version semantics, update-table topology changes, close, unregister, and
    table delete/open-after-delete semantics.
37. Readonly replica replay loop now has C++ `Replicator::UpdateRemoteInfo`-style primary route
    change handling for the implemented HTTP stream path: every loop resolves the metaserver route,
    records `primary_route_change_total`, clears stale consecutive-failure backoff state when the
    primary endpoint changes, exposes the counter in Prometheus events, and has a regression test
    that starts with a bad primary route then recovers after the metaserver points to a healthy
    primary.
38. Metaserver table delete lifecycle is now implemented instead of returning an alias-only 404:
    `DeleteTableRequest` is a durable metadata mutation, `/tables/delete` and
    `/MasterService/DeleteTable` mark tables as `Dropped`, namespace table counts exclude dropped
    tables, topology/open calls reject dropped tables with `table_not_found`, and mutation-log plus
    MetaRaft tests cover replay and replication.
39. Metaserver table update/alter lifecycle is now implemented for the open-source topology model:
    `UpdateTableRequest` is a durable metadata mutation, `/tables/update`, `PATCH /tables`,
    `/MasterService/UpdateTable`, and `/MasterService/AlterTable` can expand shard count and change
    replica count, bump topology version, replay from the mutation log, and replicate through
    MetaRaft. Unsafe C++-layout changes such as shrinking shard count or changing derived
    partition-id fields are rejected.
40. Data-node Raft external snapshot bootstrap is now exposed at the process boundary:
    standalone `raft_node` and raft-enabled `server` both provide gated
    `POST /raft/admin/publish_external_snapshot` and
    `POST /raft/admin/bootstrap_external_snapshot`, which publish/download S3/MinIO-compatible
    `ShardSnapshotRef`s through the snapshot-store abstraction, verify manifest/checksum/size and
    stale-local-state guards, install the snapshot into the target replica engine state, record
    the external snapshot ref, and catch up from the leader log. Route tests cover both binaries,
    and the distributed harness now validates HTTP publish -> HTTP bootstrap -> follower read.
41. A direct scan of the C++ risk module closed a concrete FOL gap: Rust now has explicit
    `RiskFolSet` / `RiskFolQuery` commands and typed client methods for the C++ `FolSet` /
    `FolQuery` string semantics. `FIRST` keeps the value with the smallest event timestamp,
    `LAST` keeps the value with the largest event timestamp, TTL/delete/index persistence are wired
    through the shard state, and RESP now supports `FOLSET key value occur_time_ms ttl_ms
    FIRST|LAST` plus `FOLQUERY key`. The older numeric `RiskFamily::Fol` shim remains for local
    compatibility tests.

This pass compared the Rust RESP layer against the local C++ server Redis command handler in
`C:\Users\Vincent Jiang\Documents\Codex\temporalstore-small\src\server\redis_service.cc` and
`redis_command_handler.cc`. It closed these small, test-backed compatibility gaps:

1. Stateful Redis operational command context for connection-local admin behavior.
2. `CONFIG GET`, `CONFIG SET`, and `CONFIG REWRITE`.
3. `AUTH` against `requirepass`.
4. `SLAVEOF host port` and `SLAVEOF NO ONE`, surfaced through `INFO replication`.
5. `INFO` sections for server, clients, memory, stats, replication, and cluster.
6. `BGSAVE` smoke response.
7. `PARTITION LOAD`, `PARTITION UNLOAD`, and `PARTITION INFO` smoke behavior for local tests.
8. Slot/hash helpers `PSLOTHASHKEY`, `PCLUSTERKEYSLOT`, and `PCLUSTERHASH` using the same
   C++ CRC64-derived slot formula already used by the Rust client router.

These are Redis/admin compatibility shims for local tooling and smoke tests. They do not replace
the C++ partition manager implementation behind those commands.

This continuation compared the Rust hash module with
`C:\Users\Vincent Jiang\Documents\Codex\temporalstore-small\src\extension\hash\test.cc` and
closed the C++ `INCRBY` edge-case behavior:

1. Existing non-integer hash values now return an `unmatched` error instead of being treated as `0`.
2. Arithmetic overflow and underflow now return `out_of_range` instead of saturating.
3. The RESP `HINCRBY` path surfaces these errors as Redis error replies.

This pass compared proxy, client, data-node, and metaserver surfaces again and closed four more test-backed control-plane gaps:

1. Proxy heartbeat auto-register: `ProxyService::heartbeat_to_meta()` sends heartbeat, registers the proxy on `not_found`, then retries heartbeat.
2. Client table topology sync: `TemporalStoreClient::open_table_from_meta()` fetches table topology from metaserver and derives `TableOptions`.
3. Data-node cancellation surface: `DataNodeRuntime::cancel_job()` reports queued cancellation, in-flight, already-finished, and not-found job states.
4. Metaserver stale heartbeat detection: `SingleNodeMeta::freeze_stale_servers()` freezes normal servers whose heartbeat age exceeds a threshold, with an HTTP route at `/servers/freeze_stale`.
5. Metaserver safe-mode cooldown: stale server/proxy freeze now records `frozen_since_ms` and
   `freeze_cooldown_until_ms`, rejects re-register/heartbeat while the cooldown is active, persists
   through the local mutation log and metabase snapshots, exposes `GET /meta/safe_mode`, and is
   available through the Raft-backed metaserver facade.
6. Table serving-option catalog: metaserver table metadata now stores and patches C++-style serving
   knobs for pin-primary, replica-read policy, preferred location, deterministic drop percent,
   read/write retry counts, retry backoff, continuous-failure window, and connect/I/O timeouts.
   These options are returned in topology, persist through snapshots/mutation paths, are accepted by
   the `MasterService` `table_options` JSON alias, and are applied by the Rust client when opening a
   table from metaserver topology.
7. Feature/sequence filters now cover inclusive C++ boundary operators: typed filters and Redis
   `FQUERYFILTER`/`FQUERYFILTERSTR` accept `>=`/`<=` plus `GE/GTE/LE/LTE`, and sequence/feature
   query execution applies those inclusive predicates.

This pass repeated the C++ vs Rust comparison again, focusing on the bigger distributed gaps. It closed eight more test-backed surfaces:

1. Raft transport contracts: added `AppendEntriesRequest/Response`, `VoteRequest/Response`, `InstallSnapshotRequest/Response`, and a `RaftTransport` trait.
2. Raft hard state: exposed `RaftHardState` with term, voted-for, and commit index.
3. Raft membership state: exposed `RaftMembership` with shard id, voters, and leader id.
4. AppendEntries local receive path: rejects stale terms and log mismatches, applies committed entries to lagging replicas.
5. RequestVote local receive path: rejects stale terms, already-voted conflicts, and candidates with behind logs.
6. InstallSnapshot transport envelope: builds and handles snapshot transfer requests through the transport trait.
7. Shared-store replay safety: `replay_oplog_strict` rejects oplog gaps instead of silently skipping them.
8. Load-aware metaserver placement: topology replica selection now prefers lower key/memory load normal servers.

The Rust code now has the message/API contracts, local safety behavior, auto-persisting local WAL mode, separate-node runtime wrapper, authenticated RPC runtime construction, timer supervisor, and multi-process chaos plan validation. It is still blocked from production readiness until real OpenRaft/raft-rs FSM/storage integration, actual mTLS transport, real snapshot install, and external process chaos tests exist. This is surfaced by `distributed_raft_readiness()` and documented in `docs/distributed_raft_readiness.md`.

This repeated audit split the old broad readiness bucket into explicit gates for data-node
distributed Raft, fault tolerance, and scale testing. The Rust code remains test-green for the local
model, but production readiness is still blocked by:

1. real OpenRaft or raft-rs data-node FSM/storage implementation
2. production durable Raft log store with segment/sync/truncation policy
3. networked metaserver-driven shard membership changes for data-node Raft groups
4. Raft snapshot install connected to `TemporalEngine` freeze/flush/download/install
5. multi-process chaos that kills and restarts real OS processes under partition, disk-full, and slow-follower conditions
6. AWS multi-node scale tests with p50/p95/p99, CPU, memory, disk, and network reporting

These blockers are now surfaced in `production_readiness_report()` under:

- `data_node_distributed_raft`
- `fault_tolerance`
- `scale_testing`

The same `ProductionReadinessReport` is now exposed by every primary Rust service process through
`GET /readiness` and `GET /cpp_parity` on the proxy, data node, and metaserver. These endpoints are
intentionally operational parity gates: they return `cpp_parity_ready=false` until the remaining C++
production blockers above are closed, instead of treating local model coverage as full parity.

This pass closed one concrete feature/raft gap found by scale testing:

- A 5k-row `SequenceAdd` command is larger than the default ByteRaft-style `32 KiB`
  `max_memory_replicate_log_bytes` in the Rust JSON command shape.
- `RaftCluster::propose` and `RaftCluster::propose_distributed` now split oversized
  `SequenceAdd` commands into ordered smaller Raft entries before appending.
- Regression tests cover both the local and distributed propose paths under the default entry limit.
- The remaining production gap is transactional chunk-group semantics if a caller requires the
  entire logical sequence append to commit all-or-nothing across multiple Raft entries.

This pass repeated the C++ vs Rust audit and closed eight smaller, test-backed parity gaps:

1. IPS range query: `IpsQueryRange` returns timestamp-ordered instances within a time window.
2. IPS batch query: `IpsBatchQueryLast` returns last-N instance groups for multiple keys.
3. Risk first/last aggregation: `RiskQuery` now accepts `first` and `last`.
4. Risk detail list: `RiskDetail` returns timestamped risk events for a time window.
5. Storage admission control: shard `maxmemory_bytes` now rejects new writes after the local storage budget is reached.
6. C++-style partition stats shape: `ShardStats` now includes loaded state, readonly flag, load version, total records, and storage bytes.
7. Client backend failure pool shape: `ClientStats` now tracks backend error streaks and successful retry recovery.
8. Proxy heartbeat/report shape plus stream safety: `ProxyHeartbeatReport` exposes config/stats/boot data, and invalid stream ranges now return `invalid_stream_range`.

These are open-source Rust API and behavior increments. They are not brpc/thrift wire compatibility.

This pass closed another ByteRaft/ByteKV `RaftEngine` API/configuration gap:

- `RaftConfig` exposes the ByteKV/ByteRaft-style knobs used by the local C++ codebase: election cycle ticks, leader-transfer timeout, offline timeout, lease settings, startup lease assumption, max memory replicate-log bytes, max disk replicate-log count, max cache memory bytes, max apply batch bytes, reorder queue controls, in-flight apply/replication limits, pre-vote/election prohibition, snapshot send timeout, transport timeout, WAL sync, segment sizing, retained segment count, snapshot trigger switch, and max applied-log bytes.
- Defaults match the local C++/ByteKV defaults where the C++ source exposed them: election cycle `3`, max memory replicate-log bytes `32 KiB`, max disk replicate-log count `64`, max cache memory bytes `32 MiB`, max apply batch bytes `64 KiB`, raft transport timeout `1000 ms`, max segment bytes `64 MiB`, WAL sync disabled, and max applied-log bytes `1 GiB`.
- `RaftConfig::validate()` rejects unusable settings before constructing a data-node or metaserver Raft group.
- `RaftReadOptions` and `RaftReadStrategy` model the C++ `ReadOptions` shape: relaxed read, lease read, read-index, follower-read switch, fill-cache flag, ignore-write-intent flag, and wait timeout.
- `DataRaftReadPolicy` models the C++ data-node `data_raft_read_mode` gflag surface:
  `leader`, `linearizable`, `bounded_stale`, and `unsafe_any_replica`, including the
  bounded-stale max index lag guard for secondary reads.
- data-node Raft and metaserver Raft now both support fallible constructors with explicit config, `config()` inspection, log-entry size enforcement, leader-only read enforcement, optional follower reads, and election prohibition.
- data-node Raft WAL now persists in-progress joint-consensus membership and restores it after restart, so a restarted group still requires both old and new majorities before writes or membership commit.
- WAL-backed data Raft clusters now auto-persist committed writes, leadership changes, membership changes, catch-up, RPC receive state, and snapshot installs; callers no longer need to remember a manual `persist_wal()` call in that mode.
- WAL-backed data Raft clusters now compact old local WAL records using `RaftConfig.max_disk_replicate_log_num`, preserving the latest recoverable state without unbounded JSONL growth in the local model.
- WAL-backed data Raft clusters now use segmented local WAL persistence with sync, bounded segment retention from `RaftConfig.min_keep_segment_num`, ordered recovery, and corrupt-tail truncation while still reading the older single-file WAL format.
- WAL-backed data Raft clusters now persist installed snapshot payload and snapshot-index floor in WAL records, so a restarted replica can recover trimmed pre-snapshot state without replaying old log entries.
- data-node and metaserver Raft election paths now reject stale candidates whose logs are not up-to-date with a voting majority before leadership can move.
- data-node Raft `RequestVote` receive path now follows Raft term monotonicity more closely: higher terms update local hard state and clear old votes before grant/reject decisions, including the candidate-log-behind rejection path.
- data-node Raft now exposes a safe membership-change unit for add/remove/replace voter plans: it validates target quorum, opens joint consensus, catches up live followers, commits the new voter set, aborts on failure, and returns a scheduler-friendly report with old/new voters and deltas.

This pass repeated the client/proxy/data-node/metaserver surface comparison and closed eight
operational parity gaps without adding brpc/thrift:

1. Client now exposes a serializable `ClientPreflightReport` for route/table cache size,
   backend-failure backlog, stats, options, and degraded reasons.
2. `ClientOptions`, `TableOptions`, `ReplicaReadPolicy`, and `ClientStats` are serializable so
   service preflight/admin responses can carry client state directly.
3. Proxy exposes C++-named admin aliases for info and heartbeat:
   `/ProxyService/GetInfo` and `/ProxyService/Heartbeat`.
4. Proxy exposes C++-named config and client-health aliases:
   `/ProxyService/GetConfig`, `/ProxyService/UpdateConfig`, and
   `/ProxyService/ClientPreflight`.
5. Data-node server exposes C++-named runtime/admin aliases:
   `/ServerService/GetRuntimeStats` and `/ServerService/Preflight`.
6. Data-node server exposes C++-named state-inspection aliases:
   `/ServerService/ListDirtyObjects` and `/ServerService/ListQueuedShardWorkers`.
7. Metaserver exposes `/meta/preflight` plus `/MasterService/Preflight` and
   `/MasterService/GetInfo` for single-node metadata.
8. Raft-backed metaserver now derives the same preflight report from committed metadata state,
   so the new metaserver preflight aliases work in both backend modes.
- metaserver Raft now exposes the same membership-change plan/apply report shape for add/remove/replace voters, including target-quorum validation, follower catch-up, committed voter reporting, and metadata availability on added voters.

This is API/config parity plus local-model enforcement. It is not yet the actual optimization implementation for reorder queues, inflight replication windows, WAL sync/segments, network transport timeout, or pre-vote. Those fields become operationally meaningful when the in-process model is replaced by OpenRaft/raft-rs plus durable WAL and transport.

The previous pass closed a ByteRaft/ByteKV `RaftEngine` behavior gap:

- `RaftClusterStatus` and `RaftNodeStatus` for leader, term, commit index, majority, live voters, lease-valid state, per-node lag, and per-node role
- `ReadIndexResponse` and `read_index(node_id)` for safe local-model reads; lagging replicas are rejected before serving read-index
- `transfer_leader(node_id)` for both data Raft and metaserver Raft; lagging or dead targets are rejected
- `local_status(node_id)` matching the shape of ByteRaft's local status inspection
- `prometheus_metrics()` for data Raft and metaserver Raft with commit index, live voters, majority, lease validity, per-node commit, lag, and liveness
- tests for data-Raft status/read-index/leader-transfer, lagging replica rejection, and metaserver-Raft status/read-index/leader-transfer
- `RaftApplyHealth` / `RaftApplyLag` for data-node and metaserver Raft, plus
  `temporalstore_raft_node_applied_index` and `temporalstore_raft_node_apply_lag` metrics
- data-Raft nonzero `lease_duration_ms` now has deterministic local-model enforcement: a logical
  clock can expire the leader lease, `status.leader_lease_valid` flips false, read-index and writes
  reject with `LeaderUnavailable`, and heartbeat/election/commit renews the lease.

This is still an in-process correctness model, not a replacement for OpenRaft/raft-rs with durable WAL and network transport.

This C++ parity pass closed the next data-node replay gap:

- `ReplicaReplayLoop` installs index bytes and page segments from the primary stream source.
- It replays index-log records separately from oplog records, in C++ secondary catch-up order.
- It persists a cursor with checkpoint install state, copied page segments, index-log byte
  offset/sequence, and oplog byte offset/sequence.
- Resume is idempotent and test-backed: a second run applies only new tail records, and a third run
  applies nothing.
- Replay rejects index-log or oplog sequence gaps before the follower is considered caught up.
- The replay loop now accepts either an in-process engine source or an HTTP remote stream source,
  so the Rust secondary catch-up path can consume another server's stream APIs instead of requiring
  direct access to the primary engine.
- The server binary now exposes `/replica/replay`, persists cursors under
  `TS_REPLICA_REPLAY_CURSOR_DIR` or the index directory, and returns a replay report/status payload.
- `TS_REPLICA_REPLAY_PRIMARY_ADDR` plus `TS_REPLICA_REPLAY_INTERVAL_MS` enables continuous
  server-side replay without manual requests.
- If `TS_REPLICA_REPLAY_PRIMARY_ADDR` is empty, the loop discovers the current primary from the
  metaserver shard route and uses that server as the remote stream source.
- The background loop now reports replay attempts/successes/failures/skips and backs off failed
  attempts up to `TS_REPLICA_REPLAY_MAX_BACKOFF_MS` instead of hammering an unavailable primary.
- The server binary now honors startup load metadata through `TS_TABLE_NAME`, `TS_SHARD_URI`,
  `TS_SHARD_LOAD_VERSION`, `TS_SHARD_START_ROUTING_SLOT`, `TS_SHARD_END_ROUTING_SLOT`,
  `TS_SERVER_NODE_ID`, and `TS_SHARD_READONLY` / `TS_SERVER_READONLY`, so a process can boot as a
  readonly replaying secondary instead of always loading a writable shard.
- The server `/update_membership` path now reports successful local membership installation back to
  metaserver with server address, shard id, load version, and status, matching the C++ callback
  control-plane shape at the HTTP/JSON layer.
- The engine now enforces existing `read_qps` and `write_qps` config fields with a one-second
  per-shard admission window, returning `admission_rejected` when the local limit is exceeded.
- A careful scan of `temporalstore-small/src/extension/common/{interface.proto,implement.cc,test.cc}`
  found that C++ `EXPIRE` rejects missing keys. Rust now enforces the same engine precondition and
  keeps Redis-compatible `EXPIRE missing` behavior as `0`.

This pass also widened data-node heartbeat/load-report parity:

- `ServerHeartbeatRequest` carries `partition_loads` beside compact `shard_loads`.
- `PartitionLoad` embeds the existing C++-style `PartitionInfoStats` shape.
- The server binary sends per-partition table name, shard URI, load version, readonly state,
  routing-slot range, storage bytes, object/page-ref counts, and dirty object/slot counts.
- The metaserver stores and returns this richer load payload from `list_servers()`.

## What Was Closed In The Previous Pass

This pass closed several behavior gaps without carrying brpc or Thrift forward:

- atomic string conditional write command: `StringSetConditional` with `always`, `if_exists`, and `if_not_exists`
- Redis `SET` options: `NX`, `XX`, `GET`, `EX`, and `PX`
- IPS operations beyond append/query-last: remove by timestamp, delete key, and count by time range
- Risk aggregation query: sum/count, min, max, and event count over a time window
- Prometheus `/metrics` on the data-node server for shard records, cache, page-store, oplog, and runtime queue/job/dirty-object stats

Tests added for these behaviors:

- `string_set_conditional_supports_nx_xx_and_get`
- `ips_remove_delete_and_count_are_supported`
- `risk_query_supports_sum_min_max_and_event_count`
- `prometheus_metrics_include_records_cache_page_and_oplog`
- Redis adapter assertions for `SET NX`, `SET XX GET`, and `SET NX PX`

## What Was Closed Earlier

Rust now has an `IndexLog` stream and a more production-shaped HTTP proxy:

```text
write command -> append oplog -> write page bytes -> append index-log record -> persist whole index
client -> proxy route cache -> metaserver route lookup on miss/stale -> data node -> refresh route after backend error
```

Files:

- `crates/temporalstore-rust/src/index_log.rs`
- `crates/temporalstore-rust/src/control.rs`
- `crates/temporalstore-rust/src/engine.rs`
- `crates/temporalstore-rust/src/proxy.rs`
- `crates/temporalstore-rust/src/bin/proxy.rs`
- `crates/temporalstore-rust/src/lib.rs`

This closes one data-node replay gap from the previous audit. C++ has binary index logs with page/object metadata. Rust now has a JSONL index-log record after every mutation. It is larger and simpler than C++, but gives replica/recovery code a separate stream of committed index metadata instead of relying only on the latest whole index file.

The C++ proxy is still much richer, but Rust no longer blindly looks up the metaserver on every forwarded request. The Rust proxy now owns a reusable `ProxyService` with route caching, forced refresh, backend retry/refresh behavior, basic runtime config, and observable request/cache/error counters.

The Rust client also now has a small table router: `key -> stable hash -> shard_id`. Pipelines split queued commands by routed shard and reassemble responses in original order, matching the shape of the C++ client batch path at a simpler HTTP/shard level.

The Rust metaserver now exposes a richer HTTP/JSON control-plane skeleton: server/proxy inventory, heartbeat liveness, namespace/table metadata, and table topology with slot ranges, shard ids, primary endpoint, replicas, and topology-version not-modified behavior.

The Rust data node now exposes C++-style checked execution routes that reject stale `load_version` requests, and the server binary reports loaded shard stats to the metaserver heartbeat endpoint.

The Rust data node also has a first runtime layer around `TemporalEngine`: worker threads, shard-affine partition lanes, bounded foreground/background queue admission, foreground-over-background scheduler priority, async job handles, dirty-object ids for mutating commands, and explicit dump/compact/GC task hooks.

## Detailed Gap Matrix

## June 12 LOC Comparison Note

The Rust tree is smaller because it does not yet include the full first-party
C++ service surface or the vendored dependency surface.

Measured locally against `/root/src/github-services/TemporalStore-main-no-deps`
and the Rust `rust-main` branch:

- C++ TemporalStore first-party service code: about 96,293 LOC across 566
  C/C++/proto/thrift files.
- C++ byteraft dependency: about 37,629 LOC.
- C++ byte dependency: about 65,828 LOC.
- Rust TemporalStore code: about 47,034 total Rust LOC, with about 23,971
  non-test Rust LOC after subtracting test/harness code.

The largest C++ first-party areas are partition/storage, model modules,
metaserver, client, stream, server, and extensions. The Rust implementation has
substantial local models for those areas, but still omits large production
pieces such as full ByteRaft/byte integration, object-manager/page-layout
internals, production model internals, brpc/thrift SDKs, dashboards, and
deployment automation.

This pass closed one concrete Risk module gap from the C++ `HSET`/`HQUERY`
`CHANGE` path. C++ stores a caller-supplied value as a distinct field and query
counts unique fields over a window. Rust now has `RiskChangeAdd`, typed client
support, RESP `RISKCHANGE`/`HCHANGE`, and `RiskQuery`/`RiskFamilyQuery` with
aggregator `change` counting unique values across the requested window.

The follow-up pass closed a generic object-lifecycle mismatch caused by Rust's
internal risk-family keys. C++ common `DEL_OBJECT`, `EXPIRE`, `TTL`, and Redis
`DEL`/`EXPIRE`/`TTL` operate on the user-visible object key. Rust now expands a
logical user key to the associated H/CPC/FOL risk-family records for existence,
delete, expire, and TTL handling, so lifecycle operations do not leave hidden
`risk:h:<key>`, `risk:cpc:<key>`, or `risk:fol:<key>` records behind.

The storage pass compared C++ `src/partition/storage`, `src/partition/index`,
`src/stream`, and `src/blockcache` with the Rust page/oplog/index/shared-store
modules. It closed page-store integrity gaps: new Rust `PageAddress` records now
carry optional SHA-256 checksums, newly appended page bytes are wrapped in a
self-describing segment record envelope with magic/version/length/checksum
fields, durable page reads verify the envelope and checksum when present, old
raw segment entries remain readable, appends flush and sync the segment data,
and segment installation writes through a synced temp file before atomic rename.
This is still much smaller than C++ zones, protobuf page headers,
object-manager metadata, and block-cache integration, but it prevents silent
local page corruption for newly written Rust pages and lets raw segment scans
identify Rust page record boundaries. Engine page-stream reads preserve the
existing C++-compat logical payload-offset contract by walking those envelopes
and returning payload bytes.

Follow-up storage parity passes compared the remaining C++ storage surface in
eight smaller loops:

1. C++ `OpLogger::Commit` and `AppendReplayedLog` commit through the stream
   layer. Rust oplog and index-log appends now flush and `sync_data`, and their
   GC rewrites sync the temp file before rename.
2. C++ `PageStore::PrepareNewZone` persists index metadata around stream
   creation. Rust page-segment rolling now syncs the new segment file and parent
   directory, and installed segment renames sync the parent directory.
3. C++ page reads integrate blockcache behind `ReadPage`. Rust's local disk
   cache remains simpler, but disk block writes now use a synced temp file and
   atomic rename so readers do not observe partial block files.
4. C++ page format includes protobuf `PageHeader` metadata with model/object/page
   ids and optional compression. Rust now stores a lightweight page record
   envelope in the segment bytes, but the protobuf header/object-id format and
   compression policy remain missing.
5. C++ `ObjectManager::Load` replays committed oplog records into hot slot/object
   state after loading dumped pages. Rust has shared-store replay and strict gap
   rejection, but not the same slot-aware hot object replay pipeline.
6. C++ `PageGc` selects low-utility frozen zones and delays destruction after
   recycle. Rust GC can retain current/live segments and delete stale local
   segments, but it does not yet implement utility scoring or delayed zone
   destruction.
7. C++ streams support local files, shared files, ByteStore, and S3-style
   backends behind one stream layer. Rust has file-backed shared-store checkpoints
   and oplogs, but not the live ByteStore/S3 stream backend abstraction.
8. C++ dump/load policy coordinates slot store, page store, index log, oplog, and
   object manager. Rust checkpoints restore index/pages and replay oplog tails,
   but the full freeze/flush/load lifecycle is still a P0 gap.

Production-readiness storage pass repeated the comparison in ten narrower loops:

1. C++ `PageStore::UpdateZones` reopens existing zones from index metadata.
   Rust now reopens the highest existing local page segment instead of always
   selecting segment `0`.
2. C++ restore/open flows keep the writable stream aligned with restored zone
   state. Rust `install_segment` now treats an installed higher segment as the
   writable current segment and resumes appends at that segment length.
3. C++ page stream bytes carry headers. Rust now writes a lightweight envelope
   for new pages while keeping logical page-stream reads payload-offset based.
4. C++ raw stream tooling can still inspect stream bytes. Rust keeps
   `read_range` and `read_segment` as physical byte reads for checkpoint/debug
   callers.
5. C++ page reads reject malformed page metadata. Rust rejects corrupt envelope
   headers and payload checksum mismatches.
6. C++ storage reopen is validated by stream tests. Rust now has tests for
   reopen-after-roll appends and cross-record logical page-stream reads.
7. C++ page restore is tied to zone state. Rust now tests that restored higher
   segments become the future append target.
8. C++ object manager is still slot/object-id aware. Rust remains map/address
   based and still lacks stable object/page ids.
9. C++ storage GC is utility/delay based. Rust still only has conservative local
   segment retention by floor/current/live refs.
10. C++ production readiness still depends on integrated freeze/flush/load,
    ByteStore/S3 streams, operational metrics, and crash/fault validation; Rust
    is improved locally but should not be called fully production-ready yet.

Feature-parity storage pass repeated the comparison in six narrower loops:

1. C++ page headers carry stable page identity; Rust page record envelopes now
   persist a monotonic `page_id` and return it in `PageAddress`.
2. C++ reopen does not reuse committed page ids; Rust now scans existing v2 page
   envelopes on reopen and resumes allocation after the highest persisted id.
3. C++ restore/install preserves source page identity; Rust `install_segment`
   now scans installed v2 segments and advances the future page-id allocator.
4. C++ reads reject mismatched page metadata; Rust now rejects reads when an
   address page id disagrees with the record envelope page id.
5. C++ old page streams remain readable during format evolution; Rust keeps v1
   envelopes and legacy raw/no-checksum pages readable while writing v2 records.
6. C++ object headers still include richer object ids and slot/zone ownership;
   Rust now has page ids but still lacks full object-id, zone, and merged dump
   parity.

Repeated feature-parity storage pass focused on object identity:

1. C++ page headers identify the logical object that owns a page. Rust v3 page
   envelopes now carry an optional stable `object_id`.
2. C++ object identity is stable across process restarts. Rust derives object
   ids from stable shard/type/key/component identities before writing pages.
3. C++ hash fields and time-series points are distinct logical objects. Rust now
   includes hash fields and feature/sequence/IPS timestamps in the object-id
   source string.
4. C++ page reads reject mismatched header metadata. Rust now rejects an address
   whose `object_id` disagrees with the envelope `object_id`.
5. C++ format evolution keeps older pages readable. Rust keeps v1/v2 envelopes
   and legacy raw pages readable while writing v3 records for new durable pages.
6. C++ still owns slot/page/zone lifecycle through ObjectManager. Rust now has
   stable page ids and object ids, but still lacks zone ownership, dirty-slot
   dump policy, and protobuf header compatibility.

Production-readiness repeat focused on slot ownership in ten loops:

1. C++ page metadata is tied to ObjectManager slot ownership. Rust v4 page
   envelopes now carry an optional `routing_slot` for newly written durable
   pages.
2. C++ routes objects through the shard's owned slot range. Rust now passes the
   loaded shard routing range into the engine write path and stamps the computed
   owned slot into `PageAddress`.
3. C++ hash fields and time-series points are owned by the base record key's
   slot. Rust uses the base key for `routing_slot` while keeping field/timestamp
   in the stable object id.
4. C++ reads reject metadata mismatch. Rust now rejects a page read when address
   routing-slot metadata disagrees with the page envelope.
5. C++ format evolution does not strand old pages. Rust keeps v1/v2/v3 envelope
   and raw page compatibility while writing v4 records for new durable writes.
6. C++ partition stats expose routing-slot ownership. Rust already reports
   routing-slot ranges and dirty-slot counts; this pass connects page metadata to
   the same range.
7. C++ compaction rewrites live pages while preserving object ownership context.
   Rust compaction now carries `PageAddress` metadata forward through normal
   reads/writes, though it still rewrites through local append-only segments.
8. C++ zone GC uses richer utility/delay policy. Rust still has floor/current/live
   segment retention rather than utility-based zone cleanup.
9. C++ page headers are protobuf-compatible and tied to full object/page ids.
   Rust headers are self-describing Rust envelopes, not protobuf wire-compatible
   headers.
10. This improves production readiness for local storage integrity, but full C++
    readiness still requires a deeper object/page/slot storage lifecycle,
    merged dump/load, real Raft integration, and crash/fault validation.

Production-readiness repeat focused on zone identity in ten loops:

1. C++ stores pages in zones. Rust still uses append-only local page segments,
   but new durable pages now stamp a `zone_id` that maps to the owning page
   segment.
2. C++ page metadata can reject zone/address inconsistencies. Rust now rejects a
   page read when address zone metadata disagrees with the envelope zone id.
3. C++ zone roll changes the future page target. Rust segment roll now also
   changes the stamped zone id for future durable appends.
4. C++ object/page/slot metadata travels together. Rust v5 page envelopes now
   carry page id, object id, routing slot, and zone id together.
5. C++ old zones remain readable across format evolution. Rust keeps v1-v4
   envelopes and legacy raw pages readable while writing v5 records.
6. C++ compaction rewrites live pages into newer storage areas. Rust compaction
   continues to rewrite live pages into fresh segments, and those rewritten
   pages get fresh zone ids through the normal append path.
7. C++ zone GC is policy-driven. Rust still lacks utility-based zone selection,
   delayed destroy, and merged dump/load zone policy.
8. C++ page headers are protobuf-compatible. Rust headers remain Rust-native
   self-describing envelopes, not protobuf wire-compatible page headers.
9. C++ ByteStore/S3 stream backends provide production storage targets. Rust
   remains local-file/shared-store modeled for this path.
10. This closes zone identity metadata only; it does not claim full C++ zone
    manager parity.

Production-readiness repeat focused on page compression in ten loops:

1. C++ storage can apply compression below the page/block layer. Rust page
   records now compress large page payloads with zstd when the compressed bytes
   are smaller than the original bytes.
2. C++ separates logical page bytes from physical storage bytes. Rust v6 page
   envelopes now persist both logical payload length and physical stored length.
3. C++ page checksums validate the logical content. Rust keeps SHA-256 over the
   original uncompressed page bytes and validates it after decompression.
4. C++ page-stream reads operate over logical page bytes. Rust logical-range
   reads now decompress each v6 record before slicing across record boundaries.
5. C++ format evolution does not strand older pages. Rust keeps legacy raw
   pages plus v1-v5 envelopes readable while writing v6 records for new durable
   appends.
6. C++ storage rejects corrupt page data. Rust now reports corrupt compressed
   payloads through checksum or envelope corruption errors.
7. C++ object/page/slot/zone metadata remains attached to compressed pages.
   Rust v6 envelopes keep page id, object id, routing slot, and zone id beside
   the compression metadata.
8. C++ production storage exposes richer compression configuration. Rust uses a
   conservative built-in policy rather than C++-style policy knobs.
9. C++ page headers are protobuf-compatible. Rust headers remain Rust-native
   self-describing envelopes, not protobuf wire-compatible page headers.
10. This closes a concrete compression gap, but full C++ storage parity still
    needs the zone manager, delayed destroy, ByteStore/S3 backend integration,
    and full merged dump/load policy.

Production-readiness repeat focused on page compression observability in ten loops:

1. C++ production storage exposes physical storage behavior separately from
   logical page bytes. Rust page-store stats now track both physical bytes and
   logical uncompressed bytes.
2. C++ operators can tell whether compression is active. Rust page-store stats
   now count compressed records written and read.
3. C++ storage capacity work depends on compression savings. Rust page-store
   stats now expose bytes saved by compressed page records.
4. C++ metrics surface storage internals for production diagnosis. Rust
   Prometheus metrics now export page-store compressed read/write counters.
5. C++ metrics distinguish logical and physical accounting. Rust Prometheus
   metrics now export logical written/read bytes and compression-saved bytes.
6. C++ page-stream reads can cross compressed records. Rust logical range reads
   now count compressed records touched while preserving logical byte results.
7. C++ debug physical reads remain physical. Rust keeps `read_range` and
   `read_segment` as physical-byte APIs rather than mixing them into logical
   byte counters.
8. C++ old storage formats remain observable after format changes. Rust keeps
   old raw/v1-v5 reads valid while reporting compression stats only for v6 zstd
   records.
9. C++ production policy still has richer zone/cache/storage metrics. Rust
   still lacks zone utility, delayed destroy, and ByteStore/S3 stream metrics.
10. This improves storage operability after compression, but does not claim
    full C++ production storage parity.

Production-readiness repeat focused on page compression policy in ten loops:

1. C++ production storage can tune compression behavior. Rust page-store
   construction now accepts `PageStoreOptions` for compression enablement,
   minimum compression size, and zstd level.
2. C++ operators can disable compression for compatibility or diagnosis. Rust
   now writes v6 page envelopes with the no-compression codec when compression
   is disabled.
3. C++ storage can avoid tiny-record compression overhead. Rust now uses a
   configurable minimum-byte threshold instead of a hard-coded policy only.
4. C++ compression policy still preserves page integrity. Rust policy changes
   keep logical SHA-256 checksums over original page bytes.
5. C++ reads remain format-driven. Rust compressed and uncompressed v6 records
   remain readable from envelope metadata without needing the writer policy.
6. C++ metrics should reflect policy outcomes. Rust compressed-record and
   compression-saved counters stay zero when policy prevents compression.
7. C++ production systems bound codec levels. Rust clamps zstd level into the
   supported range before encoding.
8. C++ policy is integrated through config/control planes. Rust has a local
   constructor-level policy, not yet metaserver or runtime reconfiguration.
9. C++ page headers are still protobuf-compatible and zone-aware. Rust remains
   Rust-envelope and segment based.
10. This closes the hard-coded compression-policy gap, but not C++ zone manager,
    policy distribution, delayed destroy, or merged dump/load parity.

Production-readiness repeat focused on page compression runtime wiring in ten loops:

1. C++ storage policy is available from service startup paths. Rust server
   startup now reads page-store compression policy from environment variables.
2. C++ page compression can be disabled per deployment. Rust server startup now
   honors `TS_PAGE_STORE_COMPRESSION_ENABLED`.
3. C++ can tune when compression starts. Rust server startup now honors
   `TS_PAGE_STORE_COMPRESSION_MIN_BYTES`.
4. C++ can tune codec level. Rust server startup now honors
   `TS_PAGE_STORE_COMPRESSION_LEVEL`.
5. C++ service constructors carry storage policy into the storage layer. Rust
   `TemporalEngine::with_local_dirs_and_page_store_options` now carries
   `PageStoreOptions` into the local page store.
6. C++ policy changes must not change read compatibility. Rust tests now verify
   engine write/read behavior with page compression disabled.
7. C++ production startup policy should be testable. Rust server tests now cover
   the compression environment parser.
8. C++ policy is still distributed through richer config/control systems. Rust
   remains startup/environment driven for this policy.
9. C++ zone and stream layers still provide richer storage control. Rust remains
   append-only local segment based for this path.
10. This closes the local runtime wiring gap only; it does not claim full C++
    distributed storage-policy parity.

Production-readiness repeat focused on delayed page-segment destroy in ten loops:

1. C++ page GC does not have to destroy recycled storage immediately. Rust page
   GC now has an explicit delayed-destroy path that moves stale page segments
   into a local trash area.
2. C++ storage keeps active/live zones separate from destroy candidates. Rust
   delayed destroy removes stale segments from active segment listing while
   retaining current and live-referenced segments.
3. C++ delayed destroy is recoverability oriented. Rust uses atomic rename into
   `.page_segment_trash` before final purge so stale segment bytes remain
   inspectable until purge.
4. C++ storage operators can observe destroy candidates. Rust now exposes
   `delayed_destroy_segment_ids`.
5. C++ destroy eventually purges recycled storage. Rust now exposes
   `purge_delayed_destroy_segments`.
6. C++ GC reports distinguish retention reasons. Rust GC reports now include
   delayed-destroy segment ids alongside removed, live-retained, and
   current-retained ids.
7. C++ storage directory updates must be durable. Rust delayed destroy syncs the
   active directory and trash directory after rename and purge.
8. C++ policy is still utility and age based. Rust delayed destroy is explicit
   local quarantine/purge, not yet utility-score or time-window policy.
9. C++ zones remain richer than Rust page segments. Rust still lacks full zone
   manager metadata and protobuf-compatible page headers.
10. This closes the immediate-delete-only gap, but not C++ utility GC, full zone
    lifecycle, or distributed storage-policy parity.

Production-readiness repeat focused on utility-bounded page GC in ten loops:

1. C++ page GC selects low-utility zones before destroy. Rust page store now
   exposes utility candidates for stale local page segments.
2. C++ GC can limit how much storage is reclaimed in one pass. Rust now has a
   bounded utility GC call with `max_destroy_segments`.
3. C++ low-utility selection avoids live/current storage. Rust utility GC
   excludes the current page segment and live index-referenced segments.
4. C++ utility ordering is deterministic for operators. Rust candidates sort by
   utility score, larger stale segment bytes, then segment id.
5. C++ delayed destroy and utility selection are connected. Rust utility GC can
   route selected segments through delayed-destroy quarantine.
6. C++ no-op GC policy must be safe. Rust utility GC with zero destroy budget
   removes no active segments.
7. C++ GC observability includes candidates and actions. Rust tests cover
   candidate order, selected removals, live retention, and delayed destroy ids.
8. C++ utility scores are richer and zone-aware. Rust uses a simple local
   stale/live/current score, not the full C++ zone heat/utility model.
9. C++ GC policy is distributed and age-aware. Rust still lacks runtime
   control-plane policy and time-window purge policy.
10. This reduces the utility-GC gap, but full C++ zone lifecycle and protobuf
    page header parity remain open.

Production-readiness grouped pass focused on crash/recovery validation:

1. C++ recovery coordinates oplog, index metadata, page streams, and zone state.
   Rust now exposes a `StorageRecoveryReport` that checks the same local
   storage planes together for a loaded shard.
2. C++ reopen must verify that indexed pages are readable. Rust recovery now
   counts total page refs and readable page refs and reports whether all live
   pages can be read from the page store.
3. C++ storage recovery depends on zone metadata. Rust recovery now includes
   active page segment ids, live page segment ids, and durable zone descriptors.
4. C++ metadata streams must survive restart. Rust recovery tests now assert
   reopened oplog and index-log record counts after durable writes.
5. C++ stream data must survive sidecar damage. Rust now rebuilds a missing
   page-zone manifest from existing page segment envelopes and writes the
   manifest back during page-store construction.
6. C++ recovery remains richer: Rust still lacks full object/page/slot replay,
   merged dump/load policy, and external process crash/fault testing.

Production-readiness grouped pass focused on metadata-log corrupt-tail recovery:

1. C++ stream recovery must tolerate a torn final append. Rust oplog recovery
   now scans JSONL records with byte offsets and truncates an incomplete or
   corrupt final record back to the last valid sequence.
2. C++ index metadata recovery must behave the same way. Rust index-log recovery
   now applies the same corrupt-tail truncation before stats, scans, GC, or
   future appends.
3. C++ append after recovery must not skip sequence numbers. Rust tests now
   append after a truncated oplog/index-log tail and verify the next sequence is
   exactly the first missing sequence.
4. C++ metadata GC should not fail because of a previous torn append. Rust runs
   tail recovery before oplog/index-log retain-from-sequence GC.
5. This narrows local crash recovery, but still does not replace external
   process crash testing or full object/page/slot replay from binary C++ logs.

Production-readiness grouped pass focused on process-level storage crash recovery:

1. C++ storage recovery must survive real process loss, not just in-process
   reopens. Rust now has a `storage_crash_harness` binary that writes durable
   data, rolls the page segment, then aborts the OS process.
2. The recovery side runs in a separate process over the same cache/page/index
   directories and verifies both values through the normal engine read path.
3. The integration test asserts the recovered storage report across index bytes,
   oplog count, index-log count, active page segments, live page segments,
   readable page refs, and zone descriptors.
4. This closes a local single-node process-abort recovery gap. It still does
   not replace multi-process Raft/data-node chaos, disk-fault injection, or full
   C++ object/page/slot replay.

Eight-loop C++ gap-fill pass focused on local disk-fault diagnostics:

1. Compared C++ recovery expectations with Rust process-abort coverage: process
   loss was covered, but disk corruption reporting was too coarse.
2. Compared C++ page checksum behavior with Rust page envelopes: Rust rejected
   corrupt pages, but recovery reports only exposed a boolean/count.
3. Compared C++ operator diagnosis needs with Rust observability: Rust now
   records unreadable page refs with segment id, offset, length, and error.
4. Compared index/page consistency handling: Rust recovery now distinguishes
   total indexed page refs from readable refs and lists the unreadable subset.
5. Compared disk-fault harness coverage: Rust `storage_crash_harness` now has a
   `corrupt-page` mode that mutates a persisted page segment after an aborted
   writer process.
6. Compared restart behavior after fault injection: the recover process still
   reports readable keys and identifies the damaged page-backed key path.
7. Compared test gates: integration tests now cover process abort followed by
   page-segment corruption and recovery report validation.
8. Remaining gap: this is local single-node disk corruption diagnostics, not
   multi-process Raft/data-node disk-fault chaos or full C++ object/page/slot
   replay.

Repeat C++ gap-fill pass focused on segment-level page-store inspection:

1. Compared C++ page/zone operator tooling with Rust recovery reports: Rust
   exposed live page-read failures, but not per-segment scan summaries.
2. Compared C++ page header scans with Rust envelopes: Rust now has
   `PageStoreSegmentReport` with physical/logical bytes, page count,
   compressed-record count, page-id range, and first scan error.
3. Compared clean restart diagnostics: `StorageRecoveryReport` now includes the
   page segment reports beside zone descriptors and live page-ref readability.
4. Compared corrupt disk diagnostics: segment inspection records the first
   checksum/envelope error without panicking or hiding healthy earlier records.
5. Compared test gates: unit tests cover clean compressed segment summaries and
   corrupt-record reporting, while the process crash harness asserts the report
   appears in recovered JSON after abort/corrupt/recover.
6. Remaining gap: this improves local first-party inspection, but still does not
   implement C++'s full object manager, slot dump scheduler, or merged dump/load
   policy.

Eighteen-loop storage-specific C++ gap-fill pass focused on object/slot
ownership observability:

1. Compared C++ `ObjectManager` ownership with Rust page segment reports: Rust
   had page counts and byte counts but did not summarize object ownership.
2. Compared C++ slot-aware page layout with Rust routing metadata: Rust stored
   routing slots in envelopes but did not expose segment-level slot density.
3. Compared C++ operator diagnostics for hot object distribution: Rust now
   reports distinct object count per segment.
4. Compared C++ partition/slot debugging needs: Rust now reports distinct
   routing-slot count per segment.
5. Compared C++ page header scans with Rust envelope scans: Rust derives object
   and slot summaries directly from the durable page envelope headers.
6. Compared repeated writes for the same logical object: Rust counts distinct
   object ids, not raw page records.
7. Compared repeated writes for the same routing slot: Rust counts distinct
   slots, not raw page records.
8. Compared range-style slot inspection: Rust reports the first and last routing
   slots observed in each segment.
9. Compared corruption handling: object/slot counting stops at the first corrupt
   record and preserves the healthy prefix summary.
10. Compared old raw segment compatibility: legacy raw/no-envelope segments
    remain readable and report no object/slot ownership.
11. Compared recovery-report composition: object/slot ownership is available
    through `StorageRecoveryReport.page_segment_reports`.
12. Compared page GC decision inputs: this does not yet implement C++ GC policy,
    but it gives Rust a durable segment summary that future policy can consume.
13. Compared compaction validation inputs: post-compaction reports can now show
    whether rewritten segments preserved object/slot ownership density.
14. Compared shared-store checkpoint debugging: restored page segments can be
    inspected with the same summary after install.
15. Compared page compression visibility: ownership counts live beside
    compressed-record counts instead of replacing them.
16. Compared existing recovery tests: the new unit test covers repeated
    object/slot writes and distinct-count semantics.
17. Compared local scale validation: this remains local first-party testing,
    not a distributed C++ cluster replay.
18. Remaining gap: Rust still lacks C++'s full object manager, slot dump
    scheduler, age/utility zone policy, and merged dump/load lifecycle.

Eighteen-loop storage-specific C++ gap-fill pass focused on live segment
density:

1. Compared C++ zone GC inputs with Rust segment reports: Rust summarized
   physical segments but did not connect them to live index references.
2. Compared C++ object manager liveness with Rust page addresses: Rust recovery
   now builds per-segment live-reference reports from the loaded shard index.
3. Compared C++ live/stale page accounting: Rust now reports live page refs and
   a stale page estimate per segment.
4. Compared C++ read-verification during recovery: Rust now reports readable and
   unreadable live refs per segment.
5. Compared C++ compaction utility signals: Rust now reports live-reference
   density in basis points for each segment.
6. Compared C++ byte-level utility signals: Rust now reports live physical bytes
   and live logical bytes beside total segment bytes.
7. Compared C++ object ownership in live sets: Rust now reports distinct live
   object count per segment.
8. Compared C++ slot ownership in live sets: Rust now reports distinct live
   routing-slot count per segment.
9. Compared healthy recovery diagnostics: recovery reports include full-density
   segments after clean restart.
10. Compared overwritten object behavior: overwrites in the same segment now
    produce stale-page estimates and reduced density.
11. Compared corruption behavior: unreadable live refs are counted both globally
    and by segment.
12. Compared raw segment compatibility: raw/no-envelope segments can still be
    summarized while live density comes from index addresses.
13. Compared future GC policy inputs: this gives Rust a policy-neutral utility
    signal without pretending to implement C++ age/utility scheduling.
14. Compared future compaction validation: tests can now assert that compaction
    improves segment density after rewrite.
15. Compared shared-store restore diagnostics: restored segments report live
    density after shard index install.
16. Compared test coverage: restart recovery now asserts density, live bytes,
    object count, and routing-slot count.
17. Compared local scale testing: the local scale harness still validates Raft,
    failover, and shared-store paths after the storage metadata change.
18. Remaining gap: Rust still lacks C++'s full zone age policy, background dump
    scheduler, and merged page/object/slot dump-load lifecycle.

Eighteen-loop proxy/client/metaserver/data-node C++ gap-fill pass focused on
service preflight visibility:

1. Compared C++ service runbooks with Rust stats: Rust had scattered stats but
   no single proxy/client preflight report.
2. Compared C++ proxy health surfaces with Rust proxy routes: Rust now exposes
   `/proxy/preflight` and C++-style `/ProxyService/Preflight`.
3. Compared C++ client backend-pool diagnostics with Rust client counters: the
   proxy preflight report now includes route cache, refresh, backend error, and
   meta-sync error counters.
4. Compared C++ proxy config visibility: preflight includes proxy addr,
   namespace, metaserver addr, and effective config version.
5. Compared C++ degraded-state signaling: proxy preflight flags metaserver
   errors, backend errors, continuous backend failures, and bad requests.
6. Compared C++ metaserver inventory checks with Rust meta stats: Rust now has
   `MetaPreflightReport`.
7. Compared C++ server/proxy liveness inventory: meta preflight reports normal
   and frozen server/proxy counts.
8. Compared C++ routing inventory: meta preflight reports shard route count and
   table/drop inventory through stats.
9. Compared C++ safe-mode diagnostics: meta preflight flags frozen servers and
   proxies as degraded reasons.
10. Compared C++ data-node worker inspection with Rust runtime stats: Rust now
    has `DataNodePreflightReport`.
11. Compared C++ queue pressure checks: data-node preflight reports foreground
    and background queue saturation.
12. Compared C++ rejection/timeout diagnostics: data-node preflight reports
    rejected and timed-out request degradation.
13. Compared C++ dirty-object dump scheduling: data-node preflight includes
    dirty shards and dirty objects.
14. Compared C++ partition-worker ownership: data-node preflight includes queued
    shard worker mapping.
15. Compared route aliases: proxy preflight has both Rust and C++-named routes,
    while brpc/thrift remains intentionally out of scope.
16. Compared tests: unit tests cover healthy/degraded proxy preflight,
    metaserver frozen inventory, and data-node dirty/queue degradation.
17. Compared local scale validation: the local scale harness still validates
    proxy/client/data-node/Raft/shared-store paths after the service visibility
    change.
18. Remaining gap: Rust still lacks Consul/service-discovery integration,
    production dashboard/alert wiring, and C++ brpc/thrift surfaces by design.

Eighteen-loop storage-specific C++ gap-fill pass focused on compaction utility
reporting:

1. Compared C++ compaction/GC policy inputs with Rust compaction output: Rust
   reported moved refs and stale segment ids, but not utility before/after.
2. Compared C++ live-page accounting: Rust now records live refs before and
   after shard page compaction.
3. Compared C++ stale-page accounting: Rust now records stale-page estimates
   before and after compaction.
4. Compared C++ zone-density scoring: Rust now records live-ref density in basis
   points.
5. Compared C++ live-zone set size: Rust now records live page segment count for
   each side of compaction.
6. Compared C++ page-count scan inputs: Rust now records total page count across
   live segments before and after compaction.
7. Compared Rust recovery live-density reports with compaction output: the same
   segment scan and live-address sources now feed compaction utility reporting.
8. Compared compaction effect visibility: overwriting an object in the same
   segment now shows stale estimate dropping after rewrite.
9. Compared operator diagnostics: `ShardCompactionReport` now carries `before`
   and `after` utility summaries.
10. Compared data-node background task output: `CompactionResponse` now exposes
    the same before/after utility summaries to runtime callers.
11. Compared failure behavior: failed/canceled compaction responses carry
    default utility summaries rather than partial guesses.
12. Compared test gates: engine compaction tests assert density improves from
    stale to full-live after rewrite.
13. Compared data-node gates: runtime compaction tests assert the same utility
    summaries propagate through async task output.
14. Compared compatibility: old JSON callers can ignore the added serde-default
    fields.
15. Compared policy scope: this does not implement C++'s full age/utility zone
    scheduler, but it makes the policy signal explicit.
16. Compared future GC integration: page GC can now consume compaction utility
    summaries without rescanning unrelated service state.
17. Compared local scale validation: the local scale harness still validates
    write/read/replication/shared-store behavior after the storage report change.
18. Remaining gap: Rust still lacks C++'s full background zone selection,
    production dump/load merge policy, and object-manager-driven compaction
    scheduler.

Eighteen-loop storage-specific C++ gap-fill pass focused on page-GC byte
accounting:

1. Compared C++ page/zone GC reporting with Rust page segment GC: Rust reported
   removed and retained segment ids, but not reclaimed or retained byte impact.
2. Compared C++ operator-facing reclaim diagnostics: `PageStoreGcReport` now
   reports `removed_physical_bytes`.
3. Compared C++ retained-zone visibility: `PageStoreGcReport` now reports
   `retained_physical_bytes`.
4. Compared delayed-destroy GC policy: Rust now reports
   `delayed_destroy_physical_bytes` for quarantined stale page segments.
5. Compared live-reference protection: Rust now reports
   `retained_live_physical_bytes` beside live-retained segment ids.
6. Compared current-writer protection: Rust now reports
   `retained_current_physical_bytes` beside current-retained segment ids.
7. Compared utility-bounded GC: no-op utility GC now proves zero removed bytes.
8. Compared selected utility GC: selected stale segment quarantine now reports
   the physical bytes moved out of active storage.
9. Compared retention-floor GC: floor-based stale segment deletion now reports
   exact removed and retained physical bytes.
10. Compared live-index retention: stale segments are counted as removed bytes,
    while live-referenced segments are counted as live-retained bytes.
11. Compared data-node task output: `GcResponse` now exposes removed page
    segment physical bytes.
12. Compared runtime retained-state visibility: `GcResponse` now exposes total
    retained page segment physical bytes.
13. Compared runtime live-retained visibility: `GcResponse` now exposes
    live-retained page segment physical bytes.
14. Compared cancellation/failure behavior: canceled GC responses default the
    byte counters to zero rather than reporting partial estimates.
15. Compared serde compatibility: added fields default when older JSON omits
    them.
16. Compared storage tests: page-store GC tests now assert byte accounting for
    deletion, delayed destroy, live retention, and bounded utility GC.
17. Compared local runtime tests: data-node GC tests now assert byte accounting
    propagates through async background task output.
18. Remaining gap: Rust still lacks C++'s full age/utility zone scheduler and
    merged dump/load policy, but GC now reports concrete reclaim impact.

Eighteen-loop storage-specific C++ gap-fill pass focused on delayed-destroy
purge accounting:

1. Compared C++ delayed zone destruction with Rust delayed-destroy quarantine:
   Rust could list quarantined segment ids but not their pending byte impact.
2. Compared C++ cleaner inventory needs: Rust now exposes
   `PageStoreDelayedDestroySegmentReport` with segment id and physical bytes.
3. Compared C++ operator preflight diagnostics: Rust callers can inspect
   delayed-destroy bytes before final purge.
4. Compared C++ reclaim accounting at destroy time: Rust now exposes
   `PageStorePurgeDelayedDestroyReport`.
5. Compared purge id reporting: the new purge report carries sorted
   `purged_page_segment_ids`.
6. Compared purge byte reporting: the new purge report carries
   `purged_physical_bytes`.
7. Compared compatibility: the older `purge_delayed_destroy_segments` API still
   returns only ids by delegating to the richer report.
8. Compared missing-trash behavior: purge returns an empty default report when
   no delayed-destroy directory exists.
9. Compared manifest rebuild behavior: Rust now rebuilds delayed-destroy zone
   descriptors with physical bytes from the trash files instead of zeroing
   them.
10. Compared delayed-destroy durability after restart: reopened page stores keep
    delayed-destroy zone physical bytes visible.
11. Compared stale-segment quarantine tests: unit tests now assert pending trash
    byte inventory after delayed-destroy GC.
12. Compared purge tests: unit tests now assert final purged byte totals.
13. Compared utility GC: bounded utility GC now exposes delayed-destroy pending
    bytes for the selected stale segment.
14. Compared zone lifecycle reporting: active, delayed-destroy, and purged
    states now retain more useful size data through the local lifecycle.
15. Compared serde/debug shape: the new report structs are serializable and
    cloneable like the existing storage diagnostics.
16. Compared local safety: purge still removes only files whose delayed-destroy
    names parse as page segments.
17. Compared local scale validation: the Rust scale harness still validates
    write/read/Raft/shared-store behavior after this storage report change.
18. Remaining gap: Rust still lacks the full C++ background cleaner scheduler,
    age policy, and merged dump/load workflow.

Eighteen-loop storage-specific C++ gap-fill pass focused on zone lifecycle
capacity accounting:

1. Compared C++ zone cleaner inputs with Rust zone descriptors: Rust exposed
   per-zone bytes but not aggregate lifecycle capacity.
2. Compared C++ active-zone accounting: Rust now reports active zone count and
   active physical bytes in `PageStoreZoneSummary`.
3. Compared C++ sealed-zone accounting: Rust now reports sealed zone count and
   sealed physical bytes.
4. Compared C++ delayed-destroy accounting: Rust now reports delayed-destroy
   zone count and delayed-destroy physical bytes.
5. Compared C++ purged-zone accounting: Rust now reports purged zone count and
   historical purged physical bytes from the manifest.
6. Compared C++ live-storage accounting: Rust now reports live physical bytes as
   active plus sealed zone bytes.
7. Compared C++ cleaner reclaim inputs: Rust now reports reclaimable physical
   bytes as delayed-destroy bytes.
8. Compared C++ capacity snapshots: Rust now reports total known physical bytes
   across all zone lifecycle states.
9. Compared direct page-store diagnostics: `LocalPageStore::zone_summary`
   returns the lifecycle aggregate without manual descriptor folding.
10. Compared recovery diagnostics: `StorageRecoveryReport` now includes
    `zone_summary`.
11. Compared serde compatibility: `StorageRecoveryReport.zone_summary` has a
    default so older JSON remains readable.
12. Compared sealed-to-active transitions: unit tests assert sealed and active
    counts/bytes after segment roll.
13. Compared delayed-destroy transitions: unit tests assert reclaimable and
    delayed-destroy bytes after quarantine.
14. Compared purge transitions: unit tests assert purged bytes persist while
    reclaimable bytes drop to zero after purge.
15. Compared manifest reopen: recovery reports expose zone summary after a clean
    reopen.
16. Compared missing-manifest rebuild: recovery reports expose zone summary
    after rebuilding page-zone metadata.
17. Compared local scale validation: the Rust scale harness still validates
    write/read/Raft/shared-store behavior after the summary addition.
18. Remaining gap: Rust still lacks C++'s full background cleaner scheduler,
    policy scoring, and merged dump/load workflow.

Eighteen-loop storage-specific C++ gap-fill pass focused on page-store zone
metrics:

1. Compared C++ storage dashboards with Rust metrics: Rust had page-store
   read/write/compression counters but not zone lifecycle capacity metrics.
2. Compared C++ active-zone metrics: Rust now exports active zone count through
   `temporalstore_page_store_zone_count`.
3. Compared C++ sealed-zone metrics: Rust now exports sealed zone count.
4. Compared C++ delayed-destroy metrics: Rust now exports delayed-destroy zone
   count.
5. Compared C++ purged-zone metrics: Rust now exports purged zone count.
6. Compared C++ physical-byte gauges: Rust now exports active physical bytes
   through `temporalstore_page_store_zone_bytes`.
7. Compared sealed capacity gauges: Rust now exports sealed physical bytes.
8. Compared delayed-destroy backlog gauges: Rust now exports delayed-destroy
   physical bytes.
9. Compared purged history gauges: Rust now exports purged physical bytes.
10. Compared cleaner inputs: Rust now exports live physical bytes as active plus
    sealed bytes.
11. Compared reclaim backlog inputs: Rust now exports reclaimable physical
    bytes.
12. Compared total capacity snapshots: Rust now exports total known zone
    physical bytes.
13. Compared JSON stats surfaces: `ShardStats` now includes
    `page_store_zones`.
14. Compared serde compatibility: `ShardStats.page_store_zones` defaults when
    old JSON omits it.
15. Compared `GetStats` tests: shard stats now assert zone summary is populated.
16. Compared Prometheus tests: metrics tests now assert zone count and byte
    gauges render.
17. Compared local scale validation: the Rust scale harness still validates
    write/read/Raft/shared-store behavior after metric expansion.
18. Remaining gap: Rust still lacks C++'s full cleaner policy loop, dashboards,
    and alert rules.

Eighteen-loop storage-specific C++ gap-fill pass focused on page-zone age
metadata:

1. Compared C++ zone cleaner policy inputs with Rust zone manifests: Rust had
   zone state and byte accounting but no age metadata.
2. Compared C++ zone creation tracking: Rust `PageStoreZoneDescriptor` now
   carries optional `created_unix_ms`.
3. Compared C++ zone transition tracking: Rust `PageStoreZoneDescriptor` now
   carries optional `updated_unix_ms`.
4. Compared write-path freshness: normal appends initialize and update active
   zone timestamps.
5. Compared segment roll behavior: rolling seals the previous zone and stamps
   its update time.
6. Compared new-zone creation: segment roll creates the next active zone with
   creation and update timestamps.
7. Compared restored segment behavior: `install_segment` stamps restored zones
   from filesystem metadata when available.
8. Compared missing-manifest rebuild: Rust rebuilds zone timestamps from segment
   file created/modified metadata.
9. Compared delayed-destroy inventory: delayed-destroy reports now include
   trash-file modified timestamps.
10. Compared delayed-zone rebuild: missing-manifest rebuild carries
    delayed-destroy trash timestamps into zone descriptors.
11. Compared delayed-destroy transitions: moving a segment to delayed destroy
    updates the zone timestamp.
12. Compared purge transitions: final purge updates the purged zone timestamp.
13. Compared serde compatibility: both timestamp fields are optional and default
    when older manifests omit them.
14. Compared manifest durability: zone timestamp metadata persists through the
    existing synced manifest write path.
15. Compared tests: page-store lifecycle tests assert timestamps exist after
    roll, survive reopen, update on delayed destroy, and update on purge.
16. Compared rebuild tests: missing-manifest rebuild tests assert recreated
    descriptors include timestamps.
17. Compared local scale validation: the Rust scale harness still validates
    write/read/Raft/shared-store behavior after the manifest expansion.
18. Remaining gap: Rust still lacks C++'s full age/utility cleaner scheduler,
    policy scoring, and merged dump/load workflow.

Eighteen-loop storage-specific C++ gap-fill pass focused on age-aware GC
candidate reporting:

1. Compared C++ age/utility cleaner inputs with Rust utility GC candidates:
   Rust candidates had utility score and bytes, but no age signal.
2. Compared C++ creation-time policy hooks: Rust GC candidates now carry
   optional `created_unix_ms`.
3. Compared C++ transition-time policy hooks: Rust GC candidates now carry
   optional `updated_unix_ms`.
4. Compared C++ age ranking inputs: Rust GC candidates now carry optional
   `age_ms`.
5. Compared manifest integration: candidate age fields are derived from
   `PageStoreZoneDescriptor` timestamp metadata.
6. Compared missing timestamp compatibility: candidate age fields remain
   optional for legacy manifests.
7. Compared bounded utility GC: selected destroy candidates can now be inspected
   for utility, size, and age before deletion/quarantine.
8. Compared policy ordering: Rust still sorts first by utility score.
9. Compared reclaim impact ordering: Rust still sorts larger stale byte
   candidates before smaller candidates at the same utility.
10. Compared age tie-breaking: Rust now prefers older candidates before segment
    id when utility and bytes tie.
11. Compared stable fallback behavior: candidates without age fall back to zero
    age for ordering.
12. Compared delayed-destroy path: age-aware candidates continue to route
    through delayed-destroy quarantine when requested.
13. Compared serde/debug shape: added candidate fields are optional and
    serializable like the existing utility report.
14. Compared storage tests: utility GC tests now assert candidate timestamp and
    age fields are populated.
15. Compared no-op utility GC: max-destroy zero still removes no segments while
    candidate reporting remains available.
16. Compared local safety: current and live-referenced segments remain excluded
    from utility candidates.
17. Compared local scale validation: the Rust scale harness still validates
    write/read/Raft/shared-store behavior after candidate expansion.
18. Remaining gap: Rust still lacks C++'s actual background cleaner scheduler,
    final policy weights, and merged dump/load workflow.

Eighteen-loop storage-specific C++ gap-fill pass focused on bounded GC policy
planning:

1. Compared C++ cleaner policy selection with Rust utility GC: Rust could rank
   candidates but only selected by raw segment count.
2. Compared C++ policy inputs: Rust now has `PageStoreGcPolicy` for max segment
   count, max physical bytes, max utility score, and minimum age.
3. Compared C++ dry-run tooling: Rust now has `PageStoreGcPolicyPlan` for
   non-destructive policy inspection before delete/quarantine.
4. Compared reclaim budgeting: Rust plans selected physical bytes before
   destructive GC.
5. Compared operator explanations: Rust reports skipped-by-policy and
   skipped-by-budget counts.
6. Compared candidate transparency: policy plans include the ordered candidate
   list used for selection.
7. Compared old utility GC behavior: the existing max-segment utility API now
   delegates through `PageStoreGcPolicy::max_segments`.
8. Compared delayed-destroy behavior: policy-selected segments still route
   through delayed-destroy quarantine when requested.
9. Compared age filters: `min_age_ms` can suppress younger candidates using the
   zone-derived candidate age metadata.
10. Compared utility filters: `max_utility_score` can suppress candidates above
    an accepted utility threshold.
11. Compared byte caps: `max_destroy_physical_bytes` prevents selecting more
    reclaim bytes than the caller budget allows.
12. Compared unlimited defaults: zero segment and byte budgets remain unbounded
    for compatibility unless a caller sets them.
13. Compared current/live protection: policy planning still starts from the same
    current/live-safe utility candidate list.
14. Compared test gates: storage tests now assert byte-bounded policy planning
    and application.
15. Compared selection order: larger stale candidates are considered first, but
    budget-ineligible candidates can be skipped in favor of smaller ones.
16. Compared serde/debug shape: policy and plan structs are serializable for
    future admin APIs or scheduler snapshots.
17. Compared local scale validation: the Rust scale harness still validates
    write/read/Raft/shared-store behavior after planner wiring.
18. Remaining gap: Rust still lacks C++'s continuous background cleaner loop,
    production policy weights, and full merged dump/load workflow.

Eighteen-loop storage-specific C++ gap-fill pass focused on GC policy-plan byte
accounting:

1. Compared C++ reclaim planning diagnostics with Rust policy plans: Rust
   counted skipped candidates but did not account skipped bytes by reason.
2. Compared total candidate capacity: Rust `PageStoreGcPolicyPlan` now reports
   `candidate_physical_bytes`.
3. Compared policy-filter impact: Rust now reports
   `skipped_by_policy_physical_bytes`.
4. Compared budget-filter impact: Rust now reports
   `skipped_by_budget_physical_bytes`.
5. Compared selected reclaim visibility: Rust already reported
   `selected_physical_bytes`, and now it can be compared with total candidate
   bytes.
6. Compared operator dry runs: callers can now see whether low reclaim is caused
   by age/utility filters or count/byte budgets before deleting anything.
7. Compared C++ cleaner explainability: skipped-by-policy count and bytes move
   together.
8. Compared budget explainability: skipped-by-budget count and bytes move
   together.
9. Compared overflow behavior: byte totals use saturating arithmetic while
   accumulating candidate decisions.
10. Compared current/live safety: byte accounting is derived after current and
    live-referenced segments have already been excluded.
11. Compared delayed-destroy behavior: byte-accounted plans still feed the same
    selected segment set into delayed-destroy quarantine.
12. Compared legacy utility GC: max-destroy zero remains a no-op for the old
    utility API.
13. Compared policy defaults: zero byte budgets remain unbounded for explicit
    policy-plan callers.
14. Compared storage tests: byte-bounded policy tests now assert candidate,
    selected, skipped-policy, and skipped-budget byte totals.
15. Compared larger-candidate handling: an oversized high-priority stale segment
    can be skipped by budget while a smaller candidate is selected and reported.
16. Compared serde/debug shape: new byte fields default through the plan struct's
    existing serde shape.
17. Compared local scale validation: the Rust scale harness still validates
    write/read/Raft/shared-store behavior after byte accounting.
18. Remaining gap: Rust still lacks C++'s continuous background cleaner loop,
    production policy weights, and full merged dump/load workflow.

Eighteen-loop storage-specific C++ gap-fill pass focused on zone-summary age
signals:

1. Compared C++ zone cleaner dashboards with Rust zone summaries: Rust exposed
   lifecycle bytes and counts but not aggregate age signals.
2. Compared total zone age visibility: Rust `PageStoreZoneSummary` now reports
   `oldest_known_zone_unix_ms`.
3. Compared total zone age duration: Rust now reports
   `oldest_known_zone_age_ms`.
4. Compared live-zone cleaner inputs: Rust now reports `oldest_live_zone_unix_ms`
   across active and sealed zones.
5. Compared live-zone age duration: Rust now reports `oldest_live_zone_age_ms`.
6. Compared reclaimable-zone visibility: Rust now reports
   `oldest_reclaimable_zone_unix_ms` for delayed-destroy zones.
7. Compared reclaimable-zone age duration: Rust now reports
   `oldest_reclaimable_zone_age_ms`.
8. Compared legacy manifest compatibility: all new summary age fields are
   optional.
9. Compared timestamp source: summary timestamps use zone `updated_unix_ms` and
   fall back to `created_unix_ms`.
10. Compared active/sealed accounting: live-zone age covers both active and
    sealed zones, matching the existing live byte definition.
11. Compared delayed-destroy accounting: reclaimable age covers delayed-destroy
    zones, matching the existing reclaimable byte definition.
12. Compared purged accounting: purged zones remain known but no longer count as
    live or reclaimable.
13. Compared operator safety: summaries provide age signals without deleting or
    moving segments.
14. Compared overflow behavior: age durations use saturating subtraction.
15. Compared test gates: zone lifecycle tests now assert known/live/reclaimable
    age fields through roll, reopen, delayed destroy, and purge.
16. Compared serde/debug shape: summary age fields skip serialization when
    absent.
17. Compared local scale validation: the Rust scale harness still validates
    write/read/Raft/shared-store behavior after summary expansion.
18. Remaining gap: Rust still lacks C++'s continuous background cleaner loop,
    production policy weights, and full merged dump/load workflow.

Eighteen-loop storage-specific C++ gap-fill pass focused on zone-age metrics:

1. Compared C++ zone cleaner dashboards with Rust metrics: Rust had zone count
   and byte gauges but not zone age gauges.
2. Compared oldest known zone timestamp visibility: Rust now exports
   `temporalstore_page_store_zone_oldest_unix_ms{scope="known"}`.
3. Compared oldest live zone timestamp visibility: Rust now exports
   `temporalstore_page_store_zone_oldest_unix_ms{scope="live"}`.
4. Compared reclaimable zone timestamp visibility: Rust exports the same metric
   with `scope="reclaimable"` when delayed-destroy zones exist.
5. Compared oldest known age visibility: Rust now exports
   `temporalstore_page_store_zone_oldest_age_ms{scope="known"}`.
6. Compared oldest live age visibility: Rust now exports
   `temporalstore_page_store_zone_oldest_age_ms{scope="live"}`.
7. Compared reclaimable age visibility: Rust exports
   `temporalstore_page_store_zone_oldest_age_ms{scope="reclaimable"}` when
   applicable.
8. Compared absent-scope behavior: gauges are omitted when the zone summary has
   no timestamp for that lifecycle scope.
9. Compared Prometheus metadata: both new metric families include HELP and TYPE
   headers.
10. Compared shard labeling: age metrics carry the same shard label as count and
    byte metrics.
11. Compared lifecycle labeling: age metrics use scope labels matching summary
    semantics: known, live, and reclaimable.
12. Compared operator correlation: dashboards can now correlate zone bytes,
    counts, oldest timestamps, and oldest age.
13. Compared C++ cleaner inputs: age metrics remain observability inputs, not a
    background deletion scheduler.
14. Compared test gates: Prometheus metrics tests now assert known/live timestamp
    and age gauges.
15. Compared delayed-destroy compatibility: reclaimable scope remains optional
    until delayed-destroy zones exist.
16. Compared serde/API compatibility: this pass does not change request or
    storage file formats.
17. Compared local scale validation: the Rust scale harness still validates
    write/read/Raft/shared-store behavior after metrics expansion.
18. Remaining gap: Rust still lacks C++'s continuous background cleaner loop,
    production policy weights, and full merged dump/load workflow.

Eighteen-loop storage-specific C++ gap-fill pass focused on structured segment
corruption diagnostics:

1. Compared C++ page/zone inspection with Rust segment reports: Rust exposed a
   first error string but not structured corruption status.
2. Compared operator-safe health flags: `PageStoreSegmentReport` now reports
   `has_corruption`.
3. Compared first-corruption location reporting: segment reports now include
   `first_error_offset`.
4. Compared healthy-prefix recovery diagnostics: segment reports now include
   `readable_prefix_physical_bytes`.
5. Compared raw legacy segment compatibility: legacy raw segments count their
   full physical length as readable prefix.
6. Compared envelope scan behavior: healthy envelope records advance readable
   prefix bytes as they are decoded.
7. Compared checksum failure behavior: checksum or envelope failures mark the
   report corrupt at the failing physical offset.
8. Compared mixed-format failure behavior: mixed raw bytes after an envelope now
   carry a structured first error offset.
9. Compared short-header failure behavior: short headers now carry a structured
   first error offset.
10. Compared payload-length failure behavior: truncated payloads now carry a
    structured first error offset.
11. Compared C++ recovery UX: tooling can now classify a report as corrupt
    without parsing `first_error`.
12. Compared prefix salvage UX: tooling can now reason about the readable prefix
    without recomputing record lengths.
13. Compared object/slot diagnostics: object and routing-slot counts remain
    populated for the healthy prefix.
14. Compared page-id diagnostics: first/last page IDs remain populated for the
    healthy prefix.
15. Compared test gates: healthy segment-report tests assert no corruption and
    full readable prefix.
16. Compared corruption tests: corrupt segment-report tests assert corruption,
    error offset, and healthy-prefix physical bytes.
17. Compared local scale validation: the Rust scale harness still validates
    write/read/Raft/shared-store behavior after report expansion.
18. Remaining gap: Rust still lacks C++'s full object/page/slot manager,
    production background cleaner loop, and merged dump/load workflow.

Storage lifecycle C++ gap-fill pass focused on slot-scoped dump/recovery
planning:

1. Compared C++ `ObjectManager`/`SlotStore` ownership with Rust page refs: Rust
   now exposes per-routing-slot storage summaries.
2. Compared C++ dirty-slot accounting: slot summaries include dirty object
   counts and dirty generation counters.
3. Compared C++ live page accounting: slot summaries include object counts,
   page refs, physical bytes, logical bytes, page segment ids, and last zone.
4. Compared C++ dump manifests: Rust now persists checksum-protected
   `SlotDumpManifest` JSON under the local index directory.
5. Compared dump sequence boundaries: manifests record oplog and index-log
   sequence boundaries alongside live page refs and page segments.
6. Compared selective dirty flush: data-node dump requests can now flush only
   selected routing slots instead of clearing all dirty objects for the shard.
7. Compared background storage manager planning: Rust now exposes a
   `StorageLifecyclePlan` with dirty slots, stale segments, delayed-destroy
   candidates, reclaimable bytes, and reasons.
8. Compared applied storage manager reports: Rust now exposes
   `StorageLifecycleReport` with optional dump manifest, cache invalidation,
   and delayed-destroy purge results.
9. Compared recovery boundary diagnosis: Rust now exposes
   `StorageRecoveryBoundaryReport` with safe replay sequences, latest dump
   sequences, orphan page segments, missing dump slots, corrupt segments, and
   unreadable bytes.
10. Compared operator APIs: server routes expose slot summaries, dump manifests,
    recovery boundaries, dry-run lifecycle plans, and applied lifecycle reports.
11. Compared metrics: Prometheus now includes slot page refs, slot logical and
    physical bytes, and dirty objects per routing slot.
12. Compared local validation: unit tests cover slot summaries, manifest
    validation failures, selected-slot dump cleanup, and lifecycle boundaries.
13. Closed the next dump/load gap: slot dump manifests now embed a checksum-
    protected index snapshot and can be installed through the Rust engine or
    `/server/storage/dumps/install`, with stale, partial, missing-segment, and
    checksum failures rejected.
14. Closed the first production cleaner-loop gap: `DataNodeRuntime` now exposes
    a periodic storage lifecycle scheduler and counts lifecycle runs in runtime
    stats.
15. Closed the local shared-store retention gap: cursor-safe oplog GC refuses
    deletion that would advance beyond a known follower replay cursor.
16. Closed the cache policy testability gap: lifecycle apply can warm cache
    entries from live page-index refs and reports the number of warmed page refs.
17. Closed the C++ `StorageManager::ShouldDelayDumpOplog`/per-round dump
    policy gap: Rust lifecycle planning now reports undumped oplog records,
    can delay automatic dirty-slot dumps until a configured oplog threshold is
    reached, and can cap selected dirty slots per lifecycle round while still
    allowing explicit operator-selected slots.
18. Closed the C++ `SlotStore::LoadPages` validation gap: Rust slot dump
    manifest validation now reconstructs live page refs from the embedded index
    snapshot and reads each referenced page before install, rejecting corrupt or
    unreadable page bytes instead of accepting a manifest that only references
    an existing segment.
19. Closed the dump manifest inventory gap: Rust now rejects a checksum-valid
    slot dump manifest whose declared page segment list does not exactly match
    the embedded index snapshot's live page references for the dumped slots.

Eight storage audit passes completed against C++ storage code:

1. `ObjectManager` object/page ownership -> Rust slot summaries and page refs.
2. `SlotStore::DumpSlots` dirty-slot dump -> Rust selected-slot manifests.
3. `SlotStore::LoadPages` read validation -> Rust manifest live-ref page reads
   and page-segment inventory matching.
4. `StorageManager::ShouldDelayDumpOplog` -> Rust undumped-oplog threshold.
5. `storage_dump_slots_per_round` -> Rust max dump slots per lifecycle round.
6. `PageGc`/delayed destroy -> Rust live-ref page GC and delayed purge.
7. `PageCompactor` -> Rust live-page compaction and stale-segment reports.
8. `Evicter`/`Expirer`/metrics loop -> Rust cache eviction, TTL sweep, runtime
   stats, Prometheus storage metrics, and local scale validation.

Remaining explicit non-goals for this pass: brpc/thrift surfaces, S3/ByteStore
integration, C++ byte-for-byte page/header compatibility, and CacheLib/mtcache
FFI. Remaining future parity depth is the larger C++ object/page allocator and
stream-backend internals, but the Rust storage lifecycle now has concrete
slot-scoped ownership summaries, dump/install validation, scheduler hooks,
cursor-safe local shared-store GC, and local cache warmup coverage.
13. Remaining gap: Rust still does not have the full C++ merged dump/load
    install pipeline, binary/protobuf logs, production cleaner weights, or
    CacheLib/mtcache-equivalent SSD cache.

| Area | C++ TemporalStore | Rust Today | Missing |
| --- | --- | --- | --- |
| Protocol | brpc/thrift/protobuf APIs and extension protos | JSON/HTTP command API plus RESP adapter; C++ `ServerService`-named JSON route aliases for load/unload/execute/batch/apply-data-raft-log/config/info/stats/stream/update-membership/ping; C++ `MasterService`-named JSON route aliases for implemented table/server control-plane operations including table update/delete; brpc/thrift intentionally excluded from the Rust parity target | SDK compatibility for the supported Rust HTTP/RESP API; no brpc/thrift parity target now |
| Proxy | brpc/thrift server, C++ client wrapper, MetaSyncer, heartbeat/config, consul registration | HTTP proxy service with `/execute`, `/batch_execute`, `/shards`, `/proxy/info`, `/proxy/config`, `/proxy/heartbeat`, `/proxy/preflight`, `/proxy/open_table`, `/proxy/table_execute`, and `/proxy/table_batch_execute`; forwarding delegates through `TemporalStoreClient` for route cache, stats sync, retries/timeouts, backend-error route refresh, continuous-failure bypass, table topology sync, key-to-shard routing, C++-style duplicate config-update no-op handling, C++-style preflight alias `/ProxyService/Preflight`, and degraded preflight reasons for meta/backend/bad-request issues; background heartbeat loop and heartbeat auto-register helper | service discovery/consul equivalent for the Rust target; no brpc/thrift proxy target now |
| Client SDK | C++ `Client`, `Table`, `Pipeline`, `MetaSyncer`, router, backend pool | Rust `TemporalStoreClient`, `TemporalStoreTable`, `TemporalStorePipeline`, typed methods, open/close table cache, close-table route-cache eviction, stats surfaced through proxy preflight, timeouts, transport retries, C++-style retryable-status read/write budgets with linear backoff, direct route refresh, per-backend continuous-failure windows, backend error streak tracking, open table from metaserver topology, background topology sync, C++ `crc64 >> 34` slot router, primary routing for writes, optional first-secondary/round-robin secondary routing for reads, location-affine replica preference, and deterministic drop-percent traffic shedding | documented Rust HTTP/RESP SDK migration API, full partition-set hierarchy, Neptune-specific routing |
| Routing | namespace/table/partition-set/slot routing | key-to-shard routing from table options using C++ CRC64 slot formula, explicit `shard_id` request, simple metaserver route, topology-cached primary/replica endpoint choice, C++ `PartitionId` bit layout helper and opt-in C++-encoded table partition ids | full partition-set hierarchy, route versioning, placement hierarchy |
| Metaserver | full topology, heartbeat, placement, scheduling, Raft-backed metadata | shard route map, namespace/table topology, C++ `MasterService`-named JSON aliases for implemented table/server control-plane operations, table update/delete lifecycle with topology-version bumps, dropped-table state, and topology rejection, table serving-option catalog for client routing/retry/drop/read-policy/timeouts, opt-in C++ `PartitionId` generation for table partitions, load-aware replica fill with location and host diversity, server/proxy register/list/heartbeat, stale resource failure-detector loop with safe-mode cooldown gates for server/proxy rejoin, durable local JSONL mutation log/replay, single-node metabase snapshot export/import and atomic local snapshot save/load, Raft-mode metabase snapshot export/save/load/restore through the metaserver admin routes, meta stats and `MetaPreflightReport` for normal/frozen server/proxy inventory and shard-route counts, optional Raft-backed HTTP mutation path through `ProductionMetaRaftRuntime`, rebalance model with C++-style balance partition counts and placement-failure counters, C++-style update-membership task model with sibling filtering, threshold checks, not-found-as-reboot success, FSM-submit gating, deterministic priority task scheduler model with retry-later backoff and abort handling, scheduler HTTP admin surface, scheduler snapshot/restore, optional local scheduler snapshot persistence, freezing-shard repair into UpdateMembership tasks | networked multi-process metaserver Raft transport, full C++ placement rule chain, full background scheduler loop executing tasks against real data-node processes |
| Data node execution | partition workers, async callbacks, load-version guards | `TemporalEngine` plus `DataNodeRuntime` shard-affine worker lanes, bounded foreground/background queue admission, foreground-over-background scheduler priority, per-shard FIFO single-lane execution, cross-shard parallelism, async jobs, immediate in-flight cancel-request status, cooperative cancellation checkpoints before destructive background phases, dirty tracking, slot-selected dump requests with persisted slot dump manifests, storage lifecycle dry-run/apply reports, `DataNodePreflightReport` for queue saturation, dirty backlog, queued shard workers, rejections, and timeouts, checked execute/batch routes, invalid stream range rejection, background expiry sweep, C++-style duplicate-load/not-found-unload/config-not-found handling, C++-style membership update version guards and local-membership validity reporting, C++ `Partition::SetConfig` stale/equal/newer version semantics, shard/table/tenant scoped QPS admission, readonly replica replay route-change reset and metrics | tonic streaming/callback shape, preemptive hard cancellation of arbitrary running user work, distributed admission policy shared across data-node processes |
| Hot object model | `ObjectManager`, model objects, dirty slots | per-type maps of key/field/timestamp to `PageAddress` plus stable page/object/routing-slot metadata, per-slot storage summaries for object/page refs, logical/physical bytes, dirty generation/counts, page segments and last zone, C++-style stats for logical object count, page refs, dirty objects, dirty routing slots, and partition info | full object lifecycle and model-specific memory layout |
| Oplog | binary mutation log with replay/reclaim semantics | JSONL command oplog with synced append, corrupt-tail truncation on recovery, explicit retain-from-sequence GC rewrite, synced temp-file GC rewrite, and reopen-after-GC tests | binary/protobuf compatibility where needed by migration API, replay into hot object manager |
| Index log | binary metadata/index log | JSONL index-log with current index metadata, synced append, corrupt-tail truncation on recovery, explicit retain-from-sequence GC rewrite, synced temp-file GC rewrite, and reopen-after-GC tests | compact incremental deltas, page/object ids, checksums, replay ordering with oplog and page dumps |
| Page store | slot/page/zone layout, protobuf page headers, dump/merge/load | append-only local page segment files plus a durable Rust zone manifest, zone descriptors with creation/update timestamps and aggregate zone summaries for active/sealed/delayed-destroy/purged lifecycle plus known/live/reclaimable oldest-zone age signals, manifest rebuild from existing segment files including delayed-destroy trash byte sizes/timestamps, dump/compact/GC task hooks, slot-scoped dump manifests with checksum validation and oplog/index-log sequence boundaries, storage lifecycle plans/reports for dirty slots, stale segments, delayed-destroy purge, cache invalidation, and recovery boundaries, local live-page rewrite compaction into fresh segments, page-GC reports for removed/retained/delayed-destroy/live-retained/current-retained physical bytes, delayed-destroy inventory reports, purge reports with final reclaimed bytes, before/after compaction utility summaries with live refs, stale-page estimate, live-segment count, total page count, and density, age-aware utility-bounded stale segment selection, dry-run GC policy plans with count/byte/utility/age filters plus candidate/selected/skipped byte accounting, delayed-destroy quarantine and explicit purge for stale local page segments, self-describing page record envelopes with magic/version/length/SHA-256/page-id/object-id/routing-slot/zone-id/compression/stored-length fields for new durable appends, zstd-compressed physical page records when compression reduces size, segment inspection reports with physical/logical bytes, page count, distinct object count, distinct routing-slot count, routing-slot range, compressed-record count, page-id range, structured corruption flag/error offset, readable-prefix physical bytes, and first corruption error, recovery live-density reports with per-segment live/readable/unreadable refs, stale-page estimate, live physical/logical bytes, live object/slot counts, and live-ref density, constructor/server-env compression policy for enablement/min-size/zstd-level, physical/logical/compressed-record/compression-saved page-store stats plus zone lifecycle and slot gauges exported through Prometheus, logical page-stream reads that skip Rust envelopes and decompress records across boundaries, highest-segment reopen for future appends, restored higher segment install becomes the current append target, persisted page-id allocation recovered from existing/install segments, page-id/object-id/routing-slot/zone-id mismatch rejection on reads, segment-roll zone-id changes for future pages, SHA-256 checksums on newly written logical page bytes, checksum verification on reads after decompression, v1/v2/v3/v4/v5 envelope and legacy raw/no-checksum read compatibility, synced appends, synced segment roll, parent-dir sync for segment creation/install, and atomic segment install through temp-file rename | deeper C++ object/page/slot layout, distributed/control-plane compression policy, full C++ background utility/age zone GC scheduler, full C++ merged dump/load policy |
| Shared store | local/ByteStore stream backends and replica replay | file-backed shared-store checkpoint, sync/async oplog publish, bounded async flush, checksum-enveloped oplog objects, strict gap-rejecting replay, persisted replay cursor, bounded object-store retry and async requeue-on-failure, oplog/checkpoint GC | automatic lifecycle safety tied to follower cursors/Raft snapshots; no S3/ByteStore integration target now |
| Raft | ByteRaft-backed production groups | separate-node data-node Raft runtime wrapper with OpenRaft/raft-rs engine selection, production metaserver Raft runtime wrapper, mTLS config validation, authenticated RPC runtime construction plus receive-side peer RPC auth enforcement, timer supervisors, metaserver stale-server detection in Raft mode, multi-process chaos plan validation, in-process behavior model plus snapshot semantics, deterministic applied-log-byte snapshot trigger reports for data and metaserver Raft, HTTP Raft transport for AppendEntries/Vote/InstallSnapshot/chunked InstallSnapshot, timeout tick election with pre-vote, randomized scheduler model, hard-state/membership inspection, local durable segmented WAL record export/load/recovery with sync, retention, corrupt-tail truncation, and installed-snapshot recovery floor, auto-persisting WAL-backed cluster mode, bounded follower catch-up with replay progress and lag reports, commit-to-apply health reports, process-level `/raft/apply_health`, apply-lag metrics, networked `/raft/membership/apply` plus C++-style `/raft/control/{list_membership,add_node,remove_node,trigger_snapshot,read_index,transfer_leader}` on raft_node and raft-enabled server, process-level external snapshot bootstrap route, bounded local WAL retention, AppendEntries/Vote/InstallSnapshot local receive behavior, joint-consensus old/new majority safety model, safe add/remove/replace membership-change planner and report, metaserver topology to data-Raft voter-plan bridge with no-op and server-state validation, Raft RPC retry/backpressure/auth/deadline wrapper, deterministic leader-lease expiry guard for data-Raft reads/writes, local partition/heal chaos coverage, ByteKV/ByteRaft-style config/read options, oversized log guard, election prohibition, status/local-status, read-index guard, leader transfer, raft metrics | OpenRaft/raft-rs FSM/storage integration, actual mTLS transport, production engine snapshot freeze/flush lifecycle, metaserver scheduler loop applying membership plans, external multi-process chaos |
| Snapshots | integrated storage/load pipeline | snapshot crate plus local Raft snapshot model, external snapshot refs, manifest/checksum/size verification, stale-local-state preflight, and process-level bootstrap restore into data-node Raft engine state | production engine freeze/flush/install lifecycle and local object-store E2E validation |
| Cache | mtcache/blockcache production cache | memory plus local disk block cache with page-address keys, versioned block envelope, zstd compression, atomic synced disk-block writes, metrics, and shard-level GC eviction | CacheLib/SSD cache parity, admission, advanced eviction policy, warmup, pinning |
| Feature | richer feature proto semantics | append/query/replace/delete/agg, write-policy append, 5k long-sequence coverage | nested point arrays, exact aggregate semantics |
| Sequence | C++ feature/data-module behavior | typed rows, timestamp ordering, inclusive/equality/inequality filters, count, batch query | exact C++ options and remaining edge-case policy |
| IPS | rich IPS add/query/remove/load/delete/stat/filter/snap | add, idempotent/dimensional add, query-last, query range, dimension-filtered range, batch query-last, remove timestamp, delete key, count range, load, range snapshot, stats, and named filter; typed client and RESP coverage | production snap metadata and server aggregation |
| Risk | H/CPC/FOL query/update/manager semantics | increment/count plus precision/TTL increment, sum/min/max/first/last/event aggregation, C++-style `CHANGE` distinct-field counting, detail list, H/CPC family set/query/set-and-get command shape, logical-key lifecycle handling for H/CPC/FOL records, explicit C++-style FOL first/last string selection, and local manager summary; typed client and RESP coverage | production CPC/list internals and full manager/debug APIs |
| Redis | not the main C++ wire API; C++ server also exposes admin commands such as `INFO`, `CONFIG`, `SLAVEOF`, and `PARTITION` | useful RESP compatibility, including `SET NX/XX GET EX/PX`, hash/set commands, feature commands, C++-style `INFO`/`CONFIG`/`SLAVEOF`/`AUTH`/`BGSAVE`/`PARTITION` smoke commands, and CRC64 slot/hash helpers | sorted sets/lists if needed; real partition-manager backing for admin commands |
| Metrics | production metrics/logging | Prometheus `/metrics` for shard/cache/page/oplog/runtime/object-manager/partition plus page-store zone lifecycle count/byte gauges, per-slot page-ref/byte/dirty gauges, and snapshot metric names; local raft metrics renderer | dashboards and alerts |
| Deployment | internal production environment | Docker and existing-EKS Terraform skeleton | service discovery, autoscale controller, rolling upgrade, runbooks, auth/TLS |
| Testing | mature internal tests and production history | local unit/integration/compat tests, storage recovery report tests covering oplog/index-log/page stream/zone manifest reopen, missing-manifest rebuild, zone lifecycle timestamps and capacity summaries, segment inspection summaries, live-density/stale-page estimates, page-GC byte accounting, delayed-destroy inventory/purge byte accounting, before/after compaction utility reporting, and process-level local storage abort/recover harness with page-segment corruption diagnostics | multi-process chaos, broader disk-fault crash recovery, perf benchmarks, C++ golden corpus |

## P0 Still Missing Before Distributed Alpha

These cannot be honestly marked done yet:

- replace local Raft consensus model with OpenRaft or raft-rs FSM/storage integration
- replace the in-process HTTP metaserver Raft backend with networked multi-process metaserver Raft
- make shard membership changes durable through metaserver Raft
- broaden crash-safe WAL/index/page/zone recovery tests across disk faults and multi-process cluster crashes
- wire engine snapshots into the full production freeze/flush/install lifecycle
- expand heartbeat/load-report payloads beyond local partition stats into the full C++ server
  heartbeat contract

## P1 Still Missing Before C++ Feature Parity

- exact C++ Feature proto nested point and aggregate semantics
- remaining IPS module details: production snap metadata and server aggregation
- remaining Risk module details: production CPC/list internals and full manager/debug APIs
- binary/protobuf-compatible oplog and index-log semantics where needed by the Rust migration API
- deeper object/page/slot layout and C++-style page rewrite garbage collection
- production cache backend
- shared-store replay offsets and retry/resume
- documented Rust HTTP/RESP SDK migration API

## Current Recommendation

Do not claim full C++ parity yet.

The next best implementation chunks are:

1. Extend the new durable zone manifest into a full object/page/slot storage lifecycle and merged dump/load policy.
2. Broaden durable WAL/recovery tests around oplog + index-log + page stream + zone manifest into disk-fault and multi-process cluster crash cases.
3. Replace the local Raft model with OpenRaft or raft-rs.
4. Connect metaserver table topology and data-node heartbeat reports to real placement/rebalance workflows.
5. Port IPS and Risk semantics from the C++ protos as separate modules.
