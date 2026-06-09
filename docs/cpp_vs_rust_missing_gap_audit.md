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

It is still not production C++ TemporalStore parity. The largest missing areas are tonic/gRPC service definitions, production storage lifecycle, and operational controls.

brpc and Thrift are intentionally not part of the Rust target. The Rust open-source target is tonic/gRPC for internal service RPC, HTTP/JSON for admin/debug, RESP for Redis compatibility, and Prometheus text for metrics.

## What Was Closed In The Latest Pass

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

This is API/config parity plus local-model enforcement. It is not yet the actual optimization implementation for reorder queues, inflight replication windows, WAL sync/segments, network transport timeout, or pre-vote. Those fields become operationally meaningful when the in-process model is replaced by OpenRaft/raft-rs plus durable WAL and transport.

The previous pass closed a ByteRaft/ByteKV `RaftEngine` behavior gap:

- `RaftClusterStatus` and `RaftNodeStatus` for leader, term, commit index, majority, live voters, lease-valid state, per-node lag, and per-node role
- `ReadIndexResponse` and `read_index(node_id)` for safe local-model reads; lagging replicas are rejected before serving read-index
- `transfer_leader(node_id)` for both data Raft and metaserver Raft; lagging or dead targets are rejected
- `local_status(node_id)` matching the shape of ByteRaft's local status inspection
- `prometheus_metrics()` for data Raft and metaserver Raft with commit index, live voters, majority, lease validity, per-node commit, lag, and liveness
- tests for data-Raft status/read-index/leader-transfer, lagging replica rejection, and metaserver-Raft status/read-index/leader-transfer

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

| Area | C++ TemporalStore | Rust Today | Missing |
| --- | --- | --- | --- |
| Protocol | brpc/thrift/protobuf APIs and extension protos | JSON/HTTP command API plus RESP adapter; brpc/thrift intentionally excluded | tonic/gRPC service definitions, prost message schema, SDK compatibility for the new API |
| Proxy | brpc/thrift server, C++ client wrapper, MetaSyncer, heartbeat/config, consul registration | HTTP proxy service with `/execute`, `/batch_execute`, `/shards`, `/proxy/info`, `/proxy/config`, `/proxy/heartbeat`; forwarding delegates through `TemporalStoreClient` for route cache, stats sync, retries/timeouts, backend-error route refresh, continuous-failure bypass; background heartbeat loop and heartbeat auto-register helper | tonic proxy service, service discovery/consul, namespace/table open path |
| Client SDK | C++ `Client`, `Table`, `Pipeline`, `MetaSyncer`, router, backend pool | Rust `TemporalStoreClient`, `TemporalStoreTable`, `TemporalStorePipeline`, typed methods, open/close table cache, stats, retries/timeouts, direct route refresh, per-backend continuous-failure windows, backend error streak tracking, open table from metaserver topology, background topology sync, C++ `crc64 >> 34` slot router, primary routing for writes, optional first-secondary routing for reads | tonic SDK, VDC affinity, full partition-set hierarchy, Neptune/drop-percent routing |
| Routing | namespace/table/partition-set/slot routing | key-to-shard routing from table options using C++ CRC64 slot formula, explicit `shard_id` request, simple metaserver route, topology-cached primary/replica endpoint choice, C++ `PartitionId` bit layout helper and opt-in C++-encoded table partition ids | full partition-set hierarchy, route versioning, placement hierarchy |
| Metaserver | full topology, heartbeat, placement, scheduling, Raft-backed metadata | shard route map, namespace/table topology, opt-in C++ `PartitionId` generation for table partitions, load-aware replica fill with location diversity, server/proxy register/list/heartbeat, stale resource failure-detector loop, durable local JSONL mutation log/replay, meta stats, optional Raft-backed HTTP mutation path through `ProductionMetaRaftRuntime`, rebalance model, C++-style update-membership task model with sibling filtering, threshold checks, not-found-as-reboot success, FSM-submit gating, deterministic priority task scheduler model with retry-later backoff and abort handling, scheduler snapshot/restore, freezing-shard repair into UpdateMembership tasks | networked multi-process metaserver Raft transport, full host/cooldown placement rule chain, full background scheduler loop |
| Data node execution | partition workers, async callbacks, load-version guards | `TemporalEngine` plus `DataNodeRuntime` shard-affine worker lanes, bounded foreground/background queue admission, foreground-over-background scheduler priority, per-shard FIFO single-lane execution, cross-shard parallelism, async jobs, cancel status surface, dirty tracking, checked execute/batch routes, invalid stream range rejection, C++-style duplicate-load/not-found-unload/config-not-found handling, C++-style membership update version guards and local-membership validity reporting | tonic streaming/callback shape, hard in-flight cancellation, full production tenant quotas/admission |
| Hot object model | `ObjectManager`, model objects, dirty slots | per-type maps of key/field/timestamp to `PageAddress` plus C++-style stats for logical object count, page refs, dirty objects, dirty routing slots, and partition info | stable object ids/page ids, dirty-slot dump scheduler, object lifecycle, model-specific memory layout |
| Oplog | binary mutation log with replay/reclaim semantics | JSONL command oplog with explicit retain-from-sequence GC rewrite | binary/protobuf compatibility, fsync policy, replay into hot object manager |
| Index log | binary metadata/index log | JSONL index-log with current index metadata and explicit retain-from-sequence GC rewrite | compact incremental deltas, page/object ids, checksums, replay ordering with oplog and page dumps |
| Page store | slot/page/zone layout, page headers, dump/merge/load | append-only local page segment files plus dump/compact/GC task hooks and conservative old segment deletion | zones, page headers, real segment rewrite compaction, checksums, atomic install |
| Shared store | local/ByteStore stream backends and replica replay | file-backed shared-store checkpoint, sync/async oplog publish, bounded async flush, checksum-enveloped oplog objects, strict gap-rejecting replay, persisted replay cursor, bounded object-store retry and async requeue-on-failure, oplog/checkpoint GC | ByteStore/S3 live object backend integration, automatic lifecycle safety tied to follower cursors/Raft snapshots |
| Raft | ByteRaft-backed production groups | separate-node data-node Raft runtime wrapper with OpenRaft/raft-rs engine selection, production metaserver Raft runtime wrapper, mTLS config validation, authenticated RPC runtime construction, timer supervisors, metaserver stale-server detection in Raft mode, multi-process chaos plan validation, in-process behavior model plus snapshot semantics, HTTP Raft transport for AppendEntries/Vote/InstallSnapshot/chunked InstallSnapshot, timeout tick election with pre-vote, randomized scheduler model, hard-state/membership inspection, local durable segmented WAL record export/load/recovery with sync, retention, corrupt-tail truncation, and installed-snapshot recovery floor, auto-persisting WAL-backed cluster mode, bounded follower catch-up with replay progress and lag reports, bounded local WAL retention, AppendEntries/Vote/InstallSnapshot local receive behavior, joint-consensus old/new majority safety model, safe add/remove/replace membership-change planner and report, metaserver topology to data-Raft voter-plan bridge with no-op and server-state validation, Raft RPC retry/backpressure/auth/deadline wrapper, local partition/heal chaos coverage, ByteKV/ByteRaft-style config/read options, oversized log guard, election prohibition, status/local-status, read-index guard, leader transfer, raft metrics | OpenRaft/raft-rs FSM/storage integration, actual mTLS transport, real snapshot install, scheduler loop applying membership plans, external multi-process chaos |
| Snapshots | integrated storage/load pipeline | S3-compatible snapshot crate plus local Raft snapshot model | engine freeze/flush, attach snapshot metadata to Raft, S3 restore into data-node FSM |
| Cache | mtcache/blockcache production cache | memory plus local disk block cache with page-address keys, versioned block envelope, zstd compression, metrics, and shard-level GC eviction | CacheLib/SSD cache parity, admission, advanced eviction policy, warmup, pinning |
| Feature | richer feature proto semantics | append/query/replace/delete/agg, write-policy append, 5k long-sequence coverage | nested point arrays, exact aggregate semantics |
| Sequence | C++ feature/data-module behavior | typed rows, timestamp ordering, filters, count, batch query | exact C++ filters/options and edge-case policy |
| IPS | rich IPS add/query/remove/load/delete/stat/filter/snap | add, idempotent/dimensional add, query-last, query range, dimension-filtered range, batch query-last, remove timestamp, delete key, count range, load, range snapshot, stats, and named filter; typed client and RESP coverage | production snap metadata and server aggregation |
| Risk | H/CPC/FOL query/update/manager semantics | increment/count plus precision/TTL increment, sum/min/max/first/last/event aggregation, detail list, H/CPC/FOL family set/query/set-and-get command shape, and local manager summary; typed client and RESP coverage | production CPC/list internals and full manager/debug APIs |
| Redis | not the main C++ wire API; C++ server also exposes admin commands such as `INFO`, `CONFIG`, `SLAVEOF`, and `PARTITION` | useful RESP compatibility, including `SET NX/XX GET EX/PX`, hash/set commands, feature commands, C++-style `INFO`/`CONFIG`/`SLAVEOF`/`AUTH`/`BGSAVE`/`PARTITION` smoke commands, and CRC64 slot/hash helpers | sorted sets/lists if needed; real partition-manager backing for admin commands |
| Metrics | production metrics/logging | Prometheus `/metrics` for shard/cache/page/oplog/runtime/object-manager/partition plus snapshot metric names; local raft metrics renderer | dashboards and alerts |
| Deployment | internal production environment | Docker and existing-EKS Terraform skeleton | service discovery, autoscale controller, rolling upgrade, runbooks, auth/TLS |
| Testing | mature internal tests and production history | local unit/integration/compat tests | multi-process chaos, crash recovery, AWS E2E, perf benchmarks, C++ golden corpus |

## P0 Still Missing Before Distributed Alpha

These cannot be honestly marked done yet:

- implement real data-node tonic/gRPC transport
- replace local Raft consensus model with OpenRaft or raft-rs FSM/storage integration
- replace the in-process HTTP metaserver Raft backend with networked multi-process metaserver Raft
- make shard membership changes durable through metaserver Raft
- add crash-safe WAL/index/page recovery tests
- wire engine snapshots into Raft snapshot install
- expand heartbeat/load-report payloads beyond local partition stats into the full C++ server
  heartbeat contract

## P1 Still Missing Before C++ Feature Parity

- exact C++ Feature proto nested point and aggregate semantics
- remaining IPS module details: production snap metadata and server aggregation
- remaining Risk module details: production CPC/list internals and full manager/debug APIs
- binary/protobuf oplog and index-log formats
- page compaction and C++-style page rewrite garbage collection
- production cache backend
- shared-store replay offsets and retry/resume
- Rust/tonic SDK compatibility and a documented migration API

## Current Recommendation

Do not claim full C++ parity yet.

The next best implementation chunks are:

1. Add durable WAL/recovery tests around oplog + index-log + page stream.
2. Replace the local Raft model with OpenRaft or raft-rs.
3. Connect metaserver table topology and data-node heartbeat reports to real placement/rebalance workflows.
4. Port IPS and Risk semantics from the C++ protos as separate modules.
