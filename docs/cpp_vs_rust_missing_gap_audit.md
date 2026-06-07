# C++ TemporalStore Vs Rust Missing Gap Audit

## Bottom Line

The Rust code now covers the local correctness skeleton for TemporalStore-style serving:

- command API for common, string, hash, set, feature, sequence, IPS-lite, and risk-lite
- local shard engine with page-address indexes
- local page segment files
- memory plus disk read-through cache
- whole index snapshot stream
- oplog stream
- index-log stream
- page stream
- shared-store checkpoint and oplog replay model
- in-process Raft behavior for data nodes and metaserver
- local Raft snapshot behavior
- ByteKV/ByteRaft-style Raft configuration defaults, validation, read options, oversized log rejection, and election prohibition
- ByteRaft-style local Raft status, local status, read-index guard, leader transfer, and Prometheus raft metrics
- proxy/client/metaserver/server binaries
- table-aware Rust client with typed string/hash/common methods, pipeline batching, HTTP timeout/retry options, and optional direct route refresh
- proxy route cache with TTL, stats/config endpoints, timeout/retry options, and backend-error route refresh
- client key-to-shard routing, table open/close cache, stats, expanded typed methods, and multi-shard pipeline grouping
- metaserver namespace/table topology, server/proxy register/list/heartbeat, topology versioning, and meta stats/info
- data-node checked execute/batch by load version, server registration, periodic heartbeat load reporting, and loaded-shard stats
- data-node worker runtime with bounded async queue, job status, dirty-object ids, dump/compact/GC hooks, and load-finish callback endpoint
- Redis RESP adapter, including conditional `SET NX/XX`, `GET`, `EX`, and `PX`
- Prometheus scrape output for shard records, cache, page-store, oplog, and data-node runtime counters
- S3-compatible snapshot store crate

It is still not production C++ TemporalStore parity. The largest missing areas are real distributed runtime, tonic/gRPC service definitions, production storage lifecycle, and operational controls.

brpc and Thrift are intentionally not part of the Rust target. The Rust open-source target is tonic/gRPC for internal service RPC, HTTP/JSON for admin/debug, RESP for Redis compatibility, and Prometheus text for metrics.

## What Was Closed In The Latest Pass

This pass closed another ByteRaft/ByteKV `RaftEngine` API/configuration gap:

- `RaftConfig` exposes the ByteKV/ByteRaft-style knobs used by the local C++ codebase: election cycle ticks, leader-transfer timeout, offline timeout, lease settings, startup lease assumption, max memory replicate-log bytes, max disk replicate-log count, max cache memory bytes, max apply batch bytes, reorder queue controls, in-flight apply/replication limits, pre-vote/election prohibition, snapshot send timeout, transport timeout, WAL sync, segment sizing, retained segment count, snapshot trigger switch, and max applied-log bytes.
- Defaults match the local C++/ByteKV defaults where the C++ source exposed them: election cycle `3`, max memory replicate-log bytes `32 KiB`, max disk replicate-log count `64`, max cache memory bytes `32 MiB`, max apply batch bytes `64 KiB`, raft transport timeout `1000 ms`, max segment bytes `64 MiB`, WAL sync disabled, and max applied-log bytes `1 GiB`.
- `RaftConfig::validate()` rejects unusable settings before constructing a data-node or metaserver Raft group.
- `RaftReadOptions` and `RaftReadStrategy` model the C++ `ReadOptions` shape: relaxed read, lease read, read-index, follower-read switch, fill-cache flag, ignore-write-intent flag, and wait timeout.
- data-node Raft and metaserver Raft now both support fallible constructors with explicit config, `config()` inspection, log-entry size enforcement, leader-only read enforcement, optional follower reads, and election prohibition.

This is API/config parity plus local-model enforcement. It is not yet the actual optimization implementation for reorder queues, inflight replication windows, WAL sync/segments, network transport timeout, or pre-vote. Those fields become operationally meaningful when the in-process model is replaced by OpenRaft/raft-rs plus durable WAL and transport.

The previous pass closed a ByteRaft/ByteKV `RaftEngine` behavior gap:

- `RaftClusterStatus` and `RaftNodeStatus` for leader, term, commit index, majority, live voters, lease-valid state, per-node lag, and per-node role
- `ReadIndexResponse` and `read_index(node_id)` for safe local-model reads; lagging replicas are rejected before serving read-index
- `transfer_leader(node_id)` for both data Raft and metaserver Raft; lagging or dead targets are rejected
- `local_status(node_id)` matching the shape of ByteRaft's local status inspection
- `prometheus_metrics()` for data Raft and metaserver Raft with commit index, live voters, majority, lease validity, per-node commit, lag, and liveness
- tests for data-Raft status/read-index/leader-transfer, lagging replica rejection, and metaserver-Raft status/read-index/leader-transfer

This is still an in-process correctness model, not a replacement for OpenRaft/raft-rs with durable WAL and network transport.

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

- `crates/temporalstore-single-node/src/index_log.rs`
- `crates/temporalstore-single-node/src/control.rs`
- `crates/temporalstore-single-node/src/engine.rs`
- `crates/temporalstore-single-node/src/proxy.rs`
- `crates/temporalstore-single-node/src/bin/proxy.rs`
- `crates/temporalstore-single-node/src/lib.rs`

This closes one data-node replay gap from the previous audit. C++ has binary index logs with page/object metadata. Rust now has a JSONL index-log record after every mutation. It is larger and simpler than C++, but gives replica/recovery code a separate stream of committed index metadata instead of relying only on the latest whole index file.

The C++ proxy is still much richer, but Rust no longer blindly looks up the metaserver on every forwarded request. The Rust proxy now owns a reusable `ProxyService` with route caching, forced refresh, backend retry/refresh behavior, basic runtime config, and observable request/cache/error counters.

The Rust client also now has a small table router: `key -> stable hash -> shard_id`. Pipelines split queued commands by routed shard and reassemble responses in original order, matching the shape of the C++ client batch path at a simpler HTTP/shard level.

The Rust metaserver now exposes a richer HTTP/JSON control-plane skeleton: server/proxy inventory, heartbeat liveness, namespace/table metadata, and table topology with slot ranges, shard ids, primary endpoint, replicas, and topology-version not-modified behavior.

The Rust data node now exposes C++-style checked execution routes that reject stale `load_version` requests, and the server binary reports loaded shard stats to the metaserver heartbeat endpoint.

The Rust data node also has a first runtime layer around `TemporalEngine`: worker threads, bounded queue/backpressure, async job handles, dirty-object ids for mutating commands, and explicit dump/compact/GC task hooks.

## Detailed Gap Matrix

| Area | C++ TemporalStore | Rust Today | Missing |
| --- | --- | --- | --- |
| Protocol | brpc/thrift/protobuf APIs and extension protos | JSON/HTTP command API plus RESP adapter; brpc/thrift intentionally excluded | tonic/gRPC service definitions, prost message schema, SDK compatibility for the new API |
| Proxy | brpc/thrift server, C++ client wrapper, MetaSyncer, heartbeat/config, consul registration | HTTP proxy service with `/execute`, `/batch_execute`, `/shards`, `/proxy/info`, `/proxy/config`, route cache, stats, retries/timeouts, backend-error route refresh | tonic proxy service, background MetaSyncer, heartbeat/config from metaserver, service discovery/auto-register, namespace/table open path |
| Client SDK | C++ `Client`, `Table`, `Pipeline`, `MetaSyncer`, router, backend pool | Rust `TemporalStoreClient`, `TemporalStoreTable`, `TemporalStorePipeline`, typed methods, open/close table cache, stats, retries/timeouts, direct route refresh | tonic SDK, background topology sync, exact CRC64 slot router, VDC affinity, backend failure pool |
| Routing | namespace/table/partition-set/slot routing | key-to-shard routing from table options, explicit `shard_id` request, simple metaserver route | namespace/table topology from metaserver, exact slot map, route versioning, partition-set placement |
| Metaserver | full topology, heartbeat, placement, scheduling, Raft-backed metadata | shard route map, namespace/table topology, server/proxy register/list/heartbeat, meta stats, in-process meta Raft, rebalance model | networked metaserver Raft for HTTP mutations, persistent metabase, placement rule chain, background scheduler, failure detector |
| Data node execution | partition workers, async callbacks, load-version guards | `TemporalEngine` plus `DataNodeRuntime` worker queue, async jobs, dirty tracking, checked execute/batch routes | tonic streaming/callback shape, in-flight cancellation, production scheduling |
| Hot object model | `ObjectManager`, model objects, dirty slots | per-type maps of key/field/timestamp to `PageAddress` | object ids, dirty slot tracking, object lifecycle, model-specific memory layout |
| Oplog | binary mutation log with replay/reclaim semantics | JSONL command oplog | binary/protobuf compatibility, fsync policy, reclaim boundary, replay into hot object manager |
| Index log | binary metadata/index log | JSONL index-log with current index metadata | compact incremental deltas, page/object ids, checksums, replay ordering with oplog and page dumps |
| Page store | slot/page/zone layout, page headers, dump/merge/load | append-only local page segment files plus dump/compact/GC task hooks | zones, page headers, real segment rewrite compaction, checksums, atomic install |
| Shared store | local/ByteStore stream backends and replica replay | file-backed shared-store checkpoint and oplog replay model | ByteStore/S3 live object backend integration, production retry/resume, replay offsets |
| Raft | ByteRaft-backed production groups | in-process behavior model plus snapshot semantics, ByteKV/ByteRaft-style config/read options, oversized log guard, election prohibition, status/local-status, read-index guard, leader transfer, raft metrics | OpenRaft/raft-rs integration, network transport, durable WAL, hard-state, real InstallSnapshot RPC, real reorder queue/inflight/WAL segment/pre-vote behavior |
| Snapshots | integrated storage/load pipeline | S3-compatible snapshot crate plus local Raft snapshot model | engine freeze/flush, attach snapshot metadata to Raft, S3 restore into data-node FSM |
| Cache | mtcache/blockcache production cache | simple memory plus disk cache | CacheLib/SSD cache parity, admission, eviction policy, compression, metrics, warmup |
| Feature | richer feature proto semantics | append/query/replace/delete/agg, 5k long-sequence coverage | nested point arrays, exact write policies, exact aggregate semantics |
| Sequence | C++ feature/data-module behavior | typed rows, timestamp ordering, filters, count | exact C++ filters/options, batch semantics, edge-case policy |
| IPS | rich IPS add/query/remove/load/delete/stat/filter/snap | add, query-last, remove timestamp, delete key, count range | batch query, load/snapshot/stat/filter, action/table dimensions, idempotence |
| Risk | H/CPC/FOL query/update/manager semantics | increment/count plus sum/min/max/event aggregation | precision buckets, first/last, detail lists, manager APIs |
| Redis | not the main C++ wire API | useful RESP compatibility, including `SET NX/XX GET EX/PX` | sorted sets/lists if needed |
| Metrics | production metrics/logging | Prometheus `/metrics` for shard/cache/page/oplog/runtime plus snapshot metric names; local raft metrics renderer | dashboards and alerts |
| Deployment | internal production environment | Docker and existing-EKS Terraform skeleton | service discovery, autoscale controller, rolling upgrade, runbooks, auth/TLS |
| Testing | mature internal tests and production history | local unit/integration/compat tests | multi-process chaos, crash recovery, AWS E2E, perf benchmarks, C++ golden corpus |

## P0 Still Missing Before Distributed Alpha

These cannot be honestly marked done yet:

- replace in-process Raft with a real Rust Raft library
- persist Raft WAL and hard state
- implement real data-node tonic/gRPC transport
- connect HTTP metaserver mutations to durable/networked metaserver Raft
- make shard membership changes durable through metaserver Raft
- add crash-safe WAL/index/page recovery tests
- wire engine snapshots into Raft snapshot install
- expand heartbeat/load-report payloads and connect them to placement decisions

## P1 Still Missing Before C++ Feature Parity

- exact C++ Feature proto semantics
- remaining IPS module details: batch query, load/snapshot/stat/filter, action/table dimensions, idempotence
- remaining Risk module details: precision buckets, first/last, detail lists, manager APIs
- binary/protobuf oplog and index-log formats
- page compaction and garbage collection
- production cache backend
- shared-store replay offsets and retry/resume
- Rust/tonic SDK compatibility and a documented migration API

## Current Recommendation

Do not claim full C++ parity yet.

The next best implementation chunks are:

1. Add durable WAL/recovery tests around oplog + index-log + page stream.
2. Add a replica replay loop that consumes checkpoint, page stream, index-log, and oplog.
3. Replace the local Raft model with OpenRaft or raft-rs.
4. Connect metaserver table topology and data-node heartbeat reports to real placement/rebalance workflows.
5. Port IPS and Risk semantics from the C++ protos as separate modules.
