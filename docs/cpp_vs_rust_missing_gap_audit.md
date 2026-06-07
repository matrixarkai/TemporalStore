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
- proxy/client/metaserver/server binaries
- Redis RESP adapter
- S3-compatible snapshot store crate

It is still not production C++ TemporalStore parity. The largest missing areas are real distributed runtime, exact C++ protocol semantics, production storage lifecycle, and operational controls.

## What Was Closed In This Pass

Rust now has an `IndexLog` stream:

```text
write command -> append oplog -> write page bytes -> append index-log record -> persist whole index
```

Files:

- `crates/temporalstore-single-node/src/index_log.rs`
- `crates/temporalstore-single-node/src/control.rs`
- `crates/temporalstore-single-node/src/engine.rs`
- `crates/temporalstore-single-node/src/lib.rs`

This closes one data-node replay gap from the previous audit. C++ has binary index logs with page/object metadata. Rust now has a JSONL index-log record after every mutation. It is larger and simpler than C++, but gives replica/recovery code a separate stream of committed index metadata instead of relying only on the latest whole index file.

## Detailed Gap Matrix

| Area | C++ TemporalStore | Rust Today | Missing |
| --- | --- | --- | --- |
| Protocol | brpc/thrift/protobuf APIs and extension protos | JSON/HTTP command API plus RESP adapter | Exact wire compatibility, SDK compatibility, C++ protobuf request/response shapes |
| Routing | namespace/table/partition-set/slot routing | explicit `shard_id` in request, simple metaserver route | namespace/table model, slot hashing, route versioning, table config, partition-set placement |
| Metaserver | full topology, heartbeat, placement, scheduling, Raft-backed metadata | simple route map, in-process meta Raft, rebalance model | real networked metaserver Raft, persistent metadata, heartbeat/load reports, placement policy, background scheduler |
| Data node execution | partition workers, async callbacks, load-version guards | direct `TemporalEngine` execution under a lock | worker pools, request controllers, load-version validation, backpressure |
| Hot object model | `ObjectManager`, model objects, dirty slots | per-type maps of key/field/timestamp to `PageAddress` | object ids, dirty slot tracking, object lifecycle, model-specific memory layout |
| Oplog | binary mutation log with replay/reclaim semantics | JSONL command oplog | binary/protobuf compatibility, fsync policy, reclaim boundary, replay into hot object manager |
| Index log | binary metadata/index log | JSONL index-log with current index metadata | compact incremental deltas, page/object ids, checksums, replay ordering with oplog and page dumps |
| Page store | slot/page/zone layout, page headers, dump/merge/load | append-only local page segment files | zones, page headers, compaction, GC, checksums, atomic install |
| Shared store | local/ByteStore stream backends and replica replay | file-backed shared-store checkpoint and oplog replay model | ByteStore/S3 live object backend integration, production retry/resume, replay offsets |
| Raft | ByteRaft-backed production groups | in-process behavior model plus snapshot semantics | OpenRaft/raft-rs integration, network transport, durable WAL, hard-state, real InstallSnapshot RPC |
| Snapshots | integrated storage/load pipeline | S3-compatible snapshot crate plus local Raft snapshot model | engine freeze/flush, attach snapshot metadata to Raft, S3 restore into data-node FSM |
| Cache | mtcache/blockcache production cache | simple memory plus disk cache | CacheLib/SSD cache parity, admission, eviction policy, compression, metrics, warmup |
| Feature | richer feature proto semantics | append/query/replace/delete/agg, 5k long-sequence coverage | nested point arrays, exact write policies, exact aggregate semantics |
| Sequence | C++ feature/data-module behavior | typed rows, timestamp ordering, filters, count | exact C++ filters/options, batch semantics, edge-case policy |
| IPS | rich IPS add/query/remove/load/delete/stat/filter/snap | `IpsAdd`, `IpsQueryLast` | batch query, remove, load, delete, feature stats, action/table dimensions, idempotence |
| Risk | H/CPC/FOL query/update/manager semantics | `RiskIncrement`, `RiskCount` | precision buckets, windows, min/max/change operators, first/last, detail lists, manager APIs |
| Redis | not the main C++ wire API | useful RESP compatibility | more Redis commands/options such as `SET NX/XX`, sorted sets/lists if needed |
| Metrics | production metrics/logging | stats structs and snapshot Prometheus metric names | metrics HTTP endpoint, raft/cache/storage metrics, dashboards and alerts |
| Deployment | internal production environment | Docker and existing-EKS Terraform skeleton | service discovery, autoscale controller, rolling upgrade, runbooks, auth/TLS |
| Testing | mature internal tests and production history | local unit/integration/compat tests | multi-process chaos, crash recovery, AWS E2E, perf benchmarks, C++ golden corpus |

## P0 Still Missing Before Distributed Alpha

These cannot be honestly marked done yet:

- replace in-process Raft with a real Rust Raft library
- persist Raft WAL and hard state
- implement real data-node RPC transport
- implement real metaserver topology and slot routing
- make shard membership changes durable through metaserver Raft
- add crash-safe WAL/index/page recovery tests
- wire engine snapshots into Raft snapshot install
- expose Prometheus metrics over HTTP
- add heartbeat/load reporting from servers to metaserver

## P1 Still Missing Before C++ Feature Parity

- exact C++ Feature proto semantics
- full IPS module
- full Risk module
- binary/protobuf oplog and index-log formats
- page compaction and garbage collection
- production cache backend
- shared-store replay offsets and retry/resume
- C++ SDK compatibility or a documented migration API

## Current Recommendation

Do not claim full C++ parity yet.

The next best implementation chunks are:

1. Add durable WAL/recovery tests around oplog + index-log + page stream.
2. Add a replica replay loop that consumes checkpoint, page stream, index-log, and oplog.
3. Replace the local Raft model with OpenRaft or raft-rs.
4. Add metaserver table/slot topology and heartbeat/load reports.
5. Port IPS and Risk semantics from the C++ protos as separate modules.

