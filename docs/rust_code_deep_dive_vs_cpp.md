# Rust TemporalStore Deep Dive Vs C++

## Executive Summary

The Rust repo is a clean open-source TemporalStore-shaped implementation, but it is not yet a drop-in replacement for the internal C++ TemporalStore.

What exists today:

- a typed command API for common, string, hash, set, feature, sequence, and the implemented IPS/Risk subset
- a TemporalStore Rust engine with shard-local indexes, local append-only page segment files, and read-through memory/disk cache
- HTTP binaries for metaserver, server, proxy, and client
- a Redis RESP proxy covering common string/hash/set commands plus feature, IPS, and Risk extensions
- an in-process Raft behavior model for data replication, metaserver replication, promotion, scale up/down, and replica read policy tests
- an S3-compatible snapshot crate with manifest-last visibility, checksum verification, retention, stale restore guard, and Prometheus metric names
- local smoke scripts, AWS existing-EKS Terraform, and compatibility-style tests

What is still missing versus C++:

- production networked Raft and durable Raft WAL
- C++-compatible metaserver topology, namespace/table model, and slot routing
- C++ ObjectManager hot object model, dirty slot tracking, oplog, dump/recover pipeline, and readonly replica replay
- full Feature, IPS, and Risk semantics from the C++ extension protos
- ByteStore/local stream backend parity, blockcache/mtcache parity, quota/admission, heartbeat/load reporting, SDK compatibility, metrics server, dashboards, and operational tooling

So the right framing is: the Rust code is a good open-source v1 skeleton and local correctness harness. The C++ code is still the production reference for complete TemporalStore behavior.

## Rust Workspace Map

| Area | Main files | What it does |
| --- | --- | --- |
| Public API model | `crates/temporalstore-rust/src/types.rs` | Defines `Command`, `CommandResponse`, request/response structs, shard id, feature/risk/sequence shapes. |
| Engine | `crates/temporalstore-rust/src/engine.rs` | Owns loaded shard state, executes commands, appends values to page files, updates indexes, persists index JSON, reads through cache/page store. |
| Page storage | `crates/temporalstore-rust/src/page_store.rs` | Appends raw bytes to local `page_segment_*.seg` files and returns `PageAddress { page_segment_id, offset, length }`. |
| Multi-layer cache | `crates/temporalstore-rust/src/cache.rs` | L1 in-memory cache plus L2 local disk block cache. Disk blocks use a versioned envelope with optional zstd compression. Disk hits are decoded and promoted back to memory. Writes invalidate affected keys. |
| Control API | `crates/temporalstore-rust/src/control.rs` | Load/unload shard, config, info/stats, membership update, stream read/scan structs. |
| HTTP transport | `crates/temporalstore-rust/src/http.rs`, `meta.rs`, `client.rs` | Small synchronous JSON-over-HTTP surface for local server/proxy/metaserver/client workflows. |
| Redis interface | `crates/temporalstore-rust/src/redis.rs` | RESP parser/server and mapper from Redis-like commands to `Command`. |
| Raft model | `crates/temporalstore-rust/src/raft.rs` | In-process data Raft and metaserver Raft behavior model: majority commit, promotion, catch-up, scale up/down, safe replica reads. |
| End-to-end workflow | `crates/temporalstore-rust/src/e2e.rs` | Simulates proxy -> client -> metaserver raft -> data raft -> engine, kill switches, async storage queue, read policies. |
| Binaries | `crates/temporalstore-rust/src/bin/*.rs` | `metaserver`, `server`, `proxy`, `redis_proxy`, `client`. |
| S3 snapshots | `crates/temporalstore-snapshot/src/*.rs` | Snapshot trait/store, S3-compatible object abstraction, manifest/checksum types, Prometheus metrics. |
| Tests | `crates/*/tests`, module unit tests | Compatibility, cache/page persistence, Redis, Raft behavior, snapshot semantics, AWS ignored smoke. |
| Deployment | `Dockerfile`, `infra/aws-existing-eks`, `tools/*.sh` | Docker build, existing-EKS Terraform, local/AWS validation scripts. |

## Exact Rust Data Path

TemporalStore Rust read/write path:

```text
HTTP/Redis/client request -> proxy/client -> shard_id -> TemporalEngine -> ShardState index -> PageAddress { segment, offset, length } -> LocalPageStore page_segment file -> MultiLayerCache -> response
```

Modeled distributed path:

```text
proxy -> RoutingClient -> MetaRaftCluster route -> RaftCluster leader/follower -> TemporalEngine on chosen replica -> ShardState index -> page bytes/cache -> response
```

This is deliberately simpler than C++. In Rust today, `shard_id` is explicit in the request. C++ computes routing from key and table topology, roughly:

```text
key -> crc64 slot -> partition set -> primary/secondary server -> worker -> object/page/index -> response
```

## Engine Deep Dive

`TemporalEngine` owns loaded shard maps behind an `RwLock`. A `ShardState` contains separate indexes per logical data type:

- `strings: key -> PageAddress`
- `hashes: key -> field -> PageAddress`
- `sets: key -> member -> PageAddress`
- `features: key -> timestamp -> PageAddress`
- `sequences: key -> timestamp -> PageAddress`
- `ips: key -> timestamp -> PageAddress`
- `risk: key -> timestamp -> i64`
- `expires_at_ms: key -> expire timestamp`

For most data types, Rust stores bytes in the local page store and keeps only the page address in the index. On mutation, the engine appends a new value, updates the per-shard index, invalidates related cache entries, and persists the full shard index as JSON. On read miss, it follows the stored `PageAddress` into the page segment file, reads the bytes, caches the page bytes under a page-address block key, builds the response, and caches the serialized response.

Important current behavior:

- local file persistence works for page bytes plus JSON index
- read miss falls back to local page file using the address in the index
- page-block and response-cache fill happens after page-file read
- expired keys are lazily removed
- `feature_max_size` defaults to `5000`
- `StreamKind::Page` and `StreamKind::Index` can read local page/index bytes
- `StreamKind::Oplog` currently returns empty bytes

The big difference from C++ is that Rust has no separate hot object manager, dirty slot scheduler, append-only mutation log, or merged background dump loop. It writes individual values to page segments immediately and persists index JSON after mutations.

## Cache Deep Dive

The Rust `MultiLayerCache` has two local layers:

- L1: process memory with byte capacity and FIFO-style eviction
- L2: local disk cache files under shard/type directories

Read behavior:

```text
response/page memory hit -> return
response/page disk hit -> decode block envelope -> promote to memory -> return
cache miss -> engine reads page file by PageAddress -> cache page bytes and response -> return
```

This proves the desired memory -> local block cache -> local page file path. The L2 cache now has page-address keys, a serialized `TSBCACHE` block envelope, zstd compression for compressible blocks, and legacy raw-block decode. It is still not CacheLib, mtcache, blockcache, or a production SSD cache. Missing production cache pieces include admission policy, advanced eviction policy, write amplification control, warmup, pinning, and integration with a production page/block cache API.

## Page Store Deep Dive

`LocalPageStore` is an append/read abstraction over local segment files. Each append returns:

```text
PageAddress {
  page_segment_id: u64,
  offset: u64,
  length: u64
}
```

That address is valid on the node whose page files and index files are available. It is not yet a distributed address. If replicas synchronize only by copying primary local files, the address can remain valid only if the replica receives the same segment bytes at the same segment id and offset. Production replication should replicate logical operations through Raft and/or install an immutable snapshot that preserves page/index layout.

For S3/shared storage later, the equivalent address needs a stable object identity:

```text
object_uri or segment_id + offset + length + checksum/version
```

The snapshot crate already has the right shape for immutable segments, but the engine does not yet read live pages directly from S3.

## Redis/API Deep Dive

The Rust API is centered on `Command`. Redis commands are adapters into that API.

Supported Redis-style commands include:

- common/string: `PING`, `GET`, `MGET`, `GETDEL`, `SET`, `MSET`, `SETEX`, `PSETEX`, `EXISTS`, `DEL`, `EXPIRE`, `PEXPIRE`, `TTL`, `PTTL`
- hash: `HSET`, `HMSET`, `HGET`, `HMGET`, `HGETALL`, `HLEN`, `HINCRBY`, `HDEL`
- set: `SADD`, `SMEMBERS`, `SREM`
- feature extensions: `FAPPEND`, `FQUERY`, `FREPLACE`, `FDEL`, `FAGG`

C++ TemporalStore has richer protocol surfaces through brpc/thrift/protobuf extension APIs. The Rust RESP layer is useful for local compatibility and load tests, but it does not yet provide C++ SDK wire compatibility.

## Raft/Replication Deep Dive

The Rust `RaftCluster` and `MetaRaftCluster` are behavior models, not real distributed Raft libraries.

Implemented behavior:

- majority check before writes
- leader/follower roles
- append log entry to live replicas
- commit and apply to each live replica engine
- reject writes without majority
- follower catch-up after outage
- manual and automatic leader promotion
- scale up with caught-up replica
- scale down and continue if majority remains
- read policies:
  - default `pin_primary = true`
  - explicit replica read
  - any live replica
  - lagging replica read rejection
- metaserver metadata replication for shard location
- metaserver read from any live committed replica

Missing production pieces:

- real network transport
- persistent Raft log
- snapshots wired into Raft install-snapshot
- joint consensus/member change protocol
- leader leases/read-index
- backpressure
- compaction
- failure detectors
- by-shard Raft group lifecycle
- security/auth/TLS
- metrics for raft internals

For a production Rust rewrite, this module should be replaced with OpenRaft or raft-rs instead of extended into a homegrown consensus implementation.

## Snapshot Deep Dive

The `temporalstore-snapshot` crate implements the S3 snapshot plan as a storage abstraction:

- `SnapshotStore` trait
- `S3SnapshotStore<O: ObjectStore>`
- file-backed object store for local tests
- manifest-last upload visibility
- temp prefix cleanup on failed upload
- checksum verification
- stale snapshot install guard
- retention garbage collection
- Prometheus metric registration and observation helpers

Snapshot layout matches the planned model:

```text
<cluster_id>/shards/<shard_id>/snapshots/<term>-<last_log_index>-<snapshot_id>/
  manifest.json
  index.bin
  checksums.json
  page_segments/*.seg
```

What is missing is integration with the data-node Raft implementation and engine snapshot creation. Today, snapshot tests use sample local files. Production needs:

- freeze/flush engine state
- create index/page snapshot from actual shard
- upload snapshot
- record snapshot metadata in Raft
- restore into a new data node
- catch up from logs after snapshot index

## C++ TemporalStore Reference Shape

From the C++ deep-dive docs and local source, C++ TemporalStore is a mature serving engine with these core subsystems:

- metaserver namespace/table/partition-set topology
- key hashing and slot routing
- primary/secondary serving roles
- partition workers and command executors
- hot `ObjectManager` state
- model-specific object implementations
- append-only `OpLogger`
- dirty index and slot tracking
- background `StorageManager` dump/merge/load
- `SlotStore` and `PageStore`
- local file and ByteStore stream backends
- optional block/page cache
- readonly replica replay from primary
- heartbeat/load reporting
- quota and admission control
- brpc/thrift/protobuf SDK/runtime integration
- ByteRaft/internal raft dependency
- richer Feature, IPS, and Risk modules

That is why "rewrite C++ TemporalStore in Rust" is not just translating syntax. It means replacing a storage engine, routing layer, distributed metadata service, replication runtime, model-specific feature stores, deployment flow, and internal dependencies.

## Feature Parity Matrix

| Subsystem | Rust status | C++ status | Gap |
| --- | --- | --- | --- |
| Common/string/hash/set APIs | Mostly present, including Redis `SET NX/XX GET EX/PX` | Mature | Missing exact internal wire compatibility. |
| Feature API | Append/query/replace/delete/agg plus write-policy append with typed client and RESP coverage | Rich feature point/range/write policy support | Need C++ proto-compatible nested feature shape and exact aggregate semantics. |
| Sequence API | Typed rows, filters, and batch query | Part of richer feature/data modules | Need exact C++ sequence edge-case policy if required by callers. |
| IPS | Add/query-last/range/batch/remove/delete/count plus idempotent/dimensional add and dimension-filtered range with typed client and RESP coverage | Rich add/batch query/remove/load/delete/stat/filter/snap behavior | Missing load/snapshot/stat/filter and server aggregation. |
| Risk | Increment/count plus precision/TTL increment, sum/min/max/first/last/events/detail with typed client and RESP coverage | Rich H/CPC/FOL/query/manager/window/precision semantics | Missing CPC/list-specific behavior and manager APIs. |
| Local storage | Local page segments + JSON index | Oplog + page/slot store + dump/recover | Need WAL/oplog, dump scheduler, recovery boundaries, compaction. |
| Cache | Simple memory + disk cache | mtcache/blockcache-like production cache | Need production SSD cache engine or Rust equivalent. |
| Replication | In-process behavior model | ByteRaft-backed production replication | Need real Raft library and networked nodes. |
| Metaserver | Simple route + in-process raft model | Full topology and routing control plane | Need namespace/table/shard/slot topology and heartbeat. |
| Read policy | Pin-primary default, optional replica reads | Primary/secondary serving with readonly replay | Need real lag control/read-index/lease semantics. |
| Snapshot | S3-compatible snapshot store crate | C++ storage/load ecosystem, ByteStore/local streams | Need actual engine/Raft integration. |
| Redis | Useful RESP adapter | C++ has product protocol/SDKs, not just Redis | Need exact client compatibility depending on migration target. |
| Deployment | Docker + existing-EKS Terraform | Internal production deployment system | Need service discovery, autoscale controllers, dashboards, runbooks. |
| Observability | Snapshot metrics and local stats | Production metrics/logging/tracing | Need metrics HTTP endpoint and raft/storage/cache dashboards. |

## Pros Of The Rust Rewrite

- **Open-source friendly:** avoids direct dependence on internal non-open-source `byte`, `byteraft`, `mtcache`, ByteStore, and related infrastructure.
- **Safer implementation base:** Rust reduces memory safety and lifetime bugs common in large C++ storage engines.
- **Clear API model:** one central `Command` enum makes behavior easy to inspect, serialize, test, and wrap with Redis/HTTP/proxy layers.
- **Fast local iteration:** TemporalStore Rust engine and in-process workflow tests run without a full internal deployment.
- **Cleaner naming:** uses `shard`, `page_segment`, `PageAddress`, and typed APIs instead of carrying every historical C++ name forward.
- **S3 snapshot design is explicit:** snapshot code has immutable object layout, checksum verification, retention, and metrics from the start.
- **Good migration harness:** compatibility-style tests can be expanded from C++ behavior into Rust without booting a whole production stack.

## Cons/Risks Of The Rust Rewrite

- **Not production equivalent yet:** the current implementation proves flows but does not have production storage, replication, or topology.
- **Consensus is only modeled:** the local Raft module must be replaced by a real Raft implementation.
- **Storage durability is incomplete:** page files plus JSON indexes are useful for local tests, but production needs WAL/oplog, atomic install, compaction, crash recovery, and checksums.
- **C++ semantics are deep:** IPS and Risk alone are large domain modules, not small Redis-style command aliases.
- **Performance is unknown:** simple locks, JSON indexes, local disk cache, and per-value appends are not benchmarked against C++.
- **Wire compatibility is not done:** Redis compatibility is helpful, but C++ clients expect brpc/thrift/protobuf-specific behavior.
- **Ops surface is thin:** no metrics server, dashboards, autoscale controller, production service discovery, auth, TLS, or chaos suite yet.

## Pros Of Keeping/Using The C++ Code

- **Mature engine:** already has object manager, oplog, page/slot dump/load, stream backends, and production-tested data modules.
- **Operational reality:** metaserver routing, primary/secondary roles, readonly replay, and internal deployment assumptions are already represented.
- **Complete domain semantics:** Feature, IPS, Risk, and related models are much richer than the Rust subset.
- **Existing clients:** C++ protocol/SDK integration is already aligned with current callers.
- **Performance history:** cache, page store, background dump, and worker architecture were designed for low-latency serving.

## Cons Of Keeping/Using The C++ Code

- **Hard to open source:** critical dependencies like `byte`, `byteraft`, `mtcache`, ByteStore, and likely internal build/runtime pieces are not open source.
- **Complex build/dependency graph:** brpc/protobuf/library ABI details make external builds harder.
- **Harder to simplify:** historical naming and internal architecture make a clean public product harder to explain and maintain.
- **Memory safety risk:** C++ needs stricter review and testing discipline for lifetime, concurrency, and ABI issues.
- **Operational coupling:** the code assumes internal infrastructure that must be replaced or shimmed for AWS/open-source users.

## What Is Missing Before Rust Can Replace C++

P0, required for a serious distributed alpha:

- replace in-process Raft with OpenRaft or raft-rs
- durable Raft log and snapshot install
- data-node RPC transport and real multi-process cluster
- metaserver topology model with namespaces, tables, shards, slots, and replica sets
- router compatible with the chosen shard/slot scheme
- WAL/oplog before page/index mutation
- crash-safe recovery tests
- metrics HTTP endpoint with Prometheus scrape
- admin APIs for load/unload, membership, shard movement, and health

P1, required for production-like parity:

- full C++ Feature/IPS/Risk semantics
- C++ protobuf/brpc/thrift compatibility or a documented migration API
- production cache backend, likely CacheLib via FFI or a Rust cache with SSD tier support
- page/index compaction and garbage collection
- snapshot integration with Raft and bootstrap
- scale up/down controller backed by metaserver decisions
- load reporting and placement policy
- S3/ByteStore-style shared snapshot/object storage support
- quota/admission and kill switch wiring into real servers

P2, required for launch quality:

- benchmark suite against C++ workloads
- chaos/failure tests
- AWS Terraform that can create or reuse clusters cleanly
- dashboards and alerts
- rolling upgrade story
- backup/restore runbook
- security/auth/TLS
- SDKs and examples

## Recommended Direction

Use the Rust repo as a clean open-source v1 and avoid trying to clone every C++ internal detail at once.

Recommended sequence:

1. Keep the current TemporalStore Rust API and Redis compatibility tests stable.
2. Add a real durable WAL/oplog underneath `TemporalEngine`.
3. Replace the in-process Raft model with OpenRaft or raft-rs.
4. Wire S3 snapshots into Raft install/restore.
5. Add production metaserver topology and shard routing.
6. Port the missing C++ domain semantics module by module, starting with the APIs users actually call.
7. Benchmark only after durability and routing are real, otherwise latency numbers will be misleading.

The Rust rewrite has a strong foundation because it is small, readable, and testable. The gap is not Rust language capability. The gap is the amount of storage-engine and distributed-systems machinery that the C++ code has accumulated over time.
