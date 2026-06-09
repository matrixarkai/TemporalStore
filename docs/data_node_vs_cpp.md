# Data Node Vs C++ TemporalStore

## C++ Data Node Shape

The C++ data node is centered on `PartitionManager` and partition storage components:

- `Load`, `Unload`, `ExecuteCmd`, `BatchExecuteCmd`
- `GetInfo`, `GetStats`, `UpdateMembership`
- `ReadPartitionStream`, `ScanPartitionStream`
- partition worker dispatch
- readonly partition behavior
- load-version guarded execution
- heartbeat/load reporting to metaserver
- `ObjectManager` for hot object state
- `OpLogger` for mutation logs
- `Index` / `IndexLog` for page/object metadata
- `PageStore`, `SlotStore`, zones, page headers, page compaction, page GC
- `StorageManager` for async dump/load/reclaim
- `Replicator` for shared-store or primary-pull replay
- `ByteraftReplicator` boundary for future strong data-plane replication

Important C++ files checked:

- `/home/vj/src/temporalstore/src/server/partition_manager.cc`
- `/home/vj/src/temporalstore/src/server/service.h`
- `/home/vj/src/temporalstore/src/protocol/server.proto`
- `/home/vj/src/temporalstore/src/protocol/storage.proto`
- `/home/vj/src/temporalstore/src/partition/storage/op_logger.cc`
- `/home/vj/src/temporalstore/src/partition/storage/object_manager.cc`
- `/home/vj/src/temporalstore/src/partition/storage/storage_manager.cc`
- `/home/vj/src/temporalstore/src/partition/storage/page_store.cc`
- `/home/vj/src/temporalstore/src/partition/storage/replicator.cc`

## Rust Coverage After This Change

Rust now covers these data-node surfaces:

- `DataNodeRuntime` worker pool with bounded queue
- async execute / checked execute job submission
- async job status tracking
- request deadline handling at the queued task boundary
- dirty object id tracking for mutating commands
- dump/compaction/GC task hooks
- `LoadShardRequest` / `UnloadShardRequest`
- execute and batch execute
- config set/get
- info/stats
- membership update
- page stream read/scan
- index stream read/scan
- oplog stream read/scan
- index-log stream read/scan
- readonly shard rejects writes
- checked execute/batch execute validates loaded `load_version`
- server register plus periodic heartbeat/load report to metaserver, including per-partition
  `partition_info` stats
- metaserver load-finish callback endpoint
- local page file persistence
- local index JSON persistence
- local read-through memory/disk cache
- shared-store checkpoint restore and oplog replay
- C++-style replica replay order for local stream sources:
  checkpoint index/pages -> index-log tail -> oplog tail -> persisted replay cursor
- remote HTTP replica stream source for `/read_stream` and `/scan_stream`
- `/replica/replay` server endpoint that lets a secondary data-node pull from a primary stream
  source and persist its replay cursor
- opt-in background replica replay loop in the server process, configured by
  `TS_REPLICA_REPLAY_PRIMARY_ADDR` and `TS_REPLICA_REPLAY_INTERVAL_MS`
- metaserver-discovered background replica replay when no fixed primary address is configured

The new oplog stream is implemented in:

```text
crates/temporalstore-rust/src/oplog.rs
```

Each mutating command appends a JSON-line `OplogRecord`:

```json
{"shard_id":1,"sequence":1,"command":{"kind":"string_set","key":"k","value":[118]}}
```

`StreamKind::Oplog` now reads or scans that file, so a secondary or test can consume mutation records through the same kind of stream API C++ exposes with `ReadPartitionStream` and `ScanPartitionStream`.

Rust also has a JSONL index-log stream in:

```text
crates/temporalstore-rust/src/index_log.rs
```

Each mutating command now follows this local durability order:

```text
append oplog -> write page bytes -> append index-log record -> persist whole index
```

The index-log record contains the current shard index metadata after the mutation. It is not yet C++'s compact binary `IndexLog`, but it gives replica/recovery tests a separate stream of committed page-address metadata instead of only a latest whole-index file.

This pass also adds C++-style server-side load-version validation:

```text
/execute_checked
/batch_execute_checked
```

These routes reject requests whose `load_version` does not match the currently loaded shard version. The original `/execute` and `/batch_execute` routes remain for compatibility with the current proxy/client path.

The Rust `server` binary now registers itself with the metaserver and periodically sends:

```text
server_addr, binary_version, shard_id, key_count, cache memory bytes
```

The heartbeat now includes both compact `ShardLoad` placement signals and a richer
`PartitionLoad` payload with the same local `partition_info` shape exposed by `GetStats`.
That carries table name, shard URI, load version, readonly state, routing-slot range, storage bytes,
logical object count, page refs, dirty object count, and dirty slot count to the metaserver.

This pass also adds an explicit runtime layer:

```text
DataNodeRuntime -> bounded queue -> worker threads -> TemporalEngine
```

The runtime supports:

- `/async_execute`
- `/async_execute_checked`
- `/jobs/<job_id>`
- `/server/runtime_stats`
- `/server/dirty_objects`
- `/dump`
- `/compact`
- `/gc`

Dump clears dirty objects for the shard. GC now clears the shard's memory/disk cache and accepts
explicit retention boundaries for the local oplog, index log, and page segments; it rewrites JSONL
log tails and deletes old non-current page segment files below the requested generation. Compaction
is still a control-plane hook and does not yet rewrite live page data into compacted zones.

Stats now expose a C++-style partition/object-manager summary:

- logical object count across strings, hashes, sets, feature series, sequence series, IPS series, and risk series
- page-address reference count separate from logical object count
- transient dirty object and dirty routing-slot counts
- table name, shard URI, load version, readonly state, routing-slot range, and storage bytes in `partition_info`
- Prometheus gauges for object count, page refs, dirty objects, dirty slots, and routing-slot span

This pass adds a reusable replica replay loop in
`crates/temporalstore-rust/src/replica_replay.rs`. It consumes the existing stream APIs in the same
order the C++ secondary catch-up path expects:

```text
install index checkpoint -> copy page segments -> load shard -> replay index-log tail -> replay oplog tail -> save cursor
```

The cursor records checkpoint install state, page segment ids, index-log byte offset/sequence, and
oplog byte offset/sequence. A resumed replay starts after the persisted offsets and rejects sequence
gaps before serving reads. The same replay loop can now pull from an in-process engine or from a
remote HTTP stream source, matching the C++ shape where a secondary consumes partition streams from
another server.

The server binary exposes this as:

```text
POST /replica/replay
{
  "shard_id": 1,
  "primary_addr": "127.0.0.1:17002",
  "cursor_path": "... optional ...",
  "max_stream_bytes": 16777216
}
```

By default, cursors are stored under `TS_REPLICA_REPLAY_CURSOR_DIR` or
`<TS_INDEX_DIR>/replica-replay-cursors`.

The same path can run continuously in a secondary process:

```text
TS_REPLICA_REPLAY_INTERVAL_MS=1000
TS_REPLICA_REPLAY_MAX_STREAM_BYTES=16777216
```

If `TS_REPLICA_REPLAY_PRIMARY_ADDR` is set, the loop uses that fixed stream source. If it is empty,
the loop resolves the current primary through `TS_META_ADDR` using `GET /shards/<shard_id>` and
skips replay when the route points back to the local advertised server.

## What Is Still Missing Vs C++

Still missing major data-node internals:

- full async callback API parity with brpc closures
- in-flight cancellation after a worker has started executing
- batch write/read splitting and pin-primary details at server layer
- binary protobuf-compatible `OpLog`, `IndexLog`, and `PageHeader`
- full C++ `ObjectManager` memory layout with stable object ids and page ids
- slot context manager with dump/rewrite ownership, not only routing-slot accounting
- dirty slot scheduling tied to background dump tasks
- background periodic dump scheduler
- merged page dump to zones
- topology-change-triggered replay rescheduling and primary-change retry policy
- production readonly-replica role scheduling, replay backoff/error reporting, and topology-change
  rescheduling around the metaserver route
- page compaction
- C++-style page rewrite garbage collection with page headers and zone ownership
- expirer/evicter background tasks
- primary-pull `RemotePartitionStream` binary/protobuf parity beyond the current HTTP stream source
- retry/backoff policy when the metaserver primary route changes
- membership finish callbacks to metaserver
- full heartbeat/load reporting payload parity beyond local `partition_info` stats
- storage quotas and admission control beyond local shard `maxmemory_bytes`
- real readonly replicator loop
- byteraft data FSM integration

## Recommended Next Data-Node Chunk

The next best implementation chunk is a replica replay loop that consumes checkpoint, page, index-log, and oplog streams:

```text
latest checkpoint -> page stream -> index-log replay -> oplog replay after checkpoint
```

That would make Rust closer to C++ secondary catch-up order:

```text
oplog replay rebuilds hot objects
index-log replay rebuilds page/object address metadata
page stream supplies durable dumped bytes
```

It should track persisted offsets and reject replay gaps before serving reads.
