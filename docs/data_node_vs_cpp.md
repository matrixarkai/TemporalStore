# Data Node Vs C++ TemporalStore

## C++ Data Node Shape

The C++ data node is centered on `PartitionManager` and partition storage components:

- `Load`, `Unload`, `ExecuteCmd`, `BatchExecuteCmd`
- `GetInfo`, `GetStats`, `UpdateMembership`
- `ReadPartitionStream`, `ScanPartitionStream`
- partition worker dispatch
- readonly partition behavior
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

- `LoadShardRequest` / `UnloadShardRequest`
- execute and batch execute
- config set/get
- info/stats
- membership update
- page stream read/scan
- index stream read/scan
- oplog stream read/scan
- readonly shard rejects writes
- local page file persistence
- local index JSON persistence
- local read-through memory/disk cache
- shared-store checkpoint restore and oplog replay

The new oplog stream is implemented in:

```text
crates/temporalstore-single-node/src/oplog.rs
```

Each mutating command appends a JSON-line `OplogRecord`:

```json
{"shard_id":1,"sequence":1,"command":{"kind":"string_set","key":"k","value":[118]}}
```

`StreamKind::Oplog` now reads or scans that file, so a secondary or test can consume mutation records through the same kind of stream API C++ exposes with `ReadPartitionStream` and `ScanPartitionStream`.

## What Is Still Missing Vs C++

Still missing major data-node internals:

- partition worker pools and async callback execution
- request controller/load-version enforcement
- batch write/read splitting and pin-primary details at server layer
- binary protobuf-compatible `OpLog`, `IndexLog`, and `PageHeader`
- object manager with object ids/page ids
- slot context manager
- dirty slot tracking
- async dump scheduler
- merged page dump to zones
- index-log replay separate from oplog replay
- page compaction
- page garbage collection
- expirer/evicter background tasks
- primary-pull `RemotePartitionStream`
- retry/refresh logic when primary changes
- membership finish callbacks to metaserver
- heartbeat/load reporting
- storage quotas and admission control
- per-partition metrics equivalent to C++ `PartitionInfo`
- real readonly replicator loop
- byteraft data FSM integration

## Recommended Next Data-Node Chunk

The next best implementation chunk is an `IndexLog` stream alongside the oplog:

```text
write command -> append oplog -> write page bytes -> append index-log record -> persist index
```

That would make Rust closer to C++ replay order:

```text
oplog replay rebuilds hot objects
index-log replay rebuilds page/object address metadata
page stream supplies durable dumped bytes
```

After that, the next large piece is a replica catch-up loop that reads checkpoint, oplog, index-log, and page streams and tracks persisted offsets.
