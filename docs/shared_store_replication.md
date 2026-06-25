# Shared Store Replication

The Rust code now has a first shared-store replication path for oplog, index, and page segment replication. It is implemented in `crates/temporalstore-rust/src/shared_store.rs` and uses the existing `ObjectStore` abstraction from `temporalstore-snapshot`, so the same path can be backed by local files in tests and S3-compatible storage later.

## Object Layout

```text
<cluster_id>/shards/<shard_id>/shared/
  index/
    shard.index.json
  page_segments/
    page_segment_<page_segment_id>.seg
  oplog/
    oplog_<oplog_index>.json
  checkpoints/
    <checkpoint_id>/
      index/
        shard.index.json
      page_segments/
        page_segment_<page_segment_id>.seg
      manifest.json
```

The replicated index is the engine's persisted shard index JSON. Page segments are copied as immutable segment files. Oplog entries are ordered JSON records:

```json
{
  "shard_id": 1,
  "oplog_index": 2,
  "command": { "kind": "string_set", "key": "k", "value": [118] }
}
```

## Write/Publish Path

Rust follows the C++ operational default here: shared-store writes use async storage by default.
`SharedStoreStorageMode::default()` is `Async`, and `SharedStoreReplicator::default_storage_writer`
queues oplog entries for background flush. Callers that need request-path durability can explicitly
select `SharedStoreStorageMode::Sync`, which publishes the oplog object before returning.

The primary can publish:

1. `publish_oplog_entry` for each committed mutation command.
2. `publish_index` after the shard index is durable locally.
3. `publish_page_segments` for local page segment files.

For production, this should become stricter:

- publish committed oplog entries only after Raft commit
- checkpoint index/page segments at a consistent log index
- publish a manifest last, or reuse the S3 snapshot manifest path, so followers never install mixed generations
- include checksums and page/index generation ids

## Replica Restore Path

The implemented restore flow is:

```text
new replica -> restore_latest_checkpoint -> load_shard -> replay_oplog(checkpoint_oplog_index) -> serve reads
```

`restore_latest_checkpoint` downloads the latest visible checkpoint manifest, verifies the index and
page segment checksums, installs the index/page files locally, and returns the checkpoint's
`checkpoint_oplog_index`. The engine then loads the restored index from local disk. `replay_oplog`
scans ordered oplog objects after the checkpoint and applies each command to the local engine.

This directly supports the desired path:

```text
shared store index -> local engine index
shared store pages -> local page store
shared store oplog -> command replay -> catch up after checkpoint
```

## Current Guarantees

- Object-store abstraction is shared with the snapshot crate.
- Checkpoint manifest is written after index/page objects, so followers only restore visible checkpoints.
- Checkpoint manifest records the durable oplog sequence covered by the index/page generation.
- Index and page segment byte size plus SHA-256 are verified before install.
- Follower restores page bytes and index bytes from shared store.
- Follower can read restored data by following `PageAddress` into local page files.
- Follower can replay oplog entries after the restored checkpoint.
- Unit tests validate checkpoint restore, later oplog replay, and corrupt page rejection.
- A C++-style compatibility test validates shared-store bootstrap plus catch-up across string, hash,
  and feature data.
- Shared-store storage supports sync publish, async queued publish, bounded flush, and persisted
  replay cursor resume.
- Oplog objects are checksum-enveloped and replay rejects corrupt entries.
- Object-store writes support a bounded retry policy; async flush requeues entries after publish
  failure.
- Shared-store GC can delete oplog objects before a replay-safe index. Cursor-safe oplog GC refuses
  deletion past a known follower cursor, and cursor-safe checkpoint GC keeps both the newest N
  checkpoints and the checkpoint generation needed by the persisted follower replay cursor.

## Local Storage Modes Harness

Run the harness below when changing shared-store storage, replay, or local Raft WAL behavior:

```bash
CARGO_TARGET_DIR=/tmp/temporalstore-rust-target \
cargo run -p temporalstore-rust --bin storage_modes_harness -- \
  --async-flush-limit 1
```

The harness validates three local paths in one run:

- sync shared-store storage publishes oplog entries immediately and a follower can replay them
- async shared-store storage queues entries, flushes them with a bounded limit, then replays them
- Raft writes committed entries to local WAL segment files and restores the shard from those files

For AWS, point shared-store paths at EFS and keep Raft WAL roots on local disk. Example:

```bash
cargo run --release -p temporalstore-rust --bin storage_modes_harness -- \
  --root /tmp/temporalstore-storage-modes \
  --shared-store-root /mnt/temporalstore-shared/rust-storage-modes/shared-store-$(date +%s) \
  --raft-wal-root /tmp/temporalstore-storage-modes/raft-wal \
  --async-flush-limit 1
```

The output is JSON and includes per-write publish/queue status, async flush progress, replay
position, restored read value, and the local WAL segment files used by each Raft replica.

## What Is Still Missing For Production

- integration with real Raft commit index
- lifecycle scheduling around oplog/checkpoint GC tied to Raft snapshot/install state
- S3 multipart upload and range-read optimization for large page segment sets
- concurrency control so followers do not install a partially uploaded generation

Shared-store replication is now present as a working local path, but production should connect it to Raft snapshots or a manifest-based checkpoint before using it for live multi-node failover.
