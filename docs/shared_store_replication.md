# Shared Store Replication

The Rust code now has a first shared-store replication path for oplog, index, and page segment replication. It is implemented in `crates/temporalstore-single-node/src/shared_store.rs` and uses the existing `ObjectStore` abstraction from `temporalstore-snapshot`, so the same path can be backed by local files in tests and S3-compatible storage later.

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
- Shared-store GC can delete oplog objects before a replay-safe index and checkpoint GC keeps the
  newest N checkpoints while deleting old checkpoint index/page payloads by prefix.

## What Is Still Missing For Production

- integration with real Raft commit index
- lifecycle scheduling around oplog/checkpoint GC, including safety checks against follower replay
  cursors and Raft snapshot/install state
- S3 multipart upload and range-read optimization for large page segment sets
- concurrency control so followers do not install a partially uploaded generation

Shared-store replication is now present as a working local path, but production should connect it to Raft snapshots or a manifest-based checkpoint before using it for live multi-node failover.
