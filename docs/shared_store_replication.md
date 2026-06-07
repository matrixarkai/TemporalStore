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
new replica -> restore_index_and_pages -> load_shard -> replay_oplog(after_index) -> serve reads
```

`restore_index_and_pages` downloads `shard.index.json` and every page segment object for the shard. The engine then loads the restored index from local disk. `replay_oplog` scans ordered oplog objects after the caller's checkpoint and applies each command to the local engine.

This directly supports the desired path:

```text
shared store index -> local engine index
shared store pages -> local page store
shared store oplog -> command replay -> catch up after checkpoint
```

## Current Guarantees

- Object-store abstraction is shared with the snapshot crate.
- Follower restores page bytes and index bytes from shared store.
- Follower can read restored data by following `PageAddress` into local page files.
- Follower can replay oplog entries after the restored checkpoint.
- A unit test validates index/page restore plus later oplog replay.

## What Is Still Missing For Production

- atomic manifest-last checkpoint for shared-store index/page generations
- checksums for index/page/oplog objects
- mapping between checkpoint log index and index/page generation
- integration with real Raft commit index
- idempotent oplog replay with persisted last-applied oplog index
- compaction/garbage collection of old oplog and old page/index generations
- S3 multipart upload and range-read optimization for large page segment sets
- concurrency control so followers do not install a partially uploaded generation

Shared-store replication is now present as a working local path, but production should connect it to Raft snapshots or a manifest-based checkpoint before using it for live multi-node failover.
