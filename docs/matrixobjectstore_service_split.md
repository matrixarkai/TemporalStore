# MatrixObjectStore Service Split

## Source Input

The downloaded object-store reference PDF is image-only: 26 pages and no embedded text layer. The implementation therefore uses the durable object-store architecture signals already extracted in this repo: separate metadata ownership, block placement metadata, byte/chunk serving, checksums, listing, deletion, and future server registration/discovery.

New public code and docs continue to use `MatrixObjectStore`. Historical external names are design ancestry only and should not reappear in public APIs.

## Service Boundary

MatrixObjectStore now has explicit internal services behind the existing `ObjectStore` trait:

- `MatrixObjectStoreRootService`: owns object manifests, object listing, object URI identity, and the root metadata view.
- `MatrixObjectStoreBlockService`: owns block metadata, block id to chunk refs, offsets, lengths, and block checksums.
- `MatrixObjectStoreChunkService`: owns payload bytes and chunk-level atomic publish/read/delete.

The current local-compatible implementation stores small objects as one block/chunk and splits large objects into chunked block refs. `TS_MATRIXOBJECTSTORE_CHUNK_TARGET_BYTES` controls the target chunk size, and `TS_MATRIXOBJECTSTORE_TRANSFER_CONCURRENCY` controls bounded parallel chunk reads, writes, deletes, and overwrite cleanup. Normal reads trust the root manifest block refs and verify chunk checksums without re-fetching every block metadata record; set `TS_MATRIXOBJECTSTORE_VERIFY_BLOCK_METADATA_ON_READ=1` for strict block metadata verification. That keeps compatibility with TemporalStore snapshot/shared-store callers while creating the seams needed to split these into separate root, block, and chunk server processes later.

Append-only shared-store objects such as WAL/oplog entries and snapshot objects use `ObjectStore::put_unique`, which skips the previous-manifest lookup and stale-ref cleanup that normal overwrite-capable writes need. Snapshot upload writes directly to the UUID-backed stable prefix, uploads data files with bounded concurrency controlled by `TS_SNAPSHOT_UPLOAD_CONCURRENCY`, and publishes `manifest.json` last, so snapshots remain list-invisible until complete while avoiding temp object copy/read amplification.

## Runtime Flow

Write path:

```text
ObjectStore::put_atomic(key, bytes)
-> RootService previous-manifest lookup runs in parallel with new chunk writes
-> ChunkService writes one or more payload chunks atomically, with bounded concurrency
-> BlockService writes block metadata for each chunk
-> RootService writes object manifest
-> stale old chunk/block cleanup runs after overwrite
-> ObjectMetadata is returned to TemporalStore
```

Read path:

```text
ObjectStore::get(key)
-> RootService reads object manifest
-> RootService manifest supplies block refs
-> ChunkService reads payload chunks with bounded concurrency
-> chunk checksum verification
-> optional strict BlockService metadata verification
-> bytes returned to TemporalStore
```

List/delete path:

```text
list(prefix) -> RootService manifest-prefix listing -> object keys
delete(key) -> RootService manifest -> bounded parallel ChunkService/BlockService cleanup -> RootService delete
```

## Future Separate Services

If/when MatrixObjectStore runs as separate processes, the natural split is:

- Root server: namespace, object manifest, ownership, conditional metadata, placement policy, server discovery.
- Block server: block placement/index metadata, block health, block checksums, lifecycle and GC eligibility.
- Chunk server: byte IO, atomic chunk publish, chunk checksum verification, chunk delete/compaction.

TemporalStore should still consume the stable `ObjectStore` API and should not couple to individual root/block/chunk server internals.
