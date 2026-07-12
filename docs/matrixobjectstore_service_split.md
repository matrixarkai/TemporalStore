# MatrixObjectStore Service Split

## Source Input

The downloaded object-store reference PDF is image-only: 26 pages and no embedded text layer. The implementation therefore uses the durable object-store architecture signals already extracted in this repo: separate metadata ownership, block placement metadata, byte/chunk serving, checksums, listing, deletion, and future server registration/discovery.

New public code and docs continue to use `MatrixObjectStore`. Historical external names are design ancestry only and should not reappear in public APIs.

## Service Boundary

MatrixObjectStore now has explicit internal services behind the existing `ObjectStore` trait:

- `MatrixObjectStoreRootService`: owns object manifests, object listing, object URI identity, and the root metadata view.
- `MatrixObjectStoreBlockService`: owns block metadata, block id to chunk refs, offsets, lengths, and block checksums.
- `MatrixObjectStoreChunkService`: owns payload bytes and chunk-level atomic publish/read/delete.

The current local-compatible implementation stores small objects as one block/chunk and splits large objects into chunked block refs. `TS_MATRIXOBJECTSTORE_CHUNK_TARGET_BYTES` controls the target chunk size, and `TS_MATRIXOBJECTSTORE_TRANSFER_CONCURRENCY` controls bounded parallel chunk reads/writes. That keeps compatibility with TemporalStore snapshot/shared-store callers while creating the seams needed to split these into separate root, block, and chunk server processes later.

## Runtime Flow

Write path:

```text
ObjectStore::put_atomic(key, bytes)
-> ChunkService writes one or more payload chunks atomically, with bounded concurrency
-> BlockService writes block metadata for each chunk
-> RootService writes object manifest
-> ObjectMetadata is returned to TemporalStore
```

Read path:

```text
ObjectStore::get(key)
-> RootService reads object manifest
-> BlockService resolves block refs
-> ChunkService reads payload chunks with bounded concurrency
-> checksum verification
-> bytes returned to TemporalStore
```

List/delete path:

```text
list(prefix) -> RootService manifest-prefix listing -> object keys
delete(key) -> RootService manifest -> ChunkService delete -> BlockService delete -> RootService delete
```

## Future Separate Services

If/when MatrixObjectStore runs as separate processes, the natural split is:

- Root server: namespace, object manifest, ownership, conditional metadata, placement policy, server discovery.
- Block server: block placement/index metadata, block health, block checksums, lifecycle and GC eligibility.
- Chunk server: byte IO, atomic chunk publish, chunk checksum verification, chunk delete/compaction.

TemporalStore should still consume the stable `ObjectStore` API and should not couple to individual root/block/chunk server internals.
