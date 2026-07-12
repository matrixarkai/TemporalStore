# MatrixObjectStore Service Split

## Source Input

The downloaded object-store reference PDF is image-only: 26 pages and no embedded text layer. The implementation therefore uses the durable object-store architecture signals already extracted in this repo: separate metadata ownership, block placement metadata, byte/chunk serving, checksums, listing, deletion, and future server registration/discovery.

New public code and docs continue to use `MatrixObjectStore`. Historical external names are design ancestry only and should not reappear in public APIs.

## Service Boundary

MatrixObjectStore now has explicit internal services behind the existing `ObjectStore` trait:

- `MatrixObjectStoreRootService`: owns object manifests, object listing, object URI identity, and the root metadata view.
- `MatrixObjectStoreBlockService`: owns block metadata, block id to chunk refs, offsets, lengths, and block checksums.
- `MatrixObjectStoreChunkService`: owns payload bytes and chunk-level atomic publish/read/delete.

The current local-compatible implementation stores small objects as one block/chunk and splits large objects into chunked block refs. `TS_MATRIXOBJECTSTORE_CHUNK_TARGET_BYTES` controls the target chunk size, and `TS_MATRIXOBJECTSTORE_TRANSFER_CONCURRENCY` controls bounded parallel chunk reads, writes, deletes, and overwrite cleanup. Normal reads trust the root manifest block refs and verify chunk checksums without re-fetching every block metadata record. By default, writes skip separate block metadata rows to avoid per-chunk write amplification; set `TS_MATRIXOBJECTSTORE_PUBLISH_BLOCK_METADATA=1` to publish them, or set `TS_MATRIXOBJECTSTORE_VERIFY_BLOCK_METADATA_ON_READ=1` for strict block metadata verification, which automatically publishes block metadata on write. That keeps compatibility with TemporalStore snapshot/shared-store callers while creating the seams needed to split these into separate root, block, and chunk server processes later.

Block metadata ids include an object-key fingerprint plus chunk offset and checksum. That prevents two different objects with identical chunk bytes from overwriting each other's block metadata in the block service, while still allowing chunk-level checksum verification.

Manifest block refs include `block_metadata_published`. New fast-path writes set it to `false` so delete/overwrite cleanup skips unnecessary block metadata deletes; strict or explicitly published writes set it to `true`. Older manifests that do not have the field default to `true` so pre-existing block metadata is still cleaned up.

Local-compatible MatrixObjectStore writes call filesystem `sync_all` and sync parent directory entries by default. Set `TS_MATRIXOBJECTSTORE_SYNC_WRITES=0` / `TS_MATRIXOBJECTSTORE_SYNC_PARENT_DIRS=0`, or use `MatrixObjectStoreConfig::with_sync_writes(false).with_sync_parent_dirs(false)`, only for benchmark, ephemeral, or externally durable deployments where higher layers own recovery. These settings are carried in `MatrixObjectStoreConfig` and threaded into root, block, and chunk services so performance reports can distinguish durable local writes from faster async local file writes.

`MatrixObjectStoreConfig` now carries a `MatrixObjectStoreServiceEndpoints` section. A deployment can use one unified external endpoint for compatibility or three independent endpoints:

```text
root_endpoint  = matrixobjectstore-root://root-service
block_endpoint = matrixobjectstore-block://block-service
chunk_endpoint = matrixobjectstore-chunk://chunk-service
```

`MatrixObjectStore::service_topology()` reports the effective root/block/chunk service roles, local compatibility roots, and configured endpoints. TemporalStore still talks to `ObjectStore`; the split is below that stable API boundary.

Append-only shared-store objects such as WAL/oplog entries and snapshot objects use `ObjectStore::put_unique`, which skips the previous-manifest lookup and stale-ref cleanup that normal overwrite-capable writes need. Snapshot upload writes directly to the UUID-backed stable prefix, uploads data files with bounded concurrency controlled by `TS_SNAPSHOT_UPLOAD_CONCURRENCY`, and publishes `manifest.json` last, so snapshots remain list-invisible until complete while avoiding temp object copy/read amplification. Snapshot listing, download, remote verification, failed-upload cleanup, and snapshot delete use bounded concurrent object reads/deletes controlled by `TS_SNAPSHOT_TRANSFER_CONCURRENCY`, falling back to `TS_SNAPSHOT_UPLOAD_CONCURRENCY` and then `4` when unset.

Snapshot upload uses `ObjectStore::put_path_unique`, and snapshot download uses `ObjectStore::get_to_path`. MatrixObjectStore overrides both hooks to move chunked objects between files and the chunk service with bounded chunk memory, instead of assembling each large page segment as one in-memory byte buffer. Local/shared file backends also override `put_path_unique` with an atomic copy-to-temp plus rename path so file-backed snapshot upload avoids full-payload buffering too.

Snapshot checksum generation and local restore verification stream files in fixed-size buffers. Remote snapshot verification uses `ObjectStore::head`, so MatrixObjectStore can validate size, checksum, and manifest block layout from manifests without downloading chunk payloads.

File-backed `ObjectStore::head` also streams checksum calculation from disk and reads size from filesystem metadata, so metadata-only verification avoids full-payload buffering on local/shared-file stores.

File-backed and MatrixObjectStore `get_to_path` restore into a sibling temp file, verify the downloaded bytes, and rename into the requested destination only after success. Durable mode syncs the temp file before publish and syncs the destination parent after rename; tuned async mode can disable those syncs through `TS_MATRIXOBJECTSTORE_SYNC_WRITES` and `TS_MATRIXOBJECTSTORE_SYNC_PARENT_DIRS`. A failed chunk read, checksum mismatch, or missing source leaves any previous destination file intact and removes the temp file best-effort.

File-backed shared-store listings hide internal upload and restore temp siblings, so crash leftovers from atomic publish paths do not appear as live object keys or slow higher-level object scans.

## Runtime Flow

Write path:

```text
ObjectStore::put_atomic(key, bytes)
-> RootService previous-manifest lookup runs in parallel with new chunk writes
-> object key, object fingerprint, and block-metadata policy are computed once per object
-> in-memory payload chunk descriptors are scheduled incrementally, not pre-materialized
-> ChunkService writes one or more payload chunks atomically, with bounded concurrency
-> BlockService writes block metadata for each chunk
-> RootService writes object manifest
-> stale old chunk/block cleanup runs after overwrite
-> ObjectMetadata is returned to TemporalStore
```

If a chunk write, block metadata write, or final manifest publish fails, MatrixObjectStore now aborts remaining write tasks where possible and deletes already-published new chunk/block refs with bounded best-effort cleanup. That keeps retry storms and partial object-store failures from accumulating orphan shared-store objects.

Read path:

```text
ObjectStore::get(key)
-> RootService reads object manifest
-> manifest block layout is validated for bounds and contiguous coverage before allocation or chunk IO
-> RootService manifest supplies block refs
-> ChunkService reads payload chunks with bounded concurrency
-> chunk length and checksum verification
-> optional strict BlockService metadata verification against the RootService manifest refs
-> chunks are copied into one pre-sized output buffer by manifest offset
-> bytes returned to TemporalStore
```

List/delete path:

```text
list(prefix) -> RootService manifest-prefix/suffix listing -> object keys
delete(key) -> RootService manifest -> bounded parallel ChunkService/BlockService cleanup -> RootService delete
```

Root listing walks only manifest files ending in `.manifest.json`, so list-heavy snapshot/shared-store scans do not materialize unrelated root-service files or payload chunk metadata.

## Future Separate Services

If/when MatrixObjectStore runs as separate processes, the natural split is:

- Root server: namespace, object manifest, ownership, conditional metadata, placement policy, server discovery.
- Block server: block placement/index metadata, block health, block checksums, lifecycle and GC eligibility.
- Chunk server: byte IO, atomic chunk publish, chunk checksum verification, chunk delete/compaction.

TemporalStore should still consume the stable `ObjectStore` API and should not couple to individual root/block/chunk server internals.

The local-compatible service structs are intentionally named as services now, not just helpers, so future RPC clients can replace the local file implementation per service without changing snapshot or shared-store callers.
