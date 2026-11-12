# Mem0 Mixed Batch Writes

Metadata updates combine hash writes with ordering-set removals and additions.
Previously one non-upsert command made the entire batch collect full block
snapshots for every touched key, including the tenant catalog and history hashes.

Mixed batches now keep snapshot semantics for keys that require them and emit
sparse rows for independently updated hashes. A hash touched by removal or an
unsupported command remains on the snapshot path. Shared-row input and pure hash
batches retain their existing paths without predecessor collection.

Before a hash rewrite, the writer captures its existing block identities and
their actual routing buckets. Only blocks that no remaining field owns are
retired. A partial packed-block rewrite keeps the shared predecessor live.
Missing ownership information keeps the whole key on the snapshot path.

Both outputs use existing index-log records. The snapshot prefix has no applied
WAL boundary; the sparse suffix covers the completed WAL batch. Dirty-bucket
claims retain the first record, not just the suffix. A missing suffix therefore
does not suppress WAL replay. No timeout, retry, acknowledgement or WAL durability
setting changes.

The anchor-only fast sweep conservatively retains the prefix. Consumer-aware
index-sequence GC reclaims it after a durable dump covers the batch. The default
maintenance configuration enables that GC; disabling it also disables this
reclamation. The GC test exercises the position-based store entry point after
obtaining a real durable-dump boundary.

```bash
cargo test -p temporalstore-rust --lib mixed_hash_batch_delta -- --test-threads=1
```

The tests cover sparse row counts at two catalog sizes, folded-index readback,
default WAL recovery, partial shared-block rewrites, same-key hash removal,
prefix boundaries, position-based GC and recovery after physically removing the suffix. These are
correctness and work-shape checks, not all-API latency or semantic retrieval
acceptance. Measure API errors, CPU, sampled RSS and storage bytes using an
exact-revision runtime bundle before claiming a serving improvement.
