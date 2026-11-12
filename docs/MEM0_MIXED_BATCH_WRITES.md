# Mem0 Mixed Batch Writes

Metadata updates combine hash writes with ordering-set removals and additions.
Previously one non-upsert command made the entire batch collect full block
snapshots for every touched key, including the tenant catalog and history hashes.

Eligible batches now emit one existing-format upsert delta. It carries explicit
retired-block rows and the live rows for the elements written by the batch. The
complete WAL boundary belongs to this single frame; a torn or missing frame does
not describe a partial batch. A newer base can filter it normally, and the
anchor-based GC path can reclaim it after a durable dump.

The optimization accepts only `HashSet`, `HashMultiSet`, `SetAdd` and `SetRemove`
batches containing a removal, with no deadlines on touched keys. Other command
families retain their existing paths. Unknown block ownership also keeps the
original path. Pure hash, pure mixed append and shared-row input paths do not
collect predecessors for this optimization.

Predecessor rows preserve actual routing buckets and complete physical block
identities. Only frames without surviving owners are retired. Partial packed
hash rewrites keep shared frames live. A set removal that still shares its old
frame uses the original whole-snapshot path for the entire batch.

```bash
cargo test -p temporalstore-rust --lib mixed_hash_batch_delta -- --test-threads=1
```

Tests cover bounded item counts, folded readback, default WAL recovery, packed
hash sharing, live set-frame protection, unsupported counter and hash-removal
fallbacks, deadline exclusion, newer-base recovery, lost-frame recovery and GC.
No timeout, retry or WAL acknowledgement setting changes. These checks do not
establish all-API latency, model quality or large distributed acceptance.

The earlier two-record experiment is superseded. Its anchorless snapshot could
overwrite newer base state during recovery. Runtime measurements of that
experiment do not establish the performance of this atomic implementation.
