# Bucket Rebuild Work

Background compaction rebuilds live bucket entries and reattaches retained tombstones.
Layout reconciliation now runs once per completed bucket in each phase, rather than after
every inserted block. Live buckets finish before deleted-object ids are restored; only
buckets that receive reattached tombstones finish again. This preserves the previous
deleted-only and re-added-member behavior.

Both rebuild paths capture tombstone identities through a temporary per-bucket address
lookup. Duplicate addresses keep the scalar lookup's first row, and capture order is
unchanged. No resident index or persisted fields are added.

The state-equivalence test compares the previous scalar rebuild with the batched path.
At 128 churn rounds, its live fixture drops from 146 layout calls visiting 10,586 block
entries to three calls visiting 163, with identical serialized bucket state. Deleted-only,
re-added-member, repeated-rebuild and scalar tombstone-capture cases are covered too.

This removes repeated prefix work, not every cost in compaction. Relocation and other
rebuild work can still hold the shard write guard. WAL fences, tombstone retention,
compaction cadence, durability and configured timeouts are unchanged; this is not an
end-to-end latency or large-corpus acceptance claim.
