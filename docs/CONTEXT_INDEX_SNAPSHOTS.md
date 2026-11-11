# Context Index Snapshot Rows

Memory updates change catalog and history fields and move a recency-index member. A removal
can select the snapshot form of an index-log record, which previously copied every retained
deleted block row as well as every live row on each update.

Snapshot production now retains at most one deleted coverage row per object key, routing
bucket, and model. All live rows and the existing object/bucket/model coverage remain.
The existing snapshot reader clears covered old state and ignores deleted addresses while
installing live blocks, so repeated deleted rows do not change its result.

This changes neither the reader nor the record format. Physical tombstone blocks, WAL
outcomes, deletion state, checksums, synchronization, and retained history are unchanged.
It is not tombstone garbage collection and does not remove deletion evidence from storage.

Tests compare uncoalesced and coalesced snapshots through the existing decoder and fold,
including a snapshot containing only deletions. They also cover packed hash values,
retained history, and restart recovery.

Key-scoped identity projections build only their position lookup. They do not allocate the
handle-only fallback used by full-shard validation to find misplaced blocks. Full-shard
validation retains both indexes, and keyed live/deleted identity results remain unchanged.

The built snapshot is bounded by live rows plus deleted coverage groups, instead of by all
past deleted versions. The underlying walk still visits retained physical entries, and live
history can still grow. Further work is required for a fully incremental batch index path;
this change alone is not large-corpus performance acceptance.
