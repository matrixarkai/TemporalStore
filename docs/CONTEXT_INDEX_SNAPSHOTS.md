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

Snapshot collection, bucket identity projection, and base-index encoding build a temporary
address lookup for each bucket's deleted identities. Previously, each deleted block scanned
the tombstone vector again. The lookup preserves the first matching row for duplicate block
addresses, costs linear temporary space in retained tombstones, and is dropped after the
bucket walk. It adds no resident index or persisted fields.

Mixed batches may keep incremental semantics when a set removal vacates its old physical
block. The existing upsert record carries a deleted-address item for that old block plus the
other commands' live upserts, under the same batch WAL anchor and one index-log append.
After all commands run, both resident set membership and the bucket index must confirm that
the old block is no longer live. Shared or revived blocks, expiry/membership shrink, and
unsupported mutations retain the snapshot path. The decoder and record format are unchanged.
Hash rewrites also carry retirement items for old blocks only after their remaining fields
and physical index confirm vacancy. Shared packed blocks remain until their last field moves,
so recovered live-block sets match the serving engine rather than retaining orphan entries.

The built snapshot is bounded by live rows plus deleted coverage groups, instead of by all
past deleted versions. The underlying walk still visits retained physical entries, and live
history can still grow. Further work is required for a fully incremental batch index path;
this change alone is not large-corpus performance acceptance.
