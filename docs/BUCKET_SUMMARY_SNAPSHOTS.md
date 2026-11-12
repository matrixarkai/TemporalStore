<!-- SPDX-License-Identifier: Apache-2.0 -->
<!-- Copyright 2026 MatrixArkAI -->

# Bucket Summary Snapshots

Maintenance compares live bucket contents with durable dump contents before reclaiming logs.
Fingerprint hashing and sorting can be substantial for a large bucket. Holding the shard
read lock through that work queues foreground writes and can delay unrelated reads behind them.

The engine captures summaries, owned live-block entries, routing information, and the dump
manifest's sequence annotations under one shard read guard. It then releases that guard before
hashing, sorting and deduplicating. The finishing phase has no engine handle or lock guard,
and the capture does not keep a full dump index image alive. Entries are released immediately
after digest computation.

This preserves the same fingerprint fields, sort order, duplicate removal, summary values,
one-listing behavior, and captured manifest annotations. It adds no persistent cache or record format.
Concurrent writes do not change the owned snapshot, and a later generation cannot pass a
comparison against that snapshot merely because hashing completed later. Existing reclaim
validation and log floors remain in force.

```bash
cargo test -p temporalstore-rust --lib bucket_summary_snapshot_ -- --test-threads=1
```

The tests cover exact output equivalence, writer progress while finishing is deferred,
manifest selection across another dump, and rejection of a later generation. A passing
component test is not an end-to-end latency result. Use the TemporalStore Mem0 scale runner
to evaluate actual ingestion and CRUD behavior with maintenance active.
