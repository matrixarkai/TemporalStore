<!-- SPDX-License-Identifier: Apache-2.0 -->
<!-- Copyright 2026 MatrixArkAI -->

# Checkpoint Captures

A checkpoint must describe one coherent shard state and WAL application boundary. Capturing
that state holds the engine guard, but rebuilding its large tree indexes need not.

The private checkpoint capture copies the WAL placement map, bucket map, object-to-block
lookup, and both dirty-object indexes into owned vectors. After the guard is released, it
rebuilds their original tree types. The remaining fields are copied unchanged. Exhaustive
patterns over both `ShardState` and `CoreIndex` require a copy decision for every new field.
The private intermediate image is not serializable and is returned as a complete shard only
after every deferred index has been restored. Empty lookup groups, dirty-key variants,
bucket flags, routing stamps, placements, object data and generations remain intact.

The normal checkpoint path, released-bucket export, and bulk index flush use this capture.
WAL boundary selection and publication fencing still occur in their original guarded view.
Released buckets are reconstructed in the owned checkpoint, not reloaded into the live shard.
Durability barriers, retirement validation, reclaim floors and persisted formats do not change.

This is not a constant-time capture. It still walks the deferred indexes and clones other
state under the guard. Temporary vectors add a bounded-by-corpus copy, not a persistent cache.
CPU, peak memory and foreground latency must be evaluated together on the complete workload;
passing a checkpoint check alone does not establish a serving improvement.

```bash
cargo test -p temporalstore-rust --lib state::checkpoint_capture::tests:: -- --test-threads=1
```

The focused checks compare the complete reconstructed state with the original clone, verify
writer progress while tree reconstruction is deferred, retain empty/degenerate index shapes,
and exercise checkpoint restoration after real bucket release without undoing live eviction.
Existing dump, restart, WAL retention and publication checks remain required.
