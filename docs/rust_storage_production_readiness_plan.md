# Rust Storage Production Readiness Plan

Goal: make Rust storage production-ready and scalable enough for the current Rust-native target
while matching the important C++ TemporalStore storage lifecycle behavior. brpc, Thrift, S3, and
ByteStore integration remain out of scope for this plan.

## 25-Cycle Backlog

1. Storage production readiness gate across recovery, lifecycle, cache, and page-store health. Done.
2. End-to-end readiness route coverage in the storage-mode harness.
3. Configurable readiness policy thresholds for dirty slots, stale zones, and dump lag. Done.
4. Durable readiness snapshots for post-crash comparison.
5. Background readiness scanner integrated with data-node preflight.
6. Readiness Prometheus gauges and blocker counters.
7. Slot dump manifest retention policy with follower cursor fixtures.
8. Roll-forward recovery harness for interrupted slot dump install phases.
9. Live page owner validation in every compaction apply path.
10. Page segment quarantine on corruption before GC.
11. Zone-level reclaim budget enforcement under sustained writes.
12. Disk cache admission policy under memory pressure. Done.
13. Cache warmup budget and backpressure controls.
14. Shared-store checkpoint retention under multiple follower cursors.
15. Oplog/index-log replay boundary fuzz tests.
16. Crash loop test: write, dump, compact, GC, restart, replay.
17. Tiny-memory soak test for memory miss -> disk cache -> page store -> cache refill. Done.
18. Multi-shard storage lifecycle scheduler fairness.
19. Per-slot dirty generation persistence across restart.
20. Manifest chain fork detection in scale harness.
21. Local object-store replay compare against primary page refs.
22. Page envelope compatibility migration policy doc.
23. Operator repair commands for corrupt/orphan/stale storage states.
24. C++ storage gap audit refresh against current first-party storage modules.
25. Final production readiness gate requiring all storage blockers clear in scale validation.

## Cycle 1 Implemented

Cycle 1 added `StorageProductionReadinessReport`, combining:

- recovery boundary safety
- corrupt/unreadable live page detection
- owner/object lifecycle mismatch detection
- interrupted slot-dump install detection
- dirty slot and stale/orphan segment warnings
- cache and page-store byte stats
- direct and C++-style server admin routes

Production readiness is intentionally strict for corruption and ownership blockers, but dirty slots
and stale/orphan segments remain warnings because lifecycle GC/dump can resolve them online.

## Cycle 2 Implemented

Cycle 2 adds policy-driven storage readiness gates for production scale validation:

- `StorageProductionReadinessPolicy` can promote dirty slots, stale segments, orphan segments,
  dump lag, missing slot-dump manifests, and warnings into hard blockers.
- The default report remains backward compatible: online dirty/stale/orphan work stays a warning
  unless a stricter policy is supplied.
- `StorageProductionReadinessReport` now echoes the applied policy and reports undumped oplog
  records so operators can see the exact dump-lag boundary chosen.
- REST `POST /server/storage/readiness` and C++-style
  `ServerService/GetStorageReadiness` accept policy-aware requests while preserving the existing
  shard-only request shape.
- Regression coverage validates five strict-readiness checks: dirty-slot threshold, undumped oplog
  threshold, required manifest, warning promotion, and default compatibility.

The next cycle should add an end-to-end storage-mode harness pass that posts a strict readiness
policy after dump, compact, GC, restart, and replay.

## Cycle 3 Implemented

Cycle 3 adds cache policy depth and inspection for storage lifecycle parity:

- Memory-cache admission now records accepted/rejected decisions and oversize rejection counters.
- Cache fill paths record disk fills, memory fills, capacity evictions, oversize evictions, and
  refill failures.
- Page cache keys now carry routing-slot metadata, allowing slot-scoped cache inspection and
  invalidation after dump/compaction workflows.
- `StorageCacheInspectionReport` exposes shard cache entries and per-slot memory/disk byte
  summaries.
- REST and C++-style admin routes expose cache inspection and slot invalidation:
  `/server/storage/cache/{shard_id}`, `/server/storage/cache/invalidate_slot`,
  `ServerService/GetStorageCache`, and `ServerService/InvalidateStorageCacheSlot`.
- Prometheus cache operation metrics now include admission, fill, eviction-reason, and refill
  failure counters.
- Regression coverage validates oversized-block admission rejection, capacity eviction reasons,
  slot-aware inspection, and slot invalidation.

The next cycle should run the storage-mode harness with tiny memory/disk cache settings through
dump, load, restart, and replay.

## Cycle 4 Implemented

Cycle 4 adds the combined tiny-cache storage flow requested for production-readiness regression:

- The test writes under a tiny memory cache, forces memory eviction through cache churn, and proves
  the target page block remains available from disk cache.
- A slot dump manifest is created, validated, and installed into a restarted engine using the same
  page and disk-cache directories.
- The restarted engine serves the restored key from the persistent disk block cache without
  rereading the page store, then promotes the block back into memory.
- Slot-aware cache inspection verifies the restored page cache entry, and slot invalidation removes
  the hot block after the dump/load restart flow.
- Storage readiness is checked after restart to ensure no corrupt or unreadable page refs are
  hidden by the cache path.

The next cycle should wire this scenario into the standalone storage-mode harness so the same
dump/load/restart/cache-churn path runs outside unit-test process state.

## Cycle 5 Implemented

Cycle 5 adds the migration-only C++ storage corpus and local production harness required for the
current Rust-native deployment target:

- `compat/storage_migration_corpus.json` defines C++-exported logical storage artifacts for common,
  string, hash, set, Feature, Sequence, IPS, Risk, Redis-compatible, and Context model coverage.
- Rust consumes that corpus into native page envelopes, then validates slot ownership summaries,
  dirty generations, dump manifest checksums, follower-cursor retention planning, cache warmup,
  recovery integrity, and post-restart logical reads.
- The same corpus replays through sync and async local shared-store oplogs and through Raft
  replication with a leader transfer before reads.
- `storage_production_harness` turns the corpus into a local scale/fault gate for dump, cache
  pressure, restart recovery, shared-store replay, and Raft movement.
- The parity gate now runs the storage production harness and validates the JSON summary.
- The parity gate now also runs `storage_fault_matrix_harness`, which rejects checksum-mismatched,
  partial, missing-segment, stale, and corrupt-page-segment slot dump manifests before install.

This closes the in-repo Rust migration verifier and local production-harness slice. Storage remains
not production-ready until an external C++ binary-artifact exporter publishes the golden corpus in
CI, the restart-during-install fault matrix is complete, long-running SSD cache pressure validation
passes, and ByteStore/S3 live integration is implemented or explicitly removed from readiness.
