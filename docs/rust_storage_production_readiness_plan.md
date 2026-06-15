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
12. Disk cache admission policy under memory pressure.
13. Cache warmup budget and backpressure controls.
14. Shared-store checkpoint retention under multiple follower cursors.
15. Oplog/index-log replay boundary fuzz tests.
16. Crash loop test: write, dump, compact, GC, restart, replay.
17. Tiny-memory soak test for memory miss -> disk cache -> page store -> cache refill.
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
