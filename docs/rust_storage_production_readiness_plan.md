# Rust Storage Production Readiness Plan

Goal: make Rust storage production-ready and scalable enough for the current Rust-native target
while matching the important C++ TemporalStore storage lifecycle behavior. legacy C++ wire, S3, and
ByteStore integration remain out of scope for this plan.

## Current Production Posture Gate

`storage_production_posture_report()` is the single readiness summary for the storage concerns that
must stay production-ready:

- orphan page detection
- missing page-reference detection
- stale page-reference detection
- corrupt page, index-log, oplog, and snapshot evidence
- follower-cursor and Raft-snapshot safe GC
- cache pressure and memory/disk/page-store refill evidence
- shared-store sync and async replay
- unified storage corpus coverage

`storage_cache` readiness depends on this posture report in addition to the storage migration
corpus, local/shared-store dependency matrix, and SSD cache pressure report. If any listed pillar
regresses, the service readiness report must fail closed with the missing evidence field.

## 25-Cycle Backlog

1. Storage production readiness gate across recovery, lifecycle, cache, and page-store health. Done.
2. End-to-end readiness route coverage in the storage-mode harness.
3. Configurable readiness policy thresholds for dirty slots, stale extents, and dump lag. Done.
4. Durable readiness snapshots for post-crash comparison.
5. Background readiness scanner integrated with data-node preflight.
6. Readiness Prometheus gauges and blocker counters.
7. Slot dump manifest retention policy with follower cursor fixtures.
8. Roll-forward recovery harness for interrupted slot dump install phases.
9. Live page owner validation in every compaction apply path.
10. Page segment quarantine on corruption before GC.
11. Extent-level reclaim budget enforcement under sustained writes.
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
- Cache eviction now uses a Rust-native weighted hotness/LRU victim policy instead of only FIFO
  order. Victim selection skips pinned entries, prefers lower hotness, then lower hit count, then
  older access epochs, and reports cold/low-hit/stale eviction reasons separately for memory and
  SSD tiers.
- Cache pressure now scores routing-slot/object groups before individual blocks, matching the
  C++ Evicter shape where sampled slots are selected first and objects/pages are evicted from that
  selected slot. Memory pressure evicts the cold slot group; SSD pressure rejects a colder incoming
  group instead of sacrificing a hotter resident slot.

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
  recovery integrity, Redis/admin reads, and post-restart logical reads.
- The same corpus replays through sync and async local shared-store oplogs and through Raft
  replication with a leader transfer before reads.
- `tools/export_cpp_storage_migration_artifacts.py` publishes deterministic object/page/slot/index/
  oplog artifact JSON plus a manifest for the migration-only C++ corpus path.
- The tracked CI workflow uploads the generated storage migration artifact directory as the golden
  corpus evidence for Rust replay.
- `storage_production_harness` turns the corpus into a local scale/fault gate for dump, cache
  pressure, restart recovery, shared-store replay, and Raft movement.
- The parity gate now runs the storage production harness and validates the JSON summary.
- The parity gate now also runs `storage_fault_matrix_harness`, which rejects checksum-mismatched,
  partial, missing-segment, stale, and corrupt-page-segment slot dump manifests before install.

This closes the in-repo Rust migration verifier, external artifact-export contract, CI-published
golden corpus path, and local production-harness slice for Rust-native storage formats. It does not
claim global storage production readiness; live ByteStore/S3 integration and broader
deployment-scale evidence remain separately tracked blockers.

## Global Readiness Boundary

The local Rust-native storage target has evidence for migration replay, slot dump/load,
cache pressure, shared-store sync/async replay, and the storage production/fault harnesses.
That evidence is sufficient for the Rust-native local/shared-store storage path, but it is not a
global production-readiness claim. The Rust-native cache path now has weighted hotness/LRU
eviction evidence, admission/eviction counters, pin-aware eviction skip accounting, warmup,
invalidation, and tiny-cache pressure coverage. The Rust-native multi-tier replacement policy,
pinned-handle accounting/eviction guards, DRAM/PMEM/SSD placement semantics, async
writeback/backpressure counters, and mature cache latency metrics are now covered by weighted
hotness/LRU memory plus SSD eviction evidence, pin/unpin state, pinned-skip counters,
`CacheTieringPolicy` placement decisions, write-through/backpressure counters, and get/put latency
metrics. PMEM is treated as an SSD-class persistent tier in the Rust-native deployment contract.

Rust now persists a first-class slot/object/page ownership index and uses it as the permanent
authority for object existence and page reads. The internal Rust type shape is now
`CoreIndex -> SlotMap -> SlotNode -> PageIndex/ObjectIndex`, mirroring the C++ `Index -> SlotMap
-> SlotNode -> PageIndex/Object` ownership model while keeping Rust-native serialization.
Legacy model-map-only state is promoted into the slot index on shard load or before command
execution; after promotion, logical model maps are secondary views/accelerators rather than a
competing primary index. Changed objects are synchronized into that slot index incrementally, with
slot dirty generations and PageIndex-like model id, page id, dirty/deleted/log flags, size, and
address metadata. Slot layout transition evidence is also present for writes, rebuilds, compaction,
tombstones, and dump/load validation.

The Rust lifecycle behavior evidence is specifically scoped to: slot-first ownership updates on
object writes and deletes, recovery validation of owner/page refs, slot-scoped dump/load manifest
validation, local/shared-store sync and async replay, follower-cursor retention, model-layout
compaction, tombstone preservation, stale-page-density accounting, and cold BlockAddress reads
through cache refill. These are readiness fields, not informal doc claims.

Rust now also exposes a Rust-native ObjectManager runtime report. It covers hot/cold/mixed
residency, tombstone objects, dirty/loading/meta/TTL object counters, dirty slot generations,
layout classes, layout transition counters, missing owner refs, owner mismatches, and object-id
reuse conflicts. That makes native Rust ObjectManager runtime mechanics readiness-backed rather
than only doc-described.

Rust also has stream-backed extent runtime evidence. The page store appends self-describing stream
records, supports logical range reads that skip envelopes and decompress records across page
boundaries, rolls segments by sealing the previous extent and opening a new active extent, persists the
extent manifest across reopen, and tracks active/sealed/delayed-destroy/purged extent states.

Page compaction is tied to model layout and tombstones through `ShardCompactionReport`.
Compaction now reports `model_layout_compaction_ready`, per-model layout rows, packed timestamped
page preservation, rewritten object/page counts, stale-page density before and after, slot layout
transition counts, and tombstone object preservation. Blockers are emitted if compaction rewrites no
live refs, loses tombstones, lacks model-layout rows, fails to improve or preserve live-ref density,
or lacks slot-layout transition evidence.

## Exact Evidence Fields For Current Storage Gaps

The Rust-native storage/cache gate should treat the following as covered only when the named
evidence is present:

| Gap | Required evidence |
|---|---|
| Merged dump/load recovery | `storage_merged_dump_load_policy_report.policy_ready`, manifest checksum/generation validation, multi-slot merged manifest source coverage, rollback/install markers, load preflight, stale object/page conflict reporting, and install/restart logical reads. |
| StorageManager continuous phase loop | `run_storage_manager_cycle` stages for prepare, WAL reclaim, expire, evict, page GC, compaction, index-GC, and reap metrics with per-phase pressure, selected slots, skipped reasons, bytes reclaimed, and floors. |
| GC/WAL/index-log reclaim rules | WAL/index-log reclaim reports must show slot-dump durability, follower cursor retention, Raft snapshot/install floor awareness, and recovery after reclaim/index-GC. |
| Cache and eviction soak | `storage_cache_replacement_policy_soak` plus cold page-address read tests must show weighted replacement, pinned-skip accounting, disk/page fallback, memory refill, writeback/backpressure, latency samples, and restart refill. |
| Stream/extent manifest rebuild | Page-store recovery must show stream record inspection, extent manifest rebuild from local segments, active/sealed/delayed-destroy state, and post-reopen append/read behavior. |
| Risk/context page-backed parity | Risk and context model tests must verify page-backed storage, secondary view reconciliation from slot/object/page authority, and logical reads after reload/compaction. |

Remaining blockers are deliberately narrower: direct CacheLib/mtcache binary/API compatibility,
live external object-store integration, and broad deployment-scale evidence remain out of scope
unless they are explicitly re-scoped.

Rust now has a StorageManager-style background loop report that covers prepare, reclaim, evict,
expire, compact, and index-GC phases in one readiness surface. The loop builds dirty-slot and
live/stale segment plans, ranks reclaim candidates, invalidates cache entries with byte accounting,
sweeps TTL metadata, runs model-layout compaction, and prunes or rolls forward slot dump manifest
state. Runtime loop reports also include per-stage pressure decisions with observed and threshold
values for dirty slots, undumped oplog records, cache memory and disk bytes, stale segments,
reclaimable physical bytes, expired records, index-GC manifest work, and foreground/background
queue depth, so continuous stages cannot pass readiness with only static phase names.
Block-store GC candidates expose C++ PageGc-style total bytes, used bytes, stale bytes, and utility
basis points in addition to the Rust reclaim score, so low-utility stale extents can be audited
directly from lifecycle plans.

Rust now also has a merged dump/load ownership policy report. `StorageMergedDumpLoadPolicyReport`
coordinates dirty-slot dump selection, slot dump manifest checksum and generation validation, load
preflight, replay-boundary selection, interrupted-install roll-forward, follower-safe manifest
retention, and index-GC as one fail-closed readiness surface. The shared
`storage_merged_dump_load_policy` case verifies restore-engine install and stale-load rejection.

The gate still fails closed on deeper C++ storage runtime mechanics that are not equivalent yet:
C++ byte-for-byte ObjectManager hot-object memory layout, byte-for-byte stream backend layout, the
C++ byte-for-byte cleaner internals, and CacheLib/mtcache-class cache behavior. The slot-first
index, ObjectManager runtime report, stream-backed extent report, StorageManager loop report, merged
dump/load policy report, and layout evidence are real readiness evidence, but they are not treated
as byte-for-byte C++ runtime parity.

The global readiness gate now uses the Docker/AWS deployment-scale SLO report as the broader
release evidence for this storage path. The `scale_testing` area is ready when
`scale_slo_report.storage_deployment_scale_slo_ready` is present with metaserver, proxy, client,
data-node, Raft failover, storage pressure, cache pressure, proxy convergence, workload replay,
p50/p95/p99, throughput, error budget, CPU/memory/disk/network collectors, replica lag, failover
count, and scale-event evidence. Long-running external cloud soaks remain useful release evidence,
but they are no longer conflated with the local/shared-store storage correctness gate.
