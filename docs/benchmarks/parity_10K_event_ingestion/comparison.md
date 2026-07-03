# MatrixArk C++ vs Rust Scale Report

- run_id: `1783074105269`
- generated_at_ms: `1783074105269`
- events: `10000`
- messages_per_ingest: `20`
- ingest_workers: `4`
- retrieve_workers: `16`
- retrieve_queries: `128`

## Results

| backend | status | message QPS | ingest p50 | ingest p95 | ingest p99 | retrieve QPS | retrieve p50 | retrieve p95 | retrieve p99 | errors | partial packs |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| cpp | backend_startup_failed | 0 | 0 ms | 0 ms | 0 ms | 0 | 0 ms | 0 ms | 0 ms | 0 | 0 |
| rust | topology_not_ready | 0 | 0.0 ms | 0.0 ms | 0.0 ms | 0.0 | 0.0 ms | 0.0 ms | 0.0 ms | 0 | 0 |

## Effective Storage Tuning

| backend | context page target | block segment target | storage zone | stream max blob | compaction watermark | cold scan no-cache | page index cache | block index cache | effective block segment |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| cpp | 65536 | 1073741824 | 10485760 | 10485760 | 268435456 | True | 67108864 | 67108864 | 10485760 |
| rust | 65536 | 1073741824 | 10485760 | 10485760 | 268435456 | True | 67108864 | 67108864 | 10485760 |

## Required Page/Block Metrics

Both C++ and Rust backends must expose these metric names before storage performance parity claims are accepted:

- `page_index_lookup_count`
- `page_index_lookup_ms`
- `page_index_cache_hit_rate`
- `block_index_lookup_count`
- `block_index_lookup_ms`
- `block_index_cache_hit_rate`
- `page_reads`
- `page_writes`
- `block_reads`
- `block_writes`
- `bytes_read`
- `bytes_written`
- `append_queue_wait_ms`
- `append_engine_ms`
- `append_queue_depth`
- `append_batch_size`
- `append_batch_bytes`
- `append_coalesced_writes`
- `append_durability_failures`
- `compaction_reclaimed_bytes`
- `cold_scan_no_cache_reads`
- `cold_scan_page_reads`
- `hot_cache_promotions`
- `append_watermark`
- `compaction_watermark`

## Required Storage Read Sequence

Normal reads must report this canonical sequence before storage read-path parity claims are accepted:

1. `logical_key_timestamp_range`
2. `object_page_index_lookup`
3. `page_address_list`
4. `block_index_lookup`
5. `page_read`
6. `decode_records`
7. `return_filtered_result`

## Required Storage Cold Scan Sequence

Cold scans must report this canonical no-promote sequence before cold lifecycle parity claims are accepted:

1. `timestamp_page_index_scan`
2. `no_cache_page_read`
3. `bounded_decode`
4. `no_hot_cache_promotion`

## Required Storage Lifecycle Phases

StorageManager/StoreManager reports must cover these phases before stream/zone/eviction/GC/reclaim parity claims are accepted:

- `prepare`
- `reclaim`
- `evict`
- `expire`
- `page_gc`
- `block_gc`
- `compaction`
- `index_gc`
- `delayed_destroy`
- `follower_cursor_safety`
- `watermark_progress`

## Required Storage Reclaim Semantics

Physical reclaim parity requires these semantics; cache eviction alone is memory-only:

- `cache_eviction_memory_only`
- `logical_tombstone_required`
- `stale_pages_blocks_rewritten_or_skipped`
- `reclaimed_bytes_reported`
- `physical_reclaim_errors_zero`

## Required Multi-Layer Cache Contract

Both engines must report these cache layers and semantics before cache parity claims are accepted:

### Layers

- `memory_object_cache`
- `page_index_cache`
- `block_index_cache`
- `disk_block_cache`
- `shared_store_read_through`

### Semantics

- `lookup_hot_to_cold`
- `refill_from_durable_on_miss`
- `invalidate_on_append_watermark`
- `invalidate_on_compaction_watermark`
- `cold_scan_no_promote`
- `writeback_backpressure_reported`

### Metrics

- `memory_cache_hits`
- `memory_cache_misses`
- `page_index_cache_hits`
- `page_index_cache_misses`
- `block_index_cache_hits`
- `block_index_cache_misses`
- `disk_cache_hits`
- `disk_cache_misses`
- `shared_store_read_throughs`
- `cache_refills`
- `cache_invalidations`
- `cache_writeback_queue_depth`
- `cache_writeback_rejections`

## Required Storage Lifecycle Metrics

Both C++ and Rust backends must expose these lifecycle metric names before stream/zone/eviction/GC/reclaim parity claims are accepted:

- `storage_manager_prepare_count`
- `storage_manager_reclaim_count`
- `storage_manager_evict_count`
- `storage_manager_expire_count`
- `storage_manager_page_gc_count`
- `storage_manager_block_gc_count`
- `storage_manager_compaction_count`
- `storage_manager_index_gc_count`
- `storage_manager_delayed_destroy_count`
- `storage_manager_follower_cursor_safety_count`
- `storage_manager_watermark_progress_count`
- `storage_manager_loop_ms`
- `stream_rollover_count`
- `segment_open_count`
- `segment_sealed_count`
- `storage_zone_total_bytes`
- `storage_zone_used_bytes`
- `storage_zone_stale_bytes`
- `append_log_replay_records`
- `append_log_reclaimed_records`
- `slot_dirty_generation_count`
- `slot_tombstone_count`
- `slot_stale_ref_count`
- `slot_owner_mismatch_count`
- `page_index_rebuild_count`
- `block_index_rebuild_count`
- `object_index_rebuild_count`
- `cache_admissions`
- `cache_evictions`
- `cache_rehydrates`
- `memory_cache_hits`
- `memory_cache_misses`
- `page_index_cache_hits`
- `page_index_cache_misses`
- `block_index_cache_hits`
- `block_index_cache_misses`
- `disk_cache_hits`
- `disk_cache_misses`
- `shared_store_read_throughs`
- `cache_refills`
- `cache_invalidations`
- `cache_writeback_queue_depth`
- `cache_writeback_rejections`
- `cold_scan_no_cache_reads`
- `hot_cache_promotions`
- `tombstone_records`
- `stale_page_tombstones`
- `stale_block_tombstones`
- `stale_pages_rewritten`
- `stale_pages_skipped`
- `stale_blocks_rewritten`
- `stale_blocks_skipped`
- `delayed_destroy_backlog`
- `follower_cursor_retention_floor`
- `reclaimable_bytes`
- `compaction_reclaimed_bytes`
- `physical_reclaimed_bytes`
- `physical_reclaim_errors`
- `append_watermark`
- `compaction_watermark`

## Raw Storage

| backend | write record QPS | write batch p95 | read QPS | read p95 | write errors | read errors |
|---|---:|---:|---:|---:|---:|---:|
| cpp | 0 | 0 ms | 0 | 0 ms | 0 | 0 |
| rust | 0 | 0 ms | 0 | 0 ms | 0 | 0 |

## Retrieval Stage Metrics

| backend | samples | query plan p95 | node traversal p95 | index prefilter p95 | candidate fetch p95 | score p95 | pack p95 | audit p95 | append queue wait p95 | append engine p95 | selected avg | dropped avg | scanned avg | index hits avg | index postings read avg | candidates avg | tokens avg | native timeouts | fallback flags | broad scan used | python pack fallback | native pack | cache hit rate | placement partitions avg |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|---:|---:|---:|---:|---:|
| cpp | 0 | 0.0 ms | 0.0 ms | 0.0 ms | 0.0 ms | 0.0 ms | 0.0 ms | 0.0 ms | 0.0 ms | 0.0 ms | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0 |  | 0 | 0 | 0 | 0.0 | 0.0 |
| rust | 0 | 0.0 ms | 0.0 ms | 0.0 ms | 0.0 ms | 0.0 ms | 0.0 ms | 0.0 ms | 0.0 ms | 0.0 ms | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0 |  | 0 | 0 | 0 | 0.0 | 0.0 |

## Status Labels

- feature_correct: `False`
- performance_candidate: `False`
- production_performance_parity: `False`

## Rust Vs C++ Parity

- feature parity: `failed`
- performance parity: `not_comparable`
- production performance parity: `unknown`
- min Rust/C++ QPS ratio: ``
- max Rust/C++ latency ratio: ``
- performance blockers: `0`

## Post-Phase Scale Matrix

- status: `incomplete`
- phase: `current`
- require gate: `False`
- open required cases: `10`
- full ContextMemory pipeline: `incomplete`

| group | case | status |
|---|---|---|
| event_ingestion | 1000 | open |
| event_ingestion | 10000 | passed |
| event_ingestion | 100000 | open |
| retrieve_workers | 4 | open |
| retrieve_workers | 8 | open |
| retrieve_workers | 16 | passed |
| retrieve_workers | 32 | open |
| resource_imports | large_pdf | open |
| resource_imports | large_csv | open |
| resource_imports | repo_directory | open |
| contextmemory_pipeline | resources | open |
| contextmemory_pipeline | skills | open |
| contextmemory_pipeline | cross_session_retrieval | passed |
| contextmemory_pipeline | compact_indexes | passed |
| contextmemory_pipeline | audit_light_telemetry | passed |

## Production Parity Policy Gate

- status: `failed`
- blockers: `3`

| rule | status | detail |
|---|---|---|
| correctness_before_latency | failed | Selected refs must be non-empty and logically equivalent across C++/Rust/Python before latency is considered. |
| cpp_selected_refs_non_empty | failed | cpp status=backend_startup_failed; selected_refs_max=0.0 |
| cpp_placement_index_driven | passed | cpp broad_scan_used_count=0; broad scan is fallback/debug only. |
| cpp_native_pack_or_dispatcher_only | passed | cpp python_pack_fallback_count=0, raw_candidate_tables_returned_count=0. |
| cpp_audit_not_hot_path_blocking | passed | cpp audit_p95_ms=0.0; rich audit/debug must be async/sampled by default. |
| rust_selected_refs_non_empty | failed | rust status=topology_not_ready; selected_refs_max=0.0 |
| rust_placement_index_driven | passed | rust broad_scan_used_count=0; broad scan is fallback/debug only. |
| rust_native_pack_or_dispatcher_only | passed | rust python_pack_fallback_count=0, raw_candidate_tables_returned_count=0. |
| rust_audit_not_hot_path_blocking | passed | rust audit_p95_ms=0.0; rich audit/debug must be async/sampled by default. |
| same_dataset_storage_topology_budget_batch_models | passed | Performance parity requires the same dataset, storage mode, topology, token budget, batch size, embedding model, reader, judge, and effective storage tuning for C++ and Rust. |
| same_effective_storage_tuning | passed | C++ and Rust passed backends must report the same effective TS_* storage tuning as the run config. all required knobs match |

### Policy

- Correctness beats latency: do not tune C++ performance until selected refs are non-empty and logically equivalent to Rust/Python.
- Python remains API/auth/model orchestration only; serving-critical scan/filter/pack/write work belongs in C++/Rust.
- Normal retrieval is placement-key and compact-index driven; broad scan is fallback/debug only.
- Audit/debug records do not block hot retrieval by default.
- Performance parity uses the same dataset, storage mode, topology, token budget, batch size, embedding model, reader, judge, and effective storage tuning for C++ and Rust.

## Comparison

`not_comparable`: both C++ and Rust backends must pass

## Phase 1 Native Retrieve Correctness Gate

- status: `failed`
- phase: `phase1_native_retrieve_correctness`
- minimum selected refs: `1`
- max selected-ref drift ratio: `0.35`
- selected-ref drift ratio: `None`

| backend | status | selected avg | selected max | dropped avg | scanned avg | index hits avg | postings avg | candidates avg | tokens avg | broad scan used | python pack fallback | native pack | timeouts | drop counters |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|
| cpp | backend_startup_failed | 0.0 | 0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0 | 0 | 0 | 0 | `{}` |
| rust | topology_not_ready | 0.0 | 0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0 | 0 | 0 | 0 | `{}` |

| failure | backend | details |
|---|---|---|
| backend_not_passed | cpp | `{"backend": "cpp", "error": "failed to load TemporalStore library: REPO_ROOT\\output-ubuntu22\\release\\sdk\\lib\\libbcache2.so: Could not find module 'REPO_ROOT\\output-ubuntu22\\release\\sdk\\lib\\libbcache2.so' (or one of its dependencies). Try using the full path with constructor syntax.; libbcache2.so: Could not find module 'libbcache2.so' (or one of its dependencies). Try using the full path with constructor syntax.; bcache2.dll: Could not find module 'bcache2.dll' (or one of its dependencies). Try using the full path with constructor syntax.; libbcache2.dylib: Could not find module 'libbcache2.dylib' (or one of its dependencies). Try using the full path with constructor syntax.", "reason": "backend_not_passed", "status": "backend_startup_failed"}` |
| backend_not_passed | rust | `{"backend": "rust", "error": null, "reason": "backend_not_passed", "status": "topology_not_ready"}` |
