# MatrixArk C++ vs Rust Scale Report

- run_id: `live_cpp_rust_compare_20260704`
- generated_at_ms: `1783155795288`
- events: `100`
- messages_per_ingest: `10`
- ingest_workers: `2`
- retrieve_workers: `4`
- retrieve_queries: `20`

## Results

| backend | status | message QPS | ingest p50 | ingest p95 | ingest p99 | retrieve QPS | retrieve p50 | retrieve p95 | retrieve p99 | errors | partial packs |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| cpp | passed | 74.732 | 196.907 ms | 394.853 ms | 394.853 ms | 1551.743 | 1.769 ms | 3.373 ms | 4.043 ms | 0 | 0 |
| rust | passed | 5.731 | 2663.581 ms | 5319.846 ms | 5319.846 ms | 629.169 | 3.437 ms | 12.722 ms | 15.109 ms | 0 | 0 |

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
| cpp | 2635.91 | 16.534 ms | 2794.385 | 7.096 ms | 0 | 0 |
| rust | 21.237 | 1749.747 ms | 372.806 | 77.052 ms | 0 | 0 |

## Retrieval Stage Metrics

| backend | samples | query plan p95 | node traversal p95 | index prefilter p95 | candidate fetch p95 | score p95 | pack p95 | audit p95 | append queue wait p95 | append engine p95 | selected avg | dropped avg | scanned avg | index hits avg | index postings read avg | candidates avg | tokens avg | native timeouts | fallback flags | broad scan used | python pack fallback | native pack | cache hit rate | placement partitions avg |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|---:|---:|---:|---:|---:|
| cpp | 20 | 0.0 ms | 0.0 ms | 0.0 ms | 0.0 ms | 0.0 ms | 0.0 ms | 0.0 ms | 0.0 ms | 0.0 ms | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0 |  | 0 | 0 | 0 | 0.0 | 0.0 |
| rust | 20 | 0.0 ms | 0.0 ms | 0.0 ms | 0.0 ms | 0.0 ms | 0.0 ms | 0.0 ms | 0.0 ms | 0.0 ms | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0 |  | 0 | 0 | 0 | 0.0 | 0.0 |

## Status Labels

- feature_correct: `False`
- performance_candidate: `True`
- production_performance_parity: `False`

## Rust Vs C++ Parity

- feature parity: `failed`
- performance parity: `failed`
- production performance parity: `failed`
- min Rust/C++ QPS ratio: `0.8`
- max Rust/C++ latency ratio: `2.0`
- performance blockers: `12`

## Post-Phase Scale Matrix

- status: `incomplete`
- phase: `current`
- require gate: `False`
- open required cases: `11`
- full ContextMemory pipeline: `incomplete`

| group | case | status |
|---|---|---|
| event_ingestion | 1000 | open |
| event_ingestion | 10000 | open |
| event_ingestion | 100000 | open |
| retrieve_workers | 4 | passed |
| retrieve_workers | 8 | open |
| retrieve_workers | 16 | open |
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
| cpp_selected_refs_non_empty | failed | cpp status=passed; selected_refs_max=0.0 |
| cpp_placement_index_driven | passed | cpp broad_scan_used_count=0; broad scan is fallback/debug only. |
| cpp_native_pack_or_dispatcher_only | passed | cpp python_pack_fallback_count=0, raw_candidate_tables_returned_count=0. |
| cpp_audit_not_hot_path_blocking | passed | cpp audit_p95_ms=0.0; rich audit/debug must be async/sampled by default. |
| rust_selected_refs_non_empty | failed | rust status=passed; selected_refs_max=0.0 |
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

## Phase 1 Native Retrieve Correctness Gate

- status: `failed`
- phase: `phase1_native_retrieve_correctness`
- shared requirements: `selected_ref_parity, scope_filtering, placement_filtering, compact_secondary_index_prefilter, stale_superseded_exclusion, shared_resource_skill_quota, cross_session_quota_rerank`
- minimum selected refs: `1`
- max selected-ref drift ratio: `0.35`
- selected-ref drift ratio: `None`

| backend | status | selected avg | selected max | dropped avg | scanned avg | index hits avg | index postings read avg | candidates avg | tokens avg | broad scan used | python pack fallback | native pack | timeouts | drop counters |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|
| cpp | passed | 0.0 | 0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0 | 0 | 0 | 0 | `{}` |
| rust | passed | 0.0 | 0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0 | 0 | 0 | 0 | `{}` |

| backend | selected-ref parity | scope filter | placement filter | compact index | stale/superseded | shared quota | cross-session rerank |
|---|---|---|---|---|---|---|---|
| cpp | False | False | False | False | False | False | False |
| rust | False | False | False | False | False | False | False |

| failure | backend | details |
|---|---|---|
| selected_refs_below_minimum | cpp | `{"backend": "cpp", "minimum": 1, "reason": "selected_refs_below_minimum", "selected_refs_avg": 0.0, "selected_refs_max": 0}` |
| missing_correctness_evidence | cpp | `{"backend": "cpp", "reason": "missing_correctness_evidence", "requirement": "scope_filtering"}` |
| missing_correctness_evidence | cpp | `{"backend": "cpp", "reason": "missing_correctness_evidence", "requirement": "placement_filtering"}` |
| missing_correctness_evidence | cpp | `{"backend": "cpp", "reason": "missing_correctness_evidence", "requirement": "compact_secondary_index_prefilter"}` |
| missing_correctness_evidence | cpp | `{"backend": "cpp", "reason": "missing_correctness_evidence", "requirement": "stale_superseded_exclusion"}` |
| missing_correctness_evidence | cpp | `{"backend": "cpp", "reason": "missing_correctness_evidence", "requirement": "shared_resource_skill_quota"}` |
| missing_correctness_evidence | cpp | `{"backend": "cpp", "reason": "missing_correctness_evidence", "requirement": "cross_session_quota_rerank"}` |
| selected_refs_below_minimum | rust | `{"backend": "rust", "minimum": 1, "reason": "selected_refs_below_minimum", "selected_refs_avg": 0.0, "selected_refs_max": 0}` |
| missing_correctness_evidence | rust | `{"backend": "rust", "reason": "missing_correctness_evidence", "requirement": "scope_filtering"}` |
| missing_correctness_evidence | rust | `{"backend": "rust", "reason": "missing_correctness_evidence", "requirement": "placement_filtering"}` |
| missing_correctness_evidence | rust | `{"backend": "rust", "reason": "missing_correctness_evidence", "requirement": "compact_secondary_index_prefilter"}` |
| missing_correctness_evidence | rust | `{"backend": "rust", "reason": "missing_correctness_evidence", "requirement": "stale_superseded_exclusion"}` |
| missing_correctness_evidence | rust | `{"backend": "rust", "reason": "missing_correctness_evidence", "requirement": "shared_resource_skill_quota"}` |
| missing_correctness_evidence | rust | `{"backend": "rust", "reason": "missing_correctness_evidence", "requirement": "cross_session_quota_rerank"}` |

## Performance Parity Gate

- status: `failed`
- minimum QPS ratio: `0.8`
- maximum latency ratio: `2.0`
- blockers: `12`
- correctness failures: `14`

| metric | C++ | Rust | threshold | ratio |
|---|---:|---:|---:|---:|
| raw_write_record_qps | 2635.91 | 21.237 | >= 2108.728 | 0.008057 |
| raw_write_p95_ms | 16.534 | 1749.747 | <= 33.068 | 105.827205 |
| raw_read_qps | 2794.385 | 372.806 | >= 2235.508 | 0.133413 |
| raw_read_p95_ms | 7.096 | 77.052 | <= 14.192 | 10.858512 |
| message_qps | 74.732 | 5.731 | >= 59.7856 | 0.076687 |
| ingest_p50_ms | 196.907 | 2663.581 | <= 393.814 | 13.527102 |
| ingest_p95_ms | 394.853 | 5319.846 | <= 789.706 | 13.472979 |
| ingest_p99_ms | 394.853 | 5319.846 | <= 789.706 | 13.472979 |
| retrieve_qps | 1551.743 | 629.169 | >= 1241.3944 | 0.40546 |
| retrieve_p95_ms | 3.373 | 12.722 | <= 6.746 | 3.771717 |
| retrieve_p99_ms | 4.043 | 15.109 | <= 8.086 | 3.737076 |
| selected_refs_avg | 0.0 | 0.0 | >= 1 on both backends and abs(delta) <= 1.0 | 1.0 |

## Rust Minus C++

| metric | C++ | Rust | delta | percent delta |
|---|---:|---:|---:|---:|
| raw_write_record_qps | 2635.91 | 21.237 | -2614.673 | -99.194% |
| raw_write_p95_ms | 16.534 | 1749.747 | 1733.213 | 10482.72% |
| raw_read_qps | 2794.385 | 372.806 | -2421.579 | -86.659% |
| raw_read_p95_ms | 7.096 | 77.052 | 69.956 | 985.851% |
| message_qps | 74.732 | 5.731 | -69.001 | -92.331% |
| ingest_p50_ms | 196.907 | 2663.581 | 2466.674 | 1252.71% |
| ingest_p95_ms | 394.853 | 5319.846 | 4924.993 | 1247.298% |
| ingest_p99_ms | 394.853 | 5319.846 | 4924.993 | 1247.298% |
| ingest_timeout_count | 0 | 0 | 0.0 | 0.0% |
| retrieve_qps | 1551.743 | 629.169 | -922.574 | -59.454% |
| retrieve_p50_ms | 1.769 | 3.437 | 1.668 | 94.291% |
| retrieve_p95_ms | 3.373 | 12.722 | 9.349 | 277.172% |
| retrieve_p99_ms | 4.043 | 15.109 | 11.066 | 273.708% |
| retrieve_timeout_count | 0 | 0 | 0.0 | 0.0% |
| partial_context_packs | 0 | 0 | 0.0 | 0.0% |
| selected_refs_avg | 0.0 | 0.0 | 0.0 | 0.0% |
| dropped_refs_avg | 0.0 | 0.0 | 0.0 | 0.0% |
| index_hits_avg | 0.0 | 0.0 | 0.0 | 0.0% |
| candidate_count_avg | 0.0 | 0.0 | 0.0 | 0.0% |
| token_count_avg | 0.0 | 0.0 | 0.0 | 0.0% |
| query_plan_p95_ms | 0.0 | 0.0 | 0.0 | 0.0% |
| node_traversal_p95_ms | 0.0 | 0.0 | 0.0 | 0.0% |
| index_prefilter_p95_ms | 0.0 | 0.0 | 0.0 | 0.0% |
| candidate_fetch_p95_ms | 0.0 | 0.0 | 0.0 | 0.0% |
| score_p95_ms | 0.0 | 0.0 | 0.0 | 0.0% |
| pack_p95_ms | 0.0 | 0.0 | 0.0 | 0.0% |
| audit_p95_ms | 0.0 | 0.0 | 0.0 | 0.0% |
| append_queue_wait_p95_ms | 0.0 | 0.0 | 0.0 | 0.0% |
| append_engine_p95_ms | 0.0 | 0.0 | 0.0 | 0.0% |
| native_timeout_count | 0 | 0 | 0.0 | 0.0% |
| scanned_records_avg | 0.0 | 0.0 | 0.0 | 0.0% |
| cache_hit_rate | 0.0 | 0.0 | 0.0 | 0.0% |
| index_postings_read_avg | 0.0 | 0.0 | 0.0 | 0.0% |
| index_postings_touched_avg | 0.0 | 0.0 | 0.0 | 0.0% |
| broad_scan_used_count | 0 | 0 | 0.0 | 0.0% |
| broad_scan_blocked_count | 20 | 0 | -20.0 | -100.0% |
| native_pack_assembly_count | 0 | 0 | 0.0 | 0.0% |
| python_pack_fallback_count | 0 | 0 | 0.0 | 0.0% |
| raw_candidate_tables_returned_count | 0 | 0 | 0.0 | 0.0% |
| placement_partitions_touched_avg | 0.0 | 0.0 | 0.0 | 0.0% |
| memory_fallback | 0 | 0 | 0.0 | 0.0% |
| hash_embedding_fallback | 0 | 0 | 0.0 | 0.0% |
| partial_pack_fallback | 0 | 0 | 0.0 | 0.0% |
| native_metrics_missing | 0 | 0 | 0.0 | 0.0% |
