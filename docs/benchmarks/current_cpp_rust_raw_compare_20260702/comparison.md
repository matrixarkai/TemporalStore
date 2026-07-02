# MatrixArk C++ vs Rust Scale Report

- run_id: `current_cpp_rust_raw_compare`
- generated_at_ms: `1782994383026`
- events: `10`
- messages_per_ingest: `5`
- ingest_workers: `1`
- retrieve_workers: `1`
- retrieve_queries: `0`

## Results

| backend | status | message QPS | ingest p50 | ingest p95 | ingest p99 | retrieve QPS | retrieve p50 | retrieve p95 | retrieve p99 | errors | partial packs |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| cpp | passed | 0.0 | 0.0 ms | 0.0 ms | 0.0 ms | 0.0 | 0.0 ms | 0.0 ms | 0.0 ms | 0 | 0 |
| rust | passed | 0.0 | 0.0 ms | 0.0 ms | 0.0 ms | 0.0 | 0.0 ms | 0.0 ms | 0.0 ms | 0 | 0 |

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
- `compaction_reclaimed_bytes`
- `cold_scan_no_cache_reads`
- `hot_cache_promotions`
- `append_watermark`
- `compaction_watermark`

## Raw Storage

| backend | write record QPS | write batch p95 | read QPS | read p95 | write errors | read errors |
|---|---:|---:|---:|---:|---:|---:|
| cpp | 168.418 | 591.936 ms | 1315.825 | 36.403 ms | 0 | 0 |
| rust | 200.721 | 495.08 ms | 1885.554 | 25.119 ms | 0 | 0 |

## Retrieval Stage Metrics

| backend | samples | query plan p95 | node traversal p95 | index prefilter p95 | candidate fetch p95 | score p95 | pack p95 | audit p95 | append queue wait p95 | append engine p95 | selected avg | dropped avg | scanned avg | index hits avg | index postings read avg | candidates avg | tokens avg | native timeouts | fallback flags | broad scan used | python pack fallback | native pack | cache hit rate | placement partitions avg |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|---:|---:|---:|---:|---:|
| cpp | 0 | 0.0 ms | 0.0 ms | 0.0 ms | 0.0 ms | 0.0 ms | 0.0 ms | 0.0 ms | 0.0 ms | 0.0 ms | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0 |  | 0 | 0 | 0 | 0.0 | 0.0 |
| rust | 0 | 0.0 ms | 0.0 ms | 0.0 ms | 0.0 ms | 0.0 ms | 0.0 ms | 0.0 ms | 0.0 ms | 0.0 ms | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0 |  | 0 | 0 | 0 | 0.0 | 0.0 |

## Status Labels

- feature_correct: `True`
- performance_candidate: `True`
- production_performance_parity: `False`

## Rust Vs C++ Parity

- feature parity: `skipped`
- performance parity: `passed`
- production performance parity: `failed`
- min Rust/C++ QPS ratio: `0.8`
- max Rust/C++ latency ratio: `2.0`
- performance blockers: `0`

## Post-Phase Scale Matrix

- status: `incomplete`
- phase: `current`
- require gate: `False`
- open required cases: `15`
- full ContextMemory pipeline: `incomplete`

| group | case | status |
|---|---|---|
| event_ingestion | 1000 | open |
| event_ingestion | 10000 | open |
| event_ingestion | 100000 | open |
| retrieve_workers | 4 | open |
| retrieve_workers | 8 | open |
| retrieve_workers | 16 | open |
| retrieve_workers | 32 | open |
| resource_imports | large_pdf | open |
| resource_imports | large_csv | open |
| resource_imports | repo_directory | open |
| contextmemory_pipeline | resources | open |
| contextmemory_pipeline | skills | open |
| contextmemory_pipeline | cross_session_retrieval | open |
| contextmemory_pipeline | compact_indexes | open |
| contextmemory_pipeline | audit_light_telemetry | open |

## Production Parity Policy Gate

- status: `failed`
- blockers: `3`

| rule | status | detail |
|---|---|---|
| correctness_before_latency | failed | Selected refs must be non-empty and logically equivalent across C++/Rust/Python before latency is considered. |
| cpp_selected_refs_non_empty | failed | cpp selected_refs_max=0.0 |
| cpp_placement_index_driven | passed | cpp broad_scan_used_count=0; broad scan is fallback/debug only. |
| cpp_native_pack_or_dispatcher_only | passed | cpp python_pack_fallback_count=0, raw_candidate_tables_returned_count=0. |
| cpp_audit_not_hot_path_blocking | passed | cpp audit_p95_ms=0.0; rich audit/debug must be async/sampled by default. |
| rust_selected_refs_non_empty | failed | rust selected_refs_max=0.0 |
| rust_placement_index_driven | passed | rust broad_scan_used_count=0; broad scan is fallback/debug only. |
| rust_native_pack_or_dispatcher_only | passed | rust python_pack_fallback_count=0, raw_candidate_tables_returned_count=0. |
| rust_audit_not_hot_path_blocking | passed | rust audit_p95_ms=0.0; rich audit/debug must be async/sampled by default. |
| same_dataset_storage_topology_budget_batch_models | passed | Performance parity requires the same dataset, storage mode, topology, token budget, batch size, embedding model, reader, judge, and effective storage tuning for C++ and Rust. |

### Policy

- Correctness beats latency: do not tune C++ performance until selected refs are non-empty and logically equivalent to Rust/Python.
- Python remains API/auth/model orchestration only; serving-critical scan/filter/pack/write work belongs in C++/Rust.
- Normal retrieval is placement-key and compact-index driven; broad scan is fallback/debug only.
- Audit/debug records do not block hot retrieval by default.
- Performance parity uses the same dataset, storage mode, topology, token budget, batch size, embedding model, reader, judge, and effective storage tuning for C++ and Rust.

## Phase 1 Native Retrieve Correctness Gate

- status: `skipped`
- phase: `None`
- shared requirements: `selected_ref_parity, scope_filtering, placement_filtering, compact_secondary_index_prefilter, stale_superseded_exclusion, shared_resource_skill_quota, cross_session_quota_rerank`
- minimum selected refs: `None`
- max selected-ref drift ratio: `None`
- selected-ref drift ratio: `None`

## Performance Parity Gate

- status: `passed`
- minimum QPS ratio: `0.8`
- maximum latency ratio: `2.0`
- blockers: `0`
- correctness failures: `0`

## Rust Minus C++

| metric | C++ | Rust | delta | percent delta |
|---|---:|---:|---:|---:|
| raw_write_record_qps | 168.418 | 200.721 | 32.303 | 19.18% |
| raw_write_p95_ms | 591.936 | 495.08 | -96.856 | -16.363% |
| raw_read_qps | 1315.825 | 1885.554 | 569.729 | 43.298% |
| raw_read_p95_ms | 36.403 | 25.119 | -11.284 | -30.997% |
| message_qps | 0.0 | 0.0 | 0.0 | 0.0% |
| ingest_p50_ms | 0.0 | 0.0 | 0.0 | 0.0% |
| ingest_p95_ms | 0.0 | 0.0 | 0.0 | 0.0% |
| ingest_p99_ms | 0.0 | 0.0 | 0.0 | 0.0% |
| ingest_timeout_count | 0 | 0 | 0.0 | 0.0% |
| retrieve_qps | 0.0 | 0.0 | 0.0 | 0.0% |
| retrieve_p50_ms | 0.0 | 0.0 | 0.0 | 0.0% |
| retrieve_p95_ms | 0.0 | 0.0 | 0.0 | 0.0% |
| retrieve_p99_ms | 0.0 | 0.0 | 0.0 | 0.0% |
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
| broad_scan_blocked_count | 0 | 0 | 0.0 | 0.0% |
| native_pack_assembly_count | 0 | 0 | 0.0 | 0.0% |
| python_pack_fallback_count | 0 | 0 | 0.0 | 0.0% |
| raw_candidate_tables_returned_count | 0 | 0 | 0.0 | 0.0% |
| placement_partitions_touched_avg | 0.0 | 0.0 | 0.0 | 0.0% |
| memory_fallback | 0 | 0 | 0.0 | 0.0% |
| hash_embedding_fallback | 0 | 0 | 0.0 | 0.0% |
| partial_pack_fallback | 0 | 0 | 0.0 | 0.0% |
| native_metrics_missing | 1 | 1 | 0.0 | 0.0% |
