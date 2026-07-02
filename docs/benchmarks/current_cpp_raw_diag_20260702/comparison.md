# MatrixArk C++ vs Rust Scale Report

- run_id: `current_cpp_raw_diag`
- generated_at_ms: `1782994343503`
- events: `10`
- messages_per_ingest: `5`
- ingest_workers: `1`
- retrieve_workers: `1`
- retrieve_queries: `0`

## Results

| backend | status | message QPS | ingest p50 | ingest p95 | ingest p99 | retrieve QPS | retrieve p50 | retrieve p95 | retrieve p99 | errors | partial packs |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| cpp | passed | 0.0 | 0.0 ms | 0.0 ms | 0.0 ms | 0.0 | 0.0 ms | 0.0 ms | 0.0 ms | 0 | 0 |

## Effective Storage Tuning

| backend | context page target | block segment target | storage zone | stream max blob | compaction watermark | cold scan no-cache | page index cache | block index cache | effective block segment |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| cpp | 65536 | 1073741824 | 10485760 | 10485760 | 268435456 | True | 67108864 | 67108864 | 10485760 |

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
| cpp | 114.983 | 172.044 ms | 1199.741 | 7.582 ms | 0 | 0 |

## Retrieval Stage Metrics

| backend | samples | query plan p95 | node traversal p95 | index prefilter p95 | candidate fetch p95 | score p95 | pack p95 | audit p95 | append queue wait p95 | append engine p95 | selected avg | dropped avg | scanned avg | index hits avg | index postings read avg | candidates avg | tokens avg | native timeouts | fallback flags | broad scan used | python pack fallback | native pack | cache hit rate | placement partitions avg |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|---:|---:|---:|---:|---:|
| cpp | 0 | 0.0 ms | 0.0 ms | 0.0 ms | 0.0 ms | 0.0 ms | 0.0 ms | 0.0 ms | 0.0 ms | 0.0 ms | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0 |  | 0 | 0 | 0 | 0.0 | 0.0 |

## Status Labels

- feature_correct: `False`
- performance_candidate: `False`
- production_performance_parity: `False`

## Rust Vs C++ Parity

- feature parity: `skipped`
- performance parity: `not_comparable`
- production performance parity: `unknown`
- min Rust/C++ QPS ratio: ``
- max Rust/C++ latency ratio: ``
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
- blockers: `2`

| rule | status | detail |
|---|---|---|
| correctness_before_latency | failed | Selected refs must be non-empty and logically equivalent across C++/Rust/Python before latency is considered. |
| cpp_selected_refs_non_empty | failed | cpp selected_refs_max=0.0 |
| cpp_placement_index_driven | passed | cpp broad_scan_used_count=0; broad scan is fallback/debug only. |
| cpp_native_pack_or_dispatcher_only | passed | cpp python_pack_fallback_count=0, raw_candidate_tables_returned_count=0. |
| cpp_audit_not_hot_path_blocking | passed | cpp audit_p95_ms=0.0; rich audit/debug must be async/sampled by default. |
| rust_selected_refs_non_empty | passed | rust selected_refs_max=0.0 |
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

## Comparison

`not_comparable`: both C++ and Rust backends must pass

## Phase 1 Native Retrieve Correctness Gate

- status: `skipped`
- phase: `None`
- minimum selected refs: `None`
- max selected-ref drift ratio: `None`
- selected-ref drift ratio: `None`

| backend | status | selected avg | selected max | dropped avg | scanned avg | index hits avg | postings avg | candidates avg | tokens avg | broad scan used | python pack fallback | native pack | timeouts | drop counters |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|
