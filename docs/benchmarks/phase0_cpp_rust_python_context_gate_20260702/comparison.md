# MatrixArk C++ vs Rust Scale Report

- run_id: `1782946964749`
- generated_at_ms: `1782946964754`
- events: `40`
- messages_per_ingest: `10`
- ingest_workers: `2`
- retrieve_workers: `4`
- retrieve_queries: `8`

## Results

| backend | status | message QPS | ingest p50 | ingest p95 | ingest p99 | retrieve QPS | retrieve p50 | retrieve p95 | retrieve p99 | errors | partial packs |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| cpp | blocked_timeout | 0 | 0 ms | 0 ms | 0 ms | 0 | 0 ms | 0 ms | 0 ms | 0 | 0 |
| rust | passed | 9.168 | 3360.589 ms | 3694.392 ms | 3694.392 ms | 3.156 | 1144.63 ms | 1635.948 ms | 1635.948 ms | 0 | 0 |
| python_ref | passed | 387.868 | 65.82 ms | 68.854 ms | 68.854 ms | 20.956 | 181.329 ms | 195.252 ms | 195.252 ms | 0 | 0 |

## Raw Storage

| backend | write record QPS | write batch p95 | read QPS | read p95 | write errors | read errors |
|---|---:|---:|---:|---:|---:|---:|
| cpp | 0 | 0 ms | 0 | 0 ms | 0 | 0 |
| rust | 4.736 | 16890.517 ms | 12.814 | 3120.011 ms | 0 | 0 |
| python_ref | 0 | 0 ms | 0 | 0 ms | 0 | 0 |

## Retrieval Stage Metrics

| backend | samples | query plan p95 | node traversal p95 | index prefilter p95 | candidate fetch p95 | score p95 | pack p95 | audit p95 | selected avg | dropped avg | scanned avg | index hits avg | candidates avg | tokens avg | cache hit rate | placement partitions avg |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| cpp | 0 | 0.0 ms | 0.0 ms | 0.0 ms | 0.0 ms | 0.0 ms | 0.0 ms | 0.0 ms | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 |
| rust | 8 | 16.396 ms | 2.788 ms | 573.288 ms | 573.288 ms | 581.6 ms | 831.385 ms | 0.203 ms | 4.0 | 0.0 | 44.0 | 0.0 | 4.0 | 840.0 | 0.0 | 0.0 |
| python_ref | 8 | 14.125 ms | 2.026 ms | 70.852 ms | 70.852 ms | 101.059 ms | 88.048 ms | 0.139 ms | 4.0 | 0.0 | 295.0 | 0.0 | 4.0 | 840.0 | 0.0 | 0.0 |

## Comparison

`not_comparable`: both C++ and Rust backends must pass

## Phase 0 Correctness Gate

- status: `failed`
- minimum selected refs: `1`
- max selected-ref drift ratio: `0.35`
- selected-ref drift ratio: `0.0`

| backend | status | selected avg | selected max | dropped avg | scanned avg | index hits avg | candidates avg | tokens avg | timeouts |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|
| cpp | blocked_timeout | 0.0 | 0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0 |
| rust | passed | 4.0 | 4 | 0.0 | 44.0 | 0.0 | 4.0 | 840.0 | 0 |
| python_ref | passed | 4.0 | 4 | 0.0 | 295.0 | 0.0 | 4.0 | 840.0 | 0 |

| failure | backend | details |
|---|---|---|
| backend_not_passed | cpp | `{"backend": "cpp", "error": "backend worker did not write result artifact", "reason": "backend_not_passed", "status": "blocked_timeout"}` |
