# MatrixArk C++ vs Rust Scale Report

- run_id: `1782759060811`
- generated_at_ms: `1782759060811`
- events: `1000`
- messages_per_ingest: `20`
- ingest_workers: `4`
- retrieve_workers: `16`
- retrieve_queries: `128`

## Results

| backend | status | message QPS | ingest p50 | ingest p95 | ingest p99 | retrieve QPS | retrieve p50 | retrieve p95 | retrieve p99 | errors | partial packs |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| cpp | passed | 0.0 | 0.0 ms | 0.0 ms | 0.0 ms | 0.0 | 0.0 ms | 0.0 ms | 0.0 ms | 0 | 0 |
| rust | passed | 0.0 | 0.0 ms | 0.0 ms | 0.0 ms | 0.0 | 0.0 ms | 0.0 ms | 0.0 ms | 0 | 0 |

## Raw Storage

| backend | write record QPS | write batch p95 | read QPS | read p95 | write errors | read errors |
|---|---:|---:|---:|---:|---:|---:|
| cpp | 305.87 | 782.728 ms | 2553.429 | 1.699 ms | 0 | 0 |
| rust | 334.434 | 2853.937 ms | 1095.145 | 1.231 ms | 0 | 0 |

## Rust Minus C++

| metric | C++ | Rust | delta | percent delta |
|---|---:|---:|---:|---:|
| raw_write_record_qps | 305.87 | 334.434 | 28.564 | 9.339% |
| raw_write_p95_ms | 782.728 | 2853.937 | 2071.209 | 264.614% |
| raw_read_qps | 2553.429 | 1095.145 | -1458.284 | -57.111% |
| raw_read_p95_ms | 1.699 | 1.231 | -0.468 | -27.546% |
| message_qps | 0.0 | 0.0 | 0.0 | 0.0% |
| ingest_p50_ms | 0.0 | 0.0 | 0.0 | 0.0% |
| ingest_p95_ms | 0.0 | 0.0 | 0.0 | 0.0% |
| ingest_p99_ms | 0.0 | 0.0 | 0.0 | 0.0% |
| retrieve_qps | 0.0 | 0.0 | 0.0 | 0.0% |
| retrieve_p50_ms | 0.0 | 0.0 | 0.0 | 0.0% |
| retrieve_p95_ms | 0.0 | 0.0 | 0.0 | 0.0% |
| retrieve_p99_ms | 0.0 | 0.0 | 0.0 | 0.0% |
| selected_refs_avg | 0.0 | 0.0 | 0.0 | 0.0% |
