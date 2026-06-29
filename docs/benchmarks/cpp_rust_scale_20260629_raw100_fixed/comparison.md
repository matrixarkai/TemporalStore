# MatrixArk C++ vs Rust Scale Report

- run_id: `1782759050386`
- generated_at_ms: `1782759050386`
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
| cpp | 239.234 | 108.749 ms | 1640.944 | 1.846 ms | 0 | 0 |
| rust | 235.664 | 422.979 ms | 796.575 | 2.262 ms | 0 | 0 |

## Rust Minus C++

| metric | C++ | Rust | delta | percent delta |
|---|---:|---:|---:|---:|
| raw_write_record_qps | 239.234 | 235.664 | -3.57 | -1.492% |
| raw_write_p95_ms | 108.749 | 422.979 | 314.23 | 288.95% |
| raw_read_qps | 1640.944 | 796.575 | -844.369 | -51.456% |
| raw_read_p95_ms | 1.846 | 2.262 | 0.416 | 22.535% |
| message_qps | 0.0 | 0.0 | 0.0 | 0.0% |
| ingest_p50_ms | 0.0 | 0.0 | 0.0 | 0.0% |
| ingest_p95_ms | 0.0 | 0.0 | 0.0 | 0.0% |
| ingest_p99_ms | 0.0 | 0.0 | 0.0 | 0.0% |
| retrieve_qps | 0.0 | 0.0 | 0.0 | 0.0% |
| retrieve_p50_ms | 0.0 | 0.0 | 0.0 | 0.0% |
| retrieve_p95_ms | 0.0 | 0.0 | 0.0 | 0.0% |
| retrieve_p99_ms | 0.0 | 0.0 | 0.0 | 0.0% |
| selected_refs_avg | 0.0 | 0.0 | 0.0 | 0.0% |
