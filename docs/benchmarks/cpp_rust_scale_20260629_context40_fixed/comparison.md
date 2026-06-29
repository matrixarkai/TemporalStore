# MatrixArk C++ vs Rust Scale Report

- run_id: `1782759078566`
- generated_at_ms: `1782759078566`
- events: `40`
- messages_per_ingest: `10`
- ingest_workers: `2`
- retrieve_workers: `4`
- retrieve_queries: `8`

## Results

| backend | status | message QPS | ingest p50 | ingest p95 | ingest p99 | retrieve QPS | retrieve p50 | retrieve p95 | retrieve p99 | errors | partial packs |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| cpp | passed | 2.416 | 14541.385 ms | 15550.636 ms | 15550.636 ms | 0.994 | 3988.719 ms | 7043.762 ms | 7043.762 ms | 0 | 0 |
| rust | passed | 204.069 | 148.864 ms | 164.549 ms | 164.549 ms | 47.856 | 86.314 ms | 89.641 ms | 89.641 ms | 0 | 0 |

## Raw Storage

| backend | write record QPS | write batch p95 | read QPS | read p95 | write errors | read errors |
|---|---:|---:|---:|---:|---:|---:|
| cpp | 183.86 | 149.543 ms | 1766.577 | 1.96 ms | 0 | 0 |
| rust | 224.056 | 445.195 ms | 863.087 | 2.71 ms | 0 | 0 |

## Rust Minus C++

| metric | C++ | Rust | delta | percent delta |
|---|---:|---:|---:|---:|
| raw_write_record_qps | 183.86 | 224.056 | 40.196 | 21.862% |
| raw_write_p95_ms | 149.543 | 445.195 | 295.652 | 197.704% |
| raw_read_qps | 1766.577 | 863.087 | -903.49 | -51.144% |
| raw_read_p95_ms | 1.96 | 2.71 | 0.75 | 38.265% |
| message_qps | 2.416 | 204.069 | 201.653 | 8346.565% |
| ingest_p50_ms | 14541.385 | 148.864 | -14392.521 | -98.976% |
| ingest_p95_ms | 15550.636 | 164.549 | -15386.087 | -98.942% |
| ingest_p99_ms | 15550.636 | 164.549 | -15386.087 | -98.942% |
| retrieve_qps | 0.994 | 47.856 | 46.862 | 4714.487% |
| retrieve_p50_ms | 3988.719 | 86.314 | -3902.405 | -97.836% |
| retrieve_p95_ms | 7043.762 | 89.641 | -6954.121 | -98.727% |
| retrieve_p99_ms | 7043.762 | 89.641 | -6954.121 | -98.727% |
| selected_refs_avg | 0.0 | 0.0 | 0.0 | 0.0% |
