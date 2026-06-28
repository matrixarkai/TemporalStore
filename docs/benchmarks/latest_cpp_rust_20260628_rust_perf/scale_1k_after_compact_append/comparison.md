# MatrixArk C++ vs Rust Scale Comparison

- status: `passed`
- events: `1000`
- queries: `100`
- ingest mode: `batch`
- batch size: `20`

## Same Config Checks

- events: `True`
- queries: `True`
- ingest_mode: `True`
- batch_size: `True`
- namespace: `True`
- table: `True`
- request_timeout_ms: `True`
- io_timeout_ms: `True`
- storage_options: `True`
- native_topology_storage_mode_confirmed: `False`

## Native Storage Mode Verification

- C++ confirmed: `False`
- Rust confirmed: `False`
- C++ warning: `live backend readiness passed, but explicit async/sync/Raft storage mode topology proof was not included; pass --topology-report-json or run the C++/Rust scale wrapper with per-backend topology reports`
- Rust warning: `live backend readiness passed, but explicit async/sync/Raft storage mode topology proof was not included; pass --topology-report-json or run the C++/Rust scale wrapper with per-backend topology reports`

## Metrics

| Metric | C++ | Rust | Delta | Percent Delta | Rust/C++ |
| --- | ---: | ---: | ---: | ---: | ---: |
| `message_qps` | 1772.451 | 75.73 | -1696.721 | -95.727385 | 0.042726 |
| `retrieve_qps` | 13.141 | 34.345 | 21.204 | 161.357583 | 2.613576 |
| `hit_rate` | 0.0 | 0.0 | 0 | 0.0 | 1.0 |
| `errors` | 0 | 0 | 0 | 0.0 | 1.0 |
| `timeouts` | 0 | 0 | 0 | 0.0 | 1.0 |
| `ingest_p50_ms` | 10.275 | 259.803 | 249.528 | 2428.49635 | 25.284964 |
| `ingest_p95_ms` | 16.115 | 346.854 | 330.739 | 2052.36736 | 21.523674 |
| `ingest_p99_ms` | 24.355 | 367.303 | 342.948 | 1408.121536 | 15.081215 |
| `retrieve_p50_ms` | 71.825 | 28.827 | -42.998 | -59.86495 | 0.401351 |
| `retrieve_p95_ms` | 99.292 | 35.906 | -63.386 | -63.837973 | 0.36162 |
| `retrieve_p99_ms` | 140.054 | 53.803 | -86.251 | -61.584103 | 0.384159 |

## Artifacts

- C++ report: `docs/benchmarks/latest_cpp_rust_20260628_rust_perf/scale_1k_after_compact_append/cpp/scale_report.json`
- Rust report: `docs/benchmarks/latest_cpp_rust_20260628_rust_perf/scale_1k_after_compact_append/rust/scale_report.json`
- comparison JSON: `docs/benchmarks/latest_cpp_rust_20260628_rust_perf/scale_1k_after_compact_append/comparison.json`
- comparison Markdown: `docs/benchmarks/latest_cpp_rust_20260628_rust_perf/scale_1k_after_compact_append/comparison.md`
