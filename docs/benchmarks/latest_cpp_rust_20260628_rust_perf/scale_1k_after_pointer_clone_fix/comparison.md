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
| `message_qps` | 1456.48 | 76.586 | -1379.894 | -94.741706 | 0.052583 |
| `retrieve_qps` | 12.587 | 35.196 | 22.609 | 179.621832 | 2.796218 |
| `hit_rate` | 0.0 | 0.0 | 0 | 0.0 | 1.0 |
| `errors` | 0 | 0 | 0 | 0.0 | 1.0 |
| `timeouts` | 0 | 0 | 0 | 0.0 | 1.0 |
| `ingest_p50_ms` | 12.503 | 248.776 | 236.273 | 1889.730465 | 19.897305 |
| `ingest_p95_ms` | 23.817 | 342.131 | 318.314 | 1336.499139 | 14.364991 |
| `ingest_p99_ms` | 33.153 | 432.446 | 399.293 | 1204.394776 | 13.043948 |
| `retrieve_p50_ms` | 73.885 | 27.469 | -46.416 | -62.821953 | 0.37178 |
| `retrieve_p95_ms` | 101.961 | 32.445 | -69.516 | -68.17901 | 0.31821 |
| `retrieve_p99_ms` | 142.529 | 55.635 | -86.894 | -60.965839 | 0.390342 |

## Artifacts

- C++ report: `docs/benchmarks/latest_cpp_rust_20260628_rust_perf/scale_1k_after_pointer_clone_fix/cpp/scale_report.json`
- Rust report: `docs/benchmarks/latest_cpp_rust_20260628_rust_perf/scale_1k_after_pointer_clone_fix/rust/scale_report.json`
- comparison JSON: `docs/benchmarks/latest_cpp_rust_20260628_rust_perf/scale_1k_after_pointer_clone_fix/comparison.json`
- comparison Markdown: `docs/benchmarks/latest_cpp_rust_20260628_rust_perf/scale_1k_after_pointer_clone_fix/comparison.md`
