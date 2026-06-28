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
| `message_qps` | 156.216 | 34.397 | -121.819 | -77.981129 | 0.220189 |
| `retrieve_qps` | 3.519 | 28.318 | 24.799 | 704.717249 | 8.047172 |
| `hit_rate` | 0.0 | 0.0 | 0 | 0.0 | 1.0 |
| `errors` | 0 | 0 | 0 | 0.0 | 1.0 |
| `timeouts` | 0 | 0 | 0 | 0.0 | 1.0 |
| `ingest_p50_ms` | 102.026 | 438.69 | 336.664 | 329.978633 | 4.299786 |
| `ingest_p95_ms` | 364.146 | 1616.616 | 1252.47 | 343.947208 | 4.439472 |
| `ingest_p99_ms` | 434.402 | 1966.98 | 1532.578 | 352.801783 | 4.528018 |
| `retrieve_p50_ms` | 274.192 | 30.415 | -243.777 | -88.907408 | 0.110926 |
| `retrieve_p95_ms` | 445.074 | 42.505 | -402.569 | -90.449903 | 0.095501 |
| `retrieve_p99_ms` | 567.769 | 65.226 | -502.543 | -88.511877 | 0.114881 |

## Artifacts

- C++ report: `docs/benchmarks/latest_cpp_rust_20260628_rust_perf/scale_1k_after_nonserving_trim/cpp/scale_report.json`
- Rust report: `docs/benchmarks/latest_cpp_rust_20260628_rust_perf/scale_1k_after_nonserving_trim/rust/scale_report.json`
- comparison JSON: `docs/benchmarks/latest_cpp_rust_20260628_rust_perf/scale_1k_after_nonserving_trim/comparison.json`
- comparison Markdown: `docs/benchmarks/latest_cpp_rust_20260628_rust_perf/scale_1k_after_nonserving_trim/comparison.md`
