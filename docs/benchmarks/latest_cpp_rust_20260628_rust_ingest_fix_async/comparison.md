# MatrixArk C++ vs Rust Scale Comparison

- status: `passed`
- events: `1000`
- queries: `40`
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
| `message_qps` | 1413.611 | 1501.503 | 87.892 | 6.217552 | 1.062176 |
| `retrieve_qps` | 11.583 | 457.797 | 446.214 | 3852.318052 | 39.523181 |
| `hit_rate` | 0.0 | 0.0 | 0 | 0.0 | 1.0 |
| `errors` | 0 | 0 | 0 | 0.0 | 1.0 |
| `timeouts` | 0 | 0 | 0 | 0.0 | 1.0 |
| `ingest_p50_ms` | 13.707 | 12.448 | -1.259 | -9.185088 | 0.908149 |
| `ingest_p95_ms` | 22.718 | 18.068 | -4.65 | -20.468351 | 0.795316 |
| `ingest_p99_ms` | 25.007 | 21.211 | -3.796 | -15.17975 | 0.848203 |
| `retrieve_p50_ms` | 82.98 | 1.91 | -81.07 | -97.698241 | 0.023018 |
| `retrieve_p95_ms` | 121.448 | 2.632 | -118.816 | -97.832817 | 0.021672 |
| `retrieve_p99_ms` | 188.148 | 10.233 | -177.915 | -94.561197 | 0.054388 |

## Artifacts

- C++ report: `docs/benchmarks/latest_cpp_rust_20260628_rust_ingest_fix_async/cpp/scale_report.json`
- Rust report: `docs/benchmarks/latest_cpp_rust_20260628_rust_ingest_fix_async/rust/scale_report.json`
- comparison JSON: `docs/benchmarks/latest_cpp_rust_20260628_rust_ingest_fix_async/comparison.json`
- comparison Markdown: `docs/benchmarks/latest_cpp_rust_20260628_rust_ingest_fix_async/comparison.md`
