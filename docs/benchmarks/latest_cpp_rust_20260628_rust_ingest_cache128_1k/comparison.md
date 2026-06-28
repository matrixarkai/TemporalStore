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
| `message_qps` | 1467.976 | 1630.727 | 162.751 | 11.086762 | 1.110868 |
| `retrieve_qps` | 12.138 | 488.063 | 475.925 | 3920.950733 | 40.209507 |
| `hit_rate` | 0.0 | 0.0 | 0 | 0.0 | 1.0 |
| `errors` | 0 | 0 | 0 | 0.0 | 1.0 |
| `timeouts` | 0 | 0 | 0 | 0.0 | 1.0 |
| `ingest_p50_ms` | 12.49 | 11.318 | -1.172 | -9.383507 | 0.906165 |
| `ingest_p95_ms` | 20.283 | 16.172 | -4.111 | -20.268205 | 0.797318 |
| `ingest_p99_ms` | 25.166 | 19.295 | -5.871 | -23.329095 | 0.766709 |
| `retrieve_p50_ms` | 82.18 | 1.807 | -80.373 | -97.801168 | 0.021988 |
| `retrieve_p95_ms` | 110.112 | 2.383 | -107.729 | -97.83584 | 0.021642 |
| `retrieve_p99_ms` | 147.491 | 10.011 | -137.48 | -93.212467 | 0.067875 |

## Artifacts

- C++ report: `docs/benchmarks/latest_cpp_rust_20260628_rust_ingest_cache128_1k/cpp/scale_report.json`
- Rust report: `docs/benchmarks/latest_cpp_rust_20260628_rust_ingest_cache128_1k/rust/scale_report.json`
- comparison JSON: `docs/benchmarks/latest_cpp_rust_20260628_rust_ingest_cache128_1k/comparison.json`
- comparison Markdown: `docs/benchmarks/latest_cpp_rust_20260628_rust_ingest_cache128_1k/comparison.md`
