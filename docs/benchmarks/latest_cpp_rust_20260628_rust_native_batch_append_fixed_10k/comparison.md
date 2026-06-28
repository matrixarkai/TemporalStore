# MatrixArk C++ vs Rust Scale Comparison

- status: `passed`
- events: `10000`
- queries: `20`
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
- native_topology_storage_mode_confirmed: `True`

## Native Storage Mode Verification

- C++ confirmed: `True`
- Rust confirmed: `True`
- C++ warning: ``
- Rust warning: ``

## Metrics

| Metric | C++ | Rust | Delta | Percent Delta | Rust/C++ |
| --- | ---: | ---: | ---: | ---: | ---: |
| `message_qps` | 1872.565 | 1283.615 | -588.95 | -31.451512 | 0.685485 |
| `retrieve_qps` | 1.537 | 7.611 | 6.074 | 395.185426 | 4.951854 |
| `hit_rate` | 0.0 | 0.0 | 0 | 0.0 | 1.0 |
| `errors` | 0 | 0 | 0 | 0.0 | 1.0 |
| `timeouts` | 0 | 0 | 0 | 0.0 | 1.0 |
| `ingest_p50_ms` | 9.987 | 13.289 | 3.302 | 33.062982 | 1.33063 |
| `ingest_p95_ms` | 14.564 | 25.292 | 10.728 | 73.661082 | 1.736611 |
| `ingest_p99_ms` | 18.309 | 42.781 | 24.472 | 133.661041 | 2.33661 |
| `retrieve_p50_ms` | 640.925 | 68.442 | -572.483 | -89.321371 | 0.106786 |
| `retrieve_p95_ms` | 724.556 | 118.704 | -605.852 | -83.617001 | 0.16383 |
| `retrieve_p99_ms` | 752.37 | 1448.746 | 696.376 | 92.557651 | 1.925577 |

## Artifacts

- C++ report: `docs/benchmarks/latest_cpp_rust_20260628_rust_native_batch_append_fixed_10k/cpp/scale_report.json`
- Rust report: `docs/benchmarks/latest_cpp_rust_20260628_rust_native_batch_append_fixed_10k/rust/scale_report.json`
- comparison JSON: `docs/benchmarks/latest_cpp_rust_20260628_rust_native_batch_append_fixed_10k/comparison.json`
- comparison Markdown: `docs/benchmarks/latest_cpp_rust_20260628_rust_native_batch_append_fixed_10k/comparison.md`
