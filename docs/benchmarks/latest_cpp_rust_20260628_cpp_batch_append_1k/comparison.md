# MatrixArk C++ vs Rust Scale Comparison

- status: `passed`
- events: `1000`
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
| `message_qps` | 1759.101 | 1681.112 | -77.989 | -4.433458 | 0.955665 |
| `retrieve_qps` | 10.786 | 225.346 | 214.56 | 1989.245318 | 20.892453 |
| `hit_rate` | 0.0 | 0.0 | 0 | 0.0 | 1.0 |
| `errors` | 0 | 0 | 0 | 0.0 | 1.0 |
| `timeouts` | 0 | 0 | 0 | 0.0 | 1.0 |
| `ingest_p50_ms` | 10.395 | 10.735 | 0.34 | 3.270803 | 1.032708 |
| `ingest_p95_ms` | 15.248 | 16.01 | 0.762 | 4.997377 | 1.049974 |
| `ingest_p99_ms` | 20.028 | 24.28 | 4.252 | 21.230278 | 1.212303 |
| `retrieve_p50_ms` | 93.717 | 3.049 | -90.668 | -96.746588 | 0.032534 |
| `retrieve_p95_ms` | 118.449 | 3.869 | -114.58 | -96.733615 | 0.032664 |
| `retrieve_p99_ms` | 130.782 | 31.015 | -99.767 | -76.284963 | 0.23715 |

## Artifacts

- C++ report: `docs/benchmarks/latest_cpp_rust_20260628_cpp_batch_append_1k/cpp/scale_report.json`
- Rust report: `docs/benchmarks/latest_cpp_rust_20260628_cpp_batch_append_1k/rust/scale_report.json`
- comparison JSON: `docs/benchmarks/latest_cpp_rust_20260628_cpp_batch_append_1k/comparison.json`
- comparison Markdown: `docs/benchmarks/latest_cpp_rust_20260628_cpp_batch_append_1k/comparison.md`
