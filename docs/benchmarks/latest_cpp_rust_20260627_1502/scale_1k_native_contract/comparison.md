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
| `message_qps` | 689.342 | 571.662 | -117.68 | -17.071352 | 0.829286 |
| `retrieve_qps` | 10.538 | 1.693 | -8.845 | -83.934333 | 0.160657 |
| `hit_rate` | 0.0 | 0.0 | 0 | 0.0 | 1.0 |
| `errors` | 0 | 0 | 0 | 0.0 | 1.0 |
| `timeouts` | 0 | 0 | 0 | 0.0 | 1.0 |
| `ingest_p50_ms` | 24.424 | 29.555 | 5.131 | 21.008025 | 1.21008 |
| `ingest_p95_ms` | 55.338 | 58.035 | 2.697 | 4.873685 | 1.048737 |
| `ingest_p99_ms` | 61.191 | 59.413 | -1.778 | -2.905656 | 0.970943 |
| `retrieve_p50_ms` | 91.383 | 594.484 | 503.101 | 550.541129 | 6.505411 |
| `retrieve_p95_ms` | 119.785 | 658.344 | 538.559 | 449.604708 | 5.496047 |
| `retrieve_p99_ms` | 144.058 | 693.898 | 549.84 | 381.679601 | 4.816796 |

## Artifacts

- C++ report: `docs/benchmarks/latest_cpp_rust_20260627_1502/scale_1k_native_contract/cpp/scale_report.json`
- Rust report: `docs/benchmarks/latest_cpp_rust_20260627_1502/scale_1k_native_contract/rust/scale_report.json`
- comparison JSON: `docs/benchmarks/latest_cpp_rust_20260627_1502/scale_1k_native_contract/comparison.json`
- comparison Markdown: `docs/benchmarks/latest_cpp_rust_20260627_1502/scale_1k_native_contract/comparison.md`
