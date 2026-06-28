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
| `message_qps` | 920.796 | 1286.359 | 365.563 | 39.700759 | 1.397008 |
| `retrieve_qps` | 1.283 | 282.335 | 281.052 | 21905.845674 | 220.058457 |
| `hit_rate` | 0.0 | 0.0 | 0 | 0.0 | 1.0 |
| `errors` | 0 | 0 | 0 | 0.0 | 1.0 |
| `timeouts` | 0 | 0 | 0 | 0.0 | 1.0 |
| `ingest_p50_ms` | 18.925 | 10.858 | -8.067 | -42.626156 | 0.573738 |
| `ingest_p95_ms` | 35.596 | 15.935 | -19.661 | -55.233734 | 0.447663 |
| `ingest_p99_ms` | 46.119 | 24.307 | -21.812 | -47.295041 | 0.52705 |
| `retrieve_p50_ms` | 706.296 | 2.136 | -704.16 | -99.697577 | 0.003024 |
| `retrieve_p95_ms` | 1403.821 | 2.59 | -1401.231 | -99.815504 | 0.001845 |
| `retrieve_p99_ms` | 1520.74 | 29.424 | -1491.316 | -98.065152 | 0.019348 |

## Artifacts

- C++ report: `docs/benchmarks/latest_cpp_rust_20260628_cpp_batch_append_10k/cpp/scale_report.json`
- Rust report: `docs/benchmarks/latest_cpp_rust_20260628_cpp_batch_append_10k/rust/scale_report.json`
- comparison JSON: `docs/benchmarks/latest_cpp_rust_20260628_cpp_batch_append_10k/comparison.json`
- comparison Markdown: `docs/benchmarks/latest_cpp_rust_20260628_cpp_batch_append_10k/comparison.md`
