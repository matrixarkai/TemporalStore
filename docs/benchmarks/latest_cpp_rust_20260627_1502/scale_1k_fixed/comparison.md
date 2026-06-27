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
| `message_qps` | 760.147 | 624.081 | -136.066 | -17.899959 | 0.821 |
| `retrieve_qps` | 11.992 | 1.615 | -10.377 | -86.532688 | 0.134673 |
| `hit_rate` | 0.0 | 0.0 | 0 | 0.0 | 1.0 |
| `errors` | 0 | 0 | 0 | 0.0 | 1.0 |
| `timeouts` | 0 | 0 | 0 | 0.0 | 1.0 |
| `ingest_p50_ms` | 24.783 | 30.486 | 5.703 | 23.011742 | 1.230117 |
| `ingest_p95_ms` | 42.547 | 47.089 | 4.542 | 10.675253 | 1.106753 |
| `ingest_p99_ms` | 48.797 | 57.274 | 8.477 | 17.37197 | 1.17372 |
| `retrieve_p50_ms` | 83.954 | 605.973 | 522.019 | 621.791695 | 7.217917 |
| `retrieve_p95_ms` | 92.978 | 693.922 | 600.944 | 646.329239 | 7.463292 |
| `retrieve_p99_ms` | 114.963 | 769.769 | 654.806 | 569.579778 | 6.695798 |

## Artifacts

- C++ report: `docs/benchmarks/latest_cpp_rust_20260627_1502/scale_1k_fixed/cpp/scale_report.json`
- Rust report: `docs/benchmarks/latest_cpp_rust_20260627_1502/scale_1k_fixed/rust/scale_report.json`
- comparison JSON: `docs/benchmarks/latest_cpp_rust_20260627_1502/scale_1k_fixed/comparison.json`
- comparison Markdown: `docs/benchmarks/latest_cpp_rust_20260627_1502/scale_1k_fixed/comparison.md`
