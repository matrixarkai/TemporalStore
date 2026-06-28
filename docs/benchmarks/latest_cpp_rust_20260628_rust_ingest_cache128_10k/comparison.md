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
- native_topology_storage_mode_confirmed: `False`

## Native Storage Mode Verification

- C++ confirmed: `False`
- Rust confirmed: `False`
- C++ warning: `live backend readiness passed, but explicit async/sync/Raft storage mode topology proof was not included; pass --topology-report-json or run the C++/Rust scale wrapper with per-backend topology reports`
- Rust warning: `live backend readiness passed, but explicit async/sync/Raft storage mode topology proof was not included; pass --topology-report-json or run the C++/Rust scale wrapper with per-backend topology reports`

## Metrics

| Metric | C++ | Rust | Delta | Percent Delta | Rust/C++ |
| --- | ---: | ---: | ---: | ---: | ---: |
| `message_qps` | 857.75 | 740.38 | -117.37 | -13.683474 | 0.863165 |
| `retrieve_qps` | 1.183 | 358.261 | 357.078 | 30184.108199 | 302.841082 |
| `hit_rate` | 0.0 | 0.0 | 0 | 0.0 | 1.0 |
| `errors` | 0 | 0 | 0 | 0.0 | 1.0 |
| `timeouts` | 0 | 0 | 0 | 0.0 | 1.0 |
| `ingest_p50_ms` | 19.184 | 18.168 | -1.016 | -5.29608 | 0.947039 |
| `ingest_p95_ms` | 39.916 | 44.529 | 4.613 | 11.556769 | 1.115568 |
| `ingest_p99_ms` | 48.688 | 61.768 | 13.08 | 26.864936 | 1.268649 |
| `retrieve_p50_ms` | 765.874 | 1.957 | -763.917 | -99.744475 | 0.002555 |
| `retrieve_p95_ms` | 1396.51 | 5.412 | -1391.098 | -99.612462 | 0.003875 |
| `retrieve_p99_ms` | 1768.223 | 14.049 | -1754.174 | -99.205474 | 0.007945 |

## Artifacts

- C++ report: `docs/benchmarks/latest_cpp_rust_20260628_rust_ingest_cache128_10k/cpp/scale_report.json`
- Rust report: `docs/benchmarks/latest_cpp_rust_20260628_rust_ingest_cache128_10k/rust/scale_report.json`
- comparison JSON: `docs/benchmarks/latest_cpp_rust_20260628_rust_ingest_cache128_10k/comparison.json`
- comparison Markdown: `docs/benchmarks/latest_cpp_rust_20260628_rust_ingest_cache128_10k/comparison.md`
