# MatrixArk C++ vs Rust Scale Reports

MatrixArk now has a canonical scale comparison runner for C++ and Rust TemporalStore backends. The runner executes the same synthetic MatrixArk context workload against both backends with identical config, then writes side-by-side reports.

## Command

```bash
python3 third_party/TemporalStoreTestCorpus/tools/run_matrixark_cpp_rust_scale_report.py \
  --consumer-repo /root/src/github-services/TemporalStore \
  --artifact-dir /tmp/matrixark-cpp-rust-scale \
  --events 120 \
  --queries 30 \
  --ingest-mode batch \
  --batch-size 20 \
  --metaserver 127.0.0.1:18000 \
  --namespace deploy_ns \
  --table deploy_table \
  --request-timeout-ms 60000 \
  --io-timeout-ms 60000
```

Use `--allow-failures` when collecting diagnostic reports from an unstable live topology. The command still marks the comparison failed, but writes the artifacts.

## Artifacts

The runner writes:

- `cpp/scale_report.json`
- `cpp/command.json`
- `rust/scale_report.json`
- `rust/command.json`
- `comparison.json`
- `comparison.md`

## Compared Metrics

The comparison includes:

- message ingest QPS
- retrieval QPS
- hit rate
- errors and timeouts
- ingest p50/p95/p99 latency
- retrieval p50/p95/p99 latency
- same-config checks for events, queries, ingest mode, batch size, namespace, table, and timeouts

## Single Backend Source Report

`run_matrixark_context_storage_benchmark.py` now supports:

- `--backend temporalstore-direct` for C++
- `--backend temporalstore-rust` for Rust

Its source report now includes p99 latency, QPS, error count, timeout count, namespace/table, and request/io timeouts so comparison reports do not need to infer those fields.
