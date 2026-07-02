# MatrixArk C++ vs Rust Scale Reports

MatrixArk now has a canonical scale comparison runner for C++ and Rust TemporalStore backends. The runner executes the same synthetic MatrixArk context workload against both backends with identical config, then writes side-by-side reports.

## Command

```bash
python3 third_party/TemporalStoreTestCorpus/tools/run_matrixark_cpp_rust_scale_report.py \
  --consumer-repo <repo> \
  --artifact-dir /tmp/matrixark-cpp-rust-scale \
  --events 120 \
  --queries 30 \
  --ingest-mode batch \
  --batch-size 20 \
  --metaserver 127.0.0.1:18000 \
  --namespace deploy_ns \
  --table deploy_table \
  --storage-mode shared_store \
  --oplog-mode async \
  --replication-mode shared_store \
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
- same-config checks for `storage_options`: storage mode, oplog mode, replication mode, Raft flag, and consistency

## Single Backend Source Report

`run_matrixark_context_storage_benchmark.py` now supports:

- `--backend temporalstore-direct` for C++
- `--backend temporalstore-rust` for Rust

Its source report now includes p99 latency, QPS, error count, timeout count, namespace/table, and request/io timeouts so comparison reports do not need to infer those fields.

## Storage Mode Parameters

MatrixArk API calls and the scale runners accept request-level storage hints:

```json
{
  "storage_options": {
    "storage_mode": "raft",
    "oplog_mode": "async",
    "replication_mode": "raft",
    "raft_mode": true,
    "consistency": "linearizable"
  }
}
```

For CLI scale reports, use:

```bash
--storage-mode raft --oplog-mode async --replication-mode raft --raft-mode --consistency linearizable
```

These are recorded in MatrixArk events, resource import tasks, retrieval policies, and audit records. The actual native C++/Rust data-node topology still must be deployed in the matching mode; request-level hints make the desired mode explicit for routing, audit, replay, and parity reporting.
