# MatrixArk Event Ingestion Scale Test - 2026-06-26

## Summary

Generated event ingestion was tested at 1K, 10K, and 100K requested events using the MatrixArk MCP pipeline and the shared `run_matrixark_context_storage_benchmark.py` runner.

This run used the local JSONL backend to isolate MatrixArk Python ingestion/extraction behavior from live C++/Rust topology issues.

Configuration:

- backend: `local`
- ingest mode: `batch`
- batch size: `100` for 1K and 10K
- prior-context lookup: disabled with `--skip-prior-context`
- storage options: `storage_mode=local`, `oplog_mode=async`, `replication_mode=none`
- retrieval check: `1` query after ingestion

## Results

| Requested events | Status | Messages ingested | Message QPS | Ingest avg ms/batch | Ingest p50 ms | Ingest p95 ms | Ingest p99 ms | Retrieval check |
| ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| 1,000 | passed | 1,000 | 451.94 | 221.076 | 155.765 | 757.266 | 757.266 | passed, 54.981 ms |
| 10,000 | passed | 10,000 | 41.329 | 2419.453 | 1791.531 | 9057.143 | 11402.225 | passed, 727.325 ms |
| 100,000 | timed out | not completed | n/a | n/a | n/a | n/a | n/a | not reached |

## 100K Finding

The 100K run did not complete within a 15-minute outer command timeout.

Two 100K attempts were made:

1. `batch_size=100`, `--skip-prior-context`
2. `batch_size=1000`, `--skip-prior-context`

Both exceeded the timeout before writing a final report. No logical MatrixArk error was returned before termination; the bottleneck is throughput/tail behavior in the local Python/JSONL ingestion path for very large generated event volumes.

This should not be treated as a C++ or Rust TemporalStore storage-engine result. It is a local backend scale limit for the current MatrixArk MCP Python path.

## Commands

1K:

```bash
TEMPORALSTORE_CONSUMER_REPO=/root/src/github-services/TemporalStore \
python3 third_party/TemporalStoreTestCorpus/tools/run_matrixark_context_storage_benchmark.py \
  --backend local \
  --events 1000 \
  --queries 1 \
  --ingest-mode batch \
  --batch-size 100 \
  --skip-prior-context \
  --storage-mode local \
  --oplog-mode async \
  --replication-mode none \
  --report-json docs/benchmarks/matrixark_ingestion_scale_20260626_skip_prior/matrixark_ingestion_1000.json
```

10K:

```bash
TEMPORALSTORE_CONSUMER_REPO=/root/src/github-services/TemporalStore \
python3 third_party/TemporalStoreTestCorpus/tools/run_matrixark_context_storage_benchmark.py \
  --backend local \
  --events 10000 \
  --queries 1 \
  --ingest-mode batch \
  --batch-size 100 \
  --skip-prior-context \
  --storage-mode local \
  --oplog-mode async \
  --replication-mode none \
  --report-json docs/benchmarks/matrixark_ingestion_scale_20260626_skip_prior/matrixark_ingestion_10000.json
```

100K attempted:

```bash
TEMPORALSTORE_CONSUMER_REPO=/root/src/github-services/TemporalStore \
python3 third_party/TemporalStoreTestCorpus/tools/run_matrixark_context_storage_benchmark.py \
  --backend local \
  --events 100000 \
  --queries 1 \
  --ingest-mode batch \
  --batch-size 1000 \
  --skip-prior-context \
  --storage-mode local \
  --oplog-mode async \
  --replication-mode none \
  --report-json docs/benchmarks/matrixark_ingestion_scale_20260626_skip_prior/matrixark_ingestion_100000_batch1000.json
```

## Artifacts

- `docs/benchmarks/matrixark_ingestion_scale_20260626_skip_prior/matrixark_ingestion_1000.json`
- `docs/benchmarks/matrixark_ingestion_scale_20260626_skip_prior/matrixark_ingestion_1000.stdout`
- `docs/benchmarks/matrixark_ingestion_scale_20260626_skip_prior/matrixark_ingestion_10000.json`
- `docs/benchmarks/matrixark_ingestion_scale_20260626_skip_prior/matrixark_ingestion_10000.stdout`
- `docs/benchmarks/matrixark_ingestion_scale_20260626_skip_prior/matrixark_ingestion_100000.stdout`
- `docs/benchmarks/matrixark_ingestion_scale_20260626_skip_prior/matrixark_ingestion_100000_batch1000.stdout`

## Next Fixes

The 100K timeout points to the next production-readiness work:

1. Run the same 1K/10K/100K sweep on the C++ and Rust backends using native append paths instead of the local JSONL backend.
2. Add an ingestion-only runner mode that does not perform a retrieval check or final full-log reload.
3. Add a native batch append fast path for MatrixArk event, embedding, index, summary-dirty, and audit records.
4. Add streaming progress metrics for long ingestion runs so a timed-out benchmark still records partial progress.
5. Keep `--skip-prior-context` for pure write-scale tests; run separate quality tests with prior context enabled.
