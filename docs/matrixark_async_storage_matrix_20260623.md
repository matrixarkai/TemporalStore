# MatrixArk Async Storage Multi-Node Benchmark Matrix - 2026-06-23

## Purpose

This run follows the staged plan for high-throughput MatrixArk context ingestion on C++ TemporalStore:

1. async storage plus three data nodes, no data Raft;
2. async storage plus sharded MatrixArk prefixes;
3. async storage plus larger record bundles as the current native-batch stand-in;
4. Raft storage gate after non-Raft throughput is stable.

Async oplog remains the recommended high-throughput context-ingestion setting. Raft is treated as the HA/correctness profile, not the first throughput tuning knob.

Result root: `/tmp/matrixark-async-storage-matrix-20260623_161434`

## Results

| Case | Status | Questions | Turns | Throughput turns/sec | p50 retrieval ms | p95 retrieval ms | Context recall | Judge score | Compression hidden | Artifacts |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| locomo_async_3node | completed | 100 | 5882 | 638.24 | 92.36 | 225.14 | 1.0000 | 0.6200 | 0 | `/tmp/matrixark-async-storage-matrix-20260623_161434/locomo_async_3node` |
| locomo_async_3node_shard0 | completed | 40 | 5882 | 620.14 | 160.63 | 220.92 | 1.0000 | 0.5750 | 0 | `/tmp/matrixark-async-storage-matrix-20260623_161434/locomo_async_3node_shard0` |
| locomo_async_3node_shard1 | completed | 40 | 5882 | 600.14 | 101.07 | 231.25 | 1.0000 | 0.5750 | 0 | `/tmp/matrixark-async-storage-matrix-20260623_161434/locomo_async_3node_shard1` |
| locomo_async_3node_batch_bundle | completed | 100 | 5882 | 566.88 | 97.85 | 233.27 | 1.0000 | 0.6200 | 0 | `/tmp/matrixark-async-storage-matrix-20260623_161434/locomo_async_3node_batch_bundle` |
| raft_storage_gate | not_run_missing_binary |  |  |  |  |  |  |  |  | `/tmp/matrixark-async-storage-matrix-20260623_161434/raft_storage_gate` |

## What This Proves

- The C++ TemporalStore direct SDK path can run MatrixArk LOCOMO extraction, ingestion, retrieval, packing, and audit against a three-data-node async deployment.
- The 3-node async q100 LOCOMO run completed all 5,882 turns with 638.24 turns/sec ingestion throughput, 92.36 ms p50 retrieval, and 225.14 ms p95 retrieval.
- Sharded prefixes completed cleanly in two independent q40 runs. This is the right direction for reducing hot-prefix pressure and making future partition-aware routing straightforward.
- The larger record-bundle case completed q100 and demonstrates the current batch-append stand-in. Native server-side batch append is still the better long-term storage API.
- The Raft gate did not run in this environment because the `replication_smoke_example` binary is missing from the current release build tree. That is a build-artifact gap, not a failed Raft correctness result.

## LongMemEval Follow-Up

The same three-data-node async deployment also completed a LongMemEval_s q20/all-session slice:

| Dataset | Questions | Sessions | Turns | Throughput turns/sec | p50 retrieval ms | p95 retrieval ms | Context recall | Judge score | Compression hidden |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| LongMemEval_s | 20 | 980 | 10135 | 154.83 | 295.69 | 483.56 | 1.0000 | 0.6000 | 0 |

Artifacts are under `/tmp/matrixark-async-storage-longmem-q20-3node-/longmemeval_s_async_3node`. The prior one-node q100 LongMemEval run dropped the local server connection; this q20 result confirms the multi-node async shape is viable and should be scaled next to q50/q100 with prefix sharding and native batch append.

## Next Steps

1. Build or restore `replication_smoke_example` and rerun the Raft storage gate.
2. Add a native MatrixArk append-batch API in C++ so bundles become one server-side append instead of hash-field bundles.
3. Repeat LongMemEval slices with the same 3-node async settings.
4. Add partition-aware prefix sharding once the C++ context APIs expose routing hints.

## Reproduce

```bash
cd /root/src/github-services/TemporalStore
BUILD_TYPE=Release \
DATASET=locomo \
DATA_PATH=/root/matrixark_benchmarks/data/locomo10.json \
QUESTION_LIMIT=100 \
BATCH_SIZE=40 \
SHARD_COUNT=2 SHARD_QUESTION_LIMIT=40 RAFT_OPS=500 RAFT_THREAD_LIST=1 \
RUN_3NODE=1 RUN_SHARDED=1 RUN_BATCH=1 RUN_RAFT=1 \
bash tools/run_matrixark_async_storage_matrix.sh
```
