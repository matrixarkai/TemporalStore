# MatrixArk Async Storage Multi-Node Benchmark Matrix - 2026-06-23

## Purpose

This run follows the staged plan for high-throughput MatrixArk context ingestion on C++ TemporalStore:

1. async storage plus three data nodes, no data Raft;
2. async storage plus sharded MatrixArk prefixes;
3. async storage plus larger record bundles as the current native-batch stand-in;
4. Raft storage gate after non-Raft throughput is stable.

Async oplog remains the recommended high-throughput context-ingestion setting. Raft is treated as the HA/correctness profile, not the first throughput tuning knob.

Result root: `/tmp/matrixark-async-storage-longmem-q20-3node-`

## Results

| Case | Status | Questions | Turns | Throughput turns/sec | p50 retrieval ms | p95 retrieval ms | Context recall | Judge score | Artifacts |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| longmemeval_s_async_3node | completed | 20 | 10135 | 154.83 | 295.69 | 483.56 | 1.0000 | 0.6000 | `/tmp/matrixark-async-storage-longmem-q20-3node-/longmemeval_s_async_3node` |

## Interpretation

- Multi-node, non-Raft async storage is the next throughput baseline after the single-node async-oplog LOCOMO pass.
- Sharded MatrixArk prefixes reduce hot-prefix pressure and make future partition-aware routing easier.
- The current batch path uses MatrixArk record bundles. A true native server-side batch append remains the next code-level storage improvement.
- Raft should be benchmarked after non-Raft throughput is stable because it answers a different question: HA and correctness under replication.

## Reproduce

```bash
cd <repo>
BUILD_TYPE=Release \
DATASET=longmemeval_s \
DATA_PATH=/root/matrixark_benchmarks/data/longmemeval_s_cleaned_official_hf.json \
QUESTION_LIMIT=20 \
BATCH_SIZE=40 \
RUN_3NODE=1 RUN_SHARDED=0 RUN_BATCH=0 RUN_RAFT=0 \
bash tools/run_matrixark_async_storage_matrix.sh
```
