# MatrixArk C++ Storage-Backed Validation - 2026-06-21

## Purpose

Validate that MatrixArk context extraction, ingestion, retrieval, feedback memory,
and replayable record mapping can run against real C++ TemporalStore storage,
not only the local JSONL adapter.

## What Changed

- `tools/matrixark_mcp_server.py` now supports:
  - `--backend local`
  - `--backend temporalstore-direct`
- `temporalstore-direct` uses the native Python SDK over `libbcache2.so`.
- MatrixArk records are persisted into TemporalStore as:
  - hash key: `<storage_prefix>:records`
  - field: ordered record id
  - value: compact JSON MatrixArk record
  - index key: `<storage_prefix>:record_index`
- The existing MatrixArk pipeline remains unchanged:
  - message normalization
  - prior context lookup
  - extraction/classification
  - context node path mapping
  - L0 summary records
  - embedding records
  - context event records
  - retrieval scoring
  - ContextPack audit records
  - feedback confirmation detection

## Local C++ Deployment

Started a real local C++ TemporalStore deployment:

```bash
BUILD_TYPE=Release \
OUT_DIR=$PWD/output-ubuntu22/release \
DEPLOY_DIR=/tmp/matrixark-cpp-direct-deploy \
CLUSTER_NAME=matrixarkcppdirect \
NAMESPACE_NAME=deploy_ns \
TABLE_NAME=deploy_table \
MS_PORT=18000 \
MS_RAFT_PORT=18010 \
MS_SNAPSHOT_PORT=18020 \
SERVER_PORT=18001 \
./tools/deploy_local_ubuntu22.sh start
```

Result:

```text
TemporalStore local deployment is running
cluster: matrixarkcppdirect
metaserver leader: 127.0.0.1:18000
server1: 127.0.0.1:18001
```

## E2E Pipeline Command

```bash
PYTHONPATH=$PWD/sdk/python \
TEMPORALSTORE_LIB=$PWD/output-ubuntu22/release/sdk/lib/libbcache2.so \
python3 tools/run_matrixark_temporalstore_direct_e2e.py \
  --metaserver 127.0.0.1:18000 \
  --namespace deploy_ns \
  --table deploy_table \
  --temporalstore-lib $PWD/output-ubuntu22/release/sdk/lib/libbcache2.so \
  --report-json /tmp/matrixark_cpp_direct_e2e_after_patch.json
```

Result:

```json
{
  "backend": "temporalstore-direct",
  "status": "passed",
  "stored_record_count": 32,
  "first_retrieve_selected": 2,
  "second_retrieve_selected": 3,
  "ingest_classifications": ["NEW_EVENT", "NEW_EVENT", "CONFIRMATION"],
  "feedback_prior_context": "explicit",
  "feedback_prior_refs": 2
}
```

This proves:

- MatrixArk ingestion writes ContextSummary, ContextEmbedding, ContextEvent,
  ContextPackAudit, and feedback records through C++ TemporalStore.
- Retrieval returns context from the MatrixArk pipeline after records have been
  persisted through the native C++ SDK.
- Feedback confirmation uses prior ContextPack refs and is stored back into
  TemporalStore.

## Storage-Backed Benchmark Command

The benchmark runner supports local JSONL and C++ direct storage:

```bash
PYTHONPATH=$PWD/sdk/python \
TEMPORALSTORE_LIB=$PWD/output-ubuntu22/release/sdk/lib/libbcache2.so \
python3 tools/run_matrixark_context_storage_benchmark.py \
  --backend temporalstore-direct \
  --events 5 \
  --queries 3 \
  --restart-before-query \
  --metaserver 127.0.0.1:18000 \
  --namespace deploy_ns \
  --table deploy_table \
  --temporalstore-lib $PWD/output-ubuntu22/release/sdk/lib/libbcache2.so \
  --report-json /tmp/matrixark_cpp_direct_storage_bench_small.json
```

Result:

```json
{
  "backend": "temporalstore-direct",
  "status": "passed",
  "events": 5,
  "queries": 3,
  "hit_rate": 1.0,
  "restart_before_query": true,
  "ingest_latency_ms": {
    "avg": 174.696,
    "p50": 88.661,
    "p95": 537.779
  },
  "retrieve_latency_ms": {
    "avg": 220.531,
    "p50": 6.396,
    "p95": 648.917
  }
}
```

`--restart-before-query` is important: it kills the ingest MCP process and starts
a fresh retrieval MCP process with the same storage prefix, forcing query-time
loading from C++ TemporalStore instead of process memory.

## Local Baseline

```bash
python3 tools/run_matrixark_context_storage_benchmark.py \
  --backend local \
  --events 60 \
  --queries 15 \
  --report-json /tmp/matrixark_local_storage_bench.json
```

Result:

```json
{
  "backend": "local",
  "status": "passed",
  "events": 60,
  "queries": 15,
  "hit_rate": 1.0,
  "ingest_latency_ms": {
    "avg": 8.195,
    "p50": 6.285,
    "p95": 8.9
  },
  "retrieve_latency_ms": {
    "avg": 8.795,
    "p50": 8.568,
    "p95": 10.23
  }
}
```

## Current Limitation

The C++ direct SDK path is now functionally proven, but scale is not yet clean on
this Windows-mounted WSL setup.

Attempts at 20-60 generated MatrixArk events hit:

```text
Internal: Request server failed[E1008]Reached timeout=20000ms @127.0.0.1:18001
```

This is now isolated as a C++ direct SDK/server stability or local deployment
performance issue under repeated MatrixArk record writes, not a MatrixArk
mapping issue. The same MatrixArk workload passes locally, and the C++ direct
E2E plus small restart-backed benchmark pass.

## Proxy/Gateway Note

The repo has:

- native direct SDK path: working for this validation
- internal `bcache2-proxy`: brpc/Thrift proxy
- mock HTTP proxy: SDK contract testing only

A production MatrixArk HTTP/JSON gateway backed by the live C++ proxy is still
not present in this repo. Until that gateway exists, true MatrixArk C++ storage
validation should use `--backend temporalstore-direct`, while proxy parity should
continue to be tested with the existing C++ proxy transport tests.

## Next Fixes

- Add a live MatrixArk HTTP/JSON gateway that maps MatrixArk record operations to
  C++ TemporalStore instead of using the mock proxy.
- Investigate direct SDK timeout under 20-60 MatrixArk events.
- Move large-scale benchmark runs to a native Linux filesystem rather than
  `/mnt/c`.
- Add a CI gate for:
  - MatrixArk local backend
  - MatrixArk temporalstore-direct backend
  - C++ proxy transport parity
  - restart-before-query storage reload
