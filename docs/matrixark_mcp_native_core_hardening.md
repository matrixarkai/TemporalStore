# MatrixArk MCP Native-Core Hardening

## Current Split

MatrixArk MCP keeps Python for the agent-facing protocol and model ecosystem:

- MCP JSON-RPC framing and tool schemas.
- OSS/OpenAI model provider glue.
- Resource and skill parsers that depend on Python document libraries.
- Small orchestration policies such as access checks and request envelopes.

Performance-critical storage and serving paths must use native backends in
benchmark and production profiles:

- C++ TemporalStore direct SDK through `--backend temporalstore-direct`.
- Rust TemporalStore Rust proxy through `--backend temporalstore-rust`.
- Native record writes, batch writes, hash/prefix scans, backend readiness,
  ContextPack audit storage, and backend metrics.

The Python `local` and `temporalstore-local` adapters are debug-only in
production/benchmark profiles unless `MATRIXARK_ALLOW_LOCAL_BACKEND=1` is set.
Production does not need the local JSONL `record_log`: TemporalStore already
has the oplog/write path, MatrixArk ContextPack audit, and replay records. Keep
local record-log writes only for offline debugging, deterministic CI fixtures,
and tiny demos.

## Production Guard

Set:

```bash
export MATRIXARK_MCP_PROFILE=production
export MATRIXARK_MCP_BACKEND=temporalstore-rust   # or temporalstore-direct
```

In this profile, `matrixark_mcp_server.py` refuses local JSONL storage. It also
runs backend readiness before serving for C++ and Rust backends, so ingestion and
retrieval cannot silently start on an unhealthy native store.
If `MATRIXARK_MCP_BACKEND` is omitted in this profile, the server and hooks
default to `temporalstore-direct` instead of `local`; set
`MATRIXARK_ALLOW_LOCAL_BACKEND=1` only when deliberately debugging local JSONL.

For benchmark/parity runs, use:

```bash
export MATRIXARK_MCP_PROFILE=benchmark
```

This applies the same native-backend policy while keeping the benchmark wording
separate from production-performance parity claims.

## Native Paths Already Covered

Rust:

- `matrixark_rust_proxy --serve` is the long-lived production path.
- `matrixark_record_log --serve` is retained only as a compatibility/debug alias.
- Single-shot record-log CLI is debug-only.
- Batch append/read: `batch_hset` / `batch_hget`.
- Prefix/hash scan fast path: `scan_hash` / `hgetall`.
- Health/readiness/metrics: `health`, `readiness`, `metrics_prometheus`.
- Graceful shutdown clears cached engines.
- Connection/client pooling: cached `TemporalEngine` instances by record-log root.

C++:

- Direct SDK adapter is the production baseline for MatrixArk storage-backed
  context pipelines.
- Backend readiness validates namespace/table/slot warmup before ingestion.

## Remaining Native-Core Migration Seams

The next performance-critical pieces to move from Python orchestration into C++
and Rust native services are:

- Tree traversal with L0/L1 summary scoring and secondary-index prefiltering.
- ContextPack assembly and dropped-ref audit construction.
- Async audit buffering and flush scheduling.
- Resource and skill registry scans.
- Native sparse/BM25 index once the shared corpus requires it.

Python should keep model calls and document parsing, but native backends should
own durable record encoding, batch writes, prefix scans, secondary-index
filtering, traversal, and high-QPS audit paths.
