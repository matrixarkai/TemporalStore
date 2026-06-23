# MatrixArk Async Audit Tail-Latency Fix

Date: 2026-06-23

## Problem

Pool/cache warmup reduced cold `read_all()` cost, but high-concurrency retrieval still showed long-tail latency. The remaining hot-path issue was that every retrieval synchronously wrote a `ContextPackAudit` record through the TemporalStore direct record log. Under C++/Rust direct backends, that makes a read-heavy retrieval request also contend on the write path.

## Fix

Retrieval now calls `append_audit()` instead of `append()` for `context_pack_audit` records.

- Local JSONL adapter: writes audits synchronously, unchanged.
- C++ TemporalStore direct adapter: buffers audit records in memory and flushes them from a background thread.
- Rust TemporalStore adapter: uses the same buffered audit behavior for parity.
- `replay()` calls `flush_audits()` before reading records so debugging can see fresh ContextPack audits.

The retrieval path no longer waits for audit persistence unless an operator explicitly calls replay/audit-style flows that need the latest audit data.

## Runtime Knobs

- `MATRIXARK_DIRECT_AUDIT_MODE`
  - Default: `buffered`
  - Options:
    - `buffered`: background flush audits; best normal serving default.
    - `deferred`: buffer audits but do not background flush; benchmark runners can flush at checkpoints/end.
    - `drop`: do not persist retrieval audits; useful only for raw engine stress isolation.
    - `sync`: write audits inline; useful for debugging old behavior.
- `MATRIXARK_DIRECT_AUDIT_BUFFER_MAX_RECORDS`
  - Default: `128`
  - Controls the normal buffered flush batch size.
- `MATRIXARK_DIRECT_AUDIT_FLUSH_INTERVAL_MS`
  - Default: `1000`
  - Controls the background flush cadence.
- `MATRIXARK_DIRECT_WRITE_RETRIES`
  - Default: `3`
  - Retries transient direct SDK `hset` / `put_string` write failures before surfacing an error.
- `MATRIXARK_DIRECT_WRITE_BACKOFF_MS`
  - Default: `25`
  - Exponential backoff base between direct SDK write retries.
- `MATRIXARK_DIRECT_WRITE_THROTTLE_MS`
  - Default: `0`
  - Optional tiny sleep after each direct SDK write. Use this for single-node benchmark load shaping if warning-log or write-queue saturation appears.

If the background flusher falls behind, the adapter keeps a bounded pending buffer and drops the oldest excess audit records with a debug-log message instead of blocking retrieval. Enable `MATRIXARK_MCP_DEBUG_LOG` to capture those warnings.

## Record Bundling Side Effects

The direct record log batches multiple MatrixArk logical records into one TemporalStore hash field using:

```json
{"record_bundle": [ ...records... ]}
```

This reduces `hset` pressure. The important semantics are:

- `record_count` counts physical log entries, not logical MatrixArk records.
- `read_all()` expands `record_bundle` entries, so callers still see the same logical records in order.
- Appends update the in-process cache only after the bundle write and count write succeed.
- A crash after bundle `hset` but before `record_count` update can leave an unreferenced physical field. That is acceptable for the MVP append log because readers only trust `record_count`; a later native append-log API should make this atomic.
- Bundling is not a substitute for native multi-field writes; it is a Python-side pressure reducer until the C++/Rust SDK exposes a dedicated batch append path.

## Expected Impact

This removes C++ direct `hset`/record-log audit contention from the normal retrieval latency path. It should improve high-concurrency p95/p99 retrieval behavior for MatrixArk benchmarks where retrieval already has a warmed record cache.

This does not yet solve:

- OSS model/scoring contention.
- Native C++ `ContextIndex` lookup pushdown.
- Native C++ layer-by-layer ContextNode traversal pushdown.
- Raw C++ storage-engine microbenchmark isolation.

## Next Backend Work

The next production fix should push more retrieval work into native C++ APIs:

1. Query `ContextIndex` in C++ before Python candidate scoring.
2. Traverse ContextNode children and L0/L1 embeddings in C++.
3. Return bounded candidate refs to Python for packing/reader logic.
4. Keep ContextPackAudit buffered or move it to a dedicated append-only audit stream.
5. Add native atomic batch append for MatrixArk record bundles so `hset` and `record_count` advance as one server-side operation.
