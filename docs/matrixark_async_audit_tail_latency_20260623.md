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

- `MATRIXARK_DIRECT_AUDIT_BUFFER_MAX_RECORDS`
  - Default: `128`
  - Controls the normal buffered flush batch size.
- `MATRIXARK_DIRECT_AUDIT_FLUSH_INTERVAL_MS`
  - Default: `1000`
  - Controls the background flush cadence.

If the background flusher falls behind, the adapter keeps a bounded pending buffer and drops the oldest excess audit records with a debug-log message instead of blocking retrieval. Enable `MATRIXARK_MCP_DEBUG_LOG` to capture those warnings.

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

