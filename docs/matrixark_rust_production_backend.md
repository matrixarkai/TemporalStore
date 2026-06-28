# MatrixArk Rust Production Backend

MatrixArk keeps Python as the MCP/model glue layer and moves storage-facing work
into the Rust TemporalStore proxy or Rust direct SDK path.

## Current Production Path

The Rust crate exposes two production-facing MatrixArk binaries:

- `matrixark_rust_proxy`
- `matrixark_rust_direct_sdk`

MatrixArk should run `matrixark_rust_proxy --serve` for the Rust proxy path, or
use `temporalstore-rust-direct` for the Rust direct SDK bridge. Both avoid the
old process-per-operation CLI path.

```bash
sdk/rust/temporalstore/target/release/matrixark_rust_proxy --serve
sdk/rust/temporalstore/target/release/matrixark_rust_direct_sdk --serve
```

Python MCP owns:

- MCP protocol
- access management
- model/provider calls
- extraction and packing orchestration

Rust owns:

- storage-facing record writes
- batch record writes
- prefix/count record reads and scans
- native MatrixArk candidate scan with secondary-index prefiltering
- native MatrixArk ContextPack scoring and budget assembly
- ContextEvent, ContextEntity, ContextSummary, ContextIndex, ContextEmbedding,
  ContextPackAudit records
- audit buffering through the MatrixArk adapter
- health/readiness/metrics commands
- graceful shutdown

## Proxy Lane Pool

MatrixArk no longer serializes all Rust proxy calls through one stdio client.
The MCP adapter keeps a small proxy/client pool with separate lanes so writes,
reads, and native ContextPack retrieval cannot block each other behind one
`BoundedSemaphore(1)`.

Default lane widths:

- `MATRIXARK_RUST_PROXY_WRITE_WORKERS=2`
- `MATRIXARK_RUST_PROXY_READ_WORKERS=4`
- `MATRIXARK_RUST_PROXY_RETRIEVE_WORKERS=4`
- `MATRIXARK_RUST_PROXY_CONTROL_WORKERS=1`

The metrics snapshot exposes `lane_worker_counts`, `lane_metrics`,
`write_lane_workers`, `read_lane_workers`, and `retrieve_lane_workers` so scale
reports can distinguish storage pressure from native retrieve-pack pressure.
Each lane reports queue wait time, while native responses also expose Rust
engine time and response serialization time. Retrieval responses additionally
report scan count, cache hit state, selected refs, and dropped refs.

## Proxy Commands

The Rust proxy supports:

- `health`
- `readiness`
- `shutdown`
- `metrics_prometheus`
- `put_string`
- `get_string`
- `hset`
- `hget`
- `batch_hset`
- `batch_hget`
- `write_matrixark_record`
- `write_matrixark_records`
- `read_matrixark_record`
- `read_matrixark_records`
- `matrixark_batch_append_records`
- `matrixark_scan_candidates`
- `matrixark_retrieve_context_pack`

`matrixark_retrieve_context_pack` is the production hot path for Rust-backed
serving: Python sends one request and receives a finished `ContextPack` plus
telemetry. Python should not materialize thousands of raw records when this
native path is available.

## MatrixArk MCP Integration

Start MatrixArk with the Rust backend:

```bash
python3 tools/matrixark_mcp_server.py \
  --line-json \
  --backend temporalstore-rust \
  --rust-proxy sdk/rust/temporalstore/target/release/matrixark_rust_proxy \
  --metaserver 127.0.0.1:18000 \
  --namespace deploy_ns \
  --table deploy_table \
  --storage-prefix matrixark:mcp
```

For the Rust direct SDK parity path:

```bash
python3 tools/matrixark_mcp_server.py \
  --line-json \
  --backend temporalstore-rust-direct \
  --rust-direct-sdk sdk/rust/temporalstore/target/release/matrixark_rust_direct_sdk \
  --metaserver 127.0.0.1:18000 \
  --namespace deploy_ns \
  --table deploy_table \
  --storage-prefix matrixark:mcp
```

Operational probes:

- `matrixark_backend_ready`: verifies topology and warmup storage.
- `matrixark_backend_metrics`: returns health, readiness, Prometheus text, audit
  buffer stats, and backend config.

## Parity Requirement

Full benchmark parity must use the Rust proxy or Rust direct SDK bridge. The
shared corpus rejects Rust CLI-per-operation mode for full parity. The accepted
production-facing binaries are `matrixark_rust_proxy` and
`matrixark_rust_direct_sdk`; `matrixark_record_log` is retained only as a
compatibility/debug wrapper and should not appear in new production reports.
