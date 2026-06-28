# MatrixArk Rust Production Backend

MatrixArk keeps Python as the MCP/model glue layer and moves storage-facing work
into the Rust TemporalStore proxy or Rust direct SDK path.

## Current Production Path

The Rust crate exposes one production proxy binary:

- `matrixark_rust_proxy`

MatrixArk should run `matrixark_rust_proxy --serve` for the Rust proxy path, or
use `temporalstore-rust-direct` for the Rust direct SDK bridge. Both avoid the
old process-per-operation CLI path.

```bash
sdk/rust/temporalstore/target/release/matrixark_rust_proxy --serve
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
- ContextEvent, ContextEntity, ContextSummary, ContextIndex, ContextEmbedding,
  ContextPackAudit records
- audit buffering through the MatrixArk adapter
- health/readiness/metrics commands
- graceful shutdown

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

Operational probes:

- `matrixark_backend_ready`: verifies topology and warmup storage.
- `matrixark_backend_metrics`: returns health, readiness, Prometheus text, audit
  buffer stats, and backend config.

## Parity Requirement

Full benchmark parity must use the Rust proxy or Rust direct SDK. The shared
corpus rejects Rust CLI-per-operation mode for full parity. The accepted modes
are `stdio-proxy` and `direct-sdk`; deprecated gateway names are normalized only
as compatibility aliases and should not appear in new reports.
