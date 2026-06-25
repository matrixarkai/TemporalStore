# MatrixArk Rust Production Backend

MatrixArk keeps Python as the MCP/model glue layer and moves storage-facing work
into the Rust TemporalStore gateway.

## Current Production Path

The Rust crate builds two binary names over the same implementation:

- `matrixark_gateway`
- `matrixark_record_log`

MatrixArk should run either binary in `--serve` mode. This creates one
long-lived Rust process, one cached TemporalStore client per backend config, and
JSON-line commands over stdin/stdout. This is not the old process-per-operation
path.

```bash
sdk/rust/temporalstore/target/release/matrixark_gateway --serve
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

## Gateway Commands

The long-lived gateway supports:

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
  --rust-cli sdk/rust/temporalstore/target/release/matrixark_gateway \
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

Full benchmark parity must use a long-lived Rust gateway/binding. The shared
corpus rejects Rust CLI-per-operation mode for full parity. The current accepted
mode is `stdio-gateway`; a native HTTP or in-process binding can replace it later
without changing MatrixArk's logical benchmark contract.
