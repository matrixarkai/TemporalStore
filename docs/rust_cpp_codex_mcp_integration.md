# Rust/C++ Codex MCP Integration

This repo now uses the same MatrixArk MCP server shape as the C++ TemporalStore
thread. The server remains the C++ MatrixArk MCP implementation, while backend
selection chooses which TemporalStore record-log adapter owns persistence.

## Backends

| Backend | TemporalStore path | Purpose |
| --- | --- | --- |
| `temporalstore-direct` | C++ TemporalStore SDK | C++ thread compatibility path. |
| `temporalstore-rust` | Long-lived Rust `matrixark_record_log --serve` gateway/proxy | Rust-native Codex integration path. |
| `temporalstore-rust-direct` | Long-lived Rust direct SDK bridge | Embedded/local Rust parity path matching the C++ direct SDK contract. |
| `local` | JSONL file | Local diagnostic path only. |

Rust does not add brpc or thrift. The Rust production/migration contract remains
Rust-native MCP plus HTTP/JSON, RESP, and tonic surfaces.

## Codex Desktop Launch

Use the Rust repo launcher so Codex gets the same MCP server and both backend
paths:

```bash
cd <repo-root>
MATRIXARK_MCP_BACKEND=temporalstore-rust \
MATRIXARK_CPP_TEMPORALSTORE_REPO=<cpp-temporalstore-checkout> \
tools/run_matrixark_mcp_server.sh
```

For the C++ path, switch only the backend:

```bash
MATRIXARK_MCP_BACKEND=temporalstore-direct \
MATRIXARK_CPP_TEMPORALSTORE_REPO=<cpp-temporalstore-checkout> \
tools/run_matrixark_mcp_server.sh \
  --metaserver 127.0.0.1:18000 \
  --namespace deploy_ns \
  --table deploy_table
```

The Rust launcher builds `matrixark_record_log` when needed and passes its path
through `MATRIXARK_TEMPORALSTORE_RUST_CLI`, which is what the shared MCP server
expects for the `temporalstore-rust` and `temporalstore-rust-direct` adapters.
Use `temporalstore-rust` for the production gateway/proxy path. Use
`temporalstore-rust-direct` when validating embedded/local parity with the C++
direct SDK path.

Optional Rust storage root:

```bash
MATRIXARK_TEMPORALSTORE_RUST_ROOT=/tmp/matrixark-rust-codex
```

If unset, the Rust CLI derives a stable local storage root from metaserver,
namespace, and table. That keeps repeated MCP calls durable across processes.

## Tool Parity

The shared server exposes the same Codex-facing tool names for both C++ and Rust:

- `matrixark_ingest`
- `matrixark_session_commit`
- `matrixark_refresh_summaries`
- `matrixark_retrieve`
- `matrixark_batch_extract`
- `matrixark_feedback`
- `matrixark_replay`
- MatrixArk admin account/user/API-key tools

The Rust backend supplies the C++ server's required record-log operations:

- `put_string`
- `get_string`
- `hset`
- `hget`

Those operations are executed through Rust `TemporalEngine` string/hash commands
and durable local index/page/oplog persistence, so Codex ingestion and retrieval
exercise Rust TemporalStore storage instead of a Python-only side log.

The Rust CLI also exposes production diagnostics and replay helpers used by
MatrixArk MCP readiness checks:

- `health` / `preflight`: open the durable Rust engine and return the selected
  storage root.
- `delete` / `del`: remove a record-log key.
- `hdel`: remove one hash field.
- `hgetall` / `scan_hash`: return all hash fields as deterministic JSON plus an
  `entries` object and `count`.

Every response keeps the shared-server-compatible `ok`, `value`, and `error`
fields, and additionally emits `op`, `root`, `entries`, and `count` when
available. Required fields are validated before the engine is opened, so bad MCP
calls fail closed with a structured error instead of creating partial state.

## Validation

Fast local checks:

```bash
cargo test -p temporalstore-rust --bin matrixark_record_log -- --test-threads=1
python3 tools/validate_codex_mcp_parity.py
python3 tools/run_temporalstore_unified_tests.py --validate-only
```

Direct Rust CLI smoke:

```bash
printf '%s' '{"op":"health","namespace":"deploy_ns","table":"deploy_table"}' |
  cargo run -q -p temporalstore-rust --bin matrixark_record_log
printf '%s' '{"op":"hgetall","key":"matrixark:mcp:records"}' |
  MATRIXARK_TEMPORALSTORE_RUST_ROOT=/tmp/matrixark-rust-codex \
  cargo run -q -p temporalstore-rust --bin matrixark_record_log
```

Manual JSON-RPC smoke:

```bash
printf '%s\n' \
  '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-06-18"}}' \
  '{"jsonrpc":"2.0","id":2,"method":"tools/list","params":{}}' |
  MATRIXARK_MCP_BACKEND=temporalstore-rust tools/run_matrixark_mcp_server.sh --line-json
```

Rust direct SDK bridge smoke:

```bash
MATRIXARK_MCP_BACKEND=temporalstore-rust-direct \
MATRIXARK_CPP_TEMPORALSTORE_REPO=<cpp-temporalstore-checkout> \
tools/run_matrixark_mcp_server.sh \
  --metaserver 127.0.0.1:18000 \
  --namespace deploy_ns \
  --table deploy_table \
  --line-json
```

Expected result:

- `initialize` returns `serverInfo.name = matrixark-context`.
- `tools/list` includes the MatrixArk context tools listed above.
- The Rust backend fails closed if `matrixark_record_log` cannot be built or
  launched.
- In `temporalstore-rust-direct` mode, backend metrics report
  `sdk_mode=direct_sdk`, `storage_mode=rust-direct-sdk-bridge`, and
  `matrixark_append_write_path=rust_direct_sdk_matrixark_batch_append_records`.

## Parity Boundary

This integration proves Codex can use the same MCP protocol/tool surface against
both codebases. It does not claim internal binary layout parity, brpc/thrift wire
compatibility, or live ByteStore/S3 integration for Rust.
