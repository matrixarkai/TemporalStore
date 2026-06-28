# MatrixArk MCP C++ / Rust TemporalStore Backend Parity

MatrixArk MCP now has three storage backends for the same public tools:

- `local`: JSONL debug backend.
- `temporalstore-direct`: C++ TemporalStore direct SDK backend, launched by `tools/matrixark_mcp_cpp_server.sh`.
- `temporalstore-rust`: Rust TemporalStore direct SDK backend, launched by `tools/matrixark_mcp_rust_server.sh`.

The MatrixArk extraction, ingestion, retrieval, summary refresh, and context-pack code is shared. Only the record-log storage boundary changes.

## Rust Path

The Rust production path uses the existing long-lived Rust proxy boundary. The production-facing binary name is `matrixark_rust_proxy --serve`. The legacy `matrixark_record_log` name remains a compatibility/debug alias, but MatrixArk should treat the runtime as the Rust proxy, not as a process-per-operation CLI. Single-shot mode remains debug-only. Rust direct SDK parity is still useful for a future embedded/local optimization, but production and benchmark paths should prefer the proxy first:

```bash
sdk/rust/temporalstore/target/release/matrixark_rust_proxy
sdk/rust/temporalstore/target/release/matrixark_rust_proxy --serve
```

Supported operations:

- `put_string`
- `get_string`
- `hset`
- `hget`

The MCP server calls `matrixark_rust_proxy --serve` through `MatrixArkRustProxyClient`, keeping one Rust process alive and reusing the Rust SDK client across storage operations. `MatrixArkRustCliClient` remains only a compatibility alias. New configuration should use `MATRIXARK_TEMPORALSTORE_RUST_PROXY` or `--rust-proxy`; `MATRIXARK_TEMPORALSTORE_RUST_CLI` and `--rust-cli` are compatibility/debug names. This gives Rust backend parity without process-per-operation latency.

## Launchers

C++ backend:

```bash
bash tools/matrixark_mcp_cpp_server.sh --line-json
```

Rust backend:

```bash
MATRIXARK_TEMPORALSTORE_RUST_PROXY=/path/to/matrixark_rust_proxy \
  bash tools/matrixark_mcp_rust_server.sh --line-json
```

Both launchers default to:

- metaserver: `127.0.0.1:18000`
- namespace: `deploy_ns`
- table: `deploy_table`
- auto-start local TemporalStore deployment if the metaserver is not listening

## Parity Test

Run the same MCP ingest / summary refresh / retrieve flow across all backends:

```bash
python3 tools/run_matrixark_mcp_backend_parity.py --backends local cpp rust
```

Run only C++ and Rust:

```bash
python3 tools/run_matrixark_mcp_backend_parity.py --backends cpp rust
```

Reports are written to:

```text
/tmp/matrixark-mcp-backend-parity/
```

Each backend gets an isolated `MATRIXARK_TEMPORALSTORE_PREFIX`, so repeated runs do not collide.

## Codex MCP Config

Keep the existing `matrixark` MCP server on C++ as the default. Add Rust as a separate server only when you want to test it:

```toml
[mcp_servers.matrixark_rust]
command = "wsl.exe"
args = ["--cd", "<repo>", "-e", "bash", "-lc", "exec tools/matrixark_mcp_rust_server.sh"]
startup_timeout_sec = 180
enabled = false
```

Set `enabled = true` when actively testing Rust. Keeping it disabled by default avoids duplicate MatrixArk tool names in Codex while the C++ backend remains the production MCP path.

## Why This Matters

This gives MatrixArk one MCP API surface with backend parity:

```text
Codex / agent
-> MatrixArk MCP tools
-> shared MatrixArk extraction + retrieval pipeline
-> C++ TemporalStore or Rust TemporalStore storage backend
```

The test goal is not different behavior per language. C++ and Rust should produce the same logical ContextEvents, ContextEntities, summaries, indexes, and ContextPacks for the same inputs.
