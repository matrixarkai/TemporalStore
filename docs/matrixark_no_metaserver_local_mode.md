# MatrixArk No-Metaserver Local Mode

## Summary

MatrixArk now has an explicit no-metaserver local mode for local development, demos, Codex hooks, and single-user debugging.

This mode is selected with:

```bash
MATRIXARK_LOCAL_MODE=no-metaserver
```

When enabled, the MatrixArk C++ and Rust MCP wrapper scripts skip metaserver probing and skip `deploy_local_ubuntu22.sh` autostart. They start `tools/matrixark_mcp_server.py` with the `temporalstore-local` backend instead.

## Why This Exists

For distributed TemporalStore, C++ direct SDK and Rust direct SDK still use metaserver-backed table discovery, placement, and partition ownership. That remains the right mode for multi-node, Raft, HA, and production throughput tests.

For local mode, a developer should not need a metaserver just to test MatrixArk context ingestion and retrieval. The no-metaserver path gives the same MatrixArk context pipeline semantics using a persistent local record log:

```text
agent / Codex / MCP
-> MatrixArk extraction
-> ContextNode / ContextEvent / ContextEntity / ContextIndex records
-> async ContextSummary and ContextEmbedding records
-> tree-first retrieval
-> ContextPack audit
-> local persistent record log
```

## How To Run

C++ wrapper in no-metaserver mode:

```bash
cd /root/src/github-services/TemporalStore

MATRIXARK_LOCAL_MODE=no-metaserver \
MATRIXARK_TEMPORALSTORE_LOCAL_STORE=/tmp/matrixark-local-cpp.jsonl \
bash tools/matrixark_mcp_cpp_server.sh --line-json
```

Rust wrapper in no-metaserver mode:

```bash
cd /root/src/github-services/TemporalStore

MATRIXARK_LOCAL_MODE=no-metaserver \
MATRIXARK_TEMPORALSTORE_LOCAL_STORE=/tmp/matrixark-local-rust.jsonl \
bash tools/matrixark_mcp_rust_server.sh --line-json
```

Direct MCP server:

```bash
python3 tools/matrixark_mcp_server.py \
  --backend temporalstore-local \
  --local-store /tmp/matrixark-local.jsonl \
  --line-json
```

## What Is Shared With Cluster Mode

The same MatrixArk logic is used:

- ingestion APIs
- session buffering and batch extraction
- event/entity/index records
- dirty summary markers
- async L0/L1 summary refresh
- summary embeddings
- tree-first retrieval
- feedback confirmation
- replay/audit

## What Is Different

| Mode | Needs metaserver | Needs data node | Storage path | Best for |
| --- | ---: | ---: | --- | --- |
| `temporalstore-local` | no | no | local JSONL record log | local dev, Codex hook debugging, demos |
| `temporalstore-direct` | yes | yes | C++ TemporalStore SDK | production-like C++ pipeline tests |
| `temporalstore-rust` | yes | yes | Rust SDK process over TemporalStore | Rust/C++ parity tests |


## Validation Result On 2026-06-23

The local C++ deployment was stopped before validation, and `127.0.0.1:18000` refused connections. The no-metaserver backend still passed the MatrixArk parity flows:

| Test | Backend | Result | Elapsed |
| --- | --- | ---: | ---: |
| backend parity | `local-nometa` | pass | 93.89 ms |
| feature parity | `local-nometa` | pass | 161.81 ms |

Feature parity covered online ingest, async summary refresh, retrieve, feedback confirmation, batch extraction, current-state retrieval, tree traversal with L0/L1 summary embeddings, and replay.

Artifacts:

- `/tmp/matrixark-mcp-backend-parity/matrixark_mcp_backend_parity_local-nometa-20260623-now2.json`
- `/tmp/matrixark-mcp-feature-parity/matrixark_mcp_feature_parity_local-nometa-feature-20260623-now2.json`

## Current Boundary

This change gives MatrixArk a no-metaserver local mode today. It does not yet turn the C++ storage server itself into a standalone single-process database. The C++ SDK currently validates that `metaserver_addr` or `metaserver_consul` is present before opening a table.

The next native-storage step is a true C++ embedded/single-node backend that bootstraps one local partition without metaserver table discovery. The public MatrixArk switch can stay the same: `MATRIXARK_LOCAL_MODE=no-metaserver`.

## Parity Test

Run the no-metaserver backend through the same parity harness:

```bash
cd /root/src/github-services/TemporalStore
PYTHONPATH=. python3 tools/run_matrixark_mcp_backend_parity.py \
  --backends local-nometa \
  --run-id local-nometa-$(date +%Y%m%d_%H%M%S)

PYTHONPATH=. python3 tools/run_matrixark_mcp_feature_parity.py \
  --backends local-nometa \
  --run-id local-nometa-feature-$(date +%Y%m%d_%H%M%S)
```


