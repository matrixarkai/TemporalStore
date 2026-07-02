# MatrixArk C++ / Rust Context Pipeline Parity - 2026-06-23

## Summary

MatrixArk context pipeline parity was revalidated against both TemporalStore backends:

- C++ TemporalStore local deployment through the MatrixArk C++ MCP/backend path.
- Rust TemporalStore `matrixark_record_log --serve` backend through the MatrixArk Rust MCP/backend path.

Both backends passed the same online and batch context pipeline checks. This validates functional parity for the current MatrixArk context MVP surface: ingestion, extraction classification, ContextNode materialization, ContextEvent writes, async ContextSummary refresh, L0/L1 summary embeddings for tree traversal, retrieval, feedback confirmation, replay, and batch extraction.

## Environment

- Repo: `<repo>`
- Branch: `main`
- C++ deployment: `127.0.0.1:18000` metaserver and `127.0.0.1:18001` server
- Rust binary: `<repo>/sdk/rust/temporalstore/target/release/matrixark_record_log`
- Run date: `2026-06-23`

## Commands

```bash
cd <repo>

BUILD_TYPE=Release \
TEMPORALSTORE_STORAGE_ASYNC=true \
TEMPORALSTORE_STORAGE_OPLOG_DELAY_DUMP_LENGTH=10000 \
SERVER_COUNT=1 \
REPLICA_COUNT=1 \
bash tools/deploy_local_ubuntu22.sh start

PYTHONPATH=. python3 tools/run_matrixark_mcp_backend_parity.py \
  --backends cpp rust \
  --run-id cpp-rust-parity-20260623-now

PYTHONPATH=. python3 tools/run_matrixark_mcp_feature_parity.py \
  --backends cpp rust \
  --run-id cpp-rust-feature-20260623-now
```

## Backend Parity Result

Report artifacts:

- JSON: `/tmp/matrixark-mcp-backend-parity/matrixark_mcp_backend_parity_cpp-rust-parity-20260623-now.json`
- Markdown: `/tmp/matrixark-mcp-backend-parity/matrixark_mcp_backend_parity_cpp-rust-parity-20260623-now.md`

| Backend | Status | Elapsed | Selected refs | Context tokens | Summary refresh |
| --- | ---: | ---: | ---: | ---: | ---: |
| C++ | pass | 200.53 ms | 1 | 18 | 3 nodes |
| Rust | pass | 164.34 ms | 1 | 18 | 3 nodes |

Validated behavior:

- `matrixark_ingest` accepted the same event shape on both backends.
- Both backends created the same logical node path: `memory / approvals / gpu`.
- Both backends marked summaries dirty and refreshed all three node summaries.
- Both backends retrieved one relevant context ref under the same token budget.

## Feature Parity Result

Report artifacts:

- JSON: `/tmp/matrixark-mcp-feature-parity/matrixark_mcp_feature_parity_cpp-rust-feature-20260623-now.json`
- Markdown: `/tmp/matrixark-mcp-feature-parity/matrixark_mcp_feature_parity_cpp-rust-feature-20260623-now.md`

| Backend | Status | Elapsed | Online retrieve | Feedback confirmation | Batch extraction | Tree traversal | Replay |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| C++ | pass | 234.31 ms | pass | pass | pass | pass | pass |
| Rust | pass | 203.63 ms | pass | pass | pass | pass | pass |

Checks passed on both backends:

- `online_ingest_new_event`
- `online_summary_refreshed`
- `online_retrieve_selected`
- `feedback_confirmation`
- `post_feedback_retrieve_selected`
- `batch_extract_committed`
- `batch_summary_refreshed`
- `current_preference_selected`
- `current_location_selected`
- `replay_has_records`

Tree traversal details on both backends:

- `enabled: true`
- `fallback_to_flat: false`
- `summary_embeddings: node_l0, node_l1`
- `top_k_per_layer: 8`
- `max_children_scored_per_parent: 10000`
- selected nodes: 3
- selected leaf nodes: 1

## What This Proves

The same MatrixArk context API path now works on both C++ and Rust TemporalStore backends for the MVP pipeline:

```text
message / batch
-> MatrixArk extraction
-> ContextNode path materialization
-> ContextEvent / ContextEntity / ContextIndex writes
-> dirty summary marker
-> async L0/L1 summary refresh + embedding write
-> tree-first retrieval over summary embeddings
-> event/entity selection
-> token-budgeted ContextPack
-> feedback confirmation
-> replay
```

The Rust path is no longer limited to process-per-operation CLI behavior for this parity test. It uses the long-lived Rust backend path and is comparable to the C++ backend for these small parity workloads.

## Remaining Gaps

This parity doc is functional pipeline parity, not final production throughput parity.

Remaining work:

- Run larger LOCOMO and LongMemEval benchmark slices with both backends after batching large record writes further.
- Add raw C++ and Rust engine microbenchmarks separate from MatrixArk pipeline benchmarks.
- Continue pushing tree traversal, secondary index filtering, and ContextPack audit writes down into native backend APIs.
- Validate multi-node async storage and Raft mode separately from this single-node parity run.
