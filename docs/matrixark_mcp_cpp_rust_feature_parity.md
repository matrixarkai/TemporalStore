# MatrixArk MCP C++ / Rust Feature Parity Test

This document records the MatrixArk MCP pipeline parity test for the C++ and Rust TemporalStore storage paths.

## Test Command

```bash
cd /root/src/github-services/TemporalStore
python3 tools/run_matrixark_mcp_feature_parity.py --backends cpp rust
```

The runner writes machine-readable artifacts to:

```text
/tmp/matrixark-mcp-feature-parity/
```

Latest validated run:

```text
/tmp/matrixark-mcp-feature-parity/matrixark_mcp_feature_parity_1782242382.json
/tmp/matrixark-mcp-feature-parity/matrixark_mcp_feature_parity_1782242382.md
```

## What The Test Covers

The same MCP calls are executed against both backends:

1. `matrixark_ingest` for lightweight online ingestion.
2. `matrixark_refresh_summaries` for async node summary refresh.
3. `matrixark_retrieve` for a token-budgeted ContextPack.
4. `matrixark_feedback` to classify a user confirmation with prior ContextPack refs.
5. `matrixark_batch_extract` over a 20-message logical session batch.
6. `matrixark_refresh_summaries` after batch extraction.
7. Current-state retrieval for location and preference.
8. `matrixark_replay` for replayable ContextPack data.

The goal is storage-path parity, not model-quality benchmarking. By default this runner uses deterministic local model settings so any C++ vs Rust difference is caused by the backend path, not model randomness or network availability.

## Latest Result

| Check | C++ TemporalStore | Rust TemporalStore |
|---|---:|---:|
| Online ingest classification | `NEW_EVENT` | `NEW_EVENT` |
| Online summary refresh count | `3` | `3` |
| Online retrieve selected refs | `1` | `1` |
| Feedback classification | `CONFIRMATION` | `CONFIRMATION` |
| Feedback prior refs | `1` | `1` |
| Post-feedback retrieve selected refs | `1` | `1` |
| Batch extraction status | `accepted` | `accepted` |
| Batch summary refresh count | `3` | `3` |
| Current location selected refs | `21` | `21` |
| Current preference selected refs | `21` | `21` |
| Current-state question type | `current_state` | `current_state` |
| Tree traversal fallback | `false` | `false` |
| L0/L1 summary embeddings used | `node_l0`, `node_l1` | `node_l0`, `node_l1` |
| Replay returned events | yes | yes |

## Timing

Latest run timing:

- C++ TemporalStore path: about `956 ms`.
- Rust TemporalStore path: about `35.7 s`.

The Rust path is logically correct but slower because the current MCP bridge invokes the Rust `matrixark_record_log` CLI once per storage operation. This is acceptable for parity validation but not the final production shape. A production Rust path should use either a long-lived Rust service/gateway or a Python-callable Rust binding to avoid process-per-operation overhead.

## C++ vs Rust Comparison

The latest run passed these parity checks:

```json
{
  "batch_status_equal": true,
  "current_location_selected_equal": true,
  "current_preference_selected_equal": true,
  "feedback_classification_equal": true,
  "location_question_type_equal": true,
  "online_retrieve_selected_equal": true,
  "preference_question_type_equal": true
}
```

## Operational Notes

- C++ remains the default MatrixArk MCP backend: `tools/matrixark_mcp_cpp_server.sh`.
- Rust is available through: `tools/matrixark_mcp_rust_server.sh`.
- Codex config has `matrixark_rust` disabled by default to avoid duplicate MatrixArk tool names.
- Enable Rust MCP only when actively testing the Rust path.

## Next Gap

The next Rust parity gap is performance, not correctness:

```text
Current Rust MCP path:
MatrixArk MCP -> Python RustCliClient -> spawn matrixark_record_log per op -> Rust SDK -> TemporalStore

Target Rust MCP path:
MatrixArk MCP -> long-lived Rust gateway or Python-callable Rust binding -> Rust SDK -> TemporalStore
```
