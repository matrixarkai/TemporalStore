# MatrixArk MCP C++ / Rust Feature Parity Test

This document records the MatrixArk MCP pipeline parity test for the C++ and Rust TemporalStore storage paths.

## Test Command

```bash
cd <repo>
python3 tools/run_matrixark_mcp_feature_parity.py --backends cpp rust
```

The runner writes machine-readable artifacts to:

```text
/tmp/matrixark-mcp-feature-parity/
```

Latest validated run:

```text
/tmp/matrixark-mcp-feature-parity/matrixark_mcp_feature_parity_1782242945.json
/tmp/matrixark-mcp-feature-parity/matrixark_mcp_feature_parity_1782242945.md
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

Latest feature-parity run timing:

- C++ TemporalStore path: about `1.33 s`.
- Rust TemporalStore path: about `1.47 s`.

Latest lightweight backend-parity run timing:

- C++ TemporalStore path: about `610 ms` after local deployment readiness.
- Rust TemporalStore path: about `444 ms` after local deployment readiness.

The Rust MCP path now keeps `matrixark_record_log --serve` alive as a persistent JSON-lines process and reuses the Rust SDK client internally. That removes the old process-per-operation bottleneck where Rust took about `6.6 s` on the lightweight parity run and about `35.7 s` on the feature-parity run.

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

The next Rust parity gap is production packaging, not correctness:

```text
Current Rust MCP path:
MatrixArk MCP -> persistent matrixark_record_log --serve process -> Rust SDK -> TemporalStore

Future production option:
MatrixArk MCP -> Rust proxy or Python-callable Rust binding -> Rust SDK -> TemporalStore
```

The persistent process is enough for MCP parity and local debugging. A native gateway or binding can further improve concurrency and operational control.
