# MatrixArk MCP Feature Parity

Run ID: `20260623-rust-parity`
All OK: `True`
Comparison: `passed`

## What Was Tested

- online `matrixark_ingest` writes a raw ContextEvent path
- `matrixark_refresh_summaries` refreshes dirty L0/L1 summaries
- `matrixark_retrieve` returns a ContextPack
- `matrixark_feedback` classifies confirmation using prior ContextPack refs
- `matrixark_batch_extract` commits a 20-message logical session batch
- current-state retrieval uses the same entity/event/summary pack path
- `matrixark_replay` returns replayable context-pack data

## cpp

- OK: `True`
- Storage prefix: `matrixark:mcp:feature-parity:cpp:20260623-rust-parity`
- Online selected refs: `1`
- Feedback classification: `CONFIRMATION`
- Batch status: `accepted`
- Batch counts: `{"dirty_markers": 0, "embeddings": 0, "entities": 0, "events": 0, "indexes": 0, "segments": 0}`
- Batch summary refresh count: `3`
- Current location selected refs: `21`
- Current preference selected refs: `21`

## rust

- OK: `True`
- Storage prefix: `matrixark:mcp:feature-parity:rust:20260623-rust-parity`
- Online selected refs: `1`
- Feedback classification: `CONFIRMATION`
- Batch status: `accepted`
- Batch counts: `{"dirty_markers": 0, "embeddings": 0, "entities": 0, "events": 0, "indexes": 0, "segments": 0}`
- Batch summary refresh count: `3`
- Current location selected refs: `21`
- Current preference selected refs: `21`

## C++ Vs Rust Comparison

```json
{
  "checks": {
    "batch_status_equal": true,
    "current_location_selected_equal": true,
    "current_preference_selected_equal": true,
    "feedback_classification_equal": true,
    "location_question_type_equal": true,
    "online_retrieve_selected_equal": true,
    "preference_question_type_equal": true
  },
  "status": "passed"
}
```
