# MatrixArk Scale Sweeps 2026-06-26

## Result

- status: `completed_with_blockers`
- shared C++/Rust context pipeline: `passed`
- C++ unified contract seconds: `6.118957975995727`
- Rust unified mock seconds: `1.8898360169987427`
- total expected events in shared pipeline: `43`
- context-pack/audit records in partial retrieval trace: `116` packs, `221` audit logs
- partial ContextPack count: `104`

## What Ran

1. Shared C++/Rust full context pipeline scale E2E: passed.
2. Live C++ raw SDK 1K hset/hget sweep: attempted, timed out before report.
3. Live C++ vs Rust scale comparison: attempted, timed out before report.
4. Local retrieval/resource 4/8/16/32 worker sweep: attempted, wrote partial JSONL trace, timed out before final report.

## Retrieval Trace Summary

```json
{
  "context_pack_audit_count": 116,
  "fallback_reasons": {
    "deadline_after_embedding_index_scan": 1,
    "deadline_after_event_scan": 6,
    "deadline_after_record_load": 97
  },
  "jsonl_bytes": 6187663,
  "jsonl_path": "/root/src/github-services/TemporalStore/docs/benchmarks/matrixark_scale_sweeps_20260626/retrieve_resource_scale/matrixark_retrieve_resource_scale.jsonl",
  "matrixark_audit_log_count": 221,
  "partial_context_pack_count": 104,
  "record_counts": {
    "context_child_ref": 21,
    "context_debug_record": 1120,
    "context_embedding": 1534,
    "context_entity": 120,
    "context_entity_update_audit": 120,
    "context_event": 1000,
    "context_extraction_audit": 10,
    "context_index": 80,
    "context_node": 22,
    "context_pack_audit": 116,
    "context_pack_telemetry": 12,
    "context_recall_reinforcement": 376,
    "context_segment": 40,
    "context_summary": 374,
    "context_summary_dirty": 40,
    "context_summary_refresh_audit": 182,
    "matrixark_audit_log": 221
  },
  "retrieval_elapsed_ms": {
    "avg": 32055.519,
    "count": 104,
    "max": 72468.813,
    "p50": 29582.856,
    "p95": 63636.795,
    "p99": 71053.461
  },
  "selected_refs": {
    "avg": 11.043,
    "p50": 8,
    "p95": 50
  },
  "used_context_tokens": {
    "avg": 623.621,
    "p50": 647,
    "p95": 656
  }
}
```

## Blockers

### C++ native backend 1K/10K/100K ingestion sweep

- result: `timed_out_after_184s_before_report`
- evidence: `No raw SDK report was produced. A lingering run was killed.`
- likely cause: Live local C++ deployment is single-node sync storage (storage_async=false) with small storage_zone_size=10485760; not the async/bundled production sweep profile.

### C++ vs Rust live scale report

- result: `timed_out_after_244s_before_report`
- evidence: `No cpp_rust_scale_120 report was produced.`
- likely cause: Live native backend write/query path still blocks before comparison artifact can complete.

### retrieval worker sweep 4/8/16/32 with resource import and audit

- result: `timed_out_after_244s_with_partial_jsonl_trace`
- evidence: `/root/src/github-services/TemporalStore/docs/benchmarks/matrixark_scale_sweeps_20260626/retrieve_resource_scale/matrixark_retrieve_resource_scale.jsonl`
- likely cause: Local Python/MCP record-log loading and audit writes degrade into timeout_partial packs; retrieval elapsed in audit records is ~56-72s despite 5s deadline fallback.

## Next Fixes

- Run native sweeps only after launching C++/Rust with async oplog/bundled write profile and adequate storage zone size.
- Move ingestion sweep below Python JSONL/MCP path: native matrixark_batch_append_records should enqueue/batch inside TemporalStore.
- For retrieval scale, push prefix scan, secondary-index prefilter, and ContextPack audit buffering into C++/Rust; current local Python audit trace shows timeout_partial fallback under load.
- Rerun 1K, 10K, 100K only after 1K produces a report under the guardrail; then run 4/8/16/32 workers separately from ingest-only.
