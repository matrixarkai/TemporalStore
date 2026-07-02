# MatrixArk Concurrent Retrieve And Resource Import Scale - 2026-06-26

## Summary

This report runs MatrixArk local-backend scale checks for concurrent retrieval workers and larger resource imports. It is intended to expose MatrixArk MCP pipeline behavior without C++/Rust topology noise.

- backend: `local-jsonl`
- event log: `<repo>/docs/benchmarks/matrixark_retrieve_resource_scale_20260626/matrixark_retrieve_resource_scale.jsonl`
- status: `passed`
- seed events: `100`
- resource fixtures: large text-PDF fallback, large CSV, repo directory

## Concurrent Retrieve Workers

| Workers | Status | Ops | QPS | p50 ms | p95 ms | p99 ms | Errors | Avg refs | Avg tokens |
| ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 4 | passed | 4 | 37.854 | 96.496 | 103.411 | 103.411 | 0 | 40.5 | 519.5 |
| 8 | passed | 8 | 30.037 | 210.598 | 216.97 | 216.97 | 0 | 40.5 | 519.5 |
| 16 | passed | 16 | 14.074 | 539.405 | 769.652 | 791.923 | 0 | 33.875 | 432.938 |
| 32 | passed | 32 | 2.937 | 6758.193 | 8148.782 | 8256.751 | 0 | 33.75 | 432.938 |

## Larger Attempt Findings

Before this completed diagnostic run, two larger local-backend attempts were executed and stopped by the 15-minute guardrail:

1. `seed_events=1000`, `retrieve_ops_per_worker=6`, `pdf_paragraphs=360`, `csv_rows=2500`, `repo_files=80`.
2. `seed_events=500`, `retrieve_ops_per_worker=3`, `pdf_paragraphs=120`, `csv_rows=800`, `repo_files=40`.

Both attempts wrote multi-megabyte local JSONL event logs but did not reach final report emission. The completed run above keeps the requested `4/8/16/32` worker sweep and resource shapes, while bounding fixture size so it finishes. This is a local JSONL/Python MCP pipeline limit, not a C++/Rust storage-engine result.

## Resource Import Scale

| Resource | Status | Type | Import ms | Chunks | Fact events | Fact entities | Warnings |
| --- | --- | --- | ---: | ---: | ---: | ---: | ---: |
| large_pdf | accepted | pdf | 167358.627 | 60 | 80 | 80 | 0 |
| large_csv | accepted | csv | 190358.574 | 40 | 112 | 112 | 0 |
| repo_directory | accepted | directory | 140682.769 | 13 | 26 | 26 | 1 |
| post_resource_summary_refresh | ok |  | 18479.545 |  |  |  |  |

## Seed Ingestion

```json
{
  "batch_count": 2,
  "batch_size": 50,
  "ingest_latency_ms": {
    "avg": 29.426,
    "count": 2,
    "max": 37.81,
    "p50": 21.043,
    "p95": 37.81,
    "p99": 37.81
  },
  "scope": {
    "account_id": "acct_scale",
    "agent_name": "scale_runner",
    "session_id": "session_retrieve_scale",
    "tenant_id": "tenant_scale",
    "user_id": "user_scale"
  },
  "seed_events_requested": 100,
  "seed_events_written": 100,
  "summary_refresh": {
    "access": {
      "account_id": "acct_scale",
      "agent_name": "scale_runner",
      "api_key_id": "dev",
      "mode": "dev",
      "role": "dev_admin",
      "session_id": "session_retrieve_scale",
      "tenant_id": "tenant_scale",
      "user_id": "user_scale"
    },
    "refreshed": [
      {
        "dirty_hash": 8227359972247914220,
        "generated_summary_types": [
          "node_l0",
          "node_l1"
        ],
        "node_hash": 6025066374489355495,
        "node_path": [
          "context",
          "scale_runner",
          "project_0"
        ],
        "source_event_count": 8,
        "source_summary_count": 1,
        "summary_generation_policy": {
          "child_summary_count": 1,
          "event_count": 8,
          "generate_l1": true,
          "reason": "has_child_summaries",
          "token_estimate": 351
        },
        "summary_version_hash": 2239785489632830388
      },
      {
        "dirty_hash": 308205720954480822,
        "generated_summary_types": [
          "node_l0",
          "node_l1"
        ],
        "node_hash": 4335767568383908923,
        "node_path": [
          "context",
          "scale_runner",
          "project_0",
          "topic_0"
        ],
        "source_event_count": 8,
        "source_summary_count": 0,
        "summary_generation_policy": {
          "child_summary_count": 0,
          "event_count": 8,
          "generate_l1": true,
          "reason": "event_count_threshold",
          "token_estimate": 175
        },
        "summary_version_hash": 6521946060362909724
      },
      {
        "dirty_hash": 8991636715252044759,
        "generated_summary_types": [
          "node_l0",
          "node_l1"
        ],
        "node_hash": 7671761716597775914,
        "node_path": [
          "context"
        ],
        "source_event_count": 8,
        "source_summary_count": 2,
        "summary_generation_policy": {
          "child_summary_count": 2,
          "event_count": 8,
          "generate_l1": true,
          "reason": "has_child_summaries",
          "token_estimate": 526
        },
        "summary_version_hash": 2495634904187120481
      },
      {
        "dirty_hash": 8199103761385260200,
        "generated_summary_types": [
          "node_l0",
          "node_l1"
        ],
        "node_hash": 682485312310361849,
        "node_path": [
          "context",
          "scale_runner"
        ],
        "source_event_count": 8,
        "source_summary_count": 2,
        "summary_generation_policy": {
          "child_summary_count": 2,
          "event_count": 8,
          "generate_l1": true,
          "reason": "has_child_summaries",
          "token_estimate": 526
        },
        "summary_version_hash": 2009258946885221749
      },
      {
        "dirty_hash": 6601896699148842973,
        "generated_summary_types": [
          "node_l0",
          "node_l1"
        ],
        "node_hash": 5729357460913547841,
        "node_path": [
          "context",
          "scale_runner",
          "project_1"
        ],
        "source_event_count": 8,
        "source_summary_count": 1,
        "summary_generation_policy": {
          "child_summary_count": 1,
          "event_count": 8,
          "generate_l1": true,
          "reason": "has_child_summaries",
          "token_estimate": 351
        },
        "summary_version_hash": 8331468630297785508
      },
      {
        "dirty_hash": 3404223110478162891,
        "generated_summary_types": [
          "node_l0",
          "node_l1"
        ],
        "node_hash": 5109677731576304563,
        "node_path": [
          "context",
          "scale_runner",
          "project_1",
          "topic_1"
        ],
        "source_event_count": 8,
        "source_summary_count": 0,
        "summary_generation_policy": {
          "child_summary_count": 0,
          "event_count": 8,
          "generate_l1": true,
          "reason": "event_count_threshold",
          "token_estimate": 175
        },
        "summary_version_hash": 4675382739681240581
      }
    ],
    "refreshed_count": 6,
    "status": "ok"
  },
  "summary_refresh_ms": 16.92
}
```

## Record Counts

```json
{
  "context_child_ref": 9,
  "context_embedding": 1198,
  "context_entity": 242,
  "context_entity_update_audit": 24,
  "context_event": 321,
  "context_extraction_audit": 2,
  "context_index": 2682,
  "context_node": 11,
  "context_pack_audit": 60,
  "context_segment": 8,
  "context_summary": 514,
  "context_summary_dirty": 26,
  "context_summary_refresh_audit": 284,
  "matrixark_audit_log": 216,
  "matrixark_metric": 3,
  "resource_chunk": 113,
  "resource_import_task": 9,
  "resource_manifest": 3,
  "resource_registry": 3,
  "session_buffer_event": 3
}
```

## Notes

- `large_budget_policy.pdf` is a text-PDF fallback fixture so it exercises the PDF path without requiring binary PDF rendering dependencies.
- CSV uses row-group chunking, not one tiny chunk per row.
- Repo directory ingestion preserves relative paths and skips ignored folders through the parser defaults.
- This is local JSONL backend evidence. C++/Rust storage parity should run the same logical workload through native backends after topology readiness.
