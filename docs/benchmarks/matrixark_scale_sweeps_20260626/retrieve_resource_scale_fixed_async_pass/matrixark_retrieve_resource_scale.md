# MatrixArk Concurrent Retrieve And Resource Import Scale - 2026-06-26

## Summary

This report runs MatrixArk local-backend scale checks for concurrent retrieval workers and larger resource imports. It is intended to expose MatrixArk MCP pipeline behavior without C++/Rust topology noise.

- backend: `local-jsonl`
- event log: `/root/src/github-services/TemporalStore/docs/benchmarks/matrixark_scale_sweeps_20260626/retrieve_resource_scale_fixed_async_pass/matrixark_retrieve_resource_scale.jsonl`
- status: `passed`
- seed events: `1000`
- resource fixtures: large text-PDF fallback, large CSV, repo directory

## Concurrent Retrieve Workers

| Workers | Status | Ops | QPS | p50 ms | p95 ms | p99 ms | Errors | Avg refs | Avg tokens |
| ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 4 | passed | 8 | 8.305 | 429.596 | 577.66 | 577.66 | 0 | 41.75 | 494.75 |
| 8 | passed | 16 | 2.11 | 3034.126 | 4876.252 | 5168.227 | 0 | 35.375 | 412.375 |
| 16 | passed | 32 | 1.371 | 9438.426 | 12206.698 | 13720.94 | 0 | 13.062 | 512.719 |
| 32 | passed | 64 | 1.629 | 17168.799 | 21842.026 | 22302.055 | 0 | 8 | 521.312 |

## Resource Import Scale

| Resource | Status | Type | Import ms | Chunks | Fact events | Fact entities | Warnings |
| --- | --- | --- | ---: | ---: | ---: | ---: | ---: |
| large_pdf | queued | pdf | 15449.456 | None | 0 | 0 | None |
| large_csv | queued | csv | 20957.663 | None | 0 | 0 | None |
| repo_directory | queued | directory | 2472.951 | None | 0 | 0 | None |
| post_resource_summary_refresh | skipped |  | 0.0 |  |  |  |  |

## Seed Ingestion

```json
{
  "batch_count": 10,
  "batch_size": 100,
  "ingest_latency_ms": {
    "avg": 58.313,
    "count": 10,
    "max": 73.214,
    "p50": 54.22,
    "p95": 73.214,
    "p99": 73.214
  },
  "scope": {
    "account_id": "acct_scale",
    "agent_name": "scale_runner",
    "session_id": "session_retrieve_scale",
    "tenant_id": "tenant_scale",
    "user_id": "user_scale"
  },
  "seed_events_requested": 1000,
  "seed_events_written": 1000,
  "summary_refresh": {
    "access": {
      "account_id": "acct_scale",
      "agent_name": "scale_runner",
      "api_key_id": "dev",
      "mode": "dev",
      "role": "dev_admin",
      "scope_key": "t=3823117250029978076|u=5172441694772987983|s=8268125161419006472|",
      "session_hash": 8268125161419006472,
      "session_id": "session_retrieve_scale",
      "tenant_hash": 3823117250029978076,
      "tenant_id": "tenant_scale",
      "user_hash": 5172441694772987983,
      "user_id": "user_scale"
    },
    "compression_created_count": 0,
    "refreshed": [
      {
        "dirty_hash": 2904347099228133541,
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
        "source_event_count": 0,
        "source_summary_count": 1,
        "summary_generation_policy": {
          "child_summary_count": 1,
          "event_count": 0,
          "generate_l1": true,
          "reason": "has_child_summaries",
          "token_estimate": 175
        },
        "summary_version_hash": 4767266791123117411,
        "time_compression": {
          "created": [],
          "created_count": 0,
          "max_raw_events_per_node": 256,
          "raw_event_count": 0,
          "reason": "raw_event_count_within_threshold",
          "status": "skipped"
        }
      },
      {
        "dirty_hash": 4275880168613244458,
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
        "summary_version_hash": 6185153138621757405,
        "time_compression": {
          "created": [],
          "created_count": 0,
          "max_raw_events_per_node": 256,
          "raw_event_count": 100,
          "reason": "raw_event_count_within_threshold",
          "status": "skipped"
        }
      },
      {
        "dirty_hash": 7842401462486137388,
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
        "source_event_count": 0,
        "source_summary_count": 1,
        "summary_generation_policy": {
          "child_summary_count": 1,
          "event_count": 0,
          "generate_l1": true,
          "reason": "has_child_summaries",
          "token_estimate": 175
        },
        "summary_version_hash": 1671487899416939747,
        "time_compression": {
          "created": [],
          "created_count": 0,
          "max_raw_events_per_node": 256,
          "raw_event_count": 0,
          "reason": "raw_event_count_within_threshold",
          "status": "skipped"
        }
      },
      {
        "dirty_hash": 8274923134395227954,
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
        "summary_version_hash": 4502062926628672667,
        "time_compression": {
          "created": [],
          "created_count": 0,
          "max_raw_events_per_node": 256,
          "raw_event_count": 100,
          "reason": "raw_event_count_within_threshold",
          "status": "skipped"
        }
      },
      {
        "dirty_hash": 3612983132742491763,
        "generated_summary_types": [
          "node_l0",
          "node_l1"
        ],
        "node_hash": 8098084367660167709,
        "node_path": [
          "context",
          "scale_runner",
          "project_2"
        ],
        "source_event_count": 0,
        "source_summary_count": 1,
        "summary_generation_policy": {
          "child_summary_count": 1,
          "event_count": 0,
          "generate_l1": true,
          "reason": "has_child_summaries",
          "token_estimate": 175
        },
        "summary_version_hash": 5137357941477105691,
        "time_compression": {
          "created": [],
          "created_count": 0,
          "max_raw_events_per_node": 256,
          "raw_event_count": 0,
          "reason": "raw_event_count_within_threshold",
          "status": "skipped"
        }
      },
      {
        "dirty_hash": 3695618678897205559,
        "generated_summary_types": [
          "node_l0",
          "node_l1"
        ],
        "node_hash": 8873368607679409574,
        "node_path": [
          "context",
          "scale_runner",
          "project_2",
          "topic_2"
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
        "summary_version_hash": 575954106318780808,
        "time_compression": {
          "created": [],
          "created_count": 0,
          "max_raw_events_per_node": 256,
          "raw_event_count": 100,
          "reason": "raw_event_count_within_threshold",
          "status": "skipped"
        }
      },
      {
        "dirty_hash": 987837431747014763,
        "generated_summary_types": [
          "node_l0",
          "node_l1"
        ],
        "node_hash": 6920081392849965770,
        "node_path": [
          "context",
          "scale_runner",
          "project_3"
        ],
        "source_event_count": 0,
        "source_summary_count": 1,
        "summary_generation_policy": {
          "child_summary_count": 1,
          "event_count": 0,
          "generate_l1": true,
          "reason": "has_child_summaries",
          "token_estimate": 175
        },
        "summary_version_hash": 1785853910500730919,
        "time_compression": {
          "created": [],
          "created_count": 0,
          "max_raw_events_per_node": 256,
          "raw_event_count": 0,
          "reason": "raw_event_count_within_threshold",
          "status": "skipped"
        }
      },
      {
        "dirty_hash": 1661142791932078931,
        "generated_summary_types": [
          "node_l0",
          "node_l1"
        ],
        "node_hash": 9159771800418959982,
        "node_path": [
          "context",
          "scale_runner",
          "project_3",
          "topic_3"
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
        "summary_version_hash": 8568205163815841491,
        "time_compression": {
          "created": [],
          "created_count": 0,
          "max_raw_events_per_node": 256,
          "raw_event_count": 100,
          "reason": "raw_event_count_within_threshold",
          "status": "skipped"
        }
      },
      {
        "dirty_hash": 852679156112865417,
        "generated_summary_types": [
          "node_l0",
          "node_l1"
        ],
        "node_hash": 3804848088574246844,
        "node_path": [
          "context",
          "scale_runner",
          "project_4"
        ],
        "source_event_count": 0,
        "source_summary_count": 1,
        "summary_generation_policy": {
          "child_summary_count": 1,
          "event_count": 0,
          "generate_l1": true,
          "reason": "has_child_summaries",
          "token_estimate": 175
        },
        "summary_version_hash": 8157192914175670027,
        "time_compression": {
          "created": [],
          "created_count": 0,
          "max_raw_events_per_node": 256,
          "raw_event_count": 0,
          "reason": "raw_event_count_within_threshold",
          "status": "skipped"
        }
      },
      {
        "dirty_hash": 3338928092439417331,
        "generated_summary_types": [
          "node_l0",
          "node_l1"
        ],
        "node_hash": 4510159217867143391,
        "node_path": [
          "context",
          "scale_runner",
          "project_4",
          "topic_4"
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
        "summary_version_hash": 5491793417323675098,
        "time_compression": {
          "created": [],
          "created_count": 0,
          "max_raw_events_per_node": 256,
          "raw_event_count": 100,
          "reason": "raw_event_count_within_threshold",
          "status": "skipped"
        }
      },
      {
        "dirty_hash": 8591548353347355256,
        "generated_summary_types": [
          "node_l0",
          "node_l1"
        ],
        "node_hash": 1709698789622994577,
        "node_path": [
          "context",
          "scale_runner",
          "project_5"
        ],
        "source_event_count": 0,
        "source_summary_count": 1,
        "summary_generation_policy": {
          "child_summary_count": 1,
          "event_count": 0,
          "generate_l1": true,
          "reason": "has_child_summaries",
          "token_estimate": 175
        },
        "summary_version_hash": 122032971102088264,
        "time_compression": {
          "created": [],
          "created_count": 0,
          "max_raw_events_per_node": 256,
          "raw_event_count": 0,
          "reason": "raw_event_count_within_threshold",
          "status": "skipped"
        }
      },
      {
        "dirty_hash": 5480499636279784682,
        "generated_summary_types": [
          "node_l0",
          "node_l1"
        ],
        "node_hash": 9153612399212971530,
        "node_path": [
          "context",
          "scale_runner",
          "project_5",
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
        "summary_version_hash": 2320931667558952402,
        "time_compression": {
          "created": [],
          "created_count": 0,
          "max_raw_events_per_node": 256,
          "raw_event_count": 100,
          "reason": "raw_event_count_within_threshold",
          "status": "skipped"
        }
      },
      {
        "dirty_hash": 3497694187062485541,
        "generated_summary_types": [
          "node_l0",
          "node_l1"
        ],
        "node_hash": 5656046992901106490,
        "node_path": [
          "context",
          "scale_runner",
          "project_6"
        ],
        "source_event_count": 0,
        "source_summary_count": 1,
        "summary_generation_policy": {
          "child_summary_count": 1,
          "event_count": 0,
          "generate_l1": true,
          "reason": "has_child_summaries",
          "token_estimate": 175
        },
        "summary_version_hash": 5099640661717925229,
        "time_compression": {
          "created": [],
          "created_count": 0,
          "max_raw_events_per_node": 256,
          "raw_event_count": 0,
          "reason": "raw_event_count_within_threshold",
          "status": "skipped"
        }
      },
      {
        "dirty_hash": 6769714373562054175,
        "generated_summary_types": [
          "node_l0",
          "node_l1"
        ],
        "node_hash": 4617939580320533347,
        "node_path": [
          "context",
          "scale_runner",
          "project_6",
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
        "summary_version_hash": 2748379702033880521,
        "time_compression": {
          "created": [],
          "created_count": 0,
          "max_raw_events_per_node": 256,
          "raw_event_count": 100,
          "reason": "raw_event_count_within_threshold",
          "status": "skipped"
        }
      },
      {
        "dirty_hash": 7907054135500836389,
        "generated_summary_types": [
          "node_l0",
          "node_l1"
        ],
        "node_hash": 3991249014498892849,
        "node_path": [
          "context",
          "scale_runner",
          "project_7"
        ],
        "source_event_count": 0,
        "source_summary_count": 1,
        "summary_generation_policy": {
          "child_summary_count": 1,
          "event_count": 0,
          "generate_l1": true,
          "reason": "has_child_summaries",
          "token_estimate": 175
        },
        "summary_version_hash": 7594585033520050325,
        "time_compression": {
          "created": [],
          "created_count": 0,
          "max_raw_events_per_node": 256,
          "raw_event_count": 0,
          "reason": "raw_event_count_within_threshold",
          "status": "skipped"
        }
      },
      {
        "dirty_hash": 6249240477213390675,
        "generated_summary_types": [
          "node_l0",
          "node_l1"
        ],
        "node_hash": 1016437133086313523,
        "node_path": [
          "context",
          "scale_runner",
          "project_7",
          "topic_2"
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
        "summary_version_hash": 1031455584001823424,
        "time_compression": {
          "created": [],
          "created_count": 0,
          "max_raw_events_per_node": 256,
          "raw_event_count": 100,
          "reason": "raw_event_count_within_threshold",
          "status": "skipped"
        }
      },
      {
        "dirty_hash": 5455851121718383827,
        "generated_summary_types": [
          "node_l0",
          "node_l1"
        ],
        "node_hash": 3025646238993786331,
        "node_path": [
          "context",
          "scale_runner",
          "project_8"
        ],
        "source_event_count": 0,
        "source_summary_count": 1,
        "summary_generation_policy": {
          "child_summary_count": 1,
          "event_count": 0,
          "generate_l1": true,
          "reason": "has_child_summaries",
          "token_estimate": 175
        },
        "summary_version_hash": 5296843336175661805,
        "time_compression": {
          "created": [],
          "created_count": 0,
          "max_raw_events_per_node": 256,
          "raw_event_count": 0,
          "reason": "raw_event_count_within_threshold",
          "status": "skipped"
        }
      },
      {
        "dirty_hash": 2140627121363776731,
        "generated_summary_types": [
          "node_l0",
          "node_l1"
        ],
        "node_hash": 574836199272572809,
        "node_path": [
          "context",
          "scale_runner",
          "project_8",
          "topic_3"
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
        "summary_version_hash": 7880876038103070583,
        "time_compression": {
          "created": [],
          "created_count": 0,
          "max_raw_events_per_node": 256,
          "raw_event_count": 100,
          "reason": "raw_event_count_within_threshold",
          "status": "skipped"
        }
      },
      {
        "dirty_hash": 8970072852478609116,
        "generated_summary_types": [
          "node_l0",
          "node_l1"
        ],
        "node_hash": 7671761716597775914,
        "node_path": [
          "context"
        ],
        "source_event_count": 0,
        "source_summary_count": 8,
        "summary_generation_policy": {
          "child_summary_count": 8,
          "event_count": 0,
          "generate_l1": true,
          "reason": "has_child_summaries",
          "token_estimate": 1402
        },
        "summary_version_hash": 9074993447675379815,
        "time_compression": {
          "created": [],
          "created_count": 0,
          "max_raw_events_per_node": 256,
          "raw_event_count": 0,
          "reason": "raw_event_count_within_threshold",
          "status": "skipped"
        }
      },
      {
        "dirty_hash": 3297151120980301429,
        "generated_summary_types": [
          "node_l0",
          "node_l1"
        ],
        "node_hash": 682485312310361849,
        "node_path": [
          "context",
          "scale_runner"
        ],
        "source_event_count": 0,
        "source_summary_count": 8,
        "summary_generation_policy": {
          "child_summary_count": 8,
          "event_count": 0,
          "generate_l1": true,
          "reason": "has_child_summaries",
          "token_estimate": 1402
        },
        "summary_version_hash": 1947646362978154296,
        "time_compression": {
          "created": [],
          "created_count": 0,
          "max_raw_events_per_node": 256,
          "raw_event_count": 0,
          "reason": "raw_event_count_within_threshold",
          "status": "skipped"
        }
      },
      {
        "dirty_hash": 3327168888300752171,
        "generated_summary_types": [
          "node_l0",
          "node_l1"
        ],
        "node_hash": 4882055323904538657,
        "node_path": [
          "context",
          "scale_runner",
          "project_9"
        ],
        "source_event_count": 0,
        "source_summary_count": 1,
        "summary_generation_policy": {
          "child_summary_count": 1,
          "event_count": 0,
          "generate_l1": true,
          "reason": "has_child_summaries",
          "token_estimate": 175
        },
        "summary_version_hash": 8810762283127420768,
        "time_compression": {
          "created": [],
          "created_count": 0,
          "max_raw_events_per_node": 256,
          "raw_event_count": 0,
          "reason": "raw_event_count_within_threshold",
          "status": "skipped"
        }
      },
      {
        "dirty_hash": 7851454682668074560,
        "generated_summary_types": [
          "node_l0",
          "node_l1"
        ],
        "node_hash": 102969511486378702,
        "node_path": [
          "context",
          "scale_runner",
          "project_9",
          "topic_4"
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
        "summary_version_hash": 4575669534690083884,
        "time_compression": {
          "created": [],
          "created_count": 0,
          "max_raw_events_per_node": 256,
          "raw_event_count": 100,
          "reason": "raw_event_count_within_threshold",
          "status": "skipped"
        }
      }
    ],
    "refreshed_count": 22,
    "status": "ok"
  },
  "summary_refresh_ms": 754.695
}
```

## Record Counts

```json
{
  "context_child_ref": 25,
  "context_debug_record": 1120,
  "context_embedding": 2420,
  "context_entity": 120,
  "context_entity_update_audit": 120,
  "context_event": 1000,
  "context_extraction_audit": 10,
  "context_index": 82,
  "context_node": 27,
  "context_pack_audit": 91,
  "context_pack_telemetry": 50,
  "context_segment": 40,
  "context_summary": 1261,
  "context_summary_dirty": 45,
  "context_summary_refresh_audit": 622,
  "matrixark_audit_log": 431,
  "resource_import_task": 5,
  "resource_manifest": 1,
  "resource_registry": 1
}
```

## Notes

- `large_budget_policy.pdf` is a text-PDF fallback fixture so it exercises the PDF path without requiring binary PDF rendering dependencies.
- CSV uses row-group chunking, not one tiny chunk per row.
- Repo directory ingestion preserves relative paths and skips ignored folders through the parser defaults.
- This is local JSONL backend evidence. C++/Rust storage parity should run the same logical workload through native backends after topology readiness.
