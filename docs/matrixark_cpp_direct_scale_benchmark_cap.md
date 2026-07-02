# MatrixArk C++ TemporalStore Concurrent Scale Benchmark

## Summary

- backend: `temporalstore-direct`
- status: `passed`
- storage_prefix: `matrixark:cpp:scale:cap:1782232768196`
- ingest concurrency: `4`
- retrieve concurrency: `8`
- ingest QPS: `0.245`
- retrieve QPS: `0.327`
- ingest errors: `0`
- retrieve errors: `0`

## Latency

```json
{
  "ingest": {
    "avg": 16084.998,
    "count": 8,
    "max": 17027.815,
    "p50": 16303.711,
    "p95": 17027.815,
    "p99": 17027.815
  },
  "retrieve": {
    "avg": 11861.7,
    "count": 24,
    "max": 61646.049,
    "p50": 1892.854,
    "p95": 61609.826,
    "p99": 61646.049
  }
}
```

## What Is Measured

- Ingest operation = `matrixark_batch_extract` with 20-message logical batch, OSS encoder understanding, ContextEvent/Entity/Segment/Index/Summary/Embedding writes, then `matrixark_refresh_summaries`.
- Retrieve operation = new process-local adapter reads the persisted C++ TemporalStore prefix and runs tree/summary/index/event retrieval into a ContextPack.
- Each ingest worker uses its own storage prefix. This avoids the current Python append-log count key becoming a write serialization artifact and gives a cleaner C++ storage + MatrixArk pipeline cap.
- This is not raw C++ engine QPS. It includes Python orchestration and OSS embedding/query-understanding work.

## C++ Service Snapshot

```json
{
  "processes": [
    {
      "args": "<repo>/output-ubuntu22/release/bcache2-metaserver --metaserver_cluster_name=localdeploy --metaserver_server_port=18000 --metaserver_work_dir=/tmp/temporalstore-deploy/runtime/metaserver1/data --metaserver_log_dir=/tmp/temporalstore-deploy/runtime/metaserver1/log --metaserver_raft_id=1 --metaserver_raft_peers=1,127.0.0.1:18010,127.0.0.1:18020,0 --metaserver_raft_heartbeat_cycle_ms=500 --metaserver_raft_election_cycle_ms=1500 --metaserver_raft_segment_size=16384 --metaserver_snapshot_trigger_interval_sec=0 --metaserver_meta_check_routine_interval_sec=1 --metaserver_balance_routine_interval_ms=3000 --metaserver_placement_host_deduplicate=false --metaserver_forbid_auto_register_for_convict_server=false --metaserver_consul_announce_enabled=false --metaserver_log_level=2",
      "command": "bcache2-metaser",
      "cpu_percent": "704",
      "mem_percent": "0.2",
      "pid": "9087",
      "rss_kb": "34208"
    },
    {
      "args": "<repo>/output-ubuntu22/release/bcache2-server --cluster_name=localdeploy --metaserver_uri=127.0.0.1:18000 --host_spec_path=/tmp/temporalstore-deploy/runtime/server1/host_spec.json --host=127.0.0.1 --port=18001 --server_log_dir=/tmp/temporalstore-deploy/runtime/server1/log --server_log_level=2 --server_meta_tinker_interval_ms=1000 --server_heartbeat_interval_ms=1000 --storage_zone_size=10485760 --stream_max_blob_size=10485760 --storage_async=false --storage_oplog_delay_dump_length=0 --replicator_out_of_sync_s=10",
      "command": "bcache2-server",
      "cpu_percent": "8.0",
      "mem_percent": "0.3",
      "pid": "9197",
      "rss_kb": "60340"
    }
  ]
}
```

## Sample Ingest Results

```json
[
  {
    "entities_written": 7,
    "events_written": 20,
    "indexes_written": 8,
    "latency_ms": 17027.815,
    "node_path": [
      "scale_memory",
      "user_0",
      "session_0",
      "topic_0"
    ],
    "op_index": 0,
    "record_count": 107,
    "scope": {
      "account_id": "acct_scale",
      "session_id": "scale_session_0",
      "tenant_id": "tenant_scale",
      "user_id": "user_0"
    },
    "segments_written": 5,
    "status": "ok",
    "storage_prefix": "matrixark:cpp:scale:cap:1782232768196:ingest:000000",
    "summary_refresh": {
      "access": {
        "account_id": "acct_scale",
        "api_key_id": "dev",
        "mode": "dev",
        "role": "dev_admin",
        "tenant_id": "tenant_scale"
      },
      "refreshed": [
        {
          "dirty_hash": 6842607682337475614,
          "node_hash": 4161404976699238217,
          "node_path": [
            "scale_memory"
          ],
          "source_event_count": 8,
          "source_summary_count": 1,
          "summary_version_hash": 1373996255070218707
        },
        {
          "dirty_hash": 8038568378220240568,
          "node_hash": 3398311254707296233,
          "node_path": [
            "scale_memory",
            "user_0"
          ],
          "source_event_count": 8,
          "source_summary_count": 1,
          "summary_version_hash": 520065526884041536
        },
        {
          "dirty_hash": 7827036468152821573,
          "node_hash": 7705047518059419169,
          "node_path": [
            "scale_memory",
            "user_0",
            "session_0"
          ],
          "source_event_count": 8,
          "source_summary_count": 1,
          "summary_version_hash": 5047356298473417904
        },
        {
          "dirty_hash": 6916799676182601925,
          "node_hash": 4032639987420463562,
          "node_path": [
            "scale_memory",
            "user_0",
            "session_0",
            "topic_0"
          ],
          "source_event_count": 8,
          "source_summary_count": 0,
          "summary_version_hash": 3770860542269499579
        }
      ],
      "refreshed_count": 4,
      "status": "ok"
    }
  },
  {
    "entities_written": 7,
    "events_written": 20,
    "indexes_written": 8,
    "latency_ms": 16865.349,
    "node_path": [
      "scale_memory",
      "user_1",
      "session_1",
      "topic_1"
    ],
    "op_index": 1,
    "record_count": 107,
    "scope": {
      "account_id": "acct_scale",
      "session_id": "scale_session_1",
      "tenant_id": "tenant_scale",
      "user_id": "user_1"
    },
    "segments_written": 5,
    "status": "ok",
    "storage_prefix": "matrixark:cpp:scale:cap:1782232768196:ingest:000001",
    "summary_refresh": {
      "access": {
        "account_id": "acct_scale",
        "api_key_id": "dev",
        "mode": "dev",
        "role": "dev_admin",
        "tenant_id": "tenant_scale"
      },
      "refreshed": [
        {
          "dirty_hash": 1494146723994496587,
          "node_hash": 4161404976699238217,
          "node_path": [
            "scale_memory"
          ],
          "source_event_count": 8,
          "source_summary_count": 1,
          "summary_version_hash": 2702502793180761097
        },
        {
          "dirty_hash": 5192341614515845625,
          "node_hash": 3582765207560127561,
          "node_path": [
            "scale_memory",
            "user_1"
          ],
          "source_event_count": 8,
          "source_summary_count": 1,
          "summary_version_hash": 9073916514873259269
        },
        {
          "dirty_hash": 5885195799061122691,
          "node_hash": 2229273187946850223,
          "node_path": [
            "scale_memory",
            "user_1",
            "session_1"
          ],
          "source_event_count": 8,
          "source_summary_count": 1,
          "summary_version_hash": 1289011365184232257
        },
        {
          "dirty_hash": 6118638578262298021,
          "node_hash": 2863495772041194278,
          "node_path": [
            "scale_memory",
            "user_1",
            "session_1",
            "topic_1"
          ],
          "source_event_count": 8,
          "source_summary_count": 0,
          "summary_version_hash": 2456390696880501464
        }
      ],
      "refreshed_count": 4,
      "status": "ok"
    }
  },
  {
    "entities_written": 7,
    "events_written": 20,
    "indexes_written": 8,
    "latency_ms": 17026.317,
    "node_path": [
      "scale_memory",
      "user_2",
      "session_2",
      "topic_2"
    ],
    "op_index": 2,
    "record_count": 107,
    "scope": {
      "account_id": "acct_scale",
      "session_id": "scale_session_2",
      "tenant_id": "tenant_scale",
      "user_id": "user_2"
    },
    "segments_written": 5,
    "status": "ok",
    "storage_prefix": "matrixark:cpp:scale:cap:1782232768196:ingest:000002",
    "summary_refresh": {
      "access": {
        "account_id": "acct_scale",
        "api_key_id": "dev",
        "mode": "dev",
        "role": "dev_admin",
        "tenant_id": "tenant_scale"
      },
      "refreshed": [
        {
          "dirty_hash": 7951288324730770258,
          "node_hash": 4161404976699238217,
          "node_path": [
            "scale_memory"
          ],
          "source_event_count": 8,
          "source_summary_count": 1,
          "summary_version_hash": 2936798941281009130
        },
        {
          "dirty_hash": 5220296539147573742,
          "node_hash": 8695006746314541035,
          "node_path": [
            "scale_memory",
            "user_2"
          ],
          "source_event_count": 8,
          "source_summary_count": 1,
          "summary_version_hash": 7233265402593834112
        },
        {
          "dirty_hash": 6204306865419974968,
          "node_hash": 6793932960607247364,
          "node_path": [
            "scale_memory",
            "user_2",
            "session_2"
          ],
          "source_event_count": 8,
          "source_summary_count": 1,
          "summary_version_hash": 1254088010516570862
        },
        {
          "dirty_hash": 2629143095145792858,
          "node_hash": 325549840268526629,
          "node_path": [
            "scale_memory",
            "user_2",
            "session_2",
            "topic_2"
          ],
          "source_event_count": 8,
          "source_summary_count": 0,
          "summary_version_hash": 6586262022883520925
        }
      ],
      "refreshed_count": 4,
      "status": "ok"
    }
  },
  {
    "entities_written": 7,
    "events_written": 20,
    "indexes_written": 8,
    "latency_ms": 16303.711,
    "node_path": [
      "scale_memory",
      "user_3",
      "session_3",
      "topic_3"
    ],
    "op_index": 3,
    "record_count": 107,
    "scope": {
      "account_id": "acct_scale",
      "session_id": "scale_session_3",
      "tenant_id": "tenant_scale",
      "user_id": "user_3"
    },
    "segments_written": 5,
    "status": "ok",
    "storage_prefix": "matrixark:cpp:scale:cap:1782232768196:ingest:000003",
    "summary_refresh": {
      "access": {
        "account_id": "acct_scale",
        "api_key_id": "dev",
        "mode": "dev",
        "role": "dev_admin",
        "tenant_id": "tenant_scale"
      },
      "refreshed": [
        {
          "dirty_hash": 3858289742697551436,
          "node_hash": 4161404976699238217,
          "node_path": [
            "scale_memory"
          ],
          "source_event_count": 8,
          "source_summary_count": 1,
          "summary_version_hash": 1860815757555880094
        },
        {
          "dirty_hash": 340626560664019791,
          "node_hash": 7831975152526328792,
          "node_path": [
            "scale_memory",
            "user_3"
          ],
          "source_event_count": 8,
          "source_summary_count": 1,
          "summary_version_hash": 3317421645281602286
        },
        {
          "dirty_hash": 2190109612907955991,
          "node_hash": 6415226549422712373,
          "node_path": [
            "scale_memory",
            "user_3",
            "session_3"
          ],
          "source_event_count": 8,
          "source_summary_count": 1,
          "summary_version_hash": 450114458229726089
        },
        {
          "dirty_hash": 526861532223830995,
          "node_hash": 8550341119414500087,
          "node_path": [
            "scale_memory",
            "user_3",
            "session_3",
            "topic_3"
          ],
          "source_event_count": 8,
          "source_summary_count": 0,
          "summary_version_hash": 3679606850896371828
        }
      ],
      "refreshed_count": 4,
      "status": "ok"
    }
  },
  {
    "entities_written": 7,
    "events_written": 20,
    "indexes_written": 8,
    "latency_ms": 15203.761,
    "node_path": [
      "scale_memory",
      "user_4",
      "session_4",
      "topic_4"
    ],
    "op_index": 4,
    "record_count": 107,
    "scope": {
      "account_id": "acct_scale",
      "session_id": "scale_session_4",
      "tenant_id": "tenant_scale",
      "user_id": "user_4"
    },
    "segments_written": 5,
    "status": "ok",
    "storage_prefix": "matrixark:cpp:scale:cap:1782232768196:ingest:000004",
    "summary_refresh": {
      "access": {
        "account_id": "acct_scale",
        "api_key_id": "dev",
        "mode": "dev",
        "role": "dev_admin",
        "tenant_id": "tenant_scale"
      },
      "refreshed": [
        {
          "dirty_hash": 8816146691699714666,
          "node_hash": 4161404976699238217,
          "node_path": [
            "scale_memory"
          ],
          "source_event_count": 8,
          "source_summary_count": 1,
          "summary_version_hash": 1474780143018327996
        },
        {
          "dirty_hash": 36001898295713683,
          "node_hash": 2147661788732287133,
          "node_path": [
            "scale_memory",
            "user_4"
          ],
          "source_event_count": 8,
          "source_summary_count": 1,
          "summary_version_hash": 2072861632744059367
        },
        {
          "dirty_hash": 7735232125701211092,
          "node_hash": 2317811423371549309,
          "node_path": [
            "scale_memory",
            "user_4",
            "session_4"
          ],
          "source_event_count": 8,
          "source_summary_count": 1,
          "summary_version_hash": 7089256272212744693
        },
        {
          "dirty_hash": 49837875907281089,
          "node_hash": 8421448580115699343,
          "node_path": [
            "scale_memory",
            "user_4",
            "session_4",
            "topic_4"
          ],
          "source_event_count": 8,
          "source_summary_count": 0,
          "summary_version_hash": 2921806345247142348
        }
      ],
      "refreshed_count": 4,
      "status": "ok"
    }
  }
]
```

## Sample Retrieve Results

```json
[
  {
    "insufficient_context": false,
    "latency_ms": 9850.57,
    "op_index": 0,
    "query": "Who approved the GPU budget and what amount is current?",
    "question_type": "evidence",
    "selected_ref_count": 13,
    "status": "ok",
    "storage_prefix": "matrixark:cpp:scale:cap:1782232768196:ingest:000000",
    "total_prompt_context_tokens": 185,
    "used_remote_context_tokens": 185
  },
  {
    "insufficient_context": false,
    "latency_ms": 9669.565,
    "op_index": 1,
    "query": "What does the user currently prefer for low latency services?",
    "question_type": "current_state",
    "selected_ref_count": 14,
    "status": "ok",
    "storage_prefix": "matrixark:cpp:scale:cap:1782232768196:ingest:000001",
    "total_prompt_context_tokens": 211,
    "used_remote_context_tokens": 211
  },
  {
    "insufficient_context": false,
    "latency_ms": 9678.836,
    "op_index": 2,
    "query": "Where is the user currently located?",
    "question_type": "current_state",
    "selected_ref_count": 13,
    "status": "ok",
    "storage_prefix": "matrixark:cpp:scale:cap:1782232768196:ingest:000002",
    "total_prompt_context_tokens": 176,
    "used_remote_context_tokens": 176
  },
  {
    "insufficient_context": false,
    "latency_ms": 9760.346,
    "op_index": 3,
    "query": "What is the user's current role?",
    "question_type": "current_state",
    "selected_ref_count": 14,
    "status": "ok",
    "storage_prefix": "matrixark:cpp:scale:cap:1782232768196:ingest:000003",
    "total_prompt_context_tokens": 211,
    "used_remote_context_tokens": 211
  },
  {
    "insufficient_context": false,
    "latency_ms": 9306.037,
    "op_index": 4,
    "query": "What is the current benchmark plan?",
    "question_type": "current_state",
    "selected_ref_count": 13,
    "status": "ok",
    "storage_prefix": "matrixark:cpp:scale:cap:1782232768196:ingest:000004",
    "total_prompt_context_tokens": 175,
    "used_remote_context_tokens": 175
  }
]
```
