# MatrixArk C++ TemporalStore Concurrent Scale Benchmark

## Summary

- backend: `temporalstore-direct`
- status: `completed_with_errors`
- storage_prefix: `matrixark:scale:20260627:cpp:10k:det`
- ingest concurrency: `2`
- retrieve concurrency: `4`
- model warmup: `1.904 ms`
- ingest QPS: `0.0`
- retrieve QPS: `0.0`
- ingest errors: `100`
- retrieve errors: `1`
- service pool: `shared_prefix_pool`
- service pool size: `0`
- pool warmup: `0.0 ms`
- direct record cache: `prefix_record_count_watermark_with_singleflight_load`
- embedding vector cache: `True`
- retrieval deadline ms: `20000`

## Latency

```json
{
  "ingest": {
    "avg": 0.0,
    "count": 0,
    "max": 0.0,
    "p50": 0.0,
    "p95": 0.0,
    "p99": 0.0
  },
  "retrieve": {
    "avg": 0.0,
    "count": 0,
    "max": 0.0,
    "p50": 0.0,
    "p95": 0.0,
    "p99": 0.0
  }
}
```

## What Is Measured

- Ingest operation = `matrixark_batch_extract` with 20-message logical batch, OSS encoder understanding, ContextEvent/Entity/Segment/Index/Summary/Embedding writes, then `matrixark_refresh_summaries`.
- Retrieve operation = new process-local adapter reads the persisted C++ TemporalStore prefix and runs tree/summary/index/event retrieval into a ContextPack.
- Each ingest worker uses its own storage prefix. This avoids the current Python append-log count key becoming a write serialization artifact and gives a cleaner C++ storage + MatrixArk pipeline cap.
- This is not raw C++ engine QPS. It includes Python orchestration and OSS embedding/query-understanding work.
- Optimized mode reuses thread-local MatrixArk services/adapters and uses a C++ direct-record cache keyed by `record_count` watermark.
- Native C++ context API pushdown is still a separate C++ API task; this runner exercises the current direct SDK storage boundary.

## C++ Service Snapshot

```json
{
  "processes": [
    {
      "args": "/root/src/github-services/TemporalStore/output-ubuntu22/release/bcache2-metaserver --metaserver_cluster_name=localdeploy --metaserver_server_port=18000 --metaserver_work_dir=/tmp/temporalstore-deploy/runtime/metaserver1/data --metaserver_log_dir=/tmp/temporalstore-deploy/runtime/metaserver1/log --metaserver_raft_id=1 --metaserver_raft_peers=1,127.0.0.1:18010,127.0.0.1:18020,0 --metaserver_raft_heartbeat_cycle_ms=500 --metaserver_raft_election_cycle_ms=1500 --metaserver_raft_segment_size=16384 --metaserver_snapshot_trigger_interval_sec=0 --metaserver_meta_check_routine_interval_sec=1 --metaserver_balance_routine_interval_ms=3000 --metaserver_placement_host_deduplicate=false --metaserver_forbid_auto_register_for_convict_server=false --metaserver_consul_announce_enabled=false --metaserver_log_level=2",
      "command": "bcache2-metaser",
      "cpu_percent": "17.8",
      "mem_percent": "0.1",
      "pid": "38250",
      "rss_kb": "27580"
    },
    {
      "args": "/root/src/github-services/TemporalStore/output-ubuntu22/release/bcache2-server --cluster_name=localdeploy --metaserver_uri=127.0.0.1:18000 --host_spec_path=/tmp/temporalstore-deploy/runtime/server1/host_spec.json --host=127.0.0.1 --port=18001 --server_log_dir=/tmp/temporalstore-deploy/runtime/server1/log --server_log_level=2 --server_meta_tinker_interval_ms=1000 --server_heartbeat_interval_ms=1000 --storage_zone_size=10485760 --stream_max_blob_size=10485760 --storage_async=false --storage_oplog_delay_dump_length=0 --replicator_out_of_sync_s=10",
      "command": "bcache2-server",
      "cpu_percent": "69.5",
      "mem_percent": "23.5",
      "pid": "38360",
      "rss_kb": "3719760"
    }
  ]
}
```

## Sample Ingest Results

```json
[]
```

## Sample Retrieve Results

```json
[]
```
