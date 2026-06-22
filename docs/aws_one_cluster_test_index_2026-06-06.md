# AWS One-Cluster Test Index - 2026-06-06

This note indexes the current AWS test documents for the shared one-cluster setup that runs TemporalStore, ByteKV, and ABase on the same EC2 cluster.

## Cluster

- Region: `us-west-2`
- Meta/proxy/client node: `i-003c930417f7ee609`, private IP `10.70.1.79`, type `t3.small`
- Data node 1: `i-0724d90b323786546`, private IP `10.70.1.163`, type `c7i.large`
- Data node 2: `i-096334bd8cc7ab259`, private IP `10.70.1.202`, type `c7i.large`
- Policy: reuse this cluster for TemporalStore, ByteKV, and ABase testing. Do not create separate product-specific AWS clusters for normal tests.

## TemporalStore Test Docs

- `docs/aws_three_service_results_summary_2026-06-06.md`
  - Consolidated TemporalStore, ByteKV, and ABase AWS run results, including one-hour soak summaries and focused scale-test links.
- `docs/aws_one_cluster_binary_topology.md`
  - Binary placement, ports, storage mounts, and client/proxy topology for the shared TemporalStore, ByteKV, and ABase AWS cluster.
- `docs/aws_cluster_teardown_context_2026-06-06.md`
  - Saved AWS resource IDs, last runtime state, recreate checklist, and teardown context for the TemporalStore AWS test cluster.
- `docs/aws_scale_replication_results_2026-06-04.md`
  - Temporal feature scale runs, EFS shared-store behavior, replication visibility, and observed failure modes.
- `docs/aws_efs_performance_summary_2026-06-04.md`
  - EFS latency and shared-store behavior notes for TemporalStore.
- `docs/aws_storage_async_compare_2026-06-06.md`
  - A/B benchmark for `storage_async=false` vs `storage_async=true` on the AWS EFS shared-file path. Async writes improved QPS by roughly 6x-13x in the tested workloads, with an explicit RPO caveat.
- `docs/secondary_replication_visibility_lag_2026-06-04.md`
  - Secondary read lag investigation and visibility expectations.
- `docs/primary_pull_vs_shared_store_replication_2026-06-05.md`
  - Primary-pull versus shared-store replication paths and tradeoffs.
- `docs/runtime_tuning.md`
  - Oplog batching, dump thresholds, and runtime tuning knobs.
- `docs/TEMPORALSTORE_ONE_CLUSTER_TEMPORAL_FEATURES.md`
  - One-cluster temporal feature serving design and product story.

## ByteKV Test Docs

- `docs/aws_bytekv_abase_one_hour_soak_2026-06-06.md`
  - One-hour ByteKV and ABase soak results, including ByteKV client cleanup crashes and ABase direct-path stability.
- `../BYTEKV/docs/aws_scale_latency_2026-06-06.md`
  - Latest ByteKV AWS scale run on the shared cluster.
- `../BYTEKV/infra/aws/bytekv-test/README.md`
  - Terraform/bootstrap instructions for ByteKV. Current operating policy is to reuse the shared TemporalStore cluster unless explicitly creating a standalone ByteKV test stack.

## ABase Test Docs

- `docs/abase_aws_deployment.md`
  - ABase one-cluster deployment notes.
- `docs/abase_redis_api_test_2026-06-06.md`
  - Latest Redis protocol compatibility test result. Services were alive, but the proxy was registered as `ABASE2_THRIFT_PROTOCOL`; common Redis commands on ports `19077` / `19078` timed out or closed.

## Latest AWS Test Summary

ByteKV scale testing ran successfully through the existing meta node against the shared data nodes. The main result directory was:

```text
outputs/aws_bytekv_scale_cpu_20260606T025613Z
```

Representative ByteKV results:

| Mode | Threads | Ops | QPS | p50 | p95 | Notes |
|---|---:|---:|---:|---:|---:|---|
| read | 2 | 5,000 | 1,328 | 906 us | 4,164 us | pass |
| read | 4 | 5,000 | 3,545 | 1,028 us | 1,910 us | pass |
| mixed 50/50 | 2 | 5,000 | 736 | 3,092 us | 5,638 us | pass |
| mixed 50/50 | 4 | 5,000 | 1,187 | 3,538 us | 6,871 us | pass |
| write | 2 | 5,000 | 434 | 4,126 us | 7,515 us | pass |
| write | 4 | 5,000 | 753 | 5,055 us | 7,408 us | benchmark printed `PASS`, then client exited with a cleanup segfault |

The CPU monitor harness was fixed after this run by writing the monitor program to a temporary script on the target node and running it with bash. Validation result directory:

```text
outputs/aws_bytekv_scale_cpu_20260606T030415Z
```

All six data-node CPU monitor files in the validation run returned `Success`.

ABase Redis protocol testing reached the service endpoint but did not complete:

- ABase master, proxy, and two datanodes were running.
- The master listed both datanodes as `DATANODE_STATE_NORMAL`.
- The master listed proxy `10.70.1.79:19078` as `ABASE2_THRIFT_PROTOCOL`, not `REDIS_PROTOCOL`.
- `redis-cli` and raw RESP tests on `19077` / `19078` did not produce working Redis command behavior for `PING`, `SET`, `GET`, `INCR`, hash commands, `MGET`, or `DEL`.
- Next step is to enable or start the Redis-compatible proxy mode before running Redis scale tests.
