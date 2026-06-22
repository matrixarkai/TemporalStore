# AWS Storage Async Comparison - 2026-06-06

## Goal

Compare TemporalStore write performance on the existing shared AWS cluster with:

- `storage_async=false`
- `storage_async=true`

The test used the same two data-node placement for both modes and changed only the data-node runtime flag. No AWS resources were created or destroyed.

## Cluster

- Metaserver/client node: `i-003c930417f7ee609`, `10.70.1.79`, `t3.small`
- Data node 1: `i-0724d90b323786546`, `10.70.1.163`, `c7i.large`, port `17001`
- Data node 2: `i-096334bd8cc7ab259`, `10.70.1.202`, `c7i.large`, port `17002`
- Storage backend: EFS mounted as `shared-file:///mnt/temporalstore-shared/aws-scale/storage/...`
- Blockcache: disabled for this comparison to isolate storage write behavior and avoid the mtcache SSD lock seen during restart.

After testing, data01 and data02 were restored to `storage_async=false`.

## Method

Reusable script:

```text
infra/aws/temporalstore-test/run_storage_async_compare.ps1
```

Each mode:

1. Restart data01 and data02 with the target `--storage_async` value.
2. Register/refresh both data nodes with metaserver.
3. Create fresh namespaces/tables for each run.
4. Run 3 repeats at 1, 2, and 4 client threads.
5. Run both `string_scale_benchmark` and `temporal_aggregate_scale_benchmark`.

Clean result directories:

```text
/var/lib/temporalstore/storage-async-compare-20260606_015137-false
/var/lib/temporalstore/storage-async-compare-20260606_015137-true
```

## Summary

| Mode | Workload | Threads | Runs | Avg write QPS | Median write p99 | Max write p99 | Failures |
|---|---|---:|---:|---:|---:|---:|---:|
| sync | STRING set | 1 | 3 | 101.33 | 15.631 ms | 18.182 ms | 0 |
| sync | STRING set | 2 | 3 | 183.33 | 21.310 ms | 26.485 ms | 0 |
| sync | STRING set | 4 | 3 | 357.00 | 21.497 ms | 22.386 ms | 0 |
| sync | TemporalAggregate incr | 1 | 3 | 103.00 | 14.760 ms | 18.291 ms | 0 |
| sync | TemporalAggregate incr | 2 | 3 | 193.67 | 19.201 ms | 20.001 ms | 0 |
| sync | TemporalAggregate incr | 4 | 3 | 352.00 | 21.373 ms | 23.754 ms | 0 |
| async | STRING set | 1 | 3 | 1,217.00 | 3.495 ms | 4.270 ms | 0 |
| async | STRING set | 2 | 3 | 2,298.67 | 2.918 ms | 4.811 ms | 0 |
| async | STRING set | 4 | 3 | 2,159.67 | 5.791 ms | 28.168 ms | 0 |
| async | TemporalAggregate incr | 1 | 3 | 1,203.33 | 3.068 ms | 9.080 ms | 0 |
| async | TemporalAggregate incr | 2 | 3 | 2,096.00 | 4.397 ms | 4.468 ms | 0 |
| async | TemporalAggregate incr | 4 | 3 | 3,152.00 | 4.837 ms | 4.874 ms | 0 |

## Per-Run Data

### `storage_async=false`

| Threads | Repeat | STRING set QPS | STRING set p99 | Aggregate incr QPS | Aggregate incr p99 |
|---:|---:|---:|---:|---:|---:|
| 1 | 1 | 103 | 14.317 ms | 103 | 18.291 ms |
| 2 | 1 | 210 | 17.773 ms | 209 | 18.026 ms |
| 4 | 1 | 397 | 18.437 ms | 378 | 19.549 ms |
| 1 | 2 | 102 | 15.631 ms | 104 | 14.599 ms |
| 2 | 2 | 178 | 21.310 ms | 188 | 19.201 ms |
| 4 | 2 | 331 | 22.386 ms | 335 | 23.754 ms |
| 1 | 3 | 99 | 18.182 ms | 102 | 14.760 ms |
| 2 | 3 | 162 | 26.485 ms | 184 | 20.001 ms |
| 4 | 3 | 343 | 21.497 ms | 343 | 21.373 ms |

### `storage_async=true`

| Threads | Repeat | STRING set QPS | STRING set p99 | Aggregate incr QPS | Aggregate incr p99 |
|---:|---:|---:|---:|---:|---:|
| 1 | 1 | 1,693 | 2.406 ms | 1,729 | 1.840 ms |
| 2 | 1 | 2,949 | 2.297 ms | 3,003 | 2.097 ms |
| 4 | 1 | 1,458 | 28.168 ms | 4,232 | 4.015 ms |
| 1 | 2 | 1,130 | 3.495 ms | 1,306 | 3.068 ms |
| 2 | 2 | 2,244 | 2.918 ms | 1,668 | 4.397 ms |
| 4 | 2 | 2,680 | 4.967 ms | 2,771 | 4.837 ms |
| 1 | 3 | 828 | 4.270 ms | 575 | 9.080 ms |
| 2 | 3 | 1,703 | 4.811 ms | 1,617 | 4.468 ms |
| 4 | 3 | 2,341 | 5.791 ms | 2,453 | 4.874 ms |

## Interpretation

`storage_async=true` materially improves write throughput on the EFS-backed shared-file path:

- STRING set QPS improved roughly 6x to 13x depending on thread count.
- TemporalAggregate increment QPS improved roughly 11x at 1 thread, 10x at 2 threads, and 9x at 4 threads.
- Write p99 dropped from roughly 15-26 ms in sync mode to mostly 2-6 ms in async mode, with one STRING 4-thread outlier at 28.168 ms.

This strongly suggests current sync-mode write latency is dominated by durable shared-store commit cost, especially EFS write/fsync behavior.

## Durability Caveat

`storage_async=true` is not equivalent to the conservative durable mode. It can return before the same durable boundary as `storage_async=false`, so it may trade lower write latency for a larger RPO window if a node dies before async persistence completes.

Recommended next step:

- Keep `storage_async=false` as the default production-safe setting.
- Add bounded async profiles with explicit RPO wording:
  - low-latency: 1 ms / 256 KB
  - default async: 2 ms / 512 KB
  - throughput: 5 ms / 1 MB
  - batch ingest: 10-50 ms / 4 MB
- Add crash/restart tests for each profile to measure actual lost-op window and replay behavior.

## Notes

An earlier accidental high-concurrency run treated the thread list as one `124`-thread case. That run also showed async much faster, but it is not used as the main result because the cluster data nodes are only 2 vCPU each.

The comparison script now avoids that issue by using its default thread array unless explicitly passed as a proper PowerShell array.
