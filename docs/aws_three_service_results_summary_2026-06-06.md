# AWS Three-Service Results Summary - 2026-06-06

This document consolidates the AWS results collected so far for TemporalStore,
ByteKV, and ABase on the shared one-cluster test setup.

## Cluster

| Role | Instance | Private IP | Type |
|---|---|---|---|
| meta / client / proxy / UI | `i-003c930417f7ee609` | `10.70.1.79` | `t3.small` |
| data01 | `i-0724d90b323786546` | `10.70.1.163` | `c7i.large` |
| data02 | `i-096334bd8cc7ab259` | `10.70.1.202` | `c7i.large` |

All three systems reuse this cluster. The client load runs from `meta-01`, so
the numbers are useful for relative testing and abnormality detection, not
maximum product claims.

## Detailed Docs

| Doc | Coverage |
|---|---|
| `docs/aws_scale_replication_results_2026-06-04.md` | TemporalStore scale, replication, cache tier, feature sequence, TemporalAggregate, CPU |
| `docs/aws_efs_performance_summary_2026-06-04.md` | TemporalStore EFS/shared-file behavior |
| `docs/aws_bytekv_abase_one_hour_soak_2026-06-06.md` | ByteKV and ABase one-hour soak results |
| `docs/aws_one_cluster_binary_topology.md` | Binary placement, ports, and storage topology |
| `docs/aws_one_cluster_test_index_2026-06-06.md` | Index of AWS test documents |
| `../BYTEKV/docs/aws_scale_latency_2026-06-06.md` | ByteKV scale latency notes |
| `../../docs/AWS_BYTEKV_ABASE_TEMPORALSTORE_LATENCY_COMPARISON_2026-06-05.md` | Earlier cross-system latency comparison |

## One-Hour Soak Summary

| System | Time window UTC | Workload | Result | Main abnormality |
|---|---|---|---|---|
| TemporalStore | `04:02:08` to `05:02:20` | concurrent STRING set/get, TemporalAggregate incr/query, secondary visibility probes | all client loops exited `0`; no error log | summary parser originally mixed aggregate columns; corrected rollup below |
| ByteKV | `05:34:39` to `06:34:55` | 87 repeated 20k-op 50/50 read/write benchmark rows | benchmark operation errors `0` | client process crashed after successful rows: 20 segfaults, 1 abort |
| ABase | `06:46:36` to `07:46:42` | direct 5-key WriteBatch + 5-key MultiGet loop | errors `0`; no client crashes | Redis/RESP proxy path was unavailable; direct path only |

Remote raw result directories:

```text
/var/lib/temporalstore/soak_20260606T040208Z
/var/lib/bytekv/soak_20260606T053439Z
/var/lib/abase/soak_20260606T064636Z
```

## TemporalStore One-Hour Soak

Runtime:

| Field | Value |
|---|---|
| Metaserver | `10.70.1.79:17000` |
| Table | `ns1/table1` |
| STRING loop | 1,000 set ops plus 1,000 get ops per iteration, 2 threads, 256 B values |
| Aggregate loop | 200 features x 100 keys x 12 buckets, 2 threads |
| Lag loop | secondary visibility probes with background write/read threads |

Corrected STRING rollup:

| Phase | Rows | Ops | Errors | Avg QPS | Avg p50 | Avg p95 | Avg p99 |
|---|---:|---:|---:|---:|---:|---:|---:|
| set | 322 | 322,000 | 0 | 174.7/s | 9.873 ms | 18.412 ms | 21.897 ms |
| get raw success | 322 | 322,000 | 0 | 235.2/s | 9.136 ms | 16.573 ms | 20.068 ms |
| get visibility retry | 322 | 322,000 | 0 | 235.2/s | 9.138 ms | 16.575 ms | 20.071 ms |

Corrected TemporalAggregate rollup:

| Phase | Rows | Ops | Errors | Avg QPS | Avg p50 | Avg p95 | Avg p99 |
|---|---:|---:|---:|---:|---:|---:|---:|
| incr | 250 | 600,000 | 0 | 177.5/s | 9.839 ms | 18.244 ms | 21.914 ms |
| query | 250 | 50,000 | 0 | 311.6/s | 8.452 ms | 13.871 ms | 16.473 ms |

Exit counts:

| Loop | Exit code `0` count |
|---|---:|
| STRING | 322 |
| TemporalAggregate | 250 |
| lag probe | 302 |

Interpretation:

- The one-hour TemporalStore soak did not report operation errors or client loop crashes.
- Read/query latency was higher than the earlier focused scale tests because three workloads ran at the same time on the same small meta/client node.
- Earlier TemporalStore focused scale results are still the better source for per-data-model latency under controlled load.

## TemporalStore Focused Scale Results

From `docs/aws_scale_replication_results_2026-06-04.md`:

| Test | Write QPS | Read/query QPS | P99 | Errors | Notes |
|---|---:|---:|---:|---:|---|
| STRING primary | 180 set/s | 4,062 get/s | 3.566 ms | 0 | 20k writes, 20k reads |
| STRING secondary | 181 set/s | 5,181 get/s | 1.055 ms | 0 | replica read path |
| Sequence primary, window 100 | 124 ingest/s | 3,603 q/s | 3.494 ms | 0 | 16k rows ingested |
| Sequence primary, complex full window | n/a | 511 q/s | 8.144 ms | 0 | 1.3M points returned |
| TemporalAggregate primary | 186 incr/s | 2,336 q/s | 5.773 ms | 0 | 1k features x 12 buckets |
| TemporalAggregate secondary | 188 incr/s | 2,597 q/s | 4.070 ms | 0 | replica lag probe 1 ms |
| TemporalAggregate lag sweep | 181 incr/s | first poll visible | 0 ms reported lag | 0 | 10k features, 120k writes |
| Concurrent STRING | 171 set/s | 4,228 get/s | 2.876 ms read | 0 | overlapped with aggregate |
| Concurrent TemporalAggregate | 173 incr/s | 263 q/s | 18.912 ms query | 0 | query latency rose under mixed load |

## ByteKV One-Hour Soak

Runtime:

| Field | Value |
|---|---|
| Master | `10.70.1.79:26010` |
| TSO | `10.70.1.79:26020` |
| Namespace | `aws_soak` |
| Workload | repeated 20,000-op 50/50 read/write benchmark, 2 threads, 128 B values |

Summary:

| Metric | Value |
|---|---:|
| Benchmark rows | 87 |
| Total ops | 1,740,000 |
| Reads | 870,000 |
| Writes | 870,000 |
| Benchmark operation errors | 0 |
| Avg QPS | 706.5/s |
| Min QPS | 640.1/s |
| Max QPS | 787.5/s |
| Avg p50 | 3.207 ms |
| Avg p95 | 5.987 ms |
| Avg p99 | 8.891 ms |
| Worst max latency | 397.834 ms |

Exit codes:

| Exit code | Count | Meaning |
|---:|---:|---|
| `0` | 66 | clean benchmark process exit |
| `139` | 20 | segfault after benchmark completion |
| `134` | 1 | abort after benchmark completion |

Interpretation:

- ByteKV services stayed healthy.
- Every benchmark row printed `PASS` and reported `0` operation errors.
- The abnormality is the benchmark client cleanup path, not a proven service-side write/read failure.
- Fix `bytekv_aws_smoke_soak` cleanup before using process exit code as a reliability signal.

## ABase One-Hour Soak

Runtime:

| Field | Value |
|---|---|
| Master | `10.70.1.79:19074` |
| Table | `aws_scale/bench` |
| Client path | direct master metadata lookup plus datanode `WriteBatch` / `MultiGet` |
| Threads | 2 |
| Batch size | 5 keys |

Summary:

| Metric | Value |
|---|---:|
| Batch rows | 205,686 |
| Key pairs written and read | 1,028,430 |
| Errors | 0 |
| Key pairs per second | 285.675/s |

Write latency per 5-key batch:

| Metric | Value |
|---|---:|
| Avg | 17.237 ms |
| p50 | 16.054 ms |
| p95 | 25.069 ms |
| p99 | 34.525 ms |
| Max | 1022.254 ms |

Read latency per 5-key batch:

| Metric | Value |
|---|---:|
| Avg | 17.212 ms |
| p50 | 16.058 ms |
| p95 | 25.008 ms |
| p99 | 34.370 ms |
| Max | 1043.537 ms |

Interpretation:

- ABase direct path completed the hour with `0` errors.
- Rare max latency spikes reached about 1 second, but p99 stayed around 34 ms for 5-key batches.
- Redis/RESP proxy path did not work in preflight. Ports `19077` and `19078` were live, but `redis-cli PING` timed out or closed the connection. Proxy metadata reports `ABASE2_THRIFT_PROTOCOL`, so ABase proxy testing needs the matching Thrift client or a RESP-compatible proxy mode.

## Cross-System Reading

| System | Strongest result from these runs | Main risk found |
|---|---|---|
| TemporalStore | Native temporal/sequence/aggregate serving works with secondary visibility and no soak loop crashes | EFS durable write path limits write QPS on small nodes; proxy FeatureQuery still needs debugging |
| ByteKV | Mixed KV workload completed 1.74M ops with `0` benchmark op errors | benchmark client cleanup segfault/abort after successful rows |
| ABase | Direct path ran one hour with `0` errors | Redis-compatible proxy path not available in current deployment |

## Recommended Next Runs

- TemporalStore: rerun one-hour soak with a cleaner single-process harness and data-node CPU sampling.
- TemporalStore: repeat shared-store versus async/local durability modes on the same workload.
- ByteKV: fix cleanup crash in the smoke client, then rerun the same one-hour soak.
- ByteKV: add a reuse-one-table mode to reduce table creation overhead across soak iterations.
- ABase: identify the correct `ABASE2_THRIFT_PROTOCOL` proxy client and run a proxy-path soak.
- ABase: collect data-node CPU/memory samples during direct-path load.
