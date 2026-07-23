# AWS ByteKV, ABase, and TemporalStore Latency Comparison

Date: 2026-06-05

## Scope

This compares the baseline AWS numbers we have for ByteKV, ABase, and TemporalStore on the same reused test cluster.

Cluster:

| Role | Instance | Type |
| --- | --- | --- |
| meta / client / proxy | `i-003c930417f7ee609` | `t3.small` |
| data01 | `i-0724d90b323786546` | `c7i.large`, 2 vCPU |
| data02 | `i-096334bd8cc7ab259` | `c7i.large`, 2 vCPU |

Important: these are small-instance baseline numbers, not max-capacity product claims. The client ran from the meta node. TemporalStore used EFS/shared-file durable storage in the AWS run, so its write path is intentionally more durable and slower than an in-memory/local-only cache path.

## Direct Baseline Results

The closest common test is plain string/KV read and write.

| System | Mode | Threads | Ops | Read ops | Write ops | QPS | p50 | p95 | p99 | Max |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| ByteKV | read | 1 | 5,000 | 5,000 | 0 | 1,200/s | 0.689 ms | 1.717 ms | 3.054 ms | 7.163 ms |
| ABase | read | 1 | 5,000 | 5,000 | 0 | 868/s | 1.045 ms | 1.718 ms | 2.996 ms | 6.720 ms |
| TemporalStore | read, primary | 2 | 20,000 | 20,000 | 0 | 4,062/s | 0.336 ms | 1.641 ms | 3.566 ms | 12.219 ms |
| TemporalStore | read, secondary | 2 | 20,000 | 20,000 | 0 | 5,181/s | 0.337 ms | 0.546 ms | 1.055 ms | 9.384 ms |

Write-only:

| System | Mode | Threads | Ops | QPS | avg | p50 | p95 | p99 | Max |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| ByteKV | write | 2 | 10,000 | 456/s | n/a | 4.049 ms | 6.577 ms | 8.819 ms | 29.485 ms |
| ABase | write | 2 | 5,000 | 1,016/s | n/a | 1.418 ms | 4.355 ms | 12.074 ms | 42.153 ms |
| TemporalStore | STRING set, EFS durable | 2 | 20,000 | 180/s | 11.039 ms | 9.988 ms | 18.197 ms | 22.203 ms | 69.230 ms |

Mixed read/write:

| System | Mode | Threads | Ops | Reads | Writes | QPS | p50 | p95 | p99 | Max |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| ByteKV | mixed | 2 | 20,000 | 10,000 | 10,000 | 766/s | 3.018 ms | 5.461 ms | 7.413 ms | 14.930 ms |
| ABase | mixed | 2 | 10,000 | 5,000 | 5,000 | 1,421/s | 1.287 ms | 2.137 ms | 2.956 ms | 10.398 ms |
| TemporalStore | concurrent STRING set/get | 2 + 2 | 20,000 write + 20,000 read | 20,000 | 20,000 | 169 set/s + 790 get/s | set 10.265 ms / get 0.387 ms | set 19.386 ms / get 10.663 ms | set 23.745 ms / get 16.622 ms | set 225.024 ms / get 24.992 ms |

## TemporalStore Temporal Feature Results

TemporalStore should not be judged only by plain STRING because its differentiator is high-cardinality temporal/windowed feature serving.

TemporalAggregate baseline, 2 threads:

| Path | Phase | Ops | QPS | avg | p50 | p95 | p99 | Max |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| primary | aggregate incr | 12,000 | 186/s | 10.711 ms | 9.712 ms | 17.190 ms | 22.275 ms | 44.154 ms |
| primary | aggregate query | 1,000 | 2,336/s | 0.853 ms | 0.486 ms | 2.881 ms | 5.773 ms | 6.940 ms |
| secondary | aggregate incr | 12,000 | 188/s | 10.588 ms | 9.735 ms | 17.070 ms | 21.164 ms | 55.763 ms |
| secondary | aggregate query | 1,000 | 2,597/s | 0.757 ms | 0.336 ms | 2.657 ms | 4.070 ms | 6.909 ms |

TemporalStore concurrent STRING plus TemporalAggregate, 2 threads per workload:

| Workload | Write QPS | Read/query QPS | Write p99 | Read/query p99 |
| --- | ---: | ---: | ---: | ---: |
| STRING set/get | 171/s | 4,228/s | 23.926 ms | 2.876 ms |
| TemporalAggregate incr/query | 173/s | 263/s | 23.605 ms | 18.912 ms |

Low-thread concurrent sweep:

| Threads per workload | STRING write QPS | STRING get QPS | STRING get p99 | Aggregate write QPS | Aggregate query QPS | Aggregate query p99 |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 87/s | 389/s | 13.067 ms | 88/s | 2,553/s | 0.764 ms |
| 2 | 178/s | 754/s | 16.245 ms | 180/s | 4,918/s | 0.855 ms |
| 4 | 382/s | 1,386/s | 16.246 ms | 388/s | 6,250/s | 3.219 ms |
| 8 | 605/s | 1,640/s | 22.273 ms | 581/s | 12,371/s | 1.583 ms |

## Interpretation

For plain KV on this cluster:

- ABase has the best write and mixed-write baseline among the three measured systems.
- ByteKV has acceptable read p99 and stronger transaction/MVCC semantics, but write throughput in this small test was lower than ABase.
- TemporalStore STRING writes are much slower because this run writes durable oplog/page/index streams through EFS/shared-file storage. That is not the right path to use as a pure Redis-like in-memory cache benchmark.

For TemporalStore's target use case:

- TemporalAggregate queries are fast after data is ingested: p99 around `4-6 ms` in baseline primary/secondary tests.
- TemporalStore gives one online serving engine for raw temporal events, long sequence features, frequency/risk windows, and aggregate queries. ABase and ByteKV do not expose this same native temporal aggregate/query model in the measured tests.
- Write-side latency is currently dominated by durable shared storage. If we want TemporalStore to compete on plain KV write latency, we need to test local primary-pull mode, async storage, larger instances, or NVMe-backed durability separately.

## Current Ranking From These Numbers

Plain KV read:

1. TemporalStore secondary read: best p99 in the measured rows, but 2-thread test and replica visibility assumptions apply.
2. ABase and ByteKV: similar p99 around `3 ms`.
3. TemporalStore primary read: high QPS, p99 around `3.6 ms`.

Plain KV write:

1. ABase: `1,016/s`, p99 `12.074 ms`.
2. ByteKV: `456/s`, p99 `8.819 ms`.
3. TemporalStore EFS durable STRING: `180/s`, p99 `22.203 ms`.

Temporal/windowed feature serving:

1. TemporalStore is the only one measured with native temporal aggregate and sequence feature workloads.
2. ABase/ByteKV would need either external aggregation, application-side logic, or a custom model/API before a fair temporal feature comparison.

## What To Test Next For A Fairer Comparison

Run one benchmark harness with the same:

- thread counts: `1, 2, 4, 8`
- operation mix: read-only, write-only, 50/50 mixed
- value size: `128 B`, `1 KB`
- durability mode clearly labeled:
  - in-memory / no fsync
  - local NVMe
  - EFS/shared storage
  - async vs sync write
- client placement: same meta/client node or a dedicated client node

For TemporalStore specifically, add:

- primary-pull local-file mode write/read benchmark
- shared-store async mode benchmark
- EFS sync mode benchmark
- native TemporalAggregate benchmark beside plain STRING
