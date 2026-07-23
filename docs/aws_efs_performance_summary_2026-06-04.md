# TemporalStore AWS/EFS Performance Summary

Date: 2026-06-04

This document summarizes the AWS scale-test performance data for the small EFS-backed TemporalStore cluster. These are end-to-end service numbers, not raw EFS filesystem microbenchmarks.

## Test Cluster

| Component | Shape |
| --- | --- |
| Region | `us-west-2` |
| Topology | 1 metaserver/proxy/UI node, 2 data nodes |
| Metaserver/proxy/UI | `t3.small`, also ran client benchmarks |
| Data nodes | `c7i.large`, 2 vCPU each |
| Shared storage | EFS mounted at `/mnt/temporalstore-shared` |
| Data-node cache disk | 10 GB gp3 EBS mounted at `/mnt/temporalstore-cache` |
| Table | `ns1/table1` |
| Storage URI | `efs:///mnt/temporalstore-shared/aws-scale/storage/` |
| Primary | `data-01`, `10.70.1.163:17001` |
| Secondary | `data-02`, `10.70.1.202:17002` |
| Runtime | mtcache enabled, SSD cache enabled, shared-file/EFS storage |

Important context:

- The metaserver/proxy/UI node also ran the benchmark clients, so metaserver CPU includes client load.
- Data nodes were small 2-vCPU instances. The `1/2/4/8` client-thread runs are the useful operating range for this shape.
- Writes are durable through the EFS/shared-file path, so write latency is much higher than in-memory read/query latency.

## Concurrent EFS-Backed STRING SET/GET

STRING write and read workloads were run concurrently from the metaserver node.

| Threads per workload | SET QPS | SET avg | SET p50 | SET p95 | SET p99 | GET QPS | GET avg | GET p50 | GET p95 | GET p99 |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 87/s | 11.392 ms | 10.285 ms | 18.720 ms | 23.073 ms | 389/s | 2.564 ms | 0.361 ms | 11.000 ms | 13.067 ms |
| 2 | 178/s | 11.194 ms | 9.982 ms | 18.424 ms | 22.461 ms | 754/s | 2.647 ms | 0.390 ms | 10.421 ms | 16.245 ms |
| 4 | 382/s | 10.397 ms | 9.244 ms | 16.814 ms | 19.725 ms | 1,386/s | 2.881 ms | 0.486 ms | 10.195 ms | 16.246 ms |
| 8 | 605/s | 13.104 ms | 10.937 ms | 21.592 ms | 30.072 ms | 1,640/s | 4.864 ms | 0.572 ms | 18.655 ms | 22.273 ms |

Interpretation:

- Durable EFS-backed writes are around `10-14 ms` average and `20-30 ms` p99 on this small cluster.
- STRING reads are often sub-millisecond at p50, but p95/p99 can rise under concurrent writes because of queueing and visibility/retry effects.
- `2-4` threads per workload is the most honest range for 2-vCPU data nodes.

## Concurrent TemporalAggregate INCR/QUERY

TemporalAggregate write and query workloads were run concurrently from the metaserver node.

| Threads per workload | INCR QPS | INCR avg | INCR p50 | INCR p95 | INCR p99 | Query QPS | Query avg | Query p50 | Query p95 | Query p99 |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 88/s | 11.266 ms | 10.272 ms | 18.363 ms | 22.787 ms | 2,553/s | 0.391 ms | 0.362 ms | 0.541 ms | 0.764 ms |
| 2 | 180/s | 11.054 ms | 9.838 ms | 18.373 ms | 22.523 ms | 4,918/s | 0.405 ms | 0.377 ms | 0.582 ms | 0.855 ms |
| 4 | 388/s | 10.295 ms | 9.262 ms | 16.646 ms | 19.850 ms | 6,250/s | 0.639 ms | 0.426 ms | 1.947 ms | 3.219 ms |
| 8 | 581/s | 13.742 ms | 11.489 ms | 22.335 ms | 29.541 ms | 12,371/s | 0.641 ms | 0.583 ms | 1.104 ms | 1.583 ms |

Interpretation:

- TemporalAggregate writes have similar EFS durability cost to STRING writes.
- TemporalAggregate queries stay much faster than writes because the read path uses in-memory/indexed aggregate state after the data is visible.
- On this small cluster, aggregate query p99 was usually sub-millisecond to a few milliseconds in the low-thread range.

## Baseline Data-Type Results

| Test | Write QPS | Read/Query QPS | Avg read/query | P95 | P99 | Errors | Notes |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| STRING primary | 180 set/s | 4,062 get/s | 489 us | 1,641 us | 3,566 us | 0 | 20k writes, 20k reads |
| STRING secondary | 181 set/s | 5,181 get/s | 383 us | 546 us | 1,055 us | 0 | Replica reads visible without retry penalty |
| Sequence primary, window 100 | 124 ingest ops/s | 3,603 q/s | 540 us | 1,921 us | 3,494 us | 0 | 16k rows ingested |
| Sequence primary, complex full window | n/a | 511 q/s | 3,879 us | 6,180 us | 8,144 us | 0 | 1.3M points returned |
| Sequence secondary, window 100 | 113 ingest ops/s | 3,898 q/s | 508 us | 824 us | 2,058 us | 0 | Secondary read path |
| Sequence secondary, complex full window | n/a | 671 q/s | 2,970 us | 4,442 us | 6,523 us | 0 | Secondary read path |
| TemporalAggregate primary | 186 incr/s | 2,336 q/s | 853 us | 2,881 us | 5,773 us | 0 | 1k features x 12 buckets |
| TemporalAggregate secondary | 188 incr/s | 2,597 q/s | 757 us | 2,657 us | 4,070 us | 0 | Replica lag probe around 1 ms |
| Concurrent STRING | 171 set/s | 4,228 get/s | 469 us | 1,125 us | 2,876 us | 0 | Overlapped with aggregate workload |
| Concurrent TemporalAggregate | 173 incr/s | 263 q/s | 7,585 us | 15,355 us | 18,912 us | 0 | Query latency rose during mixed workload |

## Sequence / Long-Window Feature Query Latency

The sequence benchmark used 64 keys, 4,000 rows per key, 6,000 query ops per case, and 2 client threads.

### Tiny DRAM + SSD Blockcache

| Query shape | QPS | Avg | P50 | P95 | P99 |
| --- | ---: | ---: | ---: | ---: | ---: |
| Ingest 64 keys x 4,000 rows | 52/s | 37.659 ms | 37.224 ms | 48.148 ms | 56.015 ms |
| Window 100, no filter | 4,118/s | 0.483 ms | 0.432 ms | 0.651 ms | 2.053 ms |
| Window 1000, `action = 3` | 1,279/s | 1.559 ms | 1.426 ms | 2.475 ms | 4.598 ms |
| Window 1000, complex filters | 596/s | 3.348 ms | 3.048 ms | 5.039 ms | 7.989 ms |
| Full-window complex filters | 165/s | 12.109 ms | 11.791 ms | 14.707 ms | 16.990 ms |

### Blockcache Disabled / EFS-Heavy Comparison

| Query shape | QPS | Avg | P50 | P95 | P99 |
| --- | ---: | ---: | ---: | ---: | ---: |
| Ingest 64 keys x 4,000 rows | 50/s | 38.735 ms | 37.345 ms | 49.856 ms | 54.884 ms |
| Window 100, no filter | 3,893/s | 0.507 ms | 0.430 ms | 0.721 ms | 2.414 ms |
| Window 1000, `action = 3` | 1,231/s | 1.620 ms | 1.431 ms | 3.053 ms | 4.881 ms |
| Window 1000, complex filters | 554/s | 3.602 ms | 3.032 ms | 6.016 ms | 11.312 ms |
| Full-window complex filters | 168/s | 11.852 ms | 11.664 ms | 14.008 ms | 16.489 ms |

Interpretation:

- Short-window and filtered sequence queries are fast enough for online serving in this small test.
- Full-window complex filtering is naturally more expensive because it scans more rows and evaluates more predicates.
- This test proves the server runs with tiny DRAM and SSD cache enabled, but it does not yet prove query payloads are being served from SSD. The benchmark ingested and queried in the same process lifetime, so many objects likely remained live in memory.

## Replication Observations

| Test | Writes | Writer threads | Ingest time | Secondary visibility |
| --- | ---: | ---: | ---: | --- |
| TemporalAggregate 1k features x 12 buckets | 12,000 | 2 | 67.5 s | 0 errors, visible at first poll |
| TemporalAggregate 10k features x 12 buckets | 120,000 | 2 | 683.2 s | 0 errors, visible at first poll |

Measured post-ingest lag:

| Metric | Lag |
| --- | ---: |
| p50 | 0 ms |
| p95 | 0 ms |
| p99 | 0 ms |
| max | 0 ms |

Caveat:

- The `0 ms` lag result means the secondary was caught up by the time the client finished ingest and started polling.
- It does not prove continuous lag is zero during heavy writes. A stronger test should poll secondary while writes are still running.

## CPU During Low-Thread Concurrent Sweep

| Node | Avg CPU | Max CPU |
| --- | ---: | ---: |
| `data-01` primary | 16.49% | 23.34% |
| `data-02` secondary | 9.47% | 11.52% |
| `meta-01` metaserver/proxy/client/UI | 35.28% | 42.11% |

Interpretation:

- Data-node CPU was not saturated in the low-thread run.
- The metaserver node had extra CPU pressure because it also ran proxy, UI, and benchmark clients.
- Cleaner product measurements should use a separate benchmark/client node.

## Practical Takeaways

1. **Write path bottleneck:** EFS-backed durable writes are the limiting side on this small cluster, around `10-14 ms` avg and `20-30 ms` p99.
2. **Read/query path strength:** In-memory/indexed aggregate queries are much faster, with low-thread p99 mostly sub-millisecond to a few milliseconds.
3. **Temporal features:** Sequence windows and TemporalAggregate queries show the strongest product fit: high-cardinality online feature serving, risk/fraud windows, frequency caps, and filtered temporal state.
4. **Secondary reads:** Secondary reads worked in these runs, but should still be treated as eventually consistent until continuous-lag testing under concurrent writes is stronger.
5. **Next benchmark:** Run a true mixed benchmark process that writes and polls secondary at the same time, plus a restart/cold-read test to isolate SSD cache versus EFS.

## Raw Artifacts

Local raw artifacts:

- `infra/aws/temporalstore-test/ssm-run-scale-tests-result.json`
- `infra/aws/temporalstore-test/ssm-run-low-thread-concurrent-sweep-result.json`
- `infra/aws/temporalstore-test/cache-tier-latency-summary.txt`
- `infra/aws/temporalstore-test/collect-data-01.txt`
- `infra/aws/temporalstore-test/collect-data-02.txt`
- `infra/aws/temporalstore-test/collect-meta-01.txt`
- `infra/aws/temporalstore-test/collect-low-thread-data-01.txt`
- `infra/aws/temporalstore-test/collect-low-thread-data-02.txt`
- `infra/aws/temporalstore-test/collect-low-thread-meta-01.txt`

Related summary doc:

- `docs/aws_scale_replication_results_2026-06-04.md`
