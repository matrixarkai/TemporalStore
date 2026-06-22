# TemporalStore AWS Scale And Replication Test Results

Date: 2026-06-04

## Cluster

- Region: `us-west-2`
- Topology: 1 metaserver/proxy/UI node, 2 data nodes
- Metaserver/proxy/UI: `t3.small`, private IP `10.70.1.79`, public UI `http://54.185.179.199:8088/`
- Data nodes: `c7i.large`, 2 vCPU each
- Data node EBS cache volume: 10 GB gp3 per data node, mounted at `/mnt/temporalstore-cache`
- Shared storage: EFS `fs-04c6e37f04543e37d`, mounted at `/mnt/temporalstore-shared`
- Table: `ns1/table1`, storage URI `efs:///mnt/temporalstore-shared/aws-scale/storage/`
- Placement: primary on `data-01` (`10.70.1.163:17001`), secondary on `data-02` (`10.70.1.202:17002`)
- Runtime flags: mtcache enabled, SSD cache enabled, EFS shared-file storage, 256 MB zone/blob size, 2-thread client tests

AWS resources were intentionally left running for later testing.

## Test Coverage

The run covered these data paths:

- Direct SDK module smoke: STRING, COMMON TTL/delete, HASH, SET, FEATURE time sequence, IPS, RISK window count, TemporalAggregate count/sum/min/max
- Proxy path smoke: STRING and HASH passed; proxy FeatureQuery returned 0 points and is a known issue to debug
- Replication smoke: secondary read matched after 1 attempt, 0 ms
- STRING scale: primary and secondary read paths
- Sequence/feature scale: 16 keys x 1000 rows/key with no filter, equality filter, complex filters, and full-window complex filters
- TemporalAggregate scale: 1000 features x 12 buckets, primary and secondary reads
- TemporalAggregate replication lag sweep: 10,000 features x 12 buckets, 120,000 aggregate writes
- Concurrent overlap: STRING set/get and TemporalAggregate incr/query running at the same time

## Key Results

| Test | Write QPS | Read/Query QPS | Avg Read | P95 Read | P99 Read | Errors | Notes |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| STRING primary | 180 set/s | 4,062 get/s | 489 us | 1,641 us | 3,566 us | 0 | 20k writes, 20k reads |
| STRING secondary | 181 set/s | 5,181 get/s | 383 us | 546 us | 1,055 us | 0 | Replica reads visible without retry penalty |
| Sequence primary, window 100 | 124 ingest ops/s | 3,603 q/s | 540 us | 1,921 us | 3,494 us | 0 | 16k rows ingested |
| Sequence primary, complex full window | - | 511 q/s | 3,879 us | 6,180 us | 8,144 us | 0 | 1.3M points returned |
| Sequence secondary, window 100 | 113 ingest ops/s | 3,898 q/s | 508 us | 824 us | 2,058 us | 0 | Secondary read path |
| Sequence secondary, complex full window | - | 671 q/s | 2,970 us | 4,442 us | 6,523 us | 0 | Secondary read path |
| TemporalAggregate primary | 186 incr/s | 2,336 q/s | 853 us | 2,881 us | 5,773 us | 0 | 1k features x 12 buckets |
| TemporalAggregate secondary | 188 incr/s | 2,597 q/s | 757 us | 2,657 us | 4,070 us | 0 | Replica lag probe: 1 ms |
| TemporalAggregate lag sweep | 181 incr/s | first poll visible | - | - | - | 0 | 10k features, 120k writes, all visible at 0 ms poll |
| Concurrent STRING | 171 set/s | 4,228 get/s | 469 us | 1,125 us | 2,876 us | 0 | Overlapped with aggregate workload |
| Concurrent TemporalAggregate | 173 incr/s | 263 q/s | 7,585 us | 15,355 us | 18,912 us | 0 | Query latency rose during mixed workload |

## Low-Thread Concurrent Read/Write QPS Sweep

After the baseline run, a focused low-thread overlap sweep was run with STRING SET/GET and TemporalAggregate INCR/QUERY running at the same time from the metaserver node. The data nodes are only `c7i.large` with 2 vCPU each, so these `1,2,4,8` thread numbers are the honest operating range. The mixed benchmark binary was not available yet, so this used two existing benchmark processes started together. This is a practical overlap test, not a perfect single-process closed-loop benchmark.

| Client threads per workload | STRING write QPS | STRING get QPS | STRING get p99 | Aggregate write QPS | Aggregate query QPS | Aggregate query p99 | Status |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| 1 | 87 | 389 | 13.067 ms | 88 | 2,553 | 0.764 ms | passed |
| 2 | 178 | 754 | 16.245 ms | 180 | 4,918 | 0.855 ms | passed |
| 4 | 382 | 1,386 | 16.246 ms | 388 | 6,250 | 3.219 ms | passed |
| 8 | 605 | 1,640 | 22.273 ms | 581 | 12,371 | 1.583 ms | passed |

Low-thread interpretation:

- Recommended honest range for this 2-vCPU data-node shape: `2-4` threads per workload.
- `8` threads is still useful, but STRING read p99 rose above `22 ms`, so it is already showing queueing.
- In the honest low-thread range, the system delivered about `178-382 STRING writes/s` plus `180-388 aggregate writes/s` concurrently, while also serving `754-1,386 STRING gets/s` and `4,918-6,250 aggregate queries/s`.
- The write path is EFS/shared-file durable, so write QPS is the limiting side. Reads and aggregate queries remain much faster than writes.

Prior overload-only reference:

- 32 threads: about `2.0k STRING set/s` plus `2.0k aggregate incr/s`, aggregate query p99 about `5.0 ms`.
- 64 threads: about `3.6k STRING set/s` plus `3.6k aggregate incr/s`, but aggregate query p99 rose to `15.2 ms`.
- Those `32/64` thread numbers are ceiling/stress data only and should not be used as product claims for 2-vCPU nodes.

The writes are slow in this setup because every write is durable through EFS/shared-file storage on small instances. Reads and temporal queries are much faster once data is visible.

## Replication

- Basic secondary read: pass, 0 ms, 1 attempt.
- TemporalAggregate secondary scale: pass, replica lag probe `1 ms`.
- TemporalAggregate 10k lag sweep: pass, `10,000/10,000` features visible at the first poll after ingest; p50/p95/p99/max lag all reported `0 ms`.
- Earlier instability around aggregate replay did not reproduce in this run with the latest shared-file condition changes and 2-thread load.

## CPU

CloudWatch CPU over the run window:

| Node | Avg CPU | Max CPU |
| --- | ---: | ---: |
| `data-01` primary | 17.15% | 23.78% |
| `data-02` secondary | 9.14% | 10.97% |
| `meta-01` metaserver/proxy/client/UI | 39.65% | 47.04% |

CloudWatch CPU during the concurrent sweep:

| Node | Avg CPU | Max CPU |
| --- | ---: | ---: |
| `data-01` primary | 19.95% | 27.96% |
| `data-02` secondary | 10.07% | 12.28% |
| `meta-01` metaserver/proxy/client/UI | 41.44% | 47.27% |

CloudWatch CPU during the low-thread concurrent sweep:

| Node | Avg CPU | Max CPU |
| --- | ---: | ---: |
| `data-01` primary | 16.49% | 23.34% |
| `data-02` secondary | 9.47% | 11.52% |
| `meta-01` metaserver/proxy/client/UI | 35.28% | 42.11% |

Process samples after the run:

- `data-01` server: around 20% process CPU, RSS about 200 MB after the run
- `data-02` server: around 7% process CPU, RSS about 80-90 MB after the run
- `meta-01` metaserver: high steady CPU around 68% process CPU; this node also ran all client benchmarks, proxy, and UI

## Storage And Cache State

Observed on `data-01`:

- EFS shared store used about 26 MB
- Cache EBS mounted at `/mnt/temporalstore-cache`, 10 GB total, about 108 MB used
- Shared store files:
  - `ns1-65536-oplog/DAT-0000000001`: about 55 MB
  - `ns1-65536-index/DAT-0000000001`: about 12 MB
  - `ns1-65536-page1/DAT-0000000001`: about 39 MB
- mtcache SSD directory exists and contains RocksDB-style metadata/log files, confirming SSD cache runtime path is active

## Cache Tier Probe: Tiny DRAM + SSD Vs EFS

Goal: pressure the blockcache by setting very little DRAM and leaving SSD cache enabled, then compare against a blockcache-disabled EFS-heavy run.

Configuration tested:

| Mode | Blockcache DRAM | Blockcache SSD | SSD path |
| --- | ---: | ---: | --- |
| Tiny DRAM + SSD | 1 MB | 2 GB | `/mnt/temporalstore-cache/mtcache-ssd-tiny-dram` |
| EFS-heavy comparison | disabled | disabled | n/a |

Workload:

- `feature_sequence_benchmark`
- 64 feature keys
- 4,000 rows per key
- 6,000 query ops per query case
- 2 client threads, pinned to primary reads

Results:

| Mode | Ingest QPS | 100-row window p99 | 1000-row filtered p99 | 1000-row complex p99 | Full-window complex p99 |
| --- | ---: | ---: | ---: | ---: | ---: |
| Tiny DRAM + SSD | 52/s | 2.053 ms | 4.598 ms | 7.989 ms | 16.990 ms |
| EFS-heavy comparison | 50/s | 2.414 ms | 4.881 ms | 11.312 ms | 16.489 ms |

Important interpretation:

- The test proves the server can run with a very small blockcache DRAM budget while SSD cache is enabled.
- It does **not** yet prove that query payloads are being served from SSD cache. After the run, the SSD cache directory remained about 4.2 MB and only contained engine metadata/log files. No meaningful SSD cache data growth was observed.
- The likely reason is that the benchmark ingests and queries in the same process lifetime, so live sequence objects remain in memory. To force a true SSD-vs-EFS read comparison, add or use a read-only fixed-prefix benchmark:
  1. ingest a known prefix,
  2. dump/checkpoint,
  3. restart data nodes to clear live objects,
  4. query the same prefix with blockcache enabled and inspect SSD cache growth,
  5. repeat with blockcache disabled for cold EFS reads.

Bug fixed in the cache-tier test harness:

- The first generated data-node command passed an empty `--metaserver_uri`, causing `no leader found` and stuck `P_CREATING` partitions. The script now passes the metaserver IP explicitly.

Current post-test state:

- Data nodes were restored to the normal blockcache-on runtime: 64 MB DRAM blockcache and 2 GB SSD blockcache.
- The AWS cluster is still running for follow-up tests.

## Issues Found

1. Proxy path is not fully clean: proxy STRING Set/Get and HASH HMSet/HMGet passed, but proxy FeatureQuery returned 0 points. Direct SDK/module Feature tests passed.
2. The test client writes are much slower than reads on EFS. For higher write QPS, next tests should compare larger instances/NVMe and shared-store tuning against EFS. A pure local primary-pull design is **not** a drop-in replacement for shared storage: the secondary can pull and replay logical oplog records from the primary, but dumped page/index metadata contains storage addresses. If those addresses point into primary-local files, another node cannot use them safely without an address-translation or page-copy rewrite protocol. Shared storage keeps page/index addresses globally meaningful.
3. Metaserver CPU is high because the same small `t3.small` node ran metaserver, proxy, UI, and all benchmarks. For cleaner product numbers, move benchmark/client load to a separate instance.

## Raw Result Files

Local raw artifacts:

- `local_build/BCache2-build-sandbox/infra/aws/temporalstore-test/ssm-run-scale-tests-result.json`
- `local_build/BCache2-build-sandbox/infra/aws/temporalstore-test/collect-data-01.txt`
- `local_build/BCache2-build-sandbox/infra/aws/temporalstore-test/collect-data-02.txt`
- `local_build/BCache2-build-sandbox/infra/aws/temporalstore-test/collect-meta-01.txt`
- `local_build/BCache2-build-sandbox/infra/aws/temporalstore-test/cloudwatch-cpu-data-01.json`
- `local_build/BCache2-build-sandbox/infra/aws/temporalstore-test/cloudwatch-cpu-data-02.json`
- `local_build/BCache2-build-sandbox/infra/aws/temporalstore-test/cloudwatch-cpu-meta-01.json`
- `local_build/BCache2-build-sandbox/infra/aws/temporalstore-test/ssm-run-concurrent-sweep-result.json`
- `local_build/BCache2-build-sandbox/infra/aws/temporalstore-test/ssm-run-concurrent-overload-result.json`
- `local_build/BCache2-build-sandbox/infra/aws/temporalstore-test/ssm-run-low-thread-concurrent-sweep-result.json`
- `local_build/BCache2-build-sandbox/infra/aws/temporalstore-test/cache-tier-latency-summary.txt`
- `local_build/BCache2-build-sandbox/infra/aws/temporalstore-test/collect-sweep-data-01.txt`
- `local_build/BCache2-build-sandbox/infra/aws/temporalstore-test/collect-sweep-data-02.txt`
- `local_build/BCache2-build-sandbox/infra/aws/temporalstore-test/collect-sweep-meta-01.txt`
- `local_build/BCache2-build-sandbox/infra/aws/temporalstore-test/collect-low-thread-data-01.txt`
- `local_build/BCache2-build-sandbox/infra/aws/temporalstore-test/collect-low-thread-data-02.txt`
- `local_build/BCache2-build-sandbox/infra/aws/temporalstore-test/collect-low-thread-meta-01.txt`

Remote result directory on metaserver:

- `/var/lib/temporalstore/aws-scale-results-20260604_203821`

## Current Access

- Monitoring UI: `http://54.185.179.199:8088/`
- SSM metaserver login:
  - `aws ssm start-session --profile temporalstore --region us-west-2 --target i-003c930417f7ee609`
- SSM data-01 login:
  - `aws ssm start-session --profile temporalstore --region us-west-2 --target i-0724d90b323786546`
