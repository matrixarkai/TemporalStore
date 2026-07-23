# TemporalStore AWS Replication Comparison - 2026-06-10

## Scope

This note records the latest local and AWS validation for three TemporalStore replication/storage modes:

- shared-store with `storage_async=true`
- shared-store with `storage_async=false`
- data-node Raft with `data_replication_mode=raft_consensus`

The important correction in this pass is secondary visibility measurement. The old `replication_smoke_example` binary waited about 100 ms between retries, so a result like `matched after 2 attempts, 104 ms` mostly measured the client sleep interval. The newer `secondary_visibility_lag_benchmark` uses a tight poll loop and reports raw visibility latency separately from poll attempts.

## Environment

| Item | Value |
|---|---|
| AWS region | `us-west-2` |
| Meta node | `i-05f55360d92c43908`, `t3.small`, private IP `10.70.1.161` |
| Data node 1 | `i-040b97008872a220a`, `c7i.large`, private IP `10.70.1.213` |
| Data node 2 | `i-07e44edc737200c99`, `c7i.large`, private IP `10.70.1.151` |
| Data node 3 | `i-0b43765700373cb00`, `c7i.large`, private IP `10.70.1.29` |
| Shared store | temporary EFS `fs-0d47912645658f222` |
| Runtime artifact | `temporalstore-raft-visibility-20260610-v2.tar.gz` |
| Client host | meta node |
| Data-node size | 2 vCPU each |

Raft needs three voters to survive one node failure. A two-node Raft group can replicate, but it cannot keep quorum after one voter dies.

## Local Raft Validation

Local Ubuntu 22.04 distributed Raft was validated before AWS.

| Check | Result |
|---|---|
| 3-node Raft startup | pass |
| steady-state write/read | pass |
| secondary visibility benchmark | pass, 100/100 samples |
| primary kill and promotion | pass |
| post-failover write/read | pass |

Tight local secondary visibility:

| Metric | Value |
|---|---:|
| avg | 1,650 us |
| p50 | 1,485 us |
| p95 | 3,324 us |
| p99 | 4,199 us |
| max | 7,637 us |
| errors | 0 |

Post-failover local raw GET latency:

| Metric | Value |
|---|---:|
| GET QPS | 1,785 |
| p50 | 830 us |
| p95 | 2,123 us |
| p99 | 4,253 us |

## AWS Results

### Secondary Visibility, Tight Polling

This is the key result for "do not count retry interval." It measures how fast a secondary can see a just-written key when the benchmark polls tightly.

| Mode | Storage async | Samples | Errors | avg | p50 | p95 | p99 | max |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| Shared store | true | 100 | 0 | 965 us | 932 us | 1,255 us | 2,058 us | 2,280 us |
| Shared store | false | 100 | 0 | 8,796 us | 8,912 us | 10,851 us | 11,977 us | 14,442 us |
| Raft | false | 100 | 0 | 636 us | 360 us | 2,065 us | 3,465 us | 4,049 us |

Interpretation:

- The old 100 ms smoke number was a polling artifact.
- In this small AWS cluster, shared-store async visibility is roughly low single-digit milliseconds at p99.
- Shared-store sync pays EFS write latency and lands around 12 ms p99 visibility.
- Raft steady-state visibility is also low single-digit milliseconds at p99 and did not hang in the fixed run.

### Raw STRING QPS And Latency

All tests used 128-byte values, one meta/client node, and 2-vCPU data nodes.

| Mode | Threads | SET QPS | SET p50 | SET p95 | SET p99 | GET QPS | GET p50 | GET p95 | GET p99 | Errors |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| Shared async | 1 | 2,063 | 345 us | 1,328 us | 2,997 us | 2,712 | 351 us | 449 us | 565 us | 0 |
| Shared async | 2 | 3,783 | 351 us | 1,978 us | 3,316 us | 4,777 | 356 us | 556 us | 2,155 us | 0 |
| Shared sync | 1 | 105 | 9,134 us | 11,999 us | 15,888 us | 2,533 | 374 us | 516 us | 764 us | 0 |
| Shared sync | 2 | 168 | 10,463 us | 19,482 us | 22,569 us | 4,573 | 384 us | 600 us | 1,057 us | 0 |
| Raft | 1 | 163 | 4,737 us | 8,569 us | 18,450 us | 1,544 | 333 us | 2,187 us | 4,047 us | 0 |
| Raft | 2 | 198 | 8,555 us | 12,038 us | 30,235 us | 4,514 | 418 us | 550 us | 687 us | 0 |

The Raft write path is now functional and no longer blocks or hangs in this steady-state AWS run. Write QPS is still closer to shared-store sync than shared-store async because each write is replicated through consensus rather than returned after local memory/asynchronous shared persistence.

### CPU Snapshot

| Mode | Node | CPU | Memory |
|---|---|---:|---:|
| Shared async | data01 | 6.7% | 1.4% |
| Shared async | data02 | 5.7% | 1.5% |
| Shared async | data03 | 6.3% | 1.4% |
| Shared sync | data01 | 6.0% | 1.4% |
| Shared sync | data02 | 5.9% | 1.5% |
| Shared sync | data03 | 6.0% | 1.5% |
| Raft | data01 | 6.1% | 1.6% |
| Raft | data02 | 4.5% | 1.7% |
| Raft | data03 | 4.3% | 1.7% |

## AWS Raft Failover

The first AWS failover check did not observe promotion because the AWS harness was missing the fast metaserver convict/failover flags used by the local test. The harness now starts metaserver with:

```text
--metaserver_convict_routine_interval_ms=500
--metaserver_convict_safe_mode_warning_ratio=100
--metaserver_convict_safe_mode_critical_ratio=100
--metaserver_meta_check_max_freeze_partition_per_min=100
```

After restarting Raft with those flags:

1. Killed data01 primary server process on port `17001`.
2. Polled `QueryService/ListPartition`.
3. Promotion was visible on the first poll.
4. Old primary `1099511627776` moved to frozen.
5. New primary became `1099511693312`.
6. Post-failover SET/GET passed with zero errors.

Post-failover AWS result:

| Phase | QPS | p50 | p95 | p99 | Errors |
|---|---:|---:|---:|---:|---:|
| SET | 188 | 8,529 us | 10,653 us | 109,787 us | 0 |
| GET raw success | 3,571 | 159 us | 2,055 us | 4,040 us | 0 |
| GET visibility retry | 3,571 | 159 us | 2,056 us | 4,041 us | 0 |

The post-failover write p99 has one high tail sample and needs more repeated runs before calling the Raft path production-ready. The important functional blocker is fixed: AWS no longer hangs on Raft steady-state, and primary-down promotion works with the correct metaserver settings.

## Current Conclusions

Shared-store async is the fastest write mode in this small AWS shape. It is the right mode for streaming/risk/feature workloads that can tolerate bounded data loss on a primary crash or can replay from upstream.

Shared-store sync is durable through EFS but write throughput is limited by synchronous shared-storage latency. It is safer for batch-loaded data that cannot be easily replayed.

Raft is now functional locally and on AWS for steady-state replication and basic primary-down failover. It gives the cleanest path toward avoiding data loss without relying on shared-store synchronous writes, but it still needs repeated failover, snapshot, membership, and long-run testing before being labeled production-ready.

## Follow-Ups

1. Rebuild `replication_smoke_example` everywhere so its optional tight-poll arguments are available; until then, rely on `secondary_visibility_lag_benchmark`.
2. Repeat AWS Raft failover 20+ times and record promotion time distribution.
3. Add a harness phase that kills the leader during concurrent writes and verifies acknowledged-write behavior.
4. Validate Raft snapshot install for a restarted or newly joined data node.
5. Package runtime shared libraries cleanly and reduce artifact size.
6. Keep the shared-store path separate and regression-tested; Raft should remain an independent replication option.
