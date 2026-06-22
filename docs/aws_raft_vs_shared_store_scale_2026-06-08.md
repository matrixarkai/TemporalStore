# AWS Raft vs Shared-Store Scale Run - 2026-06-08

This note records the last AWS comparison run before the cleanup incident. It is intentionally explicit about both the good shared-store result and the blocked Raft path so the next run starts from facts instead of memory.

## Topology

- Metaserver and benchmark host: `i-05f55360d92c43908`, private `10.70.1.161`, public `44.248.70.48`, type `t3.small`.
- Data node 1: `i-0cfbef56e86551535`, private `10.70.1.214`, public `54.186.230.140`, type `c7i.large`.
- Data node 2: `i-04c93ad8271e5b64a`, private `10.70.1.24`, public `44.248.14.216`, type `c7i.large`.
- Data-node test size: 2 vCPU instances.
- Benchmark style: STRING set/get scale smoke with low thread counts first, because the instances are intentionally small.

## Shared-Store Path

- Run id: `shared_store-20260608_004319`.
- Storage URI: `shared-file:///mnt/temporalstore-shared/aws-scale/storage/shared_store-20260608_004319/`.
- Replication smoke: `PASS` after 2 attempts, 102 ms.

| Threads | Set QPS | Set p50 us | Set p95 us | Set p99 us | Get QPS | Get p50 us | Get p95 us | Get p99 us | Errors |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | 2,934 | 240 | 610 | 2,422 | 2,502 | 241 | 916 | 3,639 | 0 |
| 2 | 6,172 | 257 | 522 | 2,035 | 5,442 | 257 | 721 | 2,470 | 0 |
| 4 | 11,827 | 279 | 556 | 1,164 | 10,346 | 259 | 806 | 2,854 | 0 |
| 8 | 17,241 | 371 | 801 | 2,525 | 19,743 | 325 | 707 | 1,986 | 0 |

Observed CPU during the low-thread run stayed modest:

- data01: about 7.5% / 2.8% memory.
- data02: about 7.4% / 2.9% memory.

## Raft Path

- Run id: `raft-20260608_003152`.
- Storage URI: `file:///var/lib/temporalstore/raft-local/raft-20260608_003152/`.
- Result: blocked on primary write timeout.

Representative failure:

```text
FAIL primary set: Internal: Request server failed[E1008]Reached timeout=5000ms @10.70.1.214:17001 endpoint=10.70.1.214:17001
```

The timeout profile was:

- Operations: 20.
- Errors: 20.
- Average latency: about 5,001,938 us.
- p50: about 5,000,409 us.
- p95: about 5,003,631 us.
- p99: about 5,026,905 us.

## Code State After The Run

The shared-store path remained the only AWS-positive-tested path in this run.

After the failed Raft run, the code gained additional guardrails and hooks:

- `ReadIndex`.
- `AddLearner`.
- `PromotePeer`.
- `CanServeBoundedStaleRead`.
- Partition-level wrappers for Raft read and membership checks.
- Fail-closed behavior when the data Raft backend is unavailable.
- CMake guard for optional benchmark source files that may not be present in a cleaned repo.

Local compile check performed after those code changes:

```bash
cmake --build build-raft-test --target bcache2-server -j2
```

That compile check passed at the time, but the Raft path still needs a fresh local and AWS smoke with the restored package.

## Remaining Raft Work

The current Raft code is a guarded integration path, not yet a production-ready data-node Raft deployment. Remaining work:

- Real partition snapshot payload and install-snapshot validation.
- Learner catch-up and promotion proof.
- Leader routing and failover validation.
- Linearizable read-index test.
- Bounded stale replica-read test.
- AWS zero-error smoke before any scale claims.
