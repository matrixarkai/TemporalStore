# TemporalStore AWS Replication Test - 2026-06-08

## Scope

Tested the latest rebuilt TemporalStore runtime on the existing AWS cluster:

- Metaserver/client node: `i-05f55360d92c43908`, `10.70.1.161`, `t3.small`
- Data node 1: `i-0cfbef56e86551535`, `10.70.1.214`, `c7i.large`
- Data node 2: `i-04c93ad8271e5b64a`, `10.70.1.24`, `c7i.large`

Runtime package:

- Local artifact: `artifacts/temporalstore-release-raft-20260608.tar.gz`
- S3 artifact: `s3://temporalstore-test-artifacts-657817560042-us-west-2/temporalstore/temporalstore-release-raft-20260608.tar.gz`
- Server flags verified: `data_replication_mode`, `data_raft_read_mode`, `data_raft_bounded_stale_max_index_lag`

The benchmark shape was intentionally small because each data node has 2 vCPUs:

- Client threads: `1`, `2`
- Operations per write/read phase: `4000`
- Value size: `128` bytes
- Blockcache disabled to isolate storage/replication behavior.

## Shared Store, Async Persistence

Mode:

- `--data_replication_mode=shared_store`
- `--storage_async=true`
- Shared storage URI: `shared-file:///mnt/temporalstore-shared/aws-scale/storage/shared_store-20260608_172242/`

Replication smoke:

- Passed.
- Secondary read matched after `2` attempts, `101 ms`.

Results:

| Threads | Set QPS | Set p50 us | Set p95 us | Set p99 us | Get QPS | Get p50 us | Get p95 us | Get p99 us | Errors |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | 2728 | 231 | 1224 | 3033 | 2693 | 221 | 1646 | 3647 | 0 |
| 2 | 5471 | 272 | 796 | 2086 | 1890 | 468 | 3251 | 11173 | 0 |

CPU scrape:

- Data01: around `6.5%` process CPU after run.
- Data02 CPU scrape failed once because the node's SSM command runner intermittently returned empty failures. The benchmark itself completed before that scrape.

Interpretation:

- Async shared-store write path gives much higher write QPS than sync on EFS.
- Replica visibility smoke was around 100 ms in this small run.
- The 2-thread read result had higher p99, likely due to mixed routing/secondary visibility behavior and small-node scheduling.

## Shared Store, Sync Persistence

Mode:

- `--data_replication_mode=shared_store`
- `--storage_async=false`
- Shared storage URI: `shared-file:///mnt/temporalstore-shared/aws-scale/storage/shared_store-20260608_173624/`

Replication smoke:

- Passed.
- Secondary read matched after `2` attempts, `101 ms`.

Results:

| Threads | Set QPS | Set p50 us | Set p95 us | Set p99 us | Get QPS | Get p50 us | Get p95 us | Get p99 us | Errors |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | 97 | 9996 | 13005 | 17470 | 3276 | 228 | 397 | 2182 | 0 |
| 2 | 170 | 11002 | 18954 | 21284 | 7751 | 237 | 354 | 482 | 0 |

CPU scrape:

- Data01: around `4.4%` process CPU after run.
- Data02: around `7.0%` process CPU after run.

Interpretation:

- Sync persistence to the current EFS/shared-file path is write-latency dominated.
- Reads remain fast because the working set is hot in memory and the read path does not pay the same EFS sync cost.
- This mode is safer for batch/materialized features where data loss is unacceptable, but it needs faster durable media or batching before it is a high-QPS write path.

## Raft Consensus Path

Mode:

- `--data_replication_mode=raft_consensus`
- `--storage_async=false`
- `--data_raft_read_mode=bounded_stale`
- `--data_raft_bounded_stale_max_index_lag=16`
- Local storage URI: `file:///var/lib/temporalstore/raft-local/raft-20260608_174634/`

Startup:

- Data01 started the TemporalStore server and exposed Raft RPC/snapshot service ports.
- Data02 started the TemporalStore server and exposed Raft RPC/snapshot service ports.

Benchmark result:

- Failed before scale benchmark.
- First replication smoke write timed out:

```text
FAIL primary set: Internal: Request server failed[E1008]Reached timeout=5000ms @10.70.1.214:17001 endpoint=10.70.1.214:17001
```

Observed control-plane behavior:

- Server logs showed repeated `UpdateMembershipRequest` operations where the metaserver kept adding active derived partition ids and freezing old derived ids.
- The table creation path still used `partition_unit_relation: ANTI_ENTROPY`.
- The clean source and tests currently reference `ANTI_ENTROPY`; no first-class Raft partition relation was found in the visible table creation paths.

Interpretation:

- The Raft backend starts, but production Raft replication is not complete.
- The data-node Raft code path is not yet integrated with metaserver placement, partition lifecycle, and failover semantics.
- The control plane still treats the table as anti-entropy/shared-store style replication, so it keeps creating derived recovery partitions instead of managing one stable Raft replica group.

## Infrastructure Notes

Data02 SSM instability was observed:

- After one reboot, short commands succeeded and runtime install succeeded.
- During later runs, some SSM commands on data02 failed instantly with empty stdout/stderr.
- The sync benchmark succeeded after another reboot and delayed SSM execution.

This is not a TemporalStore data-path result, but it affects automation reliability. The AWS test harness now retries empty SSM failures and treats CPU scrape failures as non-fatal.

## Conclusion

Current production-ready path:

- Shared-store replication with async or sync persistence is operational on AWS.
- Async shared-store is the practical high-write-QPS option today, with accepted RPO risk.
- Sync shared-store is safer but EFS latency makes writes much slower in this small test.

Current non-production path:

- Data-node Raft is not production-ready yet.
- The missing work is not only transport or WAL; the key blocker is metaserver/control-plane integration for Raft replica groups.

## Next Required Raft Work

1. Add a first-class Raft table/partition relation, separate from `ANTI_ENTROPY`.
2. Make metaserver create exactly one logical shard with one stable Raft group, not repeated derived recovery partitions.
3. Store Raft group membership in metaserver metadata: voters, learners, leader, epoch, term, and placement.
4. Route writes to the Raft leader and reject stale/manual writes on non-leaders.
5. Implement learner catch-up and promotion without using anti-entropy derived partitions.
6. Implement install-snapshot with real partition snapshot payloads for file-backed local storage.
7. Implement failover validation: kill leader, elect/promote secondary, verify writes resume and old data is readable.
8. Re-run this benchmark matrix:
   - shared-store async
   - shared-store sync
   - Raft consensus leader-read
   - Raft consensus bounded-stale replica-read
