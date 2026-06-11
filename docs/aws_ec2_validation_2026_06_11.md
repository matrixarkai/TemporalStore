# TemporalStore Rust AWS EC2 Validation - 2026-06-11

## Scope

This run validated the pushed TemporalStore Rust serving and replication code on the existing AWS EC2 environment, using SSM to run prebuilt Linux release binaries on the existing node.

No EKS or Kubernetes path was used.

## AWS Environment

- AWS account: `657817560042`
- Region: `us-west-2`
- SSM profile: `temporalstore`
- Artifact bucket: `temporalstore-test-artifacts-657817560042-us-west-2`
- Running EC2 node:
  - Instance: `i-05f55360d92c43908`
  - Name: `temporalstore-test-meta-01`
  - Role tag: `metaserver`
  - Type: `t3.small`
  - Private IP: `10.70.1.161`
  - Public IP: `34.223.234.63`
  - OS: Ubuntu, kernel `6.8.0-1057-aws`
- Existing data ASG nodes were not running during this test:
  - `temporalstore-test-data-asg-manual` instances were `terminated`

## Code Under Test

- Rust repo commit tested first: `4056746`
- AWS-discovered fix commit: `335ce55`
- MatrixArkAI main mirror after fix: `b14952b`

Release binaries were built locally, uploaded to S3, downloaded on the EC2 node, and run from:

```text
/tmp/temporalstore-aws-run-4056746/temporalstore-aws-bundle-4056746/bin
```

EC2 log outputs were saved under:

```text
/tmp/temporalstore-aws-validation
```

## Validation Commands

The EC2 node ran these binaries:

- `distributed_raft_harness`
- `scale_harness`
- `storage_modes_harness`
- `raft_secondary_replication_harness`

The scale run used:

```text
--nodes 3
--string-ops 500
--hash-ops 100
--sequence-keys 2
--sequence-len 100
--scale-events 2
--failover-every 100
--read-sample-every 20
--compare-shared-store true
--shared-store-ops 500
--shared-store-flush-every 20
```

## Distributed Raft Harness

Result: passed.

Key outcomes:

- 4-node Raft group
- Leader: node `2`
- Term: `2`
- Commit index: `5`
- Majority: `3`
- Live voters: `4`
- Leader lease valid: `true`
- Max apply lag: `0`
- Apply health: `healthy=true`
- All nodes had local WAL segment files:
  - `shard-1/node-1.segments/00000000000000000001.wal`
  - `shard-1/node-2.segments/00000000000000000001.wal`
  - `shard-1/node-3.segments/00000000000000000001.wal`
  - `shard-1/node-4.segments/00000000000000000001.wal`

Functional checks:

- Initial proposal: `ok`
- Replica reads from all 4 nodes returned `replicated-value`
- Follower write was rejected:
  - Code: `raft_error`
  - Message: `node 2 is not leader`
- Leader transfer to node `2`: passed
- Post-transfer write: `ok`
- Scale down to voters `[1, 2, 3]`: passed
- Post-scale-down write/read: `ok`
- Scale up to voters `[1, 2, 3, 4]`: passed
- Post-scale-up write/read: `ok`
- External snapshot publish/bootstrap/read: passed

## Scale And Latency

Result: passed.

Top-level result:

```text
final_nodes: [1, 3, 4]
leader_id: 1
commit_index: 602
string_ops: 500
hash_ops: 100
sequence_rows: 200
failovers: 4
scale_events: 2
elapsed_ms: 7518
write_ops_per_sec: 80.07
replication_healthy: true
max_replica_lag: 0
max_log_entry_bytes: 32768
```

Raft replica read latency:

| Metric | Value |
|---|---:|
| samples | 27 |
| p50 | 114 us |
| p95 | 167 us |
| p99 | 1737 us |
| max | 1792 us |

Shared-store sync primary write latency:

| Metric | Value |
|---|---:|
| samples | 500 |
| p50 | 978 us |
| p95 | 1683 us |
| p99 | 1802 us |
| max | 5129 us |

Shared-store async primary write latency:

| Metric | Value |
|---|---:|
| samples | 500 |
| p50 | 898 us |
| p95 | 1688 us |
| p99 | 1796 us |
| max | 3208 us |

Sync storage write latency:

| Metric | Value |
|---|---:|
| samples | 500 |
| p50 | 148 us |
| p95 | 210 us |
| p99 | 1012 us |
| max | 4543 us |

Async storage enqueue latency:

| Metric | Value |
|---|---:|
| samples | 500 |
| p50 | 0 us |
| p95 | 0 us |
| p99 | 1 us |
| max | 1 us |

This is the response-path async queue cost, not durable shared-store publish latency. In the C++
path, `Partition::OnExecuteCmdDone` returns before `op_logger_->Commit` when
`PERSISTENT_ASYNC && FLAGS_storage_async`, so the directly comparable client-visible metric is
`async_primary_write_latency`; durable async publish/flush must be measured separately.

Replica read latency through shared-store modes:

| Mode | Samples | p50 | p95 | p99 | max |
|---|---:|---:|---:|---:|---:|
| sync | 500 | 91 us | 139 us | 176 us | 609 us |
| async | 25 | 108 us | 123 us | 151 us | 168 us |

Secondary lag:

| Path | Max Lag |
|---|---:|
| Raft replica lag | 0 |
| Shared-store sync lag | 0 |
| Shared-store async lag | 19 |

The async shared-store lag is expected from `--shared-store-flush-every 20`; writes are queued and flushed in batches.

## C++ p99 Target Gate

Result: passed.

The C++ baseline used here is the data-type smoke report in
`benchmarks/data-type-trace-2026-05-27-r2/README.md` from the C++ TemporalStore tree. That report is
a small local smoke measurement with 5 operations per case, primary-pinned reads, 2 metaservers, 2
data servers, replica count 2, and local file storage. It is not a saturation benchmark.

To compare Rust against that p99 level without mixing in deliberate failover/scale events, the EC2
node ran a steady-state profile:

```text
--nodes 3
--string-ops 2000
--hash-ops 250
--sequence-keys 2
--sequence-len 500
--scale-events 0
--failover-every 0
--read-sample-every 1
--compare-shared-store true
--shared-store-ops 2000
--shared-store-flush-every 20
```

Output was saved on the EC2 node at:

```text
/tmp/temporalstore-aws-validation/cpp_p99_gate_steady.json
```

Comparison against the C++ p99 rows:

| Rust metric | Rust p99 | C++ p99 target | Result |
|---|---:|---:|---|
| Raft replica read latency | 266 us | 1593 us | pass |
| Shared-store sync primary write latency | 12783 us | 15695 us | pass |
| Shared-store async primary write latency | 14135 us | 15695 us | pass |
| Shared-store sync replica read latency | 295 us | 1353 us | pass |
| Shared-store async replica read latency | 306 us | 1353 us | pass |

Additional steady-state values:

- Samples: `2002` Raft replica reads, `2000` sync shared-store ops, `2000` async shared-store ops
- Raft max replica lag: `0`
- Sync shared-store max lag: `0`
- Async shared-store max lag: `19`, expected from `--shared-store-flush-every 20`
- Raft read max was `12391 us`; p99 stayed low, but this max shows the EC2 `t3.small` still has
  occasional scheduler/IO outliers.

## Storage Modes Harness

Result: passed.

Sync shared-store:

- Writes: 2
- Both writes published immediately
- Queue used: no
- Replay applied: 2
- Last oplog index: 2
- Read value after replay: `sync-value`

Async shared-store:

- Writes: 2
- Both writes initially queued
- Flush 1:
  - flushed: 1
  - remaining: 1
  - last oplog index: 1
- Flush 2:
  - flushed: 1
  - remaining: 0
  - last oplog index: 2
- Replay applied: 2
- Read value after replay: `async-value`

Raft local file mode:

- WAL root: `/tmp/temporalstore-storage-modes-1781198425382/raft-wal`
- Leader id: `1`
- Commit index before restore: `1`
- Commit index after restore: `1`
- Read value after restore: `wal-value`
- WAL files:
  - `shard-7/node-1.segments/00000000000000000001.wal`
  - `shard-7/node-2.segments/00000000000000000001.wal`
  - `shard-7/node-3.segments/00000000000000000001.wal`

This confirms the async/sync shared-store path did not break the Raft local-file persistence path.

## Process-Level Secondary Raft Harness

Initial result: failed on EC2 before fix.

The first EC2 run exposed a real harness ordering issue after leader crash:

```text
node 3 did not return after-leader-crash=v5
last response: raft_error
message: replica 3 is behind leader commit index: replica=14, leader=15
```

Interpretation:

- The replica was behaving safely.
- It rejected a strict read while one index behind.
- The harness was reading immediately after the failover write, before waiting for survivor commit/apply convergence.

Fix:

- Commit: `335ce55`
- Change: wait for cluster commit and apply health before reading replicas after leader crash.

After the fix, the same EC2 process-level harness passed.

Post-fix result:

- Surviving nodes: `2`, `3`
- Leader: node `2`
- Term: `30`
- Commit index: `15`
- Majority: `2`
- Live voters: `2`
- Has majority: `true`
- Leader lease valid: `true`
- Max apply lag among live nodes: `0`
- Apply health: `healthy=true`
- Writes: `14`, all `ok`
- Reads after leader crash:
  - node `2`: `after-leader-crash = v5`
  - node `3`: `after-leader-crash = v5`

Final live-node state:

| Node | Role | Alive | Commit | Last Log | Applied | Lag |
|---:|---|---|---:|---:|---:|---:|
| 2 | Leader | true | 15 | 15 | 15 | 0 |
| 3 | Follower | true | 15 | 15 | 15 | 0 |

Node `1` was intentionally killed as the old leader:

| Node | Alive | Commit | Last Log | Applied | Lag |
|---:|---|---:|---:|---:|---:|
| 1 | false | 14 | 14 | 14 | 1 |

This is expected: the killed node did not receive the post-crash write.

## Conclusions

- Raft distributed behavior passed on the existing EC2 node for local multi-node process simulation.
- Replica reads passed after convergence.
- Leader transfer, scale down, scale up, external snapshot bootstrap, and failover paths passed.
- Raft max replica lag in the scale test was `0`.
- Process-level failover showed a real timing bug in the harness, not in strict read safety; the fix was pushed.
- Shared-store sync path showed no lag.
- Shared-store async path showed bounded lag of `19`, matching the configured flush interval of `20`.
- Async storage enqueue/write latency was effectively zero in this run: p99 `1 us`.
- Raft local-file WAL restore passed, confirming the async/sync storage changes did not break local Raft persistence.

## Limitations

- This used the currently running EC2 node only. The previous data ASG nodes were terminated.
- The distributed harnesses run multiple local processes/listeners on the EC2 host; they do not yet prove multi-host cross-EC2 network behavior.
- The node type was `t3.small`, so QPS is a sanity/functional signal, not a production capacity number.
- EFS/EBS multi-node comparison was not run because only one EC2 instance was active.
