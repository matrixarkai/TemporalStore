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

After the follow-up fixes, the Rust scale harness reports async shared-store latency as three
separate values: queue-only latency under `async_storage_enqueue_latency`, amortized per-entry
durable publish latency under `async_storage_write_latency`, and whole-batch flush latency under
`async_storage_flush_latency`. For EFS-backed AWS runs, compare per-write sync storage latency with
`async_storage_write_latency`, not enqueue latency or whole-batch flush latency.

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

## More Data-Node Replica Test

Result: passed.

This run used the existing single EC2 node but increased the in-process Raft data-node replica count
to 7. It is useful for secondary lag, replica-read, failover, and scale-up/scale-down coverage, but
it is still not a true multi-EC2 network test.

Updated release binary:

```text
/tmp/temporalstore-more-nodes-a0cc2fd/bin/scale_harness
```

Output was saved on the EC2 node at:

```text
/tmp/temporalstore-aws-validation/more_data_nodes_7node.json
```

Command:

```text
--nodes 7
--string-ops 2000
--hash-ops 500
--sequence-keys 4
--sequence-len 1000
--scale-events 6
--failover-every 250
--read-sample-every 10
--compare-shared-store true
--shared-store-ops 1000
--shared-store-flush-every 20
```

Top-level result:

```text
final_nodes: [2, 5, 6, 7, 8, 9, 10]
leader_id: 2
commit_index: 2512
failovers: 7
scale_events: 6
replication_healthy: true
max_replica_lag: 0
```

Raft replica read latency with 7 data-node replicas:

| Metric | Value |
|---|---:|
| samples | 204 |
| p50 | 149 us |
| p95 | 268 us |
| p99 | 2249 us |
| max | 3766 us |

Per-node Raft status:

| Node | Role | Commit | Applied | Lag | Alive |
|---:|---|---:|---:|---:|---|
| 2 | leader | 2512 | 2512 | 0 | true |
| 5 | follower | 2512 | 2512 | 0 | true |
| 6 | follower | 2512 | 2512 | 0 | true |
| 7 | follower | 2512 | 2512 | 0 | true |
| 8 | follower | 2512 | 2512 | 0 | true |
| 9 | follower | 2512 | 2512 | 0 | true |
| 10 | follower | 2512 | 2512 | 0 | true |

Shared-store comparison during the same run:

| Metric | Value |
|---|---:|
| sync replica read p99 | 248 us |
| async replica read p99 | 272 us |
| sync max lag | 0 |
| async max lag | 19 |
| async storage enqueue p99 | 1 us |
| async storage flush p99 | 24170 us |

Interpretation:

- The Raft path caught all final secondaries up to the leader before exit; every live voter had
  `lag=0` and matching commit/applied index.
- Replica reads stayed low at p50/p95. The p99 was higher than the steady-state C++ p99 gate because
  this run intentionally mixed failovers, leader transfer, scale-up/scale-down, long sequence writes,
  and shared-store work on a `t3.small`.
- Async shared-store lag stayed bounded by the configured batch size: `--shared-store-flush-every 20`
  produced `async_max_lag=19`.

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

## AWS Revalidation After 7-Node Profile

Result: passed after one harness fix.

The latest Rust code was rebuilt into a fresh release bundle and validated on the existing EC2 node:

```text
Instance: i-05f55360d92c43908
Bundle: /tmp/temporalstore-aws-bundle-b013a35-v3
```

The validation ran:

- `distributed_raft_harness`
- `scale_harness` with 7 in-process data-node replicas
- `storage_modes_harness`
- `raft_secondary_replication_harness`

The first combined AWS attempt exposed two outstanding issues:

- Packaging issue: the secondary harness requires the sibling `raft_node` binary. The first bundle
  only included the harness binaries, so `raft_secondary_replication_harness` failed before starting.
- Timing issue: after the leader-crash phase, a surviving follower could be alive but still behind
  the new leader. The post-crash write was attempted too soon and correctly failed with:
  `not enough live replicas for majority`.

Fix:

- Include `raft_node` in the AWS validation bundle.
- In `raft_secondary_replication_harness`, converge surviving replicas before the post-crash write.
- Retry transient proposal errors for temporary `not enough live replicas` and
  `behind leader commit index` states.

Final AWS results:

Distributed Raft harness:

- Passed.
- 4-node group reached commit index `5`.
- All nodes reported apply health `healthy=true`.
- Replica reads, follower-write rejection, leader transfer, scale-down, scale-up, and external
  snapshot bootstrap all passed.

7-node scale/lag harness:

```text
final_nodes: [2, 5, 6, 7, 8, 9, 10]
leader_id: 2
commit_index: 2512
failovers: 7
scale_events: 6
replication_healthy: true
max_replica_lag: 0
```

Raft replica-read latency:

| Metric | Value |
|---|---:|
| samples | 204 |
| p50 | 147 us |
| p95 | 211 us |
| p99 | 1881 us |
| max | 3977 us |

Shared-store metrics in the same run:

| Metric | Value |
|---|---:|
| sync replica read p99 | 245 us |
| async replica read p99 | 272 us |
| sync max lag | 0 |
| async max lag | 19 |
| async enqueue p99 | 0 us |
| async flush p99 | 18913 us |

Storage modes harness:

- Sync shared-store replay read `sync-value`.
- Async shared-store replay read `async-value`.
- Raft local-file WAL restore read `wal-value`.

Secondary/failover harness:

- Passed after the catch-up fix.
- Surviving live voters: `2`.
- New leader: node `2`.
- Commit index: `15`.
- Surviving nodes 2 and 3 had apply lag `0`.
- Both surviving nodes read `after-leader-crash=v5`.

## EFS Shared-Store Validation Attempt

Result: blocked by AWS environment, after code fix.

The harness code now supports the intended storage split:

- sync shared-store and async shared-store use an explicit shared-store root
- on AWS, that shared-store root should be EFS-backed
- Raft WAL/local-file persistence remains on local disk

Code paths changed:

- `scale_harness` accepts `--shared-store-root` and reports `shared_store.shared_store_root`.
- `storage_modes_harness` accepts `--shared-store-root` and `--raft-wal-root` separately.
- `tools/run_temporalstore_scale_harness.sh` accepts `TS_SCALE_SHARED_STORE_ROOT`.
- `tools/run_temporalstore_more_data_nodes.sh` accepts `TS_MORE_NODES_SHARED_STORE_ROOT`.

Local validation passed with split paths:

```text
storage_modes_harness:
  shared store root: /tmp/ts-shared-local
  raft WAL root: /tmp/ts-raft-local
  sync read: sync-value
  async read: async-value
  raft WAL restore read: wal-value

scale_harness:
  shared_store.shared_store_root: /tmp/ts-scale-shared-local
  sync_max_lag: 0
```

AWS test attempted to use:

```text
shared store: /mnt/temporalstore-shared/rust-validation/<run_id>
raft WAL: /tmp/temporalstore-raft-local/<run_id>
```

The EFS preflight failed before the harness ran:

```text
timeout 15 mkdir -p "$SHARED_ROOT"
exit status: 124
```

Follow-up AWS discovery found no EFS filesystem in account `657817560042`, region `us-west-2`:

```text
aws efs describe-file-systems --region us-west-2
```

returned an empty filesystem list. The active EC2 node is in `us-west-2`, so the current reused EC2
environment cannot honestly run EFS-backed shared-store validation until an EFS filesystem is
created/mounted or the old EFS mount is restored.

## Multi-EC2 Data-Node Raft Validation

Result: partially passed, with two production findings.

This run launched three new EC2 data-node instances in the existing TemporalStore VPC/subnet/security
group and ran one standalone `raft_node` process per instance. The existing metaserver EC2 drove the
test over private IPs, so the test exercised real cross-EC2 network traffic rather than only
same-host loopback listeners.

Temporary data-node EC2 instances:

| Node | Instance | Private IP | Process |
|---:|---|---|---|
| 1 | `i-03a294b9428c914cb` | `10.70.1.214` | `raft_node :19001` |
| 2 | `i-024df97e01d43523c` | `10.70.1.40` | `raft_node :19001` |
| 3 | `i-0c139d94d153fd21b` | `10.70.1.155` | `raft_node :19001` |

Bundle under test:

```text
s3://temporalstore-test-artifacts-657817560042-us-west-2/temporalstore-aws-bundle-multiec2-4611a6a.tar.gz
```

Happy-path replication:

- Three data-node EC2 processes started and answered `/health`.
- Initial leader: node `1`.
- `60` writes through node `1` succeeded.
- Replica reads from nodes `1`, `2`, and `3` returned the written values.
- Read failure count: `0`.

Happy-path latency:

| Metric | p50 | p95 | p99 | Max |
|---|---:|---:|---:|---:|
| Raft write via leader node 1 | `503073 us` | `1132164 us` | `1253259 us` | `1311653 us` |
| Replica read node 1 | `909 us` | `3232 us` | `4943 us` | `5677 us` |
| Replica read node 2 | `1004 us` | `2097 us` | `9035 us` | `123987 us` |
| Replica read node 3 | `2021 us` | `5500 us` | `39110 us` | `184872 us` |

Leader-transfer finding:

- Applying `/raft/control/transfer_leader` only on node `1` made node `1` report leader `2`, but
  nodes `2` and `3` did not converge to the same top-level `leader_id`.
- Applying the transfer to all three nodes still left node `3` with a stale top-level `leader_id`
  for more than `20s`, even though its per-node status table showed node `2` as leader.
- This shows a real production control-plane gap in the standalone `raft_node` path: leadership and
  membership control changes still need a single authoritative replicated control flow, not
  per-process local admin mutation.

Node-down surviving-majority test:

- Node `1` process was killed with SSM.
- Nodes `2` and `3` marked node `1` not alive.
- Node `2` was elected leader in the surviving runtimes.
- `40` writes through node `2` succeeded.
- Replica reads from nodes `2` and `3` returned all written values.
- Read failure count: `0`.
- Final leader: node `2`.
- Final commit index on live nodes: `100`.
- Live-node lag from node `2` status: `0` for nodes `2` and `3`.

Node-down latency:

| Metric | p50 | p95 | p99 | Max |
|---|---:|---:|---:|---:|
| Raft write via surviving leader node 2 | `1034646 us` | `1179792 us` | `1191108 us` | `1191108 us` |
| Replica read node 2 | `961 us` | `2829 us` | `4164 us` | `4164 us` |
| Replica read node 3 | `2595 us` | `7388 us` | `298038 us` | `298038 us` |

Write-latency finding:

- Cross-EC2 replica reads are mostly low millisecond or sub-millisecond.
- Raft writes are too slow for production. The degraded write p50 is approximately the configured
  `TS_RAFT_RPC_DEADLINE_MS=1000`.
- The current write path still attempts follower RPCs sequentially and can wait on a dead or slow
  follower before returning, instead of committing/returning as soon as a live majority has
  acknowledged.
- Production fix: parallelize AppendEntries RPCs, stop waiting after majority success, and move slow
  follower catch-up to the background heartbeat/snapshot path.

### Post-Fix Multi-EC2 Degraded Write Validation

Fix commit under test locally before push:

- `RaftCluster::propose_distributed_one` now orders live followers before known-dead followers.
- The leader response path now stops sending foreground AppendEntries after quorum success.
- Dead or lagging follower catch-up remains for the heartbeat/snapshot path.

Fresh temporary EC2 data-node instances:

| Node | Instance | Private IP | Process |
|---:|---|---|---|
| 1 | `i-0bc810c6ebceaa298` | `10.70.1.165` | killed before degraded write phase |
| 2 | `i-0cd976af2ba8f77a1` | `10.70.1.182` | surviving leader |
| 3 | `i-09be93ca1be7be9ff` | `10.70.1.236` | surviving follower |

Degraded-majority result after the fix:

- Node `1` process was killed.
- Nodes `2` and `3` marked node `1` not alive.
- Node `2` was elected leader.
- `40` writes through node `2` succeeded.
- Replica reads from nodes `2` and `3` returned all written values.
- Write failure count: `0`.
- Read failure count: `0`.
- Final commit index on live nodes: `40`.
- Live-node lag from node `2` status: `0` for nodes `2` and `3`.

Latency improvement:

| Metric | Before Fix | After Fix |
|---|---:|---:|
| Degraded Raft write p50 | `1034646 us` | `115178 us` |
| Degraded Raft write p95 | `1179792 us` | `134956 us` |
| Degraded Raft write p99 | `1191108 us` | `148500 us` |
| Degraded Raft write max | `1191108 us` | `148500 us` |
| Replica read node 2 p99 | `4164 us` | `6929 us` |
| Replica read node 3 p99 | `298038 us` | `28170 us` |

Remaining after the fix:

- The degraded write path is no longer pinned to the dead follower's `1s` RPC deadline.
- The write p50 is still around `115 ms`, so the next optimization is parallel AppendEntries and
  less synchronous foreground work on the leader path.
- Node `3` can still report local `apply_health.healthy=false` because its local status model sees
  node `2`'s applied index as stale, even while node `2` status shows both live nodes applied with
  lag `0` and both nodes serve correct reads. That is an observability/control-state convergence gap
  to fix separately.

### Post-Fix Multi-EC2 Control Convergence Validation

Fix under test:

- Successful AppendEntries now updates the receiver's observed `leader_id`.
- Successful AppendEntries demotes stale local leaders in the receiver's node table.
- Standalone `raft_node` `/raft/apply_health` now reports local-observer apply health instead of
  treating locally stale remote process state as authoritative.

Fresh temporary EC2 data-node instances:

| Node | Instance | Private IP |
|---:|---|---|
| 1 | `i-0777f7691c52fe8a9` | `10.70.1.122` |
| 2 | `i-037dcfd9ff0818bcb` | `10.70.1.166` |
| 3 | `i-0f77587f41763a06a` | `10.70.1.135` |

Control-convergence result:

- Initial leader: node `1`.
- `20` seed writes through node `1` succeeded.
- Transfer/election target: node `2`.
- All three nodes converged to top-level `leader_id=2`.
- Post-transfer write through node `2` succeeded.
- Replica reads from nodes `1`, `2`, and `3` all returned `aws-controlfix-after-transfer`.
- `/raft/apply_health` returned `healthy=true` from all three standalone nodes.
- Final commit index: `21`.

Final apply-health response shape:

| Node | Reported Leader | Fully Applied Nodes | Healthy |
|---:|---:|---|---|
| 1 | 2 | `[1]` | `true` |
| 2 | 2 | `[2]` | `true` |
| 3 | 2 | `[3]` | `true` |

One expected detail remains: asking every process to execute `/raft/control/transfer_leader` is not
the production control flow. Node `3` returned a local `replica 2 is behind` error for that direct
admin mutation because its local model had not yet observed node `2` catch-up. The authoritative
result is the leader's transfer plus replicated AppendEntries: after heartbeats, all nodes reported
leader `2`, writes through node `2` worked, and all local apply-health endpoints were healthy.

### Outstanding Raft Control Fix Validation

Outstanding issues fixed in this pass:

- Foreground Raft writes now fan out live follower AppendEntries in parallel and return after quorum
  instead of waiting behind a slow or dead follower RPC path.
- `/raft/control/transfer_leader` is now leader-authoritative. Followers return a clean not-leader
  result instead of mutating stale local control state.
- Standalone `raft_node` now exposes `/raft/control/accept_leadership`, so the current leader can
  catch up the target process and make the target node accept the transferred leadership locally.

Local validation before AWS:

- `cargo test -p temporalstore-rust raft_transport --lib`
- `cargo test -p temporalstore-rust append_entries_updates_observed_leader_for_standalone_node_status --lib`
- `cargo build --release -p temporalstore-rust --bin raft_node`

The first AWS attempt reproduced the real stale-control bug: after leader transfer, node `1`
reported top-level `leader_id=2`, but nodes `2` and `3` still reported top-level `leader_id=1`.
Data replication was not accepted as sufficient evidence because the control plane view had not
converged across processes.

Second AWS attempt after the fix:

| Node | Instance | Private IP |
|---:|---|---|
| 1 | `i-059b6c78a4476144c` | `10.70.1.136` |
| 2 | `i-0d5ec2b1d38a3772d` | `10.70.1.152` |
| 3 | `i-0b14d779b36b92b94` | `10.70.1.217` |

Result:

- Follower pre-transfer control call returned clean not-leader:
  `node 3 is not leader`.
- `80` seed writes through node `1` succeeded.
- Leader transfer to node `2` succeeded.
- All three standalone nodes converged to top-level `leader_id=2`.
- Follower post-transfer transfer-to-current-leader calls returned success as no-ops.
- `40` post-transfer writes through node `2` succeeded.
- Replica reads from nodes `1`, `2`, and `3` all returned the latest post-transfer value.
- `/raft/apply_health` returned `healthy=true` from all three nodes.

Latency from the AWS control-fix run:

| Path | p50 | p95 | p99 | Max |
|---|---:|---:|---:|---:|
| Seed writes through node 1 | `131545 us` | `214616 us` | `229631 us` | `234450 us` |
| Post-transfer writes through node 2 | `367367 us` | `493878 us` | `1015299 us` | `1015299 us` |

Remaining observation:

- Per-process remote node tables can still show stale remote lag from another process's local view.
  The authoritative checks for this run were top-level leader convergence, local observer
  `/raft/apply_health`, successful post-transfer writes, and replica-read correctness.

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

### AWS Retest After Durable Async Latency Fix

Code under test:

- Commit: `32cb710`
- Instance: `i-05f55360d92c43908`
- Artifact:
  `s3://temporalstore-test-artifacts-657817560042-us-west-2/temporalstore-aws-retest-20260611185411-32cb710.tar.gz`
- Output directory:
  `/tmp/temporalstore-aws-validation/aws-retest-20260611185411-32cb710`

Scope:

- `distributed_raft_harness`
- `scale_harness` with 3 in-process Raft data nodes
- `scale_harness` with 7 in-process Raft data nodes
- `storage_modes_harness`

The reused EC2 environment still had no usable EFS mount, so shared-store testing used the local
file-backed shared-store path on the EC2 host. This validates the corrected metric semantics and
local-file object-store behavior, but it is not an EFS latency measurement.

3-node scale result:

| Metric | Value |
|---|---:|
| commit index | `962` |
| leader | `1` |
| replication healthy | `true` |
| max Raft replica lag | `0` |
| Raft replica read p50 | `109 us` |
| Raft replica read p95 | `153 us` |
| Raft replica read p99 | `1783 us` |
| Raft replica read max | `1808 us` |
| sync primary write p99 | `7490 us` |
| async primary write p99 | `4579 us` |
| sync storage write p99 | `4950 us` |
| async durable storage write p99, batch metric before follow-up | `23953 us` |
| async enqueue p99 | `1 us` |
| async flush p99 | `23953 us` |
| sync replica read p99 | `216 us` |
| async replica read p99 | `207 us` |
| sync shared-store lag | `0` |
| async shared-store lag | `19` |

7-node scale result:

| Metric | Value |
|---|---:|
| commit index | `2512` |
| leader | `2` |
| replication healthy | `true` |
| max Raft replica lag | `0` |
| Raft replica read p50 | `146 us` |
| Raft replica read p95 | `216 us` |
| Raft replica read p99 | `2048 us` |
| Raft replica read max | `3760 us` |
| sync primary write p99 | `8489 us` |
| async primary write p99 | `13664 us` |
| sync storage write p99 | `4501 us` |
| async durable storage write p99, batch metric before follow-up | `31637 us` |
| async enqueue p99 | `1 us` |
| async flush p99 | `31637 us` |
| sync replica read p99 | `259 us` |
| async replica read p99 | `315 us` |
| sync shared-store lag | `0` |
| async shared-store lag | `19` |

Storage modes result:

- Async shared-store replay applied `2` entries.
- Async flushes were ordered and bounded:
  - first flush: `flushed=1`, `remaining=1`, `last_oplog_index=1`
  - second flush: `flushed=1`, `remaining=0`, `last_oplog_index=2`

Interpretation:

- The previous near-zero async storage number was queue latency, not EFS/shared-store durability.
- This run was before the amortized-per-entry metric fix, so `async_storage_write_latency` still
  equaled whole-batch durable async flush latency in these tables.
- After the follow-up fix, `async_storage_write_latency` is the amortized per-entry durable async
  write cost and `async_storage_flush_latency` remains the whole-batch flush cost.
- The retest shows the expected split:
  - client-visible async enqueue p99: `1 us`
  - durable async shared-store write/flush p99: `23953 us` to `31637 us`
- Raft replication stayed healthy with max replica lag `0` in both 3-node and 7-node runs.
- Shared-store async lag stayed bounded at `19`, matching `--shared-store-flush-every 20`.

### AWS Retest After Production Hardening

Code under test:

- Commit: `f5c277a`
- Instance: `i-05f55360d92c43908`
- Artifact:
  `s3://temporalstore-test-artifacts-657817560042-us-west-2/temporalstore-aws-prod-hardening3-20260611193206-f5c277a.tar.gz`
- Output directory:
  `/tmp/temporalstore-aws-validation/aws-prod-hardening3-20260611193206-f5c277a`

Scope:

- `distributed_raft_harness`
- `raft_secondary_replication_harness`
- `scale_harness` with 3 in-process Raft data nodes
- `storage_modes_harness`

Fixes validated:

- The raft-enabled `server` `/raft/apply_health` endpoint now reports local-observer apply health,
  matching standalone `raft_node` and avoiding stale remote-table false negatives.
- `scale_harness` now reports async shared-store latency as:
  - `async_storage_enqueue_latency`: queue-only client response cost
  - `async_storage_write_latency`: amortized per-entry durable async publish cost
  - `async_storage_flush_latency`: whole-batch flush cost
- `distributed_raft_harness` now refreshes quorum before retrying writes after leader transfer and
  membership changes, avoiding EC2 timing races where timer health marks peers stale between the
  convergence wait and the write.

3-node AWS scale result:

| Metric | Value |
|---|---:|
| commit index | `962` |
| leader | `1` |
| replication healthy | `true` |
| max Raft replica lag | `0` |
| Raft replica read p50 | `114 us` |
| Raft replica read p95 | `174 us` |
| Raft replica read p99 | `1648 us` |
| Raft replica read max | `1704 us` |
| sync storage write p99 | `4191 us` |
| async durable per-entry write p99 | `1079 us` |
| async enqueue p99 | `1 us` |
| async flush batch p99 | `15280 us` |
| sync shared-store lag | `0` |
| async shared-store lag | `19` |

Validation checks:

- `async_storage_write_latency.p99_us <= sync_storage_write_latency.p99_us`
- `async_storage_flush_latency.p99_us >= async_storage_write_latency.p99_us`
- Raft max replica lag stayed `0`
- Sync shared-store lag stayed `0`
- Async shared-store lag stayed within `flush_every - 1`
- Storage modes replay applied `2` async entries with ordered bounded flush:
  - first flush: `flushed=1`, `remaining=1`, `last_oplog_index=1`
  - second flush: `flushed=1`, `remaining=0`, `last_oplog_index=2`

### AWS Retest After Shared-Store Hot-Path Trim

Code under test:

- Commit: `eb1c4c0`
- Instance: `i-05f55360d92c43908`
- Artifact:
  `s3://temporalstore-test-artifacts-657817560042-us-west-2/temporalstore-aws-sharedstore-latency-20260611202602-eb1c4c0.tar.gz`
- Output directory:
  `/tmp/temporalstore-aws-validation/aws-sharedstore-latency-20260611202602-eb1c4c0`

Fixes validated:

- Shared-store oplog publish now uses compact JSON instead of pretty JSON.
- The file-backed object store caches created parent directories, avoiding repeated `create_dir_all`
  calls for each object under the same shard oplog prefix.

3-node AWS scale result:

| Metric | Value |
|---|---:|
| commit index | `962` |
| leader | `1` |
| replication healthy | `true` |
| max Raft replica lag | `0` |
| Raft replica read p50 | `118 us` |
| Raft replica read p95 | `175 us` |
| Raft replica read p99 | `1693 us` |
| Raft replica read max | `1719 us` |
| sync primary write p99 | `9840 us` |
| async primary write p99 | `4018 us` |
| sync storage write p99 | `6582 us` |
| async durable per-entry write p99 | `1316 us` |
| async enqueue p99 | `1 us` |
| async flush batch p99 | `19040 us` |
| sync shared-store lag | `0` |
| async shared-store lag | `19` |

Interpretation:

- Async response-path latency is lower than sync response-path latency.
- Async durable per-entry publish cost is lower than sync per-entry publish cost because the async
  path publishes in batches.
- The whole-batch flush number remains larger than per-entry sync storage because it covers up to
  `20` oplog entries in one flush.
- This still used the local file-backed shared-store path on EC2 because the reused AWS environment
  has no mounted EFS filesystem.

### AWS Retest With Explicit QPS And Primary/Storage Metrics

Code under test:

- Commit: `3c9ec6b`
- Instance: `i-05f55360d92c43908`
- Artifact:
  `s3://temporalstore-test-artifacts-657817560042-us-west-2/temporalstore-aws-qps-latency-20260611204120-3c9ec6b.tar.gz`
- Output directory:
  `/tmp/temporalstore-aws-validation/aws-qps-latency-20260611204120-3c9ec6b`

Metric semantics:

- `sync_primary_write_latency` is the foreground primary write path. It includes primary engine
  work plus synchronous shared-store publish.
- `sync_storage_write_latency` is only the synchronous shared-store publish subspan.
- Therefore `sync_primary_write_latency` should normally be greater than or equal to the storage
  subspan. This run shows that expected ordering: primary write p99 `5549 us` vs storage write p99
  `3431 us`.
- `async_primary_write_latency` is the foreground primary write path with async shared-store queueing.
- `async_storage_enqueue_latency` is only queueing cost, matching the C++ async response-path idea.
- `async_storage_write_latency` is amortized durable async publish cost per entry.
- `async_storage_flush_latency` is the whole durable batch flush cost.

3-node AWS scale result:

| Metric | Value |
|---|---:|
| commit index | `962` |
| replication healthy | `true` |
| max Raft replica lag | `0` |
| Raft write p50 | `4358 us` |
| Raft write p99 | `8773 us` |
| Raft write max | `21874 us` |
| Raft write QPS | `50.83` |
| Raft replica read p50 | `113 us` |
| Raft replica read p99 | `1687 us` |
| Raft replica read QPS | `4.33` |
| sync primary write p50 | `1461 us` |
| sync primary write p99 | `5549 us` |
| sync primary write max | `10423 us` |
| sync primary write QPS | `200.4` |
| sync storage write p50 | `135 us` |
| sync storage write p99 | `3431 us` |
| sync storage write max | `8501 us` |
| sync replica read QPS | `200.4` |
| async primary write p50 | `1484 us` |
| async primary write p99 | `4062 us` |
| async primary write max | `6127 us` |
| async primary write QPS | `92.24` |
| async enqueue p99 | `1 us` |
| async durable per-entry write p50 | `85 us` |
| async durable per-entry write p99 | `658 us` |
| async flush batch p50 | `1718 us` |
| async flush batch p99 | `10997 us` |
| async replica read QPS | `4.61` |
| sync shared-store lag | `0` |
| async shared-store lag | `19` |

Replication comparison:

- Raft secondary lag: `0`. Raft writes replicate to quorum before success and secondaries served
  reads after convergence.
- Sync shared-store lag: `0`. The write is durable in the shared store before the primary returns,
  and the replica replay path can catch the exact committed index.
- Async shared-store lag: `19`, matching `flush_every - 1` for `--shared-store-flush-every 20`.
- This run still used the local file-backed shared-store path on EC2, not EFS. It validates the
  corrected Rust metric semantics and local object-store behavior. A direct C++ EFS latency
  comparison still needs the reused EC2 cluster to mount an EFS filesystem at the shared-store root.

## Conclusions

- Raft distributed behavior passed on the existing EC2 node for local multi-node process simulation.
- Multi-EC2 data-node replication now has a real AWS validation run across three separate EC2
  instances.
- Multi-EC2 data writes and replica reads passed functionally in happy-path and node-1-down
  surviving-majority scenarios.
- Multi-EC2 validation exposed two remaining production gaps: non-authoritative per-process
  leadership control updates, and write latency caused by sequential follower RPC timeout behavior.
- Replica reads passed after convergence.
- Leader transfer, scale down, scale up, external snapshot bootstrap, and failover paths passed.
- Raft max replica lag in the scale test was `0`.
- Process-level failover showed a real timing bug in the harness, not in strict read safety; the fix was pushed.
- Shared-store sync path showed no lag.
- Shared-store async path showed bounded lag of `19`, matching the configured flush interval of `20`.
- Async storage enqueue latency was effectively zero in this run: p99 `1 us`. That is the
  client-visible async response path, not durable shared-store/EFS completion. Durable async
  storage latency is now reported separately as `async_storage_write_latency` and
  `async_storage_flush_latency`.
- Raft local-file WAL restore passed, confirming the async/sync storage changes did not break local Raft persistence.

## Limitations

- Earlier validation used the currently running EC2 node only. The multi-EC2 run above launched
  three temporary data-node EC2 instances and did prove cross-EC2 `raft_node` traffic.
- The node type was `t3.small`, so QPS is a sanity/functional signal, not a production capacity number.
- EFS-backed shared-store comparison still was not run because the reused AWS environment has no
  usable EFS filesystem mounted at `/mnt/temporalstore-shared`.
