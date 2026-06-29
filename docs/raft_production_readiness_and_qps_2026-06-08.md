# Data-Node Raft Production Readiness And QPS - 2026-06-08

## Executive Status

TemporalStore data-node Raft is not production-ready yet.

The current source has an important guarded Raft shape:

- RustRaft-backed `DataRaftConsensusBackend`
- write-only command proposal before mutation
- committed FSM apply on the partition owner thread
- direct-write fail-closed guard
- read-mode flags in source
- learner/membership operation hooks

But production-ready Raft still requires:

- durable applied Raft index inside partition state
- real partition snapshot export/import
- install-snapshot for new/far-behind replicas
- learner catch-up and promotion validation
- leader kill/failover validation
- read-index and bounded-stale read validation
- AWS zero-error Raft smoke and scale results

Until those pass, `shared_store` remains the safe tested deployment path.

## Shared-Store No-Regression Data

The current AWS shared-store run passed with zero errors.

Cluster:

| Role | Instance type |
| --- | --- |
| metaserver/client | `t3.small` |
| data01 | `c7i.large` |
| data02 | `c7i.large` |

Mode:

```bash
--data_replication_mode=shared_store
--secondary_pull_stream_from_primary=false
--storage_async=true
```

Replication smoke:

```text
PASS replication smoke: secondary read matched after 2 attempts, 101 ms
```

STRING QPS:

| Threads | Write QPS | Write p99 | Read QPS | Read p99 | Errors |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 3,460 | 1,060 us | 3,633 | 561 us | 0 |
| 2 | 4,352 | 3,965 us | 5,899 | 2,146 us | 0 |
| 4 | 9,345 | 1,535 us | 10,810 | 696 us | 0 |
| 8 | 19,900 | 1,105 us | 20,725 | 877 us | 0 |

Reference: `docs/aws_shared_store_string_qps_2026-06-08.md`.

## Raft AWS Bring-Up Attempt

Raft mode was attempted with:

```powershell
tools/workspace/aws_temporalstore_raft_vs_shared_test.ps1 `
  -Profile temporalstore `
  -Region us-west-2 `
  -ThreadList "1 2" `
  -Ops 1000 `
  -ValueBytes 128 `
  -Modes raft `
  -AllowRaftBringup
```

Result: blocked before benchmark.

The deployed AWS data-node binary rejected current-source read-mode flags:

```text
ERROR: unknown command line flag 'data_raft_bounded_stale_max_index_lag'
ERROR: unknown command line flag 'data_raft_read_mode'
```

Interpretation:

- The AWS package is stale relative to current source.
- No Raft QPS, latency, secondary lag, or failover claim can be made from that package.
- The next Raft test must rebuild and deploy one coherent server/client package from the current source before running AWS again.

## Stale-Binary Guard Added

The AWS release staging path now refuses to package a server binary unless the staged `bcache2-server --help` contains:

- `data_replication_mode`
- `data_raft_read_mode`
- `data_raft_bounded_stale_max_index_lag`

The AWS shared-store/Raft comparison harness also checks the deployed data-node binary before starting a Raft test. If the server is stale, it exits with:

```text
STALE_TEMPORALSTORE_SERVER missing_flag=<flag>
Deploy one coherent current-source package before raft benchmarking.
```

This fixes the specific `DATA_RAFT_READ_MODE` failure mode by making package/runtime skew fail fast.

Follow-up note: an immediate rerun after this harness change was blocked before SSM submission because the AWS SSO token had expired. The harness now also fails clearly when SSM command submission does not return a command id.

## Can We Remove Primary-Pull If Raft Exists?

Eventually, yes, for the no-data-loss production path.

If full data-node Raft is complete, then a secondary should not need to pull oplog/page/index streams from the primary for normal replication. Raft would provide:

- quorum commit
- local follower apply
- leader election
- snapshot install for new/far-behind replicas
- controlled read modes

However, do not remove `primary_pull` yet.

Keep it until Raft passes:

- follower restart from local WAL
- new learner catch-up through snapshot
- old page restore after checkpoint/log compaction
- leader kill and new leader write
- scale-up learner promotion
- scale-down leader drain
- secondary stale-read lag tests

After those pass, `primary_pull` can become a debug/migration/recovery fallback instead of a normal production replication mode.

## Production-Ready Raft Acceptance Criteria

Raft should be called production-ready only after all rows pass:

| Area | Required proof |
| --- | --- |
| Write correctness | write success only after quorum commit and committed FSM apply |
| Applied index | persisted durably with data mutation and restored after restart |
| Snapshot | real partition snapshot includes index, pages, oplog checkpoint, metadata, and applied index |
| Install snapshot | new/far-behind follower can restore without reading old primary local files |
| Read policy | leader read, linearizable read-index, and bounded-stale replica read tested |
| Failover | kill leader, elect/promote new leader, continue writes, old leader fenced |
| Learner promotion | add learner, catch up, promote, serve reads, optionally receive primaries |
| No regression | shared-store test still passes after Raft changes |
| AWS scale | collect write QPS, read QPS, p50/p95/p99, CPU, Raft commit lag, applied lag, and secondary visibility lag |

## Current Code Status After Raft Hardening

The Raft path is safer than the previous bring-up branch, but it is still not ready
to call the default production no-data-loss path.

Done in the current source:

- `DataRaftConsensusOptions` now carries partition-owned snapshot and snapshot-load callbacks.
- `DataRaftFsm::Checkpoint()` and `DataRaftFsm::OnSnapshotLoad()` call those callbacks instead of silently accepting empty snapshots.
- `Partition` now restores a per-partition applied-index sidecar from `--data_raft_work_dir/applied/<partition_id>` before starting the Raft backend.
- `Partition` advances that applied-index sidecar only after a committed Raft command/log entry applies to the partition.
- Data-Raft replicas no longer use the legacy shared-store remote-info restore path while loading local streams.
- Data-Raft replicas open local streams writable for committed Raft apply, while still rejecting direct client writes to readonly partitions.
- Committed Raft apply bypasses normal readonly/quota write-admission checks only inside the apply path, so a quorum-committed entry is not rejected by a follower's local admission state.
- Empty snapshots remain test-only through `--data_raft_enable_empty_snapshot_for_tests=true`.

Still intentionally fail-closed:

1. Real partition snapshots.
   - `Partition::CreateDataRaftSnapshot()` now exports local-filesystem condition/index/oplog/page stream files into the RustRaft snapshot directory.
   - `Partition::LoadDataRaftSnapshot()` now reinstalls those files and rebuilds volatile partition managers before later Raft entries replay.
   - Supported snapshot/install URI schemes are `file://`, `shared-file://`, `shared://`, `efs://`, and `nfs://`.
   - Object-store snapshot adapters for `s3://` and S3-compatible schemes are still not implemented and intentionally fail closed.
   - A production snapshot must export and import object/page/index/oplog state plus the applied index.

2. Atomic durable applied index.
   - The sidecar removes the old restart-forgets-progress hole.
   - The sidecar is now persisted with fsync, atomic rename, and parent-directory fsync.
   - Startup now fails closed if existing Raft WAL is present but the applied-index checkpoint is missing.
   - It is still not a single transaction with object/page/index/oplog mutation; closing that final crash window requires either an engine-native recovery metadata record or idempotent command-log application for every command type.

3. Distributed membership control.
   - Membership operations now require an active leader lease, wait for config-change application, log pending RustRaft config indexes, and make duplicate add/promote/remove requests idempotent where safe.
   - The backend rejects unsafe leader transfer to learners, local leader removal, and last-voter removal unless the single-voter smoke-test flag is explicitly enabled.
   - Partition membership reconciliation now adds/promotes active replicas, transfers leadership to the intended primary, and removes all inactive peers.

Until these rows are completed and tested with leader kill, learner catch-up, config changes, and snapshot install, Raft is a guarded experimental path, not the default production no-data-loss path.

## Next Concrete Step

1. Build a coherent current-source release package containing:
   - `bcache2-server`
   - `bcache2-metaserver`
   - `string_scale_benchmark`
   - `replication_smoke_example`
   - matching runtime libraries

2. Deploy that package to the existing AWS cluster.

3. Run this sequence:
   - shared-store smoke/QPS regression
   - Raft one-leader/two-replica smoke
   - Raft write/read QPS
   - Raft secondary visibility-lag probe
   - leader kill/failover test

4. Only then compare:
   - shared-store sync
   - shared-store async
   - primary-pull
   - Raft consensus
