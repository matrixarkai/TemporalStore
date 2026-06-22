# TemporalStore Data-Node Raft Support

Last updated: 2026-06-08

## Current Status

TemporalStore currently has three data replication paths:

| Mode | Status | What it does |
| --- | --- | --- |
| `shared_store` | Existing path | Primary and secondary use shared stream storage for index, oplog, and page recovery. |
| `primary_pull` | Existing path | Secondary pulls stream bytes from the primary for catch-up instead of reading shared storage directly. |
| `raft_consensus` | Backend linked, guarded write/read path active | Starts a Byteraft-backed data-node consensus backend with transport, WAL, snapshot server, membership operations, command proposal, and FSM apply hooks. Write-only batches propose before mutation; mixed read/write batches remain rejected. Reads are gated by `data_raft_read_mode`. |

`raft_consensus` is the target production default for no-data-loss deployments, but it is still behind a production-readiness gate until real partition snapshots and failover validation are complete. It now has a Byteraft-backed backend and starts the Byteraft transport/WAL/snapshot stack. Write-only batches are serialized into `DataRaftCommandEntry`, proposed through Byteraft, applied from the committed FSM entry, and acknowledged with the committed apply response. The apply path is bounced onto the partition owning worker thread to preserve the share-nothing mutation model.

The only escape hatch is:

```bash
--data_raft_enable_experimental_direct_writes=true
```

That flag is for isolated backend bring-up only. It must not be used as a production durability mode because it permits local mutation before quorum commit.

Read serving is also explicit:

```bash
--data_raft_read_mode=leader
--data_raft_read_mode=linearizable
--data_raft_read_mode=bounded_stale
--data_raft_read_mode=unsafe_any_replica
```

- `leader` is the default. It rejects secondary reads in Raft mode.
- `linearizable` serves only on the leader and performs `ReadIndex` before executing the read.
- `bounded_stale` allows secondary reads only when local applied index lag is within `--data_raft_bounded_stale_max_index_lag`.
- `unsafe_any_replica` is for bring-up only and must not be used for correctness-sensitive serving.

## Production Durability Policy

Use one of three explicit production profiles:

| Profile | Flags | RPO | When to use |
| --- | --- | --- | --- |
| No-data-loss Raft | `--data_replication_mode=raft_consensus`, direct writes disabled | RPO 0 after quorum commit for write-only batches after quorum/apply succeeds | Default target for serving data that cannot be regenerated, after snapshot/failover/read-mode validation is complete. |
| Conservative shared storage | `--data_replication_mode=shared_store --storage_async=false` | Depends on shared storage durability, usually no acknowledged-loss if the shared store commit succeeds | Use when EFS/object/shared storage is provisioned strongly enough and the Raft write path is not enabled yet. |
| Streaming/ephemeral async | `--data_replication_mode=shared_store --storage_async=true` or a future async Raft profile | Possible loss inside the async flush window | Use for streaming features/events that can be replayed or recomputed from Kafka/offline logs. |

If EFS/shared storage is scaled enough, conservative shared-store mode is the safe fallback today. If the workload is streaming and upstream replay is acceptable, async storage can trade RPO for write QPS. For primary data that must survive a primary-node loss, Byteraft quorum commit plus snapshots should become the default.

## What Full Data Raft Must Include

Full support means each logical data partition has a real consensus group. The group owns write ordering, replica membership, snapshot transfer, and leader failover.

Required components:

1. **Group identity**
   - One Raft group per logical shard or partition unit.
   - Stable group ID derived from namespace, table, partition unit, and generation.
   - Metaserver owns the authoritative mapping from logical shard to group membership.

2. **Transport**
   - Dedicated RPC endpoints for append entries, vote, install snapshot, timeout-now, and leadership transfer.
   - Transport should use the existing server listen address and endpoint registry, but it must be isolated from normal client command RPCs.
   - Each node must register its Raft transport endpoint with metaserver during heartbeat.

3. **Durable Raft log**
   - Local per-group Raft WAL stored on data-node disk.
   - The WAL records consensus entries before they are applied to the object engine.
   - Local object/oplog/page files remain the storage engine state; the Raft WAL is the replication and recovery log.

4. **FSM apply**
   - Client writes go to the group leader.
   - Leader proposes serialized storage operations.
   - After quorum commit, the FSM applies the operation through the same command/object manager path used by local execution.
   - Followers apply entries in commit order and expose replay lag metrics.

5. **Snapshots**
   - Snapshot must capture enough partition state to bootstrap a new node without reading the old leader's local disk.
   - Snapshot payload should include index metadata, page/object checkpoint metadata, and the minimum oplog sequence needed after restore.
   - Snapshot data can be stored locally for small tests and object/S3-compatible storage for production-size restore.

6. **Read modes**
   - `leader_read`: read only from leader after local commit index is current.
   - `linearizable_read`: use ReadIndex or a quorum barrier before serving.
   - `stale_replica_read`: allow follower reads with a max-lag budget for latency-sensitive feature serving.
   - The client/proxy must choose the read mode explicitly.

7. **Membership and autoscale**
   - New node joins metaserver first.
   - Metaserver adds it as a non-voting learner or catch-up replica.
   - After snapshot/install and log catch-up, metaserver promotes it to voting replica or read replica.
   - Scale-down drains primaries first, transfers leadership, waits for healthy replicas, then removes membership.

8. **Failover**
   - If the leader dies, remaining voting replicas elect a new leader.
   - Metaserver updates routing after leadership is confirmed.
   - Writes must be rejected on non-leaders.
   - Follower reads need either explicit stale-read semantics or a linearizable read barrier.

9. **Metrics and tests**
   - Per-group commit index, applied index, leader term, role, vote status, quorum health, append latency, snapshot latency, and replay lag.
   - Component tests for leader crash, follower crash, network partition, node restart, snapshot restore, learner promotion, stale read, and linearizable read.

## Minimal Implementation Plan

## ByteKV Partitionserver Blueprint

ByteKV's partitionserver has the right production shape for a transactional partition:

- `PartitionReplica` owns role, lifecycle, leader events, serving checks, shutdown, transfer leader, and per-replica metrics.
- `PartitionEngine` wraps the local engine and turns writes into serialized redo entries.
- `PartitionFSM` applies committed entries, handles leader start/stop, configuration changes, snapshots, split, merge, and remove tasks.
- The master/control plane owns partition placement, replica membership, location cache, and operator workflows.

TemporalStore should borrow that structure, not the internal dependency code:

| ByteKV concept | TemporalStore equivalent |
| --- | --- |
| `PartitionReplica` | `DataPartitionReplica` wrapper around `Partition` lifecycle and role |
| `PartitionEngine::async_propose` | Temporal command serialization followed by `DataRaftConsensusBackend::Propose` |
| `PartitionFSM` | `DataPartitionFsm` that applies committed `DataRaftLogEntry` through `ObjectManager::ReplayOplog` |
| Applied index in engine | applied raft index and applied oplog sequence in partition status |
| Snapshot load/checkpoint | index/page/oplog checkpoint manifest plus remaining oplog replay |
| Master location cache | metaserver partition placement and proxy/client router refresh |
| Transfer leader/admin tools | metaserver-owned drain, transfer-primary, add-replica, remove-replica operations |

The clean repository must keep this behind a Raft adapter. It should not vendor internal dependency code or require the old internal Raft headers to build.

Phase 1: adapter interface

- Add a `DataRaftConsensusBackend` interface with `Start`, `Stop`, `Propose`, snapshot, membership, transfer-leader, and status methods.
- Use Byteraft as the concrete backend.
- Keep the old RPC-forwarded data-node replication path removed; it was not quorum Raft.

Phase 2: Byteraft consensus backend

- Use the Byteraft backend adapter.
- Use node-level `MultiRaftServerImpl` and `MultiSnapshotServerImpl`, shared by partition Raft nodes on the same data node.
- Use local disk WAL and snapshot directories first.
- Test one leader and two followers in one process or one host.

Phase 3: production write path

- Convert write commands into deterministic Raft log entries before local mutation.
  - `DataRaftCommandEntry` and `SerializeDataRaftCommand` now define the isolated command envelope.
  - `Partition::ApplyDataRaftCommand` now applies a committed command envelope from the FSM side.
  - The write-only path now routes leader batches through this envelope, proposes with Byteraft, and waits for the local applied index before acknowledging.
- Propose through `DataRaftConsensusBackend::Propose`.
- Apply committed entries through the data FSM.
  - `ApplyDataRaftEntry` accepts both new command entries and the older committed-oplog codec for bring-up compatibility.
  - The data FSM now implements `FlexibleApply` so batched Raft data, no-op, meta, and config-change entries advance applied index in order, matching the ByteKV FSM shape more closely than single-entry-only `Apply`.
  - Data-Raft replicas now load their own local streams instead of restoring primary/shared-store stream metadata.
  - Data-Raft replicas open local streams writable for committed FSM apply, while still rejecting direct client writes to readonly partitions.
  - Committed FSM apply bypasses local readonly/quota write-admission checks so a committed entry is deterministic on every replica.
- Return write success only after quorum commit and FSM apply status.
  - The leader now tracks pending Raft apply responses by request id and returns the committed FSM response.
  - The partition now restores and advances a per-partition applied-index sidecar under `--data_raft_work_dir/applied/<partition_id>`.
- Add routing rules so writes always go to the Raft leader.
  - Leader proposal waiting runs on the server background async pool, while committed FSM apply runs on the owning partition worker thread.

Still open before calling the Raft path production-ready:

- Implement real snapshot/checkpoint and install-snapshot for new or far-behind replicas.
  - The Byteraft FSM now calls partition-owned snapshot/load callbacks.
- `Partition::CreateDataRaftSnapshot()` and `Partition::LoadDataRaftSnapshot()` now export/import real local-filesystem partition state: condition, index stream, oplog stream, and page-zone streams are copied into the Byteraft snapshot directory and reinstalled by rebuilding the partition's in-memory managers before later Raft entries replay.
- Supported snapshot source/install URI schemes are `file://`, `shared-file://`, `shared://`, `efs://`, and `nfs://`. This covers local Docker/shared-file tests and AWS EFS/NFS-style shared filesystem tests.
- Object-store snapshot exporters/importers still fail closed for `s3://` and S3-compatible schemes. Use local Raft streams plus Byteraft snapshot transfer for production Raft today; add an explicit S3 snapshot archive/import adapter before using S3 as the snapshot payload store.
- Empty Byteraft snapshots remain test-only through `--data_raft_enable_empty_snapshot_for_tests=true`; production Raft should leave this false.
- The applied-index sidecar is now written through an fsync+rename+directory-fsync path, and startup fails closed if existing Raft WAL is present without an applied-index checkpoint.
- Full transactional coupling between storage mutation and applied-index advancement still requires either an engine-native recovery metadata record or an idempotent command log format for every command type.
- Wire membership changes, learner catch-up, promotion, and leader transfer into metaserver/autoscale.
  - The data-node backend now guards membership changes behind an active leader lease, waits for config-change application, treats already-present peers idempotently, rejects learner leadership transfer, and prevents accidental removal of the local leader or the last voter. Pending Byteraft config indexes are logged but not used as a hard preflight rejection because Byteraft can continue applying while reporting the last config index.
  - `Partition::UpdateMembership()` now reconciles active replicas by adding learners, promoting caught-up replicas, transferring leadership to the intended primary, and removing every inactive peer instead of stopping after the first removal.
- Read-index and bounded-stale replica-read mode hooks are now present. They still need local multi-node and AWS validation under concurrent writes before being enabled for correctness-sensitive all-replica serving.

Phase 4: snapshots and scale

- Implement partition snapshots.
- Support learner catch-up and promotion.
- Add autoscale lifecycle hooks: join, catch up, serve reads, receive primaries, drain, remove.

Phase 5: AWS validation

- Compare `shared_store`, `primary_pull`, and `raft_consensus`.
- Measure write QPS, read QPS, p50/p95/p99 latency, CPU, network, disk, failover RTO, and RPO.

## Guardrail Added

`raft_consensus` is accepted as a flag value and starts the Byteraft backend. Direct local writes are still rejected by default; write-only command batches must go through the Raft proposal path. The guard is intentionally fail-closed; direct writes require `--data_raft_enable_experimental_direct_writes=true`.

For shared-store AWS tests, use:

```bash
--data_replication_mode=shared_store
```

Use `raft_consensus` for backend bring-up tests until leader/follower/snapshot/failover/read-mode tests are complete. The AWS comparison harness defaults to `shared_store`; Raft runs require explicit `-Modes raft -AllowRaftBringup` so the result cannot be mistaken for a production benchmark.
