# ByteKV Raft Blueprint For TemporalStore

This note captures the concrete production shape to borrow from the local ByteKV codebase without vendoring ByteKV or RustRaft dependency code into the clean TemporalStore repo.

## ByteKV Files Reviewed

- `bytekv/partitionserver/partition/replica.h`
- `bytekv/partitionserver/partition/replica.cc`
- `bytekv/partitionserver/partition/engine.h`
- `bytekv/partitionserver/partition/engine.cc`
- `bytekv/partitionserver/partition/fsm.h`
- `bytekv/partitionserver/partition/fsm.cc`
- `bytekv/partitionserver/partition/replica_manager.cc`
- `bytekv/partitionserver/server/serving_replicas.h`
- `bytekv/engine/raft_engine.h`
- `bytekv/engine/raft_engine.cc`
- `bytekv/engine/raft_fsm.h`
- `bytekv/engine/raft_fsm.cc`
- `bytekv/engine/raft_mvcc_fsm.h`
- `bytekv/engine/raft_mvcc_fsm.cc`
- `bytekv/engine/rocksdb/replica_engine.cc`
- `bytekv/engine/blockdb/replica_engine.cc`

## Production Pattern To Borrow

ByteKV does not treat Raft as a background copy path. It makes Raft the partition write and recovery authority:

- `PartitionReplica` owns lifecycle, role, serving checks, shutdown, leadership events, and per-replica metrics.
- `PartitionEngine` is the write proposal surface. Writes become deterministic redo entries before local mutation.
- `RaftEngine` wraps RustRaft operations: propose, AddNode/AddLearner, RemoveNode, TransferLeader, Campaign, ReadIndex, and Checkpoint.
- `PartitionFSM` applies committed entries, handles leader/follower callbacks, configuration changes, admin commands, snapshot creation, and snapshot load.
- The storage engine persists applied index in the engine state. Snapshot export includes engine data plus replica metadata and applied index.
- Reads are not casual follower reads. ByteKV checks leader/lease/read-index semantics before serving correctness-sensitive reads.

## What ByteKV Proves

The biggest lesson is architectural: a production Raft path is not just "send the oplog to secondaries." It is a full partition lifecycle.

1. **Write authority**
   - Client writes are serialized into deterministic entries.
   - The leader proposes those entries through Raft.
   - Local mutation happens from committed FSM apply, not from a side-channel write path.

2. **Durable apply point**
   - The data engine stores the applied Raft index with the same durable write batch as the data mutation.
   - After restart, the replica knows exactly which committed entries are already reflected in local state.
   - This is the first production gap TemporalStore must close.

3. **Snapshot is real data**
   - Snapshot is not an empty RustRaft checkpoint.
   - It contains data files plus replica metadata plus applied index.
   - Install-snapshot imports that state before the replica can serve.

4. **Role gates serving**
   - Leader/follower callbacks update serving state.
   - Reads check role and read strategy.
   - Membership and routing changes are control-plane operations, not ad hoc client behavior.

5. **Learners are first-class**
   - New replicas join as learners.
   - They catch up through log and snapshot.
   - Only then should the control plane promote them or route reads.

## TemporalStore Mapping

| ByteKV concept | TemporalStore target |
| --- | --- |
| `PartitionReplica` | Keep `Partition` for now, but add a `DataPartitionReplica` wrapper later for role/lifecycle/metrics/failover. |
| `PartitionEngine::async_propose` | `Partition::ProposeDataRaftCommand` and `DataRaftConsensusBackend::Propose`. |
| `RaftFsm::FlexibleApply` | `DataRaftFsm::FlexibleApply`; added for data/no-op/meta/config-change batches. |
| `ReplicaEngine::GetAppliedIndex/PutAppliedIndex` | Durable per-partition `data_raft_applied_index` stored with committed mutation state. |
| `ReplicaEngine::ExportSnapshot` | TemporalStore partition snapshot export: index stream, oplog stream, page-zone streams, table/config/membership metadata, applied Raft index. |
| `ReplicaEngine::ImportSnapshot/LoadSnapshot` | TemporalStore snapshot install: atomically replace local partition stream state before serving. |
| `RaftEngine::ReadIndex` | `Partition::DataRaftReadIndex` plus `--data_raft_read_mode=linearizable`. |
| `RaftEngine::AddNode/RemoveNode/TransferLeader` | `DataRaftConsensusBackend` membership operations, driven by metaserver placement/autoscale. |
| `PartitionFSM::OnLeaderStart/OnLeaderStop` | Data-node Raft role callbacks that update metaserver/routing/readiness state. |

## Borrow Immediately

These are the pieces TemporalStore should borrow first because they directly decide correctness:

1. **Persist applied index inside partition state**
   - Add a durable `data_raft_applied_index` record per partition.
   - Update it atomically with the committed object/index/oplog mutation.
   - On restart, initialize `DataRaftFsm::applied_index_` from this durable value.
   - This mirrors ByteKV's replica-engine pattern where applied index is not only an in-memory FSM variable.

2. **Implement real partition snapshot and install-snapshot**
   - Snapshot must export live index metadata, live page streams, oplog checkpoint metadata, partition config, and applied Raft index.
   - Install-snapshot must clear or version away old local state, import the snapshot, set applied index, and only then allow serving.
   - This is required before log compaction, new learners, and far-behind replicas are safe.

3. **Add a thin `DataPartitionReplica` lifecycle wrapper**
   - Keep current `Partition` mutation code.
   - Move Raft role, readiness, leader callbacks, freshness, and read-mode checks into a wrapper.
   - This avoids spreading leader/follower state across request handling, storage, and metaserver code.

4. **Keep writes leader-only and committed-apply-only**
   - Direct writes must remain fail-closed in Raft mode.
   - Write success should mean quorum commit plus local committed apply on the leader.
   - Any async/streaming mode should be an explicit different profile, not hidden behind the Raft profile.

5. **Make metaserver own learner promotion and leader movement**
   - New node registers with metaserver.
   - Metaserver adds learner.
   - Learner catches up.
   - Metaserver promotes learner and optionally moves primaries.
   - Scale-down first transfers leaders away, then removes the voter.

## Borrow Later

These are valuable but not first-order blockers for TemporalStore's data-node Raft path:

- Split/merge admin-command workflow.
- Checksum and replica consistency scanner.
- Transfer-leader balancing scheduler.
- Table-level operator task framework.
- Backup/export orchestration.
- Transaction-specific MVCC redo logic.

## Do Not Borrow Directly

These parts are ByteKV-specific and should not be copied into TemporalStore:

- RocksDB/BlockDB MVCC storage-engine internals.
- ByteKV transaction protocol, TSO, and MVCC conflict checks.
- Internal dependency code, RustRaft source, or Byte libraries.
- Master scheduler implementation details that assume ByteKV partition/table semantics.

TemporalStore should borrow the shape, interfaces, and correctness checkpoints, while keeping the implementation clean and storage-model-specific.

## What Is Already In TemporalStore

- RustRaft-backed `DataRaftConsensusBackend`.
- Node-level transport and snapshot server startup.
- Write-only command envelope: `DataRaftCommandEntry`.
- Leader proposal path before local mutation for write-only batches.
- Committed FSM apply on the partition owner thread.
- Pending committed response returned to the leader client.
- Fail-closed direct-write guard.
- Fail-closed empty snapshot guard.
- ReadIndex/AddLearner/PromotePeer/RemovePeer/TransferLeader hooks.
- Explicit Raft read policy:
  - `leader`
  - `linearizable`
  - `bounded_stale`
  - `unsafe_any_replica`
- ByteKV-style `FlexibleApply` for batched RustRaft apply.

## Remaining Work Before Production Ready

The blocker is real partition snapshot/install-snapshot. Until this exists, a new or far-behind replica cannot be guaranteed to reconstruct old pages after Raft log compaction.

Required implementation:

1. Add durable applied-index state on `Partition`.
   - `GetDataRaftAppliedIndex()`
   - `PersistDataRaftAppliedIndex(index)`
   - ensure committed command apply persists the mutation and applied index as one logical recovery point

2. Add a `DataRaftSnapshot` API on `Partition`.
   - `CreateDataRaftSnapshot(path, applied_index*)`
   - `LoadDataRaftSnapshot(path)`

3. Snapshot manifest must include:
   - partition id and generation
   - table name and config version
   - membership version and primary id
   - applied Raft index
   - index stream info and copied records
   - oplog stream info and copied records
   - all live page-zone stream info and copied records
   - checksum for every copied stream record/file

4. Snapshot creation must be atomic:
   - pause or fence storage mutation enough to capture a consistent point
   - commit dirty streams
   - write manifest to a temporary directory
   - fsync files/directories where supported
   - rename manifest last

5. Snapshot load must be atomic:
   - reject mismatched partition id/generation
   - stop serving and storage manager
   - clear local stream state for that partition
   - import snapshot stream data
   - restore stream info
   - load index and replay only needed oplog after snapshot point
   - set applied Raft index before serving

6. Metaserver/autoscale must drive membership:
   - new node registers with metaserver
   - metaserver adds it as learner
   - learner receives snapshot and catches up
   - metaserver promotes learner after lag/freshness checks
   - scale-down transfers leaders away before removing a voter

7. Test suite required before AWS scale claims:
   - one leader plus two followers local smoke
   - write-only command quorum commit
   - follower restart from WAL
   - new learner snapshot install
   - learner promotion
   - leader kill and new leader write
   - linearizable read
   - bounded-stale follower read under writes
   - snapshot after log compaction
   - shared-store path regression

## Recommended Implementation Order

Do this in the order below to avoid regressions:

1. Keep the current shared-store path untouched and fully tested.
2. Add durable applied-index persistence for Raft mode only.
3. Add partition snapshot export/import behind the existing fail-closed snapshot flag.
4. Add local three-replica Raft tests: write, restart follower, add learner, install snapshot, promote learner.
5. Add leader-kill failover and read-mode tests.
6. Enable AWS Raft comparison only after local tests pass.
7. Keep `shared_store` and `raft_consensus` selectable by config so we can compare durability/performance profiles without mixing code paths.

## Current Policy

`shared_store --storage_async=false` remains the conservative production fallback today. `raft_consensus` is the target no-data-loss production path, but it must remain guarded until the snapshot/install-snapshot and failover tests above pass.
