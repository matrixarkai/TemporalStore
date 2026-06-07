# Raft Replication And Snapshots

## Current Default

Raft is the default write-replication path in the Rust TemporalStore workflow.

The single-node crate currently models this with in-process clusters:

- `RaftCluster` for data-node shard replication.
- `MetaRaftCluster` for metaserver metadata replication.
- `EndToEndWorkflow::new` uses `ReplicationMode::Raft`.
- `ReplicationMode::SharedStore` is named, but rejected for the normal write path until it has full semantics.

This is a behavior model, not a production networked Raft implementation yet.

## Data Node Raft

The data-node model supports:

- majority write replication
- committed follower reads when `pin_primary = false`
- lagging replica read rejection
- follower catch-up
- leader promotion when the primary is down
- scale-up by adding a caught-up follower
- scale-down by removing a replica
- local Raft snapshot create/install

Data-node snapshots contain committed log entries up to the leader commit index.
Installing a snapshot rebuilds the follower shard engine from those entries, sets
the follower commit index to the snapshot index, and keeps newer local log entries.
Stale snapshots cannot overwrite a node that already has a higher commit index.

## Metaserver Raft

The metaserver model supports:

- majority metadata command replication
- committed reads from any live metadata replica
- leader promotion
- membership changes
- local metadata snapshot create/install

Metaserver snapshots store the compacted `MetaState` plus the last included Raft
index and term. A lagging metadata node can install a snapshot and serve the same
shard route state as the leader. Stale metadata snapshots are rejected.

## Raft Snapshot Role

Snapshots are not on the leader-election critical path.

The expected production path remains:

1. Raft log handles normal writes and consensus.
2. A leader periodically creates a durable snapshot.
3. Large snapshots are uploaded to S3-compatible storage.
4. A new or lagging replica installs the snapshot.
5. The replica catches up by applying Raft logs after the snapshot index.

The `temporalstore-snapshot` crate already models the S3-compatible snapshot
store. The current `temporalstore-single-node` Raft snapshot support proves the
state transition and safety behavior locally, but does not yet wire S3 snapshot
references into a production `InstallSnapshot` RPC.

## Tests

Covered locally:

- data-node majority replication
- data-node follower catch-up
- data-node replica reads and lag rejection
- data-node leader promotion
- data-node scale-up and scale-down
- data-node snapshot bootstrap and log catch-up
- data-node stale snapshot rejection
- election without snapshot availability
- metaserver metadata replication
- metaserver committed replica reads
- metaserver promotion and membership change
- metaserver snapshot bootstrap
- metaserver stale snapshot rejection
- end-to-end workflow defaults to Raft

## Missing For Production Raft Parity

Before claiming production parity with Byteraft or a real OpenRaft/raft-rs based
system, the Rust code still needs:

- real network transport between nodes
- persistent Raft WAL and hard-state storage
- actual leader election protocol, vote handling, and term persistence
- AppendEntries and InstallSnapshot RPCs
- snapshot metadata stored in Raft state
- S3 snapshot references attached to Raft snapshot metadata
- log compaction after snapshot finalization
- membership-change safety through a real Raft joint-consensus or equivalent path
- crash/restart tests using persisted WAL and snapshots
- chaos tests across processes and hosts

