# Raft Replication And Snapshots

## Current Default

Raft is the default write-replication path in the Rust TemporalStore workflow.

The distributed TemporalStore Rust crate currently models this with in-process clusters:

- `RaftCluster` for data-node shard replication.
- `MetaRaftCluster` for metaserver metadata replication.
- `ProductionRaftRuntime` wraps data-node Raft runtime/options/timers.
- `ProductionMetaRaftRuntime` wraps metaserver Raft runtime/options/timers and stale-server detection.
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
- chunked snapshot create/install through `InstallSnapshotChunkRequest`
- joint-consensus membership safety checks requiring old and new majorities
- retry/backoff/backpressure wrapper for Raft transport calls
- request id/deadline/auth metadata on Raft RPCs with an authenticated transport wrapper
- randomized heartbeat/election scheduler model

Data-node snapshots contain committed log entries up to the leader commit index.
Installing a snapshot rebuilds the follower shard engine from those entries, sets
the follower commit index to the snapshot index, and keeps newer local log entries.
Stale snapshots cannot overwrite a node that already has a higher commit index.
External S3/MinIO snapshot bootstrap now performs this stale-local-state check before downloading
or verifying the object-store snapshot, so a newer replica does not waste work or risk installing an
older checkpoint.

## Metaserver Raft

The metaserver model supports:

- majority metadata command replication
- committed reads from any live metadata replica
- leader promotion
- membership changes
- local metadata snapshot create/install
- production runtime validation/status surface
- background failover/catch-up tick loop
- background stale-server failure detector in Raft mode

The `metaserver` binary uses `ProductionMetaRaftRuntime` when `TS_META_RAFT=1` or
`TS_META_RAFT_NODES` is set. It exposes `/meta/raft/status` and
`/meta/raft/ready`. `TS_META_RAFT_NODES` accepts either `1,2,3` or
`1=127.0.0.1:17101,2=127.0.0.1:17102,3=127.0.0.1:17103`.

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
store. The current `temporalstore-rust` Raft snapshot support proves the
state transition and safety behavior locally. Chunked JSON snapshot install is
present for local/HTTP tests, but S3 snapshot references are not yet attached to
a production `InstallSnapshot` RPC.

## Tests

Covered locally:

- data-node majority replication
- data-node follower catch-up
- data-node replica reads and lag rejection
- data-node leader promotion
- data-node scale-up and scale-down
- data-node snapshot bootstrap and log catch-up
- data-node chunked snapshot bootstrap
- joint-consensus old/new majority safety
- Raft transport retry/backpressure wrapper
- Raft RPC auth/deadline metadata
- randomized scheduler behavior
- local partition/heal chaos behavior
- data-node stale snapshot rejection
- election without snapshot availability
- metaserver metadata replication
- metaserver committed replica reads
- metaserver promotion and membership change
- metaserver snapshot bootstrap
- metaserver stale snapshot rejection
- end-to-end workflow defaults to Raft
- latest external snapshot metadata is stored in Raft cluster state and restored
  from the local WAL
- S3 snapshot references are attached to Raft snapshot metadata for external
  install/bootstrap paths
- external snapshot bootstrap validates the metaserver snapshot reference against the
  downloaded manifest and `index.bin` payload before install, including shard id,
  last log index, byte size, and SHA-256 checksum
- chunked timestamped KV commands are covered across data-Raft command serialization,
  packed page storage layout, follower reads, and snapshot install

## Missing For Production Raft Parity

Before claiming production parity with Byteraft or a real OpenRaft/raft-rs based
system, the Rust code still needs:

- HTTP transport for AppendEntries/Vote/InstallSnapshot exists; production still needs pooled/authenticated RPC with full observability
- data Raft WAL recovery is present locally; production still needs metaserver HTTP mutation recovery
- local timeout tick election, pre-vote, and randomized scheduler model are present; production still needs integration with an external Raft runtime
- production streaming InstallSnapshot RPCs over long-lived streams
- log compaction after snapshot finalization
- membership-change safety through the actual OpenRaft/raft-rs joint-consensus log path
- crash/restart tests using persisted WAL and snapshots
- chaos tests across separate OS processes and hosts
