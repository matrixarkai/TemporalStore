# Distributed Raft Readiness

## Current Status

Distributed Raft is not production-complete yet.

The Rust code currently has:

- in-process data-node Raft model
- in-process metaserver Raft model
- majority commit behavior
- primary/leader promotion
- follower catch-up
- scale up/down model
- read-index and leader-transfer guards
- local snapshot install
- chunked snapshot install that assembles multiple `InstallSnapshotChunkRequest` messages before
  replacing follower state
- joint-consensus membership safety model requiring old and new voter majorities
- `RaftRpcRuntime` wrapper with max-inflight backpressure, retries, and retry backoff
- Raft RPC metadata with request id, deadline, and auth token, plus an authenticated transport wrapper
- randomized heartbeat/election scheduler model
- Raft transport message contracts:
  - `AppendEntriesRequest`
  - `VoteRequest`
  - `InstallSnapshotRequest`
  - `InstallSnapshotChunkRequest`
- local receive-path validation for AppendEntries and RequestVote
- HTTP transport smoke coverage for AppendEntries, RequestVote, and InstallSnapshot
- local timeout tick election and pre-vote coverage
- strict shared-store oplog gap rejection
- partition/heal chaos coverage in the local model
- tests for the above behavior

The readiness API is:

```rust
temporalstore_single_node::distributed_raft_readiness()
```

It intentionally returns `complete = false` until production requirements are implemented.

## Required Before Claiming Complete

To mark distributed Raft complete, the code still needs:

- real OpenRaft or raft-rs consensus integration
- production RPC runtime with connection pooling, TLS/mTLS, and observability
- randomized heartbeat/election scheduler integrated with an external production Raft runtime
- durable Raft WAL and hard-state files
- durable membership-change log
- timer-driven elections, heartbeats, and pre-vote
- production chunk streaming over long-lived network streams rather than request-sized JSON chunks
- multi-process crash/restart tests
- external multi-process network partition tests
- slow follower and lagging replica tests
- rolling restart tests

## Current Test Coverage

The local model is covered by unit and integration tests:

```text
raft_replicates_committed_write_to_majority_and_followers
raft_rejects_write_without_majority
raft_follower_catches_up_after_outage
raft_transport_append_entries_catches_up_lagging_replica
raft_transport_rejects_stale_append_entries_and_behind_vote
raft_hard_state_membership_and_snapshot_transport_are_exposed
streaming_snapshot_chunks_install_only_after_all_chunks_arrive
joint_consensus_requires_old_and_new_majorities_before_commit_or_write
raft_rpc_runtime_retries_transport_errors_and_releases_inflight
raft_rpc_runtime_attaches_auth_and_deadline_metadata
raft_scheduler_randomizes_election_timeout_and_emits_heartbeats
partition_chaos_majority_side_continues_and_healed_replica_catches_up
distributed_raft_readiness_reports_remaining_production_gaps
metaserver_raft_promotes_follower_after_leader_failure_and_keeps_metadata_available
metaserver_raft_rejects_reads_and_writes_without_majority
```

These tests prove the local model and transport contracts. They do not prove real distributed consensus across OS processes.
