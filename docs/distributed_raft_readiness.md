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
- explicit replication health reports with per-voter lag
- catch-up heartbeat helper for recovered live followers
- safe data-node scale-up that returns only after the new replica is caught up
- safe data-node scale-down that rejects quorum loss and promotes only a caught-up successor
- primary crash failover report for deterministic secondary promotion
- equivalent metaserver health, catch-up, safe scale, and failover APIs
- read-index and leader-transfer guards
- local snapshot install
- chunked snapshot install that assembles multiple `InstallSnapshotChunkRequest` messages before
  replacing follower state
- joint-consensus membership safety model requiring old and new voter majorities
- `RaftRpcRuntime` wrapper with max-inflight backpressure, retries, and retry backoff
- Raft RPC metadata with request id, deadline, and auth token, plus an authenticated transport wrapper
- randomized heartbeat/election scheduler model
- durable local metaserver mutation log and replay for HTTP/admin mutations when
  `TS_META_MUTATION_LOG` is set
- Raft transport message contracts:
  - `AppendEntriesRequest`
  - `VoteRequest`
  - `InstallSnapshotRequest`
  - `InstallSnapshotChunkRequest`
- local receive-path validation for AppendEntries and RequestVote
- HTTP transport smoke coverage for AppendEntries, RequestVote, and InstallSnapshot
- local timeout tick election and pre-vote coverage
- append-only local Raft WAL records with checksum validation, fsync on append, latest-valid
  recovery, and corrupt-tail truncation
- in-progress joint-consensus membership persisted in local Raft WAL and restored after restart
- leader election rejects stale candidates unless their log is up-to-date with a voting majority
- RequestVote receive path updates higher terms and clears prior votes before grant/reject decisions
- strict shared-store oplog gap rejection
- partition/heal chaos coverage in the local model
- tests for the above behavior

The readiness API is:

```rust
temporalstore_single_node::distributed_raft_readiness()
```

It intentionally returns `complete = false` and `production_ready = false` until production
requirements are implemented.

Production callers should use the hard guard:

```rust
temporalstore_single_node::require_production_raft_ready()
```

or validate an explicit deployment mode:

```rust
temporalstore_single_node::validate_raft_deployment_mode(
    temporalstore_single_node::RaftDeploymentMode::ProductionDistributed,
)
```

Today that production mode returns `RaftProductionReadinessError`. `LocalModel` remains allowed for
unit tests, compatibility tests, and local correctness work.

## Required Before Claiming Complete

To mark distributed Raft complete, the code still needs:

- real OpenRaft or raft-rs consensus integration
- production RPC runtime with connection pooling, TLS/mTLS, and observability
- randomized heartbeat/election scheduler integrated with an external production Raft runtime
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
raft_rejects_electing_stale_replica_until_it_catches_up
raft_transport_append_entries_catches_up_lagging_replica
raft_transport_rejects_stale_append_entries_and_behind_vote
request_vote_higher_term_resets_prior_vote_before_decision
request_vote_higher_term_updates_term_even_when_candidate_log_is_behind
raft_hard_state_membership_and_snapshot_transport_are_exposed
streaming_snapshot_chunks_install_only_after_all_chunks_arrive
joint_consensus_requires_old_and_new_majorities_before_commit_or_write
joint_consensus_state_survives_wal_restore_and_still_requires_both_majorities
raft_rpc_runtime_retries_transport_errors_and_releases_inflight
raft_rpc_runtime_attaches_auth_and_deadline_metadata
raft_scheduler_randomizes_election_timeout_and_emits_heartbeats
partition_chaos_majority_side_continues_and_healed_replica_catches_up
replication_health_reports_lag_and_heartbeat_catches_up_secondary
safe_scale_up_adds_replica_only_after_catchup
safe_scale_down_rejects_quorum_loss_and_promotes_caught_up_leader_successor
primary_crash_promotes_caught_up_secondary_and_old_primary_recovers
local_raft_wal_persists_hard_state_membership_and_entries
local_raft_wal_recovers_latest_valid_record_and_truncates_corrupt_tail
raft_cluster_recovers_committed_state_from_local_wal
distributed_raft_readiness_reports_remaining_production_gaps
production_raft_mode_is_blocked_until_real_engine_exists
metaserver_raft_health_catchup_safe_scale_and_failover_work
metaserver_raft_promotes_follower_after_leader_failure_and_keeps_metadata_available
metaserver_raft_rejects_reads_and_writes_without_majority
metaserver_mutation_log_recovers_routes_tables_and_state_changes
```

These tests prove the local model and transport contracts. They do not prove real distributed consensus across OS processes.
