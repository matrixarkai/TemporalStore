# Distributed Raft Readiness

## Current Status

Distributed Raft now has a separate-node deployment wrapper around the existing data-node Raft
contracts, but it is not production complete yet.

The Rust code currently has:

- production data-node Raft runtime options with OpenRaft/raft-rs engine selection
- production metaserver Raft runtime options with failover/catch-up timer and stale-server detection
- WAL-backed production runtime startup and recovery
- authenticated production RPC runtime construction over the existing Raft HTTP transport
- mTLS configuration validation for production deployments
- plaintext transport allowed only for local chaos tests
- timer supervisor for election ticks, heartbeat cadence, and follower catch-up
- leader heartbeat loop sends network AppendEntries to secondary data-node processes so restarted
  secondaries can catch up from the leader's log and local WAL state
- multi-process chaos plan validation for crash/restart and partition scenarios
- in-process data-node Raft model used by the runtime FSM/tests
- in-process metaserver Raft model behind `ProductionMetaRaftRuntime`
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
- equivalent metaserver membership plan/apply reports for add/remove/replace voters
- read-index and leader-transfer guards
- local snapshot install
- chunked snapshot install that assembles multiple `InstallSnapshotChunkRequest` messages before
  replacing follower state
- joint-consensus membership safety model requiring old and new voter majorities
- `RaftRpcRuntime` wrapper with max-inflight backpressure, retries, retry backoff, and
  runtime counters for attempts, successes, failures, retries, inflight requests, and
  backpressure rejections
- Raft RPC metadata with request id, deadline, and auth token, plus an authenticated transport wrapper
- randomized heartbeat/election scheduler model
- external snapshot references on Raft install-snapshot requests, so a leader can attach the
  immutable S3/MinIO snapshot manifest URI, checksum, and byte size while keeping Raft log catch-up
  as the source for recent writes
- external snapshot bootstrap rejects stale snapshots before download/verify work when the target
  replica already has a higher local commit index
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
- WAL-backed Raft clusters that automatically persist committed writes, leadership changes,
  membership changes, catch-up, RPC receive state, and snapshot installs without requiring callers
  to remember a manual `persist_wal()` call
- bounded local WAL retention for WAL-backed clusters, using `RaftConfig.max_disk_replicate_log_num`
  to compact old durable records while preserving latest recovery state
- installed Raft snapshot payload and snapshot-index floor persisted into WAL-backed data-node records,
  so restart can recover state even after pre-snapshot log entries are trimmed
- leader election rejects stale candidates unless their log is up-to-date with a voting majority
- RequestVote receive path updates higher terms and clears prior votes before grant/reject decisions
- strict shared-store oplog gap rejection
- partition/heal chaos coverage in the local model
- tests for the above behavior

The readiness API is:

```rust
temporalstore_rust::distributed_raft_readiness()
```

It returns `complete = false` and `production_ready = false` until the real consensus/storage and
external chaos requirements are implemented.

Production callers should use the hard guard:

```rust
temporalstore_rust::require_production_raft_ready()
```

or validate an explicit deployment mode:

```rust
temporalstore_rust::validate_raft_deployment_mode(
    temporalstore_rust::RaftDeploymentMode::ProductionDistributed,
)
```

Production mode is intentionally blocked by `RaftProductionReadinessError`. `LocalModel` remains
allowed for unit tests, compatibility tests, and local correctness work.

## Production Runtime Surface

The production runtime surface is:

- `ProductionRaftRuntimeOptions`
- `ProductionRaftRuntime::start`
- `ProductionRaftRuntime::transport`
- `ProductionRaftRuntime::propose`
- `ProductionRaftRuntime::start_timer_loop`
- `ProductionMetaRaftRuntime::start`
- `ProductionMetaRaftRuntime::start_timer_loop`
- `ProductionMetaRaftRuntime::validate_ready`
- `ProductionRaftSecurity::mtls`
- `ProductionRaftChaosPlan`
- `raft_node` binary with `/raft/propose`, `/raft/read`, `/raft/status`, gated local-chaos
  `/raft/admin/*` endpoints, local peer-block chaos controls, and peer `/raft/*` transport endpoints
- `raft_secondary_replication_harness` binary that starts real `raft_node` OS processes, kills and
  restarts a secondary, injects a network partition with stale-read rejection, heals and verifies
  follower catch-up, kills the original leader, triggers surviving-node failover, and verifies
  post-failover reads

Production deployments should use `ProductionRaftSecurity::mtls`. Local chaos tests can use
`ProductionRaftSecurity::plaintext_for_local_chaos` only when
`allow_plaintext_for_local_chaos = true`.

## Required Before Production Ready

- Replace the local consensus model with OpenRaft or raft-rs FSM/storage integration.
- Wire data-node Raft snapshots to real engine snapshot create/download/install.
- Run external multi-process packet-loss, slow follower, and rolling restart tests.
- Add actual mTLS transport implementation, not just config validation and authenticated metadata.
- Integrate metaserver shard membership changes with networked data-node Raft groups.

## Repeated Data-Node Raft Check

The repeated audit still finds that data-node distributed Raft is not production complete.

Covered today:

- separate `raft_node` process wrapper
- HTTP Raft transport endpoints for propose/read/status and peer messages
- local OS-process harness coverage for network partition stale-read rejection, heal, and catch-up
- local majority commit, catch-up, safe reads, promotion, safe scale up, and safe scale down
- WAL-backed local model recovery for commits, leadership, and membership
- local snapshot and chunked snapshot message behavior

Still missing:

- real OpenRaft or raft-rs data-node FSM/storage implementation
- production OpenRaft/raft-rs durable log-store adapter beyond the local segmented WAL model
- metaserver-driven networked Raft membership changes for each shard
- snapshot install wired to `TemporalEngine` freeze, flush, download, verify, and install
- production engine snapshot install with freeze/flush/download/verify/install lifecycle; the local
  external snapshot path now has stale-local-state preflight before download
- external chaos tests that inject packet loss, slow followers, disk pressure, and rolling restarts
- AWS multi-node validation with real metaserver, proxy, client, and data-node processes

The executable readiness gate for this repeated check is:

```rust
temporalstore_rust::production_readiness_report()
```

It now has separate areas for:

- `data_node_distributed_raft`
- `fault_tolerance`
- `scale_testing`

## Separate Node Run

Start three local Raft nodes as separate OS processes:

```bash
TS_RAFT_NODE_ID=1 TS_RAFT_BIND_ADDR=127.0.0.1:19001 TS_RAFT_WAL_DIR=target/raft-node-1 \
TS_RAFT_NODES='1=127.0.0.1:19001,2=127.0.0.1:19002,3=127.0.0.1:19003' \
cargo run -p temporalstore-rust --bin raft_node

TS_RAFT_NODE_ID=2 TS_RAFT_BIND_ADDR=127.0.0.1:19002 TS_RAFT_WAL_DIR=target/raft-node-2 \
TS_RAFT_NODES='1=127.0.0.1:19001,2=127.0.0.1:19002,3=127.0.0.1:19003' \
cargo run -p temporalstore-rust --bin raft_node

TS_RAFT_NODE_ID=3 TS_RAFT_BIND_ADDR=127.0.0.1:19003 TS_RAFT_WAL_DIR=target/raft-node-3 \
TS_RAFT_NODES='1=127.0.0.1:19001,2=127.0.0.1:19002,3=127.0.0.1:19003' \
cargo run -p temporalstore-rust --bin raft_node
```

Then send writes to the leader node:

```bash
curl -s http://127.0.0.1:19001/raft/propose \
  -H 'content-type: application/json' \
  -d '{"command":{"kind":"string_set","key":"k","value":[118]}}'
```

## Local Data-Node Raft Harnesses

For a one-command local proof that the data-node Raft HTTP transport replicates between three
separate node endpoints and writes local WAL segment files, run:

```bash
CARGO_TARGET_DIR=/tmp/temporalstore-rust-target \
cargo run -p temporalstore-rust --bin distributed_raft_harness
```

The harness starts three data-node Raft runtimes on different loopback ports, proposes a write
through node 1, waits until reads from nodes 1, 2, and 3 return the replicated value, and prints a
JSON report with each node address, Raft status, replica read result, and WAL files.

For a stronger process-level secondary replication check, build all binaries and run:

```bash
CARGO_TARGET_DIR=/tmp/temporalstore-rust-target \
cargo build -p temporalstore-rust --bins

CARGO_TARGET_DIR=/tmp/temporalstore-rust-target \
cargo run -p temporalstore-rust --bin raft_secondary_replication_harness
```

This harness starts three real `raft_node` OS processes with `TS_RAFT_ENABLE_LOCAL_ADMIN=true`,
writes through the leader, kills secondary node 3, writes while that secondary is down, restarts
node 3 with the same local WAL directory, waits until all nodes can read the pre-stop, while-down,
and after-restart keys, then kills leader node 1, marks it dead in the surviving local-chaos
runtimes, triggers failover on node 2, writes through the new leader, and verifies the surviving
replicas can read the post-failover key.

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
raft_install_snapshot_request_carries_external_snapshot_reference
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
wal_backed_raft_cluster_auto_persists_commits_leadership_and_membership
wal_backed_raft_cluster_compacts_wal_tail_but_recovers_latest_state
distributed_raft_readiness_reports_remaining_production_blockers
production_raft_mode_is_blocked_until_real_engine_and_chaos_exist
production_raft_runtime_validates_security_timer_and_chaos_contracts
production_raft_runtime_replicates_over_separate_http_nodes
raft_secondary_replication_harness
metaserver_raft_health_catchup_safe_scale_and_failover_work
metaserver_raft_promotes_follower_after_leader_failure_and_keeps_metadata_available
metaserver_raft_rejects_reads_and_writes_without_majority
metaserver_mutation_log_recovers_routes_tables_and_state_changes
```

These tests cover the production wrapper, runtime validation, timers, RPC/auth construction, local
chaos contracts, local model behavior, and transport contracts.
