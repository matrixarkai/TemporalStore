# C++/Rust Raft Shared Contract

This is the Phase 0 contract for C++ and Rust Raft parity across metaserver
and data-node modes. Live performance and failover claims must use reports that
expose this public shape before comparison.

## Canonical Public Types

Reports and public APIs use these names exactly:

- `RaftNodeId`
- `RaftGroupId`
- `Term`
- `LogIndex`
- `CommitIndex`
- `AppliedIndex`
- `SnapshotIndex`
- `LeaderId`
- `MembershipConfig`
- `ReplicaSet`
- `LearnerSet`
- `RaftHealth`
- `RaftFailoverEvent`

Implementation-specific names may remain private, but comparison artifacts must
normalize into these public names.

## Canonical Operational Report Shape

C++ and Rust reports must expose the same operational top-level keys:

- `raft_backend_identity`
- `metaserver_raft`
- `data_node_raft`
- `membership_events`
- `leader_election_events`
- `replication_metrics`
- `failover_metrics`
- `snapshot_restore_metrics`
- `readiness`
- `parity_status`

Reports also include metadata top-level keys:

- `schema_version`
- `raft_public_contract`

The full C++ and Rust top-level key sets must match exactly.

## Required Subsystem Shape

`metaserver_raft` and `data_node_raft` must both expose:

- `raft_group_id`
- `leader_id`
- `term`
- `commit_index`
- `applied_index`
- `snapshot_index`
- `membership_config`
- `replica_set`
- `learner_set`
- `raft_health`

## Required Evidence

`membership_events` use `MembershipConfig`, `ReplicaSet`, and `LearnerSet`
semantics. `leader_election_events` use `Term`, `LeaderId`, `CommitIndex`, and
`AppliedIndex` semantics. `failover_metrics` must report
`RaftFailoverEvent` evidence.

`tools/validate_raft_cpp_rust_parity_contract.py` validates this contract and
the synthetic C++/Rust report pair in
`compat/raft_parity_report_pair_corpus.json`.
