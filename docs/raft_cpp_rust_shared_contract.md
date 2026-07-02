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
- `test_matrix`
- `fail_closed_gates`
- `report_summary`

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
- `metrics`

`metrics` must include these required fields for both metaserver and data-node
Raft reports:

- `leader_election_ms`
- `term_changes`
- `commit_index`
- `applied_index`
- `membership_change_count`
- `topology_ready_ms`
- `snapshot_restore_ms`
- `failed_ready_checks`
- `stale_leader_observed`

`data_node_raft.metrics` must also include these replicated read/write
performance and correctness fields:

- `append_qps`
- `replication_p50_ms`
- `replication_p95_ms`
- `replication_p99_ms`
- `apply_lag_max`
- `commit_lag_max`
- `follower_visible_lag_ms`
- `failover_recovery_ms`
- `snapshot_install_ms`
- `quorum_write_failures`
- `stale_read_count`

## Required Evidence

`membership_events` use `MembershipConfig`, `ReplicaSet`, and `LearnerSet`
semantics. `leader_election_events` use `Term`, `LeaderId`, `CommitIndex`, and
`AppliedIndex` semantics. `failover_metrics` must report
`RaftFailoverEvent` evidence.

## Phase 1 Metaserver Raft Behavior

`metaserver_raft.behavior_evidence` must include these behavior keys, each with
`status: passed`, before metaserver Raft parity can be marked feature-correct:

- `leader_election`
- `namespace_table_creation`
- `slot_assignment`
- `primary_placement`
- `topology_readiness`
- `membership_add_remove`
- `follower_catch_up`
- `leader_failover`
- `restart_recovery`
- `snapshot_restore`

## Phase 2 Data Node Raft Behavior

`data_node_raft.behavior_evidence` must include these behavior keys, each with
`status: passed`, before data-node Raft parity can be marked feature-correct:

- `append_replication`
- `quorum_write`
- `async_sync_apply`
- `follower_visibility`
- `leader_failover`
- `replica_add_remove`
- `learner_promotion`
- `snapshot_install`
- `apply_lag_recovery`
- `read_after_write_under_leader_change`

## Phase 3 Unified Test Matrix

`test_matrix` must include these cases, each with `status: passed`, for C++
and Rust before unified Raft parity can be marked feature-correct:

- `three_node_metaserver_raft`
- `three_node_data_node_raft`
- `combined_metaserver_data_node_raft`
- `leader_kill_restart`
- `follower_kill_restart`
- `add_replica`
- `remove_replica`
- `learner_catch_up`
- `snapshot_restore`
- `network_delay_simulation`
- `disk_restart_recovery`
- `stale_follower_cursor_blocks_unsafe_reclaim`

## Phase 4 Fail-Closed Gates

`fail_closed_gates` must include these gates, each with `status: passed`:

- `same_quorum_rule`
- `commit_applied_index_no_unexpected_drift`
- `no_stale_follower_reads_when_ready`
- `membership_change_result_match`
- `snapshot_restore_record_count_checksum_match`
- `metaserver_ready_after_slot_primary_assignment`
- `data_node_unhealthy_when_apply_lag_exceeds_threshold`

The gate evidence must include:

- `quorum_rule`
- `commit_index_drift`
- `applied_index_drift`
- `max_allowed_drift`
- `readiness_status`
- `stale_read_count`
- `membership_change_result`
- `record_count_match`
- `checksum_match`
- `topology_ready`
- `slot_assignment_complete`
- `primary_assignment_complete`
- `raft_health_status`
- `apply_lag_max`
- `apply_lag_threshold`

C++ and Rust must report the same `quorum_rule` and
`membership_change_result`; otherwise parity fails closed.

## Phase 5 Shared Report Summary

`report_summary` must include these fields:

- `command`
- `backend`
- `storage_mode`
- `metaserver_status`
- `data_node_status`
- `leader_election_result`
- `membership_result`
- `failover_result`
- `snapshot_result`
- `latency_qps`
- `errors`
- `open_blockers`
- `status_labels`

`parity_status` uses these labels:

- `feature_correct`: shared Raft contract passes
- `performance_candidate`: live C++ and Rust runs complete under same config
- `production_performance_parity`: failover/recovery/QPS/latency within thresholds

`report_summary.status_labels` must repeat these labels so a single shared
report can be rendered without digging into backend internals.
`report_summary.status_label_descriptions` must repeat the exact label
definitions.

If `data_node_unhealthy_when_apply_lag_exceeds_threshold` reports
`raft_health_status: healthy` while `apply_lag_max` is greater than
`apply_lag_threshold`, parity fails closed.

`tools/validate_raft_cpp_rust_parity_contract.py` validates this contract and
the synthetic C++/Rust report pair in
`compat/raft_parity_report_pair_corpus.json`.
