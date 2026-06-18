#!/usr/bin/env python3
"""Validate ByteRaft-derived production-readiness evidence in Rust Raft.

TemporalStore C++ relies on ByteRaft behavior beyond basic log replication:
durable hard state, joint membership, bounded reads, leader transfer, learner
promotion, lag-aware catch-up, snapshot install, election guards, and operator
control surfaces. This guard keeps those Rust readiness contracts explicit and
fast to check before heavier distributed harnesses run.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


@dataclass(frozen=True)
class Evidence:
    path: str
    snippets: tuple[str, ...]


@dataclass(frozen=True)
class ReadinessArea:
    name: str
    evidence: tuple[Evidence, ...]


AREAS: tuple[ReadinessArea, ...] = (
    ReadinessArea(
        name="config_and_election_guards",
        evidence=(
            Evidence(
                "crates/temporalstore-rust/src/raft.rs",
                (
                    "RaftConfig",
                    "enable_pre_vote",
                    "prohibits_election",
                    "max_memory_replicate_log_bytes",
                    "raft_config_rejects_oversized_log_entries_and_prohibited_elections",
                    "raft_tick_election_waits_for_timeout_and_prevotes_before_promotion",
                    "raft_prevote_rejects_candidate_without_quorum",
                ),
            ),
        ),
    ),
    ReadinessArea(
        name="durable_wal_hard_state_and_membership",
        evidence=(
            Evidence(
                "crates/temporalstore-rust/src/raft.rs",
                (
                    "LocalRaftWal",
                    "RaftHardState",
                    "hard_state",
                    "joint_membership",
                    "local_raft_wal_persists_hard_state_membership_and_entries",
                    "joint_consensus_state_survives_wal_restore_and_still_requires_both_majorities",
                ),
            ),
        ),
    ),
    ReadinessArea(
        name="joint_membership_and_safe_scale",
        evidence=(
            Evidence(
                "crates/temporalstore-rust/src/raft.rs",
                (
                    "JointConsensusMembership",
                    "begin_joint_consensus",
                    "commit_joint_consensus",
                    "apply_membership_change_safely",
                    "safe_membership_change_adds_voter_through_joint_consensus",
                    "joint_consensus_requires_old_and_new_majorities_before_commit_or_write",
                ),
            ),
            Evidence(
                "crates/temporalstore-rust/src/bin/distributed_raft_harness.rs",
                ("apply_membership_on_all", "rescale_down_after_snapshot", "rescale_up_after_snapshot"),
            ),
        ),
    ),
    ReadinessArea(
        name="linearizable_and_bounded_reads",
        evidence=(
            Evidence(
                "crates/temporalstore-rust/src/raft.rs",
                (
                    "RaftReadOptions",
                    "DataRaftReadPolicy",
                    "ReadIndexResponse",
                    "read_index",
                    "leader_lease_valid",
                    "raft_leader_lease_expiry_blocks_linearizable_reads_and_writes_until_heartbeat",
                    "data_raft_read_policy_matches_cpp_partition_manager_modes",
                    "raft_read_index_and_transfer_reject_lagging_replica",
                ),
            ),
        ),
    ),
    ReadinessArea(
        name="learner_promotion_campaign_and_leader_transfer",
        evidence=(
            Evidence(
                "crates/temporalstore-rust/src/raft.rs",
                (
                    "bootstrap_as_learner",
                    "auto_promote",
                    "add_learner",
                    "promote_peer",
                    "transfer_leader",
                    "campaign",
                    "openraft_data_node_backend_bootstraps_learner_and_auto_promotes_peer",
                    "openraft_data_node_backend_persists_log_snapshot_read_index_and_leader_transfer",
                ),
            ),
            Evidence(
                "crates/temporalstore-rust/src/bin/metaserver_raft_harness.rs",
                ("transfer_leader(11)", "leader_after_transfer", "leader_after_failover"),
            ),
        ),
    ),
    ReadinessArea(
        name="snapshot_install_bootstrap_and_catchup",
        evidence=(
            Evidence(
                "crates/temporalstore-rust/src/raft.rs",
                (
                    "RaftSnapshot",
                    "RaftExternalSnapshotRef",
                    "RaftSnapshotTransferPolicy",
                    "install_snapshot",
                    "install_snapshot_chunk",
                    "bootstrap_replica_from_external_snapshot",
                    "raft_election_does_not_depend_on_snapshot_availability",
                ),
            ),
            Evidence(
                "crates/temporalstore-rust/src/bin/distributed_raft_harness.rs",
                ("external_snapshot_read", "bootstrap_external_snapshot", "catch_up"),
            ),
        ),
    ),
    ReadinessArea(
        name="replication_health_lag_and_failover",
        evidence=(
            Evidence(
                "crates/temporalstore-rust/src/raft.rs",
                (
                    "RaftReplicationHealth",
                    "RaftApplyHealth",
                    "RaftCatchUpReport",
                    "RaftFailoverReport",
                    "catch_up_live_followers",
                    "replication_health_reports_lag_and_heartbeat_catches_up_secondary",
                    "raft_follower_catches_up_after_outage",
                ),
            ),
            Evidence(
                "crates/temporalstore-rust/src/bin/raft_secondary_replication_harness.rs",
                ("partition", "reads_after_restart", "membership_scale_down", "membership_scale_up"),
            ),
        ),
    ),
    ReadinessArea(
        name="operator_status_metrics_and_local_status",
        evidence=(
            Evidence(
                "crates/temporalstore-rust/src/raft.rs",
                (
                    "RaftClusterStatus",
                    "RaftNodeStatus",
                    "local_status",
                    "prometheus_metrics",
                    "raft_status_prometheus",
                    "temporalstore_raft_cluster_commit_index",
                    "temporalstore_raft_cluster_has_majority",
                    "temporalstore_raft_node_commit_index",
                    "temporalstore_raft_node_applied_index",
                    "temporalstore_raft_node_lag",
                    "temporalstore_raft_node_apply_lag",
                    "raft_status_read_index_and_transfer_leader_match_engine_control_shape",
                    "raft_apply_health_reports_commit_to_apply_lag",
                    "metaserver_raft_status_read_index_and_transfer_leader_work",
                    "metaserver_raft_apply_health_reports_commit_to_apply_lag",
                    "byteraft_operator_observability_present",
                ),
            ),
            Evidence(
                "crates/temporalstore-rust/src/bin/raft_node.rs",
                ("/raft/status", "/raft/apply_health", "local_apply_health"),
            ),
            Evidence(
                "crates/temporalstore-rust/src/bin/server.rs",
                ("/raft/status", "/raft/apply_health", "local_apply_health"),
            ),
        ),
    ),
    ReadinessArea(
        name="rpc_retry_backpressure_auth_deadline",
        evidence=(
            Evidence(
                "crates/temporalstore-rust/src/raft.rs",
                (
                    "RaftRpcRuntime",
                    "RaftRpcRuntimeOptions",
                    "RaftRpcRuntimeMetrics",
                    "RaftRpcMetadata",
                    "AuthenticatedRaftTransport",
                    "handle_authenticated_raft_http",
                    "validate_raft_rpc_metadata",
                    "backpressure_rejections",
                    "retry_backoff_ms",
                    "deadline_ms",
                    "auth_token_required",
                    "raft_rpc_runtime_retries_transport_errors_and_releases_inflight",
                    "raft_rpc_runtime_attaches_auth_and_deadline_metadata",
                    "byteraft_rpc_transport_contract_present",
                ),
            ),
            Evidence(
                "crates/temporalstore-rust/src/bin/raft_node.rs",
                ("handle_authenticated_raft_http", "TS_RAFT_RPC_DEADLINE_MS", "TS_RAFT_AUTH_TOKEN"),
            ),
            Evidence(
                "crates/temporalstore-rust/src/bin/server.rs",
                ("handle_authenticated_raft_http", "TS_RAFT_RPC_DEADLINE_MS", "TS_RAFT_AUTH_TOKEN"),
            ),
            Evidence(
                "crates/temporalstore-rust/src/bin/raft_secondary_replication_harness.rs",
                ("unauthorized_rejection", "RaftRpcMetadata", "deadline_ms"),
            ),
        ),
    ),
    ReadinessArea(
        name="log_retention_and_snapshot_trigger",
        evidence=(
            Evidence(
                "crates/temporalstore-rust/src/raft.rs",
                (
                    "RaftSnapshotTriggerReport",
                    "max_disk_replicate_log_num",
                    "max_applied_log_bytes",
                    "persist_node_with_retention",
                    "compact_node_records",
                    "maybe_trigger_snapshot",
                    "applied_log_bytes_threshold",
                    "local_raft_wal_segments_roll_retain_and_recover_latest_state",
                    "wal_backed_raft_cluster_compacts_wal_tail_but_recovers_latest_state",
                    "data_raft_snapshot_trigger_compacts_applied_log_bytes",
                    "metaserver_raft_snapshot_trigger_compacts_applied_log_bytes",
                    "byteraft_log_retention_snapshot_trigger_present",
                ),
            ),
            Evidence(
                "docs/distributed_raft_readiness.md",
                ("bounded local WAL retention", "deterministic ByteRaft-style snapshot trigger reports"),
            ),
        ),
    ),
    ReadinessArea(
        name="apply_snapshot_durability_fence",
        evidence=(
            Evidence(
                "crates/temporalstore-rust/src/raft.rs",
                (
                    "RaftApplySnapshotFence",
                    "apply_snapshot_fence",
                    "raft_apply_snapshot_fence",
                    "validate_raft_apply_snapshot_fence",
                    "ApplySnapshotFence",
                    "wal_backed_apply_snapshot_fence_survives_snapshot_restart",
                    "wal_recovery_rejects_inconsistent_apply_snapshot_fence",
                    "byteraft_apply_snapshot_fence_present",
                ),
            ),
            Evidence(
                "docs/distributed_raft_readiness.md",
                ("durable apply/snapshot fence", "applied-index/storage/snapshot atomicity contract"),
            ),
        ),
    ),
    ReadinessArea(
        name="snapshot_floor_log_matching",
        evidence=(
            Evidence(
                "crates/temporalstore-rust/src/raft.rs",
                (
                    "node_next_log_index",
                    "node_term_at_log_or_snapshot_index",
                    "build_append_entries_request",
                    "receive_append_entries",
                    "append_entries_matches_snapshot_floor_after_leader_compaction",
                    "byteraft_snapshot_floor_log_matching_present",
                ),
            ),
            Evidence(
                "docs/distributed_raft_readiness.md",
                ("snapshot-floor log matching", "post-compaction AppendEntries continuity"),
            ),
        ),
    ),
    ReadinessArea(
        name="snapshot_tail_catchup",
        evidence=(
            Evidence(
                "crates/temporalstore-rust/src/raft.rs",
                (
                    "install_leader_snapshot_tail",
                    "install_snapshot_state_for_role",
                    "catch_up_live_followers_bounded",
                    "add_node_after_leader_snapshot_installs_snapshot_and_tail",
                    "byteraft_snapshot_tail_catchup_present",
                ),
            ),
            Evidence(
                "docs/distributed_raft_readiness.md",
                ("snapshot-tail catch-up", "replicas receive the leader snapshot floor"),
            ),
        ),
    ),
    ReadinessArea(
        name="compacted_entry_rejection",
        evidence=(
            Evidence(
                "crates/temporalstore-rust/src/raft.rs",
                (
                    "append_entry",
                    "last_included_index",
                    "append_entries_ignores_entries_at_or_below_snapshot_floor",
                    "byteraft_compacted_entry_rejection_present",
                ),
            ),
            Evidence(
                "docs/distributed_raft_readiness.md",
                ("compacted-entry rejection", "snapshot floor are ignored"),
            ),
        ),
    ),
    ReadinessArea(
        name="operator_control_surfaces",
        evidence=(
            Evidence(
                "crates/temporalstore-rust/src/bin/raft_node.rs",
                (
                    "/raft/control/list_membership",
                    "/raft/control/add_node",
                    "/raft/control/remove_node",
                    "/raft/control/read_index",
                    "/raft/control/transfer_leader",
                    "/raft/admin/bootstrap_external_snapshot",
                ),
            ),
            Evidence(
                "crates/temporalstore-rust/src/bin/server.rs",
                (
                    "/raft/membership/apply",
                    "/raft/control/list_membership",
                    "/raft/control/add_node",
                    "/raft/control/remove_node",
                    "/raft/control/read_index",
                    "/raft/control/transfer_leader",
                ),
            ),
        ),
    ),
)


def read(path: str) -> str:
    target = ROOT / path
    if not target.exists():
        raise SystemExit(f"missing readiness evidence file: {path}")
    return target.read_text(encoding="utf-8")


def main() -> int:
    missing: list[str] = []
    checked_snippets = 0
    for area in AREAS:
        for evidence in area.evidence:
            text = read(evidence.path)
            for snippet in evidence.snippets:
                checked_snippets += 1
                if snippet not in text:
                    missing.append(f"{area.name}: {evidence.path}: {snippet}")

    if missing:
        details = "\n".join(f"- {item}" for item in missing)
        raise SystemExit(f"missing ByteRaft-derived readiness evidence:\n{details}")

    print(f"byteraft_derived_readiness_areas={len(AREAS)}")
    print(f"byteraft_derived_readiness_snippets={checked_snippets}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
