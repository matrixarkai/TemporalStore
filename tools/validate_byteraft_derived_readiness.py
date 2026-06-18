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
