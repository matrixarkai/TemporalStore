#!/usr/bin/env python3
"""Validate the shared C++/Rust Raft Phase 0 public contract."""

from __future__ import annotations

import argparse
import json
import pathlib
import sys
from typing import Any


ROOT = pathlib.Path(__file__).resolve().parents[1]
CONTRACT = ROOT / "docs" / "raft_cpp_rust_shared_contract.md"
REPORT_PAIR_CORPUS = ROOT / "compat" / "raft_parity_report_pair_corpus.json"
METASERVER_REPORT_PAIR_CORPUS = ROOT / "compat" / "raft_metaserver_parity_report_pair.json"
DATA_NODE_REPORT_PAIR_CORPUS = ROOT / "compat" / "raft_data_node_parity_report_pair.json"

CANONICAL_RAFT_TYPES = [
    "RaftNodeId",
    "RaftGroupId",
    "Term",
    "LogIndex",
    "CommitIndex",
    "AppliedIndex",
    "SnapshotIndex",
    "LeaderId",
    "MembershipConfig",
    "ReplicaSet",
    "LearnerSet",
    "RaftHealth",
    "RaftFailoverEvent",
]

CANONICAL_RAFT_CONTRACT_FIELDS = [
    "raft_node_id",
    "raft_group_id",
    "term",
    "log_index",
    "commit_index",
    "applied_index",
    "snapshot_index",
    "leader_id",
    "membership_config",
    "replica_set",
    "learner_set",
    "raft_health",
    "raft_failover_event",
]

REQUIRED_RAFT_TOP_LEVEL_KEYS = [
    "schema_version",
    "raft_public_contract",
    "raft_backend_identity",
    "metaserver_raft",
    "data_node_raft",
    "membership_events",
    "leader_election_events",
    "replication_metrics",
    "failover_metrics",
    "snapshot_restore_metrics",
    "readiness",
    "parity_status",
    "test_matrix",
    "fail_closed_gates",
    "report_summary",
]

REQUIRED_RAFT_OPERATIONAL_TOP_LEVEL_KEYS = [
    "raft_backend_identity",
    "metaserver_raft",
    "data_node_raft",
    "membership_events",
    "leader_election_events",
    "replication_metrics",
    "failover_metrics",
    "snapshot_restore_metrics",
    "readiness",
    "parity_status",
    "test_matrix",
    "fail_closed_gates",
    "report_summary",
]

REQUIRED_RAFT_SUBSYSTEM_KEYS = [
    "raft_group_id",
    "leader_id",
    "term",
    "commit_index",
    "applied_index",
    "snapshot_index",
    "membership_config",
    "replica_set",
    "learner_set",
    "raft_health",
]

REQUIRED_METASERVER_RAFT_BEHAVIORS = [
    "leader_election",
    "namespace_table_creation",
    "slot_assignment",
    "primary_placement",
    "topology_readiness",
    "membership_add_remove",
    "follower_catch_up",
    "leader_failover",
    "restart_recovery",
    "snapshot_restore",
]

REQUIRED_DATA_NODE_RAFT_BEHAVIORS = [
    "append_replication",
    "quorum_write",
    "async_sync_apply",
    "follower_visibility",
    "leader_failover",
    "replica_add_remove",
    "learner_promotion",
    "snapshot_install",
    "apply_lag_recovery",
    "read_after_write_under_leader_change",
]

REQUIRED_RAFT_METRICS = [
    "leader_election_ms",
    "term_changes",
    "commit_index",
    "applied_index",
    "membership_change_count",
    "topology_ready_ms",
    "snapshot_restore_ms",
    "failed_ready_checks",
    "stale_leader_observed",
]

REQUIRED_DATA_NODE_RAFT_METRICS = [
    "append_qps",
    "replication_p50_ms",
    "replication_p95_ms",
    "replication_p99_ms",
    "apply_lag_max",
    "commit_lag_max",
    "follower_visible_lag_ms",
    "failover_recovery_ms",
    "snapshot_install_ms",
    "quorum_write_failures",
    "stale_read_count",
]

REQUIRED_RAFT_TEST_MATRIX_CASES = [
    "three_node_metaserver_raft",
    "three_node_data_node_raft",
    "combined_metaserver_data_node_raft",
    "leader_kill_restart",
    "follower_kill_restart",
    "add_replica",
    "remove_replica",
    "learner_catch_up",
    "snapshot_restore",
    "network_delay_simulation",
    "disk_restart_recovery",
    "stale_follower_cursor_blocks_unsafe_reclaim",
]

REQUIRED_RAFT_FAIL_CLOSED_GATES = [
    "same_quorum_rule",
    "commit_applied_index_no_unexpected_drift",
    "no_stale_follower_reads_when_ready",
    "membership_change_result_match",
    "snapshot_restore_record_count_checksum_match",
    "metaserver_ready_after_slot_primary_assignment",
    "data_node_unhealthy_when_apply_lag_exceeds_threshold",
]

REQUIRED_RAFT_FAIL_CLOSED_GATE_FIELDS = {
    "same_quorum_rule": ["quorum_rule"],
    "commit_applied_index_no_unexpected_drift": [
        "commit_index_drift",
        "applied_index_drift",
        "max_allowed_drift",
    ],
    "no_stale_follower_reads_when_ready": ["readiness_status", "stale_read_count"],
    "membership_change_result_match": ["membership_change_result"],
    "snapshot_restore_record_count_checksum_match": ["record_count_match", "checksum_match"],
    "metaserver_ready_after_slot_primary_assignment": [
        "topology_ready",
        "slot_assignment_complete",
        "primary_assignment_complete",
    ],
    "data_node_unhealthy_when_apply_lag_exceeds_threshold": [
        "raft_health_status",
        "apply_lag_max",
        "apply_lag_threshold",
    ],
}

PAIRWISE_RAFT_FAIL_CLOSED_FIELDS = {
    "same_quorum_rule": ["quorum_rule"],
    "membership_change_result_match": ["membership_change_result"],
}

REQUIRED_RAFT_REPORT_SUMMARY_KEYS = [
    "command",
    "backend",
    "storage_mode",
    "metaserver_status",
    "data_node_status",
    "leader_election_result",
    "membership_result",
    "failover_result",
    "snapshot_result",
    "latency_qps",
    "errors",
    "open_blockers",
]


def _load_json(path: pathlib.Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8-sig"))


def _validate_contract_doc() -> list[str]:
    text = CONTRACT.read_text(encoding="utf-8")
    failures: list[str] = []
    for name in (
        CANONICAL_RAFT_TYPES
        + REQUIRED_RAFT_TOP_LEVEL_KEYS
        + REQUIRED_RAFT_SUBSYSTEM_KEYS
        + REQUIRED_METASERVER_RAFT_BEHAVIORS
        + REQUIRED_DATA_NODE_RAFT_BEHAVIORS
        + REQUIRED_RAFT_METRICS
        + REQUIRED_DATA_NODE_RAFT_METRICS
        + REQUIRED_RAFT_TEST_MATRIX_CASES
        + REQUIRED_RAFT_FAIL_CLOSED_GATES
        + sorted({field for fields in REQUIRED_RAFT_FAIL_CLOSED_GATE_FIELDS.values() for field in fields})
        + REQUIRED_RAFT_REPORT_SUMMARY_KEYS
    ):
        if f"`{name}`" not in text:
            failures.append(f"contract missing `{name}`")
    return failures


def _validate_metaserver_behaviors(backend: str, metaserver: dict[str, Any]) -> list[str]:
    return _validate_behavior_evidence(backend, "metaserver_raft", metaserver, REQUIRED_METASERVER_RAFT_BEHAVIORS)


def _validate_data_node_behaviors(backend: str, data_node: dict[str, Any]) -> list[str]:
    return _validate_behavior_evidence(backend, "data_node_raft", data_node, REQUIRED_DATA_NODE_RAFT_BEHAVIORS)


def _validate_behavior_evidence(
    backend: str,
    section: str,
    subsystem: dict[str, Any],
    required_behaviors: list[str],
) -> list[str]:
    failures: list[str] = []
    evidence = subsystem.get("behavior_evidence")
    if not isinstance(evidence, dict):
        return [f"{backend} {section} missing object `behavior_evidence`"]
    for behavior in required_behaviors:
        item = evidence.get(behavior)
        if not isinstance(item, dict):
            failures.append(f"{backend} {section}.behavior_evidence missing `{behavior}`")
            continue
        if item.get("status") != "passed":
            failures.append(
                f"{backend} {section}.behavior_evidence.{behavior} status drift: {item.get('status')!r}"
            )
    return failures


def _validate_raft_metrics(backend: str, section: str, subsystem: dict[str, Any]) -> list[str]:
    failures: list[str] = []
    metrics = subsystem.get("metrics")
    if not isinstance(metrics, dict):
        return [f"{backend} {section} missing object `metrics`"]
    for metric in REQUIRED_RAFT_METRICS:
        if metric not in metrics:
            failures.append(f"{backend} {section}.metrics missing `{metric}`")
    if section == "data_node_raft":
        for metric in REQUIRED_DATA_NODE_RAFT_METRICS:
            if metric not in metrics:
                failures.append(f"{backend} {section}.metrics missing `{metric}`")
    return failures


def _validate_named_passed_map(backend: str, section: str, value: Any, required: list[str]) -> list[str]:
    failures: list[str] = []
    if not isinstance(value, dict):
        return [f"{backend} report missing object `{section}`"]
    for name in required:
        item = value.get(name)
        if not isinstance(item, dict):
            failures.append(f"{backend} {section} missing `{name}`")
            continue
        if item.get("status") != "passed":
            failures.append(f"{backend} {section}.{name} status drift: {item.get('status')!r}")
    return failures


def _validate_fail_closed_gates(backend: str, gates: Any) -> list[str]:
    failures = _validate_named_passed_map(backend, "fail_closed_gates", gates, REQUIRED_RAFT_FAIL_CLOSED_GATES)
    if not isinstance(gates, dict):
        return failures
    for gate, fields in REQUIRED_RAFT_FAIL_CLOSED_GATE_FIELDS.items():
        item = gates.get(gate)
        if not isinstance(item, dict):
            continue
        for field in fields:
            if field not in item:
                failures.append(f"{backend} fail_closed_gates.{gate} missing `{field}`")
    return failures


def _validate_fail_closed_pair(cpp_report: dict[str, Any], rust_report: dict[str, Any]) -> list[str]:
    failures: list[str] = []
    cpp_gates = cpp_report.get("fail_closed_gates")
    rust_gates = rust_report.get("fail_closed_gates")
    if not isinstance(cpp_gates, dict) or not isinstance(rust_gates, dict):
        return failures
    for gate, fields in PAIRWISE_RAFT_FAIL_CLOSED_FIELDS.items():
        cpp_gate = cpp_gates.get(gate)
        rust_gate = rust_gates.get(gate)
        if not isinstance(cpp_gate, dict) or not isinstance(rust_gate, dict):
            continue
        for field in fields:
            if cpp_gate.get(field) != rust_gate.get(field):
                failures.append(
                    f"fail_closed_gates.{gate}.{field} drift: "
                    f"cpp={cpp_gate.get(field)!r} rust={rust_gate.get(field)!r}"
                )
    return failures


def _validate_report_summary(backend: str, value: Any) -> list[str]:
    failures: list[str] = []
    if not isinstance(value, dict):
        return [f"{backend} report missing object `report_summary`"]
    for key in REQUIRED_RAFT_REPORT_SUMMARY_KEYS:
        if key not in value:
            failures.append(f"{backend} report_summary missing `{key}`")
    if value.get("backend") != backend:
        failures.append(f"{backend} report_summary.backend drift: {value.get('backend')!r}")
    if value.get("storage_mode") not in {"raft async", "raft sync"}:
        failures.append(f"{backend} report_summary.storage_mode drift: {value.get('storage_mode')!r}")
    latency_qps = value.get("latency_qps")
    if not isinstance(latency_qps, dict):
        failures.append(f"{backend} report_summary.latency_qps missing object")
    return failures


def validate_report_pair(cpp_report: dict[str, Any], rust_report: dict[str, Any]) -> list[str]:
    failures: list[str] = []
    if set(cpp_report.keys()) != set(rust_report.keys()):
        failures.append(
            "C++/Rust top-level report shape drift: "
            f"cpp_only={sorted(set(cpp_report.keys()) - set(rust_report.keys()))} "
            f"rust_only={sorted(set(rust_report.keys()) - set(cpp_report.keys()))}"
        )
    for backend, report in [("cpp", cpp_report), ("rust", rust_report)]:
        for key in REQUIRED_RAFT_TOP_LEVEL_KEYS:
            if key not in report:
                failures.append(f"{backend} report missing top-level `{key}`")
        for key in REQUIRED_RAFT_OPERATIONAL_TOP_LEVEL_KEYS:
            if key not in report:
                failures.append(f"{backend} report missing operational top-level `{key}`")
        contract = report.get("raft_public_contract")
        if not isinstance(contract, dict):
            failures.append(f"{backend} report missing object `raft_public_contract`")
            continue
        for field, type_name in zip(CANONICAL_RAFT_CONTRACT_FIELDS, CANONICAL_RAFT_TYPES, strict=True):
            if contract.get(field) != type_name:
                failures.append(f"{backend} raft_public_contract.{field} drift: {contract.get(field)!r}")
        for section in ["metaserver_raft", "data_node_raft"]:
            value = report.get(section)
            if not isinstance(value, dict):
                failures.append(f"{backend} report missing object `{section}`")
                continue
            for key in REQUIRED_RAFT_SUBSYSTEM_KEYS:
                if key not in value:
                    failures.append(f"{backend} {section} missing `{key}`")
            failures.extend(_validate_raft_metrics(backend, section, value))
            if section == "metaserver_raft":
                failures.extend(_validate_metaserver_behaviors(backend, value))
            if section == "data_node_raft":
                failures.extend(_validate_data_node_behaviors(backend, value))
        if not isinstance(report.get("membership_events"), list):
            failures.append(f"{backend} membership_events must be a list")
        if not isinstance(report.get("leader_election_events"), list):
            failures.append(f"{backend} leader_election_events must be a list")
        if not isinstance(report.get("failover_metrics", {}).get("raft_failover_event"), dict):
            failures.append(f"{backend} failover_metrics missing `raft_failover_event`")
        readiness = report.get("readiness")
        if not isinstance(readiness, dict) or "status" not in readiness:
            failures.append(f"{backend} readiness missing status")
        parity_status = report.get("parity_status")
        if not isinstance(parity_status, dict) or "feature_correct" not in parity_status:
            failures.append(f"{backend} parity_status missing feature_correct")
        elif not {"feature_correct", "performance_candidate", "production_performance_parity"}.issubset(parity_status):
            failures.append(f"{backend} parity_status missing required status labels")
        failures.extend(
            _validate_named_passed_map(
                backend,
                "test_matrix",
                report.get("test_matrix"),
                REQUIRED_RAFT_TEST_MATRIX_CASES,
            )
        )
        failures.extend(_validate_fail_closed_gates(backend, report.get("fail_closed_gates")))
        failures.extend(_validate_report_summary(backend, report.get("report_summary")))

    cpp_contract = cpp_report.get("raft_public_contract")
    rust_contract = rust_report.get("raft_public_contract")
    if isinstance(cpp_contract, dict) and isinstance(rust_contract, dict) and cpp_contract != rust_contract:
        failures.append("C++/Rust raft_public_contract drift")
    failures.extend(_validate_fail_closed_pair(cpp_report, rust_report))

    for section in ["metaserver_raft", "data_node_raft"]:
        cpp_section = cpp_report.get(section)
        rust_section = rust_report.get(section)
        if not isinstance(cpp_section, dict) or not isinstance(rust_section, dict):
            continue
        for key in ["raft_group_id", "commit_index", "applied_index", "snapshot_index"]:
            if key in cpp_section and key in rust_section and type(cpp_section[key]).__name__ != type(rust_section[key]).__name__:
                failures.append(
                    f"{section}.{key} type drift: cpp={type(cpp_section[key]).__name__} "
                    f"rust={type(rust_section[key]).__name__}"
                )
    return failures


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--cpp-report", type=pathlib.Path)
    parser.add_argument("--rust-report", type=pathlib.Path)
    args = parser.parse_args()

    failures = _validate_contract_doc()
    if bool(args.cpp_report) != bool(args.rust_report):
        failures.append("--cpp-report and --rust-report must be provided together")
    if args.cpp_report and args.rust_report:
        failures.extend(validate_report_pair(_load_json(args.cpp_report), _load_json(args.rust_report)))
    else:
        corpus = _load_json(REPORT_PAIR_CORPUS)
        failures.extend(validate_report_pair(corpus["cpp"], corpus["rust"]))

    if failures:
        for failure in failures:
            print(failure, file=sys.stderr)
        return 1

    print("raft C++/Rust Phase 0 shared contract passed:")
    print("- types=" + ", ".join(CANONICAL_RAFT_TYPES))
    print("- operational_top_level_shape=" + ", ".join(REQUIRED_RAFT_OPERATIONAL_TOP_LEVEL_KEYS))
    print("- metadata_top_level_shape=schema_version, raft_public_contract")
    print("- subsystem_shape=" + ", ".join(REQUIRED_RAFT_SUBSYSTEM_KEYS))
    print("- metaserver_behaviors=" + ", ".join(REQUIRED_METASERVER_RAFT_BEHAVIORS))
    print("- data_node_behaviors=" + ", ".join(REQUIRED_DATA_NODE_RAFT_BEHAVIORS))
    print("- required_metrics=" + ", ".join(REQUIRED_RAFT_METRICS))
    print("- data_node_required_metrics=" + ", ".join(REQUIRED_DATA_NODE_RAFT_METRICS))
    print("- test_matrix=" + ", ".join(REQUIRED_RAFT_TEST_MATRIX_CASES))
    print("- fail_closed_gates=" + ", ".join(REQUIRED_RAFT_FAIL_CLOSED_GATES))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
