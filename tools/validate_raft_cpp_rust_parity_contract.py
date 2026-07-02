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


def _load_json(path: pathlib.Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8-sig"))


def _validate_contract_doc() -> list[str]:
    text = CONTRACT.read_text(encoding="utf-8")
    failures: list[str] = []
    for name in CANONICAL_RAFT_TYPES + REQUIRED_RAFT_TOP_LEVEL_KEYS + REQUIRED_RAFT_SUBSYSTEM_KEYS:
        if f"`{name}`" not in text:
            failures.append(f"contract missing `{name}`")
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

    cpp_contract = cpp_report.get("raft_public_contract")
    rust_contract = rust_report.get("raft_public_contract")
    if isinstance(cpp_contract, dict) and isinstance(rust_contract, dict) and cpp_contract != rust_contract:
        failures.append("C++/Rust raft_public_contract drift")

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
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
