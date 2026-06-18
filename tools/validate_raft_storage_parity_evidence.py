#!/usr/bin/env python3
"""Validate Rust evidence for the shared C++ storage/Raft parity gates.

The unified corpus proves that current C++ storage and Raft surfaces still
exist. This guard makes the other side explicit: every storage/Raft parity
surface must also have Rust implementation, test, or harness evidence checked
into this repo. It is intentionally static and fast so it can run in the local
unified validation pass before heavier harnesses start.
"""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
CORPUS = ROOT / "compat" / "unified_temporalstore_cases.json"
PARITY_SUITES = {"cpp_storage_parity", "cpp_data_raft_parity"}


@dataclass(frozen=True)
class RustEvidence:
    path: str
    snippets: tuple[str, ...]


@dataclass(frozen=True)
class ParityArea:
    name: str
    corpus_cases: tuple[str, ...]
    rust_evidence: tuple[RustEvidence, ...]


AREAS: tuple[ParityArea, ...] = (
    ParityArea(
        name="storage_object_page_slot_lifecycle",
        corpus_cases=("cpp_storage_object_page_slot_parity_surfaces",),
        rust_evidence=(
            RustEvidence(
                "crates/temporalstore-rust/src/engine.rs",
                ("SlotStorageSummary", "SlotDumpManifest", "StorageLifecyclePlan"),
            ),
            RustEvidence(
                "crates/temporalstore-rust/src/page_store.rs",
                ("PageAddress", "PageStoreSegmentReport", "PageStoreZoneManifest"),
            ),
        ),
    ),
    ParityArea(
        name="storage_slot_dump_load_recovery",
        corpus_cases=(
            "cpp_storage_oplog_index_replay_parity_surfaces",
            "cpp_storage_slot_context_test_parity_surfaces",
        ),
        rust_evidence=(
            RustEvidence(
                "crates/temporalstore-rust/tests/storage_migration_corpus.rs",
                (
                    "verify_engine_dump_load_recovery",
                    "install_slot_dump_manifest",
                    "assert_clean_recovery",
                ),
            ),
            RustEvidence(
                "crates/temporalstore-rust/tests/storage_crash_harness.rs",
                ("storage_crash_harness_recovers_after_abrupt_process_abort",),
            ),
        ),
    ),
    ParityArea(
        name="storage_compaction_gc_delayed_destroy",
        corpus_cases=(
            "cpp_storage_manager_compaction_gc_parity_surfaces",
            "cpp_storage_object_zone_evicter_expirer_parity_surfaces",
        ),
        rust_evidence=(
            RustEvidence(
                "crates/temporalstore-rust/src/page_store.rs",
                (
                    "gc_segments_before_with_live_refs_delayed_destroy",
                    "purge_delayed_destroy_segments_with_report",
                    "delayed_destroy_gc_quarantines_stale_segments_before_purge",
                ),
            ),
            RustEvidence(
                "crates/temporalstore-rust/src/engine.rs",
                ("StorageLifecycleReport", "delayed_destroy_page_segment_ids"),
            ),
        ),
    ),
    ParityArea(
        name="shared_store_sync_async_replication_gc",
        corpus_cases=(
            "cpp_storage_replicator_guardrail_parity_surfaces",
            "cpp_local_docker_replication_matrix_parity_surfaces",
        ),
        rust_evidence=(
            RustEvidence(
                "crates/temporalstore-rust/src/shared_store.rs",
                (
                    "shared_store_sync_storage_publishes_and_cursor_replay_resumes",
                    "shared_store_async_storage_flushes_in_order_with_limit",
                    "gc_oplog_before_cursor_safe",
                    "gc_checkpoints_cursor_safe",
                ),
            ),
            RustEvidence(
                "crates/temporalstore-rust/tests/storage_migration_corpus.rs",
                ("verify_shared_store_replay", "SharedStoreStorageMode::Sync", "SharedStoreStorageMode::Async"),
            ),
        ),
    ),
    ParityArea(
        name="raft_command_log_wal_codec",
        corpus_cases=(
            "cpp_data_raft_consensus_parity_surfaces",
            "cpp_data_raft_replication_parity_surfaces",
            "cpp_data_raft_unit_test_parity_surfaces",
        ),
        rust_evidence=(
            RustEvidence(
                "crates/temporalstore-rust/src/raft.rs",
                (
                    "DataRaftLogCodecEntry",
                    "serialize_data_raft_log",
                    "parse_data_raft_log",
                    "LocalRaftWal",
                    "data_raft_log_codec_round_trips_cxx_style_header",
                ),
            ),
            RustEvidence(
                "crates/temporalstore-rust/tests/temporalstore_compat.rs",
                ("consistency_bench_style_hash_writes_are_linearizable_through_raft",),
            ),
            RustEvidence(
                "compat/unified_temporalstore_cases.json",
                (
                    '"rust_parity_gate": "tools/run_raft_distributed_parity.sh"',
                    '"rust_parity_validator": "python3 tools/validate_aws_validation_log.py --job temporalstore-raft-distributed-parity-validation --log <raft-distributed-parity.json>"',
                ),
            ),
        ),
    ),
    ParityArea(
        name="raft_snapshot_membership_scale",
        corpus_cases=(
            "cpp_data_raft_snapshot_restore_harness_parity_surfaces",
            "cpp_data_raft_scale_transition_harness_parity_surfaces",
            "cpp_data_raft_multinode_scale_harness_parity_surfaces",
        ),
        rust_evidence=(
            RustEvidence(
                "crates/temporalstore-rust/src/raft.rs",
                ("install_snapshot_chunk", "apply_membership_change_safely", "read_index"),
            ),
            RustEvidence(
                "crates/temporalstore-rust/src/bin/distributed_raft_harness.rs",
                ("install_snapshot", "apply_membership_on_all", "transfer_leader"),
            ),
        ),
    ),
    ParityArea(
        name="data_node_raft_consensus_contract",
        corpus_cases=(
            "cpp_data_raft_consensus_parity_surfaces",
            "storage_data_raft_replication_gtest",
            "raft_data_node_scale_failover_snapshot",
            "raft_data_node_mixed_rw_and_membership",
        ),
        rust_evidence=(
            RustEvidence(
                "crates/temporalstore-rust/src/raft.rs",
                (
                    "bootstrap_as_learner",
                    "auto_promote",
                    "fatal_event_count",
                    "snapshot_creating",
                    "fn campaign",
                ),
            ),
            RustEvidence(
                "crates/temporalstore-rust/src/raft.rs",
                (
                    "openraft_data_node_backend_bootstraps_learner_and_auto_promotes_peer",
                    "openraft_data_node_backend_persists_log_snapshot_read_index_and_leader_transfer",
                    "backend.campaign",
                ),
            ),
        ),
    ),
    ParityArea(
        name="metaserver_raft_distributed_fault_contract",
        corpus_cases=(
            "cpp_metaserver_raft_harness_parity_surfaces",
            "raft_metaserver_membership_failover_snapshot",
            "raft_production_gate",
        ),
        rust_evidence=(
            RustEvidence(
                "crates/temporalstore-rust/src/raft.rs",
                (
                    "ProductionMetaRaftRuntime",
                    "list_membership",
                    "wait_for_log_applied",
                    "trigger_snapshot",
                    "transfer_leader",
                ),
            ),
            RustEvidence(
                "crates/temporalstore-rust/src/raft.rs",
                (
                    "production_meta_raft_runtime_matches_cpp_multinode_control_and_fault_contract",
                    "openraft_metaserver_backend_supports_membership_and_bounded_reads",
                    "metaserver_raft_promotes_follower_after_leader_failure_and_keeps_metadata_available",
                ),
            ),
            RustEvidence(
                "crates/temporalstore-rust/src/bin/metaserver_raft_harness.rs",
                (
                    "MetaserverRaftHarnessSummary",
                    "unsupported_role_rejected",
                    "unavailable_without_majority",
                    "snapshot_restore_read",
                    "lagging_snapshot_restore_missed_tail",
                    "lagging_catchup_read",
                    "membership_replace_after_failover",
                    "membership_scale_down_after_replace",
                ),
            ),
            RustEvidence(
                "compat/unified_temporalstore_cases.json",
                (
                    "temporalstore-unified-metaserver-raft-membership",
                    "temporalstore-unified-metaserver-raft-failover",
                    "temporalstore-unified-metaserver-raft-snapshot",
                    "temporalstore-metaserver-raft-validation",
                ),
            ),
        ),
    ),
    ParityArea(
        name="raft_failover_secondary_replication",
        corpus_cases=(
            "cpp_data_raft_failover_harness_parity_surfaces",
            "cpp_data_raft_mixed_rw_harness_parity_surfaces",
            "cpp_raft_production_stress_gate_parity_surfaces",
            "cpp_metaserver_raft_harness_parity_surfaces",
        ),
        rust_evidence=(
            RustEvidence(
                "crates/temporalstore-rust/src/bin/raft_secondary_replication_harness.rs",
                ("trigger_failover", "catch_up_peer", "local_catch_up", "rolling restart"),
            ),
            RustEvidence(
                "tools/run_temporalstore_parity_gate.sh",
                (
                    "distributed_raft_harness",
                    "metaserver_raft_harness",
                    "raft_secondary_replication_harness",
                    "raft-distributed-parity-validation",
                ),
            ),
            RustEvidence(
                "tools/run_temporalstore_unified_validation.sh",
                (
                    "data-node/metaserver raft distributed parity",
                    "run_raft_distributed_parity.sh",
                ),
            ),
        ),
    ),
    ParityArea(
        name="unified_cpp_raft_case_names",
        corpus_cases=(
            "storage_data_raft_replication_gtest",
            "raft_metaserver_membership_failover_snapshot",
            "raft_data_node_scale_failover_snapshot",
            "raft_data_node_mixed_rw_and_membership",
            "raft_production_gate",
        ),
        rust_evidence=(
            RustEvidence(
                "crates/temporalstore-rust/src/bin/distributed_raft_harness.rs",
                (
                    "transfer_leader",
                    "apply_membership_on_all",
                    "external_snapshot_bootstrap",
                    "rescale_down_after_snapshot",
                    "rescale_up_after_snapshot",
                ),
            ),
            RustEvidence(
                "crates/temporalstore-rust/src/bin/raft_secondary_replication_harness.rs",
                (
                    "reads_after_leader_crash",
                    "lagging_follower",
                    "rolling_restart",
                ),
            ),
            RustEvidence(
                "tools/run_storage_raft_production_readiness.sh",
                (
                    "distributed_raft_harness",
                    "metaserver_raft_harness",
                    "raft_secondary_replication_harness",
                    "external_chaos_gate",
                ),
            ),
            RustEvidence(
                "tools/run_raft_distributed_parity.sh",
                (
                    "distributed_raft_harness",
                    "raft_secondary_replication_harness",
                    "metaserver_raft_harness",
                    "raft-distributed-parity-validation",
                ),
            ),
            RustEvidence(
                "tools/build_raft_distributed_parity_summary.py",
                (
                    "distributed_all_nodes_have_majority",
                    "partition_isolated_read_rejected",
                    "lagging_catchup_read",
                    "rescale_down_voters",
                    "membership_replace_after_failover_voters",
                    "namespace_after_failover_visible",
                    "unavailable_without_majority",
                ),
            ),
            RustEvidence(
                "docs/distributed_raft_readiness.md",
                (
                    "storage_data_raft_replication_gtest",
                    "raft_data_node_scale_failover_snapshot",
                    "raft_production_gate",
                ),
            ),
            RustEvidence(
                "compat/unified_temporalstore_cases.json",
                (
                    '"required_raft_case_names"',
                    '"storage_data_raft_replication_gtest"',
                    '"raft_metaserver_membership_failover_snapshot"',
                    '"raft_data_node_scale_failover_snapshot"',
                    '"raft_data_node_mixed_rw_and_membership"',
                    '"raft_production_gate"',
                    '"metaserver_post_failover_replacement_scale_down"',
                    '"data_raft_post_snapshot_rescale"',
                    "tools/run_storage_raft_production_readiness.sh && tools/run_raft_distributed_parity.sh",
                    "temporalstore-raft-distributed-parity-validation",
                ),
            ),
            RustEvidence(
                "tools/run_cpp_raft_cases_on_rust.py",
                (
                    '"required_raft_case_names"',
                    "cpp_required_paths_checked",
                    "run_raft_distributed_parity.sh",
                ),
            ),
        ),
    ),
    ParityArea(
        name="local_scale_fault_readiness_gate",
        corpus_cases=(
            "cpp_redis_live_storage_smoke_parity_surfaces",
            "cpp_local_docker_replication_matrix_parity_surfaces",
        ),
        rust_evidence=(
            RustEvidence(
                "tools/run_temporalstore_unified_validation.sh",
                ("scale_harness", "storage_modes_harness", "readiness_gate"),
            ),
            RustEvidence(
                "tools/run_temporalstore_parity_gate.sh",
                ("storage_fault_matrix_harness", "validate_aws_validation_log.py"),
            ),
        ),
    ),
    ParityArea(
        name="byteraft_derived_readiness_contract",
        corpus_cases=(
            "cpp_data_raft_consensus_parity_surfaces",
            "cpp_data_raft_replication_parity_surfaces",
            "cpp_metaserver_raft_harness_parity_surfaces",
            "raft_production_gate",
        ),
        rust_evidence=(
            RustEvidence(
                "tools/validate_byteraft_derived_readiness.py",
                (
                    "config_and_election_guards",
                    "durable_wal_hard_state_and_membership",
                    "joint_membership_and_safe_scale",
                    "linearizable_and_bounded_reads",
                    "learner_promotion_campaign_and_leader_transfer",
                    "snapshot_install_bootstrap_and_catchup",
                    "replication_health_lag_and_failover",
                    "operator_control_surfaces",
                ),
            ),
        ),
    ),
)


def load_corpus() -> dict:
    with CORPUS.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def case_map(corpus: dict) -> dict[str, dict]:
    cases = corpus.get("cases")
    if not isinstance(cases, list):
        raise SystemExit(f"{CORPUS}: cases must be a list")
    return {case["name"]: case for case in cases}


def validate_corpus_area(area: ParityArea, cases: dict[str, dict]) -> set[str]:
    required = set(load_corpus()["coverage"]["required_case_names"])
    existing_paths: set[str] = set()
    for case_name in area.corpus_cases:
        if case_name not in required:
            raise SystemExit(f"{area.name}: {case_name} missing from coverage.required_case_names")
        case = cases.get(case_name)
        if case is None:
            raise SystemExit(f"{area.name}: missing shared corpus case {case_name}")
        steps = case.get("steps") or []
        if not steps:
            raise SystemExit(f"{area.name}: corpus case {case_name} has no steps")
        for step in steps:
            command = step.get("command", {})
            if command.get("kind") != "existing_test":
                raise SystemExit(f"{area.name}: {case_name}/{step.get('name')} is not existing_test")
            if command.get("suite") not in PARITY_SUITES:
                raise SystemExit(
                    f"{area.name}: {case_name}/{step.get('name')} suite {command.get('suite')!r} "
                    "is not a storage/Raft parity suite"
                )
            paths = command.get("required_paths") or []
            if not paths:
                raise SystemExit(f"{area.name}: {case_name}/{step.get('name')} has no required_paths")
            existing_paths.update(paths)
    return existing_paths


def validate_rust_evidence(area: ParityArea) -> int:
    snippet_count = 0
    for evidence in area.rust_evidence:
        path = ROOT / evidence.path
        if not path.exists():
            raise SystemExit(f"{area.name}: missing Rust evidence file {evidence.path}")
        text = path.read_text(encoding="utf-8", errors="ignore")
        for snippet in evidence.snippets:
            if snippet not in text:
                raise SystemExit(
                    f"{area.name}: Rust evidence file {evidence.path} missing snippet {snippet!r}"
                )
            snippet_count += 1
    return snippet_count


def validate_cpp_paths(area: ParityArea, cases: dict[str, dict], cpp_repo: Path) -> set[str]:
    checked: set[str] = set()
    for case_name in area.corpus_cases:
        for step in cases[case_name].get("steps") or []:
            for required_path in step["command"].get("required_paths") or []:
                if not (cpp_repo / required_path).exists():
                    raise SystemExit(f"{area.name}: C++ required path missing: {required_path}")
                checked.add(required_path)
    return checked


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cpp-repo", type=Path, help="optional C++ checkout for required path checks")
    args = parser.parse_args()

    corpus = load_corpus()
    cases = case_map(corpus)
    total_cpp_paths: set[str] = set()
    total_rust_snippets = 0
    checked_cpp_paths: set[str] = set()
    for area in AREAS:
        total_cpp_paths.update(validate_corpus_area(area, cases))
        total_rust_snippets += validate_rust_evidence(area)
        if args.cpp_repo is not None:
            checked_cpp_paths.update(validate_cpp_paths(area, cases, args.cpp_repo))
        print(f"validated raft/storage parity area: {area.name}")

    print(f"raft_storage_parity_areas={len(AREAS)}")
    print(f"corpus_required_cpp_paths={len(total_cpp_paths)}")
    print(f"rust_evidence_snippets={total_rust_snippets}")
    if args.cpp_repo is not None:
        print(f"checked_cpp_required_paths={len(checked_cpp_paths)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
