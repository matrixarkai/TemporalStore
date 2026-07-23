#!/usr/bin/env python3
"""Run or validate per-family Rust/C++ shared-test migration checks."""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
CORPUS = ROOT / "compat" / "unified_temporalstore_cases.json"


@dataclass(frozen=True)
class FamilyConfig:
    name: str
    case_ids: tuple[str, ...]
    rust_tests: tuple[str, ...]
    rust_command: str
    cpp_suites: tuple[str, ...]


FAMILIES: dict[str, FamilyConfig] = {
    "storage/cache": FamilyConfig(
        "storage/cache",
        (
            "storage_dump_load_recovery",
            "storage_fault_matrix",
            "storage_follower_safe_gc",
            "storage_shared_store_oplog_cursor_retention",
            "storage_shared_store_checkpoint_cursor_retention",
            "storage_cache_refill",
            "storage_shared_store_sync_replay",
            "storage_shared_store_async_replay",
            "storage_slot_first_physical_index",
            "storage_object_manager_slotstore_runtime_authority",
            "storage_slot_layout_transitions",
            "storage_model_layout_compaction_policies",
            "storage_merged_dump_load_lifecycle",
            "storage_object_manager_cold_hot_reload",
            "storage_page_address_disk_cache_shared_store_fallback",
            "storage_tombstone_compaction",
            "storage_stale_page_density_compaction",
            "storage_merged_dump_load_restart_interruption",
            "storage_gc_eviction_cold_reads",
            "storage_risk_context_page_backed_parity",
            "storage_manager_continuous_background_runtime",
            "storage_manager_real_pressure_signals",
            "storage_manager_wal_reclaim_slot_generation_retention",
            "storage_manager_expire_cursor_scan_limits",
            "storage_manager_active_eviction_runtime",
            "storage_manager_page_gc_dependency_refusal",
            "storage_manager_index_gc_thresholds_recovery",
            "storage_manager_metrics_admin_phase_reports",
        ),
        (
            "crates/temporalstore-rust/src/shared_store.rs::shared_store_replays_chunked_timestamped_kv_pages_in_sync_and_async_modes",
            "crates/temporalstore-rust/src/shared_store.rs::shared_store_gc_refuses_oplog_needed_by_replay_cursor",
            "crates/temporalstore-rust/src/shared_store.rs::shared_store_checkpoint_gc_retains_cursor_anchor_checkpoint",
            "crates/temporalstore-rust/src/engine.rs::storage_physical_index_report_is_slot_first_and_page_index_complete",
            "crates/temporalstore-rust/src/engine.rs::storage_object_manager_and_slotstore_runtime_modules_are_authoritative",
            "crates/temporalstore-rust/src/engine.rs::storage_slot_layout_transitions_cover_growth_compaction_delete_dump_load_restart",
            "crates/temporalstore-rust/src/engine.rs::storage_compaction_reports_model_layout_policies_and_density",
            "crates/temporalstore-rust/src/engine.rs::storage_merged_dump_load_tracks_rollback_handoff_and_conflicts",
            "crates/temporalstore-rust/src/engine.rs::storage_object_manager_cold_hot_reload_and_page_address_fallback",
            "crates/temporalstore-rust/src/engine.rs::storage_compaction_reports_tombstones_and_stale_density",
            "crates/temporalstore-rust/src/engine.rs::storage_merged_dump_load_restart_interruption_reports_rollback_marker",
            "crates/temporalstore-rust/src/engine.rs::storage_risk_and_context_page_backed_restart_parity",
            "crates/temporalstore-rust/src/engine.rs::storage_manager_cycle_applies_dump_expire_evict_reclaim_index_gc_and_compact",
            "crates/temporalstore-rust/src/data_node/tests.rs::storage_manager_runtime_supports_stop_pause_resume_jitter_backoff_and_phase_flags",
            "crates/temporalstore-rust/src/engine.rs::storage_manager_wal_reclaim_requires_durable_slot_generation_frontier",
            "crates/temporalstore-rust/src/engine.rs::storage_manager_wal_reclaim_honors_follower_cursor_frontier",
            "crates/temporalstore-rust/src/engine.rs::expiry_sweep_uses_hot_cursors_limits_and_cold_load_policy",
            "crates/temporalstore-rust/src/engine.rs::storage_manager_active_eviction_supports_weighted_dump_drop_batch_and_cooldown",
            "crates/temporalstore-rust/src/engine.rs::storage_manager_page_gc_refuses_reclaim_with_retained_dependencies",
            "crates/temporalstore-rust/src/engine.rs::storage_manager_index_gc_thresholds_budget_dirty_commit_and_restart_recovery",
            "crates/temporalstore-rust/src/engine.rs::storage_manager_cycle_reports_cpp_order_without_mutating_on_dry_run",
        ),
        "cargo test -p temporalstore-rust shared_store_ -- --test-threads=1",
        ("cpp_storage_parity",),
    ),
    "Raft": FamilyConfig(
        "Raft",
        (
            "raft_rustraft_read_safety_policy",
            "raft_rustraft_metrics_admin_pipeline_status",
            "raft_rustraft_snapshot_lifecycle_depth",
            "raft_rustraft_replication_backpressure",
            "raft_rustraft_election_controls",
            "raft_rustraft_shared_fault_gate",
            "storage_data_raft_replication_gtest",
        ),
        (
            "crates/temporalstore-rust/src/e2e.rs::e2e_uses_raft_replication_by_default",
        ),
        "cargo test -p temporalstore-rust e2e_uses_raft_replication_by_default -- --test-threads=1",
        ("cpp_data_raft_parity",),
    ),
    "Redis/admin": FamilyConfig(
        "Redis/admin",
        ("cpp_redis_live_storage_smoke_parity_surfaces", "redis_compatible_set_core"),
        ("crates/temporalstore-rust/src/bin/server.rs::server_ping_routes_match_cpp_ping_rpc",),
        "cargo test -p temporalstore-rust server_ping_routes_match_cpp_ping_rpc -- --test-threads=1",
        ("cpp_storage_parity",),
    ),
    "Feature": FamilyConfig(
        "Feature",
        (
            "feature_packed_timestamped_pages",
            "feature_policy_filter_aggregate_lifecycle",
            "feature_nested_proto_aggregate_semantics",
        ),
        ("crates/temporalstore-rust/src/engine.rs::feature_query_filtered_matches_cpp_protobuf_feature_point",),
        "cargo test -p temporalstore-rust feature_query_filtered_matches_cpp_protobuf_feature_point -- --test-threads=1",
        (),
    ),
    "Sequence": FamilyConfig(
        "Sequence",
        ("sequence_cpp_feature_rows", "sequence_batch_filter_groups"),
        ("crates/temporalstore-rust/src/engine.rs::sequence_query_filters_typed_rows",),
        "cargo test -p temporalstore-rust sequence_query_filters_typed_rows -- --test-threads=1",
        (),
    ),
    "IPS": FamilyConfig(
        "IPS",
        ("ips_options_range", "ips_snapshot_stat_filter_batch"),
        ("crates/temporalstore-rust/src/engine.rs::ips_range_and_batch_queries_match_cpp_style_read_shapes",),
        "cargo test -p temporalstore-rust ips_range_and_batch_queries_match_cpp_style_read_shapes -- --test-threads=1",
        (),
    ),
    "Risk": FamilyConfig(
        "Risk",
        ("risk_counter_window", "risk_family_query_and_delete", "risk_manager_debug_fol"),
        ("crates/temporalstore-rust/src/engine.rs::risk_query_supports_sum_min_max_and_event_count",),
        "cargo test -p temporalstore-rust risk_query_supports_sum_min_max_and_event_count -- --test-threads=1",
        (),
    ),
    "context": FamilyConfig(
        "context",
        (
            "context_events_segments_entities_child_refs",
            "context_cpp_wire_model_descriptor_roundtrip",
            "context_embeddings_summaries_l0_l1_pipeline",
            "context_compression_secondary_index_query_debug_flow",
            "context_event_index_audit_dirty_models",
        ),
        (
            "crates/temporalstore-rust/src/engine.rs::context_models_match_cpp_keys_timeline_pages_and_filters",
            "crates/temporalstore-rust/src/types.rs::context_models_round_trip_cpp_wire_payloads_and_type_alias",
        ),
        "cargo test -p temporalstore-rust context_models_match_cpp_keys_timeline_pages_and_filters -- --test-threads=1",
        ("cpp_context_pipeline_parity",),
    ),
    "control plane": FamilyConfig(
        "control plane",
        (
            "control_metaserver_scheduler_lifecycle_workflow",
            "control_scheduler_token_stale_rejection",
            "control_data_node_load_reload_unload_lifecycle",
            "control_cpp_server_service_alias_surface",
        ),
        (
            "crates/temporalstore-rust/src/bin/metaserver.rs::metaserver_scheduler_execute_next_installs_token_then_loads_node",
            "crates/temporalstore-rust/src/bin/server.rs::cpp_server_service_aliases_cover_partition_manager_surface",
        ),
        "cargo test -p temporalstore-rust metaserver_scheduler_execute_next_installs_token_then_loads_node -- --test-threads=1",
        (
            "cpp_client_control_plane_parity",
            "cpp_proxy_control_plane_parity",
            "cpp_data_node_lifecycle_parity",
            "cpp_metaserver_control_plane_parity",
        ),
    ),
    "ingestion": FamilyConfig(
        "ingestion",
        (
            "ingestion_kafka_offset_ledger",
            "ingestion_flink_checkpoint_lifecycle",
            "ingestion_dead_letter_lag_report_contract",
        ),
        (
            "crates/temporalstore-rust/src/bin/server.rs::server_ingest_batch_routes_execute_api_kafka_and_flink_records",
        ),
        "cargo test -p temporalstore-rust server_ingest_batch_routes_execute_api_kafka_and_flink_records -- --test-threads=1",
        ("cpp_ingestion_parity",),
    ),
    "ops/scale": FamilyConfig(
        "ops/scale",
        ("ops_scale_readiness_slo_gate", "raft_production_gate"),
        ("crates/temporalstore-rust/src/bin/readiness_gate.rs::readiness_gate_can_filter_one_service",),
        "cargo test -p temporalstore-rust readiness_gate_can_filter_one_service -- --test-threads=1",
        ("cpp_ops_scale_parity",),
    ),
}


def load_corpus() -> dict[str, Any]:
    return json.loads(CORPUS.read_text(encoding="utf-8"))


def corpus_case_names(corpus: dict[str, Any]) -> set[str]:
    return {case["name"] for case in corpus.get("cases", []) if isinstance(case, dict)}


def adapter_suites(corpus: dict[str, Any]) -> set[str]:
    suites: set[str] = set()
    for entry in corpus.get("coverage", {}).get("cpp_adapter_coverage", []):
        suites.update(entry.get("suites", []))
    return suites


def cpp_surface_report(corpus: dict[str, Any], config: FamilyConfig, cpp_repo: Path) -> dict[str, Any]:
    required_paths: set[str] = set()
    blockers: list[str] = []
    for entry in corpus.get("coverage", {}).get("cpp_adapter_coverage", []):
        suites = set(entry.get("suites", []))
        if suites & set(config.cpp_suites) and entry.get("blocker"):
            blockers.append(str(entry["blocker"]))
    for case in corpus.get("cases", []):
        for step in case.get("steps", []):
            command = step.get("command", {})
            if command.get("kind") != "existing_test":
                continue
            if command.get("suite") not in config.cpp_suites:
                continue
            required_paths.update(command.get("required_paths", []))
    missing = sorted(path for path in required_paths if not (cpp_repo / path).exists())
    return {
        "family": config.name,
        "cpp_repo": str(cpp_repo),
        "required_path_count": len(required_paths),
        "missing_required_paths": missing,
        "temporary_static_blockers": sorted(set(blockers)),
        "ready": not missing,
    }


def test_marker_present(test_id: str, expected_cases: set[str]) -> bool:
    path_text, fn_name = test_id.split("::", 1)
    path = ROOT / path_text
    if not path.exists():
        raise SystemExit(f"missing Rust test file: {path_text}")
    lines = path.read_text(encoding="utf-8", errors="ignore").splitlines()
    fn_pattern = re.compile(rf"\bfn\s+{re.escape(fn_name)}\s*\(")
    for index, line in enumerate(lines):
        if not fn_pattern.search(line):
            continue
        context = "\n".join(lines[max(0, index - 8) : index + 1])
        markers = re.findall(r"shared-corpus:\s*([A-Za-z0-9_,\-\s]+)", context)
        marked_cases: set[str] = set()
        for marker in markers:
            marked_cases.update(item for item in re.split(r"[,\s]+", marker) if item)
        return bool(marked_cases & expected_cases)
    raise SystemExit(f"missing Rust test function: {test_id}")


def run_shell(command: str) -> None:
    print(f"+ {command}", flush=True)
    subprocess.run(command, cwd=ROOT, shell=True, check=True)


def validate_family(config: FamilyConfig, corpus: dict[str, Any]) -> dict[str, Any]:
    cases = corpus_case_names(corpus)
    suites = adapter_suites(corpus)
    missing_cases = sorted(set(config.case_ids) - cases)
    missing_suites = sorted(set(config.cpp_suites) - suites)
    missing_markers = [
        test_id
        for test_id in config.rust_tests
        if not test_marker_present(test_id, set(config.case_ids))
    ]
    ready = not missing_cases and not missing_suites and not missing_markers
    return {
        "family": config.name,
        "ready": ready,
        "case_ids": list(config.case_ids),
        "rust_tests": list(config.rust_tests),
        "rust_command": config.rust_command,
        "cpp_suites": list(config.cpp_suites),
        "missing_case_ids": missing_cases,
        "missing_cpp_adapter_suites": missing_suites,
        "rust_tests_missing_shared_corpus_marker": missing_markers,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--family", choices=sorted(FAMILIES) + ["all"], default="all")
    parser.add_argument("--run-rust", action="store_true")
    parser.add_argument("--cpp-repo", type=Path)
    parser.add_argument(
        "--run-cpp-runner",
        action="store_true",
        help="run the C++ unified runner instead of only checking static surfaces",
    )
    parser.add_argument("--rust-report", type=Path)
    parser.add_argument("--cpp-report", type=Path)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()

    corpus = load_corpus()
    selected = FAMILIES.values() if args.family == "all" else [FAMILIES[args.family]]
    reports = [validate_family(config, corpus) for config in selected]
    failures = [report for report in reports if not report["ready"]]
    result = {"schema": "temporalstore_per_family_migration_report_v1", "ready": not failures, "families": reports}

    if args.run_rust:
        for config in selected:
            run_shell(config.rust_command)
    if args.cpp_repo and args.run_cpp_runner:
        run_shell(
            "python3 tools/run_temporalstore_unified_tests.py "
            f"--corpus {CORPUS} --cpp --require-cpp --cpp-repo {args.cpp_repo}"
        )
    elif args.cpp_repo:
        cpp_reports = [
            cpp_surface_report(corpus, config, args.cpp_repo)
            for config in selected
            if config.cpp_suites
        ]
        result["cpp_static_surface_reports"] = cpp_reports
        if any(not report["ready"] for report in cpp_reports):
            result["ready"] = False
    if args.rust_report or args.cpp_report:
        if not args.rust_report or not args.cpp_report:
            raise SystemExit("--rust-report and --cpp-report must be passed together")
        run_shell(
            "python3 tools/compare_unified_cpp_rust_case_reports.py "
            f"--rust-report {args.rust_report} --cpp-report {args.cpp_report}"
        )

    text = json.dumps(result, indent=2, sort_keys=True)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(text + "\n", encoding="utf-8")
    print(text)
    return 0 if result["ready"] else 1


if __name__ == "__main__":
    sys.exit(main())
