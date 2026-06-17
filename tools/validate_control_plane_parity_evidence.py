#!/usr/bin/env python3
"""Validate Rust evidence for shared C++ control-plane parity gates."""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
CORPUS = ROOT / "compat" / "unified_temporalstore_cases.json"
PARITY_SUITES = {
    "cpp_client_control_plane_parity",
    "cpp_proxy_control_plane_parity",
    "cpp_metaserver_control_plane_parity",
    "cpp_data_node_lifecycle_parity",
}


@dataclass(frozen=True)
class RustEvidence:
    path: str
    snippets: tuple[str, ...]


@dataclass(frozen=True)
class ParityArea:
    name: str
    corpus_case: str
    suite: str
    rust_evidence: tuple[RustEvidence, ...]


AREAS: tuple[ParityArea, ...] = (
    ParityArea(
        name="client_meta_sync_route_retry",
        corpus_case="cpp_client_meta_sync_route_parity_surfaces",
        suite="cpp_client_control_plane_parity",
        rust_evidence=(
            RustEvidence(
                "crates/temporalstore-rust/src/client.rs",
                (
                    "sync_table_topology",
                    "refresh_stale_routes_from_meta",
                    "invalidate_routes_from_meta_topology",
                    "meta_sync_tables",
                    "safe_budget_free_write_retry",
                ),
            ),
            RustEvidence(
                "crates/temporalstore-rust/tests/temporalstore_compat.rs",
                ("production_readiness_service_summary_is_public_api",),
            ),
        ),
    ),
    ParityArea(
        name="proxy_serving_admission_topology",
        corpus_case="cpp_proxy_serving_admission_parity_surfaces",
        suite="cpp_proxy_control_plane_parity",
        rust_evidence=(
            RustEvidence(
                "crates/temporalstore-rust/src/proxy.rs",
                (
                    "heartbeat_to_meta",
                    "apply_heartbeat_config",
                    "refresh_topology_from_meta",
                    "check_admission_for_commands",
                    "drop_percent",
                    "topology_cache_stale",
                ),
            ),
            RustEvidence(
                "crates/temporalstore-rust/src/proxy.rs",
                ("proxy_exposes_cpp_parity_readiness_report",),
            ),
        ),
    ),
    ParityArea(
        name="metaserver_scheduler_repair_snapshot",
        corpus_case="cpp_metaserver_scheduler_repair_parity_surfaces",
        suite="cpp_metaserver_control_plane_parity",
        rust_evidence=(
            RustEvidence(
                "crates/temporalstore-rust/src/rebalance.rs",
                (
                    "DeterministicTaskScheduler",
                    "TaskSchedulerSnapshot",
                    "repair_broken_membership_tasks",
                    "SchedulerLifecycleToken",
                    "task_scheduler_retries_with_capped_backoff_and_aborts",
                ),
            ),
            RustEvidence(
                "crates/temporalstore-rust/src/bin/metaserver.rs",
                (
                    "MetaTaskScheduler",
                    "save_scheduler_snapshot",
                    "metaserver_scheduler_persists_snapshot_file_after_mutations",
                    "metaserver_scheduler_reload_survives_nodeserver_lifecycle_snapshot_restart",
                ),
            ),
        ),
    ),
    ParityArea(
        name="data_node_lifecycle_server_surface",
        corpus_case="cpp_data_node_lifecycle_server_parity_surfaces",
        suite="cpp_data_node_lifecycle_parity",
        rust_evidence=(
            RustEvidence(
                "crates/temporalstore-rust/src/data_node.rs",
                (
                    "load_shard_with",
                    "reload_shard_with",
                    "unload_shard_with",
                    "lifecycle_snapshot",
                    "lifecycle_write_blocked",
                    "lifecycle_snapshot_persist_failed",
                ),
            ),
            RustEvidence(
                "crates/temporalstore-rust/src/bin/server.rs",
                (
                    "/ServerService/GetLifecycle",
                    "/ServerService/RequireLifecycleToken",
                    "cpp_server_service_lifecycle_snapshot_routes_restore_scheduler_state",
                    "cpp_server_service_lifecycle_snapshot_survives_http_restart_boundary",
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


def validate_corpus_area(area: ParityArea, cases: dict[str, dict], required: set[str]) -> set[str]:
    if area.corpus_case not in required:
        raise SystemExit(f"{area.name}: {area.corpus_case} missing from coverage.required_case_names")
    case = cases.get(area.corpus_case)
    if case is None:
        raise SystemExit(f"{area.name}: missing shared corpus case {area.corpus_case}")
    steps = case.get("steps") or []
    if not steps:
        raise SystemExit(f"{area.name}: corpus case {area.corpus_case} has no steps")

    paths: set[str] = set()
    for step in steps:
        command = step.get("command", {})
        if command.get("kind") != "existing_test":
            raise SystemExit(f"{area.name}: {area.corpus_case}/{step.get('name')} is not existing_test")
        if command.get("suite") != area.suite:
            raise SystemExit(
                f"{area.name}: {area.corpus_case}/{step.get('name')} suite "
                f"{command.get('suite')!r} != {area.suite!r}"
            )
        required_paths = command.get("required_paths") or []
        if not required_paths:
            raise SystemExit(f"{area.name}: {area.corpus_case}/{step.get('name')} has no required_paths")
        paths.update(required_paths)
    return paths


def validate_rust_evidence(area: ParityArea) -> int:
    count = 0
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
            count += 1
    return count


def validate_cpp_paths(area: ParityArea, paths: set[str], cpp_repo: Path) -> set[str]:
    checked: set[str] = set()
    for required_path in paths:
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
    required = set(corpus.get("coverage", {}).get("required_case_names", []))
    total_cpp_paths: set[str] = set()
    total_rust_snippets = 0
    checked_cpp_paths: set[str] = set()

    for area in AREAS:
        paths = validate_corpus_area(area, cases, required)
        total_cpp_paths.update(paths)
        total_rust_snippets += validate_rust_evidence(area)
        if args.cpp_repo is not None:
            checked_cpp_paths.update(validate_cpp_paths(area, paths, args.cpp_repo))
        print(f"validated control-plane parity area: {area.name}")

    print(f"control_plane_parity_areas={len(AREAS)}")
    print(f"corpus_required_cpp_paths={len(total_cpp_paths)}")
    print(f"rust_evidence_snippets={total_rust_snippets}")
    if args.cpp_repo is not None:
        print(f"checked_cpp_required_paths={len(checked_cpp_paths)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
