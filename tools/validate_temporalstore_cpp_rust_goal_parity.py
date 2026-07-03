#!/usr/bin/env python3
"""Validate the goal-level Rust-vs-C++ TemporalStore parity status.

This gate is intentionally stricter than a prose report and intentionally more
honest than a green unit test. It verifies that all user-facing parity areas are
tracked with machine-readable status, evidence, and blockers, and that the repo
does not claim full production parity while required live performance evidence
is still open.
"""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
STATUS = ROOT / "compat" / "temporalstore_cpp_rust_goal_parity_status.json"
PERFORMANCE_VALIDATOR = ROOT / "tools" / "validate_temporalstore_cpp_rust_performance_parity.py"


REQUIRED_AREAS = [
    "feature_parity",
    "performance_parity",
    "storage_lifecycle_parity",
    "storage_manager_parity",
    "store_manager_parity",
    "gc_eviction_reclaim_parity",
    "zone_stream_segment_slot_parity",
    "page_block_page_address_index_parity",
    "multi_layer_cache_parity",
]

REQUIRED_STATUS_LABELS = [
    "feature_correct",
    "performance_candidate",
    "production_performance_parity",
    "active_gap",
]

REQUIRED_SCALE_WORKLOADS = [
    "1K_event_ingestion",
    "10K_event_ingestion",
    "100K_event_ingestion",
    "retrieve_workers_4",
    "retrieve_workers_8",
    "retrieve_workers_16",
    "retrieve_workers_32",
]

REQUIRED_SCALE_METRICS = [
    "message_qps",
    "retrieve_qps",
    "p50_ms",
    "p95_ms",
    "p99_ms",
    "timeout_count",
    "error_count",
    "fallback_flags",
    "selected_ref_parity",
]

REQUIRED_GC_RECLAIM_EVIDENCE = [
    "cache_evictions",
    "tombstone_records",
    "stale_page_tombstones",
    "stale_block_tombstones",
    "stale_pages_rewritten",
    "stale_pages_skipped",
    "stale_blocks_rewritten",
    "stale_blocks_skipped",
    "compaction_reclaimed_bytes",
    "physical_reclaimed_bytes",
    "physical_reclaim_errors",
]

REQUIRED_CACHE_EVIDENCE = [
    "memory_object_cache",
    "page_index_cache",
    "block_index_cache",
    "disk_block_cache",
    "shared_store_read_through",
    "cache_writeback_rejections",
    "hot_cache_promotions",
]

REQUIRED_INDEX_EVIDENCE = [
    "PageAddress",
    "BlockAddress",
    "PageIndexEntry",
    "BlockIndexEntry",
    "ObjectIndexEntry",
]


def _as_strings(value: Any) -> list[str]:
    if isinstance(value, list):
        return [str(item) for item in value]
    return []


def _require_contains(haystack: list[str], needles: list[str], label: str, failures: list[str]) -> None:
    missing = [needle for needle in needles if needle not in haystack]
    if missing:
        failures.append(f"{label} missing: {', '.join(missing)}")


def main() -> int:
    subprocess.run([sys.executable, str(PERFORMANCE_VALIDATOR)], cwd=ROOT, check=True)
    data = json.loads(STATUS.read_text(encoding="utf-8"))
    failures: list[str] = []

    if data.get("schema") != "temporalstore_cpp_rust_goal_parity_status_v1":
        failures.append("unexpected or missing schema")

    status_labels = data.get("status_labels")
    if not isinstance(status_labels, dict):
        failures.append("status_labels must be an object")
        status_labels = {}
    for label in REQUIRED_STATUS_LABELS:
        if label not in status_labels:
            failures.append(f"status_labels missing `{label}`")

    global_status = data.get("global_status")
    if not isinstance(global_status, dict):
        failures.append("global_status must be an object")
        global_status = {}

    goal_complete = global_status.get("goal_complete")
    production_parity = global_status.get("production_performance_parity")
    global_blockers = _as_strings(global_status.get("open_blockers"))
    if goal_complete is True and global_blockers:
        failures.append("goal_complete cannot be true while global open_blockers remain")
    if goal_complete is True and production_parity is not True:
        failures.append("goal_complete requires production_performance_parity=true")
    if production_parity is True and global_blockers:
        failures.append("production_performance_parity cannot be true while blockers remain")

    scale_matrix = data.get("required_scale_matrix")
    if not isinstance(scale_matrix, dict):
        failures.append("required_scale_matrix must be an object")
        scale_matrix = {}
    _require_contains(
        _as_strings(scale_matrix.get("workloads")),
        REQUIRED_SCALE_WORKLOADS,
        "required_scale_matrix.workloads",
        failures,
    )
    _require_contains(
        _as_strings(scale_matrix.get("required_metrics")),
        REQUIRED_SCALE_METRICS,
        "required_scale_matrix.required_metrics",
        failures,
    )

    areas = data.get("areas")
    if not isinstance(areas, dict):
        failures.append("areas must be an object")
        areas = {}
    for area in REQUIRED_AREAS:
        section = areas.get(area)
        if not isinstance(section, dict):
            failures.append(f"areas missing `{area}`")
            continue
        status = section.get("status")
        if status not in REQUIRED_STATUS_LABELS:
            failures.append(f"{area} has invalid status `{status}`")
        evidence = _as_strings(section.get("evidence"))
        if not evidence:
            failures.append(f"{area} must include evidence")
        blockers = _as_strings(section.get("open_blockers"))
        if status in {"feature_correct", "performance_candidate", "production_performance_parity"} and blockers:
            failures.append(f"{area} cannot be `{status}` while open_blockers remain")
        if status == "production_performance_parity" and data["global_status"].get("production_performance_parity") is not True:
            failures.append(f"{area} cannot claim production performance parity before global parity is true")

    if isinstance(areas.get("gc_eviction_reclaim_parity"), dict):
        _require_contains(
            _as_strings(areas["gc_eviction_reclaim_parity"].get("evidence")),
            REQUIRED_GC_RECLAIM_EVIDENCE,
            "gc_eviction_reclaim_parity.evidence",
            failures,
        )
    if isinstance(areas.get("multi_layer_cache_parity"), dict):
        _require_contains(
            _as_strings(areas["multi_layer_cache_parity"].get("evidence")),
            REQUIRED_CACHE_EVIDENCE,
            "multi_layer_cache_parity.evidence",
            failures,
        )
    if isinstance(areas.get("page_block_page_address_index_parity"), dict):
        _require_contains(
            _as_strings(areas["page_block_page_address_index_parity"].get("evidence")),
            REQUIRED_INDEX_EVIDENCE,
            "page_block_page_address_index_parity.evidence",
            failures,
        )

    if failures:
        details = "\n".join(f"- {failure}" for failure in failures)
        raise SystemExit(f"TemporalStore C++/Rust goal parity status failed:\n{details}")

    print("TemporalStore C++/Rust goal parity status is explicit and fail-closed")
    print(f"- goal_complete={global_status.get('goal_complete')}")
    print(f"- production_performance_parity={global_status.get('production_performance_parity')}")
    print(f"- tracked_areas={len(REQUIRED_AREAS)}")
    print(f"- open_blockers={len(global_blockers)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
