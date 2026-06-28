#!/usr/bin/env python3
"""Validate storage/proxy/client parity checklist coverage."""

from __future__ import annotations

import json
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
CORPUS = ROOT / "compat" / "unified_temporalstore_cases.json"
INVENTORY = ROOT / "docs" / "unified_test_case_inventory.md"
ENGINE = ROOT / "crates" / "temporalstore-rust" / "src" / "engine.rs"


REQUIRED_CASES = {
    "storage_slot_first_physical_index": "slot/object/page-first authority",
    "storage_object_manager_slotstore_runtime_authority": "ObjectManager and SlotStore runtime modules",
    "storage_slot_layout_transitions": "native SlotStore layout transitions",
    "storage_model_layout_compaction_policies": "model-layout-aware compaction",
    "storage_merged_dump_load_lifecycle": "merged dump/load lifecycle",
    "storage_object_manager_cold_hot_reload": "ObjectManager cold/hot reload",
    "storage_page_address_disk_cache_shared_store_fallback": "PageAddress disk/cache fallback",
    "storage_tombstone_compaction": "tombstone compaction",
    "storage_stale_page_density_compaction": "stale page density compaction",
    "storage_merged_dump_load_restart_interruption": "merged dump/load interruption",
    "storage_gc_eviction_cold_reads": "GC plus eviction under cold reads",
    "storage_manager_continuous_background_runtime": "continuous StorageManager background runtime",
    "storage_manager_real_pressure_signals": "real StorageManager pressure signals",
    "storage_manager_wal_reclaim_slot_generation_retention": "slot-generation WAL reclaim retention",
    "storage_manager_expire_cursor_scan_limits": "expire hot/cold cursor scan limits",
    "storage_risk_context_page_backed_parity": "Risk/Context page-backed parity",
    "control_multi_proxy_topology_churn_scale": "multi-proxy convergence scale",
    "control_client_cpp_partition_set_route_cache": "direct SDK partition-set route cache",
    "control_client_pipeline_batch_partial_timeout_contract": "direct SDK pipeline parity",
    "control_client_deployment_placement_routing_hooks": "deployment placement routing hooks",
}

REQUIRED_DOC_PHRASES = [
    "ObjectManager/SlotStore runtime authority modules",
    "storage_object_manager_slotstore_runtime_authority",
    "storage_manager_continuous_background_runtime",
    "storage_manager_real_pressure_signals",
    "storage_manager_wal_reclaim_slot_generation_retention",
    "storage_manager_expire_cursor_scan_limits",
    "control_multi_proxy_topology_churn_scale",
    "control_client_cpp_partition_set_route_cache",
    "control_client_pipeline_batch_partial_timeout_contract",
    "control_client_deployment_placement_routing_hooks",
]


def fail(message: str) -> None:
    raise SystemExit(message)


def main() -> None:
    corpus = json.loads(CORPUS.read_text())
    case_names = {case["name"] for case in corpus["cases"]}
    missing_cases = [
        f"{case_name} ({reason})"
        for case_name, reason in REQUIRED_CASES.items()
        if case_name not in case_names
    ]
    if missing_cases:
        fail("missing shared parity cases:\n  - " + "\n  - ".join(missing_cases))

    rust_sources = "\n".join(
        path.read_text(errors="ignore")
        for path in (ROOT / "crates" / "temporalstore-rust" / "src").rglob("*.rs")
    )
    marker_lines = [
        line for line in rust_sources.splitlines() if "shared-corpus:" in line
    ]
    missing_markers = sorted(
        case_name
        for case_name in REQUIRED_CASES
        if not any(case_name in line for line in marker_lines)
    )
    if missing_markers:
        fail("missing Rust shared-corpus test markers:\n  - " + "\n  - ".join(missing_markers))

    if "mod object_manager;" not in ENGINE.read_text():
        fail("engine.rs must declare object_manager runtime module")
    if "mod slot_store;" not in ENGINE.read_text():
        fail("engine.rs must declare slot_store runtime module")

    inventory = INVENTORY.read_text()
    missing_docs = [phrase for phrase in REQUIRED_DOC_PHRASES if phrase not in inventory]
    if missing_docs:
        fail("missing inventory phrases:\n  - " + "\n  - ".join(missing_docs))

    print(
        "storage/proxy/client parity coverage passed "
        f"cases={len(REQUIRED_CASES)} markers={len(REQUIRED_CASES)}"
    )


if __name__ == "__main__":
    try:
        main()
    except BrokenPipeError:
        sys.exit(1)
