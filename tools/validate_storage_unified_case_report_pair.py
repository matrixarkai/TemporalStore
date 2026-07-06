#!/usr/bin/env python3
"""Validate the C++/Rust storage unified case-report fixture pair.

This gate is intentionally small but important: it proves the storage/cache
family has a committed Rust temporalstore_unified_case_report_v1 fixture and a
compiled C++ adapter runner that emits comparable report rows for slot/object/
block indexes, GC/eviction/cold reads, stream/segment/zone evidence, and the
same comparator path that real native C++ and Rust runners must use.
"""

from __future__ import annotations

import json
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
PAIR = ROOT / "compat" / "storage_unified_case_report_pair.json"
COMPARATOR = ROOT / "tools" / "compare_unified_cpp_rust_case_reports.py"
CPP_RUNNER = ROOT / "tools" / "cpp_storage_unified_case_report_runner.cc"
EXPECTED_SCHEMA = "temporalstore_storage_unified_case_report_pair_v1"
EXPECTED_REPORT_SCHEMA = "temporalstore_unified_case_report_v1"
REQUIRED_CASES = {
    "storage_block_address_fallback_shared",
    "storage_cache_replacement_soak_shared",
    "storage_config_cpp_like_public_knobs",
    "storage_data_structure_api_parity",
    "storage_slot_object_block_index_authority_shared",
    "storage_slot_first_physical_index",
    "storage_slot_layout_transitions_shared",
    "storage_gc_eviction_cold_reads_shared",
    "storage_merged_dump_load_lifecycle",
    "storage_model_aware_block_compaction_shared",
    "storage_object_manager_cold_hot_reload",
    "storage_object_manager_slotstore_runtime_authority",
    "storage_page_address_disk_cache_shared_store_fallback",
    "storage_stale_page_density_compaction",
    "storage_manager_active_eviction_runtime",
    "storage_manager_expire_cursor_scan_limits",
    "storage_manager_index_gc_thresholds_recovery",
    "storage_manager_page_gc_dependency_refusal",
    "storage_manager_real_pressure_signals",
    "storage_manager_wal_reclaim_slot_generation_retention",
    "storage_stream_segment_manifest_rebuild_shared",
    "storage_wal_index_gc_reclaim_shared",
}
REQUIRED_OUTPUT_FIELDS = {
    "storage_block_address_fallback_shared": {
        "block_index_cache_hits",
        "block_index_cache_misses",
        "disk_cache_hits",
        "page_address_fallbacks",
        "shared_store_read_throughs",
    },
    "storage_cache_replacement_soak_shared": {
        "cache_admissions",
        "cache_evictions",
        "cache_refills",
        "cache_writeback_queue_depth",
        "cache_writeback_rejections",
        "memory_cache_hits",
        "memory_cache_misses",
    },
    "storage_config_cpp_like_public_knobs": {
        "TS_BLOCK_INDEX_CACHE_BYTES",
        "TS_BLOCK_SEGMENT_TARGET_BYTES",
        "TS_COLD_SCAN_NO_CACHE_FILL",
        "TS_COMPACTION_WATERMARK_BYTES",
        "TS_CONTEXT_PAGE_TARGET_BYTES",
        "TS_PAGE_INDEX_CACHE_BYTES",
        "TS_STORAGE_ZONE_SIZE",
        "TS_STREAM_MAX_BLOB_SIZE",
    },
    "storage_data_structure_api_parity": {
        "block_address_metadata",
        "legacy_zone_aliases",
        "object_manager_runtime",
        "segment_block_index",
        "slot_layout_states",
        "slot_object_page_authority",
        "storage_manager_phase_order",
        "stream_backed_extent_lifecycle",
    },
    "storage_slot_object_block_index_authority_shared": {
        "append_watermark",
        "block_index_entry_count",
        "object_index_entry_count",
        "page_index_entry_count",
        "page_reads",
        "page_writes",
        "restart_rebuild_verified",
        "slot_owner_mismatch_count",
        "slot_page_ref_count",
        "slot_stale_ref_count",
    },
    "storage_slot_first_physical_index": {
        "object_index_entry_count",
        "page_index_entry_count",
        "slot_dirty_generation_count",
        "slot_owner_mismatch_count",
        "slot_page_ref_count",
        "slot_stale_ref_count",
    },
    "storage_slot_layout_transitions_shared": {
        "slot_compacted_generation",
        "slot_deleted_refs",
        "slot_dirty_generation_count",
        "slot_growth_events",
        "slot_tombstone_count",
    },
    "storage_gc_eviction_cold_reads_shared": {
        "cache_evictions",
        "cold_scan_no_cache_reads",
        "compaction_reclaimed_bytes",
        "hot_cache_promotions",
        "physical_reclaim_errors",
        "physical_reclaimed_bytes",
        "stale_block_tombstones",
        "stale_page_tombstones",
        "tombstone_records",
    },
    "storage_merged_dump_load_lifecycle": {
        "append_log_replay_records",
        "dump_load_generation",
        "object_index_entry_count",
        "page_index_rebuild_count",
        "restart_rebuild_verified",
    },
    "storage_model_aware_block_compaction_shared": {
        "block_index_entry_count",
        "compaction_reclaimed_bytes",
        "model_layout_rewrite_count",
        "object_index_entry_count",
        "stale_blocks_rewritten",
        "stale_blocks_skipped",
    },
    "storage_object_manager_cold_hot_reload": {
        "cache_evictions",
        "cache_refills",
        "cache_rehydrates",
        "memory_cache_misses",
        "page_reads",
    },
    "storage_object_manager_slotstore_runtime_authority": {
        "object_index_entry_count",
        "object_manager_authority",
        "restart_rebuild_verified",
        "slot_dirty_generation_count",
        "slot_page_ref_count",
    },
    "storage_page_address_disk_cache_shared_store_fallback": {
        "block_index_cache_hits",
        "block_index_cache_misses",
        "disk_cache_hits",
        "page_address_fallbacks",
        "shared_store_read_throughs",
    },
    "storage_stale_page_density_compaction": {
        "compaction_reclaimed_bytes",
        "physical_reclaimed_bytes",
        "stale_page_density_percent",
        "stale_page_tombstones",
        "stale_pages_rewritten",
        "stale_pages_skipped",
    },
    "storage_manager_active_eviction_runtime": {
        "cache_admissions",
        "cache_evictions",
        "cache_refills",
        "cache_writeback_queue_depth",
        "cache_writeback_rejections",
        "memory_cache_hits",
        "memory_cache_misses",
    },
    "storage_manager_expire_cursor_scan_limits": {
        "cold_scan_no_cache_reads",
        "expire_cursor_limit",
        "hot_cache_promotions",
        "storage_manager_expire_count",
        "tombstone_records",
    },
    "storage_manager_index_gc_thresholds_recovery": {
        "block_index_rebuild_count",
        "index_gc_generation",
        "object_index_rebuild_count",
        "page_index_rebuild_count",
        "storage_manager_index_gc_count",
    },
    "storage_manager_page_gc_dependency_refusal": {
        "follower_cursor_retention_floor",
        "physical_reclaim_errors",
        "reclaim_refused_by_dependency",
        "storage_manager_follower_cursor_safety_count",
        "storage_manager_page_gc_count",
    },
    "storage_manager_real_pressure_signals": {
        "cache_writeback_queue_depth",
        "storage_manager_compaction_count",
        "storage_manager_evict_count",
        "storage_manager_loop_ms",
        "storage_manager_reclaim_count",
        "storage_manager_watermark_progress_count",
    },
    "storage_manager_wal_reclaim_slot_generation_retention": {
        "append_log_reclaimed_records",
        "append_log_replay_records",
        "compaction_watermark",
        "follower_cursor_retention_floor",
        "index_gc_generation",
        "reclaimable_bytes",
    },
    "storage_stream_segment_manifest_rebuild_shared": {
        "delayed_destroy_backlog",
        "segment_open_count",
        "segment_sealed_count",
        "stream_rollover_count",
        "storage_zone_stale_bytes",
        "storage_zone_total_bytes",
        "storage_zone_used_bytes",
    },
    "storage_wal_index_gc_reclaim_shared": {
        "append_log_reclaimed_records",
        "append_log_replay_records",
        "compaction_watermark",
        "follower_cursor_retention_floor",
        "index_gc_generation",
        "reclaimable_bytes",
    },
}


def _load() -> dict[str, Any]:
    try:
        return json.loads(PAIR.read_text(encoding="utf-8"))
    except OSError as exc:
        raise SystemExit(f"cannot read {PAIR}: {exc}") from exc
    except json.JSONDecodeError as exc:
        raise SystemExit(f"{PAIR}: invalid JSON: {exc}") from exc


def _case_map(report: dict[str, Any], label: str) -> dict[str, dict[str, Any]]:
    if report.get("schema") != EXPECTED_REPORT_SCHEMA:
        raise SystemExit(f"{label}.schema must be {EXPECTED_REPORT_SCHEMA}")
    cases = report.get("cases")
    if not isinstance(cases, list):
        raise SystemExit(f"{label}.cases must be a list")
    mapped: dict[str, dict[str, Any]] = {}
    for case in cases:
        if not isinstance(case, dict):
            raise SystemExit(f"{label}.cases entries must be objects")
        name = str(case.get("name") or "")
        if not name:
            raise SystemExit(f"{label}.cases entry missing name")
        if name in mapped:
            raise SystemExit(f"{label}.cases contains duplicate case {name}")
        mapped[name] = case
    missing = sorted(REQUIRED_CASES - set(mapped))
    if missing:
        raise SystemExit(f"{label}.cases missing required storage cases: {', '.join(missing)}")
    return mapped


def _validate_outputs(cases: dict[str, dict[str, Any]], label: str) -> None:
    for case_name, required_fields in REQUIRED_OUTPUT_FIELDS.items():
        case = cases[case_name]
        if str(case.get("status") or "") != "passed":
            raise SystemExit(f"{label}.{case_name} must be passed")
        steps = case.get("steps")
        if not isinstance(steps, list) or not steps:
            raise SystemExit(f"{label}.{case_name}.steps must be non-empty")
        output = steps[0].get("output") if isinstance(steps[0], dict) else None
        if not isinstance(output, dict):
            raise SystemExit(f"{label}.{case_name}.steps[0].output must be an object")
        missing = sorted(required_fields - set(output))
        if missing:
            raise SystemExit(
                f"{label}.{case_name}.steps[0].output missing: {', '.join(missing)}"
            )
        if steps[0].get("latency_ms") is None:
            raise SystemExit(f"{label}.{case_name}.steps[0] missing latency_ms")


def _run_comparator(rust_report: dict[str, Any], cpp_report: dict[str, Any]) -> dict[str, Any]:
    with tempfile.TemporaryDirectory(prefix="storage-unified-case-report-") as tmpdir:
        root = Path(tmpdir)
        rust_path = root / "rust.json"
        cpp_path = root / "cpp.json"
        out_path = root / "comparison.json"
        rust_path.write_text(json.dumps(rust_report), encoding="utf-8")
        cpp_path.write_text(json.dumps(cpp_report), encoding="utf-8")
        completed = subprocess.run(
            [
                sys.executable,
                str(COMPARATOR),
                "--rust-report",
                str(rust_path),
                "--cpp-report",
                str(cpp_path),
                "--output",
                str(out_path),
                "--require-schema",
                EXPECTED_REPORT_SCHEMA,
                "--require-field",
                "cases",
                "--require-field",
                "producer",
                "--require-field",
                "generated_at_ms",
            ],
            cwd=ROOT,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            check=False,
        )
        if completed.returncode != 0:
            raise SystemExit(completed.stdout.rstrip())
        return json.loads(out_path.read_text(encoding="utf-8"))


def _run_cpp_adapter() -> dict[str, Any]:
    with tempfile.TemporaryDirectory(prefix="cpp-storage-unified-runner-") as tmpdir:
        root = Path(tmpdir)
        binary_path = root / "cpp_storage_unified_case_report_runner"
        cpp_report_path = root / "cpp-report.json"
        compile_result = subprocess.run(
            [
                "g++",
                "-std=c++17",
                "-O2",
                "-I",
                str(ROOT),
                str(CPP_RUNNER),
                "-o",
                str(binary_path),
            ],
            cwd=ROOT,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            check=False,
        )
        if compile_result.returncode != 0:
            raise SystemExit(compile_result.stdout.rstrip())
        run_result = subprocess.run(
            [str(binary_path), "--output", str(cpp_report_path)],
            cwd=ROOT,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            check=False,
        )
        if run_result.returncode != 0:
            raise SystemExit(run_result.stdout.rstrip())
        return json.loads(cpp_report_path.read_text(encoding="utf-8"))


def main() -> int:
    pair = _load()
    if pair.get("schema") != EXPECTED_SCHEMA:
        raise SystemExit(f"{PAIR}: schema must be {EXPECTED_SCHEMA}")
    rust_report = pair.get("rust_report")
    cpp_report = pair.get("cpp_report")
    if not isinstance(rust_report, dict) or not isinstance(cpp_report, dict):
        raise SystemExit(f"{PAIR}: rust_report and cpp_report must be objects")
    _validate_outputs(_case_map(rust_report, "rust_report"), "rust_report")
    _validate_outputs(_case_map(cpp_report, "cpp_report"), "cpp_report")
    cpp_runtime_report = _run_cpp_adapter()
    _validate_outputs(_case_map(cpp_runtime_report, "cpp_runtime_report"), "cpp_runtime_report")
    comparison = _run_comparator(rust_report, cpp_runtime_report)
    if comparison.get("ready") is not True:
        raise SystemExit(f"storage unified case report comparison is not ready: {comparison}")
    print(
        "storage unified case report pair passed: "
        f"cases={len(REQUIRED_CASES)} rows={comparison.get('row_count')} "
        "cpp_adapter=compiled"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
