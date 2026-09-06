#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Validate the conformance storage unified case-report fixture pair.

This gate is intentionally small but important: it proves the storage/cache
family has a committed Rust temporalstore_unified_case_report_v1 fixture and a
compiled adapter runner that emits comparable report rows for slot/object/
block indexes, GC/eviction/cold reads, stream/segment/zone evidence, and the
same comparator path that real native and Rust runners must use.
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
CORPUS = ROOT / "compat" / "unified_temporalstore_cases.json"
COMPARATOR = ROOT / "tools" / "compare_unified_rust_case_reports.py"
NATIVE_RUNNER = ROOT / "tools" / "native_storage_unified_case_report_runner.cc"
EXPECTED_SCHEMA = "temporalstore_storage_unified_case_report_pair_v1"
EXPECTED_REPORT_SCHEMA = "temporalstore_unified_case_report_v1"
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
    "storage_config_like_public_knobs": {
        "TS_BLOCK_INDEX_CACHE_BYTES",
        "TS_BLOCK_SLAB_TARGET_BYTES",
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


def _required_cases() -> set[str]:
    try:
        corpus = json.loads(CORPUS.read_text(encoding="utf-8"))
    except OSError as exc:
        raise SystemExit(f"cannot read {CORPUS}: {exc}") from exc
    coverage = corpus.get("coverage") if isinstance(corpus.get("coverage"), dict) else {}
    rows = coverage.get("native_adapter_coverage")
    if not isinstance(rows, list):
        raise SystemExit(f"{CORPUS}: coverage.native_adapter_coverage must be a list")
    for row in rows:
        if isinstance(row, dict) and row.get("family") == "storage/cache":
            cases = row.get("adapter_contract_case_names")
            if not isinstance(cases, list) or not cases:
                raise SystemExit(
                    f"{CORPUS}: storage/cache adapter_contract_case_names must be non-empty"
                )
            return {str(case) for case in cases}
    raise SystemExit(f"{CORPUS}: missing storage/cache coverage row")


def _case_map(
    report: dict[str, Any],
    label: str,
    required_cases: set[str],
) -> dict[str, dict[str, Any]]:
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
    missing = sorted(required_cases - set(mapped))
    if missing:
        raise SystemExit(f"{label}.cases missing required storage cases: {', '.join(missing)}")
    return mapped


def _validate_outputs(cases: dict[str, dict[str, Any]], label: str) -> None:
    for case_name, case in cases.items():
        if str(case.get("status") or "") != "passed":
            raise SystemExit(f"{label}.{case_name} must be passed")
        steps = case.get("steps")
        if not isinstance(steps, list) or not steps:
            raise SystemExit(f"{label}.{case_name}.steps must be non-empty")
        output = steps[0].get("output") if isinstance(steps[0], dict) else None
        if not isinstance(output, dict) or not output:
            raise SystemExit(f"{label}.{case_name}.steps[0].output must be a non-empty object")
        if steps[0].get("latency_ms") is None:
            raise SystemExit(f"{label}.{case_name}.steps[0] missing latency_ms")
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


def _run_comparator(rust_report: dict[str, Any], native_report: dict[str, Any]) -> dict[str, Any]:
    with tempfile.TemporaryDirectory(prefix="storage-unified-case-report-") as tmpdir:
        root = Path(tmpdir)
        rust_path = root / "rust.json"
        native_path = root / "native.json"
        out_path = root / "comparison.json"
        rust_path.write_text(json.dumps(rust_report), encoding="utf-8")
        native_path.write_text(json.dumps(native_report), encoding="utf-8")
        completed = subprocess.run(
            [
                sys.executable,
                str(COMPARATOR),
                "--rust-report",
                str(rust_path),
                "--native-report",
                str(native_path),
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


def _run_adapter() -> dict[str, Any]:
    with tempfile.TemporaryDirectory(prefix="native-storage-unified-runner-") as tmpdir:
        root = Path(tmpdir)
        binary_path = root / "native_storage_unified_case_report_runner"
        native_report_path = root / "native-report.json"
        compile_result = subprocess.run(
            [
                "g++",
                "-std=native17",
                "-O2",
                "-I",
                str(ROOT),
                str(NATIVE_RUNNER),
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
            [str(binary_path), "--output", str(native_report_path)],
            cwd=ROOT,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            check=False,
        )
        if run_result.returncode != 0:
            raise SystemExit(run_result.stdout.rstrip())
        return json.loads(native_report_path.read_text(encoding="utf-8"))


def main() -> int:
    pair = _load()
    required_cases = _required_cases()
    if pair.get("schema") != EXPECTED_SCHEMA:
        raise SystemExit(f"{PAIR}: schema must be {EXPECTED_SCHEMA}")
    rust_report = pair.get("rust_report")
    native_report = pair.get("native_report")
    if not isinstance(rust_report, dict) or not isinstance(native_report, dict):
        raise SystemExit(f"{PAIR}: rust_report and native_report must be objects")
    _validate_outputs(_case_map(rust_report, "rust_report", required_cases), "rust_report")
    _validate_outputs(_case_map(native_report, "native_report", required_cases), "native_report")
    native_runtime_report = _run_adapter()
    _validate_outputs(
        _case_map(native_runtime_report, "native_runtime_report", required_cases),
        "native_runtime_report",
    )
    comparison = _run_comparator(rust_report, native_runtime_report)
    if comparison.get("ready") is not True:
        raise SystemExit(f"storage unified case report comparison is not ready: {comparison}")
    print(
        "storage unified case report pair passed: "
        f"cases={len(required_cases)} rows={comparison.get('row_count')} "
        "native_adapter=compiled"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
