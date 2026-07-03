#!/usr/bin/env python3
"""Validate committed C++/Rust parity report storage shape.

The shared storage lifecycle validators protect the canonical contract and
synthetic corpus. This gate protects the committed benchmark evidence under
``docs/benchmarks/parity_*`` so reports cannot drift back to backend-specific
public names or omit the storage lifecycle sections used by C++/Rust parity
comparisons.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any

from validate_storage_lifecycle_parity import (
    ALLOWED_ALIAS_CONTAINERS,
    CANONICAL_JSON_FIELDS,
    CANONICAL_PUBLIC_FIELDS,
    LEGACY_ALIAS_MAP,
    REQUIRED_STORAGE_CACHE_LAYERS,
    REQUIRED_STORAGE_CACHE_SEMANTICS,
    REQUIRED_STORAGE_CACHE_CONTRACT_FIELDS,
    REQUIRED_STORAGE_COLD_SCAN_SEQUENCE,
    REQUIRED_STORAGE_COLD_SCAN_METRICS,
    REQUIRED_STORAGE_COLD_SCAN_RESULT_FIELDS,
    REQUIRED_STORAGE_INDEX_CONTRACT_FIELDS,
    REQUIRED_STORAGE_INDEX_BEHAVIORS,
    REQUIRED_STORAGE_LIFECYCLE_METRICS,
    REQUIRED_STORAGE_LIFECYCLE_PHASES,
    REQUIRED_STORAGE_MANAGER_CONTRACT_FIELDS,
    REQUIRED_STORAGE_MANAGER_PHASE_METRICS,
    REQUIRED_STORAGE_READ_SEQUENCE,
    REQUIRED_STORAGE_READ_METRICS,
    REQUIRED_STORAGE_READ_RESULT_FIELDS,
    REQUIRED_STORAGE_RECLAIM_CONTRACT_FIELDS,
    REQUIRED_STORAGE_RECLAIM_SEMANTICS,
    REQUIRED_STORAGE_RECLAIM_SCOPE,
    REQUIRED_STORAGE_WRITE_METRICS,
    REQUIRED_STORAGE_WRITE_RESULT_FIELDS,
    REQUIRED_STORAGE_WRITE_SEQUENCE,
)
from validate_storage_tuning_parity import EXPECTED_DEFAULTS


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_ARTIFACT_ROOT = ROOT / "docs" / "benchmarks"

REPORT_NAMES = {"cpp.json", "rust.json", "comparison.json"}
REQUIRED_TOP_LEVEL_SECTIONS = (
    "effective_storage_tuning",
    "public_storage_contract",
    "storage_write_sequence",
    "storage_write_contract",
    "storage_read_sequence",
    "storage_read_contract",
    "storage_cold_scan_sequence",
    "storage_cold_scan_contract",
    "storage_lifecycle_phases",
    "storage_lifecycle_metrics",
    "storage_cache_layers",
    "storage_cache_semantics",
    "storage_reclaim_semantics",
    "storage_reclaim_scope",
    "storage_cache_contract",
    "storage_reclaim_contract",
    "storage_manager_contract",
    "storage_index_contract",
)

LEGACY_PUBLIC_KEYS = set(LEGACY_ALIAS_MAP)
ALLOWED_ALIAS_CONTAINER_KEYS = set(ALLOWED_ALIAS_CONTAINERS)
REQUIRED_PUBLIC_STORAGE_CONTRACT = {
    json_field: public_field
    for json_field, public_field in zip(CANONICAL_JSON_FIELDS, CANONICAL_PUBLIC_FIELDS)
}
REQUIRED_PUBLIC_STORAGE_CONTRACT["compatibility_aliases"] = {}


def _load_json(path: Path) -> dict[str, Any]:
    data = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise ValueError(f"{path}: report must be a JSON object")
    return data


def _iter_backend_reports(path: Path, data: dict[str, Any]) -> list[tuple[str, dict[str, Any]]]:
    if path.name == "comparison.json" and isinstance(data.get("backends"), dict):
        reports: list[tuple[str, dict[str, Any]]] = []
        for backend, report in data["backends"].items():
            if isinstance(report, dict):
                reports.append((str(backend), report))
        return reports
    return [(str(data.get("backend") or path.stem), data)]


def _find_legacy_alias_leaks(value: Any, path: tuple[str, ...] = ()) -> list[str]:
    leaks: list[str] = []
    if isinstance(value, dict):
        for key, child in value.items():
            if key in LEGACY_PUBLIC_KEYS and not any(
                container in ALLOWED_ALIAS_CONTAINER_KEYS for container in path
            ):
                leaks.append(".".join((*path, key)))
            leaks.extend(_find_legacy_alias_leaks(child, (*path, str(key))))
    elif isinstance(value, list):
        for index, child in enumerate(value):
            leaks.extend(_find_legacy_alias_leaks(child, (*path, f"[{index}]")))
    return leaks


def _require_object_fields(
    prefix: str,
    report: dict[str, Any],
    section: str,
    required_fields: list[str],
) -> list[str]:
    failures: list[str] = []
    value = report.get(section)
    if not isinstance(value, dict):
        return [f"{prefix} {section} must be an object"]
    for field in required_fields:
        if field not in value:
            failures.append(f"{prefix} {section} missing `{field}`")
    return failures


def _as_number(value: Any) -> float | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    return None


def _require_number(
    prefix: str,
    section: str,
    field: str,
    value: Any,
    failures: list[str],
    *,
    positive: bool = False,
    zero: bool = False,
) -> None:
    number = _as_number(value)
    if number is None:
        failures.append(f"{prefix} {section}.{field} must be numeric")
        return
    if zero and number != 0:
        failures.append(f"{prefix} {section}.{field} must be zero")
    elif positive and number <= 0:
        failures.append(f"{prefix} {section}.{field} must be positive")
    elif number < 0:
        failures.append(f"{prefix} {section}.{field} must be non-negative")


def _validate_manager_contract(prefix: str, contract: Any) -> list[str]:
    failures: list[str] = []
    if not isinstance(contract, dict):
        return [f"{prefix} storage_manager_contract must be an object"]
    if contract.get("manager_identity") != "StorageManager/StoreManager":
        failures.append(f"{prefix} storage_manager_contract.manager_identity drift")
    if contract.get("cpp_public_name") != "StorageManager":
        failures.append(f"{prefix} storage_manager_contract.cpp_public_name drift")
    if contract.get("rust_public_name") != "StoreManager":
        failures.append(f"{prefix} storage_manager_contract.rust_public_name drift")
    if contract.get("phase_order") != REQUIRED_STORAGE_LIFECYCLE_PHASES:
        failures.append(f"{prefix} storage_manager_contract.phase_order drift")
    if contract.get("phase_metrics") != REQUIRED_STORAGE_MANAGER_PHASE_METRICS:
        failures.append(f"{prefix} storage_manager_contract.phase_metrics drift")
    if contract.get("loop_metric") != "storage_manager_loop_ms":
        failures.append(f"{prefix} storage_manager_contract.loop_metric drift")
    if contract.get("phase_order_enforced") is not True:
        failures.append(f"{prefix} storage_manager_contract.phase_order_enforced must be true")
    _require_number(prefix, "storage_manager_contract", "missing_phase_count", contract.get("missing_phase_count"), failures, zero=True)
    _require_number(prefix, "storage_manager_contract", "loop_ms", contract.get("loop_ms"), failures)
    phase_counts = contract.get("phase_counts")
    if not isinstance(phase_counts, dict):
        failures.append(f"{prefix} storage_manager_contract.phase_counts must be an object")
    else:
        for phase in REQUIRED_STORAGE_LIFECYCLE_PHASES:
            if phase not in phase_counts:
                failures.append(f"{prefix} storage_manager_contract.phase_counts missing `{phase}`")
            else:
                _require_number(prefix, "storage_manager_contract.phase_counts", phase, phase_counts.get(phase), failures)
    return failures


def _validate_index_contract(prefix: str, contract: Any) -> list[str]:
    failures: list[str] = []
    if not isinstance(contract, dict):
        return [f"{prefix} storage_index_contract must be an object"]
    expected = {
        "page_address_codec": "PageAddress",
        "block_address_codec": "BlockAddress",
        "slot_index": "slot -> object/page refs",
        "object_index_entry": "{model/table/object_key} -> current page chain",
        "page_index": "logical timestamp/key ranges -> page addresses",
        "block_index": "page addresses -> physical durable locations",
    }
    for field, value in expected.items():
        if contract.get(field) != value:
            failures.append(f"{prefix} storage_index_contract.{field} drift")
    if contract.get("stable_order") != ["shard_id", "zone_id", "segment_id", "page_id", "offset"]:
        failures.append(f"{prefix} storage_index_contract.stable_order drift")
    if contract.get("required_behaviors") != REQUIRED_STORAGE_INDEX_BEHAVIORS:
        failures.append(f"{prefix} storage_index_contract.required_behaviors drift")
    for field in [
        "page_address_encode_decode",
        "block_address_encode_decode",
        "stable_order_verified",
        "timestamp_range_lookup_verified",
        "restart_rebuild_verified",
    ]:
        if contract.get(field) is not True:
            failures.append(f"{prefix} storage_index_contract.{field} must be true")
    for field in [
        "slot_index_entry_count",
        "slot_object_ref_count",
        "slot_page_ref_count",
        "object_index_entry_count",
        "page_index_entry_count",
        "block_index_entry_count",
    ]:
        _require_number(prefix, "storage_index_contract", field, contract.get(field), failures, positive=True)
    for field in ["unreadable_page_refs", "checksum_mismatches"]:
        _require_number(prefix, "storage_index_contract", field, contract.get(field), failures, zero=True)
    return failures


def _validate_cache_contract(prefix: str, contract: Any) -> list[str]:
    failures: list[str] = []
    if not isinstance(contract, dict):
        return [f"{prefix} storage_cache_contract must be an object"]
    if contract.get("layers") != REQUIRED_STORAGE_CACHE_LAYERS:
        failures.append(f"{prefix} storage_cache_contract.layers drift")
    if contract.get("semantics") != REQUIRED_STORAGE_CACHE_SEMANTICS:
        failures.append(f"{prefix} storage_cache_contract.semantics drift")
    expected_metrics = [
        "memory_cache_hits",
        "memory_cache_misses",
        "page_index_cache_hits",
        "page_index_cache_misses",
        "block_index_cache_hits",
        "block_index_cache_misses",
        "disk_cache_hits",
        "disk_cache_misses",
        "shared_store_read_throughs",
        "cache_refills",
        "cache_invalidations",
        "cache_writeback_queue_depth",
        "cache_writeback_rejections",
    ]
    if contract.get("metrics") != expected_metrics:
        failures.append(f"{prefix} storage_cache_contract.metrics drift")
    for field in [
        "hot_to_cold_lookup",
        "durable_refill_on_miss",
        "append_watermark_invalidation",
        "compaction_watermark_invalidation",
        "cold_scan_no_promote",
        "writeback_backpressure_measured",
    ]:
        if contract.get(field) is not True:
            failures.append(f"{prefix} storage_cache_contract.{field} must be true")
    for field in [
        "cache_refills",
        "cache_invalidations",
        "cache_writeback_queue_depth",
        "cache_writeback_rejections",
    ]:
        _require_number(prefix, "storage_cache_contract", field, contract.get(field), failures)
    _require_number(prefix, "storage_cache_contract", "hot_cache_promotions", contract.get("hot_cache_promotions"), failures, zero=True)
    return failures


def _validate_reclaim_contract(prefix: str, contract: Any) -> list[str]:
    failures: list[str] = []
    if not isinstance(contract, dict):
        return [f"{prefix} storage_reclaim_contract must be an object"]
    for field in [
        "cache_eviction_frees_memory_only",
        "logical_gc_marks_expired_deletable",
        "physical_reclaim_requires_compaction_or_safe_skip",
    ]:
        if contract.get(field) is not True:
            failures.append(f"{prefix} storage_reclaim_contract.{field} must be true")
    for field in [
        "cache_evictions",
        "tombstone_records",
        "stale_page_tombstones",
        "stale_block_tombstones",
        "stale_pages_rewritten",
        "stale_pages_skipped",
        "stale_blocks_rewritten",
        "stale_blocks_skipped",
        "reclaimable_bytes",
        "compaction_reclaimed_bytes",
        "physical_reclaimed_bytes",
    ]:
        _require_number(prefix, "storage_reclaim_contract", field, contract.get(field), failures)
    _require_number(prefix, "storage_reclaim_contract", "physical_reclaim_errors", contract.get("physical_reclaim_errors"), failures, zero=True)
    physical = _as_number(contract.get("physical_reclaimed_bytes")) or 0
    if physical > 0:
        tombstones = sum(_as_number(contract.get(field)) or 0 for field in [
            "tombstone_records",
            "stale_page_tombstones",
            "stale_block_tombstones",
        ])
        rewrite_or_skip = sum(_as_number(contract.get(field)) or 0 for field in [
            "stale_pages_rewritten",
            "stale_pages_skipped",
            "stale_blocks_rewritten",
            "stale_blocks_skipped",
        ])
        if tombstones <= 0:
            failures.append(f"{prefix} storage_reclaim_contract physical reclaim requires tombstone evidence")
        if rewrite_or_skip <= 0:
            failures.append(f"{prefix} storage_reclaim_contract physical reclaim requires rewrite-or-skip evidence")
        if (_as_number(contract.get("compaction_reclaimed_bytes")) or 0) <= 0:
            failures.append(f"{prefix} storage_reclaim_contract physical reclaim requires compaction_reclaimed_bytes")
    return failures


def _validate_report(path: Path, backend: str, report: dict[str, Any]) -> list[str]:
    prefix = f"{path}:{backend}"
    failures: list[str] = []

    for section in REQUIRED_TOP_LEVEL_SECTIONS:
        if section not in report:
            failures.append(f"{prefix} missing top-level `{section}`")

    tuning = report.get("effective_storage_tuning")
    if not isinstance(tuning, dict):
        failures.append(f"{prefix} effective_storage_tuning must be an object")
    else:
        for key, expected in sorted(EXPECTED_DEFAULTS.items()):
            if key not in tuning:
                failures.append(f"{prefix} missing effective storage tuning `{key}`")
            elif tuning.get(key) != expected:
                failures.append(
                    f"{prefix} effective storage tuning `{key}` drift: "
                    f"expected {expected!r} got {tuning.get(key)!r}"
                )

    public_contract = report.get("public_storage_contract")
    if public_contract != REQUIRED_PUBLIC_STORAGE_CONTRACT:
        failures.append(f"{prefix} public_storage_contract drift")

    if report.get("storage_write_sequence") != REQUIRED_STORAGE_WRITE_SEQUENCE:
        failures.append(f"{prefix} storage_write_sequence drift")
    if report.get("storage_read_sequence") != REQUIRED_STORAGE_READ_SEQUENCE:
        failures.append(f"{prefix} storage_read_sequence drift")
    if report.get("storage_cold_scan_sequence") != REQUIRED_STORAGE_COLD_SCAN_SEQUENCE:
        failures.append(f"{prefix} storage_cold_scan_sequence drift")
    if report.get("storage_lifecycle_phases") != REQUIRED_STORAGE_LIFECYCLE_PHASES:
        failures.append(f"{prefix} storage_lifecycle_phases drift")
    if report.get("storage_cache_layers") != REQUIRED_STORAGE_CACHE_LAYERS:
        failures.append(f"{prefix} storage_cache_layers drift")
    if report.get("storage_cache_semantics") != REQUIRED_STORAGE_CACHE_SEMANTICS:
        failures.append(f"{prefix} storage_cache_semantics drift")
    if report.get("storage_reclaim_semantics") != REQUIRED_STORAGE_RECLAIM_SEMANTICS:
        failures.append(f"{prefix} storage_reclaim_semantics drift")
    if report.get("storage_reclaim_scope") != REQUIRED_STORAGE_RECLAIM_SCOPE:
        failures.append(f"{prefix} storage_reclaim_scope drift")

    lifecycle = report.get("storage_lifecycle_metrics")
    if not isinstance(lifecycle, dict):
        failures.append(f"{prefix} storage_lifecycle_metrics must be an object")
    else:
        for metric in REQUIRED_STORAGE_LIFECYCLE_METRICS:
            if metric not in lifecycle:
                failures.append(f"{prefix} missing storage lifecycle metric `{metric}`")

    failures.extend(
        _require_object_fields(
            prefix,
            report,
            "storage_write_contract",
            [*REQUIRED_STORAGE_WRITE_RESULT_FIELDS, *REQUIRED_STORAGE_WRITE_METRICS],
        )
    )
    failures.extend(
        _require_object_fields(
            prefix,
            report,
            "storage_read_contract",
            [*REQUIRED_STORAGE_READ_RESULT_FIELDS, *REQUIRED_STORAGE_READ_METRICS],
        )
    )
    failures.extend(
        _require_object_fields(
            prefix,
            report,
            "storage_cold_scan_contract",
            [*REQUIRED_STORAGE_COLD_SCAN_RESULT_FIELDS, *REQUIRED_STORAGE_COLD_SCAN_METRICS],
        )
    )
    failures.extend(
        _require_object_fields(
            prefix,
            report,
            "storage_manager_contract",
            REQUIRED_STORAGE_MANAGER_CONTRACT_FIELDS,
        )
    )
    failures.extend(
        _require_object_fields(
            prefix,
            report,
            "storage_index_contract",
            REQUIRED_STORAGE_INDEX_CONTRACT_FIELDS,
        )
    )
    failures.extend(
        _require_object_fields(
            prefix,
            report,
            "storage_cache_contract",
            REQUIRED_STORAGE_CACHE_CONTRACT_FIELDS,
        )
    )
    failures.extend(
        _require_object_fields(
            prefix,
            report,
            "storage_reclaim_contract",
            REQUIRED_STORAGE_RECLAIM_CONTRACT_FIELDS,
        )
    )
    failures.extend(_validate_manager_contract(prefix, report.get("storage_manager_contract")))
    failures.extend(_validate_index_contract(prefix, report.get("storage_index_contract")))
    failures.extend(_validate_cache_contract(prefix, report.get("storage_cache_contract")))
    failures.extend(_validate_reclaim_contract(prefix, report.get("storage_reclaim_contract")))

    leaks = _find_legacy_alias_leaks(report)
    for leak in leaks:
        failures.append(f"{prefix} legacy alias exposed outside compatibility_aliases at {leak}")

    return failures


def validate_artifacts(root: Path = DEFAULT_ARTIFACT_ROOT) -> tuple[int, list[str]]:
    failures: list[str] = []
    scanned = 0
    for path in sorted(root.glob("parity_*/*.json")):
        if path.name not in REPORT_NAMES:
            continue
        data = _load_json(path)
        for backend, report in _iter_backend_reports(path, data):
            scanned += 1
            failures.extend(_validate_report(path, backend, report))
    return scanned, failures


def main() -> int:
    scanned, failures = validate_artifacts()
    if failures:
        raise SystemExit(
            "TemporalStore storage parity report artifacts drifted:\n"
            + "\n".join(f"- {failure}" for failure in failures[:80])
        )
    print(f"TemporalStore storage parity report artifacts validated reports_scanned={scanned}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
