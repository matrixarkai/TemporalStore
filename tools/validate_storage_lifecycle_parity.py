#!/usr/bin/env python3
"""Validate C++/Rust storage lifecycle parity contract and optional reports.

The lightweight default mode checks that the shared docs and scale runner agree
on the canonical StorageManager lifecycle metrics. When --cpp-report and
--rust-report are provided, the tool also normalizes backend reports and fails
closed on missing canonical metrics/config or obvious semantic drift.
"""

from __future__ import annotations

import argparse
import ast
import json
import pathlib
import sys
from typing import Any


ROOT = pathlib.Path(__file__).resolve().parents[1]
CONTRACT = ROOT / "docs" / "temporalstore_page_block_address_contract.md"
SCALE_REPORT = ROOT / "tools" / "run_matrixark_cpp_rust_scale_report.py"
REPORT_PAIR_CORPUS = ROOT / "compat" / "storage_lifecycle_report_pair_corpus.json"

REQUIRED_STORAGE_LIFECYCLE_METRICS = [
    "storage_manager_prepare_count",
    "storage_manager_reclaim_count",
    "storage_manager_evict_count",
    "storage_manager_expire_count",
    "storage_manager_page_gc_count",
    "storage_manager_block_gc_count",
    "storage_manager_compaction_count",
    "storage_manager_index_gc_count",
    "storage_manager_delayed_destroy_count",
    "storage_manager_follower_cursor_safety_count",
    "storage_manager_watermark_progress_count",
    "storage_manager_loop_ms",
    "stream_rollover_count",
    "storage_zone_total_bytes",
    "storage_zone_used_bytes",
    "storage_zone_stale_bytes",
    "cache_admissions",
    "cache_evictions",
    "cache_rehydrates",
    "cold_scan_no_cache_reads",
    "hot_cache_promotions",
    "tombstone_records",
    "stale_page_tombstones",
    "stale_block_tombstones",
    "stale_pages_rewritten",
    "stale_pages_skipped",
    "stale_blocks_rewritten",
    "stale_blocks_skipped",
    "delayed_destroy_backlog",
    "follower_cursor_retention_floor",
    "reclaimable_bytes",
    "compaction_reclaimed_bytes",
    "physical_reclaimed_bytes",
    "physical_reclaim_errors",
    "append_watermark",
    "compaction_watermark",
]

REQUIRED_STORAGE_READ_SEQUENCE = [
    "logical_key_timestamp_range",
    "page_index_lookup",
    "page_address_list",
    "block_index_lookup",
    "page_read",
    "decode_records",
]

REQUIRED_STORAGE_COLD_SCAN_SEQUENCE = [
    "timestamp_page_index_scan",
    "no_cache_page_read",
    "bounded_decode",
    "no_hot_cache_promotion",
]

REQUIRED_STORAGE_LIFECYCLE_PHASES = [
    "prepare",
    "reclaim",
    "evict",
    "expire",
    "page_gc",
    "block_gc",
    "compaction",
    "index_gc",
    "delayed_destroy",
    "follower_cursor_safety",
    "watermark_progress",
]

REQUIRED_STORAGE_RECLAIM_SEMANTICS = [
    "cache_eviction_memory_only",
    "logical_tombstone_required",
    "stale_pages_blocks_rewritten_or_skipped",
    "reclaimed_bytes_reported",
    "physical_reclaim_errors_zero",
]

REQUIRED_STORAGE_RECLAIM_SCOPE = {
    "owner": "temporalstore_storage_lifecycle",
    "matrixark_context_gc_role": "marks_logical_raw_event_eligibility_only",
    "physical_reclaim_context_specific": False,
}

REQUIRED_CONFIG_FIELDS = [
    "TS_CONTEXT_PAGE_TARGET_BYTES",
    "TS_BLOCK_SEGMENT_TARGET_BYTES",
    "TS_STORAGE_ZONE_SIZE",
    "TS_STREAM_MAX_BLOB_SIZE",
    "TS_COMPACTION_WATERMARK_BYTES",
    "TS_COLD_SCAN_NO_CACHE_FILL",
    "TS_PAGE_INDEX_CACHE_BYTES",
    "TS_BLOCK_INDEX_CACHE_BYTES",
]

CANONICAL_PUBLIC_FIELDS = [
    "PageAddress",
    "BlockAddress",
    "PageIndexEntry",
    "BlockIndexEntry",
    "StorageZone",
    "Segment",
    "Extent",
    "AppendWatermark",
    "CompactionWatermark",
]

CANONICAL_JSON_FIELDS = [
    "page_address",
    "block_address",
    "page_index_entry",
    "block_index_entry",
    "storage_zone",
    "segment",
    "extent",
    "append_watermark",
    "compaction_watermark",
]

LEGACY_ALIAS_MAP = {
    "page_store": "storage_zone",
    "block_store": "storage_zone",
    "page_segment": "segment",
    "page_segment_id": "segment_id",
    "stream_blob": "segment",
    "stream_blob_id": "segment_id",
    "zone": "storage_zone",
    "extent_id": "extent_id",
}

ALLOWED_ALIAS_CONTAINERS = {"compatibility_aliases", "legacy_alias", "legacy_aliases", "migration_aliases"}


def _extract_runner_list(name: str) -> list[str]:
    tree = ast.parse(SCALE_REPORT.read_text(encoding="utf-8"), filename=str(SCALE_REPORT))
    for node in tree.body:
        if isinstance(node, ast.Assign):
            if any(isinstance(target, ast.Name) and target.id == name for target in node.targets):
                value = ast.literal_eval(node.value)
                if not isinstance(value, list) or not all(isinstance(item, str) for item in value):
                    raise AssertionError(f"{name} must be a list[str]")
                return value
    raise AssertionError(f"{name} not found in scale report runner")


def _load_json(path: pathlib.Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _dig_metrics(report: dict[str, Any]) -> dict[str, Any]:
    candidates = [
        report.get("storage_lifecycle_metrics"),
        report.get("storage_lifecycle", {}).get("metrics") if isinstance(report.get("storage_lifecycle"), dict) else None,
        report.get("metrics", {}).get("storage_lifecycle") if isinstance(report.get("metrics"), dict) else None,
        report.get("metrics"),
    ]
    for candidate in candidates:
        if isinstance(candidate, dict):
            return candidate
    return {}


def _dig_config(report: dict[str, Any]) -> dict[str, Any]:
    candidates = [
        report.get("effective_storage_tuning"),
        report.get("config", {}).get("effective_storage_tuning") if isinstance(report.get("config"), dict) else None,
    ]
    for candidate in candidates:
        if isinstance(candidate, dict):
            return candidate
    return {}


def _dig_sequence(report: dict[str, Any], key: str) -> list[str]:
    candidates = [
        report.get(key),
        report.get("storage_lifecycle", {}).get(key) if isinstance(report.get("storage_lifecycle"), dict) else None,
        report.get("storage_sequences", {}).get(key) if isinstance(report.get("storage_sequences"), dict) else None,
    ]
    for candidate in candidates:
        if isinstance(candidate, list) and all(isinstance(item, str) for item in candidate):
            return candidate
    return []


def _dig_lifecycle_phases(report: dict[str, Any]) -> list[str]:
    candidates = [
        report.get("storage_lifecycle_phases"),
        report.get("storage_lifecycle", {}).get("phases") if isinstance(report.get("storage_lifecycle"), dict) else None,
    ]
    for candidate in candidates:
        if isinstance(candidate, list) and all(isinstance(item, str) for item in candidate):
            return candidate
    return []


def _dig_reclaim_semantics(report: dict[str, Any]) -> list[str]:
    candidates = [
        report.get("storage_reclaim_semantics"),
        report.get("storage_lifecycle", {}).get("reclaim_semantics") if isinstance(report.get("storage_lifecycle"), dict) else None,
    ]
    for candidate in candidates:
        if isinstance(candidate, list) and all(isinstance(item, str) for item in candidate):
            return candidate
    return []


def _dig_reclaim_scope(report: dict[str, Any]) -> dict[str, Any]:
    candidates = [
        report.get("storage_reclaim_scope"),
        report.get("storage_lifecycle", {}).get("reclaim_scope") if isinstance(report.get("storage_lifecycle"), dict) else None,
    ]
    for candidate in candidates:
        if isinstance(candidate, dict):
            return candidate
    return {}


def _as_number(value: Any) -> float | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value)
        except ValueError:
            return None
    return None


def _walk_public_keys(value: Any, *, in_alias_container: bool = False, path: tuple[str, ...] = ()) -> list[tuple[tuple[str, ...], str]]:
    violations: list[tuple[tuple[str, ...], str]] = []
    if isinstance(value, dict):
        for key, child in value.items():
            child_path = (*path, str(key))
            child_in_alias = in_alias_container or str(key) in ALLOWED_ALIAS_CONTAINERS
            if not child_in_alias and str(key) in LEGACY_ALIAS_MAP:
                violations.append((child_path, str(key)))
            violations.extend(_walk_public_keys(child, in_alias_container=child_in_alias, path=child_path))
    elif isinstance(value, list):
        for index, child in enumerate(value):
            violations.extend(_walk_public_keys(child, in_alias_container=in_alias_container, path=(*path, str(index))))
    return violations


def _normalize_public_storage_shape(report: dict[str, Any]) -> dict[str, Any]:
    """Return canonical public storage fields from a backend report.

    Canonical fields win. Legacy aliases are accepted only from compatibility
    alias containers and normalized for comparison.
    """
    source_candidates = [
        report.get("public_storage_contract"),
        report.get("storage_public_contract"),
        report.get("storage_lifecycle", {}).get("public_contract") if isinstance(report.get("storage_lifecycle"), dict) else None,
    ]
    source = next((candidate for candidate in source_candidates if isinstance(candidate, dict)), {})
    aliases = source.get("compatibility_aliases") if isinstance(source.get("compatibility_aliases"), dict) else {}

    normalized: dict[str, Any] = {}
    for key in CANONICAL_JSON_FIELDS:
        if key in source:
            normalized[key] = source[key]
    for alias, canonical in LEGACY_ALIAS_MAP.items():
        if alias in aliases and canonical not in normalized:
            normalized[canonical] = aliases[alias]
    return normalized


def _metric_number(metrics: dict[str, Any], name: str) -> float:
    value = _as_number(metrics.get(name))
    return 0.0 if value is None else value


def _validate_physical_reclaim_evidence(backend: str, metrics: dict[str, Any]) -> list[str]:
    failures: list[str] = []
    cache_evictions = _metric_number(metrics, "cache_evictions")
    physical_reclaimed = _metric_number(metrics, "physical_reclaimed_bytes")
    compaction_reclaimed = _metric_number(metrics, "compaction_reclaimed_bytes")
    physical_errors = _metric_number(metrics, "physical_reclaim_errors")
    tombstone_evidence = (
        _metric_number(metrics, "tombstone_records")
        + _metric_number(metrics, "stale_page_tombstones")
        + _metric_number(metrics, "stale_block_tombstones")
    )
    rewrite_or_skip_evidence = (
        _metric_number(metrics, "stale_pages_rewritten")
        + _metric_number(metrics, "stale_pages_skipped")
        + _metric_number(metrics, "stale_blocks_rewritten")
        + _metric_number(metrics, "stale_blocks_skipped")
    )

    if cache_evictions > 0 and physical_reclaimed == 0 and compaction_reclaimed == 0:
        # This is valid and intentional: cache eviction frees memory only.
        return failures

    if physical_reclaimed > 0:
        if physical_errors != 0:
            failures.append(f"{backend} physical reclaim reported bytes with errors={physical_errors}")
        if tombstone_evidence <= 0:
            failures.append(f"{backend} physical reclaim reported bytes without tombstone evidence")
        if rewrite_or_skip_evidence <= 0:
            failures.append(f"{backend} physical reclaim reported bytes without stale page/block rewrite-or-skip evidence")
        if compaction_reclaimed <= 0:
            failures.append(f"{backend} physical reclaim reported bytes without compaction_reclaimed_bytes")
    return failures


def validate_contract_and_runner() -> list[str]:
    failures: list[str] = []
    contract_text = CONTRACT.read_text(encoding="utf-8")
    runner_metrics = _extract_runner_list("STORAGE_LIFECYCLE_METRIC_NAMES")
    runner_read_sequence = _extract_runner_list("STORAGE_READ_SEQUENCE_STEPS")
    runner_cold_scan_sequence = _extract_runner_list("STORAGE_COLD_SCAN_SEQUENCE_STEPS")
    runner_lifecycle_phases = _extract_runner_list("STORAGE_LIFECYCLE_PHASE_NAMES")
    runner_reclaim_semantics = _extract_runner_list("STORAGE_RECLAIM_SEMANTICS")
    for name in CANONICAL_PUBLIC_FIELDS + CANONICAL_JSON_FIELDS:
        if f"`{name}`" not in contract_text:
            failures.append(f"contract missing canonical public field `{name}`")
    if runner_metrics != REQUIRED_STORAGE_LIFECYCLE_METRICS:
        failures.append("runner:STORAGE_LIFECYCLE_METRIC_NAMES does not match the canonical lifecycle metric order")
    if runner_read_sequence != REQUIRED_STORAGE_READ_SEQUENCE:
        failures.append("runner:STORAGE_READ_SEQUENCE_STEPS does not match the canonical read sequence")
    if runner_cold_scan_sequence != REQUIRED_STORAGE_COLD_SCAN_SEQUENCE:
        failures.append("runner:STORAGE_COLD_SCAN_SEQUENCE_STEPS does not match the canonical cold scan sequence")
    if runner_lifecycle_phases != REQUIRED_STORAGE_LIFECYCLE_PHASES:
        failures.append("runner:STORAGE_LIFECYCLE_PHASE_NAMES does not match the canonical lifecycle phase order")
    if runner_reclaim_semantics != REQUIRED_STORAGE_RECLAIM_SEMANTICS:
        failures.append("runner:STORAGE_RECLAIM_SEMANTICS does not match the canonical reclaim semantics")
    for metric in REQUIRED_STORAGE_LIFECYCLE_METRICS:
        if f"`{metric}`" not in contract_text:
            failures.append(f"contract missing lifecycle metric `{metric}`")
        if metric not in runner_metrics:
            failures.append(f"runner missing lifecycle metric `{metric}`")
    for step in REQUIRED_STORAGE_READ_SEQUENCE + REQUIRED_STORAGE_COLD_SCAN_SEQUENCE:
        if f"`{step}`" not in contract_text:
            failures.append(f"contract missing storage sequence step `{step}`")
    for phase in REQUIRED_STORAGE_LIFECYCLE_PHASES:
        if f"`{phase}`" not in contract_text:
            failures.append(f"contract missing lifecycle phase `{phase}`")
    for semantic in REQUIRED_STORAGE_RECLAIM_SEMANTICS:
        if f"`{semantic}`" not in contract_text:
            failures.append(f"contract missing reclaim semantic `{semantic}`")
    for value in REQUIRED_STORAGE_RECLAIM_SCOPE.values():
        if isinstance(value, str) and f"`{value}`" not in contract_text and f'"{value}"' not in contract_text:
            failures.append(f"contract missing reclaim scope value `{value}`")
    return failures


def validate_report_pair(cpp_report: dict[str, Any], rust_report: dict[str, Any]) -> list[str]:
    failures: list[str] = []
    cpp_metrics = _dig_metrics(cpp_report)
    rust_metrics = _dig_metrics(rust_report)
    cpp_config = _dig_config(cpp_report)
    rust_config = _dig_config(rust_report)
    cpp_public_shape = _normalize_public_storage_shape(cpp_report)
    rust_public_shape = _normalize_public_storage_shape(rust_report)
    cpp_read_sequence = _dig_sequence(cpp_report, "storage_read_sequence")
    rust_read_sequence = _dig_sequence(rust_report, "storage_read_sequence")
    cpp_cold_scan_sequence = _dig_sequence(cpp_report, "storage_cold_scan_sequence")
    rust_cold_scan_sequence = _dig_sequence(rust_report, "storage_cold_scan_sequence")
    cpp_lifecycle_phases = _dig_lifecycle_phases(cpp_report)
    rust_lifecycle_phases = _dig_lifecycle_phases(rust_report)
    cpp_reclaim_semantics = _dig_reclaim_semantics(cpp_report)
    rust_reclaim_semantics = _dig_reclaim_semantics(rust_report)
    cpp_reclaim_scope = _dig_reclaim_scope(cpp_report)
    rust_reclaim_scope = _dig_reclaim_scope(rust_report)

    for field in REQUIRED_CONFIG_FIELDS:
        if field not in cpp_config:
            failures.append(f"cpp config missing `{field}`")
        if field not in rust_config:
            failures.append(f"rust config missing `{field}`")
        if field in cpp_config and field in rust_config and cpp_config[field] != rust_config[field]:
            failures.append(f"config drift `{field}`: cpp={cpp_config[field]!r} rust={rust_config[field]!r}")

    for metric in REQUIRED_STORAGE_LIFECYCLE_METRICS:
        if metric not in cpp_metrics:
            failures.append(f"cpp metrics missing `{metric}`")
        if metric not in rust_metrics:
            failures.append(f"rust metrics missing `{metric}`")

    if cpp_read_sequence != REQUIRED_STORAGE_READ_SEQUENCE:
        failures.append(f"cpp storage_read_sequence drift: {cpp_read_sequence!r}")
    if rust_read_sequence != REQUIRED_STORAGE_READ_SEQUENCE:
        failures.append(f"rust storage_read_sequence drift: {rust_read_sequence!r}")
    if cpp_cold_scan_sequence != REQUIRED_STORAGE_COLD_SCAN_SEQUENCE:
        failures.append(f"cpp storage_cold_scan_sequence drift: {cpp_cold_scan_sequence!r}")
    if rust_cold_scan_sequence != REQUIRED_STORAGE_COLD_SCAN_SEQUENCE:
        failures.append(f"rust storage_cold_scan_sequence drift: {rust_cold_scan_sequence!r}")
    if cpp_lifecycle_phases != REQUIRED_STORAGE_LIFECYCLE_PHASES:
        failures.append(f"cpp storage_lifecycle_phases drift: {cpp_lifecycle_phases!r}")
    if rust_lifecycle_phases != REQUIRED_STORAGE_LIFECYCLE_PHASES:
        failures.append(f"rust storage_lifecycle_phases drift: {rust_lifecycle_phases!r}")
    if cpp_reclaim_semantics != REQUIRED_STORAGE_RECLAIM_SEMANTICS:
        failures.append(f"cpp storage_reclaim_semantics drift: {cpp_reclaim_semantics!r}")
    if rust_reclaim_semantics != REQUIRED_STORAGE_RECLAIM_SEMANTICS:
        failures.append(f"rust storage_reclaim_semantics drift: {rust_reclaim_semantics!r}")
    if cpp_reclaim_scope != REQUIRED_STORAGE_RECLAIM_SCOPE:
        failures.append(f"cpp storage_reclaim_scope drift: {cpp_reclaim_scope!r}")
    if rust_reclaim_scope != REQUIRED_STORAGE_RECLAIM_SCOPE:
        failures.append(f"rust storage_reclaim_scope drift: {rust_reclaim_scope!r}")

    for backend, report in [("cpp", cpp_report), ("rust", rust_report)]:
        for path, key in _walk_public_keys(report):
            failures.append(
                f"{backend} public report exposes legacy alias `{key}` outside compatibility_aliases at {'.'.join(path)}"
            )

    if cpp_public_shape or rust_public_shape:
        for field in CANONICAL_JSON_FIELDS:
            if field not in cpp_public_shape:
                failures.append(f"cpp public storage shape missing canonical `{field}`")
            if field not in rust_public_shape:
                failures.append(f"rust public storage shape missing canonical `{field}`")
        comparable_fields = [field for field in CANONICAL_JSON_FIELDS if field in cpp_public_shape and field in rust_public_shape]
        for field in comparable_fields:
            if type(cpp_public_shape[field]).__name__ != type(rust_public_shape[field]).__name__:
                failures.append(
                    f"public storage shape type drift `{field}`: cpp={type(cpp_public_shape[field]).__name__} "
                    f"rust={type(rust_public_shape[field]).__name__}"
                )

    for metric in [
        "cold_scan_no_cache_reads",
        "hot_cache_promotions",
        "compaction_reclaimed_bytes",
        "physical_reclaimed_bytes",
        "physical_reclaim_errors",
        "append_watermark",
        "compaction_watermark",
    ]:
        cpp_value = _as_number(cpp_metrics.get(metric))
        rust_value = _as_number(rust_metrics.get(metric))
        if cpp_value is None or rust_value is None:
            continue
        if metric == "physical_reclaim_errors" and (cpp_value != 0 or rust_value != 0):
            failures.append(f"physical reclaim errors must be zero: cpp={cpp_value} rust={rust_value}")
        if metric.endswith("_watermark") and (cpp_value < 0 or rust_value < 0):
            failures.append(f"watermark must be non-negative for `{metric}`")
    failures.extend(_validate_physical_reclaim_evidence("cpp", cpp_metrics))
    failures.extend(_validate_physical_reclaim_evidence("rust", rust_metrics))
    return failures


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--cpp-report", type=pathlib.Path)
    parser.add_argument("--rust-report", type=pathlib.Path)
    parser.add_argument(
        "--skip-report-pair-corpus",
        action="store_true",
        help="Only validate docs/runner contract unless explicit reports are supplied.",
    )
    args = parser.parse_args()

    failures = validate_contract_and_runner()
    validated_pair = False
    if not args.skip_report_pair_corpus and not (args.cpp_report or args.rust_report):
        corpus = _load_json(REPORT_PAIR_CORPUS)
        failures.extend(validate_report_pair(corpus["cpp"], corpus["rust"]))
        validated_pair = True
    if bool(args.cpp_report) != bool(args.rust_report):
        failures.append("--cpp-report and --rust-report must be provided together")
    if args.cpp_report and args.rust_report:
        failures.extend(validate_report_pair(_load_json(args.cpp_report), _load_json(args.rust_report)))
        validated_pair = True

    if failures:
        for failure in failures:
            print(failure, file=sys.stderr)
        return 1

    print("storage lifecycle parity contract passed:")
    print("- storage_read_sequence=" + " -> ".join(REQUIRED_STORAGE_READ_SEQUENCE))
    print("- storage_cold_scan_sequence=" + " -> ".join(REQUIRED_STORAGE_COLD_SCAN_SEQUENCE))
    print("- storage_lifecycle_phases=" + ", ".join(REQUIRED_STORAGE_LIFECYCLE_PHASES))
    print("- storage_reclaim_semantics=" + ", ".join(REQUIRED_STORAGE_RECLAIM_SEMANTICS))
    print(
        "- storage_reclaim_scope="
        + REQUIRED_STORAGE_RECLAIM_SCOPE["owner"]
        + " / context_gc="
        + REQUIRED_STORAGE_RECLAIM_SCOPE["matrixark_context_gc_role"]
    )
    for metric in REQUIRED_STORAGE_LIFECYCLE_METRICS:
        print(f"- {metric}")
    if validated_pair:
        print("- C++/Rust report pair exposes matching config, lifecycle metrics, and canonical public storage shape")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
