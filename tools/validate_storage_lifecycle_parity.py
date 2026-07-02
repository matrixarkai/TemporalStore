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

REQUIRED_STORAGE_LIFECYCLE_METRICS = [
    "storage_manager_prepare_count",
    "storage_manager_reclaim_count",
    "storage_manager_evict_count",
    "storage_manager_expire_count",
    "storage_manager_page_gc_count",
    "storage_manager_compaction_count",
    "storage_manager_index_gc_count",
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
    "delayed_destroy_backlog",
    "follower_cursor_retention_floor",
    "reclaimable_bytes",
    "compaction_reclaimed_bytes",
    "physical_reclaim_errors",
    "append_watermark",
    "compaction_watermark",
]

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


def validate_contract_and_runner() -> list[str]:
    failures: list[str] = []
    contract_text = CONTRACT.read_text(encoding="utf-8")
    runner_metrics = _extract_runner_list("STORAGE_LIFECYCLE_METRIC_NAMES")
    if runner_metrics != REQUIRED_STORAGE_LIFECYCLE_METRICS:
        failures.append("runner:STORAGE_LIFECYCLE_METRIC_NAMES does not match the canonical lifecycle metric order")
    for metric in REQUIRED_STORAGE_LIFECYCLE_METRICS:
        if f"`{metric}`" not in contract_text:
            failures.append(f"contract missing lifecycle metric `{metric}`")
        if metric not in runner_metrics:
            failures.append(f"runner missing lifecycle metric `{metric}`")
    return failures


def validate_report_pair(cpp_report: dict[str, Any], rust_report: dict[str, Any]) -> list[str]:
    failures: list[str] = []
    cpp_metrics = _dig_metrics(cpp_report)
    rust_metrics = _dig_metrics(rust_report)
    cpp_config = _dig_config(cpp_report)
    rust_config = _dig_config(rust_report)

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

    for metric in [
        "cold_scan_no_cache_reads",
        "hot_cache_promotions",
        "compaction_reclaimed_bytes",
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
    return failures


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--cpp-report", type=pathlib.Path)
    parser.add_argument("--rust-report", type=pathlib.Path)
    args = parser.parse_args()

    failures = validate_contract_and_runner()
    if bool(args.cpp_report) != bool(args.rust_report):
        failures.append("--cpp-report and --rust-report must be provided together")
    if args.cpp_report and args.rust_report:
        failures.extend(validate_report_pair(_load_json(args.cpp_report), _load_json(args.rust_report)))

    if failures:
        for failure in failures:
            print(failure, file=sys.stderr)
        return 1

    print("storage lifecycle parity contract passed:")
    for metric in REQUIRED_STORAGE_LIFECYCLE_METRICS:
        print(f"- {metric}")
    if args.cpp_report and args.rust_report:
        print("- C++/Rust report pair exposes matching config and lifecycle metrics")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
