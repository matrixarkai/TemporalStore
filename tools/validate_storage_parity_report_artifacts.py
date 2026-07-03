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
    CANONICAL_JSON_FIELDS,
    CANONICAL_PUBLIC_FIELDS,
    REQUIRED_STORAGE_CACHE_LAYERS,
    REQUIRED_STORAGE_CACHE_SEMANTICS,
    REQUIRED_STORAGE_COLD_SCAN_SEQUENCE,
    REQUIRED_STORAGE_LIFECYCLE_METRICS,
    REQUIRED_STORAGE_LIFECYCLE_PHASES,
    REQUIRED_STORAGE_READ_SEQUENCE,
    REQUIRED_STORAGE_RECLAIM_SEMANTICS,
)
from validate_storage_tuning_parity import EXPECTED_KNOBS


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_ARTIFACT_ROOT = ROOT / "docs" / "benchmarks"

REPORT_NAMES = {"cpp.json", "rust.json", "comparison.json"}
REQUIRED_TOP_LEVEL_SECTIONS = (
    "effective_storage_tuning",
    "public_storage_contract",
    "storage_read_sequence",
    "storage_cold_scan_sequence",
    "storage_lifecycle_phases",
    "storage_lifecycle_metrics",
    "storage_cache_layers",
    "storage_cache_semantics",
    "storage_reclaim_semantics",
    "storage_cache_contract",
    "storage_reclaim_contract",
    "storage_manager_contract",
    "storage_index_contract",
)

LEGACY_PUBLIC_KEYS = {
    "page_store",
    "block_store",
    "stream_blob",
    "oplog",
}
ALLOWED_ALIAS_CONTAINER_KEYS = {
    "compatibility_aliases",
    "legacy_alias",
    "legacy_aliases",
    "migration_aliases",
}
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
        for key in sorted(EXPECTED_KNOBS):
            if key not in tuning:
                failures.append(f"{prefix} missing effective storage tuning `{key}`")

    public_contract = report.get("public_storage_contract")
    if public_contract != REQUIRED_PUBLIC_STORAGE_CONTRACT:
        failures.append(f"{prefix} public_storage_contract drift")

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

    lifecycle = report.get("storage_lifecycle_metrics")
    if not isinstance(lifecycle, dict):
        failures.append(f"{prefix} storage_lifecycle_metrics must be an object")
    else:
        for metric in REQUIRED_STORAGE_LIFECYCLE_METRICS:
            if metric not in lifecycle:
                failures.append(f"{prefix} missing storage lifecycle metric `{metric}`")

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
