#!/usr/bin/env python3
"""Audit C++/Rust performance artifacts against the fail-closed matrix policy."""

from __future__ import annotations

import argparse
import copy
import json
from pathlib import Path
from typing import Any

from import_temporalstore_cpp_rust_performance_evidence import (
    DEFAULT_MATRIX,
    _candidate_workloads,
    import_report,
)


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_ARTIFACT_ROOT = ROOT / "docs" / "benchmarks"


def _load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _row_by_workload(matrix: dict[str, Any]) -> dict[str, dict[str, Any]]:
    rows = matrix.get("rows") if isinstance(matrix.get("rows"), list) else []
    return {str(row.get("workload")): row for row in rows if isinstance(row, dict)}


def audit_report(base_matrix: dict[str, Any], report_path: Path) -> dict[str, Any]:
    report = _load_json(report_path)
    workloads = [workload for workload, _mode in _candidate_workloads(report)]
    updated = import_report(copy.deepcopy(base_matrix), report)
    rows = _row_by_workload(updated)
    workload_results = []
    for workload in workloads:
        row = rows.get(workload, {})
        workload_results.append(
            {
                "workload": workload,
                "status": row.get("status"),
                "same_config_match": row.get("same_config_match") is True,
                "open_blockers": row.get("open_blockers") or [],
                "ratios": row.get("ratios") or {},
            }
        )
    importable = [
        row
        for row in workload_results
        if row["status"] in {"performance_candidate", "production_performance_parity"}
        and not row["open_blockers"]
    ]
    importable_workloads = {item["workload"] for item in importable}
    return {
        "path": str(report_path.relative_to(ROOT) if report_path.is_relative_to(ROOT) else report_path),
        "candidate_workloads": workloads,
        "importable_workloads": [row["workload"] for row in importable],
        "blocked_workloads": [
            row for row in workload_results if row["workload"] not in importable_workloads
        ],
    }


def audit_artifacts(artifact_root: Path, matrix_path: Path) -> dict[str, Any]:
    base_matrix = _load_json(matrix_path)
    reports = sorted(artifact_root.rglob("comparison.json"))
    entries = [audit_report(base_matrix, report) for report in reports]
    with_workloads = [entry for entry in entries if entry["candidate_workloads"]]
    importable = [
        entry
        for entry in with_workloads
        if entry["importable_workloads"]
    ]
    return {
        "schema": "temporalstore_cpp_rust_performance_artifact_audit_v1",
        "artifact_root": str(artifact_root),
        "matrix": str(matrix_path),
        "reports_scanned": len(reports),
        "reports_with_candidate_workloads": len(with_workloads),
        "reports_with_importable_workloads": len(importable),
        "entries": with_workloads,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--artifact-root", type=Path, default=DEFAULT_ARTIFACT_ROOT)
    parser.add_argument("--matrix", type=Path, default=DEFAULT_MATRIX)
    parser.add_argument("--output", type=Path)
    parser.add_argument(
        "--require-importable",
        action="store_true",
        help="Fail if no scanned artifact is admissible as parity evidence.",
    )
    args = parser.parse_args()

    audit = audit_artifacts(args.artifact_root, args.matrix)
    text = json.dumps(audit, indent=2) + "\n"
    if args.output:
        args.output.write_text(text, encoding="utf-8")
    else:
        print(text, end="")
    if args.require_importable and not audit["reports_with_importable_workloads"]:
        raise SystemExit("no importable C++/Rust performance artifacts found")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
