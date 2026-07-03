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
    SAME_CONFIG_KEYS,
    _candidate_workloads,
    import_report,
)


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_ARTIFACT_ROOT = ROOT / "docs" / "benchmarks"
RUNNER = "tools/run_matrixark_cpp_rust_scale_report.py"
IMPORTER = "tools/import_temporalstore_cpp_rust_performance_evidence.py"
WORKLOAD_RUN_ARGS = {
    "1K_event_ingestion": ["--events", "1000"],
    "10K_event_ingestion": ["--events", "10000"],
    "100K_event_ingestion": ["--events", "100000"],
    "retrieve_workers_4": ["--retrieve-workers", "4"],
    "retrieve_workers_8": ["--retrieve-workers", "8"],
    "retrieve_workers_16": ["--retrieve-workers", "16"],
    "retrieve_workers_32": ["--retrieve-workers", "32"],
}


def _artifact_dir(workload: str) -> str:
    return f"docs/benchmarks/parity_{workload}"


def _load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _row_by_workload(matrix: dict[str, Any]) -> dict[str, dict[str, Any]]:
    rows = matrix.get("rows") if isinstance(matrix.get("rows"), list) else []
    return {str(row.get("workload")): row for row in rows if isinstance(row, dict)}


def _next_run_command(workload: str) -> list[str]:
    artifact_dir = _artifact_dir(workload)
    return [
        "python",
        RUNNER,
        *WORKLOAD_RUN_ARGS.get(workload, []),
        "--backends",
        "cpp",
        "rust",
        "--artifact-dir",
        artifact_dir,
        "--require-phase-scale-matrix",
        "--require-perf-parity",
    ]


def _import_command(workload: str) -> list[str]:
    return [
        "python",
        IMPORTER,
        "--report",
        f"{_artifact_dir(workload)}/comparison.json",
        "--validate",
    ]


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
    required_workloads = [
        str(workload)
        for workload in base_matrix.get("required_workloads", [])
        if isinstance(workload, str) and workload
    ]
    reports = sorted(artifact_root.rglob("comparison.json"))
    entries = [audit_report(base_matrix, report) for report in reports]
    with_workloads = [entry for entry in entries if entry["candidate_workloads"]]
    importable = [
        entry
        for entry in with_workloads
        if entry["importable_workloads"]
    ]
    coverage = {
        workload: {
            "candidate_report_count": 0,
            "importable_report_count": 0,
            "blocked_report_count": 0,
            "blockers": [],
        }
        for workload in required_workloads
    }
    for entry in with_workloads:
        for workload in entry["candidate_workloads"]:
            target = coverage.setdefault(
                workload,
                {
                    "candidate_report_count": 0,
                    "importable_report_count": 0,
                    "blocked_report_count": 0,
                    "blockers": [],
                },
            )
            target["candidate_report_count"] += 1
        for workload in entry["importable_workloads"]:
            coverage[workload]["importable_report_count"] += 1
        for blocked in entry["blocked_workloads"]:
            target = coverage.setdefault(
                str(blocked.get("workload")),
                {
                    "candidate_report_count": 0,
                    "importable_report_count": 0,
                    "blocked_report_count": 0,
                    "blockers": [],
                },
            )
            target["blocked_report_count"] += 1
            for blocker in blocked.get("open_blockers") or []:
                if blocker not in target["blockers"]:
                    target["blockers"].append(blocker)
    missing_required_workloads = [
        workload
        for workload in required_workloads
        if coverage.get(workload, {}).get("candidate_report_count", 0) == 0
    ]
    blocked_required_workloads = [
        workload
        for workload in required_workloads
        if coverage.get(workload, {}).get("candidate_report_count", 0) > 0
        and coverage.get(workload, {}).get("importable_report_count", 0) == 0
    ]
    required_workload_status = {}
    for workload in required_workloads:
        row = coverage.get(workload, {})
        if row.get("importable_report_count", 0) > 0:
            status = "has_importable"
        elif row.get("candidate_report_count", 0) > 0:
            status = "blocked_no_importable"
        else:
            status = "missing_candidate"
        required_workload_status[workload] = {
            "status": status,
            "candidate_report_count": row.get("candidate_report_count", 0),
            "importable_report_count": row.get("importable_report_count", 0),
            "blocked_report_count": row.get("blocked_report_count", 0),
            "blockers": row.get("blockers", []),
            "next_run_hint": {
                "workload": workload,
                "artifact_dir": _artifact_dir(workload),
                "comparison_path": f"{_artifact_dir(workload)}/comparison.json",
                "command": _next_run_command(workload),
                "import_command": _import_command(workload),
                "required_same_config_fields": SAME_CONFIG_KEYS,
                "required_result": (
                    "same-config C++ and Rust comparison.json with passed backends, "
                    "selected_ref_parity=true, zero errors/timeouts/fallbacks, "
                    "and QPS/latency ratios within policy"
                ),
            },
        }
    next_required_runs = [
        {
            "workload": workload,
            "reason": details["status"],
            "blockers": details["blockers"],
            "artifact_dir": details["next_run_hint"]["artifact_dir"],
            "comparison_path": details["next_run_hint"]["comparison_path"],
            "command": details["next_run_hint"]["command"],
            "import_command": details["next_run_hint"]["import_command"],
            "required_same_config_fields": details["next_run_hint"]["required_same_config_fields"],
            "required_result": details["next_run_hint"]["required_result"],
        }
        for workload, details in required_workload_status.items()
        if details["status"] != "has_importable"
    ]
    next_required_runs.sort(
        key=lambda item: (
            0 if item["reason"] == "missing_candidate" else 1,
            required_workloads.index(item["workload"]) if item["workload"] in required_workloads else len(required_workloads),
        )
    )
    return {
        "schema": "temporalstore_cpp_rust_performance_artifact_audit_v1",
        "artifact_root": str(artifact_root),
        "matrix": str(matrix_path),
        "required_workloads": required_workloads,
        "reports_scanned": len(reports),
        "reports_with_candidate_workloads": len(with_workloads),
        "reports_with_importable_workloads": len(importable),
        "missing_required_workloads": missing_required_workloads,
        "blocked_required_workloads": blocked_required_workloads,
        "required_workload_status": required_workload_status,
        "next_required_runs": next_required_runs,
        "workload_coverage": coverage,
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
