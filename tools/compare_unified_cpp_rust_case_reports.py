#!/usr/bin/env python3
"""Compare Rust and C++ shared TemporalStore case reports.

Expected input shape is intentionally small so both repos can emit it:

{
  "schema": "temporalstore_unified_case_report_v1",
  "cases": [
    {
      "name": "case_name",
      "status": "passed|failed|skipped",
      "steps": [
        {
          "name": "step_name",
          "status": "passed|failed|skipped",
          "output": {"kind": "bytes", "value": [1]},
          "latency_ms": 1.25
        }
      ]
    }
  ]
}

The output highlights Rust-only misses, C++-only misses, shared-hard failures,
output diffs, and latency deltas so migrated shared cases can replace duplicated
language-specific product tests without hiding parity gaps.
"""

from __future__ import annotations

import argparse
import json
import math
import sys
from pathlib import Path
from typing import Any


PASSED = {"passed", "ok", "success"}
FAILED = {"failed", "error", "timeout"}
SKIPPED = {"skipped", "not_run", "blocked"}


def load_report(path: Path) -> dict[str, Any]:
    try:
        with path.open("r", encoding="utf-8") as handle:
            report = json.load(handle)
    except OSError as exc:
        raise SystemExit(f"cannot read {path}: {exc}") from exc
    except json.JSONDecodeError as exc:
        raise SystemExit(f"{path}: invalid JSON: {exc}") from exc
    if not isinstance(report, dict):
        raise SystemExit(f"{path}: report must be an object")
    cases = report.get("cases")
    if not isinstance(cases, list):
        raise SystemExit(f"{path}: report.cases must be a list")
    return report


def normalize_status(value: Any) -> str:
    text = str(value or "").strip().lower()
    if text in PASSED:
        return "passed"
    if text in FAILED:
        return "failed"
    if text in SKIPPED:
        return "skipped"
    return "failed"


def passed(row: dict[str, Any] | None) -> bool:
    return row is not None and normalize_status(row.get("status")) == "passed"


def report_rows(report: dict[str, Any], side: str) -> dict[str, dict[str, Any]]:
    rows: dict[str, dict[str, Any]] = {}
    for case_index, case in enumerate(report.get("cases") or []):
        if not isinstance(case, dict):
            raise SystemExit(f"{side}: cases[{case_index}] must be an object")
        case_name = case.get("name")
        if not isinstance(case_name, str) or not case_name:
            raise SystemExit(f"{side}: cases[{case_index}] has no name")
        steps = case.get("steps") or []
        if not isinstance(steps, list):
            raise SystemExit(f"{side}: case {case_name} steps must be a list")
        if not steps:
            rows[case_name] = {
                "case": case_name,
                "step": None,
                "status": normalize_status(case.get("status")),
                "output": case.get("output"),
                "latency_ms": case.get("latency_ms"),
            }
            continue
        for step_index, step in enumerate(steps):
            if not isinstance(step, dict):
                raise SystemExit(f"{side}: case {case_name} steps[{step_index}] must be an object")
            step_name = step.get("name")
            if not isinstance(step_name, str) or not step_name:
                raise SystemExit(f"{side}: case {case_name} steps[{step_index}] has no name")
            key = f"{case_name}/{step_name}"
            if key in rows:
                raise SystemExit(f"{side}: duplicate row {key}")
            rows[key] = {
                "case": case_name,
                "step": step_name,
                "status": normalize_status(step.get("status", case.get("status"))),
                "output": step.get("output"),
                "latency_ms": step.get("latency_ms"),
            }
    return rows


def number(value: Any) -> float | None:
    if value is None:
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(parsed):
        return None
    return parsed


def row_summary(key: str, rust_row: dict[str, Any] | None, cpp_row: dict[str, Any] | None) -> dict[str, Any]:
    return {
        "row": key,
        "case": (rust_row or cpp_row or {}).get("case"),
        "step": (rust_row or cpp_row or {}).get("step"),
        "rust_status": None if rust_row is None else rust_row.get("status"),
        "cpp_status": None if cpp_row is None else cpp_row.get("status"),
    }


def compare_reports(
    rust: dict[str, Any],
    cpp: dict[str, Any],
    *,
    latency_ratio_tolerance: float,
    strict_outputs: bool,
) -> dict[str, Any]:
    rust_rows = report_rows(rust, "rust")
    cpp_rows = report_rows(cpp, "cpp")
    all_keys = sorted(set(rust_rows) | set(cpp_rows))

    rust_only_misses: list[dict[str, Any]] = []
    cpp_only_misses: list[dict[str, Any]] = []
    shared_hard_failures: list[dict[str, Any]] = []
    output_diffs: list[dict[str, Any]] = []
    latency_deltas: list[dict[str, Any]] = []
    missing_in_rust: list[str] = []
    missing_in_cpp: list[str] = []

    for key in all_keys:
        rust_row = rust_rows.get(key)
        cpp_row = cpp_rows.get(key)
        if rust_row is None:
            missing_in_rust.append(key)
        if cpp_row is None:
            missing_in_cpp.append(key)

        rust_passed = passed(rust_row)
        cpp_passed = passed(cpp_row)
        if not rust_passed and cpp_passed:
            rust_only_misses.append(row_summary(key, rust_row, cpp_row))
        elif rust_passed and not cpp_passed:
            cpp_only_misses.append(row_summary(key, rust_row, cpp_row))
        elif not rust_passed and not cpp_passed:
            shared_hard_failures.append(row_summary(key, rust_row, cpp_row))

        if rust_row is not None and cpp_row is not None:
            if rust_row.get("output") != cpp_row.get("output"):
                output_diffs.append(
                    {
                        "row": key,
                        "case": rust_row.get("case"),
                        "step": rust_row.get("step"),
                        "rust_output": rust_row.get("output"),
                        "cpp_output": cpp_row.get("output"),
                    }
                )
            rust_latency = number(rust_row.get("latency_ms"))
            cpp_latency = number(cpp_row.get("latency_ms"))
            if rust_latency is not None and cpp_latency is not None:
                smaller = max(min(rust_latency, cpp_latency), 1e-9)
                ratio = max(rust_latency, cpp_latency) / smaller
                delta = {
                    "row": key,
                    "case": rust_row.get("case"),
                    "step": rust_row.get("step"),
                    "rust_latency_ms": rust_latency,
                    "cpp_latency_ms": cpp_latency,
                    "absolute_ms": abs(rust_latency - cpp_latency),
                    "ratio": ratio,
                }
                latency_deltas.append(delta)

    latency_violations = [
        delta for delta in latency_deltas if delta["ratio"] > latency_ratio_tolerance
    ]
    failures: list[str] = []
    if rust_only_misses:
        failures.append(f"rust_only_misses={len(rust_only_misses)}")
    if cpp_only_misses:
        failures.append(f"cpp_only_misses={len(cpp_only_misses)}")
    if shared_hard_failures:
        failures.append(f"shared_hard_failures={len(shared_hard_failures)}")
    if strict_outputs and output_diffs:
        failures.append(f"output_diffs={len(output_diffs)}")
    if latency_violations:
        failures.append(f"latency_delta_violations={len(latency_violations)}")

    return {
        "schema": "temporalstore_unified_case_comparison_v1",
        "ready": not failures,
        "failures": failures,
        "rust_case_count": len(rust.get("cases") or []),
        "cpp_case_count": len(cpp.get("cases") or []),
        "row_count": len(all_keys),
        "missing_in_rust": missing_in_rust,
        "missing_in_cpp": missing_in_cpp,
        "rust_only_misses": rust_only_misses,
        "cpp_only_misses": cpp_only_misses,
        "shared_hard_failures": shared_hard_failures,
        "output_diffs": output_diffs,
        "latency_deltas": latency_deltas,
        "latency_delta_violations": latency_violations,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rust-report", type=Path, required=True)
    parser.add_argument("--cpp-report", type=Path, required=True)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--latency-ratio-tolerance", type=float, default=3.0)
    parser.add_argument(
        "--allow-output-diffs",
        action="store_true",
        help="include output_diffs in JSON but do not fail the comparison on them",
    )
    args = parser.parse_args()

    result = compare_reports(
        load_report(args.rust_report),
        load_report(args.cpp_report),
        latency_ratio_tolerance=args.latency_ratio_tolerance,
        strict_outputs=not args.allow_output_diffs,
    )
    text = json.dumps(result, indent=2, sort_keys=True)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(text + "\n", encoding="utf-8")
    print(text)
    return 0 if result["ready"] else 1


if __name__ == "__main__":
    sys.exit(main())
