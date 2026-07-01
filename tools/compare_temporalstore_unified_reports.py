#!/usr/bin/env python3
"""Compare Rust/C++ TemporalStore unified report JSON files.

The comparator is intentionally schema-light: shared runners can evolve their
payloads as long as they emit case IDs, pass/fail status, and optional latency.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any


def _load(path: str) -> dict[str, Any]:
    with Path(path).open("r", encoding="utf-8") as fh:
        data = json.load(fh)
    if not isinstance(data, dict):
        raise SystemExit(f"{path}: report must be a JSON object")
    return data


def _case_id(case: dict[str, Any], index: int) -> str:
    for key in ("case_id", "id", "name"):
        value = case.get(key)
        if isinstance(value, str) and value:
            return value
    return f"case[{index}]"


def _cases(report: dict[str, Any]) -> dict[str, dict[str, Any]]:
    raw = report.get("cases", report.get("results", []))
    if not isinstance(raw, list):
        raise SystemExit("report cases/results must be a list")
    out: dict[str, dict[str, Any]] = {}
    for index, case in enumerate(raw):
        if isinstance(case, dict):
            out[_case_id(case, index)] = case
    return out


def _passed(case: dict[str, Any]) -> bool:
    for key in ("passed", "ok", "success"):
        if key in case:
            return bool(case[key])
    status = str(case.get("status", "")).lower()
    return status in {"pass", "passed", "ok", "success"}


def _latency_ms(case: dict[str, Any]) -> float | None:
    for key in ("latency_ms", "p95_ms", "duration_ms"):
        value = case.get(key)
        if isinstance(value, (int, float)):
            return float(value)
    metrics = case.get("metrics")
    if isinstance(metrics, dict):
        value = metrics.get("latency_ms", metrics.get("p95_ms"))
        if isinstance(value, (int, float)):
            return float(value)
    return None


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--rust", required=True)
    parser.add_argument("--cpp", required=True)
    parser.add_argument("--emit-misses", default="")
    parser.add_argument("--latency-delta-ms", type=float, default=50.0)
    args = parser.parse_args()

    rust_cases = _cases(_load(args.rust))
    cpp_cases = _cases(_load(args.cpp))
    rust_ids = set(rust_cases)
    cpp_ids = set(cpp_cases)
    shared = sorted(rust_ids & cpp_ids)

    rust_only = sorted(rust_ids - cpp_ids)
    cpp_only = sorted(cpp_ids - rust_ids)
    shared_failures = [
        case_id
        for case_id in shared
        if _passed(rust_cases[case_id]) != _passed(cpp_cases[case_id])
    ]
    latency_deltas = []
    for case_id in shared:
        rust_latency = _latency_ms(rust_cases[case_id])
        cpp_latency = _latency_ms(cpp_cases[case_id])
        if rust_latency is None or cpp_latency is None:
            continue
        delta = rust_latency - cpp_latency
        if abs(delta) > args.latency_delta_ms:
            latency_deltas.append(
                {
                    "case_id": case_id,
                    "rust_latency_ms": rust_latency,
                    "cpp_latency_ms": cpp_latency,
                    "delta_ms": delta,
                }
            )

    report = {
        "schema": "temporalstore_unified_report_comparison_v1",
        "rust_case_count": len(rust_cases),
        "cpp_case_count": len(cpp_cases),
        "shared_case_count": len(shared),
        "rust_only_misses": rust_only,
        "cpp_only_misses": cpp_only,
        "shared_failures": shared_failures,
        "latency_deltas": latency_deltas,
    }
    print(json.dumps(report, indent=2, sort_keys=True))
    return 1 if rust_only or cpp_only or shared_failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
