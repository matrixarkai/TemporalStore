#!/usr/bin/env python3
"""Compare Rust and C++ Context benchmark reports against the shared contract."""

from __future__ import annotations

import argparse
import json
import math
import sys
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CORPUS = ROOT / "compat" / "unified_temporalstore_cases.json"
BENCHMARK_SUITE = "cpp_context_benchmark_parity"
NUMERIC_SUMMARY_FIELDS = (
    "case_count",
    "benchmark_per_query_count",
    "hit_rate",
    "benchmark_hit_at_k",
    "benchmark_recall_at_k",
    "benchmark_mean_reciprocal_rank",
    "benchmark_token_reduction_percent",
    "reader_hit_rate",
    "benchmark_threshold_violation_count",
)
LATENCY_FIELDS = (
    "benchmark_retrieval_p50_ms",
    "benchmark_retrieval_p95_ms",
    "benchmark_reader_p50_ms",
    "benchmark_reader_p95_ms",
)
PER_QUERY_EXACT_FIELDS = (
    "query_id",
    "category",
    "hit",
    "rank",
    "reader_hit",
    "retrieved_blocks",
    "source_tokens",
    "retrieved_tokens",
)
PER_QUERY_NUMERIC_FIELDS = (
    "token_reduction_percent",
)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rust-report", type=Path, required=True)
    parser.add_argument("--cpp-report", type=Path, required=True)
    parser.add_argument("--corpus", type=Path, default=DEFAULT_CORPUS)
    parser.add_argument("--case-name", default="")
    parser.add_argument("--dataset", default="")
    parser.add_argument("--numeric-tolerance", type=float, default=1e-9)
    parser.add_argument(
        "--latency-ratio-tolerance",
        type=float,
        default=5.0,
        help="Allow p50/p95 latency fields to differ by this multiplicative ratio.",
    )
    parser.add_argument("--output", type=Path, default=None)
    args = parser.parse_args()

    contract = load_benchmark_contract(args.corpus, args.case_name)
    rust = load_report(args.rust_report)
    cpp = load_report(args.cpp_report)
    failures: list[str] = []

    validate_report("rust", rust, contract, failures)
    validate_report("cpp", cpp, contract, failures)
    if args.dataset:
        compare_equal("dataset", rust.get("dataset"), cpp.get("dataset"), failures)
        if rust.get("dataset") != args.dataset:
            failures.append(f"rust dataset {rust.get('dataset')!r} != expected {args.dataset!r}")
        if cpp.get("dataset") != args.dataset:
            failures.append(f"cpp dataset {cpp.get('dataset')!r} != expected {args.dataset!r}")

    compare_summary(rust, cpp, args.numeric_tolerance, args.latency_ratio_tolerance, failures)
    compare_thresholds(rust, cpp, args.numeric_tolerance, failures)
    compare_per_query(rust, cpp, args.numeric_tolerance, failures)

    result = {
        "ready": not failures,
        "rust_report": str(args.rust_report),
        "cpp_report": str(args.cpp_report),
        "case_name": contract["case_name"],
        "format": contract["format"],
        "rust_case_count": rust.get("case_count"),
        "cpp_case_count": cpp.get("case_count"),
        "rust_per_query_count": len(rust.get("benchmark_per_query") or []),
        "cpp_per_query_count": len(cpp.get("benchmark_per_query") or []),
        "failure_count": len(failures),
        "failures": failures,
    }
    text = json.dumps(result, indent=2, sort_keys=True) + "\n"
    if args.output is not None:
        args.output.write_text(text, encoding="utf-8")
    print(text, end="")
    return 0 if not failures else 1


def load_benchmark_contract(corpus_path: Path, case_name: str) -> dict[str, Any]:
    corpus = json.loads(corpus_path.read_text(encoding="utf-8"))
    candidates = []
    for case in corpus.get("cases", []):
        if case_name and case.get("name") != case_name:
            continue
        for step in case.get("steps", []):
            command = step.get("command", {})
            if command.get("suite") != BENCHMARK_SUITE:
                continue
            report_contract = command.get("report_contract", {})
            candidates.append(
                {
                    "case_name": case.get("name"),
                    "step_name": step.get("name"),
                    "format": report_contract.get("format"),
                    "required_fields": report_contract.get("required_fields", []),
                    "per_query_required_fields": report_contract.get("per_query_required_fields", []),
                    "threshold_profiles": command.get("threshold_profiles", []),
                    "datasets": command.get("datasets", []),
                }
            )
    if not candidates:
        raise SystemExit(f"{corpus_path}: no benchmark contract found for case {case_name or '<any>'}")
    if case_name:
        return candidates[0]
    full = [candidate for candidate in candidates if candidate["case_name"] == "context_benchmark_full_dataset_gates"]
    return full[0] if full else candidates[0]


def load_report(path: Path) -> dict[str, Any]:
    try:
        report = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:  # noqa: BLE001 - report validator should surface exact local issue.
        raise SystemExit(f"failed to read {path}: {exc}") from exc
    if not isinstance(report, dict):
        raise SystemExit(f"{path}: report must be a JSON object")
    return report


def validate_report(label: str, report: dict[str, Any], contract: dict[str, Any], failures: list[str]) -> None:
    for field in contract["required_fields"]:
        if field not in report:
            failures.append(f"{label}: missing required field {field}")
    rows = report.get("benchmark_per_query")
    if not isinstance(rows, list):
        failures.append(f"{label}: benchmark_per_query must be a list")
        return
    for index, row in enumerate(rows):
        if not isinstance(row, dict):
            failures.append(f"{label}: benchmark_per_query[{index}] must be an object")
            continue
        for field in contract["per_query_required_fields"]:
            if field not in row:
                failures.append(f"{label}: benchmark_per_query[{index}] missing {field}")
    count = report.get("benchmark_per_query_count")
    if isinstance(count, int) and count != len(rows):
        failures.append(f"{label}: benchmark_per_query_count {count} != rows {len(rows)}")


def compare_summary(
    rust: dict[str, Any],
    cpp: dict[str, Any],
    tolerance: float,
    latency_ratio_tolerance: float,
    failures: list[str],
) -> None:
    for field in ("benchmark_family", "reader_provider_name", "reader_model"):
        compare_equal(field, rust.get(field), cpp.get(field), failures)
    for field in NUMERIC_SUMMARY_FIELDS:
        compare_number(field, rust.get(field), cpp.get(field), tolerance, failures)
    for field in LATENCY_FIELDS:
        compare_latency(field, rust.get(field), cpp.get(field), latency_ratio_tolerance, failures)
    compare_equal(
        "benchmark_threshold_violations",
        rust.get("benchmark_threshold_violations"),
        cpp.get("benchmark_threshold_violations"),
        failures,
    )


def compare_thresholds(
    rust: dict[str, Any],
    cpp: dict[str, Any],
    tolerance: float,
    failures: list[str],
) -> None:
    rust_thresholds = rust.get("benchmark_thresholds")
    cpp_thresholds = cpp.get("benchmark_thresholds")
    if not isinstance(rust_thresholds, dict) or not isinstance(cpp_thresholds, dict):
        failures.append("benchmark_thresholds must be objects in both reports")
        return
    if set(rust_thresholds) != set(cpp_thresholds):
        failures.append(
            f"benchmark_thresholds keys differ: rust={sorted(rust_thresholds)} cpp={sorted(cpp_thresholds)}"
        )
        return
    for field in sorted(rust_thresholds):
        if isinstance(rust_thresholds[field], bool) or isinstance(cpp_thresholds[field], bool):
            compare_equal(f"benchmark_thresholds.{field}", rust_thresholds[field], cpp_thresholds[field], failures)
        else:
            compare_number(
                f"benchmark_thresholds.{field}",
                rust_thresholds[field],
                cpp_thresholds[field],
                tolerance,
                failures,
            )


def compare_per_query(
    rust: dict[str, Any],
    cpp: dict[str, Any],
    tolerance: float,
    failures: list[str],
) -> None:
    rust_rows = rows_by_query_id("rust", rust.get("benchmark_per_query"), failures)
    cpp_rows = rows_by_query_id("cpp", cpp.get("benchmark_per_query"), failures)
    if not rust_rows or not cpp_rows:
        return
    missing_in_cpp = sorted(set(rust_rows) - set(cpp_rows))
    missing_in_rust = sorted(set(cpp_rows) - set(rust_rows))
    if missing_in_cpp:
        failures.append(f"cpp missing query_ids: {missing_in_cpp[:20]}")
    if missing_in_rust:
        failures.append(f"rust missing query_ids: {missing_in_rust[:20]}")
    for query_id in sorted(set(rust_rows) & set(cpp_rows)):
        rust_row = rust_rows[query_id]
        cpp_row = cpp_rows[query_id]
        for field in PER_QUERY_EXACT_FIELDS:
            compare_equal(f"query[{query_id}].{field}", rust_row.get(field), cpp_row.get(field), failures)
        for field in PER_QUERY_NUMERIC_FIELDS:
            compare_number(
                f"query[{query_id}].{field}",
                rust_row.get(field),
                cpp_row.get(field),
                tolerance,
                failures,
            )


def rows_by_query_id(label: str, rows: Any, failures: list[str]) -> dict[str, dict[str, Any]]:
    if not isinstance(rows, list):
        return {}
    out: dict[str, dict[str, Any]] = {}
    for index, row in enumerate(rows):
        if not isinstance(row, dict):
            continue
        query_id = row.get("query_id")
        if not isinstance(query_id, str) or not query_id:
            failures.append(f"{label}: benchmark_per_query[{index}] missing query_id")
            continue
        if query_id in out:
            failures.append(f"{label}: duplicate query_id {query_id}")
        out[query_id] = row
    return out


def compare_equal(field: str, rust_value: Any, cpp_value: Any, failures: list[str]) -> None:
    if rust_value != cpp_value:
        failures.append(f"{field}: rust={rust_value!r} cpp={cpp_value!r}")


def compare_number(field: str, rust_value: Any, cpp_value: Any, tolerance: float, failures: list[str]) -> None:
    try:
        rust_number = float(rust_value)
        cpp_number = float(cpp_value)
    except (TypeError, ValueError):
        failures.append(f"{field}: both values must be numeric, rust={rust_value!r} cpp={cpp_value!r}")
        return
    if not math.isfinite(rust_number) or not math.isfinite(cpp_number):
        failures.append(f"{field}: values must be finite, rust={rust_value!r} cpp={cpp_value!r}")
        return
    if abs(rust_number - cpp_number) > tolerance:
        failures.append(f"{field}: rust={rust_number} cpp={cpp_number} tolerance={tolerance}")


def compare_latency(field: str, rust_value: Any, cpp_value: Any, ratio: float, failures: list[str]) -> None:
    try:
        rust_number = float(rust_value)
        cpp_number = float(cpp_value)
    except (TypeError, ValueError):
        failures.append(f"{field}: both latency values must be numeric, rust={rust_value!r} cpp={cpp_value!r}")
        return
    if rust_number < 0 or cpp_number < 0:
        failures.append(f"{field}: latency values must be non-negative, rust={rust_number} cpp={cpp_number}")
        return
    smaller = max(min(rust_number, cpp_number), 1e-9)
    larger = max(rust_number, cpp_number)
    if larger / smaller > ratio:
        failures.append(f"{field}: rust={rust_number} cpp={cpp_number} ratio_limit={ratio}")


if __name__ == "__main__":
    raise SystemExit(main())
