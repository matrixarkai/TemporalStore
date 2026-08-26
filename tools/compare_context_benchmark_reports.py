#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Compare Rust and Context benchmark reports against the shared contract."""

from __future__ import annotations

import argparse
import json
import math
import sys
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CORPUS = ROOT / "compat" / "unified_temporalstore_cases.json"
BENCHMARK_SUITE = "native_context_benchmark_parity"
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
    "benchmark_avg_retrieved_source_groups_per_query",
    "benchmark_multi_source_group_query_rate",
    "benchmark_max_retrieved_tokens_per_query",
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
    "reader_answer",
    "expected_answer_terms",
    "expected_source_ref_ids",
    "retrieved_source_ids",
    "retrieved_source_group_ids",
    "retrieved_blocks",
    "retrieved_source_groups",
    "source_tokens",
    "retrieved_tokens",
)
PER_QUERY_NUMERIC_FIELDS = (
    "token_reduction_percent",
)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rust-report", type=Path, required=True)
    parser.add_argument("--native-report", type=Path, required=True)
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
    native = load_report(args.native_report)
    failures: list[str] = []

    validate_report("rust", rust, contract, failures)
    validate_report("native", native, contract, failures)
    if args.dataset:
        compare_equal("dataset", rust.get("dataset"), native.get("dataset"), failures)
        if rust.get("dataset") != args.dataset:
            failures.append(f"rust dataset {rust.get('dataset')!r} != expected {args.dataset!r}")
        if native.get("dataset") != args.dataset:
            failures.append(f"native dataset {native.get('dataset')!r} != expected {args.dataset!r}")

    summary_compare = compare_summary(rust, native, args.numeric_tolerance, args.latency_ratio_tolerance, failures)
    compare_thresholds(rust, native, args.numeric_tolerance, failures)
    category_compare = compare_category_breakdown(rust, native, args.numeric_tolerance, failures)
    per_query_compare = compare_per_query(rust, native, args.numeric_tolerance, failures)

    result = {
        "schema": "matrixark_external_baseline_context_benchmark_report_compare_v2",
        "ready": not failures,
        "rust_report": str(args.rust_report),
        "native_report": str(args.native_report),
        "case_name": contract["case_name"],
        "report_contract_format": contract["format"],
        "rust_case_count": rust.get("case_count"),
        "native_case_count": native.get("case_count"),
        "rust_per_query_count": len(rust.get("benchmark_per_query") or []),
        "native_per_query_count": len(native.get("benchmark_per_query") or []),
        "report_pair_summary": report_pair_summary(rust, native),
        "summary_compare": summary_compare,
        "category_compare": category_compare,
        "per_query_compare": per_query_compare,
        "latency_deltas": summary_compare["latency_deltas"],
        "token_reduction_delta": summary_compare["numeric_deltas"].get("benchmark_token_reduction_percent"),
        "category_deltas": category_compare["category_deltas"],
        "rust_only_miss_count": per_query_compare["retrieval_misses"]["rust_only_count"],
        "native_only_miss_count": per_query_compare["retrieval_misses"]["native_only_count"],
        "shared_hard_miss_count": per_query_compare["retrieval_misses"]["shared_hard_count"],
        "reader_rust_only_miss_count": per_query_compare["reader_misses"]["rust_only_count"],
        "reader_only_miss_count": per_query_compare["reader_misses"]["native_only_count"],
        "reader_shared_hard_miss_count": per_query_compare["reader_misses"]["shared_hard_count"],
        "misses_by_category": per_query_compare["misses_by_category"],
        "field_mismatches_by_query": per_query_compare["field_mismatches_by_query"],
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
    native: dict[str, Any],
    tolerance: float,
    latency_ratio_tolerance: float,
    failures: list[str],
) -> dict[str, Any]:
    numeric_deltas: dict[str, float | None] = {}
    latency_deltas: dict[str, dict[str, float | None]] = {}
    for field in (
        "schema",
        "benchmark_family",
        "reader_provider_name",
        "reader_model",
        "paper_comparable_claim_ready",
    ):
        compare_equal(field, rust.get(field), native.get(field), failures)
    for field in NUMERIC_SUMMARY_FIELDS:
        compare_number(field, rust.get(field), native.get(field), tolerance, failures)
        numeric_deltas[field] = numeric_delta(rust.get(field), native.get(field))
    for field in LATENCY_FIELDS:
        compare_latency(field, rust.get(field), native.get(field), latency_ratio_tolerance, failures)
        latency_deltas[field] = latency_delta(rust.get(field), native.get(field))
    compare_equal(
        "benchmark_threshold_violations",
        rust.get("benchmark_threshold_violations"),
        native.get("benchmark_threshold_violations"),
        failures,
    )
    return {
        "numeric_deltas": numeric_deltas,
        "latency_deltas": latency_deltas,
    }


def compare_thresholds(
    rust: dict[str, Any],
    native: dict[str, Any],
    tolerance: float,
    failures: list[str],
) -> None:
    rust_thresholds = rust.get("benchmark_thresholds")
    native_thresholds = native.get("benchmark_thresholds")
    if not isinstance(rust_thresholds, dict) or not isinstance(native_thresholds, dict):
        failures.append("benchmark_thresholds must be objects in both reports")
        return
    if set(rust_thresholds) != set(native_thresholds):
        failures.append(
            f"benchmark_thresholds keys differ: rust={sorted(rust_thresholds)} native={sorted(native_thresholds)}"
        )
        return
    for field in sorted(rust_thresholds):
        if isinstance(rust_thresholds[field], bool) or isinstance(native_thresholds[field], bool):
            compare_equal(f"benchmark_thresholds.{field}", rust_thresholds[field], native_thresholds[field], failures)
        else:
            compare_number(
                f"benchmark_thresholds.{field}",
                rust_thresholds[field],
                native_thresholds[field],
                tolerance,
                failures,
            )


def compare_category_breakdown(
    rust: dict[str, Any],
    native: dict[str, Any],
    tolerance: float,
    failures: list[str],
) -> dict[str, Any]:
    result: dict[str, Any] = {"category_deltas": {}, "missing_categories": []}
    rust_categories = rust.get("category_breakdown")
    native_categories = native.get("category_breakdown")
    if not isinstance(rust_categories, dict) or not isinstance(native_categories, dict):
        failures.append("category_breakdown must be objects in both reports")
        return result
    if set(rust_categories) != set(native_categories):
        failures.append(
            f"category_breakdown keys differ: rust={sorted(rust_categories)} native={sorted(native_categories)}"
        )
        result["missing_categories"] = sorted(set(rust_categories) ^ set(native_categories))
        return result
    for category in sorted(rust_categories):
        rust_row = rust_categories[category]
        native_row = native_categories[category]
        if not isinstance(rust_row, dict) or not isinstance(native_row, dict):
            failures.append(f"category_breakdown.{category} must be objects in both reports")
            continue
        result["category_deltas"][category] = {}
        for field in (
            "case_count",
            "hit_rate",
            "mean_reciprocal_rank",
            "answer_term_coverage",
            "zero_hit_queries",
            "reader_hit_rate",
            "reader_answer_coverage",
        ):
            compare_number(
                f"category_breakdown.{category}.{field}",
                rust_row.get(field),
                native_row.get(field),
                tolerance,
                failures,
            )
            result["category_deltas"][category][field] = numeric_delta(rust_row.get(field), native_row.get(field))
    compare_number("weak_category_count", rust.get("weak_category_count"), native.get("weak_category_count"), tolerance, failures)
    compare_equal("weak_categories", rust.get("weak_categories"), native.get("weak_categories"), failures)
    compare_equal("weak_category_policy", rust.get("weak_category_policy"), native.get("weak_category_policy"), failures)
    return result


def compare_per_query(
    rust: dict[str, Any],
    native: dict[str, Any],
    tolerance: float,
    failures: list[str],
) -> dict[str, Any]:
    rust_rows = rows_by_query_id("rust", rust.get("benchmark_per_query"), failures)
    native_rows = rows_by_query_id("native", native.get("benchmark_per_query"), failures)
    result: dict[str, Any] = {
        "common_query_count": 0,
        "missing_in_native": [],
        "missing_in_rust": [],
        "field_mismatch_count": 0,
        "field_mismatches": [],
        "selected_source_delta_count": 0,
        "selected_source_deltas": [],
        "latency_deltas": {},
        "token_reduction_deltas": {},
        "retrieval_misses": empty_miss_partition(),
        "reader_misses": empty_miss_partition(),
        "misses_by_category": {},
        "field_mismatches_by_query": {},
    }
    if not rust_rows or not native_rows:
        return result
    missing_in_native = sorted(set(rust_rows) - set(native_rows))
    missing_in_rust = sorted(set(native_rows) - set(rust_rows))
    result["missing_in_native"] = missing_in_native
    result["missing_in_rust"] = missing_in_rust
    if missing_in_native:
        failures.append(f"native missing query_ids: {missing_in_native[:20]}")
    if missing_in_rust:
        failures.append(f"rust missing query_ids: {missing_in_rust[:20]}")
    common_query_ids = sorted(set(rust_rows) & set(native_rows))
    result["common_query_count"] = len(common_query_ids)
    result["retrieval_misses"] = classify_misses(common_query_ids, rust_rows, native_rows, field="hit")
    result["reader_misses"] = classify_misses(common_query_ids, rust_rows, native_rows, field="reader_hit")
    result["misses_by_category"] = {
        "retrieval": misses_by_category(result["retrieval_misses"]),
        "reader": misses_by_category(result["reader_misses"]),
    }
    retrieval_latency_deltas = []
    reader_latency_deltas = []
    token_reduction_deltas = []
    for query_id in common_query_ids:
        rust_row = rust_rows[query_id]
        native_row = native_rows[query_id]
        for field in PER_QUERY_EXACT_FIELDS:
            compare_equal_tracking(
                f"query[{query_id}].{field}",
                rust_row.get(field),
                native_row.get(field),
                failures,
                result["field_mismatches"],
            )
        for field in PER_QUERY_NUMERIC_FIELDS:
            compare_number_tracking(
                f"query[{query_id}].{field}",
                rust_row.get(field),
                native_row.get(field),
                tolerance,
                failures,
                result["field_mismatches"],
            )
        if normalize_id_list(rust_row.get("retrieved_source_ids")) != normalize_id_list(native_row.get("retrieved_source_ids")):
            result["selected_source_deltas"].append(
                {
                    "query_id": query_id,
                    "rust_retrieved_source_ids": rust_row.get("retrieved_source_ids"),
                    "native_retrieved_source_ids": native_row.get("retrieved_source_ids"),
                }
            )
        retrieval_latency_deltas.append(abs_numeric_delta(rust_row.get("retrieval_ms"), native_row.get("retrieval_ms")))
        reader_latency_deltas.append(abs_numeric_delta(rust_row.get("reader_ms"), native_row.get("reader_ms")))
        token_reduction_deltas.append(abs_numeric_delta(rust_row.get("token_reduction_percent"), native_row.get("token_reduction_percent")))
    result["field_mismatch_count"] = len(result["field_mismatches"])
    result["field_mismatches_by_query"] = field_mismatches_by_query(result["field_mismatches"])
    result["selected_source_delta_count"] = len(result["selected_source_deltas"])
    result["selected_source_deltas"] = result["selected_source_deltas"][:50]
    result["latency_deltas"] = {
        "retrieval_ms_p50": percentile_present(retrieval_latency_deltas, 50),
        "retrieval_ms_p95": percentile_present(retrieval_latency_deltas, 95),
        "reader_ms_p50": percentile_present(reader_latency_deltas, 50),
        "reader_ms_p95": percentile_present(reader_latency_deltas, 95),
    }
    result["token_reduction_deltas"] = {
        "p50": percentile_present(token_reduction_deltas, 50),
        "p95": percentile_present(token_reduction_deltas, 95),
        "max": max((value for value in token_reduction_deltas if value is not None), default=None),
    }
    return result


def report_pair_summary(rust: dict[str, Any], native: dict[str, Any]) -> dict[str, Any]:
    return {
        "dataset": {"rust": rust.get("dataset"), "native": native.get("dataset")},
        "reader_model": {"rust": rust.get("reader_model"), "native": native.get("reader_model")},
        "reader_mode_effective": {
            "rust": rust.get("reader_mode_effective"),
            "native": native.get("reader_mode_effective"),
        },
        "hit_at_k": {"rust": rust.get("benchmark_hit_at_k"), "native": native.get("benchmark_hit_at_k")},
        "reader_hit_rate": {"rust": rust.get("reader_hit_rate"), "native": native.get("reader_hit_rate")},
        "token_reduction_percent": {
            "rust": rust.get("benchmark_token_reduction_percent"),
            "native": native.get("benchmark_token_reduction_percent"),
        },
        "rust_temporalstore_full_replay_ready": rust.get("rust_temporalstore_full_replay_ready"),
        "native_rust_temporalstore_full_replay_ready_field": native.get("rust_temporalstore_full_replay_ready"),
    }


def misses_by_category(partition: dict[str, Any]) -> dict[str, dict[str, int]]:
    out: dict[str, dict[str, int]] = {}
    for bucket in ("rust_only", "native_only", "shared_hard"):
        for row in partition.get(bucket, []):
            category = str(row.get("category") or "unknown")
            out.setdefault(category, {"rust_only": 0, "native_only": 0, "shared_hard": 0})
            out[category][bucket] += 1
    return out


def field_mismatches_by_query(mismatches: list[dict[str, Any]]) -> dict[str, list[str]]:
    out: dict[str, list[str]] = {}
    for mismatch in mismatches:
        field = str(mismatch.get("field") or "")
        if not field.startswith("query["):
            continue
        query_id = field.split("]", 1)[0][len("query[") :]
        leaf = field.split("].", 1)[1] if "]." in field else field
        out.setdefault(query_id, []).append(leaf)
    return dict(sorted(out.items()))


def empty_miss_partition() -> dict[str, Any]:
    return {
        "rust_only_count": 0,
        "native_only_count": 0,
        "shared_hard_count": 0,
        "rust_only": [],
        "native_only": [],
        "shared_hard": [],
    }


def classify_misses(
    query_ids: list[str],
    rust_rows: dict[str, dict[str, Any]],
    native_rows: dict[str, dict[str, Any]],
    *,
    field: str,
) -> dict[str, Any]:
    rust_only = []
    native_only = []
    shared_hard = []
    for query_id in query_ids:
        rust_hit = bool(rust_rows[query_id].get(field))
        native_hit = bool(native_rows[query_id].get(field))
        if not rust_hit and native_hit:
            rust_only.append(query_summary(query_id, rust_rows[query_id], native_rows[query_id], field))
        elif rust_hit and not native_hit:
            native_only.append(query_summary(query_id, rust_rows[query_id], native_rows[query_id], field))
        elif not rust_hit and not native_hit:
            shared_hard.append(query_summary(query_id, rust_rows[query_id], native_rows[query_id], field))
    return {
        "rust_only_count": len(rust_only),
        "native_only_count": len(native_only),
        "shared_hard_count": len(shared_hard),
        "rust_only": rust_only,
        "native_only": native_only,
        "shared_hard": shared_hard,
    }


def query_summary(
    query_id: str,
    rust_row: dict[str, Any],
    native_row: dict[str, Any],
    field: str,
) -> dict[str, Any]:
    return {
        "query_id": query_id,
        "category": rust_row.get("category") or native_row.get("category"),
        "field": field,
        "rust_hit": bool(rust_row.get(field)),
        "native_hit": bool(native_row.get(field)),
        "rust_rank": rust_row.get("rank"),
        "native_rank": native_row.get("rank"),
        "rust_retrieved_blocks": rust_row.get("retrieved_blocks"),
        "native_retrieved_blocks": native_row.get("retrieved_blocks"),
    }


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


def compare_equal(field: str, rust_value: Any, native_value: Any, failures: list[str]) -> None:
    if rust_value != native_value:
        failures.append(f"{field}: rust={rust_value!r} native={native_value!r}")


def compare_equal_tracking(
    field: str,
    rust_value: Any,
    native_value: Any,
    failures: list[str],
    mismatches: list[dict[str, Any]],
) -> None:
    before = len(failures)
    compare_equal(field, rust_value, native_value, failures)
    if len(failures) != before:
        mismatches.append({"field": field, "rust": rust_value, "native": native_value})


def compare_number(field: str, rust_value: Any, native_value: Any, tolerance: float, failures: list[str]) -> None:
    try:
        rust_number = float(rust_value)
        native_number = float(native_value)
    except (TypeError, ValueError):
        failures.append(f"{field}: both values must be numeric, rust={rust_value!r} native={native_value!r}")
        return
    if not math.isfinite(rust_number) or not math.isfinite(native_number):
        failures.append(f"{field}: values must be finite, rust={rust_value!r} native={native_value!r}")
        return
    if abs(rust_number - native_number) > tolerance:
        failures.append(f"{field}: rust={rust_number} native={native_number} tolerance={tolerance}")


def compare_number_tracking(
    field: str,
    rust_value: Any,
    native_value: Any,
    tolerance: float,
    failures: list[str],
    mismatches: list[dict[str, Any]],
) -> None:
    before = len(failures)
    compare_number(field, rust_value, native_value, tolerance, failures)
    if len(failures) != before:
        mismatches.append({"field": field, "rust": rust_value, "native": native_value})


def numeric_delta(rust_value: Any, native_value: Any) -> float | None:
    try:
        rust_number = float(rust_value)
        native_number = float(native_value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(rust_number) or not math.isfinite(native_number):
        return None
    return rust_number - native_number


def abs_numeric_delta(rust_value: Any, native_value: Any) -> float | None:
    delta = numeric_delta(rust_value, native_value)
    return abs(delta) if delta is not None else None


def latency_delta(rust_value: Any, native_value: Any) -> dict[str, float | None]:
    try:
        rust_number = float(rust_value)
        native_number = float(native_value)
    except (TypeError, ValueError):
        return {"absolute_ms": None, "ratio": None}
    if rust_number < 0 or native_number < 0:
        return {"absolute_ms": None, "ratio": None}
    smaller = max(min(rust_number, native_number), 1e-9)
    larger = max(rust_number, native_number)
    return {"absolute_ms": abs(rust_number - native_number), "ratio": larger / smaller}


def percentile_present(values: list[float | None], pct: float) -> float | None:
    present = sorted(value for value in values if value is not None)
    if not present:
        return None
    if len(present) == 1:
        return present[0]
    rank = (len(present) - 1) * pct / 100.0
    lower = math.floor(rank)
    upper = math.ceil(rank)
    if lower == upper:
        return present[int(rank)]
    return present[lower] + (present[upper] - present[lower]) * (rank - lower)


def normalize_id_list(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    return [str(item).strip().lower() for item in value if str(item).strip()]


def compare_latency(field: str, rust_value: Any, native_value: Any, ratio: float, failures: list[str]) -> None:
    try:
        rust_number = float(rust_value)
        native_number = float(native_value)
    except (TypeError, ValueError):
        failures.append(f"{field}: both latency values must be numeric, rust={rust_value!r} native={native_value!r}")
        return
    if rust_number < 0 or native_number < 0:
        failures.append(f"{field}: latency values must be non-negative, rust={rust_number} native={native_number}")
        return
    smaller = max(min(rust_number, native_number), 1e-9)
    larger = max(rust_number, native_number)
    if larger / smaller > ratio:
        failures.append(f"{field}: rust={rust_number} native={native_number} ratio_limit={ratio}")


if __name__ == "__main__":
    raise SystemExit(main())
