#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Summarize fair OSS long-memory benchmark comparisons."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--comparison", action="append", required=True)
    parser.add_argument("--matrixark-report", action="append", required=True)
    parser.add_argument("--baseline-report", action="append", required=True)
    parser.add_argument("--contract-validation", action="append", default=[])
    parser.add_argument("--output-json", required=True)
    parser.add_argument("--output-md", required=True)
    args = parser.parse_args()

    count = len(args.comparison)
    if len(args.matrixark_report) != count or len(args.baseline_report) != count:
        raise SystemExit("--comparison, --matrixark-report, and --baseline-report counts must match")

    rows = []
    validations = list(args.contract_validation)
    for index, name in enumerate(args.comparison):
        validation_path = validations[index] if index < len(validations) else ""
        rows.append(
            summarize_pair(
                name,
                Path(args.matrixark_report[index]),
                Path(args.baseline_report[index]),
                Path(validation_path) if validation_path else None,
            )
        )

    result = {
        "schema": "matrixark_oss_benchmark_comparison_summary_v1",
        "comparison_count": len(rows),
        "comparisons": rows,
        "overall": summarize_overall(rows),
    }
    output_json = Path(args.output_json)
    output_md = Path(args.output_md)
    output_json.parent.mkdir(parents=True, exist_ok=True)
    output_md.parent.mkdir(parents=True, exist_ok=True)
    output_json.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    output_md.write_text(render_markdown(result), encoding="utf-8")
    print(output_json)
    print(output_md)
    return 0


def summarize_pair(name: str, matrixark_path: Path, baseline_path: Path, validation_path: Path | None) -> dict[str, Any]:
    matrixark = load_json(matrixark_path)
    baseline = load_json(baseline_path)
    validation = load_json(validation_path) if validation_path else {}
    matrixark_metrics = metrics(matrixark)
    baseline_metrics = metrics(baseline)
    return {
        "name": name,
        "matrixark_report": str(matrixark_path),
        "baseline_report": str(baseline_path),
        "contract_validation": str(validation_path) if validation_path else "",
        "contract_passed": bool(validation.get("passed", False)),
        "contract_errors": validation.get("errors", []) if isinstance(validation.get("errors"), list) else [],
        "matrixark": matrixark_metrics,
        "baseline": baseline_metrics,
        "delta": deltas(matrixark_metrics, baseline_metrics),
        "status": comparison_status(matrixark_metrics, baseline_metrics, validation),
    }


def metrics(report: dict[str, Any]) -> dict[str, Any]:
    return {
        "case_count": first_int(report, "case_count", "benchmark_case_count"),
        "reader_hit_rate": first_number(report, "benchmark_reader_hit_rate", "reader_hit_rate"),
        "retrieval_hit_rate": first_number(report, "benchmark_hit_at_k", "retrieval_hit_rate", "benchmark_retrieval_hit_rate"),
        "token_reduction_percent": first_number(report, "benchmark_token_reduction_percent"),
        "total_retrieved_tokens": first_number(report, "benchmark_total_retrieved_tokens"),
        "total_source_tokens": first_number(report, "benchmark_total_source_tokens"),
        "avg_retrieved_tokens": first_number(report, "benchmark_avg_retrieved_tokens_per_query", "benchmark_avg_retrieved_tokens"),
        "retrieval_p95_ms": first_number(report, "benchmark_retrieval_p95_ms"),
        "reader_p95_ms": first_number(report, "benchmark_reader_p95_ms"),
        "reader_open_source_calls": first_int(report, "reader_open_source_calls"),
        "reader_error_count": first_int(report, "reader_error_count"),
        "reader_fallback_count": first_int(report, "reader_fallback_count"),
        "adaptive_effective_max_event_counts": report.get("adaptive_effective_max_event_counts") or {},
        "retrieval_budget_config": report.get("retrieval_budget_config") or {},
    }


def deltas(matrixark: dict[str, Any], baseline: dict[str, Any]) -> dict[str, Any]:
    return {
        "reader_hit_rate": diff(matrixark, baseline, "reader_hit_rate"),
        "retrieval_hit_rate": diff(matrixark, baseline, "retrieval_hit_rate"),
        "token_reduction_percent": diff(matrixark, baseline, "token_reduction_percent"),
        "retrieved_token_ratio": ratio(matrixark.get("total_retrieved_tokens"), baseline.get("total_retrieved_tokens")),
        "avg_retrieved_token_ratio": ratio(matrixark.get("avg_retrieved_tokens"), baseline.get("avg_retrieved_tokens")),
        "retrieval_p95_ms": diff(matrixark, baseline, "retrieval_p95_ms"),
        "reader_p95_ms": diff(matrixark, baseline, "reader_p95_ms"),
    }


def comparison_status(matrixark: dict[str, Any], baseline: dict[str, Any], validation: dict[str, Any]) -> str:
    if not validation.get("passed"):
        return "contract_not_comparable"
    if matrixark.get("reader_error_count") or matrixark.get("reader_fallback_count"):
        return "matrixark_reader_not_clean"
    if baseline.get("reader_error_count") or baseline.get("reader_fallback_count"):
        return "baseline_reader_not_clean"
    reader_delta = diff(matrixark, baseline, "reader_hit_rate")
    token_delta = diff(matrixark, baseline, "token_reduction_percent")
    if reader_delta is None or token_delta is None:
        return "missing_metrics"
    if reader_delta >= -0.02 and token_delta >= -1.0:
        return "competitive"
    if reader_delta >= -0.05:
        return "quality_close_token_tradeoff"
    return "quality_gap"


def summarize_overall(rows: list[dict[str, Any]]) -> dict[str, Any]:
    statuses: dict[str, int] = {}
    for row in rows:
        status = str(row.get("status") or "unknown")
        statuses[status] = statuses.get(status, 0) + 1
    return {
        "status_counts": statuses,
        "all_contracts_passed": all(bool(row.get("contract_passed")) for row in rows),
        "clean_readers": all(
            row["matrixark"].get("reader_error_count", 0) == 0
            and row["matrixark"].get("reader_fallback_count", 0) == 0
            and row["baseline"].get("reader_error_count", 0) == 0
            and row["baseline"].get("reader_fallback_count", 0) == 0
            for row in rows
        ),
    }


def render_markdown(result: dict[str, Any]) -> str:
    lines = [
        "# MatrixArk OSS Benchmark Comparison Summary",
        "",
        f"Comparisons: {result['comparison_count']}",
        f"All contracts passed: {yes_no(result['overall']['all_contracts_passed'])}",
        f"Clean OSS reader calls: {yes_no(result['overall']['clean_readers'])}",
        "",
        "| Benchmark | Status | Reader Hit M/B | Retrieval Hit M/B | Token Reduction M/B | Retrieved Token Ratio | p95 Retrieval M/B | p95 Reader M/B |",
        "|---|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for row in result["comparisons"]:
        matrixark = row["matrixark"]
        baseline = row["baseline"]
        delta = row["delta"]
        lines.append(
            "| {name} | {status} | {mh}/{bh} | {mr}/{br} | {mt}/{bt} | {ratio} | {mret}/{bret} | {mread}/{bread} |".format(
                name=row["name"],
                status=row["status"],
                mh=fmt_pct(matrixark.get("reader_hit_rate")),
                bh=fmt_pct(baseline.get("reader_hit_rate")),
                mr=fmt_pct(matrixark.get("retrieval_hit_rate")),
                br=fmt_pct(baseline.get("retrieval_hit_rate")),
                mt=fmt_pct(matrixark.get("token_reduction_percent")),
                bt=fmt_pct(baseline.get("token_reduction_percent")),
                ratio=fmt_num(delta.get("retrieved_token_ratio")),
                mret=fmt_ms(matrixark.get("retrieval_p95_ms")),
                bret=fmt_ms(baseline.get("retrieval_p95_ms")),
                mread=fmt_ms(matrixark.get("reader_p95_ms")),
                bread=fmt_ms(baseline.get("reader_p95_ms")),
            )
        )
    lines.extend(["", "## Notes", ""])
    for row in result["comparisons"]:
        lines.append(f"### {row['name']}")
        lines.append(f"- Contract passed: {yes_no(row['contract_passed'])}")
        if row["contract_errors"]:
            lines.append(f"- Contract errors: `{row['contract_errors']}`")
        lines.append(f"- MatrixArk report: `{row['matrixark_report']}`")
        lines.append(f"- Baseline report: `{row['baseline_report']}`")
        lines.append(f"- Retrieval budget: `{row['matrixark'].get('retrieval_budget_config')}`")
        lines.append(f"- Adaptive caps: `{row['matrixark'].get('adaptive_effective_max_event_counts')}`")
        lines.append("")
    return "\n".join(lines)


def load_json(path: Path | None) -> dict[str, Any]:
    if path is None:
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def first_number(report: dict[str, Any], *keys: str) -> float | None:
    for key in keys:
        value = report.get(key)
        if value is None:
            continue
        try:
            return float(value)
        except (TypeError, ValueError):
            continue
    return None


def first_int(report: dict[str, Any], *keys: str) -> int:
    value = first_number(report, *keys)
    return int(value) if value is not None else 0


def diff(left: dict[str, Any], right: dict[str, Any], key: str) -> float | None:
    if left.get(key) is None or right.get(key) is None:
        return None
    return float(left[key]) - float(right[key])


def ratio(left: Any, right: Any) -> float | None:
    try:
        denominator = float(right)
        if denominator == 0:
            return None
        return float(left) / denominator
    except (TypeError, ValueError):
        return None


def fmt_pct(value: Any) -> str:
    if value is None:
        return "n/a"
    value = float(value)
    if value <= 1.0:
        value *= 100.0
    return f"{value:.2f}%"


def fmt_ms(value: Any) -> str:
    if value is None:
        return "n/a"
    return f"{float(value):.1f} ms"


def fmt_num(value: Any) -> str:
    if value is None:
        return "n/a"
    return f"{float(value):.3f}"


def yes_no(value: Any) -> str:
    return "yes" if value else "no"


if __name__ == "__main__":
    raise SystemExit(main())
