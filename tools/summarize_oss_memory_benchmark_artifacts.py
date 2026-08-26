#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Summarize OSS memory benchmark artifacts across MatrixArk/ExternalBaseline-style runs.

The goal is not to bless a run as paper-comparable. It produces a compact,
auditable table for retrieval quality, reader quality, token savings, latency,
and claim status across heterogeneous JSON reports.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any


SUMMARY_FIELDS = (
    "case_count",
    "retrieval_hit_at_k",
    "context_answer_coverage",
    "reader_hit_rate",
    "token_reduction_percent",
    "retrieval_p95_ms",
    "reader_p95_ms",
    "avg_source_tokens",
    "avg_retrieved_tokens",
)

PAPER_COMPARABLE_MIN_CASES = {
    "locomo": 1542,
    "longmemeval_s": 500,
}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--report", action="append", required=True, help="JSON report path. May be repeated.")
    parser.add_argument("--label", action="append", default=[], help="Optional label for the matching --report.")
    parser.add_argument("--output-json", default="/tmp/oss_memory_benchmark_summary.json")
    parser.add_argument("--output-md", default="/tmp/oss_memory_benchmark_summary.md")
    parser.add_argument("--locomo-paper-min-cases", type=int, default=PAPER_COMPARABLE_MIN_CASES["locomo"])
    parser.add_argument("--longmemeval-paper-min-cases", type=int, default=PAPER_COMPARABLE_MIN_CASES["longmemeval_s"])
    args = parser.parse_args()

    paper_min_cases = {
        "locomo": args.locomo_paper_min_cases,
        "longmemeval_s": args.longmemeval_paper_min_cases,
    }
    labels = list(args.label)
    rows = []
    for index, report_path in enumerate(args.report):
        path = Path(report_path)
        label = labels[index] if index < len(labels) else path.stem
        rows.append(summarize_report(path, label, paper_min_cases))

    result = {
        "schema": "matrixark_oss_memory_benchmark_artifact_summary_v1",
        "report_count": len(rows),
        "rows": rows,
        "paper_comparable_ready_count": sum(1 for row in rows if row["paper_comparable_ready"]),
        "non_paper_comparable_count": sum(1 for row in rows if not row["paper_comparable_ready"]),
    }
    Path(args.output_json).write_text(json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    Path(args.output_md).write_text(render_markdown(result), encoding="utf-8")
    print(args.output_json)
    print(args.output_md)
    return 0


def summarize_report(path: Path, label: str, paper_min_cases: dict[str, int] | None = None) -> dict[str, Any]:
    paper_min_cases = paper_min_cases or PAPER_COMPARABLE_MIN_CASES
    data = json.loads(path.read_text(encoding="utf-8"))
    metrics_source = data.get("harness") if isinstance(data.get("harness"), dict) else data
    batch_metrics = extract_batch_replay_metrics(data)
    if batch_metrics and not number(metrics_source, "case_count", "benchmark_per_query_count", "external_benchmark_case_count"):
        metrics_source = batch_metrics
    quality_gate = data.get("quality_gate") if isinstance(data.get("quality_gate"), dict) else {}
    if data is not metrics_source and not quality_gate:
        quality_gate = metrics_source.get("quality_gate") if isinstance(metrics_source.get("quality_gate"), dict) else {}
    model_contract = extract_model_contract(data, metrics_source, quality_gate)
    dataset = data.get("dataset") or data.get("benchmark_dataset") or infer_dataset(path)
    dataset = dataset or metrics_source.get("dataset") or metrics_source.get("external_benchmark_dataset")
    benchmark_family = data.get("benchmark_family") or metrics_source.get("benchmark_family") or ""
    case_count = number(metrics_source, "case_count", "benchmark_per_query_count", "external_benchmark_case_count")
    source_claim_ready = bool(
        data.get("paper_comparable_claim_ready")
        or metrics_source.get("paper_comparable_claim_ready")
        or metrics_source.get("external_benchmark_ready")
        or quality_gate.get("paper_comparable_claim_ready")
    )
    shared_contract_required = requires_shared_oss_model_contract(data, metrics_source, quality_gate)
    shared_contract_passed = bool(model_contract.get("shared_oss_model_contract_passed"))
    if shared_contract_required and not shared_contract_passed:
        source_claim_ready = False
    retrieval_ready_reader_not_run = bool(
        batch_metrics
        and data.get("batch_replay_used")
        and data.get("returncode") == 0
        and batch_metrics.get("external_benchmark_case_count")
        and batch_metrics.get("external_benchmark_hit_at_k", 0.0) >= 0.90
        and not number(metrics_source, "benchmark_reader_hit_rate", "reader_hit_rate")
    )
    min_cases = paper_min_cases.get(str(dataset), 1)
    scale_ready = isinstance(case_count, (int, float)) and case_count >= min_cases
    row = {
        "label": label,
        "path": str(path),
        "dataset": dataset,
        "reader_model": data.get("reader_model") or data.get("model") or "",
        "reader_provider": data.get("reader_provider_name") or data.get("reader_base_url") or "",
        "paper_comparable_ready": bool(source_claim_ready and scale_ready),
        "blocker": data.get("benchmark_readiness_blocker")
        or quality_gate.get("benchmark_readiness_blocker")
        or ";".join(data.get("blockers") or []),
    }
    threshold_violations = extract_threshold_violations(data, metrics_source, quality_gate)
    if threshold_violations:
        row["blocker"] = ";".join(filter(None, [row["blocker"], *threshold_violations]))
    if source_claim_ready and not scale_ready:
        row["blocker"] = ";".join(filter(None, [row["blocker"], f"case_count_below_paper_min_{min_cases}"]))
    if shared_contract_required and not shared_contract_passed:
        row["blocker"] = ";".join(filter(None, [row["blocker"], "shared_oss_model_contract_not_satisfied"]))
    raw_claim = data.get("claim_status") or data.get("claim_level") or ""
    if not raw_claim and (data.get("rust_temporalstore_full_replay_ready") or retrieval_ready_reader_not_run) and not number(metrics_source, "benchmark_reader_hit_rate", "reader_hit_rate"):
        raw_claim = "retrieval_ready_reader_not_run"
    row["diagnostic_only"] = bool(
        data.get("diagnostic_only")
        or data.get("python_only_diagnostic")
        or str(raw_claim).startswith("diagnostic")
        or not row["paper_comparable_ready"]
    )
    row["claim_status"] = raw_claim or ("paper_comparable" if row["paper_comparable_ready"] else "not_paper_comparable")
    row.update({field: value for field, value in extract_metrics(metrics_source).items() if field in SUMMARY_FIELDS})
    row.update(extract_model_contract_summary(model_contract, data, metrics_source))
    row["benchmark_family"] = benchmark_family
    row["ready"] = bool(data.get("ready") or data.get("benchmark_quality_ready") or quality_gate.get("quality_ready"))
    return row


def extract_threshold_violations(
    data: dict[str, Any],
    metrics_source: dict[str, Any],
    quality_gate: dict[str, Any],
) -> list[str]:
    for source in (data, metrics_source, quality_gate):
        violations = source.get("benchmark_threshold_violations") if isinstance(source, dict) else None
        if isinstance(violations, list):
            return [str(item) for item in violations if str(item)]
    return []


def extract_model_contract(
    data: dict[str, Any],
    metrics_source: dict[str, Any],
    quality_gate: dict[str, Any],
) -> dict[str, Any]:
    for source in (data, metrics_source, quality_gate):
        contract = source.get("benchmark_model_contract") if isinstance(source, dict) else None
        if isinstance(contract, dict):
            return contract
    return {}


def requires_shared_oss_model_contract(
    data: dict[str, Any],
    metrics_source: dict[str, Any],
    quality_gate: dict[str, Any],
) -> bool:
    contract = extract_model_contract(data, metrics_source, quality_gate)
    if contract.get("shared_oss_model_contract_required"):
        return True
    family = str(data.get("benchmark_family") or metrics_source.get("benchmark_family") or "").lower()
    schema = str(data.get("schema") or metrics_source.get("schema") or "").lower()
    claim = str(data.get("claim_status") or data.get("claim_level") or "").lower()
    return bool(
        # One token now, where there were two names. It has to stay distinctive enough to match
        # as a substring without matching anything else -- which is why this is not just
        # "baseline".
        "external_baseline" in family
        or "external_baseline" in schema
        or "paper_comparable" in claim
        or quality_gate.get("paper_comparable_claim_ready")
        or data.get("paper_comparable_claim_ready")
        or metrics_source.get("paper_comparable_claim_ready")
    )


def extract_model_contract_summary(
    contract: dict[str, Any],
    data: dict[str, Any],
    metrics_source: dict[str, Any],
) -> dict[str, Any]:
    return {
        "shared_oss_model_contract_passed": bool(contract.get("shared_oss_model_contract_passed")),
        "shared_oss_model_contract_required": bool(contract.get("shared_oss_model_contract_required")),
        "matrixark_reader_model": (
            contract.get("matrixark_reader_model")
            or data.get("reader_model")
            or metrics_source.get("reader_model")
            or data.get("model")
            or ""
        ),
        "matrixark_embedding_model": contract.get("matrixark_embedding_model") or data.get("embedding_model") or "",
        "matrixark_max_events": contract.get("matrixark_max_events"),
        "matrixark_reader_max_context_chars": contract.get("matrixark_reader_max_context_chars"),
        "baseline_provider_name": contract.get("baseline_provider_name") or "",
        "baseline_reader_model": contract.get("baseline_reader_model") or "",
        "baseline_embedding_model": contract.get("baseline_embedding_model") or "",
        "baseline_max_events": contract.get("baseline_max_events"),
        "baseline_reader_max_context_chars": contract.get("baseline_reader_max_context_chars"),
        "reader_model_match": bool(contract.get("reader_model_match")),
        "embedding_model_match": bool(contract.get("embedding_model_match")),
        "max_events_match": bool(contract.get("max_events_match")),
        "reader_context_budget_match": bool(contract.get("reader_context_budget_match")),
    }



def extract_batch_replay_metrics(data: dict[str, Any]) -> dict[str, Any]:
    batch_reports = data.get("batch_reports")
    if not isinstance(batch_reports, list) or not batch_reports:
        return {}
    case_count = 0
    hit_count = 0
    elapsed_ms: list[float] = []
    failed_batches = 0
    for row in batch_reports:
        if not isinstance(row, dict):
            continue
        count = int(row.get("case_count") or 0)
        hit_rate = float(row.get("hit_at_k") or 0.0)
        case_count += count
        hit_count += round(hit_rate * count)
        if row.get("returncode") not in (0, None):
            failed_batches += 1
        value = row.get("elapsed_ms")
        if isinstance(value, (int, float)):
            elapsed_ms.append(float(value))
    hit_at_k = hit_count / case_count if case_count else 0.0
    return {
        "case_count": case_count,
        "external_benchmark_case_count": case_count,
        "external_benchmark_hit_at_k": hit_at_k,
        "benchmark_hit_at_k": hit_at_k,
        "retrieval_p95_ms": percentile(elapsed_ms, 95),
        "benchmark_retrieval_p95_ms": percentile(elapsed_ms, 95),
        "failed_batch_count": failed_batches,
        "external_benchmark_ready": bool(case_count and failed_batches == 0 and hit_count == case_count),
    }


def percentile(values: list[float], pct: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    index = int((len(ordered) - 1) * pct / 100.0)
    return round(ordered[index], 4)

def extract_metrics(data: dict[str, Any]) -> dict[str, Any]:
    return {
        "case_count": number(data, "case_count", "benchmark_per_query_count", "external_benchmark_case_count"),
        "retrieval_hit_at_k": number(data, "benchmark_hit_at_k", "hit_rate", "benchmark_recall_at_k", "external_benchmark_hit_at_k"),
        "context_answer_coverage": number(data, "benchmark_context_answer_coverage"),
        "reader_hit_rate": number(data, "benchmark_reader_hit_rate", "reader_hit_rate", "deterministic_reader_hit_rate"),
        "token_reduction_percent": number(data, "benchmark_token_reduction_percent"),
        "retrieval_p95_ms": number(data, "benchmark_retrieval_p95_ms", "retrieval_p95_ms"),
        "reader_p95_ms": number(data, "benchmark_reader_p95_ms"),
        "avg_source_tokens": number(data, "benchmark_avg_source_tokens_per_query"),
        "avg_retrieved_tokens": number(data, "benchmark_avg_retrieved_tokens_per_query"),
    }


def number(data: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        value = data.get(key)
        if isinstance(value, (int, float)):
            return round(float(value), 4)
    return None


def infer_dataset(path: Path) -> str:
    lowered = path.name.lower()
    if "longmem" in lowered:
        return "longmemeval_s"
    if "locomo" in lowered:
        return "locomo"
    return ""


def render_markdown(result: dict[str, Any]) -> str:
    lines = [
        "# OSS Memory Benchmark Artifact Summary",
        "",
        f"Reports: {result['report_count']}",
        f"Paper-comparable ready: {result['paper_comparable_ready_count']}",
        f"Non-paper-comparable: {result['non_paper_comparable_count']}",
        "",
        "| Label | Dataset | Cases | Retrieval Hit@K | Context Answer Coverage | Reader Hit | Token Reduction | Reader | Baseline Reader | Embedding | Budget | Shared OSS Contract | Claim | Blocker |",
        "|---|---:|---:|---:|---:|---:|---:|---|---|---|---|---:|---|---|",
    ]
    for row in result["rows"]:
        budget = format_budget(row)
        embedding = format_pair(row.get("matrixark_embedding_model"), row.get("baseline_embedding_model"))
        lines.append(
            "| {label} | {dataset} | {case_count} | {retrieval_hit_at_k} | {context_answer_coverage} | {reader_hit_rate} | "
            "{token_reduction_percent} | {matrixark_reader_model} | {baseline_reader_model} | "
            "{embedding} | {budget} | {shared_oss_model_contract_passed} | {claim_status} | {blocker} |".format(
                **{key: md(row.get(key)) for key in (
                    "label",
                    "dataset",
                    "case_count",
                    "retrieval_hit_at_k",
                    "context_answer_coverage",
                    "reader_hit_rate",
                    "token_reduction_percent",
                    "matrixark_reader_model",
                    "baseline_reader_model",
                    "shared_oss_model_contract_passed",
                    "claim_status",
                    "blocker",
                )},
                embedding=md(embedding),
                budget=md(budget),
            )
        )
    lines.append("")
    lines.append(
        "All values are copied from source artifacts; this summary does not upgrade diagnostic runs "
        "into paper-comparable evidence. ExternalBaseline/ExternalBaseline comparisons require the shared OSS "
        "model contract to pass: same reader, embedding/encoding model, retrieval budget, and reader budget."
    )
    return "\n".join(lines) + "\n"


def format_pair(left: Any, right: Any) -> str:
    left_text = "" if left is None else str(left)
    right_text = "" if right is None else str(right)
    if not left_text and not right_text:
        return ""
    if left_text == right_text or not right_text:
        return left_text
    return f"{left_text} / {right_text}"


def format_budget(row: dict[str, Any]) -> str:
    events = format_pair(row.get("matrixark_max_events"), row.get("baseline_max_events"))
    chars = format_pair(row.get("matrixark_reader_max_context_chars"), row.get("baseline_reader_max_context_chars"))
    parts = []
    if events:
        parts.append(f"events={events}")
    if chars:
        parts.append(f"chars={chars}")
    return ", ".join(parts)


def md(value: Any) -> str:
    if value is None:
        return ""
    return str(value).replace("|", "\\|").replace("\n", " ")


if __name__ == "__main__":
    raise SystemExit(main())
