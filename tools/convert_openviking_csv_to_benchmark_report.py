#!/usr/bin/env python3
"""Convert OpenViking/VikingMem CSV results into MatrixArk benchmark artifacts.

The OpenViking benchmark scripts write CSV files, while MatrixArk summaries use
JSON artifacts with explicit model and budget contracts. This converter keeps the
baseline honest: missing judge, token, or full-scale evidence is preserved as a
blocker instead of being silently upgraded into a comparable claim.
"""

from __future__ import annotations

import argparse
import csv
import json
import re
import time
from pathlib import Path
from typing import Any


WORD_RE = re.compile(r"[a-z0-9]+")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--csv", required=True, help="OpenViking benchmark CSV output.")
    parser.add_argument("--dataset", choices=("locomo", "longmemeval_s"), required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--provider-name", default="openviking")
    parser.add_argument("--reader-model", required=True)
    parser.add_argument("--embedding-model", required=True)
    parser.add_argument("--max-events", type=int, required=True)
    parser.add_argument("--reader-max-context-chars", type=int, required=True)
    parser.add_argument("--matrixark-provider-name", default="matrixark")
    parser.add_argument("--matrixark-reader-model", required=True)
    parser.add_argument("--matrixark-embedding-model", required=True)
    parser.add_argument("--matrixark-max-events", type=int, required=True)
    parser.add_argument("--matrixark-reader-max-context-chars", type=int, required=True)
    parser.add_argument("--paper-min-cases", type=int, default=0)
    args = parser.parse_args()

    started = time.time()
    rows = read_rows(Path(args.csv))
    case_count = len(rows)
    reader_hits = [reader_hit(row) for row in rows]
    judged_rows = sum(1 for row in rows if str(row.get("result") or "").strip())
    elapsed_ms = [float(row.get("time_cost") or 0.0) * 1000.0 for row in rows if row.get("time_cost")]
    prompt_tokens = sum_token_usage(rows, "prompt_tokens")
    completion_tokens = sum_token_usage(rows, "completion_tokens")
    total_tokens = sum_token_usage(rows, "total_tokens")
    blockers = []
    if case_count == 0:
        blockers.append("empty_openviking_csv")
    if judged_rows < case_count:
        blockers.append("openviking_judge_results_missing")
    if total_tokens <= 0:
        blockers.append("openviking_token_usage_missing")
    if args.paper_min_cases and case_count < args.paper_min_cases:
        blockers.append(f"case_count_below_paper_min_{args.paper_min_cases}")

    report: dict[str, Any] = {
        "schema": "matrixark_vikingmem_context_benchmark_report_v1",
        "benchmark_family": "vikingmem_long_memory",
        "baseline": "openviking_csv",
        "dataset": args.dataset,
        "input_csv": args.csv,
        "case_count": case_count,
        "reader_provider_name": args.provider_name,
        "reader_model": args.reader_model,
        "embedding_model": args.embedding_model,
        "benchmark_reader_hit_rate": safe_div(sum(1 for hit in reader_hits if hit), case_count),
        "reader_hit_rate": safe_div(sum(1 for hit in reader_hits if hit), case_count),
        "benchmark_reader_p50_ms": percentile(elapsed_ms, 50),
        "benchmark_reader_p95_ms": percentile(elapsed_ms, 95),
        "openviking_judged_rows": judged_rows,
        "openviking_prompt_tokens": prompt_tokens,
        "openviking_completion_tokens": completion_tokens,
        "openviking_total_tokens": total_tokens,
        "benchmark_model_contract": model_contract(args),
        "paper_comparable_claim_ready": False,
        "diagnostic_only": True,
        "claim_status": "diagnostic_not_paper_comparable",
        "blockers": blockers,
        "benchmark_per_query": [summarize_row(row, index) for index, row in enumerate(rows)],
        "elapsed_seconds": round(time.time() - started, 3),
    }
    if total_tokens > 0:
        report["benchmark_avg_retrieved_tokens_per_query"] = safe_div(prompt_tokens, case_count)
    Path(args.output).write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(args.output)
    return 0 if case_count else 1


def read_rows(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def reader_hit(row: dict[str, str]) -> bool:
    result = str(row.get("result") or "").strip().lower()
    if result in {"true", "1", "yes", "correct", "pass", "passed"}:
        return True
    if result in {"false", "0", "no", "incorrect", "fail", "failed"}:
        return False
    return answer_matches(str(row.get("answer") or ""), str(row.get("response") or ""))


def answer_matches(expected: str, actual: str) -> bool:
    expected_norm = normalize(expected)
    actual_norm = normalize(actual)
    if not expected_norm:
        return False
    if expected_norm in actual_norm:
        return True
    expected_terms = set(WORD_RE.findall(expected_norm))
    actual_terms = set(WORD_RE.findall(actual_norm))
    if not expected_terms:
        return False
    return len(expected_terms & actual_terms) / len(expected_terms) >= 0.8


def normalize(value: str) -> str:
    return re.sub(r"\s+", " ", str(value).strip().lower())


def sum_token_usage(rows: list[dict[str, str]], key: str) -> int:
    total = 0
    for row in rows:
        raw = row.get("token_usage")
        if not raw:
            continue
        try:
            usage = json.loads(raw)
        except json.JSONDecodeError:
            continue
        try:
            total += int(float(usage.get(key) or 0))
        except (TypeError, ValueError):
            continue
    return total


def model_contract(args: argparse.Namespace) -> dict[str, Any]:
    reader_matches = normalized_model_name(args.matrixark_reader_model) == normalized_model_name(args.reader_model)
    embedding_matches = normalized_model_name(args.matrixark_embedding_model) == normalized_model_name(
        args.embedding_model
    )
    max_events_matches = args.matrixark_max_events == args.max_events
    reader_budget_matches = args.matrixark_reader_max_context_chars == args.reader_max_context_chars
    return {
        "matrixark_provider_name": args.matrixark_provider_name,
        "matrixark_reader_model": args.matrixark_reader_model,
        "matrixark_embedding_model": args.matrixark_embedding_model,
        "matrixark_max_events": args.matrixark_max_events,
        "matrixark_reader_max_context_chars": args.matrixark_reader_max_context_chars,
        "baseline_provider_name": args.provider_name,
        "baseline_reader_model": args.reader_model,
        "baseline_embedding_model": args.embedding_model,
        "baseline_max_events": args.max_events,
        "baseline_reader_max_context_chars": args.reader_max_context_chars,
        "provider_identity_declared": bool(args.matrixark_provider_name and args.provider_name),
        "reader_model_match": reader_matches,
        "embedding_model_match": embedding_matches,
        "max_events_match": max_events_matches,
        "reader_context_budget_match": reader_budget_matches,
        "shared_oss_model_contract_required": True,
        "shared_oss_model_contract_passed": (
            bool(args.matrixark_provider_name and args.provider_name)
            and reader_matches
            and embedding_matches
            and max_events_matches
            and reader_budget_matches
        ),
        "comparison_rule": (
            "MatrixArk and OpenViking/VikingMem rows must use the same OSS reader model, "
            "embedding/encoding model, retrieval block budget, and reader context budget."
        ),
    }


def normalized_model_name(value: str) -> str:
    return re.sub(r"[^a-z0-9._:-]+", "", str(value).strip().lower())


def summarize_row(row: dict[str, str], index: int) -> dict[str, Any]:
    return {
        "query_index": index,
        "sample_id": row.get("sample_id", ""),
        "question_index": row.get("question_index", ""),
        "question": row.get("question", ""),
        "expected_answer": row.get("answer", ""),
        "reader_answer": row.get("response", ""),
        "reader_hit": reader_hit(row),
        "result": row.get("result", ""),
        "time_cost_seconds": to_float(row.get("time_cost")),
        "evidence": row.get("evidence", ""),
        "tools_used_names": row.get("tools_used_names", ""),
    }


def to_float(value: str | None) -> float | None:
    try:
        return float(value) if value not in (None, "") else None
    except ValueError:
        return None


def percentile(values: list[float], pct: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    index = int((len(ordered) - 1) * pct / 100.0)
    return round(ordered[index], 4)


def safe_div(numerator: float, denominator: float) -> float:
    return round(float(numerator) / float(denominator), 6) if denominator else 0.0


if __name__ == "__main__":
    raise SystemExit(main())
