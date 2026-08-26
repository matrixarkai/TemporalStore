#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Convert ExternalBaseline/ExternalBaseline CSV results into MatrixArk benchmark artifacts.

The ExternalBaseline benchmark scripts write CSV files, while MatrixArk summaries use
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
import os


WORD_RE = re.compile(r"[a-z0-9]+")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--csv", required=True, help="ExternalBaseline benchmark CSV output.")
    parser.add_argument("--dataset", choices=("locomo", "longmemeval_s"), required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--provider-name", default="external_baseline")
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
    parser.add_argument(
        "--source-json",
        default="",
        help="Optional source benchmark JSON used to compute full-source token denominators.",
    )
    parser.add_argument(
        "--import-error-log",
        default="",
        help="Optional ExternalBaseline import error log; timeout/auth blockers are copied into the report.",
    )
    args = parser.parse_args()

    started = time.time()
    rows = read_rows(Path(args.csv))
    case_count = len(rows)
    source_tokens_by_sample = load_source_tokens_by_sample(Path(args.source_json), args.dataset) if args.source_json else {}
    reader_hits = [reader_hit(row) for row in rows]
    judged_rows = sum(1 for row in rows if str(row.get("result") or "").strip())
    elapsed_ms = [float(row.get("time_cost") or 0.0) * 1000.0 for row in rows if row.get("time_cost")]
    prompt_tokens = sum_token_usage(rows, "prompt_tokens")
    completion_tokens = sum_token_usage(rows, "completion_tokens")
    total_tokens = sum_token_usage(rows, "total_tokens")
    memory_prompt_tokens = sum_numeric_column(rows, "memory_prompt_tokens")
    memory_chars = sum_numeric_column(rows, "memory_chars")
    retrieved_uri_counts = [retrieved_uri_count(row) for row in rows]
    answer_in_context = [context_contains_answer(row) for row in rows]
    source_tokens = sum(source_tokens_for_row(row, source_tokens_by_sample) for row in rows)
    archive_fallback_used = any("session_archive_fallback" in str(row.get("tools_used_names") or "") for row in rows)
    blockers = []
    if case_count == 0:
        blockers.append("empty_external_baseline_csv")
    if judged_rows < case_count:
        blockers.append("external_baseline_judge_results_missing")
    if total_tokens <= 0:
        blockers.append("external_baseline_token_usage_missing")
    if case_count and max(retrieved_uri_counts, default=0) <= 0:
        blockers.append("external_baseline_retrieved_uris_empty")
    if case_count and sum(1 for hit in answer_in_context if hit) < case_count:
        blockers.append("external_baseline_context_missing_expected_answer")
    if archive_fallback_used:
        blockers.append("external_baseline_session_archive_fallback_used")
    import_error_summary = summarize_import_errors(Path(args.import_error_log)) if args.import_error_log else {}
    blockers.extend(import_error_summary.get("blockers", []))
    if args.paper_min_cases and case_count < args.paper_min_cases:
        blockers.append(f"case_count_below_paper_min_{args.paper_min_cases}")

    report: dict[str, Any] = {
        "schema": "matrixark_external_baseline_context_benchmark_report_v1",
        "benchmark_family": "external_baseline_long_memory",
        "baseline": "external_baseline_csv",
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
        "external_baseline_judged_rows": judged_rows,
        "external_baseline_prompt_tokens": prompt_tokens,
        "external_baseline_completion_tokens": completion_tokens,
        "external_baseline_total_tokens": total_tokens,
        "external_baseline_source_tokens": source_tokens,
        "external_baseline_memory_prompt_tokens": memory_prompt_tokens,
        "external_baseline_memory_chars": memory_chars,
        "external_baseline_retrieved_uri_count_avg": safe_div(sum(retrieved_uri_counts), case_count),
        "external_baseline_retrieved_uri_count_max": max(retrieved_uri_counts, default=0),
        "benchmark_context_answer_coverage": safe_div(sum(1 for hit in answer_in_context if hit), case_count),
        "external_baseline_context_missing_expected_answer_count": sum(1 for hit in answer_in_context if not hit),
        "external_baseline_session_archive_fallback_used": archive_fallback_used,
        "external_baseline_import_error_summary": import_error_summary,
        "benchmark_model_contract": model_contract(args),
        "paper_comparable_claim_ready": False,
        "diagnostic_only": True,
        "claim_status": "diagnostic_not_paper_comparable",
        "blockers": blockers,
        "benchmark_per_query": [summarize_row(row, index) for index, row in enumerate(rows)],
        "elapsed_seconds": round(time.time() - started, 3),
    }
    if memory_prompt_tokens > 0:
        report["benchmark_avg_retrieved_tokens_per_query"] = safe_div(memory_prompt_tokens, case_count)
    elif total_tokens > 0:
        report["benchmark_avg_retrieved_tokens_per_query"] = safe_div(prompt_tokens, case_count)
    if source_tokens > 0:
        report["benchmark_avg_source_tokens_per_query"] = safe_div(source_tokens, case_count)
        report["benchmark_token_reduction_percent"] = token_reduction_percent(
            source_tokens,
            memory_prompt_tokens or prompt_tokens,
        )
    Path(args.output).write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(args.output)
    return 0 if case_count else 1


def summarize_import_errors(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {"path": str(path), "exists": False, "line_count": 0, "blockers": ["external_baseline_import_error_log_missing"]}
    lines = [line.strip() for line in path.read_text(encoding="utf-8", errors="replace").splitlines() if line.strip()]
    blockers: list[str] = []
    lower = "\n".join(lines).lower()
    if lines:
        blockers.append("external_baseline_import_errors_present")
    if "request timed out" in lower or "timeout" in lower:
        blockers.append("external_baseline_session_commit_timeout")
    if "permissiondenied" in lower or "root api keys cannot access" in lower:
        blockers.append("external_baseline_import_auth_failed")
    return {
        "path": str(path),
        "exists": True,
        "line_count": len(lines),
        "blockers": blockers,
        "sample": lines[:5],
    }


def read_rows(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def load_source_tokens_by_sample(path: Path, dataset: str) -> dict[str, int]:
    data = json.loads(path.read_text(encoding="utf-8"))
    records = data if isinstance(data, list) else data.get("data", data.get("records", []))
    if not isinstance(records, list):
        return {}
    result: dict[str, int] = {}
    for index, record in enumerate(records):
        if not isinstance(record, dict):
            continue
        sample_keys = {f"sample_{index}"}
        raw_id = record.get("sample_id") or record.get("conversation_id") or record.get("id")
        if raw_id:
            sample_keys.add(str(raw_id))
        tokens = source_token_count(record, dataset)
        for sample_key in sample_keys:
            result[sample_key] = tokens
    return result


def source_token_count(record: dict[str, Any], dataset: str) -> int:
    if dataset == "locomo":
        return sum(estimated_tokens(text) for text in locomo_conversation_texts(record))
    return estimated_tokens(json.dumps(record, ensure_ascii=False))


def locomo_conversation_texts(record: dict[str, Any]) -> list[str]:
    conversation = record.get("conversation")
    if not isinstance(conversation, dict):
        return []
    texts: list[str] = []
    for key in sorted(conversation, key=locomo_sort_key):
        if not key.startswith("session_") or key.endswith("_date_time"):
            continue
        turns = conversation.get(key)
        if not isinstance(turns, list):
            continue
        for turn in turns:
            if not isinstance(turn, dict):
                continue
            speaker = str(turn.get("speaker") or "").strip()
            text = str(turn.get("text") or "").strip()
            if text:
                texts.append(f"{speaker}: {text}" if speaker else text)
    return texts


def locomo_sort_key(key: str) -> tuple[int, str]:
    match = re.match(r"session_(\d+)$", key)
    return (int(match.group(1)) if match else 1_000_000, key)


def source_tokens_for_row(row: dict[str, str], source_tokens_by_sample: dict[str, int]) -> int:
    sample_id = str(row.get("sample_id") or "").strip()
    return int(source_tokens_by_sample.get(sample_id) or 0)


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


def context_contains_answer(row: dict[str, str]) -> bool:
    expected = str(row.get("answer") or "")
    context = str(row.get("model_input_prompt") or row.get("memory_prompt") or row.get("context") or "")
    expected_norm = normalize(expected)
    context_norm = normalize(context)
    if not expected_norm:
        return False
    if expected_norm in context_norm:
        return True
    if re.search(r"\d", expected_norm):
        return False
    return answer_matches(expected, context)


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


def sum_numeric_column(rows: list[dict[str, str]], key: str) -> int:
    total = 0
    for row in rows:
        try:
            total += int(float(row.get(key) or 0))
        except (TypeError, ValueError):
            continue
    return total


def retrieved_uri_count(row: dict[str, str]) -> int:
    raw = row.get("retrieved_uris_by_iteration") or row.get("retrieved_uris") or ""
    if not raw:
        return 0
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError:
        return 0
    return count_uris(parsed)


def count_uris(value: Any) -> int:
    if isinstance(value, str):
        scheme = os.environ.get("EXTERNAL_BASELINE_URI_SCHEME", "")
        return 1 if scheme and value.startswith(scheme + "://") else 0
    if isinstance(value, list):
        return sum(count_uris(item) for item in value)
    if isinstance(value, dict):
        total = 0
        for key in ("retrieved_uris", "context_uris"):
            items = value.get(key)
            if isinstance(items, list):
                total += sum(count_uris(item) for item in items)
        return total
    return 0


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
            "MatrixArk and ExternalBaseline/ExternalBaseline rows must use the same OSS reader model, "
            "embedding/encoding model, retrieval block budget, and reader context budget."
        ),
    }


def normalized_model_name(value: str) -> str:
    return re.sub(r"[^a-z0-9._:-]+", "", str(value).strip().lower())


def estimated_tokens(text: str) -> int:
    return max(1, int((len(str(text).split()) * 1.15) + 0.999999)) if str(text).strip() else 0


def token_reduction_percent(source_tokens: int, retrieved_tokens: int) -> float:
    if source_tokens <= 0:
        return 0.0
    return round(max(0.0, (source_tokens - retrieved_tokens) * 100.0 / source_tokens), 6)


def summarize_row(row: dict[str, str], index: int) -> dict[str, Any]:
    return {
        "query_index": index,
        "sample_id": row.get("sample_id", ""),
        "question_index": row.get("question_index", ""),
        "question": row.get("question", ""),
        "expected_answer": row.get("answer", ""),
        "reader_answer": row.get("response", ""),
        "reader_hit": reader_hit(row),
        "expected_answer_in_context": context_contains_answer(row),
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
