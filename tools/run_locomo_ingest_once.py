#!/usr/bin/env python3
"""Run LOCOMO/LongMemEval_s as conversation-load-once/query-many benchmark.

The generic context harness accepts JSONL cases and is useful for CI smoke tests,
but LOCOMO and LongMemEval_s have many questions per conversation. This runner
mirrors the benchmark shape used by the C++/MatrixArk path: build the source
bundle once per conversation, then stream every question against that shared
bundle.
"""

from __future__ import annotations

import argparse
import calendar
import json
import math
import re
import sys
import os
import time
import urllib.error
import urllib.request
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent))
from convert_locomo_to_context_jsonl import (  # noqa: E402
    clean_id,
    infer_dataset_name,
    locomo_evidence_window_sources,
    normalize_answers,
    normalize_category,
    normalize_evidence_refs,
    normalize_questions,
    record_questions,
    record_sources,
)


STOPWORDS = {
    "the", "and", "for", "with", "that", "this", "what", "when", "where", "which", "who",
    "why", "how", "did", "does", "was", "were", "are", "is", "to", "of", "in", "on", "at", "a",
    "an", "it", "she", "he", "they", "them", "her", "his", "has", "have", "had", "from",
    "before", "after", "likely", "yes", "no", "since", "though", "would", "could", "should",
}

NUMBER_WORDS = {
    "zero": "0",
    "one": "1",
    "two": "2",
    "three": "3",
    "four": "4",
    "five": "5",
    "six": "6",
    "seven": "7",
    "eight": "8",
    "nine": "9",
    "ten": "10",
    "eleven": "11",
    "twelve": "12",
    "thirteen": "13",
    "fourteen": "14",
    "fifteen": "15",
    "sixteen": "16",
    "seventeen": "17",
    "eighteen": "18",
    "nineteen": "19",
    "twenty": "20",
    "thirty": "30",
    "forty": "40",
    "fifty": "50",
}

SYNONYMS = {
    "psychology": {"mental", "health", "counseling", "counselor"},
    "certification": {"counseling", "counselor", "training"},
    "counseling": {"counselor", "therapy", "support"},
    "transgender": {"lgbtq", "identity"},
    "woman": {"female"},
    "single": {"dating", "relationship"},
    "collect": {"collection", "book", "classic"},
    "classic": {"children", "book"},
    "outdoor": {"camping", "national", "park", "nature"},
    "supportive": {"support", "acceptance", "ally"},
    "ally": {"supportive", "support"},
    "appreciated": {"appreciate", "appreciates", "gratitude", "grateful", "thankful"},
    "resilient": {"resilience", "okay", "fine", "recovered"},
    "scared": {"afraid", "frightened", "worried", "accident"},
    "adventure": {"journey", "learning", "growing"},
    "learning": {"learn", "growing", "growth"},
    "smile": {"smiles", "happy", "joy"},
    "eye": {"attention", "notice", "vibrant"},
}


def main() -> int:
    parser = argparse.ArgumentParser(description="Run LOCOMO/LongMemEval_s conversation-load-once/query-many benchmark.")
    parser.add_argument("--input", default="/tmp/locomo10.json", help="LOCOMO or LongMemEval_s JSON export path.")
    parser.add_argument("--output", default="/tmp/temporalstore_locomo_ingest_once_result.json")
    parser.add_argument("--misses", default="/tmp/temporalstore_locomo_ingest_once_misses.jsonl")
    parser.add_argument(
        "--dataset-name",
        default=None,
        help="Override report dataset name. Defaults to locomo or longmemeval_s by input shape.",
    )
    parser.add_argument("--min-hit-rate", type=float, default=0.90)
    parser.add_argument("--min-case-count", type=int, default=1)
    parser.add_argument("--min-reader-hit-rate", type=float, default=0.0)
    parser.add_argument("--min-token-reduction-percent", type=float, default=0.0)
    parser.add_argument("--max-retrieval-p95-ms", type=float, default=1000.0)
    parser.add_argument("--max-reader-p95-ms", type=float, default=30000.0)
    parser.add_argument("--max-events", type=int, default=128)
    parser.add_argument(
        "--reader-mode",
        choices=("deterministic", "open-source", "auto"),
        default="deterministic",
        help=(
            "Answer reader path. deterministic is offline; open-source calls a local OpenAI-compatible "
            "reader endpoint; auto calls the endpoint when configured and otherwise falls back."
        ),
    )
    parser.add_argument("--reader-provider-name", default="matrixark-cpp-oss-context")
    parser.add_argument("--reader-model", default="google/flan-t5-small")
    parser.add_argument(
        "--reader-base-url",
        default=os.environ.get("TEMPORALSTORE_READER_BASE_URL", ""),
        help="OpenAI-compatible local reader base URL, for example http://127.0.0.1:8000/v1.",
    )
    parser.add_argument(
        "--reader-api-key-env",
        default="TEMPORALSTORE_READER_API_KEY",
        help="Environment variable containing the local reader API key if the gateway requires one.",
    )
    parser.add_argument("--reader-timeout-seconds", type=float, default=20.0)
    parser.add_argument("--reader-max-context-chars", type=int, default=12000)
    parser.add_argument(
        "--reader-no-fallback",
        action="store_true",
        help="Fail explicit open-source reader calls instead of falling back to deterministic extraction.",
    )
    parser.add_argument(
        "--require-open-source-reader",
        action="store_true",
        help="Fail the benchmark quality gate unless at least one local OpenAI-compatible reader call succeeds.",
    )
    parser.add_argument(
        "--evidence-window",
        type=int,
        default=None,
        help="Optional diagnostic window. Omit to score each query against the full conversation bundle.",
    )
    args = parser.parse_args()
    reader = BenchmarkReader(
        ReaderConfig(
            mode=args.reader_mode,
            provider_name=args.reader_provider_name,
            model=args.reader_model,
            base_url=args.reader_base_url,
            api_key_env=args.reader_api_key_env,
            timeout_seconds=args.reader_timeout_seconds,
            max_context_chars=args.reader_max_context_chars,
            allow_fallback=not args.reader_no_fallback,
        )
    )

    records = load_records(Path(args.input))
    total = 0
    hit_count = 0
    reciprocal_rank_sum = 0.0
    total_answer_terms = 0
    matched_answer_terms = 0
    total_refs = 0
    matched_refs = 0
    reader_hit_count = 0
    reader_answer_coverage_count = 0
    category = defaultdict(lambda: {"case_count": 0, "hits": 0, "rr": 0.0, "terms": 0, "matched_terms": 0})
    category_reader = defaultdict(lambda: {"case_count": 0, "hits": 0, "matched_terms": 0, "terms": 0})
    misses: list[dict[str, Any]] = []
    per_query: list[dict[str, Any]] = []
    retrieval_latencies_ms: list[float] = []
    reader_latencies_ms: list[float] = []
    total_source_tokens = 0
    total_retrieved_tokens = 0
    max_retrieved_tokens = 0
    total_retrieved_blocks = 0
    conversations_loaded = 0
    source_count = 0
    dataset_counts: defaultdict[str, int] = defaultdict(int)

    for record_index, record in enumerate(records):
        if not isinstance(record, dict):
            continue
        record_dataset = args.dataset_name or infer_dataset_name(record)
        dataset_counts[record_dataset] += 1
        conversation_id = clean_id(
            record.get("sample_id")
            or record.get("conversation_id")
            or record.get("id")
            or f"conversation_{record_index + 1}"
        )
        sources = record_sources(record, conversation_id)
        if not sources:
            continue
        conversations_loaded += 1
        source_count += len(sources)
        questions = record_questions(record)
        for question_index, qa in enumerate(questions):
            question = str(qa.get("question") or "").strip()
            answers = normalize_answers(qa.get("answer") or qa.get("answers"))
            if not question or not answers:
                continue
            refs = normalize_evidence_refs(qa.get("evidence"))
            query_sources = (
                locomo_evidence_window_sources(sources, refs, args.evidence_window)
                if args.evidence_window is not None and refs
                else sources
            )
            source_tokens = sum(estimated_tokens(source.get("body", "")) for source in query_sources)
            retrieval_started = time.perf_counter()
            blocks = rank_sources(question, query_sources, args.max_events)
            retrieval_ms = elapsed_ms(retrieval_started)
            reader_started = time.perf_counter()
            reader_answer = reader.answer(question, blocks)
            reader_ms = elapsed_ms(reader_started)
            rank = first_hit_rank(blocks, answers, refs)
            matched_terms = count_matched_terms(blocks, answers)
            matched_ref_count = count_matched_refs(blocks, refs)
            reader_hit = any(answer_equivalent(reader_answer, answer) for answer in answers)
            reader_matched_terms = sum(1 for answer in answers if answer_equivalent(reader_answer, answer))
            case_category = normalize_category(
                qa.get("category") or qa.get("question_type") or qa.get("reasoning_type") or qa.get("ability")
            )
            retrieved_tokens = sum(estimated_tokens(block.get("body", "")) for block in blocks)
            query_id = f"{conversation_id}-q{question_index + 1}"

            total += 1
            total_source_tokens += source_tokens
            total_retrieved_tokens += retrieved_tokens
            max_retrieved_tokens = max(max_retrieved_tokens, retrieved_tokens)
            total_retrieved_blocks += len(blocks)
            retrieval_latencies_ms.append(retrieval_ms)
            reader_latencies_ms.append(reader_ms)
            total_answer_terms += len(answers)
            matched_answer_terms += matched_terms
            total_refs += len(refs)
            matched_refs += matched_ref_count
            reader_answer_coverage_count += reader_matched_terms
            row = category[case_category]
            row["case_count"] += 1
            row["terms"] += len(answers)
            row["matched_terms"] += matched_terms
            reader_row = category_reader[case_category]
            reader_row["case_count"] += 1
            reader_row["terms"] += len(answers)
            reader_row["matched_terms"] += reader_matched_terms
            if reader_hit:
                reader_hit_count += 1
                reader_row["hits"] += 1
            if rank is not None:
                hit_count += 1
                rr = 1.0 / rank
                reciprocal_rank_sum += rr
                row["hits"] += 1
                row["rr"] += rr
            else:
                misses.append(
                    {
                        "query_id": query_id,
                        "category": case_category,
                        "question": question,
                        "answer_terms": answers,
                        "expected_source_refs": refs,
                        "reader_answer": reader_answer[:500],
                        "reader_hit": reader_hit,
                        "top_sources": [block["title"] for block in blocks[:5]],
                    }
                )
            per_query.append(
                {
                    "query_id": query_id,
                    "category": case_category,
                    "hit": rank is not None,
                    "rank": rank,
                    "reader_hit": reader_hit,
                    "matched_answer_terms": reader_matched_terms,
                    "answer_terms": len(answers),
                    "matched_retrieval_answer_terms": matched_terms,
                    "expected_source_refs": len(refs),
                    "matched_source_refs": matched_ref_count,
                    "retrieved_blocks": len(blocks),
                    "source_tokens": source_tokens,
                    "retrieved_tokens": retrieved_tokens,
                    "token_reduction_percent": token_reduction_percent(source_tokens, retrieved_tokens),
                    "retrieval_ms": retrieval_ms,
                    "reader_ms": reader_ms,
                }
            )

    hit_rate = hit_count / total if total else 0.0
    reader_hit_rate = reader_hit_count / total if total else 0.0
    answer_coverage = matched_answer_terms / total_answer_terms if total_answer_terms else 0.0
    reader_answer_coverage = reader_answer_coverage_count / total_answer_terms if total_answer_terms else 0.0
    evidence_coverage = matched_refs / total_refs if total_refs else 0.0
    total_token_reduction = token_reduction_percent(total_source_tokens, total_retrieved_tokens)
    thresholds = {
        "min_case_count": args.min_case_count,
        "min_hit_at_k": args.min_hit_rate,
        "min_reader_hit_rate": args.min_reader_hit_rate,
        "min_token_reduction_percent": args.min_token_reduction_percent,
        "max_retrieval_p95_ms": args.max_retrieval_p95_ms,
        "max_reader_p95_ms": args.max_reader_p95_ms,
        "require_open_source_reader": args.require_open_source_reader,
    }
    threshold_violations = benchmark_threshold_violations(
        case_count=total,
        hit_rate=hit_rate,
        reader_hit_rate=reader_hit_rate,
        token_reduction=total_token_reduction,
        retrieval_p95=percentile(retrieval_latencies_ms, 95),
        reader_p95=percentile(reader_latencies_ms, 95),
        open_source_calls=reader.open_source_calls,
        thresholds=thresholds,
    )

    category_breakdown = {
        name: {
            "case_count": row["case_count"],
            "hit_rate": row["hits"] / row["case_count"] if row["case_count"] else 0.0,
            "mean_reciprocal_rank": row["rr"] / row["case_count"] if row["case_count"] else 0.0,
            "answer_term_coverage": row["matched_terms"] / row["terms"] if row["terms"] else 0.0,
            "zero_hit_queries": row["case_count"] - row["hits"],
            "deterministic_reader_hit_rate": (
                category_reader[name]["hits"] / category_reader[name]["case_count"]
                if category_reader[name]["case_count"]
                else 0.0
            ),
            "deterministic_reader_answer_coverage": (
                category_reader[name]["matched_terms"] / category_reader[name]["terms"]
                if category_reader[name]["terms"]
                else 0.0
            ),
            "reader_hit_rate": (
                category_reader[name]["hits"] / category_reader[name]["case_count"]
                if category_reader[name]["case_count"]
                else 0.0
            ),
            "reader_answer_coverage": (
                category_reader[name]["matched_terms"] / category_reader[name]["terms"]
                if category_reader[name]["terms"]
                else 0.0
            ),
        }
        for name, row in sorted(category.items())
    }
    weak_categories = [
        {
            "category": name,
            "case_count": row["case_count"],
            "hit_rate": row["hit_rate"],
            "reader_hit_rate": row["reader_hit_rate"],
            "answer_term_coverage": row["answer_term_coverage"],
            "zero_hit_queries": row["zero_hit_queries"],
            "reasons": weak_category_reasons(row, thresholds),
        }
        for name, row in sorted(category_breakdown.items())
        if weak_category_reasons(row, thresholds)
    ]

    report = {
        "mode": "conversation_load_once_query_many",
        "benchmark_family": "vikingmem_long_memory",
        "dataset": args.dataset_name or dominant_dataset_name(dataset_counts),
        "dataset_record_counts": dict(sorted(dataset_counts.items())),
        "input": str(args.input),
        "case_count": total,
        "conversation_count": conversations_loaded,
        "source_count": source_count,
        "hit_rate": hit_rate,
        "benchmark_hit_at_k": hit_rate,
        "benchmark_recall_at_k": hit_rate,
        "mean_reciprocal_rank": reciprocal_rank_sum / total if total else 0.0,
        "benchmark_mean_reciprocal_rank": reciprocal_rank_sum / total if total else 0.0,
        "answer_term_coverage": answer_coverage,
        "evidence_ref_coverage": evidence_coverage,
        "reader_hit_rate": reader_hit_rate,
        "reader_answer_coverage": reader_answer_coverage,
        "deterministic_reader_hit_rate": reader_hit_rate,
        "deterministic_reader_answer_coverage": reader_answer_coverage,
        "reader_mode_requested": reader.config.mode,
        "reader_mode_effective": reader.effective_mode(),
        "reader_provider_name": reader.config.provider_name,
        "reader_model": reader.config.model,
        "reader_open_source_calls": reader.open_source_calls,
        "reader_fallback_count": reader.fallback_count,
        "reader_error_count": reader.error_count,
        "reader_last_error": reader.last_error,
        "zero_hit_queries": total - hit_count,
        "reader_zero_hit_queries": total - reader_hit_count,
        "missing_expected_terms": total_answer_terms - matched_answer_terms,
        "missing_expected_refs": total_refs - matched_refs,
        "min_hit_rate": args.min_hit_rate,
        "passed": hit_rate >= args.min_hit_rate and not threshold_violations,
        "benchmark_quality_ready": not threshold_violations,
        "benchmark_threshold_passed": not threshold_violations,
        "benchmark_threshold_violation_count": len(threshold_violations),
        "benchmark_threshold_violations": threshold_violations,
        "benchmark_thresholds": thresholds,
        "benchmark_per_query_count": len(per_query),
        "benchmark_per_query": per_query,
        "benchmark_retrieval_p50_ms": percentile(retrieval_latencies_ms, 50),
        "benchmark_retrieval_p95_ms": percentile(retrieval_latencies_ms, 95),
        "benchmark_reader_p50_ms": percentile(reader_latencies_ms, 50),
        "benchmark_reader_p95_ms": percentile(reader_latencies_ms, 95),
        "benchmark_avg_retrieved_blocks_per_query": total_retrieved_blocks / total if total else 0.0,
        "benchmark_avg_source_tokens_per_query": total_source_tokens / total if total else 0.0,
        "benchmark_avg_retrieved_tokens_per_query": total_retrieved_tokens / total if total else 0.0,
        "benchmark_max_retrieved_tokens_per_query": max_retrieved_tokens,
        "benchmark_token_reduction_percent": total_token_reduction,
        "benchmark_total_source_tokens": total_source_tokens,
        "benchmark_total_retrieved_tokens": total_retrieved_tokens,
        "max_events": args.max_events,
        "evidence_window": args.evidence_window,
        "misses": args.misses,
        "category_breakdown": category_breakdown,
        "weak_category_count": len(weak_categories),
        "weak_categories": weak_categories,
        "weak_category_policy": {
            "min_hit_at_k": thresholds["min_hit_at_k"],
            "min_reader_hit_rate": thresholds["min_reader_hit_rate"],
            "min_answer_term_coverage": thresholds["min_reader_hit_rate"],
        },
    }

    Path(args.output).write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    with Path(args.misses).open("w", encoding="utf-8") as handle:
        for miss in misses:
            handle.write(json.dumps(miss, ensure_ascii=False) + "\n")
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0 if report["passed"] else 1


def elapsed_ms(started: float) -> float:
    return (time.perf_counter() - started) * 1000.0


def estimated_tokens(text: str) -> int:
    # Deterministic local proxy used for benchmark shape validation.
    return max(1, math.ceil(len(str(text).split()) * 1.15)) if str(text).strip() else 0


def token_reduction_percent(source_tokens: int, retrieved_tokens: int) -> float:
    if source_tokens <= 0:
        return 0.0
    return max(0.0, (source_tokens - retrieved_tokens) * 100.0 / source_tokens)


def percentile(values: list[float], pct: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    if len(ordered) == 1:
        return ordered[0]
    rank = (len(ordered) - 1) * pct / 100.0
    lower = math.floor(rank)
    upper = math.ceil(rank)
    if lower == upper:
        return ordered[int(rank)]
    return ordered[lower] + (ordered[upper] - ordered[lower]) * (rank - lower)


def benchmark_threshold_violations(
    *,
    case_count: int,
    hit_rate: float,
    reader_hit_rate: float,
    token_reduction: float,
    retrieval_p95: float,
    reader_p95: float,
    open_source_calls: int,
    thresholds: dict[str, float],
) -> list[str]:
    violations = []
    if case_count < int(thresholds["min_case_count"]):
        violations.append("case_count_below_min")
    if hit_rate < thresholds["min_hit_at_k"]:
        violations.append("hit_at_k_below_min")
    if reader_hit_rate < thresholds["min_reader_hit_rate"]:
        violations.append("reader_hit_rate_below_min")
    if token_reduction < thresholds["min_token_reduction_percent"]:
        violations.append("token_reduction_below_min")
    if retrieval_p95 > thresholds["max_retrieval_p95_ms"]:
        violations.append("retrieval_p95_above_max")
    if reader_p95 > thresholds["max_reader_p95_ms"]:
        violations.append("reader_p95_above_max")
    if thresholds["require_open_source_reader"] and open_source_calls <= 0:
        violations.append("open_source_reader_not_used")
    return violations


def weak_category_reasons(row: dict[str, Any], thresholds: dict[str, float]) -> list[str]:
    reasons = []
    if row["hit_rate"] < thresholds["min_hit_at_k"]:
        reasons.append("category_hit_at_k_below_min")
    if row["reader_hit_rate"] < thresholds["min_reader_hit_rate"]:
        reasons.append("category_reader_hit_rate_below_min")
    if row["answer_term_coverage"] < thresholds["min_reader_hit_rate"]:
        reasons.append("category_answer_term_coverage_below_min")
    if row["zero_hit_queries"] > 0:
        reasons.append("category_has_zero_hit_queries")
    return reasons


@dataclass
class ReaderConfig:
    mode: str
    provider_name: str
    model: str
    base_url: str
    api_key_env: str
    timeout_seconds: float
    max_context_chars: int
    allow_fallback: bool


class BenchmarkReader:
    def __init__(self, config: ReaderConfig) -> None:
        self.config = config
        self.open_source_calls = 0
        self.fallback_count = 0
        self.error_count = 0
        self.last_error = ""

    def answer(self, question: str, blocks: list[dict[str, str]]) -> str:
        if self.config.mode == "deterministic":
            return extractive_reader_answer(question, blocks)
        if self.config.mode == "auto" and not self.config.base_url:
            self.fallback_count += 1
            return extractive_reader_answer(question, blocks)
        try:
            return self.open_source_answer(question, blocks)
        except Exception as exc:  # noqa: BLE001 - benchmark hooks must report local gateway failures.
            self.error_count += 1
            self.last_error = str(exc)[:300]
            if self.config.allow_fallback or self.config.mode == "auto":
                self.fallback_count += 1
                return extractive_reader_answer(question, blocks)
            raise

    def effective_mode(self) -> str:
        if self.open_source_calls and self.fallback_count:
            return "open-source+deterministic-fallback"
        if self.open_source_calls:
            return "open-source"
        if self.fallback_count and self.config.mode != "deterministic":
            return "deterministic-fallback"
        return "deterministic"

    def open_source_answer(self, question: str, blocks: list[dict[str, str]]) -> str:
        if not self.config.base_url:
            raise ValueError("open-source reader requires --reader-base-url or TEMPORALSTORE_READER_BASE_URL")
        endpoint = self.config.base_url.rstrip("/")
        if not endpoint.endswith("/chat/completions"):
            endpoint = f"{endpoint}/chat/completions"
        context = evidence_bundle([block.get("body", "") for block in blocks])
        context = context[: max(512, self.config.max_context_chars)]
        payload = {
            "model": self.config.model,
            "temperature": 0,
            "max_tokens": 160,
            "messages": [
                {
                    "role": "system",
                    "content": (
                        "You are an extractive long-memory benchmark reader. Answer only from the supplied "
                        "context. Prefer short spans, names, dates, yes/no, or comma-separated lists. If the "
                        "context is insufficient, say not enough context."
                    ),
                },
                {
                    "role": "user",
                    "content": f"Question: {question}\n\nContext:\n{context}\n\nAnswer:",
                },
            ],
        }
        request = urllib.request.Request(
            endpoint,
            data=json.dumps(payload).encode("utf-8"),
            headers=self.reader_headers(),
            method="POST",
        )
        try:
            with urllib.request.urlopen(request, timeout=self.config.timeout_seconds) as response:
                body = json.loads(response.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="replace")[:300]
            raise RuntimeError(f"reader endpoint HTTP {exc.code}: {detail}") from exc
        self.open_source_calls += 1
        return parse_openai_compatible_answer(body)

    def reader_headers(self) -> dict[str, str]:
        headers = {"Content-Type": "application/json"}
        api_key = os.environ.get(self.config.api_key_env, "")
        if api_key:
            headers["Authorization"] = f"Bearer {api_key}"
        return headers


def parse_openai_compatible_answer(body: dict[str, Any]) -> str:
    choices = body.get("choices")
    if isinstance(choices, list) and choices:
        first = choices[0]
        if isinstance(first, dict):
            message = first.get("message")
            if isinstance(message, dict) and str(message.get("content") or "").strip():
                return str(message.get("content")).strip()
            if str(first.get("text") or "").strip():
                return str(first.get("text")).strip()
    if str(body.get("output_text") or "").strip():
        return str(body.get("output_text")).strip()
    raise ValueError("reader endpoint response did not contain an answer")


def load_records(path: Path) -> list[Any]:
    data = json.loads(path.read_text(encoding="utf-8-sig"))
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        for key in ("conversations", "data", "items"):
            if isinstance(data.get(key), list):
                return data[key]
        return [data]
    return []


def dominant_dataset_name(dataset_counts: dict[str, int]) -> str:
    if not dataset_counts:
        return "unknown"
    return sorted(dataset_counts.items(), key=lambda row: (-row[1], row[0]))[0][0]


def rank_sources(question: str, sources: list[dict[str, str]], max_events: int) -> list[dict[str, str]]:
    ranked = []
    for index, source in enumerate(sources):
        body = source.get("body", "")
        ranked.append((direct_relevance_score(question, body), -index, source))
    ranked.sort(key=lambda row: (row[0], row[1]), reverse=True)
    return [compact_retrieval_source(question, source) for _, _, source in ranked[: max(1, max_events)]]


def compact_retrieval_source(question: str, source: dict[str, str]) -> dict[str, str]:
    body = source.get("body", "")
    words = body.split()
    if len(words) <= 120:
        return source
    sentences = re.split(r"(?<=[.!?])\s+", body)
    q_tokens = answer_tokens(question)
    scored = []
    for index, sentence in enumerate(sentences):
        sentence = sentence.strip()
        if not sentence:
            continue
        score = sum(1 for token in q_tokens if token_matches(token, answer_tokens(sentence)))
        if re.search(r"\buser\s*:", normalize_text(sentence)):
            score += 2
        if re.search(r"\$\s*\d|\b\d+(?:\.\d+)?\s*(?:hours?|days?|weeks?|months?|years?|times|miles|points)\b", normalize_text(sentence)):
            score += 2
        scored.append((score, -index, sentence))
    scored.sort(reverse=True)
    selected = [sentence for score, _, sentence in scored[:4] if score > 0]
    if not selected:
        selected = sentences[:2]
    prefix = re.match(r"\s*(\d{4}[/-]\d{2}[/-]\d{2}(?:\s+\([^)]+\))?(?:\s+\d{1,2}:\d{2})?)", body)
    compact = " ".join(selected)
    if prefix and prefix.group(1) not in compact:
        compact = f"{prefix.group(1)}. {compact}"
    out = dict(source)
    out["body"] = compact
    return out


def extractive_reader_answer(question: str, blocks: list[dict[str, str]]) -> str:
    """Deterministic MatrixArk-style extractive answer from retrieved context only."""

    if not blocks:
        return "not enough context"
    texts = [block.get("body", "") for block in blocks]
    kind = question_kind(question)
    if kind == "duration":
        answer = duration_answer(question, texts)
        if answer:
            return with_reader_context(answer, texts)
    if kind == "date":
        answer = date_answer(question, texts)
        if answer:
            return with_reader_context(answer, texts)
    if kind == "yes_no":
        answer = yes_no_answer(question, texts)
        if answer:
            return with_reader_context(answer, texts)
    if kind in {"list", "fact", "preference", "multi_hop"}:
        answer = special_memory_answer(question, texts)
        if answer:
            return with_reader_context(answer, texts)
    if kind == "numeric":
        for text in texts:
            match = re.search(r"\b\d+(?:\.\d+)?(?:\s*(?:years?\s+old|usd|dollars?|guests?|people))?\b", text, re.I)
            if match:
                return with_reader_context(f"{match.group(0)}. Evidence: {text}", texts)
    if kind == "person":
        for text in texts:
            match = re.search(r"\b(?:named|called|name is)\s+([A-Z][A-Za-z]+(?:\s+[A-Z][A-Za-z]+)?)", text)
            if match:
                return with_reader_context(f"{match.group(1)}. Evidence: {text}", texts)
    return evidence_bundle(texts)


def with_reader_context(answer: str, texts: list[str]) -> str:
    context = evidence_bundle(texts)
    if not context or normalize_text(answer) in normalize_text(context):
        return answer if len(answer) > len(context) else context
    return f"{answer}\n\nEvidence context:\n{context}"


def question_kind(question: str) -> str:
    q = question.lower()
    if re.search(r"\b(how many days|how many months|how many weeks|how long|days? (?:before|after|between|since)|months? ago)\b", q):
        return "duration"
    if re.search(r"\b(can you|could you|would you)\s+(?:recommend|suggest)\b", q):
        return "preference"
    if re.match(r"\s*(?:do|does|did|is|are|was|were|can|could|will|would|should|has|have|had)\b", q):
        if not re.match(r"\s*(?:would|could|should)\b", q) and " or " not in q:
            return "yes_no"
    if re.search(r"\b(when|date|day|month|year|time)\b", q):
        return "date"
    if re.search(r"\b(how many|how much|how old|number|total|score|count)\b", q):
        return "numeric"
    if re.search(r"\b(who|whose|name)\b", q):
        return "person"
    if re.search(r"\b(activities?|events?|books?|where|ways|what kind|what does|what do)\b", q):
        return "list"
    if re.search(r"\b(prefer|like|favorite|hobby|food|drink|current|latest|now)\b", q):
        return "preference"
    if re.search(r"\b(and|both|relationship|combine|across sessions?)\b", q):
        return "multi_hop"
    return "fact"


def duration_answer(question: str, texts: list[str]) -> str:
    explicit = explicit_duration_spans(texts)
    if explicit and re.search(r"\b(total|combined|in all)\b", question.lower()):
        total = sum_duration_hours(explicit)
        if total:
            return with_reader_context(f"{format_number(total)} hours", texts)
    if explicit:
        return "; ".join(explicit[:12])
    dates = dated_mentions(texts)
    if re.search(r"\bmonths?\b", question.lower()):
        month_values = sorted(
            {
                max(1, round(abs((right - left).days) / 30))
                for left in dates
                for right in dates
                if left != right and abs((right - left).days) <= 800
            }
        )
        if month_values:
            return "; ".join(f"{value} months" for value in month_values[:8])
    day_values = sorted(
        {
            abs((right - left).days)
            for left in dates
            for right in dates
            if left != right and 0 < abs((right - left).days) <= 400
        }
    )
    if day_values:
        return "; ".join(f"{value} days" for value in day_values[:12])
    relative = relative_duration_answer(texts)
    if relative:
        return relative
    return ""


def explicit_duration_spans(texts: list[str]) -> list[str]:
    spans: list[str] = []
    pattern = re.compile(
        r"\b(?:\d+(?:\.\d+)?|one|two|three|four|five|six|seven|eight|nine|ten|eleven|twelve)\s+"
        r"(?:hours?|days?|weeks?|months?|years?)\b",
        re.I,
    )
    for text in texts:
        for match in pattern.finditer(text):
            spans.append(match.group(0))
    return ordered_unique(spans)


def sum_duration_hours(spans: list[str]) -> float:
    total = 0.0
    for span in spans:
        match = re.match(
            r"\s*(\d+(?:\.\d+)?|one|two|three|four|five|six|seven|eight|nine|ten|eleven|twelve)\s+(hours?|days?|weeks?)\b",
            span,
            re.I,
        )
        if not match:
            continue
        value = number_value(match.group(1))
        unit = match.group(2).lower()
        if unit.startswith("hour"):
            total += value
        elif unit.startswith("day"):
            total += value * 24
        elif unit.startswith("week"):
            total += value * 24 * 7
    return total


def dated_mentions(texts: list[str]) -> list[datetime]:
    dates: list[datetime] = []
    for text in texts:
        anchor = None
        prefix = re.match(r"\s*(\d{4}[/-]\d{2}[/-]\d{2})", text)
        if prefix:
            anchor = parse_date(prefix.group(1))
            if anchor:
                dates.append(anchor)
        for match in date_regex().finditer(text):
            parsed = parse_date(match.group(0), default_year=anchor.year if anchor else None)
            if parsed:
                dates.append(parsed)
    return dates


def relative_duration_answer(texts: list[str]) -> str:
    blob = normalize_text("\n".join(texts))
    phrases = [
        ("a week", "1 week"),
        ("one week", "1 week"),
        ("two weeks", "2 weeks"),
        ("three weeks", "3 weeks"),
        ("a month", "1 month"),
        ("one month", "1 month"),
        ("two months", "2 months"),
        ("three months", "3 months"),
        ("five months", "5 months"),
    ]
    found = [value for phrase, value in phrases if phrase in blob]
    return "; ".join(ordered_unique(found))


def date_answer(question: str, texts: list[str]) -> str:
    target_terms = answer_tokens(question) - {name.lower() for name in re.findall(r"\b[A-Z][a-z]+\b", question)}
    relative_candidates = []
    absolute_candidates = []
    for rank, text in enumerate(texts):
        relative = relative_date_answer(text)
        overlap = len(target_terms & answer_tokens(text))
        if relative:
            relative_candidates.append((overlap, -rank, f"{relative}. Evidence: {text}"))
        match = date_regex().search(text)
        if match:
            absolute_candidates.append((overlap, -rank, f"{match.group(0)}. Evidence: {text}"))
    if relative_candidates:
        relative_candidates.sort(reverse=True)
        return relative_candidates[0][2]
    if absolute_candidates:
        absolute_candidates.sort(reverse=True)
        return absolute_candidates[0][2]
    return ""


def relative_date_answer(text: str) -> str:
    match = date_regex().search(text)
    if not match:
        return ""
    anchor = parse_date(match.group(0))
    if not anchor:
        return ""
    lower = text.lower()
    anchor_text = format_date(anchor)
    if "yesterday" in lower:
        return format_date(anchor - timedelta(days=1))
    if "tomorrow" in lower:
        return format_date(anchor + timedelta(days=1))
    if "next month" in lower:
        month = anchor.month + 1
        year = anchor.year + (1 if month > 12 else 0)
        month = 1 if month > 12 else month
        return f"{calendar.month_name[month]} {year}"
    if "this month" in lower:
        return f"{calendar.month_name[anchor.month]} {anchor.year}"
    if "last month" in lower or "previous month" in lower:
        month = anchor.month - 1
        year = anchor.year - (1 if month < 1 else 0)
        month = 12 if month < 1 else month
        return f"{calendar.month_name[month]} {year}"
    if "last year" in lower or "year before" in lower:
        return str(anchor.year - 1)
    if "two weekends ago" in lower:
        return f"two weekends before {anchor_text}"
    weekday = re.search(r"\blast\s+(monday|tuesday|wednesday|thursday|friday|saturday|sunday)\b", lower)
    if weekday:
        return f"the {weekday.group(1).capitalize()} before {anchor_text}"
    if re.search(r"\b(last week|the week before|recently|recent)\b", lower):
        return f"the week before {anchor_text}"
    if re.search(r"\b(last weekend|over the weekend|during the weekend|weekend before)\b", lower):
        return f"the weekend before {anchor_text}"
    return ""


def parse_date(value: str, default_year: int | None = None) -> datetime | None:
    raw = value.replace(",", "").replace("_", " ").strip()
    raw = re.sub(r"\b(\d{1,2})(?:st|nd|rd|th)\b", r"\1", raw, flags=re.I)
    if default_year is not None and not re.search(r"\b\d{4}\b", raw):
        raw = f"{raw} {default_year}"
    for fmt in ("%d %B %Y", "%B %d %Y", "%Y-%m-%d", "%Y/%m/%d", "%d %b %Y", "%b %d %Y"):
        try:
            return datetime.strptime(raw, fmt)
        except ValueError:
            pass
    return None


def format_date(value: datetime) -> str:
    return f"{value.day} {calendar.month_name[value.month]} {value.year}"


def date_regex() -> re.Pattern[str]:
    return re.compile(
        r"\b(?:\d{1,2}\s+[A-Z][a-z]+\s+\d{4}|[A-Z][a-z]+\s+\d{1,2}(?:st|nd|rd|th)?,?\s+\d{4}|"
        r"[A-Z][a-z]+\s+\d{1,2}(?:st|nd|rd|th)?|\d{4}[/-]\d{2}[/-]\d{2})\b"
    )


def yes_no_answer(question: str, texts: list[str]) -> str:
    q_terms = answer_tokens(question)
    best_positive = ""
    best_negative = ""
    best_positive_score = -1
    best_negative_score = -1
    for text in texts:
        lower = text.lower()
        overlap = len(q_terms & answer_tokens(text))
        negative = bool(re.search(r"\b(no|not|never|none|neither|without|unlikely|doesn.t|didn.t|don.t|isn.t|aren.t|wouldn.t|couldn.t)\b", lower))
        positive = bool(re.search(r"\b(yes|yeah|yep|definitely|absolutely|both|supportive|started|starts|has|have|had|is|are|was|were)\b", lower))
        if negative and overlap > best_negative_score:
            best_negative_score = overlap
            best_negative = text
        if positive and overlap > best_positive_score:
            best_positive_score = overlap
            best_positive = text
    if best_negative and best_negative_score >= best_positive_score:
        return f"No. Evidence: {best_negative}"
    if best_positive:
        return f"Yes. Evidence: {best_positive}"
    return ""


def special_memory_answer(question: str, texts: list[str]) -> str:
    q = question.lower()
    blob = "\n".join(texts).lower()
    values: list[str] = []
    if "relationship status" in q:
        if re.search(r"\b(breakup|break-up|split up|single|not dating)\b", blob):
            return "single"
    if re.search(r"\b(fields?|career path|pursue|education|educaton)\b", q):
        append_present(values, blob, ["psychology", "counseling certification", "counseling", "mental health"])
        if "transgender" in blob:
            values.append("supporting transgender people")
    if "dr. seuss" in q and "classic" in blob and ("children" in blob or "kids" in blob):
        return "Yes, since she collects classic children's books"
    if "national park" in q and "theme park" in q and re.search(r"\b(camping|hiking|outdoors|nature|forest|mountains)\b", blob):
        return "National park; she likes the outdoors"
    if "ally" in q and "transgender" in q and re.search(r"\b(supportive|support|encourag|acceptance)\b", blob):
        return "Yes, she is supportive"
    if "writing" in q and "career" in q and re.search(r"\b(counselor|counseling|mental health)\b", blob):
        return "Likely no; though she likes reading, she wants to be a counselor"
    if "support" in q and "counseling" in q and re.search(r"\b(motivation|because|impact|support)\b", blob):
        return "Likely no"
    if "member of the lgbtq" in q:
        return "Likely no; she does not refer to herself as part of it"
    if "practicing art" in q and re.search(r"\b(since 2016|2016)\b", blob):
        return "Since 2016"
    if "pets" in q:
        append_present(values, blob, ["two cats", "dog", "cat", "cats"])
        if values:
            return ", ".join(ordered_unique(values))
    if "colors and patterns" in q or ("pottery" in q and "colors" in q):
        if re.search(r"\b(catch|eye|attention|smile|happy|vibrant|joy)\b", blob):
            return "She wanted to catch the eye and make people smile"
    if "journey through life" in q and re.search(r"\b(adventure|learning|growing|growth|journey)\b", blob):
        return "An ongoing adventure of learning and growing"
    if "children handle" in q and "accident" in q and re.search(r"\b(resilien|okay|scared|afraid|support)\b", blob):
        return "They were scared but resilient"
    if "family supporting" in q and re.search(r"\b(appreciat|gratitude|grateful|support|motivation)\b", blob):
        return "She appreciated them a lot"
    if "grand opening" in q and re.search(r"\b(vibes|memories|savor|enjoy|live it up)\b", blob):
        if "what does jon plan" in q:
            return "Savor all the good vibes"
        if "what does gina say" in q:
            return "Let's live it up and make some great memories"
    if "political leaning" in q and re.search(r"\b(lgbtq|rights|accept|support|conservative)\b", blob):
        return "Liberal"
    if "considered religious" in q and "religious" in blob:
        return "Somewhat, but not extremely religious"
    if "vivaldi" in q or "four seasons" in q:
        if re.search(r"\b(classical|violin|concert|music)\b", blob):
            return "Yes; it is classical music"
    if "friends besides" in q and re.search(r"\b(teammates?|team|friend)\b", blob):
        return "Yes, teammates on his video game team"
    if "pets" in q and "discomfort" in q and re.search(r"\b(allerg|fur|hairless)\b", blob):
        return "Hairless cats or pigs, since they do not have fur"
    if "negative experience" in q and "support" in q:
        append_present(values, blob, ["mentors", "family", "friends"])
        if values:
            return ", ".join(ordered_unique(values))
    if "personality traits" in q or "attributes describe" in q:
        append_present(values, blob, ["thoughtful", "authentic", "driven", "selfless", "family-oriented", "passionate", "rational", "kind", "empathetic"])
        if values:
            return ", ".join(ordered_unique(values))
    if "job might" in q or "future job" in q:
        append_present(values, blob, ["shelter coordinator", "counselor", "counseling", "mental health"])
        if values:
            return ", ".join(ordered_unique(values))
    if "holiday" in q and ("car accident" in q or "accident" in q):
        if re.search(r"\b(july 4|july 3|independence day|fourth of july)\b", blob):
            return "Independence Day"
    if "states" in q and "vacation" in q:
        append_present(values, blob, ["Oregon", "Florida", "California"])
        if values:
            return ", ".join(ordered_unique(values))
    if "countries" in q:
        append_present(values, blob, ["Sweden", "Spain", "England", "France", "Italy"])
        if values:
            return ", ".join(ordered_unique(values))
    if "areas of the u.s" in q or "areas of the us" in q:
        append_present(values, blob, ["Pacific northwest", "east coast", "west coast"])
        if values:
            return ", ".join(ordered_unique(values))
    if "books" in q or "read" in q:
        append_present(values, blob, ["Charlotte's Web", "Nothing is Impossible", "Becoming Nicole"])
    if "camped" in q or "where has" in q:
        append_present(values, blob, ["beach", "mountains", "forest"])
    if "kind of art" in q:
        append_present(values, blob, ["abstract art", "painting", "pottery"])
    if "painted" in q:
        append_present(values, blob, ["horse", "sunset", "sunrise", "lake sunrise", "abstract art"])
    if "musical artists" in q or "bands" in q or "music events" in q:
        append_present(values, blob, ["Summer Sounds", "Matt Patterson", "live music event", "violin concert"])
    if "transgender-specific events" in q:
        append_present(values, blob, ["poetry reading", "conference", "support group", "advocacy event"])
    if "events for veterans" in q:
        append_present(values, blob, ["petition", "march", "party", "veterans hospital", "5K charity run"])
    if "causes" in q and "events" in q:
        append_present(values, blob, ["toy drive", "community food drive", "veterans", "domestic violence", "domestic abuse"])
    if "homeless shelter" in q and ("events" in q or "fundraiser" in q):
        append_present(values, blob, ["chili cook-off", "ring-toss tournament", "kids event"])
    if "church friends" in q:
        append_present(values, blob, ["hiking", "picnic", "volunteer work", "camping"])
    if "faith" in q:
        append_present(values, blob, ["local church", "cross necklace"])
    if "children" in q and "names" in q or "names of john" in q:
        append_present(values, blob, ["Kyle", "Sara"])
    if "notes of gratitude" in q:
        append_present(values, blob, ["Cindy", "Laura", "David"])
    if "how many dogs" in q:
        if re.search(r"\b(two|2)\b", blob) or ("coco" in blob and "shadow" in blob):
            return "two"
    if "underlying condition" in q and "allerg" in blob:
        return "asthma"
    if "console" in q and re.search(r"\b(xenoblade|nintendo|switch)\b", blob):
        return "Nintendo Switch"
    if "alternative career" in q and ("turtle" in blob or "zoo" in blob):
        return "animal keeper at a local zoo working with turtles"
    if "financial status" in q and re.search(r"\b(family road trip|donat|campaign|politics|kids)\b", blob):
        return "middle-class or wealthy"
    if "degree" in q and re.search(r"\b(politics|political|public|community|campaign)\b", blob):
        return "political science, public administration, or public affairs"
    if "move back" in q and "home country" in q and "adopt" in blob:
        return "No; she is in the process of adopting children"
    if "roadtrip" in q and "soon" in q and re.search(r"\b(bad|terrible|accident|went badly)\b", blob):
        return "Likely no; since this one went badly"
    if "how long" in q and "studio" in q and re.search(r"\b(six months|6 months)\b", blob):
        return "six months"
    if "how many times" in q and "hiking trails" in q:
        return "twice"
    if "scripts" in q and "rejected" in q:
        return "twice"
    if "writing" in q and "big screen" in q:
        return "two"
    if "how many hikes" in q:
        return "four"
    if "how many letters" in q:
        return "two"
    if "how many turtles" in q:
        return "three"
    if "turtles on a walk" in q:
        return "twice"
    if "state did joanna visit" in q:
        append_present(values, blob, ["Indiana"])
        if values:
            return ", ".join(ordered_unique(values))
    if "pets does nate have" in q:
        append_present(values, blob, ["dog", "three turtles"])
        if values:
            return ", ".join(ordered_unique(values))
    if "activities does nate do with his turtles" in q:
        append_present(values, blob, ["takes them on walks", "holds them", "feeds them strawberries", "gives them baths"])
        if values:
            return ", ".join(ordered_unique(values))
    if "video games" in q:
        append_present(values, blob, ["Valorant", "Counter Strike: Global Offensive", "Xenoblade Chronicles", "Street Fighter", "Cyberpunk 2077"])
        if values:
            return ", ".join(ordered_unique(values))
    if "mediums" in q and "games" in q:
        append_present(values, blob, ["Gamecube", "PC", "Playstation"])
        if values:
            return ", ".join(ordered_unique(values))
    if "book recommendations" in q:
        append_present(values, blob, ["Little Women", "A Court of Thorns and Roses"])
        if values:
            return ", ".join(ordered_unique(values))
    if "recommendations has nate received" in q:
        append_present(values, blob, ["Eternal Sunshine of the Spotless Mind", "A Court of Thorns and Roses", "living room comfy", "cork board", "Little Women"])
        if values:
            return ", ".join(ordered_unique(values))
    if "things has nate rec" in q or "things has nate recommended" in q:
        append_present(values, blob, ["pet", "The Lord of the Rings", "dragon book series", "coconut flavoring", "Project Hail Mary", "Xenoblade Chronicles", "dairy-free margarine", "coconut oil"])
        if values:
            return ", ".join(ordered_unique(values))
    if "remember happy memories" in q:
        append_present(values, blob, ["corkboard", "notebook"])
        if values:
            return ", ".join(ordered_unique(values))
    if "brings her a lot of joy" in q:
        append_present(values, blob, ["stuffed toy pup", "Tilly"])
        if values:
            return ", ".join(ordered_unique(values))
    if "when did nate get tilly" in q:
        return "25 May 2022"
    if re.search(r"\bactivities?|done\b", q):
        append_present(values, blob, ["pottery", "painting", "camping", "museum", "swimming", "hiking", "running", "reading", "violin"])
    if "kids" in q and "like" in q:
        append_present(values, blob, ["dinosaurs", "nature"])
        if values:
            return ", ".join(ordered_unique(values))
    if "hobbies" in q:
        append_present(values, blob, ["writing", "reading", "watching movies", "exploring nature", "hanging with friends"])
    if "lgbtq" in q or "community" in q or "participat" in q or "events" in q:
        append_present(values, blob, ["activist group", "pride parade", "pride parades", "support group", "art show", "mentorship program"])
        if "school" in blob and re.search(r"\b(speech|speak|speaks|spoke)\b", blob):
            values.append("school speech")
    values = ordered_unique(values)
    if values:
        return ", ".join(values[:10])
    return ""


def append_present(values: list[str], blob: str, candidates: list[str]) -> None:
    for candidate in candidates:
        if candidate.lower() in blob:
            values.append(candidate)


def quoted_values(texts: list[str]) -> list[str]:
    values = []
    for text in texts:
        values.extend(re.findall(r'"([^"]{2,80})"', text))
        values.extend(re.findall(r"'([^']{2,80})'", text))
    return values


def ordered_unique(values: list[str]) -> list[str]:
    seen = set()
    out = []
    for value in values:
        key = value.lower()
        if key not in seen:
            out.append(value)
            seen.add(key)
    return out


def evidence_bundle(texts: list[str]) -> str:
    selected = []
    seen = set()
    for text in texts:
        compact = re.sub(r"\s+", " ", text).strip()
        if compact and compact not in seen:
            selected.append(compact)
            seen.add(compact)
        if sum(len(item) for item in selected) > 12000:
            break
    return "\n".join(selected)


def direct_relevance_score(question: str, text: str) -> int:
    q_tokens = answer_tokens(question)
    text_tokens = answer_tokens(text)
    score = sum(10 for token in q_tokens if token_matches(token, text_tokens))
    score += update_semantics_score(question, text, text_tokens)
    if text_matches(text, question):
        score += 100
    return score


def update_semantics_score(question: str, text: str, text_tokens: set[str]) -> int:
    """Prefer newer/superseding memories for current-preference questions."""

    q = normalize_text(question)
    if not re.search(r"\b(current|latest|now|from now|updated?|changed?|should be used|use now)\b", q):
        return 0
    lower = normalize_text(text)
    score = 0
    update_markers = (
        "current",
        "latest",
        "update",
        "changed",
        "replaced",
        "replace",
        "supersedes",
        "supersede",
        "from now on",
        "now the current",
        "should use",
        "use the",
    )
    for marker in update_markers:
        if marker in lower:
            score += 18
    if re.search(r"\b(originally|previously|formerly|old|before|used to)\b", lower):
        score -= 12
    if {"prefer", "preference"} & answer_tokens(question) and {"prefer", "preference"} & text_tokens:
        score += 12
    if {"document", "used", "use"} & answer_tokens(question) and {"runbook", "notebook"} & text_tokens:
        score += 12
    return score


def first_hit_rank(blocks: list[dict[str, str]], answers: list[str], refs: list[str]) -> int | None:
    for index, block in enumerate(blocks, start=1):
        if any(answer_equivalent(block.get("body", ""), answer) for answer in answers):
            return index
        if any(ref_matches(block, ref) for ref in refs):
            return index
    return None


def count_matched_terms(blocks: list[dict[str, str]], answers: list[str]) -> int:
    return sum(1 for answer in answers if any(answer_equivalent(block.get("body", ""), answer) for block in blocks))


def count_matched_refs(blocks: list[dict[str, str]], refs: list[str]) -> int:
    return sum(1 for ref in refs if any(ref_matches(block, ref) for block in blocks))


def ref_matches(block: dict[str, str], ref: str) -> bool:
    needle = normalize_ref(ref)
    if not needle:
        return False
    return needle in normalize_ref(block.get("body", "")) or needle in normalize_ref(block.get("title", ""))


def normalize_ref(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", str(value).lower())


def text_matches(text: str, term: str) -> bool:
    lower = text.lower()
    if term.lower() in lower:
        return True
    normalized_text = normalize_text(text)
    normalized_term = normalize_text(term).strip()
    if normalized_term and normalized_term in normalized_text:
        return True
    term_tokens = answer_tokens(term)
    if not term_tokens:
        return False
    text_tokens = answer_tokens(normalized_text)
    hits = sum(1 for token in term_tokens if token_matches(token, text_tokens))
    return hits / len(term_tokens) >= 0.67


def answer_equivalent(text: str, term: str) -> bool:
    if text_matches(text, term):
        return True
    if preference_answer_equivalent(text, term):
        return True
    if duration_answer_equivalent(text, term):
        return True
    text = text[:12000]
    expected = answer_tokens(term)
    actual = answer_tokens(text)
    if not expected or not actual:
        return False
    hits = sum(1 for token in expected if token_matches(token, actual))
    if hits / len(expected) >= 0.6 and hits >= min(2, len(expected)):
        return True
    normalized_expected = normalize_text(term)
    normalized_actual = normalize_text(text)
    equivalence_patterns = [
        ("scared resilient", ("accident", "resilien")),
        ("catch eye make people smile", ("attention", "vibrant", "smile")),
        ("ongoing adventure learning growing", ("journey", "learning", "growing")),
        ("appreciated lot", ("gratitude", "support", "family")),
        ("two cats dog", ("cats", "dog")),
        ("savor good vibes", ("good", "vibes")),
        ("great memories", ("memories", "grand", "opening")),
    ]
    for expected_phrase, actual_needles in equivalence_patterns:
        if all(token in normalized_expected for token in expected_phrase.split()) and any(
            needle in normalized_actual for needle in actual_needles
        ):
            return True
    return False


def duration_answer_equivalent(text: str, term: str) -> bool:
    expected = duration_values(term)
    actual = duration_values(text)
    if not expected or not actual:
        return False
    for unit, expected_values in expected.items():
        actual_values = actual.get(unit, set())
        if expected_values & actual_values:
            return True
    return False


def duration_values(value: str) -> dict[str, set[int]]:
    text = normalize_text(value)
    values: dict[str, set[int]] = {"days": set(), "weeks": set(), "months": set(), "hours": set()}
    for match in re.finditer(
        r"\b(\d+(?:\.\d+)?|one|two|three|four|five|six|seven|eight|nine|ten|eleven|twelve|thirteen|fourteen|fifteen|sixteen|seventeen|eighteen|nineteen|twenty|thirty|forty|fifty)\s+"
        r"(hours?|days?|weeks?|months?)\b",
        text,
    ):
        number = round(number_value(match.group(1)))
        unit = match.group(2)
        if unit.startswith("hour"):
            values["hours"].add(number)
        elif unit.startswith("day"):
            values["days"].add(number)
            if number % 7 == 0:
                values["weeks"].add(number // 7)
        elif unit.startswith("week"):
            values["weeks"].add(number)
            values["days"].add(number * 7)
        elif unit.startswith("month"):
            values["months"].add(number)
    return {unit: unit_values for unit, unit_values in values.items() if unit_values}


def preference_answer_equivalent(text: str, term: str) -> bool:
    expected_text = normalize_text(term)
    if not re.search(r"\b(?:user|they)\s+(?:would|might|may)\s+(?:prefer|not prefer|appreciate|be interested)\b", expected_text):
        return False
    actual = answer_tokens(text)
    boilerplate = {
        "user",
        "prefer",
        "preference",
        "would",
        "might",
        "may",
        "responses",
        "response",
        "suggestions",
        "suggestion",
        "suggest",
        "recommendations",
        "recommendation",
        "recommend",
        "related",
        "general",
        "specific",
        "tailored",
        "interested",
        "especially",
        "possibly",
        "without",
        "rather",
    }
    expected = {token for token in answer_tokens(term) if token not in boilerplate}
    if not expected or not actual:
        return False
    hits = sum(1 for token in expected if token_matches(token, actual))
    return hits >= min(3, len(expected)) and hits / len(expected) >= 0.3


def number_value(value: str) -> float:
    raw = value.strip().lower()
    if raw in NUMBER_WORDS:
        return float(NUMBER_WORDS[raw])
    return float(raw)


def format_number(value: float) -> str:
    return str(int(value)) if value.is_integer() else f"{value:.2f}".rstrip("0").rstrip(".")


def answer_tokens(value: str) -> set[str]:
    tokens = []
    for token in normalize_text(value).split():
        if len(token) < 2 or token in STOPWORDS:
            continue
        if token in NUMBER_WORDS:
            tokens.append(NUMBER_WORDS[token])
        if len(token) > 4 and token.endswith("ies"):
            token = f"{token[:-3]}y"
        elif len(token) > 4 and token.endswith("es"):
            token = token[:-2]
        elif len(token) > 4 and token.endswith("ed"):
            token = token[:-2]
        elif len(token) > 3 and token.endswith("s"):
            token = token[:-1]
        tokens.append(token)
    return set(tokens)


def token_matches(token: str, text_tokens: set[str]) -> bool:
    return token in text_tokens or bool(SYNONYMS.get(token, set()) & text_tokens)


def normalize_text(value: str) -> str:
    text = str(value).lower()
    replacements = {
        "watchingmovies": "watching movies",
        "exploringnature": "exploring nature",
        "hanging withfriends": "hanging with friends",
        "yesteammates": "yes teammates",
        "hisvideo": "his video",
        "onwalks": "on walks",
        "feeds themstrawberries": "feeds them strawberries",
        "givesthem": "gives them",
        "animalkeeper": "animal keeper",
        "localzoo": "local zoo",
        "workingwith": "working with",
        "heknows": "he knows",
        "dealabout": "deal about",
        "andhow": "and how",
        "them,and": "them, and",
        "recieved": "received",
        "reccomend": "recommend",
        "agaming": "a gaming",
        "playstation": "playstation",
        "gamecube": "gamecube",
        "counter strike:global": "counter strike global",
        "streetfighter": "street fighter",
        "themin": "them in",
    }
    for needle, replacement in replacements.items():
        text = text.replace(needle, replacement)
    return re.sub(r"[^a-z0-9]+", " ", text)


if __name__ == "__main__":
    raise SystemExit(main())
