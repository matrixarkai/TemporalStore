#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Run a LoCoMo direct-source ExternalBaseline/ExternalBaseline diagnostic baseline.

This baseline uses the same LoCoMo source records as the MatrixArk runner, ranks
raw conversation turns directly, and calls the same OpenAI-compatible OSS reader
configuration. It is diagnostic, not a paper-comparable ExternalBaseline memory
pipeline claim, but it is useful for apples-to-apples reader/model/budget
comparisons.
"""

from __future__ import annotations

import argparse
import json
import math
import sys
import time
from collections import defaultdict
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent))
from convert_locomo_to_context_jsonl import (  # noqa: E402
    clean_id,
    expand_evidence_refs_for_sources,
    infer_dataset_name,
    locomo_evidence_window_sources,
    normalize_answers,
    normalize_category,
    normalize_evidence_refs,
    record_questions,
    record_sources,
)
from run_locomo_ingest_once import (  # noqa: E402
    BenchmarkReader,
    ReaderConfig,
    RetrievalBudgetConfig,
    answer_equivalent,
    adaptive_max_events_for_question,
    clamp_percent,
    configure_retrieval_embedding_model,
    count_matched_refs,
    count_matched_terms,
    count_reader_context_terms,
    distinct_source_group_count,
    elapsed_ms,
    estimated_tokens,
    first_hit_rank,
    rank_sources,
    retrieval_budget_counts,
    unsupported_benchmark_reason,
)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", default="/root/matrixark_benchmarks/data/locomo10.json")
    parser.add_argument("--report", default="/tmp/external_baseline_locomo_source_retrieval_report.json")
    parser.add_argument("--matrixark-report", default="/tmp/matrixark_locomo_report.json")
    parser.add_argument("--reader-base-url", default="http://127.0.0.1:11434/v1")
    parser.add_argument("--reader-model", default="qwen2.5:1.5b")
    parser.add_argument("--embedding-model", default="matrixark-hash-embedding-32")
    parser.add_argument("--provider-name", default="external_baseline-direct-source-qwen25-1.5b")
    parser.add_argument("--reader-timeout-seconds", type=float, default=60.0)
    parser.add_argument("--reader-max-context-chars", type=int, default=12000)
    parser.add_argument("--reader-max-tokens", type=int, default=96)
    parser.add_argument("--max-events", "--top-k", dest="max_events", type=int, default=128)
    parser.add_argument("--adaptive-max-events", action="store_true")
    parser.add_argument("--adaptive-base-max-events", type=int, default=128)
    parser.add_argument("--question-limit", type=int, default=0)
    parser.add_argument("--question-offset", type=int, default=0)
    parser.add_argument("--evidence-window", type=int, default=None)
    parser.add_argument("--reader-include-extractive-hint", action="store_true")
    parser.add_argument("--reader-candidate-only", action="store_true")
    parser.add_argument("--reader-candidate-first", action="store_true")
    parser.add_argument("--reader-candidate-hybrid", action="store_true")
    parser.add_argument("--reader-focus-evidence", action="store_true")
    parser.add_argument("--same-session-percent", type=float, default=1.0)
    parser.add_argument("--cross-session-percent", type=float, default=1.0)
    parser.add_argument("--summary-percent", type=float, default=1.0)
    parser.add_argument("--entity-percent", type=float, default=1.0)
    parser.add_argument("--event-percent", type=float, default=1.0)
    parser.add_argument(
        "--require-shared-oss-models",
        dest="require_shared_oss_models",
        action="store_true",
        default=True,
        help=(
            "Fail if this baseline does not match MatrixArk reader, embedding/encoding, "
            "retrieval, context-budget, and output-token settings. Enabled by default."
        ),
    )
    parser.add_argument(
        "--allow-shared-oss-model-drift",
        dest="require_shared_oss_models",
        action="store_false",
        help="Diagnostic-only escape hatch for intentionally unfair local model or budget experiments.",
    )
    args = parser.parse_args()
    try:
        configure_retrieval_embedding_model(
            args.embedding_model,
            require_runtime=bool(args.require_shared_oss_models),
        )
    except RuntimeError as exc:
        report = {
            "benchmark_family": "locomo",
            "baseline": "external_baseline_style_direct_source_retrieval",
            "ready": False,
            "blockers": [str(exc)],
        }
        return finish(report, args.report, time.time(), 2)

    started = time.time()
    report: dict[str, Any] = {
        "benchmark_family": "locomo",
        "baseline": "external_baseline_style_direct_source_retrieval",
        "claim_status": "diagnostic_not_paper_comparable",
        "diagnostic_only": True,
        "input": args.input,
        "reader_provider_name": args.provider_name,
        "reader_model": args.reader_model,
        "embedding_model": args.embedding_model,
        "encoding_model": args.embedding_model,
        "max_events": args.max_events,
        "top_k": args.max_events,
        "adaptive_max_events": bool(args.adaptive_max_events),
        "adaptive_base_max_events": args.adaptive_base_max_events,
        "reader_max_context_chars": args.reader_max_context_chars,
        "reader_max_tokens": args.reader_max_tokens,
        "reader_include_extractive_hint": bool(args.reader_include_extractive_hint),
        "reader_candidate_only": bool(args.reader_candidate_only),
        "reader_candidate_first": bool(args.reader_candidate_first or args.reader_candidate_hybrid),
        "reader_candidate_hybrid": bool(args.reader_candidate_hybrid),
        "reader_focus_evidence": bool(args.reader_focus_evidence),
        "question_limit": args.question_limit,
        "question_offset": args.question_offset,
        "question_slice_diagnostic": bool(args.question_limit or args.question_offset),
        "warnings": [
            "This diagnostic ranks raw LoCoMo source turns directly; it is not an ExternalBaseline memory-extraction pipeline claim.",
            "Fair comparison requires the shared OSS contract validator to pass across MatrixArk and this baseline.",
        ],
        "blockers": [],
    }

    input_path = Path(args.input)
    if not input_path.exists():
        report["blockers"].append(f"missing_input:{args.input}")
        return finish(report, args.report, started, 2)

    reader = BenchmarkReader(
        ReaderConfig(
            mode="open-source",
            provider_name=args.provider_name,
            model=args.reader_model,
            base_url=args.reader_base_url,
            api_key_env="",
            timeout_seconds=args.reader_timeout_seconds,
            max_context_chars=args.reader_max_context_chars,
            allow_fallback=False,
            include_extractive_hint=bool(args.reader_include_extractive_hint),
            focus_evidence=bool(args.reader_focus_evidence),
            candidate_first=bool(args.reader_candidate_first or args.reader_candidate_hybrid),
            candidate_only=bool(args.reader_candidate_only),
            candidate_hybrid=bool(args.reader_candidate_hybrid),
        )
    )
    retrieval_budget = RetrievalBudgetConfig(
        same_session_percent=args.same_session_percent,
        cross_session_percent=args.cross_session_percent,
        summary_percent=args.summary_percent,
        entity_percent=args.entity_percent,
        event_percent=args.event_percent,
    )
    retrieval_budget_report = retrieval_budget.to_report()
    report["retrieval_budget_config"] = retrieval_budget_report

    records = json.loads(input_path.read_text(encoding="utf-8"))
    if not isinstance(records, list) or not records:
        report["blockers"].append("invalid_locomo_input")
        return finish(report, args.report, started, 2)

    total = 0
    supported_question_index = 0
    hit_count = 0
    reciprocal_rank_sum = 0.0
    reader_hit_count = 0
    total_source_tokens = 0
    total_retrieved_tokens = 0
    total_answer_terms = 0
    matched_answer_terms = 0
    matched_context_answer_terms = 0
    reader_answer_coverage_count = 0
    total_refs = 0
    matched_refs = 0
    adaptive_max_event_totals: defaultdict[int, int] = defaultdict(int)
    source_count = 0
    conversations_loaded = 0
    retrieval_latencies_ms: list[float] = []
    reader_latencies_ms: list[float] = []
    category: defaultdict[str, dict[str, Any]] = defaultdict(category_row)
    dataset_counts: defaultdict[str, int] = defaultdict(int)
    per_query: list[dict[str, Any]] = []

    for record_index, record in enumerate(records):
        if not isinstance(record, dict):
            continue
        dataset_counts[infer_dataset_name(record)] += 1
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
        for question_index, qa in enumerate(record_questions(record)):
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
            scored_refs = (
                expand_evidence_refs_for_sources(refs, query_sources)
                if args.evidence_window is not None and refs
                else refs
            )
            case_category = normalize_category(
                qa.get("category") or qa.get("question_type") or qa.get("reasoning_type") or qa.get("ability")
            )
            query_id = f"{conversation_id}-q{question_index + 1}"
            unsupported_reason = unsupported_benchmark_reason(
                answers,
                scored_refs,
                query_sources,
                qa.get("unsupported_reason"),
                qa.get("unsupported_benchmark_case"),
            )
            if unsupported_reason:
                continue
            if args.question_offset and supported_question_index < args.question_offset:
                supported_question_index += 1
                continue
            if args.question_limit and total >= args.question_limit:
                break
            supported_question_index += 1

            source_tokens = sum(estimated_tokens(source.get("body", "")) for source in query_sources)
            retrieval_started = time.perf_counter()
            effective_max_events = adaptive_max_events_for_question(question, args)
            blocks = rank_sources(question, query_sources, effective_max_events, retrieval_budget)
            adaptive_max_event_totals[effective_max_events] += 1
            retrieval_ms = elapsed_ms(retrieval_started)
            reader_started = time.perf_counter()
            reader_answer = reader.answer(question, blocks)
            reader_ms = elapsed_ms(reader_started)
            rank = first_hit_rank(blocks, answers, scored_refs)
            matched_terms = count_matched_terms(blocks, answers)
            matched_context_terms = count_reader_context_terms(question, blocks, answers, reader.config)
            matched_ref_count = count_matched_refs(blocks, scored_refs)
            reader_hit = any(answer_equivalent(reader_answer, answer) for answer in answers)
            reader_matched_terms = sum(1 for answer in answers if answer_equivalent(reader_answer, answer))
            retrieved_tokens = sum(estimated_tokens(block.get("body", "")) for block in blocks)
            budget_counts = retrieval_budget_counts(blocks)

            total += 1
            hit_count += int(rank is not None)
            if rank:
                reciprocal_rank_sum += 1.0 / rank
            reader_hit_count += int(reader_hit)
            total_source_tokens += source_tokens
            total_retrieved_tokens += retrieved_tokens
            total_answer_terms += len(answers)
            matched_answer_terms += matched_terms
            matched_context_answer_terms += matched_context_terms
            reader_answer_coverage_count += reader_matched_terms
            total_refs += len(scored_refs)
            matched_refs += matched_ref_count
            retrieval_latencies_ms.append(retrieval_ms)
            reader_latencies_ms.append(reader_ms)

            row = category[case_category]
            row["case_count"] += 1
            row["hit_count"] += int(rank is not None)
            row["reader_hit_count"] += int(reader_hit)
            row["answer_terms"] += len(answers)
            row["matched_terms"] += matched_terms
            row["reader_matched_terms"] += reader_matched_terms

            per_query.append(
                {
                    "query_id": query_id,
                    "category": case_category,
                    "question": question,
                    "expected_answers": answers,
                    "expected_source_refs": scored_refs,
                    "retrieved_source_refs": [str(block.get("id") or block.get("title") or "") for block in blocks],
                    "retrieval_hit": rank is not None,
                    "rank": rank,
                    "reader_answer": reader_answer,
                    "reader_hit": reader_hit,
                    "retrieved_tokens": retrieved_tokens,
                    "source_tokens": source_tokens,
                    "token_reduction_percent": reduction_percent(retrieved_tokens, source_tokens),
                    "retrieval_ms": round(retrieval_ms, 3),
                    "reader_ms": round(reader_ms, 3),
                    "retrieval_budget_counts": budget_counts,
                    "selected_source_group_count": distinct_source_group_count(blocks),
                }
            )
        if args.question_limit and total >= args.question_limit:
            break

    category_breakdown = build_category_breakdown(category)
    n = max(1, total)
    report.update(
        {
            "dataset_name": dominant_dataset(dataset_counts),
            "conversation_count": conversations_loaded,
            "source_count": source_count,
            "case_count": total,
            "benchmark_hit_at_k": hit_count / n,
            "benchmark_mean_reciprocal_rank": reciprocal_rank_sum / n,
            "benchmark_reader_hit_rate": reader_hit_count / n,
            "reader_hit_rate": reader_hit_count / n,
            "reader_answer_coverage": reader_answer_coverage_count / max(1, total_answer_terms),
            "benchmark_context_answer_coverage": matched_context_answer_terms / max(1, total_answer_terms),
            "benchmark_retrieval_answer_coverage": matched_answer_terms / max(1, total_answer_terms),
            "evidence_ref_coverage": matched_refs / max(1, total_refs),
            "reader_open_source_calls": reader.open_source_calls,
            "reader_fallback_count": reader.fallback_count,
            "reader_error_count": reader.error_count,
            "reader_last_error": reader.last_error,
            "benchmark_avg_source_tokens_per_query": total_source_tokens / n,
            "benchmark_avg_retrieved_tokens_per_query": total_retrieved_tokens / n,
            "benchmark_total_source_tokens": total_source_tokens,
            "benchmark_total_retrieved_tokens": total_retrieved_tokens,
            "benchmark_token_reduction_percent": reduction_percent(total_retrieved_tokens, total_source_tokens),
            "adaptive_effective_max_event_counts": {
                str(key): value for key, value in sorted(adaptive_max_event_totals.items())
            },
            "benchmark_retrieval_p50_ms": percentile(retrieval_latencies_ms, 50),
            "benchmark_retrieval_p95_ms": percentile(retrieval_latencies_ms, 95),
            "benchmark_reader_p50_ms": percentile(reader_latencies_ms, 50),
            "benchmark_reader_p95_ms": percentile(reader_latencies_ms, 95),
            "category_breakdown": category_breakdown,
            "benchmark_per_query_count": len(per_query),
            "benchmark_per_query": per_query,
            "matrixark_reference": load_matrixark_reference(args.matrixark_report),
        }
    )
    report["benchmark_model_contract"] = benchmark_model_contract(args, report["matrixark_reference"])
    return finish_with_contract_gate(report, args.report, started, args.require_shared_oss_models)


def category_row() -> dict[str, Any]:
    return {
        "case_count": 0,
        "hit_count": 0,
        "reader_hit_count": 0,
        "answer_terms": 0,
        "matched_terms": 0,
        "reader_matched_terms": 0,
    }


def build_category_breakdown(rows: dict[str, dict[str, Any]]) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for name, row in sorted(rows.items()):
        count = int(row.get("case_count") or 0)
        terms = int(row.get("answer_terms") or 0)
        out[name] = {
            "case_count": count,
            "hit_at_k": row.get("hit_count", 0) / count if count else 0.0,
            "reader_hit_rate": row.get("reader_hit_count", 0) / count if count else 0.0,
            "answer_term_coverage": row.get("matched_terms", 0) / terms if terms else 0.0,
            "reader_answer_coverage": row.get("reader_matched_terms", 0) / terms if terms else 0.0,
        }
    return out


def dominant_dataset(counts: dict[str, int]) -> str:
    if not counts:
        return "unknown"
    return sorted(counts.items(), key=lambda row: (-row[1], row[0]))[0][0]


def load_matrixark_reference(path: str) -> dict[str, Any]:
    p = Path(path)
    if not p.exists():
        return {"available": False, "path": path}
    data = json.loads(p.read_text(encoding="utf-8"))
    return {
        "available": True,
        "path": path,
        "case_count": data.get("case_count"),
        "benchmark_hit_at_k": data.get("benchmark_hit_at_k"),
        "benchmark_reader_hit_rate": data.get("benchmark_reader_hit_rate", data.get("reader_hit_rate")),
        "benchmark_token_reduction_percent": data.get("benchmark_token_reduction_percent"),
        "benchmark_retrieval_p95_ms": data.get("benchmark_retrieval_p95_ms"),
        "benchmark_reader_p95_ms": data.get("benchmark_reader_p95_ms"),
        "python_only_diagnostic": data.get("python_only_diagnostic"),
        "adaptive_max_events": data.get("adaptive_max_events"),
        "adaptive_base_max_events": data.get("adaptive_base_max_events"),
        "benchmark_model_contract": data.get("benchmark_model_contract") or {},
    }


def benchmark_model_contract(args: argparse.Namespace, matrixark_reference: dict[str, Any]) -> dict[str, Any]:
    reference_contract = matrixark_reference.get("benchmark_model_contract") if isinstance(matrixark_reference, dict) else {}
    if not isinstance(reference_contract, dict):
        reference_contract = {}
    matrixark_reader = str(reference_contract.get("matrixark_reader_model") or args.reader_model).strip()
    matrixark_embedding = str(reference_contract.get("matrixark_embedding_model") or args.embedding_model).strip()
    matrixark_max_events = int(reference_contract.get("matrixark_max_events") or args.max_events)
    matrixark_reader_budget = int(reference_contract.get("matrixark_reader_max_context_chars") or args.reader_max_context_chars)
    matrixark_reader_max_tokens = int(reference_contract.get("matrixark_reader_max_tokens") or args.reader_max_tokens)
    matrixark_reader_fallback_allowed = bool(reference_contract.get("matrixark_reader_fallback_allowed"))
    baseline_reader = str(args.reader_model).strip()
    baseline_embedding = str(args.embedding_model).strip()
    baseline_max_events = int(args.max_events)
    baseline_reader_budget = int(args.reader_max_context_chars)
    baseline_reader_max_tokens = int(args.reader_max_tokens)
    baseline_reader_fallback_allowed = False
    matrixark_adaptive = bool(
        reference_contract.get("matrixark_adaptive_max_events")
        if "matrixark_adaptive_max_events" in reference_contract
        else matrixark_reference.get("adaptive_max_events")
    )
    matrixark_adaptive_base = int(
        reference_contract.get("matrixark_adaptive_base_max_events")
        if "matrixark_adaptive_base_max_events" in reference_contract
        else matrixark_reference.get("adaptive_base_max_events")
        or 0
    )
    baseline_adaptive = bool(args.adaptive_max_events)
    baseline_adaptive_base = int(args.adaptive_base_max_events) if baseline_adaptive else 0
    baseline_same_session_percent = clamp_percent(float(args.same_session_percent))
    baseline_cross_session_percent = clamp_percent(float(args.cross_session_percent))
    baseline_summary_percent = clamp_percent(float(args.summary_percent))
    baseline_entity_percent = clamp_percent(float(args.entity_percent))
    baseline_event_percent = clamp_percent(float(args.event_percent))
    matrixark_budget = matrixark_reference.get("retrieval_budget_config")
    if not isinstance(matrixark_budget, dict):
        matrixark_budget = {}
    matrixark_same_session_percent = float(
        reference_contract.get("matrixark_retrieval_same_session_percent")
        if "matrixark_retrieval_same_session_percent" in reference_contract
        else matrixark_budget.get("same_session_percent")
        or baseline_same_session_percent
    )
    matrixark_cross_session_percent = float(
        reference_contract.get("matrixark_retrieval_cross_session_percent")
        if "matrixark_retrieval_cross_session_percent" in reference_contract
        else matrixark_budget.get("cross_session_percent")
        or baseline_cross_session_percent
    )
    matrixark_summary_percent = float(
        reference_contract.get("matrixark_retrieval_summary_percent")
        if "matrixark_retrieval_summary_percent" in reference_contract
        else matrixark_budget.get("summary_percent")
        or baseline_summary_percent
    )
    matrixark_entity_percent = float(
        reference_contract.get("matrixark_retrieval_entity_percent")
        if "matrixark_retrieval_entity_percent" in reference_contract
        else matrixark_budget.get("entity_percent")
        or baseline_entity_percent
    )
    matrixark_event_percent = float(
        reference_contract.get("matrixark_retrieval_event_percent")
        if "matrixark_retrieval_event_percent" in reference_contract
        else matrixark_budget.get("event_percent")
        or baseline_event_percent
    )
    adaptive_policy_match = (
        matrixark_adaptive == baseline_adaptive
        and (not matrixark_adaptive or matrixark_adaptive_base == baseline_adaptive_base)
    )
    retrieval_budget_match = (
        round(matrixark_same_session_percent, 6) == round(baseline_same_session_percent, 6)
        and round(matrixark_cross_session_percent, 6) == round(baseline_cross_session_percent, 6)
        and round(matrixark_summary_percent, 6) == round(baseline_summary_percent, 6)
        and round(matrixark_entity_percent, 6) == round(baseline_entity_percent, 6)
        and round(matrixark_event_percent, 6) == round(baseline_event_percent, 6)
    )
    return {
        "matrixark_provider_name": str(reference_contract.get("matrixark_provider_name") or "matrixark").strip(),
        "matrixark_reader_model": matrixark_reader,
        "matrixark_embedding_model": matrixark_embedding,
        "matrixark_encoding_model": matrixark_embedding,
        "matrixark_max_events": matrixark_max_events,
        "matrixark_reader_max_context_chars": matrixark_reader_budget,
        "matrixark_reader_max_tokens": matrixark_reader_max_tokens,
        "matrixark_reader_fallback_allowed": matrixark_reader_fallback_allowed,
        "matrixark_adaptive_max_events": matrixark_adaptive,
        "matrixark_adaptive_base_max_events": matrixark_adaptive_base,
        "matrixark_retrieval_same_session_percent": matrixark_same_session_percent,
        "matrixark_retrieval_cross_session_percent": matrixark_cross_session_percent,
        "matrixark_retrieval_summary_percent": matrixark_summary_percent,
        "matrixark_retrieval_entity_percent": matrixark_entity_percent,
        "matrixark_retrieval_event_percent": matrixark_event_percent,
        "baseline_provider_name": str(args.provider_name).strip(),
        "baseline_reader_model": baseline_reader,
        "baseline_embedding_model": baseline_embedding,
        "baseline_encoding_model": baseline_embedding,
        "baseline_max_events": baseline_max_events,
        "baseline_reader_max_context_chars": baseline_reader_budget,
        "baseline_reader_max_tokens": baseline_reader_max_tokens,
        "baseline_reader_fallback_allowed": baseline_reader_fallback_allowed,
        "baseline_adaptive_max_events": baseline_adaptive,
        "baseline_adaptive_base_max_events": baseline_adaptive_base,
        "baseline_retrieval_same_session_percent": baseline_same_session_percent,
        "baseline_retrieval_cross_session_percent": baseline_cross_session_percent,
        "baseline_retrieval_summary_percent": baseline_summary_percent,
        "baseline_retrieval_entity_percent": baseline_entity_percent,
        "baseline_retrieval_event_percent": baseline_event_percent,
        "provider_identity_declared": bool(args.provider_name),
        "reader_model_match": normalized_model_name(matrixark_reader) == normalized_model_name(baseline_reader),
        "embedding_model_match": normalized_model_name(matrixark_embedding) == normalized_model_name(baseline_embedding),
        "max_events_match": matrixark_max_events == baseline_max_events,
        "reader_context_budget_match": matrixark_reader_budget == baseline_reader_budget,
        "reader_output_budget_match": matrixark_reader_max_tokens == baseline_reader_max_tokens,
        "reader_fallback_policy_match": matrixark_reader_fallback_allowed == baseline_reader_fallback_allowed,
        "adaptive_policy_match": adaptive_policy_match,
        "retrieval_budget_match": retrieval_budget_match,
        "shared_oss_model_contract_required": True,
        "shared_oss_models_forced": bool(args.require_shared_oss_models),
        "same_oss_reader_model_forced": bool(args.require_shared_oss_models),
        "same_oss_encoding_model_forced": bool(args.require_shared_oss_models),
        "shared_oss_model_contract_passed": (
            normalized_model_name(matrixark_reader) == normalized_model_name(baseline_reader)
            and normalized_model_name(matrixark_embedding) == normalized_model_name(baseline_embedding)
            and matrixark_max_events == baseline_max_events
            and matrixark_reader_budget == baseline_reader_budget
            and matrixark_reader_max_tokens == baseline_reader_max_tokens
            and matrixark_reader_fallback_allowed == baseline_reader_fallback_allowed
            and adaptive_policy_match
            and retrieval_budget_match
        ),
        "comparison_rule": (
            "MatrixArk and ExternalBaseline/ExternalBaseline rows must use the same OSS reader model, "
            "embedding/encoding model, retrieval block budget, adaptive retrieval policy, retrieval budget split, "
            "reader context budget, reader output-token budget, and reader fallback policy."
        ),
    }


def normalized_model_name(value: Any) -> str:
    import re

    return re.sub(r"[^a-z0-9._:-]+", "", str(value or "").strip().lower())


def percentile(values: list[float], pct: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    if len(ordered) == 1:
        return round(ordered[0], 3)
    rank = (len(ordered) - 1) * (pct / 100.0)
    low = math.floor(rank)
    high = math.ceil(rank)
    if low == high:
        return round(ordered[int(rank)], 3)
    value = ordered[low] * (high - rank) + ordered[high] * (rank - low)
    return round(value, 3)


def reduction_percent(retrieved: int, source: int) -> float:
    if source <= 0:
        return 0.0
    return round(100.0 * (1.0 - (retrieved / source)), 4)


def finish_with_contract_gate(report: dict[str, Any], path: str, started: float, require_contract: bool) -> int:
    contract = report.get("benchmark_model_contract")
    if require_contract and not (isinstance(contract, dict) and contract.get("shared_oss_model_contract_passed")):
        report.setdefault("blockers", []).append("shared_oss_model_contract_mismatch")
        return finish(report, path, started, 2)
    return finish(report, path, started, 0)


def finish(report: dict[str, Any], path: str, started: float, code: int) -> int:
    report["duration_seconds"] = round(time.time() - started, 3)
    report["ready"] = code == 0 and not report.get("blockers")
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    Path(path).write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(path)
    return code


if __name__ == "__main__":
    raise SystemExit(main())
