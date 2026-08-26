# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Split out of run_locomo_ingest_once.py; re-exported at that module's end via the dual
relative/absolute import pattern so the same module object is reused under both
the package path (tools.<mod>) and the top-level path. No import-time cycle.
__all__ lists every moved name for total re-export."""
import argparse
import os
import re
from typing import Any

try:  # package path (tools.run_locomo_ingest_once)
    from .run_locomo_ingest_once import (
        clamp_percent,
    )
except ImportError:  # top-level path (run_locomo_ingest_once)
    from run_locomo_ingest_once import (
        clamp_percent,
    )

__all__ = ['benchmark_threshold_violations', 'benchmark_model_contract', 'reader_max_tokens_from_env', 'shared_oss_model_contract_is_required', 'normalized_model_name', 'title_answer', 'weak_category_reasons', 'paper_comparable_claim_ready']


def benchmark_threshold_violations(
    *,
    case_count: int,
    hit_rate: float,
    reader_hit_rate: float,
    context_answer_coverage: float,
    token_reduction: float,
    retrieval_p95: float,
    reader_p95: float,
    open_source_calls: int,
    model_contract: dict[str, Any],
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
    if thresholds.get("require_shared_oss_models") and not model_contract.get("shared_oss_model_contract_passed"):
        violations.append("shared_oss_model_contract_not_satisfied")
    return violations


def benchmark_model_contract(args: argparse.Namespace, reader: "BenchmarkReader") -> dict[str, Any]:
    matrixark_provider = str(reader.config.provider_name or "").strip()
    matrixark_reader = str(reader.config.model or "").strip()
    matrixark_embedding = str(args.embedding_model or "").strip()
    baseline_provider = str(args.baseline_provider_name or "").strip()
    baseline_reader = str(args.baseline_reader_model or "").strip()
    baseline_embedding = str(args.baseline_embedding_model or "").strip()
    baseline_max_events = int(args.baseline_max_events or 0)
    baseline_reader_max_context_chars = int(args.baseline_reader_max_context_chars or 0)
    matrixark_reader_max_tokens = reader_max_tokens_from_env()
    baseline_reader_max_tokens = int(
        os.environ.get("MATRIXARK_BENCHMARK_BASELINE_READER_MAX_TOKENS")
        or reader_max_tokens_from_env()
    )
    same_session_percent = clamp_percent(float(args.retrieval_same_session_percent))
    cross_session_percent = clamp_percent(float(args.retrieval_cross_session_percent))
    summary_percent = clamp_percent(float(args.retrieval_summary_percent))
    entity_percent = clamp_percent(float(args.retrieval_entity_percent))
    event_percent = clamp_percent(float(args.retrieval_event_percent))
    reader_fallback_allowed = not bool(args.reader_no_fallback)
    reader_matches = bool(baseline_reader) and normalized_model_name(matrixark_reader) == normalized_model_name(baseline_reader)
    embedding_matches = bool(baseline_embedding) and normalized_model_name(matrixark_embedding) == normalized_model_name(
        baseline_embedding
    )
    max_events_matches = baseline_max_events > 0 and int(args.max_events) == baseline_max_events
    reader_budget_matches = (
        baseline_reader_max_context_chars > 0
        and int(reader.config.max_context_chars) == baseline_reader_max_context_chars
    )
    reader_output_budget_matches = (
        baseline_reader_max_tokens > 0 and matrixark_reader_max_tokens == baseline_reader_max_tokens
    )
    provider_declared = bool(matrixark_provider) and bool(baseline_provider)
    return {
        "matrixark_provider_name": matrixark_provider,
        "matrixark_reader_model": matrixark_reader,
        "matrixark_embedding_model": matrixark_embedding,
        "matrixark_encoding_model": matrixark_embedding,
        "matrixark_max_events": int(args.max_events),
        "matrixark_reader_max_context_chars": int(reader.config.max_context_chars),
        "matrixark_reader_max_tokens": matrixark_reader_max_tokens,
        "matrixark_reader_fallback_allowed": reader_fallback_allowed,
        "matrixark_adaptive_max_events": bool(args.adaptive_max_events),
        "matrixark_adaptive_base_max_events": int(args.adaptive_base_max_events)
        if bool(args.adaptive_max_events)
        else 0,
        "matrixark_retrieval_same_session_percent": same_session_percent,
        "matrixark_retrieval_cross_session_percent": cross_session_percent,
        "matrixark_retrieval_summary_percent": summary_percent,
        "matrixark_retrieval_entity_percent": entity_percent,
        "matrixark_retrieval_event_percent": event_percent,
        "baseline_provider_name": baseline_provider,
        "baseline_reader_model": baseline_reader,
        "baseline_embedding_model": baseline_embedding,
        "baseline_encoding_model": baseline_embedding,
        "baseline_max_events": baseline_max_events,
        "baseline_reader_max_context_chars": baseline_reader_max_context_chars,
        "baseline_reader_max_tokens": baseline_reader_max_tokens,
        "baseline_reader_fallback_allowed": reader_fallback_allowed,
        "baseline_adaptive_max_events": bool(args.adaptive_max_events),
        "baseline_adaptive_base_max_events": int(args.adaptive_base_max_events)
        if bool(args.adaptive_max_events)
        else 0,
        "baseline_retrieval_same_session_percent": same_session_percent,
        "baseline_retrieval_cross_session_percent": cross_session_percent,
        "baseline_retrieval_summary_percent": summary_percent,
        "baseline_retrieval_entity_percent": entity_percent,
        "baseline_retrieval_event_percent": event_percent,
        "provider_identity_declared": provider_declared,
        "reader_model_match": reader_matches,
        "embedding_model_match": embedding_matches,
        "max_events_match": max_events_matches,
        "reader_context_budget_match": reader_budget_matches,
        "reader_output_budget_match": reader_output_budget_matches,
        "shared_oss_model_contract_required": bool(args.require_shared_oss_models),
        "shared_oss_models_forced": bool(args.require_shared_oss_models),
        "same_oss_reader_model_forced": bool(args.require_shared_oss_models),
        "same_oss_encoding_model_forced": bool(args.require_shared_oss_models),
        "shared_oss_model_contract_passed": (
            provider_declared
            and reader_matches
            and embedding_matches
            and max_events_matches
            and reader_budget_matches
            and reader_output_budget_matches
        ),
        "comparison_rule": (
            "Paper/comparable claims require MatrixArk, ExternalBaseline/ExternalBaseline, and other baselines "
            "to use the same OSS reader model, embedding/encoding model, retrieval block budget, "
            "adaptive retrieval policy, retrieval budget split, reader context budget, and reader "
            "output-token budget, with the same reader fallback policy. Provider "
            "names are recorded so reports cannot hide which runtime produced each side."
        ),
    }


def reader_max_tokens_from_env() -> int:
    return int(os.environ.get("MATRIXARK_READER_MAX_TOKENS", "160") or "160")


def shared_oss_model_contract_is_required(args: argparse.Namespace) -> bool:
    if bool(getattr(args, "allow_shared_oss_model_drift", False)):
        return False
    return bool(getattr(args, "require_shared_oss_models", True))


def normalized_model_name(value: str) -> str:
    return re.sub(r"[^a-z0-9._:-]+", "", str(value).strip().lower())


def title_answer(value: str) -> str:
    words = []
    for word in re.sub(r"\s+", " ", str(value).strip(" .")).split(" "):
        if not word:
            continue
        if word.lower() in {"a", "an", "the", "at", "in", "of", "for", "and"}:
            words.append(word.lower())
        else:
            words.append(word[:1].upper() + word[1:])
    if words and words[0].lower() in {"a", "an", "the"}:
        words[0] = words[0][:1].upper() + words[0][1:]
    return " ".join(words)


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


def paper_comparable_claim_ready(report: dict[str, Any], thresholds: dict[str, Any]) -> bool:
    if report.get("question_slice_diagnostic"):
        return False
    if report.get("gold_evidence_window_used"):
        return False
    if not report.get("benchmark_threshold_passed"):
        return False
    if not report.get("all_pipelines_use_rust_temporalstore"):
        return False
    if not report.get("rust_temporalstore_backend_ready"):
        return False
    if not report.get("rust_temporalstore_context_event_ingest_ready"):
        return False
    if report.get("rust_temporalstore_direct_source_scoring"):
        return False
    if not report.get("rust_temporalstore_full_dataset_replay_ready"):
        return False
    if int(report.get("reader_open_source_calls") or 0) <= 0:
        return False
    contract = report.get("benchmark_model_contract") or {}
    if not contract.get("shared_oss_model_contract_passed"):
        return False
    return True
