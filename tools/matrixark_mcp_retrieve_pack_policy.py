#!/usr/bin/env python3
"""Context-pack policy summary helpers for MatrixArk retrieval."""

from __future__ import annotations

try:
    from tools.matrixark_mcp_core import (
        DEFAULT_TIME_DECAY_HALFLIFE_MS,
        DEFAULT_TIME_DECAY_TOLERANCE_MS,
        Json,
    )
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_core import (
        DEFAULT_TIME_DECAY_HALFLIFE_MS,
        DEFAULT_TIME_DECAY_TOLERANCE_MS,
        Json,
    )


def build_rerank_policy(
    *,
    first_stage_candidate_count: int,
    rerank_candidate_limit: int,
    question_type: str,
    min_similarity_score: float,
    budget_fill_policy: str,
) -> Json:
    return {
        "enabled": True,
        "stage": "packing_rerank",
        "mode": "question_type_token_efficiency",
        "input_candidate_count": first_stage_candidate_count,
        "max_candidates": rerank_candidate_limit,
        "reranked_candidate_count": min(first_stage_candidate_count, rerank_candidate_limit),
        "question_type": question_type,
        "signals": [
            "weighted_recall_score",
            "question_type_ref_boost",
            "cross_session_rerank_boost",
            "token_efficiency",
            "multi_hop_node_diversity",
        ],
        "cross_session_rerank_enabled": True,
        "cross_session_signals": ["entity_state", "resource_fact_citation", "answer_event", "compression", "summary_demotion"],
        "fallback": "weighted_recall",
        "heavy_rerank_enabled": False,
        "min_similarity_score": min_similarity_score,
        "budget_fill_policy": budget_fill_policy,
    }


def build_time_weighted_recall(
    *,
    ranking: Json,
    selected: list[Json],
    reference_time_ms: int,
) -> Json:
    freshness_tolerance_ms = int(ranking.get("freshness_tolerance_ms", DEFAULT_TIME_DECAY_TOLERANCE_MS))
    half_life_ms = int(ranking.get("half_life_ms", DEFAULT_TIME_DECAY_HALFLIFE_MS))
    selected_time_scores = [float(item.get("time_score", 0.0)) for item in selected if "time_score" in item]
    selected_age_ms: list[int] = []
    for item in selected:
        try:
            selected_age_ms.append(max(0, int(reference_time_ms) - int(item.get("updated_at_ms") or reference_time_ms)))
        except (TypeError, ValueError):
            continue
    return {
        "enabled": True,
        "role": "ranking_prior_not_temporal_compression",
        "score_field": "time_score",
        "formula": "Sfinal=(1-wtime-wbusi)*Sorigin+wtime*Stime+wbusi*Sbusi",
        "freshness_tolerance_ms": freshness_tolerance_ms,
        "half_life_ms": half_life_ms,
        "selected_ref_count": len(selected),
        "avg_selected_time_score": round(sum(selected_time_scores) / len(selected_time_scores), 6) if selected_time_scores else 0.0,
        "min_selected_time_score": round(min(selected_time_scores), 6) if selected_time_scores else 0.0,
        "max_selected_age_ms": max(selected_age_ms) if selected_age_ms else 0,
        "recent_selected_ref_count": sum(1 for age_ms in selected_age_ms if age_ms <= freshness_tolerance_ms),
        "older_selected_ref_count": sum(1 for age_ms in selected_age_ms if age_ms > freshness_tolerance_ms),
    }
