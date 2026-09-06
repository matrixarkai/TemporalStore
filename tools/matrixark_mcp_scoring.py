#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""MatrixArk retrieval scoring helpers."""

from __future__ import annotations

import math
import re
from typing import Any


Json = dict[str, Any]

try:
    from tools.matrixark_mcp_errors import MatrixArkError
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_errors import MatrixArkError


def tokens(text: str) -> list[str]:
    return re.findall(r"[a-z0-9_]+", text.lower())


# Near-duplicate detection lives here rather than beside one of the two packers that need it.
# It was defined in matrixark_mcp_core_ref_selection, which imports matrixark_mcp_core, so the
# other packer -- matrixark_mcp_budget_pack, which the gateway reaches -- could not use it without
# taking that whole dependency at import time. It is built on `tokens`, which this module owns.
def normalized_token_set(text: str) -> frozenset[str]:
    """Lower-cased, de-duplicated token set used for near-duplicate detection."""
    return frozenset(token.lower() for token in tokens(str(text or "")) if token)


def near_duplicate_overlap_ratio(candidate_tokens: frozenset[str], selected_tokens: frozenset[str]) -> float:
    """Jaccard token-set similarity between two refs (|A ∩ B| / |A ∪ B|).

    Jaccard is the near-duplicate metric here (rather than containment) so a
    short ref whose few tokens merely happen to be a subset of a longer but
    genuinely different ref is NOT collapsed: near-duplicate requires the two
    texts to be substantially the same, in both content and size. Ranges 0.0
    (disjoint) .. 1.0 (identical token sets).
    """
    if not candidate_tokens or not selected_tokens:
        return 0.0
    intersection = len(candidate_tokens & selected_tokens)
    if intersection == 0:
        return 0.0
    union = len(candidate_tokens | selected_tokens)
    if union <= 0:
        return 0.0
    return intersection / union


def cosine(left: list[float], right: list[float]) -> float:
    """Cosine similarity, normalised on both sides.

    This used to return the bare dot product, which is the same number while both sides are unit
    vectors and a different one as soon as either is not. Every vector-compaction option produces
    a non-unit vector: int8 divides each vector by its own peak (a NON-uniform rescale, which
    under an unnormalised dot can reorder two stored vectors against one query), and an integer
    scale multiplies uniformly (which cannot reorder, but leaves every score far outside the
    [-1, 1] that normalized_dense_score clamps into, pinning them all to an endpoint).

    Dividing by both norms makes the score invariant to either, so a compacted vector ranks
    exactly where the float it replaced did. For the unit vectors stored today the result is
    unchanged, since a unit vector's dot already IS its cosine.
    """
    if not left or not right or len(left) != len(right):
        return 0.0
    dot = 0.0
    left_norm = 0.0
    right_norm = 0.0
    for a, b in zip(left, right):
        dot += a * b
        left_norm += a * a
        right_norm += b * b
    if left_norm <= 0.0 or right_norm <= 0.0:
        return 0.0
    return round(dot / (math.sqrt(left_norm) * math.sqrt(right_norm)), 6)


def clamp01(value: Any, default: float = 0.0) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError):
        number = default
    return max(0.0, min(1.0, number))


def normalized_dense_score(value: float) -> float:
    return clamp01((value + 1.0) / 2.0)


def sparse_lexical_score(query_terms: set[str], text: str) -> float:
    if not query_terms:
        return 0.0
    matched = len(query_terms.intersection(tokens(text)))
    return clamp01(matched / max(len(query_terms), 1))


def hybrid_origin_score(query_terms: set[str], text: str, embedding_score: float, node_score: float) -> float:
    dense = normalized_dense_score(embedding_score)
    sparse = sparse_lexical_score(query_terms, text)
    node = normalized_dense_score(node_score)
    return round(clamp01(0.55 * dense + 0.35 * sparse + 0.10 * node), 6)


def final_recall_score(
    origin_score: float,
    time_score: float,
    business_score: float,
    weights: Json,
    *,
    default_time_weight: float = 0.18,
    default_business_weight: float = 0.22,
) -> float:
    time_weight = clamp01(weights.get("time", default_time_weight), default_time_weight)
    business_weight = clamp01(weights.get("business", default_business_weight), default_business_weight)
    if time_weight + business_weight > 1.0:
        scale = 1.0 / (time_weight + business_weight)
        time_weight *= scale
        business_weight *= scale
    origin_weight = 1.0 - time_weight - business_weight
    return round(
        origin_weight * origin_score + time_weight * time_score + business_weight * business_score,
        6,
    )


def time_decay_score(
    record_time_ms: Any,
    *,
    reference_time_ms: int,
    freshness_tolerance_ms: int,
    half_life_ms: int,
) -> float:
    try:
        event_time_ms = int(record_time_ms)
    except (TypeError, ValueError):
        return 0.5
    age_ms = max(0, reference_time_ms - event_time_ms)
    if age_ms <= freshness_tolerance_ms:
        return 1.0
    decay_age = age_ms - freshness_tolerance_ms
    half_life_ms = max(1, half_life_ms)
    # Fast initial decay, then slower long-tail decay for durable memories.
    return round(math.exp(-math.sqrt(decay_age / half_life_ms)), 6)


def business_instance_weight(*sources: Json) -> float | None:
    for source in sources:
        if not isinstance(source, dict):
            continue
        for field in ["business_weight", "business_score", "importance", "priority"]:
            if field in source:
                return clamp01(source.get(field))
    return None


def business_type_score(type_name: str, type_weights: Json) -> float:
    if not type_name:
        return 0.5
    normalized = type_name.lower()
    if normalized in type_weights:
        return clamp01(type_weights[normalized], 0.5)
    if "approval" in normalized or "budget" in normalized:
        return 0.9
    if "correction" in normalized or "confirmation" in normalized:
        return 1.0
    if "preference" in normalized or "plan" in normalized or "status" in normalized:
        return 0.75
    return 0.5


def business_score_for_candidate(candidate: Json, type_weights: Json) -> float:
    instance = business_instance_weight(candidate, candidate.get("metadata", {}), candidate.get("scope", {}))
    if instance is not None:
        return instance
    type_name = str(
        candidate.get("event_type")
        or candidate.get("entity_type")
        or candidate.get("topic")
        or candidate.get("ref_type")
        or ""
    )
    return business_type_score(type_name, type_weights)


def numeric_field(record: Json, field: str = "value") -> float | None:
    for source in [record, record.get("metadata", {}), record.get("envelope", {}).get("metadata", {})]:
        if not isinstance(source, dict) or field not in source:
            continue
        try:
            return float(source[field])
        except (TypeError, ValueError):
            return None
    return None


def apply_statistical_operator(operator: str, records: list[Json], *, field: str = "value") -> float | int | None:
    values = [value for record in records if (value := numeric_field(record, field)) is not None]
    op = operator.upper()
    if op == "COUNT":
        return len(records)
    if not values:
        return None
    if op == "SUM":
        return round(sum(values), 6)
    if op == "AVG":
        return round(sum(values) / len(values), 6)
    if op == "MAX":
        return max(values)
    raise MatrixArkError(f"unsupported statistical operator: {operator}")


def latest_record(records: list[Json], *, time_field: str = "updated_at_ms") -> Json | None:
    if not records:
        return None
    return max(records, key=lambda record: int(record.get(time_field) or 0))
