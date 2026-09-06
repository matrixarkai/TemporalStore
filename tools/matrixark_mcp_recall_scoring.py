#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Recall scoring, ranked-path merge, and pack-order helpers for MatrixArk."""

from __future__ import annotations

import re
from typing import Any

try:
    from tools.matrixark_mcp_access_scope import (
        cross_session_rerank_adjustment,
        session_continuity_boost,
        sharing_scope_from_candidate,
    )
    from tools.matrixark_mcp_runtime_config import (
        DEFAULT_BUSINESS_TYPE_WEIGHTS,
        DEFAULT_TIME_DECAY_HALFLIFE_MS,
        DEFAULT_TIME_DECAY_TOLERANCE_MS,
        DEFAULT_BUSINESS_WEIGHT,
        DEFAULT_TIME_WEIGHT,
    )
    from tools.matrixark_mcp_text import token_count
    from tools.matrixark_mcp_validation import integer_arg, optional_object
    from tools.matrixark_mcp_scoring import (
        business_score_for_candidate,
        clamp01,
        final_recall_score,
        time_decay_score,
    )
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_access_scope import (
        cross_session_rerank_adjustment,
        session_continuity_boost,
        sharing_scope_from_candidate,
    )
    from matrixark_mcp_runtime_config import (
        DEFAULT_BUSINESS_TYPE_WEIGHTS,
        DEFAULT_TIME_DECAY_HALFLIFE_MS,
        DEFAULT_TIME_DECAY_TOLERANCE_MS,
        DEFAULT_BUSINESS_WEIGHT,
        DEFAULT_TIME_WEIGHT,
    )
    from matrixark_mcp_text import token_count
    from matrixark_mcp_validation import integer_arg, optional_object
    from matrixark_mcp_scoring import (
        business_score_for_candidate,
        clamp01,
        final_recall_score,
        time_decay_score,
    )


Json = dict[str, Any]


def is_shared_resource_candidate(candidate: Json) -> bool:
    return str(candidate.get("ref_type") or "") == "resource_chunk" and sharing_scope_from_candidate(candidate) in {"tenant_shared", "global_shared"}


def is_shared_skill_candidate(candidate: Json) -> bool:
    return str(candidate.get("ref_type") or "") == "skill_section" and sharing_scope_from_candidate(candidate) in {"tenant_shared", "global_shared"}


def score_recall_candidate(candidate: Json, ranking: Json, *, reference_time_ms: int) -> Json:
    freshness_tolerance_ms = integer_arg(
        ranking,
        "freshness_tolerance_ms",
        DEFAULT_TIME_DECAY_TOLERANCE_MS,
        minimum=0,
    )
    half_life_ms = integer_arg(
        ranking,
        "half_life_ms",
        DEFAULT_TIME_DECAY_HALFLIFE_MS,
        minimum=1,
    )
    type_weights = {**DEFAULT_BUSINESS_TYPE_WEIGHTS, **optional_object(ranking, "business_type_weights")}
    weights = optional_object(ranking, "weights")
    origin_score = clamp01(candidate.get("origin_score"))
    s_time = time_decay_score(
        candidate.get("updated_at_ms"),
        reference_time_ms=reference_time_ms,
        freshness_tolerance_ms=freshness_tolerance_ms,
        half_life_ms=half_life_ms,
    )
    s_busi = business_score_for_candidate(candidate, type_weights)
    question_type = str(candidate.get("question_type") or candidate.get("packing_policy") or "fact")
    continuity_boost = session_continuity_boost(candidate, question_type)
    cross_session_rerank_boost = cross_session_rerank_adjustment(candidate, question_type)
    final_score = clamp01(
        final_recall_score(
            origin_score,
            s_time,
            s_busi,
            weights,
            default_time_weight=DEFAULT_TIME_WEIGHT,
            default_business_weight=DEFAULT_BUSINESS_WEIGHT,
        )
        + continuity_boost
        + cross_session_rerank_boost
    )
    return {
        **candidate,
        "origin_score": origin_score,
        "time_score": s_time,
        "business_score": s_busi,
        "continuity_boost": round(continuity_boost, 6),
        "cross_session_rerank_boost": round(cross_session_rerank_boost, 6),
        "final_score": final_score,
        "score": final_score,
        "ranking_formula": "Sfinal=(1-wtime-wbusi)*Sorigin+wtime*Stime+wbusi*Sbusi+continuity_boost+cross_session_rerank_boost",
    }


def merge_ranked_paths(primary: list[Json], auxiliary: list[Json], *, total_limit: int, auxiliary_quota: int) -> list[Json]:
    selected: list[Json] = []
    seen: set[tuple[str, Any]] = set()

    def take(items: list[Json], limit: int) -> None:
        for item in items:
            key = (str(item.get("ref_type", "")), item.get("ref_hash"))
            if key in seen:
                continue
            selected.append(item)
            seen.add(key)
            if len(selected) >= limit:
                return

    auxiliary_quota = max(0, min(auxiliary_quota, total_limit))
    primary_quota = max(0, total_limit - auxiliary_quota)
    take(primary, primary_quota)
    take(auxiliary, total_limit)
    if len(selected) < total_limit:
        take(primary, total_limit)
    return selected[:total_limit]


def question_type_ref_boost(candidate: Json, question_type: str) -> float:
    ref_type = str(candidate.get("ref_type", ""))
    context_class = str(candidate.get("context_class") or ref_type)
    text = str(candidate.get("text", "")).lower()
    event_type = str(candidate.get("event_type") or candidate.get("entity_type") or candidate.get("topic") or "").lower()
    has_citation = bool(candidate.get("source_ref") or candidate.get("citation") or candidate.get("source_chunk_hash"))
    profile_memory_kind = str(candidate.get("profile_memory_kind") or "").strip().lower()
    if ref_type == "entity" and profile_memory_kind == "codex_outcome" and question_type in {"current_state", "latest", "evidence", "benchmark_quality"}:
        return 0.52
    if ref_type == "entity" and profile_memory_kind in {"durable_profile", "memory_feature"} and question_type == "profile_memory":
        return 0.46
    if question_type == "procedure":
        if ref_type == "skill_section":
            return 0.36
        if context_class in {"resource_fact", "resource_entity_fact"} and re.search(r"(procedure|troubleshoot|debug|rollback|runbook|checklist|alert|fix|remediation|mitigation)", event_type + " " + text):
            return 0.34
        if ref_type == "resource_chunk" and re.search(r"(procedure|troubleshoot|debug|rollback|runbook|checklist|alert|fix|remediation|mitigation)", text):
            return 0.30
        return 0.0
    if question_type == "broad_exploration":
        if ref_type == "summary":
            return 0.35
        if ref_type in {"segment", "compression"}:
            return 0.16
        return 0.02 if ref_type in {"resource_chunk", "event", "entity"} else 0.0
    if ref_type == "compression" and question_type in {"fact", "current_state", "multi_hop"}:
        source_count = len(candidate.get("source_event_ids", []) or [])
        return 0.50 if source_count >= 2 else 0.24
    if question_type in {"current_state", "latest"}:
        if ref_type == "entity" and event_type == "assistant_decision":
            return 0.44
        if ref_type == "entity" and event_type == "tool_evidence":
            return 0.40
        if ref_type == "entity":
            return 0.30
        if context_class == "resource_entity_fact":
            return 0.28
        if context_class == "resource_fact" or "correction" in event_type or "confirmation" in event_type:
            return 0.18
        if ref_type == "resource_chunk" and has_citation:
            return 0.10
        return 0.0
    if question_type == "evidence":
        if ref_type == "resource_chunk" and has_citation:
            return 0.30
        if ref_type == "event":
            return 0.24
        return 0.05 if ref_type == "segment" else 0.0
    if question_type == "date":
        if ref_type == "event" and re.search(r"(20\d{2}|19\d{2}|jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec|monday|tuesday|wednesday|thursday|friday|saturday|sunday|before|after|on)", text):
            return 0.28
        return 0.08 if ref_type == "entity" else 0.0
    if question_type == "multi_hop":
        return 0.14 if ref_type in {"entity", "segment"} else 0.04
    if question_type == "why_emotion":
        return 0.18 if re.search(r"(because|reason|felt|feel|happy|sad|angry|worried|excited|concerned)", text) else 0.0
    if question_type == "fact":
        negated_approval = bool(
            re.search(r"(no|not|without|missing|lacks?|lacked).{0,48}(approval|approved|decision)", text)
            or re.search(r"(approval|approved|decision).{0,48}(no|not|without|missing|lacks?|lacked)", text)
        )
        affirmative_approval = bool(re.search(r"(approved|approval granted|approval confirmed|confirmed approval)", text))
        if negated_approval and not affirmative_approval:
            return -0.12
        if affirmative_approval:
            return 0.38 if ref_type in {"event", "entity"} else 0.26
        if context_class in {"resource_fact", "resource_entity_fact"}:
            return 0.30
        if ref_type in {"entity", "event"}:
            return 0.18
        if ref_type == "resource_chunk" and has_citation:
            return 0.06
        return 0.03
    return 0.0


_CORE_PACKING_SORT_KEY = None


def packing_sort_key(candidate: Json, question_type: str) -> tuple[float, ...]:
    """Return the packing sort key. The one implementation lives in matrixark_mcp_core_packing.

    Delegates deliberately, in the idiom this tree already uses for the same situation (see
    `matrixark_mcp_core_compact.latest_context_state_key`): the target imports matrixark_mcp_core,
    which is far more than this module needs at import time, so it is resolved on first call and
    cached.

    This module used to carry its own copy, and the two had drifted -- in one direction only, with
    the other one gaining three things this never did:

      * the `pending_async` penalty of 0.32. Without it a candidate whose extraction has not
        finished sorted ABOVE settled content: 0.90 against the 0.58 the other packer gave the same
        candidate. matrixark_mcp_budget_pack sorts with this copy and the retrieve path sorts with
        the other, so the two put provisional content in opposite places, on every question type,
        with no flag involved.
      * `MATRIXARK_PACK_RAW_PRECISION`. The flag shifts events up and summaries down for precision
        questions, and it existed in one packer only, so turning it on changed the retrieve
        ordering and left this one alone -- the flag reached half the system.
      * the feature-profile-memory component, which is why the key is five values there and was
        four here. Every caller either sorts by the whole tuple or reads [0], so the extra value
        changes no call site.

    Nothing here says the omissions were meant, and the shape of the difference -- three separate
    additions, all absent -- reads as a copy that stopped receiving them.
    """
    global _CORE_PACKING_SORT_KEY
    delegate = _CORE_PACKING_SORT_KEY
    if delegate is None:
        try:
            from tools.matrixark_mcp_core_packing import packing_sort_key as delegate
        except ImportError:  # Direct script execution from tools/.
            from matrixark_mcp_core_packing import packing_sort_key as delegate
        _CORE_PACKING_SORT_KEY = delegate
    return delegate(candidate, question_type)

def is_resource_or_skill_candidate(candidate: Json) -> bool:
    ref_type = str(candidate.get("ref_type") or "")
    context_class = str(candidate.get("context_class") or "")
    return ref_type in {"resource_chunk", "skill_section"} or context_class in {"resource_fact", "resource_entity_fact"}


#: Carried only when the candidate has them, because they answer a question the always-fields
#: cannot: WHICH budget or floor removed this ref.
#:
#: They were on the copy in matrixark_mcp_core_ref_selection, which delegated to this one on the
#: grounds that the survivor took "the wider rule and the richer record". It took the wider rule.
#: The record was twenty fields poorer, and every reader asking why a ref was dropped got a
#: KeyError instead of a reason.
_EXPLANATORY_FIELDS = (
    "budget_memory_layer",
    "budget_memory_selection_policies",
    "budget_source_role_counts",
    "budget_source_roles",
    "classification",
    "event_type",
    "extraction_mode",
    "extraction_phase",
    "extraction_status",
    "memory_layer_budget_capped_layer",
    "memory_layer_floor_reserved_layer",
    "memory_selection_policy_budget_capped_policies",
    "source_codex_event_counts",
    "source_codex_events",
    "source_hook_type_counts",
    "source_hook_types",
    "source_role",
    "source_role_budget_capped_roles",
    "source_role_counts",
    "source_roles",
)


def dropped_candidate_audit_ref(candidate: Json, *, reason: str, token_estimate: int) -> Json:
    metadata = candidate.get("metadata", {}) if isinstance(candidate.get("metadata"), dict) else {}
    audit_ref = {
        "ref_type": candidate.get("ref_type", ""),
        "ref_hash": candidate.get("ref_hash"),
        "context_class": candidate.get("context_class") or candidate.get("ref_type", ""),
        "drop_reason": reason,
        "reason": reason,
        "score": candidate.get("score", 0.0),
        "origin_score": candidate.get("origin_score", 0.0),
        "packing_score": round(packing_sort_key(candidate, str(candidate.get("packing_policy") or "fact"))[0], 6),
        "token_estimate": token_estimate,
        "token_cost": token_estimate,
        "raw_uri": candidate.get("raw_uri", ""),
        "source_ref": candidate.get("source_ref", ""),
        "citation": candidate.get("citation") or candidate.get("source_ref", ""),
        "resource_type": candidate.get("resource_type", ""),
        "resource_version": candidate.get("resource_version") or metadata.get("resource_version", ""),
        "version_state": candidate.get("version_state", "current"),
        "stale_or_superseded": bool(candidate.get("stale_or_superseded", False)),
        "session_continuity": candidate.get("session_continuity", ""),
        "memory_scope": candidate.get("memory_scope", ""),
        "entity_type": candidate.get("entity_type") or metadata.get("entity_type", ""),
        "entity_name": candidate.get("entity_name") or metadata.get("entity_name", ""),
        "profile_shadowed_by_ref_hash": candidate.get("profile_shadowed_by_ref_hash"),
        "profile_shadowed_reason": candidate.get("profile_shadowed_reason", ""),
        "access_decision": candidate.get("access_decision", "allowed_by_scope"),
        "selection_reason": candidate.get("selection_reason", ""),
        "matched_index_terms": candidate.get("matched_index_terms", []),
        "node_hash": candidate.get("node_hash"),
        "node_path": candidate.get("node_path", []),
    }
    # Absent rather than empty: a reader distinguishes "this budget did not act" from "this budget
    # capped nothing", and a key present with "" says the second when the first is true.
    for field in _EXPLANATORY_FIELDS:
        value = candidate.get(field)
        if value not in (None, "", [], {}):
            audit_ref[field] = value
    return audit_ref


#: A candidate whose drop is worth explaining. Taken from
#: matrixark_mcp_core_ref_selection, which recorded these while this module recorded only a
#: resource/skill candidate or a stale entity -- so an audit produced here counted drops it never
#: explained, `cross_session_budget: 2` beside `refs: []`.
MEMORY_CANDIDATE_REF_TYPES = {
    "event",
    "entity",
    "segment",
    "summary",
    "compression",
    "resource_chunk",
    "skill_section",
}
MEMORY_CANDIDATE_CONTEXT_CLASSES = {
    "raw_event",
    "entity_state",
    "assistant_decision",
    "tool_evidence",
    "summary",
    "compression",
    "resource_fact",
    "resource_entity_fact",
}


def record_dropped_candidate(dropped: Json, candidate: Json, *, reason: str, token_estimate: int) -> None:
    """Record WHICH ref was dropped, not only that one was.

    The rule is the wider of the two this replaces and the record is the richer, because the two
    had drifted in opposite directions and neither was complete: that one recorded more candidate
    kinds, this one recorded more fields per candidate.

    `reason == "stale"` on an entity is kept as its own clause even though an entity is now a
    memory candidate anyway -- it was the one case the narrow rule went out of its way to include,
    and dropping the clause would rely on the set above continuing to contain "entity".
    """
    ref_type = str(candidate.get("ref_type") or "")
    context_class = str(candidate.get("context_class") or "")
    is_memory_candidate = (ref_type in MEMORY_CANDIDATE_REF_TYPES
                           or context_class in MEMORY_CANDIDATE_CONTEXT_CLASSES)
    if not is_memory_candidate and not is_resource_or_skill_candidate(candidate) and not (
        reason == "stale" and ref_type == "entity"
    ):
        return
    dropped.setdefault("refs", []).append(dropped_candidate_audit_ref(candidate, reason=reason, token_estimate=token_estimate))


def diversify_for_question_type(candidates: list[Json], question_type: str, *, total_limit: int) -> list[Json]:
    if question_type == "broad_exploration":
        summary = next((candidate for candidate in candidates if candidate.get("ref_type") == "summary"), None)
        if summary is None:
            return candidates[:total_limit]
        selected = [summary]
        selected.extend(candidate for candidate in candidates if candidate is not summary)
        return selected[:total_limit]
    if question_type != "multi_hop":
        return candidates[:total_limit]
    selected: list[Json] = []
    deferred: list[Json] = []
    seen_nodes: set[Any] = set()
    for candidate in candidates:
        node_hash = candidate.get("node_hash")
        if node_hash not in seen_nodes:
            selected.append(candidate)
            seen_nodes.add(node_hash)
        else:
            deferred.append(candidate)
        if len(selected) >= total_limit:
            return selected
    selected.extend(deferred)
    return selected[:total_limit]
