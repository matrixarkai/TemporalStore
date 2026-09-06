#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Retrieval planning helpers shared by MatrixArk adapters."""

from __future__ import annotations

from dataclasses import dataclass
import os

try:
    from tools.matrixark_mcp_core import (
        DEFAULT_BUDGET_FILL_POLICY,
        DEFAULT_MAX_CANDIDATES_PER_NODE,
        DEFAULT_MAX_CHILDREN_SCORED_PER_PARENT,
        DEFAULT_MAX_GLOBAL_CANDIDATES,
        DEFAULT_MAX_SELECTED_REFS,
        DEFAULT_RETRIEVAL_MIN_SCORE,
        DEFAULT_TOP_K_PER_LAYER,
        PROFILE_MEMORY_QUERY_RE,
        HARD_MAX_CHILDREN_SCORED_PER_PARENT,
        TIME_COMPRESSION_MAX_RAW_EVENTS_PER_NODE,
        Json,
        MatrixArkError,
        bounded_max_children_scored_per_parent,
        build_cross_session_policy,
        build_shared_context_policy,
        build_structured_query_plan,
        clamp01,
        feature_scope_excludes_outcome_evidence,
        float_arg,
        infer_query_type,
        infer_secondary_index_filter_groups,
        integer_arg,
        local_context_budget,
        normalize_message_role,
        now_ms,
        optional_object,
        tokens,
    )
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_core import (
        DEFAULT_BUDGET_FILL_POLICY,
        DEFAULT_MAX_CANDIDATES_PER_NODE,
        DEFAULT_MAX_CHILDREN_SCORED_PER_PARENT,
        DEFAULT_MAX_GLOBAL_CANDIDATES,
        DEFAULT_MAX_SELECTED_REFS,
        DEFAULT_RETRIEVAL_MIN_SCORE,
        DEFAULT_TOP_K_PER_LAYER,
        PROFILE_MEMORY_QUERY_RE,
        HARD_MAX_CHILDREN_SCORED_PER_PARENT,
        TIME_COMPRESSION_MAX_RAW_EVENTS_PER_NODE,
        Json,
        MatrixArkError,
        bounded_max_children_scored_per_parent,
        build_cross_session_policy,
        build_shared_context_policy,
        build_structured_query_plan,
        clamp01,
        feature_scope_excludes_outcome_evidence,
        float_arg,
        infer_query_type,
        infer_secondary_index_filter_groups,
        integer_arg,
        local_context_budget,
        normalize_message_role,
        now_ms,
        optional_object,
        tokens,
    )

try:
    from tools import matrixark_mcp_retrieve_pre_refresh as pre_refresh_helpers
except ModuleNotFoundError:  # Direct script execution from tools/.
    import matrixark_mcp_retrieve_pre_refresh as pre_refresh_helpers

RETRIEVAL_STAGE_NAMES = ["query_understanding", "candidate_fetch", "node_traversal", "rerank_score", "pack", "audit"]


@dataclass(frozen=True)
class RetrievalRankingLimits:
    top_k_per_layer: int
    max_children_scored_per_parent: int
    hard_max_children_scored_per_parent: int
    max_candidates_per_node: int
    max_selected_refs: int
    max_global_candidates: int
    min_similarity_score: float
    budget_fill_policy: str
    max_raw_events_per_node: int


def retrieval_ranking_limits(ranking: Json) -> RetrievalRankingLimits:
    budget_fill_policy = str(
        ranking.get("budget_fill_policy", DEFAULT_BUDGET_FILL_POLICY) or DEFAULT_BUDGET_FILL_POLICY
    ).strip().lower()
    if budget_fill_policy not in {"quality_first", "force_fill"}:
        raise MatrixArkError("budget_fill_policy must be quality_first or force_fill")
    return RetrievalRankingLimits(
        top_k_per_layer=integer_arg(ranking, "top_k_per_layer", DEFAULT_TOP_K_PER_LAYER, minimum=1),
        max_children_scored_per_parent=bounded_max_children_scored_per_parent(
            integer_arg(
                ranking,
                "max_children_scored_per_parent",
                DEFAULT_MAX_CHILDREN_SCORED_PER_PARENT,
                minimum=1,
            )
        ),
        hard_max_children_scored_per_parent=max(1, HARD_MAX_CHILDREN_SCORED_PER_PARENT),
        max_candidates_per_node=integer_arg(
            ranking,
            "max_candidates_per_node",
            DEFAULT_MAX_CANDIDATES_PER_NODE,
            minimum=1,
        ),
        max_selected_refs=integer_arg(ranking, "max_selected_refs", DEFAULT_MAX_SELECTED_REFS, minimum=1),
        max_global_candidates=integer_arg(ranking, "max_global_candidates", DEFAULT_MAX_GLOBAL_CANDIDATES, minimum=1),
        min_similarity_score=float_arg(
            ranking,
            "min_similarity_score",
            DEFAULT_RETRIEVAL_MIN_SCORE,
            minimum=0.0,
            maximum=1.0,
        ),
        budget_fill_policy=budget_fill_policy,
        max_raw_events_per_node=integer_arg(
            ranking,
            "max_raw_events_per_node",
            TIME_COMPRESSION_MAX_RAW_EVENTS_PER_NODE,
            minimum=1,
        ),
    )


def retrieval_audit_policy(args: Json, default: str = "telemetry_only") -> tuple[str, float]:
    """The audit mode and sample rate for one retrieve.

    `default` is what an unset MATRIXARK_CONTEXT_AUDIT_MODE means for THIS caller, and the retrieve
    paths do not agree on it: the request path and the direct read take telemetry_only, the local
    adapter takes off. That disagreement used to live in a second copy of this function, where the
    only way to notice it was to read both. As an argument it is visible at the call, and changing
    it is a decision made in one place -- it decides whether a retrieve records anything at all,
    since `telemetry_record` is `audit_mode != "off"`.
    """
    audit_mode = str(
        args.get("audit_mode") or os.environ.get("MATRIXARK_CONTEXT_AUDIT_MODE", default)
    ).strip().lower()
    if audit_mode not in {"full", "telemetry_only", "off"}:
        raise MatrixArkError("audit_mode must be full, telemetry_only, or off")
    if "audit_sample_rate" in args:
        raw_audit_sample_rate = args.get("audit_sample_rate")
    elif audit_mode == "full":
        raw_audit_sample_rate = 1.0
    else:
        raw_audit_sample_rate = os.environ.get("MATRIXARK_CONTEXT_AUDIT_SAMPLE_RATE", 0.01)
    try:
        audit_sample_rate = clamp01(float(raw_audit_sample_rate))
    except (TypeError, ValueError):
        raise MatrixArkError("audit_sample_rate must be a number between 0 and 1")
    return audit_mode, audit_sample_rate


def retrieval_deadline_ms(args: Json, ranking: Json) -> int:
    """The deadline THIS request was given, or 0 for none.

    The 0 is a sentinel, not a competing default. It is the answer to "did anyone budget this
    request", and `default_stage_budgets` below reads it that way: at 0 no stage budgets are
    computed and the retrieve runs to completion. The MCP server's own 30000 is a different
    layer -- how long it waits for the tool call before abandoning it -- so the two numbers are
    not two opinions about one value, and a reader that resolved this to 30000 would silently
    turn stage budgeting on for every unbudgeted request.
    """
    raw_deadline_ms = args.get(
        "deadline_ms",
        ranking.get("deadline_ms", os.environ.get("MATRIXARK_RETRIEVAL_TIMEOUT_MS", 0)),
    )
    try:
        return int(raw_deadline_ms or 0)
    except (TypeError, ValueError):
        raise MatrixArkError("deadline_ms must be an integer")


def default_stage_budgets(deadline_ms: int) -> dict[str, int]:
    if deadline_ms > 0:
        return {
            "query_understanding": max(25, int(deadline_ms * 0.15)),
            "candidate_fetch": max(25, int(deadline_ms * 0.20)),
            "node_traversal": max(25, int(deadline_ms * 0.15)),
            "rerank_score": max(25, int(deadline_ms * 0.30)),
            "pack": max(25, int(deadline_ms * 0.15)),
            "audit": max(10, int(deadline_ms * 0.05)),
        }
    return {
        "query_understanding": 500,
        "candidate_fetch": 750,
        "node_traversal": 500,
        "rerank_score": 1000,
        "pack": 500,
        "audit": 250,
    }


def retrieval_stage_budgets(args: Json, ranking: Json, *, deadline_ms: int) -> tuple[dict[str, int], Json]:
    explicit_stage_budgets = optional_object(args, "stage_budgets_ms") or optional_object(ranking, "stage_budgets_ms")
    defaults = default_stage_budgets(deadline_ms)
    stage_budgets_ms: dict[str, int] = {}
    for stage in RETRIEVAL_STAGE_NAMES:
        value = explicit_stage_budgets.get(stage, ranking.get(f"{stage}_budget_ms", defaults[stage]))
        if not isinstance(value, int) or value < 0:
            raise MatrixArkError(f"stage budget for {stage} must be a non-negative integer")
        stage_budgets_ms[stage] = value
    return stage_budgets_ms, explicit_stage_budgets


def stage_budget_snapshot(
    *,
    stage_budgets_ms: dict[str, int],
    stage_latencies_ms: dict[str, float],
    explicit_stage_budgets: Json,
    deadline_ms: int,
) -> Json:
    stages = {
        stage: {
            "budget_ms": stage_budgets_ms[stage],
            "elapsed_ms": round(float(stage_latencies_ms.get(stage, 0.0)), 3),
            "over_budget": bool(
                stage_budgets_ms[stage] > 0
                and float(stage_latencies_ms.get(stage, 0.0)) > stage_budgets_ms[stage]
            ),
        }
        for stage in RETRIEVAL_STAGE_NAMES
    }
    return {
        "enabled": True,
        "source": "explicit" if explicit_stage_budgets else ("deadline_derived" if deadline_ms > 0 else "defaults"),
        "stages": stages,
        "over_budget_stages": [stage for stage, row in stages.items() if row["over_budget"]],
    }


FEATURE_SCOPE_EXCLUDED_MEMORY_LAYERS = {
    "pending_async_codex_outcome_event",
    "same_session_codex_outcome_event",
    "cross_session_codex_outcome_event",
    "same_session_codex_outcome_segment",
    "cross_session_codex_outcome_segment",
    "cross_session_codex_outcome_entity",
    "cross_session_codex_outcome_summary",
    "cross_session_codex_outcome_compression",
}

FEATURE_SCOPE_EXCLUDED_MEMORY_SELECTION_POLICIES = {
    "selected_tool_evidence_only",
    "selected_assistant_decision_outcome_only",
}

FEATURE_SCOPE_EXCLUDED_SOURCE_ROLES = {
    "tool",
}


def feature_scope_excludes_evidence(query: str) -> bool:
    return feature_scope_excludes_outcome_evidence(query)


def prune_feature_scope_evidence_budgets(
    *,
    query: str,
    source_role_budget_tokens: Json,
    memory_layer_budget_tokens: Json,
    memory_selection_policy_budget_tokens: Json,
) -> tuple[Json, Json, Json]:
    if not feature_scope_excludes_evidence(query):
        return source_role_budget_tokens, memory_layer_budget_tokens, memory_selection_policy_budget_tokens
    pruned_source_roles: Json = {}
    for role, tokens in (source_role_budget_tokens or {}).items():
        role_name = normalize_message_role(role)
        if role_name in FEATURE_SCOPE_EXCLUDED_SOURCE_ROLES:
            pruned_source_roles[role_name] = 0
        else:
            pruned_source_roles[role] = tokens
    for role in FEATURE_SCOPE_EXCLUDED_SOURCE_ROLES:
        pruned_source_roles.setdefault(role, 0)

    pruned_memory_layers: Json = {}
    for layer, tokens in (memory_layer_budget_tokens or {}).items():
        layer_name = str(layer or "").strip().lower()
        if layer_name in FEATURE_SCOPE_EXCLUDED_MEMORY_LAYERS:
            pruned_memory_layers[layer_name] = 0
        else:
            pruned_memory_layers[layer] = tokens
    for layer in FEATURE_SCOPE_EXCLUDED_MEMORY_LAYERS:
        pruned_memory_layers.setdefault(layer, 0)

    pruned_selection_policies: Json = {}
    for policy, tokens in (memory_selection_policy_budget_tokens or {}).items():
        policy_name = str(policy or "").strip()
        if policy_name in FEATURE_SCOPE_EXCLUDED_MEMORY_SELECTION_POLICIES:
            pruned_selection_policies[policy_name] = 0
        else:
            pruned_selection_policies[policy] = tokens
    for policy in FEATURE_SCOPE_EXCLUDED_MEMORY_SELECTION_POLICIES:
        pruned_selection_policies.setdefault(policy, 0)
    return pruned_source_roles, pruned_memory_layers, pruned_selection_policies


def retrieval_query_budget_plan(
    args: Json,
    ranking: Json,
    *,
    query: str,
    scope: Json,
    default_max_context_tokens: int,
) -> Json:
    question_type = str(args.get("question_type") or infer_query_type(query))
    if PROFILE_MEMORY_QUERY_RE.search(str(query or "").lower()):
        question_type = "profile_memory"
    retrieval_session_scope = str(args.get("session_scope") or ranking.get("session_scope") or "prefer").strip().lower()
    if retrieval_session_scope not in {"prefer", "only"}:
        raise MatrixArkError("session_scope must be prefer or only")
    retrieval_scope = {**scope, "_session_scope": retrieval_session_scope}
    secondary_index_filter_groups = infer_secondary_index_filter_groups(query, question_type)
    secondary_index_filter_mode = "any_group" if len(secondary_index_filter_groups) > 1 else "all_groups"
    budget_source = (
        "agent_provided_max_context_tokens"
        if "max_context_tokens" in args
        else "matrixark_default_max_context_tokens"
    )
    max_context_tokens = args.get("max_context_tokens", default_max_context_tokens)
    if not isinstance(max_context_tokens, int) or max_context_tokens <= 0:
        raise MatrixArkError("max_context_tokens must be a positive integer")
    local_budget = local_context_budget(args)
    local_tokens = int(local_budget.get("token_estimate", 0))
    safety_margin_tokens = int(local_budget.get("safety_margin_tokens", 0))
    remote_context_budget_tokens = max(0, max_context_tokens - local_tokens - safety_margin_tokens)
    local_budget["remote_budget_tokens"] = remote_context_budget_tokens
    cross_session_policy = build_cross_session_policy(
        args,
        ranking,
        question_type=question_type,
        session_scope=retrieval_session_scope,
        remote_budget_tokens=remote_context_budget_tokens,
    )
    shared_context_policy = build_shared_context_policy(
        args,
        ranking,
        remote_budget_tokens=remote_context_budget_tokens,
    )
    source_role_budget_tokens = optional_object(args, "source_role_budget_tokens") or optional_object(ranking, "source_role_budget_tokens")
    source_role_budget_mode = "explicit" if source_role_budget_tokens else ""
    if not source_role_budget_tokens:
        source_role_budget_tokens, source_role_budget_mode = pre_refresh_helpers.auto_source_role_budget_tokens(
            args,
            ranking,
            remote_budget_tokens=remote_context_budget_tokens,
            question_type=question_type,
        )
    memory_layer_budget_tokens = optional_object(args, "memory_layer_budget_tokens") or optional_object(ranking, "memory_layer_budget_tokens")
    memory_layer_budget_mode = "explicit" if memory_layer_budget_tokens else ""
    if not memory_layer_budget_tokens:
        memory_layer_budget_tokens, memory_layer_budget_mode = pre_refresh_helpers.auto_memory_layer_budget_tokens(
            args,
            ranking,
            remote_budget_tokens=remote_context_budget_tokens,
            question_type=question_type,
        )
    if pre_refresh_helpers.pre_retrieval_summary_refresh_enabled(args, ranking) and not memory_layer_budget_tokens:
        memory_layer_budget_tokens, memory_layer_budget_mode = pre_refresh_helpers.pre_retrieval_summary_refresh_memory_layer_budget_tokens(
            remote_budget_tokens=remote_context_budget_tokens,
            question_type=question_type,
            args=args,
            ranking=ranking,
        )
    memory_selection_policy_budget_tokens = (
        optional_object(args, "memory_selection_policy_budget_tokens")
        or optional_object(ranking, "memory_selection_policy_budget_tokens")
    )
    memory_selection_policy_budget_mode = "explicit" if memory_selection_policy_budget_tokens else ""
    if not memory_selection_policy_budget_tokens:
        memory_selection_policy_budget_tokens, memory_selection_policy_budget_mode = (
            pre_refresh_helpers.auto_memory_selection_policy_budget_tokens(
                args,
                ranking,
                remote_budget_tokens=remote_context_budget_tokens,
                question_type=question_type,
            )
        )
    extraction_phase_budget_tokens = (
        optional_object(args, "extraction_phase_budget_tokens")
        or optional_object(ranking, "extraction_phase_budget_tokens")
    )
    extraction_phase_budget_mode = "explicit" if extraction_phase_budget_tokens else ""
    if not extraction_phase_budget_tokens:
        extraction_phase_budget_tokens, extraction_phase_budget_mode = (
            pre_refresh_helpers.auto_extraction_phase_budget_tokens(
                args,
                ranking,
                remote_budget_tokens=remote_context_budget_tokens,
                question_type=question_type,
            )
        )
    source_role_budget_tokens, memory_layer_budget_tokens, memory_selection_policy_budget_tokens = prune_feature_scope_evidence_budgets(
        query=query,
        source_role_budget_tokens=source_role_budget_tokens,
        memory_layer_budget_tokens=memory_layer_budget_tokens,
        memory_selection_policy_budget_tokens=memory_selection_policy_budget_tokens,
    )
    raw_reference_time_ms = args.get("reference_time_ms", now_ms())
    if not isinstance(raw_reference_time_ms, int):
        raise MatrixArkError("reference_time_ms must be an integer")
    reference_time_ms = raw_reference_time_ms
    query_plan = build_structured_query_plan(
        query,
        question_type=question_type,
        secondary_index_filter_groups=secondary_index_filter_groups,
        secondary_index_filter_mode=secondary_index_filter_mode,
        reference_time_ms=reference_time_ms,
    )
    return {
        "question_type": question_type,
        "retrieval_session_scope": retrieval_session_scope,
        "retrieval_scope": retrieval_scope,
        "secondary_index_filter_groups": secondary_index_filter_groups,
        "secondary_index_filter_mode": secondary_index_filter_mode,
        "budget_source": budget_source,
        "max_context_tokens": max_context_tokens,
        "local_budget": local_budget,
        "remote_context_budget_tokens": remote_context_budget_tokens,
        "cross_session_policy": cross_session_policy,
        "shared_context_policy": shared_context_policy,
        "source_role_budget_tokens": source_role_budget_tokens,
        "source_role_budget_mode": source_role_budget_mode,
        "memory_layer_budget_tokens": memory_layer_budget_tokens,
        "memory_layer_budget_mode": memory_layer_budget_mode,
        "memory_selection_policy_budget_tokens": memory_selection_policy_budget_tokens,
        "memory_selection_policy_budget_mode": memory_selection_policy_budget_mode,
        "extraction_phase_budget_tokens": extraction_phase_budget_tokens,
        "extraction_phase_budget_mode": extraction_phase_budget_mode,
        "query_terms": {term for term in tokens(query) if len(term) > 2},
        "reference_time_ms": reference_time_ms,
        "query_plan": query_plan,
    }
