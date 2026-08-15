#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Cross-session and shared-context retrieval budget policies."""

from __future__ import annotations

import re
from typing import Any

try:
    from tools.matrixark_mcp_errors import MatrixArkError
    from tools.matrixark_mcp_runtime_config import (
        DEFAULT_AUGMENT_CROSS_SESSION_BUDGET_RATIO,
        DEFAULT_REMOTE_ONLY_CROSS_SESSION_BUDGET_RATIO,
        MODE_DEPENDENT_QUOTA_ENABLED,
        DEFAULT_CROSS_SESSION_BROAD_BUDGET_RATIO,
        DEFAULT_CROSS_SESSION_BUDGET_RATIO,
        DEFAULT_CROSS_SESSION_CURRENT_STATE_BUDGET_RATIO,
        DEFAULT_CROSS_SESSION_MAX_BUDGET_RATIO,
        DEFAULT_CROSS_SESSION_MAX_BUDGET_TOKENS,
        DEFAULT_CROSS_SESSION_MAX_CANDIDATES,
        DEFAULT_CROSS_SESSION_MAX_SESSIONS,
        DEFAULT_CROSS_SESSION_MIN_BUDGET_TOKENS,
        DEFAULT_CROSS_SESSION_MIN_ENTITY_BRIDGE_REFS,
        DEFAULT_CROSS_SESSION_MIN_SCORE,
        DEFAULT_CROSS_SESSION_MULTI_HOP_BUDGET_RATIO,
        DEFAULT_CROSS_SESSION_PARALLELISM,
        DEFAULT_CROSS_SESSION_PREFERRED_REF_TYPES,
        DEFAULT_CROSS_SESSION_PROFILE_BUDGET_RATIO,
        DEFAULT_CROSS_SESSION_PROFILE_MAX_BUDGET_RATIO,
        DEFAULT_CROSS_SESSION_PROFILE_MAX_BUDGET_TOKENS,
        DEFAULT_CROSS_SESSION_PROFILE_MAX_CANDIDATES,
        DEFAULT_CROSS_SESSION_PROFILE_MAX_SESSIONS,
        DEFAULT_CROSS_SESSION_PROFILE_MIN_ENTITY_BRIDGE_REFS,
        DEFAULT_CROSS_SESSION_RAW_EVIDENCE_MIN_SCORE,
        DEFAULT_SHARED_CONTEXT_MIN_SCORE,
        DEFAULT_SHARED_RESOURCE_BUDGET_RATIO,
        DEFAULT_SHARED_RESOURCE_MAX_BUDGET_RATIO,
        DEFAULT_SHARED_RESOURCE_MAX_BUDGET_TOKENS,
        DEFAULT_SHARED_SKILL_BUDGET_RATIO,
        DEFAULT_SHARED_SKILL_MAX_BUDGET_RATIO,
        DEFAULT_SHARED_SKILL_MAX_BUDGET_TOKENS,
        HARD_MAX_CHILDREN_SCORED_PER_PARENT,
    )
    from tools.matrixark_mcp_validation import float_arg, integer_arg
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_errors import MatrixArkError
    from matrixark_mcp_runtime_config import (
        DEFAULT_AUGMENT_CROSS_SESSION_BUDGET_RATIO,
        DEFAULT_REMOTE_ONLY_CROSS_SESSION_BUDGET_RATIO,
        MODE_DEPENDENT_QUOTA_ENABLED,
        DEFAULT_CROSS_SESSION_BROAD_BUDGET_RATIO,
        DEFAULT_CROSS_SESSION_BUDGET_RATIO,
        DEFAULT_CROSS_SESSION_CURRENT_STATE_BUDGET_RATIO,
        DEFAULT_CROSS_SESSION_MAX_BUDGET_RATIO,
        DEFAULT_CROSS_SESSION_MAX_BUDGET_TOKENS,
        DEFAULT_CROSS_SESSION_MAX_CANDIDATES,
        DEFAULT_CROSS_SESSION_MAX_SESSIONS,
        DEFAULT_CROSS_SESSION_MIN_BUDGET_TOKENS,
        DEFAULT_CROSS_SESSION_MIN_ENTITY_BRIDGE_REFS,
        DEFAULT_CROSS_SESSION_MIN_SCORE,
        DEFAULT_CROSS_SESSION_MULTI_HOP_BUDGET_RATIO,
        DEFAULT_CROSS_SESSION_PARALLELISM,
        DEFAULT_CROSS_SESSION_PREFERRED_REF_TYPES,
        DEFAULT_CROSS_SESSION_PROFILE_BUDGET_RATIO,
        DEFAULT_CROSS_SESSION_PROFILE_MAX_BUDGET_RATIO,
        DEFAULT_CROSS_SESSION_PROFILE_MAX_BUDGET_TOKENS,
        DEFAULT_CROSS_SESSION_PROFILE_MAX_CANDIDATES,
        DEFAULT_CROSS_SESSION_PROFILE_MAX_SESSIONS,
        DEFAULT_CROSS_SESSION_PROFILE_MIN_ENTITY_BRIDGE_REFS,
        DEFAULT_CROSS_SESSION_RAW_EVIDENCE_MIN_SCORE,
        DEFAULT_SHARED_CONTEXT_MIN_SCORE,
        DEFAULT_SHARED_RESOURCE_BUDGET_RATIO,
        DEFAULT_SHARED_RESOURCE_MAX_BUDGET_RATIO,
        DEFAULT_SHARED_RESOURCE_MAX_BUDGET_TOKENS,
        DEFAULT_SHARED_SKILL_BUDGET_RATIO,
        DEFAULT_SHARED_SKILL_MAX_BUDGET_RATIO,
        DEFAULT_SHARED_SKILL_MAX_BUDGET_TOKENS,
        HARD_MAX_CHILDREN_SCORED_PER_PARENT,
    )
    from matrixark_mcp_validation import float_arg, integer_arg


Json = dict[str, Any]


PROFILE_MEMORY_QUERY_RE = re.compile(
    r"\b(user profile|profile memory|long[- ]term memor(?:y|ies)|cross[- ]session memor(?:y|ies)|profile entit(?:y|ies)|profile summar(?:y|ies)|identity profile|communication profile|workspace profile|openviking|vikingmem|mem0|memory feature parity|feature parity|feature[- ]focused memor(?:y|ies)|feature[- ]focused|features? only|features? referring to|focuns on features?|focus(?:ed)? on features?|functionality only|memory functionalit(?:y|ies)|memory algorithms?|memory algos?|no testing|no teseting|no monitoring|no debugging|no evidence|no evident|session memory|remember about me|remember about|what should (?:i|you|we) remember|standing instructions?|standing preferences?|persistent instructions?|saved preferences?|know about (?:me|my|the user)|what (?:have|did) i (?:tell|told) you|what (?:are|were|do|did) my preferences|what do i prefer|do i prefer|my preferences|my .*?(?:policy|policies|instruction|instructions|preference|preferences)|told you before|from previous sessions?|across sessions?|across conversations?|between conversations?|how should (?:you|codex) (?:address|reply|respond|answer)|what (?:is|are) my (?:name|nickname|pronouns?|preferred language|preferred format|communication style|response style|workspace rules?|repo rules?|repository rules?|branch rules?|build rules?|deployment rules?)|what (?:workspace|repo|repository|branch|build|deployment|github|remote) rules? (?:do|should) (?:you|codex) remember|what (?:workflow|workflows|rules?|instructions?|preferences?) (?:do|should) (?:you|codex) follow)\b"
)
FEATURE_MEMORY_QUERY_RE = re.compile(
    r"\b(?:openviking|vikingmem|mem0|feature parity|feature[- ]focused|features? only|features? referring to|focuns on features?|focus(?:ed)? on features?|functionalit(?:y|ies)|algorithms?|algos?|memory feature|session memory|profile memory|cross[- ]session memory|long[- ]term memory|threshold|idle batch|batch extraction)\b"
)


def bounded_max_children_scored_per_parent(value: int) -> int:
    hard_cap = max(1, HARD_MAX_CHILDREN_SCORED_PER_PARENT)
    if value > hard_cap:
        raise MatrixArkError(
            "max_children_scored_per_parent must be <= "
            f"{hard_cap}; split over-wide ContextNode children into deeper node layers"
        )
    return value


def build_cross_session_policy(
    args: Json,
    ranking: Json,
    *,
    question_type: str,
    session_scope: str,
    remote_budget_tokens: int,
    context_source_mode: str = "",
) -> Json:
    raw = args.get("cross_session", ranking.get("cross_session", {}))
    if isinstance(raw, bool):
        config: Json = {"enabled": raw}
    elif raw is None:
        config = {}
    elif isinstance(raw, dict):
        config = raw
    else:
        raise MatrixArkError("cross_session must be an object or boolean")

    normalized_question_type = str(question_type or "fact").strip().lower()
    query_text = str(args.get("query") or ranking.get("query") or "")
    query_lower = query_text.lower()
    profile_memory_query = normalized_question_type == "profile_memory" or bool(
        query_lower and PROFILE_MEMORY_QUERY_RE.search(query_lower)
    )
    feature_memory_query = bool(query_lower and FEATURE_MEMORY_QUERY_RE.search(query_lower))
    cross_session_query = normalized_question_type in {
        "current_state",
        "latest",
        "multi_hop",
        "date",
        "broad_exploration",
        "evidence",
        "benchmark_quality",
    }
    cross_session_allowed = session_scope == "prefer" or profile_memory_query or feature_memory_query or cross_session_query
    default_enabled = cross_session_allowed and remote_budget_tokens > 0
    enabled = bool(config.get("enabled", default_enabled)) and cross_session_allowed and remote_budget_tokens > 0
    profile_budget_query = profile_memory_query or feature_memory_query
    if normalized_question_type in {"current_state", "latest", "profile_memory"} or profile_budget_query:
        default_ratio = DEFAULT_CROSS_SESSION_PROFILE_BUDGET_RATIO if profile_budget_query else DEFAULT_CROSS_SESSION_CURRENT_STATE_BUDGET_RATIO
        if profile_budget_query:
            question_budget_reason = "profile_memory_queries_need_long_term profile and cross-session state"
        else:
            question_budget_reason = "current_state_or_latest_queries_need_prior entity state and stale blockers"
    elif normalized_question_type in {"multi_hop", "date"}:
        default_ratio = DEFAULT_CROSS_SESSION_MULTI_HOP_BUDGET_RATIO
        question_budget_reason = (
            "multi_hop_or_date_queries_need cross-session memory for comparisons, timelines, "
            "and facts that may live outside the active session"
        )
    elif normalized_question_type in {"broad_exploration", "evidence"}:
        default_ratio = DEFAULT_CROSS_SESSION_BROAD_BUDGET_RATIO
        question_budget_reason = "broad_or_evidence_queries_get_extra cross-session exploration"
    else:
        default_ratio = DEFAULT_CROSS_SESSION_BUDGET_RATIO
        question_budget_reason = "normal_queries_keep_cross_session_small so current session/resources/skills dominate"
    # Mode-dependent quota (opt-in). Augment: local carries the current session, so route the
    # memory budget to cross-session + long-term profile. Remote-only: remote reconstructs the
    # working context too, so cross-session takes the minority. OFF by default (legacy ratios).
    _mode = str(context_source_mode or "").strip().lower()
    if MODE_DEPENDENT_QUOTA_ENABLED and _mode == "local_and_remote":
        default_ratio = max(default_ratio, DEFAULT_AUGMENT_CROSS_SESSION_BUDGET_RATIO)
        question_budget_reason = "augment_mode_routes_memory_budget_to_cross_session_and_profile_local_carries_current_session"
    elif MODE_DEPENDENT_QUOTA_ENABLED and _mode == "remote_only":
        default_ratio = DEFAULT_REMOTE_ONLY_CROSS_SESSION_BUDGET_RATIO
        question_budget_reason = "remote_only_reserves_majority_of_budget_for_current_session_reconstruction"
    default_max_budget_ratio = DEFAULT_CROSS_SESSION_PROFILE_MAX_BUDGET_RATIO if profile_budget_query else DEFAULT_CROSS_SESSION_MAX_BUDGET_RATIO
    if MODE_DEPENDENT_QUOTA_ENABLED and _mode in {"local_and_remote", "remote_only"}:
        # do not let the profile/default max-ratio cap the mode-dependent cross-session allocation
        default_max_budget_ratio = max(default_max_budget_ratio, default_ratio)
    max_budget_ratio = max(0.0, min(1.0, float(config.get("max_budget_ratio", default_max_budget_ratio))))
    budget_ratio = float_arg(config, "budget_ratio", min(default_ratio, max_budget_ratio), minimum=0.0, maximum=max_budget_ratio)
    max_budget_default = (
        DEFAULT_CROSS_SESSION_PROFILE_MAX_BUDGET_TOKENS
        if profile_budget_query
        else DEFAULT_CROSS_SESSION_MAX_BUDGET_TOKENS
    )
    max_budget_tokens = integer_arg(config, "max_budget_tokens", max_budget_default, minimum=0)
    ratio_budget_cap = int(remote_budget_tokens * max_budget_ratio) if max_budget_ratio > 0 else 0
    max_budget_cap = max_budget_tokens if max_budget_tokens > 0 else remote_budget_tokens
    computed_budget_before_floor = int(remote_budget_tokens * budget_ratio)
    computed_budget = computed_budget_before_floor
    budget_floor_tokens = DEFAULT_CROSS_SESSION_MIN_BUDGET_TOKENS
    budget_floor_applied = False
    budget_floor_eligible = (
        remote_budget_tokens >= 1200
        and computed_budget > 0
        and (ratio_budget_cap == 0 or ratio_budget_cap >= budget_floor_tokens)
        and max_budget_cap >= budget_floor_tokens
    )
    if budget_floor_eligible:
        computed_budget = max(DEFAULT_CROSS_SESSION_MIN_BUDGET_TOKENS, computed_budget)
        budget_floor_applied = computed_budget != computed_budget_before_floor
    budget_floor_status = (
        "floor_applied"
        if budget_floor_applied
        else (
            "remote_budget_too_small_for_profile_floor"
            if enabled
            and remote_budget_tokens > 0
            and computed_budget_before_floor < budget_floor_tokens
            and not budget_floor_eligible
            else "not_needed"
        )
    )
    budget_tokens = integer_arg(config, "budget_tokens", computed_budget, minimum=0) if "budget_tokens" in config else computed_budget
    budget_tokens = min(
        remote_budget_tokens,
        budget_tokens,
        ratio_budget_cap if ratio_budget_cap > 0 else remote_budget_tokens,
        max_budget_tokens if max_budget_tokens > 0 else remote_budget_tokens,
    )
    max_sessions_default = DEFAULT_CROSS_SESSION_PROFILE_MAX_SESSIONS if profile_budget_query else DEFAULT_CROSS_SESSION_MAX_SESSIONS
    max_candidates_default = DEFAULT_CROSS_SESSION_PROFILE_MAX_CANDIDATES if profile_budget_query else DEFAULT_CROSS_SESSION_MAX_CANDIDATES
    min_bridge_default = DEFAULT_CROSS_SESSION_PROFILE_MIN_ENTITY_BRIDGE_REFS if profile_budget_query else DEFAULT_CROSS_SESSION_MIN_ENTITY_BRIDGE_REFS
    max_sessions = integer_arg(config, "max_sessions", max_sessions_default, minimum=0)
    max_candidates = integer_arg(config, "max_candidates", max_candidates_default, minimum=0)
    min_entity_bridge_refs = integer_arg(config, "min_entity_bridge_refs", min_bridge_default, minimum=0)
    parallelism = integer_arg(config, "parallelism", DEFAULT_CROSS_SESSION_PARALLELISM, minimum=1)
    min_score = float_arg(config, "min_score", DEFAULT_CROSS_SESSION_MIN_SCORE, minimum=0.0, maximum=1.0)
    raw_evidence_min_score = float_arg(config, "raw_evidence_min_score", DEFAULT_CROSS_SESSION_RAW_EVIDENCE_MIN_SCORE, minimum=0.0, maximum=1.0)
    preferred_ref_types = config.get("preferred_ref_types", list(DEFAULT_CROSS_SESSION_PREFERRED_REF_TYPES))
    if not isinstance(preferred_ref_types, list):
        raise MatrixArkError("cross_session.preferred_ref_types must be an array")
    preferred_ref_types = [str(item).strip() for item in preferred_ref_types if str(item).strip()]
    return {
        "enabled": enabled,
        "mode": "prefer" if enabled else "disabled",
        "decision": (
            "always_consider_same_user_cross_session_for_profile_memory"
            if enabled and profile_memory_query and session_scope != "prefer"
            else "always_consider_same_user_cross_session_for_feature_memory"
            if enabled and feature_memory_query and session_scope != "prefer"
            else "always_consider_same_user_cross_session_for_query_type"
            if enabled and cross_session_query and session_scope != "prefer"
            else "always_consider_same_user_cross_session_when_session_scope_prefer"
            if enabled
            else "disabled_by_session_scope_or_budget"
        ),
        "question_type": normalized_question_type,
        "question_budget_reason": question_budget_reason,
        "budget_ratio": round(budget_ratio, 6),
        "max_budget_ratio": round(max_budget_ratio, 6),
        "budget_tokens": budget_tokens if enabled else 0,
        "remote_budget_tokens": remote_budget_tokens,
        "computed_budget_tokens": computed_budget_before_floor,
        "budget_floor_tokens": budget_floor_tokens,
        "budget_floor_applied": budget_floor_applied if enabled else False,
        "budget_floor_status": budget_floor_status if enabled else "disabled",
        "max_budget_tokens": max_budget_tokens,
        "max_sessions": max_sessions if enabled else 0,
        "max_candidates": max_candidates if enabled else 0,
        "min_score": min_score if enabled else 0.0,
        "raw_evidence_min_score": raw_evidence_min_score if enabled else 0.0,
        "preferred_ref_types": preferred_ref_types if enabled else [],
        "min_entity_bridge_refs": min_entity_bridge_refs if enabled else 0,
        "parallelism": parallelism if enabled else 0,
        "strategy": "same_session_first_entity_bridge_then_bounded_cross_session",
        "budget_guidance": "cross-session budget is a maximum cap, not a quota: keep normal queries small, raise profile-memory queries for long-term profile state, spend it only on high-quality refs, prefer entities/summaries/compressions, and require high-confidence raw events",
    }


def build_shared_context_policy(args: Json, ranking: Json, *, remote_budget_tokens: int) -> Json:
    raw = args.get("shared_context", ranking.get("shared_context", {}))
    if isinstance(raw, bool):
        config: Json = {"enabled": raw}
    elif raw is None:
        config = {}
    elif isinstance(raw, dict):
        config = raw
    else:
        raise MatrixArkError("shared_context must be an object or boolean")
    enabled = bool(config.get("enabled", True)) and remote_budget_tokens > 0
    resource_max_budget_ratio = float_arg(
        config,
        "resource_max_budget_ratio",
        DEFAULT_SHARED_RESOURCE_MAX_BUDGET_RATIO,
        minimum=0.0,
        maximum=1.0,
    )
    skill_max_budget_ratio = float_arg(
        config,
        "skill_max_budget_ratio",
        DEFAULT_SHARED_SKILL_MAX_BUDGET_RATIO,
        minimum=0.0,
        maximum=1.0,
    )
    resource_budget_ratio = float_arg(
        config,
        "resource_budget_ratio",
        min(DEFAULT_SHARED_RESOURCE_BUDGET_RATIO, resource_max_budget_ratio),
        minimum=0.0,
        maximum=resource_max_budget_ratio,
    )
    skill_budget_ratio = float_arg(
        config,
        "skill_budget_ratio",
        min(DEFAULT_SHARED_SKILL_BUDGET_RATIO, skill_max_budget_ratio),
        minimum=0.0,
        maximum=skill_max_budget_ratio,
    )
    resource_budget_tokens = int(remote_budget_tokens * resource_budget_ratio)
    skill_budget_tokens = int(remote_budget_tokens * skill_budget_ratio)
    if "resource_budget_tokens" in config:
        resource_budget_tokens = integer_arg(config, "resource_budget_tokens", resource_budget_tokens, minimum=0)
    if "skill_budget_tokens" in config:
        skill_budget_tokens = integer_arg(config, "skill_budget_tokens", skill_budget_tokens, minimum=0)
    resource_max = integer_arg(config, "resource_max_budget_tokens", DEFAULT_SHARED_RESOURCE_MAX_BUDGET_TOKENS, minimum=0)
    skill_max = integer_arg(config, "skill_max_budget_tokens", DEFAULT_SHARED_SKILL_MAX_BUDGET_TOKENS, minimum=0)
    resource_ratio_cap = int(remote_budget_tokens * resource_max_budget_ratio) if resource_max_budget_ratio > 0 else 0
    skill_ratio_cap = int(remote_budget_tokens * skill_max_budget_ratio) if skill_max_budget_ratio > 0 else 0
    if resource_ratio_cap == 0 and remote_budget_tokens > 0 and resource_max_budget_ratio > 0:
        resource_ratio_cap = 1
    if skill_ratio_cap == 0 and remote_budget_tokens > 0 and skill_max_budget_ratio > 0:
        skill_ratio_cap = 1
    resource_budget_tokens = min(
        remote_budget_tokens,
        resource_budget_tokens,
        resource_ratio_cap if resource_ratio_cap > 0 else remote_budget_tokens,
        resource_max if resource_max > 0 else remote_budget_tokens,
    )
    skill_budget_tokens = min(
        remote_budget_tokens,
        skill_budget_tokens,
        skill_ratio_cap if skill_ratio_cap > 0 else remote_budget_tokens,
        skill_max if skill_max > 0 else remote_budget_tokens,
    )
    min_score = float_arg(config, "min_score", DEFAULT_SHARED_CONTEXT_MIN_SCORE, minimum=0.0, maximum=1.0)
    return {
        "enabled": enabled,
        "mode": "bounded_shared_context" if enabled else "disabled",
        "decision": "tenant_or_global_shared_resources_and_skills_visible_after_access_scope_then_quota_bounded" if enabled else "disabled_by_budget_or_config",
        "resource_budget_ratio": round(resource_budget_ratio, 6),
        "resource_max_budget_ratio": round(resource_max_budget_ratio, 6),
        "skill_budget_ratio": round(skill_budget_ratio, 6),
        "skill_max_budget_ratio": round(skill_max_budget_ratio, 6),
        "resource_budget_tokens": resource_budget_tokens if enabled else 0,
        "skill_budget_tokens": skill_budget_tokens if enabled else 0,
        "resource_max_budget_tokens": resource_max,
        "skill_max_budget_tokens": skill_max,
        "remote_budget_tokens": remote_budget_tokens,
        "min_score": min_score if enabled else 0.0,
        "visibility_labels": ["tenant_shared", "global_shared"],
        "strategy": "shared_resources_and_skills_live_outside_sessions_and_are_bounded_before_final_pack",
    }
