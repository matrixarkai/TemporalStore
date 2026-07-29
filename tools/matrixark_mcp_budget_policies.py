#!/usr/bin/env python3
"""Cross-session and shared-context retrieval budget policies."""

from __future__ import annotations

from typing import Any

try:
    from tools.matrixark_mcp_errors import MatrixArkError
    from tools.matrixark_mcp_runtime_config import (
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

    default_enabled = session_scope == "prefer" and remote_budget_tokens > 0
    enabled = bool(config.get("enabled", default_enabled)) and session_scope == "prefer" and remote_budget_tokens > 0
    normalized_question_type = str(question_type or "fact").strip().lower()
    if normalized_question_type in {"current_state", "latest", "profile_memory"}:
        default_ratio = DEFAULT_CROSS_SESSION_CURRENT_STATE_BUDGET_RATIO
        if normalized_question_type == "profile_memory":
            question_budget_reason = "profile_memory_queries_need_long_term profile and cross-session state"
        else:
            question_budget_reason = "current_state_or_latest_queries_need_prior entity state and stale blockers"
    elif normalized_question_type in {"multi_hop", "date"}:
        default_ratio = DEFAULT_CROSS_SESSION_MULTI_HOP_BUDGET_RATIO
        question_budget_reason = "multi_hop_or_date_queries_often_need_multiple sessions"
    elif normalized_question_type in {"broad_exploration", "evidence"}:
        default_ratio = DEFAULT_CROSS_SESSION_BROAD_BUDGET_RATIO
        question_budget_reason = "broad_or_evidence_queries_get_extra cross-session exploration"
    else:
        default_ratio = DEFAULT_CROSS_SESSION_BUDGET_RATIO
        question_budget_reason = "normal_queries_keep_cross_session_small so current session/resources/skills dominate"
    max_budget_ratio = max(0.0, min(1.0, float(config.get("max_budget_ratio", DEFAULT_CROSS_SESSION_MAX_BUDGET_RATIO))))
    budget_ratio = float_arg(config, "budget_ratio", min(default_ratio, max_budget_ratio), minimum=0.0, maximum=max_budget_ratio)
    max_budget_tokens = integer_arg(config, "max_budget_tokens", DEFAULT_CROSS_SESSION_MAX_BUDGET_TOKENS, minimum=0)
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
    max_sessions = integer_arg(config, "max_sessions", DEFAULT_CROSS_SESSION_MAX_SESSIONS, minimum=0)
    max_candidates = integer_arg(config, "max_candidates", DEFAULT_CROSS_SESSION_MAX_CANDIDATES, minimum=0)
    min_entity_bridge_refs = integer_arg(config, "min_entity_bridge_refs", DEFAULT_CROSS_SESSION_MIN_ENTITY_BRIDGE_REFS, minimum=0)
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
        "decision": "always_consider_same_user_cross_session_when_session_scope_prefer" if enabled else "disabled_by_session_scope_or_budget",
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
        "budget_guidance": "cross-session budget is a maximum cap, not a quota: 12% normally, 15% for broad/evidence, 20% for current-state/latest/profile-memory/multi-hop/date; spend it only on high-quality refs, prefer entities/summaries/compressions, and require high-confidence raw events",
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
