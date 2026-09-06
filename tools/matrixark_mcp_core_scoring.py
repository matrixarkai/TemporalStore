# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Split out of matrixark_mcp_core.py; re-exported at core end via the dual
relative/absolute import pattern so the same core module object is reused under
both the package path (tools.matrixark_mcp_core) and the top-level path. No
import-time cycle. __all__ lists every moved name for total re-export."""
import math
from typing import Any

try:  # package path (tools.matrixark_mcp_core)
    from .matrixark_mcp_core import (
        ACTIVE_MEMORY_GOAL_QUERY_RE,
        live_float,
        live_int,
        DEFAULT_BUSINESS_WEIGHT,
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
        DEFAULT_TIME_WEIGHT,
        FEATURE_MEMORY_QUERY_RE,
        Json,
        MatrixArkError,
        PROFILE_MEMORY_QUERY_RE,
        PROFILE_MEMORY_STANDING_RULE_QUERY_RE,
        clamp01,
        normalized_dense_score,
        sparse_lexical_score,
    )
except ImportError:  # top-level path (matrixark_mcp_core)
    from matrixark_mcp_core import (
        ACTIVE_MEMORY_GOAL_QUERY_RE,
        live_float,
        live_int,
        DEFAULT_BUSINESS_WEIGHT,
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
        DEFAULT_TIME_WEIGHT,
        FEATURE_MEMORY_QUERY_RE,
        Json,
        MatrixArkError,
        PROFILE_MEMORY_QUERY_RE,
        PROFILE_MEMORY_STANDING_RULE_QUERY_RE,
        clamp01,
        normalized_dense_score,
        sparse_lexical_score,
    )

# Mode-dependent quota knobs come from the leaf runtime_config (no import cycle) rather than
# mcp_core, which re-declares constants by hand and would not have these.
try:
    from tools.matrixark_mcp_runtime_config import (
        DEFAULT_AUGMENT_CROSS_SESSION_BUDGET_RATIO,
        DEFAULT_REMOTE_ONLY_CROSS_SESSION_BUDGET_RATIO,
        MODE_DEPENDENT_QUOTA_ENABLED,
    )
except ImportError:
    from matrixark_mcp_runtime_config import (
        DEFAULT_AUGMENT_CROSS_SESSION_BUDGET_RATIO,
        DEFAULT_REMOTE_ONLY_CROSS_SESSION_BUDGET_RATIO,
        MODE_DEPENDENT_QUOTA_ENABLED,
    )

__all__ = ['passes_secondary_index_filters', 'passes_applicable_secondary_index_filters', 'hybrid_origin_score', 'time_decay_score', 'business_instance_weight', 'business_type_score', 'business_score_for_candidate', 'final_recall_score', 'integer_arg', 'float_arg', 'build_cross_session_policy', 'build_shared_context_policy']


def passes_secondary_index_filters(candidate_terms: set[str], required_groups: list[set[str]], *, mode: str = "all_groups") -> bool:
    if not required_groups:
        return True
    if mode == "any_group":
        return any(bool(candidate_terms.intersection(group)) for group in required_groups)
    return all(bool(candidate_terms.intersection(group)) for group in required_groups)


def passes_applicable_secondary_index_filters(
    candidate_terms: set[str],
    required_groups: list[set[str]],
    *,
    mode: str = "all_groups",
) -> bool:
    """Apply only filter groups whose index prefix is present on this candidate."""
    candidate_prefixes = {term.split(":", 1)[0] for term in candidate_terms if ":" in term}
    candidate_is_context_asset = bool(
        candidate_terms.intersection({"source_type:resource", "source_type:skill"})
    )
    applicable_groups = [
        group
        for group in required_groups
        if candidate_prefixes.intersection({term.split(":", 1)[0] for term in group if ":" in term})
        and not (
            candidate_is_context_asset
            and {term.split(":", 1)[0] for term in group if ":" in term} == {"source_type"}
            and not candidate_terms.intersection(group)
        )
    ]
    return passes_secondary_index_filters(candidate_terms, applicable_groups, mode=mode)



def hybrid_origin_score(query_terms: set[str], text: str, embedding_score: float, node_score: float) -> float:
    dense = normalized_dense_score(embedding_score)
    sparse = sparse_lexical_score(query_terms, text)
    node = normalized_dense_score(node_score)
    return round(clamp01(0.55 * dense + 0.35 * sparse + 0.10 * node), 6)


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


def final_recall_score(origin_score: float, time_score: float, business_score: float, weights: Json) -> float:
    time_weight = clamp01(weights.get("time", DEFAULT_TIME_WEIGHT), DEFAULT_TIME_WEIGHT)
    business_weight = clamp01(weights.get("business", DEFAULT_BUSINESS_WEIGHT), DEFAULT_BUSINESS_WEIGHT)
    if time_weight + business_weight > 1.0:
        scale = 1.0 / (time_weight + business_weight)
        time_weight *= scale
        business_weight *= scale
    origin_weight = 1.0 - time_weight - business_weight
    return round(
        origin_weight * origin_score + time_weight * time_score + business_weight * business_score,
        6,
    )


def integer_arg(data: Json, field: str, default: int, *, minimum: int = 0) -> int:
    value = data.get(field, default)
    if not isinstance(value, int):
        raise MatrixArkError(f"{field} must be an integer")
    if value < minimum:
        raise MatrixArkError(f"{field} must be >= {minimum}")
    return value


def float_arg(data: Json, field: str, default: float, *, minimum: float = 0.0, maximum: float | None = None) -> float:
    value = data.get(field, default)
    if not isinstance(value, (int, float)):
        raise MatrixArkError(f"{field} must be a number")
    result = float(value)
    if result < minimum:
        raise MatrixArkError(f"{field} must be >= {minimum}")
    if maximum is not None and result > maximum:
        raise MatrixArkError(f"{field} must be <= {maximum}")
    return result


def _tenant_profile_max_candidates(args: Json, fallback: int) -> int:
    """This tenant's cross-session PROFILE candidate ceiling, or the build's default.

    Reads the shared explicit-only resolver rather than `resolve()`: the knob registry's default is
    2000 and the value in use here is 48, so resolving would multiply the budget forty-fold for
    every deployment that never configured one.
    """
    try:
        from matrixark_tenant_policy import explicit_int
    except Exception:  # pragma: no cover - policy module absent
        return fallback
    try:
        return explicit_int("cross_session_profile_max_candidates",
                            (args or {}).get("scope"), fallback)
    except Exception:  # pragma: no cover - a malformed policy must not break retrieval
        return fallback


def build_cross_session_policy(args: Json, ranking: Json, *, question_type: str, session_scope: str, remote_budget_tokens: int, context_source_mode: str = "", mode_dependent_quota: bool | None = None) -> Json:
    # `mode_dependent_quota` exists so a delegating caller can supply ITS module's flag. The flag is
    # defined once in matrixark_mcp_runtime_config, but every importer binds its own name, so
    # rebinding one module's attribute -- which is how this behaviour is toggled in tests -- does
    # not reach a reader in another module. Passing it keeps one implementation of the logic while
    # leaving each caller's flag in charge of its own path. None means "use mine".
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
        query_lower
        and (
            PROFILE_MEMORY_QUERY_RE.search(query_lower)
            or PROFILE_MEMORY_STANDING_RULE_QUERY_RE.search(query_lower)
            or ACTIVE_MEMORY_GOAL_QUERY_RE.search(query_lower)
        )
    )
    feature_memory_query = bool(query_lower and (FEATURE_MEMORY_QUERY_RE.search(query_lower) or ACTIVE_MEMORY_GOAL_QUERY_RE.search(query_lower)))
    explicit_cross_session_enabled = "enabled" in config and bool(config.get("enabled"))
    try:
        explicit_bridge_refs = int(config.get("min_entity_bridge_refs", DEFAULT_CROSS_SESSION_MIN_ENTITY_BRIDGE_REFS) or 0)
    except (TypeError, ValueError):
        explicit_bridge_refs = DEFAULT_CROSS_SESSION_MIN_ENTITY_BRIDGE_REFS
    explicit_profile_bridge_requested = bool(explicit_cross_session_enabled and explicit_bridge_refs > 0)
    cross_session_query = normalized_question_type in {
        "current_state",
        "latest",
        "multi_hop",
        "date",
        "broad_exploration",
        "evidence",
        "benchmark_quality",
    }
    cross_session_allowed = (
        session_scope == "prefer"
        or profile_memory_query
        or feature_memory_query
        or cross_session_query
        or explicit_profile_bridge_requested
    )
    default_enabled = cross_session_allowed and remote_budget_tokens > 0
    enabled = bool(config.get("enabled", default_enabled)) and cross_session_allowed and remote_budget_tokens > 0
    profile_budget_query = profile_memory_query or feature_memory_query
    if profile_budget_query:
        default_ratio = live_float("MATRIXARK_CROSS_SESSION_PROFILE_BUDGET_RATIO",
                                   DEFAULT_CROSS_SESSION_PROFILE_BUDGET_RATIO)
        question_budget_reason = "profile_memory_queries_need_long_term profile and cross-session state"
    elif normalized_question_type in {"current_state", "latest"}:
        default_ratio = DEFAULT_CROSS_SESSION_CURRENT_STATE_BUDGET_RATIO
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
        default_ratio = live_float("MATRIXARK_CROSS_SESSION_BUDGET_RATIO",
                                   DEFAULT_CROSS_SESSION_BUDGET_RATIO)
        question_budget_reason = "normal_queries_keep_cross_session_small so current session/resources/skills dominate"
    # Mode-dependent quota (opt-in). Augment: local carries the current session, so route the
    # memory budget to cross-session + long-term profile. Remote-only: remote reconstructs the
    # working context too, so cross-session takes the minority. OFF by default (legacy ratios).
    _mode = str(context_source_mode or "").strip().lower()
    _mode_quota = (MODE_DEPENDENT_QUOTA_ENABLED if mode_dependent_quota is None
                   else bool(mode_dependent_quota))
    if _mode_quota and _mode == "local_and_remote":
        default_ratio = max(default_ratio, DEFAULT_AUGMENT_CROSS_SESSION_BUDGET_RATIO)
        question_budget_reason = "augment_mode_routes_memory_budget_to_cross_session_and_profile_local_carries_current_session"
    elif _mode_quota and _mode == "remote_only":
        default_ratio = DEFAULT_REMOTE_ONLY_CROSS_SESSION_BUDGET_RATIO
        question_budget_reason = "remote_only_reserves_majority_of_budget_for_current_session_reconstruction"
    default_max_budget_ratio = (
        live_float("MATRIXARK_CROSS_SESSION_PROFILE_MAX_BUDGET_RATIO",
                   DEFAULT_CROSS_SESSION_PROFILE_MAX_BUDGET_RATIO)
        if profile_budget_query
        else live_float("MATRIXARK_CROSS_SESSION_MAX_BUDGET_RATIO",
                        DEFAULT_CROSS_SESSION_MAX_BUDGET_RATIO)
    )
    if _mode_quota and _mode in {"local_and_remote", "remote_only"}:
        # do not let the profile/default max-ratio cap the mode-dependent cross-session allocation
        default_max_budget_ratio = max(default_max_budget_ratio, default_ratio)
    max_budget_ratio = max(0.0, min(1.0, float(config.get("max_budget_ratio", default_max_budget_ratio))))
    budget_ratio = float_arg(config, "budget_ratio", min(default_ratio, max_budget_ratio), minimum=0.0, maximum=max_budget_ratio)
    # Read per pack, so raising a ceiling applies to the next retrieve rather than the next
    # restart. The constant is the fallback.
    max_budget_default = (
        live_int("MATRIXARK_CROSS_SESSION_PROFILE_MAX_BUDGET_TOKENS",
                 DEFAULT_CROSS_SESSION_PROFILE_MAX_BUDGET_TOKENS)
        if profile_budget_query
        else live_int("MATRIXARK_CROSS_SESSION_MAX_BUDGET_TOKENS",
                      DEFAULT_CROSS_SESSION_MAX_BUDGET_TOKENS)
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
    # The last of the tenant knobs that resolved correctly and was read by nobody. It applies
    # only to the PROFILE budget, which is what it is named for; the non-profile default is
    # untouched. Explicit-only precedence, so a deployment that configured nothing does not move --
    # the registry default for this knob is 2000 against the 48 used here.
    max_candidates_default = (
        _tenant_profile_max_candidates(args, DEFAULT_CROSS_SESSION_PROFILE_MAX_CANDIDATES)
        if profile_budget_query
        else DEFAULT_CROSS_SESSION_MAX_CANDIDATES
    )
    min_bridge_default = (
        DEFAULT_CROSS_SESSION_PROFILE_MIN_ENTITY_BRIDGE_REFS
        if profile_budget_query
        else (1 if enabled and session_scope == "prefer" and remote_budget_tokens > 0 else DEFAULT_CROSS_SESSION_MIN_ENTITY_BRIDGE_REFS)
    )
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
            else "allow_durable_profile_bridge_inside_session_only_scope"
            if enabled and explicit_profile_bridge_requested and session_scope == "only"
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
    # Read per pack, like the ceilings below: a share of the next pack is decided when the next
    # pack is built. The constants are the fallback, not the answer.
    resource_max_budget_ratio = float_arg(
        config,
        "resource_max_budget_ratio",
        live_float("MATRIXARK_SHARED_RESOURCE_MAX_BUDGET_RATIO",
                   DEFAULT_SHARED_RESOURCE_MAX_BUDGET_RATIO),
        minimum=0.0,
        maximum=1.0,
    )
    skill_max_budget_ratio = float_arg(
        config,
        "skill_max_budget_ratio",
        live_float("MATRIXARK_SHARED_SKILL_MAX_BUDGET_RATIO",
                   DEFAULT_SHARED_SKILL_MAX_BUDGET_RATIO),
        minimum=0.0,
        maximum=1.0,
    )
    resource_budget_ratio = float_arg(
        config,
        "resource_budget_ratio",
        min(live_float("MATRIXARK_SHARED_RESOURCE_BUDGET_RATIO",
                       DEFAULT_SHARED_RESOURCE_BUDGET_RATIO),
            resource_max_budget_ratio),
        minimum=0.0,
        maximum=resource_max_budget_ratio,
    )
    skill_budget_ratio = float_arg(
        config,
        "skill_budget_ratio",
        min(live_float("MATRIXARK_SHARED_SKILL_BUDGET_RATIO",
                       DEFAULT_SHARED_SKILL_BUDGET_RATIO),
            skill_max_budget_ratio),
        minimum=0.0,
        maximum=skill_max_budget_ratio,
    )
    resource_budget_tokens = int(remote_budget_tokens * resource_budget_ratio)
    skill_budget_tokens = int(remote_budget_tokens * skill_budget_ratio)
    if "resource_budget_tokens" in config:
        resource_budget_tokens = integer_arg(config, "resource_budget_tokens", resource_budget_tokens, minimum=0)
    if "skill_budget_tokens" in config:
        skill_budget_tokens = integer_arg(config, "skill_budget_tokens", skill_budget_tokens, minimum=0)
    # Read per pack, exactly as the copy of this in matrixark_mcp_budget_policies does. Making
    # one of the two live and not the other is how a setting comes to work on some requests.
    resource_max = integer_arg(
        config, "resource_max_budget_tokens",
        live_int("MATRIXARK_SHARED_RESOURCE_MAX_BUDGET_TOKENS",
                 DEFAULT_SHARED_RESOURCE_MAX_BUDGET_TOKENS),
        minimum=0)
    skill_max = integer_arg(
        config, "skill_max_budget_tokens",
        live_int("MATRIXARK_SHARED_SKILL_MAX_BUDGET_TOKENS",
                 DEFAULT_SHARED_SKILL_MAX_BUDGET_TOKENS),
        minimum=0)
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


