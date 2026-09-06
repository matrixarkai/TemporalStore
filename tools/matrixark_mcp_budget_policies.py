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
        live_float,
        live_int,
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
        live_float,
        live_int,
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
    r"\b(user profile|profile memory|long[- ]term memor(?:y|ies)|cross[- ]session memor(?:y|ies)|profile entit(?:y|ies)|profile summar(?:y|ies)|identity profile|communication profile|workspace profile|mem0|memory feature parity|feature parity|feature[- ]focused memor(?:y|ies)|feature[- ]focused|features? only|features? referring to|focuns on features?|focus(?:ed)? on features?|functionality only|memory functionalit(?:y|ies)|memory algorithms?|memory algos?|no testing|no teseting|no monitoring|no debugging|no evidence|no evident|session memory|remember about me|remember about|what should (?:i|you|we) remember|standing instructions?|standing preferences?|persistent instructions?|saved preferences?|know about (?:me|my|the user)|what (?:have|did) i (?:tell|told) you|what (?:are|were|do|did) my preferences|what do i prefer|do i prefer|my preferences|my .*?(?:policy|policies|instruction|instructions|preference|preferences)|told you before|from previous sessions?|across sessions?|across conversations?|between conversations?|how should (?:you|codex) (?:address|reply|respond|answer)|what (?:is|are) my (?:name|nickname|pronouns?|preferred language|preferred format|communication style|response style|workspace rules?|repo rules?|repository rules?|branch rules?|build rules?|deployment rules?)|what (?:workspace|repo|repository|branch|build|deployment|github|remote) rules? (?:do|should) (?:you|codex) remember|what (?:workflow|workflows|rules?|instructions?|preferences?) (?:do|should) (?:you|codex) follow)\b"
)
FEATURE_MEMORY_QUERY_RE = re.compile(
    r"\b(?:mem0|feature parity|feature[- ]focused|features? only|features? referring to|focuns on features?|focus(?:ed)? on features?|functionalit(?:y|ies)|algorithms?|algos?|memory feature|session memory|profile memory|cross[- ]session memory|long[- ]term memory|threshold|idle batch|batch extraction)\b"
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
    """Delegates to the one implementation, in matrixark_mcp_core_scoring.

    This module used to carry its own copy. The two had drifted: this one matched a single
    profile-memory pattern where the other matches three, and it had no handling for an explicitly
    requested cross-session bridge. Callers reaching this module -- matrixark_mcp_budget_pack --
    therefore behaved differently from callers reaching the other, for no reason anyone chose.

    Comparing them by identifier and then line by line showed this copy held nothing the other
    lacked: every line unique to it was a narrower form of a line over there. So it delegates, and
    the difference disappears rather than being maintained in two places.

    Imported here rather than at module scope: neither module imports the other, and keeping it
    that way avoids a new edge between two modules that both sit low in the import graph.
    """
    try:  # package path
        from tools.matrixark_mcp_core_scoring import (  # type: ignore
            build_cross_session_policy as _build,
        )
    except ImportError:
        from matrixark_mcp_core_scoring import (  # type: ignore
            build_cross_session_policy as _build,
        )
    return _build(
        args,
        ranking,
        question_type=question_type,
        session_scope=session_scope,
        remote_budget_tokens=remote_budget_tokens,
        context_source_mode=context_source_mode,
        # THIS module's binding, looked up now rather than captured at import, so a rebound
        # attribute is seen. Without it the delegation would read the other module's copy and a
        # toggle here would silently stop working -- which it did, changing two budget ratios.
        mode_dependent_quota=MODE_DEPENDENT_QUOTA_ENABLED,
    )


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
    # Read per pack rather than per process: the constants above are the fallback, not the answer.
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
