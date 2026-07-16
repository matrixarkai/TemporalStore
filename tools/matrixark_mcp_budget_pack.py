#!/usr/bin/env python3
"""Token-budget packing helpers for MatrixArk retrieval."""

from __future__ import annotations

import os
import re
from typing import Any, Callable

try:
    from tools.matrixark_mcp_errors import MatrixArkError
    from tools.matrixark_mcp_identity import stable_hash
    from tools.matrixark_mcp_runtime_config import (
        DEFAULT_BUDGET_FILL_POLICY,
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
        DEFAULT_MAX_CONTEXT_TOKENS,
        DEFAULT_MAX_GLOBAL_CANDIDATES,
        DEFAULT_MAX_SELECTED_REFS,
        DEFAULT_RETRIEVAL_MIN_SCORE,
        DEFAULT_SHARED_CONTEXT_MIN_SCORE,
        DEFAULT_SHARED_RESOURCE_BUDGET_RATIO,
        DEFAULT_SHARED_RESOURCE_MAX_BUDGET_TOKENS,
        DEFAULT_SHARED_SKILL_BUDGET_RATIO,
        DEFAULT_SHARED_SKILL_MAX_BUDGET_TOKENS,
    )
    from tools.matrixark_mcp_scoring import tokens
    from tools.matrixark_mcp_text import clip_context_text, token_count
    from tools.matrixark_mcp_validation import float_arg, integer_arg
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_errors import MatrixArkError
    from matrixark_mcp_identity import stable_hash
    from matrixark_mcp_runtime_config import (
        DEFAULT_BUDGET_FILL_POLICY,
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
        DEFAULT_MAX_CONTEXT_TOKENS,
        DEFAULT_MAX_GLOBAL_CANDIDATES,
        DEFAULT_MAX_SELECTED_REFS,
        DEFAULT_RETRIEVAL_MIN_SCORE,
        DEFAULT_SHARED_CONTEXT_MIN_SCORE,
        DEFAULT_SHARED_RESOURCE_BUDGET_RATIO,
        DEFAULT_SHARED_RESOURCE_MAX_BUDGET_TOKENS,
        DEFAULT_SHARED_SKILL_BUDGET_RATIO,
        DEFAULT_SHARED_SKILL_MAX_BUDGET_TOKENS,
    )
    from matrixark_mcp_scoring import tokens
    from matrixark_mcp_text import clip_context_text, token_count
    from matrixark_mcp_validation import float_arg, integer_arg


Json = dict[str, Any]

try:
    from tools.matrixark_mcp_recall_scoring import (
        diversify_for_question_type,
        is_shared_resource_candidate,
        is_shared_skill_candidate,
        merge_ranked_paths,
        packing_sort_key,
        record_dropped_candidate,
    )
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_recall_scoring import (
        diversify_for_question_type,
        is_shared_resource_candidate,
        is_shared_skill_candidate,
        merge_ranked_paths,
        packing_sort_key,
        record_dropped_candidate,
    )


def build_cross_session_policy(args: Json, ranking: Json, *, question_type: str, session_scope: str, remote_budget_tokens: int) -> Json:
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
    if normalized_question_type in {"current_state", "latest"}:
        default_ratio = DEFAULT_CROSS_SESSION_CURRENT_STATE_BUDGET_RATIO
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
    max_budget_default = DEFAULT_CROSS_SESSION_MAX_BUDGET_TOKENS
    max_budget_tokens = integer_arg(config, "max_budget_tokens", max_budget_default, minimum=0)
    computed_budget = int(remote_budget_tokens * budget_ratio)
    if remote_budget_tokens >= 1200 and computed_budget > 0:
        computed_budget = max(DEFAULT_CROSS_SESSION_MIN_BUDGET_TOKENS, computed_budget)
    budget_tokens = integer_arg(config, "budget_tokens", computed_budget, minimum=0) if "budget_tokens" in config else computed_budget
    ratio_budget_cap = int(remote_budget_tokens * max_budget_ratio) if max_budget_ratio > 0 else 0
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
        "max_budget_tokens": max_budget_tokens,
        "max_sessions": max_sessions if enabled else 0,
        "max_candidates": max_candidates if enabled else 0,
        "min_score": min_score if enabled else 0.0,
        "raw_evidence_min_score": raw_evidence_min_score if enabled else 0.0,
        "preferred_ref_types": preferred_ref_types if enabled else [],
        "min_entity_bridge_refs": min_entity_bridge_refs if enabled else 0,
        "parallelism": parallelism if enabled else 0,
        "strategy": "same_session_first_entity_bridge_then_bounded_cross_session",
        "budget_guidance": "cross-session budget is a maximum cap, not a quota: 12% normally, 15% for broad/evidence, 20% for current-state/latest/multi-hop/date; spend it only on high-quality refs, prefer entities/summaries/compressions, and require high-confidence raw events",
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
    resource_budget_ratio = float_arg(
        config,
        "resource_budget_ratio",
        DEFAULT_SHARED_RESOURCE_BUDGET_RATIO,
        minimum=0.0,
        maximum=1.0,
    )
    skill_budget_ratio = float_arg(
        config,
        "skill_budget_ratio",
        DEFAULT_SHARED_SKILL_BUDGET_RATIO,
        minimum=0.0,
        maximum=1.0,
    )
    resource_budget_tokens = int(remote_budget_tokens * resource_budget_ratio)
    skill_budget_tokens = int(remote_budget_tokens * skill_budget_ratio)
    if "resource_budget_tokens" in config:
        resource_budget_tokens = integer_arg(config, "resource_budget_tokens", resource_budget_tokens, minimum=0)
    if "skill_budget_tokens" in config:
        skill_budget_tokens = integer_arg(config, "skill_budget_tokens", skill_budget_tokens, minimum=0)
    resource_max = integer_arg(config, "resource_max_budget_tokens", DEFAULT_SHARED_RESOURCE_MAX_BUDGET_TOKENS, minimum=0)
    skill_max = integer_arg(config, "skill_max_budget_tokens", DEFAULT_SHARED_SKILL_MAX_BUDGET_TOKENS, minimum=0)
    if resource_max > 0:
        resource_budget_tokens = min(resource_budget_tokens, resource_max)
    if skill_max > 0:
        skill_budget_tokens = min(skill_budget_tokens, skill_max)
    min_score = float_arg(config, "min_score", DEFAULT_SHARED_CONTEXT_MIN_SCORE, minimum=0.0, maximum=1.0)
    return {
        "enabled": enabled,
        "mode": "bounded_shared_context" if enabled else "disabled",
        "decision": "tenant_or_global_shared_resources_and_skills_visible_after_access_scope_then_quota_bounded" if enabled else "disabled_by_budget_or_config",
        "resource_budget_ratio": round(resource_budget_ratio, 6),
        "skill_budget_ratio": round(skill_budget_ratio, 6),
        "resource_budget_tokens": resource_budget_tokens if enabled else 0,
        "skill_budget_tokens": skill_budget_tokens if enabled else 0,
        "resource_max_budget_tokens": resource_max,
        "skill_max_budget_tokens": skill_max,
        "remote_budget_tokens": remote_budget_tokens,
        "min_score": min_score if enabled else 0.0,
        "visibility_labels": ["tenant_shared", "global_shared"],
        "strategy": "shared_resources_and_skills_live_outside_sessions_and_are_bounded_before_final_pack",
    }


def context_text_hashes(text: str) -> set[int]:
    compact = " ".join(str(text).split())
    variants = {compact[:512]}
    without_role = re.sub(r"^(user|assistant|tool|system):\s*", "", compact, flags=re.IGNORECASE)
    variants.add(without_role[:512])
    tokenized = tokens(compact)
    if tokenized:
        variants.add(" ".join(tokenized)[:512])
        if tokenized[0] in {"user", "assistant", "tool", "system"}:
            variants.add(" ".join(tokenized[1:])[:512])
    return {stable_hash(variant) for variant in variants if variant}


def local_context_budget(args: Json) -> Json:
    raw_items = args.get("local_context", [])
    if raw_items is None:
        raw_items = []
    if not isinstance(raw_items, list):
        raise MatrixArkError("local_context must be an array")
    items: list[Json] = []
    text_hashes: set[int] = set()
    token_total = 0
    for index, item in enumerate(raw_items):
        if isinstance(item, str):
            text = item
            source = f"local:{index}"
            ref_type = "local_context"
        elif isinstance(item, dict):
            text = str(item.get("text") or item.get("content") or "")
            source = str(item.get("source") or item.get("ref") or f"local:{index}")
            ref_type = str(item.get("ref_type") or "local_context")
        else:
            raise MatrixArkError("local_context items must be strings or objects")
        text = clip_context_text(text)
        if not text:
            continue
        item_tokens = token_count(text)
        token_total += item_tokens
        text_hashes.update(context_text_hashes(text))
        items.append(
            {
                "ref_type": ref_type,
                "source": source,
                "text": text,
                "token_estimate": item_tokens,
                "text_hash": stable_hash(text[:512]),
            }
        )
    explicit_tokens = args.get("local_context_tokens")
    token_source = "estimated_from_local_context"
    if explicit_tokens is not None:
        if not isinstance(explicit_tokens, int) or explicit_tokens < 0:
            raise MatrixArkError("local_context_tokens must be a non-negative integer")
        token_total = max(token_total, explicit_tokens)
        token_source = "agent_provided_local_context_tokens"
    raw_safety_margin = args.get("local_context_safety_margin_tokens")
    if raw_safety_margin is None:
        raw_safety_margin = os.environ.get("MATRIXARK_LOCAL_CONTEXT_SAFETY_MARGIN_TOKENS")
    if raw_safety_margin is None:
        raw_max_context = args.get("max_context_tokens", DEFAULT_MAX_CONTEXT_TOKENS)
        try:
            max_context_tokens = max(0, int(raw_max_context or DEFAULT_MAX_CONTEXT_TOKENS))
        except (TypeError, ValueError):
            max_context_tokens = DEFAULT_MAX_CONTEXT_TOKENS
        safety_margin_tokens = min(512, max_context_tokens // 20)
        safety_margin_source = "matrixark_default_5_percent_capped"
    else:
        try:
            safety_margin_tokens = int(raw_safety_margin or 0)
        except (TypeError, ValueError):
            raise MatrixArkError("local_context_safety_margin_tokens must be a non-negative integer")
        safety_margin_source = "agent_provided_safety_margin" if "local_context_safety_margin_tokens" in args else "env_safety_margin"
    if safety_margin_tokens < 0:
        raise MatrixArkError("local_context_safety_margin_tokens must be a non-negative integer")
    return {
        "items": items,
        "token_estimate": token_total,
        "text_hashes": text_hashes,
        "token_source": token_source,
        "safety_margin_tokens": safety_margin_tokens,
        "safety_margin_source": safety_margin_source,
    }


def compact_local_context_refs(local_budget: Json) -> list[Json]:
    refs: list[Json] = []
    for item in local_budget.get("items", []):
        if not isinstance(item, dict):
            continue
        refs.append(
            {
                "ref_type": item.get("ref_type", "local_context"),
                "source": item.get("source", ""),
                "token_estimate": item.get("token_estimate", 0),
                "text_hash": item.get("text_hash"),
            }
        )
    return refs


def local_context_refs_for_pack(local_budget: Json) -> list[Json]:
    refs: list[Json] = []
    for item in local_budget.get("items", []):
        if not isinstance(item, dict):
            continue
        refs.append(
            {
                "ref_type": item.get("ref_type", "local_context"),
                "source": item.get("source", ""),
                "token_estimate": item.get("token_estimate", 0),
                "text_hash": item.get("text_hash"),
                "text": item.get("text", ""),
                "selection_reason": "provided by agent-visible local context before MatrixArk remote retrieval",
            }
        )
    return refs


def select_token_budgeted_refs(
    primary: list[Json],
    auxiliary: list[Json],
    *,
    max_context_tokens: int,
    auxiliary_quota: int,
    question_type: str = "fact",
    reserved_tokens: int = 0,
    max_selected_refs: int | None = None,
    min_score: float = DEFAULT_RETRIEVAL_MIN_SCORE,
    max_global_candidates: int | None = None,
    budget_fill_policy: str = DEFAULT_BUDGET_FILL_POLICY,
    duplicate_text_hashes: set[int] | None = None,
    deadline_exceeded: Callable[[], bool] | None = None,
    deadline_reason: str = "deadline_during_pack",
    cross_session_policy: Json | None = None,
    shared_context_policy: Json | None = None,
) -> tuple[list[Json], int, Json]:
    duplicate_text_hashes = duplicate_text_hashes or set()
    remote_budget = max(0, max_context_tokens - max(0, reserved_tokens))
    selected_ref_cap = max(1, int(max_selected_refs or DEFAULT_MAX_SELECTED_REFS))
    candidate_pool_limit = max(
        selected_ref_cap,
        max(1, int(max_global_candidates or DEFAULT_MAX_GLOBAL_CANDIDATES)),
    )
    min_score = max(0.0, min(1.0, float(min_score)))
    budget_fill_policy = (budget_fill_policy or DEFAULT_BUDGET_FILL_POLICY).strip().lower()
    candidates = merge_ranked_paths(
        primary,
        auxiliary,
        total_limit=candidate_pool_limit,
        auxiliary_quota=auxiliary_quota,
    )
    candidates.sort(key=lambda item: packing_sort_key(item, question_type), reverse=True)
    candidates = diversify_for_question_type(candidates, question_type, total_limit=candidate_pool_limit)
    selected: list[Json] = []
    used_tokens = 0
    cross_session_policy = cross_session_policy or {"enabled": False, "budget_tokens": 0, "max_sessions": 0, "max_candidates": 0, "min_entity_bridge_refs": 0}
    cross_enabled = bool(cross_session_policy.get("enabled"))
    cross_budget_tokens = int(cross_session_policy.get("budget_tokens") or 0)
    cross_max_sessions = int(cross_session_policy.get("max_sessions") or 0)
    cross_max_candidates = int(cross_session_policy.get("max_candidates") or 0)
    cross_min_score = max(0.0, min(1.0, float(cross_session_policy.get("min_score") or 0.0)))
    cross_raw_evidence_min_score = max(0.0, min(1.0, float(cross_session_policy.get("raw_evidence_min_score") or 0.0)))
    cross_min_entity_bridge_refs = int(cross_session_policy.get("min_entity_bridge_refs") or 0)
    shared_context_policy = shared_context_policy or {"enabled": False, "resource_budget_tokens": 0, "skill_budget_tokens": 0, "min_score": 0.0}
    shared_enabled = bool(shared_context_policy.get("enabled"))
    shared_resource_budget_tokens = int(shared_context_policy.get("resource_budget_tokens") or 0)
    shared_skill_budget_tokens = int(shared_context_policy.get("skill_budget_tokens") or 0)
    shared_min_score = max(0.0, min(1.0, float(shared_context_policy.get("min_score") or 0.0)))
    cross_used_tokens = 0
    cross_selected_ref_count = 0
    entity_bridge_selected_ref_count = 0
    shared_resource_used_tokens = 0
    shared_skill_used_tokens = 0
    shared_resource_selected_ref_count = 0
    shared_skill_selected_ref_count = 0
    selected_cross_sessions: set[str] = set()
    def cross_session_key(candidate: Json) -> str:
        for source in [candidate, candidate.get("access_scope", {}), candidate.get("scope", {}), candidate.get("metadata", {}).get("access_scope", {}) if isinstance(candidate.get("metadata"), dict) else {}]:
            if isinstance(source, dict):
                for field in ["session_id", "scope_key", "node_hash"]:
                    value = source.get(field)
                    if value:
                        return str(value)
        return "unknown_cross_session"

    dropped: Json = {
        "over_budget": 0,
        "duplicate": 0,
        "low_score": 0,
        "stale": 0,
        "summary": 0,
        "raw_l2": 0,
        "cross_session_budget": 0,
        "cross_session_session_cap": 0,
        "cross_session_candidate_cap": 0,
        "shared_resource_budget": 0,
        "shared_skill_budget": 0,
        "deadline": 0,
        "max_selected_refs": 0,
        "estimated_tokens": {
            "over_budget": 0,
            "duplicate": 0,
            "low_score": 0,
            "stale": 0,
            "summary": 0,
            "raw_l2": 0,
            "cross_session_budget": 0,
            "cross_session_session_cap": 0,
            "cross_session_candidate_cap": 0,
            "shared_resource_budget": 0,
            "shared_skill_budget": 0,
            "deadline": 0,
            "max_selected_refs": 0,
        },
        "reason_descriptions": {
            "over_budget": "candidate was relevant but exceeded the remaining remote context token budget",
            "duplicate": "candidate duplicated local context or an already selected ref",
            "low_score": "candidate score was below the minimum packing threshold",
            "stale": "candidate was stale or superseded for the query policy",
            "summary": "summary text was dropped in favor of denser raw/evidence refs",
            "raw_l2": "raw L2 content was dropped because a smaller cited chunk or summary was enough",
            "cross_session_budget": "cross-session candidate exceeded the configured cross-session token budget",
            "cross_session_session_cap": "cross-session candidate came from a session beyond max cross-session session fanout",
            "cross_session_candidate_cap": "cross-session candidate exceeded the configured cross-session candidate cap",
            "shared_resource_budget": "shared resource candidate exceeded the configured shared-resource token budget",
            "shared_skill_budget": "shared skill candidate exceeded the configured shared-skill token budget",
            "deadline": "candidate was not packed because the hard retrieval deadline was reached",
            "max_selected_refs": "candidate was relevant but dropped because max_selected_refs was reached",
        },
        "refs": [],
        "deadline_exceeded": False,
        "deadline_reason": "",
        "min_score": min_score,
        "budget_fill_policy": budget_fill_policy,
    }
    seen_text_hashes: set[int] = set()
    for index, candidate in enumerate(candidates):
        if len(selected) >= selected_ref_cap:
            remaining_candidates = candidates[index:]
            dropped["max_selected_refs"] += len(remaining_candidates)
            for skipped in remaining_candidates:
                skipped_tokens = max(1, token_count(str(skipped.get("text", ""))))
                dropped["estimated_tokens"]["max_selected_refs"] += skipped_tokens
                record_dropped_candidate(dropped, skipped, reason="max_selected_refs", token_estimate=skipped_tokens)
            break
        if deadline_exceeded is not None and deadline_exceeded():
            dropped["deadline_exceeded"] = True
            dropped["deadline_reason"] = deadline_reason
            remaining = max(0, len(candidates) - index)
            dropped["deadline"] += remaining
            for skipped in candidates[index:]:
                skipped_tokens = max(1, token_count(str(skipped.get("text", ""))))
                dropped["estimated_tokens"]["deadline"] += skipped_tokens
                record_dropped_candidate(dropped, skipped, reason="deadline", token_estimate=skipped_tokens)
            break
        ref_tokens = max(1, token_count(str(candidate.get("text", ""))))
        candidate_text_hashes = context_text_hashes(str(candidate.get("text", "")))
        if candidate_text_hashes.intersection(duplicate_text_hashes):
            dropped["duplicate"] += 1
            dropped["estimated_tokens"]["duplicate"] += ref_tokens
            record_dropped_candidate(dropped, candidate, reason="duplicate", token_estimate=ref_tokens)
            continue
        text_hash = stable_hash(str(candidate.get("text", ""))[:512])
        if text_hash in seen_text_hashes:
            dropped["duplicate"] += 1
            dropped["estimated_tokens"]["duplicate"] += ref_tokens
            record_dropped_candidate(dropped, candidate, reason="duplicate", token_estimate=ref_tokens)
            continue
        if float(candidate.get("score", 0.0)) < min_score:
            dropped["low_score"] += 1
            dropped["estimated_tokens"]["low_score"] += ref_tokens
            record_dropped_candidate(dropped, candidate, reason="low_score", token_estimate=ref_tokens)
            continue
        if remote_budget <= 0 or (selected and used_tokens + ref_tokens > remote_budget):
            dropped["over_budget"] += 1
            dropped["estimated_tokens"]["over_budget"] += ref_tokens
            record_dropped_candidate(dropped, candidate, reason="over_budget", token_estimate=ref_tokens)
            continue
        is_cross_session = candidate.get("session_continuity") == "cross_session"
        ref_type = str(candidate.get("ref_type") or "")
        is_entity_bridge = is_cross_session and ref_type == "entity"
        is_cross_session_raw_evidence = is_cross_session and ref_type in {"event", "segment"}
        candidate_score = float(candidate.get("score", 0.0))
        candidate_cross_key = cross_session_key(candidate) if is_cross_session else ""
        if is_cross_session and not cross_enabled:
            dropped["cross_session_budget"] += 1
            dropped["estimated_tokens"]["cross_session_budget"] += ref_tokens
            record_dropped_candidate(dropped, candidate, reason="cross_session_budget", token_estimate=ref_tokens)
            continue
        if is_cross_session and cross_min_score > 0.0 and candidate_score < cross_min_score:
            dropped["low_score"] += 1
            dropped["estimated_tokens"]["low_score"] += ref_tokens
            record_dropped_candidate(dropped, candidate, reason="low_score", token_estimate=ref_tokens)
            continue
        if is_cross_session_raw_evidence and cross_raw_evidence_min_score > 0.0 and candidate_score < cross_raw_evidence_min_score:
            dropped["low_score"] += 1
            dropped["estimated_tokens"]["low_score"] += ref_tokens
            record_dropped_candidate(dropped, candidate, reason="low_score", token_estimate=ref_tokens)
            continue
        if is_cross_session and cross_max_candidates > 0 and cross_selected_ref_count >= cross_max_candidates:
            dropped["cross_session_candidate_cap"] += 1
            dropped["estimated_tokens"]["cross_session_candidate_cap"] += ref_tokens
            record_dropped_candidate(dropped, candidate, reason="cross_session_candidate_cap", token_estimate=ref_tokens)
            continue
        if is_cross_session and cross_max_sessions > 0 and candidate_cross_key not in selected_cross_sessions and len(selected_cross_sessions) >= cross_max_sessions:
            dropped["cross_session_session_cap"] += 1
            dropped["estimated_tokens"]["cross_session_session_cap"] += ref_tokens
            record_dropped_candidate(dropped, candidate, reason="cross_session_session_cap", token_estimate=ref_tokens)
            continue
        if is_cross_session and cross_budget_tokens > 0 and cross_used_tokens + ref_tokens > cross_budget_tokens and not (is_entity_bridge and entity_bridge_selected_ref_count < cross_min_entity_bridge_refs):
            dropped["cross_session_budget"] += 1
            dropped["estimated_tokens"]["cross_session_budget"] += ref_tokens
            record_dropped_candidate(dropped, candidate, reason="cross_session_budget", token_estimate=ref_tokens)
            continue
        is_shared_resource = is_shared_resource_candidate(candidate)
        is_shared_skill = is_shared_skill_candidate(candidate)
        if (is_shared_resource or is_shared_skill) and not shared_enabled:
            reason = "shared_resource_budget" if is_shared_resource else "shared_skill_budget"
            dropped[reason] += 1
            dropped["estimated_tokens"][reason] += ref_tokens
            record_dropped_candidate(dropped, candidate, reason=reason, token_estimate=ref_tokens)
            continue
        if (is_shared_resource or is_shared_skill) and shared_min_score > 0.0 and candidate_score < shared_min_score:
            dropped["low_score"] += 1
            dropped["estimated_tokens"]["low_score"] += ref_tokens
            record_dropped_candidate(dropped, candidate, reason="low_score", token_estimate=ref_tokens)
            continue
        if is_shared_resource and shared_resource_budget_tokens > 0 and shared_resource_used_tokens + ref_tokens > shared_resource_budget_tokens:
            dropped["shared_resource_budget"] += 1
            dropped["estimated_tokens"]["shared_resource_budget"] += ref_tokens
            record_dropped_candidate(dropped, candidate, reason="shared_resource_budget", token_estimate=ref_tokens)
            continue
        if is_shared_skill and shared_skill_budget_tokens > 0 and shared_skill_used_tokens + ref_tokens > shared_skill_budget_tokens:
            dropped["shared_skill_budget"] += 1
            dropped["estimated_tokens"]["shared_skill_budget"] += ref_tokens
            record_dropped_candidate(dropped, candidate, reason="shared_skill_budget", token_estimate=ref_tokens)
            continue
        seen_text_hashes.add(text_hash)
        selected.append(
            {
                **candidate,
                "token_estimate": ref_tokens,
                "packing_score": round(packing_sort_key(candidate, question_type)[0], 6),
                "packing_policy": question_type,
            }
        )
        used_tokens += ref_tokens
        if is_cross_session:
            cross_used_tokens += ref_tokens
            cross_selected_ref_count += 1
            selected_cross_sessions.add(candidate_cross_key)
            if is_entity_bridge:
                entity_bridge_selected_ref_count += 1
        if is_shared_resource_candidate(candidate):
            shared_resource_used_tokens += ref_tokens
            shared_resource_selected_ref_count += 1
        if is_shared_skill_candidate(candidate):
            shared_skill_used_tokens += ref_tokens
            shared_skill_selected_ref_count += 1
        if used_tokens >= remote_budget:
            break
    dropped["cross_session_policy"] = {
        **cross_session_policy,
        "selected_tokens": cross_used_tokens,
        "selected_ref_count": cross_selected_ref_count,
        "selected_session_count": len(selected_cross_sessions),
        "entity_bridge_selected_ref_count": entity_bridge_selected_ref_count,
    }
    dropped["shared_context_policy"] = {
        **shared_context_policy,
        "resource_selected_tokens": shared_resource_used_tokens,
        "skill_selected_tokens": shared_skill_used_tokens,
        "resource_selected_ref_count": shared_resource_selected_ref_count,
        "skill_selected_ref_count": shared_skill_selected_ref_count,
    }
    if not selected and candidates and remote_budget > 0 and budget_fill_policy != "quality_first":
        first = next(
            (
                candidate
                for candidate in candidates
                if not context_text_hashes(str(candidate.get("text", ""))).intersection(duplicate_text_hashes)
            ),
            None,
        )
        if first is None:
            return selected, used_tokens, dropped
        clipped_words = tokens(str(first.get("text", "")))[:remote_budget]
        selected = [{**first, "text": " ".join(clipped_words), "token_estimate": len(clipped_words)}]
        used_tokens = len(clipped_words)
        dropped["over_budget"] = max(0, len(candidates) - 1)
        for candidate in candidates[1:]:
            record_dropped_candidate(
                dropped,
                candidate,
                reason="over_budget",
                token_estimate=max(1, token_count(str(candidate.get("text", "")))),
            )
    return selected, used_tokens, dropped
