#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Pre-retrieval summary refresh and recall budget helpers."""

from __future__ import annotations

try:
    from tools.matrixark_mcp_env import env_bool
except ImportError:  # Direct script execution from tools/.
    from matrixark_mcp_env import env_bool


import os
import re
import time
from typing import Any

try:
    from tools.matrixark_mcp_core import (
        CODEX_OUTCOME_QUERY_RE,
        FEATURE_SCOPE_EXCLUSION_RE,
        PROFILE_MEMORY_QUERY_RE,
        Json,
        access_scope_matches_before_scoring,
        feature_scope_excludes_outcome_evidence,
        now_ms,
        optional_object,
        profile_entity_type_for_memory_text,
    )
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_core import (
        CODEX_OUTCOME_QUERY_RE,
        FEATURE_SCOPE_EXCLUSION_RE,
        PROFILE_MEMORY_QUERY_RE,
        Json,
        access_scope_matches_before_scoring,
        feature_scope_excludes_outcome_evidence,
        now_ms,
        optional_object,
        profile_entity_type_for_memory_text,
    )


def _positive_int_env(name: str, default: int) -> int:
    try:
        return max(1, int(os.environ.get(name, default)))
    except (TypeError, ValueError):
        return default


PRE_RETRIEVAL_SUMMARY_REFRESH = env_bool("MATRIXARK_PRE_RETRIEVAL_SUMMARY_REFRESH", False)
PRE_RETRIEVAL_SUMMARY_REFRESH_LIMIT = _positive_int_env(
    "MATRIXARK_PRE_RETRIEVAL_SUMMARY_REFRESH_LIMIT", 2
)

AUTO_BUDGET_QUERY_TYPES = {
    "current_state",
    "latest",
    "profile_memory",
    "multi_hop",
    "date",
    "broad_exploration",
    "evidence",
    "benchmark_quality",
}

FEATURE_MEMORY_BUDGET_QUERY_RE = re.compile(
    r"\b(?:mem0|feature parity|feature[- ]focused|features? only|features? referring to|focuns on features?|focus(?:ed)? on features?|functionalit(?:y|ies)|algorithms?|memory feature|session memory|profile memory|cross[- ]session memory|long[- ]term memory|threshold|idle batch|batch extraction)\b"
)


def _explicit_cross_session_requested(args: Json, ranking: Json) -> bool:
    raw = args.get("cross_session", ranking.get("cross_session"))
    if isinstance(raw, bool):
        return raw
    if isinstance(raw, dict):
        return bool(raw.get("enabled"))
    return False


def _default_memory_budget_mode(args: Json, ranking: Json, *, field: str, question_type: str) -> str:
    mode = str(args.get(field) or ranking.get(field) or "").strip().lower()
    if mode:
        return mode
    normalized_question_type = str(question_type or "fact").strip().lower()
    if (
        normalized_question_type in AUTO_BUDGET_QUERY_TYPES
        or feature_profile_memory_budget_query(args, ranking, question_type=question_type)
        or _explicit_cross_session_requested(args, ranking)
    ):
        return "auto"
    return ""


def feature_profile_memory_budget_query(args: Json, ranking: Json, *, question_type: str = "fact") -> bool:
    normalized_question_type = str(question_type or "fact").strip().lower()
    if normalized_question_type == "profile_memory":
        return True
    query = str(args.get("query") or ranking.get("query") or "").strip()
    if not query:
        return False
    lower = query.lower()
    return bool(
        PROFILE_MEMORY_QUERY_RE.search(lower)
        or FEATURE_MEMORY_BUDGET_QUERY_RE.search(lower)
        or profile_entity_type_for_memory_text(query) == "memory_feature_profile"
        or (FEATURE_SCOPE_EXCLUSION_RE.search(lower) and "feature" in lower)
    )


def codex_user_goal_budget_query(args: Json, ranking: Json, *, question_type: str = "fact") -> bool:
    normalized_question_type = str(question_type or "fact").strip().lower()
    if normalized_question_type not in {"profile_memory", "current_state", "latest", "multi_hop", "date"}:
        return False
    query = str(args.get("query") or ranking.get("query") or "").strip()
    if not query:
        return False
    lower = query.lower()
    return bool(
        re.search(
            r"\b(?:what|which|show|list|recall|remember|find)\b.{0,80}\b(?:goal|task|plan|requirement|request|asked|ask|instruction|directive)\b",
            lower,
        )
        or re.search(
            r"\b(?:goal|task|plan|requirement|request|instruction|directive)\b.{0,80}\b(?:codex|implement|fix|add|remove|replace|move|build|work)\b",
            lower,
        )
        or re.search(r"\b(?:what did i ask|what have i asked|user asked|user request|current plan)\b", lower)
    )


def codex_outcome_budget_query(args: Json, ranking: Json, *, question_type: str = "fact") -> bool:
    normalized_question_type = str(question_type or "fact").strip().lower()
    if normalized_question_type not in {"evidence", "current_state", "latest", "benchmark_quality"}:
        return False
    query = str(args.get("query") or ranking.get("query") or "").strip()
    if not query:
        return False
    lower = query.lower()
    return bool(
        CODEX_OUTCOME_QUERY_RE.search(lower)
        or re.search(
            r"\b(?:assistant decision|tool evidence|validation evidence|pushed commit|blocked work|next action|what did codex|what was done)\b",
            lower,
        )
    )


def feature_scope_budget_query(args: Json, ranking: Json) -> bool:
    query = str(args.get("query") or ranking.get("query") or ranking.get("question") or "")
    return feature_scope_excludes_outcome_evidence(query)


def auto_source_role_budget_tokens(
    args: Json,
    ranking: Json,
    *,
    remote_budget_tokens: int,
    question_type: str = "fact",
) -> tuple[Json, str]:
    mode = _default_memory_budget_mode(
        args,
        ranking,
        field="source_role_budget_mode",
        question_type=question_type,
    )
    if mode not in {"auto", "balanced", "codex_auto"}:
        return {}, ""
    try:
        remote_budget = max(0, int(remote_budget_tokens or 0))
    except (TypeError, ValueError):
        remote_budget = 0
    if remote_budget <= 0:
        return {}, mode
    fractions = optional_object(args, "source_role_budget_fractions") or optional_object(ranking, "source_role_budget_fractions")
    defaults = {"assistant": 0.45, "tool": 0.35, "user": 0.60}
    normalized_question_type = str(question_type or "fact").strip().lower()
    feature_profile_query = feature_profile_memory_budget_query(args, ranking, question_type=question_type)
    if codex_outcome_budget_query(args, ranking, question_type=question_type):
        defaults.update({"assistant": 0.55, "tool": 0.55, "user": 0.40})
    elif feature_profile_query:
        defaults.update({"assistant": 0.50, "tool": 0.20, "user": 0.70})
    elif codex_user_goal_budget_query(args, ranking, question_type=question_type):
        defaults.update({"assistant": 0.35, "tool": 0.25, "user": 0.70})
    elif normalized_question_type in {"current_state", "latest"}:
        defaults.update({"assistant": 0.50, "tool": 0.40, "user": 0.50})
    elif normalized_question_type == "profile_memory":
        defaults.update({"assistant": 0.50, "tool": 0.45, "user": 0.50})
    elif normalized_question_type == "evidence":
        defaults.update({"assistant": 0.35, "tool": 0.50, "user": 0.45})
    elif normalized_question_type == "benchmark_quality":
        defaults.update({"assistant": 0.50, "tool": 0.60, "user": 0.30})
    elif normalized_question_type in {"broad_exploration", "multi_hop", "date"}:
        defaults.update({"assistant": 0.45, "tool": 0.45, "user": 0.50})
    budgets: Json = {}
    for role, default_fraction in defaults.items():
        raw_fraction = fractions.get(role, default_fraction) if isinstance(fractions, dict) else default_fraction
        try:
            fraction = max(0.0, min(1.0, float(raw_fraction)))
        except (TypeError, ValueError):
            fraction = default_fraction
        budgets[role] = max(1, int(remote_budget * fraction))
    return budgets, mode


def auto_memory_selection_policy_budget_tokens(
    args: Json,
    ranking: Json,
    *,
    remote_budget_tokens: int,
    question_type: str = "fact",
) -> tuple[Json, str]:
    mode = _default_memory_budget_mode(
        args,
        ranking,
        field="memory_selection_policy_budget_mode",
        question_type=question_type,
    )
    if mode not in {"auto", "balanced", "codex_auto"}:
        sibling_mode = str(
            args.get("source_role_budget_mode")
            or ranking.get("source_role_budget_mode")
            or args.get("memory_layer_budget_mode")
            or ranking.get("memory_layer_budget_mode")
            or ""
        ).strip().lower()
        if sibling_mode in {"auto", "balanced", "codex_auto"}:
            mode = sibling_mode
        else:
            return {}, ""
    try:
        remote_budget = max(0, int(remote_budget_tokens or 0))
    except (TypeError, ValueError):
        remote_budget = 0
    if remote_budget <= 0:
        return {}, mode
    fractions = (
        optional_object(args, "memory_selection_policy_budget_fractions")
        or optional_object(ranking, "memory_selection_policy_budget_fractions")
    )
    defaults = {
        "selected_user_prompt": 0.45,
        "selected_user_profile_fact": 0.35,
        "selected_assistant_profile_fact": 0.35,
        "selected_assistant_decision_outcome_only": 0.30,
        "selected_tool_evidence_only": 0.30,
    }
    normalized_question_type = str(question_type or "fact").strip().lower()
    feature_profile_query = feature_profile_memory_budget_query(args, ranking, question_type=question_type)
    if codex_outcome_budget_query(args, ranking, question_type=question_type):
        defaults.update(
            {
                "selected_user_prompt": 0.35,
                "selected_user_profile_fact": 0.45,
                "selected_assistant_profile_fact": 0.35,
                "selected_assistant_decision_outcome_only": 0.58,
                "selected_tool_evidence_only": 0.55,
                "selected_profile_current_state": 0.55,
            }
        )
    elif feature_profile_query:
        defaults.update(
            {
                "selected_user_prompt": 0.70,
                "selected_user_profile_fact": 0.70,
                "selected_assistant_profile_fact": 0.70,
                "selected_assistant_decision_outcome_only": 0.20,
                "selected_tool_evidence_only": 0.20,
                "selected_profile_current_state": 0.55,
            }
        )
    elif codex_user_goal_budget_query(args, ranking, question_type=question_type):
        defaults.update(
            {
                "selected_user_prompt": 0.70,
                "selected_user_profile_fact": 0.55,
                "selected_assistant_profile_fact": 0.45,
                "selected_assistant_decision_outcome_only": 0.30,
                "selected_tool_evidence_only": 0.25,
                "selected_profile_current_state": 0.55,
            }
        )
    elif normalized_question_type in {"current_state", "latest"}:
        defaults.update(
            {
                "selected_user_prompt": 0.40,
                "selected_user_profile_fact": 0.60,
                "selected_assistant_profile_fact": 0.55,
                "selected_assistant_decision_outcome_only": 0.45,
                "selected_tool_evidence_only": 0.30,
                "selected_profile_current_state": 0.50,
            }
        )
    elif normalized_question_type == "profile_memory":
        defaults.update(
            {
                "selected_user_prompt": 0.35,
                "selected_user_profile_fact": 0.70,
                "selected_assistant_profile_fact": 0.65,
                "selected_assistant_decision_outcome_only": 0.40,
                "selected_tool_evidence_only": 0.30,
                "selected_profile_current_state": 0.65,
            }
        )
    elif normalized_question_type == "benchmark_quality":
        defaults.update(
            {
                "selected_user_prompt": 0.25,
                "selected_user_profile_fact": 0.35,
                "selected_assistant_profile_fact": 0.30,
                "selected_assistant_decision_outcome_only": 0.50,
                "selected_tool_evidence_only": 0.65,
                "selected_profile_current_state": 0.40,
            }
        )
    elif normalized_question_type in {"multi_hop", "date", "broad_exploration", "evidence"}:
        defaults.update(
            {
                "selected_user_prompt": 0.35,
                "selected_user_profile_fact": 0.45,
                "selected_assistant_profile_fact": 0.45,
                "selected_assistant_decision_outcome_only": 0.45,
                "selected_tool_evidence_only": 0.50,
            }
        )
    budgets: Json = {}
    for policy, default_fraction in defaults.items():
        raw_fraction = fractions.get(policy, default_fraction) if isinstance(fractions, dict) else default_fraction
        try:
            fraction = max(0.0, min(1.0, float(raw_fraction)))
        except (TypeError, ValueError):
            fraction = default_fraction
        budgets[policy] = max(1, int(remote_budget * fraction))
    return budgets, mode


def codex_outcome_event_segment_layer_fractions(question_type: str, *, outcome_query: bool = False) -> Json:
    normalized_question_type = str(question_type or "fact").strip().lower()
    defaults: Json = {
        "same_session_codex_outcome_event": 0.22,
        "cross_session_codex_outcome_event": 0.20,
        "same_session_codex_outcome_segment": 0.20,
        "cross_session_codex_outcome_segment": 0.18,
    }
    if outcome_query:
        defaults.update(
            {
                "same_session_codex_outcome_event": 0.45,
                "cross_session_codex_outcome_event": 0.42,
                "same_session_codex_outcome_segment": 0.38,
                "cross_session_codex_outcome_segment": 0.36,
            }
        )
    elif normalized_question_type in {"current_state", "latest"}:
        defaults.update(
            {
                "same_session_codex_outcome_event": 0.35,
                "cross_session_codex_outcome_event": 0.30,
                "same_session_codex_outcome_segment": 0.30,
                "cross_session_codex_outcome_segment": 0.28,
            }
        )
    elif normalized_question_type == "profile_memory":
        defaults.update(
            {
                "same_session_codex_outcome_event": 0.25,
                "cross_session_codex_outcome_event": 0.35,
                "same_session_codex_outcome_segment": 0.22,
                "cross_session_codex_outcome_segment": 0.32,
            }
        )
    elif normalized_question_type in {"multi_hop", "date"}:
        defaults.update(
            {
                "same_session_codex_outcome_event": 0.35,
                "cross_session_codex_outcome_event": 0.35,
                "same_session_codex_outcome_segment": 0.32,
                "cross_session_codex_outcome_segment": 0.32,
            }
        )
    elif normalized_question_type == "benchmark_quality":
        defaults.update(
            {
                "same_session_codex_outcome_event": 0.42,
                "cross_session_codex_outcome_event": 0.45,
                "same_session_codex_outcome_segment": 0.35,
                "cross_session_codex_outcome_segment": 0.40,
            }
        )
    elif normalized_question_type in {"broad_exploration", "evidence"}:
        defaults.update(
            {
                "same_session_codex_outcome_event": 0.38,
                "cross_session_codex_outcome_event": 0.35,
                "same_session_codex_outcome_segment": 0.34,
                "cross_session_codex_outcome_segment": 0.32,
            }
        )
    return defaults


def auto_memory_layer_budget_tokens(args: Json, ranking: Json, *, remote_budget_tokens: int, question_type: str = "fact") -> tuple[Json, str]:
    mode = _default_memory_budget_mode(
        args,
        ranking,
        field="memory_layer_budget_mode",
        question_type=question_type,
    )
    if mode not in {"auto", "balanced", "codex_auto"}:
        return {}, ""
    try:
        remote_budget = max(0, int(remote_budget_tokens or 0))
    except (TypeError, ValueError):
        remote_budget = 0
    if remote_budget <= 0:
        return {}, mode
    fractions = optional_object(args, "memory_layer_budget_fractions") or optional_object(ranking, "memory_layer_budget_fractions")
    defaults = {
        "summary": 0.20,
        "profile_summary": 0.30,
        "same_session_summary": 0.20,
        "cross_session_summary": 0.20,
        "compression": 0.25,
        "profile_compression": 0.25,
        "same_session_compression": 0.20,
        "cross_session_compression": 0.20,
        "pending_async_event": 0.20,
        "pending_async_codex_outcome_event": 0.20,
        "pending_async_memory_feature_event": 0.20,
        "same_session_event": 0.45,
        "same_session_memory_feature_event": 0.35,
        "cross_session_memory_feature_event": 0.25,
        "cross_session_event": 0.25,
        "same_session_segment": 0.35,
        "same_session_memory_feature_segment": 0.30,
        "cross_session_memory_feature_segment": 0.25,
        "cross_session_segment": 0.25,
        "same_session_memory_feature_entity": 0.35,
        "profile_entity": 0.40,
        "cross_session_codex_outcome_entity": 0.25,
        "cross_session_memory_feature_entity": 0.25,
        "cross_session_codex_outcome_summary": 0.25,
        "cross_session_codex_outcome_compression": 0.25,
    }
    normalized_question_type = str(question_type or "fact").strip().lower()
    outcome_query = codex_outcome_budget_query(args, ranking, question_type=question_type)
    feature_profile_query = feature_profile_memory_budget_query(args, ranking, question_type=question_type)
    if outcome_query:
        defaults.update(
            {
                "summary": 0.18,
                "profile_summary": 0.35,
                "same_session_summary": 0.18,
                "cross_session_summary": 0.32,
                "compression": 0.25,
                "profile_compression": 0.35,
                "same_session_compression": 0.20,
                "cross_session_compression": 0.32,
                "pending_async_event": 0.20,
                "pending_async_codex_outcome_event": 0.42,
                "same_session_event": 0.35,
                "cross_session_event": 0.38,
                "same_session_segment": 0.30,
                "cross_session_segment": 0.35,
                "profile_entity": 0.45,
                "cross_session_codex_outcome_entity": 0.62,
                "cross_session_memory_feature_entity": 0.35,
                "cross_session_codex_outcome_summary": 0.45,
                "cross_session_codex_outcome_compression": 0.45,
            }
        )
    elif feature_profile_query:
        defaults.update(
            {
                "summary": 0.15,
                "profile_summary": 0.50,
                "same_session_summary": 0.15,
                "cross_session_summary": 0.45,
                "compression": 0.20,
                "profile_compression": 0.45,
                "same_session_compression": 0.15,
                "cross_session_compression": 0.40,
                "pending_async_event": 0.12,
                "pending_async_codex_outcome_event": 0.10,
                "same_session_event": 0.25,
                "cross_session_event": 0.35,
                "same_session_segment": 0.25,
                "cross_session_segment": 0.35,
                "profile_entity": 0.65,
                "cross_session_codex_outcome_entity": 0.20,
                "cross_session_memory_feature_entity": 0.75,
                "cross_session_codex_outcome_summary": 0.20,
                "cross_session_codex_outcome_compression": 0.20,
            }
        )
    elif normalized_question_type in {"current_state", "latest"}:
        defaults.update(
            {
                "summary": 0.15,
                "profile_summary": 0.20,
                "same_session_summary": 0.15,
                "cross_session_summary": 0.15,
                "compression": 0.20,
                "profile_compression": 0.25,
                "same_session_compression": 0.15,
                "cross_session_compression": 0.20,
                "pending_async_event": 0.15,
                "same_session_event": 0.35,
                "cross_session_event": 0.30,
                "same_session_segment": 0.30,
                "cross_session_segment": 0.30,
                "profile_entity": 0.55,
                "cross_session_codex_outcome_entity": 0.45,
                "cross_session_memory_feature_entity": 0.50,
                "cross_session_codex_outcome_summary": 0.35,
                "cross_session_codex_outcome_compression": 0.35,
            }
        )
    elif normalized_question_type == "profile_memory":
        defaults.update(
            {
                "summary": 0.15,
                "profile_summary": 0.45,
                "same_session_summary": 0.15,
                "cross_session_summary": 0.40,
                "compression": 0.25,
                "profile_compression": 0.40,
                "same_session_compression": 0.20,
                "cross_session_compression": 0.35,
                "pending_async_event": 0.15,
                "same_session_event": 0.25,
                "cross_session_event": 0.40,
                "same_session_segment": 0.25,
                "cross_session_segment": 0.40,
                "profile_entity": 0.60,
                "cross_session_codex_outcome_entity": 0.30,
                "cross_session_memory_feature_entity": 0.65,
                "cross_session_codex_outcome_summary": 0.35,
                "cross_session_codex_outcome_compression": 0.35,
            }
        )
    elif normalized_question_type in {"multi_hop", "date"}:
        defaults.update(
            {
                "summary": 0.20,
                "profile_summary": 0.35,
                "same_session_summary": 0.20,
                "cross_session_summary": 0.35,
                "compression": 0.30,
                "profile_compression": 0.35,
                "same_session_compression": 0.25,
                "cross_session_compression": 0.35,
                "pending_async_event": 0.20,
                "same_session_event": 0.40,
                "cross_session_event": 0.35,
                "same_session_segment": 0.35,
                "cross_session_segment": 0.35,
                "profile_entity": 0.45,
                "cross_session_codex_outcome_entity": 0.40,
                "cross_session_memory_feature_entity": 0.45,
                "cross_session_codex_outcome_summary": 0.35,
                "cross_session_codex_outcome_compression": 0.35,
            }
        )
    elif normalized_question_type == "benchmark_quality":
        defaults.update(
            {
                "summary": 0.20,
                "profile_summary": 0.35,
                "same_session_summary": 0.20,
                "cross_session_summary": 0.35,
                "compression": 0.30,
                "profile_compression": 0.35,
                "same_session_compression": 0.20,
                "cross_session_compression": 0.35,
                "pending_async_event": 0.20,
                "same_session_event": 0.35,
                "cross_session_event": 0.35,
                "same_session_segment": 0.30,
                "cross_session_segment": 0.35,
                "profile_entity": 0.50,
                "cross_session_codex_outcome_entity": 0.58,
                "cross_session_memory_feature_entity": 0.35,
                "cross_session_codex_outcome_summary": 0.45,
                "cross_session_codex_outcome_compression": 0.45,
            }
        )
    elif normalized_question_type in {"broad_exploration", "evidence"}:
        defaults.update(
            {
                "summary": 0.20,
                "profile_summary": 0.35,
                "same_session_summary": 0.25,
                "cross_session_summary": 0.30,
                "compression": 0.30,
                "profile_compression": 0.35,
                "same_session_compression": 0.30,
                "cross_session_compression": 0.30,
                "pending_async_event": 0.25,
                "same_session_event": 0.45,
                "cross_session_event": 0.30,
                "same_session_segment": 0.40,
                "cross_session_segment": 0.30,
                "profile_entity": 0.45,
                "cross_session_codex_outcome_entity": 0.45,
                "cross_session_memory_feature_entity": 0.45,
                "cross_session_codex_outcome_summary": 0.35,
                "cross_session_codex_outcome_compression": 0.35,
            }
        )
    defaults.update(
        codex_outcome_event_segment_layer_fractions(
            normalized_question_type,
            outcome_query=outcome_query,
        )
    )
    if feature_scope_budget_query(args, ranking):
        for outcome_layer in [
            "same_session_codex_outcome_event",
            "pending_async_codex_outcome_event",
            "cross_session_codex_outcome_event",
            "same_session_codex_outcome_segment",
            "cross_session_codex_outcome_segment",
            "cross_session_codex_outcome_entity",
            "cross_session_codex_outcome_summary",
            "cross_session_codex_outcome_compression",
        ]:
            defaults[outcome_layer] = 0.0
    defaults["cross_session_memory_feature_summary"] = max(
        defaults.get("cross_session_memory_feature_entity", 0.25),
        defaults.get("profile_summary", 0.30),
    )
    defaults["cross_session_memory_feature_compression"] = max(
        defaults.get("cross_session_memory_feature_entity", 0.25),
        defaults.get("profile_compression", 0.25),
    )
    defaults["same_session_memory_feature_summary"] = max(
        defaults.get("same_session_memory_feature_entity", 0.25),
        defaults.get("same_session_summary", 0.20),
    )
    defaults["same_session_memory_feature_compression"] = max(
        defaults.get("same_session_memory_feature_entity", 0.25),
        defaults.get("same_session_compression", 0.20),
    )
    budgets: Json = {}
    for layer, default_fraction in defaults.items():
        raw_fraction = fractions.get(layer, default_fraction) if isinstance(fractions, dict) else default_fraction
        try:
            fraction = max(0.0, min(1.0, float(raw_fraction)))
        except (TypeError, ValueError):
            fraction = default_fraction
        if fraction <= 0.0:
            continue
        amount = max(1, int(remote_budget * fraction))
        if amount:
            budgets[layer] = amount
    return budgets, mode


def auto_extraction_phase_budget_tokens(
    args: Json,
    ranking: Json,
    *,
    remote_budget_tokens: int,
    question_type: str = "fact",
) -> tuple[Json, str]:
    mode = _default_memory_budget_mode(
        args,
        ranking,
        field="extraction_phase_budget_mode",
        question_type=question_type,
    )
    if not mode:
        sibling_mode = str(
            args.get("source_role_budget_mode")
            or ranking.get("source_role_budget_mode")
            or args.get("memory_layer_budget_mode")
            or ranking.get("memory_layer_budget_mode")
            or args.get("memory_selection_policy_budget_mode")
            or ranking.get("memory_selection_policy_budget_mode")
            or ""
        ).strip().lower()
        if sibling_mode in {"auto", "balanced", "codex_auto"}:
            mode = sibling_mode
    if mode not in {"auto", "balanced", "codex_auto"}:
        return {}, ""
    try:
        remote_budget = max(0, int(remote_budget_tokens or 0))
    except (TypeError, ValueError):
        remote_budget = 0
    if remote_budget <= 0:
        return {}, mode
    fractions = optional_object(args, "extraction_phase_budget_fractions") or optional_object(
        ranking,
        "extraction_phase_budget_fractions",
    )
    defaults = {
        "pending_async": 0.12,
        "provisional": 0.25,
        "final": 0.70,
    }
    normalized_question_type = str(question_type or "fact").strip().lower()
    if normalized_question_type in {"current_state", "latest"}:
        defaults.update({"pending_async": 0.12, "provisional": 0.25, "final": 0.75})
    elif normalized_question_type == "profile_memory":
        defaults.update({"pending_async": 0.10, "provisional": 0.20, "final": 0.80})
    elif normalized_question_type in {"multi_hop", "date"}:
        defaults.update({"pending_async": 0.15, "provisional": 0.30, "final": 0.70})
    elif normalized_question_type == "benchmark_quality":
        defaults.update({"pending_async": 0.12, "provisional": 0.25, "final": 0.75})
    elif normalized_question_type in {"broad_exploration", "evidence"}:
        defaults.update({"pending_async": 0.15, "provisional": 0.35, "final": 0.70})
    budgets: Json = {}
    for phase, default_fraction in defaults.items():
        raw_fraction = fractions.get(phase, default_fraction) if isinstance(fractions, dict) else default_fraction
        try:
            fraction = max(0.0, min(1.0, float(raw_fraction)))
        except (TypeError, ValueError):
            fraction = default_fraction
        budgets[phase] = max(1, int(remote_budget * fraction))
    return budgets, mode


def pre_retrieval_summary_refresh_memory_layer_budget_tokens(
    *,
    remote_budget_tokens: int,
    question_type: str = "fact",
    args: Json | None = None,
    ranking: Json | None = None,
) -> tuple[Json, str]:
    try:
        remote_budget = max(0, int(remote_budget_tokens or 0))
    except (TypeError, ValueError):
        remote_budget = 0
    args = args if isinstance(args, dict) else {}
    ranking = ranking if isinstance(ranking, dict) else {}
    normalized_question_type = str(question_type or "fact").strip().lower()
    feature_profile_query = feature_profile_memory_budget_query(args, ranking, question_type=question_type)
    mode = "pre_retrieval_summary_refresh_balanced"
    if feature_profile_query:
        mode = "pre_retrieval_summary_refresh_feature_profile_memory"
    elif normalized_question_type in {"current_state", "latest"}:
        mode = "pre_retrieval_summary_refresh_current_state"
    elif normalized_question_type == "profile_memory":
        mode = "pre_retrieval_summary_refresh_profile_memory"
    elif normalized_question_type in {"multi_hop", "date"}:
        mode = "pre_retrieval_summary_refresh_multi_hop"
    elif normalized_question_type == "benchmark_quality":
        mode = "pre_retrieval_summary_refresh_benchmark_quality"
    elif normalized_question_type in {"broad_exploration", "evidence"}:
        mode = "pre_retrieval_summary_refresh_evidence"
    if remote_budget <= 0:
        return {}, mode
    fractions = {
        "summary": 0.15,
        "profile_summary": 0.30,
        "same_session_summary": 0.20,
        "cross_session_summary": 0.25,
        "compression": 0.20,
        "profile_compression": 0.25,
        "same_session_compression": 0.20,
        "cross_session_compression": 0.25,
        "pending_async_event": 0.20,
        "pending_async_codex_outcome_event": 0.20,
        "pending_async_memory_feature_event": 0.20,
        "same_session_event": 0.45,
        "same_session_memory_feature_event": 0.35,
        "cross_session_memory_feature_event": 0.25,
        "cross_session_event": 0.25,
        "same_session_segment": 0.30,
        "same_session_memory_feature_segment": 0.30,
        "cross_session_memory_feature_segment": 0.25,
        "cross_session_segment": 0.25,
        "same_session_memory_feature_entity": 0.35,
        "profile_entity": 0.45,
        "cross_session_codex_outcome_entity": 0.25,
        "cross_session_memory_feature_entity": 0.25,
        "cross_session_codex_outcome_summary": 0.25,
        "cross_session_codex_outcome_compression": 0.25,
    }
    outcome_query = normalized_question_type in {
        "benchmark_quality",
        "evidence",
        "current_state",
        "latest",
    }
    if feature_profile_query:
        fractions.update(
            {
                "summary": 0.15,
                "profile_summary": 0.50,
                "same_session_summary": 0.15,
                "cross_session_summary": 0.45,
                "profile_compression": 0.45,
                "cross_session_compression": 0.40,
                "pending_async_codex_outcome_event": 0.10,
                "same_session_event": 0.25,
                "cross_session_event": 0.35,
                "same_session_segment": 0.25,
                "cross_session_segment": 0.35,
                "profile_entity": 0.65,
                "cross_session_codex_outcome_entity": 0.20,
                "cross_session_memory_feature_entity": 0.75,
                "cross_session_codex_outcome_summary": 0.20,
                "cross_session_codex_outcome_compression": 0.20,
            }
        )
    elif normalized_question_type in {"current_state", "latest"}:
        fractions.update(
            {
                "profile_summary": 0.35,
                "cross_session_summary": 0.30,
                "profile_compression": 0.35,
                "cross_session_compression": 0.30,
                "cross_session_event": 0.30,
                "cross_session_segment": 0.30,
                "profile_entity": 0.55,
                "cross_session_codex_outcome_entity": 0.45,
                "cross_session_memory_feature_entity": 0.50,
                "cross_session_codex_outcome_summary": 0.35,
                "cross_session_codex_outcome_compression": 0.35,
            }
        )
    elif normalized_question_type == "profile_memory":
        fractions.update(
            {
                "summary": 0.15,
                "profile_summary": 0.45,
                "same_session_summary": 0.15,
                "cross_session_summary": 0.40,
                "profile_compression": 0.40,
                "cross_session_compression": 0.35,
                "same_session_event": 0.25,
                "cross_session_event": 0.40,
                "same_session_segment": 0.25,
                "cross_session_segment": 0.40,
                "profile_entity": 0.60,
                "cross_session_codex_outcome_entity": 0.30,
                "cross_session_memory_feature_entity": 0.65,
                "cross_session_codex_outcome_summary": 0.35,
                "cross_session_codex_outcome_compression": 0.35,
            }
        )
    elif normalized_question_type in {"multi_hop", "date"}:
        fractions.update(
            {
                "profile_summary": 0.35,
                "cross_session_summary": 0.35,
                "compression": 0.30,
                "profile_compression": 0.35,
                "cross_session_compression": 0.35,
                "cross_session_event": 0.35,
                "cross_session_segment": 0.35,
                "profile_entity": 0.50,
                "cross_session_codex_outcome_entity": 0.40,
                "cross_session_memory_feature_entity": 0.45,
                "cross_session_codex_outcome_summary": 0.35,
                "cross_session_codex_outcome_compression": 0.35,
            }
        )
    elif normalized_question_type == "benchmark_quality":
        fractions.update(
            {
                "summary": 0.20,
                "profile_summary": 0.35,
                "cross_session_summary": 0.35,
                "profile_compression": 0.35,
                "cross_session_compression": 0.35,
                "cross_session_event": 0.35,
                "cross_session_segment": 0.35,
                "profile_entity": 0.50,
                "cross_session_codex_outcome_entity": 0.58,
                "cross_session_memory_feature_entity": 0.35,
                "cross_session_codex_outcome_summary": 0.45,
                "cross_session_codex_outcome_compression": 0.45,
            }
        )
    elif normalized_question_type in {"broad_exploration", "evidence"}:
        fractions.update(
            {
                "same_session_summary": 0.25,
                "profile_summary": 0.35,
                "cross_session_summary": 0.30,
                "compression": 0.30,
                "profile_compression": 0.35,
                "same_session_compression": 0.30,
                "cross_session_compression": 0.30,
                "pending_async_event": 0.25,
                "pending_async_codex_outcome_event": 0.25,
                "same_session_event": 0.35,
                "cross_session_event": 0.30,
                "same_session_segment": 0.35,
                "cross_session_segment": 0.30,
                "profile_entity": 0.50,
                "cross_session_codex_outcome_entity": 0.45,
                "cross_session_memory_feature_entity": 0.45,
                "cross_session_codex_outcome_summary": 0.35,
                "cross_session_codex_outcome_compression": 0.35,
            }
        )
    fractions.update(
        codex_outcome_event_segment_layer_fractions(
            normalized_question_type,
            outcome_query=outcome_query,
        )
    )
    if feature_scope_budget_query(args, ranking):
        for outcome_layer in [
            "same_session_codex_outcome_event",
            "pending_async_codex_outcome_event",
            "cross_session_codex_outcome_event",
            "same_session_codex_outcome_segment",
            "cross_session_codex_outcome_segment",
            "cross_session_codex_outcome_entity",
            "cross_session_codex_outcome_summary",
            "cross_session_codex_outcome_compression",
        ]:
            fractions[outcome_layer] = 0.0
    fractions["cross_session_memory_feature_summary"] = max(
        fractions.get("cross_session_memory_feature_entity", 0.25),
        fractions.get("profile_summary", 0.30),
    )
    fractions["cross_session_memory_feature_compression"] = max(
        fractions.get("cross_session_memory_feature_entity", 0.25),
        fractions.get("profile_compression", 0.25),
    )
    fractions["same_session_memory_feature_summary"] = max(
        fractions.get("same_session_memory_feature_entity", 0.25),
        fractions.get("same_session_summary", 0.20),
    )
    fractions["same_session_memory_feature_compression"] = max(
        fractions.get("same_session_memory_feature_entity", 0.25),
        fractions.get("same_session_compression", 0.20),
    )
    budgets: Json = {}
    for layer, fraction in fractions.items():
        try:
            bounded_fraction = max(0.0, min(1.0, float(fraction)))
        except (TypeError, ValueError):
            continue
        if bounded_fraction <= 0.0:
            continue
        amount = max(1, int(remote_budget * bounded_fraction))
        if amount:
            budgets[layer] = amount
    return budgets, mode


def pre_retrieval_summary_refresh_enabled(args: Json, ranking: Json) -> bool:
    value = (
        args.get("pre_retrieval_summary_refresh")
        if "pre_retrieval_summary_refresh" in args
        else ranking.get("pre_retrieval_summary_refresh")
        if "pre_retrieval_summary_refresh" in ranking
        else PRE_RETRIEVAL_SUMMARY_REFRESH
    )
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "auto", "bounded"}
    return bool(value)


def pre_retrieval_summary_refresh_limit(args: Json, ranking: Json) -> int:
    raw_limit = args.get("pre_retrieval_summary_refresh_limit") or ranking.get("pre_retrieval_summary_refresh_limit")
    explicit_limit = raw_limit not in (None, "", [], {})
    if not explicit_limit:
        raw_limit = PRE_RETRIEVAL_SUMMARY_REFRESH_LIMIT
    try:
        limit = max(1, int(raw_limit))
    except (TypeError, ValueError):
        limit = PRE_RETRIEVAL_SUMMARY_REFRESH_LIMIT
    if not explicit_limit and feature_profile_memory_budget_query(args, ranking):
        return max(limit, 4)
    return limit


def fresh_idle_commit_summary_refresh_state(idle_commit: Json | None) -> Json:
    if not isinstance(idle_commit, dict):
        return {}
    summary_refresh = idle_commit.get("summary_refresh") if isinstance(idle_commit.get("summary_refresh"), dict) else {}
    memory_layers = idle_commit.get("memory_layers_written") if isinstance(idle_commit.get("memory_layers_written"), dict) else {}
    committed_count = int(idle_commit.get("committed_event_count") or 0)
    dirty_hashes = summary_refresh.get("dirty_hashes") if isinstance(summary_refresh.get("dirty_hashes"), list) else []
    session_dirty_hashes = summary_refresh.get("session_dirty_hashes") if isinstance(summary_refresh.get("session_dirty_hashes"), list) else []
    profile_dirty_hashes = summary_refresh.get("profile_dirty_hashes") if isinstance(summary_refresh.get("profile_dirty_hashes"), list) else []
    summary_required = bool(
        committed_count > 0
        and (
            dirty_hashes
            or session_dirty_hashes
            or profile_dirty_hashes
            or summary_refresh.get("profile_summary_refresh_required")
            or memory_layers.get("summary_dirty_nodes")
        )
    )
    return {
        "fresh_idle_commit_dirty": summary_required,
        "fresh_idle_commit_summary_required": summary_required,
        "fresh_idle_commit_committed_event_count": committed_count,
        "fresh_idle_commit_summary_dirty_nodes": int(memory_layers.get("summary_dirty_nodes") or 0),
        "fresh_idle_commit_profile_summary_required": bool(summary_refresh.get("profile_summary_refresh_required", False)),
    }


def run_pre_retrieval_summary_refresh(target: Any, args: Json, ranking: Json, *, scope: Json, idle_commit: Json | None = None) -> tuple[Json, list[Json]]:
    refresh: Json = {
        "enabled": pre_retrieval_summary_refresh_enabled(args, ranking),
        "requested_limit": pre_retrieval_summary_refresh_limit(args, ranking),
        "refreshed_count": 0,
        "status": "disabled",
        **fresh_idle_commit_summary_refresh_state(idle_commit),
    }
    refreshed_records: list[Json] = []
    if not refresh["enabled"]:
        if refresh.get("fresh_idle_commit_summary_required"):
            refresh["status_reason"] = "fresh_idle_commit_dirty_summary_pending"
        return refresh, refreshed_records
    started = time.perf_counter()
    try:
        result = target.refresh_summaries(
            {
                "scope": scope,
                "limit": int(refresh["requested_limit"]),
                "refreshed_at_ms": now_ms(),
                **(
                    {"skip_dirty_reasons": args.get("pre_retrieval_summary_refresh_skip_dirty_reasons")}
                    if isinstance(args.get("pre_retrieval_summary_refresh_skip_dirty_reasons"), list)
                    else {}
                ),
            }
        )
        refreshed_count = int(result.get("refreshed_count") or 0)
        refreshed_records = [record for record in result.get("refreshed", []) if isinstance(record, dict)]
        refresh.update(
            {
                "status": "refreshed" if refreshed_count else "no_dirty_nodes",
                "refreshed_count": refreshed_count,
                "compression_created_count": int(result.get("compression_created_count") or 0),
                "skipped_dirty_count": int(result.get("skipped_dirty_count") or 0),
                "skipped_dirty_reasons": result.get("skipped_dirty_reasons") if isinstance(result.get("skipped_dirty_reasons"), dict) else {},
                "elapsed_ms": round((time.perf_counter() - started) * 1000.0, 3),
            }
        )
    except Exception as exc:
        refresh.update({"status": "error", "error": str(exc)[:240], "elapsed_ms": round((time.perf_counter() - started) * 1000.0, 3)})
    return refresh, refreshed_records


def merge_refreshed_summary_records(target: Any, records: list[Json], *, retrieval_scope: Json, refreshed_records: list[Json], refresh: Json) -> list[Json]:
    if not refreshed_records and int(refresh.get("refreshed_count") or 0) <= 0:
        return records
    same_user_summary_records = list(refreshed_records)
    try:
        same_user_summary_records.extend(
            record
            for record in target.read_all()
            if isinstance(record, dict)
            and record.get("record_type") == "context_summary"
            and access_scope_matches_before_scoring(record, retrieval_scope)
        )
    except Exception:
        pass
    seen = {
        (record.get("record_type"), record.get("summary_hash") or record.get("node_hash"), tuple(record.get("node_path", [])))
        for record in records
        if isinstance(record, dict)
    }
    for record in same_user_summary_records:
        if not isinstance(record, dict) or record.get("record_type") != "context_summary":
            continue
        identity = (record.get("record_type"), record.get("summary_hash") or record.get("node_hash"), tuple(record.get("node_path", [])))
        if identity in seen:
            continue
        records.append(record)
        seen.add(identity)
    return records
