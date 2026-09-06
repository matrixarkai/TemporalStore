#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Token-budget packing helpers for MatrixArk retrieval."""

from __future__ import annotations

import os
import re
from typing import Any, Callable

try:
    from tools.matrixark_mcp_errors import MatrixArkError
    from tools.matrixark_mcp_budget_policies import (
        bounded_max_children_scored_per_parent,
        build_cross_session_policy,
        build_shared_context_policy,
    )
    from tools.matrixark_mcp_identity import stable_hash
    from tools.matrixark_mcp_runtime_config import (
        DEFAULT_BUDGET_FILL_POLICY,
        DEFAULT_NEAR_DUPLICATE_OVERLAP_THRESHOLD,
        DEFAULT_MAX_CONTEXT_TOKENS,
        DEFAULT_MAX_GLOBAL_CANDIDATES,
        DEFAULT_MAX_SELECTED_REFS,
        DEFAULT_RETRIEVAL_MIN_SCORE,
    )
    from tools.matrixark_mcp_scoring import (
        near_duplicate_overlap_ratio,
        normalized_token_set,
        tokens,
    )
    from tools.matrixark_mcp_text import clip_context_text, token_count
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_errors import MatrixArkError
    from matrixark_mcp_budget_policies import (
        bounded_max_children_scored_per_parent,
        build_cross_session_policy,
        build_shared_context_policy,
    )
    from matrixark_mcp_identity import stable_hash
    from matrixark_mcp_runtime_config import (
        DEFAULT_BUDGET_FILL_POLICY,
        DEFAULT_NEAR_DUPLICATE_OVERLAP_THRESHOLD,
        DEFAULT_MAX_CONTEXT_TOKENS,
        DEFAULT_MAX_GLOBAL_CANDIDATES,
        DEFAULT_MAX_SELECTED_REFS,
        DEFAULT_RETRIEVAL_MIN_SCORE,
    )
    from matrixark_mcp_scoring import (
        near_duplicate_overlap_ratio,
        normalized_token_set,
        tokens,
    )
    from matrixark_mcp_text import clip_context_text, token_count


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


def normalize_message_role(role: Any) -> str:
    role_name = str(role or "").strip().lower()
    role_aliases = {
        "human": "user",
        "prompt": "user",
        "assistant_response": "assistant",
        "agent": "assistant",
        "ai": "assistant",
        "bot": "assistant",
        "llm": "assistant",
        "model": "assistant",
        "tool_result": "tool",
        "tool-output": "tool",
        "tooloutput": "tool",
        "tool_output": "tool",
        "function": "tool",
        "function_call_output": "tool",
        "custom_tool_call_output": "tool",
        "tool_call_output": "tool",
    }
    return role_aliases.get(role_name, role_name)


def entity_current_state_key(candidate: Json) -> tuple[str, str] | None:
    if str(candidate.get("ref_type") or "") != "entity":
        return None
    metadata = candidate.get("metadata", {}) if isinstance(candidate.get("metadata"), dict) else {}
    entity_type = str(candidate.get("entity_type") or metadata.get("entity_type") or "").strip().lower()
    entity_name = str(candidate.get("entity_name") or metadata.get("entity_name") or "").strip().lower()
    if not entity_type or not entity_name:
        return None
    return entity_type, entity_name


def prefer_profile_entities_for_current_state(candidates: list[Json], question_type: str) -> list[Json]:
    if question_type not in {"current_state", "latest"}:
        return candidates
    latest_profile_by_entity: dict[tuple[str, str], Json] = {}
    latest_profile_by_source_entity_hash: dict[Any, Json] = {}
    for candidate in candidates:
        key = entity_current_state_key(candidate)
        if key is None:
            continue
        if str(candidate.get("memory_scope") or "") != "user_profile":
            continue
        if str(candidate.get("session_continuity") or "") != "cross_session":
            continue
        existing = latest_profile_by_entity.get(key)
        if existing is None or int(candidate.get("updated_at_ms") or 0) >= int(existing.get("updated_at_ms") or 0):
            latest_profile_by_entity[key] = candidate
        for source_entity_hash in candidate.get("source_entity_hashes", []):
            existing_by_source = latest_profile_by_source_entity_hash.get(source_entity_hash)
            if existing_by_source is None or int(candidate.get("updated_at_ms") or 0) >= int(existing_by_source.get("updated_at_ms") or 0):
                latest_profile_by_source_entity_hash[source_entity_hash] = candidate
    if not latest_profile_by_entity:
        return candidates
    adjusted: list[Json] = []
    for candidate in candidates:
        key = entity_current_state_key(candidate)
        profile = latest_profile_by_source_entity_hash.get(candidate.get("ref_hash"))
        if profile is None and key is not None:
            profile = latest_profile_by_entity.get(key)
        if profile is None:
            adjusted.append(candidate)
            continue
        if candidate is profile or candidate.get("ref_hash") == profile.get("ref_hash"):
            adjusted.append({
                **candidate,
                "score": min(1.0, max(0.0, float(candidate.get("score", 0.0)) + 0.18)),
                "profile_current_state_boost": 0.18,
                "selection_reason": candidate.get("selection_reason") or "current profile entity preferred over session-local historical state",
            })
            continue
        if str(candidate.get("memory_scope") or "") == "session":
            adjusted.append({
                **candidate,
                "stale_or_superseded": True,
                "profile_shadowed_by_ref_hash": profile.get("ref_hash"),
                "profile_shadowed_reason": (
                    "source_entity_lineage"
                    if candidate.get("ref_hash") in set(profile.get("source_entity_hashes", []))
                    else "same_entity_identity"
                ),
                "selection_reason": candidate.get("selection_reason") or "session-local entity kept as historical evidence behind current profile state",
            })
            continue
        adjusted.append(candidate)
    return adjusted


def is_stale_or_superseded_candidate(candidate: Json) -> bool:
    if bool(candidate.get("stale_or_superseded") or candidate.get("stale")):
        return True
    if candidate.get("superseded_by_ref_hash") or candidate.get("superseded_by_entity_hash"):
        return True
    version_state = str(candidate.get("version_state") or candidate.get("current_state_policy") or "").strip().lower()
    return version_state in {"stale", "superseded", "historical_superseded"}


def carries_pending_async_marker(candidate: Json) -> bool:
    """Does this candidate carry a pending-async marker, whatever shape it is?

    Named apart from `is_pending_async_candidate` on purpose. That one is the RETRIEVAL predicate
    and returns False unless `ref_type == "event"`, because an event is the only shape it ranks --
    `matrixark_local_adapter_dashboard._embedding_is_pending` already says so, and explains what
    counting a backlog with it costs. This module defined a THIRD function under that same name,
    without the event gate, so the tree had one name meaning two things and a comment elsewhere
    explaining that it meant one.

    Both call sites keep exactly the function they had. At the memory-layer call the surrounding
    branch has already established `ref_type == "event"`, so the gate would change nothing there;
    at `candidate_extraction_phase_name` the unguarded reading is the one wanted, because a
    resource chunk or a skill section waiting on extraction is in that phase whether or not
    retrieval would rank it.
    """
    metadata = candidate.get("metadata", {}) if isinstance(candidate.get("metadata"), dict) else {}
    return (
        str(candidate.get("event_type") or metadata.get("event_type") or "").strip().lower() == "pending_async"
        or str(candidate.get("classification") or metadata.get("classification") or "").strip().upper() == "PENDING_ASYNC_EXTRACTION"
        or str(candidate.get("extraction_phase") or metadata.get("extraction_phase") or "").strip().lower() == "pending_async"
    )


def candidate_memory_layer_name(candidate: Json) -> str:
    metadata = candidate.get("metadata", {}) if isinstance(candidate.get("metadata"), dict) else {}
    ref_type = str(candidate.get("ref_type") or metadata.get("ref_type") or "")
    context_class = str(candidate.get("context_class") or metadata.get("context_class") or ref_type)
    memory_scope = str(candidate.get("memory_scope") or metadata.get("memory_scope") or "")
    session_continuity = str(candidate.get("session_continuity") or metadata.get("session_continuity") or "")
    profile_memory_kind = str(candidate.get("profile_memory_kind") or metadata.get("profile_memory_kind") or "")
    profile_memory_class = str(candidate.get("profile_memory_class") or metadata.get("profile_memory_class") or "")
    source_profile_memory_classes = {
        str(value or "").strip()
        for value in (
            candidate.get("source_profile_memory_classes")
            if isinstance(candidate.get("source_profile_memory_classes"), list)
            else metadata.get("source_profile_memory_classes", [])
            if isinstance(metadata.get("source_profile_memory_classes"), list)
            else []
        )
        if str(value or "").strip()
    }
    source_profile_memory_kinds = {
        str(value or "").strip()
        for value in (
            candidate.get("source_profile_memory_kinds")
            if isinstance(candidate.get("source_profile_memory_kinds"), list)
            else metadata.get("source_profile_memory_kinds", [])
            if isinstance(metadata.get("source_profile_memory_kinds"), list)
            else []
        )
        if str(value or "").strip()
    }
    is_memory_feature = (
        profile_memory_kind == "memory_feature"
        or profile_memory_class == "memory_feature"
        or "memory_feature" in source_profile_memory_kinds
        or "memory_feature" in source_profile_memory_classes
    )
    if context_class == "resource_entity_fact":
        return "resource_entity_fact"
    if context_class == "resource_fact":
        return "resource_fact"
    if ref_type == "resource_chunk":
        return "resource_chunk"
    if ref_type == "skill_section":
        return "skill_section"
    if ref_type == "compression" or context_class == "compression":
        if memory_scope == "user_profile" and session_continuity == "cross_session":
            if is_memory_feature:
                return "cross_session_memory_feature_compression"
            return "profile_compression"
        if session_continuity == "same_session":
            if is_memory_feature:
                return "same_session_memory_feature_compression"
            return "same_session_compression"
        if session_continuity == "cross_session":
            return "cross_session_compression"
        return "compression"
    if ref_type == "summary" or context_class == "summary":
        if memory_scope == "user_profile" and session_continuity == "cross_session":
            if is_memory_feature:
                return "cross_session_memory_feature_summary"
            return "profile_summary"
        if session_continuity == "same_session":
            if is_memory_feature:
                return "same_session_memory_feature_summary"
            return "same_session_summary"
        if session_continuity == "cross_session":
            return "cross_session_summary"
        return "summary"
    if ref_type == "segment":
        if is_memory_feature and session_continuity == "same_session":
            return "same_session_memory_feature_segment"
        if is_memory_feature and session_continuity == "cross_session":
            return "cross_session_memory_feature_segment"
        if session_continuity == "same_session":
            return "same_session_segment"
        if session_continuity == "cross_session":
            return "cross_session_segment"
        return "session_neutral_segment"
    if ref_type == "event":
        if carries_pending_async_marker(candidate):
            if is_memory_feature:
                return "pending_async_memory_feature_event"
            if memory_scope == "user_profile" and session_continuity == "cross_session":
                return "pending_async_memory_feature_event"
            return "pending_async_event"
        if is_memory_feature and session_continuity == "same_session":
            return "same_session_memory_feature_event"
        if is_memory_feature and session_continuity == "cross_session":
            return "cross_session_memory_feature_event"
        if session_continuity == "same_session":
            return "same_session_event"
        if session_continuity == "cross_session":
            return "cross_session_event"
        return "session_neutral_event"
    if ref_type == "entity":
        if memory_scope == "user_profile":
            if profile_memory_kind == "codex_outcome":
                return "cross_session_codex_outcome_entity"
            if is_memory_feature:
                return "cross_session_memory_feature_entity"
            return "profile_entity"
        if session_continuity == "same_session":
            if is_memory_feature:
                return "same_session_memory_feature_entity"
            return "same_session_entity"
        if session_continuity == "cross_session":
            return "cross_session_entity"
        return "session_entity"
    return context_class or ref_type or "unknown"


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
    near_duplicate_overlap_threshold: float = DEFAULT_NEAR_DUPLICATE_OVERLAP_THRESHOLD,
    duplicate_text_hashes: set[int] | None = None,
    deadline_exceeded: Callable[[], bool] | None = None,
    deadline_reason: str = "deadline_during_pack",
    cross_session_policy: Json | None = None,
    shared_context_policy: Json | None = None,
    source_role_budget_tokens: Json | None = None,
    memory_layer_budget_tokens: Json | None = None,
    memory_selection_policy_budget_tokens: Json | None = None,
    extraction_phase_budget_tokens: Json | None = None,
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
    try:
        near_duplicate_overlap_threshold = float(near_duplicate_overlap_threshold)
    except (TypeError, ValueError):
        near_duplicate_overlap_threshold = DEFAULT_NEAR_DUPLICATE_OVERLAP_THRESHOLD
    near_duplicate_overlap_threshold = max(0.0, min(1.0, near_duplicate_overlap_threshold))
    #: A threshold of 0 turns the suppression off, which is how the setting is disabled.
    near_duplicate_enabled = near_duplicate_overlap_threshold > 0.0
    selected_token_sets: list[frozenset[str]] = []
    candidates = merge_ranked_paths(
        primary,
        auxiliary,
        total_limit=candidate_pool_limit,
        auxiliary_quota=auxiliary_quota,
    )
    candidates = prefer_profile_entities_for_current_state(candidates, question_type)
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
    source_role_budget_tokens = source_role_budget_tokens if isinstance(source_role_budget_tokens, dict) else {}
    normalized_source_role_budget_tokens: Json = {}
    for role, budget in source_role_budget_tokens.items():
        role_name = normalize_message_role(role)
        if not role_name:
            continue
        try:
            budget_tokens = max(0, int(budget or 0))
        except (TypeError, ValueError):
            continue
        normalized_source_role_budget_tokens[role_name] = budget_tokens
    source_role_used_tokens: Json = {role: 0 for role in normalized_source_role_budget_tokens}
    source_role_selected_ref_counts: Json = {role: 0 for role in normalized_source_role_budget_tokens}
    memory_layer_budget_tokens = memory_layer_budget_tokens if isinstance(memory_layer_budget_tokens, dict) else {}
    normalized_memory_layer_budget_tokens: Json = {}
    for layer, budget in memory_layer_budget_tokens.items():
        layer_name = str(layer or "").strip().lower()
        if not layer_name:
            continue
        try:
            budget_tokens = max(0, int(budget or 0))
        except (TypeError, ValueError):
            continue
        normalized_memory_layer_budget_tokens[layer_name] = budget_tokens
    memory_layer_used_tokens: Json = {layer: 0 for layer in normalized_memory_layer_budget_tokens}
    memory_layer_selected_ref_counts: Json = {layer: 0 for layer in normalized_memory_layer_budget_tokens}
    memory_selection_policy_budget_tokens = (
        memory_selection_policy_budget_tokens if isinstance(memory_selection_policy_budget_tokens, dict) else {}
    )
    normalized_memory_selection_policy_budget_tokens: Json = {}
    for policy, budget in memory_selection_policy_budget_tokens.items():
        policy_name = str(policy or "").strip()
        if not policy_name:
            continue
        try:
            budget_tokens = max(0, int(budget or 0))
        except (TypeError, ValueError):
            continue
        normalized_memory_selection_policy_budget_tokens[policy_name] = budget_tokens
    memory_selection_policy_used_tokens: Json = {
        policy: 0 for policy in normalized_memory_selection_policy_budget_tokens
    }
    memory_selection_policy_selected_ref_counts: Json = {
        policy: 0 for policy in normalized_memory_selection_policy_budget_tokens
    }
    extraction_phase_budget_tokens = extraction_phase_budget_tokens if isinstance(extraction_phase_budget_tokens, dict) else {}
    normalized_extraction_phase_budget_tokens: Json = {}
    for phase, budget in extraction_phase_budget_tokens.items():
        phase_name = str(phase or "").strip().lower()
        if not phase_name:
            continue
        try:
            budget_tokens = max(0, int(budget or 0))
        except (TypeError, ValueError):
            continue
        normalized_extraction_phase_budget_tokens[phase_name] = budget_tokens
    extraction_phase_used_tokens: Json = {phase: 0 for phase in normalized_extraction_phase_budget_tokens}
    extraction_phase_selected_ref_counts: Json = {phase: 0 for phase in normalized_extraction_phase_budget_tokens}
    profile_entity_floor_enabled = bool(
        normalized_memory_layer_budget_tokens.get("profile_entity")
        and any(
            normalized_memory_layer_budget_tokens.get(layer)
            for layer in ["summary", "profile_summary", "same_session_summary", "cross_session_summary"]
        )
    )

    def profile_entity_floor_satisfied() -> bool:
        return int(memory_layer_selected_ref_counts.get("profile_entity", 0) or 0) > 0

    def remaining_profile_entity_candidate_exists(start_index: int) -> bool:
        for remaining in candidates[start_index:]:
            if candidate_memory_layer_name(remaining) != "profile_entity":
                continue
            try:
                remaining_score = float(remaining.get("score", 0.0))
            except (TypeError, ValueError):
                remaining_score = 0.0
            if remaining_score < min_score:
                continue
            if question_type in {"current_state", "latest"} and is_stale_or_superseded_candidate(remaining):
                continue
            remaining_tokens = max(1, token_count(str(remaining.get("text", ""))))
            if remote_budget <= 0 or (selected and used_tokens + remaining_tokens > remote_budget):
                continue
            if remaining.get("session_continuity") == "cross_session" and not cross_enabled:
                continue
            return True
        return False

    def candidate_source_role_names(candidate: Json) -> set[str]:
        role_names: set[str] = set()
        sources = [candidate]
        metadata = candidate.get("metadata")
        if isinstance(metadata, dict):
            sources.append(metadata)
        for source in sources:
            roles = source.get("source_roles")
            if isinstance(roles, list):
                role_names.update(normalize_message_role(role) for role in roles if normalize_message_role(role))
        metadata_entity_type = metadata.get("entity_type") if isinstance(metadata, dict) else ""
        entity_type = str(candidate.get("entity_type") or metadata_entity_type or "").strip().lower()
        role_specific_entity_types = {
            "assistant_decision": "assistant",
            "assistant_response": "assistant",
            "tool_evidence": "tool",
            "user_requirement": "user",
            "user_preference": "user",
        }
        semantic_role = role_specific_entity_types.get(entity_type)
        if semantic_role and semantic_role in role_names:
            return {semantic_role}
        for source in sources:
            source_counts = source.get("source_role_counts") if isinstance(source.get("source_role_counts"), dict) else {}
            for role, count in source_counts.items():
                role_name = normalize_message_role(role)
                if not role_name:
                    continue
                try:
                    source_count = int(count or 0)
                except (TypeError, ValueError):
                    source_count = 0
                if source_count > 0:
                    role_names.add(role_name)
        if semantic_role and semantic_role in role_names:
            return {semantic_role}
        return role_names

    def candidate_budget_source_role_counts(candidate: Json, role_names: set[str]) -> Json:
        normalized_source_counts: Json = {}
        sources = [candidate]
        metadata = candidate.get("metadata")
        if isinstance(metadata, dict):
            sources.append(metadata)
        for source in sources:
            source_counts = source.get("source_role_counts") if isinstance(source.get("source_role_counts"), dict) else {}
            for role, count in source_counts.items():
                role_name = normalize_message_role(role)
                if not role_name:
                    continue
                try:
                    normalized_source_counts[role_name] = int(normalized_source_counts.get(role_name, 0)) + max(0, int(count or 0))
                except (TypeError, ValueError):
                    continue
        result: Json = {}
        for role in sorted(role_names):
            try:
                source_count = max(0, int(normalized_source_counts.get(role, 0) or 0))
            except (TypeError, ValueError):
                source_count = 0
            result[role] = source_count if source_count > 0 else 1
        return result

    def candidate_memory_selection_policy_names(candidate: Json) -> set[str]:
        policy_names: set[str] = set()
        sources = [candidate]
        metadata = candidate.get("metadata")
        if isinstance(metadata, dict):
            sources.append(metadata)
        for source in sources:
            policies = source.get("source_memory_selection_policies")
            if isinstance(policies, list):
                policy_names.update(str(policy or "").strip() for policy in policies if str(policy or "").strip())
            policy_counts = (
                source.get("source_memory_selection_policy_counts")
                if isinstance(source.get("source_memory_selection_policy_counts"), dict)
                else {}
            )
            for policy, count in policy_counts.items():
                policy_name = str(policy or "").strip()
                if not policy_name:
                    continue
                try:
                    source_count = int(count or 0)
                except (TypeError, ValueError):
                    source_count = 0
                if source_count > 0:
                    policy_names.add(policy_name)
        return policy_names

    def candidate_extraction_phase_name(candidate: Json) -> str:
        phase = str(candidate.get("extraction_phase") or "").strip().lower()
        if not phase and isinstance(candidate.get("metadata"), dict):
            phase = str(candidate.get("metadata", {}).get("extraction_phase") or "").strip().lower()
        if not phase and carries_pending_async_marker(candidate):
            phase = "pending_async"
        return phase or "unknown"

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
        "near_duplicate": 0,
        "low_score": 0,
        "stale": 0,
        "summary": 0,
        "raw_l2": 0,
        "cross_session_budget": 0,
        "cross_session_session_cap": 0,
        "cross_session_candidate_cap": 0,
        "entity_bridge_slot_reserved": 0,
        "shared_resource_budget": 0,
        "shared_skill_budget": 0,
        "source_role_budget": 0,
        "memory_layer_budget": 0,
        "memory_selection_policy_budget": 0,
        "extraction_phase_budget": 0,
        "memory_layer_floor": 0,
        "deadline": 0,
        "max_selected_refs": 0,
        "estimated_tokens": {
            "over_budget": 0,
            "duplicate": 0,
            "near_duplicate": 0,
            "low_score": 0,
            "stale": 0,
            "summary": 0,
            "raw_l2": 0,
            "cross_session_budget": 0,
            "cross_session_session_cap": 0,
            "cross_session_candidate_cap": 0,
            "entity_bridge_slot_reserved": 0,
            "shared_resource_budget": 0,
            "shared_skill_budget": 0,
            "source_role_budget": 0,
            "memory_layer_budget": 0,
            "memory_selection_policy_budget": 0,
            "extraction_phase_budget": 0,
            "memory_layer_floor": 0,
            "deadline": 0,
            "max_selected_refs": 0,
        },
        "reason_descriptions": {
            "over_budget": "candidate was relevant but exceeded the remaining remote context token budget",
            "duplicate": "candidate duplicated local context or an already selected ref",
            "near_duplicate": "candidate text near-duplicated a higher-ranked already selected ref (token overlap above the configured threshold)",
            "low_score": "candidate score was below the minimum packing threshold",
            "stale": "candidate was stale or superseded for the query policy",
            "summary": "summary text was dropped in favor of denser raw/evidence refs",
            "raw_l2": "raw L2 content was dropped because a smaller cited chunk or summary was enough",
            "cross_session_budget": "cross-session candidate exceeded the configured cross-session token budget",
            "cross_session_session_cap": "cross-session candidate came from a session beyond max cross-session session fanout",
            "cross_session_candidate_cap": "cross-session candidate exceeded the configured cross-session candidate cap",
            "entity_bridge_slot_reserved": "candidate was skipped to preserve a minimum cross-session entity bridge slot",
            "shared_resource_budget": "shared resource candidate exceeded the configured shared-resource token budget",
            "shared_skill_budget": "shared skill candidate exceeded the configured shared-skill token budget",
            "source_role_budget": "candidate exceeded a configured source-role token budget",
            "memory_layer_budget": "candidate exceeded a configured memory-layer token budget",
            "memory_selection_policy_budget": "candidate exceeded a configured memory-selection-policy token budget",
            "extraction_phase_budget": "candidate exceeded a configured extraction-phase token budget",
            "memory_layer_floor": "candidate was deferred so a required lower-layer entity could be selected first",
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

    def eligible_entity_bridge_remains(start_index: int) -> bool:
        if not cross_enabled or cross_min_entity_bridge_refs <= entity_bridge_selected_ref_count:
            return False
        for future in candidates[start_index:]:
            if future.get("session_continuity") != "cross_session" or str(future.get("ref_type") or "") != "entity":
                continue
            future_text = str(future.get("text", ""))
            if context_text_hashes(future_text).intersection(duplicate_text_hashes):
                continue
            if stable_hash(future_text[:512]) in seen_text_hashes:
                continue
            future_score = float(future.get("score", 0.0))
            if future_score < min_score:
                continue
            if cross_min_score > 0.0 and future_score < cross_min_score:
                continue
            return True
        return False

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
        if question_type in {"current_state", "latest"} and is_stale_or_superseded_candidate(candidate):
            dropped["stale"] += 1
            dropped["estimated_tokens"]["stale"] += ref_tokens
            record_dropped_candidate(dropped, candidate, reason="stale", token_estimate=ref_tokens)
            continue
        # Near-duplicate suppression: drop a candidate whose normalized token set overlaps an
        # already-selected (strictly higher-ranked, since selection is in ranked order) ref
        # above the configured threshold, so repetitive refs do not dilute precision or spend
        # budget on redundant content. The highest-ranked instance is the one already in
        # `selected`, so it is kept. Same placement as matrixark_mcp_core_ref_selection: before
        # the budget check, so a near-duplicate is not counted as over_budget instead.
        if near_duplicate_enabled and selected_token_sets:
            candidate_token_set = normalized_token_set(candidate.get("text", ""))
            if candidate_token_set and any(
                near_duplicate_overlap_ratio(candidate_token_set, selected_tokens) >= near_duplicate_overlap_threshold
                for selected_tokens in selected_token_sets
            ):
                dropped["near_duplicate"] += 1
                dropped["estimated_tokens"]["near_duplicate"] += ref_tokens
                record_dropped_candidate(dropped, candidate, reason="near_duplicate", token_estimate=ref_tokens)
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
        remaining_slots = selected_ref_cap - len(selected)
        remaining_required_bridge_refs = max(0, cross_min_entity_bridge_refs - entity_bridge_selected_ref_count)
        if (
            cross_enabled
            and not is_entity_bridge
            and remaining_required_bridge_refs > 0
            and remaining_slots <= remaining_required_bridge_refs
            and eligible_entity_bridge_remains(index + 1)
        ):
            dropped["entity_bridge_slot_reserved"] += 1
            dropped["estimated_tokens"]["entity_bridge_slot_reserved"] += ref_tokens
            record_dropped_candidate(dropped, candidate, reason="entity_bridge_slot_reserved", token_estimate=ref_tokens)
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
        candidate_memory_layer = candidate_memory_layer_name(candidate)
        if (
            profile_entity_floor_enabled
            and candidate_memory_layer in {"summary", "profile_summary", "same_session_summary", "cross_session_summary"}
            and not profile_entity_floor_satisfied()
            and remaining_profile_entity_candidate_exists(index + 1)
        ):
            dropped["memory_layer_floor"] += 1
            dropped["estimated_tokens"]["memory_layer_floor"] += ref_tokens
            record_dropped_candidate(
                dropped,
                {
                    **candidate,
                    "budget_memory_layer": candidate_memory_layer,
                    "memory_layer_budget_capped_layer": candidate_memory_layer,
                    "memory_layer_floor_reserved_layer": "profile_entity",
                },
                reason="memory_layer_floor",
                token_estimate=ref_tokens,
            )
            continue
        if (
            candidate_memory_layer in normalized_memory_layer_budget_tokens
            and int(memory_layer_used_tokens.get(candidate_memory_layer, 0)) + ref_tokens
            > int(normalized_memory_layer_budget_tokens[candidate_memory_layer])
        ):
            dropped["memory_layer_budget"] += 1
            dropped["estimated_tokens"]["memory_layer_budget"] += ref_tokens
            record_dropped_candidate(
                dropped,
                {
                    **candidate,
                    "budget_memory_layer": candidate_memory_layer,
                    "memory_layer_budget_capped_layer": candidate_memory_layer,
                },
                reason="memory_layer_budget",
                token_estimate=ref_tokens,
            )
            continue
        candidate_source_roles = candidate_source_role_names(candidate)
        capped_roles = [
            role
            for role in sorted(candidate_source_roles)
            if role in normalized_source_role_budget_tokens
            and int(source_role_used_tokens.get(role, 0)) + ref_tokens > int(normalized_source_role_budget_tokens[role])
        ]
        if capped_roles:
            dropped["source_role_budget"] += 1
            dropped["estimated_tokens"]["source_role_budget"] += ref_tokens
            record_dropped_candidate(
                dropped,
                {
                    **candidate,
                    "budget_source_roles": sorted(candidate_source_roles),
                    "budget_source_role_counts": candidate_budget_source_role_counts(candidate, candidate_source_roles),
                    "source_role_budget_capped_roles": capped_roles,
                },
                reason="source_role_budget",
                token_estimate=ref_tokens,
            )
            continue
        candidate_memory_selection_policies = candidate_memory_selection_policy_names(candidate)
        capped_memory_selection_policies = [
            policy
            for policy in sorted(candidate_memory_selection_policies)
            if policy in normalized_memory_selection_policy_budget_tokens
            and int(memory_selection_policy_used_tokens.get(policy, 0)) + ref_tokens
            > int(normalized_memory_selection_policy_budget_tokens[policy])
        ]
        if capped_memory_selection_policies:
            dropped["memory_selection_policy_budget"] += 1
            dropped["estimated_tokens"]["memory_selection_policy_budget"] += ref_tokens
            record_dropped_candidate(
                dropped,
                {
                    **candidate,
                    "budget_memory_selection_policies": sorted(candidate_memory_selection_policies),
                    "memory_selection_policy_budget_capped_policies": capped_memory_selection_policies,
                },
                reason="memory_selection_policy_budget",
                token_estimate=ref_tokens,
            )
            continue
        candidate_extraction_phase = candidate_extraction_phase_name(candidate)
        if (
            candidate_extraction_phase in normalized_extraction_phase_budget_tokens
            and int(extraction_phase_used_tokens.get(candidate_extraction_phase, 0)) + ref_tokens
            > int(normalized_extraction_phase_budget_tokens[candidate_extraction_phase])
        ):
            dropped["extraction_phase_budget"] += 1
            dropped["estimated_tokens"]["extraction_phase_budget"] += ref_tokens
            record_dropped_candidate(
                dropped,
                {
                    **candidate,
                    "budget_extraction_phase": candidate_extraction_phase,
                    "extraction_phase_budget_capped_phase": candidate_extraction_phase,
                },
                reason="extraction_phase_budget",
                token_estimate=ref_tokens,
            )
            continue
        seen_text_hashes.add(text_hash)
        if near_duplicate_enabled:
            selected_token_sets.append(normalized_token_set(candidate.get("text", "")))
        selected.append(
            {
                **candidate,
                "token_estimate": ref_tokens,
                "packing_score": round(packing_sort_key(candidate, question_type)[0], 6),
                "packing_policy": question_type,
                "budget_memory_layer": candidate_memory_layer,
                "budget_source_roles": sorted(candidate_source_roles),
                "budget_source_role_counts": candidate_budget_source_role_counts(candidate, candidate_source_roles),
                "budget_memory_selection_policies": sorted(candidate_memory_selection_policies),
                "budget_extraction_phase": candidate_extraction_phase,
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
        if candidate_memory_layer in normalized_memory_layer_budget_tokens:
            memory_layer_used_tokens[candidate_memory_layer] = int(memory_layer_used_tokens.get(candidate_memory_layer, 0)) + ref_tokens
            memory_layer_selected_ref_counts[candidate_memory_layer] = int(memory_layer_selected_ref_counts.get(candidate_memory_layer, 0)) + 1
        for role in candidate_source_roles:
            if role in normalized_source_role_budget_tokens:
                source_role_used_tokens[role] = int(source_role_used_tokens.get(role, 0)) + ref_tokens
                source_role_selected_ref_counts[role] = int(source_role_selected_ref_counts.get(role, 0)) + 1
        for policy in candidate_memory_selection_policies:
            if policy in normalized_memory_selection_policy_budget_tokens:
                memory_selection_policy_used_tokens[policy] = int(memory_selection_policy_used_tokens.get(policy, 0)) + ref_tokens
                memory_selection_policy_selected_ref_counts[policy] = int(
                    memory_selection_policy_selected_ref_counts.get(policy, 0)
                ) + 1
        if candidate_extraction_phase in normalized_extraction_phase_budget_tokens:
            extraction_phase_used_tokens[candidate_extraction_phase] = (
                int(extraction_phase_used_tokens.get(candidate_extraction_phase, 0)) + ref_tokens
            )
            extraction_phase_selected_ref_counts[candidate_extraction_phase] = (
                int(extraction_phase_selected_ref_counts.get(candidate_extraction_phase, 0)) + 1
            )
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
    dropped["source_role_budget_policy"] = {
        "enabled": bool(normalized_source_role_budget_tokens),
        "budget_tokens": normalized_source_role_budget_tokens,
        "selected_tokens_by_role": source_role_used_tokens,
        "selected_ref_count_by_role": source_role_selected_ref_counts,
    }
    dropped["memory_layer_budget_policy"] = {
        "enabled": bool(normalized_memory_layer_budget_tokens),
        "budget_tokens": normalized_memory_layer_budget_tokens,
        "selected_tokens_by_layer": memory_layer_used_tokens,
        "selected_ref_count_by_layer": memory_layer_selected_ref_counts,
    }
    dropped["memory_selection_policy_budget_policy"] = {
        "enabled": bool(normalized_memory_selection_policy_budget_tokens),
        "budget_tokens": normalized_memory_selection_policy_budget_tokens,
        "selected_tokens_by_policy": memory_selection_policy_used_tokens,
        "selected_ref_count_by_policy": memory_selection_policy_selected_ref_counts,
    }
    dropped["extraction_phase_budget_policy"] = {
        "enabled": bool(normalized_extraction_phase_budget_tokens),
        "budget_tokens": normalized_extraction_phase_budget_tokens,
        "selected_tokens_by_phase": extraction_phase_used_tokens,
        "selected_ref_count_by_phase": extraction_phase_selected_ref_counts,
    }
    if not selected and candidates and remote_budget > 0 and budget_fill_policy != "quality_first":
        first: Json | None = None
        first_source_roles: set[str] = set()
        first_memory_selection_policies: set[str] = set()
        first_extraction_phase = ""
        first_memory_layer = ""
        first_clipped_words: list[str] = []
        for candidate in candidates:
            if context_text_hashes(str(candidate.get("text", ""))).intersection(duplicate_text_hashes):
                continue
            clipped_words_for_candidate = tokens(str(candidate.get("text", "")))[:remote_budget]
            fallback_tokens = len(clipped_words_for_candidate)
            if fallback_tokens <= 0:
                continue
            candidate_memory_layer = candidate_memory_layer_name(candidate)
            if (
                candidate_memory_layer in normalized_memory_layer_budget_tokens
                and int(memory_layer_used_tokens.get(candidate_memory_layer, 0)) + fallback_tokens
                > int(normalized_memory_layer_budget_tokens[candidate_memory_layer])
            ):
                continue
            candidate_source_roles = candidate_source_role_names(candidate)
            if any(
                role in normalized_source_role_budget_tokens
                and int(source_role_used_tokens.get(role, 0)) + fallback_tokens > int(normalized_source_role_budget_tokens[role])
                for role in candidate_source_roles
            ):
                continue
            candidate_memory_selection_policies = candidate_memory_selection_policy_names(candidate)
            if any(
                policy in normalized_memory_selection_policy_budget_tokens
                and int(memory_selection_policy_used_tokens.get(policy, 0)) + fallback_tokens
                > int(normalized_memory_selection_policy_budget_tokens[policy])
                for policy in candidate_memory_selection_policies
            ):
                continue
            candidate_extraction_phase = candidate_extraction_phase_name(candidate)
            if (
                candidate_extraction_phase in normalized_extraction_phase_budget_tokens
                and int(extraction_phase_used_tokens.get(candidate_extraction_phase, 0)) + fallback_tokens
                > int(normalized_extraction_phase_budget_tokens[candidate_extraction_phase])
            ):
                continue
            first = candidate
            first_source_roles = candidate_source_roles
            first_memory_selection_policies = candidate_memory_selection_policies
            first_extraction_phase = candidate_extraction_phase
            first_memory_layer = candidate_memory_layer
            first_clipped_words = clipped_words_for_candidate
            break
        if first is None:
            return selected, used_tokens, dropped
        clipped_words = first_clipped_words
        selected = [
            {
                **first,
                "text": " ".join(clipped_words),
                "token_estimate": len(clipped_words),
                "budget_memory_layer": first_memory_layer,
                "budget_source_roles": sorted(first_source_roles),
                "budget_source_role_counts": candidate_budget_source_role_counts(first, first_source_roles),
                "budget_memory_selection_policies": sorted(first_memory_selection_policies),
                "budget_extraction_phase": first_extraction_phase,
            }
        ]
        used_tokens = len(clipped_words)
        for role in first_source_roles:
            if role in normalized_source_role_budget_tokens:
                source_role_used_tokens[role] = int(source_role_used_tokens.get(role, 0)) + used_tokens
                source_role_selected_ref_counts[role] = int(source_role_selected_ref_counts.get(role, 0)) + 1
        if first_memory_layer in normalized_memory_layer_budget_tokens:
            memory_layer_used_tokens[first_memory_layer] = int(memory_layer_used_tokens.get(first_memory_layer, 0)) + used_tokens
            memory_layer_selected_ref_counts[first_memory_layer] = int(memory_layer_selected_ref_counts.get(first_memory_layer, 0)) + 1
        for policy in first_memory_selection_policies:
            if policy in normalized_memory_selection_policy_budget_tokens:
                memory_selection_policy_used_tokens[policy] = int(memory_selection_policy_used_tokens.get(policy, 0)) + used_tokens
                memory_selection_policy_selected_ref_counts[policy] = int(
                    memory_selection_policy_selected_ref_counts.get(policy, 0)
                ) + 1
        if first_extraction_phase in normalized_extraction_phase_budget_tokens:
            extraction_phase_used_tokens[first_extraction_phase] = (
                int(extraction_phase_used_tokens.get(first_extraction_phase, 0)) + used_tokens
            )
            extraction_phase_selected_ref_counts[first_extraction_phase] = (
                int(extraction_phase_selected_ref_counts.get(first_extraction_phase, 0)) + 1
            )
        dropped["memory_selection_policy_budget_policy"] = {
            "enabled": bool(normalized_memory_selection_policy_budget_tokens),
            "budget_tokens": normalized_memory_selection_policy_budget_tokens,
            "selected_tokens_by_policy": memory_selection_policy_used_tokens,
            "selected_ref_count_by_policy": memory_selection_policy_selected_ref_counts,
        }
        dropped["extraction_phase_budget_policy"] = {
            "enabled": bool(normalized_extraction_phase_budget_tokens),
            "budget_tokens": normalized_extraction_phase_budget_tokens,
            "selected_tokens_by_phase": extraction_phase_used_tokens,
            "selected_ref_count_by_phase": extraction_phase_selected_ref_counts,
        }
        dropped["over_budget"] = max(0, len(candidates) - 1)
        for candidate in candidates[1:]:
            record_dropped_candidate(
                dropped,
                candidate,
                reason="over_budget",
                token_estimate=max(1, token_count(str(candidate.get("text", "")))),
            )
    return selected, used_tokens, dropped
