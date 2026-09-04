# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Split out of matrixark_mcp_core.py; re-exported at core end via the dual
relative/absolute import pattern so the same core module object is reused under
both the package path (tools.matrixark_mcp_core) and the top-level path. No
import-time cycle. __all__ lists every moved name for total re-export."""
import os

try:
    from tools.matrixark_mcp_env import env_bool
except ImportError:  # Direct script execution from tools/.
    from matrixark_mcp_env import env_bool

import re

try:  # package path (tools.matrixark_mcp_core)
    from .matrixark_mcp_core import (
        CODEX_OUTCOME_ENTITY_TYPES,
        CONTEXT_PACK_DEBUG_LINEAGE,
        DEFAULT_MAX_CONTEXT_TOKENS,
        Json,
        MatrixArkError,
        candidate_codex_outcome_terms,
        candidate_is_feature_profile_memory,
        clamp01,
        clip_context_text,
        cross_session_rerank_adjustment,
        is_pending_async_candidate,
        normalize_message_role,
        ordered_normalized_role_list,
        question_type_ref_boost,
        session_continuity_boost,
        stable_hash,
        token_count,
        tokens,
    )
except ImportError:  # top-level path (matrixark_mcp_core)
    from matrixark_mcp_core import (
        CODEX_OUTCOME_ENTITY_TYPES,
        CONTEXT_PACK_DEBUG_LINEAGE,
        DEFAULT_MAX_CONTEXT_TOKENS,
        Json,
        MatrixArkError,
        candidate_codex_outcome_terms,
        candidate_is_feature_profile_memory,
        clamp01,
        clip_context_text,
        cross_session_rerank_adjustment,
        is_pending_async_candidate,
        normalize_message_role,
        ordered_normalized_role_list,
        question_type_ref_boost,
        session_continuity_boost,
        stable_hash,
        token_count,
        tokens,
    )

__all__ = ['packing_sort_key', 'context_text_hashes', 'local_context_budget', 'compact_local_context_refs', 'local_context_refs_for_pack', 'memory_layer_for_serving_ref', 'memory_layer_counts', 'default_memory_layer_for_pack', 'serving_ref_for_pack', 'session_continuity_counts', 'default_session_continuity_for_pack', 'serving_refs_for_pack', 'serving_ref_groups_for_pack', 'selected_ref_count_from_pack', 'is_resource_or_skill_candidate', 'candidate_memory_layer_name']


# Precision-aware raw preference (A/B-validated): the compression/summary token-efficiency
# boost can crowd out raw events, and raw events retain exact hashes/numbers/lists that
# summaries drop (measured ~2/6 exact-fact loss). When enabled, dampen the compression boost
# and lift raw events for precision question-types so exactness is preserved. Ships OFF.
PACK_RAW_PRECISION = env_bool("MATRIXARK_PACK_RAW_PRECISION", False)
# Exact-fact query types where raw events (hashes/numbers/lists) beat lossy summaries.
# NOT current_state/latest — those legitimately want the distilled current-value entity.
PRECISION_QUESTION_TYPES = {"fact", "multi_hop", "evidence", "benchmark_quality", "date"}


def packing_sort_key(candidate: Json, question_type: str) -> tuple[float, float, float, float, float]:
    score = float(candidate.get("score", 0.0))
    prefer_raw = PACK_RAW_PRECISION and question_type in PRECISION_QUESTION_TYPES
    profile_memory_kind = str(candidate.get("profile_memory_kind") or "").strip().lower()
    is_feature_profile_memory = candidate_is_feature_profile_memory(candidate)
    ref_type = str(candidate.get("ref_type") or "")
    memory_scope = str(candidate.get("memory_scope") or "").strip().lower()
    session_continuity = str(candidate.get("session_continuity") or "").strip().lower()
    profile_current = bool(candidate.get("profile_entity_current"))
    try:
        profile_revision = max(0, int(candidate.get("profile_revision") or 0))
    except (TypeError, ValueError):
        profile_revision = 0
    pending_async_penalty = 0.32 if is_pending_async_candidate(candidate) else 0.0
    profile_current_boost = 0.0
    if ref_type == "entity" and memory_scope == "user_profile" and session_continuity == "cross_session":
        if profile_current:
            profile_current_boost = 0.10 if question_type in {"current_state", "latest", "profile_memory"} else 0.04
        if profile_revision > 0:
            profile_current_boost += min(0.04, 0.01 * profile_revision)
    boosted = clamp01(
        score
        + question_type_ref_boost(candidate, question_type)
        + session_continuity_boost(candidate, question_type)
        + cross_session_rerank_adjustment(candidate, question_type)
        + profile_current_boost
        - pending_async_penalty
    )
    if prefer_raw:
        # Precision queries: shift the PRIMARY sort key toward raw events so exact
        # hashes/numbers/lists (which summaries drop) survive into the pack.
        if ref_type == "event":
            boosted = clamp01(boosted + 0.20)
        elif ref_type in {"compression", "summary"}:
            boosted = clamp01(boosted - 0.20)
    token_efficiency = boosted / max(1, token_count(str(candidate.get("text", ""))))
    if not prefer_raw and ref_type == "compression" and question_type in {"fact", "current_state", "multi_hop"}:
        source_count = len(candidate.get("source_event_ids", []) or [])
        if source_count >= 2:
            token_efficiency *= 1.5
    if not prefer_raw and ref_type == "compression" and profile_memory_kind == "codex_outcome":
        source_count = len(candidate.get("source_event_ids", []) or [])
        if question_type in {"current_state", "latest", "evidence", "benchmark_quality"}:
            token_efficiency *= 1.35 if source_count >= 2 else 1.15
            boosted = clamp01(boosted + 0.08)
        elif question_type in {"profile_memory", "multi_hop", "date"}:
            token_efficiency *= 1.20 if source_count >= 2 else 1.08
    if question_type == "profile_memory" and (profile_memory_kind == "durable_profile" or is_feature_profile_memory):
        source_count = len(candidate.get("source_event_ids", []) or [])
        source_entity_count = len(candidate.get("source_entity_hashes", []) or [])
        if ref_type in {"summary", "compression"}:
            token_efficiency *= 1.35 if max(source_count, source_entity_count) >= 2 else 1.12
            boosted = clamp01(boosted + 0.06)
        elif ref_type == "entity":
            boosted = clamp01(boosted + 0.04)
    ref_priority = 0.0
    query_specificity = 0.0
    try:
        sparse_score = float(candidate.get("sparse_score") or 0.0)
    except (TypeError, ValueError):
        sparse_score = 0.0
    try:
        keyword_score = max(0, int(candidate.get("keyword_score") or 0))
    except (TypeError, ValueError):
        keyword_score = 0
    if ref_type == "entity" and question_type in {"current_state", "latest", "evidence", "benchmark_quality", "profile_memory"}:
        query_specificity = clamp01(sparse_score + min(0.45, 0.08 * keyword_score))
        boosted = clamp01(boosted + min(0.18, 0.12 * sparse_score + 0.02 * keyword_score))
    if ref_type == "compression" and profile_memory_kind == "codex_outcome":
        if question_type in {"current_state", "latest", "evidence", "benchmark_quality"}:
            ref_priority = 0.82
            query_specificity = clamp01(sparse_score + min(0.35, 0.06 * keyword_score))
        elif question_type in {"profile_memory", "multi_hop", "date"}:
            ref_priority = 0.62
    elif ref_type == "entity":
        entity_type = str(candidate.get("entity_type") or candidate.get("event_type") or "").strip().lower()
        if question_type in {"current_state", "latest"}:
            if profile_memory_kind == "codex_outcome":
                ref_priority = 1.0
            elif entity_type in CODEX_OUTCOME_ENTITY_TYPES:
                ref_priority = 1.0
            elif entity_type == "assistant_decision":
                ref_priority = 1.0
            elif entity_type == "tool_evidence":
                ref_priority = 0.9
            elif bool(candidate.get("profile_current_state_representative")) or bool(candidate.get("profile_current_state_boost")):
                ref_priority = 0.75
            elif profile_current and memory_scope == "user_profile" and session_continuity == "cross_session":
                ref_priority = 0.78
            elif memory_scope == "user_profile" and session_continuity == "cross_session":
                ref_priority = 0.5
        elif question_type in {"evidence", "benchmark_quality"}:
            codex_outcome_terms = candidate_codex_outcome_terms(candidate)
            if profile_memory_kind == "codex_outcome":
                ref_priority = 1.0
            elif entity_type in CODEX_OUTCOME_ENTITY_TYPES:
                ref_priority = 1.0
            elif entity_type == "tool_evidence":
                ref_priority = 1.0
            elif entity_type == "assistant_decision":
                ref_priority = 0.95 if codex_outcome_terms else 0.7
        elif question_type == "profile_memory" and is_feature_profile_memory:
            ref_priority = max(ref_priority, 1.28 if profile_current else 1.18)
        elif question_type == "profile_memory" and profile_memory_kind == "durable_profile":
            ref_priority = max(ref_priority, 0.98 if profile_current else 0.95)
        elif (
            question_type == "profile_memory"
            and memory_scope == "user_profile"
            and session_continuity == "cross_session"
        ):
            ref_priority = max(ref_priority, 0.82 if profile_current else 0.72)
        if (
            memory_scope == "user_profile"
            and session_continuity == "cross_session"
            and str(candidate.get("entity_name") or "").strip().lower() == entity_type
            and entity_type in {"assistant_decision", "tool_evidence"}
        ):
            ref_priority += 0.30 if entity_type == "assistant_decision" else 0.12
        if profile_current and memory_scope == "user_profile" and session_continuity == "cross_session":
            ref_priority += min(0.08, 0.02 + 0.01 * profile_revision)
    elif question_type == "profile_memory" and (profile_memory_kind == "durable_profile" or is_feature_profile_memory) and ref_type in {"summary", "compression"}:
        ref_priority = 0.82
        query_specificity = clamp01(sparse_score + min(0.35, 0.06 * keyword_score))
    elif question_type in {"evidence", "benchmark_quality"} and str(candidate.get("event_type") or "").strip().lower() == "tool_evidence":
        ref_priority = 0.8
    return (boosted, ref_priority, query_specificity, token_efficiency, score)


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


def memory_layer_for_serving_ref(ref: Json) -> str:
    if not isinstance(ref, dict):
        return ""
    metadata = ref.get("metadata", {}) if isinstance(ref.get("metadata"), dict) else {}
    explicit = str(ref.get("memory_layer") or metadata.get("memory_layer") or "").strip().lower()
    if explicit:
        return explicit
    if (
        str(ref.get("event_type") or metadata.get("event_type") or "").strip().lower() == "pending_async"
        or str(ref.get("classification") or metadata.get("classification") or "").strip().upper() == "PENDING_ASYNC_EXTRACTION"
        or str(ref.get("extraction_phase") or metadata.get("extraction_phase") or "").strip().lower() == "pending_async"
    ):
        return candidate_memory_layer_name(ref)
    sharing_scope = str(ref.get("sharing_scope") or metadata.get("sharing_scope") or "").strip().lower()
    ref_type = str(ref.get("ref_type") or "")
    if sharing_scope in {"tenant_shared", "global_shared"} or ref_type in {"resource_chunk", "skill_section"}:
        return "shared_context"
    memory_scope = str(ref.get("memory_scope") or metadata.get("memory_scope") or "").strip().lower()
    profile_memory_kind = str(ref.get("profile_memory_kind") or metadata.get("profile_memory_kind") or "").strip().lower()
    ref_layer = candidate_memory_layer_name(ref)
    if ref_layer in {
        "cross_session_codex_outcome_compression",
        "cross_session_codex_outcome_summary",
        "cross_session_codex_outcome_entity",
        "cross_session_codex_outcome_event",
        "cross_session_codex_outcome_segment",
    }:
        return "cross_session_codex_outcome"
    if ref_layer in {
        "pending_async_memory_feature_event",
        "same_session_memory_feature_compression",
        "same_session_memory_feature_summary",
        "same_session_memory_feature_segment",
        "same_session_memory_feature_event",
        "same_session_memory_feature_entity",
        "cross_session_memory_feature_compression",
        "cross_session_memory_feature_summary",
        "cross_session_memory_feature_segment",
        "cross_session_memory_feature_event",
        "cross_session_memory_feature_entity",
    }:
        return ref_layer
    if ref_layer in {"same_session_codex_outcome_event", "same_session_codex_outcome_segment"}:
        return "session_codex_outcome"
    if profile_memory_kind == "codex_outcome":
        session_continuity = str(ref.get("session_continuity") or metadata.get("session_continuity") or "").strip().lower()
        if memory_scope in {"user_profile", "profile", "cross_session_profile"} or session_continuity == "cross_session":
            return "cross_session_codex_outcome"
        return "session_codex_outcome"
    if memory_scope in {"user_profile", "profile", "cross_session_profile"}:
        return "profile"
    if memory_scope in {"session", "session_memory"}:
        return "session"
    session_continuity = str(ref.get("session_continuity") or metadata.get("session_continuity") or "")
    if session_continuity == "same_session":
        return "session"
    if session_continuity == "cross_session":
        context_class = str(ref.get("context_class") or ref_type)
        if ref_type == "entity" or context_class in {"resource_entity_fact", "profile_entity", "user_profile"}:
            return "profile"
        return "cross_session"
    return ""


def memory_layer_counts(refs: list[Json]) -> Json:
    counts: Json = {}
    for ref in refs:
        layer = memory_layer_for_serving_ref(ref)
        if layer:
            counts[layer] = int(counts.get(layer, 0)) + 1
    return counts


def default_memory_layer_for_pack(refs: list[Json]) -> str:
    counts = memory_layer_counts(refs)
    if not counts:
        return ""
    return max(counts.items(), key=lambda item: (item[1], item[0]))[0]


def serving_ref_for_pack(ref: Json, *, default_session_continuity: str = "", default_memory_layer: str = "") -> Json:
    """Return only answer-bearing fields for the serving ContextPack payload."""
    metadata = ref.get("metadata", {}) if isinstance(ref.get("metadata"), dict) else {}
    item: Json = {
        "text": ref.get("text", ""),
    }
    source = ref.get("citation") or ref.get("source_ref") or ref.get("source_locator") or metadata.get("source_locator")
    if source:
        item["source"] = source
    optional_field_aliases = [
        ("resource_type", "resource_type"),
        ("unit_kind", "unit_kind"),
        ("heading", "heading"),
        ("heading_slug", "heading_slug"),
        ("relative_path", "path"),
        ("entity_type", "entity_type"),
        ("entity_name", "entity"),
        ("operator", "operator"),
        ("summary_type", "summary_type"),
        ("memory_scope", "memory_scope"),
        ("resource_version", "version"),
        ("version_state", "version_state"),
    ]
    for field, alias in optional_field_aliases:
        value = ref.get(field, metadata.get(field))
        if value not in (None, "", [], {}):
            item[alias] = value
    session_continuity = str(ref.get("session_continuity") or metadata.get("session_continuity") or "")
    if session_continuity and session_continuity != default_session_continuity:
        item["session_continuity"] = session_continuity
    memory_layer = memory_layer_for_serving_ref(ref)
    if memory_layer and memory_layer != default_memory_layer:
        item["memory_layer"] = memory_layer
    profile_memory_kind = str(ref.get("profile_memory_kind") or metadata.get("profile_memory_kind") or "").strip()
    if profile_memory_kind:
        item["profile_memory_kind"] = profile_memory_kind
    profile_memory_class = str(ref.get("profile_memory_class") or metadata.get("profile_memory_class") or "").strip()
    if profile_memory_class:
        item["profile_memory_class"] = profile_memory_class
    if bool(ref.get("profile_entity_current") or metadata.get("profile_entity_current")):
        item["profile_entity_current"] = True
    if bool(ref.get("profile_summary_current") or metadata.get("profile_summary_current")):
        item["profile_summary_current"] = True
    lineage_fields = [
        "source_session_ids",
        "source_roles",
        "source_role_counts",
        "budget_source_roles",
        "budget_source_role_counts",
        "source_hook_types",
        "source_hook_type_counts",
        "source_codex_events",
        "source_codex_event_counts",
        "source_memory_selection_policies",
        "source_memory_selection_policy_counts",
        "source_memory_scopes",
        "source_session_continuities",
        "source_extraction_phases",
        "source_entity_types",
        "source_profile_promotion_policies",
        "source_profile_promotion_blockers",
        "source_final_session_boundary_count",
        "source_event_ids",
    ] if CONTEXT_PACK_DEBUG_LINEAGE else []
    for field in lineage_fields:
        value = ref.get(field, metadata.get(field))
        if isinstance(value, list) and value:
            if field in {"source_roles", "budget_source_roles"}:
                roles = ordered_normalized_role_list(value)
                if roles:
                    item[field] = roles[:8]
            else:
                item[field] = value[:8]
        elif isinstance(value, dict) and value:
            compact_counts: Json = {}
            for key, count in list(value.items())[:8]:
                name = normalize_message_role(key) if field in {"source_role_counts", "budget_source_role_counts"} else str(key or "").strip()
                if not name:
                    continue
                try:
                    compact_count = int(count or 0)
                except (TypeError, ValueError):
                    continue
                if compact_count:
                    compact_counts[name] = int(compact_counts.get(name, 0)) + compact_count
            if compact_counts:
                item[field] = compact_counts
    if CONTEXT_PACK_DEBUG_LINEAGE:
        extraction_phase = ref.get("extraction_phase", metadata.get("extraction_phase"))
        if extraction_phase not in (None, "", [], {}):
            item["extraction_phase"] = extraction_phase
        if bool(ref.get("final_session_boundary") or metadata.get("final_session_boundary")):
            item["final_session_boundary"] = True
        source_entity_hashes = ref.get("source_entity_hashes", metadata.get("source_entity_hashes"))
        if isinstance(source_entity_hashes, list) and source_entity_hashes:
            item["source_entity_count"] = len(source_entity_hashes)
        source_entity_count = ref.get("source_entity_count", metadata.get("source_entity_count"))
        if isinstance(source_entity_count, int) and source_entity_count > 0:
            item["source_entity_count"] = source_entity_count
        source_event_count = ref.get("source_event_count", metadata.get("source_event_count"))
        if isinstance(source_event_count, int) and source_event_count > 0:
            item["source_event_count"] = source_event_count
        source_record_type = ref.get("source_record_type", metadata.get("source_record_type"))
        if isinstance(source_record_type, str) and source_record_type.strip():
            item["source_record_type"] = source_record_type.strip()
        segment_origin = ref.get("segment_origin", metadata.get("segment_origin"))
        if isinstance(segment_origin, str) and segment_origin.strip():
            item["segment_origin"] = segment_origin.strip()
        if ref.get("derived_from_context_events") is True or metadata.get("derived_from_context_events") is True:
            item["derived_from_context_events"] = True
    return item


def session_continuity_counts(refs: list[Json]) -> Json:
    counts: Json = {}
    for ref in refs:
        if not isinstance(ref, dict):
            continue
        metadata = ref.get("metadata", {}) if isinstance(ref.get("metadata"), dict) else {}
        value = str(ref.get("session_continuity") or metadata.get("session_continuity") or "")
        if not value:
            continue
        counts[value] = int(counts.get(value, 0)) + 1
    return counts


def default_session_continuity_for_pack(refs: list[Json]) -> str:
    counts = session_continuity_counts(refs)
    if not counts:
        return ""
    return max(counts.items(), key=lambda item: (item[1], item[0]))[0]


def serving_refs_for_pack(
    refs: list[Json],
    *,
    default_session_continuity: str = "",
    default_memory_layer: str = "",
) -> list[Json]:
    return [
        serving_ref_for_pack(
            ref,
            default_session_continuity=default_session_continuity,
            default_memory_layer=default_memory_layer,
        )
        for ref in refs
    ]


def serving_ref_groups_for_pack(
    refs: list[Json],
    *,
    default_session_continuity: str = "",
    default_memory_layer: str = "",
) -> list[Json]:
    groups: dict[tuple[str, str], Json] = {}
    order: list[tuple[str, str]] = []
    for ref in refs:
        if not isinstance(ref, dict):
            continue
        ref_type = str(ref.get("ref_type") or "")
        context_class = str(ref.get("context_class") or ref_type)
        key = (ref_type, context_class)
        if key not in groups:
            groups[key] = {"type": ref_type, "n": 0, "items": []}
            if context_class and context_class != ref_type:
                groups[key]["class"] = context_class
            order.append(key)
        item = serving_ref_for_pack(
            ref,
            default_session_continuity=default_session_continuity,
            default_memory_layer=default_memory_layer,
        )
        groups[key]["items"].append(item)
        groups[key]["n"] += 1
    return [groups[key] for key in order]


def selected_ref_count_from_pack(pack: Json) -> int:
    refs = pack.get("selected_refs")
    if isinstance(refs, list):
        return len(refs)
    groups = pack.get("selected_ref_groups")
    if isinstance(groups, list):
        total = 0
        for group in groups:
            if not isinstance(group, dict):
                continue
            refs_in_group = group.get("refs", group.get("items", []))
            total += int(group.get("count") or group.get("n") or (len(refs_in_group) if isinstance(refs_in_group, list) else 0))
        return total
    groups = pack.get("groups")
    if isinstance(groups, list):
        total = 0
        for group in groups:
            if not isinstance(group, dict):
                continue
            refs_in_group = group.get("items", group.get("refs", []))
            total += int(group.get("n") or group.get("count") or (len(refs_in_group) if isinstance(refs_in_group, list) else 0))
        return total
    return 0


def is_resource_or_skill_candidate(candidate: Json) -> bool:
    metadata = candidate.get("metadata", {}) if isinstance(candidate.get("metadata"), dict) else {}
    ref_type = str(candidate.get("ref_type") or metadata.get("ref_type") or "")
    context_class = str(candidate.get("context_class") or metadata.get("context_class") or "")
    return ref_type in {"resource_chunk", "skill_section"} or context_class in {"resource_fact", "resource_entity_fact"}


def candidate_memory_layer_name(candidate: Json) -> str:
    metadata = candidate.get("metadata", {}) if isinstance(candidate.get("metadata"), dict) else {}
    explicit_memory_layer = str(candidate.get("memory_layer") or metadata.get("memory_layer") or "").strip()
    if explicit_memory_layer:
        return explicit_memory_layer
    record_type = str(candidate.get("record_type") or metadata.get("record_type") or "")
    ref_type = str(candidate.get("ref_type") or metadata.get("ref_type") or "")
    if not ref_type and record_type == "context_event":
        ref_type = "event"
    elif not ref_type and record_type == "context_entity":
        ref_type = "entity"
    elif not ref_type and record_type == "context_summary":
        ref_type = "summary"
    elif not ref_type and record_type == "context_segment":
        ref_type = "segment"
    context_class = str(candidate.get("context_class") or metadata.get("context_class") or ref_type)
    memory_scope = str(candidate.get("memory_scope") or metadata.get("memory_scope") or "")
    session_continuity = str(candidate.get("session_continuity") or metadata.get("session_continuity") or "")
    profile_memory_class = str(candidate.get("profile_memory_class") or metadata.get("profile_memory_class") or "").strip().lower()
    profile_memory_kind = str(candidate.get("profile_memory_kind") or metadata.get("profile_memory_kind") or "").strip().lower()
    source_profile_memory_classes = {
        str(value or "").strip().lower()
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
        str(value or "").strip().lower()
        for value in (
            candidate.get("source_profile_memory_kinds")
            if isinstance(candidate.get("source_profile_memory_kinds"), list)
            else metadata.get("source_profile_memory_kinds", [])
            if isinstance(metadata.get("source_profile_memory_kinds"), list)
            else []
        )
        if str(value or "").strip()
    }
    source_memory_layers = {
        str(value or "").strip().lower()
        for value in (
            candidate.get("source_memory_layers")
            if isinstance(candidate.get("source_memory_layers"), list)
            else metadata.get("source_memory_layers", [])
            if isinstance(metadata.get("source_memory_layers"), list)
            else []
        )
        if str(value or "").strip()
    }
    event_type = str(candidate.get("event_type") or metadata.get("event_type") or candidate.get("entity_type") or metadata.get("entity_type") or "").strip().lower()
    is_codex_outcome_memory = (
        profile_memory_kind == "codex_outcome"
        or "codex_outcome" in source_profile_memory_kinds
        or any("codex_outcome" in layer for layer in source_memory_layers)
        or event_type in {"assistant_response", "assistant_decision", "tool_evidence", *CODEX_OUTCOME_ENTITY_TYPES}
    )
    is_memory_feature_memory = (
        profile_memory_kind == "memory_feature"
        or profile_memory_class == "memory_feature"
        or "memory_feature" in source_profile_memory_kinds
        or "memory_feature" in source_profile_memory_classes
        or any("memory_feature" in layer for layer in source_memory_layers)
        or event_type == "memory_feature"
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
            if is_codex_outcome_memory:
                return "cross_session_codex_outcome_compression"
            if is_memory_feature_memory:
                return "cross_session_memory_feature_compression"
            return "profile_compression"
        if session_continuity == "same_session":
            if is_memory_feature_memory:
                return "same_session_memory_feature_compression"
            return "same_session_compression"
        if session_continuity == "cross_session":
            return "cross_session_compression"
        return "compression"
    if ref_type == "summary" or context_class == "summary":
        if memory_scope == "user_profile" and session_continuity == "cross_session":
            if is_codex_outcome_memory:
                return "cross_session_codex_outcome_summary"
            if is_memory_feature_memory:
                return "cross_session_memory_feature_summary"
            return "profile_summary"
        if session_continuity == "same_session":
            if is_memory_feature_memory:
                return "same_session_memory_feature_summary"
            return "same_session_summary"
        if session_continuity == "cross_session":
            return "cross_session_summary"
        return "summary"
    if ref_type == "segment":
        if is_memory_feature_memory and session_continuity == "same_session":
            return "same_session_memory_feature_segment"
        if is_memory_feature_memory and session_continuity == "cross_session":
            return "cross_session_memory_feature_segment"
        if is_codex_outcome_memory and session_continuity == "same_session":
            return "same_session_codex_outcome_segment"
        if is_codex_outcome_memory and session_continuity == "cross_session":
            return "cross_session_codex_outcome_segment"
        if session_continuity == "same_session":
            return "same_session_segment"
        if session_continuity == "cross_session":
            return "cross_session_segment"
        return "session_neutral_segment"
    if ref_type == "event":
        if is_pending_async_candidate(candidate):
            if is_memory_feature_memory:
                return "pending_async_memory_feature_event"
            if is_codex_outcome_memory:
                return "pending_async_codex_outcome_event"
            return "pending_async_event"
        if is_memory_feature_memory and session_continuity == "same_session":
            return "same_session_memory_feature_event"
        if is_memory_feature_memory and session_continuity == "cross_session":
            return "cross_session_memory_feature_event"
        if is_codex_outcome_memory and session_continuity == "same_session":
            return "same_session_codex_outcome_event"
        if is_codex_outcome_memory and session_continuity == "cross_session":
            return "cross_session_codex_outcome_event"
        if session_continuity == "same_session":
            return "same_session_event"
        if session_continuity == "cross_session":
            return "cross_session_event"
        return "session_neutral_event"
    if ref_type == "entity":
        if memory_scope == "user_profile":
            if profile_memory_kind == "codex_outcome":
                return "cross_session_codex_outcome_entity"
            if is_memory_feature_memory:
                return "cross_session_memory_feature_entity"
            return "profile_entity"
        if session_continuity == "same_session":
            if is_memory_feature_memory:
                return "same_session_memory_feature_entity"
            return "same_session_entity"
        if session_continuity == "cross_session":
            if is_memory_feature_memory:
                return "cross_session_memory_feature_entity"
            return "cross_session_entity"
        return "session_entity"
    return context_class or ref_type or "unknown"


