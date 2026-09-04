#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""ContextPack serving and audit compaction helpers for MatrixArk MCP."""

from __future__ import annotations

try:
    from tools.matrixark_mcp_env import env_bool
except ImportError:  # Direct script execution from tools/.
    from matrixark_mcp_env import env_bool


import os as _os

import os
from typing import Any

Json = dict[str, Any]

AUDIT_DEBUG_PAYLOAD = os.environ.get("MATRIXARK_AUDIT_DEBUG_PAYLOAD", "0").strip().lower() in {"1", "true", "yes"}

DEFAULT_HIDDEN_DEBUG_LINEAGE_FIELDS = {
    "debug",
    "debug_payload",
    "debug_refs",
    "debug_record",
    "metadata_debug",
    "memory_lineage",
    "memory_hierarchy",
    "lineage",
    "current_state_policy",
    "source_session_ids",
    "source_roles",
    "budget_source_roles",
    "source_hook_types",
    "source_codex_events",
    "source_memory_selection_policies",
    "source_memory_layers",
    "source_memory_scopes",
    "source_session_continuities",
    "source_extraction_phases",
    "source_profile_promotion_policies",
    "source_profile_promotion_blockers",
    "source_final_session_boundary_count",
    "source_role_counts",
    "budget_source_role_counts",
    "source_hook_type_counts",
    "source_codex_event_counts",
    "source_memory_selection_policy_counts",
    "source_memory_layer_counts",
    "source_message_counts_by_role",
    "source_hook_counts_by_type",
    "source_codex_event_counts_by_event",
    "source_lineage",
    "source_event_ids",
    "source_event_count",
    "pending_source_roles",
    "pending_source_hook_types",
    "pending_source_codex_events",
    "pending_memory_scopes",
    "pending_session_continuities",
    "pending_extraction_phases",
    "pending_final_session_boundary_count",
    "by_source_role",
    "by_hook_type",
    "by_codex_event",
    "by_memory_selection_policy",
    "source_entity_types",
    "source_entity_hashes",
    "source_entity_count",
    "current_state_source_session_count",
    "current_state_source_entity_count",
    "context_pack_payload_policy",
    "operational_visibility_policy",
}

DEFAULT_HIDDEN_DEBUG_LINEAGE_KEY_FRAGMENTS = (
    "debug_",
    "_debug",
    "lineage",
)


def debug_lineage_enabled(*, include_debug: bool = False) -> bool:
    # The caller decides. This used to be OR'd with an environment flag, which gave the
    # question two answers depending on how the process was started.
    return bool(include_debug)


def _is_default_hidden_debug_lineage_key(key: Any) -> bool:
    name = str(key or "").strip()
    if name in DEFAULT_HIDDEN_DEBUG_LINEAGE_FIELDS:
        return True
    lowered = name.lower()
    return any(fragment in lowered for fragment in DEFAULT_HIDDEN_DEBUG_LINEAGE_KEY_FRAGMENTS)


def strip_default_debug_lineage_fields(value: Any) -> Any:
    """Remove debug/lineage fields from the default prompt-facing ContextPack."""
    if isinstance(value, dict):
        return {
            key: strip_default_debug_lineage_fields(item)
            for key, item in value.items()
            if not _is_default_hidden_debug_lineage_key(key)
        }
    if isinstance(value, list):
        return [strip_default_debug_lineage_fields(item) for item in value]
    return value


def _clip_context_text(text: str, *, max_chars: int = 160) -> str:
    if len(text) <= max_chars:
        return text
    return text[:max_chars].rstrip() + " ...[truncated]"


def _compact_count_map(value: Any, *, limit: int = 8) -> Json:
    if not isinstance(value, dict):
        return {}
    compact: Json = {}
    for key, count in list(value.items())[:limit]:
        name = str(key or "").strip()
        if not name:
            continue
        try:
            compact_count = int(count or 0)
        except (TypeError, ValueError):
            continue
        if compact_count:
            compact[name] = compact_count
    return compact


def _normalize_message_role(role: Any) -> str:
    role_name = str(role or "").strip().lower()
    return {
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
    }.get(role_name, role_name)


def _ordered_normalized_roles(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    roles: list[str] = []
    seen: set[str] = set()
    for item in value:
        role = _normalize_message_role(item)
        if role and role not in seen:
            roles.append(role)
            seen.add(role)
    return roles


def _compact_role_count_map(value: Any, *, limit: int = 8, include_zero: bool = False) -> Json:
    if not isinstance(value, dict):
        return {}
    compact: Json = {}
    for key, count in list(value.items())[:limit]:
        name = _normalize_message_role(key)
        if not name:
            continue
        try:
            compact_count = int(count or 0)
        except (TypeError, ValueError):
            continue
        if compact_count or include_zero:
            compact[name] = int(compact.get(name, 0)) + compact_count
    return compact


def _compact_role_bucket_map(value: Any) -> Json:
    if not isinstance(value, dict):
        return {}
    compact: Json = {}
    for key, bucket in value.items():
        role = _normalize_message_role(key)
        if not role or not isinstance(bucket, dict):
            continue
        target = compact.setdefault(role, {})
        for metric in ["refs", "tokens", "selected_refs", "selected_tokens", "dropped_refs", "dropped_tokens"]:
            try:
                amount = int(bucket.get(metric) or 0)
            except (TypeError, ValueError):
                amount = 0
            if amount:
                target[metric] = int(target.get(metric, 0)) + amount
    return compact


def compact_memory_layer_budget_roles(value: Any) -> Json:
    if not isinstance(value, dict):
        return {}
    compact = dict(value)
    role_counts = _compact_role_count_map(compact.get("source_message_counts_by_role"))
    if role_counts:
        compact["source_message_counts_by_role"] = role_counts
    role_buckets = _compact_role_bucket_map(compact.get("by_source_role"))
    if role_buckets:
        compact["by_source_role"] = role_buckets
    return compact


def serving_memory_selection_policy_budget(value: Any) -> Json:
    if not isinstance(value, dict):
        return {}
    compact: Json = {}
    if "enabled" in value:
        compact["enabled"] = bool(value.get("enabled"))
    for field in [
        "mode",
        "budget_semantics",
        "independent_caps",
        "global_remote_budget_enforced",
    ]:
        item = value.get(field)
        if item not in (None, "", [], {}):
            compact[field] = item
    try:
        remote_budget = int(value.get("remote_budget_tokens") or 0)
    except (TypeError, ValueError):
        remote_budget = 0
    if remote_budget > 0:
        compact["remote_budget_tokens"] = remote_budget
    for field in ["budget_tokens", "selected_tokens_by_policy", "selected_ref_count_by_policy"]:
        values = value.get(field)
        if not isinstance(values, dict):
            continue
        normalized: Json = {}
        for key, raw in values.items():
            label = str(key or "").strip()
            if not label:
                continue
            try:
                amount = int(raw or 0)
            except (TypeError, ValueError):
                continue
            if amount > 0:
                normalized[label] = amount
        if normalized:
            compact[field] = normalized
    return {key: item for key, item in compact.items() if item not in (None, "", [], {})}


def serving_memory_layer_budget(value: Any) -> Json:
    compact = compact_memory_layer_budget_roles(value)
    for field in [
        "by_source_role",
        "by_hook_type",
        "by_codex_event",
        "by_memory_selection_policy",
        "source_message_counts_by_role",
        "source_hook_counts_by_type",
        "source_codex_event_counts_by_event",
    ]:
        compact.pop(field, None)
    compact = strip_default_debug_lineage_fields(compact)
    for field in list(compact):
        if isinstance(compact.get(field), dict) and not compact[field]:
            compact.pop(field, None)
    return compact


def serving_memory_layer_pressure(value: Any) -> Json:
    if not isinstance(value, dict):
        return {}
    compact = dict(value)
    lineage_dimensions = {
        "by_source_role",
        "by_hook_type",
        "by_codex_event",
        "source_message_counts_by_role",
        "source_hook_counts_by_type",
        "source_codex_event_counts_by_event",
    }
    for list_field in ["pressure_dimensions", "dropped_dimensions"]:
        values = compact.get(list_field)
        if isinstance(values, list):
            compact[list_field] = [value for value in values if str(value) not in lineage_dimensions]
    by_dimension = compact.get("by_dimension")
    if isinstance(by_dimension, dict):
        compact["by_dimension"] = {
            str(key): value for key, value in by_dimension.items() if str(key) not in lineage_dimensions
        }
    for field in [
        "assistant_memory_pressure",
        "user_memory_pressure",
        "tool_memory_pressure",
        "assistant_source_message_pressure",
        "user_source_message_pressure",
        "tool_source_message_pressure",
        "hook_boundary_source_pressure",
        "after_llm_source_pressure",
        "tool_result_source_pressure",
        "stop_event_source_pressure",
        "post_tool_use_source_pressure",
    ]:
        compact.pop(field, None)
    compact = strip_default_debug_lineage_fields(compact)
    by_dimension = compact.get("by_dimension")
    if isinstance(by_dimension, dict):
        compact["by_dimension"] = {
            str(key): value
            for key, value in by_dimension.items()
            if not (isinstance(value, dict) and not value)
        }
        valid_dimensions = set(compact["by_dimension"])
        for list_field in ["pressure_dimensions", "dropped_dimensions"]:
            values = compact.get(list_field)
            if isinstance(values, list):
                compact[list_field] = [
                    value
                    for value in values
                    if not (str(value).startswith("by_") and str(value) not in valid_dimensions)
                ]
        if not compact["by_dimension"]:
            compact.pop("by_dimension", None)
    return compact


def serving_retrieval_metrics(value: Any, *, include_debug: bool = False) -> Json:
    if not isinstance(value, dict):
        return {}
    if debug_lineage_enabled(include_debug=include_debug):
        return value
    compact: Json = {}
    for field in [
        "selected_refs",
        "remote_context_budget_tokens",
        "used_remote_context_tokens",
        "used_local_context_tokens",
        "total_prompt_context_tokens",
        "partial_context_pack",
        "fallback_reason",
    ]:
        metric = value.get(field)
        if metric not in (None, "", [], {}):
            compact[field] = metric
    memory_layer_budget = value.get("memory_layer_budget")
    if isinstance(memory_layer_budget, dict):
        summary = serving_memory_layer_budget(memory_layer_budget)
        if summary:
            compact["memory_layer_budget"] = summary
    dropped_memory_layer_budget = value.get("dropped_memory_layer_budget")
    if isinstance(dropped_memory_layer_budget, dict):
        summary = serving_memory_layer_budget(dropped_memory_layer_budget)
        if summary:
            compact["dropped_memory_layer_budget"] = summary
    memory_layer_pressure = value.get("memory_layer_pressure")
    if isinstance(memory_layer_pressure, dict):
        summary = serving_memory_layer_pressure(memory_layer_pressure)
        if summary:
            compact["memory_layer_pressure"] = summary
    async_pipeline_readiness = value.get("async_pipeline_readiness")
    if isinstance(async_pipeline_readiness, dict):
        compact["async_pipeline_readiness"] = serving_async_pipeline_readiness(
            async_pipeline_readiness,
            include_debug=include_debug,
        )
    quality_first_underfill = value.get("quality_first_underfill")
    if isinstance(quality_first_underfill, dict):
        compact_underfill = {
            field: quality_first_underfill.get(field)
            for field in [
                "enabled",
                "unused_remote_context_tokens",
                "dropped_ref_count",
                "dropped_reason_counts",
            ]
            if quality_first_underfill.get(field) not in (None, "", [], {})
        }
        if compact_underfill.get("enabled"):
            compact["quality_first_underfill"] = compact_underfill
    memory_inventory = value.get("memory_inventory")
    if isinstance(memory_inventory, dict):
        compact["memory_inventory"] = memory_inventory
    pre_retrieval_summary_refresh = value.get("pre_retrieval_summary_refresh")
    if debug_lineage_enabled(include_debug=include_debug) and isinstance(pre_retrieval_summary_refresh, dict):
        compact["pre_retrieval_summary_refresh"] = {
            field: pre_retrieval_summary_refresh.get(field)
            for field in [
                "enabled",
                "status",
                "requested_limit",
                "refreshed_count",
                "compression_created_count",
                "skipped_dirty_count",
                "skipped_dirty_reasons",
                "elapsed_ms",
            ]
            if pre_retrieval_summary_refresh.get(field) not in (None, "", [], {})
        }
    return strip_default_debug_lineage_fields(compact)


def serving_async_pipeline_readiness(value: Any, *, include_debug: bool = False) -> Json:
    if not isinstance(value, dict):
        return {}
    if debug_lineage_enabled(include_debug=include_debug):
        return value
    return strip_default_debug_lineage_fields(value)


def selected_context_class_counts(refs: list[Json]) -> Json:
    counts: Json = {
        "event": 0,
        "entity": 0,
        "segment": 0,
        "compression": 0,
        "resource_fact": 0,
        "resource_entity_fact": 0,
        "resource_chunk": 0,
        "skill_section": 0,
        "summary": 0,
    }
    for ref in refs:
        context_class = str(ref.get("context_class") or ref.get("ref_type") or "")
        counts[context_class] = int(counts.get(context_class, 0)) + 1
    return counts



def _context_memory_source_ref_is_debug_only(ref: Json) -> bool:
    ref_type = str(ref.get("ref_type") or "").strip().lower()
    memory_scope = str(ref.get("memory_scope") or "").strip().lower()
    session_continuity = str(ref.get("session_continuity") or "").strip().lower()
    context_class = str(ref.get("context_class") or "").strip().lower()
    return (
        ref_type in {"event", "entity", "segment", "summary"}
        or context_class in {"event", "entity", "segment", "summary"}
        or memory_scope in {"session", "session_memory", "user_profile", "profile", "cross_session_profile"}
        or session_continuity in {"same_session", "cross_session"}
    )

def compact_context_pack_ref(ref: Json, *, include_debug: bool = False) -> Json:
    """Return the prompt-facing ContextPack ref shape.

    Audit records keep hashes, matched indexes, provider/debug fields, score
    breakdowns, raw URIs, and routing internals. The live ContextPack should
    spend tokens on evidence and citations.
    """
    item: Json = {}
    for field in [
        "ref_type",
        "text",
        "text_preview",
        "citation",
        "source_ref",
        "resource_type",
        "sharing_scope",
        "event_type",
        "source_role",
        "summary_type",
        "operator",
        "memory_scope",
        "session_continuity",
        "profile_memory_class",
        "profile_memory_kind",
        "profile_entity_current",
        "profile_summary_current",
        "entity_type",
        "entity_name",
        "profile_current_state_representative",
    ]:
        value = ref.get(field)
        if field == "source_ref" and _context_memory_source_ref_is_debug_only(ref) and not include_debug:
            continue
        if value not in (None, "", [], {}):
            item[field] = value
    memory_layer = _memory_layer_for_ref(ref)
    if memory_layer:
        item["memory_layer"] = memory_layer
    flat_debug_lineage_fields = [
        "source_session_ids",
        "source_roles",
        "budget_source_roles",
        "source_hook_types",
        "source_codex_events",
        "source_memory_selection_policies",
        "source_memory_scopes",
        "source_session_continuities",
        "source_extraction_phases",
        "source_entity_types",
        "source_profile_promotion_policies",
        "source_profile_promotion_blockers",
        "source_final_session_boundary_count",
        "source_event_ids",
        "source_role_counts",
        "budget_source_role_counts",
        "source_hook_type_counts",
        "source_codex_event_counts",
        "source_memory_selection_policy_counts",
        "source_memory_selection_lossy_count",
        "source_memory_selection_complete_count",
        "source_memory_selection_dropped_text_chars",
        "source_memory_selection_dropped_line_count",
        "source_memory_selection_retained_text_ratio_avg",
        "source_memory_selection_retained_line_ratio_avg",
        "profile_promotion_policy",
        "profile_promotion_blocker",
        "profile_entity_current",
        "profile_revision",
        "previous_profile_revision",
        "previous_profile_updated_at_ms",
        "supersedes_session_entity_hash",
    ] if debug_lineage_enabled(include_debug=include_debug) else []
    if debug_lineage_enabled(include_debug=include_debug):
        value = ref.get("extraction_phase")
        if value not in (None, "", [], {}):
            item["extraction_phase"] = value
        if bool(ref.get("final_session_boundary")):
            item["final_session_boundary"] = True
    for field in flat_debug_lineage_fields:
        value = ref.get(field)
        if isinstance(value, list) and value:
            if field in {"source_roles", "budget_source_roles"}:
                normalized_roles = _ordered_normalized_roles(value)
                if normalized_roles:
                    item[field] = normalized_roles[:8]
            else:
                item[field] = value[:8]
        elif isinstance(value, dict):
            compact_counts = (
                _compact_role_count_map(value)
                if field in {"source_role_counts", "budget_source_role_counts"}
                else _compact_count_map(value)
            )
            if compact_counts:
                item[field] = compact_counts
        elif isinstance(value, int) and value > 0:
            item[field] = value
        elif isinstance(value, float) and value > 0:
            item[field] = round(value, 6)
        elif isinstance(value, str) and value.strip():
            item[field] = value.strip()

    if debug_lineage_enabled(include_debug=include_debug):
        value = ref.get("extraction_context_event_ids")
        if isinstance(value, list) and value:
            item["extraction_context_event_ids"] = value[:8]
        value = ref.get("source_entity_hashes")
        if isinstance(value, list) and value:
            item["source_entity_count"] = len(value)
        value = ref.get("supersedes_session_entity_hashes")
        if isinstance(value, list) and value:
            item["supersedes_session_entity_hashes"] = value[:8]
            item["supersedes_session_entity_count"] = len(value)
        value = ref.get("source_entity_count")
        if isinstance(value, int) and value > 0:
            item["source_entity_count"] = value
        value = ref.get("source_event_count")
        if isinstance(value, int) and value > 0:
            item["source_event_count"] = value
        for field in ["source_record_type", "segment_origin"]:
            value = ref.get(field)
            if isinstance(value, str) and value.strip():
                item[field] = value.strip()
        if ref.get("derived_from_context_events") is True:
            item["derived_from_context_events"] = True

        for field in ["current_state_policy", "current_state_source_session_count", "current_state_source_entity_count"]:
            value = ref.get(field)
            if value not in (None, "", [], {}) and not (isinstance(value, int) and value <= 0):
                item[field] = value
    context_class = ref.get("context_class")
    if context_class and context_class != item.get("ref_type"):
        item["context_class"] = context_class
    if False:  # scores are not served; see native_serving_ref
        if "score" in ref:
            try:
                item["score"] = round(float(ref.get("score") or 0.0), 4)
            except (TypeError, ValueError):
                pass
        if "token_estimate" in ref:
            try:
                item["token_estimate"] = int(ref.get("token_estimate") or 0)
            except (TypeError, ValueError):
                pass
    metadata = ref.get("metadata")
    if isinstance(metadata, dict):
        compact_metadata = {
            field: metadata[field]
            for field in [
                "unit_kind",
                "heading",
                "relative_path",
                "page",
                "page_section",
                "slide_number",
                "sheet_name",
                "row_start",
                "row_end",
                "record_start",
                "record_end",
                "row_count",
                "citation",
            ]
            if metadata.get(field) not in (None, "", [], {})
        }
        if compact_metadata:
            item["metadata"] = compact_metadata
    return item


def compact_context_pack_refs(refs: list[Json], *, include_debug: bool = False) -> list[Json]:
    return [compact_context_pack_ref(ref, include_debug=include_debug) for ref in refs]


def compact_context_pack_policy(policy: Any) -> Json:
    if not isinstance(policy, dict):
        return {}
    keep_fields = [
        "enabled",
        "mode",
        "decision",
        "strategy",
        "quality_gate",
        "resource_mode",
        "skill_mode",
        "budget_semantics",
    ]
    compact: Json = {
        field: policy.get(field)
        for field in keep_fields
        if policy.get(field) not in (None, "", [], {})
    }
    is_layer_policy = isinstance(policy.get("selected_tokens_by_layer"), dict) or isinstance(policy.get("selected_ref_count_by_layer"), dict)
    for field in [
        "selected_ref_count",
        "selected_session_count",
        "resource_selected_ref_count",
        "skill_selected_ref_count",
        "entity_bridge_selected_ref_count",
        "selected_tokens",
        "resource_selected_tokens",
        "skill_selected_tokens",
        "remote_budget_tokens",
    ]:
        value = policy.get(field)
        if isinstance(value, int) and value > 0:
            compact[field] = value
    for field in [
        "derived",
        "independent_caps",
        "global_remote_budget_enforced",
    ]:
        value = policy.get(field)
        if isinstance(value, bool):
            compact[field] = value
    for field in [
        "budget_tokens",
        "selected_tokens_by_role",
        "selected_ref_count_by_role",
    ]:
        value = policy.get(field)
        if isinstance(value, dict) and value:
            if field == "budget_tokens" and is_layer_policy:
                compact[field] = {
                    str(key): int(amount)
                    for key, amount in value.items()
                    if str(key or "").strip() and isinstance(amount, int)
                }
            else:
                compact[field] = _compact_role_count_map(value, include_zero=True)
    for field in [
        "selected_tokens_by_layer",
        "selected_ref_count_by_layer",
    ]:
        value = policy.get(field)
        if isinstance(value, dict) and value:
            compact[field] = {
                str(key): int(amount)
                for key, amount in value.items()
                if str(key or "").strip() and isinstance(amount, int)
            }
    return compact


def compact_dropped_refs_for_context_pack(dropped: Json, *, include_debug: bool = False) -> Json:
    if include_debug or not isinstance(dropped, dict):
        return dropped
    compact: Json = {
        key: value
        for key, value in dropped.items()
        if isinstance(value, int) and value
    }
    estimated = dropped.get("estimated_tokens")
    if isinstance(estimated, dict):
        compact_estimated = {key: value for key, value in estimated.items() if isinstance(value, int) and value}
        if compact_estimated:
            compact["estimated_tokens"] = compact_estimated
    for field in [
        "deadline_exceeded",
        "deadline_reason",
        "min_score",
        "budget_fill_policy",
    ]:
        value = dropped.get(field)
        if value not in (None, "", [], {}):
            compact[field] = value
    for field in ["cross_session_policy", "shared_context_policy", "source_role_budget_policy", "memory_layer_budget_policy"]:
        value = compact_context_pack_policy(dropped.get(field))
        if value:
            compact[field] = value
    if dropped.get("refs"):
        compact["dropped_ref_detail_available_in_audit"] = True
        compact["dropped_ref_count"] = len(dropped.get("refs") or [])
    return compact


def compact_recall_policy_for_audit(recall_policy: Json, *, include_debug: bool = False) -> Json:
    if not isinstance(recall_policy, dict):
        return {}
    tree = recall_policy.get("tree_traversal") if isinstance(recall_policy.get("tree_traversal"), dict) else {}
    secondary = recall_policy.get("secondary_index_filter") if isinstance(recall_policy.get("secondary_index_filter"), dict) else {}
    rerank = recall_policy.get("rerank") if isinstance(recall_policy.get("rerank"), dict) else {}
    hard_deadline = recall_policy.get("hard_deadline") if isinstance(recall_policy.get("hard_deadline"), dict) else {}
    session = recall_policy.get("session_continuity") if isinstance(recall_policy.get("session_continuity"), dict) else {}
    storage_options = recall_policy.get("storage_options") if isinstance(recall_policy.get("storage_options"), dict) else {}
    compact: Json = {}
    query_plan = recall_policy.get("query_plan")
    if isinstance(query_plan, dict):
        compact["query_plan"] = {
            field: query_plan[field]
            for field in ["query_type", "temporal_window", "secondary_filters"]
            if query_plan.get(field) not in (None, "", [], {})
        }
    if tree:
        compact["tree"] = {
            field: tree.get(field)
            for field in [
                "enabled",
                "fallback_to_flat",
                "selected_node_count",
                "selected_leaf_count",
                "candidate_records_after_tree",
                "records_dropped_by_tree",
                "max_candidates_per_node",
            ]
            if tree.get(field) not in (None, "", [], {})
        }
    if secondary:
        compact["secondary_index"] = {
            field: secondary.get(field)
            for field in ["enabled", "matched_candidate_count", "dropped_candidate_count", "effective_mode"]
            if secondary.get(field) not in (None, "", [], {})
        }
    if rerank:
        compact["rerank"] = {
            field: rerank.get(field)
            for field in ["enabled", "mode", "reranked_candidate_count", "min_similarity_score", "budget_fill_policy"]
            if rerank.get(field) not in (None, "", [], {})
        }
    if session:
        compact["session_continuity"] = {
            field: session.get(field)
            for field in ["mode", "same_session_selected_ref_count", "cross_session_selected_ref_count", "entity_bridge_selected_ref_count"]
            if session.get(field) not in (None, "", [], {})
        }
    memory_layer_budget = recall_policy.get("memory_layer_budget")
    if isinstance(memory_layer_budget, dict):
        compact["memory_layer_budget"] = serving_memory_layer_budget(memory_layer_budget)
    dropped_memory_layer_budget = recall_policy.get("dropped_memory_layer_budget")
    if isinstance(dropped_memory_layer_budget, dict):
        compact["dropped_memory_layer_budget"] = serving_memory_layer_budget(dropped_memory_layer_budget)
    memory_layer_pressure = recall_policy.get("memory_layer_pressure")
    if isinstance(memory_layer_pressure, dict):
        compact["memory_layer_pressure"] = serving_memory_layer_pressure(memory_layer_pressure)
    async_pipeline_readiness = recall_policy.get("async_pipeline_readiness")
    if isinstance(async_pipeline_readiness, dict):
        compact["async_pipeline_readiness"] = serving_async_pipeline_readiness(
            async_pipeline_readiness,
            include_debug=include_debug,
        )
    if storage_options:
        compact["storage_route"] = {
            field: storage_options.get(field)
            for field in ["route", "storage_family", "write_mode", "durability_result", "background_write"]
            if storage_options.get(field) not in (None, "", [], {})
        }
    if hard_deadline:
        compact["deadline"] = {
            field: hard_deadline.get(field)
            for field in ["deadline_ms", "elapsed_ms", "partial_context_pack", "fallback_reason"]
            if hard_deadline.get(field) not in (None, "", [], {})
        }
    return compact


def compact_context_pack_audit_record(record: Json, *, include_debug: bool = False) -> Json:
    if include_debug or AUDIT_DEBUG_PAYLOAD:
        return record
    compact: Json = {
        "record_type": record.get("record_type", "context_pack_audit"),
        "context_pack_id": record.get("context_pack_id", ""),
        "query": record.get("query", ""),
        "summary_text": record.get("summary_text", ""),
        "selected_refs": compact_context_pack_refs(record.get("selected_refs", []), include_debug=False),
        "selected_ref_counts": record.get("selected_ref_counts", {}),
        "dropped_refs": compact_dropped_refs_for_context_pack(record.get("dropped_refs", {}), include_debug=False),
        "quality_warnings": record.get("quality_warnings", []),
        "partial_context_pack": bool(record.get("partial_context_pack", False)),
        "question_type": record.get("question_type", ""),
        "packing_policy": record.get("packing_policy", ""),
        "used_local_context_tokens": record.get("used_local_context_tokens", 0),
        "used_remote_context_tokens": record.get("used_remote_context_tokens", 0),
        "total_prompt_context_tokens": record.get("total_prompt_context_tokens", 0),
        "remote_context_budget_tokens": record.get("remote_context_budget_tokens", 0),
        "requested_max_context_tokens": record.get("requested_max_context_tokens", 0),
        "primary_candidate_count": record.get("primary_candidate_count", 0),
        "auxiliary_candidate_count": record.get("auxiliary_candidate_count", 0),
        "tree_prefilter_dropped_count": record.get("tree_prefilter_dropped_count", 0),
        "fanout_dropped_count": record.get("fanout_dropped_count", 0),
        "max_candidates_per_node": record.get("max_candidates_per_node", 0),
        "max_selected_refs": record.get("max_selected_refs", 0),
        "created_at_ms": record.get("created_at_ms"),
        "payload_policy": {
            "mode": "compact_audit",
            "verbose_with": "MATRIXARK_AUDIT_DEBUG_PAYLOAD=1 or replay include_debug_records=true",
        },
    }
    recall_summary = compact_recall_policy_for_audit(record.get("recall_policy", {}))
    if recall_summary:
        compact["recall_policy_summary"] = recall_summary
    recall_policy = record.get("recall_policy", {})
    if isinstance(recall_policy, dict):
        try:
            from tools.matrixark_mcp_core import memory_hierarchy_contract_from_recall_policy
        except ModuleNotFoundError:  # Direct script execution from tools/.
            from matrixark_mcp_core import memory_hierarchy_contract_from_recall_policy
        memory_hierarchy = memory_hierarchy_contract_from_recall_policy(recall_policy)
        if memory_hierarchy and include_debug:
            compact["memory_hierarchy"] = memory_hierarchy
    memory_layer_budget = record.get("memory_layer_budget")
    if not isinstance(memory_layer_budget, dict):
        recall_policy = record.get("recall_policy") if isinstance(record.get("recall_policy"), dict) else {}
        memory_layer_budget = recall_policy.get("memory_layer_budget")
    if isinstance(memory_layer_budget, dict):
        compact["memory_layer_budget"] = serving_memory_layer_budget(memory_layer_budget)
    dropped_memory_layer_budget = record.get("dropped_memory_layer_budget")
    if not isinstance(dropped_memory_layer_budget, dict):
        recall_policy = record.get("recall_policy") if isinstance(record.get("recall_policy"), dict) else {}
        dropped_memory_layer_budget = recall_policy.get("dropped_memory_layer_budget")
    if isinstance(dropped_memory_layer_budget, dict):
        compact["dropped_memory_layer_budget"] = serving_memory_layer_budget(dropped_memory_layer_budget)
    memory_layer_pressure = record.get("memory_layer_pressure")
    if not isinstance(memory_layer_pressure, dict):
        recall_policy = record.get("recall_policy") if isinstance(record.get("recall_policy"), dict) else {}
        memory_layer_pressure = recall_policy.get("memory_layer_pressure")
    if isinstance(memory_layer_pressure, dict):
        compact["memory_layer_pressure"] = serving_memory_layer_pressure(memory_layer_pressure)
    async_pipeline_readiness = record.get("async_pipeline_readiness")
    if not isinstance(async_pipeline_readiness, dict):
        recall_policy = record.get("recall_policy") if isinstance(record.get("recall_policy"), dict) else {}
        async_pipeline_readiness = recall_policy.get("async_pipeline_readiness")
    if isinstance(async_pipeline_readiness, dict):
        compact["async_pipeline_readiness"] = serving_async_pipeline_readiness(
            async_pipeline_readiness,
            include_debug=include_debug,
        )
    local_policy = record.get("local_context_policy")
    if isinstance(local_policy, dict):
        compact["local_context_policy"] = {
            field: local_policy.get(field)
            for field in ["local_context_count", "local_context_tokens", "safety_margin_tokens", "remote_is_additive_only_within_remaining_budget"]
            if local_policy.get(field) not in (None, "", [], {})
        }
    visibility = record.get("operational_visibility_policy")
    if isinstance(visibility, dict):
        compact["operational_visibility_policy"] = {
            field: visibility.get(field)
            for field in ["audit_mode", "audit_sample_rate", "rich_replay_audit", "telemetry_record"]
            if visibility.get(field) not in (None, "", [], {})
        }
    return {key: value for key, value in compact.items() if value not in (None, "", [], {})}


def compact_refs_for_audit(refs: list[Json], *, preview_chars: int = 160) -> list[Json]:
    compact: list[Json] = []
    keep_fields = [
        "ref_type",
        "ref_hash",
        "node_hash",
        "node_path",
        "scope",
        "recall_path",
        "score",
        "origin_score",
        "time_score",
        "business_score",
        "embedding_score",
        "sparse_score",
        "keyword_score",
        "token_estimate",
        "updated_at_ms",
        "selection_reason",
        "matched_index_terms",
        "resource_hash",
        "raw_uri_hash",
        "source_locator",
        "resource_type",
        "resource_version",
        "context_class",
        "source_chunk_hash",
        "access_decision",
        "access_scope",
        "deployment_scope",
        "version_state",
        "stale_or_superseded",
        "citation",
        "operator",
        "source_start_ms",
        "source_end_ms",
        "source_event_ids",
        "summary_type",
        "memory_scope",
        "session_continuity",
        "entity_type",
        "entity_name",
        "extraction_phase",
        "profile_current_state_representative",
        "current_state_policy",
        "final_session_boundary",
        "source_roles",
        "source_role_counts",
        "source_hook_types",
        "source_hook_type_counts",
        "source_codex_events",
        "source_codex_event_counts",
        "source_memory_selection_policies",
        "source_memory_selection_policy_counts",
        "source_memory_scopes",
        "source_session_continuities",
        "source_extraction_phases",
        "source_profile_promotion_policies",
        "source_profile_promotion_blockers",
        "source_entity_types",
        "source_final_session_boundary_count",
        "current_state_source_session_count",
        "current_state_source_entity_count",
        "continuity_boost",
        "continuity_reason",
    ]
    metadata_keep_fields = [
        "unit_kind",
        "heading",
        "heading_slug",
        "heading_path",
        "relative_path",
        "keywords",
        "source_locator",
        "resource_version",
        "row_start",
        "row_end",
        "record_start",
        "record_end",
        "page",
        "page_section",
        "slide_number",
        "sheet_name",
        "row_count",
        "supersedes_chunk_hash",
        "parse_warnings",
    ]
    for ref in refs:
        item = {field: ref[field] for field in keep_fields if field in ref}
        for field in ["source_roles"]:
            value = item.get(field)
            if isinstance(value, list):
                roles = _ordered_normalized_roles(value)
                if roles:
                    item[field] = roles
        for field in ["source_role_counts"]:
            value = item.get(field)
            role_counts = _compact_role_count_map(value)
            if role_counts:
                item[field] = role_counts
        metadata = ref.get("metadata", {})
        if isinstance(metadata, dict):
            compact_metadata = {field: metadata[field] for field in metadata_keep_fields if field in metadata}
            if compact_metadata:
                item["metadata"] = compact_metadata
        text = str(ref.get("text", ""))
        if text:
            item["text_preview"] = _clip_context_text(text, max_chars=preview_chars)
        compact.append(item)
    return compact


def _memory_layer_for_ref(ref: Json) -> str:
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
        return "pending_async_event"
    sharing_scope = str(ref.get("sharing_scope") or metadata.get("sharing_scope") or "").strip().lower()
    ref_type = str(ref.get("ref_type") or "")
    if sharing_scope in {"tenant_shared", "global_shared"} or ref_type in {"resource_chunk", "skill_section"}:
        return "shared_context"
    memory_scope = str(ref.get("memory_scope") or metadata.get("memory_scope") or "").strip().lower()
    session_continuity = str(ref.get("session_continuity") or metadata.get("session_continuity") or "").strip().lower()
    profile_memory_kind = str(ref.get("profile_memory_kind") or metadata.get("profile_memory_kind") or "").strip().lower()
    source_profile_memory_kinds = {
        str(value or "").strip().lower()
        for value in (
            ref.get("source_profile_memory_kinds")
            if isinstance(ref.get("source_profile_memory_kinds"), list)
            else metadata.get("source_profile_memory_kinds", [])
            if isinstance(metadata.get("source_profile_memory_kinds"), list)
            else []
        )
        if str(value or "").strip()
    }
    event_type = str(ref.get("event_type") or metadata.get("event_type") or ref.get("entity_type") or metadata.get("entity_type") or "").strip().lower()
    codex_outcome_types = {
        "assistant_response",
        "assistant_decision",
        "tool_evidence",
        "codex_next_action",
        "codex_blocker",
        "codex_publish_outcome",
        "codex_code_change",
        "codex_benchmark_result",
    }
    is_codex_outcome_memory = profile_memory_kind == "codex_outcome" or "codex_outcome" in source_profile_memory_kinds or event_type in codex_outcome_types
    is_memory_feature_memory = profile_memory_kind == "memory_feature" or "memory_feature" in source_profile_memory_kinds
    if memory_scope in {"user_profile", "profile", "cross_session_profile"} and session_continuity == "cross_session":
        if is_codex_outcome_memory:
            return "cross_session_codex_outcome"
        if is_memory_feature_memory:
            ref_kind = str(ref.get("ref_type") or metadata.get("ref_type") or ref.get("context_class") or metadata.get("context_class") or "").strip().lower()
            if ref_kind in {"entity", "context_entity", "profile_entity"}:
                return "cross_session_memory_feature_entity"
            if ref_kind in {"summary", "context_summary"}:
                return "cross_session_memory_feature_summary"
            if ref_kind in {"compression", "context_compression_event"}:
                return "cross_session_memory_feature_compression"
            return "cross_session_memory_feature_entity"
    if memory_scope in {"user_profile", "profile", "cross_session_profile"}:
        return "profile"
    if memory_scope in {"session", "session_memory"}:
        return "session"
    if session_continuity == "same_session":
        return "session"
    if session_continuity == "cross_session":
        context_class = str(ref.get("context_class") or ref_type)
        if ref_type == "entity" or context_class in {"resource_entity_fact", "profile_entity", "user_profile"}:
            return "profile"
        return "cross_session"
    return ""


def _memory_layer_counts(refs: list[Json]) -> Json:
    counts: Json = {}
    for ref in refs:
        layer = _memory_layer_for_ref(ref)
        if layer:
            counts[layer] = int(counts.get(layer, 0)) + 1
    return counts


def _default_memory_layer_for_pack(refs: list[Json]) -> str:
    counts = _memory_layer_counts(refs)
    if not counts:
        return ""
    return max(counts.items(), key=lambda item: (item[1], item[0]))[0]


def serving_ref_for_pack(ref: Json, *, default_session_continuity: str = "", default_memory_layer: str = "", include_debug: bool = False) -> Json:
    """Return only answer-bearing fields for the serving ContextPack payload."""
    metadata = ref.get("metadata", {}) if isinstance(ref.get("metadata"), dict) else {}
    item: Json = {
        "text": ref.get("text", ""),
    }
    memory_source_ref_debug_only = _context_memory_source_ref_is_debug_only(ref)
    source = ref.get("citation") or ref.get("source_locator") or metadata.get("source_locator")
    if not source and not memory_source_ref_debug_only:
        source = ref.get("source_ref")
    if source:
        item["source"] = source
    if debug_lineage_enabled(include_debug=include_debug) and memory_source_ref_debug_only:
        source_ref = ref.get("source_ref") or metadata.get("source_ref")
        if source_ref not in (None, "", [], {}):
            item["source_ref"] = source_ref
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
        ("profile_memory_kind", "profile_memory_kind"),
        ("profile_memory_class", "profile_memory_class"),
        ("profile_entity_current", "profile_entity_current"),
        ("profile_summary_current", "profile_summary_current"),
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
    memory_layer = _memory_layer_for_ref(ref)
    if memory_layer and memory_layer != default_memory_layer:
        item["memory_layer"] = memory_layer
    debug_lineage_fields = [
        "source_session_ids",
        "source_roles",
        "source_hook_types",
        "source_codex_events",
        "source_memory_selection_policies",
        "source_memory_scopes",
        "source_session_continuities",
        "source_extraction_phases",
        "source_entity_types",
        "source_profile_promotion_policies",
        "source_profile_promotion_blockers",
        "source_final_session_boundary_count",
        "source_event_ids",
        "source_role_counts",
        "source_hook_type_counts",
        "source_codex_event_counts",
        "source_memory_selection_policy_counts",
    ] if debug_lineage_enabled(include_debug=include_debug) else []
    if debug_lineage_enabled(include_debug=include_debug):
        value = ref.get("extraction_phase", metadata.get("extraction_phase"))
        if value not in (None, "", [], {}):
            item["extraction_phase"] = value
        if bool(ref.get("final_session_boundary") or metadata.get("final_session_boundary")):
            item["final_session_boundary"] = True
    for field in debug_lineage_fields:
        value = ref.get(field, metadata.get(field))
        if isinstance(value, list) and value:
            if field == "source_roles":
                roles = _ordered_normalized_roles(value)
                if roles:
                    item[field] = roles[:8]
            else:
                item[field] = value[:8]
        elif isinstance(value, dict):
            compact_counts = _compact_role_count_map(value) if field == "source_role_counts" else _compact_count_map(value)
            if compact_counts:
                item[field] = compact_counts

    if debug_lineage_enabled(include_debug=include_debug):
        value = ref.get("source_entity_hashes", metadata.get("source_entity_hashes"))
        if isinstance(value, list) and value:
            item["source_entity_count"] = len(value)
        value = ref.get("source_entity_count", metadata.get("source_entity_count"))
        if isinstance(value, int) and value > 0:
            item["source_entity_count"] = value
        value = ref.get("source_event_count", metadata.get("source_event_count"))
        if isinstance(value, int) and value > 0:
            item["source_event_count"] = value
        for field in ["source_record_type", "segment_origin"]:
            value = ref.get(field, metadata.get(field))
            if isinstance(value, str) and value.strip():
                item[field] = value.strip()
        if ref.get("derived_from_context_events") is True or metadata.get("derived_from_context_events") is True:
            item["derived_from_context_events"] = True
        value = ref.get("current_state_policy", metadata.get("current_state_policy"))
        if value not in (None, "", [], {}):
            item["current_state_policy"] = value
        for field in ["current_state_source_session_count", "current_state_source_entity_count"]:
            value = ref.get(field, metadata.get(field))
            if isinstance(value, int) and value > 0:
                item[field] = value
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


def serving_refs_for_pack(refs: list[Json], *, default_session_continuity: str = "", default_memory_layer: str = "", include_debug: bool = False) -> list[Json]:
    return [
        serving_ref_for_pack(
            ref,
            default_session_continuity=default_session_continuity,
            default_memory_layer=default_memory_layer,
            include_debug=include_debug,
        )
        for ref in refs
    ]


def strip_raw_debug_payload_fields(value: Any) -> Any:
    """Remove unbounded debug payloads while preserving compact debug counters."""
    raw_debug_fields = {
        "debug",
        "debug_payload",
        "debug_refs",
        "debug_record",
        "metadata_debug",
        "memory_lineage",
        "lineage",
    }
    if isinstance(value, dict):
        compact: Json = {}
        for key, item in value.items():
            name = str(key or "")
            lowered = name.lower()
            if name in raw_debug_fields or "debug_" in lowered or "_debug" in lowered:
                continue
            if name == "source_entity_hashes" and isinstance(item, list):
                compact["source_entity_count"] = len(item)
                continue
            compact[key] = strip_raw_debug_payload_fields(item)
        return compact
    if isinstance(value, list):
        return [strip_raw_debug_payload_fields(item) for item in value]
    return value


def _bound_prebuilt_serving_lineage_item(item: Json) -> Json:
    compact = strip_raw_debug_payload_fields(item)
    for field in [
        "source_session_ids",
        "source_hook_types",
        "source_codex_events",
        "source_memory_selection_policies",
        "source_memory_scopes",
        "source_session_continuities",
        "source_extraction_phases",
        "source_entity_types",
        "source_profile_promotion_policies",
        "source_profile_promotion_blockers",
        "source_event_ids",
    ]:
        value = compact.get(field)
        if isinstance(value, list):
            compact[field] = value[:8]
    for field in ["source_event_count", "source_record_type", "segment_origin", "derived_from_context_events"]:
        value = compact.get(field)
        if value in (None, "", [], {}) or (isinstance(value, int) and value <= 0) or value is False:
            compact.pop(field, None)
    for field in ["source_roles", "budget_source_roles"]:
        value = compact.get(field)
        if isinstance(value, list):
            roles = _ordered_normalized_roles(value)
            if roles:
                compact[field] = roles[:8]
            else:
                compact.pop(field, None)
    for field in ["source_role_counts", "budget_source_role_counts"]:
        compact_counts = _compact_role_count_map(compact.get(field))
        if compact_counts:
            compact[field] = compact_counts
        else:
            compact.pop(field, None)
    for field in ["source_hook_type_counts", "source_codex_event_counts"]:
        compact_counts = _compact_count_map(compact.get(field))
        if compact_counts:
            compact[field] = compact_counts
        else:
            compact.pop(field, None)
    value = compact.get("source_entity_hashes")
    if isinstance(value, list) and value:
        compact["source_entity_count"] = len(value)
        compact.pop("source_entity_hashes", None)
    return compact


def compact_prebuilt_serving_groups(groups: list[Json], *, include_debug: bool = False) -> list[Json]:
    include_lineage = debug_lineage_enabled(include_debug=include_debug)
    compact_groups: list[Json] = []
    for group in groups:
        if not isinstance(group, dict):
            continue
        compact_group = _bound_prebuilt_serving_lineage_item(group) if include_lineage else strip_default_debug_lineage_fields(group)
        items = group.get("items")
        if isinstance(items, list):
            compact_group["items"] = [
                _bound_prebuilt_serving_lineage_item(item) if include_lineage else compact_context_pack_ref(item)
                for item in items
                if isinstance(item, dict)
            ]
        compact_groups.append(compact_group)
    return compact_groups


def _normalized_item_text(item: Json) -> str:
    text = str(item.get("text") or "")
    if "=" in text:
        text = text.split("=", 1)[1]
    return " ".join(text.split()).strip().lower().rstrip(".")


def drop_redundant_pack_items(groups: list[Json]) -> list[Json]:
    """Remove items whose content a LONGER item elsewhere in the pack already carries.

    An entity item is a projection of the event it came from, so one fact is commonly shipped twice:
    ``user: I live in Kyoto and my favorite drink is matcha.`` and, in the entity group,
    ``preference: preference = drink is matcha``. Both are billed to the reader's token budget.

    Only a strict containment is dropped, comparing the entity's VALUE half (after ``=``) against the
    other item's text, so nothing unique is lost -- an entity that adds a name, a type, or a value not
    literally present in the kept item survives. The longer item wins because it carries the
    surrounding context that makes the fact usable.

    Group ``n`` is recomputed, and a group emptied by the sweep is dropped entirely."""
    surviving: list[tuple[Json, list[Json]]] = []
    everything: list[tuple[str, Json]] = []
    for group in groups:
        for item in group.get("items") or []:
            everything.append((_normalized_item_text(item), item))
    if not everything:
        return groups

    redundant: set[int] = set()
    for text, item in everything:
        if not text or len(text) < 8:
            continue
        for other_text, other in everything:
            if other is item or id(other) in redundant:
                continue
            if len(other_text) <= len(text):
                continue
            if text in other_text:
                redundant.add(id(item))
                break
    if not redundant:
        return groups

    for group in groups:
        kept = [item for item in (group.get("items") or []) if id(item) not in redundant]
        if not kept:
            continue
        trimmed = dict(group)
        trimmed["items"] = kept
        trimmed["n"] = len(kept)
        surviving.append((trimmed, kept))
    return [group for group, _kept in surviving]


def _pack_redundancy_filter_enabled() -> bool:
    """Whether to drop pack items another item already carries.

    Resolved through the per-tenant policy layer when that layer is present, and from the environment
    otherwise. The fallback is what keeps this module standalone: the pack builder is useful without
    the policy layer, and a hard import would make it unimportable wherever that layer is not shipped
    -- which is exactly what happened (ModuleNotFoundError at pack-build time). Default ON either
    way, so behaviour does not change with the layer's presence."""
    try:
        try:
            from tools.matrixark_index_growth_bound import pack_drop_redundant_items_enabled
        except ImportError:  # Direct script execution from tools/.
            from matrixark_index_growth_bound import pack_drop_redundant_items_enabled
    except ImportError:  # Policy layer not shipped here.
        return _os.environ.get("MATRIXARK_PACK_DROP_REDUNDANT_ITEMS", "1").strip().lower() not in {
            "0", "false", "no", "off"}
    return bool(pack_drop_redundant_items_enabled(None))


def serving_ref_groups_for_pack(refs: list[Json], *, default_session_continuity: str = "", default_memory_layer: str = "", include_debug: bool = False) -> list[Json]:
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
            include_debug=include_debug,
        )
        groups[key]["items"].append(item)
        groups[key]["n"] += 1
    built = [groups[key] for key in order]
    if _pack_redundancy_filter_enabled():
        built = drop_redundant_pack_items(built)
    return built


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


def serving_retrieval_decision(recall_policy: Json) -> Json:
    if not isinstance(recall_policy, dict) or not recall_policy:
        return {}
    query_plan = recall_policy.get("query_plan") if isinstance(recall_policy.get("query_plan"), dict) else {}
    cross_session = recall_policy.get("cross_session") if isinstance(recall_policy.get("cross_session"), dict) else {}
    query_type = str(query_plan.get("query_type") or cross_session.get("question_type") or "").strip()
    decision: Json = {}
    if query_type:
        decision["query_type"] = query_type
    if cross_session:
        enabled = bool(cross_session.get("enabled"))
        budget_ratio = cross_session.get("budget_ratio")
        budget_class = "custom"
        try:
            ratio = round(float(budget_ratio), 2)
        except (TypeError, ValueError):
            ratio = None
        if not enabled:
            budget_class = "disabled"
        elif ratio == 0.12:
            budget_class = "normal_12_percent"
        elif ratio == 0.15:
            budget_class = "broad_or_evidence_15_percent"
        elif ratio == 0.20:
            budget_class = "current_latest_multi_hop_or_date_20_percent"
        compact_cross = {
            "enabled": enabled,
            "mode": cross_session.get("mode") or ("prefer" if enabled else "disabled"),
            "budget_class": budget_class,
        }
        for field in ["decision", "question_budget_reason", "strategy", "budget_floor_status"]:
            value = cross_session.get(field)
            if value not in (None, "", [], {}):
                compact_cross[field] = value
        decision["cross_session"] = compact_cross
    return decision


def compact_context_pack_for_serving(pack: Json, *, include_debug: bool = False) -> Json:
    """Strip planner/audit/debug fields from the default returned ContextPack.

    Full retrieval policy, score details, dropped refs, storage mode, model
    fallback flags, and operational visibility live in ContextPackAudit or
    telemetry records when enabled. The serving pack should spend tokens on
    evidence and citations.
    """
    compact: Json = {"context_pack_id": pack.get("context_pack_id") or pack.get("pack_id") or ""}
    selected_refs = pack.get("selected_refs", [])
    if isinstance(selected_refs, list) and (selected_refs or not isinstance(pack.get("groups"), list)):
        default_session_continuity = default_session_continuity_for_pack(selected_refs)
        default_memory_layer = _default_memory_layer_for_pack(selected_refs)
        compact["groups"] = serving_ref_groups_for_pack(
            selected_refs,
            default_session_continuity=default_session_continuity,
            default_memory_layer=default_memory_layer,
            include_debug=include_debug,
        )
        if pack.get("selected_ref_counts"):
            compact.setdefault("counts", {})["refs"] = pack.get("selected_ref_counts", {})
        continuity_counts = session_continuity_counts(selected_refs)
        if continuity_counts:
            compact.setdefault("defaults", {})["session_continuity"] = default_session_continuity
            compact.setdefault("counts", {})["session_continuity"] = continuity_counts
        layer_counts = _memory_layer_counts(selected_refs)
        if layer_counts:
            compact.setdefault("defaults", {})["memory_layer"] = default_memory_layer
            compact.setdefault("counts", {})["memory_layer"] = layer_counts
    elif isinstance(pack.get("groups"), list):
        # Some adapters already return the serving shape. Preserve it so a
        # second compaction pass in the MCP entrypoint does not erase refs.
        compact["groups"] = compact_prebuilt_serving_groups(
            pack.get("groups", []),
            include_debug=include_debug,
        )
        if isinstance(pack.get("counts"), dict):
            compact["counts"] = pack.get("counts", {})
        if isinstance(pack.get("defaults"), dict):
            compact["defaults"] = pack.get("defaults", {})
    local_refs = pack.get("local_context_refs", [])
    if isinstance(local_refs, list):
        local = [
            {
                ("tokens" if key == "token_estimate" else key): value
                for key, value in ref.items()
                if key in {"source", "token_estimate", "text"} and value not in (None, "", [], {})
            }
            for ref in local_refs
            if isinstance(ref, dict)
        ]
        if local:
            compact["local"] = local
    tokens_summary = {
        "remote": pack.get("used_remote_context_tokens", pack.get("used_context_tokens", 0)),
        "local": pack.get("used_local_context_tokens", 0),
        "total": pack.get("total_prompt_context_tokens", 0),
        "remote_budget": pack.get("remote_context_budget_tokens", 0),
    }
    compact["tokens"] = {key: value for key, value in tokens_summary.items() if value not in (None, "", 0)}
    if not compact["tokens"] and isinstance(pack.get("tokens"), dict):
        compact["tokens"] = pack.get("tokens", {})
    if pack.get("quality_warnings"):
        compact["warnings"] = pack.get("quality_warnings", [])
    # Vectors the query could not compare against. Carried only when something was actually
    # declined: a healthy deployment should not pay bytes on every pack to be told nothing
    # happened, and a caller reading this key at all is reading about a real problem.
    # Which implementation answered, and how it ordered the results. Carried always, not only when
    # something went wrong: "why do these two deployments rank differently" is a question asked
    # about working systems.
    served = pack.get("served_by")
    if not isinstance(served, dict):
        metrics = pack.get("retrieval_metrics")
        metrics = metrics if isinstance(metrics, dict) else {}
        assembly = (pack.get("context_pack_assembly")
                    or metrics.get("source")
                    or ("native_backend" if pack.get("native_context_pack") else ""))
        served = {
            # Empty rather than "unknown": a pack whose producer is not identifiable carries no
            # claim at all. Printing "answered by unknown" is noise dressed as provenance.
            "assembly": str(assembly or ""),
            # Absent means the producer did not say, which is not the same as "no vectors".
            "ranking": metrics.get("ranking"),
            "ranking_uses_vectors": metrics.get("ranking_uses_vectors"),
        }
    if served.get("assembly"):
        compact["served_by"] = {key: value for key, value in served.items() if value is not None}

    conflicts = pack.get("embedding_conflicts")
    if isinstance(conflicts, dict) and (conflicts.get("encoder_change") or
                                        conflicts.get("vector_width")):
        compact["embedding_conflicts"] = {
            key: value for key, value in conflicts.items() if value not in (None, "", 0)
        }
    if debug_lineage_enabled(include_debug=include_debug) and isinstance(pack.get("memory_inventory"), dict):
        compact["memory_inventory"] = pack["memory_inventory"]
    if pack.get("partial_context_pack"):
        compact["partial"] = True
    if pack.get("insufficient_context"):
        compact["insufficient_context"] = True
    if pack.get("include_retrieval_metrics"):
        compact["include_retrieval_metrics"] = True
    if (
        isinstance(pack.get("retrieval_metrics"), dict)
        and (pack.get("include_retrieval_metrics") or include_debug)
    ):
        compact["retrieval_metrics"] = serving_retrieval_metrics(pack["retrieval_metrics"], include_debug=include_debug)
    retrieval_metrics = pack.get("retrieval_metrics") if isinstance(pack.get("retrieval_metrics"), dict) else {}
    recall_policy = pack.get("recall_policy") if isinstance(pack.get("recall_policy"), dict) else {}
    retrieval_decision = serving_retrieval_decision(recall_policy)
    if retrieval_decision:
        compact["retrieval_decision"] = retrieval_decision
    memory_selection_policy_budget = (
        pack.get("memory_selection_policy_budget")
        if isinstance(pack.get("memory_selection_policy_budget"), dict)
        else retrieval_metrics.get("memory_selection_policy_budget")
        if isinstance(retrieval_metrics.get("memory_selection_policy_budget"), dict)
        else recall_policy.get("memory_selection_policy_budget_policy")
        if isinstance(recall_policy.get("memory_selection_policy_budget_policy"), dict)
        else {}
    )
    compact_memory_selection_policy_budget = serving_memory_selection_policy_budget(memory_selection_policy_budget)
    if compact_memory_selection_policy_budget:
        compact["memory_selection_policy_budget"] = compact_memory_selection_policy_budget
    memory_layer_budget = (
        retrieval_metrics.get("memory_layer_budget")
        if isinstance(retrieval_metrics.get("memory_layer_budget"), dict)
        else recall_policy.get("memory_layer_budget")
        if isinstance(recall_policy.get("memory_layer_budget"), dict)
        else {}
    )
    if debug_lineage_enabled(include_debug=include_debug) and isinstance(memory_layer_budget, dict) and memory_layer_budget:
        compact["memory_layer_budget"] = serving_memory_layer_budget(memory_layer_budget)
    dropped_memory_layer_budget = (
        retrieval_metrics.get("dropped_memory_layer_budget")
        if isinstance(retrieval_metrics.get("dropped_memory_layer_budget"), dict)
        else recall_policy.get("dropped_memory_layer_budget")
        if isinstance(recall_policy.get("dropped_memory_layer_budget"), dict)
        else {}
    )
    if debug_lineage_enabled(include_debug=include_debug) and isinstance(dropped_memory_layer_budget, dict) and dropped_memory_layer_budget:
        compact["dropped_memory_layer_budget"] = serving_memory_layer_budget(dropped_memory_layer_budget)
    memory_layer_pressure = (
        retrieval_metrics.get("memory_layer_pressure")
        if isinstance(retrieval_metrics.get("memory_layer_pressure"), dict)
        else recall_policy.get("memory_layer_pressure")
        if isinstance(recall_policy.get("memory_layer_pressure"), dict)
        else {}
    )
    if debug_lineage_enabled(include_debug=include_debug) and isinstance(memory_layer_pressure, dict) and memory_layer_pressure:
        compact["memory_layer_pressure"] = serving_memory_layer_pressure(memory_layer_pressure)
    async_pipeline_readiness = (
        retrieval_metrics.get("async_pipeline_readiness")
        if isinstance(retrieval_metrics.get("async_pipeline_readiness"), dict)
        else recall_policy.get("async_pipeline_readiness")
        if isinstance(recall_policy.get("async_pipeline_readiness"), dict)
        else {}
    )
    if debug_lineage_enabled(include_debug=include_debug) and isinstance(async_pipeline_readiness, dict) and async_pipeline_readiness:
        compact["async_pipeline_readiness"] = serving_async_pipeline_readiness(
            async_pipeline_readiness,
            include_debug=include_debug,
        )
    pre_retrieval_summary_refresh = (
        retrieval_metrics.get("pre_retrieval_summary_refresh")
        if isinstance(retrieval_metrics.get("pre_retrieval_summary_refresh"), dict)
        else recall_policy.get("pre_retrieval_summary_refresh")
        if isinstance(recall_policy.get("pre_retrieval_summary_refresh"), dict)
        else {}
    )
    if debug_lineage_enabled(include_debug=include_debug) and isinstance(pre_retrieval_summary_refresh, dict) and pre_retrieval_summary_refresh:
        compact["pre_retrieval_summary_refresh"] = {
            field: pre_retrieval_summary_refresh.get(field)
            for field in [
                "enabled",
                "status",
                "requested_limit",
                "refreshed_count",
                "compression_created_count",
                "skipped_dirty_count",
                "skipped_dirty_reasons",
                "elapsed_ms",
            ]
            if pre_retrieval_summary_refresh.get(field) not in (None, "", [], {})
        }
    if recall_policy:
        try:
            from tools.matrixark_mcp_core import memory_hierarchy_contract_from_recall_policy
        except ModuleNotFoundError:  # Direct script execution from tools/.
            from matrixark_mcp_core import memory_hierarchy_contract_from_recall_policy
        memory_hierarchy = memory_hierarchy_contract_from_recall_policy(recall_policy)
        if memory_hierarchy and include_debug:
            compact["memory_hierarchy"] = memory_hierarchy
    if include_debug:
        return compact
    return strip_default_debug_lineage_fields(compact)
