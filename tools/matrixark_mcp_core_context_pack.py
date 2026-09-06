# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Split out of matrixark_mcp_core.py; re-exported at core end via the dual
relative/absolute import pattern so the same core module object is reused under
both the package path (tools.matrixark_mcp_core) and the top-level path. No
import-time cycle. __all__ lists every moved name for total re-export."""
import os
from typing import Any, Callable

try:  # package path (tools.matrixark_mcp_core)
    from .matrixark_mcp_core import (
        Json,
        compact_dropped_refs_for_context_pack,
        compact_recall_policy_for_audit,
        memory_hierarchy_contract_from_recall_policy,
        memory_layer_for_serving_ref,
        normalize_message_role,
        ordered_normalized_role_list,
        serving_memory_layer_budget,
        serving_memory_layer_pressure,
    )
except ImportError:  # top-level path (matrixark_mcp_core)
    from matrixark_mcp_core import (
        Json,
        compact_dropped_refs_for_context_pack,
        compact_recall_policy_for_audit,
        memory_hierarchy_contract_from_recall_policy,
        memory_layer_for_serving_ref,
        normalize_message_role,
        ordered_normalized_role_list,
        serving_memory_layer_budget,
        serving_memory_layer_pressure,
    )

__all__ = ['selected_context_class_counts', '_positive_count_from_ref', '_attach_compact_profile_source_counts', '_context_memory_source_ref_is_debug_only', 'compact_context_pack_ref', 'compact_context_pack_refs', 'compact_context_pack_for_serving_flat', 'compact_context_pack_for_serving', 'normalized_role_int_map', 'normalized_string_int_map', 'budget_control_policy_summary']


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


def _positive_count_from_ref(ref: Json, *fields: str, list_fields: tuple[str, ...] = ()) -> int:
    for field in fields:
        value = ref.get(field)
        if isinstance(value, int) and value > 0:
            return value
        if isinstance(value, str):
            try:
                parsed = int(value)
            except ValueError:
                parsed = 0
            if parsed > 0:
                return parsed
    for field in list_fields:
        value = ref.get(field)
        if isinstance(value, list) and value:
            return len(value)
    return 0


def _attach_compact_profile_source_counts(item: Json, ref: Json) -> None:
    if ref.get("memory_scope") != "user_profile" or ref.get("session_continuity") != "cross_session":
        return
    session_count = _positive_count_from_ref(
        ref,
        "profile_source_session_count",
        "current_state_source_session_count",
        "source_session_count",
        list_fields=("source_session_ids",),
    )
    entity_count = _positive_count_from_ref(
        ref,
        "profile_source_entity_count",
        "current_state_source_entity_count",
        "source_entity_count",
        list_fields=("source_entity_hashes", "supersedes_session_entity_hashes"),
    )
    if session_count > 0:
        item["profile_source_session_count"] = session_count
    if entity_count > 0:
        item["profile_source_entity_count"] = entity_count



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
        "entity_type",
        "entity_name",
        "summary_type",
        "operator",
        "memory_scope",
        "session_continuity",
        "profile_memory_kind",
        "profile_memory_class",
        "profile_entity_current",
        "profile_summary_current",
        "profile_current_state_representative",
    ]:
        value = ref.get(field)
        if field == "source_ref" and _context_memory_source_ref_is_debug_only(ref) and not include_debug:
            continue
        if value not in (None, "", [], {}):
            if field in {"source_roles", "budget_source_roles"} and isinstance(value, list):
                roles = ordered_normalized_role_list(value)
                if roles:
                    item[field] = roles
            elif field in {"source_role_counts", "budget_source_role_counts"} and isinstance(value, dict):
                role_counts = normalized_role_int_map(value)
                if role_counts:
                    item[field] = role_counts
            else:
                item[field] = value
    memory_layer = memory_layer_for_serving_ref(ref)
    if memory_layer:
        item["memory_layer"] = memory_layer
    if include_debug:
        _attach_compact_profile_source_counts(item, ref)
        for field in ["extraction_phase", "final_session_boundary"]:
            value = ref.get(field)
            if value not in (None, "", [], {}):
                item[field] = value
        for field in [
            "current_state_policy",
            "source_roles",
            "budget_source_roles",
            "source_hook_types",
            "source_codex_events",
            "source_memory_selection_policies",
            "source_memory_layers",
            "source_memory_scopes",
            "source_session_continuities",
            "source_extraction_phases",
            "source_entity_types",
            "source_profile_promotion_policies",
            "source_profile_promotion_blockers",
            "source_final_session_boundary_count",
            "source_role_counts",
            "budget_source_role_counts",
            "source_hook_type_counts",
            "source_codex_event_counts",
            "source_memory_selection_policy_counts",
            "source_memory_layer_counts",
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
        ]:
            value = ref.get(field)
            if isinstance(value, list) and value:
                if field in {"source_roles", "budget_source_roles"}:
                    roles = ordered_normalized_role_list(value)
                    if roles:
                        item[field] = roles[:8]
                else:
                    item[field] = value[:8]
            elif isinstance(value, dict) and value:
                if field in {"source_role_counts", "budget_source_role_counts"}:
                    role_counts = normalized_role_int_map(value)
                    if role_counts:
                        item[field] = role_counts
                else:
                    item[field] = value
            elif isinstance(value, int) and value > 0:
                item[field] = value
            elif isinstance(value, float) and value > 0:
                item[field] = round(value, 6)
            elif isinstance(value, str) and value.strip():
                item[field] = value.strip()

    if include_debug:
        value = ref.get("source_session_ids")
        if isinstance(value, list) and value:
            item["source_session_ids"] = value[:8]
        value = ref.get("source_event_ids")
        if isinstance(value, list) and value:
            item["source_event_ids"] = value[:8]
        value = ref.get("source_event_count")
        if isinstance(value, int) and value > 0:
            item["source_event_count"] = value
        for field in ["source_record_type", "segment_origin"]:
            value = ref.get(field)
            if isinstance(value, str) and value.strip():
                item[field] = value.strip()
        if ref.get("derived_from_context_events") is True:
            item["derived_from_context_events"] = True

        value = ref.get("extraction_context_event_ids")
        if isinstance(value, list) and value:
            item["extraction_context_event_ids"] = value[:8]
        value = ref.get("source_session_count")
        if isinstance(value, int) and value > 0:
            item["source_session_count"] = value
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
        for field in ["current_state_source_session_count", "current_state_source_entity_count"]:
            value = ref.get(field)
            if isinstance(value, int) and value > 0:
                item[field] = value
    context_class = ref.get("context_class")
    if context_class and context_class != item.get("ref_type"):
        item["context_class"] = context_class
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


def compact_context_pack_for_serving_flat(pack: Json, *, include_debug: bool = False) -> Json:
    """Strip non-answer-bearing routing details from normal ContextPack output."""
    compact = dict(pack)
    serving_aliases = {
        "context_pack_id": "pack_id",
    }
    for source, target in serving_aliases.items():
        if compact.get(source) not in (None, "", [], {}):
            compact[target] = compact.get(source)
        compact.pop(source, None)
    compact["selected_refs"] = compact_context_pack_refs(list(compact.get("selected_refs", [])), include_debug=include_debug)
    remote_refs = compact_context_pack_refs(list(compact.get("remote_context_refs", compact.get("selected_refs", []))), include_debug=include_debug)
    if remote_refs and remote_refs != compact["selected_refs"]:
        compact["remote_context_refs"] = remote_refs
    else:
        compact.pop("remote_context_refs", None)
    compact["dropped_refs"] = compact_dropped_refs_for_context_pack(compact.get("dropped_refs", {}), include_debug=False)

    # Normal serving output should carry answer evidence, not operational counters.
    for field in [
        "context_sources_order",
        "selected_ref_counts",
        "primary_candidate_count",
        "auxiliary_candidate_count",
        "budget_source",
        "local_context_safety_margin_tokens",
        "remote_context_budget_tokens",
        "request_deadline_ms",
        "request_elapsed_ms",
        "context_pack_cache_hit",
        "cache_hit",
        "cache_hit_used",
        "context_pack_assembly",
        "assembly",
        "native_pack_assembly",
        "raw_records_returned",
        "python_hot_path_records",
        "scan_count",
        "selected_ref_count",
        "dropped_ref_count",
        "backend",
        "storage_mode",
        "question_type",
    ]:
        compact.pop(field, None)
    if compact.get("used_remote_context_tokens") == compact.get("used_context_tokens"):
        compact.pop("used_remote_context_tokens", None)
    if not compact.get("used_local_context_tokens"):
        compact.pop("used_local_context_tokens", None)
    if compact.get("total_prompt_context_tokens") == compact.get("used_context_tokens"):
        compact.pop("total_prompt_context_tokens", None)
    compact.pop("requested_max_context_tokens", None)
    if not compact.get("quality_warnings"):
        compact.pop("dropped_refs", None)
    if not compact.get("insufficient_context"):
        compact.pop("insufficient_context", None)
    if not compact.get("partial_context_pack"):
        compact.pop("partial_context_pack", None)
    compact.pop("packing_policy", None)

    recall_summary = compact_recall_policy_for_audit(
        compact.get("recall_policy", {}),
        include_debug=include_debug,
    )
    serving_recall: Json = {}
    query_plan = recall_summary.get("query_plan") if isinstance(recall_summary, dict) else {}
    if isinstance(query_plan, dict):
        temporal_window = query_plan.get("temporal_window")
        if isinstance(temporal_window, dict) and temporal_window.get("mode") not in (None, "", [], {}):
            serving_recall["temporal"] = temporal_window.get("mode")
    tree = recall_summary.get("tree") if isinstance(recall_summary, dict) else {}
    if isinstance(tree, dict) and tree.get("fallback_to_flat"):
        serving_recall["tree_fallback"] = True
    deadline = recall_summary.get("deadline") if isinstance(recall_summary, dict) else {}
    if isinstance(deadline, dict) and deadline.get("partial_context_pack"):
        serving_recall["partial_reason"] = deadline.get("fallback_reason") or "deadline"
    if serving_recall:
        compact["recall"] = serving_recall
    memory_layer_budget = recall_summary.get("memory_layer_budget") if isinstance(recall_summary, dict) else {}
    if include_debug and isinstance(memory_layer_budget, dict) and memory_layer_budget:
        compact["memory_layer_budget"] = serving_memory_layer_budget(memory_layer_budget, include_debug=include_debug)
    dropped_memory_layer_budget = recall_summary.get("dropped_memory_layer_budget") if isinstance(recall_summary, dict) else {}
    if include_debug and isinstance(dropped_memory_layer_budget, dict) and dropped_memory_layer_budget:
        compact["dropped_memory_layer_budget"] = serving_memory_layer_budget(dropped_memory_layer_budget, include_debug=include_debug)
    memory_layer_pressure = recall_summary.get("memory_layer_pressure") if isinstance(recall_summary, dict) else {}
    if include_debug and isinstance(memory_layer_pressure, dict) and memory_layer_pressure:
        compact["memory_layer_pressure"] = serving_memory_layer_pressure(memory_layer_pressure, include_debug=include_debug)
    memory_selection_policy_budget = recall_summary.get("memory_selection_policy_budget") if isinstance(recall_summary, dict) else {}
    if isinstance(memory_selection_policy_budget, dict) and memory_selection_policy_budget:
        compact["memory_selection_policy_budget"] = memory_selection_policy_budget
    async_pipeline_readiness = recall_summary.get("async_pipeline_readiness") if isinstance(recall_summary, dict) else {}
    if include_debug and isinstance(async_pipeline_readiness, dict) and async_pipeline_readiness:
        compact["async_pipeline_readiness"] = async_pipeline_readiness
    pre_retrieval_summary_refresh = compact.get("pre_retrieval_summary_refresh")
    if not isinstance(pre_retrieval_summary_refresh, dict):
        pre_retrieval_summary_refresh = (
            compact.get("recall_policy", {}).get("pre_retrieval_summary_refresh")
            if isinstance(compact.get("recall_policy"), dict)
            else {}
        )
    if include_debug and isinstance(pre_retrieval_summary_refresh, dict) and pre_retrieval_summary_refresh.get("enabled"):
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
    memory_hierarchy = memory_hierarchy_contract_from_recall_policy(compact.get("recall_policy", {}))
    if memory_hierarchy and include_debug:
        compact["memory_hierarchy"] = memory_hierarchy
    compact.pop("recall_policy", None)

    local_policy = compact.get("local_context_policy")
    if isinstance(local_policy, dict):
        compact_local_policy = {
            field: local_policy.get(field)
            for field in [
                "local_context_count",
                "local_context_tokens",
                "safety_margin_tokens",
            ]
            if local_policy.get(field) not in (None, "", [], {})
        }
        if compact_local_policy.get("local_context_count") or compact_local_policy.get("local_context_tokens"):
            compact["local_context_policy"] = compact_local_policy
        else:
            compact.pop("local_context_policy", None)

    if compact.get("embedding_fallback_used"):
        compact["embedding_status"] = {
            "fallback_used": True,
            "execution_mode": compact.get("embedding_execution_mode", ""),
            "model": compact.get("query_embedding_model", ""),
        }
    for field in [
        "context_assembly_policy",
        "context_pack_payload_policy",
        "operational_visibility_policy",
        "layer_scores",
        "query_embedding_model",
        "embedding_execution_mode",
        "embedding_fallback_used",
    ]:
        compact.pop(field, None)
    if include_debug or compact.get("include_retrieval_metrics"):
        metrics = compact.get("retrieval_metrics")
        if isinstance(metrics, dict):
            try:
                from tools.matrixark_mcp_context_pack import serving_retrieval_metrics
            except ModuleNotFoundError:  # Direct script execution from tools/.
                from matrixark_mcp_context_pack import serving_retrieval_metrics
            compact["retrieval_metrics"] = serving_retrieval_metrics(metrics, include_debug=include_debug)
    else:
        for field in [
            "retrieval_metrics",
            "memory_inventory",
            "pre_retrieval_idle_commit",
            "pre_retrieval_summary_refresh",
        ]:
            compact.pop(field, None)
    compact = {key: value for key, value in compact.items() if value not in (None, "", [], {})}
    if include_debug:
        return compact
    try:
        from tools.matrixark_mcp_context_pack import strip_default_debug_lineage_fields
    except ModuleNotFoundError:  # Direct script execution from tools/.
        from matrixark_mcp_context_pack import strip_default_debug_lineage_fields
    return strip_default_debug_lineage_fields(compact)


def compact_context_pack_for_serving(pack: Json, *, include_debug: bool = False) -> Json:
    """Return the compact server-facing ContextPack shape.

    MCP serving groups answer evidence by context class and session continuity.
    Adapter-direct callers that need the historical selected_refs list should
    call compact_context_pack_for_serving_flat.
    """
    try:
        from tools.matrixark_mcp_context_pack import compact_context_pack_for_serving as grouped_compactor
    except ModuleNotFoundError:  # Direct script execution from tools/.
        from matrixark_mcp_context_pack import compact_context_pack_for_serving as grouped_compactor
    return grouped_compactor(pack, include_debug=include_debug)


def normalized_role_int_map(raw: Any) -> Json:
    if not isinstance(raw, dict):
        return {}
    normalized: Json = {}
    for role, value in raw.items():
        role_name = normalize_message_role(role)
        if not role_name:
            continue
        try:
            amount = int(value or 0)
        except (TypeError, ValueError):
            continue
        normalized[role_name] = int(normalized.get(role_name, 0)) + amount
    return normalized


def normalized_string_int_map(raw: Any) -> Json:
    if not isinstance(raw, dict):
        return {}
    normalized: Json = {}
    for key, value in raw.items():
        key_name = str(key or "").strip().lower()
        if not key_name:
            continue
        try:
            amount = int(value or 0)
        except (TypeError, ValueError):
            continue
        normalized[key_name] = int(normalized.get(key_name, 0)) + amount
    return normalized


def budget_control_policy_summary(
    *,
    selected_budget: Any,
    budget_tokens: Any,
    mode: str,
    remote_budget_tokens: int,
    bucket_name: str,
    semantics: str,
    normalize_keys: Callable[[Any], str] | None = None,
    question_type: str = "",
) -> Json:
    normalize_keys = normalize_keys or (lambda value: str(value or "").strip())
    normalized_budget: Json = {}
    if isinstance(budget_tokens, dict):
        for key, raw_amount in budget_tokens.items():
            label = normalize_keys(key)
            if not label:
                continue
            try:
                amount = max(0, int(raw_amount or 0))
            except (TypeError, ValueError):
                amount = 0
            if amount > 0:
                normalized_budget[label] = int(normalized_budget.get(label, 0)) + amount
    bucket_values = selected_budget.get(bucket_name) if isinstance(selected_budget, dict) else {}
    if not isinstance(bucket_values, dict):
        bucket_values = {}
    selected_tokens: Json = {label: 0 for label in normalized_budget}
    selected_refs: Json = {label: 0 for label in normalized_budget}
    for key, bucket in bucket_values.items():
        label = normalize_keys(key)
        if not label or not isinstance(bucket, dict):
            continue
        try:
            tokens_value = max(0, int(bucket.get("tokens") or 0))
        except (TypeError, ValueError):
            tokens_value = 0
        try:
            refs_value = max(0, int(bucket.get("refs") or 0))
        except (TypeError, ValueError):
            refs_value = 0
        if label in selected_tokens or tokens_value > 0:
            selected_tokens[label] = int(selected_tokens.get(label, 0)) + tokens_value
        if label in selected_refs or refs_value > 0:
            selected_refs[label] = int(selected_refs.get(label, 0)) + refs_value
    active_mode = str(mode or ("explicit" if normalized_budget else "disabled"))
    if bucket_name == "by_memory_selection_policy":
        token_field = "selected_tokens_by_policy"
        ref_field = "selected_ref_count_by_policy"
    elif bucket_name == "by_memory_layer":
        token_field = "selected_tokens_by_layer"
        ref_field = "selected_ref_count_by_layer"
    else:
        token_field = "selected_tokens_by_role"
        ref_field = "selected_ref_count_by_role"
    policy: Json = {
        "enabled": bool(normalized_budget),
        "mode": active_mode,
        "remote_budget_tokens": max(0, int(remote_budget_tokens or 0)),
        "derived": active_mode in {"auto", "balanced", "codex_auto", "pre_retrieval_summary_refresh_balanced"},
        "budget_semantics": semantics,
        "independent_caps": True,
        "global_remote_budget_enforced": True,
        "budget_tokens": normalized_budget,
        token_field: selected_tokens,
        ref_field: selected_refs,
    }
    if question_type:
        policy["question_type"] = question_type
    return policy


