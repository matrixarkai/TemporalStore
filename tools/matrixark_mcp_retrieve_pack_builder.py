#!/usr/bin/env python3
"""ContextPack payload builders for MatrixArk retrieval."""

from __future__ import annotations

import time

try:
    from tools.matrixark_mcp_core import (
        DEFAULT_BUSINESS_WEIGHT,
        DEFAULT_TIME_WEIGHT,
        Json,
        compact_context_pack_refs,
        compact_dropped_refs_for_context_pack,
        embedding_execution_mode_name,
        embedding_fallback_used,
        embedding_model_name,
        local_context_refs_for_pack,
        optional_object,
        selected_context_class_counts,
    )
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_core import (
        DEFAULT_BUSINESS_WEIGHT,
        DEFAULT_TIME_WEIGHT,
        Json,
        compact_context_pack_refs,
        compact_dropped_refs_for_context_pack,
        embedding_execution_mode_name,
        embedding_fallback_used,
        embedding_model_name,
        local_context_refs_for_pack,
        optional_object,
        selected_context_class_counts,
    )

def selected_ref_layer_budget(refs: list[Json]) -> Json:
    breakdown: Json = {
        "by_memory_scope": {},
        "by_session_continuity": {},
        "by_extraction_phase": {},
        "by_ref_type": {},
        "by_entity_type": {},
        "by_source_role": {},
        "by_hook_type": {},
        "by_codex_event": {},
        "source_message_counts_by_role": {},
        "source_hook_counts_by_type": {},
        "source_codex_event_counts_by_event": {},
        "final_session_boundary_ref_count": 0,
        "provisional_ref_count": 0,
        "final_ref_count": 0,
        "total_selected_refs": len(refs),
        "total_selected_tokens": 0,
    }
    for ref in refs:
        try:
            token_estimate = max(0, int(ref.get("token_estimate") or 0))
        except (TypeError, ValueError):
            token_estimate = 0
        breakdown["total_selected_tokens"] += token_estimate
        for field, bucket_name, default_value in [
            ("memory_scope", "by_memory_scope", "unscoped"),
            ("session_continuity", "by_session_continuity", "neutral"),
            ("extraction_phase", "by_extraction_phase", "unknown"),
            ("ref_type", "by_ref_type", "unknown"),
        ]:
            value = str(ref.get(field) or default_value)
            bucket = breakdown[bucket_name].setdefault(value, {"refs": 0, "tokens": 0})
            bucket["refs"] += 1
            bucket["tokens"] += token_estimate
        entity_type = str(ref.get("entity_type") or "")
        if entity_type:
            bucket = breakdown["by_entity_type"].setdefault(entity_type, {"refs": 0, "tokens": 0})
            bucket["refs"] += 1
            bucket["tokens"] += token_estimate
        source_roles = ref.get("budget_source_roles") if isinstance(ref.get("budget_source_roles"), list) else ref.get("source_roles") if isinstance(ref.get("source_roles"), list) else []
        for role in source_roles:
            role_name = str(role or "").strip()
            if role_name:
                bucket = breakdown["by_source_role"].setdefault(role_name, {"refs": 0, "tokens": 0})
                bucket["refs"] += 1
                bucket["tokens"] += token_estimate
        source_hook_types = ref.get("source_hook_types") if isinstance(ref.get("source_hook_types"), list) else []
        for hook_type in source_hook_types:
            hook_name = str(hook_type or "").strip()
            if hook_name:
                bucket = breakdown["by_hook_type"].setdefault(hook_name, {"refs": 0, "tokens": 0})
                bucket["refs"] += 1
                bucket["tokens"] += token_estimate
        source_codex_events = ref.get("source_codex_events") if isinstance(ref.get("source_codex_events"), list) else []
        for codex_event in source_codex_events:
            event_name = str(codex_event or "").strip()
            if event_name:
                bucket = breakdown["by_codex_event"].setdefault(event_name, {"refs": 0, "tokens": 0})
                bucket["refs"] += 1
                bucket["tokens"] += token_estimate
        for source_field, aggregate_field in [
            ("budget_source_role_counts", "source_message_counts_by_role"),
            ("source_hook_type_counts", "source_hook_counts_by_type"),
            ("source_codex_event_counts", "source_codex_event_counts_by_event"),
        ]:
            if source_field == "budget_source_role_counts" and not isinstance(ref.get(source_field), dict):
                source_field = "source_role_counts"
            source_counts = ref.get(source_field) if isinstance(ref.get(source_field), dict) else {}
            for name, count in source_counts.items():
                bucket_name = str(name or "").strip()
                if not bucket_name:
                    continue
                try:
                    source_count = max(0, int(count or 0))
                except (TypeError, ValueError):
                    source_count = 0
                if source_count:
                    breakdown[aggregate_field][bucket_name] = int(breakdown[aggregate_field].get(bucket_name, 0)) + source_count
        if bool(ref.get("final_session_boundary")):
            breakdown["final_session_boundary_ref_count"] += 1
        if str(ref.get("extraction_phase") or "") == "provisional":
            breakdown["provisional_ref_count"] += 1
        if str(ref.get("extraction_phase") or "") == "final":
            breakdown["final_ref_count"] += 1
    return breakdown


def dropped_ref_layer_budget(dropped: Json) -> Json:
    refs = dropped.get("refs") if isinstance(dropped, dict) else []
    if not isinstance(refs, list):
        refs = []
    breakdown: Json = {
        "by_drop_reason": {},
        "by_memory_scope": {},
        "by_session_continuity": {},
        "by_extraction_phase": {},
        "by_ref_type": {},
        "by_entity_type": {},
        "by_source_role": {},
        "by_hook_type": {},
        "by_codex_event": {},
        "source_message_counts_by_role": {},
        "source_hook_counts_by_type": {},
        "source_codex_event_counts_by_event": {},
        "by_profile_shadowed_reason": {},
        "final_session_boundary_ref_count": 0,
        "provisional_ref_count": 0,
        "final_ref_count": 0,
        "total_dropped_refs_with_detail": len(refs),
        "total_dropped_refs": len(refs),
        "total_dropped_tokens_with_detail": 0,
        "total_dropped_tokens": 0,
        "stale_ref_count": 0,
        "stale_token_estimate": 0,
        "profile_shadowed_ref_count": 0,
        "profile_shadowed_token_estimate": 0,
    }
    estimated_tokens = dropped.get("estimated_tokens") if isinstance(dropped, dict) else {}
    if isinstance(estimated_tokens, dict):
        compact_estimated = {
            str(key): int(value)
            for key, value in estimated_tokens.items()
            if isinstance(value, int) and value > 0
        }
        if compact_estimated:
            breakdown["estimated_tokens_by_reason"] = compact_estimated
    for ref in refs:
        if not isinstance(ref, dict):
            continue
        try:
            token_estimate = max(0, int(ref.get("token_estimate") or ref.get("token_cost") or 0))
        except (TypeError, ValueError):
            token_estimate = 0
        breakdown["total_dropped_tokens_with_detail"] += token_estimate
        breakdown["total_dropped_tokens"] += token_estimate
        reason = str(ref.get("drop_reason") or ref.get("reason") or "unknown")
        bucket = breakdown["by_drop_reason"].setdefault(reason, {"refs": 0, "tokens": 0})
        bucket["refs"] += 1
        bucket["tokens"] += token_estimate
        for field, bucket_name, default_value in [
            ("memory_scope", "by_memory_scope", "unscoped"),
            ("session_continuity", "by_session_continuity", "neutral"),
            ("extraction_phase", "by_extraction_phase", "unknown"),
            ("ref_type", "by_ref_type", "unknown"),
        ]:
            value = str(ref.get(field) or default_value)
            bucket = breakdown[bucket_name].setdefault(value, {"refs": 0, "tokens": 0})
            bucket["refs"] += 1
            bucket["tokens"] += token_estimate
        entity_type = str(ref.get("entity_type") or "")
        if entity_type:
            bucket = breakdown["by_entity_type"].setdefault(entity_type, {"refs": 0, "tokens": 0})
            bucket["refs"] += 1
            bucket["tokens"] += token_estimate
        source_roles = ref.get("budget_source_roles") if isinstance(ref.get("budget_source_roles"), list) else ref.get("source_roles") if isinstance(ref.get("source_roles"), list) else []
        for role in source_roles:
            role_name = str(role or "").strip()
            if role_name:
                bucket = breakdown["by_source_role"].setdefault(role_name, {"refs": 0, "tokens": 0})
                bucket["refs"] += 1
                bucket["tokens"] += token_estimate
        source_hook_types = ref.get("source_hook_types") if isinstance(ref.get("source_hook_types"), list) else []
        for hook_type in source_hook_types:
            hook_name = str(hook_type or "").strip()
            if hook_name:
                bucket = breakdown["by_hook_type"].setdefault(hook_name, {"refs": 0, "tokens": 0})
                bucket["refs"] += 1
                bucket["tokens"] += token_estimate
        source_codex_events = ref.get("source_codex_events") if isinstance(ref.get("source_codex_events"), list) else []
        for codex_event in source_codex_events:
            event_name = str(codex_event or "").strip()
            if event_name:
                bucket = breakdown["by_codex_event"].setdefault(event_name, {"refs": 0, "tokens": 0})
                bucket["refs"] += 1
                bucket["tokens"] += token_estimate
        for source_field, aggregate_field in [
            ("budget_source_role_counts", "source_message_counts_by_role"),
            ("source_hook_type_counts", "source_hook_counts_by_type"),
            ("source_codex_event_counts", "source_codex_event_counts_by_event"),
        ]:
            if source_field == "budget_source_role_counts" and not isinstance(ref.get(source_field), dict):
                source_field = "source_role_counts"
            source_counts = ref.get(source_field) if isinstance(ref.get(source_field), dict) else {}
            for name, count in source_counts.items():
                bucket_name = str(name or "").strip()
                if not bucket_name:
                    continue
                try:
                    source_count = max(0, int(count or 0))
                except (TypeError, ValueError):
                    source_count = 0
                if source_count:
                    breakdown[aggregate_field][bucket_name] = int(breakdown[aggregate_field].get(bucket_name, 0)) + source_count
        if bool(ref.get("final_session_boundary")):
            breakdown["final_session_boundary_ref_count"] += 1
        if str(ref.get("extraction_phase") or "") == "provisional":
            breakdown["provisional_ref_count"] += 1
        if str(ref.get("extraction_phase") or "") == "final":
            breakdown["final_ref_count"] += 1
        if bool(ref.get("stale_or_superseded")) or reason == "stale":
            breakdown["stale_ref_count"] += 1
            breakdown["stale_token_estimate"] += token_estimate
        if ref.get("profile_shadowed_by_ref_hash") not in (None, "", [], {}):
            breakdown["profile_shadowed_ref_count"] += 1
            breakdown["profile_shadowed_token_estimate"] += token_estimate
            shadow_reason = str(ref.get("profile_shadowed_reason") or "unknown")
            bucket = breakdown["by_profile_shadowed_reason"].setdefault(shadow_reason, {"refs": 0, "tokens": 0})
            bucket["refs"] += 1
            bucket["tokens"] += token_estimate
    return breakdown


def _budget_total(budget: Json, *names: str) -> int:
    for name in names:
        if not isinstance(budget, dict) or name not in budget:
            continue
        value = budget.get(name)
        try:
            return max(0, int(value or 0))
        except (TypeError, ValueError):
            continue
    return 0


def memory_layer_pressure_summary(selected_budget: Json, dropped_budget: Json) -> Json:
    selected_budget = selected_budget if isinstance(selected_budget, dict) else {}
    dropped_budget = dropped_budget if isinstance(dropped_budget, dict) else {}
    summary: Json = {
        "selected_refs": _budget_total(selected_budget, "total_selected_refs"),
        "selected_tokens": _budget_total(selected_budget, "total_selected_tokens"),
        "dropped_refs": _budget_total(dropped_budget, "total_dropped_refs", "total_dropped_refs_with_detail"),
        "dropped_tokens": _budget_total(dropped_budget, "total_dropped_tokens", "total_dropped_tokens_with_detail"),
        "pressure_dimensions": [],
        "dropped_dimensions": [],
        "by_dimension": {},
    }
    for dimension in [
        "by_drop_reason",
        "by_memory_scope",
        "by_session_continuity",
        "by_extraction_phase",
        "by_ref_type",
        "by_entity_type",
        "by_source_role",
        "by_hook_type",
        "by_codex_event",
        "by_profile_shadowed_reason",
    ]:
        selected_buckets = selected_budget.get(dimension) if isinstance(selected_budget.get(dimension), dict) else {}
        dropped_buckets = dropped_budget.get(dimension) if isinstance(dropped_budget.get(dimension), dict) else {}
        dimension_summary: Json = {}
        for bucket_name in sorted(set(selected_buckets) | set(dropped_buckets)):
            selected_bucket = selected_buckets.get(bucket_name, {}) if isinstance(selected_buckets.get(bucket_name), dict) else {}
            dropped_bucket = dropped_buckets.get(bucket_name, {}) if isinstance(dropped_buckets.get(bucket_name), dict) else {}
            selected_refs = _budget_total(selected_bucket, "refs")
            dropped_refs = _budget_total(dropped_bucket, "refs")
            if not selected_refs and not dropped_refs:
                continue
            selected_tokens = _budget_total(selected_bucket, "tokens")
            dropped_tokens = _budget_total(dropped_bucket, "tokens")
            dimension_summary[str(bucket_name)] = {
                "selected_refs": selected_refs,
                "selected_tokens": selected_tokens,
                "dropped_refs": dropped_refs,
                "dropped_tokens": dropped_tokens,
                "selected_and_dropped": bool(selected_refs and dropped_refs),
            }
        if dimension_summary:
            summary["by_dimension"][dimension] = dimension_summary
            if any(bucket["dropped_refs"] > 0 for bucket in dimension_summary.values()):
                summary["dropped_dimensions"].append(dimension)
            if any(bucket["selected_and_dropped"] for bucket in dimension_summary.values()):
                summary["pressure_dimensions"].append(dimension)
    for dimension in [
        "source_message_counts_by_role",
        "source_hook_counts_by_type",
        "source_codex_event_counts_by_event",
    ]:
        selected_counts = selected_budget.get(dimension) if isinstance(selected_budget.get(dimension), dict) else {}
        dropped_counts = dropped_budget.get(dimension) if isinstance(dropped_budget.get(dimension), dict) else {}
        count_summary: Json = {}
        for bucket_name in sorted(set(selected_counts) | set(dropped_counts)):
            selected_count = _budget_total(selected_counts, str(bucket_name))
            dropped_count = _budget_total(dropped_counts, str(bucket_name))
            if not selected_count and not dropped_count:
                continue
            count_summary[str(bucket_name)] = {
                "selected_count": selected_count,
                "dropped_count": dropped_count,
                "selected_and_dropped": bool(selected_count and dropped_count),
            }
        if count_summary:
            summary["by_dimension"][dimension] = count_summary
            if any(bucket["dropped_count"] > 0 for bucket in count_summary.values()):
                summary["dropped_dimensions"].append(dimension)
            if any(bucket["selected_and_dropped"] for bucket in count_summary.values()):
                summary["pressure_dimensions"].append(dimension)
    dimension_data = summary["by_dimension"]
    def dropped_in(dimension: str, bucket: str) -> int:
        return int(dimension_data.get(dimension, {}).get(bucket, {}).get("dropped_refs", 0))
    def dropped_count_in(dimension: str, bucket: str) -> int:
        return int(dimension_data.get(dimension, {}).get(bucket, {}).get("dropped_count", 0))
    summary["profile_memory_pressure"] = dropped_in("by_memory_scope", "user_profile") > 0
    summary["session_memory_pressure"] = dropped_in("by_memory_scope", "session") > 0
    summary["cross_session_pressure"] = dropped_in("by_session_continuity", "cross_session") > 0
    summary["same_session_pressure"] = dropped_in("by_session_continuity", "same_session") > 0
    summary["summary_memory_pressure"] = dropped_in("by_ref_type", "summary") > 0
    summary["entity_memory_pressure"] = dropped_in("by_ref_type", "entity") > 0
    summary["event_memory_pressure"] = dropped_in("by_ref_type", "event") > 0
    summary["final_memory_pressure"] = dropped_in("by_extraction_phase", "final") > 0
    summary["provisional_memory_pressure"] = dropped_in("by_extraction_phase", "provisional") > 0
    summary["stale_current_state_pressure"] = _budget_total(dropped_budget, "stale_ref_count") > 0
    summary["profile_shadowed_current_state_pressure"] = _budget_total(dropped_budget, "profile_shadowed_ref_count") > 0
    summary["assistant_memory_pressure"] = dropped_in("by_source_role", "assistant") > 0
    summary["user_memory_pressure"] = dropped_in("by_source_role", "user") > 0
    summary["tool_memory_pressure"] = dropped_in("by_source_role", "tool") > 0
    summary["assistant_source_message_pressure"] = dropped_count_in("source_message_counts_by_role", "assistant") > 0
    summary["user_source_message_pressure"] = dropped_count_in("source_message_counts_by_role", "user") > 0
    summary["tool_source_message_pressure"] = dropped_count_in("source_message_counts_by_role", "tool") > 0
    summary["pressure_bucket_count"] = sum(
        1
        for buckets in dimension_data.values()
        for bucket in buckets.values()
        if bucket.get("selected_and_dropped")
    )
    summary["dropped_bucket_count"] = sum(
        1
        for buckets in dimension_data.values()
        for bucket in buckets.values()
        if int(bucket.get("dropped_refs", 0)) > 0
    )
    return summary


def build_context_pack(
    *,
    context_pack_id: int,
    selected: list[Json],
    local_budget: Json,
    serving_selected: list[Json],
    dropped_over_budget: Json,
    serving_dropped: list[Json],
    layer_scores: list[Json],
    question_type: str,
    query_plan: Json,
    retrieval_session_scope: str,
    cross_session_policy: Json,
    shared_context_policy: Json,
    retrieval_scan_stats: Json,
    ranking: Json,
    min_similarity_score: float,
    max_global_candidates: int,
    max_selected_refs: int,
    budget_fill_policy: str,
    traversal: Json,
    top_k_per_layer: int,
    max_children_scored_per_parent: int,
    hard_max_children_scored_per_parent: int,
    max_candidates_per_node: int,
    max_raw_events_per_node: int,
    selected_node_hashes: set[int],
    selected_paths: set[tuple[str, ...]],
    tree_candidate_records_count: int,
    tree_prefilter_dropped_count: int,
    fanout_dropped_count: int,
    raw_event_time_window_dropped_count: int,
    secondary_index_filter_groups: list[set[str]],
    secondary_index_matched_count: int,
    secondary_index_dropped_count: int,
    secondary_index_filter_mode: str,
    rerank_policy: Json,
    time_weighted_recall: Json,
    reinforcement: Json,
    auxiliary_quota: float,
    storage_options: Json,
    deadline_ms: int,
    started_perf: float,
    partial_context_pack: bool,
    primary_candidate_count: int,
    auxiliary_candidate_count: int,
    used_context_tokens: int,
    local_tokens: int,
    remote_context_budget_tokens: int,
    max_context_tokens: int,
    safety_margin_tokens: int,
    budget_source: str,
    quality_warnings: list[str],
    audit_mode: str,
    audit_sample_rate: float,
    debug_refs: bool,
) -> Json:
    selected_context_counts = selected_context_class_counts(selected)
    memory_layer_budget = selected_ref_layer_budget(selected)
    dropped_memory_layer_budget = dropped_ref_layer_budget(dropped_over_budget)
    memory_layer_pressure = memory_layer_pressure_summary(memory_layer_budget, dropped_memory_layer_budget)
    return {
        "context_pack_id": str(context_pack_id),
        "context_sources_order": ["local_context", "matrixark_remote_context"],
        "local_context_refs": local_context_refs_for_pack(local_budget),
        "selected_refs": serving_selected,
        "remote_context_refs": serving_selected,
        "selected_ref_counts": selected_context_counts,
        "context_assembly_policy": {
            "access_scope_before_scoring": True,
            "skill_selection": "skill_section_only",
            "resource_selection": "resource_facts_entities_and_chunks_are_ranked_separately",
            "recall_reinforcement": "selected event refs and compression source ids receive protection markers before raw-event pruning",
        },
        "layer_scores": layer_scores[:24],
        "question_type": question_type,
        "packing_policy": f"question_type_aware:{question_type}",
        "query_embedding_model": embedding_model_name(),
        "embedding_execution_mode": embedding_execution_mode_name(),
        "embedding_fallback_used": embedding_fallback_used(),
        "recall_policy": {
            "query_plan": query_plan,
            "session_continuity": {
                "mode": retrieval_session_scope,
                "policy": "same-session continuity first; entity state bridges cross-session memory; cross-session evidence remains eligible under account/tenant/user scope",
                "same_session_selected_ref_count": sum(1 for item in selected if item.get("session_continuity") == "same_session"),
                "cross_session_selected_ref_count": sum(1 for item in selected if item.get("session_continuity") == "cross_session"),
                "entity_bridge_selected_ref_count": sum(1 for item in selected if item.get("session_continuity") == "cross_session" and item.get("ref_type") == "entity"),
            },
            "memory_layer_budget": memory_layer_budget,
            "dropped_memory_layer_budget": dropped_memory_layer_budget,
            "memory_layer_pressure": memory_layer_pressure,
            "cross_session": dropped_over_budget.get("cross_session_policy", cross_session_policy),
            "shared_context": dropped_over_budget.get("shared_context_policy", shared_context_policy),
            "backend_retrieval_pushdown": retrieval_scan_stats,
            "ranking": {
                "min_similarity_score": min_similarity_score,
                "max_global_candidates": max_global_candidates,
                "max_selected_refs": max_selected_refs,
                "budget_fill_policy": budget_fill_policy,
                "quality_first_budget_underfill_allowed": budget_fill_policy == "quality_first",
            },
            "tree_traversal": {
                "enabled": True,
                "summary_embeddings": ["node_l0", "node_l1"],
                "top_k_per_layer": top_k_per_layer,
                "max_children_scored_per_parent": max_children_scored_per_parent,
                "hard_max_children_scored_per_parent": hard_max_children_scored_per_parent,
                "children_scoring_policy": "score_all_children_up_to_hard_cap_then_split_node_layers",
                "max_candidates_per_node": max_candidates_per_node,
                "max_raw_events_per_node": max_raw_events_per_node,
                "max_selected_refs": max_selected_refs,
                "selected_node_count": len(selected_node_hashes),
                "selected_path_count": len(selected_paths),
                "selected_leaf_count": len(traversal.get("leaf_paths", [])),
                "candidate_records_after_tree": tree_candidate_records_count,
                "records_dropped_by_tree": tree_prefilter_dropped_count,
                "records_dropped_by_node_fanout": fanout_dropped_count,
                "raw_events_dropped_by_time_window": raw_event_time_window_dropped_count,
                "cold_events_represented_by_compression": raw_event_time_window_dropped_count > 0,
                "leaf_record_fetch_policy": "events/entities/resources/skills/compressions scanned only inside selected L0/L1 folders",
                "fallback_to_flat": bool(traversal.get("fallback_to_flat")),
                "fallback_reason": "missing_or_stale_summary_embeddings" if traversal.get("fallback_to_flat") else "",
            },
            "secondary_index_filter": {
                "enabled": bool(secondary_index_filter_groups),
                "required_groups": [sorted(group) for group in secondary_index_filter_groups],
                "matched_candidate_count": secondary_index_matched_count,
                "dropped_candidate_count": secondary_index_dropped_count,
                "mode": "ANY group for multi-intent raw query, otherwise AND across groups; OR within each group",
                "effective_mode": secondary_index_filter_mode,
                "applied_before_embedding_scoring": True,
                "fanout_cap_applied_before_embedding_scoring": True,
            },
            "rerank": rerank_policy,
            "primary_path": "tree-first hybrid dense semantic + sparse lexical after secondary-index prefilter",
            "auxiliary_path": "keyword graph inside selected tree after secondary-index prefilter",
            "time_decay": {
                "freshness_tolerance_ms": time_weighted_recall["freshness_tolerance_ms"],
                "half_life_ms": time_weighted_recall["half_life_ms"],
            },
            "time_weighted_recall": time_weighted_recall,
            "recall_reinforcement": reinforcement,
            "weights": {
                "time": optional_object(ranking, "weights").get("time", DEFAULT_TIME_WEIGHT),
                "business": optional_object(ranking, "weights").get("business", DEFAULT_BUSINESS_WEIGHT),
            },
            "auxiliary_quota": auxiliary_quota,
            "storage_options": storage_options,
            "hard_deadline": {
                "deadline_ms": deadline_ms,
                "elapsed_ms": round((time.perf_counter() - started_perf) * 1000.0, 3),
                "partial_context_pack": partial_context_pack,
                "fallback_reason": dropped_over_budget.get("deadline_reason", "") if partial_context_pack else "",
            },
        },
        "primary_candidate_count": primary_candidate_count,
        "auxiliary_candidate_count": auxiliary_candidate_count,
        "used_context_tokens": used_context_tokens,
        "used_remote_context_tokens": used_context_tokens,
        "used_local_context_tokens": local_tokens,
        "total_prompt_context_tokens": used_context_tokens + local_tokens,
        "remote_context_budget_tokens": remote_context_budget_tokens,
        "requested_max_context_tokens": max_context_tokens,
        "local_context_safety_margin_tokens": safety_margin_tokens,
        "budget_source": budget_source,
        "local_context_policy": {
            "mode": "shared_budget_dedupe",
            "local_context_count": len(local_budget["items"]),
            "local_context_tokens": local_tokens,
            "local_context_token_source": local_budget.get("token_source", "estimated_from_local_context"),
            "safety_margin_tokens": safety_margin_tokens,
            "safety_margin_source": local_budget.get("safety_margin_source", "matrixark_default_5_percent_capped"),
            "dedupe_remote_against_local": True,
            "remote_is_additive_only_within_remaining_budget": True,
        },
        "dropped_refs": serving_dropped,
        "quality_warnings": quality_warnings,
        "insufficient_context": not selected,
        "partial_context_pack": partial_context_pack,
        "context_pack_payload_policy": {
            "serving_refs": "compact" if not debug_refs else "debug_full",
            "hashes_and_matched_indexes": "audit_only" if not debug_refs else "included",
            "dropped_ref_details": "audit_only" if not debug_refs else "included",
            "enable_debug_refs_with": "include_debug_refs=true or MATRIXARK_CONTEXT_PACK_DEBUG_REFS=1",
        },
        "operational_visibility_policy": {
            "audit_mode": audit_mode,
            "audit_sample_rate": audit_sample_rate,
            "telemetry_record": audit_mode != "off",
            "rich_replay_audit": audit_mode == "full" and audit_sample_rate > 0,
            "rich_replay_audit_force_on_partial_or_warning": True,
        },
    }


def prepare_serving_refs(
    *,
    selected: list[Json],
    dropped_over_budget: Json,
    debug_refs: bool,
) -> tuple[list[Json], list[Json]]:
    return (
        compact_context_pack_refs(selected, include_debug=debug_refs),
        compact_dropped_refs_for_context_pack(dropped_over_budget, include_debug=debug_refs),
    )
