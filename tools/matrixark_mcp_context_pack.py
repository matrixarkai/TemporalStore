#!/usr/bin/env python3
"""ContextPack serving and audit compaction helpers for MatrixArk MCP."""

from __future__ import annotations

import os
from typing import Any

Json = dict[str, Any]

AUDIT_DEBUG_PAYLOAD = os.environ.get("MATRIXARK_AUDIT_DEBUG_PAYLOAD", "0").strip().lower() in {"1", "true", "yes"}


def _clip_context_text(text: str, *, max_chars: int = 160) -> str:
    if len(text) <= max_chars:
        return text
    return text[:max_chars].rstrip() + " ...[truncated]"


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


def compact_context_pack_ref(ref: Json) -> Json:
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
        "summary_type",
        "operator",
        "memory_scope",
        "session_continuity",
        "entity_type",
        "entity_name",
        "extraction_phase",
        "profile_current_state_representative",
        "current_state_policy",
        "source_memory_scopes",
        "source_session_continuities",
        "source_extraction_phases",
        "source_final_session_boundary_count",
    ]:
        value = ref.get(field)
        if value not in (None, "", [], {}):
            item[field] = value
    if bool(ref.get("final_session_boundary")):
        item["final_session_boundary"] = True
    for field in [
        "source_session_ids",
        "source_roles",
        "source_hook_types",
        "source_codex_events",
    ]:
        value = ref.get(field)
        if isinstance(value, list) and value:
            item[field] = value[:8]
    value = ref.get("source_entity_hashes")
    if isinstance(value, list) and value:
        item["source_entity_count"] = len(value)
    for field in ["current_state_source_session_count", "current_state_source_entity_count"]:
        value = ref.get(field)
        if isinstance(value, int) and value > 0:
            item[field] = value
    context_class = ref.get("context_class")
    if context_class and context_class != item.get("ref_type"):
        item["context_class"] = context_class
    if os.environ.get("MATRIXARK_CONTEXT_PACK_INCLUDE_SCORES", "0").strip().lower() in {"1", "true", "yes"}:
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
    if include_debug:
        return refs
    return [compact_context_pack_ref(ref) for ref in refs]


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
    ]
    compact: Json = {
        field: policy.get(field)
        for field in keep_fields
        if policy.get(field) not in (None, "", [], {})
    }
    for field in [
        "selected_ref_count",
        "selected_session_count",
        "resource_selected_ref_count",
        "skill_selected_ref_count",
        "entity_bridge_selected_ref_count",
        "selected_tokens",
        "resource_selected_tokens",
        "skill_selected_tokens",
    ]:
        value = policy.get(field)
        if isinstance(value, int) and value > 0:
            compact[field] = value
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
    for field in ["cross_session_policy", "shared_context_policy"]:
        value = compact_context_pack_policy(dropped.get(field))
        if value:
            compact[field] = value
    if dropped.get("refs"):
        compact["dropped_ref_detail_available_in_audit"] = True
        compact["dropped_ref_count"] = len(dropped.get("refs") or [])
    return compact


def compact_recall_policy_for_audit(recall_policy: Json) -> Json:
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
        compact["memory_layer_budget"] = memory_layer_budget
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
    memory_layer_budget = record.get("memory_layer_budget")
    if not isinstance(memory_layer_budget, dict):
        recall_policy = record.get("recall_policy") if isinstance(record.get("recall_policy"), dict) else {}
        memory_layer_budget = recall_policy.get("memory_layer_budget")
    if isinstance(memory_layer_budget, dict):
        compact["memory_layer_budget"] = memory_layer_budget
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
        "session_continuity",
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


def serving_ref_for_pack(ref: Json, *, default_session_continuity: str = "") -> Json:
    """Return only answer-bearing fields for the serving ContextPack payload."""
    metadata = ref.get("metadata", {}) if isinstance(ref.get("metadata"), dict) else {}
    item: Json = {
        "text": ref.get("text", ""),
        "tokens": ref.get("token_estimate", 0),
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
        ("extraction_phase", "extraction_phase"),
        ("resource_version", "version"),
        ("version_state", "version_state"),
        ("source_final_session_boundary_count", "source_final_session_boundary_count"),
    ]
    for field, alias in optional_field_aliases:
        value = ref.get(field, metadata.get(field))
        if value not in (None, "", [], {}):
            item[alias] = value
    session_continuity = str(ref.get("session_continuity") or metadata.get("session_continuity") or "")
    if session_continuity and session_continuity != default_session_continuity:
        item["session_continuity"] = session_continuity
    if bool(ref.get("final_session_boundary") or metadata.get("final_session_boundary")):
        item["final_session_boundary"] = True
    for field in [
        "source_session_ids",
        "source_roles",
        "source_hook_types",
        "source_codex_events",
        "source_memory_scopes",
        "source_session_continuities",
        "source_extraction_phases",
    ]:
        value = ref.get(field, metadata.get(field))
        if isinstance(value, list) and value:
            item[field] = value[:8]
    value = ref.get("source_entity_hashes", metadata.get("source_entity_hashes"))
    if isinstance(value, list) and value:
        item["source_entity_count"] = len(value)
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


def serving_refs_for_pack(refs: list[Json], *, default_session_continuity: str = "") -> list[Json]:
    return [serving_ref_for_pack(ref, default_session_continuity=default_session_continuity) for ref in refs]


def serving_ref_groups_for_pack(refs: list[Json], *, default_session_continuity: str = "") -> list[Json]:
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
        item = serving_ref_for_pack(ref, default_session_continuity=default_session_continuity)
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


def compact_context_pack_for_serving(pack: Json, *, include_debug: bool = False) -> Json:
    """Strip planner/audit/debug fields from the default returned ContextPack.

    Full retrieval policy, score details, dropped refs, storage mode, model
    fallback flags, and operational visibility live in ContextPackAudit or
    telemetry records when enabled. The serving pack should spend tokens on
    evidence and citations.
    """
    _ = include_debug
    compact: Json = {"context_pack_id": pack.get("context_pack_id") or pack.get("pack_id") or ""}
    selected_refs = pack.get("selected_refs", [])
    if isinstance(selected_refs, list) and (selected_refs or not isinstance(pack.get("groups"), list)):
        default_session_continuity = default_session_continuity_for_pack(selected_refs)
        compact["groups"] = serving_ref_groups_for_pack(selected_refs, default_session_continuity=default_session_continuity)
        if pack.get("selected_ref_counts"):
            compact.setdefault("counts", {})["refs"] = pack.get("selected_ref_counts", {})
        continuity_counts = session_continuity_counts(selected_refs)
        if continuity_counts:
            compact.setdefault("defaults", {})["session_continuity"] = default_session_continuity
            compact.setdefault("counts", {})["session_continuity"] = continuity_counts
    elif isinstance(pack.get("groups"), list):
        # Some adapters already return the serving shape. Preserve it so a
        # second compaction pass in the MCP entrypoint does not erase refs.
        compact["groups"] = pack.get("groups", [])
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
    if pack.get("partial_context_pack"):
        compact["partial"] = True
    if pack.get("insufficient_context"):
        compact["insufficient_context"] = True
    if pack.get("include_retrieval_metrics"):
        compact["include_retrieval_metrics"] = True
    if isinstance(pack.get("retrieval_metrics"), dict):
        compact["retrieval_metrics"] = pack["retrieval_metrics"]
    retrieval_metrics = pack.get("retrieval_metrics") if isinstance(pack.get("retrieval_metrics"), dict) else {}
    recall_policy = pack.get("recall_policy") if isinstance(pack.get("recall_policy"), dict) else {}
    memory_layer_budget = (
        retrieval_metrics.get("memory_layer_budget")
        if isinstance(retrieval_metrics.get("memory_layer_budget"), dict)
        else recall_policy.get("memory_layer_budget")
        if isinstance(recall_policy.get("memory_layer_budget"), dict)
        else {}
    )
    if isinstance(memory_layer_budget, dict) and memory_layer_budget:
        compact["memory_layer_budget"] = memory_layer_budget
    return compact
