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
        "summary_type",
        "operator",
        "session_continuity",
    ]:
        value = ref.get(field)
        if value not in (None, "", [], {}):
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
