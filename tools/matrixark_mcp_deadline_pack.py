#!/usr/bin/env python3
"""Deadline fallback ContextPack assembly for MatrixArk MCP adapters."""

from __future__ import annotations

from typing import Any

try:
    from tools.matrixark_mcp_core import (
        Json,
        candidate_access_scope,
        clip_context_text,
        compact_context_pack_audit_record,
        compact_context_pack_for_serving,
        compact_context_pack_refs,
        compact_local_context_refs,
        compact_refs_for_audit,
        embedding_execution_mode_name,
        embedding_fallback_used,
        embedding_model_name,
        local_context_refs_for_pack,
        normalize_message_role,
        now_ms,
        scope_matches,
        stable_hash,
        summarize_text,
        token_count,
        selected_context_class_counts,
    )
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_core import (
        Json,
        candidate_access_scope,
        clip_context_text,
        compact_context_pack_audit_record,
        compact_context_pack_for_serving,
        compact_context_pack_refs,
        compact_local_context_refs,
        compact_refs_for_audit,
        embedding_execution_mode_name,
        embedding_fallback_used,
        embedding_model_name,
        local_context_refs_for_pack,
        normalize_message_role,
        now_ms,
        scope_matches,
        stable_hash,
        summarize_text,
        token_count,
        selected_context_class_counts,
    )

try:
    from tools.matrixark_mcp_retrieve_pack_builder import (
        memory_layer_pressure_summary,
        selected_ref_layer_budget,
    )
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_retrieve_pack_builder import (
        memory_layer_pressure_summary,
        selected_ref_layer_budget,
    )

try:
    from tools.matrixark_mcp_async_readiness import async_pipeline_retrieval_readiness
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_async_readiness import async_pipeline_retrieval_readiness


def deadline_fallback_pack(
    adapter: Any,
    *,
    query: str,
    scope: Json,
    question_type: str,
    max_context_tokens: int,
    local_budget: Json,
    deadline_ms: int,
    elapsed_ms: float,
    records: list[Json],
    reason: str,
    budget_source: str = "matrixark_default_max_context_tokens",
    retrieval_scope: Json | None = None,
) -> Json:
    selected = []
    used_context_tokens = 0
    local_tokens = int(local_budget.get("token_estimate", 0))
    safety_margin_tokens = int(local_budget.get("safety_margin_tokens", 0))
    remote_budget = max(0, max_context_tokens - local_tokens - safety_margin_tokens)
    for record in reversed(records):
        record_type = record.get("record_type")
        metadata = record.get("metadata", {}) if isinstance(record.get("metadata"), dict) else {}
        record_scope = candidate_access_scope(record)
        if record_type not in {"context_summary", "context_entity", "context_event", "context_segment"}:
            continue
        if not scope_matches(record_scope, scope):
            continue
        if record_type == "context_summary":
            text = str(record.get("summary_text", ""))
            ref_type = "summary"
            ref_hash = record.get("summary_hash") or record.get("node_hash")
        elif record_type == "context_entity":
            text = f"{record.get('entity_type', '')}: {record.get('entity_name', '')} = {record.get('state', '')}"
            ref_type = "entity"
            ref_hash = record.get("entity_hash")
        elif record_type == "context_segment":
            text = f"{record.get('topic', '')}: {record.get('summary_text', '')}"
            ref_type = "segment"
            ref_hash = record.get("segment_hash")
        else:
            text = str(record.get("summary_text") or record.get("text") or "")
            ref_type = "event"
            ref_hash = record.get("event_id_hash")
        if not text or ref_hash is None:
            continue
        item_tokens = token_count(text)
        if used_context_tokens + item_tokens > remote_budget:
            continue
        ref = {
            "ref_type": ref_type,
            "ref_hash": ref_hash,
            "node_hash": record.get("node_hash"),
            "node_path": record.get("node_path", []),
            "score": 0.0,
            "recall_path": "deadline_fallback_recent_context",
            "updated_at_ms": record.get("updated_at_ms", record.get("envelope", {}).get("ingestion_time_ms", now_ms())),
            "text": clip_context_text(text),
            "token_estimate": item_tokens,
        }
        for field in [
            "memory_scope",
            "session_continuity",
            "extraction_phase",
            "entity_type",
            "entity_name",
            "summary_type",
            "profile_current_state_representative",
            "current_state_policy",
            "current_state_source_session_count",
            "current_state_source_entity_count",
            "source_final_session_boundary_count",
        ]:
            value = record.get(field, metadata.get(field))
            if value not in (None, "", [], {}):
                ref[field] = value
        if bool(record.get("final_session_boundary") or metadata.get("final_session_boundary")):
            ref["final_session_boundary"] = True
        for field in [
            "source_roles",
            "source_hook_types",
            "source_codex_events",
            "source_memory_scopes",
            "source_session_continuities",
            "source_extraction_phases",
            "source_session_ids",
            "source_entity_hashes",
        ]:
            value = record.get(field, metadata.get(field))
            if isinstance(value, list) and value:
                ref[field] = value[:16]
        for field in [
            "source_role_counts",
            "source_hook_type_counts",
            "source_codex_event_counts",
        ]:
            value = record.get(field, metadata.get(field))
            if isinstance(value, dict) and value:
                compact_counts: Json = {}
                for key, count in value.items():
                    count_key = normalize_message_role(key) if field == "source_role_counts" else str(key or "").strip()
                    if not count_key:
                        continue
                    try:
                        count_value = int(count or 0)
                    except (TypeError, ValueError):
                        continue
                    if count_value > 0:
                        compact_counts[count_key] = int(compact_counts.get(count_key, 0)) + count_value
                if compact_counts:
                    ref[field] = compact_counts
        selected.append(ref)
        used_context_tokens += item_tokens
        if len(selected) >= 8:
            break
    context_pack_id = str(stable_hash(f"deadline:{query}:{selected}:{now_ms()}"))
    serving_selected = compact_context_pack_refs(selected, include_debug=False)
    memory_layer_budget = selected_ref_layer_budget(selected)
    memory_layer_pressure = memory_layer_pressure_summary(memory_layer_budget, {})
    async_readiness_scope = retrieval_scope if isinstance(retrieval_scope, dict) else {**scope, "_session_scope": "prefer"}
    async_pipeline_readiness = async_pipeline_retrieval_readiness(records, async_readiness_scope)
    cross_session_selected = [item for item in selected if item.get("session_continuity") == "cross_session"]
    cross_session_source_sessions = {
        str(source_session)
        for item in cross_session_selected
        for source_session in item.get("source_session_ids", [])
        if source_session
    }
    quality_warnings = [
        f"retrieval_deadline_exceeded:{reason}",
        *async_pipeline_readiness.get("freshness_warnings", []),
    ]
    pack = {
        "context_pack_id": context_pack_id,
        "context_sources_order": ["local_context", "matrixark_remote_context"],
        "local_context_refs": local_context_refs_for_pack(local_budget),
        "selected_refs": serving_selected,
        "remote_context_refs": serving_selected,
        "selected_ref_counts": selected_context_class_counts(selected),
        "layer_scores": [],
        "question_type": question_type,
        "packing_policy": f"deadline_fallback:{question_type}",
        "query_embedding_model": embedding_model_name(),
        "embedding_execution_mode": embedding_execution_mode_name(),
        "embedding_fallback_used": embedding_fallback_used(),
        "recall_policy": {
            "deadline_ms": deadline_ms,
            "elapsed_ms": elapsed_ms,
            "partial_context_pack": True,
            "fallback_reason": reason,
            "memory_layer_budget": memory_layer_budget,
            "memory_layer_pressure": memory_layer_pressure,
            "async_pipeline_readiness": async_pipeline_readiness,
            "session_continuity": {
                "mode": "fallback_recent_context",
                "policy": "deadline fallback preserves same-session/cross-session/profile lineage while staying within the remaining remote budget",
                "same_session_selected_ref_count": sum(1 for item in selected if item.get("session_continuity") == "same_session"),
                "cross_session_selected_ref_count": sum(1 for item in selected if item.get("session_continuity") == "cross_session"),
                "entity_bridge_selected_ref_count": sum(1 for item in selected if item.get("session_continuity") == "cross_session" and item.get("ref_type") == "entity"),
            },
            "cross_session": {
                "enabled": bool(cross_session_selected),
                "budget_tokens": remote_budget,
                "remote_budget_tokens": remote_budget,
                "computed_budget_tokens": remote_budget,
                "budget_floor_tokens": 0,
                "budget_floor_applied": False,
                "budget_floor_status": "deadline_fallback_uses_remaining_remote_budget",
                "max_sessions": len(cross_session_source_sessions),
                "max_candidates": len(cross_session_selected),
            },
            "shared_context": {"enabled": False},
        },
        "primary_candidate_count": 0,
        "auxiliary_candidate_count": 0,
        "used_context_tokens": used_context_tokens,
        "used_remote_context_tokens": used_context_tokens,
        "used_local_context_tokens": local_tokens,
        "total_prompt_context_tokens": used_context_tokens + local_tokens,
        "remote_context_budget_tokens": remote_budget,
        "requested_max_context_tokens": max_context_tokens,
        "retrieval_metrics": {
            "memory_layer_budget": memory_layer_budget,
            "memory_layer_pressure": memory_layer_pressure,
            "async_pipeline_readiness": async_pipeline_readiness,
            "requested_max_context_tokens": max_context_tokens,
            "used_local_context_tokens": local_tokens,
            "used_remote_context_tokens": used_context_tokens,
            "total_prompt_context_tokens": used_context_tokens + local_tokens,
            "remote_context_budget_tokens": remote_budget,
            "partial_context_pack": True,
            "fallback_reason": reason,
            "source": "deadline_fallback_pack",
        },
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
        "dropped_refs": {},
        "quality_warnings": quality_warnings,
        "insufficient_context": not selected,
        "partial_context_pack": True,
    }
    if reason != "service_backpressure":
        adapter.append_audit(
            compact_context_pack_audit_record({
                "record_type": "context_pack_audit",
                "context_pack_id": context_pack_id,
                "query": query,
                "scope": scope,
                "summary_text": summarize_text(" ".join(str(item.get("text", "")) for item in selected), limit=512),
                "selected_refs": compact_refs_for_audit(selected),
                "selected_ref_counts": selected_context_class_counts(selected),
                "local_context_refs": compact_local_context_refs(local_budget),
                "context_sources_order": pack["context_sources_order"],
                "question_type": question_type,
                "packing_policy": pack["packing_policy"],
                "recall_policy": pack["recall_policy"],
                "quality_warnings": pack["quality_warnings"],
                "partial_context_pack": True,
                "memory_layer_budget": memory_layer_budget,
                "async_pipeline_readiness": async_pipeline_readiness,
                "local_context_policy": pack["local_context_policy"],
                "used_local_context_tokens": pack["used_local_context_tokens"],
                "used_remote_context_tokens": pack["used_remote_context_tokens"],
                "total_prompt_context_tokens": pack["total_prompt_context_tokens"],
                "remote_context_budget_tokens": pack["remote_context_budget_tokens"],
                "requested_max_context_tokens": pack["requested_max_context_tokens"],
                "local_context_safety_margin_tokens": pack["local_context_safety_margin_tokens"],
                "budget_source": pack["budget_source"],
                "primary_candidate_count": 0,
                "auxiliary_candidate_count": 0,
                "created_at_ms": now_ms(),
            })
        )
    else:
        pack["operational_visibility_policy"] = {
            "audit_mode": "telemetry_only",
            "rich_replay_audit": False,
            "reason": "service_backpressure_uses_access_audit_only",
        }
    return compact_context_pack_for_serving(pack)
