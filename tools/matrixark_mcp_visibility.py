#!/usr/bin/env python3
"""ContextPack telemetry and visibility policy helpers for MatrixArk MCP."""

from __future__ import annotations

from typing import Any

try:
    from tools.matrixark_mcp_core import (
        CONTEXT_TELEMETRY_WRITE_MODE,
        Json,
        MatrixArkError,
        compact_context_pack_audit_record,
        now_ms,
        stable_hash,
    )
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_core import (
        CONTEXT_TELEMETRY_WRITE_MODE,
        Json,
        MatrixArkError,
        compact_context_pack_audit_record,
        now_ms,
        stable_hash,
    )


def telemetry_record_for_context_pack(pack: Json, *, query: str, scope: Json, audit_mode: str) -> Json:
    recall_policy = pack.get("recall_policy", {}) if isinstance(pack.get("recall_policy"), dict) else {}
    stage_budgets = recall_policy.get("stage_latency_budgets", {}) if isinstance(recall_policy.get("stage_latency_budgets"), dict) else {}
    tree = recall_policy.get("tree_traversal", {}) if isinstance(recall_policy.get("tree_traversal"), dict) else {}
    secondary = recall_policy.get("secondary_index_filter", {}) if isinstance(recall_policy.get("secondary_index_filter"), dict) else {}
    rerank = recall_policy.get("rerank", {}) if isinstance(recall_policy.get("rerank"), dict) else {}
    time_weighted = recall_policy.get("time_weighted_recall", {}) if isinstance(recall_policy.get("time_weighted_recall"), dict) else {}
    dropped_refs = pack.get("dropped_refs", {}) if isinstance(pack.get("dropped_refs"), dict) else {}
    dropped_ref_count = int(dropped_refs.get("dropped_ref_count") or 0)
    if not dropped_ref_count and isinstance(dropped_refs.get("refs"), list):
        dropped_ref_count = len(dropped_refs.get("refs") or [])
    if not dropped_ref_count:
        dropped_ref_count = sum(
            value for key, value in dropped_refs.items() if isinstance(value, int) and key not in {"deadline_exceeded"}
        )
    return {
        "record_type": "context_pack_telemetry",
        "context_pack_id": pack.get("context_pack_id", ""),
        "query_hash": stable_hash(query),
        "scope": scope,
        "audit_mode": audit_mode,
        "question_type": pack.get("question_type", ""),
        "query_plan": recall_policy.get("query_plan", {}),
        "selected_ref_count": len(pack.get("selected_refs", []) or []),
        "selected_ref_counts": pack.get("selected_ref_counts", {}),
        "dropped_ref_count": dropped_ref_count,
        "dropped_ref_bucket_counts": {k: v for k, v in dropped_refs.items() if isinstance(v, int)},
        "used_local_context_tokens": pack.get("used_local_context_tokens", 0),
        "used_remote_context_tokens": pack.get("used_remote_context_tokens", 0),
        "total_prompt_context_tokens": pack.get("total_prompt_context_tokens", 0),
        "remote_context_budget_tokens": pack.get("remote_context_budget_tokens", 0),
        "requested_max_context_tokens": pack.get("requested_max_context_tokens", 0),
        "partial_context_pack": bool(pack.get("partial_context_pack", False)),
        "insufficient_context": bool(pack.get("insufficient_context", False)),
        "quality_warning_count": len(pack.get("quality_warnings", []) or []),
        "primary_candidate_count": pack.get("primary_candidate_count", 0),
        "auxiliary_candidate_count": pack.get("auxiliary_candidate_count", 0),
        "tree_fallback_to_flat": bool(tree.get("fallback_to_flat", False)),
        "tree_selected_node_count": tree.get("selected_node_count", 0),
        "secondary_index_matched_candidate_count": secondary.get("matched_candidate_count", 0),
        "secondary_index_dropped_candidate_count": secondary.get("dropped_candidate_count", 0),
        "rerank_mode": rerank.get("mode", ""),
        "rerank_candidate_count": rerank.get("reranked_candidate_count", 0),
        "time_weighted_recall": time_weighted,
        "stage_latency_budgets": stage_budgets,
        "created_at_ms": now_ms(),
    }


def context_pack_visibility_decision(
    *,
    pack: Json,
    query: str,
    audit_mode: str,
    audit_sample_rate: float,
    telemetry_write_mode: str,
) -> Json:
    force_rich_audit = bool(
        pack.get("partial_context_pack")
        or pack.get("insufficient_context")
        or pack.get("quality_warnings")
    )
    sample_basis = stable_hash(f"{pack.get('context_pack_id', '')}:{query}") % 1_000_000
    sample_value = sample_basis / 1_000_000.0
    rich_audit_sampled = bool(audit_mode == "full" and (force_rich_audit or sample_value < audit_sample_rate))
    telemetry_enabled = audit_mode != "off" and telemetry_write_mode != "off"
    return {
        "audit_mode": audit_mode,
        "audit_sample_rate": round(audit_sample_rate, 6),
        "audit_sample_value": round(sample_value, 6),
        "rich_replay_audit": rich_audit_sampled,
        "full_replay_audit_enabled": audit_mode == "full",
        "rich_replay_audit_force_reason": (
            "partial_or_warning" if force_rich_audit and audit_mode == "full" else "sampled" if rich_audit_sampled else "not_sampled"
        ),
        "telemetry_record": telemetry_enabled,
        "telemetry_write_mode": telemetry_write_mode,
        "serving_blocked_on_full_audit": False,
        "full_replay_audit_requires_full_mode": True,
    }


def append_context_pack_visibility(
    adapter: Any,
    *,
    pack: Json,
    audit_record: Json,
    query: str,
    scope: Json,
    audit_mode: str,
    audit_sample_rate: float = 1.0,
) -> Json:
    telemetry_write_mode = CONTEXT_TELEMETRY_WRITE_MODE
    if telemetry_write_mode not in {"inline", "async", "sync", "off"}:
        raise MatrixArkError("MATRIXARK_CONTEXT_TELEMETRY_WRITE_MODE must be inline, async, sync, or off")
    visibility_decision = context_pack_visibility_decision(
        pack=pack,
        query=query,
        audit_mode=audit_mode,
        audit_sample_rate=audit_sample_rate,
        telemetry_write_mode=telemetry_write_mode,
    )
    telemetry_enabled = bool(visibility_decision.get("telemetry_record"))
    rich_audit_sampled = bool(visibility_decision.get("rich_replay_audit"))
    telemetry = telemetry_record_for_context_pack(pack, query=query, scope=scope, audit_mode=audit_mode)
    telemetry["visibility_decision"] = visibility_decision
    if telemetry_enabled and telemetry_write_mode == "sync":
        adapter.append(telemetry)
    elif telemetry_enabled and telemetry_write_mode == "async":
        adapter.append_audit(telemetry)
    if rich_audit_sampled:
        audit_record["operational_visibility_policy"] = visibility_decision
        adapter.append_audit(compact_context_pack_audit_record(audit_record))
    return visibility_decision
