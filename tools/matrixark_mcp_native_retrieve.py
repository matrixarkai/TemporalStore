#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Native MatrixArk retrieve-pack orchestration helpers."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

try:
    from tools.matrixark_mcp_core import (
        Json,
        compact_context_pack_audit_record,
        compact_context_pack_for_serving,
        compact_context_pack_refs,
        compact_dropped_refs_for_context_pack,
        compact_local_context_refs,
        compact_refs_for_audit,
        now_ms,
        selected_context_class_counts,
        stable_hash,
        summarize_text,
    )
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_core import (
        Json,
        compact_context_pack_audit_record,
        compact_context_pack_for_serving,
        compact_context_pack_refs,
        compact_dropped_refs_for_context_pack,
        compact_local_context_refs,
        compact_refs_for_audit,
        now_ms,
        selected_context_class_counts,
        stable_hash,
        summarize_text,
    )


def try_native_context_pack(
    target: Any,
    *,
    args: Json,
    query: str,
    scope: Json,
    retrieval_scope: Json,
    question_type: str,
    query_plan: Json,
    secondary_index_filter_groups: list[set[str]],
    secondary_index_filter_mode: str,
    max_context_tokens: int,
    local_budget: Json,
    cross_session_policy: Json,
    shared_context_policy: Json,
    ranking: Json,
    deadline_ms: int,
    reference_time_ms: int,
    audit_mode: str,
    audit_sample_rate: float,
    storage_options: Json,
    debug_refs: bool,
    stage_budget_snapshot: Callable[[], Json],
) -> Json | None:
    native_pack = target.native_context_pack({
        "query": query,
        "scope": retrieval_scope,
        "question_type": question_type,
        "query_plan": query_plan,
        "secondary_index_groups": [sorted(group) for group in secondary_index_filter_groups],
        "secondary_index_filter_mode": secondary_index_filter_mode,
        "max_context_tokens": max_context_tokens,
        "local_budget": {
            "token_estimate": int(local_budget.get("token_estimate", 0)),
            "safety_margin_tokens": int(local_budget.get("safety_margin_tokens", 0)),
            "remote_budget_tokens": int(local_budget.get("remote_budget_tokens", max_context_tokens)),
        },
        "cross_session": cross_session_policy,
        "shared_context": shared_context_policy,
        "ranking": ranking,
        "deadline_ms": deadline_ms,
        "reference_time_ms": reference_time_ms,
        "include_superseded_resources": bool(args.get("include_superseded_resources", False) or args.get("historical_replay", False)),
        "audit_mode": audit_mode,
    })
    if native_pack is None:
        return None

    recall_policy = native_pack.get("recall_policy") if isinstance(native_pack.get("recall_policy"), dict) else {}
    recall_policy.setdefault("native_context_pack", {
        "enabled": True,
        "python_role": "mcp_auth_model_request_shaping_only",
        "backend_role": "scan_filter_score_pack",
    })
    recall_policy.setdefault("stage_latency_budgets", stage_budget_snapshot())
    native_pack["recall_policy"] = recall_policy
    native_pack.setdefault("context_pack_cache_hit", False)
    native_pack.setdefault("context_pack_assembly", "native_backend")
    native_pack.setdefault("remote_context_refs", native_pack.get("selected_refs", []))
    native_pack.setdefault("selected_ref_counts", selected_context_class_counts(native_pack.get("selected_refs", [])))
    selected_refs = native_pack.get("selected_refs", []) if isinstance(native_pack.get("selected_refs"), list) else []
    context_pack_id_text = str(native_pack.get("context_pack_id") or stable_hash(f"native:{query}:{selected_refs}:{now_ms()}"))
    native_pack["context_pack_id"] = context_pack_id_text
    if audit_mode == "full" and audit_sample_rate > 0 and (
        audit_sample_rate >= 1.0 or stable_hash(context_pack_id_text) % 10000 < int(audit_sample_rate * 10000)
    ):
        target.append_audit(
            compact_context_pack_audit_record({
                "record_type": "context_pack_audit",
                "context_pack_id": context_pack_id_text,
                "query": query,
                "scope": scope,
                "summary_text": summarize_text(" ".join(str(item.get("text", "")) for item in selected_refs), limit=512),
                "selected_refs": compact_refs_for_audit(selected_refs),
                "local_context_refs": compact_local_context_refs(local_budget),
                "context_sources_order": native_pack.get("context_sources_order", []),
                "selected_ref_counts": native_pack.get("selected_ref_counts", {}),
                "dropped_refs": native_pack.get("dropped_refs", {}),
                "quality_warnings": native_pack.get("quality_warnings", []),
                "question_type": question_type,
                "packing_policy": native_pack.get("packing_policy", "native_backend"),
                "recall_policy": recall_policy,
                "stage_latency_budgets": recall_policy.get("stage_latency_budgets", {}),
                "storage_options": storage_options,
                "used_remote_context_tokens": native_pack.get("used_remote_context_tokens", native_pack.get("used_context_tokens", 0)),
                "remote_context_budget_tokens": native_pack.get("remote_context_budget_tokens", max_context_tokens),
                "requested_max_context_tokens": native_pack.get("requested_max_context_tokens", max_context_tokens),
                "created_at_ms": now_ms(),
            })
        )
    serving_selected_refs = compact_context_pack_refs(selected_refs, include_debug=debug_refs)
    native_pack["selected_refs"] = serving_selected_refs
    native_pack["remote_context_refs"] = serving_selected_refs
    native_pack["dropped_refs"] = compact_dropped_refs_for_context_pack(native_pack.get("dropped_refs", {}), include_debug=debug_refs)
    native_pack["context_pack_payload_policy"] = {
        "serving_refs": "compact" if not debug_refs else "debug_full",
        "hashes_and_matched_indexes": "audit_only" if not debug_refs else "included",
        "dropped_ref_details": "audit_only" if not debug_refs else "included",
        "enable_debug_refs_with": "include_debug_refs=true or MATRIXARK_CONTEXT_PACK_DEBUG_REFS=1",
    }
    return compact_context_pack_for_serving(native_pack, include_debug=debug_refs)
