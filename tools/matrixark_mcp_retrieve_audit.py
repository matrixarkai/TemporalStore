#!/usr/bin/env python3
"""Audit payload builders for MatrixArk retrieval."""

from __future__ import annotations

try:
    from tools.matrixark_mcp_core import (
        Json,
        compact_local_context_refs,
        compact_refs_for_audit,
        now_ms,
    )
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_core import (
        Json,
        compact_local_context_refs,
        compact_refs_for_audit,
        now_ms,
    )


def build_context_pack_audit_record(
    *,
    context_pack_id_text: str,
    query: str,
    scope: Json,
    pack_summary: str,
    selected: list[Json],
    local_budget: Json,
    pack: Json,
    dropped_over_budget: Json,
    quality_warnings: list[str],
    partial_context_pack: bool,
    layer_scores: list[Json],
    question_type: str,
    rerank_policy: Json,
    storage_options: Json,
    primary_candidate_count: int,
    auxiliary_candidate_count: int,
    tree_candidate_records_count: int,
    tree_prefilter_dropped_count: int,
    fanout_dropped_count: int,
    max_candidates_per_node: int,
    max_selected_refs: int,
) -> Json:
    return {
        "record_type": "context_pack_audit",
        "context_pack_id": context_pack_id_text,
        "query": query,
        "scope": scope,
        "summary_text": pack_summary,
        "selected_refs": compact_refs_for_audit(selected),
        "local_context_refs": compact_local_context_refs(local_budget),
        "context_sources_order": pack["context_sources_order"],
        "selected_ref_counts": pack["selected_ref_counts"],
        "context_assembly_policy": pack["context_assembly_policy"],
        "dropped_refs": dropped_over_budget,
        "quality_warnings": quality_warnings,
        "partial_context_pack": partial_context_pack,
        "layer_scores": layer_scores[:24],
        "tree_traversal": pack["recall_policy"]["tree_traversal"],
        "secondary_index_filter": pack["recall_policy"]["secondary_index_filter"],
        "question_type": question_type,
        "packing_policy": pack["packing_policy"],
        "rerank_policy": rerank_policy,
        "recall_policy": pack["recall_policy"],
        "stage_latency_budgets": pack["recall_policy"]["stage_latency_budgets"],
        "storage_options": storage_options,
        "local_context_policy": pack["local_context_policy"],
        "used_local_context_tokens": pack["used_local_context_tokens"],
        "used_remote_context_tokens": pack["used_remote_context_tokens"],
        "total_prompt_context_tokens": pack["total_prompt_context_tokens"],
        "remote_context_budget_tokens": pack["remote_context_budget_tokens"],
        "requested_max_context_tokens": pack["requested_max_context_tokens"],
        "local_context_safety_margin_tokens": pack["local_context_safety_margin_tokens"],
        "budget_source": pack["budget_source"],
        "operational_visibility_policy": pack["operational_visibility_policy"],
        "primary_candidate_count": primary_candidate_count,
        "auxiliary_candidate_count": auxiliary_candidate_count,
        "tree_candidate_records": tree_candidate_records_count,
        "tree_prefilter_dropped_count": tree_prefilter_dropped_count,
        "fanout_dropped_count": fanout_dropped_count,
        "max_candidates_per_node": max_candidates_per_node,
        "max_selected_refs": max_selected_refs,
        "created_at_ms": now_ms(),
    }
