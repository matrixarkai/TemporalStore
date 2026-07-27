#!/usr/bin/env python3
"""Retrieval metrics attachment helpers for MatrixArk local runtime."""

from __future__ import annotations

from typing import Any

Json = dict[str, Any]

try:
    from tools.matrixark_mcp_retrieve_pack_builder import memory_layer_pressure_summary
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_retrieve_pack_builder import memory_layer_pressure_summary


def attach_python_retrieval_metrics(
    pack: Json,
    args: Json,
    *,
    stage_latencies_ms: dict[str, float],
    retrieval_scan_stats: Json,
    selected: list[Json],
    dropped_over_budget: list[Json],
    records: list[Json],
) -> None:
    placement = retrieval_scan_stats.get("native_selected_node_locations", {}) if isinstance(retrieval_scan_stats, dict) else {}
    recall_policy = pack.get("recall_policy") if isinstance(pack.get("recall_policy"), dict) else {}
    memory_layer_budget = recall_policy.get("memory_layer_budget") if isinstance(recall_policy.get("memory_layer_budget"), dict) else {}
    dropped_memory_layer_budget = recall_policy.get("dropped_memory_layer_budget") if isinstance(recall_policy.get("dropped_memory_layer_budget"), dict) else {}
    memory_layer_pressure = (
        recall_policy.get("memory_layer_pressure")
        if isinstance(recall_policy.get("memory_layer_pressure"), dict)
        else memory_layer_pressure_summary(memory_layer_budget, dropped_memory_layer_budget)
    )
    candidate_cache_hit = bool(
        isinstance(retrieval_scan_stats, dict)
        and (
            retrieval_scan_stats.get("cache_hit")
            or retrieval_scan_stats.get("candidate_cache_hit")
            or retrieval_scan_stats.get("native_placement_candidate_cache_hit")
        )
    )
    index_postings_read = (
        int(retrieval_scan_stats.get("index_postings_read") or 0)
        if isinstance(retrieval_scan_stats, dict)
        else 0
    )
    if isinstance(retrieval_scan_stats, dict) and not index_postings_read:
        index_postings_read = int(
            retrieval_scan_stats.get("index_postings_touched")
            or retrieval_scan_stats.get("native_index_postings_found")
            or 0
        )
    pack["retrieval_metrics"] = {
        "query_plan_ms": round(float(stage_latencies_ms.get("query_understanding", 0.0)), 3),
        "node_traversal_ms": round(float(stage_latencies_ms.get("node_traversal", 0.0)), 3),
        "index_prefilter_ms": round(float(stage_latencies_ms.get("candidate_fetch", 0.0)), 3),
        "candidate_fetch_ms": round(float(stage_latencies_ms.get("candidate_fetch", 0.0)), 3),
        "score_ms": round(float(stage_latencies_ms.get("rerank_score", 0.0)), 3),
        "pack_ms": round(float(stage_latencies_ms.get("pack", 0.0)), 3),
        "audit_ms": round(float(stage_latencies_ms.get("audit", 0.0)), 3),
        "append_queue_wait_ms": 0.0,
        "append_engine_ms": 0.0,
        "selected_refs": len(selected),
        "dropped_refs": int(len(dropped_over_budget)),
        "scanned_records": int(retrieval_scan_stats.get("loaded_records") or retrieval_scan_stats.get("scanned_records") or len(records)) if isinstance(retrieval_scan_stats, dict) else len(records),
        "candidate_cache_hit": candidate_cache_hit,
        "cache_hit": candidate_cache_hit,
        "index_postings_read": index_postings_read,
        "index_postings_touched": index_postings_read,
        "placement_partitions_touched": len(placement.get("locations", []) or []) if isinstance(placement, dict) else 0,
        "native_pack_assembly": False,
        "python_pack_fallback": True,
        "raw_candidate_tables_returned": False,
        "memory_layer_budget": memory_layer_budget,
        "dropped_memory_layer_budget": dropped_memory_layer_budget,
        "memory_layer_pressure": memory_layer_pressure,
        "source": "python_reference_pack",
    }
    if bool(args.get("include_retrieval_metrics")):
        pack["include_retrieval_metrics"] = True
