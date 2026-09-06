#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Retrieval metrics attachment helpers for MatrixArk local runtime."""

from __future__ import annotations

import os
from typing import Any

Json = dict[str, Any]

try:
    from tools.matrixark_mcp_retrieve_pack_builder import memory_layer_pressure_summary
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_retrieve_pack_builder import memory_layer_pressure_summary




def serving_memory_layer_budget(memory_layer_budget: Any) -> Json:
    if not isinstance(memory_layer_budget, dict):
        return {}
    compact = dict(memory_layer_budget)
    for field in [
        "by_source_role",
        "by_hook_type",
        "by_codex_event",
        "source_message_counts_by_role",
        "source_hook_counts_by_type",
        "source_codex_event_counts_by_event",
    ]:
        compact.pop(field, None)
    return compact


def serving_memory_layer_pressure(memory_layer_pressure: Any) -> Json:
    if not isinstance(memory_layer_pressure, dict):
        return {}
    compact = dict(memory_layer_pressure)
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
    return compact


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
    serving_budget = serving_memory_layer_budget(memory_layer_budget)
    serving_dropped_budget = serving_memory_layer_budget(dropped_memory_layer_budget)
    serving_pressure = serving_memory_layer_pressure(memory_layer_pressure)
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
        "memory_layer_budget": serving_budget,
        "dropped_memory_layer_budget": serving_dropped_budget,
        "memory_layer_pressure": serving_pressure,
        "source": "python_reference_pack",
    }
    if bool(args.get("include_retrieval_metrics")):
        pack["include_retrieval_metrics"] = True
