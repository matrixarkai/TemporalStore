#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Native ContextPack request helpers for MatrixArk TemporalStore adapters."""

from __future__ import annotations

try:
    from tools.matrixark_mcp_env import env_bool
except ImportError:  # Direct script execution from tools/.
    from matrixark_mcp_env import env_bool


import os
from typing import Any

try:
    from tools.matrixark_mcp_core import (
        DEFAULT_MAX_CONTEXT_TOKENS,
        DIRECT_RECORD_LOG_SHARD_SIZE,
        Json,
        canonical_scope_key,
        now_ms,
        optional_object,
        require_string,
        stable_hash,
    )
    from tools.matrixark_mcp_native_helpers import native_scope_with_hashes
    from tools.matrixark_mcp_retrieval import native_retrieve_fallback_allowed
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_core import (
        DEFAULT_MAX_CONTEXT_TOKENS,
        DIRECT_RECORD_LOG_SHARD_SIZE,
        Json,
        canonical_scope_key,
        now_ms,
        optional_object,
        require_string,
        stable_hash,
    )
    from matrixark_mcp_native_helpers import native_scope_with_hashes
    from matrixark_mcp_retrieval import native_retrieve_fallback_allowed


def build_native_context_pack_request(target: Any, args: Json) -> Json:
    scope = native_scope_with_hashes(optional_object(args, "scope"))
    query = require_string(args, "query")
    ranking = optional_object(args, "ranking")
    scope_key = canonical_scope_key(scope)
    native_node_path = optional_object(args, "metadata").get("node_path")
    if not isinstance(native_node_path, list) or not native_node_path:
        native_node_path = target.default_session_node_path(scope)
    native_start_node_hash = stable_hash("/".join(str(part) for part in native_node_path))
    reference_time_ms = int(args.get("reference_time_ms", now_ms()) or now_ms())
    local_context = args.get("local_context", [])
    if not isinstance(local_context, list):
        local_context = []
    watermark_count = target._entry_count_cache if target._entry_count_cache is not None else target._get_count()
    resource_version_watermark = str(
        ranking.get("resource_version_watermark")
        or args.get("resource_version_watermark")
        or ""
    )
    skill_status_watermark = str(
        ranking.get("skill_status_watermark")
        or args.get("skill_status_watermark")
        or ""
    )
    normal_path_stages = [
        "query_understanding",
        "scope_filter",
        "l0_l1_node_traversal",
        "compact_secondary_index_prefilter",
        "placement_key_candidate_fetch",
        "native_score_rerank_pack",
    ]
    debug_context_pack = bool(args.get("debug_context_pack") or args.get("include_retrieval_debug"))
    verbose_native_contract = debug_context_pack or env_bool("MATRIXARK_NATIVE_CONTEXT_PACK_VERBOSE_CONTRACT", False)
    request: Json = {
        "api_version": 1,
        "storage_prefix": target._storage_prefix,
        "backend": target._backend_label(),
        "watermark_count": watermark_count,
        "append_watermark": watermark_count,
        "resource_version_watermark": resource_version_watermark,
        "skill_status_watermark": skill_status_watermark,
        "index_posting_watermark": watermark_count,
        "query": query,
        "scope": scope,
        "scope_key": scope_key,
        "tenant_hash": int(scope.get("tenant_hash") or 0),
        "scope_hash": stable_hash(scope_key) if scope_key else 0,
        "start_node_hash": native_start_node_hash,
        "placement_node_hash": native_start_node_hash,
        "placement_key": f"context:{scope_key}:node={native_start_node_hash}",
        "native_start_node_path": [str(part) for part in native_node_path],
        "start_time_ms": 1,
        "end_time_ms": reference_time_ms,
        "as_of_ms": reference_time_ms,
        "max_selected_refs": int(ranking.get("max_selected_refs") or args.get("max_selected_refs") or 24),
        "min_score": float(ranking.get("min_score") or args.get("min_score") or 0.0),
        "decay_half_life_ms": int(ranking.get("half_life_ms") or 0),
        "max_depth": int(ranking.get("max_depth") or 4),
        "top_k_per_depth": int(ranking.get("top_k_per_layer") or ranking.get("top_k_per_depth") or 16),
        "max_children_scored_per_parent": int(ranking.get("max_children_scored_per_parent") or 256),
        "max_candidate_nodes": int(ranking.get("max_candidate_nodes") or 64),
        "shared_resource_max_refs": int(ranking.get("shared_resource_max_refs") or args.get("shared_resource_max_refs") or 4),
        "skill_max_refs": int(ranking.get("skill_max_refs") or args.get("skill_max_refs") or 4),
        "cross_session_max_refs": int(ranking.get("cross_session_max_refs") or args.get("cross_session_max_refs") or 4),
        "cross_session_rerank": bool(ranking.get("cross_session_rerank", True)),
        "same_session_priority": bool(ranking.get("same_session_priority", True)),
        "leaf_only": bool(ranking.get("leaf_only", False)),
        "allow_broad_scan_fallback": bool(native_retrieve_fallback_allowed(args)),
        "ranking": ranking,
        "storage_options": optional_object(args, "storage_options"),
        "max_context_tokens": int(args.get("max_context_tokens") or DEFAULT_MAX_CONTEXT_TOKENS),
        "local_context": local_context,
        "local_context_tokens": int(args.get("local_context_tokens") or 0),
        "local_context_safety_margin_tokens": args.get("local_context_safety_margin_tokens"),
        "reference_time_ms": reference_time_ms,
        "include_superseded": bool(args.get("include_superseded_resources", False) or args.get("historical_replay", False)),
        "include_superseded_resources": bool(args.get("include_superseded_resources", False) or args.get("historical_replay", False)),
        "debug_context_pack": debug_context_pack,
        "include_retrieval_metrics": bool(args.get("include_retrieval_metrics")),
        "normal_path_stages": normal_path_stages,
    }
    if verbose_native_contract:
        request.update(native_context_pack_verbose_contract(args))
    if not hasattr(target, "_shard_size"):
        target._shard_size = DIRECT_RECORD_LOG_SHARD_SIZE
    cache_key = target._direct_context_pack_response_cache_key(
        count_key=target._count_key,
        record_hash_key=target._record_hash_key,
        shard_size=target._shard_size,
        request=request,
    )
    return {
        "request": request,
        "cache_key": cache_key,
        "scope": scope,
        "query": query,
        "debug_context_pack": debug_context_pack,
    }


def native_context_pack_verbose_contract(args: Json) -> Json:
    return {
        "required_native_apis": [
            "health",
            "readiness",
            "metrics",
            "matrixark_batch_append_records",
            "matrixark_retrieve_context_pack",
            "compact_secondary_index_lookup",
            "placement_key_candidate_fetch",
        ],
        "normalization_requirements": {
            "scope_key": "canonical",
            "node_hash": "integer",
            "placement_key": "context:{scope_key}:node={node_hash}",
            "resource_visibility": "apply_scope_before_scoring",
            "skill_visibility": "apply_scope_before_scoring",
            "shared_resource_scope": "tenant_or_global_visible_before_scoring",
            "stale_superseded_state": "exclude_unless_include_superseded_resources",
        },
        "execution_plan_requirements": {
            "phase": "phase4_native_score_rerank_pack",
            "context_record_route": "context:{scope_key}:node={node_hash}",
            "traversal": "score_l0_l1_then_fetch_selected_node_partitions",
            "candidate_fetch": "selected_node_placement_partitions_only",
            "candidate_cache": "scope_key+node_hash+record_type+append_watermark+resource_version_watermark",
            "candidate_cache_payload": "compact_structs_not_json_strings",
            "secondary_index": "compact_postings_by_scope_index_time_bucket",
            "scoring": "native_embedding_similarity_temporal_decay_business_boost_same_session_boost",
            "quotas": "native_shared_resource_quota_cross_session_quota_current_session_priority",
            "rerank": "native_score_fusion_then_budget_aware_rerank",
            "token_budget_pack": "native_budget_pack_with_selected_refs_and_dropped_summary",
            "pack_assembly": "native_score_rank_budget_pack_selected_refs_dropped_summary",
            "python_role": "dispatcher_only_no_candidate_materialization_no_hot_path_pack",
            "write_path": "native_batch_append_records_append_queue_coalesced_persistence",
            "write_route": "placement_key_partition_route_before_persistence",
            "write_coalescing": "native_append_queue_coalesces_by_record_key_field",
            "durability": "storage_options_select_async_sync_shared_store_or_raft",
            "retrieval_hot_path_audit": "inline_counters_only_no_full_audit_blocking",
            "context_pack_audit": "sample_or_enqueue_async_policy_enabled",
            "full_replay_audit_default": "disabled",
            "broad_prefix_scan": "disabled_unless_explicit_debug_fallback",
            "fallback_telemetry_required": True,
            "health_readiness_metrics": "native_backend_must_expose_health_readiness_metrics",
            "normal_path": "query_understanding_scope_filter_l0_l1_traversal_compact_index_placement_fetch_native_score_rerank_pack",
        },
        "required_output": {
            "context_pack": True,
            "selected_refs": True,
            "dropped_summary": True,
            "drop_counters": [
                "scope",
                "placement",
                "index_filter",
                "stale",
                "token_budget",
                "score_threshold",
            ],
            "telemetry": True,
            "retrieval_metrics": bool(args.get("include_retrieval_metrics")),
            "placement_partitions_touched": True,
            "index_postings_read": True,
            "candidate_cache_hit": True,
            "candidate_cache_key_shape": True,
            "native_pack_assembly": True,
            "raw_candidate_tables": False,
            "python_pack_fallback": False,
            "broad_scan_used": True,
            "normal_path_stages": True,
            "health_readiness_metrics": True,
        },
    }
