#!/usr/bin/env python3
"""Request setup helpers for MatrixArk local retrieval."""

from __future__ import annotations

import time
from typing import Any

try:
    from tools.matrixark_mcp_core import (
        CONTEXT_PACK_DEBUG_REFS,
        DEFAULT_MAX_CONTEXT_TOKENS,
        Json,
        integer_arg,
        normalize_storage_options,
        optional_object,
        require_string,
    )
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_core import (
        CONTEXT_PACK_DEBUG_REFS,
        DEFAULT_MAX_CONTEXT_TOKENS,
        Json,
        integer_arg,
        normalize_storage_options,
        optional_object,
        require_string,
    )

try:
    from tools import matrixark_mcp_retrieve_cache as retrieve_cache_helpers
except ModuleNotFoundError:  # Direct script execution from tools/.
    import matrixark_mcp_retrieve_cache as retrieve_cache_helpers

try:
    from tools import matrixark_mcp_retrieve_continuity as retrieve_continuity_helpers
except ModuleNotFoundError:  # Direct script execution from tools/.
    import matrixark_mcp_retrieve_continuity as retrieve_continuity_helpers

try:
    from tools import matrixark_mcp_retrieve_deadline as retrieve_deadline_helpers
except ModuleNotFoundError:  # Direct script execution from tools/.
    import matrixark_mcp_retrieve_deadline as retrieve_deadline_helpers

try:
    from tools import matrixark_mcp_retrieve_planning as retrieve_planning_helpers
except ModuleNotFoundError:  # Direct script execution from tools/.
    import matrixark_mcp_retrieve_planning as retrieve_planning_helpers


def prepare_retrieval_request(target: Any, args: Json, *, started_perf: float) -> Json:
    query = require_string(args, "query")
    scope = optional_object(args, "scope")
    storage_options = normalize_storage_options(args)
    ranking = optional_object(args, "ranking")
    audit_mode, audit_sample_rate = retrieve_planning_helpers.retrieval_audit_policy(args)
    deadline_ms = retrieve_planning_helpers.retrieval_deadline_ms(args, ranking)
    stage_budgets_ms, explicit_stage_budgets = retrieve_planning_helpers.retrieval_stage_budgets(
        args,
        ranking,
        deadline_ms=deadline_ms,
    )
    deadline_tracker = retrieve_deadline_helpers.RetrievalDeadlineTracker(
        started_perf=started_perf,
        deadline_ms=deadline_ms,
        stage_budgets_ms=stage_budgets_ms,
        explicit_stage_budgets=explicit_stage_budgets,
        observe_latency=target._observe_model_latency,
    )
    stage_started_perf = time.perf_counter()

    retrieval_plan = retrieve_planning_helpers.retrieval_query_budget_plan(
        args,
        ranking,
        query=query,
        scope=scope,
        default_max_context_tokens=DEFAULT_MAX_CONTEXT_TOKENS,
    )
    question_type = str(retrieval_plan["question_type"])
    retrieval_session_scope = str(retrieval_plan["retrieval_session_scope"])
    retrieval_scope = retrieval_plan["retrieval_scope"]
    local_budget = retrieval_plan["local_budget"]
    max_context_tokens = int(retrieval_plan["max_context_tokens"])
    debug_refs = bool(args.get("include_debug_refs") or ranking.get("include_debug_refs") or CONTEXT_PACK_DEBUG_REFS)
    pack_cache_key = retrieve_cache_helpers.context_pack_cache_key(
        target,
        scope=scope,
        query=query,
        question_type=question_type,
        retrieval_session_scope=retrieval_session_scope,
        max_context_tokens=max_context_tokens,
        local_budget=local_budget,
        ranking=ranking,
        include_superseded=bool(args.get("include_superseded_resources", False) or args.get("historical_replay", False)),
    )
    cached_pack = retrieve_cache_helpers.get_cached_context_pack(target, pack_cache_key, include_debug=debug_refs)
    annotate_session_continuity = retrieve_continuity_helpers.make_session_continuity_annotator(
        retrieval_scope=retrieval_scope,
        question_type=question_type,
    )

    return {
        "query": query,
        "scope": scope,
        "storage_options": storage_options,
        "ranking": ranking,
        "audit_mode": audit_mode,
        "audit_sample_rate": audit_sample_rate,
        "deadline_ms": deadline_ms,
        "deadline_tracker": deadline_tracker,
        "stage_started_perf": stage_started_perf,
        "retrieval_plan": retrieval_plan,
        "question_type": question_type,
        "retrieval_session_scope": retrieval_session_scope,
        "retrieval_scope": retrieval_scope,
        "secondary_index_filter_groups": retrieval_plan["secondary_index_filter_groups"],
        "secondary_index_filter_mode": str(retrieval_plan["secondary_index_filter_mode"]),
        "budget_source": str(retrieval_plan["budget_source"]),
        "max_context_tokens": max_context_tokens,
        "local_budget": local_budget,
        "local_tokens": int(local_budget.get("token_estimate", 0)),
        "safety_margin_tokens": int(local_budget.get("safety_margin_tokens", 0)),
        "remote_context_budget_tokens": int(retrieval_plan["remote_context_budget_tokens"]),
        "cross_session_policy": retrieval_plan["cross_session_policy"],
        "shared_context_policy": retrieval_plan["shared_context_policy"],
        "query_terms": retrieval_plan["query_terms"],
        "reference_time_ms": int(retrieval_plan["reference_time_ms"]),
        "query_plan": retrieval_plan["query_plan"],
        "debug_refs": debug_refs,
        "pack_cache_key": pack_cache_key,
        "cached_pack": cached_pack,
        "auxiliary_quota": integer_arg(ranking, "auxiliary_quota", 2, minimum=0),
        "annotate_session_continuity": annotate_session_continuity,
    }
