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
        candidate_access_scope,
        integer_arg,
        normalize_storage_options,
        now_ms,
        optional_object,
        require_string,
        scope_matches,
    )
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_core import (
        CONTEXT_PACK_DEBUG_REFS,
        DEFAULT_MAX_CONTEXT_TOKENS,
        Json,
        candidate_access_scope,
        integer_arg,
        normalize_storage_options,
        now_ms,
        optional_object,
        require_string,
        scope_matches,
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

try:
    from tools import matrixark_mcp_retrieve_pre_refresh as pre_refresh_helpers
except ModuleNotFoundError:  # Direct script execution from tools/.
    import matrixark_mcp_retrieve_pre_refresh as pre_refresh_helpers


def pre_retrieval_idle_commit_flush(target: Any, args: Json, ranking: Json, *, scope: Json) -> Json:
    enabled = (
        args.get("pre_retrieval_idle_commit_flush")
        if "pre_retrieval_idle_commit_flush" in args
        else ranking.get("pre_retrieval_idle_commit_flush", True)
    )
    result: Json = {"enabled": bool(enabled), "status": "disabled"}
    if not enabled:
        return result
    read_all = getattr(target, "read_all", None)
    session_commit = getattr(target, "session_commit", None)
    if not callable(read_all) or not callable(session_commit):
        return {**result, "status": "unavailable", "reason": "session_commit_or_read_all_missing"}
    try:
        records = read_all()
    except Exception as exc:
        return {**result, "status": "error", "reason": "read_all_failed", "error": str(exc)[:240]}
    current_time_ms = now_ms()
    due_tasks: list[Json] = []
    for record in records:
        if not isinstance(record, dict):
            continue
        if record.get("record_type") != "matrixark_async_pipeline_task":
            continue
        if str(record.get("status") or "") != "idle_commit_scheduled":
            continue
        if not scope_matches(candidate_access_scope(record), scope):
            continue
        try:
            deadline_ms = int(record.get("idle_commit_deadline_ms") or 0)
        except (TypeError, ValueError):
            deadline_ms = 0
        if deadline_ms > 0 and deadline_ms <= current_time_ms:
            due_tasks.append(record)
    if not due_tasks:
        return {**result, "status": "no_due_idle_commits", "due_task_count": 0}
    due_tasks.sort(key=lambda item: int(item.get("idle_commit_deadline_ms") or item.get("updated_at_ms") or 0))
    task = due_tasks[0]
    try:
        threshold = int(task.get("threshold_messages") or args.get("session_buffer_threshold") or 20)
    except (TypeError, ValueError):
        threshold = 20
    try:
        timeout_ms = int(task.get("idle_commit_timeout_ms") or args.get("idle_commit_timeout_ms") or 0)
    except (TypeError, ValueError):
        timeout_ms = 0
    commit_args: Json = {
        "scope": scope,
        "metadata": optional_object(args, "metadata"),
        "threshold_messages": max(1, threshold),
        "force": False,
        "idle_timeout_ms": max(0, timeout_ms),
        "commit_reason": "idle_timeout",
        "skip_prior_context": bool(args.get("skip_prior_context", False)),
        "storage_options": normalize_storage_options(args),
    }
    for field in [
        "understanding_provider",
        "extraction_provider",
        "segment_provider",
        "segment_model",
        "segment_model_path",
        "segment_max_new_tokens",
        "segment_provider_fallback",
    ]:
        if args.get(field) not in (None, "", [], {}):
            commit_args[field] = args.get(field)
    try:
        commit_result = session_commit(commit_args)
    except Exception as exc:
        return {
            **result,
            "status": "error",
            "reason": "session_commit_failed",
            "due_task_count": len(due_tasks),
            "error": str(exc)[:240],
        }
    return {
        **result,
        "status": "committed" if isinstance(commit_result, dict) and commit_result.get("status") == "committed" else "attempted",
        "due_task_count": len(due_tasks),
        "committed_event_count": int(commit_result.get("committed_event_count") or 0) if isinstance(commit_result, dict) else 0,
        "trigger_policy": commit_result.get("trigger_policy") if isinstance(commit_result, dict) else "",
        "commit_result_status": commit_result.get("status") if isinstance(commit_result, dict) else "",
    }


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
    pre_retrieval_idle_commit = pre_retrieval_idle_commit_flush(
        target,
        args,
        ranking,
        scope=scope,
    )
    pre_retrieval_summary_refresh, pre_retrieval_refreshed_records = pre_refresh_helpers.run_pre_retrieval_summary_refresh(
        target,
        args,
        ranking,
        scope=scope,
    )
    debug_refs = bool(args.get("include_debug_refs") or ranking.get("include_debug_refs") or CONTEXT_PACK_DEBUG_REFS)
    cache_ranking = {
        **ranking,
        "_source_role_budget_tokens": retrieval_plan["source_role_budget_tokens"],
        "_memory_layer_budget_tokens": retrieval_plan["memory_layer_budget_tokens"],
        "_memory_selection_policy_budget_tokens": retrieval_plan["memory_selection_policy_budget_tokens"],
        "_extraction_phase_budget_tokens": retrieval_plan["extraction_phase_budget_tokens"],
        "_pre_retrieval_summary_refresh": {
            "enabled": bool(pre_retrieval_summary_refresh.get("enabled")),
            "requested_limit": int(pre_retrieval_summary_refresh.get("requested_limit") or 0),
            "status": pre_retrieval_summary_refresh.get("status"),
            "refreshed_count": int(pre_retrieval_summary_refresh.get("refreshed_count") or 0),
        },
        "_pre_retrieval_idle_commit": {
            "enabled": bool(pre_retrieval_idle_commit.get("enabled")),
            "status": pre_retrieval_idle_commit.get("status"),
            "committed_event_count": int(pre_retrieval_idle_commit.get("committed_event_count") or 0),
        },
    }
    pack_cache_key = retrieve_cache_helpers.context_pack_cache_key(
        target,
        scope=scope,
        query=query,
        question_type=question_type,
        retrieval_session_scope=retrieval_session_scope,
        max_context_tokens=max_context_tokens,
        local_budget=local_budget,
        ranking=cache_ranking,
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
        "source_role_budget_tokens": retrieval_plan["source_role_budget_tokens"],
        "source_role_budget_mode": retrieval_plan["source_role_budget_mode"],
        "memory_layer_budget_tokens": retrieval_plan["memory_layer_budget_tokens"],
        "memory_layer_budget_mode": retrieval_plan["memory_layer_budget_mode"],
        "memory_selection_policy_budget_tokens": retrieval_plan["memory_selection_policy_budget_tokens"],
        "memory_selection_policy_budget_mode": retrieval_plan["memory_selection_policy_budget_mode"],
        "extraction_phase_budget_tokens": retrieval_plan["extraction_phase_budget_tokens"],
        "extraction_phase_budget_mode": retrieval_plan["extraction_phase_budget_mode"],
        "pre_retrieval_summary_refresh": pre_retrieval_summary_refresh,
        "pre_retrieval_idle_commit": pre_retrieval_idle_commit,
        "pre_retrieval_refreshed_records": pre_retrieval_refreshed_records,
        "query_terms": retrieval_plan["query_terms"],
        "reference_time_ms": int(retrieval_plan["reference_time_ms"]),
        "query_plan": retrieval_plan["query_plan"],
        "debug_refs": debug_refs,
        "pack_cache_key": pack_cache_key,
        "cached_pack": cached_pack,
        "auxiliary_quota": integer_arg(ranking, "auxiliary_quota", 2, minimum=0),
        "annotate_session_continuity": annotate_session_continuity,
    }
