#!/usr/bin/env python3
"""Async extraction pipeline readiness helpers for MatrixArk retrieval."""

from __future__ import annotations

try:
    from tools.matrixark_mcp_core import Json, candidate_access_scope, scope_matches
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_core import Json, candidate_access_scope, scope_matches


def latest_async_pipeline_rows(rows: list[Json]) -> list[Json]:
    status_rank = {"pending": 0, "extraction_committed": 1, "summary_completed": 2}
    latest_by_task: dict[int, Json] = {}
    for row in rows:
        try:
            task_hash = int(row.get("task_hash") or row.get("event_id_hash"))
        except (TypeError, ValueError):
            continue
        current = latest_by_task.get(task_hash)
        current_rank = status_rank.get(str(current.get("status") or ""), -1) if current else -1
        row_rank = status_rank.get(str(row.get("status") or ""), -1)
        current_time = int(current.get("updated_at_ms") or current.get("created_at_ms") or 0) if current else -1
        row_time = int(row.get("updated_at_ms") or row.get("created_at_ms") or 0)
        if current is None or (row_rank, row_time) >= (current_rank, current_time):
            latest_by_task[task_hash] = row
    return list(latest_by_task.values())


def async_pipeline_retrieval_readiness(records: list[Json], scope: Json) -> Json:
    readiness_scope = dict(scope)
    session_scope_mode = str(readiness_scope.pop("_session_scope", "") or "")
    if session_scope_mode == "prefer":
        readiness_scope.pop("session_id", None)
    latest_rows = latest_async_pipeline_rows(
        [
            record
            for record in records
            if record.get("record_type") == "matrixark_async_pipeline_task"
            and scope_matches(candidate_access_scope(record), readiness_scope)
        ]
    )
    status_counts: dict[str, int] = {}
    pending_task_count = 0
    extraction_committed_task_count = 0
    summary_completed_task_count = 0
    remaining_stages: set[str] = set()
    completed_stages: set[str] = set()
    for row in latest_rows:
        status = str(row.get("status") or "unknown")
        status_counts[status] = status_counts.get(status, 0) + 1
        pending_task_count += int(status == "pending")
        extraction_committed_task_count += int(status == "extraction_committed")
        summary_completed_task_count += int(status == "summary_completed")
        for stage in row.get("remaining_stages") if isinstance(row.get("remaining_stages"), list) else []:
            stage_name = str(stage or "").strip()
            if stage_name:
                remaining_stages.add(stage_name)
        for stage in row.get("completed_stages") if isinstance(row.get("completed_stages"), list) else []:
            stage_name = str(stage or "").strip()
            if stage_name:
                completed_stages.add(stage_name)
    warnings: list[str] = []
    if pending_task_count:
        warnings.append("async_pipeline_pending")
    if extraction_committed_task_count:
        warnings.append("async_pipeline_followup_pending")
    if remaining_stages:
        warnings.append("async_pipeline_remaining_stages:" + ",".join(sorted(remaining_stages)))
    return {
        "task_count": len(latest_rows),
        "status_counts": dict(sorted(status_counts.items())),
        "pending_task_count": pending_task_count,
        "extraction_committed_task_count": extraction_committed_task_count,
        "summary_completed_task_count": summary_completed_task_count,
        "completed_stages": sorted(completed_stages),
        "remaining_stages": sorted(remaining_stages),
        "ready_for_retrieval": not pending_task_count and not extraction_committed_task_count and not remaining_stages,
        "freshness_warnings": warnings,
    }
