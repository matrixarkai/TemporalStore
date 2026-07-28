#!/usr/bin/env python3
"""Async extraction pipeline readiness helpers for MatrixArk retrieval."""

from __future__ import annotations

try:
    from tools.matrixark_mcp_core import Json, candidate_access_scope, normalize_message_role, scope_matches
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_core import Json, candidate_access_scope, normalize_message_role, scope_matches


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


def _layer_readiness_entry(*, pending_tasks: int, remaining_stages: list[str]) -> Json:
    return {
        "ready": pending_tasks == 0 and not remaining_stages,
        "pending_task_count": pending_tasks,
        "remaining_stages": remaining_stages,
    }


def async_memory_layer_readiness(
    *,
    pending_memory_scopes: dict[str, int],
    pending_session_continuities: dict[str, int],
    remaining_stage_counts: dict[str, int],
) -> Json:
    layers: Json = {
        "session": _layer_readiness_entry(
            pending_tasks=int(pending_memory_scopes.get("session", 0)),
            remaining_stages=[],
        ),
        "user_profile": _layer_readiness_entry(
            pending_tasks=int(pending_memory_scopes.get("user_profile", 0)),
            remaining_stages=[],
        ),
        "same_session": _layer_readiness_entry(
            pending_tasks=int(pending_session_continuities.get("same_session", 0)),
            remaining_stages=[],
        ),
        "cross_session": _layer_readiness_entry(
            pending_tasks=int(pending_session_continuities.get("cross_session", 0)),
            remaining_stages=[],
        ),
        "summary": _layer_readiness_entry(
            pending_tasks=int(remaining_stage_counts.get("summary", 0)),
            remaining_stages=["summary"] if int(remaining_stage_counts.get("summary", 0)) else [],
        ),
        "compression": _layer_readiness_entry(
            pending_tasks=int(remaining_stage_counts.get("compression", 0)),
            remaining_stages=["compression"] if int(remaining_stage_counts.get("compression", 0)) else [],
        ),
        "embedding": _layer_readiness_entry(
            pending_tasks=int(remaining_stage_counts.get("embedding", 0)),
            remaining_stages=["embedding"] if int(remaining_stage_counts.get("embedding", 0)) else [],
        ),
    }
    blocked_layers = [name for name, layer in layers.items() if not bool(layer.get("ready"))]
    return {
        "layers": layers,
        "blocked_layers": blocked_layers,
        "ready_layers": [name for name, layer in layers.items() if bool(layer.get("ready"))],
        "ready_for_retrieval": not blocked_layers,
    }


def async_memory_layer_freshness_warnings(layer_readiness: Json) -> list[str]:
    blocked_layers = {
        str(layer or "").strip()
        for layer in layer_readiness.get("blocked_layers", [])
        if str(layer or "").strip()
    } if isinstance(layer_readiness, dict) else set()
    warning_by_layer = {
        "session": "session_memory_stale",
        "user_profile": "profile_memory_stale",
        "same_session": "same_session_memory_stale",
        "cross_session": "cross_session_memory_stale",
        "summary": "summary_memory_stale",
        "compression": "compression_memory_pending",
        "embedding": "embedding_memory_pending",
    }
    return [
        warning
        for layer, warning in warning_by_layer.items()
        if layer in blocked_layers
    ]


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
    remaining_stage_counts: dict[str, int] = {}
    pending_source_roles: dict[str, int] = {}
    pending_source_hook_types: dict[str, int] = {}
    pending_source_codex_events: dict[str, int] = {}
    pending_memory_scopes: dict[str, int] = {}
    pending_session_continuities: dict[str, int] = {}
    pending_extraction_phases: dict[str, int] = {}
    pending_final_session_boundary_count = 0
    pending_task_count = 0
    extraction_committed_task_count = 0
    summary_completed_task_count = 0
    remaining_stages: set[str] = set()
    completed_stages: set[str] = set()

    def add_count(bucket: dict[str, int], key: object) -> None:
        value = str(key or "").strip()
        if value:
            bucket[value] = bucket.get(value, 0) + 1

    def add_count_map(bucket: dict[str, int], counts: object, *, normalize_roles: bool = False) -> bool:
        if not isinstance(counts, dict):
            return False
        added = False
        for key, raw_count in counts.items():
            value = normalize_message_role(key) if normalize_roles else str(key or "").strip()
            if not value:
                continue
            try:
                count = int(raw_count or 0)
            except (TypeError, ValueError):
                continue
            if count <= 0:
                continue
            bucket[value] = bucket.get(value, 0) + count
            added = True
        return added

    for row in latest_rows:
        status = str(row.get("status") or "unknown")
        status_counts[status] = status_counts.get(status, 0) + 1
        pending_task_count += int(status == "pending")
        extraction_committed_task_count += int(status == "extraction_committed")
        summary_completed_task_count += int(status == "summary_completed")
        row_remaining_stages = row.get("remaining_stages") if isinstance(row.get("remaining_stages"), list) else []
        for stage in row_remaining_stages:
            stage_name = str(stage or "").strip()
            if stage_name:
                remaining_stages.add(stage_name)
                remaining_stage_counts[stage_name] = remaining_stage_counts.get(stage_name, 0) + 1
        for stage in row.get("completed_stages") if isinstance(row.get("completed_stages"), list) else []:
            stage_name = str(stage or "").strip()
            if stage_name:
                completed_stages.add(stage_name)
        if row_remaining_stages or status in {"pending", "extraction_committed"}:
            if not add_count_map(pending_source_roles, row.get("source_role_counts"), normalize_roles=True):
                for role in row.get("source_roles") if isinstance(row.get("source_roles"), list) else []:
                    add_count(pending_source_roles, normalize_message_role(role))
            if not add_count_map(pending_source_hook_types, row.get("source_hook_type_counts")):
                for hook_type in row.get("source_hook_types") if isinstance(row.get("source_hook_types"), list) else []:
                    add_count(pending_source_hook_types, hook_type)
            if not add_count_map(pending_source_codex_events, row.get("source_codex_event_counts")):
                for codex_event in row.get("source_codex_events") if isinstance(row.get("source_codex_events"), list) else []:
                    add_count(pending_source_codex_events, codex_event)
            layers = row.get("memory_layers_written") if isinstance(row.get("memory_layers_written"), dict) else {}
            if int(layers.get("session_entities") or 0) > 0:
                add_count(pending_memory_scopes, "session")
            if int(layers.get("profile_entities") or 0) > 0:
                add_count(pending_memory_scopes, "user_profile")
            if int(layers.get("same_session_entities") or 0) > 0:
                add_count(pending_session_continuities, "same_session")
            if int(layers.get("cross_session_entities") or 0) > 0:
                add_count(pending_session_continuities, "cross_session")
            add_count(pending_extraction_phases, row.get("extraction_phase"))
            if bool(row.get("final_session_boundary")):
                pending_final_session_boundary_count += 1
    warnings: list[str] = []
    if pending_task_count:
        warnings.append("async_pipeline_pending")
    if extraction_committed_task_count:
        warnings.append("async_pipeline_followup_pending")
    if remaining_stages:
        warnings.append("async_pipeline_remaining_stages:" + ",".join(sorted(remaining_stages)))
    layer_readiness = async_memory_layer_readiness(
        pending_memory_scopes=pending_memory_scopes,
        pending_session_continuities=pending_session_continuities,
        remaining_stage_counts=remaining_stage_counts,
    )
    warnings.extend(async_memory_layer_freshness_warnings(layer_readiness))
    return {
        "task_count": len(latest_rows),
        "status_counts": dict(sorted(status_counts.items())),
        "pending_task_count": pending_task_count,
        "extraction_committed_task_count": extraction_committed_task_count,
        "summary_completed_task_count": summary_completed_task_count,
        "completed_stages": sorted(completed_stages),
        "remaining_stages": sorted(remaining_stages),
        "remaining_stage_counts": dict(sorted(remaining_stage_counts.items())),
        "pending_source_roles": dict(sorted(pending_source_roles.items())),
        "pending_source_hook_types": dict(sorted(pending_source_hook_types.items())),
        "pending_source_codex_events": dict(sorted(pending_source_codex_events.items())),
        "pending_memory_scopes": dict(sorted(pending_memory_scopes.items())),
        "pending_session_continuities": dict(sorted(pending_session_continuities.items())),
        "pending_extraction_phases": dict(sorted(pending_extraction_phases.items())),
        "pending_final_session_boundary_count": pending_final_session_boundary_count,
        "memory_layer_readiness": layer_readiness,
        "ready_for_retrieval": not pending_task_count and not extraction_committed_task_count and not remaining_stages,
        "freshness_warnings": warnings,
    }
