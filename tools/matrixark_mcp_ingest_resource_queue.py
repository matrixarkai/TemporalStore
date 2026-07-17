#!/usr/bin/env python3
"""Resource import queue orchestration for MatrixArk local ingest."""

from __future__ import annotations

from typing import Any

try:
    from tools.matrixark_mcp_core import Json, MatrixArkError, now_ms
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_core import Json, MatrixArkError, now_ms

try:
    from tools.matrixark_mcp_ingest_response import build_resource_import_queued_response
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_ingest_response import build_resource_import_queued_response

try:
    from tools.matrixark_mcp_resource_import_task import resource_import_task_record
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_resource_import_task import resource_import_task_record


def queue_resource_import_if_needed(
    adapter: Any,
    *,
    args: Json,
    envelope: Json,
    hook: Json,
    event_id_hash: int,
    node_hash: int,
    node_path: list[str],
    node_materialization: Json,
    resource_record_scope: Json,
    requested_raw_uri: str,
    resource_type: str,
    resource_import_task_hash: int,
    resource_import_wait: bool,
    resource_import_background: bool,
    storage_resolution: Json,
    raw_storage_policy: str,
    async_default_reason: str,
) -> Json | None:
    if not resource_import_background:
        adapter.append(
            resource_import_task_record(
                task_hash=resource_import_task_hash,
                status="queued",
                kind=envelope["kind"],
                raw_uri=requested_raw_uri,
                requested_raw_uri=requested_raw_uri,
                resource_type=resource_type,
                raw_storage_mode=str(storage_resolution["storage_mode"]),
                raw_storage_policy=raw_storage_policy,
                node_hash=node_hash,
                node_path=node_path,
                scope=resource_record_scope,
                storage_options=envelope.get("storage_options", {}),
                wait=resource_import_wait,
                async_default_reason=async_default_reason,
                progress={"stage": "queued", "percent": 0},
                updated_at_ms=envelope["ingestion_time_ms"],
                extra={"created_at_ms": envelope["ingestion_time_ms"]},
            )
        )
    if resource_import_wait:
        return None
    background_args = {
        **args,
        "wait": True,
        "_background_resource_import": True,
        "_resource_import_task_hash": resource_import_task_hash,
    }
    try:
        queue_status = adapter._enqueue_resource_import(
            args=background_args,
            hook=hook,
            task_hash=resource_import_task_hash,
        )
    except MatrixArkError as exc:
        adapter.append(
            resource_import_task_record(
                task_hash=resource_import_task_hash,
                status="failed",
                kind=envelope["kind"],
                raw_uri=requested_raw_uri,
                requested_raw_uri=requested_raw_uri,
                resource_type=resource_type,
                raw_storage_mode=str(storage_resolution["storage_mode"]),
                raw_storage_policy=raw_storage_policy,
                node_hash=node_hash,
                node_path=node_path,
                scope=resource_record_scope,
                storage_options=envelope.get("storage_options", {}),
                progress={"stage": "failed", "percent": 100},
                updated_at_ms=now_ms(),
                extra={"error": str(exc)},
            )
        )
        raise
    return build_resource_import_queued_response(
        event_id_hash=event_id_hash,
        node_hash=node_hash,
        resource_import_task_hash=resource_import_task_hash,
        requested_raw_uri=requested_raw_uri,
        resource_type=resource_type,
        storage_resolution=storage_resolution,
        raw_storage_policy=raw_storage_policy,
        queue_status=queue_status,
        async_default_reason=async_default_reason,
        node_materialization=node_materialization,
    )
