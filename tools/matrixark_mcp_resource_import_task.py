#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Resource import task record builders."""

from __future__ import annotations

from typing import Any

try:
    from tools.matrixark_mcp_core import Json
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_core import Json


def resource_import_task_record(
    *,
    task_hash: int,
    status: str,
    kind: str,
    raw_uri: str,
    requested_raw_uri: str,
    resource_type: str,
    raw_storage_mode: str,
    raw_storage_policy: str,
    node_hash: int,
    node_path: list[str],
    scope: Json,
    updated_at_ms: int,
    raw_bytes_stored: bool = False,
    progress: Json | None = None,
    storage_options: Json | None = None,
    wait: bool | None = None,
    async_default_reason: str | None = None,
    extra: Json | None = None,
) -> Json:
    record: Json = {
        "record_type": "resource_import_task",
        "task_hash": task_hash,
        "status": status,
        "kind": kind,
        "raw_uri": raw_uri,
        "requested_raw_uri": requested_raw_uri,
        "resource_type": resource_type,
        "raw_storage_mode": raw_storage_mode,
        "raw_storage_policy": raw_storage_policy,
        "raw_bytes_stored": raw_bytes_stored,
        "node_hash": node_hash,
        "node_path": node_path,
        "scope": scope,
        "progress": progress or {"stage": status, "percent": 100 if status in {"completed", "failed"} else 0},
        "updated_at_ms": updated_at_ms,
    }
    if storage_options is not None:
        record["storage_options"] = storage_options
    if wait is not None:
        record["wait"] = wait
    if async_default_reason is not None:
        record["async_default_reason"] = async_default_reason
    if extra:
        record.update(extra)
    return record
