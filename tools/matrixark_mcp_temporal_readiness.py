#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Backend readiness helpers for TemporalStore MatrixArk adapters."""

from __future__ import annotations

import json
import os
import time
from typing import Any

try:
    from tools.matrixark_mcp_core import (
        BACKEND_READINESS_BACKOFF_MS,
        BACKEND_READINESS_TIMEOUT_MS,
        Json,
        MatrixArkError,
        is_retryable_temporalstore_error,
        metaserver_reachable,
        now_ms,
        stable_hash,
    )
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_core import (
        BACKEND_READINESS_BACKOFF_MS,
        BACKEND_READINESS_TIMEOUT_MS,
        Json,
        MatrixArkError,
        is_retryable_temporalstore_error,
        metaserver_reachable,
        now_ms,
        stable_hash,
    )


def readiness_failure_result(
    adapter: Any,
    *,
    reason: str,
    probe: bool,
    attempts: int,
    attempt_log: list[Json],
    error: str,
    checks: Json,
    metaserver: str,
    warmup_key: str,
    warmup_field: str,
) -> Json:
    return {
        "status": "topology_not_ready",
        "backend": adapter._backend_label(),
        "reason": reason,
        "probe": bool(probe),
        "attempts": attempts,
        "attempt_log": attempt_log,
        "error": error,
        "topology": {
            "metaserver": metaserver,
            "namespace": adapter._namespace,
            "table": adapter._table,
            "storage_prefix": adapter._storage_prefix,
            "warmup_key": warmup_key,
            "warmup_field": warmup_field,
        },
        "checks": checks,
    }


def run_backend_readiness_gate(
    adapter: Any,
    *,
    reason: str,
    probe: bool = True,
    timeout_ms: int | None = None,
) -> Json:
    timeout = max(1, int(timeout_ms or BACKEND_READINESS_TIMEOUT_MS))
    timeout_s = max(0.1, timeout / 1000.0)
    backoff_s = max(0.01, BACKEND_READINESS_BACKOFF_MS / 1000.0)
    deadline = time.monotonic() + timeout_s
    attempts = 0
    metaserver = adapter._backend_metaserver()
    key = f"{adapter._storage_prefix}:readiness"
    field = f"{os.getpid()}:{int(time.time() * 1000)}:{stable_hash(reason)}"
    value = json.dumps({"reason": reason, "pid": os.getpid(), "created_at_ms": now_ms()}, sort_keys=True, separators=(",", ":"))
    attempt_log: list[Json] = []
    while True:
        attempts += 1
        checks: Json = {
            "mcp_process_started": True,
            "metaserver_reachable": {"ok": False, "address": metaserver, "error": "not checked"},
            "namespace_table_opened": False,
            "slot_coverage_verified_by_warmup_hset_hget": False,
        }
        if metaserver:
            meta_check = metaserver_reachable(metaserver)
            checks["metaserver_reachable"] = meta_check
            if not bool(meta_check.get("ok")):
                last_error = f"metaserver unreachable: {meta_check.get('error', 'unknown')}"
                attempt_log.append({"attempt": attempts, "ok": False, "retryable": True, "error": last_error, "checks": checks})
                if time.monotonic() >= deadline:
                    return readiness_failure_result(
                        adapter,
                        reason=reason,
                        probe=probe,
                        attempts=attempts,
                        attempt_log=attempt_log,
                        error=last_error,
                        checks=checks,
                        metaserver=metaserver,
                        warmup_key=key,
                        warmup_field=field,
                    )
                time.sleep(min(backoff_s * attempts, 2.0))
                continue
        try:
            checks["namespace_table_opened"] = True
            if probe:
                adapter._client.hset(key, field, value)
                readback = adapter._client.hget(key, field)
                if readback != value:
                    raise MatrixArkError("readiness hget readback mismatch")
                checks["slot_coverage_verified_by_warmup_hset_hget"] = True
            return {
                "status": "ready",
                "backend": adapter._backend_label(),
                "reason": reason,
                "probe": bool(probe),
                "metaserver": metaserver,
                "storage_prefix": adapter._storage_prefix,
                "warmup_key": key,
                "attempts": attempts,
                "attempt_log": attempt_log,
                "topology": {
                    "metaserver": metaserver,
                    "namespace": adapter._namespace,
                    "table": adapter._table,
                    "storage_prefix": adapter._storage_prefix,
                    "warmup_key": key,
                    "warmup_field": field,
                },
                "checks": checks,
            }
        except Exception as exc:
            last_error = str(exc)
            retryable = is_retryable_temporalstore_error(exc)
            attempt_log.append({"attempt": attempts, "ok": False, "retryable": retryable, "error": last_error, "checks": checks})
            if time.monotonic() >= deadline or not retryable:
                return readiness_failure_result(
                    adapter,
                    reason=reason,
                    probe=probe,
                    attempts=attempts,
                    attempt_log=attempt_log,
                    error=last_error,
                    checks=checks,
                    metaserver=metaserver,
                    warmup_key=key,
                    warmup_field=field,
                )
            time.sleep(min(backoff_s * attempts, 2.0))
