#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""C++ proxy/direct backend readiness helpers for MatrixArk TemporalStore."""

from __future__ import annotations

import time
from typing import Any

try:
    from tools.matrixark_mcp_core import (
        BACKEND_READINESS_BACKOFF_MS,
        BACKEND_READINESS_TIMEOUT_MS,
        Json,
        MatrixArkError,
        is_retryable_temporalstore_error,
        json,
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
        json,
        metaserver_reachable,
        now_ms,
        stable_hash,
    )


def ensure_backend_ready(target: Any, *, reason: str = "manual", probe: bool = True, timeout_ms: int | None = None) -> Json:
    with target._readiness_lock:
        if target._readiness_cache and target._readiness_cache.get("status") == "ready":
            cached = dict(target._readiness_cache)
            cached["cached"] = True
            cached["reason"] = reason
            return cached
        timeout = max(1, int(timeout_ms or BACKEND_READINESS_TIMEOUT_MS))
        deadline = time.monotonic() + timeout / 1000.0
        attempts: list[Json] = []
        attempt = 0
        warmup_key = f"{target._storage_prefix}:readiness"
        warmup_field = f"{stable_hash(f'{target._storage_prefix}:{reason}'):020d}"
        warmup_value = json.dumps(
            {
                "probe": "matrixark_backend_ready",
                "backend": target._backend_label(),
                "reason": reason,
                "ts_ms": now_ms(),
            },
            sort_keys=True,
        )
        while True:
            attempt += 1
            checks: Json = {
                "mcp_process_started": True,
                "metaserver_reachable": metaserver_reachable(target._metaserver),
                "namespace_table_opened": False,
                "slot_coverage_verified_by_warmup_hset_hget": False,
            }
            try:
                if not checks["metaserver_reachable"].get("ok"):
                    raise MatrixArkError(checks["metaserver_reachable"].get("error", "metaserver is not reachable"))
                if probe:
                    target._client.hset(warmup_key, warmup_field, warmup_value)
                    checks["namespace_table_opened"] = True
                    readback = target._client.hget(warmup_key, warmup_field)
                    if readback != warmup_value:
                        raise MatrixArkError("readiness warmup readback mismatch")
                    checks["slot_coverage_verified_by_warmup_hset_hget"] = True
                else:
                    checks["namespace_table_opened"] = True
                result: Json = {
                    "status": "ready",
                    "backend": target._backend_label(),
                    "reason": reason,
                    "probe": bool(probe),
                    "attempts": attempt,
                    "attempt_log": attempts,
                    "topology": {
                        "metaserver": target._metaserver,
                        "namespace": target._namespace,
                        "table": target._table,
                        "storage_prefix": target._storage_prefix,
                        "warmup_key": warmup_key,
                        "warmup_field": warmup_field,
                    },
                    "checks": checks,
                }
                target._readiness_cache = result
                return dict(result)
            except Exception as exc:
                retryable = is_retryable_temporalstore_error(exc)
                attempts.append({"attempt": attempt, "ok": False, "retryable": retryable, "error": str(exc), "checks": checks})
                if not retryable or time.monotonic() >= deadline:
                    return {
                        "status": "topology_not_ready",
                        "backend": target._backend_label(),
                        "reason": reason,
                        "probe": bool(probe),
                        "attempts": attempt,
                        "attempt_log": attempts,
                        "error": str(exc),
                        "topology": {
                            "metaserver": target._metaserver,
                            "namespace": target._namespace,
                            "table": target._table,
                            "storage_prefix": target._storage_prefix,
                            "warmup_key": warmup_key,
                            "warmup_field": warmup_field,
                        },
                        "checks": checks,
                    }
                time.sleep(max(0.05, BACKEND_READINESS_BACKOFF_MS / 1000.0))

