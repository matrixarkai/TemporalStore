#!/usr/bin/env python3
"""Ingest planning helpers for MatrixArk adapters."""

from __future__ import annotations

from typing import Any

try:
    from tools.matrixark_mcp_core import Json, MatrixArkError, normalize_envelope, validate_hook
    from tools import matrixark_mcp_local_ingest as local_ingest_helpers
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_core import Json, MatrixArkError, normalize_envelope, validate_hook
    import matrixark_mcp_local_ingest as local_ingest_helpers


def prepare_ingest_start(
    target: Any,
    args: Json,
    *,
    hook: Json | None,
    default_idle_commit_timeout_ms: int,
) -> Json:
    envelope = normalize_envelope(args, default_kind="message")
    hook = validate_hook(hook)
    backend_readiness: Json | None = None
    if envelope["kind"] in {"resource", "skill"}:
        backend_readiness = target.ensure_backend_ready(reason=f"{envelope['kind']}_ingest")

    idle_commit_result: Json | None = None
    idle_commit_timeout_ms = args.get("idle_commit_timeout_ms", default_idle_commit_timeout_ms)
    if idle_commit_timeout_ms is not None:
        if not isinstance(idle_commit_timeout_ms, int) or idle_commit_timeout_ms < 0:
            raise MatrixArkError("idle_commit_timeout_ms must be a non-negative integer")
    if (
        isinstance(idle_commit_timeout_ms, int)
        and idle_commit_timeout_ms > 0
        and target.auto_batch_extract_enabled(args, kind=envelope["kind"])
    ):
        idle_commit_result = target.session_commit(
            {
                "scope": envelope["scope"],
                "metadata": envelope["metadata"],
                "threshold_messages": args.get("session_buffer_threshold", 20),
                "force": False,
                "idle_timeout_ms": idle_commit_timeout_ms,
                "commit_reason": "idle_timeout",
                "skip_prior_context": bool(args.get("skip_prior_context", False)),
                "storage_options": envelope.get("storage_options", {}),
            },
            hook=hook,
        )

    lightweight_result = local_ingest_helpers.lightweight_async_accept(
        target,
        args,
        envelope=envelope,
        hook=hook,
        idle_commit_result=idle_commit_result,
    )
    return {
        "envelope": envelope,
        "hook": hook,
        "backend_readiness": backend_readiness,
        "idle_commit_result": idle_commit_result,
        "lightweight_result": lightweight_result,
    }
