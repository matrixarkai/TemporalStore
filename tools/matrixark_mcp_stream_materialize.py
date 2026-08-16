#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Background stream-materializer flush.

Proactively drains SCHEDULED idle-commit tasks so a plain (non-finalized) streaming
ingest becomes retrievable on its own after a short debounce, instead of waiting for a
client retrieve to happen to run the flush. It calls the SAME backend-native flush the
retrieve path uses (`pre_retrieval_idle_commit_flush` over the rust-native
`idle_commit_task_records` scan) -- never Python `read_all`, which is intentionally
disabled on the production rust adapter.

Draining is driven by a per-scope registry populated at ingest time (see
`MatrixArkMcpServer.register_stream_materialize_scope`), so each flush is a cheap SCOPED
native scan. A broad, cross-scope scan (`scope={}`) is deliberately avoided: it is a
full-store scan that saturates the shared backend proxy.

The thread lifecycle lives on `MatrixArkMcpServer` (mirroring the summary worker); this
module holds the per-flush logic so the MCP entrypoint stays small.
"""

from __future__ import annotations

from typing import Any

try:
    from tools.matrixark_mcp_core import Json, _mcp_debug_log
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_core import Json, _mcp_debug_log  # type: ignore


def flush_due_scopes(adapter: Any, due_scopes: list[Json]) -> Json:
    """Flush each due scope through the canonical native idle-commit flush.

    `due_scopes` are the scopes whose streaming-ingest debounce has elapsed. Each is drained
    with a SCOPED native scan (the exact path the retrieve endpoint uses). Idempotent: a
    already-resolved task is skipped inside the flush and `session_commit` dedupes by cutoff,
    so re-flushing a quiet scope is a cheap no-op.
    """
    if not due_scopes:
        return {"status": "ok", "due_scope_count": 0, "flushed": 0, "committed_event_count": 0}
    idle_task_records = getattr(adapter, "idle_commit_task_records", None)
    if not callable(idle_task_records):
        return {"status": "unavailable", "reason": "idle_commit_task_records_missing"}
    try:
        from tools.matrixark_mcp_retrieve_request import pre_retrieval_idle_commit_flush
    except ModuleNotFoundError:  # Direct script execution from tools/.
        from matrixark_mcp_retrieve_request import pre_retrieval_idle_commit_flush

    _mcp_debug_log(
        f"matrixark stream materializer draining due_scope_count={len(due_scopes)} "
        "via native idle_commit_task_records (scoped)"
    )
    flushed = 0
    committed_events = 0
    for scope in due_scopes:
        outcome = pre_retrieval_idle_commit_flush(adapter, {}, {}, scope=scope or {})
        flushed += 1
        if isinstance(outcome, dict):
            committed_events += int(outcome.get("committed_event_count") or 0)
    return {
        "status": "ok",
        "due_scope_count": len(due_scopes),
        "flushed": flushed,
        "committed_event_count": committed_events,
    }
