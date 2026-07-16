#!/usr/bin/env python3
"""Batch-extract request planning helpers for MatrixArk adapters."""

from __future__ import annotations

try:
    from tools.matrixark_mcp_core import Json, MatrixArkError, normalize_envelope, validate_hook
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_core import Json, MatrixArkError, normalize_envelope, validate_hook


def prepare_batch_extract_start(args: Json, *, hook: Json | None) -> Json:
    envelope = normalize_envelope(args, default_kind="message")
    hook = validate_hook(hook)
    threshold = args.get("threshold_messages", 20)
    force = bool(args.get("force", False))
    derive_from_existing_events = bool(args.get("derive_from_existing_events", False))
    source_event_ids = (
        [int(ref) for ref in args.get("source_event_ids", [])]
        if isinstance(args.get("source_event_ids", []), list)
        else []
    )
    if not isinstance(threshold, int) or threshold <= 0:
        raise MatrixArkError("threshold_messages must be a positive integer")
    deferred_result: Json | None = None
    if len(envelope["messages"]) < threshold and not force:
        deferred_result = {
            "status": "deferred",
            "message_count": len(envelope["messages"]),
            "threshold_messages": threshold,
            "reason": "logical batch below extraction threshold",
        }
    return {
        "envelope": envelope,
        "hook": hook,
        "threshold": threshold,
        "force": force,
        "derive_from_existing_events": derive_from_existing_events,
        "source_event_ids": source_event_ids,
        "deferred_result": deferred_result,
    }
