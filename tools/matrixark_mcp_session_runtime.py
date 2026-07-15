#!/usr/bin/env python3
"""Session buffer commit runtime for MatrixArk MCP adapters."""

from __future__ import annotations

try:
    from tools.matrixark_mcp_core import (
        Json,
        MatrixArkError,
        canonical_storage_route,
        message_from_event_record,
        normalize_storage_options,
        now_ms,
        optional_object,
        optional_string,
        stable_hash,
    )
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_core import (
        Json,
        MatrixArkError,
        canonical_storage_route,
        message_from_event_record,
        normalize_storage_options,
        now_ms,
        optional_object,
        optional_string,
        stable_hash,
    )


def session_commit(adapter: object, args: Json, *, hook: Json | None = None) -> Json:
    scope = optional_object(args, "scope")
    threshold = args.get("threshold_messages", 20)
    if not isinstance(threshold, int) or threshold <= 0:
        raise MatrixArkError("threshold_messages must be a positive integer")
    force = bool(args.get("force", True))
    commit_reason = optional_string(args, "commit_reason") or ("manual_api" if force else "threshold")
    idle_timeout_ms = args.get("idle_timeout_ms")
    if idle_timeout_ms is not None and (not isinstance(idle_timeout_ms, int) or idle_timeout_ms < 0):
        raise MatrixArkError("idle_timeout_ms must be a non-negative integer")
    max_messages = args.get("max_messages")
    if max_messages is not None and (not isinstance(max_messages, int) or max_messages <= 0):
        raise MatrixArkError("max_messages must be a positive integer")
    pending_all = adapter.pending_session_events(scope)
    pending_event_count = len(pending_all)
    idle_elapsed_ms = 0
    idle_ready = False
    if pending_all and idle_timeout_ms is not None:
        latest_event_time = max(
            int(record.get("envelope", {}).get("ingestion_time_ms") or record.get("updated_at_ms") or 0)
            for record in pending_all
        )
        idle_elapsed_ms = max(0, now_ms() - latest_event_time)
        idle_ready = idle_elapsed_ms >= idle_timeout_ms
    threshold_ready = pending_event_count >= threshold
    if not force and not threshold_ready and not idle_ready:
        return {
            "status": "deferred",
            "pending_event_count": pending_event_count,
            "threshold_messages": threshold,
            "commit_reason": commit_reason,
            "idle_timeout_ms": idle_timeout_ms,
            "idle_elapsed_ms": idle_elapsed_ms,
            "reason": "session buffer below extraction threshold and idle timeout not reached",
        }
    if max_messages is not None:
        commit_limit = max_messages
    elif force or idle_ready:
        commit_limit = None
    else:
        commit_limit = threshold
    pending = pending_all[:commit_limit] if commit_limit is not None else pending_all
    messages = []
    source_event_ids = []
    for record in pending:
        message = message_from_event_record(record)
        if not message:
            continue
        messages.append(message)
        source_event_ids.append(record["event_id_hash"])
    if not messages:
        return {
            "status": "empty",
            "pending_event_count": pending_event_count,
            "threshold_messages": threshold,
            "commit_reason": commit_reason,
        }
    metadata = optional_object(args, "metadata")
    storage_options = normalize_storage_options(args, metadata)
    if "node_path" not in metadata:
        metadata = {**metadata, "node_path": adapter.default_session_node_path(scope)}
    batch_result = adapter.batch_extract(
        {
            "messages": messages,
            "scope": scope,
            "metadata": metadata,
            "storage_options": storage_options,
            "threshold_messages": threshold,
            "force": True,
            "derive_from_existing_events": True,
            "source_event_ids": source_event_ids,
            "understanding_provider": args.get("understanding_provider"),
            "extraction_provider": args.get("extraction_provider"),
            "segment_provider": args.get("segment_provider"),
            "segment_model": args.get("segment_model"),
            "segment_model_path": args.get("segment_model_path"),
            "segment_max_new_tokens": args.get("segment_max_new_tokens"),
            "segment_provider_fallback": args.get("segment_provider_fallback"),
            "skip_prior_context": bool(args.get("skip_prior_context", False)),
        },
        hook=hook,
    )
    commit_id_hash = stable_hash(f"commit:{scope}:{source_event_ids}:{now_ms()}")
    adapter.append(
        {
            "record_type": "context_batch_commit",
            "commit_id_hash": commit_id_hash,
            "batch_id_hash": batch_result.get("batch_id_hash"),
            "node_hash": batch_result.get("node_hash"),
            "node_path": metadata["node_path"],
            "source_event_ids": source_event_ids,
            "scope": scope,
            "message_count": len(messages),
            "threshold_messages": threshold,
            "commit_reason": commit_reason,
            "trigger_policy": "force" if force else "idle_timeout" if idle_ready else "threshold",
            "pending_event_count_before_commit": pending_event_count,
            "committed_event_count": len(source_event_ids),
            "idle_timeout_ms": idle_timeout_ms,
            "idle_elapsed_ms": idle_elapsed_ms,
            "agent_hook": hook,
            "storage_options": storage_options,
            "storage_route": canonical_storage_route(storage_options),
            "created_at_ms": now_ms(),
        }
    )
    return {
        **batch_result,
        "status": "committed",
        "commit_id_hash": commit_id_hash,
        "storage_options": storage_options,
        "storage_route": canonical_storage_route(storage_options),
        "pending_event_count": pending_event_count,
        "committed_event_count": len(source_event_ids),
        "source_event_ids": source_event_ids,
        "commit_reason": commit_reason,
        "trigger_policy": "force" if force else "idle_timeout" if idle_ready else "threshold",
        "idle_timeout_ms": idle_timeout_ms,
        "idle_elapsed_ms": idle_elapsed_ms,
        "raw_events_duplicated": False,
    }
