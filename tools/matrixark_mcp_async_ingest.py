#!/usr/bin/env python3
"""Async ingest acceptance helpers for MatrixArk local runtime."""

from __future__ import annotations

from typing import Any

try:
    from tools.matrixark_mcp_core import (
        Json,
        MatrixArkError,
        messages_from_event_record,
        normalize_message_role,
        normalized_node_path,
        session_buffer_key,
        stable_hash,
        summarize_text,
        text_from_messages,
    )
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_core import (
        Json,
        MatrixArkError,
        messages_from_event_record,
        normalize_message_role,
        normalized_node_path,
        session_buffer_key,
        stable_hash,
        summarize_text,
        text_from_messages,
    )


def _idle_commit_schedule(args: Json, envelope: Json, pending_event_count: int, pending_message_count: int) -> Json:
    idle_timeout_ms = args.get("idle_commit_timeout_ms")
    if idle_timeout_ms is None:
        return {}
    if not isinstance(idle_timeout_ms, int) or idle_timeout_ms < 0:
        raise MatrixArkError("idle_commit_timeout_ms must be a non-negative integer")
    deadline_ms = int(envelope.get("ingestion_time_ms") or 0) + idle_timeout_ms
    return {
        "idle_commit_timeout_ms": idle_timeout_ms,
        "idle_commit_deadline_ms": deadline_ms,
        "idle_commit_cutoff_ms": int(envelope.get("ingestion_time_ms") or 0),
        "idle_commit_pending_event_count": pending_event_count,
        "idle_commit_pending_message_count": pending_message_count,
        "idle_commit_due": idle_timeout_ms == 0,
    }


def _source_count_summary(envelope: Json, hook: Json | None) -> Json:
    metadata = envelope.get("metadata") if isinstance(envelope.get("metadata"), dict) else {}
    hook = hook if isinstance(hook, dict) else {}

    def add_count(bucket: Json, key: object, amount: object = 1, *, normalize_role: bool = False) -> None:
        name = normalize_message_role(key) if normalize_role else str(key or "").strip()
        if not name:
            return
        try:
            count = max(0, int(amount or 0))
        except (TypeError, ValueError):
            count = 0
        if count:
            bucket[name] = int(bucket.get(name, 0)) + count

    def add_values(bucket: Json, values: object, *, normalize_role: bool = False) -> None:
        if isinstance(values, list):
            for value in values:
                add_count(bucket, value, normalize_role=normalize_role)
        else:
            add_count(bucket, values, normalize_role=normalize_role)

    role_counts: Json = {}
    metadata_role_counts = metadata.get("source_role_counts") if isinstance(metadata.get("source_role_counts"), dict) else {}
    if metadata_role_counts:
        for role, count in metadata_role_counts.items():
            add_count(role_counts, role, count, normalize_role=True)
    else:
        for message in envelope.get("messages", []) if isinstance(envelope.get("messages"), list) else []:
            if isinstance(message, dict):
                add_count(role_counts, message.get("role"), normalize_role=True)
        add_values(role_counts, metadata.get("source_roles"), normalize_role=True)

    hook_type_counts: Json = {}
    metadata_hook_type_counts = metadata.get("source_hook_type_counts") if isinstance(metadata.get("source_hook_type_counts"), dict) else {}
    if metadata_hook_type_counts:
        for hook_type, count in metadata_hook_type_counts.items():
            add_count(hook_type_counts, hook_type, count)
    else:
        add_values(hook_type_counts, envelope.get("hook_type"))
        add_values(hook_type_counts, metadata.get("hook_type"))
        add_values(hook_type_counts, metadata.get("source_hook_types"))
        add_values(hook_type_counts, hook.get("hook_type"))

    codex_event_counts: Json = {}
    metadata_codex_event_counts = metadata.get("source_codex_event_counts") if isinstance(metadata.get("source_codex_event_counts"), dict) else {}
    if metadata_codex_event_counts:
        for codex_event, count in metadata_codex_event_counts.items():
            add_count(codex_event_counts, codex_event, count)
    else:
        add_values(codex_event_counts, envelope.get("codex_event"))
        add_values(codex_event_counts, metadata.get("codex_event"))
        add_values(codex_event_counts, metadata.get("source_codex_events"))
        add_values(codex_event_counts, hook.get("codex_event"))
        add_values(codex_event_counts, hook.get("trigger"))

    return {
        "source_roles": sorted(role_counts),
        "source_role_counts": dict(sorted(role_counts.items())),
        "source_hook_types": sorted(hook_type_counts),
        "source_hook_type_counts": dict(sorted(hook_type_counts.items())),
        "source_codex_events": sorted(codex_event_counts),
        "source_codex_event_counts": dict(sorted(codex_event_counts.items())),
    }


def lightweight_async_accept(
    target: Any,
    args: Json,
    *,
    envelope: Json,
    hook: Json | None,
    idle_commit_result: Json | None,
) -> Json | None:
    enabled = envelope["kind"] in {"message", "business_data", "feedback"} and (
        bool(args.get("async_processing", False))
        or args.get("wait") is False
        or target.auto_batch_extract_enabled(args, kind=envelope["kind"])
    )
    if not enabled:
        return None

    text = text_from_messages(envelope["messages"])
    event_id_hash = stable_hash(
        f"{envelope['kind']}:{text}:{envelope['scope']}:{envelope['ingestion_time_ms']}"
    )
    node_hint = envelope["metadata"].get("node_path") or target.default_session_node_path(envelope["scope"])
    node_path = normalized_node_path(envelope, node_hint)
    node_hash = stable_hash("/".join(node_path))
    source_counts = _source_count_summary(envelope, hook)
    node_materialization = target.ensure_context_node_path(
        node_path=node_path,
        scope=envelope["scope"],
        updated_at_ms=envelope["ingestion_time_ms"],
    )
    with target.write_batch("message_ingest_sync_accept"):
        summary_dirty_hashes = target.mark_node_summary_dirty(
            node_path=node_path,
            scope=envelope["scope"],
            updated_at_ms=envelope["ingestion_time_ms"],
            source_ref_type="event",
            source_hash_field="source_event_hash",
            source_hash=event_id_hash,
            dirty_reason="new_event",
        )
        target.append(
            {
                "record_type": "context_event",
                "event_id_hash": event_id_hash,
                "node_hash": node_hash,
                "node_path": node_path,
                "text": text,
                "summary_text": summarize_text(text),
                "classification": "PENDING_ASYNC_EXTRACTION",
                "event_type": "pending_async",
                "status": "pending",
                "source_kind": envelope.get("kind", "message"),
                "envelope": envelope,
                "internal_extraction": {
                    "mode": "async_pending",
                    "classification": "PENDING_ASYNC_EXTRACTION",
                    "event_type": "pending_async",
                    "status": "pending",
                },
                "agent_hook": hook,
                "storage_options": envelope.get("storage_options", {}),
                **source_counts,
                "async_processing": True,
                "updated_at_ms": envelope["ingestion_time_ms"],
            }
        )
        if target.session_buffer_enabled(args, kind=envelope["kind"]):
            target.append_session_buffer_event(
                envelope=envelope,
                event_id_hash=event_id_hash,
                node_hash=node_hash,
                node_path=node_path,
                hook=hook,
            )
        target.append(
            {
                "record_type": "matrixark_async_pipeline_task",
                "task_hash": stable_hash(f"async_pipeline:{event_id_hash}"),
                "event_id_hash": event_id_hash,
                "node_hash": node_hash,
                "node_path": node_path,
                "scope": envelope["scope"],
                "status": "pending",
                "stages": ["extraction", "summary", "compression", "embedding"],
                "reason": "sync_accept_async_processing",
                **source_counts,
                "created_at_ms": envelope["ingestion_time_ms"],
                "updated_at_ms": envelope["ingestion_time_ms"],
            }
        )

    session_buffer_enabled = target.session_buffer_enabled(args, kind=envelope["kind"])
    pending_events = target.pending_session_events(envelope["scope"]) if session_buffer_enabled else []
    pending_event_count = len(pending_events)
    pending_message_count = sum(len(messages_from_event_record(record)) for record in pending_events)
    auto_batch_extract = target.auto_batch_extract_enabled(args, kind=envelope["kind"])
    session_boundary_commit = target.session_boundary_commit_requested(args, hook=hook)
    auto_batch_result: Json | None = None
    session_buffer_threshold = args.get("session_buffer_threshold", 20)
    if not isinstance(session_buffer_threshold, int) or session_buffer_threshold <= 0:
        raise MatrixArkError("session_buffer_threshold must be a positive integer")
    threshold_ready = pending_event_count >= session_buffer_threshold or pending_message_count >= session_buffer_threshold
    idle_schedule = (
        _idle_commit_schedule(args, envelope, pending_event_count, pending_message_count)
        if session_buffer_enabled and auto_batch_extract and pending_event_count and not threshold_ready
        else {}
    )
    idle_ready = bool(
        isinstance(idle_commit_result, dict)
        and idle_commit_result.get("status") in {"accepted", "committed"}
        and idle_commit_result.get("trigger_policy") == "idle_timeout"
    )
    if auto_batch_extract and (session_boundary_commit or threshold_ready):
        auto_batch_result = target.session_commit(
            {
                "scope": envelope["scope"],
                "metadata": envelope["metadata"],
                "threshold_messages": session_buffer_threshold,
                "force": session_boundary_commit,
                "max_messages": None if session_boundary_commit else session_buffer_threshold,
                "commit_reason": "hook_boundary" if session_boundary_commit else "threshold",
                "understanding_provider": args.get("understanding_provider"),
                "extraction_provider": args.get("extraction_provider"),
                "segment_provider": args.get("segment_provider"),
                "segment_model": args.get("segment_model"),
                "segment_model_path": args.get("segment_model_path"),
                "segment_max_new_tokens": args.get("segment_max_new_tokens"),
                "segment_provider_fallback": args.get("segment_provider_fallback"),
                "skip_prior_context": bool(args.get("skip_prior_context", False)),
                "storage_options": envelope.get("storage_options", {}),
            },
            hook=hook,
        )
    elif auto_batch_extract and idle_schedule.get("idle_commit_due"):
        auto_batch_result = target.session_commit(
            {
                "scope": envelope["scope"],
                "metadata": envelope["metadata"],
                "threshold_messages": session_buffer_threshold,
                "force": False,
                "idle_timeout_ms": int(idle_schedule.get("idle_commit_timeout_ms") or 0),
                "commit_before_ms": idle_schedule.get("idle_commit_cutoff_ms"),
                "commit_reason": "idle_timeout",
                "understanding_provider": args.get("understanding_provider"),
                "extraction_provider": args.get("extraction_provider"),
                "segment_provider": args.get("segment_provider"),
                "segment_model": args.get("segment_model"),
                "segment_model_path": args.get("segment_model_path"),
                "segment_max_new_tokens": args.get("segment_max_new_tokens"),
                "segment_provider_fallback": args.get("segment_provider_fallback"),
                "skip_prior_context": bool(args.get("skip_prior_context", False)),
                "storage_options": envelope.get("storage_options", {}),
            },
            hook=hook,
        )
    elif auto_batch_extract and idle_ready and isinstance(idle_commit_result, dict):
        auto_batch_result = idle_commit_result

    if idle_schedule and auto_batch_result is None:
        target.append(
            {
                "record_type": "matrixark_async_pipeline_task",
                "task_hash": stable_hash(f"async_pipeline_idle_commit:{event_id_hash}"),
                "event_id_hash": event_id_hash,
                "node_hash": node_hash,
                "node_path": node_path,
                "scope": envelope["scope"],
                "status": "idle_commit_scheduled",
                "stages": ["extraction", "summary", "compression", "embedding"],
                "reason": "session_buffer_idle_deadline",
                "trigger_policy": "idle_timeout",
                "auto_batch_extract": auto_batch_extract,
                "threshold_messages": session_buffer_threshold,
                **idle_schedule,
                **source_counts,
                "source_extraction_phases": ["provisional"],
                "extraction_phase": "provisional",
                "final_session_boundary": False,
                "created_at_ms": envelope["ingestion_time_ms"],
                "updated_at_ms": envelope["ingestion_time_ms"],
            }
        )

    return {
        "status": "accepted",
        "sync_write_mode": "lightweight_event",
        "async_processing": True,
        "async_pipeline_status": "pending",
        "event_id_hash": event_id_hash,
        "node_hash": node_hash,
        "storage_options": envelope.get("storage_options", {}),
        "storage_route": envelope.get("storage_route", {}),
        "hook_captured": hook is not None,
        "extraction_mode": "async_pending",
        "summary_refresh": {
            "status": "dirty_marked",
            "dirty_hashes": summary_dirty_hashes,
            "async_required": True,
        },
        "node_materialization": node_materialization,
        "session_buffer": {
            "enabled": session_buffer_enabled,
            "buffer_key": list(session_buffer_key(envelope)),
            "pending_event_count": pending_event_count,
            "pending_message_count": pending_message_count,
            "threshold_messages": session_buffer_threshold,
            "threshold_ready": threshold_ready,
            "idle_ready": idle_ready,
            **idle_schedule,
            "auto_batch_extract": auto_batch_extract,
            "boundary_commit_requested": session_boundary_commit,
            **source_counts,
        },
        "auto_batch_extract_result": auto_batch_result,
        "idle_commit_result": idle_commit_result,
        "quality_warnings": ["async_processing_pending:extraction,summary,compression,embedding"],
    }
