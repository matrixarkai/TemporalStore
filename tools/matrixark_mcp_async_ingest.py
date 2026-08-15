#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Async ingest acceptance helpers for MatrixArk local runtime."""

from __future__ import annotations

from typing import Any

try:
    from tools.matrixark_mcp_core import (
        Json,
        MatrixArkError,
        candidate_memory_layer_name,
        context_index_name,
        context_index_posting_record,
        embedding_for_text,
        embedding_model_name,
        feature_scope_excludes_outcome_evidence,
        messages_from_event_record,
        normalize_message_role,
        normalized_node_path,
        ordered_unique,
        profile_entity_type_for_memory_text,
        session_buffer_key,
        stable_hash,
        summarize_text,
        text_from_messages,
    )
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_core import (
        Json,
        MatrixArkError,
        candidate_memory_layer_name,
        context_index_name,
        context_index_posting_record,
        embedding_for_text,
        embedding_model_name,
        feature_scope_excludes_outcome_evidence,
        messages_from_event_record,
        normalize_message_role,
        normalized_node_path,
        ordered_unique,
        profile_entity_type_for_memory_text,
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


def _metadata_string_values(metadata: Json, *fields: str) -> list[str]:
    values: list[str] = []
    for field in fields:
        raw = metadata.get(field)
        if isinstance(raw, list):
            values.extend(str(item or "").strip() for item in raw)
        elif isinstance(raw, dict):
            values.extend(str(key or "").strip() for key, count in raw.items() if count)
        else:
            values.append(str(raw or "").strip())
    return ordered_unique([value for value in values if value])


def _lightweight_memory_policy_lineage(envelope: Json) -> Json:
    metadata = envelope.get("metadata") if isinstance(envelope.get("metadata"), dict) else {}
    selection = metadata.get("codex_memory_selection") if isinstance(metadata.get("codex_memory_selection"), dict) else {}
    messages = envelope.get("messages") if isinstance(envelope.get("messages"), list) else []
    text = text_from_messages(messages)
    roles = ordered_unique(
        normalize_message_role(message.get("role"))
        for message in messages
        if isinstance(message, dict)
    )
    policies = ordered_unique(
        _metadata_string_values(
            metadata,
            "source_memory_selection_policies",
            "source_memory_selection_policy_counts",
            "memory_selection_policy",
        )
        + _metadata_string_values(selection, "policies", "policy_counts", "policy")
    )
    policy_counts = {policy: 1 for policy in policies}
    profile_kinds = ordered_unique(
        _metadata_string_values(metadata, "source_profile_memory_kinds", "profile_memory_kind")
        + _metadata_string_values(selection, "source_profile_memory_kinds", "profile_memory_kind")
    )
    profile_classes = ordered_unique(
        _metadata_string_values(metadata, "source_profile_memory_classes", "profile_memory_class")
        + _metadata_string_values(selection, "source_profile_memory_classes", "profile_memory_class")
    )
    feature_memory_only = bool(feature_scope_excludes_outcome_evidence(text))
    inferred_profile_type = profile_entity_type_for_memory_text(text)
    if not profile_classes and (
        inferred_profile_type == "memory_feature_profile"
        or feature_memory_only
    ):
        profile_classes = ["memory_feature"]
    if not profile_kinds and profile_classes == ["memory_feature"]:
        profile_kinds = ["memory_feature"]
    if (
        not feature_memory_only
        and not profile_classes
        and any(role in {"assistant", "tool"} for role in roles)
    ):
        profile_classes = ["codex_outcome"]
    if (
        not feature_memory_only
        and not profile_kinds
        and profile_classes == ["codex_outcome"]
    ):
        profile_kinds = ["codex_outcome"]
    if not policies and profile_kinds == ["codex_outcome"]:
        policies = (
            ["selected_tool_evidence_only"]
            if "tool" in roles
            else ["selected_assistant_decision_outcome_only"]
        )
        policy_counts = {policy: 1 for policy in policies}
    lineage: Json = {}
    if policies:
        lineage["source_memory_selection_policies"] = policies
        lineage["source_memory_selection_policy_counts"] = policy_counts
    if profile_kinds:
        lineage["source_profile_memory_kinds"] = profile_kinds
        lineage["profile_memory_kind"] = profile_kinds[0]
    if profile_classes:
        lineage["source_profile_memory_classes"] = profile_classes
        lineage["profile_memory_class"] = profile_classes[0]
    return lineage


def _lightweight_source_layer_lineage(memory_layer_fields: Json) -> Json:
    memory_scope = str(memory_layer_fields.get("memory_scope") or "").strip()
    session_continuity = str(memory_layer_fields.get("session_continuity") or "").strip()
    extraction_phase = str(memory_layer_fields.get("extraction_phase") or "").strip()
    lineage: Json = {}
    if memory_scope:
        lineage["source_memory_scopes"] = [memory_scope]
    if session_continuity:
        lineage["source_session_continuities"] = [session_continuity]
    if extraction_phase:
        lineage["source_extraction_phases"] = [extraction_phase]
    lineage["source_final_session_boundary_count"] = (
        1 if bool(memory_layer_fields.get("final_session_boundary")) else 0
    )
    return lineage


def _lightweight_context_event_index_terms(source_counts: Json, memory_layer_fields: Json, envelope: Json) -> list[str]:
    policy_lineage = _lightweight_memory_policy_lineage(envelope)
    memory_layer = candidate_memory_layer_name(
        {
            "record_type": "context_event",
            "ref_type": "event",
            "event_type": "pending_async",
            **memory_layer_fields,
            **policy_lineage,
        }
    )
    terms = [
        context_index_name("event_type", "pending_async"),
        context_index_name("classification", "pending_async_extraction"),
        context_index_name("status", "pending"),
        context_index_name("source_type", envelope.get("kind", "message")),
    ]
    if memory_layer:
        terms.append(context_index_name("memory_layer", memory_layer))
    for field in ["memory_scope", "session_continuity", "extraction_phase"]:
        value = str(memory_layer_fields.get(field) or "").strip()
        if value:
            terms.append(context_index_name(field, value))
    for role in source_counts.get("source_role_counts", {}):
        terms.append(context_index_name("source_role", role))
    for hook_type in source_counts.get("source_hook_type_counts", {}):
        terms.append(context_index_name("hook_type", hook_type))
    for codex_event in source_counts.get("source_codex_event_counts", {}):
        terms.append(context_index_name("codex_event", codex_event))
    for policy in policy_lineage.get("source_memory_selection_policies", []):
        terms.append(context_index_name("memory_selection_policy", policy))
    for profile_kind in policy_lineage.get("source_profile_memory_kinds", []):
        terms.append(context_index_name("profile_memory_kind", profile_kind))
    for profile_class in policy_lineage.get("source_profile_memory_classes", []):
        terms.append(context_index_name("profile_memory_class", profile_class))
    return ordered_unique(terms)


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
    policy_lineage = _lightweight_memory_policy_lineage(envelope)
    memory_layer_fields: Json = {
        "memory_scope": "session",
        "session_continuity": "same_session",
        "extraction_phase": "pending_async",
        "final_session_boundary": False,
    }
    source_layer_lineage = _lightweight_source_layer_lineage(memory_layer_fields)
    memory_layer = candidate_memory_layer_name(
        {
            "record_type": "context_event",
            "ref_type": "event",
            "event_type": "pending_async",
            **memory_layer_fields,
            **policy_lineage,
        }
    )
    event_embedding = embedding_for_text(text)
    event_embedding_record: Json = {
        "record_type": "context_embedding",
        "embedding_type": "event_text",
        "ref_type": "event",
        "ref_hash": event_id_hash,
        "node_hash": node_hash,
        "node_path": node_path,
        "dim": len(event_embedding),
        "model": embedding_model_name(),
        "vector": event_embedding,
        "scope": envelope["scope"],
        "updated_at_ms": envelope["ingestion_time_ms"],
        **memory_layer_fields,
        **source_layer_lineage,
        **policy_lineage,
        **({"memory_layer": memory_layer} if memory_layer else {}),
    }
    event_index_terms = _lightweight_context_event_index_terms(source_counts, memory_layer_fields, envelope)
    event_index_records = [
        {
            **context_index_posting_record(
                index_name=index_name,
                data_model="context_event",
                ref_type="event",
                ref_hashes=[event_id_hash],
                node_hash=node_hash,
                scope=envelope["scope"],
                updated_at_ms=envelope["ingestion_time_ms"],
            ),
            "access_scope": envelope["scope"],
            **memory_layer_fields,
        }
        for index_name in event_index_terms
    ]
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
        target.append(event_embedding_record)
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
                **memory_layer_fields,
                **source_layer_lineage,
                **policy_lineage,
                **({"memory_layer": memory_layer} if memory_layer else {}),
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
        if event_index_records:
            append_many = getattr(target, "append_many", None)
            if callable(append_many):
                append_many(event_index_records)
            else:
                for index_record in event_index_records:
                    target.append(index_record)
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
                **memory_layer_fields,
                **source_layer_lineage,
                **policy_lineage,
                **({"memory_layer": memory_layer} if memory_layer else {}),
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
                **memory_layer_fields,
                **source_layer_lineage,
                **policy_lineage,
                **({"memory_layer": memory_layer} if memory_layer else {}),
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
