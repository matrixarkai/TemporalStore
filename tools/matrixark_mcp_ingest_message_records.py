#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Message hot-path record builders for MatrixArk local ingestion."""

from __future__ import annotations

try:
    from tools.matrixark_mcp_core import (
        Json,
        context_index_name,
        context_index_posting_record,
        compact_embedding_vector,
        embedding_model_name,
        infer_event_type,
        non_default_classification,
        ordered_unique,
        normalize_message_role,
    )
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_core import (
        Json,
        context_index_name,
        context_index_posting_record,
        compact_embedding_vector,
        embedding_model_name,
        infer_event_type,
        non_default_classification,
        ordered_unique,
        normalize_message_role,
    )


def _positive_count_map(value: object, *, normalize_roles: bool = False) -> Json:
    if not isinstance(value, dict):
        return {}
    counts: Json = {}
    for key, count in value.items():
        name = normalize_message_role(key) if normalize_roles else str(key or "").strip()
        if not name:
            continue
        try:
            amount = max(0, int(count or 0))
        except (TypeError, ValueError):
            continue
        if amount:
            counts[name] = int(counts.get(name, 0)) + amount
    return counts


def _source_role_counts(envelope: Json) -> Json:
    metadata = envelope.get("metadata") if isinstance(envelope.get("metadata"), dict) else {}
    metadata_counts = _positive_count_map(metadata.get("source_role_counts"), normalize_roles=True)
    if metadata_counts:
        return metadata_counts
    counts: Json = {}
    messages = envelope.get("messages") if isinstance(envelope.get("messages"), list) else []
    for message in messages:
        if not isinstance(message, dict):
            continue
        role = normalize_message_role(message.get("role"))
        if role:
            counts[role] = int(counts.get(role, 0)) + 1
    return counts


def _source_value_counts(envelope: Json, metadata_count_field: str, metadata_list_field: str, envelope_field: str) -> Json:
    metadata = envelope.get("metadata") if isinstance(envelope.get("metadata"), dict) else {}
    metadata_counts = _positive_count_map(metadata.get(metadata_count_field))
    if metadata_counts:
        return metadata_counts
    values: list[object] = []
    if isinstance(metadata.get(metadata_list_field), list):
        values.extend(metadata[metadata_list_field])
    values.extend([metadata.get(envelope_field), envelope.get(envelope_field)])
    counts: Json = {}
    for value in values:
        name = str(value or "").strip()
        if name:
            counts[name] = int(counts.get(name, 0)) + 1
    return counts


def _source_memory_selection_policy_counts(envelope: Json) -> Json:
    metadata = envelope.get("metadata") if isinstance(envelope.get("metadata"), dict) else {}
    metadata_counts = _positive_count_map(metadata.get("source_memory_selection_policy_counts"))
    if metadata_counts:
        return metadata_counts
    envelope_counts = _positive_count_map(envelope.get("source_memory_selection_policy_counts"))
    if envelope_counts:
        return envelope_counts
    values: list[object] = []
    for source in [metadata, envelope]:
        if isinstance(source.get("source_memory_selection_policies"), list):
            values.extend(source["source_memory_selection_policies"])
        selection = source.get("codex_memory_selection")
        if isinstance(selection, dict):
            values.append(selection.get("policy"))
    counts: Json = {}
    for value in values:
        name = str(value or "").strip()
        if name:
            counts[name] = int(counts.get(name, 0)) + 1
    return counts


def _context_event_source_index_terms(envelope: Json) -> list[str]:
    terms: list[str] = []
    for role in _source_role_counts(envelope):
        terms.append(context_index_name("source_role", role))
    for hook_type in _source_value_counts(
        envelope,
        "source_hook_type_counts",
        "source_hook_types",
        "hook_type",
    ):
        terms.append(context_index_name("hook_type", hook_type))
    for codex_event in _source_value_counts(
        envelope,
        "source_codex_event_counts",
        "source_codex_events",
        "codex_event",
    ):
        terms.append(context_index_name("codex_event", codex_event))
    for policy in _source_memory_selection_policy_counts(envelope):
        terms.append(context_index_name("memory_selection_policy", policy))
    return ordered_unique(terms)


def session_l0_summary_record(
    *,
    summary_hash: int,
    node_hash: int,
    node_path: list[str],
    context_node_key: list[str],
    summary_text: str,
    source_event_hash: int,
    scope: Json,
    updated_at_ms: int,
) -> Json:
    return {
        "record_type": "context_summary",
        "summary_type": "session_l0",
        "summary_hash": summary_hash,
        "summary_identity": "stable_per_session_node",
        "node_hash": node_hash,
        "node_path": node_path,
        "context_node_key": context_node_key,
        "summary_text": summary_text,
        "source_event_hash": source_event_hash,
        "scope": scope,
        "updated_at_ms": updated_at_ms,
    }


def context_embedding_record(
    *,
    embedding_type: str,
    ref_type: str,
    ref_hash: int,
    node_hash: int,
    node_path: list[str],
    vector: list[float],
    scope: Json,
    updated_at_ms: int,
    memory_scope: str = "",
    session_continuity: str = "",
) -> Json:
    record = {
        "record_type": "context_embedding",
        "embedding_type": embedding_type,
        "ref_type": ref_type,
        "ref_hash": ref_hash,
        "node_hash": node_hash,
        "node_path": node_path,
        "dim": len(vector),
        "model": embedding_model_name(),
        "vector": compact_embedding_vector(vector),
        "scope": scope,
        "updated_at_ms": updated_at_ms,
    }
    if memory_scope:
        record["memory_scope"] = memory_scope
    if session_continuity:
        record["session_continuity"] = session_continuity
    return record


def context_event_record(
    *,
    event_id_hash: int,
    node_hash: int,
    node_path: list[str],
    text: str,
    extraction: Json,
    envelope: Json,
    prior_context: Json,
    hook: Json,
) -> Json:
    metadata = envelope.get("metadata") if isinstance(envelope.get("metadata"), dict) else {}
    source_role_counts = _source_role_counts(envelope)
    source_hook_type_counts = _source_value_counts(
        envelope,
        "source_hook_type_counts",
        "source_hook_types",
        "hook_type",
    )
    source_codex_event_counts = _source_value_counts(
        envelope,
        "source_codex_event_counts",
        "source_codex_events",
        "codex_event",
    )
    hook_type = str(hook.get("hook_type") or "").strip() if isinstance(hook, dict) else ""
    if hook_type and hook_type not in source_hook_type_counts:
        source_hook_type_counts[hook_type] = 1
    codex_event = str(hook.get("codex_event") or hook.get("trigger") or "").strip() if isinstance(hook, dict) else ""
    if codex_event and codex_event not in source_codex_event_counts:
        source_codex_event_counts[codex_event] = 1
    extraction_phase = str(metadata.get("extraction_phase") or envelope.get("extraction_phase") or "pending_async").strip()
    return {
        "record_type": "context_event",
        "event_id_hash": event_id_hash,
        "node_hash": node_hash,
        "node_path": node_path,
        "scope": envelope.get("scope", {}),
        "access_scope": envelope.get("scope", {}),
        "text": text,
        "summary_text": text[:512],
        "classification": extraction.get("classification", ""),
        "event_type": extraction.get("event_type", ""),
        "entity_type": extraction.get("entity_type", ""),
        "status": extraction.get("status", "observed"),
        "source_kind": envelope.get("kind", "message"),
        "source_role": next(iter(source_role_counts), ""),
        "source_roles": sorted(source_role_counts),
        "source_role_counts": source_role_counts,
        "source_hook_types": sorted(source_hook_type_counts),
        "source_hook_type_counts": source_hook_type_counts,
        "source_codex_events": sorted(source_codex_event_counts),
        "source_codex_event_counts": source_codex_event_counts,
        "memory_scope": "session",
        "session_continuity": "same_session",
        "extraction_phase": extraction_phase,
        "final_session_boundary": bool(metadata.get("final_session_boundary") or envelope.get("final_session_boundary")),
        "envelope": envelope,
        "internal_extraction": extraction,
        "prior_context": prior_context,
        "agent_hook": hook,
        "storage_options": envelope.get("storage_options", {}),
        "updated_at_ms": envelope.get("ingestion_time_ms", 0),
    }


def context_event_index_terms(*, extraction: Json, text: str, envelope: Json) -> list[str]:
    terms = list(
        extraction.get("indexes")
        or [
            context_index_name("event_type", extraction.get("event_type") or infer_event_type(text)),
            context_index_name("classification", non_default_classification(extraction.get("classification"))),
            context_index_name("status", extraction.get("status") or "observed"),
            context_index_name("source_type", envelope.get("kind", "message")),
            context_index_name("memory_scope", "session"),
            context_index_name("session_continuity", "same_session"),
        ]
    )
    terms.extend(_context_event_source_index_terms(envelope))
    return ordered_unique(terms)


def context_event_index_records(
    *,
    index_terms: list[str],
    event_id_hash: int,
    node_hash: int,
    scope: Json,
    updated_at_ms: int,
    memory_scope: str = "session",
    session_continuity: str = "same_session",
    extraction_phase: str = "pending_async",
) -> list[Json]:
    return [
        {
            **context_index_posting_record(
                index_name=index_name,
                data_model="context_event",
                ref_type="event",
                ref_hashes=[event_id_hash],
                node_hash=node_hash,
                scope=scope,
                updated_at_ms=updated_at_ms,
            ),
            "access_scope": scope,
            "memory_scope": memory_scope,
            "session_continuity": session_continuity,
            "extraction_phase": extraction_phase,
        }
        for index_name in index_terms
    ]
