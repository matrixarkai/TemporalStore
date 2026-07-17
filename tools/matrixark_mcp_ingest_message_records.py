#!/usr/bin/env python3
"""Message hot-path record builders for MatrixArk local ingestion."""

from __future__ import annotations

try:
    from tools.matrixark_mcp_core import (
        Json,
        context_index_name,
        embedding_model_name,
        infer_event_type,
        non_default_classification,
        ordered_unique,
    )
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_core import (
        Json,
        context_index_name,
        embedding_model_name,
        infer_event_type,
        non_default_classification,
        ordered_unique,
    )


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
) -> Json:
    return {
        "record_type": "context_embedding",
        "embedding_type": embedding_type,
        "ref_type": ref_type,
        "ref_hash": ref_hash,
        "node_hash": node_hash,
        "node_path": node_path,
        "dim": len(vector),
        "model": embedding_model_name(),
        "vector": vector,
        "scope": scope,
        "updated_at_ms": updated_at_ms,
    }


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
    return {
        "record_type": "context_event",
        "event_id_hash": event_id_hash,
        "node_hash": node_hash,
        "node_path": node_path,
        "text": text,
        "classification": extraction.get("classification", ""),
        "event_type": extraction.get("event_type", ""),
        "entity_type": extraction.get("entity_type", ""),
        "status": extraction.get("status", "observed"),
        "source_kind": envelope.get("kind", "message"),
        "envelope": envelope,
        "internal_extraction": extraction,
        "prior_context": prior_context,
        "agent_hook": hook,
        "storage_options": envelope.get("storage_options", {}),
    }


def context_event_index_terms(*, extraction: Json, text: str, envelope: Json) -> list[str]:
    return ordered_unique(
        extraction.get("indexes")
        or [
            context_index_name("event_type", extraction.get("event_type") or infer_event_type(text)),
            context_index_name("classification", non_default_classification(extraction.get("classification"))),
            context_index_name("status", extraction.get("status") or "observed"),
            context_index_name("source_type", envelope["kind"]),
        ]
    )


def context_event_index_records(
    *,
    index_terms: list[str],
    event_id_hash: int,
    node_hash: int,
    scope: Json,
    updated_at_ms: int,
) -> list[Json]:
    return [
        {
            "record_type": "context_index",
            "index_name": index_name,
            "capability": "context_event",
            "ref_type": "event",
            "ref_hashes": [event_id_hash],
            "node_hash": node_hash,
            "scope": scope,
            "updated_at_ms": updated_at_ms,
        }
        for index_name in index_terms
    ]
