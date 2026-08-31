#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Serving-record compaction and materialization helpers for MatrixArk MCP."""

from __future__ import annotations

import os
import sys
from typing import Any

try:
    from tools.matrixark_mcp_event_keys import (
        attach_context_event_time_key,
        attach_context_placement,
        context_event_time_key,
        context_event_timestamp_ms,
    )
    from tools.matrixark_mcp_identity import canonical_scope_key, now_ms
    from tools.matrixark_mcp_indexing import compact_context_index_postings, non_default_classification
    from tools.matrixark_mcp_model_registry import context_model_registry_records
    from tools.matrixark_mcp_models import embedding_model_ref_for_name
    from tools.matrixark_mcp_storage_options import (
        canonical_storage_route,
        storage_options_for_record,
        storage_record_kind,
    )
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_event_keys import (
        attach_context_event_time_key,
        attach_context_placement,
        context_event_time_key,
        context_event_timestamp_ms,
    )
    from matrixark_mcp_identity import canonical_scope_key, now_ms
    from matrixark_mcp_indexing import compact_context_index_postings, non_default_classification
    from matrixark_mcp_model_registry import context_model_registry_records
    from matrixark_mcp_models import embedding_model_ref_for_name
    from matrixark_mcp_storage_options import (
        canonical_storage_route,
        storage_options_for_record,
        storage_record_kind,
    )


Json = dict[str, Any]

ENABLE_CONTEXT_DEBUG_RECORDS = os.environ.get("MATRIXARK_CONTEXT_DEBUG_RECORDS", "0").strip().lower() in {"1", "true", "yes"}

HOT_SERVING_RECORD_TYPES = {
    "context_event",
    "context_entity",
    "context_segment",
    "resource_chunk",
    "skill_section",
    "context_index",
    "context_embedding",
}
COMPACT_SCOPE_RECORD_TYPES = HOT_SERVING_RECORD_TYPES | {
    "context_node",
    "context_child_ref",
    "context_summary",
    "context_summary_dirty",
    "context_compression_event",
    "context_event_retention_marker",
    "resource_manifest",
    "resource_registry",
    "skill_manifest",
    "skill_registry",
    "skill_registry_update",
}
COMPACT_TIMESTAMP_RECORD_TYPES = COMPACT_SCOPE_RECORD_TYPES | {
    "session_buffer_event",
    "matrixark_async_pipeline_task",
}
TOPOLOGY_DERIVED_PATH_RECORD_TYPES = {"context_child_ref"}
NODE_PATH_HEAVY_RECORD_TYPES = {
    "context_event",
    "context_entity",
    "context_segment",
    "resource_chunk",
    "skill_section",
    "context_index",
}
EVENT_DEBUG_FIELDS = {"envelope", "internal_extraction", "prior_context", "agent_hook", "storage_options"}
ENTITY_DEBUG_FIELDS = {"previous_state", "field_patches", "patch_results"}
EMBEDDING_LINEAGE_DEBUG_FIELDS = {
    "source_event_ids",
    "source_entity_hashes",
    "source_summary_hashes",
    "source_segment_hashes",
    "source_session_ids",
    "supersedes_session_entity_hash",
    "supersedes_session_entity_hashes",
    "previous_profile_revision",
    "previous_profile_updated_at_ms",
    "extraction_context_event_ids",
    "summary_generation_policy",
    "dirty_hash",
}
HOT_EMBEDDING_COMPACT_TYPES = {"event_text", "entity_state", "profile_entity_state", "segment_text"}
HOT_SESSION_SUMMARY_EMBEDDING_COMPACT_TYPES = {"batch_l0"}
HOT_EMBEDDING_LINEAGE_FIELDS = {
    "source_roles",
    "source_role_counts",
    "source_hook_types",
    "source_hook_type_counts",
    "source_codex_events",
    "source_codex_event_counts",
    "source_memory_selection_policies",
    "source_memory_selection_policy_counts",
    "source_memory_selection_lossy_count",
    "source_memory_selection_complete_count",
    "source_memory_selection_dropped_text_chars",
    "source_memory_selection_dropped_line_count",
    "source_memory_selection_retained_text_ratio_avg",
    "source_memory_selection_retained_line_ratio_avg",
    "source_memory_scopes",
    "source_session_continuities",
    "source_extraction_phases",
    "source_profile_promotion_policies",
    "source_profile_promotion_blockers",
    "promoted_from_memory_scope",
    "extraction_phase",
    "final_session_boundary",
}


def compact_hot_context_embedding_record(record: Json) -> Json:
    compacted = dict(record)
    for field in EMBEDDING_LINEAGE_DEBUG_FIELDS:
        compacted.pop(field, None)
    if str(compacted.get("record_type") or "") != "context_embedding":
        return compacted
    for field in HOT_EMBEDDING_LINEAGE_FIELDS:
        compacted.pop(field, None)
    return compacted

COMPACT_DERIVED_SCOPE_FIELDS = {"_explicit_scope_keys"}
COMPACT_TOPOLOGY_SCOPE_STRING_RECORD_TYPES = {
    "context_node",
    "context_child_ref",
    "context_summary",
    "context_summary_dirty",
}
COMPACT_TOPOLOGY_SCOPE_STRING_FIELDS = {
    "account_id",
    "account_hash",
    "tenant_id",
    "tenant_hash",
    "user_id",
    "user_hash",
    "session_id",
    "session_hash",
    "agent_name",
}


def context_debug_records_enabled() -> bool:
    if ENABLE_CONTEXT_DEBUG_RECORDS:
        return True
    for module_name in ("tools.matrixark_mcp_core", "matrixark_mcp_core"):
        module = sys.modules.get(module_name)
        if module is not None and bool(getattr(module, "ENABLE_CONTEXT_DEBUG_RECORDS", False)):
            return True
    return False


def compact_record_scope(record: Json) -> Json:
    record_type = str(record.get("record_type") or "")
    if record_type not in COMPACT_SCOPE_RECORD_TYPES:
        return record
    compacted = dict(record)
    scope = compacted.get("scope") if isinstance(compacted.get("scope"), dict) else {}
    existing_scope_key = str(compacted.get("scope_key") or "")
    scope_key = existing_scope_key or (canonical_scope_key(scope) if scope else "")
    if scope_key:
        compacted["scope_key"] = scope_key
        if record_type == "context_event":
            session_id = scope.get("session_id")
            if session_id is not None and str(session_id):
                compacted["session_id"] = str(session_id)
        compacted.pop("scope", None)
    if str(compacted.get("scope_key") or ""):
        for field in COMPACT_DERIVED_SCOPE_FIELDS:
            compacted.pop(field, None)
        if record_type in COMPACT_TOPOLOGY_SCOPE_STRING_RECORD_TYPES:
            for field in COMPACT_TOPOLOGY_SCOPE_STRING_FIELDS:
                compacted.pop(field, None)
    return compacted


def _record_debug_ref(record: Json) -> tuple[str, Any]:
    record_type = str(record.get("record_type") or "")
    if record_type == "context_event":
        return "event", record.get("event_id_hash")
    if record_type == "context_entity":
        return "entity", record.get("entity_hash")
    if record_type == "context_segment":
        return "segment", record.get("segment_hash")
    if record_type == "resource_chunk":
        return "resource_chunk", record.get("chunk_hash")
    if record_type == "skill_section":
        return "skill_section", record.get("section_hash")
    return record_type, record.get("ref_hash")


def attach_storage_route(record: Json) -> Json:
    route_source = storage_options_for_record(record)
    envelope = record.get("envelope") if isinstance(record.get("envelope"), dict) else {}
    record_kind = storage_record_kind(record)
    if "storage_route" not in record or not isinstance(record.get("storage_route"), dict):
        if route_source:
            record = {
                **record,
                "storage_options": route_source,
                "storage_record_kind": record_kind,
                "storage_part": record_kind,
                "storage_route": canonical_storage_route(route_source),
            }
    elif record_kind and "storage_record_kind" not in record:
        record = {**record, "storage_record_kind": record_kind, "storage_part": record.get("storage_part") or record_kind}
    return record


def compact_record_lifecycle_fields(record: Json) -> Json:
    record_type = str(record.get("record_type") or "")
    if record_type not in COMPACT_TIMESTAMP_RECORD_TYPES:
        return record
    compacted = dict(record)
    if str(compacted.get("scope_key") or ""):
        for field in COMPACT_DERIVED_SCOPE_FIELDS:
            compacted.pop(field, None)
        if record_type in COMPACT_TOPOLOGY_SCOPE_STRING_RECORD_TYPES:
            for field in COMPACT_TOPOLOGY_SCOPE_STRING_FIELDS:
                compacted.pop(field, None)
    if record_type == "context_event":
        compacted.pop("context_event_key", None)
    if record_type == "context_embedding":
        model_name = str(compacted.get("model") or "")
        if model_name:
            compacted.setdefault("model_ref", embedding_model_ref_for_name(model_name))
            compacted.pop("model_hash", None)
    if compacted.get("created_at_ms") is not None and compacted.get("updated_at_ms") is not None:
        try:
            created_at_ms = int(compacted.get("created_at_ms"))
            updated_at_ms = int(compacted.get("updated_at_ms"))
        except (TypeError, ValueError):
            created_at_ms = None
            updated_at_ms = None
        if created_at_ms is not None and created_at_ms == updated_at_ms:
            compacted.pop("created_at_ms", None)
    node_path = compacted.get("node_path")
    if isinstance(node_path, list) and compacted.get("depth") is not None:
        try:
            depth = int(compacted.get("depth"))
        except (TypeError, ValueError):
            depth = None
        if depth == len(node_path):
            compacted.pop("depth", None)
    if record_type == "context_node" and isinstance(node_path, list) and node_path:
        if str(compacted.get("node_name") or "") == str(node_path[-1]):
            compacted.pop("node_name", None)
    if record_type in TOPOLOGY_DERIVED_PATH_RECORD_TYPES:
        compacted.pop("parent_path", None)
        compacted.pop("child_path", None)
        compacted.pop("child_name", None)
        compacted.pop("depth", None)
    return compacted


def compact_storage_record(record: Json) -> Json:
    return compact_record_lifecycle_fields(compact_record_scope(record))


def materialize_serving_records(record: Json) -> list[Json]:
    """Split bulky provider/debug fields from hot serving records."""
    record = compact_storage_record(attach_context_event_time_key(attach_storage_route(record)))
    record_type = str(record.get("record_type") or "")
    if record_type not in HOT_SERVING_RECORD_TYPES:
        return [record]

    serving = dict(record)
    envelope = serving.get("envelope") if isinstance(serving.get("envelope"), dict) else {}
    existing_scope_key = str(serving.get("scope_key") or "")
    scope = serving.get("scope") if isinstance(serving.get("scope"), dict) else envelope.get("scope", {})
    scope_key = canonical_scope_key(scope) if isinstance(scope, dict) and scope else existing_scope_key
    if scope_key:
        serving["scope_key"] = scope_key
    if record_type == "context_event" and isinstance(scope, dict):
        session_id = scope.get("session_id")
        if session_id is not None and str(session_id):
            serving["session_id"] = str(session_id)
    serving.pop("scope", None)

    serving.pop("node_id", None)
    if record_type in NODE_PATH_HEAVY_RECORD_TYPES:
        serving.pop("node_path", None)
    node_hash = serving.get("node_hash") or serving.get("node_id") or 0
    serving = attach_context_placement(serving, scope_key=scope_key, node_hash=node_hash)

    debug_payload: Json = {}
    debug_type = ""
    if record_type == "context_event":
        extraction = serving.get("internal_extraction") if isinstance(serving.get("internal_extraction"), dict) else {}
        classification = non_default_classification(extraction.get("classification", serving.get("classification", "")))
        if classification:
            serving["classification"] = classification
        else:
            serving.pop("classification", None)
        serving["event_type"] = extraction.get("event_type", serving.get("event_type", ""))
        serving["status"] = extraction.get("status", serving.get("status", "observed"))
        serving["source_kind"] = envelope.get("kind", serving.get("source_kind", "message")) if isinstance(envelope, dict) else serving.get("source_kind", "message")
        timestamp_ms = context_event_timestamp_ms(serving)
        event_id_hash = serving.get("event_id_hash")
        serving["timestamp_key_ms"] = timestamp_ms
        serving.setdefault("updated_at_ms", timestamp_ms)
        if event_id_hash is not None:
            event_time_key = context_event_time_key(timestamp_ms, event_id_hash)
            serving["event_time_key"] = f"{timestamp_ms:020d}:{event_id_hash}"
            serving["context_event_key"] = (
                f"context_event:{serving.get('context_event_parent_type', 'context_node')}:"
                f"{serving.get('context_event_parent_hash', serving.get('node_hash') or 0)}:"
                f"{event_time_key:020d}:{event_id_hash}"
            )
        debug_payload = {field: record[field] for field in EVENT_DEBUG_FIELDS if field in record and record[field] not in (None, "", [], {})}
        debug_type = "event_extraction_detail"
        for field in EVENT_DEBUG_FIELDS:
            serving.pop(field, None)
    elif record_type == "context_entity":
        debug_payload = {field: record[field] for field in ENTITY_DEBUG_FIELDS if field in record and record[field] not in (None, "", [], {})}
        debug_type = "entity_update_detail"
        for field in ENTITY_DEBUG_FIELDS:
            serving.pop(field, None)
    elif record_type == "context_embedding":
        source_session_ids = serving.get("source_session_ids")
        if isinstance(source_session_ids, list) and source_session_ids:
            serving.setdefault("profile_source_session_count", len(source_session_ids))
        source_entity_hashes = serving.get("source_entity_hashes")
        if isinstance(source_entity_hashes, list) and source_entity_hashes:
            serving.setdefault("profile_source_entity_count", len(source_entity_hashes))
        debug_payload = {
            field: record[field]
            for field in EMBEDDING_LINEAGE_DEBUG_FIELDS
            if field in record and record[field] not in (None, "", [], {})
        }
        debug_type = "embedding_lineage_detail"
        serving = compact_hot_context_embedding_record(serving)

    if not debug_payload or not context_debug_records_enabled():
        return [serving]

    ref_type, ref_hash = _record_debug_ref(record)
    debug_record: Json = {
        "record_type": "context_debug_record",
        "debug_type": debug_type,
        "ref_type": ref_type,
        "ref_hash": ref_hash,
        "node_hash": record.get("node_hash"),
        "node_path": record.get("node_path", []),
        "scope_key": scope_key,
        "debug_payload": debug_payload,
        "updated_at_ms": record.get("updated_at_ms") or (envelope.get("ingestion_time_ms") if isinstance(envelope, dict) else now_ms()),
    }
    debug_record = attach_context_placement(debug_record, scope_key=scope_key, node_hash=record.get("node_hash"))
    return [debug_record, serving]


def materialize_serving_record_batch(records: list[Json]) -> list[Json]:
    materialized: list[Json] = []
    materialized.extend(context_model_registry_records(records))
    for record in records:
        materialized.extend(materialize_serving_records(record))
    return compact_context_index_postings(materialized)


def latest_context_state_key(record: Json) -> tuple[Any, ...] | None:
    """Return the logical latest-state key for versionless context records."""
    record_type = str(record.get("record_type") or "")
    if record_type == "context_summary":
        summary_type = str(record.get("summary_type") or "")
        summary_hash = record.get("summary_hash") or record.get("node_hash")
        if summary_type and summary_hash is not None:
            return ("context_summary", summary_type, summary_hash)
    # `matrixark_async_pipeline_task` deliberately has NO latest-state identity, though it is
    # the obvious candidate: the drain folds tasks by task_hash, so collapsing each to one row
    # looks free. It is not. The latest-state hash is read WHOLESALE by
    # `_load_latest_context_state_records()` on every idle-commit check and folded by other
    # readers besides, so it is only cheap while its identity count stays small. Tasks are per
    # event, so their count grows with the corpus. Measured both ways on a 600-add store: giving
    # them an identity cut the per-call task count 545.8 -> 310.8 and made an add 143.2 -> 265.6
    # ms, against two control arms 7% apart. The append log is the right home for a record type
    # with unbounded distinct identities.
    if record_type == "context_model_registry":
        model_kind = str(record.get("model_kind") or "embedding")
        model_ref = str(record.get("model_ref") or "")
        model_hash = record.get("model_hash")
        if model_ref or model_hash is not None:
            return ("context_model_registry", model_kind, model_ref or model_hash)
    if record_type == "context_embedding":
        embedding_type = str(record.get("embedding_type") or "")
        ref_type = str(record.get("ref_type") or "")
        if embedding_type in {"session_l0", "node_l0", "node_l1", "resource_l0", "skill_l0", "skill_summary"} and ref_type in {"summary", "node"}:
            ref_hash = record.get("ref_hash") or record.get("node_hash")
            if ref_hash is not None:
                return ("context_embedding", embedding_type, ref_type, ref_hash)
    return None


def compact_latest_context_state_records(records: list[Json]) -> list[Json]:
    """Collapse append-log state into compact serving records."""
    records = compact_context_index_postings(records)
    latest: dict[tuple[Any, ...], tuple[int, Json]] = {}
    passthrough: list[tuple[int, Json]] = []
    for index, record in enumerate(records):
        key = latest_context_state_key(record)
        if key is None:
            passthrough.append((index, record))
            continue
        compacted = dict(record)
        compacted.pop("summary_version_hash", None)
        latest[key] = (index, compacted)
    combined = passthrough + list(latest.values())
    combined.sort(key=lambda item: item[0])
    return [record for _index, record in combined]
