# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Compact / context-record materialization helpers.

Split out of matrixark_mcp_core.py, re-exported via `from ...core_compact import *`
at the END of matrixark_mcp_core.py (after the identity re-export, which populates
canonical_scope_key/now_ms/stable_hash on core). Dual relative/absolute imports so
the same core module object is reused under both the package path
(tools.matrixark_mcp_core, 110 importers) and the top-level path — no double
execution, no import-time cycle. __all__ lists every moved name (incl. the private
_record_debug_ref) for total re-export.
"""
import json
from typing import Any

try:  # package path
    from .matrixark_mcp_core import (
        ENABLE_CONTEXT_DEBUG_RECORDS,
        Json,
        SECONDARY_INDEX_POSTING_BUCKET_MS,
        canonical_scope_key,
        canonical_storage_route,
        compact_context_index_postings,
        embedding_model_ref_for_name,
        non_default_classification,
        now_ms,
        stable_hash,
    )
except ImportError:  # top-level path
    from matrixark_mcp_core import (
        ENABLE_CONTEXT_DEBUG_RECORDS,
        Json,
        SECONDARY_INDEX_POSTING_BUCKET_MS,
        canonical_scope_key,
        canonical_storage_route,
        compact_context_index_postings,
        embedding_model_ref_for_name,
        non_default_classification,
        now_ms,
        stable_hash,
    )

__all__ = ['HOT_SERVING_RECORD_TYPES', 'COMPACT_SCOPE_RECORD_TYPES', 'COMPACT_TIMESTAMP_RECORD_TYPES', 'TOPOLOGY_DERIVED_PATH_RECORD_TYPES', 'NODE_PATH_HEAVY_RECORD_TYPES', 'EVENT_DEBUG_FIELDS', 'ENTITY_DEBUG_FIELDS', 'EMBEDDING_LINEAGE_DEBUG_FIELDS', 'HOT_EMBEDDING_COMPACT_TYPES', 'HOT_SESSION_SUMMARY_EMBEDDING_COMPACT_TYPES', 'HOT_EMBEDDING_LINEAGE_FIELDS', 'compact_hot_context_embedding_record', 'legacy_hook_type_from_codex_event', 'CONTEXT_TIMELINE_FANOUT', 'COMPACT_DERIVED_SCOPE_FIELDS', 'COMPACT_TOPOLOGY_SCOPE_STRING_RECORD_TYPES', 'COMPACT_TOPOLOGY_SCOPE_STRING_FIELDS', 'compact_record_scope', '_record_debug_ref', 'context_event_timestamp_ms', 'context_event_time_key', 'attach_context_event_time_key', 'attach_storage_route', 'context_placement_key', 'attach_context_placement', 'compact_record_lifecycle_fields', 'compact_storage_record', 'materialize_serving_records', 'context_index_timestamp_key', 'context_index_posting_bucket', 'context_index_data_model', 'context_index_ref_hashes', 'materialize_serving_record_batch', 'latest_context_state_key', 'compact_latest_context_state_records']

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


def legacy_hook_type_from_codex_event(event: Any) -> str:
    label = str(event or "").strip()
    if not label:
        return ""
    normalized = label.lower()
    if "tool" in normalized or "permissionrequest" in normalized:
        return "tool_result"
    if "previousassistantbackfill" in normalized or normalized.startswith(("stop", "postcompact", "subagentstop")):
        return "after_llm"
    if normalized.startswith(("idletimeout", "sessionidle")):
        return "session_commit"
    if normalized.startswith("userpromptsubmit"):
        return "before_llm"
    return ""


CONTEXT_TIMELINE_FANOUT = 1024 * 1024

# Fields that are useful while debugging a request but are derivable from
# scope_key, event_time_key, node_path, or ContextEmbedding metadata. Keep them
# out of hot serving records unless the caller explicitly asks for debug data.
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


def context_event_timestamp_ms(record: Json) -> int:
    envelope = record.get("envelope") if isinstance(record.get("envelope"), dict) else {}
    for value in (
        envelope.get("ingestion_time_ms") if isinstance(envelope, dict) else None,
        record.get("timestamp_key_ms"),
        record.get("updated_at_ms"),
        record.get("created_at_ms"),
        record.get("event_time_ms"),
    ):
        try:
            timestamp = int(value)
        except (TypeError, ValueError):
            continue
        if timestamp > 0:
            return timestamp
    return now_ms()


def context_event_time_key(timestamp_ms: int, event_id_hash: Any) -> int:
    try:
        event_hash = int(event_id_hash or 0)
    except (TypeError, ValueError):
        event_hash = 0
    disambiguator = stable_hash(f"context_event_time_key:{event_hash}") if event_hash else 0
    return int(timestamp_ms) * CONTEXT_TIMELINE_FANOUT + (disambiguator % CONTEXT_TIMELINE_FANOUT)


def attach_context_event_time_key(record: Json) -> Json:
    if str(record.get("record_type") or "") != "context_event":
        return record
    enriched = dict(record)
    event_hash = enriched.get("event_id_hash") or stable_hash(json.dumps(enriched, sort_keys=True, separators=(",", ":")))
    timestamp_ms = context_event_timestamp_ms(enriched)
    time_key = context_event_time_key(timestamp_ms, event_hash)
    enriched.setdefault("event_id_hash", event_hash)
    enriched.setdefault("timestamp_key_ms", timestamp_ms)
    enriched.setdefault("context_event_key", f"{time_key:020d}:{event_hash}")
    segment_hash = enriched.get("segment_hash")
    if segment_hash:
        enriched.setdefault("context_event_parent_type", "context_segment")
        enriched.setdefault("context_event_parent_hash", segment_hash)
    else:
        enriched.setdefault("context_event_parent_type", "context_node")
        enriched.setdefault("context_event_parent_hash", enriched.get("node_hash") or 0)
    return enriched


def attach_storage_route(record: Json) -> Json:
    route_source = record.get("storage_options") if isinstance(record.get("storage_options"), dict) else {}
    envelope = record.get("envelope") if isinstance(record.get("envelope"), dict) else {}
    if not route_source and isinstance(envelope.get("storage_options"), dict):
        route_source = envelope.get("storage_options", {})
    if "storage_route" not in record or not isinstance(record.get("storage_route"), dict):
        if route_source:
            record = {**record, "storage_route": canonical_storage_route(route_source)}
    return record


def context_placement_key(record: Json, *, scope_key: str = "", node_hash: Any = None) -> str:
    explicit = str(record.get("placement_key") or "")
    if explicit:
        return explicit
    scope_key = scope_key or str(record.get("scope_key") or "")
    if not scope_key:
        scope = record.get("scope") if isinstance(record.get("scope"), dict) else {}
        scope_key = canonical_scope_key(scope) if scope else ""
    if node_hash is None:
        node_hash = record.get("node_hash") or record.get("node_id")
    try:
        node_hash_int = int(node_hash or 0)
    except (TypeError, ValueError):
        node_hash_int = 0
    if scope_key and node_hash_int:
        return f"context:{scope_key}:node={node_hash_int}"
    if scope_key:
        return f"context:{scope_key}"
    try:
        tenant_hash = int(record.get("tenant_hash") or 0)
    except (TypeError, ValueError):
        tenant_hash = 0
    return f"context:t={tenant_hash}" if tenant_hash else ""


def attach_context_placement(record: Json, *, scope_key: str = "", node_hash: Any = None) -> Json:
    placement_key = context_placement_key(record, scope_key=scope_key, node_hash=node_hash)
    if not placement_key:
        return record
    placement_hash = stable_hash(placement_key)
    route = record.get("storage_route") if isinstance(record.get("storage_route"), dict) else {}
    route = dict(route)
    route["placement_key"] = placement_key
    route["placement_hash"] = placement_hash
    route.setdefault("routing_key", placement_key)
    route.setdefault("partition_key", placement_key)
    route.setdefault("colocation_group", "matrixark_context")
    record["placement_key"] = placement_key
    record["placement_hash"] = placement_hash
    record["storage_route"] = route
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
        # event_time_key + parent hash/type is the serving key; the fully
        # expanded string is debug-only noise in hot records.
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
    """Split bulky provider/debug fields from hot serving records.

    Serving records are optimized for retrieval scans and packing. Replay/debug
    rows keep provider payloads, raw extraction details, old entity patches, and
    full path context without forcing every hot read to load them.
    """
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
        source_event_ids = serving.get("source_event_ids")
        if isinstance(source_event_ids, list) and source_event_ids:
            serving.setdefault("source_event_count", len(source_event_ids))
        source_segment_hashes = serving.get("source_segment_hashes")
        if isinstance(source_segment_hashes, list) and source_segment_hashes:
            serving.setdefault("source_segment_count", len(source_segment_hashes))
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

    if not debug_payload or not ENABLE_CONTEXT_DEBUG_RECORDS:
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


def context_index_timestamp_key(record: Json) -> int:
    for field in ("timestamp_key_ms", "updated_at_ms", "created_at_ms", "event_time_ms"):
        try:
            value = int(record.get(field) or 0)
        except (TypeError, ValueError):
            value = 0
        if value > 0:
            return value
    return now_ms()


def context_index_posting_bucket(timestamp_ms: int) -> int:
    bucket_ms = max(1, int(SECONDARY_INDEX_POSTING_BUCKET_MS))
    return int(timestamp_ms) - (int(timestamp_ms) % bucket_ms)


def context_index_data_model(record: Json) -> str:
    explicit = str(record.get("data_model") or "").strip()
    if explicit:
        return explicit
    ref_type = str(record.get("ref_type") or "").strip()
    if ref_type:
        return ref_type
    if record.get("batch_id_hash") is not None:
        return "context_batch_commit"
    if record.get("summary_hash") is not None:
        return "context_summary"
    if record.get("chunk_hash") is not None:
        return "resource_chunk"
    if record.get("skill_hash") is not None or record.get("section_hash") is not None:
        return "skill"
    return "context"


def context_index_ref_hashes(record: Json) -> list[int]:
    values: list[Any] = []
    raw_refs = record.get("ref_hashes")
    if isinstance(raw_refs, list):
        values.extend(raw_refs)
    for field in (
        "ref_hash",
        "event_id_hash",
        "chunk_hash",
        "section_hash",
        "skill_hash",
        "resource_hash",
        "summary_hash",
        "batch_id_hash",
    ):
        if record.get(field) is not None:
            values.append(record.get(field))
    refs: list[int] = []
    seen: set[int] = set()
    for value in values:
        try:
            ref_hash = int(value)
        except (TypeError, ValueError):
            continue
        if ref_hash and ref_hash not in seen:
            seen.add(ref_hash)
            refs.append(ref_hash)
    return refs


def materialize_serving_record_batch(records: list[Json]) -> list[Json]:
    materialized: list[Json] = []
    for record in records:
        materialized.extend(materialize_serving_records(record))
    return compact_context_index_postings(materialized)

def latest_context_state_key(record: Json) -> tuple[Any, ...] | None:
    """Return the logical latest-state key for versionless context records.

    Delegates deliberately: the single definition lives in matrixark_mcp_serving_records. This
    module used to carry its own copy, and the two drifted -- the other one learned to give
    `matrixark_async_pipeline_task` an identity and this one did not. Because the write path
    resolves THIS module (it does `import *`, and this module re-exports the name), tasks kept
    their append-log rows and every status transition accumulated, which is the cost that
    identity exists to remove.
    """
    try:
        from tools.matrixark_mcp_serving_records import (
            latest_context_state_key as _serving_latest_context_state_key,
        )
    except ImportError:  # Direct script execution from tools/.
        from matrixark_mcp_serving_records import (
            latest_context_state_key as _serving_latest_context_state_key,
        )
    return _serving_latest_context_state_key(record)


def compact_latest_context_state_records(records: list[Json]) -> list[Json]:
    """Collapse append-log state into compact serving records.

    The physical log can retain older writes for durability/debug, but serving,
    retrieval, and normal debug tables should see ContextSummary L0/L1 as state
    and ContextIndex as Feature-style timestamped posting rows.
    """
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


