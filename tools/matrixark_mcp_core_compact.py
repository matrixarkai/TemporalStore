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

# Imported from the modules that DEFINE these, not from matrixark_mcp_core, which only
# republishes them. core star-imports this module, so taking them from there closed a cycle:
# importing either module by its flat name failed part-way with "cannot import name
# 'canonical_scope_key' from partially initialized module", and four suites could not load.
#
# Each name was compared against core's view before being moved: eight resolve to the same
# definition either way. `canonical_storage_route` does NOT -- core's honours `durability` and
# matrixark_mcp_storage_options' returns a `read_preference` core's does not, with neither a
# superset -- so that one still comes from core, at its single call site below, and the choice of
# implementation is unchanged.
Json = dict[str, Any]

try:  # package path
    from .matrixark_mcp_identity import canonical_scope_key, now_ms, stable_hash
    from .matrixark_mcp_indexing import (
        SECONDARY_INDEX_POSTING_BUCKET_MS,
        compact_context_index_postings,
        non_default_classification,
    )
    from .matrixark_mcp_models import embedding_model_ref_for_name
    from .matrixark_mcp_runtime_config import ENABLE_CONTEXT_DEBUG_RECORDS
except ImportError:  # top-level path
    from matrixark_mcp_identity import canonical_scope_key, now_ms, stable_hash
    from matrixark_mcp_indexing import (
        SECONDARY_INDEX_POSTING_BUCKET_MS,
        compact_context_index_postings,
        non_default_classification,
    )
    from matrixark_mcp_models import embedding_model_ref_for_name
    from matrixark_mcp_runtime_config import ENABLE_CONTEXT_DEBUG_RECORDS

__all__ = ['HOT_SERVING_RECORD_TYPES', 'COMPACT_SCOPE_RECORD_TYPES', 'COMPACT_TIMESTAMP_RECORD_TYPES', 'TOPOLOGY_DERIVED_PATH_RECORD_TYPES', 'NODE_PATH_HEAVY_RECORD_TYPES', 'EVENT_DEBUG_FIELDS', 'ENTITY_DEBUG_FIELDS', 'EMBEDDING_LINEAGE_DEBUG_FIELDS', 'HOT_EMBEDDING_COMPACT_TYPES', 'HOT_SESSION_SUMMARY_EMBEDDING_COMPACT_TYPES', 'HOT_EMBEDDING_LINEAGE_FIELDS', 'compact_hot_context_embedding_record', 'legacy_hook_type_from_codex_event', 'CONTEXT_TIMELINE_FANOUT', 'COMPACT_DERIVED_SCOPE_FIELDS', 'COMPACT_TOPOLOGY_SCOPE_STRING_RECORD_TYPES', 'COMPACT_TOPOLOGY_SCOPE_STRING_FIELDS', 'compact_record_scope', '_record_debug_ref', 'context_event_timestamp_ms', 'context_event_time_key', 'attach_context_event_time_key', 'attach_storage_route', 'context_placement_key', 'attach_context_placement', 'compact_record_lifecycle_fields', 'compact_storage_record', 'materialize_serving_records', 'context_index_timestamp_key', 'context_index_posting_bucket', 'context_index_data_model', 'context_index_ref_hashes', 'materialize_serving_record_batch', 'latest_context_state_key', 'compact_latest_context_state_records']

# These record-shape constants live in matrixark_mcp_serving_records, and did so here as a second
# copy: which record types are hot, which fields are debug-only, which carry a heavy node path,
# which topology scope fields are strings. The two agreed -- which is what a pair does until one of
# them is extended, and three constants elsewhere in this tree disagreed exactly that way, on the
# copy the live path used.
#
# serving_records owns them because the dependency already runs that way: it imports nothing from
# here, and `compact_hot_context_embedding_record` has been taken from it for some time.
#
# Imported HERE rather than beside that function further down, because module-scope code in between
# reads these names -- COMPACT_SCOPE_RECORD_TYPES is built from HOT_SERVING_RECORD_TYPES two lines
# below. Appending to the lower block broke this module with a NameError at import.
try:
    from .matrixark_mcp_serving_records import (  # noqa: F401
        COMPACT_TOPOLOGY_SCOPE_STRING_FIELDS,
        COMPACT_TOPOLOGY_SCOPE_STRING_RECORD_TYPES,
        EMBEDDING_LINEAGE_DEBUG_FIELDS,
        ENTITY_DEBUG_FIELDS,
        EVENT_DEBUG_FIELDS,
        HOT_EMBEDDING_COMPACT_TYPES,
        HOT_EMBEDDING_LINEAGE_FIELDS,
        HOT_SERVING_RECORD_TYPES,
        NODE_PATH_HEAVY_RECORD_TYPES,
    )
except ImportError:  # Direct script execution from tools/.
    from matrixark_mcp_serving_records import (  # noqa: F401
        COMPACT_TOPOLOGY_SCOPE_STRING_FIELDS,
        COMPACT_TOPOLOGY_SCOPE_STRING_RECORD_TYPES,
        EMBEDDING_LINEAGE_DEBUG_FIELDS,
        ENTITY_DEBUG_FIELDS,
        EVENT_DEBUG_FIELDS,
        HOT_EMBEDDING_COMPACT_TYPES,
        HOT_EMBEDDING_LINEAGE_FIELDS,
        HOT_SERVING_RECORD_TYPES,
        NODE_PATH_HEAVY_RECORD_TYPES,
    )


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
HOT_SESSION_SUMMARY_EMBEDDING_COMPACT_TYPES = {"batch_l0"}


try:  # the implementation lives in matrixark_mcp_serving_records; this module re-exports it
    from .matrixark_mcp_serving_records import compact_hot_context_embedding_record
except ImportError:  # Direct script execution from tools/.
    from matrixark_mcp_serving_records import compact_hot_context_embedding_record


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


try:  # the implementation lives in matrixark_mcp_serving_records; this module re-exports it
    from .matrixark_mcp_serving_records import _record_debug_ref
except ImportError:  # Direct script execution from tools/.
    from matrixark_mcp_serving_records import _record_debug_ref


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


# Not defined here: the implementation lives in matrixark_mcp_event_keys and this module carried an
# identical second copy of each. Every caller importing these names from here is
# unaffected -- it is the same code, and the free names each body reads are bound the
# same way in both modules, which is what makes re-exporting a no-op rather than a
# swap.
try:
    from tools.matrixark_mcp_event_keys import (
        attach_context_event_time_key,
        attach_context_placement,
    )
except ImportError:  # Direct script execution from tools/.
    from matrixark_mcp_event_keys import (
        attach_context_event_time_key,
        attach_context_placement,
    )


def attach_storage_route(record: Json) -> Json:
    route_source = record.get("storage_options") if isinstance(record.get("storage_options"), dict) else {}
    envelope = record.get("envelope") if isinstance(record.get("envelope"), dict) else {}
    if not route_source and isinstance(envelope.get("storage_options"), dict):
        route_source = envelope.get("storage_options", {})
    if "storage_route" not in record or not isinstance(record.get("storage_route"), dict):
        if route_source:
            # From core deliberately: see the note on the imports above. The copy in
            # matrixark_mcp_storage_options answers differently and choosing between them is not
            # this change's to make, so this keeps the one that has always run here.
            try:  # package path
                from .matrixark_mcp_core import canonical_storage_route
            except ImportError:  # top-level path
                from matrixark_mcp_core import canonical_storage_route
            record = {**record, "storage_route": canonical_storage_route(route_source)}
    return record


try:  # the implementation lives in matrixark_mcp_event_keys; this module re-exports it
    from .matrixark_mcp_event_keys import context_placement_key
except ImportError:  # Direct script execution from tools/.
    from matrixark_mcp_event_keys import context_placement_key


try:  # the implementation lives in matrixark_mcp_serving_records; this module re-exports it
    from tools.matrixark_mcp_serving_records import compact_record_lifecycle_fields
except ImportError:  # Direct script execution from tools/.
    from matrixark_mcp_serving_records import compact_record_lifecycle_fields


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


# Not defined here: the implementation lives in matrixark_mcp_indexing and this module carried an
# identical second copy of each. Every caller importing these names from here is
# unaffected -- it is the same code, and the free names each body reads are bound the
# same way in both modules, which is what makes re-exporting a no-op rather than a
# swap.
try:
    from tools.matrixark_mcp_indexing import (
        context_index_ref_hashes,
    )
except ImportError:  # Direct script execution from tools/.
    from matrixark_mcp_indexing import (
        context_index_ref_hashes,
    )


def materialize_serving_record_batch(records: list[Json]) -> list[Json]:
    materialized: list[Json] = []
    for record in records:
        materialized.extend(materialize_serving_records(record))
    return compact_context_index_postings(materialized)

#: Resolved once. The import used to run on EVERY call, and compaction calls this for every
#: record it holds -- 285,928 times over 200 skill ingests. Resolving it per call made the
#: delegating wrapper 3.3x the cost of the function it delegates to, which was 18% of the
#: dominant compaction stage. Which of the two module paths wins is unchanged; it is just
#: decided once instead of a quarter of a million times.
_SERVING_LATEST_CONTEXT_STATE_KEY = None


def latest_context_state_key(record: Json) -> tuple[Any, ...] | None:
    """Return the logical latest-state key for versionless context records.

    Delegates deliberately: the single definition lives in matrixark_mcp_serving_records. This
    module used to carry its own copy and the two drifted, which is the reason for delegating --
    the write path resolves THIS module (it does `import *`, and this module re-exports the
    name), so a copy that answered differently here was the answer that shipped.

    What the delegate decides is worth reading there rather than inferring from here, because it
    is not the obvious answer: `matrixark_async_pipeline_task` deliberately has NO latest-state
    identity. Collapsing each task to one row looks free and is not -- the latest-state hash is
    read WHOLESALE on every idle-commit check, so it is only cheap while its identity count stays
    small, and tasks are per event. Measured on a 600-add store, giving them an identity cut the
    per-call task count 545.8 -> 310.8 and made an add 143.2 -> 265.6 ms.

    So task rows are NOT collapsed by compaction: every status transition stays in the append
    log, and a reader that wants the latest status folds the rows itself. Do not read this
    delegation as "tasks have an identity now".
    """
    global _SERVING_LATEST_CONTEXT_STATE_KEY
    delegate = _SERVING_LATEST_CONTEXT_STATE_KEY
    if delegate is None:
        try:
            from tools.matrixark_mcp_serving_records import (
                latest_context_state_key as delegate,
            )
        except ImportError:  # Direct script execution from tools/.
            from matrixark_mcp_serving_records import (
                latest_context_state_key as delegate,
            )
        _SERVING_LATEST_CONTEXT_STATE_KEY = delegate
    return delegate(record)


# Not defined here: the implementation lives in matrixark_mcp_serving_records and this module carried an
# identical second copy of each.
try:
    from tools.matrixark_mcp_serving_records import (
        compact_latest_context_state_records,
    )
except ImportError:  # Direct script execution from tools/.
    from matrixark_mcp_serving_records import (
        compact_latest_context_state_records,
    )


