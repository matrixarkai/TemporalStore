#!/usr/bin/env python3
"""MatrixArk MCP storage route and per-record durability policy helpers."""

from __future__ import annotations

from typing import Any

try:
    from tools.matrixark_mcp_errors import MatrixArkError
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_errors import MatrixArkError


Json = dict[str, Any]

STORAGE_ROUTE_PRESETS: dict[str, Json] = {
    "shared_store_async": {
        "storage_family": "shared_store",
        "write_mode": "async",
        "storage_mode": "shared_store",
        "replication_mode": "shared_store",
        "oplog_mode": "async",
        "raft_mode": False,
    },
    "shared_store_sync": {
        "storage_family": "shared_store",
        "write_mode": "sync",
        "storage_mode": "shared_store",
        "replication_mode": "shared_store",
        "oplog_mode": "sync",
        "raft_mode": False,
    },
    "raft_async": {
        "storage_family": "raft",
        "write_mode": "async",
        "storage_mode": "raft",
        "replication_mode": "raft",
        "oplog_mode": "async",
        "raft_mode": True,
    },
    "raft_sync": {
        "storage_family": "raft",
        "write_mode": "sync",
        "storage_mode": "raft",
        "replication_mode": "raft",
        "oplog_mode": "sync",
        "raft_mode": True,
    },
}

DEFAULT_ASYNC_INGEST_STORAGE_OPTIONS: Json = {
    "durability": "async",
    "write_mode": "async",
    "oplog_mode": "async",
    "background_write": True,
    "read_preference": "replica_preferred",
}

KNOWN_RECORD_STORAGE_KEYS = {
    "raw_ingestion",
    "context_event",
    "session_buffer",
    "entity",
    "summary",
    "embedding",
    "index",
    "resource",
    "resource_chunk",
    "skill",
    "compression",
    "feedback",
    "debug",
}
KNOWN_PART_STORAGE_KEYS = KNOWN_RECORD_STORAGE_KEYS


def _optional_object(data: Json, field: str) -> Json:
    value = data.get(field)
    if value is None:
        return {}
    if not isinstance(value, dict):
        raise MatrixArkError(f"{field} must be an object")
    return value


def storage_record_kind(record: Json) -> str:
    explicit = str(record.get("storage_record_kind") or record.get("storage_part") or "").strip().lower().replace("-", "_")
    if explicit:
        return explicit
    record_type = str(record.get("record_type") or "").strip().lower()
    envelope = record.get("envelope") if isinstance(record.get("envelope"), dict) else {}
    kind = str(envelope.get("kind") or record.get("kind") or "").strip().lower()
    if kind == "feedback" or record_type in {"context_feedback", "feedback_event"}:
        return "feedback"
    if record_type in {"raw_ingestion", "raw_ingestion_event", "raw_agent_message"}:
        return "raw_ingestion"
    if record_type in {"session_buffer_event", "session_commit_marker"}:
        return "session_buffer"
    if record_type == "context_event":
        return "context_event"
    if record_type in {"context_entity", "context_entity_state"}:
        return "entity"
    if record_type in {"context_summary", "context_summary_dirty", "context_summary_refresh"}:
        return "summary"
    if record_type == "context_embedding":
        return "embedding"
    if record_type in {"context_index", "context_child_ref", "context_event_ref"}:
        return "index"
    if record_type in {"resource_manifest", "resource_registry_update"}:
        return "resource"
    if record_type in {"resource_chunk", "resource_fact", "extracted_resource_fact_event"}:
        return "resource_chunk"
    if record_type in {"skill_section", "skill_registry_update"}:
        return "skill"
    if record_type in {"context_compression_event", "context_temporal_compression"}:
        return "compression"
    if record_type in {"context_pack_audit", "context_pack_telemetry", "context_debug_record", "context_model_registry"}:
        return "debug"
    return record_type or "context_event"


def storage_part_for_record(record: Json) -> str:
    return storage_record_kind(record)


def canonical_storage_route(storage_options: Json | None) -> Json:
    options = storage_options if isinstance(storage_options, dict) else {}
    storage_mode = str(options.get("storage_mode") or "default")
    replication_mode = str(options.get("replication_mode") or "default")
    oplog_mode = str(options.get("oplog_mode") or "default")
    requested_durability = str(options.get("durability") or "default")
    raft_mode = bool(options.get("raft_mode", False))
    requested_family = str(options.get("storage_family") or options.get("family") or "default")
    requested_write_mode = str(options.get("write_mode") or requested_durability or "default")
    if requested_write_mode == "default" and requested_durability in {"async", "sync"}:
        requested_write_mode = requested_durability
    write_mode = requested_write_mode if requested_write_mode in {"async", "sync"} else oplog_mode
    if write_mode not in {"async", "sync"}:
        write_mode = "async"
    if requested_family == "raft" or storage_mode == "raft" or replication_mode == "raft" or raft_mode:
        route = f"raft_{write_mode}"
        backend_family = "raft"
        storage_mode = "raft" if storage_mode == "default" else storage_mode
        replication_mode = "raft" if replication_mode == "default" else replication_mode
        raft_mode = True
    elif requested_family == "shared_store" or storage_mode == "shared_store" or replication_mode == "shared_store":
        route = f"shared_store_{write_mode}"
        backend_family = "shared_store"
        storage_mode = "shared_store" if storage_mode == "default" else storage_mode
        replication_mode = "shared_store" if replication_mode == "default" else replication_mode
    else:
        route = f"{storage_mode}_{write_mode}" if storage_mode != "default" else "default"
        backend_family = storage_mode
    oplog_mode = write_mode if oplog_mode == "default" else oplog_mode
    background_write = bool(options.get("background_write", write_mode == "async"))
    read_preference = str(options.get("read_preference") or ("replica_preferred" if write_mode == "async" else "primary"))
    return {
        "route": route,
        "route_key": route,
        "backend_family": backend_family,
        "storage_family": backend_family,
        "write_mode": write_mode,
        "storage_mode": storage_mode,
        "replication_mode": replication_mode,
        "oplog_mode": oplog_mode,
        "durability": write_mode,
        "raft_mode": raft_mode,
        "consistency": str(options.get("consistency") or "default"),
        "read_preference": read_preference,
        "replica_read": read_preference in {"replica", "replica_preferred"},
        "sync_write": write_mode == "sync",
        "async_write": write_mode == "async",
        "background_write": background_write,
        "write_ack_policy": "ack_after_durable_commit" if write_mode == "sync" else "ack_after_memory_append",
        "native_backend_decides_route": True,
        "selected_storage_family": backend_family,
        "selected_write_mode": write_mode,
        "durability_result": "durable_before_ack" if write_mode == "sync" else "accepted_for_async_durability",
    }


def normalize_storage_options(args: Json, metadata: Json | None = None) -> Json:
    metadata = metadata if isinstance(metadata, dict) else _optional_object(args, "metadata")
    raw_options = args.get("storage_options")
    options = dict(raw_options) if isinstance(raw_options, dict) else {}
    metadata_options = metadata.get("storage_options") if isinstance(metadata, dict) else None
    if isinstance(metadata_options, dict):
        options = {**metadata_options, **options}
    aliases = {
        "temporalstore_storage_mode": "storage_mode",
        "temporalstore_oplog_mode": "oplog_mode",
        "temporalstore_replication_mode": "replication_mode",
        "temporalstore_raft_mode": "raft_mode",
        "temporalstore_consistency": "consistency",
        "temporalstore_route": "route",
        "temporalstore_storage_family": "storage_family",
        "temporalstore_write_mode": "write_mode",
        "temporalstore_durability": "durability",
        "temporalstore_background_write": "background_write",
        "temporalstore_read_preference": "read_preference",
    }
    for source, target in aliases.items():
        if source in args:
            options[target] = args[source]
        if isinstance(metadata, dict) and source in metadata:
            options.setdefault(target, metadata[source])
    if not options:
        return {}

    allowed = {
        "storage_mode": {"default", "local", "single_node", "multi_node", "shared_store", "raft"},
        "oplog_mode": {"default", "async", "sync"},
        "durability": {"default", "async", "sync"},
        "replication_mode": {"default", "none", "shared_store", "raft"},
        "consistency": {"default", "eventual", "read_your_writes", "linearizable"},
        "read_preference": {"default", "primary", "replica", "replica_preferred"},
        "route": set(STORAGE_ROUTE_PRESETS) | {"default"},
        "storage_family": {"default", "shared_store", "raft"},
        "family": {"default", "shared_store", "raft"},
        "write_mode": {"default", "async", "sync"},
    }
    route_value = options.get("route")
    if route_value is not None:
        if not isinstance(route_value, str):
            raise MatrixArkError("storage_options.route must be a string")
        route_key = route_value.strip().lower().replace("-", "_")
        if route_key == "default":
            options = {**options, "route": route_key}
        elif route_key not in STORAGE_ROUTE_PRESETS:
            raise MatrixArkError(f"storage_options.route must be one of {sorted(STORAGE_ROUTE_PRESETS)}")
        else:
            options = {**STORAGE_ROUTE_PRESETS[route_key], **options, "route": route_key}

    normalized: Json = {}
    for key, value in options.items():
        if key in {"raft_mode", "background_write"}:
            if not isinstance(value, bool):
                raise MatrixArkError(f"storage_options.{key} must be a boolean")
            normalized[key] = value
            continue
        if key not in allowed:
            normalized[key] = value
            continue
        if not isinstance(value, str):
            raise MatrixArkError(f"storage_options.{key} must be a string")
        compact = value.strip().lower().replace("-", "_")
        if compact not in allowed[key]:
            raise MatrixArkError(f"storage_options.{key} must be one of {sorted(allowed[key])}")
        normalized[key] = compact
    storage_family = normalized.get("storage_family") or normalized.get("family")
    explicit_modes = {
        str(value)
        for value in (normalized.get("storage_mode"), normalized.get("replication_mode"), storage_family)
        if value in {"shared_store", "raft"}
    }
    if len(explicit_modes) > 1:
        raise MatrixArkError("storage_options must route to exactly one storage_family; do not mix raft and shared_store in one request")
    if storage_family == "raft":
        normalized.setdefault("replication_mode", "raft")
        normalized.setdefault("storage_mode", "raft")
        normalized["raft_mode"] = True
    elif storage_family == "shared_store":
        normalized.setdefault("replication_mode", "shared_store")
        normalized.setdefault("storage_mode", "shared_store")
        normalized["raft_mode"] = False
    if normalized.get("write_mode") in {"async", "sync"}:
        normalized["oplog_mode"] = normalized["write_mode"]
        normalized["durability"] = normalized["write_mode"]
    elif normalized.get("durability") in {"async", "sync"}:
        normalized["write_mode"] = normalized["durability"]
        normalized["oplog_mode"] = normalized["durability"]
    if normalized.get("oplog_mode") == "sync" and normalized.get("background_write") is True:
        raise MatrixArkError("storage_options.background_write cannot be true when write_mode/oplog_mode is sync")
    if normalized.get("raft_mode") is True:
        normalized.setdefault("replication_mode", "raft")
        normalized.setdefault("storage_mode", "raft")
    route = canonical_storage_route(normalized)
    normalized.update(
        {
            key: value
            for key, value in route.items()
            if key
            in {
                "route",
                "route_key",
                "backend_family",
                "storage_family",
                "write_mode",
                "durability",
                "sync_write",
                "async_write",
                "background_write",
                "write_ack_policy",
                "native_backend_decides_route",
                "selected_storage_family",
                "selected_write_mode",
                "read_preference",
                "replica_read",
                "durability_result",
            }
        }
    )
    normalized["request_level"] = True
    return normalized


def normalize_record_storage_options(args: Json, metadata: Json | None = None, base_options: Json | None = None) -> Json:
    metadata = metadata if isinstance(metadata, dict) else _optional_object(args, "metadata")
    raw_options = args.get("record_storage_options")
    if raw_options is None:
        raw_options = args.get("part_storage_options")
    if raw_options is None and isinstance(metadata, dict):
        raw_options = metadata.get("record_storage_options")
    if raw_options is None and isinstance(metadata, dict):
        raw_options = metadata.get("part_storage_options")
    if raw_options is None:
        return {}
    if not isinstance(raw_options, dict):
        raise MatrixArkError("record_storage_options must be an object keyed by record kind")
    base = base_options if isinstance(base_options, dict) else {}
    normalized_parts: Json = {}
    for raw_record_kind, raw_record_options in raw_options.items():
        record_kind = str(raw_record_kind or "").strip().lower().replace("-", "_")
        if not record_kind:
            raise MatrixArkError("record_storage_options keys must be non-empty")
        if not isinstance(raw_record_options, dict):
            raise MatrixArkError(f"record_storage_options.{record_kind} must be an object")
        route_defining_keys = {
            "route",
            "storage_family",
            "family",
            "storage_mode",
            "replication_mode",
            "raft_mode",
            "write_mode",
            "durability",
            "oplog_mode",
        }
        if route_defining_keys.intersection(raw_record_options):
            merged_options = dict(raw_record_options)
        else:
            merged_options = {**base, **raw_record_options}
        normalized = normalize_storage_options({"storage_options": merged_options})
        if not normalized:
            normalized = normalize_storage_options({"storage_options": DEFAULT_ASYNC_INGEST_STORAGE_OPTIONS})
        normalized["request_level"] = False
        normalized["record_level"] = True
        normalized["part_level"] = True
        normalized["storage_record_kind"] = record_kind
        normalized["storage_part"] = record_kind
        normalized_parts[record_kind] = normalized
    return normalized_parts


def normalize_part_storage_options(args: Json, metadata: Json | None = None, base_options: Json | None = None) -> Json:
    return normalize_record_storage_options(args, metadata, base_options)


def default_async_ingest_storage_options() -> Json:
    normalized = normalize_storage_options({"storage_options": DEFAULT_ASYNC_INGEST_STORAGE_OPTIONS})
    normalized["request_level"] = False
    normalized["default_async_ingest"] = True
    return normalized


def storage_options_for_record(record: Json) -> Json:
    explicit = record.get("storage_options") if isinstance(record.get("storage_options"), dict) else {}
    if explicit:
        normalized = normalize_storage_options({"storage_options": explicit})
        normalized["request_level"] = False
        normalized["record_level"] = True
        record_kind = storage_record_kind(record)
        normalized.setdefault("storage_record_kind", record_kind)
        normalized.setdefault("storage_part", record_kind)
        return normalized
    envelope = record.get("envelope") if isinstance(record.get("envelope"), dict) else {}
    record_kind = storage_record_kind(record)
    record_options = envelope.get("record_storage_options") if isinstance(envelope.get("record_storage_options"), dict) else {}
    if not record_options and isinstance(envelope.get("part_storage_options"), dict):
        record_options = envelope.get("part_storage_options", {})
    if isinstance(record_options, dict):
        selected = record_options.get(record_kind)
        if not selected and record_kind == "resource_chunk":
            selected = record_options.get("resource")
        if isinstance(selected, dict) and selected:
            normalized = normalize_storage_options({"storage_options": selected})
            normalized["request_level"] = False
            normalized["record_level"] = True
            normalized["part_level"] = True
            normalized["storage_record_kind"] = record_kind
            normalized["storage_part"] = record_kind
            return normalized
    inherited = envelope.get("storage_options") if isinstance(envelope.get("storage_options"), dict) else {}
    if inherited:
        normalized = normalize_storage_options({"storage_options": inherited})
        normalized["request_level"] = False
        normalized["inherited_from_ingest"] = True
        normalized["storage_record_kind"] = record_kind
        normalized["storage_part"] = record_kind
        return normalized
    normalized = default_async_ingest_storage_options()
    normalized["storage_record_kind"] = record_kind
    normalized["storage_part"] = record_kind
    return normalized
