#!/usr/bin/env python3
"""Raw ingestion helper functions for MatrixArk TemporalStore adapters."""

from __future__ import annotations

import json
from typing import Any

try:
    from tools.matrixark_mcp_errors import MatrixArkError
    from tools.matrixark_mcp_identity import stable_hash
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_errors import MatrixArkError
    from matrixark_mcp_identity import stable_hash


Json = dict[str, Any]


def normalize_raw_storage_backend(value: Any) -> str:
    backend = str(value or "temporalstore").strip().lower().replace("-", "_")
    if backend in {"", "temporal", "temporal_store", "ts"}:
        backend = "temporalstore"
    if backend in {"matrix_kv", "kv"}:
        backend = "matrixkv"
    if backend in {
        "matrix_object",
        "matrixobjectstore",
        "matrix_object_store",
        "objectstore",
        "object_store",
        "object",
        "blob",
        "blobstore",
        "blob_store",
    }:
        backend = "matrixobject"
    if backend in {"aws_s3", "s3_object", "s3_objectstore"}:
        backend = "s3"
    if backend not in {"temporalstore", "matrixkv", "s3", "matrixobject"}:
        raise MatrixArkError(
            "MATRIXARK_RAW_INGESTION_BACKEND must be temporalstore, matrixkv, s3, or matrixobject"
        )
    return backend


def raw_ingestion_append_path_for_backend(backend: Any) -> str:
    normalized = normalize_raw_storage_backend(backend)
    if normalized == "temporalstore":
        return "matrixark_raw_ingestion_temporalstore_log"
    if normalized == "matrixkv":
        return "matrixark_raw_ingestion_matrixkv_log"
    if normalized == "s3":
        return "matrixark_raw_ingestion_s3_object_ref"
    return "matrixark_raw_ingestion_matrixobject_ref"


def raw_ingestion_append_options(backend: Any) -> Json:
    normalized = normalize_raw_storage_backend(backend)
    return {
        "append_path": raw_ingestion_append_path_for_backend(normalized),
        "raw_storage_backend": normalized,
        "raw_message_store": normalized,
        "coalesce_writes": True,
        "route_by": "raw_ingestion_prefix",
        "persist_from_storage_options": True,
        "hset_lowering": "forbidden_for_parity",
        "count_update": "same_batch",
        "source": "matrixark_live_ingestion_dual_write",
    }


def raw_session_index_key(raw_ingestion_prefix: str, session_id: str) -> str:
    return f"{raw_ingestion_prefix}:session_index:{stable_hash(str(session_id))}"


def raw_record_scope_value(record: Json, name: str) -> str:
    scope = record.get("scope") if isinstance(record.get("scope"), dict) else {}
    envelope = record.get("envelope") if isinstance(record.get("envelope"), dict) else {}
    envelope_scope = envelope.get("scope") if isinstance(envelope.get("scope"), dict) else {}
    for key in (name, name.replace("_id", "")):
        for container in (record, scope, envelope_scope, envelope):
            if not isinstance(container, dict):
                continue
            value = container.get(key)
            if value not in (None, ""):
                return str(value)
    return ""


def raw_record_session_ids(record: Json) -> set[str]:
    candidates = {
        raw_record_scope_value(record, "session_id"),
        raw_record_scope_value(record, "conversation_id"),
    }
    scope = record.get("scope") if isinstance(record.get("scope"), dict) else {}
    envelope = record.get("envelope") if isinstance(record.get("envelope"), dict) else {}
    envelope_scope = envelope.get("scope") if isinstance(envelope.get("scope"), dict) else {}
    metadata = record.get("metadata") if isinstance(record.get("metadata"), dict) else {}
    for container in (scope, envelope_scope, metadata, envelope, record):
        if not isinstance(container, dict):
            continue
        for key in ("session_id", "session", "conversation_id", "conversation"):
            value = container.get(key)
            if value not in (None, ""):
                candidates.add(str(value))
    return {item for item in candidates if item}


def raw_session_index_entries(
    *,
    raw_ingestion_prefix: str,
    shard_size: int,
    sequence: int,
    record: Json,
) -> list[Json]:
    shard = sequence // shard_size
    offset = sequence % shard_size
    ref = json.dumps(
        {
            "sequence": sequence,
            "shard": shard,
            "field": f"{offset:020d}",
        },
        sort_keys=True,
        separators=(",", ":"),
    )
    return [
        {
            "key": raw_session_index_key(raw_ingestion_prefix, session_id),
            "field": f"{sequence:020d}",
            "value": ref,
        }
        for session_id in sorted(raw_record_session_ids(record))
    ]
