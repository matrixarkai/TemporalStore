#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Raw ingestion helper functions for MatrixArk TemporalStore adapters."""

from __future__ import annotations

import json
import os
import time
from typing import Any

try:
    from tools.matrixark_mcp_errors import MatrixArkError
    from tools.matrixark_mcp_identity import stable_hash
    from tools.matrixark_mcp_temporal_append import slim_persisted_record
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_errors import MatrixArkError
    from matrixark_mcp_identity import stable_hash
    from matrixark_mcp_temporal_append import slim_persisted_record


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


def ensure_raw_ingestion_fields(target: Any) -> None:
    if not hasattr(target, "_raw_storage_backend"):
        target._raw_storage_backend = normalize_raw_storage_backend(
            os.environ.get("MATRIXARK_RAW_INGESTION_BACKEND", "temporalstore")
        )
    else:
        target._raw_storage_backend = normalize_raw_storage_backend(target._raw_storage_backend)
    if not hasattr(target, "_raw_ingestion_prefix"):
        storage_prefix = str(getattr(target, "_storage_prefix", "matrixark:mcp")).rstrip(":")
        configured_raw_prefix = os.environ.get("MATRIXARK_DIRECT_RAW_STORAGE_PREFIX", "").strip().rstrip(":")
        target._raw_ingestion_prefix = configured_raw_prefix or f"{storage_prefix}:raw_ingestion"
    if not hasattr(target, "_raw_record_hash_key"):
        target._raw_record_hash_key = f"{target._raw_ingestion_prefix}:records"
    if not hasattr(target, "_raw_count_key"):
        target._raw_count_key = f"{target._raw_ingestion_prefix}:record_count"
    if not hasattr(target, "_raw_entry_count_cache"):
        target._raw_entry_count_cache = None


def raw_record_location(raw_record_hash_key: str, shard_size: int, sequence: int) -> tuple[str, str]:
    shard = sequence // shard_size
    offset = sequence % shard_size
    return f"{raw_record_hash_key}:{shard:06d}", f"{offset:020d}"


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


def normalize_raw_ingestion_record(record: Json) -> Json:
    normalized = dict(record)
    if normalized.get("record_type") == "agent_message":
        normalized.setdefault("raw_record_type", "raw_agent_message")
    else:
        normalized.setdefault("raw_record_type", "raw_ingestion_event")
    normalized.setdefault("raw_ingestion_visibility", "backfill_only")
    normalized.setdefault("serving_visible", False)
    normalized.setdefault("session_binding", "metadata_only_for_backfill_batching")
    return normalized


def get_raw_count(target: Any) -> int:
    ensure_raw_ingestion_fields(target)
    try:
        raw = target._client.get_string(target._raw_count_key)
    except Exception:
        return 0
    if not raw:
        return 0
    try:
        value = int(raw)
    except ValueError:
        return 0
    return max(0, value)


def append_raw_ingestion_records(target: Any, records: list[Json], *, allow_queue: bool = True) -> None:
    if not records:
        return
    records = [normalize_raw_ingestion_record(record) for record in records]
    ensure_raw_ingestion_fields(target)
    if target._raw_ingestion_prefix == target._storage_prefix:
        raise MatrixArkError("MATRIXARK_DIRECT_RAW_STORAGE_PREFIX must differ from the serving storage prefix")
    if (
        allow_queue
        and bool(getattr(target, "_direct_raw_ingestion_queue_enabled", False))
        and bool(getattr(target, "_direct_write_queue_enabled", False))
        and getattr(target, "_direct_write_queue_mode", "memory") == "memory"
    ):
        target._enqueue_direct_write_item({"queue_mode": "raw_ingestion", "records": list(records)}, len(records))
        return
    started_perf = time.perf_counter()
    with target._records_lock:
        count = target._raw_entry_count_cache if target._raw_entry_count_cache is not None else get_raw_count(target)
        sequence = count
        entries: list[Json] = []
        for record in records:
            record_key, record_id = raw_record_location(target._raw_record_hash_key, target._shard_size, sequence)
            payload = json.dumps(slim_persisted_record(record),
                                 sort_keys=True, separators=(",", ":"))
            route = record.get("storage_route") if isinstance(record.get("storage_route"), dict) else {}
            entries.append({"key": record_key, "field": record_id, "value": payload, "storage_route": route})
            entries.extend(
                raw_session_index_entries(
                    raw_ingestion_prefix=target._raw_ingestion_prefix,
                    shard_size=target._shard_size,
                    sequence=sequence,
                    record=record,
                )
            )
            sequence += 1
        append_records = getattr(target._client, "matrixark_batch_append_records", None)
        if callable(append_records):
            target._write_with_backoff(
                lambda: append_records(
                    entries,
                    count_key=target._raw_count_key,
                    count_value=str(sequence),
                    append_options=raw_ingestion_append_options(target._raw_storage_backend),
                ),
                op="matrixark_batch_append_raw_ingestion_records",
            )
        else:
            target._hset_many_with_backoff(entries)
            target._put_string_with_backoff(target._raw_count_key, str(sequence))
        if target._raw_ingestion_visibility_required_after_flush():
            target._note_pending_visibility_keys(
                [target._raw_count_key] + [str(entry.get("key") or "") for entry in entries]
            )
        target._raw_entry_count_cache = sequence
        elapsed_ms = (time.perf_counter() - started_perf) * 1000.0
        target._observe_backend_command(elapsed_ms, records_written=len(records))


class RawIngestionAdapterMixin:
    """Adapter methods for raw ingestion storage and indexing."""

    def _normalize_raw_storage_backend(self, value: Any) -> str:
        return normalize_raw_storage_backend(value)

    def _raw_ingestion_append_path(self) -> str:
        return raw_ingestion_append_path_for_backend(
            getattr(self, "_raw_storage_backend", "temporalstore")
        )

    def _raw_ingestion_append_options(self) -> Json:
        return raw_ingestion_append_options(
            getattr(self, "_raw_storage_backend", "temporalstore")
        )

    def _ensure_raw_ingestion_fields(self) -> None:
        ensure_raw_ingestion_fields(self)

    def _raw_record_location(self, sequence: int) -> tuple[str, str]:
        self._ensure_raw_ingestion_fields()
        return raw_record_location(self._raw_record_hash_key, self._shard_size, sequence)

    def _raw_session_index_key(self, session_id: str) -> str:
        self._ensure_raw_ingestion_fields()
        return raw_session_index_key(self._raw_ingestion_prefix, session_id)

    def _raw_record_scope_value(self, record: Json, name: str) -> str:
        return raw_record_scope_value(record, name)

    def _raw_record_session_ids(self, record: Json) -> set[str]:
        return raw_record_session_ids(record)

    def _raw_session_index_entries(self, *, sequence: int, record: Json) -> list[Json]:
        self._ensure_raw_ingestion_fields()
        return raw_session_index_entries(
            raw_ingestion_prefix=self._raw_ingestion_prefix,
            shard_size=self._shard_size,
            sequence=sequence,
            record=record,
        )

    def _get_raw_count(self) -> int:
        return get_raw_count(self)

    def _append_raw_ingestion_records(self, records: list[Json], *, allow_queue: bool = True) -> None:
        append_raw_ingestion_records(self, records, allow_queue=allow_queue)
