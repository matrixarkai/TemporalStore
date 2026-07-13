#!/usr/bin/env python3
"""Shared raw-message storage target contract for TemporalStore and MatrixKV.

This module is intentionally small and dependency-free so C++ tooling, Rust
corpus validators, and benchmark adapters can assert the same API semantics:
TemporalStore is the default target, MatrixKV is selectable through the same
storage_target shape, and raw agent-message payloads use a timestamp key plus an event key with
the raw body as the stored value.
"""

from __future__ import annotations

import json
import hashlib
from dataclasses import dataclass, field
from typing import Any

Json = dict[str, Any]
SUPPORTED_BACKENDS = {"temporalstore", "matrixkv", "s3", "objectstore"}
KV_INLINE_BACKENDS = {"temporalstore", "matrixkv"}
DEFAULT_MAX_INLINE_BYTES = 1 * 1024 * 1024
DEFAULT_OBJECT_STORE_PREFIX = "matrixobject://matrixark/raw-agent-messages"
RAW_MESSAGE_DEFAULT_WRITE_POLICY = "ColdStoreOnly"
RAW_MESSAGE_DEFAULT_CACHE_POLICY = "NoCachePromotion"
RAW_MESSAGE_DEFAULT_PROMOTION_POLICY = "NoPromotion"
RAW_MESSAGE_TEMPORALSTORE_METADATA_TABLE = "context_raw_agent_messages"
RAW_MESSAGE_MATRIXKV_METADATA_TABLE = "context_raw_agent_messages"


def normalize_raw_backend(value: Any) -> str:
    backend = str(value or "temporalstore").strip().lower().replace("-", "_")
    if backend in {"", "temporal", "temporal_store", "ts"}:
        return "temporalstore"
    if backend in {"matrix_kv", "kv"}:
        return "matrixkv"
    if backend in {
        "object_store",
        "object",
        "blob",
        "blobstore",
        "blob_store",
        "matrixobject",
        "matrix_object",
        "matrixobjectstore",
        "matrix_object_store",
    }:
        return "objectstore"
    if backend in {"aws_s3", "s3_object", "s3_objectstore"}:
        return "s3"
    if backend not in SUPPORTED_BACKENDS:
        raise ValueError("raw message backend must be temporalstore, matrixkv, s3, or objectstore")
    return backend


@dataclass(frozen=True)
class RawMessageStorageTarget:
    backend: str = "temporalstore"
    namespace: str = ""
    table: str = ""
    key: str = ""
    uri: str = ""
    options: dict[str, str] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(self, "backend", normalize_raw_backend(self.backend))

    @classmethod
    def temporalstore(cls) -> "RawMessageStorageTarget":
        return cls()

    @classmethod
    def matrixkv(cls, namespace: str, table: str, key: str = "") -> "RawMessageStorageTarget":
        return cls(backend="matrixkv", namespace=namespace, table=table, key=key)

    @classmethod
    def s3(cls, uri: str = "", *, bucket: str = "", prefix: str = "") -> "RawMessageStorageTarget":
        options = {}
        if bucket:
            options["bucket"] = bucket
        if prefix:
            options["prefix"] = prefix
        return cls(backend="s3", uri=uri, options=options)

    @classmethod
    def objectstore(cls, uri: str = "") -> "RawMessageStorageTarget":
        return cls(backend="objectstore", uri=uri)

    @classmethod
    def from_dict(cls, value: Json | None) -> "RawMessageStorageTarget":
        if not isinstance(value, dict):
            return cls.temporalstore()
        options = value.get("options") if isinstance(value.get("options"), dict) else {}
        return cls(
            backend=str(value.get("backend") or "temporalstore"),
            namespace=str(value.get("namespace") or ""),
            table=str(value.get("table") or ""),
            key=str(value.get("key") or ""),
            uri=str(value.get("uri") or ""),
            options={str(k): str(v) for k, v in options.items()},
        )

    def resolve(
        self,
        *,
        tenant_hash: int = 0,
        node_hash: int = 0,
        event_time_ms: int = 0,
        event_id_hash: int = 0,
    ) -> "RawMessageStorageTarget":
        if self.backend == "temporalstore":
            return self
        namespace = self.namespace or f"tenant:{tenant_hash}"
        table = self.table or "context_raw_agent_messages"
        key = self.key or f"node:{node_hash}:event:{event_time_ms}:{event_id_hash}"
        uri = self.uri
        options = dict(self.options)
        if self.backend == "s3":
            bucket = options.get("bucket") or "matrixark-raw-agent-messages"
            prefix = (options.get("prefix") or "raw-agent-messages").strip("/")
            uri = uri or f"s3://{bucket}/{prefix}/{event_time_ms:020d}/{event_id_hash:020d}.json"
        elif self.backend == "objectstore":
            base = (uri or DEFAULT_OBJECT_STORE_PREFIX).rstrip("/")
            uri = f"{base}/{event_time_ms:020d}/{event_id_hash:020d}.json"
        return RawMessageStorageTarget(
            backend=self.backend,
            namespace=namespace if self.backend in KV_INLINE_BACKENDS else self.namespace,
            table=table if self.backend in KV_INLINE_BACKENDS else self.table,
            key=key if self.backend in KV_INLINE_BACKENDS else self.key,
            uri=uri,
            options=options,
        )

    def object_key(self) -> str:
        if self.backend == "temporalstore":
            return "temporalstore:context_event"
        if self.backend == "matrixkv":
            return f"matrixkv:{self.namespace}:{self.table}:{self.key}"
        if self.backend in {"s3", "objectstore"}:
            return self.uri
        return self.uri

    def as_dict(self) -> Json:
        return {
            "backend": self.backend,
            "namespace": self.namespace,
            "table": self.table,
            "key": self.key,
            "uri": self.uri,
            "options": dict(self.options),
        }


def raw_message_time_ms(message: Json) -> int:
    for field_name in ("event_time_ms", "updated_at_ms", "timestamp_ms", "time_ms"):
        try:
            value = int(message.get(field_name, 0) or 0)
        except Exception:
            value = 0
        if value > 0:
            return value
    return 1


def raw_message_value(message: Json) -> str:
    for field_name in ("body", "message", "text", "raw_message"):
        value = message.get(field_name)
        if value is not None:
            return str(value)
    return json.dumps(message, sort_keys=True, separators=(",", ":"))


def raw_message_value_bytes(message: Json) -> bytes:
    return raw_message_value(message).encode("utf-8")


def raw_message_payload_sha256(message: Json) -> str:
    return hashlib.sha256(raw_message_value_bytes(message)).hexdigest()


def raw_message_payload_size_bytes(message: Json) -> int:
    return len(raw_message_value_bytes(message))


def raw_message_max_inline_bytes(target: RawMessageStorageTarget | None = None) -> int:
    options = target.options if target is not None else {}
    try:
        value = int(options.get("max_inline_bytes", DEFAULT_MAX_INLINE_BYTES))
    except Exception:
        value = DEFAULT_MAX_INLINE_BYTES
    return max(1, value)


def raw_message_should_spill_to_object_store(
    message: Json,
    target: RawMessageStorageTarget | None = None,
    *,
    max_inline_bytes: int | None = None,
) -> bool:
    limit = raw_message_max_inline_bytes(target) if max_inline_bytes is None else max(1, int(max_inline_bytes))
    selected = target or RawMessageStorageTarget.temporalstore()
    return selected.backend in {"s3", "objectstore"} or raw_message_payload_size_bytes(message) > limit


def raw_message_object_ref_value(marker: Json) -> str:
    return json.dumps({
        "schema": marker["schema"],
        "backend": marker["backend"],
        "metadata_backend": marker["metadata_backend"],
        "object_key": marker["object_key"],
        "payload_size_bytes": marker["payload_size_bytes"],
        "timestamp_key_ms": marker["timestamp_key_ms"],
        "event_key_hash": marker["event_key_hash"],
        "timeline_key": marker["timeline_key"],
        "value_encoding": marker["value_encoding"],
        "payload_sha256": marker["payload_sha256"],
        "object_store_name": marker["object_store_name"],
    }, sort_keys=True, separators=(",", ":"))


def raw_message_timeline_key(timestamp_key_ms: int, event_key_hash: int) -> str:
    return f"{int(timestamp_key_ms):020d}:{int(event_key_hash):020d}"


def raw_message_metadata_backend(target: RawMessageStorageTarget) -> str:
    """Return the backend that owns raw-message metadata rows.

    TemporalStore-backed pipelines keep S3/object-store metadata in
    TemporalStore so backfills can discover raw objects without scanning blob
    storage. MatrixKV mode stores equivalent metadata in MatrixKV through the
    same general API.
    """
    return "matrixkv" if target.backend == "matrixkv" else "temporalstore"


def raw_message_metadata_target(
    target: RawMessageStorageTarget,
    *,
    event_time_ms: int,
    event_id_hash: int,
    tenant_hash: int = 0,
    node_hash: int = 0,
) -> RawMessageStorageTarget:
    metadata_backend = raw_message_metadata_backend(target)
    timeline_key = raw_message_timeline_key(event_time_ms, event_id_hash)
    if metadata_backend == "matrixkv":
        base = target.resolve(
            tenant_hash=tenant_hash,
            node_hash=node_hash,
            event_time_ms=event_time_ms,
            event_id_hash=event_id_hash,
        )
        return RawMessageStorageTarget.matrixkv(
            base.namespace,
            base.table or RAW_MESSAGE_MATRIXKV_METADATA_TABLE,
            base.key or f"raw-agent-message:{timeline_key}",
        )
    return RawMessageStorageTarget(
        backend="temporalstore",
        namespace=target.namespace,
        table=target.table or RAW_MESSAGE_TEMPORALSTORE_METADATA_TABLE,
        key=target.key or f"raw-agent-message:{timeline_key}",
    )


def raw_message_marker(
    message: Json,
    *,
    target: RawMessageStorageTarget,
    event_id_hash: int,
    event_time_ms: int | None = None,
    max_inline_bytes: int | None = None,
) -> Json:
    resolved_time = raw_message_time_ms(message) if event_time_ms is None else int(event_time_ms)
    event_key_hash = int(event_id_hash)
    payload_size = raw_message_payload_size_bytes(message)
    inline_limit = raw_message_max_inline_bytes(target) if max_inline_bytes is None else max(1, int(max_inline_bytes))
    spill = target.backend in {"s3", "objectstore"} or payload_size > inline_limit
    selected = target
    if spill and selected.backend in KV_INLINE_BACKENDS:
        selected = RawMessageStorageTarget.objectstore()
    resolved = selected.resolve(event_time_ms=resolved_time, event_id_hash=event_key_hash)
    metadata_target = raw_message_metadata_target(
        target,
        event_time_ms=resolved_time,
        event_id_hash=event_key_hash,
    )
    return {
        "schema": "matrixark.context.raw_agent_message_ref.v1",
        "raw_schema": "matrixark.context.raw_agent_message.v1",
        "object_store_name": "MatrixObject",
        "backend": resolved.backend,
        "payload_backend": resolved.backend,
        "metadata_backend": metadata_target.backend,
        "metadata_object_key": metadata_target.object_key(),
        "object_key": resolved.object_key(),
        "timestamp_key_ms": resolved_time,
        "event_key_hash": event_key_hash,
        "timeline_key": raw_message_timeline_key(resolved_time, event_key_hash),
        "value_encoding": "object_ref_json" if spill else "raw_body_utf8",
        "payload_size_bytes": payload_size,
        "payload_sha256": raw_message_payload_sha256(message),
        "max_inline_bytes": inline_limit,
        "inline_payload": not spill,
        "spilled_to_object_store": spill,
        "cold_storage": True,
        "write_policy": RAW_MESSAGE_DEFAULT_WRITE_POLICY,
        "cache_policy": RAW_MESSAGE_DEFAULT_CACHE_POLICY,
        "promotion_policy": RAW_MESSAGE_DEFAULT_PROMOTION_POLICY,
    }


def contract_report(
    message: Json,
    target: RawMessageStorageTarget | None = None,
    *,
    event_id_hash: int = 0,
    max_inline_bytes: int | None = None,
) -> Json:
    selected = target or RawMessageStorageTarget.temporalstore()
    timestamp_key_ms = raw_message_time_ms(message)
    event_key_hash = int(event_id_hash or message.get("event_id_hash") or 0)
    marker = raw_message_marker(
        message,
        target=selected,
        event_id_hash=event_key_hash,
        event_time_ms=timestamp_key_ms,
        max_inline_bytes=max_inline_bytes,
    )
    metadata_target = raw_message_metadata_target(
        selected,
        event_time_ms=timestamp_key_ms,
        event_id_hash=event_key_hash,
    )
    if marker["backend"] in {"s3", "objectstore"}:
        target_dict = {
            "backend": marker["backend"],
            "namespace": "",
            "table": "",
            "key": "",
            "uri": marker["object_key"],
            "options": dict(selected.options),
        }
    else:
        target_dict = selected.resolve(event_time_ms=timestamp_key_ms, event_id_hash=event_key_hash).as_dict()
    stored_value = raw_message_value(message) if marker["inline_payload"] else raw_message_object_ref_value(marker)
    return {
        "schema": "matrixark.raw_message_storage_contract.v1",
        "supported_backends": sorted(SUPPORTED_BACKENDS),
        "default_backend": "temporalstore",
        "kv_inline_backends": sorted(KV_INLINE_BACKENDS),
        "target": target_dict,
        "metadata_target": metadata_target.as_dict(),
        "metadata_backend": marker["metadata_backend"],
        "metadata_object_key": marker["metadata_object_key"],
        "metadata_persisted_in_temporalstore": marker["metadata_backend"] == "temporalstore",
        "payload_backend": marker["payload_backend"],
        "timestamp_key_ms": timestamp_key_ms,
        "event_key_hash": event_key_hash,
        "timeline_key": raw_message_timeline_key(timestamp_key_ms, event_key_hash),
        "payload_size_bytes": marker["payload_size_bytes"],
        "payload_sha256": marker["payload_sha256"],
        "max_inline_bytes": marker["max_inline_bytes"],
        "inline_payload": marker["inline_payload"],
        "spilled_to_object_store": marker["spilled_to_object_store"],
        "cold_storage": marker["cold_storage"],
        "write_policy": marker["write_policy"],
        "cache_policy": marker["cache_policy"],
        "promotion_policy": marker["promotion_policy"],
        "object_ref": marker if marker["spilled_to_object_store"] else {},
        "object_store_name": marker["object_store_name"],
        "stored_value": stored_value,
        "stored_value_mode": marker["value_encoding"],
        "uses_timestamp_and_event_key": True,
    }
