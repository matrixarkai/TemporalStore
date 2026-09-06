#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Context event key and placement helpers for MatrixArk MCP records."""

from __future__ import annotations

try:
    from tools.matrixark_mcp_env import env_bool
except ImportError:  # Direct script execution from tools/.
    from matrixark_mcp_env import env_bool


import json
import os
from typing import Any

try:
    from tools.matrixark_mcp_identity import canonical_scope_key, now_ms, stable_hash
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_identity import canonical_scope_key, now_ms, stable_hash


Json = dict[str, Any]

CONTEXT_TIMELINE_FANOUT = 1024 * 1024


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


def context_event_time_index_key(storage_prefix: str, record: Json) -> str:
    enriched = attach_context_event_time_key(record)
    parent_type = str(enriched.get("context_event_parent_type") or "context_node")
    parent_hash = enriched.get("context_event_parent_hash") or 0
    return f"{storage_prefix}:context_event_by_ingestion_time:{parent_type}:{parent_hash}"


def context_event_time_index_field(record: Json) -> str:
    event_hash = record.get("event_id_hash") or stable_hash(
        json.dumps(record, sort_keys=True, separators=(",", ":"))
    )
    timestamp_ms = context_event_timestamp_ms(record)
    return f"{context_event_time_key(timestamp_ms, event_hash):020d}:{event_hash}"


def context_event_time_index_payload(record: Json) -> str:
    scope_key = str(record.get("scope_key") or "")
    if not scope_key:
        scope = record.get("scope") if isinstance(record.get("scope"), dict) else {}
        scope_key = canonical_scope_key(scope)
    payload: Json = {
        "record_type": "context_event_ref",
        "ref_hash": int(record.get("event_id_hash") or 0),
        "node_hash": int(record.get("node_hash") or 0),
        "scope_key": scope_key,
        "timestamp_key_ms": context_event_timestamp_ms(record),
    }
    payload["context_event_key"] = context_event_time_key(
        payload["timestamp_key_ms"], payload["ref_hash"]
    )
    source_chunk_hash = record.get("source_chunk_hash")
    if source_chunk_hash is not None:
        payload["source_chunk_hash"] = source_chunk_hash
    return json.dumps(payload, sort_keys=True, separators=(",", ":"))


def context_event_time_index_entries(storage_prefix: str, records: list[Json]) -> list[Json]:
    entries: list[Json] = []
    full_payload = env_bool("MATRIXARK_CONTEXT_EVENT_TIME_INDEX_FULL_PAYLOAD", False)
    for record in records:
        if record.get("record_type") != "context_event":
            continue
        if record.get("event_id_hash") is None:
            continue
        enriched = attach_context_event_time_key(record)
        payload = (
            json.dumps(enriched, sort_keys=True, separators=(",", ":"))
            if full_payload
            else context_event_time_index_payload(enriched)
        )
        entries.append(
            {
                "key": context_event_time_index_key(storage_prefix, enriched),
                "field": context_event_time_index_field(enriched),
                "value": payload,
                "storage_route": record.get("storage_route") if isinstance(record.get("storage_route"), dict) else {},
            }
        )
    return entries


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
