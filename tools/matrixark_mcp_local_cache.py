#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Local adapter cache helpers for MatrixArk MCP."""

from __future__ import annotations

import threading

try:
    from tools.matrixark_mcp_core import (
        Json,
        compact_latest_context_state_records,
        session_buffer_key,
        session_buffer_key_from_scope,
        stable_hash,
    )
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_core import (
        Json,
        compact_latest_context_state_records,
        session_buffer_key,
        session_buffer_key_from_scope,
        stable_hash,
    )

try:
    from tools import matrixark_mcp_retrieval_records as retrieval_record_helpers
except ModuleNotFoundError:  # Direct script execution from tools/.
    import matrixark_mcp_retrieval_records as retrieval_record_helpers

_LOCAL_READ_CACHE_LOCK = threading.RLock()
_LOCAL_READ_CACHE: dict[str, tuple[int, int, list[Json]]] = {}


def update_read_cache_after_append(adapter: object, records: list[Json]) -> None:
    if not records:
        return
    cache_key = str(adapter.event_log.resolve())
    with adapter._read_cache_lock:
        if adapter._read_cache_records is not None:
            adapter._read_cache_records.extend(records)
            adapter._read_cache_records = compact_latest_context_state_records(adapter._read_cache_records)
        try:
            stat = adapter.event_log.stat()
            adapter._read_cache_size = int(stat.st_size)
            adapter._read_cache_mtime_ns = int(stat.st_mtime_ns)
        except FileNotFoundError:
            adapter._read_cache_records = None
            adapter._read_cache_size = -1
            adapter._read_cache_mtime_ns = -1
    with _LOCAL_READ_CACHE_LOCK:
        cached = _LOCAL_READ_CACHE.get(cache_key)
        if cached is not None:
            _, _, cached_records = cached
            cached_records = compact_latest_context_state_records(list(cached_records) + list(records))
            _LOCAL_READ_CACHE[cache_key] = (adapter._read_cache_size, adapter._read_cache_mtime_ns, cached_records)
        elif adapter._read_cache_records is not None:
            _LOCAL_READ_CACHE[cache_key] = (
                adapter._read_cache_size,
                adapter._read_cache_mtime_ns,
                compact_latest_context_state_records(list(adapter._read_cache_records)),
            )
    if any(str(record.get("record_type") or "") in retrieval_record_helpers.RETRIEVAL_HOT_RECORD_TYPES for record in records):
        with adapter._retrieval_records_cache_lock:
            adapter._retrieval_records_cache_generation += 1
            adapter._retrieval_records_cache.clear()
            with adapter._context_pack_cache_lock:
                adapter._context_pack_cache.clear()


def clear_read_cache_for_missing_log(adapter: object) -> None:
    cache_key = str(adapter.event_log.resolve())
    with adapter._read_cache_lock:
        adapter._read_cache_records = []
        adapter._read_cache_size = -1
        adapter._read_cache_mtime_ns = -1
    with _LOCAL_READ_CACHE_LOCK:
        _LOCAL_READ_CACHE.pop(cache_key, None)


def ensure_session_cache_fields(adapter: object) -> None:
    if not hasattr(adapter, "_session_buffer_cache_lock"):
        adapter._session_buffer_cache_lock = threading.RLock()
    if not hasattr(adapter, "_context_event_by_hash"):
        adapter._context_event_by_hash = {}
    if not hasattr(adapter, "_session_pending_event_ids_by_key"):
        adapter._session_pending_event_ids_by_key = {}
    if not hasattr(adapter, "_session_committed_event_ids_by_key"):
        adapter._session_committed_event_ids_by_key = {}


def update_latest_entity_cache(adapter: object, records: list[Json]) -> None:
    ensure_session_cache_fields(adapter)
    for record in records:
        record_type = record.get("record_type")
        if record_type == "context_event":
            try:
                event_hash = int(record.get("event_id_hash", 0))
            except (TypeError, ValueError):
                event_hash = 0
            if event_hash:
                with adapter._session_buffer_cache_lock:
                    adapter._context_event_by_hash[event_hash] = record
            continue
        if record_type == "session_buffer_event":
            try:
                event_hash = int(record.get("event_id_hash", 0))
            except (TypeError, ValueError):
                event_hash = 0
            raw_key = record.get("buffer_key", [])
            if event_hash and isinstance(raw_key, list) and len(raw_key) == 4:
                key = tuple(str(item) for item in raw_key)
                with adapter._session_buffer_cache_lock:
                    committed = adapter._session_committed_event_ids_by_key.setdefault(key, set())
                    pending = adapter._session_pending_event_ids_by_key.setdefault(key, [])
                    if event_hash not in committed and event_hash not in pending:
                        pending.append(event_hash)
            continue
        if record_type == "context_batch_commit":
            key = session_buffer_key_from_scope(record.get("scope", {}))
            source_ids: list[int] = []
            for ref in record.get("source_event_ids", []):
                try:
                    source_ids.append(int(ref))
                except (TypeError, ValueError):
                    continue
            if source_ids:
                with adapter._session_buffer_cache_lock:
                    committed = adapter._session_committed_event_ids_by_key.setdefault(key, set())
                    committed.update(source_ids)
                    pending = adapter._session_pending_event_ids_by_key.setdefault(key, [])
                    if pending:
                        source_set = set(source_ids)
                        adapter._session_pending_event_ids_by_key[key] = [event_id for event_id in pending if event_id not in source_set]
            continue
        if record_type == "context_node":
            try:
                node_hash = int(record.get("node_hash", 0))
            except (TypeError, ValueError):
                node_hash = 0
            if node_hash:
                adapter._context_node_hashes.add(node_hash)
            continue
        if record_type == "context_child_ref":
            try:
                child_ref_hash = int(record.get("child_ref_hash", 0))
            except (TypeError, ValueError):
                child_ref_hash = 0
            if child_ref_hash:
                adapter._context_child_ref_hashes.add(child_ref_hash)
            continue
        if record_type != "context_entity":
            continue
        try:
            entity_hash = int(record.get("entity_hash", 0))
        except (TypeError, ValueError):
            continue
        if entity_hash:
            adapter._latest_entity_by_hash[entity_hash] = record


def ensure_context_node_cache_loaded(adapter: object) -> None:
    if adapter._context_node_cache_loaded:
        return
    adapter._context_node_hashes = set()
    adapter._context_child_ref_hashes = set()
    for record in adapter.read_all():
        if record.get("record_type") == "context_node" and record.get("node_hash") is not None:
            try:
                adapter._context_node_hashes.add(int(record.get("node_hash")))
            except (TypeError, ValueError):
                pass
        elif record.get("record_type") == "context_child_ref" and record.get("child_ref_hash") is not None:
            try:
                adapter._context_child_ref_hashes.add(int(record.get("child_ref_hash")))
            except (TypeError, ValueError):
                pass
    adapter._context_node_cache_loaded = True


def ensure_latest_entity_cache_loaded(adapter: object) -> None:
    if adapter._entity_cache_loaded:
        return
    records = adapter.read_all()
    adapter._latest_entity_by_hash = {}
    for record in records:
        if record.get("record_type") != "context_entity":
            continue
        try:
            entity_hash = int(record.get("entity_hash", 0))
        except (TypeError, ValueError):
            continue
        if entity_hash:
            adapter._latest_entity_by_hash[entity_hash] = record
    adapter._entity_cache_loaded = True


def find_latest_entity(adapter: object, *, node_hash: int, entity_type: str, entity_name: str) -> Json | None:
    entity_hash = stable_hash(f"{node_hash}:{entity_type}:{entity_name}")
    if entity_hash in adapter._latest_entity_by_hash:
        return adapter._latest_entity_by_hash[entity_hash]
    ensure_latest_entity_cache_loaded(adapter)
    return adapter._latest_entity_by_hash.get(entity_hash)


def pending_session_events(adapter: object, scope: Json, *, limit: int | None = None) -> list[Json]:
    key = session_buffer_key_from_scope(scope)
    ensure_session_cache_fields(adapter)
    with adapter._session_buffer_cache_lock:
        if key in adapter._session_pending_event_ids_by_key:
            pending_ids = list(adapter._session_pending_event_ids_by_key.get(key, []))
            events = [adapter._context_event_by_hash[event_hash] for event_hash in pending_ids if event_hash in adapter._context_event_by_hash]
            return events[:limit] if limit is not None else events
    committed: set[int] = set()
    reader = getattr(adapter, "records_for_session_buffer", None)
    records = reader(scope) if callable(reader) else adapter.read_all()
    for record in records:
        if record.get("record_type") == "context_batch_commit" and session_buffer_key_from_scope(record.get("scope", {})) == key:
            for ref in record.get("source_event_ids", []):
                try:
                    committed.add(int(ref))
                except (TypeError, ValueError):
                    continue
    pending_ids: list[int] = []
    for record in records:
        if record.get("record_type") != "session_buffer_event" or tuple(record.get("buffer_key", [])) != key:
            continue
        try:
            event_hash = int(record.get("event_id_hash"))
        except (TypeError, ValueError):
            continue
        if event_hash not in committed:
            pending_ids.append(event_hash)
    event_by_id: dict[int, Json] = {}
    fallback_events: list[Json] = []
    for record in records:
        if record.get("record_type") != "context_event":
            continue
        try:
            event_hash = int(record.get("event_id_hash"))
        except (TypeError, ValueError):
            continue
        event_by_id[event_hash] = record
        if not pending_ids and session_buffer_key(record.get("envelope", {})) == key and event_hash not in committed:
            fallback_events.append(record)
    events = [event_by_id[event_hash] for event_hash in pending_ids if event_hash in event_by_id]
    if not events:
        events = fallback_events
    with adapter._session_buffer_cache_lock:
        adapter._context_event_by_hash.update(event_by_id)
        adapter._session_committed_event_ids_by_key[key] = set(committed)
        cached_pending_ids: list[int] = []
        for record in events:
            try:
                cached_pending_ids.append(int(record.get("event_id_hash")))
            except (TypeError, ValueError):
                continue
        adapter._session_pending_event_ids_by_key[key] = cached_pending_ids
    if limit is not None:
        return events[:limit]
    return events
