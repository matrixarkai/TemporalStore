#!/usr/bin/env python3
"""Direct TemporalStore record and candidate cache helpers."""

from __future__ import annotations

import json
import threading
from typing import Any

try:
    from tools.matrixark_mcp_core import (
        Json,
        _DIRECT_PLACEMENT_CANDIDATE_TABLE_CACHE,
        _DIRECT_PLACEMENT_CANDIDATE_TABLE_CACHE_LOCK,
        _DIRECT_PLACEMENT_CANDIDATE_TABLE_CACHE_MAX_ENTRIES,
        _DIRECT_RECORD_CACHE,
        _DIRECT_RECORD_CACHE_LOCK,
        _DIRECT_RECORD_CACHE_MAX_PREFIXES,
        _DIRECT_RECORD_LOAD_LOCKS,
        _DIRECT_RETRIEVAL_CANDIDATE_CACHE,
        _DIRECT_RETRIEVAL_CANDIDATE_CACHE_LOCK,
        _DIRECT_RETRIEVAL_CANDIDATE_CACHE_MAX_ENTRIES,
        canonical_scope_key,
        stable_hash,
    )
    from tools.matrixark_mcp_local_adapter import RETRIEVAL_HOT_RECORD_TYPES
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_core import (
        Json,
        _DIRECT_PLACEMENT_CANDIDATE_TABLE_CACHE,
        _DIRECT_PLACEMENT_CANDIDATE_TABLE_CACHE_LOCK,
        _DIRECT_PLACEMENT_CANDIDATE_TABLE_CACHE_MAX_ENTRIES,
        _DIRECT_RECORD_CACHE,
        _DIRECT_RECORD_CACHE_LOCK,
        _DIRECT_RECORD_CACHE_MAX_PREFIXES,
        _DIRECT_RECORD_LOAD_LOCKS,
        _DIRECT_RETRIEVAL_CANDIDATE_CACHE,
        _DIRECT_RETRIEVAL_CANDIDATE_CACHE_LOCK,
        _DIRECT_RETRIEVAL_CANDIDATE_CACHE_MAX_ENTRIES,
        canonical_scope_key,
        stable_hash,
    )
    from matrixark_mcp_local_adapter import RETRIEVAL_HOT_RECORD_TYPES


def direct_record_load_lock(target: Any) -> threading.RLock:
    with _DIRECT_RECORD_CACHE_LOCK:
        lock = _DIRECT_RECORD_LOAD_LOCKS.get(target._storage_prefix)
        if lock is None:
            lock = threading.RLock()
            _DIRECT_RECORD_LOAD_LOCKS[target._storage_prefix] = lock
        return lock


def get_direct_record_cache(target: Any, count: int) -> list[Json] | None:
    if not target.python_hot_cache_enabled():
        return None
    with _DIRECT_RECORD_CACHE_LOCK:
        cached = _DIRECT_RECORD_CACHE.get(target._storage_prefix)
        if cached is None:
            return None
        cached_count, records = cached
        if cached_count != count:
            return None
        return list(records)


def put_direct_record_cache(target: Any, count: int, records: list[Json]) -> None:
    if not target.python_hot_cache_enabled():
        return
    with _DIRECT_RECORD_CACHE_LOCK:
        if len(_DIRECT_RECORD_CACHE) >= _DIRECT_RECORD_CACHE_MAX_PREFIXES and target._storage_prefix not in _DIRECT_RECORD_CACHE:
            oldest = next(iter(_DIRECT_RECORD_CACHE))
            _DIRECT_RECORD_CACHE.pop(oldest, None)
        _DIRECT_RECORD_CACHE[target._storage_prefix] = (count, list(records))


def drop_direct_record_cache(target: Any) -> None:
    target._entry_count_cache = None
    target._records_cache = None
    target._index_cache = None
    with _DIRECT_RECORD_CACHE_LOCK:
        _DIRECT_RECORD_CACHE.pop(target._storage_prefix, None)
    with target._retrieval_candidate_cache_lock:
        target._retrieval_candidate_cache.clear()


def retrieval_candidate_cache_key(
    target: Any,
    *,
    count: int,
    scope: Json,
    record_types: set[str] | None,
    secondary_index_groups: list[set[str]] | None,
    selected_node_hashes: set[int] | None,
) -> str:
    return json.dumps(
        {
            "count": count,
            "storage_prefix": target._storage_prefix,
            "scope": scope or {},
            "record_types": sorted(record_types or RETRIEVAL_HOT_RECORD_TYPES),
            "secondary_index_groups": [
                sorted(group)
                for group in (secondary_index_groups or [])
            ],
            "selected_node_hashes": sorted(selected_node_hashes or []),
        },
        sort_keys=True,
        separators=(",", ":"),
    )


def prune_retrieval_candidate_cache(target: Any, current_count: int) -> None:
    with _DIRECT_RETRIEVAL_CANDIDATE_CACHE_LOCK:
        stale_keys = [
            key
            for key, cached in _DIRECT_RETRIEVAL_CANDIDATE_CACHE.items()
            if cached.get("storage_prefix") == target._storage_prefix
            and int(cached.get("count") or -1) != int(current_count)
        ]
        for key in stale_keys:
            _DIRECT_RETRIEVAL_CANDIDATE_CACHE.pop(key, None)
        if len(_DIRECT_RETRIEVAL_CANDIDATE_CACHE) > _DIRECT_RETRIEVAL_CANDIDATE_CACHE_MAX_ENTRIES:
            overflow = len(_DIRECT_RETRIEVAL_CANDIDATE_CACHE) - _DIRECT_RETRIEVAL_CANDIDATE_CACHE_MAX_ENTRIES
            for key in list(_DIRECT_RETRIEVAL_CANDIDATE_CACHE)[:overflow]:
                _DIRECT_RETRIEVAL_CANDIDATE_CACHE.pop(key, None)
    with _DIRECT_PLACEMENT_CANDIDATE_TABLE_CACHE_LOCK:
        stale_keys = [
            key
            for key in _DIRECT_PLACEMENT_CANDIDATE_TABLE_CACHE
            if key.startswith(f"{target._storage_prefix}|")
            and f"|wm={int(current_count)}|" not in key
        ]
        for key in stale_keys:
            _DIRECT_PLACEMENT_CANDIDATE_TABLE_CACHE.pop(key, None)
        if len(_DIRECT_PLACEMENT_CANDIDATE_TABLE_CACHE) > _DIRECT_PLACEMENT_CANDIDATE_TABLE_CACHE_MAX_ENTRIES:
            overflow = len(_DIRECT_PLACEMENT_CANDIDATE_TABLE_CACHE) - _DIRECT_PLACEMENT_CANDIDATE_TABLE_CACHE_MAX_ENTRIES
            for key in list(_DIRECT_PLACEMENT_CANDIDATE_TABLE_CACHE)[:overflow]:
                _DIRECT_PLACEMENT_CANDIDATE_TABLE_CACHE.pop(key, None)


def placement_candidate_table_cache_key(
    target: Any,
    *,
    count: int,
    scope_key: str,
    node_hash: int,
    record_type: str,
    resource_version_watermark: str = "",
) -> str:
    return (
        f"{target._storage_prefix}|wm={int(count)}|scope={stable_hash(scope_key)}|"
        f"node={int(node_hash)}|type={record_type}|rv={stable_hash(resource_version_watermark)}"
    )


def record_primary_hash(record: Json) -> int:
    for field in (
        "event_id_hash",
        "entity_hash",
        "segment_hash",
        "compression_id_hash",
        "summary_hash",
        "chunk_hash",
        "section_hash",
        "skill_hash",
        "resource_hash",
        "batch_id_hash",
        "ref_hash",
    ):
        value = record.get(field)
        if value is not None:
            try:
                return int(value)
            except (TypeError, ValueError):
                break
    return stable_hash(json.dumps(record, sort_keys=True, separators=(",", ":")))


def placement_candidate_records_from_cache_or_load(
    target: Any,
    *,
    count: int,
    scope: Json,
    allowed_types: set[str],
    selected_nodes: set[int],
    locations: list[Json],
    resource_version_watermark: str = "",
) -> Json:
    scope_key = canonical_scope_key(scope)
    if not scope_key or not selected_nodes or not allowed_types:
        return {"records": [], "cache_hit": False, "cache_entries": 0, "loaded_records": 0}

    keys = [
        target._placement_candidate_table_cache_key(
            count=count,
            scope_key=scope_key,
            node_hash=node_hash,
            record_type=record_type,
            resource_version_watermark=resource_version_watermark,
        )
        for node_hash in sorted(selected_nodes)
        for record_type in sorted(allowed_types)
    ]
    with _DIRECT_PLACEMENT_CANDIDATE_TABLE_CACHE_LOCK:
        cached_tables = [_DIRECT_PLACEMENT_CANDIDATE_TABLE_CACHE.get(key) for key in keys]
        if keys and all(table is not None for table in cached_tables):
            compact_rows = [
                row
                for table in cached_tables
                for row in (table or [])
            ]
            return {
                "records": [dict(row[3]) for row in compact_rows],
                "cache_hit": True,
                "cache_entries": len(compact_rows),
                "loaded_records": 0,
                "resource_version_watermark": resource_version_watermark,
            }

    loaded_records = target._load_records_from_locations(locations)
    grouped: dict[str, list[tuple[str, int, int, Json]]] = {key: [] for key in keys}
    for record in loaded_records:
        record_type = str(record.get("record_type") or "")
        if record_type not in allowed_types:
            continue
        try:
            node_hash = int(record.get("node_hash"))
        except (TypeError, ValueError):
            continue
        if node_hash not in selected_nodes:
            continue
        key = target._placement_candidate_table_cache_key(
            count=count,
            scope_key=scope_key,
            node_hash=node_hash,
            record_type=record_type,
            resource_version_watermark=resource_version_watermark,
        )
        if key not in grouped:
            continue
        grouped[key].append((record_type, target._record_primary_hash(record), node_hash, dict(record)))

    with _DIRECT_PLACEMENT_CANDIDATE_TABLE_CACHE_LOCK:
        for key, compact_rows in grouped.items():
            _DIRECT_PLACEMENT_CANDIDATE_TABLE_CACHE[key] = compact_rows
        if len(_DIRECT_PLACEMENT_CANDIDATE_TABLE_CACHE) > _DIRECT_PLACEMENT_CANDIDATE_TABLE_CACHE_MAX_ENTRIES:
            overflow = len(_DIRECT_PLACEMENT_CANDIDATE_TABLE_CACHE) - _DIRECT_PLACEMENT_CANDIDATE_TABLE_CACHE_MAX_ENTRIES
            for key in list(_DIRECT_PLACEMENT_CANDIDATE_TABLE_CACHE)[:overflow]:
                _DIRECT_PLACEMENT_CANDIDATE_TABLE_CACHE.pop(key, None)

    compact_rows = [row for table in grouped.values() for row in table]
    return {
        "records": [dict(row[3]) for row in compact_rows],
        "cache_hit": False,
        "cache_entries": len(compact_rows),
        "loaded_records": len(loaded_records),
        "resource_version_watermark": resource_version_watermark,
    }
