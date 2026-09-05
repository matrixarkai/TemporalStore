#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Direct TemporalStore record and candidate cache helpers."""

from __future__ import annotations

import copy
import hashlib
import json
import os
import threading
from collections import OrderedDict
from typing import Any

try:
    from tools.matrixark_mcp_direct_cache_state import (
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
    )
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_direct_cache_state import (
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
    )

try:
    from tools.matrixark_mcp_core import (
        Json,
        canonical_scope_key,
        stable_hash,
    )
    from tools.matrixark_mcp_local_adapter import RETRIEVAL_HOT_RECORD_TYPES
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_core import (
        Json,
        canonical_scope_key,
        stable_hash,
    )
    from matrixark_mcp_local_adapter import RETRIEVAL_HOT_RECORD_TYPES


def direct_cache_scope(target: Any) -> str:
    """The identity of the store a cache entry belongs to.

    `_storage_prefix` alone is NOT that identity: it defaults to "matrixark:mcp" for every
    adapter, while the store a client actually talks to is separated by namespace and table. Two
    adapters over different stores in one process therefore share a cache key, and whichever
    wrote last wins -- one store serving another store's records. It has been latent because the
    record cache was off for native backends; turning it on makes it reachable, so the key names
    the store.
    """
    return "%s|%s|%s" % (
        str(getattr(target, "_namespace", "") or ""),
        str(getattr(target, "_table", "") or ""),
        str(getattr(target, "_storage_prefix", "") or ""),
    )


def direct_record_load_lock(target: Any) -> threading.RLock:
    with _DIRECT_RECORD_CACHE_LOCK:
        lock = _DIRECT_RECORD_LOAD_LOCKS.get(direct_cache_scope(target))
        if lock is None:
            lock = threading.RLock()
            _DIRECT_RECORD_LOAD_LOCKS[direct_cache_scope(target)] = lock
        return lock


def get_direct_record_cache(target: Any, count: int) -> list[Json] | None:
    if not target.python_hot_cache_enabled():
        return None
    with _DIRECT_RECORD_CACHE_LOCK:
        cached = _DIRECT_RECORD_CACHE.get(direct_cache_scope(target))
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
        if len(_DIRECT_RECORD_CACHE) >= _DIRECT_RECORD_CACHE_MAX_PREFIXES and direct_cache_scope(target) not in _DIRECT_RECORD_CACHE:
            oldest = next(iter(_DIRECT_RECORD_CACHE))
            _DIRECT_RECORD_CACHE.pop(oldest, None)
        _DIRECT_RECORD_CACHE[direct_cache_scope(target)] = (count, list(records))


def drop_direct_record_cache(target: Any) -> None:
    target._entry_count_cache = None
    target._records_cache = None
    target._index_cache = None
    with _DIRECT_RECORD_CACHE_LOCK:
        _DIRECT_RECORD_CACHE.pop(direct_cache_scope(target), None)
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


def ensure_direct_context_pack_response_cache(target: Any) -> None:
    if hasattr(target, "_direct_context_pack_response_cache"):
        return
    target._direct_context_pack_response_cache_enabled = True
    target._direct_context_pack_response_cache_max_entries = max(
        1, int(os.environ.get("MATRIXARK_DIRECT_CONTEXT_PACK_RESPONSE_CACHE_MAX_ENTRIES", "256"))
    )
    target._direct_context_pack_response_cache_lock = threading.Lock()
    target._direct_context_pack_response_cache: OrderedDict[str, Json] = OrderedDict()
    target._direct_context_pack_response_cache_hits_total = 0
    target._direct_context_pack_response_cache_misses_total = 0
    target._direct_context_pack_response_cache_updates_total = 0


def direct_context_pack_response_cache_key(
    target: Any,
    *,
    count_key: str,
    record_hash_key: str,
    shard_size: int,
    request: Json,
) -> str:
    ranking = request.get("ranking") if isinstance(request, dict) else {}
    backend_label = target._backend_label() if callable(getattr(target, "_backend_label", None)) else ""
    payload = {
        "backend": backend_label,
        "storage_prefix": str(getattr(target, "_storage_prefix", "") or ""),
        "count_key": count_key,
        "record_hash_key": record_hash_key,
        "shard_size": int(shard_size),
        "scope": request.get("scope", {}) if isinstance(request, dict) else {},
        "secondary_index_groups": request.get("secondary_index_groups", []) if isinstance(request, dict) else [],
        "query": request.get("query", "") if isinstance(request, dict) else "",
        "max_selected_refs": ranking.get("max_selected_refs") if isinstance(ranking, dict) else None,
        "max_context_tokens": request.get("max_context_tokens") if isinstance(request, dict) else None,
    }
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str).encode()
    return hashlib.blake2b(encoded, digest_size=16).hexdigest()


def direct_context_pack_response_cache_get(target: Any, cache_key: str) -> Json | None:
    ensure_direct_context_pack_response_cache(target)
    if not target._direct_context_pack_response_cache_enabled:
        return None
    with target._direct_context_pack_response_cache_lock:
        cached = target._direct_context_pack_response_cache.get(cache_key)
        if cached is not None:
            target._direct_context_pack_response_cache.move_to_end(cache_key)
            target._direct_context_pack_response_cache_hits_total += 1
            result = copy.deepcopy(cached)
        else:
            target._direct_context_pack_response_cache_misses_total += 1
            result = None
    if result is not None:
        metrics = result.get("retrieval_metrics")
        if isinstance(metrics, dict):
            metrics["context_pack_response_cache_hit"] = True
            metrics["cache_hit"] = True
    return result


def direct_context_pack_response_cache_put(target: Any, cache_key: str, response: Json) -> None:
    ensure_direct_context_pack_response_cache(target)
    if not target._direct_context_pack_response_cache_enabled:
        return
    with target._direct_context_pack_response_cache_lock:
        target._direct_context_pack_response_cache[cache_key] = copy.deepcopy(response)
        target._direct_context_pack_response_cache.move_to_end(cache_key)
        while len(target._direct_context_pack_response_cache) > target._direct_context_pack_response_cache_max_entries:
            target._direct_context_pack_response_cache.popitem(last=False)
        target._direct_context_pack_response_cache_updates_total += 1
