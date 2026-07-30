#!/usr/bin/env python3
"""Cache helpers for the MatrixArk Rust proxy client."""

from __future__ import annotations

import copy
import hashlib
import json
import threading
import time
from typing import Any, Iterable

try:
    from tools.matrixark_mcp_core import Json, MatrixArkError
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_core import Json, MatrixArkError


def string_cache_key_allowed(target: Any, key: str) -> bool:
    return target._string_cache_enabled and str(key).endswith((":record_count", ":record_index"))


def string_cache_get(target: Any, key: str) -> str | None:
    if not string_cache_key_allowed(target, key):
        return None
    with target._string_cache_lock:
        value = target._string_cache.get(key)
    with target._metrics_lock:
        if value is None:
            target._string_cache_misses_total += 1
        else:
            target._string_cache_hits_total += 1
    return value


def string_cache_put(target: Any, key: str, value: str) -> None:
    if not string_cache_key_allowed(target, key):
        return
    with target._string_cache_lock:
        target._string_cache[key] = str(value)
    with target._metrics_lock:
        target._string_cache_updates_total += 1


def scan_hash_cache_get(target: Any, key: str) -> Json | None:
    if not target._scan_hash_cache_enabled:
        return None
    with target._scan_hash_cache_lock:
        cached = target._scan_hash_cache.get(key)
        if cached is None:
            value = None
        else:
            target._scan_hash_cache.move_to_end(key)
            value = copy.deepcopy(cached)
    with target._metrics_lock:
        if value is None:
            target._scan_hash_cache_misses_total += 1
        else:
            target._scan_hash_cache_hits_total += 1
    return value


def scan_hash_cache_put(target: Any, key: str, response: Json) -> None:
    if not target._scan_hash_cache_enabled:
        return
    with target._scan_hash_cache_lock:
        target._scan_hash_cache[key] = copy.deepcopy(response)
        target._scan_hash_cache.move_to_end(key)
        while len(target._scan_hash_cache) > target._scan_hash_cache_max_entries:
            target._scan_hash_cache.popitem(last=False)
    with target._metrics_lock:
        target._scan_hash_cache_updates_total += 1


def scan_hash_cache_invalidate_keys(target: Any, keys: Iterable[str]) -> None:
    if not target._scan_hash_cache_enabled:
        return
    removed = 0
    with target._scan_hash_cache_lock:
        for key in set(str(item) for item in keys if str(item)):
            if target._scan_hash_cache.pop(key, None) is not None:
                removed += 1
    if removed:
        with target._metrics_lock:
            target._scan_hash_cache_invalidations_total += removed


def context_pack_response_cache_key(
    *,
    count_key: str,
    record_hash_key: str,
    shard_size: int,
    request: Json,
) -> str:
    ranking = request.get("ranking") if isinstance(request, dict) else {}
    payload = {
        "count_key": count_key,
        "record_hash_key": record_hash_key,
        "shard_size": int(shard_size),
        "scope": request.get("scope", {}) if isinstance(request, dict) else {},
        "secondary_index_groups": request.get("secondary_index_groups", []) if isinstance(request, dict) else [],
        "query": request.get("query", "") if isinstance(request, dict) else "",
        "max_selected_refs": ranking.get("max_selected_refs") if isinstance(ranking, dict) else None,
    }
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str).encode()
    return hashlib.blake2b(encoded, digest_size=16).hexdigest()


def mark_context_pack_response_cache_hit(response: Json) -> Json:
    cached = dict(response)
    cached["cache_hit"] = True
    cached["context_pack_response_cache_hit"] = True
    metrics = cached.get("retrieval_metrics")
    if isinstance(metrics, dict):
        metrics = dict(metrics)
        cached["retrieval_metrics"] = metrics
        metrics["cache_hit"] = True
        metrics["context_pack_response_cache_hit"] = True
        metrics.setdefault("candidate_cache_hit", False)
    pack = cached.get("context_pack")
    if isinstance(pack, dict):
        pack = dict(pack)
        cached["context_pack"] = pack
        pack_metrics = pack.get("retrieval_metrics")
        if isinstance(pack_metrics, dict):
            pack_metrics = dict(pack_metrics)
            pack["retrieval_metrics"] = pack_metrics
            pack_metrics["cache_hit"] = True
            pack_metrics["context_pack_response_cache_hit"] = True
            pack_metrics.setdefault("candidate_cache_hit", False)
    return cached


def context_pack_response_cache_get(target: Any, cache_key: str) -> Json | None:
    if not target._context_pack_response_cache_enabled:
        return None
    with target._context_pack_response_cache_lock:
        cached = target._context_pack_response_cache.get(cache_key)
        if cached is not None:
            target._context_pack_response_cache.move_to_end(cache_key)
    with target._metrics_lock:
        if cached is None:
            target._context_pack_response_cache_misses_total += 1
        else:
            target._context_pack_response_cache_hits_total += 1
    if cached is None:
        return None
    return mark_context_pack_response_cache_hit(cached)


def context_pack_response_cache_put(target: Any, cache_key: str, response: Json) -> None:
    if not target._context_pack_response_cache_enabled:
        return
    with target._context_pack_response_cache_lock:
        target._context_pack_response_cache[cache_key] = copy.deepcopy(response)
        target._context_pack_response_cache.move_to_end(cache_key)
        while len(target._context_pack_response_cache) > target._context_pack_response_cache_max_entries:
            target._context_pack_response_cache.popitem(last=False)
    with target._metrics_lock:
        target._context_pack_response_cache_updates_total += 1


def context_pack_response_cache_clear(target: Any) -> None:
    if not target._context_pack_response_cache_enabled:
        return
    with target._context_pack_response_cache_lock:
        removed = len(target._context_pack_response_cache)
        target._context_pack_response_cache.clear()
    if removed:
        with target._metrics_lock:
            target._context_pack_response_cache_invalidations_total += removed


def context_pack_response_singleflight_enter(target: Any, cache_key: str) -> tuple[Json, bool]:
    if not target._context_pack_response_cache_enabled:
        return {"event": threading.Event(), "error": None}, True
    with target._context_pack_response_cache_lock:
        inflight = target._context_pack_response_inflight.get(cache_key)
        if inflight is not None:
            return inflight, False
        inflight = {"event": threading.Event(), "error": None}
        target._context_pack_response_inflight[cache_key] = inflight
        return inflight, True


def context_pack_response_singleflight_finish(
    target: Any,
    cache_key: str,
    inflight: Json,
    error: BaseException | None,
) -> None:
    if not target._context_pack_response_cache_enabled:
        return
    with target._context_pack_response_cache_lock:
        current = target._context_pack_response_inflight.get(cache_key)
        if current is inflight:
            target._context_pack_response_inflight.pop(cache_key, None)
        inflight["error"] = error
        event = inflight.get("event")
        if isinstance(event, threading.Event):
            event.set()


def context_pack_response_singleflight_wait(target: Any, cache_key: str, inflight: Json) -> Json:
    event = inflight.get("event")
    if not isinstance(event, threading.Event):
        raise MatrixArkError("invalid ContextPack singleflight state")
    started = time.perf_counter()
    timeout_s = max(target._backpressure_timeout_s, target.request_timeout_ms / 1000.0 + 2.0)
    if not event.wait(timeout=timeout_s):
        raise MatrixArkError(f"Rust TemporalStore ContextPack singleflight timed out after {timeout_s:.1f}s")
    wait_ms = (time.perf_counter() - started) * 1000.0
    with target._metrics_lock:
        target._context_pack_response_singleflight_waits_total += 1
        target._context_pack_response_singleflight_wait_ms_total += wait_ms
        target._context_pack_response_singleflight_wait_ms_max = max(
            target._context_pack_response_singleflight_wait_ms_max,
            wait_ms,
        )
    error = inflight.get("error")
    if error:
        raise error
    cached = context_pack_response_cache_get(target, cache_key)
    if cached is not None:
        return cached
    raise MatrixArkError("ContextPack singleflight completed without cached response")
