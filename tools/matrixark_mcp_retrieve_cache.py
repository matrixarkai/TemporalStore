#!/usr/bin/env python3
"""ContextPack cache helpers for MatrixArk retrieval."""

from __future__ import annotations

import json
import time
from typing import Any

try:
    from tools.matrixark_mcp_core import (
        Json,
        canonical_scope_key,
        compact_context_pack_for_serving_flat as compact_context_pack_for_serving,
        python_hot_cache_allowed,
    )
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_core import (
        Json,
        canonical_scope_key,
        compact_context_pack_for_serving_flat as compact_context_pack_for_serving,
        python_hot_cache_allowed,
    )


def context_pack_cache_enabled(target: Any) -> bool:
    return (
        target._context_pack_cache_max_entries > 0
        and target._context_pack_cache_ttl_s > 0
        and python_hot_cache_allowed(backend_label=str(getattr(target, "_backend_label", lambda: "local")()))
    )


def context_pack_cache_key(
    target: Any,
    *,
    scope: Json,
    query: str,
    question_type: str,
    retrieval_session_scope: str,
    max_context_tokens: int,
    local_budget: Json,
    ranking: Json,
    include_superseded: bool,
) -> tuple[Any, ...]:
    return (
        target._retrieval_records_cache_generation,
        canonical_scope_key(scope),
        query,
        question_type,
        retrieval_session_scope,
        max_context_tokens,
        int(local_budget.get("token_estimate", 0)),
        tuple(sorted(local_budget.get("text_hashes", set()))),
        json.dumps(ranking, sort_keys=True, separators=(",", ":")),
        include_superseded,
    )


def get_cached_context_pack(target: Any, cache_key: tuple[Any, ...], *, include_debug: bool) -> Json | None:
    if not context_pack_cache_enabled(target):
        return None
    with target._context_pack_cache_lock:
        cached = target._context_pack_cache.get(cache_key)
        if cached is None:
            return None
        cached_at, cached_pack = cached
        if time.monotonic() - cached_at > target._context_pack_cache_ttl_s:
            target._context_pack_cache.pop(cache_key, None)
            return None
        pack = json.loads(json.dumps(cached_pack))
        pack["context_pack_cache_hit"] = True
        recall_policy = pack.get("recall_policy") if isinstance(pack.get("recall_policy"), dict) else {}
        recall_policy["context_pack_cache"] = {"hit": True, "ttl_s": target._context_pack_cache_ttl_s}
        pack["recall_policy"] = recall_policy
        return compact_context_pack_for_serving(pack, include_debug=include_debug)


def invalidate_context_pack_cache(target: Any) -> Json:
    """Clear ContextPack cache after retrieval-visible writes."""
    cleared_count = 0
    lock = getattr(target, "_context_pack_cache_lock", None)
    cache = getattr(target, "_context_pack_cache", None)
    if lock is not None and isinstance(cache, dict):
        with lock:
            cleared_count = len(cache)
            cache.clear()
    try:
        target._retrieval_records_cache_generation = int(getattr(target, "_retrieval_records_cache_generation", 0)) + 1
    except (TypeError, ValueError):
        target._retrieval_records_cache_generation = 1
    return {
        "cleared_context_pack_cache_count": cleared_count,
        "retrieval_records_cache_generation": int(getattr(target, "_retrieval_records_cache_generation", 0)),
    }


def put_cached_context_pack(target: Any, cache_key: tuple[Any, ...], pack: Json) -> None:
    if not context_pack_cache_enabled(target) or pack.get("partial_context_pack"):
        return
    cached_pack = json.loads(json.dumps(pack))
    cached_recall = cached_pack.get("recall_policy") if isinstance(cached_pack.get("recall_policy"), dict) else {}
    cached_recall["context_pack_cache"] = {"hit": False, "ttl_s": target._context_pack_cache_ttl_s}
    cached_pack["recall_policy"] = cached_recall
    with target._context_pack_cache_lock:
        if len(target._context_pack_cache) >= target._context_pack_cache_max_entries:
            oldest_key = next(iter(target._context_pack_cache))
            target._context_pack_cache.pop(oldest_key, None)
        target._context_pack_cache[cache_key] = (time.monotonic(), cached_pack)
