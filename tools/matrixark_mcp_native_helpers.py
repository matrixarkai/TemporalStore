#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Small helpers shared by MatrixArk native TemporalStore adapters."""

from __future__ import annotations

import math
from typing import Any

try:
    from tools.matrixark_mcp_identity import (
        canonical_scope_key,
        identity_hashes,
        local_identity_defaults,
        stable_hash,
    )
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_identity import (
        canonical_scope_key,
        identity_hashes,
        local_identity_defaults,
        stable_hash,
    )


Json = dict[str, Any]


def latency_quantile_from_cumulative_buckets(
    buckets: list[int],
    bucket_bounds: tuple[float, ...],
    total: int,
    quantile: float,
) -> float:
    if total <= 0:
        return 0.0
    target = max(1, math.ceil(total * quantile))
    previous_bound = 0.0
    for count, bound in zip(buckets, bucket_bounds):
        if int(count) >= target:
            return previous_bound if bound == float("inf") else float(bound)
        if bound != float("inf"):
            previous_bound = float(bound)
    return previous_bound


def latency_quantile_from_bucket_map(
    buckets: dict[str, Any],
    total: int,
    quantile: float,
) -> float:
    if total <= 0:
        return 0.0
    parsed: list[tuple[float, int]] = []
    for key, value in buckets.items():
        bound = float("inf") if str(key) == "+Inf" else float(key)
        parsed.append((bound, int(value or 0)))
    parsed.sort(key=lambda item: item[0])
    target = max(1, math.ceil(total * quantile))
    previous = 0.0
    for bound, count in parsed:
        if count >= target:
            return previous if bound == float("inf") else bound
        if bound != float("inf"):
            previous = bound
    return previous


def float_metric_or_default(metrics: dict[str, Any], name: str, default: float = 0.0) -> float:
    if name not in metrics or metrics.get(name) is None:
        return float(default)
    try:
        return float(metrics.get(name))
    except (TypeError, ValueError):
        return float(default)


def native_scope_with_hashes(scope: Json) -> Json:
    if not isinstance(scope, dict):
        return {}
    if int(scope.get("tenant_hash") or 0) and canonical_scope_key(scope):
        return dict(scope)
    defaults = local_identity_defaults({}, scope)
    account_id = str(scope.get("account_id") or defaults.get("account_id") or "acct_local")
    tenant_id = str(scope.get("tenant_id") or defaults.get("tenant_id") or "tenant_local_agent")
    user_id = str(scope.get("user_id") or defaults.get("user_id") or "")
    session_id = str(scope.get("session_id") or defaults.get("session_id") or "")
    agent_id = str(scope.get("agent_id") or "")
    hashes = identity_hashes(account_id, tenant_id, user_id, session_id, agent_id)
    explicit_scope_keys = {str(key) for key in scope.get("_explicit_scope_keys", []) if isinstance(key, str)}
    explicit_scope_keys.update(str(key) for key in scope.keys())
    enriched = {
        **scope,
        "account_id": account_id,
        "tenant_id": tenant_id,
        "tenant_hash": hashes["tenant_hash"],
        "scope_key": hashes["scope_key"],
        "_explicit_scope_keys": sorted(explicit_scope_keys),
    }
    if user_id:
        enriched["user_id"] = user_id
        enriched["user_hash"] = hashes["user_hash"]
    if session_id:
        enriched["session_id"] = session_id
        enriched["session_hash"] = hashes["session_hash"]
    if agent_id:
        enriched["agent_id"] = agent_id
        enriched["agent_hash"] = hashes["agent_hash"]
    return enriched


def selected_ref_class(ref: Json) -> str:
    raw = str(ref.get("context_class") or ref.get("ref_type") or ref.get("type") or "").lower()
    if "entity" in raw:
        return "entity"
    if "segment" in raw:
        return "segment"
    if "summary" in raw:
        return "summary"
    if "resource" in raw or "chunk" in raw:
        return "resource"
    if "skill" in raw:
        return "skill"
    if "event" in raw:
        return "event"
    return raw or "ref"


def selected_ref_stable_key(ref: Json) -> str:
    ref_class = selected_ref_class(ref)
    stable_id = (
        ref.get("source_ref")
        or ref.get("context_event_key")
        or ref.get("summary_key")
        or ref.get("entity_name")
        or ref.get("resource_id")
        or ref.get("skill_id")
        or ref.get("ref_hash")
        or ref.get("event_id_hash")
        or ref.get("entity_hash")
        or ref.get("chunk_hash")
    )
    if stable_id is not None:
        return f"{ref_class}:{stable_id}"
    text = str(ref.get("text") or ref.get("summary_text") or ref.get("state") or "")
    return f"{ref_class}:text:{stable_hash(text)}"


def compact_native_selected_refs(
    selected_refs: list[Json],
    *,
    max_total: int = 4,
    max_text_chars: int = 480,
) -> tuple[list[Json], int]:
    """Deduplicate and cap already-selected native refs without Python scans."""

    if not selected_refs:
        return [], 0
    per_class_limit = {
        "entity": 1,
        "event": 1,
        "segment": 1,
        "summary": 1,
        "resource": 1,
        "skill": 1,
        "ref": 1,
    }
    selected: list[Json] = []
    seen: set[str] = set()
    class_counts: dict[str, int] = {}
    dropped = 0
    for ref in selected_refs:
        if not isinstance(ref, dict):
            dropped += 1
            continue
        ref_class = selected_ref_class(ref)
        key = selected_ref_stable_key(ref)
        limit = per_class_limit.get(ref_class, 1)
        if key in seen or class_counts.get(ref_class, 0) >= limit or len(selected) >= max_total:
            dropped += 1
            continue
        normalized = dict(ref)
        normalized.setdefault("context_class", ref_class)
        text = normalized.get("text")
        if isinstance(text, str) and len(text) > max_text_chars:
            normalized["text"] = text[: max(0, max_text_chars - 1)].rstrip() + "..."
            normalized["token_estimate"] = max(1, (len(str(normalized["text"])) + 3) // 4)
        selected.append(normalized)
        seen.add(key)
        class_counts[ref_class] = class_counts.get(ref_class, 0) + 1
    return selected, dropped
