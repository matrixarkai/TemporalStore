#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Secondary-index growth bound: temporal compaction, per-scope cap, hard ceiling.

Even after term/posting pruning, every ingested turn appends secondary-index postings, so a
subject's TOTAL ``context_index`` footprint grows LINEARLY over its lifetime. This module bounds
that growth in three layers, applied in this order:

1. **Temporal index compaction** (``MATRIXARK_INDEX_COMPACT_ON_SUMMARY``, default ON) -- principled
   and recall-preserving. Once an event has been rolled up into an L0/L1 ``context_summary``, its
   PER-EVENT postings are redundant with the summary's own postings, so the summary emits an
   ``index_compact`` tombstone naming exactly the events it covers and the serving sweep drops those
   event postings. The index stays dense for recent turns and sparse-summarized for old ones; old
   content stays retrievable through its summary.

2. **Per-scope cap** (``MATRIXARK_MAX_SECONDARY_INDEX_RECORDS_PER_SCOPE``, 0 = unlimited) -- a
   deterministic backstop for a subject whose events were never summarized. Oldest postings evict
   first.

3. **Hard ceiling** (``MATRIXARK_SECONDARY_INDEX_HARD_CEILING``, 0 = unlimited) -- a store-wide
   runaway guard that is never exceeded regardless of the other two.

Layers 2 and 3 CAN drop postings whose events are not summarized, so they log what they dropped --
never a silent truncation.
"""

from __future__ import annotations

import logging
import os
from typing import Any, Callable, Iterable

Json = dict[str, Any]

try:
    from tools.matrixark_mcp_identity import now_ms
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_identity import now_ms


LOGGER = logging.getLogger("matrixark.index_growth_bound")

# Mirrors ``matrixark_mcp_local_adapter.MEMORY_TOMBSTONE_RECORD_TYPE``. Duplicated (not imported) so
# this module stays leaf-level: the adapter imports it, not the other way round.
MEMORY_TOMBSTONE_RECORD_TYPE = "matrixark_memory_tombstone"
INDEX_COMPACT_TOMBSTONE_KIND = "index_compact"

DEFAULT_MAX_INDEX_RECORDS_PER_SCOPE = 5000
DEFAULT_INDEX_HARD_CEILING = 20000


def _env_flag(name: str, default: bool) -> bool:
    raw = os.environ.get(name)
    if raw is None or str(raw).strip() == "":
        return default
    return str(raw).strip().lower() not in {"0", "false", "no", "off"}


def _env_int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None or str(raw).strip() == "":
        return default
    try:
        value = int(str(raw).strip())
    except (TypeError, ValueError):
        return default
    return max(0, value)


def index_compact_on_summary_enabled() -> bool:
    """Lever 1 gate. Read per call so a test (or an operator) can flip it without re-import."""
    return _env_flag("MATRIXARK_INDEX_COMPACT_ON_SUMMARY", True)


def max_index_records_per_scope() -> int:
    """Lever 2 cap; ``0`` disables it."""
    return _env_int("MATRIXARK_MAX_SECONDARY_INDEX_RECORDS_PER_SCOPE", DEFAULT_MAX_INDEX_RECORDS_PER_SCOPE)


def index_hard_ceiling() -> int:
    """Lever 3 store-wide ceiling; ``0`` disables it."""
    return _env_int("MATRIXARK_SECONDARY_INDEX_HARD_CEILING", DEFAULT_INDEX_HARD_CEILING)


def index_compaction_tombstone(
    *,
    source_event_ids: Iterable[Any],
    scope: Json | None = None,
    scope_key: str = "",
    summary_hash: Any = None,
    created_at_ms: Any = None,
) -> Json | None:
    """Build the ``index_compact`` tombstone for the events a summary now covers.

    Returns ``None`` when there is nothing to compact, so the caller appends nothing and a
    compaction-free log stays byte-identical to pre-Lever-1 behavior."""
    targets = sorted({str(value) for value in source_event_ids or () if value not in (None, "")})
    if not targets:
        return None
    tombstone: Json = {
        "record_type": MEMORY_TOMBSTONE_RECORD_TYPE,
        "tombstone_kind": INDEX_COMPACT_TOMBSTONE_KIND,
        "target_ref_ids": targets,
        "target_ref_count": len(targets),
        "tombstone_reason": "summary_rollup",
        "created_at_ms": int(created_at_ms or now_ms()),
    }
    if summary_hash is not None:
        tombstone["superseded_by"] = summary_hash
    if scope:
        tombstone["scope"] = scope
        resolved_scope_key = str(scope.get("scope_key") or "") if isinstance(scope, dict) else ""
        if resolved_scope_key:
            tombstone["scope_key"] = resolved_scope_key
    if scope_key:
        tombstone["scope_key"] = scope_key
    return tombstone


def _posting_ref_hashes(record: Json) -> list[Any]:
    refs = record.get("ref_hashes")
    if isinstance(refs, list) and refs:
        return [ref for ref in refs if ref is not None]
    legacy = record.get("ref_hash")
    return [legacy] if legacy not in (None, "") else []


def index_compact_tombstone_kills_record(tombstone: Json, record: Json) -> bool:
    """True when an ``index_compact`` tombstone removes `record`.

    Only ``context_index`` postings whose ``ref_type`` is ``event`` are ever matched, and only when
    EVERY ref the posting carries is covered by the summary. A coalesced posting that still points at
    an unsummarized event survives intact -- partial compaction is correct, partial loss is not."""
    if str(tombstone.get("tombstone_kind") or "") != INDEX_COMPACT_TOMBSTONE_KIND:
        return False
    if str(record.get("record_type") or "") != "context_index":
        return False
    if str(record.get("ref_type") or "") != "event":
        return False
    targets = tombstone.get("target_ref_ids")
    if not targets:
        return False
    target_set = targets if isinstance(targets, (set, frozenset)) else {str(value) for value in targets}
    tombstone_scope_key = str(tombstone.get("scope_key") or "")
    record_scope_key = str(record.get("scope_key") or "")
    if tombstone_scope_key and record_scope_key and tombstone_scope_key != record_scope_key:
        return False
    refs = _posting_ref_hashes(record)
    if not refs:
        return False
    return all(str(ref) in target_set for ref in refs)


def _eviction_sort_key(entry: tuple[int, Json]) -> tuple[int, str, int]:
    position, record = entry
    try:
        timestamp = int(record.get("timestamp_key_ms") or record.get("updated_at_ms") or 0)
    except (TypeError, ValueError):
        timestamp = 0
    # (oldest first, then a stable tiebreak so eviction is deterministic across processes /
    # PYTHONHASHSEED -- index_hash is content-derived, position is the final tiebreak).
    return (timestamp, str(record.get("index_hash") or ""), position)


def enforce_secondary_index_bounds(
    records: list[Json],
    *,
    on_drop: Callable[[Json], None] | None = None,
) -> list[Json]:
    """Apply Lever 2 (per-scope cap) then Lever 3 (store-wide ceiling) to `records`.

    Input order is preserved for everything kept. Non-``context_index`` records are never touched.
    Returns `records` itself (identity) when both levers are disabled or nothing is over budget, so
    the disabled path is byte-identical."""
    per_scope_cap = max_index_records_per_scope()
    ceiling = index_hard_ceiling()
    if per_scope_cap <= 0 and ceiling <= 0:
        return records

    postings: list[tuple[int, Json]] = [
        (position, record)
        for position, record in enumerate(records)
        if str(record.get("record_type") or "") == "context_index"
    ]
    if not postings:
        return records

    dropped: set[int] = set()
    dropped_by_scope: dict[str, int] = {}
    if per_scope_cap > 0:
        by_scope: dict[str, list[tuple[int, Json]]] = {}
        for entry in postings:
            by_scope.setdefault(str(entry[1].get("scope_key") or ""), []).append(entry)
        for scope_key, entries in by_scope.items():
            overflow = len(entries) - per_scope_cap
            if overflow <= 0:
                continue
            for position, _record in sorted(entries, key=_eviction_sort_key)[:overflow]:
                dropped.add(position)
            dropped_by_scope[scope_key] = overflow

    dropped_by_ceiling = 0
    if ceiling > 0:
        survivors = [entry for entry in postings if entry[0] not in dropped]
        overflow = len(survivors) - ceiling
        if overflow > 0:
            for position, _record in sorted(survivors, key=_eviction_sort_key)[:overflow]:
                dropped.add(position)
            dropped_by_ceiling = overflow

    if not dropped:
        return records

    # No silent truncation: say exactly how much was dropped and by which lever.
    LOGGER.warning(
        "secondary_index_bound_evicted total=%d per_scope_cap=%d ceiling=%d by_scope=%s by_ceiling=%d",
        len(dropped),
        per_scope_cap,
        ceiling,
        dropped_by_scope,
        dropped_by_ceiling,
    )
    kept: list[Json] = []
    for position, record in enumerate(records):
        if position in dropped:
            if on_drop is not None:
                on_drop(record)
            continue
        kept.append(record)
    return kept


def secondary_index_bound_stats(records: list[Json]) -> Json:
    """Observability helper: live posting counts by scope and ref_type (used by the tests/harness)."""
    total = 0
    by_scope: dict[str, int] = {}
    by_ref_type: dict[str, int] = {}
    for record in records:
        if str(record.get("record_type") or "") != "context_index":
            continue
        total += 1
        scope_key = str(record.get("scope_key") or "")
        by_scope[scope_key] = by_scope.get(scope_key, 0) + 1
        ref_type = str(record.get("ref_type") or "")
        by_ref_type[ref_type] = by_ref_type.get(ref_type, 0) + 1
    return {
        "context_index_total": total,
        "by_scope": by_scope,
        "by_ref_type": by_ref_type,
        "per_scope_cap": max_index_records_per_scope(),
        "hard_ceiling": index_hard_ceiling(),
        "compact_on_summary": index_compact_on_summary_enabled(),
    }


def records_contain_index_compaction(records: list[Json]) -> bool:
    for record in records:
        if (
            str(record.get("record_type") or "") == MEMORY_TOMBSTONE_RECORD_TYPE
            and str(record.get("tombstone_kind") or "") == INDEX_COMPACT_TOMBSTONE_KIND
        ):
            return True
    return False


def sweep_index_compaction(records: list[Json]) -> list[Json]:
    """Apply every ``index_compact`` tombstone in ONE reverse pass and strip the tombstones.

    Semantically identical to running ``index_compact_tombstone_kills_record`` for each tombstone
    against each earlier record (order-aware: a tombstone only removes records that PRECEDE it), but
    O(n) instead of O(tombstones x records) -- and summaries emit one of these per rollup, so the
    quadratic form would dominate a long-lived store's serving recompute.

    Returns `records` unchanged (identity) when the log carries no compaction tombstone."""
    if not records_contain_index_compaction(records):
        return records
    targets_any: set[str] = set()
    targets_by_scope: dict[str, set[str]] = {}
    kept_reversed: list[Json] = []
    for record in reversed(records):
        record_type = str(record.get("record_type") or "")
        if (
            record_type == MEMORY_TOMBSTONE_RECORD_TYPE
            and str(record.get("tombstone_kind") or "") == INDEX_COMPACT_TOMBSTONE_KIND
        ):
            ids = {str(value) for value in (record.get("target_ref_ids") or ()) if value not in (None, "")}
            scope_key = str(record.get("scope_key") or "")
            if scope_key:
                targets_by_scope.setdefault(scope_key, set()).update(ids)
            else:
                targets_any.update(ids)
            continue  # the tombstone itself never serves
        if record_type == "context_index" and str(record.get("ref_type") or "") == "event":
            refs = _posting_ref_hashes(record)
            if refs:
                record_scope_key = str(record.get("scope_key") or "")
                if record_scope_key:
                    visible = targets_any | targets_by_scope.get(record_scope_key, set())
                else:
                    # A posting with no scope cannot contradict any tombstone's scope, so every
                    # target is in play (mirrors the per-tombstone scope guard).
                    visible = set(targets_any)
                    for scoped in targets_by_scope.values():
                        visible |= scoped
                if visible and all(str(ref) in visible for ref in refs):
                    continue
        kept_reversed.append(record)
    kept_reversed.reverse()
    return kept_reversed
