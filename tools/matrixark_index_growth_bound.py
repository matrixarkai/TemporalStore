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

0. **Lossless dedup** (``MATRIXARK_DEDUPE_INDEX_POSTINGS``, default ON) -- collapse postings that
   repeat an identical (scope, term, ref set) across time buckets. Costs nothing and shrinks the
   index enough that the budgets below rarely have to bind.

2. **Per-session budget** (``MATRIXARK_MAX_SECONDARY_INDEX_RECORDS_PER_SESSION``, default ``256``,
   0 = unlimited) -- how many postings one session's index may hold.

3. **Per-tenant budget** (``MATRIXARK_MAX_SECONDARY_INDEX_RECORDS_PER_TENANT``, default ``2048``,
   0 = unlimited) -- how many that tenant may hold across all its sessions.

There is deliberately **no store-wide total**. A global budget in a multi-tenant process makes one
tenant's growth evict another tenant's memory -- a cross-tenant side effect, not a memory policy.
Every budget stops at a tenant boundary; the process bounds memory by bounding each tenant.

**They can cost recall, and the eviction order is built to minimize that.** Lever 1 has already
removed every posting that was purely redundant, so whatever a cap evicts on top of it is a live
lookup path. Postings are therefore evicted in RECALL-PRIORITY order, oldest first within each
class:

    event -> segment -> node/other -> entity -> summary

``summary`` postings go LAST because they are exactly what lever 1 leaves behind as the surviving
route to compacted old content -- evicting them by age (the naive rule) throws away the recall path
lever 1 worked to preserve, which is what a measured 5/5 -> 4/5 recall drop at cap=150 looked like.
``entity`` postings are next-to-last for the same reason: an entity is the durable distillation of
an event, so it out-recalls the raw event posting per byte.

Both layers log exactly what they dropped, per class -- never a silent truncation.
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

try:
    from tools.matrixark_tenant_policy import resolve as resolve_tenant_policy
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_tenant_policy import resolve as resolve_tenant_policy

try:
    from tools.matrixark_tenant_policy import tenant_of as tenant_of_scope
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_tenant_policy import tenant_of as tenant_of_scope


LOGGER = logging.getLogger("matrixark.index_growth_bound")

# The bounds run on EVERY serving recompute, so a binding cap would otherwise emit an identical
# WARNING per read. Log only when the eviction picture actually changes -- still no silent
# truncation, just no spam.
_LAST_EVICTION_SIGNATURE: tuple | None = None

# Mirrors ``matrixark_mcp_local_adapter.MEMORY_TOMBSTONE_RECORD_TYPE``. Duplicated (not imported) so
# this module stays leaf-level: the adapter imports it, not the other way round.
MEMORY_TOMBSTONE_RECORD_TYPE = "matrixark_memory_tombstone"
INDEX_COMPACT_TOMBSTONE_KIND = "index_compact"

# Small and ON: the index must not grow linearly with turns. See the docstring for the recall
# trade-off and the eviction priority that limits it. 0 disables either layer.
DEFAULT_MAX_INDEX_RECORDS_PER_SCOPE = 256
DEFAULT_INDEX_HARD_CEILING = 2048

# Lower evicts first. summary postings are the surviving recall path for content lever 1 compacted,
# so they are the last thing given up; entities outrank raw events because they are the distilled,
# longer-lived form of the same fact.
EVICTION_PRIORITY_BY_REF_TYPE = {
    "event": 0,
    "segment": 1,
    "compression": 2,
    "node": 3,
    "": 3,
    "entity": 4,
    "summary": 5,
}


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


def index_compact_on_summary_enabled(scope: Any = None) -> bool:
    """Lever 1 gate, resolved for `scope`'s tenant (tenant override -> env -> default ON)."""
    return bool(resolve_tenant_policy("compact_index_on_summary", scope))


def extract_segments_enabled(scope: Any = None) -> bool:
    """Whether ingest materializes ``context_segment`` rows (default OFF).

    A segment is a restatement of its event -- same text up to a role/index prefix -- so each one
    costs a record, an embedding and a set of index postings to store what the event already holds.
    Entities remain the distillation of a turn; segments are the redundant middle layer. Set
    ``MATRIXARK_EXTRACT_SEGMENTS=1`` to restore them -- or set ``extract_segments`` for one tenant,
    since whether a tenant wants segments is a per-tenant decision, not a process-wide one."""
    return bool(resolve_tenant_policy("extract_segments", scope))


def max_index_records_per_scope(scope: Any = None) -> int:
    """Lever 2 cap for `scope`'s tenant (default 256); ``0`` disables it."""
    return int(resolve_tenant_policy("max_secondary_index_records_per_session", scope))


def index_hard_ceiling(scope: Any = None) -> int:
    """Lever 3 ceiling for `scope`'s tenant (default 2048); ``0`` disables it.

    Enforced PER TENANT, not store-wide: a store-wide ceiling in a multi-tenant process lets a busy
    tenant's postings evict a quiet tenant's index, which is a cross-tenant isolation break, not a
    memory policy. Scopes with no resolvable tenant share one bucket and behave as before."""
    return int(resolve_tenant_policy("max_secondary_index_records_per_tenant", scope))


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


def _tenant_of_scope_key(scope_key: str) -> str:
    """Tenant identity for a posting's scope_key ("" when the key carries no tenant)."""
    return tenant_of_scope(scope_key)


def _sample_scope_for(by_scope: dict[str, list[tuple[int, Json]]], scope_keys: list[str]) -> Any:
    """A representative record scope for policy resolution -- any posting of this tenant will do,
    and a full scope dict resolves a tenant_id where the bare scope_key only has the hash."""
    for scope_key in scope_keys:
        for _position, record in by_scope.get(scope_key, ()):
            scope = record.get("scope") if isinstance(record.get("scope"), dict) else None
            if scope:
                return scope
    return scope_keys[0] if scope_keys else ""


def dedupe_index_postings_enabled() -> bool:
    """Lever 0 gate (default ON). Lossless, so it runs before any lever that can cost recall."""
    return _env_flag("MATRIXARK_DEDUPE_INDEX_POSTINGS", True)


def dedupe_index_postings(records: list[Json]) -> list[Json]:
    """Collapse postings that repeat an identical (scope, index_name, ref_type, ref set).

    Postings are keyed by time bucket, so every summary refresh and every entity re-stamp mints a
    NEW posting row carrying the same index term pointing at the same refs. Measured on a 30-turn
    store: entity postings 180 rows for 30 distinct terms (83% duplicates), summary postings 191
    rows for 108 (43%). The duplicates buy nothing -- a lookup on that term reaches the same refs
    through any one of them -- so the newest row (freshest timestamp) is kept and the rest dropped.

    Lossless, and therefore the FIRST bound applied: it shrinks the index without touching any
    lookup path, which is what makes a genuinely small per-session budget affordable.

    Returns `records` itself (identity) when disabled or nothing is duplicated."""
    if not dedupe_index_postings_enabled():
        return records
    newest: dict[tuple, int] = {}
    duplicates = 0
    for position, record in enumerate(records):
        if str(record.get("record_type") or "") != "context_index":
            continue
        refs = tuple(sorted(str(ref) for ref in _posting_ref_hashes(record)))
        if not refs:
            continue
        key = (
            str(record.get("scope_key") or ""),
            str(record.get("index_name") or ""),
            str(record.get("ref_type") or ""),
            str(record.get("capability") or ""),
            str(record.get("data_model") or ""),
            refs,
        )
        previous = newest.get(key)
        if previous is None:
            newest[key] = position
            continue
        duplicates += 1
        current_time = _posting_time(record)
        previous_time = _posting_time(records[previous])
        if (current_time, position) >= (previous_time, previous):
            newest[key] = position
    if not duplicates:
        return records
    keep = set(newest.values())
    output: list[Json] = []
    for position, record in enumerate(records):
        if str(record.get("record_type") or "") != "context_index":
            output.append(record)
            continue
        refs = tuple(sorted(str(ref) for ref in _posting_ref_hashes(record)))
        if not refs or position in keep:
            output.append(record)
    return output


def _posting_time(record: Json) -> int:
    try:
        return int(record.get("timestamp_key_ms") or record.get("updated_at_ms") or 0)
    except (TypeError, ValueError):
        return 0


def eviction_class(record: Json) -> int:
    """Recall priority of a posting: lower is given up first. See EVICTION_PRIORITY_BY_REF_TYPE."""
    return EVICTION_PRIORITY_BY_REF_TYPE.get(str(record.get("ref_type") or ""), 3)


def _eviction_sort_key(entry: tuple[int, Json]) -> tuple[int, int, str, int]:
    position, record = entry
    try:
        timestamp = int(record.get("timestamp_key_ms") or record.get("updated_at_ms") or 0)
    except (TypeError, ValueError):
        timestamp = 0
    # (cheapest recall class first, then oldest, then a stable tiebreak so eviction is deterministic
    # across processes / PYTHONHASHSEED -- index_hash is content-derived, position breaks the rest).
    return (eviction_class(record), timestamp, str(record.get("index_hash") or ""), position)


def enforce_secondary_index_bounds(
    records: list[Json],
    *,
    on_drop: Callable[[Json], None] | None = None,
) -> list[Json]:
    """Apply Lever 2 (per-scope cap) then Lever 3 (store-wide ceiling) to `records`.

    Input order is preserved for everything kept. Non-``context_index`` records are never touched.
    Returns `records` itself (identity) when both levers are disabled or nothing is over budget, so
    the disabled path is byte-identical."""
    # Lossless first: drop repeated postings before any budget decides what to give up, so a
    # budget is never spent on duplicates and never evicts a live lookup path it did not have to.
    records = dedupe_index_postings(records)

    postings: list[tuple[int, Json]] = [
        (position, record)
        for position, record in enumerate(records)
        if str(record.get("record_type") or "") == "context_index"
    ]
    if not postings:
        return records

    # Budgets are per TENANT: each tenant's postings are counted, capped and evicted against that
    # tenant's own policy, so one tenant can neither borrow nor consume another's index budget.
    by_scope: dict[str, list[tuple[int, Json]]] = {}
    for entry in postings:
        by_scope.setdefault(str(entry[1].get("scope_key") or ""), []).append(entry)
    by_tenant: dict[str, list[str]] = {}
    for scope_key in by_scope:
        by_tenant.setdefault(_tenant_of_scope_key(scope_key), []).append(scope_key)

    dropped: set[int] = set()
    dropped_by_scope: dict[str, int] = {}
    dropped_by_ceiling = 0
    caps_seen: set[int] = set()
    ceilings_seen: set[int] = set()
    for tenant, scope_keys in by_tenant.items():
        sample_scope = _sample_scope_for(by_scope, scope_keys)
        per_scope_cap = max_index_records_per_scope(sample_scope)
        ceiling = index_hard_ceiling(sample_scope)
        caps_seen.add(per_scope_cap)
        ceilings_seen.add(ceiling)
        if per_scope_cap <= 0 and ceiling <= 0:
            continue
        if per_scope_cap > 0:
            for scope_key in scope_keys:
                entries = by_scope[scope_key]
                overflow = len(entries) - per_scope_cap
                if overflow <= 0:
                    continue
                for position, _record in sorted(entries, key=_eviction_sort_key)[:overflow]:
                    dropped.add(position)
                dropped_by_scope[scope_key] = overflow
        if ceiling > 0:
            tenant_entries = [entry for scope_key in scope_keys for entry in by_scope[scope_key]]
            survivors = [entry for entry in tenant_entries if entry[0] not in dropped]
            overflow = len(survivors) - ceiling
            if overflow > 0:
                for position, _record in sorted(survivors, key=_eviction_sort_key)[:overflow]:
                    dropped.add(position)
                dropped_by_ceiling += overflow
    per_scope_cap = max(caps_seen) if caps_seen else 0
    ceiling = max(ceilings_seen) if ceilings_seen else 0

    if not dropped:
        return records

    # No silent truncation: say exactly how much was dropped, by which lever, and -- because the
    # recall cost depends entirely on WHICH postings went -- by ref_type.
    dropped_by_ref_type: dict[str, int] = {}
    for position, record in postings:
        if position in dropped:
            key = str(record.get("ref_type") or "")
            dropped_by_ref_type[key] = dropped_by_ref_type.get(key, 0) + 1
    global _LAST_EVICTION_SIGNATURE
    signature = (
        len(dropped),
        per_scope_cap,
        ceiling,
        tuple(sorted(dropped_by_scope.items())),
        dropped_by_ceiling,
        tuple(sorted(dropped_by_ref_type.items())),
    )
    if signature != _LAST_EVICTION_SIGNATURE:
        _LAST_EVICTION_SIGNATURE = signature
        LOGGER.warning(
            "secondary_index_bound_evicted total=%d per_scope_cap=%d ceiling=%d by_scope=%s by_ceiling=%d by_ref_type=%s",
            len(dropped),
            per_scope_cap,
            ceiling,
            dropped_by_scope,
            dropped_by_ceiling,
            dropped_by_ref_type,
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
