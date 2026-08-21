#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Collapse re-stamped async-pipeline task rows, and (optionally) age out their diagnostic payload.

A resident-memory census (deep ``sizeof``, shared objects charged once, 25-turn workload) put
``matrixark_async_pipeline_task`` at **26-30% of everything the process holds** -- the largest record
type in the store, ahead of embeddings. The reason is not that the rows are individually huge: it is
that the SAME task state is re-appended over and over. Measured on that store: **193 rows for 25
distinct ``task_hash`` values** -- 168 of them re-stamps of ``summary_completed``, one per task per
summary refresh.

## Lever A -- collapse re-stamps (``MATRIXARK_COLLAPSE_PIPELINE_TASK_ROWS``, default ON)

Keep, per ``(task_hash, status)``, the NEWEST row by time and the newest by log position; drop the
rest. Every distinct state a task passed through survives -- only duplicate stampings of a state the
task is already recorded as having reached are removed.

That specific rule (rather than a plain latest-value collapse on ``task_hash``) is forced by the
consumers, which disagree about what "latest" means:

* ``matrixark_mcp_async_readiness.latest_async_pipeline_rows`` -- max by (8-status rank, time)
* ``matrixark_mcp_dashboard`` / ``matrixark_mcp_recovery`` -- max by (3-status rank, time)
* ``drain_due_idle_session_commits`` -- max by LOG POSITION, and requires that row to be
  ``idle_commit_scheduled``
* ``matrixark_local_adapter_retrieve`` -- scans for ANY row with ``status == "extraction_committed"``,
  not the latest one, to decide which events have been extracted

A latest-value collapse on ``task_hash`` alone would keep one row -- typically ``summary_completed``
-- and silently empty retrieve's ``extraction_committed`` set. Keeping the newest row per status
preserves every one of those four answers exactly: each rank rule's winner is the newest row of its
winning status; the positional rule's winner is kept explicitly; and every status stays present.

## Lever B -- age out diagnostics (``MATRIXARK_SLIM_TERMINAL_PIPELINE_TASKS``, default OFF)

Strips the dashboard-only payload (``memory_layers_written``, stage lists, lineage count maps) from
finished tasks beyond the newest N per scope, marking them ``detail_slimmed``. Measured at only
**-0.6% of resident memory** once Lever A has run, so it is off by default: the bytes were in the
duplicate ROWS, not in the surviving rows' fields. Kept because it is a legitimate knob for a store
that keeps very long task histories, but it is not the win and is not pretending to be.

Both levers are serving-side only: the JSONL log keeps every row, so the durable audit trail is
intact and either flag can be flipped back without a rewrite.
"""

from __future__ import annotations

import json

import os
from typing import Any

Json = dict[str, Any]

PIPELINE_TASK_RECORD_TYPE = "matrixark_async_pipeline_task"

# A task in one of these states has nothing left to run.
FINISHED_STATUSES = frozenset({
    "extraction_committed",
    "idle_commit_committed",
    "idle_commit_skipped",
    "threshold_commit_committed",
    "committed",
    "completed",
    "finalized",
    "summary_completed",
    "failed",
    "error",
    "timeout",
    "skipped",
})

SLIMMED_FIELDS = (
    "memory_layers_written",
    "stages",
    "completed_stages",
    "remaining_stages",
    "summary_node_path",
    "generated_summary_types",
    "source_roles",
    "source_role_counts",
    "source_hook_types",
    "source_hook_type_counts",
    "source_codex_events",
    "source_codex_event_counts",
    "source_memory_selection_policies",
    "source_memory_selection_policy_counts",
    "source_profile_promotion_policies",
    "source_profile_promotion_blockers",
    "source_entity_hashes",
    "source_session_ids",
)

DEFAULT_DETAIL_RETAIN_PER_SCOPE = 50


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
        return max(0, int(str(raw).strip()))
    except (TypeError, ValueError):
        return default


def collapse_pipeline_task_rows_enabled() -> bool:
    return _env_flag("MATRIXARK_COLLAPSE_PIPELINE_TASK_ROWS", True)


def slim_terminal_pipeline_tasks_enabled() -> bool:
    """Lever B is opt-in: measured at -0.6% once Lever A has collapsed the duplicate rows."""
    return _env_flag("MATRIXARK_SLIM_TERMINAL_PIPELINE_TASKS", False)


def pipeline_task_detail_retain_per_scope() -> int:
    return _env_int("MATRIXARK_PIPELINE_TASK_DETAIL_RETAIN_PER_SCOPE", DEFAULT_DETAIL_RETAIN_PER_SCOPE)


def _task_identity(record: Json) -> str:
    identity = record.get("task_hash")
    if identity in (None, ""):
        identity = record.get("event_id_hash")
    return str(identity if identity not in (None, "") else "")


def _task_time(record: Json) -> int:
    try:
        return int(record.get("updated_at_ms") or record.get("created_at_ms") or 0)
    except (TypeError, ValueError):
        return 0


def collapse_pipeline_task_rows(records: list[Json]) -> list[Json]:
    """Drop re-stamps of a pipeline-task state, keeping every distinct (task, status) it reached.

    Returns `records` itself (identity) when the flag is off or nothing is duplicated."""
    if not collapse_pipeline_task_rows_enabled():
        return records
    newest_by_time: dict[tuple[str, str], int] = {}
    newest_by_position: dict[tuple[str, str], int] = {}
    duplicates = 0
    for position, record in enumerate(records):
        if str(record.get("record_type") or "") != PIPELINE_TASK_RECORD_TYPE:
            continue
        identity = _task_identity(record)
        if not identity:
            continue  # unkeyable row: never dropped
        key = (identity, str(record.get("status") or ""))
        if key in newest_by_time:
            duplicates += 1
        else:
            newest_by_time[key] = position
            newest_by_position[key] = position
            continue
        if (_task_time(record), position) > (_task_time(records[newest_by_time[key]]), newest_by_time[key]):
            newest_by_time[key] = position
        newest_by_position[key] = position  # enumeration order == log order
    if not duplicates:
        return records
    keep = set(newest_by_time.values()) | set(newest_by_position.values())
    output: list[Json] = []
    for position, record in enumerate(records):
        if str(record.get("record_type") or "") != PIPELINE_TASK_RECORD_TYPE:
            output.append(record)
            continue
        if not _task_identity(record) or position in keep:
            output.append(record)
    return output


def _task_scope_key(record: Json) -> str:
    scope_key = record.get("scope_key")
    if scope_key:
        return str(scope_key)
    scope = record.get("scope")
    if isinstance(scope, dict):
        return str(scope.get("scope_key") or f"{scope.get('tenant_id')}/{scope.get('user_id')}")
    return ""


def _recency_key(entry: tuple[int, Json]) -> tuple[int, int]:
    position, record = entry
    return (_task_time(record), position)


def slim_terminal_pipeline_tasks(records: list[Json]) -> list[Json]:
    """Lever B: strip the dashboard-only payload from aged-out finished tasks (identity when off)."""
    if not slim_terminal_pipeline_tasks_enabled():
        return records
    retain = pipeline_task_detail_retain_per_scope()
    finished: list[tuple[int, Json]] = []
    for position, record in enumerate(records):
        if str(record.get("record_type") or "") != PIPELINE_TASK_RECORD_TYPE:
            continue
        if str(record.get("status") or "") not in FINISHED_STATUSES:
            continue
        if record.get("detail_slimmed"):
            continue
        if any(field in record for field in SLIMMED_FIELDS):
            finished.append((position, record))
    if not finished:
        return records
    by_scope: dict[str, list[tuple[int, Json]]] = {}
    for entry in finished:
        by_scope.setdefault(_task_scope_key(entry[1]), []).append(entry)
    slim_positions: set[int] = set()
    for entries in by_scope.values():
        if retain <= 0:
            slim_positions.update(position for position, _record in entries)
            continue
        for position, _record in sorted(entries, key=_recency_key, reverse=True)[retain:]:
            slim_positions.add(position)
    if not slim_positions:
        return records
    output: list[Json] = []
    for position, record in enumerate(records):
        if position not in slim_positions:
            output.append(record)
            continue
        slimmed = {key: value for key, value in record.items() if key not in SLIMMED_FIELDS}
        slimmed["detail_slimmed"] = True
        output.append(slimmed)
    return output


AUDIT_PAYLOAD_RECORD_TYPES = frozenset({
    "context_extraction_audit",
    "context_batch_commit",
    "context_entity_update_audit",
    "matrixark_audit_log",
})

# Dropped from an aged-out audit row. Every field the ingest/retrieval paths actually read is
# excluded: record_type, scope, scope_key, status, the *_hash identities, and the timestamps.
# What goes is what only the dashboards render.
AUDIT_PAYLOAD_FIELDS = (
    "outputs",
    "schema",
    "profile_promotion_summary",
    "memory_layers_written",
    "summary_refresh",
    "trigger_evidence",
    "inputs",
    "response",
    "source_refs",
    "extraction_debug",
)


def _audit_scope_key(record: Json) -> str:
    scope_key = record.get("scope_key")
    if scope_key:
        return str(scope_key)
    scope = record.get("scope")
    if isinstance(scope, dict):
        return str(scope.get("scope_key") or f"{scope.get('tenant_id')}/{scope.get('user_id')}")
    return ""


def slim_audit_payloads(records: list[Json]) -> list[Json]:
    """Strip diagnostic payloads from audit/commit rows beyond the newest N per scope.

    These rows are load-bearing for their IDENTITY -- session-commit and retrieval look them up by
    record_type + scope, and the dashboards read the payload -- so the row itself always survives and
    only the payload ages out. A field-level census put these payloads at ~11% of resident memory
    (context_extraction_audit.outputs alone was 5.6%), with no retention policy of any kind: unlike
    the debug-record knobs, which default OFF, these are written unconditionally.

    Returns `records` itself (identity) when retention is disabled or nothing qualifies."""
    try:
        from tools.matrixark_index_growth_bound import audit_payload_retain_per_scope
    except ImportError:  # Direct script execution from tools/.
        from matrixark_index_growth_bound import audit_payload_retain_per_scope

    candidates: list[tuple[int, Json]] = []
    for position, record in enumerate(records):
        if str(record.get("record_type") or "") not in AUDIT_PAYLOAD_RECORD_TYPES:
            continue
        if record.get("payload_slimmed"):
            continue
        if any(field in record for field in AUDIT_PAYLOAD_FIELDS):
            candidates.append((position, record))
    if not candidates:
        return records

    by_scope: dict[str, list[tuple[int, Json]]] = {}
    for entry in candidates:
        by_scope.setdefault(_audit_scope_key(entry[1]), []).append(entry)

    slim_positions: set[int] = set()
    for scope_key, entries in by_scope.items():
        retain = audit_payload_retain_per_scope(entries[0][1].get("scope") or scope_key)
        if retain <= 0:
            continue
        for position, _record in sorted(entries, key=_recency_key, reverse=True)[retain:]:
            slim_positions.add(position)
    if not slim_positions:
        return records

    output: list[Json] = []
    for position, record in enumerate(records):
        if position not in slim_positions:
            output.append(record)
            continue
        slimmed = {key: value for key, value in record.items() if key not in AUDIT_PAYLOAD_FIELDS}
        slimmed["payload_slimmed"] = True
        output.append(slimmed)
    return output


# Near-constant dicts stamped on almost every record. A store holds a handful of distinct values
# (measured: 3 storage_route, 12 storage_options, 3 scope) across hundreds of records.
SHARED_VALUE_FIELDS = ("storage_route", "storage_options", "scope", "access_scope", "storage_options_ref")


def canonicalize_shared_values(records: list[Json]) -> list[Json]:
    """Point every record at ONE object per distinct routing/scope value.

    Interned expansion canonicalizes what it expands, but records written inline never pass through
    it, and ``scope`` / ``access_scope`` are not part of the intern bundle at all -- so the serving
    view still held 50 objects for 3 route values and 44 for 3 scopes. This is the read-side sweep
    that closes both gaps.

    The records reaching this stage are NOT private copies: the upstream compaction stages pass a
    record straight through when they have nothing to change, so they are the adapter's own cached
    objects. Writing the shared value onto them therefore mutates cached state, which measurably
    perturbed the store (a module that failed 0/12 runs on main failed 6/12 with an in-place sweep,
    via background writes landing after teardown). So this is copy-on-write: a record whose value is
    being re-pointed is shallow-copied first, and untouched records are returned by identity.

    The VALUES are shared across the returned records, so a caller that needs to change one must copy
    it first -- which is what ``attach_context_placement`` already does (``route = dict(route)``).
    That is the invariant this function depends on; breaking it would let one record's placement leak
    into every other record sharing the value. (Verified with a tripwire ``dict`` subclass over the
    gateway/admin path: zero in-place writes to any shared value.)"""
    try:
        from tools.matrixark_index_growth_bound import share_serving_values_enabled
    except ImportError:  # Direct script execution from tools/.
        from matrixark_index_growth_bound import share_serving_values_enabled
    if not share_serving_values_enabled(None):
        return records

    cache: dict[tuple[str, str], Any] = {}
    output: list[Json] = []
    for record in records:
        if not isinstance(record, dict):
            output.append(record)
            continue
        replacements: dict[str, Any] = {}
        for field in SHARED_VALUE_FIELDS:
            value = record.get(field)
            if not isinstance(value, dict) or not value:
                continue
            try:
                fingerprint = (field, json.dumps(value, sort_keys=True, separators=(",", ":"), default=str))
            except (TypeError, ValueError):
                continue
            existing = cache.get(fingerprint)
            if existing is None:
                cache[fingerprint] = value
            elif existing is not value:
                replacements[field] = existing
        if not replacements:
            output.append(record)
            continue
        copied = dict(record)
        copied.update(replacements)
        output.append(copied)
    return output


def bound_pipeline_task_footprint(records: list[Json]) -> list[Json]:
    """Lever A, lever B, audit-payload retention, then value sharing -- the serving pipeline's entry."""
    return canonicalize_shared_values(
        slim_audit_payloads(slim_terminal_pipeline_tasks(collapse_pipeline_task_rows(records)))
    )


def pipeline_task_footprint_stats(records: list[Json]) -> Json:
    total = finished = slimmed = 0
    identities: set[str] = set()
    states: set[tuple[str, str]] = set()
    for record in records:
        if str(record.get("record_type") or "") != PIPELINE_TASK_RECORD_TYPE:
            continue
        total += 1
        identity = _task_identity(record)
        identities.add(identity)
        states.add((identity, str(record.get("status") or "")))
        if str(record.get("status") or "") in FINISHED_STATUSES:
            finished += 1
        if record.get("detail_slimmed"):
            slimmed += 1
    return {
        "pipeline_tasks": total,
        "distinct_tasks": len(identities),
        "distinct_task_states": len(states),
        "finished": finished,
        "detail_slimmed": slimmed,
        "collapse_enabled": collapse_pipeline_task_rows_enabled(),
        "slim_enabled": slim_terminal_pipeline_tasks_enabled(),
    }
