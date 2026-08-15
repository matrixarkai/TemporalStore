#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Append materialization helpers for TemporalStore-backed MatrixArk adapters."""

from __future__ import annotations

import json
import time
from typing import Any

try:
    from tools.matrixark_mcp_core import Json, compact_latest_context_state_records, stable_hash
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_core import Json, compact_latest_context_state_records, stable_hash


def materialize_appended_records_locked(
    target: Any,
    *,
    prior_entry_count: int,
    new_entry_count: int,
    records: list[Json],
) -> None:
    """Refresh process-local materialized views after native latest-state writes."""
    if not records:
        return
    try:
        target._entry_count_cache = max(int(new_entry_count or 0), int(prior_entry_count or 0))
    except Exception:
        pass
    if getattr(target, "_records_cache", None) is not None:
        try:
            target._records_cache.extend(records)
            target._put_direct_record_cache(len(target._records_cache), target._records_cache)
        except Exception:
            pass
    try:
        target._prune_retrieval_candidate_cache(
            getattr(target, "_entry_count_cache", None) or int(new_entry_count or 0)
        )
    except Exception:
        pass
    try:
        target._update_latest_entity_cache(records)
    except Exception:
        pass


def append_many_materialized(target: Any, records: list[Json], *, allow_queue: bool = True) -> None:
    if not records:
        return
    records = compact_latest_context_state_records(records)
    latest_state_entries, append_records_for_log = target._split_compacted_latest_context_state(records)
    target._validate_storage_routes_available(records)
    if latest_state_entries and not append_records_for_log:
        target._hset_many_with_backoff(latest_state_entries)
        materialize_appended_records_locked(
            target,
            prior_entry_count=getattr(target, "_entry_count_cache", None) or target._get_count(),
            new_entry_count=getattr(target, "_entry_count_cache", None) or target._get_count(),
            records=records,
        )
        return
    records_to_append = append_records_for_log
    if allow_queue and target._records_can_use_direct_write_queue(records_to_append):
        target._enqueue_direct_write(records)
        return
    started_perf = time.perf_counter()
    with target._records_lock:
        entry_count_cache = getattr(target, "_entry_count_cache", None)
        count = entry_count_cache if entry_count_cache is not None else target._get_count()
        if count <= 0 and target._index_cache is None:
            target._index_cache = target._get_index()
            target._legacy_index_mode = bool(target._index_cache)
        event_time_entries = target._context_event_time_index_entries(records_to_append)
        if target._legacy_index_mode:
            if target._index_cache is None:
                target._index_cache = target._get_index()
            entries: list[Json] = []
            for record in records_to_append:
                payload = json.dumps(record, sort_keys=True, separators=(",", ":"))
                record_id = (
                    f"{len(target._index_cache):020d}:"
                    f"{record.get('record_type', 'record')}:"
                    f"{stable_hash(json.dumps(record, sort_keys=True))}"
                )
                route = record.get("storage_route") if isinstance(record.get("storage_route"), dict) else {}
                entries.append(
                    {"key": target._record_hash_key, "field": record_id, "value": payload, "storage_route": route}
                )
                target._index_cache.append(record_id)
            target._hset_many_with_backoff(latest_state_entries + event_time_entries + entries)
            target._put_string_with_backoff(target._index_key, json.dumps(target._index_cache, separators=(",", ":")))
            target._note_pending_visibility_keys(
                [target._index_key]
                + [str(entry.get("key") or "") for entry in latest_state_entries]
                + [str(entry.get("key") or "") for entry in event_time_entries]
                + [str(entry.get("key") or "") for entry in entries]
            )
            if target._records_cache is not None:
                target._records_cache.extend(records)
                target._put_direct_record_cache(len(target._records_cache), target._records_cache)
            target._update_latest_entity_cache(records)
            elapsed_ms = (time.perf_counter() - started_perf) * 1000.0
            target._observe_append_engine(elapsed_ms)
            target._observe_backend_command(elapsed_ms, records_written=len(records))
            return

        sequence = count
        entries = []
        located_bundles: list[tuple[list[Json], str, str]] = []
        for bundle in target._record_bundles(records):
            record_key, record_id = target._record_location(sequence)
            payload_value: Json
            payload_value = bundle[0] if len(bundle) == 1 else {"record_bundle": bundle}
            payload = json.dumps(payload_value, sort_keys=True, separators=(",", ":"))
            entries.append(
                {
                    "key": record_key,
                    "field": record_id,
                    "value": payload,
                    "storage_route": target._storage_route_for_bundle(bundle),
                }
            )
            located_bundles.append((bundle, record_key, record_id))
            sequence += 1
        native_index_entries = target._native_side_index_entries_for_bundles(located_bundles)
        append_records = getattr(target._client, "matrixark_batch_append_records", None)
        if callable(append_records):
            target._write_with_backoff(
                lambda: append_records(
                    event_time_entries + native_index_entries + entries,
                    count_key=target._count_key,
                    count_value=str(sequence),
                    append_options=target._native_append_options(),
                ),
                op="matrixark_batch_append_records",
            )
            if target._write_throttle_s > 0:
                time.sleep(target._write_throttle_s)
        else:
            target._hset_many_with_backoff(event_time_entries + native_index_entries + entries)
            target._put_string_with_backoff(target._count_key, str(sequence))
        target._note_pending_visibility_keys(
            [target._count_key]
            + [str(entry.get("key") or "") for entry in latest_state_entries]
            + [str(entry.get("key") or "") for entry in event_time_entries]
            + [str(entry.get("key") or "") for entry in native_index_entries]
            + [str(entry.get("key") or "") for entry in entries]
        )
        target._entry_count_cache = sequence
        if target._records_cache is not None:
            target._records_cache.extend(records)
            target._put_direct_record_cache(target._entry_count_cache, target._records_cache)
        target._prune_retrieval_candidate_cache(sequence)
        target._update_latest_entity_cache(records)
        elapsed_ms = (time.perf_counter() - started_perf) * 1000.0
        target._observe_append_engine(elapsed_ms)
        target._observe_backend_command(elapsed_ms, records_written=len(records))
