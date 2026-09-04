#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Append materialization helpers for TemporalStore-backed MatrixArk adapters."""

from __future__ import annotations

import os

import json
import time
from typing import Any

try:
    from tools.matrixark_mcp_core import Json, compact_latest_context_state_records, stable_hash
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_core import Json, compact_latest_context_state_records, stable_hash


# Keys of `storage_route` that are NOT recoverable from `storage_options`: where this record was
# placed. Everything else `canonical_storage_route` produces is a pure function of the options,
# which are stored on the record already.
_ROUTE_KEYS_WORTH_STORING = (
    "placement_key",
    "placement_hash",
)
# What was dropped and why: `routing_key` and `partition_key` are set to `placement_key` and
# `colocation_group` to a constant, so all three are copies of something already here. Measured on
# one add, they cost 25 index postings x ~280 bytes -- about 7 KB per add to say the same thing
# three more times. The one place that reads them consults `placement_key` first and falls through
# to `routing_key` / `partition_key` only when it is absent, which it is not.
def _placement_is_derivable(record: Json) -> bool:
    """Can this record's placement be rebuilt from what it already carries?

    Two ways, and an index posting only has the second:

    * the record states `placement_key` and `placement_hash` at its top level, which
      `attach_context_placement` writes beside the very route being dropped; or
    * it states `scope_key` and `node_hash`, from which the pair is a pure function --
      `placement_key = f"context:{scope_key}:node={node_hash}"` and `placement_hash =
      stable_hash(placement_key)`.

    Index postings take the second path and are where this matters most: measured over 300
    ingests, an add writes ~10 postings and each carried 159 bytes of route and 58 of
    `posting_policy` -- a constant with ONE distinct value in the whole store, and no reader.
    """
    if record.get("placement_key") and record.get("placement_hash"):
        return True
    if not record.get("scope_key"):
        return False
    node_hash = record.get("node_hash")
    try:
        return int(node_hash or 0) != 0
    except (TypeError, ValueError):
        return False


# --------------------------------------------------------------------------------------------- #
# Backend metadata interning (write side gated, read side always on).
#
# `storage_options` is the largest field in the store -- 2,139 KB, 13.2% of all record bytes, with
# NINE distinct values across 3,610 rows. `INTERN_METADATA_FIELDS` already names it and the local
# JSONL codec already tokenises it; the primary store does not, so the optimisation landed on the
# 4-copy/7-day mirror instead of the durable store.
#
# The write side is gated OFF. The read side always expands, so a store written with the flag ON
# still reads correctly if the flag is later turned OFF -- the same asymmetry the JSONL codec uses.
#
# NOT yet established, which is why the default is OFF: the JSONL codec's crash-safety argument is
# "the dict record precedes the data record under the event-log lock". The backend has no such
# ordering; it would rely on the engine's batch append being atomic, which is a different claim and
# is unverified. Do not flip this default until that is settled.
# --------------------------------------------------------------------------------------------- #
BACKEND_INTERN_METADATA = os.environ.get(
    "MATRIXARK_INTERN_BACKEND_METADATA", "0").strip().lower() in {"1", "true", "yes", "on"}
BACKEND_INTERN_FIELDS = ("storage_options",)
BACKEND_INTERN_TOKEN_KEY = "_bi"
BACKEND_INTERN_DICT_RECORD_TYPE = "matrixark_backend_intern_dict"


def _backend_intern_token(value: Any) -> str:
    return str(stable_hash(json.dumps(value, sort_keys=True, separators=(",", ":"))))


def backend_intern_records(records: list[Json], emitted: set[str]) -> list[Json]:
    """Replace the interned fields with a token, emitting any new sidecar record FIRST.

    Returns the input unchanged when the flag is off, so the write path is byte-identical to today.
    """
    if not BACKEND_INTERN_METADATA:
        return records
    dict_records: list[Json] = []
    encoded: list[Json] = []
    for record in records:
        if not isinstance(record, dict) or record.get("record_type") == BACKEND_INTERN_DICT_RECORD_TYPE:
            encoded.append(record)
            continue
        present = {f: record[f] for f in BACKEND_INTERN_FIELDS if f in record}
        if not present:
            encoded.append(record)
            continue
        out = dict(record)
        tokens: Json = {}
        for field, value in present.items():
            token = _backend_intern_token(value)
            tokens[field] = token
            if token not in emitted:
                emitted.add(token)
                dict_records.append({
                    "record_type": BACKEND_INTERN_DICT_RECORD_TYPE,
                    "bi_field": field,
                    "bi_token": token,
                    "bi_value": value,
                })
            out.pop(field, None)
        out[BACKEND_INTERN_TOKEN_KEY] = tokens
        encoded.append(out)
    return dict_records + encoded


_BACKEND_INTERN_TABLE: dict[str, Any] = {}


def backend_intern_learn(records: list[Json]) -> None:
    """Absorb any sidecar records into the token table."""
    for record in records:
        if isinstance(record, dict) and record.get("record_type") == BACKEND_INTERN_DICT_RECORD_TYPE:
            token = record.get("bi_token")
            if isinstance(token, str):
                _BACKEND_INTERN_TABLE[token] = record.get("bi_value")


def backend_expand_records(records: list[Json]) -> list[Json]:
    """Put the interned fields back. ALWAYS runs, flag or no flag.

    A record with no token key is returned untouched, so a store written before this existed -- or
    with the flag off -- expands as a no-op.
    """
    if not records:
        return records
    backend_intern_learn(records)
    out: list[Json] = []
    for record in records:
        if not isinstance(record, dict):
            out.append(record)
            continue
        if record.get("record_type") == BACKEND_INTERN_DICT_RECORD_TYPE:
            continue        # sidecars are storage, not data
        tokens = record.get(BACKEND_INTERN_TOKEN_KEY)
        if not isinstance(tokens, dict) or not tokens:
            out.append(record)
            continue
        expanded = dict(record)
        expanded.pop(BACKEND_INTERN_TOKEN_KEY, None)
        for field, token in tokens.items():
            if token in _BACKEND_INTERN_TABLE:
                expanded[field] = _BACKEND_INTERN_TABLE[token]
        out.append(expanded)
    return out


def slim_persisted_storage_route(record: Json) -> Json:
    """Persist the placement half of `storage_route`, not the derived half.

    `canonical_storage_route(storage_options)` is a pure function producing ~25 fields, and a copy
    of its output was written onto every record. Measured on one add of a 62-byte message: 51
    copies, 41 477 bytes, and only THREE distinct values -- one ~1 KB blob repeated onto 29 index
    postings. That was 35% of everything the record payload carried.

    What survives here is what nothing can recompute: the placement fields, which readers do use
    (index enrichment falls back to `storage_route.placement_key` when the top-level one is
    missing). The derived fields are dropped because they are already implied by the record's own
    `storage_options`; a consumer that wants them calls `canonical_storage_route` on those.
    """
    if not isinstance(record, dict):
        return record
    route = record.get("storage_route")
    if not isinstance(route, dict) or not route:
        return record
    # The two keys this used to keep -- placement_key and placement_hash -- are written onto the
    # record's TOP LEVEL by the same function that builds this route
    # (`attach_context_placement`), so the nested pair was a second copy of fields sitting beside
    # it. Nothing reads the nested ones: a search for a read of `storage_route["placement_key"]`
    # or `.get("placement_key")` on a route finds none, while the top-level `placement_key` is what
    # consumers actually use.
    #
    # And the pair is not information either. `placement_key` is
    # `f"context:{scope_key}:node={node_hash}"` and `placement_hash` is `stable_hash` of it, both
    # from fields already on the record -- so a reader that wanted them could rebuild them exactly.
    #
    # Measured over 300 ingests by walking the page segments, `storage_route` cost 1.76 KB per add
    # across records, and 161 bytes on every one of the ~10 index postings an add writes.
    if _placement_is_derivable(record):
        slim = dict(record)
        slim.pop("storage_route", None)
        slim.pop("posting_policy", None)
        return slim
    kept = {key: route[key] for key in _ROUTE_KEYS_WORTH_STORING if key in route}
    if len(kept) == len(route):
        return record
    slim = dict(record)
    if kept:
        slim["storage_route"] = kept
    else:
        slim.pop("storage_route", None)
    return slim


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
            slim = [slim_persisted_storage_route(record) for record in bundle]
            payload_value = slim[0] if len(slim) == 1 else {"record_bundle": slim}
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
