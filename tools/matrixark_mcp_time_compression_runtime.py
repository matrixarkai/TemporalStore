#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Time-compression runtime helpers for MatrixArk MCP adapters."""

from __future__ import annotations

from typing import Any, Callable

try:
    from tools.matrixark_mcp_core import (
        Json,
        MatrixArkError,
        TIME_COMPRESSION_MAX_RAW_EVENTS_PER_NODE,
        TIME_COMPRESSION_MAX_WINDOWS_PER_REFRESH,
        TIME_COMPRESSION_MIN_EVENTS,
        TIME_COMPRESSION_MIN_EVENT_AGE_MS,
        TIME_COMPRESSION_RAW_EVENT_TTL_AFTER_COMPRESSION_MS,
        TIME_COMPRESSION_WINDOW_EVENTS,
        candidate_access_scope,
        embedding_for_text,
        embedding_model_name,
        generate_time_compression_summary,
        scope_matches,
        scope_from_serving_record,
        stable_hash,
        summarize_text,
    )
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_core import (
        Json,
        MatrixArkError,
        TIME_COMPRESSION_MAX_RAW_EVENTS_PER_NODE,
        TIME_COMPRESSION_MAX_WINDOWS_PER_REFRESH,
        TIME_COMPRESSION_MIN_EVENTS,
        TIME_COMPRESSION_MIN_EVENT_AGE_MS,
        TIME_COMPRESSION_RAW_EVENT_TTL_AFTER_COMPRESSION_MS,
        TIME_COMPRESSION_WINDOW_EVENTS,
        candidate_access_scope,
        embedding_for_text,
        embedding_model_name,
        generate_time_compression_summary,
        scope_matches,
        scope_from_serving_record,
        stable_hash,
        summarize_text,
    )

AppendRecord = Callable[[Json], None]
AppendManyRecords = Callable[[list[Json]], None]


def context_event_ingestion_time_ms(record: Json, debug_by_ref: dict[Any, Json] | None = None) -> int:
    event_hash = record.get("event_id_hash")
    debug_payload = (debug_by_ref or {}).get(event_hash, {}) if event_hash is not None else {}
    envelope = record.get("envelope", {}) if isinstance(record.get("envelope"), dict) else debug_payload.get("envelope", {})
    if not isinstance(envelope, dict):
        envelope = {}
    for value in (envelope.get("ingestion_time_ms"), record.get("updated_at_ms"), record.get("created_at_ms")):
        try:
            timestamp = int(value)
        except (TypeError, ValueError):
            continue
        if timestamp > 0:
            return timestamp
    return 0


def write_time_compression_from_events(
    *,
    append: AppendRecord,
    append_many: AppendManyRecords,
    scope: Json,
    node_hash: int,
    node_path: list[str],
    selected: list[Json],
    event_times: dict[int, int],
    compressed_time_ms: int,
    summary: str = "",
    truncated: bool = False,
    mode: str = "manual",
    raw_event_ttl_after_compression_ms: int = TIME_COMPRESSION_RAW_EVENT_TTL_AFTER_COMPRESSION_MS,
    summary_provider_meta: Json | None = None,
) -> Json:
    if not selected:
        raise MatrixArkError("no source events matched compression window")
    source_event_ids = [int(record["event_id_hash"]) for record in selected if record.get("event_id_hash") is not None]
    if not source_event_ids:
        raise MatrixArkError("source events need event_id_hash for compression")
    source_times = [event_times.get(event_id, 0) for event_id in source_event_ids if event_times.get(event_id, 0) > 0]
    source_start_ms = min(source_times) if source_times else compressed_time_ms
    source_end_ms = max(source_times) if source_times else compressed_time_ms
    if not summary:
        snippets = [summarize_text(str(record.get("text", "")), limit=180) for record in selected[:5]]
        suffix = " plus additional source events" if truncated else ""
        summary = (
            f"Temporal compression window [{source_start_ms}, {source_end_ms}] contains "
            f"{len(selected)} selected events{suffix}. " + " | ".join(snippets)
        )
    compression_id_hash = stable_hash(f"compress:{scope}:{node_hash}:{source_start_ms}:{source_end_ms}:{source_event_ids}")
    record = {
        "record_type": "context_compression_event",
        "compression_id_hash": compression_id_hash,
        "node_hash": node_hash,
        "node_path": node_path,
        "scope": scope,
        "source_start_ms": source_start_ms,
        "source_end_ms": source_end_ms,
        "compressed_time_ms": compressed_time_ms,
        "summary_text": summarize_text(summary, limit=1200),
        "source_event_ids": source_event_ids,
        "source_event_count": len(source_event_ids),
        "truncated_source_events": truncated,
        "operator": "TIME_COMPRESS",
        "compression_mode": mode,
        "summary_provider": summary_provider_meta
        or {
            "provider": "deterministic",
            "model": "",
            "fallback_used": False,
        },
        "compression_safety": {
            "source_event_ids_retained": bool(source_event_ids),
            "source_event_count": len(source_event_ids),
            "summary_non_empty": bool(summary.strip()),
            "raw_events_remain_replayable": True,
            "ttl_marker_only": True,
        },
        "retention_policy": {
            "raw_event_ttl_after_compression_ms": max(0, int(raw_event_ttl_after_compression_ms)),
            "evict_after_ms": compressed_time_ms + max(0, int(raw_event_ttl_after_compression_ms))
            if raw_event_ttl_after_compression_ms > 0
            else 0,
            "requires_no_recent_reinforcement": True,
        },
        "updated_at_ms": compressed_time_ms,
    }
    append(record)
    summary_vector = embedding_for_text(record["summary_text"])
    append(
        {
            "record_type": "context_embedding",
            "embedding_type": "compression_summary",
            "ref_type": "compression",
            "ref_hash": compression_id_hash,
            "node_hash": node_hash,
            "node_path": node_path,
            "dim": len(summary_vector),
            "model": embedding_model_name(),
            "vector": summary_vector,
            "scope": scope,
            "updated_at_ms": compressed_time_ms,
        }
    )
    retention_records = []
    evict_after_ms = int(record["retention_policy"]["evict_after_ms"] or 0)
    for event_id in source_event_ids:
        retention_records.append(
            {
                "record_type": "context_event_retention_marker",
                "event_id_hash": event_id,
                "compression_id_hash": compression_id_hash,
                "node_hash": node_hash,
                "node_path": node_path,
                "scope": scope,
                "retention_state": "compressed_retained",
                "evict_after_ms": evict_after_ms,
                "raw_events_remain_replayable": True,
                "requires_no_recent_reinforcement": True,
                "created_at_ms": compressed_time_ms,
                "updated_at_ms": compressed_time_ms,
            }
        )
    if retention_records:
        append_many(retention_records)
    return record


def auto_time_compress_node_events(
    *,
    append: AppendRecord,
    append_many: AppendManyRecords,
    records: list[Json],
    scope: Json,
    node_hash: int,
    node_path: list[str],
    compressed_time_ms: int,
    max_raw_events_per_node: int = TIME_COMPRESSION_MAX_RAW_EVENTS_PER_NODE,
    max_source_events: int = TIME_COMPRESSION_WINDOW_EVENTS,
    min_source_events: int = TIME_COMPRESSION_MIN_EVENTS,
    max_windows: int = TIME_COMPRESSION_MAX_WINDOWS_PER_REFRESH,
    min_event_age_ms: int = TIME_COMPRESSION_MIN_EVENT_AGE_MS,
    raw_event_ttl_after_compression_ms: int = TIME_COMPRESSION_RAW_EVENT_TTL_AFTER_COMPRESSION_MS,
) -> Json:
    max_raw_events_per_node = max(1, int(max_raw_events_per_node))
    max_source_events = max(1, int(max_source_events))
    min_source_events = max(1, int(min_source_events))
    max_windows = max(0, int(max_windows))
    if max_windows <= 0:
        return {"status": "disabled", "created_count": 0, "created": []}
    debug_by_ref = {
        record.get("ref_hash"): record.get("debug_payload", {})
        for record in records
        if record.get("record_type") == "context_debug_record" and record.get("ref_type") == "event"
    }
    compressed_source_ids: set[int] = set()
    reinforced_source_ids: set[int] = set()
    for record in records:
        if record.get("record_type") != "context_compression_event":
            if record.get("record_type") == "context_recall_reinforcement":
                if int(record.get("node_hash") or 0) != node_hash:
                    continue
                if not scope_matches(candidate_access_scope(record), scope):
                    continue
                if int(record.get("protected_until_ms") or 0) < compressed_time_ms:
                    continue
                try:
                    reinforced_source_ids.add(int(record.get("event_id_hash")))
                except (TypeError, ValueError):
                    pass
            continue
        if int(record.get("node_hash") or 0) != node_hash:
            continue
        if not scope_matches(candidate_access_scope(record), scope):
            continue
        for event_id in record.get("source_event_ids", []) or []:
            try:
                compressed_source_ids.add(int(event_id))
            except (TypeError, ValueError):
                pass
    events: list[Json] = []
    event_times: dict[int, int] = {}
    event_scopes: dict[int, Json] = {}
    for record in records:
        if record.get("record_type") != "context_event":
            continue
        if int(record.get("node_hash") or 0) != node_hash:
            continue
        if not scope_matches(candidate_access_scope(record), scope):
            continue
        try:
            event_hash = int(record.get("event_id_hash"))
        except (TypeError, ValueError):
            continue
        event_time = context_event_ingestion_time_ms(record, debug_by_ref)
        if event_time <= 0:
            continue
        events.append(record)
        event_times[event_hash] = event_time
        event_scopes[event_hash] = candidate_access_scope(record)
    events.sort(key=lambda record: (event_times.get(int(record.get("event_id_hash") or 0), 0), int(record.get("event_id_hash") or 0)))
    if len(events) <= max_raw_events_per_node:
        return {
            "status": "skipped",
            "reason": "raw_event_count_within_threshold",
            "raw_event_count": len(events),
            "max_raw_events_per_node": max_raw_events_per_node,
            "created_count": 0,
            "created": [],
        }
    newest_raw_ids = {
        int(record.get("event_id_hash"))
        for record in events[-max_raw_events_per_node:]
        if record.get("event_id_hash") is not None
    }
    cold_cutoff_ms = compressed_time_ms - max(0, int(min_event_age_ms))
    old_uncompressed = [
        record
        for record in events
        if int(record.get("event_id_hash") or 0) not in newest_raw_ids
        and int(record.get("event_id_hash") or 0) not in compressed_source_ids
        and int(record.get("event_id_hash") or 0) not in reinforced_source_ids
        and (
            min_event_age_ms <= 0
            or event_times.get(int(record.get("event_id_hash") or 0), compressed_time_ms) <= cold_cutoff_ms
        )
    ]
    created: list[Json] = []
    for window_start in range(0, len(old_uncompressed), max_source_events):
        if len(created) >= max_windows:
            break
        window = old_uncompressed[window_start : window_start + max_source_events]
        if len(window) < min_source_events:
            continue
        first_hash = int(window[0].get("event_id_hash") or 0)
        compression_scope = event_scopes.get(first_hash, scope)
        source_ids = [int(record["event_id_hash"]) for record in window if record.get("event_id_hash") is not None]
        source_times = [event_times.get(event_id, 0) for event_id in source_ids if event_times.get(event_id, 0) > 0]
        summary_result = generate_time_compression_summary(
            node_path=node_path,
            source_start_ms=min(source_times) if source_times else compressed_time_ms,
            source_end_ms=max(source_times) if source_times else compressed_time_ms,
            event_texts=[str(record.get("text", "")) for record in window if record.get("text")],
            max_raw_events_per_node=max_raw_events_per_node,
        )
        created.append(
            write_time_compression_from_events(
                append=append,
                append_many=append_many,
                scope=compression_scope,
                node_hash=node_hash,
                node_path=node_path,
                selected=window,
                event_times=event_times,
                compressed_time_ms=compressed_time_ms,
                summary=str(summary_result.get("summary", "")),
                truncated=len(old_uncompressed) > len(source_ids),
                mode="automatic",
                raw_event_ttl_after_compression_ms=raw_event_ttl_after_compression_ms,
                summary_provider_meta={
                    "provider": summary_result.get("provider", "deterministic"),
                    "model": summary_result.get("model", ""),
                    "fallback_used": bool(summary_result.get("fallback_used", False)),
                    "warning": summary_result.get("warning", ""),
                },
            )
        )
    return {
        "status": "ok" if created else "skipped",
        "reason": "" if created else "no_uncompressed_old_window_met_minimum",
        "raw_event_count": len(events),
        "max_raw_events_per_node": max_raw_events_per_node,
        "min_event_age_ms": max(0, int(min_event_age_ms)),
        "cold_cutoff_ms": cold_cutoff_ms,
        "old_uncompressed_event_count": len(old_uncompressed),
        "reinforced_event_count": len(reinforced_source_ids),
        "created_count": len(created),
        "created": [
            {
                "compression_id_hash": item.get("compression_id_hash"),
                "source_start_ms": item.get("source_start_ms"),
                "source_end_ms": item.get("source_end_ms"),
                "source_event_count": item.get("source_event_count"),
            }
            for item in created
        ],
    }


def write_time_compression(
    *,
    append: AppendRecord,
    records: list[Json],
    scope: Json,
    node_hash: int,
    node_path: list[str],
    source_start_ms: int,
    source_end_ms: int,
    compressed_time_ms: int,
    max_source_events: int = 32,
    min_confidence: float = 0.0,
    min_importance: float = 0.0,
    summary: str = "",
) -> Json:
    if source_start_ms > source_end_ms:
        raise MatrixArkError("source_start_ms must be <= source_end_ms")
    if max_source_events <= 0:
        raise MatrixArkError("max_source_events must be positive")
    debug_by_ref = {
        record.get("ref_hash"): record.get("debug_payload", {})
        for record in records
        if record.get("record_type") == "context_debug_record" and record.get("ref_type") == "event"
    }
    source_events = []
    event_times: dict[int, int] = {}
    event_scopes: dict[int, Json] = {}
    for record in records:
        if record.get("record_type") != "context_event":
            continue
        if int(record.get("node_hash") or 0) != node_hash:
            continue
        event_hash = int(record.get("event_id_hash") or 0)
        debug_payload = debug_by_ref.get(event_hash, {}) if event_hash else {}
        envelope = record.get("envelope", {}) if isinstance(record.get("envelope"), dict) else debug_payload.get("envelope", {})
        if not isinstance(envelope, dict):
            envelope = {}
        event_scope = envelope.get("scope", scope_from_serving_record(record))
        if not scope_matches(event_scope, scope):
            continue
        event_time = int(envelope.get("ingestion_time_ms") or record.get("updated_at_ms") or 0)
        if event_time < source_start_ms or event_time > source_end_ms:
            continue
        extraction = debug_payload.get("internal_extraction", {}) if isinstance(debug_payload.get("internal_extraction"), dict) else {}
        confidence = float(extraction.get("confidence", record.get("confidence", 1.0)) or 1.0)
        metadata = envelope.get("metadata", {}) if isinstance(envelope.get("metadata"), dict) else {}
        importance = float(metadata.get("importance", record.get("importance", 1.0)) or 1.0)
        if confidence < min_confidence or importance < min_importance:
            continue
        source_events.append(record)
        event_times[event_hash] = event_time
        event_scopes[event_hash] = event_scope
    source_events.sort(key=lambda record: event_times.get(int(record.get("event_id_hash") or 0), 0))
    selected = source_events[:max_source_events]
    if not selected:
        raise MatrixArkError("no source events matched compression window")
    truncated = len(source_events) > len(selected)
    source_event_ids = [int(record["event_id_hash"]) for record in selected]
    compression_scope = event_scopes.get(int(selected[0].get("event_id_hash") or 0), scope)
    if not summary:
        snippets = [summarize_text(str(record.get("text", "")), limit=180) for record in selected[:5]]
        suffix = " plus additional source events" if truncated else ""
        summary = (
            f"Temporal compression window [{source_start_ms}, {source_end_ms}] contains "
            f"{len(selected)} selected events{suffix}. " + " | ".join(snippets)
        )
    compression_id_hash = stable_hash(f"compress:{scope}:{node_hash}:{source_start_ms}:{source_end_ms}:{source_event_ids}")
    record = {
        "record_type": "context_compression_event",
        "compression_id_hash": compression_id_hash,
        "node_hash": node_hash,
        "node_path": node_path,
        "scope": compression_scope,
        "source_start_ms": source_start_ms,
        "source_end_ms": source_end_ms,
        "compressed_time_ms": compressed_time_ms,
        "summary_text": summarize_text(summary, limit=1200),
        "source_event_ids": source_event_ids,
        "source_event_count": len(selected),
        "truncated_source_events": truncated,
        "operator": "TIME_COMPRESS",
        "updated_at_ms": compressed_time_ms,
    }
    append(record)
    summary_vector = embedding_for_text(record["summary_text"])
    append(
        {
            "record_type": "context_embedding",
            "embedding_type": "compression_summary",
            "ref_type": "compression",
            "ref_hash": compression_id_hash,
            "node_hash": node_hash,
            "node_path": node_path,
            "dim": len(summary_vector),
            "model": embedding_model_name(),
            "vector": summary_vector,
            "scope": compression_scope,
            "updated_at_ms": compressed_time_ms,
        }
    )
    return record


def append_recall_reinforcement_markers(
    *,
    append_many: AppendManyRecords,
    context_pack_id: str,
    selected_refs: list[Json],
    reinforced_at_ms: int,
    protect_ms: int,
) -> Json:
    protect_ms = max(0, int(protect_ms))
    protected_until_ms = reinforced_at_ms + protect_ms if protect_ms else 0
    records: list[Json] = []
    seen: set[tuple[int, int]] = set()
    for ref in selected_refs:
        source_ids: list[int] = []
        if ref.get("ref_type") == "event" and ref.get("ref_hash") is not None:
            try:
                source_ids.append(int(ref.get("ref_hash")))
            except (TypeError, ValueError):
                pass
        for event_id in ref.get("source_event_ids", []) or []:
            try:
                source_ids.append(int(event_id))
            except (TypeError, ValueError):
                pass
        for event_id in source_ids:
            try:
                node_hash = int(ref.get("node_hash") or 0)
            except (TypeError, ValueError):
                node_hash = 0
            key = (event_id, node_hash)
            if key in seen:
                continue
            seen.add(key)
            records.append(
                {
                    "record_type": "context_recall_reinforcement",
                    "event_id_hash": event_id,
                    "node_hash": node_hash,
                    "node_path": ref.get("node_path", []),
                    "context_pack_id": context_pack_id,
                    "source_ref_type": ref.get("ref_type"),
                    "source_ref_hash": ref.get("ref_hash"),
                    "scope": ref.get("scope", {}),
                    "reinforced_at_ms": reinforced_at_ms,
                    "protected_until_ms": protected_until_ms,
                    "reason": "selected_in_context_pack",
                    "created_at_ms": reinforced_at_ms,
                    "updated_at_ms": reinforced_at_ms,
                }
            )
    if records:
        append_many(records)
    return {
        "reinforced_event_count": len(records),
        "protect_ms": protect_ms,
        "protected_until_ms": protected_until_ms,
    }


def query_time_compressions(
    *,
    records: list[Json],
    scope: Json,
    node_hashes: set[int],
    start_time_ms: int,
    end_time_ms: int,
    limit: int = 16,
) -> list[Json]:
    matches = []
    for record in records:
        if record.get("record_type") != "context_compression_event":
            continue
        if node_hashes and int(record.get("node_hash") or 0) not in node_hashes:
            continue
        if not scope_matches(candidate_access_scope(record), scope):
            continue
        if int(record.get("source_end_ms") or 0) >= start_time_ms and int(record.get("source_start_ms") or 0) <= end_time_ms:
            matches.append(record)
    matches.sort(key=lambda record: (int(record.get("source_end_ms") or 0), int(record.get("compressed_time_ms") or 0)), reverse=True)
    return matches[:limit]
