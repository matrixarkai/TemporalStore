#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Coalesced read/write helpers for the MatrixArk Rust proxy client."""

from __future__ import annotations

import json
import threading
import time
from collections import defaultdict, deque
from typing import Any

try:
    from tools.matrixark_mcp_core import Json, MatrixArkError
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_core import Json, MatrixArkError


def coalesced_batch_hset(target: Any, compact_entries: list[list[str]]) -> None:
    event = threading.Event()
    request: Json = {
        "entries_compact": compact_entries,
        "event": event,
        "error": None,
    }
    became_leader = False
    queued_at = time.perf_counter()
    with target._batch_hset_coalesce_lock:
        target._batch_hset_coalesce_queue.append(request)
        if not target._batch_hset_coalesce_active:
            target._batch_hset_coalesce_active = True
            became_leader = True
    if became_leader:
        drain_batch_hset_coalescer(target)
    else:
        timeout_s = max(target._backpressure_timeout_s, target.request_timeout_ms / 1000.0 + 2.0)
        if not event.wait(timeout=timeout_s):
            raise MatrixArkError(f"Rust TemporalStore batch_hset coalescer timed out after {timeout_s:.1f}s")
    wait_ms = (time.perf_counter() - queued_at) * 1000.0
    with target._metrics_lock:
        target._batch_hset_coalesced_wait_ms_total += wait_ms
        target._batch_hset_coalesced_wait_ms_max = max(target._batch_hset_coalesced_wait_ms_max, wait_ms)
    error = request.get("error")
    if error:
        raise error


def drain_batch_hset_coalescer(target: Any) -> None:
    try:
        if target._batch_hset_coalesce_wait_s > 0:
            time.sleep(target._batch_hset_coalesce_wait_s)
        while True:
            with target._batch_hset_coalesce_lock:
                pending = target._batch_hset_coalesce_queue[: target._batch_hset_coalesce_max_batches]
                del target._batch_hset_coalesce_queue[: len(pending)]
            if not pending:
                with target._batch_hset_coalesce_lock:
                    if not target._batch_hset_coalesce_queue:
                        target._batch_hset_coalesce_active = False
                        return
                continue
            merged: list[list[str]] = []
            for item in pending:
                merged.extend(item.get("entries_compact") or [])
            error: BaseException | None = None
            try:
                target._call_hash_batch_json("batch_hset", merged)
            except BaseException as exc:
                error = exc
            if error is None:
                target._scan_hash_cache_invalidate_keys(entry[0] for entry in merged)
                target._context_pack_response_cache_clear()
            with target._metrics_lock:
                target._batch_hset_coalesced_batches_total += 1
                target._batch_hset_coalesced_calls_total += len(pending)
                target._batch_hset_coalesced_records_total += len(merged)
            for item in pending:
                item["error"] = error
                item["event"].set()
            if error is not None:
                with target._batch_hset_coalesce_lock:
                    remaining = target._batch_hset_coalesce_queue
                    target._batch_hset_coalesce_queue = []
                    target._batch_hset_coalesce_active = False
                for item in remaining:
                    item["error"] = error
                    item["event"].set()
                return
    except BaseException as exc:
        with target._batch_hset_coalesce_lock:
            remaining = target._batch_hset_coalesce_queue
            target._batch_hset_coalesce_queue = []
            target._batch_hset_coalesce_active = False
        for item in remaining:
            item["error"] = exc
            item["event"].set()
        raise


def append_options_signature(append_options: Json) -> str:
    try:
        return json.dumps(append_options or {}, sort_keys=True, separators=(",", ":"), default=str)
    except Exception:
        return str(sorted((append_options or {}).items())) if isinstance(append_options, dict) else str(append_options)


def max_count_value(values: list[str]) -> str:
    numeric: list[int] = []
    for value in values:
        try:
            numeric.append(int(str(value)))
        except (TypeError, ValueError):
            continue
    if numeric:
        return str(max(numeric))
    return values[-1] if values else ""


def coalesced_matrixark_batch_append_records(
    target: Any,
    compact_entries: list[list[str]],
    *,
    count_key: str,
    count_value: str,
    append_options: Json,
) -> None:
    event = threading.Event()
    request: Json = {
        "entries_compact": compact_entries,
        "count_key": count_key,
        "count_value": count_value,
        "append_options": append_options,
        "append_options_signature": append_options_signature(append_options),
        "event": event,
        "error": None,
    }
    became_leader = False
    queued_at = time.perf_counter()
    with target._append_coalesce_lock:
        target._append_coalesce_queue.append(request)
        if not target._append_coalesce_active:
            target._append_coalesce_active = True
            became_leader = True
    if became_leader:
        drain_append_coalescer(target)
    else:
        timeout_s = max(target._backpressure_timeout_s, target.request_timeout_ms / 1000.0 + 2.0)
        if not event.wait(timeout=timeout_s):
            raise MatrixArkError(f"Rust TemporalStore matrixark append coalescer timed out after {timeout_s:.1f}s")
    wait_ms = (time.perf_counter() - queued_at) * 1000.0
    with target._metrics_lock:
        target._append_coalesced_wait_ms_total += wait_ms
        target._append_coalesced_wait_ms_max = max(target._append_coalesced_wait_ms_max, wait_ms)
    error = request.get("error")
    if error:
        raise error


def drain_append_coalescer(target: Any) -> None:
    try:
        if target._append_coalesce_wait_s > 0:
            time.sleep(target._append_coalesce_wait_s)
        while True:
            with target._append_coalesce_lock:
                pending = target._append_coalesce_queue[: target._append_coalesce_max_batches]
                del target._append_coalesce_queue[: len(pending)]
            if not pending:
                with target._append_coalesce_lock:
                    if not target._append_coalesce_queue:
                        target._append_coalesce_active = False
                        return
                continue
            grouped: dict[tuple[str, str], list[Json]] = {}
            for item in pending:
                signature = (str(item.get("count_key") or ""), str(item.get("append_options_signature") or ""))
                grouped.setdefault(signature, []).append(item)
            for items in grouped.values():
                merged: list[list[str]] = []
                count_values: list[str] = []
                append_options = items[0].get("append_options") or {}
                count_key = str(items[0].get("count_key") or "")
                for item in items:
                    merged.extend(item.get("entries_compact") or [])
                    value = str(item.get("count_value") or "")
                    if value:
                        count_values.append(value)
                count_value = max_count_value(count_values)
                error: BaseException | None = None
                try:
                    target._call_json(
                        "matrixark_batch_append_records",
                        entries_compact=merged,
                        key=count_key,
                        value=count_value,
                        append_options=append_options,
                    )
                except BaseException as exc:
                    error = exc
                if error is None and count_key:
                    target._string_cache_put(count_key, count_value)
                if error is None:
                    target._scan_hash_cache_invalidate_keys(entry[0] for entry in merged)
                    target._context_pack_response_cache_clear()
                with target._metrics_lock:
                    target._append_coalesced_batches_total += 1
                    target._append_coalesced_calls_total += len(items)
                    target._append_coalesced_records_total += len(merged)
                for item in items:
                    item["error"] = error
                    item["event"].set()
                if error is not None:
                    with target._append_coalesce_lock:
                        remaining = target._append_coalesce_queue
                        target._append_coalesce_queue = []
                        target._append_coalesce_active = False
                    for item in remaining:
                        item["error"] = error
                        item["event"].set()
                    return
    except BaseException as exc:
        with target._append_coalesce_lock:
            remaining = target._append_coalesce_queue
            target._append_coalesce_queue = []
            target._append_coalesce_active = False
        for item in remaining:
            item["error"] = exc
            item["event"].set()
        raise


def coalesced_batch_hget(target: Any, compact_entries: list[list[str]]) -> list[Json]:
    event = threading.Event()
    request: Json = {
        "entries_compact": compact_entries,
        "event": event,
        "error": None,
        "records": None,
    }
    became_leader = False
    queued_at = time.perf_counter()
    with target._batch_hget_coalesce_lock:
        target._batch_hget_coalesce_queue.append(request)
        if not target._batch_hget_coalesce_active:
            target._batch_hget_coalesce_active = True
            became_leader = True
    if became_leader:
        drain_batch_hget_coalescer(target)
    else:
        timeout_s = max(target._backpressure_timeout_s, target.request_timeout_ms / 1000.0 + 2.0)
        if not event.wait(timeout=timeout_s):
            raise MatrixArkError(f"Rust TemporalStore batch_hget coalescer timed out after {timeout_s:.1f}s")
    wait_ms = (time.perf_counter() - queued_at) * 1000.0
    with target._metrics_lock:
        target._batch_hget_coalesced_wait_ms_total += wait_ms
        target._batch_hget_coalesced_wait_ms_max = max(target._batch_hget_coalesced_wait_ms_max, wait_ms)
    error = request.get("error")
    if error:
        raise error
    records = request.get("records")
    return records if isinstance(records, list) else []


def drain_batch_hget_coalescer(target: Any) -> None:
    try:
        if target._batch_hget_coalesce_wait_s > 0:
            time.sleep(target._batch_hget_coalesce_wait_s)
        while True:
            with target._batch_hget_coalesce_lock:
                pending = target._batch_hget_coalesce_queue[: target._batch_hget_coalesce_max_batches]
                del target._batch_hget_coalesce_queue[: len(pending)]
            if not pending:
                with target._batch_hget_coalesce_lock:
                    if not target._batch_hget_coalesce_queue:
                        target._batch_hget_coalesce_active = False
                        return
                continue
            merged: list[list[str]] = []
            for item in pending:
                merged.extend(item.get("entries_compact") or [])
            error: BaseException | None = None
            rows: list[Json] = []
            try:
                response = target._call_hash_batch_json(
                    "batch_hget",
                    merged,
                    compact_read_response=True,
                )
                rows = target._batch_hget_records_from_response(merged, response)
            except BaseException as exc:
                error = exc
            if error is None:
                if len(rows) == len(merged):
                    cursor = 0
                    ordered = True
                    for item in pending:
                        item_records: list[Json] = []
                        for key, field, _ in item.get("entries_compact") or []:
                            row = rows[cursor] if cursor < len(rows) else {}
                            cursor += 1
                            if (
                                not isinstance(row, dict)
                                or str(row.get("key") or "") != key
                                or str(row.get("field") or "") != field
                            ):
                                ordered = False
                                break
                            item_records.append(row)
                        if not ordered:
                            break
                        item["records"] = item_records
                    if not ordered:
                        assign_coalesced_batch_hget_by_key(pending, rows)
                else:
                    assign_coalesced_batch_hget_by_key(pending, rows)
            with target._metrics_lock:
                target._batch_hget_coalesced_batches_total += 1
                target._batch_hget_coalesced_calls_total += len(pending)
                target._batch_hget_coalesced_records_total += len(merged)
            for item in pending:
                item["error"] = error
                item["event"].set()
            if error is not None:
                with target._batch_hget_coalesce_lock:
                    remaining = target._batch_hget_coalesce_queue
                    target._batch_hget_coalesce_queue = []
                    target._batch_hget_coalesce_active = False
                for item in remaining:
                    item["error"] = error
                    item["event"].set()
                return
    except BaseException as exc:
        with target._batch_hget_coalesce_lock:
            remaining = target._batch_hget_coalesce_queue
            target._batch_hget_coalesce_queue = []
            target._batch_hget_coalesce_active = False
        for item in remaining:
            item["error"] = exc
            item["event"].set()
        raise


def assign_coalesced_batch_hget_by_key(pending: list[Json], rows: list[Json]) -> None:
    records_by_entry: dict[tuple[str, str], deque[Json]] = defaultdict(deque)
    for row in rows:
        if not isinstance(row, dict):
            continue
        records_by_entry[(str(row.get("key") or ""), str(row.get("field") or ""))].append(row)
    for item in pending:
        item_records: list[Json] = []
        for key, field, _ in item.get("entries_compact") or []:
            bucket = records_by_entry.get((key, field))
            if bucket:
                item_records.append(bucket.popleft())
            else:
                item_records.append({"key": key, "field": field, "value": ""})
        item["records"] = item_records
