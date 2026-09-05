#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Direct-write queue helper functions for MatrixArk TemporalStore adapters."""

from __future__ import annotations

import json
import os
import queue
import threading
import time
from typing import Any

try:
    from tools.matrixark_mcp_core import _mcp_debug_log
    from tools.matrixark_mcp_errors import MatrixArkError
    from tools.matrixark_mcp_identity import now_ms, stable_hash
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_core import _mcp_debug_log
    from matrixark_mcp_errors import MatrixArkError
    from matrixark_mcp_identity import now_ms, stable_hash


Json = dict[str, Any]


def direct_write_durable_payload(
    records: list[Json],
    *,
    backend: str,
    storage_prefix: str,
) -> Json:
    created_at = now_ms()
    return {
        "queue_version": 1,
        "status": "pending",
        "attempts": 0,
        "created_at_ms": created_at,
        "updated_at_ms": created_at,
        "record_count": len(records),
        "backend": backend,
        "storage_prefix": storage_prefix,
        "records": records,
    }


def direct_write_durable_field(payload: Json) -> str:
    digest = stable_hash(json.dumps(payload, sort_keys=True, separators=(",", ":")))
    return f"{int(payload.get('created_at_ms') or now_ms()):020d}:{digest}"


def direct_write_payload_is_pending(payload: Json) -> bool:
    return str(payload.get("status") or "pending") in {"pending", "failed", "running"}


def _env_bool(name: str, default: bool) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.environ.get(name, str(default)))
    except (TypeError, ValueError):
        return default


def ensure_direct_write_queue_fields(target: Any) -> None:
    if not hasattr(target, "_direct_write_queue_enabled"):
        target._direct_write_queue_enabled = _env_bool("MATRIXARK_DIRECT_WRITE_QUEUE", False)
    if not hasattr(target, "_direct_write_queue_max_records"):
        target._direct_write_queue_max_records = max(1, _env_int("MATRIXARK_DIRECT_WRITE_QUEUE_MAX_RECORDS", 10000))
    if not hasattr(target, "_direct_write_queue_put_timeout_s"):
        target._direct_write_queue_put_timeout_s = max(
            0.01,
            _env_int("MATRIXARK_DIRECT_WRITE_QUEUE_PUT_TIMEOUT_MS", 1000) / 1000.0,
        )
    if not hasattr(target, "_direct_write_queue_mode"):
        target._direct_write_queue_mode = os.environ.get("MATRIXARK_DIRECT_WRITE_QUEUE_MODE", "memory").strip().lower() or "memory"
    if target._direct_write_queue_mode not in {"memory", "temporalstore"}:
        target._direct_write_queue_mode = "memory"
    if not hasattr(target, "_direct_write_queue_drain_max_batches"):
        target._direct_write_queue_drain_max_batches = max(
            1,
            _env_int("MATRIXARK_DIRECT_WRITE_QUEUE_DRAIN_MAX_BATCHES", 64),
        )
    if not hasattr(target, "_direct_write_queue_allow_sync_context"):
        target._direct_write_queue_allow_sync_context = _env_bool("MATRIXARK_DIRECT_WRITE_QUEUE_ALLOW_SYNC_CONTEXT", False)
    if not hasattr(target, "_direct_write_queue_autostart"):
        target._direct_write_queue_autostart = True
    if not hasattr(target, "_native_side_index_assume_fresh"):
        target._native_side_index_assume_fresh = _env_bool("MATRIXARK_NATIVE_SIDE_INDEX_ASSUME_FRESH", False)
    if not hasattr(target, "_direct_raw_ingestion_queue_enabled"):
        target._direct_raw_ingestion_queue_enabled = _env_bool("MATRIXARK_DIRECT_RAW_INGESTION_QUEUE", False)
    storage_prefix = str(getattr(target, "_storage_prefix", "matrixark:mcp")).rstrip(":")
    if not hasattr(target, "_direct_write_queue_key"):
        target._direct_write_queue_key = f"{storage_prefix}:direct_write_queue"
    if not hasattr(target, "_direct_write_queue_done_key"):
        target._direct_write_queue_done_key = f"{storage_prefix}:direct_write_queue_done"
    if not hasattr(target, "_direct_write_queue_dead_key"):
        target._direct_write_queue_dead_key = f"{storage_prefix}:direct_write_queue_dead"
    if not hasattr(target, "_direct_write_queue"):
        target._direct_write_queue = queue.Queue(maxsize=int(target._direct_write_queue_max_records))
    if not hasattr(target, "_direct_write_worker_started"):
        target._direct_write_worker_started = False
    if not hasattr(target, "_direct_write_worker_lock"):
        target._direct_write_worker_lock = threading.RLock()
    if not hasattr(target, "_direct_write_stop"):
        target._direct_write_stop = threading.Event()
    if not hasattr(target, "_direct_write_failures"):
        target._direct_write_failures = 0
    if not hasattr(target, "_direct_write_enqueued_records"):
        target._direct_write_enqueued_records = 0
    if not hasattr(target, "_direct_write_flushed_records"):
        target._direct_write_flushed_records = 0
    if not hasattr(target, "_direct_write_enqueued_batches"):
        target._direct_write_enqueued_batches = 0
    if not hasattr(target, "_direct_write_flushed_batches"):
        target._direct_write_flushed_batches = 0
    if not hasattr(target, "_direct_write_dead_letter_batches"):
        target._direct_write_dead_letter_batches = 0


def records_can_use_direct_write_queue(target: Any, records: list[Json]) -> bool:
    ensure_direct_write_queue_fields(target)
    if not bool(getattr(target, "_direct_write_queue_enabled", False)):
        return False
    if not records:
        return False
    if bool(getattr(target, "_direct_write_queue_allow_sync_context", False)):
        return all(isinstance(record, dict) for record in records)
    saw_background_route = False
    for record in records:
        route = record.get("storage_route")
        if not isinstance(route, dict) or not route:
            continue
        if route.get("sync_write") is True or route.get("background_write") is not True:
            return False
        saw_background_route = True
    return saw_background_route


def start_direct_write_worker(target: Any) -> None:
    ensure_direct_write_queue_fields(target)
    with target._direct_write_worker_lock:
        if not target._direct_write_worker_started:
            target._direct_write_worker_started = True
            thread = threading.Thread(
                target=target._direct_write_loop,
                name="matrixark-direct-write-queue",
                daemon=True,
            )
            thread.start()


def enqueue_direct_write_item(target: Any, item: Any, record_count: int) -> None:
    ensure_direct_write_queue_fields(target)
    if bool(getattr(target, "_direct_write_queue_autostart", True)):
        start_direct_write_worker(target)
    wait_started_perf = time.perf_counter()
    try:
        target._direct_write_queue.put(item, timeout=target._direct_write_queue_put_timeout_s)
    except queue.Full as exc:
        target._observe_append_queue_wait((time.perf_counter() - wait_started_perf) * 1000.0)
        if isinstance(item, dict) and item.get("queue_mode") == "temporalstore":
            _mcp_debug_log(
                "matrixark durable direct write queue accepted batch but local worker queue is full; "
                "batch will be recovered by drain"
            )
            target._direct_write_enqueued_records += record_count
            target._direct_write_enqueued_batches += 1
            return
        raise MatrixArkError("direct TemporalStore write queue is full") from exc
    target._observe_append_queue_wait((time.perf_counter() - wait_started_perf) * 1000.0)
    target._direct_write_enqueued_records += record_count
    target._direct_write_enqueued_batches += 1


def enqueue_direct_write(target: Any, records: list[Json]) -> None:
    item: Any = list(records)
    if getattr(target, "_direct_write_queue_mode", "memory") == "temporalstore":
        item = {"queue_mode": "temporalstore", "field": target._enqueue_direct_write_durable(records)}
    enqueue_direct_write_item(target, item, len(records))


def direct_write_loop(target: Any) -> None:
    while not target._direct_write_stop.is_set():
        try:
            first = target._direct_write_queue.get(timeout=0.1)
        except queue.Empty:
            continue
        items = [first]
        max_batches = max(1, int(getattr(target, "_direct_write_queue_drain_max_batches", 64) or 64))
        while len(items) < max_batches:
            try:
                items.append(target._direct_write_queue.get_nowait())
            except queue.Empty:
                break
        try:
            flushed = target._flush_direct_write_items(items)
            target._direct_write_flushed_records += flushed
            target._direct_write_flushed_batches += len(items)
        except Exception as exc:
            target._direct_write_failures += 1
            _mcp_debug_log(f"matrixark direct write queue flush failed: {exc}")
        finally:
            for _item in items:
                try:
                    target._direct_write_queue.task_done()
                except Exception:
                    pass


def flush_direct_write_items(target: Any, items: list[Any]) -> int:
    memory_records: list[Json] = []
    raw_ingestion_records: list[Json] = []
    flushed = 0
    for item in items:
        if isinstance(item, dict) and item.get("queue_mode") == "temporalstore":
            flushed += target._flush_direct_write_durable_field(str(item.get("field") or ""))
        elif isinstance(item, dict) and item.get("queue_mode") == "raw_ingestion":
            rows = item.get("records")
            if isinstance(rows, list):
                raw_ingestion_records.extend(row for row in rows if isinstance(row, dict))
        elif isinstance(item, list):
            memory_records.extend(row for row in item if isinstance(row, dict))
        else:
            raise MatrixArkError("unknown direct write queue item")
    if raw_ingestion_records:
        target._append_raw_ingestion_records(raw_ingestion_records, allow_queue=False)
        flushed += len(raw_ingestion_records)
    if memory_records:
        target._append_many_materialized(memory_records, allow_queue=False)
        flushed += len(memory_records)
    return flushed


def flush_direct_write_item(target: Any, item: Any) -> int:
    return flush_direct_write_items(target, [item])


def load_direct_write_durable_payload(target: Any, field: str) -> Json | None:
    if not field:
        return None
    raw = target._client.hget(target._direct_write_queue_key, field)
    if not raw:
        return None
    payload = json.loads(raw)
    return payload if isinstance(payload, dict) else None


def write_direct_write_durable_status(
    target: Any,
    field: str,
    payload: Json,
    status: str,
    error: str | None = None,
) -> None:
    updated = dict(payload)
    updated["status"] = status
    updated["updated_at_ms"] = now_ms()
    updated["attempts"] = int(updated.get("attempts") or 0) + (1 if status in {"running", "failed", "dead"} else 0)
    if error:
        updated["error"] = error
    key = (
        target._direct_write_queue_done_key
        if status == "done"
        else target._direct_write_queue_dead_key if status == "dead" else target._direct_write_queue_key
    )
    target._hset_with_backoff(key, field, json.dumps(updated, separators=(",", ":")))
    if key != target._direct_write_queue_key:
        target._hset_with_backoff(target._direct_write_queue_key, field, json.dumps(updated, separators=(",", ":")))


def flush_direct_write_durable_field(target: Any, field: str) -> int:
    payload = target._load_direct_write_durable_payload(field)
    if not payload:
        return 0
    status = str(payload.get("status") or "pending")
    if status == "done":
        return 0
    if status == "dead":
        return 0
    records = payload.get("records")
    if not isinstance(records, list):
        target._write_direct_write_durable_status(field, payload, "dead", "durable queue payload has no records list")
        target._direct_write_dead_letter_batches += 1
        return 0
    target._write_direct_write_durable_status(field, payload, "running")
    try:
        target._append_many_materialized(records, allow_queue=False)
    except Exception as exc:
        refreshed = target._load_direct_write_durable_payload(field) or payload
        target._write_direct_write_durable_status(field, refreshed, "failed", str(exc))
        raise
    refreshed = target._load_direct_write_durable_payload(field) or payload
    target._write_direct_write_durable_status(field, refreshed, "done")
    return len(records)


def drain_durable_direct_write_queue(target: Any, *, limit: int | None = None) -> Json:
    target._ensure_direct_write_queue_fields()
    if getattr(target, "_direct_write_queue_mode", "memory") != "temporalstore":
        return {"status": "skipped", "reason": "queue_mode_not_temporalstore"}
    scanner = getattr(target._client, "scan_hash", None)
    if not callable(scanner):
        return {"status": "skipped", "reason": "backend_has_no_scan_hash"}
    response = scanner(target._direct_write_queue_key)
    records = response.get("records") if isinstance(response, dict) else []
    fields: list[str] = []
    for row in records if isinstance(records, list) else []:
        if not isinstance(row, dict):
            continue
        field = str(row.get("field") or "")
        value = row.get("value")
        if not field or not isinstance(value, str):
            continue
        try:
            payload = json.loads(value)
        except Exception:
            continue
        if isinstance(payload, dict) and str(payload.get("status") or "pending") in {"pending", "failed", "running"}:
            fields.append(field)
        if limit is not None and len(fields) >= limit:
            break
    target._start_direct_write_worker()
    for field in fields:
        target._direct_write_queue.put({"queue_mode": "temporalstore", "field": field}, timeout=target._direct_write_queue_put_timeout_s)
    return {"status": "queued", "pending_batches": len(fields), "queue_key": target._direct_write_queue_key}


def direct_write_durable_pending_count(target: Any) -> int:
    target._ensure_direct_write_queue_fields()
    scanner = getattr(getattr(target, "_client", None), "scan_hash", None)
    if not callable(scanner):
        return 0
    try:
        response = scanner(target._direct_write_queue_key)
    except Exception:
        return 0
    rows = response.get("records") if isinstance(response, dict) else []
    count = 0
    for row in rows if isinstance(rows, list) else []:
        if not isinstance(row, dict):
            continue
        value = row.get("value")
        if not isinstance(value, str):
            continue
        try:
            payload = json.loads(value)
        except Exception:
            continue
        if isinstance(payload, dict) and direct_write_payload_is_pending(payload):
            count += 1
    return count


def flush_direct_writes(target: Any, timeout_s: float | None = None) -> None:
    target._ensure_direct_write_queue_fields()
    target._start_direct_write_worker()
    if getattr(target, "_direct_write_queue_mode", "memory") == "temporalstore":
        target.drain_durable_direct_write_queue()
    deadline = time.monotonic() + float(timeout_s if timeout_s is not None else 30.0)
    while target._direct_write_queue.unfinished_tasks:
        if time.monotonic() >= deadline:
            raise MatrixArkError("timed out waiting for direct TemporalStore write queue to drain")
        time.sleep(0.01)


class DirectWriteQueueAdapterMixin:
    """Adapter methods for direct TemporalStore write queue handling."""

    def _ensure_direct_write_queue_fields(self) -> None:
        ensure_direct_write_queue_fields(self)

    def _records_can_use_direct_write_queue(self, records: list[Json]) -> bool:
        return records_can_use_direct_write_queue(self, records)

    def _start_direct_write_worker(self) -> None:
        start_direct_write_worker(self)

    def _direct_write_durable_payload(self, records: list[Json]) -> Json:
        return direct_write_durable_payload(
            records,
            backend=self._backend_label(),
            storage_prefix=self._storage_prefix,
        )

    def _direct_write_durable_field(self, payload: Json) -> str:
        return direct_write_durable_field(payload)

    def _enqueue_direct_write_durable(self, records: list[Json]) -> str:
        payload = self._direct_write_durable_payload(list(records))
        field = self._direct_write_durable_field(payload)
        self._hset_with_backoff(self._direct_write_queue_key, field, json.dumps(payload, separators=(",", ":")))
        return field

    def _enqueue_direct_write_item(self, item: Any, record_count: int) -> None:
        enqueue_direct_write_item(self, item, record_count)

    def _enqueue_direct_write(self, records: list[Json]) -> None:
        enqueue_direct_write(self, records)

    def _direct_write_loop(self) -> None:
        direct_write_loop(self)

    def _flush_direct_write_items(self, items: list[Any]) -> int:
        return flush_direct_write_items(self, items)

    def _flush_direct_write_item(self, item: Any) -> int:
        return flush_direct_write_item(self, item)

    def _load_direct_write_durable_payload(self, field: str) -> Json | None:
        return load_direct_write_durable_payload(self, field)

    def _write_direct_write_durable_status(self, field: str, payload: Json, status: str, error: str | None = None) -> None:
        write_direct_write_durable_status(self, field, payload, status, error)

    def _flush_direct_write_durable_field(self, field: str) -> int:
        return flush_direct_write_durable_field(self, field)

    def drain_durable_direct_write_queue(self, *, limit: int | None = None) -> Json:
        return drain_durable_direct_write_queue(self, limit=limit)

    def _direct_write_durable_pending_count(self) -> int:
        return direct_write_durable_pending_count(self)

    def flush_direct_writes(self, timeout_s: float | None = None) -> None:
        flush_direct_writes(self, timeout_s=timeout_s)
