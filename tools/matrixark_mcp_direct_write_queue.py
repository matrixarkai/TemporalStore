#!/usr/bin/env python3
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
        target._direct_write_queue_autostart = _env_bool("MATRIXARK_DIRECT_WRITE_QUEUE_AUTOSTART", True)
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
