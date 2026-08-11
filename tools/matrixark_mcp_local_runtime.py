#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Local MatrixArk runtime state and write-batch helpers."""

from __future__ import annotations

from contextlib import contextmanager
import json
import queue as thread_queue
import threading
from typing import Any, Iterator

try:
    from tools.matrixark_mcp_core import Json
    from tools.matrixark_mcp_core import materialize_serving_record_batch
    from tools.matrixark_mcp_env import env_float, env_int
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_core import Json
    from matrixark_mcp_core import materialize_serving_record_batch
    from matrixark_mcp_env import env_float, env_int


def init_local_runtime_state(adapter: Any) -> None:
    adapter.event_log.parent.mkdir(parents=True, exist_ok=True)
    adapter._write_batch_local = threading.local()
    adapter._event_log_lock = threading.RLock()
    adapter._resource_import_worker_count = max(1, env_int("MATRIXARK_RESOURCE_IMPORT_WORKERS", 2))
    adapter._resource_import_queue_max = max(1, env_int("MATRIXARK_RESOURCE_IMPORT_QUEUE_MAX", 64))
    adapter._resource_import_queue = thread_queue.Queue(maxsize=adapter._resource_import_queue_max)
    adapter._resource_import_workers_started = False
    adapter._resource_import_worker_lock = threading.RLock()
    adapter._resource_import_stop = threading.Event()
    adapter._resource_import_threads = []
    adapter._latest_entity_by_hash = {}
    adapter._entity_cache_loaded = False
    adapter._session_buffer_cache_lock = threading.RLock()
    adapter._context_event_by_hash = {}
    adapter._session_pending_event_ids_by_key = {}
    adapter._session_committed_event_ids_by_key = {}
    adapter._context_node_hashes = set()
    adapter._context_child_ref_hashes = set()
    adapter._context_node_cache_loaded = False
    adapter._read_cache_lock = threading.RLock()
    adapter._read_cache_records = None
    adapter._read_cache_size = -1
    adapter._read_cache_mtime_ns = -1
    adapter._retrieval_records_cache_lock = threading.RLock()
    adapter._retrieval_records_cache_generation = 0
    adapter._retrieval_records_cache = {}
    adapter._context_pack_cache_lock = threading.RLock()
    adapter._context_pack_cache = {}
    adapter._context_pack_cache_max_entries = max(0, env_int("MATRIXARK_CONTEXT_PACK_CACHE_MAX_ENTRIES", 256))
    adapter._context_pack_cache_ttl_s = max(0.0, env_float("MATRIXARK_CONTEXT_PACK_CACHE_TTL_S", 30.0))


def write_batch_stack(adapter: Any) -> list[list[Json]]:
    local = getattr(adapter, "_write_batch_local", None)
    if local is None:
        adapter._write_batch_local = threading.local()
        local = adapter._write_batch_local
    stack = getattr(local, "stack", None)
    if stack is None:
        stack = []
        local.stack = stack
    return stack


def current_write_batch(adapter: Any) -> list[Json] | None:
    stack = write_batch_stack(adapter)
    return stack[-1] if stack else None


def queue_batched_records(adapter: Any, records: list[Json]) -> bool:
    batch = current_write_batch(adapter)
    if batch is None:
        return False
    batch.extend(records)
    return True


def append(adapter: Any, record: Json) -> None:
    append_many(adapter, [record])


def append_many(adapter: Any, records: list[Json]) -> None:
    records = materialize_serving_record_batch(records)
    if not records:
        return
    if queue_batched_records(adapter, records):
        return
    with adapter._event_log_lock:
        with adapter.event_log.open("a", encoding="utf-8") as handle:
            for record in records:
                handle.write(json.dumps(record, separators=(",", ":")) + "\n")
    adapter._update_latest_entity_cache(records)


@contextmanager
def write_batch(adapter: Any, label: str = "hot_path") -> Iterator[list[Json]]:
    del label
    stack = write_batch_stack(adapter)
    batch: list[Json] = []
    stack.append(batch)
    try:
        yield batch
    except Exception:
        stack.pop()
        raise
    else:
        stack.pop()
        if batch:
            adapter.append_many(batch)
