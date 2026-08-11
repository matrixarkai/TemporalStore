#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Async resource import runtime helpers for MatrixArk MCP."""

from __future__ import annotations

from pathlib import Path
import queue as thread_queue
import threading
import time

try:
    from tools.matrixark_mcp_core import (
        Json,
        MatrixArkError,
        RESOURCE_ASYNC_DEFAULT_BYTES,
        RESOURCE_ASYNC_DEFAULT_PATH_COUNT,
        RESOURCE_ASYNC_DEFAULT_TEXT_CHARS,
        _mcp_debug_log,
        deployment_scope_from_args,
        normalize_envelope,
        now_ms,
        optional_object,
        stable_hash,
    )
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_core import (
        Json,
        MatrixArkError,
        RESOURCE_ASYNC_DEFAULT_BYTES,
        RESOURCE_ASYNC_DEFAULT_PATH_COUNT,
        RESOURCE_ASYNC_DEFAULT_TEXT_CHARS,
        _mcp_debug_log,
        deployment_scope_from_args,
        normalize_envelope,
        now_ms,
        optional_object,
        stable_hash,
    )

RESOURCE_IMPORT_IGNORE_DIRS = {".git", "node_modules", "target", "build", "dist", ".venv", "__pycache__"}


def resource_import_pool_status(adapter: object) -> Json:
    queue_depth = adapter._resource_import_queue.qsize()
    return {
        "worker_count": adapter._resource_import_worker_count,
        "queue_max": adapter._resource_import_queue_max,
        "queue_depth": queue_depth,
        "queue_remaining_capacity": max(0, adapter._resource_import_queue_max - queue_depth),
        "bounded": True,
    }


def ensure_resource_import_workers(adapter: object) -> None:
    with adapter._resource_import_worker_lock:
        if adapter._resource_import_workers_started:
            return
        adapter._resource_import_stop.clear()
        for worker_index in range(adapter._resource_import_worker_count):
            thread = threading.Thread(
                target=adapter._resource_import_worker_loop,
                name=f"matrixark-resource-import-{worker_index}",
                daemon=True,
            )
            thread.start()
            adapter._resource_import_threads.append(thread)
        adapter._resource_import_workers_started = True


def resource_import_worker_loop(adapter: object) -> None:
    while True:
        item = adapter._resource_import_queue.get()
        try:
            if item.get("_stop"):
                return
            args = item.get("args", {})
            hook = item.get("hook")
            adapter._run_background_resource_import(args, hook if isinstance(hook, dict) else None)
        finally:
            adapter._resource_import_queue.task_done()


def close_resource_import_runtime(adapter: object, *, timeout_s: float = 5.0) -> None:
    deadline = time.monotonic() + max(0.0, timeout_s)
    while getattr(adapter._resource_import_queue, "unfinished_tasks", 0) and time.monotonic() < deadline:
        time.sleep(0.01)
    adapter._resource_import_stop.set()
    with adapter._resource_import_worker_lock:
        if adapter._resource_import_workers_started:
            for _thread in adapter._resource_import_threads:
                remaining = max(0.0, deadline - time.monotonic())
                try:
                    adapter._resource_import_queue.put({"_stop": True}, timeout=remaining if remaining > 0 else 0.01)
                except thread_queue.Full:
                    pass
            for thread in list(adapter._resource_import_threads):
                thread.join(timeout=max(0.0, deadline - time.monotonic()))
            adapter._resource_import_threads = [thread for thread in adapter._resource_import_threads if thread.is_alive()]
            adapter._resource_import_workers_started = bool(adapter._resource_import_threads)


def enqueue_resource_import(adapter: object, *, args: Json, hook: Json | None, task_hash: int) -> Json:
    adapter._ensure_resource_import_workers()
    queue_before = adapter._resource_import_queue.qsize()
    try:
        adapter._resource_import_queue.put_nowait(
            {
                "args": args,
                "hook": hook,
                "task_hash": task_hash,
                "queued_at_ms": now_ms(),
            }
        )
    except thread_queue.Full:
        raise MatrixArkError(
            f"resource import queue is full; workers={adapter._resource_import_worker_count} max_queue={adapter._resource_import_queue_max}"
        )
    status = resource_import_pool_status(adapter)
    status["queue_depth_before_enqueue"] = queue_before
    adapter._observe_model_latency("resource_import_queue_wait", 0.0)
    metrics = getattr(adapter, "_matrixark_service_metrics", None)
    if metrics is not None:
        metrics.observe_resource_queue_depth(int(status.get("queue_depth") or 0))
    return status


def run_background_resource_import(adapter: object, args: Json, hook: Json | None) -> None:
    task_hash = args.get("_resource_import_task_hash", 0)
    try:
        adapter.ingest(args, hook=hook)
    except Exception as exc:  # pragma: no cover - background failure path is validated via records.
        scope = optional_object(args, "scope")
        metadata = optional_object(args, "metadata")
        envelope = normalize_envelope(args, default_kind="resource")
        deployment_scope = deployment_scope_from_args(args, envelope)
        sharing_scope = adapter.resource_sharing_scope(args, envelope, deployment_scope)
        node_hint = adapter.default_resource_node_path(args, envelope, deployment_scope=deployment_scope, sharing_scope=sharing_scope)
        node_path = [str(part) for part in node_hint if str(part)]
        try:
            adapter.append(
                {
                    "record_type": "resource_import_task",
                    "task_hash": task_hash,
                    "status": "failed",
                    "kind": str(args.get("kind") or "resource"),
                    "raw_uri": str(args.get("raw_uri") or metadata.get("raw_uri") or "inline-resource"),
                    "resource_type": str(args.get("resource_type") or metadata.get("resource_type") or ""),
                    "error": str(exc),
                    "node_hash": stable_hash("/".join(node_path)),
                    "node_path": node_path,
                    "scope": dict(scope),
                    "updated_at_ms": now_ms(),
                }
            )
        except Exception:
            _mcp_debug_log(f"resource import background failure could not be recorded: {exc}")


def resource_import_async_default_reason(args: Json, envelope: Json, raw_uri: str) -> str:
    if "wait" in args:
        return ""
    inline_text = "\n\n".join(str(message.get("content", "")) for message in envelope.get("messages", []))
    if len(inline_text) >= RESOURCE_ASYNC_DEFAULT_TEXT_CHARS:
        return f"inline_text_chars>={RESOURCE_ASYNC_DEFAULT_TEXT_CHARS}"
    try:
        path = Path(raw_uri)
        if not path.exists():
            return ""
        if path.is_file():
            size = path.stat().st_size
            if size >= RESOURCE_ASYNC_DEFAULT_BYTES:
                return f"file_bytes>={RESOURCE_ASYNC_DEFAULT_BYTES}"
        elif path.is_dir():
            file_count = 0
            total_size = 0
            for child in path.rglob("*"):
                if not child.is_file():
                    continue
                if any(part in RESOURCE_IMPORT_IGNORE_DIRS for part in child.parts):
                    continue
                file_count += 1
                try:
                    total_size += child.stat().st_size
                except OSError:
                    pass
                if file_count >= RESOURCE_ASYNC_DEFAULT_PATH_COUNT:
                    return f"path_count>={RESOURCE_ASYNC_DEFAULT_PATH_COUNT}"
                if total_size >= RESOURCE_ASYNC_DEFAULT_BYTES:
                    return f"directory_bytes>={RESOURCE_ASYNC_DEFAULT_BYTES}"
    except (OSError, ValueError):
        return ""
    return ""
