#!/usr/bin/env python3
"""Backend metric state helpers for MatrixArk TemporalStore adapters."""

from __future__ import annotations

import threading
from typing import Any

try:
    from tools.matrixark_mcp_core import now_ms
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_core import now_ms


def initialize_backend_metric_state(target: Any, latency_buckets: list[float]) -> None:
    target._metrics_lock = threading.RLock()
    target._metrics_started_at_ms = now_ms()
    target._commands_total = 0
    target._errors_total = 0
    target._timeouts_total = 0
    target._latency_sum_ms = 0.0
    target._latency_max_ms = 0.0
    target._latency_buckets = [0 for _ in latency_buckets]
    target._records_written_total = 0
    target._records_read_total = 0
    target._append_queue_wait_ms_total = 0.0
    target._append_queue_wait_count = 0
    target._append_engine_ms_total = 0.0
    target._append_engine_count = 0


def ensure_backend_metric_state(target: Any, latency_buckets: list[float]) -> None:
    if not hasattr(target, "_metrics_lock"):
        target._metrics_lock = threading.RLock()
    if not hasattr(target, "_metrics_started_at_ms"):
        target._metrics_started_at_ms = now_ms()
    if not hasattr(target, "_commands_total"):
        target._commands_total = 0
    if not hasattr(target, "_errors_total"):
        target._errors_total = 0
    if not hasattr(target, "_timeouts_total"):
        target._timeouts_total = 0
    if not hasattr(target, "_latency_sum_ms"):
        target._latency_sum_ms = 0.0
    if not hasattr(target, "_latency_max_ms"):
        target._latency_max_ms = 0.0
    if not hasattr(target, "_latency_buckets"):
        target._latency_buckets = [0 for _ in latency_buckets]
    if not hasattr(target, "_records_written_total"):
        target._records_written_total = 0
    if not hasattr(target, "_records_read_total"):
        target._records_read_total = 0
    if not hasattr(target, "_append_queue_wait_ms_total"):
        target._append_queue_wait_ms_total = 0.0
    if not hasattr(target, "_append_queue_wait_count"):
        target._append_queue_wait_count = 0
    if not hasattr(target, "_append_engine_ms_total"):
        target._append_engine_ms_total = 0.0
    if not hasattr(target, "_append_engine_count"):
        target._append_engine_count = 0


def metric_average(total: Any, count: Any) -> float:
    count_value = int(count or 0)
    return float(total or 0.0) / count_value if count_value else 0.0

