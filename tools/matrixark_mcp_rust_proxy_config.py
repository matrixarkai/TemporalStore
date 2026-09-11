#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Configuration helpers for the MatrixArk Rust proxy client."""

from __future__ import annotations

import os
from typing import Any


def _env_bool(name: str, default: str = "1") -> bool:
    return os.environ.get(name, default).strip().lower() not in {"0", "false", "no"}


def _env_int(name: str, default: str, *, minimum: int = 1) -> int:
    return max(minimum, int(os.environ.get(name, default)))


def _env_seconds_from_ms(name: str, default_ms: str, *, minimum: float = 0.0) -> float:
    return max(minimum, float(os.environ.get(name, default_ms)) / 1000.0)


try:  # pragma: no cover - import shape differs when run as a package
    from matrixark_json_lane import LANE_RESPONSE_GRACE_S, lane_response_deadline_s
except ImportError:  # pragma: no cover
    from tools.matrixark_json_lane import LANE_RESPONSE_GRACE_S, lane_response_deadline_s

_ = LANE_RESPONSE_GRACE_S  # noqa: F401 - re-exported for readers of this module


def _unused_lane_response_deadline_s(request_timeout_ms: int) -> float:
    """How long one call may hold a lane waiting for the proxy to answer.

    This is the ONLY definition. It used to be written out again inside `_read_json_line`, and the
    backpressure timeout was derived from `request_timeout_ms` alone -- two seconds less. A waiter
    that gives up before the holder's own deadline can never be granted the lane, so any call that
    ran to its deadline rejected every caller queued behind it, reporting lane backpressure when
    the truth was one slow call.
    """
    return max(LANE_RESPONSE_GRACE_S, request_timeout_ms / 1000.0 + LANE_RESPONSE_GRACE_S)


def initialize_rust_proxy_config(target: Any, *, request_timeout_ms: int) -> None:
    # Defaults to the longest one in-flight call may legitimately take, so backpressure means the
    # lane is saturated rather than merely busy. An operator value still wins, in either spelling;
    # `.strip() or` on both because an exported-but-empty variable is not a value.
    configured_backpressure_ms = (
        os.environ.get("MATRIXARK_RUST_PROXY_BACKPRESSURE_TIMEOUT_MS", "").strip()
        or os.environ.get("MATRIXARK_RUST_GATEWAY_BACKPRESSURE_TIMEOUT_MS", "").strip()
    )
    target._backpressure_timeout_s = max(
        0.05,
        (
            int(configured_backpressure_ms) / 1000.0
            if configured_backpressure_ms
            else lane_response_deadline_s(request_timeout_ms)
        ),
    )
    target._write_lane_count = _env_int("MATRIXARK_RUST_PROXY_WRITE_LANES", "4")
    target._read_lane_count = _env_int("MATRIXARK_RUST_PROXY_READ_LANES", "4")
    # Native ContextPack assembly should not over-provision proxy processes by
    # default. Match read lanes unless operators explicitly widen it.
    target._pack_lane_count = _env_int(
        "MATRIXARK_RUST_PROXY_PACK_LANES",
        str(target._read_lane_count),
    )
    target._control_lane_count = _env_int("MATRIXARK_RUST_PROXY_CONTROL_LANES", "1")
    target._shared_process_mode = _env_bool("MATRIXARK_RUST_PROXY_SHARED_PROCESS")
    target._dedicated_pack_lanes_enabled = _env_bool("MATRIXARK_RUST_PROXY_DEDICATED_PACK_LANES")

    target._batch_hset_coalesce_enabled = _env_bool("MATRIXARK_RUST_PROXY_BATCH_HSET_COALESCE")
    target._batch_hset_coalesce_max_batches = _env_int("MATRIXARK_RUST_PROXY_BATCH_HSET_COALESCE_MAX_BATCHES", "32")
    target._batch_hset_coalesce_min_records = _env_int("MATRIXARK_RUST_PROXY_BATCH_HSET_COALESCE_MIN_RECORDS", "16")
    target._batch_hset_coalesce_wait_s = _env_seconds_from_ms("MATRIXARK_RUST_PROXY_BATCH_HSET_COALESCE_WAIT_MS", "0")

    target._batch_hget_coalesce_enabled = _env_bool("MATRIXARK_RUST_PROXY_BATCH_HGET_COALESCE")
    target._batch_hget_coalesce_max_batches = _env_int("MATRIXARK_RUST_PROXY_BATCH_HGET_COALESCE_MAX_BATCHES", "32")
    target._batch_hget_coalesce_min_records = _env_int("MATRIXARK_RUST_PROXY_BATCH_HGET_COALESCE_MIN_RECORDS", "16")
    target._batch_hget_coalesce_wait_s = _env_seconds_from_ms("MATRIXARK_RUST_PROXY_BATCH_HGET_COALESCE_WAIT_MS", "1.0")

    target._append_coalesce_enabled = _env_bool("MATRIXARK_RUST_PROXY_APPEND_COALESCE")
    target._append_coalesce_max_batches = _env_int("MATRIXARK_RUST_PROXY_APPEND_COALESCE_MAX_BATCHES", "32")
    target._append_coalesce_min_records = _env_int("MATRIXARK_RUST_PROXY_APPEND_COALESCE_MIN_RECORDS", "16")
    target._append_coalesce_wait_s = _env_seconds_from_ms("MATRIXARK_RUST_PROXY_APPEND_COALESCE_WAIT_MS", "0.0")

    target._string_cache_enabled = _env_bool("MATRIXARK_RUST_PROXY_STRING_CACHE")
    target._scan_hash_cache_enabled = _env_bool("MATRIXARK_RUST_PROXY_SCAN_HASH_CACHE")
    target._scan_hash_cache_max_entries = _env_int("MATRIXARK_RUST_PROXY_SCAN_HASH_CACHE_MAX_ENTRIES", "1024")
    target._context_pack_response_cache_enabled = _env_bool("MATRIXARK_RUST_PROXY_CONTEXT_PACK_CLIENT_CACHE")
    target._context_pack_response_cache_max_entries = _env_int(
        "MATRIXARK_RUST_PROXY_CONTEXT_PACK_CLIENT_CACHE_MAX_ENTRIES",
        "256",
    )
