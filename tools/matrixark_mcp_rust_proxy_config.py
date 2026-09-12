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

    # The nineteen settings below were read from the environment and nothing could set them: every
    # reader of each sits in a module unreachable from any production entry point, so the value an
    # operator exported never arrived anywhere. They are folded to the value they already produced,
    # so the behaviour of this module is unchanged and only the knob is gone.
    #
    # Each folded value was computed by RUNNING the expression it replaces with the variable unset,
    # not by reading the default out of the call. Two would have been wrong read that way:
    # `_env_bool` defaults to "1", so every coalescer and cache here is ON and folding them to
    # False would have turned the lot off; and `_env_seconds_from_ms(..., "1.0")` is 0.001 seconds,
    # not 1.0.
    target._batch_hset_coalesce_enabled = True
    target._batch_hset_coalesce_max_batches = 32
    target._batch_hset_coalesce_min_records = 16
    target._batch_hset_coalesce_wait_s = 0.0

    target._batch_hget_coalesce_enabled = True
    target._batch_hget_coalesce_max_batches = 32
    target._batch_hget_coalesce_min_records = 16
    target._batch_hget_coalesce_wait_s = 0.001

    target._append_coalesce_enabled = True
    target._append_coalesce_max_batches = 32
    target._append_coalesce_min_records = 16
    target._append_coalesce_wait_s = 0.0

    target._string_cache_enabled = True
    target._scan_hash_cache_enabled = True
    target._scan_hash_cache_max_entries = 1024
    target._context_pack_response_cache_enabled = True
    target._context_pack_response_cache_max_entries = 256
