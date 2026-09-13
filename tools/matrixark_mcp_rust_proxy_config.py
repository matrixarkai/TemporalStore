#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Configuration helpers for the MatrixArk Rust proxy client."""

from __future__ import annotations

import os
from typing import Any


try:  # pragma: no cover - import shape differs when run as a package
    from matrixark_mcp_env import FALSE_VALUES, env_bool
except ImportError:  # pragma: no cover
    from tools.matrixark_mcp_env import FALSE_VALUES, env_bool


def _env_bool(name: str, default: str = "1") -> bool:
    """A boolean flag, in the one vocabulary.

    This used to be `... not in {"0", "false", "no"}` -- a DENY list where every other reader in
    the tree uses an allow list, and one that is missing `off`. So `MATRIXARK_RUST_PROXY_SHARED_
    PROCESS=off` left the flag ON, and so did `=disabled`. test_env_flag_vocabulary settled this
    vocabulary after boolean flags were found parsed six different ways, and it did not catch this
    one: its scan reads `os.environ.get("NAME")` written out, and this reads `os.environ.get(name)`
    where the name is a PARAMETER. The same blind spot hid seventy flags from the surface count.

    Delegating rather than restating: a vocabulary stated twice is the thing that was being fixed.
    """
    return env_bool(name, default.strip().lower() not in FALSE_VALUES)


def _raw(name: str, default: str) -> str:
    """The value, with a BLANK treated as unset.

    `MATRIXARK_RUST_PROXY_WRITE_LANES=` -- an export with nothing after the `=`, which is what a
    shell leaves behind when a variable is built from another that is empty -- reached `int("")`
    and raised ValueError out of lane configuration. Every other reader in this tree spells the
    read `os.environ.get(name, "").strip() or default`, so blank means unset for all of them; these
    two were the only readers where it meant crash.

    A value that is malformed rather than absent still raises, and deliberately: `int("abc")` is an
    operator error nothing can guess past, and the alternative is a proxy that quietly runs on four
    lanes because somebody typed `four`. Blank is different because it carries no intent.
    """
    value = os.environ.get(name)
    if value is None or not value.strip():
        return default
    return value.strip()


def _env_int(name: str, default: str, *, minimum: int = 1) -> int:
    return max(minimum, int(_raw(name, default)))


def _env_seconds_from_ms(name: str, default_ms: str, *, minimum: float = 0.0) -> float:
    return max(minimum, float(_raw(name, default_ms)) / 1000.0)


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
