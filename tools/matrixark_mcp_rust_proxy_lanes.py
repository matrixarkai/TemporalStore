#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Lane pool helpers for the MatrixArk Rust proxy client."""

from __future__ import annotations

import threading
from typing import Any


Json = dict[str, Any]


def make_lanes(count: int) -> list[Json]:
    return [
        {
            "proc": None,
            "lock": threading.Lock(),
            "semaphore": threading.BoundedSemaphore(1),
        }
        for _ in range(count)
    ]


def build_lane_pools(
    *,
    shared_process_mode: bool,
    dedicated_pack_lanes_enabled: bool,
    write_lane_count: int,
    read_lane_count: int,
    pack_lane_count: int,
    control_lane_count: int,
) -> dict[str, list[Json]]:
    if shared_process_mode:
        # The local Rust TemporalEngine is embedded in the proxy process. A
        # multi-process write lane pool can hide writes from reads until there
        # is a real shared server/proxy behind it, so writes/control stay on one
        # process. Retrieve-pack is read-mostly and may use warm process lanes.
        shared_lanes = make_lanes(1)
        pack_lanes = make_lanes(pack_lane_count) if dedicated_pack_lanes_enabled else shared_lanes
        return {
            "write": shared_lanes,
            "read": shared_lanes,
            "pack": pack_lanes,
            "control": shared_lanes,
        }
    return {
        "write": make_lanes(write_lane_count),
        "read": make_lanes(read_lane_count),
        "pack": make_lanes(pack_lane_count),
        "control": make_lanes(control_lane_count),
    }
