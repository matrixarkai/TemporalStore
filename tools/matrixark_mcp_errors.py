#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Shared MatrixArk MCP exception and retry classification helpers."""

from __future__ import annotations

from typing import Any


class MatrixArkError(ValueError):
    pass


def is_retryable_temporalstore_error(error: Any) -> bool:
    text = str(error).lower()
    retryable_fragments = (
        "slot not found",
        "partition info not found",
        "partition no primary",
        "no primary",
        "not ready",
        "unavailable",
        "timed out",
        "timeout",
        "connection refused",
        "connection reset",
        "temporarily unavailable",
        "server is busy",
        # The engine's load-window statuses: a shard that has not loaded yet, or is mid
        # WAL-replay ("shard_not_loaded: shard is recovering"), answers again once the load
        # completes. Treating these as terminal made callers give up -- or worse, serve the
        # store as empty -- during exactly the window a retry would have covered.
        "not loaded",
        "recovering",
    )
    return any(fragment in text for fragment in retryable_fragments)
