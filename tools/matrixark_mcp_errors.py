#!/usr/bin/env python3
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
    )
    return any(fragment in text for fragment in retryable_fragments)
