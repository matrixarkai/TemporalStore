#!/usr/bin/env python3
"""Rust proxy call-metrics primitive helpers."""

from __future__ import annotations

import json
import math
from typing import Any

Json = dict[str, Any]


def nested_float(payload: Json, *paths: str) -> float:
    for path in paths:
        current: Any = payload
        for part in path.split("."):
            if not isinstance(current, dict) or part not in current:
                current = None
                break
            current = current[part]
        if current is None:
            continue
        try:
            return float(current)
        except (TypeError, ValueError):
            continue
    return 0.0


def count_context_record(context_record_counts: dict[str, int], value: Any) -> None:
    if not isinstance(value, str) or not value.startswith("{"):
        return
    if '"record_type"' not in value:
        return
    try:
        payload = json.loads(value)
    except Exception:
        return
    record_type = str(payload.get("record_type") or "")
    if not record_type:
        return
    context_record_counts[record_type] = context_record_counts.get(record_type, 0) + 1


def percentile(values: list[float], percentile_value: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    index = min(len(ordered) - 1, max(0, math.ceil(percentile_value * len(ordered)) - 1))
    return ordered[index]
