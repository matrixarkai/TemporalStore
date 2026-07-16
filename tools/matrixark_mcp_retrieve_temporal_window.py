#!/usr/bin/env python3
"""Temporal-compression candidate window helpers for MatrixArk retrieval."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

try:
    from tools.matrixark_mcp_core import Json
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_core import Json


def raw_event_admission_window(
    records: list[Json],
    *,
    max_raw_events_per_node: int,
    context_event_ingestion_time_ms: Callable[[Json], int],
) -> tuple[dict[Any, set[int]], int]:
    raw_event_ids_by_node: dict[Any, set[int]] = {}
    raw_event_time_window_dropped_count = 0
    events_by_node: dict[Any, list[Json]] = {}
    nodes_with_compression: set[Any] = set()
    for record in records:
        if record.get("record_type") == "context_compression_event":
            node_key_for_compression: Any = record.get("node_hash")
            if node_key_for_compression is None:
                node_key_for_compression = tuple(record.get("node_path", []))
            nodes_with_compression.add(node_key_for_compression)
            continue
        if record.get("record_type") != "context_event":
            continue
        if record.get("source_chunk_hash"):
            continue
        node_key: Any = record.get("node_hash")
        if node_key is None:
            node_key = tuple(record.get("node_path", []))
        events_by_node.setdefault(node_key, []).append(record)
    for node_key, node_events in events_by_node.items():
        if node_key not in nodes_with_compression:
            continue
        node_events.sort(
            key=lambda item: (
                context_event_ingestion_time_ms(item),
                int(item.get("event_id_hash") or 0),
            ),
            reverse=True,
        )
        admitted = {
            int(record.get("event_id_hash"))
            for record in node_events[:max_raw_events_per_node]
            if record.get("event_id_hash") is not None
        }
        raw_event_ids_by_node[node_key] = admitted
        raw_event_time_window_dropped_count += max(0, len(node_events) - len(admitted))
    return raw_event_ids_by_node, raw_event_time_window_dropped_count
