#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""TemporalStore direct adapter record-loading helpers."""

from __future__ import annotations

import json
from typing import Any

Json = dict[str, Any]


def load_records_by_count(adapter: Any, count: int) -> list[Json]:
    records: list[Json] = []
    adapter._last_read_all_native_shard_scan = False
    scan_records = adapter._load_records_by_native_shard_scan(count)
    if scan_records is not None:
        adapter._last_read_all_native_shard_scan = True
        return scan_records
    batch_hget = getattr(adapter._client, "batch_hget", None)
    if callable(batch_hget):
        entries = []
        for sequence in range(count):
            record_key, record_id = adapter._record_location(sequence)
            entries.append({"key": record_key, "field": record_id})
        try:
            read_records = batch_hget(entries)
        except Exception:
            read_records = []
        for item in read_records:
            if not isinstance(item, dict):
                continue
            payload = item.get("value", "")
            if not payload:
                continue
            decoded = json.loads(str(payload))
            if isinstance(decoded, dict) and isinstance(decoded.get("record_bundle"), list):
                records.extend(item for item in decoded["record_bundle"] if isinstance(item, dict))
            elif isinstance(decoded, dict):
                records.append(decoded)
        if records or count == 0:
            return records
    for sequence in range(count):
        record_key, record_id = adapter._record_location(sequence)
        try:
            payload = adapter._client.hget(record_key, record_id)
        except Exception:
            continue
        if not payload:
            continue
        decoded = json.loads(payload)
        if isinstance(decoded, dict) and isinstance(decoded.get("record_bundle"), list):
            records.extend(item for item in decoded["record_bundle"] if isinstance(item, dict))
        elif isinstance(decoded, dict):
            records.append(decoded)
    return records


def load_records_by_native_shard_scan(adapter: Any, count: int) -> list[Json] | None:
    scanner = getattr(getattr(adapter, "_client", None), "scan_hash", None)
    if not callable(scanner) or count <= 0:
        return None
    max_shard = (count - 1) // adapter._shard_size
    records_by_sequence: list[tuple[int, Json]] = []
    for shard in range(max_shard + 1):
        key = f"{adapter._record_hash_key}:{shard:06d}"
        try:
            response = scanner(key)
        except Exception:
            return None
        rows = response.get("records") if isinstance(response, dict) else None
        if not isinstance(rows, list):
            return None
        for row in rows:
            if not isinstance(row, dict):
                continue
            field = str(row.get("field") or "")
            value = row.get("value")
            if not field or not isinstance(value, str):
                continue
            try:
                offset = int(field)
                decoded = json.loads(value)
            except Exception:
                continue
            sequence = shard * adapter._shard_size + offset
            if sequence >= count:
                continue
            if isinstance(decoded, dict) and isinstance(decoded.get("record_bundle"), list):
                for item in decoded["record_bundle"]:
                    if isinstance(item, dict):
                        records_by_sequence.append((sequence, item))
            elif isinstance(decoded, dict):
                records_by_sequence.append((sequence, decoded))
    records_by_sequence.sort(key=lambda item: item[0])
    return [record for _, record in records_by_sequence]


def record_location(adapter: Any, sequence: int) -> tuple[str, str]:
    shard = sequence // adapter._shard_size
    offset = sequence % adapter._shard_size
    return f"{adapter._record_hash_key}:{shard:06d}", f"{offset:020d}"


def load_records(adapter: Any, index: list[str]) -> list[Json]:
    records = []
    for record_id in index:
        try:
            payload = adapter._client.hget(adapter._record_hash_key, record_id)
        except Exception:
            continue
        if not payload:
            continue
        records.append(json.loads(payload))
    return records
