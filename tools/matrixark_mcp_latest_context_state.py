#!/usr/bin/env python3
"""Latest context-state storage helpers for MatrixArk TemporalStore adapters."""

from __future__ import annotations

import json

try:
    from tools.matrixark_mcp_core import Json
    from tools.matrixark_mcp_serving_records import (
        compact_latest_context_state_records,
        latest_context_state_key,
    )
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_core import Json
    from matrixark_mcp_serving_records import (
        compact_latest_context_state_records,
        latest_context_state_key,
    )


def latest_context_state_storage_key(storage_prefix: str) -> str:
    return f"{storage_prefix}:context_latest_state"


def latest_context_state_field(record: Json) -> str | None:
    key = latest_context_state_key(record)
    if key is None:
        return None
    return ":".join(str(part) for part in key)


def latest_context_state_payload(record: Json) -> str:
    payload_record = dict(record)
    payload_record.pop("summary_version_hash", None)
    return json.dumps(payload_record, sort_keys=True, separators=(",", ":"))


def latest_context_state_entries(storage_prefix: str, records: list[Json]) -> list[Json]:
    entries: list[Json] = []
    latest_state_key = latest_context_state_storage_key(storage_prefix)
    for record in compact_latest_context_state_records(records):
        field = latest_context_state_field(record)
        if not field:
            continue
        entries.append(
            {
                "key": latest_state_key,
                "field": field,
                "value": latest_context_state_payload(record),
                "storage_route": record.get("storage_route") if isinstance(record.get("storage_route"), dict) else {},
            }
        )
    return entries


def append_log_records_without_latest_state(records: list[Json]) -> list[Json]:
    return [record for record in records if latest_context_state_key(record) is None]
