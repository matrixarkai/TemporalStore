#!/usr/bin/env python3
"""Latest context-state storage helpers for MatrixArk TemporalStore adapters."""

from __future__ import annotations

import json

try:
    from tools.matrixark_mcp_core import Json, candidate_access_scope, scope_matches
    from tools.matrixark_mcp_serving_records import (
        compact_latest_context_state_records,
        latest_context_state_key,
    )
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_core import Json, candidate_access_scope, scope_matches
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


class LatestContextStateAdapterMixin:
    """Adapter methods for latest context-state storage and retrieval."""

    def _latest_context_state_key(self) -> str:
        return latest_context_state_storage_key(self._storage_prefix)

    def _latest_context_state_field(self, record: Json) -> str | None:
        return latest_context_state_field(record)

    def _append_log_records(self, records: list[Json]) -> list[Json]:
        return append_log_records_without_latest_state(records)

    def _split_compacted_latest_context_state(self, records: list[Json]) -> tuple[list[Json], list[Json]]:
        latest_state_entries: list[Json] = []
        append_records_for_log: list[Json] = []
        for record in records:
            field = self._latest_context_state_field(record)
            if not field:
                append_records_for_log.append(record)
                continue
            latest_state_entries.extend(latest_context_state_entries(self._storage_prefix, [record]))
        return latest_state_entries, append_records_for_log

    def _load_latest_context_state_records(self) -> list[Json]:
        scanner = getattr(getattr(self, "_client", None), "scan_hash", None)
        if not callable(scanner):
            return []
        try:
            response = scanner(self._latest_context_state_key())
        except Exception:
            return []
        rows = response.get("records") if isinstance(response, dict) else []
        records: list[Json] = []
        for row in rows if isinstance(rows, list) else []:
            if not isinstance(row, dict):
                continue
            value = row.get("value")
            if not isinstance(value, str) or not value:
                continue
            try:
                decoded = json.loads(value)
            except Exception:
                continue
            if isinstance(decoded, dict):
                records.append(decoded)
        return records

    def _with_latest_context_state_records(self, records: list[Json]) -> list[Json]:
        return compact_latest_context_state_records(list(records) + self._load_latest_context_state_records())

    def _latest_context_state_records_for_candidate_scan(
        self,
        *,
        scope: Json,
        record_types: set[str],
        selected_node_hashes: set[int] | None,
    ) -> list[Json]:
        selected = {int(item) for item in (selected_node_hashes or set())}
        filtered: list[Json] = []
        for record in self._load_latest_context_state_records():
            record_type = str(record.get("record_type") or "")
            if record_type not in record_types:
                continue
            if not scope_matches(candidate_access_scope(record), scope):
                continue
            if selected:
                try:
                    node_hash = int(record.get("node_hash") or 0)
                except (TypeError, ValueError):
                    node_hash = 0
                if node_hash and node_hash not in selected:
                    continue
            filtered.append(record)
        return filtered
