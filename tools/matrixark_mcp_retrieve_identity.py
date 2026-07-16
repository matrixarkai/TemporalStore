#!/usr/bin/env python3
"""Record identity helpers for MatrixArk retrieval."""

from __future__ import annotations

from typing import Any

try:
    from tools.matrixark_mcp_core import Json, context_index_ref_hashes, json, stable_hash
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_core import Json, context_index_ref_hashes, json, stable_hash


PRIMARY_IDENTITY_FIELDS = (
    "event_id_hash",
    "entity_hash",
    "segment_hash",
    "compression_id_hash",
    "summary_hash",
    "chunk_hash",
    "section_hash",
    "skill_hash",
    "resource_hash",
    "batch_id_hash",
)


def record_identity(record: Json) -> tuple[str, Any]:
    record_type = str(record.get("record_type") or "")
    for field in PRIMARY_IDENTITY_FIELDS:
        if record.get(field) is not None:
            return (record_type, record.get(field))
    if record_type == "context_index":
        return (
            record_type,
            (
                record.get("index_name"),
                record.get("node_hash"),
                tuple(context_index_ref_hashes(record)),
                record.get("timestamp_key_ms"),
            ),
        )
    return (record_type, stable_hash(json.dumps(record, sort_keys=True, separators=(",", ":"))))


def append_unique_records(records: list[Json], candidate_records: list[Json]) -> None:
    seen_record_identities = {record_identity(record) for record in records}
    for record in candidate_records:
        identity = record_identity(record)
        if identity in seen_record_identities:
            continue
        records.append(record)
        seen_record_identities.add(identity)
