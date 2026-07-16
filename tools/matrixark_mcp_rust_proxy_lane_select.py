#!/usr/bin/env python3
"""Lane selection policy for the MatrixArk Rust proxy client."""

from __future__ import annotations

import hashlib
import json

try:
    from tools.matrixark_mcp_core import Json
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_core import Json


WRITE_OPS = {
    "batch_hset",
    "matrixark_append_records",
    "matrixark_batch_append_records",
    "matrixark_batch_append_raw_ingestion_records",
    "hset",
    "put_string",
    "write_matrixark_record",
    "write_matrixark_records",
}

PACK_OPS = {"matrixark_retrieve_context_pack"}

READ_OPS = {
    "batch_hget",
    "hgetall",
    "scan_hash",
    "hget",
    "get_string",
    "read_matrixark_record",
    "read_matrixark_records",
}


def lane_group_for_op(op: str) -> str:
    if op in WRITE_OPS:
        return "write"
    if op in PACK_OPS:
        return "pack"
    if op in READ_OPS:
        return "read"
    return "control"


def pack_lane_sticky_index(lanes: list[Json], kwargs: Json) -> int | None:
    if not lanes or len(lanes) <= 1:
        return None
    request = kwargs.get("record")
    if isinstance(request, dict):
        query_id = request.get("query_id")
        if isinstance(query_id, int):
            return query_id % len(lanes)
        try:
            if query_id is not None:
                return int(str(query_id)) % len(lanes)
        except (TypeError, ValueError):
            pass
    query = request.get("query") if isinstance(request, dict) else ""
    ranking = request.get("ranking") if isinstance(request, dict) else {}
    sticky_payload = {
        "count_key": kwargs.get("count_key"),
        "record_hash_key": kwargs.get("record_hash_key"),
        "scope": kwargs.get("scope"),
        "secondary_index_groups": kwargs.get("secondary_index_groups"),
        "query": query,
        "max_selected_refs": ranking.get("max_selected_refs") if isinstance(ranking, dict) else None,
    }
    try:
        encoded = json.dumps(sticky_payload, sort_keys=True, separators=(",", ":")).encode()
    except Exception:
        return None
    digest = hashlib.blake2b(encoded, digest_size=8).digest()
    return int.from_bytes(digest, "big") % len(lanes)
