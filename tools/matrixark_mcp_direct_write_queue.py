#!/usr/bin/env python3
"""Direct-write queue helper functions for MatrixArk TemporalStore adapters."""

from __future__ import annotations

import json
from typing import Any

try:
    from tools.matrixark_mcp_identity import now_ms, stable_hash
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_identity import now_ms, stable_hash


Json = dict[str, Any]


def direct_write_durable_payload(
    records: list[Json],
    *,
    backend: str,
    storage_prefix: str,
) -> Json:
    created_at = now_ms()
    return {
        "queue_version": 1,
        "status": "pending",
        "attempts": 0,
        "created_at_ms": created_at,
        "updated_at_ms": created_at,
        "record_count": len(records),
        "backend": backend,
        "storage_prefix": storage_prefix,
        "records": records,
    }


def direct_write_durable_field(payload: Json) -> str:
    digest = stable_hash(json.dumps(payload, sort_keys=True, separators=(",", ":")))
    return f"{int(payload.get('created_at_ms') or now_ms()):020d}:{digest}"
