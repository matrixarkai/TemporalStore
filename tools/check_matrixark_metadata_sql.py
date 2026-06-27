#!/usr/bin/env python3
"""Probe MatrixArk SQL metadata deployment and table readiness."""

from __future__ import annotations

import json
import os
import tempfile
from pathlib import Path

from matrixark_access import MatrixArkSqlMetadataStore, build_matrixark_metadata_store
from matrixark_mcp_server import MatrixArkLocalAdapter
from matrixark_mcp_core import now_ms, stable_hash


def main() -> int:
    backend = os.environ.get("MATRIXARK_METADATA_BACKEND", "mysql")
    dsn = os.environ.get("MATRIXARK_METADATA_DSN", "")
    if backend in {"mysql", "matrixkv_sql", "bytekv_sql"} and not dsn:
        raise SystemExit("MATRIXARK_METADATA_DSN is required for mysql/matrixkv_sql/bytekv_sql probe")

    tmp = tempfile.TemporaryDirectory()
    adapter = MatrixArkLocalAdapter(Path(tmp.name) / "metadata_probe.jsonl")
    store = build_matrixark_metadata_store(adapter)
    if not isinstance(store, MatrixArkSqlMetadataStore):
        raise SystemExit(f"expected SQL metadata store, got {store.backend_name}")
    ready = store.check_ready()
    probe_record = {
        "record_type": "matrixark_metadata_probe",
        "account_id": "acct_metadata_probe",
        "tenant_id": "tenant_metadata_probe",
        "user_id": "user_metadata_probe",
        "probe_id_hash": stable_hash(f"metadata-probe:{now_ms()}"),
        "created_at_ms": now_ms(),
    }
    store.append(probe_record)
    records = store.read_all()
    found = any(row.get("probe_id_hash") == probe_record["probe_id_hash"] for row in records)
    result = {
        "status": "ok" if found else "failed",
        "ready": ready,
        "backend_info": store.backend_info(),
        "probe_found": found,
        "records_seen": len(records),
        "normalized_counts": store.normalized_counts(),
    }
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0 if found else 2


if __name__ == "__main__":
    raise SystemExit(main())
