#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""The two raw-ingestion normalisers the adapters call.

This module once held an extraction of the whole raw-ingestion path -- an append function, a
`RawIngestionAdapterMixin` carrying thirteen methods, and a raw session index. Nothing ever
mixed the class in and nothing ever called the function: the live adapters take their
`_append_raw_ingestion_records` from `_TemporalDirectBackendMixin`, and the only names imported
from here are the two below. The copy here had drifted into the weaker one -- no backend metric
fields, no write-debug stamping, no append-options wrapper -- so adopting it later would have
cost those silently. Removed rather than repaired, because a second implementation nothing runs
cannot be trusted to still match the one that does.
"""

from __future__ import annotations

from typing import Any

try:
    from tools.matrixark_mcp_errors import MatrixArkError
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_errors import MatrixArkError


Json = dict[str, Any]


def normalize_raw_storage_backend(value: Any) -> str:
    backend = str(value or "temporalstore").strip().lower().replace("-", "_")
    if backend in {"", "temporal", "temporal_store", "ts"}:
        backend = "temporalstore"
    if backend in {"matrix_kv", "kv"}:
        backend = "matrixkv"
    if backend in {
        "matrix_object",
        "matrixobjectstore",
        "matrix_object_store",
        "objectstore",
        "object_store",
        "object",
        "blob",
        "blobstore",
        "blob_store",
    }:
        backend = "matrixobject"
    if backend in {"aws_s3", "s3_object", "s3_objectstore"}:
        backend = "s3"
    if backend not in {"temporalstore", "matrixkv", "s3", "matrixobject"}:
        raise MatrixArkError(
            "MATRIXARK_RAW_INGESTION_BACKEND must be temporalstore, matrixkv, s3, or matrixobject"
        )
    return backend


def normalize_raw_ingestion_record(record: Json) -> Json:
    normalized = dict(record)
    if normalized.get("record_type") == "agent_message":
        normalized.setdefault("raw_record_type", "raw_agent_message")
    else:
        normalized.setdefault("raw_record_type", "raw_ingestion_event")
    normalized.setdefault("raw_ingestion_visibility", "backfill_only")
    normalized.setdefault("serving_visible", False)
    normalized.setdefault("session_binding", "metadata_only_for_backfill_batching")
    return normalized
