#!/usr/bin/env python3
"""MatrixArk MCP ingestion envelope normalization."""

from __future__ import annotations

from typing import Any

try:
    from tools.matrixark_mcp_errors import MatrixArkError
    from tools.matrixark_mcp_identity import now_ms
    from tools.matrixark_mcp_storage_options import (
        canonical_storage_route,
        default_async_ingest_storage_options,
        normalize_record_storage_options,
        normalize_storage_options,
    )
    from tools.matrixark_mcp_validation import optional_object, require_messages
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_errors import MatrixArkError
    from matrixark_mcp_identity import now_ms
    from matrixark_mcp_storage_options import (
        canonical_storage_route,
        default_async_ingest_storage_options,
        normalize_record_storage_options,
        normalize_storage_options,
    )
    from matrixark_mcp_validation import optional_object, require_messages


Json = dict[str, Any]


def normalize_envelope(args: Json, *, default_kind: str) -> Json:
    messages = require_messages(args)
    scope = optional_object(args, "scope")
    metadata = optional_object(args, "metadata")
    kind = args.get("kind", default_kind)
    if kind not in {"message", "feedback", "resource", "skill", "business_data"}:
        raise MatrixArkError("kind is invalid")
    storage_options = normalize_storage_options(args, metadata)
    if not storage_options:
        storage_options = default_async_ingest_storage_options()
        storage_options["request_level"] = True
    record_storage_options = normalize_record_storage_options(args, metadata, storage_options)
    envelope: Json = {
        "kind": kind,
        "messages": messages,
        "scope": scope,
        "metadata": metadata,
        "ingestion_time_ms": now_ms(),
        "storage_options": storage_options,
    }
    if record_storage_options:
        envelope["record_storage_options"] = record_storage_options
        envelope["part_storage_options"] = record_storage_options
    envelope["storage_route"] = canonical_storage_route(envelope.get("storage_options", {}))
    for field in [
        "context_pack_id",
        "query_id_hash",
        "accepted_refs",
        "rejected_refs",
        "raw_uri",
        "resource_type",
        "source_event_ids",
        "segment_provider",
        "segment_model",
        "segment_model_path",
        "segment_max_new_tokens",
        "segment_provider_fallback",
        "understanding_provider",
        "extraction_provider",
    ]:
        if field in args:
            envelope[field] = args[field]
    return envelope
