#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Local idempotency records for the MatrixArk MCP adapter.

Phase-2 size lever -- SLIM idempotency response
------------------------------------------------
An idempotency record exists so a REPLAYED (duplicate) write returns the original result WITHOUT
re-executing the side effect. The dedup contract is satisfied purely by the presence of the record
keyed on ``key_hash`` (the replay path short-circuits execution before the tool runs). The full
``response`` payload that the first call already returned to the caller is re-echoed on every replay;
measured at ~12.7% of on-disk memory it is by far dominated by redundant echoed working data
(``storage_route``, ``resource_*`` status blocks, ``session_buffer``, ``prior_context`` ...), none of
which the replay needs -- the caller received it on the first, non-replay call.

With ``MATRIXARK_SLIM_IDEMPOTENCY_RESPONSE`` ON (default) we persist only the response's stable
IDENTITY/STATUS fields (:data:`IDEMPOTENCY_RESPONSE_KEEP_KEYS`) plus a content hash of the full
original response (``response_content_hash``, for integrity/debuggability), and mark the record
``response_slimmed``. The replay path reads ``response`` exactly as before, so a replay still returns a
valid ``{status, event_id_hash, ...}`` result and is still deduped; it simply omits the redundant
echoed payload. Flag OFF stores the full response -- byte-identical to prior behaviour.
"""

from __future__ import annotations

import json
import os

try:
    from tools.matrixark_mcp_identity import Json, now_ms, stable_hash
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_identity import Json, now_ms, stable_hash


def _bool_env(name: str, default: bool = False) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    return raw.strip().lower() in ("1", "true", "yes", "on")


SLIM_IDEMPOTENCY_RESPONSE = _bool_env("MATRIXARK_SLIM_IDEMPOTENCY_RESPONSE", True)

# Stable identity/status fields a replay caller can rely on. Everything else in the response is
# redundant echoed working data the caller already received on the first (non-replay) call.
IDEMPOTENCY_RESPONSE_KEEP_KEYS = (
    "status",
    "event_id_hash",
    "node_hash",
    "classification",
    "extraction_mode",
    "quality_warning",
    "deleted",
    "memory_id",
    "forgotten",
)


def _response_content_hash(response: Json) -> int:
    try:
        canonical = json.dumps(response, sort_keys=True, separators=(",", ":"), default=str)
    except (TypeError, ValueError):
        canonical = repr(response)
    return stable_hash(canonical)


def slim_idempotency_response(response: Json) -> tuple[Json, int | None]:
    """Return ``(stored_response, content_hash)``. With the flag ON the stored response keeps only the
    identity/status fields and the caller records the content hash; with the flag OFF the full
    response is stored and ``content_hash`` is ``None`` (prior behaviour, byte-identical)."""
    if not SLIM_IDEMPOTENCY_RESPONSE or not isinstance(response, dict):
        return response, None
    slim = {key: response[key] for key in IDEMPOTENCY_RESPONSE_KEEP_KEYS if key in response}
    return slim, _response_content_hash(response)


def build_idempotency_record(
    *,
    key_hash: int,
    tool_name: str,
    raw_key: str,
    identity: Json,
    response: Json,
) -> Json:
    stored_response, content_hash = slim_idempotency_response(response)
    record: Json = {
        "record_type": "matrixark_idempotency",
        "key_hash": key_hash,
        "tool_name": tool_name,
        "raw_key_hash": stable_hash(raw_key),
        "scope_key": identity.get("scope_key", ""),
        "account_id": identity.get("account_id", ""),
        "tenant_id": identity.get("tenant_id", ""),
        "user_id": identity.get("user_id", ""),
        "session_id": identity.get("session_id", ""),
        "response": stored_response,
        "created_at_ms": now_ms(),
    }
    if content_hash is not None:
        record["response_slimmed"] = True
        record["response_content_hash"] = content_hash
    return record


def find_idempotency_record(adapter: object, key_hash: int) -> Json | None:
    for record in reversed(adapter.read_all()):
        if record.get("record_type") == "matrixark_idempotency" and record.get("key_hash") == key_hash:
            return record
    return None


def append_idempotency_record(
    adapter: object,
    *,
    key_hash: int,
    tool_name: str,
    raw_key: str,
    identity: Json,
    response: Json,
) -> None:
    adapter.append(
        build_idempotency_record(
            key_hash=key_hash,
            tool_name=tool_name,
            raw_key=raw_key,
            identity=identity,
            response=response,
        )
    )
