#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Caller-side SDK helper for ingesting a large skill/resource file from the
customer's OWN machine into a REMOTE MatrixArk / TemporalStore deployment.

The file lives on the caller; the server is remote. Rather than shipping the raw
bytes inside a JSON ingest body (which caps out and re-sends on every retry), the
helper content-addresses the file, streams it to the server's blob tier once, and
then ingests a tiny pointer (``temporalstore://<key>``). Only ``http.client`` +
``json`` from the stdlib are used, mirroring ``matrixark_temporalstore_blob``.

Two entry points:

  * ``ingest_large_file(path, *, base_url, api_key, ...)`` — upload a local file to
    the blob tier, then ingest the pointer. One helper, two round trips (PUT blob,
    POST ingest). For a single HTTP round trip use the server's ``/v1/ingest_file``
    endpoint directly (see ``docs/ingest_large_files.md``).
  * ``ingest_text(text, *, base_url, api_key, ...)`` — post small content inline in
    the ingest body; no upload.

Both entry points accept an optional ``sharing_scope`` (alias ``visibility``) — one of
``private_user`` / ``tenant_shared`` / ``global_shared`` — surfaced as the top-level
``sharing_scope`` key the server already consumes. Omit it to keep the server default.

RETRY GUIDANCE (important)
--------------------------
On ANY error — network blip, timeout, 5xx, a partial/corrupt upload — the caller
should simply call ``ingest_large_file`` again with the same arguments. It is
idempotent: the key is derived from a SHA-256 of the file bytes, so the SAME bytes
always produce the SAME content-addressed key. Re-uploading is de-duplicated by the
blob tier (a no-op replace under the identical key), so retries never create
duplicates and cost almost nothing. The server also integrity-checks the blob on
fetch (verifying the bytes still hash to the key), so a corrupt or partial upload
fails loudly at ingest time rather than silently ingesting bad data — which is
exactly the signal to retry. There is no retry cap to worry about; just retry.
"""
from __future__ import annotations

import http.client
import json
import os
from typing import Any, Optional
from urllib.parse import urlsplit

try:  # top-level (run from tools/) ...
    from matrixark_temporalstore_blob import content_hash, content_key, blob_uri
except ImportError:  # ... or package path.
    from tools.matrixark_temporalstore_blob import content_hash, content_key, blob_uri  # type: ignore

Json = dict[str, Any]

# Accepted values for the top-level ``sharing_scope`` ingest knob. The server's
# resource_sharing_scope / registry_access_scope read this off the ingest body.
VALID_SHARING_SCOPES = ("private_user", "tenant_shared", "global_shared")


def _resolve_sharing_scope(sharing_scope: Optional[str], visibility: Optional[str]) -> Optional[str]:
    """Resolve and validate the sharing scope. ``visibility`` is an alias for
    ``sharing_scope`` (``sharing_scope`` wins when both are given). Returns the
    validated value, or None when neither is provided. Raises ``ValueError`` on an
    unknown non-None value."""
    value = sharing_scope if sharing_scope is not None else visibility
    if value is None:
        return None
    if value not in VALID_SHARING_SCOPES:
        raise ValueError(
            f"unknown sharing_scope {value!r} (use one of {', '.join(VALID_SHARING_SCOPES)})")
    return value


# resource_type -> HTTP content-type for the blob PUT (best-effort; the server does
# not depend on it for correctness, it re-derives the parser from the suffix).
_CONTENT_TYPES = {
    "md": "text/markdown",
    "markdown": "text/markdown",
    "txt": "text/plain",
    "text": "text/plain",
    "pdf": "application/pdf",
    "json": "application/json",
    "html": "text/html",
    "csv": "text/csv",
}


def _infer_resource_type(path: str) -> str:
    """Derive a resource_type from a filename suffix; default ``md`` (skills are
    markdown, and the server re-infers anyway)."""
    suffix = os.path.splitext(str(path))[1].lstrip(".").lower()
    return suffix or "md"


def _content_type_for(resource_type: str) -> str:
    return _CONTENT_TYPES.get((resource_type or "").lower(), "application/octet-stream")


def _connect(base_url: str, timeout: float) -> http.client.HTTPConnection:
    parsed = urlsplit(base_url)
    scheme = parsed.scheme or "http"
    host = parsed.hostname or "127.0.0.1"
    port = parsed.port or (443 if scheme == "https" else 80)
    if scheme == "https":
        return http.client.HTTPSConnection(host, port, timeout=timeout)
    return http.client.HTTPConnection(host, port, timeout=timeout)


def _put_blob(base_url: str, api_key: str, key: str, data: bytes,
              content_type: str, timeout: float) -> Json:
    """Stream ``PUT {base_url}/v1/blob/<key>`` with a Bearer token. Content-addressed
    and idempotent: re-PUTting the same key replaces byte-identical content (no dup)."""
    conn = _connect(base_url, timeout)
    try:
        conn.putrequest("PUT", f"/v1/blob/{key.lstrip('/')}")
        conn.putheader("Authorization", f"Bearer {api_key}")
        conn.putheader("Content-Length", str(len(data)))
        conn.putheader("Content-Type", content_type)
        conn.endheaders()
        conn.send(data)
        resp = conn.getresponse()
        raw = resp.read()
        status = int(getattr(resp, "status", 0) or 0)
    finally:
        try:
            conn.close()
        except Exception:
            pass
    if status >= 400:
        raise RuntimeError(
            f"blob upload PUT /v1/blob/{key} failed: HTTP {status}: {raw[:256]!r} "
            f"(idempotent — safe to retry the same call)")
    try:
        return json.loads(raw) if raw else {}
    except (json.JSONDecodeError, ValueError):
        return {}


def _post_json(base_url: str, api_key: str, path: str, body: Json, timeout: float) -> Json:
    data = json.dumps(body).encode("utf-8")
    conn = _connect(base_url, timeout)
    try:
        conn.putrequest("POST", path)
        conn.putheader("Authorization", f"Bearer {api_key}")
        conn.putheader("Content-Type", "application/json")
        conn.putheader("Content-Length", str(len(data)))
        conn.endheaders()
        conn.send(data)
        resp = conn.getresponse()
        raw = resp.read()
        status = int(getattr(resp, "status", 0) or 0)
    finally:
        try:
            conn.close()
        except Exception:
            pass
    if status >= 400:
        raise RuntimeError(
            f"ingest POST {path} failed: HTTP {status}: {raw[:256]!r} "
            f"(content-addressed + idempotent — safe to retry the same call)")
    try:
        return json.loads(raw) if raw else {}
    except (json.JSONDecodeError, ValueError):
        return {"raw": raw.decode("utf-8", "replace")}


def _upload_matrixobject(data: bytes, content_type: str, *, kind: str,
                         scope: Optional[Json]) -> str:
    """Upload to the object store (requires object-store env config on the CALLER:
    MATRIXARK_OBJECT_RPC_URL or MATRIXARK_OBJECT_STORE_DIR) and return a
    ``matrixobject://<bucket>/<key>`` URI."""
    try:
        from matrixark_object_store import MatrixObjectClient
    except ImportError:
        from tools.matrixark_object_store import MatrixObjectClient  # type: ignore
    client = MatrixObjectClient()
    result = client.put(data, content_type=content_type,
                        metadata={"kind": kind, "scope": scope or {}})
    if not result.uri:
        raise RuntimeError(
            "matrixobject backend is not configured on the caller (inline mode has "
            "nothing to reference); set MATRIXARK_OBJECT_RPC_URL or "
            "MATRIXARK_OBJECT_STORE_DIR, or use backend='temporalstore'")
    return result.uri


def ingest_large_file(path: str, *, base_url: str, api_key: str,
                      kind: str = "skill", resource_type: Optional[str] = None,
                      backend: str = "temporalstore", scope: Optional[Json] = None,
                      sharing_scope: Optional[str] = None,
                      visibility: Optional[str] = None,
                      wait: bool = False, timeout: float = 60.0) -> dict:
    """Ingest a large local skill/resource file into a remote deployment.

    Reads the file on the caller side, content-addresses it, streams it to the
    server's blob tier ONCE, then ingests a tiny ``temporalstore://<key>`` (or
    ``matrixobject://<bucket>/<key>``) pointer. Idempotent on the file bytes: retry
    the exact same call on any error (see the module RETRY GUIDANCE).

    Args:
        path: local path to the file (on the caller's machine).
        base_url: the remote gateway base URL, e.g. ``https://api.example.com``.
        api_key: Bearer token for ``Authorization``.
        kind: ``"skill"`` or ``"resource"`` (default ``"skill"``).
        resource_type: ``md``/``pdf``/``txt``/…; inferred from the suffix when None.
        backend: ``"temporalstore"`` (default, uploads over ``base_url``) or
            ``"matrixobject"`` (uploads via the object store; needs caller env).
        scope: optional scope object forwarded to ingest (tenant is pinned by auth).
        sharing_scope: optional visibility knob — one of ``private_user`` /
            ``tenant_shared`` / ``global_shared``. Sent as the top-level
            ``sharing_scope`` ingest key; omitted (server default) when None.
        visibility: alias for ``sharing_scope`` (``sharing_scope`` wins if both set).
        wait: when True, request synchronous extraction (maps to ``finalize``);
            otherwise the server fast-acks (202) and extracts asynchronously.
        timeout: per-request socket timeout in seconds.

    Returns:
        The parsed ingest response JSON (dict).
    """
    effective_sharing_scope = _resolve_sharing_scope(sharing_scope, visibility)
    with open(path, "rb") as fh:
        data = fh.read()
    rtype = resource_type or _infer_resource_type(path)
    ctype = _content_type_for(rtype)

    if backend == "matrixobject":
        raw_uri = _upload_matrixobject(data, ctype, kind=kind, scope=scope)
    elif backend == "temporalstore":
        key = content_key(data)  # resources/<sha2>/<sha256> — same bytes -> same key
        _put_blob(base_url, api_key, key, data, ctype, timeout)
        raw_uri = blob_uri(key)  # temporalstore://<key>
    else:
        raise ValueError(f"unknown backend {backend!r} (use 'temporalstore' or 'matrixobject')")

    body: Json = {"kind": kind, "raw_uri": raw_uri, "resource_type": rtype, "wait": bool(wait)}
    if scope is not None:
        body["scope"] = scope
    if effective_sharing_scope is not None:
        body["sharing_scope"] = effective_sharing_scope
    if wait:
        body["finalize"] = True  # synchronous extraction via the gateway's finalize path
    return _post_json(base_url, api_key, "/v1/ingest", body, timeout)


def ingest_text(text: str, *, base_url: str, api_key: str,
                kind: str = "skill", resource_type: str = "md",
                scope: Optional[Json] = None,
                sharing_scope: Optional[str] = None,
                visibility: Optional[str] = None,
                wait: bool = False,
                timeout: float = 60.0) -> dict:
    """Ingest small content inline (no upload). Posts ``text`` directly in the ingest
    body — use this for short skills/resources; use ``ingest_large_file`` for files.

    ``sharing_scope`` (alias ``visibility``) is an optional visibility knob — one of
    ``private_user`` / ``tenant_shared`` / ``global_shared`` — sent as the top-level
    ``sharing_scope`` ingest key; omitted (server default) when None."""
    effective_sharing_scope = _resolve_sharing_scope(sharing_scope, visibility)
    body: Json = {"kind": kind, "text": text, "resource_type": resource_type, "wait": bool(wait)}
    if scope is not None:
        body["scope"] = scope
    if effective_sharing_scope is not None:
        body["sharing_scope"] = effective_sharing_scope
    if wait:
        body["finalize"] = True
    return _post_json(base_url, api_key, "/v1/ingest", body, timeout)


__all__ = ["ingest_large_file", "ingest_text", "content_hash", "content_key",
           "VALID_SHARING_SCOPES"]
