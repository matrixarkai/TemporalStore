#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""TemporalStore-native blob storage for resource/skill raw content (RAFT mode).

A parallel to ``matrixark_object_store`` for deployments that run TemporalStore in
RAFT mode with NO shared object store configured. Instead of the object store, the
raw bytes of a large skill/resource attachment are stored in TemporalStore's OWN
blob tier (the datanode ``POST|PUT|GET /blob/<key>`` endpoint, or the gateway's
``/v1/blob/<key>`` proxy) and referenced by a ``temporalstore://<key>`` URI.

  raw bytes  -> TemporalStore blob store, content-addressed under a ``resources/``
                namespace, referenced as ``temporalstore://<key>``.
  metadata   -> TemporalStore records carry the pointer (cloud_key, stored_raw_uri)
                + serving fields, exactly like the object-store path. The blob never
                sits in the hot serving path.

Backends, resolved like the engine's storage_backend.rs (blob endpoint > inline):
  * temporalstore_blob — the datanode/gateway HTTP blob endpoint (production).
  * inline             — nothing configured; caller keeps the content inline.

This is OSS-safe: it uses TemporalStore's own blob tier, NOT any enterprise object
store. Everything here is the MatrixArk (Python) front; TemporalStore stays in Rust.
"""
from __future__ import annotations

import hashlib
import http.client
import json
import os
from dataclasses import dataclass, field
from typing import Any, Optional
from urllib.parse import urlsplit

Json = dict[str, Any]

# Default datanode blob endpoint (matches TS_SERVER blob port). Used as the client
# target when no endpoint is configured explicitly; NOT the same as "configured"
# for backend resolution (see resolve_ts_blob_backend, which is inline when unset).
DEFAULT_DATANODE_URL = "http://127.0.0.1:17102"
BLOB_URI_SCHEME = "temporalstore"
DEFAULT_KEY_PREFIX = os.environ.get("MATRIXARK_TS_BLOB_PREFIX", "resources")


def _env_blob_url() -> str:
    return os.environ.get("MATRIXARK_TS_BLOB_URL", "").rstrip("/")


def _env_gateway_url() -> str:
    # Optional gateway mode: when set, the client talks to the gateway's
    # `/v1/blob/<key>` proxy instead of the datanode's `/blob/<key>`.
    return os.environ.get("MATRIXARK_TS_BLOB_GATEWAY_URL", "").rstrip("/")


def content_hash(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def content_key(data: bytes, *, prefix: str = DEFAULT_KEY_PREFIX) -> str:
    """Content-addressed, sharded key. Same bytes -> same key -> automatic dedup.

    Shape ``<prefix>/<sha2>/<sha256>`` (e.g. ``resources/ab/<sha256>``), mirroring
    the object store's sharded content-addressing so identical bytes coalesce.
    """
    h = content_hash(data)
    pre = (prefix or "").strip("/")
    return f"{pre}/{h[:2]}/{h}" if pre else f"{h[:2]}/{h}"


def blob_uri(key: str) -> str:
    return f"{BLOB_URI_SCHEME}://{key}"


def _content_addressed_sha(key: str) -> Optional[str]:
    """Return the embedded sha256 hex when ``key`` is content-addressed, else None.

    A content-addressed key has the shape ``<prefix>/<sha2>/<sha256>`` (see
    ``content_key``), i.e. the last path segment is a 64-char lowercase hex digest.
    Keys that don't match (caller-supplied opaque keys) are not integrity-checked.
    """
    seg = str(key or "").rstrip("/").rsplit("/", 1)[-1].lower()
    if len(seg) == 64 and all(c in "0123456789abcdef" for c in seg):
        return seg
    return None


def resolve_ts_blob_backend(*, blob_url: Optional[str] = None,
                            gateway_url: Optional[str] = None) -> str:
    """Pick the blob backend the way storage_backend.rs picks storage: endpoint > inline.

    Returns the configured endpoint URL (gateway preferred, then datanode) or
    ``"inline"`` when nothing is configured. Mirrors ``resolve_object_backend``.
    """
    blob_url = _env_blob_url() if blob_url is None else (blob_url or "").rstrip("/")
    gateway_url = _env_gateway_url() if gateway_url is None else (gateway_url or "").rstrip("/")
    if gateway_url:
        return gateway_url
    if blob_url:
        return blob_url
    return "inline"


@dataclass
class BlobPutResult:
    key: str
    uri: str
    size: int
    content_hash: str
    backend: str
    deduplicated: bool = False
    metadata: Json = field(default_factory=dict)


class TemporalStoreBlobClient:
    """Put/get raw blobs to TemporalStore's own blob tier, content-addressed.

    Talks to the datanode ``/blob/<key>`` endpoint by default, or the gateway's
    ``/v1/blob/<key>`` proxy when a gateway URL is configured (or ``gateway=True``).
    Uses only ``http.client`` from the stdlib, mirroring the gateway's blob proxy.
    """

    def __init__(self, *, base_url: Optional[str] = None, gateway: Optional[bool] = None,
                 key_prefix: str = DEFAULT_KEY_PREFIX, timeout: float = 30.0):
        env_gw = _env_gateway_url()
        env_blob = _env_blob_url()
        if base_url is not None:
            self.base_url = base_url.rstrip("/")
            self.gateway = bool(gateway) if gateway is not None else False
        elif env_gw:
            self.base_url = env_gw
            self.gateway = True if gateway is None else bool(gateway)
        elif env_blob:
            self.base_url = env_blob
            self.gateway = bool(gateway) if gateway is not None else False
        else:
            self.base_url = DEFAULT_DATANODE_URL
            self.gateway = bool(gateway) if gateway is not None else False
        self.path_prefix = "/v1/blob" if self.gateway else "/blob"
        self.key_prefix = key_prefix
        self.timeout = timeout
        # `configured` is False only when nothing was set (pure default) -> inline.
        self.configured = bool(base_url is not None or env_gw or env_blob)

    # -- connection helper ----------------------------------------------------
    def _connect(self) -> http.client.HTTPConnection:
        parsed = urlsplit(self.base_url)
        scheme = parsed.scheme or "http"
        host = parsed.hostname or "127.0.0.1"
        port = parsed.port or (443 if scheme == "https" else 80)
        if scheme == "https":
            return http.client.HTTPSConnection(host, port, timeout=self.timeout)
        return http.client.HTTPConnection(host, port, timeout=self.timeout)

    def _path(self, key: str) -> str:
        return f"{self.path_prefix}/{key.lstrip('/')}"

    # -- public ---------------------------------------------------------------
    def put(self, data: bytes, *, key: Optional[str] = None,
            content_type: str = "application/octet-stream") -> str:
        """Store bytes and return the key. Content-addresses when key is None.

        Idempotent on content hash: identical bytes produce the same key, and the
        datanode replaces any prior object under that key, so a re-put is a no-op.
        """
        if not isinstance(data, (bytes, bytearray)):
            data = str(data).encode("utf-8")
        data = bytes(data)
        if key is None:
            key = content_key(data, prefix=self.key_prefix)
        conn = self._connect()
        try:
            conn.putrequest("PUT", self._path(key))
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
            raise RuntimeError(f"temporalstore blob PUT {key} failed: HTTP {status}: {raw[:256]!r}")
        return key

    def get(self, key: str) -> tuple[bytes, Json]:
        """Fetch bytes for a key. Returns (bytes, meta). Raises on 404/missing."""
        conn = self._connect()
        try:
            conn.putrequest("GET", self._path(key))
            conn.endheaders()
            resp = conn.getresponse()
            status = int(getattr(resp, "status", 0) or 0)
            data = resp.read()
            content_type = resp.getheader("Content-Type") or "application/octet-stream"
            meta_header = resp.getheader("X-Blob-Meta")
        finally:
            try:
                conn.close()
            except Exception:
                pass
        if status == 404:
            raise FileNotFoundError(f"temporalstore blob not found: {key}")
        if status >= 400:
            raise RuntimeError(f"temporalstore blob GET {key} failed: HTTP {status}: {data[:256]!r}")
        # Integrity check: when the key is content-addressed, the downloaded bytes MUST
        # hash back to the sha embedded in the key. A mismatch means a corrupt/partial
        # upload (or truncated download) -- surface it loudly so the customer re-uploads
        # and retries, rather than ingesting bad data. We do NOT auto-retry here.
        expected_sha = _content_addressed_sha(key)
        actual_sha = content_hash(data)
        if expected_sha is not None and actual_sha != expected_sha:
            raise RuntimeError(
                f"temporalstore blob {key} failed integrity check: sha256 mismatch "
                f"(corrupt/partial upload) -- re-upload and retry")
        meta: Json = {"content_type": content_type, "size": len(data),
                      "content_hash": actual_sha}
        if meta_header:
            try:
                extra = json.loads(meta_header)
                if isinstance(extra, dict):
                    meta.update(extra)
            except (json.JSONDecodeError, ValueError):
                pass
        return data, meta

    def exists(self, key: str) -> bool:
        try:
            self.get(key)
            return True
        except FileNotFoundError:
            return False
        except Exception:
            return False


def ts_blob_storage_resolution(
    client: TemporalStoreBlobClient,
    data: bytes,
    *,
    source_uri: str,
    content_type: str = "text/markdown",
    scope: Optional[Json] = None,
    kind: str = "resource",   # "resource" | "skill"
    name: str = "",
) -> Json:
    """Upload a resource/skill raw blob to TemporalStore's blob tier and return the
    ``storage_resolution`` the ingest path expects — pointer + status the
    manifest/section records carry, in the SAME shape as the object-store path.

    In inline mode (no blob endpoint configured) the blob stays inline and upload
    is 'not_required'; the caller keeps serving from the store as before.
    """
    if not isinstance(data, (bytes, bytearray)):
        data = str(data).encode("utf-8")
    data = bytes(data)
    ch = content_hash(data)
    if not getattr(client, "configured", False):
        return {
            "storage_mode": "inline", "backend": "inline",
            "original_raw_uri": source_uri, "stored_raw_uri": source_uri, "parse_uri": source_uri,
            "raw_storage_policy": "inline", "raw_bytes_stored": False,
            "upload_status": "not_required", "cloud_bucket": "", "cloud_key": "",
            "content_hash": ch, "raw_bytes": len(data), "content_type": content_type,
        }
    key = content_key(data, prefix=client.key_prefix)
    deduped = client.exists(key)
    metadata = {
        "original_raw_uri": source_uri, "kind": kind, "name": name,
        "scope": scope or {}, "content_hash": ch, "content_type": content_type,
    }
    if not deduped:
        client.put(data, key=key, content_type=content_type)
    uri = blob_uri(key)
    return {
        "storage_mode": "temporalstore", "backend": "temporalstore_blob",
        "original_raw_uri": source_uri, "stored_raw_uri": uri, "parse_uri": uri,
        "raw_storage_policy": "temporalstore_blob", "raw_bytes_stored": True,
        "upload_status": "deduplicated" if deduped else "uploaded",
        "cloud_bucket": "", "cloud_key": key,
        "content_hash": ch, "raw_bytes": len(data), "content_type": content_type,
        "metadata": metadata,
    }


# ---------------------------------------------------------------------------------------------
# Engine command tier -- the embedded/proxy counterpart of the HTTP /blob endpoint above.
#
# Deployments that run NO datanode HTTP server (the embedded engine behind the rust proxy)
# store attachments through the engine's own blob commands instead. Both tiers publish
# temporalstore:// URIs; they are told apart by KEY SHAPE, never by configuration:
#   HTTP tier   : resources/<sha2>/<sha256>            (64-hex content digest)
#   engine tier : resources/<tenant:016x>/<hash:016x>  (two 16-hex segments)
# The helpers below ride any client exposing the resource_blob_* ops (both rust proxy client
# classes do); everything degrades to None/False when handed a client without them.
# ---------------------------------------------------------------------------------------------

_ENGINE_BLOB_URI_RE = None


def parse_engine_blob_uri(uri: str) -> Optional[tuple[int, int]]:
    """(tenant_hash, content_hash) for an ENGINE-tier URI, else None.

    Strict: exactly two 16-digit lowercase-hex segments under resources/. The HTTP tier's
    sha256 shape (64 hex digits) and every caller-supplied opaque key fall through to None,
    so the two tiers can never capture each other's fetches.
    """
    global _ENGINE_BLOB_URI_RE
    if _ENGINE_BLOB_URI_RE is None:
        import re
        _ENGINE_BLOB_URI_RE = re.compile(
            r"^temporalstore://resources/([0-9a-f]{16})/([0-9a-f]{16})$"
        )
    match = _ENGINE_BLOB_URI_RE.match(str(uri or ""))
    if not match:
        return None
    return int(match.group(1), 16), int(match.group(2), 16)


def engine_blob_supported(client: Any) -> bool:
    return client is not None and callable(getattr(client, "resource_blob_fetch", None))


def engine_blob_put(client: Any, tenant_hash: int, data: bytes) -> Json:
    """Store bytes in the engine tier; returns {stored_raw_uri, content_hash, raw_bytes, ...}
    shaped like ts_blob_storage_resolution so the resolver treats both tiers alike."""
    import base64
    response = client.resource_blob_put(int(tenant_hash), base64.b64encode(data).decode("ascii"))
    uri = str(response.get("matrixark_blob_uri") or "")
    if not uri:
        raise MatrixArkBlobError(f"engine blob put returned no uri: {response}")
    return {
        "storage_mode": "temporalstore", "backend": "temporalstore_engine_blob",
        "stored_raw_uri": uri, "raw_storage_policy": "temporalstore_engine_blob",
        "raw_bytes_stored": True, "upload_status": "uploaded",
        "cloud_bucket": "", "cloud_key": uri.split("://", 1)[-1],
        "content_hash": str(response.get("matrixark_blob_content_hash") or ""),
        "raw_bytes": int(response.get("matrixark_blob_size_bytes") or len(data)),
    }


def engine_blob_get(client: Any, uri: str, *, chunk_bytes: int = 4 * 1024 * 1024) -> bytes:
    """Fetch an engine-tier blob whole, in bounded range reads until eof."""
    import base64
    parts: list[bytes] = []
    offset = 0
    while True:
        response = client.resource_blob_fetch(uri, offset=offset, length=chunk_bytes)
        payload = base64.b64decode(str(response.get("value") or ""))
        parts.append(payload)
        offset += len(payload)
        if bool(response.get("matrixark_blob_eof")) or not payload:
            break
    return b"".join(parts)


def engine_blob_sweep(client: Any, tenant_hash: int, referenced_hex: list[str], min_age_ms: int) -> Json:
    response = client.resource_blob_sweep(int(tenant_hash), list(referenced_hex), int(min_age_ms))
    return {
        "scanned": int(response.get("matrixark_blob_scanned") or 0),
        "deleted": int(response.get("matrixark_blob_deleted") or 0),
    }


class MatrixArkBlobError(RuntimeError):
    pass
