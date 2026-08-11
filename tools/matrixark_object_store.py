#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""MatrixObject storage for resource/skill raw content (distributed mode).

Split of concerns for a resource or skill in distributed/cloud mode:

  raw bytes  -> MatrixObject (object store), content-addressed, deduped.
  metadata   -> TemporalStore records (manifest/registry/section) carrying the
                pointer (cloud_bucket, cloud_key, stored_raw_uri) + serving fields
                (name, description, triggers, section text/preview) that retrieval
                scans. The blob never sits in the hot serving path.

Backends, resolved like the engine's storage_backend.rs (object > shared-fs > inline):
  * matrixobject_rpc  — the Rust proxy object RPC (HTTP), for production.
  * local_fs          — a content-addressed directory; the default when no RPC
                        endpoint is set, so this is testable and works today.
  * inline            — nothing configured; caller keeps the content inline.

Everything here is the MatrixArk (Python) front; TemporalStore/MatrixObject stay in Rust.
"""
from __future__ import annotations

import hashlib
import json
import os
import urllib.error
import urllib.request
from dataclasses import dataclass, field
from typing import Any, Optional

Json = dict[str, Any]

DEFAULT_BUCKET = os.environ.get("MATRIXARK_OBJECT_BUCKET", "matrixark-resources")
OBJECT_RPC_URL = os.environ.get("MATRIXARK_OBJECT_RPC_URL", "").rstrip("/")   # rust proxy object RPC base
LOCAL_OBJECT_DIR = os.environ.get("MATRIXARK_OBJECT_STORE_DIR", "")           # dev/local backend dir
OBJECT_URI_SCHEME = "matrixobject"


def content_hash(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def content_key(data: bytes, *, prefix: str = "") -> str:
    """Content-addressed, sharded key. Same bytes -> same key -> automatic dedup."""
    h = content_hash(data)
    return f"{prefix}{h[:2]}/{h[2:4]}/{h}"


def object_uri(bucket: str, key: str) -> str:
    return f"{OBJECT_URI_SCHEME}://{bucket}/{key}"


def resolve_object_backend(*, rpc_url: str = OBJECT_RPC_URL, local_dir: str = LOCAL_OBJECT_DIR) -> str:
    """Pick the object backend the way storage_backend.rs picks storage: RPC > local-fs > inline."""
    if rpc_url:
        return "matrixobject_rpc"
    if local_dir:
        return "local_fs"
    return "inline"


@dataclass
class ObjectPutResult:
    bucket: str
    key: str
    uri: str
    size: int
    content_hash: str
    backend: str
    deduplicated: bool = False
    metadata: Json = field(default_factory=dict)


class MatrixObjectClient:
    """Put/get raw blobs to MatrixObject (or a local-fs stand-in), content-addressed."""

    def __init__(self, *, backend: Optional[str] = None, bucket: str = DEFAULT_BUCKET,
                 rpc_url: str = OBJECT_RPC_URL, local_dir: str = LOCAL_OBJECT_DIR, timeout: float = 30.0):
        self.bucket = bucket
        self.rpc_url = (rpc_url or "").rstrip("/")
        self.local_dir = local_dir
        self.timeout = timeout
        self.backend = backend or resolve_object_backend(rpc_url=self.rpc_url, local_dir=self.local_dir)

    # -- public ---------------------------------------------------------------
    def put(self, data: bytes, *, content_type: str = "application/octet-stream",
            metadata: Optional[Json] = None, bucket: Optional[str] = None) -> ObjectPutResult:
        if not isinstance(data, (bytes, bytearray)):
            data = str(data).encode("utf-8")
        bucket = bucket or self.bucket
        key = content_key(data)
        ch = content_hash(data)
        meta = {"content_hash": ch, "content_type": content_type, "size": len(data), **(metadata or {})}
        if self.backend == "matrixobject_rpc":
            deduped = self._rpc_put(bucket, key, data, meta)
        elif self.backend == "local_fs":
            deduped = self._fs_put(bucket, key, data, meta)
        else:  # inline: nothing to store
            return ObjectPutResult(bucket=bucket, key="", uri="", size=len(data),
                                   content_hash=ch, backend="inline", deduplicated=False, metadata=meta)
        return ObjectPutResult(bucket=bucket, key=key, uri=object_uri(bucket, key), size=len(data),
                               content_hash=ch, backend=self.backend, deduplicated=deduped, metadata=meta)

    def get(self, key: str, *, bucket: Optional[str] = None) -> tuple[bytes, Json]:
        bucket = bucket or self.bucket
        if self.backend == "matrixobject_rpc":
            return self._rpc_get(bucket, key)
        if self.backend == "local_fs":
            return self._fs_get(bucket, key)
        raise RuntimeError("no object backend configured (inline mode has nothing to get)")

    def exists(self, key: str, *, bucket: Optional[str] = None) -> bool:
        bucket = bucket or self.bucket
        if self.backend == "local_fs":
            return os.path.exists(self._fs_path(bucket, key))
        if self.backend == "matrixobject_rpc":
            try:
                self._rpc_get(bucket, key)
                return True
            except Exception:
                return False
        return False

    # -- local-fs backend (tested default) ------------------------------------
    def _fs_path(self, bucket: str, key: str) -> str:
        return os.path.join(self.local_dir, bucket, key)

    def _fs_put(self, bucket: str, key: str, data: bytes, meta: Json) -> bool:
        path = self._fs_path(bucket, key)
        if os.path.exists(path):  # content-addressed -> identical bytes already stored
            return True
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "wb") as fh:
            fh.write(data)
        with open(path + ".meta.json", "w", encoding="utf-8") as fh:
            json.dump(meta, fh, ensure_ascii=False)
        return False

    def _fs_get(self, bucket: str, key: str) -> tuple[bytes, Json]:
        path = self._fs_path(bucket, key)
        with open(path, "rb") as fh:
            data = fh.read()
        meta: Json = {}
        if os.path.exists(path + ".meta.json"):
            with open(path + ".meta.json", encoding="utf-8") as fh:
                meta = json.load(fh)
        return data, meta

    # -- matrixobject rust-proxy RPC backend (production) ---------------------
    # Contract: PUT {rpc}/v1/objects/{bucket}/{key} body=bytes, X-Object-Meta: <json>;
    #           GET {rpc}/v1/objects/{bucket}/{key} -> bytes + X-Object-Meta header.
    def _rpc_put(self, bucket: str, key: str, data: bytes, meta: Json) -> bool:
        url = f"{self.rpc_url}/v1/objects/{bucket}/{key}"
        req = urllib.request.Request(url, data=data, method="PUT", headers={
            "Content-Type": meta.get("content_type", "application/octet-stream"),
            "X-Object-Meta": json.dumps(meta, ensure_ascii=False),
        })
        with urllib.request.urlopen(req, timeout=self.timeout) as resp:
            return resp.status == 208 or resp.getheader("X-Object-Deduplicated") == "true"

    def _rpc_get(self, bucket: str, key: str) -> tuple[bytes, Json]:
        url = f"{self.rpc_url}/v1/objects/{bucket}/{key}"
        with urllib.request.urlopen(urllib.request.Request(url, method="GET"), timeout=self.timeout) as resp:
            data = resp.read()
            raw_meta = resp.getheader("X-Object-Meta") or "{}"
        try:
            meta = json.loads(raw_meta)
        except (json.JSONDecodeError, ValueError):
            meta = {}
        return data, meta


def resource_blob_storage_resolution(
    client: MatrixObjectClient,
    data: bytes,
    *,
    source_uri: str,
    content_type: str = "text/markdown",
    scope: Optional[Json] = None,
    kind: str = "resource",   # "resource" | "skill"
    name: str = "",
    bucket: Optional[str] = None,
) -> Json:
    """Upload a resource/skill raw blob and return the `storage_resolution` the ingest
    path expects — the pointer + status that the manifest/section records carry.

    In inline mode (no object backend) the blob stays inline and upload is 'not_required';
    the caller keeps serving from the store as before.
    """
    result = client.put(
        data, content_type=content_type, bucket=bucket,
        metadata={
            "original_raw_uri": source_uri, "kind": kind, "name": name,
            "scope": scope or {},
        },
    )
    if result.backend == "inline":
        return {
            "storage_mode": "inline", "backend": "inline",
            "original_raw_uri": source_uri, "stored_raw_uri": source_uri, "parse_uri": source_uri,
            "raw_storage_policy": "inline", "raw_bytes_stored": False,
            "upload_status": "not_required", "cloud_bucket": "", "cloud_key": "",
            "content_hash": result.content_hash, "raw_bytes": result.size, "content_type": content_type,
        }
    return {
        "storage_mode": "matrixobject", "backend": result.backend,
        "original_raw_uri": source_uri, "stored_raw_uri": result.uri, "parse_uri": result.uri,
        "raw_storage_policy": "object_store", "raw_bytes_stored": True,
        "upload_status": "deduplicated" if result.deduplicated else "uploaded",
        "cloud_bucket": result.bucket, "cloud_key": result.key,
        "content_hash": result.content_hash, "raw_bytes": result.size, "content_type": content_type,
    }
