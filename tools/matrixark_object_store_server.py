#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Standalone durable MatrixObject object-store HTTP service.

Protocol-compatible with ``MatrixObjectClient`` (tools/matrixark_object_store.py,
``matrixobject_rpc`` backend). Pure Python stdlib so it runs anywhere with no deps.

Wire contract (must match ``_rpc_put`` / ``_rpc_get`` in matrixark_object_store.py):

  PUT /v1/objects/{bucket}/{key}
      body    = raw object bytes (streamed; large bodies are not buffered in RAM)
      headers = Content-Type, X-Object-Meta: <json>
      -> 201 Created                      on a fresh store
      -> 208 Already Reported             on a content-addressed dedup hit
         (also always sets ``X-Object-Deduplicated: true|false`` — the client
          treats ``status == 208`` OR that header == "true" as a dedup)

  GET /v1/objects/{bucket}/{key}
      -> 200 body = raw bytes, header X-Object-Meta: <json>
      -> 404 when the object is absent

  GET /v1/healthz  -> 200 {"status":"ok"}   (health probe; not part of the client contract)

Durability: objects land under
    {MATRIXARK_OBJECT_STORE_DIR}/{bucket}/{key}          (the bytes)
    {MATRIXARK_OBJECT_STORE_DIR}/{bucket}/{key}.meta.json (the X-Object-Meta sidecar)
Keys are content-addressed (``ab/cd/<sha256>``) so an existing path == identical bytes
== a safe dedup. Writes go to a temp file + os.replace for crash-atomicity.

NOTE: This is the durable stdlib reference server. A Rust ``matrixobjectstore-rs``-backed
server (enterprise) is the production follow-up and speaks the same HTTP contract.

Env:
    MATRIXARK_OBJECT_STORE_SERVE_HOST   (default 127.0.0.1)
    MATRIXARK_OBJECT_STORE_SERVE_PORT   (default 17200)
    MATRIXARK_OBJECT_STORE_DIR          (default /opt/temporalstore/data/objectstore)
"""
from __future__ import annotations

import json
import os
import posixpath
import shutil
import sys
import tempfile
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

STREAM_CHUNK = 1024 * 1024          # 1 MiB streaming chunk
OBJECTS_PREFIX = "/v1/objects/"


def _data_dir() -> str:
    return os.environ.get("MATRIXARK_OBJECT_STORE_DIR", "/opt/temporalstore/data/objectstore")


def _safe_segment_join(base: str, bucket: str, key: str) -> str | None:
    """Join base/bucket/key, refusing any path that escapes base (``..`` traversal)."""
    if not bucket or not key:
        return None
    # Normalise the posix-style key (the client always builds ``ab/cd/<hash>``).
    rel = posixpath.normpath("/" + bucket + "/" + key).lstrip("/")
    parts = [p for p in rel.split("/") if p not in ("", ".")]
    if any(p == ".." for p in parts):
        return None
    full = os.path.join(base, *parts)
    base_abs = os.path.abspath(base)
    full_abs = os.path.abspath(full)
    if full_abs != base_abs and not full_abs.startswith(base_abs + os.sep):
        return None
    return full_abs


class ObjectStoreHandler(BaseHTTPRequestHandler):
    server_version = "MatrixObjectStore/1.0"
    protocol_version = "HTTP/1.1"

    # -- helpers --------------------------------------------------------------
    def _send_json(self, status: int, payload: dict, extra_headers: dict | None = None) -> None:
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        for k, v in (extra_headers or {}).items():
            self.send_header(k, v)
        self.end_headers()
        if self.command != "HEAD":
            self.wfile.write(body)

    def _parse_object_path(self) -> tuple[str, str] | None:
        if not self.path.startswith(OBJECTS_PREFIX):
            return None
        rest = self.path[len(OBJECTS_PREFIX):]
        rest = rest.split("?", 1)[0].split("#", 1)[0]
        bucket, sep, key = rest.partition("/")
        if not bucket or not sep or not key:
            return None
        return bucket, key

    def log_message(self, fmt, *args):  # keep the console quiet; journald owns the log
        pass

    # -- routes ---------------------------------------------------------------
    def do_GET(self):
        if self.path.split("?", 1)[0] in ("/v1/healthz", "/healthz"):
            self._send_json(200, {"status": "ok", "service": "matrixark-objectstore",
                                  "dir": _data_dir()})
            return
        parsed = self._parse_object_path()
        if parsed is None:
            self._send_json(404, {"error": "not found"})
            return
        bucket, key = parsed
        path = _safe_segment_join(_data_dir(), bucket, key)
        if path is None or not os.path.isfile(path):
            self._send_json(404, {"error": "object not found", "bucket": bucket, "key": key})
            return
        meta_raw = "{}"
        meta_path = path + ".meta.json"
        if os.path.isfile(meta_path):
            try:
                with open(meta_path, "r", encoding="utf-8") as fh:
                    meta_raw = fh.read().strip() or "{}"
            except OSError:
                meta_raw = "{}"
        try:
            meta = json.loads(meta_raw)
        except (json.JSONDecodeError, ValueError):
            meta = {}
        size = os.path.getsize(path)
        self.send_response(200)
        self.send_header("Content-Type", str(meta.get("content_type", "application/octet-stream")))
        self.send_header("Content-Length", str(size))
        self.send_header("X-Object-Meta", json.dumps(meta, ensure_ascii=False))
        self.end_headers()
        with open(path, "rb") as fh:
            shutil.copyfileobj(fh, self.wfile, STREAM_CHUNK)

    def do_HEAD(self):
        parsed = self._parse_object_path()
        if parsed is None:
            self.send_response(404)
            self.end_headers()
            return
        bucket, key = parsed
        path = _safe_segment_join(_data_dir(), bucket, key)
        if path is None or not os.path.isfile(path):
            self.send_response(404)
            self.end_headers()
            return
        self.send_response(200)
        self.send_header("Content-Length", str(os.path.getsize(path)))
        self.end_headers()

    def do_PUT(self):
        parsed = self._parse_object_path()
        if parsed is None:
            self._send_json(400, {"error": "bad object path"})
            self._drain_body()
            return
        bucket, key = parsed
        path = _safe_segment_join(_data_dir(), bucket, key)
        if path is None:
            self._send_json(400, {"error": "invalid bucket/key"})
            self._drain_body()
            return

        # Content-addressed dedup: an existing object == identical bytes already stored.
        if os.path.isfile(path):
            self._drain_body()
            self.send_response(208)  # Already Reported
            self.send_header("X-Object-Deduplicated", "true")
            self.send_header("Content-Length", "0")
            self.end_headers()
            return

        os.makedirs(os.path.dirname(path), exist_ok=True)
        meta_header = self.headers.get("X-Object-Meta")
        content_type = self.headers.get("Content-Type", "application/octet-stream")

        # Stream the body to a temp file, then atomically replace into place.
        tmp_fd, tmp_path = tempfile.mkstemp(dir=os.path.dirname(path), prefix=".upload-")
        written = 0
        try:
            with os.fdopen(tmp_fd, "wb") as out:
                for chunk in self._iter_body():
                    out.write(chunk)
                    written += len(chunk)
                out.flush()
                os.fsync(out.fileno())
            # Build the meta sidecar (prefer the client-supplied X-Object-Meta json).
            meta: dict = {}
            if meta_header:
                try:
                    meta = json.loads(meta_header)
                except (json.JSONDecodeError, ValueError):
                    meta = {}
            meta.setdefault("content_type", content_type)
            meta.setdefault("size", written)
            meta_path = path + ".meta.json"
            with open(meta_path + ".tmp", "w", encoding="utf-8") as mfh:
                json.dump(meta, mfh, ensure_ascii=False)
            os.replace(meta_path + ".tmp", meta_path)
            os.replace(tmp_path, path)
        except Exception as exc:  # noqa: BLE001 - report to client, clean the temp
            try:
                os.unlink(tmp_path)
            except OSError:
                pass
            self._send_json(500, {"error": f"store failed: {exc}"})
            return
        self.send_response(201)  # Created
        self.send_header("X-Object-Deduplicated", "false")
        self.send_header("X-Object-Size", str(written))
        self.send_header("Content-Length", "0")
        self.end_headers()

    # -- body streaming -------------------------------------------------------
    def _iter_body(self):
        """Yield the request body in chunks, honouring Content-Length / chunked TE."""
        te = (self.headers.get("Transfer-Encoding") or "").lower()
        if "chunked" in te:
            while True:
                size_line = self.rfile.readline().strip()
                if not size_line:
                    break
                try:
                    size = int(size_line.split(b";", 1)[0], 16)
                except ValueError:
                    break
                if size == 0:
                    self.rfile.readline()  # trailing CRLF
                    break
                remaining = size
                while remaining > 0:
                    chunk = self.rfile.read(min(STREAM_CHUNK, remaining))
                    if not chunk:
                        return
                    remaining -= len(chunk)
                    yield chunk
                self.rfile.readline()  # CRLF after each chunk
            return
        length = int(self.headers.get("Content-Length", 0) or 0)
        remaining = length
        while remaining > 0:
            chunk = self.rfile.read(min(STREAM_CHUNK, remaining))
            if not chunk:
                break
            remaining -= len(chunk)
            yield chunk

    def _drain_body(self):
        for _ in self._iter_body():
            pass


def main() -> int:
    host = os.environ.get("MATRIXARK_OBJECT_STORE_SERVE_HOST", "127.0.0.1")
    port = int(os.environ.get("MATRIXARK_OBJECT_STORE_SERVE_PORT", "17200"))
    data_dir = _data_dir()
    os.makedirs(data_dir, exist_ok=True)
    httpd = ThreadingHTTPServer((host, port), ObjectStoreHandler)
    httpd.daemon_threads = True
    sys.stderr.write(f"matrixark-objectstore listening on {host}:{port} dir={data_dir}\n")
    sys.stderr.flush()
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        httpd.server_close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
