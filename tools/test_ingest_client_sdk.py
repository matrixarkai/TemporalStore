#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Caller-side SDK helper (matrixark_ingest_client): upload-then-ingest a local file,
and inline-text ingest. Spins a tiny in-process stdlib gateway that implements
PUT /v1/blob/<key> (into a dict) + POST /v1/ingest (echoing the raw_uri/body).
No live services: ephemeral port + temp files only."""
import json
import os
import tempfile
import threading
import unittest
from http.server import BaseHTTPRequestHandler, HTTPServer

import matrixark_ingest_client as sdk
import matrixark_temporalstore_blob as tsblob


_BLOBS: dict[str, bytes] = {}
_INGESTS: list[dict] = []
_AUTH_SEEN: list[str] = []


class _GatewayHandler(BaseHTTPRequestHandler):
    def log_message(self, *args):  # silence
        pass

    def _read(self) -> bytes:
        length = int(self.headers.get("Content-Length") or 0)
        return self.rfile.read(length) if length else b""

    def do_PUT(self):
        if not self.path.startswith("/v1/blob/"):
            self.send_response(404); self.send_header("Content-Length", "0"); self.end_headers(); return
        _AUTH_SEEN.append(self.headers.get("Authorization") or "")
        key = self.path[len("/v1/blob/"):]
        _BLOBS[key] = self._read()
        body = json.dumps({"key": key, "bytes": len(_BLOBS[key]), "stored": True}).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_POST(self):
        if self.path != "/v1/ingest":
            self.send_response(404); self.send_header("Content-Length", "0"); self.end_headers(); return
        _AUTH_SEEN.append(self.headers.get("Authorization") or "")
        parsed = json.loads(self._read() or b"{}")
        _INGESTS.append(parsed)
        out = {"accepted": 0, "raw_uri": parsed.get("raw_uri"),
               "text": parsed.get("text"), "kind": parsed.get("kind"),
               "resource_type": parsed.get("resource_type"),
               "finalized": bool(parsed.get("finalize")), "echo": parsed}
        body = json.dumps(out).encode()
        self.send_response(202)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


class _Gateway:
    def __init__(self):
        self.httpd = HTTPServer(("127.0.0.1", 0), _GatewayHandler)
        self.port = self.httpd.server_address[1]
        self.thread = threading.Thread(target=self.httpd.serve_forever, daemon=True)

    def __enter__(self):
        self.thread.start()
        return self

    def __exit__(self, *exc):
        self.httpd.shutdown(); self.httpd.server_close()

    @property
    def url(self) -> str:
        return f"http://127.0.0.1:{self.port}"


class IngestLargeFileTest(unittest.TestCase):
    def setUp(self):
        _BLOBS.clear(); _INGESTS.clear(); _AUTH_SEEN.clear()

    def _write(self, body: bytes, suffix=".md") -> str:
        fd, path = tempfile.mkstemp(prefix="sdk-src-", suffix=suffix)
        with os.fdopen(fd, "wb") as fh:
            fh.write(body)
        self.addCleanup(lambda: os.path.exists(path) and os.unlink(path))
        return path

    def test_upload_then_ingest_returns_content_addressed_raw_uri(self):
        body = b"# big skill\n" + b"x" * 4096
        path = self._write(body)
        with _Gateway() as gw:
            result = sdk.ingest_large_file(path, base_url=gw.url, api_key="k-acme",
                                           kind="skill")
        expected_key = tsblob.content_key(body)  # resources/<sha2>/<sha256>
        self.assertEqual(f"temporalstore://{expected_key}", result["raw_uri"])
        # The bytes were actually streamed to the blob tier under the content key.
        self.assertIn(expected_key, _BLOBS)
        self.assertEqual(body, _BLOBS[expected_key])
        # Ingest carried the pointer + kind + inferred resource_type.
        self.assertEqual(1, len(_INGESTS))
        self.assertEqual("skill", _INGESTS[0]["kind"])
        self.assertEqual("md", _INGESTS[0]["resource_type"])
        self.assertEqual(f"temporalstore://{expected_key}", _INGESTS[0]["raw_uri"])
        # Bearer token forwarded on both hops.
        self.assertTrue(all(a == "Bearer k-acme" for a in _AUTH_SEEN))

    def test_resource_type_inferred_from_suffix(self):
        path = self._write(b"%PDF-1.4 fake", suffix=".pdf")
        with _Gateway() as gw:
            sdk.ingest_large_file(path, base_url=gw.url, api_key="k")
        self.assertEqual("pdf", _INGESTS[0]["resource_type"])

    def test_explicit_resource_type_and_scope_and_wait(self):
        path = self._write(b"hello", suffix=".bin")
        with _Gateway() as gw:
            result = sdk.ingest_large_file(path, base_url=gw.url, api_key="k",
                                           resource_type="md", scope={"user_id": "u1"},
                                           wait=True)
        self.assertEqual("md", _INGESTS[0]["resource_type"])
        self.assertEqual({"user_id": "u1"}, _INGESTS[0]["scope"])
        self.assertTrue(_INGESTS[0]["finalize"])   # wait -> synchronous extraction
        self.assertTrue(result["finalized"])

    def test_retry_is_idempotent_no_duplicate(self):
        body = b"identical bytes retried"
        path = self._write(body)
        with _Gateway() as gw:
            sdk.ingest_large_file(path, base_url=gw.url, api_key="k")
            sdk.ingest_large_file(path, base_url=gw.url, api_key="k")   # retry same file
        # Same content key both times -> one blob entry (dedup), never duplicated.
        self.assertEqual(1, len(_BLOBS))
        self.assertIn(tsblob.content_key(body), _BLOBS)


class IngestTextTest(unittest.TestCase):
    def setUp(self):
        _BLOBS.clear(); _INGESTS.clear(); _AUTH_SEEN.clear()

    def test_inline_text_no_upload(self):
        with _Gateway() as gw:
            result = sdk.ingest_text("short knowledge body", base_url=gw.url, api_key="k",
                                     kind="resource", resource_type="md")
        self.assertEqual({}, _BLOBS)                       # no blob upload for inline text
        self.assertEqual("short knowledge body", _INGESTS[0]["text"])
        self.assertEqual("resource", _INGESTS[0]["kind"])
        self.assertEqual("short knowledge body", result["text"])


if __name__ == "__main__":
    unittest.main(verbosity=2)
