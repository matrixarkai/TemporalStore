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

_ENV_KEYS = ("MATRIXARK_BASE_URL", "MATRIXARK_GATEWAY_URL", "MATRIXARK_API_KEY")


class _EnvGuard:
    """Set/clear env vars and restore them on exit (None -> unset)."""

    def __init__(self, **values):
        self._values = values
        self._saved: dict[str, str | None] = {}

    def __enter__(self):
        for key, value in self._values.items():
            self._saved[key] = os.environ.get(key)
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value
        return self

    def __exit__(self, *exc):
        for key, prev in self._saved.items():
            if prev is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = prev


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

    def test_sharing_scope_in_ingest_body(self):
        path = self._write(b"# team skill\n")
        with _Gateway() as gw:
            sdk.ingest_large_file(path, base_url=gw.url, api_key="k",
                                  kind="skill", sharing_scope="tenant_shared")
        self.assertEqual("tenant_shared", _INGESTS[0]["sharing_scope"])

    def test_visibility_alias_maps_to_sharing_scope(self):
        path = self._write(b"# skill via alias\n")
        with _Gateway() as gw:
            sdk.ingest_large_file(path, base_url=gw.url, api_key="k",
                                  visibility="global_shared")
        self.assertEqual("global_shared", _INGESTS[0]["sharing_scope"])

    def test_sharing_scope_omitted_key_absent(self):
        path = self._write(b"# no scope\n")
        with _Gateway() as gw:
            sdk.ingest_large_file(path, base_url=gw.url, api_key="k")
        self.assertNotIn("sharing_scope", _INGESTS[0])   # backward compatible

    def test_unknown_sharing_scope_raises(self):
        path = self._write(b"# bad scope\n")
        with self.assertRaises(ValueError):
            sdk.ingest_large_file(path, base_url="http://127.0.0.1:1", api_key="k",
                                  sharing_scope="everyone")


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
        self.assertNotIn("sharing_scope", _INGESTS[0])   # omitted -> absent

    def test_sharing_scope_global_shared_in_body(self):
        with _Gateway() as gw:
            sdk.ingest_text("shared body", base_url=gw.url, api_key="k",
                            kind="resource", sharing_scope="global_shared")
        self.assertEqual("global_shared", _INGESTS[0]["sharing_scope"])

    def test_visibility_alias(self):
        with _Gateway() as gw:
            sdk.ingest_text("private body", base_url=gw.url, api_key="k",
                            visibility="private_user")
        self.assertEqual("private_user", _INGESTS[0]["sharing_scope"])

    def test_unknown_sharing_scope_raises(self):
        with self.assertRaises(ValueError):
            sdk.ingest_text("x", base_url="http://127.0.0.1:1", api_key="k",
                            sharing_scope="public")


class EnvResolutionTest(unittest.TestCase):
    """Deliverable 1: base_url / api_key are optional — resolved from env with a
    localhost default, and anonymous (no Authorization header) when no key is set."""

    def setUp(self):
        _BLOBS.clear(); _INGESTS.clear(); _AUTH_SEEN.clear()

    def _write(self, body: bytes, suffix=".md") -> str:
        fd, path = tempfile.mkstemp(prefix="sdk-env-", suffix=suffix)
        with os.fdopen(fd, "wb") as fh:
            fh.write(body)
        self.addCleanup(lambda: os.path.exists(path) and os.unlink(path))
        return path

    def test_base_url_default_localhost_when_unset(self):
        with _EnvGuard(**{k: None for k in _ENV_KEYS}):
            self.assertEqual("http://127.0.0.1:8080", sdk._resolve_base_url(None))
            self.assertEqual(sdk.DEFAULT_BASE_URL, sdk._resolve_base_url(None))

    def test_base_url_from_env_var(self):
        with _EnvGuard(MATRIXARK_BASE_URL="http://example.test:9999"):
            self.assertEqual("http://example.test:9999", sdk._resolve_base_url(None))
        # Alias MATRIXARK_GATEWAY_URL is also accepted.
        with _EnvGuard(MATRIXARK_BASE_URL=None,
                       MATRIXARK_GATEWAY_URL="http://alias.test:1234"):
            self.assertEqual("http://alias.test:1234", sdk._resolve_base_url(None))

    def test_no_base_url_resolves_from_env_end_to_end(self):
        path = self._write(b"# env-resolved skill\n")
        with _Gateway() as gw:
            with _EnvGuard(MATRIXARK_BASE_URL=gw.url, MATRIXARK_API_KEY=None,
                           MATRIXARK_GATEWAY_URL=None):
                result = sdk.ingest_large_file(path)   # no base_url / api_key
        self.assertEqual(1, len(_INGESTS))
        self.assertTrue(result["raw_uri"].startswith("temporalstore://"))

    def test_no_auth_header_when_api_key_unset(self):
        path = self._write(b"# anon skill\n")
        with _Gateway() as gw:
            with _EnvGuard(MATRIXARK_BASE_URL=gw.url, MATRIXARK_API_KEY=None,
                           MATRIXARK_GATEWAY_URL=None):
                sdk.ingest_large_file(path)
        # Both hops (PUT blob, POST ingest) sent NO Authorization header (anonymous).
        self.assertTrue(_AUTH_SEEN)
        self.assertTrue(all(a == "" for a in _AUTH_SEEN))

    def test_auth_header_from_env_api_key(self):
        path = self._write(b"# keyed via env\n")
        with _Gateway() as gw:
            with _EnvGuard(MATRIXARK_BASE_URL=gw.url, MATRIXARK_API_KEY="k-env",
                           MATRIXARK_GATEWAY_URL=None):
                sdk.ingest_large_file(path)
        self.assertTrue(all(a == "Bearer k-env" for a in _AUTH_SEEN))

    def test_auth_header_when_api_key_passed(self):
        path = self._write(b"# keyed via arg\n")
        with _Gateway() as gw:
            with _EnvGuard(**{k: None for k in _ENV_KEYS}):
                sdk.ingest_large_file(path, base_url=gw.url, api_key="k-arg")
        self.assertTrue(all(a == "Bearer k-arg" for a in _AUTH_SEEN))

    def test_explicit_args_win_over_env(self):
        path = self._write(b"# explicit wins\n")
        with _Gateway() as gw:
            # Env points elsewhere / to a different key; explicit args must win.
            with _EnvGuard(MATRIXARK_BASE_URL="http://wrong.test:1",
                           MATRIXARK_API_KEY="k-env", MATRIXARK_GATEWAY_URL=None):
                sdk.ingest_large_file(path, base_url=gw.url, api_key="k-explicit")
        self.assertEqual(1, len(_INGESTS))                       # reached the real mock server
        self.assertTrue(all(a == "Bearer k-explicit" for a in _AUTH_SEEN))

    def test_ingest_text_anonymous_and_env_resolved(self):
        with _Gateway() as gw:
            with _EnvGuard(MATRIXARK_BASE_URL=gw.url, MATRIXARK_API_KEY=None,
                           MATRIXARK_GATEWAY_URL=None):
                sdk.ingest_text("short body")                    # no base_url / api_key
        self.assertEqual("short body", _INGESTS[0]["text"])
        self.assertTrue(all(a == "" for a in _AUTH_SEEN))        # anonymous


if __name__ == "__main__":
    unittest.main(verbosity=2)
