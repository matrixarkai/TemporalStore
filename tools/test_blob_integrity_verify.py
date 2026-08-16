#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Hash-verify-on-fetch: TemporalStoreBlobClient.get() integrity-checks a
content-addressed key (last segment is a 64-hex sha256) against the downloaded
bytes, and raises loudly on mismatch (corrupt/partial upload) so customers retry.
Non-content-addressed keys are not checked. Also verified through the ingest
resolver's temporalstore:// branch. Ephemeral port + in-memory store; no live svc."""
import json
import os
import threading
import unittest
from http.server import BaseHTTPRequestHandler, HTTPServer

import matrixark_temporalstore_blob as tsblob
import matrixark_mcp_core_resource_io as rio


_STORE: dict[str, bytes] = {}


class _BlobHandler(BaseHTTPRequestHandler):
    def log_message(self, *args):
        pass

    def _key(self):
        return self.path[len("/blob/"):]

    def do_GET(self):
        key = self._key()
        if key not in _STORE:
            self.send_response(404); self.send_header("Content-Length", "0"); self.end_headers(); return
        data = _STORE[key]
        self.send_response(200)
        self.send_header("Content-Type", "application/octet-stream")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)


class _Srv:
    def __init__(self):
        self.httpd = HTTPServer(("127.0.0.1", 0), _BlobHandler)
        self.port = self.httpd.server_address[1]
        self.thread = threading.Thread(target=self.httpd.serve_forever, daemon=True)

    def __enter__(self):
        self.thread.start(); return self

    def __exit__(self, *exc):
        self.httpd.shutdown(); self.httpd.server_close()

    @property
    def url(self):
        return f"http://127.0.0.1:{self.port}"


def _clear_env():
    for k in ("MATRIXARK_TS_BLOB_URL", "MATRIXARK_TS_BLOB_GATEWAY_URL"):
        os.environ.pop(k, None)


class KeyShapeTest(unittest.TestCase):
    def test_detects_content_addressed_keys(self):
        good = tsblob.content_key(b"payload")           # resources/<sha2>/<sha256>
        self.assertIsNotNone(tsblob._content_addressed_sha(good))
        self.assertEqual(tsblob.content_hash(b"payload"), tsblob._content_addressed_sha(good))

    def test_ignores_non_content_addressed_keys(self):
        self.assertIsNone(tsblob._content_addressed_sha("resources/opaque/name.md"))
        self.assertIsNone(tsblob._content_addressed_sha("some/user/key"))
        self.assertIsNone(tsblob._content_addressed_sha("deadbeef"))  # too short


class GetIntegrityTest(unittest.TestCase):
    def setUp(self):
        _clear_env(); _STORE.clear()

    def tearDown(self):
        _clear_env(); _STORE.clear()

    def test_correct_blob_passes(self):
        with _Srv() as srv:
            os.environ["MATRIXARK_TS_BLOB_URL"] = srv.url
            good_bytes = b"the real content"
            key = tsblob.content_key(good_bytes)
            _STORE[key] = good_bytes                      # stored bytes MATCH the key
            data, meta = tsblob.TemporalStoreBlobClient().get(key)
            self.assertEqual(good_bytes, data)
            self.assertEqual(tsblob.content_hash(good_bytes), meta["content_hash"])

    def test_corrupt_blob_raises_integrity_error(self):
        with _Srv() as srv:
            os.environ["MATRIXARK_TS_BLOB_URL"] = srv.url
            key = tsblob.content_key(b"the real content")
            _STORE[key] = b"CORRUPTED / partial bytes"    # stored bytes do NOT match the key
            with self.assertRaises(RuntimeError) as ctx:
                tsblob.TemporalStoreBlobClient().get(key)
            msg = str(ctx.exception)
            self.assertIn("integrity check", msg)
            self.assertIn("sha256 mismatch", msg)
            self.assertIn("re-upload and retry", msg)

    def test_non_content_addressed_key_skips_check(self):
        with _Srv() as srv:
            os.environ["MATRIXARK_TS_BLOB_URL"] = srv.url
            _STORE["resources/opaque/name.md"] = b"whatever bytes, not verified"
            data, _ = tsblob.TemporalStoreBlobClient().get("resources/opaque/name.md")
            self.assertEqual(b"whatever bytes, not verified", data)

    def test_ingest_resolver_surfaces_integrity_error(self):
        with _Srv() as srv:
            os.environ["MATRIXARK_TS_BLOB_URL"] = srv.url
            key = tsblob.content_key(b"good resource body")
            _STORE[key] = b"tampered"                     # mismatch
            raw_uri = tsblob.blob_uri(key)
            with self.assertRaises(RuntimeError) as ctx:
                rio.resolve_raw_resource_for_ingest(
                    args={}, envelope={}, raw_uri=raw_uri, resource_type="md",
                    deployment_scope="cloud", resource_text="")
            self.assertIn("integrity check", str(ctx.exception))


if __name__ == "__main__":
    unittest.main(verbosity=2)
