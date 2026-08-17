#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Tests for TemporalStore-native blob ingest (RAFT mode, no object store).

Spins a tiny in-process stdlib HTTP server implementing PUT/POST/GET /blob/<key>
backed by an in-memory dict, points MATRIXARK_TS_BLOB_URL at it, and exercises the
blob client + the ingest resolver's temporalstore:// download/upload branches.
No live services: ephemeral port + temp dirs only.
"""
import json
import os
import tempfile
import threading
import unittest
from http.server import BaseHTTPRequestHandler, HTTPServer

import matrixark_temporalstore_blob as tsblob
import matrixark_mcp_core_resource_io as rio


# In-memory blob store shared with the request handler.
_STORE: dict[str, bytes] = {}


class _BlobHandler(BaseHTTPRequestHandler):
    def log_message(self, *args):  # silence
        pass

    def _key(self) -> str:
        # path is /blob/<key> (key may contain slashes)
        return self.path[len("/blob/"):]

    def _put(self):
        length = int(self.headers.get("Content-Length") or 0)
        body = self.rfile.read(length) if length else b""
        key = self._key()
        _STORE[key] = body
        receipt = json.dumps({"status": {"ok": True}, "key": key,
                              "bytes_written": len(body), "object_length": len(body),
                              "chunks": 1}).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(receipt)))
        self.end_headers()
        self.wfile.write(receipt)

    do_PUT = _put
    do_POST = _put

    def do_GET(self):
        key = self._key()
        if key not in _STORE:
            self.send_response(404)
            self.send_header("Content-Length", "0")
            self.end_headers()
            return
        data = _STORE[key]
        self.send_response(200)
        self.send_header("Content-Type", "application/octet-stream")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)


class _ServerFixture:
    def __init__(self):
        self.httpd = HTTPServer(("127.0.0.1", 0), _BlobHandler)
        self.port = self.httpd.server_address[1]
        self.thread = threading.Thread(target=self.httpd.serve_forever, daemon=True)

    def __enter__(self):
        self.thread.start()
        return self

    def __exit__(self, *exc):
        self.httpd.shutdown()
        self.httpd.server_close()

    @property
    def url(self) -> str:
        return f"http://127.0.0.1:{self.port}"


def _clear_backend_env():
    for k in ("MATRIXARK_TS_BLOB_URL", "MATRIXARK_TS_BLOB_GATEWAY_URL",
              "MATRIXARK_OBJECT_RPC_URL", "MATRIXARK_OBJECT_STORE_DIR",
              "MATRIXARK_RESOURCE_OBJECT_BACKEND", "MATRIXARK_RESOURCE_STORAGE_MODE"):
        os.environ.pop(k, None)


class BackendResolutionTest(unittest.TestCase):
    def setUp(self):
        _clear_backend_env()

    def tearDown(self):
        _clear_backend_env()

    def test_inline_when_unset(self):
        self.assertEqual("inline", tsblob.resolve_ts_blob_backend())

    def test_url_when_configured(self):
        os.environ["MATRIXARK_TS_BLOB_URL"] = "http://x:17102"
        self.assertEqual("http://x:17102", tsblob.resolve_ts_blob_backend())

    def test_content_key_is_content_addressed(self):
        self.assertEqual(tsblob.content_key(b"hello"), tsblob.content_key(b"hello"))
        self.assertNotEqual(tsblob.content_key(b"hello"), tsblob.content_key(b"world"))
        self.assertTrue(tsblob.content_key(b"hello").endswith(tsblob.content_hash(b"hello")))
        self.assertTrue(tsblob.content_key(b"hello").startswith("resources/"))

    def test_blob_uri_shape(self):
        self.assertEqual("temporalstore://resources/ab/cd", tsblob.blob_uri("resources/ab/cd"))


class ClientRoundTripTest(unittest.TestCase):
    def setUp(self):
        _clear_backend_env()
        _STORE.clear()

    def tearDown(self):
        _clear_backend_env()
        _STORE.clear()

    def test_put_get_roundtrip(self):
        with _ServerFixture() as srv:
            os.environ["MATRIXARK_TS_BLOB_URL"] = srv.url
            client = tsblob.TemporalStoreBlobClient()
            key = client.put(b"# skill body\nlarge attachment", content_type="text/markdown")
            self.assertTrue(key.startswith("resources/"))
            got, meta = client.get(key)
            self.assertEqual(b"# skill body\nlarge attachment", got)
            self.assertEqual(meta["content_hash"], tsblob.content_hash(got))

    def test_get_missing_raises(self):
        with _ServerFixture() as srv:
            os.environ["MATRIXARK_TS_BLOB_URL"] = srv.url
            client = tsblob.TemporalStoreBlobClient()
            with self.assertRaises(FileNotFoundError):
                client.get("resources/no/such")

    def test_storage_resolution_shape(self):
        with _ServerFixture() as srv:
            os.environ["MATRIXARK_TS_BLOB_URL"] = srv.url
            client = tsblob.TemporalStoreBlobClient()
            res = tsblob.ts_blob_storage_resolution(
                client, b"skill body", source_uri="skill://discovered/fix-auth",
                content_type="text/markdown", kind="skill", name="Fix auth",
            )
            self.assertEqual("temporalstore", res["storage_mode"])
            self.assertEqual("temporalstore_blob", res["raw_storage_policy"])
            self.assertEqual("uploaded", res["upload_status"])
            self.assertEqual("", res["cloud_bucket"])
            self.assertTrue(res["cloud_key"])
            self.assertTrue(res["stored_raw_uri"].startswith("temporalstore://"))
            self.assertTrue(res["raw_bytes_stored"])
            self.assertEqual(len(b"skill body"), res["raw_bytes"])

    def test_second_upload_is_deduplicated(self):
        with _ServerFixture() as srv:
            os.environ["MATRIXARK_TS_BLOB_URL"] = srv.url
            client = tsblob.TemporalStoreBlobClient()
            tsblob.ts_blob_storage_resolution(client, b"doc", source_uri="x")
            res2 = tsblob.ts_blob_storage_resolution(client, b"doc", source_uri="x")
            self.assertEqual("deduplicated", res2["upload_status"])

    def test_inline_when_client_unconfigured(self):
        client = tsblob.TemporalStoreBlobClient()  # no env -> pure default -> inline
        res = tsblob.ts_blob_storage_resolution(client, b"doc", source_uri="mem://a")
        self.assertEqual("inline", res["storage_mode"])
        self.assertEqual("not_required", res["upload_status"])
        self.assertFalse(res["raw_bytes_stored"])
        self.assertEqual("", res["cloud_key"])


class IngestResolverTest(unittest.TestCase):
    def setUp(self):
        _clear_backend_env()
        _STORE.clear()

    def tearDown(self):
        _clear_backend_env()
        _STORE.clear()

    def test_download_branch(self):
        with _ServerFixture() as srv:
            os.environ["MATRIXARK_TS_BLOB_URL"] = srv.url
            client = tsblob.TemporalStoreBlobClient()
            key = client.put(b"downloaded resource bytes", content_type="text/markdown")
            raw_uri = tsblob.blob_uri(key)
            res = rio.resolve_raw_resource_for_ingest(
                args={}, envelope={}, raw_uri=raw_uri, resource_type="md",
                deployment_scope="cloud", resource_text="",
            )
            try:
                self.assertEqual("temporalstore", res["storage_mode"])
                self.assertIsNone(res["parse_text"])
                self.assertEqual(raw_uri, res["stored_raw_uri"])
                self.assertEqual(key, res["cloud_key"])
                with open(res["parse_uri"], "rb") as fh:
                    self.assertEqual(b"downloaded resource bytes", fh.read())
            finally:
                rio.cleanup_temp_paths(res.get("temp_paths", []))

    def test_upload_branch(self):
        with _ServerFixture() as srv:
            os.environ["MATRIXARK_TS_BLOB_URL"] = srv.url
            # MatrixObject NOT configured; select temporalstore explicitly.
            tmp = tempfile.mkdtemp(prefix="matrixark-tsblob-src-")
            src = os.path.join(tmp, "resource.md")
            with open(src, "w", encoding="utf-8") as fh:
                fh.write("# large resource\nbody bytes here")
            res = rio.resolve_raw_resource_for_ingest(
                args={"raw_object_backend": "temporalstore"}, envelope={},
                raw_uri=src, resource_type="md", deployment_scope="cloud", resource_text="",
            )
            try:
                self.assertEqual("temporalstore", res["storage_mode"])
                self.assertTrue(res["stored_raw_uri"].startswith("temporalstore://"))
                self.assertEqual("temporalstore_blob", res["raw_storage_policy"])
                self.assertTrue(res["raw_bytes_stored"])
                # blob is fetchable back
                key = res["cloud_key"]
                got, _ = tsblob.TemporalStoreBlobClient().get(key)
                self.assertEqual(b"# large resource\nbody bytes here", got)
            finally:
                rio.cleanup_temp_paths(res.get("temp_paths", []))

    def test_auto_picks_temporalstore_when_only_blob_configured(self):
        with _ServerFixture() as srv:
            os.environ["MATRIXARK_TS_BLOB_URL"] = srv.url
            res = rio.resolve_raw_resource_for_ingest(
                args={}, envelope={}, raw_uri="inline-resource", resource_type="md",
                deployment_scope="cloud", resource_text="# inline body",
            )
            try:
                self.assertEqual("temporalstore", res["storage_mode"])
                self.assertTrue(res["stored_raw_uri"].startswith("temporalstore://"))
            finally:
                rio.cleanup_temp_paths(res.get("temp_paths", []))

    def test_local_input_regression_unchanged(self):
        # No cloud backends configured; local mode must behave exactly as before.
        tmp = tempfile.mkdtemp(prefix="matrixark-tsblob-local-")
        src = os.path.join(tmp, "local.md")
        with open(src, "w", encoding="utf-8") as fh:
            fh.write("# local file")
        res = rio.resolve_raw_resource_for_ingest(
            args={}, envelope={}, raw_uri=src, resource_type="md",
            deployment_scope="local", resource_text="# local file",
        )
        self.assertEqual("local", res["storage_mode"])
        self.assertEqual("not_required", res["upload_status"])
        self.assertFalse(res["raw_bytes_stored"])
        self.assertEqual("", res["cloud_key"])
        self.assertEqual(src, res["stored_raw_uri"])

    def test_inline_input_regression_unchanged(self):
        res = rio.resolve_raw_resource_for_ingest(
            args={}, envelope={}, raw_uri="", resource_type="md",
            deployment_scope="local", resource_text="# inline",
        )
        self.assertEqual("local", res["storage_mode"])
        self.assertFalse(res["raw_bytes_stored"])
        self.assertEqual("inline-resource", res["stored_raw_uri"])


if __name__ == "__main__":
    unittest.main(verbosity=2)
