#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""The engine blob tier behind the python surface.

Two tiers hold attachment bytes under the same temporalstore:// scheme: the datanode's HTTP
/blob endpoint (sha256 keys) and the embedded engine's command tier (two 16-hex segments).
These tests pin the property that makes them safe to coexist: KEY SHAPE alone decides which
tier serves a fetch, and an engine-shaped URI without a rust proxy client fails loudly rather
than turning into the HTTP tier's guaranteed 404.
"""
from __future__ import annotations

import base64
import unittest
from pathlib import Path

import matrixark_temporalstore_blob as blob
from matrixark_mcp_core_resource_io import resolve_raw_resource_for_ingest
from matrixark_mcp_core_identity import MatrixArkError

ENGINE_URI = "temporalstore://resources/000000000000002a/00000000deadbeef"
HTTP_URI = "temporalstore://resources/ab/" + "c" * 64


class FakeEngineClient:
    """Speaks the resource_blob_* ops, serving one payload in bounded chunks."""

    def __init__(self, payload: bytes) -> None:
        self.payload = payload
        self.fetch_calls: list[tuple[str, int, int]] = []
        self.put_calls: list[tuple[int, bytes]] = []

    def resource_blob_put(self, tenant_hash: int, payload_base64: str):
        data = base64.b64decode(payload_base64)
        self.put_calls.append((tenant_hash, data))
        return {
            "ok": True,
            "matrixark_blob_uri": ENGINE_URI,
            "matrixark_blob_size_bytes": len(data),
            "matrixark_blob_content_hash": "00000000deadbeef",
        }

    def resource_blob_fetch(self, uri: str, *, offset: int = 0, length: int = 0):
        self.fetch_calls.append((uri, offset, length))
        window = self.payload[offset : offset + (length or len(self.payload))]
        eof = offset + len(window) >= len(self.payload)
        return {
            "ok": True,
            "value": base64.b64encode(window).decode("ascii"),
            "matrixark_blob_total_size": len(self.payload),
            "matrixark_blob_eof": eof,
        }


class EngineBlobUriShape(unittest.TestCase):
    def test_engine_shape_parses_and_the_http_shape_does_not(self):
        self.assertEqual((0x2A, 0xDEADBEEF), blob.parse_engine_blob_uri(ENGINE_URI))
        self.assertIsNone(blob.parse_engine_blob_uri(HTTP_URI))

    def test_non_canonical_spellings_are_rejected(self):
        for bad in [
            "temporalstore://resources/000000000000002A/00000000DEADBEEF",  # uppercase
            "temporalstore://resources/002a/00000000deadbeef",  # short tenant
            "temporalstore://resources/000000000000002a/00000000deadbeef/x",  # trailing path
            "objectstore://resources/000000000000002a/00000000deadbeef",  # wrong scheme
        ]:
            self.assertIsNone(blob.parse_engine_blob_uri(bad), bad)


class EngineBlobChunkLoop(unittest.TestCase):
    def test_get_reassembles_the_payload_from_bounded_ranges(self):
        payload = bytes(range(256)) * 40
        client = FakeEngineClient(payload)
        fetched = blob.engine_blob_get(client, ENGINE_URI, chunk_bytes=4096)
        self.assertEqual(payload, fetched)
        self.assertGreater(len(client.fetch_calls), 1, "must have taken multiple range reads")
        offsets = [offset for _, offset, _ in client.fetch_calls]
        self.assertEqual(sorted(offsets), offsets, "ranges must advance monotonically")


class ResolverTierDiscrimination(unittest.TestCase):
    def _resolve(self, uri: str, args: dict):
        return resolve_raw_resource_for_ingest(
            args,
            {"kind": "resource", "scope": {"account_id": "a", "tenant_id": "t"},
             "metadata": {}, "messages": []},
            uri,
            "md",
            "local",
            "",
        )

    def test_engine_uri_is_served_by_the_engine_client(self):
        payload = b"engine tier attachment bytes"
        client = FakeEngineClient(payload)
        result = self._resolve(ENGINE_URI, {"_engine_blob_client": client})
        self.assertEqual("temporalstore_engine_blob", result["raw_storage_policy"])
        self.assertTrue(result["raw_bytes_stored"])
        self.assertIsNone(result["parse_text"])
        self.assertEqual(payload, Path(result["parse_uri"]).read_bytes())
        self.assertTrue(client.fetch_calls, "the engine client must have served the fetch")

    def test_engine_uri_without_a_client_fails_loudly(self):
        with self.assertRaises(MatrixArkError):
            self._resolve(ENGINE_URI, {})

    def test_http_shaped_uri_never_consults_the_engine_client(self):
        engine_client = FakeEngineClient(b"must not be read")

        class StubHttpClient:
            def get(self, key, **kwargs):
                return b"http tier bytes", {"content_hash": "c" * 64}

        original = blob.TemporalStoreBlobClient
        blob.TemporalStoreBlobClient = StubHttpClient
        try:
            result = self._resolve(HTTP_URI, {"_engine_blob_client": engine_client})
        finally:
            blob.TemporalStoreBlobClient = original
        self.assertEqual([], engine_client.fetch_calls,
                         "the sha256 shape belongs to the HTTP tier alone")
        self.assertEqual(b"http tier bytes", Path(result["parse_uri"]).read_bytes())
        self.assertEqual("temporalstore_blob", result["raw_storage_policy"])


class EnginePutJoinsTheBackendChain(unittest.TestCase):
    def test_cloud_mode_with_only_an_engine_client_stores_through_it(self):
        client = FakeEngineClient(b"")
        result = resolve_raw_resource_for_ingest(
            {"raw_storage_mode": "cloud", "_engine_blob_client": client},
            {"kind": "resource", "scope": {"account_id": "a", "tenant_id": "t"},
             "metadata": {}, "messages": []},
            "inline-resource",
            "md",
            "local",
            "the attachment text that must land in the engine tier",
        )
        self.assertEqual(1, len(client.put_calls))
        _, stored = client.put_calls[0]
        self.assertEqual(b"the attachment text that must land in the engine tier", stored)
        self.assertEqual(ENGINE_URI, result["stored_raw_uri"])
        self.assertEqual("temporalstore_engine_blob", result["raw_storage_policy"])
        self.assertTrue(result["raw_bytes_stored"])


if __name__ == "__main__":
    unittest.main()
