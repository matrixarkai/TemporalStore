#!/usr/bin/env python3
"""Tests for MatrixObject resource/skill blob storage (local-fs backend)."""
import os
import tempfile
import unittest

import matrixark_object_store as obj


class BackendResolutionTest(unittest.TestCase):
    def test_priority_rpc_then_fs_then_inline(self):
        self.assertEqual("matrixobject_rpc", obj.resolve_object_backend(rpc_url="http://x", local_dir="/tmp/z"))
        self.assertEqual("local_fs", obj.resolve_object_backend(rpc_url="", local_dir="/tmp/z"))
        self.assertEqual("inline", obj.resolve_object_backend(rpc_url="", local_dir=""))

    def test_content_key_is_content_addressed(self):
        self.assertEqual(obj.content_key(b"hello"), obj.content_key(b"hello"))
        self.assertNotEqual(obj.content_key(b"hello"), obj.content_key(b"world"))
        self.assertTrue(obj.content_key(b"hello").endswith(obj.content_hash(b"hello")))

    def test_object_uri(self):
        self.assertEqual("matrixobject://b/k", obj.object_uri("b", "k"))


class LocalFsBackendTest(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.mkdtemp()
        self.client = obj.MatrixObjectClient(backend="local_fs", bucket="res", local_dir=self.dir)

    def test_put_get_roundtrip(self):
        data = b"# CLAUDE.md\nrules here"
        res = self.client.put(data, content_type="text/markdown", metadata={"name": "claude"})
        self.assertEqual("local_fs", res.backend)
        self.assertFalse(res.deduplicated)
        self.assertTrue(res.uri.startswith("matrixobject://res/"))
        got, meta = self.client.get(res.key)
        self.assertEqual(data, got)
        self.assertEqual("text/markdown", meta["content_type"])
        self.assertEqual(res.content_hash, meta["content_hash"])
        self.assertEqual("claude", meta["name"])

    def test_identical_bytes_dedup(self):
        d = b"same bytes"
        first = self.client.put(d)
        second = self.client.put(d)
        self.assertFalse(first.deduplicated)
        self.assertTrue(second.deduplicated)          # content-addressed -> not rewritten
        self.assertEqual(first.key, second.key)

    def test_exists(self):
        res = self.client.put(b"probe")
        self.assertTrue(self.client.exists(res.key))
        self.assertFalse(self.client.exists("no/such/key"))


class StorageResolutionTest(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.mkdtemp()

    def test_matrixobject_resolution_shape(self):
        client = obj.MatrixObjectClient(backend="local_fs", bucket="skills", local_dir=self.dir)
        res = obj.resource_blob_storage_resolution(
            client, b"skill body", source_uri="skill://discovered/fix-auth",
            content_type="text/markdown", kind="skill", name="Fix auth",
        )
        self.assertEqual("matrixobject", res["storage_mode"])
        self.assertEqual("uploaded", res["upload_status"])
        self.assertEqual("skills", res["cloud_bucket"])
        self.assertTrue(res["cloud_key"])
        self.assertTrue(res["stored_raw_uri"].startswith("matrixobject://skills/"))
        self.assertTrue(res["raw_bytes_stored"])
        self.assertEqual(len(b"skill body"), res["raw_bytes"])

    def test_second_upload_is_deduplicated(self):
        client = obj.MatrixObjectClient(backend="local_fs", bucket="res", local_dir=self.dir)
        obj.resource_blob_storage_resolution(client, b"doc", source_uri="x")
        res2 = obj.resource_blob_storage_resolution(client, b"doc", source_uri="x")
        self.assertEqual("deduplicated", res2["upload_status"])

    def test_inline_mode_keeps_content_inline(self):
        client = obj.MatrixObjectClient(backend="inline")
        res = obj.resource_blob_storage_resolution(client, b"doc", source_uri="mem://a")
        self.assertEqual("inline", res["storage_mode"])
        self.assertEqual("not_required", res["upload_status"])
        self.assertFalse(res["raw_bytes_stored"])
        self.assertEqual("", res["cloud_key"])


if __name__ == "__main__":
    unittest.main()
