#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Enterprise ingest client: buffering, finality, idempotency, retry — no network."""
import io
import os
import sys
import unittest
import urllib.error

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from temporalstore.ingest_client import TemporalStoreClient, TemporalStoreError


class BufferingTest(unittest.TestCase):
    def setUp(self):
        self.c = TemporalStoreClient("http://x", "k", account_id="a", user_id="u", flush_threshold=3)
        self.posts = []
        self.c._post = lambda path, body, idempotency_key=None: (
            self.posts.append((path, body, idempotency_key)) or {"status": "ok"}
        )

    def test_add_buffers_without_posting(self):
        self.c.add("s1", "user", "hello")
        self.assertEqual([], self.posts)                     # buffered, not sent

    def test_auto_flush_at_threshold(self):
        for i in range(3):
            self.c.add("s1", "tool", f"out {i}")
        self.assertEqual(1, len(self.posts))                 # threshold=3 -> one flush
        self.assertEqual("/api/ingest", self.posts[0][0])
        self.assertEqual(3, len(self.posts[0][1]["messages"]))
        self.assertNotIn("final_session_boundary", self.posts[0][1])

    def test_finalize_flushes_as_boundary(self):
        self.c.add("s1", "user", "q")
        self.c.finalize("s1", "assistant", "answer")
        self.assertEqual(1, len(self.posts))
        body = self.posts[0][1]
        self.assertTrue(body["final_session_boundary"])
        self.assertEqual("final", body["messages"][-1]["finality"])
        self.assertEqual("provisional", body["messages"][0]["finality"])
        self.assertEqual({"account_id": "a", "user_id": "u", "session_id": "s1"}, body["scope"])

    def test_flush_empty_is_noop(self):
        self.assertEqual("empty", self.c.flush("nope")["status"])
        self.assertEqual([], self.posts)


class IdempotencyTest(unittest.TestCase):
    def test_key_is_deterministic_for_same_batch(self):
        c = TemporalStoreClient("http://x", "k")
        b = [{"role": "user", "content": "hi", "finality": "provisional"}]
        k1 = c._idempotency_key("s", b, True)
        k2 = c._idempotency_key("s", b, True)
        self.assertEqual(k1, k2)                              # retries reuse the key -> server dedups
        self.assertNotEqual(k1, c._idempotency_key("s", b, False))
        self.assertTrue(k1.startswith("ts-"))


class RetryTest(unittest.TestCase):
    def test_retries_on_503_then_succeeds(self):
        c = TemporalStoreClient("http://x", "k", max_retries=2, backoff_base_s=0.0)
        calls = {"n": 0}

        def fake_urlopen(req, timeout=None):
            calls["n"] += 1
            if calls["n"] == 1:
                raise urllib.error.HTTPError(req.full_url, 503, "busy", {}, io.BytesIO(b""))
            return io.BytesIO(b'{"status":"ok"}')

        import temporalstore.ingest_client as m
        orig = m.urllib.request.urlopen
        m.urllib.request.urlopen = fake_urlopen
        try:
            out = c._post("/api/ingest", {"messages": []})
        finally:
            m.urllib.request.urlopen = orig
        self.assertEqual({"status": "ok"}, out)
        self.assertEqual(2, calls["n"])                      # one retry

    def test_4xx_raises_without_retry(self):
        c = TemporalStoreClient("http://x", "k", max_retries=3, backoff_base_s=0.0)

        def fake_urlopen(req, timeout=None):
            raise urllib.error.HTTPError(req.full_url, 400, "bad", {}, io.BytesIO(b"bad request"))

        import temporalstore.ingest_client as m
        orig = m.urllib.request.urlopen
        m.urllib.request.urlopen = fake_urlopen
        try:
            with self.assertRaises(TemporalStoreError):
                c._post("/api/ingest", {"messages": []})
        finally:
            m.urllib.request.urlopen = orig


if __name__ == "__main__":
    unittest.main()
