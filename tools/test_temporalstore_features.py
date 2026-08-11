# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Offline unit tests for temporalstore.features (mock transport, no server)."""
import json
import sys
import unittest
from pathlib import Path
from typing import Any, Dict, List, Tuple

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "sdk" / "python"))

from temporalstore.features import (  # noqa: E402
    CapDecision,
    Config,
    TemporalFeatureStore,
    TransientError,
    bucket_floor,
)

MINUTE = 60_000


class MockTransport:
    """Records posts; returns canned {status,response} envelopes via `responder`."""

    def __init__(self, responder):
        self.responder = responder
        self.calls: List[Tuple[str, Dict[str, Any]]] = []
        self.fail_next = 0

    def post(self, url, body, headers, timeout_s):
        path = "/" + url.split("://", 1)[-1].split("/", 1)[-1]
        payload = json.loads(body.decode()) if body else {}
        self.calls.append((path, payload))
        if self.fail_next > 0:
            self.fail_next -= 1
            raise TransientError(0, "mock transient")
        kind, value = self.responder(path, payload)
        env = {"status": {"ok": True}, "response": {"kind": kind, "value": value}}
        return 200, json.dumps(env).encode()

    def close(self):
        pass

    def last(self, path):
        for p, b in reversed(self.calls):
            if p == path:
                return b
        return None


def _store(responder):
    cfg = Config(endpoint="http://127.0.0.1:17102", namespace="ns", table="feat",
                 backoff_base_s=0.0, backoff_max_s=0.0)
    t = MockTransport(responder)
    return TemporalFeatureStore(cfg, transport=t), t


class FeatureStoreTests(unittest.TestCase):
    def test_append_and_aggregate(self):
        fs, t = _store(lambda p, b: ("aggregate", 42) if p.endswith("/FeatureAggQuery") else ("empty", 0))
        fs.append("feature:user:u42", 5000, 91)
        add = t.last("/ProxyService/FeatureAdd")
        self.assertEqual(add["namespace"], "ns")
        self.assertEqual(add["table_name"], "feat")
        self.assertEqual(add["points"][0]["timestamp_ms"], 5000)
        self.assertEqual(add["points"][0]["value"], list(b"91"))
        self.assertEqual(fs.aggregate("feature:user:u42", 0, 10_000, "sum"), 42)
        agg = t.last("/ProxyService/FeatureAggQuery")
        self.assertEqual(agg["aggregator"], "sum")

    def test_control_state_increment_count(self):
        fs, t = _store(lambda p, b: ("integer", 7) if p.endswith("/ControlStateCount") else ("empty", 0))
        fs.cs_increment("cs:u42", 6100, amount=1, precision_ms=MINUTE, ttl_ms=86_400_000)
        inc = t.last("/ProxyService/ControlStateIncrement")
        self.assertEqual(inc["timestamp_ms"], 6100)
        self.assertEqual(inc["precision_ms"], MINUTE)
        self.assertEqual(fs.cs_count("cs:u42", 0, 7000), 7)

    def test_hybrid_boundary_sum_and_count(self):
        now = 7 * 86_400_000 + 3 * MINUTE + 30_000
        window_start = now - 7 * 86_400_000
        tail_start = bucket_floor(now, MINUTE)
        self.assertEqual(now - tail_start, 30_000)

        def resp_sum(p, b):
            if p.endswith("/ExecuteTableCmd"):
                self.assertEqual(b["command"]["kind"], "control_state_family_query")
                self.assertEqual(b["command"]["aggregator"], "sum")
                self.assertEqual(b["command"]["end_ms"], tail_start - 1)
                return "aggregate", 1000
            if p.endswith("/FeatureAggQuery"):
                self.assertEqual(b["start_ms"], tail_start)
                return "aggregate", 7
            return "empty", 0

        fs, _ = _store(resp_sum)
        self.assertEqual(
            fs.aggregate_long_window("f", "cs", window_start, now, op="sum", precision_ms=MINUTE), 1007)

        def resp_count(p, b):
            if p.endswith("/ControlStateCount"):
                self.assertEqual(b["end_ms"], tail_start - 1)
                return "integer", 1000
            if p.endswith("/FeatureAggQuery"):
                self.assertEqual(b["aggregator"], "count")
                return "aggregate", 7
            return "empty", 0

        fs2, _ = _store(resp_count)
        self.assertEqual(
            fs2.aggregate_long_window("f", "cs", window_start, now, op="count", precision_ms=MINUTE), 1007)

    def test_retry_then_success(self):
        fs, t = _store(lambda p, b: ("aggregate", 5))
        t.fail_next = 2
        self.assertEqual(fs.aggregate("f", 0, 1, "count"), 5)
        self.assertEqual(len([c for c in t.calls if c[0].endswith("/FeatureAggQuery")]), 3)

    def test_frequency_cap(self):
        state = {"count": 4}

        def responder(p, b):
            if p.endswith("/ControlStateCount"):
                return "integer", state["count"]
            if p.endswith("/ControlStateIncrement"):
                state["count"] += 1
                return "empty", 0
            return "empty", 0

        fs, _ = _store(responder)
        d1 = fs.frequency_cap("cs:cap", 1_000_000, limit=5, window_ms=86_400_000)
        self.assertIsInstance(d1, CapDecision)
        self.assertTrue(d1.allowed)
        self.assertEqual(d1.remaining, 0)
        d2 = fs.frequency_cap("cs:cap", 1_000_100, limit=5, window_ms=86_400_000)
        self.assertFalse(d2.allowed)
        self.assertEqual(d2.reason, "frequency_cap_exceeded")


if __name__ == "__main__":
    unittest.main(verbosity=2)
