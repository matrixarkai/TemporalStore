#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""A failed idempotency point-read propagates; it never becomes a full-store scan.

The scan fallback was a congestion amplifier, caught live: the point-read only fails when the
proxy lanes are already starved, this lookup runs first on EVERY tool call, and the fallback
launched a full-store `read_all()` at exactly that moment -- starving the lanes further so the
next point-read failed too. Two of three request threads were sitting in that scan while metrics
timed out at 120s.

The invariants pinned here: an engine failure is retried on the CHEAP read and then raised, and
the scan survives only for a value that does not parse -- corruption, which is rare and not
load-correlated, so scanning there cannot spiral.
"""
import json
import unittest

try:
    from tools import matrixark_mcp_temporal_adapters as adapters
except ImportError:  # run from tools/ dir
    import matrixark_mcp_temporal_adapters as adapters


class _Client:
    def __init__(self, *, value=None, fail_times=0):
        self.value = value
        self.fail_times = fail_times
        self.hget_calls = 0

    def hget(self, key, field):
        self.hget_calls += 1
        if self.hget_calls <= self.fail_times:
            raise RuntimeError("lane starved: request timed out")
        return self.value or ""


def _adapter(client):
    adapter = object.__new__(adapters.MatrixArkTemporalStoreDirectAdapter)
    adapter._storage_prefix = "matrixark:mcp"
    adapter._client = client
    adapter._idempotency_index_built = True  # the backfill is not under test
    adapter.scans = 0

    def _scan(key_hash):
        adapter.scans += 1
        return {"from": "scan", "key_hash": key_hash}

    adapter.find_idempotency_record_in_log = _scan
    return adapter


class IdempotencyLookupTests(unittest.TestCase):
    def test_a_hit_is_served_from_the_point_read(self):
        stored = json.dumps({"tool_name": "matrixark_ingest", "key_hash": 7})
        adapter = _adapter(_Client(value=stored))
        record = adapter.find_idempotency_record(7)
        self.assertEqual("matrixark_ingest", record["tool_name"])
        self.assertEqual(0, adapter.scans)

    def test_a_miss_is_none_without_a_scan(self):
        adapter = _adapter(_Client(value=""))
        self.assertIsNone(adapter.find_idempotency_record(7))
        self.assertEqual(0, adapter.scans)

    def test_a_transient_failure_is_retried_on_the_cheap_read(self):
        """One blip must heal with another point-read, not a store-wide one."""
        stored = json.dumps({"tool_name": "matrixark_ingest"})
        client = _Client(value=stored, fail_times=1)
        adapter = _adapter(client)
        record = adapter.find_idempotency_record(7)
        self.assertEqual("matrixark_ingest", record["tool_name"])
        self.assertEqual(2, client.hget_calls)
        self.assertEqual(0, adapter.scans)

    def test_a_persistent_failure_raises_and_never_scans(self):
        """The amplifier: under lane starvation the old code launched a full-store read here."""
        client = _Client(fail_times=99)
        adapter = _adapter(client)
        with self.assertRaises(RuntimeError):
            adapter.find_idempotency_record(7)
        self.assertEqual(3, client.hget_calls, "retried twice, then propagated")
        self.assertEqual(0, adapter.scans, "an engine failure must never become a scan")

    def test_corruption_still_falls_back_to_the_log(self):
        """The one surviving scan: bytes the log never produced. Rare, and not load-correlated."""
        adapter = _adapter(_Client(value="{not json"))
        record = adapter.find_idempotency_record(7)
        self.assertEqual("scan", record["from"])
        self.assertEqual(1, adapter.scans)


if __name__ == "__main__":
    unittest.main()
