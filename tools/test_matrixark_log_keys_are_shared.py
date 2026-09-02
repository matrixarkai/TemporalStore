# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""One string per key name across the whole log, not one per record.

The JSON decoder memoises key strings within a single call, but the log is read a line at a time,
so before this every record carried its own copy of every key name. Measured over a cold read of
914 records: 148 distinct key names backed by **16,743 separate string objects**, holding 1,009.7
KB of which 1,000.3 KB was one name repeated.

Interning them took a cold read from 6,484 B/record to 4,532 B/record -- 30% -- with the served
records byte-identical and read latency unchanged.
"""
import json
import os
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import matrixark_mcp_local_adapter as adapter_module


class LogKeysAreShared(unittest.TestCase):
    def setUp(self):
        with adapter_module._LOCAL_READ_CACHE_LOCK:
            adapter_module._LOCAL_READ_CACHE.clear()
        adapter_module._SHARED_VALUE_TABLE.clear()

    def _written_log(self, count=6):
        store = Path(tempfile.mkdtemp())
        log = store / "events.jsonl"
        adapter = adapter_module.MatrixArkLocalAdapter(log)
        body = "\n\n".join("## S%d\n\nrunbook %d." % (i, i) for i in range(4))
        scope = {"tenant_id": "acme", "user_id": "dana", "session_id": "s0"}
        for i in range(count):
            adapter.ingest({
                "kind": "resource", "scope": scope,
                "text": "# A %d\n\n%s" % (i, body),
                "metadata": {"raw_uri": "file:///d/a-%d.md" % i, "title": "a-%d" % i},
            })
        return log

    def test_two_lines_parsed_separately_share_their_key_strings(self):
        """The exact thing the plain decoder does not do."""
        line = json.dumps({"record_type": "context_event", "node_hash": 1})
        first = adapter_module.loads_with_interned_keys(line)
        second = adapter_module.loads_with_interned_keys(line)
        for key in ("record_type", "node_hash"):
            a = next(k for k in first if k == key)
            b = next(k for k in second if k == key)
            self.assertIs(a, b, "%r is a separate object in each record" % key)

    def test_a_cold_read_holds_one_object_per_key_name(self):
        log = self._written_log()
        with adapter_module._LOCAL_READ_CACHE_LOCK:
            adapter_module._LOCAL_READ_CACHE.clear()
        records = adapter_module.MatrixArkLocalAdapter(log).read_all()
        self.assertGreater(len(records), 1, "one record proves nothing about sharing")

        objects_per_name = {}
        for record in records:
            for key in record.keys():
                objects_per_name.setdefault(key, set()).add(id(key))
        repeated = {k: len(v) for k, v in objects_per_name.items() if len(v) > 1}
        self.assertEqual({}, repeated,
                         "these key names are backed by more than one string object")

    def test_interning_does_not_change_what_was_decoded(self):
        """Interning is invisible: the strings compare equal either way."""
        for payload in (
            {"record_type": "context_event", "updated_at_ms": 17, "scope": {"tenant_id": "acme"}},
            {"a": [1, {"b": "c"}], "": None, "9": 9.5, "unicode key é": True},
        ):
            line = json.dumps(payload)
            self.assertEqual(json.loads(line), adapter_module.loads_with_interned_keys(line))

    def test_a_non_string_key_is_left_alone(self):
        """JSON object keys are always strings, but the hook must not assume it and crash."""
        self.assertEqual(
            {"1": "a"}, adapter_module._interned_pairs([("1", "a")]))
        self.assertEqual(
            {2: "b"}, adapter_module._interned_pairs([(2, "b")]))


if __name__ == "__main__":
    unittest.main()
