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


class RepeatedStringValuesAreShared(unittest.TestCase):
    """A value that repeats down a column is held once, and a column of distinct values is not paid for.

    Sharing the strings a column repeats is worth 11.3% of a cold read. The risk is the opposite
    column: `row_key` holds 149 distinct values over 149 rows, and at scale those would flood a
    table and crowd out the columns that do repeat. Each field is abandoned once it proves itself
    distinct, so the two kinds separate on their own.
    """

    def setUp(self):
        adapter_module._SHARED_STRINGS.clear()
        adapter_module._SHARED_STRINGS_ABANDONED.clear()

    def test_a_repeated_value_is_held_once(self):
        first = adapter_module._shared_string("record_type", "context_" + "event")
        second = adapter_module._shared_string("record_type", "context_" + "event")
        self.assertEqual(first, second)
        self.assertIs(first, second, "the same value in two records is two objects")

    def test_two_fields_do_not_share_a_table(self):
        """Cardinality is a property of the column, so one busy column must not poison another."""
        a = adapter_module._shared_string("record_type", "x" * 5)
        b = adapter_module._shared_string("state", "x" * 5)
        self.assertEqual(a, b)
        self.assertIn("record_type", adapter_module._SHARED_STRINGS)
        self.assertIn("state", adapter_module._SHARED_STRINGS)

    def test_a_field_of_distinct_values_is_abandoned(self):
        limit = adapter_module._STRING_FIELD_CARDINALITY_LIMIT
        for i in range(limit + 5):
            adapter_module._shared_string("row_key", "row-%06d" % i)
        self.assertIn("row_key", adapter_module._SHARED_STRINGS_ABANDONED)
        self.assertNotIn("row_key", adapter_module._SHARED_STRINGS,
                         "the abandoned field is still holding its values")

    def test_an_abandoned_field_still_returns_its_value(self):
        adapter_module._SHARED_STRINGS_ABANDONED.add("row_key")
        self.assertEqual("abc", adapter_module._shared_string("row_key", "abc"))

    def test_a_long_value_is_not_hashed(self):
        long_value = "x" * (adapter_module._SHARED_STRING_MAX_LEN + 1)
        self.assertEqual(long_value, adapter_module._shared_string("text", long_value))
        self.assertNotIn("text", adapter_module._SHARED_STRINGS)

    def test_a_cold_read_shares_a_repeated_column(self):
        store = Path(tempfile.mkdtemp())
        log = store / "events.jsonl"
        adapter = adapter_module.MatrixArkLocalAdapter(log)
        blank = chr(10) + chr(10)
        body = blank.join("## S%d%srunbook %d." % (i, blank, i) for i in range(4))
        scope = {"tenant_id": "acme", "user_id": "dana", "session_id": "s0"}
        for i in range(6):
            adapter.ingest({"kind": "resource", "scope": scope,
                            "text": "# A %d%s%s" % (i, blank, body),
                            "metadata": {"raw_uri": "file:///d/a-%d.md" % i,
                                         "title": "a-%d" % i}})
        with adapter_module._LOCAL_READ_CACHE_LOCK:
            adapter_module._LOCAL_READ_CACHE.clear()
        records = adapter_module.MatrixArkLocalAdapter(log).read_all()
        objects, values = set(), set()
        for record in records:
            value = record.get("record_type")
            if isinstance(value, str):
                objects.add(id(value))
                values.add(value)
        self.assertGreater(len(records), len(values),
                           "every row has its own type, so nothing repeats")
        self.assertEqual(len(objects), len(values),
                         "record_type is %d objects for %d values"
                         % (len(objects), len(values)))

if __name__ == "__main__":
    unittest.main()
