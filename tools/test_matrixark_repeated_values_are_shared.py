# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""A value the corpus repeats is held once, not once per record.

``expand_interned_records`` already shares one object per distinct value, but only for records it
decodes off disk. A record that reaches the read cache from the append path was built field by
field in memory and never passed through it, so it kept a private dict for a value the corpus
repeats endlessly. Measured over 914 cached records from 60 attachments, ``storage_options`` was
held as 673 separate objects for 11 distinct values and ``storage_route`` as 793 objects for 2.

Sharing them took the cache from 3,877 B/record to 2,323 B/record -- 40% -- with the served
records byte-identical, which is asserted here.
"""
import json
import os
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import matrixark_mcp_local_adapter as adapter_module


def _ingest(adapter, count):
    body = "\n\n".join("## S%d\n\nrunbook %d." % (i, i) for i in range(4))
    scope = {"tenant_id": "acme", "user_id": "dana", "session_id": "s0"}
    for i in range(count):
        adapter.ingest({
            "kind": "resource",
            "scope": scope,
            "text": "# A %d\n\n%s" % (i, body),
            "metadata": {"raw_uri": "file:///d/a-%d.md" % i, "title": "a-%d" % i},
        })


class RepeatedValuesAreShared(unittest.TestCase):
    def setUp(self):
        adapter_module._SHARED_VALUE_TABLE.clear()
        with adapter_module._LOCAL_READ_CACHE_LOCK:
            adapter_module._LOCAL_READ_CACHE.clear()

    def test_records_written_in_memory_share_one_object_per_value(self):
        """The append path, which is the one that was not covered."""
        store = Path(tempfile.mkdtemp())
        adapter = adapter_module.MatrixArkLocalAdapter(store / "events.jsonl")
        _ingest(adapter, 8)
        cached = adapter._read_cache_records
        self.assertIsNotNone(cached, "nothing reached the cache, so this proves nothing")

        for field in ("storage_route", "storage_options"):
            values, objects = set(), set()
            for record in cached:
                value = record.get(field)
                # A value holding a container is deliberately left alone -- it cannot be keyed
                # by its contents cheaply -- so only the shareable ones are the claim here.
                if not isinstance(value, dict) or not value:
                    continue
                if not all(type(v) in adapter_module._SHAREABLE_SCALARS for v in value.values()):
                    continue
                objects.add(id(value))
                values.add(json.dumps(value, sort_keys=True, default=str))
            self.assertGreater(len(values), 0, "%s never appeared in a shareable form" % field)
            self.assertEqual(
                len(objects), len(values),
                "%s is held as %d objects for %d distinct values"
                % (field, len(objects), len(values)))

    def test_sharing_does_not_change_what_is_served(self):
        """The whole point is that this is invisible above the cache.

        ONE log, read twice. Ingesting separately per setting compares two different ingests --
        the timestamps and generated ids differ, so the digests could never match and the
        comparison would say nothing about sharing.
        """
        store = Path(tempfile.mkdtemp())
        log = store / "events.jsonl"
        _ingest(adapter_module.MatrixArkLocalAdapter(log), 6)

        served = {}
        for enabled in (False, True):
            adapter_module._SHARED_VALUE_TABLE.clear()
            with adapter_module._LOCAL_READ_CACHE_LOCK:
                adapter_module._LOCAL_READ_CACHE.clear()
            previous = adapter_module.SHARE_REPEATED_VALUES
            adapter_module.SHARE_REPEATED_VALUES = enabled
            try:
                records = adapter_module.MatrixArkLocalAdapter(log).read_all()
                served[enabled] = [json.dumps(r, sort_keys=True, default=str) for r in records]
            finally:
                adapter_module.SHARE_REPEATED_VALUES = previous

        self.assertGreater(len(served[True]), 0, "nothing was served, so this proves nothing")
        self.assertEqual(len(served[False]), len(served[True]), "the record COUNT changed")
        self.assertEqual(served[False], served[True],
                         "the served records changed, in content or in order")

    def test_a_shared_value_refuses_to_be_changed(self):
        """A mutation would reach every record carrying the value, so it must fail loudly."""
        store = Path(tempfile.mkdtemp())
        adapter = adapter_module.MatrixArkLocalAdapter(store / "events.jsonl")
        _ingest(adapter, 4)
        shared = next(
            (r["storage_route"] for r in adapter._read_cache_records
             if isinstance(r.get("storage_route"), adapter_module._SharedInternedValue)),
            None)
        self.assertIsNotNone(shared, "no storage_route was shared")
        with self.assertRaises(TypeError):
            shared["tier"] = "cold"
        self.assertEqual(dict(shared), dict(shared), "copying it must still work")

    def test_the_caller_record_is_left_writable(self):
        """Sharing happens on the copy the cache keeps, never on the caller's own record."""
        mine = {"record_type": "context_event", "storage_route": {"tier": "hot"}}
        table = {}
        out = adapter_module.share_repeated_values([mine], table)
        self.assertIsNot(out[0], mine, "the caller's record was replaced in place")
        mine["storage_route"]["tier"] = "cold"      # must not raise
        self.assertEqual("hot", out[0]["storage_route"]["tier"],
                         "the cache copy followed the caller's mutation")

    def test_a_value_holding_a_container_is_left_alone(self):
        """It cannot be keyed by its contents cheaply, so it keeps its own object."""
        nested = {"record_type": "context_event", "envelope": {"tags": ["a"], "tier": "hot"}}
        table = {}
        out = adapter_module.share_repeated_values([nested], table)
        self.assertIs(out[0], nested, "a record with nothing shareable should pass straight through")
        self.assertEqual({}, table)

    def test_the_table_stops_growing(self):
        """A field with unbounded distinct values must not turn the table into a leak."""
        table = {}
        limit = adapter_module._SHARED_VALUE_TABLE_LIMIT
        records = [{"record_type": "context_event", "storage_route": {"n": i}}
                   for i in range(limit + 50)]
        adapter_module.share_repeated_values(records, table)
        self.assertLessEqual(len(table), limit)


class RepeatedListsAreShared(unittest.TestCase):
    """A list a column repeats is held once too.

    Sharing lists was left out when values were first shared, because making one safe meant
    turning it into a tuple and changing the type a caller sees. A list SUBCLASS keeps the type
    and refuses the mutation instead. Worth 11.8% of a cold read; `node_path` alone was 147
    objects for THREE distinct values.
    """

    def test_a_repeated_flat_list_is_held_once(self):
        table = {}
        records = [{"record_type": "context_node", "node_path": ["a", "b"]},
                   {"record_type": "context_node", "node_path": ["a", "b"]}]
        first, second = adapter_module.share_repeated_values(records, table)
        self.assertEqual(["a", "b"], first["node_path"])
        self.assertIs(first["node_path"], second["node_path"],
                      "the same path in two records is two objects")

    def test_a_shared_list_refuses_every_way_of_changing_it(self):
        table = {}
        shared = adapter_module.share_repeated_values(
            [{"record_type": "context_node", "node_path": ["a"]}], table)[0]["node_path"]
        self.assertIsInstance(shared, list, "a caller must still see a list")
        for change in (lambda: shared.append("b"),
                       lambda: shared.extend(["b"]),
                       lambda: shared.insert(0, "b"),
                       lambda: shared.pop(),
                       lambda: shared.clear(),
                       lambda: shared.sort(),
                       lambda: shared.reverse(),
                       lambda: shared.__setitem__(0, "b")):
            with self.assertRaises(TypeError):
                change()
        self.assertEqual(["a"], list(shared), "a refused change still happened")

    def test_the_way_a_caller_is_told_to_change_one_works(self):
        table = {}
        shared = adapter_module.share_repeated_values(
            [{"record_type": "context_node", "node_path": ["a"]}], table)[0]["node_path"]
        mine = list(shared)
        mine.append("b")
        self.assertEqual(["a"], list(shared), "the copy reached the shared list")

    def test_a_list_holding_a_container_is_left_alone(self):
        table = {}
        record = {"record_type": "context_node", "tags": [{"k": "v"}]}
        out = adapter_module.share_repeated_values([record], table)
        self.assertIs(out[0], record)
        self.assertEqual({}, table)

    def test_a_shared_list_survives_json_and_copying(self):
        """It is serialised on the way out and deep-copied by callers that need their own."""
        import copy as copy_module
        table = {}
        shared = adapter_module.share_repeated_values(
            [{"record_type": "context_node", "node_path": ["a", "b"]}], table)[0]["node_path"]
        self.assertEqual('["a", "b"]', json.dumps(shared))
        copied = copy_module.deepcopy(shared)
        copied.append("c")
        self.assertEqual(["a", "b"], list(shared))

if __name__ == "__main__":
    unittest.main()
