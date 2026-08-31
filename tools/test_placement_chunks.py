#!/usr/bin/env python3
"""Placement chunking must bound what an append writes WITHOUT ever losing a location.

The dangerous failure is silent: a dropped location is a memory that retrieval simply cannot see,
with no error anywhere. So the test drives many appends through the real writer, reassembles the
chunks the way the reader does, and asserts the full set survives -- while the per-append write
stays bounded.
"""
import json
import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from tools.matrixark_temporal_direct_backend import _TemporalDirectBackendMixin  # noqa: E402


class PlacementChunkTest(unittest.TestCase):
    def setUp(self):
        self.store = {}          # (key, field) -> value, standing in for the hash

        backend = _TemporalDirectBackendMixin.__new__(_TemporalDirectBackendMixin)
        self.backend = backend

    def existing_for(self, key, field):
        return self.store.get((key, field), "")

    def append(self, node_field, locations, versions=frozenset()):
        entries = self.backend._placement_entries_for_node(
            "ph:context_placement_lookup:s1", node_field, locations, set(versions),
            self.existing_for, {},
        )
        for entry in entries:
            self.store[(entry["key"], entry["field"])] = entry["value"]
        return entries

    def reassemble(self, node_field):
        """What the reader does: head, then each chunk it names."""
        key = "ph:context_placement_lookup:s1"
        head = self.store.get((key, node_field), "")
        if not head:
            return []
        decoded = json.loads(head)
        found = list(decoded.get("locations") or [])
        for index in range(1, int(decoded.get("location_chunks") or 0) + 1):
            chunk = self.store.get((key, f"{node_field}#{index}"), "")
            if chunk:
                found.extend(json.loads(chunk).get("locations") or [])
        return found

    def test_every_location_survives_and_each_append_stays_bounded(self):
        total = 500
        biggest_write = 0
        for i in range(total):
            entries = self.append("node1", [{"key": "recs:000001", "field": "%020d" % i}])
            for entry in entries:
                biggest_write = max(biggest_write, len(entry["value"]))

        found = self.reassemble("node1")
        pairs = {(loc["key"], loc["field"]) for loc in found}
        self.assertEqual(len(pairs), total, "a location was lost: %d of %d" % (len(pairs), total))
        for i in range(total):
            self.assertIn(("recs:000001", "%020d" % i), pairs)

        # Bounded: the whole list is 500 locations, but no single append ever wrote all of them.
        # Without chunking the last append alone would carry every one.
        whole_list_bytes = len(json.dumps([{"key": "recs:000001", "field": "%020d" % i}
                                           for i in range(total)], separators=(",", ":")))
        self.assertLess(biggest_write, whole_list_bytes // 4,
                        "an append wrote %d bytes; the unchunked list is %d"
                        % (biggest_write, whole_list_bytes))

    def test_a_list_written_before_chunking_still_reads_and_keeps_growing(self):
        key = "ph:context_placement_lookup:s1"
        legacy = [{"key": "recs:000001", "field": "%020d" % i} for i in range(200)]
        self.store[(key, "node2")] = json.dumps(
            {"locations": legacy, "resource_versions": []}, separators=(",", ":"))

        self.append("node2", [{"key": "recs:000002", "field": "new-1"}])
        self.append("node2", [{"key": "recs:000002", "field": "new-2"}])

        pairs = {(loc["key"], loc["field"]) for loc in self.reassemble("node2")}
        self.assertEqual(len(pairs), 202, "legacy locations or new ones were lost")
        self.assertIn(("recs:000001", "%020d" % 0), pairs)
        self.assertIn(("recs:000002", "new-2"), pairs)

    def test_repeating_a_location_does_not_duplicate_it(self):
        for _ in range(5):
            self.append("node3", [{"key": "recs:000003", "field": "same"}])
        found = self.reassemble("node3")
        self.assertEqual(len(found), 1, "a repeated append duplicated a location: %r" % (found,))


if __name__ == "__main__":
    unittest.main(verbosity=2)
