#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""A placement head is rewritten in whatever form it was already stored in.

`_locator_entries_for_ref` and `_placement_entries_for_node` are the same routine written twice:
read the head, append to the tail chunk, and rewrite the head when the chunk count rolls over. The
locator compacts its head on that rewrite. The placement one did not -- it wrote `head_locations`
straight back -- so a head holding the sixty-nine-byte `{"key", "field"}` form re-emitted every one
of those entries on each rollover and never upgraded, however many times it was rewritten.

That is not only a historical-data concern. The head is read back from storage, so the long form is
self-perpetuating: once a head holds it, every later rollover writes it out again.

Measured on the live one-box log before this: one 331,054-byte segment carried 364 long-form
location dicts, 28,392 bytes, 8.58% of the segment -- and every one of them was the plainly
compactable shape (key `base` + six digits, field twenty digits).

The tail assertions are the positive control. They share the fixture and the base with the head, so
a failure there would mean the fixture never had a compactable location to begin with; the head
assertions only mean something because the tail ones pass.
"""
import json
import unittest

try:
    from tools import matrixark_temporal_direct_backend as backend
    from tools.matrixark_temporal_location_codec import expand_location
except ImportError:  # run from tools/ dir
    import matrixark_temporal_direct_backend as backend
    from matrixark_temporal_location_codec import expand_location

PREFIX = "matrixark:mcp"
BASE = f"{PREFIX}:records"
NODE_KEY = f"{PREFIX}:context_placement_lookup:t=1"
NODE_FIELD = "909"


class _Client:
    def __init__(self, stored):
        self.stored = dict(stored)

    def hget(self, key, field):
        return self.stored.get((key, field), "")

    def batch_hget(self, entries):
        return [
            {"key": e["key"], "field": e["field"],
             "value": self.stored.get((e["key"], e["field"]), "")}
            for e in entries
        ]


def _adapter(client):
    obj = object.__new__(backend._TemporalDirectBackendMixin)
    obj._client = client
    obj._storage_prefix = PREFIX
    return obj


def _long(shard: int, offset: int) -> dict:
    """The sixty-nine byte form, in the shape the codec is able to compact."""
    return {"key": "%s:%06d" % (BASE, shard), "field": "%020d" % offset}


def _run(adapter, client, head_payload, new_locations):
    client.stored[(NODE_KEY, NODE_FIELD)] = json.dumps(head_payload)

    def existing_for(key, field):
        return client.stored.get((key, field), "")

    return adapter._placement_entries_for_node(
        NODE_KEY, NODE_FIELD, new_locations, set(), existing_for, {})


def _by_field(entries):
    return {row["field"]: json.loads(row["value"]) for row in entries}


class PlacementHeadFormTests(unittest.TestCase):
    def setUp(self):
        self.client = _Client({})
        self.adapter = _adapter(self.client)
        # A head already at the chunk limit, so appending one more location rolls the chunk over
        # and forces the head rewrite -- the only branch that touches head_locations.
        self.head_locations = [_long(0, i) for i in range(backend._TemporalDirectBackendMixin.PLACEMENT_CHUNK_LOCATIONS)]

    def test_the_rewritten_head_is_stored_compact(self):
        entries = _run(self.adapter, self.client,
                       {"locations": self.head_locations, "resource_versions": []},
                       [_long(0, 500)])
        by_field = _by_field(entries)
        self.assertIn(NODE_FIELD, by_field, "the head was not rewritten, so the branch never ran")
        head = by_field[NODE_FIELD]["locations"]
        long_form = [item for item in head if isinstance(item, dict)]
        self.assertEqual(
            [], long_form,
            f"{len(long_form)} of {len(head)} head locations kept the long form")

    def test_the_tail_is_stored_compact(self):
        """Positive control: the same fixture and base DO compact on the tail."""
        entries = _run(self.adapter, self.client,
                       {"locations": self.head_locations, "resource_versions": []},
                       [_long(0, 500)])
        by_field = _by_field(entries)
        tail_fields = [f for f in by_field if f != NODE_FIELD]
        self.assertTrue(tail_fields, "no tail chunk was written")
        tail = by_field[tail_fields[0]]["locations"]
        self.assertEqual([], [item for item in tail if isinstance(item, dict)],
                         "the tail did not compact, so the fixture is wrong, not the head")
        self.assertIn("0:500", tail)

    def test_the_head_still_names_the_same_records(self):
        """Compacting is a re-encoding, not a change of contents."""
        entries = _run(self.adapter, self.client,
                       {"locations": self.head_locations, "resource_versions": []},
                       [_long(0, 500)])
        head = _by_field(entries)[NODE_FIELD]["locations"]
        self.assertEqual(
            [(loc["key"], loc["field"]) for loc in self.head_locations],
            [expand_location(item, BASE) for item in head],
            "the head no longer points at the same records")

    def test_a_head_already_compact_is_left_alone(self):
        """Idempotent: re-encoding an already-compact head must not disturb it."""
        compact_head = ["0:%d" % i for i in range(backend._TemporalDirectBackendMixin.PLACEMENT_CHUNK_LOCATIONS)]
        entries = _run(self.adapter, self.client,
                       {"locations": compact_head, "resource_versions": []},
                       [_long(0, 500)])
        self.assertEqual(compact_head, _by_field(entries)[NODE_FIELD]["locations"])

    def test_a_foreign_base_stays_a_dict(self):
        """A location the compact form cannot express must survive the rewrite unchanged."""
        foreign = {"key": "someone:else:records:000000", "field": "00000000000000000007"}
        head = [foreign] + [_long(0, i) for i in range(backend._TemporalDirectBackendMixin.PLACEMENT_CHUNK_LOCATIONS)]
        entries = _run(self.adapter, self.client,
                       {"locations": head, "resource_versions": []},
                       [_long(0, 500)])
        stored = _by_field(entries)[NODE_FIELD]["locations"]
        self.assertIn(foreign, stored, "the foreign-base location was lost or mangled")


if __name__ == "__main__":
    unittest.main()
