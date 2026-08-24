#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""The side index ACCUMULATES: a new write must not erase what earlier writes stored there.

The batched read exists to stop an ingest costing one round trip per side-index entry. Its danger
is silent: these rows are built by merging new refs into the EXISTING value, so a read that returns
"" where a value exists does not fail -- it drops every posting already under that key.

A retrieval check cannot catch that. It was tried: with the bug deliberately injected, five facts
were still all recalled, because retrieval does not depend on these rows for that workload. So the
rows themselves are asserted here, against a client that HAS the earlier value stored.
"""
import json
import unittest

try:
    from tools import matrixark_temporal_direct_backend as backend
except ImportError:  # run from tools/ dir
    import matrixark_temporal_direct_backend as backend

PREFIX = "matrixark:mcp"
SCOPE_KEY = "t=11|u=22|s=33|"


class _Client:
    """A client whose hash already holds the side-index rows an earlier ingest wrote."""

    def __init__(self, stored, *, batch=True):
        self.stored = dict(stored)
        self.batch_calls = 0
        self.hget_calls = 0
        if not batch:
            self.batch_hget = None

    def hget(self, key, field):
        self.hget_calls += 1
        return self.stored.get((key, field), "")

    def batch_hget(self, entries):
        self.batch_calls += 1
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


def _index_record(ref_hash):
    return {
        "record_type": "context_index",
        "index_name": "text",
        "scope_key": SCOPE_KEY,
        "ref_hash": ref_hash,
        "ref_hashes": [ref_hash],
        "node_hash": 909,
        "posting_bucket": 1,
    }


def _entries_by_key(entries):
    return {(row["key"], row["field"]): row for row in entries}


class SideIndexMergeTests(unittest.TestCase):
    def setUp(self):
        self.locator_key = f"{PREFIX}:context_ref_locator"
        self.existing_locator = json.dumps(
            {"locations": [{"key": "recs:000000", "field": "00000000000000000001"}]})

    def _run(self, client, ref_hash=777):
        bundle = [_index_record(ref_hash)]
        return _adapter(client)._native_side_index_entries_for_bundles(
            [(bundle, "recs:000000", "00000000000000000002")])

    def test_an_earlier_location_survives_a_later_write(self):
        """The regression the batched read could cause: the stored location must still be there."""
        client = _Client({(self.locator_key, "777"): self.existing_locator})
        rows = _entries_by_key(self._run(client))
        locations = json.loads(rows[(self.locator_key, "777")]["value"])["locations"]
        fields = {loc.get("field") for loc in locations}
        self.assertIn("00000000000000000001", fields, "the earlier location was dropped")
        self.assertIn("00000000000000000002", fields, "the new location was not added")

    def test_the_existing_value_is_read_through_the_batch(self):
        client = _Client({(self.locator_key, "777"): self.existing_locator})
        self._run(client)
        self.assertEqual(1, client.batch_calls)
        self.assertEqual(0, client.hget_calls, "the batch should have covered every pair")

    def test_a_client_without_the_batch_read_still_accumulates(self):
        """The fallback path has to preserve the earlier location too."""
        client = _Client({(self.locator_key, "777"): self.existing_locator}, batch=False)
        rows = _entries_by_key(self._run(client))
        locations = json.loads(rows[(self.locator_key, "777")]["value"])["locations"]
        self.assertIn("00000000000000000001", {loc.get("field") for loc in locations})
        self.assertGreater(client.hget_calls, 0)

    def test_an_index_lookup_row_keeps_the_refs_already_stored(self):
        lookup_key = f"{PREFIX}:context_index_lookup:{backend.stable_hash(SCOPE_KEY)}"
        client = _Client({
            (lookup_key, "text"): json.dumps({"ref_hashes": [111], "posting_buckets": [1]}),
        })
        rows = _entries_by_key(self._run(client, ref_hash=222))
        stored = json.loads(rows[(lookup_key, "text")]["value"])
        self.assertIn(111, stored["ref_hashes"], "an earlier ref was dropped from the lookup row")
        self.assertIn(222, stored["ref_hashes"], "the new ref was not added")

    def test_nothing_stored_yet_is_not_treated_as_a_failure(self):
        client = _Client({})
        rows = _entries_by_key(self._run(client))
        locations = json.loads(rows[(self.locator_key, "777")]["value"])["locations"]
        self.assertEqual(["00000000000000000002"], [loc.get("field") for loc in locations])


if __name__ == "__main__":
    unittest.main()
