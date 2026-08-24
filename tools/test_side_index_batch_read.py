#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""The side index reads its existing values in one batched call, and never mistakes a miss for one.

Rebuilding the three side-index maps used to read each existing value with its own `hget`: 47 of
them for a single steady-state ingest, the majority of that ingest's engine traffic, each a round
trip holding the shared proxy lane.

The dangerous failure mode is not slowness, it is silence. These merges fold NEW refs into the
EXISTING value, so a batch that quietly returns "" for a pair it did not resolve does not error --
it drops every posting already stored under that key, and retrieval simply stops finding older
memories. So the batch is only ever allowed to supply pairs it actually returned; anything else
falls back to the per-entry read.
"""
import unittest

try:
    from tools import matrixark_temporal_direct_backend as backend
except ImportError:  # run from tools/ dir
    import matrixark_temporal_direct_backend as backend


class _Client:
    _NO_OVERRIDE = object()

    def __init__(self, stored, *, batch=True, raises=False, returns=_NO_OVERRIDE):
        self.stored = stored
        self.raises = raises
        self.returns = returns
        self.hget_calls = []
        self.batch_calls = []
        if not batch:
            self.batch_hget = None

    def hget(self, key, field):
        self.hget_calls.append((key, field))
        return self.stored.get((key, field), "")

    def batch_hget(self, entries):
        self.batch_calls.append(list(entries))
        if self.raises:
            raise RuntimeError("engine unreachable")
        # A sentinel, not None: a None response is itself a case worth testing, and a None
        # default would make it indistinguishable from "no override".
        if self.returns is not _Client._NO_OVERRIDE:
            return self.returns
        return [
            {"key": e["key"], "field": e["field"],
             "value": self.stored.get((e["key"], e["field"]), "")}
            for e in entries
        ]


def _adapter(client):
    obj = object.__new__(backend._TemporalDirectBackendMixin)
    obj._client = client
    return obj


class BatchReadTests(unittest.TestCase):
    def test_many_pairs_cost_one_call(self):
        client = _Client({("k", "a"): "1", ("k", "b"): "2", ("j", "c"): "3"})
        out = _adapter(client)._read_hash_values_best_effort([("k", "a"), ("k", "b"), ("j", "c")])
        self.assertEqual({("k", "a"): "1", ("k", "b"): "2", ("j", "c"): "3"}, out)
        self.assertEqual(1, len(client.batch_calls))
        self.assertEqual([], client.hget_calls)

    def test_a_client_without_the_batch_read_returns_nothing_to_use(self):
        """Empty map, not empty values: the caller then reads each pair the old way."""
        client = _Client({("k", "a"): "1"}, batch=False)
        self.assertEqual({}, _adapter(client)._read_hash_values_best_effort([("k", "a")]))

    def test_a_failing_batch_returns_nothing_to_use(self):
        client = _Client({("k", "a"): "1"}, raises=True)
        self.assertEqual({}, _adapter(client)._read_hash_values_best_effort([("k", "a")]))

    def test_pairs_the_batch_did_not_return_are_absent_rather_than_empty(self):
        """The pair must fall through to a real read, not be merged against ''."""
        client = _Client({("k", "a"): "1", ("k", "b"): "2"},
                         returns=[{"key": "k", "field": "a", "value": "1"}])
        out = _adapter(client)._read_hash_values_best_effort([("k", "a"), ("k", "b")])
        self.assertIn(("k", "a"), out)
        self.assertNotIn(("k", "b"), out,
                         "an unreturned pair must not appear as an empty value")

    def test_a_junk_response_yields_nothing_to_use(self):
        for junk in ("not a list", None, [None, 7, "x"]):
            with self.subTest(junk=junk):
                client = _Client({}, returns=junk)
                self.assertEqual({}, _adapter(client)._read_hash_values_best_effort([("k", "a")]))

    def test_no_pairs_means_no_call(self):
        client = _Client({})
        self.assertEqual({}, _adapter(client)._read_hash_values_best_effort([]))
        self.assertEqual([], client.batch_calls)

    def test_assume_fresh_skips_the_read_entirely(self):
        """The existing fast path: when the index is known fresh, nothing needs reading."""
        client = _Client({("k", "a"): "1"})
        adapter = _adapter(client)
        adapter._native_side_index_assume_fresh = True
        self.assertEqual({}, adapter._read_hash_values_best_effort([("k", "a")]))
        self.assertEqual([], client.batch_calls)


if __name__ == "__main__":
    unittest.main()
