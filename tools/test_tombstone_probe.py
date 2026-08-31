#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""The delete-before-extract guard asks whether a tombstone exists before reading the whole log.

The guard has to run on every commit, but on a log with no tombstone it changes nothing --
`surviving_source_event_ids` returns None and every pending event is kept. Establishing that cost
a full raw record-log read per commit on a native backend, up to 2392 records, for one boolean.

These tests pin both halves: the probe answers from a type-filtered scan, and it answers
CONSERVATIVELY -- anything it cannot determine is reported as "a tombstone may exist", because a
false negative skips the guard and lets extraction re-materialise deleted content, while a false
positive only costs the read the guard used to do unconditionally.
"""
import unittest

try:
    from tools import matrixark_mcp_temporal_adapters as adapters
    from tools import matrixark_mcp_local_adapter as local_mod
except ImportError:  # run from tools/ dir
    import matrixark_mcp_temporal_adapters as adapters
    import matrixark_mcp_local_adapter as local_mod

TOMBSTONE = local_mod.MEMORY_TOMBSTONE_RECORD_TYPE


class _Client:
    """A native client whose candidate scan returns whatever the test wants."""

    def __init__(self, response, *, raises=False):
        self.response = response
        self.raises = raises
        self.calls = []

    def matrixark_scan_candidates(self, **kwargs):
        self.calls.append(kwargs)
        if self.raises:
            raise RuntimeError("engine unreachable")
        return self.response


def _adapter(client, *, raw_records=None):
    adapter = object.__new__(adapters.MatrixArkTemporalStoreDirectAdapter)
    adapter._storage_prefix = "matrixark:mcp"
    adapter._local_jsonl_enabled = False
    adapter._count_key = "matrixark:mcp:record_count"
    adapter._record_hash_key = "matrixark:mcp:records"
    adapter._shard_size = 1024
    adapter._client = client
    adapter.raw_reads = 0

    def _read_raw_records():
        adapter.raw_reads += 1
        return list(raw_records or [])

    adapter._read_raw_records = _read_raw_records
    return adapter


class TombstoneProbeTests(unittest.TestCase):
    def test_no_tombstone_rows_means_no_tombstone(self):
        adapter = _adapter(_Client({"records": []}))
        self.assertFalse(adapter.memory_tombstones_may_exist())

    def test_the_probe_does_not_read_the_raw_log(self):
        """The whole point: the answer arrives without the full record-log read."""
        adapter = _adapter(_Client({"records": []}), raw_records=[{"record_type": "context_event"}])
        adapter.memory_tombstones_may_exist()
        self.assertEqual(0, adapter.raw_reads)

    def test_it_asks_the_engine_for_tombstones_only(self):
        client = _Client({"records": []})
        _adapter(client).memory_tombstones_may_exist()
        # A store with no tombstones asks twice: the capped probe, then the read that confirms the
        # empty answer. A capped probe alone cannot tell "there are none" from "the newest one no
        # longer resolves", and a false negative here is the dangerous direction. The confirming
        # read is cheap precisely when it runs -- there is nothing to read.
        self.assertEqual(2, len(client.calls))
        for call in client.calls:
            self.assertEqual([TOMBSTONE], call["record_types"])
            self.assertEqual({}, call["scope"],
                             "the guard is store-wide: a tombstone in any scope counts")
        self.assertEqual({TOMBSTONE: 1}, client.calls[0].get("newest_by_type"),
                         "the probe asks for ONE tombstone, not every one")
        self.assertIsNone(client.calls[1].get("newest_by_type"),
                          "the confirming read must not be capped")

    def test_a_store_with_a_tombstone_is_answered_from_the_probe_alone(self):
        """The point of the probe: one tombstone settles it, so the rest are never read."""
        client = _Client({"records": [{"record_type": TOMBSTONE, "tombstone_kind": "delete"}]})
        self.assertTrue(_adapter(client).memory_tombstones_may_exist())
        self.assertEqual(1, len(client.calls),
                         "a positive answer must not go on to read every tombstone")
        self.assertEqual({TOMBSTONE: 1}, client.calls[0].get("newest_by_type"))

    def test_a_tombstone_row_is_reported(self):
        adapter = _adapter(_Client({"records": [{"record_type": TOMBSTONE, "tombstone_kind": "delete"}]}))
        self.assertTrue(adapter.memory_tombstones_may_exist())

    def test_rows_of_another_type_do_not_count_as_a_tombstone(self):
        """The filter is the engine's; this is the check that a wrong answer cannot sneak through."""
        adapter = _adapter(_Client({"records": [{"record_type": "context_event"}]}))
        self.assertFalse(adapter.memory_tombstones_may_exist())

    def test_a_scan_failure_reports_that_a_tombstone_may_exist(self):
        adapter = _adapter(_Client(None, raises=True))
        self.assertTrue(adapter.memory_tombstones_may_exist(),
                        "an unanswered question must not skip the guard")

    def test_an_unexpected_response_shape_reports_that_one_may_exist(self):
        for response in ({"records": "not a list"}, {}, None, []):
            with self.subTest(response=response):
                self.assertTrue(_adapter(_Client(response)).memory_tombstones_may_exist())

    def test_a_client_without_the_scan_falls_back_to_reading_the_log(self):
        class _Bare:
            pass

        adapter = _adapter(_Bare(), raw_records=[{"record_type": TOMBSTONE}])
        self.assertTrue(adapter.memory_tombstones_may_exist())
        self.assertEqual(1, adapter.raw_reads, "the fallback is the old behaviour, not a guess")

    def test_the_fallback_reads_the_log_and_reports_no_tombstone_when_there_is_none(self):
        class _Bare:
            pass

        adapter = _adapter(_Bare(), raw_records=[{"record_type": "context_event"}])
        self.assertFalse(adapter.memory_tombstones_may_exist())
        self.assertEqual(1, adapter.raw_reads)


if __name__ == "__main__":
    unittest.main()
