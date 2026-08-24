#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""The delete-before-extract guard decides from the tombstones, not from the whole raw log.

One tombstone anywhere in the store used to send EVERY later commit back to a full raw-log read --
the probe only helped tombstone-free stores. The decision per pending event needs the tombstones
plus one ordering rule, so it is computable from a type-filtered scan.

The parts a wrong implementation would get silently wrong, pinned here:

* a `delete` of a pending id kills it with NO timestamp check -- ids are unique per ingest, so the
  tombstone postdates the event by construction, and demanding order would let a same-ms delete
  resurrect content;
* a re-ingest AFTER a forget survives -- kill-on-match without order would silently drop it from
  extraction;
* a timestamp TIE is ambiguous and takes the full read -- guessing either way is a real bug.
"""
import unittest

try:
    from tools import matrixark_mcp_temporal_adapters as adapters
except ImportError:  # run from tools/ dir
    import matrixark_mcp_temporal_adapters as adapters

TENANT = 11
USER = 22
SCOPE_KEY = f"t={TENANT}|u={USER}|s=33|"


def _event(event_id, *, at_ms):
    return {
        "record_type": "context_event",
        "event_id_hash": event_id,
        "text": "pending text %s" % event_id,
        "scope_key": SCOPE_KEY,
        "access_scope": {"tenant_hash": TENANT, "user_hash": USER, "scope_key": SCOPE_KEY},
        "updated_at_ms": at_ms,
    }


def _delete(target_id):
    return {"record_type": "matrixark_memory_tombstone", "tombstone_kind": "delete",
            "target_memory_id": str(target_id), "created_at_ms": 5000}


def _forget(*, at_ms):
    return {"record_type": "matrixark_memory_tombstone", "tombstone_kind": "forget",
            "target_tenant_hash": TENANT, "target_user_hash": USER,
            "target_scope_key": SCOPE_KEY, "created_at_ms": at_ms}


class _Adapter(adapters.MatrixArkTemporalStoreDirectAdapter):
    def __init__(self, tombstones):
        self.tombstones = tombstones
        self.full_reads = 0

    def _memory_tombstone_records(self):
        return self.tombstones

    def memory_tombstones_may_exist(self):  # the base fallback consults this
        return bool(self.tombstones) or self.tombstones is None

    def _read_raw_records(self):
        self.full_reads += 1
        return []  # the fallback ran; content does not matter for these tests


class GuardFromTombstonesTests(unittest.TestCase):
    def test_no_tombstones_keeps_everything_without_any_read(self):
        adapter = _Adapter([])
        self.assertIsNone(adapter.surviving_ids_for_pending_events([_event("1", at_ms=100)]))
        self.assertEqual(0, adapter.full_reads)

    def test_a_delete_of_a_pending_id_kills_it_without_needing_order(self):
        adapter = _Adapter([_delete("2")])
        surviving = adapter.surviving_ids_for_pending_events(
            [_event("1", at_ms=100), _event("2", at_ms=100)])
        self.assertEqual({"1"}, surviving)
        self.assertEqual(0, adapter.full_reads)

    def test_a_forget_after_the_event_kills_it(self):
        adapter = _Adapter([_forget(at_ms=200)])
        surviving = adapter.surviving_ids_for_pending_events([_event("1", at_ms=100)])
        self.assertEqual(set(), surviving)
        self.assertEqual(0, adapter.full_reads)

    def test_a_reingest_after_the_forget_survives(self):
        """The order-awareness the sweep guarantees; kill-on-match would silently drop this."""
        adapter = _Adapter([_forget(at_ms=200)])
        surviving = adapter.surviving_ids_for_pending_events([_event("9", at_ms=300)])
        self.assertEqual({"9"}, surviving)
        self.assertEqual(0, adapter.full_reads)

    def test_a_forget_for_another_subject_changes_nothing(self):
        other = {"record_type": "matrixark_memory_tombstone", "tombstone_kind": "forget",
                 "target_tenant_hash": 99, "target_user_hash": 88, "created_at_ms": 200}
        adapter = _Adapter([other])
        self.assertEqual({"1"}, adapter.surviving_ids_for_pending_events([_event("1", at_ms=100)]))

    def test_a_timestamp_tie_takes_the_full_read(self):
        """Ambiguity must not be guessed: either guess is a real bug in one direction."""
        adapter = _Adapter([_forget(at_ms=100)])
        adapter.surviving_ids_for_pending_events([_event("1", at_ms=100)])
        self.assertEqual(1, adapter.full_reads)

    def test_a_missing_event_timestamp_takes_the_full_read(self):
        adapter = _Adapter([_forget(at_ms=100)])
        event = _event("1", at_ms=100)
        del event["updated_at_ms"]
        adapter.surviving_ids_for_pending_events([event])
        self.assertEqual(1, adapter.full_reads)

    def test_an_unanswerable_scan_takes_the_full_read(self):
        adapter = _Adapter(None)
        adapter.surviving_ids_for_pending_events([_event("1", at_ms=100)])
        self.assertEqual(1, adapter.full_reads)

    def test_no_pending_events_asks_nothing(self):
        adapter = _Adapter(None)  # even an unanswerable scan must not be consulted
        self.assertIsNone(adapter.surviving_ids_for_pending_events([]))
        self.assertEqual(0, adapter.full_reads)


if __name__ == "__main__":
    unittest.main()
